from __future__ import annotations

import threading
import time
from typing import Optional, Any, Dict, Callable

# No radio library is imported here. The radio is reached through a
# core.transport.Transport, which keeps this module importable in tests even
# when no radio library is installed.

from core.logger_setup import server_logger
from core.config_loader import (
    load_app_config,
    load_bitcoin_rpc_config,
    load_reassembly_timeout,
)
from core.reassembler import (
    TransactionReassembler,
    InvalidChunkFormatError,
    MismatchedTotalChunksError,
    ReassemblyError,
    CHUNK_PREFIX,
    CHUNK_PARTS_DELIMITER,
)
from core.rpc_client import BitcoinRPCClient
from core.transaction_history import TransactionHistory
from core.transport import Transport, connect_transport

# Global instance of the TransactionReassembler
# Now initialized in main() with config timeout
transaction_reassembler: TransactionReassembler = None  # type: ignore

# Global instance of TransactionHistory for recording broadcast results
transaction_history: TransactionHistory = None  # type: ignore

bitcoin_rpc = None  # Global RPC connection for broadcasting

# Max length of a NACK message, to fit radio payload constraints. Transports
# with smaller messages lower it (see _max_nack_len).
MAX_NACK_LEN = 200


def _max_nack_len(transport: Optional[Transport]) -> int:
    limit = getattr(transport, "max_text_len", None)
    if isinstance(limit, int) and 0 < limit < MAX_NACK_LEN:
        return limit
    return MAX_NACK_LEN

# Ensure .env is loaded at application startup
load_app_config()


def _extract_session_id_from_raw_chunk(message_text: str) -> Optional[str]:
    """Rudimentary attempt to extract tx_session_id for NACKs if full parsing fails."""
    try:
        if message_text.startswith(CHUNK_PREFIX):
            parts = message_text[len(CHUNK_PREFIX) :].split(CHUNK_PARTS_DELIMITER)
            if len(parts) > 0 and parts[0]:
                return parts[0]
    except Exception:  # pylint: disable=broad-except
        pass  # Best effort
    return None


def handle_text_message(
    transport: Optional[Transport],
    sender_node_id_for_reply: str,
    message_text: str,
    send_reply_func: Optional[Callable[..., bool]] = None,
    logger=None,
) -> None:
    """
    Handle a direct text message addressed to this relay.

    The transport has already filtered out broadcasts, messages for other
    nodes, messages from ourselves and non-text packets. This function only
    implements the BTCMesh protocol: BTC_TX chunks are reassembled, ACKed and,
    once complete, broadcast to the Bitcoin network.

    Args:
        transport: Transport the message arrived on; replies are sent through it.
        sender_node_id_for_reply: Sender ID, in the transport's node ID format.
        message_text: The message text.
        send_reply_func: Injected for tests. Called as
            send_reply_func(transport, destination_id, message, session_id).
        logger: Injected for tests.
    """
    # Use dependency injection for reply function
    if send_reply_func is None:
        send_reply_func = send_reply
    # Use dependency injection for logger
    if logger is None:
        logger = server_logger

    try:
        logger.info(
            f"Direct text from {sender_node_id_for_reply}: '{message_text}'"
        )

        if message_text.startswith(CHUNK_PREFIX):
            # Log every received chunk for diagnostics
            try:
                # Always parse session_id and chunk_info for NACK, even if add_chunk fails
                parts = message_text[len(CHUNK_PREFIX) :].split(
                    CHUNK_PARTS_DELIMITER
                )
                session_id = parts[0] if len(parts) > 0 else "UNKNOWN"
                chunk_info = parts[1] if len(parts) > 1 else "UNKNOWN"
            except Exception:
                session_id = "UNKNOWN"
                chunk_info = "UNKNOWN"
            logger.debug(
                f"Received BTC_TX chunk: session_id={session_id}, chunk={chunk_info}, from={sender_node_id_for_reply}"
            )
            logger.info(
                f"Potential BTC transaction chunk from {sender_node_id_for_reply}. Processing..."
            )
            try:
                reassembled_hex = None  # Initialize for exception handler
                chunk_number = 1
                total_chunks = 1
                try:
                    if chunk_info and "/" in chunk_info:
                        chunk_number, total_chunks = chunk_info.split("/")
                        chunk_number = int(chunk_number)
                        total_chunks = int(total_chunks)
                except Exception:
                    # If chunk_info is invalid, leave chunk_number/total_chunks as 1
                    pass
                reassembled_hex = transaction_reassembler.add_chunk(
                    sender_node_id_for_reply, message_text
                )

                # ACK valid chunk and request next
                if chunk_number < total_chunks:
                    ack_msg = f"BTC_CHUNK_ACK|{session_id}|{chunk_number}|OK|REQUEST_CHUNK|{chunk_number+1}"
                elif chunk_number == total_chunks:
                    ack_msg = f"BTC_CHUNK_ACK|{session_id}|{chunk_number}|OK|ALL_CHUNKS_RECEIVED"
                send_reply_func(
                    transport, sender_node_id_for_reply, ack_msg, session_id
                )

                if reassembled_hex:
                    logger.info(
                        f"[Sender: {sender_node_id_for_reply}] Successfully reassembled transaction: "
                        f"{reassembled_hex[:80]}{'...' if len(reassembled_hex) > 80 else ''} (len: {len(reassembled_hex)})"
                    )

                    # Log the full raw transaction hex before broadcasting
                    logger.info(
                        f"[Sender: {sender_node_id_for_reply}, Session: {session_id}] Attempting to broadcast raw TX: {reassembled_hex}"
                    )

                    txid, error = bitcoin_rpc.broadcast_transaction(
                            reassembled_hex
                    )
                    if txid:
                        ack_msg = f"BTC_ACK|{session_id}|SUCCESS|TXID:{txid}"
                        send_reply_func(
                            transport, sender_node_id_for_reply, ack_msg, session_id
                        )
                        logger.info(
                            f"[Sender: {sender_node_id_for_reply}, Session: {session_id}] Broadcast success. TXID: {txid}"
                        )
                        # Record successful transaction in history
                        if transaction_history:
                            transaction_history.add(
                                session_id=session_id,
                                sender=sender_node_id_for_reply,
                                status="success",
                                txid=txid,
                                raw_tx=reassembled_hex
                            )
                    else:
                        # Optimize error message for size constraints
                        error_msg = str(error)
                        if "Transaction outputs already in utxo set" in error_msg:
                            error_msg = "TX already in UTXO set"
                        elif "Transaction already in block chain" in error_msg:
                            error_msg = "TX already in chain"
                        elif "insufficient fee" in error_msg.lower():
                            error_msg = "Insufficient fee"
                        elif "missing inputs" in error_msg.lower():
                            error_msg = "Missing inputs"
                        elif "bad-txns-inputs-spent" in error_msg:
                            error_msg = "Inputs spent"
                        elif "bad-txns-in-belowout" in error_msg:
                            error_msg = "Input < Output"
                        elif "too-long-mempool-chain" in error_msg:
                            error_msg = "Chain too long"
                        elif "mempool full" in error_msg.lower():
                            error_msg = "Mempool full"
                        elif "replacement transaction" in error_msg.lower():
                            error_msg = "RBF disabled"
                        elif "non-mandatory-script-verify-flag" in error_msg:
                            error_msg = "Script verify failed"
                        elif "transaction already abandoned" in error_msg.lower():
                            error_msg = "TX abandoned"
                        elif "bad-txns-nonstandard-inputs" in error_msg:
                            error_msg = "Non-std inputs"
                        elif "bad-txns-oversize" in error_msg:
                            error_msg = "TX too large"
                        elif "version" in error_msg.lower() and "reject" in error_msg.lower():
                            error_msg = "Version rejected"
                        elif "dust" in error_msg.lower():
                            error_msg = "Dust output"
                        elif "fee is too high" in error_msg.lower():
                            error_msg = "Fee too high"
                        elif "absurdly-high-fee" in error_msg.lower():
                            error_msg = "Absurd fee"
                        # Keep original error in logs but use concise version in NACK
                        nack_msg = f"BTC_NACK|{session_id}|ERROR|{error_msg}"
                        max_len = _max_nack_len(transport)
                        if len(nack_msg) > max_len:
                            nack_msg = nack_msg[: max_len - 3] + "..."
                        send_reply_func(
                            transport, sender_node_id_for_reply, nack_msg, session_id
                        )
                        logger.error(
                            f"[Sender: {sender_node_id_for_reply}, Session: {session_id}] Broadcast failed: {error}. Sending NACK."
                        )
                        # Record failed transaction in history
                        if transaction_history:
                            transaction_history.add(
                                session_id=session_id,
                                sender=sender_node_id_for_reply,
                                status="failed",
                                error=str(error),
                                raw_tx=reassembled_hex
                            )
            except (InvalidChunkFormatError, MismatchedTotalChunksError) as e:
                tx_session_id_for_nack = (
                    session_id
                    or _extract_session_id_from_raw_chunk(message_text)
                    or "UNKNOWN"
                )
                error_type_str = (
                    type(e)
                    .__name__.replace("Error", "")
                    .replace("Invalid", "Invalid ")
                )
                nack_message_detail = f"{error_type_str}: {str(e)}"
                nack_message = (
                    f"BTC_NACK|{tx_session_id_for_nack}|ERROR|{nack_message_detail}"
                )
                max_nack_len = _max_nack_len(transport)
                if len(nack_message) > max_nack_len:
                    available_len_for_detail = max_nack_len - len(
                        f"BTC_NACK|{tx_session_id_for_nack}|ERROR|...e"
                    )
                    if available_len_for_detail > 0:
                        nack_message_detail = (
                            nack_message_detail[:available_len_for_detail] + "..."
                        )
                    else:
                        nack_message_detail = "Error detail too long"
                    nack_message = f"BTC_NACK|{tx_session_id_for_nack}|ERROR|{nack_message_detail}"
                logger.error(
                    f"[Sender: {sender_node_id_for_reply}, Session: {tx_session_id_for_nack}] "
                    f"Reassembly error: {str(e)}. Sending NACK."
                )
                if transport:
                    send_reply_func(
                        transport,
                        sender_node_id_for_reply,
                        nack_message,
                        tx_session_id_for_nack,
                    )
                else:
                    server_logger.error(
                        f"[Sender: {sender_node_id_for_reply}, Session: {tx_session_id_for_nack}] "
                        f"Cannot send NACK: radio transport not available."
                    )
            except (
                ReassemblyError
            ) as e:  # Catch other specific reassembly errors if any
                tx_session_id_for_nack = (
                    _extract_session_id_from_raw_chunk(message_text) or "UNKNOWN"
                )
                server_logger.error(
                    f"[Sender: {sender_node_id_for_reply}, Session: {tx_session_id_for_nack}] "
                    f"General reassembly error: {e}. Notifying sender may be needed."
                )
                # Decide if a generic NACK is useful here. Current stories might NACK on timeout instead.
            except Exception as e:  # Catch-all for unexpected errors in add_chunk or broadcast
                tx_session_id_for_nack = (
                    _extract_session_id_from_raw_chunk(message_text) or "UNKNOWN"
                )
                server_logger.error(
                    f"[Sender: {sender_node_id_for_reply}, Session: {tx_session_id_for_nack}] "
                    f"Unexpected error processing chunk: {e}. Not NACKing automatically.",
                    exc_info=True,
                )
                # Record failed transaction in history if we had a reassembled tx
                if transaction_history:
                    transaction_history.add(
                        session_id=tx_session_id_for_nack,
                        sender=sender_node_id_for_reply,
                        status="failed",
                        error=str(e),
                        raw_tx=reassembled_hex  # Will be None if error was during reassembly
                    )
        else:
            server_logger.info(
                f"Std direct text from {sender_node_id_for_reply} (not BTC_TX): '{message_text}'"
            )

    except Exception as e:
        server_logger.error(f"Error processing received message: {e}", exc_info=True)


def send_reply(
    transport: Optional[Transport],
    destination_id: str,
    message_text: str,
    tx_session_id: Optional[str] = None,
) -> bool:
    """
    Sends a reply message to a node through the radio transport.

    Args:
        transport: The connected transport.
        destination_id: Target node ID, in the transport's node ID format.
        message_text: The text message to send.
        tx_session_id: Optional transaction session ID for logging context.

    Returns:
        True if the message was handed to the radio, False otherwise.
    """
    log_prefix = f"[Session: {tx_session_id}] " if tx_session_id else ""
    if not transport:
        server_logger.error(
            f"{log_prefix}Cannot send reply: radio transport is not available."
        )
        return False

    if not destination_id or not transport.is_valid_node_id(destination_id):
        server_logger.error(
            f"{log_prefix}Cannot send reply: Invalid destination_id format "
            f"'{destination_id}' for {transport.name}. "
            f"Message: '{message_text}'"
        )
        return False

    server_logger.info(
        f"{log_prefix}Attempting to send reply to {destination_id}: '{message_text}'"
    )
    try:
        transport.send_text(destination_id, message_text)
    except Exception as e:
        server_logger.error(
            f"{log_prefix}Failed to send reply to {destination_id}: {e}. "
            f"Message: '{message_text}'",
            exc_info=True,
        )
        return False

    server_logger.info(
        f"{log_prefix}Successfully queued reply to {destination_id}: '{message_text}'"
    )
    return True


def send_timeout_nacks(
    transport: Optional[Transport],
    timed_out_sessions: list,
    send_reply_func: Optional[Callable[..., bool]] = None,
) -> None:
    """Record and NACK reassembly sessions that timed out."""
    if send_reply_func is None:
        send_reply_func = send_reply
    for session_info in timed_out_sessions:
        sender = session_info["sender_id_str"]
        session_id = session_info["tx_session_id"]
        nack_message = f"BTC_NACK|{session_id}|ERROR|{session_info['error_message']}"
        max_len = _max_nack_len(transport)
        if len(nack_message) > max_len:
            nack_message = nack_message[: max_len - 3] + "..."

        server_logger.info(
            f"[Sender: {sender}, Session: {session_id}] "
            f"Session timed out. Sending NACK: {nack_message}"
        )
        if transaction_history:
            transaction_history.add(
                session_id=session_id,
                sender=sender,
                status="failed",
                error=session_info["error_message"],
                raw_tx=None,  # No complete tx for timeouts
            )
        send_reply_func(transport, sender, nack_message, session_id)


def main(stop_event: Optional[threading.Event] = None,
        rpc_config: Optional[Dict[str, Any]] = None,
        serial_port: Optional[str] = None,
        reassembly_timeout: Optional[int] = None,
        session_update_callback: Optional[Callable[[list], None]] = None,
        transport_type: Optional[str] = None) -> None:
    """
    Main function for the BTC Mesh Server.

    Args:
        stop_event: Optional threading.Event to signal server shutdown.
                    If None, runs until KeyboardInterrupt.
        rpc_config: Optional dict with RPC connection settings (host, port, user, password).
                    If None, loads from environment/.env file.
        serial_port: Optional serial port for the radio device.
                    If None, loads from environment or auto-detects.
        reassembly_timeout: Optional timeout in seconds for transaction reassembly.
                    If None, loads from environment or uses default (300s).
        session_update_callback: Optional callback called periodically with list of active
                    session info dicts. Each dict contains: session_id, sender,
                    chunks_received, total_chunks, elapsed_seconds.
        transport_type: Optional radio transport ('meshtastic' or 'meshcore').
                    If None, loads BTCMESH_TRANSPORT from environment.
    """
    global transaction_reassembler
    global transaction_history
    server_logger.info("Starting BTC Mesh Relay Server...")

    tor_process = None
    tor_data_dir = None
    try:
        # --- Story 5.2 & 18.3: Load reassembly timeout from parameter, env, or default ---
        if reassembly_timeout is not None:
            timeout_seconds = reassembly_timeout
            timeout_source = "gui"
        else:
            timeout_seconds, timeout_source = load_reassembly_timeout()
        transaction_reassembler = TransactionReassembler(
            timeout_seconds=timeout_seconds
        )
        server_logger.info(
            f"TransactionReassembler initialized with timeout: {timeout_seconds}s (source: {timeout_source})"
        )
        # --- End Story 5.2 & 18.3 integration ---

        # --- Story 17.3: Initialize transaction history ---
        transaction_history = TransactionHistory()
        server_logger.info(
            f"TransactionHistory initialized. File: {transaction_history.filepath}"
        )

        # --- Story 4.2: Connect to Bitcoin RPC ---
        try:
            # Use injected config if provided, otherwise load from environment
            if rpc_config is None:
                rpc_config = load_bitcoin_rpc_config()
            global bitcoin_rpc

            bitcoin_rpc = BitcoinRPCClient(rpc_config)
            # Build host display string for GUI status (mask .onion for privacy)
            host = rpc_config.get('host', 'unknown')
            port = rpc_config.get('port', '')
            is_tor = host.endswith('.onion')
            host_display = "*.onion" if is_tor else f"{host}:{port}"
            server_logger.info(
                f"Connected to Bitcoin Core RPC node successfully. Host: {host_display}, Tor: {is_tor}, Chain: {bitcoin_rpc.chain}"
            )
        except Exception as e:
            bitcoin_rpc = None
            server_logger.error(
                f"Failed to connect to Bitcoin Core RPC node: {e}. Continuing without RPC connection."
            )
        # Store bitcoin_rpc for later use (e.g., as a global or pass to handlers)
        # --- End Story 4.2 integration ---

        transport = connect_transport(
            kind=transport_type, port=serial_port, logger=server_logger
        )
        if not transport:
            server_logger.error("Failed to initialize radio interface. Exiting.")
            return

        def on_text(sender_id: str, text: str) -> None:
            handle_text_message(transport, sender_id, text)

        try:
            transport.subscribe(on_text)
            server_logger.info(
                f"Registered {transport.name} message handler. Waiting for messages..."
            )
        except Exception as e:
            server_logger.error(
                f"Failed to subscribe to {transport.name} messages: {e}", exc_info=True
            )
            transport.close()
            return

        try:
            cleanup_interval = 10  # seconds, same as sleep
            session_update_interval = 1  # Update session info every second for GUI
            last_cleanup_time = time.time()
            last_session_update_time = time.time()

            while True:
                # Check for stop signal from GUI
                if stop_event and stop_event.is_set():
                    server_logger.info("Stop signal received. Shutting down...")
                    break

                current_time = time.time()

                # Send session updates to GUI callback
                if session_update_callback and current_time - last_session_update_time >= session_update_interval:
                    sessions_info = transaction_reassembler.get_active_sessions_info()
                    session_update_callback(sessions_info)
                    last_session_update_time = current_time

                if current_time - last_cleanup_time >= cleanup_interval:
                    server_logger.debug(
                        "Running periodic cleanup of stale reassembly sessions..."
                    )
                    timed_out_sessions = (
                        transaction_reassembler.cleanup_stale_sessions()
                    )
                    send_timeout_nacks(transport, timed_out_sessions)
                    last_cleanup_time = current_time
                time.sleep(
                    1.0
                )  # Check for cleanup every second, actual cleanup every 10s.
        except KeyboardInterrupt:
            server_logger.info("Server shutting down by user request (Ctrl+C).")
        except Exception as e:
            server_logger.error(f"Unhandled exception in main loop: {e}", exc_info=True)
        finally:
            server_logger.info(f"Closing {transport.name} interface...")
            transport.close()

    except Exception as e:
        server_logger.error(f"Unhandled exception in main function: {e}", exc_info=True)


if __name__ == "__main__":
    main()
