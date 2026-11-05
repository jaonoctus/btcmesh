import unittest
import unittest.mock
from unittest.mock import MagicMock, call, patch

from btcmesh_server import (
    handle_text_message,
    send_reply,
    send_timeout_nacks,
)

from core.reassembler import (
    InvalidChunkFormatError,
    MismatchedTotalChunksError,
    ReassemblyError,
    CHUNK_PREFIX,
    TransactionReassembler,
)

# Packet filtering (broadcasts, messages for other nodes, from self, non-text)
# and device initialization are Meshtastic specific and tested in
# tests/test_meshtastic_transport.py. The tests here exercise the BTCMesh
# protocol on top of an already filtered (sender_id, text) message.

SENDER = "!00012345"


def make_transport():
    """A Transport double that accepts Meshtastic style node IDs."""
    transport = MagicMock()
    transport.name = "Meshtastic"
    transport.is_valid_node_id.side_effect = lambda node_id: node_id.startswith("!")
    return transport


class ServerHandlerTestCase(unittest.TestCase):
    """Patches the server's globals that handle_text_message relies on."""

    def setUp(self):
        self.patcher_logger = patch("btcmesh_server.server_logger", MagicMock())
        self.mock_logger = self.patcher_logger.start()

        self.patcher_reassembler = patch(
            "btcmesh_server.transaction_reassembler", autospec=True
        )
        self.mock_reassembler = self.patcher_reassembler.start()

        self.patcher_send_reply = patch("btcmesh_server.send_reply", autospec=True)
        self.mock_send_reply = self.patcher_send_reply.start()

        self.patcher_extract_id = patch(
            "btcmesh_server._extract_session_id_from_raw_chunk", autospec=True
        )
        self.mock_extract_id = self.patcher_extract_id.start()

        self.patcher_rpc = patch("btcmesh_server.bitcoin_rpc")
        self.mock_rpc = self.patcher_rpc.start()

        self.patcher_history = patch("btcmesh_server.transaction_history", None)
        self.patcher_history.start()

        self.transport = make_transport()

    def tearDown(self):
        self.patcher_logger.stop()
        self.patcher_reassembler.stop()
        self.patcher_send_reply.stop()
        self.patcher_extract_id.stop()
        self.patcher_rpc.stop()
        self.patcher_history.stop()


class TestMessageHandling(ServerHandlerTestCase):
    def test_receive_standard_text_message_for_server(self):
        """A non-BTC_TX text is logged and otherwise ignored."""
        handle_text_message(self.transport, SENDER, "Hello from the mesh!")

        self.mock_reassembler.add_chunk.assert_not_called()
        self.mock_send_reply.assert_not_called()
        self.mock_logger.info.assert_any_call(
            f"Std direct text from {SENDER} (not BTC_TX): 'Hello from the mesh!'"
        )

    def test_receive_btc_tx_chunk_partial_acks_and_requests_next(self):
        """A chunk that does not complete the transaction is ACKed with REQUEST_CHUNK."""
        message_text = f"{CHUNK_PREFIX}sess2|1/2|partial_payload_data"
        self.mock_reassembler.add_chunk.return_value = None

        handle_text_message(self.transport, SENDER, message_text)

        self.mock_reassembler.add_chunk.assert_called_once_with(SENDER, message_text)
        self.mock_send_reply.assert_called_once_with(
            self.transport,
            SENDER,
            "BTC_CHUNK_ACK|sess2|1|OK|REQUEST_CHUNK|2",
            "sess2",
        )
        self.mock_rpc.broadcast_transaction.assert_not_called()

    def test_receive_btc_tx_chunk_success_reassembly_broadcasts(self):
        """The last chunk is ACKed, the transaction broadcast and a BTC_ACK sent."""
        session_id = "sessok"
        message_text = f"{CHUNK_PREFIX}{session_id}|1/1|deadbeefcafebabe"
        self.mock_reassembler.add_chunk.return_value = "deadbeefcafebabe"
        self.mock_rpc.broadcast_transaction.return_value = ("txid123", None)

        handle_text_message(self.transport, SENDER, message_text)

        self.mock_rpc.broadcast_transaction.assert_called_once_with("deadbeefcafebabe")
        self.assertEqual(
            self.mock_send_reply.call_args_list,
            [
                call(
                    self.transport,
                    SENDER,
                    f"BTC_CHUNK_ACK|{session_id}|1|OK|ALL_CHUNKS_RECEIVED",
                    session_id,
                ),
                call(
                    self.transport,
                    SENDER,
                    f"BTC_ACK|{session_id}|SUCCESS|TXID:txid123",
                    session_id,
                ),
            ],
        )

    def test_receive_btc_tx_chunk_broadcast_failure_sends_condensed_nack(self):
        """An RPC error is condensed into a short BTC_NACK."""
        session_id = "sessfail"
        message_text = f"{CHUNK_PREFIX}{session_id}|1/1|deadbeef"
        self.mock_reassembler.add_chunk.return_value = "deadbeef"
        self.mock_rpc.broadcast_transaction.return_value = (
            None,
            "error code -26: min relay fee not met, insufficient fee",
        )

        handle_text_message(self.transport, SENDER, message_text)

        self.mock_send_reply.assert_called_with(
            self.transport,
            SENDER,
            f"BTC_NACK|{session_id}|ERROR|Insufficient fee",
            session_id,
        )

    def test_receive_btc_tx_chunk_invalid_format_sends_nack(self):
        """InvalidChunkFormatError from add_chunk results in a NACK."""
        session_id = "sessbad"
        message_text = f"{CHUNK_PREFIX}{session_id}|bad_chunk_format|payload"
        exception_message = "Invalid chunk format detected by reassembler"
        self.mock_reassembler.add_chunk.side_effect = InvalidChunkFormatError(
            exception_message
        )

        handle_text_message(self.transport, SENDER, message_text)

        self.mock_reassembler.add_chunk.assert_called_once_with(SENDER, message_text)
        self.mock_send_reply.assert_called_once_with(
            self.transport,
            SENDER,
            f"BTC_NACK|{session_id}|ERROR|Invalid ChunkFormat: {exception_message}",
            session_id,
        )
        self.mock_logger.error.assert_any_call(
            f"[Sender: {SENDER}, Session: {session_id}] "
            f"Reassembly error: {exception_message}. Sending NACK."
        )

    def test_receive_btc_tx_chunk_mismatched_total_sends_nack(self):
        """MismatchedTotalChunksError from add_chunk results in a NACK."""
        session_id = "sessmis"
        message_text = f"{CHUNK_PREFIX}{session_id}|1/3|payload_part"
        exception_message = "Mismatched total chunks detected by reassembler"
        self.mock_reassembler.add_chunk.side_effect = MismatchedTotalChunksError(
            exception_message
        )

        handle_text_message(self.transport, SENDER, message_text)

        self.mock_send_reply.assert_called_once_with(
            self.transport,
            SENDER,
            f"BTC_NACK|{session_id}|ERROR|MismatchedTotalChunks: {exception_message}",
            session_id,
        )

    def test_nack_is_truncated_to_max_length(self):
        """Long reassembly error details are cut so the NACK fits in one packet."""
        session_id = "sesslong"
        message_text = f"{CHUNK_PREFIX}{session_id}|1/2|payload"
        self.mock_reassembler.add_chunk.side_effect = InvalidChunkFormatError("x" * 500)

        handle_text_message(self.transport, SENDER, message_text)

        nack = self.mock_send_reply.call_args.args[2]
        self.assertTrue(nack.startswith(f"BTC_NACK|{session_id}|ERROR|"))
        self.assertLessEqual(len(nack), 200)

    def test_receive_btc_tx_chunk_other_reassembly_error_no_nack_by_default(self):
        """A generic ReassemblyError from add_chunk does NOT send a NACK."""
        session_id = "sessother"
        message_text = f"{CHUNK_PREFIX}{session_id}|1/1|payload_for_other_error"
        exception_message = "Generic reassembly problem"
        self.mock_reassembler.add_chunk.side_effect = ReassemblyError(exception_message)
        self.mock_extract_id.return_value = session_id

        handle_text_message(self.transport, SENDER, message_text)

        self.mock_reassembler.add_chunk.assert_called_once_with(SENDER, message_text)
        self.mock_extract_id.assert_called_once_with(message_text)
        self.mock_send_reply.assert_not_called()
        self.mock_logger.error.assert_any_call(
            f"[Sender: {SENDER}, Session: {session_id}] "
            f"General reassembly error: {exception_message}. "
            f"Notifying sender may be needed."
        )

    def test_receive_btc_tx_chunk_unexpected_error_no_nack(self):
        """An unexpected Exception from add_chunk does NOT send a NACK."""
        session_id = "sessunexp"
        message_text = f"{CHUNK_PREFIX}{session_id}|1/1|payload"
        exception_message = "Totally unexpected problem during add_chunk"
        self.mock_reassembler.add_chunk.side_effect = Exception(exception_message)
        self.mock_extract_id.return_value = session_id

        handle_text_message(self.transport, SENDER, message_text)

        self.mock_send_reply.assert_not_called()
        self.mock_logger.error.assert_any_call(
            f"[Sender: {SENDER}, Session: {session_id}] "
            f"Unexpected error processing chunk: {exception_message}. "
            f"Not NACKing automatically.",
            exc_info=True,
        )

    def test_explicit_send_reply_func_is_used(self):
        """An injected send_reply_func takes precedence over send_reply."""
        injected = MagicMock()
        self.mock_reassembler.add_chunk.return_value = None

        handle_text_message(
            self.transport,
            SENDER,
            f"{CHUNK_PREFIX}sessinj|1/2|aa",
            send_reply_func=injected,
        )

        injected.assert_called_once_with(
            self.transport, SENDER, "BTC_CHUNK_ACK|sessinj|1|OK|REQUEST_CHUNK|2", "sessinj"
        )
        self.mock_send_reply.assert_not_called()


class TestReplySending(unittest.TestCase):
    def setUp(self):
        self.patcher_logger = patch("btcmesh_server.server_logger")
        self.mock_logger = self.patcher_logger.start()
        self.transport = make_transport()

    def tearDown(self):
        self.patcher_logger.stop()

    def test_send_reply_success(self):
        dest_id = "!abcdef12"
        session_id = "sess123"
        message = f"BTC_ACK|{session_id}|SUCCESS|TXID:sampletxid"

        result = send_reply(self.transport, dest_id, message, session_id)

        self.assertTrue(result)
        self.transport.send_text.assert_called_once_with(dest_id, message)
        self.mock_logger.info.assert_any_call(
            f"[Session: {session_id}] Attempting to send reply to {dest_id}: '{message}'"
        )
        self.mock_logger.info.assert_any_call(
            f"[Session: {session_id}] Successfully queued reply to {dest_id}: '{message}'"
        )

    def test_send_reply_without_session_id_has_no_prefix(self):
        dest_id = "!abcdef12"
        message = "BTC_NACK||ERROR|Reassembly timeout"

        self.assertTrue(send_reply(self.transport, dest_id, message))
        self.mock_logger.info.assert_any_call(
            f"Attempting to send reply to {dest_id}: '{message}'"
        )

    def test_send_reply_invalid_destination_id_format(self):
        dest_id = "dummyNodeId3"  # Missing '!'
        message = "BTC_NACK||ERROR|Invalid destination"
        session_id = "sess456"

        result = send_reply(self.transport, dest_id, message, session_id)

        self.assertFalse(result)
        self.transport.send_text.assert_not_called()
        self.mock_logger.error.assert_called_once_with(
            f"[Session: {session_id}] Cannot send reply: Invalid "
            f"destination_id format '{dest_id}' for Meshtastic. "
            f"Message: '{message}'"
        )

    def test_send_reply_send_text_raises_exception(self):
        self.transport.send_text.side_effect = Exception("Send failed")
        dest_id = "!errorabc"
        message = "BTC_ACK|sessErr|SUCCESS|TXID:errtxid"
        session_id = "sessErr"

        result = send_reply(self.transport, dest_id, message, session_id)

        self.assertFalse(result)
        self.mock_logger.error.assert_called_once_with(
            f"[Session: {session_id}] Failed to send reply to {dest_id}: "
            f"Send failed. Message: '{message}'",
            exc_info=True,
        )

    def test_send_reply_no_transport(self):
        session_id = "noIface"
        result = send_reply(None, "!abcdef12", "BTC_NACK|noIface|ERROR|x", session_id)

        self.assertFalse(result)
        self.mock_logger.error.assert_called_once_with(
            f"[Session: {session_id}] Cannot send reply: "
            f"radio transport is not available."
        )


class TestTimeoutNacks(unittest.TestCase):
    def setUp(self):
        self.patcher_logger = patch("btcmesh_server.server_logger")
        self.patcher_logger.start()
        self.patcher_history = patch("btcmesh_server.transaction_history")
        self.mock_history = self.patcher_history.start()

    def tearDown(self):
        self.patcher_logger.stop()
        self.patcher_history.stop()

    def test_timed_out_sessions_are_nacked_and_recorded(self):
        transport = make_transport()
        reply = MagicMock()
        sessions = [
            {
                "sender_id_str": SENDER,
                "tx_session_id": "sesst",
                "error_message": "Reassembly timeout",
            }
        ]

        send_timeout_nacks(transport, sessions, send_reply_func=reply)

        reply.assert_called_once_with(
            transport, SENDER, "BTC_NACK|sesst|ERROR|Reassembly timeout", "sesst"
        )
        self.mock_history.add.assert_called_once_with(
            session_id="sesst",
            sender=SENDER,
            status="failed",
            error="Reassembly timeout",
            raw_tx=None,
        )


class TestTransactionReassemblerStory21(unittest.TestCase):
    def setUp(self):
        # Patch logger for log assertions
        self.logger_patcher = patch("core.reassembler.server_logger", MagicMock())
        self.mock_logger = self.logger_patcher.start()
        self.reassembler = TransactionReassembler(
            timeout_seconds=1
        )  # Short timeout for test
        # Use already-formatted sender ID (server layer handles formatting)
        self.sender_id = "!3039"  # Equivalent to formatting 12345 as !hex
        self.session_id = "story21sess"

    def tearDown(self):
        self.logger_patcher.stop()

    def test_logs_on_new_session_and_chunk(self):
        chunk1 = f"BTC_TX|{self.session_id}|1/2|AAA"
        self.reassembler.add_chunk(self.sender_id, chunk1)
        # Should log new session start and chunk add
        log_ctx = f"[Sender: {self.sender_id}, Session: {self.session_id}]"
        self.mock_logger.info.assert_any_call(
            f"{log_ctx} New reassembly session started. Expecting 2 chunks."
        )
        self.mock_logger.debug.assert_any_call(
            f"{log_ctx} Added chunk 1/2. Collected 1 chunks."
        )

    def test_logs_on_duplicate_chunk(self):
        chunk1 = f"BTC_TX|{self.session_id}|1/2|AAA"
        self.reassembler.add_chunk(self.sender_id, chunk1)
        self.reassembler.add_chunk(self.sender_id, chunk1)  # Duplicate
        log_ctx = f"[Sender: {self.sender_id}, Session: {self.session_id}]"
        self.mock_logger.warning.assert_any_call(
            f"{log_ctx} Duplicate chunk 1/2 received. Ignoring."
        )

    def test_logs_on_out_of_order_and_reassembly_success(self):
        chunk2 = f"BTC_TX|{self.session_id}|2/2|BBB"
        chunk1 = f"BTC_TX|{self.session_id}|1/2|AAA"
        self.reassembler.add_chunk(self.sender_id, chunk2)
        self.reassembler.add_chunk(self.sender_id, chunk1)
        log_ctx = f"[Sender: {self.sender_id}, Session: {self.session_id}]"
        self.mock_logger.debug.assert_any_call(
            f"{log_ctx} Added chunk 2/2. Collected 1 chunks."
        )
        self.mock_logger.debug.assert_any_call(
            f"{log_ctx} Added chunk 1/2. Collected 2 chunks."
        )
        self.mock_logger.info.assert_any_call(
            f"{log_ctx} All 2 chunks received. Attempting reassembly."
        )
        self.mock_logger.info.assert_any_call(f"{log_ctx} Reassembly successful.")

    def test_logs_on_timeout(self):
        chunk1 = f"BTC_TX|{self.session_id}|1/2|AAA"
        self.reassembler.add_chunk(self.sender_id, chunk1)
        import time as _time

        _time.sleep(1.1)
        self.reassembler.cleanup_stale_sessions()
        # Timeout log uses the sender_id as provided (already formatted)
        log_ctx = f"[Sender: {self.sender_id}, Session: {self.session_id}]"
        self.mock_logger.warning.assert_any_call(
            f"{log_ctx} Reassembly timeout after 1s. Received 1/2 chunks. Discarding."
        )
        self.mock_logger.info.assert_any_call(
            "Identified 1 stale reassembly sessions for cleanup and NACK."
        )

    def test_logs_timeout_value_on_init(self):
        # The info log for timeout value should be called on init
        self.mock_logger.info.assert_any_call(
            "TransactionReassembler initialized with timeout: 1s"
        )


class TestHexValidationStory22(unittest.TestCase):
    def setUp(self):
        from core.reassembler import TransactionReassembler

        self.reassembler = TransactionReassembler(timeout_seconds=1)
        self.sender_id = 54321
        self.session_id = "hexval22"

    def test_valid_hex_string(self):
        """Given a valid hex string, When validated, Then it passes validation."""
        valid_hex = "deadbeefCAFEBABE0123456789abcdef"
        # Should not raise
        try:
            int(valid_hex, 16)
        except ValueError:
            self.fail("Valid hex string did not pass validation")

    def test_invalid_hex_string(self):
        """Given an invalid hex string, When validated, Then it fails validation and logs error."""
        invalid_hex = "deadbeefZZZ01234"
        with self.assertRaises(ValueError):
            int(invalid_hex, 16)

    def test_integration_reassembled_hex_validation(self):
        """Given a reassembled payload, When it is not valid hex, Then the server should log and prepare NACK."""
        # Simulate reassembly
        chunk1 = f"BTC_TX|{self.session_id}|1/2|deadbeef"
        chunk2 = f"BTC_TX|{self.session_id}|2/2|ZZZ01234"  # Invalid hex part
        self.reassembler.add_chunk(self.sender_id, chunk1)
        reassembled = self.reassembler.add_chunk(self.sender_id, chunk2)
        self.assertEqual(reassembled, "deadbeefZZZ01234")
        # Now validate
        with self.assertRaises(ValueError):
            int(reassembled, 16)

        # In the real server, this would trigger a NACK and log an error


class TestTransactionDecodeStory23(unittest.TestCase):
    def setUp(self):
        from core.transaction_parser import decode_raw_transaction_hex

        self.decode = decode_raw_transaction_hex

    def test_valid_raw_transaction_hex(self):
        """Given a valid raw transaction hex, When decoded, Then it returns a dict with fields."""
        # This is a minimal valid Bitcoin tx (version 1, 1 input, 1 output, locktime 0)
        # 01000000 (version 1)
        # 01 (input count)
        # 00..00 (32 bytes prev txid) + 00000000 (vout) + 00 (script len) + ffffffff (sequence)
        # 01 (output count)
        # 00e1f50500000000 (8 bytes value = 1 BTC)
        # 00 (script len)
        # 00000000 (locktime)
        raw_hex = (
            "01000000"
            + "01"
            + "00" * 32
            + "00000000"
            + "00"
            + "ffffffff"
            + "01"
            + "00e1f50500000000"
            + "00"
            + "00000000"
        )
        result = self.decode(raw_hex)
        self.assertIsInstance(result, dict)
        self.assertEqual(result["version"], 1)
        self.assertEqual(result["input_count"], 1)
        self.assertEqual(result["locktime"], 0)
        # output_count is None (placeholder)

    def test_malformed_raw_transaction_hex(self):
        """Given a malformed raw transaction hex, When decoded, Then it raises ValueError and logs error."""
        bad_hex = "deadbeef"  # Too short to be a valid tx
        with self.assertRaises(ValueError):
            self.decode(bad_hex)


class TestTransactionSanityChecksStory31(unittest.TestCase):
    def setUp(self):
        # Minimal stub for validation logic, to be replaced by actual import
        self.valid_tx = {
            "version": 1,
            "input_count": 1,
            "output_count": 1,
            "locktime": 0,
        }
        self.no_inputs_tx = {
            "version": 1,
            "input_count": 0,
            "output_count": 1,
            "locktime": 0,
        }
        self.no_outputs_tx = {
            "version": 1,
            "input_count": 1,
            "output_count": 0,
            "locktime": 0,
        }
        # The actual validation function will be implemented in core.transaction_parser

    def test_no_inputs_fails_validation(self):
        """Given a decoded transaction with zero inputs, When validated, Then it fails and NACK is prepared."""
        from core.transaction_parser import basic_sanity_check

        valid, error = basic_sanity_check(self.no_inputs_tx)
        self.assertFalse(valid)
        self.assertIn("No inputs", error)

    def test_no_outputs_fails_validation(self):
        """Given a decoded transaction with zero outputs, When validated, Then it fails and NACK is prepared."""
        from core.transaction_parser import basic_sanity_check

        valid, error = basic_sanity_check(self.no_outputs_tx)
        self.assertFalse(valid)
        self.assertIn("No outputs", error)

    def test_valid_tx_passes_validation(self):
        """Given a decoded transaction with at least one input and one output, When validated, Then it passes."""
        from core.transaction_parser import basic_sanity_check

        valid, error = basic_sanity_check(self.valid_tx)
        self.assertTrue(valid)
        self.assertIsNone(error)


class TestBitcoinRpcConfigStory41(unittest.TestCase):
    def setUp(self):
        self.env_keys = [
            "BITCOIN_RPC_HOST",
            "BITCOIN_RPC_PORT",
            "BITCOIN_RPC_USER",
            "BITCOIN_RPC_PASSWORD",
        ]
        self.default_env = {
            "BITCOIN_RPC_HOST": "127.0.0.1",
            "BITCOIN_RPC_PORT": "8332",
            "BITCOIN_RPC_USER": "testuser",
            "BITCOIN_RPC_PASSWORD": "testpass",
        }

    def test_all_fields_present_in_env(self):
        """Given all required RPC fields in .env, When loaded, Then config is correct."""
        from core.config_loader import load_bitcoin_rpc_config

        with unittest.mock.patch.dict("os.environ", self.default_env, clear=True):
            config = load_bitcoin_rpc_config()
            self.assertEqual(config["host"], "127.0.0.1")
            self.assertEqual(config["port"], 8332)
            self.assertEqual(config["user"], "testuser")
            self.assertEqual(config["password"], "testpass")

    def test_missing_required_field_raises(self):
        """Given missing required RPC fields, When loaded, Then error is raised or logged."""
        from core.config_loader import load_bitcoin_rpc_config

        env = self.default_env.copy()
        del env["BITCOIN_RPC_USER"]
        with unittest.mock.patch.dict("os.environ", env, clear=True):
            with self.assertRaises(ValueError):
                load_bitcoin_rpc_config()

    def test_partial_fields_use_defaults(self):
        """Given only host and port in .env, When loaded, Then user/password missing triggers error."""
        from core.config_loader import load_bitcoin_rpc_config

        env = {
            "BITCOIN_RPC_HOST": "10.0.0.2",
            "BITCOIN_RPC_PORT": "18443",
        }
        with unittest.mock.patch.dict("os.environ", env, clear=True):
            with self.assertRaises(ValueError):
                load_bitcoin_rpc_config()


class TestBitcoinRpcConnectionStory42(unittest.TestCase):
    def setUp(self):
        self.valid_config = {
            "host": "127.0.0.1",
            "port": 8332,
            "user": "testuser",
            "password": "testpass",
        }

    def test_valid_config_node_reachable(self):
        """Given valid config and node reachable, When connecting, Then connection is established."""
        with unittest.mock.patch("core.rpc_client.requests.post") as mock_post:
            from core.rpc_client import BitcoinRPCClient

            # Configure the mock to return a successful response
            mock_response = MagicMock()
            mock_response.json.return_value = {"result": {"chain": "main"}, "error": None}
            mock_post.return_value = mock_response

            # Call the method
            rpc = BitcoinRPCClient(self.valid_config)

            # Assertions
            self.assertIsNotNone(rpc)
            mock_post.assert_called_once_with(
                rpc.uri,
                data='{"jsonrpc": "1.0", "id": "btcmesh", "method": "getblockchaininfo", "params": []}',
                headers={'Content-Type': 'application/json'},
                proxies={},
                timeout=30
            )
            

    def test_non_int_port_invalid_config_raises(self):
        """Given invalid config, When connecting, Then error is raised."""
        from core.rpc_client import BitcoinRPCClient

        bad_config = self.valid_config.copy()
        bad_config["port"] = "notanint"

        # Assertions
        with self.assertRaises(Exception):
            # Call the method
            BitcoinRPCClient(bad_config)

    def test_no_port_invalid_config_raises(self):
        """Given invalid config, When connecting, Then error is raised."""
        from core.rpc_client import BitcoinRPCClient

        bad_config = self.valid_config.copy()
        bad_config["port"] = None

        # Assertions
        with self.assertRaises(Exception):
            # Call the method
            BitcoinRPCClient(bad_config)

    def test_no_host_invalid_config_raises(self):
        """Given invalid config, When connecting, Then error is raised."""
        from core.rpc_client import BitcoinRPCClient

        bad_config = self.valid_config.copy()
        bad_config["host"] = None

        # Assertions
        with self.assertRaises(Exception):
            # Call the method
            BitcoinRPCClient(bad_config)

    def test_rpc_request_retries_on_connection_error_three_times_last_success(self):
        """Given valid config but node unreachable, When connecting, retries twice, succeeds on third try."""
        with unittest.mock.patch("core.rpc_client.requests.post") as mock_post:
            from core.rpc_client import BitcoinRPCClient

            # Mock the response to raise ConnectionError for the first two calls and success on last
            mock_post.side_effect = [
                ConnectionError("Connection error"), 
                ConnectionError("Connection error"),
                MagicMock(json=MagicMock(return_value={"result": {"chain": "main"}, "error": None}))
            ]

            # Call the method
            rpc = BitcoinRPCClient(self.valid_config)

            # Assertions
            self.assertIsNotNone(rpc)
            self.assertEqual(mock_post.call_count, 3)  # Ensure post was called three times

    def test_rpc_request_retries_on_connection_error_second_success(self):
        """Given valid config but node unreachable on first try, retries and suceeds on second try."""
        with unittest.mock.patch("core.rpc_client.requests.post") as mock_post:
            from core.rpc_client import BitcoinRPCClient

            # Mock the response to raise ConnectionError for the first call and success on the second
            mock_post.side_effect = [
                ConnectionError("Connection error"), 
                MagicMock(json=MagicMock(return_value={"result": {"chain": "main"}, "error": None}))
            ]

            # Call the method
            rpc = BitcoinRPCClient(self.valid_config)

            # Assertions
            self.assertIsNotNone(rpc)
            self.assertEqual(mock_post.call_count, 2)  # Ensure post was called two times

    def test_rpc_request_retries_on_connection_error_three_times_failure(self):
        """Given valid config but node unreachable, When connecting, retries 3 times, fails."""
        with unittest.mock.patch("core.rpc_client.requests.post") as mock_post:
            from core.rpc_client import BitcoinRPCClient

            # Mock the response to raise ConnectionError
            mock_post.side_effect = ConnectionError("Connection error")

            # Assert that the ConnectionError is raised after 3 attempts
            with self.assertRaises(ConnectionError) as context:
                # Call the method
                rpc = BitcoinRPCClient(self.valid_config)

            # Assertions
            self.assertTrue("Connection error" in str(context.exception))
            self.assertEqual(mock_post.call_count, 3)  # Ensure post was called three times


class TestBitcoinRpcBroadcastStory43(unittest.TestCase):
    def setUp(self):
        self.config = {
            'user': 'testuser',
            'password': 'testpass',
            'host': 'localhost',
            'port': 8332
        }
        self.valid_tx_hex = "0100000001abcdef..."
        self.txid = "deadbeefcafebabe1234567890abcdef1234567890abcdef"

    def test_successful_broadcast_returns_txid(self):
        """Given valid hex and RPC connection, When broadcast, Then TXID is returned."""
        from core.rpc_client import BitcoinRPCClient

        # Mock connect to prevent actual connection, and requests.post for the broadcast call
        with unittest.mock.patch.object(BitcoinRPCClient, 'connect'), \
            unittest.mock.patch('requests.post') as mock_post:
            # Create client (connect is mocked so no actual connection)
            client = BitcoinRPCClient(self.config)

            # Simulate a successful response from the RPC server
            mock_post.return_value.json.return_value = {
                "result": self.txid,
                "error": None
            }

            # Call the method
            txid, error = client.broadcast_transaction(self.valid_tx_hex)

            # Assertions
            self.assertEqual(txid, self.txid, "TXID returned should match expected TXID.")
            self.assertIsNone(error, "There should be no error for valid transaction.")

    def test_rpc_error_returns_error_message(self):
        """Given valid hex but RPC error, When broadcast, Then error message is returned."""
        from core.rpc_client import BitcoinRPCClient

        with unittest.mock.patch.object(BitcoinRPCClient, 'connect'), \
            unittest.mock.patch('requests.post') as mock_post:
            client = BitcoinRPCClient(self.config)

            # Simulate an error response from the RPC server
            mock_post.return_value.json.return_value = {
                "result": None,
                "error": {"code": -26, "message": "txn-mempool-conflict"}
            }

            # Call the method
            txid, error = client.broadcast_transaction(self.valid_tx_hex)

            # Assertions
            self.assertIsNone(txid)
            self.assertIn("txn-mempool-conflict", error)

    def test_no_rpc_connection_returns_error(self):
        """Given connection failure during broadcast, Then txid=None and error message is returned."""
        from core.rpc_client import BitcoinRPCClient
        import requests

        with unittest.mock.patch.object(BitcoinRPCClient, 'connect'), \
            unittest.mock.patch('requests.post') as mock_post:
            client = BitcoinRPCClient(self.config)

            # Simulate connection failure when trying to broadcast
            mock_post.side_effect = requests.exceptions.ConnectionError("Connection refused")

            txid, error = client.broadcast_transaction(self.valid_tx_hex)

            self.assertIsNone(txid)
            self.assertIsNotNone(error)
            self.assertIn("Connection refused", error)


class TestReassemblyTimeoutConfigStory52(unittest.TestCase):
    def setUp(self):
        self.default_timeout = 30
        self.env_key = "REASSEMBLY_TIMEOUT_SECONDS"
        self.env = {
            "BITCOIN_RPC_HOST": "127.0.0.1",
            "BITCOIN_RPC_PORT": "8332",
            "BITCOIN_RPC_USER": "user",
            "BITCOIN_RPC_PASSWORD": "pass",
        }

    def test_timeout_loaded_from_env(self):
        from core.config_loader import load_reassembly_timeout

        env = self.env.copy()
        env[self.env_key] = "42"
        with unittest.mock.patch.dict("os.environ", env, clear=True):
            timeout, source = load_reassembly_timeout()
            self.assertEqual(timeout, 42)
            self.assertEqual(source, "env")

    def test_timeout_missing_uses_default(self):
        from core.config_loader import load_reassembly_timeout

        with unittest.mock.patch.dict("os.environ", self.env, clear=True):
            timeout, source = load_reassembly_timeout()
            self.assertEqual(timeout, self.default_timeout)
            self.assertEqual(source, "default")

    def test_timeout_invalid_uses_default_and_logs_warning(self):
        from core.config_loader import load_reassembly_timeout

        env = self.env.copy()
        env[self.env_key] = "notanint"
        with unittest.mock.patch.dict("os.environ", env, clear=True):
            with unittest.mock.patch("core.config_loader.server_logger") as mock_logger:
                timeout, source = load_reassembly_timeout()
                self.assertEqual(timeout, self.default_timeout)
                self.assertEqual(source, "default")
                mock_logger.warning.assert_any_call(
                    "Invalid REASSEMBLY_TIMEOUT_SECONDS value 'notanint'. Using default: 30s."
                )

    def test_timeout_zero_or_negative_uses_default_and_logs_warning(self):
        from core.config_loader import load_reassembly_timeout

        for bad_val in ["0", "-5"]:
            env = self.env.copy()
            env[self.env_key] = bad_val
            with unittest.mock.patch.dict("os.environ", env, clear=True):
                with unittest.mock.patch(
                    "core.config_loader.server_logger"
                ) as mock_logger:
                    timeout, source = load_reassembly_timeout()
                    self.assertEqual(timeout, self.default_timeout)
                    self.assertEqual(source, "default")
                    mock_logger.warning.assert_any_call(
                        f"Invalid REASSEMBLY_TIMEOUT_SECONDS value '{bad_val}'. Using default: 30s."
                    )


class TestMultipleConcurrentSessions(ServerHandlerTestCase):
    def test_multiple_sessions_are_independent(self):
        """The server tracks and reassembles multiple sessions independently."""
        client1 = "!00111111"
        client2 = "!00222222"
        session1 = "sessA"
        session2 = "sessB"
        chunk1a = f"{CHUNK_PREFIX}{session1}|1/2|AAA"
        chunk1b = f"{CHUNK_PREFIX}{session1}|2/2|BBB"
        chunk2a = f"{CHUNK_PREFIX}{session2}|1/2|XXX"
        chunk2b = f"{CHUNK_PREFIX}{session2}|2/2|YYY"
        self.mock_reassembler.add_chunk.side_effect = [None, None, "AAABBB", "XXXYYY"]
        self.mock_rpc.broadcast_transaction.return_value = ("txid", None)

        # Interleave chunks: client1 chunk1, client2 chunk1, client1 chunk2, client2 chunk2
        for sender, chunk in [
            (client1, chunk1a),
            (client2, chunk2a),
            (client1, chunk1b),
            (client2, chunk2b),
        ]:
            handle_text_message(self.transport, sender, chunk)

        self.mock_reassembler.add_chunk.assert_has_calls(
            [
                call(client1, chunk1a),
                call(client2, chunk2a),
                call(client1, chunk1b),
                call(client2, chunk2b),
            ]
        )
        self.mock_send_reply.assert_any_call(
            self.transport, client1, f"BTC_ACK|{session1}|SUCCESS|TXID:txid", session1
        )
        self.mock_send_reply.assert_any_call(
            self.transport, client2, f"BTC_ACK|{session2}|SUCCESS|TXID:txid", session2
        )


class TestAckNackAndErrorHandling(ServerHandlerTestCase):
    def test_ack_on_valid_chunk(self):
        session_id = "sess_ack"
        self.mock_reassembler.add_chunk.return_value = None

        handle_text_message(self.transport, SENDER, f"BTC_TX|{session_id}|1/2|deadbeef")

        self.mock_send_reply.assert_called_with(
            self.transport,
            SENDER,
            f"BTC_CHUNK_ACK|{session_id}|1|OK|REQUEST_CHUNK|2",
            session_id,
        )

    def test_nack_on_invalid_chunk(self):
        session_id = "sess_nack"
        self.mock_reassembler.add_chunk.side_effect = InvalidChunkFormatError(
            "bad format"
        )

        handle_text_message(self.transport, SENDER, f"BTC_TX|{session_id}|bad/format|badhex")

        self.mock_send_reply.assert_called_with(
            self.transport,
            SENDER,
            f"BTC_NACK|{session_id}|ERROR|Invalid ChunkFormat: bad format",
            session_id,
        )

    def test_duplicate_chunk_handling(self):
        session_id = "sess_dup"
        chunk_msg = f"BTC_TX|{session_id}|1/2|deadbeef"
        self.mock_reassembler.add_chunk.side_effect = [
            None,
            InvalidChunkFormatError("Duplicate chunk"),
        ]

        handle_text_message(self.transport, SENDER, chunk_msg)
        handle_text_message(self.transport, SENDER, chunk_msg)

        self.mock_send_reply.assert_called_with(
            self.transport,
            SENDER,
            f"BTC_NACK|{session_id}|ERROR|Invalid ChunkFormat: Duplicate chunk",
            session_id,
        )


if __name__ == "__main__":
    unittest.main()
