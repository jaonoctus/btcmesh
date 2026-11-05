"""
Meshtastic implementation of the BTCMesh Transport interface.

Wraps meshtastic.serial_interface.SerialInterface. Incoming packets arrive via
the global pypubsub topic "meshtastic.receive"; this module filters them down
to direct text messages addressed to the connected node and hands them to
subscribers as (sender_id, text).

Node IDs use Meshtastic's canonical form: '!' followed by 8 lowercase hex
digits (e.g. '!a1b2c3d4').
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from core import meshtastic_utils
from core.transport.base import Transport, TransportError

RECEIVE_TOPIC = "meshtastic.receive"


def format_node_id(node_id_val: Any) -> Optional[str]:
    """Format a Meshtastic node number or ID string as '!xxxxxxxx'.

    Accepts an int node number, a '!hex' string, or a bare hex string.
    Returns None if the value cannot be interpreted as a node ID.
    """
    if isinstance(node_id_val, bool):
        return None
    if isinstance(node_id_val, int):
        return f"!{node_id_val:08x}"
    if isinstance(node_id_val, str) and node_id_val:
        raw = node_id_val[1:] if node_id_val.startswith("!") else node_id_val
        try:
            return f"!{int(raw, 16):08x}"
        except ValueError:
            return None
    return None


def parse_text_packet(
    packet: Dict[str, Any],
    my_node_num: Optional[int],
    logger: logging.Logger,
) -> Optional[Tuple[str, str]]:
    """Reduce a raw Meshtastic packet to (sender_id, text).

    Returns None (after logging why at debug level) unless the packet is a
    TEXT_MESSAGE_APP direct message addressed to my_node_num from another node.
    """
    if not packet:
        return None

    decoded = packet.get("decoded")
    if not decoded:
        logger.debug(
            f"Received packet without 'decoded' content: {packet.get('id', 'N/A')}"
        )
        return None

    my_node_id = format_node_id(my_node_num)
    if not my_node_id:
        logger.warning("Could not determine or format my node ID from interface.")
        return None

    sender_id = format_node_id(
        packet.get("from") if packet.get("from") is not None else packet.get("fromId")
    )

    if sender_id == my_node_id:
        text = decoded.get("text") or ""
        preview = text[:30] + ("..." if len(text) > 30 else "")
        logger.debug(
            f"Ignoring DM from self. From: {my_node_id}, To: {my_node_id}, "
            f"Text: '{preview or '(No text field)'}'"
        )
        return None

    dest_val = packet.get("to")
    if dest_val is None:
        dest_val = packet.get("toId")
    destination_id = format_node_id(dest_val)

    if destination_id != my_node_id:
        # Broadcasts and messages for other nodes are not ours to process.
        if dest_val is not None:
            logger.debug(
                f"Received message not for this node. To: {destination_id or dest_val}, "
                f"MyID: {my_node_id}, From: {sender_id or 'UnknownSender'}"
            )
        return None

    if not sender_id:
        logger.warning(
            f"Could not determine a valid sender node ID from packet: {packet}. Ignoring DM."
        )
        return None

    portnum = str(decoded.get("portnum"))
    if portnum != "TEXT_MESSAGE_APP":
        logger.debug(
            f"Received direct message, but not TEXT_MESSAGE_APP. "
            f"Portnum: {portnum}, From: {sender_id}, ID: {packet.get('id', 'N/A')}"
        )
        return None

    text = decoded.get("text")
    if not text and isinstance(decoded.get("payload"), (bytes, bytearray)):
        try:
            text = decoded["payload"].decode("utf-8")
        except UnicodeDecodeError as e:
            logger.error(f"Failed to decode payload as UTF-8: {e}")
            text = None
    if not text:
        logger.debug(
            f"Direct text message with no text from {sender_id}: {packet.get('id', 'N/A')}"
        )
        return None

    return sender_id, text


class MeshtasticTransport(Transport):
    """Transport backed by a Meshtastic serial device."""

    name = "Meshtastic"
    # 170 hex chars + ~20 chars of BTC_TX header stays well under the
    # ~228 byte Meshtastic text payload limit.
    chunk_size = 170
    node_id_hint = "!node_id"
    invalid_node_id_message = "Destination must start with '!'"

    def __init__(self, iface: Any, logger: Optional[logging.Logger] = None) -> None:
        super().__init__()
        self.iface = iface
        self.logger = logger or logging.getLogger("btcmesh.transport.meshtastic")
        self._listening = False

    # --- Construction ----------------------------------------------------

    @classmethod
    def open(
        cls, port: Optional[str] = None, logger: Optional[logging.Logger] = None
    ) -> "MeshtasticTransport":
        """Connect to a Meshtastic device. Raises on failure.

        Args:
            port: Serial device path. None lets the library auto-detect.
        """
        import meshtastic.serial_interface

        iface = (
            meshtastic.serial_interface.SerialInterface(devPath=port)
            if port
            else meshtastic.serial_interface.SerialInterface()
        )
        transport = cls(iface, logger=logger)
        if not transport.own_node_id():
            try:
                iface.close()
            except Exception:
                pass
            raise TransportError(
                "Meshtastic interface created but could not retrieve device info. "
                "This usually means no Meshtastic device is connected. "
                "Please ensure your device is plugged in and recognized by the system."
            )
        return transport

    @staticmethod
    def scan_devices() -> List[str]:
        """Serial ports that look like Meshtastic devices."""
        return meshtastic_utils.scan_meshtastic_devices()

    @staticmethod
    def describe_connect_error(error: Exception) -> str:
        """Turn a connection exception into a short message for logs and GUIs."""
        err_type = type(error).__name__
        msg = str(error)
        if isinstance(error, TransportError):
            return msg
        if err_type == "NoDeviceError" or "No Meshtastic" in msg or "No serial" in msg:
            return "No Meshtastic device found. Ensure it is connected and drivers are installed."
        if err_type == "MeshtasticError" or "Meshtastic error" in msg:
            return f"Meshtastic library error during initialization: {msg}"
        return msg

    # --- Receiving -------------------------------------------------------

    def subscribe(self, handler) -> None:
        super().subscribe(handler)
        if not self._listening:
            from pubsub import pub

            pub.subscribe(self._on_packet, RECEIVE_TOPIC)
            self._listening = True

    def unsubscribe(self, handler) -> None:
        super().unsubscribe(handler)
        if self._listening and not self._handlers:
            self._stop_listening()

    def _stop_listening(self) -> None:
        try:
            from pubsub import pub

            pub.unsubscribe(self._on_packet, RECEIVE_TOPIC)
        except Exception as e:  # pragma: no cover - best effort cleanup
            self.logger.debug(f"Error unsubscribing from {RECEIVE_TOPIC}: {e}")
        self._listening = False

    def _on_packet(self, packet: Dict[str, Any], interface: Any = None) -> None:
        """pubsub listener. The topic is global, so ignore packets that came in
        on a different interface than ours."""
        if interface is not None and interface is not self.iface:
            return
        try:
            parsed = parse_text_packet(packet, self._my_node_num(), self.logger)
            if parsed:
                self._dispatch(*parsed)
        except Exception as e:
            self.logger.error(f"Error processing received packet: {e}", exc_info=True)

    # --- Sending and identity --------------------------------------------

    def send_text(self, destination_id: str, text: str) -> None:
        self.iface.sendText(text=text, destinationId=destination_id, wantAck=False)

    def _my_node_num(self) -> Optional[int]:
        my_info = getattr(self.iface, "myInfo", None)
        return getattr(my_info, "my_node_num", None) if my_info else None

    def own_node_id(self) -> Optional[str]:
        return format_node_id(self._my_node_num())

    def own_node_name(self) -> Optional[str]:
        return meshtastic_utils.get_own_node_name(self.iface)

    def known_nodes(self, exclude_own: bool = True) -> List[Dict]:
        return meshtastic_utils.get_known_nodes(self.iface, exclude_own=exclude_own)

    @property
    def device_path(self) -> Optional[str]:
        raw = getattr(self.iface, "devicePath", None) or getattr(self.iface, "devPath", None)
        return str(raw) if raw not in (None, "", "?", "None") else None

    @staticmethod
    def is_valid_node_id(node_id: str) -> bool:
        return bool(node_id) and node_id.startswith("!") and format_node_id(node_id) is not None

    def normalize_node_id(self, node_id: str) -> str:
        return format_node_id(node_id.strip()) or node_id.strip().lower()

    def close(self) -> None:
        if self._listening:
            self._stop_listening()
        if self.iface is not None and hasattr(self.iface, "close"):
            self.iface.close()
