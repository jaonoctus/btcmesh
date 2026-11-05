"""
Radio transport abstraction for BTCMesh.

The BTCMesh protocol (see project/protocol_spec.md) only needs three things from
the radio underneath it: send a direct text message to a node, receive direct
text messages addressed to us, and know who we are. Everything specific to a
mesh firmware (Meshtastic, MeshCore, ...) lives behind the Transport interface
defined here, so the server, CLI and GUIs never touch a radio library directly.
"""
from __future__ import annotations

import logging
import threading
from abc import ABC, abstractmethod
from typing import Callable, Dict, List, Optional

# Called with (sender_id, text) for every direct text message addressed to us.
# sender_id is in the transport's own node ID format and can be passed back to
# Transport.send_text() to reply.
TextHandler = Callable[[str, str], None]


class TransportError(Exception):
    """Raised when a transport cannot connect or send."""


class Transport(ABC):
    """A connected radio that can exchange direct text messages."""

    #: Human readable name of the radio firmware, used in logs and GUI labels.
    name: str = "Radio"

    #: Maximum number of hex characters of transaction payload per BTC_TX chunk.
    #: Chosen so the full chunk message (header included) fits in one packet.
    chunk_size: int = 170

    #: Longest message (in characters) this transport should be asked to send.
    #: Replies such as NACKs are truncated to fit.
    max_text_len: int = 200

    #: Short hint shown to users describing what a node ID looks like.
    node_id_hint: str = "node id"

    #: Error shown when a destination fails is_valid_node_id().
    invalid_node_id_message: str = "Invalid destination node ID"

    def __init__(self) -> None:
        self._handlers: List[TextHandler] = []
        self._handlers_lock = threading.Lock()

    # --- Construction ----------------------------------------------------

    @classmethod
    @abstractmethod
    def open(
        cls, port: Optional[str] = None, logger: Optional[logging.Logger] = None
    ) -> "Transport":
        """Connect to a device and return a ready transport. Raises on failure."""

    @staticmethod
    def scan_devices() -> List[str]:
        """Device paths that look like radios of this kind."""
        return []

    @staticmethod
    def describe_connect_error(error: Exception) -> str:
        """Short, user facing description of an exception raised by open()."""
        return str(error)

    # --- Receiving -------------------------------------------------------

    def subscribe(self, handler: TextHandler) -> None:
        """Register a handler for incoming direct text messages."""
        with self._handlers_lock:
            if handler not in self._handlers:
                self._handlers.append(handler)

    def unsubscribe(self, handler: TextHandler) -> None:
        """Remove a previously registered handler. Unknown handlers are ignored."""
        with self._handlers_lock:
            if handler in self._handlers:
                self._handlers.remove(handler)

    def _dispatch(self, sender_id: str, text: str) -> None:
        """Deliver an incoming message to every subscribed handler."""
        with self._handlers_lock:
            handlers = list(self._handlers)
        for handler in handlers:
            handler(sender_id, text)

    # --- Sending and identity --------------------------------------------

    @abstractmethod
    def send_text(self, destination_id: str, text: str) -> None:
        """Send a direct text message. Raises TransportError (or the radio
        library's own exception) on failure."""

    @abstractmethod
    def own_node_id(self) -> Optional[str]:
        """ID of the connected device, or None if unknown."""

    def own_node_name(self) -> Optional[str]:
        """Human readable name of the connected device, if it has one."""
        return None

    def known_nodes(self, exclude_own: bool = True) -> List[Dict]:
        """Nodes this device knows about.

        Returns a list of dicts with keys: id, name, lastHeard, is_recent.
        Sorted most recently heard first.
        """
        return []

    @property
    def device_path(self) -> Optional[str]:
        """Path or address of the connected device, if known."""
        return None

    @staticmethod
    @abstractmethod
    def is_valid_node_id(node_id: str) -> bool:
        """Whether node_id looks like a destination this transport can address."""

    def normalize_node_id(self, node_id: str) -> str:
        """Canonical form of node_id, used to compare IDs typed by users with
        IDs reported by the radio."""
        return node_id.strip().lower()

    def same_node(self, a: Optional[str], b: Optional[str]) -> bool:
        """Whether two node IDs refer to the same node."""
        if not a or not b:
            return False
        return self.normalize_node_id(a) == self.normalize_node_id(b)

    @abstractmethod
    def close(self) -> None:
        """Disconnect from the device and release resources."""


def format_node_display(node: Dict) -> str:
    """Format a node dict (from Transport.known_nodes) for a dropdown or list."""
    return f"{node['name']} ({node['id']})"
