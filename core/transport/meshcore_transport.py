"""
MeshCore implementation of the BTCMesh Transport interface.

Talks to a MeshCore *companion* radio (USB serial, TCP or BLE) through the
`meshcore` Python library. That library is asyncio based, so this transport
runs its own event loop on a background thread and exposes the blocking
interface the rest of BTCMesh expects.

Differences from Meshtastic that matter to BTCMesh:

* Node IDs are public key prefixes. MeshCore messages identify the sender by
  the first 6 bytes of its public key, so that 12 hex character prefix is the
  node ID (e.g. 'a1b2c3d4e5f6'). A longer prefix or the full 64 character key
  is accepted as a destination too.
* Direct messages only work between contacts. The relay must have the client
  as a contact (and vice versa) before they can exchange messages. This
  transport sends a flood advert on connect so peers learn about it, and by
  default adds newly heard nodes to the device's contacts.
* Text messages are smaller, so chunks are smaller (see chunk_size).

Device paths:
    /dev/ttyUSB0, COM3       USB serial companion
    tcp:192.168.1.50:5000    WiFi companion
    ble:AA:BB:CC:DD:EE:FF    Bluetooth companion (ble: alone scans for a device
                             named "MeshCore..."; on macOS the address is a UUID).
                             Set MESHCORE_BLE_PIN if the radio needs a pairing PIN.
"""
from __future__ import annotations

import asyncio
import logging
import os
import queue
import re
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from core.transport.base import Transport, TransportError

NODE_ID_LEN = 12  # hex chars, the 6 byte public key prefix MeshCore messages carry
_NODE_ID_RE = re.compile(r"[0-9a-fA-F]{12,64}")

# Plain text messages. 1 is a CLI command, 2 a signed message (prefixed with
# the signer's key, not plain protocol text).
TXT_TYPE_PLAIN = 0

# Largest text the firmware accepts in one direct message, in bytes.
MAX_TEXT_BYTES = 160

DEFAULT_BAUDRATE = 115200
CONNECT_TIMEOUT = 20.0
# BLE needs time to scan, connect and possibly pair.
BLE_CONNECT_TIMEOUT = 60.0
COMMAND_TIMEOUT = 15.0


def _env_flag(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None or val.strip() == "":
        return default
    return val.strip().lower() in ("1", "true", "yes", "on")


def parse_device_path(path: Optional[str]) -> Tuple[str, Dict[str, Any]]:
    """Split a device path into (connection type, kwargs for MeshCore.create_*).

    Raises TransportError if the path is malformed.
    """
    if not path:
        raise TransportError("No MeshCore device given and none found")
    if path.lower().startswith("tcp:"):
        host, sep, port = path[4:].rpartition(":")
        if not sep or not host or not port.isdigit():
            raise TransportError(f"Invalid MeshCore TCP address '{path}'. Use tcp:<host>:<port>")
        return "tcp", {"host": host, "port": int(port)}
    if path.lower().startswith("ble:"):
        kwargs: Dict[str, Any] = {"address": path[4:] or None}
        pin = os.getenv("MESHCORE_BLE_PIN", "").strip()
        if pin:
            kwargs["pin"] = pin
        return "ble", kwargs
    return "serial", {"port": path, "baudrate": DEFAULT_BAUDRATE}


def _stop_loop(loop: asyncio.AbstractEventLoop, timeout: float = 5.0) -> None:
    """Cancel the loop's remaining tasks, then stop it (avoids 'Task was
    destroyed but it is pending' noise from the serial/BLE libraries)."""

    async def cancel_all():
        current = asyncio.current_task()
        tasks = [t for t in asyncio.all_tasks() if t is not current]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    if not loop.is_running():
        return
    try:
        asyncio.run_coroutine_threadsafe(cancel_all(), loop).result(timeout)
    except Exception:
        pass
    loop.call_soon_threadsafe(loop.stop)


class MeshCoreTransport(Transport):
    """Transport backed by a MeshCore companion radio."""

    name = "MeshCore"
    # MeshCore text messages carry at most ~160 bytes. 120 hex chars plus the
    # ~20 char BTC_TX header leaves some margin.
    chunk_size = 120
    max_text_len = 150
    node_id_hint = "public key prefix (12 hex)"
    invalid_node_id_message = "Destination must be a MeshCore public key prefix (12+ hex chars)"

    def __init__(
        self,
        mc: Any,
        loop: asyncio.AbstractEventLoop,
        loop_thread: Optional[threading.Thread] = None,
        device_path: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
        auto_add_contacts: bool = True,
    ) -> None:
        super().__init__()
        self.mc = mc
        self._loop = loop
        self._loop_thread = loop_thread
        self._device_path = device_path
        self.logger = logger or logging.getLogger("btcmesh.transport.meshcore")
        self.auto_add_contacts = auto_add_contacts
        self._subscriptions: List[Any] = []
        self._pending_tasks: set = set()
        self._closed = False

        # Handlers run on their own thread, never on the event loop: the
        # server's handler sends replies (which need the loop) and talks to
        # Bitcoin RPC (which blocks).
        self._inbox: "queue.Queue[Optional[Tuple[str, str]]]" = queue.Queue()
        self._worker = threading.Thread(
            target=self._deliver_messages, name="meshcore-dispatch", daemon=True
        )
        self._worker.start()

    # --- Construction ----------------------------------------------------

    @classmethod
    def open(
        cls, port: Optional[str] = None, logger: Optional[logging.Logger] = None
    ) -> "MeshCoreTransport":
        """Connect to a MeshCore companion radio. Raises on failure.

        Args:
            port: Device path (see module docstring). None picks the only
                USB serial port present, if there is exactly one.
        """
        from meshcore import EventType, MeshCore

        log = logger or logging.getLogger("btcmesh.transport.meshcore")
        if port is None:
            candidates = cls.scan_devices()
            if len(candidates) != 1:
                raise TransportError(
                    "No MeshCore device found. Ensure it is connected, or set MESHCORE_SERIAL_PORT."
                    if not candidates
                    else f"Several serial devices found ({', '.join(candidates)}). "
                    "Choose one with MESHCORE_SERIAL_PORT or --port."
                )
            port = candidates[0]
        kind, kwargs = parse_device_path(port)

        loop = asyncio.new_event_loop()
        loop_thread = threading.Thread(
            target=loop.run_forever, name="meshcore-loop", daemon=True
        )
        loop_thread.start()

        connect_timeout = BLE_CONNECT_TIMEOUT if kind == "ble" else CONNECT_TIMEOUT

        def run(coro, timeout=connect_timeout):
            return asyncio.run_coroutine_threadsafe(coro, loop).result(timeout)

        mc = None
        transport = None
        try:
            factory = {
                "serial": MeshCore.create_serial,
                "tcp": MeshCore.create_tcp,
                "ble": MeshCore.create_ble,
            }[kind]
            mc = run(factory(**kwargs))
            if mc is None:
                hint = (
                    "Is it powered on, in range, running BLE companion firmware and not "
                    "connected to another app (e.g. the phone app)?"
                    if kind == "ble"
                    else "Is it running MeshCore USB companion firmware? (The BLE "
                    "companion build does not answer over USB serial; use ble: instead.)"
                )
                raise TransportError(
                    f"No response from MeshCore device on {port}. {hint}"
                )
            transport = cls(
                mc,
                loop,
                loop_thread=loop_thread,
                device_path=port,
                logger=log,
                auto_add_contacts=_env_flag("MESHCORE_AUTO_ADD_CONTACTS", True),
            )
            if not transport.own_node_id():
                raise TransportError(
                    "MeshCore interface created but could not retrieve device info. "
                    "Please ensure your device is plugged in and running companion firmware."
                )
            run(transport._start(EventType, advert=_env_flag("MESHCORE_ADVERT_ON_CONNECT", True)))
            return transport
        except BaseException:
            if transport is not None:
                transport._inbox.put(None)  # Stop the dispatch thread
            if mc is not None:
                try:
                    run(mc.disconnect(), timeout=5)
                except Exception:
                    pass
            _stop_loop(loop)
            raise

    async def _start(self, EventType: Any, advert: bool) -> None:
        """Runs on the event loop: load contacts and start receiving."""
        self._subscriptions.append(
            self.mc.subscribe(EventType.CONTACT_MSG_RECV, self._on_contact_message)
        )
        self._subscriptions.append(
            self.mc.subscribe(EventType.NEW_CONTACT, self._on_new_contact)
        )
        await self.mc.ensure_contacts()
        # Keep the contact cache fresh as adverts and path updates arrive.
        self.mc.auto_update_contacts = True
        await self.mc.start_auto_message_fetching()
        if advert:
            # Let other nodes (the relay, or clients) learn our public key so
            # they can add us as a contact.
            result = await self.mc.commands.send_advert(flood=True)
            if result is not None and result.type == EventType.ERROR:
                self.logger.warning(f"MeshCore advert failed: {result.payload}")

    @staticmethod
    def scan_devices() -> List[str]:
        """USB serial ports. MeshCore radios can't be told apart from other
        serial devices without connecting, so this lists all USB ports."""
        try:
            from serial.tools import list_ports
        except ImportError:
            return []
        try:
            return sorted(p.device for p in list_ports.comports() if p.vid is not None)
        except Exception:
            return []

    @staticmethod
    def describe_connect_error(error: Exception) -> str:
        if isinstance(error, TransportError):
            return str(error)
        if isinstance(error, (TimeoutError, asyncio.TimeoutError)):
            return "Timed out connecting to MeshCore device. Is it running companion firmware?"
        msg = str(error)
        if "could not open port" in msg.lower() or "No such file" in msg:
            return f"No MeshCore device found: {msg}"
        return msg

    # --- Event loop helpers ----------------------------------------------

    def _run(self, coro, timeout: float = COMMAND_TIMEOUT):
        if self._closed or threading.current_thread() is self._loop_thread:
            coro.close()
            if self._closed:
                raise TransportError("MeshCore transport is closed")
            raise RuntimeError("MeshCoreTransport blocking call made from its own event loop")
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result(timeout)

    # --- Receiving -------------------------------------------------------

    def _on_contact_message(self, event: Any) -> None:
        """Event loop callback for incoming direct messages."""
        payload = event.payload or {}
        sender = payload.get("pubkey_prefix")
        text = payload.get("text")
        txt_type = payload.get("txt_type", TXT_TYPE_PLAIN)
        if txt_type != TXT_TYPE_PLAIN:
            self.logger.debug(f"Ignoring MeshCore message of txt_type {txt_type} from {sender}")
            return
        if not sender or not text:
            self.logger.debug(f"Ignoring MeshCore message without sender or text: {payload}")
            return
        self._inbox.put((sender.lower(), text))

    def _on_new_contact(self, event: Any) -> None:
        """Event loop callback: a node we don't have as a contact advertised.
        Fires only when the device is set to add contacts manually."""
        contact = event.payload or {}
        if not self.auto_add_contacts or not contact.get("public_key"):
            return
        name = contact.get("adv_name") or contact["public_key"][:NODE_ID_LEN]
        self.logger.info(f"Adding MeshCore contact {name} ({contact['public_key'][:NODE_ID_LEN]})")

        async def add():
            try:
                await self.mc.commands.add_contact(contact)
                await self.mc.ensure_contacts(follow=True)
            except Exception as e:
                self.logger.warning(f"Could not add MeshCore contact {name}: {e}")

        task = asyncio.ensure_future(add())
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)

    def _deliver_messages(self) -> None:
        while True:
            item = self._inbox.get()
            if item is None:
                return
            try:
                self._dispatch(*item)
            except Exception as e:
                self.logger.error(f"Error processing received message: {e}", exc_info=True)

    # --- Sending and identity --------------------------------------------

    def _find_contact(self, node_id: str) -> Optional[Dict[str, Any]]:
        return self.mc.get_contact_by_key_prefix(node_id.strip().lower())

    def send_text(self, destination_id: str, text: str) -> None:
        if not self.is_valid_node_id(destination_id):
            raise TransportError(f"Invalid MeshCore destination '{destination_id}'")
        if len(text.encode("utf-8")) > MAX_TEXT_BYTES:
            # The radio would truncate it, corrupting the chunk.
            raise TransportError(
                f"Message of {len(text.encode('utf-8'))} bytes is too long for MeshCore "
                f"(max {MAX_TEXT_BYTES})"
            )
        self._run(self._send_text(destination_id, text))

    async def _send_text(self, destination_id: str, text: str) -> None:
        from meshcore import EventType

        contact = self._find_contact(destination_id)
        if contact is None:
            # Maybe we heard its advert after the last refresh.
            await self.mc.ensure_contacts(follow=True)
            contact = self._find_contact(destination_id)
        if contact is None:
            raise TransportError(
                f"{destination_id} is not in this MeshCore device's contacts. "
                "Wait for its advert, or add it with the MeshCore app."
            )
        result = await self.mc.commands.send_msg(contact, text)
        if result is None or result.type == EventType.ERROR:
            detail = result.payload if result is not None else "no response"
            raise TransportError(f"MeshCore device rejected message: {detail}")

    def own_node_id(self) -> Optional[str]:
        key = (self.mc.self_info or {}).get("public_key")
        return key[:NODE_ID_LEN].lower() if key else None

    def own_node_name(self) -> Optional[str]:
        return (self.mc.self_info or {}).get("name") or None

    def known_nodes(self, exclude_own: bool = True) -> List[Dict]:
        own = self.own_node_id()
        now = int(time.time())
        nodes = []
        for contact in list((self.mc.contacts or {}).values()):
            key = (contact.get("public_key") or "").lower()
            if not key:
                continue
            node_id = key[:NODE_ID_LEN]
            if exclude_own and node_id == own:
                continue
            last_heard = int(contact.get("last_advert") or 0)
            nodes.append(
                {
                    "id": node_id,
                    "name": contact.get("adv_name") or node_id,
                    "lastHeard": last_heard,
                    "is_recent": bool(last_heard) and now - last_heard < 24 * 60 * 60,
                }
            )
        nodes.sort(key=lambda n: n["lastHeard"], reverse=True)
        return nodes

    @property
    def device_path(self) -> Optional[str]:
        return self._device_path

    @staticmethod
    def is_valid_node_id(node_id: str) -> bool:
        return bool(node_id) and _NODE_ID_RE.fullmatch(node_id.strip()) is not None

    def normalize_node_id(self, node_id: str) -> str:
        return node_id.strip().lower()[:NODE_ID_LEN]

    def close(self) -> None:
        if self._closed:
            return
        try:
            if self._loop.is_running():
                self._run(self._shutdown(), timeout=10)
        except Exception as e:
            self.logger.debug(f"Error closing MeshCore connection: {e}")
        finally:
            self._closed = True
            self._inbox.put(None)
            if self._loop.is_running():
                _stop_loop(self._loop)

    async def _shutdown(self) -> None:
        for sub in self._subscriptions:
            self.mc.unsubscribe(sub)
        self._subscriptions.clear()
        await self.mc.disconnect()
