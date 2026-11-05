"""Tests for the MeshCore transport, against an in-memory fake of the meshcore
library's MeshCore object (each transport runs its real event loop thread)."""
import asyncio
import sys
import threading
import time
import types
import unittest
from unittest.mock import MagicMock, patch

try:
    from meshcore import EventType
except ImportError:  # pragma: no cover
    EventType = None

import btcmesh_cli
import btcmesh_server
from core.reassembler import TransactionReassembler
from core.transport import TransportError, get_transport_class
from core.transport.meshcore_transport import (
    MAX_TEXT_BYTES,
    MeshCoreTransport,
    parse_device_path,
)

RELAY_KEY = "aa11bb22cc33" + "0" * 52
CLIENT_KEY = "dd44ee55ff66" + "1" * 52


class FakeEvent:
    def __init__(self, type_, payload=None):
        self.type = type_
        self.payload = payload or {}


class FakeCommands:
    def __init__(self, mc):
        self.mc = mc
        self.sent = []
        self.adverts = []
        self.added = []
        self.fail_send = False

    async def send_msg(self, contact, text):
        self.sent.append((contact["public_key"], text))
        if self.fail_send:
            return FakeEvent(EventType.ERROR, {"reason": "radio busy"})
        peer = self.mc.network.get(contact["public_key"])
        if peer is not None and peer.transport is not None:
            peer.transport._on_contact_message(
                FakeEvent(
                    EventType.CONTACT_MSG_RECV,
                    {
                        "pubkey_prefix": self.mc.self_info["public_key"][:12],
                        "text": text,
                        "txt_type": 0,
                    },
                )
            )
        return FakeEvent(EventType.MSG_SENT, {"expected_ack": b"\x00" * 4})

    async def send_advert(self, flood=False):
        self.adverts.append(flood)
        return FakeEvent(EventType.OK)

    async def add_contact(self, contact):
        self.added.append(contact)
        self.mc.contacts[contact["public_key"]] = contact
        return FakeEvent(EventType.OK)


class FakeMeshCore:
    """Just enough of meshcore.MeshCore for MeshCoreTransport."""

    def __init__(self, key, name, network=None):
        self.self_info = {"public_key": key, "name": name}
        self.contacts = {}
        self.commands = FakeCommands(self)
        self.subscriptions = []
        self.auto_update_contacts = False
        self.ensure_calls = 0
        self.disconnected = False
        self.network = network if network is not None else {}
        self.network[key] = self
        self.transport = None
        self.on_ensure = None  # Optional hook to simulate contacts arriving

    def add_contact(self, key, name, last_advert=0):
        self.contacts[key] = {"public_key": key, "adv_name": name, "last_advert": last_advert}

    def get_contact_by_key_prefix(self, prefix):
        for key, contact in self.contacts.items():
            if key.startswith(prefix.lower()):
                return contact
        return None

    async def ensure_contacts(self, follow=False):
        self.ensure_calls += 1
        if self.on_ensure:
            self.on_ensure()
        return True

    def subscribe(self, event_type, callback, attribute_filters=None):
        sub = (event_type, callback)
        self.subscriptions.append(sub)
        return sub

    def unsubscribe(self, sub):
        self.subscriptions.remove(sub)

    async def start_auto_message_fetching(self):
        return None

    async def disconnect(self):
        self.disconnected = True


def start_loop():
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    return loop, thread


def make_transport(mc, start=True):
    loop, thread = start_loop()
    transport = MeshCoreTransport(mc, loop, loop_thread=thread, device_path="/dev/fake", logger=MagicMock())
    mc.transport = transport
    if start:
        asyncio.run_coroutine_threadsafe(transport._start(EventType, advert=True), loop).result(5)
    return transport


def wait_for(predicate, timeout=2.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


class TestParseDevicePath(unittest.TestCase):
    def test_serial(self):
        self.assertEqual(
            parse_device_path("/dev/ttyUSB0"),
            ("serial", {"port": "/dev/ttyUSB0", "baudrate": 115200}),
        )

    def test_tcp(self):
        self.assertEqual(
            parse_device_path("tcp:192.168.1.5:5000"),
            ("tcp", {"host": "192.168.1.5", "port": 5000}),
        )

    def test_bad_tcp(self):
        with self.assertRaises(TransportError):
            parse_device_path("tcp:nohostport")

    def test_ble(self):
        with patch.dict("os.environ", {"MESHCORE_BLE_PIN": ""}):
            self.assertEqual(parse_device_path("ble:AA:BB"), ("ble", {"address": "AA:BB"}))
            self.assertEqual(parse_device_path("ble:"), ("ble", {"address": None}))

    def test_ble_pin(self):
        with patch.dict("os.environ", {"MESHCORE_BLE_PIN": "123456"}):
            self.assertEqual(
                parse_device_path("ble:"), ("ble", {"address": None, "pin": "123456"})
            )

    def test_empty(self):
        with self.assertRaises(TransportError):
            parse_device_path(None)


class TestNodeIds(unittest.TestCase):
    def test_valid_ids(self):
        self.assertTrue(MeshCoreTransport.is_valid_node_id("aa11bb22cc33"))
        self.assertTrue(MeshCoreTransport.is_valid_node_id(RELAY_KEY))
        self.assertFalse(MeshCoreTransport.is_valid_node_id("aa11bb"))
        self.assertFalse(MeshCoreTransport.is_valid_node_id("!aa11bb22cc33"))
        self.assertFalse(MeshCoreTransport.is_valid_node_id(""))

    def test_registered(self):
        self.assertIs(get_transport_class("meshcore"), MeshCoreTransport)

    def test_chunk_message_fits(self):
        header = "BTC_TX|abcde|99/99|"
        self.assertLessEqual(len(header) + MeshCoreTransport.chunk_size, MAX_TEXT_BYTES)


@unittest.skipIf(EventType is None, "meshcore library not installed")
class TestMeshCoreTransport(unittest.TestCase):
    def setUp(self):
        self.mc = FakeMeshCore(RELAY_KEY, "relay")
        self.mc.add_contact(CLIENT_KEY, "client", last_advert=int(time.time()))
        self.transport = make_transport(self.mc)
        self.addCleanup(self.transport.close)

    def test_start_subscribes_fetches_and_adverts(self):
        types_ = [t for t, _ in self.mc.subscriptions]
        self.assertIn(EventType.CONTACT_MSG_RECV, types_)
        self.assertIn(EventType.NEW_CONTACT, types_)
        self.assertTrue(self.mc.auto_update_contacts)
        self.assertEqual(self.mc.commands.adverts, [True])

    def test_identity(self):
        self.assertEqual(self.transport.own_node_id(), "aa11bb22cc33")
        self.assertEqual(self.transport.own_node_name(), "relay")
        self.assertEqual(self.transport.device_path, "/dev/fake")

    def test_known_nodes(self):
        self.mc.add_contact(RELAY_KEY, "me")  # Own key is excluded
        nodes = self.transport.known_nodes()
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0]["id"], "dd44ee55ff66")
        self.assertEqual(nodes[0]["name"], "client")
        self.assertTrue(nodes[0]["is_recent"])

    def test_incoming_message_dispatched_off_loop(self):
        got = []
        self.transport.subscribe(lambda s, t: got.append((s, t, threading.current_thread().name)))
        self.transport._on_contact_message(
            FakeEvent(EventType.CONTACT_MSG_RECV, {"pubkey_prefix": "DD44EE55FF66", "text": "hi", "txt_type": 0})
        )
        self.assertTrue(wait_for(lambda: got))
        self.assertEqual(got[0][:2], ("dd44ee55ff66", "hi"))
        self.assertEqual(got[0][2], "meshcore-dispatch")

    def test_signed_and_empty_messages_ignored(self):
        handler = MagicMock()
        self.transport.subscribe(handler)
        self.transport._on_contact_message(
            FakeEvent(EventType.CONTACT_MSG_RECV, {"pubkey_prefix": "dd44ee55ff66", "text": "x", "txt_type": 2})
        )
        self.transport._on_contact_message(
            FakeEvent(EventType.CONTACT_MSG_RECV, {"pubkey_prefix": "dd44ee55ff66", "text": "", "txt_type": 0})
        )
        time.sleep(0.05)
        handler.assert_not_called()

    def test_send_text_to_contact(self):
        self.transport.send_text("dd44ee55ff66", "BTC_CHUNK_ACK|s|1|OK|ALL_CHUNKS_RECEIVED")
        self.assertEqual(
            self.mc.commands.sent, [(CLIENT_KEY, "BTC_CHUNK_ACK|s|1|OK|ALL_CHUNKS_RECEIVED")]
        )

    def test_send_text_refreshes_contacts_once_for_unknown_node(self):
        new_key = "123456abcdef" + "2" * 52
        self.mc.on_ensure = lambda: self.mc.add_contact(new_key, "late")
        calls_before = self.mc.ensure_calls
        self.transport.send_text("123456abcdef", "hello")
        self.assertEqual(self.mc.ensure_calls, calls_before + 1)
        self.assertEqual(self.mc.commands.sent[-1], (new_key, "hello"))

    def test_send_text_unknown_contact_raises(self):
        with self.assertRaises(TransportError) as cm:
            self.transport.send_text("0000000000ff", "hello")
        self.assertIn("not in this MeshCore device's contacts", str(cm.exception))

    def test_send_text_device_error_raises(self):
        self.mc.commands.fail_send = True
        with self.assertRaises(TransportError):
            self.transport.send_text("dd44ee55ff66", "hello")

    def test_send_text_too_long_raises(self):
        with self.assertRaises(TransportError):
            self.transport.send_text("dd44ee55ff66", "x" * (MAX_TEXT_BYTES + 1))
        self.assertEqual(self.mc.commands.sent, [])

    def test_new_contact_is_added(self):
        contact = {"public_key": "999999999999" + "3" * 52, "adv_name": "newbie"}
        asyncio.run_coroutine_threadsafe(
            self._call_on_loop(self.transport._on_new_contact, FakeEvent(EventType.NEW_CONTACT, contact)),
            self.transport._loop,
        ).result(2)
        self.assertTrue(wait_for(lambda: self.mc.commands.added == [contact]))

    def test_new_contact_not_added_when_disabled(self):
        self.transport.auto_add_contacts = False
        contact = {"public_key": "999999999999" + "3" * 52}
        asyncio.run_coroutine_threadsafe(
            self._call_on_loop(self.transport._on_new_contact, FakeEvent(EventType.NEW_CONTACT, contact)),
            self.transport._loop,
        ).result(2)
        time.sleep(0.05)
        self.assertEqual(self.mc.commands.added, [])

    @staticmethod
    async def _call_on_loop(fn, arg):
        fn(arg)

    def test_close_disconnects_and_is_idempotent(self):
        self.transport.close()
        self.transport.close()
        self.assertTrue(self.mc.disconnected)
        self.assertEqual(self.mc.subscriptions, [])
        with self.assertRaises(TransportError):
            self.transport.send_text("dd44ee55ff66", "late")


@unittest.skipIf(EventType is None, "meshcore library not installed")
class TestMeshCoreOpen(unittest.TestCase):
    def test_no_port_and_no_devices(self):
        with patch.object(MeshCoreTransport, "scan_devices", return_value=[]):
            with self.assertRaises(TransportError) as cm:
                MeshCoreTransport.open(port=None)
        self.assertIn("No MeshCore device found", str(cm.exception))

    def test_no_port_and_several_devices(self):
        with patch.object(MeshCoreTransport, "scan_devices", return_value=["/dev/a", "/dev/b"]):
            with self.assertRaises(TransportError) as cm:
                MeshCoreTransport.open(port=None)
        self.assertIn("Several serial devices", str(cm.exception))

    def test_open_serial(self):
        mc = FakeMeshCore(RELAY_KEY, "relay")

        async def create_serial(port, baudrate):
            self.assertEqual((port, baudrate), ("/dev/ttyACM0", 115200))
            return mc

        with patch("meshcore.MeshCore.create_serial", side_effect=create_serial):
            transport = MeshCoreTransport.open(port="/dev/ttyACM0")
        self.addCleanup(transport.close)
        self.assertEqual(transport.own_node_id(), "aa11bb22cc33")
        self.assertEqual(mc.commands.adverts, [True])

    def test_open_no_response(self):
        async def create_serial(port, baudrate):
            return None

        with patch("meshcore.MeshCore.create_serial", side_effect=create_serial):
            with self.assertRaises(TransportError) as cm:
                MeshCoreTransport.open(port="/dev/ttyACM0")
        self.assertIn("companion firmware", str(cm.exception))

    def test_open_without_advert(self):
        mc = FakeMeshCore(RELAY_KEY, "relay")

        async def create_serial(port, baudrate):
            return mc

        with patch("meshcore.MeshCore.create_serial", side_effect=create_serial), patch.dict(
            "os.environ", {"MESHCORE_ADVERT_ON_CONNECT": "false"}
        ):
            transport = MeshCoreTransport.open(port="/dev/ttyACM0")
        self.addCleanup(transport.close)
        self.assertEqual(mc.commands.adverts, [])


@unittest.skipIf(EventType is None, "meshcore library not installed")
class TestMeshCoreEndToEnd(unittest.TestCase):
    """Real CLI and server handler talking over two MeshCore transports."""

    def setUp(self):
        network = {}
        self.relay_mc = FakeMeshCore(RELAY_KEY, "relay", network)
        self.client_mc = FakeMeshCore(CLIENT_KEY, "client", network)
        self.relay_mc.add_contact(CLIENT_KEY, "client")
        self.client_mc.add_contact(RELAY_KEY, "relay")
        self.relay = make_transport(self.relay_mc)
        self.client = make_transport(self.client_mc)
        self.addCleanup(self.relay.close)
        self.addCleanup(self.client.close)
        self.relay.subscribe(
            lambda sender, text: btcmesh_server.handle_text_message(self.relay, sender, text)
        )

        self.rpc = MagicMock()
        for p in [
            patch.object(btcmesh_server, "transaction_reassembler", TransactionReassembler(timeout_seconds=30)),
            patch.object(btcmesh_server, "bitcoin_rpc", self.rpc),
            patch.object(btcmesh_server, "transaction_history", None),
            patch.object(btcmesh_server, "server_logger", MagicMock()),
            patch.object(btcmesh_cli, "ACK_TIMEOUT", 5),
            patch.object(btcmesh_cli, "RETRY_TIMEOUT", 0),
        ]:
            p.start()
            self.addCleanup(p.stop)

    def run_cli(self, tx_hex):
        args = types.SimpleNamespace(
            destination="AA11BB22CC33", tx=tx_hex, dry_run=False, session_id="abcde"
        )
        with patch("builtins.print"):
            return btcmesh_cli.cli_main(
                args=args, injected_transport=self.client, injected_logger=MagicMock()
            )

    def test_multi_chunk_broadcast(self):
        tx_hex = "ab" * 150  # 300 hex chars, 3 MeshCore chunks of 120
        self.rpc.broadcast_transaction.return_value = ("f00dtxid", None)

        self.assertEqual(self.run_cli(tx_hex), 0)

        self.rpc.broadcast_transaction.assert_called_once_with(tx_hex)
        chunks = [text for _, text in self.client_mc.commands.sent]
        self.assertEqual(len(chunks), 3)
        self.assertTrue(all(len(c.encode()) <= MAX_TEXT_BYTES for c in chunks))
        self.assertEqual(
            self.relay_mc.commands.sent[-1],
            (CLIENT_KEY, "BTC_ACK|abcde|SUCCESS|TXID:f00dtxid"),
        )

    def test_long_rpc_error_nack_fits_in_meshcore_message(self):
        self.rpc.broadcast_transaction.return_value = (None, "some unmapped error " * 20)

        self.assertEqual(self.run_cli("cd" * 20), 3)

        nack = self.relay_mc.commands.sent[-1][1]
        self.assertTrue(nack.startswith("BTC_NACK|abcde|ERROR|"))
        self.assertLessEqual(len(nack), MeshCoreTransport.max_text_len)


if __name__ == "__main__":
    unittest.main()
