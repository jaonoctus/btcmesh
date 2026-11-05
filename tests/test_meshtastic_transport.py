"""Tests for core.transport and its Meshtastic implementation."""
import sys
import unittest
from unittest.mock import MagicMock, call, patch

from core.reassembler import CHUNK_PREFIX
from core.transport import (
    TransportError,
    available_transports,
    connect_transport,
    get_transport_class,
)
from core.transport.base import Transport
from core.transport.meshtastic_transport import (
    MeshtasticTransport,
    format_node_id,
    parse_text_packet,
)

MY_NODE_NUM = 0xABCDEF
MY_ID = "!00abcdef"
SENDER_NUM = 0xFEEDFACE
SENDER_ID = "!feedface"


def text_packet(text, to=MY_ID, sender=SENDER_NUM, portnum="TEXT_MESSAGE_APP", **extra):
    packet = {
        "from": sender,
        "toId": to,
        "decoded": {"portnum": portnum, "text": text},
        "id": "packet_id",
    }
    packet.update(extra)
    return packet


class TestFormatNodeId(unittest.TestCase):
    def test_int_is_zero_padded(self):
        self.assertEqual(format_node_id(0x12345), "!00012345")

    def test_bang_string_is_normalized(self):
        self.assertEqual(format_node_id("!ABCDEF12"), "!abcdef12")
        self.assertEqual(format_node_id("!abc"), "!00000abc")

    def test_bare_hex_string(self):
        self.assertEqual(format_node_id("abcdef12"), "!abcdef12")

    def test_invalid_values(self):
        self.assertIsNone(format_node_id(None))
        self.assertIsNone(format_node_id(""))
        self.assertIsNone(format_node_id("!notHex"))
        self.assertIsNone(format_node_id(True))


class TestParseTextPacket(unittest.TestCase):
    def setUp(self):
        self.logger = MagicMock()

    def parse(self, packet):
        return parse_text_packet(packet, MY_NODE_NUM, self.logger)

    def test_direct_text_message(self):
        self.assertEqual(self.parse(text_packet("hello")), (SENDER_ID, "hello"))

    def test_uses_to_before_to_id(self):
        packet = text_packet("hi", to="!ffffffff")
        packet["to"] = MY_NODE_NUM
        self.assertEqual(self.parse(packet), (SENDER_ID, "hi"))

    def test_falls_back_to_from_id(self):
        packet = text_packet("hi")
        del packet["from"]
        packet["fromId"] = SENDER_ID
        self.assertEqual(self.parse(packet), (SENDER_ID, "hi"))

    def test_payload_bytes_when_text_missing(self):
        packet = text_packet(None)
        packet["decoded"]["payload"] = b"from payload"
        self.assertEqual(self.parse(packet), (SENDER_ID, "from payload"))

    def test_message_not_for_this_node(self):
        other = "!feedbeef"
        self.assertIsNone(self.parse(text_packet(f"{CHUNK_PREFIX}s|1/1|aa", to=other)))
        self.logger.debug.assert_any_call(
            f"Received message not for this node. To: {other}, MyID: {MY_ID}, From: {SENDER_ID}"
        )

    def test_broadcast_ignored(self):
        self.assertIsNone(self.parse(text_packet("broadcast", to="!ffffffff")))
        self.logger.debug.assert_any_call(
            f"Received message not for this node. To: !ffffffff, MyID: {MY_ID}, From: {SENDER_ID}"
        )

    def test_non_text_message(self):
        self.assertIsNone(self.parse(text_packet(None, portnum="POSITION_APP")))
        self.logger.debug.assert_any_call(
            f"Received direct message, but not TEXT_MESSAGE_APP. "
            f"Portnum: POSITION_APP, From: {SENDER_ID}, ID: packet_id"
        )

    def test_packet_without_decoded(self):
        packet = text_packet("x")
        del packet["decoded"]
        self.assertIsNone(self.parse(packet))
        self.logger.debug.assert_any_call(
            "Received packet without 'decoded' content: packet_id"
        )

    def test_missing_none_or_empty_text(self):
        for text in (None, ""):
            with self.subTest(text=text):
                self.assertIsNone(self.parse(text_packet(text)))
        packet = text_packet("x")
        del packet["decoded"]["text"]
        self.assertIsNone(self.parse(packet))
        self.logger.debug.assert_any_call(
            f"Direct text message with no text from {SENDER_ID}: packet_id"
        )

    def test_message_from_self_ignored(self):
        text = f"{CHUNK_PREFIX}self_sess|1/1|payload_from_self"
        self.assertIsNone(self.parse(text_packet(text, sender=MY_NODE_NUM)))
        self.logger.debug.assert_any_call(
            f"Ignoring DM from self. From: {MY_ID}, To: {MY_ID}, Text: '{text[:30]}...'"
        )

    def test_unknown_own_node(self):
        self.assertIsNone(parse_text_packet(text_packet("x"), None, self.logger))
        self.logger.warning.assert_called_once()

    def test_empty_packet(self):
        self.assertIsNone(self.parse(None))
        self.assertIsNone(self.parse({}))


class TestMeshtasticTransport(unittest.TestCase):
    def setUp(self):
        self.iface = MagicMock()
        self.iface.myInfo.my_node_num = MY_NODE_NUM
        self.iface.devicePath = "/dev/ttyUSB0"
        self.transport = MeshtasticTransport(self.iface, logger=MagicMock())
        self.mock_pub = MagicMock()
        self.pubsub_patcher = patch.dict(sys.modules, {"pubsub": MagicMock(pub=self.mock_pub)})
        self.pubsub_patcher.start()

    def tearDown(self):
        self.pubsub_patcher.stop()

    def test_is_a_transport(self):
        self.assertIsInstance(self.transport, Transport)
        self.assertEqual(self.transport.name, "Meshtastic")

    def test_send_text(self):
        self.transport.send_text("!abcdef12", "BTC_TX|s|1/1|aa")
        self.iface.sendText.assert_called_once_with(
            text="BTC_TX|s|1/1|aa", destinationId="!abcdef12", wantAck=False
        )

    def test_identity(self):
        self.assertEqual(self.transport.own_node_id(), MY_ID)
        self.assertEqual(self.transport.device_path, "/dev/ttyUSB0")

    def test_node_id_validation(self):
        self.assertTrue(MeshtasticTransport.is_valid_node_id("!abcdef12"))
        self.assertFalse(MeshtasticTransport.is_valid_node_id("abcdef12"))
        self.assertFalse(MeshtasticTransport.is_valid_node_id("!zzz"))
        self.assertFalse(MeshtasticTransport.is_valid_node_id(""))

    def test_same_node_ignores_padding_and_case(self):
        self.assertTrue(self.transport.same_node("!ABC", "!00000abc"))
        self.assertFalse(self.transport.same_node("!abc", "!abd"))
        self.assertFalse(self.transport.same_node(None, "!abc"))

    def test_subscribe_registers_pubsub_listener_once(self):
        h1, h2 = MagicMock(), MagicMock()
        self.transport.subscribe(h1)
        self.transport.subscribe(h2)
        self.mock_pub.subscribe.assert_called_once_with(
            self.transport._on_packet, "meshtastic.receive"
        )

    def test_unsubscribe_last_handler_removes_pubsub_listener(self):
        h1, h2 = MagicMock(), MagicMock()
        self.transport.subscribe(h1)
        self.transport.subscribe(h2)
        self.transport.unsubscribe(h1)
        self.mock_pub.unsubscribe.assert_not_called()
        self.transport.unsubscribe(h2)
        self.mock_pub.unsubscribe.assert_called_once_with(
            self.transport._on_packet, "meshtastic.receive"
        )

    def test_packets_are_dispatched_to_handlers(self):
        handler = MagicMock()
        self.transport.subscribe(handler)
        self.transport._on_packet(text_packet("hello"), interface=self.iface)
        handler.assert_called_once_with(SENDER_ID, "hello")

    def test_filtered_packets_are_not_dispatched(self):
        handler = MagicMock()
        self.transport.subscribe(handler)
        self.transport._on_packet(text_packet("x", to="!ffffffff"), interface=self.iface)
        handler.assert_not_called()

    def test_packets_from_other_interfaces_are_ignored(self):
        handler = MagicMock()
        self.transport.subscribe(handler)
        self.transport._on_packet(text_packet("hello"), interface=MagicMock())
        handler.assert_not_called()

    def test_handler_exception_is_logged_not_raised(self):
        handler = MagicMock(side_effect=RuntimeError("boom"))
        self.transport.subscribe(handler)
        self.transport._on_packet(text_packet("hello"), interface=self.iface)
        self.transport.logger.error.assert_called_once()

    def test_close_unsubscribes_and_closes_iface(self):
        self.transport.subscribe(MagicMock())
        self.transport.close()
        self.mock_pub.unsubscribe.assert_called_once()
        self.iface.close.assert_called_once()


class TestConnectTransport(unittest.TestCase):
    """Connection logging, ported from the old server initialization tests."""

    def setUp(self):
        self.MockSerialInterface = MagicMock()
        serial_interface_module = MagicMock(SerialInterface=self.MockSerialInterface)
        meshtastic_module = MagicMock(serial_interface=serial_interface_module)
        self.MockNoDeviceError = type("NoDeviceError", (Exception,), {})
        self.MockMeshtasticError = type("MeshtasticError", (Exception,), {})
        self.sys_modules_patcher = patch.dict(
            sys.modules,
            {
                "meshtastic": meshtastic_module,
                "meshtastic.serial_interface": serial_interface_module,
            },
        )
        self.sys_modules_patcher.start()
        self.port_patcher = patch("core.config_loader.get_serial_port", return_value=None)
        self.mock_get_port = self.port_patcher.start()
        self.logger = MagicMock()

        iface = self.MockSerialInterface.return_value
        iface.devicePath = "/dev/ttyUSB0"
        iface.myInfo.my_node_num = 0xDEADBEEF

    def tearDown(self):
        self.sys_modules_patcher.stop()
        self.port_patcher.stop()

    def connect(self, port=None):
        return connect_transport(kind="meshtastic", port=port, logger=self.logger)

    def test_successful_autodetect(self):
        transport = self.connect()

        self.assertIsInstance(transport, MeshtasticTransport)
        self.MockSerialInterface.assert_called_once_with()
        self.mock_get_port.assert_called_once_with("meshtastic")
        self.logger.info.assert_has_calls(
            [
                call("Attempting to initialize Meshtastic interface (auto-detect)..."),
                call(
                    "Meshtastic interface initialized successfully. "
                    "Device: /dev/ttyUSB0, My Node Num: !deadbeef"
                ),
            ]
        )

    def test_successful_with_config_port(self):
        self.mock_get_port.return_value = "/dev/ttyS0"
        self.assertIsNotNone(self.connect())
        self.MockSerialInterface.assert_called_once_with(devPath="/dev/ttyS0")
        self.logger.info.assert_any_call(
            "Attempting to initialize Meshtastic interface on port /dev/ttyS0..."
        )

    def test_override_port_skips_config(self):
        self.mock_get_port.return_value = "/dev/ttyS0"
        self.assertIsNotNone(self.connect(port="/dev/ttyACM1"))
        self.MockSerialInterface.assert_called_once_with(devPath="/dev/ttyACM1")
        self.mock_get_port.assert_not_called()

    def test_no_device_error(self):
        self.MockSerialInterface.side_effect = self.MockNoDeviceError("nope")
        self.assertIsNone(self.connect())
        self.logger.error.assert_any_call(
            "No Meshtastic device found. Ensure it is connected and drivers are installed."
        )

    def test_meshtastic_error(self):
        self.MockSerialInterface.side_effect = self.MockMeshtasticError("generic")
        self.assertIsNone(self.connect())
        self.logger.error.assert_any_call(
            "Meshtastic library error during initialization: generic"
        )

    def test_unexpected_error(self):
        self.MockSerialInterface.side_effect = RuntimeError("totally unexpected")
        self.assertIsNone(self.connect())
        self.logger.error.assert_any_call(
            "An unexpected error occurred during Meshtastic initialization: totally unexpected",
            exc_info=True,
        )

    def test_missing_device_info_closes_interface(self):
        iface = self.MockSerialInterface.return_value
        iface.myInfo = None
        self.assertIsNone(self.connect())
        iface.close.assert_called_once()
        self.assertIn(
            "could not retrieve device info", self.logger.error.call_args.args[0]
        )

    def test_unknown_transport(self):
        self.assertIsNone(connect_transport(kind="carrier-pigeon", logger=self.logger))
        self.assertIn("Unknown transport", self.logger.error.call_args.args[0])


class TestTransportRegistry(unittest.TestCase):
    def test_meshtastic_is_available(self):
        self.assertIn("meshtastic", available_transports())
        self.assertIs(get_transport_class("Meshtastic"), MeshtasticTransport)

    def test_unknown_transport_raises(self):
        with self.assertRaises(TransportError):
            get_transport_class("nope")

    def test_default_comes_from_env(self):
        with patch.dict("os.environ", {"BTCMESH_TRANSPORT": "meshtastic"}):
            self.assertIs(get_transport_class(), MeshtasticTransport)


if __name__ == "__main__":
    unittest.main()
