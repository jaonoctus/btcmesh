"""End to end: real CLI and real server handler talking over in-memory transports.

Exercises the whole BTCMesh protocol (chunking, ACKs, reassembly, broadcast,
final ACK/NACK) through the Transport interface, with only Bitcoin RPC mocked.
"""
import threading
import types
import unittest
from unittest.mock import MagicMock, patch

import btcmesh_cli
import btcmesh_server
from core.reassembler import TransactionReassembler
from core.transport.base import Transport


class LoopbackTransport(Transport):
    """Delivers sent messages to a peer transport on a background thread,
    like a radio would."""

    name = "Loopback"
    chunk_size = 64  # Small, so the test transaction spans several chunks.

    def __init__(self, node_id):
        super().__init__()
        self.node_id = node_id
        self.peer = None
        self.sent = []

    @classmethod
    def open(cls, port=None, logger=None):  # pragma: no cover - not used
        raise NotImplementedError

    def send_text(self, destination_id, text):
        assert self.peer is not None and destination_id == self.peer.node_id
        self.sent.append(text)
        threading.Thread(
            target=self.peer._dispatch, args=(self.node_id, text), daemon=True
        ).start()

    def own_node_id(self):
        return self.node_id

    @staticmethod
    def is_valid_node_id(node_id):
        return node_id.startswith("@")

    def close(self):
        pass


def make_pair():
    client, server = LoopbackTransport("@client"), LoopbackTransport("@relay")
    client.peer, server.peer = server, client
    server.subscribe(lambda sender, text: btcmesh_server.handle_text_message(server, sender, text))
    return client, server


class TestLoopback(unittest.TestCase):
    def setUp(self):
        self.rpc = MagicMock()
        patches = [
            patch.object(btcmesh_server, "transaction_reassembler", TransactionReassembler(timeout_seconds=30)),
            patch.object(btcmesh_server, "bitcoin_rpc", self.rpc),
            patch.object(btcmesh_server, "transaction_history", None),
            patch.object(btcmesh_server, "server_logger", MagicMock()),
            patch.object(btcmesh_cli, "ACK_TIMEOUT", 5),
            patch.object(btcmesh_cli, "RETRY_TIMEOUT", 0),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def run_cli(self, client, tx_hex):
        args = types.SimpleNamespace(
            destination="@relay", tx=tx_hex, dry_run=False, session_id="abcde"
        )
        with patch("builtins.print"):
            return btcmesh_cli.cli_main(
                args=args, injected_transport=client, injected_logger=MagicMock()
            )

    def test_successful_broadcast(self):
        tx_hex = "ab" * 100  # 200 hex chars, 4 chunks of 64
        self.rpc.broadcast_transaction.return_value = ("f00dtxid", None)
        client, server = make_pair()

        self.assertEqual(self.run_cli(client, tx_hex), 0)

        self.rpc.broadcast_transaction.assert_called_once_with(tx_hex)
        self.assertEqual(len(client.sent), 4)
        self.assertEqual(client.sent[0], f"BTC_TX|abcde|1/4|{tx_hex[:64]}")
        self.assertEqual(server.sent[-1], "BTC_ACK|abcde|SUCCESS|TXID:f00dtxid")
        # The CLI cleans up after itself.
        self.assertEqual(client._handlers, [])

    def test_broadcast_failure_returns_exit_code_3(self):
        self.rpc.broadcast_transaction.return_value = (None, "bad-txns-inputs-spent")
        client, server = make_pair()

        self.assertEqual(self.run_cli(client, "cd" * 20), 3)
        self.assertEqual(server.sent[-1], "BTC_NACK|abcde|ERROR|Inputs spent")


if __name__ == "__main__":
    unittest.main()
