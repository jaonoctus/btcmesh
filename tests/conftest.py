"""Keep the test suite independent of a developer's local .env.

python-dotenv never overrides variables that are already set, so pinning the
radio settings here (before any test module imports the app) means a .env
that selects MeshCore or a specific device can't change test behaviour.
Tests that need other values patch os.environ themselves.
"""
import os

os.environ["BTCMESH_TRANSPORT"] = "meshtastic"
for _var in (
    "MESHTASTIC_SERIAL_PORT",
    "MESHCORE_SERIAL_PORT",
    "MESHCORE_BLE_PIN",
    "MESHCORE_AUTO_ADD_CONTACTS",
    "MESHCORE_ADVERT_ON_CONNECT",
):
    os.environ[_var] = ""
