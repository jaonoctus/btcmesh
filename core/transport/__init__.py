"""
Radio transports for BTCMesh.

Use connect_transport() from the server and CLI (logs and returns None on
failure) or open_transport() from GUIs (raises, so the caller can show the
error). The transport type comes from the BTCMESH_TRANSPORT setting.
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional, Type

from core.transport.base import (
    TextHandler,
    Transport,
    TransportError,
    format_node_display,
)

DEFAULT_TRANSPORT = "meshtastic"


def _registry() -> Dict[str, Type[Transport]]:
    # Imported lazily so importing core.transport never pulls in a radio library.
    from core.transport.meshcore_transport import MeshCoreTransport
    from core.transport.meshtastic_transport import MeshtasticTransport

    return {"meshtastic": MeshtasticTransport, "meshcore": MeshCoreTransport}


def available_transports() -> List[str]:
    return sorted(_registry())


def get_transport_class(kind: Optional[str] = None) -> Type[Transport]:
    """Transport class for kind (defaults to the configured transport)."""
    if kind is None:
        from core.config_loader import get_transport_type

        kind = get_transport_type()
    registry = _registry()
    try:
        return registry[kind.lower()]
    except KeyError:
        raise TransportError(
            f"Unknown transport '{kind}'. Available: {', '.join(sorted(registry))}"
        ) from None


def scan_devices(kind: Optional[str] = None) -> List[str]:
    """Device paths that look like radios of the given kind."""
    try:
        return get_transport_class(kind).scan_devices()
    except Exception:
        return []


def open_transport(
    kind: Optional[str] = None,
    port: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
) -> Transport:
    """Connect to a radio. Raises on failure (ImportError if the radio library
    is missing, TransportError or the library's own exception otherwise).

    port None uses the configured port (<TRANSPORT>_SERIAL_PORT), then auto-detect.
    """
    from core.config_loader import get_serial_port

    cls = get_transport_class(kind)
    if port is None:
        port = get_serial_port(cls.name.lower())
    return cls.open(port=port, logger=logger)


def list_devices(kind: Optional[str] = None) -> List[str]:
    """Devices to offer in a device picker: the configured device (which may
    be a BLE or TCP address a scan can't find) first, then scanned ports."""
    from core.config_loader import get_serial_port

    try:
        cls = get_transport_class(kind)
    except TransportError:
        return []
    devices = scan_devices(kind)
    configured = get_serial_port(cls.name.lower())
    if configured:
        devices = [configured] + [d for d in devices if d != configured]
    return devices


def connect_transport(
    kind: Optional[str] = None,
    port: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
) -> Optional[Transport]:
    """Connect to a radio, logging progress. Returns None on failure.

    Args:
        kind: Transport type ('meshtastic' or 'meshcore'). None uses BTCMESH_TRANSPORT.
        port: Device path. None uses the configured port, then auto-detect.
    """
    from core.config_loader import get_serial_port

    log = logger or logging.getLogger("btcmesh.transport")
    try:
        cls = get_transport_class(kind)
    except TransportError as e:
        log.error(str(e))
        return None

    port_to_use = port if port is not None else get_serial_port(cls.name.lower())
    port_info = f" on port {port_to_use}" if port_to_use else " (auto-detect)"
    log.info(f"Attempting to initialize {cls.name} interface{port_info}...")

    try:
        transport = cls.open(port=port_to_use, logger=log)
    except ImportError as e:
        log.error(f"{cls.name} library not found ({e}). Install the dependencies in requirements.txt.")
        return None
    except TransportError as e:
        log.error(str(e))
        return None
    except Exception as e:
        described = cls.describe_connect_error(e)
        if described != str(e):
            log.error(described)
        else:
            log.error(
                f"An unexpected error occurred during {cls.name} initialization: {e}",
                exc_info=True,
            )
        return None

    log.info(
        f"{cls.name} interface initialized successfully. "
        f"Device: {transport.device_path or '?'}, My Node Num: {transport.own_node_id()}"
    )
    return transport


__all__ = [
    "DEFAULT_TRANSPORT",
    "TextHandler",
    "Transport",
    "TransportError",
    "available_transports",
    "connect_transport",
    "format_node_display",
    "get_transport_class",
    "list_devices",
    "open_transport",
    "scan_devices",
]
