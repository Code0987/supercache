"""SuperCache Cache client. Dial the cache port, not the peer port."""

from supercache.client import Client, dial, dial_tls
from supercache.types import (
    BitPos,
    BitValue,
    GeoMember,
    HashField,
    Present,
    StreamEntry,
    TLS,
    TopKEntry,
    TopKResult,
    VSimHit,
    ZMember,
)
from supercache.errors import (
    KeyErrorItem,
    KeyErrors,
    NotFound,
    PeerFailure,
    PeerFailures,
    StatusError,
)
from supercache.session import Session

__all__ = [
    "BitPos",
    "BitValue",
    "Client",
    "GeoMember",
    "HashField",
    "KeyErrorItem",
    "KeyErrors",
    "NotFound",
    "PeerFailure",
    "PeerFailures",
    "Present",
    "Session",
    "StatusError",
    "StreamEntry",
    "TLS",
    "TopKEntry",
    "TopKResult",
    "VSimHit",
    "ZMember",
    "dial",
    "dial_tls",
]
