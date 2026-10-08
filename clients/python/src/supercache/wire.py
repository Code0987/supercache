"""Caller values to proto fields. Not part of the public client."""

from __future__ import annotations

from datetime import timedelta
from typing import Sequence

from supercache.errors import KeyErrorItem, KeyErrors, PeerFailure, PeerFailures


def as_bytes(value: bytes, what: str) -> bytes:
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    raise TypeError(f"{what} must be bytes")


def as_field(value: str | bytes) -> bytes:
    if isinstance(value, str):
        return value.encode("utf-8")
    return as_bytes(value, "field")


def as_vector(vec: Sequence[float]) -> list[float]:
    if isinstance(vec, (str, bytes, bytearray)):
        raise TypeError("vector must be a sequence of floats")
    return [float(x) for x in vec]


def ttl_fields(ttl: timedelta | None) -> tuple[int, bool]:
    """(ttl_nanos, ttl_set). None leaves the flag false. Zero sets the flag."""
    if ttl is None:
        return 0, False
    nanos = int(ttl.total_seconds() * 1_000_000_000)
    return nanos, True


def raise_key_errors(errors) -> None:
    if not errors:
        return
    items = []
    for err in errors:
        peers = [PeerFailure(p.peer_id, p.message) for p in err.peer_failures]
        items.append(KeyErrorItem(err.key, err.message, peers))
    raise KeyErrors(items)


def raise_peer_failures(failures) -> None:
    if not failures:
        return
    raise PeerFailures([PeerFailure(p.peer_id, p.message) for p in failures])
