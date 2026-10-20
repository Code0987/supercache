"""Values returned by Client. These are not wire messages."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ZMember:
    member: bytes
    score: float


@dataclass(frozen=True)
class GeoMember:
    member: bytes
    lon: float
    lat: float
    dist: float


@dataclass(frozen=True)
class HashField:
    field: bytes
    value: bytes


@dataclass(frozen=True)
class StreamEntry:
    id: str
    payload: bytes


@dataclass(frozen=True)
class VSimHit:
    member: bytes
    score: float


@dataclass(frozen=True)
class TopKEntry:
    item: bytes
    count: int


@dataclass(frozen=True)
class Present:
    """A read that distinguishes a missing structure from a real zero."""

    value: int
    found: bool


@dataclass(frozen=True)
class TopKResult:
    entries: list[TopKEntry]
    found: bool


@dataclass(frozen=True)
class BitValue:
    value: bool
    found: bool


@dataclass(frozen=True)
class BitPos:
    pos: int
    found: bool


@dataclass(frozen=True)
class TLS:
    ca_file: str
    server_name: str
    client_cert: str | None = None
    client_key: str | None = None
