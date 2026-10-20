"""Errors raised by the SuperCache client.

NotFound is only a Get that returned found=false. A gRPC status, including
NOT_FOUND for a missing keyspace, is StatusError.
"""

from __future__ import annotations

from dataclasses import dataclass


class NotFound(Exception):
    """Get returned found=false (missing, negative cache, or tombstone)."""


class StatusError(Exception):
    """A gRPC status. code is the name, for example NOT_FOUND or UNAVAILABLE."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True)
class PeerFailure:
    peer_id: str
    message: str


class PeerFailures(Exception):
    """Delete succeeded on the wire and reported replica failures."""

    def __init__(self, failures: list[PeerFailure]) -> None:
        self.failures = failures
        super().__init__(f"{len(failures)} peer failure(s)")


@dataclass(frozen=True)
class KeyErrorItem:
    key: str
    message: str
    peer_failures: list[PeerFailure]


class KeyErrors(Exception):
    """PutMany or DeleteMany succeeded on the wire and reported per-key failures."""

    def __init__(self, errors: list[KeyErrorItem]) -> None:
        self.errors = errors
        super().__init__(f"{len(errors)} key error(s)")


def is_transport(err: BaseException) -> bool:
    """True for a dead connection. A call deadline is not transport: the RPC may have landed."""
    if not isinstance(err, StatusError):
        return False
    if err.code == "UNAVAILABLE":
        return True
    msg = err.message.lower()
    needles = (
        "connection refused",
        "connection reset",
        "broken pipe",
        "socket hang up",
        "econnrefused",
        "econnreset",
    )
    return any(n in msg for n in needles)
