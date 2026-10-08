"""Sticky multi-seed dialing, same policy as cmd/sc."""

from __future__ import annotations

from datetime import timedelta
from threading import Lock
from typing import Callable, TypeVar

from supercache.client import Client, dial, dial_tls
from supercache.errors import StatusError, is_transport
from supercache.types import TLS

T = TypeVar("T")


class Session:
    """Walk seeds on dial. Retry a call once after UNAVAILABLE or a reset connection.

    A call-level deadline is not retried: the server may already have applied it.
    """

    def __init__(
        self,
        addrs: list[str],
        *,
        tls: TLS | None = None,
        timeout: timedelta | None = None,
        probe_s: float = 0.5,
    ) -> None:
        if not addrs:
            raise ValueError("at least one address is required")
        self._addrs = list(addrs)
        self._tls = tls
        self._timeout = timeout
        self._probe = probe_s
        self._idx = 0
        self._client: Client | None = None
        self._addr: str | None = None
        self._lock = Lock()

    def __enter__(self) -> Session:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @property
    def connected_addr(self) -> str | None:
        return self._addr

    def close(self) -> None:
        with self._lock:
            if self._client is not None:
                self._client.close()
                self._client = None
            self._addr = None

    def call(self, fn: Callable[[Client], T]) -> T:
        return self._with_client(fn)

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        attr = getattr(Client, name, None)
        if attr is None or not callable(attr):
            raise AttributeError(name)

        def wrapper(*args, **kwargs):
            return self._with_client(lambda c: getattr(c, name)(*args, **kwargs))

        return wrapper

    def _dial(self, addr: str) -> Client:
        if self._tls is None:
            return dial(addr, timeout=self._timeout)
        tls = self._tls
        return dial_tls(
            addr,
            tls.ca_file,
            tls.server_name,
            tls.client_cert,
            tls.client_key,
            timeout=self._timeout,
        )

    def _ensure(self) -> Client:
        with self._lock:
            if self._client is not None:
                return self._client
            errors: list[str] = []
            n = len(self._addrs)
            for i in range(n):
                idx = (self._idx + i) % n
                addr = self._addrs[idx]
                cli: Client | None = None
                try:
                    cli = self._dial(addr)
                    cli.wait_ready(self._probe)
                except Exception as exc:  # noqa: BLE001 — dial failure walks the list
                    errors.append(f"{addr}: {exc}")
                    if cli is not None:
                        cli.close()
                    continue
                self._client = cli
                self._idx = idx
                self._addr = addr
                return cli
            raise StatusError("UNAVAILABLE", "all cache seeds failed:\n  " + "\n  ".join(errors))

    def _invalidate(self) -> None:
        with self._lock:
            if self._client is not None:
                self._client.close()
                self._client = None
            self._idx = (self._idx + 1) % len(self._addrs)
            self._addr = None

    def _with_client(self, fn: Callable[[Client], T]) -> T:
        cli = self._ensure()
        try:
            return fn(cli)
        except StatusError as err:
            if not is_transport(err):
                raise
            first = err
        self._invalidate()
        try:
            cli = self._ensure()
        except Exception as err2:  # noqa: BLE001
            raise StatusError("UNAVAILABLE", f"{first} (re-dial: {err2})") from err2
        return fn(cli)
