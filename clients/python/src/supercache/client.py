"""Sync Cache client. Semantics match pkg/client in the Go module.

Return types live in supercache.types. Each keyspace mode lives in supercache.ops.
This file is the connection and the KV calls.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Sequence

import grpc

from supercache._gen import cache_pb2 as pb
from supercache._gen import cache_pb2_grpc as pb_grpc
from supercache.errors import NotFound, StatusError
from supercache.ops.bitmap import BitmapOps
from supercache.ops.bloom import BloomOps
from supercache.ops.cms import CMSOps
from supercache.ops.counter import CounterOps
from supercache.ops.geo import GeoOps
from supercache.ops.hash import HashOps
from supercache.ops.hll import HLLOps
from supercache.ops.json import JsonOps
from supercache.ops.list import ListOps
from supercache.ops.set import SetOps
from supercache.ops.stream import StreamOps
from supercache.ops.topk import TopKOps
from supercache.ops.vecset import VecSetOps
from supercache.ops.zset import ZSetOps
from supercache.wire import as_bytes, raise_key_errors, raise_peer_failures, ttl_fields


class Client(
    BloomOps,
    SetOps,
    ZSetOps,
    GeoOps,
    ListOps,
    HashOps,
    CounterOps,
    JsonOps,
    BitmapOps,
    HLLOps,
    TopKOps,
    CMSOps,
    VecSetOps,
    StreamOps,
):
    """One channel to a cache node. Close it when finished."""

    def __init__(self, channel: grpc.Channel, timeout: timedelta | None = None) -> None:
        self._channel = channel
        self._stub = pb_grpc.CacheStub(channel)
        self.timeout = timeout
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._channel.close()

    def wait_ready(self, timeout_s: float) -> None:
        grpc.channel_ready_future(self._channel).result(timeout=timeout_s)

    def _seconds(self, timeout: timedelta | None) -> float | None:
        chosen = self.timeout if timeout is None else timeout
        if chosen is None:
            return None
        return chosen.total_seconds()

    def _call(self, method, request, timeout: timedelta | None):
        kwargs = {}
        seconds = self._seconds(timeout)
        if seconds is not None:
            kwargs["timeout"] = seconds
        try:
            return method(request, **kwargs)
        except grpc.RpcError as exc:
            code = exc.code()
            name = code.name if code is not None else "UNKNOWN"
            raise StatusError(name, exc.details() or "") from exc

    def get(self, keyspace: str, key: str, *, timeout: timedelta | None = None) -> bytes:
        request = pb.GetRequest(keyspace=keyspace, key=key)
        response = self._call(self._stub.Get, request, timeout)
        if not response.found:
            raise NotFound(f"{keyspace}/{key}")
        return response.value

    def put(
        self,
        keyspace: str,
        key: str,
        value: bytes,
        *,
        ttl: timedelta | None = None,
        timeout: timedelta | None = None,
    ) -> None:
        nanos, set_ = ttl_fields(ttl)
        request = pb.PutRequest(
            keyspace=keyspace,
            key=key,
            value=as_bytes(value, "value"),
            ttl_nanos=nanos,
            ttl_set=set_,
        )
        self._call(self._stub.Put, request, timeout)

    def put_many(
        self,
        keyspace: str,
        items: Sequence[tuple[str, bytes]],
        *,
        ttl: timedelta | None = None,
        timeout: timedelta | None = None,
    ) -> None:
        nanos, set_ = ttl_fields(ttl)
        request = pb.PutManyRequest(
            keyspace=keyspace,
            items=[pb.KV(key=key, value=as_bytes(value, "value")) for key, value in items],
            ttl_nanos=nanos,
            ttl_set=set_,
        )
        response = self._call(self._stub.PutMany, request, timeout)
        raise_key_errors(response.errors)

    def delete(self, keyspace: str, key: str, *, timeout: timedelta | None = None) -> None:
        request = pb.DeleteRequest(keyspace=keyspace, key=key)
        response = self._call(self._stub.Delete, request, timeout)
        raise_peer_failures(response.peer_failures)

    def delete_many(self, keyspace: str, keys: Sequence[str], *, timeout: timedelta | None = None) -> None:
        request = pb.DeleteManyRequest(keyspace=keyspace, keys=list(keys))
        response = self._call(self._stub.DeleteMany, request, timeout)
        raise_key_errors(response.errors)


def dial(addr: str, *, timeout: timedelta | None = None) -> Client:
    """Plaintext dial. For local development and tests."""
    return Client(grpc.insecure_channel(addr), timeout)


def dial_tls(
    addr: str,
    ca_file: str,
    server_name: str,
    client_cert: str | None = None,
    client_key: str | None = None,
    *,
    timeout: timedelta | None = None,
) -> Client:
    """TLS dial. server_name is required (the cert name, not the IP)."""
    if not server_name:
        raise ValueError("server_name is required")
    if bool(client_cert) != bool(client_key):
        raise ValueError("client_cert and client_key must both be set")
    root = Path(ca_file).read_bytes()
    if client_cert and client_key:
        creds = grpc.ssl_channel_credentials(
            root_certificates=root,
            private_key=Path(client_key).read_bytes(),
            certificate_chain=Path(client_cert).read_bytes(),
        )
    else:
        creds = grpc.ssl_channel_credentials(root_certificates=root)
    channel = grpc.secure_channel(addr, creds, (("grpc.ssl_target_name_override", server_name),))
    return Client(channel, timeout)
