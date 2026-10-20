from __future__ import annotations

from datetime import timedelta

from supercache._gen import cache_pb2 as pb
from supercache.wire import as_bytes


class BloomOps:
    def bloom_add(self, keyspace: str, name: str, item: bytes, *, timeout: timedelta | None = None) -> None:
        request = pb.BloomAddRequest(
            keyspace=keyspace,
            name=name,
            item=as_bytes(item, "item"),
        )
        self._call(self._stub.BloomAdd, request, timeout)

    def bloom_test(self, keyspace: str, name: str, item: bytes, *, timeout: timedelta | None = None) -> bool:
        request = pb.BloomTestRequest(
            keyspace=keyspace,
            name=name,
            item=as_bytes(item, "item"),
        )
        response = self._call(self._stub.BloomTest, request, timeout)
        return bool(response.maybe)
