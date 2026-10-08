from __future__ import annotations

from datetime import timedelta

from supercache._gen import cache_pb2 as pb
from supercache.types import Present
from supercache.wire import as_bytes


class HLLOps:
    def hll_add(self, keyspace: str, name: str, item: bytes, *, timeout: timedelta | None = None) -> None:
        request = pb.HLLAddRequest(
            keyspace=keyspace,
            name=name,
            item=as_bytes(item, "item"),
        )
        self._call(self._stub.HLLAdd, request, timeout)

    def hll_count(self, keyspace: str, name: str, *, timeout: timedelta | None = None) -> Present:
        request = pb.HLLCountRequest(keyspace=keyspace, name=name)
        response = self._call(self._stub.HLLCount, request, timeout)
        return Present(int(response.count), bool(response.present))
