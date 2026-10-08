from __future__ import annotations

from datetime import timedelta

from supercache._gen import cache_pb2 as pb
from supercache.types import TopKEntry, TopKResult
from supercache.wire import as_bytes


class TopKOps:
    def topk_add(self, keyspace: str, name: str, item: bytes, *, timeout: timedelta | None = None) -> None:
        request = pb.TopKAddRequest(
            keyspace=keyspace,
            name=name,
            item=as_bytes(item, "item"),
        )
        self._call(self._stub.TopKAdd, request, timeout)

    def topk_list(self, keyspace: str, name: str, *, timeout: timedelta | None = None) -> TopKResult:
        request = pb.TopKListRequest(keyspace=keyspace, name=name)
        response = self._call(self._stub.TopKList, request, timeout)
        entries = [TopKEntry(entry.item, int(entry.count)) for entry in response.entries]
        return TopKResult(entries, bool(response.present))
