from __future__ import annotations

from datetime import timedelta

from supercache._gen import cache_pb2 as pb
from supercache.types import Present, StreamEntry
from supercache.wire import as_bytes


class StreamOps:
    def xadd(self, keyspace: str, name: str, payload: bytes, *, timeout: timedelta | None = None) -> str:
        request = pb.XAddRequest(
            keyspace=keyspace,
            name=name,
            payload=as_bytes(payload, "payload"),
        )
        response = self._call(self._stub.XAdd, request, timeout)
        return response.id

    def xrange(
        self,
        keyspace: str,
        name: str,
        start: str,
        end: str,
        count: int,
        *,
        timeout: timedelta | None = None,
    ) -> list[StreamEntry]:
        request = pb.XRangeRequest(keyspace=keyspace, name=name, start=start, end=end, count=count)
        response = self._call(self._stub.XRange, request, timeout)
        return [StreamEntry(entry.id, entry.payload) for entry in response.entries]

    def xrevrange(
        self,
        keyspace: str,
        name: str,
        start: str,
        end: str,
        count: int,
        *,
        timeout: timedelta | None = None,
    ) -> list[StreamEntry]:
        request = pb.XRevRangeRequest(keyspace=keyspace, name=name, start=start, end=end, count=count)
        response = self._call(self._stub.XRevRange, request, timeout)
        return [StreamEntry(entry.id, entry.payload) for entry in response.entries]

    def xlen(self, keyspace: str, name: str, *, timeout: timedelta | None = None) -> Present:
        request = pb.XLenRequest(keyspace=keyspace, name=name)
        response = self._call(self._stub.XLen, request, timeout)
        return Present(int(response.n), bool(response.present))

    def xdel(self, keyspace: str, name: str, id: str, *, timeout: timedelta | None = None) -> None:
        request = pb.XDelRequest(keyspace=keyspace, name=name, id=id)
        self._call(self._stub.XDel, request, timeout)

    def xtrim(self, keyspace: str, name: str, max_len: int, *, timeout: timedelta | None = None) -> None:
        request = pb.XTrimRequest(keyspace=keyspace, name=name, max_len=max_len)
        self._call(self._stub.XTrim, request, timeout)
