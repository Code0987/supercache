from __future__ import annotations

from datetime import timedelta

from supercache._gen import cache_pb2 as pb
from supercache.wire import as_bytes


class ListOps:
    def lpush(self, keyspace: str, name: str, item: bytes, *, timeout: timedelta | None = None) -> None:
        request = pb.LPushRequest(
            keyspace=keyspace,
            name=name,
            item=as_bytes(item, "item"),
        )
        self._call(self._stub.LPush, request, timeout)

    def rpush(self, keyspace: str, name: str, item: bytes, *, timeout: timedelta | None = None) -> None:
        request = pb.RPushRequest(
            keyspace=keyspace,
            name=name,
            item=as_bytes(item, "item"),
        )
        self._call(self._stub.RPush, request, timeout)

    def lpop(self, keyspace: str, name: str, *, timeout: timedelta | None = None) -> bytes | None:
        request = pb.LPopRequest(keyspace=keyspace, name=name)
        response = self._call(self._stub.LPop, request, timeout)
        if not response.present:
            return None
        return response.item

    def rpop(self, keyspace: str, name: str, *, timeout: timedelta | None = None) -> bytes | None:
        request = pb.RPopRequest(keyspace=keyspace, name=name)
        response = self._call(self._stub.RPop, request, timeout)
        if not response.present:
            return None
        return response.item

    def llen(self, keyspace: str, name: str, *, timeout: timedelta | None = None) -> int:
        request = pb.LLenRequest(keyspace=keyspace, name=name)
        response = self._call(self._stub.LLen, request, timeout)
        return int(response.len)

    def lindex(self, keyspace: str, name: str, index: int, *, timeout: timedelta | None = None) -> bytes | None:
        request = pb.LIndexRequest(keyspace=keyspace, name=name, index=index)
        response = self._call(self._stub.LIndex, request, timeout)
        if not response.present:
            return None
        return response.item

    def lrange(
        self,
        keyspace: str,
        name: str,
        start: int,
        stop: int,
        *,
        timeout: timedelta | None = None,
    ) -> list[bytes]:
        request = pb.LRangeRequest(keyspace=keyspace, name=name, start=start, stop=stop)
        response = self._call(self._stub.LRange, request, timeout)
        return list(response.items)
