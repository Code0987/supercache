from __future__ import annotations

from datetime import timedelta

from supercache._gen import cache_pb2 as pb
from supercache.types import Present
from supercache.wire import as_bytes


class CMSOps:
    def cms_incr(
        self,
        keyspace: str,
        name: str,
        item: bytes,
        n: int,
        *,
        timeout: timedelta | None = None,
    ) -> None:
        request = pb.CMSIncrRequest(
            keyspace=keyspace,
            name=name,
            item=as_bytes(item, "item"),
            n=n,
        )
        self._call(self._stub.CMSIncr, request, timeout)

    def cms_query(self, keyspace: str, name: str, item: bytes, *, timeout: timedelta | None = None) -> Present:
        request = pb.CMSQueryRequest(
            keyspace=keyspace,
            name=name,
            item=as_bytes(item, "item"),
        )
        response = self._call(self._stub.CMSQuery, request, timeout)
        return Present(int(response.count), bool(response.present))
