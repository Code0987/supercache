from __future__ import annotations

from datetime import timedelta

from supercache._gen import cache_pb2 as pb
from supercache.wire import as_bytes


class JsonOps:
    def json_set(
        self,
        keyspace: str,
        name: str,
        path: str,
        value: bytes,
        *,
        timeout: timedelta | None = None,
    ) -> None:
        request = pb.JsonSetRequest(
            keyspace=keyspace,
            name=name,
            path=path,
            value=as_bytes(value, "value"),
        )
        self._call(self._stub.JsonSet, request, timeout)

    def json_get(self, keyspace: str, name: str, path: str, *, timeout: timedelta | None = None) -> bytes | None:
        request = pb.JsonGetRequest(keyspace=keyspace, name=name, path=path)
        response = self._call(self._stub.JsonGet, request, timeout)
        if not response.present:
            return None
        return response.value

    def json_del(self, keyspace: str, name: str, path: str, *, timeout: timedelta | None = None) -> None:
        request = pb.JsonDelRequest(keyspace=keyspace, name=name, path=path)
        self._call(self._stub.JsonDel, request, timeout)
