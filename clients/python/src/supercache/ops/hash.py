from __future__ import annotations

from datetime import timedelta

from supercache._gen import cache_pb2 as pb
from supercache.types import HashField
from supercache.wire import as_bytes, as_field


class HashOps:
    def hset(
        self,
        keyspace: str,
        name: str,
        field: str | bytes,
        value: bytes,
        *,
        timeout: timedelta | None = None,
    ) -> None:
        request = pb.HSetRequest(
            keyspace=keyspace,
            name=name,
            field=as_field(field),
            value=as_bytes(value, "value"),
        )
        self._call(self._stub.HSet, request, timeout)

    def hget(self, keyspace: str, name: str, field: str | bytes, *, timeout: timedelta | None = None) -> bytes | None:
        request = pb.HGetRequest(keyspace=keyspace, name=name, field=as_field(field))
        response = self._call(self._stub.HGet, request, timeout)
        if not response.present:
            return None
        return response.value

    def hdel(self, keyspace: str, name: str, field: str | bytes, *, timeout: timedelta | None = None) -> None:
        request = pb.HDelRequest(keyspace=keyspace, name=name, field=as_field(field))
        self._call(self._stub.HDel, request, timeout)

    def hexists(self, keyspace: str, name: str, field: str | bytes, *, timeout: timedelta | None = None) -> bool:
        request = pb.HExistsRequest(keyspace=keyspace, name=name, field=as_field(field))
        response = self._call(self._stub.HExists, request, timeout)
        return bool(response.present)

    def hlen(self, keyspace: str, name: str, *, timeout: timedelta | None = None) -> int:
        request = pb.HLenRequest(keyspace=keyspace, name=name)
        response = self._call(self._stub.HLen, request, timeout)
        return int(response.len)

    def hgetall(self, keyspace: str, name: str, *, timeout: timedelta | None = None) -> list[HashField]:
        request = pb.HGetAllRequest(keyspace=keyspace, name=name)
        response = self._call(self._stub.HGetAll, request, timeout)
        return [HashField(field.field, field.value) for field in response.fields]
