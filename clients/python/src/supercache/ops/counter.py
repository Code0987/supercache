from __future__ import annotations

from datetime import timedelta

from supercache._gen import cache_pb2 as pb
from supercache.types import Present


class CounterOps:
    def incr(self, keyspace: str, name: str, delta: int, *, timeout: timedelta | None = None) -> int:
        request = pb.IncrRequest(keyspace=keyspace, name=name, delta=delta)
        response = self._call(self._stub.Incr, request, timeout)
        return int(response.value)

    def counter_get(self, keyspace: str, name: str, *, timeout: timedelta | None = None) -> Present:
        request = pb.CounterGetRequest(keyspace=keyspace, name=name)
        response = self._call(self._stub.CounterGet, request, timeout)
        return Present(int(response.value), bool(response.present))
