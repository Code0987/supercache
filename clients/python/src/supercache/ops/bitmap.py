from __future__ import annotations

from datetime import timedelta

from supercache._gen import cache_pb2 as pb
from supercache.types import BitPos, BitValue


class BitmapOps:
    def bit_set(
        self,
        keyspace: str,
        name: str,
        offset: int,
        bit: bool,
        *,
        timeout: timedelta | None = None,
    ) -> None:
        request = pb.BitSetRequest(keyspace=keyspace, name=name, offset=offset, bit=bit)
        self._call(self._stub.BitSet, request, timeout)

    def bit_get(self, keyspace: str, name: str, offset: int, *, timeout: timedelta | None = None) -> BitValue:
        request = pb.BitGetRequest(keyspace=keyspace, name=name, offset=offset)
        response = self._call(self._stub.BitGet, request, timeout)
        return BitValue(bool(response.bit), bool(response.present))

    def bit_count(
        self,
        keyspace: str,
        name: str,
        start: int,
        end: int,
        *,
        timeout: timedelta | None = None,
    ) -> int:
        request = pb.BitCountRequest(keyspace=keyspace, name=name, start=start, end=end)
        response = self._call(self._stub.BitCount, request, timeout)
        return int(response.count)

    def bit_pos(
        self,
        keyspace: str,
        name: str,
        bit: bool,
        start: int,
        end: int,
        *,
        timeout: timedelta | None = None,
    ) -> BitPos:
        request = pb.BitPosRequest(keyspace=keyspace, name=name, bit=bit, start=start, end=end)
        response = self._call(self._stub.BitPos, request, timeout)
        return BitPos(int(response.pos), bool(response.found))
