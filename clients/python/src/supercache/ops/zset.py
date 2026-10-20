from __future__ import annotations

from datetime import timedelta

from supercache._gen import cache_pb2 as pb
from supercache.types import ZMember
from supercache.wire import as_bytes


class ZSetOps:
    def zadd(
        self,
        keyspace: str,
        name: str,
        member: bytes,
        score: float,
        *,
        timeout: timedelta | None = None,
    ) -> None:
        request = pb.ZAddRequest(
            keyspace=keyspace,
            name=name,
            member=as_bytes(member, "member"),
            score=score,
        )
        self._call(self._stub.ZAdd, request, timeout)

    def zrem(self, keyspace: str, name: str, member: bytes, *, timeout: timedelta | None = None) -> None:
        request = pb.ZRemRequest(
            keyspace=keyspace,
            name=name,
            member=as_bytes(member, "member"),
        )
        self._call(self._stub.ZRem, request, timeout)

    def zscore(self, keyspace: str, name: str, member: bytes, *, timeout: timedelta | None = None) -> float | None:
        request = pb.ZScoreRequest(
            keyspace=keyspace,
            name=name,
            member=as_bytes(member, "member"),
        )
        response = self._call(self._stub.ZScore, request, timeout)
        if not response.present:
            return None
        return float(response.score)

    def zcard(self, keyspace: str, name: str, *, timeout: timedelta | None = None) -> int:
        request = pb.ZCardRequest(keyspace=keyspace, name=name)
        response = self._call(self._stub.ZCard, request, timeout)
        return int(response.card)

    def zrange(
        self,
        keyspace: str,
        name: str,
        start: int,
        stop: int,
        *,
        timeout: timedelta | None = None,
    ) -> list[ZMember]:
        request = pb.ZRangeRequest(keyspace=keyspace, name=name, start=start, stop=stop)
        response = self._call(self._stub.ZRange, request, timeout)
        return [ZMember(member.member, member.score) for member in response.members]

    def zrange_by_score(
        self,
        keyspace: str,
        name: str,
        min_score: float,
        max_score: float,
        *,
        timeout: timedelta | None = None,
    ) -> list[ZMember]:
        request = pb.ZRangeByScoreRequest(
            keyspace=keyspace,
            name=name,
            min=min_score,
            max=max_score,
        )
        response = self._call(self._stub.ZRangeByScore, request, timeout)
        return [ZMember(member.member, member.score) for member in response.members]
