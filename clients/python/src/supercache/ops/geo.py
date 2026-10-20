from __future__ import annotations

from datetime import timedelta

from supercache._gen import cache_pb2 as pb
from supercache.types import GeoMember
from supercache.wire import as_bytes


class GeoOps:
    def geo_add(
        self,
        keyspace: str,
        name: str,
        member: bytes,
        lon: float,
        lat: float,
        *,
        timeout: timedelta | None = None,
    ) -> None:
        request = pb.GeoAddRequest(
            keyspace=keyspace,
            name=name,
            member=as_bytes(member, "member"),
            lon=lon,
            lat=lat,
        )
        self._call(self._stub.GeoAdd, request, timeout)

    def geo_rem(self, keyspace: str, name: str, member: bytes, *, timeout: timedelta | None = None) -> None:
        request = pb.GeoRemRequest(
            keyspace=keyspace,
            name=name,
            member=as_bytes(member, "member"),
        )
        self._call(self._stub.GeoRem, request, timeout)

    def geo_pos(
        self,
        keyspace: str,
        name: str,
        member: bytes,
        *,
        timeout: timedelta | None = None,
    ) -> tuple[float, float] | None:
        request = pb.GeoPosRequest(
            keyspace=keyspace,
            name=name,
            member=as_bytes(member, "member"),
        )
        response = self._call(self._stub.GeoPos, request, timeout)
        if not response.present:
            return None
        return float(response.lon), float(response.lat)

    def geo_card(self, keyspace: str, name: str, *, timeout: timedelta | None = None) -> int:
        request = pb.GeoCardRequest(keyspace=keyspace, name=name)
        response = self._call(self._stub.GeoCard, request, timeout)
        return int(response.card)

    def geo_dist(
        self,
        keyspace: str,
        name: str,
        a: bytes,
        b: bytes,
        *,
        timeout: timedelta | None = None,
    ) -> float | None:
        request = pb.GeoDistRequest(
            keyspace=keyspace,
            name=name,
            a=as_bytes(a, "a"),
            b=as_bytes(b, "b"),
        )
        response = self._call(self._stub.GeoDist, request, timeout)
        if not response.present:
            return None
        return float(response.meters)

    def geo_radius(
        self,
        keyspace: str,
        name: str,
        lon: float,
        lat: float,
        radius_m: float,
        limit: int,
        *,
        timeout: timedelta | None = None,
    ) -> list[GeoMember]:
        request = pb.GeoRadiusRequest(
            keyspace=keyspace,
            name=name,
            lon=lon,
            lat=lat,
            radius_meters=radius_m,
            limit=limit,
        )
        response = self._call(self._stub.GeoRadius, request, timeout)
        return [
            GeoMember(member.member, member.lon, member.lat, member.dist_meters)
            for member in response.members
        ]
