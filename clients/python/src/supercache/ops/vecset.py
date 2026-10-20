from __future__ import annotations

from datetime import timedelta
from typing import Sequence

from supercache._gen import cache_pb2 as pb
from supercache.types import Present, VSimHit
from supercache.wire import as_bytes, as_vector


class VecSetOps:
    def vadd(
        self,
        keyspace: str,
        name: str,
        member: bytes,
        vec: Sequence[float],
        *,
        timeout: timedelta | None = None,
    ) -> None:
        request = pb.VAddRequest(
            keyspace=keyspace,
            name=name,
            member=as_bytes(member, "member"),
            vec=as_vector(vec),
        )
        self._call(self._stub.VAdd, request, timeout)

    def vrem(self, keyspace: str, name: str, member: bytes, *, timeout: timedelta | None = None) -> None:
        request = pb.VRemRequest(
            keyspace=keyspace,
            name=name,
            member=as_bytes(member, "member"),
        )
        self._call(self._stub.VRem, request, timeout)

    def vsim(
        self,
        keyspace: str,
        name: str,
        vec: Sequence[float],
        k: int,
        *,
        timeout: timedelta | None = None,
    ) -> list[VSimHit]:
        request = pb.VSimRequest(
            keyspace=keyspace,
            name=name,
            vec=as_vector(vec),
            k=k,
        )
        response = self._call(self._stub.VSim, request, timeout)
        return [VSimHit(hit.member, hit.score) for hit in response.hits]

    def vcard(self, keyspace: str, name: str, *, timeout: timedelta | None = None) -> Present:
        request = pb.VCardRequest(keyspace=keyspace, name=name)
        response = self._call(self._stub.VCard, request, timeout)
        return Present(int(response.n), bool(response.present))

    def vdim(self, keyspace: str, name: str, *, timeout: timedelta | None = None) -> Present:
        request = pb.VDimRequest(keyspace=keyspace, name=name)
        response = self._call(self._stub.VDim, request, timeout)
        return Present(int(response.dim), bool(response.present))

    def vemb(
        self,
        keyspace: str,
        name: str,
        member: bytes,
        *,
        timeout: timedelta | None = None,
    ) -> list[float] | None:
        request = pb.VEmbRequest(
            keyspace=keyspace,
            name=name,
            member=as_bytes(member, "member"),
        )
        response = self._call(self._stub.VEmb, request, timeout)
        if not response.found:
            return None
        return [float(x) for x in response.vec]
