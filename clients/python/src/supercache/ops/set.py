from __future__ import annotations

from datetime import timedelta

from supercache._gen import cache_pb2 as pb
from supercache.wire import as_bytes


class SetOps:
    def set_add(self, keyspace: str, name: str, item: bytes, *, timeout: timedelta | None = None) -> None:
        request = pb.SetAddRequest(
            keyspace=keyspace,
            name=name,
            item=as_bytes(item, "item"),
        )
        self._call(self._stub.SetAdd, request, timeout)

    def set_remove(self, keyspace: str, name: str, item: bytes, *, timeout: timedelta | None = None) -> None:
        request = pb.SetRemoveRequest(
            keyspace=keyspace,
            name=name,
            item=as_bytes(item, "item"),
        )
        self._call(self._stub.SetRemove, request, timeout)

    def set_contains(self, keyspace: str, name: str, item: bytes, *, timeout: timedelta | None = None) -> bool:
        request = pb.SetContainsRequest(
            keyspace=keyspace,
            name=name,
            item=as_bytes(item, "item"),
        )
        response = self._call(self._stub.SetContains, request, timeout)
        return bool(response.present)

    def set_card(self, keyspace: str, name: str, *, timeout: timedelta | None = None) -> int:
        request = pb.SetCardRequest(keyspace=keyspace, name=name)
        response = self._call(self._stub.SetCard, request, timeout)
        return int(response.card)

    def set_members(self, keyspace: str, name: str, *, timeout: timedelta | None = None) -> list[bytes]:
        request = pb.SetMembersRequest(keyspace=keyspace, name=name)
        response = self._call(self._stub.SetMembers, request, timeout)
        return list(response.members)
