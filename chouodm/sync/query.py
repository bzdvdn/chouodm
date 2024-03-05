from typing import Type

from ..query.builder import Builder
from ..query.query import QueryBuilder


class SyncQuery(object):
    __slots__ = ("_builder", "method_name")

    def __init__(self, _querybuilder: Builder, method_name: str):
        self._builder = _querybuilder
        self.method_name = method_name

    def __getattr__(self, method_name: str) -> "SyncQuery":
        return SyncQuery(self._builder, method_name)

    def __call__(self, *args, **kwargs):
        method = getattr(self._builder, self.method_name)
        return self._builder.odm_manager._io_loop.run_until_complete(
            method(*args, **kwargs)
        )


class SyncQueryBuilder(QueryBuilder):
    query_class: Type[SyncQuery] = SyncQuery  # type: ignore
