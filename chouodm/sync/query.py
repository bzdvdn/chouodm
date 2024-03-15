from typing import Type, Dict, TYPE_CHECKING, Any

from ..query.builder import Builder

if TYPE_CHECKING:
    from ..typing import DictStrAny


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


class SyncQueryBuilder(object):
    query_class: Type[SyncQuery] = SyncQuery  # type: ignore

    def __init__(self, builder: "Builder"):
        self.builder = builder

    def __getattr__(self, method_name: str) -> Any:
        if method_name != "odm_manager" and hasattr(self.builder, method_name):
            return self.query_class(self.builder, method_name)
        raise AttributeError(f"invalid Q attr query: {method_name}")

    def _validate_query_data(self, query: Dict) -> "DictStrAny":
        return self.builder._validate_query_data(query)

    def _check_query_args(self, *args, **kwargs):
        return self.builder._check_query_args(*args, **kwargs)

    def _validate_raw_query(self, *args, **kwargs):
        return self.builder._validate_raw_query(*args, **kwargs)

    def __call__(self, method_name, *args, **method_kwargs):
        return getattr(self, method_name)(*args, **method_kwargs)
