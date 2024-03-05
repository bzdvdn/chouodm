import copy
from json import dumps
from typing import (
    Generator,
    List,
    Union,
    Any,
    Tuple,
    List,
    TYPE_CHECKING,
    Union,
    Dict,
    Type,
)

from bson import ObjectId

from .extra import ExtraQueryMapper

from ..validation import validate_field_value


__all__ = (
    "Q",
    "QCombination",
    "FindResult",
    "SimpleAggregateResult",
    "generate_basic_query",
)


if TYPE_CHECKING:
    from ..document import Document
    from ..manager import ODMManager
    from .builder import Builder
    from ..typing import DictStrAny


class Query(object):
    __slots__ = ("_builder", "method_name")

    def __init__(self, builder: "Builder", method_name: str):
        self._builder = builder
        self.method_name = method_name

    def __getattr__(self, method_name: str) -> "Query":
        return Query(self._builder, method_name)

    def __call__(self, *args, **kwargs):
        method = getattr(self._builder, self.method_name)
        return method(*args, **kwargs)


class QueryBuilder(object):
    query_class: Type[Query] = Query

    __slots__ = ("builder",)

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


def _validate_query_data(builder: "Builder", query: dict) -> dict:
    return builder._validate_query_data(query)


class QNodeVisitor(object):
    """Base visitor class for visiting Q-object nodes in a query tree."""

    def prepare_combination(
        self, combination: "QCombination"
    ) -> Union["QCombination", dict]:
        """Called by QCombination objects."""
        return combination

    def visit_query(self, query: "Q") -> Union["Q", dict]:
        """Called by (New)Q objects."""
        return query


class SimplificationVisitor(QNodeVisitor):
    def __init__(self, builder: "Builder"):
        self.builder = builder

    def prepare_combination(
        self, combination: "QCombination"
    ) -> Union["QCombination", dict]:
        if combination.operation == combination.AND:
            # The simplification only applies to 'simple' queries
            if all(isinstance(node, Q) for node in combination.children):
                queries = [n.query for n in combination.children]
                query = self._query_conjunction(queries)
                return {"$and": query}

        return combination

    def _query_conjunction(self, queries):
        """Merges query dicts - effectively &ing them together."""
        combined_query = []
        for query in queries:
            query = _validate_query_data(self.builder, query)
            combined_query.append(copy.deepcopy(query))
        return combined_query


class QCompilerVisitor(QNodeVisitor):
    """Compiles the nodes in a query tree to a PyMongo-compatible query
    dictionary.
    """

    def __init__(self, builder: "Builder"):
        self.builder = builder

    def prepare_combination(
        self, combination: "QCombination"
    ) -> Union["QCombination", dict]:
        operator = "$and"
        if combination.operation == combination.OR:
            operator = "$or"
        return {operator: combination.children}

    def visit_query(self, query: "Q") -> Union["Q", dict]:
        data = _validate_query_data(self.builder, query.query)
        return data


class QNode(object):
    """Base class for nodes in query trees."""

    AND = 0
    OR = 1

    def to_query(self, builder: "Builder") -> dict:
        query = self.accept(SimplificationVisitor(builder))
        if not isinstance(query, dict):
            query = query.accept(QCompilerVisitor(builder))
        return query

    def accept(self, visitor):
        raise NotImplementedError

    def _combine(self, other, operation):
        """Combine this node with another node into a QCombination
        object.
        """
        # If the other Q() is empty, ignore it and just use `self`.
        if getattr(other, "empty", True):
            return self

        # Or if this Q is empty, ignore it and just use `other`.
        if self.empty:
            return other

        return QCombination(operation, [self, other])

    @property
    def empty(self):
        return False

    def __or__(self, other):
        return self._combine(other, self.OR)

    def __and__(self, other):
        return self._combine(other, self.AND)


class QCombination(QNode):
    def __init__(self, operation, children):
        self.operation = operation
        self.children = []
        for node in children:
            # If the child is a combination of the same type, we can merge its
            # children directly into this combinations children
            if isinstance(node, QCombination) and node.operation == operation:
                self.children += node.children
            else:
                self.children.append(node)

    def __repr__(self):
        op = " & " if self.operation is self.AND else " | "
        return "(%s)" % op.join([repr(node) for node in self.children])

    def __bool__(self):
        return bool(self.children)

    def accept(self, visitor) -> Union["QCombination", dict]:
        for i in range(len(self.children)):
            if isinstance(self.children[i], QNode):
                self.children[i] = self.children[i].accept(visitor)

        return visitor.prepare_combination(self)

    @property
    def empty(self):
        return not bool(self.children)

    def __eq__(self, other):
        return (
            self.__class__ == other.__class__
            and self.operation == other.operation
            and self.children == other.children
        )


class Q(QNode):
    """A simple query object, used in a query tree to build up more complex
    query structures.
    """

    def __init__(self, **query):
        self.query = query

    def __repr__(self):
        return "Q(**%s)" % repr(self.query)

    def __bool__(self):
        return bool(self.query)

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.query == other.query

    def accept(self, visit: "QNodeVisitor") -> Union["Q", dict]:
        return visit.visit_query(self)

    @property
    def empty(self) -> bool:
        return not bool(self.query)


class FindResult(object):
    __slots__ = ("_data", "mongo_document_class")

    def __init__(
        self,
        mongo_document_class: "Document",
        data: list,
    ):
        self._data = data
        self.mongo_document_class = mongo_document_class

    # @handle_and_convert_connection_errors
    def __iter__(self):
        for obj in self._data:
            yield obj

    def __next__(self):
        return next(self.__iter__())

    @property
    def data(self) -> List:
        return [obj.data for obj in self.__iter__()]

    @property
    def generator(self) -> Generator:
        return self.__iter__()

    @property
    def data_generator(self) -> Generator:
        for obj in self.__iter__():
            yield obj.data

    @property
    def list(self) -> List:
        return list(self.__iter__())

    def json(self) -> str:
        return dumps(self.data)

    def first(self) -> Any:
        return next(self.__iter__())

    def serialize(
        self, fields: Union[Tuple, List], to_list: bool = True
    ) -> Union[Tuple, List]:
        return (
            [obj.serialize(fields) for obj in self.__iter__()]
            if to_list
            else tuple(obj.serialize(fields) for obj in self.__iter__())
        )

    def serialize_generator(self, fields: Union[Tuple, List]) -> Generator:
        for obj in self.__iter__():
            yield obj.serialize(fields)

    def serialize_json(self, fields: Union[Tuple, List]) -> str:
        return dumps(self.serialize(fields))


class SimpleAggregateResult(object):
    __slots__ = ("_data", "mongo_document_class")

    def __init__(
        self,
        mongo_document_class: "Document",
        data: dict,
    ):
        self._data = data
        self.mongo_document_class = mongo_document_class

    def json(self) -> str:
        return dumps(self._data)

    @property
    def data(self) -> dict:
        return self._data


def generate_basic_query(
    manager: "ODMManager",
    query: dict,
    with_validate_document_fields: bool = True,
) -> dict:
    query_params: dict = {}
    for query_field, value in query.items():
        field, *extra_params = query_field.split("__")
        inners, extra_params = manager._parse_extra_params(extra_params)
        if with_validate_document_fields and not manager._validate_field(field):
            continue
        extra = ExtraQueryMapper(manager.document, field).query(extra_params, value)
        if extra:
            value = extra[field]
        elif field == "_id":
            value = ObjectId(value)
        else:
            value = (
                validate_field_value(manager.document, field, value)
                if not inners
                else value
            )
        if inners:
            field = f'{field}.{".".join(i for i in inners)}'
        if (
            extra
            and field in query_params
            and ("__gt" in query_field or "__lt" in query_field)
        ):
            query_params[field].update(value)
        else:
            query_params[field] = value
    return query_params
