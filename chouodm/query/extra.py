from typing import (
    Optional,
    List,
    Any,
    Dict,
    Union,
    List,
    Tuple,
    TYPE_CHECKING,
)
from re import compile, IGNORECASE

from bson import ObjectId, Regex
from pymongo import UpdateOne

from ..property import cached_classproperty
from ..validation import validate_field_value
from ..errors import QueryValidationError

__all__ = (
    "ExtraQueryMapper",
    "group_by_aggregate_generation",
    "generate_name_field",
    "bulk_query_generator",
)

if TYPE_CHECKING:
    from ..document import Document
    from ..typing import DocumentType


class ExtraQueryMapper(object):
    """extra mapper for __ queries like find(_id__in=[], name__regex='123')"""

    __slots__ = ("document", "field_name")

    def __init__(self, document: Union["Document", "DocumentType"], field_name: str):
        self.field_name = field_name
        self.document = document

    def query(self, extra_methods: List, values: Any) -> Dict:
        if self.field_name == "_id":
            values = (
                [ObjectId(v) for v in values]
                if isinstance(values, list)
                else ObjectId(values)
            )
        if extra_methods:
            query: Dict = {self.field_name: {}}
            for extra_method in extra_methods:
                if extra_method == "in":
                    extra_method = "in_"
                elif extra_method == "inc":
                    return self.inc(values)
                elif extra_method == "unset":
                    return self.unset(values)
                query[self.field_name].update(getattr(self, extra_method)(values))
            return query
        return {}

    def in_(self, list_values: List) -> dict:
        if not isinstance(list_values, list):
            raise TypeError("values must be a list type")
        try:
            return {
                "$in": [
                    validate_field_value(self.document, self.field_name, v)
                    for v in list_values
                ]
            }
        except QueryValidationError:
            return {"$in": list_values}

    def regex(self, regex_value: str) -> dict:
        return {"$regex": Regex.from_native(compile(regex_value))}

    def iregex(self, regex_value: str) -> dict:
        return {"$regex": Regex.from_native(compile(regex_value, IGNORECASE))}

    def regex_ne(self, regex_value: str) -> dict:
        return {"$not": Regex.from_native(compile(regex_value))}

    def ne(self, value: Any) -> dict:
        return {"$ne": validate_field_value(self.document, self.field_name, value)}

    def startswith(self, value: str) -> dict:
        return {"$regex": Regex.from_native(compile(f"^{value}"))}

    def istartswith(self, value: str) -> dict:
        return {"$regex": Regex.from_native(compile(f"^{value}", IGNORECASE))}

    def not_startswith(self, value: str) -> dict:
        return {"$not": Regex.from_native(compile(f"^{value}"))}

    def not_istartswith(self, value: str) -> dict:
        return {"$not": Regex.from_native(compile(f"^{value}", IGNORECASE))}

    def endswith(self, value: str) -> dict:
        return {"$regex": Regex.from_native(compile(f"{value}$"))}

    def iendswith(self, value: str) -> dict:
        return {"$regex": Regex.from_native(compile(f"{value}$", IGNORECASE))}

    def not_endswith(self, value: str) -> dict:
        return {"$not": Regex.from_native(compile(f"{value}$"))}

    def nin(self, list_values: List) -> dict:
        if not isinstance(list_values, list):
            raise TypeError("values must be a list type")
        try:
            return {
                "$nin": [
                    validate_field_value(self.document, self.field_name, v)
                    for v in list_values
                ]
            }
        except QueryValidationError:
            return {"$nin": list_values}

    def exists(self, boolean_value: bool) -> dict:
        if not isinstance(boolean_value, bool):
            raise TypeError("boolean_value must be a bool type")
        return {"$exists": boolean_value}

    def type(self, bson_type) -> dict:
        return {"$type": bson_type}

    def search(self, search_text: str) -> dict:
        return {"$search": search_text}

    def all(self, query: Any) -> dict:
        return {"$all": query}

    def unset(self, value: Any) -> dict:
        return {"$unset": {self.field_name: value}}

    def gte(self, value: Any) -> dict:
        return {"$gte": validate_field_value(self.document, self.field_name, value)}

    def lte(self, value: Any) -> dict:
        return {"$lte": validate_field_value(self.document, self.field_name, value)}

    def gt(self, value: Any) -> dict:
        return {"$gt": validate_field_value(self.document, self.field_name, value)}

    def lt(self, value: Any) -> dict:
        return {"$lt": validate_field_value(self.document, self.field_name, value)}

    def inc(self, value: int) -> dict:
        if isinstance(value, int):
            return {"$inc": {self.field_name: value}}
        raise ValueError("value must be integer")

    def range(self, range_values: Union[List, Tuple]) -> dict:
        if len(range_values) != 2:
            raise ValueError("range must have 2 params")
        from_ = range_values[0]
        to_ = range_values[1]
        return {
            "$gte": validate_field_value(self.document, self.field_name, from_),
            "$lte": validate_field_value(self.document, self.field_name, to_),
        }

    @cached_classproperty
    def methods(cls) -> list:
        methods = []
        for f in cls.__dict__:
            if f == "in_":
                methods.append("in")
            elif not f.startswith("__") and f != "query":
                methods.append(f)
        return methods


def group_by_aggregate_generation(
    group_by: Union[str, list, tuple]
) -> Union[str, dict]:
    """group by parametr generation helper"""

    if isinstance(group_by, (list, tuple)):
        return {
            g if "." not in g else g.split(".")[-1]: f"${g}" if "$" not in g else g
            for g in group_by
        }
    if "." in group_by:
        name = group_by.split(".")[-1]
        return {name: f"${group_by}"}
    return f"${group_by}" if not "$" in group_by else group_by


def generate_name_field(name: Union[dict, str, None] = None) -> Optional[str]:
    if isinstance(name, dict):
        return "|".join(str(v) for v in name.values())
    return name


def bulk_query_generator(
    requests: List,
    updated_fields: Optional[List] = None,
    query_fields: Optional[List] = None,
    upsert=False,
) -> List:
    """ "helper for generate bulk query"""

    data = []
    if updated_fields:
        for obj in requests:
            query = {"_id": ObjectId(obj._id)}
            update = {}
            for field in updated_fields:
                value = getattr(obj, field)
                update.update({field: value})
            data.append(UpdateOne(query, {"$set": update}, upsert=upsert))
    elif query_fields:
        for obj in requests:
            query = {}
            update = {}
            for field, value in obj.data.items():
                if field not in query_fields:
                    update.update({field: value})
                else:
                    query.update({field: value})
            data.append(UpdateOne(query, {"$set": update}, upsert=upsert))
    return data
