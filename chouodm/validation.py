from typing import Any, Union, Optional, Tuple, TYPE_CHECKING
from uuid import UUID
from collections.abc import Collection

from bson import ObjectId
from msgspec import Struct, structs

from .errors import QueryValidationError
from .types import Relation


if TYPE_CHECKING:
    from .document import Document
    from .typing import DocumentType


def validate_field_value(
    document: Union["Document", "DocumentType"], name: str, value: Any
) -> Any:
    """field value validtion

    Args:
        document (Union[Document, DocumentType]): document
        name (str): field name
        value (Any): field value
    Raises:
        AttributeError: if not field in fields
        QueryValidationError: if invalid value type

    Returns:
        Any: validated value
    """
    if name == "_id":
        field_type = ObjectId
    else:
        field_type = document.__annotations__.get(name)

    if not field_type:
        raise AttributeError(f"invalid field - {name}")
    origin = getattr(field_type, "__origin__", None)
    if origin == list or origin == Union:
        args = getattr(field_type, "__args__", ())
        if args:
            origin = getattr(args[0], "__origin__", None)

    if origin and issubclass(origin, Relation):
        if isinstance(value, list):
            return [
                Relation.validate(
                    v, document.__relation_info__[name].document_class
                ).to_db_ref()
                for v in value
            ]
        return (
            Relation.validate(
                value, document.__relation_info__[name].document_class
            ).to_db_ref()
            if value
            else None
        )
    if isinstance(value, UUID):
        return str(value)
    elif isinstance(value, Struct):
        return structs.asdict(value)
    elif isinstance(value, field_type):
        return value
    elif field_type == str and isinstance(value, Collection):
        raise QueryValidationError(f"field - {name} for field_type: {field_type}")
    try:
        converted_value = field_type(value)
        return converted_value
    except (TypeError, ValueError) as e:
        raise QueryValidationError(f"field - {name}, native error - {e}")


def sort_validation(
    sort: Optional[int] = None, sort_fields: Union[list, tuple, None] = None
) -> Tuple[Any, ...]:
    if sort is not None:
        if sort not in (1, -1):
            raise ValueError(f"invalid sort value must be 1 or -1 not {sort}")
        if not sort_fields:
            sort_fields = ("_id",)
    return sort, sort_fields
