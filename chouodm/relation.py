import asyncio
from typing import Optional, Any, TYPE_CHECKING, List
from enum import Enum

from msgspec import inspect, Struct

from .typing import DocumentType
from .types import Relation

if TYPE_CHECKING:
    from .document import Document
    from .typing import DictStrList


class RelationInfoTypes(str, Enum):
    SINGLE = "SINGLE"
    OPTIONAL_SINGLE = "OPTIONAL_SINGLE"
    ARRAY = "ARRAY"


class RelationInfo(Struct):
    field: str
    document_class: DocumentType
    relation_type: RelationInfoTypes


class RelationManager(object):
    """relation manager for get and set data from to document instances"""

    __slots__ = ("document_class", "relation_fields")

    def __init__(self, document_class: "Document"):
        self.document_class = document_class
        self.relation_fields = self._get_relation_fields(document_class)

    @classmethod
    def _get_relation_fields(cls, document_class: "Document") -> dict:
        return document_class.__relation_info__ or {}

    def _relation_data_setter(self, document: "Document", data: dict) -> "Document":
        """data setter

        Args:
            document (Document): document insatance
            data (dict): relations objects map

        Returns:
            Document: updated document
        """
        for field, relation_info in self.relation_fields.items():
            relation_attr = getattr(document, field)
            if not relation_attr:
                continue
            if relation_info.relation_type == RelationInfoTypes.ARRAY:
                relation_value = []
                for rel in relation_attr:
                    v = data[field].get(rel.db_ref.id)
                    if v:
                        relation_value.append(v)
            else:
                relation_value = data[field].get(relation_attr.db_ref.id)
            setattr(document, field, relation_value)
        return document

    async def _get_relation_objects_by_document_class(
        self,
        field: str,
        document_class: DocumentType,
        ids: list,
    ) -> dict:
        result = await document_class.Q().find(_id__in=ids, with_relations_objects=True)
        return {field: {str(o._id): o for o in result}}

    async def get_relation_objects(self, pre_relation: "DictStrList") -> dict:
        futures = []
        for field, ids in pre_relation.items():
            document_class = self.relation_fields[field].document_class
            futures.append(
                asyncio.ensure_future(
                    self._get_relation_objects_by_document_class(
                        field, document_class, ids
                    )
                )
            )

        relation_objects = {}
        for _, field_relation_result in enumerate(
            asyncio.as_completed(futures), start=1
        ):
            relation_objects.update(await field_relation_result)
        return relation_objects

    def _get_pre_relation(self, document_instances: List["Document"]) -> "DictStrList":
        pre_relation: "DictStrList" = {field: [] for field in self.relation_fields}
        for document_instance in document_instances:
            for field in self.relation_fields:
                attr = getattr(document_instance, field)
                if isinstance(attr, list):
                    ids = tuple(row.db_ref.id for row in attr)
                else:
                    ids = (attr.db_ref.id,) if attr else tuple()
                if ids:
                    pre_relation[field].extend(ids)
        return {f: list(set(items)) for f, items in pre_relation.items()}

    async def map_relation_for_single(
        self, document_instance: "Document"
    ) -> "Document":
        """map relation data to mongo model instanc

        Args:
            document_instance (Document): list of instances from Document

        Returns:
            Document: mapped document
        """
        pre_relation: "DictStrList" = self._get_pre_relation([document_instance])
        relation_objects = await self.get_relation_objects(pre_relation)
        print(relation_objects)
        return self._relation_data_setter(document_instance, relation_objects)

    async def map_relation_for_array(self, result: List) -> List["Document"]:
        """map relations data for _find method list result

        Args:
            result (List): _find query result converted to list

        Returns:
            List[Document]: mapped list
        """
        pre_relation: "DictStrList" = self._get_pre_relation(result)
        relation_objects = await self.get_relation_objects(pre_relation)
        generated_result = [
            self._relation_data_setter(r, relation_objects) for r in result
        ]
        return generated_result


def _take_relation_info_by_union(
    inspected_type: Any,
    field_type: Any,
    field: str,
    to_array: bool = False,
) -> Optional[RelationInfo]:
    types_ = inspected_type.types
    first_type = types_[0]
    last_type = types_[-1]
    first_type_cls = getattr(first_type, "cls", None)
    if first_type_cls and issubclass(first_type_cls, Relation):
        if to_array:
            relation_type = RelationInfoTypes.ARRAY
        else:
            relation_type = (
                RelationInfoTypes.OPTIONAL_SINGLE
                if isinstance(last_type, inspect.NoneType)
                else RelationInfoTypes.SINGLE
            )
        document_class = field_type.__args__[0].__args__[0]
        return RelationInfo(
            field=field,
            relation_type=relation_type,
            document_class=document_class,
        )
    return None


def _take_relation_info_by_custom_type(
    inspected_type: Any, field_type: Any, field: str, to_array: bool = False
) -> Optional[RelationInfo]:
    cls = inspected_type.cls
    if issubclass(cls, Relation):
        relation_type = (
            RelationInfoTypes.ARRAY if to_array else RelationInfoTypes.SINGLE
        )
        document_class = field_type.__args__[0]
        if hasattr(document_class, "__args__"):
            document_class = document_class.__args__[0]
        return RelationInfo(
            field=field,
            relation_type=relation_type,
            document_class=document_class,
        )
    return None


def take_relation_info(
    inspected_type: Any,
    field_type: Any,
    field: str,
) -> Optional[RelationInfo]:
    if isinstance(inspected_type, inspect.UnionType):
        return _take_relation_info_by_union(
            inspected_type=inspected_type,
            field_type=field_type,
            field=field,
        )
    elif isinstance(inspected_type, inspect.ListType):
        item_type = inspected_type.item_type
        if isinstance(item_type, inspect.CustomType):
            return _take_relation_info_by_custom_type(
                inspected_type=item_type,
                field_type=field_type,
                field=field,
                to_array=True,
            )
        elif isinstance(item_type, inspect.UnionType):
            _take_relation_info_by_union(
                inspected_type=item_type,
                field_type=field_type,
                field=field,
                to_array=True,
            )
        return None
    elif isinstance(inspected_type, inspect.CustomType):
        return _take_relation_info_by_custom_type(
            inspected_type=inspected_type,
            field_type=field_type,
            field=field,
            to_array=False,
        )
    return None
