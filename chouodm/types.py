from typing_extensions import Annotated
from typing import TYPE_CHECKING, Generic, Optional, TypeVar, Union

from msgspec import Meta, Struct, structs

from bson import ObjectId, DBRef

from .typing import DocumentType

if TYPE_CHECKING:
    from .document import Document

ObjectIdType = Annotated[
    ObjectId, Meta(description="_id", title="id", extra_json_schema={})
]

T = TypeVar("T")


class Relation(Generic[T]):
    def __init__(self, db_ref: DBRef, document_class: Optional[DocumentType] = None):
        self.db_ref = db_ref
        self.document_class = document_class

    async def get(self) -> Optional["Document"]:
        result = await self.document_class.Q().find_one(_id=self.db_ref.id, with_relations_objects=True)  # type: ignore
        return result

    @classmethod
    def from_db_ref(cls, db_ref: DBRef) -> "Relation":
        return cls(db_ref=db_ref, document_class=cls.__annotations__["document_class"])

    def to_db_ref(self) -> DBRef:
        return self.db_ref

    def to_dict(self) -> dict:
        return {"id": self.db_ref.id, "collection": self.db_ref.collection}

    @classmethod
    def _validate_for_model(
        cls, v: Union[dict, Struct], document_class: DocumentType
    ) -> "Relation":
        parsed = (
            document_class.parse_obj(v)
            if isinstance(v, dict)
            else document_class.parse_obj(structs.asdict(v))
        )
        new_id = (
            parsed._id if isinstance(parsed._id, ObjectId) else ObjectId(parsed._id)
        )
        db_ref = DBRef(collection=document_class.get_collection_name(), id=new_id)
        return cls(db_ref=db_ref, document_class=document_class)

    @classmethod
    def validate(cls, v: Union[DBRef, T], document_class: DocumentType) -> "Relation":
        if isinstance(v, DBRef):
            return cls(db_ref=v, document_class=document_class)
        if isinstance(v, Relation):
            return v
        if isinstance(v, dict):
            try:
                return cls(db_ref=DBRef(**v), document_class=document_class)
            except TypeError:
                return cls._validate_for_model(v, document_class)
        if isinstance(v, Struct):
            return cls._validate_for_model(v, document_class)
        raise ValueError(f"invalod type - {v}")
