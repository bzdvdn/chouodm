from typing import Set, Optional, Union, ClassVar, Union, Tuple, List, TYPE_CHECKING

from msgspec import Struct, json, structs, inspect
from bson import ObjectId
from motor.core import AgnosticClientSession
from bson.raw_bson import RawBSONDocument
from bson import decode as bson_decode, DBRef

from pymongo import IndexModel

from .manager import ODMManager
from .property import classproperty
from .hooks import enc_hook, dec_hook
from .errors import QueryValidationError
from .config import BaseConfig
from .types import ObjectIdType
from .relation import take_relation_info, Relation, RelationInfoTypes

if TYPE_CHECKING:
    from .query import QueryBuilder
    from .sync import SyncQueryBuilder


class Document(Struct, kw_only=True, forbid_unknown_fields=True):  # type: ignore
    __indexes__: ClassVar[Set] = set()
    __database_exclude_fields__: ClassVar[Union[list, tuple]] = tuple()
    __collection_name__: ClassVar[Optional[str]] = None
    __relation_info__: ClassVar[dict] = {}
    __manager__: ClassVar[ODMManager]
    has_relations: ClassVar[bool] = False
    _id: Optional[ObjectIdType] = None

    # config
    Config = BaseConfig

    def __init_subclass__(cls, *args, **kwargs):
        super().__init_subclass__(*args, **kwargs)
        cls._init_subclass(*args, **kwargs)
        cls.init_manager()

    def validate(self):
        encoded = self.to_json(False)
        return self.from_json(encoded)

    @classmethod
    def _init_subclass(cls, *args, **kwargs):
        relation_infos = {}
        for field, field_type in cls.__annotations__.items():
            if field.startswith("_"):
                continue
            inspected = inspect.type_info(field_type)
            insected_cls = getattr(inspected, "cls", None)
            if insected_cls and issubclass(insected_cls, Document):
                raise ValueError(
                    f"{field} - cant be instance of Document without Relation"
                )
            relation_info = take_relation_info(inspected, field_type, field)
            if relation_info is not None:
                relation_infos[field] = relation_info

        indexes = getattr(cls.Config, "indexes", [])
        if not all([isinstance(index, IndexModel) for index in indexes]):
            raise ValueError("indexes must be list of IndexModel instances")
        exclude_fields = getattr(cls.Config, "database_exclude_fields", tuple())  # type: ignore
        collection_name = getattr(cls.Config, "collection_name", None) or None
        setattr(cls, "__indexes__", indexes)
        setattr(cls, "__database_exclude_fields__", exclude_fields)
        setattr(cls, "__collection_name__", collection_name)
        setattr(cls, "__relation_info__", relation_infos)
        if relation_infos:
            setattr(cls, "has_relations", True)

    @classmethod
    def init_manager(cls):
        setattr(cls, "__manager__", ODMManager(cls))  # type: ignore

    @classproperty
    def __encoder__(cls) -> json.Encoder:
        return json.Encoder(enc_hook=enc_hook)

    @classproperty
    def __decoder__(cls) -> json.Decoder:
        return json.Decoder(cls, dec_hook=dec_hook)

    @classproperty
    def manager(cls):
        return cls.__manager__

    @classmethod
    def Q(cls) -> "QueryBuilder":
        return cls.manager.querybuilder()

    @classmethod
    def Qsync(cls) -> "SyncQueryBuilder":
        return cls.manager.sync_querybuilder()

    def to_dict(self, with_props: bool = True) -> dict:
        data = structs.asdict(self)
        if with_props:
            data.update({prop: getattr(self, prop) for prop in self._get_properties()})
        if self.has_relations:
            for field in self.__relation_info__:
                relation = data[field]
                if relation and not isinstance(relation, dict):
                    data[field] = (
                        relation.to_dict()
                        if not isinstance(relation, list)
                        else [
                            a.to_dict() if not isinstance(a, dict) else a
                            for a in relation
                        ]
                    )
        return {k: v for k, v in data.items()}

    def to_bytes(self, with_props: bool = True) -> bytes:
        data = self.to_dict(with_props=with_props)
        return self.__encoder__.encode(data)

    def to_json(self, with_props: bool = True) -> str:
        return self.to_bytes(with_props).decode("utf-8")

    @classmethod
    def from_json(cls, json_data: Union[str, bytes]) -> "Document":
        return cls.__decoder__.decode(json_data)

    @classmethod
    def from_dict(cls, data: dict) -> "Document":
        if cls.has_relations:
            for field_name, info in cls.__relation_info__.items():
                rel_data = data.get(field_name)
                if not rel_data:
                    if info.relation_type != RelationInfoTypes.OPTIONAL_SINGLE:
                        raise ValueError(f"relation field: {field_name} cant be empty.")
                    else:
                        data[field_name] = None
                        continue
                if isinstance(rel_data, dict):
                    db_rel = Relation.validate(rel_data, info.document_class)
                elif isinstance(rel_data, list):
                    db_rel = [
                        Relation.validate(r, info.document_class) for r in rel_data
                    ]  # type: ignore
                else:
                    db_rel = rel_data.to_relation(rel_data._id)
                data[field_name] = db_rel
        json_data = cls.__encoder__.encode(data)
        return cls.from_json(json_data)

    @classmethod
    def from_bson(cls, bson_raw_data: RawBSONDocument) -> "Document":
        data = bson_decode(bson_raw_data.raw)
        if cls.has_relations:
            for field, info in cls.__relation_info__.items():
                row_data = data[field]
                if row_data:
                    rel = (
                        [Relation.validate(r, info.document_class) for r in row_data]
                        if isinstance(row_data, list)
                        else Relation.validate(row_data, info.document_class)
                    )
                    data[field] = rel
                else:
                    data[field] = None
        return cls(**data)

    @classmethod
    def New(cls, **kwargs):
        return cls.parse_obj(kwargs)

    @classmethod
    def parse_obj(cls, data: dict) -> "Document":
        return cls.from_dict(data)

    @classmethod
    def _get_properties(cls) -> list:
        return [
            prop
            for prop in dir(cls)
            if prop
            not in (
                "__values__",
                "__decoder__",
                "__encoder__",
                "manager",
                "data",
                "pk",
                "schema",
                "_mongo_query_data",
                "fields_all",
            )
            and isinstance(getattr(cls, prop), property)
        ]

    @classproperty
    def schema(cls) -> dict:
        generated_schema = json.schema(cls)
        return cls.__encoder__.encode(generated_schema)

    @classproperty
    def fields_all(cls) -> list:
        fields = [f for f in cls.__struct_fields__ if not f.startswith("__")]
        return_fields = fields + cls._get_properties()
        return return_fields

    def serialize(self, fields: Union[tuple, list]) -> dict:
        data = self.to_dict()
        return {k: data[k] for k in fields}

    def serialize_json(self, fields: Union[tuple, list]) -> str:
        result = self.serialize(fields)
        return json.encode(result).decode("utf-8")

    @property
    def data(self) -> dict:
        return self.to_dict()

    @property
    def _mongo_query_data(self) -> dict:
        data = self.to_dict(with_props=False)
        _ = data.pop("_id")
        return data

    @classmethod
    def get_collection_name(cls) -> str:
        """main method for set collection

        Returns:
            str: collection name
        """
        return cls.__collection_name__ or cls.__name__.lower()

    @classmethod
    def to_db_ref(cls, object_id: Union[str, ObjectId]) -> DBRef:
        if isinstance(object_id, str):
            object_id = ObjectId(object_id)
        db_ref = DBRef(collection=cls.get_collection_name(), id=object_id)
        return db_ref

    @classmethod
    def to_relation(cls, object_id: Union[str, ObjectId]) -> Relation:
        db_ref = cls.to_db_ref(object_id=object_id)
        return Relation(db_ref, cls)

    async def save(
        self,
        updated_fields: Union[Tuple, List] = [],
        session: Optional[AgnosticClientSession] = None,
    ) -> "Document":
        if self._id is not None:
            data = {
                "_id": (
                    self._id if isinstance(self._id, ObjectId) else ObjectId(self._id)
                )
            }
            if updated_fields:
                if not all(field in self.__struct_fields__ for field in updated_fields):
                    raise QueryValidationError("invalid field in updated_fields")
            else:
                updated_fields = self.__struct_fields__
            for field in updated_fields:
                data[f"{field}__set"] = getattr(self, field)
            await self.Q().update_one(
                session=session,
                **data,
            )
            return self
        data = self.to_dict(with_props=False)
        object_id = await self.Q().insert_one(
            session=session,
            **data,
        )
        self._id = object_id
        return self

    def save_sync(
        self,
        updated_fields: Union[Tuple, List] = [],
        session: Optional[AgnosticClientSession] = None,
    ):
        return self.Q().sync._io_loop.run_until_complete(
            self.save(updated_fields, session)
        )


class DynamicCollectionDocument(Document):
    base_collection_name_prefix: str = ""

    @classmethod
    def _init_subclass(cls, *args, **kwargs):
        relation_infos = {}
        for field, field_type in cls.__annotations__.items():
            if field.startswith("_"):
                continue
            inspected = inspect.type_info(field_type)
            insected_cls = getattr(inspected, "cls", None)
            if insected_cls and issubclass(insected_cls, Document):
                raise ValueError(
                    f"{field} - cant be instance of Document without Relation"
                )
            relation_info = take_relation_info(inspected, field_type, field)
            if relation_info is not None:
                raise ValueError(
                    f"{field} - DynamicCollectionDocument cant be using Relation"
                )

        indexes = getattr(cls.Config, "indexes", [])
        if not all([isinstance(index, IndexModel) for index in indexes]):
            raise ValueError("indexes must be list of IndexModel instances")
        exclude_fields = getattr(cls.Config, "database_exclude_fields", tuple())  # type: ignore
        collection_name = getattr(cls.Config, "collection_name", None) or None
        setattr(cls, "__indexes__", indexes)
        setattr(cls, "__database_exclude_fields__", exclude_fields)
        setattr(cls, "__collection_name__", collection_name)
        setattr(cls, "__relation_info__", relation_infos)
        setattr(cls, "has_relations", False)

    @classmethod
    def Q(cls, collection_name: str) -> "QueryBuilder":
        return cls.manager.querybuilder(collection_name)

    @classmethod
    def Qsync(cls, collection_name: str) -> "SyncQueryBuilder":
        return cls.manager.sync_querybuilder(collection_name)

    async def save(self, *args, **kwargs):
        raise AttributeError("save method cant be used in DynamicCollectionDocument")

    def save_sync(self, *args, **kwargs):
        raise AttributeError(
            "save_sync method cant be used in DynamicCollectionDocument"
        )

    @classmethod
    def generate_collection_name(
        cls, values: List[str], name_separator: str = "__"
    ) -> str:
        if not values:
            raise ValueError("values cant be empty")
        values_string = f"{name_separator}".join(values)
        if cls.base_collection_name_prefix:
            collection_name = f"{cls.base_collection_name_prefix}_{values_string}"
        else:
            collection_name = values_string
        return collection_name

    @classmethod
    def get_collection_name(cls) -> str:
        raise AttributeError("get_collection_name cant be used in DynamicCollectionDocument")