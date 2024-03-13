from typing import ClassVar

import pytest
import pytest_asyncio

from chouodm.document import DynamicCollectionDocument, Document
from msgspec import Struct


class Config(Struct):
    path: str = "/home/"
    env: str = "test"


class Application(DynamicCollectionDocument):
    __base_collection_name_prefix__: ClassVar[str] = "prefix_"
    name: str
    config: Config
    lang: str

    @property
    def lang_upper(self) -> str:
        return self.lang.upper()


COLLECTION_NAME_PARAMS = ["dynamic", "application", "data", "id-2310"]


@pytest_asyncio.fixture(scope="session", autouse=True)
async def dynamic_application_data(connection):
    collection_name = Application.generate_collection_name(COLLECTION_NAME_PARAMS)
    assert collection_name == "prefix__dynamic__application__data__id-2310"
    application = Application.New(name="test", config=Config(), lang="python")
    await application.save(collection_name)
    yield
    await Application.Q(collection_name).drop_collection(force=True)


def test_schema(connection):
    application = Application(name="test", config=Config(), lang="python")
    assert application.serialize(["lang_upper"]) == {"lang_upper": "PYTHON"}
    assert application.serialize_json(["lang_upper"]) == '{"lang_upper":"PYTHON"}'
    assert (
        application.to_json()
        == '{"name":"test","config":{"path":"/home/","env":"test"},"lang":"python","_id":null,"lang_upper":"PYTHON"}'
    )


@pytest.mark.asyncio
async def test_raise_with_field_mongo_document(connection):
    with pytest.raises(ValueError):

        class Default(DynamicCollectionDocument):
            name: str
            app: Application

    with pytest.raises(ValueError):

        class A(Document):
            a: str

        class NewDefault(DynamicCollectionDocument):
            name: str
            a: A
