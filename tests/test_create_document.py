import pytest
import pytest_asyncio

from chouodm.document import Document
from msgspec import Struct


class Config(Struct):
    path: str = "/home/"
    env: str = "test"


class Application(Document):
    name: str
    config: Config
    lang: str

    @property
    def lang_upper(self) -> str:
        return self.lang.upper()


@pytest_asyncio.fixture(scope="session", autouse=True)
async def application_data(connection):
    application = await Application.New(
        name="test", config=Config(), lang="python"
    ).save()
    yield
    await Application.Q().drop_collection(force=True)


def test_schema(connection):
    application = Application(name="test", config=Config(), lang="python")
    assert application.serialize(["lang_upper"]) == {"lang_upper": "PYTHON"}
    assert application.serialize_json(["lang_upper"]) == '{"lang_upper":"PYTHON"}'
    assert (
        application.to_json()
        == '{"name":"test","config":{"path":"/home/","env":"test"},"lang":"python","_id":null,"lang_upper":"PYTHON"}'
    )


@pytest.mark.asyncio
async def test_application_data(connection):
    application = await Application.Q().find_one(name="test")
    data = application.data
    assert data["name"] == "test"
    data = await Application.Q().find_one(config__env="test")
    assert data.name == "test"
    data = await Application.Q().find_one(config__env="invalid")
    assert data is None


@pytest.mark.asyncio
async def test_raise_with_field_mongo_model(connection):
    with pytest.raises(ValueError):

        class Default(Document):
            name: str
            app: Application
