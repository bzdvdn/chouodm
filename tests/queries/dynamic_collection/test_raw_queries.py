import uuid

import pytest_asyncio
import pytest

from bson import ObjectId

from chouodm.document import DynamicCollectionDocument
from chouodm.errors import QueryValidationError

collection_name = "dynamic_user_123"


class DynamicUser(DynamicCollectionDocument):
    id: uuid.UUID
    name: str
    email: str

    class Config:
        excluded_query_fields = ("sign", "type")


@pytest_asyncio.fixture(scope="session", autouse=True)
async def manage_users(event_loop):
    yield
    await DynamicUser.Q(collection_name).drop_collection(force=True)


@pytest.mark.asyncio
async def test_raw_insert_one(connection):
    with pytest.raises(QueryValidationError):
        result = await DynamicUser.Q(collection_name).raw_query(
            "insert_one", {"id": uuid.uuid4(), "name": {}, "email": []}
        )
    result = await DynamicUser.Q(collection_name).raw_query(
        "insert_one",
        {"id": uuid.uuid4(), "name": "first", "email": "first@mail.ru"},
    )
    assert isinstance(result.inserted_id, ObjectId)

    r = await DynamicUser.Q(collection_name).find_one()
    # assert r == 1
