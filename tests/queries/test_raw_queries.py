import uuid

import pytest_asyncio
import pytest

from bson import ObjectId

from chouodm.document import Document
from chouodm.errors import QueryValidationError


class User(Document):
    id: uuid.UUID
    name: str
    email: str

    class Config:
        excluded_query_fields = ("sign", "type")


@pytest_asyncio.fixture(scope="session", autouse=True)
async def manage_users(event_loop):
    yield
    await User.Q().drop_collection(force=True)


@pytest.mark.asyncio
async def test_raw_insert_one(connection):
    with pytest.raises(QueryValidationError):
        result = await User.Q().raw_query(
            "insert_one", {"id": uuid.uuid4(), "name": {}, "email": []}
        )
    result = await User.Q().raw_query(
        "insert_one",
        {"id": uuid.uuid4(), "name": "first", "email": "first@mail.ru"},
    )
    assert isinstance(result.inserted_id, ObjectId)

    r = await User.Q().find_one()
    # assert r == 1
