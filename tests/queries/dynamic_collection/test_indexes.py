import pytest
import pytest_asyncio
from pymongo import IndexModel

from chouodm.document import DynamicCollectionDocument
from chouodm.errors import ODMIndexError

index_ticket_collection = "dynamic_index_ticket"


class DynamicIndexTicket(DynamicCollectionDocument):
    name: str
    position: int
    config: dict

    class Config:
        indexes = [
            IndexModel([("position", 1)]),
            IndexModel([("name", 1)]),
        ]


@pytest_asyncio.fixture(scope="session", autouse=True)
async def drop_ticket_collection(event_loop):
    await DynamicIndexTicket.manager.ensure_indexes(index_ticket_collection)
    yield
    await DynamicIndexTicket.Q(index_ticket_collection).drop_collection(force=True)


@pytest.mark.asyncio
async def test_check_indexes(connection):
    result = await DynamicIndexTicket.Q(index_ticket_collection).list_indexes()
    assert result == {
        "_id_": {"key": {"_id": 1}},
        "position_1": {"key": {"position": 1}},
        "name_1": {"key": {"name": 1}},
    }


@pytest.mark.asyncio
async def test_check_indexes_if_remove(connection):
    class IndexTicket(DynamicCollectionDocument):
        name: str
        position: int
        config: dict

        class Config:
            indexes = [
                IndexModel([("position", 1)]),
            ]

    await IndexTicket.manager.ensure_indexes(index_ticket_collection)
    result = await DynamicIndexTicket.Q(index_ticket_collection).list_indexes()
    assert result == {
        "_id_": {"key": {"_id": 1}},
        "position_1": {"key": {"position": 1}},
    }


@pytest.mark.asyncio
async def test_drop_index(connection):
    with pytest.raises(ODMIndexError):
        result = await DynamicIndexTicket.Q(index_ticket_collection).drop_index(
            "position1111"
        )

    result = await DynamicIndexTicket.Q(index_ticket_collection).drop_index(
        "position_1"
    )
    assert result == "position_1 dropped."
