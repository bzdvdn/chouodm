import pytest
import pytest_asyncio

from bson import ObjectId
from msgspec import field

from motor.motor_asyncio import AsyncIOMotorClientSession
from chouodm.document import DynamicCollectionDocument
from chouodm.session import Session

ticked_collection_name = "dynamic_ticket"
trash_collection_name = "dynamic_trash"


class DynamicTicket(DynamicCollectionDocument):
    name: str
    position: int
    config: dict
    sign: int = 1
    type_: str = "ga"
    array: list = field(default_factory=list)

    @property
    def position_property(self) -> int:
        return self.position


class DynamicTrash(DynamicCollectionDocument):
    name: str
    date: str


@pytest_asyncio.fixture(scope="session", autouse=True)
async def drop_collection(event_loop):
    yield
    await DynamicTicket.Q(ticked_collection_name).drop_collection(force=True)
    await DynamicTrash.Q(trash_collection_name).drop_collection(force=True)


@pytest.mark.asyncio
async def test_insert_one(connection):
    data = {
        "name": "second",
        "position": 2,
        "config": {"param1": "value2"},
    }
    object_id = await DynamicTicket.Q(ticked_collection_name).insert_one(**data)
    assert isinstance(object_id, ObjectId)


@pytest.mark.asyncio
async def test_insert_many(connection):
    data = [
        {
            "name": "first",
            "position": 1,
            "config": {"param1": "value"},
            "array": ["test", "adv", "calltouch"],
        },
        {
            "name": "third",
            "position": 3,
            "config": {"param1": "value3"},
            "array": ["test", "adv", "comagic", "cost"],
        },
        {
            "name": "third",
            "position": 4,
            "config": {"param1": "value3"},
            "array": ["test", "adv", "comagic", "cost", "trash"],
        },
    ]
    inserted = await DynamicTicket.Q(ticked_collection_name).insert_many(data)
    assert inserted == 3


@pytest.mark.asyncio
async def test_find_one(connection):
    data = await DynamicTicket.Q(ticked_collection_name).find_one(name="second")
    second = await DynamicTicket.Q(ticked_collection_name).find_one(_id=data._id)
    assert isinstance(data, DynamicCollectionDocument)
    assert data.name == "second"  # type: ignore
    assert data.position == 2  # type: ignore
    assert second._id == data._id
    assert second.position_property == 2
    assert second.data["position_property"] == 2


@pytest.mark.asyncio
async def test_distinct(connection):
    data = await DynamicTicket.Q(ticked_collection_name).distinct(
        "position", name="second"
    )
    assert data == [2]


@pytest.mark.asyncio
async def test_find(connection):
    result = await DynamicTicket.Q(ticked_collection_name).find(
        limit_rows=1, name="second"
    )
    assert len(result.data) == 1


@pytest.mark.asyncio
async def test_update_one(connection):
    updated = await DynamicTicket.Q(ticked_collection_name).update_one(
        name="second", config__set={"updated": 1}
    )
    data = await DynamicTicket.Q(ticked_collection_name).find_one(name="second")
    assert updated == 1
    assert data.config == {"updated": 1}


@pytest.mark.asyncio
async def test_update_many(connection):
    data = await DynamicTicket.Q(ticked_collection_name).update_many(
        name="third", config__set={"updated": 3}
    )
    updated = await DynamicTicket.Q(ticked_collection_name).find_one(name="third")
    assert data == 2
    assert updated.config == {"updated": 3}


@pytest.mark.asyncio
async def test_save(connection):
    obj = await DynamicTicket.Q(ticked_collection_name).find_one(name="second")
    obj.position = 2310
    obj.name = "updated"
    await obj.save(ticked_collection_name)
    none_obj = await DynamicTicket.Q(ticked_collection_name).find_one(
        name="second", position=222222
    )
    assert none_obj is None
    new_obj = await DynamicTicket.Q(ticked_collection_name).find_one(_id=obj._id)
    assert new_obj.name == "updated"
    assert new_obj.position == 2310

    obj.name = "second"
    obj.position = 2

    await obj.save(ticked_collection_name)
    last_obj = await DynamicTicket.Q(ticked_collection_name).find_one(name="second")
    assert last_obj.name == "second"
    assert last_obj.position == 2


@pytest.mark.asyncio
async def test_queryset_serialize(connection):
    result = await DynamicTicket.Q(ticked_collection_name).find(name="second")
    data = result.serialize(fields=["name", "config"])
    assert len(data[0]) == 2
    assert data[0]["config"] == {"updated": 1}
    assert data[0]["name"] == "second"
    assert isinstance(data, list)


@pytest.mark.asyncio
async def test_gte_lte_in_one_field(connection):

    await DynamicTrash.Q(trash_collection_name).insert_one(
        name="first", date="2022-01-01"
    )
    await DynamicTrash.Q(trash_collection_name).insert_one(
        name="second", date="2022-01-01"
    )
    await DynamicTrash.Q(trash_collection_name).insert_one(
        name="third", date="2022-01-03"
    )
    await DynamicTrash.Q(trash_collection_name).insert_one(name="4", date="2022-01-05")

    data = await DynamicTrash.Q(trash_collection_name).count(
        date__gte="2022-01-01",
        date__lte="2022-01-03",
    )
    assert data == 3


@pytest.mark.asyncio
async def test_find_with_regex(connection):
    third = await DynamicTicket.Q(ticked_collection_name).find_one(name__regex="ird")
    assert third.name == "third"

    starts = await DynamicTicket.Q(ticked_collection_name).find_one(
        name__startswith="t"
    )
    assert starts._id == third._id

    istarts = await DynamicTicket.Q(ticked_collection_name).find_one(
        name__istartswith="T"
    )
    assert istarts._id == third._id

    not_istarts = await DynamicTicket.Q(ticked_collection_name).find_one(
        name__not_istartswith="S", position=1
    )
    assert not_istarts.name == "first"


@pytest.mark.asyncio
async def test_find_and_update(connection):
    data_default = await DynamicTicket.Q(ticked_collection_name).find_one_and_update(
        name="second", position__set=23
    )
    assert data_default.position == 23

    data_with_prejection = await DynamicTicket.Q(
        ticked_collection_name
    ).find_one_and_update(
        name="first", position__set=12, projection_fields=["position"]
    )
    assert isinstance(data_with_prejection, dict)
    assert data_with_prejection["position"] == 12


@pytest.mark.asyncio
async def test_session_start(connection):
    session = await DynamicTicket.manager._start_session()
    assert isinstance(session, AsyncIOMotorClientSession)
    await session.end_session()  # type: ignore


@pytest.mark.asyncio
async def test_session_find(connection):
    session = await DynamicTicket.manager._start_session()
    ticket = await DynamicTicket.Q(ticked_collection_name).find_one(session=session)
    assert ticket is not None


@pytest.mark.asyncio
async def test_session_context(connection):
    async with Session(DynamicTicket.manager) as session:
        ticket = await DynamicTicket.Q(ticked_collection_name).find_one(session=session)
        assert ticket is not None
        new_ticket_id = await DynamicTicket.Q(ticked_collection_name).insert_one(
            name="session ticket",
            position=100,
            config={"param1": "session"},
            session=session,
        )
        assert isinstance(new_ticket_id, ObjectId)

        deleted = await DynamicTicket.Q(ticked_collection_name).delete_one(
            _id=new_ticket_id
        )
        assert deleted == 1


@pytest.mark.asyncio
async def test_delete_one(connection):
    deleted = await DynamicTicket.Q(ticked_collection_name).delete_one(name="second")
    assert deleted == 1


@pytest.mark.asyncio
async def test_delete_many(connection):
    deleted = await DynamicTicket.Q(ticked_collection_name).delete_many(name="third")
    assert deleted == 2
