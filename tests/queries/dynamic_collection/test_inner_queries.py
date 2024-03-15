import pytest_asyncio
import pytest

from chouodm.document import DynamicCollectionDocument

collection_name = "dynamic_collection_name_1"


class DynamicInnerTicket(DynamicCollectionDocument):
    name: str
    position: int
    config: dict
    params: dict
    sign: int = 1
    type_: str = "ga"
    array: list = []

    class Config:
        database_exclude_fields = ("sign", "type")


@pytest_asyncio.fixture(scope="session", autouse=True)
async def inner_tickets(event_loop):

    await DynamicInnerTicket.Q(collection_name).insert_one(
        name="first",
        position=1,
        config={"url": "localhost", "username": "admin"},
        params={},
        array=["1", "type"],
    )
    await DynamicInnerTicket.Q(collection_name).insert_one(
        name="second",
        position=2,
        config={"url": "google.com", "username": "staff"},
        params={"1": 2},
        array=["second", "type2"],
    )
    await DynamicInnerTicket.Q(collection_name).insert_one(
        name="third",
        position=3,
        config={"url": "yahoo.com", "username": "trololo"},
        params={"1": 1},
        array=["third", "type3"],
    )
    await DynamicInnerTicket.Q(collection_name).insert_one(
        name="fourth",
        position=4,
        config={"url": "yahoo.com", "username": "trololo"},
        params={"2": 2},
        array=["fourth", "type4"],
    )
    yield
    await DynamicInnerTicket.Q(collection_name).drop_collection(force=True)


@pytest.mark.asyncio
async def test_update_many(connection):
    # .create_documents()
    updated = await DynamicInnerTicket.Q(collection_name).update_many(
        position__range=[3, 4], name__ne="hhh", config__url__set="test.io"
    )
    assert updated == 2
    last = await DynamicInnerTicket.Q(collection_name).find_one(position=4)
    assert last.config["url"] == "test.io"


@pytest.mark.asyncio
async def test_inner_find_one_in_array(connection):
    data = await DynamicInnerTicket.Q(collection_name).find_one(array__1__regex="2")
    assert data.name == "second"

    data = await DynamicInnerTicket.Q(collection_name).find_one(
        array__1__regex="2", array__0__regex="1"
    )
    assert data is None

    data = await DynamicInnerTicket.Q(collection_name).find_one(array__0="1")
    assert data.name == "first"


@pytest.mark.asyncio
async def test_inner_find_one(connection):
    data = await DynamicInnerTicket.Q(collection_name).find_one(
        config__url__startswith="google", params__1=2
    )
    assert data.name == "second"

    data = await DynamicInnerTicket.Q(collection_name).find_one(
        config__url__startswith="yahoo", params__1="qwwe"
    )
    assert data is None


@pytest.mark.asyncio
async def test_inner_update_one(connection):
    updated = await DynamicInnerTicket.Q(collection_name).update_one(
        config__url__startswith="goo", config__url__set="test.io"
    )
    assert updated == 1
    data = await DynamicInnerTicket.Q(collection_name).find_one(
        config__url__startswith="test"
    )
    assert data.name == "second"
