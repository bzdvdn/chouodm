import asyncio

import pytest
from chouodm.connection import connect


@pytest.fixture(scope="session")
async def connection():
    connection = connect("mongodb://127.0.0.1:27017", "test")
    yield connection


@pytest.fixture(scope="session")
def event_loop():
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    yield loop
    loop.close()
