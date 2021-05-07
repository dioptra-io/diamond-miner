import asyncio

import pytest
from aioch import Client as AsyncClient
from clickhouse_driver import Client


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def client():
    return Client("127.0.0.1")


@pytest.fixture(scope="session")
async def async_client():
    return AsyncClient("127.0.0.1")
