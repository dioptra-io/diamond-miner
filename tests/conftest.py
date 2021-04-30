import asyncio

import pytest
from aioch import Client as AsyncClient
from clickhouse_driver import Client

from diamond_miner.test import get_test_data


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def client():
    client = Client("127.0.0.1")
    for statement in get_test_data():
        client.execute(statement)
    return client


@pytest.fixture(scope="session")
async def async_client():
    client = AsyncClient("127.0.0.1")
    for statement in get_test_data():
        await client.execute(statement)
    return client
