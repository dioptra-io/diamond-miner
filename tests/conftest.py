import asyncio

import pytest
from aioch import Client

from diamond_miner.test import insert_test_data


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
async def client():
    client = Client("127.0.0.1")
    await insert_test_data(client)
    return client
