import pytest
from clickhouse_driver import Client

from diamond_miner.test import insert_test_data


@pytest.fixture(scope="session")
def client():
    client = Client("127.0.0.1")
    insert_test_data(client)
    return client
