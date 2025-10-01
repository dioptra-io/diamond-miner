from os import environ

from pych_client import ClickHouseClient

# TODO: Rename to base_url
url = environ.get("DIAMOND_MINER_TEST_DATABASE_URL", "http://localhost:8123")

base_url = environ.get("DIAMOND_MINER_TEST_DATABASE_URL", "http://localhost:8123")
client = ClickHouseClient(base_url=base_url, database="default", username="default")


def create_tables(client: ClickHouseClient, measurement_id: str) -> None:
    # Avoid circular imports.
    from diamond_miner.queries.create_tables import CreateTables
    from diamond_miner.queries.drop_tables import DropTables

    DropTables().execute(client, measurement_id)
    CreateTables().execute(client, measurement_id)
