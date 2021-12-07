url = "http://127.0.0.1:8123"


def create_tables(url: str, measurement_id: str) -> None:
    # Avoid circular imports.
    from diamond_miner.queries.create_tables import CreateTables
    from diamond_miner.queries.drop_tables import DropTables

    DropTables().execute(url, measurement_id)
    CreateTables().execute(url, measurement_id)
