#!/usr/bin/env python3
import re
from pathlib import Path

from clickhouse_driver import Client

from diamond_miner.queries import (
    CreateFlowsView,
    CreateLinksTable,
    CreateResultsTable,
    GetLinksFromView,
    flows_table,
    links_table,
    results_table,
)


def get_rank(file: Path):
    """
    >>> get_rank(Path("42_test_example.sql"))
    42
    """
    return int(re.match(r"(\d+)_.+", file.name).group(1))


def get_table_name(file: Path):
    """
    >>> get_table_name(Path("42_test_example.sql"))
    'test_example'
    """
    return re.match(r"\d+_(.+)\.sql", file.name).group(1)


def get_statements(file: Path):
    # We omit the last item, since it's an empty statement:
    # "stmt1;stmt2;".split(";") = ["stmt1", "stmt2", ""]
    return file.read_text().split(";")[:-1]


def insert_file(client: Client, file: Path):
    measurement_id = get_table_name(file)
    results_table_name = results_table(measurement_id)
    flows_table_name = flows_table(measurement_id)
    links_table_name = links_table(measurement_id)
    print(f"Processing {file.name} -> {results_table_name}")
    client.execute(f"DROP TABLE IF EXISTS {results_table_name}")
    client.execute(f"DROP TABLE IF EXISTS {flows_table_name}")
    client.execute(f"DROP TABLE IF EXISTS {links_table_name}")
    # Create results table + flows view
    CreateResultsTable().execute(client, measurement_id)
    CreateFlowsView().execute(client, measurement_id)
    for statement in get_statements(file):
        client.execute(statement)
    # Create links table from flows view
    CreateLinksTable().execute(client, measurement_id)
    client.execute(
        f"""
        INSERT INTO {links_table_name}
        SELECT * FROM ({GetLinksFromView().query(measurement_id)})
        """
    )


if __name__ == "__main__":
    client = Client("127.0.0.1")
    files = Path(__file__).parent.glob("*.sql")
    files = sorted(files, key=get_rank)
    for file in files:
        insert_file(client, file)
