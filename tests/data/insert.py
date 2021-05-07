#!/usr/bin/env python3
import re
from pathlib import Path

from clickhouse_driver import Client

from diamond_miner.queries import (
    CreateFlowsView,
    CreateLinksTable,
    CreateResultsTable,
    GetLinksFromView,
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
    results_table = get_table_name(file)
    flows_view = f"{results_table}_flows"
    links_table = f"{results_table}_links"
    print(f"Processing {file.name} -> {results_table}")
    client.execute(f"DROP TABLE IF EXISTS {results_table}")
    client.execute(f"DROP TABLE IF EXISTS {flows_view}")
    client.execute(f"DROP TABLE IF EXISTS {links_table}")
    # Create results table + flows view
    CreateResultsTable().execute(client, results_table)
    CreateFlowsView(parent=results_table).execute(client, flows_view)
    for statement in get_statements(file):
        client.execute(statement)
    # Create links table from flows view
    CreateLinksTable().execute(client, links_table)
    client.execute(
        f"""
        INSERT INTO {links_table}
        SELECT * FROM ({GetLinksFromView().query(flows_view)})
        """
    )


if __name__ == "__main__":
    client = Client("127.0.0.1")
    files = Path(__file__).parent.glob("*.sql")
    files = sorted(files, key=get_rank)
    for file in files:
        insert_file(client, file)
