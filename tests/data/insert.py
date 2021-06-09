#!/usr/bin/env python3
import asyncio
import re
from pathlib import Path

from aioch import Client

from diamond_miner.database import create_tables, drop_tables, insert_links


def get_rank(file: Path):
    """
    >>> get_rank(Path("42_test_example.sql"))
    42
    """
    return int(re.match(r"(\d+)_.+", file.name).group(1))


def get_measurement_id(file: Path):
    """
    >>> get_measurement_id(Path("42_test_example.sql"))
    'test_example'
    """
    return re.match(r"\d+_(.+)\.sql", file.name).group(1)


def get_statements(file: Path):
    # We omit the last item, since it's an empty statement:
    # "stmt1;stmt2;".split(";") = ["stmt1", "stmt2", ""]
    return file.read_text().split(";")[:-1]


async def insert_file(client: Client, file: Path):
    measurement_id = get_measurement_id(file)
    print(f"Processing {file.name} -> {measurement_id}")
    await drop_tables(client, measurement_id)
    await create_tables(client, measurement_id)
    for statement in get_statements(file):
        await client.execute(statement)
    await insert_links(client, measurement_id)


async def main():
    client = Client("127.0.0.1")
    files = Path(__file__).parent.glob("*.sql")
    files = sorted(files, key=get_rank)
    for file in files:
        await insert_file(client, file)


if __name__ == "__main__":
    asyncio.run(main())
