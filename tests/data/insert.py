#!/usr/bin/env python3
import asyncio
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries import InsertLinks, Query
from diamond_miner.queries.create_tables import CreateTables
from diamond_miner.queries.drop_tables import DropTables
from diamond_miner.queries.insert_prefixes import InsertPrefixes
from diamond_miner.test import url
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class QueryFromFile(Query):
    file: Path = Path()

    def statements(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> Sequence[str]:
        # We omit the last item, since it's an empty statement:
        # "stmt1;stmt2;".split(";") = ["stmt1", "stmt2", ""]
        return self.file.read_text().split(";")[:-1]


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


async def insert_file(url: str, file: Path):
    measurement_id = get_measurement_id(file)
    print(f"Processing {file.name} -> {measurement_id}")
    DropTables().execute(url, measurement_id)
    CreateTables().execute(url, measurement_id)
    QueryFromFile(file=file).execute(url, measurement_id)
    InsertPrefixes().execute(url, measurement_id)
    InsertLinks().execute(url, measurement_id)


async def main():
    logging.basicConfig(level=logging.INFO)
    files = Path(__file__).parent.glob("*.sql")
    files = sorted(files, key=get_rank)
    for file in files:
        await insert_file(url, file)


if __name__ == "__main__":
    asyncio.run(main())
