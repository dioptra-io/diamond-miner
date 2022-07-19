from collections.abc import Sequence
from dataclasses import dataclass

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries.query import (
    Query,
    links_table,
    prefixes_table,
    probes_table,
    results_table,
)
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class DropTables(Query):
    """
    Drop the tables associated to a measurement.

    Examples:
        >>> from diamond_miner.test import client
        >>> from diamond_miner.queries import DropTables
        >>> DropTables().execute(client, "test")
        []
    """

    def statements(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> Sequence[str]:
        return (
            f"DROP TABLE IF EXISTS {results_table(measurement_id)}",
            f"DROP TABLE IF EXISTS {links_table(measurement_id)}",
            f"DROP TABLE IF EXISTS {prefixes_table(measurement_id)}",
            f"DROP TABLE IF EXISTS {probes_table(measurement_id)}",
        )
