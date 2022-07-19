from collections.abc import Sequence
from dataclasses import dataclass, fields

from diamond_miner.defaults import (
    DEFAULT_PREFIX_LEN_V4,
    DEFAULT_PREFIX_LEN_V6,
    UNIVERSE_SUBSET,
)
from diamond_miner.queries import (
    CreateLinksTable,
    CreatePrefixesTable,
    CreateProbesTable,
    CreateResultsTable,
)
from diamond_miner.queries.query import Query, StoragePolicy
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class CreateTables(Query):
    """
    Create the tables necessary for a measurement.

    Examples:
        >>> from diamond_miner.test import client
        >>> from diamond_miner.queries import CreateTables
        >>> CreateTables().execute(client, "test")
        []
    """

    prefix_len_v4: int = DEFAULT_PREFIX_LEN_V4
    "The prefix length used to compute the IPv4 prefix of an IP address."

    prefix_len_v6: int = DEFAULT_PREFIX_LEN_V6
    "The prefix length used to compute the IPv6 prefix of an IP address."

    storage_policy: StoragePolicy = StoragePolicy()
    "ClickHouse storage policy to use."

    def statements(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> Sequence[str]:
        all_params = {field.name: getattr(self, field.name) for field in fields(self)}
        # Only CreateResultsTable accepts these parameters.
        params = {
            x: y
            for x, y in all_params.items()
            if x not in ["prefix_len_v4", "prefix_len_v6"]
        }
        return (
            *CreateResultsTable(**all_params).statements(measurement_id, subset),
            *CreateLinksTable(**params).statements(measurement_id, subset),
            *CreatePrefixesTable(**params).statements(measurement_id, subset),
            *CreateProbesTable(**params).statements(measurement_id, subset),
        )
