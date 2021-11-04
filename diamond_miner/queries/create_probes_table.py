from dataclasses import dataclass

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries.fragments import date_time
from diamond_miner.queries.query import Query, StoragePolicy, probes_table
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class CreateProbesTable(Query):
    """Create the table containing the cumulative number of probes sent over the rounds."""

    SORTING_KEY = "probe_protocol, probe_dst_prefix, probe_ttl"

    storage_policy: StoragePolicy = StoragePolicy()

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {probes_table(measurement_id)}
        (
            probe_protocol         UInt8,
            probe_dst_prefix       IPv6,
            probe_ttl              UInt8,
            cumulative_probes      UInt32,
            round                  UInt8
        )
        ENGINE MergeTree
        ORDER BY ({self.SORTING_KEY})
        TTL {date_time(self.storage_policy.archive_on)} TO VOLUME '{self.storage_policy.archive_to}'
        SETTINGS storage_policy = '{self.storage_policy.name}'
        """
