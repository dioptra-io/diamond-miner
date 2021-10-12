from dataclasses import dataclass

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries import CreateFlowsView
from diamond_miner.queries.fragments import date_time
from diamond_miner.queries.query import Query, StoragePolicy, links_table
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class CreateLinksTable(Query):
    """Create the links table containing one line per (flow, link) pair."""

    SORTING_KEY = CreateFlowsView.SORTING_KEY

    storage_policy: StoragePolicy = StoragePolicy()

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        assert subset == UNIVERSE_SUBSET, "subset not allowed for this query"
        return f"""
        CREATE TABLE IF NOT EXISTS {links_table(measurement_id)}
        (
            probe_protocol    UInt8,
            probe_src_addr    IPv6,
            probe_dst_prefix  IPv6,
            probe_dst_addr    IPv6,
            probe_src_port    UInt16,
            probe_dst_port    UInt16,
            near_round        UInt8,
            far_round         UInt8,
            near_ttl          UInt8,
            far_ttl           UInt8,
            near_addr         IPv6,
            far_addr          IPv6,
            -- Materialized columns
            is_destination    UInt8 MATERIALIZED (near_addr = probe_dst_addr) OR (far_addr = probe_dst_addr),
            is_inter_round    UInt8 MATERIALIZED near_round != far_round,
            is_partial        UInt8 MATERIALIZED near_addr = toIPv6('::') OR far_addr = toIPv6('::'),
            is_virtual        UInt8 MATERIALIZED near_addr = toIPv6('::') AND far_addr = toIPv6('::')
        )
        ENGINE MergeTree
        ORDER BY ({self.SORTING_KEY})
        TTL {date_time(self.storage_policy.archive_on)} TO VOLUME '{self.storage_policy.archive_to}'
        SETTINGS storage_policy = '{self.storage_policy.name}'
        """
