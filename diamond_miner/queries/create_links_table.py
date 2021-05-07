from dataclasses import dataclass

from diamond_miner.defaults import DEFAULT_SUBSET
from diamond_miner.queries import CreateFlowsView
from diamond_miner.queries.query import Query
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class CreateLinksTable(Query):
    """Create the links table containing one line per (flow, link) pair."""

    SORTING_KEY = CreateFlowsView.SORTING_KEY

    def query(self, table: str, subset: IPNetwork = DEFAULT_SUBSET) -> str:
        assert subset == DEFAULT_SUBSET, "subset not allowed for this query"
        return f"""
        CREATE TABLE IF NOT EXISTS {table}
        (
            round                  UInt8,
            probe_protocol         UInt8,
            probe_src_addr         IPv6,
            probe_dst_prefix       IPv6,
            probe_dst_addr         IPv6,
            probe_src_port         UInt16,
            probe_dst_port         UInt16,
            near_ttl               UInt8,
            near_addr              IPv6,
            far_addr               IPv6
        )
        ENGINE MergeTree
        ORDER BY ({self.SORTING_KEY})
        """
