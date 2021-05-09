from dataclasses import dataclass

from diamond_miner.defaults import DEFAULT_SUBSET
from diamond_miner.queries.query import Query
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class CreateFlowsView(Query):
    """Create the flows view."""

    SORTING_KEY = "probe_protocol, probe_src_addr, probe_dst_prefix, probe_dst_addr, probe_src_port, probe_dst_port"
    parent: str = ""

    def query(self, table: str, subset: IPNetwork = DEFAULT_SUBSET) -> str:
        assert subset == DEFAULT_SUBSET, "subset not allowed for this query"
        assert self.parent
        return f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table}
        ENGINE = AggregatingMergeTree
        ORDER BY ({self.SORTING_KEY})
        AS SELECT
            round,
            {self.SORTING_KEY},
            -- The duplication of the round column here allows us to drop `round`
            -- from the GROUP BY clause when querying the view, and still get the
            -- round information in the replies. This is useful for computing
            -- inter-round links.
            groupUniqArrayState((round, probe_ttl_l4, reply_src_addr)) AS replies
        FROM {self.parent}
        WHERE {self.common_filters(subset)}
        GROUP BY (round, {self.SORTING_KEY})
        """
