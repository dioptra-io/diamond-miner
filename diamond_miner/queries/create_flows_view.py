from dataclasses import dataclass

from diamond_miner.defaults import DEFAULT_SUBSET
from diamond_miner.queries.query import Query
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class CreateFlowsView(Query):
    """
    Create the flow view.
    Currently this query is tested indirectly in GetLinksFromView.
    """

    FLOW_COLUMNS = "round, probe_protocol, probe_src_addr, probe_dst_prefix, probe_dst_addr, probe_src_port, probe_dst_port"
    parent: str = ""

    def query(self, table: str, subset: IPNetwork = DEFAULT_SUBSET) -> str:
        assert subset == DEFAULT_SUBSET, "subset not allowed for this query"
        assert self.parent
        return f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table}
        ENGINE = AggregatingMergeTree
        ORDER BY ({self.FLOW_COLUMNS})
        AS SELECT
            {self.FLOW_COLUMNS},
            groupUniqArrayState((probe_ttl_l4, reply_src_addr)) AS replies
        FROM {self.parent}
        WHERE {self.common_filters(subset)}
        GROUP BY ({self.FLOW_COLUMNS})
        """
