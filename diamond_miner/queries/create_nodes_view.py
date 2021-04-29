from dataclasses import dataclass

from diamond_miner.defaults import DEFAULT_SUBSET
from diamond_miner.queries.fragments import IPNetwork
from diamond_miner.queries.query import Query

# TODO: Handle protocol


@dataclass(frozen=True)
class CreateNodesView(Query):
    """
    Create the nodes view.

    >>> from diamond_miner.test import execute
    >>> from diamond_miner.queries.create_results_table import CreateResultsTable
    >>> _ = execute(CreateResultsTable(), "results_test")
    >>> execute(CreateNodesView(results_table="results_test"), "nodes_test")
    []
    """

    results_table: str = ""

    def query(self, table: str, subset: IPNetwork = DEFAULT_SUBSET) -> str:
        assert self.results_table
        return f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table}
        ENGINE = AggregatingMergeTree
        ORDER BY (reply_src_addr)
        AS SELECT
            reply_src_addr,
            groupUniqArrayState(probe_ttl_l4) AS ttls,
            avgState(rtt)                     AS avg_rtt,
            minState(rtt)                     AS min_rtt,
            maxState(rtt)                     AS max_rtt
        FROM {self.results_table}
        WHERE reply_src_addr != probe_dst_addr
        AND private_reply_src_addr = 0
        AND time_exceeded_reply = 1
        GROUP BY reply_src_addr
        SETTINGS optimize_aggregation_in_order = 1
        """
