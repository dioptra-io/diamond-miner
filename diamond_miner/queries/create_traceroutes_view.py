from dataclasses import dataclass

from diamond_miner.defaults import DEFAULT_SUBSET
from diamond_miner.queries.query import Query
from diamond_miner.typing import IPNetwork

# TODO: Handle protocol


@dataclass(frozen=True)
class CreateTraceroutesView(Query):
    """
    Create the traceroutes view.

    >>> from diamond_miner.test import execute
    >>> from diamond_miner.queries.create_results_table import CreateResultsTable
    >>> _ = execute(CreateResultsTable(), "results_test")
    >>> execute(CreateTraceroutesView(results_table="results_test"), "traceroutes_test")
    []
    """

    results_table: str = ""

    def query(self, table: str, subset: IPNetwork = DEFAULT_SUBSET) -> str:
        assert self.results_table
        return f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table}
        ENGINE = AggregatingMergeTree
        ORDER BY (probe_src_addr, probe_dst_prefix, probe_dst_addr, probe_src_port, probe_dst_port)
        AS SELECT
            probe_src_addr,
            probe_dst_prefix,
            probe_dst_addr,
            probe_src_port,
            probe_dst_port,
            groupArrayInsertAtState(NULL, 32)(reply_src_addr, probe_ttl_l4) AS replies
        FROM {self.results_table}
        WHERE reply_src_addr != probe_dst_addr
        AND private_reply_src_addr = 0
        AND time_exceeded_reply = 1
        GROUP BY (probe_src_addr, probe_dst_prefix, probe_dst_addr, probe_src_port, probe_dst_port)
        SETTINGS optimize_aggregation_in_order = 1
        """
