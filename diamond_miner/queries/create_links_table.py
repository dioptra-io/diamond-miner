from dataclasses import dataclass

from diamond_miner.defaults import DEFAULT_SUBSET
from diamond_miner.queries import CreateFlowsView
from diamond_miner.queries.query import Query
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class CreateLinksTable(Query):
    """
    Create the links table containing one line per (flow, link) pair.

    >>> from diamond_miner.test import execute
    >>> from diamond_miner.queries.create_results_table import CreateResultsTable
    >>> from diamond_miner.queries.create_flows_view import CreateFlowsView
    >>> from diamond_miner.queries.get_links_from_view import GetLinksFromView
    >>> _ = execute("DROP TABLE IF EXISTS results_test")
    >>> _ = execute("DROP TABLE IF EXISTS flows_test")
    >>> _ = execute(CreateResultsTable(), "results_test")
    >>> _ = execute(CreateFlowsView(parent="results_test"), "flows_test")
    >>> _ = execute(CreateLinksTable(), "links_test")
    >>> _ = execute("INSERT INTO results_test SELECT * FROM test_nsdi_example")
    >>> sql = GetLinksFromView().query("flows_test")
    >>> _ = execute(f"INSERT INTO links_test SELECT * FROM ({sql})")
    """

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
                ORDER BY ({CreateFlowsView.FLOW_COLUMNS}, near_ttl)
        """
