from dataclasses import dataclass

from diamond_miner.defaults import DEFAULT_SUBSET
from diamond_miner.queries import CreateFlowsView
from diamond_miner.queries.query import Query
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class GetLinksFromView(Query):
    """
    Compute the links from the flows view.
    This returns one line per (flow, link) pair.

    TODO: Better test.

    >>> from diamond_miner.test import execute
    >>> from diamond_miner.queries.create_results_table import CreateResultsTable
    >>> _ = execute("DROP TABLE IF EXISTS results_test")
    >>> _ = execute("DROP TABLE IF EXISTS flows_test")
    >>> _ = execute(CreateResultsTable(), "results_test")
    >>> _ = execute(CreateFlowsView(parent="results_test"), "flows_test")
    >>> _ = execute("INSERT INTO results_test SELECT * FROM test_nsdi_example")
    >>> links = execute(GetLinksFromView(), "flows_test")
    >>> len(links)
    58
    """

    def query(self, table: str, subset: IPNetwork = DEFAULT_SUBSET) -> str:
        # TODO: Include * - node and node - *
        return f"""
        WITH
            groupUniqArrayMerge(replies) AS traceroute,
            arraySort(x -> x.1, traceroute) AS traceroute_sorted,
            arrayPushBack(arrayPopFront(traceroute_sorted), (0, toIPv6('::'))) AS traceroute_shifted,
            arrayZip(traceroute_sorted, traceroute_shifted) AS candidates,
            arrayFilter(x -> x.1.1 + 1 == x.2.1, candidates) AS links,
            arrayJoin(links) AS link
        SELECT
            {CreateFlowsView.FLOW_COLUMNS},
            link.1.1 AS near_ttl,
            link.1.2 AS near_addr,
            link.2.2 AS far_addr
        FROM {table}
        GROUP BY ({CreateFlowsView.FLOW_COLUMNS})
        SETTINGS optimize_aggregation_in_order = 1
        """
