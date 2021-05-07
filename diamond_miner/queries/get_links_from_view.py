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
        return f"""
        WITH
            groupUniqArrayMerge(replies) AS traceroute,
            arrayMap(x -> x.1, traceroute) AS ttls,
            arrayMap(x -> x.2, traceroute) AS addrs,
            CAST((ttls, addrs), 'Map(UInt8, IPv6)') AS map,
            arrayReduce('min', ttls) AS first_ttl,
            arrayReduce('max', ttls) AS last_ttl,
            arrayMap(i -> (toUInt8(i), map[toUInt8(i)], map[toUInt8(i + 1)]), range(first_ttl, last_ttl)) AS candidates,
            arrayFilter(x -> x.2 != toIPv6('::') OR x.3 != toIPv6('::'), candidates) AS links,
            arrayJoin(links) AS link
        SELECT
            {CreateFlowsView.FLOW_COLUMNS},
            link.1 AS near_ttl,
            link.2 AS near_addr,
            link.3 AS far_addr
        FROM {table}
        GROUP BY ({CreateFlowsView.FLOW_COLUMNS})
        SETTINGS optimize_aggregation_in_order = 1
        """
