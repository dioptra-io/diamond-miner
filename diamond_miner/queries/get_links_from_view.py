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

    We do not emit a link in the case of single reply in a traceroute.
    For example: * * node * *, does not generate a link.
    However, * * node * * node', will generate (node, *) and (*, node').

    >>> from diamond_miner.test import client
    >>> links = GetLinksFromView().execute(client, "test_nsdi_example_flows")
    >>> len(links)
    58
    """

    # TODO: Ignore invalid prefixes
    # => modify the GetInvalidPrefixes to work on the FlowsView?

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
            {CreateFlowsView.SORTING_KEY},
            link.1 AS near_ttl,
            link.2 AS near_addr,
            link.3 AS far_addr
        FROM {table}
        GROUP BY ({CreateFlowsView.SORTING_KEY})
        SETTINGS optimize_aggregation_in_order = 1
        """
