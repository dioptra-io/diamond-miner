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

    We emit cross-rounds links.
    For example if flow N sees node A at TTL 10 at round 1 and flow N sees node B at TTL 11 at round 2,
    we will emit (1, 10, A) -> (2, 11, B).

    We assume that there exists a single (flow, ttl) pair over all rounds.
    TODO: Assert this?

    >>> from diamond_miner.test import client
    >>> links = GetLinksFromView().execute(client, "test_nsdi_example_flows")
    >>> len(links)
    58
    """

    # TODO: Ignore invalid prefixes
    # => modify the GetInvalidPrefixes to work on the FlowsView?
    # count replies_per_ttl and HAVING max(replies_per_ttl) = 1

    def query(self, table: str, subset: IPNetwork = DEFAULT_SUBSET) -> str:
        return f"""
        WITH
            -- (round, ttl, addr)
            groupUniqArrayMerge(replies) AS traceroute,
            arrayMap(x -> x.2, traceroute) AS ttls,
            arrayMap(x -> (x.1, x.3), traceroute) AS val,
            CAST((ttls, val), 'Map(UInt8, Tuple(UInt8, IPv6))') AS map,
            arrayReduce('min', ttls) AS first_ttl,
            arrayReduce('max', ttls) AS last_ttl,
            arrayMap(i -> (toUInt8(i), toUInt8(i + 1), map[toUInt8(i)], map[toUInt8(i + 1)]), range(first_ttl, last_ttl)) AS links,
            arrayJoin(links) AS link
        SELECT
            {CreateFlowsView.SORTING_KEY},
            -- Set the round number for partial links:
            -- the link (1, 1, A) -> (0, 2, *) becomes (1, 1, A) -> (1, 2, *)
            if(link.3.1 != 0, link.3.1, link.4.1) AS near_round,
            if(link.4.1 != 0, link.4.1, link.3.1) AS far_round,
            link.1 AS near_ttl,
            link.2 AS far_ttl,
            link.3.2 AS near_addr,
            link.4.2 AS far_addr
        FROM {table}
        GROUP BY ({CreateFlowsView.SORTING_KEY})
        SETTINGS optimize_aggregation_in_order = 1
        """
