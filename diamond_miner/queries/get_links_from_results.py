from dataclasses import dataclass

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries.fragments import ip_in
from diamond_miner.queries.query import ResultsQuery, prefixes_table, results_table
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class GetLinksFromResults(ResultsQuery):
    """
    Compute the links from the results table.
    This returns one line per `(flow, link)` pair.

    We do not emit a link in the case of single reply in a traceroute.
    For example: `* * node * *`, does not generate a link.
    However, `* * node * * node'`, will generate `(node, *)` and `(*, node')`.

    We emit cross-rounds links.
    For example if flow N sees node A at TTL 10 at round 1 and flow N sees node B at TTL 11 at round 2,
    we will emit `(1, 10, A) - (2, 11, B)`.

    We assume that there exists a single (flow, ttl) pair over all rounds (TODO: assert this).

    If `round_eq` is none, compute the links per flow, across all rounds.
    Otherwise, compute the links per flow, for the specified round.
    This is useful if you want to update a `links` table round-by-round:
    such a table will contain only intra-round links but can be updated incrementally.

    Examples:
        >>> from diamond_miner.test import client
        >>> from diamond_miner.queries import GetLinksFromResults
        >>> links = GetLinksFromResults().execute(client, "test_nsdi_example")
        >>> len(links)
        58
    """

    ignore_invalid_prefixes: bool = True
    "If true, exclude invalid prefixes from links computation."

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        if self.ignore_invalid_prefixes:
            invalid_filter = f"""
            AND (probe_protocol, probe_src_addr, probe_dst_prefix)
            NOT IN (
                SELECT probe_protocol, probe_src_addr, probe_dst_prefix
                FROM {prefixes_table(measurement_id)}
                WHERE {ip_in('probe_dst_prefix', subset)}
                AND has_amplification
            )
            """
            # TODO: We currently do not drop prefixes with loops as this considerably
            # reduces the number of discoveries. As such, we send more probe than necessary.
            # A better way would be to detect the min/max TTL of a loop, and to ignore it
            # in the next round query.
        else:
            invalid_filter = ""

        return f"""
        WITH
            groupUniqArray((round, probe_ttl, reply_src_addr)) AS traceroute,
            arrayMap(x -> x.2, traceroute) AS ttls,
            arrayMap(x -> (x.1, x.3), traceroute) AS val,
            CAST((ttls, val), 'Map(UInt8, Tuple(UInt8, IPv6))') AS map,
            arrayMin(ttls) AS first_ttl,
            arrayMax(ttls) AS last_ttl,
            arrayMap(i -> (toUInt8(i), toUInt8(i + 1), map[toUInt8(i)], map[toUInt8(i + 1)]), range(first_ttl, last_ttl)) AS links,
            arrayJoin(links) AS link
        SELECT
            probe_protocol,
            probe_src_addr,
            probe_dst_prefix,
            probe_dst_addr,
            probe_src_port,
            probe_dst_port,
            -- Set the round number for partial links:
            -- The link (1, 10, A) -> (null, 11, *) becomes
            --          (1, 10, A) -> (1,    11, *)
            if(link.3.1 != 0, link.3.1, link.4.1) AS near_round,
            if(link.4.1 != 0, link.4.1, link.3.1) AS far_round,
            link.1 AS near_ttl,
            link.2 AS far_ttl,
            link.3.2 AS near_addr,
            link.4.2 AS far_addr
        FROM {results_table(measurement_id)}
        WHERE {self.filters(subset)}
        {invalid_filter}
        GROUP BY (
            probe_protocol,
            probe_src_addr,
            probe_dst_prefix,
            probe_dst_addr,
            probe_src_port,
            probe_dst_port
        )
        """
