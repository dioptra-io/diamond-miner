from dataclasses import dataclass

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries.query import ResultsQuery, results_table
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class GetPrefixesWithAmplification(ResultsQuery):
    """
    Return the prefixes for which we have more than one reply per (flow ID, TTL).

    .. important:: This query assumes that a single probe is sent per (flow ID, TTL) pair.

    >>> from diamond_miner.test import addr_to_string, url
    >>> rows = GetPrefixesWithAmplification().execute(url, "test_invalid_prefixes")
    >>> [addr_to_string(x[2]) for x in rows]
    ['201.0.0.0', '202.0.0.0']
    """

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        return f"""
        SELECT DISTINCT
            probe_protocol,
            probe_src_addr,
            probe_dst_prefix,
            -- This column is to simplify the InsertPrefixes query.
            1 AS has_amplification
        FROM {results_table(measurement_id)}
        WHERE {self.filters(subset)}
        GROUP BY (
            probe_protocol,
            probe_src_addr,
            probe_dst_prefix,
            probe_dst_addr,
            probe_src_port,
            probe_dst_port,
            probe_ttl
        )
        HAVING count() > 1
        """


@dataclass(frozen=True)
class GetPrefixesWithLoops(ResultsQuery):
    """
    Return the prefixes for which an IP address appears multiple time for a single flow ID.

    .. note:: Prefixes with amplification (multiple replies per probe) may trigger a false positive
              for this query, since we do not check that the IP appears at two *different* TTLs.

    >>> from diamond_miner.test import addr_to_string, url
    >>> rows = GetPrefixesWithLoops().execute(url, "test_invalid_prefixes")
    >>> [addr_to_string(x[2]) for x in rows]
    ['201.0.0.0']
    """

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        return f"""
        SELECT DISTINCT
            probe_protocol,
            probe_src_addr,
            probe_dst_prefix,
            -- This column is to simplify the InsertPrefixes query.
            1 AS has_loops
        FROM {results_table(measurement_id)}
        WHERE {self.filters(subset)}
        GROUP BY (
            probe_protocol,
            probe_src_addr,
            probe_dst_prefix,
            probe_dst_addr,
            probe_src_port,
            probe_dst_port
        )
        HAVING uniqExact(reply_src_addr) < count()
        """
