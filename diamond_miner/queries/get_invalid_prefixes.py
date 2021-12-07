from dataclasses import dataclass

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries.query import (
    PrefixesQuery,
    ResultsQuery,
    prefixes_table,
    results_table,
)
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class GetInvalidPrefixes(PrefixesQuery):
    """
    Return the prefixes with unexpected behavior
    (see :class:`GetPrefixesWithAmplification` and :class:`GetPrefixesWithLoops`).

    >>> from diamond_miner.test import url
    >>> rows = GetInvalidPrefixes().execute(url, "test_invalid_prefixes")
    >>> [x["probe_dst_prefix"] for x in rows]
    ['::ffff:201.0.0.0', '::ffff:202.0.0.0']
    """

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        return f"""
        SELECT probe_dst_prefix
        FROM {prefixes_table(measurement_id)}
        WHERE {self.filters(subset)} AND (has_amplification OR has_loops)
        """


@dataclass(frozen=True)
class GetPrefixesWithAmplification(ResultsQuery):
    """
    Return the prefixes for which we have more than one reply per (flow ID, TTL).

    .. important:: This query assumes that a single probe is sent per (flow ID, TTL) pair.

    >>> from diamond_miner.test import url
    >>> rows = GetPrefixesWithAmplification().execute(url, "test_invalid_prefixes")
    >>> [x["probe_dst_prefix"] for x in rows]
    ['::ffff:202.0.0.0']
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

    >>> from diamond_miner.test import url
    >>> GetPrefixesWithLoops().execute(url, "test_invalid_prefixes")
    [{'probe_protocol': 1, 'probe_src_addr': '::ffff:100.0.0.1', 'probe_dst_prefix': '::ffff:201.0.0.0', 'has_loops': 1}]
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
