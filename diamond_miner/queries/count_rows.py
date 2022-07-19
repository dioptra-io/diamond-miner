from dataclasses import dataclass

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries.fragments import cut_ipv6
from diamond_miner.queries.query import (
    LinksQuery,
    ProbesQuery,
    ResultsQuery,
    links_table,
    probes_table,
    results_table,
)
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class CountLinksPerPrefix(LinksQuery):
    """
    Count the number of (non-distinct) links per prefix.

    Examples:
        >>> from diamond_miner.test import client
        >>> from diamond_miner.queries import CountLinksPerPrefix
        >>> rows = CountLinksPerPrefix().execute(client, 'test_nsdi_example')
        >>> sorted((row["prefix"], row["count"]) for row in rows)
        [('::ffff:200.0.0.0', 58)]
    """

    prefix_len_v4: int = 16
    "The IPv4 prefix length to consider."

    prefix_len_v6: int = 8
    "The IPv6 prefix length to consider."

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        return f"""
        WITH {cut_ipv6('probe_dst_addr', self.prefix_len_v4, self.prefix_len_v6)} AS prefix
        SELECT prefix, count() AS count
        FROM {links_table(measurement_id)}
        WHERE {self.filters(subset)}
        GROUP BY prefix
        """


@dataclass(frozen=True)
class CountProbesPerPrefix(ProbesQuery):
    """
    Count the number of probes sent per prefix.

    Examples:
        >>> from diamond_miner.test import client
        >>> from diamond_miner.queries import CountProbesPerPrefix
        >>> rows = CountResultsPerPrefix(round_eq=1).execute(client, 'test_nsdi_example')
        >>> sorted((row["prefix"], row["count"]) for row in rows)
        [('::ffff:200.0.0.0', 24)]
    """

    prefix_len_v4: int = 16
    "The IPv4 prefix length to consider."

    prefix_len_v6: int = 8
    "The IPv6 prefix length to consider."

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        assert self.round_eq
        return f"""
        WITH {cut_ipv6('probe_dst_prefix', self.prefix_len_v4, self.prefix_len_v6)} AS prefix
        SELECT prefix, sum(cumulative_probes) AS count
        FROM {probes_table(measurement_id)}
        WHERE {self.filters(subset)}
        GROUP BY prefix
        """


@dataclass(frozen=True)
class CountResultsPerPrefix(ResultsQuery):
    """
    Count the number of results per prefix.

    Examples:
        >>> from diamond_miner.test import client
        >>> from diamond_miner.queries import CountResultsPerPrefix
        >>> rows = CountResultsPerPrefix(prefix_len_v4=8, prefix_len_v6=8).execute(client, 'test_count_replies')
        >>> sorted((row["prefix"], row["count"]) for row in rows)
        [('::ffff:1.0.0.0', 2), ('::ffff:2.0.0.0', 1), ('::ffff:204.0.0.0', 1)]
    """

    prefix_len_v4: int = 16
    "The IPv4 prefix length to consider."

    prefix_len_v6: int = 8
    "The IPv6 prefix length to consider."

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        return f"""
        WITH {cut_ipv6('probe_dst_addr', self.prefix_len_v4, self.prefix_len_v6)} AS prefix
        SELECT prefix, count() AS count
        FROM {results_table(measurement_id)}
        WHERE {self.filters(subset)}
        GROUP BY prefix
        """
