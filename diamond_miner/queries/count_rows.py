from dataclasses import dataclass

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries.fragments import cut_ipv6
from diamond_miner.queries.query import (
    FlowsQuery,
    LinksQuery,
    ProbesQuery,
    ResultsQuery,
    flows_table,
    links_table,
    probes_table,
    results_table,
)
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class CountFlowsPerPrefix(FlowsQuery):
    """
    Count rows per prefix.
    """

    prefix_len_v4: int = 8
    prefix_len_v6: int = 8

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        return f"""
        WITH {cut_ipv6('probe_dst_addr', self.prefix_len_v4, self.prefix_len_v6)} AS prefix
        SELECT prefix, count() AS count
        FROM {flows_table(measurement_id)}
        WHERE {self.filters(subset)}
        GROUP BY prefix
        """


@dataclass(frozen=True)
class CountLinksPerPrefix(LinksQuery):
    """
    Count rows per prefix.
    """

    prefix_len_v4: int = 8
    prefix_len_v6: int = 8

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
    Count the number of probes per prefix.
    """

    prefix_len_v4: int = 8
    prefix_len_v6: int = 8

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
    Count rows per prefix.

    >>> from diamond_miner.test import url
    >>> rows = CountResultsPerPrefix(prefix_len_v4=8, prefix_len_v6=8).execute(url, 'test_count_replies')
    >>> sorted((row["prefix"], row["count"]) for row in rows)
    [('::ffff:1.0.0.0', 2), ('::ffff:2.0.0.0', 1), ('::ffff:204.0.0.0', 1)]
    """

    prefix_len_v4: int = 8
    prefix_len_v6: int = 8

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
