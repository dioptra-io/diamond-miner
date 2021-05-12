from dataclasses import dataclass

from diamond_miner.defaults import DEFAULT_PROBE_TTL_COLUMN
from diamond_miner.queries.query import DEFAULT_SUBSET, Query
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class GetLinksFromResults(Query):
    # This query is tested in test_queries.py due to its complexity.

    def query(self, table: str, subset: IPNetwork = DEFAULT_SUBSET) -> str:
        return f"""
        WITH -- 1) Compute the links
             --  x.1             x.2             x.3             x.4        x.5             x.6
             -- (probe_dst_addr, probe_src_port, probe_dst_port, probe_ttl, reply_src_addr, round)
             groupUniqArray((probe_dst_addr, probe_src_port, probe_dst_port, {DEFAULT_PROBE_TTL_COLUMN}, reply_src_addr, round)) AS replies_unsorted,
             -- sort by (probe_dst_addr, probe_src_port, probe_dst_port, probe_ttl)
             arraySort(x -> (x.1, x.2, x.3, x.4), replies_unsorted) AS replies,
             -- shift by 1: remove the element and append a NULL row
             arrayConcat(arrayPopFront(replies), [(toIPv6('::'), 0, 0, 0, toIPv6('::'), 0)]) AS replies_shifted,
             -- compute the links by zipping the two arrays
             arrayFilter(x -> x.1.4 + 1 == x.2.4, arrayZip(replies, replies_shifted)) AS links,
             arrayDistinct(arrayMap(x -> (x.1.5, x.2.5), links)) AS links_minimal
        SELECT
               probe_protocol,
               probe_dst_prefix,
               links_minimal
        FROM {table}
        WHERE {self.common_filters(subset)}
        GROUP BY (probe_protocol, probe_dst_prefix)
        """


@dataclass(frozen=True)
class GetLinks(Query):
    # NOTE: Does not group by probe_protocol and probe_src_addr

    # Get the ('::', a) and (a, '::') links
    is_partial: bool = True

    # Get the ('::', '::') links
    is_virtual: bool = True

    # Get the inter round links
    is_inter_round: bool = True

    def links_filters(self):
        s = "1=1"
        if not self.is_partial:
            s += "\nAND NOT is_partial"
        if not self.is_virtual:
            s += "\nAND NOT is_virtual"
        if not self.is_inter_round:
            s += "\nAND NOT is_inter_round"
        return s

    def query(self, table: str, subset: IPNetwork = DEFAULT_SUBSET) -> str:
        return f"""
        SELECT DISTINCT ({self.addr_cast('near_addr')}, {self.addr_cast('far_addr')})
        FROM {table}
        WHERE {self.links_filters()}
        """


@dataclass(frozen=True)
class GetLinksPerPrefix(Query):

    # Get the ('::', a) and (a, '::') links
    is_partial: bool = True

    # Get the ('::', '::') links
    is_virtual: bool = True

    # Get the inter round links
    is_inter_round: bool = True

    def links_filters(self):
        s = "1=1"
        if not self.is_partial:
            s += "\nAND NOT is_partial"
        if not self.is_virtual:
            s += "\nAND NOT is_virtual"
        if not self.is_inter_round:
            s += "\nAND NOT is_inter_round"
        return s

    def query(self, table: str, subset: IPNetwork = DEFAULT_SUBSET) -> str:
        return f"""
        SELECT
            probe_protocol,
            probe_src_addr,
            probe_dst_prefix,
            groupUniqArray(
                ({self.addr_cast('near_addr')}, {self.addr_cast('far_addr')})
            )
        FROM {table}
        WHERE {self.links_filters()}
        GROUP BY (probe_protocol, probe_src_addr, probe_dst_prefix)
        """
