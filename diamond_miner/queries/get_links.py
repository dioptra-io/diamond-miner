from dataclasses import dataclass

from diamond_miner.queries.query import DEFAULT_SUBSET, Query
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class GetLinks(Query):
    """
    Return the links pre-computed in the links table.
    This doesn't group replies by probe protocol and probe source address,
    in other words, it assumes that the table contains the replies for a
    single vantage point and a single protocol.
    """

    filter_inter_round: bool = False
    "If true, exclude links inferred across rounds."

    filter_partial: bool = False
    "If true, exclude partial links: ('::', node) and (node, '::')."

    filter_virtual: bool = False
    "If true, exclude virtual links: ('::', '::')."

    def link_filter(self) -> str:
        s = "1"
        if self.filter_inter_round:
            s += "\nAND NOT is_inter_round"
        if self.filter_partial:
            s += "\nAND NOT is_partial"
        if self.filter_virtual:
            s += "\nAND NOT is_virtual"
        return s

    def query(self, table: str, subset: IPNetwork = DEFAULT_SUBSET) -> str:
        return f"""
        SELECT DISTINCT ({self.addr_cast('near_addr')}, {self.addr_cast('far_addr')})
        FROM {table}
        WHERE {self.link_filter()}
        """


@dataclass(frozen=True)
class GetLinksPerPrefix(Query):
    """
    Return the links pre-computed in the links table, grouped by
    protocol, source address and destination prefix.
    """

    filter_inter_round: bool = False
    "If true, exclude links inferred across rounds."

    filter_partial: bool = False
    "If true, exclude partial links: ('::', node) and (node, '::')."

    filter_virtual: bool = False
    "If true, exclude virtual links: ('::', '::')."

    def link_filter(self) -> str:
        s = "1"
        if self.filter_inter_round:
            s += "\nAND NOT is_inter_round"
        if self.filter_partial:
            s += "\nAND NOT is_partial"
        if self.filter_virtual:
            s += "\nAND NOT is_virtual"
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
        WHERE {self.link_filter()}
        GROUP BY (probe_protocol, probe_src_addr, probe_dst_prefix)
        """


@dataclass(frozen=True)
class GetLinksFromResults(Query):
    """
    Compute the links directly from the results table (legacy).
    This doesn't group replies by probe source address, in other words,
    it assumes that the table contains the replies for a single vantage point.
    """

    def query(self, table: str, subset: IPNetwork = DEFAULT_SUBSET) -> str:
        return f"""
        WITH -- 1) Compute the links
             --  x.1             x.2             x.3             x.4        x.5             x.6
             -- (probe_dst_addr, probe_src_port, probe_dst_port, probe_ttl, reply_src_addr, round)
             groupUniqArray((probe_dst_addr, probe_src_port, probe_dst_port, probe_ttl, reply_src_addr, round)) AS replies_unsorted,
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
