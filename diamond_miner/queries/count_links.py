from dataclasses import dataclass

from diamond_miner.queries.query import IPNetwork, Query, ip_in, ip_not_private


@dataclass(frozen=True)
class CountLinks(Query):
    """
    Count all the discovered links.

    >>> from diamond_miner.test import execute
    >>> execute(CountLinks(), 'test_nsdi_example')[0][0]
    8
    """

    filter_private: bool = True

    def _query(self, table: str, subset: IPNetwork):
        return f"""
        WITH
            toIPv6(cutIPv6(probe_dst_addr, 8, 1)) AS probe_dst_prefix,
            -- 1) Compute the links
            --  x.1             x.2             x.3             x.4           x.5             x.6
            -- (probe_dst_addr, probe_src_port, probe_dst_port, probe_ttl_l3, reply_src_addr, round)
            groupUniqArray((probe_dst_addr, probe_src_port, probe_dst_port, probe_ttl_l3, reply_src_addr, round)) AS replies_unsorted,
            -- sort by (probe_dst_addr, probe_src_port, probe_dst_port, probe_ttl_l3)
            arraySort(x -> (x.1, x.2, x.3, x.4), replies_unsorted) AS replies,
            -- shift by 1: remove the element and append a NULL row
            arrayConcat(arrayPopFront(replies), [(toIPv6('::'), 0, 0, 0, toIPv6('::'), 0)]) AS replies_shifted,
            -- compute the links by zipping the two arrays
            arrayFilter(x -> x.1.4 + 1 == x.2.4, arrayZip(replies, replies_shifted)) AS links,
            arrayDistinct(arrayMap(x -> (x.1.5, x.2.5), links)) AS links_minimal
            SELECT length(links_minimal)
            FROM {table}
            WHERE {ip_in('probe_dst_prefix', subset)}
            AND {ip_not_private('reply_src_addr')}
            AND probe_dst_addr != reply_src_addr
            AND reply_icmp_type = 11
        """
