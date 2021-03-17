from dataclasses import dataclass

from diamond_miner.queries.query import (
    IPNetwork,
    Query,
    addr_to_string,
    ip_in,
    ip_not_private,
)


@dataclass(frozen=True)
class GetLinks(Query):
    """
    Return all the discovered links.

    >>> from diamond_miner.test import execute
    >>> nodes = execute(GetLinks(), 'test_nsdi_example')[0]
    >>> nodes[0]
    '100.0.0.1'
    >>> nodes[1]
    '200.0.0.0'
    >>> sorted(nodes[2])[:3]
    [('150.0.1.1', '150.0.2.1'), ('150.0.1.1', '150.0.3.1'), ('150.0.2.1', '150.0.4.1')]
    >>> sorted(nodes[2])[3:6]
    [('150.0.3.1', '150.0.5.1'), ('150.0.3.1', '150.0.7.1'), ('150.0.4.1', '150.0.6.1')]
    >>> sorted(nodes[2])[6:]
    [('150.0.5.1', '150.0.6.1'), ('150.0.7.1', '150.0.6.1')]
    """

    filter_private: bool = True

    def query(self, table: str, subset: IPNetwork):
        return f"""
        WITH
            range(1, 32) AS TTLs, -- TTLs used to group nodes and links
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
            SELECT probe_src_addr,
                   probe_dst_prefix,
                   links_minimal
            FROM {table}
            WHERE {ip_in('probe_dst_prefix', subset)}
            AND {ip_not_private('reply_src_addr')}
            AND probe_dst_addr != reply_src_addr
            AND reply_icmp_type = 11
            GROUP BY (probe_src_addr, probe_dst_prefix)
        """

    def format(self, row):
        return (
            addr_to_string(row[0]),
            addr_to_string(row[1]),
            [(addr_to_string(a), addr_to_string(b)) for a, b in row[2]],
        )
