from dataclasses import dataclass

from diamond_miner.queries.query import IPNetwork, Query, ip_in, ipv6


@dataclass(frozen=True)
class CountNodesPerTTL(Query):
    """
    Return the number of nodes discovered at each TTL.

    >>> from diamond_miner.test import execute
    >>> execute(CountNodesPerTTL('100.0.0.1'), 'test_nsdi_example')
    [(1, 1), (2, 2), (3, 3), (4, 1)]
    """

    source: str
    max_ttl: int = 255

    def query(self, table: str, subset: IPNetwork):
        return f"""
        WITH toIPv6(cutIPv6(probe_dst_addr, 8, 1)) AS probe_dst_prefix
        SELECT probe_ttl_l4, uniqExact(reply_src_addr)
        FROM {table}
        WHERE {ip_in('probe_dst_prefix', subset)}
        AND probe_src_addr = {ipv6(self.source)}
        AND reply_src_addr != probe_dst_addr
        AND reply_icmp_type = 11
        AND probe_ttl_l4 <= {self.max_ttl}
        GROUP BY probe_ttl_l4
        """
