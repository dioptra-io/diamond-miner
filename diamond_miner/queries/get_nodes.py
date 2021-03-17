from dataclasses import dataclass

from diamond_miner.queries.query import (
    IPNetwork,
    Query,
    addr_to_string,
    ip_in,
    ip_not_private,
)


@dataclass(frozen=True)
class GetNodes(Query):
    """
    Return all the discovered nodes.

    >>> from diamond_miner.test import execute
    >>> nodes = execute(GetNodes(), 'test_nsdi_example')
    >>> sorted(nodes)
    ['150.0.1.1', '150.0.2.1', '150.0.3.1', '150.0.4.1', '150.0.5.1', '150.0.6.1', '150.0.7.1']
    """

    filter_private: bool = True

    def query(self, table: str, subset: IPNetwork):
        q = f"""
        WITH toIPv6(cutIPv6(probe_dst_addr, 8, 1)) AS probe_dst_prefix
        SELECT DISTINCT reply_src_addr
        FROM {table}
        WHERE {ip_in('probe_dst_prefix', subset)}
        AND reply_src_addr != probe_dst_addr
        AND reply_icmp_type = 11
        """
        if self.filter_private:
            q += f"AND {ip_not_private('reply_src_addr')}"
        return q

    def format(self, row):
        return addr_to_string(row[0])
