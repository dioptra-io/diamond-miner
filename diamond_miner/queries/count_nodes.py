from dataclasses import dataclass

from diamond_miner.queries.query import (  # noqa
    IPNetwork,
    Query,
    addr_to_string,
    ip_in,
    ip_not_private,
)


@dataclass(frozen=True)
class CountNodes(Query):
    """
    Count all the discovered nodes.

    >>> from diamond_miner.test import execute
    >>> execute(CountNodes(), 'test_nsdi_example')[0][0]
    7
    """

    filter_private: bool = True

    def _query(self, table: str, subset: IPNetwork):
        q = f"""
        WITH toIPv6(cutIPv6(probe_dst_addr, 8, 1)) AS probe_dst_prefix
        SELECT uniqExact(reply_src_addr)
        FROM {table}
        WHERE {ip_in('probe_dst_prefix', subset)}
        AND reply_src_addr != probe_dst_addr
        AND reply_icmp_type = 11
        """
        if self.filter_private:
            q += f"AND {ip_not_private('reply_src_addr')}"
        return q
