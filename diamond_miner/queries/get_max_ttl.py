from dataclasses import dataclass

from diamond_miner.queries.get_invalid_prefixes import GetInvalidPrefixes
from diamond_miner.queries.get_resolved_prefixes import GetResolvedPrefixes
from diamond_miner.queries.query import (  # noqa
    IPNetwork,
    Query,
    addr_to_string,
    ip_in,
    ip_not_private,
    ipv6,
)


@dataclass(frozen=True)
class GetMaxTTL(Query):
    """
    Return the maximum TTL for each dst_addr.

    >>> from diamond_miner.test import execute
    >>> rows = execute(GetMaxTTL('100.0.0.1', 1), 'test_max_ttl')
    >>> sorted((addr_to_string(a), b) for a, b in rows)
    [('200.0.0.0', 3), ('201.0.0.0', 2)]
    """

    source: str
    round: int

    def _query(self, table: str, subset: IPNetwork):
        invalid_prefixes_query = GetInvalidPrefixes(self.source)._query(table, subset)
        resolved_prefixes_query = GetResolvedPrefixes(self.source, self.round)._query(
            table, subset
        )
        return f"""
        WITH toIPv6(cutIPv6(probe_dst_addr, 8, 1)) AS probe_dst_prefix
        SELECT probe_dst_addr,
               max(probe_ttl_l4) AS max_ttl
        FROM {table}
        WHERE {ip_in('probe_dst_prefix', subset)}
        AND probe_dst_prefix NOT IN ({invalid_prefixes_query})
        AND probe_dst_prefix NOT IN ({resolved_prefixes_query})
        AND {ip_not_private('reply_src_addr')}
        AND probe_src_addr = {ipv6(self.source)}
        AND probe_dst_addr != reply_src_addr
        AND reply_icmp_type = 11
        AND round <= {self.round}
        GROUP BY probe_dst_addr
        """
