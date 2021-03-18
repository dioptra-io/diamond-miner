from dataclasses import dataclass

from diamond_miner.queries.query import (  # noqa
    IPNetwork,
    Query,
    addr_to_string,
    ip_in,
    ipv6,
)


@dataclass(frozen=True)
class GetInvalidPrefixes(Query):
    """
    Return the prefixes with per-packet LB or that sends more replies than probes.
    >>> from diamond_miner.test import execute
    >>> execute(GetInvalidPrefixes('100.0.0.1'), 'test_nsdi_example')
    []
    >>> prefixes = execute(GetInvalidPrefixes('100.0.0.1'), 'test_invalid_prefixes')
    >>> sorted(addr_to_string(pfx[0]) for pfx in prefixes)
    ['201.0.0.0', '202.0.0.0']
    """

    source: str

    def _query(self, table: str, subset: IPNetwork):
        return f"""
        WITH toIPv6(cutIPv6(probe_dst_addr, 8, 1)) AS probe_dst_prefix,
             count(reply_src_addr)         AS n_replies,
             uniqExact(reply_src_addr)     AS n_distinct_replies
        SELECT DISTINCT probe_dst_prefix
        FROM {table}
        WHERE {ip_in('probe_dst_prefix', subset)}
        AND probe_src_addr = {ipv6(self.source)}
        GROUP BY (
            probe_src_addr,
            probe_dst_addr,
            probe_src_port,
            probe_dst_port,
            probe_ttl_l4
        )
        HAVING (n_replies > 2) OR (n_distinct_replies > 1)
        """
