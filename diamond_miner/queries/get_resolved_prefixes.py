from dataclasses import dataclass

from diamond_miner.queries.query import (  # noqa
    IPNetwork,
    Query,
    addr_to_string,
    ip_in,
    ipv6,
)


@dataclass(frozen=True)
class GetResolvedPrefixes(Query):
    """
    Return the prefixes for which no replies have been received at the previous round
    (i.e. no probes have been sent, most likely).

    >>> from diamond_miner.test import execute
    >>> execute(GetResolvedPrefixes('100.0.0.1', 1), 'test_nsdi_example')
    []
    >>> prefixes = execute(GetResolvedPrefixes('100.0.0.1', 5), 'test_nsdi_example')
    >>> [addr_to_string(pfx[0]) for pfx in prefixes]
    ['200.0.0.0']
    """

    source: str
    round: int

    def _query(self, table: str, subset: IPNetwork):
        return f"""
        WITH toIPv6(cutIPv6(probe_dst_addr, 8, 1)) AS probe_dst_prefix
        SELECT DISTINCT probe_dst_prefix
        FROM {table}
        WHERE {ip_in('probe_dst_prefix', subset)}
        AND probe_src_addr = {ipv6(self.source)}
        GROUP BY (probe_src_addr, probe_dst_prefix)
        HAVING max(round) < {self.round - 1}
        """
