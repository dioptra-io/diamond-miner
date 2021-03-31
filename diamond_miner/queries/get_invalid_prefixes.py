from dataclasses import dataclass

from diamond_miner.defaults import DEFAULT_PROBE_TTL_COLUMN, DEFAULT_SUBSET
from diamond_miner.queries.fragments import IPNetwork
from diamond_miner.queries.query import Query, addr_to_string  # noqa


@dataclass(frozen=True)
class GetInvalidPrefixes(Query):
    """
    Return the prefixes with per-packet LB or that sends more replies than probes.
    >>> from diamond_miner.test import execute
    >>> execute(GetInvalidPrefixes(), 'test_nsdi_example')
    []
    >>> prefixes = execute(GetInvalidPrefixes(), 'test_invalid_prefixes')
    >>> sorted(addr_to_string(pfx[0]) for pfx in prefixes)
    ['201.0.0.0', '202.0.0.0']
    """

    def query(self, table: str, subset: IPNetwork = DEFAULT_SUBSET) -> str:
        return f"""
        WITH count(reply_src_addr)     AS n_replies,
             uniqExact(reply_src_addr) AS n_distinct_replies
        SELECT DISTINCT probe_dst_prefix
        FROM {table}
        WHERE {self.common_filters(subset)}
        GROUP BY (
            probe_src_addr,
            probe_dst_prefix,
            probe_dst_addr,
            probe_src_port,
            probe_dst_port,
            {DEFAULT_PROBE_TTL_COLUMN}
        )
        HAVING (n_replies > 2) OR (n_distinct_replies > 1)
        """
