from dataclasses import dataclass

from diamond_miner.defaults import DEFAULT_SUBSET
from diamond_miner.queries.query import Query, addr_to_string  # noqa
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class GetInvalidPrefixes(Query):
    """
    Return the prefixes with per-packet LB or that sends more replies than probes.

    >>> from diamond_miner.test import client
    >>> GetInvalidPrefixes().execute(client, 'test_nsdi_example')
    []
    >>> prefixes = GetInvalidPrefixes().execute(client, 'test_invalid_prefixes')
    >>> sorted((x[0], addr_to_string(x[1])) for x in prefixes)
    [(1, '201.0.0.0'), (1, '202.0.0.0')]
    """

    def query(self, table: str, subset: IPNetwork = DEFAULT_SUBSET) -> str:
        return f"""
        WITH count(reply_src_addr)     AS n_replies,
             uniqExact(reply_src_addr) AS n_distinct_replies
        SELECT DISTINCT probe_protocol, probe_dst_prefix
        FROM {table}
        WHERE {self.common_filters(subset)}
        GROUP BY (
            probe_protocol,
            probe_src_addr,
            probe_dst_prefix,
            probe_dst_addr,
            probe_src_port,
            probe_dst_port,
            probe_ttl
        )
        HAVING (n_replies > 2) OR (n_distinct_replies > 1)
        """
