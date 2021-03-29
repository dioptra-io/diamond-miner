from dataclasses import asdict, dataclass

from diamond_miner.queries.fragments import IPNetwork
from diamond_miner.queries.get_invalid_prefixes import GetInvalidPrefixes
from diamond_miner.queries.get_resolved_prefixes import GetResolvedPrefixes
from diamond_miner.queries.query import DEFAULT_SUBSET, Query, addr_to_string  # noqa


@dataclass(frozen=True)
class GetMaxTTL(Query):
    """
    Return the maximum TTL for each dst_addr.

    >>> from diamond_miner.test import execute
    >>> rows = execute(GetMaxTTL(round_leq=1), 'test_max_ttl')
    >>> sorted((addr_to_string(a), b) for a, b in rows)
    [('200.0.0.0', 3), ('201.0.0.0', 2)]
    """

    def query(self, table: str, subset: IPNetwork = DEFAULT_SUBSET) -> str:
        invalid_prefixes_query = GetInvalidPrefixes(**asdict(self)).query(table, subset)
        resolved_prefixes_query = GetResolvedPrefixes(**asdict(self)).query(
            table, subset
        )

        return f"""
        SELECT probe_dst_addr, max(probe_ttl_l3) AS max_ttl
        FROM {table}
        WHERE {self.common_filters(subset)}
        AND probe_dst_prefix NOT IN ({invalid_prefixes_query})
        AND probe_dst_prefix NOT IN ({resolved_prefixes_query})
        GROUP BY probe_dst_addr
        """
