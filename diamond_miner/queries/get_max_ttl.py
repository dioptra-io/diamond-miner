from dataclasses import asdict, dataclass

from diamond_miner.queries.get_invalid_prefixes import GetInvalidPrefixes
from diamond_miner.queries.get_resolved_prefixes import GetResolvedPrefixes
from diamond_miner.queries.query import UNIVERSE_SUBSET, ResultsQuery, results_table
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class GetMaxTTL(ResultsQuery):
    """
    Return the maximum TTL for each dst_addr.

    >>> from diamond_miner.test import addr_to_string, client
    >>> rows = GetMaxTTL(round_leq=1).execute(client, 'test_max_ttl')
    >>> sorted((a, addr_to_string(b), c) for a, b, c in rows)
    [(1, '200.0.0.0', 3), (1, '201.0.0.0', 2)]
    """

    def query(self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET) -> str:
        invalid_prefixes_query = GetInvalidPrefixes(**asdict(self)).query(
            measurement_id, subset
        )
        resolved_prefixes_query = GetResolvedPrefixes(**asdict(self)).query(
            measurement_id, subset
        )

        return f"""
        SELECT probe_protocol, probe_dst_addr, max(probe_ttl) AS max_ttl
        FROM {results_table(measurement_id)}
        WHERE {self.filters(subset)}
        AND (probe_protocol, probe_dst_prefix) NOT IN ({invalid_prefixes_query})
        AND (probe_protocol, probe_dst_prefix) NOT IN ({resolved_prefixes_query})
        GROUP BY (probe_protocol, probe_dst_addr)
        """
