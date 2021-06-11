from dataclasses import dataclass

from diamond_miner.queries.query import UNIVERSE_SUBSET, ResultsQuery, results_table
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class GetMaxTTL(ResultsQuery):
    """
    Return the maximum TTL for each dst_addr.

    >>> from diamond_miner.test import addr_to_string, url
    >>> rows = GetMaxTTL(round_leq=1).execute(url, 'test_max_ttl')
    >>> sorted((a, addr_to_string(b), c) for a, b, c in rows)
    [(1, '200.0.0.0', 3), (1, '201.0.0.0', 2)]
    """

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        return f"""
        SELECT probe_protocol, probe_dst_addr, max(probe_ttl) AS max_ttl
        FROM {results_table(measurement_id)}
        WHERE {self.filters(subset)}
        GROUP BY (probe_protocol, probe_dst_addr)
        """
