from dataclasses import dataclass

from diamond_miner.defaults import DEFAULT_SUBSET
from diamond_miner.queries.fragments import IPNetwork
from diamond_miner.queries.query import Query


@dataclass(frozen=True)
class CountNodesPerTTL(Query):
    """
    Return the number of nodes discovered at each TTL.

    >>> from diamond_miner.test import execute
    >>> execute(CountNodesPerTTL(), 'test_nsdi_example')
    [(1, 1), (2, 2), (3, 3), (4, 1)]
    """

    max_ttl: int = 255

    def query(self, table: str, subset: IPNetwork = DEFAULT_SUBSET) -> str:
        return f"""
        SELECT probe_ttl_l3, uniqExact(reply_src_addr)
        FROM {table}
        WHERE {self.common_filters(subset)}
        AND probe_ttl_l3 <= {self.max_ttl}
        GROUP BY probe_ttl_l3
        """
