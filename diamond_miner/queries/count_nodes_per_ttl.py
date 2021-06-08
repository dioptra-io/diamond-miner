from dataclasses import dataclass

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries.query import ResultsQuery, results_table
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class CountNodesPerTTL(ResultsQuery):
    """
    Return the number of nodes discovered at each TTL.
    This query does not support the ``subset`` parameter.

    >>> from diamond_miner.test import client
    >>> CountNodesPerTTL().execute(client, 'test_nsdi_example')
    [(1, 1), (2, 2), (3, 3), (4, 1)]
    """

    def query(self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET) -> str:
        assert subset == UNIVERSE_SUBSET, "subset not allowed for this query"
        return f"""
        SELECT probe_ttl, uniqExact(reply_src_addr)
        FROM {results_table(measurement_id)}
        WHERE {self.filters(subset)}
        GROUP BY probe_ttl
        """
