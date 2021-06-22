from dataclasses import dataclass

from diamond_miner.queries.query import UNIVERSE_SUBSET, ResultsQuery, results_table
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class CountNodes(ResultsQuery):
    """
    Count the distinct nodes from the results table.

    >>> from diamond_miner.test import url
    >>> CountNodes().execute(url, 'test_nsdi_example')[0][2]
    7
    """

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        return f"""
        SELECT probe_protocol, probe_src_addr, uniqExact(reply_src_addr)
        FROM {results_table(measurement_id)}
        WHERE {self.filters(subset)}
        GROUP BY (probe_protocol, probe_src_addr)
        """
