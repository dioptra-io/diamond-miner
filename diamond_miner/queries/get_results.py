from dataclasses import dataclass

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries.query import ResultsQuery, results_table
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class GetResults(ResultsQuery):
    """
    Return all the columns from the results table.

    >>> from diamond_miner.test import addr_to_string, url
    >>> rows = GetResults().execute(url, 'test_nsdi_example')
    >>> len(rows)
    85
    """

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        return f"""
        SELECT *
        FROM {results_table(measurement_id)}
        WHERE {self.filters(subset)}
        """
