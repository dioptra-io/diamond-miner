from dataclasses import dataclass

from diamond_miner.queries.query import UNIVERSE_SUBSET, ResultsQuery, results_table
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class GetPrefixes(ResultsQuery):
    """
    Return the destination prefixes appearing in the results table.

    >>> from diamond_miner.test import addr_to_string, url
    >>> rows = GetPrefixes().execute(url, 'test_nsdi_example')
    >>> len(rows)
    1
    """

    # TODO: Filter prefixes that sees some IPs, networks, ASes...

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        return f"""
        SELECT DISTINCT probe_dst_prefix
        FROM {results_table(measurement_id)}
        WHERE {self.filters(subset)}
        ORDER BY probe_dst_prefix
        """
