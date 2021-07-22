from dataclasses import dataclass

from diamond_miner.queries import CreatePrefixesTable
from diamond_miner.queries.query import UNIVERSE_SUBSET, PrefixQuery, prefixes_table
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class GetPrefixes(PrefixQuery):
    """
    Return the destination prefixes for which replies have been received.

    >>> from diamond_miner.test import addr_to_string, url
    >>> rows = GetPrefixes().execute(url, 'test_nsdi_example')
    >>> len(rows)
    1
    >>> rows = GetPrefixes().execute(url, 'test_invalid_prefixes')
    >>> len(rows)
    3
    """

    # TODO: Filter prefixes that sees some IPs, networks, ASes...

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        return f"""
        SELECT probe_dst_prefix, has_amplification, has_loops
        FROM {prefixes_table(measurement_id)}
        WHERE {self.filters(subset)}
        ORDER BY {CreatePrefixesTable.SORTING_KEY}
        """
