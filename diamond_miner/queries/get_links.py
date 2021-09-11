from dataclasses import dataclass
from typing import List

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries.query import LinksQuery, links_table
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class GetLinks(LinksQuery):
    """
    Return the links pre-computed in the links table.

    >>> from diamond_miner.test import addr_to_string, url
    >>> links = GetLinks(include_metadata=False).execute(url, 'test_nsdi_example')
    >>> len(links)
    8
    >>> links = GetLinks(include_metadata=True).execute(url, 'test_nsdi_example')
    >>> len(links)
    8
    >>> links = GetLinks(near_or_far_addr="150.0.6.1").execute(url, 'test_nsdi_example')
    >>> len(links)
    3
    """

    include_metadata: bool = False
    "If true, include the TTLs at which `near_addr` and `far_addr` were seen."

    def columns(self) -> List[str]:
        columns = [self._addr_cast("near_addr"), self._addr_cast("far_addr")]
        if self.include_metadata:
            columns = ["near_ttl", "far_ttl", *columns]
        return columns

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        return f"""
        SELECT DISTINCT {','.join(self.columns())}
        FROM {links_table(measurement_id)}
        WHERE {self.filters(subset)}
        """
