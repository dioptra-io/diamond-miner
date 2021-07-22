from dataclasses import dataclass
from typing import List

from diamond_miner.queries.query import UNIVERSE_SUBSET, LinksQuery, links_table
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class GetLinks(LinksQuery):
    """
    Return the links pre-computed in the links table.

    >>> from diamond_miner.test import addr_to_string, url
    >>> links = GetLinks(include_near_ttl=False).execute(url, 'test_nsdi_example')
    >>> len(links)
    8
    >>> links = GetLinks(include_near_ttl=True).execute(url, 'test_nsdi_example')
    >>> len(links)
    8
    """

    include_near_ttl: bool = False
    "If true, include the TTL at which `near_addr` was seen."

    def columns(self) -> List[str]:
        columns = [self.addr_cast("near_addr"), self.addr_cast("far_addr")]
        if self.include_near_ttl:
            columns.insert(0, "near_ttl")
        return columns

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        return f"""
        SELECT DISTINCT {','.join(self.columns())}
        FROM {links_table(measurement_id)}
        WHERE {self.filters(subset)}
        """
