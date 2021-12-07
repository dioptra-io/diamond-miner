from dataclasses import dataclass
from typing import List

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries import GetInvalidPrefixes
from diamond_miner.queries.query import LinksQuery, links_table
from diamond_miner.typing import IPNetwork
from diamond_miner.utilities import common_parameters


@dataclass(frozen=True)
class GetLinks(LinksQuery):
    """
    Return the links pre-computed in the links table.

    >>> from diamond_miner.test import url
    >>> links = GetLinks(filter_invalid_prefixes=False).execute(url, 'test_invalid_prefixes')
    >>> len(links)
    3
    >>> links = GetLinks(filter_invalid_prefixes=True).execute(url, 'test_invalid_prefixes')
    >>> len(links)
    1
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

    filter_invalid_prefixes: bool = False
    "If true, exclude links from prefixes with amplification or loops."

    include_metadata: bool = False
    "If true, include the TTLs at which `near_addr` and `far_addr` were seen."

    def columns(self) -> List[str]:
        columns = ["near_addr", "far_addr"]
        if self.include_metadata:
            columns = ["near_ttl", "far_ttl", *columns]
        return columns

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        if self.filter_invalid_prefixes:
            invalid_prefixes_query = GetInvalidPrefixes(
                **common_parameters(self, GetInvalidPrefixes)
            )
            prefix_filter = f"""
            probe_dst_prefix NOT IN ({invalid_prefixes_query.statement(measurement_id, subset)})
            """
        else:
            prefix_filter = "1"
        return f"""
        SELECT DISTINCT {','.join(self.columns())}
        FROM {links_table(measurement_id)}
        WHERE {self.filters(subset)} AND {prefix_filter}
        """
