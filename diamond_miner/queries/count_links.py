from dataclasses import dataclass

from diamond_miner.queries.query import UNIVERSE_SUBSET, LinksQuery, links_table
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class CountLinks(LinksQuery):
    """
    Count the distinct links in the links table.

    >>> from diamond_miner.test import url
    >>> CountLinks().execute(url, 'test_nsdi_example')[0][2]
    8
    """

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        return f"""
        SELECT probe_protocol, probe_src_addr, uniqExact(near_addr, far_addr)
        FROM {links_table(measurement_id)}
        WHERE {self.filters(subset)}
        GROUP BY (probe_protocol, probe_src_addr)
        """
