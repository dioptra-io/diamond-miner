from dataclasses import dataclass

from diamond_miner.queries.query import UNIVERSE_SUBSET, LinksQuery, links_table
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class CountLinks(LinksQuery):
    """
    Count the distinct links in the links table, including
    ``('::', node)``, ``(node, '::')`` and ``('::', '::')``.

    .. note:: This query doesn't group replies by probe protocol and probe source address:
              it assumes that the table contains the replies for a single vantage point and a single protocol.
    """

    def query(self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET) -> str:
        return f"""
        SELECT uniqExact(near_addr, far_addr)
        FROM {links_table(measurement_id)}
        WHERE {self.filters(subset)}
        """
