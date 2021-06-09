from dataclasses import dataclass
from typing import Optional

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries import GetLinksFromView
from diamond_miner.queries.query import Query, links_table
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class InsertLinks(Query):
    """Create the tables necessary for a measurement."""

    round_eq: Optional[int] = None
    "See :attr:`GetLinksFromView.round_eq`"

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        return f"""
        INSERT INTO {links_table(measurement_id)}
        SELECT * FROM ({GetLinksFromView(round_eq=self.round_eq).statement(measurement_id)})
        """
