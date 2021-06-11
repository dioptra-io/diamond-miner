from dataclasses import asdict, dataclass

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries import GetLinksFromView
from diamond_miner.queries.query import FlowsQuery, links_table
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class InsertLinks(FlowsQuery):
    """Create the tables necessary for a measurement."""

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        links_query = GetLinksFromView(**asdict(self)).statement(measurement_id, subset)
        return f"""
        INSERT INTO {links_table(measurement_id)}
        SELECT * FROM ({links_query})
        """
