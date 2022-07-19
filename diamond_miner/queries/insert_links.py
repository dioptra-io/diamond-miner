from dataclasses import asdict, dataclass

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries import GetLinksFromResults
from diamond_miner.queries.query import ResultsQuery, links_table
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class InsertLinks(ResultsQuery):
    """
    Insert the results of the `GetLinksFromResults` query into the links table.
    """

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        links_query = GetLinksFromResults(**asdict(self)).statement(
            measurement_id, subset
        )
        return f"""
        INSERT INTO {links_table(measurement_id)}
        SELECT * FROM ({links_query})
        """
