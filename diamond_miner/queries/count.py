from dataclasses import dataclass

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries.query import Query
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class Count(Query):
    """
    Count the number of rows returned by a given query.

    Examples:
        >>> from diamond_miner.test import client
        >>> from diamond_miner.queries import GetLinks, GetNodes
        >>> Count(query=GetNodes()).execute(client, 'test_nsdi_example')[0]["count()"]
        7
        >>> Count(query=GetLinks()).execute(client, 'test_nsdi_example')[0]["count()"]
        8
    """

    query: Query | None = None
    "The query for which to count the nodes."

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        # `query` must be typed `Optional` since it appears after arguments with default values.
        assert self.query is not None
        return f"SELECT count() FROM ({self.query.statement(measurement_id, subset)})"
