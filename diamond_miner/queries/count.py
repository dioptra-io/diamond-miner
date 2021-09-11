from dataclasses import dataclass
from typing import Optional

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries.query import Query
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class Count(Query):
    """
    Count the rows of a given query.

    >>> from diamond_miner.test import url
    >>> from diamond_miner.queries.get_nodes import GetNodes
    >>> from diamond_miner.queries.get_links import GetLinks
    >>> Count(query=GetNodes()).execute(url, 'test_nsdi_example')[0][0]
    7
    >>> Count(query=GetLinks()).execute(url, 'test_nsdi_example')[0][0]
    8
    """

    query: Optional[Query] = None
    "The query for which to count the nodes."

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        # `query` must be typed `Optional` since it appears after arguments with default values.
        assert self.query is not None
        return f"SELECT COUNT() FROM ({self.query.statement(measurement_id, subset)})"
