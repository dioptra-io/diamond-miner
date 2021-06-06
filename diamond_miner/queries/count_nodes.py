from dataclasses import dataclass

from diamond_miner.queries.query import UNIVERSE_SUBSET, LinksQuery, ResultsQuery
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class CountNodes(LinksQuery):
    """
    Count the distinct nodes from the links table, including the ``::`` node.

    .. note:: This query doesn't group replies by probe protocol and probe source address:
              it assumes that the table contains the replies for a single vantage point and a single protocol.

    >>> from diamond_miner.test import client
    >>> CountNodes().execute(client, 'test_nsdi_example_links')[0][0]
    7
    """

    def query(self, table: str, subset: IPNetwork = UNIVERSE_SUBSET) -> str:
        return f"""
        SELECT arrayUniq(
            arrayConcat(
                groupUniqArray(near_addr),
                groupUniqArray(far_addr)
            )
        )
        FROM {table}
        WHERE {self.filters(subset)}
        """


@dataclass(frozen=True)
class CountNodesFromResults(ResultsQuery):
    """
    Count the distinct nodes from the results table.
    This query does not support the ``subset`` parameter.

    >>> from diamond_miner.test import client
    >>> CountNodesFromResults().execute(client, 'test_nsdi_example')[0][0]
    7
    """

    def query(self, table: str, subset: IPNetwork = UNIVERSE_SUBSET) -> str:
        assert subset == UNIVERSE_SUBSET, "subset not allowed for this query"
        return f"""
        SELECT uniqExact(reply_src_addr)
        FROM {table}
        WHERE {self.filters(subset)}
        """
