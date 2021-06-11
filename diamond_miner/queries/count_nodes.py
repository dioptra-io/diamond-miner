from dataclasses import dataclass

from diamond_miner.queries.query import (
    UNIVERSE_SUBSET,
    LinksQuery,
    ResultsQuery,
    links_table,
    results_table,
)
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class CountNodes(LinksQuery):
    """
    Count the distinct nodes from the links table, including the ``::`` node.

    .. note:: This query doesn't group replies by probe protocol and probe source address:
              it assumes that the table contains the replies for a single vantage point and a single protocol.

    >>> from diamond_miner.test import url
    >>> CountNodes().execute(url, 'test_nsdi_example')[0][0]
    7
    """

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        return f"""
        SELECT arrayUniq(
            arrayConcat(
                groupUniqArray(near_addr),
                groupUniqArray(far_addr)
            )
        )
        FROM {links_table(measurement_id)}
        WHERE {self.filters(subset)}
        """


@dataclass(frozen=True)
class CountNodesFromResults(ResultsQuery):
    """
    Count the distinct nodes from the results table.
    This query does not support the ``subset`` parameter.

    >>> from diamond_miner.test import url
    >>> CountNodesFromResults().execute(url, 'test_nsdi_example')[0][0]
    7
    """

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        assert subset == UNIVERSE_SUBSET, "subset not allowed for this query"
        return f"""
        SELECT uniqExact(reply_src_addr)
        FROM {results_table(measurement_id)}
        WHERE {self.filters(subset)}
        """
