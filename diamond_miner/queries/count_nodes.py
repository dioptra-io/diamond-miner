from dataclasses import dataclass

from diamond_miner.queries.query import DEFAULT_SUBSET, Query  # noqa
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class CountNodesFromResults(Query):
    """
    Return the number of nodes discovered.
    This query does not support the ``subset`` parameter.

    >>> from diamond_miner.test import client
    >>> CountNodesFromResults().execute(client, 'test_nsdi_example')[0][0]
    7
    """

    def query(self, table: str, subset: IPNetwork = DEFAULT_SUBSET) -> str:
        assert subset == DEFAULT_SUBSET, "subset not allowed for this query"
        return f"""
        SELECT uniqExact(reply_src_addr)
        FROM {table}
        WHERE {self.common_filters(subset)}
        """


@dataclass(frozen=True)
class CountNodesFromLinks(Query):
    # NOTE: It counts the node '::'
    # Slower than computing on results table

    def query(self, table: str, subset: IPNetwork = DEFAULT_SUBSET) -> str:
        return f"""
        SELECT uniqExact(*)
        FROM
        (
            SELECT near_addr FROM {table}
            UNION DISTINCT
            SELECT far_addr FROM {table}
        )
        """
