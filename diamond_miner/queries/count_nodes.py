from dataclasses import dataclass

from diamond_miner.queries.fragments import IPNetwork
from diamond_miner.queries.query import DEFAULT_SUBSET, Query  # noqa


@dataclass(frozen=True)
class CountNodes(Query):
    """
    Count all the discovered nodes.

    >>> from diamond_miner.test import execute
    >>> execute(CountNodes(), 'test_nsdi_example')[0][0]
    7
    """

    def query(self, table: str, subset: IPNetwork = DEFAULT_SUBSET) -> str:
        return f"""
        SELECT uniqExact(reply_src_addr)
        FROM {table}
        WHERE {self.common_filters(subset)}
        """
