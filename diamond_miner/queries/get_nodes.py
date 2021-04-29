from dataclasses import dataclass

from diamond_miner.queries.query import DEFAULT_SUBSET, Query, addr_to_string  # noqa
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class GetNodes(Query):
    """
    Return all the discovered nodes.

    >>> from diamond_miner.test import execute
    >>> nodes = execute(GetNodes(), 'test_nsdi_example')
    >>> sorted((x[0], addr_to_string(x[1])) for x in nodes)
    [(1, '150.0.1.1'), (1, '150.0.2.1'), (1, '150.0.3.1'), (1, '150.0.4.1'), (1, '150.0.5.1'), (1, '150.0.6.1'), (1, '150.0.7.1')]
    """

    def query(self, table: str, subset: IPNetwork = DEFAULT_SUBSET) -> str:
        return f"""
        SELECT DISTINCT probe_protocol, reply_src_addr
        FROM {table}
        WHERE {self.common_filters(subset)}
        """
