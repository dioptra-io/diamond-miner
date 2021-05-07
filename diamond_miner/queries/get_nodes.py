from dataclasses import dataclass

from diamond_miner.queries.query import DEFAULT_SUBSET, Query, addr_to_string  # noqa
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class GetNodesFromResults(Query):
    """
    Return all the discovered nodes.

    >>> from diamond_miner.test import client
    >>> nodes = GetNodesFromResults().execute(client, 'test_nsdi_example')
    >>> sorted((x[0], addr_to_string(x[1])) for x in nodes)
    [(1, '150.0.1.1'), (1, '150.0.2.1'), (1, '150.0.3.1'), (1, '150.0.4.1'), (1, '150.0.5.1'), (1, '150.0.6.1'), (1, '150.0.7.1')]
    """

    def query(self, table: str, subset: IPNetwork = DEFAULT_SUBSET) -> str:
        return f"""
        SELECT DISTINCT probe_protocol, reply_src_addr
        FROM {table}
        WHERE {self.common_filters(subset)}
        """


@dataclass(frozen=True)
class GetNodes(Query):
    # NOTE: It counts the node '::'
    # Slower than computing on results table

    def query(self, table: str, subset: IPNetwork = DEFAULT_SUBSET) -> str:
        return f"""
        SELECT near_addr FROM {table}
        UNION DISTINCT
        SELECT far_addr FROM {table}
        """
