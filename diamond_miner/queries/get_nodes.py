from dataclasses import dataclass

from diamond_miner.queries.fragments import IPNetwork
from diamond_miner.queries.query import DEFAULT_SUBSET, Query, addr_to_string  # noqa


@dataclass(frozen=True)
class GetNodes(Query):
    """
    Return all the discovered nodes.

    >>> from diamond_miner.test import execute
    >>> nodes = execute(GetNodes(), 'test_nsdi_example')
    >>> sorted(addr_to_string(node[0]) for node in nodes)
    ['150.0.1.1', '150.0.2.1', '150.0.3.1', '150.0.4.1', '150.0.5.1', '150.0.6.1', '150.0.7.1']
    """

    def query(self, table: str, subset: IPNetwork = DEFAULT_SUBSET) -> str:
        return f"""
        WITH {self.probe_dst_prefix()} AS probe_dst_prefix
        SELECT DISTINCT reply_src_addr
        FROM {table}
        WHERE {self.common_filters(subset)}
        """
