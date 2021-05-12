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
        SELECT DISTINCT probe_protocol, {self.addr_cast('reply_src_addr')}
        FROM {table}
        WHERE {self.common_filters(subset)}
        """


@dataclass(frozen=True)
class GetNodes(Query):
    # NOTE: It counts the node '::'
    # Does not group by probe_protocol and probe_src_addr

    def query(self, table: str, subset: IPNetwork = DEFAULT_SUBSET) -> str:
        return f"""
        SELECT
            arrayJoin(
                arrayDistinct(
                    arrayConcat(
                        groupUniqArray({self.addr_cast('near_addr')}),
                        groupUniqArray({self.addr_cast('far_addr')})
                    )
                )
            )
        FROM {table}
        """


@dataclass(frozen=True)
class GetNodesPerPrefix(Query):
    # NOTE: It counts the links ('::', a), (a, '::') and ('::', '::')

    def query(self, table: str, subset: IPNetwork = DEFAULT_SUBSET) -> str:
        return f"""
        SELECT
            probe_protocol,
            probe_src_addr,
            probe_src_addr,
            arrayDistinct(
                    arrayConcat(
                        groupUniqArray({self.addr_cast('near_addr')}),
                        groupUniqArray({self.addr_cast('far_addr')})
                    )
                )
        FROM {table}
        GROUP BY (probe_protocol, probe_src_addr, probe_dst_prefix)
        """
