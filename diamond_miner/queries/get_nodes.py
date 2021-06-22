from dataclasses import dataclass

from diamond_miner.queries.query import UNIVERSE_SUBSET, ResultsQuery, results_table
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class GetNodes(ResultsQuery):
    """
    Return all the discovered nodes.

    .. note:: This doesn't group replies by probe protocol and probe source address,
    in other words, it assumes that the table contains the replies for a
    single vantage point and a single protocol.

    >>> from diamond_miner.test import addr_to_string, url
    >>> nodes = GetNodes().execute(url, 'test_nsdi_example')
    >>> sorted(addr_to_string(x[0]) for x in nodes)
    ['150.0.1.1', '150.0.2.1', '150.0.3.1', '150.0.4.1', '150.0.5.1', '150.0.6.1', '150.0.7.1']
    """

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        return f"""
        SELECT DISTINCT {self.addr_cast('reply_src_addr')}
        FROM {results_table(measurement_id)}
        WHERE {self.filters(subset)}
        """


@dataclass(frozen=True)
class GetNodesPerPrefix(ResultsQuery):
    """
    Return the nodes, grouped by protocol, source address and destination prefix.

    >>> from diamond_miner.test import addr_to_string, url
    >>> nodes = GetNodesPerPrefix().execute(url, 'test_nsdi_example')[0][3]
    >>> sorted(addr_to_string(x) for x in nodes)
    ['150.0.1.1', '150.0.2.1', '150.0.3.1', '150.0.4.1', '150.0.5.1', '150.0.6.1', '150.0.7.1']
    """

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        return f"""
        SELECT
            probe_protocol,
            probe_src_addr,
            probe_dst_prefix,
            groupUniqArray({self.addr_cast('reply_src_addr')})
        FROM {results_table(measurement_id)}
        WHERE {self.filters(subset)}
        GROUP BY (probe_protocol, probe_src_addr, probe_dst_prefix)
        """
