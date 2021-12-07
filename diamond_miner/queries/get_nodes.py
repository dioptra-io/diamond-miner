from dataclasses import dataclass
from typing import List

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries import GetInvalidPrefixes
from diamond_miner.queries.query import ResultsQuery, results_table
from diamond_miner.typing import IPNetwork
from diamond_miner.utilities import common_parameters


@dataclass(frozen=True)
class GetNodes(ResultsQuery):
    """
    Return all the discovered nodes.

    >>> from diamond_miner.test import url
    >>> nodes = GetNodes(include_probe_ttl=True).execute(url, 'test_nsdi_example')
    >>> sorted((node["probe_ttl"], node["reply_src_addr"]) for node in nodes)
    [(1, '::ffff:150.0.1.1'), (2, '::ffff:150.0.2.1'), (2, '::ffff:150.0.3.1'), (3, '::ffff:150.0.4.1'), (3, '::ffff:150.0.5.1'), (3, '::ffff:150.0.7.1'), (4, '::ffff:150.0.6.1')]
    >>> nodes = GetNodes(filter_invalid_prefixes=False).execute(url, 'test_invalid_prefixes')
    >>> sorted(node["reply_src_addr"] for node in nodes)
    ['::ffff:150.0.0.1', '::ffff:150.0.0.2', '::ffff:150.0.1.1', '::ffff:150.0.1.2', '::ffff:150.0.2.1', '::ffff:150.0.2.2', '::ffff:150.0.2.3']
    >>> nodes = GetNodes(filter_invalid_prefixes=True).execute(url, 'test_invalid_prefixes')
    >>> sorted(node["reply_src_addr"] for node in nodes)
    ['::ffff:150.0.0.1', '::ffff:150.0.0.2', '::ffff:150.0.2.3']
    """

    filter_invalid_prefixes: bool = False
    "If true, exclude nodes from prefixes with amplification or loops."

    include_probe_ttl: bool = False
    "If true, include the TTL at which `reply_src_addr` was seen."

    def columns(self) -> List[str]:
        columns = ["reply_src_addr"]
        if self.include_probe_ttl:
            columns.insert(0, "probe_ttl")
        return columns

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        if self.filter_invalid_prefixes:
            invalid_prefixes_query = GetInvalidPrefixes(
                **common_parameters(self, GetInvalidPrefixes)
            )
            prefix_filter = f"""
                    probe_dst_prefix NOT IN ({invalid_prefixes_query.statement(measurement_id, subset)})
                    """
        else:
            prefix_filter = "1"
        return f"""
        SELECT DISTINCT {','.join(self.columns())}
        FROM {results_table(measurement_id)}
        WHERE {self.filters(subset)} AND {prefix_filter}
        """
