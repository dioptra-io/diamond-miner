from dataclasses import dataclass
from typing import List

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries.query import ResultsQuery, results_table
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class GetNodes(ResultsQuery):
    """
    Return all the discovered nodes.

    >>> from diamond_miner.test import addr_to_string, url
    >>> nodes = GetNodes(include_probe_ttl=True).execute(url, 'test_nsdi_example')
    >>> sorted((x[0], addr_to_string(x[1])) for x in nodes)
    [(1, '150.0.1.1'), (2, '150.0.2.1'), (2, '150.0.3.1'), (3, '150.0.4.1'), (3, '150.0.5.1'), (3, '150.0.7.1'), (4, '150.0.6.1')]
    """

    include_probe_ttl: bool = False
    "If true, include the TTL at which `reply_src_addr` was seen."

    def columns(self) -> List[str]:
        columns = [self._addr_cast("reply_src_addr")]
        if self.include_probe_ttl:
            columns.insert(0, "probe_ttl")
        return columns

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        return f"""
        SELECT DISTINCT {','.join(self.columns())}
        FROM {results_table(measurement_id)}
        WHERE {self.filters(subset)}
        """
