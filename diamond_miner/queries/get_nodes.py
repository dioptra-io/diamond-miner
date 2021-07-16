from dataclasses import dataclass
from typing import List

from diamond_miner.queries.query import UNIVERSE_SUBSET, ResultsQuery, results_table
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class GetNodes(ResultsQuery):
    """
    Return all the discovered nodes.

    >>> from diamond_miner.test import addr_to_string, url
    >>> nodes = GetNodes().execute(url, 'test_nsdi_example')
    >>> sorted(addr_to_string(x[0]) for x in nodes)
    ['150.0.1.1', '150.0.2.1', '150.0.3.1', '150.0.4.1', '150.0.5.1', '150.0.6.1', '150.0.7.1']
    """

    include_reply_ttl: bool = False
    "If true, include the TTL at which `reply_src_addr` was seen."

    def columns(self) -> List[str]:
        columns = [self.addr_cast("reply_src_addr")]
        if self.include_reply_ttl:
            columns.insert(0, "reply_ttl")
        return columns

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        return f"""
        SELECT DISTINCT {','.join(self.columns())}
        FROM {results_table(measurement_id)}
        WHERE {self.filters(subset)}
        """
