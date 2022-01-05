from dataclasses import dataclass

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries.query import ResultsQuery, results_table
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class GetSlidingPrefixes(ResultsQuery):
    """
    Get the prefixes to probe for a given sliding window.
    """

    stopping_condition: int = 0  # number of stars
    window_max_ttl: int = 0  # set to 0 to return every prefix

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        assert self.filter_destination_host and self.time_exceeded_only
        return f"""
        SELECT
            probe_protocol,
            probe_src_addr,
            probe_dst_prefix
        FROM {results_table(measurement_id)}
        WHERE {self.filters(subset)}
        AND probe_ttl >= {self.window_max_ttl - self.stopping_condition}
        GROUP BY (
            probe_protocol,
            probe_src_addr,
            probe_dst_prefix
        )
        """
