from dataclasses import dataclass

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries.query import ResultsQuery, results_table
from diamond_miner.typing import IPNetwork

# TODO: Implement over the flows view instead?


@dataclass(frozen=True)
class GetTraceroutes(ResultsQuery):
    """
    Return all the columns from the results table.

    >>> from diamond_miner.test import url
    >>> rows = GetTraceroutes().execute(url, 'test_nsdi_example')
    >>> len(rows)
    27
    """

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        return f"""
        SELECT
            probe_protocol,
            probe_src_addr,
            probe_dst_addr,
            probe_src_port,
            groupArray((
                toUnixTimestamp(capture_timestamp),
                probe_ttl,
                reply_ttl,
                reply_size,
                reply_mpls_labels,
                reply_src_addr,
                rtt
            )) AS replies
        FROM {results_table(measurement_id)}
        WHERE {self.filters(subset)}
        GROUP BY (
            probe_protocol,
            probe_src_addr,
            probe_dst_addr,
            probe_src_port,
            probe_dst_port,
        )
        """
