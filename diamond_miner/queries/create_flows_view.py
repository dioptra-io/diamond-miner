from dataclasses import dataclass

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries.fragments import date_time
from diamond_miner.queries.query import (
    ResultsQuery,
    StoragePolicy,
    flows_table,
    results_table,
)
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class CreateFlowsView(ResultsQuery):
    """Create the flows view."""

    PRIMARY_KEY = "probe_protocol, probe_src_addr, probe_dst_prefix"
    SORTING_KEY = "probe_protocol, probe_src_addr, probe_dst_prefix, probe_dst_addr, probe_src_port, probe_dst_port"

    storage_policy: StoragePolicy = StoragePolicy()

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        assert subset == UNIVERSE_SUBSET, "subset not allowed for this query"
        return f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {flows_table(measurement_id)}
        ENGINE = AggregatingMergeTree
        ORDER BY ({self.SORTING_KEY})
        PRIMARY KEY ({self.PRIMARY_KEY})
        TTL {date_time(self.storage_policy.archive_on)} TO VOLUME '{self.storage_policy.archive_to}'
        SETTINGS storage_policy = '{self.storage_policy.name}'
        AS SELECT
            round,
            {self.SORTING_KEY},
            -- The duplication of the round column here allows us to drop `round`
            -- from the GROUP BY clause when querying the view, and still get the
            -- round information in the replies. This is useful for computing
            -- inter-round links.
            groupUniqArrayState((round, probe_ttl, reply_src_addr)) AS replies
        FROM {results_table(measurement_id)}
        WHERE {self.filters(subset)}
        GROUP BY (round, {self.SORTING_KEY})
        """
