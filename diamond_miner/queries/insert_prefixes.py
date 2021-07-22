from dataclasses import asdict, dataclass

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries import GetPrefixesWithAmplification, GetPrefixesWithLoops
from diamond_miner.queries.query import ResultsQuery, prefixes_table, results_table
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class InsertPrefixes(ResultsQuery):
    """Insert (invalid) prefixes into the prefixes table."""

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        amplification_query = GetPrefixesWithAmplification(**asdict(self)).statement(
            measurement_id, subset
        )
        loops_query = GetPrefixesWithLoops(**asdict(self)).statement(
            measurement_id, subset
        )
        return f"""
        INSERT INTO {prefixes_table(measurement_id)}
        SELECT
            prefixes.probe_protocol,
            prefixes.probe_src_addr,
            prefixes.probe_dst_prefix,
            amplification.has_amplification,
            loops.has_loops
        FROM (
            SELECT DISTINCT probe_protocol, probe_src_addr, probe_dst_prefix
            FROM {results_table(measurement_id)}
            WHERE {self.filters(subset)}
        ) AS prefixes
        FULL OUTER JOIN ({amplification_query}) AS amplification
        ON  prefixes.probe_protocol   = amplification.probe_protocol
        AND prefixes.probe_src_addr   = amplification.probe_src_addr
        AND prefixes.probe_dst_prefix = amplification.probe_dst_prefix
        FULL OUTER JOIN ({loops_query}) AS loops
        ON  prefixes.probe_protocol   = loops.probe_protocol
        AND prefixes.probe_src_addr   = loops.probe_src_addr
        AND prefixes.probe_dst_prefix = loops.probe_dst_prefix
        WHERE prefixes.probe_dst_prefix != toIPv6('::')
        """
