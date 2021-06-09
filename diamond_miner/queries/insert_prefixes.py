from dataclasses import asdict, dataclass

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries import GetPrefixesWithAmplification, GetPrefixesWithLoops
from diamond_miner.queries.query import ResultsQuery, prefixes_table
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
            probe_protocol,
            probe_src_addr,
            probe_dst_prefix,
            has_amplification,
            has_loops
        FROM ({amplification_query}) AS amplification
        FULL OUTER JOIN ({loops_query}) AS loops
        ON  amplification.probe_protocol = loops.probe_protocol
        AND amplification.probe_src_addr = loops.probe_src_addr
        AND amplification.probe_dst_prefix = loops.probe_dst_prefix
        """
