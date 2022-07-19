from dataclasses import dataclass

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries.get_mda_probes import GetMDAProbes
from diamond_miner.queries.query import probes_table
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class InsertMDAProbes(GetMDAProbes):
    """
    Insert the result of the `GetMDAProbes` queries
    into the probes table.
    """

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        assert self.round_leq
        return f"""
        INSERT INTO {probes_table(measurement_id)}
        WITH
            arrayJoin(arrayZip(TTLs, cumulative_probes)) AS ttl_probe
        SELECT
            probe_protocol,
            probe_dst_prefix,
            ttl_probe.1,
            ttl_probe.2,
            {self.round_leq + 1}
        FROM ({super().statement(measurement_id, subset)})
        """
