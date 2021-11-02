from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries import ProbesQuery, probes_table
from diamond_miner.typing import IPNetwork


class DeleteProbes(ProbesQuery):
    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        return f"""
        ALTER TABLE {probes_table(measurement_id)}
        DELETE WHERE {self.filters(subset)}
        SETTINGS mutations_sync = 1
        """
