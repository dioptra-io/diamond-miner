from dataclasses import asdict, dataclass
from typing import Sequence

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries import (
    CreateFlowsView,
    CreateLinksTable,
    CreatePrefixesTable,
    CreateResultsTable,
)
from diamond_miner.queries.query import Query
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class CreateTables(Query):
    """Create the tables necessary for a measurement."""

    def statements(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> Sequence[str]:
        return (
            *CreateResultsTable(**asdict(self)).statements(measurement_id, subset),
            *CreateFlowsView(**asdict(self)).statements(measurement_id, subset),
            *CreateLinksTable(**asdict(self)).statements(measurement_id, subset),
            *CreatePrefixesTable(**asdict(self)).statements(measurement_id, subset),
        )
