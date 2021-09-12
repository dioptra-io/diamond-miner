from dataclasses import asdict, dataclass
from typing import Sequence

from diamond_miner.defaults import (
    DEFAULT_PREFIX_LEN_V4,
    DEFAULT_PREFIX_LEN_V6,
    UNIVERSE_SUBSET,
)
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

    prefix_len_v4: int = DEFAULT_PREFIX_LEN_V4
    prefix_len_v6: int = DEFAULT_PREFIX_LEN_V6

    def statements(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> Sequence[str]:
        # Only CreateResultsTable accepts these parameters.
        params = {
            x: y
            for x, y in asdict(self).items()
            if x not in ["prefix_len_v4", "prefix_len_v6"]
        }
        return (
            *CreateResultsTable(**asdict(self)).statements(measurement_id, subset),
            *CreateFlowsView(**params).statements(measurement_id, subset),
            *CreateLinksTable(**params).statements(measurement_id, subset),
            *CreatePrefixesTable(**params).statements(measurement_id, subset),
        )
