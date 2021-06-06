from dataclasses import dataclass

from diamond_miner.queries.query import UNIVERSE_SUBSET, LinksQuery
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class GetLinks(LinksQuery):
    """
    Return the links pre-computed in the links table.
    This doesn't group replies by probe protocol and probe source address,
    in other words, it assumes that the table contains the replies for a
    single vantage point and a single protocol.
    """

    def query(self, table: str, subset: IPNetwork = UNIVERSE_SUBSET) -> str:
        return f"""
        SELECT DISTINCT ({self.addr_cast('near_addr')}, {self.addr_cast('far_addr')})
        FROM {table}
        WHERE {self.filters(subset)}
        """


@dataclass(frozen=True)
class GetLinksPerPrefix(LinksQuery):
    """
    Return the links pre-computed in the links table, grouped by
    protocol, source address and destination prefix.
    """

    def query(self, table: str, subset: IPNetwork = UNIVERSE_SUBSET) -> str:
        return f"""
        SELECT
            probe_protocol,
            probe_src_addr,
            probe_dst_prefix,
            groupUniqArray(
                ({self.addr_cast('near_addr')}, {self.addr_cast('far_addr')})
            )
        FROM {table}
        WHERE {self.filters(subset)}
        GROUP BY (probe_protocol, probe_src_addr, probe_dst_prefix)
        """
