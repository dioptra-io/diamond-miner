from dataclasses import dataclass

from diamond_miner.queries.query import UNIVERSE_SUBSET, LinksQuery, links_table
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class GetLinks(LinksQuery):
    """
    Return the links pre-computed in the links table.
    This doesn't group replies by probe protocol and probe source address,
    in other words, it assumes that the table contains the replies for a
    single vantage point and a single protocol.
    """

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        return f"""
        SELECT DISTINCT ({self.addr_cast('near_addr')}, {self.addr_cast('far_addr')})
        FROM {links_table(measurement_id)}
        WHERE {self.filters(subset)}
        """


@dataclass(frozen=True)
class GetLinksPerPrefix(LinksQuery):
    """
    Return the links pre-computed in the links table, grouped by
    protocol, source address and destination prefix.
    """

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        return f"""
        SELECT
            probe_protocol,
            probe_src_addr,
            probe_dst_prefix,
            groupUniqArray(
                ({self.addr_cast('near_addr')}, {self.addr_cast('far_addr')})
            )
        FROM {links_table(measurement_id)}
        WHERE {self.filters(subset)}
        GROUP BY (probe_protocol, probe_src_addr, probe_dst_prefix)
        """
