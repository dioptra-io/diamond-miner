from dataclasses import dataclass

from diamond_miner.queries.query import DEFAULT_SUBSET, Query
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class CountLinks(Query):
    # NOTE: It counts the links ('::', a), (a, '::') and ('::', '::')
    # Does not group by probe_protocol and probe_src_addr

    def query(self, table: str, subset: IPNetwork = DEFAULT_SUBSET) -> str:
        return f"""
        SELECT uniqExact(near_addr, far_addr)
        FROM {table}
        """
