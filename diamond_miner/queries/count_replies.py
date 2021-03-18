from dataclasses import dataclass

from diamond_miner.queries.query import IPNetwork, Query, addr_to_string, ipv6  # noqa
from diamond_miner.test import execute  # noqa


@dataclass(frozen=True)
class CountReplies(Query):
    """
    Count replies by "chunks".
    >>> rows = execute(CountReplies('100.0.0.1'), 'test_count_replies')
    >>> sorted((addr_to_string(a), b) for a, b in rows)
    [('0.0.0.0', 2), ('1.0.0.0', 1), ('230.0.0.0', 1)]
    """

    source: str
    preflen_v4: int = 8
    preflen_v6: int = 8

    def _query(self, table: str, subset: IPNetwork):
        v4_bytes = int(4 - (self.preflen_v4 / 8))
        v6_bytes = int(16 - (self.preflen_v6 / 8))
        return f"""
        WITH toIPv6(cutIPv6(probe_dst_addr, {v6_bytes}, {v4_bytes})) AS chunk
        SELECT chunk, count()
        FROM {table}
        WHERE probe_src_addr = {ipv6(self.source)}
        GROUP BY chunk
        """
