from dataclasses import dataclass

from diamond_miner.queries.fragments import IPNetwork, cut_ipv6
from diamond_miner.queries.query import DEFAULT_SUBSET, Query, addr_to_string  # noqa
from diamond_miner.test import execute  # noqa


@dataclass(frozen=True)
class CountReplies(Query):
    """
    Count replies by "chunks".
    >>> rows = execute(CountReplies(chunk_len_v4=8, chunk_len_v6=8), 'test_count_replies')
    >>> sorted((addr_to_string(a), b) for a, b in rows)
    [('0.0.0.0', 2), ('1.0.0.0', 1), ('230.0.0.0', 1)]
    """

    chunk_len_v4: int = 8
    chunk_len_v6: int = 8

    def query(self, table: str, subset: IPNetwork = DEFAULT_SUBSET) -> str:
        return f"""
        WITH {cut_ipv6('probe_dst_addr', self.chunk_len_v4, self.chunk_len_v6)} AS chunk
        SELECT chunk, count()
        FROM {table}
        WHERE {self.common_filters(subset)}
        GROUP BY chunk
        """