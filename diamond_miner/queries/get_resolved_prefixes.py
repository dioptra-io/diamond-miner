from dataclasses import dataclass

from diamond_miner.queries.fragments import IPNetwork
from diamond_miner.queries.query import DEFAULT_SUBSET, Query, addr_to_string  # noqa


@dataclass(frozen=True)
class GetResolvedPrefixes(Query):
    """
    Return the prefixes for which no replies have been received at the previous round
    (i.e. no probes have been sent, most likely).

    >>> from diamond_miner.test import execute
    >>> execute(GetResolvedPrefixes(round_leq=1), 'test_nsdi_example')
    []
    >>> prefixes = execute(GetResolvedPrefixes(round_leq=5), 'test_nsdi_example')
    >>> [addr_to_string(pfx[0]) for pfx in prefixes]
    ['200.0.0.0']
    """

    def query(self, table: str, subset: IPNetwork = DEFAULT_SUBSET) -> str:
        assert self.round_leq is not None
        return f"""
        WITH {self.probe_dst_prefix()} AS probe_dst_prefix
        SELECT DISTINCT probe_dst_prefix
        FROM {table}
        WHERE {self.common_filters(subset)}
        GROUP BY (probe_src_addr, probe_dst_prefix)
        HAVING max(round) < {self.round_leq - 1}
        """
