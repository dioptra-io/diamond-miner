from collections import namedtuple

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries.query import ProbesQuery, probes_table
from diamond_miner.typing import IPNetwork


class GetProbes(ProbesQuery):
    """
    Return the number of probes sent.

    >>> from diamond_miner.test import addr_to_string, url
    >>> row = GetProbes.Row(*GetProbes(round_lt=2).execute(url, 'test_nsdi_example')[0])
    >>> row.protocol
    1
    >>> addr_to_string(row.dst_prefix)
    '200.0.0.0'
    >>> sorted(row.probes_per_ttl)
    [(1, 6), (2, 6), (3, 6), (4, 6)]
    >>> row = GetProbes.Row(*GetProbes(round_lt=4).execute(url, 'test_nsdi_example')[0])
    >>> sorted(row.probes_per_ttl)
    [(1, 11), (2, 20), (3, 27), (4, 27)]
    >>> row = GetProbes.Row(*GetProbes(round_eq=3).execute(url, 'test_nsdi_example')[0])
    >>> sorted(row.probes_per_ttl)
    [(2, 2), (3, 9), (4, 9)]
    """

    Row = namedtuple("Row", "protocol,dst_prefix,probes_per_ttl")

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        return f"""
        SELECT probe_protocol, probe_dst_prefix, groupArray((probe_ttl, total_probes))
        FROM (
            SELECT
                probe_protocol,
                probe_dst_prefix,
                probe_ttl,
                sum(n_probes) AS total_probes
            FROM {probes_table(measurement_id)}
            WHERE {self.filters(subset)}
            GROUP BY (probe_protocol, probe_dst_prefix, probe_ttl)
        )
        GROUP BY (probe_protocol, probe_dst_prefix)
        """
