from collections import namedtuple

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries.query import ProbesQuery, probes_table
from diamond_miner.typing import IPNetwork


class GetProbes(ProbesQuery):
    """
    Return the cumulative number of probes sent at a specified round.

    >>> from diamond_miner.test import addr_to_string, url
    >>> row = GetProbes.Row(*GetProbes(round_eq=1).execute(url, 'test_nsdi_example')[0])
    >>> row.protocol
    1
    >>> addr_to_string(row.dst_prefix)
    '200.0.0.0'
    >>> sorted(row.probes_per_ttl)
    [(1, 6), (2, 6), (3, 6), (4, 6)]
    >>> row = GetProbes.Row(*GetProbes(round_eq=2).execute(url, 'test_nsdi_example')[0])
    >>> sorted(row.probes_per_ttl)
    [(1, 11), (2, 18), (3, 18), (4, 18)]
    >>> row = GetProbes.Row(*GetProbes(round_eq=3).execute(url, 'test_nsdi_example')[0])
    >>> sorted(row.probes_per_ttl)
    [(1, 11), (2, 20), (3, 27), (4, 27)]
    >>> row = GetProbes.Row(*GetProbes(round_eq=3, probe_ttl_geq=2, probe_ttl_leq=3).execute(url, 'test_nsdi_example')[0])
    >>> sorted(row.probes_per_ttl)
    [(2, 20), (3, 27)]
    >>> GetProbes(round_eq=4).execute(url, 'test_nsdi_example')
    []
    """

    Row = namedtuple("Row", "protocol,dst_prefix,probes_per_ttl")

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        assert self.round_eq, "`round_eq` must be specified."
        return f"""
        SELECT
            probe_protocol,
            probe_dst_prefix,
            groupArray((probe_ttl, cumulative_probes)) AS probes_per_ttl
        FROM {probes_table(measurement_id)}
        WHERE {self.filters(subset)}
        GROUP BY (probe_protocol, probe_dst_prefix)
        """


class GetProbesDiff(ProbesQuery):
    """
    Return the number of probes sent at a specific round and at the previous round.

    >>> from diamond_miner.test import addr_to_string, url
    >>> row = GetProbesDiff.Row(*GetProbesDiff(round_eq=1).execute(url, 'test_nsdi_example')[0])
    >>> row.protocol
    1
    >>> addr_to_string(row.dst_prefix)
    '200.0.0.0'
    >>> sorted(row.probes_per_ttl)
    [(1, 6, 0), (2, 6, 0), (3, 6, 0), (4, 6, 0)]
    >>> row = GetProbesDiff.Row(*GetProbesDiff(round_eq=2).execute(url, 'test_nsdi_example')[0])
    >>> sorted(row.probes_per_ttl)
    [(1, 11, 6), (2, 18, 6), (3, 18, 6), (4, 18, 6)]
    >>> row = GetProbesDiff.Row(*GetProbesDiff(round_eq=3).execute(url, 'test_nsdi_example')[0])
    >>> sorted(row.probes_per_ttl)
    [(1, 11, 11), (2, 20, 18), (3, 27, 18), (4, 27, 18)]
    >>> GetProbesDiff(round_eq=4).execute(url, 'test_nsdi_example')
    []
    """

    Row = namedtuple("Row", "protocol,dst_prefix,probes_per_ttl")

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        assert self.round_eq
        return f"""
        SELECT
            current.probe_protocol,
            current.probe_dst_prefix,
            groupArray((current.probe_ttl, current.cumulative_probes, previous.cumulative_probes)) AS probes_per_ttl
        FROM {probes_table(measurement_id)} AS current
        LEFT JOIN (
            SELECT
                probe_protocol,
                probe_dst_prefix,
                probe_ttl,
                cumulative_probes
            FROM {probes_table(measurement_id)}
            WHERE round = {self.round_eq - 1}
            -- TODO: Filter on subset for greater performance?
        ) AS previous
        ON current.probe_protocol = previous.probe_protocol
        AND current.probe_dst_prefix = previous.probe_dst_prefix
        AND current.probe_ttl = previous.probe_ttl
        WHERE {self.filters(subset)}
        GROUP BY (current.probe_protocol, current.probe_dst_prefix)
        """
