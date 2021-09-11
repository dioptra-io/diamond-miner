from dataclasses import dataclass
from typing import Optional

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries import CreatePrefixesTable
from diamond_miner.queries.fragments import ip_in
from diamond_miner.queries.query import ResultsQuery, prefixes_table, results_table
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class GetPrefixes(ResultsQuery):
    """
    Return the destination prefixes for which replies have been received.

    >>> from diamond_miner.test import addr_to_string, url
    >>> from ipaddress import ip_network
    >>> rows = GetPrefixes().execute(url, 'test_nsdi_example')
    >>> len(rows)
    1
    >>> rows = GetPrefixes().execute(url, 'test_invalid_prefixes')
    >>> len(rows)
    3
    >>> rows = GetPrefixes(reply_src_addr_in=ip_network("150.0.1.0/24")).execute(url, 'test_invalid_prefixes')
    >>> len(rows)
    1
    """

    reply_src_addr_in: Optional[IPNetwork] = None

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        # The prefixes table doesn't contains network information, so we
        # need to join the results table for these filters.
        join_fragment = ""
        if self.reply_src_addr_in:
            join_fragment = f"""
            INNER JOIN (
                SELECT DISTINCT probe_protocol, probe_src_addr, probe_dst_prefix
                FROM {results_table(measurement_id)}
                WHERE {self.filters(subset)} AND {ip_in('reply_src_addr', self.reply_src_addr_in)}
            ) AS results
            ON  prefixes.probe_protocol   = results.probe_protocol
            AND prefixes.probe_src_addr   = results.probe_src_addr
            AND prefixes.probe_dst_prefix = results.probe_dst_prefix
            """
        return f"""
        SELECT probe_dst_prefix, has_amplification, has_loops
        FROM {prefixes_table(measurement_id)} AS prefixes
        {join_fragment}
        ORDER BY {CreatePrefixesTable.SORTING_KEY}
        """
