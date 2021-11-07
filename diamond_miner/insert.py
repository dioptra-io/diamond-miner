import os
from typing import Iterable, Tuple

from diamond_miner.defaults import (
    DEFAULT_FAILURE_RATE,
    DEFAULT_PREFIX_LEN_V4,
    DEFAULT_PREFIX_LEN_V6,
    PROTOCOLS,
    UNIVERSE_SUBSET,
)
from diamond_miner.generators.standalone import split_prefix
from diamond_miner.queries import InsertMDAProbes
from diamond_miner.queries.query import client, probes_table
from diamond_miner.subsets import subsets_for
from diamond_miner.typing import IPNetwork


def insert_probe_counts(
    url: str,
    measurement_id: str,
    round_: int,
    # prefix (/32 or / 128 if nothing specified), protocol, ttls, n_probes
    prefixes: Iterable[Tuple[str, str, Iterable[int], int]],
    prefix_len_v4: int = DEFAULT_PREFIX_LEN_V4,
    prefix_len_v6: int = DEFAULT_PREFIX_LEN_V6,
    batch_size: int = 1_000_000,
) -> None:
    """
    >>> from diamond_miner.test import addr_to_string, create_tables, url
    >>> from diamond_miner.queries import GetProbes
    >>> create_tables(url, "test_probe_counts")
    >>> insert_probe_counts(url, "test_probe_counts", 1, [("8.8.0.0/22", "icmp", range(2, 5), 6)], batch_size=2)
    >>> rows = sorted(GetProbes(round_eq=1).execute(url, "test_probe_counts"))
    >>> len(rows)
    4
    >>> row = GetProbes.Row(*rows[0])
    >>> addr_to_string(row.dst_prefix)
    '8.8.0.0'
    >>> sorted(row.probes_per_ttl)
    [(2, 6), (3, 6), (4, 6)]
    >>> row = GetProbes.Row(*rows[1])
    >>> addr_to_string(row.dst_prefix)
    '8.8.1.0'
    >>> sorted(row.probes_per_ttl)
    [(2, 6), (3, 6), (4, 6)]
    """
    with client(url) as c:
        rows = []
        sql = f"INSERT INTO {probes_table(measurement_id)} VALUES"
        for prefix, protocol, ttls, n_probes in prefixes:
            protocol = PROTOCOLS[protocol]  # type: ignore
            for af, subprefix, subprefix_size in split_prefix(
                prefix, prefix_len_v4, prefix_len_v6
            ):
                for ttl in ttls:
                    rows.append((protocol, subprefix, ttl, n_probes, round_))
                if len(rows) >= batch_size:
                    c.execute(sql, rows)
                    rows.clear()
        c.execute(sql, rows)


def insert_mda_probe_counts(
    url: str,
    measurement_id: str,
    previous_round: int,
    adaptive_eps: bool = False,
    target_epsilon: float = DEFAULT_FAILURE_RATE,
    subsets: Iterable[IPNetwork] = (UNIVERSE_SUBSET,),
) -> None:
    # TODO: set filter_partial and filter_virtual to false?
    InsertMDAProbes(
        adaptive_eps=adaptive_eps,
        round_leq=previous_round,
        filter_partial=True,
        filter_virtual=True,
        filter_inter_round=True,
        target_epsilon=target_epsilon,
    ).execute(url, measurement_id, subsets)


async def insert_mda_probe_counts_parallel(
    url: str,
    measurement_id: str,
    previous_round: int,
    adaptive_eps: bool = False,
    target_epsilon: float = DEFAULT_FAILURE_RATE,
    concurrent_requests: int = (os.cpu_count() or 2) // 2,
) -> None:
    # TODO: set filter_partial and filter_virtual to false?
    query = InsertMDAProbes(
        adaptive_eps=adaptive_eps,
        round_leq=previous_round,
        filter_partial=True,
        filter_virtual=True,
        filter_inter_round=True,
        target_epsilon=target_epsilon,
    )
    subsets = await subsets_for(query, url, measurement_id)
    await query.execute_concurrent(
        url, measurement_id, subsets, concurrent_requests=concurrent_requests
    )
