from collections.abc import Iterable, Iterator
from dataclasses import dataclass

from pych_client import ClickHouseClient

from diamond_miner.defaults import (
    DEFAULT_FAILURE_RATE,
    DEFAULT_PREFIX_LEN_V4,
    DEFAULT_PREFIX_LEN_V6,
    PROTOCOLS,
    UNIVERSE_SUBSET,
)
from diamond_miner.format import format_ipv6
from diamond_miner.generators.standalone import split_prefix
from diamond_miner.queries.insert_mda_probes import InsertMDAProbes
from diamond_miner.queries.query import Query, probes_table
from diamond_miner.subsets import subsets_for
from diamond_miner.typing import IPNetwork
from diamond_miner.utilities import available_cpus


@dataclass(frozen=True)
class InsertProbes(Query):
    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        return f"INSERT INTO {probes_table(measurement_id)} FORMAT JSONCompactEachRow"


def insert_probe_counts(
    client: ClickHouseClient,
    measurement_id: str,
    round_: int,
    prefixes: Iterable[tuple[str, str, Iterable[int], int]],
    prefix_len_v4: int = DEFAULT_PREFIX_LEN_V4,
    prefix_len_v6: int = DEFAULT_PREFIX_LEN_V6,
) -> None:
    """
    Insert the probe counts specified by `prefixes` into the probes table.

    Args:
        client: ClickHouse client.
        measurement_id: Measurement id.
        round_: Round number for which to insert the probe counts.
        prefixes: A list of `(prefix, protocol, ttls, n_probes)` tuples. /32 or /128 is assumed if not specified.
        prefix_len_v4: The prefix length to which the IPv4 prefixes will be split to.
        prefix_len_v6: The prefix length to which the IPv6 prefixes will be split to.

    Examples:
        >>> from diamond_miner.test import client, create_tables
        >>> from diamond_miner.queries import GetProbes
        >>> create_tables(client, "test_probe_counts")
        >>> insert_probe_counts(client, "test_probe_counts", 1, [("8.8.0.0/22", "icmp", range(2, 5), 6)])
        >>> rows = sorted(GetProbes(round_eq=1).execute(client, "test_probe_counts"), key=lambda x: x["probe_dst_prefix"])
        >>> len(rows)
        4
        >>> row = rows[0]
        >>> row["probe_dst_prefix"]
        '::ffff:8.8.0.0'
        >>> sorted(row["probes_per_ttl"])
        [[2, 6], [3, 6], [4, 6]]
        >>> row = rows[1]
        >>> row["probe_dst_prefix"]
        '::ffff:8.8.1.0'
        >>> sorted(row["probes_per_ttl"])
        [[2, 6], [3, 6], [4, 6]]
    """

    def gen() -> Iterator[bytes]:
        for prefix, protocol, ttls, n_probes in prefixes:
            protocol = PROTOCOLS[protocol]  # type: ignore
            for af, subprefix, subprefix_size in split_prefix(
                prefix, prefix_len_v4, prefix_len_v6
            ):
                yield "\n".join(
                    f'[{protocol},"{format_ipv6(subprefix)}",{ttl},{n_probes},{round_}]'
                    for ttl in ttls
                ).encode()

    InsertProbes().execute(client, measurement_id, data=gen())


def insert_mda_probe_counts(
    client: ClickHouseClient,
    measurement_id: str,
    previous_round: int,
    adaptive_eps: bool = False,
    target_epsilon: float = DEFAULT_FAILURE_RATE,
    concurrent_requests: int = max(available_cpus() // 8, 1),
) -> None:
    """
    Run the Diamond-Miner algorithm and insert the resulting probes into the probes table.

    Args:
        client: ClickHouse client.
        measurement_id: Measurement id.
        previous_round: Round on which to run the Diamond-Miner algorithm.
        adaptive_eps: Set to `True` to handle nested load-balancers.
        target_epsilon: Target failure rate of the MDA algorithm.
        concurrent_requests: Maximum number of requests to execute concurrently.
    """
    # TODO: set filter_partial and filter_virtual to false?
    query = InsertMDAProbes(
        adaptive_eps=adaptive_eps,
        round_leq=previous_round,
        filter_partial=True,
        filter_virtual=True,
        filter_inter_round=True,
        target_epsilon=target_epsilon,
    )
    subsets = subsets_for(query, client, measurement_id)
    query.execute_concurrent(
        client, measurement_id, subsets=subsets, concurrent_requests=concurrent_requests
    )
