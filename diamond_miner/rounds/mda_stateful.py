from collections import defaultdict
from typing import Dict, Iterable, Iterator, Tuple

from diamond_miner.defaults import (
    DEFAULT_PROBE_DST_PORT,
    DEFAULT_PROBE_SRC_PORT,
    PROTOCOLS,
    UNIVERSE_SUBSET,
)
from diamond_miner.logging import logger
from diamond_miner.queries import GetNextRound, GetNextRoundStateful, GetProbes
from diamond_miner.typing import FlowMapper, IPNetwork, Probe


def mda_probes_stateful(
    url: str,
    measurement_id: str,
    round_: int,
    mapper_v4: FlowMapper,
    mapper_v6: FlowMapper,
    probe_src_port: int = DEFAULT_PROBE_SRC_PORT,
    probe_dst_port: int = DEFAULT_PROBE_DST_PORT,
    adaptive_eps: bool = False,
    target_epsilon: float = 0.05,
    subsets: Iterable[IPNetwork] = (UNIVERSE_SUBSET,),
) -> Iterator[Probe]:
    """
    Compute the probes to send given the previously discovered links.
    """
    # 1) Get the number of probes sent during the previous rounds.
    rows = GetProbes(round_leq=round_).execute_iter(url, measurement_id, subsets)
    already_sent = defaultdict(int)
    for row in rows:
        row = GetProbes.Row(*row)
        for ttl, n_probes in row.probes_per_ttl:
            already_sent[(row.protocol, int(row.dst_prefix), ttl)] = n_probes

    # 2) Compute the probes to send for the next round.
    # TODO: filter_partial is temporary.
    # TODO: filter_inter_round?
    query = GetNextRoundStateful(
        adaptive_eps=adaptive_eps,
        round_leq=round_,
        filter_partial=True,
        filter_virtual=True,
        filter_inter_round=True,
        target_epsilon=target_epsilon,
    )
    rows = query.execute_iter(url, measurement_id, subsets)
    for row in rows:
        for probe in row_to_probes(
            GetNextRoundStateful.Row(*row),
            mapper_v4,
            mapper_v6,
            probe_src_port,
            probe_dst_port,
            already_sent,
        ):
            # TEMP: Log prefixes that overflows the port number.
            if probe[1] > (2 ** 16 - 1):
                logger.warning("Port overflow for %s", GetNextRound.Row(*row))
                break
            yield probe


def row_to_probes(
    row: GetNextRoundStateful.Row,
    mapper_v4: FlowMapper,
    mapper_v6: FlowMapper,
    probe_src_port: int,
    probe_dst_port: int,
    already_sent: Dict[Tuple[int, int, int], int],
) -> Iterator[Probe]:
    dst_prefix_int = int(row.dst_prefix)
    mapper = mapper_v4 if row.dst_prefix.ipv4_mapped else mapper_v6
    protocol_str = PROTOCOLS[row.protocol]

    for ttl, total_probes in zip(row.ttls, row.total_probes):
        already_sent_ = already_sent[(row.protocol, dst_prefix_int, ttl)]
        for flow_id in range(already_sent_, total_probes):
            addr_offset, port_offset = mapper.offset(
                flow_id=flow_id, prefix=dst_prefix_int
            )
            yield (
                dst_prefix_int + addr_offset,
                probe_src_port + port_offset,
                probe_dst_port,
                ttl,
                protocol_str,
            )
