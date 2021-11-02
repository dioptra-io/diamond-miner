from typing import Iterable, Iterator

from diamond_miner.defaults import (
    DEFAULT_PROBE_DST_PORT,
    DEFAULT_PROBE_SRC_PORT,
    PROTOCOLS,
    UNIVERSE_SUBSET,
)
from diamond_miner.logging import logger
from diamond_miner.queries import GetProbesDiff, InsertMDAProbes
from diamond_miner.queries.delete_probes import DeleteProbes
from diamond_miner.typing import FlowMapper, IPNetwork, Probe


def mda_probes(
    url: str,
    measurement_id: str,
    previous_round: int,
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
    next_round = previous_round + 1

    # 1) Delete eventual pre-existing probes.
    DeleteProbes(round_eq=next_round).execute(url, measurement_id, subsets)

    # 2) Compute and save the new probes.
    InsertMDAProbes(
        adaptive_eps=adaptive_eps,
        round_leq=previous_round,
        filter_partial=True,
        filter_virtual=True,
        filter_inter_round=True,
        target_epsilon=target_epsilon,
    ).execute(url, measurement_id, subsets)

    # 3) Generate the probe tuples for the next round.
    rows = GetProbesDiff(round_eq=next_round).execute_iter(url, measurement_id, subsets)
    for row in rows:
        row = GetProbesDiff.Row(*row)
        for probe in row_to_probes(
            row,
            mapper_v4,
            mapper_v6,
            probe_src_port,
            probe_dst_port,
        ):
            # TEMP: Log prefixes that overflows the port number.
            if probe[1] > (2 ** 16 - 1):
                logger.warning("Port overflow for %s", row)
                break
            yield probe


def row_to_probes(
    row: GetProbesDiff.Row,
    mapper_v4: FlowMapper,
    mapper_v6: FlowMapper,
    probe_src_port: int,
    probe_dst_port: int,
) -> Iterator[Probe]:
    dst_prefix_int = int(row.dst_prefix)
    mapper = mapper_v4 if row.dst_prefix.ipv4_mapped else mapper_v6
    protocol_str = PROTOCOLS[row.protocol]

    for ttl, total_probes, already_sent in row.probes_per_ttl:
        for flow_id in range(already_sent, total_probes):
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
