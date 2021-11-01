from typing import Iterable, Iterator

from diamond_miner.defaults import (
    DEFAULT_PROBE_DST_PORT,
    DEFAULT_PROBE_SRC_PORT,
    PROTOCOLS,
    UNIVERSE_SUBSET,
)
from diamond_miner.logging import logger
from diamond_miner.queries import GetNextRound
from diamond_miner.queries.get_next_round import InsertMDAProbes
from diamond_miner.queries.get_probes import GetProbesDiff
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
    # TODO: Separate in a separate function?
    # (compute_mda_probes?)
    InsertMDAProbes(
        adaptive_eps=adaptive_eps,
        round_leq=round_,
        filter_partial=True,
        filter_virtual=True,
        filter_inter_round=True,
        target_epsilon=target_epsilon,
    ).execute(url, measurement_id, subsets)
    print(round_)
    # TODO: Cleanup import/export in __init__.py
    query = GetProbesDiff(round_eq=round_ + 1)
    rows = query.execute_iter(url, measurement_id, subsets)
    for row in rows:
        for probe in row_to_probes(
            GetProbesDiff.Row(*row),
            mapper_v4,
            mapper_v6,
            probe_src_port,
            probe_dst_port,
        ):
            # TEMP: Log prefixes that overflows the port number.
            if probe[1] > (2 ** 16 - 1):
                logger.warning("Port overflow for %s", GetNextRound.Row(*row))
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
        print(ttl, total_probes, already_sent)
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
