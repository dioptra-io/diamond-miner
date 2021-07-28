from typing import Iterable, Iterator

from diamond_miner.defaults import (
    DEFAULT_PROBE_DST_PORT,
    DEFAULT_PROBE_SRC_PORT,
    PROTOCOLS,
    UNIVERSE_SUBSET,
)
from diamond_miner.logging import logger
from diamond_miner.queries import GetNextRound
from diamond_miner.typing import FlowMapper, IPNetwork, Probe


def mda_probes(
    url: str,
    measurement_id: str,
    round_: int,
    mapper_v4: FlowMapper,
    mapper_v6: FlowMapper,
    probe_src_port: int = DEFAULT_PROBE_SRC_PORT,
    probe_dst_port: int = DEFAULT_PROBE_DST_PORT,
    adaptive_eps: bool = False,
    subsets: Iterable[IPNetwork] = (UNIVERSE_SUBSET,),
) -> Iterator[Probe]:
    """
    Compute the probes to send given the previously discovered links.
    """
    # TODO: filter_partial is temporary.
    # TODO: filter_inter_round?
    query = GetNextRound(
        adaptive_eps=adaptive_eps,
        round_leq=round_,
        filter_partial=True,
        filter_virtual=True,
        filter_inter_round=True,
    )
    rows = query.execute_iter(url, measurement_id, subsets)
    for row in rows:
        for probe in row_to_probes(
            GetNextRound.Row(*row), mapper_v4, mapper_v6, probe_src_port, probe_dst_port
        ):
            # TEMP: Log prefixes that overflows the port number.
            if probe[1] > (2 ** 16 - 1):
                logger.warning("Port overflow for %s", GetNextRound.Row(*row))
                break
            yield probe


def row_to_probes(
    row: GetNextRound.Row,
    mapper_v4: FlowMapper,
    mapper_v6: FlowMapper,
    probe_src_port: int,
    probe_dst_port: int,
) -> Iterator[Probe]:
    dst_prefix_int = int(row.dst_prefix)
    mapper = mapper_v4 if row.dst_prefix.ipv4_mapped else mapper_v6
    protocol_str = PROTOCOLS[row.protocol]

    for ttl, to_send, already_sent in zip(row.ttls, row.to_send, row.already_sent):
        for flow_id in range(already_sent, already_sent + to_send):
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
