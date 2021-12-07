from ipaddress import IPv6Address
from typing import Iterable, Iterator, Optional

from diamond_miner.defaults import (
    DEFAULT_PREFIX_SIZE_V4,
    DEFAULT_PREFIX_SIZE_V6,
    DEFAULT_PROBE_DST_PORT,
    DEFAULT_PROBE_SRC_PORT,
    PROTOCOLS,
    UNIVERSE_SUBSET,
)
from diamond_miner.logger import logger
from diamond_miner.mappers import SequentialFlowMapper
from diamond_miner.queries import GetProbesDiff
from diamond_miner.typing import FlowMapper, IPNetwork, Probe


def probe_generator_from_database(
    url: str,
    measurement_id: str,
    round_: int,
    *,
    mapper_v4: FlowMapper = SequentialFlowMapper(DEFAULT_PREFIX_SIZE_V4),
    mapper_v6: FlowMapper = SequentialFlowMapper(DEFAULT_PREFIX_SIZE_V6),
    probe_src_port: int = DEFAULT_PROBE_SRC_PORT,
    probe_dst_port: int = DEFAULT_PROBE_DST_PORT,
    probe_ttl_geq: Optional[int] = None,
    probe_ttl_leq: Optional[int] = None,
    subsets: Iterable[IPNetwork] = (UNIVERSE_SUBSET,),
) -> Iterator[Probe]:
    """
    TODO: Doctest, note that this doesn't randomize probes.
    >>> from ipaddress import ip_address
    >>> from diamond_miner.insert import insert_probe_counts
    >>> from diamond_miner.test import create_tables, url
    >>> create_tables(url, "test_probe_gen")
    >>> insert_probe_counts(url, "test_probe_gen", 1, [("8.8.0.0/23", "icmp", [1, 2], 2)])
    >>> probes = list(probe_generator_from_database(url, "test_probe_gen", 1))
    >>> len(probes)
    8
    >>> (str(ip_address(probes[0][0])), *probes[0][1:])
    ('::ffff:808:100', 24000, 33434, 1, 'icmp')
    """
    rows = GetProbesDiff(
        round_eq=round_, probe_ttl_geq=probe_ttl_geq, probe_ttl_leq=probe_ttl_leq
    ).execute_iter(url, measurement_id, subsets=subsets)
    for row in rows:
        dst_prefix_int = int(IPv6Address(row["probe_dst_prefix"]))
        mapper = (
            mapper_v4 if row["probe_dst_prefix"].startswith("::ffff:") else mapper_v6
        )
        protocol_str = PROTOCOLS[row["probe_protocol"]]

        for ttl, total_probes, already_sent in row["probes_per_ttl"]:
            for flow_id in range(already_sent, total_probes):
                addr_offset, port_offset = mapper.offset(flow_id, dst_prefix_int)
                dst_addr = dst_prefix_int + addr_offset
                src_port = probe_src_port + port_offset
                if src_port > (2 ** 16 - 1):
                    # TEMP: Log prefixes that overflows the port number and skip prefix.
                    logger.warning("Port overflow for %s", row)
                    break
                yield dst_addr, src_port, probe_dst_port, ttl, protocol_str  # type: ignore
