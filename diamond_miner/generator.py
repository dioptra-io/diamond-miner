from ipaddress import IPv4Network, ip_network
from typing import Iterable, Iterator, List, Optional, Tuple

from diamond_miner.defaults import (
    DEFAULT_PREFIX_LEN_V4,
    DEFAULT_PREFIX_LEN_V6,
    DEFAULT_PREFIX_SIZE_V4,
    DEFAULT_PREFIX_SIZE_V6,
    DEFAULT_PROBE_DST_PORT,
    DEFAULT_PROBE_SRC_PORT,
)
from diamond_miner.grid import ParameterGrid
from diamond_miner.mappers import SequentialFlowMapper
from diamond_miner.utilities import subnets

ProbeType = Tuple[int, int, int, int, str]


def count_prefixes(
    prefixes: Iterable[str],
    prefix_len_v4: int = DEFAULT_PREFIX_LEN_V4,
    prefix_len_v6: int = DEFAULT_PREFIX_LEN_V6,
) -> int:
    """
    >>> count_prefixes(["8.8.4.0/24", "8.8.8.0/24"])
    2
    >>> count_prefixes(["0.0.0.0/22"])
    4
    >>> count_prefixes(["0.0.0.0/0"])
    16777216
    >>> count_prefixes(["2001::/48"])
    65536
    >>> count_prefixes(["0.0.0.0/32"], prefix_len_v4=24)
    Traceback (most recent call last):
        ...
    ValueError: prefix length must be <= 24
    """
    count = 0
    for prefix in prefixes:
        network = ip_network(prefix)
        if isinstance(network, IPv4Network):
            prefix_len = prefix_len_v4
        else:
            prefix_len = prefix_len_v6
        if network.prefixlen > prefix_len:
            raise ValueError(f"prefix length must be <= {prefix_len}")
        count += 2 ** (prefix_len - network.prefixlen)
    return count


def split_prefix(
    prefix: str, prefix_len_v4: int, prefix_len_v6: int
) -> Iterator[Tuple[int, int, int]]:
    network = ip_network(prefix.strip())
    if isinstance(network, IPv4Network):
        # We add 0xFFFF00000000 to convert the network address
        # to an IPv4-mapped IPv6 address.
        prefix_size = 2 ** (32 - prefix_len_v4)
        for x in subnets(network, prefix_len_v4):
            yield 4, x + 0xFFFF00000000, prefix_size
    else:
        prefix_size = 2 ** (128 - prefix_len_v6)
        for x in subnets(network, prefix_len_v6):
            yield 6, x, prefix_size


def probe_generator(
    prefixes: Iterable[Tuple[str, str]],  # /32 or / 128 if nothing specified
    flow_ids: Iterable[int] = range(6),
    ttls: Iterable[int] = range(1, 33),
    prefix_len_v4: int = DEFAULT_PREFIX_LEN_V4,
    prefix_len_v6: int = DEFAULT_PREFIX_LEN_V6,
    probe_src_port: int = DEFAULT_PROBE_SRC_PORT,
    probe_dst_port: int = DEFAULT_PROBE_DST_PORT,
    mapper_v4=SequentialFlowMapper(DEFAULT_PREFIX_SIZE_V4),
    mapper_v6=SequentialFlowMapper(DEFAULT_PREFIX_SIZE_V6),
    seed: Optional[int] = None,
) -> Iterator[ProbeType]:
    """
    Generate a probe for each prefix, flow_id and TTL, in a random order.
    """
    prefixes_: List[Tuple[int, int, int, str]] = []
    for prefix, protocol in prefixes:
        for af, subprefix, subprefix_size in split_prefix(
            prefix, prefix_len_v4, prefix_len_v6
        ):
            prefixes_.append((af, subprefix, subprefix_size, protocol))

    grid = ParameterGrid(prefixes_, ttls, flow_ids)
    grid = grid.shuffled(seed=seed)

    for (af, prefix, prefix_size, protocol), ttl, flow_id in grid:
        mapper = mapper_v4 if af == 4 else mapper_v6
        addr_offset, port_offset = mapper.offset(flow_id=flow_id, prefix=prefix)
        yield prefix + addr_offset, probe_src_port + port_offset, probe_dst_port, ttl, protocol


def probe_generator_by_flow(
    prefixes: Iterable[
        Tuple[str, str, Iterable[int]]
    ],  # /32 or / 128 if nothing specified
    flow_ids: Iterable[int] = range(6),
    prefix_len_v4: int = DEFAULT_PREFIX_LEN_V4,
    prefix_len_v6: int = DEFAULT_PREFIX_LEN_V6,
    probe_src_port: int = DEFAULT_PROBE_SRC_PORT,
    probe_dst_port: int = DEFAULT_PROBE_DST_PORT,
    mapper_v4=SequentialFlowMapper(DEFAULT_PREFIX_SIZE_V4),
    mapper_v6=SequentialFlowMapper(DEFAULT_PREFIX_SIZE_V6),
    seed: Optional[int] = None,
) -> Iterator[ProbeType]:
    """
    Generate a probe for each prefix, flow id and TTL, in a random order.
    This function differs from `probe_generator` in two aspects:
    - The TTLs are specified for each prefixes, and not globally.
    - All the probes for a given prefix and flow id are generated sequentially.
    """
    prefixes_: List[Tuple[int, int, int, str, Iterable[int]]] = []
    for prefix, protocol, ttls in prefixes:
        for af, subprefix, subprefix_size in split_prefix(
            prefix, prefix_len_v4, prefix_len_v6
        ):
            prefixes_.append((af, subprefix, subprefix_size, protocol, ttls))

    grid = ParameterGrid(prefixes_, flow_ids)
    grid = grid.shuffled(seed=seed)

    for (af, prefix, prefix_size, protocol, ttls), flow_id in grid:
        mapper = mapper_v4 if af == 4 else mapper_v6
        for ttl in ttls:
            addr_offset, port_offset = mapper.offset(flow_id=flow_id, prefix=prefix)
            yield prefix + addr_offset, probe_src_port + port_offset, probe_dst_port, ttl, protocol
