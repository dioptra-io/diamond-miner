from ipaddress import IPv4Network, ip_network
from typing import AsyncIterator, Iterable, List, Optional, Tuple

from diamond_miner.defaults import (
    DEFAULT_PREFIX_LEN_V4,
    DEFAULT_PREFIX_LEN_V6,
    DEFAULT_PROBE_DST_PORT,
    DEFAULT_PROBE_SRC_PORT,
)
from diamond_miner.mappers import SequentialFlowMapper
from diamond_miner.utilities import ParameterGrid, subnets

ProbeType = Tuple[int, int, int, int]


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


async def probe_generator(
    prefixes: Iterable[str],  # /32 or / 128 if nothing specified
    flow_ids: Iterable[int] = range(6),
    ttls: Iterable[int] = range(1, 33),
    prefix_len_v4: int = DEFAULT_PREFIX_LEN_V4,
    prefix_len_v6: int = DEFAULT_PREFIX_LEN_V6,
    probe_src_port: int = DEFAULT_PROBE_SRC_PORT,
    probe_dst_port: int = DEFAULT_PROBE_DST_PORT,
    mapper=SequentialFlowMapper(),
    seed: Optional[int] = None,
) -> AsyncIterator[ProbeType]:
    """
    # Returns:
    #     destination address (little endian), source port, destination port, TTL.
    """
    prefixes_: List[Tuple[int, int]] = []
    for prefix in prefixes:
        network = ip_network(prefix.strip())
        if isinstance(network, IPv4Network):
            # We add 0xFFFF00000000 to convert the network address
            # to an IPv4-mapped IPv6 address.
            prefixes_.extend(
                (2 ** (32 - prefix_len_v4), x + 0xFFFF00000000)
                for x in subnets(network, prefix_len_v4)
            )
        else:
            prefixes_.extend(
                (2 ** (128 - prefix_len_v6), x) for x in subnets(network, prefix_len_v6)
            )

    grid = ParameterGrid(prefixes_, ttls, flow_ids)
    grid = grid.shuffled(seed=seed)

    for (prefix_size, prefix), ttl, flow_id in grid:
        addr_offset, port_offset = mapper.offset(flow_id=flow_id, prefix=prefix)
        yield prefix + addr_offset, probe_src_port + port_offset, probe_dst_port, ttl
