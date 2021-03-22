from ipaddress import IPv4Network, ip_network
from typing import AsyncIterator, Iterable, List, Optional, Tuple

from diamond_miner.mappers import SequentialFlowMapper
from diamond_miner.utilities import ParameterGrid, subnets

ProbeType = Tuple[int, int, int, int]


def count_prefixes(
    prefixes: Iterable[str], prefix_len_v4: int = 24, prefix_len_v6: int = 64
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
    prefix_len_v4: int = 24,
    prefix_len_v6: int = 64,
    flow_ids: List[int] = range(6),
    ttls: List[int] = range(1, 33),
    src_port: int = 24000,
    dst_port: int = 33434,
    mapper=SequentialFlowMapper(),
    seed: Optional[int] = None,
) -> AsyncIterator[ProbeType]:
    """
    # Returns:
    #     destination address (little endian), source port, destination port, TTL.
    """
    prefixes_ = []
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
        addr_offset, port_offset = mapper.offset(
            flow_id=flow_id, prefix=prefix, prefix_size=prefix_size
        )
        yield prefix + addr_offset, src_port + port_offset, dst_port, ttl
