from ipaddress import IPv4Network, ip_network
from typing import Iterable, Optional

from diamond_miner.mappers import SequentialFlowMapper
from diamond_miner.utils import permutation, subnets


def count_prefixes(prefixes: Iterable[str], prefix_len: int = 24) -> int:
    """
    >>> count_prefixes(["8.8.4.0/24", "8.8.8.0/24"])
    2
    >>> count_prefixes(["0.0.0.0/22"])
    4
    >>> count_prefixes(["0.0.0.0/0"])
    16777216
    """
    count = 0
    for prefix in prefixes:
        network = ip_network(prefix)
        assert isinstance(network, IPv4Network)
        if network.prefixlen > prefix_len:
            raise ValueError(f"prefix length must be <= {prefix_len}")
        count += 2 ** (prefix_len - network.prefixlen)
    return count


async def probe_generator(
    prefixes: Iterable[str],  # /32 or / 128 if nothing specified
    prefix_len: int = 24,
    min_flow: int = 0,
    max_flow: int = 5,  # inclusive
    min_ttl: int = 1,
    max_ttl: int = 32,  # inclusive
    src_port: int = 24000,
    dst_port: int = 33434,
    mapper=SequentialFlowMapper(),
    seed: Optional[int] = None,
):
    """
    # Returns:
    #     destination address (little endian), source port, destination port, TTL.
    """
    prefixes_ = []
    for prefix in prefixes:
        network = ip_network(prefix)
        prefixes_.extend(subnets(network, prefix_len))

    ranges = [(0, len(prefixes_)), (min_ttl, max_ttl + 1), (min_flow, max_flow + 1)]
    it = permutation(ranges, seed=seed)

    for prefix_idx, ttl, flow_id in it:
        prefix = prefixes_[prefix_idx]
        addr_offset, port_offset = mapper.offset(
            flow_id=flow_id, prefix=prefix, prefix_len=prefix_len
        )
        yield prefix + addr_offset, src_port + port_offset, dst_port, ttl


def probe_to_csv(dst_addr, src_port, dst_port, ttl):
    return f"{dst_addr},{src_port},{dst_port},{ttl}"
