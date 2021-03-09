from ipaddress import ip_network
from typing import Iterable, Optional

from diamond_miner import SequentialFlowMapper
from diamond_miner.utils import permutation, subnets


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
