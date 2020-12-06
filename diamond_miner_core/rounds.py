from ipaddress import ip_address
from math import ceil, log2
from secrets import token_bytes

from cperm import Permutation

# TODO: Move "flush_traceroute" ("mda_round"?) here?

def swap32(x):
    return (((x << 24) & 0xFF000000) |
            ((x <<  8) & 0x00FF0000) |
            ((x >>  8) & 0x0000FF00) |
            ((x >> 24) & 0x000000FF))


async def exhaustive_round(mapper, n_flows=8, src_port=24000, dst_port=33434, seed=None):
    """
    Generate 2**32 probes: `n_flows` probes per TTLs in (1, 32) per /24 subnets.

    Args:
        mapper: flow mapper used to compute the destination address and the
        source port from the flow id.
        src_port: minimum source port.
        dst_port: minimum destination port.
        seed: 8 bytes string.

    Returns:
        destination address (little endian), source port, destination port, TTL.
    """
    #  seed = seed or token_bytes(8)
    #  perm = Permutation(2 ** 32 - 1, "cycle", "speck", seed)
    seed = seed or token_bytes(16)
    perm = Permutation(2 ** 32 - 1, "cycle", "rc5", seed)
    for val in perm:
        # 1. Unpack bits
        # Prefix: 24 bits (0, 2**24-1)
        # TTL: 5 bits (0, 31)
        # Flow ID: 3 bits (0, 7)
        prefix = val & 0x00FFFFFF
        ttl = (val >> 24) & 0x0000001F
        flow_id = val >> 29
        # Kevin's comment from C++ source code:
        # > Avoid CEF drops on prefixes, addresses are big endian,
        # > so convert it to little endian.
        prefix = swap32(prefix)
        # 2. Generate probe
        addr_offset, port_offset = mapper.offset(
            flow_id=flow_id, prefix=prefix, prefix_size=24
        )
        if flow_id < n_flows:
            yield (prefix + addr_offset, src_port + port_offset, dst_port, ttl)


async def targets_round(targets, src_port=24000, dst_port=33434, seed=None):
    """
    Generate 1 probe per TTLs in (1, 32) per targets.
    See `exhaustive_round` for the arguments and the return values.
    """
    #  seed = seed or token_bytes(8)
    seed = seed or token_bytes(16)
    target_bits = ceil(log2(len(targets)))
    range_ = 2 ** (target_bits + 5)
    assert range_ < 2 ** 32, "targets list is too long"
    mode = "prefix" if range_ < 10 ** 6 else "cycle"
    #  perm = Permutation(range_, mode, "speck", seed)
    perm = Permutation(range_, mode, "rc5", seed)
    for val in perm:
        # 1. Unpack bits
        # Target: X bits (0, len(targets) - 1)
        # TTL: 5 bits (0, 31)
        target_i = val & ((1 << target_bits) - 1)
        ttl = val >> target_bits
        if target_i >= len(targets):
            continue
        # 2. Generate probe
        target = targets[target_i]
        if isinstance(target, str):
            target = int(ip_address(target.strip()))
        yield (target, src_port, dst_port, ttl)


def probe_to_csv(dst_addr, src_port, dst_port, ttl, human=False):
    """
    Format the probe in a padded CSV representation.

    Args:
        dst_addr: destination address (little endian)
        src_port: source port
        dst_port: destination port
        ttl: TTL
        human: if true, output the address in numbers-and-dots notation.

    Returns:
        The probe in a padded CSV (constant row size) representation.
    """
    if human:
        dst_addr_ = "{:03}.{:03}.{:03}.{:03}".format(
            (dst_addr & 0xFF000000) >> 24,
            (dst_addr & 0x00FF0000) >> 16,
            (dst_addr & 0x0000FF00) >> 8,
            (dst_addr & 0x000000FF),
        )
    else:
        dst_addr_ = f"{dst_addr:010}"
    return f"{dst_addr_},{src_port:05},{dst_port:05},{ttl:03}"
