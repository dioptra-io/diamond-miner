from ipaddress import IPv4Network, IPv6Network, ip_network
from typing import Iterable, Iterator, List, Optional, Sequence, Tuple, Union

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
from diamond_miner.typing import FlowMapper, Probe


def count_prefixes(
    prefixes: Iterable[str],
    prefix_len_v4: int = DEFAULT_PREFIX_LEN_V4,
    prefix_len_v6: int = DEFAULT_PREFIX_LEN_V6,
) -> int:
    """
    Count the number of prefixes yielded by a list of prefixes.

    :param prefixes: A list of IPv4/v6 prefixes.
    :param prefix_len_v4: The target prefix length for v4 prefixes.
    :param prefix_len_v6: The target prefix length for v6 prefixes.
    :return: The number of prefixes yielded by the prefix list.

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


def subnets(network: Union[IPv4Network, IPv6Network], new_prefix: int) -> Sequence[int]:
    """
    Faster version of :py:meth:`ipaddress.IPv4Network.subnets`.
    Returns only the network address as an integer.

    >>> from ipaddress import ip_network
    >>> list(subnets(ip_network("0.0.0.0/0"), new_prefix=2))
    [0, 1073741824, 2147483648, 3221225472]
    >>> subnets(ip_network("0.0.0.0/32"), new_prefix=24)
    Traceback (most recent call last):
        ...
    ValueError: new prefix must be longer
    """
    if new_prefix < network.prefixlen:
        raise ValueError("new prefix must be longer")
    start = int(network.network_address)
    end = int(network.broadcast_address) + 1
    step = (int(network.hostmask) + 1) >> (new_prefix - network.prefixlen)
    return range(start, end, step)


def probe_generator(
    prefixes: Sequence[Tuple[str, str]],  # /32 or / 128 if nothing specified
    flow_ids: Sequence[int] = range(6),
    ttls: Sequence[int] = range(1, 33),
    prefix_len_v4: int = DEFAULT_PREFIX_LEN_V4,
    prefix_len_v6: int = DEFAULT_PREFIX_LEN_V6,
    probe_src_port: int = DEFAULT_PROBE_SRC_PORT,
    probe_dst_port: int = DEFAULT_PROBE_DST_PORT,
    mapper_v4: FlowMapper = SequentialFlowMapper(DEFAULT_PREFIX_SIZE_V4),
    mapper_v6: FlowMapper = SequentialFlowMapper(DEFAULT_PREFIX_SIZE_V6),
    seed: Optional[int] = None,
) -> Iterator[Probe]:
    """
    Generate a probe for each prefix, flow ID and TTL, in a random order.

    :param prefixes: A list of (prefix, protocol) tuples. The protocol can be ``icmp``, ``icmp6`` or ``udp``.
    :param flow_ids: The flow IDs to probe.
    :param ttls: The TTLs to probe.
    :param prefix_len_v4: The prefix length to which the IPv4 prefixes will be split to.
    :param prefix_len_v6: The prefix length to which the IPv6 prefixes will be split to.
    :param probe_src_port: The minimum source port of the probes (can be incremented by the flow mapper).
    :param probe_dst_port: The destination port of the probes (constant).
    :param mapper_v4: The flow mapper for IPv4 probes.
    :param mapper_v6: The flow mapper for IPv6 probes.
    :param seed: The seed of the random permutation (two calls with the same seed will yield the probes in the same order).

    This function is very versatile, it can generate Tokyo-Ping :cite:`pelsser2013paris`,
    Paris-Traceroute :cite:`augustin2006avoiding` or Yarrp-like :cite:`beverly2016yarrp` probes.

    For ICMP probes, the source port is encoded by caracal in the checksum field of the ICMP header
    which is generally used by routers for per-flow load-balancing.

    .. code-block:: python

        # Example: ICMP ping towards Google DNS servers with 2 flows per address and a TTL of 32.
        prefixes = [("8.8.8.8/32", "icmp"), ("2001:4860:4860::8888/128", "icmp6")]
        prefix_len_v4 = 32
        prefix_len_v6 = 128
        flow_ids = range(2)
        ttls = [32]

        # When given a prefix size of 1, the sequential flow mapper will only vary the port.
        mapper_v4 = SequentialFlowMapper(prefix_size=1)
        mapper_v6 = SequentialFlowMapper(prefix_size=1)

        # Example: ICMP ping towards 1.0.0.0/24 with 1 flow per address and a TTL of 32.
        prefixes = [("1.0.0.0/24", "icmp")]
        prefix_len_v4 = 32 # The generator will cut the /24 in 256 /32.
        flow_ids = range(1)
        ttls = [32]
        # Same flow mappers as above.

        # Example: UDP traceroute towards 1.0.0.0/24 with 2 flows per address.
        # 256 addresses * 2 flows * 30 TTLs = 15,360 probes.
        prefixes = [("1.0.0.0/24", "udp")]
        prefix_len_v4 = 32 # The generator will cut the /24 in 256 /32.
        flow_ids = range(2)
        ttls = range(2, 32)
        # Same flow mappers as above.

        # Example: UDP traceroute towards 1.0.0.0/24 with 6 flows **per prefix**.
        # 1 prefix * 6 flows * 30 TTLs = 180 probes.
        prefixes = [("1.0.0.0/24", "udp")]
        prefix_len_v4 = 24 # We want to target the prefix, not its individual addresses.
        flow_ids = range(6)
        ttls = range(2, 32)

        # The random flow mapper will assign a random destination (in the /24) to each flow.
        mapper_v4 = RandomFlowMapper(prefix_size=256)
    """
    prefixes_: List[Tuple[int, int, int, str]] = []
    for prefix, protocol in prefixes:
        for af, subprefix, subprefix_size in split_prefix(
            prefix, prefix_len_v4, prefix_len_v6
        ):
            prefixes_.append((af, subprefix, subprefix_size, protocol))

    grid = ParameterGrid(prefixes_, ttls, flow_ids).shuffled(seed=seed)

    for (af, subprefix, subprefix_size, protocol), ttl, flow_id in grid:
        mapper = mapper_v4 if af == 4 else mapper_v6
        addr_offset, port_offset = mapper.offset(flow_id=flow_id, prefix=subprefix)
        yield subprefix + addr_offset, probe_src_port + port_offset, probe_dst_port, ttl, protocol


def probe_generator_by_flow(
    prefixes: Iterable[
        Tuple[str, str, Iterable[int]]
    ],  # /32 or / 128 if nothing specified
    flow_ids: Sequence[int] = range(6),
    prefix_len_v4: int = DEFAULT_PREFIX_LEN_V4,
    prefix_len_v6: int = DEFAULT_PREFIX_LEN_V6,
    probe_src_port: int = DEFAULT_PROBE_SRC_PORT,
    probe_dst_port: int = DEFAULT_PROBE_DST_PORT,
    mapper_v4: FlowMapper = SequentialFlowMapper(DEFAULT_PREFIX_SIZE_V4),
    mapper_v6: FlowMapper = SequentialFlowMapper(DEFAULT_PREFIX_SIZE_V6),
    seed: Optional[int] = None,
) -> Iterator[Probe]:
    """
    Generate a probe for each prefix, flow id and TTL, in a random order.
    This function differs from :func:`probe_generator` in two aspects:

        * The TTLs are specified for each prefixes, and not globally.
        * All the probes for a given prefix and flow id are generated sequentially.

    The parameters and output are identical to :func:`probe_generator`,
    excepted for ``prefixes`` which is a list of (prefix, protocol, TTLs) tuples,
    and the absence of the ``ttls`` parameter.
    """
    prefixes_: List[Tuple[int, int, int, str, Iterable[int]]] = []
    for prefix, protocol, ttls in prefixes:
        for af, subprefix, subprefix_size in split_prefix(
            prefix, prefix_len_v4, prefix_len_v6
        ):
            prefixes_.append((af, subprefix, subprefix_size, protocol, ttls))

    grid = ParameterGrid(prefixes_, flow_ids).shuffled(seed=seed)

    for (af, subprefix, subprefix_size, protocol, ttls), flow_id in grid:
        mapper = mapper_v4 if af == 4 else mapper_v6
        for ttl in ttls:
            addr_offset, port_offset = mapper.offset(flow_id=flow_id, prefix=subprefix)
            yield subprefix + addr_offset, probe_src_port + port_offset, probe_dst_port, ttl, protocol
