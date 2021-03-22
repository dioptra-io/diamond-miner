from ipaddress import ip_network

from aioch import Client

from diamond_miner.mappers import FlowMapper
from diamond_miner.queries.count_nodes_per_ttl import CountNodesPerTTL
from diamond_miner.queries.count_replies import CountReplies
from diamond_miner.queries.get_max_ttl import GetMaxTTL
from diamond_miner.queries.get_next_round import GetNextRound


def get_subsets(counts, max_replies_per_subset):
    """
    >>> get_subsets({} ,10)
    []
    >>> counts = {ip_network("::ffff:8.8.4.0/120"): 10, ip_network("::ffff:8.8.8.0/120"): 5}
    >>> get_subsets(counts, 15)
    [IPv6Network('::/0')]
    >>> get_subsets(counts, 10)
    [IPv6Network('::ffff:808:800/117'), IPv6Network('::ffff:808:0/117')]
    """

    def count_replies(subset):
        total = 0
        for network, count in counts.items():
            if network.subnet_of(subset):
                total += count
        return total

    candidates = [(ip_network("::/0"), count_replies(ip_network("::/0")))]
    subsets = []

    while candidates:
        candidate, n_replies = candidates.pop()
        if n_replies == 0:
            continue
        elif n_replies <= max_replies_per_subset:
            subsets.append(candidate)
        else:
            # Can we split?
            a, b = tuple(candidate.subnets(prefixlen_diff=1))
            n_replies_a = count_replies(a)
            n_replies_b = count_replies(b)
            if n_replies_a + n_replies_b == 0:
                subsets.append(candidate)
            else:
                candidates.append((a, n_replies_a))
                candidates.append((b, n_replies_b))

    return subsets


async def compute_next_round(
    host: str,
    table: str,
    round_: int,
    src_addr: str,
    src_port: int,
    dst_port: int,
    mapper,
    adaptive_eps: bool = False,
    max_replies_per_subset=64_000_000,
    probe_far_ttls: bool = False,
    skip_unpopulated_ttls: bool = False,
    ttl_limit: int = 40,
):
    client = Client(host=host)

    # Compute the subsets such that each queries runs on at-most X rows.
    preflen_v4, preflen_v6 = 8, 8
    query = CountReplies(src_addr, preflen_v4, preflen_v6)

    counts = {}
    for chunk, count in await query.execute(client, table):
        if chunk.ipv4_mapped:
            net = ip_network(str(chunk) + f"/{96+preflen_v4}")
        else:
            net = ip_network(str(chunk) + f"/{preflen_v6}")
        counts[net] = count

    subsets = get_subsets(counts, max_replies_per_subset)

    # Skip the TTLs where few nodes are discovered, in order to avoid
    # re-probing them extensively (e.g. low TTLs).
    if skip_unpopulated_ttls:
        threshold = 100
        query = CountNodesPerTTL(src_addr, ttl_limit)
        nodes_per_ttl = await query.execute(client, table)
        skipped_ttls = {ttl for ttl, n_nodes in nodes_per_ttl if n_nodes < threshold}
    else:
        skipped_ttls = set()

    if probe_far_ttls:
        async for specs in far_ttls_probes(
            client,
            table,
            round_,
            src_addr,
            src_port,
            dst_port,
            far_ttl_min=20,
            far_ttl_max=ttl_limit,
            subsets=subsets,
        ):
            yield specs

    async for specs in next_round_probes(
        client,
        table,
        round_,
        src_addr,
        src_port,
        dst_port,
        mapper,
        skipped_ttls,
        adaptive_eps=adaptive_eps,
        subsets=subsets,
    ):
        yield specs


async def far_ttls_probes(
    client: Client,
    table: str,
    round_: int,
    src_addr: str,
    src_port: int,
    dst_port: int,
    far_ttl_min: int,
    far_ttl_max: int,
    subsets=(ip_network("::0/0"),),
):
    query = GetMaxTTL(src_addr, round_)
    rows = query.execute_iter(client, table, subsets)

    async for dst_addr, max_ttl in rows:
        if far_ttl_min <= max_ttl <= far_ttl_max:
            probe_specs = []
            dst_addr = int(dst_addr.ipv4_mapped)
            for ttl in range(max_ttl + 1, far_ttl_max + 1):
                probe_specs.append(
                    (str(dst_addr), str(src_port), str(dst_port), str(ttl))
                )
            if probe_specs:
                yield probe_specs


async def next_round_probes(
    client: Client,
    table: str,
    round_: int,
    src_addr: str,
    src_port: int,
    dst_port: int,
    mapper: FlowMapper,
    skipped_ttls,
    adaptive_eps=True,
    subsets=(ip_network("::0/0"),),
):
    # TODO: IPv6
    prefix_len = 24

    query = GetNextRound(src_addr, round_, adaptive_eps=adaptive_eps)
    rows = query.execute_iter(client, table, subsets)

    async for row in rows:
        row = GetNextRound.Row(*row)
        dst_prefix = int(row.dst_prefix.ipv4_mapped)  # TODO: IPv6

        if row.skip_prefix:
            continue

        probe_specs = []
        for ttl, n_to_send in enumerate(row.probes):
            if ttl in skipped_ttls:
                continue

            flow_ids = range(row.prev_max_flow[ttl], row.prev_max_flow[ttl] + n_to_send)
            for flow_id in flow_ids:
                addr_offset, port_offset = mapper.offset(
                    flow_id=flow_id,
                    prefix_size=2 ** (32 - prefix_len),
                    prefix=dst_prefix,
                )

                if port_offset > 0 and (
                    (row.min_dst_port != dst_port)
                    or (row.max_dst_port != dst_port)  # noqa
                    or (row.min_src_port < src_port)  # noqa
                ):
                    # There is a case where max_src_port > sport,
                    # but real_flow_id < 255 (see dst_prefix == 28093440)
                    # It's probably NAT, nothing to do more
                    continue

                probe_specs.append(
                    (
                        str(dst_prefix + addr_offset),
                        str(src_port + port_offset),
                        str(dst_port),
                        # TTL in enumerate starts at 0 instead of 1
                        str(ttl + 1),
                    )
                )
        if probe_specs:
            yield probe_specs
