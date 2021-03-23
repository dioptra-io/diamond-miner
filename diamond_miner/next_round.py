from ipaddress import ip_network
from typing import Iterable

from aioch import Client

from diamond_miner.config import Config
from diamond_miner.defaults import DEFAULT_SUBSET
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
        if n_replies <= max_replies_per_subset:
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


async def compute_next_round(config: Config, client: Client, table: str, round_: int):
    # Compute the subsets such that each queries runs on at-most X rows.
    count_replies_query = CountReplies(
        probe_src_addr=config.probe_src_addr, prefix_len_v4=8, prefix_len_v6=8
    )

    counts = {}
    for chunk, count in await count_replies_query.execute(client, table):
        if chunk.ipv4_mapped:
            net = ip_network(str(chunk) + f"/{96+8}")
        else:
            net = ip_network(str(chunk) + f"/{8}")
        counts[net] = count

    subsets = get_subsets(counts, config.max_replies_per_subset)

    # Skip the TTLs where few nodes are discovered, in order to avoid
    # re-probing them extensively (e.g. low TTLs).
    if config.skip_unpopulated_ttls:
        threshold = 100
        count_nodes_query = CountNodesPerTTL(
            probe_src_addr=config.probe_src_addr, max_ttl=config.far_ttl_max
        )
        nodes_per_ttl = await count_nodes_query.execute(client, table)
        skipped_ttls = {ttl for ttl, n_nodes in nodes_per_ttl if n_nodes < threshold}
    else:
        skipped_ttls = set()

    if config.probe_far_ttls:
        async for specs in far_ttls_probes(
            config=config, client=client, table=table, round_=round_, subsets=subsets
        ):
            yield specs

    async for specs in next_round_probes(
        config=config,
        client=client,
        table=table,
        round_=round_,
        skipped_ttls=skipped_ttls,
        subsets=subsets,
    ):
        yield specs


async def far_ttls_probes(
    config: Config,
    client: Client,
    table: str,
    round_: int,
    subsets=(DEFAULT_SUBSET,),
):
    query = GetMaxTTL(
        prefix_len_v4=config.prefix_len_v4,
        prefix_len_v6=config.prefix_len_v6,
        probe_src_addr=config.probe_src_addr,
        round_leq=round_,
    )
    rows = query.execute_iter(client, table, subsets)

    async for dst_addr, max_ttl in rows:
        if config.far_ttl_min <= max_ttl <= config.far_ttl_max:
            probe_specs = []
            for ttl in range(max_ttl + 1, config.far_ttl_max + 1):
                probe_specs.append(
                    (int(dst_addr), config.probe_src_port, config.probe_dst_port, ttl)
                )
            if probe_specs:
                yield probe_specs


async def next_round_probes(
    config: Config,
    client: Client,
    table: str,
    round_: int,
    skipped_ttls: Iterable[int],
    subsets=(DEFAULT_SUBSET,),
):
    query = GetNextRound(
        adaptive_eps=config.adaptive_eps,
        prefix_len_v4=config.prefix_len_v4,
        prefix_len_v6=config.prefix_len_v6,
        probe_src_addr=config.probe_src_addr,
        round_leq=round_,
    )
    rows = query.execute_iter(client, table, subsets)

    async for row in rows:
        row = GetNextRound.Row(*row)
        dst_prefix_int = int(row.dst_prefix)

        # prefix_size: number of addresses in the prefix.
        prefix_size = 2 ** (128 - config.prefix_len_v6)
        if row.dst_prefix.ipv4_mapped:
            prefix_size = 2 ** (32 - config.prefix_len_v4)

        if row.skip_prefix:
            continue

        probe_specs = []
        for ttl, n_to_send in enumerate(row.probes):
            if ttl in skipped_ttls:
                continue

            flow_ids = range(row.prev_max_flow[ttl], row.prev_max_flow[ttl] + n_to_send)
            for flow_id in flow_ids:
                addr_offset, port_offset = config.mapper.offset(
                    flow_id=flow_id,
                    prefix_size=prefix_size,
                    prefix=dst_prefix_int,
                )

                if port_offset > 0 and (
                    (row.min_dst_port != config.probe_dst_port)
                    or (row.max_dst_port != config.probe_dst_port)  # noqa
                    or (row.min_src_port < config.probe_src_port)  # noqa
                ):
                    # There is a case where max_src_port > sport,
                    # but real_flow_id < 255 (see dst_prefix == 28093440)
                    # It's probably NAT, nothing to do more
                    continue

                probe_specs.append(
                    (
                        dst_prefix_int + addr_offset,
                        config.probe_src_port + port_offset,
                        config.probe_dst_port,
                        # TTL in enumerate starts at 0 instead of 1
                        ttl + 1,
                    )
                )
        if probe_specs:
            yield probe_specs
