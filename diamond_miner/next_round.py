from ipaddress import ip_address, ip_network

from aioch import Client

from diamond_miner.queries.count_nodes_per_ttl import CountNodesPerTTL
from diamond_miner.queries.get_max_ttl import GetMaxTTL
from diamond_miner.queries.get_next_round import GetNextRound


async def compute_next_round(
    host: str,
    table: str,
    round_: int,
    src_addr: str,
    src_port: int,
    dst_port: int,
    mapper,
    adaptive_eps: bool = False,
    probe_far_ttls: bool = False,
    skip_unpopulated_ttls: bool = False,
    subset_prefix_len=6,
    ttl_limit: int = 40,
):
    client = Client(host=host)

    # TODO: IPv6
    subsets = ip_network("0.0.0.0/0").subnets(new_prefix=subset_prefix_len)

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
            dst_addr = int(ip_address(dst_addr))
            for ttl in range(max_ttl + 1, far_ttl_max + 1):
                probe_specs.append(
                    (str(dst_addr), str(src_port), str(dst_port), str(ttl))
                )
            yield probe_specs


async def next_round_probes(
    client: Client,
    table: str,
    round_: int,
    src_addr: str,
    src_port: int,
    dst_port: int,
    mapper,
    skipped_ttls,
    adaptive_eps=True,
    subsets=(ip_network("::0/0"),),
):
    # TODO: IPv6
    prefix_len = 24

    query = GetNextRound(src_addr, round_, adaptive_eps=adaptive_eps)
    rows = query.execute_iter(client, table, subsets)

    async for row in rows:
        dst_prefix = int(ip_address(row.dst_prefix))

        if row.skip_prefix:
            continue

        probe_specs = []
        for ttl, n_to_send in enumerate(row.probes):
            if ttl in skipped_ttls:
                continue

            flow_ids = range(row.prev_max_flow[ttl], row.prev_max_flow[ttl] + n_to_send)
            for flow_id in flow_ids:
                addr_offset, port_offset = mapper.offset(
                    flow_id, 2 ** (32 - prefix_len), row.dst_prefix
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

        yield probe_specs
