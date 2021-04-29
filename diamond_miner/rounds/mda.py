from clickhouse_driver import Client

from diamond_miner.defaults import (
    DEFAULT_PROBE_DST_PORT,
    DEFAULT_PROBE_SRC_PORT,
    DEFAULT_SUBSET,
)
from diamond_miner.logging import logger
from diamond_miner.mappers import FlowMapper
from diamond_miner.queries import CountNodesPerTTL, GetNextRound
from diamond_miner.timer import Timer


def mda_probes(
    client: Client,
    table: str,
    round_: int,
    mapper_v4: FlowMapper,
    mapper_v6: FlowMapper,
    probe_src_addr: str,
    probe_src_port: int = DEFAULT_PROBE_SRC_PORT,
    probe_dst_port: int = DEFAULT_PROBE_DST_PORT,
    adaptive_eps: bool = False,
    skip_unpopulated_ttls: bool = False,
    skip_unpopulated_ttls_threshold: int = 100,
    subsets=(DEFAULT_SUBSET,),
):
    # Skip the TTLs where few nodes are discovered, in order to avoid
    # re-probing them extensively (e.g. low TTLs).
    skipped_ttls = set()

    if skip_unpopulated_ttls:
        count_nodes_query = CountNodesPerTTL(probe_src_addr=probe_src_addr)
        nodes_per_ttl = count_nodes_query.execute(client, table)
        skipped_ttls = {
            ttl
            for ttl, n_nodes in nodes_per_ttl
            if n_nodes < skip_unpopulated_ttls_threshold
        }

    query = GetNextRound(
        adaptive_eps=adaptive_eps,
        probe_src_addr=probe_src_addr,
        round_leq=round_,
    )
    rows = query.execute_iter(client, table, subsets)

    # Monitor time spent in the loop and in foreign code, excluding database code.
    loop_timer = Timer()
    yield_timer = Timer()

    for row in rows:
        loop_timer.start()
        row = GetNextRound.Row(*row)
        dst_prefix_int = int(row.dst_prefix)
        mapper = mapper_v4 if row.dst_prefix.ipv4_mapped else mapper_v6

        if row.skip_prefix:
            continue

        probe_specs = []
        for ttl, n_to_send in enumerate(row.probes):
            if ttl in skipped_ttls:
                continue

            flow_ids = range(row.prev_max_flow[ttl], row.prev_max_flow[ttl] + n_to_send)
            for flow_id in flow_ids:
                addr_offset, port_offset = mapper.offset(
                    flow_id=flow_id, prefix=dst_prefix_int
                )

                if port_offset > 0 and (
                    (row.min_dst_port != probe_dst_port)
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
                        probe_src_port + port_offset,
                        probe_dst_port,
                        # TTL in enumerate starts at 0 instead of 1
                        ttl + 1,
                    )
                )
        loop_timer.stop()
        if probe_specs:
            with yield_timer:
                yield probe_specs

    logger.info(f"timer=next_round_probes_loop, time_ms={loop_timer.total_ms}")
    logger.info(f"timer=next_round_probes_yield, time_ms={yield_timer.total_ms}")
