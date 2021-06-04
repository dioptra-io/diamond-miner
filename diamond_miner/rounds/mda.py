from typing import AsyncIterator, Iterable, List

from aioch import Client

from diamond_miner.defaults import (
    DEFAULT_PROBE_DST_PORT,
    DEFAULT_PROBE_SRC_PORT,
    PROTOCOLS,
    UNIVERSE_SUBSET,
)
from diamond_miner.logging import logger
from diamond_miner.queries import CountNodesPerTTL, GetNextRound
from diamond_miner.timer import Timer
from diamond_miner.typing import FlowMapper, IPNetwork, Probe


async def mda_probes(
    client: Client,
    table: str,
    round_: int,
    mapper_v4: FlowMapper,
    mapper_v6: FlowMapper,
    probe_src_port: int = DEFAULT_PROBE_SRC_PORT,
    probe_dst_port: int = DEFAULT_PROBE_DST_PORT,
    adaptive_eps: bool = False,
    skip_unpopulated_ttls: bool = False,
    # TODO: Compute CountNodesPerTTL on the links table instead.
    skip_unpopulated_ttls_table: str = "",
    skip_unpopulated_ttls_threshold: int = 100,
    subsets: Iterable[IPNetwork] = (UNIVERSE_SUBSET,),
) -> AsyncIterator[List[Probe]]:
    """TODO"""
    # Skip the TTLs where few nodes are discovered, in order to avoid
    # re-probing them extensively (e.g. low TTLs).
    skipped_ttls = set()

    if skip_unpopulated_ttls:
        count_nodes_query = CountNodesPerTTL()
        nodes_per_ttl = await count_nodes_query.execute_async(
            client, skip_unpopulated_ttls_table
        )
        skipped_ttls = {
            ttl
            for ttl, n_nodes in nodes_per_ttl
            if n_nodes < skip_unpopulated_ttls_threshold
        }

    query = GetNextRound(adaptive_eps=adaptive_eps, round_leq=round_)
    rows = query.execute_iter_async(client, table, subsets)

    # Monitor time spent in the loop and in foreign code, excluding database code.
    loop_timer = Timer()
    yield_timer = Timer()

    async for row in rows:
        loop_timer.start()
        row = GetNextRound.Row(*row)
        dst_prefix_int = int(row.dst_prefix)
        protocol_str = PROTOCOLS[row.protocol]
        mapper = mapper_v4 if row.dst_prefix.ipv4_mapped else mapper_v6

        probe_specs = []
        for ttl, n_to_send, prev_max_flow in zip(
            row.ttls, row.probes, row.prev_max_flow
        ):
            if ttl in skipped_ttls:
                continue

            flow_ids = range(prev_max_flow, prev_max_flow + n_to_send)
            for flow_id in flow_ids:
                addr_offset, port_offset = mapper.offset(
                    flow_id=flow_id, prefix=dst_prefix_int
                )

                probe_specs.append(
                    (
                        dst_prefix_int + addr_offset,
                        probe_src_port + port_offset,
                        probe_dst_port,
                        ttl,
                        protocol_str,
                    )
                )
        loop_timer.stop()
        if probe_specs:
            with yield_timer:
                yield probe_specs

    logger.info(f"timer=next_round_probes_loop, time_ms={loop_timer.total_ms}")
    logger.info(f"timer=next_round_probes_yield, time_ms={yield_timer.total_ms}")
