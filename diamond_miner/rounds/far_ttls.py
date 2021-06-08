from typing import AsyncIterator, Iterable, List

from aioch import Client

from diamond_miner.defaults import (
    DEFAULT_PROBE_DST_PORT,
    DEFAULT_PROBE_SRC_PORT,
    PROTOCOLS,
    UNIVERSE_SUBSET,
)
from diamond_miner.logging import logger
from diamond_miner.queries import GetMaxTTL
from diamond_miner.timer import Timer
from diamond_miner.typing import IPNetwork, Probe


async def far_ttls_probes(
    client: Client,
    measurement_id: str,
    round_: int,
    probe_src_port: int = DEFAULT_PROBE_SRC_PORT,
    probe_dst_port: int = DEFAULT_PROBE_DST_PORT,
    far_ttl_min: int = 20,
    far_ttl_max: int = 40,
    subsets: Iterable[IPNetwork] = (UNIVERSE_SUBSET,),
) -> AsyncIterator[List[Probe]]:
    """TODO"""
    query = GetMaxTTL(round_leq=round_)
    rows = query.execute_iter_async(client, measurement_id, subsets)

    # Monitor time spent in the loop and in foreign code, excluding database code.
    loop_timer = Timer()
    yield_timer = Timer()

    rows = (
        (protocol, dst_addr, max_ttl)
        async for protocol, dst_addr, max_ttl in rows
        if far_ttl_min <= max_ttl <= far_ttl_max
    )

    async for protocol, dst_addr, max_ttl in rows:
        probe_specs = []
        protocol_str = PROTOCOLS[protocol]
        with loop_timer:
            for ttl in range(max_ttl + 1, far_ttl_max + 1):
                probe_specs.append(
                    (int(dst_addr), probe_src_port, probe_dst_port, ttl, protocol_str)
                )
        if probe_specs:
            with yield_timer:
                yield probe_specs

    logger.info(f"timer=far_ttls_probes_loop, time_ms={loop_timer.total_ms}")
    logger.info(f"timer=far_ttls_probes_yield, time_ms={yield_timer.total_ms}")
