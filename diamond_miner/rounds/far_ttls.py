from clickhouse_driver import Client

from diamond_miner.defaults import (
    DEFAULT_PROBE_DST_PORT,
    DEFAULT_PROBE_SRC_PORT,
    DEFAULT_SUBSET,
)
from diamond_miner.logging import logger
from diamond_miner.queries import GetMaxTTL
from diamond_miner.timer import Timer


def far_ttls_probes(
    client: Client,
    table: str,
    round_: int,
    probe_src_addr: str,
    probe_src_port: int = DEFAULT_PROBE_SRC_PORT,
    probe_dst_port: int = DEFAULT_PROBE_DST_PORT,
    far_ttl_min: int = 20,
    far_ttl_max: int = 40,
    subsets=(DEFAULT_SUBSET,),
):
    query = GetMaxTTL(
        probe_src_addr=probe_src_addr,
        round_leq=round_,
    )
    rows = query.execute_iter(client, table, subsets)

    # Monitor time spent in the loop and in foreign code, excluding database code.
    loop_timer = Timer()
    yield_timer = Timer()

    rows = (
        (dst_addr, max_ttl)
        for dst_addr, max_ttl in rows
        if far_ttl_min <= max_ttl <= far_ttl_max
    )

    for dst_addr, max_ttl in rows:
        probe_specs = []
        with loop_timer:
            for ttl in range(max_ttl + 1, far_ttl_max + 1):
                probe_specs.append(
                    (
                        int(dst_addr),
                        probe_src_port,
                        probe_dst_port,
                        ttl,
                    )
                )
        if probe_specs:
            with yield_timer:
                yield probe_specs

    logger.info(f"timer=far_ttls_probes_loop, time_ms={loop_timer.total_ms}")
    logger.info(f"timer=far_ttls_probes_yield, time_ms={yield_timer.total_ms}")
