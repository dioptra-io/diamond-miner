import asyncio
import os
from concurrent.futures import ProcessPoolExecutor
from ipaddress import ip_network
from math import log2
from pathlib import Path
from tempfile import TemporaryDirectory

from clickhouse_driver import Client

from diamond_miner.defaults import DEFAULT_PROBE_DST_PORT, DEFAULT_PROBE_SRC_PORT
from diamond_miner.format import format_probe
from diamond_miner.logging import logger
from diamond_miner.rounds.mda import mda_probes
from diamond_miner.typing import FlowMapper, IPNetwork


async def mda_probes_parallel(
    filepath: Path,
    client: Client,
    table: str,
    round_: int,
    mapper_v4: FlowMapper,
    mapper_v6: FlowMapper,
    probe_src_port: int = DEFAULT_PROBE_SRC_PORT,
    probe_dst_port: int = DEFAULT_PROBE_DST_PORT,
    adaptive_eps: bool = False,
    n_workers: int = (os.cpu_count() or 2) // 2,
) -> None:
    # TODO: IPv6
    # NOTE: We create more subsets than workers in order to keep all the worker busy.
    subsets = list(
        ip_network("::ffff:0.0.0.0/96").subnets(prefixlen_diff=int(log2(n_workers * 4)))
    )
    logger.info("mda_probes n_subsets=%s n_workers=%s", len(subsets), n_workers)
    loop = asyncio.get_running_loop()

    with TemporaryDirectory(dir=filepath.parent) as temp_dir:
        with ProcessPoolExecutor(n_workers) as pool:
            futures = [
                loop.run_in_executor(
                    pool,
                    worker,
                    Path(temp_dir) / f"subset_{i}.csv",
                    client,
                    table,
                    round_,
                    mapper_v4,
                    mapper_v6,
                    probe_src_port,
                    probe_dst_port,
                    adaptive_eps,
                    subset,
                )
                for i, subset in enumerate(subsets)
            ]
            done, pending = await asyncio.wait(futures)
            for future in pending:
                future.cancel()
            for future in done:
                if e := future.exception():
                    raise e

        logger.info("mda_probes status=merging")
        proc = await asyncio.create_subprocess_shell(
            f"cat {temp_dir}/subset_*.csv > {filepath}"
        )
        await proc.wait()

        logger.info("mda_probes status=cleaning")
        proc = await asyncio.create_subprocess_shell(f"rm {temp_dir}/subset_*.csv")
        await proc.wait()


def worker(
    filepath: Path,
    client: Client,
    table: str,
    round_: int,
    mapper_v4: FlowMapper,
    mapper_v6: FlowMapper,
    probe_src_port: int,
    probe_dst_port: int,
    adaptive_eps: bool,
    subset: IPNetwork,
) -> None:
    """
    Execute the GetNextRound query on the specified subset,
    and write the probes to the specified file.
    """
    with filepath.open("w") as f:
        for probe in mda_probes(
            client,
            table,
            round_,
            mapper_v4,
            mapper_v6,
            probe_src_port,
            probe_dst_port,
            adaptive_eps,
            (subset,),
        ):
            f.write(format_probe(*probe) + "\n")
