import asyncio
import os
import random
from concurrent.futures import ProcessPoolExecutor
from ipaddress import ip_network
from math import log2
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List

from clickhouse_driver import Client
from zstandard import ZstdCompressor

from diamond_miner.defaults import DEFAULT_PROBE_DST_PORT, DEFAULT_PROBE_SRC_PORT
from diamond_miner.format import format_probe
from diamond_miner.logging import logger
from diamond_miner.rounds.mda import mda_probes
from diamond_miner.typing import FlowMapper, IPNetwork, Probe


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
    # TODO: Better subsets based on the number of links.
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
                    Path(temp_dir) / f"subset_{i}",
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
            done, pending = await asyncio.wait(
                futures, return_when=asyncio.FIRST_EXCEPTION
            )
            for future in pending:
                future.cancel()
            for future in done:
                if e := future.exception():
                    raise e

        logger.info("mda_probes status=merging")
        files = [str(x) for x in Path(temp_dir).glob("subset_*.csv.zst")]
        random.shuffle(files)
        proc = await asyncio.create_subprocess_shell(
            f"cat {' '.join(files)} > {filepath}"
        )
        await proc.wait()


def worker(
    prefix: Path,
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
    # TODO: How to ensure that we will never run out of memory?
    # => Get subsets based on number of links.
    n_buckets = 16
    buckets: List[List[Probe]] = [[] for _ in range(n_buckets)]
    ctx = ZstdCompressor(level=1)
    # https://lemire.me/blog/2010/03/15/external-memory-shuffling-in-linear-time/
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
        # probe[:-2] => (probe_dst_addr, probe_src_port, probe_dst_port)
        buckets[hash(probe[:-2]) % n_buckets].append(probe)
    for i, bucket in enumerate(buckets):
        random.shuffle(bucket)
        with prefix.with_suffix(f".{i}.csv.zst").open("wb") as f:
            with ctx.stream_writer(f) as compressor:
                for probe in bucket:
                    compressor.write(format_probe(*probe).encode("ascii") + b"\n")
