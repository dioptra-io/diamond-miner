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
    measurement_id: str,
    round_: int,
    mapper_v4: FlowMapper,
    mapper_v6: FlowMapper,
    probe_src_port: int = DEFAULT_PROBE_SRC_PORT,
    probe_dst_port: int = DEFAULT_PROBE_DST_PORT,
    adaptive_eps: bool = False,
    n_workers: int = (os.cpu_count() or 2) // 2,
) -> int:
    """
    https://lemire.me/blog/2010/03/15/external-memory-shuffling-in-linear-time/
    """
    # TODO: IPv6
    # TODO: Better subsets based on the number of links.
    # NOTE: We create more subsets than workers in order to keep all the worker busy.
    subsets = list(
        ip_network("::ffff:0.0.0.0/96").subnets(prefixlen_diff=int(log2(n_workers * 4)))
    )
    n_files_per_subset = 8192 // len(subsets)
    logger.info(
        "mda_probes n_workers=%s n_subsets=%s n_files_per_subset=%s",
        n_workers,
        len(subsets),
        n_files_per_subset,
    )

    loop = asyncio.get_running_loop()
    n_probes = 0

    with TemporaryDirectory(dir=filepath.parent) as temp_dir:
        with ProcessPoolExecutor(n_workers) as pool:
            futures = [
                loop.run_in_executor(
                    pool,
                    worker,
                    Path(temp_dir) / f"subset_{i}",
                    client,
                    measurement_id,
                    round_,
                    mapper_v4,
                    mapper_v6,
                    probe_src_port,
                    probe_dst_port,
                    adaptive_eps,
                    subset,
                    n_files_per_subset,
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
                n_probes += future.result()

        logger.info("mda_probes status=merging")
        file_list = Path(temp_dir) / "files.txt"
        proc = await asyncio.create_subprocess_shell(
            f"find {temp_dir} -name 'subset_*.csv.zst' | shuf > {file_list}"
        )
        await proc.wait()
        proc = await asyncio.create_subprocess_shell(
            f"xargs cat < {file_list} > {filepath}"
        )
        await proc.wait()

    return n_probes


def worker(
    prefix: Path,
    client: Client,
    measurement_id: str,
    round_: int,
    mapper_v4: FlowMapper,
    mapper_v6: FlowMapper,
    probe_src_port: int,
    probe_dst_port: int,
    adaptive_eps: bool,
    subset: IPNetwork,
    n_files: int,
) -> int:
    """
    Execute the GetNextRound query on the specified subset, and write the probes to the specified file.
    """
    # TODO: random.shuffle is slow...
    # A potentially simpler and better way would be to shuffle
    # the result of the next round query, which is small (~10M rows),
    # and to directly write the probes to a file depending on hash(flow_id).
    # e.g. ORDER BY rand() in GetNextRound.

    # The larger `max_probes_in_memory`, the better the performance
    # (less calls to `probes.clear()`) and the randomization but the more the memory usage.
    max_probes_in_memory = 1_000_000

    outputs = []
    for i in range(n_files):
        ctx = ZstdCompressor(level=1)
        file = prefix.with_suffix(f".{i}.csv.zst").open("wb")
        stream = ctx.stream_writer(file)
        outputs.append((ctx, file, stream))

    probes_by_file: List[List[Probe]] = [[] for _ in range(n_files)]
    n_probes = 0

    for probe in mda_probes(
        client,
        measurement_id,
        round_,
        mapper_v4,
        mapper_v6,
        probe_src_port,
        probe_dst_port,
        adaptive_eps,
        (subset,),
    ):
        # probe[:-2] => (probe_dst_addr, probe_src_port, probe_dst_port)
        probes_by_file[hash(probe[:-2]) % n_files].append(probe)
        n_probes += 1

        # Flush in-memory probes
        if n_probes % max_probes_in_memory == 0:
            for file_id, probes in enumerate(probes_by_file):
                ctx, file, stream = outputs[file_id]
                random.shuffle(probes)
                for probe_ in probes:
                    stream.write(format_probe(*probe_).encode("ascii") + b"\n")
                probes.clear()

    # Flush remaining probes
    for file_id, probes in enumerate(probes_by_file):
        ctx, file, stream = outputs[file_id]
        random.shuffle(probes)
        for probe_ in probes:
            stream.write(format_probe(*probe_).encode("ascii") + b"\n")
        probes.clear()

    for ctx, file, stream in outputs:
        stream.close()
        file.close()

    return n_probes
