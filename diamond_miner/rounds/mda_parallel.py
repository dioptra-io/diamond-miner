import asyncio
import os
import random
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from ipaddress import ip_network
from math import log2
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict, List, Tuple

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
    """
    https://lemire.me/blog/2010/03/15/external-memory-shuffling-in-linear-time/
    """
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
        file_list = Path(temp_dir) / "files.txt"
        proc = await asyncio.create_subprocess_shell(
            f"find {temp_dir} -name 'subset_*.csv.zst' | shuf > {file_list}"
        )
        await proc.wait()
        proc = await asyncio.create_subprocess_shell(
            f"xargs cat < {file_list} > {filepath}"
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
    Execute the GetNextRound query on the specified subset, and write the probes to the specified file.
    """
    # The larger `n_file`, the better the randomization.
    n_files = 32
    # The larger `max_probes_in_memory`, the better the performance
    # (less calls to `probes_by_flow.clear()`) and the more the memory usage.
    max_probes_in_memory = 1_000_000

    outputs = []
    for i in range(n_files):
        ctx = ZstdCompressor(level=1)
        file = prefix.with_suffix(f".{i}.csv.zst").open("wb")
        stream = ctx.stream_writer(file)
        outputs.append((ctx, file, stream))

    probes_by_flow: Dict[Tuple[int, int, int], List[Probe]] = defaultdict(list)

    for i, probe in enumerate(
        mda_probes(
            client,
            table,
            round_,
            mapper_v4,
            mapper_v6,
            probe_src_port,
            probe_dst_port,
            adaptive_eps,
            (subset,),
        )
    ):
        # probe[:-2] => (probe_dst_addr, probe_src_port, probe_dst_port)
        probes_by_flow[probe[:-2]].append(probe)

        # Flush in-memory probes
        if (i + 1) % max_probes_in_memory == 0:
            for probes in probes_by_flow.values():
                ctx, file, stream = random.choice(outputs)
                for probe_ in probes:
                    stream.write(format_probe(*probe_).encode("ascii") + b"\n")
            probes_by_flow.clear()

    # Flush remaining probes
    for probes in probes_by_flow.values():
        ctx, file, stream = random.choice(outputs)
        for probe_ in probes:
            stream.write(format_probe(*probe_).encode("ascii") + b"\n")

    for ctx, file, stream in outputs:
        stream.close()
        file.close()
