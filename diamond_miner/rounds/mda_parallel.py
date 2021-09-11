import asyncio
import os
import random
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List, Tuple

from zstandard import ZstdCompressor

from diamond_miner.defaults import DEFAULT_PROBE_DST_PORT, DEFAULT_PROBE_SRC_PORT
from diamond_miner.format import format_probe
from diamond_miner.logging import logger
from diamond_miner.queries import GetNextRound
from diamond_miner.rounds.mda import mda_probes
from diamond_miner.subsets import subsets_for
from diamond_miner.typing import FlowMapper, IPNetwork, Probe


async def mda_probes_parallel(
    filepath: Path,
    url: str,
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
    Compute the probes to send given the previously discovered links.
    This function shuffle the probes on-disk using the following algorithm:
    https://lemire.me/blog/2010/03/15/external-memory-shuffling-in-linear-time/
    """
    loop = asyncio.get_running_loop()

    # NOTE: Make sure that the parameter of GetNextRound are equal to the
    # ones defines in mda_probes, in order to guarantee optimal subsets.
    subsets = await subsets_for(
        GetNextRound(
            adaptive_eps=adaptive_eps,
            round_leq=round_,
            filter_virtual=True,
            filter_inter_round=True,
        ),
        url,
        measurement_id,
    )

    if not subsets:
        return 0

    n_files_per_subset = 8192 // len(subsets)

    logger.info(
        "mda_probes n_workers=%s n_subsets=%s n_files_per_subset=%s",
        n_workers,
        len(subsets),
        n_files_per_subset,
    )

    with TemporaryDirectory(dir=filepath.parent) as temp_dir:
        with ProcessPoolExecutor(n_workers) as pool:
            futures = [
                loop.run_in_executor(
                    pool,
                    worker,
                    Path(temp_dir) / f"subset_{i}",
                    url,
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
            n_probes = sum(await asyncio.gather(*futures))

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
    url: str,
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
    Execute the :class:`diamond_miner.queries.GetNextRound` query on the specified subset, and write the probes to the specified file.
    """
    # TODO: random.shuffle is slow...
    # A potentially simpler and better way would be to shuffle
    # the result of the next round query, which is small (~10M rows),
    # and to directly write the probes to a file depending on hash(flow_id).
    # e.g. ORDER BY rand() in GetNextRound.

    # The larger `max_probes_in_memory`, the better the performance
    # (less calls to `probes.clear()`) and the randomization but the more the memory usage.
    max_probes_in_memory = 1_000_000

    outputs: List[Tuple] = []
    for i in range(n_files):
        ctx = ZstdCompressor(level=1)
        file = prefix.with_suffix(f".{i}.csv.zst").open("wb")
        stream = ctx.stream_writer(file)
        outputs.append((ctx, file, stream))

    probes_by_file: List[List[Probe]] = [[] for _ in range(n_files)]
    n_probes = 0

    for probe in mda_probes(
        url,
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
        if n_probes % max_probes_in_memory == 0:
            flush(probes_by_file, outputs)

    flush(probes_by_file, outputs)

    for ctx, file, stream in outputs:
        stream.close()  # type: ignore
        file.close()

    return n_probes


def flush(probes_by_file: List[List[Probe]], outputs: List[Tuple]) -> None:
    for file_id, probes in enumerate(probes_by_file):
        _, _, stream = outputs[file_id]
        random.shuffle(probes)
        for probe in probes:
            stream.write(format_probe(*probe).encode("ascii") + b"\n")
        probes.clear()
