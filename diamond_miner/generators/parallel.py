import os
import random
import shutil
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List, Optional, Tuple

from zstandard import ZstdCompressor

from diamond_miner.defaults import (
    DEFAULT_PREFIX_SIZE_V4,
    DEFAULT_PREFIX_SIZE_V6,
    DEFAULT_PROBE_DST_PORT,
    DEFAULT_PROBE_SRC_PORT,
)
from diamond_miner.format import format_probe
from diamond_miner.generators.database import probe_generator_from_database
from diamond_miner.logger import logger
from diamond_miner.mappers import SequentialFlowMapper
from diamond_miner.queries import GetProbesDiff
from diamond_miner.subsets import subsets_for
from diamond_miner.typing import FlowMapper, IPNetwork, Probe


def probe_generator_parallel(
    filepath: Path,
    url: str,
    measurement_id: str,
    round_: int,
    *,
    mapper_v4: FlowMapper = SequentialFlowMapper(DEFAULT_PREFIX_SIZE_V4),
    mapper_v6: FlowMapper = SequentialFlowMapper(DEFAULT_PREFIX_SIZE_V6),
    probe_src_port: int = DEFAULT_PROBE_SRC_PORT,
    probe_dst_port: int = DEFAULT_PROBE_DST_PORT,
    probe_ttl_geq: Optional[int] = None,
    probe_ttl_leq: Optional[int] = None,
    max_open_files: int = 8192,
    n_workers: int = (os.cpu_count() or 2) // 2,
) -> int:
    """
    Compute the probes to send given the previously discovered links.
    This function shuffle the probes on-disk using the following algorithm:
    https://lemire.me/blog/2010/03/15/external-memory-shuffling-in-linear-time/
    """
    # TODO: These subsets are sub-optimal, `CountProbesPerPrefix` should count
    # the actual number of probes to be sent, not the total number of probes sent.
    subsets = subsets_for(
        GetProbesDiff(
            round_eq=round_, probe_ttl_geq=probe_ttl_geq, probe_ttl_leq=probe_ttl_leq
        ),
        url,
        measurement_id,
    )

    if not subsets:
        return 0

    n_files_per_subset = max_open_files // len(subsets)

    logger.info(
        "mda_probes n_workers=%s n_subsets=%s n_files_per_subset=%s",
        n_workers,
        len(subsets),
        n_files_per_subset,
    )

    with TemporaryDirectory(dir=filepath.parent) as temp_dir:
        with ProcessPoolExecutor(n_workers) as executor:
            futures = [
                executor.submit(
                    worker,
                    Path(temp_dir) / f"subset_{i}",
                    url,
                    measurement_id,
                    round_,
                    mapper_v4,
                    mapper_v6,
                    probe_src_port,
                    probe_dst_port,
                    probe_ttl_geq,
                    probe_ttl_leq,
                    subset,
                    n_files_per_subset,
                )
                for i, subset in enumerate(subsets)
            ]
            n_probes = sum(future.result() for future in as_completed(futures))

        files = list(Path(temp_dir).glob("subset_*.csv.zst"))
        random.shuffle(files)

        logger.info("mda_probes status=merging n_files=%s", len(files))
        with filepath.open("wb") as out:
            for f in files:
                with f.open("rb") as inp:
                    shutil.copyfileobj(inp, out)

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
    probe_ttl_geq: int,
    probe_ttl_leq: int,
    subset: IPNetwork,
    n_files: int,
) -> int:
    """
    Execute the :class:`diamond_miner.queries.GetNextRound` query
    on the specified subset, and write the probes to the specified file.
    """
    # TODO: random.shuffle is slow...
    # A potentially simpler and better way would be to shuffle
    # the result of the next round query, which is small (~10M rows),
    # and to directly write the probes to a file depending on hash(flow_id).
    # e.g. ORDER BY rand() in GetNextRound.

    # The larger `max_probes_in_memory`, the better the performance
    # (less calls to `probes.clear()`)
    # and the randomization but the more the memory usage.
    max_probes_in_memory = 1_000_000

    outputs: List[Tuple] = []
    for i in range(n_files):
        ctx = ZstdCompressor(level=1)
        file = prefix.with_suffix(f".{i}.csv.zst").open("wb")
        stream = ctx.stream_writer(file)
        outputs.append((ctx, file, stream))

    probes_by_file: List[List[Probe]] = [[] for _ in range(n_files)]
    n_probes = 0

    for probe in probe_generator_from_database(
        url=url,
        measurement_id=measurement_id,
        round_=round_,
        mapper_v4=mapper_v4,
        mapper_v6=mapper_v6,
        probe_src_port=probe_src_port,
        probe_dst_port=probe_dst_port,
        probe_ttl_geq=probe_ttl_geq,
        probe_ttl_leq=probe_ttl_leq,
        subsets=(subset,),
    ):
        # probe[:-2] => (probe_dst_addr, probe_src_port, probe_dst_port)
        probes_by_file[hash(probe[:-2]) % n_files].append(probe)
        n_probes += 1
        if n_probes % max_probes_in_memory == 0:
            flush(probes_by_file, outputs)

    flush(probes_by_file, outputs)

    for ctx, file, stream in outputs:
        stream.close()
        file.close()

    return n_probes


def flush(probes_by_file: List[List[Probe]], outputs: List[Tuple]) -> None:
    for file_id, probes in enumerate(probes_by_file):
        _, _, stream = outputs[file_id]
        random.shuffle(probes)
        for probe in probes:
            stream.write(format_probe(*probe).encode("ascii") + b"\n")
        probes.clear()
