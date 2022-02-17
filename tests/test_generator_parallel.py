from io import TextIOWrapper
from ipaddress import ip_address

from zstandard import ZstdDecompressor

from diamond_miner.defaults import DEFAULT_PREFIX_SIZE_V4, DEFAULT_PREFIX_SIZE_V6
from diamond_miner.generators import probe_generator_parallel
from diamond_miner.insert import insert_mda_probe_counts
from diamond_miner.mappers import SequentialFlowMapper
from diamond_miner.queries.delete_probes import DeleteProbes
from diamond_miner.test import client


def test_mda_probes_parallel(tmp_path):
    measurement_id = "test_nsdi_lite"
    probe_dst_prefix = int(ip_address("::ffff:200.0.0.0"))
    probe_src_port = 24000
    probe_dst_port = 33434

    def probes_for_round(round_):
        filepath = tmp_path / f"probes-{round_}.csv.zst"
        DeleteProbes(round_eq=round_ + 1).execute(client, measurement_id)
        insert_mda_probe_counts(
            client=client,
            measurement_id=measurement_id,
            previous_round=round_,
            adaptive_eps=False,
        )
        n_probes = probe_generator_parallel(
            filepath=filepath,
            client=client,
            measurement_id=measurement_id,
            round_=round_ + 1,
            mapper_v4=SequentialFlowMapper(prefix_size=DEFAULT_PREFIX_SIZE_V4),
            mapper_v6=SequentialFlowMapper(prefix_size=DEFAULT_PREFIX_SIZE_V6),
            probe_src_port=probe_src_port,
            probe_dst_port=probe_dst_port,
            probe_ttl_geq=1,
            probe_ttl_leq=32,
            n_workers=4,
        )
        probes = []
        with filepath.open("rb") as f:
            reader = ZstdDecompressor().stream_reader(f)
            text = TextIOWrapper(reader, encoding="utf-8")
            for line in text:
                dst_addr, src_port, dst_port, ttl, protocol = line.strip().split(",")
                probes.append(
                    (
                        int(ip_address(dst_addr)),
                        int(src_port),
                        int(dst_port),
                        int(ttl),
                        protocol,
                    )
                )
        assert n_probes == len(probes)
        return probes

    # Round 1 -> 2, 5 probes at TTL 1-4
    target_specs = []
    for ttl in range(1, 5):
        for flow_id in range(6, 6 + 5):
            target_specs.append(
                (
                    probe_dst_prefix + flow_id,
                    probe_src_port,
                    probe_dst_port,
                    ttl,
                    "icmp",
                )
            )

    assert sorted(probes_for_round(1)) == sorted(target_specs)

    # Round 2 -> 3, 5 probes at TTL 2-4
    target_specs = []
    for ttl in range(2, 5):
        for flow_id in range(11, 11 + 5):
            target_specs.append(
                (
                    probe_dst_prefix + flow_id,
                    probe_src_port,
                    probe_dst_port,
                    ttl,
                    "icmp",
                )
            )

    assert sorted(probes_for_round(2)) == sorted(target_specs)

    # Round 3 -> 4, 0 probes
    assert probes_for_round(3) == []
