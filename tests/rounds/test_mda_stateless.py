from ipaddress import ip_address

from diamond_miner.defaults import DEFAULT_PREFIX_SIZE_V4, DEFAULT_PREFIX_SIZE_V6
from diamond_miner.mappers import (
    IntervalFlowMapper,
    RandomFlowMapper,
    ReverseByteFlowMapper,
    SequentialFlowMapper,
)
from diamond_miner.rounds.mda_stateless import mda_probes


def test_mda_probes_lite(url):
    table = "test_nsdi_lite"
    probe_dst_prefix = int(ip_address("::ffff:200.0.0.0"))

    probe_src_port = 24000
    probe_dst_port = 33434

    def probes_for_round(round_):
        return list(
            mda_probes(
                url=url,
                measurement_id=table,
                round_=round_,
                mapper_v4=SequentialFlowMapper(prefix_size=DEFAULT_PREFIX_SIZE_V4),
                mapper_v6=SequentialFlowMapper(prefix_size=DEFAULT_PREFIX_SIZE_V6),
                probe_src_port=probe_src_port,
                probe_dst_port=probe_dst_port,
                adaptive_eps=False,
            )
        )

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


def test_mda_probes_lite_adaptive(url):
    table = "test_nsdi_lite"

    def probes_for_round(round_):
        return list(
            mda_probes(
                url=url,
                measurement_id=table,
                round_=round_,
                mapper_v4=SequentialFlowMapper(prefix_size=DEFAULT_PREFIX_SIZE_V4),
                mapper_v6=SequentialFlowMapper(prefix_size=DEFAULT_PREFIX_SIZE_V6),
                adaptive_eps=True,
            )
        )

    # Simple test to make sure the query works.
    # TODO: Better adaptive eps test in the future.
    assert len(probes_for_round(1)) > 20


def test_mda_probes_lite_mappers(url):
    table = "test_nsdi_lite"

    # In this test, we simplify verify that the next round works with
    # all the different flow mappers. We do not check the probes themselves.
    mappers_v4 = [
        IntervalFlowMapper(prefix_size=DEFAULT_PREFIX_SIZE_V4),
        RandomFlowMapper(prefix_size=DEFAULT_PREFIX_SIZE_V4, seed=2021),
        ReverseByteFlowMapper(),
        SequentialFlowMapper(prefix_size=DEFAULT_PREFIX_SIZE_V4),
    ]

    mappers_v6 = [
        IntervalFlowMapper(prefix_size=DEFAULT_PREFIX_SIZE_V6),
        RandomFlowMapper(prefix_size=DEFAULT_PREFIX_SIZE_V6, seed=2021),
        ReverseByteFlowMapper(),
        SequentialFlowMapper(prefix_size=DEFAULT_PREFIX_SIZE_V6),
    ]

    all_probes = []

    for mapper_v4, mapper_v6 in zip(mappers_v4, mappers_v6):
        all_probes.append(
            list(
                mda_probes(
                    url=url,
                    measurement_id=table,
                    round_=1,
                    mapper_v4=mapper_v4,
                    mapper_v6=mapper_v6,
                )
            )
        )

    # Ensure that we get the same number of probes for every mapper.
    assert len(set(len(probes) for probes in all_probes)) == 1


def test_next_round_probes_multi_protocol(url):
    table = "test_multi_protocol"
    probe_dst_prefix = int(ip_address("::ffff:200.0.0.0"))

    probe_src_port = 24000
    probe_dst_port = 33434

    def probes_for_round(round_):
        return list(
            mda_probes(
                url=url,
                measurement_id=table,
                round_=round_,
                mapper_v4=SequentialFlowMapper(prefix_size=DEFAULT_PREFIX_SIZE_V4),
                mapper_v6=SequentialFlowMapper(prefix_size=DEFAULT_PREFIX_SIZE_V6),
                probe_src_port=probe_src_port,
                probe_dst_port=probe_dst_port,
                adaptive_eps=False,
            )
        )

    # Round 1 -> 2, 5 probes at TTL 1-2 only for ICMP
    # TODO: Better test/test table
    target_specs = []
    for ttl in range(1, 3):
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
