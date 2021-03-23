from ipaddress import ip_address

import pytest

from diamond_miner.config import Config
from diamond_miner.mappers import (
    IntervalFlowMapper,
    RandomFlowMapper,
    ReverseByteFlowMapper,
    SequentialFlowMapper,
)
from diamond_miner.next_round import (
    compute_next_round,
    far_ttls_probes,
    next_round_probes,
)


async def collect(f):
    res = []
    async for xs in f:
        res.extend(xs)
    return res


@pytest.mark.asyncio
async def test_compute_next_round(client):
    table = "test_nsdi_lite"
    probe_dst_prefix = int(ip_address("::ffff:200.0.0.0"))

    config = Config(
        adaptive_eps=False, mapper=SequentialFlowMapper(), probe_src_addr="100.0.0.1"
    )

    # Round 1 -> 2, 5 probes at TTL 1-4
    probes = await collect(compute_next_round(config, client, table, 1))

    target_specs = []
    for ttl in range(1, 5):
        for flow_id in range(6, 6 + 5):
            target_specs.append(
                (
                    probe_dst_prefix + flow_id,
                    config.probe_src_port,
                    config.probe_dst_port,
                    ttl,
                )
            )

    assert sorted(probes) == sorted(target_specs)


@pytest.mark.asyncio
async def test_compute_next_round_mappers(client):
    table = "test_nsdi_lite"
    prefix_size = 2 ** (32 - 24)

    # In this test, we simplify verify that the next round works with
    # all the different flow mappers. We do not check the probes themselves.
    mappers = [
        IntervalFlowMapper(prefix_size=prefix_size),
        RandomFlowMapper(prefix_size=prefix_size, master_seed=2021),
        ReverseByteFlowMapper(),
        SequentialFlowMapper(),
    ]

    all_probes = []

    for mapper in mappers:
        config = Config(adaptive_eps=False, mapper=mapper, probe_src_addr="100.0.0.1")
        all_probes.append(await collect(compute_next_round(config, client, table, 1)))

    # Ensure that we get the same number of probes for every mapper.
    assert len(set(len(probes) for probes in all_probes)) == 1


@pytest.mark.asyncio
async def test_far_ttls_probes(client):
    table = "test_nsdi_lite"
    probe_dst_prefix = int(ip_address("::ffff:200.0.0.0"))

    config = Config(
        far_ttl_min=1,
        far_ttl_max=10,
        mapper=SequentialFlowMapper(),
        probe_src_addr="100.0.0.1",
    )

    probes = await collect(far_ttls_probes(config, client, table, 1))

    # This should generate probes beyond TTL 4, up to 10 (far_ttl_max),
    # for each flow ID previously used.
    target_specs = []
    for ttl in range(5, 11):
        for flow_id in range(0, 6):
            target_specs.append(
                (
                    probe_dst_prefix + flow_id,
                    config.probe_src_port,
                    config.probe_dst_port,
                    ttl,
                )
            )

    assert sorted(probes) == sorted(target_specs)


@pytest.mark.asyncio
async def test_next_round_probes_lite(client):
    table = "test_nsdi_lite"
    probe_dst_prefix = int(ip_address("::ffff:200.0.0.0"))

    config = Config(
        adaptive_eps=False, mapper=SequentialFlowMapper(), probe_src_addr="100.0.0.1"
    )

    async def probes_for_round(round_):
        return await collect(next_round_probes(config, client, table, round_, set()))

    # Round 1 -> 2, 5 probes at TTL 1-4
    target_specs = []
    for ttl in range(1, 5):
        for flow_id in range(6, 6 + 5):
            target_specs.append(
                (
                    probe_dst_prefix + flow_id,
                    config.probe_src_port,
                    config.probe_dst_port,
                    ttl,
                )
            )

    assert sorted(await probes_for_round(1)) == sorted(target_specs)

    # Round 2 -> 3, 5 probes at TTL 2-4
    target_specs = []
    for ttl in range(2, 5):
        for flow_id in range(11, 11 + 5):
            target_specs.append(
                (
                    probe_dst_prefix + flow_id,
                    config.probe_src_port,
                    config.probe_dst_port,
                    ttl,
                )
            )

    assert sorted(await probes_for_round(2)) == sorted(target_specs)

    # Round 3 -> 4, 0 probes
    assert await probes_for_round(3) == []


@pytest.mark.asyncio
async def test_next_round_probes_lite_adaptive(client):
    table = "test_nsdi_lite"

    config = Config(
        adaptive_eps=False, mapper=SequentialFlowMapper(), probe_src_addr="100.0.0.1"
    )

    async def probes_for_round(round_):
        return await collect(next_round_probes(config, client, table, round_, set()))

    # Simple test to make sure the query works.
    # TODO: Better adaptive eps test in the future.
    assert len(await probes_for_round(1)) >= 5
