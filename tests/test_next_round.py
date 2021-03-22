from ipaddress import ip_address

import pytest

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
    src_addr = "100.0.0.1"
    dst_prefix = int(ip_address("200.0.0.0"))
    src_port = 24000
    dst_port = 33434
    mapper = SequentialFlowMapper()

    # Round 1 -> 2, 5 probes at TTL 1-4
    probes = await collect(
        compute_next_round(
            client,
            table,
            1,
            src_addr,
            src_port,
            dst_port,
            mapper,
            adaptive_eps=False,
        )
    )

    target_specs = []
    for ttl in range(1, 5):
        for flow_id in range(6, 6 + 5):
            target_specs.append(
                (str(dst_prefix + flow_id), str(src_port), str(dst_port), str(ttl))
            )

    assert sorted(probes) == sorted(target_specs)


@pytest.mark.asyncio
async def test_compute_next_round_mappers(client):
    table = "test_nsdi_lite"
    src_addr = "100.0.0.1"
    src_port = 24000
    dst_port = 33434

    # In this test, we simplify verify that the next round works with
    # all the different flow mappers. We do not check the probes themselves.
    mappers = [
        IntervalFlowMapper(),
        RandomFlowMapper(master_seed=2021),
        ReverseByteFlowMapper(),
        SequentialFlowMapper,
    ]

    all_probes = []

    for mapper in mappers:
        all_probes.append(
            await collect(
                compute_next_round(
                    client,
                    table,
                    1,
                    src_addr,
                    src_port,
                    dst_port,
                    mapper,
                    adaptive_eps=False,
                )
            )
        )

    # Ensure that we get the same number of probes for every mapper.
    assert len(set(len(probes) for probes in all_probes)) == 1


@pytest.mark.asyncio
async def test_far_ttls_probes(client):
    table = "test_nsdi_lite"
    src_addr = "100.0.0.1"
    dst_prefix = int(ip_address("200.0.0.0"))
    src_port = 24000
    dst_port = 33434
    far_ttl_min = 1
    far_ttl_max = 10
    round_ = 1

    probes = await collect(
        far_ttls_probes(
            client,
            table,
            round_,
            src_addr,
            src_port,
            dst_port,
            far_ttl_min=far_ttl_min,
            far_ttl_max=far_ttl_max,
        )
    )

    # This should generate probes beyond TTL 4, up to 10 (far_ttl_max),
    # for each flow ID previously used.
    target_specs = []
    for ttl in range(5, 11):
        for flow_id in range(0, 6):
            target_specs.append(
                (str(dst_prefix + flow_id), str(src_port), str(dst_port), str(ttl))
            )

    assert sorted(probes) == sorted(target_specs)


@pytest.mark.asyncio
async def test_next_round_probes_lite(client):
    table = "test_nsdi_lite"
    src_addr = "100.0.0.1"
    dst_prefix = int(ip_address("200.0.0.0"))
    src_port = 24000
    dst_port = 33434
    mapper = SequentialFlowMapper()

    async def probes_for_round(round_):
        return await collect(
            next_round_probes(
                client,
                table,
                round_,
                src_addr,
                src_port,
                dst_port,
                mapper,
                set(),
                adaptive_eps=False,
            )
        )

    # Round 1 -> 2, 5 probes at TTL 1-4
    target_specs = []
    for ttl in range(1, 5):
        for flow_id in range(6, 6 + 5):
            target_specs.append(
                (str(dst_prefix + flow_id), str(src_port), str(dst_port), str(ttl))
            )

    assert sorted(await probes_for_round(1)) == sorted(target_specs)

    # Round 2 -> 3, 5 probes at TTL 2-4
    target_specs = []
    for ttl in range(2, 5):
        for flow_id in range(11, 11 + 5):
            target_specs.append(
                (str(dst_prefix + flow_id), str(src_port), str(dst_port), str(ttl))
            )

    assert sorted(await probes_for_round(2)) == sorted(target_specs)

    # Round 3 -> 4, 0 probes
    assert await probes_for_round(3) == []


@pytest.mark.asyncio
async def test_next_round_probes_lite_adaptive(client):
    table = "test_nsdi_lite"
    src_addr = "100.0.0.1"
    src_port = 24000
    dst_port = 33434
    mapper = SequentialFlowMapper()

    async def probes_for_round(round_):
        return await collect(
            next_round_probes(
                client,
                table,
                round_,
                src_addr,
                src_port,
                dst_port,
                mapper,
                set(),
                adaptive_eps=True,
            )
        )

    # Simple test to make sure the query works.
    # TODO: Better adaptive eps test in the future.
    assert len(await probes_for_round(1)) >= 5
