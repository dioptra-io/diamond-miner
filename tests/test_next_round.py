from ipaddress import ip_address

import pytest

from diamond_miner.mappers import SequentialFlowMapper
from diamond_miner.next_round import next_round_probes


async def collect(f):
    res = []
    async for xs in f:
        res.extend(xs)
    return res


@pytest.mark.asyncio
async def test_next_round_probes_nsdi_lite(client):
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
