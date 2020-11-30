import pytest
from ipaddress import ip_address

from diamond_miner_core.flow import SequentialFlowMapper
from diamond_miner_core.rounds import exhaustive_round, probe_to_csv, targets_round


def test_probe_to_csv():
    dst_addr = int(ip_address("8.8.0.0"))
    row = probe_to_csv(dst_addr, 24, 3, 31, human=True)
    assert row == "008.008.000.000,00024,00003,031"


@pytest.mark.asyncio
async def test_exhaustive_round():
    mapper = SequentialFlowMapper()
    # Too long to test the full round round, so we juste do a simple test.
    probes = exhaustive_round(mapper, src_port=24000, dst_port=33434)
    for _ in range(1000):
        probe = await probes.__anext__()
        assert 0 <= probe[0] <= (2 ** 32 - 1)
        assert probe[1] == 24000
        assert probe[2] == 33434
        assert 1 <= probe[3] <= 32

@pytest.mark.asyncio
async def test_targets_round():
    targets = ["8.8.8.8", "8.8.9.9"]
    probes = [x async for x in targets_round(targets, src_port=24000, dst_port=33434)]
    assert len(probes) == 2 * 32
    for probe in probes:
        assert 0 <= probe[0] <= (2 ** 32 - 1)
        assert probe[1] == 24000
        assert probe[2] == 33434
        assert 1 <= probe[3] <= 32
