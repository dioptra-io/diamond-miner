from diamond_miner.generator import probe_generator, probe_generator_by_flow
from diamond_miner.mappers import SequentialFlowMapper
from diamond_miner.utilities import format_ipv6


def test_probe_generator_128():
    prefixes = ["2001:4860:4860::8888/128\n"]
    generator = probe_generator(
        prefixes=prefixes,
        prefix_len_v6=128,
        flow_ids=[10, 11, 12],
        ttls=[41, 42],
        mapper_v6=SequentialFlowMapper(prefix_size=1),
    )
    probes = [x for x in generator]
    assert len(probes) == len(set(probes)) == 6
    for addr, src_port, dst_port, ttl, protocol in probes:
        assert format_ipv6(addr) == "2001:4860:4860:0:0:0:0:8888"
        assert src_port in range(24010, 24013)
        assert dst_port == 33434
        assert ttl in range(41, 43)
        assert protocol == "icmp"


def test_probe_generator_63():
    prefixes = ["2001:4860:4860:0000::/63\n"]
    generator = probe_generator(
        prefixes=prefixes,
        prefix_len_v6=64,
        flow_ids=[10],
        ttls=[41],
        mapper_v6=SequentialFlowMapper(prefix_size=2 ** 64),
    )
    probes = [x for x in generator]
    assert len(probes) == len(set(probes)) == 2
    for addr, src_port, dst_port, ttl, protocol in probes:
        assert format_ipv6(addr) in [
            "2001:4860:4860:0:0:0:0:A",
            "2001:4860:4860:1:0:0:0:A",
        ]
        assert src_port == 24000
        assert dst_port == 33434
        assert ttl == 41
        assert protocol == "icmp"


def test_probe_generator_32():
    prefixes = ["8.8.8.8/32\n"]
    generator = probe_generator(
        prefixes=prefixes,
        prefix_len_v4=32,
        flow_ids=[10, 11, 12],
        ttls=[41, 42],
        mapper_v4=SequentialFlowMapper(prefix_size=1),
    )
    probes = [x for x in generator]
    assert len(probes) == len(set(probes)) == 6
    for addr, src_port, dst_port, ttl, protocol in probes:
        assert format_ipv6(addr) == "0:0:0:0:0:FFFF:808:808"
        assert src_port in range(24010, 24013)
        assert dst_port == 33434
        assert ttl in range(41, 43)
        assert protocol == "icmp"


def test_probe_generator_23():
    prefixes = ["0.0.0.0/23"]
    generator = probe_generator(
        prefixes=prefixes,
        prefix_len_v4=24,
        flow_ids=[10],
        ttls=[41],
        mapper_v4=SequentialFlowMapper(prefix_size=2 ** 8),
    )
    probes = [x for x in generator]
    assert len(probes) == len(set(probes)) == 2
    for addr, src_port, dst_port, ttl, protocol in probes:
        assert format_ipv6(addr) in [
            "0:0:0:0:0:FFFF:0:A",
            "0:0:0:0:0:FFFF:0:10A",
        ]
        assert src_port == 24000
        assert dst_port == 33434
        assert ttl == 41
        assert protocol == "icmp"


# TODO: Better test, with hypothesis? Or list probes exhaustively...
def test_probe_generator_by_flow():
    prefixes = [("0.0.0.0/23", [10, 20, 30]), ("2001:4860:4860::8888/128", range(2))]
    generator = probe_generator_by_flow(
        prefixes,
        prefix_len_v4=24,
        prefix_len_v6=128,
        flow_ids=[10, 11],
        mapper_v4=SequentialFlowMapper(prefix_size=2 ** 8),
        mapper_v6=SequentialFlowMapper(prefix_size=1),
    )
    probes = [x for x in generator]
    assert len(probes) == len(set(probes)) == 16
    for addr, src_port, dst_port, ttl, protocol in probes:
        assert format_ipv6(addr) in [
            "0:0:0:0:0:FFFF:0:A",
            "0:0:0:0:0:FFFF:0:B",
            "0:0:0:0:0:FFFF:0:10A",
            "0:0:0:0:0:FFFF:0:10B",
            "2001:4860:4860:0:0:0:0:8888",
        ]
        assert src_port in [24000, 24010, 24011]
        assert dst_port == 33434
        assert ttl in [0, 1, 10, 20, 30]
        assert protocol == "icmp"
