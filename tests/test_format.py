from ipaddress import ip_address

from hypothesis import given
from hypothesis.strategies import ip_addresses

from diamond_miner.format import format_ipv6, format_probe


@given(ip_addresses(v=6))
def test_format_ipv6(addr):
    assert ip_address(format_ipv6(int(addr))) == addr


def test_format_probe():
    assert format_probe(0, 0, 0, 0, "icmp") == "0:0:0:0:0:0:0:0,0,0,0,icmp"
    assert (
        format_probe(2 ** 128 - 1, 2 ** 16 - 1, 2 ** 16 - 1, 2 ** 8 - 1, "udp")
        == "FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF,65535,65535,255,udp"
    )
