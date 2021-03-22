from ipaddress import ip_address

from diamond_miner.utilities import format_ipv6, format_probe


def test_format_ipv6():
    assert (
        format_ipv6(int(ip_address("2001:4860:4860::8888")))
        == "2001:4860:4860:0:0:0:0:8888"
    )
    assert format_ipv6(2 ** 128 - 1) == "FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF"


def test_format_probe():
    assert (
        format_probe(2 ** 128 - 1, 2 ** 16 - 1, 2 ** 16 - 1, 2 ** 8 - 1)
        == "FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF,65535,65535,255"
    )
