from ipaddress import IPv6Address


def format_probe(
    dst_addr_v6: int, src_port: int, dst_port: int, ttl: int, protocol: str
) -> str:
    """
    Create a Caracal probe string.
    Examples:
        >>> from diamond_miner.format import format_probe
        >>> format_probe(281470816487432, 24000, 33434, 1, "icmp")
        '::ffff:8.8.8.8,24000,33434,1,icmp'
    """
    return f"{format_ipv6(dst_addr_v6)},{src_port},{dst_port},{ttl},{protocol}"


def format_ipv6(addr: int) -> str:
    """
    Convert an IPv6 UInt128 to a string.
        >>> from diamond_miner.format import format_ipv6
        >>> format_ipv6(281470816487432)
        '::ffff:8.8.8.8'
    """
    return str(IPv6Address(addr))
