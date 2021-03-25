from ipaddress import IPv4Network, IPv6Network, ip_network
from typing import Any, Iterable, Optional, Union

IPNetwork = Union[IPv4Network, IPv6Network]


def cut_ipv6(column: str, prefix_len_v4: int, prefix_len_v6: int):
    bytes_v4 = int(4 - (prefix_len_v4 / 8))
    bytes_v6 = int(16 - (prefix_len_v6 / 8))
    return f"toIPv6(cutIPv6({column}, {bytes_v6}, {bytes_v4}))"


def eq(column: str, value: Optional[Any]):
    if not value:
        return "1"
    if isinstance(value, str):
        return f"{column} = '{value}'"
    return f"{column} = {value}"


def leq(column: str, value: Optional[Any]):
    if not value:
        return "1"
    if isinstance(value, str):
        return f"{column} <= '{value}'"
    return f"{column} <= {value}"


def in_(column: str, values: Iterable[Any]):
    if not values:
        return "1"
    return f"{column} in [{','.join(map(str, values))}]"


def ipv6(x):
    return f"toIPv6('{x}')"


def ip_eq(column: str, value: Optional[str]):
    if not value:
        return "1"
    return f"{column} = {ipv6(value)}"


def ip_in(column: str, subset: IPNetwork):
    return f"""
    ({column} >= {ipv6(subset[0])} AND {column} <= {ipv6(subset[-1])})
    """


def ip_not_in(column: str, subset: IPNetwork):
    return f"""
    ({column} < {ipv6(subset[0])} OR {column} > {ipv6(subset[-1])})
    """


def ip_not_private(column: str):
    return f"""
    {ip_not_in(column, ip_network('10.0.0.0/8'))}
    AND {ip_not_in(column, ip_network('172.16.0.0/12'))}
    AND {ip_not_in(column, ip_network('192.168.0.0/16'))}
    AND {ip_not_in(column, ip_network('fd00::/8'))}
    """
