from collections.abc import Iterable
from datetime import datetime
from typing import Any

from diamond_miner.typing import IPNetwork


def cut_ipv6(column: str, prefix_len_v4: int, prefix_len_v6: int) -> str:
    """
    >>> cut_ipv6("col", 24, 64)
    'toIPv6(cutIPv6(col, 8, 1))'
    """
    bytes_v4 = int(4 - (prefix_len_v4 / 8))
    bytes_v6 = int(16 - (prefix_len_v6 / 8))
    return f"toIPv6(cutIPv6({column}, {bytes_v6}, {bytes_v4}))"


def eq(column: str, value: Any | None) -> str:
    """
    >>> eq("col", None)
    '1'
    >>> eq("col", 1)
    'col = 1'
    >>> eq("col", "1")
    "col = '1'"
    """
    if not value:
        return "1"
    if isinstance(value, str):
        return f"{column} = '{value}'"
    return f"{column} = {value}"


def geq(column: str, value: Any | None) -> str:
    """
    >>> geq("col", None)
    '1'
    >>> geq("col", 1)
    'col >= 1'
    >>> geq("col", "1")
    "col >= '1'"
    """
    if not value:
        return "1"
    if isinstance(value, str):
        return f"{column} >= '{value}'"
    return f"{column} >= {value}"


def lt(column: str, value: Any | None) -> str:
    """
    >>> lt("col", None)
    '1'
    >>> lt("col", 1)
    'col < 1'
    >>> lt("col", "1")
    "col < '1'"
    """
    if not value:
        return "1"
    if isinstance(value, str):
        return f"{column} < '{value}'"
    return f"{column} < {value}"


def leq(column: str, value: Any | None) -> str:
    """
    >>> leq("col", None)
    '1'
    >>> leq("col", 1)
    'col <= 1'
    >>> leq("col", "1")
    "col <= '1'"
    """
    if not value:
        return "1"
    if isinstance(value, str):
        return f"{column} <= '{value}'"
    return f"{column} <= {value}"


def not_(column: str) -> str:
    """
    >>> not_("col")
    'NOT col'
    """
    return f"NOT {column}"


def in_(column: str, values: Iterable[Any]) -> str:
    """
    >>> in_("col", [])
    '1'
    >>> in_("col", [1,2,3])
    'col in [1,2,3]'
    """
    if not values:
        return "1"
    return f"{column} in [{','.join(map(str, values))}]"


def date_time(d: datetime) -> str:
    """
    >>> from datetime import timezone
    >>> date_time(datetime(2021,10,12,10,57,30, tzinfo=timezone.utc))
    "parseDateTimeBestEffort('1634036250')"
    """
    return f"parseDateTimeBestEffort('{int(d.timestamp())}')"


def ipv6(x: Any) -> str:
    """
    >>> ipv6("8.8.8.8")
    "toIPv6('8.8.8.8')"
    """
    return f"toIPv6('{x}')"


def ip_eq(column: str, value: str | None) -> str:
    """
    >>> ip_eq("col", None)
    '1'
    >>> ip_eq("col", "8.8.8.8")
    "col = toIPv6('8.8.8.8')"
    """
    if not value:
        return "1"
    return f"{column} = {ipv6(value)}"


def ip_in(column: str, subset: IPNetwork | None) -> str:
    """
    >>> from ipaddress import ip_network
    >>> ip_in("col", None)
    '1'
    >>> ip_in("col", ip_network("::/0"))
    "(col >= toIPv6('::') AND col <= toIPv6('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'))"
    """
    if not subset:
        return "1"
    return f"({column} >= {ipv6(subset[0])} AND {column} <= {ipv6(subset[-1])})"


def ip_not_in(column: str, subset: IPNetwork | None) -> str:
    """
    >>> from ipaddress import ip_network
    >>> ip_not_in("col", None)
    '1'
    >>> ip_not_in("col", ip_network("::/0"))
    "(col < toIPv6('::') OR col > toIPv6('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'))"
    """
    if not subset:
        return "1"
    return f"({column} < {ipv6(subset[0])} OR {column} > {ipv6(subset[-1])})"


def and_(a: str, b: str) -> str:
    """
    >>> and_("0", "1")
    '(0 AND 1)'
    """
    return f"({a} AND {b})"


def or_(a: str, b: str) -> str:
    """
    >>> or_("0", "1")
    '(0 OR 1)'
    """
    return f"({a} OR {b})"
