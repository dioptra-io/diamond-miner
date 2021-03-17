from dataclasses import dataclass
from ipaddress import IPv4Network, IPv6Address, IPv6Network, ip_network
from typing import Union

from aioch import Client

CH_QUERY_SETTINGS = {
    "max_block_size": 100000,
    # Avoid timeout in case of a slow connection
    "connect_timeout": 1000,
    "send_timeout": 6000,
    "receive_timeout": 6000,
    # https://github.com/ClickHouse/ClickHouse/issues/18406
    "read_backoff_min_latency_ms": 100000,
}

IPNetwork = Union[IPv4Network, IPv6Network]


def addr_to_string(addr: IPv6Address):
    """
    >>> from ipaddress import ip_address
    >>> addr_to_string(ip_address('::dead:beef'))
    '::dead:beef'
    >>> addr_to_string(ip_address('::ffff:8.8.8.8'))
    '8.8.8.8'
    """
    return str(addr.ipv4_mapped or addr)


def ipv6(x):
    return f"toIPv6('{x}')"


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


@dataclass(frozen=True)
class Query:
    async def execute(self, *args, **kwargs):
        return [row async for row in self.execute_iter(*args, **kwargs)]

    async def execute_iter(
        self, client: Client, table: str, subsets=(ip_network("::/0"),)
    ):
        for subset in subsets:
            query = self.query(table, subset)
            rows = await client.execute_iter(query, settings=CH_QUERY_SETTINGS)
            async for row in rows:
                yield self.format(row)

    def format(self, row):
        return row

    def query(self, table: str, subset: IPNetwork):
        raise NotImplementedError
