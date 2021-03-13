from dataclasses import dataclass
from ipaddress import IPv4Network, IPv6Network, ip_network
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


def ipv6(x):
    return f"toIPv6('{x}')"


def ip_in(column: str, subset: Union[IPv4Network, IPv6Network]):
    return f"""
    ({column} >= {ipv6(subset[0])} AND {column} <= {ipv6(subset[-1])})
    """


def ip_not_in(column: str, subset: Union[IPv4Network, IPv6Network]):
    return f"""
    ({column} < {ipv6(subset[0])} OR {column} > {ipv6(subset[-1])})
    """


@dataclass(frozen=True)
class Query:
    async def execute(self, client: Client, table: str):
        query = self.query(table)
        rows = await client.execute_iter(query, settings=CH_QUERY_SETTINGS)
        return (self.format(row) async for row in rows)

    def format(self, row):
        return row

    def query(self, table: str):
        raise NotImplementedError


@dataclass(frozen=True)
class CountNodesPerTTL(Query):
    """
    Return the number of nodes discovered at each TTL.

    >>> from diamond_miner.test import execute
    >>> execute(CountNodesPerTTL('100.0.0.1'), 'test_nsdi_figure2')
    [(1, 1), (2, 2), (3, 3), (4, 1)]
    """

    source: str
    max_ttl: int = 255

    def query(self, table: str):
        return f"""
        SELECT probe_ttl_l3, uniqExact(reply_src_addr)
        FROM {table}
        WHERE probe_src_addr = {ipv6(self.source)}
        AND reply_src_addr != probe_dst_addr
        AND reply_icmp_type = 11
        AND probe_ttl_l3 <= {self.max_ttl}
        GROUP BY probe_ttl_l3
        """


@dataclass(frozen=True)
class GetNodes(Query):
    """
    Return all the discovered nodes.

    >>> from diamond_miner.test import execute
    >>> nodes = execute(GetNodes(), 'test_nsdi_figure2')
    >>> sorted(nodes)
    ['150.0.1.1', '150.0.2.1', '150.0.3.1', '150.0.4.1', '150.0.5.1', '150.0.6.1', '150.0.7.1'] # noqa
    """

    filter_private: bool = True

    def query(self, table: str):
        q = f"""
        SELECT DISTINCT reply_src_addr
        FROM {table}
        WHERE reply_src_addr != probe_dst_addr
        AND reply_icmp_type = 11
        """
        if self.filter_private:
            q += f"""
            AND {ip_not_in('reply_src_addr', ip_network('10.0.0.0/8'))}
            AND {ip_not_in('reply_src_addr', ip_network('172.16.0.0/12'))}
            AND {ip_not_in('reply_src_addr', ip_network('192.168.0.0/16'))}
            AND {ip_not_in('reply_src_addr', ip_network('fd00::/8'))}
            """
        return q

    def format(self, row):
        addr = row[0]
        addr = addr.ipv4_mapped or addr
        return str(addr)


@dataclass(frozen=True)
class GetResolvedPrefixes(Query):
    """
    Return the prefixes for which no replies have been received at the previous round.

    >>> from diamond_miner.test import execute
    >>> table = 'test_nsdi_figure2'
    >>> execute(GetResolvedPrefixes('100.0.0.1', 1), table)
    []
    >>> execute(GetResolvedPrefixes('100.0.0.1', 4), table)
    ['::ffff:200.0.0.0']
    """

    source: str
    round: int
    subset: IPv6Network = ip_network("::/0")

    def query(self, table: str):
        return f"""
        WITH cutIPv6(probe_dst_addr, 8, 1) as probe_dst_prefix
        SELECT DISTINCT probe_dst_prefix
        FROM {table}
        WHERE {ip_in('probe_dst_prefix', self.subset)}
        AND probe_src_addr = {ipv6(self.source)}
        GROUP BY (probe_src_addr, probe_dst_prefix)
        HAVING MAX(round) < {self.round - 1}
        """

    def format(self, row):
        return row[0]


@dataclass(frozen=True)
class GetInvalidPrefixes(Query):
    """
    Return the prefixes with per-packet LB or that sends more replies than probes.
    >>> from diamond_miner.test import execute
    >>> execute(GetInvalidPrefixes('100.0.0.1'), 'test_nsdi_figure2')
    []
    >>> execute(GetInvalidPrefixes('100.0.0.1'), 'test_invalid_prefixes')
    ['::ffff:201.0.0.0', '::ffff:202.0.0.0']
    """

    source: str
    subset: IPv6Network = ip_network("::/0")

    def query(self, table: str):
        return f"""
        WITH cutIPv6(probe_dst_addr, 8, 1) AS probe_dst_prefix,
             count(reply_src_addr)         AS n_replies,
             uniqExact(reply_src_addr)     AS n_distinct_replies
        SELECT DISTINCT probe_dst_prefix
        FROM {table}
        WHERE {ip_in('probe_dst_prefix', self.subset)}
        AND probe_src_addr = {ipv6(self.source)}
        GROUP BY (
            probe_src_addr,
            probe_dst_addr,
            probe_src_port,
            probe_dst_port,
            probe_ttl_l3
        )
        HAVING (n_replies > 2) OR (n_distinct_replies > 1)
        """

    def format(self, row):
        return row[0]
