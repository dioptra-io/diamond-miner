import logging
from dataclasses import dataclass, field
from datetime import datetime
from ipaddress import IPv6Address
from typing import List, Optional

from aioch import Client

from diamond_miner.defaults import (
    DEFAULT_PREFIX_LEN_V4,
    DEFAULT_PREFIX_LEN_V6,
    DEFAULT_SUBSET,
)
from diamond_miner.queries.fragments import (
    IPNetwork,
    cut_ipv6,
    eq,
    in_,
    ip_eq,
    ip_in,
    ip_not_private,
    leq,
)

CH_QUERY_SETTINGS = {
    "max_block_size": 100000,
    # Avoid timeout in case of a slow connection
    "connect_timeout": 1000,
    "send_timeout": 6000,
    "receive_timeout": 6000,
    # https://github.com/ClickHouse/ClickHouse/issues/18406
    "read_backoff_min_latency_ms": 100000,
}


def addr_to_string(addr: IPv6Address):
    """
    >>> from ipaddress import ip_address
    >>> addr_to_string(ip_address('::dead:beef'))
    '::dead:beef'
    >>> addr_to_string(ip_address('::ffff:8.8.8.8'))
    '8.8.8.8'
    """
    return str(addr.ipv4_mapped or addr)


@dataclass(frozen=True)
class Query:
    # Properties common to all queries.

    filter_destination: bool = True
    filter_private: bool = True
    prefix_len_v4: int = DEFAULT_PREFIX_LEN_V4
    prefix_len_v6: int = DEFAULT_PREFIX_LEN_V6
    probe_src_addr: Optional[str] = None
    reply_icmp_type: List[int] = field(default_factory=list)
    round_eq: Optional[int] = None
    round_leq: Optional[int] = None

    async def execute(self, *args, **kwargs):
        return [row async for row in self.execute_iter(*args, **kwargs)]

    async def execute_iter(self, client: Client, table: str, subsets=(DEFAULT_SUBSET,)):
        for subset in subsets:
            query = self.query(table, subset)
            query_start = datetime.now()
            logging.info(
                "query=%s table=%s subset=%s", self.__class__.__name__, table, subset
            )
            rows = await client.execute_iter(query, settings=CH_QUERY_SETTINGS)
            async for row in rows:
                yield row
            query_time = datetime.now() - query_start
            logging.info(
                "query=%s table=%s subset=%s time=%s",
                self.__class__.__name__,
                table,
                subset,
                query_time,
            )

    def common_filters(self, subset: IPNetwork):
        s = f"""
        {ip_in('probe_dst_prefix', subset)}
        AND {ip_eq('probe_src_addr', self.probe_src_addr)}
        AND {in_('reply_icmp_type', self.reply_icmp_type)}
        AND {eq('round', self.round_eq)}
        AND {leq('round', self.round_leq)}
        """
        if self.filter_private:
            s += f"\nAND {ip_not_private('reply_src_addr')}"
        return s

    def probe_dst_prefix(self):
        return f"{cut_ipv6('probe_dst_addr', self.prefix_len_v4, self.prefix_len_v6)}"

    def query(self, table: str, subset: IPNetwork = DEFAULT_SUBSET) -> str:
        raise NotImplementedError
