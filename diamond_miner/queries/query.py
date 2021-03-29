from dataclasses import dataclass
from datetime import datetime
from ipaddress import IPv6Address
from typing import AsyncIterator, Iterable, List, Optional

from aioch import Client

from diamond_miner.defaults import DEFAULT_SUBSET
from diamond_miner.logging import logger
from diamond_miner.queries.fragments import IPNetwork, eq, in_, ip_eq, ip_in, leq

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
    filter_destination: bool = True
    "If true, ignore the replies from the destination."

    filter_private: bool = True
    "If true, ignore the replies from private IP addresses."

    probe_src_addr: Optional[str] = None
    "If specified, keep only the replies to probes sent by this address."

    reply_icmp_type: Iterable[int] = (3, 11)
    "If specified, keep only these types of ICMP replies."

    round_eq: Optional[int] = None
    "If specified, keep only the replies from this round."

    round_leq: Optional[int] = None
    "If specified, keep only the replies from this round or before."

    async def execute(self, *args, **kwargs) -> List:
        return [row async for row in self.execute_iter(*args, **kwargs)]

    async def execute_iter(
        self, client: Client, table: str, subsets=(DEFAULT_SUBSET,)
    ) -> AsyncIterator:
        for subset in subsets:
            query = self.query(table, subset)
            start = datetime.now()
            logger.info("query=%s table=%s subset=%s", self.name, table, subset)
            rows = await client.execute_iter(query, settings=CH_QUERY_SETTINGS)
            async for row in rows:
                yield row
            delta = datetime.now() - start
            logger.info(
                "query=%s table=%s subset=%s time=%s", self.name, table, subset, delta
            )

    def common_filters(self, subset: IPNetwork) -> str:
        """`WHERE` clause common to all queries."""
        s = f"""
        {ip_in('probe_dst_prefix', subset)}
        AND {ip_eq('probe_src_addr', self.probe_src_addr)}
        AND {in_('reply_icmp_type', self.reply_icmp_type)}
        AND {eq('round', self.round_eq)}
        AND {leq('round', self.round_leq)}
        """
        if self.filter_destination:
            s += "\nAND reply_src_addr != probe_dst_addr"
        if self.filter_private:
            s += "\nAND private_reply_src_addr = 0"
        return s

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def query(self, table: str, subset: IPNetwork = DEFAULT_SUBSET) -> str:
        raise NotImplementedError
