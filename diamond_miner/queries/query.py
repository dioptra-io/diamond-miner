from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from ipaddress import IPv6Address
from typing import AsyncIterator, Iterable, Iterator, List, Optional

from aioch import Client as AsyncClient
from clickhouse_driver import Client

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.logging import logger
from diamond_miner.queries.fragments import eq, ip_eq, ip_in, leq
from diamond_miner.typing import IPNetwork

CH_QUERY_SETTINGS = {
    "max_block_size": 100000,
    # Avoid timeout in case of a slow connection
    "connect_timeout": 1000,
    "send_timeout": 6000,
    "receive_timeout": 6000,
    # https://github.com/ClickHouse/ClickHouse/issues/18406
    "read_backoff_min_latency_ms": 100000,
}


class AddrType(str, Enum):
    IPv6 = "IPv6"
    String = "String"
    FixedString = "FixedString"
    IPv6NumToString = "IPv6NumToString"


def addr_to_string(addr: IPv6Address) -> str:
    """
    >>> from ipaddress import ip_address
    >>> addr_to_string(ip_address('::dead:beef'))
    '::dead:beef'
    >>> addr_to_string(ip_address('::ffff:8.8.8.8'))
    '8.8.8.8'
    """
    return str(addr.ipv4_mapped or addr)


def bytes_to_addr(b: bytes) -> IPv6Address:
    return IPv6Address(bytes_to_addr_int(b))


def bytes_to_addr_int(b: bytes) -> int:
    return int.from_bytes(b, "big")


def bytes_to_addr_str(b: bytes) -> str:
    return addr_to_string(bytes_to_addr(b))


@dataclass(frozen=True)
class Query:
    filter_destination: bool = True
    "If true, ignore the replies from the destination."

    filter_private: bool = True
    "If true, ignore the replies from private IP addresses."

    filter_invalid_probe_protocol: bool = True
    "If true, ignore the replies with probe protocol ≠ ICMP, ICMPv6 or UDP."

    time_exceeded_only: bool = True
    "If true, ignore non ICMP time exceeded replies."

    probe_src_addr: Optional[str] = None
    """
    If specified, keep only the replies to probes sent by this address.
    This filter is relatively costly (IPv6 comparison on each row).
    """

    round_eq: Optional[int] = None
    "If specified, keep only the replies from this round."

    round_leq: Optional[int] = None
    "If specified, keep only the replies from this round or before."

    addr_type: AddrType = AddrType.IPv6

    def execute(
        self,
        client: Client,
        table: str,
        subsets: Iterable[IPNetwork] = (UNIVERSE_SUBSET,),
    ) -> List:
        return [row for row in self.execute_iter(client, table, subsets)]

    async def execute_async(
        self,
        client: AsyncClient,
        table: str,
        subsets: Iterable[IPNetwork] = (UNIVERSE_SUBSET,),
    ) -> List:
        return [row async for row in self.execute_iter_async(client, table, subsets)]

    def execute_iter(
        self,
        client: Client,
        table: str,
        subsets: Iterable[IPNetwork] = (UNIVERSE_SUBSET,),
    ) -> Iterator:
        for subset in subsets:
            query = self.query(table, subset)
            with self._log_time(table, subset):
                rows = client.execute_iter(query, settings=CH_QUERY_SETTINGS)
                for row in rows:
                    yield row

    async def execute_iter_async(
        self,
        client: AsyncClient,
        table: str,
        subsets: Iterable[IPNetwork] = (UNIVERSE_SUBSET,),
    ) -> AsyncIterator:
        for subset in subsets:
            query = self.query(table, subset)
            with self._log_time(table, subset):
                rows = await client.execute_iter(query, settings=CH_QUERY_SETTINGS)
                async for row in rows:
                    yield row

    def common_filters(self, subset: IPNetwork) -> str:
        """``WHERE`` clause common to all queries on the results table."""
        s = "1"
        if subset != UNIVERSE_SUBSET:
            s += f"\n{ip_in('probe_dst_prefix', subset)}"
        if self.probe_src_addr:
            s += f"\nAND {ip_eq('probe_src_addr', self.probe_src_addr)}"
        if self.round_eq:
            s += f"\nAND {eq('round', self.round_eq)}"
        if self.round_leq:
            s += f"\nAND {leq('round', self.round_leq)}"
        if self.filter_destination:
            s += "\nAND NOT destination_reply"
        if self.filter_private:
            s += "\nAND NOT private_probe_dst_prefix"
            s += "\nAND NOT private_reply_src_addr"
        if self.time_exceeded_only:
            s += "\nAND time_exceeded_reply"
        if self.filter_invalid_probe_protocol:
            s += "\nAND valid_probe_protocol"
        return s

    def addr_cast(self, column: str) -> str:
        """Returns the column casted to the specified address type."""
        if self.addr_type == AddrType.IPv6:
            return column
        elif self.addr_type == AddrType.String:
            return f"CAST({column} AS String)"
        elif self.addr_type == AddrType.FixedString:
            return f"CAST({column} AS FixedString(16))"
        elif self.addr_type == AddrType.IPv6NumToString:
            return f"IPv6NumToString({column})"
        else:
            raise AttributeError("`addr_type` must be `AddrType` type")

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def query(self, table: str, subset: IPNetwork = UNIVERSE_SUBSET) -> str:
        raise NotImplementedError

    @contextmanager
    def _log_time(self, table: str, subset: IPNetwork) -> Iterator:
        logger.info("query=%s table=%s subset=%s", self.name, table, subset)
        start = datetime.now()
        yield
        delta = datetime.now() - start
        logger.info(
            "query=%s table=%s subset=%s time=%s", self.name, table, subset, delta
        )
