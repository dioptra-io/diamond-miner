from dataclasses import dataclass
from enum import Enum
from typing import AsyncIterator, Iterable, Iterator, List, Optional

from aioch import Client as AsyncClient
from clickhouse_driver import Client

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.logging import logger
from diamond_miner.queries.fragments import eq, ip_eq, ip_in, leq
from diamond_miner.timer import LoggingTimer
from diamond_miner.typing import IPNetwork

CH_QUERY_SETTINGS = {
    "max_block_size": 128_000,
    # Avoid timeout in case of a slow connection
    "connect_timeout": 1000,
    "send_timeout": 6000,
    "receive_timeout": 6000,
    # https://github.com/ClickHouse/ClickHouse/issues/18406
    "read_backoff_min_latency_ms": 100_000,
}


class AddrType(str, Enum):
    """ClickHouse IPv6 type."""

    IPv6 = "IPv6"
    """
    IPv6 stored as FixedString(16) internally.
    Slow with clickhouse-driver as it will call :func:`ipaddress.IPv6Address` on each address.
    """

    String = "String"
    """
    String representation of the internal FixedString.
    With clickhouse-driver each address will be decoded as Python string.
    """

    FixedString = "FixedString"
    """
    Native FixedString(16) representation.
    Fastest with clickhouse-driver as it will return directly a byte string.
    """

    IPv6NumToString = "IPv6NumToString"
    """
    Human string representation of the IPv6.
    With clickhouse-driver each address will be decoded as Python string.
    """


@dataclass(frozen=True)
class Query:
    addr_type: AddrType = AddrType.IPv6

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def query(self, table: str, subset: IPNetwork = UNIVERSE_SUBSET) -> str:
        raise NotImplementedError

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
            with LoggingTimer(
                logger, f"query={self.name} table={table} subset={subset}"
            ):
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
            with LoggingTimer(
                logger, f"query={self.name} table={table} subset={subset}"
            ):
                rows = await client.execute_iter(query, settings=CH_QUERY_SETTINGS)
                async for row in rows:
                    yield row

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


@dataclass(frozen=True)
class LinksQuery(Query):
    filter_inter_round: bool = False
    "If true, exclude links inferred across rounds."

    filter_partial: bool = False
    "If true, exclude partial links: ``('::', node)`` and ``(node, '::')``."

    filter_virtual: bool = False
    "If true, exclude virtual links: ``('::', '::')``."

    probe_src_addr: Optional[str] = None
    """
    If specified, keep only the links inferred from probes sent by this address.
    This filter is relatively costly (IPv6 comparison on each row).
    """

    round_eq: Optional[int] = None
    "If specified, keep only the links from this round."

    round_leq: Optional[int] = None
    "If specified, keep only the links from this round or before."

    def filters(self, subset: IPNetwork) -> str:
        """``WHERE`` clause common to all queries on the links table."""
        s = "1"
        if subset != UNIVERSE_SUBSET:
            s += f"\nAND {ip_in('probe_dst_prefix', subset)}"
        if self.probe_src_addr:
            s += f"\nAND {ip_eq('probe_src_addr', self.probe_src_addr)}"
        if self.round_eq:
            s += f"\nAND near_round = {self.round_eq}"
            s += f"\nAND far_round = {self.round_eq}"
        if self.round_leq:
            s += f"\nAND near_round <= {self.round_leq}"
            s += f"\nAND far_round <= {self.round_leq}"
        if self.filter_inter_round:
            s += "\nAND NOT is_inter_round"
        if self.filter_partial:
            s += "\nAND NOT is_partial"
        if self.filter_virtual:
            s += "\nAND NOT is_virtual"
        return s


@dataclass(frozen=True)
class ResultsQuery(Query):
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

    def filters(self, subset: IPNetwork) -> str:
        """``WHERE`` clause common to all queries on the results table."""
        s = "1"
        if subset != UNIVERSE_SUBSET:
            s += f"\nAND {ip_in('probe_dst_prefix', subset)}"
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
