import asyncio
import json
import os
from asyncio import Semaphore
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from functools import reduce
from typing import AsyncIterator, Iterable, Iterator, List, Optional, Sequence, Tuple

import httpx
from aioch import Client as AsyncClient
from clickhouse_driver import Client
from clickhouse_driver.errors import ServerException

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.logging import logger
from diamond_miner.queries.fragments import and_, eq, ip_eq, ip_in, leq, not_, or_
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


@asynccontextmanager
async def async_client(url: str):
    c = AsyncClient.from_url(url)
    try:
        yield c
    finally:
        await c.disconnect()


@contextmanager
def client(url: str):
    c = Client.from_url(url)
    try:
        yield c
    finally:
        c.disconnect()


def flows_table(measurement_id: str) -> str:
    return f"flows__{measurement_id}".replace("-", "_")


def links_table(measurement_id: str) -> str:
    return f"links__{measurement_id}".replace("-", "_")


def prefixes_table(measurement_id: str) -> str:
    return f"prefixes__{measurement_id}".replace("-", "_")


def results_table(measurement_id: str) -> str:
    return f"results__{measurement_id}".replace("-", "_")


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
class StoragePolicy:
    """
    See
    - https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree/#table_engine-mergetree-ttl
    - https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree/#table_engine-mergetree-multiple-volumes
    """

    name: str = "default"
    """Name of the ClickHouse storage policy to use for the table."""
    archive_to: str = "default"
    """Name of the ClickHouse archive volume."""
    archive_on: datetime = datetime(2100, 1, 1)
    """Date at which the table will be moved to the archive volume."""


@dataclass(frozen=True)
class Query:
    addr_type: AddrType = AddrType.IPv6

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        # As a query user, prefer calling ``statements`` instead of ``statement`` as there
        # is no guarantees that the query will implement this method and return a single statement.
        raise NotImplementedError

    def statements(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> Sequence[str]:
        # Override this method if you want your query to return multiple statements.
        return (self.statement(measurement_id, subset),)

    def execute(
        self,
        url: str,
        measurement_id: str,
        subsets: Iterable[IPNetwork] = (UNIVERSE_SUBSET,),
        limit: Optional[Tuple[int, int]] = None,
    ) -> List:
        return [row for row in self.execute_iter(url, measurement_id, subsets, limit)]

    async def execute_async(
        self,
        url: str,
        measurement_id: str,
        subsets: Iterable[IPNetwork] = (UNIVERSE_SUBSET,),
        limit: Optional[Tuple[int, int]] = None,
    ) -> List:
        return [
            row
            async for row in self.execute_iter_async(
                url, measurement_id, subsets, limit
            )
        ]

    def execute_iter(
        self,
        url: str,
        measurement_id: str,
        subsets: Iterable[IPNetwork] = (UNIVERSE_SUBSET,),
        limit: Optional[Tuple[int, int]] = None,
    ) -> Iterator:
        with client(url) as c:
            for subset in subsets:
                for i, statement in enumerate(self.statements(measurement_id, subset)):
                    if limit:
                        statement += f"\nLIMIT {limit[0]} OFFSET {limit[1]}"
                    with LoggingTimer(
                        logger,
                        f"query={self.name}#{i} measurement_id={measurement_id} subset={subset}",
                    ):
                        rows = c.execute_iter(statement, settings=CH_QUERY_SETTINGS)
                        for row in rows:
                            yield row

    async def execute_iter_async(
        self,
        url: str,
        measurement_id: str,
        subsets: Iterable[IPNetwork] = (UNIVERSE_SUBSET,),
        limit: Optional[Tuple[int, int]] = None,
    ) -> AsyncIterator:
        async with async_client(url) as c:
            for subset in subsets:
                for i, statement in enumerate(self.statements(measurement_id, subset)):
                    if limit:
                        statement += f"\nLIMIT {limit[0]} OFFSET {limit[1]}"
                    with LoggingTimer(
                        logger,
                        f"query={self.name}#{i} measurement_id={measurement_id} subset={subset} limit={limit}",
                    ):
                        rows = await c.execute_iter(
                            statement, settings=CH_QUERY_SETTINGS
                        )
                        async for row in rows:
                            yield row

    def execute_http(
        self,
        url: str,
        measurement_id: str,
        subsets: Iterable[IPNetwork] = (UNIVERSE_SUBSET,),
        limit: Optional[Tuple[int, int]] = None,
    ) -> Iterator[dict]:
        for subset in subsets:
            for i, statement in enumerate(self.statements(measurement_id, subset)):
                if limit:
                    statement += f"\nLIMIT {limit[0]} OFFSET {limit[1]}"
                statement += "\nFORMAT JSONEachRow"
                with LoggingTimer(
                    logger,
                    f"query={self.name}#{i} measurement_id={measurement_id} subset={subset} limit={limit}",
                ):
                    r = httpx.get(url, params={"query": statement})
                    for line in r.content.splitlines():
                        yield json.loads(line)

    async def execute_http_async(
        self,
        url: str,
        measurement_id: str,
        subsets: Iterable[IPNetwork] = (UNIVERSE_SUBSET,),
        limit: Optional[Tuple[int, int]] = None,
    ) -> AsyncIterator[dict]:
        async with httpx.AsyncClient() as c:
            for subset in subsets:
                for i, statement in enumerate(self.statements(measurement_id, subset)):
                    if limit:
                        statement += f"\nLIMIT {limit[0]} OFFSET {limit[1]}"
                    statement += "\nFORMAT JSONEachRow"
                    with LoggingTimer(
                        logger,
                        f"query={self.name}#{i} measurement_id={measurement_id} subset={subset} limit={limit}",
                    ):
                        r = await c.get(url, params={"query": statement})
                        for line in r.content.splitlines():
                            yield json.loads(line)

    async def execute_concurrent(
        self,
        url: str,
        measurement_id: str,
        subsets: Iterable[IPNetwork] = (UNIVERSE_SUBSET,),
        limit: Optional[Tuple[int, int]] = None,
        concurrent_requests: int = (os.cpu_count() or 2) // 2,
    ) -> None:
        semaphore = Semaphore(concurrent_requests)

        async def do(subset: IPNetwork) -> None:
            async with semaphore:
                try:
                    await self.execute_async(url, measurement_id, (subset,), limit)
                except ServerException as e:
                    logger.error(
                        "query=%s subset=%s exception=%s", self.name, subset, e
                    )
                    raise e

        logger.info("query=%s concurrent_requests=%s", self.name, concurrent_requests)
        await asyncio.gather(*[do(subset) for subset in subsets])

    def _addr_cast(self, column: str) -> str:
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
class FlowsQuery(Query):
    round_eq: Optional[int] = None
    "If specified, keep only the flows from this round."

    def filters(self, subset: IPNetwork) -> str:
        """``WHERE`` clause common to all queries on the flows table."""
        s = []
        if subset != UNIVERSE_SUBSET:
            s += [ip_in("probe_dst_prefix", subset)]
        if self.round_eq:
            s += [eq("round", self.round_eq)]
        return reduce(and_, s or ["1"])


@dataclass(frozen=True)
class LinksQuery(Query):
    filter_inter_round: bool = False
    "If true, exclude links inferred across rounds."

    filter_partial: bool = False
    "If true, exclude partial links: ``('::', node)`` and ``(node, '::')``."

    filter_virtual: bool = False
    "If true, exclude virtual links: ``('::', '::')``."

    near_or_far_addr: Optional[str] = None
    "If specified, keep only the links that contains this IP address."

    probe_protocol: Optional[int] = None
    "If specified, keep only the links inferred from probes sent with this protocol."

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
        s = []
        if subset != UNIVERSE_SUBSET:
            s += [ip_in("probe_dst_prefix", subset)]
        if self.probe_protocol:
            s += [eq("probe_protocol", self.probe_protocol)]
        if self.probe_src_addr:
            s += [ip_eq("probe_src_addr", self.probe_src_addr)]
        if self.round_eq:
            s += [eq("near_round", self.round_eq), eq("far_round", self.round_eq)]
        if self.round_leq:
            s += [leq("near_round", self.round_leq), leq("far_round", self.round_leq)]
        if self.near_or_far_addr:
            s += [
                or_(
                    ip_eq("near_addr", self.near_or_far_addr),
                    ip_eq("far_addr", self.near_or_far_addr),
                )
            ]
        if self.filter_inter_round:
            s += [not_("is_inter_round")]
        if self.filter_partial:
            s += [not_("is_partial")]
        if self.filter_virtual:
            s += [not_("is_virtual")]
        return reduce(and_, s or ["1"])


@dataclass(frozen=True)
class PrefixesQuery(Query):
    probe_protocol: Optional[int] = None
    "If specified, keep only the links inferred from probes sent with this protocol."

    probe_src_addr: Optional[str] = None
    """
    If specified, keep only the links inferred from probes sent by this address.
    This filter is relatively costly (IPv6 comparison on each row).
    """

    def filters(self, subset: IPNetwork) -> str:
        """``WHERE`` clause common to all queries on the prefixes table."""
        s = []
        if subset != UNIVERSE_SUBSET:
            s += [ip_in("probe_dst_prefix", subset)]
        if self.probe_protocol:
            s += [eq("probe_protocol", self.probe_protocol)]
        if self.probe_src_addr:
            s += [ip_eq("probe_src_addr", self.probe_src_addr)]
        return reduce(and_, s or ["1"])


@dataclass(frozen=True)
class ResultsQuery(Query):
    filter_destination_host: bool = True
    "If true, ignore the replies from the destination host."

    filter_destination_prefix: bool = True
    "If true, ignore the replies from the destination prefix."

    filter_private: bool = True
    "If true, ignore the replies from private IP addresses."

    filter_invalid_probe_protocol: bool = True
    "If true, ignore the replies with probe protocol ≠ ICMP, ICMPv6 or UDP."

    time_exceeded_only: bool = True
    "If true, ignore non ICMP time exceeded replies."

    probe_protocol: Optional[int] = None
    "If specified, keep only the replies to probes sent with this protocol."

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
        s = []
        if subset != UNIVERSE_SUBSET:
            s += [ip_in("probe_dst_prefix", subset)]
        if self.probe_protocol:
            s += [eq("probe_protocol", self.probe_protocol)]
        if self.probe_src_addr:
            s += [ip_eq("probe_src_addr", self.probe_src_addr)]
        if self.round_eq:
            s += [eq("round", self.round_eq)]
        if self.round_leq:
            s += [leq("round", self.round_leq)]
        if self.filter_destination_host:
            s += [not_("destination_host_reply")]
        if self.filter_destination_prefix:
            s += [not_("destination_prefix_reply")]
        if self.filter_private:
            s += [not_("private_probe_dst_prefix"), not_("private_reply_src_addr")]
        if self.time_exceeded_only:
            s += ["time_exceeded_reply"]
        if self.filter_invalid_probe_protocol:
            s += ["valid_probe_protocol"]
        return reduce(and_, s or ["1"])
