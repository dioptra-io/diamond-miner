import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from functools import reduce
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple, Union

import orjson
import requests

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.logger import logger
from diamond_miner.queries.fragments import (
    and_,
    eq,
    geq,
    ip_eq,
    ip_in,
    leq,
    lt,
    not_,
    or_,
)
from diamond_miner.typing import IPNetwork
from diamond_miner.utilities import LoggingTimer


def links_table(measurement_id: str) -> str:
    return f"links__{measurement_id}".replace("-", "_")


def prefixes_table(measurement_id: str) -> str:
    return f"prefixes__{measurement_id}".replace("-", "_")


def probes_table(measurement_id: str) -> str:
    return f"probes__{measurement_id}".replace("-", "_")


def results_table(measurement_id: str) -> str:
    return f"results__{measurement_id}".replace("-", "_")


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

    def execute(self, *args: Any, **kwargs: Any) -> List[dict]:
        return list(self.execute_iter(*args, **kwargs))

    def execute_iter(
        self,
        url: str,
        measurement_id: str,
        *,
        data: Optional[Any] = None,
        limit: Optional[Tuple[int, int]] = None,
        subsets: Iterable[IPNetwork] = (UNIVERSE_SUBSET,),
        timeout: Optional[Tuple[int, int]] = (5, 3600),
    ) -> Iterator[dict]:
        for subset in subsets:
            for i, statement in enumerate(self.statements(measurement_id, subset)):
                with LoggingTimer(
                    logger,
                    f"query={self.name}#{i} measurement_id={measurement_id} subset={subset} limit={limit}",
                ):
                    params: Dict[str, Union[int, str]] = {
                        "default_format": "JSONEachRow",
                        "limit": limit[0] if limit else 0,
                        "offset": limit[1] if limit else 0,
                        "output_format_json_quote_64bit_integers": 0,
                        "query": statement,
                    }
                    r = requests.post(
                        url,
                        headers={"Accept-encoding": "gzip"},
                        params=params,
                        data=data,
                        stream=True,
                        timeout=timeout,
                    )
                    try:
                        for line in r.iter_lines(chunk_size=2 ** 20):
                            yield orjson.loads(line)
                    except orjson.JSONDecodeError as e:
                        raise RuntimeError(f"Invalid database response: {line}") from e

    def execute_concurrent(
        self,
        url: str,
        measurement_id: str,
        *,
        subsets: Iterable[IPNetwork] = (UNIVERSE_SUBSET,),
        limit: Optional[Tuple[int, int]] = None,
        concurrent_requests: int = (os.cpu_count() or 2) // 2,
    ) -> None:
        logger.info("query=%s concurrent_requests=%s", self.name, concurrent_requests)
        with ThreadPoolExecutor(concurrent_requests) as executor:
            futures = [
                executor.submit(
                    self.execute,
                    url=url,
                    measurement_id=measurement_id,
                    subsets=(subset,),
                    limit=limit,
                )
                for subset in subsets
            ]
            for future in as_completed(futures):
                future.result()


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
class ProbesQuery(Query):
    probe_protocol: Optional[int] = None
    "If specified, keep only probes sent with this protocol."

    probe_ttl_geq: Optional[int] = None
    "If specified, keep only the probes with TTL >= this value."

    probe_ttl_leq: Optional[int] = None
    "If specified, keep only the probes with TTL <= this value."

    round_eq: Optional[int] = None
    "If specified, keep only the probes from this round."

    round_geq: Optional[int] = None
    "If specified, keep only the probes from this round or after."

    round_leq: Optional[int] = None
    "If specified, keep only the probes from this round or before."

    round_lt: Optional[int] = None
    "If specified, keep only the probes from before this round."

    def filters(self, subset: IPNetwork) -> str:
        """``WHERE`` clause common to all queries on the probes table."""
        s = []
        if subset != UNIVERSE_SUBSET:
            s += [ip_in("probe_dst_prefix", subset)]
        if self.probe_protocol:
            s += [eq("probe_protocol", self.probe_protocol)]
        if self.probe_ttl_geq:
            s += [geq("probe_ttl", self.probe_ttl_geq)]
        if self.probe_ttl_leq:
            s += [leq("probe_ttl", self.probe_ttl_leq)]
        if self.round_eq:
            s += [eq("round", self.round_eq)]
        if self.round_geq:
            s += [geq("round", self.round_geq)]
        if self.round_lt:
            s += [lt("round", self.round_lt)]
        if self.round_leq:
            s += [leq("round", self.round_leq)]
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
