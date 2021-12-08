from dataclasses import dataclass
from ipaddress import ip_network
from typing import Sequence

import pytest

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries import Query
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class ValidQuery(Query):
    def statements(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> Sequence[str]:
        return ["SELECT arrayJoin([1,2,3,4]) AS a", "SELECT 10 AS a, 20 AS b"]


@dataclass(frozen=True)
class InvalidQuery(Query):
    def statements(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> Sequence[str]:
        return ["SELECT a FROM invalid_table", "SELECT zzz"]


def test_execute(url):
    rows = ValidQuery().execute(url, "")
    assert rows == [{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}, {"a": 10, "b": 20}]
    with pytest.raises(RuntimeError):
        InvalidQuery().execute(url, "")


def test_execute_concurrent(url):
    subsets = list(ip_network("0.0.0.0/0").subnets(prefixlen_diff=2))
    assert ValidQuery().execute_concurrent(url, "", subsets=subsets) is None
    with pytest.raises(RuntimeError):
        InvalidQuery().execute_concurrent(url, "", subsets=subsets)
