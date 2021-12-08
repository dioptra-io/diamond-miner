from ipaddress import IPv6Network, ip_network
from typing import Dict, List, Union

from diamond_miner.queries import (
    CountFlowsPerPrefix,
    CountLinksPerPrefix,
    CountProbesPerPrefix,
    CountResultsPerPrefix,
    FlowsQuery,
    LinksQuery,
    ProbesQuery,
    ResultsQuery,
)
from diamond_miner.utilities import common_parameters

Counts = Dict[IPv6Network, int]


def subsets_for(
    query: Union[FlowsQuery, LinksQuery, ProbesQuery, ResultsQuery],
    url: str,
    measurement_id: str,
    *,
    max_items_per_subset: int = 8_000_000,
) -> List[IPv6Network]:
    """
    >>> from diamond_miner.test import url
    >>> from diamond_miner.queries import GetLinksFromView, GetLinks, GetProbes, GetResults
    >>> subsets_for(GetLinksFromView(), url, 'test_nsdi_example', max_items_per_subset=1)
    [IPv6Network('::ffff:c800:0/104')]
    >>> subsets_for(GetLinks(), url, 'test_nsdi_example', max_items_per_subset=1)
    [IPv6Network('::ffff:c800:0/104')]
    >>> subsets_for(GetProbes(round_eq=1), url, 'test_nsdi_example', max_items_per_subset=1)
    [IPv6Network('::ffff:c800:0/104')]
    >>> subsets_for(GetResults(), url, 'test_nsdi_example', max_items_per_subset=1)
    [IPv6Network('::ffff:c800:0/104')]
    """
    if isinstance(query, FlowsQuery):
        count_query = CountFlowsPerPrefix(**common_parameters(query, FlowsQuery))
    elif isinstance(query, LinksQuery):
        count_query = CountLinksPerPrefix(**common_parameters(query, LinksQuery))  # type: ignore
    elif isinstance(query, ProbesQuery):
        count_query = CountProbesPerPrefix(**common_parameters(query, ProbesQuery))  # type: ignore
    elif isinstance(query, ResultsQuery):
        count_query = CountResultsPerPrefix(**common_parameters(query, ResultsQuery))  # type: ignore
    else:
        raise NotImplementedError
    counts = {
        addr_to_network(
            row["prefix"], count_query.prefix_len_v4, count_query.prefix_len_v6
        ): row["count"]
        for row in count_query.execute_iter(url, measurement_id)
    }
    return split(counts, max_items_per_subset)


def split(counts: Counts, max_items_per_subset: int) -> List[IPv6Network]:
    """
    Return the IP networks such that there are no more than `max_items_per_subset`
    per network.

    :param counts: Number of items per prefix in the database table.
    :param max_items_per_subset: Maximum number of items per network.

    >>> counts = {ip_network("::ffff:8.8.4.0/120"): 10, ip_network("::ffff:8.8.8.0/120"): 5}
    >>> split(counts, 15)
    [IPv6Network('::/0')]
    >>> split(counts, 10)
    [IPv6Network('::ffff:808:0/117'), IPv6Network('::ffff:808:800/117')]
    >>> split(counts, 1) # Impossible case, should return the minimal feasible networks.
    [IPv6Network('::ffff:808:400/120'), IPv6Network('::ffff:808:800/120')]
    >>> split({}, 10)
    []
    """
    candidates = [(ip_network("::/0"), n_items(counts, ip_network("::/0")))]
    subsets = []

    while candidates:
        candidate, n_replies = candidates.pop()
        if max_items_per_subset >= n_replies > 0:
            subsets.append(candidate)
        elif n_replies > 0:
            a, b = tuple(candidate.subnets(prefixlen_diff=1))
            n_items_a = n_items(counts, a)
            n_items_b = n_items(counts, b)
            if n_items_a + n_items_b == 0:
                subsets.append(candidate)
            else:
                candidates.append((a, n_items_a))
                candidates.append((b, n_items_b))

    return sorted(subsets)


def addr_to_network(addr: str, prefix_len_v4: int, prefix_len_v6: int) -> IPv6Network:
    """
    >>> addr_to_network("::ffff:8.8.8.0", 24, 64)
    IPv6Network('::ffff:808:800/120')
    >>> addr_to_network("2001:4860:4860:1234::", 24, 64)
    IPv6Network('2001:4860:4860:1234::/64')
    """
    assert ":" in addr, "`addr` must be an (IPv4-mapped) IPv6 address."
    if addr.startswith("::ffff:"):
        return IPv6Network(f"{addr}/{96+prefix_len_v4}")
    return IPv6Network(f"{addr}/{prefix_len_v6}")


def n_items(counts: Counts, subset: IPv6Network) -> int:
    """
    >>> counts = {IPv6Network("1000::/16"): 2, IPv6Network("8000::/16"): 10}
    >>> n_items(counts, IPv6Network("0000::/1"))
    2
    >>> n_items(counts, IPv6Network("8000::/1"))
    10
    >>> n_items(counts, IPv6Network("::/0"))
    12
    """
    total = 0
    for network, count_ in counts.items():
        if network.subnet_of(subset):
            total += count_
    return total
