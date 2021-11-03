from ipaddress import IPv6Address, IPv6Network, ip_network
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


async def subsets_for(
    query: Union[FlowsQuery, LinksQuery, ProbesQuery, ResultsQuery],
    url: str,
    measurement_id: str,
    max_items_per_subset: int = 8_000_000,
) -> List[IPv6Network]:
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
            addr, count_query.prefix_len_v4, count_query.prefix_len_v6
        ): count
        for addr, count in await count_query.execute_async(url, measurement_id)
    }
    return split(counts, max_items_per_subset)


def split(counts: Counts, max_items_per_subset: int) -> List[IPv6Network]:
    """
    Return the IP networks such that there are no more than `max_items_per_subset`
    per network.

    :param counts: Number of items per prefix in the database table.
    :param max_items_per_subset: Maximum number of items per network.
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


def addr_to_network(
    addr: IPv6Address, prefix_len_v4: int, prefix_len_v6: int
) -> IPv6Network:
    if addr.ipv4_mapped:
        return IPv6Network(f"{addr}/{96+prefix_len_v4}")
    return IPv6Network(f"{addr}/{prefix_len_v6}")


def n_items(counts: Counts, subset: IPv6Network) -> int:
    total = 0
    for network, count_ in counts.items():
        if network.subnet_of(subset):
            total += count_
    return total
