from ipaddress import IPv6Address, IPv6Network, ip_network
from typing import Any, Dict, List, Union

from diamond_miner.queries import CountLinksPerPrefix, CountResultsPerPrefix

Counts = Dict[IPv6Network, int]


async def links_subsets(
    url: str,
    measurement_id: str,
    max_rows_per_subset: int = 8_000_000,
    **kwargs: Any,
) -> List[IPv6Network]:
    return await generic_subsets(
        CountLinksPerPrefix(**kwargs), url, measurement_id, max_rows_per_subset
    )


async def results_subsets(
    url: str,
    measurement_id: str,
    max_rows_per_subset: int = 8_000_000,
    **kwargs: Any,
) -> List[IPv6Network]:
    return await generic_subsets(
        CountResultsPerPrefix(**kwargs), url, measurement_id, max_rows_per_subset
    )


async def generic_subsets(
    query: Union[CountLinksPerPrefix, CountResultsPerPrefix],
    url: str,
    measurement_id: str,
    max_rows_per_subset: int,
) -> List[IPv6Network]:
    counts: Counts = {
        addr_to_network(addr, query.prefix_len_v4, query.prefix_len_v6): count
        for addr, count in await query.execute_async(url, measurement_id)
    }
    return split(counts, max_rows_per_subset)


def split(counts: Counts, max_rows_per_subset: int) -> List[IPv6Network]:
    """
    Return the IP prefixes such that there are no more than `max_replies_per_subset`
    replies per prefix.

    :param counts: Number of replies per prefix in the database table.
    :param max_rows_per_subset: Maximum number of replies per prefix.
    """
    candidates = [(ip_network("::/0"), n_rows(counts, ip_network("::/0")))]
    subsets = []

    while candidates:
        candidate, n_replies = candidates.pop()
        if max_rows_per_subset >= n_replies > 0:
            subsets.append(candidate)
        elif n_replies > 0:
            a, b = tuple(candidate.subnets(prefixlen_diff=1))
            n_rows_a = n_rows(counts, a)
            n_rows_b = n_rows(counts, b)
            if n_rows_a + n_rows_b == 0:
                subsets.append(candidate)
            else:
                candidates.append((a, n_rows_a))
                candidates.append((b, n_rows_b))

    return sorted(subsets)


def addr_to_network(
    addr: IPv6Address, prefix_len_v4: int, prefix_len_v6: int
) -> IPv6Network:
    if addr.ipv4_mapped:
        return IPv6Network(f"{addr}/{96+prefix_len_v4}")
    return IPv6Network(f"{addr}/{prefix_len_v6}")


def n_rows(counts: Counts, subset: IPv6Network) -> int:
    total = 0
    for network, count_ in counts.items():
        if network.subnet_of(subset):
            total += count_
    return total
