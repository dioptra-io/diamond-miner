from ipaddress import IPv6Network, ip_network
from typing import Dict, List

from clickhouse_driver import Client

from diamond_miner.queries import CountReplies


def get_subsets(
    counts: Dict[IPv6Network, int], max_replies_per_subset: int
) -> List[IPv6Network]:
    """
    Return the IP prefixes such that there are no more than `max_replies_per_subset`
    replies per prefix.

    :param counts: Number of replies per prefix in the database table.
    :param max_replies_per_subset: Maximum number of replies per prefix.

    >>> counts = {ip_network("::ffff:8.8.4.0/120"): 10, ip_network("::ffff:8.8.8.0/120"): 5}
    >>> get_subsets(counts, 15)
    [IPv6Network('::/0')]
    >>> get_subsets(counts, 10)
    [IPv6Network('::ffff:808:800/117'), IPv6Network('::ffff:808:0/117')]
    >>> get_subsets({} , 10)
    []
    """

    def count_replies(subset):
        total = 0
        for network, count in counts.items():
            if network.subnet_of(subset):
                total += count
        return total

    candidates = [(ip_network("::/0"), count_replies(ip_network("::/0")))]
    subsets = []

    while candidates:
        candidate, n_replies = candidates.pop()
        if n_replies == 0:
            continue
        if n_replies <= max_replies_per_subset:
            subsets.append(candidate)
        else:
            # Can we split?
            a, b = tuple(candidate.subnets(prefixlen_diff=1))
            n_replies_a = count_replies(a)
            n_replies_b = count_replies(b)
            if n_replies_a + n_replies_b == 0:
                subsets.append(candidate)
            else:
                candidates.append((a, n_replies_a))
                candidates.append((b, n_replies_b))

    return subsets


def subsets_for_table(
    client: Client, table: str, max_replies_per_subset=256_000_000, query_kwargs=None
):
    """
    Return the IP prefixes such that there are no more than `max_replies_per_subset`
    replies per prefix.

    :param client: Clickhouse client.
    :param table: Database table.
    :param max_replies_per_subset: Maximum number of replies per prefix.
    :param query_kwargs: Keyword arguments forwarded to `CountReplies`.
    """
    # Compute the subsets such that each queries runs on at-most X rows.
    count_replies_query = CountReplies(
        chunk_len_v4=8, chunk_len_v6=8, **(query_kwargs or {})
    )

    # TODO: Cleanup this
    counts = {}
    for chunk, count in count_replies_query.execute(client, table):
        if chunk.ipv4_mapped:
            net = ip_network(str(chunk) + f"/{96 + 8}")
        else:
            net = ip_network(str(chunk) + f"/{8}")
        counts[net] = count

    return get_subsets(counts, max_replies_per_subset)
