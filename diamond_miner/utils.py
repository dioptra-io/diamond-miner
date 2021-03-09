from functools import reduce
from ipaddress import IPv4Network, IPv6Network
from operator import mul
from random import randint
from typing import Iterable, Iterator, List, Optional, Tuple, Union

from blackrock import Permutation


def linear_to_subscript(index: int, dims: Iterable[int]) -> List[int]:
    """
    Convert a linear index to subscript indices.
    >>> linear_to_subscript(0, [2, 2])
    [0, 0]
    >>> linear_to_subscript(2, [2, 2])
    [0, 1]
    >>> linear_to_subscript(23, [2, 3, 4])
    [1, 2, 3]
    """
    assert index < reduce(mul, dims, 1)
    coordinates = []
    for dim in dims:
        index, coordinate = divmod(index, dim)
        coordinates.append(coordinate)
    return coordinates


def permutation(
    ranges: List[Tuple[int, int]], rounds: int = 14, seed: Optional[int] = None
) -> Iterator[List[int]]:
    """
    Iterate over a random permutation of the space defined by `ranges`.
    >>> it = permutation([(2, 4), (21, 23)], seed=42)
    >>> list(it)
    [[3, 21], [2, 22], [2, 21], [3, 22]]
    """
    seed = seed or randint(0, 2 ** 64 - 1)
    dims = [(stop - start) for start, stop in ranges]
    perm = Permutation(reduce(mul, dims, 1), seed, rounds)
    for value in perm:
        indices = linear_to_subscript(value, dims)
        for i, (start, stop) in enumerate(ranges):
            indices[i] += start
        yield indices


def subnets(network: Union[IPv4Network, IPv6Network], new_prefix: int):
    """
    Faster version of ipaddress.IPv4Network.subnets(...).
    Returns only the network address as an integer.
    """
    if new_prefix < network.prefixlen:
        raise ValueError("new prefix must be longer")
    start = int(network.network_address)
    end = int(network.broadcast_address) + 1
    step = (int(network.hostmask) + 1) >> (new_prefix - network.prefixlen)
    return range(start, end, step)
