from functools import reduce
from ipaddress import IPv4Network, IPv6Network
from operator import mul
from random import randint
from typing import Optional, Union

from pygfc import Permutation


# TODO: Cythonize for faster iteration?
class ParameterGrid:
    """
    >>> grid = ParameterGrid(['a', 'b'], range(3))
    >>> grid.size
    (2, 3)
    >>> len(grid)
    6
    >>> grid[1,2]
    ['b', 2]
    >>> grid[5]
    ['b', 2]
    >>> grid[6]
    Traceback (most recent call last):
        ...
    IndexError: index out of range
    >>> list(grid)
    [['a', 0], ['b', 0], ['a', 1], ['b', 1], ['a', 2], ['b', 2]]
    >>> list(grid.shuffled(seed=42))
    [['a', 1], ['b', 1], ['b', 0], ['a', 0], ['b', 2], ['a', 2]]
    """

    __slots__ = ("parameters", "size", "len")

    def __init__(self, *parameters):
        self.parameters = parameters
        self.size = tuple(len(p) for p in self.parameters)
        self.len = reduce(mul, self.size, 1)

    def __iter__(self):
        return (self[index] for index in range(len(self)))

    def __getitem__(self, index):
        if isinstance(index, int):
            if index >= len(self):
                raise IndexError("index out of range")
            index = self._lin_to_sub(index)
        return [p[i] for p, i in zip(self.parameters, index)]

    def __len__(self):
        return self.len

    def _lin_to_sub(self, index):
        """
        Convert a linear index to subscript indices.
        >>> grid = ParameterGrid(range(2), range(2))
        >>> grid._lin_to_sub(0)
        [0, 0]
        >>> grid._lin_to_sub(3)
        [1, 1]
        """
        coordinates = []
        for dim in self.size:
            index, coordinate = divmod(index, dim)
            coordinates.append(coordinate)
        return coordinates

    def shuffled(self, rounds: int = 6, seed: Optional[int] = None):
        seed = seed or randint(0, 2 ** 64 - 1)
        perm = Permutation(len(self), rounds, seed)
        return (self[index] for index in perm)


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


def probe_to_csv(dst_addr: int, src_port: int, dst_port: int, ttl: int) -> str:
    return f"{dst_addr},{src_port},{dst_port},{ttl}"
