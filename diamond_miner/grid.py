from collections.abc import Iterator, Sequence
from random import randint
from typing import Any

from pygfc import Permutation


# The Cython version is approx. 2x faster on a M1 CPU.
class ParameterGrid:
    def __init__(self, *parameters: Sequence):
        self.parameters = parameters
        self.size_ = tuple(len(p) for p in self.parameters)
        self.len = 1
        for dim in self.size_:
            self.len *= dim

    def __getitem__(self, index: Sequence[int] | int) -> Sequence[Any]:
        if isinstance(index, int):
            if index >= len(self):
                raise IndexError("index out of range")
            index = self.linear_to_subscript(index)
        return [p[i] for p, i in zip(self.parameters, index)]

    def __iter__(self) -> Iterator[Sequence[Any]]:
        return (self[index] for index in range(len(self)))

    def __len__(self) -> int:
        return self.len

    @property
    def size(self) -> Sequence[int]:
        return self.size_

    def shuffled(
        self, rounds: int = 6, seed: int | None = None
    ) -> Iterator[Sequence[Any]]:
        seed = seed or randint(0, 2**64 - 1)
        perm = Permutation(len(self), rounds, seed)
        return (self[index] for index in perm)

    def linear_to_subscript(self, index: int) -> Sequence[int]:
        coordinates = []
        for dim in self.size_:
            index, coordinate = divmod(index, dim)
            coordinates.append(coordinate)
        return coordinates
