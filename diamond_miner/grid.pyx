from random import randint

from pygfc import Permutation


# The Cython version is approx. 2x faster on a M1 CPU.
cdef class ParameterGrid:
    cdef tuple parameters
    cdef tuple size_
    cdef len

    def __init__(self, *parameters):
        self.parameters = parameters
        self.size_ = tuple(len(p) for p in self.parameters)
        self.len = 1
        for dim in self.size_:
            self.len *= dim

    def __getitem__(self, index):
        if isinstance(index, int):
            if index >= len(self):
                raise IndexError("index out of range")
            index = self.linear_to_subscript(index)
        return [p[i] for p, i in zip(self.parameters, index)]

    def __iter__(self):
        return (self[index] for index in range(len(self)))

    def __len__(self):
        return self.len

    @property
    def size(self):
        return self.size_

    def shuffled(self, int rounds = 6, seed = None):
        seed = seed or randint(0, 2 ** 64 - 1)
        perm = Permutation(len(self), rounds, seed)
        return (self[index] for index in perm)

    cdef list linear_to_subscript(self, index):
        cdef list coordinates = []
        for dim in self.size_:
            index, coordinate = divmod(index, dim)
            coordinates.append(coordinate)
        return coordinates
