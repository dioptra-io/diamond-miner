"""
Functions for mapping flow IDs to addresses and ports.
We make the flow ID start at 0.
`prefix_size` is the number of addresses in the prefix:
    2**(32-24) for a /24 in IPv4
"""
import random
from typing import List, Protocol, Tuple
from libc.stdint cimport uint8_t, uint16_t
from pygfc import Permutation

from diamond_miner.defaults import DEFAULT_PREFIX_SIZE_V4

cdef extern from *:
    ctypedef long long int128_t "__int128_t"
    ctypedef unsigned long long uint128_t "__uint128_t"

Offset = Tuple[int, int]

class FlowMapper(Protocol):
    def flow_id(self, addr_offset: int, prefix: int) -> int:
        pass

    def offset(self, flow_id: int, prefix: int) -> Offset:
        pass


cdef class SequentialFlowMapper:
    """
    Maps flow 0 to address 0, flow 1 to address 1, and so on until we have done
    the whole prefix. It then increases the port number in the same manner.
    """

    cdef uint128_t prefix_size

    def __init__(self, uint128_t prefix_size = DEFAULT_PREFIX_SIZE_V4):
        self.prefix_size = prefix_size

    cpdef uint128_t flow_id(self, uint128_t addr_offset, uint128_t prefix = 0):
        return addr_offset

    cpdef (uint128_t, uint16_t) offset(self, uint128_t flow_id, uint128_t prefix = 0):
        if flow_id < self.prefix_size:
            return flow_id, 0
        return self.prefix_size - 1, flow_id - self.prefix_size + 1


cdef class IntervalFlowMapper:
    """
    Similar to the `SequentialFlowMapper` but with an increment >= 1.
    This allows to target addresses .1, .33, .65, ... in priority,
    which according to a paper by J. Heidemann are more likely to respond to probes.
    """

    cdef uint128_t period
    cdef uint128_t prefix_size
    cdef uint128_t step

    def __init__(self, uint128_t prefix_size = DEFAULT_PREFIX_SIZE_V4, uint128_t step = 32):
        assert prefix_size % 2 == 0, "prefix_size must be pair."
        assert step % 2 == 0, "step must be pair."
        self.period = prefix_size // step
        self.prefix_size = prefix_size
        self.step = step

    cpdef uint128_t flow_id(self, uint128_t addr_offset, uint128_t prefix = 0):
        if addr_offset == 0:
            return self.prefix_size - 1
        q, r = divmod(addr_offset - 1, self.step)
        return r * self.period + q

    cpdef (uint128_t, uint16_t) offset(self, uint128_t flow_id, uint128_t prefix = 0):
        if flow_id < self.prefix_size - 1:
            return ((flow_id * self.step) % (self.prefix_size - 1)) + 1, 0
        if flow_id == self.prefix_size - 1:
            return 0, 0
        return self.prefix_size - 1, flow_id - self.prefix_size + 1


cdef class ReverseByteFlowMapper:
    """
    Maps flow n to address reverse(n) until we have done the whole prefix.
    It then increases the port number sequentially.
    """

    cpdef uint128_t flow_id(self, uint128_t addr_offset, uint128_t prefix = 0):
        assert addr_offset < 256
        return self.reverse_byte(addr_offset)

    cpdef (uint128_t, uint16_t) offset(self, uint128_t flow_id, uint128_t prefix = 0):
        if flow_id < 256:
            return self.reverse_byte(flow_id), 0
        return 255, flow_id - 255

    cdef uint8_t reverse_byte(self, uint8_t i):
        # https://stackoverflow.com/a/2602885
        i = (i & 0xF0) >> 4 | (i & 0x0F) << 4
        i = (i & 0xCC) >> 2 | (i & 0x33) << 2
        i = (i & 0xAA) >> 1 | (i & 0x55) << 1
        return i


cdef class RandomFlowMapper:
    """
    Similar to the `SequentialFlowMapper` but with a random mapping
    between flow IDs and addresses.
    The mapping is randomized by prefix.
    """

    cdef list permutations
    cdef uint128_t prefix_size

    def __init__(self, int seed, uint128_t prefix_size = DEFAULT_PREFIX_SIZE_V4):
        # We can generate a random permutation up to 2^64 only.
        self.permutations = []
        self.prefix_size = min(prefix_size, 2**64)
        random.seed(seed)
        for i in range(1024):
            perm = Permutation(self.prefix_size, 3, random.randint(0, 2 ** 64))
            self.permutations.append(perm)

    cpdef uint128_t flow_id(self, uint128_t addr_offset, uint128_t prefix):
        assert addr_offset < self.prefix_size
        perm = self.permutations[prefix % len(self.permutations)]
        return perm.inv(addr_offset)

    cpdef (uint128_t, uint16_t) offset(self, uint128_t flow_id, uint128_t prefix):
        if flow_id < self.prefix_size:
            perm = self.permutations[prefix % len(self.permutations)]
            return perm[flow_id], 0
        else:
            return self.prefix_size - 1, flow_id - self.prefix_size + 1
