"""
Functions for mapping flow IDs to addresses and ports.
We make the flow ID start at 0.
`prefix_size` is the number of addresses in the prefix:
`2 ** (32 - 24)` for a /24 in IPv4.
"""
import random

from pygfc import Permutation

from diamond_miner.defaults import DEFAULT_PREFIX_SIZE_V4


class SequentialFlowMapper:
    """
    Maps flow 0 to address 0, flow 1 to address 1, and so on until we have done
    the whole prefix. It then increases the port number in the same manner.

    Examples:
        >>> from diamond_miner.mappers import SequentialFlowMapper
        >>> mapper = SequentialFlowMapper()
        >>> mapper.offset(1)
        (1, 0)
    """

    def __init__(self, prefix_size: int = DEFAULT_PREFIX_SIZE_V4):
        assert prefix_size > 0, "prefix_size must be positive."
        self.prefix_size = prefix_size

    def flow_id(self, addr_offset: int, port_offset: int, prefix: int = 0) -> int:
        return addr_offset + port_offset

    def offset(self, flow_id: int, prefix: int = 0) -> tuple[int, int]:
        if flow_id < self.prefix_size:
            return flow_id, 0
        return self.prefix_size - 1, flow_id - self.prefix_size + 1


class IntervalFlowMapper:
    """
    Similar to the `SequentialFlowMapper` but with an increment >= 1.
    This allows to target addresses .1, .33, .65, ... in priority,
    which are more likely to respond to probes[@fan2010selecting].

    Examples:
        >>> from diamond_miner.mappers import IntervalFlowMapper
        >>> mapper = IntervalFlowMapper()
        >>> mapper.offset(1)
        (33, 0)
    """

    def __init__(self, prefix_size: int = DEFAULT_PREFIX_SIZE_V4, step: int = 32):
        assert prefix_size > 0, "prefix_size must be positive."
        assert prefix_size % 2 == 0, "prefix_size must be pair."
        assert step > 0, "step must be positive."
        assert step % 2 == 0, "step must be pair."
        self.period = prefix_size // step
        self.prefix_size = prefix_size
        self.step = step

    def flow_id(self, addr_offset: int, port_offset: int, prefix: int = 0) -> int:
        if addr_offset == 0:
            return self.prefix_size - 1
        if port_offset != 0:
            return self.prefix_size + port_offset - 1
        q, r = divmod(addr_offset - 1, self.step)
        return r * self.period + q

    def offset(self, flow_id: int, prefix: int = 0) -> tuple[int, int]:
        if flow_id < self.prefix_size - 1:
            return ((flow_id * self.step) % (self.prefix_size - 1)) + 1, 0
        if flow_id == self.prefix_size - 1:
            return 0, 0
        return self.prefix_size - 1, flow_id - self.prefix_size + 1


class ReverseByteFlowMapper:
    """
    Maps flow `n` to address `reverse(n)` until we have done the whole prefix.
    It then increases the port number sequentially.

    Examples:
        >>> from diamond_miner.mappers import ReverseByteFlowMapper
        >>> mapper = ReverseByteFlowMapper()
        >>> mapper.offset(1)
        (129, 0)
    """

    def flow_id(self, addr_offset: int, port_offset: int, prefix: int = 0) -> int:
        assert addr_offset < 256
        if addr_offset == 0:
            return 255
        if port_offset != 0:
            return 255 + port_offset
        return self.reverse_byte(addr_offset - 1)

    def offset(self, flow_id: int, prefix: int = 0) -> tuple[int, int]:
        if flow_id < 255:
            return self.reverse_byte(flow_id) + 1, 0
        if flow_id == 255:
            return 0, 0
        return 255, flow_id - 255

    def reverse_byte(self, i: int) -> int:
        # https://stackoverflow.com/a/2602885
        i = (i & 0xF0) >> 4 | (i & 0x0F) << 4
        i = (i & 0xCC) >> 2 | (i & 0x33) << 2
        i = (i & 0xAA) >> 1 | (i & 0x55) << 1
        return i


class RandomFlowMapper:
    """
    Similar to the `SequentialFlowMapper` but with a random mapping between flow IDs and addresses.
    The mapping is randomized by prefix.

    Examples:
        >>> from diamond_miner.mappers import RandomFlowMapper
        >>> mapper = RandomFlowMapper(seed=2022)
        >>> mapper.offset(1, prefix=1)
        (34, 0)
        >>> mapper.offset(1, prefix=2)
        (145, 0)
    """

    def __init__(self, seed: int, prefix_size: int = DEFAULT_PREFIX_SIZE_V4):
        # We can generate a random permutation up to 2^64-1 only.
        assert prefix_size > 0, "prefix_size must be positive."
        self.permutations = []
        self.prefix_size = min(prefix_size, (2**64) - 1)
        random.seed(seed)
        for i in range(1024):
            perm = Permutation(self.prefix_size, 3, random.randint(0, 2**64))
            self.permutations.append(perm)

    def flow_id(self, addr_offset: int, port_offset: int, prefix: int) -> int:
        assert addr_offset < self.prefix_size
        if port_offset != 0:
            return self.prefix_size + port_offset - 1
        perm = self.permutations[prefix % len(self.permutations)]
        return perm.inv(addr_offset)  # type: ignore

    def offset(self, flow_id: int, prefix: int) -> tuple[int, int]:
        if flow_id < self.prefix_size:
            perm = self.permutations[prefix % len(self.permutations)]
            return perm[flow_id], 0
        else:
            return self.prefix_size - 1, flow_id - self.prefix_size + 1
