"""
Functions for mapping flow IDs to addresses and ports.
We make the flow ID start at 0.
`prefix_size` is the number of addresses in the prefix:
    2**(32-24) for a /24 in IPv4
"""
import random
from typing import List, Protocol, Tuple

Offset = Tuple[int, int]


class FlowMapper(Protocol):
    def flow_id(self, addr_offset: int, prefix: int) -> int:
        pass

    def offset(self, flow_id: int, prefix: int, prefix_size: int) -> Offset:
        pass


class SequentialFlowMapper:
    """
    Maps flow 0 to address 0, flow 1 to address 1, and so on until we have done
    the whole prefix. It then increases the port number in the same manner.
    >>> mapper = SequentialFlowMapper()
    >>> mapper.offset(10, prefix=100, prefix_size=256)
    (10, 0)
    >>> mapper.offset(256, prefix=100, prefix_size=256)
    (255, 1)
    """

    @staticmethod
    def flow_id(addr_offset: int, prefix: int) -> int:
        return addr_offset

    @staticmethod
    def offset(flow_id: int, prefix: int, prefix_size: int) -> Offset:
        if flow_id < prefix_size:
            return flow_id, 0
        return prefix_size - 1, flow_id - prefix_size + 1


class IntervalFlowMapper:
    """
    Similar to the `SequentialFlowMapper` but with an increment >= 1.
    This allows to target addresses .1, .33, .65, ... in priority,
    which according to a paper by J. Heidemann are more likely to respond to probes.
    >>> mapper = IntervalFlowMapper(prefix_size=256, step=32)
    >>> mapper.offset(0, prefix=100, prefix_size=256)
    (1, 0)
    >>> mapper.offset(1, prefix=100, prefix_size=256)
    (33, 0)
    >>> mapper.offset(8, prefix=100, prefix_size=256)
    (2, 0)
    """

    flow_to_offset: List[int]
    prefix_size: int

    def __init__(self, prefix_size: int, step: int = 32):
        self.prefix_size = prefix_size
        self.flow_to_offset = [1]
        i = 1
        for flow_id in range(prefix_size - 2):
            offset = self.flow_to_offset[-1] + step
            if offset >= prefix_size:
                i += 1
                offset = i
            self.flow_to_offset.append(offset)
        self.flow_to_offset.append(0)
        self.offset_to_flow = {
            offset: flow for flow, offset in enumerate(self.flow_to_offset)
        }

    def flow_id(self, addr_offset: int, prefix: int) -> int:
        return self.offset_to_flow[addr_offset]

    def offset(self, flow_id: int, prefix: int, prefix_size: int) -> Offset:
        assert prefix_size == self.prefix_size
        if flow_id < prefix_size:
            return self.flow_to_offset[flow_id], 0
        return prefix_size - 1, flow_id - prefix_size + 1


class ReverseByteFlowMapper:
    """
    Maps flow n to address reverse(n) until we have done the whole prefix.
    It then increases the port number sequentially.
    >>> mapper = ReverseByteFlowMapper()
    >>> mapper.offset(0, prefix=100, prefix_size=256)
    (0, 0)
    >>> mapper.offset(1, prefix=100, prefix_size=256)
    (128, 0)
    >>> mapper.offset(2, prefix=100, prefix_size=256)
    (64, 0)
    >>> mapper.offset(3, prefix=100, prefix_size=256)
    (192, 0)
    """

    @classmethod
    def flow_id(cls, addr_offset: int, prefix: int) -> int:
        return cls.reverse_byte(addr_offset)

    @classmethod
    def offset(cls, flow_id: int, prefix: int, prefix_size: int) -> Offset:
        assert prefix_size >= 256, "the prefix must have at-least 256 addresses"
        if flow_id < 256:
            return cls.reverse_byte(flow_id), 0
        return 255, flow_id - 255

    @staticmethod
    def reverse_byte(i: int) -> int:
        return int("{:08b}".format(i)[::-1], 2)


class RandomFlowMapper:
    """
    Similar to the `SequentialFlowMapper` but with a random mapping
    between flow IDs and addresses.
    The mapping is randomized by prefix.
    >>> mapper = RandomFlowMapper(prefix_size=256, master_seed=42)
    >>> mapper.offset(0, prefix=100, prefix_size=256)
    (1, 0)
    >>> mapper.offset(0, prefix=200, prefix_size=256)
    (133, 0)
    """

    flow_arrays: List[List[int]]
    prefix_size: int

    def __init__(self, prefix_size: int, master_seed: int):
        self.flow_arrays = []
        self.prefix_size = prefix_size
        random.seed(master_seed)
        for i in range(1000):
            flow_array = list(range(prefix_size))
            random.shuffle(flow_array)
            self.flow_arrays.append(flow_array)

    def flow_id(self, addr_offset: int, prefix: int) -> int:
        flow_array = self.flow_arrays[prefix % len(self.flow_arrays)]
        return flow_array.index(addr_offset)

    def offset(self, flow_id, prefix: int, prefix_size: int) -> Offset:
        assert prefix_size == self.prefix_size
        if flow_id < prefix_size:
            flow_array = self.flow_arrays[prefix % len(self.flow_arrays)]
            return flow_array[flow_id], 0
        else:
            return prefix_size - 1, flow_id - prefix_size + 1
