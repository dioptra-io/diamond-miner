"""
Functions for mapping flow IDs to addresses and ports.
We currently map IPs to (0, 254) and by convention we make the flow ID start at 0.
`prefix_size` is the number of addresses in the prefix:
    2**(32-24) for a /24 in IPv4
"""
import random


class SequentialFlowMapper:
    """
    Maps flow 0 to address 0, flow 1 to address 1, and so on until we have done
    the whole prefix. It then increases the port number in the same manner.
    >>> mapper = SequentialFlowMapper()
    >>> mapper.offset(10, prefix_size=256)
    (10, 0)
    >>> mapper.offset(256, prefix_size=256)
    (255, 1)
    """

    @staticmethod
    def flow_id(addr_offset, *args, **kwargs):
        return addr_offset

    @staticmethod
    def offset(flow_id, prefix_size, *args, **kwargs):
        if flow_id < prefix_size:
            return flow_id, 0
        return prefix_size - 1, flow_id - prefix_size + 1


class IntervalFlowMapper:
    """
    Similar to the `SequentialFlowMapper` but with an increment >= 1.
    This allows to target addresses .1, .33, .65, ... in priority,
    which according to a paper by J. Heidemann are more likely to respond to probes.
    >>> mapper = IntervalFlowMapper(step=32)
    >>> mapper.offset(0, prefix_size=256)
    (1, 0)
    >>> mapper.offset(1, prefix_size=256)
    (33, 0)
    >>> mapper.offset(8, prefix_size=256)
    (2, 0)
    """

    def __init__(self, step=32):
        self.flow_to_offset = [1]
        i = 1
        for flow_id in range(254):
            offset = self.flow_to_offset[-1] + step
            if offset >= 256:
                i += 1
                offset = i
            self.flow_to_offset.append(offset)
        self.flow_to_offset.append(0)
        self.offset_to_flow = {
            offset: flow for flow, offset in enumerate(self.flow_to_offset)
        }

    def flow_id(self, addr_offset, *args, **kwargs):
        return self.offset_to_flow[addr_offset]

    def offset(self, flow_id, prefix_size, *args, **kwargs):
        assert prefix_size == 256, "prefixes != /24 are not supported"
        if flow_id < 256:
            return self.flow_to_offset[flow_id], 0
        return 255, flow_id - 255


class ReverseByteFlowMapper:
    """
    Maps flow n to address reverse(n) until we have done the whole prefix.
    It then increases the port number sequentially.
    >>> mapper = ReverseByteFlowMapper()
    >>> mapper.offset(0, prefix_size=256)
    (0, 0)
    >>> mapper.offset(1, prefix_size=256)
    (128, 0)
    >>> mapper.offset(2, prefix_size=256)
    (64, 0)
    >>> mapper.offset(3, prefix_size=256)
    (192, 0)
    """

    @classmethod
    def flow_id(cls, addr_offset, *args, **kwargs):
        return cls.reverse_byte(addr_offset)

    @classmethod
    def offset(cls, flow_id, prefix_size, *args, **kwargs):
        assert prefix_size == 256, "prefixes != /24 are not supported"
        if flow_id < 256:
            return cls.reverse_byte(flow_id), 0
        return 255, flow_id - 255

    @staticmethod
    def reverse_byte(i):
        return int("{:08b}".format(i)[::-1], 2)


class RandomFlowMapper:
    """
    Similar to the `SequentialFlowMapper` but with a random mapping
    between flow IDs and addresses.
    The mapping is randomized by prefix.
    >>> mapper = RandomFlowMapper(master_seed=42)
    >>> mapper.offset(0, prefix_size=256, prefix=100)
    (1, 0)
    >>> mapper.offset(0, prefix_size=256, prefix=200)
    (133, 0)
    """

    def __init__(self, master_seed):
        self.master_seed = master_seed
        self.flow_arrays = []
        random.seed(master_seed)
        for i in range(1000):
            flow_array = [i for i in range(0, 256)]
            random.shuffle(flow_array)
            self.flow_arrays.append(flow_array)

    def flow_id(self, addr_offset, prefix, *args, **kwargs):
        flow_array = self.flow_arrays[prefix % len(self.flow_arrays)]
        return flow_array.index(addr_offset)

    def offset(self, flow_id, prefix_size, prefix, *args, **kwargs):
        assert prefix_size == 256, "prefixes != /24 are not supported"
        if flow_id < 256:
            flow_array = self.flow_arrays[prefix % len(self.flow_arrays)]
            return flow_array[flow_id], 0
        else:
            return 255, flow_id - 255
