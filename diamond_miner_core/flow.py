"""Flow mapper operations."""

import random

from abc import ABC, abstractmethod

# NOTE: Currently we map IPs to (0, 254).
# NOTE: By convention we make the flow ID starts at 0.


class AbstractFlowMapper(ABC):
    def flow_id(self, addr_offset, *args, **kwargs):
        """
        Retrieve the flow_id from the tuple (addr_offset, port_offset).
        """
        assert addr_offset >= 0
        return self._flow_id(addr_offset, *args, **kwargs)

    def offset(self, flow_id, prefix_size, *args, **kwargs):
        """
        Given a `flow_id` and a `prefix_size`,
        returns a tuple (addr_offset, port_offset).
        """
        assert flow_id >= 0
        assert prefix_size >= 0
        return self._offset(flow_id, prefix_size, *args, **kwargs)

    @abstractmethod
    def _flow_id(self, addr_offset, *args, **kwargs):
        pass

    @abstractmethod
    def _offset(self, flow_id, prefix_size, *args, **kwargs):
        pass


class SequentialFlowMapper(AbstractFlowMapper):
    """Sequential flow mapper (legacy)."""

    def _flow_id(self, addr_offset, *args, **kwargs):
        return addr_offset

    def _offset(self, flow_id, prefix_size, *args, **kwargs):
        n = 2 ** (32 - prefix_size)
        if flow_id < n:
            return (flow_id, 0)
        return (254, flow_id - n + 1)


class ReverseByteOrderFlowMapper(AbstractFlowMapper):
    """Reverse byte order flow mapper."""

    def _reverse_bytes(self, i):
        return int("{:08b}".format(i)[::-1], 2)

    def _flow_id(self, addr_offset, *args, **kwargs):
        return self._reverse_bytes(addr_offset)

    def _offset(self, flow_id, prefix_size, *args, **kwargs):
        n = 2 ** (32 - prefix_size)
        if flow_id < n:
            return (self._reverse_bytes(flow_id), 0)
        return (254, flow_id - n + 1)


class RandomFlowMapper(AbstractFlowMapper):
    """Random flow mapper."""

    def __init__(self, master_seed, n_array=1000):
        self.master_seed = master_seed
        self.flow_arrays = []
        random.seed(master_seed)
        for i in range(1000):
            flow_array = [i for i in range(0, 255)]
            random.shuffle(flow_array)
            self.flow_arrays.append(flow_array)

    def _flow_id(self, addr_offset, prefix):
        flow_array = self.flow_arrays[prefix % len(self.flow_arrays)]
        return flow_array.index(addr_offset)

    def _offset(self, flow_id, prefix_size, prefix):
        n = 2 ** (32 - prefix_size)
        if flow_id < n:
            flow_array = self.flow_arrays[prefix % len(self.flow_arrays)]
            return (flow_array[flow_id], 0)
        else:
            return (254, flow_id - n + 1)
