"""Flow mapper operations."""

import random

from abc import ABC, abstractmethod


class AbtractFlowMapper(ABC):
    @abstractmethod
    def flow_id(self, addr_offset, *args, **kwargs):
        """
        Retrieve the flow_id from the tuple (addr_offset, port_offset).
        """
        pass

    @abstractmethod
    def offset(self, flow_id, prefix_size, *args, **kwargs):
        """
        Given a `flow_id` and a `prefix_size`,
        returns a tuple (addr_offset, port_offset).
        """
        pass


class SequentialFlowMapper(ABC):
    """Sequential flow mapper (legacy)."""

    def flow_id(self, addr_offset, *args, **kwargs):
        return addr_offset

    def offset(self, flow_id, prefix_size, *args, **kwargs):
        n = 2 ** (32 - prefix_size)
        if flow_id < n:
            return (flow_id, 0)
        return (254, flow_id - n + 1)


class ReverseByteOrderFlowMapper(ABC):
    """Reverse byte order flow mapper."""

    def _reverse_bytes(self, i):
        return int("{:08b}".format(i)[::-1], 2)

    def flow_id(self, addr_offset, *args, **kwargs):
        return self._reverse_bytes(addr_offset)

    def offset(self, flow_id, prefix_size, *args, **kwargs):
        n = 2 ** (32 - prefix_size)
        if flow_id < n:
            return (self._reverse_bytes(flow_id), 0)
        return (254, flow_id - n + 1)


class RandomFlowMapper(ABC):
    """Random flow mapper."""

    def __init__(self, master_seed):
        self.master_seed = master_seed

    def _generate_flow_array(self, prefix):
        flow_array = [i for i in range(0, 256)]
        random.seed(prefix + self.master_seed)
        random.shuffle(flow_array)
        return flow_array

    def flow_id(self, i, prefix):
        flow_array = self._generate_flow_array(prefix)
        return flow_array.index(i)

    def offset(self, flow_id, prefix_size, prefix):
        n = 2 ** (32 - prefix_size)
        if flow_id < n:
            flow_array = self._generate_flow_array(prefix)
            return (flow_array[flow_id], 0)
        else:
            return (254, flow_id - n + 1)
