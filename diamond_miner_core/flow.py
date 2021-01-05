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


class HeidemannFlowMapper(AbstractFlowMapper):
    """Host distribution from Heidemann paper."""

    def __init__(self, step=32):
        assert step % 2 == 0, "`step` must be pair"
        # Flow 0: 0 * step + 1 [255] => 0 + 1   [255] => 1
        # Flow 1: 1 * step + 1 [255] => 32 + 1  [255] => 33
        # ...
        # Flow 8: 8 * step + 1 [255] => 256 + 1 [255] => 2
        self.flow_to_offset = [(i * step + 1) % 255 for i in range(255)]
        self.offset_to_flow = {
            offset: flow for flow, offset in enumerate(self.flow_to_offset)
        }

    def _flow_id(self, addr_offset, *args, **kwargs):
        # flow_to_offset maps [0,254] to [0,254], however we could theoretically
        # receive an (erroneous) reply from .255, in this case we return an
        # aribtraty flow id.
        if addr_offset <= 254:
            return self.offset_to_flow[addr_offset]
        return 255

    def _offset(self, flow_id, prefix_size, *args, **kwargs):
        assert prefix_size == 24, "`prefix_size` != 24 are not supported"
        if flow_id <= 254:
            return (self.flow_to_offset[flow_id], 0)
        return (254, flow_id - 254)


class RandomFlowMapper(AbstractFlowMapper):
    """Random flow mapper."""

    def __init__(self, master_seed, n_array=1000):
        self.master_seed = master_seed
        self.flow_arrays = []
        random.seed(master_seed)
        for i in range(1000):
            flow_array = [i for i in range(0, 256)]
            random.shuffle(flow_array)
            self.flow_arrays.append(flow_array)

    def _flow_id(self, addr_offset, prefix):
        flow_array = self.flow_arrays[prefix % len(self.flow_arrays)]
        return flow_array.index(addr_offset)

    def _offset(self, flow_id, prefix_size, prefix):
        assert prefix_size == 24, "TODO: Handle other sizes"
        n = 2 ** (32 - prefix_size)
        if flow_id < n:
            flow_array = self.flow_arrays[prefix % len(self.flow_arrays)]
            return (flow_array[flow_id], 0)
        else:
            return (254, flow_id - n + 1)
