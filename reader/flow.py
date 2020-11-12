"""Flow mapper operations."""

from abc import ABC, abstractmethod


class AbtractFlowMapper(ABC):
    @abstractmethod
    def flow_id(self, addr_offset):
        """
        Retrieve the flow_id from the tuple (addr_offset, port_offset).
        """
        pass

    @abstractmethod
    def offset(self, flow_id, prefix_size):
        """
        Given a `flow_id` and a `prefix_size`,
        returns a tuple (addr_offset, port_offset).
        """
        pass


class SequentialFlowMapper(ABC):
    """Sequential flow mapper (legacy)."""

    def flow_id(self, addr_offset):
        return addr_offset

    def offset(self, flow_id, prefix_size):
        n = 2 ** (32 - prefix_size)
        if flow_id < n:
            return (flow_id, 0)
        return (254, flow_id - n + 1)


class ReverseByteOrderFlowMapper(ABC):
    """Reverse byte order flow mapper."""

    def _reverse_bytes(self, i):
        return int("{:08b}".format(i)[::-1], 2)

    def flow_id(self, addr_offset):
        return self._reverse_bytes(addr_offset)

    def offset(self, flow_id, prefix_size):
        n = 2 ** (32 - prefix_size)
        if flow_id < n:
            return (self._reverse_bytes(flow_id), 0)
        return (254, flow_id - n + 1)
