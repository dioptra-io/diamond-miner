from ipaddress import IPv4Network, IPv6Network
from typing import Protocol, Tuple, Union


class FlowMapper(Protocol):
    """Protocol to which a flow mapper must conform."""

    def flow_id(self, addr_offset: int, port_offset: int, prefix: int) -> int:
        """Return the flow ID for a given address and port offset."""
        ...

    def offset(self, flow_id: int, prefix: int) -> Tuple[int, int]:
        """Return the address and port offset for a given flow ID."""
        ...


Probe = Tuple[int, int, int, int, str]
IPNetwork = Union[IPv4Network, IPv6Network]
