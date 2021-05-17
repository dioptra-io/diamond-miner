from ipaddress import IPv4Network, IPv6Network
from typing import Protocol, Tuple, Union


class FlowMapper(Protocol):
    def flow_id(self, addr_offset: int, port_offset: int, prefix: int) -> int:
        pass

    def offset(self, flow_id: int, prefix: int) -> Tuple[int, int]:
        pass


Probe = Tuple[int, int, int, int, str]
IPNetwork = Union[IPv4Network, IPv6Network]
