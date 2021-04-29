from typing import Protocol, Tuple


class FlowMapper(Protocol):
    def flow_id(self, addr_offset: int, prefix: int) -> int:
        pass

    def offset(self, flow_id: int, prefix: int) -> Tuple[int, int]:
        pass


ProbeType = Tuple[int, int, int, int, str]
