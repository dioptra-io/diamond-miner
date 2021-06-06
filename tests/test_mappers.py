from diamond_miner.mappers import (
    IntervalFlowMapper,
    RandomFlowMapper,
    ReverseByteFlowMapper,
    SequentialFlowMapper,
)
from diamond_miner.typing import FlowMapper


def _test_mapper(mapper: FlowMapper, prefix: int, prefix_size: int):
    for flow_id in range(prefix_size + 1024):
        addr_offset, port_offset = mapper.offset(flow_id, prefix=prefix)
        assert mapper.flow_id(addr_offset, port_offset, prefix) == flow_id


def test_sequential_flow_mapper():
    for prefix_len in [23, 24, 28, 32]:
        mapper = SequentialFlowMapper(prefix_size=2 ** (32 - prefix_len))
        _test_mapper(mapper, 100, 2 ** (32 - prefix_len))


def test_interval_flow_mapper():
    for step in [2, 16, 32]:
        mapper = IntervalFlowMapper(prefix_size=2 ** (32 - 24), step=step)
        _test_mapper(mapper, 100, 2 ** (32 - 24))


def test_reverse_byte_flow_mapper():
    mapper = ReverseByteFlowMapper()
    _test_mapper(mapper, 100, 2 ** (32 - 24))


def test_random_flow_mapper():
    mapper = RandomFlowMapper(prefix_size=2 ** (32 - 24), seed=42)
    _test_mapper(mapper, prefix=100, prefix_size=2 ** (32 - 24))
    a1 = mapper.offset(42, prefix=100)

    mapper = RandomFlowMapper(prefix_size=2 ** (32 - 24), seed=42)
    a2 = mapper.offset(42, prefix=100)
    b1 = mapper.offset(42, prefix=200)
    assert a1 == a2 != b1
