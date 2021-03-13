from diamond_miner.mappers import (
    IntervalFlowMapper,
    RandomFlowMapper,
    ReverseByteFlowMapper,
    SequentialFlowMapper,
)


def _test_mapper(mapper, prefix_len, **kwargs):
    n_addrs = 2 ** (32 - prefix_len)
    offsets = []

    # Ensure that there is a unique flow ID <-> address mapping
    for flow_id in range(n_addrs):
        addr_offset, port_offset = mapper.offset(
            flow_id, prefix_len=prefix_len, **kwargs
        )
        assert mapper.flow_id(addr_offset, **kwargs) == flow_id
        assert port_offset == 0
        offsets.append((addr_offset, port_offset))

    # We currently do not use the port number (due to NAT, etc.) to compute the flow ID,
    # so we can't test mapper.flow_id(...) when the flow ID is larger
    # than the number of addresses.
    for flow_id in range(n_addrs, n_addrs + 100):
        addr_offset, port_offset = mapper.offset(
            flow_id, prefix_len=prefix_len, **kwargs
        )
        assert addr_offset == n_addrs - 1
        assert port_offset == flow_id - n_addrs + 1
        offsets.append((addr_offset, port_offset))

    # Ensure that there are no duplicate offsets.
    assert len(offsets) == len(set(offsets))


def test_sequential_flow_mapper():
    for prefix_len in [23, 24, 28, 32]:
        mapper = SequentialFlowMapper()
        _test_mapper(mapper, prefix_len)


def test_interval_flow_mapper():
    for step in [1, 2, 15, 32]:
        mapper = IntervalFlowMapper(step=step)
        _test_mapper(mapper, prefix_len=24)


def test_reverse_byte_flow_mapper():
    mapper = ReverseByteFlowMapper()
    _test_mapper(mapper, prefix_len=24)


def test_random_flow_mapper():
    mapper = RandomFlowMapper(master_seed=42)
    _test_mapper(mapper, prefix_len=24, prefix=100)
    a1 = mapper.offset(42, prefix_len=24, prefix=100)
    a2 = mapper.offset(42, prefix_len=24, prefix=100)
    b1 = mapper.offset(42, prefix_len=24, prefix=200)
    assert a1 == a2 != b1