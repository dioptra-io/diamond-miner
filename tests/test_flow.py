from diamond_miner.flow import (
    CIDRFlowMapper,
    RandomFlowMapper,
    ReverseByteOrderFlowMapper,
    SequentialFlowMapper,
)


def test_sequential_flow_mapper():
    """Test of `SequentialFlowMapper` class."""

    mapper = SequentialFlowMapper()

    # dst_ip + src_port => flow_id
    assert mapper.flow_id(1) == 1
    assert mapper.flow_id(6) == 6

    # flow_id => dst_ip + src_port
    assert mapper.offset(1, 24) == (1, 0)
    assert mapper.offset(255, 24) == (255, 0)
    assert mapper.offset(256, 24) == (254, 1)
    assert mapper.offset(512, 24) == (254, 257)


def test_reverse_order_flow_mapper():
    """Test of `ReverseByteOrderFlowMapper` class."""

    mapper = ReverseByteOrderFlowMapper()

    # dst_ip + src_port => flow_id
    assert mapper.flow_id(1) == 128
    assert mapper.flow_id(6) == 96

    # flow_id => dst_ip + src_port
    assert mapper.offset(128, 24) == (1, 0)
    assert mapper.offset(96, 24) == (6, 0)
    assert mapper.offset(256, 24) == (254, 1)
    assert mapper.offset(512, 24) == (254, 257)


def test_cidr_flow_mapper():
    """Test of `CIDRFlowMapper` class."""

    mapper = CIDRFlowMapper()
    prefix_size = 24

    for flow_id in range(255):
        addr_offset, port_offset = mapper.offset(flow_id, prefix_size)
        assert mapper.flow_id(addr_offset) == flow_id

    assert mapper.offset(0, 24) == (1, 0)
    assert mapper.offset(1, 24) == (33, 0)
    assert mapper.offset(254, 24) == (224, 0)
    assert mapper.offset(255, 24) == (254, 1)
    assert mapper.flow_id(1) == 0
    assert mapper.flow_id(33) == 1
    assert mapper.flow_id(255) == 255


def test_random_flow_mapper():
    """Test of `RandomFlowMapper` class."""

    mapper = RandomFlowMapper(master_seed=27)

    # dst_ip + src_port => flow_id
    assert mapper.flow_id(1, 134744064) == 207
    assert mapper.flow_id(6, 134744064) == 96

    assert mapper.flow_id(1, 167772160) == 113
    assert mapper.flow_id(6, 167772160) == 110

    # flow_id => dst_ip + src_port
    assert mapper.offset(177, 24, 134744064) == (119, 0)
    assert mapper.offset(44, 24, 134744064) == (126, 0)
    assert mapper.offset(256, 24, 134744064) == (254, 1)
    assert mapper.offset(512, 24, 134744064) == (254, 257)

    assert mapper.offset(223, 24, 167772160) == (28, 0)
    assert mapper.offset(193, 24, 167772160) == (189, 0)
