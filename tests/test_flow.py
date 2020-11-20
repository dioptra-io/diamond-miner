from diamond_miner_core.flow import (
    SequentialFlowMapper,
    ReverseByteOrderFlowMapper,
    RandomFlowMapper,
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


def test_random_flow_mapper():
    """Test of `RandomFlowMapper` class."""

    mapper = RandomFlowMapper(master_seed=27)

    # dst_ip + src_port => flow_id
    assert mapper.flow_id(1, 134744064) == 177
    assert mapper.flow_id(6, 134744064) == 44

    assert mapper.flow_id(1, 167772160) == 89
    assert mapper.flow_id(6, 167772160) == 76

    # flow_id => dst_ip + src_port
    assert mapper.offset(177, 24, 134744064) == (1, 0)
    assert mapper.offset(44, 24, 134744064) == (6, 0)
    assert mapper.offset(256, 24, 134744064) == (254, 1)
    assert mapper.offset(512, 24, 134744064) == (254, 257)

    assert mapper.offset(89, 24, 167772160) == (1, 0)
    assert mapper.offset(76, 24, 167772160) == (6, 0)
