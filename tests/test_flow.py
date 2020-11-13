from diamond_miner_core.flow import SequentialFlowMapper, ReverseByteOrderFlowMapper


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
