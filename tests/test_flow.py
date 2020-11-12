from reader.flow import SequentialFlowMapper, ReverseByteOrderFlowMapper


def test_sequential_flow_mapper():
    """Test of `SequentialFlowMapper` class."""

    mapper = SequentialFlowMapper()

    # dst_ip + src_port => flow_id
    assert mapper.flow_id(1, 1, 24) == 1
    assert mapper.flow_id(0, 1, 24) == 256
    assert mapper.flow_id(0, 257, 24) == 512

    # flow_id => dst_ip + src_port
    assert mapper.offset(1, 24) == (1, 0)
    assert mapper.offset(255, 24) == (255, 0)
    assert mapper.offset(256, 24) == (0, 1)
    assert mapper.offset(512, 24) == (0, 257)


def test_reverse_order_flow_mapper():
    """Test of `ReverseByteOrderFlowMapper` class."""

    mapper = ReverseByteOrderFlowMapper()

    # dst_ip + src_port => flow_id
    assert mapper.flow_id(1, 0, 24) == 128
    assert mapper.flow_id(6, 0, 24) == 96
    assert mapper.flow_id(0, 1, 24) == 256
    assert mapper.flow_id(0, 257, 24) == 512

    # flow_id => dst_ip + src_port
    assert mapper.offset(128, 24) == (1, 0)
    assert mapper.offset(96, 24) == (6, 0)
    assert mapper.offset(256, 24) == (0, 1)
    assert mapper.offset(512, 24) == (0, 257)
