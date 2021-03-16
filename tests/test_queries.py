from diamond_miner.queries import GetNextRound  # noqa
from diamond_miner.test import execute  # noqa


def test_get_next_round_nsdi():
    """
    >>> row = execute(GetNextRound('100.0.0.1', 1, adaptive_eps=False), 'test_nsdi_lite')[0]
    >>> row.dst_prefix, row.min_src_port, row.min_dst_port, row.max_dst_port, row.skip_prefix
    ('200.0.0.0', 24000, 33434, 33434, 0)
    >>> row.prev_max_flow
    [6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6]
    >>> row.probes
    [5, 5, 5, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

    >>> row = execute(GetNextRound('100.0.0.1', 2, adaptive_eps=False), 'test_nsdi_lite')[0]
    >>> row.dst_prefix, row.min_src_port, row.min_dst_port, row.max_dst_port, row.skip_prefix
    ('200.0.0.0', 24000, 33434, 33434, 0)
    >>> row.prev_max_flow
    [11, 11, 11, 11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    >>> row.probes
    [0, 5, 5, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

    >>> row = execute(GetNextRound('100.0.0.1', 3, adaptive_eps=False), 'test_nsdi_lite')[0]
    >>> row.dst_prefix, row.min_src_port, row.min_dst_port, row.max_dst_port, row.skip_prefix
    ('200.0.0.0', 24000, 33434, 33434, 1)
    >>> row.prev_max_flow
    [11, 16, 16, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    >>> row.probes
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    """


def test_get_next_round_star():
    """
    >>> row = execute(GetNextRound('100.0.0.1', 1, adaptive_eps=False), 'test_star_node_star')[0]
    >>> row.dst_prefix, row.min_src_port, row.min_dst_port, row.max_dst_port, row.skip_prefix
    ('200.0.0.0', 24000, 33434, 33434, 0)
    >>> row.prev_max_flow
    [6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6]
    >>> row.probes
    [0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

    >>> row = execute(GetNextRound('100.0.0.1', 2, adaptive_eps=False), 'test_star_node_star')[0]
    >>> row.dst_prefix, row.min_src_port, row.min_dst_port, row.max_dst_port, row.skip_prefix
    ('200.0.0.0', 24000, 33434, 33434, 0)
    >>> row.prev_max_flow
    [0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    >>> row.probes
    [0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

    >>> row = execute(GetNextRound('100.0.0.1', 3, adaptive_eps=False), 'test_star_node_star')[0]
    >>> row.dst_prefix, row.min_src_port, row.min_dst_port, row.max_dst_port, row.skip_prefix
    ('200.0.0.0', 24000, 33434, 33434, 1)
    >>> row.prev_max_flow
    [0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    >>> row.probes
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    """
