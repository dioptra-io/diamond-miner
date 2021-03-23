from diamond_miner.queries.get_links import GetLinks  # noqa
from diamond_miner.queries.get_next_round import GetNextRound  # noqa
from diamond_miner.queries.query import addr_to_string  # noqa
from diamond_miner.test import execute  # noqa


def test_get_links_nsdi():
    """
    >>> row = execute(GetLinks(), 'test_nsdi_example')[0]
    >>> addr_to_string(row[0])
    '100.0.0.1'
    >>> addr_to_string(row[1])
    '200.0.0.0'
    >>> links = [(addr_to_string(a), addr_to_string(b)) for a, b in row[2]]
    >>> sorted(links)[:3]
    [('150.0.1.1', '150.0.2.1'), ('150.0.1.1', '150.0.3.1'), ('150.0.2.1', '150.0.4.1')]
    >>> sorted(links)[3:6]
    [('150.0.3.1', '150.0.5.1'), ('150.0.3.1', '150.0.7.1'), ('150.0.4.1', '150.0.6.1')]
    >>> sorted(links)[6:]
    [('150.0.5.1', '150.0.6.1'), ('150.0.7.1', '150.0.6.1')]
    """


def test_get_next_round_nsdi():
    """
    >>> rows = execute(GetNextRound(round_leq=1, adaptive_eps=False), 'test_nsdi_lite')
    >>> row = GetNextRound.Row(*rows[0])
    >>> addr_to_string(row.dst_prefix), row.min_src_port, row.min_dst_port, row.max_dst_port, row.skip_prefix
    ('200.0.0.0', 24000, 33434, 33434, 0)
    >>> row.prev_max_flow
    [6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6]
    >>> row.probes
    [5, 5, 5, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

    >>> rows = execute(GetNextRound(round_leq=2, adaptive_eps=False), 'test_nsdi_lite')
    >>> row = GetNextRound.Row(*rows[0])
    >>> addr_to_string(row.dst_prefix), row.min_src_port, row.min_dst_port, row.max_dst_port, row.skip_prefix
    ('200.0.0.0', 24000, 33434, 33434, 0)
    >>> row.prev_max_flow
    [11, 11, 11, 11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    >>> row.probes
    [0, 5, 5, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

    >>> rows = execute(GetNextRound(round_leq=3, adaptive_eps=False), 'test_nsdi_lite')
    >>> row = GetNextRound.Row(*rows[0])
    >>> addr_to_string(row.dst_prefix), row.min_src_port, row.min_dst_port, row.max_dst_port, row.skip_prefix
    ('200.0.0.0', 24000, 33434, 33434, 1)
    >>> row.prev_max_flow
    [11, 16, 16, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    >>> row.probes
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    """


def test_get_next_round_star():
    """
    >>> rows = execute(GetNextRound(round_leq=1, adaptive_eps=False), 'test_star_node_star')
    >>> row = GetNextRound.Row(*rows[0])
    >>> addr_to_string(row.dst_prefix), row.min_src_port, row.min_dst_port, row.max_dst_port, row.skip_prefix
    ('200.0.0.0', 24000, 33434, 33434, 0)
    >>> row.prev_max_flow
    [6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6]
    >>> row.probes
    [0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

    >>> rows = execute(GetNextRound(round_leq=2, adaptive_eps=False), 'test_star_node_star')
    >>> row = GetNextRound.Row(*rows[0])
    >>> addr_to_string(row.dst_prefix), row.min_src_port, row.min_dst_port, row.max_dst_port, row.skip_prefix
    ('200.0.0.0', 24000, 33434, 33434, 0)
    >>> row.prev_max_flow
    [0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    >>> row.probes
    [0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

    >>> rows = execute(GetNextRound(round_leq=3, adaptive_eps=False), 'test_star_node_star')
    >>> row = GetNextRound.Row(*rows[0])
    >>> addr_to_string(row.dst_prefix), row.min_src_port, row.min_dst_port, row.max_dst_port, row.skip_prefix
    ('200.0.0.0', 24000, 33434, 33434, 1)
    >>> row.prev_max_flow
    [0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    >>> row.probes
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    """
