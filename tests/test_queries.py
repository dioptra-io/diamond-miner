from diamond_miner.queries import (  # noqa
    GetLinks,
    GetLinksFromResults,
    GetNextRound,
    addr_to_string,
)
from diamond_miner.test import client  # noqa


def test_get_links_nsdi():
    """
    >>> row = GetLinksFromResults().execute(client, 'test_nsdi_example')[0]
    >>> row[0]
    1
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


def test_get_links_multi_protocol():
    """
    >>> rows = GetLinksFromResults().execute(client, 'test_multi_protocol')
    >>> rows = sorted(rows)
    >>> addr_to_string(rows[0][1])
    '200.0.0.0'
    >>> links = [(addr_to_string(a), addr_to_string(b)) for a, b in rows[0][2]]
    >>> sorted(links)
    [('150.0.0.1', '150.0.1.1'), ('150.0.0.2', '150.0.1.1')]
    >>> rows[1][0]
    17
    >>> addr_to_string(rows[1][1])
    '200.0.0.0'
    >>> links = [(addr_to_string(a), addr_to_string(b)) for a, b in rows[1][2]]
    >>> sorted(links)
    [('150.0.0.1', '150.0.1.1')]
    """


def test_get_next_round_nsdi():
    """
    >>> rows = GetNextRound(round_leq=1, adaptive_eps=False).execute(client, 'test_nsdi_lite_links')
    >>> row = GetNextRound.Row(*rows[0])
    >>> addr_to_string(row.dst_prefix), row.min_src_port, row.min_dst_port, row.max_dst_port
    ('200.0.0.0', 24000, 33434, 33434)
    >>> row.prev_max_flow
    [6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6]
    >>> row.probes
    [5, 5, 5, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

    >>> rows = GetNextRound(round_leq=2, adaptive_eps=False).execute(client, 'test_nsdi_lite_links')
    >>> row = GetNextRound.Row(*rows[0])
    >>> addr_to_string(row.dst_prefix), row.min_src_port, row.min_dst_port, row.max_dst_port
    ('200.0.0.0', 24000, 33434, 33434)
    >>> row.prev_max_flow
    [11, 11, 11, 11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    >>> row.probes
    [0, 5, 5, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

    >>> GetNextRound(round_leq=3, adaptive_eps=False).execute(client, 'test_nsdi_lite_links')
    []
    """


def test_get_next_round_star():
    """
    With the current links computation (in GetLinksFromView), we do not emit a link
    in the case of *single* reply in a traceroute. For example: * * node * *, does
    not generate a link. In this case this means that we never see a link including V_7.

    >>> rows = GetNextRound(round_leq=1, adaptive_eps=False).execute(client, 'test_star_node_star_links')
    >>> row = GetNextRound.Row(*rows[0])
    >>> addr_to_string(row.dst_prefix), row.min_src_port, row.min_dst_port, row.max_dst_port
    ('200.0.0.0', 24000, 33434, 33434)
    >>> row.prev_max_flow
    [6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6]
    >>> row.probes
    [0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

    >>> GetNextRound(round_leq=2, adaptive_eps=False).execute(client, 'test_star_node_star_links')
    []
    """
