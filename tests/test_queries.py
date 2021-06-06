from diamond_miner.queries import GetNextRound  # noqa
from diamond_miner.queries.get_links import GetLinksPerPrefix  # noqa
from diamond_miner.test import addr_to_string, client  # noqa


def test_get_links_nsdi():
    """
    >>> row = GetLinksPerPrefix().execute(client, 'test_nsdi_example_links')[0]
    >>> row[0]
    1
    >>> addr_to_string(row[1])
    '100.0.0.1'
    >>> addr_to_string(row[2])
    '200.0.0.0'
    >>> links = [(addr_to_string(a), addr_to_string(b)) for a, b in row[3]]
    >>> sorted(links)[:3]
    [('150.0.1.1', '150.0.2.1'), ('150.0.1.1', '150.0.3.1'), ('150.0.2.1', '150.0.4.1')]
    >>> sorted(links)[3:6]
    [('150.0.3.1', '150.0.5.1'), ('150.0.3.1', '150.0.7.1'), ('150.0.4.1', '150.0.6.1')]
    >>> sorted(links)[6:]
    [('150.0.5.1', '150.0.6.1'), ('150.0.7.1', '150.0.6.1')]
    """


def test_get_links_multi_protocol():
    """
    >>> rows = GetLinksPerPrefix().execute(client, 'test_multi_protocol_links')
    >>> rows = sorted(rows)
    >>> rows[0][0]
    1
    >>> addr_to_string(rows[0][2])
    '200.0.0.0'
    >>> links = [(addr_to_string(a), addr_to_string(b)) for a, b in rows[0][3]]
    >>> sorted(links)
    [('150.0.0.1', '150.0.1.1'), ('150.0.0.2', '150.0.1.1')]
    >>> rows[1][0]
    17
    >>> addr_to_string(rows[1][2])
    '200.0.0.0'
    >>> links = [(addr_to_string(a), addr_to_string(b)) for a, b in rows[1][3]]
    >>> sorted(links)
    [('150.0.0.1', '150.0.1.1')]
    """


def test_get_next_round_nsdi():
    """
    >>> rows = GetNextRound(round_leq=1, adaptive_eps=False).execute(client, 'test_nsdi_lite_links')
    >>> row = GetNextRound.Row(*rows[0])
    >>> addr_to_string(row.dst_prefix)
    '200.0.0.0'
    >>> row.ttls
    [1, 2, 3, 4]
    >>> row.already_sent
    [6, 6, 6, 6]
    >>> row.to_send
    [5, 5, 5, 5]

    >>> rows = GetNextRound(round_leq=2, adaptive_eps=False).execute(client, 'test_nsdi_lite_links')
    >>> row = GetNextRound.Row(*rows[0])
    >>> addr_to_string(row.dst_prefix)
    '200.0.0.0'
    >>> row.ttls
    [1, 2, 3, 4]
    >>> row.already_sent
    [11, 11, 11, 11]
    >>> row.to_send
    [0, 5, 5, 5]

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
    >>> addr_to_string(row.dst_prefix)
    '200.0.0.0'
    >>> row.ttls
    [1, 2, 3]
    >>> row.already_sent
    [6, 6, 6]
    >>> row.to_send
    [0, 0, 5]

    >>> GetNextRound(round_leq=2, adaptive_eps=False).execute(client, 'test_star_node_star_links')
    []
    """
