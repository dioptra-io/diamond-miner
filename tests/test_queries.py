from diamond_miner.queries import GetMDAProbes  # noqa
from diamond_miner.queries.get_links import GetLinks  # noqa
from diamond_miner.test import addr_to_string, url  # noqa


def test_get_links_nsdi():
    """
    >>> rows = GetLinks().execute(url, 'test_nsdi_example')
    >>> links = [(addr_to_string(a), addr_to_string(b)) for a, b in rows]
    >>> sorted(links)[:3]
    [('150.0.1.1', '150.0.2.1'), ('150.0.1.1', '150.0.3.1'), ('150.0.2.1', '150.0.4.1')]
    >>> sorted(links)[3:6]
    [('150.0.3.1', '150.0.5.1'), ('150.0.3.1', '150.0.7.1'), ('150.0.4.1', '150.0.6.1')]
    >>> sorted(links)[6:]
    [('150.0.5.1', '150.0.6.1'), ('150.0.7.1', '150.0.6.1')]
    """


def test_get_links_multi_protocol():
    """
    >>> rows = GetLinks(probe_protocol=1).execute(url, 'test_multi_protocol')
    >>> links = [(addr_to_string(a), addr_to_string(b)) for a, b in rows]
    >>> sorted(links)
    [('150.0.0.1', '150.0.1.1'), ('150.0.0.2', '150.0.1.1')]
    >>> rows = GetLinks(probe_protocol=17).execute(url, 'test_multi_protocol')
    >>> links = [(addr_to_string(a), addr_to_string(b)) for a, b in rows]
    >>> sorted(links)
    [('150.0.0.1', '150.0.1.1')]
    """


def test_get_mda_probes_nsdi():
    """
    >>> rows = GetMDAProbes(round_leq=1, adaptive_eps=False).execute(url, 'test_nsdi_lite')
    >>> row = GetMDAProbes.Row(*rows[0])
    >>> addr_to_string(row.dst_prefix)
    '200.0.0.0'
    >>> row.ttls
    [1, 2, 3, 4]
    >>> row.cumulative_probes
    [11, 11, 11, 11]

    >>> rows = GetMDAProbes(round_leq=2, adaptive_eps=False).execute(url, 'test_nsdi_lite')
    >>> row = GetMDAProbes.Row(*rows[0])
    >>> addr_to_string(row.dst_prefix)
    '200.0.0.0'
    >>> row.ttls
    [1, 2, 3, 4]
    >>> row.cumulative_probes
    [11, 16, 16, 16]

    >>> rows = GetMDAProbes(round_leq=3, adaptive_eps=False).execute(url, 'test_nsdi_lite')
    >>> row = GetMDAProbes.Row(*rows[0])
    >>> addr_to_string(row.dst_prefix)
    '200.0.0.0'
    >>> row.ttls
    [1, 2, 3, 4]
    >>> row.cumulative_probes
    [11, 16, 16, 16]
    """


# TODO: Rename to test_get_mda_probes_nsdi...
def test_get_mda_probes_stateful_nsdi():
    """
    >>> rows = GetMDAProbes(round_leq=1, adaptive_eps=False).execute(url, 'test_nsdi_lite')
    >>> row = GetMDAProbes.Row(*rows[0])
    >>> addr_to_string(row.dst_prefix)
    '200.0.0.0'
    >>> row.ttls
    [1, 2, 3, 4]
    >>> row.cumulative_probes
    [11, 11, 11, 11]
    >>> rows = GetMDAProbes(round_leq=2, adaptive_eps=False).execute(url, 'test_nsdi_lite')
    >>> row = GetMDAProbes.Row(*rows[0])
    >>> addr_to_string(row.dst_prefix)
    '200.0.0.0'
    >>> row.ttls
    [1, 2, 3, 4]
    >>> row.cumulative_probes
    [11, 16, 16, 16]

    >>> rows = GetMDAProbes(round_leq=3, adaptive_eps=False).execute(url, 'test_nsdi_lite')
    >>> row = GetMDAProbes.Row(*rows[0])
    >>> addr_to_string(row.dst_prefix)
    '200.0.0.0'
    >>> row.ttls
    [1, 2, 3, 4]
    >>> row.cumulative_probes
    [11, 16, 16, 16]
    """


# TODO: Make this test pass
#  def test_get_mda_probes_star():
#  """
#  With the current links computation (in GetLinksFromView), we do not emit a link
#  in the case of *single* reply in a traceroute. For example: * * node * *, does
#  not generate a link. In this case this means that we never see a link including V_7.
#
#  >>> rows = GetMDAProbes(round_leq=1, adaptive_eps=False).execute(url, 'test_star_node_star')
#  >>> row = GetMDAProbes.Row(*rows[0])
#  >>> addr_to_string(row.dst_prefix)
#  '200.0.0.0'
#  >>> row.ttls
#  [1, 2, 3]
#  >>> row.already_sent
#  [6, 6, 6]
#  >>> row.to_send
#  [0, 0, 5]
#
#  >>> GetMDAProbes(round_leq=2, adaptive_eps=False).execute(url, 'test_star_node_star')
#  []
#      """
