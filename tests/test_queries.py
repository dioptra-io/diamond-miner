def test_get_links_nsdi():
    """
    >>> from diamond_miner.test import client
    >>> from diamond_miner.queries import GetLinks
    >>> rows = GetLinks().execute(client, 'test_nsdi_example')
    >>> links = [(row["near_addr"], row["far_addr"]) for row in rows]
    >>> sorted(links)[:3]
    [('::ffff:150.0.1.1', '::ffff:150.0.2.1'), ('::ffff:150.0.1.1', '::ffff:150.0.3.1'), ('::ffff:150.0.2.1', '::ffff:150.0.4.1')]
    >>> sorted(links)[3:6]
    [('::ffff:150.0.3.1', '::ffff:150.0.5.1'), ('::ffff:150.0.3.1', '::ffff:150.0.7.1'), ('::ffff:150.0.4.1', '::ffff:150.0.6.1')]
    >>> sorted(links)[6:]
    [('::ffff:150.0.5.1', '::ffff:150.0.6.1'), ('::ffff:150.0.7.1', '::ffff:150.0.6.1')]
    """


def test_get_links_multi_protocol():
    """
    >>> from diamond_miner.test import client
    >>> from diamond_miner.queries import GetLinks
    >>> rows = GetLinks(probe_protocol=1).execute(client, 'test_multi_protocol')
    >>> links = [(row["near_addr"], row["far_addr"]) for row in rows]
    >>> sorted(links)
    [('::ffff:150.0.0.1', '::ffff:150.0.1.1'), ('::ffff:150.0.0.2', '::ffff:150.0.1.1')]
    >>> rows = GetLinks(probe_protocol=17).execute(client, 'test_multi_protocol')
    >>> links = [(row["near_addr"], row["far_addr"]) for row in rows]
    >>> sorted(links)
    [('::ffff:150.0.0.1', '::ffff:150.0.1.1')]
    """


def test_get_mda_probes_nsdi():
    """
    >>> from diamond_miner.test import client
    >>> from diamond_miner.queries import GetMDAProbes
    >>> row = GetMDAProbes(round_leq=1, adaptive_eps=False).execute(client, 'test_nsdi_lite')[0]
    >>> row["probe_dst_prefix"]
    '::ffff:200.0.0.0'
    >>> row["TTLs"]
    [1, 2, 3, 4]
    >>> row["cumulative_probes"]
    [11, 11, 11, 11]

    >>> row = GetMDAProbes(round_leq=2, adaptive_eps=False).execute(client, 'test_nsdi_lite')[0]
    >>> row["probe_dst_prefix"]
    '::ffff:200.0.0.0'
    >>> row["TTLs"]
    [1, 2, 3, 4]
    >>> row["cumulative_probes"]
    [11, 16, 16, 16]

    >>> row = GetMDAProbes(round_leq=3, adaptive_eps=False).execute(client, 'test_nsdi_lite')[0]
    >>> row["probe_dst_prefix"]
    '::ffff:200.0.0.0'
    >>> row["TTLs"]
    [1, 2, 3, 4]
    >>> row["cumulative_probes"]
    [11, 16, 16, 16]
    """


# TODO: Rename to test_get_mda_probes_nsdi...
def test_get_mda_probes_stateful_nsdi():
    """
    >>> from diamond_miner.test import client
    >>> from diamond_miner.queries import GetMDAProbes
    >>> row = GetMDAProbes(round_leq=1, adaptive_eps=False).execute(client, 'test_nsdi_lite')[0]
    >>> row["probe_dst_prefix"]
    '::ffff:200.0.0.0'
    >>> row["TTLs"]
    [1, 2, 3, 4]
    >>> row["cumulative_probes"]
    [11, 11, 11, 11]
    >>> row = GetMDAProbes(round_leq=2, adaptive_eps=False).execute(client, 'test_nsdi_lite')[0]
    >>> row["probe_dst_prefix"]
    '::ffff:200.0.0.0'
    >>> row["TTLs"]
    [1, 2, 3, 4]
    >>> row["cumulative_probes"]
    [11, 16, 16, 16]
    >>> row = GetMDAProbes(round_leq=3, adaptive_eps=False).execute(client, 'test_nsdi_lite')[0]
    >>> row["probe_dst_prefix"]
    '::ffff:200.0.0.0'
    >>> row["TTLs"]
    [1, 2, 3, 4]
    >>> row["cumulative_probes"]
    [11, 16, 16, 16]
    """


# TODO: Make this test pass
#  def test_get_mda_probes_star():
#  """
#  With the current links computation (in GetLinksFromView), we do not emit a link
#  in the case of *single* reply in a traceroute. For example: * * node * *, does
#  not generate a link. In this case this means that we never see a link including V_7.
#
#  >>> rows = GetMDAProbes(round_leq=1, adaptive_eps=False).execute(client, 'test_star_node_star')
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
#  >>> GetMDAProbes(round_leq=2, adaptive_eps=False).execute(client, 'test_star_node_star')
#  []
#      """
