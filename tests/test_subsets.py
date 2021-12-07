from ipaddress import IPv6Network, ip_network

from diamond_miner.subsets import addr_to_network, split


def test_addr_to_network():
    assert addr_to_network("::ffff:8.8.8.0", 32, 0) == IPv6Network("::ffff:8.8.8.0/128")
    assert addr_to_network("::ffff:0.0.0.0", 0, 0) == IPv6Network("::ffff:0.0.0.0/96")
    assert addr_to_network("dead:beef::", 0, 64) == IPv6Network("dead:beef::/64")


def test_split():
    counts = {ip_network("::ffff:8.8.4.0/120"): 10, ip_network("::ffff:8.8.8.0/120"): 5}
    assert split(counts, 15) == [IPv6Network("::/0")]
    assert split(counts, 10) == [
        IPv6Network("::ffff:8.8.0.0/117"),
        IPv6Network("::ffff:8.8.8.0/117"),
    ]
    assert split({}, 10) == []


# TODO: test_links_subsets
# TODO: test_results_subsets
