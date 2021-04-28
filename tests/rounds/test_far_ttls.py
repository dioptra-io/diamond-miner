from ipaddress import ip_address

from diamond_miner.rounds.far_ttls import far_ttls_probes


def collect(f):
    res = []
    for xs in f:
        res.extend(xs)
    return res


def test_far_ttls_probes(client):
    table = "test_nsdi_lite"

    probe_dst_prefix = int(ip_address("::ffff:200.0.0.0"))
    probe_src_port = 24000
    probe_dst_port = 33434

    probes = collect(
        far_ttls_probes(
            client=client,
            table=table,
            round_=1,
            probe_src_addr="100.0.0.1",
            far_ttl_min=1,
            far_ttl_max=10,
        )
    )

    # This should generate probes beyond TTL 4, up to 10 (far_ttl_max),
    # for each flow ID previously used.
    target_specs = []
    for ttl in range(5, 11):
        for flow_id in range(0, 6):
            target_specs.append(
                (
                    probe_dst_prefix + flow_id,
                    probe_src_port,
                    probe_dst_port,
                    ttl,
                )
            )

    assert sorted(probes) == sorted(target_specs)
