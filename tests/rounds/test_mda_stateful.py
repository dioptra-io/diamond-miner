from ipaddress import ip_address

from diamond_miner.defaults import DEFAULT_PREFIX_SIZE_V4, DEFAULT_PREFIX_SIZE_V6

# TODO: Merge with test_mda?
from diamond_miner.mappers import SequentialFlowMapper
from diamond_miner.queries.delete_probes import DeleteProbes
from diamond_miner.rounds.mda_stateful import mda_probes_stateful


def test_mda_probes_lite(url):
    table = "test_nsdi_lite"
    probe_dst_prefix = int(ip_address("::ffff:200.0.0.0"))

    probe_src_port = 24000
    probe_dst_port = 33434

    def probes_for_round(round_):
        return list(
            mda_probes_stateful(
                url=url,
                measurement_id=table,
                round_=round_,
                mapper_v4=SequentialFlowMapper(prefix_size=DEFAULT_PREFIX_SIZE_V4),
                mapper_v6=SequentialFlowMapper(prefix_size=DEFAULT_PREFIX_SIZE_V6),
                probe_src_port=probe_src_port,
                probe_dst_port=probe_dst_port,
                adaptive_eps=False,
            )
        )

    DeleteProbes(round_geq=2).execute(url, table)

    # Round 1 -> 2, 5 probes at TTL 1-4
    target_specs = []
    for ttl in range(1, 5):
        for flow_id in range(6, 6 + 5):
            target_specs.append(
                (
                    probe_dst_prefix + flow_id,
                    probe_src_port,
                    probe_dst_port,
                    ttl,
                    "icmp",
                )
            )

    assert sorted(probes_for_round(1)) == sorted(target_specs)

    # Round 2 -> 3, 5 probes at TTL 2-4
    target_specs = []
    for ttl in range(2, 5):
        for flow_id in range(11, 11 + 5):
            target_specs.append(
                (
                    probe_dst_prefix + flow_id,
                    probe_src_port,
                    probe_dst_port,
                    ttl,
                    "icmp",
                )
            )

    assert sorted(probes_for_round(2)) == sorted(target_specs)

    # Round 3 -> 4, 0 probes
    assert probes_for_round(3) == []
