import csv

from reader.flow import SequentialFlowMapper
from reader.mda import stopping_point

from bisect import bisect_left
from collections import namedtuple
from socket import htonl


default_1_round_flows = 6
# NOTE: max_ttl = 30 in probing_options_t.cpp !?
max_ttl = 40

stopping_points = [stopping_point(k, 0.05) for k in range(1, 65536)]
mapper = SequentialFlowMapper()

Options = namedtuple("Options", ("sport", "dport"))


def flush_traceroute(
    round,  # Previous round number
    src_ip,
    dst_prefix,
    dst_ip,
    min_dst_port,
    max_dst_port,
    max_src_port,
    nodes_per_ttl,
    links_per_ttl,
    previous_max_flow_per_ttl,
    options,
    ostream,
):
    # Number of flows to send at `ttl`.
    flows_per_ttl = {}

    # Recovered max_flow at `ttl` from the previous `round`.
    real_previous_max_flow_per_ttl = {}

    # TODO: Factor #flows computation out.
    # TODO: Factor IO out of this function.
    writer = csv.writer(ostream, delimiter=",", lineterminator="\n")

    for ttl in range(max_ttl):
        # Skip this TTL if there are no nodes or links.
        if links_per_ttl[ttl] == 0 and nodes_per_ttl[ttl] == 0:
            continue

        # Recover the number of flows sent during the previous round.
        # It is the n_k closest (above) to the maximum destination IP for
        # which a reply was received in the previous round.
        # e.g. max_dst_ip = 5 => closest n_k = n_1 = 6.
        # TODO: Does this work if we increased the src_port instead at the prev round ?
        # TODO: We can make this much faster by reducing the size of stopping_points,
        # or by searching from the left (since the expected k is probably small).
        prev_k = bisect_left(stopping_points, previous_max_flow_per_ttl[ttl])
        max_flow = stopping_points[prev_k]

        # We know the max_flow for the first round (exhaustive scan),
        # so we make sure that we are not below.
        # TODO: Can we possibly be above this (6) at the first round?
        if round == 1:
            max_flow = default_1_round_flows

        real_previous_max_flow_per_ttl[ttl] = max_flow

        # Compute the number of flows to send.
        if links_per_ttl[ttl] == 0:
            flows_per_ttl[ttl] = stopping_points[nodes_per_ttl[ttl]] - max_flow
        else:
            flows_per_ttl[ttl] = stopping_points[links_per_ttl[ttl]] - max_flow

    for ttl in range(max_ttl):
        # TODO: This is contradictory with the previous loop,
        # where we assign a value to flows_per_ttl without
        # checking if nodes_per_ttl[ttl] == 0.
        if nodes_per_ttl[ttl] == 0:
            continue

        # If there is at least one link.
        if links_per_ttl.get(ttl, 0) > 0 or links_per_ttl.get(ttl - 1, 0) > 0:
            flows = [flows_per_ttl.get(ttl - 1, 0), flows_per_ttl.get(ttl, 0)]
            if ttl < max_ttl:
                flows.append(flows_per_ttl.get(ttl + 1, 0))

            n_to_send = max(flows)
            dominant_ttl = ttl - 1 + flows.index(n_to_send)
        # Otherwise, only look at the nodes if there are no links
        else:
            n_to_send = flows_per_ttl[ttl]
            dominant_ttl = ttl

        # Generate the next probes to send
        for flow_id in range(n_to_send):

            real_flow_id = real_previous_max_flow_per_ttl[dominant_ttl] + flow_id
            offset = mapper.offset(real_flow_id, 24)

            if (min_dst_port != options.dport) or (max_dst_port != options.dport):
                # TODO: Do this check before ?
                # There is a case where max_src_port > sport,
                # but real_flow_id < 255 (see dst_prefix == 28093440)
                # It's probably NAT, nothing to do more
                break

            writer.writerow(
                [
                    htonl(src_ip),
                    htonl(dst_prefix + 1 + offset[0]),
                    options.sport + offset[1],
                    options.dport,
                    ttl,
                ]
            )
