import csv

from bisect import bisect_left
from collections import defaultdict, namedtuple
from socket import htonl
from .mda import stopping_point

# From parameters_utils_t.cpp
default_1_round_flows = 6
# NOTE: max_ttl = 30 in probing_options_t.cpp !?
max_ttl = 40

original_cpp_behavior = False
# original_cpp_behavior = True

stopping_points = [stopping_point(k, 0.05) for k in range(1, 65536)]

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
        # TODO: Does this work if we increased the src_port instead at the previous round?
        # TODO: We can make this much faster by reducing the size of stopping_points,
        # or by searching from the left (since the expected k is probably small).
        prev_k = bisect_left(stopping_points, previous_max_flow_per_ttl[ttl])
        max_flow = stopping_points[prev_k]

        # We know the max_flow for the first round (exhaustive scan),
        # so we make sure that we are not below.
        # TODO: Can we possibly be above this (6) at the first round?
        if round == 1:
            max_flow = max(max_flow, default_1_round_flows)

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
            # (a) Reproduce (incorrect?) C++ code behavior.
            if original_cpp_behavior:
                # Possible bug in C++ code since the range of
                # std::max_element is [first, last).
                flows = [flows_per_ttl.get(ttl - 1, 0)]
                if ttl < max_ttl:
                    flows.append(flows_per_ttl.get(ttl, 0))

            # (b) Fixed behavior.
            else:
                flows = [flows_per_ttl.get(ttl - 1, 0), flows_per_ttl.get(ttl, 0)]
                if ttl < max_ttl:
                    flows.append(flows_per_ttl.get(ttl + 1, 0))

            n_to_send = max(flows)
            dominant_ttl = ttl - 1 + flows.index(n_to_send)
        # Otherwise, only look at the nodes if there are no links
        else:
            n_to_send = flows_per_ttl[ttl]
            dominant_ttl = ttl

        is_per_flow_needed = False

        if original_cpp_behavior:
            # Possible bug in C++ code? (break before setting remaining_flow_to_send)
            remaining_flow_to_send = 0
        else:
            remaining_flow_to_send = n_to_send

        for flow_id in range(n_to_send):
            dst_ip_in_24 = real_previous_max_flow_per_ttl[dominant_ttl] + flow_id

            # TODO: Move this out of the loop?
            if (max_src_port > options.sport) or dst_ip_in_24 > 255:
                is_per_flow_needed = True
                break

            writer.writerow(
                [
                    htonl(src_ip),
                    htonl(dst_prefix + 1 + dst_ip_in_24),
                    options.sport,
                    options.dport,
                    ttl,
                ]
            )

            # TODO: remaining_flow_to_send -= 1 ?
            remaining_flow_to_send = n_to_send - (flow_id + 1)

        if is_per_flow_needed:
            print(f"Per flow needed for {dst_ip} at TTL={ttl}")
            if (min_dst_port != options.dport) or (max_dst_port != options.dport):
                # // NAT, so nothing to play with ports.
                print("NAT, so nothing to play with ports")
                return

            for flow_id in range(remaining_flow_to_send):
                writer.writerow(
                    [
                        htonl(src_ip),
                        htonl(dst_ip),
                        max_src_port + flow_id + 1,
                        options.dport,
                        ttl,
                    ]
                )
