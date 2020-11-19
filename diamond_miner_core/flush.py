from diamond_miner_core.mda import stopping_point

from bisect import bisect_left
from socket import htonl


# NOTE: max_ttl = 30 in probing_options_t.cpp !?
absolute_max_ttl = 40  # TODO Better parameter handling

default_1_round_flows = 6
stopping_points = [stopping_point(k, 0.05) for k in range(1, 65536)]


def flush_format(dst_ip, src_port, dst_port, ttl):
    return [f"{dst_ip:010}", f"{src_port:05}", f"{dst_port:05}", f"{ttl:03}"]


def flush_traceroute(
    dst_prefix,
    dst_ip,
    min_dst_port,
    max_dst_port,
    max_src_port,
    nodes_per_ttl,
    links_per_ttl,
    previous_max_flow_per_ttl,
    measurement_parameters,
    mapper,
    writer,
):
    # Number of flows to send at `ttl`.
    flows_per_ttl = {}

    # Recovered max_flow at `ttl` from the previous `round`.
    real_previous_max_flow_per_ttl = {}

    # TODO: Factor #flows computation out.

    # TODO: `absolute_max_ttl` or `max_ttl` ?
    for ttl in range(absolute_max_ttl):
        # Skip this TTL if there are no nodes or links.
        if links_per_ttl[ttl] == 0 and nodes_per_ttl[ttl] == 0:
            continue

        # Recover the number of flows sent during the previous round.
        # It is the n_k closest (above) to the maximum destination IP for
        # which a reply was received in the previous round.
        # e.g. max_dst_ip = 5 => closest n_k = n_1 = 6.
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

    # TODO: `absolute_max_ttl` or `max_ttl` ?
    for ttl in range(absolute_max_ttl):
        # TODO: This is contradictory with the previous loop,
        # where we assign a value to flows_per_ttl without
        # checking if nodes_per_ttl[ttl] == 0.
        if nodes_per_ttl[ttl] == 0:
            continue

        # If there is at least one link.
        if links_per_ttl.get(ttl, 0) > 0 or links_per_ttl.get(ttl - 1, 0) > 0:
            flows = [
                flows_per_ttl.get(ttl - 1, 0),
                flows_per_ttl.get(ttl, 0),
                flows_per_ttl.get(ttl + 1, 0),  # TODO Check with paper
            ]

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

            if (
                offset[1] > 0
                and (min_dst_port != measurement_parameters.source_port)
                or (max_dst_port != measurement_parameters.destination_port)
            ):
                # There is a case where max_src_port > sport,
                # but real_flow_id < 255 (see dst_prefix == 28093440)
                # It's probably NAT, nothing to do more
                continue

            writer.writerow(
                flush_format(
                    htonl(dst_prefix + offset[0]),
                    measurement_parameters.source_port + offset[1],
                    measurement_parameters.destination_port,
                    ttl,
                )
            )
