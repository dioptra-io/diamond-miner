import math

from diamond_miner_core.mda import stopping_point

# from bisect import bisect_left

# NOTE: max_ttl = 30 in probing_options_t.cpp !?
absolute_max_ttl = 40  # TODO Better parameter handling

# default_1_round_flows = 6
# stopping_points = [stopping_point(k, 0.05) for k in range(1, 65536)]


def compute_next_round_probes_per_ttl(
    topology_state,
    distribution_probes_per_ttl,
    star_nodes_star_per_ttl,
    stopping_points,
    nodes_active,
):
    # Now that we have the epsilon, compute the number of probes needed per TTL.
    # Recovered max_flow at `ttl` from the previous `round`.
    probes_per_ttl = {}
    for ttl in range(absolute_max_ttl):
        if ttl not in distribution_probes_per_ttl:
            # No probes to send as no replies from this TTL
            continue
        # Number of probes is the number given in the NSDI D-Miner paper
        next_round_probes = []
        total_probes_ttl = distribution_probes_per_ttl[ttl]
        for node, (n_successors, n_probes) in topology_state[ttl].items():
            # Check if the node has reached its statistical guarantees or is not active
            if (
                stopping_points[n_successors] <= n_probes
                or (node, ttl) not in nodes_active
            ):
                next_round_probes.append(0)
                continue
            # The nkv/Dh(v) of the paper
            probes_to_send = stopping_points[n_successors] / (
                n_probes / total_probes_ttl
            )
            next_round_probes.append(int(probes_to_send))
        if not next_round_probes:
            n_next_round_probes = 0
        else:
            n_next_round_probes = max(next_round_probes)
        probes_per_ttl.setdefault(ttl, []).append(n_next_round_probes)
        probes_per_ttl.setdefault(ttl + 1, []).append(n_next_round_probes)

    # Add the * node * pattern
    for ttl, n_nodes in star_nodes_star_per_ttl.items():
        n_next_round_probes = stopping_points[n_nodes]
        # Get number of nodes that were computed in the previous round
        probes_per_ttl.setdefault(ttl, []).append(n_next_round_probes)
    return probes_per_ttl


def compute_topology_state(nodes_per_ttl, links_per_ttl, round):
    # topology_state is a structure with {ttl: {node : (n_successors, probes)}}
    topology_state = {}
    # n_load_balancers compute the number of branching point
    # of the whole topology to adapt the epsilon
    n_load_balancers = 0
    max_successors = 0  # To avoid computing too much nks
    # Needed to compute the distribution of probes
    distribution_probes_per_ttl = {}  # D

    # Structure for * nodes * pattern
    star_nodes_star_per_ttl = {}

    # Filter nodes that were found only at most at this round
    nodes_per_ttl = {
        ttl: [
            (node, node_round)
            for node, node_round in nodes_per_ttl[ttl]
            if node_round <= round
        ]
        for ttl in nodes_per_ttl
    }

    links_per_ttl = {
        ttl: [
            ((s, s_round), (d, d_round))
            for (s, s_round), (d, d_round) in links_per_ttl[ttl]
            if s_round <= round and d_round <= round
        ]
        for ttl in links_per_ttl
    }

    for ttl in range(absolute_max_ttl):
        # Skip this TTL if there are no nodes or links.
        if ttl not in nodes_per_ttl and ttl not in links_per_ttl:
            continue
        # if len(links_per_ttl[ttl]) == 0 and len(nodes_per_ttl[ttl]) == 0:
        #     continue
        # The number of successors is computed per TTL,
        # meaning that we might send more than needed.
        # Compute successors and number of probes per nodes received at this TTL.
        successors_per_node = {}
        if ttl in links_per_ttl:
            for link in links_per_ttl[ttl]:
                successors_per_node.setdefault(link[0][0], set()).add(link[1][0])

        distribution_probes = {}  # Dh in the paper
        if ttl in nodes_per_ttl:
            for node, _ in nodes_per_ttl[ttl]:
                distribution_probes.setdefault(node, 0)
                distribution_probes[node] += 1
                successors_per_node.setdefault(node, set())
        distribution_probes_per_ttl[ttl] = distribution_probes

        n_load_balancers += len(
            [x for x in successors_per_node if len(successors_per_node[x]) > 1]
        )

        # to compute a minimum number of stopping points
        if len(successors_per_node) > 0:
            max_successors = max(
                max_successors,
                max(len(successors_per_node[x]) for x in successors_per_node),
            )
        # Compute the total number of probes at this TTL to compute the distribution
        topology_state_ttl = {
            node: (len(successors_per_node[node]), distribution_probes[node])
            for node, _ in nodes_per_ttl[ttl]
        }
        topology_state[ttl] = topology_state_ttl

        # Compute * nodes * pattern
        if (
            ttl - 1 not in nodes_per_ttl
            and ttl + 1 not in nodes_per_ttl
            and len(nodes_per_ttl[ttl]) > 0
        ):
            star_nodes_star_per_ttl[ttl] = len(nodes_per_ttl[ttl])

    return (
        topology_state,
        distribution_probes_per_ttl,
        star_nodes_star_per_ttl,
        max_successors,
        n_load_balancers,
    )


def flush_format(dst_ip, src_port, dst_port, ttl):
    return [f"{dst_ip:010}", f"{src_port:05}", f"{dst_port:05}", f"{ttl:03}"]



def flush_traceroute(
            d_miner_paper_probes_w_star_nodes_star,
            previous_max_flow_per_ttl,
            dst_prefix,
            min_dst_port,
            max_dst_port,
            min_src_port,
            measurement_parameters,
            mapper,
            writer,
            ttl_skipped,
        ):

    probes_per_ttl = dict(d_miner_paper_probes_w_star_nodes_star)
    previous_max_flow_per_ttl = dict(previous_max_flow_per_ttl)
    if measurement_parameters.round_number == 1:
        for ttl in previous_max_flow_per_ttl:
            previous_max_flow_per_ttl[ttl] = 6

    rows_to_flush = []
    for ttl, n_to_send in probes_per_ttl.items():

        # Re-probe unpopulated TTLs avoidance
        if ttl in ttl_skipped:
            continue

        # Generate the next probes to send
        for flow_id in range(0, n_to_send):

            real_flow_id = previous_max_flow_per_ttl[ttl] + flow_id
            offset = mapper.offset(real_flow_id, 24, dst_prefix)

            if offset[1] > 0 and (
                (min_dst_port != measurement_parameters.destination_port)
                or (max_dst_port != measurement_parameters.destination_port)
                or (min_src_port < measurement_parameters.source_port)
            ):
                # There is a case where max_src_port > sport,
                # but real_flow_id < 255 (see dst_prefix == 28093440)
                # It's probably NAT, nothing to do more
                continue

            rows_to_flush.append(
                # flush_format(
                [
                    str(dst_prefix + offset[0]),
                    str(measurement_parameters.source_port + offset[1]),
                    str(measurement_parameters.destination_port),
                    str(ttl),
                ]
                # )
            )

    if not rows_to_flush:
        return

    writer.write("".join(["\n".join([",".join(row) for row in rows_to_flush]), "\n"]))