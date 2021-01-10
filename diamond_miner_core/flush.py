import math

from diamond_miner_core.mda import stopping_point

# from bisect import bisect_left

# NOTE: max_ttl = 30 in probing_options_t.cpp !?
absolute_max_ttl = 40  # TODO Better parameter handling

default_1_round_flows = 6
stopping_points = [stopping_point(k, 0.05) for k in range(1, 65536)]


def compute_next_round_probes_per_ttl(
    topology_state,
    distribution_probes_per_ttl,
    star_nodes_star_per_ttl,
    stopping_points,
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
        total_probes_ttl = sum(distribution_probes_per_ttl[ttl].values())
        for node, (n_successors, n_probes) in topology_state[ttl].items():
            # Check if the node has reached its statistical guarantees
            if stopping_points[n_successors] <= n_probes:
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
    dst_prefix,
    dst_ip,
    min_dst_port,
    max_dst_port,
    max_src_port,
    max_round,
    nodes_per_ttl,
    links_per_ttl,
    previous_max_flow_per_ttl,
    measurement_parameters,
    mapper,
    writer,
    ttl_skipped,
):
    # Compute the topology state ah round N
    (
        topology_state,
        distribution_probes_per_ttl,
        star_nodes_star_per_ttl,
        max_successors,
        n_load_balancers,
    ) = compute_topology_state(
        nodes_per_ttl, links_per_ttl, measurement_parameters.round_number
    )

    # Compute the topology state ah round N-1
    (
        topology_state_previous,
        distribution_probes_per_ttl_previous,
        star_nodes_star_per_ttl_previous,
        max_successors_previous,
        n_load_balancers_previous,
    ) = compute_topology_state(
        nodes_per_ttl, links_per_ttl, measurement_parameters.round_number - 1
    )

    # This clause is for avoiding generating new probes * nodes *
    n_nodes_per_ttl = {ttl: len(nodes_per_ttl[ttl]) for ttl in nodes_per_ttl}
    if len(n_nodes_per_ttl) == 0 or max(n_nodes_per_ttl.values()) == 0:
        return

    # Now adapt the epsilon such that it fits to the number of load balancer.
    # epsilon is the probablity to fail to reach statistical guarantees for 1 LB
    # let assume target epsilon (failure probability) is 0.01 (for the whole topology)
    # We have:
    # p(success)  = p(success on all load balancers) = p(sucess_1) * ... * p(success_n)
    # = ((1 - p(fail_1)) * ... * (1 - p(fail_n)) = (1 - epsilon)^n
    # so we have target_epsilon = (1-epsilon)^n. So we compute the epsilon accordingly.
    target_epsilon = 0.05
    if n_load_balancers == 0:
        epsilon = target_epsilon
    else:
        epsilon = 1 - math.exp(math.log(1 - target_epsilon) / n_load_balancers)

    if len(star_nodes_star_per_ttl) > 0:
        max_stopping_point = max(max_successors, max(star_nodes_star_per_ttl.values()))
    else:
        max_stopping_point = max_successors
    stopping_points = [
        stopping_point(k, epsilon) for k in range(1, max_stopping_point + 2)
    ]

    if len(star_nodes_star_per_ttl_previous) > 0:
        max_stopping_point_previous = max(
            max_successors_previous, max(star_nodes_star_per_ttl_previous.values())
        )
    else:
        max_stopping_point_previous = max_successors_previous

    if measurement_parameters.round_number == 1:
        epsilon_previous_round = target_epsilon
        stopping_points_previous = [
            stopping_point(k, epsilon_previous_round)
            for k in range(1, max_stopping_point_previous + 3)
        ]
    else:
        if n_load_balancers_previous == 0:
            epsilon_previous_round = target_epsilon
        else:
            epsilon_previous_round = 1 - math.exp(
                math.log(1 - target_epsilon) / n_load_balancers_previous
            )
        stopping_points_previous = [
            stopping_point(k, epsilon_previous_round)
            for k in range(1, max_stopping_point_previous + 2)
        ]

    probes_per_ttl = compute_next_round_probes_per_ttl(
        topology_state,
        distribution_probes_per_ttl,
        star_nodes_star_per_ttl,
        stopping_points,
    )

    # To compute the number of probes previously sent,
    # we need to recompute the state of the topology at round - 1...
    # this should be optimized by keeping it somehwere
    # th = stopping_points[bisect_left(stopping_points_previous_round, previous_max_flow_per_ttl[ttl])]  # noqa
    real_previous_max_flow_per_ttl = {}
    if measurement_parameters.round_number > 1:
        probes_per_ttl_previous_round = compute_next_round_probes_per_ttl(
            topology_state_previous,
            distribution_probes_per_ttl_previous,
            star_nodes_star_per_ttl_previous,
            stopping_points_previous,
        )

        for ttl, probes_to_send in probes_per_ttl.items():
            previous_max_flow = max(probes_per_ttl_previous_round[ttl])
            for i in range(len(probes_to_send)):
                probes_to_send[i] = max(0, probes_to_send[i] - previous_max_flow)
            real_previous_max_flow_per_ttl[ttl] = previous_max_flow
    else:
        # Round 1
        for ttl, probes_to_send in probes_per_ttl.items():
            previous_max_flow = 6
            for i in range(len(probes_to_send)):
                probes_to_send[i] = max(0, probes_to_send[i] - previous_max_flow)
            real_previous_max_flow_per_ttl[ttl] = previous_max_flow

    probes_per_ttl = {ttl: max(probes_per_ttl[ttl]) for ttl in probes_per_ttl}
    for ttl, n_to_send in probes_per_ttl.items():

        # Re-probe unpopulated TTLs avoidance
        if ttl in ttl_skipped:
            continue

        # Generate the next probes to send
        for flow_id in range(0, n_to_send):

            real_flow_id = real_previous_max_flow_per_ttl[ttl] + flow_id
            offset = mapper.offset(real_flow_id, 24, dst_prefix)

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
                    dst_prefix + offset[0],
                    measurement_parameters.source_port + offset[1],
                    measurement_parameters.destination_port,
                    ttl,
                )
            )
