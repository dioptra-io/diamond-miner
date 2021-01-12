import ipaddress

from diamond_miner_core.database import (
    query_max_ttl,
    query_next_round,
    query_discoveries_per_ttl,
)
from diamond_miner_core.flush import flush_traceroute


def next_max_ttl(database_host: str, table_name: str, measurement_parameters, writer):
    """Compute the next max TTL."""

    absolute_max_ttl = 40  # TODO Better parameter handling

    for (src_ip, dst_ip, max_ttl) in query_max_ttl(
        database_host,
        table_name,
        measurement_parameters.source_ip,
        measurement_parameters.round_number,
    ):
        if max_ttl > absolute_max_ttl:
            continue
        if max_ttl > 20:
            for ttl in range(measurement_parameters.max_ttl + 1, absolute_max_ttl + 1):
                writer.write(
                    # flush_format(
                    ",".join(
                        [
                            str(dst_ip),
                            str(measurement_parameters.source_port),
                            str(measurement_parameters.destination_port),
                            str(ttl),
                        ]
                    )
                    # )
                )


def fill_topology_state(n_probes_per_node, n_links_per_source):
    # Compute topology state
    topology_state = {}
    distribution_probes_per_ttl = {}
    nodes_per_ttl = {}
    n_load_balancers = 0
    max_successors = 0
    for (node, ttl), n_probes in n_probes_per_node:
        nodes_per_ttl.setdefault(ttl, set()).add(node)
        topology_state.setdefault(ttl, {}).setdefault(node, ())
        distribution_probes_per_ttl.setdefault(ttl, 0)
        distribution_probes_per_ttl[ttl] += n_probes
        if (node, ttl) in n_links_per_source:
            n_successors = n_links_per_source[(node, ttl)]
            if n_successors > 1:
                n_load_balancers += 1
            if n_successors > max_successors:
                max_successors = n_successors
            topology_state[ttl][node] = (n_successors, n_probes)
        else:
            topology_state[ttl][node] = (0, n_probes)

        # Not sure we want to continue probing nodes without sucessors...
        # else:
        #     topology_state[ttl][node] = (0, n_probes)
    star_nodes_star = {}
    for ttl, nodes in nodes_per_ttl.items():
        if ttl - 1 not in nodes_per_ttl and ttl + 1 not in nodes:
            star_nodes_star[ttl] = len(nodes)

    return (
        topology_state,
        distribution_probes_per_ttl,
        star_nodes_star,
        n_load_balancers,
        max_successors,
    )


def next_round(
    database_host: str,
    table_name: str,
    measurement_parameters,
    mapper,
    writer,
    skip_unpopulated_ttl=False,
):

    absolute_max_ttl = 40  # TODO Better parameter handling

    # With this manimulation we skip the TTLs that are not very populated to avoid
    # re-probing it extensively (e.g., low TTLs)
    ttl_skipped = set()
    if skip_unpopulated_ttl is True:
        population_threshold = 100
        for ttl, n_discoveries in query_discoveries_per_ttl(
            database_host,
            table_name,
            measurement_parameters.source_ip,
            measurement_parameters.round_number,
            absolute_max_ttl,
        ):
            if n_discoveries < population_threshold:
                ttl_skipped.add(ttl)

    for (
        src_ip,
        dst_prefix,
        nodes_active,
        nodes_active_previous,
        n_probes_per_node,
        n_probes_per_node_previous,
        n_links_per_sources,
        n_links_per_sources_previous,
        min_src_port,
        min_dst_port,
        max_dst_port,
    ) in query_next_round(
        database_host,
        table_name,
        measurement_parameters.source_ip,
        measurement_parameters.round_number,
    ):

        if ipaddress.ip_address(dst_prefix).is_private:
            continue

        n_links_per_sources = dict(n_links_per_sources)
        n_links_per_sources_previous = dict(n_links_per_sources_previous)

        (
            topology_state,
            distribution_probes_per_ttl,
            star_nodes_star_per_ttl,
            n_load_balancers,
            max_successors,
        ) = fill_topology_state(n_probes_per_node, n_links_per_sources)
        (
            topology_state_previous,
            distribution_probes_per_ttl_previous,
            star_nodes_star_previous_per_ttl,
            n_load_balancers_previous,
            max_successors_previous,
        ) = fill_topology_state(
            n_probes_per_node_previous, n_links_per_sources_previous
        )

        flush_traceroute(
            topology_state,
            distribution_probes_per_ttl,
            star_nodes_star_per_ttl,
            n_load_balancers,
            max_successors,
            topology_state_previous,
            distribution_probes_per_ttl_previous,
            star_nodes_star_previous_per_ttl,
            n_load_balancers_previous,
            max_successors_previous,
            set(nodes_active),
            set(nodes_active_previous),
            dst_prefix,
            min_dst_port,
            max_dst_port,
            min_src_port,
            measurement_parameters,
            mapper,
            writer,
            ttl_skipped,
        )
