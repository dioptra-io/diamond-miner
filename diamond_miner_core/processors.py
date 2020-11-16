from collections import defaultdict
from socket import htonl


from diamond_miner_core.flush import flush_traceroute
from diamond_miner_core.database import query_max_ttl, query_next_round


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
                writer.writerow(
                    [
                        htonl(src_ip),
                        htonl(dst_ip),
                        measurement_parameters.source_port,
                        measurement_parameters.destination_port,
                        ttl,
                    ]
                )


def next_round(
    database_host: str, table_name: str, measurement_parameters, mapper, writer
):
    """Compute the next round."""

    absolute_max_ttl = 40  # TODO Better parameter handling

    current_prefix = None
    current_max_dst_ip = 0
    current_max_src_port = 0
    current_min_dst_port = measurement_parameters.destination_port
    current_max_dst_port = measurement_parameters.destination_port
    current_max_round = 0

    max_flow_per_ttl = defaultdict(int)

    nodes_per_ttl = defaultdict(int)
    links_per_ttl = defaultdict(int)

    for (
        src_ip,
        dst_prefix,
        max_dst_ip,
        ttl,
        n_links,
        max_src_port,
        min_dst_port,
        max_dst_port,
        max_round,
        n_nodes,
    ) in query_next_round(
        database_host,
        table_name,
        measurement_parameters.source_ip,
        measurement_parameters.round_number,
    ):
        # Each iteration is the information of a tuple (src_ip, dst_prefix, dst_ip, ttl)
        if not current_prefix:
            # Initialization of the current prefix
            current_prefix = dst_prefix

        if current_prefix == dst_prefix:
            # The computation for the prefix is not finished yet
            # So we update the current variables
            if current_max_dst_ip < max_dst_ip:
                current_max_dst_ip = max_dst_ip
            if current_max_src_port < max_src_port:
                current_max_src_port = max_src_port
            if current_max_round < max_round:
                current_max_round = max_round
            if current_min_dst_port > min_dst_port:
                current_min_dst_port = min_dst_port
            if current_max_dst_port < max_dst_port:
                current_max_dst_port = max_dst_port
        else:
            # We are beginning to compute a new prefix
            # So we can flush the previous prefix.
            if current_max_round == measurement_parameters.round_number:
                flush_traceroute(
                    current_prefix,
                    current_max_dst_ip,
                    current_min_dst_port,
                    current_max_dst_port,
                    current_max_src_port,
                    nodes_per_ttl,
                    links_per_ttl,
                    max_flow_per_ttl,
                    measurement_parameters,
                    mapper,
                    writer,
                )

            # Initialize the variables again
            current_prefix = dst_prefix
            current_max_dst_ip = max_dst_ip
            current_max_src_port = max_src_port
            current_min_dst_port = min_dst_port
            current_max_dst_port = max_dst_port
            current_max_round = max_round
            max_flow_per_ttl = defaultdict(int)
            nodes_per_ttl = defaultdict(int)
            links_per_ttl = defaultdict(int)

        # TODO `absolute_max_ttl` or `max_ttl` ?
        if ttl > absolute_max_ttl:
            continue

        # Compute the maximum flow from the `max_dst_ip`
        # NOTE We don't take into account the `src_port` (for now)
        # to avoid issues due to NAT source port re-writing
        # TODO The `-1` is to begin to flow 0, is it correct ?
        max_flow_per_ttl[ttl] = mapper.flow_id(max_dst_ip - dst_prefix)

        nodes_per_ttl[ttl] = n_nodes
        links_per_ttl[ttl] = n_links

    # Flush the last prefix
    flush_traceroute(
        current_prefix,
        current_max_dst_ip,
        current_min_dst_port,
        current_max_dst_port,
        current_max_src_port,
        nodes_per_ttl,
        links_per_ttl,
        max_flow_per_ttl,
        measurement_parameters,
        mapper,
        writer,
    )
