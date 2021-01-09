from collections import defaultdict

from diamond_miner_core.database import (
    query_max_ttl,
    query_next_round,
    query_discoveries_per_ttl,
)
from diamond_miner_core.flush import flush_format, flush_traceroute


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
                    flush_format(
                        dst_ip,
                        measurement_parameters.source_port,
                        measurement_parameters.destination_port,
                        ttl,
                    )
                )


def next_round(
    database_host: str,
    table_name: str,
    measurement_parameters,
    mapper,
    writer,
    skip_unpopulated_ttl=False,
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
    # max_flow_per_ttl_previous_round = defaultdict(int)
    nodes_per_ttl = defaultdict(list)
    links_per_ttl = defaultdict(list)

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
        dst_ip,
        src_port,
        dst_port,
        nodes,
        links,
        max_round,
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
            if current_max_dst_ip < dst_ip:
                current_max_dst_ip = dst_ip
            if current_max_src_port < src_port:
                current_max_src_port = src_port
            if current_max_round < max_round:
                current_max_round = max_round
            if current_min_dst_port > dst_port:
                current_min_dst_port = dst_port
            if current_max_dst_port < dst_port:
                current_max_dst_port = dst_port
        else:
            # We are beginning to compute a new prefix
            # So we can flush the previous prefix.
            flush_traceroute(
                current_prefix,
                current_max_dst_ip,
                current_min_dst_port,
                current_max_dst_port,
                current_max_src_port,
                current_max_round,
                nodes_per_ttl,
                links_per_ttl,
                max_flow_per_ttl,
                measurement_parameters,
                mapper,
                writer,
                ttl_skipped,
            )

            # Initialize the variables again
            current_prefix = dst_prefix
            current_max_dst_ip = dst_ip
            current_max_src_port = src_port
            current_min_dst_port = dst_port
            current_max_dst_port = dst_port
            current_max_round = max_round
            max_flow_per_ttl = defaultdict(int)
            # max_flow_per_ttl_previous_round = defaultdict(int)
            nodes_per_ttl = defaultdict(list)
            links_per_ttl = defaultdict(list)

        for s_node, d_node in links:
            s_reply_ip, s_ttl, s_round = s_node
            d_reply_ip, d_ttl, d_round = d_node

            # TODO `absolute_max_ttl` or `max_ttl` ?
            if s_ttl > absolute_max_ttl:
                continue

            # Compute the maximum flow from the `max_dst_ip`
            # NOTE We don't take into account the `src_port` (for now)
            # to avoid issues due to NAT source port re-writing
            max_flow = mapper.flow_id(dst_ip - dst_prefix, dst_prefix)
            if max_flow_per_ttl[s_ttl] < max_flow:
                max_flow_per_ttl[s_ttl] = max_flow

            if max_flow_per_ttl[d_ttl] < max_flow:
                max_flow_per_ttl[d_ttl] = max_flow

            # if s_round < measurement_parameters.round_number:
            #     max_flow_previous_round = mapper.flow_id(dst_ip - dst_prefix, dst_prefix)  # noqa
            #     if max_flow_per_ttl_previous_round[s_ttl] < max_flow_previous_round:
            #         max_flow_per_ttl_previous_round[s_ttl] = max_flow_previous_round
            #
            # if d_round < measurement_parameters.round_number:
            #     max_flow_previous_round = mapper.flow_id(dst_ip - dst_prefix, dst_prefix)  # noqa
            #     if max_flow_per_ttl_previous_round[d_ttl] < max_flow_previous_round:
            #         max_flow_per_ttl_previous_round[d_ttl] = max_flow_previous_round

            links_per_ttl[s_ttl].append(((s_reply_ip, s_round), (d_reply_ip, d_round)))

        for node, ttl, round in nodes:
            # TODO `absolute_max_ttl` or `max_ttl` ?
            if ttl > absolute_max_ttl:
                continue

            # Compute the maximum flow from the `max_dst_ip`
            # NOTE We don't take into account the `src_port` (for now)
            # to avoid issues due to NAT source port re-writing
            max_flow = mapper.flow_id(dst_ip - dst_prefix, dst_prefix)
            if max_flow_per_ttl[ttl] < max_flow:
                max_flow_per_ttl[ttl] = max_flow

            # # Compute the max flow for the previous round (necessary in flush traceroute)  # noqa
            # if round < measurement_parameters.round_number:
            #     max_flow_previous_round = mapper.flow_id(dst_ip - dst_prefix, dst_prefix)  # noqa
            #     if max_flow_per_ttl_previous_round[ttl] < max_flow_previous_round:
            #         max_flow_per_ttl_previous_round[ttl] = max_flow_previous_round

            nodes_per_ttl[ttl].append((node, round))

    # Flush the last prefix
    flush_traceroute(
        current_prefix,
        current_max_dst_ip,
        current_min_dst_port,
        current_max_dst_port,
        current_max_src_port,
        current_max_round,
        nodes_per_ttl,
        links_per_ttl,
        max_flow_per_ttl,
        measurement_parameters,
        mapper,
        writer,
        ttl_skipped,
    )
