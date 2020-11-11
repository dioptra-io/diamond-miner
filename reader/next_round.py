from collections import defaultdict

from reader.flush import flush_traceroute, Options
from reader.links import links


def next_round(
    database_host: str, table_name: str, source_ip: int, round_number: int, ostream
):
    """Compute the next round."""

    # TODO Parameters
    sport = 24000
    dport = 33434
    max_ttl = 40

    current_prefix = None
    current_max_dst_ip = 0
    current_max_src_port = 0
    current_min_dst_port = dport
    current_max_dst_port = dport
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
    ) in links(database_host, table_name, source_ip, round_number):
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
            if current_max_round == round_number:
                flush_traceroute(
                    round_number,
                    source_ip,
                    current_prefix,
                    current_max_dst_ip,
                    current_min_dst_port,
                    current_max_dst_port,
                    current_max_src_port,
                    nodes_per_ttl,
                    links_per_ttl,
                    max_flow_per_ttl,
                    Options(sport, dport),
                    ostream,
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

        if ttl > max_ttl:
            continue

        # Compute the maximum flow from the `max_dst_ip`
        max_flow = max_dst_ip - (dst_prefix + 1)
        if round_number == 1:
            if max_flow < 6:
                max_flow = 6

        max_flow_per_ttl[ttl] = max_flow

        nodes_per_ttl[ttl] = n_nodes
        links_per_ttl[ttl] = n_links

    # Flush the last prefix
    flush_traceroute(
        round_number,
        source_ip,
        current_prefix,
        current_max_dst_ip,
        current_min_dst_port,
        current_max_dst_port,
        current_max_src_port,
        nodes_per_ttl,
        links_per_ttl,
        max_flow_per_ttl,
        Options(sport, dport),
        ostream,
    )
