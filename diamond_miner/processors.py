import ipaddress

from diamond_miner.database import (
    query_discoveries_per_ttl,
    query_max_ttl,
    query_next_round,
)
from diamond_miner.flush import flush_traceroute


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
        skip_prefix,
        d_miner_paper_probes_w_star_nodes_star,
        previous_max_flow_per_ttl,
        min_src_port,
        min_dst_port,
        max_dst_port,
    ) in query_next_round(
        database_host,
        table_name,
        measurement_parameters.source_ip,
        measurement_parameters.round_number,
    ):

        if skip_prefix == 1:
            continue

        if ipaddress.ip_address(dst_prefix).is_private:
            continue

        flush_traceroute(
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
        )
