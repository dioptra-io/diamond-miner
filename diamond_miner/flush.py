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
                [
                    str(dst_prefix + offset[0]),
                    str(measurement_parameters.source_port + offset[1]),
                    str(measurement_parameters.destination_port),
                    str(ttl),
                ]
            )

    if not rows_to_flush:
        return

    writer.write("".join(["\n".join([",".join(row) for row in rows_to_flush]), "\n"]))
