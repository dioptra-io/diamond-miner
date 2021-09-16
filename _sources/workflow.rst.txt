Workflow
========

This library contains three main components:

- Database queries that implements most of the algorithms in ClickHouse SQL.
- Flow mappers, to map between flow IDs and (address, port) offsets.
- Probe generators, to generate randomized probes on-the-fly.

The example below show how to use these components together with `caracal <https://github.com/dioptra-io/caracal>`_
in order to discover load-balanced paths.
Refer to the reference section of the documentation to learn more about the different functions available.

.. code-block:: python

    import logging
    import random
    import subprocess

    from diamond_miner.generator import probe_generator_by_flow
    from diamond_miner.mappers import RandomFlowMapper
    from diamond_miner.rounds.mda import mda_probes
    from diamond_miner.queries import (
        CreateTables,
        GetLinks,
        InsertLinks,
        InsertPrefixes,
        results_table,
    )
    from pycaracal import cast_addr, make_probe, prober


    def bulk_insert_results(database, host, measurement_id, csv_path):
        """Insert results CSV into ClickHouse."""
        cmd = f"""
        clickhouse-client \
            --database={database} \
            --host={host} \
            --query='INSERT INTO {results_table(measurement_id)} FORMAT CSV' \
        < {csv_path}
        """
        subprocess.run(cmd, check=True, shell=True)


    def make_probes(generator):
        """Create pycaracal probes."""
        return (
            make_probe(cast_addr(dst_addr), src_port, dst_port, ttl, protocol)
            for dst_addr, src_port, dst_port, ttl, protocol in generator
        )


    # Configuration
    database_url = "clickhouse://localhost/default"
    measurement_id = "sample_measurement"
    probing_rate = 10_000 # packets per second
    prefix_len_v4 = 24
    prefix_len_v6 = 64
    mapper_v4 = RandomFlowMapper(seed=2021, prefix_size=2**(32 - prefix_len_v4))
    mapper_v6 = RandomFlowMapper(seed=2021, prefix_size=2**(128 - prefix_len_v6))

    # Enable logging
    logging.basicConfig(level=logging.INFO)

    # Create the results/prefixes/flows/links tables
    CreateTables().execute(database_url, measurement_id)

    # Round 1
    # ICMP traceroute towards every /24 in 1.0.0.0/22 with 6 flows per prefix between TTLs 2-32
    generator = probe_generator_by_flow(
        prefixes=[("1.0.0.0/16", "icmp", range(2, 32))],
        prefix_len_v4=prefix_len_v4,
        prefix_len_v6=prefix_len_v6,
        mapper_v4=mapper_v4,
        mapper_v6=mapper_v6,
        flow_ids=range(6),
    )

    # Send the probes
    config = prober.Config()
    config.set_output_file_csv("round_1.csv")  # use .csv.zst to enable compression
    config.set_probing_rate(probing_rate)
    prober.probe(config, make_probes(generator))

    # Round 2+
    for round_ in range(1, 10):
        bulk_insert_results("default", "localhost", measurement_id, f"round_{round_}.csv")
        # See `execute_concurrent` and `subsets_for` to insert replies in parallel.
        InsertPrefixes(round_eq=round_).execute(database_url, measurement_id)
        InsertLinks(round_eq=round_).execute(database_url, measurement_id)

        # Generate round n+1 probes from round n replies.
        probes = list(
            mda_probes(
                database_url,
                measurement_id,
                mapper_v4=mapper_v4,
                mapper_v6=mapper_v6,
                round_=round_,
            )
        )
        if not probes:
            break

        # Use `mda_parallel` when the number of prefixes/probes is large in order to
        # shuffle the probes on disk.
        random.shuffle(probes)

        # Send the probes
        config = prober.Config()
        config.set_output_file_csv(f"round_{round_+1}.csv")
        config.set_probing_rate(probing_rate)
        prober.probe(config, make_probes(probes))

    links = GetLinks().execute(database_url, measurement_id)
    print(f"{len(links)} links discovered")
