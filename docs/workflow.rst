Workflow
========

This library contains three main components:

- Database queries that implements most of the algorithms in ClickHouse SQL.
- Flow mappers, to map between flow IDs and (address, port) offsets.
- Probe generators, to generate randomized probes on-the-fly.

The example below show how to use these components together with `caracal <https://github.com/dioptra-io/caracal>`_
in order to discover load-balanced paths.
Refer to the reference section of the documentation to learn more about the different functions available.

For a more complex example, to handle measurements with billion of probes and results, see:

- https://github.com/dioptra-io/iris/blob/main/iris/commons/clickhouse.py
- https://github.com/dioptra-io/iris/blob/main/iris/worker/inner_pipeline/diamond_miner.py

.. code-block:: bash

    pip install diamond-miner pycaracal pych-client
    docker run --rm -d -p 8123:8123 clickhouse/clickhouse-server:22.6

.. code-block:: python

    import logging
    from pathlib import Path
    from uuid import uuid4

    from diamond_miner.generators import probe_generator_parallel
    from diamond_miner.insert import insert_mda_probe_counts, insert_probe_counts
    from diamond_miner.queries import (
        CreateTables,
        GetLinks,
        InsertLinks,
        InsertPrefixes,
        results_table,
    )
    from pycaracal import prober
    from pych_client import ClickHouseClient

    # Configuration
    credentials = {
        "base_url": "http://localhost:8123",
        "database": "default",
        "username": "default",
        "password": "",
    }
    measurement_id = str(uuid4())
    probes_filepath = Path("probes.csv.zst")
    results_filepath = Path("results.csv")

    # ICMP traceroute towards every /24 in 1.0.0.0/22 starting with 6 flows per prefix between TTLs 2-32
    prefixes = [("1.0.0.0/22", "icmp", range(2, 33), 6)]

    if __name__ == "__main__":
        logging.basicConfig(level=logging.INFO)
        with ClickHouseClient(**credentials) as client:
            CreateTables().execute(client, measurement_id)
            for round_ in range(1, 10):
                if round_ == 1:
                    # Compute the initial probes
                    insert_probe_counts(
                        client=client,
                        measurement_id=measurement_id,
                        round_=1,
                        prefixes=prefixes,
                    )
                else:
                    # Insert results from the previous round
                    client.execute(
                        "INSERT INTO {table:Identifier} FORMAT CSVWithNames",
                        params={"table": results_table(measurement_id)},
                        data=results_filepath.read_bytes(),
                    )
                    # Insert invalid prefixes and links
                    InsertPrefixes().execute(client, measurement_id)
                    InsertLinks().execute(client, measurement_id)
                    # Compute subsequent probes
                    insert_mda_probe_counts(
                        client=client,
                        measurement_id=measurement_id,
                        previous_round=round_ - 1,
                    )

                # Write the probes to a file
                n_probes = probe_generator_parallel(
                    filepath=probes_filepath,
                    client=client,
                    measurement_id=measurement_id,
                    round_=round_,
                )
                if n_probes == 0:
                    break

                # Send the probes
                config = prober.Config()
                config.set_output_file_csv(str(results_filepath))
                config.set_probing_rate(10_000)
                config.set_sniffer_wait_time(1)
                prober.probe(config, str(probes_filepath))

            links = GetLinks().execute(client, measurement_id)
            print(f"{len(links)} links discovered")
