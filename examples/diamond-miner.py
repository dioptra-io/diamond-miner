import logging
from pathlib import Path
from uuid import uuid4

from pycaracal import prober
from pych_client import ClickHouseClient

from diamond_miner.generators import probe_generator_parallel
from diamond_miner.insert import insert_mda_probe_counts, insert_probe_counts
from diamond_miner.queries import (
    CreateTables,
    GetLinks,
    InsertLinks,
    InsertPrefixes,
    InsertResults,
)

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
            logging.info("round=%s", round_)
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
                InsertResults().execute(
                    client, measurement_id, data=results_filepath.read_bytes()
                )
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
            logging.info("n_probes=%s", n_probes)
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
