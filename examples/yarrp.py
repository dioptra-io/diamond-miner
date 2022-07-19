import logging
from pathlib import Path
from uuid import uuid4

from pycaracal import Probe, prober
from pych_client import ClickHouseClient

from diamond_miner.format import format_ipv6
from diamond_miner.generators import probe_generator
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
results_filepath = Path("results.csv")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Configure pycaracal
    config = prober.Config()
    config.set_output_file_csv(str(results_filepath))
    config.set_probing_rate(10_000)
    config.set_sniffer_wait_time(1)

    # Generate ICMP probes towards every /24 in 1.0.0.0/22,
    # with a single flow per prefix between TTLs 2-32.
    gen = probe_generator(
        prefixes=[("1.0.0.0/22", "icmp")],
        flow_ids=range(1),
        ttls=range(2, 33),
    )

    # Convert tuples output by `probe_generator` to pycaracal probes.
    probes = (
        Probe(format_ipv6(dst_addr), src_port, dst_port, ttl, protocol, 0)
        for dst_addr, src_port, dst_port, ttl, protocol in gen
    )

    # Send the probes.
    # Note that the probes are randomized and sent on-the-fly,
    # without being buffered in memory.
    prober_stats, sniffer_stats, pcap_stats = prober.probe(config, probes)

    # Display some statistics from pycaracal.
    print(f"{prober_stats.read} probes read")
    print(f"{sniffer_stats.received_count} probes received")

    with ClickHouseClient(**credentials) as client:
        # Insert the results into the database
        CreateTables().execute(client, measurement_id)
        InsertResults().execute(
            client, measurement_id, data=results_filepath.read_bytes()
        )
        InsertPrefixes().execute(client, measurement_id)
        InsertLinks().execute(client, measurement_id)

        # Query the results
        links = GetLinks().execute(client, measurement_id)
        print(f"{len(links)} links discovered")
