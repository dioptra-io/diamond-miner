#!/usr/bin/env python3

import csv
import sys
from diamond_miner_core.database import query_next_round
from ipaddress import ip_address


def sanitize(uuid):
    return uuid.replace("-", "_")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"usage: {sys.argv[0]} measurement_uuid agent_uuid")
        sys.exit(1)

    _, measurement_uuid, agent_uuid = sys.argv
    table_name = f"iris.results__{sanitize(measurement_uuid)}__{sanitize(agent_uuid)}"
    output_file = f"query_next_round_{table_name}.csv"

    print(f"Dumping table {table_name} to {output_file}")

    rows = query_next_round(
        database_host="127.0.0.1",
        table_name=table_name,
        source_ip=int(ip_address("132.227.123.9")),
        round_number=1,
    )

    with open(output_file, "w") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "src_ip",
                "dst_prefix",
                "max(p1.dst_ip)",
                "ttl"
                "n_links"
                "max(src_port)"
                "min(dst_port)"
                "max(dst_port)"
                "max(round)"
                "n_nodes",
            ]
        )
        writer.writerows(rows)
