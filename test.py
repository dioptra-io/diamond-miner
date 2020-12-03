import ipaddress
import sys

from diamond_miner_core import (
    compute_next_round,
    MeasurementParameters,
    RandomFlowMapper,
)


measurement_uuid = "9ef5b32d-614a-4ef0-8d2f-b0a78f7c50b3"
agent_uuid = "ddd8541d-b4f5-42ce-b163-e3e9bfcd0a47"


def sanitize(uuid):
    return uuid.replace("-", "_")


if __name__ == "__main__":

    database_host = "127.0.0.1"
    table_name = f"iris.results__{sanitize(measurement_uuid)}__{sanitize(agent_uuid)}"

    measurement_parameters = MeasurementParameters(
        source_ip=int(ipaddress.ip_address("132.227.123.9")),
        source_port=24000,
        destination_port=33434,
        min_ttl=2,
        max_ttl=30,
        round_number=1,
    )

    try:
        output_file_path = sys.argv[1]
    except IndexError:
        print("Output file path required. Exiting.")
        exit(1)

    mapper = RandomFlowMapper(master_seed=27, n_array=1000)

    compute_next_round(
        database_host,
        table_name,
        measurement_parameters,
        output_file_path,
        mapper=mapper,
        use_max_ttl_feature=False,
    )
