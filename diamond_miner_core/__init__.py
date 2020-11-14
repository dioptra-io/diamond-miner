import csv

from diamond_miner_core.flow import SequentialFlowMapper, ReverseByteOrderFlowMapper
from diamond_miner_core.processors import next_max_ttl, next_round

from collections import namedtuple


MeasurementParameters = namedtuple(
    "MeasurementParameters",
    (
        "source_ip",
        "source_port",
        "destination_port",
        "min_ttl",
        "max_ttl",
        "round_number",
    ),
)


def compute_next_round(
    database_host: str,
    table_name: str,
    measurement_parameters: MeasurementParameters,
    output_file_path: str,
    mapper=SequentialFlowMapper(),
    use_max_ttl_feature=False,
):
    with open(output_file_path, "w", newline="") as fout:
        writer = csv.writer(fout, delimiter=",", lineterminator="\n")
        if use_max_ttl_feature:
            next_max_ttl(database_host, table_name, measurement_parameters, writer)
        next_round(database_host, table_name, measurement_parameters, mapper, writer)


__all__ = [
    "compute_next_round",
    "MeasurementParameters",
    "SequentialFlowMapper",
    "ReverseByteOrderFlowMapper",
]
__version__ = "0.1.0"
