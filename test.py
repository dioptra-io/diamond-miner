import ipaddress

from reader import next_round, next_max_ttl


measurement_uuid = "9ef5b32d-614a-4ef0-8d2f-b0a78f7c50b3"
agent_uuid = "ddd8541d-b4f5-42ce-b163-e3e9bfcd0a47"


def sanitize(uuid):
    return uuid.replace("-", "_")


if __name__ == "__main__":

    database_host = "127.0.0.1"
    table_name = f"iris.results__{sanitize(measurement_uuid)}__{sanitize(agent_uuid)}"

    source_ip = int(ipaddress.ip_address("132.227.123.9"))
    round_number = 1

    output_file = "resources/reader_ttl_1.csv"

    fd = open(output_file, "a+", newline="")
    next_max_ttl(database_host, table_name, source_ip, round_number, fd)
