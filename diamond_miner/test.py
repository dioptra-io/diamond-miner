from pathlib import Path

from clickhouse_driver import Client

test_data = Path(__file__).parent / ".." / "tests" / "test_data.sql"


def get_test_data():
    # We omit the last item, since it's an empty statement:
    # "stmt1;stmt2;".split(";") = ["stmt1", "stmt2", ""]
    return test_data.read_text().split(";")[:-1]


def execute(q, table=None):
    if not hasattr(execute, "test_data_inserted"):
        execute.test_data_inserted = False

    client = Client(host="127.0.0.1")
    if not execute.test_data_inserted:
        for statement in get_test_data():
            client.execute(statement)
        execute.test_data_inserted = True
    if isinstance(q, str):
        return client.execute(q)
    return q.execute(client, table)
