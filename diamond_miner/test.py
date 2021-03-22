import asyncio
from pathlib import Path

from aioch import Client

test_data = Path(__file__).parent / ".." / "data" / "test_data.sql"


async def insert_test_data(client):
    # We omit the last item, since it's an empty statement:
    # "stmt1;stmt2;".split(";") = ["stmt1", "stmt2", ""]
    for statement in test_data.read_text().split(";")[:-1]:
        await client.execute(statement)


def execute(q, table):
    if not hasattr(execute, "test_data_inserted"):
        execute.test_data_inserted = False

    async def do():
        client = Client(host="127.0.0.1")
        if not execute.test_data_inserted:
            await insert_test_data(client)
            execute.test_data_inserted = True
        return await q.execute(client, table)

    return asyncio.run(do())
