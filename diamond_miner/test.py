import asyncio
from pathlib import Path

from aioch import Client

test_data = Path(__file__).parent / ".." / "data" / "test_data.sql"
test_data_inserted = False


async def insert_test_data(client):
    # We omit the last item, since it's an empty statement:
    # "stmt1;stmt2;".split(";") = ["stmt1", "stmt2", ""]
    for statement in test_data.read_text().split(";")[:-1]:
        await client.execute(statement)


def execute(q, table):
    async def do():
        global test_data_inserted
        client = Client(host="127.0.0.1")
        if not test_data_inserted:
            await insert_test_data(client)
            test_data_inserted = True
        gen = q.execute_iter(client, table)
        return [x async for x in gen]

    return asyncio.run(do())
