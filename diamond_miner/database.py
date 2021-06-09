from collections import defaultdict
from dataclasses import dataclass
from ipaddress import ip_network
from typing import Any, Dict, Tuple

from aioch import Client

from diamond_miner.queries import (
    CreateFlowsView,
    CreateLinksTable,
    CreatePrefixesTable,
    CreateResultsTable,
    GetLinksFromView,
    GetPrefixesWithAmplification,
    GetPrefixesWithLoops,
    flows_table,
    links_table,
    prefixes_table,
    results_table,
)

# NOTE: Should we move these function to dedicated queries (CreateTables, ...)?


async def create_tables(client: Client, measurement_id: str) -> None:
    await CreateResultsTable().execute_async(client, measurement_id)
    await CreateFlowsView().execute_async(client, measurement_id)
    await CreateLinksTable().execute_async(client, measurement_id)
    await CreatePrefixesTable().execute_async(client, measurement_id)


async def drop_tables(client: Client, measurement_id: str) -> None:
    await client.execute(f"DROP TABLE IF EXISTS {results_table(measurement_id)}")
    await client.execute(f"DROP TABLE IF EXISTS {flows_table(measurement_id)}")
    await client.execute(f"DROP TABLE IF EXISTS {links_table(measurement_id)}")
    await client.execute(f"DROP TABLE IF EXISTS {prefixes_table(measurement_id)}")


async def insert_links(client: Client, measurement_id: str, **kwargs: Any) -> None:
    await client.execute(
        f"""
        INSERT INTO {links_table(measurement_id)}
        SELECT * FROM ({GetLinksFromView(**kwargs).query(measurement_id)})
        """
    )


async def insert_prefixes(client: Client, measurement_id: str, **kwargs: Any) -> None:
    @dataclass
    class Prefix:
        has_amplification: bool = False
        has_loops: bool = False

    # TODO: IPv6, better subsets based on the number of results
    subsets = list(ip_network("::ffff:0.0.0.0/96").subnets(prefixlen_diff=6))
    prefixes: Dict[Tuple, Prefix] = defaultdict(Prefix)
    rows = GetPrefixesWithAmplification(**kwargs).execute_iter_async(
        client, measurement_id, subsets
    )
    async for prefix in rows:
        prefixes[prefix].has_amplification = True
    rows = GetPrefixesWithLoops(**kwargs).execute_iter_async(
        client, measurement_id, subsets
    )
    async for prefix in rows:
        prefixes[prefix].has_loops = True
    await client.execute(
        f"""
        INSERT INTO {prefixes_table(measurement_id)}
        (probe_protocol, probe_src_addr, probe_dst_prefix, has_amplification, has_loops)
        VALUES
        """,
        [
            {
                "probe_protocol": probe_protocol,
                "probe_src_addr": probe_src_addr,
                "probe_dst_prefix": probe_dst_prefix,
                "has_amplification": prefix.has_amplification,
                "has_loops": prefix.has_loops,
            }
            for (
                probe_protocol,
                probe_src_addr,
                probe_dst_prefix,
            ), prefix in prefixes.items()
        ],
    )
