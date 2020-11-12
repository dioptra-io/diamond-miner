"""Links generator."""

from clickhouse_driver import Client


def links(database_host, table_name, src_ip, round_number):
    """Generator of links."""
    snapshot = 1  # not used
    # ipv4_split = 4096  # HACK (default value: 64)
    ipv4_split = 64
    ttl_column_name = "ttl_from_udp_length"

    for j in range(0, ipv4_split):
        inf_born = int(j * ((2 ** 32 - 1) / ipv4_split))
        sup_born = int((j + 1) * ((2 ** 32 - 1) / ipv4_split))

        # TODO Excluded prefix ?

        if sup_born > 3758096384:
            # exclude prefixes >= 224.0.0.0 (multicast)
            break

        query = (
            "WITH groupUniqArray((dst_prefix, dst_ip, p1.reply_ip, p2.reply_ip)) as links_per_dst_ip,\n"  # noqa: E501
            "arrayFilter((x->(x.2 != x.4 AND x.3 != x.4 AND x.3!=0  AND x.4 != 0 )), links_per_dst_ip) as core_links_per_dst_ip,\n"  # noqa: E501
            "arrayMap((x->(x.3, x.4)), core_links_per_dst_ip) as core_links_per_prefix,\n"  # noqa: E501
            "arrayDistinct(core_links_per_prefix) as unique_core_links_per_prefix,\n"
            "length(unique_core_links_per_prefix) as n_links,\n"
            "length(groupUniqArray((p1.reply_ip))) as n_nodes \n"
            "SELECT \n"
            "    src_ip, \n"
            "    dst_prefix, \n"
            "    max(p1.dst_ip), \n"
            f"   {ttl_column_name}, \n"
            "    n_links, \n"
            "    max(src_port), \n"
            "    min(dst_port), \n"
            "    max(dst_port), \n"
            "    max(round), \n"
            "    n_nodes \n"
            "FROM \n"
            # Build links by joining on p1.ttl+1 = p2.ttl
            "(\n"
            "    SELECT *\n"
            f"    FROM  {table_name}\n"
            f"    WHERE dst_prefix > {inf_born} AND dst_prefix <= {sup_born} AND round <= {round_number}\n"  # noqa: E501
            f"    AND src_ip = {src_ip} \n"
            f"    AND snapshot = {snapshot} \n"
            ") AS p1 \n"
            "LEFT OUTER JOIN \n"
            "(\n"
            "    SELECT *\n"
            f"    FROM {table_name}\n"
            f"    WHERE dst_prefix > {inf_born} AND dst_prefix <= {sup_born} AND round <=  {round_number}\n"  # noqa: E501
            f"    AND src_ip = {src_ip} \n"
            f"    AND snapshot = {snapshot} \n"
            ") AS p2 ON (p1.src_ip = p2.src_ip) AND (p1.dst_ip = p2.dst_ip) "
            " AND (p1.src_port = p2.src_port) AND (p1.dst_port = p2.dst_port) "
            " AND (p1.round = p2.round) AND (p1.snapshot = p2.snapshot) "
            f" AND (toUInt8(p1.{ttl_column_name} + toUInt8(1)) = p2.{ttl_column_name})\n"  # noqa: E501
            # Exclude dest. prefixes for which no probes have been sent
            # during the previous round (?)
            "WHERE dst_prefix NOT IN (\n"
            "    SELECT DISTINCT(dst_prefix)\n"
            f"   FROM {table_name}\n"
            f"   WHERE dst_prefix > {inf_born} AND dst_prefix <= {sup_born}\n"
            f"   AND src_ip = {src_ip} \n"
            f"   AND snapshot = {snapshot} \n"
            "    GROUP BY (src_ip, dst_prefix)\n"
            f"   HAVING MAX(round) < {round_number - 1}\n"
            ") \n"
            # Exclude dest. prefixes with per-packet load-balancing (?)
            "AND dst_prefix NOT IN (\n"
            "    SELECT distinct(dst_prefix)\n"
            "    FROM \n"
            "    (\n"
            "        SELECT \n"
            "            src_ip, \n"
            "            dst_prefix, \n"
            f"            {ttl_column_name}, \n"
            "            COUNTDistinct(reply_ip) AS n_ips_per_ttl_flow, \n"
            f"            COUNT((src_ip, dst_ip, {ttl_column_name}"
            + ", src_port, dst_port)) AS cnt \n"
            f"        FROM {table_name}\n"
            f"        WHERE dst_prefix > {inf_born} AND dst_prefix <= {sup_born}\n"
            f"        AND src_ip = {src_ip} \n"
            f"        AND snapshot = {snapshot} \n"
            f"        GROUP BY (src_ip, dst_prefix, dst_ip, {ttl_column_name}"
            + ", src_port, dst_port, snapshot)\n"
            "        HAVING (cnt > 2) OR (n_ips_per_ttl_flow > 1)\n"
            "    ) \n"
            "    GROUP BY (src_ip, dst_prefix)\n"
            ")\n"
            f"GROUP BY (src_ip, dst_prefix, {ttl_column_name})\n"
            # Exclude TTLs where there are no nodes and no links (?)
            "HAVING n_links > 1 or (n_links=0 and n_nodes > 1) \n"
            "ORDER BY \n"
            "    dst_prefix ASC, \n"
            f"   {ttl_column_name} ASC\n "
        )

        client = Client(database_host, connect_timeout=1000, send_receive_timeout=6000)
        for row in client.execute_iter(
            query,
            settings={
                "max_block_size": 100000,
                "connect_timeout": 1000,
                "send_timeout": 6000,
                "receive_timeout": 6000,
            },
        ):
            yield row

        # HACK remove it to do the full next round !
        break
