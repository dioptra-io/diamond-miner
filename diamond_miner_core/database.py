"""Database interface."""

from clickhouse_driver import Client


def query_max_ttl(database_host, table_name, source_ip, round_number):
    """Generator of max TTL database query."""

    snapshot = 1  # NOTE Not currently used
    ipv4_split = 64
    ttl_column_name = "ttl_from_udp_length"

    for j in range(0, ipv4_split):
        inf_born = int(j * ((2 ** 32 - 1) / ipv4_split))
        sup_born = int((j + 1) * ((2 ** 32 - 1) / ipv4_split))

        # TODO Excluded prefixes ?

        if sup_born > 3758096384:
            # exclude prefixes >= 224.0.0.0 (multicast)
            break

        query = (
            "SELECT \n"
            "    src_ip, \n"
            "    dst_ip, \n"
            f"    max({ttl_column_name}) as max_ttl \n"
            "FROM \n"
            "(\n"
            "    SELECT *\n"
            f"   FROM {table_name}\n"
            f"   WHERE dst_prefix > {inf_born} AND dst_prefix <= {sup_born}\n"
            f"   AND round <= {round_number}\n"
            f"   AND src_ip = {source_ip} \n"
            f"   AND snapshot = {snapshot} \n"
            ") \n"
            "WHERE "
            " dst_prefix NOT IN "
            "(\n"
            " SELECT distinct(dst_prefix)\n"
            "    FROM \n"
            "    (\n"
            "        SELECT \n"
            "            src_ip, \n"
            "            dst_prefix, \n"
            "            MAX(round) AS max_round\n"
            f"       FROM {table_name}\n"
            f"       WHERE dst_prefix > {inf_born} AND dst_prefix <= {sup_born}\n"
            f"        AND src_ip = {source_ip} \n"
            f"       AND snapshot = {snapshot} \n"
            "        GROUP BY (src_ip, dst_prefix)\n"
            f"        HAVING max_round < {round_number - 1}\n"
            "    ) \n"
            ") "
            " AND dst_prefix NOT IN (\n"
            " SELECT distinct(dst_prefix)\n"
            "    FROM \n"
            "    (\n"
            "        SELECT \n"
            "            src_ip, \n"
            "            dst_prefix, \n"
            f"            {ttl_column_name}, \n"
            "            COUNTDistinct(reply_ip) AS n_ips_per_ttl_flow, \n"
            f"            COUNT((src_ip, dst_ip,  {ttl_column_name}, src_port, dst_port)) AS cnt \n"  # noqa: E501
            f"       FROM {table_name}\n"
            f"       WHERE dst_prefix > {inf_born} AND dst_prefix <= {sup_born}\n"
            f"        AND src_ip = {source_ip} \n"
            f"       AND snapshot = {snapshot} \n"
            f"        GROUP BY (src_ip, dst_prefix, dst_ip, {ttl_column_name}, src_port, dst_port, snapshot)\n"  # noqa: E501
            "        HAVING (cnt > 2) OR (n_ips_per_ttl_flow > 1)\n"
            "    ) \n"
            "    GROUP BY (src_ip, dst_prefix)\n"
            "   )"
            " AND dst_ip != reply_ip AND type = 11\n"
            " GROUP BY (src_ip, dst_ip) \n"
        )

        print(query)
        client = Client(database_host, connect_timeout=1000, send_receive_timeout=6000)
        for row in client.execute_iter(
            query,
            settings={
                "max_block_size": 100000,
                "connect_timeout": 1000,
                "send_timeout": 6000,
                "receive_timeout": 6000,
                "read_backoff_min_latency_ms": 100000,
            },
        ):
            yield row


def query_next_round(database_host, table_name, source_ip, round_number):
    """Generator of next round database query."""

    snapshot = 1  # NOTE Not currently used
    ipv4_split = 64
    ttl_column_name = "ttl_from_udp_length"

    for j in range(0, ipv4_split):
        inf_born = int(j * ((2 ** 32 - 1) / ipv4_split))
        sup_born = int((j + 1) * ((2 ** 32 - 1) / ipv4_split))
        # if j < 35:
        #     continue
        print(j)
        # TODO Excluded prefixed ?

        if sup_born > 3758096384:
            # exclude prefixes >= 224.0.0.0 (multicast)
            break

        query = (
            "WITH groupUniqArray((reply_ip, ttl, round)) as replies_s, "
            "arraySort(x->(x.2, x.1), replies_s) as sorted_replies_s, "
            "arrayPopFront(sorted_replies_s) as replies_d, "
            "arrayConcat(replies_d, [(0,0,0)]) as replies_d_sized, "
            "arrayZip(sorted_replies_s, replies_d_sized) as potential_links, "
            "arrayFilter(x->x.1.2 + 1 == x.2.2, potential_links) as links, "
            "max(round) as max_round "
            "SELECT  "
            "    src_ip,  "
            "    dst_prefix,  "
            "    dst_ip,  "
            "    src_port,  "
            "    dst_port,  "
            "    replies_s,  "
            "    links,  "
            "    max_round  "
            f"FROM {table_name}  "
            # Exclude dest. prefixes for which no probes have been sent
            # during the previous round (?)
            # " WHERE 1 = 1 " #
            "WHERE dst_prefix NOT IN ( "
            "    SELECT DISTINCT(dst_prefix) "
            f"   FROM {table_name} "
            f"   WHERE dst_prefix > {inf_born} AND dst_prefix <= {sup_born} "
            f"   AND src_ip = {source_ip}  "
            f"   AND snapshot = {snapshot}  "
            "    GROUP BY (src_ip, dst_prefix) "
            f"   HAVING MAX(round) < {round_number - 1} "
            ")  "
            # Exclude dest. prefixes with per-packet load-balancing (?)
            "AND dst_prefix NOT IN ( "
            "    SELECT distinct(dst_prefix) "
            "    FROM  "
            "    ( "
            "        SELECT  "
            "            src_ip,  "
            "            dst_prefix,  "
            f"            {ttl_column_name},  "
            "            COUNTDistinct(reply_ip) AS n_ips_per_ttl_flow,  "
            f"            COUNT((src_ip, dst_ip, {ttl_column_name}"
            + ", src_port, dst_port)) AS cnt  "
            f"        FROM {table_name} "
            f"        WHERE dst_prefix > {inf_born} AND dst_prefix <= {sup_born} "
            f"        AND src_ip = {source_ip}  "
            f"        AND snapshot = {snapshot}  "
            f"        GROUP BY (src_ip, dst_prefix, dst_ip, {ttl_column_name}"
            + ", src_port, dst_port, snapshot) "
            "        HAVING (cnt > 2) OR (n_ips_per_ttl_flow > 1) "
            "    )  "
            "    GROUP BY (src_ip, dst_prefix) "
            ")  "
            f"AND src_ip = {source_ip} AND snapshot = {snapshot} "
            f"AND dst_prefix > {inf_born} AND dst_prefix <= {sup_born}  "
            f"AND round <= {round_number}  "
            "AND dst_ip != reply_ip AND type = 11  "
            "GROUP BY (src_ip, dst_prefix, dst_ip, src_port, dst_port) "
            # Exclude TTLs where there are no nodes and no links (?)
            "HAVING length(links) >= 1 or length(replies_s) >= 1  "
            "ORDER BY dst_prefix ASC"
        )
        print(query)
        client = Client(database_host, connect_timeout=1000, send_receive_timeout=6000)
        for row in client.execute_iter(
            query,
            settings={
                "max_block_size": 100000,
                "connect_timeout": 1000,
                "send_timeout": 6000,
                "receive_timeout": 6000,
                "read_backoff_min_latency_ms": 100000,
            },
        ):
            yield row
