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

        # print(query)
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


def build_next_round_query(
    table_name, source_ip, round_number, snapshot, ttl_column_name, inf_born, sup_born
):
    query = (
        # f"WITH "
        # # Compute max src_port per ttl
        # f"groupUniqArray(src_port) as src_ports, "
        # f"arrayReduce('min', src_ports) as min_src_port, "
        # # f" arrayFlatten(groupArray(src_port_ttl)) as src_port_ttls, "
        # # f" arrayDistinct(arrayMap(x->x.2, src_port_ttls)) as distinct_src_ttls, "
        # # f" arrayMap(x->(x, arrayFilter(y->y.2=x, src_port_ttls)), distinct_src_ttls) as src_port_ttl_per_ttl, "  # noqa
        # # f" arrayMap(x->(x.1, arrayMap(y->y.1, x.2)), src_port_ttl_per_ttl) as src_port_per_ttl,"  # noqa
        # # f" arrayMap(x->(x.1, arrayReduce('max', x.2)), src_port_per_ttl) as max_src_port_per_ttl,"  # noqa
        # # Compute max dst_port per ttl
        # f"groupUniqArray(dst_port) as dst_ports, "
        # f"arrayReduce('min', dst_ports) as min_dst_port, "
        # f"arrayReduce('max', dst_ports) as max_dst_port, "
        # # f" arrayFlatten(groupArray(dst_port_ttl)) as dst_port_ttls,"
        # # f" arrayDistinct(arrayMap(x->x.2, dst_port_ttls)) as distinct_dst_ttls, "  # noqa
        # # f" arrayMap(x->(x, arrayFilter(y->y.2=x, dst_port_ttls)), distinct_dst_ttls) as dst_port_ttl_per_ttl, "  # noqa
        # # f" arrayMap(x->(x.1, arrayMap(y->y.1, x.2)), dst_port_ttl_per_ttl) as dst_port_per_ttl,"  # noqa
        # # f" arrayMap(x->(x.1, arrayReduce('max', x.2)), dst_port_per_ttl) as max_dst_port_per_ttl,"  # noqa
        # # Compute number of probes per src, ttl
        # f" arrayFlatten(groupArray(replies_s)) as replies,"
        # # x is (node, ttl, round)
        # f" arrayDistinct(arrayMap(x->(x.1, x.2), replies)) as nodes, "
        # # f" arrayDistinct(arrayMap(x->x.2, replies)) as distinct_nodes_ttl, "
        #
        #
        # f" arrayMap(r->(r.1, r.2), replies) as replies_no_round, "
        # f" arrayMap(r->(r.1, r.2), arrayFilter(x->x.3 < {round_number}, replies)) as replies_no_round_previous, "
        # ########################## Links per TTL ######################
        # f" range(1, 31) as ttls, "
        # f" arrayFlatten(groupArray(links_no_round)) as links_flat, "
        # f" arrayFlatten(groupArray(links_no_round_previous)) as links_flat_previous, "
        #
        # # Compute links between TTL t and t + 1
        # f" arrayMap(t->(t, arrayUniq(arrayFilter(x->x.1.2==t, links_flat))), ttls) as links_per_ttl, "
        # f" arrayMax(t->t.2, links_per_ttl).2 as max_links, "
        # f" arrayMap(t->(t, arrayUniq(arrayFilter(x->x.1.2==t, links_flat_previous))), ttls) as links_per_ttl_previous, "
        # f" arrayMax(t->t.2, links_per_ttl_previous).2 as max_links_previous, "
        #
        # # f" arrayReduce('max', n_links_per_ttl_no_ttl) as max_links, "
        # # f" arrayReduce('max', n_links_per_ttl_no_ttl_previous) as max_links_previous, "
        #
        #
        # ########################## Nodes per TTL ######################
        #
        # f" arrayMap(t->arrayUniq(arrayFilter(x->x.2 == t, replies_no_round)), ttls) as nodes_per_ttl, "
        # f" arrayMap(t->arrayUniq(arrayFilter(x->x.2 == t, replies_no_round_previous)), ttls) as nodes_per_ttl_previous, "
        # f" arrayReduce('max', nodes_per_ttl) as max_nodes, "
        #
        # # Fast fail
        # f"if(equals(links_per_ttl, links_per_ttl_previous), 1, 0)  as skip_prefix, "
        # ########################## Epsilon is based on number of links ####################
        # f" 0.05 as target_epsilon, "
        # # f" 0.05 as epsilon, "
        # # f" 0.05 as epsilon_previous, "
        # f" if(max_links == 0, 0.05, 1 - exp(log(1 - target_epsilon) / max_links)) as epsilon, "
        # f" if(max_links_previous == 0, 0.05, 1 - exp(log(1 - target_epsilon) / max_links_previous)) as epsilon_previous, "
        #
        #
        # ###################### Nks ####################
        # f" range(1, arrayReduce('max', array(max_links + 2, max_nodes + 2))) as nks_index, "
        # f" arrayMap(k-> toUInt32(ceil(ln(epsilon / k) / ln((k - 1) / k))), nks_index) as nks, "
        # f" arrayMap(k-> toUInt32(ceil(ln(epsilon_previous / k) / ln((k - 1) / k))), nks_index) as nks_previous, "
        #
        # ###################### The D-Miner lite formula ####################
        #
        # f" arrayMap(t->(t.1, nks[t.2 + 1]), links_per_ttl) as nkv_Dhv, "
        # f" arrayMap(t->(t.1, nks_previous[t.2 + 1]), links_per_ttl_previous) as nkv_Dhv_previous, "
        # # Compute the probes sent at previous round
        # f" arrayMap(t->(t, if(t == 1, nkv_Dhv_previous[t].2, arrayReduce('max', array(nkv_Dhv_previous[t].2, nkv_Dhv_previous[t+1].2)))), ttls) as max_nkv_Dhv_previous, "
        # f" arrayMap(t->(t, if({round_number}==1, 6, if(t == 1, nkv_Dhv[t].2, arrayReduce('max', array(nkv_Dhv[t].2, nkv_Dhv[t+1].2))))), ttls) as max_nkv_Dhv, "
        # # Take the max of each between TTL and TTL + 1
        # f" arrayMap(t->(t, toInt32(if(t == 1, max_nkv_Dhv[t].2 - max_nkv_Dhv_previous[t].2,  "
        # f"                         arrayReduce('max', array(max_nkv_Dhv[t].2 - max_nkv_Dhv_previous[t].2,"
        # f"                         max_nkv_Dhv[t-1].2 - max_nkv_Dhv_previous[t-1].2))))), ttls) as d_miner_lite_probes, "
        #
        #
        # ########################## * nodes * ############################
        # f" arrayMap(t->(t, if(nodes_per_ttl[t] == 0, 0, d_miner_lite_probes[t].2)),  ttls) as d_miner_lite_probes_no_probe_star, "
        # f" arrayMap(t->(t, if(nodes_per_ttl[t-1] == 0 and nodes_per_ttl[t] > 0 and nodes_per_ttl[t+1] == 0, nks[nodes_per_ttl[t]] - nks_previous[nodes_per_ttl_previous[t]], d_miner_lite_probes_no_probe_star[t].2)), ttls) as d_miner_paper_probes_w_star_nodes_star, "
        #
        #
        # ######################### Compute max flow for previous round, it's th w/ the * nodes * heuristic ####################
        # f" arrayMap(t->(t, toInt32(if(nodes_per_ttl[t-1] == 0 and nodes_per_ttl[t] > 0 and nodes_per_ttl[t+1] == 0, nks_previous[nodes_per_ttl_previous[t]], max_nkv_Dhv_previous[t].2))), ttls) as previous_max_flow_per_ttl "
        #
        # " SELECT src_ip, dst_prefix, skip_prefix, "
        # # " max_nkv_Dhv,"
        # # " d_miner_paper_probes, max_nodes, "
        # " d_miner_lite_probes_no_probe_star, previous_max_flow_per_ttl, "
        # # " d_miner_paper_probes, th, "
        # # " epsilon, epsilon_previous, nodes_per_ttl, nodes_per_ttl_previous, links_per_ttl, links_per_ttl_previous, "
        # # " nodes_active, nodes_active_previous, n_probes_per_node, n_probes_per_node_previous, n_links_per_sources, n_links_per_sources_previous, "  # noqa
        # " min_src_port, min_dst_port, max_dst_port "
        # "FROM "
        # "("
        "WITH "
        # "groupUniqArray(src_port, ttl)) as src_port_ttl,"
        # "groupUniqArray((dst_port, ttl)) as dst_port_ttl,"
        "groupUniqArray((dst_ip, src_port, dst_port, reply_ip, ttl, round)) as replies_s, "
        "arraySort(x->(x.1, x.2, x.3, x.5), replies_s) as sorted_replies_s, "
        "arrayPopFront(sorted_replies_s) as replies_d, "
        " arrayConcat(replies_d, [(0,0,0,0,0,0)]) as replies_d_sized, "
        
        f" arrayMap(r->(r.4, r.5), replies_s) as replies_no_round, "
        f" arrayMap(r->(r.4, r.5), arrayFilter(x->x.3 < {round_number}, replies_s)) as replies_no_round_previous, "
        
        " arrayZip(sorted_replies_s, replies_d_sized) as potential_links, "
        " arrayFilter(x->x.1.5 + 1 == x.2.5, potential_links) as links, "
        f"arrayFilter(x->x.1.6 < {round_number} and x.2.6 < {round_number}, links) as links_previous, "
        " arrayDistinct(arrayMap(x->((x.1.4, x.1.5), (x.2.4, x.2.5)), links)) as links_no_round, "
        " arrayDistinct(arrayMap(x->((x.1.4, x.1.5), (x.2.4, x.2.5)), links_previous)) as links_no_round_previous, "
        # Gives n_links_per_ttl
        f" range(1, 31) as ttls, "

        # Compute links between TTL t and t + 1
        f" arrayMap(t->(t, arrayUniq(arrayFilter(x->x.1.2==t, links_no_round))), ttls) as links_per_ttl, "
        f" arrayReduce('max', arrayMap(t->t.2, links_per_ttl)) as max_links, "
        f" arrayMap(t->(t, arrayUniq(arrayFilter(x->x.1.2==t, links_no_round_previous))), ttls) as links_per_ttl_previous, "
        f" arrayReduce('max', arrayMap(t->t.2, links_per_ttl_previous)) as max_links_previous, "
        
        
        # ########################## Nodes per TTL ######################
        #
        f" arrayMap(t->arrayUniq(arrayFilter(x->x.2 == t, replies_no_round)), ttls) as nodes_per_ttl, "
        f" arrayMap(t->arrayUniq(arrayFilter(x->x.2 == t, replies_no_round_previous)), ttls) as nodes_per_ttl_previous, "
        f" arrayReduce('max', nodes_per_ttl) as max_nodes, "
        #
        # # Fast fail
        f"if(equals(links_per_ttl, links_per_ttl_previous), 1, 0)  as skip_prefix, "
        # ########################## Epsilon is based on number of links ####################
        f" 0.05 as target_epsilon, "
        # # f" 0.05 as epsilon, "
        # # f" 0.05 as epsilon_previous, "
        f" if(max_links == 0, 0.05, 1 - exp(log(1 - target_epsilon) / max_links)) as epsilon, "
        f" if(max_links_previous == 0, 0.05, 1 - exp(log(1 - target_epsilon) / max_links_previous)) as epsilon_previous, "
        #
        #
        # ###################### Nks ####################
        f" range(1, arrayReduce('max', array(max_links + 2, max_nodes + 2))) as nks_index, "
        f" arrayMap(k-> toUInt32(ceil(ln(epsilon / k) / ln((k - 1) / k))), nks_index) as nks, "
        f" arrayMap(k-> toUInt32(ceil(ln(epsilon_previous / k) / ln((k - 1) / k))), nks_index) as nks_previous, "
        #
        # ###################### The D-Miner lite formula ####################
        # Number of probes to send, the two followinng lines are correct
        f" arrayMap(t->(t.1, nks[t.2 + 1]), links_per_ttl) as nkv_Dhv, "
        f" arrayMap(t->(t.1, nks_previous[t.2 + 1]), links_per_ttl_previous) as nkv_Dhv_previous, "
        # # Compute the probes sent at previous round
        f" arrayMap(t->(t, if({round_number} == 1, 6, if(t == 1, nkv_Dhv_previous[t].2, arrayReduce('max', array(nkv_Dhv_previous[t].2, nkv_Dhv_previous[t-1].2))))), ttls) as max_nkv_Dhv_previous, "
        f" arrayMap(t->(t, toInt32(if(t == 1, nkv_Dhv[t].2 - max_nkv_Dhv_previous[t].2, arrayReduce('max', array(nkv_Dhv[t].2 - max_nkv_Dhv_previous[t].2, nkv_Dhv[t-1].2 - max_nkv_Dhv_previous[t-1].2))))), ttls) as d_miner_lite_probes, "
        # Take the max of each between TTL and TTL - 1
        # f" arrayMap(t->(t, toInt32(if(t == 1, max_nkv_Dhv[t].2 - max_nkv_Dhv_previous[t].2,  "
        # f"                         arrayReduce('max', array(max_nkv_Dhv[t].2 - max_nkv_Dhv_previous[t].2,"
        # f"                         max_nkv_Dhv[t-1].2 - max_nkv_Dhv_previous[t-1].2))))), ttls) as d_miner_lite_probes, "


        ########################## * nodes * ############################
        f" arrayMap(t->(t, if(nodes_per_ttl[t] == 0, 0, d_miner_lite_probes[t].2)),  ttls) as d_miner_lite_probes_no_probe_star, "
        f" arraySlice(ttls, 2) as sliced_ttls, "
        
        f"arrayMap(t -> (t, if(((nodes_per_ttl[ttls[t - 1]]) = 0) AND ((nodes_per_ttl[ttls[t]]) > 0) AND ((nodes_per_ttl[ttls[t + 1]]) = 0), nks[nodes_per_ttl[ttls[t]] + 1] - nks_previous[nodes_per_ttl_previous[ttls[t]] + 1], 0)), sliced_ttls) AS d_miner_paper_probes_w_star_nodes_star,"
        f"arrayPushFront(d_miner_paper_probes_w_star_nodes_star, (1, 0)) AS d_miner_paper_probes_w_star_nodes_star_new,"
        f"arrayMap(t->(t, arrayReduce('max', array(d_miner_paper_probes_w_star_nodes_star_new[t].2, d_miner_lite_probes_no_probe_star[t].2))), ttls) as final_probes, "
        
        # f" arrayMap(t->(t, if(nodes_per_ttl[ttls[t-1]] == 0 and nodes_per_ttl[ttls[t]] > 0 and nodes_per_ttl[ttls[t+1]] == 0, nks[nodes_per_ttl[ttls[t]] + 1] - nks_previous[nodes_per_ttl_previous[ttls[t]] + 1], d_miner_lite_probes_no_probe_star[ttls[t]].2)), sliced_ttls) as d_miner_paper_probes_w_star_nodes_star,"
        # f" arrayMap(t->(t, if(nodes_per_ttl[ttls[t-1]] == 0 and nodes_per_ttl[ttls[t]] > 0 and nodes_per_ttl[ttls[t+1]] == 0, 1, d_miner_lite_probes_no_probe_star[ttls[t]].2)), sliced_ttls) as d_miner_paper_probes_w_star_nodes_star "
        # f" arrayMap(t->(t, if(nodes_per_ttl[ttls[t-1]] == 0 and nodes_per_ttl[ttls[t]] > 0 and nodes_per_ttl[ttls[t+1]] == 0, nks[nodes_per_ttl[ttls[t]] + 1] - nks_previous[nodes_per_ttl_previous[ttls[t]] + 1], d_miner_lite_probes_no_probe_star[ttls[t]].2)), arraySlice(ttls, 2)) as d_miner_paper_probes_w_star_nodes_star, "
        # f" arrayMap(t->(t, if(1 == 1, 0, d_miner_lite_probes_no_probe_star[t].2)), ttls) as d_miner_paper_probes_w_star_nodes_star, "
        # f" arrayMap(t->(t, d_miner_lite_probes_no_probe_star[t].2), ttls) as d_miner_paper_probes_w_star_nodes_star, "
        #
        #
        # ######################### Compute max flow for previous round, it's th w/ the * nodes * heuristic ####################
        f" arrayMap(t->(t, toInt32(if(nodes_per_ttl[ttls[t-1]] == 0 and nodes_per_ttl[ttls[t]] > 0 and nodes_per_ttl[ttls[t+1]] == 0, nks_previous[nodes_per_ttl_previous[ttls[t]] + 1], max_nkv_Dhv_previous[ttls[t]].2))), sliced_ttls) as previous_max_flow_per_ttl, "
        f" arrayPushFront(previous_max_flow_per_ttl, max_nkv_Dhv_previous[ttls[1]]) as previous_max_flow_per_ttl_final "        
        # "max(round) as max_round "
        " SELECT src_ip, dst_prefix, skip_prefix, "
        # " max_nkv_Dhv,"
        # " d_miner_paper_probes, max_nodes, "
        # " nodes_per_ttl, "
        # " epsilon, epsilon_previous, "
        # " nks, nks_previous, max_nkv_Dhv_previous, "
        # " links_per_ttl, links_per_ttl_previous, "
        " final_probes, "
        " previous_max_flow_per_ttl_final, "
        # " previous_max_flow_per_ttl, "
        # " d_miner_paper_probes, th, "
        # " epsilon, epsilon_previous, nodes_per_ttl, nodes_per_ttl_previous, links_per_ttl, links_per_ttl_previous, "
        # " nodes_active, nodes_active_previous, n_probes_per_node, n_probes_per_node_previous, n_links_per_sources, n_links_per_sources_previous, "  # noqa
        " min(src_port), min(dst_port), max(dst_port) "
        # "SELECT  "
        # "    src_ip,  "
        # "    dst_prefix,  "
        # "    max(src_port), "
        # "    min(dst_port), "
        # # "    sorted_replies_s,  "
        # "    links_per_ttl,  "
        # "    links_per_ttl_previous,"
        # "    max_round  "
        f"FROM {table_name}  "
        # Exclude dest. prefixes for which no probes have been sent
        # during the previous round (?)
        # " WHERE 1 = 1 " #
        "WHERE"
        # "  dst_prefix = 98698240 AND "
        "  dst_prefix NOT IN ( "
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
        f" AND "
        f" src_ip = {source_ip} AND snapshot = {snapshot} "
        f"AND dst_prefix > {inf_born} AND dst_prefix <= {sup_born} "
        f"AND round <= {round_number}  "
        "AND dst_ip != reply_ip AND type = 11  "
        "GROUP BY (src_ip, dst_prefix) "
        # Exclude TTLs where there are no nodes and no links (?)
        "HAVING "
        " length(links) >= 1 OR "
        " length(replies_s) >= 1  "
        # # "ORDER BY dst_prefix DESC"
        # ") "
        # f" WHERE dst_prefix=31024640 "
        # "GROUP BY  src_ip, dst_prefix "
        # " ORDER BY dst_prefix ASC"
    )
    print(query)
    return query


def n_split_lines(client, table_name, source_ip, round_number, inf_born, sup_born):
    return client.execute(
        f"SELECT Count() from {table_name} "
        f"WHERE src_ip = {source_ip} AND snapshot = 1 "
        f"AND dst_prefix > {inf_born} AND dst_prefix <= {sup_born} "
        f"AND round <= {round_number}  AND dst_ip != reply_ip AND type = 11",
        settings={
            "max_block_size": 100000,
            "connect_timeout": 1000,
            "send_timeout": 6000,
            "receive_timeout": 6000,
            "read_backoff_min_latency_ms": 100000,
        },
    )[0][0]


def query_next_round_recurse(
    client,
    table_name,
    source_ip,
    round_number,
    snapshot,
    ttl_column_name,
    inf_born,
    sup_born,
    batch_limit,
):
    sup_born_division = 1
    temporary_sup_born = sup_born
    split_lines = batch_limit + 1
    import time
    start = time.time()
    while split_lines > batch_limit:
        split_lines = n_split_lines(
            client, table_name, source_ip, round_number, inf_born, temporary_sup_born
        )
        # print(split_lines)
        if split_lines > batch_limit:
            temporary_sup_born = (temporary_sup_born - inf_born) / 2 + inf_born
            sup_born_division += 1

    for j in range(0, 2 ** (sup_born_division - 1)):
        inf_born_div = int(
            inf_born + j * ((sup_born - inf_born) / (2 ** (sup_born_division - 1)))
        )
        sup_born_div = int(
            inf_born
            + (j + 1) * ((sup_born - inf_born) / (2 ** (sup_born_division - 1)))
        )
        if sup_born_division > 1:
            # print(inf_born_div, sup_born_div)
            yield from query_next_round_recurse(
                client,
                table_name,
                source_ip,
                round_number,
                snapshot,
                ttl_column_name,
                inf_born_div,
                sup_born_div,
                batch_limit,
            )
        else:
            # print("query", inf_born_div, sup_born_div)
            query = build_next_round_query(
                table_name,
                source_ip,
                round_number,
                snapshot,
                ttl_column_name,
                inf_born_div,
                sup_born_div,
            )

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


    elapsed = time.time() - start
    print(f"Split between {inf_born} and {sup_born} took {elapsed} seconds.")

def query_next_round(database_host, table_name, source_ip, round_number):
    """Generator of next round database query."""

    snapshot = 1  # NOTE Not currently used
    ipv4_split = 16
    ttl_column_name = "ttl_from_udp_length"
    batch_limit = 25_000_000

    for j in range(0, ipv4_split):
        inf_born = int(j * ((2 ** 32 - 1) / ipv4_split))
        sup_born = int((j + 1) * ((2 ** 32 - 1) / ipv4_split))

        # TODO Excluded prefixes ?
        if j != 3:
            continue
        if sup_born > 3758096384:
            # exclude prefixes >= 224.0.0.0 (multicast)
            break

        # print(j)
        # if j != 3:
        #     continue

        client = Client(database_host, connect_timeout=1000, send_receive_timeout=6000)
        # print(inf_born, sup_born)
        yield from query_next_round_recurse(
            client,
            table_name,
            source_ip,
            round_number,
            snapshot,
            ttl_column_name,
            inf_born,
            sup_born,
            batch_limit,
        )


def query_discoveries_per_ttl(
    database_host, table_name, source_ip, round_number, absolute_max_ttl=255
):
    query = (
        "SELECT ttl, CountDistinct(reply_ip)"
        f"FROM {table_name} "
        f"WHERE src_ip = {source_ip} AND dst_prefix > 0 "
        "AND reply_ip != dst_ip AND type = 11 "
        f"AND ttl <= {absolute_max_ttl} "
        "GROUP BY ttl"
    )
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
