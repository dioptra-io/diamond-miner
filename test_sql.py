import ipaddress


def select_resolved_prefixes(
    table_name, src_ip, round_number, snapshot, inf_born, sup_born
):
    return (
        " SELECT dst_prefix"
        "    FROM"
        "    ("
        "        SELECT"
        "            src_ip,"
        "            dst_prefix,"
        "            MAX(round) AS max_round"
        f"        FROM {table_name}"
        f"        WHERE dst_prefix > {inf_born} AND dst_prefix <= {sup_born}"
        f"        AND src_ip = {src_ip}"
        f"        AND snapshot = {snapshot}"
        "         GROUP BY (src_ip, dst_prefix)"
        f"        HAVING max_round < {round_number - 1}"
        "    )"
    )


def select_resolved_prefixes2(
    table_name, src_ip, round_number, snapshot, inf_born, sup_born
):
    return (
        # " SELECT dst_prefix"
        # "    FROM"
        # "    ("
        "        SELECT"
        # "            src_ip,"
        "            dst_prefix"
        # "            MAX(round) AS max_round"
        f"        FROM {table_name}"
        f"        WHERE dst_prefix > {inf_born} AND dst_prefix <= {sup_born}"
        f"        AND src_ip = {src_ip}"
        f"        AND snapshot = {snapshot}"
        "         GROUP BY (src_ip, dst_prefix)"
        f"        HAVING MAX(round) < {round_number - 1}"
        # "    )"
    )


if __name__ == "__main__":
    j = 0
    ipv4_split = 64
    # inf_born = int(j * ((2 ** 32 - 1) / ipv4_split))
    # sup_born = int((j + 1) * ((2 ** 32 - 1) / ipv4_split))
    inf_born = 0
    sup_born = 2 ** 32
    print(
        select_resolved_prefixes2(
            "results__9ef5b32d_614a_4ef0_8d2f_b0a78f7c50b3__ddd8541d_b4f5_42ce_b163_e3e9bfcd0a47",
            int(ipaddress.ip_address("132.227.123.9")),
            10,
            1,
            inf_born,
            sup_born,
        )
    )
