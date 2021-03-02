import csv
import ipaddress
from diamond_miner_core.flow import CIDRFlowMapper, SequentialFlowMapper
from diamond_miner_core.processors import next_round
from clickhouse_driver import Client

class MeasurementParameters:
    source_ip = 2229500681
    round_number = 1
    source_port = 24000
    destination_port = 33434


def drop_table(db, table):
    query = (
        f" DROP TABLE IF EXISTS {db}.{table}"
    )

    client = Client("127.0.0.1")
    client.execute(query)

def create_test_table(db, table):
    query = (
        f" CREATE TABLE {db}.{table}"
        f"("
        f"    `src_ip` UInt32,"
        f"    `dst_prefix` UInt32,"
        f"    `dst_ip` UInt32,"
        f"    `reply_ip` UInt32,"
        f"    `proto` UInt8,"
        f"    `src_port` UInt16,"
        f"    `dst_port` UInt16,"
        f"    `ttl` UInt8,"
        f"    `ttl_from_udp_length` UInt8,"
        f"    `type` UInt8,"
        f"    `code` UInt8,"
        f"    `rtt` Float64,"
        f"    `reply_ttl` UInt8,"
        f"    `reply_size` UInt16,"
        f"    `round` UInt32,"
        f"    `snapshot` UInt16"
        f")"
        f"ENGINE = MergeTree()"
        f"ORDER BY (src_ip, dst_prefix, dst_ip, ttl, src_port, dst_port, snapshot)"
        # f"SETTINGS index_granularity = 8192"
    )

    client = Client("127.0.0.1")
    client.execute(query)

def insert_rows(db, table, rows):
    query = (
        f"INSERT INTO {db}.{table} (src_ip, dst_prefix, dst_ip, reply_ip, "
        f"proto, src_port, dst_port, ttl, ttl_from_udp_length, type, code, rtt, reply_ttl, reply_size, round, snapshot ) "
        f"VALUES"
    )
    client = Client("127.0.0.1")
    client.execute(query, rows)

def replies_from(reply_ip, ttl, flow_start, flow_end, round):
    replies = [
        # src, dst_prefix, dst_ip
        (int(ipaddress.ip_address("132.227.123.9")), int(ipaddress.ip_address("1.0.0.0")),
         int(ipaddress.ip_address(f"1.0.0.{i}")),
         # reply_ip, proto, src_port, dst_port, ttl, ttl_from_udp_length, type, code, rtt, reply_ttl, reply_size, round, snapshot
         int(ipaddress.ip_address(reply_ip)), 17, 24000, 33434, ttl, ttl, 11, 0, 0.1, 63, 0, round, 1)
        for i in range(flow_start, flow_end)
    ]
    return replies

def insert_round(db, table, round):
    rows = []
    if round == 1:
        other_prefix_rows = [
            # src, dst_prefix, dst_ip
            (int(ipaddress.ip_address("132.227.123.9")), int(ipaddress.ip_address("1.0.1.0")),
             int(ipaddress.ip_address(f"1.0.1.{i}")),
             # reply_ip, proto, src_port, dst_port, ttl, ttl_from_udp_length, type, code, rtt, reply_ttl, reply_size, round, snapshot
             int(ipaddress.ip_address("0.0.0.1")), 17, 24000, 33434, 1, 1, 11, 0, 0.1, 63, 0, 1, 1)
            for i in range(0, 6)
        ]
        # To flush faster
        rows.extend(other_prefix_rows)


        rows.extend(replies_from("0.0.0.1", ttl=1, flow_start=0, flow_end=6, round=round))
        rows.extend(replies_from("0.0.0.2", ttl=2, flow_start=0, flow_end=4, round=round))
        rows.extend(replies_from("0.0.0.3", ttl=2, flow_start=4, flow_end=6, round=round))
        rows.extend(replies_from("0.0.0.4", ttl=3, flow_start=0, flow_end=4, round=round))
        rows.extend(replies_from("0.0.0.5", ttl=3, flow_start=4, flow_end=6, round=round))
        rows.extend(replies_from("0.0.0.6", ttl=4, flow_start=0, flow_end=6, round=round))

    if round == 2:
        rows.extend(replies_from("0.0.0.1", ttl=1, flow_start=6, flow_end=11, round=round))
        rows.extend(replies_from("0.0.0.2", ttl=2, flow_start=6, flow_end=10, round=round))
        rows.extend(replies_from("0.0.0.3", ttl=2, flow_start=10,flow_end=18, round=round))
        rows.extend(replies_from("0.0.0.4", ttl=3, flow_start=6, flow_end=10, round=round))
        rows.extend(replies_from("0.0.0.5", ttl=3, flow_start=10,flow_end=14, round=round))
        rows.extend(replies_from("0.0.0.7", ttl=3, flow_start=14,flow_end=18, round=round))
        rows.extend(replies_from("0.0.0.6", ttl=4, flow_start=6, flow_end=18, round=round))

    if round == 3:
        rows.extend(replies_from("0.0.0.2", ttl=2, flow_start=18,flow_end=19, round=round))
        rows.extend(replies_from("0.0.0.3", ttl=2, flow_start=19,flow_end=20, round=round))
        rows.extend(replies_from("0.0.0.4", ttl=3, flow_start=18,flow_end=19, round=round))
        rows.extend(replies_from("0.0.0.5", ttl=3, flow_start=19,flow_end=20, round=round))
        rows.extend(replies_from("0.0.0.4", ttl=3, flow_start=20,flow_end=24, round=round))
        rows.extend(replies_from("0.0.0.7", ttl=3, flow_start=24,flow_end=27, round=round))
        rows.extend(replies_from("0.0.0.6", ttl=4, flow_start=18,flow_end=27, round=round))

    insert_rows(db, table, rows)


def test_paper_data():
    # Prepare
    db = "iris"
    table = "test_paper_example"
    drop_table(db, table)
    create_test_table(db, table)
    for r in range(1, 4):
        insert_round(db, table, r)
    # Execute
    for r in range(1, 4):
        mp = MeasurementParameters()
        mp.round_number = r
        if r < 3:
            continue
        with open("try.csv", "w") as fd:
            writer = fd
            next_round(
                "127.0.0.1",
                # "iris.results__51aec6e6_030e_4a7d_b52f_54c58c3ef6f6__ddd8541d_b4f5_42ce_b163_e3e9bfcd0a47",  # noqa
                f"{db}.{table}",
                mp,
                SequentialFlowMapper(),
                writer,
                skip_unpopulated_ttl=False
            )
            fd.flush()
    # Clean
    drop_table(db, table)


def test_iris_data():
    db = "iris"
    table = "results__b0965916_ac69_4d36_a436_3d52d93adcd5__ddd8541d_b4f5_42ce_b163_e3e9bfcd0a47"
    # table = "results__51aec6e6_030e_4a7d_b52f_54c58c3ef6f6__2d339af6_6e30_4393_8436_78a2080bc151"
    table = "results__058d9fa4_4b98_49f1_9a83_0acb16c8bf84__ddd8541d_b4f5_42ce_b163_e3e9bfcd0a47"
    # table = "results__51aec6e6_030e_4a7d_b52f_54c58c3ef6f6__ddd8541d_b4f5_42ce_b163_e3e9bfcd0a47"
    mp = MeasurementParameters()
    # mp.source_ip = 2165314631
    # Heartbeat
    mp.source_ip = 2229500681
    mp.round_number = 2
    # mp.source_ip = 222950068
    with open("try.csv", "w") as fd:
        writer = fd
        next_round(
            "127.0.0.1",
            # "iris.results__51aec6e6_030e_4a7d_b52f_54c58c3ef6f6__2d339af6_6e30_4393_8436_78a2080bc151",  # noqa
            f"{db}.{table}",
            mp,
            CIDRFlowMapper(),
            writer,
            skip_unpopulated_ttl=False
        )

if __name__ == "__main__":
    import time
    start = time.time()
    test_iris_data()
    elapsed = time.time() - start
    print(f"{elapsed} seconds.")

