from dataclasses import dataclass

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries.query import Query, results_table
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class CreateResultsTable(Query):
    """Create the table used to store the measurement results from the prober."""

    SORTING_KEY = "probe_protocol, probe_src_addr, probe_dst_prefix, probe_dst_addr, probe_src_port, probe_dst_port, probe_ttl"

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {results_table(measurement_id)}
        (
            probe_protocol         UInt8,
            probe_src_addr         IPv6,
            probe_dst_addr         IPv6,
            probe_src_port         UInt16,
            probe_dst_port         UInt16,
            probe_ttl              UInt8,
            quoted_ttl             UInt8,
            reply_src_addr         IPv6,
            reply_protocol         UInt8,
            reply_icmp_type        UInt8,
            reply_icmp_code        UInt8,
            reply_ttl              UInt8,
            reply_size             UInt16,
            reply_mpls_labels      Array(UInt32),
            -- The rtt column is the largest compressed column, we use T64 and ZSTD to reduce its size, see:
            -- https://altinity.com/blog/2019/7/new-encodings-to-improve-clickhouse
            -- https://clickhouse.tech/docs/en/sql-reference/statements/create/table/#codecs
            rtt                    UInt16 CODEC(T64, ZSTD(1)),
            round                  UInt8,
            -- Materialized columns
            probe_dst_prefix       IPv6 MATERIALIZED toIPv6(cutIPv6(probe_dst_addr, 8, 1)),
            reply_src_prefix       IPv6 MATERIALIZED toIPv6(cutIPv6(reply_src_addr, 8, 1)),
            -- https://en.wikipedia.org/wiki/Reserved_IP_addresses
            private_probe_dst_prefix UInt8 MATERIALIZED
                (probe_dst_prefix >= toIPv6('0.0.0.0')      AND probe_dst_prefix <= toIPv6('0.255.255.255'))   OR
                (probe_dst_prefix >= toIPv6('10.0.0.0')     AND probe_dst_prefix <= toIPv6('10.255.255.255'))  OR
                (probe_dst_prefix >= toIPv6('100.64.0.0')   AND probe_dst_prefix <= toIPv6('100.127.255.255')) OR
                (probe_dst_prefix >= toIPv6('127.0.0.0')    AND probe_dst_prefix <= toIPv6('127.255.255.255')) OR
                (probe_dst_prefix >= toIPv6('172.16.0.0')   AND probe_dst_prefix <= toIPv6('172.31.255.255'))  OR
                (probe_dst_prefix >= toIPv6('192.0.0.0')    AND probe_dst_prefix <= toIPv6('192.0.0.255'))     OR
                (probe_dst_prefix >= toIPv6('192.0.2.0')    AND probe_dst_prefix <= toIPv6('192.0.2.255'))     OR
                (probe_dst_prefix >= toIPv6('192.88.99.0')  AND probe_dst_prefix <= toIPv6('192.88.99.255'))   OR
                (probe_dst_prefix >= toIPv6('192.168.0.0')  AND probe_dst_prefix <= toIPv6('192.168.255.255')) OR
                (probe_dst_prefix >= toIPv6('198.18.0.0')   AND probe_dst_prefix <= toIPv6('198.19.255.255'))  OR
                (probe_dst_prefix >= toIPv6('198.51.100.0') AND probe_dst_prefix <= toIPv6('198.51.100.255'))  OR
                (probe_dst_prefix >= toIPv6('203.0.113.0')  AND probe_dst_prefix <= toIPv6('203.0.113.255'))   OR
                (probe_dst_prefix >= toIPv6('224.0.0.0')    AND probe_dst_prefix <= toIPv6('239.255.255.255')) OR
                (probe_dst_prefix >= toIPv6('233.252.0.0')  AND probe_dst_prefix <= toIPv6('233.252.0.255'))   OR
                (probe_dst_prefix >= toIPv6('240.0.0.0')    AND probe_dst_prefix <= toIPv6('255.255.255.255')) OR
                (probe_dst_prefix >= toIPv6('fd00::')       AND probe_dst_prefix <= toIPv6('fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff')),
            private_reply_src_addr UInt8 MATERIALIZED
                (reply_src_addr >= toIPv6('0.0.0.0')        AND reply_src_addr <= toIPv6('0.255.255.255'))     OR
                (reply_src_addr >= toIPv6('10.0.0.0')       AND reply_src_addr <= toIPv6('10.255.255.255'))    OR
                (reply_src_addr >= toIPv6('100.64.0.0')     AND reply_src_addr <= toIPv6('100.127.255.255'))   OR
                (reply_src_addr >= toIPv6('127.0.0.0')      AND reply_src_addr <= toIPv6('127.255.255.255'))   OR
                (reply_src_addr >= toIPv6('172.16.0.0')     AND reply_src_addr <= toIPv6('172.31.255.255'))    OR
                (reply_src_addr >= toIPv6('192.0.0.0')      AND reply_src_addr <= toIPv6('192.0.0.255'))       OR
                (reply_src_addr >= toIPv6('192.0.2.0')      AND reply_src_addr <= toIPv6('192.0.2.255'))       OR
                (reply_src_addr >= toIPv6('192.88.99.0')    AND reply_src_addr <= toIPv6('192.88.99.255'))     OR
                (reply_src_addr >= toIPv6('192.168.0.0')    AND reply_src_addr <= toIPv6('192.168.255.255'))   OR
                (reply_src_addr >= toIPv6('198.18.0.0')     AND reply_src_addr <= toIPv6('198.19.255.255'))    OR
                (reply_src_addr >= toIPv6('198.51.100.0')   AND reply_src_addr <= toIPv6('198.51.100.255'))    OR
                (reply_src_addr >= toIPv6('203.0.113.0')    AND reply_src_addr <= toIPv6('203.0.113.255'))     OR
                (reply_src_addr >= toIPv6('224.0.0.0')      AND reply_src_addr <= toIPv6('239.255.255.255'))   OR
                (reply_src_addr >= toIPv6('233.252.0.0')    AND reply_src_addr <= toIPv6('233.252.0.255'))     OR
                (reply_src_addr >= toIPv6('240.0.0.0')      AND reply_src_addr <= toIPv6('255.255.255.255'))   OR
                (reply_src_addr >= toIPv6('fd00::')         AND reply_src_addr <= toIPv6('fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff')),
            destination_host_reply   UInt8 MATERIALIZED probe_dst_addr = reply_src_addr,
            destination_prefix_reply UInt8 MATERIALIZED probe_dst_prefix = reply_src_prefix,
            -- ICMP: protocol 1, UDP: protocol 17, ICMPv6: protocol 58
            valid_probe_protocol   UInt8 MATERIALIZED probe_protocol IN [1, 17, 58],
            time_exceeded_reply    UInt8 MATERIALIZED (reply_protocol = 1 AND reply_icmp_type = 11) OR (reply_protocol = 58 AND reply_icmp_type = 3)
        )
        ENGINE MergeTree
        ORDER BY ({self.SORTING_KEY})
        """
