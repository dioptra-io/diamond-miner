DROP TABLE IF EXISTS test_schema;
CREATE TABLE test_schema
(
    probe_src_addr         IPv6,
    probe_dst_addr         IPv6,
    probe_src_port         UInt16,
    probe_dst_port         UInt16,
    probe_ttl_l3           UInt8,
    probe_ttl_l4           UInt8,
    probe_protocol         UInt8,
    reply_src_addr         IPv6,
    reply_protocol         UInt8,
    reply_icmp_type        UInt8,
    reply_icmp_code        UInt8,
    reply_ttl              UInt8,
    reply_size             UInt16,
    rtt                    Float64,
    round                  UInt8,
    -- Materialized columns
    probe_dst_prefix       IPv6 MATERIALIZED toIPv6(cutIPv6(probe_dst_addr, 8, 1)),
    private_reply_src_addr UInt8 MATERIALIZED
                                   (reply_src_addr >= toIPv6('10.0.0.0') AND reply_src_addr <= toIPv6('10.255.255.255')) OR
                                   (reply_src_addr >= toIPv6('172.16.0.0') AND reply_src_addr <= toIPv6('172.31.255.255')) OR
                                   (reply_src_addr >= toIPv6('192.168.0.0') AND reply_src_addr <= toIPv6('192.168.255.255')) OR
                                   (reply_src_addr >= toIPv6('fd00::') AND reply_src_addr <= toIPv6('fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff')),
    -- ICMP: protocol 1, ICMPv6: protocol 58
    time_exceeded_reply    UInt8 MATERIALIZED (reply_protocol = 1 AND reply_icmp_type = 11) OR (reply_protocol = 58 AND reply_icmp_type = 3)
)
    ENGINE MergeTree
        ORDER BY (probe_protocol, probe_src_addr, probe_dst_prefix, probe_dst_addr, probe_src_port, probe_dst_port, probe_ttl_l4);

-- NSDI '20 paper, Figure 2
-- 100.0.0.1 => 200.0.0.0/24
-- V_n => 150.0.n.1
DROP TABLE IF EXISTS test_nsdi_example;
CREATE TABLE test_nsdi_example AS test_schema;

-- Routes per flow ID
-- 0  1 2 4 6
-- 1  1 2 4 6
-- 2  1 2 4 6
-- 3  1 2 4 6
-- 4  1 3 5 6
-- 5  1 3 5 6
-- 6  1 2 4 6
-- 7  1 2 4 6
-- 8  1 2 4 6
-- 9 1 2 4 6
-- 10 1 3 5 6
-- 11 1 3 5 6
-- 12 1 3 5 6
-- 13 1 3 5 6
-- 14 1 3 7 6
-- 15 1 3 7 6
-- 16 1 3 7 6
-- 17 1 3 7 6
-- 18 1 2 4 6
-- 19 1 3 5 6
-- 20 1 2 4 6
-- 21 1 2 4 6
-- 22 1 2 4 6
-- 23 1 2 4 6
-- 24 1 3 7 6
-- 25 1 3 7 6
-- 26 1 3 7 6

-- Round 1, 6 probes per TTL
INSERT INTO test_nsdi_example
VALUES ('::ffff:100.0.0.1', '::ffff:200.0.0.0', 24000, 33434, 1, 1, 1, '::ffff:150.0.1.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.1', 24000, 33434, 1, 1, 1, '::ffff:150.0.1.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.2', 24000, 33434, 1, 1, 1, '::ffff:150.0.1.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.3', 24000, 33434, 1, 1, 1, '::ffff:150.0.1.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.4', 24000, 33434, 1, 1, 1, '::ffff:150.0.1.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.5', 24000, 33434, 1, 1, 1, '::ffff:150.0.1.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.0', 24000, 33434, 2, 2, 1, '::ffff:150.0.2.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.1', 24000, 33434, 2, 2, 1, '::ffff:150.0.2.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.2', 24000, 33434, 2, 2, 1, '::ffff:150.0.2.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.3', 24000, 33434, 2, 2, 1, '::ffff:150.0.2.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.4', 24000, 33434, 2, 2, 1, '::ffff:150.0.3.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.5', 24000, 33434, 2, 2, 1, '::ffff:150.0.3.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.0', 24000, 33434, 3, 3, 1, '::ffff:150.0.4.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.1', 24000, 33434, 3, 3, 1, '::ffff:150.0.4.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.2', 24000, 33434, 3, 3, 1, '::ffff:150.0.4.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.3', 24000, 33434, 3, 3, 1, '::ffff:150.0.4.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.4', 24000, 33434, 3, 3, 1, '::ffff:150.0.5.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.5', 24000, 33434, 3, 3, 1, '::ffff:150.0.5.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.0', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.1', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.2', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.3', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.4', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.5', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 1);

-- Round 2, 5 probes at TTL 1, 12 probes at TTL 2-4
INSERT INTO test_nsdi_example
VALUES ('::ffff:100.0.0.1', '::ffff:200.0.0.6', 24000, 33434, 1, 1, 1, '::ffff:150.0.1.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.7', 24000, 33434, 1, 1, 1, '::ffff:150.0.1.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.8', 24000, 33434, 1, 1, 1, '::ffff:150.0.1.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.9', 24000, 33434, 1, 1, 1, '::ffff:150.0.1.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.10', 24000, 33434, 1, 1, 1, '::ffff:150.0.1.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.6', 24000, 33434, 2, 2, 1, '::ffff:150.0.2.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.7', 24000, 33434, 2, 2, 1, '::ffff:150.0.2.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.8', 24000, 33434, 2, 2, 1, '::ffff:150.0.2.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.9', 24000, 33434, 2, 2, 1, '::ffff:150.0.2.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.10', 24000, 33434, 2, 2, 1, '::ffff:150.0.3.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.11', 24000, 33434, 2, 2, 1, '::ffff:150.0.3.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.12', 24000, 33434, 2, 2, 1, '::ffff:150.0.3.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.13', 24000, 33434, 2, 2, 1, '::ffff:150.0.3.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.14', 24000, 33434, 2, 2, 1, '::ffff:150.0.3.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.15', 24000, 33434, 2, 2, 1, '::ffff:150.0.3.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.16', 24000, 33434, 2, 2, 1, '::ffff:150.0.3.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.17', 24000, 33434, 2, 2, 1, '::ffff:150.0.3.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.6', 24000, 33434, 3, 3, 1, '::ffff:150.0.4.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.7', 24000, 33434, 3, 3, 1, '::ffff:150.0.4.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.8', 24000, 33434, 3, 3, 1, '::ffff:150.0.4.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.9', 24000, 33434, 3, 3, 1, '::ffff:150.0.4.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.10', 24000, 33434, 3, 3, 1, '::ffff:150.0.5.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.11', 24000, 33434, 3, 3, 1, '::ffff:150.0.5.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.12', 24000, 33434, 3, 3, 1, '::ffff:150.0.5.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.13', 24000, 33434, 3, 3, 1, '::ffff:150.0.5.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.14', 24000, 33434, 3, 3, 1, '::ffff:150.0.7.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.15', 24000, 33434, 3, 3, 1, '::ffff:150.0.7.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.16', 24000, 33434, 3, 3, 1, '::ffff:150.0.7.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.17', 24000, 33434, 3, 3, 1, '::ffff:150.0.7.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.6', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.7', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.8', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.9', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.10', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.11', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.12', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.13', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.14', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.15', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.16', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.17', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 2);

-- Round 3, 2 probes at TTL 2, 9 probes at TTL 3-4
INSERT INTO test_nsdi_example
VALUES ('::ffff:100.0.0.1', '::ffff:200.0.0.18', 24000, 33434, 2, 2, 1, '::ffff:150.0.2.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.19', 24000, 33434, 2, 2, 1, '::ffff:150.0.3.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.18', 24000, 33434, 3, 3, 1, '::ffff:150.0.4.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.19', 24000, 33434, 3, 3, 1, '::ffff:150.0.5.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.20', 24000, 33434, 3, 3, 1, '::ffff:150.0.4.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.21', 24000, 33434, 3, 3, 1, '::ffff:150.0.4.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.22', 24000, 33434, 3, 3, 1, '::ffff:150.0.4.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.23', 24000, 33434, 3, 3, 1, '::ffff:150.0.4.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.24', 24000, 33434, 3, 3, 1, '::ffff:150.0.7.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.25', 24000, 33434, 3, 3, 1, '::ffff:150.0.7.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.26', 24000, 33434, 3, 3, 1, '::ffff:150.0.7.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.18', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.19', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.20', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.21', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.22', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.23', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.24', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.25', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.26', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 3);

-- NSDI topology but with MDA-lite instead of MDA
DROP TABLE IF EXISTS test_nsdi_lite;
CREATE TABLE test_nsdi_lite AS test_schema;

-- Routes per flow ID
-- 0  1 2 4 6
-- 1  1 2 4 6
-- 2  1 2 4 6
-- 3  1 2 4 6
-- 4  1 3 5 6
-- 5  1 3 5 6
-- 6  1 2 4 6
-- 7  1 2 4 6
-- 8  1 2 4 6
-- 9 1 3 5 6
-- 10 1 3 7 6
-- 11 1 2 4 6
-- 12 1 2 4 6
-- 13 1 2 4 6
-- 14 1 3 5 6
-- 15 1 3 7 6

-- Round 1, 6 probes per TTL
INSERT INTO test_nsdi_lite
SELECT *
FROM test_nsdi_example
WHERE round = 1;

-- Round 2, 5 probes at TTL 1-4
INSERT INTO test_nsdi_lite
VALUES ('::ffff:100.0.0.1', '::ffff:200.0.0.6', 24000, 33434, 1, 1, 1, '::ffff:150.0.1.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.7', 24000, 33434, 1, 1, 1, '::ffff:150.0.1.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.8', 24000, 33434, 1, 1, 1, '::ffff:150.0.1.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.9', 24000, 33434, 1, 1, 1, '::ffff:150.0.1.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.10', 24000, 33434, 1, 1, 1, '::ffff:150.0.1.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.6', 24000, 33434, 2, 2, 1, '::ffff:150.0.2.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.7', 24000, 33434, 2, 2, 1, '::ffff:150.0.2.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.8', 24000, 33434, 2, 2, 1, '::ffff:150.0.2.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.9', 24000, 33434, 2, 2, 1, '::ffff:150.0.3.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.10', 24000, 33434, 2, 2, 1, '::ffff:150.0.3.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.6', 24000, 33434, 3, 3, 1, '::ffff:150.0.4.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.7', 24000, 33434, 3, 3, 1, '::ffff:150.0.4.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.8', 24000, 33434, 3, 3, 1, '::ffff:150.0.4.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.9', 24000, 33434, 3, 3, 1, '::ffff:150.0.5.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.10', 24000, 33434, 3, 3, 1, '::ffff:150.0.7.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.6', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.7', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.8', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.9', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 2),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.10', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 2);

-- Round 3, 5 probes at TTL 3-4
INSERT INTO test_nsdi_lite
VALUES ('::ffff:100.0.0.1', '::ffff:200.0.0.11', 24000, 33434, 2, 2, 1, '::ffff:150.0.2.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.12', 24000, 33434, 2, 2, 1, '::ffff:150.0.2.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.13', 24000, 33434, 2, 2, 1, '::ffff:150.0.2.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.14', 24000, 33434, 2, 2, 1, '::ffff:150.0.3.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.15', 24000, 33434, 2, 2, 1, '::ffff:150.0.3.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.11', 24000, 33434, 3, 3, 1, '::ffff:150.0.4.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.12', 24000, 33434, 3, 3, 1, '::ffff:150.0.4.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.13', 24000, 33434, 3, 3, 1, '::ffff:150.0.4.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.14', 24000, 33434, 3, 3, 1, '::ffff:150.0.5.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.15', 24000, 33434, 3, 3, 1, '::ffff:150.0.7.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.11', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.12', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.13', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.14', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 3),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.15', 24000, 33434, 4, 4, 1, '::ffff:150.0.6.1', 1, 11, 0, 250, 0, 0.0, 3);

-- We reuse the NSDI example, but with no replies at TTL 2 and 4
DROP TABLE IF EXISTS test_star_node_star;
CREATE TABLE test_star_node_star AS test_schema;

INSERT INTO test_star_node_star
SELECT *
FROM test_nsdi_example
WHERE probe_ttl_l4 NOT IN (2, 4);

-- We send 2 probes per flow
-- Prefix 200.0.0.0/24 is OK (1 node discovered)
-- Prefix 201.0.0.0/24 is doing per-packet LB (2 nodes discovered)
-- Prefix 202.0.0.0/24 is sending multiple replies per probe (3 replies)
DROP TABLE IF EXISTS test_invalid_prefixes;
CREATE TABLE test_invalid_prefixes AS test_schema;

INSERT INTO test_invalid_prefixes
VALUES ('::ffff:100.0.0.1', '::ffff:200.0.0.0', 24000, 33434, 1, 1, 1, '::ffff:150.0.0.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.0', 24000, 33434, 1, 1, 1, '::ffff:150.0.0.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:201.0.0.0', 24000, 33434, 1, 1, 1, '::ffff:150.0.0.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:201.0.0.0', 24000, 33434, 1, 1, 1, '::ffff:150.0.1.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:202.0.0.0', 24000, 33434, 1, 1, 1, '::ffff:150.0.0.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:202.0.0.0', 24000, 33434, 1, 1, 1, '::ffff:150.0.0.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:202.0.0.0', 24000, 33434, 1, 1, 1, '::ffff:150.0.0.1', 1, 11, 0, 250, 0, 0.0, 1);

-- 100.0.0.1 - 200.0.0.1 => max(ttl) = 3
-- 100.0.0.1 - 201.0.0.1 => max(ttl) = 2
DROP TABLE IF EXISTS test_max_ttl;
CREATE TABLE test_max_ttl AS test_schema;

INSERT INTO test_max_ttl
VALUES ('::ffff:100.0.0.1', '::ffff:200.0.0.0', 24000, 33434, 1, 1, 1, '::ffff:150.0.0.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.0', 24000, 33434, 2, 2, 1, '::ffff:150.0.1.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.0', 24000, 33434, 3, 3, 1, '::ffff:150.0.2.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:201.0.0.0', 24000, 33434, 1, 1, 1, '::ffff:150.0.3.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:201.0.0.0', 24000, 33434, 2, 2, 1, '::ffff:150.0.4.1', 1, 11, 0, 250, 0, 0.0, 1);

-- Table with replies from prefixes spread over /0
-- 2 replies in 0.0.0.0/8
-- 1 reply in 1.0.0.0/8
-- 1 reply in 230.0.0/8
DROP TABLE IF EXISTS test_count_replies;
CREATE TABLE test_count_replies AS test_schema;

INSERT INTO test_count_replies
VALUES ('::ffff:100.0.0.1', '::ffff:0.0.0.0', 24000, 33434, 1, 1, 1, '::ffff:150.0.0.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:0.1.0.0', 24000, 33434, 1, 1, 1, '::ffff:150.0.0.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:1.0.0.0', 24000, 33434, 1, 1, 1, '::ffff:150.0.0.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:230.0.0.0', 24000, 33434, 1, 1, 1, '::ffff:150.0.0.1', 1, 11, 0, 250, 0, 0.0, 1);

-- At TTL 1, we discover 1 node with UDP, 2 with ICMP
-- At TTL 2, we discover 1 node for both protocols
DROP TABLE IF EXISTS test_multi_protocol;
CREATE TABLE test_multi_protocol AS test_schema;

INSERT INTO test_multi_protocol
VALUES ('::ffff:100.0.0.1', '::ffff:200.0.0.1', 24000, 33434, 1, 1, 17, '::ffff:150.0.0.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.2', 24000, 33434, 1, 1, 17, '::ffff:150.0.0.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.1', 24000, 33434, 1, 1, 1, '::ffff:150.0.0.1', 1, 11, 0, 250, 0, 0.0, 1),
       ('::ffff:100.0.0.1', '::ffff:200.0.0.2', 24000, 33434, 1, 1, 1, '::ffff:150.0.0.2', 1, 11, 0, 250, 0, 0.0, 1)
       ('::ffff:100.0.0.1', '::ffff:200.0.0.1', 24000, 33434, 2, 2, 17, '::ffff:150.0.1.1', 1, 11, 0, 250, 0, 0.0, 1)
       ('::ffff:100.0.0.1', '::ffff:200.0.0.2', 24000, 33434, 2, 2, 17, '::ffff:150.0.1.1', 1, 11, 0, 250, 0, 0.0, 1)
       ('::ffff:100.0.0.1', '::ffff:200.0.0.1', 24000, 33434, 2, 2, 1, '::ffff:150.0.1.1', 1, 11, 0, 250, 0, 0.0, 1)
       ('::ffff:100.0.0.1', '::ffff:200.0.0.2', 24000, 33434, 2, 2, 1, '::ffff:150.0.1.1', 1, 11, 0, 250, 0, 0.0, 1);
