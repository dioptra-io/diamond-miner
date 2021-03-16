from ipaddress import ip_address

import pandas as pd

src = "data/sample_results.csv"
dst = "data/sample_results_new.csv"


def to_v6(x):
    return "::ffff:" + str(ip_address(x))


df = pd.read_csv(
    src,
    names=[
        "dst_ip",
        "prefix",
        "inner_dst_ip",
        "src_ip",
        "inner_proto",
        "inner_src_port",
        "inner_dst_port",
        "inner_ttl",
        "inner_ttl_from_transport",
        "icmp_type",
        "icmp_code",
        "rtt",
        "ttl",
        "size",
    ],
)

df.src_ip = df.src_ip.apply(to_v6)
df.dst_ip = df.dst_ip.apply(to_v6)
df.inner_dst_ip = df.inner_dst_ip.apply(to_v6)

df.to_csv(
    dst,
    header=False,
    index=False,
    columns=[
        "dst_ip",
        "inner_dst_ip",
        "inner_src_port",
        "inner_dst_port",
        "inner_ttl",
        "inner_ttl_from_transport",
        "src_ip",
        "inner_proto",
        "icmp_type",
        "icmp_code",
        "ttl",
        "size",
        "rtt",
    ],
)
