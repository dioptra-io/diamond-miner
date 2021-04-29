from ipaddress import IPv6Network

DEFAULT_PREFIX_LEN_V4 = 24
DEFAULT_PREFIX_LEN_V6 = 64

DEFAULT_PREFIX_SIZE_V4 = 2 ** (32 - DEFAULT_PREFIX_LEN_V4)
DEFAULT_PREFIX_SIZE_V6 = 2 ** (128 - DEFAULT_PREFIX_LEN_V6)

DEFAULT_PROBE_SRC_PORT = 24000
DEFAULT_PROBE_DST_PORT = 33434

DEFAULT_SUBSET = IPv6Network("::/0")

# NOTE: If you change the TTL column, make sure to update the schema
# in test_data.sql and in Iris. In particular, the column in the ORDER BY clause.
DEFAULT_PROBE_TTL_COLUMN = "probe_ttl_l4"

# Mapping of IP protocol number to caracal protocol string
PROTOCOLS = {1: "icmp", 17: "udp", 58: "icmp6"}
