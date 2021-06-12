from ipaddress import IPv6Network

DEFAULT_PREFIX_LEN_V4 = 24
DEFAULT_PREFIX_LEN_V6 = 64

DEFAULT_PREFIX_SIZE_V4 = 2 ** (32 - DEFAULT_PREFIX_LEN_V4)
DEFAULT_PREFIX_SIZE_V6 = 2 ** (128 - DEFAULT_PREFIX_LEN_V6)

DEFAULT_PROBE_SRC_PORT = 24000
DEFAULT_PROBE_DST_PORT = 33434

UNIVERSE_SUBSET = IPv6Network("::/0")

PROTOCOLS = {1: "icmp", 17: "udp", 58: "icmp6"}
"Mapping of IP protocol numbers to caracal protocol strings."
