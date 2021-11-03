from ipaddress import IPv6Network
from typing import Dict, Union

DEFAULT_FAILURE_RATE = 0.05
"""Default MDA failure rate."""

DEFAULT_PREFIX_LEN_V4 = 24
"""Default prefix length for IPv4."""
DEFAULT_PREFIX_LEN_V6 = 64
"""Default prefix length for IPv6."""

DEFAULT_PREFIX_SIZE_V4 = 2 ** (32 - DEFAULT_PREFIX_LEN_V4)
"""Default prefix size (number of addresses) for IPv4."""
DEFAULT_PREFIX_SIZE_V6 = 2 ** (128 - DEFAULT_PREFIX_LEN_V6)
"""Default prefix size (number of addresses) for IPv6."""

DEFAULT_PROBE_SRC_PORT = 24000
"""Default probe source port. Encoded in the ICMP checksum field for ICMP probes."""
DEFAULT_PROBE_DST_PORT = 33434
"""Default probe destination port. Unused for ICMP probes."""

UNIVERSE_SUBSET = IPv6Network("::/0")
"""Set of all possible IP addresses."""

PROTOCOLS: Dict[Union[int, str], Union[int, str]] = {
    1: "icmp",
    17: "udp",
    58: "icmp6",
    "icmp": 1,
    "udp": 17,
    "icmp6": 58,
}
"""Mapping of IP protocol numbers to caracal protocol strings."""
