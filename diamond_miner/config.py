from dataclasses import dataclass

from diamond_miner.defaults import (
    DEFAULT_PREFIX_LEN_V4,
    DEFAULT_PREFIX_LEN_V6,
    DEFAULT_PROBE_DST_PORT,
    DEFAULT_PROBE_SRC_PORT,
)
from diamond_miner.mappers import FlowMapper


@dataclass(frozen=True)
class Config:
    mapper: FlowMapper
    probe_src_addr: str
    probe_src_port: int = DEFAULT_PROBE_SRC_PORT
    probe_dst_port: int = DEFAULT_PROBE_DST_PORT
    prefix_len_v4: int = DEFAULT_PREFIX_LEN_V4
    prefix_len_v6: int = DEFAULT_PREFIX_LEN_V6
    adaptive_eps: bool = False
    far_ttl_min: int = 20
    far_ttl_max: int = 40
    max_replies_per_subset: int = 64_000_000
    probe_far_ttls: bool = False
    skip_unpopulated_ttls: bool = False
