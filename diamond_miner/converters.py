import math
from hashlib import sha256
from typing import Iterator, List, Union
from uuid import UUID

from diamond_miner import __version__
from diamond_miner.defaults import PROTOCOLS


def format_addr(addr: str) -> str:
    if addr.startswith("::ffff:"):
        return addr[7:]
    return addr


def measurement_id_from_name(name: str) -> int:
    m = sha256()
    m.update(name.encode("utf-8"))
    return int.from_bytes(m.digest()[:4], "little")


def probe_id_from_uuid(uuid: Union[UUID, str]) -> int:
    m = sha256()
    m.update(str(uuid).encode())
    return int.from_bytes(m.digest()[:4], "little")


def from_ripe_atlas(traceroute: dict) -> dict:
    """
    >>> from diamond_miner.test import url
    >>> from diamond_miner.queries import GetTraceroutes
    >>> rows = GetTraceroutes().execute(url, 'test_nsdi_example')
    >>> row = sorted(rows, key=lambda x: x["probe_dst_addr"])[0]
    >>> atlas = to_ripe_atlas("measurement", "agent", **row, ipv4_mapped=True)
    >>> res = from_ripe_atlas(atlas)
    >>> res == row
    True
    """
    probe_protocol = PROTOCOLS[traceroute["proto"].lower()]
    probe_src_addr = traceroute["src_addr"]
    probe_dst_addr = traceroute["dst_addr"]
    probe_src_port = traceroute["paris_id"]
    replies = []

    for result in traceroute["result"]:
        for result_ in result["result"]:
            reply_mpls_labels = []
            for obj in result_.get("icmpext", {}).get("obj", []):
                for entry in obj.get("mpls", []):
                    reply_mpls_labels.append(
                        (entry["label"], entry["exp"], entry["s"], entry["ttl"])
                    )

            replies.append(
                [
                    0,
                    result["hop"],
                    result_["ttl"],
                    result_["size"],
                    reply_mpls_labels,
                    result_["from"],
                    result_["rtt"],
                ]
            )

    return {
        "probe_protocol": probe_protocol,
        "probe_src_addr": probe_src_addr,
        "probe_dst_addr": probe_dst_addr,
        "probe_src_port": probe_src_port,
        "replies": replies,
    }


def to_ripe_atlas(
    measurement_uuid: Union[UUID, str],
    agent_uuid: Union[UUID, str],
    probe_protocol: int,
    probe_src_addr: str,
    probe_dst_addr: str,
    probe_src_port: int,
    replies: List[dict],
    *,
    ipv4_mapped: bool = False,
) -> dict:
    """
    >>> from diamond_miner.test import url
    >>> from diamond_miner.queries import GetTraceroutes
    >>> rows = GetTraceroutes().execute(url, 'test_nsdi_example')
    >>> rows = sorted(rows, key=lambda x: x["probe_dst_addr"])
    >>> atlas = to_ripe_atlas("measurement", "agent", **rows[0])
    >>> atlas["src_addr"]
    '100.0.0.1'
    >>> atlas["dst_addr"]
    '200.0.0.0'
    >>> atlas["result"][0]
    {'hop': 1, 'result': [{'from': '150.0.1.1', 'rtt': 0, 'size': 0, 'ttl': 250}]}
    >>> atlas["result"][3]
    {'hop': 4, 'result': [{'from': '150.0.6.1', 'rtt': 0, 'size': 0, 'ttl': 250}]}
    """
    af = 4 if probe_dst_addr.startswith("::ffff:") else 6
    if ipv4_mapped:
        probe_src_addr_str = probe_src_addr
        probe_dst_addr_str = probe_dst_addr
    else:
        probe_src_addr_str = format_addr(probe_src_addr)
        probe_dst_addr_str = format_addr(probe_dst_addr)
    start_timestamp = math.inf
    end_timestamp = -math.inf
    results = []

    for (
        capture_timestamp,
        probe_ttl,
        reply_ttl,
        reply_size,
        reply_mpls_labels,
        reply_src_addr,
        rtt,
    ) in replies:
        start_timestamp = min(start_timestamp, capture_timestamp)
        end_timestamp = max(end_timestamp, capture_timestamp)
        result = {
            "from": reply_src_addr if ipv4_mapped else format_addr(reply_src_addr),
            "rtt": rtt,
            "size": reply_size,
            "ttl": reply_ttl,
        }
        if reply_mpls_labels:
            result["icmpext"] = {
                "obj": [
                    {
                        "mpls": [
                            {"exp": exp, "label": label, "s": s, "ttl": ttl}
                            for (label, exp, s, ttl) in reply_mpls_labels
                        ]
                    }
                ]
            }
        results.append({"hop": probe_ttl, "result": [result]})

    return {
        "af": af,
        "fw": 1,  # TODO: Iris version (must be int)?
        "msm_name": f"{measurement_uuid}__{agent_uuid}",
        "msm_id": measurement_id_from_name(f"{measurement_uuid}__{agent_uuid}"),
        "prb_id": probe_id_from_uuid(agent_uuid),
        "dst_addr": probe_dst_addr_str,
        "dst_name": probe_dst_addr_str,
        "src_addr": probe_src_addr_str,
        "from": probe_src_addr_str,
        "paris_id": probe_src_port,
        "proto": str(PROTOCOLS[probe_protocol]).upper(),
        "result": results,
        "size": 0,  # TODO: Can we know this? Infer from protocol?
        "timestamp": start_timestamp,
        "endtime": end_timestamp,
        "stored_timestamp": end_timestamp,
        "mver": __version__,
        "type": "traceroute",
    }


def unfold(
    probe_protocol: int,
    probe_src_addr: str,
    probe_dst_addr: str,
    probe_src_port: int,
    replies: List[dict],
) -> Iterator[tuple]:
    # TODO: Return dict instead?
    """
    >>> from diamond_miner.test import url
    >>> from diamond_miner.queries import GetTraceroutes
    >>> rows = GetTraceroutes().execute(url, 'test_nsdi_example')
    >>> rows = sorted(rows, key=lambda x: x["probe_dst_addr"])
    >>> next(unfold(**rows[0]))
    (0, 1, '::ffff:100.0.0.1', '::ffff:200.0.0.0', 24000, 0, 1, 0, '::ffff:150.0.1.1', 1, 11, 0, 250, 0, [], 0, 1)
    """
    for (
        capture_timestamp,
        probe_ttl,
        reply_ttl,
        reply_size,
        reply_mpls_labels,
        reply_src_addr,
        rtt,
    ) in replies:
        yield (
            capture_timestamp,
            probe_protocol,
            probe_src_addr,
            probe_dst_addr,
            probe_src_port,
            0,
            probe_ttl,
            0,
            reply_src_addr,
            1,  # TODO: Handle ICMPv6
            11,  # TODO: Handle ICMPv6 Echo Reply
            0,  # TODO: Handle ICMPv6 Echo Reply
            reply_ttl,
            reply_size,
            reply_mpls_labels,
            rtt,
            1,
        )
