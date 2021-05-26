import random
from dataclasses import dataclass
from enum import Enum
from functools import lru_cache
from ipaddress import IPv6Address
from typing import Dict, List, Optional, Tuple


@dataclass(frozen=True)
class Node:
    address: IPv6Address
    reply_probability = 1.0


class Protocol(Enum):
    icmp = 1
    icmp6 = 58
    udp = 17


@dataclass(frozen=True)
class Probe:
    protocol: Protocol
    dst_addr: IPv6Address
    src_port: int
    dst_port: int
    ttl: int

    @property
    def flow_id(self) -> int:
        return int(self.dst_addr) + self.src_port + self.dst_port

    @property
    def ipv6(self) -> bool:
        return not self.dst_addr.ipv4_mapped


@dataclass(frozen=True)
class Reply:
    probe_protocol: Protocol
    probe_src_addr: IPv6Address
    probe_dst_addr: IPv6Address
    probe_src_port: int
    probe_dst_port: int
    probe_ttl_l3: int
    probe_ttl_l4: int
    reply_protocol: Protocol
    reply_src_addr: IPv6Address
    reply_icmp_type: int
    reply_icmp_code: int
    reply_ttl: int
    reply_size: int
    reply_mpls_labels: List[int]
    rtt: float


class Simulator:
    """
    Simulate routers from a source to a destination prefix.
    It can simulate:
    - Routing loops
    - What else?
    """

    links: List[Tuple[Node, Node]]
    """A list of links as pairs of nodes."""

    successors: Dict[Node, List[Node]]
    """A mapping of nodes to their successors."""

    SOURCE_NODE = Node(IPv6Address("::ffff:132.227.123.9"))
    """A special node that represents the source."""

    def __init__(self, links: List[Tuple[Node, Node]]):
        self.links = links
        self.successors = {}
        for (near, far) in self.links:
            self.successors.setdefault(near, []).append(far)

    @lru_cache(maxsize=2048)
    def path_for_flow(self, flow_id: int) -> List[Node]:
        path = [self.SOURCE_NODE]
        # Stop at TTL 255 in case of routing loops.
        for _ in range(256):
            if successors := self.successors.get(path[-1]):
                path.append(successors[flow_id % len(successors)])
            else:
                break
        return path

    def simulate(self, probe: Probe) -> Optional[Reply]:
        path = self.path_for_flow(probe.flow_id)

        # If the path is shorter or equal to the TTL, there is two possible cases:
        # 1) We've reached the destination => ICMP(v6) Echo Reply for ICMP(v6)
        #    or ICMP(v6) destination unreachable for UDP
        # 2) Blackhole => no reply
        if len(path) <= probe.ttl:
            node = path[-1]
            if probe.dst_addr == node.address:
                if probe.protocol == Protocol.udp:
                    return self.destination_unreachable(probe, node)
                else:
                    return self.echo_reply(probe, node)

        # If the path is longer than the TTL, there is two possible cases:
        # 3) We've reached the destination => same as above
        # 4) We've reached a load balancer => ICMP TTL Exceeded
        else:
            node = path[probe.ttl]
            if probe.dst_addr == node.address:
                if probe.protocol == Protocol.udp:
                    return self.destination_unreachable(probe, node)
                else:
                    return self.echo_reply(probe, node)
            else:
                return self.ttl_exceeded(probe, node)

    @staticmethod
    def destination_unreachable(probe: Probe, node: Node) -> Optional[Reply]:
        raise NotImplementedError

    @staticmethod
    def echo_reply(probe: Probe, node: Node) -> Optional[Reply]:
        if random.random() <= node.reply_probability:
            return Reply(
                probe_protocol=probe.protocol,
                # We cannot recover the probe source address in an echo reply.
                probe_src_addr=IPv6Address("::"),
                probe_dst_addr=probe.dst_addr,
                # We cannot recover the probe "ports" in an echo reply.
                probe_src_port=0,
                probe_dst_port=0,
                probe_ttl_l3=probe.ttl,
                probe_ttl_l4=probe.ttl,
                reply_protocol=probe.protocol,
                reply_src_addr=node.address,
                reply_icmp_type=0 if probe.protocol == Protocol.icmp else 129,
                reply_icmp_code=0,
                reply_ttl=random.randint(0, 255),
                reply_size=random.randint(0, 65535),
                reply_mpls_labels=[],
                rtt=random.random() * 100,
            )

    @staticmethod
    def ttl_exceeded(probe: Probe, node: Node) -> Optional[Reply]:
        if random.random() <= node.reply_probability:
            # ICMP probes have no "destination port".
            probe_dst_port = 0
            if probe.protocol == Protocol.udp:
                probe_dst_port = probe_dst_port
            return Reply(
                probe_protocol=probe.protocol,
                probe_src_addr=Simulator.SOURCE_NODE.address,
                probe_dst_addr=probe.dst_addr,
                probe_src_port=probe.src_port,
                probe_dst_port=probe_dst_port,
                probe_ttl_l3=probe.ttl,
                probe_ttl_l4=probe.ttl,
                reply_protocol=probe.protocol,
                reply_src_addr=node.address,
                reply_icmp_type=3 if probe.ipv6 else 11,
                reply_icmp_code=0,
                reply_ttl=random.randint(0, 255),
                reply_size=random.randint(0, 65535),
                reply_mpls_labels=[],
                rtt=random.random() * 100,
            )


if __name__ == "__main__":
    A = Node(IPv6Address("::1"))
    B = Node(IPv6Address("::2"))
    C = Node(IPv6Address("::3"))

    sim = Simulator([(Simulator.SOURCE_NODE, A), (A, B), (B, C)])
    print(sim.successors)
    print(sim.path_for_flow(1))
    print(sim.simulate(Probe(Protocol.icmp, C.address, 24000, 33434, 2)))

# TODO: Random topologies.
