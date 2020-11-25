from cperm import Permutation
from diamond_miner_core.prober import Prober
from ipaddress import ip_address
from random import randbytes

class Probe:
    __slots__ = 'dst_addr', 'src_port', 'dst_port', 'ttl'

    def __init__(self, dst_addr, src_port, dst_port, ttl):
        self.dst_addr = dst_addr
        self.src_port = src_port
        self.dst_port = dst_port
        self.ttl = ttl

    def to_csv(self):
        return f"{self.dst_addr},{self.src_port},{self.dst_port},{self.ttl}"

if __name__ == "__main__":
    exe = "/Users/maxmouchet/Clones/github.com/dioptra-io/diamond-miner-prober/build/diamond-miner-prober"

    with Prober(exe, ["-o", "tmp", "-r", "100000", "-L", "trace"]) as prober:
        perm = Permutation(2**32-1, "cycle", "speck", randbytes(8))
        for val in perm:
            # TODO: Verify
            prefix = val & 0xFFFFFF00
            ttl = val & 0x0000001F
            host = val >> 29
            prober.send(Probe(ip_address(prefix) + host, 24000, 33434, ttl))
