from collections import defaultdict
from pathlib import Path
from socket import htonl


def read_file(file):
    rows = set()
    src_ips = set()
    dst_pfxs = set()
    dst_offs = set()
    sports = set()
    dports = set()
    ttls = set()
    tpls = set()
    ttl_dist = defaultdict(int)
    with file.open() as f:
        for line in f:
            src, dst, sport, dport, ttl = line.split(",")
            dst = htonl(int(dst))
            dst_pfx = int(dst) & 0xFFFFFF00
            dst_off = int(dst) & 0x000000FF
            rows.add(line)
            src_ips.add(src)
            dst_pfxs.add(dst_pfx)
            dst_offs.add(dst_off)
            sports.add(sport)
            dports.add(dport)
            tpls.add((dst_pfx, dst_off, ttl))
            ttls.add(ttl)
            ttl_dist[int(ttl)] += 1
    return rows, src_ips, dst_pfxs, dst_offs, sports, dports, ttls, tpls, ttl_dist


def print_set_stats(a, b):
    print(f"a = {len(a)}")
    print(f"b = {len(b)}")
    print(f"a & b = {len(a & b)}")
    print(f"a - b = {len(a - b)}")
    print(f"b - a = {len(b - a)}")


if __name__ == "__main__":
    file_test = Path("resources/reader_test_1.csv")
    file_new = Path("resources/reader_new_full_2.csv")

    rows1, src1, dstp1, dsto1, sports1, dports1, ttls1, tpls1, dist1 = read_file(
        file_test
    )
    rows2, src2, dstp2, dsto2, sports2, dports2, ttls2, tpls2, dist2 = read_file(
        file_new
    )

    for i in range(40):
        print(f"[{i}] {dist1[i]} {dist2[i]}")

    print("a = test set, b = new set")
    print("src ip")
    print_set_stats(src1, src2)
    print("dst pfx")
    print_set_stats(dstp1, dstp2)
    print("dst off")
    print_set_stats(dsto1, dsto2)
    print("sport")
    print_set_stats(sports1, sports2)
    print("dport")
    print_set_stats(dports1, dports2)
    print("ttl")
    print_set_stats(ttls1, ttls2)
    print("tpls")
    print_set_stats(tpls1, tpls2)
    print("rows")
    print_set_stats(rows1, rows2)

    # print(rows1 - rows2)
