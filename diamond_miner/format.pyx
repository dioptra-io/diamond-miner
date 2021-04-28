from libc.stdint cimport uint8_t, uint16_t
from libc.stdio cimport snprintf
from libc.stdlib cimport free, malloc


# https://pubs.opengroup.org/onlinepubs/9699919799/basedefs/netinet_in.h.html
# > INET6_ADDRSTRLEN: 46. Length of the string form for IPv6.
cdef size_t INET6_ADDRSTRLEN = 46

def format_probe(dst_addr_v6, uint16_t src_port, uint16_t dst_port, uint8_t ttl, str protocol):
    return f"{format_ipv6(dst_addr_v6)},{src_port},{dst_port},{ttl},{protocol}"

def format_ipv6(int_addr):
    """
    Convert IPv6 uint128 to string.
    Faster than building an ip_address object and calling str().
    """
    cdef size_t size = INET6_ADDRSTRLEN + 1
    cdef char *c_str = <char *> malloc(size * sizeof(char))
    cdef int length = format_ipv6_(int_addr, c_str, size)
    if length < 0:
        return None
    py_str = c_str[:length].decode('UTF-8')
    free(c_str)
    return py_str

cdef inline int format_ipv6_(int_addr, char* c_str, size_t size):
    cdef uint16_t a, b, c, d, e, f, g, h
    a = (int_addr & 0xFFFF0000000000000000000000000000) >> 112
    b = (int_addr & 0x0000FFFF000000000000000000000000) >> 96
    c = (int_addr & 0x00000000FFFF00000000000000000000) >> 80
    d = (int_addr & 0x000000000000FFFF0000000000000000) >> 64
    e = (int_addr & 0x0000000000000000FFFF000000000000) >> 48
    f = (int_addr & 0x00000000000000000000FFFF00000000) >> 32
    g = (int_addr & 0x000000000000000000000000FFFF0000) >> 16
    h = int_addr & 0x0000000000000000000000000000FFFF
    return snprintf(c_str, size, "%X:%X:%X:%X:%X:%X:%X:%X", a, b, c, d, e, f, g, h)
