from ipaddress import IPv4Network, IPv6Network
from typing import Union


def subnets(network: Union[IPv4Network, IPv6Network], new_prefix: int):
    """
    Faster version of ipaddress.IPv4Network.subnets(...).
    Returns only the network address as an integer.
    >>> from ipaddress import ip_network
    >>> list(subnets(ip_network("0.0.0.0/0"), new_prefix=2))
    [0, 1073741824, 2147483648, 3221225472]
    >>> subnets(ip_network("0.0.0.0/32"), new_prefix=24)
    Traceback (most recent call last):
        ...
    ValueError: new prefix must be longer
    """
    if new_prefix < network.prefixlen:
        raise ValueError("new prefix must be longer")
    start = int(network.network_address)
    end = int(network.broadcast_address) + 1
    step = (int(network.hostmask) + 1) >> (new_prefix - network.prefixlen)
    return range(start, end, step)
