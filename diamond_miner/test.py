from ipaddress import IPv6Address

url = "clickhouse://localhost"


def addr_to_string(addr: IPv6Address) -> str:
    """
    >>> from ipaddress import ip_address
    >>> addr_to_string(ip_address('::dead:beef'))
    '::dead:beef'
    >>> addr_to_string(ip_address('::ffff:8.8.8.8'))
    '8.8.8.8'
    """
    return str(addr.ipv4_mapped or addr)


def create_tables(url: str, measurement_id: str) -> None:
    # Avoid circular imports.
    from diamond_miner.queries.create_tables import CreateTables
    from diamond_miner.queries.drop_tables import DropTables

    DropTables().execute(url, measurement_id)
    CreateTables().execute(url, measurement_id)
