# flake8: noqa
from collections import namedtuple
from dataclasses import dataclass, field
from ipaddress import IPv4Network, IPv6Address, IPv6Network, ip_network
from typing import List, Union

from aioch import Client

CH_QUERY_SETTINGS = {
    "max_block_size": 100000,
    # Avoid timeout in case of a slow connection
    "connect_timeout": 1000,
    "send_timeout": 6000,
    "receive_timeout": 6000,
    # https://github.com/ClickHouse/ClickHouse/issues/18406
    "read_backoff_min_latency_ms": 100000,
}

IPNetwork = Union[IPv4Network, IPv6Network]


def addr_to_string(addr: IPv6Address):
    """
    >>> from ipaddress import ip_address
    >>> addr_to_string(ip_address('::dead:beef'))
    '::dead:beef'
    >>> addr_to_string(ip_address('::ffff:8.8.8.8'))
    '8.8.8.8'
    """
    return str(addr.ipv4_mapped or addr)


def ipv6(x):
    return f"toIPv6('{x}')"


def ip_in(column: str, subset: IPNetwork):
    return f"""
    ({column} >= {ipv6(subset[0])} AND {column} <= {ipv6(subset[-1])})
    """


def ip_not_in(column: str, subset: IPNetwork):
    return f"""
    ({column} < {ipv6(subset[0])} OR {column} > {ipv6(subset[-1])})
    """


@dataclass(frozen=True)
class Query:
    async def execute(self, *args, **kwargs):
        return [row async for row in self.execute_iter(*args, **kwargs)]

    async def execute_iter(
        self, client: Client, table: str, subsets=(ip_network("::/0"),)
    ):
        for subset in subsets:
            query = self.query(table, subset)
            rows = await client.execute_iter(query, settings=CH_QUERY_SETTINGS)
            async for row in rows:
                yield self.format(row)

    def format(self, row):
        return row

    def query(self, table: str, subset: IPNetwork):
        raise NotImplementedError


@dataclass(frozen=True)
class CountNodesPerTTL(Query):
    """
    Return the number of nodes discovered at each TTL.

    >>> from diamond_miner.test import execute
    >>> execute(CountNodesPerTTL('100.0.0.1'), 'test_nsdi_example')
    [(1, 1), (2, 2), (3, 3), (4, 1)]
    """

    source: str
    max_ttl: int = 255

    def query(self, table: str, subset: IPNetwork):
        return f"""
        WITH toIPv6(cutIPv6(probe_dst_addr, 8, 1)) AS probe_dst_prefix
        SELECT probe_ttl_l4, uniqExact(reply_src_addr)
        FROM {table}
        WHERE {ip_in('probe_dst_prefix', subset)}
        AND probe_src_addr = {ipv6(self.source)}
        AND reply_src_addr != probe_dst_addr
        AND reply_icmp_type = 11
        AND probe_ttl_l4 <= {self.max_ttl}
        GROUP BY probe_ttl_l4
        """


@dataclass(frozen=True)
class GetNodes(Query):
    """
    Return all the discovered nodes.

    >>> from diamond_miner.test import execute
    >>> nodes = execute(GetNodes(), 'test_nsdi_example')
    >>> sorted(nodes)
    ['150.0.1.1', '150.0.2.1', '150.0.3.1', '150.0.4.1', '150.0.5.1', '150.0.6.1', '150.0.7.1']
    """

    filter_private: bool = True

    def query(self, table: str, subset: IPNetwork):
        q = f"""
        WITH toIPv6(cutIPv6(probe_dst_addr, 8, 1)) AS probe_dst_prefix
        SELECT DISTINCT reply_src_addr
        FROM {table}
        WHERE {ip_in('probe_dst_prefix', subset)}
        AND reply_src_addr != probe_dst_addr
        AND reply_icmp_type = 11
        """
        if self.filter_private:
            q += f"""
            AND {ip_not_in('reply_src_addr', ip_network('10.0.0.0/8'))}
            AND {ip_not_in('reply_src_addr', ip_network('172.16.0.0/12'))}
            AND {ip_not_in('reply_src_addr', ip_network('192.168.0.0/16'))}
            AND {ip_not_in('reply_src_addr', ip_network('fd00::/8'))}
            """
        return q

    def format(self, row):
        return addr_to_string(row[0])


@dataclass(frozen=True)
class GetResolvedPrefixes(Query):
    """
    Return the prefixes for which no replies have been received at the previous round
    (i.e. no probes have been sent, most likely).

    >>> from diamond_miner.test import execute
    >>> execute(GetResolvedPrefixes('100.0.0.1', 1), 'test_nsdi_example')
    []
    >>> execute(GetResolvedPrefixes('100.0.0.1', 5), 'test_nsdi_example')
    ['200.0.0.0']
    """

    source: str
    round: int

    def query(self, table: str, subset: IPNetwork):
        return f"""
        WITH toIPv6(cutIPv6(probe_dst_addr, 8, 1)) AS probe_dst_prefix
        SELECT DISTINCT probe_dst_prefix
        FROM {table}
        WHERE {ip_in('probe_dst_prefix', subset)}
        AND probe_src_addr = {ipv6(self.source)}
        GROUP BY (probe_src_addr, probe_dst_prefix)
        HAVING max(round) < {self.round - 1}
        """

    def format(self, row):
        return addr_to_string(row[0])


@dataclass(frozen=True)
class GetInvalidPrefixes(Query):
    """
    Return the prefixes with per-packet LB or that sends more replies than probes.
    >>> from diamond_miner.test import execute
    >>> execute(GetInvalidPrefixes('100.0.0.1'), 'test_nsdi_example')
    []
    >>> execute(GetInvalidPrefixes('100.0.0.1'), 'test_invalid_prefixes')
    ['201.0.0.0', '202.0.0.0']
    """

    source: str

    def query(self, table: str, subset: IPNetwork):
        return f"""
        WITH toIPv6(cutIPv6(probe_dst_addr, 8, 1)) AS probe_dst_prefix,
             count(reply_src_addr)         AS n_replies,
             uniqExact(reply_src_addr)     AS n_distinct_replies
        SELECT DISTINCT probe_dst_prefix
        FROM {table}
        WHERE {ip_in('probe_dst_prefix', subset)}
        AND probe_src_addr = {ipv6(self.source)}
        GROUP BY (
            probe_src_addr,
            probe_dst_addr,
            probe_src_port,
            probe_dst_port,
            probe_ttl_l4
        )
        HAVING (n_replies > 2) OR (n_distinct_replies > 1)
        """

    def format(self, row):
        return addr_to_string(row[0])


@dataclass(frozen=True)
class GetMaxTTL(Query):
    """
    Return the maximum TTL for each (src_addr, dst_addr) pair.

    >>> from diamond_miner.test import execute
    >>> sorted(execute(GetMaxTTL(), 'test_max_ttl'))
    [('100.0.0.1', '200.0.0.1', 3), ('100.0.0.1', '201.0.0.1', 2)]
    """

    excluded_prefixes: List[str] = field(default_factory=list)

    def query(self, table: str, subset: IPNetwork):
        # CH wants at-least one element in `NOT IN (...)`
        excluded_prefixes = [f"'{x}'" for x in self.excluded_prefixes + [""]]
        return f"""
        WITH toIPv6(cutIPv6(probe_dst_addr, 8, 1)) AS probe_dst_prefix
        SELECT probe_src_addr,
               probe_dst_addr,
               max(probe_ttl_l4) AS max_ttl
        FROM {table}
        WHERE {ip_in('probe_dst_prefix', subset)}
        AND probe_dst_prefix NOT IN ({','.join(excluded_prefixes)})
        AND probe_dst_addr != reply_src_addr
        AND reply_icmp_type = 11
        GROUP BY (probe_src_addr, probe_dst_addr)
        """

    def format(self, row):
        return addr_to_string(row[0]), addr_to_string(row[1]), row[2]


@dataclass(frozen=True)
class GetNextRound(Query):
    """
    >>> from diamond_miner.test import execute
    >>> row = execute(GetNextRound('100.0.0.1', 1, adaptive_eps=False), 'test_nsdi_example')[0]
    >>> row.dst_prefix, row.min_src_port, row.min_dst_port, row.max_dst_port, row.skip_prefix
    ('200.0.0.0', 24000, 33434, 33434, 0)
    >>> row.prev_max_flow
    [6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6]
    >>> row.probes
    [5, 5, 5, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    >>> row = execute(GetNextRound('100.0.0.1', 2, adaptive_eps=False), 'test_nsdi_example')[0]
    >>> row.dst_prefix, row.min_src_port, row.min_dst_port, row.max_dst_port, row.skip_prefix
    ('200.0.0.0', 24000, 33434, 33434, 0)
    >>> row.prev_max_flow
    [11, 11, 11, 11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    >>> row.probes
    [0, 5, 5, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    >>> row = execute(GetNextRound('100.0.0.1', 3, adaptive_eps=False), 'test_nsdi_example')[0]
    >>> row.dst_prefix, row.min_src_port, row.min_dst_port, row.max_dst_port, row.skip_prefix
    ('200.0.0.0', 24000, 33434, 33434, 1)
    >>> row.prev_max_flow
    [11, 16, 16, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    >>> row.probes
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    >>> row = execute(GetNextRound('100.0.0.1', 1, adaptive_eps=False), 'test_star_node_star')[0]
    >>> row.dst_prefix, row.min_src_port, row.min_dst_port, row.max_dst_port, row.skip_prefix
    ('200.0.0.0', 24000, 33434, 33434, 0)
    >>> row.prev_max_flow
    [6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6]
    >>> row.probes
    [0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    >>> row = execute(GetNextRound('100.0.0.1', 2, adaptive_eps=False), 'test_star_node_star')[0]
    >>> row.dst_prefix, row.min_src_port, row.min_dst_port, row.max_dst_port, row.skip_prefix
    ('200.0.0.0', 24000, 33434, 33434, 0)
    >>> row.prev_max_flow
    [0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    >>> row.probes
    [0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    >>> row = execute(GetNextRound('100.0.0.1', 3, adaptive_eps=False), 'test_star_node_star')[0]
    >>> row.dst_prefix, row.min_src_port, row.min_dst_port, row.max_dst_port, row.skip_prefix
    ('200.0.0.0', 24000, 33434, 33434, 1)
    >>> row.prev_max_flow
    [0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    >>> row.probes
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    """

    source: str
    round: int
    adaptive_eps: bool = True
    dminer_lite: bool = True
    excluded_prefixes: List[str] = field(default_factory=list)
    target_epsilon: float = 0.05

    Row = namedtuple(
        "GetNextRoundRow",
        "dst_prefix,skip_prefix,probes,prev_max_flow,min_src_port,min_dst_port,max_dst_port",
    )

    def query(self, table: str, subset: IPNetwork):
        # CH wants at-least one element in `NOT IN (...)`
        excluded_prefixes = [f"'{x}'" for x in self.excluded_prefixes + [""]]

        if self.adaptive_eps:
            eps_fragment = """
            if(max_links == 0, target_epsilon, 1 - exp(log(1 - target_epsilon) / max_links))
                AS epsilon,
            if(max_links_previous == 0, target_epsilon, 1 - exp(log(1 - target_epsilon) / max_links_prev))
                AS epsilon_prev,
            """
        else:
            eps_fragment = """
            target_epsilon AS epsilon,
            target_epsilon AS epsilon_prev,
            """

        if self.dminer_lite:
            dm_fragment = """
            arrayMap(n -> nks[n + 1], links_per_ttl) AS nkv_Dhv,
            arrayMap(n -> nks_prev[n + 1], links_per_ttl_prev) AS nkv_Dhv_prev,
            """
        else:
            dm_fragment = """
            TODO: Implement by computing Dh(v)
            """

        return f"""
        WITH
            range(1, 32) AS TTLs, -- TTLs used to group nodes and links
            range(1, 256) AS ks,  -- Values of `k` used to compute MDA stopping points `n_k`.
            toIPv6(cutIPv6(probe_dst_addr, 8, 1)) AS probe_dst_prefix,
            -- 1) Compute the links
            --  x.1             x.2             x.3             x.4           x.5             x.6
            -- (probe_dst_addr, probe_src_port, probe_dst_port, probe_ttl_l3, reply_src_addr, round)
            groupUniqArray((probe_dst_addr, probe_src_port, probe_dst_port, probe_ttl_l3, reply_src_addr, round)) AS replies_unsorted,
            -- sort by (probe_dst_addr, probe_src_port, probe_dst_port, probe_ttl_l3)
            arraySort(x -> (x.1, x.2, x.3, x.4), replies_unsorted) AS replies,
            -- shift by 1: remove the element and append a NULL row
            arrayConcat(arrayPopFront(replies), [(toIPv6('::'), 0, 0, 0, toIPv6('::'), 0)]) AS replies_shifted,
            -- compute the links by zipping the two arrays
            arrayFilter(x -> x.1.4 + 1 == x.2.4, arrayZip(replies, replies_shifted)) AS links,
            -- links for round < current round
            arrayFilter(x -> x.1.6 < {self.round} AND x.2.6 < {self.round}, links) AS links_prev,
            -- 2) Count the number of nodes per TTL
            -- (probe_ttl_l3, reply_src_addr)
            arrayMap(r -> (r.4, r.5), replies) AS ttl_node,
            arrayMap(r -> (r.4, r.5), arrayFilter(x -> x.6 < {self.round}, replies)) AS ttl_node_prev,
            -- count distinct nodes per TTL
            arrayMap(t -> arrayUniq(arrayFilter(x -> x.1 == t, ttl_node)), TTLs) AS nodes_per_ttl,
            arrayMap(t -> arrayUniq(arrayFilter(x -> x.1 == t, ttl_node_prev)), TTLs) AS nodes_per_ttl_prev,
            -- find the maximum number of nodes over all TTLs
            arrayReduce('max', nodes_per_ttl) AS max_nodes,
            -- 3) Count the number of links per TTL
            -- ((probe_ttl_l3, reply_src_addr), (probe_ttl_l3, reply_src_addr))
            arrayDistinct(arrayMap(x -> ((x.1.4, x.1.5), (x.2.4, x.2.5)), links)) AS ttl_link,
            arrayDistinct(arrayMap(x -> ((x.1.4, x.1.5), (x.2.4, x.2.5)), links_prev)) AS ttl_link_prev,
            -- count distinct links per TTL
            arrayMap(t -> arrayUniq(arrayFilter(x -> x.1.1 == t, ttl_link)), TTLs) AS links_per_ttl,
            arrayMap(t -> arrayUniq(arrayFilter(x -> x.1.1 == t, ttl_link_prev)), TTLs) AS links_per_ttl_prev,
            -- find the maximum number of links over all TTLs
            arrayReduce('max', links_per_ttl) AS max_links,
            arrayReduce('max', links_per_ttl_previous) AS max_links_prev,
            -- 4) Determine if the prefix can be skipped at the next round
            -- 1 if no new links and nodes have been discovered in the current round
            equals(links_per_ttl, links_per_ttl_prev) AND equals(nodes_per_ttl, nodes_per_ttl_prev) AS skip_prefix,
            -- 5) Compute MDA stopping points
            -- epsilon (MDA probability of missing links)
            {self.target_epsilon} AS target_epsilon,
            {eps_fragment}
            -- Compute `n_k` for each `k`
            arrayMap(k -> toUInt32(ceil(ln(epsilon / k) / ln((k - 1) / k))), ks) AS nks,
            arrayMap(k -> toUInt32(ceil(ln(epsilon_prev / k) / ln((k - 1) / k))), ks) AS nks_prev,
            -- 6) Compute the number of probes to send during the next round
            {dm_fragment}
            -- compute the number of probes sent at previous round
            -- if round = 1: we known that we sent 6 probes
            -- if round > 1 and TTL = 1: we use the DMiner formula with
            -- the number of links discovered during the previous rounds
            -- if round > 1 and TTL > 1: we take the max of the DMiner formula
            -- over the links discovered during the previous rounds at TTL t and t-1
            arrayMap(t -> if({self.round} == 1, 6, if(t == 1, nkv_Dhv_prev[t], arrayReduce('max', [nkv_Dhv_prev[t], nkv_Dhv_prev[t-1]]))), TTLs) AS max_nkv_Dhv_prev,
            -- compute the number of probes to send during the next round
            -- if TTL = 1: we use the DMiner formula and we substract the
            -- number of probes sent during the previous rounds.
            -- if TTL > 1: we take the max of probes to send over TTL t and t-1
            arrayMap(t -> if(t == 1, nkv_Dhv[t] - max_nkv_Dhv_prev[t], arrayReduce('max', [nkv_Dhv[t] - max_nkv_Dhv_prev[t], nkv_Dhv[t-1] - max_nkv_Dhv_prev[t-1]])), TTLs) AS dminer_probes_nostar,
            -- 7) Compute the number of probes to send for the * node * pattern
            -- TODO: Document/verify/reformat the code below
            -- star_node_star[t] = 1 if we match the * node * pattern with a node at TTL `t`
            arrayPushFront(arrayMap(t -> (nodes_per_ttl[t - 1] = 0) AND (nodes_per_ttl[t] > 0) AND (nodes_per_ttl[t + 1] = 0), arrayPopFront(TTLs)), 0) AS star_node_star,
            -- Compute the number of probes sent during the previous round, including the * nodes * heuristic
            arrayMap(t -> if({self.round} == 1, 6, if(star_node_star[t], nks_prev[nodes_per_ttl_prev[t] + 1], max_nkv_Dhv_prev[t])), TTLs) AS prev_max_flow_per_ttl,
            -- Compute the probes to send for the * node * pattern
            arrayMap(t -> if(star_node_star[t], nks[nodes_per_ttl[t] + 1] - prev_max_flow_per_ttl[t], 0), TTLs) AS dminer_probes_star,
            -- 8) Compute the final number of probes to send
            arrayMap(t -> arrayReduce('max', [dminer_probes_nostar[t], dminer_probes_star[t]]), TTLs) AS dminer_probes
            SELECT probe_dst_prefix,
                   skip_prefix,
                   dminer_probes,
                   prev_max_flow_per_ttl,
                   min(probe_src_port),
                   min(probe_dst_port),
                   max(probe_dst_port)
            FROM {table}
            WHERE {ip_in('probe_dst_prefix', subset)}
            AND probe_dst_prefix NOT IN ({','.join(excluded_prefixes)})
            AND probe_src_addr = {ipv6(self.source)}
            AND probe_dst_addr != reply_src_addr
            AND reply_icmp_type = 11
            AND round <= {self.round}
            GROUP BY probe_dst_prefix
            HAVING length(links) >= 1 OR length(replies) >= 1
        """

    def format(self, row):
        return self.Row(addr_to_string(row[0]), *row[1:])
