# flake8: noqa
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
    ['::ffff:200.0.0.0']
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
        return row[0]


@dataclass(frozen=True)
class GetInvalidPrefixes(Query):
    """
    Return the prefixes with per-packet LB or that sends more replies than probes.
    >>> from diamond_miner.test import execute
    >>> execute(GetInvalidPrefixes('100.0.0.1'), 'test_nsdi_example')
    []
    >>> execute(GetInvalidPrefixes('100.0.0.1'), 'test_invalid_prefixes')
    ['::ffff:201.0.0.0', '::ffff:202.0.0.0']
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
        return row[0]


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

    round: int
    adaptive_eps: bool = True
    excluded_prefixes: List[str] = field(default_factory=list)

    def query(self, table: str, subset: IPNetwork):
        # CH wants at-least one element in `NOT IN (...)`
        excluded_prefixes = [f"'{x}'" for x in self.excluded_prefixes + [""]]

        if self.adaptive_eps:
            eps_fragment = """
            if(max_links == 0, target_epsilon, 1 - exp(log(1 - target_epsilon) / max_links))
                AS epsilon,
            if(max_links_previous == 0, target_epsilon, 1 - exp(log(1 - target_epsilon) / max_links_previous))
                AS epsilon_previous,
            """
        else:
            eps_fragment = """
            target_epsilon AS epsilon,
            target_epsilon AS epsilon_previous,
            """

        return f"""
        WITH
            toIPv6(cutIPv6(probe_dst_addr, 8, 1)) AS probe_dst_prefix,
            -- replies_s
            --  x.1             x.2             x.3             x.4             x.5           x.6
            -- (probe_dst_addr, probe_src_port, probe_dst_port, reply_src_addr, probe_ttl_l3, round)
            groupUniqArray((probe_dst_addr, probe_src_port, probe_dst_port, reply_src_addr, probe_ttl_l3, round)) AS replies_s,
            -- sorted_replies_s
            -- replies_s sorted by (probe_dst_addr, probe_src_port, probe_dst_port, probe_ttl_l3)
            arraySort(x -> (x.1, x.2, x.3, x.5), replies_s) AS sorted_replies_s,
            -- replies_d
            -- sorted_replies_s with the first element removed
            arrayPopFront(sorted_replies_s) AS replies_d,
            -- replies_d_sized
            -- replies_d with (0, 0, 0, 0, 0, 0) appended
            -- (Essentially, sorted_replies_s shifted by 1 item)
            arrayConcat(replies_d, [(toIPv6('::'), 0, 0, toIPv6('::'), 0, 0)]) AS replies_d_sized,
            -- replies_no_round
            -- (reply_src_addr, probe_ttl_l3) over all rounds
            -- TODO: Is this necessary (can't we do with replies_s directly)?
            arrayMap(r -> (r.4, r.5), replies_s) AS replies_no_round,
            -- replies_no_round_previous
            -- (reply_src_addr, probe_ttl_l3) for round < current round
            -- TODO: x.6 instead?
            arrayMap(r -> (r.4, r.5), arrayFilter(x -> x.6 < {self.round}, replies_s)) AS replies_no_round_previous,
            -- potential_links
            arrayZip(sorted_replies_s, replies_d_sized) AS potential_links,
            -- links
            -- remove non-consecutive TTLs
            arrayFilter(x -> x.1.5 + 1 == x.2.5, potential_links) AS links,
            -- links_previous
            -- links for round < current round
            arrayFilter(x -> x.1.6 < {self.round} AND x.2.6 < {self.round}, links) AS links_previous,
            -- links_no_round
            -- ((reply_src_addr, probe_ttl_l3), (reply_src_addr, probe_ttl_l3))
            arrayDistinct(arrayMap(x -> ((x.1.4, x.1.5), (x.2.4, x.2.5)), links)) AS links_no_round,
            -- links_no_round_previous
            -- links_no_round for round < current round
            arrayDistinct(arrayMap(x -> ((x.1.4, x.1.5), (x.2.4, x.2.5)), links_previous)) AS links_no_round_previous,
            -- ttls
            -- (1, 2, ... 31)
            range(1, 32) AS ttls,
            -- links_per_ttl
            -- Number of distinct links between TTL t AND t+1
            -- [(1, x), (2, y), ...]
            -- TODO: Can we instead group by links by TTL earlier on?
            arrayMap(t -> (t, arrayUniq(arrayFilter(x -> x.1.2 == t, links_no_round))), ttls) AS links_per_ttl,
            -- links_per_ttl_previous
            -- links_per_ttl for round < current round
            arrayMap(t -> (t, arrayUniq(arrayFilter(x -> x.1.2 == t, links_no_round_previous))), ttls) AS links_per_ttl_previous,
            -- max_links
            -- The maximum number of links over all TTLs
            arrayReduce('max', arrayMap(t -> t.2, links_per_ttl)) AS max_links,
            -- max_links_previous
            -- max_links for round < current_round
            arrayReduce('max', arrayMap(t -> t.2, links_per_ttl_previous)) AS max_links_previous,
            -- nodes_per_ttl
            -- Number of distinct nodes for each TTL
            -- [(1, x), (2, y), ...]
            arrayMap(t -> arrayUniq(arrayFilter(x -> x.2 == t, replies_no_round)), ttls) AS nodes_per_ttl,
            -- nodes_per_ttl_previous
            -- nodes_per_ttl for round < current_round
            arrayMap(t -> arrayUniq(arrayFilter(x -> x.2 == t, replies_no_round_previous)), ttls) AS nodes_per_ttl_previous,
            -- max_nodes
            -- The maximum number of nodes over all TTLs
            arrayReduce('max', nodes_per_ttl) AS max_nodes,
            -- skip_prefix
            -- 1 if no new links have been discovered in the current round
            if(equals(links_per_ttl, links_per_ttl_previous), 1, 0) AS skip_prefix,
            -- epsilon (MDA probability of missing links)
            -- Here we make epsilon decrease (exponentially fast) AS the number of links increases.
            0.05 AS target_epsilon,
            {eps_fragment}
            -- nks (MDA stopping points)
            -- Compute the values of `k`
            range(1, arrayReduce('max', [max_links + 2, max_nodes + 2])) AS nks_index,
            -- Compute `n_k` for each `k`
            arrayMap(k -> toUInt32(ceil(ln(epsilon / k) / ln((k - 1) / k))), nks_index) AS nks,
            arrayMap(k -> toUInt32(ceil(ln(epsilon_previous / k) / ln((k - 1) / k))), nks_index) AS nks_previous,
            -- D-Miner Lite Formula
            -- Number of probes to send at each TTLs
            arrayMap(t -> (t.1, nks[t.2 + 1]), links_per_ttl) AS nkv_Dhv,
            arrayMap(t -> (t.1, nks_previous[t.2 + 1]), links_per_ttl_previous) AS nkv_Dhv_previous,
            -- TODO: Document/verify/reformat the code below
            -- Compute the probes sent at previous round
            arrayMap(t -> (t, if({self.round} == 1, 6, if(t == 1, nkv_Dhv_previous[t].2, arrayReduce('max', [nkv_Dhv_previous[t].2, nkv_Dhv_previous[t-1].2])))), ttls) AS max_nkv_Dhv_previous,
            arrayMap(t -> (t, toInt32(if(t == 1, nkv_Dhv[t].2 - max_nkv_Dhv_previous[t].2, arrayReduce('max', [nkv_Dhv[t].2 - max_nkv_Dhv_previous[t].2, nkv_Dhv[t-1].2 - max_nkv_Dhv_previous[t-1].2])))), ttls) AS d_miner_lite_probes,
            -- *-node-*
            arrayMap(t -> (t, if(nodes_per_ttl[t] == 0, 0, d_miner_lite_probes[t].2)), ttls) AS d_miner_lite_probes_no_probe_star,
            arraySlice(ttls, 2) AS sliced_ttls,
            arrayMap(t -> (t, if(((nodes_per_ttl[ttls[t - 1]]) = 0) AND ((nodes_per_ttl[ttls[t]]) > 0) AND ((nodes_per_ttl[ttls[t + 1]]) = 0), nks[nodes_per_ttl[ttls[t]] + 1] - nks_previous[nodes_per_ttl_previous[ttls[t]] + 1], 0)), sliced_ttls) AS d_miner_paper_probes_w_star_nodes_star,
            arrayPushFront(d_miner_paper_probes_w_star_nodes_star, (1, 0)) AS d_miner_paper_probes_w_star_nodes_star_new,
            arrayMap(t -> (t, arrayReduce('max', [d_miner_paper_probes_w_star_nodes_star_new[t].2, d_miner_lite_probes_no_probe_star[t].2])), ttls) AS final_probes,
            -- Compute max flow for previous round, it's th w/ the * nodes * heuristic
            arrayMap(t->(t, toInt32(if(nodes_per_ttl[ttls[t-1]] == 0 AND nodes_per_ttl[ttls[t]] > 0 AND nodes_per_ttl[ttls[t+1]] == 0, nks_previous[nodes_per_ttl_previous[ttls[t]] + 1], max_nkv_Dhv_previous[ttls[t]].2))), sliced_ttls) AS previous_max_flow_per_ttl,
            arrayPushFront(previous_max_flow_per_ttl, max_nkv_Dhv_previous[ttls[1]]) AS previous_max_flow_per_ttl_final
            SELECT probe_src_addr,
                   probe_dst_prefix,
--                    skip_prefix,
                   final_probes
--                    previous_max_flow_per_ttl_final,
--                    min(probe_src_port), min(probe_dst_port), max(probe_dst_port)
            FROM {table}
            WHERE {ip_in('probe_dst_prefix', subset)}
            AND probe_dst_prefix NOT IN ({','.join(excluded_prefixes)})
            AND round <= {self.round}
            AND probe_dst_addr != reply_src_addr
            AND reply_icmp_type = 11
            GROUP BY (probe_src_addr, probe_dst_prefix)
            HAVING length(links) >= 1 OR length(replies_s) >= 1
        """
