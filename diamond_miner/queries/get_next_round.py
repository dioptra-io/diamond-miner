from collections import namedtuple
from dataclasses import asdict, dataclass

from diamond_miner.defaults import DEFAULT_SUBSET
from diamond_miner.queries.fragments import IPNetwork
from diamond_miner.queries.get_invalid_prefixes import GetInvalidPrefixes
from diamond_miner.queries.get_resolved_prefixes import GetResolvedPrefixes
from diamond_miner.queries.query import Query


@dataclass(frozen=True)
class GetNextRound(Query):
    # This query is tested in test_queries.py due to its complexity.

    adaptive_eps: bool = True
    dminer_lite: bool = True
    target_epsilon: float = 0.05

    Row = namedtuple(
        "Row",
        "dst_prefix,skip_prefix,probes,prev_max_flow,min_src_port,min_dst_port,max_dst_port",
    )

    def query(self, table: str, subset: IPNetwork = DEFAULT_SUBSET) -> str:
        params = {
            x: y
            for x, y in asdict(self).items()
            if x not in ["adaptive_eps", "dminer_lite", "target_epsilon"]
        }
        invalid_prefixes_query = GetInvalidPrefixes(**params).query(table, subset)
        resolved_prefixes_query = GetResolvedPrefixes(**params).query(table, subset)

        if self.adaptive_eps:
            eps_fragment = """
            if(max_links == 0, target_epsilon, 1 - exp(log(1 - target_epsilon) / max_links))
                AS epsilon,
            if(max_links_prev == 0, target_epsilon, 1 - exp(log(1 - target_epsilon) / max_links_prev))
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
            range(1, 32)  AS TTLs, -- TTLs used to group nodes and links
            range(1, 256) AS ks,  -- Values of `k` used to compute MDA stopping points `n_k`.
            invalid_prefixes  AS ({invalid_prefixes_query}),
            resolved_prefixes AS ({resolved_prefixes_query}),
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
            arrayFilter(x -> x.1.6 < {self.round_leq} AND x.2.6 < {self.round_leq}, links) AS links_prev,
            -- 2) Count the number of nodes per TTL
            -- (probe_ttl_l3, reply_src_addr)
            arrayMap(r -> (r.4, r.5), replies) AS ttl_node,
            arrayMap(r -> (r.4, r.5), arrayFilter(x -> x.6 < {self.round_leq}, replies)) AS ttl_node_prev,
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
            arrayReduce('max', links_per_ttl_prev) AS max_links_prev,
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
            arrayMap(t -> if({self.round_leq} == 1, 6, if(t == 1, nkv_Dhv_prev[t], arrayReduce('max', [nkv_Dhv_prev[t], nkv_Dhv_prev[t-1]]))), TTLs) AS max_nkv_Dhv_prev,
            -- compute the number of probes to send during the next round
            -- if TTL = 1: we use the DMiner formula and we substract the
            -- number of probes sent during the previous rounds.
            -- if TTL > 1: we take the max of probes to send over TTL t and t-1
            arrayMap(t -> if(t == 1, nkv_Dhv[t] - max_nkv_Dhv_prev[t], arrayReduce('max', [nkv_Dhv[t] - max_nkv_Dhv_prev[t], nkv_Dhv[t-1] - max_nkv_Dhv_prev[t-1]])), TTLs) AS dminer_probes_nostar,
            -- 7) Compute the number of probes to send for the * node * pattern
            -- star_node_star[t] = 1 if we match the * node * pattern with a node at TTL `t`
            arrayPushFront(arrayMap(t -> (nodes_per_ttl[t - 1] = 0) AND (nodes_per_ttl[t] > 0) AND (nodes_per_ttl[t + 1] = 0), arrayPopFront(TTLs)), 0) AS star_node_star,
            -- Compute the number of probes sent during the previous round, including the * nodes * heuristic
            arrayMap(t -> if({self.round_leq} == 1, 6, if(star_node_star[t], nks_prev[nodes_per_ttl_prev[t] + 1], max_nkv_Dhv_prev[t])), TTLs) AS prev_max_flow_per_ttl,
            -- Compute the probes to send for the * node * pattern
            arrayMap(t -> if(star_node_star[t], nks[nodes_per_ttl[t] + 1] - prev_max_flow_per_ttl[t], 0), TTLs) AS dminer_probes_star,
            -- 8) Compute the final number of probes to send
            arrayMap(t -> arrayReduce('max', [dminer_probes_nostar[t], dminer_probes_star[t]]), TTLs) AS dminer_probes
        SELECT
            probe_dst_prefix,
            skip_prefix,
            dminer_probes,
            prev_max_flow_per_ttl,
            min(probe_src_port),
            min(probe_dst_port),
            max(probe_dst_port)
        FROM {table}
        WHERE {self.common_filters(subset)}
            AND probe_dst_prefix NOT IN invalid_prefixes
            AND probe_dst_prefix NOT IN resolved_prefixes
        GROUP BY (probe_src_addr, probe_dst_prefix)
        HAVING length(links) >= 1 OR length(replies) >= 1
        """
