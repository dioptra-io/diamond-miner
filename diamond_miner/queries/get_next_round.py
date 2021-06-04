from collections import namedtuple
from dataclasses import dataclass

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries.query import Query
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class GetNextRound(Query):
    # This query is tested in test_queries.py due to its complexity.

    adaptive_eps: bool = True
    dminer_lite: bool = True
    target_epsilon: float = 0.05

    Row = namedtuple(
        "Row",
        "protocol,dst_prefix,ttls,probes,prev_max_flow",
    )

    def query(self, table: str, subset: IPNetwork = UNIVERSE_SUBSET) -> str:
        if self.adaptive_eps:
            eps_fragment = """
            if(max_links_curr == 0, target_epsilon, 1 - exp(log(1 - target_epsilon) / max_links_curr))
                AS epsilon_curr,
            if(max_links_prev == 0, target_epsilon, 1 - exp(log(1 - target_epsilon) / max_links_prev))
                AS epsilon_prev,
            """
        else:
            eps_fragment = """
            target_epsilon AS epsilon_curr,
            target_epsilon AS epsilon_prev,
            """

        if self.dminer_lite:
            dm_fragment = """
            arrayMap(n -> nks_curr[n + 1], links_per_ttl_curr) AS nkv_Dhv_curr,
            arrayMap(n -> nks_prev[n + 1], links_per_ttl_prev) AS nkv_Dhv_prev,
            """
        else:
            # TODO: Implement by computing Dh(v)
            raise NotImplementedError

        # TODO: Speed-up by ignoring resolved prefixes?
        # (As we did with the old query)

        return f"""
        WITH
            {self.round_leq} AS current_round,
            range(1, 256) AS ks,  -- Values of `k` used to compute MDA stopping points `n_k`.
            -- 1) Compute the links
            --  x.1       x.2        x.3
            -- (near_ttl, near_addr, far_addr)
            groupUniqArray((near_ttl, near_addr, far_addr)) AS links_curr,
            groupUniqArrayIf((near_ttl, near_addr, far_addr), near_round < current_round) AS links_prev,
            -- 2) Count the number of links per TTL
            -- find the min/max TTLs
            -- we add +2 since range() is exclusive and that we compute the max over the *near* TTL
            range(arrayMin(x -> x.1, links_curr), arrayMax(x -> x.1, links_curr) + 2) AS TTLs,
            -- count distinct links per TTL
            arrayMap(t -> arrayCount(x -> x.1 = t, links_curr), TTLs) AS links_per_ttl_curr,
            arrayMap(t -> arrayCount(x -> x.1 = t, links_prev), TTLs) AS links_per_ttl_prev,
            -- find the maximum number of links over all TTLs
            arrayMax(links_per_ttl_curr) AS max_links_curr,
            arrayMax(links_per_ttl_prev) AS max_links_prev,
            -- 3) Determine if the prefix can be skipped at the next round
            -- 1 if no new links and nodes have been discovered in the current round
            equals(links_per_ttl_curr, links_per_ttl_prev) AS skip_prefix,
            -- 4) Compute MDA stopping points
            -- epsilon (MDA probability of missing links)
            {self.target_epsilon} AS target_epsilon,
            {eps_fragment}
            -- Compute `n_k` for each `k`
            arrayMap(k -> toUInt32(ceil(ln(epsilon_curr / k) / ln((k - 1) / k))), ks) AS nks_curr,
            arrayMap(k -> toUInt32(ceil(ln(epsilon_prev / k) / ln((k - 1) / k))), ks) AS nks_prev,
            -- 6) Compute the number of probes to send during the next round
            {dm_fragment}
            -- compute the number of probes sent at previous round
            -- if round = 1: we known that we sent 6 probes
            -- if round > 1 and TTL = 1: we use the DMiner formula with
            -- the number of links discovered during the previous rounds
            -- if round > 1 and TTL > 1: we take the max of the DMiner formula
            -- over the links discovered during the previous rounds at TTL t and t-1
            arrayMap(t -> if(current_round == 1, 6, if(t == 1, nkv_Dhv_prev[t], arrayMax([nkv_Dhv_prev[t], nkv_Dhv_prev[t-1]]))), TTLs) AS prev_max_flow_per_ttl,
            -- compute the number of probes to send during the next round
            -- if TTL = 1: we use the DMiner formula and we substract the
            -- number of probes sent during the previous rounds.
            -- if TTL > 1: we take the max of probes to send over TTL t and t-1
            arrayMap(t -> if(t == 1, nkv_Dhv_curr[t] - prev_max_flow_per_ttl[t], arrayMax([nkv_Dhv_curr[t] - prev_max_flow_per_ttl[t], nkv_Dhv_curr[t-1] - prev_max_flow_per_ttl[t-1]])), TTLs) AS candidate_probes,
            arrayMap(t -> if(candidate_probes[t] < 0, 0, candidate_probes[t]), TTLs) AS probes,
            -- TODO: Cleanup/optimize/rewrite/... below
            -- do not send probes to TTLs where no replies have been received
            -- it is unlikely that we will discover more at this TTL if the first 6 flows have seen nothing
            arrayMap(t -> arrayUniq(arrayFilter(x -> x != toIPv6('::'), arrayMap(x -> x.2, arrayFilter(x -> x.1 == t, links_curr)))), TTLs) AS nodes_per_ttl_near,
            arrayMap(t -> arrayUniq(arrayFilter(x -> x != toIPv6('::'), arrayMap(x -> x.3, arrayFilter(x -> x.1 + 1 == t, links_curr)))), TTLs) AS nodes_per_ttl_far,
            arrayMap(t -> arrayMax([nodes_per_ttl_near[t], nodes_per_ttl_far[t]]), TTLs) AS nodes_per_ttl,
            arrayMap(t -> if(nodes_per_ttl[t] > 0, probes[t], 0), TTLs) AS probes_final
        SELECT
            probe_protocol,
            probe_dst_prefix,
            TTLs,
            probes_final,
            prev_max_flow_per_ttl
        FROM {table}
        WHERE near_round <= current_round
          AND NOT is_inter_round
          AND NOT is_virtual
          -- TODO: Temporary
          AND NOT is_partial
        GROUP BY (probe_protocol, probe_src_addr, probe_dst_prefix)
        HAVING length(links_curr) >= 1 AND NOT skip_prefix
        """
