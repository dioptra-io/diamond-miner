from collections import namedtuple
from dataclasses import dataclass

from diamond_miner.defaults import UNIVERSE_SUBSET
from diamond_miner.queries.query import LinksQuery, links_table
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class GetNextRound(LinksQuery):
    # This query is tested in test_queries.py due to its complexity.

    adaptive_eps: bool = True
    dminer_lite: bool = True
    target_epsilon: float = 0.05

    Row = namedtuple(
        "Row",
        "protocol,dst_prefix,already_sent,to_send,ttls",
    )

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
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
            arrayMap(k -> toUInt32(ceil(ln(epsilon_curr / (k + 1)) / ln((k + 1 - 1) / (k + 1)))), links_per_ttl_curr) AS mda_flows_curr,
            arrayMap(k -> toUInt32(ceil(ln(epsilon_prev / (k + 1)) / ln((k + 1 - 1) / (k + 1)))), links_per_ttl_prev) AS mda_flows_prev,
            """
        else:
            # TODO: Implement by computing Dh(v)
            raise NotImplementedError

        return f"""
        WITH
            {self.round_leq} AS current_round,
            {self.target_epsilon} AS target_epsilon,
            -- 1) Compute the links
            --  x.1       x.2        x.3
            -- (near_ttl, near_addr, far_addr)
            groupUniqArray((near_ttl, near_addr, far_addr)) AS links_curr,
            groupUniqArrayIf((near_ttl, near_addr, far_addr), near_round < current_round) AS links_prev,
            -- 2) Count the number of links per TTL
            -- extract only the TTLs, this greatly speeds-up arrayCount
            arrayMap(x -> x.1, links_curr) AS TTLs_curr,
            arrayMap(x -> x.1, links_prev) AS TTLs_prev,
            -- find the min/max TTLs
            -- we add +2 since range() is exclusive and that we compute the max over the *near* TTL
            range(arrayMin(TTLs_curr), arrayMax(TTLs_curr) + 2) AS TTLs,
            -- count distinct links per TTL
            arrayMap(t -> countEqual(TTLs_curr, t), TTLs) AS links_per_ttl_curr,
            arrayMap(t -> countEqual(TTLs_prev, t), TTLs) AS links_per_ttl_prev,
            -- find the maximum number of links over all TTLs
            arrayMax(links_per_ttl_curr) AS max_links_curr,
            arrayMax(links_per_ttl_prev) AS max_links_prev,
            -- 3) Determine if the prefix can be skipped at the next round
            -- 1 if no new links and nodes have been discovered in the current round
            equals(links_per_ttl_curr, links_per_ttl_prev) AS skip_prefix,
            -- 4) Compute MDA stopping points
            {eps_fragment}
            -- 5) Compute the number of probes to send during the next round
            {dm_fragment}
            -- compute the number of probes sent at previous round
            -- if round = 1: we known that we sent 6 probes
            -- if round > 1 and TTL > 1: we take the max of the DMiner formula
            -- over the links discovered during the previous rounds at TTL t and t-1
            if(
                current_round == 1,
                arrayMap(t -> 6, TTLs),
                arrayMap(i -> arrayMax([mda_flows_prev[i], mda_flows_prev[i - 1]]), arrayEnumerate(TTLs))
            ) AS already_sent,
            -- compute the number of probes to send during the next round
            -- => max of probes to send over TTL t and t-1
            arrayMap(i -> arrayMax([
                0,
                mda_flows_curr[i] - already_sent[i],
                mda_flows_curr[i - 1] - already_sent[i - 1]
            ]), arrayEnumerate(TTLs)) AS to_send
            -- TODO: Cleanup/optimize/rewrite/... below
            -- do not send probes to TTLs where no replies have been received
            -- it is unlikely that we will discover more at this TTL if the first 6 flows have seen nothing
            -- arrayMap(t -> arrayUniq(arrayFilter(x -> x != toIPv6('::'), arrayMap(x -> x.2, arrayFilter(x -> x.1 == t, links_curr)))), TTLs) AS nodes_per_ttl_near,
            -- arrayMap(t -> arrayUniq(arrayFilter(x -> x != toIPv6('::'), arrayMap(x -> x.3, arrayFilter(x -> x.1 + 1 == t, links_curr)))), TTLs) AS nodes_per_ttl_far,
            -- arrayMap(t -> arrayMax([nodes_per_ttl_near[t], nodes_per_ttl_far[t]]), TTLs) AS nodes_per_ttl,
            -- arrayMap(t -> if(nodes_per_ttl[t] > 0, probes[t], 0), TTLs) AS probes_final
        SELECT
            probe_protocol,
            probe_dst_prefix,
            already_sent,
            to_send,
            TTLs
        FROM {links_table(measurement_id)}
        WHERE {self.filters(subset)}
        GROUP BY (probe_protocol, probe_src_addr, probe_dst_prefix)
        HAVING NOT skip_prefix
        """
