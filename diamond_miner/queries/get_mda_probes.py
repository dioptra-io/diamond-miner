from dataclasses import dataclass

from diamond_miner.defaults import DEFAULT_FAILURE_RATE, UNIVERSE_SUBSET
from diamond_miner.queries.query import LinksQuery, links_table, probes_table
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class GetMDAProbes(LinksQuery):
    adaptive_eps: bool = True
    dminer_lite: bool = True
    target_epsilon: float = DEFAULT_FAILURE_RATE

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        if self.adaptive_eps:
            eps_fragment = """
            arrayMax(links_per_ttl) AS max_links,
            if(max_links == 0, target_epsilon, 1 - exp(log(1 - target_epsilon) / max_links))
                AS epsilon,
            """
        else:
            eps_fragment = """
            target_epsilon AS epsilon,
            """

        if self.dminer_lite:
            dm_fragment = """
            arrayMap(k -> toUInt32(ceil(ln(epsilon / (k + 1)) / ln((k + 1 - 1) / (k + 1)))), links_per_ttl) AS mda_flows,
            """
        else:
            # TODO: Implement by computing Dh(v)
            raise NotImplementedError

        return f"""
        WITH
            {self.target_epsilon} AS target_epsilon,
            -- 1) Compute the links
            --  x.1       x.2        x.3
            -- (near_ttl, near_addr, far_addr)
            groupUniqArray((near_ttl, near_addr, far_addr)) AS links,
            -- 2) Count the number of links per TTL
            -- extract only the TTLs, this greatly speeds-up arrayCount
            arrayMap(x -> x.1, links) AS links_ttls,
            -- find the min/max TTLs
            -- we add +2 since range() is exclusive and that we compute the max over the *near* TTL
            range(arrayMin(links_ttls), arrayMax(links_ttls) + 2) AS TTLs,
            -- count distinct links per TTL
            arrayMap(t -> countEqual(links_ttls, t), TTLs) AS links_per_ttl,
            -- 3) Compute MDA stopping points
            {eps_fragment}
            -- 4) Compute the number of probes to send during the next round
            {dm_fragment}
            -- compute the number of probes to send during the next round
            -- => max of probes to send over TTL t and t-1
            arrayMap(i -> arrayMax([mda_flows[i], mda_flows[i - 1]]), arrayEnumerate(TTLs)) AS cumulative_probes
            -- TODO: Cleanup/optimize/rewrite/... below
            -- do not send probes to TTLs where no replies have been received
            -- it is unlikely that we will discover more at this TTL if the first 6 flows have seen nothing
            -- (see GetNextRoundStateless)
        SELECT
            probe_protocol,
            probe_dst_prefix,
            cumulative_probes,
            TTLs
        FROM {links_table(measurement_id)} AS links_table
        WHERE {self.filters(subset)}
        GROUP BY (probe_protocol, probe_src_addr, probe_dst_prefix)
        """


@dataclass(frozen=True)
class InsertMDAProbes(GetMDAProbes):
    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        assert self.round_leq
        return f"""
        INSERT INTO {probes_table(measurement_id)}
        WITH
            arrayJoin(arrayZip(TTLs, cumulative_probes)) AS ttl_probe
        SELECT
            probe_protocol,
            probe_dst_prefix,
            ttl_probe.1,
            ttl_probe.2,
            {self.round_leq + 1}
        FROM ({super().statement(measurement_id, subset)})
        """
