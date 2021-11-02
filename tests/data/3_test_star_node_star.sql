-- We reuse the NSDI example, but with no replies at TTL 2 and 4
INSERT INTO probes__test_star_node_star
SELECT *
FROM probes__test_nsdi_example;

INSERT INTO results__test_star_node_star
SELECT *
FROM results__test_nsdi_example
WHERE probe_ttl NOT IN (2, 4);
