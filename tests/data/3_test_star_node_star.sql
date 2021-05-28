-- We reuse the NSDI example, but with no replies at TTL 2 and 4

INSERT INTO test_star_node_star
SELECT *
FROM test_nsdi_example
WHERE probe_ttl NOT IN (2, 4);
