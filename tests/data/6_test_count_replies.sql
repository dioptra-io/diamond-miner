-- Table with replies from prefixes spread over /0
-- 2 replies in 1.0.0.0/8
-- 1 reply in 2.0.0.0/8
-- 1 reply in 204.0.0/8
INSERT INTO probes__test_count_replies
VALUES (1, '::ffff:1.0.0.0', 1, 1, 1),
       (1, '::ffff:1.1.0.0', 1, 1, 1),
       (1, '::ffff:2.0.0.0', 1, 1, 1),
       (1, '::ffff:204.0.0.0', 1, 1, 1);

INSERT INTO results__test_count_replies
VALUES (0, 1, '::ffff:100.0.0.1', '::ffff:1.0.0.0', 24000, 33434, 1, 1, '::ffff:150.0.0.1', 1, 11, 0, 250, 0, [], 0.0, 1),
       (0, 1, '::ffff:100.0.0.1', '::ffff:1.1.0.0', 24000, 33434, 1, 1, '::ffff:150.0.0.1', 1, 11, 0, 250, 0, [], 0.0, 1),
       (0, 1, '::ffff:100.0.0.1', '::ffff:2.0.0.0', 24000, 33434, 1, 1, '::ffff:150.0.0.1', 1, 11, 0, 250, 0, [], 0.0, 1),
       (0, 1, '::ffff:100.0.0.1', '::ffff:204.0.0.0', 24000, 33434, 1, 1, '::ffff:150.0.0.1', 1, 11, 0, 250, 0, [], 0.0, 1);
