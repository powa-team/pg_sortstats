CREATE EXTENSION pg_sortstats;

SELECT pg_sortstats_reset();

CREATE TABLE sorts (id integer, val text COLLATE "C");
INSERT INTO sorts SELECT i, 'line ' || i FROM generate_series(1, 100000) i;
VACUUM ANALYZE sorts;

SET work_mem = '64kB';
WITH src AS MATERIALIZED (
    SELECT * FROM sorts ORDER BY val, id DESC
)
SELECT * FROM src LIMIT 1;
SELECT * FROM sorts ORDER BY id DESC LIMIT 1;

SELECT nb_keys, sort_keys, lines, lines_to_sort,
    work_mems < (12 * 1024) AS "exp_less_12MB",
    topn_sorts, quicksorts, external_sorts, external_merges,
    nb_tapes > 2 AS multiple_tapes,
    space_disk > 1024 AS "disk_more_1MB",
    space_memory > 1024 AS  "mem_more_1MB",
    non_parallels, COALESCE(nb_workers, 0) AS nb_workers
FROM pg_sortstats(true) ORDER BY nb_keys;

SELECT pg_sortstats_reset();

SET work_mem = '12MB';
WITH src AS MATERIALIZED (
    SELECT * FROM sorts ORDER BY val, id DESC
)
SELECT * FROM src LIMIT 1;

SELECT nb_keys, sort_keys, lines, lines_to_sort,
    work_mems < (12 * 1024) AS "exp_less_12MB",
    topn_sorts, quicksorts, external_sorts, external_merges,
    nb_tapes > 2 AS multiple_tapes,
    space_disk > 1024 AS "disk_more_1MB",
    space_memory > 1024 AS  "mem_more_1MB",
    non_parallels, COALESCE(nb_workers, 0) AS nb_workers
FROM pg_sortstats(true) ORDER BY nb_keys;

SELECT pg_sortstats_reset();
