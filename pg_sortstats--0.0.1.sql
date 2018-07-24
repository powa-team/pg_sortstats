-- This program is open source, licensed under the PostgreSQL License.
-- For license terms, see the LICENSE file.
--
-- Copyright (C) 2018: Julien Rouhaud

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_sortstats" to load this file. \quit

SET client_encoding = 'UTF8';

CREATE FUNCTION pg_sortstats(
    OUT queryid bigint,
    OUT userid oid,
    OUT dbid oid,
    OUT sort_key text,
    OUT lines bigint,
    OUT lines_to_sort bigint,
    OUT work_mems bigint,
    OUT topn_sorts bigint,
    OUT quicksorts bigint,
    OUT external_sorts bigint,
    OUT external_merges bigint,
    OUT nbtapes bigint
)
    RETURNS SETOF record
LANGUAGE c COST 1000
AS '$libdir/pg_sortstats', 'pg_sortstats';

CREATE FUNCTION pg_sortstats_reset()
    RETURNS void
LANGUAGE c COST 1000
AS '$libdir/pg_sortstats', 'pg_sortstats_reset';
