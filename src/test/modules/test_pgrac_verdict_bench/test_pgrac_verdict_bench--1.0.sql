-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_pgrac_verdict_bench" to load this file. \quit

CREATE FUNCTION test_pgrac_vb_segments()
RETURNS integer
AS 'MODULE_PATHNAME', 'test_pgrac_vb_segments'
LANGUAGE C STRICT PARALLEL UNSAFE;

CREATE FUNCTION test_pgrac_vb_provision(nsegments integer, method text)
RETURNS text
AS 'MODULE_PATHNAME', 'test_pgrac_vb_provision'
LANGUAGE C STRICT PARALLEL UNSAFE;

CREATE FUNCTION test_pgrac_vb_evict()
RETURNS text
AS 'MODULE_PATHNAME', 'test_pgrac_vb_evict'
LANGUAGE C STRICT PARALLEL UNSAFE;

CREATE FUNCTION test_pgrac_vb_run(mode text, xids xid[], iterations integer,
                                  latch_stripe boolean)
RETURNS text
AS 'MODULE_PATHNAME', 'test_pgrac_vb_run'
LANGUAGE C STRICT PARALLEL UNSAFE;

REVOKE ALL ON FUNCTION test_pgrac_vb_segments() FROM PUBLIC;
REVOKE ALL ON FUNCTION test_pgrac_vb_provision(integer, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION test_pgrac_vb_evict() FROM PUBLIC;
REVOKE ALL ON FUNCTION test_pgrac_vb_run(text, xid[], integer, boolean) FROM PUBLIC;
