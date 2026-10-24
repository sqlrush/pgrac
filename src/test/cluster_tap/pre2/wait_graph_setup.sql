-- Register only the existing test diagnostic APIs in native shared mode.
-- CREATE EXTENSION is unsupported there; each C entry still requires superuser.
-- Author: SqlRush <sqlrush@gmail.com>
CREATE FUNCTION test_pgrac_wait_graph_prepare()
RETURNS boolean AS '$libdir/test_pgrac_shared_config', 'test_pgrac_wait_graph_prepare'
LANGUAGE C VOLATILE;
REVOKE ALL ON FUNCTION test_pgrac_wait_graph_prepare() FROM PUBLIC;
CREATE FUNCTION test_pgrac_wait_graph()
RETURNS text AS '$libdir/test_pgrac_shared_config', 'test_pgrac_wait_graph'
LANGUAGE C VOLATILE;
REVOKE ALL ON FUNCTION test_pgrac_wait_graph() FROM PUBLIC;
CREATE FUNCTION test_pgrac_wait_graph_enqueue(bigint,bigint,bigint,bigint,bigint,bigint)
RETURNS text AS '$libdir/test_pgrac_shared_config', 'test_pgrac_wait_graph_enqueue'
LANGUAGE C VOLATILE STRICT;
REVOKE ALL ON FUNCTION test_pgrac_wait_graph_enqueue(bigint,bigint,bigint,bigint,bigint,bigint) FROM PUBLIC;
CREATE FUNCTION test_pgrac_wait_graph_block(bigint,bigint,bigint,bigint,bigint)
RETURNS text AS '$libdir/test_pgrac_shared_config', 'test_pgrac_wait_graph_block'
LANGUAGE C VOLATILE STRICT;
REVOKE ALL ON FUNCTION test_pgrac_wait_graph_block(bigint,bigint,bigint,bigint,bigint) FROM PUBLIC;
