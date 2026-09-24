-- PGRAC test-only functions, not part of the product catalog.
-- Author: SqlRush <sqlrush@gmail.com>
CREATE FUNCTION test_pgrac_config_entry(integer, text, text, boolean)
RETURNS text AS 'MODULE_PATHNAME', 'test_pgrac_config_entry'
LANGUAGE C STRICT;
CREATE FUNCTION test_pgrac_config_object(text, boolean)
RETURNS text AS 'MODULE_PATHNAME', 'test_pgrac_config_object'
LANGUAGE C STRICT;
REVOKE ALL ON FUNCTION test_pgrac_config_entry(integer, text, text, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION test_pgrac_config_object(text, boolean) FROM PUBLIC;
CREATE FUNCTION test_pgrac_config_registration()
RETURNS boolean AS 'MODULE_PATHNAME', 'test_pgrac_config_registration'
LANGUAGE C STRICT;
REVOKE ALL ON FUNCTION test_pgrac_config_registration() FROM PUBLIC;
