-- PGRAC test-only functions, not part of the product catalog.
-- Author: SqlRush <sqlrush@gmail.com>
CREATE FUNCTION test_pgrac_config_active(boolean)
RETURNS text AS 'MODULE_PATHNAME', 'test_pgrac_config_active'
LANGUAGE C STRICT;
REVOKE ALL ON FUNCTION test_pgrac_config_active(boolean) FROM PUBLIC;
CREATE FUNCTION test_pgrac_config_define_common()
RETURNS void AS 'MODULE_PATHNAME', 'test_pgrac_config_define_common'
LANGUAGE C VOLATILE;
REVOKE ALL ON FUNCTION test_pgrac_config_define_common() FROM PUBLIC;
CREATE FUNCTION test_pgrac_config_active_census()
RETURNS text AS 'MODULE_PATHNAME', 'test_pgrac_config_active_census'
LANGUAGE C VOLATILE;
REVOKE ALL ON FUNCTION test_pgrac_config_active_census() FROM PUBLIC;
CREATE FUNCTION test_pgrac_config_delivery_state()
RETURNS text AS 'MODULE_PATHNAME', 'test_pgrac_config_delivery_state'
LANGUAGE C STRICT;
REVOKE ALL ON FUNCTION test_pgrac_config_delivery_state() FROM PUBLIC;
CREATE FUNCTION test_pgrac_config_census()
RETURNS text AS 'MODULE_PATHNAME', 'test_pgrac_config_census'
LANGUAGE C VOLATILE;
REVOKE ALL ON FUNCTION test_pgrac_config_census() FROM PUBLIC;

CREATE FUNCTION test_pgrac_config_delivery_refuse()
RETURNS boolean AS 'MODULE_PATHNAME', 'test_pgrac_config_delivery_refuse'
LANGUAGE C STRICT;
REVOKE ALL ON FUNCTION test_pgrac_config_delivery_refuse() FROM PUBLIC;
CREATE FUNCTION test_pgrac_config_entry(integer, text, text, boolean)
RETURNS text AS 'MODULE_PATHNAME', 'test_pgrac_config_entry'
LANGUAGE C STRICT;
CREATE FUNCTION test_pgrac_config_object(text, boolean)
RETURNS text AS 'MODULE_PATHNAME', 'test_pgrac_config_object'
LANGUAGE C STRICT;
REVOKE ALL ON FUNCTION test_pgrac_config_entry(integer, text, text, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION test_pgrac_config_object(text, boolean) FROM PUBLIC;
CREATE FUNCTION test_pgrac_config_change(text, integer, text, text, boolean)
RETURNS text AS 'MODULE_PATHNAME', 'test_pgrac_config_change'
LANGUAGE C;
REVOKE ALL ON FUNCTION test_pgrac_config_change(text, integer, text, text, boolean) FROM PUBLIC;
CREATE FUNCTION test_pgrac_config_registration()
RETURNS boolean AS 'MODULE_PATHNAME', 'test_pgrac_config_registration'
LANGUAGE C STRICT;
REVOKE ALL ON FUNCTION test_pgrac_config_registration() FROM PUBLIC;
CREATE FUNCTION test_pgrac_config_backend_apply()
RETURNS boolean AS 'MODULE_PATHNAME', 'test_pgrac_config_backend_apply'
LANGUAGE C STRICT;
REVOKE ALL ON FUNCTION test_pgrac_config_backend_apply() FROM PUBLIC;
CREATE FUNCTION test_pgrac_config_bootstrap(text, integer, text, text, text, boolean)
RETURNS text AS 'MODULE_PATHNAME', 'test_pgrac_config_bootstrap'
LANGUAGE C STRICT;
REVOKE ALL ON FUNCTION test_pgrac_config_bootstrap(text, integer, text, text, text, boolean) FROM PUBLIC;
CREATE FUNCTION test_pgrac_control_image(text, bigint)
RETURNS boolean AS 'MODULE_PATHNAME', 'test_pgrac_control_image'
LANGUAGE C STRICT;
REVOKE ALL ON FUNCTION test_pgrac_control_image(text, bigint) FROM PUBLIC;
CREATE FUNCTION test_pgrac_recovery_capacity(text, bigint)
RETURNS text AS 'MODULE_PATHNAME', 'test_pgrac_recovery_capacity'
LANGUAGE C STRICT;
REVOKE ALL ON FUNCTION test_pgrac_recovery_capacity(text, bigint) FROM PUBLIC;
CREATE FUNCTION test_pgrac_bootstrap_fixture(text)
RETURNS boolean AS 'MODULE_PATHNAME', 'test_pgrac_bootstrap_fixture'
LANGUAGE C STRICT;
REVOKE ALL ON FUNCTION test_pgrac_bootstrap_fixture(text) FROM PUBLIC;
CREATE FUNCTION test_pgrac_bootstrap_late()
RETURNS boolean AS 'MODULE_PATHNAME', 'test_pgrac_bootstrap_late'
LANGUAGE C STRICT;
REVOKE ALL ON FUNCTION test_pgrac_bootstrap_late() FROM PUBLIC;
CREATE FUNCTION test_pgrac_bootstrap_control_late(boolean)
RETURNS boolean AS 'MODULE_PATHNAME', 'test_pgrac_bootstrap_control_late'
LANGUAGE C STRICT;
REVOKE ALL ON FUNCTION test_pgrac_bootstrap_control_late(boolean) FROM PUBLIC;
CREATE FUNCTION test_pgrac_wal_publish_native(boolean)
RETURNS boolean AS 'MODULE_PATHNAME', 'test_pgrac_wal_publish_native'
LANGUAGE C STRICT;
REVOKE ALL ON FUNCTION test_pgrac_wal_publish_native(boolean) FROM PUBLIC;
CREATE FUNCTION test_pgrac_wal_history_native()
RETURNS boolean AS 'MODULE_PATHNAME', 'test_pgrac_wal_history_native'
LANGUAGE C STRICT;
REVOKE ALL ON FUNCTION test_pgrac_wal_history_native() FROM PUBLIC;
CREATE FUNCTION test_pgrac_config_reload(text, text, integer, text)
RETURNS text AS 'MODULE_PATHNAME', 'test_pgrac_config_reload'
LANGUAGE C STRICT;
REVOKE ALL ON FUNCTION test_pgrac_config_reload(text, text, integer, text) FROM PUBLIC;
CREATE FUNCTION test_pgrac_config_process(integer, text)
RETURNS text AS 'MODULE_PATHNAME', 'test_pgrac_config_process'
LANGUAGE C STRICT;
REVOKE ALL ON FUNCTION test_pgrac_config_process(integer, text) FROM PUBLIC;
CREATE FUNCTION test_pgrac_config_parallel_observe()
RETURNS text AS 'MODULE_PATHNAME', 'test_pgrac_config_parallel_observe'
LANGUAGE C STRICT PARALLEL SAFE;
REVOKE ALL ON FUNCTION test_pgrac_config_parallel_observe() FROM PUBLIC;
CREATE FUNCTION test_pgrac_config_enrollment(integer)
RETURNS text AS 'MODULE_PATHNAME', 'test_pgrac_config_enrollment'
LANGUAGE C STRICT;
REVOKE ALL ON FUNCTION test_pgrac_config_enrollment(integer) FROM PUBLIC;
CREATE FUNCTION test_pgrac_config_slot_probe(integer)
RETURNS boolean AS 'MODULE_PATHNAME', 'test_pgrac_config_slot_probe'
LANGUAGE C STRICT;
REVOKE ALL ON FUNCTION test_pgrac_config_slot_probe(integer) FROM PUBLIC;
-- PGRAC: read-only native selected-input memory lifetime, no CF admission.
-- Author: SqlRush <sqlrush@gmail.com>
CREATE FUNCTION test_pgrac_config_selection_cleanup()
RETURNS boolean AS 'MODULE_PATHNAME', 'test_pgrac_config_selection_cleanup'
LANGUAGE C STRICT;
REVOKE ALL ON FUNCTION test_pgrac_config_selection_cleanup() FROM PUBLIC;
-- PGRAC: exact detached process observation; no runtime mutation capability.
-- Author: SqlRush <sqlrush@gmail.com>
CREATE FUNCTION test_pgrac_config_delivery() RETURNS text
AS 'MODULE_PATHNAME', 'test_pgrac_config_delivery' LANGUAGE C;
REVOKE ALL ON FUNCTION test_pgrac_config_delivery() FROM PUBLIC;
