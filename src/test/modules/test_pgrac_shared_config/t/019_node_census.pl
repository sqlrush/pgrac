# PGRAC: actual native participant census, not cluster/member admission.
# Selected-source injection is test-only; production consumers are real.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('node_census');
$node->init;
$node->append_conf('postgresql.conf',
	"shared_preload_libraries='test_pgrac_shared_config'\n"
	. "test_pgrac_shared_config.apply_node=0\n"
	. "test_pgrac_shared_config.delivery=on\nlogging_collector=on\n"
	. "work_mem='6MB'\nmax_connections=10\n");
append_to_file($node->data_dir . '/test_config.input', "common.work_mem='6MB'\n");
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
my $all_current = q{WITH c AS (SELECT string_to_array(test_pgrac_config_census(), ':') a)
 SELECT CASE WHEN array_length(a,1)=9 THEN
 a[1]::int=a[2]::int AND a[3]::int=0 AND a[4]::int=0 AND a[5]::int=0
 ELSE false END FROM c};
ok($node->poll_query_until('postgres', $all_current),
	'actual parent, children, auxiliaries and detached logger observed current');
my $old = $node->background_psql('postgres');
$old->query_safe('SET test_pgrac_shared_config.defer_process=on');
append_to_file($node->data_dir . '/test_config.reload', "common.work_mem='9MB'\n");
$node->append_conf('postgresql.conf', "test_pgrac_shared_config.reload_generation=2\n");
$node->reload;
ok($node->poll_query_until('postgres', q{SELECT current_setting('work_mem')='9MB'}),
	'actual parent and newly born backend use the new default');
ok($node->poll_query_until('postgres', q{WITH c AS
 (SELECT string_to_array(test_pgrac_config_census(), ':') a)
 SELECT CASE WHEN array_length(a,1)=9 THEN a[3]::int>0 AND a[4]::int=0 ELSE false END FROM c}),
	'parent current cannot hide the actual old delayed child');
is($old->query_safe('SHOW work_mem'), '6MB', 'delayed child really still uses old native value');
$old->query_safe(q{SELECT test_pgrac_config_process(2,E'common.work_mem=''9MB''\n')});
ok($node->poll_query_until('postgres', $all_current),
	'actual child application closes the old observation');
$old->query_safe(q{DO $$ BEGIN
 PERFORM test_pgrac_config_process(3,
 E'common.check_function_bodies=''off''\ncommon.cluster.native_config_process_failure=''2''\ncommon.work_mem=''9MB''\n');
EXCEPTION WHEN OTHERS THEN
 IF SQLERRM <> 'test native reload assignment failure' THEN RAISE; END IF;
END $$});
ok($node->poll_query_until('postgres', q{WITH c AS
 (SELECT string_to_array(test_pgrac_config_census(), ':') a)
 SELECT CASE WHEN array_length(a,1)=9 THEN a[4]::int=1 ELSE false END FROM c}),
	'actual partial-hook failure is distinct from target waiting');
$old->quit;
ok($node->poll_query_until('postgres', $all_current),
	'actual exited lifetime is absent, not silently healed in place');
unlink($node->data_dir . '/test_config.reload') or die "remove only disposable fixture: $!";
append_to_file($node->data_dir . '/test_config.reload',
	"common.max_connections='14'\ncommon.work_mem='9MB'\n");
$node->append_conf('postgresql.conf', "test_pgrac_shared_config.reload_generation=3\n");
$node->reload;
ok($node->poll_query_until('postgres', q{WITH c AS
 (SELECT string_to_array(test_pgrac_config_census(), ':') a)
 SELECT CASE WHEN array_length(a,1)=9 THEN a[1]::int=a[2]::int
 AND a[6]::int>0 AND a[8]::bigint>0 ELSE false END FROM c}),
	'consumed target and cumulative pending-restart coexist, never active-value proof');
is($node->safe_psql('postgres', 'SHOW max_connections'), '10',
	'pending native static value has not become active');
$node->stop('fast');
done_testing();
