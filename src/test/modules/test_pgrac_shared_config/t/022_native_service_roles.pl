# PGRAC: real native role/ordinal enrollment, not distributed qualification.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('native_roles');
$node->init;
$node->append_conf('postgresql.conf',
	"shared_preload_libraries='test_pgrac_shared_config'\n"
	. "test_pgrac_shared_config.apply_node=0\n"
	. "test_pgrac_shared_config.delivery=on\nlogging_collector=on\nwork_mem='6MB'\n");
append_to_file($node->data_dir . '/test_config.input', "common.work_mem='6MB'\n");
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
is($node->safe_psql('postgres', 'SELECT test_pgrac_config_native_role(-1)'), '0:-1',
	'actual postmaster does not claim an auxiliary ordinal');
is($node->safe_psql('postgres', 'SELECT test_pgrac_config_native_role(pg_backend_pid())'), '4:-1',
	'actual frontend backend does not claim an auxiliary ordinal');
is($node->safe_psql('postgres', 'SELECT test_pgrac_config_native_role(-2)'), '8:-1',
	'actual detached logger does not claim an auxiliary ordinal');

for my $case (['background writer','6:1'], ['checkpointer','7:3'], ['walwriter','13:4']) {
	my ($role, $expected) = @$case;
	is($node->safe_psql('postgres', "SELECT test_pgrac_config_native_role(pid) FROM pg_stat_activity WHERE backend_type='$role'"),
		$expected, "actual $role has its own native ordinal");
}
append_to_file($node->data_dir . '/test_config.reload', "common.work_mem='9MB'\n");
$node->append_conf('postgresql.conf', "test_pgrac_shared_config.reload_generation=2\n");
$node->reload;
ok($node->poll_query_until('postgres', q{SELECT current_setting('work_mem')='9MB'}),
	'actual parent consumes the changed default');
is($node->safe_psql('postgres', q{SELECT test_pgrac_config_native_role(pid) FROM pg_stat_activity WHERE backend_type='checkpointer'}),
	'7:3', 'native reload keeps the actual auxiliary identity');
is($node->safe_psql('postgres', 'SELECT test_pgrac_config_native_role(-1)'), '0:-1',
	'native reload keeps the parent distinct from auxiliaries');
$node->stop('fast');
done_testing();
