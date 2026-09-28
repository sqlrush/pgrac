# PGRAC: native parallel restoration must not retain a false PM receipt.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('parallel_config');
$node->init;
$node->append_conf('postgresql.conf',
	"shared_preload_libraries='test_pgrac_shared_config'\n"
	. "test_pgrac_shared_config.apply_node=0\nwork_mem='6MB'\n"
	. "max_parallel_workers=4\nmax_parallel_workers_per_gather=2\n");
append_to_file($node->data_dir . '/test_config.input', "common.work_mem='6MB'\n");
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
$node->safe_psql('postgres', 'CREATE TABLE config_parallel AS SELECT generate_series(1,10000) AS n; '
	. 'ALTER TABLE config_parallel SET (parallel_workers=2); ANALYZE config_parallel');
my $old = $node->background_psql('postgres');
$old->query_safe('SET test_pgrac_shared_config.defer_process=on; '
	. 'SET parallel_setup_cost=0; SET parallel_tuple_cost=0; '
	. 'SET min_parallel_table_scan_size=0; SET parallel_leader_participation=off');
is($old->query_safe(q{SELECT test_pgrac_config_parallel_observe()}), '0:1:1:6144',
	'leader begins with actual generation1 defaults');

append_to_file($node->data_dir . '/test_config.reload', "common.work_mem='9MB'\n");
$node->append_conf('postgresql.conf', "test_pgrac_shared_config.reload_generation=2\n");
$node->reload;
ok($node->poll_query_until('postgres',
	q{SELECT current_setting('test_pgrac_shared_config.reload_generation')='2'}),
	'actual postmaster applied generation2');
is($node->safe_psql('postgres', q{SELECT test_pgrac_config_parallel_observe()}), '0:1:2:9216',
	'ordinary fresh child inherits generation2 and9MB');
is($old->query_safe(q{SELECT test_pgrac_config_parallel_observe()}), '0:1:1:6144',
	'old query leader retains its actual generation1 and6MB');

my $query = q{SELECT min(test_pgrac_config_parallel_observe()),
 max(test_pgrac_config_parallel_observe()), count(*)
 FROM config_parallel};
like($old->query_safe('EXPLAIN (COSTS OFF) ' . $query), qr/Gather.*Partial Aggregate/s,
	'probe executes in worker partial aggregates, not above Gather');
is($old->query_safe($query), '1:1:0:6144|1:1:0:6144|10000',
	'all actual parallel workers use leader values without a selected-config receipt');
is($old->query_safe(q{SELECT test_pgrac_config_parallel_observe()}), '0:1:1:6144',
	'worker restoration cannot change the leader receipt');
$old->quit;
$node->stop('fast');
done_testing();
