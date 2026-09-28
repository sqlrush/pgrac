# PGRAC: real native process observations and typed local disagreement.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('active_census');
$node->init;
$node->append_conf('postgresql.conf',
	"shared_preload_libraries='test_pgrac_shared_config'\n"
	. "test_pgrac_shared_config.apply_node=0\n"
	. "test_pgrac_shared_config.delivery=on\nlogging_collector=on\nmax_connections=10\n");
my $body = "common.cluster.ges_handoff='off'\n";
append_to_file($node->data_dir . '/test_config.input', $body);
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
my $profile = 'SELECT test_pgrac_config_active_census()';
my $agree = q{SELECT test_pgrac_config_active_census()='1:0:0:0'};
ok($node->poll_query_until('postgres', $agree),
	'actual parent, logger, auxiliaries and backends have matching common values');
my $backend = $node->background_psql('postgres');
$backend->query_safe('BEGIN; SET LOCAL cluster.ges_handoff=on');
is($node->safe_psql('postgres', $profile), '1:1:0:0',
	'real LOCAL mutation cannot retain a current native profile');
$backend->query_safe(q{SELECT test_pgrac_config_process(1,E'common.cluster.ges_handoff=''off''\n')});
is($node->safe_psql('postgres', $profile), '1:0:0:1',
	'same consumed ref with different actual dynamic values is a mismatch');
$backend->query_safe('ROLLBACK');
is($node->safe_psql('postgres', $profile), '1:1:0:0',
	'real rollback revokes the overlay observation before old value can be reused');
$backend->query_safe(q{SELECT test_pgrac_config_process(1,E'common.cluster.ges_handoff=''off''\n')});
is($node->safe_psql('postgres', $profile), '1:0:0:0',
	'actual restoration and safe observation reestablish matching native values');
$backend->quit;
append_to_file($node->data_dir . '/test_config.reload', $body . "common.max_connections='14'\n");
$node->append_conf('postgresql.conf', "test_pgrac_shared_config.reload_generation=2\n");
$node->reload;
ok($node->poll_query_until('postgres', q{SELECT test_pgrac_config_process(0,'') LIKE '2:%'}),
	'actual parent consumes the static target');
ok($node->poll_query_until('postgres', $agree),
	'pending restart is compatible with equal old active static values, not new-value permission');
is($node->safe_psql('postgres', 'SHOW max_connections'), '10', 'actual current static value remains old');
$node->stop('fast');
done_testing();
