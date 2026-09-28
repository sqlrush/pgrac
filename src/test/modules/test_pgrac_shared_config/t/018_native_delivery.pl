# PGRAC: real native PM/fork/detached logger delivery, selected source injected
# by the existing test-only assign hook. Not a LMON/CF or node-ACK qualification.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('native_delivery');
$node->init;
$node->append_conf('postgresql.conf', "shared_preload_libraries='test_pgrac_shared_config'\n"
	. "test_pgrac_shared_config.apply_node=0\n"
	. "test_pgrac_shared_config.delivery=on\nlogging_collector=on\nwork_mem='6MB'\n");
append_to_file($node->data_dir . '/test_config.input', "common.work_mem='6MB'\n");
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
my $logger = $node->safe_psql('postgres', 'SELECT test_pgrac_config_delivery()');
like($logger, qr/^1:\d+:\d+:1$/, 'actual detached logger enrolled with inherited startup outcome');
if ($logger eq 'none')
{
	$node->stop('fast');
	done_testing();
	exit 1; # Real missing native delivery is the bounded RED, not a long timeout.
}
my (undef, $registration, $pid) = split(/:/, $logger);
is($node->safe_psql('postgres', 'SELECT test_pgrac_config_delivery_refuse()'), 't',
	'an actual backend cannot publish for LMON or the parent');
my $old = $node->background_psql('postgres');
$old->query_safe('SET test_pgrac_shared_config.defer_process=on');
append_to_file($node->data_dir . '/test_config.reload', "common.work_mem='9MB'\n");
$node->append_conf('postgresql.conf', "test_pgrac_shared_config.reload_generation=2\n");
$node->reload;
ok($node->poll_query_until('postgres', q{
 SELECT split_part(test_pgrac_config_delivery(), ':', 1) = '2'
}), 'detached logger applied the actual parent-accepted generation');
is($node->safe_psql('postgres', 'SHOW work_mem'), '9MB', 'new child inherits actual new parent value');
is($old->query_safe('SHOW work_mem'), '6MB', 'old delayed session did not acquire a fabricated ACK');
is($old->query_safe(q{SELECT test_pgrac_config_process(0, '')}), '1:0:0:0:1',
	'old session retains actual old ref');
$old->quit;

# A valid parent update can fail in a real child's native assign hook. It must
# not kill logging or manufacture success; replacement is a new lifetime.
open(my $fixture, '>', $node->data_dir . '/test_config.reload') or die $!;
print {$fixture} "common.cluster.native_config_logger_failure='2'\ncommon.work_mem='9MB'\n";
close($fixture) or die $!;
$node->append_conf('postgresql.conf', "test_pgrac_shared_config.reload_generation=3\n");
$node->reload;
ok($node->poll_query_until('postgres', q{
 SELECT test_pgrac_config_delivery_state() = '2:1'
}), 'real logger partial failure stays old and failed, never acknowledges target');
is($node->safe_psql('postgres', q{SELECT split_part(test_pgrac_config_delivery(), ':', 3)}),
	$pid, 'partial failure did not kill the detached logging process');
is($node->safe_psql('postgres', q{SELECT test_pgrac_config_process(0, '')}),
	'3:0:0:0:1', 'healthy new session inherits successful actual parent, not failed logger');

# A later image cannot erase the logger's potentially partial native state.
$node->append_conf('postgresql.conf', "test_pgrac_shared_config.reload_generation=4\n");
$node->reload;
ok($node->poll_query_until('postgres', q{
 SELECT split_part(test_pgrac_config_process(0, ''), ':', 1) = '4'
}), 'actual parent advances independently of the failed child');
is($node->safe_psql('postgres', 'SELECT test_pgrac_config_delivery_state()'), '2:1',
	'a later target cannot clear failed logger state');

# SIGKILL only the disposable native logger. PG reaps/replaces it; no data
# process or shared cluster scene is killed, and no crash recovery is tested.
ok(kill('KILL', $pid), 'terminate disposable logger to exercise its actual reaper');
ok($node->poll_query_until('postgres', qq{
 SELECT test_pgrac_config_delivery() <> 'none'
 AND NULLIF(split_part(test_pgrac_config_delivery(), ':', 3), '')::int <> $pid
}), 'actual new logger lifetime, not a sticky dead PID');
my $replacement = $node->safe_psql('postgres', 'SELECT test_pgrac_config_delivery()');
my ($generation, $next_registration, $next_pid, $role) = split(/:/, $replacement);
is($generation, 4, 'replacement inherits actual parent generation');
cmp_ok($next_registration, '>', $registration, 'registration serial is not reset on logger reuse');
is($role, 1, 'replacement observation is from the actual logger role');
$node->stop('fast');
done_testing();
