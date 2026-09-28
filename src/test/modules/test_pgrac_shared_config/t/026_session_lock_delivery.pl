# PGRAC: real session locks retain old common configuration across transactions.
# Exercise native ownership and a waiting peer, not fabricated PROCLOCK flags.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('session_lock_delivery');
$node->init;
$node->append_conf('postgresql.conf', "shared_preload_libraries='test_pgrac_shared_config'\n"
	. "test_pgrac_shared_config.apply_node=0\n"
	. "test_pgrac_shared_config.delivery=on\nlogging_collector=on\nmax_connections=10\nwork_mem='6MB'\n");
sub body
{
	my ($scache, $work) = @_;
	return "common.cluster.read_scache='$scache'\ncommon.work_mem='$work'\n";
}
append_to_file($node->data_dir . '/test_config.input', body('off', '6MB'));
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
sub publish
{
	my ($generation, $image) = @_;
	open(my $fixture, '>', $node->data_dir . '/test_config.reload') or die $!;
	print {$fixture} $image;
	close($fixture) or die $!;
	$node->append_conf('postgresql.conf', "test_pgrac_shared_config.reload_generation=$generation\n");
	$node->reload;
	ok($node->poll_query_until('postgres', qq{
	 SELECT split_part(test_pgrac_config_delivery(), ':', 1) = '$generation'
	}), "native parent and logger consume generation $generation");
}
my $old = $node->background_psql('postgres', on_error_stop => 0);
my $old_work_mem = $old->query_safe('SHOW work_mem');
my $receive = 'SELECT test_pgrac_config_delivery_receive()';
my $ref = q{SELECT split_part(test_pgrac_config_process(0, ''), ':', 1)};
$old->query_safe('SELECT pg_advisory_lock(880001); SELECT pg_advisory_lock(880001)');
$old->query_safe('BEGIN; COMMIT');
publish(2, body('on', '9MB'));
is($old->query_safe($receive), 'f', 'COMMIT does not retire actual session-lock ownership');
is($old->query_safe('SHOW cluster.read_scache'), 'off', 'session-lock owner retains old common value');
is($old->query_safe($ref), '1', 'session-lock owner does not acknowledge the new image');
is($old->query_safe('SHOW work_mem'), $old_work_mem, 'no partial image application while old work survives');
is($old->query_safe('SELECT pg_advisory_unlock(880001)'), 't', 'one reentrant hold can release normally');
is($old->query_safe($receive), 'f', 'remaining reentrant ownership still defers common reload');

my $peer = $node->background_psql('postgres');
my $peer_pid = $peer->query_safe('SELECT pg_backend_pid()');
$peer->query_until(qr/peer_wait_started/, "\\echo peer_wait_started\nSELECT pg_advisory_lock(880001);\n");
ok($node->poll_query_until('postgres', qq{
 SELECT wait_event_type = 'Lock' FROM pg_stat_activity WHERE pid = $peer_pid
}), 'real peer waits on the retained native session lock');
is($old->query_safe('SELECT pg_advisory_unlock(880001)'), 't', 'owner can release its last lock while common delivery waits');
$peer->query_safe('SELECT 1');
pass('real waiting peer completes after unlock');
is($old->query_safe('SHOW cluster.read_scache'), 'on', 'idle retry applies common value after the real last release');
is($old->query_safe($ref), '2', 'post-release receipt records actual native application');
$peer->query_safe('SELECT pg_advisory_unlock_all()');
$peer->quit;

$old->query_safe('SELECT pg_advisory_lock(880002); SELECT pg_advisory_lock(880003)');
$old->query_safe('BEGIN; ROLLBACK');
publish(3, body('off', '11MB'));
is($old->query_safe($receive), 'f', 'ROLLBACK also preserves granted session ownership');
$old->query_safe('SELECT pg_advisory_unlock(880002)');
is($old->query_safe($receive), 'f', 'a different still-held session lock prevents premature reload');
$old->query_safe('BEGIN');
my (undef, $error_rc) = $old->query('SELECT 1/0');
is($error_rc, 1, 'real native ERROR aborts the transaction');
like($old->{stderr}, qr/division by zero/, 'failure is the intentional arithmetic ERROR');
$old->{stderr} = '';
$old->query_safe('ROLLBACK');
is($old->query_safe($receive), 'f', 'ERROR cleanup does not pretend the session lock was released');
$old->query_safe('SELECT pg_advisory_unlock_all()');
is($old->query_safe('SHOW cluster.read_scache'), 'off', 'unlock-all reaches the normal idle retry');
is($old->query_safe($ref), '3', 'all-lock retirement advances to the real accepted image');

$old->query_safe('BEGIN; SELECT pg_advisory_xact_lock(880004)');
publish(4, body('on', '13MB'));
is($old->query_safe($receive), 'f', 'active transaction retains transaction-scoped lock and values');
$old->query_safe('COMMIT');
is($old->query_safe('SHOW cluster.read_scache'), 'on', 'transaction-only lock creates no permanent session debt');

$old->query_safe('SELECT pg_advisory_lock(880005)');
publish(5, body('on', '15MB'));
is($old->query_safe($receive), 't', 'ordinary defaults do not acquire a common-data hold');
is($old->query_safe('SHOW work_mem'), '15MB', 'native default-only reload still works with a session lock');
publish(6, body('off', '17MB'));
is($old->query_safe($receive), 'f', 'new common change retains the last actual values');
publish(7, body('on', '19MB'));
is($old->query_safe($receive), 't', 'superseding default-only image is read instead of a retained stale target');
$old->query_safe('SELECT pg_advisory_unlock_all()');
is($old->query_safe('SHOW cluster.read_scache'), 'on', 'retired intermediate generation never applies after unlock');
is($old->query_safe($ref), '7', 'last actual accepted image owns the post-release receipt');
$old->quit;
$node->stop('fast');
done_testing();
