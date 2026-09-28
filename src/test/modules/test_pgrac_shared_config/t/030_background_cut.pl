#-------------------------------------------------------------------------
#
# 030_background_cut.pl
#    Real auxiliary loops preserve queued work through a held local cut.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
# IDENTIFICATION
#    src/test/modules/test_pgrac_shared_config/t/030_background_cut.pl
# NOTES
#    Local test control is not distributed application or admission.
#-------------------------------------------------------------------------
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

my $node = PostgreSQL::Test::Cluster->new('background_cut');
$node->init;
$node->append_conf('postgresql.conf', "shared_preload_libraries='test_pgrac_shared_config'\n"
	. "test_pgrac_shared_config.apply_node=0\ntest_pgrac_shared_config.delivery=on\n"
	. "autovacuum=off\ncheckpoint_timeout='1h'\n");
append_to_file($node->data_dir . '/test_config.input', "common.cluster.read_scache='off'\n");
$node->start;
$node->safe_psql('postgres', q{
 CREATE EXTENSION test_pgrac_shared_config;
 CREATE TABLE background_data AS SELECT i AS id, repeat('a', 100) AS value
 FROM generate_series(1,1000) i;
 CHECKPOINT;
});
my @roles = ('checkpointer', 'background writer', 'walwriter');
sub control
{
	my ($kind, $action, $cookie) = @_;
	$cookie //= 0;
	return [split(/:/, $node->safe_psql('postgres',
		"SELECT test_pgrac_config_background($kind, '$action', $cookie)"))];
}
sub waiting
{
	my ($pid) = @_;
	for (1..40) {
		return 1 if $node->safe_psql('postgres', qq{
 SELECT coalesce(wait_event='ReconfigSharedConfigWait', false) FROM pg_stat_activity WHERE pid=$pid
}) eq 't';
		usleep(25000);
	}
	return 0;
}

for my $kind (0..2) {
	my $role = $roles[$kind];
	my $pid = $node->safe_psql('postgres', "SELECT pid FROM pg_stat_activity WHERE backend_type='$role'");
	ok($pid > 0, "actual $role exists");
	my $state = control($kind, 'close');
	ok($state->[0] && $state->[1], "local $role producer cut is closed");
	my $held = waiting($pid);
	ok($held, "actual $role waits without a held pass or error");
	# Preserve an actual missing-consumer RED, rather than letting the fixture
	# wait indefinitely. The production loop, not the raw gate, must honor CLOSE.
	if (!$held) {
		control($kind, 'open', $state->[2]);
		next;
	}
	is(control($kind, 'state')->[3], 0, 'completed old pass really retired');
	ok(!control($kind, 'open', $state->[2] - 1)->[0], 'wrong cut cannot release the original owner');
	ok(control($kind, 'bind', $state->[2])->[0], 'bind actual native common values at the empty cut');
	if ($kind == 0) {
		my $client = $node->background_psql('postgres');
		my $client_pid = $client->query_safe('SELECT pg_backend_pid()');
		$client->query_until(qr/checkpoint_queued/, "\\echo checkpoint_queued\nCHECKPOINT;\n");
		ok($node->poll_query_until('postgres', qq{
 SELECT wait_event='CheckpointStart' FROM pg_stat_activity WHERE pid=$client_pid
}), 'queued CHECKPOINT is not acknowledged as started through CLOSE');
		$node->reload;
		ok(waiting($pid), 'native reload and control cleanup do not consume the queued checkpoint');
		ok(control($kind, 'open', $state->[2])->[0], 'exact common-bound cut opens');
		$client->query_until(qr/checkpoint_done/, "\\echo checkpoint_done\n");
		is($client->{stderr}, '', 'original queued CHECKPOINT completes without error');
		$client->quit;
	} else {
		ok(control($kind, 'open', $state->[2])->[0], 'exact common-bound cut opens');
	}
	ok($node->poll_query_until('postgres', qq{
 SELECT coalesce(wait_event<>'ReconfigSharedConfigWait', true) FROM pg_stat_activity WHERE pid=$pid
}), "original $role resumes without replacement");
}
$node->safe_psql('postgres', 'UPDATE background_data SET value=reverse(value); CHECKPOINT;');
is($node->safe_psql('postgres', 'SELECT count(*) FROM background_data'), '1000', 'native work still completes');
$node->stop('fast');
$node->start;
is($node->safe_psql('postgres', 'SELECT count(*) FROM background_data'), '1000', 'normal stop and same-data restart retain every row');
$node->stop('fast');
done_testing();
