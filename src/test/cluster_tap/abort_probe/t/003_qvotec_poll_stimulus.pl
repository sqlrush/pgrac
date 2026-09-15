# Author: SqlRush <sqlrush@gmail.com>
# Qualify the actual background scheduling seam before any charged soak.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('poll_stimulus');
$node->init;
$node->append_conf('postgresql.conf', qq{
cluster.enabled=on
cluster.node_id=0
cluster.allow_single_node=on
cluster.interconnect_tier=stub
cluster.quorum_poll_interval_ms=2000
autovacuum=off
});
$node->start;
is($node->safe_psql('postgres', q{SELECT setting FROM pg_settings
 WHERE name='cluster.quorum_poll_interval_ms'}),
	'2000', 'fresh process uses the approved poll configuration');

my $offset = -s $node->logfile;
# The backend-local SQL arm is deliberately a decoy. It must not delay the
# separate qvotec, consume its hit, or substitute for its execution evidence.
is($node->safe_psql('postgres', q{
 SELECT cluster_inject_fault('cluster-qvotec-poll-pre','sleep',75000);
 SELECT hits FROM pg_stat_cluster_injections WHERE name='cluster-qvotec-poll-pre';
}), "t\n0", 'SQL decoy arm does not count as background consumption');
unlike(slurp_file($node->logfile, $offset), qr/poll-pre injection begin/,
	'decoy backend cannot execute the owner-only stimulus');

for my $ordinal (1, 2)
{
	$offset = -s $node->logfile;
	$node->append_conf('postgresql.conf',
		"cluster.injection_points='cluster-qvotec-poll-pre:sleep:75000'\n");
	$node->reload;
	$node->wait_for_log(qr/qvotec poll-pre injection end node=0 pid=\d+ elapsed_us=\d+/, $offset);
	# The generic counter registry is process-local, not an aux-process SQL
	# snapshot. Keep this arm across two poll periods and prove the owner's
	# actual cycle advance from its begin/disarm records below.
	$node->safe_psql('postgres', 'SELECT pg_sleep(4.1)');
	my $log = slurp_file($node->logfile, $offset);
	my @begin = $log =~ /qvotec poll-pre injection begin node=0 pid=(\d+) delay_us=75000/g;
	my @elapsed = $log =~ /qvotec poll-pre injection end node=0 pid=\d+ elapsed_us=(\d+)/g;
	is(scalar(@begin), 1, "arm $ordinal consumed by one actual qvotec exactly once");
	is(scalar(@elapsed), 1, "arm $ordinal has one completion record");
	cmp_ok($elapsed[0] // 0, '>=', 75000, "arm $ordinal real delay is not an interrupted sleep");
	# SLEEP arms deliberately survive an empty GUC. Explicit NONE must reach
	# this same background registry; a backend-local SQL disarm cannot do it.
	$node->append_conf('postgresql.conf',
		"cluster.injection_points='cluster-qvotec-poll-pre:none:0'\n");
	$node->reload;
	$node->wait_for_log(qr/qvotec poll-pre injection disarmed node=0 pid=$begin[0]/, $offset);
	pass("arm $ordinal disarm acknowledged by the same background owner");
	$log = slurp_file($node->logfile, $offset);
	my ($cycle_begin) = $log =~ /injection begin[^\n]* poll_cycle=(\d+)/;
	my ($cycle_disarm) = $log =~ /injection disarmed[^\n]* poll_cycle=(\d+)/;
	cmp_ok(($cycle_disarm // 0) - ($cycle_begin // 0), '>=', 2,
		"arm $ordinal spans two actual polls without reinjection");
}
$node->stop;
done_testing();
