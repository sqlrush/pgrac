#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 365_lms_worker_pool_2node.pl
#	  spec-7.3 D3 -- 2-node LMS DATA-plane worker-pool topology + HELLO gate.
#
#	  L1  matched pool: both nodes cluster.lms_workers=2.  Each node runs
#	      worker 0 (LmsProcess) + worker 1 (LmsWorker), each binding its own
#	      DATA listener (data_port + worker_id).
#	  L2  shard-aligned i<->i mesh: each node's log shows the DATA plane
#	      reaching state CONNECTED on BOTH worker channels (0 and 1) -- the
#	      per-worker mesh formed, tagged by channel.
#	  L3  no worker mismatch on the matched pool + zero plane misroutes.
#	  L4  n_workers mismatch (node0=2, node1=3) is refused fail-closed:
#	      the DATA HELLO verify rejects with "HELLO DATA worker mismatch"
#	      (8.A: a skew would make the two ends' shard tables disagree).
#
#	  DATA-plane per-worker peer state is not exposed via a SQL view (a
#	  backend runs on the CONTROL plane), so connectivity is asserted from
#	  the channel-tagged server log (the t/358 evidence pattern).
#
# Author: SqlRush <sqlrush@gmail.com>
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Time::HiRes qw(usleep);
use Test::More;

my @base_conf = (
	'autovacuum = off',
	'fsync = off',
	'shared_buffers = 64MB',
	'cluster.ges_request_timeout_ms = 30000',
	'cluster.gcs_reply_timeout_ms = 3000',
	'cluster.online_join = on',
	'cluster.xid_striping = on',
	'cluster.crossnode_runtime_visibility = on',
	'cluster.crossnode_cr_data_plane = on',
	'cluster.block_self_contained = on');

sub gcs_int
{
	my ($node, $key) = @_;
	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state WHERE category='gcs' AND key='$key'});
	return defined($v) && $v ne '' ? int($v) : 0;
}

# Slurp + concatenate several nodes' logs.  The tier1 mesh is asymmetric: the
# active (dialing) side logs "HELLO sent ... (active)", while the passive side
# logs "HELLO verified ... (DATA worker N)" / the reject — and which node is
# passive is a connection race, so DATA-plane assertions read the merged log.
sub merged_log
{
	my @nodes = @_;
	return join('', map { PostgreSQL::Test::Utils::slurp_file($_->logfile) } @nodes);
}

# Poll the merged log of @nodes until $re appears (the DATA mesh reaches
# CONNECTED a few seconds after CONTROL comes up).  1 on match, 0 on timeout.
sub wait_for_log
{
	my ($nodes, $re, $timeout_s) = @_;
	$timeout_s //= 30;
	my $deadline = time + $timeout_s;
	while (time < $deadline)
	{
		return 1 if merged_log(@$nodes) =~ $re;
		usleep(500_000);
	}
	return 0;
}

# ============================================================
# Matched pool: both nodes lms_workers=2.
# ============================================================
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'lmspool',
	quorum_voting_disks => 3,
	shared_data         => 1,
	storage_backend     => 'block_device',
	data_port_span      => 2,
	extra_conf          => [ @base_conf, 'cluster.lms_workers = 2' ]);
$pair->start_pair;
usleep(2_000_000);
ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'CONTROL peers up 0->1');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'CONTROL peers up 1->0');

my ($node0, $node1) = ($pair->node0, $pair->node1);

# L2 — shard-aligned mesh: poll the merged log until BOTH worker channels
# reach CONNECTED (verified on the passive side).  Worker 1 is the last to
# form; if it never does this fails (would surface a real port/offset bug),
# a few-second lag after CONTROL is normal (reconnect backoff).
ok(wait_for_log([ $node0, $node1 ], qr/HELLO verified.*\(DATA worker 1\)/, 30),
	'L2 DATA worker 1 channel mesh verified');

# Now read the settled merged log and assert the rest.
my $merged = merged_log($node0, $node1);
my $log0 = PostgreSQL::Test::Utils::slurp_file($node0->logfile);
my $log1 = PostgreSQL::Test::Utils::slurp_file($node1->logfile);

# L1 — each node spawned worker 1 (LmsWorker) and bound its DATA listener.
like($log0, qr/DATA-plane worker 1 started/, 'L1 node0 spawned LMS worker 1');
like($log1, qr/DATA-plane worker 1 started/, 'L1 node1 spawned LMS worker 1');
like($log0, qr/DATA-plane listener bound/, 'L1 node0 DATA listener bound');
like($log1, qr/DATA-plane listener bound/, 'L1 node1 DATA listener bound');

# L2b — worker 0's channel also reached CONNECTED (merged: passive side).
like($merged, qr/HELLO verified.*\(DATA worker 0\)/, 'L2 DATA worker 0 channel mesh verified');

# L3 — matched pool: no worker mismatch reject + zero plane misroutes.
unlike($merged, qr/HELLO DATA worker mismatch/, 'L3 matched pool: no worker mismatch');
is(gcs_int($node0, 'plane_misroute_reject'), 0, 'L3 node0 zero plane misroutes');
is(gcs_int($node1, 'plane_misroute_reject'), 0, 'L3 node1 zero plane misroutes');

# ============================================================
# L5 — spec-7.3 D7: epoch-reset ×N.  Each DATA worker runs its own tick in its
# OWN process and observes the shared epoch / reconfig reset signal
# independently, so a reset reaches the WHOLE pool with no cross-process driver
# (the tier1 sender gate is the structural backstop for the window between the
# bump and each worker's next tick).  Arm the one-shot conn-reset injection on
# node0 -- SIGHUP fans out to every worker pid -- and assert that BOTH worker 0
# AND worker 1 log their own per-worker DATA-mesh reset.
# ============================================================
my $w0_reset_re = qr/DATA mesh reset \(worker 0\)/;
my $w1_reset_re = qr/DATA mesh reset \(worker 1\)/;

$node0->append_conf('postgresql.conf',
	"cluster.injection_points = 'cluster-lms-conn-reset:skip'");
$node0->reload;

my $both_reset = 0;
for my $i (1 .. 60)
{
	usleep(500_000);
	my $l = PostgreSQL::Test::Utils::slurp_file($node0->logfile);
	$both_reset = ($l =~ $w0_reset_re && $l =~ $w1_reset_re) ? 1 : 0;
	last if $both_reset;
}
ok($both_reset,
	'L5 epoch ×N: both worker 0 and worker 1 reset their DATA mesh independently');

# spec-7.3 D8 — the per-worker conn-reset counters carry the same fact
# (stronger than the log grep: exact worker attribution via the dump).
sub lms_int
{
	my ($node, $key) = @_;
	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state WHERE category='lms' AND key='$key'});
	return defined($v) && $v ne '' ? int($v) : 0;
}
cmp_ok(lms_int($node0, 'lms_conn_reset_count_w0'), '>=', 1,
	'L5 worker 0 conn-reset counter moved');
cmp_ok(lms_int($node0, 'lms_conn_reset_count_w1'), '>=', 1,
	'L5 worker 1 conn-reset counter moved');
is(lms_int($node0, 'lms_conn_reset_count'),
	lms_int($node0, 'lms_conn_reset_count_w0') + lms_int($node0, 'lms_conn_reset_count_w1'),
	'L5 aggregate conn-reset = sum of the per-worker counters');

# Disarm before the recycle leg (the one-shot latch already prevents a storm;
# a later appended line wins on reload).
$node0->append_conf('postgresql.conf', "cluster.injection_points = ''");
$node0->reload;
usleep(1_000_000);

# ============================================================
# L6 — spec-7.3 D7: graceful per-worker recycle isolation (L3a, Path A).
# SIGTERM ONE DATA worker (worker 1) on node0.  A clean worker exit respawns
# just that slot via the ServerLoop WITHOUT a node crash cascade, so node0
# stays up and serving throughout, worker 1 comes back with a fresh pid, and
# its DATA mesh re-forms -- worker 0 (the other shard) is never disturbed.
# (Contrast: a SIGKILL hard-crash cascades the whole node -- KNOWN-BLOCKED on
# the orthogonal F1 recovery-vs-membership wall, see t/360 L4.)
# ============================================================
my $w1_pid = $node0->safe_psql('postgres',
	q{SELECT pid FROM pg_stat_activity
	   WHERE backend_type = 'lms worker' ORDER BY pid LIMIT 1});
if (defined $w1_pid && $w1_pid =~ /^\d+$/)
{
	my $verify_re = qr/HELLO verified.*\(DATA worker 1\)/;
	my $verified_before = () = merged_log($node0, $node1) =~ /$verify_re/g;

	kill 'TERM', $w1_pid;

	# node0 stays UP the whole time -- a crash cascade would drop SELECT 1
	# during PM_WAIT_BACKENDS; a graceful recycle never does.
	my $stayed_up = 1;
	for my $i (1 .. 10)
	{
		$stayed_up = 0 unless eval { $node0->safe_psql('postgres', 'SELECT 1'); 1 };
		usleep(300_000);
	}
	ok($stayed_up,
		'L6 node0 stayed up through the graceful worker recycle (no crash cascade)');

	# worker 1 respawned with a fresh pid (isolated restart).
	my $new_pid = '';
	for my $i (1 .. 60)
	{
		$new_pid = $node0->safe_psql('postgres',
			q{SELECT pid FROM pg_stat_activity
			   WHERE backend_type = 'lms worker' ORDER BY pid LIMIT 1});
		last if defined $new_pid && $new_pid =~ /^\d+$/ && $new_pid ne $w1_pid;
		usleep(500_000);
	}
	ok(defined $new_pid && $new_pid =~ /^\d+$/ && $new_pid ne $w1_pid,
		"L6 DATA worker 1 respawned isolated (pid $w1_pid -> $new_pid)");

	# The respawned worker re-forms its DATA mesh: a FRESH HELLO verify appears
	# (count strictly increases over the pre-kill baseline).
	my $reverified = 0;
	for my $i (1 .. 60)
	{
		my $now = () = merged_log($node0, $node1) =~ /$verify_re/g;
		$reverified = ($now > $verified_before) ? 1 : 0;
		last if $reverified;
		usleep(500_000);
	}
	ok($reverified,
		'L6 respawned worker 1 re-formed its DATA mesh (fresh re-HELLO converged)');
}
else
{
	SKIP:
	{
		skip 'no lms worker in pg_stat_activity (unexpected on a matched pool)', 3;
	}
}

$pair->stop_pair;

# ============================================================
# Mismatch pool: node0=2, node1=3 -> DATA HELLO refused fail-closed.
# ============================================================
my $mix = PostgreSQL::Test::ClusterPair->new_pair(
	'lmsmix',
	quorum_voting_disks => 3,
	shared_data         => 1,
	storage_backend     => 'block_device',
	data_port_span      => 3,
	extra_conf          => [@base_conf]);
$mix->node0->append_conf('postgresql.conf', "cluster.lms_workers = 2\n");
$mix->node1->append_conf('postgresql.conf', "cluster.lms_workers = 3\n");
$mix->start_pair;
usleep(2_000_000);
# CONTROL plane is independent of the DATA worker gate, so the cluster still
# forms; only the DATA mesh is refused.
ok($mix->wait_for_peer_state(0, 1, 'connected', 30), 'L4 CONTROL still forms under mismatch');

# The passive side rejects the peer's HELLO on the n_workers skew (2 vs 3);
# poll the merged log for the fail-closed reject (appears once HELLO is sent).
ok(wait_for_log([ $mix->node0, $mix->node1 ], qr/HELLO DATA worker mismatch/, 30),
	'L4 n_workers mismatch refused fail-closed (HELLO DATA worker mismatch)');

$mix->stop_pair;

done_testing();
