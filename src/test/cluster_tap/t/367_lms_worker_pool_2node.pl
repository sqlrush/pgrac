#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 367_lms_worker_pool_2node.pl
#	  spec-7.3 D3/D7/D9 -- 2-node LMS DATA-plane worker pool: topology,
#	  HELLO gate, live multi-tag routing, and fault legs.
#
#	  L1  matched pool: both nodes cluster.lms_workers=2.  Each node runs
#	      worker 0 (LmsProcess) + worker 1 (LmsWorker), each binding its own
#	      DATA listener (data_port + worker_id).
#	  L2  shard-aligned i<->i mesh: each node's log shows the DATA plane
#	      reaching state CONNECTED on BOTH worker channels (0 and 1) -- the
#	      per-worker mesh formed, tagged by channel.
#	  L3  no worker mismatch on the matched pool + zero plane misroutes.
#	  L7  (D9 multi-tag fan-out) ping-pong X over a many-block shared table:
#	      BOTH workers' dispatch counters move on BOTH nodes (+ aggregate
#	      inline-serve liveness);  zero plane misroutes AND zero dedup-shard
#	      fail-closed drops.
#	  L8  (D9 per-tag affinity) invalidate/re-read one single-block table:
#	      the moved-worker SET is identical on both ends (double-end shard
#	      agreement) and the serving-side per-frame shard check stays clean
#	      (a heap read also fetches undo/verdict tags, so >1 worker may
#	      legitimately move — see the leg comment).
#	  L5  (D7 epoch xN) one-shot conn-reset injection: every worker resets
#	      its OWN DATA mesh; per-worker conn-reset counters + aggregate.
#	  L6  (D7 graceful recycle) SIGTERM one worker: no node cascade, the
#	      slot respawns isolated with a fresh pid, mesh re-HELLOs.
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
# L7 / L8 — spec-7.3 D9: real cross-node block traffic over the pool.
#
#	L7 (multi-tag fan-out): ping-pong X over a shared table spanning many
#	blocks.  Distinct BufferTags hash onto DISTINCT workers (D1 golden), so
#	BOTH per-worker dispatch counters move on BOTH nodes -- the D4 ring
#	routing, D5 dedup shard and D6 inline serve run xN on live traffic --
#	and the zero-misroute hygiene holds (incl. the D5 dedup-shard
#	fail-closed drop counter, SQL-surfaced at D9).
#
#	L8 (single-tag affinity): invalidate/re-read ONE single-block table.
#	Every frame of that tag rides ONE worker stream, so exactly one
#	worker's dispatch counter moves per node AND it is the SAME worker
#	index on both ends -- the two ends' shard tables agree (R1, e2e).
# ============================================================
sub poll_write_ok
{
	my ($node, $sql, $timeout_s, $label) = @_;
	$timeout_s //= 60;
	my $deadline = time + $timeout_s;
	while (time < $deadline)
	{
		return 1 if eval { $node->safe_psql('postgres', $sql); 1 };
		usleep(500_000);
	}
	diag("poll_write_ok timeout ($label)");
	return 0;
}

sub timed_update_retry
{
	my ($node, $sql, $tries) = @_;
	$tries //= 30;
	for my $attempt (1 .. $tries)
	{
		return 1 if eval { $node->safe_psql('postgres', $sql); 1 };
		usleep(500_000);
	}
	return 0;
}

ok(poll_write_ok($node0, 'CREATE TABLE wg0 (x int)', 90, 'node0 gate'),
	'L7 node0 write gate open (join admitted)');
ok(poll_write_ok($node1, 'CREATE TABLE wg1 (x int)', 90, 'node1 gate'),
	'L7 node1 write gate open (join admitted)');

# Shared-table relfilenode coincidence (t/347 OID-align pattern), for BOTH
# the multi-block L7 table and the single-block L8 table.
my ($p0, $p1, $q0, $q1) = ('', '', '', '');
for my $attempt (1 .. 8)
{
	for my $n ($node0, $node1)
	{
		$n->safe_psql('postgres',
			'CREATE TABLE pool_t (aid int, bal int) WITH (fillfactor = 50)');
		$n->safe_psql('postgres',
			'CREATE TABLE pool_one (aid int, bal int) WITH (fillfactor = 50)');
	}
	$p0 = $node0->safe_psql('postgres', q{SELECT pg_relation_filepath('pool_t')});
	$p1 = $node1->safe_psql('postgres', q{SELECT pg_relation_filepath('pool_t')});
	$q0 = $node0->safe_psql('postgres', q{SELECT pg_relation_filepath('pool_one')});
	$q1 = $node1->safe_psql('postgres', q{SELECT pg_relation_filepath('pool_one')});
	last if $p0 eq $p1 && $q0 eq $q1;
	my ($n0) = $p0 =~ /(\d+)$/;
	my ($n1) = $p1 =~ /(\d+)$/;
	my ($lag, $burn) = $n0 < $n1 ? ($node0, $n1 - $n0) : ($node1, $n0 - $n1);
	$burn = 1 if $burn <= 0;
	$lag->safe_psql('postgres',
		"SELECT lo_unlink(lo_create(0)) FROM generate_series(1, $burn)");
	for my $n ($node0, $node1)
	{
		$n->safe_psql('postgres', 'DROP TABLE pool_t');
		$n->safe_psql('postgres', 'DROP TABLE pool_one');
	}
}
is($p0, $p1, 'L7 shared-table coincidence (pool_t)');
is($q0, $q1, 'L8 shared-table coincidence (pool_one)');
$node0->safe_psql('postgres',
	'INSERT INTO pool_t SELECT g, 0 FROM generate_series(1, 5000) g');
$node0->safe_psql('postgres', 'INSERT INTO pool_one VALUES (1, 0)');
$node0->safe_psql('postgres', 'CHECKPOINT');
$node1->safe_psql('postgres', 'CHECKPOINT');

# L7 — baselines, then ping-pong the multi-block table X between the nodes.
my %l7_base;
for my $n ($node0, $node1)
{
	for my $w (0, 1)
	{
		$l7_base{ $n->name }{"disp$w"} = lms_int($n, "lms_data_dispatch_count_w$w");
		$l7_base{ $n->name }{"reply$w"} = lms_int($n, "lms_direct_reply_count_w$w");
	}
}
my $swaps = 0;
for my $r (1 .. 3)
{
	last unless timed_update_retry($node0, 'UPDATE pool_t SET bal = bal + 1', 20);
	usleep(100_000);
	last unless timed_update_retry($node1, 'UPDATE pool_t SET bal = bal + 1', 20);
	usleep(100_000);
	$swaps++;
}
ok($swaps > 0, "L7 completed $swaps multi-tag X ping-pong swaps over the DATA plane");

# Per-worker DISPATCH is the shard-balance surface: a dispatched frame is
# handled in that worker's own process, so both counters moving == distinct
# tags landed on distinct workers (fan-out) and each was served where it
# landed (the ping-pong made progress).  The per-worker direct-reply counter
# deliberately counts ONLY the D6 inline CR / undo / verdict serves
# (cluster_lms.h D8 semantic) -- block-ship replies (the dominant frames
# here) ride the SGE/envelope ship path and are out of its scope, and the
# undo serves concentrate on the ACTIVE undo head block's tag (one shard at
# a time), so only the AGGREGATE reply counter is asserted for liveness.
for my $n ($node0, $node1)
{
	my $d0 = lms_int($n, 'lms_data_dispatch_count_w0') - $l7_base{ $n->name }{disp0};
	my $d1 = lms_int($n, 'lms_data_dispatch_count_w1') - $l7_base{ $n->name }{disp1};
	my $r0 = lms_int($n, 'lms_direct_reply_count_w0') - $l7_base{ $n->name }{reply0};
	my $r1 = lms_int($n, 'lms_direct_reply_count_w1') - $l7_base{ $n->name }{reply1};

	diag('L7 ' . $n->name . " deltas: dispatch w0=$d0 w1=$d1  reply w0=$r0 w1=$r1");
	cmp_ok($d0, '>', 0, 'L7 ' . $n->name . ' worker 0 dispatched multi-tag frames');
	cmp_ok($d1, '>', 0, 'L7 ' . $n->name . ' worker 1 dispatched multi-tag frames');
	cmp_ok($r0 + $r1, '>', 0,
		'L7 ' . $n->name . ' inline CR/undo/verdict serves alive (aggregate)');
}

# L7 hygiene — fan-out produced ZERO misroutes: neither the tier1 plane gate
# nor the D5 per-worker dedup-shard fail-closed drop fired.
for my $n ($node0, $node1)
{
	is(gcs_int($n, 'plane_misroute_reject'), 0,
		'L7 ' . $n->name . ' zero plane misroutes after fan-out');
	is(gcs_int($n, 'dedup_misroute_failclosed_count'), 0,
		'L7 ' . $n->name . ' zero dedup-shard misroute drops after fan-out');
}

# L8 — quiesce, then single-BLOCK invalidate/re-read rounds on pool_one.
#
#	A "single tag" SQL workload does not exist under pgrac MVCC: every
#	cross-node read of the one heap block also fetches its writers' undo /
#	verdict blocks (runtime visibility), whose tags shard independently
#	(wire-traced at D9: the heap family rides shard(heap tag), each undo
#	fetch rides shard(undo tag) — per-tag affinity holds for EVERY tag).
#	So per-tag affinity is asserted e2e by the surfaces that check it
#	per-frame:  (a) the serving side verifies shard(tag) == its own
#	channel on every REQUEST (D5) — the fail-closed drop counter must
#	stay 0;  and (b) the same workload must move the SAME worker SET on
#	both ends (both ends run one shard table — R1 double-end agreement;
#	a skew would land a tag's frames on different indexes on the two
#	nodes).  The exact (tag -> worker) truth table is pinned in
#	test_cluster_lms_shard / test_cluster_gcs_block_shard.
my $settle_tries = 20;
my %prev;
while ($settle_tries-- > 0)
{
	my $stable = 1;
	for my $n ($node0, $node1)
	{
		for my $w (0, 1)
		{
			my $v = lms_int($n, "lms_data_dispatch_count_w$w");
			$stable = 0
			  if !defined $prev{ $n->name }{$w} || $prev{ $n->name }{$w} != $v;
			$prev{ $n->name }{$w} = $v;
		}
	}
	last if $stable;
	usleep(500_000);
}

my %l8_base;
for my $n ($node0, $node1)
{
	for my $w (0, 1)
	{
		$l8_base{ $n->name }{$w} = lms_int($n, "lms_data_dispatch_count_w$w");
	}
}
my $rounds = 0;
for my $r (1 .. 12)
{
	last
	  unless timed_update_retry($node0,
		'UPDATE pool_one SET bal = bal + 1 WHERE aid = 1', 20);
	$node1->safe_psql('postgres', 'SELECT bal FROM pool_one WHERE aid = 1');
	$rounds++;
}
ok($rounds > 0, "L8 completed $rounds single-block invalidate/re-read rounds");

my %l8_moved;
for my $n ($node0, $node1)
{
	my $d0 = lms_int($n, 'lms_data_dispatch_count_w0') - $l8_base{ $n->name }{0};
	my $d1 = lms_int($n, 'lms_data_dispatch_count_w1') - $l8_base{ $n->name }{1};

	diag('L8 ' . $n->name . " deltas: dispatch w0=$d0 w1=$d1");
	cmp_ok($d0 + $d1, '>=', 3, 'L8 ' . $n->name . ' single-block stream carried frames');
	$l8_moved{ $n->name } = ($d0 > 0 ? 1 : 0) | ($d1 > 0 ? 2 : 0);
	is(gcs_int($n, 'dedup_misroute_failclosed_count'), 0,
		'L8 ' . $n->name . ' serving-side per-tag shard check clean (0 drops)');
}
is($l8_moved{ $node0->name },
	$l8_moved{ $node1->name },
	'L8 same moved-worker SET on BOTH ends (double-end shard agreement, R1)');

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

# ============================================================
# L9 — spec-7.3 D9: N=1 whole-stack identity sentinel.  Both nodes run
# cluster.lms_workers=1 with the PRE-7.3 single data port (span 1): the
# spec-7.2 topology identity is "same topology", not just "same behavior"
# -- no worker sibling forked, ONE DATA channel (worker 0) and no worker-1
# mesh, live block traffic rides worker 0 alone (aggregate == _w0, no _w1
# rows), zero misroutes.  This is the regression sentinel of spec-7.3 §3
# contract 5 under REAL cross-node traffic.
# ============================================================
my $one = PostgreSQL::Test::ClusterPair->new_pair(
	'lmsone',
	quorum_voting_disks => 3,
	shared_data         => 1,
	storage_backend     => 'block_device',
	data_port_span      => 1,
	extra_conf          => [ @base_conf, 'cluster.lms_workers = 1' ]);
$one->start_pair;
usleep(2_000_000);
ok($one->wait_for_peer_state(0, 1, 'connected', 30), 'L9 CONTROL peers up 0->1 (N=1)');
ok($one->wait_for_peer_state(1, 0, 'connected', 30), 'L9 CONTROL peers up 1->0 (N=1)');
my ($one0, $one1) = ($one->node0, $one->node1);

# Topology identity: the worker-0 channel forms; worker 1 never exists.
ok(wait_for_log([ $one0, $one1 ], qr/HELLO verified.*\(DATA worker 0\)/, 30),
	'L9 DATA worker 0 channel mesh verified (N=1)');
my $one_merged = merged_log($one0, $one1);
unlike($one_merged, qr/DATA worker 1/, 'L9 no worker-1 channel anywhere (N=1)');
for my $n ($one0, $one1)
{
	is( $n->safe_psql('postgres',
			q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'lms worker'}),
		'0',
		'L9 ' . $n->name . ' no lms worker forked (7.2 identity)');
	is( $n->safe_psql('postgres', q{
		SELECT count(*) FROM pg_cluster_state
		 WHERE category='lms' AND key = 'lms_data_dispatch_count_w1'}),
		'0', 'L9 ' . $n->name . ' no _w1 observability row (live pool = 1)');
}

# Live traffic sentinel: block ping-pong rides worker 0 alone.
ok(poll_write_ok($one0, 'CREATE TABLE og0 (x int)', 90, 'L9 node0 gate'),
	'L9 node0 write gate open');
ok(poll_write_ok($one1, 'CREATE TABLE og1 (x int)', 90, 'L9 node1 gate'),
	'L9 node1 write gate open');
my ($o0, $o1) = ('', '');
for my $attempt (1 .. 8)
{
	$one0->safe_psql('postgres', 'CREATE TABLE one_t (aid int, bal int)');
	$one1->safe_psql('postgres', 'CREATE TABLE one_t (aid int, bal int)');
	$o0 = $one0->safe_psql('postgres', q{SELECT pg_relation_filepath('one_t')});
	$o1 = $one1->safe_psql('postgres', q{SELECT pg_relation_filepath('one_t')});
	last if $o0 eq $o1;
	my ($m0) = $o0 =~ /(\d+)$/;
	my ($m1) = $o1 =~ /(\d+)$/;
	my ($lag, $burn) = $m0 < $m1 ? ($one0, $m1 - $m0) : ($one1, $m0 - $m1);
	$burn = 1 if $burn <= 0;
	$lag->safe_psql('postgres',
		"SELECT lo_unlink(lo_create(0)) FROM generate_series(1, $burn)");
	$one0->safe_psql('postgres', 'DROP TABLE one_t');
	$one1->safe_psql('postgres', 'DROP TABLE one_t');
}
is($o0, $o1, 'L9 shared-table coincidence (one_t)');
$one0->safe_psql('postgres',
	'INSERT INTO one_t SELECT g, 0 FROM generate_series(1, 200) g');
$one0->safe_psql('postgres', 'CHECKPOINT');
$one1->safe_psql('postgres', 'CHECKPOINT');

my %l9_base;
for my $n ($one0, $one1)
{
	$l9_base{ $n->name } = 0 + $n->safe_psql('postgres', q{
		SELECT value FROM pg_cluster_state
		 WHERE category='lms' AND key='lms_data_dispatch_count_w0'});
}
my $one_swaps = 0;
for my $r (1 .. 3)
{
	last unless timed_update_retry($one0, 'UPDATE one_t SET bal = bal + 1', 20);
	usleep(100_000);
	last unless timed_update_retry($one1, 'UPDATE one_t SET bal = bal + 1', 20);
	usleep(100_000);
	$one_swaps++;
}
ok($one_swaps > 0, "L9 completed $one_swaps X ping-pong swaps (N=1)");
for my $n ($one0, $one1)
{
	my $w0 = 0 + $n->safe_psql('postgres', q{
		SELECT value FROM pg_cluster_state
		 WHERE category='lms' AND key='lms_data_dispatch_count_w0'});
	my $agg = 0 + $n->safe_psql('postgres', q{
		SELECT value FROM pg_cluster_state
		 WHERE category='lms' AND key='lms_data_dispatch_count'});

	cmp_ok($w0 - $l9_base{ $n->name },
		'>', 0, 'L9 ' . $n->name . ' worker 0 carried the traffic (N=1)');
	is($agg, $w0, 'L9 ' . $n->name . ' aggregate == _w0 (single stream identity)');
	is( $n->safe_psql('postgres',
			q{SELECT value FROM pg_cluster_state
			   WHERE category='gcs' AND key='plane_misroute_reject'}),
		'0',
		'L9 ' . $n->name . ' zero plane misroutes (N=1)');
}

$one->stop_pair;

done_testing();
