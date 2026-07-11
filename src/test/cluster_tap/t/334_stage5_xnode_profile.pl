#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 334_stage5_xnode_profile.pl
#    spec-5.59 D9 — cross-node profiling buckets: hard-availability legs.
#
#    Measure-only (same paradigm as t/300 / spec-5.50): boots native /
#    single-cluster / 2-node ClusterPair tiers, drives all four axes
#    (write / read / index / commit-SCN) with cluster.xnode_profile = on,
#    and asserts the profiling surface is alive — dump keys present,
#    per-axis n_events > 0, read amortization probe recording, sum
#    sanity, off-on-off toggling harmless, observe-only legs recorded.
#    NO tax-percentage thresholds are asserted anywhere.
#
#    Legs:
#      L1  three boot tiers come up (native / single cluster / pair)
#      L2  pair peers connected + in_quorum
#      L3  write axis: cross-node writes -> >=1 W bucket n_events > 0
#      L4  read axis: node1 cross-node reads -> R bucket + reship probe
#      L5  index axis: right-growing index -> rightmost-leaf probe counts
#          (hard); index-block-transfer aggregate is OBSERVED only (a real
#          cross-node index-block transfer needs node1 to touch the index,
#          which has no shared catalog in this tier -- see the L5 comment)
#      L6  commit axis: SCN commit-advance + BOC broadcast n_events > 0
#      L7  sum sanity: requester decision buckets <= workload wall clock
#      L8  off -> on -> off toggle leaves the hot path healthy
#      L9  observe-only: concurrent same-block write pressure recorded
#          (errors are OBSERVATIONS, never failures)
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#    src/test/cluster_tap/t/334_stage5_xnode_profile.pl
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use Time::HiRes qw(time);

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use PostgreSQL::Test::ClusterPair;
use Test::More;

# ----------
# helpers
# ----------
sub poll_sql_eq
{
	my ($node, $sql, $want, $timeout_s) = @_;
	$timeout_s //= 15;
	my $deadline = time + $timeout_s;
	my $last = '(never-queried)';
	while (time < $deadline)
	{
		my $got = eval { $node->safe_psql('postgres', $sql); };
		$last = defined $got ? $got : '(undef)';
		return 1 if defined $got && $got eq $want;
		select(undef, undef, undef, 0.25);
	}
	diag("poll_sql_eq timeout after ${timeout_s}s: want='$want' last='$last' sql=$sql");
	return 0;
}

# Cross-node statements can hit retryable fail-closed errors (rule-8.A
# behaviour, e.g. "TT slot recycled -> retry"); retry a bounded number of
# times and report success/failure without dying.
sub psql_retry
{
	my ($node, $sql, $tries) = @_;
	$tries //= 5;
	for my $i (1 .. $tries)
	{
		my $ok = eval { $node->safe_psql('postgres', $sql); 1 };
		return 1 if $ok;
		select(undef, undef, undef, 0.3);
	}
	return 0;
}

# One xnode_profile dump value (bigint) from a node.
sub xp_val
{
	my ($node, $key) = @_;
	my $v = $node->safe_psql('postgres',
		"SELECT value FROM pg_cluster_state WHERE category='xnode_profile' AND key='$key'");
	return defined $v && $v ne '' ? $v + 0 : 0;
}

# Sum of n_events over buckets whose key matches a LIKE pattern.
sub xp_sum_events
{
	my ($node, $like) = @_;
	my $v = $node->safe_psql('postgres',
		"SELECT COALESCE(sum(value::bigint),0) FROM pg_cluster_state "
		. "WHERE category='xnode_profile' AND key LIKE '$like'");
	return defined $v && $v ne '' ? $v + 0 : 0;
}

# Sum of total_nanos over the requester decision buckets (P1-4 table 1:
# requester-exclusive-wait only; nested/service/overlay buckets excluded).
my @DECISION_NANOS_KEYS = qw(
	bucket.w_gcs_x_request.total_nanos
	bucket.r_gcs_s_request.total_nanos
	bucket.w_ges_enqueue.total_nanos
	bucket.w_ges_convert.total_nanos
	bucket.w_hw_extend.total_nanos
	bucket.r_cr_construct.total_nanos
	bucket.r_tt_visibility_resolve.total_nanos
	bucket.c_scn_commit_advance.total_nanos
	bucket.local_undo_itl_wal.total_nanos
);

sub xp_decision_nanos
{
	my ($node) = @_;
	my $sum = 0;
	$sum += xp_val($node, $_) for @DECISION_NANOS_KEYS;
	return $sum;
}

my @perf_conf = (
	"autovacuum = off\n",
	"fsync = off\n",
	"shared_buffers = 64MB\n",
);

# =====================================================================
# Tier 1 + 2: native and single-cluster boots (L1 partial, L8).
# =====================================================================
my $native = PostgreSQL::Test::Cluster->new('xp_native');
$native->init;
$native->append_conf('postgresql.conf', $_) for @perf_conf;
$native->start;
ok(1, 'L1a native node boots');
is($native->safe_psql('postgres', 'SELECT 1'), '1', 'L1a native answers');

my $ic_port = PostgreSQL::Test::Cluster::get_free_port();
my $solo = PostgreSQL::Test::Cluster->new('xp_solo');
$solo->init;
$solo->append_conf('postgresql.conf', "cluster.enabled = on\n");
$solo->append_conf('postgresql.conf', "cluster.interconnect_tier = tier1\n");
$solo->append_conf('postgresql.conf', "cluster.allow_single_node = on\n");
$solo->append_conf('postgresql.conf', "cluster.node_id = 0\n");
$solo->append_conf('postgresql.conf', $_) for @perf_conf;
PostgreSQL::Test::Utils::append_to_file(
	$solo->data_dir . '/pgrac.conf',
	"[cluster]\nname = xp_solo\n\n[node.0]\ninterconnect_addr = 127.0.0.1:$ic_port\n\n");
$solo->start;
ok(1, 'L1b single cluster node boots');

# Dump surface exists and is all-zero while the GUC is off.
my $key_count = $solo->safe_psql('postgres',
	"SELECT count(*) FROM pg_cluster_state WHERE category='xnode_profile'");
is($key_count, '61', 'xnode_profile dump surface: 61 keys (28 buckets x2 + 5 probes)');
my $nonzero_off = $solo->safe_psql('postgres',
	"SELECT count(*) FROM pg_cluster_state WHERE category='xnode_profile' "
	. "AND key LIKE 'bucket.%' AND value <> '0'");
is($nonzero_off, '0', 'GUC off: all buckets zero');

# L8: off -> on -> off toggle with a small write workload at each step.
$solo->safe_psql('postgres',
	'CREATE TABLE t334_l8 (id serial PRIMARY KEY, v int); '
	. 'INSERT INTO t334_l8 (v) SELECT g FROM generate_series(1,200) g;');
$solo->safe_psql('postgres', "ALTER SYSTEM SET cluster.xnode_profile = on");
$solo->safe_psql('postgres', 'SELECT pg_reload_conf()');
poll_sql_eq($solo, "SHOW cluster.xnode_profile", 'on', 10)
  or diag('cluster.xnode_profile did not turn on via reload');
$solo->safe_psql('postgres', 'UPDATE t334_l8 SET v = v + 1');
my $l8_on_local = xp_val($solo, 'bucket.local_undo_itl_wal.n_events');
$solo->safe_psql('postgres', "ALTER SYSTEM SET cluster.xnode_profile = off");
$solo->safe_psql('postgres', 'SELECT pg_reload_conf()');
poll_sql_eq($solo, "SHOW cluster.xnode_profile", 'off', 10);
my $l8_frozen = xp_val($solo, 'bucket.local_undo_itl_wal.n_events');
$solo->safe_psql('postgres', 'UPDATE t334_l8 SET v = v + 1');
is($solo->safe_psql('postgres', 'SELECT count(*) FROM t334_l8'), '200',
	'L8 off->on->off: workload healthy across toggles');
cmp_ok($l8_on_local, '>', 0, 'L8 GUC on: local undo/ITL/WAL bucket counted');
is(xp_val($solo, 'bucket.local_undo_itl_wal.n_events'), $l8_frozen,
	'L8 GUC off again: bucket frozen (no further accumulation)');

$native->stop;
$solo->stop;

# =====================================================================
# Tier 3: 2-node ClusterPair with profiling ON from boot (L1c/L2-L7/L9).
# =====================================================================
my $pair_ok = 0;
my $pair;
eval {
	my @pair_conf = map { my $l = $_; chomp $l; $l } @perf_conf;
	$pair = PostgreSQL::Test::ClusterPair->new_pair(
		'xp_pair',
		quorum_voting_disks => 3,
		shared_data         => 1,
		extra_conf          => [
			@pair_conf,
			'cluster.xnode_profile = on',
			'cluster.quorum_poll_interval_ms = 500',
			'cluster.cssd_heartbeat_interval_ms = 2000',
			'cluster.cssd_dead_deadband_factor = 10',
		]);
	$pair->start_pair;
	$pair_ok = 1;
};
ok($pair_ok, 'L1c 2-node ClusterPair boots') or diag($@);

my $ready =
	 $pair_ok
  && $pair->wait_for_peer_state(0, 1, 'connected', 30)
  && $pair->wait_for_peer_state(1, 0, 'connected', 30)
  && poll_sql_eq($pair->node0, 'SELECT in_quorum FROM pg_cluster_quorum_state', 't', 20)
  && poll_sql_eq($pair->node1, 'SELECT in_quorum FROM pg_cluster_quorum_state', 't', 20);
ok($ready, 'L2 peers connected + in_quorum');

my $pair_legs_ok = 0;
if ($ready)
{
	# Phantom-shared-block harness (same pattern as t/280): identical DDL
	# on both nodes must land the same relfilepath, then node0 seeds rows
	# and CHECKPOINTs them to shared storage so node1's access goes
	# through cross-node coordination.  Plain heap only (no PK, no text/
	# TOAST): duplicated index DDL cannot work over coincident files (the
	# second node's btbuild finds the first node's metapage).  The index
	# axis is exercised separately below via a node0-only index whose
	# blocks hash-master ~50% onto node1.
	$pair->node0->safe_psql('postgres', 'CREATE TABLE t334 (id int, v int)');
	$pair->node1->safe_psql('postgres', 'CREATE TABLE t334 (id int, v int)');
	my $t0 = $pair->node0->safe_psql('postgres', "SELECT pg_relation_filepath('t334')");
	my $t1 = $pair->node1->safe_psql('postgres', "SELECT pg_relation_filepath('t334')");
	$pair_legs_ok = ($t0 eq $t1) ? 1 : 0;
	diag("relfilepath coincidence does not hold: table n0=$t0 n1=$t1")
		unless $pair_legs_ok;
	if ($pair_legs_ok)
	{
		$pair->node0->safe_psql('postgres',
			'INSERT INTO t334 SELECT g, g FROM generate_series(1,500) g');
		$pair->node0->safe_psql('postgres', 'CHECKPOINT');
	}
}

SKIP:
{
	skip 'pair not ready or relfilepath coincidence does not hold', 10
		unless $ready && $pair_legs_ok;

	my $node0 = $pair->node0;
	my $node1 = $pair->node1;

	# ----------
	# L3 write axis: writes from BOTH nodes touch cross-node block X /
	# GES / HW paths (node1's writes hit node0-seeded blocks).
	# ----------
	my $w_wall_start = time;
	psql_retry($node0, 'UPDATE t334 SET v = v + 1 WHERE id <= 250');
	psql_retry($node1, 'UPDATE t334 SET v = v + 1 WHERE id > 250');
	psql_retry($node1,
		'INSERT INTO t334 SELECT 500 + g, g FROM generate_series(1,300) g');
	my $w_wall_ns = (time - $w_wall_start) * 1_000_000_000;

	my $w_events = xp_sum_events($node0, 'bucket.w_%.n_events')
	  + xp_sum_events($node1, 'bucket.w_%.n_events');
	cmp_ok($w_events, '>', 0, "L3 write axis: >=1 W bucket n_events > 0 (got $w_events)");

	# ----------
	# L4 read axis: node1 repeatedly reads node0-written blocks.
	# ----------
	psql_retry($node0, 'UPDATE t334 SET v = v + 10 WHERE id <= 100');
	my $r_wall_start = time;
	for my $i (1 .. 3)
	{
		psql_retry($node1, 'SELECT count(*), sum(v) FROM t334');
	}
	my $r_wall_ns = (time - $r_wall_start) * 1_000_000_000;

	my $r_events = xp_sum_events($node1, 'bucket.r_%.n_events');
	cmp_ok($r_events, '>', 0, "L4 read axis: >=1 R bucket n_events > 0 (got $r_events)");

	my $reship = xp_val($node1, 'read_reship_count');
	my $sholder = xp_val($node1, 'read_sholder_hit_count');
	ok(1, "L4 read amortization probe recorded (reship=$reship sholder_hit=$sholder)");
	diag(sprintf('L4 reship rate: %s',
		($reship + $sholder) > 0
			? sprintf('%.1f%% (%d reship / %d total)',
				100.0 * $reship / ($reship + $sholder), $reship, $reship + $sholder)
			: 'n/a (no cross-node reads recorded)'));

	# ----------
	# L5 index axis (test contract layering, spec-5.59 round-2 L5):
	# the index axis is alive iff the compiled-in, GUC-gated rightmost-leaf
	# probe counts on right-growing inserts -- that is the HARD assertion.
	#
	# The i_index_block_xfer bucket is DIFFERENT: it counts a real dirty-block
	# Cache Fusion TRANSFER for an index block (a request to a remote master
	# that holds/dirtied the block).  This tier boots a 2-node pair WITHOUT a
	# shared catalog, so node1 cannot see t334_rg at all and never touches --
	# let alone dirties -- its index blocks.  node1 is only the passive PCM
	# master of ~half the blocks by tag hash, and node0 holds every block it
	# built, so a genuine cross-node index-block transfer structurally almost
	# never occurs; whether the counter ends non-zero is a build-phase timing
	# artifact (platform-dependent: nightly Linux occasionally >0, local macOS
	# always 0).  Asserting > 0 on it made L5 flake with no correctness or
	# perf meaning (this is a measure-only test).  So we OBSERVE it (diag) and
	# keep the rightmost-probe hard assertion, which fully covers "the index
	# axis records events".  A faithful i_index_block_xfer assertion needs a
	# real 2-node shared-catalog index-contention workload (node1 reads/writes
	# the same index) -- that is a separate, larger test, tracked as a
	# follow-up, not this measure-only surface check.
	# ----------
	psql_retry($node0, 'CREATE TABLE t334_rg (id int PRIMARY KEY, v int)');
	psql_retry($node0,
		'INSERT INTO t334_rg SELECT g, g FROM generate_series(1,2000) g');
	psql_retry($node0, 'UPDATE t334_rg SET v = v + 1 WHERE id % 7 = 0');

	my $idx_events = xp_sum_events($node0, 'bucket.i_index_block_xfer.n_events')
	  + xp_sum_events($node1, 'bucket.i_index_block_xfer.n_events');
	diag("L5 index-block-xfer aggregate = $idx_events (observation: a real "
		. "cross-node index-block transfer needs node1 to touch the index, "
		. "which has no shared catalog here; see the block comment)");

	my $rightmost = xp_val($node0, 'bucket.i_rightmost_leaf_ping.n_events')
	  + xp_val($node1, 'bucket.i_rightmost_leaf_ping.n_events');
	cmp_ok($rightmost, '>', 0,
		"L5 index axis alive: rightmost-leaf probe counted (got $rightmost; "
		. "ascending-id inserts are right-growing)");

	# ----------
	# L6 commit axis.
	# ----------
	my $commit_adv = xp_val($node0, 'bucket.c_scn_commit_advance.n_events')
	  + xp_val($node1, 'bucket.c_scn_commit_advance.n_events');
	cmp_ok($commit_adv, '>', 0, "L6 SCN commit-advance counted (got $commit_adv)");

	my $boc = xp_val($node0, 'bucket.c_scn_boc_broadcast.n_events')
	  + xp_val($node1, 'bucket.c_scn_boc_broadcast.n_events');
	cmp_ok($boc, '>', 0, "L6 SCN BOC broadcast counted (got $boc)");

	# ----------
	# L7 sum sanity: requester decision buckets accumulated on node1 must
	# not exceed the total wall clock this test spent driving node1 (its
	# sessions are sequential, so bucket time is a subset of wall time).
	# Generous 2x margin: this is a structural no-double-count sanity,
	# not a perf gate.
	# ----------
	my $total_wall_ns = ($w_wall_ns + $r_wall_ns) * 2;
	my $decision_ns = xp_decision_nanos($node1);
	cmp_ok($decision_ns, '<', $total_wall_ns,
		sprintf('L7 sum sanity: node1 decision buckets %.1fms < 2x workload wall %.1fms',
			$decision_ns / 1e6, $total_wall_ns / 1e6));

	# ----------
	# L9 observe-only: concurrent same-rows write pressure from both
	# nodes.  Errors (e.g. cross-node retry fail-closed) are recorded
	# as observations, never failures.
	# ----------
	my $l9_errors = 0;
	for my $i (1 .. 5)
	{
		my $ok0 = eval { $node0->safe_psql('postgres', 'UPDATE t334 SET v = v + 1 WHERE id <= 50'); 1 };
		$l9_errors++ unless $ok0;
		my $ok1 = eval { $node1->safe_psql('postgres', 'UPDATE t334 SET v = v + 1 WHERE id <= 50'); 1 };
		$l9_errors++ unless $ok1;
	}
	ok(1, "L9 observe-only same-block write pressure recorded (errors=$l9_errors of 10)");
	diag("L9 observation: $l9_errors of 10 concurrent same-block updates errored "
		. '(fail-closed retries are expected behaviour, recorded not judged)');

	# reset_generation visible and stable (runner uses fresh-boot + this
	# key to detect resets; no SQL reset surface exists by design).
	is(xp_val($node0, 'reset_generation'), 0, 'reset_generation exposed (0, never reset)');

	$pair->stop_pair if $pair->can('stop_pair');
}

done_testing();
