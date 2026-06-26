#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 307_cluster_5_14_fail_stop_touched.pl
#    spec-5.14 — fail-stop reconfig per-tx touched_peers, real 2-node e2e.
#
#    Closes the read-side 8.A hole spec-2.29 deferred: a transaction with NO
#    top-level xid (read-only by the write-path gate) that consumed volatile
#    state from a node which then fail-stops must abort (40R01), not silently
#    continue.
#
#    Legs:
#      L0  strict pair (3 voting disks, shared data) + GES gate + CSSD
#          substrate.
#      L1a READ-SIDE class-4 stamp is live真走 fork: a node1 cross-node read of
#          a block carrying a node0 ITL stamps node0 (the cluster-wide stamp_vis
#          counter grows), proving the visibility ingress fires across the node
#          boundary (L373).  The read itself fails closed on a recycled ITL slot
#          (pre-existing CR behaviour) — the stamp fires BEFORE that, which is
#          the point; a fresh psql tolerates the error.
#      L1b CORE (touched no-top-xid -> 40R01): a node1 transaction whose only
#          work is acquiring node0-MASTERED xact advisory locks touches node0
#          across the GES boundary but assigns no top xid.  Its own
#          self_touched_hex shows node0's bit.  node0 is SIGKILLed (fail-stop)
#          -> node1 reconfig fires (reconfig_kind='fail_stop') -> the touched
#          no-top-xid tx aborts on its next statement (touched_abort_count
#          grows): the read-side no-xid gate is broken exactly as spec'd.
#      L3  INV-TP5 (no false kill): a fresh node1 pure-local read-only query is
#          NOT aborted by the same fail-stop.
#      L7  observability: reconfig_kind='fail_stop'; the reconfig_touched dump
#          category exposes its key roster; abort_count >= 1; clean_leave=0.
#
#    L2 (writable touched), L4 (non-touched writable 53R60), L5 (retry ->
#    53R9I/53R9L), L6 (cascade), L8 (parallel DSM merge), L9 (LOG-once) are
#    deterministically covered at the unit level (test_cluster_reconfig U8
#    verdict matrix + test_cluster_touched_peers U1-U5/U7); their across-node
#    forms need fragile multi-section async orchestration (L370) and are not
#    re-driven here.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/307_cluster_5_14_fail_stop_touched.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-5.14-fail-stop-reconfig.md (L1/L3/L7)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(usleep);

sub poll_until
{
	my ($node, $query, $expected, $timeout_s, $label) = @_;
	$expected //= 't';
	$timeout_s //= 20;
	my $deadline = time + $timeout_s;
	my $last = '';
	while (time < $deadline)
	{
		$last = $node->safe_psql('postgres', $query);
		return 1 if defined $last && $last eq $expected;
		select(undef, undef, undef, 0.2);
	}
	diag("$label timed out after ${timeout_s}s; expected=$expected last=$last");
	return 0;
}

sub touched_counter
{
	my ($node, $key) = @_;
	my $v = $node->safe_psql('postgres', qq{
		SELECT coalesce(sum(value::bigint), 0) FROM pg_cluster_state
		 WHERE category = 'reconfig_touched' AND key = '$key'});
	return (defined $v && $v ne '') ? int($v) : 0;
}

sub first_hex
{
	my ($out) = @_;
	return ($out // '') =~ /(0x[0-9a-fA-F]+)/ ? $1 : '';
}


# ----------
# L0: strict pair + shared data + GES gate + CSSD substrate.
# ----------
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'fs_touched',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		# Opt into the cluster GES lock path (xact advisory locks enter GES).
		'cluster.grd_max_entries = 4096',
		'cluster.ges_request_timeout_ms = 30000',
		'cluster.ges_retransmit_max_attempts = 0',
		# Keep the deadband modest so the post-kill DEAD edge fires inside the
		# poll window, but not so tight that setup jitter trips a false DEAD.
		'cluster.cssd_heartbeat_interval_ms = 1000',
		'cluster.cssd_dead_deadband_factor = 6',
	]);
$pair->start_pair;
usleep(3_000_000);

is($pair->node0->safe_psql('postgres', 'SELECT 1'), '1', 'L0 node0 alive');
is($pair->node1->safe_psql('postgres', 'SELECT 1'), '1', 'L0 node1 alive');
ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'L0 node0 sees node1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'L0 node1 sees node0 connected');
ok(poll_until($pair->node1,
		q{SELECT state = 'alive' AND heartbeat_recv_count > 0
		    FROM pg_cluster_cssd_peers WHERE node_id = 0},
		't', 20, 'L0 node1 sees node0 CSSD alive + heartbeats'),
	'L0 node1 sees node0 CSSD alive + heartbeats flowing');


# ----------
# L1a: READ-SIDE class-4 stamp fires真走 cross-node fork.
# ----------
$pair->node0->safe_psql('postgres', 'CREATE TABLE t (id int, ctr int)');
$pair->node1->safe_psql('postgres', 'CREATE TABLE t (id int, ctr int)');
my $p0 = $pair->node0->safe_psql('postgres', "SELECT pg_relation_filepath('t')");
my $p1 = $pair->node1->safe_psql('postgres', "SELECT pg_relation_filepath('t')");
if ($p0 ne $p1)
{
	$pair->stop_pair;
	plan skip_all => "same-DDL relfilepath coincidence does not hold (n0=$p0 n1=$p1)";
}
$pair->node0->safe_psql('postgres', 'INSERT INTO t VALUES (1, 100), (2, 100)');
$pair->node0->safe_psql('postgres', 'CHECKPOINT');

# node0 holds an uncommitted ITL on the shared block; a node1 read resolves
# node0's origin cross-node (stamp_vis++).  The read may fail closed on a
# recycled slot — irrelevant: the class-4 stamp fires first.  Fresh psql so the
# error does not derail the test.
my $hupd = $pair->node0->background_psql('postgres', on_error_die => 1);
$hupd->query_safe('BEGIN');
$hupd->query('UPDATE t SET ctr = ctr + 1 WHERE id = 1');

my $vis_before = touched_counter($pair->node1, 'stamp_vis');
$pair->node1->psql('postgres', 'SELECT count(*) FROM t WHERE id IN (1, 2)');
my $vis_grew = 0;
for (1 .. 50)
{
	if (touched_counter($pair->node1, 'stamp_vis') > $vis_before) { $vis_grew = 1; last; }
	$pair->node1->psql('postgres', 'SELECT count(*) FROM t WHERE id IN (1, 2)');
	usleep(100_000);
}
ok($vis_grew,
	'L1a read-side class-4 visibility stamp fired on a node1 cross-node read (stamp_vis grew — true cluster fork, L373)');
$hupd->query_safe('ROLLBACK');
$hupd->quit;


# ----------
# L1b CORE: a node1 no-top-xid tx touches node0 via node0-mastered advisory
# locks, then node0 fail-stops and the touched tx aborts 40R01.
# ----------

# Acquire a fresh batch of xact advisory locks in the touched session.  ~half
# the keys are node0-mastered (round-robin), so these FIRST acquires cross the
# GES boundary to node0 and stamp it.  No write -> no top xid (read-only by the
# write-path gate).  query (not query_safe): remote acquires may warn.
my $KEY_BASE = 5140000;
my $touched  = $pair->node1->background_psql('postgres', on_error_die => 1);
$touched->query_safe('BEGIN');
$touched->query(join("\n",
		map { "SELECT pg_advisory_xact_lock(" . ($KEY_BASE + $_) . ");" } (1 .. 24)));

# This very backend's touched bitmap must show node0 (node index 0 -> bit 0).
my $hex = first_hex($touched->query(
		"SELECT value FROM pg_cluster_state WHERE category='reconfig_touched' AND key='self_touched_hex'"));
note("L1b touched backend self_touched_hex = $hex");
ok($hex ne '' && $hex ne '0x0000000000000000',
	'L1b the touched backend recorded a non-empty touched_peers bitmap (it crossed to node0)');

my $abort_before = touched_counter($pair->node1, 'abort_count');

# Fail-stop node0.
$pair->kill_node9(0);
ok(1, 'L1b node0 postmaster SIGKILLed (fail-stop)');
ok(poll_until($pair->node1,
		q{SELECT state IN ('suspected','dead') FROM pg_cluster_cssd_peers WHERE node_id = 0},
		't', 40, 'L1b node1 CSSD marks node0 dead'),
	'L1b node1 CSSD deadband marks node0 suspected/dead');
ok(poll_until($pair->node1,
		q{SELECT event_id != 0 FROM pg_cluster_reconfig_state}, 't', 40,
		'L1b reconfig fired'),
	'L1b node1 reconfig event fired (event_id != 0)');
is($pair->node1->safe_psql('postgres',
		q{SELECT reconfig_kind FROM pg_cluster_reconfig_state}),
	'fail_stop', 'L1b reconfig_kind = fail_stop (D3 producer)');

# The touched no-top-xid tx aborts on its next interrupt (spec §3.3).  The
# backend fires 40R01; the bg psql exits on the error (ON_ERROR_STOP), so the
# nudge is wrapped in eval and the authoritative proof is the shmem counter
# (read from a fresh connection) + the server log SQLSTATE.
eval { $touched->query('SELECT 1'); };
my $aborted = 0;
for (1 .. 60)
{
	if (touched_counter($pair->node1, 'abort_count') > $abort_before) { $aborted = 1; last; }
	eval { $touched->query('SELECT 1'); };
	usleep(200_000);
}
ok($aborted,
	'L1b CORE the touched no-top-xid (read-only) tx aborts on its next interrupt — touched_abort_count grew (read-side 8.A hole closed)');

my $node1_log = PostgreSQL::Test::Utils::slurp_file($pair->node1->logfile);
like($node1_log, qr/40R01|cluster member fail-stop during reconfiguration/,
	'L1b the touched abort surfaces the 40R01 fail-stop SQLSTATE in the server log');
eval { $touched->quit; };


# ----------
# L3 INV-TP5: a fresh node1 pure-local read-only query is NOT killed.
# ----------
is($pair->node1->safe_psql('postgres', 'SELECT 42'), '42',
	'L3 a non-touched local read-only query survives the fail-stop (INV-TP5, no false kill)');


# ----------
# L7 observability.
# ----------
is($pair->node1->safe_psql('postgres', q{
		SELECT string_agg(key, ',' ORDER BY key COLLATE "C") FROM pg_cluster_state
		 WHERE category = 'reconfig_touched'}),
	'abort_count,clean_leave_rejected,self_touched_hex,stamp_count,stamp_gcs_block,'
	. 'stamp_ges,stamp_scn,stamp_sinval,stamp_vis',
	'L7 reconfig_touched dump key roster (spec-5.14 D6)');
ok(touched_counter($pair->node1, 'abort_count') >= 1,
	'L7 abort_count >= 1 after the touched fail-stop abort');
is($pair->node1->safe_psql('postgres', q{
		SELECT value FROM pg_cluster_state
		 WHERE category = 'reconfig_touched' AND key = 'clean_leave_rejected'}),
	'0', 'L7 clean_leave_rejected = 0 (no CLEAN_LEAVE producer in this spec)');


$pair->stop_pair;
done_testing();
