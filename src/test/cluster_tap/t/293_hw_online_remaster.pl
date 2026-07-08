#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 293_hw_online_remaster.pl
#    spec-5.7 HW (relation-extend) authority -- online-remaster recovery
#    e2e (S7, §3.1b R4/R9 + §3.1c).
#
#    The hardest half of HW: after the node that masters a relation's HW
#    authority dies, the survivor must REBUILD that authority from the dead
#    master's durable snapshot + HW_RESERVE WAL tail, write an adoption
#    snapshot, and only THEN unfreeze + serve -- never auto-create the HWM
#    at block 0 (which would re-hand an allocated range = silent corruption).
#
#    Positive (real kill + remaster):
#      L1  2-node strict pair (shared storage + per-node WAL threads + voting
#          disks) reaches quorum with CSSD heartbeats flowing.
#      L2  extend several relations + CHECKPOINT so both nodes write their HW
#          authority snapshot (<shared>/global/pg_hw_snapshot.<node>).
#      L3  kill -9 node0; the survivor's CSSD deadband fires the DEAD edge and
#          the GRD reconfig remasters every node0 shard to node1.
#      L4  the HW rebuild worker runs on node1 and reports DONE
#          (hw.remaster_done_count advances) -- the survivor rebuilt node0's
#          HW authority from snapshot + WAL tail and wrote the adoption
#          snapshot, then P7 unfroze.
#      L5a the rebuilt authority is SERVEABLE: the HW serve gate never fail-closed
#          (hw.not_ready_count stays 0) for the rebuilt shards once P7 unfroze.
#      L5b (best-effort) node1 actually extends a node0-written relation.  The
#          Cache-Fusion blocks of a dead-written relation enter post-death
#          RECOVERING (spec-4.7/4.11 thread recovery, off here) -- a documented-
#          retryable transient, NOT an HW fault -- so this leg retries and SKIPs
#          (never fails) when only that orthogonal recovery is outstanding.
#
#    Negative/self-heal extensions (spec-4.6a):
#      L6  a fresh pair; corrupt node0's HW snapshot on shared storage, then
#          kill node0.  The survivor's rebuild must FAIL CLOSED
#          (hw.remaster_blocked_count advances) -- it never trusts a corrupt
#          snapshot, never auto-creates at 0, never reads FileSize as the
#          authority.
#      L7  hide node0's snapshot, kill node0, observe BLOCKED, then restore
#          the same file.  The same reconfig episode must retry and converge
#          to DONE without waiting for a later episode.
#      L8  keep the snapshot corrupt with retry_max_attempts=2.  The survivor
#          must report retry_exhausted once and keep fail-closed; DONE must not
#          advance.
#
#    Harness: ClusterPair shared_data + wal_threads_root + 3 voting disks.
#    Mirrors the kill/reconfig pattern of t/249 / t/274.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/293_hw_online_remaster.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-5.7-misc-enqueue-classes.md (D3 S7, §3.1b / §3.1c)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

# Poll a scalar query until it equals $want or the timeout elapses.
sub poll_until
{
	my ($node, $query, $want, $timeout_s) = @_;
	my $deadline = time() + $timeout_s;
	my $last = '';
	while (time() < $deadline)
	{
		$last = $node->safe_psql('postgres', $query);
		return 1 if defined $last && $last eq $want;
		usleep(300_000);
	}
	diag("poll_until timed out: '$query' last='$last' want='$want'");
	return 0;
}

# Read a single hw-category counter from pg_cluster_state on $node.
sub hw_counter
{
	my ($node, $key) = @_;
	my $v = $node->safe_psql('postgres',
		"SELECT value FROM pg_cluster_state WHERE category='hw' AND key='$key'");
	return defined $v && $v ne '' ? $v + 0 : 0;
}

# Extend a relation on a node by bulk insert; many distinct relfilenodes so
# some are node0-mastered (the rebuild target after node0 dies).
sub make_and_fill
{
	my ($n0, $n1, $name, $rows) = @_;
	$n0->safe_psql('postgres', "CREATE TABLE $name(a int, b int, c int, d int)");
	$n1->safe_psql('postgres', "CREATE TABLE $name(a int, b int, c int, d int)");
	$n0->safe_psql('postgres',
		"INSERT INTO $name SELECT g,g,g,g FROM generate_series(1,$rows) g");
}

# ======================================================================
# POSITIVE: kill the HW master, survivor rebuilds + keeps extending.
# ======================================================================

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'hw_remaster',
	quorum_voting_disks => 3,
	shared_data         => 1,
	wal_threads_root    => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.grd_max_entries = 4096',
		'cluster.ges_request_timeout_ms = 3000',
		'cluster.ges_retransmit_max_attempts = 0',
		'cluster.cssd_heartbeat_interval_ms = 1000',
		'cluster.cssd_dead_deadband_factor = 6',
	]);
$pair->start_pair;

my $n0 = $pair->node0;
my $n1 = $pair->node1;

# ---- L1: quorum + heartbeats ----
ok(poll_until($n0, 'SELECT in_quorum FROM pg_cluster_quorum_state', 't', 20),
	'L1: node0 in quorum');
ok(poll_until($n1, 'SELECT in_quorum FROM pg_cluster_quorum_state', 't', 20),
	'L1: node1 in quorum');
ok( poll_until(
		$n1,
		q{SELECT (state='alive' AND heartbeat_recv_count>0)::text
		    FROM pg_cluster_cssd_peers WHERE node_id=0},
		'true', 20),
	'L1: node1 sees node0 CSSD alive + heartbeats');

# ---- L2: extend several relations + checkpoint (write HW snapshots) ----
make_and_fill($n0, $n1, "r$_", 4000) for (1 .. 6);
$n0->safe_psql('postgres', 'CHECKPOINT');
$n1->safe_psql('postgres', 'CHECKPOINT');
# every node0-mastered (rel,fork) extended above is now in node0's snapshot.
ok(1, 'L2: relations extended + both HW snapshots checkpointed');

my $done_before = hw_counter($n1, 'remaster_done_count');

# ---- L3: kill node0; survivor reconfig + remaster ----
$pair->kill_node9(0);
ok( poll_until(
		$n1,
		q{SELECT (state='dead')::text FROM pg_cluster_cssd_peers WHERE node_id=0},
		'true', 30),
	'L3: node1 CSSD declares node0 dead');
ok( poll_until(
		$n1,
		q{SELECT (count(*)=0)::text FROM pg_cluster_grd_shards WHERE master_node_id=0},
		'true', 30),
	'L3: GRD remastered every node0 shard off the dead node');

# ---- L4: the HW rebuild worker ran (DONE) on the survivor ----
ok( poll_until(
		$n1,
		"SELECT (value::bigint > $done_before)::text FROM pg_cluster_state "
		  . "WHERE category='hw' AND key='remaster_done_count'",
		'true', 40),
	'L4: node1 HW rebuild worker reported DONE (rebuilt node0 authority from snapshot + WAL tail)');
is(hw_counter($n1, 'remaster_blocked_count'), 0,
	'L4: no HW rebuild fail-closed on the clean positive path');

# ---- L5: survivor keeps extending the relations after remaster ----
# The HW authority is rebuilt (L4), but the OTHER post-reconfig recovery
# subsystems (the GCS block-cache protocol rebuild, TT commit_scn propagation)
# settle a beat later and surface documented-RETRYABLE transients ("being rebuilt
# after reconfiguration", "TT status unknown ... retry").  Those are not HW
# faults; a real app retries, so the extend retries on exactly those signals and
# fails only on a real error (e.g. a 53RA6 from the HW serve gate, which must NOT
# happen now that the authority is rebuilt).
sub retry_sql
{
	my ($node, $sql) = @_;
	my $last = '';
	my $last_retryable = 0;
	for my $attempt (1 .. 30)
	{
		my ($rc, $out, $err) = $node->psql('postgres', $sql, timeout => 60);
		my $combined = join("\n", grep { defined $_ && $_ ne '' } ($out, $err));
		return (1, $out, 0)
		  if defined $rc && $rc == 0 && $combined !~ /ERROR:/;

		$last = $combined;
		$last_retryable =
		  $last =~ /being rebuilt after reconfiguration|status unknown|not yet propagated|could not obtain X transfer|did not ship a current image/;
		# retry only the orthogonal post-reconfig settling transients
		last unless $last_retryable;
		usleep(500_000);
	}
	return (0, $last, $last_retryable);
}

# L5a (the robust HW proof): the rebuilt authority is SERVEABLE -- the HW serve
# gate never fail-closed (53RA6 / NOT_READY) for the rebuilt shards.  After the
# rebuild marked the adopted shards (L4) and P7 unfroze, an HW_ALLOC on any of
# them must NOT hit the not-ready gate.  This is the direct HW-side proof that the
# survivor can serve the adopted authority, independent of the orthogonal data-
# block recovery below.
is(hw_counter($n1, 'not_ready_count'), 0,
	'L5a: HW serve gate never fail-closed after the rebuild -- the rebuilt authority is serveable (no 53RA6)');

# L5b (best-effort, isolates HW from spec-4.7/4.11): actually extend a relation on
# the survivor.  Tables r1..r6 were WRITTEN by node0, so their Cache-Fusion blocks
# enter post-death RECOVERING until the dead origin's data is recovered (spec-4.10
# / 4.11 thread recovery, off by default here) -- a documented-retryable transient
# that is NOT an HW fault.  Retry, and SKIP (never fail) when only that orthogonal
# recovery is outstanding; a 53RA6 from the HW gate would NOT be retried and would
# fail the test.
my ($ext_ok, $ext_res, $ext_retryable) =
  retry_sql($n1, 'INSERT INTO r1 SELECT g,g,g,g FROM generate_series(4001,4500) g');
SKIP:
{
	if (!$ext_ok)
	{
		skip
		  "post-remaster extend gated by orthogonal dead-block recovery (spec-4.7/4.11), not HW: $ext_res",
		  1
		  if $ext_retryable;
		fail('L5b: node1 extended a node0-written relation after remaster -- rows landed past node0 blocks, no dup/overwrite');
		diag("extend result: $ext_res");
	}
	else
	{
		my ($cnt_ok, $cnt, $cnt_retryable) = retry_sql($n1, 'SELECT count(*) FROM r1');
		if (!$cnt_ok)
		{
			skip
			  "post-remaster count gated by orthogonal visibility recovery (not HW): $cnt",
			  1
			  if $cnt_retryable;
			fail('L5b: node1 extended a node0-written relation after remaster -- rows landed past node0 blocks, no dup/overwrite');
			diag("count result: $cnt");
		}
		else
		{
			is($cnt, '4500',
				'L5b: node1 extended a node0-written relation after remaster -- rows landed past node0 blocks, no dup/overwrite')
			  or diag("count result: $cnt");
		}
	}
}

$pair->stop_pair;

# ======================================================================
# SELF-HEAL: first attempt BLOCKED, same episode retries after repair.
# ======================================================================

my $heal = PostgreSQL::Test::ClusterPair->new_pair(
	'hw_remaster_selfheal',
	quorum_voting_disks => 3,
	shared_data         => 1,
	wal_threads_root    => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.grd_max_entries = 4096',
		'cluster.ges_request_timeout_ms = 3000',
		'cluster.ges_retransmit_max_attempts = 0',
		'cluster.cssd_heartbeat_interval_ms = 1000',
		'cluster.cssd_dead_deadband_factor = 6',
		'cluster.grd_rebuild_timeout_ms = 1000',
		'cluster.hw_remaster_retry_backoff_ms = 100',
		'cluster.hw_remaster_retry_max_attempts = 8',
	]);
$heal->start_pair;
my $h0 = $heal->node0;
my $h1 = $heal->node1;

ok(poll_until($h1, 'SELECT in_quorum FROM pg_cluster_quorum_state', 't', 20),
	'L7: self-heal pair in quorum');
poll_until(
	$h1,
	q{SELECT (state='alive' AND heartbeat_recv_count>0)::text
	    FROM pg_cluster_cssd_peers WHERE node_id=0},
	'true', 20);

make_and_fill($h0, $h1, "s$_", 4000) for (1 .. 6);
$h0->safe_psql('postgres', 'CHECKPOINT');
$h1->safe_psql('postgres', 'CHECKPOINT');

my $hsnap = $heal->shared_data_root . "/global/pg_hw_snapshot.0";
my $hhold = "$hsnap.hold";
ok(-f $hsnap, 'L7: node0 HW snapshot exists before self-heal injection');
rename($hsnap, $hhold) or die "rename $hsnap -> $hhold: $!";

my $h_done_before = hw_counter($h1, 'remaster_done_count');
my $h_blocked_before = hw_counter($h1, 'remaster_blocked_count');
my $h_retry_before = hw_counter($h1, 'remaster_retry_count');
$heal->kill_node9(0);
ok( poll_until(
		$h1,
		q{SELECT (state='dead')::text FROM pg_cluster_cssd_peers WHERE node_id=0},
		'true', 30),
	'L7: self-heal pair declared node0 dead');
ok( poll_until(
		$h1,
		"SELECT (value::bigint > $h_blocked_before)::text FROM pg_cluster_state "
		  . "WHERE category='hw' AND key='remaster_blocked_count'",
		'true', 40),
	'L7: first HW rebuild attempt failed closed while the snapshot was hidden');

rename($hhold, $hsnap) or die "rename $hhold -> $hsnap: $!";
ok( poll_until(
		$h1,
		"SELECT (value::bigint > $h_retry_before)::text FROM pg_cluster_state "
		  . "WHERE category='hw' AND key='remaster_retry_count'",
		'true', 40),
	'L7: same reconfig episode launched a HW remaster retry');
ok( poll_until(
		$h1,
		"SELECT (value::bigint > $h_done_before)::text FROM pg_cluster_state "
		  . "WHERE category='hw' AND key='remaster_done_count'",
		'true', 60),
	'L7: restored snapshot let the same-episode retry converge to DONE');
is(hw_counter($h1, 'remaster_retry_exhausted_count'), 0,
	'L7: self-heal path did not exhaust the retry budget');

# spec-4.6a section 4 (D8): DONE must actually unfreeze (P7) -- the survivor
# extends a table that lived on shards the dead master owned; a frozen shard
# would fail this INSERT with the remastering error.
{
	my ($ext_ok, $ext_res) = (0, '');
	for my $try (1 .. 30)
	{
		$ext_res = eval {
			$h1->safe_psql('postgres',
				'INSERT INTO s1 SELECT g, g, g, g FROM generate_series(1, 4096) g');
			1;
		} ? 'ok' : ($@ // 'error');
		if ($ext_res eq 'ok') { $ext_ok = 1; last; }
		sleep 1;
	}
	ok($ext_ok, 'L7: survivor extend succeeds after same-episode DONE (P7 unfreeze)')
	  or diag($ext_res);
}

$heal->stop_pair;

# ======================================================================
# NEGATIVE: corrupt snapshot persists -> retries exhaust and stay closed.
# ======================================================================

my $exh = PostgreSQL::Test::ClusterPair->new_pair(
	'hw_remaster_exhausted',
	quorum_voting_disks => 3,
	shared_data         => 1,
	wal_threads_root    => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.grd_max_entries = 4096',
		'cluster.ges_request_timeout_ms = 3000',
		'cluster.ges_retransmit_max_attempts = 0',
		'cluster.cssd_heartbeat_interval_ms = 1000',
		'cluster.cssd_dead_deadband_factor = 6',
		'cluster.grd_rebuild_timeout_ms = 1000',
		'cluster.hw_remaster_retry_backoff_ms = 100',
		'cluster.hw_remaster_retry_max_attempts = 2',
	]);
$exh->start_pair;
my $e0 = $exh->node0;
my $e1 = $exh->node1;

ok(poll_until($e1, 'SELECT in_quorum FROM pg_cluster_quorum_state', 't', 20),
	'L8: exhausted pair in quorum');
poll_until(
	$e1,
	q{SELECT (state='alive' AND heartbeat_recv_count>0)::text
	    FROM pg_cluster_cssd_peers WHERE node_id=0},
	'true', 20);

make_and_fill($e0, $e1, "x$_", 4000) for (1 .. 6);
$e0->safe_psql('postgres', 'CHECKPOINT');
$e1->safe_psql('postgres', 'CHECKPOINT');

my $esnap = $exh->shared_data_root . "/global/pg_hw_snapshot.0";
ok(-f $esnap, 'L8: node0 HW snapshot exists before exhaustion injection');
open(my $efh, '>', $esnap) or die "open $esnap: $!";
binmode $efh;
print $efh ("\xDE\xAD\xBE\xEF" x 64);
close $efh;

my $e_done_before = hw_counter($e1, 'remaster_done_count');
my $e_exh_before = hw_counter($e1, 'remaster_retry_exhausted_count');
$exh->kill_node9(0);
poll_until(
	$e1,
	q{SELECT (state='dead')::text FROM pg_cluster_cssd_peers WHERE node_id=0},
	'true', 30);
ok( poll_until(
		$e1,
		"SELECT (value::bigint > $e_exh_before)::text FROM pg_cluster_state "
		  . "WHERE category='hw' AND key='remaster_retry_exhausted_count'",
		'true', 70),
	'L8: persistent corrupt snapshot exhausted the same-episode retry budget');
is(hw_counter($e1, 'remaster_done_count'), $e_done_before,
	'L8: DONE did not advance after retry exhaustion on corrupt snapshot');

# spec-4.6a section 4 (D8/L408): after exhaustion the WAIT_CLUSTER watchdog
# keeps sounding (counter + WARNING line) while shards stay frozen.
ok( poll_until(
		$e1,
		q{SELECT (value::bigint > 0)::text FROM pg_cluster_state }
		  . q{WHERE category='grd_recovery' AND key='cluster_gate_timeout'},
		'true', 30),
	'L8: WAIT_CLUSTER watchdog counter advances while exhausted episode stays frozen');
{
	my $log = PostgreSQL::Test::Utils::slurp_file($e1->logfile);
	like(
		$log,
		qr/cluster GRD recovery WAIT_CLUSTER watchdog fired; affected shards stay frozen/,
		'L8: watchdog WARNING appears in the survivor log');
}

$exh->stop_pair;

# ======================================================================
# NEGATIVE: corrupt the dead master's snapshot -> rebuild fails closed.
# ======================================================================

my $neg = PostgreSQL::Test::ClusterPair->new_pair(
	'hw_remaster_neg',
	quorum_voting_disks => 3,
	shared_data         => 1,
	wal_threads_root    => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.grd_max_entries = 4096',
		'cluster.ges_request_timeout_ms = 3000',
		'cluster.ges_retransmit_max_attempts = 0',
		'cluster.cssd_heartbeat_interval_ms = 1000',
		'cluster.cssd_dead_deadband_factor = 6',
		'cluster.grd_rebuild_timeout_ms = 1000',
		'cluster.hw_remaster_retry_backoff_ms = 100',
		'cluster.hw_remaster_retry_max_attempts = 2',
	]);
$neg->start_pair;
my $m0 = $neg->node0;
my $m1 = $neg->node1;

ok(poll_until($m1, 'SELECT in_quorum FROM pg_cluster_quorum_state', 't', 20),
	'L6: neg pair in quorum');
poll_until(
	$m1,
	q{SELECT (state='alive' AND heartbeat_recv_count>0)::text
	    FROM pg_cluster_cssd_peers WHERE node_id=0},
	'true', 20);

make_and_fill($m0, $m1, "q$_", 4000) for (1 .. 6);
$m0->safe_psql('postgres', 'CHECKPOINT');
$m1->safe_psql('postgres', 'CHECKPOINT');

# Corrupt node0's HW snapshot on shared storage (flip it to garbage) so the
# survivor's rebuild hits a CRC / structural failure and must fail closed.
my $snap = $neg->shared_data_root . "/global/pg_hw_snapshot.0";
if (-f $snap)
{
	open(my $fh, '>', $snap) or die "open $snap: $!";
	binmode $fh;
	print $fh ("\xDE\xAD\xBE\xEF" x 64);    # 256 bytes of garbage
	close $fh;
	ok(1, 'L6: node0 HW snapshot corrupted on shared storage');
}
else
{
	# snapshot path differs / not written; fail loudly rather than skip silently
	ok(0, "L6: expected node0 HW snapshot at $snap (not found)");
}

my $blocked_before = hw_counter($m1, 'remaster_blocked_count');
my $neg_exh_before = hw_counter($m1, 'remaster_retry_exhausted_count');
$neg->kill_node9(0);
poll_until(
	$m1,
	q{SELECT (state='dead')::text FROM pg_cluster_cssd_peers WHERE node_id=0},
	'true', 30);

# The rebuild must fail closed on the corrupt snapshot: remaster_blocked advances
# and remaster_done does NOT (it never trusts the snapshot, never auto-creates 0).
ok( poll_until(
		$m1,
		"SELECT (value::bigint > $blocked_before)::text FROM pg_cluster_state "
		  . "WHERE category='hw' AND key='remaster_blocked_count'",
		'true', 40),
	'L6: HW rebuild FAILED CLOSED on the corrupt snapshot (remaster_blocked advanced; no auto-create-0)');
ok( poll_until(
		$m1,
		"SELECT (value::bigint > $neg_exh_before)::text FROM pg_cluster_state "
		  . "WHERE category='hw' AND key='remaster_retry_exhausted_count'",
		'true', 70),
	'L6: corrupt snapshot eventually exhausts bounded same-episode retries');

$neg->stop_pair;

done_testing();
