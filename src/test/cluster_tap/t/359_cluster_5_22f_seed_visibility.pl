#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 359_cluster_5_22f_seed_visibility.pl
#	  spec-5.22f D6 — shared-catalog seed / fresh-ref visibility consumer
#	  end-to-end on a 2-node ClusterPair + shared cluster_fs root.  This is
#	  the ROOT-CAUSE #1 real fix: the "committed seed visible" leg that
#	  D2-7 (t/255 L6) explicitly forward-linked to D6.
#
#	  Substrate: node0 (cluster-enabled -> storage-mode, peer-independent)
#	  commits a NORMAL-xid INSERT while undo GCS coherence is ON, so the
#	  seed xact's undo segment (block0 TT + commit_scn) migrates to the
#	  SHARED cluster_fs root (the D2-2 heart).  The heap page is
#	  phantom-shared, so node1 reads the SAME bytes node0 wrote, carrying
#	  node0's ITL binding.  The D6 mechanism is tuple-agnostic (heap or
#	  catalog): a fresh remote ITL ref (local_xid == raw_xid, origin != self)
#	  that misses node1's empty TT overlay is exactly the seed/joiner case.
#
#	  Striping stays OFF, so cluster_xid_origin_slot(seed_xid) is
#	  underivable (-1) on node1 -- the true root-cause #1 timeline (a seed
#	  DDL committed pre-striping).  Option B (Q2 / P1-a): underivable STILL
#	  asks the D3 verdict (ref->origin is the physical binding, authoritative;
#	  Rule 8.A safety is anchored inside D3's wrap-suspect / covers / serve
#	  gates), so the seed resolves visible instead of self-deadlocking on a
#	  derived>=0 guard.
#
#	  L1  pair boots + shared cluster_fs; seed table + coherence armed; the
#	      seed xact's undo lands on the SHARED root.
#	  L2  命门 2 baseline: crossnode_runtime_visibility OFF -> node1's fresh
#	      read of the seed tuple fails closed 53R97 (overlay miss, no
#	      widening) -- the RED state D6 fixes.
#	  L3  ROOT-CAUSE #1 FIX (spec L1): crossnode ON -> node1's fresh read
#	      SUCCEEDS via the D6 fresh-ref widening -> D3 verdict.  The
#	      vis_freshref_verdict_resolved_count counter increment is a HARD
#	      assertion (P1-b): it proves the read went through the fresh-ref
#	      widening path, not an overlay hit or ordinary propagation -- else
#	      the test would silently degrade to a formed-cluster DDL test that
#	      does NOT prove root-cause #1.
#	  L4  aborted seed (spec L2): a rolled-back seed row resolves ABORTED via
#	      the verdict -> NOT visible (never false-visible).
#	  L5  off regression (spec L4): crossnode OFF again -> a fresh backend
#	      fails closed 53R97; the off path is byte-for-byte the pre-D6
#	      resolve_from_remote_ref (no widening, zero regression).
#	  L6  dead-owner (spec L3): node0 fail-stops; node1's fresh read of the
#	      seed tuple hits the verdict serve-gate deny -> UNKNOWN -> 53R97,
#	      never a stale false-visible.  The crash-rejoin positive leg
#	      (survivor SERVES the dead owner's verdict) is honestly forward-D4
#	      (Rule 8.B): D6 tests only the deny side.
#
# Spec: spec-5.22f-shared-catalog-seed-visibility-consumer.md (D6, §4.2)
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/359_cluster_5_22f_seed_visibility.pl
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

sub state_val
{
	my ($node, $cat, $key) = @_;
	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state WHERE category='$cat' AND key='$key'});
	return defined($v) && $v ne '' ? $v + 0 : 0;
}

sub write_retry
{
	my ($node, $sql) = @_;
	for my $i (1 .. 10)
	{
		my $ok = eval { $node->safe_psql('postgres', $sql); 1 };
		return 1 if $ok;
		usleep(500_000);
	}
	return 0;
}

sub poll_until
{
	my ($node, $sql, $want, $secs) = @_;
	my $deadline = time() + $secs;
	while (time() < $deadline)
	{
		my $v = eval { $node->safe_psql('postgres', $sql); };
		return 1 if defined $v && $v eq $want;
		usleep(250_000);
	}
	return 0;
}

sub arm_guc_both
{
	my ($pair, $guc, $val) = @_;
	for my $n ($pair->node0, $pair->node1)
	{
		$n->safe_psql('postgres', "ALTER SYSTEM SET $guc = $val");
		$n->safe_psql('postgres', 'SELECT pg_reload_conf()');
	}
	usleep(1_000_000);
}

# ============================================================
# L1: boot + shared cluster_fs; seed table + coherence armed; the seed
#     xact's undo lands on the SHARED root (readable by node1 as foreign).
# ============================================================
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'spec_5_22f_seed_vis',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.ges_request_timeout_ms = 30000',
		'cluster.gcs_reply_timeout_ms = 3000',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 5',
		'cluster.undo_segments_max_per_instance = 256',
		'cluster.undo_segment_create_timeout_ms = 5000',
		# crossnode_runtime_visibility stays OFF at boot (L2 is the 命门 2
		# baseline; L3 arms it so the off->on fix transition is proven).
		# xid_striping stays OFF -> the seed xid is underivable on node1
		# (root-cause #1 pre-striping timeline; Option B still resolves it).
	]);
$pair->start_pair;
usleep(2_000_000);

ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'L1 node0 sees node1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'L1 node1 sees node0 connected');

my ($node0, $node1) = ($pair->node0, $pair->node1);

is($node0->safe_psql('postgres', 'SHOW cluster.crossnode_runtime_visibility'),
	'off', 'L1 crossnode_runtime_visibility default off (命门 2 baseline)');

# Phantom-shared seed heap table (coincident relfilepath, like t/346/t/255).
$node0->safe_psql('postgres', 'CREATE TABLE s_t (id int, v int)');
$node1->safe_psql('postgres', 'CREATE TABLE s_t (id int, v int)');
my $p0 = $node0->safe_psql('postgres', q{SELECT pg_relation_filepath('s_t')});
my $p1 = $node1->safe_psql('postgres', q{SELECT pg_relation_filepath('s_t')});
is($p0, $p1, 'L1 s_t relfilepath coincidence holds (phantom-shared)');

# Arm undo GCS coherence so node0's seed undo migrates to the shared root.
arm_guc_both($pair, 'cluster.undo_gcs_coherence', 'on');
is($node0->safe_psql('postgres', 'SHOW cluster.undo_gcs_coherence'),
	'on', 'L1 undo_gcs_coherence armed on');

# Force a fresh segment under coherence=on so the seed xact's undo lands on
# the shared cluster_fs root, then commit the NORMAL-xid seed INSERT (ONE xact,
# NOT recycled -> the tuple's ITL slot still binds the seed xid: a FRESH ref).
ok(write_retry($node0, q{SELECT cluster_undo_test_force_segment_end()}),
	'L1 forced fresh undo segment under coherence=on');
ok(write_retry($node0, 'INSERT INTO s_t SELECT g, g * 10 FROM generate_series(1, 8) g'),
	'L1 seed xact committed 8 NORMAL-xid rows (fresh ITL binding, undo on shared root)');
ok(write_retry($node0, 'CHECKPOINT'), 'L1 checkpoint');

# ============================================================
# L2: 命门 2 baseline -- crossnode OFF -> node1's fresh read of the seed
#     tuple fails closed 53R97 (fresh remote ITL ref, overlay miss, no
#     widening).  This is the RED state D6 fixes.
# ============================================================
{
	my ($rc, $out, $err) = $node1->psql('postgres', 'SELECT count(*) FROM s_t');
	isnt($rc, 0, 'L2 crossnode-off: node1 fresh read of seed errors (fail-closed)');
	like($err, qr/cluster TT status unknown/,
		'L2 命门 2: overlay-miss on the fresh remote ITL ref keeps 53R97 (no widening)');
}

# ============================================================
# L3: ROOT-CAUSE #1 FIX (spec L1) -- crossnode ON -> node1's fresh read
#     SUCCEEDS via the D6 fresh-ref widening -> D3 verdict.  The
#     vis_freshref_verdict_resolved_count increment is a HARD assertion (P1-b).
# ============================================================
arm_guc_both($pair, 'cluster.crossnode_runtime_visibility', 'on');
is($node1->safe_psql('postgres', 'SHOW cluster.crossnode_runtime_visibility'),
	'on', 'L3 crossnode_runtime_visibility armed on both nodes');

{
	my $resolved0 = state_val($node1, 'cr', 'vis_freshref_verdict_resolved_count');

	# A first read may fail 53R97 while the shared undo / verdict wire warms,
	# or be refused by leg (e) clock skew (the Lamport observe heals the next).
	# Poll a fresh backend until the seed resolves visible -- but the SUCCESS
	# itself (8 rows) and the counter increment are real, never faked.
	my $ok_rows = 0;
	my $last_err = '';
	for my $try (1 .. 20)
	{
		my ($rc, $out, $err) = $node1->psql('postgres', 'SELECT count(*) FROM s_t');
		if (defined $out && $out =~ /^8$/m) { $ok_rows = 1; last; }
		$last_err = $err // '';
		usleep(500_000);
	}

	# Diagnostic snapshot of the D2->D3->D6 chain (localizes a 0-resolve stall).
	for my $k (qw(vis_freshref_verdict_resolved_count vis_freshref_verdict_failclosed_count
		rtvis_undo_fetch_wire_count rtvis_undo_fetch_failclosed_count
		rtvis_resolve_committed_count rtvis_resolve_failclosed_count
		rtvis_verdict_wire_count rtvis_verdict_failclosed_count
		rtvis_underivable_failclosed_count))
	{
		diag("L3 node1 cr.$k = " . state_val($node1, 'cr', $k));
	}
	for my $k (qw(cr_server_undo_served_count cr_server_undo_denied_count
		cr_server_verdict_served_count cr_server_verdict_denied_count))
	{
		diag("L3 node0 cr.$k = " . state_val($node0, 'cr', $k));
	}
	diag("L3 last node1 read error: $last_err") if $last_err;

	ok($ok_rows,
		'L3 ROOT-CAUSE #1 FIX: node1 fresh read sees the 8 committed seed rows (D6 widening -> verdict)');

	cmp_ok(state_val($node1, 'cr', 'vis_freshref_verdict_resolved_count'), '>', $resolved0,
		'L3 P1-b HARD ASSERT: vis_freshref_verdict_resolved_count moved (the fresh-ref widening path ran, not overlay/propagation)');
}

# ============================================================
# L4: aborted seed (spec L2) -- a rolled-back seed row resolves ABORTED via
#     the verdict -> NOT visible (never false-visible).
# ============================================================
{
	# A distinguishable row inserted then rolled back on node0.  Its tuple
	# stays physically on the shared page (no vacuum), so node1 sees it and
	# must resolve its xmin ABORTED -> invisible.
	$node0->safe_psql('postgres', q{
		BEGIN;
		INSERT INTO s_t VALUES (999, -1);
		ROLLBACK;
	});
	ok(write_retry($node0, 'CHECKPOINT'), 'L4 checkpoint after aborted seed row');

	my $seen_aborted = 0;
	for my $try (1 .. 20)
	{
		my ($rc, $out, $err) = $node1->psql('postgres',
			'SELECT count(*) FROM s_t WHERE id = 999');
		if (defined $out && $out =~ /^0$/m) { $seen_aborted = 1; last; }
		usleep(500_000);
	}
	ok($seen_aborted,
		'L4 aborted seed row is NOT visible on node1 (verdict ABORTED, never false-visible)');
}

# ============================================================
# L5: off regression (spec L4) -- crossnode OFF again -> a fresh backend
#     fails closed 53R97; the off path is the pre-D6 resolve_from_remote_ref
#     (no widening, zero regression to the single-node / off surface).
# ============================================================
{
	arm_guc_both($pair, 'cluster.crossnode_runtime_visibility', 'off');

	my ($rc, $out, $err) = $node1->psql('postgres', 'SELECT count(*) FROM s_t');
	isnt($rc, 0, 'L5 crossnode-off regression: fresh backend read fails closed again');
	like($err, qr/cluster TT status unknown/,
		'L5 off path unchanged (widening gated off -> byte-for-byte pre-D6 overlay-miss 53R97)');

	# Re-arm for the dead-owner leg.
	arm_guc_both($pair, 'cluster.crossnode_runtime_visibility', 'on');
}

# ============================================================
# L6: dead-owner (spec L3) -- node0 fail-stops; node1's fresh read of the
#     seed hits the verdict serve-gate deny -> UNKNOWN -> 53R97, never a
#     stale false-visible.  The crash-rejoin positive leg (survivor SERVES
#     the dead owner's verdict) is honestly forward-D4 (Rule 8.B).
# ============================================================
{
	$pair->kill_node9(0);
	ok( poll_until($node1,
			q{SELECT state IN ('suspected','dead') FROM pg_cluster_cssd_peers WHERE node_id = 0},
			't', 40),
		'L6 node1 CSSD marks node0 suspected/dead');

	# A fresh backend read: the origin is dead, the verdict serve-gate denies,
	# and the read MUST error -- above all it must NOT return the 8 seed rows
	# from a stale path.
	my ($rc, $out, $err) = $node1->psql('postgres', 'SELECT count(*) FROM s_t');
	isnt($rc, 0, 'L6 dead-owner: node1 fresh read errors (fail-closed, not stale false-visible)');
	unlike(($out // ''), qr/^\s*8\s*$/m,
		'L6 dead-owner read did NOT serve the 8 seed rows from a stale path');
	like($err,
		qr/cluster TT status unknown|failed to enqueue|fail-stop|could not obtain|connection/i,
		'L6 the failure is a cluster fail-closed error (serve-gate deny -> 53R97; positive serve = forward-D4)');
}

$pair->stop_pair if $pair->can('stop_pair');
done_testing();
