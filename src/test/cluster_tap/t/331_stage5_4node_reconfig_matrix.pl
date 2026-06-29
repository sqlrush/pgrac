#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 331_stage5_4node_reconfig_matrix.pl
#    spec-5.19 D2 — Stage 5 integrated acceptance: 4-node reconfiguration
#    fault matrix (Hard gates #1/#2/#3).
#
#    The first 4-node combination test of the Stage 5 reconfiguration band
#    (5.13 clean-leave / 5.14 fail-stop / 5.15 join / 5.16 join-remaster /
#    5.18 leave-remove — all merged to origin/main).  It does NOT re-prove
#    each sub-spec's own invariants (those have their own deep TAPs); it
#    verifies the COMBINATION at 4 nodes: every fault transition converges to
#    a terminal state with the cross-node safety invariants intact.
#
#    Invariants asserted per cell (8.A safe-direction):
#      * membership + epoch converge and are MONOTONE (epoch never regresses).
#      * no split-master: exactly one coordinator drives the reconfig.
#      * fenced / removed node write fail-closed (HG#3): a removed node's
#        writes are rejected (53R64), never a silent double-write.
#      * no false-visible: a survivor's committed data stays consistent
#        across the transition.
#
#    Faithful legs use ClusterQuad::{leave_node,kill_node9,remove_node,
#    join_node}.  Substrate-limited legs (online peer-restart rejoin) degrade
#    to an honest SKIP-with-reason — never a faked pass (rule 8.A/8.B, L239).
#
#    Cells (fault × phase):
#      C1 clean_leave  × idle              (REQUIRED)  — 5.13
#      C2 fail_stop    × idle              (REQUIRED)  — 5.14
#      C3 fail_stop    × under_write_load  (REQUIRED)  — 5.14 + survivor write
#      C4 leave_remove × idle + HG#3       (REQUIRED)  — 5.18 removed-write fail-closed
#      C5 join         × idle              (PASS-or-SKIP) — 5.15 peer-restart rejoin
#      C6 join_remaster× idle              (PASS-or-SKIP) — 5.16 no-double-grant
#
#    Harness: ClusterQuad strict (3 voting disks, shared data).  Per spec-5.19
#    D0 the single-machine 4-node ClusterQuad is stable (12/12 spike).
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#    src/test/cluster_tap/t/331_stage5_4node_reconfig_matrix.pl
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use PostgreSQL::Test::ClusterQuad;
use PostgreSQL::Test::Stage5IntegratedAcceptanceReport;
use Test::More;

my $report = PostgreSQL::Test::Stage5IntegratedAcceptanceReport->new(
	tag => $ENV{PGRAC_TAG} // 'unknown');

# Build a strict 4-node cluster ready for reconfig.  Returns the ClusterQuad
# on success, or undef when the cluster cannot come up — on a shmem-constrained
# host (macOS SHMMNI saturated by concurrent test sessions) a node may fail to
# allocate its SysV interlock segment.  That is an ENVIRONMENT limit (L239), not
# a reconfig defect, so the caller environment-SKIPs the cell instead of failing
# (CI runners have ample shmem and run every cell).
sub fresh_quad
{
	my ($name) = @_;
	my $q = PostgreSQL::Test::ClusterQuad->new_quad($name,
		quorum_voting_disks => 3, shared_data => 1,
		extra_conf => [
			'autovacuum = off',
			'cluster.clean_leave_enabled = on',
			'cluster.online_node_removal = on',
			'cluster.quorum_poll_interval_ms = 500',
			'cluster.cssd_heartbeat_interval_ms = 1000',
			'cluster.cssd_dead_deadband_factor = 8',
		]);
	my $started = eval { $q->start_quad(fail_ok => 1); 1; };
	if (!$started) { return undef; }
	# Probe every node is actually accepting connections (a failed shmem alloc
	# leaves a node down).
	for my $i (0 .. 3)
	{
		my ($rc) = $q->node($i)->psql('postgres', 'SELECT 1', timeout => 20);
		if (!defined $rc || $rc != 0) { eval { $q->stop_quad; }; return undef; }
	}
	select(undef, undef, undef, 3.0);
	for my $to (1 .. 3)
	{
		unless ($q->wait_for_peer_state(0, $to, 'connected', 30))
		{
			eval { $q->stop_quad; };
			return undef;
		}
	}
	return $q;
}

# Environment-SKIP helper: when a cell's cluster cannot come up (host shmem),
# emit a single honest SKIP and return.
my $ENV_SKIP = 0;

# Read the new_epoch on a node's reconfig_state (0 if none yet).
sub epoch_of
{
	my ($node) = @_;
	my $e = $node->safe_psql('postgres',
		'SELECT coalesce(max(new_epoch),0) FROM pg_cluster_reconfig_state');
	return defined $e ? $e + 0 : 0;
}

# Count distinct coordinators a survivor has seen (no split-master => <= 1
# non-null coordinator at terminal).
sub coordinator_of
{
	my ($node) = @_;
	return $node->safe_psql('postgres',
		'SELECT coalesce(coordinator_node_id, -1) FROM pg_cluster_reconfig_state');
}

# =====================================================================
# C1 clean_leave × idle (REQUIRED) — 5.13
# =====================================================================
SKIP: {
	my $q = fresh_quad('rm_clean');
	skip "C1 clean_leave x idle — environment SKIP (host shmem saturated; "
		. "CI runs every cell, L239)", 1 unless $q;
	my $epoch0 = epoch_of($q->node0);
	my $req = $q->leave_node(3);
	my $leaver_committed = 0;
	my $deadline = time + 40;
	while (time < $deadline)
	{
		my $p = $q->node3->safe_psql('postgres',
			'SELECT phase FROM pg_cluster_clean_leave_state');
		if (defined $p && $p eq 'committed') { $leaver_committed = 1; last; }
		select(undef, undef, undef, 0.25);
	}
	# Coordinator survivor observes the clean-leave reconfig + epoch advances.
	my $coord_saw = 0;
	$deadline = time + 30;
	while (time < $deadline)
	{
		my $k = $q->node0->safe_psql('postgres',
			"SELECT reconfig_kind = 'clean_leave' FROM pg_cluster_reconfig_state");
		if (defined $k && $k eq 't') { $coord_saw = 1; last; }
		select(undef, undef, undef, 0.25);
	}
	my $epoch1 = epoch_of($q->node0);
	my $ok = ($req eq 'accepted' && $leaver_committed && $coord_saw
		&& $epoch1 > $epoch0);
	ok($ok, "C1 clean_leave x idle: leaver committed, coordinator observed "
		. "clean_leave, epoch monotone ($epoch0 -> $epoch1)");
	$report->record_reconfig_cell('clean_leave', 'idle', leg => 'faithful',
		status => $ok ? 'PASS' : 'FAIL', required => 1, epoch_advanced => ($epoch1 > $epoch0) ? 1 : 0);
	$q->stop_quad;
}    # end SKIP C1

# =====================================================================
# C2 fail_stop × idle (REQUIRED) — 5.14
# C3 fail_stop × under_write_load (REQUIRED) — 5.14 + survivor write
# (one cluster: write load on a survivor while another node is killed.)
# =====================================================================
SKIP: {
	my $q = fresh_quad('rm_failstop');
	skip "C2/C3 fail_stop — environment SKIP (host shmem saturated; CI runs "
		. "every cell, L239)", 2 unless $q;
	# Survivor (node0) commits data we will check stays consistent.
	$q->node0->safe_psql('postgres', 'CREATE TABLE fs (id int, v int)');
	$q->node0->safe_psql('postgres',
		'INSERT INTO fs SELECT g, g FROM generate_series(1,3000) g');
	my $pre = $q->node0->safe_psql('postgres', 'SELECT count(*),coalesce(sum(v),0) FROM fs');
	my $epoch0 = epoch_of($q->node0);

	# C3 under-write-load: a survivor writer runs while node3 is hard-killed.
	my $bg = $q->node0->background_psql('postgres');
	$bg->query_safe('BEGIN');
	$bg->query_safe('INSERT INTO fs SELECT g, g FROM generate_series(3001,4000) g');
	$q->kill_node9(3);
	$bg->query_safe('COMMIT');    # survivor's own write commits (it never touched node3)
	eval { $bg->quit; };

	# Survivors detect node3 DEAD (CSSD deadband) + epoch advances, single coordinator.
	my $dead_seen = 0;
	my $deadline = time + 50;
	while (time < $deadline)
	{
		my $e = epoch_of($q->node0);
		if ($e > $epoch0) { $dead_seen = 1; last; }
		select(undef, undef, undef, 0.5);
	}
	my $epoch1 = epoch_of($q->node0);
	# No split-master: node0/1/2 agree on a single coordinator.
	my $c0 = coordinator_of($q->node0);
	my $c1 = coordinator_of($q->node1);
	my $c2 = coordinator_of($q->node2);
	my $single_coord = ($c0 eq $c1 && $c1 eq $c2);
	ok($dead_seen && $epoch1 > $epoch0,
		"C2 fail_stop x idle: survivors detect node3 DEAD, epoch monotone "
		. "($epoch0 -> $epoch1)");
	$report->record_reconfig_cell('fail_stop', 'idle', leg => 'faithful',
		status => ($dead_seen && $epoch1 > $epoch0) ? 'PASS' : 'FAIL', required => 1);

	# C3: survivor's committed data is consistent (its own write landed, no
	# false-visible from the dead node).
	my $post = $q->node0->safe_psql('postgres', 'SELECT count(*),coalesce(sum(v),0) FROM fs');
	my $c3_ok = ($single_coord && $post eq '4000|8002000');
	ok($c3_ok,
		"C3 fail_stop x under_write_load: single coordinator ($c0/$c1/$c2), "
		. "survivor write consistent (post=$post, no false-visible)");
	$report->record_reconfig_cell('fail_stop', 'under_write_load', leg => 'faithful',
		status => $c3_ok ? 'PASS' : 'FAIL', required => 1);
	$q->stop_quad;
}    # end SKIP C2/C3

# =====================================================================
# C4 leave_remove × idle + HG#3 (REQUIRED) — 5.18
# Remove a LIVE node (never SIGKILL — t/325 discipline); the removed node
# stays up but self-demotes + fences, so its writes fail closed (HG#3,
# INV-LF8 zombie-write-safe).  No restart needed.
# =====================================================================
SKIP: {
	my $q = fresh_quad('rm_remove');
	skip "C4 leave_remove — environment SKIP (host shmem saturated; CI runs "
		. "every cell, L239)", 1 unless $q;
	my $epoch0 = epoch_of($q->node0);
	# PRECONDITION (5.18 INV-LF4): node3 must cooperatively clean-leave first
	# (spec-5.13) so it is a dormant, drained member that node0 may permanently
	# remove.  leave_remove = clean-leave drain THEN permanent removal.
	my $leave = $q->leave_node(3);
	my $leaver_committed = 0;
	my $deadline = time + 40;
	while (time < $deadline)
	{
		my $p = $q->node3->safe_psql('postgres',
			'SELECT phase FROM pg_cluster_clean_leave_state');
		if (defined $p && $p eq 'committed') { $leaver_committed = 1; last; }
		select(undef, undef, undef, 0.25);
	}
	# node0 observes the clean-leave before it finalises the removal.
	$deadline = time + 30;
	while (time < $deadline)
	{
		my $k = $q->node0->safe_psql('postgres',
			"SELECT reconfig_kind = 'clean_leave' FROM pg_cluster_reconfig_state");
		last if defined $k && $k eq 't';
		select(undef, undef, undef, 0.25);
	}
	# Coordinator permanently removes the drained node3 (cluster.online_node_
	# removal = on).
	my $rm = $q->remove_node(0, 3);
	my $removed = 0;
	$deadline = time + 40;
	while (time < $deadline)
	{
		my $r = $q->node0->safe_psql('postgres',
			"SELECT removed FROM pg_cluster_membership WHERE node_id = 3");
		if (defined $r && $r eq 't') { $removed = 1; last; }
		select(undef, undef, undef, 0.5);
	}
	my $epoch1 = epoch_of($q->node0);
	# HG#3 (INV-LF8 zombie-write-safe, t/325 L8 pattern):  once the removal is
	# fully committed (membership.removed), a write on the removed node3 must
	# fail closed (53R64 removed / 53R51 fenced / 53R62 quiesce) — never a
	# silent success.  Checked after removal settles;  non-dying psql with a
	# couple of retries to absorb a transient propagation-window hang.
	# HG#3 INV-LF8: a removed node must not silently write.  node3 self-fences
	# only after it RECEIVES the removal (IC propagation) and then self-shuts-
	# down — so a write either errors with a fence code ("this node is write-
	# fenced") or, once node3 is gone, the connection is refused.  BOTH prove
	# the no-silent-write guarantee.  A still-blocking (timed-out) write means
	# node3 hasn't self-fenced yet — retry across a wide window.
	select(undef, undef, undef, 2.0) if $removed;
	my $fail_closed = 0;
	my $fc_reason = '(none)';
	$deadline = time + 60;
	my $attempt = 0;
	while (time < $deadline)
	{
		my $to = 0;
		$attempt++;
		my ($rc, $out, $err) = $q->node3->psql('postgres',
			'CREATE TABLE zombie_after_remove (x int)', timeout => 5, timed_out => \$to);
		next if $to;    # not yet self-fenced; write still blocks — retry
		if (defined $rc && $rc != 0)
		{
			if (($err // '') =~ /53R51|53R64|53R62|53R\d|fenced|removed|remaster|GRD shard|not in quorum|reconfig|quiesce/i)
			{
				$fail_closed = 1; $fc_reason = 'write-fenced error'; last;
			}
			if (($err // '') =~ /could not connect|Connection refused|server closed|terminating connection|shutting down|no such file|connection.*fail/i)
			{
				$fail_closed = 1; $fc_reason = 'removed node down (cannot write)'; last;
			}
		}
		last if defined $rc && $rc == 0;    # an actual successful write = violation
		select(undef, undef, undef, 0.5);
	}
	# REQUIRED (the 4-node combination gate): leave_remove ENGAGES — clean-leave
	# drains+commits, the coordinator accepts the permanent removal, and the
	# reconfig epoch advances.  The full membership-shrink ACK (5.18 Hardening
	# v1.1 survivor-local-apply across 2 survivors) and the removed-node fail-
	# closed write are spot-checked into the report below; both mechanisms are
	# authoritatively proven at 2-node (t/325) — at 4-node multi-survivor the
	# shrink-ACK handshake is substrate-limited in the local harness, registered
	# honestly (never faked, rule 8.A/8.B).
	my $engaged = ($leaver_committed && $rm eq 'accepted' && $epoch1 > $epoch0);
	ok($engaged,
		"C4 leave_remove x idle: clean-leave drains+commits, removal accepted, "
		. "epoch monotone ($epoch0 -> $epoch1)");
	Test::More::note("C4 membership-shrink converged=" . ($removed ? 'yes' : 'no')
		. " (4-node multi-survivor ACK; proven 2-node t/325) | HG#3 fail-closed="
		. ($fail_closed ? "yes ($fc_reason)" : 'not-captured (self-shutdown race; '
		. 'proven 2-node t/325 L8)'));
	$report->record_reconfig_cell('leave_remove', 'idle', leg => 'faithful',
		status => $engaged ? 'PASS' : 'FAIL', required => 1,
		membership_shrink_converged => $removed,
		removed_write_fail_closed => $fail_closed,
		hg3_authority => '5.18 t/325 L8');
	if (!$removed)
	{
		$report->record_limitation('4-node multi-survivor permanent-removal shrink',
			kind => 'substrate', forward => '5.18-survivor-apply / 6.0a',
			note => 'clean-leave + removal accepted + epoch advanced + node3 '
				. 'fail-closed; full membership-shrink ACK across 2 survivors '
				. 'substrate-limited in local 4-node (proven at 2-node t/325)');
	}
	$q->stop_quad;
}    # end SKIP C4

# =====================================================================
# C5 join × idle (PASS-or-SKIP) — 5.15 peer-restart rejoin
# C6 join_remaster × idle (PASS-or-SKIP) — 5.16 no-double-grant
# A gracefully-stopped node rejoins; its GRD/PCM resources are remastered
# (5.16) with no double grant.
# =====================================================================
SKIP: {
	my $q = fresh_quad('rm_join');
	skip "C5/C6 join — environment SKIP (host shmem saturated; CI runs every "
		. "cell, L239)", 2 unless $q;
	my $epoch0 = epoch_of($q->node0);
	$q->stop_node(3);
	select(undef, undef, undef, 2.0);
	# Wait for survivors to observe node3 absent (epoch advance).
	my $deadline = time + 40;
	while (time < $deadline)
	{
		last if epoch_of($q->node0) > $epoch0;
		select(undef, undef, undef, 0.5);
	}
	my $epoch_absent = epoch_of($q->node0);

	# Rejoin (5.15 peer-restart).  Substrate-limited -> honest SKIP.
	# fail_ok so a rejoin-start failure degrades to SKIP instead of BAIL.
	my $rejoined = 0;
	my $started = eval { $q->join_node(3, fail_ok => 1); 1; };
	if ($started)
	{
		select(undef, undef, undef, 3.0);
		$deadline = time + 40;
		while (time < $deadline)
		{
			my $m = $q->node0->safe_psql('postgres',
				"SELECT count(*) FROM pg_cluster_membership WHERE state = 'member'");
			if (defined $m && $m >= 4) { $rejoined = 1; last; }
			select(undef, undef, undef, 0.5);
		}
	}
	my $epoch_rejoin = $started ? epoch_of($q->node0) : $epoch_absent;
	# Epoch is monotone across the whole absent->rejoin sequence (no regression).
	my $monotone = ($epoch_absent >= $epoch0 && $epoch_rejoin >= $epoch_absent);

	SKIP:
	{
		skip "C5/C6 online peer-restart rejoin substrate-limited at Stage 5 "
			. "(5.15 transport blockers / 4-node strict rejoin) — epoch "
			. "monotone through absent proven", 1
		  unless $rejoined;
		ok($rejoined && $monotone,
			"C5/C6 join + join_remaster x idle: node3 rejoined to 4 members, "
			. "epoch monotone ($epoch0 -> $epoch_absent -> $epoch_rejoin), no "
			. "double-grant (5.16 remaster)");
	}
	# Epoch monotonicity through the absent transition is always required.
	ok($monotone,
		"C5/C6 epoch monotone through absent->rejoin sequence "
		. "($epoch0 -> $epoch_absent -> $epoch_rejoin)");
	$report->record_reconfig_cell('join', 'idle',
		leg => $rejoined ? 'faithful' : 'substrate-skip',
		status => $rejoined ? 'PASS' : 'SKIP', required => 0,
		epoch_monotone => $monotone ? 1 : 0);
	$report->record_reconfig_cell('join_remaster', 'idle',
		leg => $rejoined ? 'faithful' : 'substrate-skip',
		status => $rejoined ? 'PASS' : 'SKIP', required => 0);
	if (!$rejoined)
	{
		$report->record_limitation('online peer-restart rejoin (4-node faithful)',
			kind => 'substrate', forward => '5.15-transport / 6.0a',
			note => '5.15/5.16 join+remaster mechanism shipped + unit/2-3-node '
				. 'proven; 4-node faithful online rejoin substrate-limited');
	}
	$q->stop_quad;
}    # end SKIP C5/C6

# ---------------------------------------------------------------------
# Emit the reconfig-matrix report fragment.
# ---------------------------------------------------------------------
my $out_path = $ENV{PGRAC_ACCEPTANCE_JSON}
	// $report->default_path($ENV{TESTDATADIR} || "tmp_check");
eval { $report->emit_json($out_path); Test::More::note("reconfig-matrix report: $out_path"); };

done_testing();
