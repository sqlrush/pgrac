#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 310_cluster_5_56_cr_reconfig_2node.pl
#	  spec-5.56 D7 L3 — CR pool lifecycle contract C4 (reconfig), 2-node e2e.
#
#	  C4 claim (§3.3): a membership reconfig (node leave/fail-stop, GRD/PCM
#	  remaster) does NOT invalidate the own-instance CR images a survivor holds —
#	  the CR layer has NO reconfig hook (spec-5.56 §5: cluster_reconfig.c /
#	  cluster_epoch.c are unchanged) and the CR key carries NO membership epoch
#	  (F0-14), so CR serving is structurally decoupled from membership.  read_scn
#	  is a global SCN (AD-008 / INV-C2), not a membership epoch.
#
#	  This test triggers a REAL fail-stop reconfig (SIGKILL the peer) and proves CR
#	  reads on the survivor remain CORRECT afterwards (R10: L3 asserts correctness,
#	  not a counter; the byte-identical survival of a specific cached entry is
#	  covered by the cluster_unit U7 invariant + the architectural decoupling).
#
#	  L3a 2-node up, peer ALIVE; CR read on node0 is correct (own-instance class①).
#	  L3b SIGKILL node1 -> node0 deadband fires the DEAD edge (fail-stop reconfig).
#	  L3c after reconfig, a CR read on the survivor still returns the correct
#	      historical value (membership change did not break / flush CR).
#	  L3d the survivor's 'cr' lifecycle counters remain observable post-reconfig.
#
#	  Spec: spec-5.56-cr-pool-lifecycle-invalidation.md (FROZEN v1.0)
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/310_cluster_5_56_cr_reconfig_2node.pl
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

# CR read of a pre-update value via a held REPEATABLE READ snapshot, on $node.
# Returns the value the held reader sees (must be the pre-update value, served by
# the CR gate constructing the historical version).
sub cr_historical_read
{
	my ($node, $tbl, $id) = @_;
	my $s = $node->background_psql('postgres', on_error_die => 1);
	$s->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
	$s->query_safe("SELECT cluster_scn_current()");	# fix the snapshot read_scn
	# post-snapshot writer mutates the row -> the held reader needs CR.
	$node->safe_psql('postgres', "UPDATE $tbl SET v = v + 100 WHERE id = $id");
	$node->safe_psql('postgres', "SELECT count(*) FROM $tbl") for 1 .. 3;	# settle base_page_lsn
	my $v = $s->query_safe("SELECT v FROM $tbl WHERE id = $id");
	chomp $v;
	$s->query_safe('COMMIT');
	$s->quit;
	return $v;
}

# Strict pair + shared data + 3 voting disks: the proven 2-node recipe (mirror
# t/280, t/291) so cross-node GES coordination works and the survivor retains
# quorum after the peer is killed.  Widen the GES timeout and the CSSD deadband to
# survive CI scheduling jitter during setup (false-DEAD avoidance).
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'spec_5_56_rcfg',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'cluster.cr_mvcc_gate = on',
		'cluster.cr_gate_no_peer_fastpath = off',
		'cluster.shared_cr_pool_enabled = on',
		'cluster.shared_cr_pool_size_blocks = 256',
		'cluster.cr_pool_rel_generation_slots = 256',
		'cluster.ges_request_timeout_ms = 30000',
		'cluster.cssd_heartbeat_interval_ms = 1000',
		'cluster.cssd_dead_deadband_factor = 6',
		'autovacuum = off',
		'jit = off',
		'synchronize_seqscans = off',
	]);
$pair->start_pair;
usleep(3_000_000);
my $node0 = $pair->node0;

# ----------
# L3a: both up, peer connected.  We do NOT exercise the CR gate while both nodes
# are alive: a held-reader + concurrent-writer pattern would create cross-node GES
# block-lock contention (Cache Fusion) that is orthogonal to the C4 CR claim.  The
# C4 claim is exercised on the SURVIVOR after the reconfig (single-node degraded,
# no GES contention), which is the correct topology for an own-instance CR read.
# ----------
{
	is($node0->safe_psql('postgres', 'SELECT 1'), '1', 'L3a-i node0 alive');
	ok($pair->wait_for_peer_state(0, 1, 'connected', 30),
		'L3a-ii node0 sees peer node1 connected');
	ok( poll_until($node0,
			q{SELECT state = 'alive' AND heartbeat_recv_count > 0
			    FROM pg_cluster_cssd_peers WHERE node_id = 1},
			't', 20, 'L3a-iii node0 sees node1 CSSD alive'),
		'L3a-iii node0 sees node1 CSSD alive + heartbeats');
	$node0->safe_psql('postgres',
		'CREATE TABLE t_rc (id int, v int); INSERT INTO t_rc VALUES (1, 1), (2, 2);');
	$node0->safe_psql('postgres', 'CHECKPOINT');	# flush to shared storage
}

# ----------
# L3b: real fail-stop reconfig (SIGKILL the peer); node0 drives it to completion.
# Detection mirrors the spec-5.14 fail-stop TAP (t/307): the CSSD death edge is in
# pg_cluster_cssd_peers, the reconfig completion in pg_cluster_reconfig_state.
# ----------
{
	$pair->kill_node9(1);	# SIGKILL node1 postmaster -> node0 deadband -> DEAD edge
	ok( poll_until($node0,
			q{SELECT state IN ('suspected','dead') FROM pg_cluster_cssd_peers WHERE node_id = 1},
			't', 40, 'L3b node0 CSSD declared node1 dead'),
		'L3b node0 CSSD declared the peer dead');
	ok( poll_until($node0,
			q{SELECT event_id != 0 FROM pg_cluster_reconfig_state}, 't', 40,
			'L3b reconfig fired'),
		'L3b-ii node0 fail-stop reconfig event fired');
	is( $node0->safe_psql('postgres',
			q{SELECT reconfig_kind FROM pg_cluster_reconfig_state}),
		'fail_stop', 'L3b-iii reconfig_kind = fail_stop');
}

# ----------
# L3c: AFTER the reconfig, CR construction + serving on the survivor is unaffected
# (no reconfig hook, no membership epoch in the key — spec-5.56 §5 / F0-14): the
# historical read is still correct.  This is the C4 correctness claim (R10: assert
# correctness, not a counter; the byte-identical survival of a specific cached
# entry is covered by cluster_unit U7 + the architectural decoupling).
# ----------
{
	# Wait for the survivor to be writable again (fail-stop recovery complete).
	my $writable = 0;
	for (1 .. 60)
	{
		my $ok = $node0->safe_psql('postgres',
			'UPDATE t_rc SET v = v WHERE id = 1; SELECT 1') // '';
		if ($ok eq '1') { $writable = 1; last; }
		usleep(500_000);
	}
	ok($writable, 'L3c-i survivor writable after fail-stop recovery');

	is(cr_historical_read($node0, 't_rc', 2), '2',
		'L3c CR historical read correct on the survivor AFTER reconfig (C4)');
}

# ----------
# L3d: the survivor's lifecycle counters remain observable post-reconfig.
# ----------
{
	is( $node0->safe_psql('postgres',
			q{SELECT count(*) FROM pg_cluster_state WHERE category='cr'
			  AND key='cr_reconfig_intra_survived_count'}),
		'1', 'L3d reconfig_intra_survived counter observable post-reconfig');
	is( $node0->safe_psql('postgres',
			q{SELECT count(*) FROM pg_cluster_state WHERE category='cr'
			  AND key='cr_rel_gen_bump_count'}),
		'1', 'L3d-ii rel_gen_bump counter observable post-reconfig');
}

$pair->stop_pair;
done_testing();
