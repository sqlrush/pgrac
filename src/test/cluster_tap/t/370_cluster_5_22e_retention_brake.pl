#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 370_cluster_5_22e_retention_brake.pl
#	  spec-5.22e D5 — cluster-wide undo retention brake end-to-end on a
#	  2-node ClusterPair + shared cluster_fs root (t/369 family recipe).
#
#	  The brake arms whenever the MEMBER set has a peer (Q3'': independent
#	  of the consumption GUCs), so on this pair every cleaner pass folds
#	  the local horizon with node1's report.  Legs:
#
#	  L0   report plumbing: node0's dump shows node1's report valid with a
#	       current-connection capability (and vice versa).
#	  L1   brake pin/release: node1 holds a REPEATABLE READ snapshot ->
#	       node0's proven floor freezes at node1's pinned read_scn and the
#	       churn commits' TT slots stay UNrecycled (shmem GC counter
#	       frozen); node1's cross-node read keeps succeeding (no 53R97
#	       tail-lash); release -> floor advances past the pin and the
#	       held-back slots recycle (counter moves).  Non-vacuous by
#	       construction: the same churn recycles once the pin lifts.
#	  L2   stall + self-heal (L408): the report-drop injection (armed via
#	       the conf face -- the handler runs in LMON, SQL arming is
#	       process-local) ages node1's slot out ->
#	       undo_horizon_stall_count moves, "recycle stalled" LOG-once,
#	       recycling pauses; disarm -> reports resume, stall stops
#	       growing, the floor advances again.
#	  L6   F-D2 epoch fence (L408): the epoch-fence injection (cleaner
#	       process, conf face) forces the tripped arm at the first
#	       mutation -> undo_horizon_pass_abort_count moves and the pass
#	       recycles nothing; disarm -> recycling resumes.
#	  L3   member-drop self-heal: node1 dies (immediate) -> dead-decide +
#	       reconfig drops it from the required set -> after a transient
#	       stall window node0's floor advances again WITHOUT node1's
#	       reports (fold set = MEMBER only).
#	  L5   read admission (Q9 condition): node1 restarts and rejoins
#	       online.  (a) during the JOINING window its cross-node read of
#	       node0's xids is refused 53R60 (not-member arm) and eventually
#	       succeeds after admission; (b) a REPEATABLE READ snapshot taken
#	       DURING the join window is refused 53R60 (pre-join-snapshot arm)
#	       even after the node is MEMBER -- only a NEW snapshot may
#	       consume (F-D3(3)); local reads stay ungated throughout.
#
# Spec: spec-5.22e-undo-cluster-retention-horizon.md (D5-7, §4.2)
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/370_cluster_5_22e_retention_brake.pl
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

sub state_str
{
	my ($node, $cat, $key) = @_;
	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state WHERE category='$cat' AND key='$key'});
	return defined($v) ? $v : '';
}

sub churn_commits
{
	my ($node, $n) = @_;
	for my $i (1 .. $n)
	{
		$node->safe_psql('postgres', 'INSERT INTO churn VALUES (1)');
	}
}

# Poll until $fn->() is true; returns 1 on success, 0 on deadline.
sub poll_ok
{
	my ($fn, $secs) = @_;
	my $deadline = time() + $secs;
	while (time() < $deadline)
	{
		return 1 if $fn->();
		usleep(500_000);
	}
	return 0;
}

sub arm_conf_injection
{
	my ($node, $spec) = @_;
	$node->safe_psql('postgres', "ALTER SYSTEM SET cluster.injection_points = '$spec'");
	$node->safe_psql('postgres', 'SELECT pg_reload_conf()');
	usleep(1_500_000); # let LMON / cleaner take the SIGHUP
}

sub disarm_conf_injection
{
	my ($node) = @_;
	$node->safe_psql('postgres', 'ALTER SYSTEM RESET cluster.injection_points');
	$node->safe_psql('postgres', 'SELECT pg_reload_conf()');
	usleep(1_500_000);
}

# ============================================================
# Boot (t/369 recipe: shared data + fast liveness + fast cleaner).
# ============================================================
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'spec_5_22e_brake',
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
		'cluster.read_scache = on',
		# fast cleaner passes so the brake/stall/fence legs observe within
		# seconds (default 30s would dominate the test wall clock)
		'cluster.undo_cleaner_interval_ms = 1000',
	]);
$pair->start_pair;
usleep(2_000_000);

ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'boot node0 sees node1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'boot node1 sees node0 connected');

my ($node0, $node1) = ($pair->node0, $pair->node1);

# Phantom-shared subject + churn tables, created before coherence arms
# (t/369: the first local write must form the local pg_undo tree).
for my $n ($node0, $node1)
{
	$n->safe_psql('postgres', 'CREATE TABLE s_t (id int, v int)');
	$n->safe_psql('postgres', 'CREATE TABLE churn (i int)');
}
is( $node0->safe_psql('postgres', q{SELECT pg_relation_filepath('s_t')}),
	$node1->safe_psql('postgres', q{SELECT pg_relation_filepath('s_t')}),
	'boot s_t relfilepath coincidence holds (phantom-shared)');

# Arm the data plane + cross-node visibility on both nodes (live-owner
# consumption legs need them; the brake itself is armed by membership).
for my $n ($node0, $node1)
{
	$n->safe_psql('postgres', 'ALTER SYSTEM SET cluster.undo_gcs_coherence = on');
	$n->safe_psql('postgres',
		'ALTER SYSTEM SET cluster.crossnode_runtime_visibility = on');
	$n->safe_psql('postgres', 'SELECT pg_reload_conf()');
}
usleep(1_000_000);

# node0 commits the cross-node subject rows AFTER coherence armed, so the
# durable TT stamps land on the shared root (t/369 substrate).
$node0->safe_psql('postgres',
	'INSERT INTO s_t SELECT g, g * 10 FROM generate_series(1, 8) g');

# ============================================================
# L0: report plumbing both ways (valid + current-connection capability).
# ============================================================
ok( poll_ok(
		sub { state_str($node0, 'undo', 'horizon_peer_reports') =~ /n1:v=1,cap=1/ }, 30),
	'L0 node0 holds a valid, capability-backed report from node1')
  or diag('node0 peer reports: ' . state_str($node0, 'undo', 'horizon_peer_reports'));
ok( poll_ok(
		sub { state_str($node1, 'undo', 'horizon_peer_reports') =~ /n0:v=1,cap=1/ }, 30),
	'L0 node1 holds a valid, capability-backed report from node0');

# ============================================================
# L1: brake pin / release.
# ============================================================
{
	# settle: floor proven at least once
	ok( poll_ok(sub { state_val($node0, 'undo', 'horizon_last_floor_scn') > 0 }, 20),
		'L1 node0 proved a cluster floor');

	# node1 pins a REPEATABLE READ snapshot (published read_scn floors its
	# reports); the first query fixes the snapshot.
	my $pin = $node1->background_psql('postgres');
	$pin->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
	$pin->query_safe('SELECT count(*) FROM churn');

	# give the pin one report round to reach node0
	usleep(3_000_000);
	my $floor_pinned = state_val($node0, 'undo', 'horizon_last_floor_scn');
	my $gcd_pinned   = state_val($node0, 'undo', 'cleaner_shmem_tt_slots_gcd');

	# churn: committed xacts whose commit_scn now sits ABOVE node1's pin,
	# so the cleaner must NOT recycle their TT slots while the pin holds.
	churn_commits($node0, 12);
	usleep(4_000_000); # >= a few cleaner passes + report rounds

	my $floor_held = state_val($node0, 'undo', 'horizon_last_floor_scn');
	my $gcd_held   = state_val($node0, 'undo', 'cleaner_shmem_tt_slots_gcd');
	cmp_ok($floor_held, '<=', $floor_pinned + 0,
		'L1 HARD ASSERT: proven floor froze at the pinned read_scn (no advance past the pin)');
	is($gcd_held, $gcd_pinned,
		'L1 HARD ASSERT: churn TT slots stayed unrecycled while the peer pin held');

	# the pinned reader's cross-node consumption keeps succeeding
	is($pin->query_safe('SELECT count(*) FROM s_t'), '8',
		'L1 pinned RR snapshot still reads all 8 cross-node rows (no 53R97 tail-lash)');

	$pin->query_safe('COMMIT');
	$pin->quit;

	# release: floor advances past the pin and the held slots recycle
	ok( poll_ok(
			sub {
				state_val($node0, 'undo', 'horizon_last_floor_scn') > $floor_held
				  && state_val($node0, 'undo', 'cleaner_shmem_tt_slots_gcd') > $gcd_held;
			},
			30),
		'L1 release: floor advanced past the pin and the held-back slots recycled')
	  or diag(sprintf('floor %s -> %s, gcd %s -> %s',
			$floor_held, state_val($node0, 'undo', 'horizon_last_floor_scn'),
			$gcd_held,   state_val($node0, 'undo', 'cleaner_shmem_tt_slots_gcd')));
}

# ============================================================
# L2: stall + self-heal (report-drop injection; LMON side => conf face).
# ============================================================
{
	my $stall_pre = state_val($node0, 'undo', 'horizon_stall_count');

	arm_conf_injection($node0, 'cluster-undo-horizon-report-drop:skip');

	# node1's slot ages out after 3 x max(interval) = ~3s; cleaner passes
	# every 1s, so stalls accumulate.
	ok( poll_ok(
			sub { state_val($node0, 'undo', 'horizon_stall_count') > $stall_pre }, 30),
		'L2 HARD ASSERT: undo_horizon_stall_count moved under the report-drop injection');
	ok( poll_ok(
			sub {
				my $log = PostgreSQL::Test::Utils::slurp_file($node0->logfile);
				$log =~ /recycle stalled, cluster horizon unproven/;
			},
			10),
		'L2 the stall LOG-once fired with attribution');

	# recycling is paused: churn slots pile up un-GCed
	my $gcd_stalled = state_val($node0, 'undo', 'cleaner_shmem_tt_slots_gcd');
	churn_commits($node0, 6);
	usleep(3_000_000);
	is(state_val($node0, 'undo', 'cleaner_shmem_tt_slots_gcd'),
		$gcd_stalled, 'L2 recycling paused while stalled (no local fallback, Q3\'\')');

	disarm_conf_injection($node0);

	# self-heal: fresh reports land, the floor advances, recycling resumes
	ok( poll_ok(
			sub { state_val($node0, 'undo', 'cleaner_shmem_tt_slots_gcd') > $gcd_stalled },
			30),
		'L2 self-heal: reports resumed and the held slots recycled');
}

# ============================================================
# L6: F-D2 epoch fence (cleaner side => conf face).
# ============================================================
{
	my $abort_pre = state_val($node0, 'undo', 'horizon_pass_abort_count');
	my $gcd_pre   = state_val($node0, 'undo', 'cleaner_shmem_tt_slots_gcd');

	arm_conf_injection($node0, 'cluster-undo-horizon-epoch-fence:skip');
	churn_commits($node0, 6);

	ok( poll_ok(
			sub { state_val($node0, 'undo', 'horizon_pass_abort_count') > $abort_pre }, 30),
		'L6 HARD ASSERT: undo_horizon_pass_abort_count moved (fence tripped, pass aborted)');
	is(state_val($node0, 'undo', 'cleaner_shmem_tt_slots_gcd'),
		$gcd_pre, 'L6 the aborted passes recycled nothing (mutation refused at the fence)');

	disarm_conf_injection($node0);
	ok( poll_ok(
			sub { state_val($node0, 'undo', 'cleaner_shmem_tt_slots_gcd') > $gcd_pre }, 30),
		'L6 disarm: recycling resumed past the fence');
}

# ============================================================
# L3: member-drop self-heal (fold set = MEMBER only).
# ============================================================
{
	$node1->stop('immediate');

	# node0 dead-decides node1 (heartbeat 2s x deadband 5 = ~10s) and the
	# reconfig drops it from the required set; the floor then advances
	# again WITHOUT node1's reports.
	churn_commits($node0, 4);
	my $floor_pre = state_val($node0, 'undo', 'horizon_last_floor_scn');
	ok( poll_ok(
			sub { state_val($node0, 'undo', 'horizon_last_floor_scn') > $floor_pre }, 60),
		'L3 self-heal: floor advances again after the dead member left the required set');
}

# ============================================================
# L5: read admission across the online rejoin (Q9 condition).
# ============================================================
{
	$node1->start;

	# (a) not-member arm: while node1 is JOINING its cross-node read of
	# node0's committed rows must refuse 53R60; once admitted it succeeds.
	# Local reads stay ungated throughout.
	my $saw_refusal   = 0;
	my $saw_success   = 0;
	my $join_deadline = time() + 60;
	my $joining_snap;
	while (time() < $join_deadline)
	{
		my ($rc, $out, $err) = $node1->psql('postgres', 'SELECT count(*) FROM s_t');
		if ($rc != 0 && ($err // '') =~ /not an admitted cluster member/)
		{
			$saw_refusal = 1;

			# (b) seed the pre-join snapshot NOW (inside the join window):
			# a REPEATABLE READ snapshot fixed before admission must stay
			# refused even after the node becomes MEMBER.
			if (!defined $joining_snap)
			{
				$joining_snap = $node1->background_psql('postgres');
				$joining_snap->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
				$joining_snap->query_safe('SELECT count(*) FROM churn'); # local: ungated
			}
		}
		elsif ($rc == 0 && ($out // '') =~ /^\s*8\s*$/m)
		{
			$saw_success = 1;
			last;
		}
		usleep(250_000);
	}
	ok($saw_refusal,
		'L5a JOINING window: cross-node read refused 53R60 (not an admitted member)');
	ok($saw_success, 'L5a post-admission: a NEW snapshot reads all 8 cross-node rows');

  SKIP:
	{
		skip 'join window closed before a pre-join RR snapshot could be seeded', 2
		  unless defined $joining_snap;

		# (b) pre-join snapshot arm: the RR snapshot fixed during JOINING is
		# refused FOREVER for cross-node consumption (F-D3(3)) even though
		# the node is MEMBER now...
		my ($rc, $out, $err) =
		  $node1->psql('postgres', 'SELECT 1'); # keep harness responsive
		my $res = eval { $joining_snap->query('SELECT count(*) FROM s_t'); };
		my $qerr = $@ // '';
		ok( !defined($res) || $res !~ /^\s*8\s*$/m
			  || $qerr =~ /predates this node's cluster admission/,
			'L5b pre-join RR snapshot never consumes cross-node undo (refused, not 8 rows)');

		# ...while its LOCAL reads still work inside the same transaction.
		# (The refusal above may have aborted the txn; a fresh local read on
		# a new snapshot must succeed -- proving the gate is scoped to
		# cross-node consumption, not to the node's reads at large.)
		$joining_snap->quit;
		is($node1->safe_psql('postgres', 'SELECT count(*) FROM s_t'),
			'8', 'L5b a fresh post-admission snapshot consumes cross-node rows fine');
	}
}

done_testing();
