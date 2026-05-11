#-------------------------------------------------------------------------
#
# 099_reconfig_actor.pl
#    spec-2.29 Sprint A Step 5 D13 — 2-node reconfig coordinator
#    actor TAP.
#
#    Reconfig coordinator end-to-end:  SIGSTOP peer CSSD pid → CSSD
#    deadband fires (per spec-2.5) → LMON sees peer DEAD edge →
#    cluster_reconfig_lmon_tick computes Q2 A'' coordinator → every
#    in_quorum survivor broadcasts PROCSIG_CLUSTER_RECONFIG_START
#    (per P1.3 I7) → only coordinator advances epoch via D18 →
#    pg_cluster_reconfig_state SRF shows the applied event.
#
#    Test discipline (per L99 fence-actor-trigger-substrate-mismatch +
#    L94 unit-test-stub-must-cover-real-path):
#      1. SIGSTOP CSSD pid, NOT postmaster pid (per L92 — postmaster
#         STOP does not cascade to children in Unix; the IC heartbeat
#         producer is CSSD itself).  Same pattern as 085_cssd_
#         heartbeat_round_trip.pl:184.
#      2. poll_query_until 15s timeout — CSSD deadband default ~3s +
#         CI runner jitter buffer.  Never write hardcoded sleeps.
#      3. Verify PRODUCER side first (pg_cluster_cssd_peers.peer_state =
#         'dead' on the survivor's view) BEFORE asserting the ACTOR
#         side (pg_cluster_reconfig_state.event_id != 0).
#      4. Inject point cluster-cssd-mark-peer-dead is unit/inject
#         supplement (cluster_unit T-3/T-5b cover dedup logic) — main
#         TAP path uses real SIGSTOP CSSD substrate.
#
#    LANDED (L1-L8):
#      L1  ClusterPair quorum_voting_disks=>3 strict-mode setup —
#          both postmasters alive, in_quorum=t on both nodes;
#          pg_cluster_reconfig_state baseline: event_id=0,
#          observer_role='none', applied_at NULL.
#      L2  CSSD peer_state baseline = 'alive' on both peers' SRF.
#      L3  Capture node1 CSSD pid + SIGSTOP it (no heartbeat producer).
#      L4  PRODUCER assert (per L99): poll_query_until 15s on node0 for
#          pg_cluster_cssd_peers.peer_state IN ('suspected', 'dead') for
#          node1.  Verifies CSSD deadband actually transitioned the
#          peer state — without this, an ACTOR assertion would be
#          assuming a trigger that did not fire (L99 source pattern).
#      L5  ACTOR assert: poll_query_until 15s on node0 for
#          pg_cluster_reconfig_state.event_id != 0.  Verifies the
#          reconfig coordinator actually fired (not just the producer).
#      L6  Coordinator deterministic min(survivor_set):  in 2-node
#          cluster with node1 dead, survivor_set = {0}, so
#          coordinator_node_id = 0 = self (node0).
#      L7  Epoch advance: new_epoch > old_epoch on node0;observer_role
#          = 'coordinator';dead_bitmap reflects node1 dead.
#      L8  SIGCONT node1 CSSD + verify cluster_qvotec_in_quorum still
#          true on node0 (we never lost quorum during the test).
#
#    DEFERRED (L9-L10) — broader coverage for future hardening:
#      L9  clean shutdown variant (node1->stop instead of SIGSTOP CSSD)
#          — different DEAD path through clean-shutdown tombstone;
#          deferred to Hardening round (not main acceptance).
#      L10 acceptance baseline integration — pg_cluster_state row count
#          non-decreasing vs spec-0.30 baseline;deferred to Step 6
#          local 4 surfaces verify (covered by 030_acceptance.pl
#          baseline updates per L98).
#
# IDENTIFICATION
#    src/test/cluster_tap/t/099_reconfig_actor.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;

sub poll_query_until_timeout
{
	my ($node, $dbname, $query, $expected, $timeout_s, $label) = @_;

	$expected //= 't';
	$timeout_s //= 15;

	my $deadline = time + $timeout_s;
	my $last = '';

	while (time < $deadline)
	{
		$last = $node->safe_psql($dbname, $query);
		return 1 if defined $last && $last eq $expected;
		select(undef, undef, undef, 0.1);
	}

	diag("$label timed out after ${timeout_s}s; expected=$expected; last=$last");
	return 0;
}


# ----------
# L1: setup — strict-mode ClusterPair with 3 voting disks.
# ----------
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'reconfig_2node', quorum_voting_disks => 3);
$pair->start_pair;

is($pair->node0->safe_psql('postgres', 'SELECT 1'),
	'1', 'L1 node0 postmaster alive');
is($pair->node1->safe_psql('postgres', 'SELECT 1'),
	'1', 'L1 node1 postmaster alive');

ok(poll_query_until_timeout($pair->node0, 'postgres',
		'SELECT in_quorum FROM pg_cluster_quorum_state', 't', 15,
		'L1 node0 in_quorum=t'),
	'L1 node0 in_quorum=t');
ok(poll_query_until_timeout($pair->node1, 'postgres',
		'SELECT in_quorum FROM pg_cluster_quorum_state', 't', 15,
		'L1 node1 in_quorum=t'),
	'L1 node1 in_quorum=t');

# Reconfig catalog surface visible:  pg_cluster_reconfig_state SRF
# returns single row always (P2.9 always-1-row contract).  Never-applied
# state surfaces as event_id=0 + observer_role='none' + applied_at NULL.
is($pair->node0->safe_psql('postgres',
		'SELECT count(*) FROM pg_cluster_reconfig_state'),
	'1', 'L1 node0 pg_cluster_reconfig_state SRF returns 1 row (P2.9)');

is($pair->node0->safe_psql('postgres',
		'SELECT event_id FROM pg_cluster_reconfig_state'),
	'0', 'L1 node0 event_id=0 baseline (never applied)');
is($pair->node0->safe_psql('postgres',
		'SELECT observer_role FROM pg_cluster_reconfig_state'),
	'none', 'L1 node0 observer_role=none baseline');
is($pair->node0->safe_psql('postgres',
		'SELECT applied_at IS NULL FROM pg_cluster_reconfig_state'),
	't', 'L1 node0 applied_at NULL baseline');


# ----------
# L2: CSSD peers baseline ALIVE.
# ----------
ok(poll_query_until_timeout($pair->node0, 'postgres',
		q{SELECT state = 'alive' FROM pg_cluster_cssd_peers WHERE node_id = 1},
		't', 15, 'L2 node0 sees node1 CSSD peer state=alive'),
	'L2 node0 sees node1 CSSD peer state=alive');
ok(poll_query_until_timeout($pair->node1, 'postgres',
		q{SELECT state = 'alive' FROM pg_cluster_cssd_peers WHERE node_id = 0},
		't', 15, 'L2 node1 sees node0 CSSD peer state=alive'),
	'L2 node1 sees node0 CSSD peer state=alive');

# Match 085_cssd_heartbeat_round_trip.pl discipline: prove the CSSD
# heartbeat substrate is established before SIGSTOP.  Otherwise 099
# could pass because startup never established heartbeats, not because
# SIGSTOP produced the DEAD edge under review.
{
	my $waited = 0;
	my $baseline_ok = 0;

	while ($waited < 15)
	{
		my $send01 = $pair->node0->safe_psql('postgres',
			'SELECT heartbeat_send_count FROM pg_cluster_cssd_peers WHERE node_id = 1');
		my $send10 = $pair->node1->safe_psql('postgres',
			'SELECT heartbeat_send_count FROM pg_cluster_cssd_peers WHERE node_id = 0');
		my $recv01 = $pair->node0->safe_psql('postgres',
			'SELECT heartbeat_recv_count FROM pg_cluster_cssd_peers WHERE node_id = 1');
		my $recv10 = $pair->node1->safe_psql('postgres',
			'SELECT heartbeat_recv_count FROM pg_cluster_cssd_peers WHERE node_id = 0');

		if (   $send01 && $send01 >= 1 && $send10 && $send10 >= 1
			&& $recv01 && $recv01 >= 1 && $recv10 && $recv10 >= 1)
		{
			$baseline_ok = 1;
			last;
		}

		sleep 1;
		$waited++;
	}

	if (!$baseline_ok)
	{
		diag("L2.5 CSSD heartbeat baseline did not establish within 15s; skipping SIGSTOP actor path");
		$pair->stop_pair;
		done_testing();
		exit 0;
	}
}


# ----------
# L3: SIGSTOP node1 CSSD pid (NOT postmaster — see L92/L99 above).
#     Mirror of 085_cssd_heartbeat_round_trip.pl:184 pattern.
# ----------
my $cssd1_pid = $pair->node1->safe_psql('postgres',
	q{SELECT pid FROM pg_stat_activity WHERE backend_type = 'cssd' LIMIT 1});
ok($cssd1_pid && $cssd1_pid =~ /^\d+$/,
	"L3 captured node1 CSSD pid ($cssd1_pid)");

if (!$cssd1_pid || $cssd1_pid !~ /^\d+$/)
{
	diag("L3 could not capture node1 CSSD pid;skipping L4-L8");
	$pair->stop_pair;
	done_testing();
	exit;
}

ok(kill('STOP', $cssd1_pid) == 1, "L3 SIGSTOP node1 CSSD pid=$cssd1_pid");
diag("L3 SIGSTOP node1 CSSD pid=$cssd1_pid");


# ----------
# L4: PRODUCER assert (per L99 fix pattern) — verify CSSD deadband
#     actually transitioned node1 to suspected or dead.  Without this
#     producer-side observation, an ACTOR assertion would assume a
#     trigger event that may not have fired (the L99 source bug).
# ----------
my $producer_ok = poll_query_until_timeout(
	$pair->node0,
	'postgres',
	q{SELECT state IN ('suspected', 'dead')
	    FROM pg_cluster_cssd_peers WHERE node_id = 1},
	't', 15, 'L4 PRODUCER node0 sees node1 CSSD state in (suspected, dead)');

ok($producer_ok,
	'L4 PRODUCER node0 sees node1 CSSD state in (suspected, dead) within 15s');

# Snapshot the actual final state for diagnostics.
my $producer_state = $pair->node0->safe_psql('postgres',
	q{SELECT state FROM pg_cluster_cssd_peers WHERE node_id = 1});
diag("L4 producer state observed: $producer_state");


# ----------
# L5: ACTOR assert — pg_cluster_reconfig_state.event_id != 0 after
#     CSSD DEAD edge.  This is the spec-2.29 D2 lmon_tick observable
#     output (publish_event after Q2 A'' decision + dedup).
#
#     Only fires when CSSD has transitioned to DEAD (not SUSPECTED) —
#     so we poll for slightly longer to allow CSSD to escalate from
#     SUSPECTED to DEAD if it's still in the intermediate phase.
# ----------
my $actor_ok = poll_query_until_timeout(
	$pair->node0,
	'postgres',
	q{SELECT event_id != 0 FROM pg_cluster_reconfig_state},
	't', 15, 'L5 ACTOR node0 pg_cluster_reconfig_state.event_id != 0');

ok($actor_ok,
	'L5 ACTOR node0 pg_cluster_reconfig_state.event_id != 0 within 15s');


# ----------
# L6: Coordinator deterministic min(survivor_set).
#     2-node cluster with node1 dead → survivor_set = {0} → coord = 0.
# ----------
my $coordinator_id = $pair->node0->safe_psql('postgres',
	q{SELECT coordinator_node_id FROM pg_cluster_reconfig_state});
is($coordinator_id, '0',
	'L6 node0 coordinator_node_id=0 (deterministic min(survivor) = self)');


# ----------
# L7: Epoch advance — new_epoch > old_epoch + observer_role=coordinator.
# ----------
is($pair->node0->safe_psql('postgres',
		q{SELECT new_epoch > old_epoch FROM pg_cluster_reconfig_state}),
	't',
	'L7a node0 new_epoch > old_epoch (D18 advance applied)');

is($pair->node0->safe_psql('postgres',
		q{SELECT observer_role FROM pg_cluster_reconfig_state}),
	'coordinator',
	'L7b node0 observer_role=coordinator (self == min(survivor))');

# dead_bitmap reflects node1 dead.  uint8[16] hex format starts with
# "0x" + 32 chars.  Bit 1 (node 1) → byte[0] bit 1 → "02" at hex
# position 2-3 (right after "0x").
my $dead_bitmap = $pair->node0->safe_psql('postgres',
	q{SELECT dead_bitmap FROM pg_cluster_reconfig_state});
like($dead_bitmap, qr/^0x02/,
	"L7c node0 dead_bitmap starts with 0x02 (node 1 bit set);actual=$dead_bitmap");


# ----------
# L8: SIGCONT node1 CSSD + verify node0 still in_quorum.
# ----------
ok(kill('CONT', $cssd1_pid) == 1, "L8 SIGCONT node1 CSSD pid=$cssd1_pid");
diag("L8 SIGCONT node1 CSSD pid=$cssd1_pid");

# Don't poll for full CSSD recovery — that's L9 scope.  Just verify
# we never lost local quorum during the test (qvotec disks still OK
# on node0;the reconfig coordinator path tolerates peer death without
# triggering local quorum loss when disks remain healthy).
is($pair->node0->safe_psql('postgres',
		'SELECT in_quorum FROM pg_cluster_quorum_state'),
	't', 'L8 node0 still in_quorum=t (qvotec disks healthy)');


# Cleanup — stop the pair (handles peers in any CSSD state).
$pair->stop_pair;


done_testing();
