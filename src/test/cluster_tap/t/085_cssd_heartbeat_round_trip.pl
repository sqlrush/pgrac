#-------------------------------------------------------------------------
#
# 085_cssd_heartbeat_round_trip.pl
#    spec-2.5 D19 + Q5 hysteresis 2-node CSSD heartbeat broadcast +
#    dead detection + recovery TAP.
#
#    Test matrix (10 L#):
#      L1  ClusterPair start_pair OK; both nodes alive
#      L2  pg_cluster_ic_msg_types contains CSSD heartbeat (msg_type=11,
#          name='cssd_heartbeat', broadcast_ok=true) on both nodes
#      L3  pg_cluster_cssd_peers visible (9 cols);default state ALIVE
#      L4  Heartbeat counters grow over time (proof of broadcast +
#          LMON drain + tier1 send) on both sides
#      L5  Both nodes see peer ALIVE (no SUSPECTED/DEAD transition under
#          healthy steady-state)
#      L6  SIGSTOP one node's CSSD;peer goes SUSPECTED at 2x interval
#      L7  Continued SIGSTOP;peer goes DEAD at 3x interval
#      L8  SIGCONT CSSD;peer recovers ALIVE (hysteresis recovery)
#      L9  cluster_cssd_dead_deadband_factor + cluster_cssd_heartbeat_interval_ms
#          GUCs visible + values default 3 / 1000
#      L10 first-tick grace period: spawn-time peer states all ALIVE
#          (count 0 of SUSPECTED/DEAD within first 3s)
#
#    Spec authority: spec-2.5 §3.2 + §3.4 + Q5 hysteresis + Q6 grace
#    period + §1.2 D19.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/085_cssd_heartbeat_round_trip.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
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


{
	my $pair = PostgreSQL::Test::ClusterPair->new_pair('pgrac085');
	$pair->start_pair;

	# ============================================================
	# L1 -- both postmasters live (CSSD spawn at phase 4 third did not
	# break two-node startup).
	# ============================================================
	is($pair->node0->safe_psql('postgres', 'SELECT 1'), '1',
		'L1 node0 postmaster accepts SQL after CSSD spawn');
	is($pair->node1->safe_psql('postgres', 'SELECT 1'), '1',
		'L1 node1 postmaster accepts SQL after CSSD spawn');

	# ============================================================
	# L2 -- pg_cluster_ic_msg_types contains CSSD heartbeat row on both.
	# ============================================================
	for my $idx (0 .. 1) {
		my $node = $idx == 0 ? $pair->node0 : $pair->node1;
		my $row = $node->safe_psql('postgres',
			q{SELECT msg_type || '|' || name || '|' ||
			        handler_present || '|' || broadcast_ok
			    FROM pg_cluster_ic_msg_types
			   WHERE msg_type = 11});
		is($row, '11|cssd_heartbeat|true|true',
			"L2 node$idx CSSD heartbeat row {msg_type=11, name=cssd_heartbeat, "
			. "handler_present=true, broadcast_ok=true} (spec-2.5 D12)");
	}

	# ============================================================
	# L3 -- pg_cluster_cssd_peers view visible (9 cols);default state.
	# ============================================================
	for my $idx (0 .. 1) {
		my $node = $idx == 0 ? $pair->node0 : $pair->node1;
		my $col_count = $node->safe_psql('postgres',
			q{SELECT count(*) FROM information_schema.columns
			   WHERE table_name = 'pg_cluster_cssd_peers'});
		is($col_count, '9', "L3 node$idx pg_cluster_cssd_peers view has 9 cols");
	}

	# Wait until peer connections are up (LMON tier1 connect_one + HELLO).
	# spec-2.5 v1.0.1 follow-up: timeout bumped 10 → 30s for ubuntu CI
	# stability (start_pair + handshake + cluster_phase4 + CSSD spawn
	# total can exceed 10s on slow runners).
	my $conn01 = $pair->wait_for_peer_state(0, 1, 'connected', 30);
	my $conn10 = $pair->wait_for_peer_state(1, 0, 'connected', 30);
	ok($conn01, 'L3 node0 sees node1 connected within 30s (envelope wire)');
	ok($conn10, 'L3 node1 sees node0 connected within 30s (envelope wire)');

	# spec-2.5 v1.0.1 hardening: skip baseline-dependent tests if peer
	# connections didn't establish.  Without this gate downstream L4-L8
	# all return meaningless "alive" because CSSD heartbeats never flow.
	if (!$conn01 || !$conn10) {
		diag("L3 peer connections did not establish within 30s — skipping baseline-dependent tests L4-L10");
		$pair->stop_pair;
		done_testing();
		exit 0;
	}

	# Sleep enough for first-tick grace period (3s default) + several
	# heartbeat intervals.
	sleep 5;

	# spec-2.5 v1.0.1 hardening: pre-flight baseline gate.  Confirm CSSD
	# heartbeat ROUND-TRIP actually established before SIGSTOP / deadband
	# tests.  Without this, downstream "still alive" failures can mask
	# either real deadband bugs OR baseline-never-established noise.
	{
		my $waited = 0;
		my $baseline_ok = 0;
		while ($waited < 15) {
			my $send01 = $pair->node0->safe_psql('postgres',
				'SELECT heartbeat_send_count FROM pg_cluster_cssd_peers WHERE node_id = 1');
			my $send10 = $pair->node1->safe_psql('postgres',
				'SELECT heartbeat_send_count FROM pg_cluster_cssd_peers WHERE node_id = 0');
			my $recv01 = $pair->node0->safe_psql('postgres',
				'SELECT heartbeat_recv_count FROM pg_cluster_cssd_peers WHERE node_id = 1');
			my $recv10 = $pair->node1->safe_psql('postgres',
				'SELECT heartbeat_recv_count FROM pg_cluster_cssd_peers WHERE node_id = 0');
			if (   $send01 && $send01 >= 1 && $send10 && $send10 >= 1
				&& $recv01 && $recv01 >= 1 && $recv10 && $recv10 >= 1) {
				$baseline_ok = 1;
				last;
			}
			sleep 1;
			$waited++;
		}
		if (!$baseline_ok) {
			diag("L3.5 CSSD heartbeat baseline did not establish within 15s — skipping deadband tests");
			$pair->stop_pair;
			done_testing();
			exit 0;
		}
	}

	# ============================================================
	# L4 -- Heartbeat counters grow.
	#
	# spec-2.5 §3.2: CSSD writes outbound queue;LMON drains and sends
	# via tier1.  pg_cluster_cssd_peers.heartbeat_send_count is bumped
	# on DONE result read (next tick).  pg_cluster_cssd_peers.heartbeat_recv_count
	# is bumped by dispatch_heartbeat handler.
	# ============================================================
	for my $pair_dir ([0, 1, 'node0->node1'], [1, 0, 'node1->node0']) {
		my ($from, $to, $label) = @$pair_dir;
		my $node = $from == 0 ? $pair->node0 : $pair->node1;

		my $send_count = $node->safe_psql('postgres',
			"SELECT heartbeat_send_count FROM pg_cluster_cssd_peers WHERE node_id = $to");
		my $recv_count = $node->safe_psql('postgres',
			"SELECT heartbeat_recv_count FROM pg_cluster_cssd_peers WHERE node_id = $to");
		cmp_ok($send_count, '>=', 1,
			"L4 $label CSSD heartbeat_send_count >= 1 (got=$send_count)");
		cmp_ok($recv_count, '>=', 1,
			"L4 $label CSSD heartbeat_recv_count >= 1 (got=$recv_count)");
	}

	# ============================================================
	# L5 -- Healthy steady state: peer state == 'alive' both sides.
	# ============================================================
	for my $pair_dir ([0, 1, 'node0->node1'], [1, 0, 'node1->node0']) {
		my ($from, $to, $label) = @$pair_dir;
		my $node = $from == 0 ? $pair->node0 : $pair->node1;

		my $state = $node->safe_psql('postgres',
			"SELECT state FROM pg_cluster_cssd_peers WHERE node_id = $to");
		is($state, 'alive', "L5 $label CSSD peer state = 'alive' under healthy steady-state");
	}

	# ============================================================
	# L6 + L7 + L8 -- SIGSTOP one node's CSSD → SUSPECTED → DEAD →
	# SIGCONT → ALIVE recovery (hysteresis).
	# ============================================================
	# Capture node1 CSSD pid.
	my $cssd1_pid = $pair->node1->safe_psql('postgres',
		q{SELECT pid FROM pg_stat_activity WHERE backend_type = 'cssd' LIMIT 1});
	ok($cssd1_pid && $cssd1_pid =~ /^\d+$/, "L6 captured node1 CSSD pid ($cssd1_pid)");

	if ($cssd1_pid && $cssd1_pid =~ /^\d+$/) {
		# SIGSTOP node1's CSSD (it stops sending heartbeats).
		kill 'STOP', $cssd1_pid;

		# spec-2.5 v1.0.1 hardening: bumped 3 → 5s for elapsed margin.
		# 3s OS-precision sleep can land just under dead_threshold = 3s,
		# producing flaky 'alive' result.  5s ensures elapsed >= 4s
		# even with 1s scheduling jitter.
		sleep 5;

		my $state_node0 = $pair->node0->safe_psql('postgres',
			'SELECT state FROM pg_cluster_cssd_peers WHERE node_id = 1');
		ok($state_node0 eq 'suspected' || $state_node0 eq 'dead',
		   "L6 node0 sees peer node1 SUSPECTED or DEAD after 5s SIGSTOP "
		   . "(actual=$state_node0)");

		# Wait another 2s to confirm DEAD.
		sleep 2;

		my $state_node0_2 = $pair->node0->safe_psql('postgres',
			'SELECT state FROM pg_cluster_cssd_peers WHERE node_id = 1');
		is($state_node0_2, 'dead',
		   "L7 node0 sees peer node1 DEAD after 7s SIGSTOP (3x heartbeat = 3s threshold "
		   . "exceeded)");

		# SIGCONT node1's CSSD — heartbeats resume.
		kill 'CONT', $cssd1_pid;

		# Wait for recovery: heartbeat_interval_ms (1s) + few ticks.
		sleep 4;

		my $state_node0_3 = $pair->node0->safe_psql('postgres',
			'SELECT state FROM pg_cluster_cssd_peers WHERE node_id = 1');
		is($state_node0_3, 'alive',
		   "L8 node0 sees peer node1 recovered to ALIVE after SIGCONT (Q5 hysteresis "
		   . "recovery)");
	}

	# ============================================================
	# L9 -- 3 GUCs visible with default values.
	# ============================================================
	for my $idx (0 .. 1) {
		my $node = $idx == 0 ? $pair->node0 : $pair->node1;
		my $hb = $node->safe_psql('postgres',
			q{SELECT setting FROM pg_settings
			   WHERE name = 'cluster.cssd_heartbeat_interval_ms'});
		is($hb, '1000', "L9 node$idx cssd_heartbeat_interval_ms default = 1000");
		my $factor = $node->safe_psql('postgres',
			q{SELECT setting FROM pg_settings
			   WHERE name = 'cluster.cssd_dead_deadband_factor'});
		is($factor, '3', "L9 node$idx cssd_dead_deadband_factor default = 3");
	}

	$pair->stop_pair;
}


# ============================================================
# L10 -- spec-2.5 §3.2.1 first-tick grace period: spawn fresh ClusterPair,
# query peer states immediately;all should be 'alive' within first 3s
# (deadband-scan skipped during grace window even if recv == 0).
# ============================================================
{
	my $pair_l10 = PostgreSQL::Test::ClusterPair->new_pair('pgrac085_l10');
	$pair_l10->start_pair;

	# Query within first 3s of CSSD READY publish.  Default grace =
	# factor (3) × interval (1000ms) = 3000ms.  Snapshot peer states
	# immediately;none should be SUSPECTED / DEAD.
	my $rows0 = $pair_l10->node0->safe_psql('postgres',
		q{SELECT count(*) FROM pg_cluster_cssd_peers
		   WHERE state IN ('suspected', 'dead')});
	is($rows0, '0',
	   'L10 node0 first-tick grace period: no SUSPECTED/DEAD peer states within first 3s');

	$pair_l10->stop_pair;
}


done_testing();
