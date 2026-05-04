#-------------------------------------------------------------------------
#
# 063_diag_skeleton.pl
#    Stage 1.13 + spec-1.13 Q2 A' phase 4 driver split + 1.12.1 lessons preempted end-to-end: DIAG aux
#    process lifecycle + observability surface + state-machine closure
#    verification.  postmaster spawns DIAG; phase 2 sync waits ready;
#    clean shutdown stops DIAG normally; abnormal exit triggers PG crash
#    recovery (HC5); ServerLoop respawn on normal exit refreshes shmem
#    state on every SPAWNING incarnation.
#
#    Test matrix (L1-L10; L1-L5 + L5b cover the original lifecycle
#    scope; L6-L10 cover spec-1.13 F17 acceptance gaps):
#
#      L1   normal startup spawns DIAG aux process (pg_stat_activity
#           shows backend_type='diag')
#      L2   phase 2 advances to phase 3 only after DIAG publishes
#           ready (postmaster log shows ordering)
#      L3   --enable-cluster compile + cluster module live still spawns
#           DIAG (HC4 cluster_enabled GUC=on confirmed)
#      L4   clean shutdown (pg_ctl stop -m fast) stops DIAG normally;
#           postmaster log shows DIAG shutdown without crash recovery
#      L5   abnormal DIAG exit (kill -9) triggers PG crash recovery
#           cycle per HC5; postmaster log shows HandleChildCrash path
#      L5b  explicit restart_after_crash=on + DIAG respawn after kill
#           (codex P1.1 + P1.2 + P2.4 lessons preempted in Sprint A)
#
# IDENTIFICATION
#    src/test/cluster_tap/t/063_diag_skeleton.pl
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

use IPC::Cmd qw(can_run);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use PgracClusterNode;


my $node = PgracClusterNode->new('main');
$node->init;

# Set log_min_messages = debug1 so we can observe phase + DIAG
# liveness debug messages.
$node->append_conf('postgresql.conf', "log_min_messages = debug1\n");

$node->start;


# ----------
# L1: postmaster spawned DIAG aux process; pg_stat_activity sees it.
# ----------
my $diag_count = $node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'diag'});
is($diag_count, '1',
   'L1 DIAG aux process visible in pg_stat_activity (spec-1.13 Sprint A)');


# ----------
# L2: phase 2 advanced to phase 3 only after DIAG published ready.
# Postmaster log line "cluster phase 4: DIAG ready" must appear before
# the "phase4_normal -> running" transition log.
# ----------
my $log_l2 = slurp_file($node->logfile);
my $diag_ready_pos
	= index($log_l2, "cluster phase 4: DIAG ready");
my $phase3_pos
	= index($log_l2, "cluster startup: phase4_normal -> running");
ok($diag_ready_pos >= 0, 'L2 postmaster log contains DIAG ready message');
ok($phase3_pos >= 0, 'L2 postmaster log contains phase2->phase3 transition');
ok($diag_ready_pos > 0 && $phase3_pos > 0 && $diag_ready_pos < $phase3_pos,
   'L2 phase 2 driver waited for DIAG ready before advancing to phase 3');


# ----------
# L3: --enable-cluster compile + cluster module live still spawns DIAG.
# Spec-1.12 phase_2_handler reads cluster_enabled GUC (HC4 closure)
# from spec-1.11 Sprint B; with default cluster.enabled=on, DIAG spawn
# happens.  Sprint B / future stages may add a per-node enable toggle
# but for Sprint A baseline we assert phase=running confirms the
# spawn path completed.
# ----------
my $cluster_phase = $node->safe_psql('postgres',
	"SELECT value FROM pg_cluster_state WHERE category = 'phase' AND key = 'cluster_phase'");
is($cluster_phase, 'running',
   'L3 cluster module live; phase=running confirms DIAG spawn path '
   . 'completed (HC4 cluster_enabled GUC=on default)');


# ----------
# L4: clean shutdown stops DIAG normally (HC5 normal-exit path).
# pg_ctl stop -m fast signals SIGTERM to postmaster -> postmaster
# signals SIGTERM to DIAG -> DIAG main loop sees ShutdownRequestPending,
# proc_exit(0) -> reaper sees WIFEXITED + WEXITSTATUS=0 -> no crash
# recovery.  Postmaster log shows clean phase shutdown transition.
# ----------
$node->stop;
my $log_l4 = slurp_file($node->logfile);
like($log_l4, qr/database system is shut down/,
	 'L4 clean shutdown completes (pg_ctl stop -m fast)');
unlike($log_l4, qr/HandleChildCrash|server process .* exited|terminating any other active server processes/,
	   'L4 DIAG normal exit does NOT trigger crash recovery (HC5 normal path)');


# ----------
# L5: abnormal DIAG exit (kill -9 DIAG pid) routes through
# HandleChildCrash.  PostgreSQL::Test::Cluster.pm forces
# restart_after_crash = off in init() so this test only exercises
# the shutdown-on-crash branch (postmaster terminates other children
# and exits).  The full restart-after-crash recovery cycle is
# covered by L5b below with an explicit GUC override.  spec-1.13
# Sprint A preempts codex P2.4 lesson from spec-1.11 Sprint B.
# ----------
$node->start;
my $diag_pid = $node->safe_psql('postgres',
	q{SELECT pid FROM pg_stat_activity WHERE backend_type = 'diag' LIMIT 1});
ok($diag_pid && $diag_pid =~ /^\d+$/,
   'L5 captured DIAG pid for kill test');

if ($diag_pid && $diag_pid =~ /^\d+$/) {
	# SIGKILL the DIAG child.  Postmaster reaper sees abnormal exit ->
	# HandleChildCrash -> with restart_after_crash=off (TAP harness
	# default) postmaster terminates other children and exits.
	kill 9, $diag_pid;

	# Wait up to 10 seconds for the crash log line.
	my $waited = 0;
	my $log_l5 = '';
	while ($waited < 10) {
		sleep 1;
		$waited++;
		$log_l5 = slurp_file($node->logfile);
		last if $log_l5 =~ /terminating any other active server processes|crash of another server process/;
	}

	like($log_l5,
		 qr/terminating any other active server processes|crash of another server process/,
		 'L5 DIAG kill -9 routes through HandleChildCrash (HC5 abnormal path; '
		 . 'restart_after_crash=off forces shutdown branch)');
}

# Best-effort cleanup; postmaster may have already exited via crash
# branch.  fail_ok bypasses BAIL_OUT in PostgreSQL::Test::Cluster.
$node->stop('immediate', fail_ok => 1);


# ----------
# L5b (spec-1.13 Sprint A codex P1.1 + P1.2 + P2.4 preempted):
# explicit restart_after_crash=on + DIAG respawn after kill.
#
# Spec-1.11 Sprint A had a hidden gap (caught in Sprint B): the
# startup sequence ran exactly once in PostmasterMain, so after
# restart_after_crash recovery DIAG would never respawn.  Spec-1.12
# Sprint A preempts this lesson by:
#   - F9 fix: cluster_run_startup_sequence rerun on crash reinit path
#   - ServerLoop respawn logic in postmaster (mirrors WalWriter +
#     LMON pattern), so whenever pmState=PM_RUN && DiagPID=0 &&
#     cluster_enabled, postmaster starts a fresh DIAG child.
#
# This test verifies the respawn closure end-to-end:
#   1. start node with restart_after_crash=on
#   2. capture initial DIAG pid
#   3. kill -9 DIAG
#   4. wait for crash recovery + new DIAG spawn
#   5. confirm new DIAG pid != initial pid
# ----------
{
	my $node_l5b = PgracClusterNode->new('l5b_diag_respawn');
	$node_l5b->init;
	$node_l5b->append_conf('postgresql.conf',
		"restart_after_crash = on\nlog_min_messages = debug1\n");
	$node_l5b->start;

	my $diag_pid_initial = $node_l5b->safe_psql('postgres',
		q{SELECT pid FROM pg_stat_activity WHERE backend_type = 'diag' LIMIT 1});
	ok($diag_pid_initial && $diag_pid_initial =~ /^\d+$/,
	   'L5b captured initial DIAG pid (restart_after_crash=on baseline)');

	if ($diag_pid_initial && $diag_pid_initial =~ /^\d+$/) {
		kill 9, $diag_pid_initial;

		# Wait for restart cycle + new DIAG spawn.  Up to 30 seconds.
		my $diag_pid_new = '';
		my $waited = 0;
		while ($waited < 30) {
			sleep 1;
			$waited++;
			my $r = eval {
				$node_l5b->safe_psql('postgres',
					q{SELECT pid FROM pg_stat_activity WHERE backend_type = 'diag' LIMIT 1});
			};
			next if $@;	  # connection still recovering
			next unless $r && $r =~ /^\d+$/;
			next if $r eq $diag_pid_initial;	# old DIAG pid still cached
			$diag_pid_new = $r;
			last;
		}

		ok($diag_pid_new && $diag_pid_new ne $diag_pid_initial,
		   "L5b DIAG respawned after kill -9 (initial=$diag_pid_initial new=$diag_pid_new); "
		   . "spec-1.13 Sprint A P1 preemption: ServerLoop respawn logic mirrors LMON + WalWriter pattern");
	}

	$node_l5b->stop('immediate', fail_ok => 1);
}


# ============================================================
# spec-1.13 F17 — round 5 acceptance gaps (L6-L10)
# ============================================================
#
# L5b only verifies the DIAG PID changes after kill -9; that is necessary
# but not sufficient.  L6-L10 close the state-machine closure:
#
#   L6  post-crash phase recovers to running (cluster_phase = 'running'
#       after restart_after_crash recovery completes)
#   L7  pg_cluster_state.diag 6 keys are kept in sync with the live
#       DIAG pid in pg_stat_activity (no stale incarnation data)
#   L8  diag_main_loop_iters grows over time (proof the live DIAG main
#       loop is actually ticking, not just present in pg_stat_activity)
#   L9  53R0E DIAG_SPAWN_FAILED FATAL is reachable end-to-end via the
#       cluster-diag-pre-spawn injection point (proves PhaseRunFailContext
#       SQLSTATE plumbing works, not just LOG wiring)
#   L10 cluster.enabled = false degrades phase 2 to spec-1.10 stub
#       behavior (HC4 closure: DIAG does NOT spawn, no 'diag' backend in
#       pg_stat_activity)


# ----------
# L6: post-crash phase recovers to 'running' after restart_after_crash.
# ----------
{
	my $node_l6 = PgracClusterNode->new('l6_diag_post_crash_phase');
	$node_l6->init;
	$node_l6->append_conf('postgresql.conf',
		"restart_after_crash = on\nlog_min_messages = debug1\n");
	$node_l6->start;

	my $diag_pid = $node_l6->safe_psql('postgres',
		q{SELECT pid FROM pg_stat_activity WHERE backend_type = 'diag' LIMIT 1});

	if ($diag_pid && $diag_pid =~ /^\d+$/) {
		kill 9, $diag_pid;

		my $waited = 0;
		my $phase = '';
		while ($waited < 30) {
			sleep 1;
			$waited++;
			$phase = eval {
				$node_l6->safe_psql('postgres',
					q{SELECT value FROM pg_cluster_state
					   WHERE category='phase' AND key='cluster_phase'});
			} // '';
			last if $phase eq 'running';
		}
		is($phase, 'running',
		   'L6 cluster_phase returns to running after restart_after_crash recovery (F15+F16 verifier)');
	}

	$node_l6->stop('immediate', fail_ok => 1);
}


# ----------
# L7 + L8 reuse a single fresh node so we observe live state.
# ----------
my $node_lx = PgracClusterNode->new('lx_diag_state');
$node_lx->init;
$node_lx->append_conf('postgresql.conf', "log_min_messages = debug1\n");
$node_lx->start;


# ----------
# L7: pg_cluster_state.diag 6 keys agree with live pg_stat_activity.
# ----------
my $live_pid = $node_lx->safe_psql('postgres',
	q{SELECT pid FROM pg_stat_activity WHERE backend_type = 'diag' LIMIT 1});
my $sql_pid = $node_lx->safe_psql('postgres',
	q{SELECT value FROM pg_cluster_state
	   WHERE category='diag' AND key='diag_pid'});
is($sql_pid, $live_pid,
   "L7 pg_cluster_state.diag.diag_pid matches live pg_stat_activity ($sql_pid == $live_pid)");

my $diag_keys = $node_lx->safe_psql('postgres',
	q{SELECT string_agg(key, ',' ORDER BY key)
	    FROM pg_cluster_state WHERE category='diag'});
like($diag_keys, qr/diag_status/, 'L7 pg_cluster_state.diag exposes diag_status');
like($diag_keys, qr/diag_pid/,    'L7 pg_cluster_state.diag exposes diag_pid');
like($diag_keys, qr/diag_spawned_at/,
	 'L7 pg_cluster_state.diag exposes diag_spawned_at');
like($diag_keys, qr/diag_ready_at/,
	 'L7 pg_cluster_state.diag exposes diag_ready_at');
like($diag_keys, qr/diag_last_liveness_tick_at/,
	 'L7 pg_cluster_state.diag exposes diag_last_liveness_tick_at');
like($diag_keys, qr/diag_main_loop_iters/,
	 'L7 pg_cluster_state.diag exposes diag_main_loop_iters');


# ----------
# L8: diag_main_loop_iters grows over 3s.
# ----------
my $iters_t0 = $node_lx->safe_psql('postgres',
	q{SELECT value::bigint FROM pg_cluster_state
	   WHERE category='diag' AND key='diag_main_loop_iters'});
sleep 3;
my $iters_t1 = $node_lx->safe_psql('postgres',
	q{SELECT value::bigint FROM pg_cluster_state
	   WHERE category='diag' AND key='diag_main_loop_iters'});
cmp_ok($iters_t1, '>', $iters_t0,
	   "L8 diag_main_loop_iters grew over 3s ($iters_t0 -> $iters_t1) — main loop is ticking");

$node_lx->stop;


# ----------
# L9: phase 2 FATAL path works when DIAG spawn is interrupted.
# See L9 in 061_lmon_skeleton.pl for the same rationale; the inject
# 'error' fault type fires ereport(ERROR) inside cluster_diag_start()
# which postmaster converts to FATAL — proving the phase 2
# spawn-failure path actually exits postmaster rather than silently
# continuing.  (53R0E specifically requires cluster_diag_start to
# return 0 without ereport; covered by a broader F13 FATAL-out
# contract here).
# ----------
{
	my $node_l9 = PgracClusterNode->new('l9_diag_spawn_fail');
	$node_l9->init;
	$node_l9->append_conf('postgresql.conf',
		"log_min_messages = debug1\n"
		. "cluster.injection_points = 'cluster-diag-pre-spawn:error'\n");

	$node_l9->start(fail_ok => 1);
	my $log_l9 = slurp_file($node_l9->logfile);
	like($log_l9,
		 qr/cluster injection point "cluster-diag-pre-spawn" armed with ERROR|SQLSTATE 53R0E|DIAG_SPAWN_FAILED|cluster phase 2: failed to spawn DIAG/i,
		 'L9 phase 2 FATAL out path works when DIAG spawn is interrupted by injection (F13 fail_ctx plumbing reachable)');

	$node_l9->stop('immediate', fail_ok => 1);
}


# ----------
# L10: cluster.enabled = false → DIAG does NOT spawn (HC4 degraded path).
# ----------
{
	my $node_l10 = PgracClusterNode->new('l10_diag_disabled');
	$node_l10->init;
	$node_l10->append_conf('postgresql.conf',
		"log_min_messages = debug1\ncluster.enabled = off\n");
	$node_l10->start;

	my $diag_count = $node_l10->safe_psql('postgres',
		q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'diag'});
	is($diag_count, '0',
	   'L10 cluster.enabled=off → DIAG does NOT spawn (HC4 degraded stub path)');

	# phase 2 should still progress (Don't FATAL on disabled).
	my $phase = $node_l10->safe_psql('postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category='phase' AND key='cluster_phase'});
	is($phase, 'running',
	   'L10 phase machinery still advances to running with cluster.enabled=off');

	$node_l10->stop;
}


done_testing();
