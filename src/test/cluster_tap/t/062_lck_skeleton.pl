#-------------------------------------------------------------------------
#
# 062_lck_skeleton.pl
#    Stage 1.12 Sprint A end-to-end: LCK aux process minimum runnable
#    closure.  postmaster spawns LCK; phase 2 sync waits ready; clean
#    shutdown stops LCK normally; abnormal exit triggers PG crash
#    recovery (HC5).  Sprint A scope.
#
#    Spec-1.12 introduces the second real cluster background process
#    spawned by postmaster (after LMON in 1.11).  Sprint A boundary
#    covers lifecycle only: AuxProcType integration / shmem state /
#    readiness sync / shutdown protocol / crash semantics.  Real LCK
#    dictionary lock acquisition land in Stage 2+.
#
#    Sprint A test matrix (L1-L5 + L5b respawn):
#
#      L1   normal startup spawns LCK aux process (pg_stat_activity
#           shows backend_type='lck')
#      L2   phase 2 advances to phase 3 only after LCK publishes
#           ready (postmaster log shows ordering)
#      L3   --enable-cluster compile + cluster module live still spawns
#           LCK (HC4 cluster_enabled GUC=on confirmed)
#      L4   clean shutdown (pg_ctl stop -m fast) stops LCK normally;
#           postmaster log shows LCK shutdown without crash recovery
#      L5   abnormal LCK exit (kill -9) triggers PG crash recovery
#           cycle per HC5; postmaster log shows HandleChildCrash path
#      L5b  explicit restart_after_crash=on + LCK respawn after kill
#           (codex P1.1 + P1.2 + P2.4 lessons preempted in Sprint A)
#
# IDENTIFICATION
#    src/test/cluster_tap/t/062_lck_skeleton.pl
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

# Set log_min_messages = debug1 so we can observe phase + LCK
# liveness debug messages.
$node->append_conf('postgresql.conf', "log_min_messages = debug1\n");

$node->start;


# ----------
# L1: postmaster spawned LCK aux process; pg_stat_activity sees it.
# ----------
my $lck_count = $node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'lck'});
is($lck_count, '1',
   'L1 LCK aux process visible in pg_stat_activity (spec-1.12 Sprint A)');


# ----------
# L2: phase 2 advanced to phase 3 only after LCK published ready.
# Postmaster log line "cluster phase 2: LCK ready" must appear before
# the "phase2_lock -> phase3_recovery" transition log.
# ----------
my $log_l2 = slurp_file($node->logfile);
my $lck_ready_pos
	= index($log_l2, "cluster phase 2: LCK ready");
my $phase3_pos
	= index($log_l2, "cluster startup: phase2_lock -> phase3_recovery");
ok($lck_ready_pos >= 0, 'L2 postmaster log contains LCK ready message');
ok($phase3_pos >= 0, 'L2 postmaster log contains phase2->phase3 transition');
ok($lck_ready_pos > 0 && $phase3_pos > 0 && $lck_ready_pos < $phase3_pos,
   'L2 phase 2 driver waited for LCK ready before advancing to phase 3');


# ----------
# L3: --enable-cluster compile + cluster module live still spawns LCK.
# Spec-1.12 phase_2_handler reads cluster_enabled GUC (HC4 closure)
# from spec-1.11 Sprint B; with default cluster.enabled=on, LCK spawn
# happens.  Sprint B / future stages may add a per-node enable toggle
# but for Sprint A baseline we assert phase=running confirms the
# spawn path completed.
# ----------
my $cluster_phase = $node->safe_psql('postgres',
	"SELECT value FROM pg_cluster_state WHERE category = 'phase' AND key = 'cluster_phase'");
is($cluster_phase, 'running',
   'L3 cluster module live; phase=running confirms LCK spawn path '
   . 'completed (HC4 cluster_enabled GUC=on default)');


# ----------
# L4: clean shutdown stops LCK normally (HC5 normal-exit path).
# pg_ctl stop -m fast signals SIGTERM to postmaster -> postmaster
# signals SIGTERM to LCK -> LCK main loop sees ShutdownRequestPending,
# proc_exit(0) -> reaper sees WIFEXITED + WEXITSTATUS=0 -> no crash
# recovery.  Postmaster log shows clean phase shutdown transition.
# ----------
$node->stop;
my $log_l4 = slurp_file($node->logfile);
like($log_l4, qr/database system is shut down/,
	 'L4 clean shutdown completes (pg_ctl stop -m fast)');
unlike($log_l4, qr/HandleChildCrash|server process .* exited|terminating any other active server processes/,
	   'L4 LCK normal exit does NOT trigger crash recovery (HC5 normal path)');


# ----------
# L5: abnormal LCK exit (kill -9 LCK pid) routes through
# HandleChildCrash.  PostgreSQL::Test::Cluster.pm forces
# restart_after_crash = off in init() so this test only exercises
# the shutdown-on-crash branch (postmaster terminates other children
# and exits).  The full restart-after-crash recovery cycle is
# covered by L5b below with an explicit GUC override.  spec-1.12
# Sprint A preempts codex P2.4 lesson from spec-1.11 Sprint B.
# ----------
$node->start;
my $lck_pid = $node->safe_psql('postgres',
	q{SELECT pid FROM pg_stat_activity WHERE backend_type = 'lck' LIMIT 1});
ok($lck_pid && $lck_pid =~ /^\d+$/,
   'L5 captured LCK pid for kill test');

if ($lck_pid && $lck_pid =~ /^\d+$/) {
	# SIGKILL the LCK child.  Postmaster reaper sees abnormal exit ->
	# HandleChildCrash -> with restart_after_crash=off (TAP harness
	# default) postmaster terminates other children and exits.
	kill 9, $lck_pid;

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
		 'L5 LCK kill -9 routes through HandleChildCrash (HC5 abnormal path; '
		 . 'restart_after_crash=off forces shutdown branch)');
}

# Best-effort cleanup; postmaster may have already exited via crash
# branch.  fail_ok bypasses BAIL_OUT in PostgreSQL::Test::Cluster.
$node->stop('immediate', fail_ok => 1);


# ----------
# L5b (spec-1.12 Sprint A codex P1.1 + P1.2 + P2.4 preempted):
# explicit restart_after_crash=on + LCK respawn after kill.
#
# Spec-1.11 Sprint A had a hidden gap (caught in Sprint B): the
# startup sequence ran exactly once in PostmasterMain, so after
# restart_after_crash recovery LCK would never respawn.  Spec-1.12
# Sprint A preempts this lesson by:
#   - F9 fix: cluster_run_startup_sequence rerun on crash reinit path
#   - ServerLoop respawn logic in postmaster (mirrors WalWriter +
#     LMON pattern), so whenever pmState=PM_RUN && LckPID=0 &&
#     cluster_enabled, postmaster starts a fresh LCK child.
#
# This test verifies the respawn closure end-to-end:
#   1. start node with restart_after_crash=on
#   2. capture initial LCK pid
#   3. kill -9 LCK
#   4. wait for crash recovery + new LCK spawn
#   5. confirm new LCK pid != initial pid
# ----------
{
	my $node_l5b = PgracClusterNode->new('l5b_lck_respawn');
	$node_l5b->init;
	$node_l5b->append_conf('postgresql.conf',
		"restart_after_crash = on\nlog_min_messages = debug1\n");
	$node_l5b->start;

	my $lck_pid_initial = $node_l5b->safe_psql('postgres',
		q{SELECT pid FROM pg_stat_activity WHERE backend_type = 'lck' LIMIT 1});
	ok($lck_pid_initial && $lck_pid_initial =~ /^\d+$/,
	   'L5b captured initial LCK pid (restart_after_crash=on baseline)');

	if ($lck_pid_initial && $lck_pid_initial =~ /^\d+$/) {
		kill 9, $lck_pid_initial;

		# Wait for restart cycle + new LCK spawn.  Up to 30 seconds.
		my $lck_pid_new = '';
		my $waited = 0;
		while ($waited < 30) {
			sleep 1;
			$waited++;
			my $r = eval {
				$node_l5b->safe_psql('postgres',
					q{SELECT pid FROM pg_stat_activity WHERE backend_type = 'lck' LIMIT 1});
			};
			next if $@;	  # connection still recovering
			next unless $r && $r =~ /^\d+$/;
			next if $r eq $lck_pid_initial;	# old LCK pid still cached
			$lck_pid_new = $r;
			last;
		}

		ok($lck_pid_new && $lck_pid_new ne $lck_pid_initial,
		   "L5b LCK respawned after kill -9 (initial=$lck_pid_initial new=$lck_pid_new); "
		   . "spec-1.12 Sprint A P1 preemption: ServerLoop respawn logic mirrors LMON + WalWriter pattern");
	}

	$node_l5b->stop('immediate', fail_ok => 1);
}


done_testing();
