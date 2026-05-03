#-------------------------------------------------------------------------
#
# 061_lmon_skeleton.pl
#    Stage 1.11 Sprint A end-to-end: LMON aux process minimum runnable
#    closure.  postmaster spawns LMON; phase 1 sync waits ready; clean
#    shutdown stops LMON normally; abnormal exit triggers PG crash
#    recovery (HC5).  Sprint A scope.
#
#    Spec-1.11 introduces the first real cluster background process
#    spawned by postmaster.  Sprint A boundary covers lifecycle only:
#    AuxProcType integration / shmem state / readiness sync / shutdown
#    protocol / crash semantics.  Heartbeat consumption / reconfig /
#    fence / GRD / Recovery Coordinator triggering land in Stage 2-6.
#
#    Sprint A test matrix (L1-L5; L6-L10 land in Sprint B):
#
#      L1   normal startup spawns LMON aux process (pg_stat_activity
#           shows backend_type='lmon')
#      L2   phase 1 advances to phase 2 only after LMON publishes
#           ready (postmaster log shows ordering)
#      L3   cluster_smgr_user_relations off baseline still spawns LMON
#           (HC4 cluster_enabled GUC + L9 deferred to Sprint B; this
#           test serves as the de-facto compile-time cluster-enabled
#           anchor for Sprint A)
#      L4   clean shutdown (pg_ctl stop -m fast) stops LMON normally;
#           postmaster log shows LMON shutdown without crash recovery
#      L5   abnormal LMON exit (kill -9) triggers PG crash recovery
#           cycle per HC5; postmaster log shows HandleChildCrash path
#
# IDENTIFICATION
#    src/test/cluster_tap/t/061_lmon_skeleton.pl
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

# Set log_min_messages = debug1 so we can observe phase + LMON
# liveness debug messages.
$node->append_conf('postgresql.conf', "log_min_messages = debug1\n");

$node->start;


# ----------
# L1: postmaster spawned LMON aux process; pg_stat_activity sees it.
# ----------
my $lmon_count = $node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'lmon'});
is($lmon_count, '1',
   'L1 LMON aux process visible in pg_stat_activity (spec-1.11 Sprint A)');


# ----------
# L2: phase 1 advanced to phase 2 only after LMON published ready.
# Postmaster log line "cluster phase 1: LMON ready" must appear before
# the "phase1_cluster -> phase2_lock" transition log.
# ----------
my $log_l2 = slurp_file($node->logfile);
my $lmon_ready_pos
	= index($log_l2, "cluster phase 1: LMON ready");
my $phase2_pos
	= index($log_l2, "cluster startup: phase1_cluster -> phase2_lock");
ok($lmon_ready_pos >= 0, 'L2 postmaster log contains LMON ready message');
ok($phase2_pos >= 0, 'L2 postmaster log contains phase1->phase2 transition');
ok($lmon_ready_pos > 0 && $phase2_pos > 0 && $lmon_ready_pos < $phase2_pos,
   'L2 phase 1 driver waited for LMON ready before advancing to phase 2');


# ----------
# L3: --enable-cluster compile + cluster module live still spawns LMON.
# Sprint A doesn't have a runtime cluster_enabled GUC (HC4 deferred to
# Sprint B per spec-1.11 §1.4 Q-amend); this test verifies the
# compile-time gate works end-to-end (LMON spawn happens in cluster
# build).  Sprint B will add cluster.enabled PGC_POSTMASTER GUC + L9
# acceptance test that toggles GUC=off path.
# ----------
my $cluster_phase = $node->safe_psql('postgres',
	"SELECT value FROM pg_cluster_state WHERE category = 'phase' AND key = 'cluster_phase'");
is($cluster_phase, 'running',
   'L3 cluster module live; phase=running confirms LMON spawn path '
   . 'completed (Sprint A compile-time gate; Sprint B adds runtime cluster_enabled GUC)');


# ----------
# L4: clean shutdown stops LMON normally (HC5 normal-exit path).
# pg_ctl stop -m fast signals SIGTERM to postmaster -> postmaster
# signals SIGTERM to LMON -> LMON main loop sees ShutdownRequestPending,
# proc_exit(0) -> reaper sees WIFEXITED + WEXITSTATUS=0 -> no crash
# recovery.  Postmaster log shows clean phase shutdown transition.
# ----------
$node->stop;
my $log_l4 = slurp_file($node->logfile);
like($log_l4, qr/database system is shut down/,
	 'L4 clean shutdown completes (pg_ctl stop -m fast)');
unlike($log_l4, qr/HandleChildCrash|server process .* exited|terminating any other active server processes/,
	   'L4 LMON normal exit does NOT trigger crash recovery (HC5 normal path)');


# ----------
# L5: abnormal LMON exit (kill -9 LMON pid) routes through
# HandleChildCrash.  PostgreSQL::Test::Cluster.pm forces
# restart_after_crash = off in init() so this test only exercises
# the shutdown-on-crash branch (postmaster terminates other children
# and exits).  The full restart-after-crash recovery cycle is
# covered by L5b below with an explicit GUC override.  Sprint A
# baseline + spec-1.11 Sprint B codex round 3 P2.4 honest scoping.
# ----------
$node->start;
my $lmon_pid = $node->safe_psql('postgres',
	q{SELECT pid FROM pg_stat_activity WHERE backend_type = 'lmon' LIMIT 1});
ok($lmon_pid && $lmon_pid =~ /^\d+$/,
   'L5 captured LMON pid for kill test');

if ($lmon_pid && $lmon_pid =~ /^\d+$/) {
	# SIGKILL the LMON child.  Postmaster reaper sees abnormal exit ->
	# HandleChildCrash -> with restart_after_crash=off (TAP harness
	# default) postmaster terminates other children and exits.
	kill 9, $lmon_pid;

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
		 'L5 LMON kill -9 routes through HandleChildCrash (HC5 abnormal path; '
		 . 'restart_after_crash=off forces shutdown branch)');
}

# Best-effort cleanup; postmaster may have already exited via crash
# branch.  fail_ok bypasses BAIL_OUT in PostgreSQL::Test::Cluster.
$node->stop('immediate', fail_ok => 1);


# ----------
# L5b (spec-1.11 Sprint B codex round 3 P1.1 + P1.2 + P2.4):
# explicit restart_after_crash=on + LMON respawn after kill.
#
# Sprint A had a hidden gap: cluster_run_startup_sequence ran exactly
# once in PostmasterMain, so after restart_after_crash recovery LMON
# was never respawned (postmaster would continue serving SQL with no
# LMON, breaking cluster coordination silently).  Sprint B fixes this
# by adding LMON respawn logic in ServerLoop (mirrors WalWriter), so
# whenever pmState=PM_RUN && LmonPID=0 && cluster_enabled, postmaster
# starts a fresh LMON child.
#
# This test verifies the respawn closure end-to-end:
#   1. start node with restart_after_crash=on
#   2. capture initial LMON pid
#   3. kill -9 LMON
#   4. wait for crash recovery + new LMON spawn
#   5. confirm new LMON pid != initial pid
# ----------
{
	my $node_l5b = PgracClusterNode->new('l5b_respawn');
	$node_l5b->init;
	$node_l5b->append_conf('postgresql.conf',
		"restart_after_crash = on\nlog_min_messages = debug1\n");
	$node_l5b->start;

	my $lmon_pid_initial = $node_l5b->safe_psql('postgres',
		q{SELECT pid FROM pg_stat_activity WHERE backend_type = 'lmon' LIMIT 1});
	ok($lmon_pid_initial && $lmon_pid_initial =~ /^\d+$/,
	   'L5b captured initial LMON pid (restart_after_crash=on baseline)');

	if ($lmon_pid_initial && $lmon_pid_initial =~ /^\d+$/) {
		kill 9, $lmon_pid_initial;

		# Wait for restart cycle + new LMON spawn.  Up to 30 seconds.
		my $lmon_pid_new = '';
		my $waited = 0;
		while ($waited < 30) {
			sleep 1;
			$waited++;
			my $r = eval {
				$node_l5b->safe_psql('postgres',
					q{SELECT pid FROM pg_stat_activity WHERE backend_type = 'lmon' LIMIT 1});
			};
			next if $@;	  # connection still recovering
			next unless $r && $r =~ /^\d+$/;
			next if $r eq $lmon_pid_initial;	# old LMON pid still cached
			$lmon_pid_new = $r;
			last;
		}

		ok($lmon_pid_new && $lmon_pid_new ne $lmon_pid_initial,
		   "L5b LMON respawned after kill -9 (initial=$lmon_pid_initial new=$lmon_pid_new); "
		   . "spec-1.11 Sprint B P1 fix: ServerLoop respawn logic mirrors WalWriter pattern");
	}

	$node_l5b->stop('immediate', fail_ok => 1);
}


done_testing();
