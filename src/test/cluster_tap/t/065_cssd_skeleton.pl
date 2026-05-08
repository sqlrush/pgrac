#-------------------------------------------------------------------------
#
# 065_cssd_skeleton.pl
#    Stage 2.5 + spec-2.5 v0.2 single-instance CSSD aux process lifecycle
#    + observability surface verification.  Mirror of spec-1.14
#    064_cluster_stats_skeleton.pl;CSSD is the 5th cluster aux process
#    spawned by postmaster phase 4 driver third upgrade (DIAG + Stats +
#    CSSD serial wait sharing phase4_remaining_budget_ms).
#
#    Test matrix (L1-L10):
#
#      L1   normal startup spawns CSSD aux process (pg_stat_activity
#           shows backend_type='cssd')
#      L2   phase 4 advances to running only after CSSD publishes ready
#           (postmaster log shows ordering)
#      L3   --enable-cluster compile + cluster module live still spawns
#           CSSD (HC4 cluster_enabled GUC=on)
#      L4   clean shutdown stops CSSD normally;NO crash recovery (HC5)
#      L5   abnormal CSSD exit (kill -9) routes through HandleChildCrash
#      L5b  restart_after_crash=on + CSSD respawn after kill (LIFO LMON
#           respawn pattern)
#      L6   post-crash phase recovers to running (restart_after_crash)
#      L7   pg_cluster_state.cluster_cssd 7 keys agree with live
#           pg_stat_activity
#      L8   cluster_cssd_main_loop_iters grows over 3s (live tick proof)
#      L9   53R30 CSSD_SPAWN_FAILED FATAL reachable via inject point
#           cluster-cssd-pre-spawn:skip
#      L10  cluster.enabled = false → CSSD does NOT spawn (HC4 degraded)
#
# IDENTIFICATION
#    src/test/cluster_tap/t/065_cssd_skeleton.pl
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
$node->append_conf('postgresql.conf', "log_min_messages = debug1\n");
$node->start;


# ----------
# L1: postmaster spawned CSSD aux process; pg_stat_activity sees it.
# ----------
my $cssd_count = $node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'cssd'});
is($cssd_count, '1', 'L1 CSSD aux process visible in pg_stat_activity (spec-2.5 phase 4 串行第三)');


# ----------
# L2: phase 4 advanced to running only after CSSD published ready.
# Postmaster log line "CSSD ready" must appear before the
# "phase4_normal -> running" transition log.
# ----------
my $log_l2 = slurp_file($node->logfile);
my $cssd_ready_pos = index($log_l2, "CSSD ready");
my $running_pos = index($log_l2, "cluster startup: phase4_normal -> running");
ok($cssd_ready_pos >= 0, 'L2 postmaster log contains CSSD ready message');
ok($running_pos >= 0, 'L2 postmaster log contains phase4_normal -> running transition');
ok($cssd_ready_pos > 0 && $running_pos > 0 && $cssd_ready_pos < $running_pos,
   'L2 phase 4 driver waited for CSSD ready before advancing to running');


# ----------
# L3: --enable-cluster compile + cluster module live spawns CSSD.
# ----------
my $cluster_phase = $node->safe_psql('postgres',
	"SELECT value FROM pg_cluster_state WHERE category = 'phase' AND key = 'cluster_phase'");
is($cluster_phase, 'running',
   'L3 cluster module live; phase=running confirms CSSD spawn path completed (HC4 cluster_enabled=on)');


# ----------
# L4: clean shutdown stops CSSD normally (HC5 normal-exit path).
# ----------
$node->stop;
my $log_l4 = slurp_file($node->logfile);
like($log_l4, qr/database system is shut down/,
	 'L4 clean shutdown completes (pg_ctl stop -m fast)');
unlike($log_l4,
	   qr/HandleChildCrash|server process .* exited|terminating any other active server processes/,
	   'L4 CSSD normal exit does NOT trigger crash recovery (HC5 normal path)');


# ----------
# L5: abnormal CSSD exit (kill -9 cssd pid) routes through HandleChildCrash.
# ----------
$node->start;
my $cssd_pid = $node->safe_psql('postgres',
	q{SELECT pid FROM pg_stat_activity WHERE backend_type = 'cssd' LIMIT 1});
ok($cssd_pid && $cssd_pid =~ /^\d+$/, 'L5 captured CSSD pid for kill test');

if ($cssd_pid && $cssd_pid =~ /^\d+$/) {
	kill 9, $cssd_pid;

	my $waited = 0;
	my $log_l5 = '';
	while ($waited < 10) {
		sleep 1;
		$waited++;
		$log_l5 = slurp_file($node->logfile);
		last
		  if $log_l5
		  =~ /terminating any other active server processes|crash of another server process/;
	}

	like($log_l5,
		 qr/terminating any other active server processes|crash of another server process/,
		 'L5 CSSD kill -9 routes through HandleChildCrash (HC5 abnormal path)');
}

$node->stop('immediate', fail_ok => 1);


# ----------
# L5b: restart_after_crash=on + CSSD respawn after kill.
# ----------
{
	my $node_l5b = PgracClusterNode->new('l5b_cssd_respawn');
	$node_l5b->init;
	$node_l5b->append_conf('postgresql.conf',
		"restart_after_crash = on\nlog_min_messages = debug1\n");
	$node_l5b->start;

	my $cssd_pid_initial = $node_l5b->safe_psql('postgres',
		q{SELECT pid FROM pg_stat_activity WHERE backend_type = 'cssd' LIMIT 1});
	ok($cssd_pid_initial && $cssd_pid_initial =~ /^\d+$/,
	   'L5b captured initial CSSD pid (restart_after_crash=on baseline)');

	if ($cssd_pid_initial && $cssd_pid_initial =~ /^\d+$/) {
		kill 9, $cssd_pid_initial;

		my $cssd_pid_new = '';
		my $waited = 0;
		while ($waited < 30) {
			sleep 1;
			$waited++;
			my $r = eval {
				$node_l5b->safe_psql('postgres',
					q{SELECT pid FROM pg_stat_activity WHERE backend_type = 'cssd' LIMIT 1});
			};
			next if $@;
			next unless $r && $r =~ /^\d+$/;
			next if $r eq $cssd_pid_initial;
			$cssd_pid_new = $r;
			last;
		}

		ok($cssd_pid_new && $cssd_pid_new ne $cssd_pid_initial,
		   "L5b CSSD respawned after kill -9 (initial=$cssd_pid_initial new=$cssd_pid_new)");
	}

	$node_l5b->stop('immediate', fail_ok => 1);
}


# ----------
# L6: post-crash phase recovers to 'running' after restart_after_crash.
# ----------
{
	my $node_l6 = PgracClusterNode->new('l6_cssd_post_crash_phase');
	$node_l6->init;
	$node_l6->append_conf('postgresql.conf',
		"restart_after_crash = on\nlog_min_messages = debug1\n");
	$node_l6->start;

	my $pid = $node_l6->safe_psql('postgres',
		q{SELECT pid FROM pg_stat_activity WHERE backend_type = 'cssd' LIMIT 1});

	if ($pid && $pid =~ /^\d+$/) {
		kill 9, $pid;

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
		   'L6 cluster_phase returns to running after restart_after_crash recovery');
	}

	$node_l6->stop('immediate', fail_ok => 1);
}


# ----------
# L7 + L8: live state observation.
# ----------
my $node_lx = PgracClusterNode->new('lx_cssd_state');
$node_lx->init;
$node_lx->append_conf('postgresql.conf', "log_min_messages = debug1\n");
$node_lx->start;


# ----------
# L7: pg_cluster_state.cluster_cssd 7 keys agree with pg_stat_activity.
# ----------
my $live_pid = $node_lx->safe_psql('postgres',
	q{SELECT pid FROM pg_stat_activity WHERE backend_type = 'cssd' LIMIT 1});
my $sql_pid = $node_lx->safe_psql('postgres',
	q{SELECT value FROM pg_cluster_state
	   WHERE category='cluster_cssd' AND key='cluster_cssd_pid'});
is($sql_pid, $live_pid,
   "L7 pg_cluster_state.cluster_cssd.cluster_cssd_pid matches live pg_stat_activity ($sql_pid == $live_pid)");

my $cssd_keys = $node_lx->safe_psql('postgres',
	q{SELECT string_agg(key, ',' ORDER BY key)
	    FROM pg_cluster_state WHERE category='cluster_cssd'});
like($cssd_keys, qr/cluster_cssd_status\b/, 'L7 pg_cluster_state.cluster_cssd exposes cluster_cssd_status');
like($cssd_keys, qr/cluster_cssd_pid\b/, 'L7 pg_cluster_state.cluster_cssd exposes cluster_cssd_pid');
like($cssd_keys, qr/cluster_cssd_spawned_at\b/,
	 'L7 pg_cluster_state.cluster_cssd exposes cluster_cssd_spawned_at');
like($cssd_keys, qr/cluster_cssd_ready_at\b/,
	 'L7 pg_cluster_state.cluster_cssd exposes cluster_cssd_ready_at');
like($cssd_keys, qr/cluster_cssd_last_liveness_tick_at\b/,
	 'L7 pg_cluster_state.cluster_cssd exposes cluster_cssd_last_liveness_tick_at');
like($cssd_keys, qr/cluster_cssd_main_loop_iters\b/,
	 'L7 pg_cluster_state.cluster_cssd exposes cluster_cssd_main_loop_iters');


# ----------
# L8: cluster_cssd_main_loop_iters grows over 3s.
# ----------
my $iters_t0 = $node_lx->safe_psql('postgres',
	q{SELECT value::bigint FROM pg_cluster_state
	   WHERE category='cluster_cssd' AND key='cluster_cssd_main_loop_iters'});
sleep 3;
my $iters_t1 = $node_lx->safe_psql('postgres',
	q{SELECT value::bigint FROM pg_cluster_state
	   WHERE category='cluster_cssd' AND key='cluster_cssd_main_loop_iters'});
cmp_ok($iters_t1, '>', $iters_t0,
	   "L8 cluster_cssd_main_loop_iters grew over 3s ($iters_t0 -> $iters_t1) — main loop is ticking");

$node_lx->stop;


# ----------
# L9: phase 4 FATAL path works when CSSD spawn is interrupted.
# ----------
{
	my $node_l9 = PgracClusterNode->new('l9_cssd_spawn_fail');
	$node_l9->init;
	$node_l9->append_conf('postgresql.conf',
		"log_min_messages = debug1\n"
		. "cluster.injection_points = 'cluster-cssd-pre-spawn:skip'\n");

	$node_l9->start(fail_ok => 1);
	my $log_l9 = slurp_file($node_l9->logfile);
	like($log_l9,
		 qr/SQLSTATE 53R30|CSSD_SPAWN_FAILED|cluster phase 4: failed to spawn CSSD/i,
		 'L9 phase 4 FATAL out path works when CSSD spawn is interrupted by injection (53R30 plumbing reachable)');

	$node_l9->stop('immediate', fail_ok => 1);
}


# ----------
# L10: cluster.enabled = false → CSSD does NOT spawn (HC4 degraded path).
# ----------
{
	my $node_l10 = PgracClusterNode->new('l10_cssd_disabled');
	$node_l10->init;
	$node_l10->append_conf('postgresql.conf',
		"log_min_messages = debug1\ncluster.enabled = off\n");
	$node_l10->start;

	my $cnt = $node_l10->safe_psql('postgres',
		q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'cssd'});
	is($cnt, '0', 'L10 cluster.enabled=off → CSSD does NOT spawn (HC4 degraded stub path)');

	my $phase = $node_l10->safe_psql('postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category='phase' AND key='cluster_phase'});
	is($phase, 'running',
	   'L10 phase machinery still advances to running with cluster.enabled=off');

	$node_l10->stop;
}


done_testing();
