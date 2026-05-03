#-------------------------------------------------------------------------
#
# 060_postmaster_phases.pl
#    Stage 1.10 end-to-end: postmaster startup phase machinery (Phase
#    0 -> 1 -> 2 -> 3 -> 4 -> RUNNING).
#
#    Spec-1.10 introduces a state machine that splits the previously
#    single cluster_init() entry into Phase 0 (pre-shmem base) ->
#    Phase 1 (cluster basics: interconnect listener / heartbeat /
#    LMON) -> Phase 2 (lock services: LMS / LMD / LCK) -> Phase 3
#    (recovery: startup process / Recovery Coordinator / Workers) ->
#    Phase 4 (normal startup: walwriter / bgwriter / DIAG / Cluster
#    Stats) -> RUNNING.  See docs/background-process-design.md §4.
#
#    Test matrix (spec-1.10 §4.2):
#
#      L1   normal startup walks Phase 0 -> 1 -> 2 -> 3 -> 4 -> RUNNING
#      L2   pg_cluster_state.phase has 5 keys (cluster_phase /
#           phase_enum_value / phase_started_at / phase_elapsed_seconds
#           / phase_history)
#      L3   cluster.phase{1..4}_timeout GUC defaults match background-
#           process-design.md §4.3
#      L4   elog DEBUG1 transition messages observable when log_min_
#           messages = debug1
#      L5   cluster-startup-phase-1-fail inject point + restart causes
#           postmaster FATAL with SQLSTATE 53R09
#      L6   wait events ClusterStartupPhase{0..4}Wait registered
#      L7   shutdown sequence transitions to SHUTDOWN
#      L8   restart preserves phase machinery semantics (second start
#           also walks Phase 0 -> RUNNING)
#      L9   stub phase 1-3 do not naturally trigger timeout (even with
#           cluster.phase1_timeout = 1s, startup completes immediately)
#      L10  cluster_phase legacy mirror string ("running") visible
#           after startup completes
#      L11  pg_stat_activity / BackendType not crashed by phase
#           machinery (basic SELECT works)
#      L12  spec-1.8 GUC=on cluster.smgr_user_relations + spec-1.10
#           phase machinery coexist (initdb + opt-in + restart)
#
# IDENTIFICATION
#    src/test/cluster_tap/t/060_postmaster_phases.pl
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

# Set log_min_messages = debug1 so we can observe phase transitions.
$node->append_conf('postgresql.conf', "log_min_messages = debug1\n");

$node->start;


# ----------
# L1: normal startup walks Phase 0 -> 1 -> 2 -> 3 -> 4 -> RUNNING.
# ----------
my $log_l1 = slurp_file($node->logfile);
my @l1_transitions = ($log_l1 =~ m/cluster startup: (\w+) -> (\w+)/g);
ok(scalar(@l1_transitions) >= 12,
   "L1 saw at least 6 phase transitions in startup log "
   . "(got " . (scalar(@l1_transitions) / 2) . " transitions)");

# Verify the canonical sequence appears.
like($log_l1, qr/cluster startup: pre_init -> phase0_base/,
	 'L1 PRE_INIT -> phase0_base transition logged');
like($log_l1, qr/cluster startup: phase0_base -> phase1_cluster/,
	 'L1 phase0_base -> phase1_cluster transition logged');
like($log_l1, qr/cluster startup: phase1_cluster -> phase2_lock/,
	 'L1 phase1_cluster -> phase2_lock transition logged');
like($log_l1, qr/cluster startup: phase2_lock -> phase3_recovery/,
	 'L1 phase2_lock -> phase3_recovery transition logged');
like($log_l1, qr/cluster startup: phase3_recovery -> phase4_normal/,
	 'L1 phase3_recovery -> phase4_normal transition logged');
like($log_l1, qr/cluster startup: phase4_normal -> running/,
	 'L1 phase4_normal -> running transition logged');


# ----------
# L2: pg_cluster_state.phase has 5 keys.
# ----------
is($node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_cluster_state WHERE category = 'phase'"),
	'5', 'L2 pg_cluster_state.phase has 5 keys');

is($node->safe_psql(
		'postgres',
		"SELECT string_agg(key, ',' ORDER BY key) FROM pg_cluster_state WHERE category = 'phase'"),
	'cluster_phase,phase_elapsed_seconds,phase_enum_value,phase_history,phase_started_at',
	'L2 pg_cluster_state.phase keys are exactly the 5 spec-1.10 names');


# ----------
# L3: cluster.phase{1..4}_timeout GUC defaults.
# ----------
is($node->safe_psql('postgres', 'SHOW cluster.phase1_timeout'),
	'1min', 'L3 cluster.phase1_timeout default 60s');
is($node->safe_psql('postgres', 'SHOW cluster.phase2_timeout'),
	'30s', 'L3 cluster.phase2_timeout default 30s');
is($node->safe_psql('postgres', 'SHOW cluster.phase3_timeout'),
	'10min', 'L3 cluster.phase3_timeout default 600s');
is($node->safe_psql('postgres', 'SHOW cluster.phase4_timeout'),
	'30s', 'L3 cluster.phase4_timeout default 30s');


# ----------
# L4: elog DEBUG1 phase stub messages observable.
# ----------
like($log_l1, qr/Phase 1 stub: skipping interconnect/,
	 'L4 Phase 1 stub DEBUG1 message logged');
like($log_l1, qr/Phase 2 stub: skipping LMS/,
	 'L4 Phase 2 stub DEBUG1 message logged');
like($log_l1, qr/Phase 3 stub:/,
	 'L4 Phase 3 stub DEBUG1 message logged');
like($log_l1, qr/Phase 4 stub:/,
	 'L4 Phase 4 stub DEBUG1 message logged');


# ----------
# L5: 17 spec-1.10 inject points registered (15 phase-specific +
# 2 driver-level).  Arming + restart-and-observe behavior testing
# of the -fail injection points lives in 030 acceptance §M / a
# future hardening once real handlers (1.11+) make the fault
# propagation deterministic.  At skeleton level (1.10), all phase
# handlers return PHASE_RUN_OK immediately so a fault armed at
# -enter does not propagate to driver behavior; we verify the
# inject points exist + framework wiring is correct.
# ----------
is( $node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_stat_cluster_injections WHERE name LIKE 'cluster-startup-phase-%' OR name LIKE 'cluster-run-%'"
	),
	'17',
	'L5 17 spec-1.10 inject points registered (5 phase x 3 each + 2 driver)');


# ----------
# L6: wait events ClusterStartupPhase{0..4}Wait registered.
# ----------
is( $node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_stat_cluster_wait_events WHERE name LIKE 'ClusterStartupPhase%Wait'"
	),
	'5', 'L6 5 ClusterStartupPhase wait events registered');


# ----------
# L7: shutdown sequence -- 1.10 stub.
#
# Spec-1.10 §3.2 SHUTDOWN row: 1.10 stub directly transitions to
# SHUTDOWN; reverse-order graceful tear-down (RUNNING -> 4 -> 3 -> 2
# -> 1 -> SHUTDOWN) is deferred to 1.11-1.14 / Stage 6 once per-phase
# background processes that need graceful stop are spawned.  We only
# verify clean postmaster stop here; explicit "shutdown" transition
# log line is wired in by 1.11+.
# ----------
$node->stop;

my $log_l7 = slurp_file($node->logfile);
unlike($log_l7, qr/PANIC:/,
	'L7 clean shutdown produced no PANIC');
like($log_l7, qr/database system is shut down/,
	'L7 logfile records the orderly shutdown message');

$node->start;


# ----------
# L8: restart re-runs phase machinery cleanly.
# ----------
$node->restart;

my $log_l8 = slurp_file($node->logfile);
my @l8_running_starts = ($log_l8 =~ m/cluster startup: phase4_normal -> running/g);
ok(scalar(@l8_running_starts) >= 2,
   "L8 phase machinery re-runs on restart "
   . "(saw " . scalar(@l8_running_starts) . " 'running' entries across all starts)");


# ----------
# L9: stub phase does not naturally trigger timeout.
#
# Set cluster.phase1_timeout = 1s and restart.  Stub handlers return
# PHASE_RUN_OK immediately so the deadline never fires.  If 1.10
# accidentally introduced a real wait somewhere, this test would
# detect it.
# ----------
$node->stop;
$node->append_conf('postgresql.conf', "cluster.phase1_timeout = 1\n");
$node->start;
is($node->safe_psql('postgres', 'SHOW cluster.phase1_timeout'),
	'1s', 'L9 cluster.phase1_timeout = 1s applies');
is($node->safe_psql('postgres', 'SELECT 1'),
	'1', 'L9 server still responsive (stub does not block)');


# ----------
# L10: cluster_phase legacy mirror string after startup completes.
# ----------
is( $node->safe_psql(
		'postgres',
		"SELECT value FROM pg_cluster_state WHERE category = 'phase' AND key = 'cluster_phase'"
	),
	'running', 'L10 cluster_phase legacy mirror reads "running" after startup');

is( $node->safe_psql(
		'postgres',
		"SELECT value FROM pg_cluster_state WHERE category = 'phase' AND key = 'phase_enum_value'"
	),
	'6',
	'L10 phase_enum_value = 6 (CLUSTER_PHASE_RUNNING) after startup');


# ----------
# L11: BackendType / pg_stat_activity not broken by phase machinery.
# ----------
ok($node->safe_psql('postgres',
		'SELECT count(*) >= 1 FROM pg_stat_activity') eq 't',
   'L11 pg_stat_activity SELECT works after phase machinery startup');


# ----------
# L12: spec-1.8 GUC=on cluster.smgr_user_relations + spec-1.10 phase
# machinery coexist.
# ----------
$node->stop;
$node->append_conf('postgresql.conf',
	"cluster.shared_storage_backend = local\ncluster.smgr_user_relations = on\n");
$node->start;
$node->safe_psql('postgres',
	'CREATE TABLE l12_smoke (id int); INSERT INTO l12_smoke VALUES (1)');
is($node->safe_psql('postgres', 'SELECT count(*) FROM l12_smoke'),
	'1', 'L12 spec-1.8 cluster_smgr opt-in + spec-1.10 phase machinery coexist');

my $log_l12 = slurp_file($node->logfile);
like($log_l12, qr/cluster startup: phase4_normal -> running/,
	 'L12 phase machinery still drives Phase 0 -> RUNNING under cluster_smgr opt-in');
like($log_l12, qr/cluster\.smgr_user_relations is experimental/,
	 'L12 spec-1.7.2 EXPERIMENTAL WARNING still fires alongside phase machinery');

$node->stop;


# ----------
# L13 (spec-1.10.1 D1 F1): backend reads phase=running from shmem,
# not from a process-local static.  On POSIX fork() platforms this
# also held with the static-globals layout (child inherited the
# postmaster's snapshot); on EXEC_BACKEND/Windows the static-globals
# layout failed because re-exec'd children re-ran static initializers
# and saw PRE_INIT.  L13 anchors the shmem-backed read path: any
# backend SQL session must see phase=running after startup completes.
# ----------
$node->start;
my $phase_l13 = $node->safe_psql('postgres',
	"SELECT value FROM pg_cluster_state WHERE category = 'phase' AND key = 'cluster_phase'");
is($phase_l13, 'running',
   'L13 backend SELECTs phase=running from shmem (spec-1.10.1 F1)');

my $phase_enum_l13 = $node->safe_psql('postgres',
	"SELECT value FROM pg_cluster_state WHERE category = 'phase' AND key = 'phase_enum_value'");
is($phase_enum_l13, '6',
   'L13 phase_enum_value = 6 (RUNNING) via shmem');

$node->stop;


# ----------
# L14 (spec-1.10.1 D2 F2 / Q2=D): driver synchronous elapsed check
# enforces cluster.phaseN_timeout.  Inject sleep(2s) on phase 1
# enter; with cluster.phase1_timeout=1, the handler returns after
# the inject sleep completes, the driver computes elapsed > timeout,
# and ereports FATAL with 53R08 (CLUSTER_PHASE_TRANSITION_TIMEOUT).
# Postmaster startup fails; logfile contains the timeout message.
# ----------
{
    my $node_l14 = PostgreSQL::Test::Cluster->new('l14_timeout');
    $node_l14->init;
    $node_l14->append_conf('postgresql.conf', q{
cluster.phase1_timeout = 1
cluster.injection_points = 'cluster-startup-phase-1-enter:sleep:2000000'
});
    # PostgreSQL::Test::Cluster::start uses BAIL_OUT on failure unless
    # fail_ok is passed; eval cannot catch BAIL_OUT, so we must opt into
    # the fail-ok path explicitly.  start() returns 1 on success and 0 on
    # failure when fail_ok is set.
    my $start_ok = $node_l14->start(fail_ok => 1);
    ok(!$start_ok, 'L14 postmaster startup fails when phase 1 elapsed > timeout');

    my $log_l14 = slurp_file($node_l14->logfile);
    like($log_l14,
         qr/cluster startup phase phase1_cluster exceeded timeout/,
         'L14 logfile contains phase 1 timeout FATAL message (53R08)');
    eval { $node_l14->stop('immediate'); };
}


# ----------
# L15 (spec-1.10.1 D4 F4): advance(RUNNING) is delayed to PostmasterMain
# just before ServerLoop entry.  Before the spec-1.10.1 fix, advance to
# RUNNING happened inside cluster_run_startup_sequence() right after
# CreateSharedMemoryAndSemaphores, before set_max_safe_fds / listen
# socket / startup process / bgwriter etc.  After the fix, the first
# SELECT after $node->start always observes phase=running because that
# transition is the immediate predecessor of ServerLoop().
# ----------
$node->start;
my $phase_l15 = $node->safe_psql('postgres',
	"SELECT value FROM pg_cluster_state WHERE category = 'phase' AND key = 'cluster_phase'");
is($phase_l15, 'running',
   'L15 first SELECT after start sees phase=running (advance delayed to ServerLoop entry)');
$node->stop;


done_testing();
