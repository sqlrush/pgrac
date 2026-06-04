#-------------------------------------------------------------------------
#
# 222_cluster_3_13_undo_cleaner.pl
#    Stage 3.13 Undo Cleaner aux process — lifecycle + observability
#    acceptance (L1-L10) for the ServerLoop-managed, best-effort form.
#
#    Unlike LMON/DIAG/Stats/CSSD (phase-4 gated, sync wait-ready), the
#    Undo Cleaner is spawned by postmaster's ServerLoop once
#    pmState == PM_RUN and respawned after exit; its absence degrades to
#    spec-3.12 lazy-only recycling.  L1-L10 therefore replace the
#    phase-ordering checks of t/064 with ServerLoop-form equivalents.
#
#      L1   ServerLoop spawns the cleaner; pg_stat_activity shows
#           backend_type='undo cleaner' (poll: spawn is post-PM_RUN)
#      L2   pg_cluster_state.undo_cleaner_status reaches 'ready'
#      L3   pg_cluster_state exposes the full 7-key surface (F11)
#      L4   NOT phase-gated: postmaster log contains no phase-4 wait
#           for the cleaner, and cluster phase still reaches 'running'
#      L5   SQL-visible pid agrees with live pg_stat_activity pid
#      L6   main_loop_iters grows under a fast interval (SIGHUP-applied)
#      L7   GUC surface: default 30000ms; ALTER SYSTEM + reload -> 500
#      L8   clean shutdown is a normal exit (no crash-recovery markers)
#      L9   kill -9 with restart_after_crash=on -> crash recovery ->
#           ServerLoop respawns cleaner with a NEW pid; status ready
#      L10  cluster.enabled=off -> no cleaner process at all (HC4)
#
#    L11-L15 (recycle/reuse/pinned-horizon/redo e2e) land in step 9 of
#    the spec-3.13 plan.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/222_cluster_3_13_undo_cleaner.pl
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
use PostgreSQL::Test::Utils;
use Test::More;
use PgracClusterNode;


# ============================================================
# Node A: default config (interval 30000ms) — L1-L5, L7, L8.
# ============================================================
my $node = PgracClusterNode->new('main');
$node->init;
$node->append_conf('postgresql.conf', "log_min_messages = debug1\n");
$node->start;


# ----------
# L1: ServerLoop spawn (post-PM_RUN, so poll instead of asserting
# immediately like the phase-gated siblings).
# ----------
ok($node->poll_query_until('postgres',
		q{SELECT count(*) = 1 FROM pg_stat_activity WHERE backend_type = 'undo cleaner'}),
   'L1 undo cleaner aux process visible in pg_stat_activity (ServerLoop spawn)');


# ----------
# L2: status reaches ready (shmem state published by the cleaner).
# ----------
ok($node->poll_query_until('postgres',
		q{SELECT value = 'ready' FROM pg_cluster_state
		   WHERE category = 'undo_cleaner' AND key = 'undo_cleaner_status'}),
   'L2 pg_cluster_state.undo_cleaner_status = ready');


# ----------
# L3: F11 7-key surface (2 status + 5 lifecycle).
# ----------
my $keys = $node->safe_psql('postgres',
	q{SELECT string_agg(key, ',' ORDER BY key) FROM pg_cluster_state
	   WHERE category = 'undo_cleaner'});
like($keys, qr/undo_cleaner_status/,         'L3 exposes undo_cleaner_status');
like($keys, qr/undo_cleaner_status_enum_value/, 'L3 exposes undo_cleaner_status_enum_value');
like($keys, qr/undo_cleaner_pid/,            'L3 exposes undo_cleaner_pid');
like($keys, qr/undo_cleaner_spawned_at/,     'L3 exposes undo_cleaner_spawned_at');
like($keys, qr/undo_cleaner_ready_at/,       'L3 exposes undo_cleaner_ready_at');
like($keys, qr/undo_cleaner_last_liveness_tick_at/,
	 'L3 exposes undo_cleaner_last_liveness_tick_at');
like($keys, qr/undo_cleaner_main_loop_iters/, 'L3 exposes undo_cleaner_main_loop_iters');


# ----------
# L4: ServerLoop-managed, NOT phase-gated.  The phase driver must not
# wait on the cleaner, and the cluster phase reaches 'running' on its
# own timeline.
# ----------
my $log_l4 = slurp_file($node->logfile);
unlike($log_l4, qr/cluster phase 4:.*[Uu]ndo [Cc]leaner/,
	   'L4 postmaster phase-4 driver never mentions the undo cleaner (not gated)');
my $cluster_phase = $node->safe_psql('postgres',
	"SELECT value FROM pg_cluster_state WHERE category = 'phase' AND key = 'cluster_phase'");
is($cluster_phase, 'running', 'L4 cluster phase running independent of cleaner');


# ----------
# L5: SQL pid agrees with live process pid.
# ----------
my $sql_pid = $node->safe_psql('postgres',
	q{SELECT value FROM pg_cluster_state
	   WHERE category = 'undo_cleaner' AND key = 'undo_cleaner_pid'});
my $live_pid = $node->safe_psql('postgres',
	q{SELECT pid FROM pg_stat_activity WHERE backend_type = 'undo cleaner'});
is($sql_pid, $live_pid, 'L5 pg_cluster_state pid agrees with pg_stat_activity pid');


# ----------
# L7 (before L6 so the fast interval helps L6): GUC default + SIGHUP.
# SignalHandlerForConfigReload sets the latch, so the new interval is
# picked up on the immediately-following loop iteration.
# ----------
my $interval_default = $node->safe_psql('postgres', 'SHOW cluster.undo_cleaner_interval_ms');
is($interval_default, '30000', 'L7 cluster.undo_cleaner_interval_ms defaults to 30000');
$node->safe_psql('postgres',
	"ALTER SYSTEM SET cluster.undo_cleaner_interval_ms = 500");
$node->safe_psql('postgres', 'SELECT pg_reload_conf()');
ok($node->poll_query_until('postgres',
		q{SELECT current_setting('cluster.undo_cleaner_interval_ms') = '500'}),
   'L7 SIGHUP reload applies new cleaner interval');


# ----------
# L6: liveness — main_loop_iters grows once the 500ms interval is live.
# ----------
my $iters_before = $node->safe_psql('postgres',
	q{SELECT value::bigint FROM pg_cluster_state
	   WHERE category = 'undo_cleaner' AND key = 'undo_cleaner_main_loop_iters'});
ok($node->poll_query_until('postgres',
		qq{SELECT value::bigint > $iters_before FROM pg_cluster_state
		   WHERE category = 'undo_cleaner' AND key = 'undo_cleaner_main_loop_iters'}),
   'L6 undo_cleaner_main_loop_iters grows (live main loop)');


# ----------
# L8: clean shutdown is a normal exit — no crash-recovery markers.
# ----------
$node->stop;
my $log_l8 = slurp_file($node->logfile);
like($log_l8, qr/database system is shut down/,
	 'L8 clean shutdown completes (pg_ctl stop -m fast)');
unlike($log_l8,
	   qr/HandleChildCrash|terminating any other active server processes/,
	   'L8 cleaner normal exit does NOT trigger crash recovery (HC5)');


# ============================================================
# Node B: restart_after_crash=on — L9 crash respawn cycle.
# ============================================================
my $node_b = PgracClusterNode->new('crashloop');
$node_b->init;
$node_b->append_conf('postgresql.conf', "restart_after_crash = on\n");
$node_b->start;

ok($node_b->poll_query_until('postgres',
		q{SELECT count(*) = 1 FROM pg_stat_activity WHERE backend_type = 'undo cleaner'}),
   'L9 cleaner up on crashloop node');
my $pid_initial = $node_b->safe_psql('postgres',
	q{SELECT pid FROM pg_stat_activity WHERE backend_type = 'undo cleaner'});
ok($pid_initial =~ /^\d+$/, 'L9 captured initial cleaner pid');

kill 'KILL', $pid_initial;

# Crash recovery cycles the whole instance (aux crash => HandleChildCrash),
# then ServerLoop respawns the cleaner with a fresh pid.
ok($node_b->poll_query_until('postgres',
		qq{SELECT count(*) = 1 FROM pg_stat_activity
		    WHERE backend_type = 'undo cleaner' AND pid <> $pid_initial}),
   'L9 cleaner respawned with a NEW pid after kill -9 + crash recovery');
ok($node_b->poll_query_until('postgres',
		q{SELECT value = 'ready' FROM pg_cluster_state
		   WHERE category = 'undo_cleaner' AND key = 'undo_cleaner_status'}),
   'L9 respawned cleaner republishes ready (F16 incarnation refresh)');
my $phase_after = $node_b->safe_psql('postgres',
	"SELECT value FROM pg_cluster_state WHERE category = 'phase' AND key = 'cluster_phase'");
is($phase_after, 'running', 'L9 post-crash cluster phase recovers to running');
$node_b->stop;


# ============================================================
# Node C: cluster.enabled=off — L10 HC4 gate.
# ============================================================
my $node_c = PgracClusterNode->new('disabled');
$node_c->init;
$node_c->append_conf('postgresql.conf',
	"log_min_messages = debug1\ncluster.enabled = off\n");
$node_c->start;

my $cleaner_off = $node_c->safe_psql('postgres',
	q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'undo cleaner'});
is($cleaner_off, '0', 'L10 cluster.enabled=off spawns no undo cleaner (HC4)');
$node_c->stop;


done_testing();
