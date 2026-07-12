#-------------------------------------------------------------------------
#
# 015_inject.pl
#    End-to-end regression for the cluster error-injection framework
#    introduced at stage 0.27.
#
#    Verifies the SQL surface backed by cluster_inject.c:
#
#      - pg_stat_cluster_injections view exists, returns six rows
#        (matches the compile-time registry).
#      - Column types are (text, text, int8, int8).
#      - Names match the spec-0.27 §2.3 list.
#      - cluster_inject_fault arms / disarms a named point in one
#        backend session; the view immediately reflects the change.
#      - Unknown name -> WARNING + RETURN false; no arm.
#      - Non-superuser arming -> ERROR.
#      - cluster.injection_points GUC auto-arms the named points at
#        backend startup; startup-fired injection points then have
#        hits >= 1 because cluster_init_shmem traverses them while
#        the global armed_count is non-zero.
#      - 0.16 baseline view (pg_stat_cluster_wait_events 51 rows) is
#        not perturbed by the 0.27 framework addition.
#
#    Per-backend semantics note: cluster_inject_fault arms state is
#    process-local atomic memory, not shmem.  Tests that arm + check
#    must therefore run in the same psql session (multi-statement
#    safe_psql) so the new connection's backend sees its own arm
#    state.  See spec-0.27-error-injection.md §3.6.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/015_inject.pl
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
use Test::More;
use PgracClusterNode;


my $node = PgracClusterNode->new('main');
$node->init;
$node->start;


# ----------
# Test 1: View exists and returns the compile-time registry size.
# ----------
is( $node->safe_psql('postgres',
		'SELECT count(*) FROM pg_stat_cluster_injections'),
	'171',
	'pg_stat_cluster_injections returns 171 rows (gcs-race-fix-2 +1 cluster-gcs-block-done-drop; gcs-race-fix +1 cluster-gcs-block-duplicate-grant-reply; spec-5.22d Hardening +1 cluster-undo-authority-scan; spec-5.22e D5-7 +2 horizon report-drop/epoch-fence; spec-5.22d D4-8 +1 authority-block0-prove; spec-7.3 review P1-1 +1 cluster-lms-cr-fence-recheck; spec-7.1 D3-a +1 cluster-mxid-halfspace-hard-limit; spec-7.3 D7 +1 cluster-lms-cr-fence-refuse; spec-7.2 D6 +2 cluster-lms-data-dispatch + cluster-lms-conn-reset; spec-6.14 D5+D8 +3; spec-5.6a +1; spec-6.12e2 +1 cluster-gcs-block-bast-nudge; spec-6.15 D3 +2 cluster-xid-herding-stall + cluster-xid-window-hard-limit; spec-6.12i +1 cluster-lms-undo-fetch; spec-6.12b +1 cluster-lms-cr-construct; spec-6.12a ㉕ +1 cluster-gcs-block-remote-downgrade; spec-5.18 +7 cluster-node-remove-*; spec-5.13 +6 cluster-clean-leave-*; spec-5.13 Hardening v1.0.3 +1 cluster-clean-leave-survivor-suppress-preflight-ack; spec-5.53 +1 cluster-cr-skip-epoch-bump; spec-5.7 +1 cluster-ko-peer-skip-ack; spec-2.41 +1 cluster-gcs-block-stale-ship; spec-5.8 +1 cluster-lmd-force-partial-round; spec-5.55 Hardening v1.1 +1 cluster-cr-resolver-memo-suspect; spec-5.15 Hardening v1.1 +1 cluster-reconfig-join-commit-marker-durable; spec-2.29a +1 cluster-qvotec-marker-service-hold)');


# ----------
# Test 2: Column types are (text, text, int8, int8).
# ----------
is( $node->safe_psql(
		'postgres', q{
	SELECT string_agg(format_type(atttypid, atttypmod), ',' ORDER BY attnum)
	  FROM pg_attribute
	 WHERE attrelid = 'pg_stat_cluster_injections'::regclass
	   AND attnum > 0 AND NOT attisdropped
}),
	'text,text,bigint,bigint',
	'pg_stat_cluster_injections columns are (name text, fault_type text, param int8, hits int8)');


# ----------
# Test 3: Names match the spec-0.30 + spec-1.1-1.3 + spec-1.7 +
# spec-1.10 list (45 entries: 6 baseline + 8 stage-0.30 sweep + 3
# stage-1.1 shared_fs + 3 stage-1.2 cluster_smgr + 4 stage-1.3 shmem
# registry + 4 stage-1.7 PCM lock + 17 stage-1.10 startup phase
# machinery).
# ----------
is( $node->safe_psql(
		'postgres',
		'SELECT string_agg(name, \',\' ORDER BY name) FROM pg_stat_cluster_injections'
	),
	'cluster-boc-event-publish,cluster-catalog-services-ready-force-closed,cluster-clean-leave-barrier-complete,cluster-clean-leave-escalate-to-failstop,cluster-clean-leave-gcs-flushed,cluster-clean-leave-ges-drained,cluster-clean-leave-quiesce-pre,cluster-clean-leave-request,cluster-clean-leave-survivor-suppress-preflight-ack,cluster-clean-xfer-stale-holder,cluster-collision-detect,cluster-conf-load-success,cluster-conf-parse-fail,cluster-conf-shmem-init,cluster-cr-resolver-memo-suspect,cluster-cr-skip-epoch-bump,cluster-cssd-main-loop-pre-tick,cluster-cssd-mark-peer-dead,cluster-cssd-post-spawn,cluster-cssd-pre-spawn,cluster-cssd-ready-publish,cluster-cssd-shutdown-post,cluster-cssd-shutdown-pre,cluster-debug-dump-entry,cluster-diag-main-loop-iter,cluster-diag-post-spawn,cluster-diag-pre-spawn,cluster-diag-ready-publish,cluster-diag-shutdown-post,cluster-diag-shutdown-pre,cluster-fence-post-thaw-broadcast,cluster-fence-pre-freeze-broadcast,cluster-fence-pre-self-fence-shutdown,cluster-gcs-block-bast-nudge,cluster-gcs-block-drop-reply-before-send,cluster-gcs-block-duplicate-grant-reply,cluster-gcs-block-evict-holder-before-ship,cluster-gcs-block-force-epoch-stale-reply,cluster-gcs-block-forward-master-side,cluster-gcs-block-invalidate-drop-broadcast,cluster-gcs-block-invalidate-stall-ack,cluster-gcs-block-remote-downgrade,cluster-gcs-block-stale-ship,cluster-gcs-block-starvation-force-denied,cluster-gcs-block-x-forward-master-side,cluster-grd-redeclare-skip,cluster-guc-init-pre-define,cluster-ic-mock-send-pre-enqueue,cluster-ic-tier-selected,cluster-init-post-shmem,cluster-init-pre-shmem,cluster-init-top,cluster-ko-peer-skip-ack,cluster-lck-main-loop-iter,cluster-lck-post-spawn,cluster-lck-pre-spawn,cluster-lck-ready-publish,cluster-lck-shutdown-post,cluster-lck-shutdown-pre,cluster-lmd-force-partial-round,cluster-lmon-main-loop-iter,cluster-lmon-post-spawn,cluster-lmon-pre-spawn,cluster-lmon-ready-publish,cluster-lmon-shutdown-post,cluster-lmon-shutdown-pre,cluster-lms-conn-reset,cluster-lms-cr-construct,cluster-lms-cr-fence-recheck,cluster-lms-cr-fence-refuse,cluster-lms-data-dispatch,cluster-lms-undo-fetch,cluster-mxid-halfspace-hard-limit,cluster-node-remove-cleanup-done,cluster-node-remove-escalate,cluster-node-remove-fence-armed,cluster-node-remove-precheck,cluster-node-remove-request,cluster-node-remove-shrink-committed,cluster-node-remove-shrink-committing,cluster-pcm-acquire-entry,cluster-pcm-convert-pre,cluster-pcm-downgrade-pre,cluster-pcm-release-pre,cluster-pgstat-mirror-sync,cluster-quorum-loss-broadcast,cluster-qvotec-marker-service-hold,cluster-qvotec-poll-post,cluster-qvotec-poll-pre,cluster-reconfig-broadcast-procsig-pre,cluster-reconfig-decide-coordinator,cluster-reconfig-epoch-bump-pre,cluster-reconfig-join-commit-marker-durable,cluster-reconfig-tick-entry,cluster-recovery-anchor-force-failclosed,cluster-relmap-crash-after-stage,cluster-relmap-crash-before-publish,cluster-run-shutdown-top,cluster-run-startup-top,cluster-scn-abort-post-advance,cluster-scn-abort-pre-advance,cluster-scn-advance-post,cluster-scn-advance-pre,cluster-scn-boc-sweep-post,cluster-scn-boc-sweep-pre,cluster-scn-commit-post-advance,cluster-scn-commit-pre-advance,cluster-scn-observe-bump-pre,cluster-scn-observe-entry,cluster-scn-replay-observe-pre,cluster-scn-wal-write-pre,cluster-scn-wraparound-warning,cluster-shared-fs-backend-register,cluster-shared-fs-init-top,cluster-shared-fs-local-open,cluster-shmem-region-init-post,cluster-shmem-region-init-pre,cluster-shmem-register-region,cluster-shmem-request,cluster-shmem-views-srf-entry,cluster-shutdown-top,cluster-sinval-ack-drop-send,cluster-sinval-ack-skip-validate,cluster-sinval-broadcast-drop-send,cluster-sinval-receive-skip-validate,cluster-smgr-create-top,cluster-smgr-open-top,cluster-smgr-which-decision,cluster-startup-phase-0-enter,cluster-startup-phase-0-exit,cluster-startup-phase-0-fail,cluster-startup-phase-1-enter,cluster-startup-phase-1-exit,cluster-startup-phase-1-fail,cluster-startup-phase-2-enter,cluster-startup-phase-2-exit,cluster-startup-phase-2-fail,cluster-startup-phase-3-enter,cluster-startup-phase-3-exit,cluster-startup-phase-3-fail,cluster-startup-phase-4-enter,cluster-startup-phase-4-exit,cluster-startup-phase-4-fail,cluster-stats-main-loop-iter,cluster-stats-post-spawn,cluster-stats-pre-spawn,cluster-stats-ready-publish,cluster-stats-shutdown-post,cluster-stats-shutdown-pre,cluster-thread-recovery-drive,cluster-undo-authority-block0-prove,cluster-undo-authority-scan,cluster-undo-horizon-epoch-fence,cluster-undo-horizon-report-drop,cluster-views-srf-entry,cluster-voting-disk-write-fail,cluster-wal-page-init-thread-id,cluster-wal-state-ensure-pre,cluster-wal-state-write-fail,cluster-wal-thread-claim-create-fail,cluster-wal-thread-validate-pre,cluster-xid-herding-stall,cluster-xid-window-hard-limit,cr_construct_delay_us,cr_corruption,cr_cross_instance,cr_force_read_scn,cr_snapshot_too_old,undo-force-wal-before-data-violation,undo-skip-checkpoint-flush-one',
	'170 injection point names match the full registry (gcs-race-fix +1 cluster-gcs-block-duplicate-grant-reply; spec-5.22d Hardening +1 cluster-undo-authority-scan; spec-5.22e D5-7 +2 cluster-undo-horizon-report-drop + cluster-undo-horizon-epoch-fence; spec-5.22d D4-8 +1 cluster-undo-authority-block0-prove; spec-7.3 review P1-1 +1 cluster-lms-cr-fence-recheck; spec-7.1 D3-a +1 cluster-mxid-halfspace-hard-limit; spec-7.3 D7 +1 cluster-lms-cr-fence-refuse; spec-7.2 D6 +2 cluster-lms-data-dispatch + cluster-lms-conn-reset; spec-6.14 D5+D8 +3; spec-5.6a +1; spec-6.12e2 +1 cluster-gcs-block-bast-nudge; spec-6.15 D3 +2 cluster-xid-herding-stall + cluster-xid-window-hard-limit; spec-6.12i +1 cluster-lms-undo-fetch; spec-6.12b +1 cluster-lms-cr-construct; spec-6.12a ㉕ +1 cluster-gcs-block-remote-downgrade; spec-5.13 +6 cluster-clean-leave-*; spec-5.13 Hardening v1.0.3 +1 cluster-clean-leave-survivor-suppress-preflight-ack; spec-5.53 +1 cluster-cr-skip-epoch-bump; spec-5.7 +1 cluster-ko-peer-skip-ack; spec-2.41 +1 cluster-gcs-block-stale-ship; spec-5.8 +1 cluster-lmd-force-partial-round; spec-5.55 Hardening v1.1 +1 cluster-cr-resolver-memo-suspect; spec-5.15 Hardening v1.1 +1 cluster-reconfig-join-commit-marker-durable; spec-2.29a +1 cluster-qvotec-marker-service-hold)');


# ----------
# Test 4: arm WARNING + view in same backend reflects fault_type='warning'.
# Single psql session so the per-backend arm state is observable.
# ----------
is( $node->safe_psql(
		'postgres', q{
	SELECT cluster_inject_fault('cluster-conf-load-success', 'warning', 0);
	SELECT fault_type FROM pg_stat_cluster_injections
	 WHERE name='cluster-conf-load-success';
}),
	"t\nwarning",
	'arm WARNING -> view fault_type = warning (same backend)');


# ----------
# Test 5: disarm restores fault_type='none' (same backend).
# ----------
is( $node->safe_psql(
		'postgres', q{
	SELECT cluster_inject_fault('cluster-init-pre-shmem', 'error', 0);
	SELECT cluster_inject_fault('cluster-init-pre-shmem', 'none', 0);
	SELECT fault_type FROM pg_stat_cluster_injections
	 WHERE name='cluster-init-pre-shmem';
}),
	"t\nt\nnone",
	'disarm (none) -> view fault_type = none (same backend)');


# ----------
# Test 6: arm with unknown name returns false (and emits a WARNING).
# ----------
my ($stdout6, $stderr6);
$node->psql(
	'postgres',
	q{SELECT cluster_inject_fault('not-a-real-point', 'warning', 0)},
	stdout => \$stdout6,
	stderr => \$stderr6);
like($stderr6, qr/unknown cluster injection point/i,
	'arm with unknown name -> WARNING in stderr');
is($stdout6, 'f', 'arm with unknown name returns false');


# ----------
# Test 7: non-superuser cannot arm.  Create a regular user and confirm
# the SRF raises permission_denied.
# ----------
$node->safe_psql('postgres',
	"CREATE ROLE regular LOGIN NOSUPERUSER;\n"
	  . "GRANT EXECUTE ON FUNCTION cluster_inject_fault(text, text, int8) TO regular;");

my ($stdout7, $stderr7);
$node->psql(
	'postgres',
	q{SET ROLE regular; SELECT cluster_inject_fault('cluster-init-pre-shmem', 'warning', 0)},
	stdout => \$stdout7,
	stderr => \$stderr7);
like($stderr7, qr/must be superuser/i,
	'non-superuser arming -> ERROR (must be superuser)');


# ----------
# Test 8: cluster.injection_points GUC auto-arm.  Set the GUC, restart
# the node, and verify the named points show fault_type='warning' in a
# brand-new backend.
# ----------
$node->stop;
$node->append_conf('postgresql.conf',
	"cluster.injection_points = 'cluster-init-pre-shmem,cluster-conf-load-success'\n");
$node->start;

is( $node->safe_psql('postgres',
		q{SELECT string_agg(name, ',' ORDER BY name)
		    FROM pg_stat_cluster_injections
		   WHERE fault_type='warning'}),
	'cluster-conf-load-success,cluster-init-pre-shmem',
	'cluster.injection_points GUC auto-arms listed names with fault_type=warning');


# ----------
# Test 9: with the GUC arm in place, startup-fired injection points
# (which run inside cluster_init_shmem) accumulate hits.  Each new
# psql connection forks a backend that runs cluster_init_shmem with
# armed_count > 0, so the in-process hit counters increment for the
# armed names.  Lifetime hit counter is per-process; the value
# observed in this view query reflects this connection's backend.
# ----------
my $hits = $node->safe_psql('postgres',
	q{SELECT hits FROM pg_stat_cluster_injections WHERE name='cluster-init-pre-shmem'}
);
ok($hits >= 1,
	"cluster-init-pre-shmem hit during startup with GUC arm (hits=$hits)");


# ----------
# Test 10: baseline regression -- the 0.16 view row count is not
# perturbed by the 0.27 framework addition.
# ----------
is( $node->safe_psql('postgres',
		'SELECT count(*) FROM pg_stat_cluster_wait_events'),
	'123',
	'pg_stat_cluster_wait_events returns 123 rows (spec-7.2 LMS data-plane waits + spec-5.22b D2-6 undo-block grant-plane waits; merge sum 118+2+3)');

# ----------
# Test 11 (Hardening v1.0.1 / codex review P2-2): SQL SRF rejects
# unknown fault types.  Pre-Hardening these silently mapped to
# CLUSTER_FAULT_NONE and returned true, masking misconfigured tests.
# ----------
my ($u_stdout, $u_stderr);
$node->psql(
	'postgres',
	q{SELECT cluster_inject_fault('cluster-init-pre-shmem', 'definitely-not-a-real-type', 0)},
	stdout => \$u_stdout,
	stderr => \$u_stderr);
like($u_stderr, qr/unknown cluster injection fault type/i,
	'unknown fault type rejected by SQL SRF (P2-2 enforce)');


# ----------
# Test 12: SQL SRF rejects negative sleep param.
# ----------
my ($n_stdout, $n_stderr);
$node->psql(
	'postgres',
	q{SELECT cluster_inject_fault('cluster-init-pre-shmem', 'sleep', -1)},
	stdout => \$n_stdout,
	stderr => \$n_stderr);
like($n_stderr, qr/sleep param must be >= 0/i,
	'negative sleep param rejected by SQL SRF (P2-2 enforce)');


# ----------
# Test 13: SQL SRF rejects sleep param > 1 hour cap.
# ----------
my ($l_stdout, $l_stderr);
$node->psql(
	'postgres',
	q{SELECT cluster_inject_fault('cluster-init-pre-shmem', 'sleep', 3600000001)},
	stdout => \$l_stdout,
	stderr => \$l_stderr);
like($l_stderr, qr/exceeds 1-hour cap/i,
	'sleep param above 1-hour cap rejected by SQL SRF (P2-2 enforce)');


# ----------
# Test 14 (Hardening v1.0.2 D-I4 / codex review P2 post-Sprint B): the
# GUC parser path (cluster.injection_points='name:sleep:param') must
# enforce the same sleep param validation as the SQL SRF.  Pre-v1.0.2
# the GUC parser only checked for unknown fault types and let any
# numeric param pass straight to pg_usleep (uint64 wrap-around for
# negative values; multi-second blocking for huge values).
# ----------
$node->stop;
unlink $node->logfile;
$node->adjust_conf('postgresql.conf', 'cluster.injection_points',
	"'cluster-init-pre-shmem:sleep:-1'");
$node->start;
my $g_log_neg = PostgreSQL::Test::Utils::slurp_file($node->logfile);
like($g_log_neg, qr/sleep param must be >= 0/i,
	'GUC parser rejects negative sleep param (D-I4 / shared validate_fault_param)');


# ----------
# Test 15: GUC parser rejects sleep param > 1 hour cap.
# ----------
$node->stop;
unlink $node->logfile;
$node->adjust_conf('postgresql.conf', 'cluster.injection_points',
	"'cluster-init-pre-shmem:sleep:3600000001'");
$node->start;
my $g_log_large = PostgreSQL::Test::Utils::slurp_file($node->logfile);
like($g_log_large, qr/exceeds 1-hour cap/i,
	'GUC parser rejects sleep param above 1-hour cap (D-I4)');


# ----------
# Test 16: GUC parser still rejects unknown fault type (regression
# guard -- pre-v1.0.2 this WARNING already existed but moved into
# shared validate_fault_param helper).
# ----------
$node->stop;
unlink $node->logfile;
$node->adjust_conf('postgresql.conf', 'cluster.injection_points',
	"'cluster-init-pre-shmem:definitely-not-a-real-type'");
$node->start;
my $g_log_unk = PostgreSQL::Test::Utils::slurp_file($node->logfile);
like($g_log_unk, qr/unknown cluster injection fault type/i,
	'GUC parser rejects unknown fault type (D-I4 regression guard)');


# Restore the GUC for downstream consumers / clean shutdown.
$node->stop;
$node->adjust_conf('postgresql.conf', 'cluster.injection_points', "''");
$node->start;
$node->stop;

done_testing();
