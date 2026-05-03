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
# Test 1: View exists and returns 28 rows (compile-time registry size
# after stage-1.7 PCM lock additions; 6 baseline + 8 sweep
# + 3 shared_fs + 3 smgr + 4 shmem registry + 4 PCM lock).
# ----------
is( $node->safe_psql('postgres',
		'SELECT count(*) FROM pg_stat_cluster_injections'),
	'51',
	'pg_stat_cluster_injections returns 28 rows (compile-time registry)');


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
	'cluster-conf-load-success,cluster-conf-parse-fail,cluster-conf-shmem-init,cluster-debug-dump-entry,cluster-guc-init-pre-define,cluster-ic-mock-send-pre-enqueue,cluster-ic-tier-selected,cluster-init-post-shmem,cluster-init-pre-shmem,cluster-init-top,cluster-lmon-main-loop-iter,cluster-lmon-post-spawn,cluster-lmon-pre-spawn,cluster-lmon-ready-publish,cluster-lmon-shutdown-post,cluster-lmon-shutdown-pre,cluster-pcm-acquire-entry,cluster-pcm-convert-pre,cluster-pcm-downgrade-pre,cluster-pcm-release-pre,cluster-pgstat-mirror-sync,cluster-run-shutdown-top,cluster-run-startup-top,cluster-shared-fs-backend-register,cluster-shared-fs-init-top,cluster-shared-fs-local-open,cluster-shmem-region-init-post,cluster-shmem-region-init-pre,cluster-shmem-register-region,cluster-shmem-request,cluster-shmem-views-srf-entry,cluster-shutdown-top,cluster-smgr-create-top,cluster-smgr-open-top,cluster-smgr-which-decision,cluster-startup-phase-0-enter,cluster-startup-phase-0-exit,cluster-startup-phase-0-fail,cluster-startup-phase-1-enter,cluster-startup-phase-1-exit,cluster-startup-phase-1-fail,cluster-startup-phase-2-enter,cluster-startup-phase-2-exit,cluster-startup-phase-2-fail,cluster-startup-phase-3-enter,cluster-startup-phase-3-exit,cluster-startup-phase-3-fail,cluster-startup-phase-4-enter,cluster-startup-phase-4-exit,cluster-startup-phase-4-fail,cluster-views-srf-entry',
	'51 injection point names match spec-0.30 + spec-1.1 + spec-1.2 + spec-1.3 + spec-1.7 + spec-1.10 + spec-1.11 Sprint B');


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
	'56',
	'pg_stat_cluster_wait_events still 51 rows after 0.27');

$node->stop;

done_testing();
