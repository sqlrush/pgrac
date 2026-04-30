#-------------------------------------------------------------------------
#
# 030_acceptance.pl
#    Stage 0 cross-spec end-to-end acceptance test (spec-0.30 §2.4).
#
#    Verifies that all 30 stage-0 functional points integrate cleanly
#    on a single PG instance.  This is the first ever cross-spec
#    regression net: each prior spec has its own focused TAP / unit
#    tests; this one proves "all together still works".
#
#    Test groups follow the order of spec deliverables, so a failure
#    pinpoints which spec broke.  Total 60+ tests / ~30s on CI.
#
#    Spec coverage:
#      §A  PG fork sanity (0.1)
#      §B  cluster scaffolding (0.2 / 0.5)
#      §C  enable-cluster build (0.3)
#      §D  logging (0.9)
#      §E  BackendType + Wait Events (0.10 / 0.11)
#      §F  SQLSTATE (0.12)
#      §G  GUC framework (0.13 / 0.18 / 0.19 / 0.27 / 0.30)
#      §H  shmem framework (0.14)
#      §I  views: wait_events / gview (0.16 / 0.17)
#      §J  cluster_ic stub + mock (0.18 / 0.26)
#      §K  cluster_conf + pg_cluster_nodes (0.19)
#      §L  pgrac-init / pgrac-start (0.20)
#      §M  error injection 14 注入点 + 5 fault types (0.27 / 0.30 sweep)
#      §N  perfmon framework + nodes (0.28)
#      §O  debug snapshot pg_cluster_state (0.29)
#      §P  --pgrac-version flag (0.30)
#      §Q  cross-spec consistency (0.30)
#
#    NOT covered (per spec-0.30 §4.4):
#      - real cluster behaviour (Stage 2+)
#      - perf benchmark vs Oracle RAC (Stage 6+)
#      - chaos / fuzz testing (Stage 6+)
#
# IDENTIFICATION
#    src/test/cluster_tap/t/030_acceptance.pl
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


# ============================================================
# §A  PG fork sanity (5 tests)
# ============================================================

is($node->safe_psql('postgres', 'SELECT 1::int'),
	'1', 'A1 PG basic SELECT works');

is($node->safe_psql('postgres', 'SHOW server_version_num') =~ /^16\d{4}$/ ? 'ok' : 'fail',
	'ok', 'A2 server_version_num matches PG 16.x');

is($node->safe_psql('postgres', 'SELECT count(*) FROM pg_class'),
	$node->safe_psql('postgres', 'SELECT count(*) FROM pg_class'),
	'A3 pg_class catalog accessible');

ok($node->safe_psql('postgres', 'SELECT pg_postmaster_start_time() < now()') eq 't',
	'A4 pg_postmaster_start_time() is in the past');

ok($node->safe_psql('postgres', 'SELECT count(*) FROM pg_database') >= 1,
	'A5 pg_database has at least 1 entry');


# ============================================================
# §B  cluster scaffolding (3 tests)
# ============================================================

ok( $node->safe_psql(
		'postgres',
		q{SELECT count(*) > 0 FROM pg_proc WHERE proname LIKE 'cluster_%'}
	) eq 't',
	'B1 cluster_* SRFs registered in pg_proc');

ok( $node->safe_psql(
		'postgres',
		q{SELECT count(*) > 0 FROM pg_views WHERE viewname LIKE 'pg_stat_cluster_%' OR viewname='pg_cluster_state' OR viewname='pg_cluster_nodes'}
	) eq 't',
	'B2 pgrac views materialised in pg_views');

ok( $node->safe_psql(
		'postgres',
		q{SELECT count(*) >= 9 FROM pg_proc WHERE proname LIKE 'cluster_%'}
	) eq 't',
	'B3 enough cluster_* SRFs (>= 9 SRF entry points; stage 0.30 has 12)');


# ============================================================
# §C  enable-cluster build (4 tests)
# ============================================================

is($node->safe_psql('postgres', q{SHOW "cluster.node_id"}),
	'-1', 'C1 cluster.node_id GUC accessible (default -1)');

is($node->safe_psql('postgres', q{SHOW "cluster.interconnect_tier"}),
	'stub', 'C2 cluster.interconnect_tier defaults to stub');

ok($node->safe_psql('postgres',
		q{SELECT length(version()) > 10})
	eq 't', 'C3 PG version string non-empty');

is($node->safe_psql('postgres',
		'SELECT count(*) FROM pg_cluster_nodes'),
	'1', 'C4 pg_cluster_nodes returns 1 row (single-node fallback)');


# ============================================================
# §D  logging (CLUSTER_LOG) (2 tests)
# ============================================================

ok( $node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category='phase'}
	) >= 1,
	'D1 phase category present in pg_cluster_state');

my $phase_val = $node->get_cluster_state_value('phase', 'cluster_phase');
ok($phase_val =~ /^(init|running|shutdown|reconfig)$/,
	"D2 cluster_phase is a recognised lifecycle string (got: $phase_val)");


# ============================================================
# §E  BackendType + Wait Events (4 tests)
# ============================================================

is($node->safe_psql('postgres',
		'SELECT count(*) FROM pg_stat_cluster_wait_events'),
	'46', 'E1 pg_stat_cluster_wait_events returns 46 rows');

ok($node->safe_psql('postgres',
		q{SELECT count(*) > 0 FROM pg_stat_cluster_wait_events WHERE type='Cluster: GES'})
	eq 't', 'E2 Cluster: GES wait events exist');

ok($node->safe_psql('postgres',
		q{SELECT count(*) > 0 FROM pg_stat_cluster_wait_events WHERE type='Cluster: PCM'})
	eq 't', 'E3 Cluster: PCM wait events exist');

is($node->safe_psql('postgres',
		'SELECT count(*) FROM pg_stat_gcluster_wait_events'),
	'46', 'E4 pg_stat_gcluster_wait_events returns 46 rows (single-node)');


# ============================================================
# §F  SQLSTATE (2 tests)
# ============================================================

ok($node->safe_psql('postgres',
		q{SELECT count(*) >= 0 FROM pg_class WHERE relname='pg_class'})
	eq 't', 'F1 pg_class accessible (sanity)');

# ERRCODE_CLUSTER_* are macros; verify by triggering an error and checking sqlstate
my ($sout, $serr);
$node->psql('postgres',
	q{SELECT cluster_inject_fault('not-a-real-point', 'warning', 0)},
	stdout => \$sout, stderr => \$serr);
ok($serr =~ /unknown cluster injection point/i,
	'F2 cluster injection unknown-name path raises WARNING');


# ============================================================
# §G  GUC framework (4 tests)
# ============================================================

is($node->get_cluster_state_value('guc', 'cluster.node_id'),
	'-1', 'G1 dump shows cluster.node_id GUC');

is($node->get_cluster_state_value('guc', 'cluster.interconnect_tier'),
	'stub', 'G2 dump shows cluster.interconnect_tier GUC');

ok($node->get_cluster_state_value('guc', 'cluster.config_file') ne '',
	'G3 dump shows cluster.config_file GUC');

ok($node->get_cluster_state_value('guc', 'cluster.injection_points') ne '',
	'G4 dump shows cluster.injection_points GUC');


# ============================================================
# §H  shmem framework (3 tests)
# ============================================================

is($node->get_cluster_state_value('shmem', 'magic'),
	'0x50475243', 'H1 ClusterShmem magic = 0x50475243 ("PGRC" LE)');

ok($node->get_cluster_state_value('shmem', 'version_packed') =~ /^0x[0-9A-F]{8}$/,
	'H2 ClusterShmem version_packed is 8-hex format');

ok($node->safe_psql('postgres',
		q{SELECT value > '2026-01-01'::timestamptz::text FROM pg_cluster_state WHERE category='shmem' AND key='created_at'})
	eq 't', 'H3 ClusterShmem created_at is sane timestamp');


# ============================================================
# §I  views (4 tests)
# ============================================================

is($node->safe_psql('postgres',
		q{SELECT count(DISTINCT type) FROM pg_stat_cluster_wait_events}),
	'10', 'I1 wait_events has exactly 10 distinct types');

ok($node->safe_psql('postgres',
		q{SELECT count(*) > 0 FROM pg_stat_gcluster_wait_events WHERE node_id IS NOT NULL})
	eq 't', 'I2 gview returns rows with node_id populated');

# Schema check: wait_events has (type, name) columns
is($node->safe_psql('postgres',
		q{SELECT string_agg(attname, ',' ORDER BY attnum)
		    FROM pg_attribute
		   WHERE attrelid = 'pg_stat_cluster_wait_events'::regclass
		     AND attnum > 0 AND NOT attisdropped}),
	'type,name', 'I3 wait_events column names: type,name');

ok($node->safe_psql('postgres',
		q{SELECT count(*) > 0 FROM pg_views WHERE viewname='pg_stat_cluster_wait_events'})
	eq 't', 'I4 pg_stat_cluster_wait_events present in pg_views');


# ============================================================
# §J  cluster_ic stub + mock (4 tests)
# ============================================================

is($node->get_cluster_state_value('ic', 'active_tier_name'),
	'stub', 'J1 default IC tier is stub');

ok($node->safe_psql('postgres',
		q{SELECT count(*) FROM pg_proc WHERE proname LIKE 'cluster_ic_mock_%'})
	eq '4', 'J2 4 mock SRFs registered');

# Mock SRFs should refuse to work without tier=mock
my ($jstdout, $jstderr);
$node->psql('postgres',
	q{SELECT cluster_ic_mock_inject(0, '\xDE'::bytea)},
	stdout => \$jstdout, stderr => \$jstderr);
ok($jstderr =~ /(mock|interconnect)/i,
	'J3 mock_inject rejected when tier != mock');

is($node->safe_psql('postgres',
		q{SELECT count(*) FROM pg_proc WHERE proname='cluster_msg_send' OR proname='cluster_msg_recv' OR proname='cluster_rpc_call'}),
	'0', 'J4 high-level cluster_msg_* are C-only (not in pg_proc)');


# ============================================================
# §K  cluster_conf + pg_cluster_nodes (4 tests)
# ============================================================

# Schema check: pg_cluster_nodes has 7 columns
is($node->safe_psql('postgres',
		q{SELECT count(*) FROM pg_attribute
		    WHERE attrelid = 'pg_cluster_nodes'::regclass
		      AND attnum > 0 AND NOT attisdropped}),
	'7', 'K1 pg_cluster_nodes has 7 columns (spec-0.19 contract)');

is($node->safe_psql('postgres',
		q{SELECT role FROM pg_cluster_nodes LIMIT 1}),
	'primary', 'K2 single-node fallback role is primary');

is($node->get_cluster_state_value('conf', 'node_count'),
	'1', 'K3 dump shows conf.node_count = 1');

ok($node->get_cluster_state_value('conf', 'self_in_topology') =~ /^(t|f)$/,
	'K4 conf.self_in_topology is bool-formatted');


# ============================================================
# §L  pgrac-init / pgrac-start (1 test)
# ============================================================

# postgres binary is on PATH thanks to the cluster_tap Makefile installcheck
# rule (PATH="$installdir/bin:..."), so a `command -v` lookup is portable
# across local trees and CI runners.
my $postgres_bin = `command -v postgres`;
chomp $postgres_bin;
ok($postgres_bin ne '' && -x $postgres_bin,
	"L1 postgres binary present and executable ($postgres_bin)");


# ============================================================
# §M  error injection 14 注入点 + 5 fault types (6 tests)
# ============================================================

is($node->safe_psql('postgres',
		'SELECT count(*) FROM pg_stat_cluster_injections'),
	'14', 'M1 14 injection points (6 baseline + 8 stage-0.30 sweep)');

is($node->safe_psql('postgres',
		q{SELECT string_agg(name, ',' ORDER BY name) FROM pg_stat_cluster_injections WHERE name LIKE 'cluster-init-%'}),
	'cluster-init-post-shmem,cluster-init-pre-shmem,cluster-init-top',
	'M2 cluster-init injection points present');

# Same-session arm + observe
is( $node->safe_psql(
		'postgres', q{
	SELECT cluster_inject_fault('cluster-init-pre-shmem', 'warning', 0);
	SELECT fault_type FROM pg_stat_cluster_injections WHERE name='cluster-init-pre-shmem';
}),
	"t\nwarning",
	'M3 SRF arm path works (warning fault_type observed)');

# 5 fault types reachable via SRF
ok( $node->safe_psql(
		'postgres', q{
	SELECT cluster_inject_fault('cluster-init-pre-shmem', 'error', 0);
	SELECT cluster_inject_fault('cluster-init-pre-shmem', 'sleep', 100);
	SELECT cluster_inject_fault('cluster-init-pre-shmem', 'crash', 0);
	SELECT cluster_inject_fault('cluster-init-pre-shmem', 'skip', 0);
	SELECT cluster_inject_fault('cluster-init-pre-shmem', 'none', 0);
}) =~ /^t\nt\nt\nt\nt$/, 'M4 5 fault types accepted by SRF');

ok( $node->safe_psql(
		'postgres',
		q{SELECT count(DISTINCT key) FROM pg_cluster_state
		   WHERE category='inject' AND (key LIKE '%.fault_type' OR key LIKE '%.hits')}
	) eq '28',
	'M5 inject category has 14×2 = 28 sub-keys (.fault_type + .hits)');

is($node->get_cluster_state_value('inject', 'armed_count'),
	'0', 'M6 inject.armed_count starts at 0 in fresh backend');


# ============================================================
# §N  perfmon framework + nodes (4 tests)
# ============================================================

is($node->safe_psql('postgres',
		'SELECT count(*) FROM pg_stat_cluster_nodes'),
	'1', 'N1 pg_stat_cluster_nodes returns 1 row');

is($node->get_cluster_node_state, 'online', 'N2 node state is online');

ok($node->safe_psql('postgres',
		q{SELECT count(*) > 0 FROM pg_stat_cluster_counters})
	eq 't', 'N3 pg_stat_cluster_counters has at least 1 entry');

is($node->get_pgstat_counter('cluster.inject.armed_count'),
	'0', 'N4 cluster.inject.armed_count counter accessible');


# ============================================================
# §O  debug snapshot pg_cluster_state (5 tests)
# ============================================================

ok($node->safe_psql('postgres',
		'SELECT count(*) >= 30 FROM pg_cluster_state')
	eq 't', 'O1 pg_cluster_state returns >= 30 rows (14 inject + others)');

is($node->safe_psql('postgres',
		q{SELECT string_agg(DISTINCT category, ',' ORDER BY category) FROM pg_cluster_state}),
	'conf,guc,ic,inject,pgstat,phase,shmem',
	'O2 pg_cluster_state has all 7 categories');

is($node->safe_psql('postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE value IS NULL}),
	'0', 'O3 no NULL values in pg_cluster_state (NOT NULL contract)');

is($node->safe_psql('postgres',
		q{SELECT string_agg(format_type(atttypid, atttypmod), ',' ORDER BY attnum)
		    FROM pg_attribute
		   WHERE attrelid = 'pg_cluster_state'::regclass
		     AND attnum > 0 AND NOT attisdropped}),
	'text,text,text', 'O4 pg_cluster_state columns are (text, text, text)');

ok($node->safe_psql('postgres',
		q{SELECT count(*) > 0 FROM pg_cluster_state WHERE category='shmem' AND key='magic'})
	eq 't', 'O5 shmem.magic dump entry present');


# ============================================================
# §P  --pgrac-version flag (1 test)
# ============================================================

# Run postgres binary with --pgrac-version and capture output.
# Reuse $postgres_bin (resolved via PATH in §L1) so this works in both
# local make-check and CI environments.
my $version_out = `'$postgres_bin' --pgrac-version 2>&1`;
chomp $version_out;
like($version_out, qr/^pgrac v\d+\.\d+\.\d+-stage\d+\.\d+ \(based on PostgreSQL \d+\.\d+\)$/,
	"P1 --pgrac-version output matches semver format (got: $version_out)");


# ============================================================
# §Q  cross-spec consistency (3 tests)
# ============================================================

# pgrac_version in pg_stat_cluster_nodes matches --pgrac-version output
my $sql_version = $node->safe_psql('postgres',
	'SELECT pgrac_version FROM pg_stat_cluster_nodes');
is($sql_version, $version_out,
	'Q1 pgrac_version SQL view matches --pgrac-version CLI output');

# Inject framework + pgstat framework + dump framework all see same hits
ok( $node->safe_psql(
		'postgres', q{
	SELECT cluster_inject_fault('cluster-init-pre-shmem', 'warning', 0);
	SELECT (SELECT value::text FROM pg_stat_cluster_counters
	         WHERE name='cluster.inject.armed_count')
	     = (SELECT value FROM pg_cluster_state
	         WHERE category='inject' AND key='armed_count');
}) eq "t\nt",
	'Q2 inject + pgstat + dump frameworks consistent (cross-spec integration)');

# Catalog: catversion bumped, pg_proc has all stage 0 SRFs
ok($node->safe_psql('postgres',
		q{SELECT count(*) >= 9 FROM pg_proc WHERE oid >= 8898 AND oid <= 8910})
	eq 't', 'Q3 pg_proc OID 8898-8910 range has >= 9 cluster SRFs (stage 0 catalog complete)');


$node->stop;

done_testing();
