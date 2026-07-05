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

use IPC::Cmd qw(can_run);
use IPC::Run qw(run);
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
	'118',
	'E1 pg_stat_cluster_wait_events returns 118 rows (spec-6.13 RDMA wait surface)');

ok($node->safe_psql('postgres',
		q{SELECT count(*) > 0 FROM pg_stat_cluster_wait_events WHERE type='Cluster: GES'})
	eq 't', 'E2 Cluster: GES wait events exist');

ok($node->safe_psql('postgres',
		q{SELECT count(*) > 0 FROM pg_stat_cluster_wait_events WHERE type='Cluster: PCM'})
	eq 't', 'E3 Cluster: PCM wait events exist');

is($node->safe_psql('postgres',
		'SELECT count(*) FROM pg_stat_gcluster_wait_events'),
	'118', 'E4 pg_stat_gcluster_wait_events returns 118 rows (single-node, spec-6.13 RDMA wait surface)');


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
# §G  GUC framework (12 tests)
# ============================================================

is($node->get_cluster_state_value('guc', 'cluster.node_id'),
	'-1', 'G1 dump shows cluster.node_id GUC');

is($node->get_cluster_state_value('guc', 'cluster.interconnect_tier'),
	'stub', 'G2 dump shows cluster.interconnect_tier GUC');

ok($node->get_cluster_state_value('guc', 'cluster.config_file') ne '',
	'G3 dump shows cluster.config_file GUC');

ok($node->get_cluster_state_value('guc', 'cluster.injection_points') ne '',
	'G4 dump shows cluster.injection_points GUC');

is($node->get_cluster_state_value('guc', 'cluster.shared_storage_backend'),
	'stub', 'G5 dump shows cluster.shared_storage_backend GUC (default stub, stage 1.1)');

is($node->safe_psql('postgres',
		q{SELECT setting FROM pg_settings WHERE name='cluster.cf_terminal_authority'}),
	'off', 'G6 pg_settings shows cluster.cf_terminal_authority default off');

is($node->get_cluster_state_value('guc', 'cluster.cf_terminal_authority'),
	'f', 'G7 dump shows cluster.cf_terminal_authority GUC');

is($node->get_cluster_state_value('guc', 'cluster.cf_delayed_cleanout'),
	'reader', 'G8 dump shows cluster.cf_delayed_cleanout GUC');

is($node->get_cluster_state_value('guc', 'cluster.smart_fusion_tier_min'),
	'tier3', 'G9 dump shows cluster.smart_fusion_tier_min GUC');

is($node->get_cluster_state_value('guc', 'cluster.smart_fusion'),
	'f', 'G10 dump shows cluster.smart_fusion guarded-off GUC');

my $sf_guard = PostgreSQL::Test::Cluster->new('smart_fusion_guard');
$sf_guard->init;
$sf_guard->append_conf('postgresql.conf', "cluster.smart_fusion = on\n");
is($sf_guard->start(fail_ok => 1), 0,
	'G11 cluster.smart_fusion=on fails closed at startup');
open my $sf_guard_log_fh, '<', $sf_guard->logfile
	or die "open smart_fusion_guard log: $!";
my $sf_guard_log = do { local $/; <$sf_guard_log_fh> };
like($sf_guard_log, qr/cluster\.smart_fusion is fail-closed/i,
	'G12 startup log explains the Smart Fusion guardrail');


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
		'13', 'I1 wait_events has exactly 13 distinct types');

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

# Resolve postgres via IPC::Cmd::can_run (walks PATH in Perl, no shell):
# robust across local make-check and CI runners (Ubuntu /bin/sh = dash
# behaves differently from macOS bash for backtick env propagation).
my $postgres_bin = can_run('postgres');
ok(defined $postgres_bin && -x $postgres_bin,
	"L1 postgres binary present and executable (" . ($postgres_bin // '<undef>') . ")");


# ============================================================
# §M  error injection 112 注入点 + 5 fault types (6 tests)
# ============================================================

is($node->safe_psql('postgres',
		'SELECT count(*) FROM pg_stat_cluster_injections'),
	'155', 'M1 155 injection points (spec-6.14 D8 +1 cluster-catalog-services-ready-force-closed; spec-6.12e2 +1 cluster-gcs-block-bast-nudge; spec-6.15 D3 +2 herding-stall + window-hard-limit; spec-6.12i +1 cluster-lms-undo-fetch; spec-6.12b +1 cluster-lms-cr-construct; spec-6.12a ㉕ +1 cluster-gcs-block-remote-downgrade; spec-5.18 +7 cluster-node-remove-*; spec-5.13 +6 cluster-clean-leave-*; spec-5.13 Hardening v1.0.3 +1 clean-leave-survivor-suppress-preflight-ack; spec-2.41 +1 gcs-block-stale-ship; spec-5.7 D6 +1 ko-peer-skip-ack; spec-5.2a +1 clean-xfer stale-holder; spec-4.8ab +2 undo boundary guards; spec-5.55 Hardening v1.1 +1 cluster-cr-resolver-memo-suspect; spec-5.15 Hardening v1.1 +1 cluster-reconfig-join-commit-marker-durable)');

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
	) eq '310',
	'M5 inject category has 155×2 = 310 sub-keys (.fault_type + .hits; spec-6.14 D8 +1 cluster-catalog-services-ready-force-closed; spec-6.12e2 +1 cluster-gcs-block-bast-nudge; spec-6.15 D3 +2 herding-stall + window-hard-limit; spec-6.12i +1 cluster-lms-undo-fetch; spec-6.12b +1 cluster-lms-cr-construct; spec-6.12a ㉕ +1 cluster-gcs-block-remote-downgrade; spec-5.13 +6 cluster-clean-leave-*; spec-2.41 +1 gcs-block-stale-ship; spec-5.55 Hardening v1.1 +1 cluster-cr-resolver-memo-suspect; spec-5.15 Hardening v1.1 +1 cluster-reconfig-join-commit-marker-durable)');

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
	eq 't', 'O1 pg_cluster_state returns >= 30 rows (20 inject + others)');

is($node->safe_psql('postgres',
		q{SELECT string_agg(DISTINCT category, ',' ORDER BY category) FROM pg_cluster_state}),
	'advisory,block_format,buffer_format,catalog,cf,cluster_cssd,cluster_stats,conf,cr,cr_coord,cr_pool,diag,dl,gcs,gcs_recovery,ges,grd,grd_recovery,guc,hang,hw,ic,inject,ir,ko,lck,lmd,lmon,lms,pcm,pgstat,phase,reconfig_join,reconfig_touched,recovery,resolver_cache,scn,sequence,shared_fs,shmem,sinval,smart_fusion,ts,tt_2pc,tt_recovery,tt_status,tt_status_hint,undo,undo_cleaner,visibility,wal_thread,write_fence,xid_stripe,xnode_lever,xnode_profile',
	'O2 pg_cluster_state has all 55 categories (spec-6.15 adds xid_stripe; spec-6.12 adds xnode_lever; spec-6.2 adds smart_fusion; spec-6.14 adds catalog)');

is($node->safe_psql('postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE value IS NULL}),
	'0', 'O3 no NULL values in pg_cluster_state (NOT NULL contract)');

# spec-5.56 D8: lifecycle counter existence + default GUC (Part B off by default).
is($node->safe_psql('postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category='cr'
		   AND key IN ('cr_global_epoch_fallback_bump_count','cr_rel_gen_bump_count',
		               'cr_rel_gen_table_overflow_count','cr_retention_horizon_advance_noted_count',
		               'cr_reconfig_intra_survived_count')}),
	'5', 'O3a all 5 spec-5.56 CR lifecycle counters present in the cr category');
is($node->safe_psql('postgres',
		q{SELECT setting FROM pg_settings WHERE name='cluster.cr_pool_rel_generation_slots'}),
	'0', 'O3b cluster.cr_pool_rel_generation_slots defaults to 0 (coarse; zero regression)');

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
# Reuse $postgres_bin (resolved via PATH in §L1) and execute through
# IPC::Run with array-form argv so we don't depend on shell PATH semantics.
my $version_out = '';
my $version_err = '';
if (defined $postgres_bin) {
	run [ $postgres_bin, '--pgrac-version' ], '>', \$version_out, '2>', \$version_err;
}
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


# ============================================================
# §P  block format change (stage 1.4) -- spec-1.4 acceptance (4 tests)
# ============================================================

# P1: block_format category exposes 4 keys.
is($node->safe_psql('postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category='block_format'}),
	'9', 'P1 block_format category has 9 keys (4 stage-1.4 + 5 stage-1.5 ITL: itl_slot_size_bytes, itl_initrans_default, itl_array_bytes, tuple_header_extra_bytes, itl_location)');

# P2: page_layout_version = 5 (PG 16 vanilla 4 + pgrac 1.4 bump).
is($node->safe_psql('postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category='block_format' AND key='page_layout_version'}),
	'5', 'P2 page_layout_version = 5');

# P3: page_header_size = 32 (24 + 8 pd_block_scn).
is($node->safe_psql('postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category='block_format' AND key='page_header_size'}),
	'32', 'P3 page_header_size = 32 (24 PG vanilla + 8B pd_block_scn)');

# P4: SCN typedef + InvalidScn invariants.
is($node->safe_psql('postgres',
		q{SELECT (SELECT value FROM pg_cluster_state
		           WHERE category='block_format' AND key='scn_size_bytes')
		     || '|' ||
		         (SELECT value FROM pg_cluster_state
		           WHERE category='block_format' AND key='invalid_scn_value')}),
	'8|0', 'P4 sizeof(SCN) = 8, InvalidScn = 0');


# ============================================================
# §P (cont.) buffer descriptor (stage 1.6) -- spec-1.6 acceptance (6 tests)
# PIVOT B (2026-05-02): on PG 16.13 BufferTag = 20B (not 16), pushing
# PG-original fields to offset 52; cluster hot tail is 12B [52, 64);
# block_scn occupies cache line 1, cr_chain_head moved to cache line 2.
# ============================================================

# P5: buffer_format category exposes 6 keys.
is($node->safe_psql('postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category='buffer_format'}),
	'6', 'P5 buffer_format category has 6 keys (stage-1.6: buffer_desc_size_bytes, buffer_desc_pad_to_size, buffer_hot_field_offset, buffer_cold_field_offset, buffer_type_count, pcm_state_count)');

# P6: BUFFERDESC_PAD_TO_SIZE = 128.
is($node->safe_psql('postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category='buffer_format' AND key='buffer_desc_pad_to_size'}),
	'128', 'P6 BUFFERDESC_PAD_TO_SIZE = 128 (PG 64 -> pgrac 128)');

# P7: sizeof(BufferDesc) <= padded size (semantic invariant).
ok($node->safe_psql('postgres',
		q{SELECT
		    (SELECT value::int FROM pg_cluster_state
		      WHERE category='buffer_format' AND key='buffer_desc_size_bytes')
		    <=
		    (SELECT value::int FROM pg_cluster_state
		      WHERE category='buffer_format' AND key='buffer_desc_pad_to_size')})
	eq 't', 'P7 sizeof(BufferDesc) <= BUFFERDESC_PAD_TO_SIZE (1st StaticAssertDecl invariant)');

# P8: PIVOT B layout invariant -- buffer_hot_field_offset = 52
# (PG 16.13: BufferTag 20 + buf_id 4 + state 4 + wait 4 + freeNext 4 + content_lock 16).
is($node->safe_psql('postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category='buffer_format' AND key='buffer_hot_field_offset'}),
	'52', 'P8 buffer_hot_field_offset = 52 (PIVOT B 实测 PG 16.13 BufferTag = 20B)');

# P9: PIVOT B layout invariant -- cluster cold body starts cache line 2.
ok($node->safe_psql('postgres',
		q{SELECT
		    (SELECT value::int FROM pg_cluster_state
		      WHERE category='buffer_format' AND key='buffer_cold_field_offset')
		    >= 64})
	eq 't', 'P9 cluster cold fields start at cache line 2 boundary >= 64 (PIVOT B + 4th StaticAssertDecl)');

# P10: BufferType enum = 5 (spec-2.31 D6: +SCUR/+XCUR), PcmState = 3.
is($node->safe_psql('postgres',
		q{SELECT (SELECT value FROM pg_cluster_state
		           WHERE category='buffer_format' AND key='buffer_type_count')
		     || '|' ||
		         (SELECT value FROM pg_cluster_state
		           WHERE category='buffer_format' AND key='pcm_state_count')}),
	'5|3', 'P10 BufferType has 5 values (CURRENT/CR/PI/SCUR/XCUR — spec-2.31 D6), PcmState has 3 values (N/S/X)');



# ============================================================
# §R  cluster_smgr end-to-end (stage 1.8) -- spec-1.8 acceptance (1 test)
# ============================================================

# R1: 1.8 milestone roll-up.  spec-1.8 is the end-to-end verification
# milestone tying together spec-1.1 (cluster_shared_fs vtable) +
# spec-1.2 (cluster_smgr in smgrsw[1] 方案 C) + spec-1.7.1 Sprint A
# (EXPERIMENTAL WARNING) + spec-1.7.2 (WARNING lifecycle fix +
# create(isRedo) signature).  The comprehensive matrix is in
# t/050_shared_storage_initdb.pl L1-L10; this acceptance test rolls
# up the milestone-level invariant: when GUC is opt-in (on + local),
# cluster_smgr is actually engaged for non-temp permanent relations.
#
# Done at 030 level so the 1.8 milestone is included in the Stage 1
# cross-spec acceptance roll-up alongside earlier milestones.
$node->safe_psql('postgres',
	"ALTER SYSTEM SET cluster.shared_storage_backend = 'local'");
$node->safe_psql('postgres',
	"ALTER SYSTEM SET cluster.smgr_user_relations = 'on'");

$node->restart;

$node->safe_psql('postgres',
	'CREATE TABLE r1_smoke (id int); INSERT INTO r1_smoke VALUES (1)');
# smgr_active_relations is a per-backend HTAB count; it must be read in the
# SAME backend that opens the USER relation.  spec-4.5a G6 routes catalogs to
# per-node md.c, so a backend that reads only pg_cluster_state opens no
# cluster_smgr relation -- touch r1_smoke first (\g discards output), then
# read the counter in one session.
my $r1_active = $node->safe_psql(
	'postgres',
	"SELECT count(*) FROM r1_smoke \\g /dev/null\n"
	  . "SELECT value::int FROM pg_cluster_state WHERE category = 'shared_fs' AND key = 'smgr_active_relations'"
);
ok( $r1_active > 0,
	"R1 stage-1.8 milestone: cluster_smgr engaged when GUC=on (smgr_active_relations=$r1_active; spec-1.8 end-to-end verified at 030 acceptance level)"
);

$node->safe_psql('postgres', 'DROP TABLE r1_smoke');


# ============================================================
# §S  postmaster startup phase machinery (stage 1.10) -- spec-1.10
#     acceptance (3 tests)
# ============================================================

# S1: phase enum 8 values + 5 keys exposed via pg_cluster_state.phase.
# Comprehensive matrix (L1-L12) lives in t/060_postmaster_phases.pl;
# 030 captures the milestone-level invariant that phase machinery
# completes startup and exposes the 5 spec-1.10 keys.
is($node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_cluster_state WHERE category = 'phase'"),
   '5',
   'S1 stage-1.10 milestone: pg_cluster_state.phase has 5 keys (cluster_phase + phase_enum_value + phase_started_at + phase_elapsed_seconds + phase_history)');

# S2: cluster_current_phase() = CLUSTER_PHASE_RUNNING (=6) after
# postmaster completes Phase 0 -> RUNNING sequence.
is($node->safe_psql(
		'postgres',
		"SELECT value FROM pg_cluster_state WHERE category = 'phase' AND key = 'phase_enum_value'"),
   '6',
   'S2 stage-1.10 milestone: phase_enum_value = 6 (CLUSTER_PHASE_RUNNING) after normal startup completes');

# S3: phase_history ring contains entries from the startup sequence
# (HC5 fixed-size ring at 8 entries; user 修订 5).  We don't pin the
# exact format but verify the ring is non-empty and contains the
# canonical "running" final state.
my $history = $node->safe_psql(
	'postgres',
	"SELECT value FROM pg_cluster_state WHERE category = 'phase' AND key = 'phase_history'"
);
like($history, qr/running@/,
   'S3 stage-1.10 milestone: phase_history ring records the running entry (HC5 fixed-size ring active)');


$node->stop;

done_testing();
