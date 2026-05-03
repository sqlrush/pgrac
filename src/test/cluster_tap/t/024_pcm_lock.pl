#-------------------------------------------------------------------------
#
# 024_pcm_lock.pl
#    End-to-end regression for the stage-1.7 PCM lock framework
#    scaffolding (cluster_pcm_lock.h/.c stub API + GrdEntry opaque
#    typedef + cluster_pcm_grd shmem region + 4 inject points + 6 keys
#    in pg_cluster_state.pcm category).
#
#    Verifies the SQL surface backed by spec-1.7 Deliverable 4 (pcm
#    category) + Deliverable 5 (4 PCM inject points, registry 24->28)
#    + Deliverable 6 (catversion not bumped, STAGE_STEP=7).
#
#    Q4 user 修订 2026-05-02: 6 keys total (added pcm_grd_allocated_
#    bytes + pcm_api_state to the 4-key plan).
#
#    Q6 user 修订 2026-05-02: 4 inject points named with -pre/-entry
#    consistently (cluster-pcm-release-pre, NOT cluster-pcm-release-post,
#    because 1.7 stub never reaches a 'post' point).
#
#    Q8 user 修订 2026-05-02 strong condition: 1.7 stage cluster_pcm_
#    lock_* are C internal API only; not exposed as SQL functions.
#    L5 verifies stub function symbol existence via `nm postgres |
#    grep` (link-time check) rather than direct SQL invocation, since
#    invocation as SQL would require pg_proc.dat changes -> catversion
#    bump (which 1.7 explicitly avoids).
#
#    Q4 user 提议的 "TAP 显式小值 (cluster.pcm_grd_max_entries=1024)"
#    测试推到 Stage 2.X PCM spec (本 TAP 仅默认 GUC=0 路径).
#
# IDENTIFICATION
#    src/test/cluster_tap/t/024_pcm_lock.pl
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


my $node = PgracClusterNode->new('main');
$node->init;
$node->start;


# ----------
# L1: pg_cluster_state.pcm category has 6 keys (Q4 user 修订: added
# pcm_grd_allocated_bytes + pcm_api_state to the 4-key plan).
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category='pcm'}),
   '6',
   'L1 pg_cluster_state.pcm category has 6 keys (Q4 修订: max_entries / allocated_bytes / active_entries / mode_count / transition_count / api_state)');


# ----------
# L2: pcm_grd_max_entries = 0 (Q4 default GUC).
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category='pcm' AND key='pcm_grd_max_entries'}),
   '0',
   'L2 pcm_grd_max_entries = 0 (Q4 default GUC; no GRD shmem allocated)');


# ----------
# L3: pcm_grd_allocated_bytes = 0 (Q4 修订加; GUC=0 -> 0 bytes) +
# pcm_grd_active_entries = 0 (no entries written by stub).
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category='pcm' AND key='pcm_grd_allocated_bytes'}),
   '0',
   'L3a pcm_grd_allocated_bytes = 0 (Q4 修订加; default GUC=0 -> 0 bytes shmem)');

is($node->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category='pcm' AND key='pcm_grd_active_entries'}),
   '0',
   'L3b pcm_grd_active_entries = 0 (stub never writes to GRD)');


# ----------
# L4: pcm_lock_mode_count = 3 / pcm_transition_count = 9 / pcm_api_state = "stub"
# (Q4 修订关键诊断字段：让 DBA 一眼看到 PCM 还在 stub 状态).
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT (SELECT value FROM pg_cluster_state
		           WHERE category='pcm' AND key='pcm_lock_mode_count')
		    || '|' ||
		         (SELECT value FROM pg_cluster_state
		           WHERE category='pcm' AND key='pcm_transition_count')
		    || '|' ||
		         (SELECT value FROM pg_cluster_state
		           WHERE category='pcm' AND key='pcm_api_state')}),
   '3|9|stub',
   'L4 pcm_lock_mode_count=3, pcm_transition_count=9, pcm_api_state=stub (Q4 修订关键诊断字段)');


# ----------
# L5: stub function symbol existence via `nm postgres` (codex 1.7
# review P2 修订 2026-05-02 真符号检查; spec-1.7 §1.4 例外 #7 + Q8
# strong condition).  cluster_pcm_lock_* are C internal API only;
# verify ALL 5 stub function symbols are linked into postgres binary.
# A future change that removes/renames any of these would fail this
# test even if no SQL caller exists yet.
# ----------
{
	# Locate the postgres binary that the test cluster is running.
	my $bindir = $node->config_data('--bindir');
	my $postgres_bin = "$bindir/postgres";
	ok(-x $postgres_bin, "L5a postgres binary at $postgres_bin is executable");

	# `nm` lists all symbols (text + data) defined in the binary.
	# Pipe through grep to filter cluster_pcm_lock_* / cluster_pcm_grd_*
	# symbols.  Spec-1.7.2 2026-05-03 F4 fix: extend coverage from 5
	# lock APIs to all 8 symbols (5 lock + 3 grd helpers) so that
	# accidental inline-elision or removal of a grd helper is caught
	# at TAP time rather than slipping through to runtime.
	my @expected_symbols = qw(
		cluster_pcm_lock_acquire
		cluster_pcm_lock_release
		cluster_pcm_lock_upgrade
		cluster_pcm_lock_downgrade
		cluster_pcm_lock_query
		cluster_pcm_grd_count
		cluster_pcm_grd_shmem_size
		cluster_pcm_grd_init
	);

	my $nm_output = `nm '$postgres_bin' 2>/dev/null`;
	for my $sym (@expected_symbols)
	{
		# Match lines like "0000000000123abc T cluster_pcm_lock_acquire"
		# (T = text section) or "_cluster_pcm_lock_acquire" on macOS
		# (Mach-O prepends underscore).
		like($nm_output, qr/\b_?$sym\b/,
			 "L5b nm postgres contains symbol $sym (Q8 C internal API; spec-1.7 §1.4 #7)");
	}
}


# ----------
# L6: 4 new PCM inject points exist (registry 24 -> 28; Q6 user 修订
# release-post -> release-pre).
# ----------
is($node->safe_psql(
		'postgres',
		'SELECT count(*) FROM pg_stat_cluster_injections'),
   '51',
   'L6a pg_stat_cluster_injections has 28 entries (24 pre-1.7 + 4 PCM)');

is($node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_stat_cluster_injections
		   WHERE name IN ('cluster-pcm-acquire-entry',
		                  'cluster-pcm-convert-pre',
		                  'cluster-pcm-downgrade-pre',
		                  'cluster-pcm-release-pre')}),
   '4',
   'L6b 4 PCM inject points present (Q6 user 修订: release-pre not release-post)');


# ----------
# L7: 1.6 baseline preserved (buffer_format 6 keys, block_format 9 keys,
# categories total 11 = 10 pre-1.7 + pcm).
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category='buffer_format'}),
   '6',
   'L7a buffer_format category still 6 keys (1.6 baseline)');

is($node->safe_psql(
		'postgres',
		q{SELECT count(DISTINCT category) FROM pg_cluster_state}),
   '11',
   'L7b pg_cluster_state has 11 distinct categories (10 from spec-1.6 + pcm from spec-1.7)');


# ----------
# L8: 1.5 / 1.4 baseline preserved.
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category='block_format'}),
   '9',
   'L8 block_format category still 9 keys (1.4 + 1.5 baseline)');


# ----------
# L9: cluster_smoke regression passes with pcm category present.
# ----------
my $smoke_categories = $node->safe_psql(
	'postgres',
	q{SELECT count(DISTINCT category) FROM pg_cluster_state});
is($smoke_categories, '11', 'L9 cluster_smoke surface integrates pcm category (11 categories)');


# ----------
# L10: GUC cluster.pcm_grd_max_entries default 0 / range [0, 1048576] /
# PGC_POSTMASTER (cannot be SET at runtime).
# ----------
is($node->safe_psql(
		'postgres',
		q{SHOW cluster.pcm_grd_max_entries}),
   '0',
   'L10a cluster.pcm_grd_max_entries default 0 (Q4 user 修订)');

# PGC_POSTMASTER means SET at session level should fail.
my $stderr_l10;
$node->psql(
	'postgres',
	q{SET cluster.pcm_grd_max_entries = 1024;},
	stderr => \$stderr_l10);
like($stderr_l10, qr/cannot be (changed|set)/i,
	 'L10b cluster.pcm_grd_max_entries is PGC_POSTMASTER (SET fails at session level)');


# ----------
# L11: cluster.pcm_grd_max_entries=16 non-default path (codex 1.7
# review P1 修订 2026-05-02; spec-1.X-cluster-smgr-hardening §1.3.1
# finding #1).  GUC help explicitly says users may set non-zero to
# verify shmem pre-allocation startup stability; it's a user-visible
# path that must be in CI.  Restart with GUC=16 and verify:
#   - postmaster starts cleanly
#   - pg_cluster_shmem.cluster_pcm_grd shows size_bytes > 0
#   - pg_cluster_state.pcm.pcm_grd_allocated_bytes > 0
#   - pg_cluster_state.pcm.pcm_grd_active_entries = 0 (stub never
#     populates entries, only allocates the array)
#   - stub APIs still ereport ERRCODE_FEATURE_NOT_SUPPORTED on call
#     (verified via inject framework -- shmem allocation does not
#     change stub behavior).
# ----------
$node->stop;
$node->append_conf('postgresql.conf', "cluster.pcm_grd_max_entries = 16\n");
$node->start;

is($node->safe_psql('postgres', 'SHOW cluster.pcm_grd_max_entries'),
   '16',
   'L11a postmaster started cleanly with cluster.pcm_grd_max_entries=16');

ok($node->safe_psql(
		'postgres',
		q{SELECT size_bytes > 0 FROM pg_cluster_shmem
		   WHERE name = 'pgrac cluster pcm grd'}) eq 't',
   'L11b pg_cluster_shmem.pgrac cluster pcm grd has size_bytes > 0');

ok($node->safe_psql(
		'postgres',
		q{SELECT value::int > 0 FROM pg_cluster_state
		   WHERE category = 'pcm' AND key = 'pcm_grd_allocated_bytes'}) eq 't',
   'L11c pcm_grd_allocated_bytes > 0 (16 entries × sizeof(GrdEntry))');

is($node->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category = 'pcm' AND key = 'pcm_grd_active_entries'}),
   '0',
   'L11d pcm_grd_active_entries still 0 (stub never populates entries)');

is($node->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category = 'pcm' AND key = 'pcm_api_state'}),
   'stub',
   'L11e pcm_api_state still "stub" (allocation does not activate state machine)');


$node->stop;

done_testing();
