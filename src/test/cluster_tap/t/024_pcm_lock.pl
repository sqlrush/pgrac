#-------------------------------------------------------------------------
#
# 024_pcm_lock.pl
#    End-to-end regression for the PCM lock framework surface.  This
#    started as the stage-1.7 scaffolding test, but spec-2.30 activates
#    the local PCM state machine and expands dump_pcm from the 6-row
#    stub surface to the active 20-row diagnostic surface.
#
#    Verifies the SQL surface backed by spec-1.7 Deliverable 4 (pcm
#    category) + Deliverable 5 (4 PCM inject points, registry 24->28)
#    + Deliverable 6 (catversion not bumped, STAGE_STEP=7).
#
#    spec-2.30 updates cluster.pcm_grd_max_entries default to -1
#    (NBuffers) and reports pcm_api_state=active unless explicitly
#    disabled with 0.
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
#    The explicit small-value startup path remains covered by restarting
#    with cluster.pcm_grd_max_entries=16.
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
# L1: pg_cluster_state.pcm category has 20 keys after spec-2.30
# activates the state-machine diagnostics.
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category='pcm'}),
   '20',
   'L1 pg_cluster_state.pcm category has 20 keys (spec-2.30 active PCM surface)');


# ----------
# L2: pcm_grd_max_entries = -1 (spec-2.30 default = NBuffers).
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category='pcm' AND key='pcm_grd_max_entries'}),
   '-1',
   'L2 pcm_grd_max_entries = -1 (spec-2.30 default maps to NBuffers)');


# ----------
# L3: pcm_grd_allocated_bytes > 0 (default -1 allocates NBuffers-sized
# PCM GRD) + pcm_grd_active_entries = 0 before any production caller
# touches the state machine.
# ----------
ok($node->safe_psql(
		'postgres',
		q{SELECT value::int > 0 FROM pg_cluster_state
		   WHERE category='pcm' AND key='pcm_grd_allocated_bytes'}) eq 't',
   'L3a pcm_grd_allocated_bytes > 0 (default -1 allocates PCM GRD shmem)');

is($node->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category='pcm' AND key='pcm_grd_active_entries'}),
   '0',
   'L3b pcm_grd_active_entries = 0 before any production caller populates entries');


# ----------
# L4: pcm_lock_mode_count = 3 / pcm_transition_count = 9 / pcm_api_state = "active".
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
   '3|9|active',
   'L4 pcm_lock_mode_count=3, pcm_transition_count=9, pcm_api_state=active');


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
   '130',
   'L6a pg_stat_cluster_injections has 130 entries (spec-5.2a +1 clean-xfer stale-holder; spec-4.8ab +2 undo boundary guards; spec-5.7 +1 cluster-ko-peer-skip-ack; spec-2.41 +1 cluster-gcs-block-stale-ship)');

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
# categories total follows the current pg_cluster_state surface).
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category='buffer_format'}),
   '6',
   'L7a buffer_format category still 6 keys (1.6 baseline)');

is($node->safe_psql(
		'postgres',
		q{SELECT count(DISTINCT category) FROM pg_cluster_state}),
   '45',
   'L7b pg_cluster_state has 45 distinct categories (spec-5.7 enqueue counters; spec-5.11 adds hang)');


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
is($smoke_categories, '45', 'L9 cluster_smoke surface integrates pcm + gcs + tt_status + tt_status_hint + tt_2pc + tt_recovery + undo_record + visibility + wal_thread + dl + hw + ir + ko + ts categories (45 categories;spec-5.11 adds hang)');


# ----------
# L10: GUC cluster.pcm_grd_max_entries default -1 / range [-1, 1048576] /
# PGC_POSTMASTER (cannot be SET at runtime).
# ----------
is($node->safe_psql(
		'postgres',
		q{SHOW cluster.pcm_grd_max_entries}),
   '-1',
   'L10a cluster.pcm_grd_max_entries default -1 (spec-2.30 default-on)');

# PGC_POSTMASTER means SET at session level should fail.
my $stderr_l10;
$node->psql(
	'postgres',
	q{SET cluster.pcm_grd_max_entries = 1024;},
	stderr => \$stderr_l10);
like($stderr_l10, qr/cannot be (changed|set)/i,
	 'L10b cluster.pcm_grd_max_entries is PGC_POSTMASTER (SET fails at session level)');


# ----------
# L11: cluster.pcm_grd_max_entries=16 non-default path.  spec-2.30 HC62
# requires an explicit positive value to cover NBuffers, so this restart
# also lowers shared_buffers to 128kB (16 buffers).  This keeps the old
# small-capacity smoke test valid without weakening the production
# fail-closed rule.
#   - postmaster starts cleanly
#   - pg_cluster_shmem.cluster_pcm_grd shows size_bytes > 0
#   - pg_cluster_state.pcm.pcm_grd_allocated_bytes > 0
#   - pg_cluster_state.pcm.pcm_grd_active_entries = 0 before production
#     callers populate entries
#   - pcm_api_state = active with a positive explicit capacity.
# ----------
$node->stop;
$node->append_conf('postgresql.conf',
	"shared_buffers = 128kB\n" . "cluster.pcm_grd_max_entries = 16\n");
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
   'L11d pcm_grd_active_entries still 0 before production callers populate entries');

is($node->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category = 'pcm' AND key = 'pcm_api_state'}),
   'active',
   'L11e pcm_api_state is "active" with explicit positive capacity');


$node->stop;

done_testing();
