#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 319_cluster_5_58_cr_fullstack_differential.pl
#	  spec-5.58 D1 (HG#1) — Stage 5.5 CR read-path full-stack differential.
#
#	  The master oracle for the integrated acceptance: with the WHOLE CR
#	  read-path band live at once (shared L2 pool + admission + tuple fast path +
#	  per-rel generation + resolver memo + L1 cache), prove the optimized REAL
#	  read path reconstructs the SAME visible rows as the AUTHORITATIVE
#	  fresh-full-block reconstruction -- no stale reuse / no false-visible / no
#	  unsafe shortcut from the combination.
#
#	  Why the real path (not the test SRF) is the differential subject: the test
#	  SRFs (cluster_cr_test_image / _construct) call cluster_cr_construct_block
#	  (raw 3.9 reconstruct) and BYPASS the pool/cache, so an SRF-vs-SRF compare
#	  would be trivially equal and prove nothing about the optimizations (L250
#	  anti-fake-acceptance).  Instead:
#	    * cluster_cr_test_image(blk, read_scn) is the AUTHORITATIVE ground truth
#	      (raw full-block reconstruct, no cache layer).
#	    * a long-snapshot SELECT goes through cluster_cr_lookup_or_construct
#	      (cluster_cr.c:1958) = L1 cache -> L2 pool -> reconstruct, i.e. the LIVE
#	      optimized band.
#	    * two backends (R1, R2) take REPEATABLE READ snapshots in the same
#	      quiescent window so they share one read_scn (5.50 §17.3) -> cross-backend
#	      equivalence of the live optimized read.  (The tuple fast path serves
#	      these verdicts WITHOUT warming the L2 pool; the pool cross-backend HIT is
#	      gated by the dedicated t/303 + t/306, re-run in nightly range 276-323.)
#
#	  Assertions (own-instance, single all-on node):
#	    L1  R1 and R2 share one read_scn (quiescent window).
#	    L2  the REAL optimized read == the AUTHORITATIVE SRF image, exact visible
#	        (id,v) set, for every sampled block (the master differential, HG#1).
#	    L3  cross-backend equivalence: R1's optimized read == R2's optimized read
#	        (no false-visible / extra / missing row across backends).
#	    L4  the band REALLY fired: the tuple-level fast path served a DECIDED
#	        verdict (cr_tuple_verdict_count advanced -- a distinct counter from the
#	        fallback outcomes), so the differential above is over the LIVE
#	        optimized path, not an all-fallback no-op (L250).  (A plain verdict
#	        SELECT does NOT warm the shared L2 pool -- the fast path short-circuits
#	        the full-block construct -- so the pool cross-backend HIT is gated by
#	        t/303 + t/306, which spec-5.58 re-runs in nightly range 276-323, not
#	        here.)
#	    L5  determinism: re-reading is identical across >=5 runs (no flake).
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/319_cluster_5_58_cr_fullstack_differential.pl
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

use Test::More;
use PgracClusterNode;

# ---- all-on profile: the whole CR read-path band live ---------------------
my $node = PgracClusterNode->new('cr_diff');
$node->init;
$node->append_conf('postgresql.conf', <<'CONF');
cluster.allow_single_node = on
cluster.node_id = 0
autovacuum = off
cluster.cr_gate_no_peer_fastpath = off
cluster.shared_cr_pool_enabled = on
cluster.shared_cr_pool_size_blocks = 256
cluster.cr_pool_admission_policy = scan_resistant
cluster.cr_tuple_level_fastpath = on
cluster.cr_pool_rel_generation_slots = 64
cluster.resolver_cache_enabled = on
cluster.resolver_cache_measure = on
cluster.resolver_cache_entries = 1024
cluster.cr_cache_max_blocks = 64
CONF
$node->start;

sub cr_counter
{
	my ($cat, $key) = @_;
	return ($node->safe_psql('postgres',
		qq{SELECT COALESCE((SELECT value::bigint FROM pg_cluster_state
		   WHERE category='$cat' AND key='$key'), 0)}) + 0);
}
sub tuple_verdict { return cr_counter('cr', 'cr_tuple_verdict_count'); }

# Authoritative ground truth: raw full-block reconstruct (no cache) as a sorted
# (id,v) digest over a block range, as-of read_scn.
sub auth_digest
{
	my ($scn, $maxblk) = @_;
	my @rows;
	for my $b (0 .. $maxblk)
	{
		my $d = $node->safe_psql('postgres',
			qq{SELECT string_agg(id || ':' || COALESCE(v,'NULL'), '|' ORDER BY id)
			   FROM cluster_cr_test_image('t_diff'::regclass, $b, $scn)
			        AS img(cr_off int2, id int, v text)});
		push @rows, $d if defined $d && $d ne '';
	}
	return join('|', @rows);
}

# ---- build the CR-heavy scenario ------------------------------------------
$node->safe_psql('postgres', 'CREATE TABLE t_diff (id int, v text)');
$node->safe_psql('postgres',
	q{INSERT INTO t_diff SELECT g, 'v0_' || g FROM generate_series(1, 400) g});
$node->safe_psql('postgres', 'CHECKPOINT');

my $maxblk = $node->safe_psql('postgres',
	q{SELECT GREATEST(0, (pg_relation_size('t_diff') / current_setting('block_size')::int)::int - 1)});
# P2-3 (review): the authoritative SRF is sampled over blocks 0..maxblk while the
# optimized read is the whole table; assert the pre-churn relation fits within the
# sampled range so the 4-block cap below can never silently drop rows from the
# authoritative side (which would make L2a fail for the wrong reason).
cmp_ok($maxblk, '<=', 3,
	"L1 pre-churn t_diff fits in the sampled block range (maxblk=$maxblk <= 3)");
$maxblk = 3 if $maxblk > 3;	# sample the first few blocks

# Two readers take REPEATABLE READ snapshots in the same quiescent window: their
# first query (SELECT cluster_scn_current()) both fixes the snapshot AND reports
# its read_scn.  No commits between -> shared read_scn.
my $r1 = $node->background_psql('postgres', on_error_stop => 0);
my $r2 = $node->background_psql('postgres', on_error_stop => 0);
$r1->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
my $scn1 = $r1->query_safe('SELECT cluster_scn_current()');
$r2->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
my $scn2 = $r2->query_safe('SELECT cluster_scn_current()');
chomp($scn1, $scn2);
is($scn1, $scn2, "L1 R1 and R2 share one read_scn ($scn1) in the quiescent window");

# Post-snapshot churn (separate xacts) -> the readers' as-of-snapshot reads must
# reconstruct via undo on every touched block.
for my $pass (1 .. 3)
{
	$node->safe_psql('postgres',
		"UPDATE t_diff SET v = 'v${pass}_' || id WHERE id % 2 = 0");
}
$node->safe_psql('postgres', 'DELETE FROM t_diff WHERE id % 50 = 0');
$node->safe_psql('postgres',
	q{INSERT INTO t_diff SELECT g, 'late_'||g FROM generate_series(401,440) g});

my $verdict_before = tuple_verdict();

# R1's optimized read: real CR path with the tuple fast path on -> per-tuple
# verdict (cluster_cr.c:1958 lookup_or_construct, short-circuited by the 5.54
# verdict-only fast path).  R2's read shares the read_scn.
my $real1 = $r1->query_safe(
	q{SELECT string_agg(id || ':' || COALESCE(v,'NULL'), '|' ORDER BY id) FROM t_diff});
my $real2 = $r2->query_safe(
	q{SELECT string_agg(id || ':' || COALESCE(v,'NULL'), '|' ORDER BY id) FROM t_diff});
chomp($real1, $real2);

my $verdict_after = tuple_verdict();

# Authoritative SRF image for the same read_scn (raw reconstruct, no cache).
my $auth = auth_digest($scn1, $maxblk);
# Normalize: both are sorted (id,v) over the same visible set; compare as sets.
my %rset = map { $_ => 1 } split /\|/, $real1;
my %aset = map { $_ => 1 } split /\|/, $auth;
# P2-2 (review): the master differential must not pass on empty input (both sets
# empty would make the diff asserts trivially green).
cmp_ok(scalar keys %rset, '>', 300,
	'L2 sanity: the optimized read returned the expected ~400 pre-churn rows (not empty)');
my $only_real = join(',', grep { !$aset{$_} } sort keys %rset);
my $only_auth = join(',', grep { !$rset{$_} } sort keys %aset);

is($only_real, '', 'L2a optimized read has NO row absent from the authoritative reconstruct (no false-visible)');
is($only_auth, '', 'L2b optimized read is MISSING no authoritative row (no false-invisible)');
is($real1, $real2, 'L3 cross-backend equivalence: R1 optimized read == R2 optimized read');
cmp_ok($verdict_after, '>', $verdict_before,
	"L4 the tuple-level fast path REALLY fired ($verdict_before -> $verdict_after verdicts) "
  . '-- the optimized band is live, not all-fallback (the equivalence above is over it)');

# Determinism across re-reads (R2 still in its snapshot).
my $flaky = 0;
for my $rep (1 .. 5)
{
	my $r = $r2->query_safe(
		q{SELECT string_agg(id || ':' || COALESCE(v,'NULL'), '|' ORDER BY id) FROM t_diff});
	chomp($r);
	$flaky++ if $r ne $real1;
}
is($flaky, 0, 'L5 optimized read is deterministic across 5 re-runs (no flake)');

$r1->query_safe('ROLLBACK');
$r2->query_safe('ROLLBACK');
$r1->quit;
$r2->quit;
$node->stop;

# NOTE (scope, honest): this leg proves the master differential over the
# tuple-fast-path read (the path a plain SELECT takes when the band is all-on:
# the 5.54 verdict-only fast path short-circuits the full-block construct, so
# cr_tuple_verdict_count is the counter that moves).  The full-block L2-pool
# cross-backend HIT leg + the resolver/admission per-feature "really-fired"
# deltas need their own triggering workloads (the pool/resolver do not engage on
# a plain verdict SELECT); they are covered by the band's own e2e regression
# (t/303 pool / t/304 admission / t/306 key / t/311 lifecycle / t/317 resolver),
# which spec-5.58 re-runs, and are folded into this differential as the suite is
# completed (HG#1 per-feature enrichment).
done_testing();
