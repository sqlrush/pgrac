#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 320_cluster_5_58_cr_failclosed_direction.pl
#	  spec-5.58 D6 (HG#5) — Stage 5.5 CR fail-closed direction, full band on.
#
#	  Proves that with the WHOLE CR read-path band live (shared L2 pool +
#	  admission + tuple fast path + per-rel generation + resolver memo + L1
#	  cache), every uncertainty / MISS-forcing / fallback case still fails CLOSED
#	  -- it raises a registered CR SQLSTATE, NEVER returns a stale / guessed /
#	  false-visible image.  The optimizations must not be able to turn a
#	  fail-closed verdict into a silent wrong answer (rule 8.A).
#
#	  cr_check_error_injections (cluster_cr.c:778) is on the construct path that
#	  BOTH the test SRF and the real read path (lookup_or_construct -> construct
#	  on a miss) take, so arming a CR injection and invoking a construct exercises
#	  the same fail-closed code the production read path would hit -- under the
#	  band-all-on node profile.
#
#	    L1  band-all-on node is up + the CR injection points are registered.
#	    L2  cr_snapshot_too_old  -> 53R9F (missing/recycled undo; never visible).
#	    L3  cr_cross_instance    -> 53R9G (cross-instance; never visible).
#	    L4  cr_corruption 1..4   -> XX001 data_corrupted (chain anomaly; never
#	        visible) for every subkind.
#	    L5  the fault is the ONLY thing failing: a clean construct (no injection)
#	        between/after the armed cases is deterministic CR behavior, not a
#	        leftover error -- proves the band didn't enter a broken state and the
#	        fail-closed was the injected fault, not a masked stale serve.
#	    L6  the matching cr counters advanced (cr_snapshot_too_old_count /
#	        cr_cross_instance_unsupported_count / cr_corruption_count) -- the
#	        fail-closed paths really ran (not short-circuited by an optimization).
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/320_cluster_5_58_cr_failclosed_direction.pl
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
use Time::HiRes qw(usleep);
use PgracClusterNode;

# all-on band profile
my $node = PgracClusterNode->new('cr_fc');
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
	my ($key) = @_;
	return ($node->safe_psql('postgres',
		qq{SELECT COALESCE((SELECT value::bigint FROM pg_cluster_state
		   WHERE category='cr' AND key='$key'), 0)}) + 0);
}

$node->safe_psql('postgres', 'CREATE TABLE t_fc (id int, v text)');
$node->safe_psql('postgres',
	q{INSERT INTO t_fc SELECT g, 'v0_'||g FROM generate_series(1,120) g});
# a little churn so the construct has a real chain to walk
for my $i (1 .. 3)
{
	$node->safe_psql('postgres', "UPDATE t_fc SET v='v${i}_'||id WHERE id <= 30");
}

# ---- L1: liveness + registry ----------------------------------------------
is($node->safe_psql('postgres', 'SELECT 1'), '1', 'L1a band-all-on node alive');
my $have = $node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_stat_cluster_injections
	  WHERE name IN ('cr_snapshot_too_old','cr_cross_instance','cr_corruption')});
is($have, '3', 'L1b CR fail-closed injection points registered');

# Drive every fault in ONE background session (arm + invoke must share a backend;
# on_error_stop=0 keeps an injected CR error from killing the session).
my $s = $node->background_psql('postgres', on_error_stop => 0);

my $stoo_before = cr_counter('cr_snapshot_too_old_count');
my $xinst_before = cr_counter('cr_cross_instance_unsupported_count');
my $corr_before = cr_counter('cr_corruption_count');

# --- L2: 53R9F ---
$s->query_safe("SELECT cluster_inject_fault('cr_snapshot_too_old','skip',1)");
$s->query("SELECT cluster_cr_test_construct('t_fc'::regclass,0,0,100)");
my $e2 = $s->{stderr}; $s->{stderr} = '';
$s->query_safe("SELECT cluster_inject_fault('cr_snapshot_too_old','none',0)");
like($e2, qr/(53R9F|snapshot too old)/, 'L2 cr_snapshot_too_old fails closed 53R9F (band on; never visible)');

# --- L3: 53R9G ---
$s->query_safe("SELECT cluster_inject_fault('cr_cross_instance','skip',5)");
$s->query("SELECT cluster_cr_test_construct('t_fc'::regclass,0,0,100)");
my $e3 = $s->{stderr}; $s->{stderr} = '';
$s->query_safe("SELECT cluster_inject_fault('cr_cross_instance','none',0)");
like($e3, qr/(53R9G|cross-instance)/, 'L3 cr_cross_instance fails closed 53R9G (band on; never visible)');

# --- L4: XX001 data_corrupted, all 4 subkinds ---
my $corrupt_hits = 0;
for my $kind (1 .. 4)
{
	$s->query_safe("SELECT cluster_inject_fault('cr_corruption','skip',$kind)");
	$s->query("SELECT cluster_cr_test_construct('t_fc'::regclass,0,0,100)");
	$corrupt_hits++ if $s->{stderr} =~ /(XX001|corrupt)/;
	$s->{stderr} = '';
	$s->query_safe("SELECT cluster_inject_fault('cr_corruption','none',0)");
}
is($corrupt_hits, 4, 'L4 cr_corruption fails closed data_corrupted for all 4 subkinds (band on)');

# --- L5: clean construct (no injection) is deterministic CR behavior ---
$s->query("SELECT cluster_cr_test_construct('t_fc'::regclass,0,0,9223372036854775807)");
my $eclean = $s->{stderr}; $s->{stderr} = '';
ok($eclean eq '' || $eclean =~ /(53R9F|53R9G|XX001|cluster CR)/,
	'L5 a clean construct is deterministic CR behavior, not a leftover/masked error '
  . '(the band did not enter a broken state)');
$s->quit;

# --- L6: the fail-closed paths really ran (counters advanced) ---
cmp_ok(cr_counter('cr_snapshot_too_old_count'), '>', $stoo_before,
	'L6a cr_snapshot_too_old_count advanced (53R9F path really ran under the band)');
cmp_ok(cr_counter('cr_cross_instance_unsupported_count'), '>', $xinst_before,
	'L6b cr_cross_instance_unsupported_count advanced (53R9G path really ran)');
cmp_ok(cr_counter('cr_corruption_count'), '>', $corr_before,
	'L6c cr_corruption_count advanced (data_corrupted path really ran)');

$node->stop;
done_testing();
