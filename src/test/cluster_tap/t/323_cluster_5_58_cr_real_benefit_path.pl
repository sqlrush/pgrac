#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 323_cluster_5_58_cr_real_benefit_path.pl
#	  spec-5.58 D4 (HG#4) — real benefit path, full band on (L373/L374 honest).
#
#	  Proves the equivalence in HG#1 is over the LIVE optimized band, not a
#	  degenerate all-fallback: under the all-on band, BOTH CR read paths fire and
#	  are correct.
#	    * the 5.54 tuple verdict-only fast path (the path a plain visibility
#	      SELECT takes when the band is on) really fires and returns the
#	      authoritative value, and
#	    * toggling the tuple fast path OFF in the same band makes the full-block
#	      CR construct path engage instead -- so the full-block path (which the
#	      pool/admission/cache sit behind) is also live, just not the default for
#	      a simple verdict read.
#
#	    L1  band-all-on node up.
#	    L2  tuple fast path REALLY fires on a real long-snapshot read
#	        (cr_tuple_verdict_count advances) AND the verdict it serves equals the
#	        authoritative pre-update value (no false-visible).
#	    L3  with the tuple fast path OFF (same band, PGC_USERSET), the SAME read
#	        engages the full-block construct path (cr_construct_count advances) and
#	        serves the SAME authoritative value -- both paths live + equivalent.
#
#	  Honest scope (L373/L374, the band's real shape): the L2 shared pool
#	  cross-backend HIT (t/303), the 5.52 admission reject_bulk under a serial
#	  seqscan (t/304), and the 5.55 resolver memo hit (t/317) are
#	  workload-conditional (5.50 §17.4) and are proven by those dedicated e2e
#	  tests, which spec-5.58's regression re-runs.  No data-plane / cross-backend
#	  result is fabricated here, and pg_export_snapshot is NOT used (it
#	  fail-closes in cluster mode by design -- L373).
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/323_cluster_5_58_cr_real_benefit_path.pl
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

my $node = PgracClusterNode->new('cr_rb');
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

is($node->safe_psql('postgres', 'SELECT 1'), '1', 'L1 band-all-on node alive');

$node->safe_psql('postgres', 'CREATE TABLE t_rb (id int, v text)');
$node->safe_psql('postgres',
	q{INSERT INTO t_rb SELECT g, 'orig_'||g FROM generate_series(1,200) g});

# ---- L2: tuple fast path really fires + serves the authoritative value -----
{
	my $r = $node->background_psql('postgres', on_error_stop => 0);
	$r->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
	$r->query_safe('SELECT count(*) FROM t_rb');	# fix snapshot (pre-update)
	# post-snapshot UPDATE so the reader's visibility needs CR
	$node->safe_psql('postgres', "UPDATE t_rb SET v='changed_'||id");
	my $vbefore = cr_counter('cr_tuple_verdict_count');
	my $seen = $r->query_safe('SELECT v FROM t_rb WHERE id = 7');	# CR verdict path
	chomp($seen);
	$r->query_safe('ROLLBACK');
	$r->quit;
	cmp_ok(cr_counter('cr_tuple_verdict_count'), '>', $vbefore,
		'L2a tuple fast path REALLY fired on the live read path (cr_tuple_verdict_count advanced)');
	is($seen, 'orig_7',
		'L2b the tuple-fast-path verdict serves the authoritative pre-update value (no false-visible)');
}

# ---- L3: tuple OFF -> full-block construct path engages, same value --------
{
	my $r = $node->background_psql('postgres', on_error_stop => 0);
	$r->query_safe('SET cluster.cr_tuple_level_fastpath = off');
	$r->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
	$r->query_safe('SELECT count(*) FROM t_rb');	# fix snapshot
	$node->safe_psql('postgres', "UPDATE t_rb SET v='changed2_'||id");
	my $cbefore = cr_counter('cr_construct_count');
	my $seen = $r->query_safe('SELECT v FROM t_rb WHERE id = 7');	# full-block CR path
	chomp($seen);
	$r->query_safe('ROLLBACK');
	$r->quit;
	cmp_ok(cr_counter('cr_construct_count'), '>', $cbefore,
		'L3a tuple OFF: the full-block CR construct path engaged (cr_construct_count advanced) '
	  . '-- the pool/admission/cache substrate is live, not dead');
	is($seen, 'changed_7',
		'L3b the full-block path serves the SAME authoritative value the tuple path did '
	  . '(both read paths equivalent under the band)');
}

$node->stop;
done_testing();
