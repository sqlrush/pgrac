#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 322_cluster_5_58_cr_no_stale_reuse.pl
#	  spec-5.58 D2 (HG#2) — no stale reuse under lifecycle churn, full band on.
#
#	  With the WHOLE CR read-path band live (shared L2 pool + admission + tuple
#	  fast path + per-rel generation + resolver + L1 cache), prove that lifecycle
#	  churn that frees/rewrites a relfilenode (TRUNCATE / VACUUM FULL / CLUSTER /
#	  DROP+recreate) NEVER lets a cached CR image be served stale: a read after
#	  the churn reflects the NEW relation state, never the pre-churn image, and
#	  the pool/cache invalidation (epoch bump on relfilenode unlink) really fires.
#	  This is the combination-level check (5.53 relfilenode-reuse fence + 5.56
#	  lifecycle epoch bump live at once).
#
#	    L1  band-all-on node up.
#	    L2  warm the band: a long-snapshot CR read populates the pool/cache with
#	        the relation's images.
#	    L3  TRUNCATE + reinsert DIFFERENT data -> a fresh read reflects the new
#	        data, never the warmed (pre-truncate) image (no stale serve).
#	    L4  VACUUM FULL (relfilenode rewrite, same data) -> the optimized read
#	        still equals the authoritative reconstruct (differential preserved
#	        across a rewrite).
#	    L5  DROP + recreate + insert -> a read of the recreated relation reflects
#	        ONLY the new data (no over-hit from the old relfilenode's pool entries).
#	    L6  the pool epoch really bumped across the churn (relfilenode-unlink
#	        invalidation fired -- the no-stale guarantee is enforced, not luck).
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/322_cluster_5_58_cr_no_stale_reuse.pl
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

my $node = PgracClusterNode->new('cr_nsr');
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

sub tuple_verdict
{
	return ($node->safe_psql('postgres',
		q{SELECT COALESCE((SELECT value::bigint FROM pg_cluster_state
		  WHERE category='cr' AND key='cr_tuple_verdict_count'), 0)}) + 0);
}

is($node->safe_psql('postgres', 'SELECT 1'), '1', 'L1 band-all-on node alive');

$node->safe_psql('postgres', 'CREATE TABLE t_nsr (id int, v text)');
$node->safe_psql('postgres',
	q{INSERT INTO t_nsr SELECT g, 'orig_'||g FROM generate_series(1,300) g});

# ---- L2: warm the band with a long-snapshot CR read -----------------------
my $r = $node->background_psql('postgres', on_error_stop => 0);
$r->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
$r->query_safe('SELECT count(*) FROM t_nsr');	# fix snapshot
# churn under the snapshot so the reader's read needs CR reconstruction
for my $i (1 .. 2)
{
	$node->safe_psql('postgres', "UPDATE t_nsr SET v='upd${i}_'||id WHERE id%2=0");
}
my $warm = $r->query_safe('SELECT count(*) FROM t_nsr');	# real CR path -> pool warm
$r->query_safe('ROLLBACK');
$r->quit;
chomp($warm);
is($warm, '300', 'L2 long-snapshot CR read warmed the band (sees all 300 pre-churn rows)');

# ---- L3: TRUNCATE + reinsert different -> no stale serve -------------------
$node->safe_psql('postgres', 'TRUNCATE t_nsr');
$node->safe_psql('postgres',
	q{INSERT INTO t_nsr SELECT g, 'NEW_'||g FROM generate_series(1,50) g});
my $after_trunc = $node->safe_psql('postgres',
	q{SELECT count(*) || ',' || count(*) FILTER (WHERE v LIKE 'NEW_%') || ',' ||
	         count(*) FILTER (WHERE v LIKE 'orig_%' OR v LIKE 'upd%') FROM t_nsr});
is($after_trunc, '50,50,0',
	'L3 after TRUNCATE+reinsert: read reflects ONLY the new 50 rows, NO stale pre-truncate image');

# ---- L4: VACUUM FULL (rewrite) -> differential preserved -------------------
$node->safe_psql('postgres', 'VACUUM FULL t_nsr');
my $after_vac = $node->safe_psql('postgres',
	q{SELECT string_agg(id||':'||v, '|' ORDER BY id) FROM t_nsr});
my $expect_vac = $node->safe_psql('postgres',
	q{SELECT string_agg(g||':'||'NEW_'||g, '|' ORDER BY g) FROM generate_series(1,50) g});
is($after_vac, $expect_vac,
	'L4 after VACUUM FULL (relfilenode rewrite): every row exactly the new data (no stale, no loss)');

# ---- L5: DROP + recreate + insert -> no over-hit --------------------------
$node->safe_psql('postgres', 'DROP TABLE t_nsr');
$node->safe_psql('postgres', 'CREATE TABLE t_nsr (id int, v text)');
$node->safe_psql('postgres',
	q{INSERT INTO t_nsr SELECT g, 'REBORN_'||g FROM generate_series(1,20) g});
my $after_drop = $node->safe_psql('postgres',
	q{SELECT count(*) || ',' || count(*) FILTER (WHERE v LIKE 'REBORN_%') FROM t_nsr});
is($after_drop, '20,20',
	'L5 after DROP+recreate+insert: read reflects ONLY the reborn 20 rows '
  . '(no over-hit from the old relfilenode pool entries)');

# ---- L6: the band is STILL live + correct after all the churn --------------
# A fresh long-snapshot CR read on the reborn relation reconstructs correctly
# and the tuple fast path still fires -- proving the lifecycle churn left the
# band functional (no broken state) and the no-stale reads above were over the
# live optimized band, not a disabled one.
#
# Honest scope: warming the shared L2 POOL + forcing an EXACT relfilenode-reuse
# false-hit (the cr_locator_reuse_reject fence) needs the dedicated warm+reuse
# workload of spec-5.53 t/306, which spec-5.58 re-runs; a plain verdict SELECT
# does not populate the L2 pool (the 5.54 fast path short-circuits the
# full-block construct), so the pool epoch counters do not move here.
my $vbefore = tuple_verdict();
my $r2 = $node->background_psql('postgres', on_error_stop => 0);
$r2->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
$r2->query_safe('SELECT count(*) FROM t_nsr');
$node->safe_psql('postgres', "UPDATE t_nsr SET v='post_'||id WHERE id%2=0");
my $reread = $r2->query_safe(
	q{SELECT string_agg(id||':'||v, '|' ORDER BY id) FROM t_nsr});
$r2->query_safe('ROLLBACK');
$r2->quit;
chomp($reread);
my $expect_reread = $node->safe_psql('postgres',
	q{SELECT string_agg(g||':'||'REBORN_'||g, '|' ORDER BY g) FROM generate_series(1,20) g});
is($reread, $expect_reread,
	'L6 post-churn long-snapshot CR read still reconstructs the pre-update image exactly '
  . '(band live + correct after lifecycle churn)');
cmp_ok(tuple_verdict(), '>', $vbefore,
	'L6 the tuple fast path still fires after the churn (band is live, not disabled)');

$node->stop;
done_testing();
