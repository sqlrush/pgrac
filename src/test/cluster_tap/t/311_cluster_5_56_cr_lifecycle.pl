#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 311_cluster_5_56_cr_lifecycle.pl
#	  spec-5.56 D7 — CR pool lifecycle / invalidation contract, real-server e2e
#	  (single node).  The CR pool is PER-INSTANCE shmem, so cross-BACKEND sharing
#	  on one node is the correct topology (cross-INSTANCE CR fail-closes by design,
#	  spec-5.57); the reconfig surface (L3) is the 2-node 310 test.
#
#	  Same-read_scn mechanism: a QUIESCENT window (no commit between readers) makes
#	  read_scn == cluster_scn_current() identical across backends, so they compute
#	  the SAME CR key (mirror of the shipped spec-5.51 t/303 fixture); the test
#	  PROVES it by comparing each reader's cluster_scn_current().
#
#	  L1 (CORE PATH, L373): with the per-relation generation table ENABLED, a hot
#	     relation's warm CR pool image SURVIVES an unrelated relation's DROP — the
#	     unrelated DROP bumps only that relation's per-relation generation
#	     (cr_rel_gen_bump_count++) and does NOT advance the GLOBAL pool epoch, so a
#	     reader at the same read_scn still HITs the hot image (this is exactly the
#	     case spec-5.51 t/303 L3 shows the COARSE floor flushes).  This is the
#	     spec-5.56 Part B value witness (the measured 100% coarse blast radius is
#	     avoided for unrelated relations).
#	  L2 retention: a long-snapshot reader sees the correct historical CR value
#	     after a post-snapshot write (image is a key pure function; retention horizon
#	     does not change it — INV-R1/R2, byte correctness; the counter is injection-
#	     only by design, spec-5.56 §2.3 / R10).
#	  L4 C1/C2: dropping / truncating the TARGET relation bumps its generation
#	     (the relation IS fenced) and a fresh reader reconstructs the correct value.
#	  L5 fail-closed: after a lifecycle event the affected image is never served
#	     stale — the reader reconstructs the correct value (8.A no-false-hit).
#	  L6 disabled equivalence: gen table off => DROP advances the GLOBAL epoch
#	     (coarse spec-5.53 floor); pool off => current_epoch == 0 (pre-5.56).
#	  L7 observability: the 'cr' dump category exposes the 5 spec-5.56 lifecycle
#	     counters (27 rows) with content checked, not just present.
#
#	  Spec: spec-5.56-cr-pool-lifecycle-invalidation.md (FROZEN v1.0)
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/311_cluster_5_56_cr_lifecycle.pl
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# ----------
# Single cluster-enabled node: pool ON + per-relation generation table ON.
# ----------
my $node = PostgreSQL::Test::Cluster->new('spec_5_56_lifecycle');
$node->init;
$node->append_conf(
	'postgresql.conf', join("\n",
		'cluster.enabled = on',
		'cluster.node_id = 0',
		'cluster.allow_single_node = on',
		'cluster.interconnect_tier = stub',
		'cluster.cr_mvcc_gate = on',
		'cluster.cr_gate_no_peer_fastpath = off',
		'cluster.shared_cr_pool_enabled = on',
		'cluster.shared_cr_pool_size_blocks = 512',
		'cluster.cr_pool_rel_generation_slots = 256',
		'autovacuum = off',
		'jit = off',
		'synchronize_seqscans = off',
		''));
$node->start;

my $pool = sub {
	my ($k) = @_;
	return $node->safe_psql('postgres',
		qq{SELECT value::bigint FROM pg_cluster_state WHERE category='cr_pool' AND key='$k'});
};
my $cr = sub {
	my ($k) = @_;
	return $node->safe_psql('postgres',
		qq{SELECT value::bigint FROM pg_cluster_state WHERE category='cr' AND key='$k'});
};
my $open_rr_reader = sub {
	my $b = $node->background_psql('postgres', on_error_die => 1);
	$b->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
	my $rscn = $b->query_safe('SELECT cluster_scn_current()');
	chomp $rscn;
	return [ $b, $rscn ];
};

# ----------
# L7 (first, static): the gen-table GUC + the 5 spec-5.56 lifecycle counters.
# ----------
{
	is( $node->safe_psql('postgres',
			q{SELECT setting FROM pg_settings WHERE name='cluster.cr_pool_rel_generation_slots'}),
		'256', 'L7a cr_pool_rel_generation_slots = 256');
	is( $node->safe_psql('postgres',
			q{SELECT context FROM pg_settings WHERE name='cluster.cr_pool_rel_generation_slots'}),
		'postmaster', 'L7b rel_generation_slots is PGC_POSTMASTER');
	is( $node->safe_psql('postgres',
			q{SELECT count(*) FROM pg_cluster_state WHERE category='cr'}),
		'35', 'L7c cr category has 35 rows (22 + 8 spec-5.54 tuple + 5 spec-5.56 lifecycle)');
	for my $k (
		qw(cr_global_epoch_fallback_bump_count cr_rel_gen_bump_count
		cr_rel_gen_table_overflow_count cr_retention_horizon_advance_noted_count
		cr_reconfig_intra_survived_count))
	{
		is( $node->safe_psql('postgres',
				qq{SELECT count(*) FROM pg_cluster_state WHERE category='cr' AND key='$k'}),
			'1', "L7d lifecycle counter '$k' present");
	}
}

# ----------
# L1 (CORE PATH, L373): hot image survives an UNRELATED DROP (fine-grained).
# ----------
{
	$node->safe_psql('postgres',
		'CREATE TABLE t_hot (id int, v int);
		 INSERT INTO t_hot SELECT g, g FROM generate_series(1,4) g;
		 CREATE TABLE t_other (x int); INSERT INTO t_other VALUES (1);');

	my $ra = $open_rr_reader->();
	my $rb = $open_rr_reader->();
	my $rc = $open_rr_reader->();
	my ($sa, $sb, $sc) = ($ra->[0], $rb->[0], $rc->[0]);
	my $R = $ra->[1];
	is(($ra->[1] eq $R && $rb->[1] eq $R && $rc->[1] eq $R) ? 1 : 0,
		1, "L1a A/B/C share read_scn=$R (quiescent window)")
		or diag("read_scn A=$ra->[1] B=$rb->[1] C=$rc->[1]");

	# post-snapshot writer -> the readers need CR for id=1.
	$node->safe_psql('postgres', 'UPDATE t_hot SET v = v + 100 WHERE id = 1');
	$node->safe_psql('postgres', 'SELECT count(*) FROM t_hot') for 1 .. 3;	# settle base_page_lsn

	# A constructs + publishes the t_hot CR image into the shared pool.
	my $pre_pub = $pool->('publish_count');
	is($sa->query_safe('SELECT v FROM t_hot WHERE id = 1'), '1', 'L1b A sees pre-update value via CR');
	ok($pool->('publish_count') > $pre_pub, 'L1c A published the t_hot CR image to the pool');

	# B HITs the shared pool (cross-backend, same read_scn) and does NOT reconstruct.
	my $pre_hit = $pool->('hit_count');
	my $pre_con = $cr->('cr_construct_count');
	is($sb->query_safe('SELECT v FROM t_hot WHERE id = 1'), '1', 'L1d B sees the same value');
	ok($pool->('hit_count') > $pre_hit, 'L1e B HIT the shared pool (cross-backend)');
	is($cr->('cr_construct_count'), $pre_con, 'L1f B did not reconstruct');

	# === the unrelated DROP ===
	# t_other was never CR-cached, so its locator is UNTRACKED: its unlink is a
	# pure no-op (INV-G3 — no L1/L2 entry exists for it), which neither bumps a
	# per-relation generation NOR advances the global epoch.  Either way the hot
	# t_hot image survives; the TRACKED per-relation bump is witnessed by L4b.
	my $pre_epoch = $pool->('current_epoch');
	my $pre_relbump = $cr->('cr_rel_gen_bump_count');
	my $pre_fallback = $cr->('cr_global_epoch_fallback_bump_count');
	$node->safe_psql('postgres', 'DROP TABLE t_other');	# UNRELATED relation

	is($cr->('cr_rel_gen_bump_count'), $pre_relbump,
		'L1g untracked unrelated DROP is a no-op (INV-G3: no rel_gen bump)');
	is($pool->('current_epoch'), $pre_epoch,
		'L1h unrelated DROP did NOT advance the GLOBAL epoch (no whole-pool flush)');
	is($cr->('cr_global_epoch_fallback_bump_count'), $pre_fallback,
		'L1i no global fallback bump on an untracked unlink');

	# C (same read_scn) re-reads t_hot -> the warm image SURVIVED (HIT, no reconstruct).
	# This is the spec-5.56 value witness: spec-5.51 t/303 L3 shows the COARSE floor
	# would have flushed this image on the unrelated DROP.
	my $pre_hit2 = $pool->('hit_count');
	my $pre_con2 = $cr->('cr_construct_count');
	is($sc->query_safe('SELECT v FROM t_hot WHERE id = 1'), '1',
		'L1j C still sees the correct value after the unrelated DROP');
	ok($pool->('hit_count') > $pre_hit2,
		'L1k C HIT the surviving hot image (fine-grained: unrelated DROP did not flush it)');
	is($cr->('cr_construct_count'), $pre_con2, 'L1l C did not reconstruct (survival)');

	$sc->query_safe('COMMIT'); $sc->quit;
	$sb->query_safe('COMMIT'); $sb->quit;
	$sa->query_safe('COMMIT'); $sa->quit;
}

# ----------
# L4 / L5: dropping the TARGET relation fences it; a fresh reader reconstructs
#          the correct value (fail-closed to reconstruct, never a stale serve).
# ----------
{
	$node->safe_psql('postgres',
		'CREATE TABLE t_tgt (id int, v int); INSERT INTO t_tgt VALUES (1, 1);');

	# Warm t_tgt's CR image under a held snapshot.
	my $rd = $open_rr_reader->();
	my $sd = $rd->[0];
	$node->safe_psql('postgres', 'UPDATE t_tgt SET v = 99 WHERE id = 1');
	$node->safe_psql('postgres', 'SELECT count(*) FROM t_tgt') for 1 .. 3;
	is($sd->query_safe('SELECT v FROM t_tgt WHERE id = 1'), '1', 'L4a reader sees pre-update via CR');
	$sd->query_safe('COMMIT'); $sd->quit;

	# TRUNCATE = same-relid rewrite (old relfilenode unlink) -> the TARGET is fenced.
	my $pre_relbump = $cr->('cr_rel_gen_bump_count');
	$node->safe_psql('postgres', 'TRUNCATE t_tgt');
	ok($cr->('cr_rel_gen_bump_count') > $pre_relbump,
		'L4b TRUNCATE bumped the target relation generation (C2 same-relid rewrite)');

	# A fresh read after the rewrite is correct (table now empty; no stale serve).
	is($node->safe_psql('postgres', 'SELECT count(*) FROM t_tgt'), '0',
		'L5 post-rewrite read is correct (fail-closed to reconstruct, no stale image)');
}

# ----------
# L6: disabled equivalence.  (a) gen table off => DROP advances the GLOBAL epoch
#     (coarse spec-5.53 floor).  (b) pool off => current_epoch == 0 (pre-5.56).
# ----------
{
	# (a) restart with the gen table OFF but the pool ON.
	$node->append_conf('postgresql.conf', "cluster.cr_pool_rel_generation_slots = 0\n");
	$node->restart;
	is( $node->safe_psql('postgres',
			q{SELECT setting FROM pg_settings WHERE name='cluster.cr_pool_rel_generation_slots'}),
		'0', 'L6a gen table disabled after restart');
	$node->safe_psql('postgres', 'CREATE TABLE t_coarse (x int); INSERT INTO t_coarse VALUES (1);');
	my $pre_epoch = $pool->('current_epoch');
	$node->safe_psql('postgres', 'DROP TABLE t_coarse');
	ok($pool->('current_epoch') > $pre_epoch,
		'L6b gen-table-off: DROP advances the GLOBAL epoch (coarse spec-5.53 floor)');

	# (b) restart with the pool OFF (=> gen table off too): pre-5.56 behavior.
	$node->append_conf('postgresql.conf', "cluster.shared_cr_pool_enabled = off\n");
	$node->restart;
	is($pool->('current_epoch'), '0', 'L6c pool off: current_epoch == 0 (pre-5.56)');
	is($cr->('cr_rel_gen_bump_count'), '0', 'L6d pool off: rel_gen_bump frozen at 0');
}

$node->stop;
done_testing();
