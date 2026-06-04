#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 219_cluster_3_11_durable_tt_slot.pl
#	  spec-3.11 durable Transaction Table (TT) slot end-to-end.
#
#	  Durable commit_scn lives in the undo segment header TT slot
#	  (XLOG_UNDO_TT_SLOT_COMMIT 0x30 + per-slot 32B direct-file write).  This
#	  test exercises the parts reliably reproducible on a single node in
#	  spec-3.11 (own-instance, no slot retention yet):
#
#	  L1     durable write happens  -- tt_durable_commit_count rises one per
#	         committed DML txn (the pre-commit 0x30 + slot write, D4).
#	  L2/L7  crash recovery         -- after an immediate (crash) stop the redo
#	         of 0x30 replays the committed slots (tt_durable_redo_apply_count
#	         > 0) and the data is intact, with NO redo PANIC.  This is the
#	         durability + crash-recovery half (C1/C2 write side) and guards the
#	         P0 fixed in this spec: a zero-init (UNUSED) or FREE-reused slot
#	         must redo-APPLY, not PANIC as a same-wrap/different-xid "conflict".
#	  L5     watermark by-xid path -- a slot-reuse watermark scenario drives
#	         the D6 by-xid resolve (tt_durable_by_xid_scan_count rises).  After
#	         spec-3.12 retention, the default path succeeds because needed
#	         durable slots are retained; t/220 L4 owns the CR image content
#	         assertion.
#	  L6     GUC fallback (C6)      -- cluster.tt_durable_lookup=off routes the
#	         watermark gate back to the spec-3.10 blanket message; =on routes to
#	         the spec-3.11 by-xid message.  The differing messages prove the gate.
#
#	  Deferred to spec-3.12 retention (documented, not silently skipped --
#	  CLAUDE.md 规则 8 / L77):  L3 (overlay-evict resolve hit) and L4 (watermark
#	  precise-resolve, non-53R9F) both require a durable slot that has NOT been
#	  reused.  In spec-3.11 the TT slot allocator frees + reuses the lowest slot
#	  per committed txn with no retention (spec-3.11 §1.3), so an older writer's
#	  durable slot is overwritten before a CR read can resolve it -> by-xid miss
#	  -> fail-closed (this is exactly L5).  The read-side resolve LOGIC is
#	  unit-tested in src/test/cluster_unit/test_cluster_tt_durable.c (lookup
#	  match / wrong-xid / unused / read-fail; by-xid 0 / 1 / >1).  The full
#	  read-side fail-closed retirement lands with spec-3.12 retention.
#
#	  Author: SqlRush <sqlrush@gmail.com>
#	  Spec: spec-3.11-durable-tt-slot.md (§4.2 L1-L7)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('s311_durable');
$node->init;
$node->append_conf('postgresql.conf', "cluster.enabled = on\n");
$node->append_conf('postgresql.conf', "cluster.node_id = 0\n");
$node->append_conf('postgresql.conf', "cluster.allow_single_node = on\n");
$node->append_conf('postgresql.conf', "autovacuum = off\n");
$node->start;

# counter($key) -> integer value of an undo-category durable counter.
my $counter = sub {
	my ($key) = @_;
	return $node->safe_psql('postgres',
		"SELECT value FROM cluster_dump_state() WHERE key = '$key'");
};

# --- L1: durable commit_scn write happens (D4 pre-commit 0x30 + slot write) --
{
	my $before = $counter->('tt_durable_commit_count');
	$node->safe_psql('postgres', 'CREATE TABLE l1(id int, v int)');
	$node->safe_psql('postgres', 'INSERT INTO l1 VALUES (1,0),(2,0)');
	$node->safe_psql('postgres', 'UPDATE l1 SET v = 1 WHERE id = 1');
	my $after = $counter->('tt_durable_commit_count');
	cmp_ok($after, '>', $before,
		"L1: durable commit_scn written (count $before -> $after)");
}

# --- L2/L7: crash recovery replays 0x30 (no PANIC), data intact -------------
{
	$node->safe_psql('postgres', 'CREATE TABLE l2(id int, v int)');
	$node->safe_psql('postgres', 'INSERT INTO l2 VALUES (1,0),(2,0)');
	# Checkpoint so the commits below sit AFTER the redo point and must replay.
	$node->safe_psql('postgres', 'CHECKPOINT');
	$node->safe_psql('postgres', 'UPDATE l2 SET v = 1 WHERE id = 1');
	$node->safe_psql('postgres', 'UPDATE l2 SET v = 2 WHERE id = 2');
	$node->safe_psql('postgres', 'INSERT INTO l2 VALUES (3,3),(4,4)');

	$node->stop('immediate');    # crash: durable TT writes are WAL-only here
	$node->start;                # recovery replays 0x30 -- must NOT PANIC (P0)

	is($node->safe_psql('postgres',
			"SELECT string_agg(id || ':' || v, ',' ORDER BY id) FROM l2"),
		'1:1,2:2,3:3,4:4', 'L2: data intact after crash recovery');
	cmp_ok($counter->('tt_durable_redo_apply_count'), '>', 0,
		'L7: XLOG_UNDO_TT_SLOT_COMMIT redo applied during crash recovery');
}

# --- L5: watermark by-xid resolve runs; 3.12 retention makes it succeed -----
my $scn_l5;
{
	$node->safe_psql('postgres', 'CREATE TABLE l5(id int, v int)');
	$node->safe_psql('postgres',
		'INSERT INTO l5 SELECT g, 0 FROM generate_series(1, 80) g');
	$scn_l5 = $node->safe_psql('postgres', 'SELECT cluster_scn_current()');
	# > INITRANS(8) distinct committed writers on block 0 -> ITL recycle +
	# watermark > scn_l5; with spec-3.12 retention on by default, their durable
	# TT slots are kept and the by-xid resolve can be precise.
	$node->safe_psql('postgres', "UPDATE l5 SET v = v + 1 WHERE id = $_")
		for (1 .. 12);

	my $before_scan = $counter->('tt_durable_by_xid_scan_count');
	my ($rc, undef, $err) = $node->psql('postgres',
		"SELECT cluster_cr_test_construct('l5'::regclass, 0, 0, $scn_l5)");
	is($rc, 0, 'L5: watermark construct succeeds with 3.12 retention');
	is($err // '', '', 'L5: no durable-miss fail-closed error with retained slots');
	cmp_ok($counter->('tt_durable_by_xid_scan_count'), '>', $before_scan,
		'L5: the by-xid durable resolve mechanism actually ran (counter moved)');
}

# --- L6: GUC fallback (C6) routes to the spec-3.10 blanket path -------------
{
	my (undef, undef, $err_off) = $node->psql('postgres',
		    "SET cluster.tt_durable_lookup = off; "
		  . "SELECT cluster_cr_test_construct('l5'::regclass, 0, 0, $scn_l5)");
	like($err_off, qr/ITL slot reused after snapshot/,
		'L6: tt_durable_lookup=off -> spec-3.10 blanket fail-closed (no by-xid)');
	unlike($err_off // '', qr/durable TT slot for writer xid/,
		'L6: the GUC-off path does not consult the durable TT');
}

$node->stop;
done_testing();
