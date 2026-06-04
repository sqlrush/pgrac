#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 220_cluster_3_12_retention_horizon.pl
#	  spec-3.12 own-instance undo / TT-slot retention horizon end-to-end.
#
#	  Retention keeps a committed transaction's durable TT slot (and its undo
#	  segment) alive while a live reader's read_scn still needs the pre-image,
#	  so the spec-3.11 watermark>read_scn by-xid resolve now HITS (precise
#	  prune) instead of failing closed (53R9F) -- this is the L4 retire of
#	  spec-3.11's deferred headline.
#
#	  L1  horizon = min(active read_scn) over backends (not the latest snapshot).
#	  L2  horizon advances once the oldest reader leaves.
#	  L3  retention gate keeps a COMMITTED slot whose commit_scn >= horizon
#	      (tt_slot_retain_skip_count rises) and that slot stays resolvable.
#	  L4  watermark precise-resolve, NON-53R9F (spec-3.11 L4 truly activated):
#	      same >INITRANS slot-reuse scenario as t/217 E1, with a retention pin
#	      covering every xid that the watermark resolver must prove.
#	  L5  the kept slots survive CHECKPOINT + restart (redo replays them) and
#	      resolve precisely afterwards.
#	  L6  once every reader leaves, the horizon advances and the old COMMITTED
#	      slots are really recycled (retention_recycle_count rises).
#	  L7  cluster.undo_retention_horizon_enabled = off bypasses the horizon
#	      gate, so a held reader no longer prevents committed slot recycle.
#	  L8  retention pressure rolls the TT allocator over to a fresh segment
#	      instead of erroring "48 slots full".
#	  L9  a single backend holding two snapshots (S1 < S2) contributes S1 to the
#	      horizon -- the live-min, not the latest GetSnapshotData.
#
#	  Author: SqlRush <sqlrush@gmail.com>
#	  Spec: spec-3.12-retention-horizon.md (§4.2 L1-L9)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('s312_retention');
$node->init;
$node->append_conf('postgresql.conf', "cluster.enabled = on\n");
$node->append_conf('postgresql.conf', "cluster.node_id = 0\n");
$node->append_conf('postgresql.conf', "cluster.allow_single_node = on\n");
$node->append_conf('postgresql.conf', "autovacuum = off\n");
$node->start;

# counter($key) -> integer value of an undo-category counter (gauge or monotonic).
my $counter = sub {
	my ($key) = @_;
	return $node->safe_psql('postgres',
		"SELECT value FROM cluster_dump_state() WHERE key = '$key'");
};

# probe_horizon() -> sample the retention_horizon_scn gauge by forcing one TT
# allocation (the gauge is written on the alloc slow path, not per read).
$node->safe_psql('postgres', 'CREATE TABLE probe(id int primary key, v int)');
$node->safe_psql('postgres', 'INSERT INTO probe VALUES (1, 0)');
my $probe_horizon = sub {
	$node->safe_psql('postgres', 'UPDATE probe SET v = v + 1 WHERE id = 1');
	return $counter->('retention_horizon_scn');
};

# open_reader() -> a background session holding a REPEATABLE READ snapshot.
my $open_reader = sub {
	my $s = $node->background_psql('postgres', on_error_stop => 0);
	$s->query_safe('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ');
	$s->query_safe('SELECT count(*) FROM probe');    # materialize the snapshot
	return $s;
};

my $export_read_scn = sub {
	my ($s) = @_;
	my $snap = $s->query_safe('SELECT pg_export_snapshot()');
	my $path = $node->data_dir . "/pg_snapshots/$snap";

	open my $fh, '<', $path or die "could not read exported snapshot $path: $!";
	local $/;
	my $contents = <$fh>;
	close $fh;

	$contents =~ /^clu_scn:(\d+)$/m
	  or die "exported snapshot $path lacks clu_scn";
	return $1;
};

# ----------------------------------------------------------------------------
# L1: horizon = min(active read_scn) -- the older reader pins it.
# L2: horizon advances once that older reader leaves.
# ----------------------------------------------------------------------------
{
	my $h_none = $probe_horizon->();    # no held reader -> ~current SCN

	my $ra = $open_reader->();          # reader A, read_scn = SA
	my $sa = $export_read_scn->($ra);
	my $h_a = $probe_horizon->();
	is($h_a, $sa, 'L1a: a held reader publishes its exact read_scn as horizon');

	my $rb = $open_reader->();          # reader B, read_scn = SB > SA
	my $sb = $export_read_scn->($rb);
	my $h_ab = $probe_horizon->();
	is($h_ab, $sa,
		'L1b: a newer reader does NOT raise the horizon (min, not latest)');

	$ra->query_safe('COMMIT');          # oldest reader leaves
	$ra->quit;
	my $h_b = $probe_horizon->();
	is($h_b, $sb, 'L2: horizon advances to the surviving reader');

	$rb->query_safe('COMMIT');
	$rb->quit;
	my $h_done = $probe_horizon->();
	cmp_ok($h_done, '>', $h_b, 'L2b: horizon advances to current once all readers leave');
}

# ----------------------------------------------------------------------------
# L3: retention gate keeps a COMMITTED slot whose commit_scn >= horizon.
# ----------------------------------------------------------------------------
{
	$node->safe_psql('postgres', 'CREATE TABLE l3(id int, v int)');
	$node->safe_psql('postgres', 'INSERT INTO l3 SELECT g, 0 FROM generate_series(1,80) g');

	my $reader = $open_reader->();      # horizon pinned below the writers below

	my $before_skip = $counter->('tt_slot_retain_skip_count');
	# A dozen committed writers on block 0; each commit_scn > the held read_scn,
	# so the allocator must RETAIN their slots and report retain-skips when it
	# rescans for a recyclable slot.
	$node->safe_psql('postgres', "UPDATE l3 SET v = v + 1 WHERE id = $_") for (1 .. 20);

	cmp_ok($counter->('tt_slot_retain_skip_count'), '>', $before_skip,
		'L3: COMMITTED slots >= horizon were retained (retain-skip counter rose)');

	$reader->query_safe('COMMIT');
	$reader->quit;
}

# ----------------------------------------------------------------------------
# L4: watermark precise-resolve, NON-53R9F (the spec-3.11 L4 retire).
# ----------------------------------------------------------------------------
my $scn_l4;
{
	# Pin retention before the base INSERT too.  Without delayed block cleanout,
	# the watermark resolver may need the base tuple creator's durable TT slot
	# to prove that kept tuples are pre-read_scn.
	my $pin = $open_reader->();

	$node->safe_psql('postgres', 'CREATE TABLE l4(id int, v int)');
	$node->safe_psql('postgres', 'INSERT INTO l4 SELECT g, 0 FROM generate_series(1,80) g');
	my $reader = $open_reader->();
	$scn_l4 = $export_read_scn->($reader);

	# > INITRANS(8) distinct committed writers on block 0 (the t/217 E1 scenario).
	$node->safe_psql('postgres', "UPDATE l4 SET v = v + 1 WHERE id = $_") for (1 .. 12);

	my $before_scan = $counter->('tt_durable_by_xid_scan_count');
	my ($rc, undef, $err) = $node->psql('postgres',
		"SELECT cluster_cr_test_construct('l4'::regclass, 0, 0, $scn_l4)");
	is($rc, 0,
		'L4: watermark>read_scn construct SUCCEEDS (precise resolve, not 53R9F)');
	is($err // '', '', 'L4: no fail-closed error raised');
	cmp_ok($counter->('tt_durable_by_xid_scan_count'), '>', $before_scan,
		'L4: the by-xid durable resolve actually ran (counter moved) AND hit');

	# Content: at scn_l4 (pre-writer) every row on block 0 is still v=0.
	my $v0 = $node->safe_psql('postgres', qq{
		SELECT count(*) FROM cluster_cr_test_image('l4'::regclass, 0, $scn_l4)
		       AS r(cr_off int2, id int, v int) WHERE v = 0});
	my $v1 = $node->safe_psql('postgres', qq{
		SELECT count(*) FROM cluster_cr_test_image('l4'::regclass, 0, $scn_l4)
		       AS r(cr_off int2, id int, v int) WHERE v = 1});
	is($v0, '80', 'L4: CR image at read_scn shows all 80 rows pre-write (v=0)');
	is($v1, '0', 'L4: no post-read_scn version (v=1) leaked into the image');

	$reader->query_safe('COMMIT');
	$reader->quit;
	$pin->query_safe('COMMIT');
	$pin->quit;
}

# ----------------------------------------------------------------------------
# L5: kept slots survive CHECKPOINT + restart and still resolve precisely.
#     (The held reader cannot survive restart, but the durable slots it kept
#      from premature reuse are on disk + redo-replayed, so the post-restart
#      resolve at the old read_scn still hits.)
# ----------------------------------------------------------------------------
{
	$node->safe_psql('postgres', 'CHECKPOINT');
	$node->restart;

	# Redo replayed the kept durable slots during restart; the resolve at the
	# old read_scn must still hit (no premature reuse + no fail-closed).
	my ($rc, undef, $err) = $node->psql('postgres',
		"SELECT cluster_cr_test_construct('l4'::regclass, 0, 0, $scn_l4)");
	is($rc, 0, 'L5: retained durable slots resolve precisely after restart');
	is($err // '', '', 'L5: no fail-closed error after restart');
}

# ----------------------------------------------------------------------------
# L6: once every reader leaves, the horizon advances and the old COMMITTED
#     slots are really recycled.
# ----------------------------------------------------------------------------
{
	$node->safe_psql('postgres', 'CREATE TABLE l6(id int, v int)');
	$node->safe_psql('postgres', 'INSERT INTO l6 SELECT g, 0 FROM generate_series(1,80) g');

	# Phase 1: writers under a held reader -> their slots are retained.
	my $reader = $open_reader->();
	$node->safe_psql('postgres', "UPDATE l6 SET v = v + 1 WHERE id = $_") for (1 .. 20);
	$reader->query_safe('COMMIT');
	$reader->quit;

	# Phase 2: no readers -> horizon = current -> new allocs recycle the old
	# COMMITTED slots (commit_scn now < horizon).
	my $before_recycle = $counter->('retention_recycle_count');
	$node->safe_psql('postgres', "UPDATE l6 SET v = v + 1 WHERE id = $_") for (21 .. 80);
	cmp_ok($counter->('retention_recycle_count'), '>', $before_recycle,
		'L6: old COMMITTED slots recycled once the horizon passed their commit_scn');
}

# ----------------------------------------------------------------------------
# L8: retention pressure rolls over to a fresh segment (no "48 slots full").
#     (run before L7 so the retention GUC is still on)
# ----------------------------------------------------------------------------
{
	$node->safe_psql('postgres', 'CREATE TABLE l8(id int, v int)');
	$node->safe_psql('postgres', 'INSERT INTO l8 VALUES (1, 0)');

	my $reader = $open_reader->();      # pin the horizon below every writer
	my $before_roll = $counter->('tt_retention_rollover_count');
	my $before_seg  = $counter->('segment_retain_skip_count');

	# >48 distinct committed write txns: their retained COMMITTED slots fill the
	# 48-slot segment, so the 49th must roll over rather than error.  safe_psql
	# die-on-error means any "48 slots full" failure fails the test outright.
	$node->safe_psql('postgres', 'UPDATE l8 SET v = v + 1 WHERE id = 1') for (1 .. 60);

	cmp_ok($counter->('tt_retention_rollover_count'), '>', $before_roll,
		'L8: retention pressure rolled the TT allocator over to a fresh segment');
	cmp_ok($counter->('segment_retain_skip_count'), '>', $before_seg,
		'L8: the rolled-away segment was counted as retained (skip counter rose)');

	$reader->query_safe('COMMIT');
	$reader->quit;
}

# ----------------------------------------------------------------------------
# L7: GUC off -> immediate recycle even while a reader is holding the horizon.
# ----------------------------------------------------------------------------
{
	$node->safe_psql('postgres',
		    'ALTER SYSTEM SET cluster.undo_retention_horizon_enabled = off; '
		  . 'SELECT pg_reload_conf();');
	ok($node->poll_query_until('postgres',
			"SHOW cluster.undo_retention_horizon_enabled", 'off'),
		'L7: retention GUC is off before recycle churn');

	$node->safe_psql('postgres', 'CREATE TABLE l7_churn(id int, v int)');
	$node->safe_psql('postgres', 'INSERT INTO l7_churn VALUES (1, 0)');

	my $reader = $open_reader->();              # held reader is IGNORED when GUC off
	my $before_recycle = $counter->('retention_recycle_count');
	$node->safe_psql('postgres', 'UPDATE l7_churn SET v = v + 1 WHERE id = 1')
	  for (1 .. 80);
	cmp_ok($counter->('retention_recycle_count'), '>', $before_recycle,
		'L7: GUC off allowed committed TT slots to be recycled');

	$reader->query_safe('COMMIT');
	$reader->quit;
	$node->safe_psql('postgres',
		    'ALTER SYSTEM SET cluster.undo_retention_horizon_enabled = on; '
		  . 'SELECT pg_reload_conf();');
	ok($node->poll_query_until('postgres',
			"SHOW cluster.undo_retention_horizon_enabled", 'on'),
		'L7: retention GUC restored');
}

# ----------------------------------------------------------------------------
# L9: same-backend live-min -- two ordinary cursors in one backend hold S1 and
#     S2.  The horizon must reflect S1, then advance to S2 once c1 closes.
# ----------------------------------------------------------------------------
{
	$node->safe_psql('postgres', 'CREATE TABLE l9(id int, v int)');
	$node->safe_psql('postgres', 'INSERT INTO l9 SELECT g, 0 FROM generate_series(1,4) g');

	my $sb = $node->background_psql('postgres', on_error_stop => 0);
	# S1: cursor c1 keeps its portal snapshot registered in this backend.
	$sb->query_safe('BEGIN');
	$sb->query_safe('DECLARE c1 CURSOR FOR SELECT * FROM l9 ORDER BY id');

	# Advance the SCN so the next snapshot is strictly newer than S1.
	$node->safe_psql('postgres', 'UPDATE l9 SET v = v + 1 WHERE id = 1');

	# S2: cursor c2 keeps a newer portal snapshot in the SAME backend.
	$sb->query_safe('DECLARE c2 CURSOR FOR SELECT * FROM l9 ORDER BY id');

	my $h_both = $probe_horizon->();    # = S1 (the older, same-backend snapshot)

	$sb->query_safe('CLOSE c1');        # release S1; backend now holds only S2
	my $h_s2 = $probe_horizon->();

	cmp_ok($h_s2, '>', $h_both,
		'L9: horizon = the backend live-min (S1), advancing to S2 when S1 closes');

	$sb->query_safe('CLOSE c2');
	$sb->query_safe('COMMIT');
	$sb->quit;
	my $h_clear = $probe_horizon->();
	cmp_ok($h_clear, '>', $h_s2, 'L9b: horizon clears once the backend drops S2');
}

$node->stop;
done_testing();
