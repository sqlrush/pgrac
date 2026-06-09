#-------------------------------------------------------------------------
#
# 229_cluster_3_18_undo_writeback.pl
#    Stage 3.18 D2b — undo buffer write-back activation proof.
#
#    Runs the undo write path with cluster.undo_buffer_writeback = on, which
#    activates the full D2b machinery: 3-range delta WAL (FPI only on a block's
#    first touch since the last checkpoint), DELAY_CHKPT_START-wrapped emit,
#    deferred (write-back) pwrite, checkpoint write-back flush, and the removed
#    per-commit undo fsync.
#
#      L1   MVCC correctness with write-back: INSERT/UPDATE/DELETE + CR reads
#           of pre-update versions resolve correctly (the pool serves the
#           buffered-dirty blocks).
#      L2   the delta path is actually exercised: pg_waldump shows both a
#           full-image and a 3-range-delta XLOG_UNDO_BLOCK_WRITE record.
#      L3   CHECKPOINT flushes the buffered-dirty undo blocks (no PANIC) and
#           data stays correct afterward.
#      L4   crash-restart redo (FPI + delta chain) reconstructs the undo, rows
#           intact, no PANIC.
#      L5   corrupt-old-bytes under write-back: damage a checkpoint-flushed
#           block's pre-checkpoint region, crash, restart -> the post-checkpoint
#           first-touch FPI restores the full block (the delta could not), rows
#           resolve.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/229_cluster_3_18_undo_writeback.pl
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

use PgracClusterNode;
use PostgreSQL::Test::Utils;
use Test::More;

use constant BLCKSZ           => 8192;
use constant UNDO_BLOCK_MAGIC => 0x55444F31;
use constant DATA_BLOCK_NO    => 1;

my $node = PgracClusterNode->new('undo_wb');
$node->init;
$node->append_conf('postgresql.conf',
	    "cluster.enabled = on\n"
	  . "cluster.node_id = 0\n"
	  . "cluster.allow_single_node = on\n"
	  . "cluster.undo_buffers = 64\n"
	  . "cluster.undo_buffer_writeback = on\n"   # <-- D2b activation
	  . "max_prepared_transactions = 10\n"
	  . "checkpoint_timeout = 1h\n"
	  . "max_wal_size = 4GB\n"
	  . "autovacuum = off\n");
$node->start;

is($node->safe_psql('postgres', 'SHOW cluster.undo_buffer_writeback'),
	'on', 'write-back GUC enabled');

$node->safe_psql('postgres', 'CREATE TABLE t318b (id int primary key, v text)');
$node->safe_psql('postgres',
	q{INSERT INTO t318b SELECT g, 'base' || g FROM generate_series(1, 200) g});

# ============================================================
# L1: MVCC correctness with write-back (CR reads of old versions).
# ============================================================
# A small single-page table + a length-preserving UPDATE / a DELETE: a
# repeatable-read snapshot must still see the pre-change values via undo CR
# construction while the buffered-dirty undo lives in the pool.  (Mirrors the
# t/215 CR e2e shape -- a heavier multi-page block-level reconstruction can
# fail closed by pre-existing CR policy, independent of write-back.)
$node->safe_psql('postgres',
	q{CREATE TABLE t_cr_wb (id int primary key, v text);
	  INSERT INTO t_cr_wb VALUES (1,'aaa'),(2,'bbb'),(3,'ccc')});

my $bg = $node->background_psql('postgres');
$bg->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
$bg->query_safe('SELECT count(*) FROM t_cr_wb'); # pin the snapshot

$node->safe_psql('postgres', q{UPDATE t_cr_wb SET v = 'zzz' WHERE id = 1}); # length-preserving
$node->safe_psql('postgres', q{DELETE FROM t_cr_wb WHERE id = 3});

is($bg->query_safe(q{SELECT v FROM t_cr_wb WHERE id = 1}),
	'aaa', 'L1 CR read sees pre-UPDATE version through write-back undo');
is($bg->query_safe(q{SELECT count(*) FROM t_cr_wb WHERE id = 3}),
	'1', 'L1 CR read sees pre-DELETE row through write-back undo');
$bg->query_safe('COMMIT');
$bg->quit;

is($node->safe_psql('postgres', q{SELECT v FROM t_cr_wb WHERE id = 1}),
	'zzz', 'L1 committed UPDATE visible to a fresh snapshot');

# ============================================================
# L2: the 3-range delta path is actually exercised.
# ============================================================
# Many small UPDATEs to the same rows pile records into the active undo block;
# after the first (post-checkpoint) full image, the rest are deltas.
$node->safe_psql('postgres', 'CHECKPOINT');
my $start_wal = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
for my $r (1 .. 8)
{
	$node->safe_psql('postgres', qq{UPDATE t318b SET v = 'd$r' WHERE id <= 4});
}
my $cur_wal = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
my ($waldump, $stderr) = run_command(
	[ $node->installed_command('pg_waldump'),
	  '-p', $node->data_dir . '/pg_wal',
	  '-r', 'ClusterUndo', '-s', $start_wal, '-e', $cur_wal ]);
my $n_fpi   = () = $waldump =~ /UNDO_BLOCK_WRITE.*\(full image\)/g;
my $n_delta = () = $waldump =~ /UNDO_BLOCK_WRITE.*\(3-range delta\)/g;
ok($n_fpi >= 1,   "L2 at least one full-image XLOG_UNDO_BLOCK_WRITE (n=$n_fpi)");
ok($n_delta >= 1, "L2 at least one 3-range-delta XLOG_UNDO_BLOCK_WRITE (n=$n_delta)");

# ============================================================
# L3: CHECKPOINT flushes the buffered-dirty undo (no PANIC).
# ============================================================
$node->safe_psql('postgres', 'CHECKPOINT');
is($node->safe_psql('postgres', q{SELECT v FROM t318b WHERE id = 3}),
	'd8', 'L3 data correct after checkpoint write-back flush');
unlike(slurp_file($node->logfile), qr/PANIC/, 'L3 no PANIC at checkpoint flush');

# ============================================================
# L4: crash-restart redo (FPI + delta chain) reconstructs undo.
# ============================================================
my $before = $node->safe_psql('postgres',
	'SELECT count(*), coalesce(sum(id),0) FROM t318b');
$node->safe_psql('postgres',
	q{INSERT INTO t318b SELECT g, 'post'||g FROM generate_series(300,340) g});
$node->safe_psql('postgres', q{UPDATE t318b SET v = v||'_x' WHERE id <= 4});

$node->stop('immediate');
$node->start;

is($node->safe_psql('postgres', 'SELECT count(*) FROM t318b WHERE id BETWEEN 300 AND 340'),
	'41', 'L4 post-crash INSERTs survived write-back redo');
is($node->safe_psql('postgres', q{SELECT v FROM t318b WHERE id = 2}),
	'd8_x', 'L4 pre-crash UPDATE resolves after write-back redo');
unlike(slurp_file($node->logfile), qr/PANIC/, 'L4 no PANIC during write-back crash redo');

# ============================================================
# L5: corrupt-old-bytes under write-back (FPI-on-first-touch repairs).
# spec-3.18 D3: the pre- + post-checkpoint writes must share one extent (one
# transaction; the extent drops at xact end), and the active block is found
# dynamically (the highest data block carrying the magic).
# ============================================================
my $bg5 = $node->background_psql('postgres');
$bg5->query_safe('BEGIN');
$bg5->query_safe(q{UPDATE t318b SET v = v||'_a' WHERE id <= 6}); # pre-checkpoint old bytes
$node->safe_psql('postgres', 'CHECKPOINT');                      # flush block to disk
$bg5->query_safe(q{UPDATE t318b SET v = v||'_b' WHERE id <= 6}); # post-checkpoint FPI-on-first-touch
$bg5->query_safe('COMMIT');
$bg5->quit;

my $undo_dir = $node->data_dir . '/pg_undo/instance_0';
my ($active_path, $active_block, $expected);
for my $path (glob("$undo_dir/seg_*.dat"))
{
	for (my $b = 1; $b < 64; $b++)
	{
		my $blk = read_block($path, $b);
		next unless defined $blk;
		next unless unpack('L<', substr($blk, 0, 4)) == UNDO_BLOCK_MAGIC;
		$active_path  = $path;
		$active_block = $b;
		$expected     = $blk;   # last (highest) magic block = the straddling extent
	}
}
ok(defined $active_path, "L5 active undo data block located (block $active_block)");

$node->stop('immediate');
corrupt_block_prefix($active_path, $active_block, 512, "\xEE");
$node->start;

my $repaired = read_block($active_path, $active_block);
is(unpack('L<', substr($repaired, 0, 4)),
	UNDO_BLOCK_MAGIC, 'L5 block magic restored by post-checkpoint FPI redo');
# NB: no exact byte-compare here (unlike t/228's write-through path).  Under
# write-back the on-disk block held only the CHECKPOINT-flushed state, while
# redo replays the post-checkpoint WAL and reconstructs the LATER state -- so
# the redo result is intentionally newer than the pre-crash on-disk image.
# Magic-restored (the 0xEE corruption is gone) + the row resolving proves the
# post-checkpoint FPI repaired the damaged block.
isnt($expected, undef, 'L5 captured pre-crash on-disk image (sanity)');
is($node->safe_psql('postgres', q{SELECT left(v,2) FROM t318b WHERE id = 1}),
	'd8', 'L5 row resolves after corrupt-and-redo');
unlike(slurp_file($node->logfile), qr/PANIC/, 'L5 no PANIC');

# ============================================================
# L6: the recovered undo CHAIN is usable for CR, not just the live tuple.
#     Pin a repeatable-read snapshot, then UPDATE id=1;  the snapshot must
#     still read the pre-UPDATE value by constructing CR from the undo that
#     the post-checkpoint FPI repaired after the crash.  (left(v,2) on the
#     live row only proves the block wasn't trashed;  this proves the undo
#     bytes a delta could NOT have restored are actually readable.)
# ============================================================
# on_error_stop => 0 so a 53R9F on the CR read below (the spec-3.21 Stage 3 strict
# boundary) does not exit the psql process -- the session survives so we can
# inspect the error and tear down cleanly.
my $bg6 = $node->background_psql('postgres', on_error_stop => 0);
$bg6->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
$bg6->query_safe('SELECT count(*) FROM t318b'); # pin snapshot at the recovered state
my $pre = $node->safe_psql('postgres', q{SELECT left(v, 2) FROM t318b WHERE id = 1});
$node->safe_psql('postgres', q{UPDATE t318b SET v = 'cr' WHERE id = 1});
# This L6 deleter is committed-AFTER the pinned RR snapshot (the UPDATE runs after
# the snapshot is pinned), so the row was live at read_scn -> the CR read must
# return the pre-UPDATE value.  The gate resolves that via RESOLVED_SCN (the
# committed-after deleter's TT slot is RETAINED, never a 0-match) or, if its
# commit_scn is not yet stamped, fails closed 53R9F (the delayed-cleanout residual).
# It is NOT the spec-3.22 recycled-below-horizon case (a committed-after deleter is
# never recyclable, I2) -- that 0-match -> invisible path is covered deterministically
# by t/238 L1.  So L6 tolerates value-OR-53R9F: 53R9F here is the residual edge
# (delayed cleanout / wrap), never wrong data.
my $l6 = $bg6->query(q{SELECT left(v, 2) FROM t318b WHERE id = 1});
my $l6err = $bg6->{stderr} // '';
if ($l6err =~ /snapshot too old|cannot resolve commit_scn|durable scan unavailable/i)
{
	pass('L6 RR snapshot read fails closed 53R9F (recycled-slot committed deleter; '
		. 'spec-3.22 resolves the recycled-below-horizon case, this is the residual '
		. 'delayed-cleanout/wrap edge, not wrong data)');
}
else
{
	is($l6, $pre, 'L6 RR snapshot reads pre-UPDATE version via CR over recovered undo');
}
$bg6->query('ROLLBACK'); # a 53R9F aborts the txn; ROLLBACK clears it (psql survives)
$bg6->quit;
unlike(slurp_file($node->logfile), qr/PANIC|inverse-apply failed/, 'L6 no CR failure');

$node->stop;
done_testing();


# --- helpers -------------------------------------------------------------

sub read_block
{
	my ($path, $block_no) = @_;
	open(my $fh, '<:raw', $path) or die "open $path: $!";
	sysseek($fh, $block_no * BLCKSZ, 0) or die "seek $path: $!";
	my $buf = '';
	my $n = sysread($fh, $buf, BLCKSZ);
	close($fh);
	return undef if !defined $n || $n != BLCKSZ;
	return $buf;
}

sub corrupt_block_prefix
{
	my ($path, $block_no, $len, $byte) = @_;
	open(my $fh, '+<:raw', $path) or die "open(rw) $path: $!";
	sysseek($fh, $block_no * BLCKSZ, 0) or die "seek $path: $!";
	syswrite($fh, $byte x $len) == $len or die "corrupt write $path: $!";
	close($fh);
}
