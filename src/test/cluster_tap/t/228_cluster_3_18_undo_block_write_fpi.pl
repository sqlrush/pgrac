#-------------------------------------------------------------------------
#
# 228_cluster_3_18_undo_block_write_fpi.pl
#    Stage 3.18 D2a — XLOG_UNDO_BLOCK_WRITE always-FPI corrupt-old-bytes proof.
#
#    The core correctness obligation of the undo-durability rework (AD-014):
#    once an undo block is touched after a checkpoint, crash redo must be able
#    to reconstruct the WHOLE block -- including the pre-checkpoint record
#    region -- even if that region was damaged on disk by a torn write.  D2a
#    ships this as always-FPI: every XLOG_UNDO_BLOCK_WRITE carries the full
#    8KB image, so redo restores it wholesale.
#
#      L1   write undo (pre-checkpoint records) -> CHECKPOINT -> write more
#           undo to the SAME block (post-checkpoint, emits a full-image
#           XLOG_UNDO_BLOCK_WRITE) -> capture block 1 -> crash -> deliberately
#           CORRUPT block 1's pre-checkpoint region on disk -> restart.
#           Crash redo replays the post-checkpoint FPI and the block is
#           byte-restored (magic + full image), proving the FPI carries the
#           old bytes a delta could not.
#      L2   the repaired undo is functionally usable: pre-crash UPDATEd rows
#           resolve to their correct (old/new) values after recovery.
#
#    NB: the delta-only negative control (a 3-range delta CANNOT repair a
#    corrupted pre-checkpoint region) belongs to D2b, where the delta form
#    exists -- D2a is always-FPI, which by construction has no uncovered
#    region.  See spec-3.18 §2.6/§3.4.
#
#    pg_ctl-level stop('immediate')/start cycles run on this host directly.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/228_cluster_3_18_undo_block_write_fpi.pl
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

use constant BLCKSZ            => 8192;
use constant UNDO_BLOCK_MAGIC  => 0x55444F31;    # "UDO1" little-endian (PGRAC_UNDO_BLOCK_MAGIC)
use constant DATA_BLOCK_NO     => 1;             # block 0 is the segment header
# spec-3.27 D3a / Q12-A: the undo block is now a standard PG page, so the
# UndoBlockHeader (magic first) lives at the payload base = SizeOfPageHeaderData,
# which is 32 on a pgrac build (stock 24 + the pd_block_scn extension), not 0.
use constant MAGIC_OFF         => 32;

my $node = PgracClusterNode->new('undo_fpi');
$node->init;
$node->append_conf('postgresql.conf',
	    "cluster.enabled = on\n"
	  . "cluster.node_id = 0\n"
	  . "cluster.allow_single_node = on\n"
	  # spec-3.25 D4: writeback default flipped ON; this test proves the
	  # WRITE-THROUGH (D2a always-FPI) semantics, so pin it off.  The
	  # writeback+delta redo path is covered by t/229 and t/225.
	  . "cluster.undo_buffer_writeback = off\n"
	  # Keep the proof deterministic: no auto-checkpoint between the post-
	  # checkpoint writes and the crash, no autovacuum undo touching block 1.
	  . "checkpoint_timeout = 1h\n"
	  . "max_wal_size = 4GB\n"
	  . "autovacuum = off\n");
$node->start;

$node->safe_psql('postgres', 'CREATE TABLE t318 (id int primary key, v text)');
$node->safe_psql('postgres',
	q{INSERT INTO t318 SELECT g, 'base' || g FROM generate_series(1, 6) g});

# spec-3.18 D3:  a backend claims a per-transaction undo extent and writes its
# records into that extent's blocks;  the extent is dropped at xact end.  So to
# get pre- AND post-checkpoint records into the SAME block, the writes that
# straddle the checkpoint must be in ONE transaction (a persistent session),
# with few enough records to stay within the extent's first block.  A separate
# session issues the CHECKPOINT in between.
my $bg = $node->background_psql('postgres');
$bg->query_safe('BEGIN');
$bg->query_safe(q{UPDATE t318 SET v = v || '_a'});   # pre-checkpoint "old bytes"
$node->safe_psql('postgres', 'CHECKPOINT');
$bg->query_safe(q{UPDATE t318 SET v = v || '_b'});   # post-checkpoint -> full-image FPI
$bg->query_safe('COMMIT');
$bg->quit;

# Locate the active undo data block: the HIGHEST data block carrying the magic
# (the most recently claimed extent's block -- where the straddling txn wrote).
my $undo_dir = $node->data_dir . '/pg_undo/instance_0';
my @segs     = glob("$undo_dir/seg_*.dat");
ok(scalar(@segs) >= 1, "found undo segment file(s) under $undo_dir");

my ($active_path, $active_block, $expected);
for my $path (@segs)
{
	for (my $b = 1; $b < 64; $b++)
	{
		my $blk = read_block($path, $b);
		next unless defined $blk;
		next unless unpack('L<', substr($blk, MAGIC_OFF, 4)) == UNDO_BLOCK_MAGIC;
		$active_path  = $path;
		$active_block = $b;
		$expected     = $blk;   # keep scanning -> last match wins (highest block)
	}
}
ok(defined $active_path, "active undo data block located (seg, block $active_block)");

# Crash BEFORE corrupting: stop('immediate') leaves the block on disk in its
# last write-through state (== $expected); no shutdown checkpoint advances the
# redo point past the post-checkpoint XLOG_UNDO_BLOCK_WRITE records.
$node->stop('immediate');

# Torn-write simulation: smash the pre-checkpoint region (header + first
# records) of the active block with garbage.  A delta-log would not carry these
# bytes;  only the always-FPI image can restore them.
corrupt_block_prefix($active_path, $active_block, 512, "\xEE");

my $corrupted = read_block($active_path, $active_block);
isnt(unpack('L<', substr($corrupted, MAGIC_OFF, 4)),
	UNDO_BLOCK_MAGIC, 'block magic clobbered by injected corruption (pre-restart)');

# Restart -> crash recovery -> redo replays the post-checkpoint full-image
# XLOG_UNDO_BLOCK_WRITE for the block, overwriting the corruption.
$node->start;

my $repaired = read_block($active_path, $active_block);
is(unpack('L<', substr($repaired, MAGIC_OFF, 4)),
	UNDO_BLOCK_MAGIC, 'L1 block magic restored by crash redo (always-FPI)');
ok($repaired eq $expected,
	'L1 block byte-restored to its pre-crash image (FPI carried the old bytes)');

my $log = slurp_file($node->logfile);
unlike($log, qr/PANIC/, 'L1 no PANIC during crash recovery of corrupted undo block');

# ============================================================
# L2: the repaired undo is functionally usable.  The straddling txn committed
# both UPDATEs, so every row resolves to base<N>_a_b after redo.
# ============================================================
is($node->safe_psql('postgres', q{SELECT v FROM t318 WHERE id = 5}),
	'base5_a_b', 'L2 committed (pre+post checkpoint) UPDATEs resolve after redo repair');
is($node->safe_psql('postgres', q{SELECT count(*) FROM t318 WHERE v LIKE 'base%\_a\_b'}),
	'6', 'L2 all rows resolve to the committed value after corrupt-and-redo');
is($node->safe_psql('postgres', q{SELECT count(*) FROM t318}),
	'6', 'L2 all rows intact after corrupt-and-redo');

$node->stop;
done_testing();


# --- helpers -------------------------------------------------------------

# Read one BLCKSZ block from an undo segment file; undef if too short.
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

# Overwrite the first $len bytes of a block with repeated $byte.
sub corrupt_block_prefix
{
	my ($path, $block_no, $len, $byte) = @_;
	open(my $fh, '+<:raw', $path) or die "open(rw) $path: $!";
	sysseek($fh, $block_no * BLCKSZ, 0) or die "seek $path: $!";
	syswrite($fh, $byte x $len) == $len or die "corrupt write $path: $!";
	close($fh);
}
