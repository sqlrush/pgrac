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

my $node = PgracClusterNode->new('undo_fpi');
$node->init;
$node->append_conf('postgresql.conf',
	    "cluster.enabled = on\n"
	  . "cluster.node_id = 0\n"
	  . "cluster.allow_single_node = on\n"
	  # Keep the proof deterministic: no auto-checkpoint between the post-
	  # checkpoint writes and the crash, no autovacuum undo touching block 1.
	  . "checkpoint_timeout = 1h\n"
	  . "max_wal_size = 4GB\n"
	  . "autovacuum = off\n");
$node->start;

$node->safe_psql('postgres', 'CREATE TABLE t318 (id int primary key, v text)');
$node->safe_psql('postgres',
	q{INSERT INTO t318 SELECT g, 'base' || g FROM generate_series(1, 40) g});

# Pre-checkpoint undo: UPDATEs write old-tuple undo records into the active
# undo data block (block 1 of the active segment).  These are the "old bytes".
$node->safe_psql('postgres', q{UPDATE t318 SET v = v || '_a' WHERE id <= 10});

$node->safe_psql('postgres', 'CHECKPOINT');

# Post-checkpoint undo to the SAME block: each append re-emits a full-image
# XLOG_UNDO_BLOCK_WRITE covering the whole block (old + new records).
$node->safe_psql('postgres', q{UPDATE t318 SET v = v || '_b' WHERE id BETWEEN 11 AND 20});

# Locate the active undo data block (block 1 whose header carries the magic)
# and capture its exact bytes -- this is what redo must reconstruct.
my $undo_dir = $node->data_dir . '/pg_undo/instance_0';
my @segs     = glob("$undo_dir/seg_*.dat");
ok(scalar(@segs) >= 1, "found undo segment file(s) under $undo_dir");

my ($active_path, $expected);
for my $path (@segs)
{
	my $blk = read_block($path, DATA_BLOCK_NO);
	next unless defined $blk;
	my $magic = unpack('L<', substr($blk, 0, 4));
	if ($magic == UNDO_BLOCK_MAGIC)
	{
		$active_path = $path;
		$expected    = $blk;
		last;
	}
}
ok(defined $active_path, 'active undo data block 1 located (magic present)');

# Crash BEFORE corrupting: stop('immediate') leaves block 1 on disk in its
# last write-through state (== $expected); no shutdown checkpoint advances the
# redo point past the post-checkpoint XLOG_UNDO_BLOCK_WRITE records.
$node->stop('immediate');

# Torn-write simulation: smash the pre-checkpoint region (header + first
# records) of block 1 with garbage.  A delta-log would not carry these bytes;
# only the always-FPI image can restore them.
corrupt_block_prefix($active_path, DATA_BLOCK_NO, 512, "\xEE");

my $corrupted = read_block($active_path, DATA_BLOCK_NO);
isnt(unpack('L<', substr($corrupted, 0, 4)),
	UNDO_BLOCK_MAGIC, 'block 1 magic clobbered by injected corruption (pre-restart)');

# Restart -> crash recovery -> redo replays the post-checkpoint full-image
# XLOG_UNDO_BLOCK_WRITE for block 1, overwriting the corruption.
$node->start;

my $repaired = read_block($active_path, DATA_BLOCK_NO);
is(unpack('L<', substr($repaired, 0, 4)),
	UNDO_BLOCK_MAGIC, 'L1 block 1 magic restored by crash redo (always-FPI)');
ok($repaired eq $expected,
	'L1 block 1 byte-restored to its pre-crash image (FPI carried the old bytes)');

my $log = slurp_file($node->logfile);
unlike($log, qr/PANIC/, 'L1 no PANIC during crash recovery of corrupted undo block');

# ============================================================
# L2: the repaired undo is functionally usable.
# ============================================================
is($node->safe_psql('postgres', q{SELECT v FROM t318 WHERE id = 5}),
	'base5_a', 'L2 pre-checkpoint UPDATEd row resolves after redo repair');
is($node->safe_psql('postgres', q{SELECT v FROM t318 WHERE id = 15}),
	'base15_b', 'L2 post-checkpoint UPDATEd row resolves after redo repair');
is($node->safe_psql('postgres', q{SELECT count(*) FROM t318}),
	'40', 'L2 all rows intact after corrupt-and-redo');

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
