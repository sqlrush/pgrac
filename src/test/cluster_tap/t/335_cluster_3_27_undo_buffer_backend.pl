#-------------------------------------------------------------------------
#
# 335_cluster_3_27_undo_buffer_backend.pl
#    Stage 3.27 D3b — buffer-backed (bufmgr) undo runtime write/read path.
#
#    spec-3.27 replaces the custom UndoBufPool serialization with PG's shared
#    buffer manager (B2-full).  D3b lands the runtime WRITE path (ReadBuffer +
#    EXCLUSIVE content lock + read-modify-write into the LIVE page + always-FPI
#    RM_CLUSTER_UNDO WAL + PageSetLSN + MarkBufferDirty) and the coherent READ
#    path (SHARE lock, so no reader misses un-checkpointed dirty undo), all
#    behind the new GUC cluster.undo_buffer_backend (default legacy_pool; this
#    test turns it to bufmgr).  recovery stays path-based (Q5-C).
#
#      L1   UPDATE ... ROLLBACK reverts every row to its pre-image -- undo was
#           written to, and read back from, the bufmgr buffer correctly.
#      L2   UPDATE ... COMMIT persists the new values.
#      L3   the bufmgr write path was actually ENGAGED (not a silent fallback
#           to the legacy pool) -- server log carries the once-per-backend
#           "buffer-backed (bufmgr) write path engaged" marker (rule 8.A:
#           a cluster path must never silently degrade to the native path).
#      L4   immediate crash -> restart -> path-redo replays the undo WAL and
#           the committed (pre- and post-checkpoint) data resolves correctly
#           with no PANIC and no undo-block corruption.
#
#    pg_ctl-level stop('immediate')/start cycles run on this host directly.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/335_cluster_3_27_undo_buffer_backend.pl
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
use constant UNDO_BLOCK_MAGIC => 0x55444F31;    # "UDO1" (PGRAC_UNDO_BLOCK_MAGIC)
# Q12-A: the UndoBlockHeader magic sits at the payload base = pd_lower of the
# standard PG page (= SizeOfPageHeaderData, which is 32 on a pgrac build:
# stock 24 + the pgrac pd_block_scn extension).  Read pd_lower per block rather
# than hard-coding the header size.

my $node = PgracClusterNode->new('undo_bufmgr');
$node->init;
$node->append_conf('postgresql.conf',
	    "cluster.enabled = on\n"
	  . "cluster.node_id = 0\n"
	  . "cluster.allow_single_node = on\n"
	  # D3b under test: route undo runtime writes through the shared buffer
	  # manager instead of the custom UndoBufPool.
	  . "cluster.undo_buffer_backend = bufmgr\n"
	  # Deterministic crash proof: no auto-checkpoint / autovacuum racing the
	  # workload; a single explicit CHECKPOINT straddles the committed writes.
	  . "checkpoint_timeout = 1h\n"
	  . "max_wal_size = 4GB\n"
	  . "autovacuum = off\n"
	  . "log_min_messages = log\n");
$node->start;

# Confirm the new GUC is recognized and set (fails pre-implementation).
is($node->safe_psql('postgres', 'SHOW cluster.undo_buffer_backend'),
	'bufmgr', 'cluster.undo_buffer_backend = bufmgr accepted');

$node->safe_psql('postgres', 'CREATE TABLE t327 (id int primary key, v text)');
$node->safe_psql('postgres',
	q{INSERT INTO t327 SELECT g, 'base' || g FROM generate_series(1, 20) g});

# ============================================================
# L1: UPDATE ... ROLLBACK reverts to the pre-image (undo correctly written to,
# and read back from, the bufmgr buffer).
# ============================================================
my $bg = $node->background_psql('postgres');
$bg->query_safe('BEGIN');
$bg->query_safe(q{UPDATE t327 SET v = v || '_rolled'});
$bg->query_safe('ROLLBACK');
$bg->quit;

is($node->safe_psql('postgres', q{SELECT count(*) FROM t327 WHERE v LIKE '%\_rolled'}),
	'0', 'L1 no _rolled value survives ROLLBACK (undo applied via bufmgr)');
is($node->safe_psql('postgres', q{SELECT v FROM t327 WHERE id = 7}),
	'base7', 'L1 row 7 reverted to its pre-image after ROLLBACK');

# ============================================================
# L2: UPDATE ... COMMIT persists (pre-checkpoint committed change).
# ============================================================
$node->safe_psql('postgres', q{UPDATE t327 SET v = v || '_a'});
is($node->safe_psql('postgres', q{SELECT count(*) FROM t327 WHERE v LIKE 'base%\_a'}),
	'20', 'L2 committed UPDATE persists (pre-checkpoint)');

# CHECKPOINT flushes the bufmgr undo buffers to their segment files, then a
# post-checkpoint committed UPDATE exercises redo from the post-checkpoint WAL.
$node->safe_psql('postgres', 'CHECKPOINT');
$node->safe_psql('postgres', q{UPDATE t327 SET v = v || '_b'});

# ============================================================
# L3: the bufmgr write path was actually engaged (anti-silent-fallback, 8.A).
# ============================================================
my $log = slurp_file($node->logfile);
like($log, qr/buffer-backed \(bufmgr\) write path engaged/,
	'L3 bufmgr undo write path engaged (no silent fallback to legacy pool)');

# Sanity: an active undo data block on disk carries the magic at payload@24
# (Q12-A standard PageHeader), proving bufmgr wrote a well-formed page.
my $undo_dir = $node->data_dir . '/pg_undo/instance_0';
my @segs     = glob("$undo_dir/seg_*.dat");
ok(scalar(@segs) >= 1, "found undo segment file(s) under $undo_dir");
my $found_magic = 0;
for my $path (@segs)
{
	for (my $b = 1; $b < 64; $b++)
	{
		my $blk = read_block($path, $b);
		next unless defined $blk;
		my $pd_lower = unpack('S<', substr($blk, 12, 2));    # PageHeaderData.pd_lower
		next if $pd_lower < 24 || $pd_lower + 4 > BLCKSZ;
		$found_magic = 1
		  if unpack('L<', substr($blk, $pd_lower, 4)) == UNDO_BLOCK_MAGIC;
	}
}
ok($found_magic, 'L3 undo data block carries UDO1 magic at payload base (Q12-A standard page)');

# ============================================================
# L4: immediate crash -> path-redo replays undo WAL -> committed data resolves,
# no PANIC, no corruption.
# ============================================================
$node->stop('immediate');
$node->start;

my $reclog = slurp_file($node->logfile);
unlike($reclog, qr/PANIC/, 'L4 no PANIC during crash recovery of bufmgr undo');

is($node->safe_psql('postgres', q{SELECT v FROM t327 WHERE id = 12}),
	'base12_a_b', 'L4 committed pre+post-checkpoint UPDATEs resolve after redo');
is($node->safe_psql('postgres', q{SELECT count(*) FROM t327 WHERE v LIKE 'base%\_a\_b'}),
	'20', 'L4 all rows resolve to the committed value after crash redo');
is($node->safe_psql('postgres', q{SELECT count(*) FROM t327}),
	'20', 'L4 all rows intact after crash redo');

# The recovered cluster still writes undo correctly under bufmgr (fresh xact
# after redo -> ROLLBACK reverts).
my $bg2 = $node->background_psql('postgres');
$bg2->query_safe('BEGIN');
$bg2->query_safe(q{UPDATE t327 SET v = v || '_post'});
$bg2->query_safe('ROLLBACK');
$bg2->quit;
is($node->safe_psql('postgres', q{SELECT count(*) FROM t327 WHERE v LIKE '%\_post'}),
	'0', 'L4 post-recovery ROLLBACK still reverts (undo write/read intact)');

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
