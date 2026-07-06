#-------------------------------------------------------------------------
#
# 349_cluster_6_12h_pi_recovery_differential.pl
#    spec-6.12h D-h3b — L-h byte-for-byte differential for the Past Image
#    recovery rebuild: PI base + ship-SCN boundary gate vs the peer's
#    checkpointed shared-storage bytes (ground truth), over the peer
#    thread's per-thread WAL (the spec-4.11 dead-thread source).
#
#    Shape (ping-pong on ONE heap block; every write an INSERT of new rows,
#    the table is NEVER read — no hint bits; autovacuum off):
#
#      pre-window  node1 INSERTs r1 (XLOG_HEAP_INIT_PAGE — off the apply
#                  matrix, deliberately kept OUTSIDE every window), then
#                  CHECKPOINTs so the next touch carries a real FPI.
#      segment A   node1 INSERTs (FPI + deltas in thread_2).
#      middle      node0 INSERTs an alien row — X transfers to node0; the
#                  records land in thread_1, OUTSIDE every thread_2 window.
#      hand-back   node1 INSERTs again — the X transfer back converts
#                  node0's copy into a Past Image stamped with the D-h3a
#                  ship SCN.  Segment B continues with more INSERTs
#                  (post-ship records).
#
#    Legs:
#      L1  PI rebuild, full window (A+B): ok; lineage gated out
#          (skipped >= 1, incl. segment A's FPI — applying it would reset
#          the page and lose node0's row), post-ship applied (>= 1).
#      L2  truth: node1 CHECKPOINT flushes the block to shared storage;
#          the L1 digest equals the shared bytes' md5 (8.A byte-for-byte).
#      L3  zero-base parity leg over the SAME full window: a single-thread
#          window cannot rebuild a block whose history spans threads —
#          segment B's deltas presuppose node0's row.  Deterministic
#          branch on whether segment B accidentally carries an FPI (the
#          foreign install LSN vs RedoRecPtr full-page decision is not
#          ours to pin): without one the leg fails closed or diverges;
#          with one it degenerates to an FPI reset and must equal truth.
#      L4  zero-base leg over the segment-B window: no FPI -> 'no-base'
#          (same branch note as L3).
#      L5  PI rebuild over the segment-B window: ok + digest == truth —
#          recovery to current WITHOUT any FPI in the window when segment
#          B is delta-only (the PI value claim), and byte-equal either way.
#      L6  a block never transferred away has no Past Image -> 'no-pi'.
#
#    Determinism: full_page_writes on, wal_keep_size large (the SRF
#    re-reads thread_2 = node1's pg_wal, initdb -X into the shared root),
#    windows anchored on node1 LSNs, thread_2 quiesced (flush-stable poll)
#    before the window end is taken, and the PI legs run BEFORE node1's
#    CHECKPOINT so the D-h2 discard ride cannot retire the PI first.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/349_cluster_6_12h_pi_recovery_differential.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use Digest::MD5 qw(md5_hex);
use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(usleep);

use constant BLCKSZ => 8192;

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'pih3b',
	quorum_voting_disks => 3,
	shared_data         => 1,
	wal_threads_root    => 1,
	extra_conf          => [
		'autovacuum = off',
		'full_page_writes = on',
		'wal_keep_size = 256MB',
		'cluster.past_image = on',
	]);
$pair->start_pair;

# Let the IC tier1 mesh settle before the first cross-node round trip
# (t/111/251 convention).
usleep(3_000_000);

my $n0 = $pair->node0;
my $n1 = $pair->node1;

# same-DDL/same-relfilenode harness (t/339 convention): identical DDL in the
# same order on both nodes maps both catalogs onto the same shared file.
$n0->safe_psql('postgres', 'CREATE TABLE pp_t (id int, v int)');
$n1->safe_psql('postgres', 'CREATE TABLE pp_t (id int, v int)');
$n0->safe_psql('postgres', 'CREATE TABLE lonely_t (id int, v int)');
$n1->safe_psql('postgres', 'CREATE TABLE lonely_t (id int, v int)');
$n0->safe_psql('postgres', 'CREATE TABLE pad_t (a int, b int)');
$n1->safe_psql('postgres', 'CREATE TABLE pad_t (a int, b int)');

# L6 substrate: a block node0 never wrote / never transferred.
$n1->safe_psql('postgres', 'INSERT INTO lonely_t VALUES (1,1)');

# ----------------------------------------------------------------------
# pre-window: r1 creates block 0 (XLOG_HEAP_INIT_PAGE, off-matrix — stays
# outside every window below), then a node1 CHECKPOINT re-arms full-page
# writes so segment A opens with a REAL FPI the zero-base legs can seed
# from.  Block 0 has no Past Image anywhere yet, so the checkpoint cannot
# retire one.
# ----------------------------------------------------------------------
$n1->safe_psql('postgres', 'INSERT INTO pp_t VALUES (1,1)');
$n1->safe_psql('postgres', 'CHECKPOINT');

my $start_full = $n1->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');

# segment A: FPI (first post-checkpoint touch) + deltas, all in thread_2.
$n1->safe_psql('postgres', 'INSERT INTO pp_t VALUES (2,2)');
$n1->safe_psql('postgres', 'INSERT INTO pp_t VALUES (3,3)');
settle();

# middle: node0's alien-row INSERT pulls X to node0 (its records live in
# thread_1 — outside every thread_2 window used below).
#
# Pad thread_1 first until node0's insert position clears node1's redo
# pointer: the hand-back install stamps the page with node0's (foreign)
# LSN, and PG's full-page decision compares that number against the LOCAL
# redo pointer — keeping it ABOVE node1's redo pointer makes segment B
# deterministically delta-only (no accidental FPI), so the zero-base legs
# below exercise the real "no FPI in the window" branch.
{
	my $n1_redo = $n1->safe_psql('postgres',
		'SELECT redo_lsn FROM pg_control_checkpoint()');
	for my $round (1 .. 20)
	{
		last
		  if $n0->safe_psql('postgres',
			"SELECT pg_current_wal_lsn() > '$n1_redo'::pg_lsn") eq 't';
		$n0->safe_psql('postgres',
			'INSERT INTO pad_t SELECT g, g FROM generate_series(1,5000) g');
	}
	is( $n0->safe_psql('postgres',
			"SELECT pg_current_wal_lsn() > '$n1_redo'::pg_lsn"),
		't',
		'pad: thread_1 position cleared node1 redo pointer (segment B stays delta-only)');
}
$n0->safe_psql('postgres', 'INSERT INTO pp_t VALUES (100,100)');
settle();

my $start_b = $n1->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');

# hand-back + segment B: node1 writes again; the X transfer back converts
# node0's copy into a stamped Past Image, and every record from here on is
# post-ship for that stamp.
$n1->safe_psql('postgres', 'INSERT INTO pp_t VALUES (4,4)');
$n1->safe_psql('postgres', 'INSERT INTO pp_t VALUES (5,5)');
settle();

# Quiesce thread_2 (its pg_wal IS the shared thread dir), then close the
# window at the flushed end.
wal_flush_stable($n1);
my $end = $n1->safe_psql('postgres', 'SELECT pg_current_wal_flush_lsn()');

# ----------------------------------------------------------------------
# L1: PI rebuild over the full window — the ship-SCN gate must SKIP the
# whole lineage (including segment A's FPI: applying it would reset the
# page and lose node0's row) and apply exactly the post-ship records.
# Runs BEFORE node1's truth CHECKPOINT (the D-h2 discard ride must not
# retire the PI under the test).
# ----------------------------------------------------------------------
my ($st1, $scn1, $ap1, $sk1, $md5_1) = pi_srf($n0, $start_full, $end, 'true');
is($st1, 'ok', 'L1 PI rebuild over the full thread_2 window succeeds');
cmp_ok($sk1, '>=', 1, 'L1 lineage records (incl. the segment-A FPI) gated out by ship-SCN');
cmp_ok($ap1, '>=', 1, 'L1 post-ship records applied onto the PI base');
cmp_ok($scn1, '>', 0, 'L1 the PI carried a valid D-h3a ship-SCN stamp');

# L5 (run also pre-checkpoint): PI rebuild over the segment-B window.
my ($st5, undef, $ap5, undef, $md5_5) = pi_srf($n0, $start_b, $end, 'true');
is($st5, 'ok', 'L5 PI rebuild over the segment-B window succeeds');
cmp_ok($ap5, '>=', 1, 'L5 post-ship records applied');

# ----------------------------------------------------------------------
# L2: ground truth — node1 flushes the block to shared storage; compare
# byte-for-byte (md5) with the PI rebuilds.
# ----------------------------------------------------------------------
$n1->safe_psql('postgres', 'CHECKPOINT');
my $relpath = $n0->safe_psql('postgres', "SELECT pg_relation_filepath('pp_t')");
my $truth = read_block($pair->shared_data_root . '/' . $relpath, 0);
ok(defined $truth, 'L2 shared-storage block readable after the peer checkpoint');
my $truth_md5 = md5_hex($truth);
is($md5_1, $truth_md5, 'L2 PI rebuild (full window) == shared-storage truth, byte-for-byte');
is($md5_5, $truth_md5, 'L5b PI rebuild (segment-B window) == truth — no FPI needed on the PI path');

# ----------------------------------------------------------------------
# L3/L4: zero-base parity legs (the spec-4.11 storage-path reference) over
# the SAME thread_2 windows.  Branch on whether segment B accidentally
# carries an FPI (see header): the L4 probe is the detector.
# ----------------------------------------------------------------------
my ($st4, undef, undef, undef, $md5_4) = pi_srf($n0, $start_b, $end, 'false');
my ($st3, undef, undef, undef, $md5_3) = pi_srf($n0, $start_full, $end, 'false');

if ($st4 eq 'no-base')
{
	# Segment B is delta-only (the expected shape): the single-thread
	# zero-base window can neither seed a base from segment B (L4) nor
	# reproduce the cross-thread block over the full window (L3) — the
	# segment-B deltas presuppose node0's row, so the apply core fails
	# closed, or a surviving rebuild diverges from truth.
	pass('L4 delta-only segment-B window fails closed without a base (no-base)');
	ok($st3 ne 'ok' || $md5_3 ne $truth_md5,
		'L3 single-thread zero-base window cannot rebuild the cross-thread block '
		  . "(status=$st3)");
}
else
{
	# FPI accident in segment B (foreign install LSN below node1's redo
	# pointer re-armed full-page writes): both zero-base legs then seed
	# from that FPI — a full reset of the installed page — and MUST match
	# truth; the discriminating power moves entirely to L1's skipped>=1.
	is($st4, 'ok', 'L4 (FPI-accident branch) segment-B window seeds from the accidental FPI');
	is($md5_4, $truth_md5, 'L4b (FPI-accident branch) FPI reset reproduces truth');
	is($st3, 'ok', 'L3 (FPI-accident branch) full window rebuilds through the FPI reset');
	is($md5_3, $truth_md5, 'L3b (FPI-accident branch) digest matches truth');
}

# ----------------------------------------------------------------------
# L6: a block never transferred away has no Past Image on node0.
# ----------------------------------------------------------------------
my ($st6) = pi_srf($n0, $start_full, $end, 'true', 'lonely_t');
is($st6, 'no-pi', 'L6 a never-transferred block has no Past Image (fail-safe)');

$pair->stop_pair;
done_testing();


# --- helpers -------------------------------------------------------------

sub settle { usleep(700_000); return; }

# Poll node WAL flush position until two consecutive samples agree (the
# Fast-Commit ITL stamp riders land within this quiesce; a still-moving
# stream would leave truth and window out of sync).
sub wal_flush_stable
{
	my ($node) = @_;
	my $prev = '';
	for my $i (1 .. 50)
	{
		my $cur =
		  $node->safe_psql('postgres', 'SELECT pg_current_wal_flush_lsn()');
		return if $cur eq $prev;
		$prev = $cur;
		usleep(200_000);
	}
	diag('wal_flush_stable: stream still moving after 10s; proceeding');
	return;
}

# cluster_pi_apply_redo_test wrapper -> (status, ship_scn, applied, skipped, md5)
sub pi_srf
{
	my ($node, $start, $end, $use_pi, $rel) = @_;
	$rel //= 'pp_t';
	my $out = $node->safe_psql('postgres',
			"SELECT cluster_pi_apply_redo_test('$rel', 0, 0, 2, "
		  . "'$start', '$end', $use_pi)");
	return split(/:/, $out, 5);
}

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
