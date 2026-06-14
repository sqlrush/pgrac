#-------------------------------------------------------------------------
#
# 257_block_recovery_d1.pl
#    spec-4.10 D1 — online single-block recovery, read-path e2e.
#
#    A data-checksums-enabled, single-node instance.  A block is corrupted on
#    disk while the server is stopped; on the next read the bufmgr hook
#    (bufmgr.c, spec-4.10 D1) rebuilds the block from WAL via
#    cluster_block_recovery_on_read instead of erroring.
#
#      L1   corrupt block read -> online recovery returns correct data
#           (without recovery the default policy would ERROR on the bad
#           checksum, so a successful + correct read proves recovery ran)
#      L2   Q3: online recovery takes PRECEDENCE over ignore_checksum_failure
#           (a corrupted tuple byte is rebuilt to its correct value even with
#           SET ignore_checksum_failure = on)
#      L3   fail-closed: with cluster.online_block_recovery = off a corrupt
#           block read ERRORs (ERRCODE_DATA_CORRUPTED) -- recovery not attempted
#
#    Each table is given an apply-able full-page image on block 0 (a
#    modification AFTER a checkpoint); the initial INIT_PAGE inserts carry no
#    FPI, so without this the block would be unrecoverable.  Corruption is
#    injected while stopped (clean shutdown), so crash recovery does not replay
#    over it; the FPI is found by reconstruct's oldest-WAL scan (retained via
#    wal_keep_size).  D1 recovery is in-buffer (the durable install is D4), so
#    the read returns good data without persisting the fix.
#
#    Multi-node own-thread gate (foreign last-writer -> not auto-recovered,
#    forward Stage 5) is enforced by cluster_conf_has_peers(); a 2-node
#    shared-storage e2e is not built here (single-node scope) -- SKIP.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/257_block_recovery_d1.pl
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

sub read_file_raw
{
	my ($p) = @_;
	open my $fh, '<:raw', $p or die "open $p: $!";
	local $/;
	my $d = <$fh>;
	close $fh;
	return $d;
}

sub write_file_raw
{
	my ($p, $d) = @_;
	open my $fh, '>:raw', $p or die "open $p: $!";
	print $fh $d;
	close $fh;
}

my $node = PgracClusterNode->new('blkrec');
$node->init(extra => [ '--data-checksums' ]);
$node->append_conf('postgresql.conf',
	    "cluster.enabled = on\n"
	  . "cluster.node_id = 0\n"
	  . "cluster.allow_single_node = on\n"
	  . "autovacuum = off\n"
	  . "full_page_writes = on\n"
	  . "wal_keep_size = 256MB\n"
	  . "cluster.online_block_recovery = on\n");
$node->start;

# Create a table and give block 0 an apply-able FPI (a modification after a
# checkpoint).  Returns its relfilepath.
sub setup_table
{
	my ($rel, $seed_sql) = @_;
	$node->safe_psql('postgres',
		"CREATE TABLE $rel (id int, v text) WITH (autovacuum_enabled = off)");
	$node->safe_psql('postgres', $seed_sql);
	$node->safe_psql('postgres', 'CHECKPOINT');
	# first block-0 touch after the checkpoint -> carries the FPI
	$node->safe_psql('postgres', "INSERT INTO $rel VALUES (999, 'fpi_filler')");
	return $node->safe_psql('postgres', "SELECT pg_relation_filepath('$rel')");
}

# Flip one byte of block 0 (node must be stopped).
sub flip_byte_at
{
	my ($relpath, $off) = @_;
	my $path = $node->data_dir . '/' . $relpath;
	my $img = read_file_raw($path);
	my $b = unpack('C', substr($img, $off, 1));
	substr($img, $off, 1) = pack('C', $b ^ 0xFF);
	write_file_raw($path, $img);
}

# Flip a byte inside the first occurrence of $marker on disk.
sub flip_in_marker
{
	my ($relpath, $marker) = @_;
	my $path = $node->data_dir . '/' . $relpath;
	my $img = read_file_raw($path);
	my $idx = index($img, $marker);
	die "marker '$marker' not found in $path" if $idx < 0;
	my $b = unpack('C', substr($img, $idx, 1));
	substr($img, $idx, 1) = pack('C', $b ^ 0xFF);
	write_file_raw($path, $img);
}

# ============================================================
# L1: corrupt block -> online recovery returns correct data.
# ============================================================
{
	my $rp = setup_table('t1', "INSERT INTO t1 SELECT g, 'v' || g FROM generate_series(1, 20) g");
	my $exp = $node->safe_psql('postgres', 'SELECT count(*), coalesce(sum(id), 0) FROM t1');

	$node->stop;            # clean shutdown flushes the good block to disk
	flip_byte_at($rp, 2000);
	$node->start;

	# safe_psql dies on error; without recovery the bad checksum would ERROR.
	my $got = $node->safe_psql('postgres', 'SELECT count(*), coalesce(sum(id), 0) FROM t1');
	is($got, $exp, 'L1 corrupt block read -> online recovery returns correct data');

	# D6 observability: the blocks_recovered counter incremented (same server
	# lifetime; shmem counters reset on restart).
	my $recovered = $node->safe_psql('postgres',
		q{SELECT value FROM pg_cluster_state
		  WHERE category = 'recovery' AND key = 'block_recovery_blocks_recovered'});
	cmp_ok($recovered, '>=', 1, 'L1 D6 blocks_recovered counter incremented');
}

# ============================================================
# L2: Q3 -- recovery takes precedence over ignore_checksum_failure.
# ============================================================
{
	my $rp = setup_table('t2', "INSERT INTO t2 VALUES (42, 'QQMARKERVALUEQQ')");

	$node->stop;
	flip_in_marker($rp, 'QQMARKERVALUEQQ');    # corrupt the tuple's text byte
	$node->start;

	# With ignore_checksum_failure on, a non-recovering server would return the
	# corrupt page (wrong text).  Q3: recovery rebuilds it -> correct text.
	my $got = $node->safe_psql('postgres',
		'SET ignore_checksum_failure = on; SELECT v FROM t2 WHERE id = 42');
	is($got, 'QQMARKERVALUEQQ',
		'L2 Q3: recovery takes precedence over ignore_checksum_failure');
}

# ============================================================
# L4: D4 durable install -- after recovery the fix is written back to disk,
#     so a re-read (after restart, even with recovery disabled) succeeds.
# ============================================================
{
	my $rp = setup_table('t4', "INSERT INTO t4 SELECT g, 'v' || g FROM generate_series(1, 20) g");
	my $exp = $node->safe_psql('postgres', 'SELECT count(*), coalesce(sum(id), 0) FROM t4');

	$node->stop;
	flip_byte_at($rp, 2000);
	$node->start;
	# recovering read rebuilds the block AND writes it back to disk (durable install)
	$node->safe_psql('postgres', 'SELECT count(*) FROM t4');
	$node->stop;            # shutdown checkpoint fsyncs the persisted block

	# disable recovery, restart, re-read: succeeds only if the on-disk block
	# was durably fixed (in-buffer-only recovery would leave disk corrupt -> ERROR).
	$node->append_conf('postgresql.conf', "cluster.online_block_recovery = off\n");
	$node->start;
	my $got = $node->safe_psql('postgres', 'SELECT count(*), coalesce(sum(id), 0) FROM t4');
	is($got, $exp, 'L4 durable install: fix persisted to disk; re-read with recovery off succeeds');
	$node->append_conf('postgresql.conf', "cluster.online_block_recovery = on\n");
}

# ============================================================
# L3: fail-closed when online recovery is disabled.
# ============================================================
{
	my $rp = setup_table('t3', "INSERT INTO t3 SELECT g, 'v' || g FROM generate_series(1, 20) g");

	$node->stop;
	flip_byte_at($rp, 2000);
	$node->append_conf('postgresql.conf', "cluster.online_block_recovery = off\n");
	$node->start;

	my ($rc, $out, $err) = $node->psql('postgres', 'SELECT count(*) FROM t3');
	isnt($rc, 0, 'L3 recovery off -> corrupt block read fails closed');
	like($err, qr/invalid page|corrupt|checksum/i,
		'L3 fail-closed error is a corruption error');
}

$node->stop;
done_testing();
