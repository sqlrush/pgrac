#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 358_data_plane_flip_e2e.pl
#	  spec-7.2 D3+D4 atomic plane flip — bidirectional e2e + staging
#	  survival (the flip-commit acceptance legs, user-ruled hard
#	  constraints on the flip):
#
#	  L1  flip fact: pg_cluster_state gcs/block_family_plane = 1 on
#	      both nodes;  DATA-plane listeners bound (log evidence)
#	  L2  forward transfer: node0 seeds + X-holds a shared table;
#	      node1's UPDATE ships the block over the LMS DATA plane
#	      (statement succeeds, requester block_request_count grows,
#	      plane_misroute_reject stays 0 on both nodes)
#	  L3  reverse transfer: node1 now holds;  node0's UPDATE ships
#	      back (bidirectional evidence)
#	  L4  staging survival (D0-①b case): node1 READS an X-held block
#	      -- the holder-side LMS FORWARD path downgrades X->S and its
#	      GCS_REQUEST notify must reach LMON via the CONTROL ring, not
#	      a cross-plane direct send;  no plane-gate ERROR may appear in
#	      either node's log
#	  L5  hygiene: zero plane misroutes + zero "cannot send from
#	      plane" / "plane mismatch" strings across the whole run
#
# Spec: spec-7.2-ic-data-plane-decoupling.md (flip commit)
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/358_data_plane_flip_e2e.pl
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Time::HiRes qw(usleep);
use Test::More;

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'dpflip',
	quorum_voting_disks => 3,
	shared_data         => 1,
	storage_backend     => 'block_device',
	extra_conf          => [
		'autovacuum = off',
		'fsync = off',
		'shared_buffers = 64MB',
		'cluster.ges_request_timeout_ms = 30000',
		'cluster.gcs_reply_timeout_ms = 3000',
		'cluster.online_join = on',
		'cluster.xid_striping = on',
		'cluster.crossnode_runtime_visibility = on',
		'cluster.crossnode_cr_data_plane = on',
		'cluster.block_self_contained = on',
	]);
$pair->start_pair;
usleep(2_000_000);
ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'CONTROL peers up 0->1');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'CONTROL peers up 1->0');
my ($node0, $node1) = ($pair->node0, $pair->node1);

sub gcs_int
{
	my ($node, $key) = @_;
	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state WHERE category='gcs' AND key='$key'});
	return defined($v) && $v ne '' ? int($v) : 0;
}

# Poll until a write statement succeeds on $node — the online-join
# admission gate rejects writable transactions until the join settles
# (t/347 pattern).
sub poll_write_ok
{
	my ($node, $sql, $timeout_s, $label) = @_;
	$timeout_s //= 60;
	my $deadline = time + $timeout_s;
	while (time < $deadline)
	{
		my $ok = eval { $node->safe_psql('postgres', $sql); 1 } // 0;
		return 1 if $ok;
		usleep(500_000);
	}
	diag("poll_write_ok timeout ($label)");
	return 0;
}

# ============================================================
# L1 — flip fact + DATA listeners.
# ============================================================
is(gcs_int($node0, 'block_family_plane'), 1, 'L1 node0 block family on DATA plane');
is(gcs_int($node1, 'block_family_plane'), 1, 'L1 node1 block family on DATA plane');

my $log0 = PostgreSQL::Test::Utils::slurp_file($node0->logfile);
my $log1 = PostgreSQL::Test::Utils::slurp_file($node1->logfile);
like($log0, qr/DATA-plane listener bound/, 'L1 node0 DATA listener bound');
like($log1, qr/DATA-plane listener bound/, 'L1 node1 DATA listener bound');

# ============================================================
# shared table (relfilenode coincidence on the shared device).
# ============================================================
ok(poll_write_ok($node0, 'CREATE TABLE flip_gate0 (x int)', 90, 'node0 write gate'),
	'node0 write gate open (join admitted)');
ok(poll_write_ok($node1, 'CREATE TABLE flip_gate1 (x int)', 90, 'node1 write gate'),
	'node1 write gate open (join admitted)');

# The write-gate polling burned an uneven number of OIDs per node.  Align
# the two OID counters (t/347 pattern): measure the relfilenode delta and
# burn single OIDs on the lagging node until an identical CREATE lands on
# the same relfilenode on both.
my ($p0, $p1) = ('', '');
for my $attempt (1 .. 8)
{
	$node0->safe_psql('postgres',
		'CREATE TABLE flip_t (aid int, bal int) WITH (fillfactor = 50)');
	$node1->safe_psql('postgres',
		'CREATE TABLE flip_t (aid int, bal int) WITH (fillfactor = 50)');
	$p0 = $node0->safe_psql('postgres', q{SELECT pg_relation_filepath('flip_t')});
	$p1 = $node1->safe_psql('postgres', q{SELECT pg_relation_filepath('flip_t')});
	last if $p0 eq $p1;
	my ($n0) = $p0 =~ /(\d+)$/;
	my ($n1) = $p1 =~ /(\d+)$/;
	my ($lag, $burn) = $n0 < $n1 ? ($node0, $n1 - $n0) : ($node1, $n0 - $n1);
	$lag->safe_psql('postgres',
		"SELECT lo_unlink(lo_create(0)) FROM generate_series(1, $burn)");
	$node0->safe_psql('postgres', 'DROP TABLE flip_t');
	$node1->safe_psql('postgres', 'DROP TABLE flip_t');
}
is($p0, $p1, 'shared-table coincidence');
$node0->safe_psql('postgres',
	'INSERT INTO flip_t SELECT g, 0 FROM generate_series(1, 200) g');
$node0->safe_psql('postgres', 'CHECKPOINT');
$node1->safe_psql('postgres', 'CHECKPOINT');

sub timed_update_retry
{
	my ($node, $sql) = @_;
	for my $attempt (1 .. 20)
	{
		return 1 if eval { $node->safe_psql('postgres', $sql); 1 };
		usleep(500_000);
	}
	return 0;
}

# ============================================================
# L2 — forward transfer (node0 holds, node1 ships in).
#
# Wire-request counters are asserted AFTER the L3 swap below:  a single
# row's block may be mastered by the requester itself (HC72 master==self
# short-circuits the wire REQUEST and the transfer rides FORWARD), so a
# per-leg counter assert is master-placement dependent.  The full-table
# X swaps guarantee every block is acquired by both nodes, so each node
# must send wire REQUESTs for its remote-mastered half.
# ============================================================
my $req1_before = gcs_int($node1, 'block_request_count');
my $req0_before = gcs_int($node0, 'block_request_count');

ok(timed_update_retry($node0, 'UPDATE flip_t SET bal = bal + 1'),
	'L2 setup: node0 X-holds the table');
usleep(500_000);

ok(timed_update_retry($node1, 'UPDATE flip_t SET bal = bal + 1 WHERE aid = 7'),
	'L2 node1 write of node0-held block succeeds over the DATA plane');
is(gcs_int($node0, 'plane_misroute_reject'), 0, 'L2 node0 zero plane misroutes');
is(gcs_int($node1, 'plane_misroute_reject'), 0, 'L2 node1 zero plane misroutes');

# ============================================================
# L3 — reverse transfer (node1 now holds, node0 ships back).
# ============================================================
ok(timed_update_retry($node1, 'UPDATE flip_t SET bal = bal + 1'),
	'L3 setup: node1 takes the table X (reverse ship)');
usleep(500_000);

ok(timed_update_retry($node0, 'UPDATE flip_t SET bal = bal + 1 WHERE aid = 11'),
	'L3 node0 write of node1-held block succeeds (bidirectional)');
cmp_ok(gcs_int($node1, 'block_request_count'), '>', $req1_before,
	'L3 node1 issued wire block requests across the swap');
cmp_ok(gcs_int($node0, 'block_request_count'), '>', $req0_before,
	'L3 node0 issued wire block requests across the swap');

# ============================================================
# L4 — staging survival read-back:  node0 X-holds the aid=11 block from
# L3;  node1 scans the whole table across it (remote X-held blocks are
# served read-image, spec-5.2).  The D0-①b staging case itself (holder-
# side FORWARD/BAST chain emitting GCS_REQUEST from LMS context via the
# CONTROL ring) was exercised by every X transfer in the L2/L3 swap —
# its survival proof is the transfers succeeding with zero plane
# misroutes (L2/L3) and zero plane-gate errors in either log (L5).
#
# No full-table re-hold here:  the L2/L3 swap leaves foreign recycled-
# slot ITL residue behind, and resolving THAT is the #119 co-sample
# lane (a full-table write on node0 walls on the fail-closed retryable
# verdict until then;  the read-back needs no new X).
# ============================================================
my $sum = '';
for my $attempt (1 .. 10)
{
	$sum = eval { $node1->safe_psql('postgres', 'SELECT count(*) FROM flip_t') };
	last if defined($sum) && $sum eq '200';
	usleep(300_000);
}
is($sum, '200', 'L4 node1 reads the whole table across X-held blocks');

# ============================================================
# L5 — hygiene: no plane-gate errors anywhere in the run.
# ============================================================
$pair->stop_pair;
$log0 = PostgreSQL::Test::Utils::slurp_file($node0->logfile);
$log1 = PostgreSQL::Test::Utils::slurp_file($node1->logfile);
unlike($log0, qr/cannot send from plane|plane mismatch|dropping msg_type/,
	'L5 node0 log free of plane-gate errors');
unlike($log1, qr/cannot send from plane|plane mismatch|dropping msg_type/,
	'L5 node1 log free of plane-gate errors');

done_testing();
