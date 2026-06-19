#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 279_cf_concurrent_relation_liveread.pl
#    spec-5.2 D3 — data-plane gate (1):  real 2-node concurrent same-relation
#    live read.  This is the test t/252 L3 documented as "NOT constructible"
#    until the concurrent-relation data plane existed:  the setup hung on the
#    stale 0-block relsize (M2) and there was no path for a node to read a
#    block another node holds in X (P1).
#
#    spec-5.2 fixes both halves:
#      D1 (M2)  relsize coherence — node0 extends the relation; a PG-native
#               SHAREDINVALSMGR_ID broadcast makes node1 drop its stale smgr
#               cache and re-stat the shared file (so node1 sees the block at
#               all, not a stale 0-block "row does not exist").
#      D2 (P1)  X-holder read-image ship — node0 holds X on the block (an
#               uncommitted FOR UPDATE row lock); node1's cross-node N->S read
#               gets node0's CURRENT image (status READ_IMAGE_FROM_XHOLDER),
#               node0 keeps its X, and node1 never registers as an S holder.
#
#    Legs (spec-5.2 §4.2):
#      L1   ClusterPair shared_data + same-DDL; both alive + connected.
#      L1.5 pg_relation_filepath identical on both nodes (feature #11
#           exclusion premise — same-DDL gives the same relfilenode; evidence
#           must be clean, L92).
#      L2   node0 INSERT+commit; node1 reads the committed row — proves relsize
#           coherence (D1): node1 re-stats the shared file rather than seeing a
#           stale 0-block relation.
#      L3   node0 holds X (BEGIN; SELECT ... FOR UPDATE — page dirty, holder is
#           a PCM X holder, NOT a transient buffer content lock); node1's plain
#           read of the SAME block succeeds — read-image ship (D2), not a hang,
#           not a stale read, not fail-closed.
#      L4   node1's read returns node0's value (content check, not just rc=0).
#      L5   the X-holder read-ship counter advanced (mechanism evidence).
#      L6   node0 (the X holder) is undisturbed — still reads/writes its block.
#
#    Harness:  ClusterPair shared_data + 3 voting disks + autovacuum off.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/279_cf_concurrent_relation_liveread.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-5.2-cf-liveread-dataplane-and-tx-row-lock-wait.md (D1/D2/D3)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(usleep);

sub gcs_int
{
	my ($node, $key) = @_;
	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state WHERE category='gcs' AND key='$key'});
	return (defined $v && $v ne '') ? int($v) : 0;
}

# Read with retry:  the relsize SMGR invalidation propagates asynchronously
# (node0 outbound -> IC -> node1 SI Broadcaster inbound drain -> SI queue ->
# node1 backend AcceptInvalidationMessages on lock acquire).  Poll the read
# until node0's value is visible or we time out (a stale-low nblocks would
# otherwise return an empty result — exactly the M2 bug D1 fixes).
sub read_with_retry
{
	my ($node, $sql, $deadline_s) = @_;
	my $deadline = time() + $deadline_s;
	my $last = '';
	while (time() < $deadline)
	{
		my ($rc, $out, $err) = $node->psql('postgres', $sql, timeout => 20);
		$last = defined $out ? $out : '';
		return ($rc, $last) if $rc == 0 && $last ne '';
		usleep(250_000);
	}
	return (1, $last);
}


# ----------
# L1 (L0): strict pair + shared data backend.
# ----------
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'cf_liveread',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => ['autovacuum = off']);
$pair->start_pair;
usleep(3_000_000);    # let the tier1 mesh settle before the first GCS round-trip

is($pair->node0->safe_psql('postgres', 'SELECT 1'), '1', 'L1 node0 alive');
is($pair->node1->safe_psql('postgres', 'SELECT 1'), '1', 'L1 node1 alive');
ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'L1 node0 sees node1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'L1 node1 sees node0 connected');


# ----------
# L1.5: same-DDL empty table on both nodes -> same relfilenode (feature #11
# exclusion premise).  Create both EMPTY first so the owner-agnostic adopt on
# node1 never has to overlay node0's data.
# ----------
$pair->node0->safe_psql('postgres', 'CREATE TABLE t (id int, ctr int)');
$pair->node1->safe_psql('postgres', 'CREATE TABLE t (id int, ctr int)');    # adopt

my $p0 = $pair->node0->safe_psql('postgres', "SELECT pg_relation_filepath('t')");
my $p1 = $pair->node1->safe_psql('postgres', "SELECT pg_relation_filepath('t')");
if ($p0 ne $p1)
{
	$pair->stop_pair;
	plan skip_all =>
	  "same-DDL relfilepath coincidence does not hold on this build "
	  . "(harness premise; production naming is feature #11): n0=$p0 n1=$p1";
}
is($p1, $p0, "L1.5 pg_relation_filepath identical on both nodes ($p0)");


# ----------
# L2: node0 writes a committed row.  Deliberately NO checkpoint — the row
# lives only in node0's buffer, so the shared-storage block stays empty.  That
# is what makes L3/L4 a real read-image test: node1 can only see the value by
# getting node0's CURRENT image (a storage-fallback read would see the empty
# shared block).
# ----------
$pair->node0->safe_psql('postgres', 'INSERT INTO t VALUES (1, 100)');
is($pair->node0->safe_psql('postgres', 'SELECT ctr FROM t WHERE id = 1'), '100',
	'L2 node0 wrote the committed row');


# ----------
# L3/L4/L5: node0 takes X on the block FIRST (a held FOR UPDATE row lock —
# page dirty, holder is a PCM X holder).  node0 must take X before node1 ever
# touches the block, so node1's read is a pure read-image and node1 never
# becomes an S holder (which would otherwise collide with node0's X — that
# X->S writer transfer is out of scope, spec-5.2 §1.3).
#
# Then node1's cross-node read of the SAME block proves BOTH halves at once:
#   D1 relsize coherence — node1 must re-stat to see the block exists, and
#   D2 read-image ship    — node1 must get node0's CURRENT image (not the
#                           stale-empty shared block / not fail-closed).
# ----------
my $sess0 = $pair->node0->background_psql('postgres', on_error_die => 1);
$sess0->query_safe('BEGIN');
$sess0->query_safe('SELECT ctr FROM t WHERE id = 1 FOR UPDATE');    # node0 holds PCM X

my $ship_before = gcs_int($pair->node0, 'cf_xheld_read_ship_count')
  + gcs_int($pair->node1, 'cf_xheld_read_ship_count');

my ($rc3, $out3) = read_with_retry($pair->node1, 'SELECT ctr FROM t WHERE id = 1', 25);
is($rc3, 0, 'L3 node1 read of X-held block succeeds (read-image ship, not fail-closed)');
is($out3, '100', 'L4 node1 reads node0 current image value (not stale, not 0)');

my $ship_after = gcs_int($pair->node0, 'cf_xheld_read_ship_count')
  + gcs_int($pair->node1, 'cf_xheld_read_ship_count');
cmp_ok($ship_after, '>', $ship_before,
	'L5 X-holder read-image ship counter advanced (mechanism actually exercised)');


# ----------
# L6: the X holder (node0) is undisturbed — it still reads its own row and can
# commit; no double-grant / no false invalidation of the holder.
# ----------
is($sess0->query_safe('SELECT ctr FROM t WHERE id = 1'), '100',
	'L6 node0 (X holder) still reads its own block');
$sess0->query_safe('COMMIT');
$sess0->quit;

$pair->stop_pair;
done_testing();
