#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 283_tm_table_lock_convert.pl
#    spec-5.3 -- TM table-lock cross-node convert (the first LIVE consumer of
#    the GES enqueue-convert + BAST-drain state machine).
#
#    A same-backend upgrade of an already-held cluster-registered weaker table
#    lock (LOCK TABLE ... IN SHARE MODE, then a stronger mode) is routed as an
#    Oracle DLM convert (opcode-2 CONVERT): the master upgrades the holder slot
#    in place and rebinds its request id (release-ownership, §3.1a), rather than
#    adding a second additive holder.
#
#    Legs (real 2-node, no SKIP -- L77 / spec-5.2 ship-gate precedent):
#      L1  pair alive + connected + same-relfilenode table + convert counters
#          present in the grd dump and 0 at a fresh start (D8 baseline).
#      L2  single-holder convert: one backend LOCKs t IN SHARE MODE then ACCESS
#          EXCLUSIVE MODE in the same txn -> OK_CONVERTED, grd_convert_granted_
#          inplace_count increases (no second additive holder).
#      L3  cross-node conflict: node0 holds SHARE, node1 holds SHARE (compatible
#          peers); node0 upgrades to ACCESS EXCLUSIVE -> conflict -> blocks in
#          wait_event = GesConvertWait; node1 releases -> node0's convert is
#          granted (wakes + completes, not a hang).
#      L4  native-probe coexistence (8A-2): node1 holds RowExclusive natively
#          (a plain INSERT, <SUEX -> PG-native, off-cluster); node0 upgrades to
#          ACCESS EXCLUSIVE -> the LMS native-lock probe must hold the convert
#          until node1 commits, then it is granted (green-path coexistence).
#      L5  illegal convert: SHARE UPDATE EXCLUSIVE then SHARE is a LATERAL (non
#          partial-order) conversion -> 53R74 with an actionable errhint.
#      L9  additive escape hatch: cluster.tm_convert_mode = 'additive' routes the
#          L2 sequence through the additive REQUEST path -> no convert counter
#          bump, no 53R74.
#      L10 release collapse: after a convert + COMMIT, the converted lock is
#          released exactly once (no leak / no double release; the pair stays
#          usable for a follow-up acquire).
#
#    Harness: ClusterPair shared_data + 3 voting disks + autovacuum off.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/283_tm_table_lock_convert.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-5.3-tm-table-lock-cross-node.md
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(usleep);

# Fire a query EXPECTED TO BLOCK without waiting for it to finish.
sub bg_start_blocking
{
	my ($h, $sql) = @_;
	$h->query_until(qr/PGRAC_FIRED/, "\\echo PGRAC_FIRED\n$sql;\n");
}

# Poll a node's pg_stat_activity until a backend running $qlike is in $event.
sub wait_for_wait_event
{
	my ($node, $qlike, $event, $secs) = @_;
	my $deadline = time() + $secs;
	while (time() < $deadline)
	{
		my $we = $node->safe_psql(
			'postgres', qq{
			SELECT coalesce(wait_event, '') FROM pg_stat_activity
			WHERE query LIKE '$qlike' AND pid <> pg_backend_pid()
			  AND state = 'active' LIMIT 1});
		return 1 if defined $we && $we eq $event;
		usleep(200_000);
	}
	return 0;
}

# Poll until no backend on $node is still ACTIVE running $qlike (it completed).
sub wait_for_query_done
{
	my ($node, $qlike, $secs) = @_;
	my $deadline = time() + $secs;
	while (time() < $deadline)
	{
		my $n = $node->safe_psql(
			'postgres', qq{
			SELECT count(*) FROM pg_stat_activity
			WHERE query LIKE '$qlike' AND state = 'active'
			  AND pid <> pg_backend_pid()});
		return 1 if defined $n && $n eq '0';
		usleep(200_000);
	}
	return 0;
}

# Sum a grd counter across both nodes (the master may be either node).
sub convert_count
{
	my ($pair, $key) = @_;
	my $sum = 0;
	for my $node ($pair->node0, $pair->node1)
	{
		my $v = $node->safe_psql(
			'postgres', qq{
			SELECT coalesce(value::bigint, 0) FROM pg_cluster_state
			WHERE category = 'grd' AND key = '$key'});
		$sum += ($v // 0);
	}
	return $sum;
}


# ----------
# L1: strict pair + shared data + same-DDL (same relfilenode) + counter baseline.
# ----------
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'tm_convert',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.ges_request_timeout_ms = 30000',
		'cluster.ges_convert_timeout_ms = 30000',
		# CI runners execute many shards in parallel; widen the CSSD misscount
		# so a healthy peer is not falsely declared DEAD while a lock-hold
		# window is open (mirrors t/280 / t/281).
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
	]);
$pair->start_pair;
usleep(3_000_000);

is($pair->node0->safe_psql('postgres', 'SELECT 1'), '1', 'L1 node0 alive');
is($pair->node1->safe_psql('postgres', 'SELECT 1'), '1', 'L1 node1 alive');
ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'L1 node0 sees node1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'L1 node1 sees node0 connected');

$pair->node0->safe_psql('postgres', 'CREATE TABLE t (id int, ctr int)');
$pair->node1->safe_psql('postgres', 'CREATE TABLE t (id int, ctr int)');
my $p0 = $pair->node0->safe_psql('postgres', "SELECT pg_relation_filepath('t')");
my $p1 = $pair->node1->safe_psql('postgres', "SELECT pg_relation_filepath('t')");
if ($p0 ne $p1)
{
	$pair->stop_pair;
	plan skip_all => "same-DDL relfilepath coincidence does not hold (n0=$p0 n1=$p1)";
}

is($pair->node0->safe_psql(
		'postgres', qq{
		SELECT count(*) FROM pg_cluster_state WHERE category = 'grd'
		  AND key IN ('grd_convert_granted_inplace_count',
		              'grd_convert_enqueued_count', 'grd_convert_illegal_count')}),
	'3', 'L1 convert verdict counters present in the grd dump (D8)');
is(convert_count($pair, 'grd_convert_granted_inplace_count'),
	0, 'L1 no convert granted at a fresh start');


# ----------
# L2: single-holder convert (no peer conflict): SHARE then ACCESS EXCLUSIVE in
# one txn -> OK_CONVERTED + grd_convert_granted_inplace_count increases.
# ----------
my $before = convert_count($pair, 'grd_convert_granted_inplace_count');
$pair->node0->safe_psql(
	'postgres', q{
	BEGIN;
	LOCK TABLE t IN SHARE MODE;
	LOCK TABLE t IN ACCESS EXCLUSIVE MODE;
	COMMIT;});
ok(convert_count($pair, 'grd_convert_granted_inplace_count') > $before,
	'L2 same-backend SHARE -> ACCESS EXCLUSIVE upgrade is a live convert (count++)');


# ----------
# L3: cross-node conflict -> GesConvertWait -> grant on peer release.
# ----------
my $h1 = $pair->node1->background_psql('postgres', on_error_die => 1);
$h1->query_safe('BEGIN');
$h1->query_safe('LOCK TABLE t IN SHARE MODE');    # node1 holds SHARE (peer)

my $h0 = $pair->node0->background_psql('postgres', on_error_die => 1);
$h0->query_safe('BEGIN');
$h0->query_safe('LOCK TABLE t IN SHARE MODE');    # node0 holds SHARE (compatible)
# node0 upgrades to ACCESS EXCLUSIVE: conflicts with node1's SHARE -> enqueue +
# block on the convert wait.
bg_start_blocking($h0, 'LOCK TABLE t IN ACCESS EXCLUSIVE MODE');
ok(wait_for_wait_event($pair->node0, '%ACCESS EXCLUSIVE%', 'GesConvertWait', 25),
	'L3 conflicting upgrade blocks in GesConvertWait (convert enqueued)');

$h1->query_safe('COMMIT');    # node1 releases SHARE -> drains node0's convert
ok(wait_for_query_done($pair->node0, '%ACCESS EXCLUSIVE%', 25),
	'L3 node0 convert is granted after peer release (wakes, not a hang)');
$h0->query_safe('COMMIT');
$h0->quit;
$h1->quit;


# ----------
# L4: native-probe coexistence (8A-2).  node1 holds RowExclusive natively (a
# plain INSERT inside an open txn -- <SUEX, PG-native, off the cluster gate);
# node0's upgrade to ACCESS EXCLUSIVE must be held by the native-lock probe
# until node1 commits, then granted.
# ----------
my $hn1 = $pair->node1->background_psql('postgres', on_error_die => 1);
$hn1->query_safe('BEGIN');
$hn1->query_safe('INSERT INTO t VALUES (4, 4)');    # node1 holds RowExclusive (native)

my $hn0 = $pair->node0->background_psql('postgres', on_error_die => 1);
$hn0->query_safe('BEGIN');
$hn0->query_safe('LOCK TABLE t IN SHARE MODE');
bg_start_blocking($hn0, 'LOCK TABLE t IN ACCESS EXCLUSIVE MODE');
# It must NOT complete while node1's native RowExclusive is held.
ok(!wait_for_query_done($pair->node0, '%ACCESS EXCLUSIVE%', 4),
	'L4 upgrade waits while a remote native RowExclusive is held (native-probe)');
$hn1->query_safe('COMMIT');    # release the native conflict
ok(wait_for_query_done($pair->node0, '%ACCESS EXCLUSIVE%', 25),
	'L4 upgrade is granted after the remote native holder commits');
$hn0->query_safe('COMMIT');
$hn0->quit;
$hn1->quit;


# ----------
# L5: illegal (LATERAL) convert -> 53R74.
# ----------
my ($rc5, $out5, $err5) = $pair->node0->psql(
	'postgres', q{
	BEGIN;
	LOCK TABLE t IN SHARE UPDATE EXCLUSIVE MODE;
	LOCK TABLE t IN SHARE MODE;
	COMMIT;});
ok($rc5 != 0 && $err5 =~ /53R74|not a valid upgrade|invalid cluster lock mode conversion/,
	'L5 a LATERAL convert is rejected with 53R74 (illegal lock conversion)')
	or diag("L5 rc=$rc5 err=$err5");


# ----------
# L9: additive escape hatch -- no convert, no 53R74.
# ----------
$pair->node0->safe_psql('postgres', "ALTER SYSTEM SET cluster.tm_convert_mode = 'additive'");
$pair->node0->safe_psql('postgres', 'SELECT pg_reload_conf()');
usleep(500_000);
my $before9 = convert_count($pair, 'grd_convert_granted_inplace_count');
$pair->node0->safe_psql(
	'postgres', q{
	BEGIN;
	LOCK TABLE t IN SHARE MODE;
	LOCK TABLE t IN ACCESS EXCLUSIVE MODE;
	COMMIT;});
is(convert_count($pair, 'grd_convert_granted_inplace_count'),
	$before9, 'L9 additive mode does NOT run a convert (no counter bump)');
$pair->node0->safe_psql('postgres', "ALTER SYSTEM SET cluster.tm_convert_mode = 'convert'");
$pair->node0->safe_psql('postgres', 'SELECT pg_reload_conf()');
usleep(500_000);


# ----------
# L10: release collapse -- a convert + commit leaves the resource fully free.
# ----------
$pair->node0->safe_psql(
	'postgres', q{
	BEGIN;
	LOCK TABLE t IN SHARE MODE;
	LOCK TABLE t IN ACCESS EXCLUSIVE MODE;
	COMMIT;});
# A fresh AccessExclusive from node1 must acquire immediately (no leaked holder).
my ($rc10, $out10, $err10) = $pair->node1->psql(
	'postgres', q{
	BEGIN;
	LOCK TABLE t IN ACCESS EXCLUSIVE MODE;
	COMMIT;});
is($rc10, 0, 'L10 a converted lock is released cleanly (no leaked holder blocks the peer)')
	or diag("L10 rc=$rc10 err=$err10");


# ----------
# L11 (review P0-2): a multi-level convert chain across nested subxacts must
# unwind correctly.  node0: SUEX -> (sp1) SRE -> (sp2) AE; roll back sp2 then
# sp1 -> node0 still holds SUEX.  The master must still show that hold: a peer
# ACCESS EXCLUSIVE must BLOCK (if the chain backout wrongly deleted the master
# slot while the weak SUEX hold survives, the peer would acquire immediately =
# false-grant).
# ----------
my $hc = $pair->node0->background_psql('postgres', on_error_die => 1);
$hc->query_safe('BEGIN');
$hc->query_safe('LOCK TABLE t IN SHARE UPDATE EXCLUSIVE MODE');    # SUEX (parent)
$hc->query_safe('SAVEPOINT sp1');
$hc->query_safe('LOCK TABLE t IN SHARE ROW EXCLUSIVE MODE');       # convert SUEX -> SRE
$hc->query_safe('SAVEPOINT sp2');
$hc->query_safe('LOCK TABLE t IN ACCESS EXCLUSIVE MODE');          # convert SRE -> AE
$hc->query_safe('ROLLBACK TO SAVEPOINT sp2');                      # back out AE -> SRE
$hc->query_safe('ROLLBACK TO SAVEPOINT sp1');                      # back out SRE -> SUEX (node0 holds SUEX)

my $hp = $pair->node1->background_psql('postgres', on_error_die => 1);
$hp->query_safe('BEGIN');
bg_start_blocking($hp, 'LOCK TABLE t IN ACCESS EXCLUSIVE MODE');
ok(!wait_for_query_done($pair->node1, '%ACCESS EXCLUSIVE%', 5),
	'L11 multi-level convert backout keeps the weak hold (peer AE blocks, no false-grant)');

$hc->query_safe('ROLLBACK');    # node0 releases SUEX
ok(wait_for_query_done($pair->node1, '%ACCESS EXCLUSIVE%', 25),
	'L11 peer AE acquires after node0 releases the surviving weak hold');
$hp->query_safe('COMMIT');
$hp->quit;
$hc->quit;


$pair->stop_pair;
done_testing();
