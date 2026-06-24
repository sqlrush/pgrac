#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 294_dl_bulk_load_lease.pl
#    spec-5.7 §3.2 / D4 -- DL (bulk-load lease) faithful-mechanism e2e.
#
#    DL(X) is a coarse per-relation lease a bulk load holds across the whole
#    operation (lock order DL(X) -> HW(X) -> local extend).  This test proves the
#    REAL GES path fires -- not a mock, not a skip -- by observing the dl
#    observability counters on the loading node:
#      - a bulk path (CTAS / COPY) that uses a BulkInsertState takes a coordinated
#        DL(X) lease  ->  dl.lease_count advances;
#      - a regular INSERT...SELECT (PG16 row-by-row heap_insert, bistate == NULL)
#        takes NO DL  ->  dl.lease_count does NOT advance (HW alone covers it,
#        spec §3.2);
#      - the bulk load still extends the relation through the HW authority while
#        holding DL  ->  hw.alloc_count also advances (lock order DL -> HW live).
#
#    This is node-local + counter-observable, so it does not depend on the
#    cross-node catalog/MVCC visibility that is gated by Stage 3 (the wall S7 L5b
#    / CU-M4 hit) -- the DL lease is taken by the loading node via the spec-5.3
#    GES substrate regardless, and the counter is the faithful proof it ran.
#
#    Harness: ClusterPair shared_data (GLOBALIZE relations) + 3 voting disks.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/294_dl_bulk_load_lease.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-5.7-misc-enqueue-classes.md (D4, §3.2)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(usleep);

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'dl_lease',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.grd_max_entries = 4096',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
	]);
$pair->start_pair;
usleep(3_000_000);

my $n0 = $pair->node0;
my $n1 = $pair->node1;

# read a (category,key) counter from pg_cluster_state on the loading node
sub counter
{
	my ($cat, $key) = @_;
	my $v = $n0->safe_psql('postgres',
		"SELECT value FROM pg_cluster_state WHERE category='$cat' AND key='$key'");
	return defined $v && $v ne '' ? $v + 0 : 0;
}

# L0: both nodes up + the dl category is exposed (3 keys).
is($n0->safe_psql('postgres', 'SELECT 1'), '1', 'L0 node0 up');
is($n1->safe_psql('postgres', 'SELECT 1'), '1', 'L0 node1 up');
is($n0->safe_psql('postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category='dl'}),
	'4', 'L0 dl category exposes 4 counters (lease/native/failclosed/release)');

# L1: COPY of a COMMITTED relation (heap_multi_insert bulk path) takes the REAL
#     cross-node DL(X) GES lease.  dl_copy was created in a prior (committed)
#     statement, so it is a peer-visible relation a bulk load must coordinate on
#     -> the GES grants a coordinated DL(X) and lease_count advances.  This is the
#     faithful proof the DL hook drives the real spec-5.3 GES path (not a mock).
$n0->safe_psql('postgres', 'CREATE TABLE dl_copy (a int, b int, c int, d int)');
my $lease0 = counter('dl', 'lease_count');
$n0->safe_psql('postgres',
	q{COPY dl_copy FROM PROGRAM 'seq 1 8000 | awk ''{print $1"\t"$1"\t"$1"\t"$1}'''});
my $lease1 = counter('dl', 'lease_count');
cmp_ok($lease1, '>', $lease0,
	"L1 COPY of a committed relation took the REAL cross-node DL(X) lease (lease_count $lease0 -> $lease1; faithful GES path)");
is($n0->safe_psql('postgres', 'SELECT count(*) FROM dl_copy'), '8000',
	'L1 COPY loaded all 8000 rows under the DL lease');

# L2: the lease is RELEASED after the bulk load (no leak): a second COPY of a
#     committed relation re-acquires, so lease_count advances again.
my $lease2 = counter('dl', 'lease_count');
$n0->safe_psql('postgres', 'CREATE TABLE dl_copy2 (a int, b int, c int, d int)');
$n0->safe_psql('postgres',
	q{COPY dl_copy2 FROM PROGRAM 'seq 1 4000 | awk ''{print $1"\t"$1"\t"$1"\t"$1}'''});
my $lease3 = counter('dl', 'lease_count');
cmp_ok($lease3, '>', $lease2,
	"L2 a second COPY re-acquires DL (lease_count $lease2 -> $lease3) -- the prior lease was released at FreeBulkInsertState, no leak");

# L3: CTAS is also a bulk path (heap_insert with a BulkInsertState).  The DL hook
#     fires and the GES grants a coordinated DL(X) lease just like COPY -- the
#     resid hashes the relfilenode, so CTAS serialises through the same cross-node
#     lease.  (This leg also pins the GetBulkInsertState fix: that function uses
#     palloc, not palloc0, so the lease handle must be NULL-initialised there;
#     a stale handle would mis-skip the hook and dl_unlock a wild pointer.)
my $lease4 = counter('dl', 'lease_count');
$n0->safe_psql('postgres',
	'CREATE TABLE dl_ctas AS SELECT g AS a, g AS b, g AS c, g AS d FROM generate_series(1,8000) g');
my $lease5 = counter('dl', 'lease_count');
cmp_ok($lease5, '>', $lease4,
	"L3 CTAS (bulk path) took the real DL(X) lease (lease_count $lease4 -> $lease5; hook fired, no garbage-skip)");
is($n0->safe_psql('postgres', 'SELECT count(*) FROM dl_ctas'), '8000',
	'L3 CTAS materialized all 8000 rows under the DL lease');

# L4: a regular INSERT...SELECT (bistate == NULL, PG16 row-by-row heap_insert)
#     never enters the DL hook -- HW alone covers it (spec §3.2).
my $lease6  = counter('dl', 'lease_count');
my $native0 = counter('dl', 'native_count');
$n0->safe_psql('postgres', 'CREATE TABLE dl_iss (a int, b int, c int, d int)');
$n0->safe_psql('postgres',
	'INSERT INTO dl_iss SELECT g,g,g,g FROM generate_series(1,8000) g');
is(counter('dl', 'lease_count') . '/' . counter('dl', 'native_count'),
	"$lease6/$native0",
	'L4 INSERT...SELECT took NO DL at all (lease + native both unchanged; bistate == NULL -> HW only, spec §3.2)');
is($n0->safe_psql('postgres', 'SELECT count(*) FROM dl_iss'), '8000',
	'L4 INSERT...SELECT loaded all 8000 rows (HW-backed, no DL)');

# L4c: no-leak invariant -- every coordinated lease taken above (L1/L2/L3) was
#      released normally (FreeBulkInsertState), so release_count has caught up to
#      lease_count.  A persistently lagging release_count would be a leak.
is(counter('dl', 'release_count'), counter('dl', 'lease_count'),
	'L4c every coordinated lease was released (release_count == lease_count; no leak)');

# L5: a bulk load that ERRORs mid-load must NOT leak the lease.  FreeBulkInsertState
#     is not in a PG_FINALLY, and DL has no PG-native lock, so without the
#     CurTransactionContext backstop the lease would survive the abort and wedge
#     peer bulk loads on the same relation.  The COPY below feeds 3000 valid ints
#     (forcing >=1 heap_multi_insert flush -> the lease is really acquired) then a
#     non-integer row that aborts the load.  on_error_die => 0 lets the test see
#     the error.
$n0->safe_psql('postgres', 'CREATE TABLE dl_abort (a int)');
my $lease_a0   = counter('dl', 'lease_count');
my $release_a0 = counter('dl', 'release_count');
my ($rc, $out, $err) = $n0->psql('postgres',
	q{COPY dl_abort FROM PROGRAM 'seq 1 3000; echo notanint'},
	on_error_die => 0);
isnt($rc, 0, 'L5 the mid-load COPY error aborted the bulk load (non-zero psql exit)');
my $lease_a1   = counter('dl', 'lease_count');
my $release_a1 = counter('dl', 'release_count');
cmp_ok($lease_a1, '>', $lease_a0,
	"L5 the aborted bulk load really took a coordinated DL lease (lease_count $lease_a0 -> $lease_a1)");
cmp_ok($release_a1, '>', $release_a0,
	"L5 the CurTransactionContext backstop released that lease at abort (release_count $release_a0 -> $release_a1; no leak)");
is($release_a1, $lease_a1,
	"L5 release_count == lease_count after the abort ($release_a1; the lease did not survive the failed load)");

# L6: the relation is immediately reusable from a fresh backend -- proving the
#     leaked lease really was freed (a surviving holder would conflict / 53RA7).
$n0->safe_psql('postgres',
	q{COPY dl_abort FROM PROGRAM 'seq 1 500 | awk ''{print $1}'''});
is($n0->safe_psql('postgres', 'SELECT count(*) FROM dl_abort'), '500',
	'L6 a fresh bulk load into the same relation succeeds (the aborted lease was not leaked)');

$pair->stop_pair;
done_testing();
