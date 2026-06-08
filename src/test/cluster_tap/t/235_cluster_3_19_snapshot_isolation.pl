#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 235_cluster_3_19_snapshot_isolation.pl
#	  spec-3.19 D4/D5 -- deterministic 8.A correctness guard for the D3
#	  CR-gate live-xmin fix.
#
#	  The D3 fix makes cluster_cr_satisfies_mvcc return *out_visible = false
#	  for a live tuple whose own xmin is not a committed-and-finished version
#	  (in-progress, in the CLOG-commit / ProcArray-exit gap, or aborted).  That
#	  closes the "attempted to update invisible tuple" crash (t/234), but it
#	  edits the visibility path, so it MUST NOT introduce:
#	    - a false-invisible (a row the snapshot should see disappears), or
#	    - a false-visible (a snapshot sees a post-read_scn version).
#
#	  These cases are made deterministic with ordered sessions (no race):
#	    A  repeatable-read snapshot isolation across a concurrent committed
#	       UPDATE -- the snapshot must still see the OLD value, exactly once.
#	    B  self-update visibility -- a txn updating its own row sees its change
#	       (the guard excludes our own xact).
#	    C  cluster=off parity -- native PG gives the same A result.
#
#	  Author: SqlRush <sqlrush@gmail.com>
#	  Spec: spec-3.19-minimal-pg-touch-mvcc-boundary.md (D4/D5)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Run scenario A (RR snapshot vs concurrent committed UPDATE) and return the
# two values the long snapshot read before/after the concurrent commit.
sub _snapshot_isolation
{
	my ($node) = @_;

	$node->safe_psql('postgres',
		'DROP TABLE IF EXISTS s319a; CREATE TABLE s319a (id int primary key, v int);'
		  . ' INSERT INTO s319a VALUES (1, 100);');

	my $s1 = $node->background_psql('postgres', on_error_die => 1);
	$s1->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
	my $before = $s1->query_safe('SELECT v FROM s319a WHERE id = 1');

	# Concurrent autocommit UPDATE commits AFTER s1's snapshot was taken, so the
	# page block_scn / slot write_scn advance past s1's read_scn and the CR gate
	# fires on s1's next read of this row.
	$node->safe_psql('postgres', 'UPDATE s319a SET v = 200 WHERE id = 1');

	my $after = $s1->query_safe('SELECT v FROM s319a WHERE id = 1');
	my $cnt   = $s1->query_safe('SELECT count(*) FROM s319a WHERE id = 1');
	$s1->query_safe('COMMIT');
	$s1->quit;

	# A fresh snapshot after everyone committed must see the new value.
	my $fresh = $node->safe_psql('postgres', 'SELECT v FROM s319a WHERE id = 1');

	return { before => $before, after => $after, cnt => $cnt, fresh => $fresh };
}


# ----------------------------------------------------------------------
# cluster.enabled = on, CR gate on (the path the fix lives in).
# ----------------------------------------------------------------------
my $on = PostgreSQL::Test::Cluster->new('s319_iso_on');
$on->init;
$on->append_conf('postgresql.conf',
	    "cluster.enabled = on\n"
	  . "cluster.node_id = 0\n"
	  . "cluster.allow_single_node = on\n"
	  . "cluster.interconnect_tier = stub\n"
	  . "cluster.cr_mvcc_gate = on\n"
	  . "autovacuum = off\n");
$on->start;

my $a = _snapshot_isolation($on);
is($a->{before}, '100', 'A1 (cluster on) RR snapshot reads v=100');
is($a->{after}, '100',
	'A2 (cluster on) RR snapshot STILL sees old v=100 after concurrent commit '
	. '(no false-visible)');
is($a->{cnt}, '1',
	'A3 (cluster on) RR snapshot row not dropped (no false-invisible)');
is($a->{fresh}, '200', 'A4 (cluster on) fresh snapshot sees committed v=200');

# Scenario B: self-update visibility (own xmin is excluded from the D3 guard).
my $b = $on->safe_psql('postgres', q{
	BEGIN;
	CREATE TABLE s319b (id int primary key, v int);
	INSERT INTO s319b VALUES (2, 1);
	UPDATE s319b SET v = 2 WHERE id = 2;
	SELECT v FROM s319b WHERE id = 2;
	COMMIT;
});
is($b, '2', 'B (cluster on) txn sees its own UPDATE of its own row (self-update)');

$on->stop;


# ----------------------------------------------------------------------
# cluster.enabled = off parity: native PG must give the same A result.
# ----------------------------------------------------------------------
my $off = PostgreSQL::Test::Cluster->new('s319_iso_off');
$off->init;
$off->append_conf('postgresql.conf', "autovacuum = off\n");
$off->start;

my $c = _snapshot_isolation($off);
is($c->{after}, '100',
	'C (cluster off) native RR snapshot also sees old v=100 (parity)');
is($c->{cnt}, '1', 'C (cluster off) native RR row not dropped (parity)');
is($c->{fresh}, '200', 'C (cluster off) native fresh snapshot sees v=200 (parity)');

$off->stop;

done_testing();
