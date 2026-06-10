#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 239_cluster_3_24_no_peer_fastpath.pl
#	  spec-3.24 D1 -- 8.A correctness differential for the no-peer + session-
#	  local CR-gate fast path.
#
#	  D1 (cluster.cr_gate_no_peer_fastpath) routes a no-peer + session-local
#	  cluster snapshot to the PG-native MVCC body instead of the CR/SCN cluster
#	  fork (AD-012 例外 9 row #1: local xid + locally-created snapshot ->
#	  ProcArray-native).  Because the fast path REPLACES the CR gate verdict on
#	  the hot post-read_scn path, it MUST be verdict-equivalent to the CR path
#	  it bypasses, and MUST NOT introduce a false-visible / false-invisible
#	  (the spec-3.19/3.21 regression classes).  It also MUST NOT apply to an
#	  imported snapshot (AD-012 row #2), which keeps the CR/SCN path.
#
#	  Deterministic (ordered sessions, no race):
#	    A  the canonical RR-snapshot-vs-concurrent-committed-UPDATE scenario
#	       gives IDENTICAL results with the fast path OFF (CR path) and ON
#	       (PG-native): old value seen once, row kept, fresh sees new.
#	    B  self-update visibility holds under the fast path.
#	    D  an imported snapshot (SET TRANSACTION SNAPSHOT) stays correct with
#	       the fast path on (excluded -> CR/SCN path).
#	    E  cluster=off native parity.
#
#	  Author: SqlRush <sqlrush@gmail.com>
#	  Spec: spec-3.24-cr-gate-write-path-perf-optimization.md (D1)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# The canonical 8.A scenario (identical to spec-3.19 t/235): a repeatable-read
# snapshot reads a row, a concurrent autocommit UPDATE commits AFTER that
# snapshot, and the snapshot re-reads.  The page block_scn / slot write_scn
# advance past the reader's read_scn, so on the CR path the gate fires.
sub _snapshot_isolation
{
	my ($node) = @_;

	$node->safe_psql('postgres',
		'DROP TABLE IF EXISTS s324a; CREATE TABLE s324a (id int primary key, v int);'
		  . ' INSERT INTO s324a VALUES (1, 100);');

	my $s1 = $node->background_psql('postgres', on_error_die => 1);
	$s1->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
	my $before = $s1->query_safe('SELECT v FROM s324a WHERE id = 1');

	$node->safe_psql('postgres', 'UPDATE s324a SET v = 200 WHERE id = 1');

	my $after = $s1->query_safe('SELECT v FROM s324a WHERE id = 1');
	my $cnt   = $s1->query_safe('SELECT count(*) FROM s324a WHERE id = 1');
	$s1->query_safe('COMMIT');
	$s1->quit;

	my $fresh = $node->safe_psql('postgres', 'SELECT v FROM s324a WHERE id = 1');

	return { before => $before, after => $after, cnt => $cnt, fresh => $fresh };
}

# Build a single-node cluster with the fast-path GUC set to $fastpath.
sub _cluster_node
{
	my ($name, $fastpath) = @_;
	my $n = PostgreSQL::Test::Cluster->new($name);
	$n->init;
	$n->append_conf('postgresql.conf',
		    "cluster.enabled = on\n"
		  . "cluster.node_id = 0\n"
		  . "cluster.allow_single_node = on\n"
		  . "cluster.interconnect_tier = stub\n"
		  . "cluster.cr_mvcc_gate = on\n"
		  . "cluster.cr_gate_no_peer_fastpath = $fastpath\n"
		  . "autovacuum = off\n");
	$n->start;
	return $n;
}

# Read a pg_cluster_state counter (instance-wide; workers bump the same shmem).
sub _counter
{
	my ($node, $cat, $key) = @_;
	return $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state WHERE category='$cat' AND key='$key'});
}

# ----------------------------------------------------------------------
# A: differential -- CR path (fast path OFF) vs PG-native (fast path ON).
# ----------------------------------------------------------------------
my $off = _cluster_node('s324_fp_off', 'off');
my $rc  = _snapshot_isolation($off);

my $on = _cluster_node('s324_fp_on', 'on');
my $rf = _snapshot_isolation($on);

for my $k (qw(before after cnt fresh))
{
	is($rf->{$k}, $rc->{$k},
		"A diff: '$k' fast-path-on == CR-path ($rc->{$k})");
}

# Absolute 8.A correctness under the fast path (not just equal to CR path).
is($rf->{after}, '100',
	'A fast-path-on: RR snapshot STILL sees old v=100 (no false-visible)');
is($rf->{cnt}, '1',
	'A fast-path-on: RR snapshot row not dropped (no false-invisible)');
is($rf->{fresh}, '200',
	'A fast-path-on: fresh snapshot sees committed v=200');

# ----------------------------------------------------------------------
# B: self-update visibility under the fast path (own xmin handled natively).
# ----------------------------------------------------------------------
my $b = $on->safe_psql('postgres', q{
	BEGIN;
	CREATE TABLE s324b (id int primary key, v int);
	INSERT INTO s324b VALUES (2, 1);
	UPDATE s324b SET v = 2 WHERE id = 2;
	SELECT v FROM s324b WHERE id = 2;
	COMMIT;
});
is($b, '2', 'B fast-path-on: txn sees its own UPDATE of its own row');

# ----------------------------------------------------------------------
# D: an imported cluster snapshot is excluded from the fast path.  In cluster
#    mode SET TRANSACTION SNAPSHOT of an exported snapshot already fail-closes
#    ("invalid cluster snapshot fields", spec-3.3), so a SET-TRANSACTION-
#    SNAPSHOT importer can never reach the no-peer fast path -- the exclusion is
#    enforced before any visibility check.  (The parallel-worker import path,
#    RestoreSnapshot, clears session_local=0 itself -- it is called directly by
#    parallel scans, bypassing SetTransactionSnapshot -- and is covered by
#    scenario F below.)
# ----------------------------------------------------------------------
my $keeper = $on->background_psql('postgres', on_error_die => 1);
$keeper->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
my $sid = $keeper->query_safe('SELECT pg_export_snapshot()');

my ($imp_out, $imp_err) = ('', '');
$on->psql('postgres',
	"BEGIN ISOLATION LEVEL REPEATABLE READ; SET TRANSACTION SNAPSHOT '$sid';",
	stdout => \$imp_out, stderr => \$imp_err,
	extra_params => [ '-v', 'ON_ERROR_STOP=0' ]);
like($imp_err, qr/invalid cluster snapshot fields/,
	'D fast-path-on: imported cluster snapshot fail-closes (cannot reach fast path)');

$keeper->query_safe('COMMIT');
$keeper->quit;

# ----------------------------------------------------------------------
# F: a restored (parallel-worker) snapshot must NOT take the fast path.
#    RestoreSnapshot() is called directly by parallel table/index/bitmap-heap
#    scans, bypassing SetTransactionSnapshot().  Under fast-path-on, a parallel
#    RR query whose workers read rows modified AFTER the snapshot must (a) still
#    see the OLD values (no false-visible), and (b) the workers must take the CR
#    path (cr_construct_count rises) -- proving the restored snapshot is excluded
#    from the fast path (session_local=0), not silently routed to PG-native.
# ----------------------------------------------------------------------
$on->safe_psql('postgres',
	'DROP TABLE IF EXISTS s324f; CREATE TABLE s324f (id int, v int);'
	  . ' INSERT INTO s324f SELECT g, 100 FROM generate_series(1, 500) g;');

my $cc_before = _counter($on, 'cr', 'cr_construct_count');

my $par = $on->background_psql('postgres', on_error_die => 1);
$par->query_safe('SET debug_parallel_query = on');
$par->query_safe('SET parallel_leader_participation = off');
$par->query_safe('SET max_parallel_workers_per_gather = 2');
$par->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
my $psum_before = $par->query_safe('SELECT sum(v) FROM s324f');

# Concurrent autocommit UPDATE commits AFTER the parallel reader's snapshot.
$on->safe_psql('postgres', 'UPDATE s324f SET v = 200');

my $psum_after = $par->query_safe('SELECT sum(v) FROM s324f');
$par->query_safe('COMMIT');
$par->quit;

my $cc_after = _counter($on, 'cr', 'cr_construct_count');

is($psum_before, '50000',
	'F parallel RR snapshot reads old sum=50000 before concurrent UPDATE');
is($psum_after, '50000',
	'F fast-path-on: parallel RR snapshot STILL sees old sum=50000 '
	. '(no false-visible via restored worker snapshot)');
cmp_ok($cc_after, '>', $cc_before,
	'F fast-path-on: restored worker snapshot took the CR path '
	. '(excluded from fast path), not PG-native');

$on->stop;
$off->stop;

# ----------------------------------------------------------------------
# E: cluster=off native parity -- the differential baseline is PG-native too.
# ----------------------------------------------------------------------
my $native = PostgreSQL::Test::Cluster->new('s324_native');
$native->init;
$native->append_conf('postgresql.conf', "autovacuum = off\n");
$native->start;

my $rn = _snapshot_isolation($native);
is($rn->{after}, '100', 'E cluster-off native parity: RR snapshot sees old v=100');
is($rn->{cnt},   '1',   'E cluster-off native parity: row not dropped');
is($rn->{fresh}, '200', 'E cluster-off native parity: fresh sees v=200');

$native->stop;

done_testing();
