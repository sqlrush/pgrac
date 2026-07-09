#-------------------------------------------------------------------------
#
# 014_ic_mock.pl
#    End-to-end regression for the cluster_ic mock vtable + four
#    cluster_ic_mock_* SRFs introduced at stage 0.26.
#
#    Stage 0.26 adds an in-memory queue-backed interconnect tier that
#    lets a single PG instance simulate cross-node IPC at the SQL
#    surface.  The four SRFs (inject / drain_outbound / clear_all /
#    recv_test) drive the mock state from the calling backend; this
#    TAP file exercises them via a single psql session, since the
#    queues are per-backend statics.  The unit test in
#    cluster_unit/test_cluster_ic_mock.c locks the symbol surface;
#    this file locks the runtime semantics.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/014_ic_mock.pl
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

use PostgreSQL::Test::Cluster;
use Test::More;
use PgracClusterNode;


my $node = PgracClusterNode->new('main');
$node->init;
$node->append_conf('postgresql.conf', "cluster.interconnect_tier = mock\n");
$node->start;


# ----------
# Test 1: cluster.interconnect_tier = mock startup OK.
# ----------
$node->assert_cluster_guc('cluster.interconnect_tier', 'mock',
	'cluster.interconnect_tier = mock starts cleanly');


# ----------
# Test 2: pg_proc exposes all four cluster_ic_mock_* SRFs.
# ----------
is( $node->safe_psql(
		'postgres',
		q{SELECT count(*)
		    FROM pg_proc
		   WHERE proname LIKE 'cluster_ic_mock_%'}),
	'4',
	'pg_proc has 4 cluster_ic_mock_* entries');


# ----------
# Test 3: inject + recv_test single-message roundtrip.
# ----------
my $rt_out = $node->safe_psql(
	'postgres', q{
	SELECT cluster_ic_mock_clear_all();
	SELECT cluster_ic_mock_inject(5, '\xDE'::bytea);
	SELECT sender, encode(payload, 'hex') FROM cluster_ic_mock_recv_test();
});
like($rt_out, qr/5\|de/i,
	'inject(sender=5, payload=\\xDE) -> recv_test returns (5, de)');


# ----------
# Test 4: FIFO ordering — three injects produce three recv_test rows
# in order.
# ----------
my $fifo_out = $node->safe_psql(
	'postgres', q{
	SELECT cluster_ic_mock_clear_all();
	SELECT cluster_ic_mock_inject(1, '\x41'::bytea);
	SELECT cluster_ic_mock_inject(2, '\x42'::bytea);
	SELECT cluster_ic_mock_inject(3, '\x43'::bytea);
	SELECT sender, encode(payload, 'hex') FROM cluster_ic_mock_recv_test();
	SELECT sender, encode(payload, 'hex') FROM cluster_ic_mock_recv_test();
	SELECT sender, encode(payload, 'hex') FROM cluster_ic_mock_recv_test();
});
my @fifo_rows = grep { /^\d+\|/ } split /\n/, $fifo_out;
is_deeply(
	\@fifo_rows,
	[ '1|41', '2|42', '3|43' ],
	'inject(A,B,C) -> recv_test x3 returns FIFO order (A,B,C)');


# ----------
# Test 5: recv_test on empty queue returns zero rows.
# ----------
my $empty_out = $node->safe_psql(
	'postgres', q{
	SELECT cluster_ic_mock_clear_all();
	SELECT count(*) FROM cluster_ic_mock_recv_test();
});
like($empty_out, qr/^0\b/m,
	'recv_test on empty inbound queue returns zero rows');


# ----------
# Test 6: drain_outbound on empty target returns zero rows.
# ----------
$node->safe_psql('postgres', 'SELECT cluster_ic_mock_clear_all()');
is( $node->safe_psql(
		'postgres',
		'SELECT count(*) FROM cluster_ic_mock_drain_outbound(7)'),
	'0',
	'drain_outbound on empty target returns zero rows');


# ----------
# Test 7: drain_outbound with target out of [0, CLUSTER_MAX_NODES) -> ERROR.
# ----------
my ($stdout7, $stderr7);
$node->psql(
	'postgres',
	q{SELECT * FROM cluster_ic_mock_drain_outbound(200)},
	stdout => \$stdout7,
	stderr => \$stderr7);
like($stderr7, qr/(out of range|invalid)/i,
	'drain_outbound(200) raises ERROR (out of range)');


# ----------
# Test 8: clear_all empties the inbound queue.
# ----------
my $clear_out = $node->safe_psql(
	'postgres', q{
	SELECT cluster_ic_mock_inject(1, '\x41'::bytea);
	SELECT cluster_ic_mock_inject(2, '\x42'::bytea);
	SELECT cluster_ic_mock_clear_all();
	SELECT count(*) FROM cluster_ic_mock_recv_test();
});
like($clear_out, qr/^0\b/m,
	'clear_all empties inbound queue (recv_test returns 0 rows after)');


# ----------
# Test 9: inject only requires tier=mock; rejected on tier!=mock.
#
# Switch to stub via a fresh node, confirm inject ERRORs.  We have to
# spin up a second node because cluster.interconnect_tier is
# PGC_POSTMASTER and we configured the primary on 'mock'.
# ----------
my $stub_node = PgracClusterNode->new('stub_check');
$stub_node->init;
$stub_node->start;

my ($stdout9, $stderr9);
$stub_node->psql(
	'postgres',
	q{SELECT cluster_ic_mock_inject(1, '\x41'::bytea)},
	stdout => \$stdout9,
	stderr => \$stderr9);
like($stderr9, qr/(mock|interconnect_tier)/i,
	'cluster_ic_mock_inject on tier!=mock raises ERROR');

$stub_node->stop;


# ----------
# Test 10: spec-0.16 / 0.17 / 0.19 baseline regression.
# ----------
is( $node->safe_psql(
		'postgres',
		'SELECT count(*) FROM pg_stat_cluster_wait_events'),
		'121',
		'pg_stat_cluster_wait_events returns 121 rows (spec-6.13 RDMA wait surface)');

$node->stop;

done_testing();
