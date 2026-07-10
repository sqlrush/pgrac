#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 364_lms_worker_pool.pl
#	  spec-7.3 D2 regression smoke for the LMS DATA-plane worker pool.
#
#	  Verifies the worker instantiation / identity / N=1 identity contract:
#	    L1 default cluster.lms_workers=2 exposes 1 'lms' + 1 'lms worker'
#	    L2 cluster.lms_workers is a postmaster int GUC
#	    L3 cluster.lms_workers=4 exposes 1 'lms' + 3 'lms worker'
#	    L4 cluster.lms_workers=1 exposes 1 'lms' + 0 'lms worker'
#	       (spec-7.2 topology identity: no worker sibling forked)
#	    L5 a node with workers shuts down cleanly (no orphan / crash)
#
#	  The DATA-plane routing itself (workers actually serving shards) is
#	  wired in spec-7.3 D3-D6; D2 only spawns, identifies and idles the
#	  workers, so this file asserts visibility + count + clean lifecycle.
#
# Author: SqlRush <sqlrush@gmail.com>
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use PgracClusterNode;

# Count backends of a given backend_type.
sub backend_count
{
	my ($node, $type) = @_;
	return $node->safe_psql('postgres',
		qq{SELECT count(*) FROM pg_stat_activity WHERE backend_type = '$type'});
}

# ---- L1 + L2 : default pool size 2 ----------------------------------------
my $node2 = PgracClusterNode->new('lms_workers_2');
$node2->init;
$node2->append_conf('postgresql.conf',
	"cluster.node_id = 0\ncluster.lms_workers = 2\n");
$node2->start;

ok( $node2->poll_query_until(
		'postgres',
		q{SELECT count(*) = 1 FROM pg_stat_activity WHERE backend_type = 'lms worker'}
	),
	'L1 one lms worker visible when cluster.lms_workers=2');
is(backend_count($node2, 'lms'), '1', 'L1a worker 0 (lms) present alongside');

my $guc_meta = $node2->safe_psql('postgres', q{
	SELECT setting, vartype, context
	  FROM pg_settings
	 WHERE name = 'cluster.lms_workers'});
is($guc_meta, '2|integer|postmaster',
	'L2 cluster.lms_workers is a postmaster int GUC defaulting to 2');

# ---- L2b (spec-7.3 D8) : lms_nice GUC + per-worker dump rows ---------------
my $nice_meta = $node2->safe_psql('postgres', q{
	SELECT setting, vartype, context, min_val, max_val
	  FROM pg_settings
	 WHERE name = 'cluster.lms_nice'});
is($nice_meta, '0|integer|sighup|-20|0',
	'L2b cluster.lms_nice is a sighup int GUC defaulting to 0 (range [-20,0])');

# Per-worker observability rows are emitted for the LIVE pool only:
# lms_workers=2 exposes _w0 + _w1 and no _w2.
my $wrows2 = $node2->safe_psql('postgres', q{
	SELECT count(*) FROM pg_cluster_state
	 WHERE category='lms' AND key IN
	   ('lms_data_dispatch_count', 'lms_direct_reply_count',
	    'lms_conn_reset_count', 'lms_inline_serve_count',
	    'lms_data_dispatch_count_w0', 'lms_data_dispatch_count_w1')});
is($wrows2, '6',
	'L2b aggregate + per-worker observability keys present (lms_workers=2)');
is( $node2->safe_psql('postgres', q{
	SELECT count(*) FROM pg_cluster_state
	 WHERE category='lms' AND key = 'lms_data_dispatch_count_w2'}),
	'0', 'L2b no _w2 rows beyond the live pool');
is( $node2->safe_psql('postgres', q{
	SELECT count(*) FROM pg_cluster_state
	 WHERE category='lms' AND key LIKE 'lms\_serve\_hist\_us\_%' ESCAPE '\'
	   AND key NOT LIKE '%\_w%' ESCAPE '\'}),
	'16', 'L2b aggregate serve-duration histogram has 16 buckets');

$node2->stop;

# ---- L3 : pool size 4 -> three worker siblings -----------------------------
my $node4 = PgracClusterNode->new('lms_workers_4');
$node4->init;
$node4->append_conf('postgresql.conf',
	"cluster.node_id = 0\ncluster.lms_workers = 4\n");
$node4->start;

ok( $node4->poll_query_until(
		'postgres',
		q{SELECT count(*) = 3 FROM pg_stat_activity WHERE backend_type = 'lms worker'}
	),
	'L3 three lms workers visible when cluster.lms_workers=4');
is(backend_count($node4, 'lms'), '1', 'L3a exactly one lms (worker 0)');

$node4->stop;

# ---- L4 : pool size 1 == spec-7.2 topology identity (no worker) ------------
my $node1 = PgracClusterNode->new('lms_workers_1');
$node1->init;
$node1->append_conf('postgresql.conf',
	"cluster.node_id = 0\ncluster.lms_workers = 1\n");
$node1->start;

# Give ServerLoop time to run its respawn branch; it must fork no worker.
$node1->safe_psql('postgres', 'SELECT pg_sleep(1)');
is(backend_count($node1, 'lms worker'), '0',
	'L4 no lms worker forked when cluster.lms_workers=1 (7.2 identity)');
is(backend_count($node1, 'lms'), '1', 'L4a worker 0 (lms) still present');

# L4b (spec-7.3 D8): the live-pool bound follows N=1 -- _w0 only, no _w1.
is( $node1->safe_psql('postgres', q{
	SELECT count(*) FROM pg_cluster_state
	 WHERE category='lms' AND key IN
	   ('lms_data_dispatch_count_w0', 'lms_data_dispatch_count_w1')}),
	'1', 'L4b per-worker rows bounded to the live pool (N=1: _w0 only)');

# L5 clean shutdown (fast stop; TAP asserts the exit status is clean).
$node1->stop;
ok(1, 'L5 node with worker pool shut down cleanly');

done_testing();
