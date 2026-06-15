#-------------------------------------------------------------------------
#
# 266_thread_launch.pl
#    spec-4.11 D1 increment 3b-4b Part 3 — the lmon launch wiring.
#
#    The reconfig FSM (cluster_grd_recovery_lmon_tick) calls
#    cluster_thread_recovery_launch_workers each WAIT_CLUSTER tick: for every
#    in-scope dead origin not already handled this episode it stamps the replay
#    slot REPLAYING and registers one per-episode executor worker
#    (cluster_thread_recovery_worker_main).  It uses the SAME decide_scope as
#    replay_one, so OUT OF SCOPE (online_thread_recovery off by default / no
#    shared backend / single node / >2-node) it is a NO-OP -- the spec-4.6/4.7
#    reconfig FSM is unchanged (no regression).
#
#    On a single machine there are no peers, so the launch is out of scope.  This
#    test pins that NO-OP property: cluster_thread_recovery_launch_test(dead_node)
#    drives the launch with a synthetic single-dead-node bitmap and returns the
#    dead thread's replay-slot state -- which must stay IDLE (0), proving the FSM
#    wiring never launches / never stamps a slot out of scope.  The in-scope
#    firing (register a worker, REPLAYING -> materialized -> unfreeze) is the
#    2-node e2e (Part 4); the no-regression of the reconfig FSM itself is covered
#    by t/249-252.
#
#      L1-L3 no-op out of scope: a synthetic dead node leaves its slot IDLE (0).
#
# IDENTIFICATION
#    src/test/cluster_tap/t/266_thread_launch.pl
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

# online_thread_recovery is left at its default (off); even on, a single node has
# no peers, so the launch is out of scope either way.
my $node = PgracClusterNode->new('threadlaunch');
$node->init;
$node->append_conf('postgresql.conf',
		"cluster.enabled = on\n"
	  . "cluster.node_id = 0\n"
	  . "cluster.allow_single_node = on\n"
	  . "cluster.online_thread_recovery = on\n"
	  . "autovacuum = off\n");
$node->start;

sub launch
{
	my ($dead_node) = @_;
	return $node->safe_psql('postgres',
		"SELECT cluster_thread_recovery_launch_test($dead_node)");
}

# ============================================================
# L1-L3 no-op out of scope: the launch never stamps a slot REPLAYING on a single
# node, so the dead thread's slot stays IDLE (0).
# ============================================================
is(launch(0), '0', 'L1 no-op out of scope: dead node 0 (thread 1) slot stays IDLE');
is(launch(1), '0', 'L2 no-op out of scope: dead node 1 (thread 2) slot stays IDLE');
is(launch(15), '0', 'L3 no-op out of scope: dead node 15 (thread 16) slot stays IDLE');

$node->stop;
done_testing();
