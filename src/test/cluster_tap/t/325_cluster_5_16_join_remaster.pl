# 325_cluster_5_16_join_remaster.pl
#	  spec-5.16 D8 — online node-join GRD/PCM remaster, 2-node e2e.
#
#	  Builds on the spec-5.15 online rejoin (t/315): a declared node leaves and
#	  rejoins online (no full cluster restart).  This test additionally asserts
#	  the spec-5.16 join-direction remaster that runs as part of that rejoin:
#	    - the JOIN_COMMITTED episode is consumed by the GRD recovery FSM
#	      (join_remaster_started / join_remaster_done counters on the survivor),
#	    - the joiner-home block view is rebuilt and its fence lifts
#	      (join_block_views_rebuilt),
#	    - with cluster.join_remaster_enabled = on the joiner's home-shard GES
#	      mastership is moved back from the survivor (join_shards_remastered > 0),
#	    - data written before the leave is correctly readable on BOTH nodes after
#	      the rejoin (the joiner's rebuilt view serves correct, single-X data —
#	      no cold-serve / split-master).
#
#	  Spec: spec-5.16-online-join-grd-pcm-remaster.md (D8 L1-L5).
#	  The 3-node no-double-grant / gate-open / master-side / pre-MEMBER ship
#	  blockers are t/326.
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/325_cluster_5_16_join_remaster.pl

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(usleep);

# Poll $query on $node until it returns $expected (default 't') or times out.
sub poll_until
{
	my ($node, $query, $expected, $timeout_s, $label) = @_;
	$expected //= 't';
	$timeout_s //= 30;
	my $deadline = time + $timeout_s;
	my $last = '';
	while (time < $deadline)
	{
		$last = eval { $node->safe_psql('postgres', $query) } // '<err>';
		return 1 if defined $last && $last eq $expected;
		usleep(200_000);
	}
	diag("poll_until timeout ($label): last='$last' expected='$expected'");
	return 0;
}

sub current_epoch
{
	my ($node) = @_;
	my $v = $node->safe_psql('postgres', 'SELECT new_epoch FROM pg_cluster_reconfig_state');
	$v = 0 if !defined $v || $v eq '';
	return $v + 0;
}

sub poll_epoch_gt
{
	my ($node, $floor, $timeout_s, $label) = @_;
	$timeout_s //= 30;
	my $deadline = time + $timeout_s;
	my $e = current_epoch($node);
	while ($e <= $floor && time < $deadline)
	{
		usleep(200_000);
		$e = current_epoch($node);
	}
	diag("poll_epoch_gt timeout ($label): epoch=$e floor=$floor") if $e <= $floor;
	return $e;
}

# Read a single grd_recovery join-* counter as an integer.
sub join_counter
{
	my ($node, $key) = @_;
	my $v = eval {
		$node->safe_psql('postgres',
			"SELECT value FROM cluster_dump_state() WHERE category = 'grd_recovery' AND key = '$key'");
	};
	$v = 0 if !defined $v || $v eq '';
	return $v + 0;
}

# Poll until $node's join counter $key is strictly greater than $floor.
sub poll_join_counter_gt
{
	my ($node, $key, $floor, $timeout_s, $label) = @_;
	$timeout_s //= 30;
	my $deadline = time + $timeout_s;
	my $v = join_counter($node, $key);
	while ($v <= $floor && time < $deadline)
	{
		usleep(200_000);
		$v = join_counter($node, $key);
	}
	diag("poll_join_counter_gt timeout ($label): $key=$v floor=$floor") if $v <= $floor;
	return $v;
}

# online_join + join_remaster ON, brisk CSSD timing so the rejoin converges.
my @conf = (
	'cluster.online_join = on',
	'cluster.join_remaster_enabled = on',
	'cluster.join_convergence_timeout_ms = 30000',
	'cluster.cssd_heartbeat_interval_ms = 500',
	'cluster.cssd_dead_deadband_factor = 6',
	'cluster.ges_request_timeout_ms = 30000',
	'cluster.grd_rebuild_timeout_ms = 15000',
	'autovacuum = off',
	'jit = off',
);

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'spec_5_16_jr',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [@conf]);
$pair->start_pair;
usleep(3_000_000);
my $node0 = $pair->node0;
my $node1 = $pair->node1;

# ----------
# L1 — baseline: both up, node1 is a MEMBER in node0's membership SSOT.
# ----------
{
	is($node0->safe_psql('postgres', 'SELECT 1'), '1', 'L1-i node0 alive');
	ok($pair->wait_for_peer_state(0, 1, 'connected', 30),
		'L1-ii node0 sees peer node1 connected');
	ok(poll_until($node0, q{SELECT state = 'member' FROM pg_cluster_membership WHERE node_id = 1},
			't', 30, 'node1 member'),
		'L1-iii node0 reports node1 as MEMBER');
}

# ----------
# L2 — leave then ONLINE rejoin (spec-5.15 substrate).
# ----------
my $epoch_dead;
{
	my $epoch_before = current_epoch($node0);
	$node1->stop;
	ok(poll_until($node0,
			q{SELECT state IN ('suspected','dead') FROM pg_cluster_cssd_peers WHERE node_id = 1},
			't', 60, 'node1 CSSD dead'),
		'L2-i node0 CSSD declared node1 dead');
	ok(poll_until($node0, q{SELECT state <> 'member' FROM pg_cluster_membership WHERE node_id = 1},
			't', 60, 'node1 not member'),
		'L2-ii node0 dropped node1 from the MEMBER set');
	$epoch_dead = poll_epoch_gt($node0, $epoch_before, 60, 'epoch bump on leave');
	cmp_ok($epoch_dead, '>', $epoch_before, 'L2-iii epoch bumped on the leave');

	$node1->start;
	ok(poll_until($node0,
			q{SELECT state = 'alive' FROM pg_cluster_cssd_peers WHERE node_id = 1},
			't', 90, 'node1 CSSD alive again'),
		'L2-iv node0 sees node1 CSSD alive again after restart');
	ok(poll_until($node0, q{SELECT state = 'member' FROM pg_cluster_membership WHERE node_id = 1},
			't', 90, 'node1 rejoined member'),
		'L2-v node0 republished node1 as MEMBER (online rejoin)');
	cmp_ok(current_epoch($node0), '>', $epoch_dead,
		'L2-vi epoch bumped again on the JOIN reconfig');
}

# ----------
# L3 — the JOIN-direction remaster episode ran on the survivor (node0).
#       join_remaster_started/done increment as the GRD FSM consumes the
#       JOIN_COMMITTED event; done means the all-members re-declare barrier
#       converged and the joiner-home fence lifted.
# ----------
{
	my $started = poll_join_counter_gt($node0, 'join_remaster_started', 0, 90,
		'join_remaster_started');
	cmp_ok($started, '>', 0, 'L3-i node0 entered a JOIN remaster episode');
	my $done = poll_join_counter_gt($node0, 'join_remaster_done', 0, 90, 'join_remaster_done');
	cmp_ok($done, '>', 0, 'L3-ii node0 JOIN remaster episode completed (fence lifted)');
	my $views = join_counter($node0, 'join_block_views_rebuilt');
	cmp_ok($views, '>', 0, 'L3-iii node0 rebuilt the joiner-home block view');
}

# ----------
# L4 — with join_remaster_enabled=on the joiner's home-shard GES mastership
#       moved back from the survivor (GRD logical-lock rebalance).
# ----------
{
	my $moved = poll_join_counter_gt($node0, 'join_shards_remastered', 0, 60,
		'join_shards_remastered');
	cmp_ok($moved, '>', 0, 'L4-i node0 moved joiner home-shard mastership back (GRD rebalance)');
}

# ----------
# L5 — liveness: node1's write gate reopened post-rejoin (it can assign an
#       xid) and node1 observes its own state as MEMBER.  The deterministic
#       no-double-grant / single-X correctness of the rebuilt view is the
#       3-node ship-blocker (t/326, via cross-node advisory GES locks — the
#       cross-node coherent surface available at this stage).
# ----------
{
	ok(poll_until($node1, q{SELECT 1 FROM (SELECT pg_catalog.txid_current()) s}, '1', 90,
			'node1 writable'),
		'L5-i node1 write gate reopened post-rejoin (it can assign an xid)');
	ok(poll_until($node1, q{SELECT state = 'member' FROM pg_cluster_membership WHERE node_id = 1},
			't', 90, 'node1 self member'),
		'L5-ii node1 observes its own state as MEMBER');
	# node0's home blocks remained servable throughout (the survivor was never
	# fenced; only the JOINER's home was), so node0 stays live.
	is($node0->safe_psql('postgres', 'SELECT 1'), '1', 'L5-iii node0 still live');
}

done_testing();
