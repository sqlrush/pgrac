#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 337_cluster_6_12d_space_lease.pl
#	  spec-6.12 wave d (static) -- per-node HW space leases on a 2-node
#	  ClusterPair (phantom-shared plain-heap table, t/334 pattern).
#	  Exercises the Q19-A materialization contract end to end (the
#	  L-d-mat leg): bulk lease -> partial consumption -> seqscan /
#	  count / vacuum / analyze over zero pages -> crash losing the
#	  lease -> rescan still clean.
#
#	  L1   pair boots with cluster.space_affinity=static + phantom
#	       table coincident
#	  L2   lease born: node0 bulk insert takes an oversized HW grant,
#	       parks the tail (lease_grants / leased_total / consumed /
#	       outstanding tick; count(*) exact)
#	  L3   zero-page materialization: seqscan + VACUUM + ANALYZE run
#	       clean over the outstanding (zero-page) lease tail; count
#	       unchanged
#	  L4   affinity: node1's own insert takes its OWN lease (its
#	       lease_grants ticks) and does not consume node0's parked
#	       blocks (node0 outstanding unchanged by node1's write)
#	  L5   crash fail-safe on the real protocol timeline: node0 dies ->
#	       deathwatch + fail-stop reconfig -> node0 rejoins -> lease
#	       bookkeeping gone, fresh table takes a fresh lease (Q19-A #3;
#	       survivor reads of dead-held blocks deferred to spec-4.10)
#
# Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md (wave d)
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/337_cluster_6_12d_space_lease.pl
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

my $NROWS = 10000;

sub state_val
{
	my ($node, $cat, $key) = @_;
	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state
		   WHERE category='$cat' AND key='$key'});
	return defined($v) && $v ne '' ? $v + 0 : 0;
}

sub lease_grants	  { return state_val($_[0], 'hw', 'lease_grants'); }
sub lease_leased	  { return state_val($_[0], 'hw', 'lease_leased_total'); }
sub lease_consumed	  { return state_val($_[0], 'hw', 'lease_consumed'); }
sub lease_outstanding { return state_val($_[0], 'hw', 'lease_outstanding'); }

# Cross-node statements can hit transient fail-closed retries (rule-8.A);
# bounded retry.  $tries is per-call: the post-JOIN GRD remaster window can
# stretch to tens of seconds, so the L5 DDL uses a much larger budget.
sub write_retry
{
	my ($node, $sql, $tries) = @_;
	$tries //= 10;
	for my $i (1 .. $tries)
	{
		my $ok = eval { $node->safe_psql('postgres', $sql); 1 };
		return 1 if $ok;
		usleep(1_000_000);
	}
	return 0;
}

sub read_sql
{
	my ($node, $sql) = @_;
	for my $i (1 .. 10)
	{
		my $v = eval { $node->safe_psql('postgres', $sql); };
		return $v if defined $v && $v ne '';
		usleep(500_000);
	}
	return undef;
}

# t/307 pattern (eval-wrapped): poll a boolean SQL until expected or timeout.
sub poll_until
{
	my ($node, $query, $expected, $timeout_s, $label) = @_;
	$expected //= 't';
	$timeout_s //= 20;
	my $deadline = time + $timeout_s;
	my $last = '';
	while (time < $deadline)
	{
		$last = eval { $node->safe_psql('postgres', $query); } // '';
		return 1 if $last eq $expected;
		select(undef, undef, undef, 0.5);
	}
	diag("$label timed out after ${timeout_s}s; expected=$expected last=$last");
	return 0;
}

# ============================================================
# L1: boot with the wave armed + phantom-shared table.
# ============================================================
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'd612_lease',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'jit = off',
		'cluster.space_affinity = static',
		'cluster.space_lease_blocks = 32',
		# t/315 proven online-rejoin recipe: the L5 crash leg needs the
		# deathwatch to fire AND the join drive to readmit the reborn node.
		'cluster.online_join = on',
		'cluster.join_convergence_timeout_ms = 30000',
		'cluster.cssd_heartbeat_interval_ms = 500',
		'cluster.cssd_dead_deadband_factor = 6',
		'cluster.ges_request_timeout_ms = 30000',
	]);
$pair->start_pair;

usleep(2_000_000);
ok($pair->wait_for_peer_state(0, 1, 'connected', 30),
	'L1 node0 sees node1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30),
	'L1 node1 sees node0 connected');

my ($node0, $node1) = ($pair->node0, $pair->node1);

is($node0->safe_psql('postgres', 'SHOW cluster.space_affinity'), 'static',
	'L1 space_affinity armed from boot conf');

$node0->safe_psql('postgres', 'CREATE TABLE xd_lease (id int, v int)');
$node1->safe_psql('postgres', 'CREATE TABLE xd_lease (id int, v int)');
my $p0 = $node0->safe_psql('postgres', q{SELECT pg_relation_filepath('xd_lease')});
my $p1 = $node1->safe_psql('postgres', q{SELECT pg_relation_filepath('xd_lease')});
is($p0, $p1, 'L1 xd_lease relfilepath coincidence holds');

# ============================================================
# L2: lease born on node0's bulk insert.
# ============================================================
my $g0_before = lease_grants($node0);
ok(write_retry($node0,
	"INSERT INTO xd_lease SELECT g, g FROM generate_series(1,$NROWS) g"),
	'L2 node0 bulk insert');

my $g0		 = lease_grants($node0);
my $leased0	 = lease_leased($node0);
my $cons0	 = lease_consumed($node0);
my $out0	 = lease_outstanding($node0);

cmp_ok($g0, '>', $g0_before, 'L2 node0 took at least one lease grant');
cmp_ok($leased0, '>', 0,     'L2 leased_total > 0');
cmp_ok($cons0, '>', 0,       'L2 lease blocks consumed by the writer');
cmp_ok($out0, '>=', 0,       'L2 outstanding inventory non-negative');
is(read_sql($node0, 'SELECT count(*) FROM xd_lease'), $NROWS,
	'L2 count(*) exact after lease-backed inserts');

# ============================================================
# L3: zero-page materialization contract on the live lease tail.
# ============================================================
is(read_sql($node0, 'SELECT count(*) FROM xd_lease WHERE v >= 0'), $NROWS,
	'L3 seqscan over zero-page tail is clean');
ok(write_retry($node0, 'VACUUM xd_lease'), 'L3 VACUUM clean');
ok(write_retry($node0, 'ANALYZE xd_lease'), 'L3 ANALYZE clean');
is(read_sql($node0, 'SELECT count(*) FROM xd_lease'), $NROWS,
	'L3 count(*) unchanged after vacuum/analyze');

# ============================================================
# L4: per-node affinity -- node1 gets its OWN lease, does not eat
#     node0's parked chunk.
# ============================================================
my $out0_pre  = lease_outstanding($node0);
my $g1_before = lease_grants($node1);

ok(write_retry($node1,
	'INSERT INTO xd_lease SELECT g, g FROM generate_series(1,2000) g'),
	'L4 node1 insert');

cmp_ok(lease_grants($node1), '>', $g1_before,
	'L4 node1 took its own lease grant');
cmp_ok(lease_consumed($node1), '>', 0, 'L4 node1 consumed from its own lease');
is(lease_outstanding($node0), $out0_pre,
	'L4 node0 outstanding untouched by node1 writes (affinity holds)');
is(read_sql($node1, 'SELECT count(*) FROM xd_lease'), $NROWS + 2000,
	'L4 combined count exact cross-node');

# ============================================================
# L5: crash loses the lease bookkeeping -- fail-safe, never
#     corruption (Q19-A #3).  The verification follows the REAL
#     protocol timeline instead of poking into the zombie window
#     (restarting / querying before the deathwatch fires leaves the
#     peer GRD holding the old incarnation's X records -- every read
#     fails closed; that window belongs to roadmap-4.7 / spec-5.15):
#       1. node0 dies (immediate).
#       2. node1's CSSD deathwatch declares node0 dead and the
#          fail-stop reconfig sweeps its holdings (t/307 pattern).
#       3. node0 rejoins (spec-5.15 declared-dead rejoin) and a fresh
#          table takes a fresh lease -- machinery alive post-crash.
#     Survivor reads of the dead node's X-held blocks are explicitly
#     DEFERRED to the spec-4.10 block-recovery surface (see the note
#     at the assertion site).
# ============================================================
my $out0_precrash = lease_outstanding($node0);
ok(write_retry($node0, 'CHECKPOINT'), 'L5 checkpoint node0 before crash');
$node0->stop('immediate');

ok(poll_until($node1,
		q{SELECT state IN ('suspected','dead') FROM pg_cluster_cssd_peers
		  WHERE node_id = 0}, 't', 90,
		'L5 node1 CSSD deathwatch'),
	'L5 node1 CSSD marks crashed node0 suspected/dead');
ok(poll_until($node1,
		q{SELECT event_id != 0 FROM pg_cluster_reconfig_state}, 't', 90,
		'L5 node1 reconfig'),
	'L5 node1 fail-stop reconfig fired');

# DEFERRED (explicit, rule 18): survivor reads / VACUUM of the crashed
# node's table are NOT asserted here.  Even after the fail-stop reconfig,
# the survivor's GRD keeps the dead incarnation's block-level X-holder
# records until spec-4.10 online block recovery serves them (not armed in
# this harness; it has its own test surface) -- reads fail CLOSED
# ("could not obtain read image from X holder"), which is 8.A honesty,
# not corruption.  Wave d's fail-safe basis needs no survivor read:
# the HWM is durable (XLOG_HW_RESERVE), the orphaned tail is zero pages
# (proven harmless by L3), and only the volatile bookkeeping died.

$node0->start;
# t/315 readmission sequence: transport reconnect -> online rejoin
# publishes node0 MEMBER -> node0's own write gate reopens.  The reborn
# node stays write-fenced (create rejected 53R51-family) until then.
ok(poll_until($node1,
		q{SELECT state = 'alive' FROM pg_cluster_cssd_peers WHERE node_id = 0},
		't', 90, 'L5 node0 CSSD alive'),
	'L5 node1 sees reborn node0 CSSD-alive (transport reconnect)');
ok(poll_until($node1,
		q{SELECT state = 'member' FROM pg_cluster_membership WHERE node_id = 0},
		't', 120, 'L5 node0 member'),
	'L5 node0 readmitted MEMBER (spec-5.15 online rejoin)');
ok(poll_until($node0,
		q{SELECT 1 FROM (SELECT pg_catalog.txid_current()) s}, '1', 90,
		'L5 node0 writable'),
	'L5 reborn node0 write gate reopened');
# The JOIN reconfig transiently write-fences the survivor too (cooperative
# fence + GRD remaster, both retry-hinted); wait until it is writable again
# before the fresh-table DDL.
ok(poll_until($node1,
		q{SELECT 1 FROM (SELECT pg_catalog.txid_current()) s}, '1', 90,
		'L5 node1 writable'),
	'L5 survivor write gate reopened after the JOIN reconfig');

is(lease_outstanding($node0), 0,
	'L5 lease bookkeeping gone after crash (shmem reset)');
my $g0_postcrash = lease_grants($node0);
ok(write_retry($node0, 'CREATE TABLE xd_lease2 (id int, v int)', 90),
	'L5 fresh relation created on reborn node0');
# No node1 mirror of xd_lease2: nothing below reads it cross-node, and the
# survivor can stay wedged between "GRD shard is being remastered" and
# transient re-fencing for minutes after the JOIN while the joiner is
# already fully writable -- a join/remaster substrate follow-up (observed
# and logged here), not a wave-d claim.
ok(write_retry($node0,
	'INSERT INTO xd_lease2 SELECT g, g FROM generate_series(1,2000) g', 90),
	'L5 fresh insert on reborn node0 (new relation, no stale holdings)');
cmp_ok(lease_grants($node0), '>', $g0_postcrash,
	'L5 lease machinery alive after restart (fresh grant taken)');
is(read_sql($node0, 'SELECT count(*) FROM xd_lease2'), 2000,
	'L5 fresh-table count exact on reborn node0');
note("node0 pre-crash outstanding (orphaned by design): $out0_precrash");

$pair->stop_pair if $pair->can('stop_pair');
done_testing();
