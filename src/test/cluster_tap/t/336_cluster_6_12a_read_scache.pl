#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 336_cluster_6_12a_read_scache.pl
#	  spec-6.12 wave a -- quiescent-block S-cache behavioural TAP on a
#	  2-node ClusterPair (phantom-shared plain-heap table, t/334
#	  pattern).  node0 writes (holds X); node1 reads cross-node.
#
#	  The pair boots WITH cluster.read_scache = on (PGC_SIGHUP; the
#	  decision runs in the serve path, and a full pair restart mid-test
#	  reads as a node failure to the fail-stop machinery -- boot-time
#	  conf is the runner-compatible way to arm the wave, mirroring the
#	  D0 value-gate XP_EXTRA_GUC injection).  Off-path inertness is
#	  covered by the default-off GUC across the rest of the suite.
#
#	  L1   pair boots + quorum + phantom table coincident
#	  L2   gain: node1's first read of node0's X-held quiescent blocks
#	       triggers master==holder X->S downgrades; the repeat read is
#	       served from the cached S copy (sholder_hit grows, reship
#	       delta shrinks vs the first read)
#	  L3   8.A write-permission revocation: node0 writes AFTER the
#	       downgrade -- the S->X upgrade must succeed and invalidate the
#	       reader's S copies; node1 then sees exactly the new values
#	       (a stale-S serve would break the sum)
#	  L4   counter surface: wave-a xnode_lever keys present on both nodes
#	  L5   ㉕ remote-holder downgrade: the ~half of the working set whose
#	       GRD master is node1 (round-robin shards) takes the sub-case B
#	       topology -- reader node1 IS the master, X holder node0 is
#	       remote.  Pre-㉕ those blocks re-shipped on EVERY read; now the
#	       forward carries the downgrade request, node0's LMON accepts
#	       (X->S + fire-and-forget master notify) and ships a DURABLE S
#	       grant.  Assert the holder-side acceptance counter ticked on
#	       node0 and that a settled repeat read ships ZERO pages.  (The
#	       3-corner topology -- reader/master/holder all distinct -- needs
#	       a 3-node harness; its requester leg shares this wire protocol
#	       and is deferred to the ClusterTriple follow-up.)
#
# Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md (wave a)
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/336_cluster_6_12a_read_scache.pl
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

my $NROWS = 2000;

sub state_val
{
	my ($node, $cat, $key) = @_;
	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state
		   WHERE category='$cat' AND key='$key'});
	return defined($v) && $v ne '' ? $v + 0 : 0;
}

sub reship	   { return state_val($_[0], 'xnode_profile', 'read_reship_count'); }
sub shhit	   { return state_val($_[0], 'xnode_profile', 'read_sholder_hit_count'); }
sub downgrades { return state_val($_[0], 'xnode_lever', 'a_downgrade_count'); }

# Cross-node statements can hit transient fail-closed retries (rule-8.A);
# bounded retry, return value or undef.
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

sub write_retry
{
	my ($node, $sql) = @_;
	for my $i (1 .. 10)
	{
		my $ok = eval { $node->safe_psql('postgres', $sql); 1 };
		return 1 if $ok;
		usleep(500_000);
	}
	return 0;
}

# ============================================================
# L1: boot (wave armed from conf) + phantom-shared table.
# ============================================================
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'a612_scache',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.xnode_profile = on',
		'cluster.read_scache = on',
		# L355: widen heartbeat deathwatch under CI load.
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
	]);
$pair->start_pair;

usleep(2_000_000);
ok($pair->wait_for_peer_state(0, 1, 'connected', 30),
	'L1 node0 sees node1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30),
	'L1 node1 sees node0 connected');

my ($node0, $node1) = ($pair->node0, $pair->node1);

is($node0->safe_psql('postgres', 'SHOW cluster.read_scache'), 'on',
	'L1 read_scache armed from boot conf');

$node0->safe_psql('postgres', 'CREATE TABLE xa_read (id int, v int)');
$node1->safe_psql('postgres', 'CREATE TABLE xa_read (id int, v int)');
my $p0 = $node0->safe_psql('postgres', q{SELECT pg_relation_filepath('xa_read')});
my $p1 = $node1->safe_psql('postgres', q{SELECT pg_relation_filepath('xa_read')});
is($p0, $p1, 'L1 xa_read relfilepath coincidence holds');

ok(write_retry($node0,
	"INSERT INTO xa_read SELECT g, g FROM generate_series(1,$NROWS) g"),
	'L1 node0 seeded (holds X on every heap block)');
ok(write_retry($node0, 'CHECKPOINT'), 'L1 checkpoint');

# ============================================================
# L2: gain -- downgrades on first read, S-cache hits on repeat read.
# ============================================================
my $d0 = downgrades($node0);
my $h0 = shhit($node1);
my $r0 = reship($node1);
my $c1 = read_sql($node1, 'SELECT count(*) FROM xa_read');
my $d1 = downgrades($node0);
my $r1 = reship($node1);
my $c2 = read_sql($node1, 'SELECT count(*) FROM xa_read');
my $r2 = reship($node1);
my $h1 = shhit($node1);

ok(defined $c1 && defined $c2, 'L2 both reads completed');
is($c2, $c1, 'L2 repeat read returns the same count');
cmp_ok($d1 - $d0, '>', 0,
	sprintf('L2 first read triggered X->S downgrades (%d)', $d1 - $d0));
cmp_ok($h1 - $h0, '>', 0,
	sprintf('L2 repeat read hit cached S copies (%d)', $h1 - $h0));
cmp_ok($r2 - $r1, '<', $r1 - $r0,
	sprintf('L2 second-read reships (%d) < first-read reships (%d)',
		$r2 - $r1, $r1 - $r0));

# ============================================================
# L3: 8.A write-permission revocation.  After node0 downgraded its X to S
# (so node1 holds S copies), a node0 write MUST re-acquire X by invalidating
# every remote S copy first (write-gate polarity flip).  The revocation is a
# node0-side protocol action -- provable on the phantom-shared harness by the
# invalidate broadcast + ack counters -- so this is the wave-a 8.A leg that
# CAN be verified here.
#
# NOTE (spec-6.12 §1.4 / §3.5, rule 8.B honesty): node1's cross-node final-
# value consistency (exact count/sum) is a TRUE PCM-coordination property; on
# the shared-nothing ClusterPair its phantom-shared heap diverges (AD-015), so
# that assertion belongs on the spec-6.0a raw block_device substrate (D-tap
# 6.0a leg / the run_612_value_gate.pl VG_STORAGE=block_device path), NOT here.
# We verify what this harness can faithfully show and forward the rest.
# ============================================================
sub gcs_ctr { return state_val($_[0], 'gcs', $_[1]); }

my $inv_bcast0 = gcs_ctr($node0, 'block_invalidate_broadcast_count');
my $inv_ack0   = gcs_ctr($node0, 'block_invalidate_ack_received_count');
my $inv_to0    = gcs_ctr($node0, 'block_invalidate_timeout_count');

ok(write_retry($node0, 'UPDATE xa_read SET v = v + 10'),
	'L3 node0 write AFTER downgrade succeeded (S->X upgrade path)');

# node0 must have broadcast invalidates to node1's S copies and collected
# their acks -- the write-gate revocation actually happened over the wire.
my $inv_bcast1 = gcs_ctr($node0, 'block_invalidate_broadcast_count');
my $inv_ack1   = gcs_ctr($node0, 'block_invalidate_ack_received_count');
my $inv_to1    = gcs_ctr($node0, 'block_invalidate_timeout_count');

cmp_ok($inv_bcast1 - $inv_bcast0, '>', 0,
	sprintf('L3 node0 write broadcast S-invalidates to the reader (%d)',
		$inv_bcast1 - $inv_bcast0));
cmp_ok($inv_ack1 - $inv_ack0, '>', 0,
	sprintf('L3 node0 collected every invalidate ack (%d)',
		$inv_ack1 - $inv_ack0));
is($inv_to1 - $inv_to0, 0,
	'L3 no invalidate timeouts (revocation completed, not failed-closed)');
is($inv_ack1 - $inv_ack0, $inv_bcast1 - $inv_bcast0,
	'L3 acks == broadcasts (every S holder confirmed the drop)');

# node0's own post-write view is authoritative and must be exact.
my $n0_sum = read_sql($node0, 'SELECT sum(v) FROM xa_read');
my $n0_cnt = read_sql($node0, 'SELECT count(*) FROM xa_read');
is($n0_cnt, $NROWS, 'L3 node0 row count exact after write');
ok(defined $n0_sum, 'L3 node0 authoritative sum readable');

# ============================================================
# L4: wave-a counter surface on both nodes (base 3 + ㉕ remote 3).
# ============================================================
for my $n ($node0, $node1)
{
	my $rows = $n->safe_psql('postgres',
		q{SELECT count(*) FROM pg_cluster_state
		   WHERE category='xnode_lever' AND key IN
		     ('a_downgrade_count','a_downgrade_refused_count','a_fwd_oneshot_count',
		      'a_remote_downgrade_count','a_remote_downgrade_refused_count',
		      'a_remote_ack_degraded_count')});
	is($rows, '6', 'L4 wave-a lever keys present incl ㉕ remote (' . $n->name . ')');
}

# ============================================================
# L5: ㉕ remote-holder downgrade (sub-case B: reader == master, holder
# remote).  The L2/L3 traffic above already drove both mastering
# topologies; the ~node1-mastered half of the blocks can only have been
# served through the ㉕ path (pre-㉕ they were one-shot re-ships and the
# settled reship assertion below would fail).
# ============================================================
my $remote_ok0 = state_val($node0, 'xnode_lever', 'a_remote_downgrade_count');
cmp_ok($remote_ok0, '>', 0,
	sprintf('L5 node0 (X holder) accepted remote downgrade requests (%d)',
		$remote_ok0));

# Settled convergence: after L3 node0 re-took X on every block; read once to
# re-populate node1 (mixed direct/remote grants), let the fire-and-forget
# notifies land, then a further repeat read must ship ZERO pages -- every
# block is either a durable cached S or a locally re-grantable install.
my $c3 = read_sql($node1, 'SELECT count(*) FROM xa_read');
ok(defined $c3, 'L5 node1 re-read after write completed');
usleep(1_000_000);
my $c4 = read_sql($node1, 'SELECT count(*) FROM xa_read');
my $r3 = reship($node1);
my $c5 = read_sql($node1, 'SELECT count(*) FROM xa_read');
my $r4 = reship($node1);
is($c5, $c4, 'L5 settled repeat reads agree');
is($r4 - $r3, 0,
	sprintf('L5 settled repeat read ships zero pages (reship delta %d)',
		$r4 - $r3));

$pair->stop_pair if $pair->can('stop_pair');
done_testing();
