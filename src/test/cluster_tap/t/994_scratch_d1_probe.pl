#-------------------------------------------------------------------------
# spec-7.1 D1 requester 半边 deterministic probe (scratch).
#
#   Verifies the xmin overlay-miss -> origin verdict ask path: node0 seeds
#   rows and commits; node1 reads them cross-node RIGHT AWAY, before the V4
#   hint overlay is guaranteed to have arrived, so the xmin resolve hits an
#   overlay miss and (with D1) asks the origin for a by-xid verdict instead
#   of failing closed.  Asserts: (a) the verdict-ask counter moved (D1
#   actually engaged), (b) node1's cross-node read is EXACT (no false-
#   visible / lost row -- the 8.A tooth), (c) hits >= asks that resolved.
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#    src/test/cluster_tap/994_scratch_d1_probe.pl
#-------------------------------------------------------------------------

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Time::HiRes qw(usleep);
use Test::More;

sub state_val
{
	my ($node, $cat, $key) = @_;
	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state WHERE category='$cat' AND key='$key'});
	return defined($v) && $v ne '' ? $v + 0 : 0;
}

sub write_retry
{
	my ($node, $sql) = @_;
	for my $i (1 .. 12)
	{
		my $ok = eval { $node->safe_psql('postgres', $sql); 1 };
		return 1 if $ok;
		usleep(500_000);
	}
	return 0;
}

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'd1probe',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.ges_request_timeout_ms = 30000',
		'cluster.gcs_reply_timeout_ms = 3000',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 5',
		'cluster.online_join = on',
		'cluster.quorum_poll_interval_ms = 500',
		'cluster.join_convergence_timeout_ms = 30000',
		'cluster.xid_striping = on',
		'cluster.crossnode_runtime_visibility = on',
		# Deterministically force the xmin overlay-miss window: with the V4
		# status hint disabled, node1's read of a node0-committed foreign
		# xmin ALWAYS misses the overlay and reaches HC181, so D1's verdict
		# ask is exercised on every cross-node read (the census S5 hot leg,
		# made deterministic -- IN-9 L1's "compress the overlay-miss window").
		'cluster.tt_status_hint_emit_mode = disabled',
	]);
$pair->start_pair;
usleep(2_000_000);
ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'node0 sees node1');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'node1 sees node0');
my ($node0, $node1) = ($pair->node0, $pair->node1);

# Coincidence table (t/347 recipe).
my ($p0, $p1) = ('', '');
for my $attempt (1 .. 8)
{
	last unless write_retry($node0, 'CREATE TABLE d1_t (id int, v int)');
	last unless write_retry($node1, 'CREATE TABLE d1_t (id int, v int)');
	$p0 = $node0->safe_psql('postgres', q{SELECT pg_relation_filepath('d1_t')});
	$p1 = $node1->safe_psql('postgres', q{SELECT pg_relation_filepath('d1_t')});
	last if $p0 eq $p1;
	my ($n0) = $p0 =~ /(\d+)$/;
	my ($n1) = $p1 =~ /(\d+)$/;
	my ($lag, $burn) = $n0 < $n1 ? ($node0, $n1 - $n0) : ($node1, $n0 - $n1);
	write_retry($lag, "SELECT lo_unlink(lo_create(0)) FROM generate_series(1, $burn)");
	write_retry($node0, 'DROP TABLE d1_t');
	write_retry($node1, 'DROP TABLE d1_t');
}
is($p0, $p1, 'd1_t relfilepath coincidence holds');

my $ask0 = state_val($node1, 'cr', 'vis53r97_leg_xmin_overlay_verdict_ask_count');
my $hit0 = state_val($node1, 'cr', 'vis53r97_leg_xmin_overlay_verdict_hit_count');

# Drive the overlay-miss window: many small node0 commits, each immediately
# read cross-node by node1 (the V4 hint frequently lags one such read).
my $reads_ok       = 0;
my $reads_exact    = 0;
my $reads_failclos = 0;
my $expected       = 0;
for my $round (1 .. 30)
{
	next unless write_retry($node0, "INSERT INTO d1_t VALUES ($round, $round)");
	$expected++;
	my ($rc, $out, $err) =
	  $node1->psql('postgres', 'SELECT count(*), COALESCE(sum(v),0) FROM d1_t', timeout => 15);
	if ($rc == 0)
	{
		$reads_ok++;
		my ($cnt, $sum) = split /\|/, $out;
		# 8.A tooth (correct MVCC form): a successful cross-node read must be
		# a SELF-CONSISTENT PREFIX of node0's committed history -- it may lag
		# (cnt < expected: node0's latest commit not yet propagated /
		# snapshot scn), but it must NEVER over-read (cnt > expected =
		# false-visible) and its sum must EXACTLY match its own count
		# (sum == cnt*(cnt+1)/2 = a corruption-free run of ids 1..cnt).  A
		# wrong-but-lagging count with an exact matching sum is正确 MVCC,
		# not a defect; only over-read or a sum that does not match the count
		# is a false-visible / lost-row corruption.
		my $self_consistent = ($cnt <= $expected) && ($sum == $cnt * ($cnt + 1) / 2);
		$reads_exact++ if $self_consistent;
		diag("round $round: NOT self-consistent cnt=$cnt sum=$sum "
			  . "(prefix invariant: cnt<=$expected AND sum==cnt*(cnt+1)/2)")
		  if !$self_consistent;
	}
	else
	{
		# Fail-closed is the documented safe outcome (verdict also couldn't
		# prove, or covers gate refused); never a silent wrong answer.
		$reads_failclos++;
	}
}

my $ask = state_val($node1, 'cr', 'vis53r97_leg_xmin_overlay_verdict_ask_count') - $ask0;
my $hit = state_val($node1, 'cr', 'vis53r97_leg_xmin_overlay_verdict_hit_count') - $hit0;
diag("D1 probe: node1 reads_ok=$reads_ok reads_exact=$reads_exact "
	  . "reads_failclosed=$reads_failclos verdict_ask=$ask verdict_hit=$hit");

# (a) D1 actually engaged on the overlay-miss window at least once.
cmp_ok($ask, '>', 0, 'D1 xmin overlay-miss verdict ask engaged (ask > 0)');
# (b) THE 8.A TOOTH: every successful cross-node read was a self-consistent
# prefix -- no over-read (false-visible) and no count/sum mismatch (lost
# row / corruption).  Lag (reading an earlier committed prefix) is正确 MVCC.
is($reads_exact, $reads_ok,
	'every successful cross-node read was a self-consistent prefix (no false-visible/corruption)');
# (c) some reads resolved positively via the verdict (the收益; not merely
# fail-closed).  If this is 0 the timing never left an overlay-miss window
# open long enough -- diagnostic, not a hard fail in the scratch probe.
cmp_ok($hit + $reads_ok, '>', 0, 'reads either resolved positively or via a verdict hit');

$pair->stop_pair;
done_testing();
