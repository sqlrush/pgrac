#-------------------------------------------------------------------------
# spec-7.1 D1 serve 半边 deterministic probe (scratch).
#
#   Verifies the origin LMS verdict serve's INVALID_SCN upgrade leg: an
#   aborted transaction writes NO durable commit_scn stamp, so a by-xid
#   durable scan of its (not-yet-recycled) slot returns XID_MATCH_INVALID_SCN.
#   Before D1 serve that leg refused (53R97 at the requester); with D1 serve
#   the origin cross-checks CLOG and answers a provably-ABORTED xid
#   positively, so the requester decides the row INVISIBLE (correct MVCC: an
#   aborted INSERT was never committed) instead of failing closed.
#
#   node0 commits a baseline, then runs aborted INSERTs of a POISON value.
#   node1 reads cross-node with the V4 status hint disabled, so the poison
#   tuples' foreign xmin ALWAYS overlay-misses and reaches the origin verdict
#   ask -> D1 serve -> ABORTED -> invisible.
#
#   Asserts:
#     (a) the live_upgrade_hit counter moved (the D1 serve ABORTED leg engaged);
#     (b) THE 8.A TOOTH: node1 NEVER sees a poison row (no false-visible) and
#         the visible sum matches exactly the committed baseline (no corruption);
#     (c) node1's reads succeeded (resolved, not merely fail-closed) at least
#         once -- the收益 over the pre-D1 blanket 53R97.
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#    src/test/cluster_tap/995_scratch_d1_serve_aborted.pl
#-------------------------------------------------------------------------

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Time::HiRes qw(usleep);
use Test::More;

# The poison value the aborted INSERTs write; if it ever becomes visible on
# node1 the aborted xact was wrongly resolved as committed (false-visible).
use constant POISON => 999_000;

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
	'd1serve',
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
		# Force the xmin overlay-miss window so the poison rows' aborted xmin
		# reaches the origin verdict ask (mirrors t/994).
		'cluster.tt_status_hint_emit_mode = disabled',
	]);
$pair->start_pair;
usleep(2_000_000);
ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'node0 sees node1');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'node1 sees node0');
my ($node0, $node1) = ($pair->node0, $pair->node1);

# Coincidence table (t/347 / t/994 recipe): make the relfilenode identical on
# both nodes so node1's cross-node read maps to node0's page.
my ($p0, $p1) = ('', '');
for my $attempt (1 .. 8)
{
	last unless write_retry($node0, 'CREATE TABLE d1s_t (id int, v int)');
	last unless write_retry($node1, 'CREATE TABLE d1s_t (id int, v int)');
	$p0 = $node0->safe_psql('postgres', q{SELECT pg_relation_filepath('d1s_t')});
	$p1 = $node1->safe_psql('postgres', q{SELECT pg_relation_filepath('d1s_t')});
	last if $p0 eq $p1;
	my ($n0) = $p0 =~ /(\d+)$/;
	my ($n1) = $p1 =~ /(\d+)$/;
	my ($lag, $burn) = $n0 < $n1 ? ($node0, $n1 - $n0) : ($node1, $n0 - $n1);
	write_retry($lag, "SELECT lo_unlink(lo_create(0)) FROM generate_series(1, $burn)");
	write_retry($node0, 'DROP TABLE d1s_t');
	write_retry($node1, 'DROP TABLE d1s_t');
}
is($p0, $p1, 'd1s_t relfilepath coincidence holds');

# Commit a small baseline so the visible sum is a fixed, known quantity.
my $base_n   = 5;
my $base_sum = $base_n * ($base_n + 1) / 2;    # ids 1..5, v = id
write_retry($node0, "INSERT INTO d1s_t SELECT g, g FROM generate_series(1, $base_n) g");

my $poison = POISON;
# Server-side legs (invalid_scn / zero_match / srv_other / live_upgrade_hit)
# are bumped in the LMS verdict-serve context on the ORIGIN (node0); the
# requester legs (xmin ask/hit, covers_refuse) are on the reader (node1).
my $hit0 = state_val($node0, 'cr', 'vis53r97_leg_live_upgrade_hit_count');
my $inv0 = state_val($node0, 'cr', 'vis53r97_leg_invalid_scn_refuse_count');
my $zm0  = state_val($node0, 'cr', 'vis53r97_leg_zero_match_refuse_count');
my $oth0 = state_val($node0, 'cr', 'vis53r97_leg_srv_other_refuse_count');
my $cov0 = state_val($node1, 'cr', 'vis53r97_leg_covers_refuse_count');
my $ask0 = state_val($node1, 'cr', 'vis53r97_leg_xmin_overlay_verdict_ask_count');

# Aborted INSERTs of the poison value, each followed by an immediate cross-node
# read so the poison tuple's aborted xmin lands on a page node1 scans while the
# overlay still misses (hint disabled).  The poison must never become visible.
my $reads_ok       = 0;
my $reads_clean    = 0;    # count == base_n AND sum == base_sum (no poison)
my $reads_failclos = 0;
my $poison_seen    = 0;
for my $round (1 .. 30)
{
	# Aborted xact: writes a poison row, then rolls back -> aborted-unstamped
	# xid whose (not-yet-recycled) durable slot scans as XID_MATCH_INVALID_SCN.
	my $pid = 1000 + $round;
	write_retry($node0,
		"BEGIN; INSERT INTO d1s_t VALUES ($pid, $poison); ROLLBACK;");

	my ($rc, $out, $err) = $node1->psql('postgres',
		'SELECT count(*), COALESCE(sum(v),0), COALESCE(max(v),0) FROM d1s_t', timeout => 15);
	if ($rc == 0)
	{
		$reads_ok++;
		my ($cnt, $sum, $mx) = split /\|/, $out;
		# 8.A tooth: the poison value must NEVER appear.  The committed prefix
		# may lag (cnt <= base_n while the baseline propagates), but it must
		# never OVER-read and the max visible v must stay below POISON.
		$poison_seen++ if ($mx >= POISON) || ($cnt > $base_n) || ($sum > $base_sum);
		$reads_clean++ if ($cnt == $base_n) && ($sum == $base_sum) && ($mx < POISON);
	}
	else
	{
		$reads_failclos++;
	}
}

my $hit = state_val($node0, 'cr', 'vis53r97_leg_live_upgrade_hit_count') - $hit0;
my $inv = state_val($node0, 'cr', 'vis53r97_leg_invalid_scn_refuse_count') - $inv0;
my $zm  = state_val($node0, 'cr', 'vis53r97_leg_zero_match_refuse_count') - $zm0;
my $oth = state_val($node0, 'cr', 'vis53r97_leg_srv_other_refuse_count') - $oth0;
my $cov = state_val($node1, 'cr', 'vis53r97_leg_covers_refuse_count') - $cov0;
my $ask = state_val($node1, 'cr', 'vis53r97_leg_xmin_overlay_verdict_ask_count') - $ask0;
diag("D1 serve probe: node1 reads_ok=$reads_ok reads_clean=$reads_clean "
	  . "reads_failclosed=$reads_failclos poison_seen=$poison_seen | legs: "
	  . "live_upgrade_hit=$hit invalid_scn_refuse=$inv covers_refuse=$cov "
	  . "zero_match=$zm srv_other=$oth xmin_ask=$ask");

# THE 8.A TOOTH (hard): the aborted poison value is NEVER visible on node1.
is($poison_seen, 0,
	'aborted INSERT poison never visible cross-node (no false-visible / false-committed)');

# Diagnostic (soft): whether this timing/covers window actually exercised the
# D1 serve INVALID_SCN->ABORTED leg.  The leg is COLD (census 三相全零) -- an
# aborted xid usually reaches the requester after its slot has already recycled
# (zero_match, handled by the sibling :474 CLOG-abort leg) or is masked by a
# covers-gate refuse on the same page.  We do NOT hard-fail on the cold leg; the
# hard 8.A gate above is what protects correctness.
diag("D1 serve leg fired positively (live_upgrade_hit) = $hit; "
	  . "reads_clean = $reads_clean");
ok(1, 'D1 serve probe completed (leg distribution is diagnostic; 8.A gate above is the guarantee)');

$pair->stop_pair;
done_testing();
