#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 404_crash_rejoin_stale_read_write_2node.pl
#	  Crash-rejoin (online_join=off) home-block coherence — the P0
#	  fail-open hole the crash-rejoin re-declare barrier (fix 1) and
#	  the cold-GRD watermark fail-closed (fix 2) close.
#
#	  Root cause (diagnosed 2026-07-15, evidence bundle bounce-read-p0/):
#	  with cluster.online_join = off (the DEFAULT), a node that crash-
#	  restarts into a running cluster skips the joiner self-tick entirely
#	  (cluster_reconfig.c:1984 `!cluster_online_join -> return`), so its
#	  self_join_admitted stays at the shmem-init value 1
#	  (cluster_reconfig.c:206 `online_join ? 0 : 1`).  It boots straight
#	  to a writable MEMBER with an EMPTY GRD and NO re-declare barrier.
#	  For blocks whose static hash master is this node
#	  (cluster_gcs.c:263), the acquire path finds master == self
#	  (cluster_pcm_lock.c:2565), reads an empty local GRD, and cold-grants
#	  from the pre-read stale/empty disk page (cluster_pcm_lock.c:2689).
#	  The round-4c freshness gate that should catch it queries the same
#	  wiped GRD watermark -> InvalidScn -> SKIP (cluster_gcs_block.c:1278).
#	  Result: silent stale/empty READ, and a WRITE on the stale image
#	  that diverges and — after flush ordering — durably LOSES a peer's
#	  committed rows.  Neither ever errors.
#
#	  Correct behaviour (both fixes): a home block of a just-rejoined node
#	  is fenced RECOVERING (retryable) until the survivors re-declare what
#	  they hold; reads/writes are either served COHERENTLY or fail closed
#	  with a retryable SQLSTATE — NEVER a silent 0-row read and NEVER a
#	  silently-lost committed write.
#
#	  L1  ClusterPair startup; a coinciding-filepath home fixture whose
#	      block0 static master is node1 is selected behaviorally
#	  L2  node0 writes 64 committed rows (kept in node0 buffers)
#	  L3  DIAGNOSTIC (never fails): the membership split after node1 bounce
#	      (node0's view of node1, node1's view of self) — pins the boot
#	      self-admit path
#	  L4  HARD (fix contract): node1's post-bounce read of its home table
#	      is COHERENT (64) or FAILS CLOSED (error) — never a silent 0
#	  L5  HARD (fix contract): node1's post-bounce write is COHERENT or
#	      FAILS CLOSED — never a silent divergent commit
#	  L6  HARD (blast radius): after node1-first / node0-last flush + a
#	      full restart, node0's committed rows SURVIVE on disk (no lost
#	      write); if L5 fenced the write this is trivially satisfied
#	  L7  fence observability: the join-block fail-closed counter moved
#	      (fix 1 armed the self-fence) — soft while unimplemented
#
# Spec: spec-5.16 (online rejoin PCM fence) extended to the crash-rejoin /
#	online_join=off path; spec-4.7 (survivor re-declare); Rule 8.A.
#
# Author: SqlRush <sqlrush@gmail.com>
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

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'crash_rejoin_coherence',
	quorum_voting_disks => 3,
	shared_data         => 1,
	data_port_span      => 2,
	extra_conf          => ['autovacuum = off']);
$pair->start_pair;
usleep(3_000_000);

ok($pair->wait_for_peer_state(0, 1, 'connected', 60)
	  && $pair->wait_for_peer_state(1, 0, 'connected', 60),
	'L1 peers connected');

sub psql_row
{
	my ($node, $sql) = @_;
	my ($rc, $out, $err) = $node->psql('postgres', $sql);
	return ($rc, $out, (split(/\n/, $err // ''))[0] // '');
}

sub state_val
{
	my ($node, $cat, $key) = @_;
	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state WHERE category='$cat' AND key='$key'});
	return defined($v) && $v ne '' ? $v + 0 : 0;
}

sub membership_state
{
	my ($node, $nid) = @_;
	my $v = eval {
		$node->safe_psql('postgres',
			"SELECT state FROM pg_cluster_membership WHERE node_id = $nid");
	};
	return defined($v) ? $v : '<err>';
}


# ============================================================
# L1b: coinciding-filepath fixture (no shared catalog on this rig, so
# create on both nodes and keep candidates whose relfilenode paths
# coincide -> same shared-storage file).
# ============================================================
my @cands;
for my $i (1 .. 16)
{
	my $t = "hb_t$i";
	$_->safe_psql('postgres', "CREATE TABLE $t (k int, v int)")
	  for ($pair->node0, $pair->node1);
	my $p0 = $pair->node0->safe_psql('postgres', "SELECT pg_relation_filepath('$t')");
	my $p1 = $pair->node1->safe_psql('postgres', "SELECT pg_relation_filepath('$t')");
	push @cands, $t if ($p0 // '') eq ($p1 // '');
}
ok(scalar(@cands) > 0, 'L1b coinciding-filepath candidates exist');

# ============================================================
# L2: node0 populates every candidate with 64 committed rows (held in
# node0's buffers until an explicit flush).
# ============================================================
$pair->node0->safe_psql('postgres',
	"INSERT INTO $_ SELECT g, 0 FROM generate_series(1, 64) g")
  for @cands;

# ============================================================
# Bounce node1 (crash-rejoin, online_join=off default).
# ============================================================
$pair->node1->stop('fast');
$pair->node1->start;
ok($pair->wait_for_peer_state(0, 1, 'connected', 60)
	  && $pair->wait_for_peer_state(1, 0, 'connected', 60),
	'L1c node1 rebounced and transport reconnected');
usleep(2_000_000);

# ============================================================
# L3: DIAGNOSTIC — the membership split (never fails the run; pins the
# self-admit boot path for the report).
# ============================================================
diag(sprintf('L3 membership after bounce: node0 sees node1=[%s]; '
	  . 'node1 sees self=[%s]',
	membership_state($pair->node0, 1), membership_state($pair->node1, 1)));
ok(1, 'L3 membership split recorded (diagnostic)');

# Behavioral master-discrimination: pick a candidate whose block0 home is
# node1 (post-bounce read is stale/empty or fail-closed) vs node0 (served
# correctly).  The vulnerable table drives L4-L6.
my ($vuln, $safe);
for my $t (@cands)
{
	my ($rc, $out, $err) = psql_row($pair->node1, "SELECT count(*) FROM $t");
	diag("probe node1 read $t: rc=$rc out=[$out] err=[$err]");
	if ($rc == 0 && $out eq '0')  { $vuln //= $t; }
	elsif ($rc == 0 && $out eq '64') { $safe //= $t; }
	elsif ($rc != 0)              { $vuln //= $t; }   # fail-closed also = home block
}
diag("selected vuln=" . ($vuln // 'NONE') . " safe=" . ($safe // 'none'));

SKIP: {
	skip 'no node1-home candidate surfaced this run', 4 unless defined $vuln;

	# ========================================================
	# L4 HARD: node1's read of its home table is COHERENT or FAIL-CLOSED,
	# never a silent 0.  RED today: rc=0 out=0 (silent empty read).
	# ========================================================
	my ($r4, $o4, $e4) = psql_row($pair->node1, "SELECT count(*) FROM $vuln");
	diag("L4 node1 home read: rc=$r4 out=[$o4] err=[$e4]");
	ok(($r4 == 0 && $o4 eq '64') || $r4 != 0,
		'L4 HARD: home-block read is coherent (64) or fail-closed, never a silent 0');

	# ========================================================
	# L5 HARD: node1's write onto its home block is COHERENT or FAIL-
	# CLOSED, never a silently divergent commit.  RED today: rc=0, node1
	# then sees 1 row while node0 sees 64 (split brain).
	# ========================================================
	my ($wr, $wo, $we) = psql_row($pair->node1, "INSERT INTO $vuln VALUES (1000, 1000)");
	diag("L5 node1 write on home block: rc=$wr err=[$we]");
	my $n1 = $pair->node1->safe_psql('postgres', "SELECT count(*) FROM $vuln");
	my $n0 = $pair->node0->safe_psql('postgres', "SELECT count(*) FROM $vuln");
	diag("L5 divergence: node1=[$n1] node0=[$n0]");
	# coherent: write refused (fail-closed) OR both nodes agree afterwards.
	ok($wr != 0 || ($n1 eq $n0),
		'L5 HARD: home-block write is fail-closed or coherent, never a silent split');

	# ========================================================
	# L5b HARD (boot-race, Shape A命门): reads issued CONCURRENTLY, the
	# instant node1 accepts connections after a fresh bounce, must ALL be
	# fail-closed for a node1-home table — not one silent 0 may slip
	# through the boot-to-decision window.  The phase-gate boot barrier
	# (flag defaults 0 at shmem init) fences self-home blocks RECOVERING
	# from process start, so every early read errors 53R9L; RED on the
	# pre-fix binary (a burst of silent 0s).
	# ========================================================
	$pair->node1->stop('fast');
	$pair->node1->start;   # do NOT wait for settle — probe the boot window
	my ($silent0, $failclosed, $coherent) = (0, 0, 0);
	for my $i (1 .. 40)
	{
		my ($rc, $out, $err) = psql_row($pair->node1, "SELECT count(*) FROM $vuln");
		if    ($rc != 0)               { $failclosed++; }
		elsif ($out eq '0')            { $silent0++; }
		else                           { $coherent++; }   # 64 = decided+served
		last if $coherent >= 3;        # barrier lifted + coherent: window over
		usleep(150_000);
	}
	diag("L5b boot-window reads: fail-closed=$failclosed silent0=$silent0 coherent=$coherent");
	is($silent0, 0,
		'L5b HARD: zero silent 0-row reads in the boot-to-decision window');
	# resettle for L6
	$pair->wait_for_peer_state(0, 1, 'connected', 60);
	usleep(1_000_000);

	# ========================================================
	# L6 HARD (blast radius): node1-first / node0-last flush, then a full
	# restart — node0's 64 committed rows must SURVIVE on disk.  RED
	# today: node0=64 wins the flush race but node1's INSERT (if it
	# committed on the stale image) is gone AND, worse, the pre-existing
	# data can be clobbered; we assert node0's committed rows are intact.
	# ========================================================
	$pair->node1->psql('postgres', 'CHECKPOINT');
	$pair->node0->psql('postgres', 'CHECKPOINT');
	$pair->node0->stop('fast');
	$pair->node1->stop('fast');
	$pair->node0->start;
	$pair->node1->start;
	usleep(3_000_000);
	$pair->wait_for_peer_state(0, 1, 'connected', 60);

	my $survive = '';
	for my $i (1 .. 8)
	{
		my ($rc, $out, $err) =
		  psql_row($pair->node0, "SELECT count(*) FROM $vuln WHERE v = 0");
		if ($rc == 0) { $survive = $out; last; }
		usleep(1_000_000);
	}
	diag("L6 node0 committed rows after restart: [$survive] (expect >= 64)");
	cmp_ok($survive eq '' ? -1 : $survive + 0, '>=', 64,
		"L6 HARD: node0's committed rows survive the crash-rejoin flush race");
}

# ============================================================
# L7: fence observability (soft until fix 1 lands) — the join-block
# fail-closed counter moved, proving the self-fence armed on the off path.
# ============================================================
my $failclosed =
  state_val($pair->node1, 'grd_recovery', 'join_block_recovering_failclosed');
diag("L7 join_block_failclosed_count on node1 = $failclosed "
	  . '(soft: > 0 once fix 1 arms the crash-rejoin self-fence)');
ok(1, 'L7 fence counter recorded (soft)');

done_testing();
