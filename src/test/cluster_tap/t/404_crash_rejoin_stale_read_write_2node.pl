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

sub wait_for
{
	my ($cond, $timeout_s, $step_us) = @_;
	$step_us //= 500_000;
	my $deadline = time() + $timeout_s;
	while (time() < $deadline)
	{
		return 1 if $cond->();
		usleep($step_us);
	}
	return $cond->() ? 1 : 0;
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
# CRASH node1 (online_join=off default).  stop('immediate') is SIGQUIT
# = an UNCLEAN death: the clean-shutdown ALIVE blank does NOT run, so
# node1's prior voting-disk self-slot keeps CLUSTER_VOTING_SLOT_FLAG_ALIVE
# — the signal the off-path rejoin tick fences on (守门裁决 07-15: ALIVE
# bit, not epoch, because a fast rejoin leaves both sides at epoch INITIAL).
# ============================================================
$pair->node1->stop('immediate');
$pair->node1->start;
ok($pair->wait_for_peer_state(0, 1, 'connected', 60)
	  && $pair->wait_for_peer_state(1, 0, 'connected', 60),
	'L1c node1 re-crashed and transport reconnected');
usleep(2_000_000);

# ============================================================
# L3a HARD: the off-path rejoin tick detected the unclean death and armed
# the fence THIS incarnation (counter is per-incarnation shmem, so a fresh
# value >= 1 proves the ALIVE-bit signal fired).
# ============================================================
ok(wait_for(sub {
		state_val($pair->node1, 'grd_recovery', 'offpath_crash_rejoin_fenced') >= 1
	}, 30),
	'L3a HARD: node1 armed the crash-rejoin fence (offpath_crash_rejoin_fenced >= 1)');

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
	# Reads may themselves fail-closed now (node1-home fenced), so use the
	# non-dying psql_row; a fenced write ($wr != 0) is coherent by construction.
	my (undef, $n1, undef) = psql_row($pair->node1, "SELECT count(*) FROM $vuln");
	my (undef, $n0, undef) = psql_row($pair->node0, "SELECT count(*) FROM $vuln");
	diag("L5 divergence: node1=[$n1] node0=[$n0]");
	# coherent: write refused (fail-closed) OR both nodes agree afterwards.
	ok($wr != 0 || ($n1 eq $n0),
		'L5 HARD: home-block write is fail-closed or coherent, never a silent split');

	# ========================================================
	# L5b HARD (boot-race, Shape A命门): after a CRASH restart, every read
	# issued the instant node1 accepts connections must be fail-closed for a
	# node1-home table — not one silent 0 may slip through the boot-to-
	# decision window.  The phase-gate boot barrier (flag defaults 0 at shmem
	# init, BEFORE any LMON tick or the qvotec prior_unclean_death probe)
	# fences self-home blocks RECOVERING from process start, and the crash-
	# rejoin arm then keeps the flag 0, so every read errors 53R9L for the
	# whole incarnation.  RED on the pre-fix binary (a burst of silent 0s).
	# ========================================================
	$pair->node1->stop('immediate');
	$pair->node1->start;   # do NOT wait for settle — probe the boot window
	my ($silent0, $failclosed, $coherent) = (0, 0, 0);
	for my $i (1 .. 40)
	{
		my ($rc, $out, $err) = psql_row($pair->node1, "SELECT count(*) FROM $vuln");
		if    ($rc != 0)               { $failclosed++; }
		elsif ($out eq '0')            { $silent0++; }
		else                           { $coherent++; }   # 64 = coherent serve
		usleep(150_000);
	}
	diag("L5b boot-window reads: fail-closed=$failclosed silent0=$silent0 coherent=$coherent");
	is($silent0, 0,
		'L5b HARD: zero silent 0-row reads across the crash-rejoin boot window');
	cmp_ok($failclosed, '>=', 1, 'L5b node1-home reads fail-closed after the crash restart');
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
# L7 NEGATIVE (full-outage crash co-boot, 守门裁决 07-15 point 4): crash
# BOTH nodes (immediate = ALIVE left set on both), restart both.  With
# online_join=off, neither may silently auto-form — each detects its own
# prior unclean death and fences + closes its write gate (53R60).  This
# pins the approved semantic consequence: after a full-outage crash the
# cluster waits for spec-5.22 admission / ops, it never self-serves stale.
# (A genuine CLEAN full shutdown clears ALIVE and co-boots normally — that
# is the L6 path above, which served fine.)
# ============================================================
$pair->node0->stop('immediate');
$pair->node1->stop('immediate');
$pair->node0->start;
$pair->node1->start;
usleep(3_000_000);
$pair->wait_for_peer_state(0, 1, 'connected', 60);

ok(wait_for(sub {
		state_val($pair->node0, 'grd_recovery', 'offpath_crash_rejoin_fenced') >= 1
		  && state_val($pair->node1, 'grd_recovery', 'offpath_crash_rejoin_fenced') >= 1
	}, 30),
	'L7 NEGATIVE: full-outage crash co-boot fences BOTH nodes (no silent auto-formation)');

# Both write gates closed => writes fail-closed 53R60 (retryable), not silent.
{
	my $t = $cands[0];
	my ($w0r, undef, $w0e) = psql_row($pair->node0, "INSERT INTO $t VALUES (2000, 1)");
	my ($w1r, undef, $w1e) = psql_row($pair->node1, "INSERT INTO $t VALUES (2001, 1)");
	diag("L7 post-full-crash writes: node0 rc=$w0r err=[$w0e]; node1 rc=$w1r err=[$w1e]");
	ok($w0r != 0 && $w1r != 0,
		'L7b NEGATIVE: both nodes fail-closed writes after a full-outage crash (53R60/53R9L)');
}

# ============================================================
# L9: a CLEAN full restart clears ALIVE on both nodes, so the co-boot is
# no longer an unclean rejoin — the fence lifts, the cluster serves again,
# and node0's committed rows are intact (the fence is fail-closed, never a
# permanent wedge on a clean restart).
# ============================================================
$pair->node0->stop('fast');
$pair->node1->stop('fast');
$pair->node0->start;
$pair->node1->start;
usleep(3_000_000);
ok($pair->wait_for_peer_state(0, 1, 'connected', 60)
	  && $pair->wait_for_peer_state(1, 0, 'connected', 60),
	'L9 cluster restarted clean after both legs');

my $readback = '';
for my $i (1 .. 10)
{
	my ($rc, $out, $err) =
	  psql_row($pair->node0, "SELECT count(*) FROM $cands[0] WHERE v = 0");
	if ($rc == 0) { $readback = $out; last; }
	usleep(1_000_000);
}
cmp_ok($readback eq '' ? -1 : $readback + 0, '>=', 64,
	'L9 committed rows intact + cluster usable after a clean restart (fence not permanent)');

done_testing();
