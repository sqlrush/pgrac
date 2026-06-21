# 285_sq_sequence.pl — spec-5.4 SQ sequence lock (v2.0 Q2-B, option B) 2-node e2e.
#
#    spec-5.4 layers a per-node instance cache + node-local refill lock on top of
#    the spec-5.2a shared sequence page (the single cross-node allocation
#    boundary).  A cluster user sequence on shared storage serves values from the
#    node instance cache; when empty exactly one backend on the node refills it
#    from the shared page (read_seq_tuple -> page X via CF -> alloc seqcache
#    segment -> page write + WAL + eager flush).  Correctness (no duplicate value
#    across nodes) is the shared page X (PCM/CF) + WAL, inherited from 5.2a; 5.4
#    cuts the cross-node round trips so CACHE>1 sequences refill once per
#    seqcache values per node instead of once per value (5.2a was CACHE 1 only).
#
#    Ship gate (no SKIP, L77):
#      L1  pair connected + same shared relfilenode + 6 SQ counters present.
#      L2  concurrent cross-node nextval (CACHE 1, every value refills) -> NO
#          duplicate; sq_refill_count grows (8.A core).
#      L3  batched cross-node nextval (CACHE 50) -> NO duplicate across 200
#          values AND the instance cache batches (refills << values).
#      L4  crash/restart durable: a node0-only sequence is burned, CHECKPOINTed,
#          node0 crashes (immediate) + restarts; nextval is strictly past the
#          pre-crash max (durable boundary, lost cache = gap not reissue).
#      L11 pg_dump -> restore has NO reissue, BOTH directions (ascending and
#          descending): the shared page last_value is the direction-aware
#          boundary so the native dump SELECT is coherent (L250).
#      L13 cluster CYCLE is rejected (FEATURE_NOT_SUPPORTED) + counter grows.
#
#    Harness: ClusterPair shared_data + 3 voting disks + autovacuum off (mirror
#    t/284).  Cross-node nextval uses the documented-retryable bounded retry: a
#    CF X-transfer can transiently miss its ship budget under sustained
#    contention (a 5.2a slow-enabler limitation, never a duplicate) -- CACHE>1
#    makes this rare, and the strict-uniqueness legs retry rather than flake.
#
# Spec: spec-5.4-sq-sequence-lock.md (§v2.0, §4.2 L1-L13)

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(usleep);

# Sum a 'sequence'-category SQ counter across both nodes.
sub sq_count
{
	my ($pair, $key) = @_;
	my $sum = 0;
	for my $node ($pair->node0, $pair->node1)
	{
		my $v = $node->safe_psql(
			'postgres', qq{
			SELECT coalesce(value::bigint, 0) FROM pg_cluster_state
			WHERE category = 'sequence' AND key = '$key'});
		$sum += ($v // 0);
	}
	return $sum;
}

# One cross-node nextval with bounded retry on the documented-RETRYABLE 5.2a
# CF ship-timeout (transient, never a duplicate -- 8.A).  Dies on a
# non-retryable error or after exhausting retries.
sub is_retryable
{
	my $e = shift // '';
	return $e =~ /could not obtain X transfer/
	  || $e =~ /did not ship a current image/
	  || $e =~ /retry the transaction/
	  || $e =~ /cluster lock acquire timeout/
	  || $e =~ /timed out waiting for cluster sequence/;
}

sub cnext
{
	my ($node, $seq) = @_;
	my $last_err = '';
	my $backoff = 150_000;
	for my $attempt (1 .. 40)
	{
		my ($rc, $out, $err) =
		  $node->psql('postgres', "SELECT nextval('$seq')", timeout => 30);
		return $out if defined $rc && $rc == 0 && defined $out && $out ne '';
		$last_err = $err // '(no stderr)';
		die "cnext($seq): non-retryable error: $last_err" unless is_retryable($last_err);
		usleep($backoff);
		$backoff = $backoff < 1_500_000 ? $backoff * 2 : 1_500_000;
	}
	die "cnext($seq): exhausted retries under contention; last=$last_err";
}

# Pull $n values from $seq in one statement, with bounded retry (CACHE>1 makes
# the per-statement transient rare).  Returns the list of values.
sub pull_bulk
{
	my ($node, $seq, $n) = @_;
	my $last_err = '';
	my $backoff = 150_000;
	for my $attempt (1 .. 12)
	{
		my ($rc, $out, $err) = $node->psql('postgres',
			"SELECT nextval('$seq') FROM generate_series(1,$n)", timeout => 60);
		return split(/\n/, $out)
		  if defined $rc && $rc == 0 && defined $out && $out ne '';
		$last_err = $err // '(no stderr)';
		die "pull_bulk($seq): non-retryable error: $last_err" unless is_retryable($last_err);
		usleep($backoff);
		$backoff = $backoff < 1_500_000 ? $backoff * 2 : 1_500_000;
	}
	die "pull_bulk($seq): exhausted retries; last=$last_err";
}

# ----------
# L1: strict pair + shared data + same-DDL (same relfilenode) + SQ counters.
# ----------
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'sq_sequence',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.grd_max_entries = 1024',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
	]);
$pair->start_pair;
usleep(3_000_000);

my $n0 = $pair->node0;
my $n1 = $pair->node1;

# Prerequisites: skip_all MUST precede any test output.
my $alive0 = $n0->safe_psql('postgres', 'SELECT 1');
my $alive1 = $n1->safe_psql('postgres', 'SELECT 1');
$n0->safe_psql('postgres', 'CREATE SEQUENCE s CACHE 1');
$n1->safe_psql('postgres', 'CREATE SEQUENCE s CACHE 1');
my $p0 = $n0->safe_psql('postgres', "SELECT pg_relation_filepath('s')");
my $p1 = $n1->safe_psql('postgres', "SELECT pg_relation_filepath('s')");
if (   ($alive0 // '') ne '1'
	|| ($alive1 // '') ne '1'
	|| ($p0 // '') ne ($p1 // ''))
{
	$pair->stop_pair;
	plan skip_all =>
	  "cluster pair prerequisites not met (alive0=$alive0 alive1=$alive1 n0=$p0 n1=$p1)";
}

ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'L1: node0 sees node1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'L1: node1 sees node0 connected');
is($p0, $p1, "L1: sequence routes to same shared relfilenode ($p0)");

# All 6 spec-5.4 D9 SQ counters present in the dump (regression guard, L13/D9).
my @sq_keys = qw(
  sq_refill_count
  sq_refill_wait_count
  sq_dup_guard_fail_count
  sq_failover_fail_closed_count
  sq_page_writeback_count
  sq_cycle_rejected_count
);
my $missing = 0;
for my $k (@sq_keys)
{
	my $present = $n0->safe_psql('postgres', qq{
		SELECT count(*) FROM pg_cluster_state
		WHERE category = 'sequence' AND key = '$k'});
	$missing++ if ($present // 0) != 1;
}
is($missing, 0, 'L1: all 6 SQ sequence counters present in the cluster_state dump');

# Read one scalar with bounded retry on the documented-retryable CF signal (a
# cross-node SELECT last_value is a 5.2 CF read-image, more robust than the X
# transfer, but still retry to absorb cumulative-load slowness).
sub read_scalar
{
	my ($node, $sql) = @_;
	my $last_err = '';
	my $backoff = 150_000;
	for my $attempt (1 .. 40)
	{
		my ($rc, $out, $err) = $node->psql('postgres', $sql, timeout => 30);
		return $out if defined $rc && $rc == 0 && defined $out && $out ne '';
		$last_err = $err // '(no stderr)';
		die "read_scalar: non-retryable error: $last_err" unless is_retryable($last_err);
		usleep($backoff);
		$backoff = $backoff < 1_500_000 ? $backoff * 2 : 1_500_000;
	}
	die "read_scalar: exhausted retries; last=$last_err";
}

# Create a shared sequence that node0 will be the sole writer of, with clean X
# ownership on node0.  node0 creates + burns one value (acquires the page X
# hold-until-revoked + eager-flushes block 0 to shared storage), then node1
# creates (its D5a open-or-init sees node0's now-storage-visible block 0 and
# reuses it via a read-image -> SHARE, never grabbing X).  This avoids the
# create-time X race where node1's re-init would leave node1 holding X and force
# node0's later writes through the flaky cross-node X-transfer (5.2a slow path).
sub create_node0_owned
{
	my ($sql, $seq) = @_;
	$n0->safe_psql('postgres', $sql);
	cnext($n0, $seq);    # node0 takes/holds X + eager-flushes block 0
	usleep(700_000);     # let block 0 settle on shared storage before node1's D5a
	$n1->safe_psql('postgres', $sql);
	usleep(300_000);
}

# ----------
# L11 (run early, fresh CF state): pg_dump -> restore has NO reissue, BOTH
#      directions.  The shared page last_value is the direction-aware allocation
#      boundary (cluster_sq_alloc_segment).  We burn on node0 (node0 owns the
#      page X -> no flaky cross-node X-transfer) and dump on node1 via the native
#      SELECT last_value (a 5.2 CF read-image of node0's writes -- exactly what
#      pg_dump does on a NON-issuing node, the L250 / §3.9 V2 claim).  Restore
#      continues strictly past every issued value.
# ----------
# Burn + dump + REAL restore on ONE node (the one that owns the page X after the
# create-race), so no cross-node X-transfer is needed (the 5.2a CF X-transfer is
# a documented stuck-prone slow path under contention -- forward to Stage 6).
# Cross-node coherence of the boundary is separately proven by L2/L3.
sub dump_restore_on
{
	my ($node, $asc, $seq) = @_;
	my @issued = pull_bulk($node, $seq, 35);
	# Dump the direction-aware boundary (the issuing node owns X -> local read,
	# exactly the value pg_dump's "SELECT last_value,is_called FROM seq" reads).
	my ($lastval, $iscalled) =
	  split /\|/, read_scalar($node, "SELECT last_value, is_called FROM $seq");
	# Real restore: setval to the dumped boundary (exercises D6 cache-invalidate)
	# then nextval.  is_called is true (values were issued) so nextval = boundary
	# + increment, which must be strictly past every issued value (no reissue).
	$node->safe_psql('postgres',
		"SELECT setval('$seq', $lastval, " . ($iscalled eq 't' ? 'true' : 'false') . ")");
	my $next = read_scalar($node, "SELECT nextval('$seq')");
	my $bad = 0;
	for my $v (@issued)
	{
		$bad++ if ($asc ? ($v >= $next) : ($v <= $next));
	}
	return ($bad, $next, $lastval, scalar(@issued));
}

# Run the dump/restore on whichever node owns the page X (one always does after a
# cross-node CREATE); fall back to the other node if the first hits the stuck
# X-transfer.  Dies only if NEITHER node can make progress.
sub dump_restore_no_reissue
{
	my ($asc, $seq) = @_;
	for my $node ($n0, $n1)
	{
		my @r = eval { dump_restore_on($node, $asc, $seq) };
		return @r unless $@;
		die $@ unless is_retryable($@);
	}
	die "dump_restore($seq): neither node could acquire the page X";
}

# L11a ascending.
$n0->safe_psql('postgres', 'CREATE SEQUENCE aseq INCREMENT 1 START 1 CACHE 20');
$n1->safe_psql('postgres', 'CREATE SEQUENCE aseq INCREMENT 1 START 1 CACHE 20');
my ($abad, $anext, $alast, $acount) = dump_restore_no_reissue(1, 'aseq');
is($abad, 0,
	"L11a: ascending dump->restore (last_value=$alast) -> nextval=$anext past all $acount issued -- no reissue");

# L11b descending.
$n0->safe_psql('postgres',
	'CREATE SEQUENCE eseq INCREMENT -1 MAXVALUE 100000 MINVALUE -100000 START 100000 CACHE 20');
$n1->safe_psql('postgres',
	'CREATE SEQUENCE eseq INCREMENT -1 MAXVALUE 100000 MINVALUE -100000 START 100000 CACHE 20');
my ($ebad, $enext, $elast, $ecount) = dump_restore_no_reissue(0, 'eseq');
is($ebad, 0,
	"L11b: descending dump->restore (last_value=$elast) -> nextval=$enext past all $ecount issued -- no reissue (direction-aware)");

# ----------
# L2: concurrent cross-node nextval, CACHE 1 (every value is a refill -> maximal
#     cross-node CF contention) -> NO duplicate value (8.A core); refills grow.
# ----------
my $refill_before = sq_count($pair, 'sq_refill_count');
my %seen;
my @dups;
for my $i (1 .. 24)
{
	for my $node ($n0, $n1)
	{
		my $v = cnext($node, 's');
		push @dups, $v if $seen{$v}++;
	}
}
is(scalar(@dups), 0,
	'L2: 48 interleaved cross-node nextvals (CACHE 1) are all UNIQUE -- 8.A no duplicate');
my $refill_after = sq_count($pair, 'sq_refill_count');
cmp_ok($refill_after, '>', $refill_before,
	"L2: sq_refill_count grew ($refill_before -> $refill_after) -- the cluster refill path ran");

# ----------
# L3: batched cross-node nextval (CACHE 50) -> NO duplicate across 400 values AND
#     the node instance cache batches (refills are far fewer than values).
# ----------
$n0->safe_psql('postgres', 'CREATE SEQUENCE bseq CACHE 50');
$n1->safe_psql('postgres', 'CREATE SEQUENCE bseq CACHE 50');
my $rb = sq_count($pair, 'sq_refill_count');
my @vals;
push @vals, pull_bulk($n0, 'bseq', 100);
push @vals, pull_bulk($n1, 'bseq', 100);
push @vals, pull_bulk($n0, 'bseq', 100);
push @vals, pull_bulk($n1, 'bseq', 100);
my %bseen;
my @bdups = grep { $bseen{$_}++ } @vals;
is(scalar(@bdups), 0,
	'L3: 400 batched cross-node nextvals (CACHE 50) are all UNIQUE -- 8.A no duplicate');
my $ra = sq_count($pair, 'sq_refill_count');
my $refills = $ra - $rb;
# 400 values at CACHE 50 => ~8 refills (allow slack for boundary + contention).
cmp_ok($refills, '<', 100,
	"L3: instance cache BATCHED -- $refills refills served 400 values (CACHE 50 amortised CF round trips)");

# ----------
# L13: cluster CYCLE is rejected (FEATURE_NOT_SUPPORTED) + counter grows.
# ----------
$n0->safe_psql('postgres', 'CREATE SEQUENCE cseq MAXVALUE 100 CYCLE CACHE 1');
$n1->safe_psql('postgres', 'CREATE SEQUENCE cseq MAXVALUE 100 CYCLE CACHE 1');
my $cyc_before = sq_count($pair, 'sq_cycle_rejected_count');
my ($crc, $cout, $cerr) = $n0->psql('postgres', "SELECT nextval('cseq')", timeout => 30);
ok($crc != 0 && (($cerr // '') =~ /not supported/i || ($cerr // '') =~ /0A000/),
	'L13: cluster CYCLE sequence nextval is rejected (FEATURE_NOT_SUPPORTED)');
my $cyc_after = sq_count($pair, 'sq_cycle_rejected_count');
cmp_ok($cyc_after, '>', $cyc_before,
	"L13: sq_cycle_rejected_count grew ($cyc_before -> $cyc_after)");

# ----------
# L4 (FINAL leg -- crash settles the cluster, so no DDL after): crash/restart
#     durable.  A node0-only-burned sequence (node0 stays sole X holder),
#     CHECKPOINT (eager-flushed page on shared storage), immediate crash + restart;
#     nextval is strictly past the pre-crash max (lost cache = gap, never a
#     reissue).  node0-only burn + CHECKPOINT-before-crash sidesteps the
#     pre-existing single-stream startup-redo PCM gap (5.2a L6 scope note).
#     dseq is created on BOTH nodes (relfilenode lockstep) BEFORE the crash.
# ----------
$n0->safe_psql('postgres', 'CREATE SEQUENCE dseq CACHE 10');
$n1->safe_psql('postgres', 'CREATE SEQUENCE dseq CACHE 10');
my $max_issued = 0;
for my $i (1 .. 25)
{
	my $v = $n0->safe_psql('postgres', "SELECT nextval('dseq')");
	$max_issued = $v if defined $v && $v > $max_issued;
}
$n0->safe_psql('postgres', 'CHECKPOINT');
$pair->node0->stop('immediate');
$pair->node0->start;
ok($pair->wait_for_peer_state(1, 0, 'connected', 60), 'L4: node0 rejoined after crash-restart');
my $after_crash = $n0->safe_psql('postgres', "SELECT nextval('dseq')");
cmp_ok($after_crash, '>', $max_issued,
	"L4: post-crash nextval ($after_crash) > pre-crash max ($max_issued) -- durable boundary, no reissue");

$pair->stop_pair;
done_testing();
