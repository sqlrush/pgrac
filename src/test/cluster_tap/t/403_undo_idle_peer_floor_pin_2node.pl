#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 403_undo_idle_peer_floor_pin_2node.pl
#	  TT lane — idle-peer floor pin + recycle cadence under write load.
#
#	  Root cause under repair (S3 pre-queue baseline 2026-07-15, node0
#	  1.46M lock-timeout + 11.3k 53R9E at segments 255/256 + repeated
#	  "retention horizon pinned" with the floor PROVEN): the cluster
#	  recycle floor folds in every idle peer's horizon report, and an
#	  idle peer reports its own Lamport clock sample.  That sample only
#	  tracks the writer through envelope observes at the heartbeat /
#	  report cadence (~1-2 LMON ticks), so the proven floor trails the
#	  writer's clock by 1-2 ticks.  The TT slot pool is
#	  undo_segments_max_per_instance x 48 slots; as soon as
#	  write-rate x floor-lag exceeds the pool, EVERY pooled slot is
#	  younger than the floor, nothing is ever recyclable, and the pool
#	  camps at the hard cap while the pinned/progress cycle oscillates.
#
#	  The fix (two pre-approved arms):
#	    H1  a provably idle peer (zero active xacts AND zero held
#	        snapshots) reports an UNCONSTRAINED sentinel instead of its
#	        clock sample; the fold's min skips it, so a lone writer's
#	        floor is its own local horizon (ms-scale lag).  Anything
#	        not provably idle keeps today's conservative sample; a peer
#	        waking mid-window self-fences through the BELOW_HORIZON
#	        read_scn admissibility gate (fail-closed, retry heals).
#	    H2  a pass that exhausts its segment batch while making
#	        progress re-runs immediately instead of sleeping the
#	        recycle interval (pressure-driven continuous mode); the
#	        "who may be recycled" decision layer is untouched.
#
#	  The lmon interval is raised to 2s as a deterministic lag
#	  amplifier and the pool is shrunk to the GUC floor (16 segments =
#	  768 slots) so the pin reproduces within seconds at single-psql
#	  write rates; budgets keep the whole file under ~90s of load.
#
#	  L1  ClusterPair startup; initial cluster floor proven
#	  L2  write phase: node0 sustained short transactions, node1
#	      strictly idle; per-sample floor lag + recycle progress
#	  L3  HARD ASSERT (H1 contract): max observed (writer clock -
#	      proven floor) stays below the pool capacity in slots
#	  L4  HARD ASSERT: zero hard-cap failures across the whole phase
#	      (undo.tt_rollover_fail_hard_cap_count et al stay 0)
#	  L5  HARD ASSERT (H2 contract): recycling makes progress DURING
#	      the phase (segments_marked_recyclable advances), not only
#	      after quiesce
#	  L6  sentinel evidence: node1 published idle sentinels and node0's
#	      fold never stalled during the phase
#	  L7  PROTECTION NEGATIVE: a live snapshot on node1 suppresses the
#	      sentinel — the floor clamps to the holder while it lives and
#	      resumes after release (predicate never fires when unsure)
#	  L8  wake-up self-heal: node1's first cross-node read after the
#	      storm succeeds (bounded retries allowed: the inadmissible
#	      BELOW_HORIZON arm is fail-closed and observe heals it)
#	  L10 GATED self-heal proof (runs before L9), RED->GREEN twin
#	      on ONE driver with NO cleanout bypass: a recycled node0
#	      xid first fails closed 53R97 under the default
#	      cluster.crossnode_runtime_visibility = off (verdict wire
#	      provably untouched), then -- GUC flipped on, nothing else
#	      changed -- resolves through the COMMITTED_BELOW_HORIZON
#	      origin-verdict arm: exact data terminal + rtvis verdict
#	      counter evidence
#	  L9  restart clean
#
# Spec: spec-5.22e (cluster undo retention brake) S2.1 sampling rule +
#	S3 pre-queue baseline forensics (TT lane, idle-peer floor pin).
#
# Author: SqlRush <sqlrush@gmail.com>
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use File::Temp qw(tempdir);
use IPC::Run ();
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep time);

my $POOL_SEGMENTS   = 16;					  # GUC floor; smallest legal pool
my $SLOTS_PER_SEG   = 48;					  # TT_SLOTS_PER_SEGMENT (ABI const)
my $POOL_SLOTS      = $POOL_SEGMENTS * $SLOTS_PER_SEG;
my $LMON_INTERVAL_S = 2;					  # lag amplifier (freshness = 3x)
my $PHASE_S         = 60;					  # FIXED write-phase duration; the
											  # writers are fed more work than
											  # they can finish and are killed
											  # at the deadline, so machine
											  # speed variance cannot flake a
											  # completion budget
my $WRITER_LINES    = 120_000;				  # per writer; >> any 60s throughput
my $SCN_LOCAL_MASK  = '72057594037927936';	  # 2^56 (numeric, server-side)

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'undo_floor_pin',
	quorum_voting_disks => 3,
	shared_data         => 1,
	data_port_span      => 2,
	extra_conf          => [
		'autovacuum = off',
		'synchronous_commit = off',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
		"cluster.lmon_main_loop_interval = ${\($LMON_INTERVAL_S * 1000)}",
		"cluster.undo_segments_max_per_instance = $POOL_SEGMENTS",
		'cluster.undo_cleaner_interval_ms = 1000',
	]);
$pair->start_pair;

usleep(3_000_000);

is($pair->node0->safe_psql('postgres', 'SELECT 1'),
	'1', 'L1 node0 postmaster alive');
is($pair->node1->safe_psql('postgres', 'SELECT 1'),
	'1', 'L1 node1 postmaster alive');

ok($pair->wait_for_peer_state(0, 1, 'connected', 60)
	  && $pair->wait_for_peer_state(1, 0, 'connected', 60),
	'L1 peers connected');


sub state_val
{
	my ($node, $cat, $key) = @_;
	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state WHERE category='$cat' AND key='$key'});
	return defined($v) && $v ne '' ? $v + 0 : 0;
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

# Retry-tolerant single-statement write.  Cross-node block ping-pong can
# transiently bounce a statement off the GES master work queue
# (source=master-reject-queue-full, elapsed_ms=0 -- a capacity reject
# mapped to the retryable 53R70 shape, structural fix owned by the
# convert-queue lane); the harness only needs SOME commits to land, so it
# retries instead of dying mid-leg.
sub write_retry
{
	my ($node, $sql, $times) = @_;
	for my $i (1 .. $times)
	{
		for my $try (1 .. 3)
		{
			my ($rc, $out, $err) = $node->psql('postgres', $sql);
			last if $rc == 0;
			usleep(200_000);
		}
	}
}

# Time-axis (local) part of the proven floor, server-side (uint64-safe).
# The encoded floor carries the winning reporter's NODE bits in the high
# byte, so ORDER comparisons on the raw value are meaningless when the
# winner changes (the L457 raw-SCN-comparison ban applies to the harness
# too); every before/after comparison below uses this projection.
sub floor_local
{
	my ($node) = @_;
	my $v = $node->safe_psql('postgres', qq{
		SELECT (value::numeric % ${SCN_LOCAL_MASK}::numeric)::text
		  FROM pg_cluster_state
		 WHERE category='undo' AND key='horizon_last_floor_scn'});
	return defined($v) && $v ne '' ? $v + 0 : 0;
}

# Floor lag in local-SCN units, computed SERVER-side (uint64-safe: Perl NV
# would lose precision on the node-tagged encoded floor).  One statement =
# one snapshot, so the pair is self-consistent.  Returns -1 while the floor
# is unproven (never counted as a lag sample).
sub floor_lag
{
	my ($node) = @_;
	my $v = $node->safe_psql('postgres', qq{
		SELECT CASE WHEN f.v = 0 THEN -1
					ELSE (s.v - (f.v % ${SCN_LOCAL_MASK}::numeric))::text::numeric END
		  FROM (SELECT value::numeric AS v FROM pg_cluster_state
				 WHERE category='scn' AND key='scn_current_local') s,
			   (SELECT value::numeric AS v FROM pg_cluster_state
				 WHERE category='undo' AND key='horizon_last_floor_scn') f});
	return defined($v) && $v ne '' ? $v + 0 : -1;
}

# ============================================================
# L1b: fixture + initial floor proven before the load phase.
#
# Coinciding-filepath heap fixture on shared storage (t/394 recipe):
# this rig has no shared catalog, so the table is created on BOTH
# nodes and candidates are retried until the relfilenode paths
# coincide -- then both catalogs address the same shared-storage
# file and node1's L8 read is a true cross-node MVCC read of
# node0's writes.
#
# The fixture commits also prime the SCN clock: a fully idle fresh
# formation has never advanced it, so the local horizon sample is
# InvalidScn and the fold self-stalls MALFORMED (U7b) until the
# first commit.
# ============================================================
my $tbl;
for my $i (1 .. 12)
{
	my $t = "t403_$i";
	$_->safe_psql('postgres', "CREATE TABLE $t (k int, v int)")
	  for ($pair->node0, $pair->node1);
	my $p0 = $pair->node0->safe_psql('postgres', "SELECT pg_relation_filepath('$t')");
	my $p1 = $pair->node1->safe_psql('postgres', "SELECT pg_relation_filepath('$t')");
	if (($p0 // '') eq ($p1 // '')) { $tbl = $t; last; }
}
die 'no coinciding filepath found' unless defined $tbl;
diag("fixture table=$tbl");
$pair->node0->safe_psql('postgres',
	"INSERT INTO $tbl SELECT g, 0 FROM generate_series(1, 64) g");

ok(wait_for(sub { state_val($pair->node0, 'undo', 'horizon_last_floor_scn') > 0 }, 30),
	'L1b initial cluster floor proven on node0');


# ============================================================
# L2: write phase.  node0 runs a sustained stream of short
# autocommit transactions from 2 background psql sessions; node1 is
# NEVER touched during the phase (its idleness is the mechanism
# under test).  All sampling runs against node0 only.
# ============================================================

my $tmpdir  = tempdir(CLEANUP => 1);
my $sqlfile = "$tmpdir/writer.sql";
{
	open(my $fh, '>', $sqlfile) or die "cannot write $sqlfile: $!";
	for my $i (1 .. $WRITER_LINES)
	{
		my $k = ($i % 64) + 1;
		print $fh "UPDATE $tbl SET v = v + 1 WHERE k = $k;\n";
	}
	close($fh);
}

my $stall_pre      = state_val($pair->node0, 'undo', 'horizon_stall_count');
my $recyc_pre      = state_val($pair->node0, 'undo', 'cleaner_segments_marked_recyclable');
my $hardcap_tt_pre = state_val($pair->node0, 'undo', 'tt_rollover_fail_hard_cap_count');
my $hardcap_seg_pre = state_val($pair->node0, 'undo', 'segment_hard_cap_fail_count');
my $rollover_pre   = state_val($pair->node0, 'undo', 'tt_retention_rollover_count');

my @writers;
for my $w (1 .. 2)
{
	my ($in, $out, $err) = ('', '', '');
	push @writers,
	  [
		IPC::Run::start(
			[
				'psql', '-X', '-A', '-t', '-q',
				'-d', $pair->node0->connstr('postgres'),
				'-f', $sqlfile
			],
			\$in, \$out, \$err),
		\$out, \$err
	  ];
}

# Sampling loop: floor lag + mid-phase recycle progress, ~2 samples/s,
# for a FIXED phase duration; the writers are then killed (they carry far
# more work than any machine finishes in the window, so wall-clock speed
# never flakes this leg).
my $lag_max          = 0;
my $recyc_mid_delta  = 0;
my $phase_t0         = time();
while (time() - $phase_t0 < $PHASE_S)
{
	my $lag = floor_lag($pair->node0);
	$lag_max = $lag if $lag > $lag_max;
	my $rd = state_val($pair->node0, 'undo', 'cleaner_segments_marked_recyclable')
	  - $recyc_pre;
	$recyc_mid_delta = $rd if $rd > $recyc_mid_delta;

	usleep(400_000);
}
$_->[0]->kill_kill for @writers;

my $writer_errs = '';
$writer_errs .= ${ $_->[2] } for @writers;
my $rollover_delta = state_val($pair->node0, 'undo', 'tt_retention_rollover_count')
  - $rollover_pre;
diag(sprintf(
	"L2 phase: %ds fixed, lag_max=%d slots-units (pool=%d), "
	  . "mid-phase recyclable delta=%d, rollovers=%d",
	$PHASE_S, $lag_max, $POOL_SLOTS, $recyc_mid_delta, $rollover_delta));

# >= 2 pool laps of slot churn proves the writers ran sustained, unwedged.
cmp_ok($rollover_delta, '>=', 2 * $POOL_SEGMENTS,
	'L2 write phase sustained (>= 2 pool laps of TT rollover)');
cmp_ok($lag_max, '>', 0, 'L2b lag sampler captured real samples');

# Post-storm GES drain barrier.  The storm leaves node1's master work
# queue saturated (source=master-reject-queue-full, elapsed_ms=0) for a
# bounded tail while admitted waiters expire their 60s timeouts.  The
# legs below measure floor semantics, not queue capacity (the
# convert-queue lane owns that wedge class), so wait for the lock plane
# to serve again -- with a HARD budget: a queue that never drains is a
# wedge this test must fail on, not paper over.
my $drained  = 0;
my $drain_t0 = time();
for my $i (1 .. 45)
{
	my ($rc, $out, $err) =
	  $pair->node0->psql('postgres', "UPDATE $tbl SET v = v + 1 WHERE k = 64");
	if ($rc == 0) { $drained = 1; last; }
	usleep(2_000_000);
}
ok($drained,
	sprintf('L2c post-storm GES plane drained (%.0fs)', time() - $drain_t0));

# ============================================================
# L3: HARD ASSERT (H1 contract) — the proven floor tracks the lone
# writer's clock: the worst observed lag stays below the pool
# capacity, so retained-younger-than-floor inventory can never fill
# the pool.  RED on the pre-fix binary: the floor trails the idle
# peers' report cadence (1-2 lmon ticks x write rate >> pool).
# ============================================================
cmp_ok($lag_max, '<', $POOL_SLOTS,
	'L3 HARD ASSERT: floor lag stays below pool capacity under a lone writer');

# ============================================================
# L4: HARD ASSERT — the pool never hit the hard cap.
# ============================================================
my $hardcap_tt  = state_val($pair->node0, 'undo', 'tt_rollover_fail_hard_cap_count')
  - $hardcap_tt_pre;
my $hardcap_seg = state_val($pair->node0, 'undo', 'segment_hard_cap_fail_count')
  - $hardcap_seg_pre;
is($hardcap_tt, 0,
	'L4 HARD ASSERT: zero TT rollover hard-cap failures during the phase');
is($hardcap_seg, 0, 'L4b zero record-extent hard-cap failures during the phase');
unlike($writer_errs, qr/hard cap|53R9E/i,
	'L4c writers saw no hard-cap errors');

# ============================================================
# L5: HARD ASSERT (H2 contract) — recycling progressed DURING the
# phase (not only at quiesce): the cleaner kept pace with the
# writer instead of camping on the interval.
# ============================================================
cmp_ok($recyc_mid_delta, '>', 0,
	'L5 HARD ASSERT: segments marked recyclable while the write phase ran');

# ============================================================
# L6: sentinel evidence.  node1 (idle throughout) published
# unconstrained sentinel reports; node0's fold consumed them without
# a single stall.  (node1 is only queried AFTER the phase, so the
# query itself cannot have broken its idleness mid-phase.)
# ============================================================
cmp_ok(state_val($pair->node1, 'undo', 'horizon_idle_sentinel_sent_count'),
	'>', 0, 'L6 node1 sent idle-unconstrained sentinel reports');
is(state_val($pair->node0, 'undo', 'horizon_stall_count') - $stall_pre,
	0, 'L6b node0 fold never stalled during the phase');

# ============================================================
# L7: PROTECTION NEGATIVE — a live snapshot on node1 suppresses the
# sentinel.  While node1 holds a REPEATABLE READ snapshot, its
# report is the conservative sample again, so node0's floor clamps
# at the holder and stops tracking new commits; releasing the
# snapshot lets the floor move again.  This pins the predicate's
# fail-conservative direction (rule 8.A polarity).
# ============================================================
my $sentinel_pre_hold =
  state_val($pair->node1, 'undo', 'horizon_idle_sentinel_sent_count');

# The snapshot is taken with SELECT 1 -- deliberately NO table access, so
# the holder cannot bounce off a transient cross-node GES reject and die
# early (which would silently un-hold the snapshot and flake both asserts
# below).  A cluster-storage-mode snapshot publishes read_scn regardless
# of what it reads.
my ($h_in, $h_out, $h_err) = ('', '', '');
my $holder = IPC::Run::start(
	[
		'psql', '-X', '-A', '-t', '-q',
		'-d', $pair->node1->connstr('postgres'),
		'-c', 'BEGIN ISOLATION LEVEL REPEATABLE READ; '
		  . 'SELECT 1; '
		  . 'SELECT pg_sleep(15); '
		  . 'COMMIT;'
	],
	\$h_in, \$h_out, \$h_err);

# Give the holder time to take its snapshot and node1 time to publish a
# conservative (non-sentinel) report at the 2s lmon cadence.
usleep(4_000_000);

my $floor_hold_a = floor_local($pair->node0);
write_retry($pair->node0, "UPDATE $tbl SET v = v + 1 WHERE k <= 8", 6);
usleep(2_500_000);
my $floor_hold_b = floor_local($pair->node0);

is($floor_hold_b, $floor_hold_a,
	'L7 HARD ASSERT: floor clamped while node1 held a live snapshot');

$holder->finish;
is($h_err, '', 'L7d holder session ran clean (snapshot actually held)');
my $sentinel_post_hold =
  state_val($pair->node1, 'undo', 'horizon_idle_sentinel_sent_count');

# The hold spanned ~7 report ticks; a sentinel-per-tick would have advanced
# the gauge by ~7.  At most one pre-snapshot tick may race in.
cmp_ok($sentinel_post_hold - $sentinel_pre_hold, '<=', 1,
	'L7c sentinel suppressed while node1 held a live snapshot');

write_retry($pair->node0, "UPDATE $tbl SET v = v + 1 WHERE k = 1", 2);
ok(wait_for(sub { floor_local($pair->node0) > $floor_hold_b }, 20),
	'L7b floor resumed after the holder released');

# ============================================================
# L8: wake-up self-heal — node1's first cross-node reads after the
# storm succeed.  A bounded number of retries is allowed: a snapshot
# taken below a just-recycled bound refuses fail-closed
# (BELOW_HORIZON inadmissible) and the wire observe heals the next
# snapshot.  Quiesce the table first (owner-side VACUUM, t/394
# recipe) so low-xid "TT slot recycled" cleanout noise cannot
# masquerade as the mechanism under test.
# ============================================================
# Owner-side cleanout MUST succeed (t/394 recipe: single-writer table, so
# FREEZE is safe): recycled-slot ITL refs left on the heap would otherwise
# fail-close every node1 read with "TT slot recycled" -- the documented
# low-xid artifact, not the mechanism under test.
#
# The cleanout needs node1-mastered GES resources (VM fork / relation
# locks) and the storm can leave node1's master work queue saturated for
# minutes (source=master-reject-queue-full at elapsed_ms=0 -- the S3
# lock-timeout wedge class; structural fix owned by the convert-queue
# lane).  So: try in place first; if the plane stays wedged, bounce node1
# -- the same operational recovery the S3 rig used -- and retry.  Once the
# queue-lane fix lands, the in-place path takes over by itself.  Either
# way the leg HARD-fails if cleanout never succeeds: the wedge is
# surfaced, not papered over.
my $vac_ok = 0;
for my $i (1 .. 6)
{
	my ($rc, $out, $err) = $pair->node0->psql('postgres', "VACUUM (FREEZE) $tbl");
	if ($rc == 0) { $vac_ok = 1; last; }
	usleep(2_500_000);
}
if (!$vac_ok)
{
	diag('in-place cleanout blocked (post-storm master-queue saturation); '
		  . 'bouncing node1');
	$pair->node1->stop('fast');
	$pair->node1->start;
	ok($pair->wait_for_peer_state(0, 1, 'connected', 60)
		  && $pair->wait_for_peer_state(1, 0, 'connected', 60),
		'L8a0 node1 bounced and reconnected');
	for my $i (1 .. 6)
	{
		my ($rc, $out, $err) = $pair->node0->psql('postgres', "VACUUM (FREEZE) $tbl");
		if ($rc == 0) { $vac_ok = 1; last; }
		usleep(2_500_000);
	}

	# Flush the frozen pages to the shared file: a REJOINED node
	# silently reads the on-disk pages of a table whose current
	# versions live only in the peer's buffers -- a PRE-EXISTING
	# silent-empty-read defect reproduced minimally on main WITHOUT
	# this test's storm (probe archived in the lane's evidence bundle;
	# reported separately, owned by the GCS read path).  The
	# checkpoint keeps THIS leg about the floor/wake semantics instead
	# of that defect; drop it once that fix lands.
	my $ckpt_ok = 0;
	for my $i (1 .. 6)
	{
		my ($rc, $out, $err) = $pair->node0->psql('postgres', 'CHECKPOINT');
		if ($rc == 0) { $ckpt_ok = 1; last; }
		usleep(2_500_000);
	}
	ok($ckpt_ok, 'L8a-post node0 checkpoint after cleanout (pre-fix rejoin-read workaround)');
}
else
{
	ok(1, 'L8a0 in-place cleanout path (no node1 bounce needed)');
}
ok($vac_ok, 'L8a owner-side cleanout (VACUUM FREEZE) succeeded');

my ($sum, $tries) = ('', 0);
for my $i (1 .. 10)
{
	$tries = $i;
	my ($rc, $stdout, $stderr) =
	  $pair->node1->psql('postgres', "SELECT count(*), sum(v) >= 0 FROM $tbl");
	if ($rc == 0 && $stdout =~ /^64\|t$/)
	{
		$sum = $stdout;
		last;
	}
	diag(sprintf('L8 attempt %d: rc=%d out=[%s] err=[%s]',
		$i, $rc, $stdout // '', join(' / ', split(/\n/, $stderr // ''))));
	usleep(1_000_000);
}
is($sum, '64|t', "L8 node1 cross-node read healed (tries=$tries)");

# ============================================================
# L10: the GATED wake-up self-heal, for real -- the
# COMMITTED_BELOW_HORIZON resolve arm under
# cluster.crossnode_runtime_visibility = on, with NO owner-side
# cleanout bypass (no FREEZE, no CHECKPOINT).
#
# L8 above only proves the read heals AFTER a VACUUM (FREEZE)
# removed the remote ITL refs -- the rtvis resolve machinery
# (cluster_runtime_visibility.c rtvis_try_origin_verdict) never ran
# there because the GUC defaults off.  This leg drives it as a
# RED->GREEN twin on ONE driver:
#
#  1. cluster.tt_status_hint_emit_mode = disabled (PGC_SIGHUP,
#     default all_status) BEFORE the fixture write: otherwise the
#     V4 hint wire installs the fixture xid's terminal status into
#     node1's TT overlay at commit time and the read resolves from
#     the overlay without ever touching the verdict wire.  This is
#     NOT an assertion-weakening bypass: it deterministically
#     manufactures the "hint never arrived" state the resolve path
#     exists for (the 256-slot outbound ring drops hints under
#     storm load anyway), FORCING the mechanism under test to carry
#     the read.  cluster.crossnode_runtime_visibility stays at its
#     default (off) until the RED twin below has run.
#  2. Fixture: ONE node0 transaction inserts 64 rows -> a single
#     fresh remote ITL ref (local_xid == raw_xid).  With
#     cluster.xid_striping off in this rig, cluster_xid_is_mine()
#     is always false at the origin, so ONLY the spec-5.22f
#     fresh-ref AUTHORITATIVE ask is ever served -- the fixture must
#     stay fresh (single writer xact, page never revisited by
#     node0, so no delayed cleanout stamps it).
#  3. Churn: a node0 writer laps the TT pool (same
#     tt_retention_rollover_count evidence as L2), so the fixture
#     xid's TT slot is provably rebound; the origin's complete
#     by-xid scan then 0-matches and -- retention legs (a)-(d) +
#     CLOG committed -- serves COMMITTED_BELOW_HORIZON{H}.
#  4. RED twin: with the GUC still off, node1's read of the fixture
#     MUST fail closed 53R97 ("cluster TT status unknown",
#     heapam_visibility.c) and node1's verdict-wire counter MUST
#     stay flat (cluster_runtime_visibility.c gates the whole
#     resolve on the GUC before any wire touch).  This proves the
#     recycled-slot window is genuinely open at the moment the
#     GREEN twin runs -- the GREEN result cannot be an overlay /
#     hint / cleanout side door.
#  5. cluster.crossnode_runtime_visibility = on (PGC_SUSET, reload)
#     on BOTH nodes: the requester's classify / fresh-ref widening
#     gates on it AND the origin's lms_undo_verdict_serve refuses
#     while it is off (cluster_cr_server.c).
#  6. GREEN twin: node1 re-reads the same fixture.  A snapshot
#     read_scn behind the shipped horizon takes the inadmissible
#     fail-closed arm and the Lamport observe heals the NEXT
#     snapshot (bounded retries, the documented self-heal); the
#     healed snapshot admits the bound and every row resolves
#     visible.
#
# HARD ASSERTS: RED twin fails closed with the wire flat + exact
# terminal data on the GREEN twin (count=64 AND sum=2080 -- not
# L8's sum >= 0 shape) + node1's rtvis verdict counters moved
# (below_horizon / exact), proving the verdict wire resolved the
# read rather than an overlay / hint / cleanout side door.
#
# Known hazard (documented, not worked around): if L8 had to bounce
# node1, the pre-existing rejoin silent-empty-read defect (see L8
# comment) can surface here as count=0 -- that is a REAL defect this
# leg must fail on, not paper over.
# ============================================================
for my $node ($pair->node0, $pair->node1)
{
	$node->safe_psql('postgres',
		"ALTER SYSTEM SET cluster.tt_status_hint_emit_mode = 'disabled'");
	$node->safe_psql('postgres', 'SELECT pg_reload_conf()');
}
ok( wait_for(
		sub {
			$pair->node0->safe_psql('postgres',
				'SHOW cluster.tt_status_hint_emit_mode') eq 'disabled'
			  && $pair->node1->safe_psql('postgres',
				'SHOW cluster.tt_status_hint_emit_mode') eq 'disabled';
		},
		15),
	'L10a status-hint wire disabled via reload (both nodes)');

# Coinciding-filepath fixture on shared storage (same t/394 recipe as
# L1b; no shared catalog in this rig).
my $l10tbl;
for my $i (1 .. 12)
{
	my $t = "t403l10_$i";
	$_->safe_psql('postgres', "CREATE TABLE $t (k int, v int)")
	  for ($pair->node0, $pair->node1);
	my $p0 = $pair->node0->safe_psql('postgres', "SELECT pg_relation_filepath('$t')");
	my $p1 = $pair->node1->safe_psql('postgres', "SELECT pg_relation_filepath('$t')");
	if (($p0 // '') eq ($p1 // '')) { $l10tbl = $t; last; }
}
ok(defined $l10tbl, 'L10b coinciding-filepath fixture table found');
die 'no coinciding filepath found for L10' unless defined $l10tbl;
diag("L10 fixture table=$l10tbl");

# ONE transaction writes all 64 rows: exact expected terminal data is
# count=64, sum(v)=sum(1..64)=2080, and the page ITL stays a single
# fresh entry bound to this xid.
$pair->node0->safe_psql('postgres',
	"INSERT INTO $l10tbl SELECT g, g FROM generate_series(1, 64) g");

# Churn phase: lap the TT pool on node0 so the fixture xid's slot is
# rebound (>= 2 laps hard floor, target 3; single background writer on
# a node0-only table so the fixture pages are never revisited).
$pair->node0->safe_psql('postgres', 'CREATE TABLE t403l10_churn (k int)');
my $churnfile = "$tmpdir/churn.sql";
{
	open(my $fh, '>', $churnfile) or die "cannot write $churnfile: $!";
	print $fh "INSERT INTO t403l10_churn VALUES (1);\n" for (1 .. $WRITER_LINES);
	close($fh);
}
my $roll_pre_l10 = state_val($pair->node0, 'undo', 'tt_retention_rollover_count');
my ($c_in, $c_out, $c_err) = ('', '', '');
my $churn = IPC::Run::start(
	[
		'psql', '-X', '-A', '-t', '-q',
		'-d', $pair->node0->connstr('postgres'),
		'-f', $churnfile
	],
	\$c_in, \$c_out, \$c_err);
my $churn_t0 = time();
my $roll_delta_l10 = 0;
while (time() - $churn_t0 < 120)
{
	$roll_delta_l10 =
	  state_val($pair->node0, 'undo', 'tt_retention_rollover_count') - $roll_pre_l10;
	last if $roll_delta_l10 >= 3 * $POOL_SEGMENTS;
	usleep(400_000);
}
$churn->kill_kill;
diag(sprintf('L10 churn: %d TT rollovers in %.0fs (hard floor %d)',
	$roll_delta_l10, time() - $churn_t0, 2 * $POOL_SEGMENTS));
cmp_ok($roll_delta_l10, '>=', 2 * $POOL_SEGMENTS,
	'L10c churn lapped the TT pool (fixture slot provably rebound)');

# ------------------------------------------------------------
# RED twin: cluster.crossnode_runtime_visibility is still at its
# default (off).  The same read the GREEN twin will heal below MUST
# fail closed here -- 53R97 "cluster TT status unknown"
# (heapam_visibility.c) -- and node1's verdict-wire counter MUST stay
# flat: cluster_runtime_visibility.c refuses before any wire touch
# while the GUC is off, so a moving counter (or a succeeding read)
# would mean a side door carried it.  Transient non-53R97 errors
# (post-churn GES tail, the L8 class) are retried; a SUCCEEDING read
# is a hard fail, never retried.
# ------------------------------------------------------------
my $wire_pre_neg = state_val($pair->node1, 'cr', 'rtvis_verdict_wire_count');
my ($neg_hit_53r97, $neg_unexpected) = (0, '');
for my $i (1 .. 10)
{
	my ($rc, $stdout, $stderr) =
	  $pair->node1->psql('postgres', "SELECT count(*), sum(v) FROM $l10tbl");
	if ($rc == 0)
	{
		$neg_unexpected = 'read SUCCEEDED with rtvis off: [' . ($stdout // '') . ']';
		last;
	}
	if (($stderr // '') =~ /cluster TT status unknown for xid/)
	{
		$neg_hit_53r97 = 1;
		last;
	}
	$neg_unexpected = join(' / ', split(/\n/, $stderr // ''));
	diag(sprintf('L10d RED-twin attempt %d: rc=%d non-53R97 err=[%s]',
		$i, $rc, $neg_unexpected));
	usleep(1_500_000);
}
ok($neg_hit_53r97,
	'L10d RED twin HARD ASSERT: rtvis off fails the read closed 53R97 '
	  . '(cluster TT status unknown)')
  or diag("L10d terminal state: $neg_unexpected");
is(state_val($pair->node1, 'cr', 'rtvis_verdict_wire_count') - $wire_pre_neg,
	0, 'L10e RED twin: verdict wire provably untouched while the GUC is off');

# GREEN twin arming: flip ONLY the GUC; driver and fixture unchanged.
for my $node ($pair->node0, $pair->node1)
{
	$node->safe_psql('postgres',
		'ALTER SYSTEM SET cluster.crossnode_runtime_visibility = on');
	$node->safe_psql('postgres', 'SELECT pg_reload_conf()');
}
ok( wait_for(
		sub {
			$pair->node0->safe_psql('postgres',
				'SHOW cluster.crossnode_runtime_visibility') eq 'on'
			  && $pair->node1->safe_psql('postgres',
				'SHOW cluster.crossnode_runtime_visibility') eq 'on';
		},
		15),
	'L10f crossnode_runtime_visibility on via reload (both nodes)');

# Requester-side rtvis verdict counters live on node1 (category 'cr').
my $bh_pre    = state_val($pair->node1, 'cr', 'rtvis_verdict_below_horizon_count');
my $ex_pre    = state_val($pair->node1, 'cr', 'rtvis_verdict_exact_count');
my $inadm_pre = state_val($pair->node1, 'cr', 'rtvis_verdict_inadmissible_count');
my $wire_pre  = state_val($pair->node1, 'cr', 'rtvis_verdict_wire_count');

# node1 cross-node read, bounded retries: the inadmissible
# BELOW_HORIZON arm is fail-closed and the wire observe heals the next
# snapshot (plus post-churn GES tail tolerance, as in L8).
my ($l10_out, $l10_tries) = ('', 0);
for my $i (1 .. 15)
{
	$l10_tries = $i;
	my ($rc, $stdout, $stderr) =
	  $pair->node1->psql('postgres', "SELECT count(*), sum(v) FROM $l10tbl");
	if ($rc == 0 && $stdout =~ /^64\|2080$/)
	{
		$l10_out = $stdout;
		last;
	}
	diag(sprintf('L10 attempt %d: rc=%d out=[%s] err=[%s]',
		$i, $rc, $stdout // '', join(' / ', split(/\n/, $stderr // ''))));
	usleep(1_500_000);
}
is($l10_out, '64|2080',
	"L10 HARD ASSERT: node1 healed read returned exact terminal data (tries=$l10_tries)");

my $bh_d    = state_val($pair->node1, 'cr', 'rtvis_verdict_below_horizon_count') - $bh_pre;
my $ex_d    = state_val($pair->node1, 'cr', 'rtvis_verdict_exact_count') - $ex_pre;
my $inadm_d = state_val($pair->node1, 'cr', 'rtvis_verdict_inadmissible_count') - $inadm_pre;
my $wire_d  = state_val($pair->node1, 'cr', 'rtvis_verdict_wire_count') - $wire_pre;
diag("L10 node1 rtvis verdict deltas: wire=$wire_d below_horizon=$bh_d "
	  . "exact=$ex_d inadmissible=$inadm_d");
cmp_ok($bh_d + $ex_d, '>', 0,
	'L10g HARD ASSERT: verdict resolve path really carried the read '
	  . '(below_horizon + exact advanced)');
cmp_ok($bh_d, '>', 0,
	'L10h below-horizon arm served the recycled fixture xid');

# ============================================================
# L9: restart clean.
# ============================================================
$pair->node0->stop('fast');
$pair->node1->stop('fast');
$pair->node0->start;
$pair->node1->start;
usleep(3_000_000);
ok($pair->wait_for_peer_state(0, 1, 'connected', 60)
	  && $pair->wait_for_peer_state(1, 0, 'connected', 60),
	'L9 cluster restarted after both legs');
my $restart_count = '';
for my $i (1 .. 5)
{
	my ($rc, $stdout, $stderr) =
	  $pair->node0->psql('postgres', "SELECT count(*) FROM $tbl");
	if ($rc == 0) { $restart_count = $stdout; last; }
	usleep(1_000_000);
}
is($restart_count, '64', 'L9 table readable after restart');

done_testing();
