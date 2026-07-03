#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 284_cf_clean_page_xfer.pl
#    spec-5.2a -- CF clean-page X-transfer enabler (the dedicated, reliable
#    cross-node X ownership transfer for clean / no-MVCC-state pages; the first
#    consumer is the shared sequence page exercised by PG-native nextval).
#
#    This TAP is the ship form of the spec-5.4 D0-B viability probe
#    (t/900_d0b_seq_probe.pl):  D0-B showed PG-native cross-node nextval failing
#    deterministically ("could not obtain X transfer from X holder 0") because
#    the CF writer-transfer was unreliable for clean pages.  The enabler makes it
#    work: requester arms the clean transfer (D5), the master takes the dedicated
#    clean path (D3), the holder eager-flushes data before dropping (D4), and a
#    stale holder FAILS CLOSED 53R9X (Q3 amended 2026-06-21 -- storage-fallback is
#    unsound on Stage-5 non-cross-instance-coherent shared storage).
#
#    Legs (real 2-node, no SKIP for the core -- L77 / spec-5.2 ship-gate
#    precedent):
#      L1  pair alive + connected + same-relfilenode sequence + the 5 clean-page
#          xfer counters present in the gcs dump and 0 at a fresh start.
#      L2  node0 nextval -> node1 nextval succeeds and is unique (the exact
#          D0-B failure now passes).
#      L3  strict interleave node0/node1 (CACHE 1) -> NO duplicates across many
#          rounds + clean_page_xfer_count grows (every cross-node refill is a
#          real clean X transfer).
#      L4  dual-master: several sequences (distinct relfilenodes -> distinct GCS
#          masters) each interleaved cross-node -> all unique (covers both the
#          master==holder path-B self-ship AND the master==requester
#          local-master transfer).
#      L5  stale-holder FAILS CLOSED (faithful injection, 8.A; Q3 amended): a
#          faithful, production-reachable stale holder (shipped + eager-flushed +
#          drop_no_wire, then the dropped holder DENIES the next forward) makes
#          the master hit the clean-page stale-holder path, which MUST fail closed
#          53R9X -- NOT storage-fallback (Stage-5 shared storage is not cross-
#          instance coherent, so a storage read would reissue a value).  Asserts:
#          fail_closed_count grows, the sequence stays UNIQUE (no stale reissue),
#          and it self-heals via the normal CF image-ship path.  L5b then restarts
#          the pair to clear the LMON-side evict injection for the remaining legs.
#      L6  restart-durable: node0 burns a node0-only sequence, CHECKPOINTs, crashes
#          (immediate), restarts; node0 nextval is strictly greater than every
#          value it handed out (the D4 eager-flush + force-WAL-log made the value
#          durable across the restart -- inv③).
#      L7  heap zero-impact: a heap UPDATE workload does NOT bump any clean-page
#          xfer counter (the eligibility flag is one-shot + sequence-only; it
#          never leaks into a heap access -- R3 / inv①).
#      L8  fail-closed is wired (report-only in 2-node): third-party-master DENY
#          (53R9X) needs >=3 nodes -- the 2-node target always has master in
#          {requester, holder}; the decision + 53R9X mapping are unit-proven
#          (U3).  The third_party_denied counter is reported (0 expected here).
#      L9  perf (report-only): PG-native cross-node nextval throughput baseline
#          for the spec-5.4 instance-cache comparison.
#
#    Harness: ClusterPair shared_data + 3 voting disks + autovacuum off.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/284_cf_clean_page_xfer.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-5.2a-cf-clean-page-x-transfer.md
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(usleep gettimeofday tv_interval);

# Sum a gcs-category clean-page counter across both nodes.
sub xfer_count
{
	my ($pair, $key) = @_;
	my $sum = 0;
	for my $node ($pair->node0, $pair->node1)
	{
		my $v = $node->safe_psql(
			'postgres', qq{
			SELECT coalesce(value::bigint, 0) FROM pg_cluster_state
			WHERE category = 'gcs' AND key = '$key'});
		$sum += ($v // 0);
	}
	return $sum;
}

sub diag_xfer_state
{
	my ($pair, $label) = @_;
	for my $i (0, 1)
	{
		my $node = $i == 0 ? $pair->node0 : $pair->node1;
		my $rows = $node->safe_psql(
			'postgres', q{
			SELECT string_agg(key || '=' || value, ' ' ORDER BY key)
			FROM pg_cluster_state
			WHERE category = 'gcs' AND key LIKE 'clean_page_xfer%' AND value <> '0'});
		diag("  [$label] node$i " . ($rows // '(all clean_page_xfer counters 0)'));
	}
}

# Cross-node nextval with bounded retry on the documented-RETRYABLE fail-closed.
# The 5.2a enabler's contract is fail-closed + retryable: under sustained
# cross-node contention the X holder may miss its ship budget ("could not obtain
# X transfer ... did not ship a current image in time; retry") -- a transient,
# never a duplicate (8.A).  A real application retries on this signal; the
# strict-uniqueness legs do the same, so they assert correctness (no dups,
# counter grows) without flaking on the slow-enabler ship-timeout (which 5.4's
# instance cache removes by cutting the cross-node round-trips).  Dies only on a
# NON-retryable error or after exhausting the bounded retries.
sub cnext
{
	my ($node, $seq) = @_;
	my $last_err = '';
	for my $attempt (1 .. 15)
	{
		my ($rc, $out, $err) =
		  $node->psql('postgres', "SELECT nextval('$seq')", timeout => 30);
		return $out if defined $rc && $rc == 0 && defined $out && $out ne '';
		$last_err = $err // '(no stderr)';
		die "cnext($seq): non-retryable error: $last_err"
		  unless $last_err =~ /could not obtain X transfer/
		  || $last_err =~ /did not ship a current image/
		  || $last_err =~ /retry the transaction/;
		usleep(200_000);
	}
	die "cnext($seq): exhausted retries under contention; last=$last_err";
}

# Retry a post-restart nextval through documented fail-closed, quorum, and
# connection readiness windows, but still require a successful value before
# checking durability.
sub restart_nextval
{
	my ($node, $seq) = @_;
	my $last_err = '';
	for my $attempt (1 .. 60)
	{
		my ($rc, $out, $err);
		my $ran = eval {
			($rc, $out, $err) =
			  $node->psql('postgres', "SELECT nextval('$seq')", timeout => 10);
			1;
		};
		if ($ran && defined $rc && $rc == 0 && defined $out && $out ne '')
		{
			return (1, $out, '');
		}

		$last_err = $ran ? ($err // '(no stderr)') : ($@ // '(psql died)');
		last
		  unless $last_err =~ /could not obtain X transfer/
		  || $last_err =~ /did not ship a current image/
		  || $last_err =~ /retry the transaction/
		  || $last_err =~ /cluster TT status unknown/
		  || $last_err =~ /Remote commit_scn not yet propagated/
		  || $last_err =~ /cluster quorum lost or uncertain/
		  || $last_err =~ /transaction aborted: cluster quorum lost/
		  || $last_err =~ /database system is starting up/
		  || $last_err =~ /server closed the connection unexpectedly/
		  || $last_err =~ /could not connect to server/
		  || $last_err =~ /Connection refused/
		  || $last_err =~ /terminating connection due to administrator command/;
		usleep(500_000);
	}
	return (0, undef, $last_err);
}

# ----------
# L1: strict pair + shared data + same-DDL (same relfilenode) + counter baseline.
# ----------
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'cf_clean_xfer',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.grd_max_entries = 1024',
		# Widen CSSD misscount so a parallel CI shard does not falsely declare a
		# healthy peer DEAD while an X-hold window is open (mirrors t/280-283).
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
$n0->safe_psql('postgres', 'CREATE SEQUENCE s');    # default CACHE 1
$n1->safe_psql('postgres', 'CREATE SEQUENCE s');
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

# All 5 spec-5.2a D6 counters must be present in the dump (regression guard).
my @ctr_keys = qw(
  clean_page_xfer_count
  clean_page_xfer_storage_fallback_count
  clean_page_xfer_fail_closed_count
  clean_page_xfer_stale_holder_recover_count
  clean_page_xfer_third_party_denied_count
);
my $present = $n0->safe_psql('postgres', q{
	SELECT count(*) FROM pg_cluster_state
	WHERE category = 'gcs' AND key LIKE 'clean_page_xfer%'});
is($present, scalar(@ctr_keys), 'L1: all 5 clean_page_xfer counters present in gcs dump');
is(xfer_count($pair, 'clean_page_xfer_count'), 0, 'L1: clean_page_xfer_count baseline 0');

# ----------
# L2: node0 nextval -> node1 nextval succeeds + unique (the D0-B failure fixed).
# ----------
# L2 proves the exact D0-B failure ("could not obtain X transfer" EVERY time) is
# fixed: a cross-node nextval now SUCCEEDS (within the retryable contract) and is
# unique.  cnext retries only the documented-retryable transient -- the D0-B
# regression was deterministic, so a success at all is the proof.
my $v0   = $n0->safe_psql('postgres', "SELECT nextval('s')");
my $out1 = cnext($n1, 's');
ok(defined $out1 && $out1 ne '', 'L2: node1 cross-node nextval succeeds (D0-B failure fixed)');
ok(defined $out1 && $out1 ne $v0, "L2: node1 nextval ($out1) is unique vs node0 ($v0)");

# ----------
# L3: strict interleave node0/node1 (CACHE 1) -> no dups + counter grows.
# ----------
my $before_xfer = xfer_count($pair, 'clean_page_xfer_count');
my @vals;
for my $i (1 .. 20)
{
	push @vals, cnext($n0, 's');
	push @vals, cnext($n1, 's');
}
my %seen;
my @dups = grep { $seen{$_}++ } @vals;
diag("L3 interleaved values: @vals") if @dups;
is(scalar(@dups), 0, 'L3: interleaved cross-node nextval has NO duplicates (single X owner)');
my $after_xfer = xfer_count($pair, 'clean_page_xfer_count');
diag_xfer_state($pair, 'after L3');
cmp_ok($after_xfer, '>', $before_xfer,
	"L3: clean_page_xfer_count grew ($before_xfer -> $after_xfer) -- real cross-node X transfers");

# ----------
# L4: dual-master -- several sequences (distinct relfilenodes/masters) interleaved.
# ----------
my $l4_ok = 1;
for my $sname (qw(sm1 sm2 sm3 sm4 sm5 sm6))
{
	$n0->safe_psql('postgres', "CREATE SEQUENCE $sname");
	$n1->safe_psql('postgres', "CREATE SEQUENCE $sname");
	my @sv;
	for my $i (1 .. 6)
	{
		push @sv, cnext($n0, $sname);
		push @sv, cnext($n1, $sname);
	}
	my %s2;
	my @d2 = grep { $s2{$_}++ } @sv;
	if (@d2) { $l4_ok = 0; diag("L4 $sname dup values: @sv"); }
}
ok($l4_ok, 'L4: dual-master -- 6 sequences (distinct masters) interleave with NO duplicates');


# ----------
# L5: clean-page stale holder FAILS CLOSED (faithful injection, 8.A).  Q3 amended
#     2026-06-21: storage-fallback is NOT sound on Stage-5 shared storage (not
#     cross-instance coherent -> a recovering node's storage read returns a stale
#     view and REISSUES sequence values).  So a clean-page stale holder must fail
#     closed (53R9X retryable), NOT storage-fallback.  This leg constructs a
#     FAITHFUL, production-reachable stale holder and asserts: it fails closed
#     (clean_page_xfer_fail_closed_count grows), and the sequence stays correct
#     afterwards (the normal CF image-ship path keeps issuing UNIQUE values -- no
#     duplicate from a stale storage read).
# ----------
$n0->safe_psql('postgres', 'CREATE SEQUENCE sseq');
$n1->safe_psql('postgres', 'CREATE SEQUENCE sseq');
# Warm up: establish a cross-node holder + flush the page to shared storage.
$n0->safe_psql('postgres', "SELECT nextval('sseq')");
$n1->safe_psql('postgres', "SELECT nextval('sseq')");
$n0->safe_psql('postgres', "SELECT nextval('sseq')");

my $fc_before = xfer_count($pair, 'clean_page_xfer_fail_closed_count');
my @l5vals;

# Set the named injection list on BOTH nodes (new backends auto-arm at startup;
# the holder-side evict fires in the long-running LMON, re-armed on reload).
sub set_inject
{
	my ($val) = @_;
	for my $node ($n0, $n1)
	{
		$node->safe_psql('postgres', "ALTER SYSTEM SET cluster.injection_points = '$val'");
		$node->safe_psql('postgres', 'SELECT pg_reload_conf()');
	}
	return;
}
# Run nextval tolerantly (the install-skip phase errors by design) and keep the
# value when it succeeds (no value may ever duplicate -- 8.A).
sub tnext
{
	my ($node) = @_;
	my ($rc, $o) = $node->psql('postgres', "SELECT nextval('sseq')", timeout => 30);
	push @l5vals, $o if defined $rc && $rc == 0 && defined $o && $o ne '';
	return;
}

# We do not know which node is sseq's GCS master, so try both (holder, master)
# assignments.  Each try constructs a FAITHFUL, production-reachable stale holder:
#   phase 1 (install-skip armed): the master's local_master_x_transfer makes the
#     holder ship + (already eager-)flush + drop_no_wire its copy (holder X
#     released, pcm_state = N) but the master skips the install, so the GRD still
#     records the now-N holder -- the real F0-4 window (a checksum mismatch /
#     interrupt would do the same in production).
#   phase 2 (evict-before-ship armed): the holder's next forward replies
#     DENIED_MASTER_NOT_HOLDER -- modelling "the dropped holder no longer has a
#     valid buffer to ship" (a real cache-eviction / buffer-gone boundary) -- so
#     the master hits the clean-page stale-holder path, which must FAIL CLOSED
#     (53R9X), never read the non-coherent storage.
for my $hm ([ $n0, $n1 ], [ $n1, $n0 ])
{
	last if xfer_count($pair, 'clean_page_xfer_fail_closed_count') > $fc_before;
	my ($holder, $master) = @$hm;

	tnext($holder);    # holder takes X (and eager-flushes the page to storage)
	set_inject('cluster-clean-xfer-stale-holder:skip');
	tnext($master);    # if master: install-skip -> holder drops X (pcm_state=N)
	set_inject('cluster-gcs-block-evict-holder-before-ship:skip');
	tnext($master);    # if master: holder DENIED -> stale-holder FAIL CLOSED 53R9X
	set_inject('');
}
# After the stale-holder fail-closed, the master keeps failing closed (53R9X)
# until the sequence self-heals through the normal CF image-ship path (the
# holder re-acquires X from its own current buffer).  Drive TOLERANT rounds:
# the 53R9X failures are expected; collect every value that DOES succeed -- none
# may ever duplicate (a stale storage read would have reissued one).
my $heal_ok = 0;
for my $i (1 .. 16)
{
	my ($r0, $o0) = $n0->psql('postgres', "SELECT nextval('sseq')", timeout => 30);
	if (defined $r0 && $r0 == 0 && defined $o0 && $o0 ne '') { push @l5vals, $o0; $heal_ok = 1; }
	my ($r1, $o1) = $n1->psql('postgres', "SELECT nextval('sseq')", timeout => 30);
	if (defined $r1 && $r1 == 0 && defined $o1 && $o1 ne '') { push @l5vals, $o1; $heal_ok = 1; }
}
my $fc_after = xfer_count($pair, 'clean_page_xfer_fail_closed_count');
my %l5seen;
my @l5dups = grep { $l5seen{$_}++ } @l5vals;
diag("L5 fail_closed $fc_before->$fc_after; values: @l5vals");
cmp_ok($fc_after, '>', $fc_before,
	'L5: clean-page stale holder FAILED CLOSED (53R9X) -- not an unsound storage-fallback');
is(scalar(@l5dups), 0,
	'L5: sequence stays UNIQUE through a stale-holder fail-closed (no stale storage reissue) -- 8.A');
ok($heal_ok,
	'L5: sequence self-heals via the normal CF image-ship path after the fail-closed (still usable)');

# ----------
# Restart both nodes to clear the LMON-side evict injection armed in L5: a
# GUC-armed SKIP fault is NOT disarmed by a GUC reload (assign_hook only
# disarms WARNING faults), but a fresh process reads the now-empty
# cluster.injection_points from auto.conf and comes up injection-free.  The
# remaining legs then run on a clean cluster.
# ----------
$pair->node0->stop('fast');
$pair->node1->stop('fast');
$pair->node0->start;
$pair->node1->start;
usleep(3_000_000);
ok($pair->wait_for_peer_state(0, 1, 'connected', 60)
	  && $pair->wait_for_peer_state(1, 0, 'connected', 60),
	'L5b: cluster restarted injection-free for the remaining legs');

# ----------
# L7: heap zero-impact -- a heap workload must NOT bump any clean-page xfer
#     counter (eligibility is one-shot + sequence-only; never leaks to heap, R3).
# ----------
# A plain heap (no index -> no shared-storage duplicate-index-build collision).
# Create it on BOTH nodes so the shared-nothing per-node OID counters stay in
# lockstep -- a single-node CREATE would advance node0's relfilenode allocator
# only, so a later cross-node CREATE SEQUENCE would land on different
# relfilenodes per node (one of which collides with a non-sequence file, which
# the D5a open-or-init guard correctly fails closed on).  The workload itself is
# node0-only: the point is that the heap path never arms the clean-page
# eligibility, so no counter moves.
$n0->safe_psql('postgres', 'CREATE TABLE h (id int, v int)');
$n1->safe_psql('postgres', 'CREATE TABLE h (id int, v int)');
my %heap_before = map { $_ => xfer_count($pair, $_) } @ctr_keys;
$n0->safe_psql('postgres', 'INSERT INTO h SELECT g, g FROM generate_series(1,50) g');
$n0->safe_psql('postgres', 'UPDATE h SET v = v + 1 WHERE id <= 25');
$n0->safe_psql('postgres', 'SELECT count(*) FROM h');
my $heap_leaked = 0;
for my $k (@ctr_keys)
{
	my $now = xfer_count($pair, $k);
	if ($now != $heap_before{$k})
	{
		$heap_leaked = 1;
		diag("L7 LEAK: $k $heap_before{$k} -> $now after heap workload");
	}
}
ok(!$heap_leaked,
	'L7: heap INSERT/UPDATE/SELECT bumped NO clean-page xfer counter (no eligibility leak)');

# ----------
# L8: fail-closed wired (report-only in 2-node).  Third-party-master DENY (53R9X)
#     needs >=3 nodes; the decision + 53R9X mapping are unit-proven (U3).
# ----------
is(xfer_count($pair, 'clean_page_xfer_third_party_denied_count'), 0,
	'L8: no third-party-master DENY in the 2-node topology (master always in {req,holder})');
diag(  "L8 (report-only) fail_closed_count="
	 . xfer_count($pair, 'clean_page_xfer_fail_closed_count')
	 . "  -- 53R9X third-party path is U3-proven; >=3-node e2e is forward");

# ----------
# L9: perf (report-only) -- PG-native cross-node nextval throughput baseline on
#     a FRESH sequence.  NOTE: this is the deliberately-slow enabler path (every
#     cross-node refill is a full CF X-transfer + a forced WAL-log + an eager
#     storage flush); spec-5.4's instance cache + SQ serialization is what makes
#     it fast.  Sustained very-heavy cross-node sequence access also stresses
#     the GES relation-lock layer beneath the CF page transfer (a separate layer
#     that 5.4's cache largely removes by cutting cross-node round-trips), so we
#     keep the baseline sample modest here.
# ----------
$n0->safe_psql('postgres', 'CREATE SEQUENCE pseq');
$n1->safe_psql('postgres', 'CREATE SEQUENCE pseq');
# Tolerant single-round sample: the enabler path is intentionally slow and, late
# in a heavy run, sustained cross-node sequence access can hit the GES relation-
# lock contention noted above.  Use psql (not safe_psql) so a slow/contended
# round-trip is REPORTED, not fatal -- this leg is a perf observation, not a
# correctness gate (the correctness gates are L2-L6).
my $pt0 = [gettimeofday];
my ($prc0) = $n0->psql('postgres', "SELECT nextval('pseq')", timeout => 30);
my ($prc1) = $n1->psql('postgres', "SELECT nextval('pseq')", timeout => 30);
my $pelapsed = tv_interval($pt0);
diag(sprintf(
	"L9 (report-only): 2 cross-node nextval round-trips in %.3fs (node0 rc=%s, node1 rc=%s) -- "
	  . "deliberately-slow enabler path; spec-5.4 instance cache is the perf story",
	$pelapsed, $prc0 // '?', $prc1 // '?'));
ok(1, 'L9: cross-node nextval perf baseline recorded (report-only)');

# ----------
# L6: restart-durable -- node0 burns a FRESH sequence (node0 stays the sole X
#     holder), CHECKPOINTs so the D4-eager-flushed page is on shared storage, then
#     crashes (immediate shutdown) and restarts; node0 must continue strictly past
#     every value it handed out (inv③: the cluster sequence's value is durable
#     across a node restart -- the D4 eager flush + force-WAL-log is what persists
#     it).  dseq is node0-only, so every burn and the post-crash read is
#     node0-local (no cross-node X transfer); skipping node1's CREATE diverges the
#     per-node OID counters by one, harmless because L6 is the final leg.
#
#     SCOPE: a CHECKPOINT precedes the crash on purpose.  This exercises 5.2a's
#     actual durability guarantee (eager flush + force WAL log => value on shared
#     storage) without driving the single-stream startup-redo loop over a
#     REMOTE-mastered user block.  That redo path is a PRE-EXISTING recovery gap
#     (the startup process has no backend slot, MyBackendId = InvalidBackendId, so
#     a remote PCM acquire during single-stream redo hits the gcs_block reserve-
#     slot guard and FATALs; the merged-replay path disables PCM via the recmerge
#     window, the single-stream path never does -- introduced long before Stage 5,
#     gcs_block.c:382 by spec-2.33, unchanged by 5.2a).  It is registered for a
#     Stage-4 recovery hardening spec (disable PCM acquire while
#     MyBackendId == InvalidBackendId / in startup redo), NOT this CF enabler.
# ----------
$n0->safe_psql('postgres', 'CREATE SEQUENCE dseq');
my $max_issued = 0;
for my $i (1 .. 10)
{
	my $v = $n0->safe_psql('postgres', "SELECT nextval('dseq')");
	$max_issued = $v if defined $v && $v > $max_issued;
}
diag("L6 node0 issued up to last_value=$max_issued before crash");
$n0->safe_psql('postgres', 'CHECKPOINT');
$pair->node0->stop('immediate');
$pair->node0->start;
ok($pair->wait_for_peer_state(1, 0, 'connected', 60), 'L6: node0 rejoined after crash-restart');
my ($after_ok, $after_crash, $after_err) = restart_nextval($n0, 'dseq');
ok($after_ok, 'L6: node0 dseq nextval succeeds after restart')
  or diag("L6 post-crash nextval did not succeed: $after_err");
if ($after_ok)
{
	cmp_ok($after_crash, '>', $max_issued,
		"L6: post-crash node0 nextval ($after_crash) > pre-crash max issued ($max_issued) -- durable across restart");
}
else
{
	fail(
		"L6: post-crash node0 nextval is available for durability comparison -- pre-crash max issued ($max_issued)");
}

$pair->stop_pair;
done_testing();
