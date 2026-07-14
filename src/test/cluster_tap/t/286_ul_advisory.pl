#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 286_ul_advisory.pl
#    spec-5.5 -- UL user lock: cross-node advisory (user) lock.
#
#    The headline feature-078 gap: session-scoped pg_advisory_lock(key) was
#    short-circuited PG-native (HC11), so two nodes never excluded each other
#    -- cross-node advisory mutual exclusion silently failed.  This spec lifts
#    LOCKTAG_ADVISORY through the existing gate onto the spec-5.3 GES enqueue
#    acquire/release substrate, with session lifetime reusing PG's native
#    session lock-owner, and a real conditional GES REQUEST_NOWAIT for try-locks.
#
#    Legs (real 2-node, no SKIP for the core -- L77):
#      L1  pair alive + connected + the 5 advisory counters present and 0.
#      L2  session same-key X mutual exclusion (THE core gap): node0 holds
#          pg_advisory_lock(42) -> node1 pg_try_advisory_lock(42) is FALSE;
#          after node0 unlocks, node1's try succeeds.
#      L3  S/S compatible + S/X conflict.
#      L4  cross-xact survival of a session lock across BEGIN/COMMIT/ROLLBACK.
#      L5  unlock_all drains every key (no double-release).
#      L6  backend-exit drain: a holder that disconnects WITHOUT unlocking still
#          releases cross-node (the :2476 fix).
#      L7  reentrant: lock x3 / unlock x2 stays held; the 3rd unlock releases.
#      L8  try returns FALSE (never ERROR) + advisory_try_notavail counter grows.
#      L9  xact-advisory regression: still excludes cross-node + auto-releases.
#      L10 fail-closed mutual exclusion is wired (failclosed counter present);
#          53R80 itself is unit-proven (U5) -- a healthy pair keeps LMS ready.
#      L11 blocking advisory waits are bounded by a finite ges_request_timeout_ms
#          (53R70) -- what bounds a cross-node deadlock; real detection fwd 5.8.
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
#    src/test/cluster_tap/t/286_ul_advisory.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-5.5-ul-advisory-lock-cross-node.md
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(usleep);

my ($pair, $n0, $n1);

# Sum an 'advisory'-category counter across both nodes.
sub adv_count
{
	my ($key) = @_;
	my $sum = 0;
	for my $node ($n0, $n1)
	{
		my $v = $node->safe_psql(
			'postgres', qq{
			SELECT coalesce(value::bigint, 0) FROM pg_cluster_state
			WHERE category = 'advisory' AND key = '$key'});
		$sum += ($v // 0);
	}
	return $sum;
}

# A fresh-session try-lock: the backend exits when safe_psql returns, so a
# successful try is auto-released (session-scoped) -- no explicit unlock needed.
sub try_lock
{
	my ($node, $expr) = @_;
	return $node->safe_psql('postgres', "SELECT $expr");
}

# Poll a fresh-session try-lock until it succeeds ('t') or we time out.  Used
# after a release to tolerate the small GES release-propagation window.
sub wait_until_acquirable
{
	my ($node, $expr, $secs) = @_;
	my $deadline = time() + $secs;
	while (time() < $deadline)
	{
		my $v = try_lock($node, $expr);
		return 1 if defined $v && $v =~ /^t/;
		usleep(200_000);
	}
	return 0;
}

# Count backends on $node actively running a blocking pg_advisory_lock() acquire
# (state='active').  Once the GES grant arrives, the statement completes and the
# session goes idle, so the count drops to 0.
sub n_blocked_advisory
{
	my ($node) = @_;
	my $n = $node->safe_psql(
		'postgres', q{
		SELECT count(*) FROM pg_stat_activity
		WHERE query LIKE '%pg_advisory_lock(%' AND state = 'active'
		  AND pid <> pg_backend_pid()});
	return defined $n ? int($n) : 0;
}

# spec-5.5 P0 regression guard: a blocking pg_advisory_lock() waiter MUST be
# granted (woken) after the holder releases — including when the resource master
# is the releasing node (local-master release must drain+wake, not just delete
# the holder).  Driven for one key in BOTH directions: the key has exactly one
# master, so node0-releases-while-node1-waits and node1-releases-while-node0-waits
# deterministically cover the local-master AND remote-master release paths.
sub blocking_grant_after_release
{
	my ($holder, $waiter, $key, $desc) = @_;

	my $h = $holder->background_psql('postgres', on_error_die => 1);
	$h->query_safe("SELECT pg_advisory_lock($key)");    # holder holds the key

	my $w = $waiter->background_psql('postgres', on_error_die => 1);
	# Bounded GES wait so a regression fails (rather than hanging the whole suite).
	$w->query_safe("SET cluster.ges_request_timeout_ms = '25s'");
	# Fire the blocking acquire + a TRAILING marker WITHOUT waiting for the grant:
	# the leading \echo returns immediately, the SELECT blocks, and PGRAC_GRANTED is
	# queued so psql only prints it once the SELECT returns (i.e. once granted).
	$w->query_until(qr/PGRAC_FIRED/,
		"\\echo PGRAC_FIRED\nSELECT pg_advisory_lock($key);\n\\echo PGRAC_GRANTED\n");

	my $blocked = 0;
	for (1 .. 50) { if (n_blocked_advisory($waiter) >= 1) { $blocked = 1; last; } usleep(100_000); }
	ok($blocked, "$desc: waiter blocks on held key $key");

	$h->query_safe("SELECT pg_advisory_unlock($key)");    # release -> waiter MUST be woken

	# Wait for the acquire to complete by consuming the queued marker.  This both
	# proves the grant (PGRAC_GRANTED only prints after the blocking SELECT returns)
	# AND re-syncs the psql stream (consumes the SELECT's own output) so the unlock
	# below reads its OWN result.  A stranded waiter never prints it -> the SET'd
	# 25s GES timeout ERRORs the acquire (on_error_die) -> this fails, not hangs.
	$w->query_until(qr/PGRAC_GRANTED/, "");
	ok(1, "$desc: blocking waiter GRANTED after release (no false 53R70 / hang)");

	# Prove the waiter actually HELD the lock: a granted waiter unlocks 't'.  This
	# distinguishes a real grant from a statement ERROR / session exit (which would
	# also clear pg_stat_activity to inactive).
	is($w->query_safe("SELECT pg_advisory_unlock($key)"),
		't', "$desc: waiter actually held the lock (unlock returns t)");

	$w->quit;
	$h->quit;
}

# ----------
# L1: strict pair + shared data + advisory counter baseline.
# ----------
$pair = PostgreSQL::Test::ClusterPair->new_pair(
	'ul_advisory',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.grd_max_entries = 1024',
		# Widen CSSD misscount so a parallel CI shard does not falsely declare a
		# healthy peer DEAD while a session-lock hold window is open (mirror t/284).
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
	]);
$pair->start_pair;
usleep(3_000_000);

$n0 = $pair->node0;
$n1 = $pair->node1;

# Prerequisites: skip_all MUST precede any test output.
my $alive0 = $n0->safe_psql('postgres', 'SELECT 1');
my $alive1 = $n1->safe_psql('postgres', 'SELECT 1');
if (($alive0 // '') ne '1' || ($alive1 // '') ne '1')
{
	$pair->stop_pair;
	plan skip_all => "cluster pair prerequisites not met (alive0=$alive0 alive1=$alive1)";
}

ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'L1: node0 sees node1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'L1: node1 sees node0 connected');

is( $n0->safe_psql('postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category = 'advisory'}),
	'5', 'L1: all 5 advisory counters present in pg_cluster_state');

# ----------
# L2: session same-key X mutual exclusion (THE core gap fix).
# ----------
my $h0 = $n0->background_psql('postgres', on_error_die => 1);
$h0->query_safe("SELECT pg_advisory_lock(42)");    # node0 holds session X(42)

is(try_lock($n1, 'pg_try_advisory_lock(42)'),
	'f', 'L2: node1 try(42) FALSE while node0 holds it (cross-node mutex -- the core gap)');

$h0->query_safe("SELECT pg_advisory_unlock(42)");
ok(wait_until_acquirable($n1, 'pg_try_advisory_lock(42)', 10),
	'L2: node1 acquires 42 after node0 unlocks');

# ----------
# L3: S/S compatible + S/X conflict.
# ----------
my $hs0 = $n0->background_psql('postgres', on_error_die => 1);
my $hs1 = $n1->background_psql('postgres', on_error_die => 1);
$hs0->query_safe("SELECT pg_advisory_lock_shared(7)");
$hs1->query_safe("SELECT pg_advisory_lock_shared(7)");    # S/S compatible -- both succeed
ok(1, 'L3: both nodes hold pg_advisory_lock_shared(7) (S/S compatible)');
is(try_lock($n1, 'pg_try_advisory_lock(7)'),
	'f', 'L3: node1 try X(7) FALSE while shares are held (S/X conflict)');
$hs0->query_safe("SELECT pg_advisory_unlock_shared(7)");
$hs1->query_safe("SELECT pg_advisory_unlock_shared(7)");
$hs0->quit;
$hs1->quit;

# ----------
# L3b: advisory S and X are INDEPENDENT additive holds -- a session holding
# S(K) can try-acquire X(K) on the same key and it returns IMMEDIATELY (true,
# self-exclusion at the master), never routing through a blocking GES convert
# (§3.8 MO2 / Q10).  If the convert router mis-handled this, query_safe would
# block on the X try until the GES timeout instead of returning at once.
# ----------
my $hsx = $n0->background_psql('postgres', on_error_die => 1);
$hsx->query_safe("SELECT pg_advisory_lock_shared(77)");          # hold S(77)
my $sx = $hsx->query_safe("SELECT pg_try_advisory_lock(77)");    # try X(77), same session
like($sx, qr/^t/,
	'L3b: same-session S(77) + try X(77) returns true immediately (no blocking convert)');
$hsx->query_safe("SELECT pg_advisory_unlock(77)");
$hsx->query_safe("SELECT pg_advisory_unlock_shared(77)");
$hsx->quit;

# ----------
# L4: cross-xact survival.
# ----------
my $h4 = $n0->background_psql('postgres', on_error_die => 1);
$h4->query_safe("SELECT pg_advisory_lock(9)");    # session lock, outside any xact
$h4->query_safe("BEGIN");
$h4->query_safe("SELECT 1");
$h4->query_safe("COMMIT");                          # must survive COMMIT
$h4->query_safe("BEGIN");
$h4->query_safe("ROLLBACK");                        # ... and ROLLBACK
is(try_lock($n1, 'pg_try_advisory_lock(9)'),
	'f', 'L4: session lock(9) survives BEGIN/COMMIT/ROLLBACK (still held cross-node)');
$h4->query_safe("SELECT pg_advisory_unlock(9)");
ok(wait_until_acquirable($n1, 'pg_try_advisory_lock(9)', 10),
	'L4: node1 acquires after node0 finally unlocks');
$h4->quit;

# ----------
# L5: unlock_all drains every key (no double-release).
# ----------
my $h5 = $n0->background_psql('postgres', on_error_die => 1);
$h5->query_safe("SELECT pg_advisory_lock(101)");
$h5->query_safe("SELECT pg_advisory_lock(102)");
$h5->query_safe("SELECT pg_advisory_lock(103)");
$h5->query_safe("SELECT pg_advisory_unlock_all()");
ok( wait_until_acquirable(
		$n1, 'pg_try_advisory_lock(101) AND pg_try_advisory_lock(102) AND pg_try_advisory_lock(103)',
		10),
	'L5: unlock_all drained 101/102/103 cross-node (node1 acquires all)');
$h5->quit;

# ----------
# L6: backend-exit drain (the :2476 sessionLock fix).
# ----------
my $h6 = $n0->background_psql('postgres', on_error_die => 1);
$h6->query_safe("SELECT pg_advisory_lock(55)");    # node0 holds, will NOT unlock
$h6->quit;                                          # backend exits WITHOUT unlock
ok(wait_until_acquirable($n1, 'pg_try_advisory_lock(55)', 15),
	'L6: backend-exit drained session lock(55) cross-node (no orphan holder)');

# ----------
# L7: reentrant (only the 0->1 / 1->0 edges hit the wire).
# ----------
my $h7 = $n0->background_psql('postgres', on_error_die => 1);
$h7->query_safe("SELECT pg_advisory_lock(5)");
$h7->query_safe("SELECT pg_advisory_lock(5)");
$h7->query_safe("SELECT pg_advisory_lock(5)");      # refcount 3
$h7->query_safe("SELECT pg_advisory_unlock(5)");
$h7->query_safe("SELECT pg_advisory_unlock(5)");    # refcount 1 -- still held
is(try_lock($n1, 'pg_try_advisory_lock(5)'),
	'f', 'L7: reentrant lock still held cross-node after 2 of 3 unlocks');
$h7->query_safe("SELECT pg_advisory_unlock(5)");    # refcount 0 -- released
ok(wait_until_acquirable($n1, 'pg_try_advisory_lock(5)', 10),
	'L7: released after the final (1->0) unlock');
$h7->quit;

# ----------
# L8: try returns FALSE (never ERROR) + the try_notavail counter grows.
# ----------
my $pre_notavail = adv_count('advisory_try_notavail_count');
my $h8 = $n0->background_psql('postgres', on_error_die => 1);
$h8->query_safe("SELECT pg_advisory_lock(88)");
my ($rc8, $out8, $err8) = $n1->psql('postgres', "SELECT pg_try_advisory_lock(88)", timeout => 30);
is($rc8, 0, 'L8: pg_try_advisory_lock returns cleanly (rc=0, no ERROR)');
is($out8, 'f', 'L8: ... and the value is FALSE (held elsewhere)');
is($err8, '', 'L8: ... with no stderr (try is not fail-closed)');
$h8->query_safe("SELECT pg_advisory_unlock(88)");
$h8->quit;
ok(adv_count('advisory_try_notavail_count') > $pre_notavail,
	"L8: advisory_try_notavail_count grew on the NOWAIT conflict");

# ----------
# L9: xact-advisory regression (spec-2.21 not broken).
# ----------
my $h9 = $n0->background_psql('postgres', on_error_die => 1);
$h9->query_safe("BEGIN");
$h9->query_safe("SELECT pg_advisory_xact_lock(404)");    # xact-scoped, node0
is($n1->safe_psql('postgres', "BEGIN; SELECT pg_try_advisory_xact_lock(404); ROLLBACK"),
	'f', 'L9: xact-advisory still excludes cross-node (spec-2.21 not regressed)');
$h9->query_safe("COMMIT");                                # xact-end releases automatically
my $xfree9 = 0;
for my $attempt (1 .. 50)
{
	my $v = $n1->safe_psql('postgres',
		"BEGIN; SELECT pg_try_advisory_xact_lock(404); ROLLBACK");
	if (defined $v && $v =~ /^t/) { $xfree9 = 1; last; }
	usleep(200_000);
}
ok($xfree9, 'L9: xact-advisory auto-released at COMMIT (node1 acquires in its own xact)');
$h9->quit;

# ----------
# L10: fail-closed mutual exclusion (wired; 53R80 unit-proven).
# ----------
ok( defined $n0->safe_psql(
		'postgres', q{
		SELECT value FROM pg_cluster_state
		WHERE category = 'advisory' AND key = 'advisory_failclosed_count'}),
	'L10: advisory_failclosed_count present (fail-closed path is wired)');
diag(
	"L10: 53R80 fail-closed (LMS unavailable -> ERROR, never a silent local grant) is "
	. "unit-proven (test_cluster_lock_acquire U5 + the lock.c FAILCLOSED counter); a healthy "
	. "2-node cluster keeps LMS ready, so it is not deterministically reachable here.");

# ----------
# L11: blocking advisory waits are bounded by a finite ges_request_timeout_ms.
# ----------
my $h11 = $n0->background_psql('postgres', on_error_die => 1);
$h11->query_safe("SELECT pg_advisory_lock(73)");    # node0 holds X(73)

# S3 forensics step 1a — pre-capture the breakdown counters so the asserts
# below are DELTA-based (a prior leg's bump can never satisfy them).
my $split_query = q{
	SELECT COALESCE(sum(value::bigint), 0) FROM pg_cluster_state
	WHERE category = 'ges'
	  AND key IN ('ges_timeout_true_wait_count',
				  'ges_timeout_retransmit_exhausted_count')};
my $split_before = $n1->safe_psql('postgres', $split_query);

# node1 blocks on the held key with a short finite per-session timeout; the wait
# must resolve (53R70) rather than hang -- this is the bound that protects a
# cross-node deadlock from hanging forever (real detection is forward spec-5.8).
my $t11_start = Time::HiRes::time();
my ($rc11, $out11, $err11) = $n1->psql(
	'postgres',
	"SET cluster.ges_request_timeout_ms = '3s'; SELECT pg_advisory_lock(73)",
	timeout => 40);
my $client11_ms = int((Time::HiRes::time() - $t11_start) * 1000);
isnt($rc11, 0, 'L11: blocking advisory acquire on a held key fails (does not hang)');
like($err11, qr/cluster lock acquire timeout|53R70|not available/i,
	'L11: ... with a finite-timeout 53R70 (bounded deadlock resolution)');

# S3 forensics step 1/1a — the folded 53R70 text must carry a fine-grained
# source attribution (errdetail) whose elapsed_ms is TRUE wall-clock (a
# genuine bounded wait burns >= 1s of the 3s window; a capacity fail would
# read ~0), and bump the matching dump_ges breakdown counter (delta-based).
# The wait resolves as either the CV deadline or the retransmit budget
# depending on which node masters the key (local- vs remote-master loop).
like(
	$err11,
	qr/source=(cv-wait-timeout|retransmit-exhausted)/,
	'L11b: 53R70 errdetail carries the timeout-source attribution');
if ($err11 =~ /elapsed_ms=(\d+)/)
{
	my $elapsed = $1;

	# S3 forensics step 1b — qualify elapsed_ms against the CLIENT-observed
	# wall clock instead of hardcoded bounds.  client - elapsed >= 0 catches
	# a filled-in configured value (a hardcoded 3000 fails on the retransmit
	# path where the client only saw ~1.7s);  the <= 1500 slack catches a
	# stale/zero fill (psql spawn + SET + connect overhead stay well under
	# 1.5s), so elapsed_ms must genuinely track this wait's wall clock.
	cmp_ok($client11_ms - $elapsed, '>=', 0,
		"L11b2: elapsed_ms=$elapsed never exceeds the client wall clock (client=${client11_ms}ms)");
	cmp_ok($client11_ms - $elapsed, '<=', 1500,
		"L11b3: elapsed_ms=$elapsed tracks the client wall clock (client=${client11_ms}ms)");
}
else
{
	fail('L11b2: errdetail carries elapsed_ms');
	fail('L11b3: errdetail carries elapsed_ms');
}
my $split_after = $n1->safe_psql('postgres', $split_query);
cmp_ok($split_after, '>', $split_before,
	'L11c: dump_ges timeout-source breakdown counter bumped by THIS bounded wait '
	. "($split_before -> $split_after)");

$h11->query_safe("SELECT pg_advisory_unlock(73)");
$h11->quit;

# ----------
# L12: blocking waiter is GRANTED after release (spec-5.5 P0 regression guard).
#   Both directions of one key -> covers local-master AND remote-master release
#   drain (the key has a single master; one direction releases on the master node,
#   the other on a non-master node).  A pre-fix local-master release deleted the
#   holder without draining the waiter -> the waiter would false-timeout 53R70.
# ----------
blocking_grant_after_release($n0, $n1, 131, 'L12a node0-release/node1-wait');
blocking_grant_after_release($n1, $n0, 131, 'L12b node1-release/node0-wait');

# ----------
# L13: mixed session+xact owner on the SAME key (§3.2 L5).  One backend holds
#   pg_advisory_lock (session) and pg_advisory_xact_lock (xact) on the same key:
#   COMMIT releases the xact owner but the session owner survives (still held
#   cross-node); backend exit drains the single GES holder once (no double
#   release, correct session-scope label at the :2476 backend-exit path).
# ----------
my $hm = $n0->background_psql('postgres', on_error_die => 1);
$hm->query_safe("SELECT pg_advisory_lock(151)");        # session owner
$hm->query_safe("BEGIN");
$hm->query_safe("SELECT pg_advisory_xact_lock(151)");    # xact owner, same key (mixed)
is(try_lock($n1, 'pg_try_advisory_lock(151)'),
	'f', 'L13: mixed session+xact owner held cross-node');
$hm->query_safe("COMMIT");                               # xact owner released; session survives
is(try_lock($n1, 'pg_try_advisory_lock(151)'),
	'f', 'L13: still held after COMMIT (session owner outlives the xact)');
$hm->quit;                                               # backend exit drains the session holder
ok(wait_until_acquirable($n1, 'pg_try_advisory_lock(151)', 15),
	'L13: released after backend exit (mixed-owner drain — single holder, no double release)');

# ----------
# L14 (S3 forensics step 1a): a MASTER-side capacity rejection must surface
#   with source=master-reject-queue-full in the folded 53R70 errdetail and
#   bump the capacity breakdown counter — never "unattributed".  The inject
#   forces node1's GES_REQUEST handler into the WORK_QUEUE_FULL reject reply,
#   so node0 trips it only on keys node1 masters (locally-mastered keys grant
#   without touching the wire handler and are skipped by the probe loop).
#   elapsed_ms must read (near) zero: the reject is immediate — the exact
#   "surfaced as timeout but never waited" class the S3 storm hid.
# ----------
$n1->safe_psql('postgres',
	"ALTER SYSTEM SET cluster.injection_points = 'cluster-ges-master-work-queue-full:skip'");
$n1->safe_psql('postgres', 'SELECT pg_reload_conf()');

my $cap_query = q{
	SELECT COALESCE(sum(value::bigint), 0) FROM pg_cluster_state
	WHERE category = 'ges' AND key = 'ges_timeout_capacity_count'};
my $cap_before = $n0->safe_psql('postgres', $cap_query);
my $mr_err = '';
my $mr_client_ms = 0;
for my $k (211 .. 226)
{
	my $t0 = Time::HiRes::time();
	my ($rc, $out, $err) = $n0->psql(
		'postgres',
		"SET cluster.ges_request_timeout_ms = '3s'; SELECT pg_advisory_lock($k)",
		timeout => 30);
	my $probe_ms = int((Time::HiRes::time() - $t0) * 1000);
	if ($rc != 0 && defined $err && $err =~ /master-reject-queue-full/)
	{
		$mr_err = $err;
		$mr_client_ms = $probe_ms;
		last;
	}
	# Locally-mastered key (granted): release and probe the next key.
	$n0->psql('postgres', 'SELECT pg_advisory_unlock_all()');
}
$n1->safe_psql('postgres', 'ALTER SYSTEM RESET cluster.injection_points');
$n1->safe_psql('postgres', 'SELECT pg_reload_conf()');

isnt($mr_err, '', 'L14: a remote-master capacity reject was driven (inject)');
like($mr_err, qr/cluster lock acquire timeout/,
	'L14b: ... surfaced through the folded 53R70 text');
like($mr_err, qr/source=master-reject-queue-full/,
	'L14c: ... with the master-reject attribution (never unattributed)');
if ($mr_err =~ /elapsed_ms=(\d+)/)
{
	# step 1b: an immediate reject reads near-zero on ITS OWN clock — and can
	# never exceed the client-observed wall time for the same statement.
	cmp_ok($1, '<=', 500,
		"L14d: errdetail elapsed_ms=$1 shows the reject was immediate");
	cmp_ok($1, '<=', $mr_client_ms,
		"L14d2: elapsed_ms=$1 within the client wall clock (client=${mr_client_ms}ms)");
}
else
{
	fail('L14d: errdetail carries elapsed_ms');
	fail('L14d2: errdetail carries elapsed_ms');
}
my $cap_after = $n0->safe_psql('postgres', $cap_query);
cmp_ok($cap_after, '>', $cap_before,
	"L14e: capacity breakdown counter bumped by the master reject ($cap_before -> $cap_after)");

$pair->stop_pair;
done_testing();
