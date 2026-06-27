#-------------------------------------------------------------------------
#
# 305_hang_manager_disposition.pl
#    spec-5.12 Hang Manager disposition end-to-end (single node).
#
#    Drives the DIAG-hosted disposition control loop against a real server
#    and asserts it really cancels/terminates the root blocker of a
#    non-deadlock hang — with counter deltas proving the disposition path ran
#    (not a hollow claim) — while NEVER disposing a non-actionable backend.
#
#    Test matrix (spec-5.12 §4.2):
#      L1  advisory mode: pg_cluster_hang_victims() recommends disposing the
#          root blocker + advisory_recommendations increments, but NOTHING is
#          signalled (dry-run; the blocker stays alive)
#      L9  pg_cluster_hang_victims() visibility: superuser / pg_read_all_stats
#          see the victim row; an unprivileged user sees only its own
#      L8  pg_cluster_hang_resolve() is superuser-only (perm error otherwise)
#      L7  manual pg_cluster_hang_resolve(pid): a non-victim pid is rejected
#          (returns false; no force); the real root victim is terminated
#      L2  enforce mode: the idle-in-tx LOCK TABLE holder (NO xid) is selected
#          as the root victim and really TERMINATED; terminates_issued +
#          resolved_confirmed increment; the waiter unblocks (real disposition)
#      L3  fail-CLOSED: an idle-in-tx holder that blocks NObody is never a
#          victim and is never terminated
#      L4  a local deadlock is resolved by PG deadlock.c (40P01), NOT by the
#          hang manager: no terminate is issued for the deadlock participants
#      L5  mode matrix: off freezes resolve_evaluations; advisory/enforce run
#      L6  a 2PC-prepared (no live backend) blocker is undisposable: the hang
#          manager does not / cannot force it — fail-CLOSED, finite-timeout
#          backstop (the SKIP_2PC "no live backend" contract)
#
# IDENTIFICATION
#    src/test/cluster_tap/t/305_hang_manager_disposition.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);
use PgracClusterNode;


my $node = PgracClusterNode->new('main');
$node->init;
$node->append_conf(
	'postgresql.conf', qq{
# spec-5.12 fast-disposition test knobs.
cluster.diag_main_loop_interval = 200
cluster.hang_manager_enabled = on
cluster.hang_sample_interval_ms = 200
cluster.hang_threshold_ms = 1500
cluster.hang_dump_enabled = on
cluster.hang_resolution_mode = advisory
cluster.hang_resolution_confirm_rounds = 1
cluster.hang_resolution_soft_timeout_ms = 1000
cluster.hang_resolution_max_per_round = 1
# deadlock.c must resolve a local deadlock well before the hang threshold so
# the hang manager never sees a deadlock waiter as a COMPLETE long-wait (L4).
deadlock_timeout = 500
# warning (not log): logs WARNING/ERROR/LOG/FATAL — keeps the LOG-level cluster
# messages AND surfaces the ERROR-level "deadlock detected" that L4 checks
# (log_min_messages=log would suppress ERROR, which sorts below LOG).
log_min_messages = warning
max_prepared_transactions = 4
});
$node->start;


# Fire a query expected to BLOCK without waiting for it to finish.
sub bg_start_blocking
{
	my ($h, $sql) = @_;
	$h->query_until(qr/PGRAC_FIRED/, "\\echo PGRAC_FIRED\n$sql;\n");
}

# A single `hang` category value.
sub hang_val
{
	my ($key) = @_;
	return $node->safe_psql('postgres',
		"SELECT value FROM pg_cluster_state WHERE category='hang' AND key='$key'");
}

# Poll pg_stat_activity until a backend running $qlike waits on a heavyweight lock.
sub wait_for_lock_wait
{
	my ($qlike, $secs) = @_;
	my $deadline = time() + $secs;
	while (time() < $deadline)
	{
		my $we = $node->safe_psql('postgres', qq{
			SELECT coalesce(wait_event_type,'') FROM pg_stat_activity
			WHERE query LIKE '$qlike' AND pid <> pg_backend_pid()
			  AND state = 'active' LIMIT 1});
		return 1 if defined $we && $we eq 'Lock';
		usleep(200_000);
	}
	return 0;
}

# Poll until $apid is recommended as a victim by pg_cluster_hang_victims().
sub wait_for_victim
{
	my ($apid, $secs) = @_;
	my $deadline = time() + $secs;
	while (time() < $deadline)
	{
		my $n = $node->safe_psql('postgres',
			"SELECT count(*) FROM pg_cluster_hang_victims() WHERE victim_pid = $apid");
		return 1 if defined $n && $n eq '1';
		usleep(200_000);
	}
	return 0;
}

# Poll until $pid is gone from pg_stat_activity.
sub wait_for_gone
{
	my ($pid, $secs) = @_;
	my $deadline = time() + $secs;
	while (time() < $deadline)
	{
		my $n = $node->safe_psql('postgres',
			"SELECT count(*) FROM pg_stat_activity WHERE pid = $pid");
		return 1 if defined $n && $n eq '0';
		usleep(200_000);
	}
	return 0;
}


$node->safe_psql('postgres', 'CREATE TABLE hangt (i int)');


# ----------
# Build a real, non-deadlock hang: idle-in-tx holder A (BEGIN; LOCK TABLE,
# which assigns NO xid) blocks waiter B.  A is the root blocker.
# ----------
my $ha = $node->background_psql('postgres', on_error_die => 1);
$ha->query_safe('BEGIN');
$ha->query_safe('LOCK TABLE hangt IN ACCESS EXCLUSIVE MODE');
my $apid = $ha->query_safe('SELECT pg_backend_pid()');

# Prove A holds the lock with NO xid (the idle-in-tx LOCK TABLE blocker that
# v0.3 P1#1 fixed: lock-holder, not xid, is the disposition gate).
my $axid = $node->safe_psql('postgres',
	"SELECT coalesce(backend_xid::text,'') FROM pg_stat_activity WHERE pid = $apid");
is($axid, '', 'root blocker A holds the lock with NO xid (idle-in-tx LOCK TABLE)');

my $hb = $node->background_psql('postgres', on_error_die => 1);
my $bpid = $hb->query_safe('SELECT pg_backend_pid()');
bg_start_blocking($hb, 'BEGIN; LOCK TABLE hangt IN ACCESS EXCLUSIVE MODE');
ok(wait_for_lock_wait('%LOCK TABLE hangt%', 20), 'waiter B is blocked on the relation lock');


# ----------
# L1: advisory mode recommends disposing A but signals nothing.
# ----------
ok(wait_for_victim($apid, 25),
   'L1 pg_cluster_hang_victims() recommends root blocker A as victim');
is($node->safe_psql('postgres',
	"SELECT recommended_action FROM pg_cluster_hang_victims() WHERE victim_pid = $apid"),
   'cancel-then-terminate',
   'L1 recommended_action is the cancel-then-terminate ladder');
is($node->safe_psql('postgres',
	"SELECT skip_reason FROM pg_cluster_hang_victims() WHERE victim_pid = $apid"),
   '', 'L1 root blocker A has no skip reason (a valid victim)');

# advisory must eventually record a recommendation but NEVER signal
my $deadline = time() + 10;
while (time() < $deadline && hang_val('hang_advisory_recommendations') eq '0') { usleep(200_000); }
cmp_ok(hang_val('hang_advisory_recommendations'), '>', '0',
	   'L1 advisory_recommendations incremented (evaluation ran)');
is(hang_val('hang_terminates_issued'), '0', 'L1 advisory issued NO terminate (dry-run)');
is(hang_val('hang_soft_cancels_issued'), '0', 'L1 advisory issued NO cancel (dry-run)');
is($node->safe_psql('postgres', "SELECT count(*) FROM pg_stat_activity WHERE pid = $apid"),
   '1', 'L1 root blocker A is still alive under advisory');


# ----------
# L9: pg_cluster_hang_victims() visibility gating (while the hang is live).
# ----------
$node->safe_psql('postgres', 'CREATE ROLE hu LOGIN');
is($node->safe_psql('postgres',
	"SELECT count(*) FROM pg_cluster_hang_victims() WHERE victim_pid = $apid"),
   '1', 'L9 superuser sees the victim row');
is($node->safe_psql('postgres',
	"SELECT count(*) FROM pg_cluster_hang_victims() WHERE victim_pid = $apid",
	extra_params => [ '-U', 'hu' ]),
   '0', 'L9 unprivileged user sees no victim rows (owned by another role)');
$node->safe_psql('postgres', 'GRANT pg_read_all_stats TO hu');
is($node->safe_psql('postgres',
	"SELECT count(*) FROM pg_cluster_hang_victims() WHERE victim_pid = $apid",
	extra_params => [ '-U', 'hu' ]),
   '1', 'L9 pg_read_all_stats member sees the victim row');


# ----------
# L8: pg_cluster_hang_resolve() is superuser-only.
# ----------
my ($rc, $stdout, $stderr) = $node->psql('postgres',
	"SELECT pg_cluster_hang_resolve($apid)", extra_params => [ '-U', 'hu' ]);
isnt($rc, 0, 'L8 non-superuser pg_cluster_hang_resolve() fails');
like($stderr, qr/must be superuser/, 'L8 perm error mentions superuser');


# ----------
# L7: manual pg_cluster_hang_resolve() rejects a non-victim pid.
# ----------
my $self_pid = $node->safe_psql('postgres', 'SELECT pg_backend_pid()');
is($node->safe_psql('postgres', "SELECT pg_cluster_hang_resolve($self_pid)"),
   'f', 'L7 pg_cluster_hang_resolve() of a non-victim pid returns false (no force)');


# ----------
# L2: enforce mode really terminates the root blocker A (cancel -> terminate
# ladder); the waiter B unblocks.  Counter deltas prove the path ran.
# ----------
$node->safe_psql('postgres', "ALTER SYSTEM SET cluster.hang_resolution_mode = enforce");
$node->reload;

ok(wait_for_gone($apid, 40), 'L2 enforce really TERMINATED the root blocker A');
cmp_ok(hang_val('hang_terminates_issued'), '>=', '1',
	   'L2 terminates_issued incremented (real disposition path ran)');
# B should now acquire the lock; give it a moment and confirm it is no longer
# waiting on a heavyweight lock (it either holds it or finished).
my $bdl = time() + 20;
my $b_unblocked = 0;
while (time() < $bdl)
{
	my $we = $node->safe_psql('postgres',
		"SELECT coalesce(wait_event_type,'') FROM pg_stat_activity WHERE pid = $bpid");
	if (!defined $we || $we ne 'Lock') { $b_unblocked = 1; last; }
	usleep(200_000);
}
ok($b_unblocked, 'L2 waiter B unblocked after the root blocker was terminated');
# spec §4.2 L2 ④: once the terminated victim is gone, a later resolve round
# confirms the disposition worked -> resolved_confirmed increments.  This also
# exercises the empty-sample success path (Hardening v1.1 F4): when the
# terminate clears every sample, the count must still move.
my $rc_deadline = time() + 20;
while (time() < $rc_deadline && hang_val('hang_resolved_confirmed') eq '0')
{
	usleep(200_000);
}
cmp_ok(hang_val('hang_resolved_confirmed'), '>=', '1',
	'L2 resolved_confirmed incremented after the victim was disposed (F4)');
# B's lock-wait left the psql mid-statement; quitting it can die on some
# platforms, so eval-wrap (cleanup must never fail the test).
eval { $hb->quit; } if defined $hb;
# A was terminated: its connection is gone, so do not ->quit the dead handle.
$ha = undef;


# ----------
# L3: fail-CLOSED — an idle-in-tx holder that blocks NObody is never disposed.
# ----------
my $h_lonely = $node->background_psql('postgres', on_error_die => 1);
$h_lonely->query_safe('BEGIN');
$h_lonely->query_safe('LOCK TABLE hangt IN ACCESS EXCLUSIVE MODE');
my $lonely_pid = $h_lonely->query_safe('SELECT pg_backend_pid()');
my $term_before = hang_val('hang_terminates_issued');
usleep(4_000_000);	  # several evaluation rounds in enforce mode
is($node->safe_psql('postgres', "SELECT count(*) FROM pg_stat_activity WHERE pid = $lonely_pid"),
   '1', 'L3 idle-in-tx holder blocking nobody is NOT terminated');
is($node->safe_psql('postgres',
	"SELECT count(*) FROM pg_cluster_hang_victims() WHERE victim_pid = $lonely_pid"),
   '0', 'L3 a holder that blocks nobody is not even a victim candidate');
eval { $h_lonely->query_safe('ROLLBACK'); };
eval { $h_lonely->quit; };


# ----------
# L4: a local deadlock is resolved by PG deadlock.c (40P01), not the hang
# manager — no terminate is attributed to the hang manager for it.
# ----------
my $term_pre_dl = hang_val('hang_terminates_issued');
$node->safe_psql('postgres', 'CREATE TABLE hangt2 (i int)');
my $d1 = $node->background_psql('postgres', on_error_die => 0);
my $d2 = $node->background_psql('postgres', on_error_die => 0);
$d1->query_safe('BEGIN');
$d2->query_safe('BEGIN');
$d1->query_safe('LOCK TABLE hangt IN ACCESS EXCLUSIVE MODE');
$d2->query_safe('LOCK TABLE hangt2 IN ACCESS EXCLUSIVE MODE');
# Cross requests -> deadlock; deadlock.c (500ms) breaks it with 40P01.
$d1->query_until(qr/PGRAC_FIRED/, "\\echo PGRAC_FIRED\nLOCK TABLE hangt2 IN ACCESS EXCLUSIVE MODE;\n");
$d2->query_until(qr/PGRAC_FIRED/, "\\echo PGRAC_FIRED\nLOCK TABLE hangt IN ACCESS EXCLUSIVE MODE;\n");
# Wait for deadlock.c to fire.
my $ddl = time() + 15;
my $deadlock_seen = 0;
while (time() < $ddl)
{
	my $log = slurp_file($node->logfile);
	if ($log =~ /deadlock detected/) { $deadlock_seen = 1; last; }
	usleep(200_000);
}
ok($deadlock_seen, 'L4 PG deadlock.c detected and broke the local deadlock');
is(hang_val('hang_terminates_issued'), $term_pre_dl,
   'L4 hang manager issued NO terminate for the deadlock (deadlock.c owns it)');
# Robust cleanup: the deadlock left one session aborted and one still holding
# locks; ->quit on such a psql handle can die on some platforms (the client may
# have exited on the async error), so eval-wrap both, then externally terminate
# any backend still holding the deadlock tables so later tests are unaffected.
eval { $d1->quit; };
eval { $d2->quit; };
$node->safe_psql('postgres', q{
	SELECT pg_terminate_backend(pid) FROM pg_stat_activity
	WHERE pid <> pg_backend_pid()
	  AND query ~ 'LOCK TABLE hangt2? '
	  AND state LIKE 'idle in transaction%'});


# ----------
# L5: mode matrix — off freezes resolve_evaluations; on runs.
# ----------
$node->safe_psql('postgres', "ALTER SYSTEM SET cluster.hang_resolution_mode = off");
$node->reload;
usleep(1_000_000);
my $eval_off1 = hang_val('hang_resolve_evaluations');
usleep(1_500_000);
my $eval_off2 = hang_val('hang_resolve_evaluations');
is($eval_off1, $eval_off2, 'L5 mode=off freezes resolve_evaluations (loop short-circuits)');
is(hang_val('hang_resolution_mode'), 'off', 'L5 dump reflects mode=off');
$node->safe_psql('postgres', "ALTER SYSTEM SET cluster.hang_resolution_mode = advisory");
$node->reload;
usleep(1_500_000);
cmp_ok(hang_val('hang_resolve_evaluations'), '>', $eval_off2,
	   'L5 mode=advisory resumes resolve_evaluations');


# ----------
# L6: a 2PC-prepared (no live backend) blocker is undisposable.  A waiter on
# its lock must NOT be terminated by the hang manager (the holder has no live
# backend to signal — SKIP_2PC "no live backend" / fail-CLOSED contract).
# ----------
$node->safe_psql('postgres', "ALTER SYSTEM SET cluster.hang_resolution_mode = enforce");
$node->reload;
$node->safe_psql('postgres', 'CREATE TABLE hangt3 (i int)');
$node->safe_psql('postgres', q{
	BEGIN;
	LOCK TABLE hangt3 IN ACCESS EXCLUSIVE MODE;
	PREPARE TRANSACTION 'hang5_12_2pc';
});
my $w2pc = $node->background_psql('postgres', on_error_die => 1);
my $w2pc_pid = $w2pc->query_safe('SELECT pg_backend_pid()');
bg_start_blocking($w2pc, 'BEGIN; LOCK TABLE hangt3 IN ACCESS EXCLUSIVE MODE');
ok(wait_for_lock_wait('%LOCK TABLE hangt3%', 20), 'L6 a waiter blocks on the 2PC-held lock');
my $term_pre_2pc = hang_val('hang_terminates_issued');
usleep(5_000_000);	  # several enforce evaluation rounds
is($node->safe_psql('postgres', "SELECT count(*) FROM pg_stat_activity WHERE pid = $w2pc_pid"),
   '1', 'L6 the waiter on a 2PC-held lock is NOT terminated (fail-CLOSED)');
is(hang_val('hang_terminates_issued'), $term_pre_2pc,
   'L6 no terminate issued for an undisposable (no live backend) blocker');
# Release the 2PC lock FIRST so the blocked waiter unblocks, THEN quit it
# (quitting a still-blocked psql would hang); eval-wrap the quit defensively.
$node->safe_psql('postgres', "ROLLBACK PREPARED 'hang5_12_2pc'");
eval { $w2pc->quit; };


# ----------
# L11: a root blocking MULTIPLE actionable waiters is genuinely disposed — the
# G-ABA edge bound to the sampled chain (Hardening v1.2) must NOT over-reject a
# real multi-waiter root; terminating it unblocks every waiter.
# ----------
$node->safe_psql('postgres', "ALTER SYSTEM SET cluster.hang_resolution_mode = enforce");
$node->reload;
$node->safe_psql('postgres', 'CREATE TABLE hangt_a (i int); CREATE TABLE hangt_b (i int)');

# R: one idle-in-tx holder of BOTH tables -> blocks two unrelated waiters.
my $hr2 = $node->background_psql('postgres', on_error_die => 1);
$hr2->query_safe('BEGIN');
$hr2->query_safe('LOCK TABLE hangt_a IN ACCESS EXCLUSIVE MODE');
$hr2->query_safe('LOCK TABLE hangt_b IN ACCESS EXCLUSIVE MODE');
my $r2pid = $hr2->query_safe('SELECT pg_backend_pid()');

my $hwa = $node->background_psql('postgres', on_error_die => 1);
my $wapid = $hwa->query_safe('SELECT pg_backend_pid()');
bg_start_blocking($hwa, 'BEGIN; LOCK TABLE hangt_a IN ACCESS EXCLUSIVE MODE');
my $hwb = $node->background_psql('postgres', on_error_die => 1);
my $wbpid = $hwb->query_safe('SELECT pg_backend_pid()');
bg_start_blocking($hwb, 'BEGIN; LOCK TABLE hangt_b IN ACCESS EXCLUSIVE MODE');
ok(wait_for_lock_wait('%hangt_a%', 20), 'L11 waiter A blocks on the multi-lock root');
ok(wait_for_lock_wait('%hangt_b%', 20), 'L11 waiter B blocks on the multi-lock root');

ok(wait_for_gone($r2pid, 40),
	'L11 multi-waiter root really TERMINATED (chain-bound edge did not over-reject)');
# both waiters must unblock once the shared root is gone
my $mw_dl = time() + 20;
my ($a_ok, $b_ok) = (0, 0);
while (time() < $mw_dl && !($a_ok && $b_ok))
{
	my $wa = $node->safe_psql('postgres',
		"SELECT coalesce(wait_event_type,'') FROM pg_stat_activity WHERE pid = $wapid");
	my $wb = $node->safe_psql('postgres',
		"SELECT coalesce(wait_event_type,'') FROM pg_stat_activity WHERE pid = $wbpid");
	$a_ok = 1 if !defined $wa || $wa ne 'Lock';
	$b_ok = 1 if !defined $wb || $wb ne 'Lock';
	usleep(200_000);
}
ok($a_ok && $b_ok, 'L11 both waiters unblocked after the multi-waiter root was terminated');
eval { $hwa->quit; };
eval { $hwb->quit; };
$hr2 = undef;    # terminated; do not quit the dead handle


# ----------
# L10: fail-CLOSED on an unprovable (truncated) root chain (Hardening v1.1 F3).
#
# With cluster.hang_max_chain_depth = 1 a 3-deep wait chain
#     W -> M1 -> M2 -> R(idle-in-tx root holder)
# truncates when ascending from the deepest waiter W (reaching the root needs
# two sampled hops), so the ascent lands on the mid-chain waiter M2.  M2 is
# itself a blocked waiter, NOT the root cause — disposing it is wrong.  The fix
# skips the truncated candidate (unprovable_root_skipped++) while the true root
# R is still found cleanly from the shallower waiters, so R may be a victim but
# the mid-chain M2 must NEVER be.  advisory mode: observe only, signal nothing.
# ----------
$node->safe_psql('postgres', 'ALTER SYSTEM SET cluster.hang_resolution_mode = advisory');
$node->safe_psql('postgres', 'ALTER SYSTEM SET cluster.hang_max_chain_depth = 1');
$node->reload;
$node->safe_psql('postgres',
	'CREATE TABLE hangt_r (i int); CREATE TABLE hangt_m2 (i int); CREATE TABLE hangt_m1 (i int)');

my $unprov_before = hang_val('hang_unprovable_root_skipped');

# R: idle-in-tx root holder of hangt_r (never waits -> not sampled).
my $hr = $node->background_psql('postgres', on_error_die => 1);
$hr->query_safe('BEGIN');
$hr->query_safe('LOCK TABLE hangt_r IN ACCESS EXCLUSIVE MODE');

# M2: holds hangt_m2, then blocks waiting on hangt_r (blocked by R).
my $hm2 = $node->background_psql('postgres', on_error_die => 1);
$hm2->query_safe('BEGIN');
$hm2->query_safe('LOCK TABLE hangt_m2 IN ACCESS EXCLUSIVE MODE');
my $m2pid = $hm2->query_safe('SELECT pg_backend_pid()');
bg_start_blocking($hm2, 'LOCK TABLE hangt_r IN ACCESS EXCLUSIVE MODE');
ok(wait_for_lock_wait('%hangt_r%', 20), 'L10 M2 blocks waiting on the root R');

# M1: holds hangt_m1, then blocks waiting on hangt_m2 (blocked by M2).
my $hm1 = $node->background_psql('postgres', on_error_die => 1);
$hm1->query_safe('BEGIN');
$hm1->query_safe('LOCK TABLE hangt_m1 IN ACCESS EXCLUSIVE MODE');
bg_start_blocking($hm1, 'LOCK TABLE hangt_m2 IN ACCESS EXCLUSIVE MODE');
ok(wait_for_lock_wait('%hangt_m2%', 20), 'L10 M1 blocks waiting on M2');

# W: deepest waiter, blocks waiting on hangt_m1 (blocked by M1).  Needs its own
# transaction block — a bare LOCK TABLE in autocommit is a no-op warning.
my $hw = $node->background_psql('postgres', on_error_die => 1);
$hw->query_safe('BEGIN');
bg_start_blocking($hw, 'LOCK TABLE hangt_m1 IN ACCESS EXCLUSIVE MODE');
ok(wait_for_lock_wait('%hangt_m1%', 20), 'L10 deepest waiter W blocks on M1');

# Once the chain is sampled, ascending from W truncates at depth 1 and the
# mid-chain candidate is dropped fail-CLOSED.
my $u_deadline = time() + 25;
while (time() < $u_deadline
	&& hang_val('hang_unprovable_root_skipped') eq $unprov_before)
{
	usleep(200_000);
}
cmp_ok(hang_val('hang_unprovable_root_skipped'), '>', $unprov_before,
	'L10 truncated root chain skipped fail-CLOSED (unprovable_root_skipped++)');

# the mid-chain waiter M2 must NEVER be advertised as a victim (only a cleanly
# provable root would be) — proves we did not pick a mid-chain victim.
is($node->safe_psql('postgres',
		"SELECT count(*) FROM pg_cluster_hang_victims() WHERE victim_pid = $m2pid"),
	'0', 'L10 mid-chain waiter M2 is never recommended as a victim');

# cleanup: terminate the chain backends, then eval-wrap the handle quits
# (a still-blocked psql quit can die — must never fail the test).
$node->safe_psql('postgres',
	"SELECT pg_terminate_backend(pid) FROM pg_stat_activity
	 WHERE pid <> pg_backend_pid()
	   AND (query LIKE '%hangt_r%' OR query LIKE '%hangt_m2%' OR query LIKE '%hangt_m1%')");
eval { $hr->quit; };
eval { $hm2->quit; };
eval { $hm1->quit; };
eval { $hw->quit; };


$node->stop;
done_testing();
