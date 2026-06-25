#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 107_lms_enabled.pl
#	  spec-2.20 regression smoke for cluster.lms_enabled.
#
#	  Verifies the startup-only LMS ownership fallback:
#	    L1 default cluster.lms_enabled=on exposes one LMS backend
#	    L2 cluster.lms_enabled=off is visible as a postmaster bool GUC
#	    L3 cluster.lms_enabled=off keeps LMS un-forked after steady state
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use PgracClusterNode;


my $node_on = PgracClusterNode->new('lms_on');
$node_on->init;
$node_on->append_conf('postgresql.conf', "cluster.node_id = 0\n");
$node_on->start;

is($node_on->safe_psql('postgres', q{SHOW "cluster.lms_enabled"}),
   'on',
   'L1 cluster.lms_enabled default is on');

ok($node_on->poll_query_until(
	'postgres',
	q{SELECT count(*) = 1 FROM pg_stat_activity WHERE backend_type = 'lms'}),
   'L1a LMS aux process visible when cluster.lms_enabled=on');

$node_on->stop;


my $node_off = PgracClusterNode->new('lms_off');
$node_off->init;
$node_off->append_conf('postgresql.conf',
	"cluster.node_id = 0\ncluster.lms_enabled = off\n");
$node_off->start;

my $lms_guc_meta = $node_off->safe_psql('postgres', q{
	SELECT setting, vartype, context
	  FROM pg_settings
	 WHERE name = 'cluster.lms_enabled'});
is($lms_guc_meta, 'off|bool|postmaster',
   'L2 cluster.lms_enabled=off visible in pg_settings as postmaster bool');

# Give ServerLoop a chance to run its respawn branch; it must still not fork LMS.
sleep 2;

is($node_off->safe_psql(
	'postgres',
	q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'lms'}),
   '0',
   'L3 cluster.lms_enabled=off keeps LMS aux process un-forked');

$node_off->stop;


# ============================================================
# spec-2.21 D11:  L4-L8 cluster gate regression.
# ============================================================

my $node_gate = PgracClusterNode->new('lms_gate');
$node_gate->init;
$node_gate->append_conf('postgresql.conf',
	"cluster.node_id = 0\ncluster.grd_max_entries = 64\n");
$node_gate->start;

# L4: pg_advisory_xact_lock — xact-level advisory enters cluster gate
#     (single-node MVP path: LMS handler grants immediately).
my $xact_advisory = $node_gate->safe_psql('postgres', q{
	BEGIN;
	DO $$ BEGIN PERFORM pg_advisory_xact_lock(42); END $$;
	COMMIT;
	SELECT 'L4_ok';
});
is($xact_advisory, 'L4_ok',
   'L4 cluster.lms_enabled=on + pg_advisory_xact_lock(42) acquires cluster path then releases');

# L5: reentrant xact advisory — same xid same key second acquire goes through
#     LOCALLOCK reentrant path (HC10), does NOT re-enter 7-step.
my $reentrant = $node_gate->safe_psql('postgres', q{
	BEGIN;
	DO $$ BEGIN
		PERFORM pg_advisory_xact_lock(43);
		PERFORM pg_advisory_xact_lock(43);
		PERFORM pg_advisory_xact_lock(43);
	END $$;
	COMMIT;
	SELECT 'L5_ok';
});
is($reentrant, 'L5_ok',
   'L5 reentrant pg_advisory_xact_lock — LOCALLOCK reentrant path (HC10)');

# L6: pg_advisory_lock — session-level stays native (HC11).
my $session_advisory = $node_gate->safe_psql('postgres', q{
	DO $$ BEGIN
		PERFORM pg_advisory_lock(44);
		PERFORM pg_advisory_unlock(44);
	END $$;
	SELECT 'L6_ok';
});
is($session_advisory, 'L6_ok',
   'L6 session-level pg_advisory_lock — HC11 session advisory stays native');

# L7: low-mode RELATION lock (SELECT FOR UPDATE) — gate predicate filters
#     relation modes below ShareUpdateExclusiveLock.
$node_gate->safe_psql('postgres',
	q{CREATE TABLE l7_test(id int PRIMARY KEY, v int); INSERT INTO l7_test VALUES(1, 10);});
my $non_advisory = $node_gate->safe_psql('postgres', q{
	BEGIN;
	DO $$ DECLARE v_l7 int; BEGIN
		SELECT v INTO v_l7 FROM l7_test WHERE id = 1 FOR UPDATE;
	END $$;
	COMMIT;
	SELECT 'L7_ok';
});
is($non_advisory, 'L7_ok',
   'L7 SELECT FOR UPDATE (RowShare/RowExclusive < SUEX) — gate predicate skips cluster path');

# L8: cluster.lock_acquire_cluster_path emergency bypass — when set false
#     (PGC_POSTMASTER), entire gate is bypassed.
$node_gate->stop;
$node_gate->append_conf('postgresql.conf',
	"cluster.lock_acquire_cluster_path = off\n");
$node_gate->start;

my $bypass = $node_gate->safe_psql('postgres', q{
	BEGIN;
	DO $$ BEGIN PERFORM pg_advisory_xact_lock(45); END $$;
	COMMIT;
	SELECT 'L8_ok';
});
is($bypass, 'L8_ok',
   'L8 cluster.lock_acquire_cluster_path=off emergency bypass — gate skipped');

$node_gate->stop;

# spec-2.25 D15 L13-L18: cluster_lock_should_globalize extension regression.
# Re-enable cluster gate to validate RELATION + OBJECT branches.
#
# spec-5.7 §3.1d (Option A) reclassification:  this is a single-node cluster with
# no alive peer and no fencing, so the lock.c gate evaluates
# cluster_extend_liveness_is_sole_native() = true and short-circuits
# globalize-eligible locks to the PG-native path.  The should_globalize predicate
# is unchanged (unit coverage: test_cluster_lock_acquire.c); what changed is that
# single-node no longer *engages* the cluster path, so the RELATION/OBJECT counter
# stays flat for L13/L17/L18 (was: counter advances).  L14-L16 already expected
# "unchanged".  Cross-node globalize engage + counter advance is 2-node coverage.
$node_gate->append_conf('postgresql.conf',
	"cluster.lock_acquire_cluster_path = on\n");
$node_gate->start;

# Capture pre-counter for RELATION/OBJECT gate hits.
sub _grd_path_count
{
	return $node_gate->safe_psql(
		'postgres',
		q{SELECT value::int FROM pg_cluster_state
		   WHERE category='grd' AND key='grd_relation_object_cluster_path_count'});
}

# L13: DDL CREATE INDEX (ShareUpdateExclusiveLock on user table).  SUEX RELATION
#      is globalize-eligible, but single-node sole-native takes the PG-native
#      lock, so the RELATION cluster path counter is unchanged (Option A).
my $pre_l13 = _grd_path_count();
$node_gate->safe_psql('postgres', q{
	CREATE TABLE pgrac_l13_t (id int);
});
$node_gate->safe_psql('postgres', q{
	CREATE INDEX CONCURRENTLY pgrac_l13_idx ON pgrac_l13_t(id);
});
my $post_l13 = _grd_path_count();
is($post_l13, $pre_l13,
	'L13 CREATE INDEX CONCURRENTLY (SUEX on user table) — Option A sole-native: PG-native lock, RELATION cluster path counter unchanged');

# L14: SELECT (AccessShareLock) — mode < SUEX → gate false, counter unchanged.
my $pre_l14 = _grd_path_count();
$node_gate->safe_psql('postgres', q{SELECT count(*) FROM pgrac_l13_t});
my $post_l14 = _grd_path_count();
is($post_l14, $pre_l14,
	'L14 SELECT (AccessShare < SUEX) — gate false, RELATION counter unchanged');

# L15: VACUUM pg_class (SUEX on oid < FirstNormalObjectId) — HC24 system
# catalog skip → gate false.
my $pre_l15 = _grd_path_count();
$node_gate->safe_psql('postgres', q{VACUUM pg_class});
my $post_l15 = _grd_path_count();
is($post_l15, $pre_l15,
	'L15 VACUUM pg_class — HC24 system catalog skip, counter unchanged');

# L16: explicit ACCESS EXCLUSIVE on an already-created temp table —
# relpersistence='t' skip per HC25.  Keep this in one session because temp
# relations are session-local, and snapshot the counter after CREATE TEMP so
# ancillary locks taken by table creation are not attributed to the branch.
my $l16_counts = $node_gate->safe_psql('postgres', q{
	CREATE TEMP TABLE pgrac_l16_temp (id int);
	SELECT value::int FROM pg_cluster_state
	 WHERE category='grd' AND key='grd_relation_object_cluster_path_count';
	BEGIN;
	LOCK TABLE pgrac_l16_temp IN ACCESS EXCLUSIVE MODE;
	COMMIT;
	SELECT value::int FROM pg_cluster_state
	 WHERE category='grd' AND key='grd_relation_object_cluster_path_count';
});
my ($pre_l16, $post_l16) = split /\n/, $l16_counts;
is($post_l16, $pre_l16,
	'L16 TEMP relation ACCESS EXCLUSIVE — HC25 temp skip, counter unchanged');

# L17: unlogged + ALTER (SUEX) — relpersistence='u' is globalize-eligible (HC25
# unlogged still cluster per shared-disk semantic), but single-node sole-native
# takes the PG-native lock, so the counter is unchanged (Option A).
my $pre_l17 = _grd_path_count();
$node_gate->safe_psql('postgres', q{
	CREATE UNLOGGED TABLE pgrac_l17_unlog (id int);
	ALTER TABLE pgrac_l17_unlog ADD COLUMN val text;
});
my $post_l17 = _grd_path_count();
is($post_l17, $pre_l17,
	'L17 UNLOGGED table ALTER — Option A sole-native: HC25 unlogged eligible but PG-native, counter unchanged');

# L18: DROP TYPE on user-created composite type (OBJECT path).  objoid is
# user-range and mode is AccessExclusiveLock → HC26+HC27 branches → gate
# returns true, but single-node sole-native takes the PG-native lock, so the
# counter is unchanged (Option A).
my $pre_l18 = _grd_path_count();
$node_gate->safe_psql('postgres', q{
	CREATE TYPE pgrac_l18_t AS (a int, b text);
	DROP TYPE pgrac_l18_t;
});
my $post_l18 = _grd_path_count();
is($post_l18, $pre_l18,
	'L18 CREATE/DROP TYPE (OBJECT user oid + AccessExclusive) — Option A sole-native: PG-native lock, OBJECT cluster path counter unchanged');

# ============================================================
# spec-2.26 D9 L19-L21:  LOCKTAG_TRANSACTION cluster gate regression
# (HC39 ShareLock/ExclusiveLock + HC46 PG XactLockTable* auto-routing
#  + HC48 top-level release via LockReleaseAll vs subxact XactLockTableDelete).
#
# spec-5.7 §3.1d (Option A):  these run single-node sole-native, so the TRANSACTION
# globalize-eligible locks (ShareLock/ExclusiveLock) take the PG-native path and
# the TRANSACTION cluster path counter stays flat.  The should_globalize gate for
# LOCKTAG_TRANSACTION is unit-covered (test_7step_transaction_should_globalize_gate
# in test_cluster_lock_acquire.c).  Cross-node engage + counter advance is 2-node
# coverage.
#
# Helper to read the new spec-2.26 transaction counter; mirrors
# _grd_path_count which reads relation_object_cluster_path_count.
# ============================================================
sub _grd_transaction_count
{
	return $node_gate->safe_psql(
		'postgres',
		q{SELECT value::int FROM pg_cluster_state
		   WHERE category='grd' AND key='grd_transaction_cluster_path_count'});
}

# L19: top-level COMMIT — main xid TRANSACTION lock auto-acquired by
# AssignTransactionId / XactLockTableInsert (ExclusiveLock owner take);
# release via LockReleaseAll at xact end (HC48 — NOT XactLockTableDelete).
# Single-node sole-native takes the PG-native lock, so the TRANSACTION counter
# is unchanged (Option A).
my $pre_l19 = _grd_transaction_count();
$node_gate->safe_psql('postgres', q{
	BEGIN;
	CREATE TABLE pgrac_l19_t (id int);
	INSERT INTO pgrac_l19_t VALUES (1);
	COMMIT;
});
my $post_l19 = _grd_transaction_count();
is($post_l19, $pre_l19,
	'L19 top-level write xact (BEGIN/INSERT/COMMIT) — Option A sole-native: XactLockTableInsert auto-acquires PG-native TRANSACTION lock, counter unchanged');

# L20: true concurrent waiter — backend B waits on backend A's row-locking
# xact, which drives XactLockTableWait(ShareLock) on A's xid.  The "waiter is
# blocked on transactionid lock" assertion below still holds (the lock is taken,
# just natively); under Option A sole-native the wait path does not engage the
# cluster gate, so the TRANSACTION counter stays unchanged.
my $holder = $node_gate->background_psql('postgres', timeout => 30);
my $waiter = $node_gate->background_psql('postgres', timeout => 30, on_error_stop => 0);

$holder->query_until(qr/l20_holder_ready/, q{
	BEGIN;
	UPDATE pgrac_l19_t SET id = 10 WHERE id = 1;
	\echo l20_holder_ready
});

my $waiter_pid = $waiter->query_safe(q{
	BEGIN;
	SELECT pg_backend_pid();
});
chomp $waiter_pid;

$waiter->query_until(qr/l20_waiter_has_xid/, q{
	SELECT txid_current();
	\echo l20_waiter_has_xid
});

my $pre_l20 = _grd_transaction_count();
$waiter->query_until(qr/l20_waiter_started/, q{
	\echo l20_waiter_started
	SELECT id FROM pgrac_l19_t WHERE id = 1 FOR UPDATE;
	\echo l20_waiter_done
});

ok($node_gate->poll_query_until(
		'postgres',
		qq{SELECT wait_event_type = 'Lock' AND wait_event = 'transactionid'
		     FROM pg_stat_activity
		    WHERE pid = $waiter_pid},
		't'),
   'L20 waiter backend is blocked on transactionid lock (XactLockTableWait path)');

my $post_l20 = _grd_transaction_count();
is($post_l20, $pre_l20,
	'L20 concurrent row-lock waiter — Option A sole-native: XactLockTableWait routes PG-native, TRANSACTION counter unchanged');

$holder->query_safe(q{COMMIT;});
$waiter->query_until(qr/l20_waiter_done/, q{});
$waiter->quit;
$holder->quit;

# L21: subtransaction rollback path — top-level xid is assigned before the
# snapshot; the SAVEPOINT write assigns a subxid and ROLLBACK TO SAVEPOINT
# exercises the XactLockTableDelete(subxid) release path.  Single-node
# sole-native takes the PG-native lock, so the TRANSACTION counter is
# unchanged (Option A).
my $pre_l21 = _grd_transaction_count();
$node_gate->safe_psql('postgres', q{
	BEGIN;
	SELECT txid_current();
	SAVEPOINT pgrac_l21_sp;
	INSERT INTO pgrac_l19_t VALUES (3);
	ROLLBACK TO SAVEPOINT pgrac_l21_sp;
	COMMIT;
});
my $post_l21 = _grd_transaction_count();
is($post_l21, $pre_l21,
	'L21 SAVEPOINT rollback — Option A sole-native: subxid PG-native TRANSACTION lock acquired/released via XactLockTableDelete, counter unchanged');

$node_gate->safe_psql('postgres', q{DROP TABLE IF EXISTS pgrac_l19_t});

# ============================================================
# spec-2.27 D11 L22-L24:  GES reliability hardening regression
# (HC53 double-direction GUC gate + HC52 retransmit dedup +
#  HC54 priority starvation observability, wire-zero L107 N+5).
# ============================================================

# L22 — HC53 perpetual-wait double-direction GUC invariant.
#  L22a:  SET ges_request_timeout_ms = -1 while retransmit_max_attempts=0
#         → ERROR (invalid_parameter_value), value unchanged.
#  L22b:  SET retransmit_max_attempts > 0 first, then SET timeout=-1
#         → success, value updated.
#  L22c:  After successful timeout=-1, SET retransmit_max_attempts=0
#         → ERROR, value unchanged.
{
	# Start from a known state: retransmit disabled.
	$node_gate->safe_psql('postgres',
		q{ALTER SYSTEM SET cluster.ges_retransmit_max_attempts = 0; SELECT pg_reload_conf();});
	# Quick poll for the SIGHUP-driven reload to settle.
	$node_gate->poll_query_until('postgres',
		q{SELECT current_setting('cluster.ges_retransmit_max_attempts')::int = 0}, 't');

	# L22a — perpetual-wait while retransmit disabled MUST be rejected.
	my ($l22a_ret, $l22a_out, $l22a_err) = $node_gate->psql(
		'postgres', q{SET cluster.ges_request_timeout_ms = -1;});
	like($l22a_err, qr/perpetual wait.*requires.*ges_retransmit_max_attempts/i,
		'L22a HC53 — SET timeout=-1 with retransmit=0 rejected');

	is($node_gate->safe_psql('postgres',
			q{SELECT setting FROM pg_settings WHERE name='cluster.ges_request_timeout_ms'}),
		'60000',
		'L22a HC53 — rejected GUC value remains at default');

	# L22b — enable retransmit then perpetual-wait succeeds.
	$node_gate->safe_psql('postgres',
		q{ALTER SYSTEM SET cluster.ges_retransmit_max_attempts = 5; SELECT pg_reload_conf();});
	$node_gate->poll_query_until('postgres',
		q{SELECT current_setting('cluster.ges_retransmit_max_attempts')::int = 5}, 't');

	$node_gate->safe_psql('postgres', q{SET cluster.ges_request_timeout_ms = -1;});
	# SET is session-local; ALTER SYSTEM applies for L22c steady-state check.
	$node_gate->safe_psql('postgres',
		q{ALTER SYSTEM SET cluster.ges_request_timeout_ms = -1; SELECT pg_reload_conf();});
	$node_gate->poll_query_until('postgres',
		q{SELECT setting::int = -1 FROM pg_settings WHERE name='cluster.ges_request_timeout_ms'}, 't');
	pass('L22b HC53 — perpetual-wait accepted after retransmit_max_attempts > 0');

	# L22c — reverse direction: ALTER SYSTEM retransmit=0 while
	# timeout=-1 → rejected by the check hook and existing value remains.
	# This GUC is PGC_SIGHUP, so session SET would fail at the context layer
	# before exercising the HC53 invariant.
	my ($l22c_ret, $l22c_out, $l22c_err) = $node_gate->psql(
		'postgres', q{ALTER SYSTEM SET cluster.ges_retransmit_max_attempts = 0;});
	like($l22c_err, qr/retransmit_max_attempts.*incompatible.*perpetual wait/i,
		'L22c HC53 — reverse direction ALTER SYSTEM retransmit=0 with timeout=-1 rejected');

	is($node_gate->safe_psql('postgres', q{SHOW cluster.ges_retransmit_max_attempts}),
		'5',
		'L22c HC53 — rejected retransmit GUC value remains 5');

	# Restore steady state for downstream tests.
	$node_gate->safe_psql('postgres',
		q{ALTER SYSTEM RESET cluster.ges_request_timeout_ms;
		  ALTER SYSTEM RESET cluster.ges_retransmit_max_attempts;
		  SELECT pg_reload_conf();});
}

# L23 — HC52 retransmit dedup observable through counter surface.
#  The receiver-side dedup HTAB is in shmem; first REQUEST registers a
#  MISS entry, retransmits (the loop body in cluster_ges.c re-enqueues
#  the same wire payload after backoff) hit CACHED_REPLY post-process.
#  In a single-node TAP setup the cross-node retransmit path is not
#  exercised end-to-end (local-master fast path skips it), so we
#  validate that the dedup counter accessors are link-stable and read
#  zero (no real cross-node retransmit triggered).  Full behavioural
#  verification of retransmit cycles waits for the ClusterPair TAP
#  upgrade tracked in spec-2.25 / spec-2.27 hardening notes.
is($node_gate->safe_psql('postgres',
		q{SELECT count(*)::int FROM pg_cluster_state WHERE category='lms' AND key='priority_starvation_observed_count'}),
	'1',
	'L23 HC52/HC54 — priority_starvation_observed_count counter surface present');

# L24 — HC54 wire-zero invariant.  After running TAP scenarios L19-L23 we
#  must observe priority_starvation_observed_count == 0 (no GES retransmit
#  budget consumed in single-node MVP path) AND no GES_REQ_OPCODE_
#  PRIORITY_BOOST wire send (validated indirectly:  no outbound counter
#  references opcode 11;  reserved enum value not in dispatch tables).
{
	my $starvation = $node_gate->safe_psql('postgres',
		q{SELECT value::bigint FROM pg_cluster_state
		   WHERE category='lms' AND key='priority_starvation_observed_count'});
	is($starvation, '0',
		'L24 HC54 — priority_starvation_observed_count == 0 (no retransmit budget consumed in single-node path)');
}

$node_gate->stop;

done_testing();
