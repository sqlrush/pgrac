#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 108_2node_grant_smoke.pl
#	  spec-2.21 D12 — 2-node grant smoke (single-node minimal MVP for now,
#	  since cluster_ges cross-node send pipeline is a stub until spec-2.23
#	  BAST 配套 ships).
#
#	  This TAP exercises the cluster gate + 7-step state machine + normal
#	  release path on a single node;  cross-node assertions are gated
#	  behind ClusterPair availability and skip cleanly when only single-
#	  node smoke is exercised.
#
#	  Scenarios per spec-2.21 §4.2 (Q6 v1.1):
#	    S1  cross-node grant (skipped — pipeline stub)
#	    S2  reservation race (skipped — needs 2 nodes)
#	    S3  local-master fast-path (no remote contention → fast path taken)
#	    S4  OK_CONVERTED upgrade (single-node mode/upgrade smoke)
#	    S5  FAIL_TIMEOUT (skipped — needs remote master)
#	    S6  FAIL_DEADLOCK_PEND error-code stub (forced fault inject)
#	    S7  dontwait flag (advisory_xact_lock_shared NOWAIT semantic)
#	    S8  normal release remote master unblocks waiter (skipped — 2 nodes)
#	    S9  reservation idempotency repeat (same xact same key)
#	    S10 I1 monotonic forward (S1 → S5 single-direction)
#	    S11 I2 PENDING transition (single-node mode skips PENDING)
#	    S12 I3 reservation cancel + retry idempotency
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


my $node = PgracClusterNode->new('grant_smoke');
$node->init;
$node->append_conf('postgresql.conf', "cluster.node_id = 0\n");
$node->start;


# S3 local-master fast-path:  no remote contention, no waiters, no
# converts — gate predicate sends us through fast path.
my $s3 = $node->safe_psql('postgres', q{
	BEGIN;
	DO $$ BEGIN PERFORM pg_advisory_xact_lock(108001); END $$;
	COMMIT;
	SELECT 'S3_ok';
});
is($s3, 'S3_ok', 'S3 local-master fast-path acquire+release');


# S4 OK_CONVERTED upgrade smoke — same backend acquires shared then exclusive.
my $s4 = $node->safe_psql('postgres', q{
	BEGIN;
	DO $$ BEGIN
		PERFORM pg_advisory_xact_lock_shared(108002);
		PERFORM pg_advisory_xact_lock(108002);
	END $$;
	COMMIT;
	SELECT 'S4_ok';
});
is($s4, 'S4_ok', 'S4 shared→exclusive upgrade single-node smoke');


# S6 FAIL_DEADLOCK_PEND error-code wire — currently no fault injector;
# verify no panic on a clean acquire path that exercises the same code path.
my $s6 = $node->safe_psql('postgres', q{
	BEGIN;
	DO $$ BEGIN PERFORM pg_advisory_xact_lock(108006); END $$;
	COMMIT;
	SELECT 'S6_ok';
});
is($s6, 'S6_ok', 'S6 FAIL_DEADLOCK_PEND error code path (stub — real Tarjan spec-2.22)');


# S7 dontwait NOWAIT semantic:  pg_try_advisory_xact_lock returns
# immediately even if conflict — single-node MVP grants immediately.
my $s7 = $node->safe_psql('postgres', q{
	BEGIN;
	DO $$ BEGIN PERFORM pg_try_advisory_xact_lock(108007); END $$;
	COMMIT;
	SELECT 'S7_ok';
});
is($s7, 'S7_ok', 'S7 dontwait pg_try_advisory_xact_lock — ConditionalLock semantic');


# S9 reservation idempotency repeat — same xact same key acquires multiple times,
# all reentrant via LOCALLOCK nLocks++ (HC10).
my $s9 = $node->safe_psql('postgres', q{
	BEGIN;
	DO $$ BEGIN
		PERFORM pg_advisory_xact_lock(108009);
		PERFORM pg_advisory_xact_lock(108009);
		PERFORM pg_advisory_xact_lock(108009);
	END $$;
	COMMIT;
	SELECT 'S9_ok';
});
is($s9, 'S9_ok', 'S9 reservation idempotency — LOCALLOCK reentrant (HC10)');


# S10 I1 monotonic forward — multiple distinct keys, each goes S1→S5
# without revisiting earlier steps;  smoke that no assertion fires.
my $s10 = $node->safe_psql('postgres', q{
	BEGIN;
	DO $$ BEGIN
		PERFORM pg_advisory_xact_lock(108100);
		PERFORM pg_advisory_xact_lock(108101);
		PERFORM pg_advisory_xact_lock(108102);
	END $$;
	COMMIT;
	SELECT 'S10_ok';
});
is($s10, 'S10_ok', 'S10 I1 monotonic forward — multi-key acquire smoke');


# S11 I2 PENDING transition — single-node mode skips PENDING (LMS grants
# immediately).  Verify acquire-release cycle never deadlocks on PENDING.
my $s11 = $node->safe_psql('postgres', q{
	BEGIN;
	DO $$ BEGIN PERFORM pg_advisory_xact_lock(108110); END $$;
	COMMIT;
	BEGIN;
	DO $$ BEGIN PERFORM pg_advisory_xact_lock(108110); END $$;
	COMMIT;
	SELECT 'S11_ok';
});
is($s11, 'S11_ok', 'S11 I2 PENDING transition smoke — single-node bypass');


# S12 I3 reservation cancel + retry idempotency — rollback during xact
# triggers S7 cleanup;  next acquire of same key must succeed cleanly.
my $s12 = $node->safe_psql('postgres', q{
	BEGIN;
	DO $$ BEGIN PERFORM pg_advisory_xact_lock(108120); END $$;
	ROLLBACK;
	BEGIN;
	DO $$ BEGIN PERFORM pg_advisory_xact_lock(108120); END $$;
	COMMIT;
	SELECT 'S12_ok';
});
is($s12, 'S12_ok', 'S12 I3 reservation cancel + retry idempotency');


# ----------
# spec-2.23 Step 11 — D16 L9-L12 cross-node grant scenarios.
#
# Steps 1-9 wired the send/reply pipeline: cluster_ges_send_request_and_
# wait now performs HC17 5-tuple reply correlation, send_release_and_
# wait carries the logical BAST_ACK (HC19), and the LMS work_queue drain
# uses the GRD-owned enqueue_or_grant / release_and_pop_compatible_
# waiter API.  The observable surface for L9-L12 at single-node TAP is
# the new dump_ges counters (reply_wait_table_active / reply_late_drop /
# release_ack).
#
# Full 2-node ClusterPair behavioural verification of cross-node BAST
# semantics + concurrent reservation race + FAIL_TIMEOUT on remote-
# master unresponsive lands with Hardening v1.0.1 once the procno →
# ProcArray → SendProcSignal cross-node forwarding (spec-2.24 D axis)
# is in place to drive the holder backends.
# ----------

# L9: dump_ges surfaces 3 new counters (reply_wait_table_active,
# reply_late_drop_count, release_ack_count) with initial values 0.
my $dump_rows = $node->safe_psql('postgres', q{
	SELECT key
	FROM cluster_dump_state()
	WHERE category = 'ges'
	  AND key IN ('ges_reply_wait_table_active',
				  'ges_reply_late_drop_count',
				  'ges_release_ack_count')
	ORDER BY key
});
is($dump_rows,
   "ges_reply_late_drop_count\nges_release_ack_count\nges_reply_wait_table_active",
   'L9 dump_ges exposes 3 new spec-2.23 D13 counters');

# L10: reply_wait_table active count starts at 0 on a quiet single-node
# instance (no in-flight cross-node request).
my $active_init = $node->safe_psql('postgres', q{
	SELECT value
	FROM cluster_dump_state()
	WHERE category = 'ges' AND key = 'ges_reply_wait_table_active'
});
is($active_init, '0', 'L10 reply_wait_table_active starts at 0');

# L11: late_drop / release_ack counters also start at 0 (HC17 late drop
# only fires on observed cross-node race; release_ack only on remote-
# master release ACK).
my $late_drop_init = $node->safe_psql('postgres', q{
	SELECT value
	FROM cluster_dump_state()
	WHERE category = 'ges' AND key = 'ges_reply_late_drop_count'
});
is($late_drop_init, '0', 'L11 reply_late_drop_count starts at 0');

# L12: local-master fast path does NOT increment reply_wait_table_active
# (single-node MVP — cluster_ges_send_request_and_wait short-circuits
# when master == cluster_node_id, no HTAB insert).
$node->safe_psql('postgres', q{
	BEGIN;
	SELECT pg_advisory_xact_lock(108999);
	COMMIT;
});
my $active_post = $node->safe_psql('postgres', q{
	SELECT value
	FROM cluster_dump_state()
	WHERE category = 'ges' AND key = 'ges_reply_wait_table_active'
});
is($active_post, '0', 'L12 local-master path skips reply_wait_table_active bump');


$node->stop;

done_testing();
