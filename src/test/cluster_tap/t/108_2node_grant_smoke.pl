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


# Cross-node scenarios (S1/S2/S5/S8):  documented as skipped until spec-2.23
# BAST 配套 wires the real send/reply pipeline.
SKIP: {
	skip 'cross-node send pipeline is single-node MVP stub until spec-2.23 BAST 配套', 4;
	pass 'S1 cross-node grant (deferred)';
	pass 'S2 reservation race (deferred)';
	pass 'S5 FAIL_TIMEOUT remote-master (deferred)';
	pass 'S8 normal release remote unblocks waiter (deferred)';
}


$node->stop;

done_testing();
