/*-------------------------------------------------------------------------
 *
 * test_cluster_recovery_merge_cold_fixture.h
 *	  The four-instance cold-crash fixture shared by the merge module's cold
 *	  preflight and completion tests: thread 1 is the founder, threads 2-4
 *	  crashed (sealed and tail-validated unless a test says otherwise), and
 *	  a preflight runner that reports a FATAL instead of aborting.
 *
 *	  Included after test_cluster_recovery_merge_seal_boundary.h.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_recovery_merge_cold_fixture.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef TEST_CLUSTER_RECOVERY_MERGE_COLD_FIXTURE_H
#define TEST_CLUSTER_RECOVERY_MERGE_COLD_FIXTURE_H

#define REQUIRED_FLAGS (SEAL_FLAGS | TAIL_FLAGS)
#define OWN_REDO 0x900

static void
fixture(void)
{
	memset(&census, 0, sizeof(census));
	census.generated = true;
	census.own_thread = 1;
	census.n_crashed_candidate = 3;
	census.candidate_bitmap[0] = 14; /* threads 2, 3 and 4 crashed */
	census.dbstate_at_startup = DB_IN_PRODUCTION;
	census.local_recovery_needed = true;
	memset(&pool, 0, sizeof(pool));
	pool_present = false;
	memset(roots, 0, sizeof(roots));
	memset(tokens, 0, sizeof(tokens));
	for (uint16 tid = 1; tid <= NODES; tid++) {
		ClusterControlRootIdentity *id = &roots[tid].identity;
		ClusterWalThreadClaim claim;

		id->system_identifier = 9;
		id->storage_uuid[0] = 1;
		id->authority_uuid[6] = 0x40;
		id->authority_uuid[8] = 0x80;
		id->origin_thread_id = tid;
		id->origin_node_id = tid - 1;
		id->thread_claim_created_at = 42;
		id->origin_owner_incarnation = 11;
		id->root_lineage_seq = 3;
		cluster_wal_thread_claim_fill(&claim, tid, tid - 1, 42);
		id->thread_claim_crc32c = claim.crc;
		/* sealed and tail-validated by a recoverer that failed */
		roots[tid].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
		roots[tid].root_flags = REQUIRED_FLAGS;
		roots[tid].checkpoint_tli = roots[tid].tail_tli = 1;
		roots[tid].checkpoint_lower_lsn = redo[tid] = 0x800;
		roots[tid].validated_tail_lsn_exclusive = tail_end[tid] = 0x1000;
		roots[tid].root_publish_seq = 5;
		tokens[tid].origin_thread_id = tid;
		tokens[tid].root_lineage_seq = 3;
		tokens[tid].lifecycle = roots[tid].lifecycle;
		tokens[tid].root_flags = roots[tid].root_flags;
		tokens[tid].file_txn_seq = 77;
		tokens[tid].root_publish_seq = 5;
		open_result[tid] = tail_result[tid] = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
		fence_current[tid] = true;
		readback_cleared[tid] = 0;
		recover_on_open[tid] = 0;
		open_calls[tid] = tail_calls[tid] = source_calls[tid] = revalidate_calls[tid] = 0;
	}
	formations_built = formations_destroyed = 0;
	completion_reset();
	events[0] = '\0';
	cluster_shared_config = true;
	fatal_armed = false;
}

/* Crashed while open: nobody sealed it.  Its checkpoint-advanced extent
 * is shorter than the tail the seal will validate. */
static void
make_open(uint16 tid)
{
	roots[tid].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	roots[tid].root_flags = SEAL_FLAGS | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID;
	roots[tid].validated_tail_lsn_exclusive = 0xc00;
	tokens[tid].lifecycle = roots[tid].lifecycle;
	tokens[tid].root_flags = roots[tid].root_flags;
}

/* Sealed, but its sealer failed before validating the tail. */
static void
make_sealed_without_tail(uint16 tid)
{
	roots[tid].root_flags = SEAL_FLAGS;
	roots[tid].tail_tli = 0;
	roots[tid].validated_tail_lsn_exclusive = 0;
	tokens[tid].root_flags = roots[tid].root_flags;
}

/* Run the preflight; a FATAL is reported (not aborted on) and returns
 * false with *plan left NULL. */
static bool
preflight(ClusterRecoveryFencePlan **plan, ClusterMergeEngage *engage)
{
	*plan = NULL;
	fatal_armed = true;
	if (setjmp(fatal_jump) != 0)
		return false;
	*engage = cluster_recovery_merge_preflight_readonly(1, OWN_REDO, plan);
	fatal_armed = false;
	return true;
}

static void
expect_engaged(ClusterRecoveryFencePlan **plan)
{
	ClusterMergeEngage engage = CLUSTER_MERGE_NO_NO_PLAN;

	if (!preflight(plan, &engage)) {
		printf("# unexpected FATAL: %s (%s)\n", last_message, last_detail);
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT_EQ(engage, CLUSTER_MERGE_ENGAGE);
	UT_ASSERT(*plan != NULL);
}

#endif /* TEST_CLUSTER_RECOVERY_MERGE_COLD_FIXTURE_H */
