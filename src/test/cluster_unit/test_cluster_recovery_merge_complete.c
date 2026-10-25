/*-------------------------------------------------------------------------
 *
 * test_cluster_recovery_merge_complete.c
 *	  Recovery completion of the cold fence plan: after typed cold replay,
 *	  every fenced origin is published RECOVERY_COMPLETE through the
 *	  exclusive handoff -- terminal patches built under the serial set, the
 *	  retention pin sealed, IR released with confirmation, each root
 *	  finalized through the sealed pin, the pin released -- and a failure
 *	  publishes nothing further.
 *
 *	  The product source is compiled into this test; the handoff's owners
 *	  are fixtures (test_cluster_recovery_merge_seal_boundary.h).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_recovery_merge_complete.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <setjmp.h>
#include <stdarg.h>
#include <stdlib.h>

/* Every ereport of the module lands in the fixture; ERROR and above jump
 * back to the test (see the boundary). */
static void test_ereport_begin(int elevel);
static void test_ereport_end(void);

#undef ereport
#define ereport(elevel, ...)                                                                       \
	do {                                                                                           \
		test_ereport_begin(elevel);                                                                \
		(void)(__VA_ARGS__);                                                                       \
		test_ereport_end();                                                                        \
	} while (0)

#include "../../backend/cluster/cluster_recovery_merge.c"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# unexpected Assert: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

#include "test_cluster_recovery_merge_seal_boundary.h"
#include "test_cluster_recovery_merge_cold_fixture.h"

/* A sealed plan as it stands after pass 2: committed, the serial set held
 * (one COLD_FORMED guard per origin, ascending) and bound to the pin. */
static ClusterRecoveryFencePlan *
hold(ClusterRecoveryFencePlan *plan)
{
	if (plan == NULL)
		return NULL;
	plan->serial_guards.count = plan->origin_count;
	plan->serial_guards.release_timeout_ms = 1000;
	for (uint16 i = 0; i < plan->origin_count; i++) {
		ClusterRecoverySerialGuard *guard = &plan->serial_guards.guards[i];

		guard->held = true;
		guard->mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
		guard->duty = plan->origins[i].duty;
		guard->root_read_token = plan->origins[i].root_token;
	}
	plan->serial_held = true;
	plan->committed = true;
	plan->retention_pin = (ClusterWalRetentionPin *)&pin_obj;
	events[0] = '\0';
	return plan;
}

static ClusterRecoveryFencePlan *
replayed_plan(void)
{
	ClusterRecoveryFencePlan *plan = NULL;

	fixture();
	expect_engaged(&plan);
	return hold(plan);
}

/* Whatever a failed completion left, release it the way the caller does. */
static void
drop_plan(ClusterRecoveryFencePlan **plan)
{
	if (*plan == NULL)
		return;
	(*plan)->serial_held = false;
	(*plan)->serial_guards.count = 0;
	(*plan)->retention_pin = NULL;
	cluster_recovery_merge_fence_plan_destroy(plan);
	UT_ASSERT(*plan == NULL);
}

static ClusterRecoveryFenceCompleteV1
complete(ClusterRecoveryFencePlan *plan, uint16 *thread, int *detail)
{
	*thread = 0;
	*detail = 0;
	return cluster_recovery_merge_fence_plan_complete_v1(plan, thread, detail);
}

/*
 * Every terminal is built while IR is held; IR is released only after the
 * pin is sealed; every root is finalized after that, in origin order; the
 * pin goes last.  The plan then holds nothing and can be destroyed.
 */
UT_TEST(test_complete_publishes_every_origin)
{
	ClusterRecoveryFencePlan *plan = replayed_plan();
	uint16 thread;
	int detail;

	if (plan == NULL)
		return;
	UT_ASSERT_EQ(complete(plan, &thread, &detail), CLUSTER_RECOVERY_FENCE_COMPLETE_OK);
	UT_ASSERT(strcmp(events, "patch:2;patch:3;patch:4;seal-pin;release-ir;finalize:2;"
							 "finalize:3;finalize:4;release-pin;")
			  == 0);
	for (uint16 tid = 2; tid <= NODES; tid++)
		UT_ASSERT_EQ(roots[tid].lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE);
	UT_ASSERT(!plan->serial_held);
	UT_ASSERT_EQ(plan->serial_guards.count, 0);
	UT_ASSERT(plan->retention_pin == NULL);
	UT_ASSERT(cluster_recovery_merge_fence_plan_release_serial(plan));
	cluster_recovery_merge_fence_plan_destroy(&plan);
	UT_ASSERT(plan == NULL);

	/* A root another founder already completed with the same terminal. */
	plan = replayed_plan();
	finalize_result[3] = CLUSTER_THREAD_ROOT_FINALIZE_ALREADY_COMPLETE;
	UT_ASSERT_EQ(complete(plan, &thread, &detail), CLUSTER_RECOVERY_FENCE_COMPLETE_OK);
	UT_ASSERT(strstr(events, "finalize:4;release-pin;") != NULL);
	drop_plan(&plan);
}

/* Generations the preflight sealed itself complete with the tail it
 * validated. */
UT_TEST(test_complete_after_preflight_seal)
{
	ClusterRecoveryFencePlan *plan = NULL;
	uint16 thread;
	int detail;

	fixture();
	make_open(2);
	make_sealed_without_tail(3);
	tail_end[3] = 0x1100;
	expect_engaged(&plan);
	if (hold(plan) == NULL)
		return;
	UT_ASSERT_EQ(complete(plan, &thread, &detail), CLUSTER_RECOVERY_FENCE_COMPLETE_OK);
	UT_ASSERT_EQ(roots[2].lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE);
	UT_ASSERT_EQ(roots[3].lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE);
	UT_ASSERT_EQ(roots[3].validated_tail_lsn_exclusive, 0x1100);
	drop_plan(&plan);
}

/* A stale authority or a root that cannot take the terminal refuses while
 * IR is still held: nothing is sealed, released or published. */
UT_TEST(test_complete_refuses_before_any_publication)
{
	ClusterRecoveryFencePlan *plan = replayed_plan();
	uint16 thread;
	int detail;

	if (plan == NULL)
		return;
	authority_result[3] = CLUSTER_THREAD_AUTHORITY_OK + 1;
	UT_ASSERT_EQ(complete(plan, &thread, &detail), CLUSTER_RECOVERY_FENCE_COMPLETE_AUTHORITY);
	UT_ASSERT_EQ(thread, 3);
	UT_ASSERT(strstr(events, "seal-pin") == NULL && strstr(events, "finalize") == NULL);
	UT_ASSERT(plan->serial_held);
	UT_ASSERT_EQ(plan->serial_guards.count, 3);
	drop_plan(&plan);

	plan = replayed_plan();
	patch_refused[4] = true;
	UT_ASSERT_EQ(complete(plan, &thread, &detail), CLUSTER_RECOVERY_FENCE_COMPLETE_PATCH);
	UT_ASSERT_EQ(thread, 4);
	UT_ASSERT(strcmp(events, "patch:2;patch:3;patch:4;") == 0);
	UT_ASSERT(plan->serial_held);
	for (uint16 tid = 2; tid <= NODES; tid++)
		UT_ASSERT_EQ(roots[tid].lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED);
	drop_plan(&plan);

	/* Not a committed plan holding its serial set and pin. */
	plan = replayed_plan();
	plan->committed = false;
	UT_ASSERT_EQ(complete(plan, &thread, &detail), CLUSTER_RECOVERY_FENCE_COMPLETE_INVALID);
	plan->committed = true;
	plan->serial_held = false;
	UT_ASSERT_EQ(complete(plan, &thread, &detail), CLUSTER_RECOVERY_FENCE_COMPLETE_INVALID);
	UT_ASSERT_EQ(events[0], '\0');
	drop_plan(&plan);
}

/* A pin that cannot be sealed keeps IR; an unconfirmed IR release publishes
 * nothing. */
UT_TEST(test_complete_handoff_failures)
{
	ClusterRecoveryFencePlan *plan = replayed_plan();
	uint16 thread;
	int detail;

	if (plan == NULL)
		return;
	pin_seal_result = CLUSTER_WAL_PIN_STALE;
	UT_ASSERT_EQ(complete(plan, &thread, &detail), CLUSTER_RECOVERY_FENCE_COMPLETE_PIN_SEAL);
	UT_ASSERT_EQ(detail, CLUSTER_WAL_PIN_STALE);
	UT_ASSERT(strstr(events, "release-ir") == NULL);
	UT_ASSERT(plan->serial_held);
	drop_plan(&plan);

	plan = replayed_plan();
	ir_release_result = CLUSTER_RECOVERY_SERIAL_RELEASE_UNCONFIRMED;
	UT_ASSERT_EQ(complete(plan, &thread, &detail), CLUSTER_RECOVERY_FENCE_COMPLETE_IR_RELEASE);
	UT_ASSERT_EQ(detail, CLUSTER_RECOVERY_SERIAL_RELEASE_UNCONFIRMED);
	UT_ASSERT(strstr(events, "finalize") == NULL);
	UT_ASSERT(!plan->serial_held); /* surrendered, even while unconfirmed */
	drop_plan(&plan);
}

/* A finalizer that cannot complete an origin stops before the next one and
 * keeps the pin; an unconfirmed pin release is reported after all are
 * published. */
UT_TEST(test_complete_finalize_failures)
{
	ClusterRecoveryFencePlan *plan = replayed_plan();
	uint16 thread;
	int detail;

	if (plan == NULL)
		return;
	finalize_result[3] = CLUSTER_THREAD_ROOT_FINALIZE_RETRY;
	UT_ASSERT_EQ(complete(plan, &thread, &detail), CLUSTER_RECOVERY_FENCE_COMPLETE_FINALIZE);
	UT_ASSERT_EQ(thread, 3);
	UT_ASSERT_EQ(detail, CLUSTER_THREAD_ROOT_FINALIZE_RETRY);
	UT_ASSERT(strstr(events, "finalize:4") == NULL && strstr(events, "release-pin") == NULL);
	UT_ASSERT_EQ(roots[2].lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE);
	UT_ASSERT_EQ(roots[4].lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED);
	UT_ASSERT(plan->retention_pin != NULL);
	drop_plan(&plan);

	plan = replayed_plan();
	finalize_result[2] = CLUSTER_THREAD_ROOT_FINALIZE_BLOCKED;
	UT_ASSERT_EQ(complete(plan, &thread, &detail), CLUSTER_RECOVERY_FENCE_COMPLETE_FINALIZE);
	UT_ASSERT_EQ(thread, 2);
	drop_plan(&plan);

	plan = replayed_plan();
	pin_release_result = CLUSTER_WALR_RELEASE_UNCONFIRMED;
	UT_ASSERT_EQ(complete(plan, &thread, &detail), CLUSTER_RECOVERY_FENCE_COMPLETE_PIN_RELEASE);
	UT_ASSERT_EQ(roots[4].lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE);
	drop_plan(&plan);
}

/* PRE2's canonical reader checks the generation claim. Its CRC is not the
 * reconstructed legacy claim CRC; this boundary fixture supplies that already
 * qualified ROOT, while the merge module must retain exact identity checks.
 * Author: SqlRush <sqlrush@gmail.com> */
UT_TEST(test_complete_generation_claim_profile)
{
	for (unsigned unsealed = 0; unsealed < 2; unsealed++) {
		ClusterRecoveryFencePlan *plan = NULL;
		uint16 thread;
		int detail;

		fixture();
		for (uint16 tid = 1; tid <= NODES; tid++) {
			roots[tid].identity.thread_claim_crc32c ^= UINT32_C(1);
			UT_ASSERT(cluster_recovery_duty_key_valid_for_claim(&roots[tid].identity, true));
			UT_ASSERT(!cluster_recovery_duty_key_valid_v1(&roots[tid].identity));
			if (unsealed && tid != 1)
				make_open(tid);
		}
		expect_engaged(&plan);
		if (plan == NULL)
			return;
		hold(plan);
		plan->committed = false;
		UT_ASSERT(cluster_recovery_merge_commit_plan_nowait(plan));
		UT_ASSERT(cluster_recovery_merge_fence_plan_revalidate_nowait(plan));
		UT_ASSERT_EQ(complete(plan, &thread, &detail), CLUSTER_RECOVERY_FENCE_COMPLETE_OK);
		drop_plan(&plan);
	}
}

UT_TEST(test_legacy_profile_rejects_generation_claim_crc)
{
	ClusterRecoveryDutyKey expected;
	ClusterRecoveryDutyKey changed;

	fixture();
	expected = roots[2].identity;
	UT_ASSERT_EQ(cluster_recovery_duty_key_compare(&expected, &expected),
				 CLUSTER_RECOVERY_DUTY_COMPARE_EXACT);
	expected.thread_claim_crc32c ^= UINT32_C(1);
	UT_ASSERT_EQ(cluster_recovery_duty_key_compare(&expected, &expected),
				 CLUSTER_RECOVERY_DUTY_COMPARE_INVALID);
	UT_ASSERT_EQ(cluster_recovery_duty_key_compare_for_claim(&expected, &expected, true),
				 CLUSTER_RECOVERY_DUTY_COMPARE_EXACT);
	changed = expected;
	changed.origin_owner_incarnation++;
	UT_ASSERT_EQ(cluster_recovery_duty_key_compare_for_claim(&expected, &changed, true),
				 CLUSTER_RECOVERY_DUTY_COMPARE_DIFFERENT);
	changed = expected;
	changed.reserved60 = 1;
	UT_ASSERT_EQ(cluster_recovery_duty_key_compare_for_claim(&expected, &changed, true),
				 CLUSTER_RECOVERY_DUTY_COMPARE_INVALID);
}

/* The same original input refs survive completion, never its old IR/pin. */
static void
tx_sources(ClusterRecoveryFencePlan *plan, ClusterWalSourceRef *sources)
{
	memset(sources, 0, sizeof(*sources) * plan->origin_count);
	for (uint16 i = 0; i < plan->origin_count; i++) {
		sources[i].claim.identity = plan->origins[i].duty;
		sources[i].claim.database_incarnation = 17;
		sources[i].claim.max_config_generation = 19;
		sources[i].claim.claim_sha256[31] = plan->origins[i].origin_thread;
		sources[i].timeline = plan->origins[i].root_snapshot.checkpoint_tli;
	}
}

UT_TEST(test_tx_proof_owner_survives_only_complete_confirmed_handoff)
{
	ClusterRecoveryFencePlan *plan = replayed_plan();
	ClusterWalSourceRef sources[NODES];
	uint16 thread;
	int detail;

	if (plan == NULL) return;
	tx_sources(plan, sources);
	UT_ASSERT(cluster_recovery_merge_fence_plan_bind_tx_inputs_v1(plan, sources, plan->origin_count));
	UT_ASSERT(!cluster_recovery_merge_fence_plan_tx_current_v1(plan, &sources[0]));
	UT_ASSERT_EQ(complete(plan, &thread, &detail), CLUSTER_RECOVERY_FENCE_COMPLETE_OK);
	UT_ASSERT_EQ(formations_destroyed, 0);
	UT_ASSERT(cluster_recovery_merge_fence_plan_tx_current_v1(plan, NULL));
	for (uint16 i = 0; i < plan->origin_count; i++)
		UT_ASSERT(cluster_recovery_merge_fence_plan_tx_current_v1(plan, &sources[i]));
	UT_ASSERT(!cluster_recovery_merge_fence_plan_revalidate_nowait(plan));
	UT_ASSERT(!plan->serial_held && plan->serial_guards.count == 0 && plan->retention_pin == NULL);
	UT_ASSERT(!cluster_recovery_merge_fence_plan_bind_tx_inputs_v1(plan, sources, plan->origin_count));
	UT_ASSERT(cluster_recovery_merge_fence_plan_tx_current_v1(plan, &sources[0]));
	cluster_recovery_merge_fence_plan_destroy(&plan);
	UT_ASSERT(plan == NULL);
	UT_ASSERT_EQ(formations_destroyed, 3);

	/* COMPLETE must never make unbound or partly completed proof owners usable. */
	for (unsigned fault = 0; fault < 4; fault++) {
		plan = replayed_plan();
		tx_sources(plan, sources);
		if (fault != 0)
			UT_ASSERT(cluster_recovery_merge_fence_plan_bind_tx_inputs_v1(plan, sources, plan->origin_count));
		if (fault == 1) ir_release_result = CLUSTER_RECOVERY_SERIAL_RELEASE_UNCONFIRMED;
		if (fault == 2) finalize_result[3] = CLUSTER_THREAD_ROOT_FINALIZE_OK + 2;
		if (fault == 3) pin_release_result = CLUSTER_WALR_RELEASE_UNCONFIRMED;
		(void)complete(plan, &thread, &detail);
		UT_ASSERT(!cluster_recovery_merge_fence_plan_tx_current_v1(plan, &sources[0]));
		UT_ASSERT_EQ(formations_destroyed, 0);
		drop_plan(&plan);
	}
}

UT_TEST(test_tx_input_binding_is_atomic_exact_and_cannot_adopt_a_new_source)
{
	for (unsigned fault = 0; fault < 9; fault++) {
		ClusterRecoveryFencePlan *plan = replayed_plan();
		ClusterWalSourceRef sources[NODES];
		uint32 count;

		if (plan == NULL) return;
		tx_sources(plan, sources);
		count = plan->origin_count;
		switch (fault) {
		case 0: count--; break; /* missing final original origin */
		case 1: sources[count] = sources[0]; count++; break;
		case 2: sources[count - 1].claim.identity.origin_owner_incarnation++; break;
		case 3: sources[count - 1].timeline++; break;
		case 4: sources[count - 1].claim.database_incarnation = 0; break;
		case 5: memset(sources[count - 1].claim.claim_sha256, 0, 32); break;
		case 6: CurrentResourceOwner = (ResourceOwner)&sources; break;
		case 7: fence_owner_boot++; break;
		case 8: break; /* opaque owner alias */
		}
		UT_ASSERT(!cluster_recovery_merge_fence_plan_bind_tx_inputs_v1(plan,
			fault == 8 ? (const ClusterWalSourceRef *)plan : sources, count));
		UT_ASSERT(!plan->tx_inputs_bound);
		for (uint16 i = 0; i < plan->origin_count; i++) {
			ClusterWalSourceRef zero = {0};
			UT_ASSERT(memcmp(&plan->origins[i].tx_source, &zero, sizeof(zero)) == 0);
		}
		drop_plan(&plan);
	}
	for (unsigned fault = 0; fault < 6; fault++) {
		ClusterRecoveryFencePlan *plan = replayed_plan();
		ClusterWalSourceRef sources[NODES], changed;
		uint16 thread;
		int detail;

		if (plan == NULL) return;
		tx_sources(plan, sources);
		UT_ASSERT(cluster_recovery_merge_fence_plan_bind_tx_inputs_v1(plan, sources, plan->origin_count));
		UT_ASSERT_EQ(complete(plan, &thread, &detail), CLUSTER_RECOVERY_FENCE_COMPLETE_OK);
		changed = sources[0];
		switch (fault) {
		case 0: changed.claim.identity.origin_owner_incarnation++; break;
		case 1: changed.claim.identity.root_lineage_seq++; break;
		case 2: changed.claim.database_incarnation++; break;
		case 3: changed.claim.max_config_generation++; break;
		case 4: changed.claim.claim_sha256[0]++; break;
		case 5: changed.timeline++; break;
		}
		UT_ASSERT(!cluster_recovery_merge_fence_plan_tx_current_v1(plan, &changed));
		UT_ASSERT(cluster_recovery_merge_fence_plan_tx_current_v1(plan, &sources[0]));
		drop_plan(&plan);
	}
}

UT_TEST(test_tx_original_proof_or_resource_loss_is_sticky)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterRecoveryFencePlan *plan = replayed_plan();
		ClusterWalSourceRef sources[NODES];
		uint16 thread;
		int detail;
		ResourceOwner owner = CurrentResourceOwner;
		uint64 boot = fence_owner_boot;

		if (plan == NULL) return;
		tx_sources(plan, sources);
		UT_ASSERT(cluster_recovery_merge_fence_plan_bind_tx_inputs_v1(plan, sources, plan->origin_count));
		UT_ASSERT_EQ(complete(plan, &thread, &detail), CLUSTER_RECOVERY_FENCE_COMPLETE_OK);
		if (fault == 0) CurrentResourceOwner = (ResourceOwner)&sources;
		if (fault == 1) fence_owner_boot++;
		if (fault == 2) fence_current[4] = false; /* last origin, not only target */
		UT_ASSERT(!cluster_recovery_merge_fence_plan_tx_current_v1(plan, &sources[0]));
		CurrentResourceOwner = owner;
		fence_owner_boot = boot;
		fence_current[4] = true;
		UT_ASSERT(!cluster_recovery_merge_fence_plan_tx_current_v1(plan, &sources[0]));
		drop_plan(&plan);
	}
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(test_complete_publishes_every_origin);
	UT_RUN(test_complete_after_preflight_seal);
	UT_RUN(test_complete_refuses_before_any_publication);
	UT_RUN(test_complete_handoff_failures);
	UT_RUN(test_complete_finalize_failures);
	UT_RUN(test_complete_generation_claim_profile);
	UT_RUN(test_legacy_profile_rejects_generation_claim_crc);
	UT_RUN(test_tx_proof_owner_survives_only_complete_confirmed_handoff);
	UT_RUN(test_tx_input_binding_is_atomic_exact_and_cannot_adopt_a_new_source);
	UT_RUN(test_tx_original_proof_or_resource_loss_is_sticky);
	UT_DONE();
	return ut_failed_count != 0;
}
