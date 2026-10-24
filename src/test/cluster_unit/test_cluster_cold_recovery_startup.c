/*-------------------------------------------------------------------------
 *
 * test_cluster_cold_recovery_startup.c
 *	  Typed cold replay pass-1 orchestration and per-block redo decisions.
 *
 *	  The real cold plan core is linked.  ROOT lookup, source selection,
 *	  fence-plan origins, the pass-1 visitor scan and DATA observation are
 *	  fixtures (test_cluster_cold_recovery_startup_boundary.h); no WAL,
 *	  storage or lock is touched.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cold_recovery_startup.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include <setjmp.h>

#include "access/xlogreader.h"
#include "access/clog.h"
#include "access/multixact.h"
#include "access/xact.h"
#include "storage/standbydefs.h"
#include "catalog/storage_xlog.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_cold_recovery_census.h"
#include "cluster/cluster_cold_recovery_plan_internal.h"
#include "cluster/cluster_side_online_plan.h"
#include "cluster/cluster_page_cold_redo.h"
#include "storage/checksum.h"
#include "storage/checksum_impl.h"
#include "cluster/cluster_recovery_duty.h"
#include "cluster/cluster_side_online_owner.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_undo_recovery.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_wal_tail.h"
#include "utils/memutils.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool cluster_shared_config;

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# unexpected Assert: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

#define CLUSTER_COLD_NATIVE_DUTY_COMPARE 1
#include "test_cluster_cold_recovery_startup_boundary.h"
#include "test_cluster_recovery_duty_compare.inc"
#include "test_cluster_cold_source_completion.inc"
#include "test_cluster_cold_endpoints.h"

UT_TEST(test_prepare_rechecks_physical_profiles_of_every_original_origin)
{
	const ClusterControlRootResult faults[] = { CLUSTER_CONTROL_ROOT_PROFILE_UNSUPPORTED,
												   CLUSTER_CONTROL_ROOT_IO_ERROR,
												   CLUSTER_CONTROL_ROOT_STALE_TOKEN };

	for (unsigned node = 0; node < NROOTS; node++)
		for (unsigned fault = 0; fault < lengthof(faults); fault++)
			for (unsigned after = 0; after < 3; after++) {
				ClusterColdTypedV1 *typed;

				fixture();
				fence_count = 1;
				history_generation(3, 9, 0x40, 0x80);
				commit_ts_profile_result[node] = faults[fault];
				commit_ts_profile_fail_after[node] = after;
				typed = prepare(0x800);
				UT_ASSERT(typed->refusal != CLUSTER_COLD_OK);
				UT_ASSERT_EQ(typed->xid_next_after_redo, 0);
				if (after == 0) {
					UT_ASSERT_EQ(tt_base_captures, 0);
					UT_ASSERT_EQ(side_creates, 0);
				}
				cluster_cold_typed_destroy_v1(&typed);
			}
}

UT_TEST(test_prepare_checks_empty_physical_profiles_including_history_only_origin)
{
	ClusterColdTypedV1 *typed;

	fixture();
	fence_count = 1;
	history_generation(3, 9, 0x40, 0x80);
	history_generation(3, 10, 0x80, 0xc0);
	history_generation(2, 8, 0x20, 0x60);
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	UT_ASSERT_EQ(typed->replay_count, 2);
	UT_ASSERT_EQ(typed->participant_count, 5);
	for (unsigned node = 0; node < NROOTS; node++) {
		UT_ASSERT(commit_ts_profile_calls[node] > 0);
		UT_ASSERT_EQ(commit_ts_profile_calls[node], commit_ts_profile_calls[0]);
	}
	cluster_cold_typed_destroy_v1(&typed);
}

UT_TEST(test_endpoints_recheck_physical_profiles_and_discard_stale_inputs)
{
	const ClusterControlRootResult faults[] = { CLUSTER_CONTROL_ROOT_PROFILE_UNSUPPORTED,
												   CLUSTER_CONTROL_ROOT_IO_ERROR,
												   CLUSTER_CONTROL_ROOT_STALE_TOKEN };

	for (unsigned node = 0; node < NROOTS; node++)
		for (unsigned fault = 0; fault < lengthof(faults); fault++)
			for (unsigned published = 0; published < 2; published++) {
				ClusterColdTypedV1 *typed = endpoint_fixture(true);

				if (published)
					UT_ASSERT(endpoint_finish(typed));
				commit_ts_profile_result[node] = faults[fault];
				UT_ASSERT(!endpoint_finish(typed));
				UT_ASSERT(typed->refusal != CLUSTER_COLD_OK);
				UT_ASSERT_EQ(typed->xid_next_after_redo, 0);
				UT_ASSERT(!tt_after_captured);
				for (uint32 p = 0; p < typed->participant_count; p++) {
					UT_ASSERT_EQ(typed->xid_next_at_drain[p], 0);
					UT_ASSERT_EQ(typed->history_xid[p].next_xid, 0);
				}
				commit_ts_profile_result[node] = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
				UT_ASSERT(!endpoint_finish(typed));
				endpoint_destroy(&typed);
			}
}

UT_TEST(test_prepare_seals_own_and_fenced_generations)
{
	ClusterColdTypedV1 *typed;

	fixture();
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	UT_ASSERT_EQ(typed->participant_count, 3);
	UT_ASSERT_EQ(typed->own_participant, 0);
	UT_ASSERT_EQ(typed->participants[0].thread_id, 1);
	UT_ASSERT_EQ(typed->participants[2].thread_id, 3);
	UT_ASSERT_EQ(typed->participants[1].owner_incarnation, 12);
	UT_ASSERT_EQ(typed->participants[1].physical_lower, 0x100);
	UT_ASSERT_EQ(typed->participants[1].native_redo, 0x800);
	UT_ASSERT_EQ(typed->participants[1].tail_end, 0x2000);
	UT_ASSERT_EQ(typed->sources[2].claim.identity.origin_thread_id, 3);
	UT_ASSERT_EQ(typed->scanned_records, 6);
	UT_ASSERT_EQ(typed->observer.pages_observed, 3);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(typed->plan), 3);
	UT_ASSERT(!scan_foreign[1]);
	UT_ASSERT(scan_foreign[2] && scan_foreign[3]);
	cluster_cold_typed_destroy_v1(&typed);
	UT_ASSERT_NULL(typed);
	UT_ASSERT_EQ(contexts_alive, 0);
}

/*
 * The census's history-only generations follow the crashed ones as
 * participants with nothing to replay; they are scanned under the census
 * scope, which pass 1 releases before it returns.
 */
UT_TEST(test_execution_pin_includes_history_only_thread_once_after_census_release)
{
	ClusterColdTypedV1 *typed;

	fixture();
	fence_count = 1;
	history_generation(3, 9, 0x40, 0x80);
	history_generation(2, 8, 0x20, 0x60);
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	UT_ASSERT_EQ(typed->replay_count, 2);
	UT_ASSERT_EQ(typed->participant_count, 4);
	UT_ASSERT(cluster_cold_typed_acquire_serial_v1(typed, (ClusterRecoveryFencePlan *)1));
	UT_ASSERT_EQ(serial_input_calls, 1);
	UT_ASSERT_EQ(serial_input_count, 3);
	for (unsigned i = 0; i < 3; i++)
		UT_ASSERT_EQ(serial_input_threads[i], i + 1);
	UT_ASSERT_EQ(census_begins, 2);
	UT_ASSERT_EQ(census_releases, 2);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	cluster_cold_typed_destroy_v1(&typed);
}

UT_TEST(test_execution_pin_refuses_changed_census_before_retention)
{
	ClusterColdTypedV1 *typed;

	fixture();
	history_generation(2, 9, 0x40, 0x80);
	typed = prepare(0x800);
	census_input_fault = 4;
	census_fault_index = 11;
	UT_ASSERT(!cluster_cold_typed_acquire_serial_v1(typed, (ClusterRecoveryFencePlan *)1));
	UT_ASSERT_EQ(serial_input_calls, 0);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SOURCE_GAP);
	UT_ASSERT(!typed->history_xid[3].begun);
	cluster_cold_typed_destroy_v1(&typed);
}

UT_TEST(test_execution_pin_acquire_failure_is_sticky_and_discards_counters)
{
	ClusterColdTypedV1 *typed;

	fixture();
	history_generation(2, 9, 0x40, 0x80);
	typed = prepare(0x800);
	serial_input_ok = false;
	UT_ASSERT(!cluster_cold_typed_acquire_serial_v1(typed, (ClusterRecoveryFencePlan *)1));
	UT_ASSERT_EQ(serial_input_calls, 1);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SOURCE_GAP);
	UT_ASSERT(!typed->census_root_captured);
	UT_ASSERT(!typed->history_xid[3].begun);
	serial_input_ok = true;
	UT_ASSERT(!cluster_cold_typed_acquire_serial_v1(typed, (ClusterRecoveryFencePlan *)1));
	UT_ASSERT_EQ(serial_input_calls, 1);
	cluster_cold_typed_destroy_v1(&typed);
}

UT_TEST(test_prepare_takes_history_generations_from_the_census)
{
	ClusterColdTypedV1 *typed;

	fixture();
	history_generation(2, 9, 0x40, 0x80);
	typed = prepare(0x800);
	if (typed->refusal != CLUSTER_COLD_OK)
		printf("# refusal: %s\n", typed->refusal_detail);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	UT_ASSERT_EQ(typed->replay_count, 3);
	UT_ASSERT_EQ(typed->participant_count, 4);
	UT_ASSERT_EQ(typed->participants[3].thread_id, 2);
	UT_ASSERT_EQ(typed->participants[3].owner_incarnation, 9);
	UT_ASSERT_EQ(typed->participants[3].native_redo, 0x80);
	UT_ASSERT_EQ(typed->input_index[3], 11);
	UT_ASSERT_EQ(history_scans, 1);
	UT_ASSERT_EQ(history_scan_index, 11);
	UT_ASSERT_EQ(typed->scanned_records, 7);
	UT_ASSERT_EQ(census_begins, 1);
	UT_ASSERT_EQ(census_releases, 1);
	UT_ASSERT_NULL(typed->inputs);
	/* history only: still the three crashed generations' steps */
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(typed->plan), 3);
	UT_ASSERT_EQ(cluster_cold_plan_replay_record_count_v1(typed->plan, 3), 0);
	UT_ASSERT(typed->history_xid[3].begun && !typed->history_xid[3].failed);
	UT_ASSERT_EQ(typed->history_xid[3].source.thread_id, 2);
	UT_ASSERT_EQ(typed->history_xid[3].source.owner_incarnation, 9);
	UT_ASSERT_EQ(typed->history_xid[3].source.native_redo, 0x40);
	UT_ASSERT_EQ(typed->history_xid[3].last_end, 0x80);
	UT_ASSERT_EQ(typed->history_xid[3].records, 1);
	UT_ASSERT_EQ(typed->history_xid[0].next_xid, 0);
	UT_ASSERT_EQ(typed->xid_next_at_drain[3], 0);
	cluster_cold_typed_destroy_v1(&typed);
	UT_ASSERT_EQ(census_releases, 1);
}

UT_TEST(test_census_root_capture_failure_discards_history)
{
	ClusterColdTypedV1 *typed;

	fixture();
	history_generation(2, 9, 0x40, 0x80);
	census_token_result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SOURCE_GAP);
	UT_ASSERT(!typed->census_root_captured);
	UT_ASSERT_EQ(typed->history_xid[3].next_xid, 0);
	UT_ASSERT_EQ(census_releases, 1);
	UT_ASSERT_NULL(typed->inputs);
	cluster_cold_typed_destroy_v1(&typed);
}

UT_TEST(test_census_recheck_preserves_original_history_inputs_only)
{
	ClusterColdTypedV1 *typed;
	ClusterColdXidProgressV1 saved;

	fixture();
	history_generation(2, 9, 0x40, 0x80);
	history_generation(3, 8, 0x20, 0x60);
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	UT_ASSERT(typed->census_root_captured);
	UT_ASSERT_EQ(memcmp(&typed->census_root, &census_token, sizeof(census_token)), 0);
	saved = typed->history_xid[3];
	UT_ASSERT(cluster_cold_typed_recheck_inputs_v1(typed));
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	UT_ASSERT_EQ(census_begins, 2);
	UT_ASSERT_EQ(census_releases, 2);
	UT_ASSERT_EQ(census_token_reads, 2);
	UT_ASSERT_NULL(typed->inputs);
	UT_ASSERT_EQ(memcmp(&saved, &typed->history_xid[3], sizeof(saved)), 0);
	UT_ASSERT_EQ(typed->participants[3].native_redo, 0x80);
	UT_ASSERT_EQ(typed->xid_next_at_drain[3], 0);
	UT_ASSERT(!typed->source_drained[3]);
	UT_ASSERT_EQ(source_sync_calls, 0);
	cluster_cold_typed_destroy_v1(&typed);
}

UT_TEST(test_census_recheck_rejects_every_changed_root_token_byte)
{
	for (size_t byte = 0; byte < sizeof(census_token); byte++) {
		ClusterColdTypedV1 *typed;

		fixture();
		history_generation(2, 9, 0x40, 0x80);
		typed = prepare(0x800);
		((unsigned char *)&census_token)[byte] ^= 1;
		UT_ASSERT(!cluster_cold_typed_recheck_inputs_v1(typed));
		UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SOURCE_GAP);
		UT_ASSERT(!typed->census_root_captured);
		UT_ASSERT_EQ(typed->history_xid[3].next_xid, 0);
		UT_ASSERT_EQ(census_releases, 2);
		UT_ASSERT_NULL(typed->inputs);
		UT_ASSERT(!cluster_cold_typed_recheck_inputs_v1(typed));
		UT_ASSERT_EQ(census_begins, 2);
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_census_recheck_keeps_current_closed_history)
{
	ClusterColdTypedV1 *typed;

	fixture();
	fence_count = 1;
	history_generation(3, 13, 0x40, 0x80);
	census_history_current = true;
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	UT_ASSERT_EQ(typed->replay_count, 2);
	UT_ASSERT_EQ(typed->participant_count, 3);
	UT_ASSERT(typed->history_xid[2].begun);
	UT_ASSERT(cluster_cold_typed_recheck_inputs_v1(typed));
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	UT_ASSERT_EQ(census_releases, 2);
	UT_ASSERT_EQ(typed->participants[2].native_redo, 0x80);
	UT_ASSERT_EQ(typed->xid_next_at_drain[2], 0);
	cluster_cold_typed_destroy_v1(&typed);
}

UT_TEST(test_census_recheck_rejects_changed_history_metadata)
{
	for (int fault = 1; fault <= 9; fault++) {
		ClusterColdTypedV1 *typed;

		fixture();
		history_generation(2, 9, 0x40, 0x80);
		typed = prepare(0x800);
		census_input_fault = fault;
		UT_ASSERT(!cluster_cold_typed_recheck_inputs_v1(typed));
		UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SOURCE_GAP);
		UT_ASSERT_EQ(typed->history_xid[3].next_xid, 0);
		UT_ASSERT_EQ(census_releases, 2);
		UT_ASSERT_NULL(typed->inputs);
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_census_recheck_scope_failures_discard_provisional_group)
{
	for (int fault = 0; fault < 3; fault++) {
		ClusterColdTypedV1 *typed;

		fixture();
		history_generation(2, 9, 0x40, 0x80);
		typed = prepare(0x800);
		if (fault == 0)
			census_begin_result = CLUSTER_CONTROL_ROOT_IO_ERROR;
		else if (fault == 1)
			census_token_result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		else
			census_revalidate_result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		UT_ASSERT(!cluster_cold_typed_recheck_inputs_v1(typed));
		UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SOURCE_GAP);
		UT_ASSERT(!typed->census_root_captured);
		UT_ASSERT_EQ(typed->history_xid[3].next_xid, 0);
		UT_ASSERT_EQ(census_releases, fault == 0 ? 1 : 2);
		UT_ASSERT_NULL(typed->inputs);
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_census_recheck_refuses_missing_or_execution_state)
{
	UT_ASSERT(!cluster_cold_typed_recheck_inputs_v1(NULL));
	for (int fault = 0; fault < 5; fault++) {
		ClusterColdTypedV1 *typed;
		ClusterColdPlanV1 *plan;

		fixture();
		history_generation(2, 9, 0x40, 0x80);
		typed = prepare(0x800);
		plan = typed->plan;
		if (fault == 0)
			typed->census_root_captured = false;
		else if (fault == 1)
			typed->plan = NULL;
		else if (fault == 2)
			cluster_cold_replay_window_enter_v1();
		else if (fault == 3)
			typed->participant_count = CLUSTER_COLD_MAX_PARTICIPANTS + 1;
		else
			typed->refusal = CLUSTER_COLD_SOURCE_GAP;
		UT_ASSERT(!cluster_cold_typed_recheck_inputs_v1(typed));
		UT_ASSERT_EQ(census_begins, 1);
		UT_ASSERT_EQ(typed->history_xid[3].next_xid, 0);
		typed->plan = plan;
		typed->participant_count = 4;
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

/* Every census refusal stops pass 1 before the plan is built or scanned,
 * and the census scope is released. */
UT_TEST(test_prepare_refuses_what_the_census_refuses)
{
	ClusterColdTypedV1 *typed;

	fixture();
	census_begin_result = CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_PARTICIPANT_INVALID);
	UT_ASSERT(strstr(typed->refusal_detail, "participant census unavailable") != NULL);
	UT_ASSERT_NULL(typed->plan);
	cluster_cold_typed_destroy_v1(&typed);
	UT_ASSERT_EQ(census_releases, 0);

	fixture();
	census_select_result = CLUSTER_COLD_CENSUS_LIVE_WRITER;
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_PARTICIPANT_INVALID);
	UT_ASSERT(strstr(typed->refusal_detail, "still OPEN (input 3") != NULL);
	UT_ASSERT_NULL(typed->plan);
	cluster_cold_typed_destroy_v1(&typed);
	UT_ASSERT_EQ(census_releases, 1);

	/* a crashed thread the fence plan leaves out would be skipped */
	fixture();
	census_cover_result = CLUSTER_COLD_CENSUS_UNCOVERED;
	census_bad_thread = 4;
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_PARTICIPANT_INVALID);
	UT_ASSERT(strstr(typed->refusal_detail, "missing from the fence plan") != NULL);
	UT_ASSERT(strstr(typed->refusal_detail, "thread 4") != NULL);
	UT_ASSERT_NULL(typed->plan);
	cluster_cold_typed_destroy_v1(&typed);
	UT_ASSERT_EQ(census_releases, 1);

	/* the census and the ROOT source disagree on a crashed cut */
	fixture();
	census_redo_skew = 0x10;
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_PARTICIPANT_INVALID);
	UT_ASSERT(strstr(typed->refusal_detail, "thread 2 census cut differs") != NULL);
	UT_ASSERT_NULL(typed->plan);
	cluster_cold_typed_destroy_v1(&typed);
	UT_ASSERT_EQ(census_releases, 1);

	/* the scope changed while the history was read */
	fixture();
	history_generation(3, 7, 0x40, 0x80);
	census_revalidate_result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SOURCE_GAP);
	UT_ASSERT(strstr(typed->refusal_detail, "census changed") != NULL);
	UT_ASSERT_EQ(typed->history_xid[3].next_xid, 0);
	UT_ASSERT_NULL(typed->inputs);
	UT_ASSERT_EQ(census_releases, 1);
	cluster_cold_typed_destroy_v1(&typed);
	UT_ASSERT_EQ(census_releases, 1);
}

UT_TEST(test_prepare_refuses_unsealed_own_generation)
{
	ClusterColdTypedV1 *typed;

	fixture();
	roots[1].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED + 1;
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_PARTICIPANT_INVALID);
	UT_ASSERT(strstr(typed->refusal_detail, "not sealed") != NULL);
	UT_ASSERT_NULL(typed->plan);
	cluster_cold_typed_destroy_v1(&typed);

	fixture();
	roots[1].root_flags &= ~CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID;
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_PARTICIPANT_INVALID);
	cluster_cold_typed_destroy_v1(&typed);

	/* The root found for this node is a valid claim of another thread. */
	fixture();
	{
		ClusterWalThreadClaim claim;

		roots[1].identity.origin_thread_id = 2;
		roots[1].identity.origin_node_id = 1;
		cluster_wal_thread_claim_fill(&claim, 2, 1, 42);
		roots[1].identity.thread_claim_crc32c = claim.crc;
	}
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_PARTICIPANT_INVALID);
	UT_ASSERT(strstr(typed->refusal_detail, "own thread 1 root") != NULL);
	UT_ASSERT_NULL(typed->plan);
	cluster_cold_typed_destroy_v1(&typed);
}

UT_TEST(test_prepare_refuses_restart_redo_mismatch)
{
	ClusterColdTypedV1 *typed;

	fixture();
	typed = prepare(0x900);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_PARTICIPANT_INVALID);
	UT_ASSERT(strstr(typed->refusal_detail, "restart redo") != NULL);
	cluster_cold_typed_destroy_v1(&typed);
}

/*
 * The own generation replayed is the one this node restarts from: the
 * bootstrap-validated restart input must name the sealed root's identity
 * and claim, not only lead to the same redo.
 */
UT_TEST(test_prepare_binds_own_generation_to_restart_input)
{
	ClusterColdTypedV1 *typed;

	fixture();
	restart_ref_present = false;
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_PARTICIPANT_INVALID);
	UT_ASSERT(strstr(typed->refusal_detail, "restart input") != NULL);
	UT_ASSERT_NULL(typed->plan);
	cluster_cold_typed_destroy_v1(&typed);

	fixture();
	restart_ref.claim.identity.origin_owner_incarnation++; /* a later generation */
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_PARTICIPANT_INVALID);
	UT_ASSERT(strstr(typed->refusal_detail, "restart input") != NULL);
	cluster_cold_typed_destroy_v1(&typed);

	fixture();
	restart_ref.claim.claim_sha256[31] ^= 1; /* same identity, another claim */
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_PARTICIPANT_INVALID);
	UT_ASSERT(strstr(typed->refusal_detail, "restart input") != NULL);
	cluster_cold_typed_destroy_v1(&typed);

	fixture();
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	cluster_cold_typed_destroy_v1(&typed);
}

UT_TEST(test_prepare_refuses_unproven_origin_source)
{
	ClusterColdTypedV1 *typed;

	fixture();
	source_result[3] = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_PARTICIPANT_INVALID);
	UT_ASSERT(strstr(typed->refusal_detail, "thread 3 native checkpoint input") != NULL);
	cluster_cold_typed_destroy_v1(&typed);

	fixture();
	native_redo[2] = 0x80; /* below the physical lower */
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_PARTICIPANT_INVALID);
	cluster_cold_typed_destroy_v1(&typed);

	fixture();
	fence_count = 0;
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_PARTICIPANT_INVALID);
	cluster_cold_typed_destroy_v1(&typed);
}

UT_TEST(test_prepare_reports_scan_and_seal_refusals)
{
	ClusterColdTypedV1 *typed;

	fixture();
	scan_result[2] = CLUSTER_COLD_SOURCE_GAP;
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SOURCE_GAP);
	UT_ASSERT(strstr(typed->refusal_detail, "pass-1 scan refused: source gap; thread 2 record "
											"0/1234")
			  != NULL);
	cluster_cold_typed_destroy_v1(&typed);

	fixture();
	data_token = 3; /* DATA version not on any page chain */
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_ANCESTOR_MISSING);
	UT_ASSERT(strstr(typed->refusal_detail, "plan seal refused: ancestor missing; thread 1 record "
											"0/800; page 1663/5/100 fork 0 block 1")
			  != NULL);
	cluster_cold_typed_destroy_v1(&typed);
}

UT_TEST(test_redo_block_decisions)
{
	ClusterColdStepV1 step;
	ClusterColdRedoBlockV1 out;
	XLogReaderState reader;

	memset(&reader, 0, sizeof(reader));
	reader.ReadRecPtr = 0x500;
	UT_ASSERT(cluster_cold_redo_block_decision_v1(&reader, 0, &out));
	UT_ASSERT_EQ(out.action, CLUSTER_COLD_REDO_NATIVE);

	memset(&step, 0, sizeof(step));
	step.read_rec_ptr = 0x500;
	step.blocks[0].verdict = CLUSTER_COLD_BLOCK_SKIP;
	step.blocks[0].expected_kind = CLUSTER_COLD_DATA_INVALID;
	step.blocks[1].verdict = CLUSTER_COLD_BLOCK_APPLY_DELTA;
	step.blocks[1].expected_kind = CLUSTER_COLD_DATA_PRESENT;
	step.blocks[1].expected_before.mutation_token = 5;
	step.blocks[1].result.mutation_token = 6;
	step.blocks[2].verdict = CLUSTER_COLD_BLOCK_APPLY_IMAGE;
	cluster_cold_redo_step_enter_v1(&step);
	UT_ASSERT(cluster_cold_redo_block_decision_v1(&reader, 0, &out));
	UT_ASSERT_EQ(out.action, CLUSTER_COLD_REDO_SKIP);
	UT_ASSERT_EQ(out.expected_kind, CLUSTER_COLD_DATA_INVALID);
	UT_ASSERT(cluster_cold_redo_block_decision_v1(&reader, 1, &out));
	UT_ASSERT_EQ(out.action, CLUSTER_COLD_REDO_APPLY);
	/* the consumer must check the exact before-state, not accept anything */
	UT_ASSERT_EQ(out.expected_kind, CLUSTER_COLD_DATA_PRESENT);
	UT_ASSERT_EQ(out.expected_before.mutation_token, 5);
	UT_ASSERT_EQ(out.result.mutation_token, 6);
	UT_ASSERT(cluster_cold_redo_block_decision_v1(&reader, 2, &out));
	UT_ASSERT_EQ(out.action, CLUSTER_COLD_REDO_APPLY);
	UT_ASSERT(cluster_cold_redo_block_decision_v1(&reader, 3, &out));
	UT_ASSERT_EQ(out.action, CLUSTER_COLD_REDO_NATIVE);
	reader.ReadRecPtr = 0x600;
	UT_ASSERT(!cluster_cold_redo_block_decision_v1(&reader, 0, &out));
	reader.ReadRecPtr = 0x500;
	UT_ASSERT(!cluster_cold_redo_block_decision_v1(&reader, XLR_MAX_BLOCK_ID + 1, &out));
	cluster_cold_redo_step_leave_v1();
	UT_ASSERT(cluster_cold_redo_block_decision_v1(&reader, 0, &out));
	UT_ASSERT_EQ(out.action, CLUSTER_COLD_REDO_NATIVE);
}

/* Shared mode takes only the typed path; everything else keeps native
 * single-stream recovery (the unshared multi-generation case is refused
 * earlier, by the merge preflight). */
UT_TEST(test_route_shared_cold_merge_only_typed)
{
	UT_ASSERT_EQ(cluster_cold_route_v1(true, true), CLUSTER_COLD_ROUTE_TYPED);
	UT_ASSERT_EQ(cluster_cold_route_v1(true, false), CLUSTER_COLD_ROUTE_NATIVE);
	UT_ASSERT_EQ(cluster_cold_route_v1(false, false), CLUSTER_COLD_ROUTE_NATIVE);
	UT_ASSERT_EQ(cluster_cold_route_v1(false, true), CLUSTER_COLD_ROUTE_REFUSE);
}

static ClusterColdTypedV1 *
sealed_with_steps(bool all_skip)
{
	fixture();
	data_token = all_skip ? 6 : 5;
	return prepare(0x800);
}

/* Pass 2 may start only with every consumer wired, and never applies a page
 * record without the per-block redo consultation. */
UT_TEST(test_ready_requires_every_consumer_before_ir)
{
	ClusterColdHandshakeV1 none = { 0 };
	ClusterColdHandshakeV1 all = { .redo_block_hook = true,
								   .participant_census = true,
								   .side_owners = true,
								   .completion_publish = true,
								   .restartpoint_hold = true };
	ClusterColdHandshakeV1 no_hook = all;
	ClusterColdHandshakeV1 no_side = all;
	ClusterColdHandshakeV1 no_hold = all;
	ClusterColdTypedV1 *typed = sealed_with_steps(false);
	char reason[256];

	no_hook.redo_block_hook = false;
	no_side.side_owners = false;
	no_hold.restartpoint_hold = false;
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	UT_ASSERT(!cluster_cold_typed_ready_v1(typed, &none, reason, sizeof(reason)));
	UT_ASSERT(strstr(reason, "redo-block consultation") != NULL);
	UT_ASSERT(strstr(reason, "participant census") != NULL);
	UT_ASSERT(strstr(reason, "side owners") != NULL);
	UT_ASSERT(strstr(reason, "completion publication") != NULL);
	UT_ASSERT(!cluster_cold_typed_ready_v1(typed, &no_hook, reason, sizeof(reason)));
	UT_ASSERT(strstr(reason, "3 page records need") != NULL);
	UT_ASSERT(!cluster_cold_typed_ready_v1(typed, &no_side, reason, sizeof(reason)));
	/* The cut must stay fixed: no restartpoint may move a participant. */
	UT_ASSERT(!cluster_cold_typed_ready_v1(typed, &no_hold, reason, sizeof(reason)));
	UT_ASSERT(strstr(reason, "restartpoint") != NULL);
	UT_ASSERT(cluster_cold_typed_ready_v1(typed, &all, reason, sizeof(reason)));
	cluster_cold_typed_destroy_v1(&typed);

	/* Fully skipped records need no hook, yet the other consumers remain. */
	typed = sealed_with_steps(true);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	UT_ASSERT(!cluster_cold_typed_ready_v1(typed, &no_hook, reason, sizeof(reason)));
	UT_ASSERT(strstr(reason, "page records need") == NULL);
	UT_ASSERT(cluster_cold_typed_ready_v1(typed, &all, reason, sizeof(reason)));
	UT_ASSERT(!cluster_cold_typed_ready_v1(NULL, &all, reason, sizeof(reason)));
	cluster_cold_typed_destroy_v1(&typed);

	/* A plan whose seal was refused keeps its (failed) plan: never ready. */
	fixture();
	data_token = 3;
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_ANCESTOR_MISSING);
	UT_ASSERT_NOT_NULL(typed->plan);
	UT_ASSERT(!cluster_cold_typed_ready_v1(typed, &all, reason, sizeof(reason)));
	UT_ASSERT(strstr(reason, "no sealed typed cold plan") != NULL);
	cluster_cold_typed_destroy_v1(&typed);
}

/*
 * Relations with SPACE effects are checked by the SPACE owner in pass 1, in
 * the namespace every participant shares, and pass 2 then needs the cold
 * SPACE install too.
 */
UT_TEST(test_space_effects_checked_and_need_the_space_owner)
{
	ClusterColdHandshakeV1 all = { .redo_block_hook = true,
								   .participant_census = true,
								   .side_owners = true,
								   .completion_publish = true,
								   .restartpoint_hold = true };
	ClusterColdHandshakeV1 with_space = all;
	ClusterColdTypedV1 *typed;
	char reason[256];

	with_space.space_owner = true;
	fixture();
	data_token = 5;
	scan_space = true;
	space_checks = 0;
	typed = prepare(0x800);
	if (typed->refusal != CLUSTER_COLD_OK)
		printf("# refusal: %s\n", typed->refusal_detail);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	UT_ASSERT_EQ(space_checks, 1);
	UT_ASSERT_EQ(space_check_count, 1);
	UT_ASSERT_EQ(space_check_namespace.system_identifier, 9);
	UT_ASSERT_EQ(space_check_namespace.database_incarnation, 5);
	UT_ASSERT_EQ(space_check_namespace.storage_uuid[0], 1);
	UT_ASSERT(!cluster_cold_typed_ready_v1(typed, &all, reason, sizeof(reason)));
	UT_ASSERT(strstr(reason, "1 SPACE relations need the cold SPACE owner") != NULL);
	UT_ASSERT(cluster_cold_typed_ready_v1(typed, &with_space, reason, sizeof(reason)));
	cluster_cold_typed_destroy_v1(&typed);

	/* Generations of different database incarnations share no namespace. */
	fixture();
	data_token = 5;
	database_incarnation[2] = 6;
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_PARTICIPANT_INVALID);
	UT_ASSERT(strstr(typed->refusal_detail, "database incarnation") != NULL);
	cluster_cold_typed_destroy_v1(&typed);

	/* Nor do generations of another cluster or another storage. */
	fixture();
	roots[3].identity.system_identifier = 8;
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_PARTICIPANT_INVALID);
	UT_ASSERT(strstr(typed->refusal_detail, "thread 3 claims system 8") != NULL);
	cluster_cold_typed_destroy_v1(&typed);
	fixture();
	roots[2].identity.storage_uuid[15] = 2;
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_PARTICIPANT_INVALID);
	UT_ASSERT(strstr(typed->refusal_detail, "another storage uuid") != NULL);
	cluster_cold_typed_destroy_v1(&typed);
	scan_space = false;
}

/* PRE2: a cold crash that needs several threads merged is refused outside
 * the shared profile, before any fence, claim or replay; single-stream and
 * warm decisions pass through unchanged. */
UT_TEST(test_unshared_multi_thread_cold_merge_refused)
{
	UT_ASSERT_EQ(cluster_recovery_merge_profile_gate(CLUSTER_MERGE_ENGAGE, false),
				 CLUSTER_MERGE_REFUSE_UNSHARED);
	UT_ASSERT_EQ(cluster_recovery_merge_profile_gate(CLUSTER_MERGE_ENGAGE, true),
				 CLUSTER_MERGE_ENGAGE);
	UT_ASSERT_EQ(cluster_recovery_merge_profile_gate(CLUSTER_MERGE_NO_NO_CANDIDATES, false),
				 CLUSTER_MERGE_NO_NO_CANDIDATES);
	UT_ASSERT_EQ(cluster_recovery_merge_profile_gate(CLUSTER_MERGE_NO_DISABLED, false),
				 CLUSTER_MERGE_NO_DISABLED);
	UT_ASSERT_EQ(cluster_recovery_merge_profile_gate(CLUSTER_MERGE_NO_NOT_COLD, false),
				 CLUSTER_MERGE_NO_NOT_COLD);
	UT_ASSERT_EQ(cluster_cold_route_v1(false, false), CLUSTER_COLD_ROUTE_NATIVE);
}

/*
 * The restartpoint owner holds own-thread checkpoints while typed cold
 * replay runs; the window is visible only between enter and leave.
 */
UT_TEST(test_cold_replay_window_tracks_pass_two)
{
	UT_ASSERT(!cluster_cold_replay_window_active_v1());
	cluster_cold_replay_window_enter_v1();
	UT_ASSERT(cluster_cold_replay_window_active_v1());
	cluster_cold_replay_window_leave_v1();
	UT_ASSERT(!cluster_cold_replay_window_active_v1());
}

/* A formatted page with a version token and a correct data checksum. */
static void
checksummed_page(char *page, BlockNumber blkno, uint64 token)
{
	PageHeader header = (PageHeader)page;

	memset(page, 0, BLCKSZ);
	header->pd_lower = SizeOfPageHeaderData;
	header->pd_upper = BLCKSZ - 64;
	header->pd_special = BLCKSZ;
	header->pd_block_scn = token;
	memset(page + BLCKSZ - 64, 0x5a, 64);
	header->pd_checksum = pg_checksum_page(page, blkno);
}

/*
 * With data checksums the observer compares the stored checksum itself, so
 * ignore_checksum_failure cannot turn a torn page into proven content (or an
 * intact one into an unproven page): only a mismatch reads INVALID, and a
 * match proves the content.
 */
UT_TEST(test_checksum_decides_torn_pages)
{
	PGAlignedBlock block;
	ClusterColdDataV1 out;

	checksummed_page(block.data, 7, 77);
	UT_ASSERT(cluster_cold_classify_page_v1(block.data, 7, true, true, &out));
	UT_ASSERT_EQ(out.kind, CLUSTER_COLD_DATA_PRESENT);
	UT_ASSERT_EQ(out.version.mutation_token, 77);
	UT_ASSERT_EQ(out.flags, CLUSTER_COLD_DATA_FLAG_CONTENT_VERIFIED);

	/* Torn body under an intact header: the native check may have passed it
	 * (ignore_checksum_failure); the checksum still refuses it. */
	block.data[BLCKSZ - 10] ^= 0x01;
	UT_ASSERT(cluster_cold_classify_page_v1(block.data, 7, true, true, &out));
	UT_ASSERT_EQ(out.kind, CLUSTER_COLD_DATA_INVALID);
	UT_ASSERT_EQ(out.flags, 0);

	/* The checksum covers the block number. */
	checksummed_page(block.data, 7, 77);
	UT_ASSERT(cluster_cold_classify_page_v1(block.data, 8, true, true, &out));
	UT_ASSERT_EQ(out.kind, CLUSTER_COLD_DATA_INVALID);

	/* Without data checksums only the header is known. */
	block.data[BLCKSZ - 10] ^= 0x01;
	UT_ASSERT(cluster_cold_classify_page_v1(block.data, 7, true, false, &out));
	UT_ASSERT_EQ(out.kind, CLUSTER_COLD_DATA_PRESENT);
	UT_ASSERT_EQ(out.flags, 0);
}

static void
bracket_apply(XLogReaderState *record, void *arg)
{
	(void)arg;
	bracket_note('a', 1, record);
}

/*
 * The native block consumer requires every applied record to be bracketed
 * by its begin/end inside the published step: begin and end re-read this
 * record's verdicts, and redo in between must see them too.
 */
UT_TEST(test_apply_step_brackets_native_redo)
{
	ClusterColdStepV1 step;
	ClusterColdRedoBlockV1 out;
	XLogReaderState reader;

	memset(&reader, 0, sizeof(reader));
	reader.ReadRecPtr = 0x700;
	memset(&step, 0, sizeof(step));
	step.read_rec_ptr = 0x700;
	step.blocks[1].verdict = CLUSTER_COLD_BLOCK_APPLY_DELTA;
	step.blocks[1].expected_kind = CLUSTER_COLD_DATA_PRESENT;
	memset(bracket_order, 0, sizeof(bracket_order));
	bracket_calls = 0;
	cluster_cold_apply_step_v1(&step, &reader, bracket_apply, NULL);
	UT_ASSERT_STR_EQ(bracket_order, "bae");
	UT_ASSERT(bracket_saw_step[0]);
	UT_ASSERT(bracket_saw_step[1]);
	UT_ASSERT(bracket_saw_step[2]);
	/* The step is no longer published afterwards. */
	UT_ASSERT(cluster_cold_redo_block_decision_v1(&reader, 1, &out));
	UT_ASSERT_EQ(out.action, CLUSTER_COLD_REDO_NATIVE);
}

/*
 * Pass-1 DATA observation of one page read from storage: an all-zero page
 * is a proven unformatted page; any other new or unverifiable page is
 * INVALID; a formatted page carries its version token, and its content is
 * proven only when an enforced checksum covered it.
 */
UT_TEST(test_observed_page_classification)
{
	PGAlignedBlock block;
	ClusterColdDataV1 out;

	memset(block.data, 0, BLCKSZ);
	UT_ASSERT(cluster_cold_classify_page_v1(block.data, 3, true, true, &out));
	UT_ASSERT_EQ(out.kind, CLUSTER_COLD_DATA_UNFORMATTED);
	UT_ASSERT_EQ(out.flags, CLUSTER_COLD_DATA_FLAG_CONTENT_VERIFIED);
	UT_ASSERT_EQ(out.version.mutation_token, 0);

	block.data[BLCKSZ - 1] = 1; /* new header, non-zero body: torn */
	UT_ASSERT(cluster_cold_classify_page_v1(block.data, 3, true, true, &out));
	UT_ASSERT_EQ(out.kind, CLUSTER_COLD_DATA_INVALID);
	UT_ASSERT_EQ(out.flags, 0);

	checksummed_page(block.data, 3, 77);
	UT_ASSERT(cluster_cold_classify_page_v1(block.data, 3, false, true, &out));
	UT_ASSERT_EQ(out.kind, CLUSTER_COLD_DATA_INVALID);
	UT_ASSERT_EQ(out.flags, 0);

	UT_ASSERT(cluster_cold_classify_page_v1(block.data, 3, true, true, &out));
	UT_ASSERT_EQ(out.kind, CLUSTER_COLD_DATA_PRESENT);
	UT_ASSERT_EQ(out.version.mutation_token, 77);
	UT_ASSERT_EQ(out.flags, CLUSTER_COLD_DATA_FLAG_CONTENT_VERIFIED);
	UT_ASSERT(cluster_cold_classify_page_v1(block.data, 3, true, false, &out));
	UT_ASSERT_EQ(out.kind, CLUSTER_COLD_DATA_PRESENT);
	UT_ASSERT_EQ(out.flags, 0);

	/* A formatted page without a version token cannot be placed. */
	checksummed_page(block.data, 3, 0);
	UT_ASSERT(!cluster_cold_classify_page_v1(block.data, 3, true, true, &out));
}

/*
 * The pass-1 budget is cluster.cold_recovery_plan_memory (kB, default
 * 4 GiB).  Exhausting it refuses before anything is modified, names the
 * budget and points at the parameter.
 */
UT_TEST(test_plan_memory_budget_parameter)
{
	ClusterColdTypedV1 *typed;
	int saved = cluster_cold_recovery_plan_memory;

	UT_ASSERT_EQ(CLUSTER_COLD_PLAN_MEMORY_DEFAULT_KB, Min(4 * 1024 * 1024, MAX_KILOBYTES));
	UT_ASSERT_EQ(cluster_cold_recovery_plan_memory, CLUSTER_COLD_PLAN_MEMORY_DEFAULT_KB);
	UT_ASSERT_EQ(CLUSTER_COLD_PLAN_MEMORY_MIN_KB, 1024);
	UT_ASSERT(strstr(cluster_cold_refusal_hint_v1(CLUSTER_COLD_CAPACITY),
					 "cluster.cold_recovery_plan_memory")
			  != NULL);

	fixture();
	cluster_cold_recovery_plan_memory = 1;
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_CAPACITY);
	UT_ASSERT(strstr(typed->refusal_detail, "1 kB") != NULL);
	cluster_cold_typed_destroy_v1(&typed);
	cluster_cold_recovery_plan_memory = saved;
}

/* Pages nothing can rebuild get a hint naming why; others the generic one. */
UT_TEST(test_refusal_hints)
{
	UT_ASSERT(strstr(cluster_cold_refusal_hint_v1(CLUSTER_COLD_CONTENT_UNPROVEN), "checksums")
			  != NULL);
	UT_ASSERT(strstr(cluster_cold_refusal_hint_v1(CLUSTER_COLD_ANCHOR_MISSING), "full-page image")
			  != NULL);
	UT_ASSERT(strstr(cluster_cold_refusal_hint_v1(CLUSTER_COLD_HISTORY_GAP), "typed cold plan")
			  != NULL);
	UT_ASSERT(strstr(cluster_cold_refusal_hint_v1(CLUSTER_COLD_OK), "typed cold plan") != NULL);
	UT_ASSERT(strstr(cluster_cold_refusal_hint_v1(CLUSTER_COLD_SPACE_INVALID), "SPACE changes")
			  != NULL);
	UT_ASSERT(strstr(cluster_cold_refusal_hint_v1(CLUSTER_COLD_SPACE_REFUSED), "SPACE pages")
			  != NULL);
	UT_ASSERT(strstr(cluster_cold_refusal_hint_v1(CLUSTER_COLD_IDENTITY_MISSING), "SPACE identity")
			  != NULL);
}

static bool
sync_sources(ClusterColdTypedV1 *typed)
{
	return cluster_cold_typed_sync_sources_v1(typed, (ClusterRecoveryFencePlan *)&fixture_context);
}

UT_TEST(test_source_barrier_all_selected_generations)
{
	ClusterColdTypedV1 *typed;

	fixture();
	history_generation(2, 9, 0x40, 0x80);
	history_generation(2, 10, 0x80, 0xc0);
	history_generation(3, 8, 0x40, 0x90);
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	UT_ASSERT_EQ(source_sync_calls, 0); /* pass 1 remains read only */
	UT_ASSERT_EQ(history_sync_calls, 0);
	cluster_cold_replay_window_enter_v1();
	UT_ASSERT(sync_sources(typed));
	UT_ASSERT_EQ(source_sync_calls, 3);
	UT_ASSERT_EQ(source_sync_order[0], 1);
	UT_ASSERT_EQ(source_sync_order[1], 2);
	UT_ASSERT_EQ(source_sync_order[2], 3);
	UT_ASSERT_EQ(history_sync_calls, 3);
	UT_ASSERT_EQ(history_sync_threads[0], 2);
	UT_ASSERT_EQ(history_sync_incarnations[0], 9);
	UT_ASSERT_EQ(history_sync_threads[1], 2);
	UT_ASSERT_EQ(history_sync_incarnations[1], 10);
	UT_ASSERT_EQ(history_sync_threads[2], 3);
	UT_ASSERT_EQ(history_sync_incarnations[2], 8);
	UT_ASSERT_EQ(history_checkpoint_reads, 12);
	UT_ASSERT_EQ(census_begins, census_releases);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	cluster_cold_replay_window_leave_v1();
	cluster_cold_typed_destroy_v1(&typed);
}

UT_TEST(test_history_source_barrier_rejects_non_history_side_effect)
{
	ClusterColdTypedV1 *typed;
	RfSideOnlinePlanV1 *history;

	fixture();
	history_generation(2, 9, 0x40, 0x80);
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	history = typed->side_plans[typed->replay_count];
	UT_ASSERT_NOT_NULL(history);
	history->operation_count = 1;
	memset(&history->operations[0], 0, sizeof(history->operations[0]));
	history->operations[0].purpose = RF_SIDE_PLAN_RECOVERY;
	cluster_cold_replay_window_enter_v1();
	UT_ASSERT(!sync_sources(typed));
	UT_ASSERT_EQ(source_sync_calls, 0);
	UT_ASSERT(typed->refusal != CLUSTER_COLD_OK);
	cluster_cold_replay_window_leave_v1();
	cluster_cold_typed_destroy_v1(&typed);
}

UT_TEST(test_history_execution_freshness_refuses_changed_input_before_sync)
{
	for (int fault = 0; fault <= 9; fault++) {
		ClusterColdTypedV1 *typed;
		fixture();
		history_generation(2, 9, 0x40, 0x80);
		typed = prepare(0x800);
		UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
		if (fault == 0)
			census_token.file_txn_seq++;
		else
			census_input_fault = fault;
		typed->xid_next_at_drain[0] = 99;
		cluster_cold_replay_window_enter_v1();
		UT_ASSERT(!sync_sources(typed));
		UT_ASSERT_EQ(source_sync_calls, 0);
		UT_ASSERT_EQ(typed->xid_next_at_drain[0], 0);
		UT_ASSERT_EQ(typed->history_xid[typed->replay_count].next_xid, 0);
		UT_ASSERT(!typed->census_root_captured);
		UT_ASSERT(typed->refusal != CLUSTER_COLD_OK);
		census_input_fault = 0;
		UT_ASSERT(!sync_sources(typed));
		UT_ASSERT_EQ(source_sync_calls, 0);
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_history_durability_failure_discards_provisional_inputs_stickily)
{
	for (unsigned fault = 1; fault <= 5; fault++)
		for (unsigned fail_at = 1; fail_at <= 2; fail_at++) {
			ClusterColdTypedV1 *typed;
			fixture();
			history_generation(2, 9, 0x40, 0x80);
			history_generation(2, 10, 0x80, 0xc0);
			typed = prepare(0x800);
			UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
			typed->xid_next_at_drain[0] = 99;
			history_sync_fault = fault;
			history_sync_fail_at = fail_at;
			cluster_cold_replay_window_enter_v1();
			UT_ASSERT(!sync_sources(typed));
			UT_ASSERT_EQ(source_sync_calls, NROOTS);
			UT_ASSERT_EQ(history_sync_calls, fail_at);
			UT_ASSERT_EQ(typed->xid_next_at_drain[0], 0);
			UT_ASSERT_EQ(typed->history_xid[typed->replay_count].next_xid, 0);
			UT_ASSERT(!typed->census_root_captured);
			UT_ASSERT(typed->refusal != CLUSTER_COLD_OK);
			history_sync_fault = 0;
			UT_ASSERT(!sync_sources(typed));
			UT_ASSERT_EQ(history_sync_calls, fail_at);
			UT_ASSERT_EQ(census_begins, census_releases);
			cluster_cold_replay_window_leave_v1();
			cluster_cold_typed_destroy_v1(&typed);
		}
}

UT_TEST(test_history_durability_rechecks_root_checkpoint_and_original_owner)
{
	for (unsigned fault = 6; fault <= 8; fault++) {
		ClusterColdTypedV1 *typed;
		fixture();
		history_generation(2, 9, 0x40, 0x80);
		history_generation(3, 8, 0x40, 0x90);
		typed = prepare(0x800);
		UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
		history_sync_fault = fault;
		history_sync_fail_at = 2;
		cluster_cold_replay_window_enter_v1();
		UT_ASSERT(!sync_sources(typed));
		UT_ASSERT_EQ(history_sync_calls, 2);
		UT_ASSERT_EQ(typed->history_xid[typed->replay_count].next_xid, 0);
		UT_ASSERT(!typed->census_root_captured);
		UT_ASSERT(typed->refusal != CLUSTER_COLD_OK);
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_history_durability_error_clears_provisional_inputs)
{
	ClusterColdTypedV1 *typed;
	volatile bool caught = false;
	fixture();
	history_generation(2, 9, 0x40, 0x80);
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	history_sync_fault = 9;
	history_sync_fail_at = 1;
	cluster_cold_replay_window_enter_v1();
	PG_TRY();
	{
		(void)sync_sources(typed);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(history_sync_calls, 1);
	UT_ASSERT_EQ(typed->history_xid[typed->replay_count].next_xid, 0);
	UT_ASSERT(!typed->census_root_captured);
	UT_ASSERT(typed->refusal != CLUSTER_COLD_OK);
	UT_ASSERT(!sync_sources(typed));
	UT_ASSERT_EQ(history_sync_calls, 1);
	cluster_cold_replay_window_leave_v1();
	cluster_cold_typed_destroy_v1(&typed);
}

UT_TEST(test_history_execution_freshness_rechecks_after_last_source_sync)
{
	for (int fault = 10; fault <= 11; fault++) {
		ClusterColdTypedV1 *typed;
		fixture();
		history_generation(2, 9, 0x40, 0x80);
		typed = prepare(0x800);
		source_sync_fault = fault;
		source_sync_fail_thread = NROOTS;
		cluster_cold_replay_window_enter_v1();
		UT_ASSERT(!sync_sources(typed));
		UT_ASSERT_EQ(source_sync_calls, NROOTS);
		UT_ASSERT_EQ(history_sync_calls, 1);
		UT_ASSERT(history_checkpoint_reads >= 2);
		UT_ASSERT_EQ(typed->history_xid[typed->replay_count].next_xid, 0);
		UT_ASSERT(!typed->census_root_captured);
		UT_ASSERT(typed->refusal != CLUSTER_COLD_OK);
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_history_execution_freshness_requires_original_owner_and_cleans_error)
{
	for (unsigned fault = 1; fault <= 2; fault++) {
		ClusterColdTypedV1 *typed;
		volatile bool caught = false;
		fixture();
		history_generation(2, 9, 0x40, 0x80);
		typed = prepare(0x800);
		history_checkpoint_fault = fault;
		cluster_cold_replay_window_enter_v1();
		PG_TRY();
		{
			UT_ASSERT(!sync_sources(typed));
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 2);
		UT_ASSERT_EQ(source_sync_calls, 0);
		UT_ASSERT_EQ(typed->history_xid[typed->replay_count].next_xid, 0);
		UT_ASSERT(!typed->census_root_captured);
		UT_ASSERT(typed->refusal != CLUSTER_COLD_OK);
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_source_barrier_refuses_sync_or_tail_failure_stickily)
{
	for (int fault = 1; fault <= 5; fault++)
		for (uint16 thread = 1; thread <= NROOTS; thread++) {
			ClusterColdTypedV1 *typed;

			fixture();
			typed = prepare(0x800);
			source_sync_fault = fault;
			source_sync_fail_thread = thread;
			typed->xid_next_at_drain[0] = 99;
			cluster_cold_replay_window_enter_v1();
			UT_ASSERT(!sync_sources(typed));
			UT_ASSERT_EQ(typed->xid_next_at_drain[0], 0);
			UT_ASSERT_EQ(source_sync_calls, thread);
			UT_ASSERT(typed->refusal != CLUSTER_COLD_OK);
			source_sync_fail_thread = 0;
			UT_ASSERT(!sync_sources(typed));
			UT_ASSERT_EQ(source_sync_calls, thread);
			cluster_cold_replay_window_leave_v1();
			cluster_cold_typed_destroy_v1(&typed);
		}
}

UT_TEST(test_source_barrier_final_whole_scope_recheck)
{
	for (int fault = 6; fault <= 7; fault++) {
		ClusterColdTypedV1 *typed;

		fixture();
		typed = prepare(0x800);
		source_sync_fault = fault;
		source_sync_fail_thread = NROOTS;
		cluster_cold_replay_window_enter_v1();
		UT_ASSERT(!sync_sources(typed));
		UT_ASSERT_EQ(source_sync_calls, NROOTS);
		UT_ASSERT(typed->refusal != CLUSTER_COLD_OK);
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_source_barrier_rechecks_complete_native_checkpoint)
{
	for (uint16 thread = 1; thread <= NROOTS; thread++)
		for (int fault = 0; fault < 6; fault++) {
			ClusterColdTypedV1 *typed;
			ClusterWalRecoveryCheckpointV1 saved;
			fixture();
			typed = prepare(0x800);
			UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
			saved = native_checkpoint[thread];
			switch (fault) {
			case 0:
				native_checkpoint[thread].checkpoint_start++;
				break;
			case 1:
				native_checkpoint[thread].checkpoint.nextXid = FullTransactionIdFromU64(
					U64FromFullTransactionId(saved.checkpoint.nextXid) + (UINT64CONST(1) << 32));
				break;
			case 2:
				native_checkpoint[thread].checkpoint.oldestXid++;
				break;
			case 3:
				native_checkpoint[thread].checkpoint.nextMulti++;
				break;
			case 4:
				native_checkpoint[thread].checkpoint.time++;
				break;
			case 5:
				native_checkpoint[thread].checkpoint.fullPageWrites
					= !saved.checkpoint.fullPageWrites;
				break;
			}
			cluster_cold_replay_window_enter_v1();
			UT_ASSERT(!sync_sources(typed));
			UT_ASSERT_EQ(source_sync_calls, 0);
			native_checkpoint[thread] = saved;
			UT_ASSERT(!sync_sources(typed));
			UT_ASSERT_EQ(source_sync_calls, 0);
			cluster_cold_replay_window_leave_v1();
			cluster_cold_typed_destroy_v1(&typed);
		}
}

UT_TEST(test_source_barrier_final_native_checkpoint_change_is_sticky)
{
	ClusterColdTypedV1 *typed;
	fixture();
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	source_sync_fault = 9;
	source_sync_fail_thread = NROOTS;
	cluster_cold_replay_window_enter_v1();
	UT_ASSERT(!sync_sources(typed));
	UT_ASSERT_EQ(source_sync_calls, NROOTS);
	native_checkpoint[1].checkpoint.oldestXid--;
	UT_ASSERT(!sync_sources(typed));
	UT_ASSERT_EQ(source_sync_calls, NROOTS);
	cluster_cold_replay_window_leave_v1();
	cluster_cold_typed_destroy_v1(&typed);
}

UT_TEST(test_source_barrier_whole_source_preflight_before_sync)
{
	for (int fault = 0; fault < 5; fault++) {
		ClusterColdTypedV1 *typed;

		fixture();
		typed = prepare(0x800);
		switch (fault) {
		case 0:
			source_result[3] = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
			break;
		case 1:
			database_incarnation[3]++;
			break;
		case 2:
			native_redo[3]++;
			break;
		case 3:
			tokens[3].file_txn_seq++;
			break;
		case 4:
			typed->sources[2].claim.claim_sha256[31] ^= 1;
			break;
		}
		cluster_cold_replay_window_enter_v1();
		UT_ASSERT(!sync_sources(typed));
		UT_ASSERT_EQ(source_sync_calls, 0);
		UT_ASSERT(typed->refusal != CLUSTER_COLD_OK);
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_source_barrier_requires_cold_window_and_original_scope)
{
	for (int fault = 0; fault < 5; fault++) {
		ClusterColdTypedV1 *typed;

		fixture();
		typed = prepare(0x800);
		if (fault != 0)
			cluster_cold_replay_window_enter_v1();
		switch (fault) {
		case 1:
			fence_current = false;
			break;
		case 2:
			typed->replay_count--;
			break;
		case 3:
			typed->own_participant = typed->participant_count;
			break;
		case 4:
			fence_count--;
			break;
		}
		UT_ASSERT(!sync_sources(typed));
		UT_ASSERT_EQ(source_sync_calls, 0);
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
	UT_ASSERT(!sync_sources(NULL));
}

UT_TEST(test_source_barrier_error_does_not_leave_success)
{
	ClusterColdTypedV1 *typed;
	volatile bool caught = false;

	fixture();
	typed = prepare(0x800);
	source_sync_fault = 8;
	source_sync_fail_thread = 2;
	cluster_cold_replay_window_enter_v1();
	if (sigsetjmp(source_sync_exception, 0) == 0)
		(void)sync_sources(typed);
	else
		caught = true;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(source_sync_calls, 2);
	UT_ASSERT(typed->refusal != CLUSTER_COLD_OK);
	source_sync_fail_thread = 0;
	UT_ASSERT(!sync_sources(typed));
	UT_ASSERT_EQ(source_sync_calls, 2);
	cluster_cold_replay_window_leave_v1();
	cluster_cold_typed_destroy_v1(&typed);
}


UT_TEST(test_side_capture_bound_to_each_original_source)
{
	ClusterColdTypedV1 *typed;
	RfSideOnlinePlanV1 *history;
	Size total_budget = (Size)cluster_cold_recovery_plan_memory * 1024;

	fixture();
	history_generation(2, 9, 0x40, 0x80);
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	UT_ASSERT_EQ(side_creates, NROOTS + 1);
	UT_ASSERT_EQ(side_seals, NROOTS + 1);
	UT_ASSERT(side_budget_total <= total_budget / 2);
	for (uint32 i = 0; i < NROOTS; i++) {
		RfSideOnlinePlanV1 *plan = typed->side_plans[i];
		UT_ASSERT_NOT_NULL(plan);
		if (plan == NULL)
			continue;
		UT_ASSERT(plan == &side_fixture[i]);
		UT_ASSERT(plan->sealed && plan->bound);
		UT_ASSERT_EQ(plan->cut.failed_thread, i + 1);
		UT_ASSERT_EQ(plan->cut.origin_owner_incarnation, 11 + i);
		UT_ASSERT_EQ(plan->cut.scan_begin_inclusive, 0x100);
		UT_ASSERT_EQ(plan->cut.scan_end_exclusive, 0x2000);
		UT_ASSERT_EQ(plan->redo, 0x800);
		UT_ASSERT_EQ(plan->captured, 2);
		UT_ASSERT_EQ(plan->source_count, typed->participant_count);
		UT_ASSERT(memcmp(plan->sources, typed->sources,
						 typed->participant_count * sizeof(typed->sources[0]))
				  == 0);
	}
	history = typed->side_plans[NROOTS];
	UT_ASSERT_NOT_NULL(history);
	if (history != NULL) {
		UT_ASSERT(history->sealed && history->bound);
		UT_ASSERT_EQ(history->cut.failed_thread, 2);
		UT_ASSERT_EQ(history->cut.origin_owner_incarnation, 9);
		UT_ASSERT_EQ(history->cut.scan_begin_inclusive, 0x40);
		UT_ASSERT_EQ(history->cut.scan_end_exclusive, 0x80);
		/* Retained history remains outside the pass-2 replay window. */
		UT_ASSERT_EQ(history->redo, history->cut.scan_end_exclusive);
		UT_ASSERT_EQ(history->captured, 1);
		UT_ASSERT_EQ(history->source_count, typed->participant_count);
		UT_ASSERT(memcmp(history->sources, typed->sources,
						 typed->participant_count * sizeof(typed->sources[0]))
				  == 0);
	}
	UT_ASSERT_EQ(typed->replay_count, NROOTS);
	UT_ASSERT_EQ(history_scans, 1);
	cluster_cold_typed_destroy_v1(&typed);
	UT_ASSERT_EQ(side_destroys, NROOTS + 1);
}

UT_TEST(test_side_capture_source_bind_failure_stops_before_scan)
{
	ClusterColdTypedV1 *typed;
	fixture();
	history_generation(2, 9, 0x40, 0x80);
	side_fault = 8;
	typed = prepare(0x800);
	UT_ASSERT_NE(typed->refusal, CLUSTER_COLD_OK);
	UT_ASSERT_EQ(typed->scanned_records, 0);
	UT_ASSERT_EQ(side_seals, 0);
	for (uint32 i = 0; i < CLUSTER_COLD_MAX_PARTICIPANTS; i++)
		UT_ASSERT_NULL(typed->side_plans[i]);
	UT_ASSERT_EQ(side_destroys, side_creates);
	cluster_cold_typed_destroy_v1(&typed);
}

UT_TEST(test_history_side_capture_refusals_keep_original_boundaries)
{
	for (int fault = 9; fault <= 14; fault++) {
		ClusterColdTypedV1 *typed;
		fixture();
		history_generation(2, 9, 0x40, 0x80);
		side_fault = fault;
		typed = prepare(0x800);
		if (fault == 10 || fault == 11) {
			/* Original history remains accepted, but no partial SIDE input. */
			UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
			UT_ASSERT_NULL(typed->side_plans[NROOTS]);
			UT_ASSERT_EQ(typed->side_capture_detail[NROOTS],
						 fault == 10 ? RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE
									 : RF_PAGE_PROOF_DETAIL_OPCODE_UNSUPPORTED);
			UT_ASSERT_EQ(typed->side_capture_read_rec_ptr[NROOTS],
						 fault == 10 ? 0x40 : InvalidXLogRecPtr);
			UT_ASSERT_EQ(side_seals, NROOTS);
			UT_ASSERT_EQ(typed->replay_count, NROOTS);
		} else {
			UT_ASSERT_NE(typed->refusal, CLUSTER_COLD_OK);
			for (uint32 i = 0; i < CLUSTER_COLD_MAX_PARTICIPANTS; i++)
				UT_ASSERT_NULL(typed->side_plans[i]);
			if (fault == 9)
				UT_ASSERT_EQ(typed->scanned_records, 0);
		}
		cluster_cold_typed_destroy_v1(&typed);
		UT_ASSERT_EQ(census_releases, 1);
		UT_ASSERT_EQ(side_destroys, side_creates);
	}
}

static void
ready_history_capture(int fault)
{
	ClusterColdTypedV1 *typed;
	ClusterColdHandshakeV1 all = { true, true, true, true, true, true };
	char reason[256];
	bool ready;

	fixture();
	history_generation(2, 9, 0x40, 0x80);
	side_fault = fault;
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	UT_ASSERT_EQ(typed->replay_count, NROOTS);
	UT_ASSERT_EQ(typed->participant_count, NROOTS + 1);
	UT_ASSERT_EQ(census_releases, 1);
	/* The native history cut remains valid input even when SIDE capture
	 * failed. Startup must refuse before acquiring the execution IR set. */
	ready = cluster_cold_typed_ready_v1(typed, &all, reason, sizeof(reason));
	if (fault == 0) {
		UT_ASSERT(ready);
		UT_ASSERT_EQ(reason[0], '\0');
	} else {
		UT_ASSERT(!ready);
		UT_ASSERT(strstr(reason, "thread 2 has no complete SIDE capture") != NULL);
		UT_ASSERT_NULL(typed->side_plans[NROOTS]);
		UT_ASSERT_EQ(typed->side_capture_detail[NROOTS],
					 fault == 10 ? RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE
								 : RF_PAGE_PROOF_DETAIL_OPCODE_UNSUPPORTED);
	}
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	UT_ASSERT_EQ(typed->participants[NROOTS].owner_incarnation, 9);
	UT_ASSERT_EQ(typed->participants[NROOTS].native_redo, 0x80);
	cluster_cold_typed_destroy_v1(&typed);
	UT_ASSERT_EQ(side_destroys, side_creates);
}

UT_TEST(test_ready_refuses_incomplete_history_capture_before_ir)
{
	ready_history_capture(0);
	ready_history_capture(10);
}

UT_TEST(test_ready_refuses_unsupported_history_capture_before_ir)
{
	ready_history_capture(11);
}

UT_TEST(test_side_capture_failure_never_exposes_a_partial_plan)
{
	for (int fault = 1; fault <= 5; fault++) {
		ClusterColdTypedV1 *typed;
		fixture();
		side_fault = fault;
		typed = prepare(0x800);
		UT_ASSERT(typed->refusal != CLUSTER_COLD_OK);
		for (uint32 i = 0; i < CLUSTER_COLD_MAX_PARTICIPANTS; i++)
			UT_ASSERT_NULL(typed->side_plans[i]);
		UT_ASSERT_EQ(side_destroys, side_creates);
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_side_capture_keeps_original_scan_and_seal_refusals)
{
	for (int fault = 0; fault < 3; fault++) {
		ClusterColdTypedV1 *typed;
		fixture();
		if (fault == 0)
			scan_result[3] = CLUSTER_COLD_SIDE_OWNER_MISSING;
		else if (fault == 1)
			census_revalidate_result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		else
			data_token = 3;
		typed = prepare(0x800);
		UT_ASSERT(typed->refusal != CLUSTER_COLD_OK);
		for (uint32 i = 0; i < CLUSTER_COLD_MAX_PARTICIPANTS; i++)
			UT_ASSERT_NULL(typed->side_plans[i]);
		UT_ASSERT_EQ(side_seals, 0);
		UT_ASSERT_EQ(side_destroys, side_creates);
		cluster_cold_typed_destroy_v1(&typed);
	}
}


UT_TEST(test_native_history_capture_gap_keeps_original_plan_but_no_side_authority)
{
	for (int fault = 6; fault <= 7; fault++) {
		ClusterColdTypedV1 *typed;
		ClusterColdHandshakeV1 all = { true, true, true, true, true, true };
		char reason[256];
		fixture();
		side_fault = fault;
		typed = prepare(0x800);
		UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
		UT_ASSERT_NULL(typed->side_plans[0]);
		UT_ASSERT_NOT_NULL(typed->side_plans[1]);
		UT_ASSERT_EQ(typed->side_capture_detail[0], RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
		UT_ASSERT_EQ(typed->side_capture_read_rec_ptr[0], fault == 6 ? 0x100 : InvalidXLogRecPtr);
		UT_ASSERT_EQ(side_seals, 2);
		UT_ASSERT(!cluster_cold_typed_ready_v1(typed, &all, reason, sizeof(reason)));
		UT_ASSERT(strstr(reason, "SIDE capture") != NULL);
		cluster_cold_typed_destroy_v1(&typed);
		UT_ASSERT_EQ(side_destroys, NROOTS);
	}
}

static ClusterColdTypedV1 *
control_fixture_kind(ClusterColdRecordV1 *record, bool with_space, bool drop)
{
	ClusterColdTypedV1 *typed;
	RfSideOnlineOperationV1 *operation;
	fixture();
	scan_space = with_space;
	scan_space_drop = drop;
	scan_space_offset = with_space ? 0x100 : 0;
	control_native_pair_ok = false;
	typed = prepare(0x800);
	control_calls = control_init_calls = control_fault = 0;
	control_selected = UINT32_MAX;
	control_epoch = 7;
	control_authority_ok = control_borrow_ok = control_cover_ok = true;
	memset(&control_duty, 0, sizeof(control_duty));
	control_duty.origin_thread_id = 2;
	control_duty.origin_owner_incarnation = 12;
	memset(&control_serial, 0, sizeof(control_serial));
	control_serial.mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
	control_root = roots[2];
	control_token = tokens[2];
	memset(record, 0, sizeof(*record));
	record->read_rec_ptr = 0x1000;
	record->end_rec_ptr = 0x1100;
	record->record_crc = 0x12345678;
	record->rmid = RM_XLOG_ID;
	record->info = XLOG_NOOP;
	record->record_flags = CLUSTER_COLD_RECORD_FOREIGN_CONTROL;
	side_fixture[1].operation_count = 3;
	operation = &side_fixture[1].operations[1];
	operation->kind = RF_SIDE_ONLINE_OPERATION_NATIVE_CONTROL;
	operation->identity.record.system_identifier = 9;
	memset(operation->identity.record.storage_uuid, 1, 16);
	operation->identity.record.origin_thread = 2;
	operation->identity.record.timeline_id = 1;
	operation->identity.record.read_rec_ptr = 0x1000;
	operation->identity.record.end_rec_ptr = 0x1100;
	operation->identity.record.record_crc = 0x12345678;
	operation->identity.record.rmid = RM_XLOG_ID;
	operation->identity.record.info = XLOG_NOOP;
	side_fixture[1].operations[0] = *operation;
	side_fixture[1].operations[0].history_only = true;
	side_fixture[1].operations[0].identity.record.read_rec_ptr = 0x100;
	side_fixture[1].operations[0].identity.record.end_rec_ptr = 0x200;
	side_fixture[1].operations[2] = *operation;
	side_fixture[1].operations[2].identity.record.read_rec_ptr = 0x1200;
	side_fixture[1].operations[2].identity.record.end_rec_ptr = 0x1300;
	if (with_space) {
		ClusterColdSpaceInputV1 input;
		RelFileLocator locator;
		uint32 count = 0;
		UT_ASSERT(cluster_cold_plan_space_relation_v1(typed->plan, 0, &locator, &count));
		UT_ASSERT_EQ(count, 1);
		UT_ASSERT(cluster_cold_plan_space_input_v1(typed->plan, 0, 0, &input));
		operation = &side_fixture[1].operations[2];
		operation->kind = RF_SIDE_ONLINE_OPERATION_SPACE;
		operation->purpose = RF_SIDE_PLAN_RECOVERY;
		operation->space_key.locator = locator;
		operation->space_key.system_identifier = 9;
		operation->space_key.database_incarnation = 5;
		memset(operation->space_key.storage_uuid, 1, 16);
		operation->identity.record.read_rec_ptr = input.read_rec_ptr;
		operation->identity.record.end_rec_ptr = input.end_rec_ptr;
		operation->owned_payload = input.payload;
		operation->owned_payload_length = input.payload_length;
	}
	cluster_cold_replay_window_enter_v1();
	return typed;
}

static ClusterColdTypedV1 *
control_fixture_mode(ClusterColdRecordV1 *record, bool with_space)
{
	return control_fixture_kind(record, with_space, false);
}

static ClusterColdTypedV1 *
control_fixture(ClusterColdRecordV1 *record)
{
	return control_fixture_mode(record, false);
}

static bool
consume_control(ClusterColdTypedV1 *typed, const ClusterColdRecordV1 *record)
{
	return cluster_cold_typed_side_record_v1(typed, (ClusterRecoveryFencePlan *)&fixture_context, 1,
											 record);
}

UT_TEST(test_control_dispatch_selects_current_record_only)
{
	ClusterColdRecordV1 record;
	ClusterColdTypedV1 *typed = control_fixture(&record);
	UT_ASSERT(consume_control(typed, &record));
	UT_ASSERT_EQ(control_calls, 1);
	UT_ASSERT_EQ(control_selected, 1);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	cluster_cold_replay_window_leave_v1();
	cluster_cold_typed_destroy_v1(&typed);
}

UT_TEST(test_control_dispatch_keeps_original_space_preflight)
{
	ClusterColdRecordV1 record;
	ClusterColdTypedV1 *typed = control_fixture_mode(&record, true);
	UT_ASSERT(consume_control(typed, &record));
	UT_ASSERT_EQ(control_calls, 1);
	UT_ASSERT_EQ(control_selected, 1);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	cluster_cold_replay_window_leave_v1();
	cluster_cold_typed_destroy_v1(&typed);
}

UT_TEST(test_control_dispatch_rejects_unbound_space_effect)
{
	for (int fault = 0; fault < 6; fault++) {
		ClusterColdRecordV1 record;
		ClusterColdTypedV1 *typed = control_fixture_mode(&record, true);
		RfSideOnlineOperationV1 *op = &side_fixture[1].operations[2];
		static const uint8 wrong[] = { 'B', 'A', 'D', '!' };
		if (fault == 0)
			op->space_key.locator.relNumber++;
		if (fault == 1)
			op->identity.record.read_rec_ptr++;
		if (fault == 2)
			op->identity.record.end_rec_ptr++;
		if (fault == 3)
			op->owned_payload_length--;
		if (fault == 4)
			op->owned_payload = wrong;
		if (fault == 5)
			op->purpose = RF_SIDE_PLAN_PI_CONTRIBUTIONS;
		UT_ASSERT(!consume_control(typed, &record));
		UT_ASSERT(typed->refusal != CLUSTER_COLD_OK);
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_control_dispatch_native_create_borrows_exact_typed_pair)
{
	for (int fault = 0; fault < 4; fault++) {
		ClusterColdRecordV1 record;
		ClusterColdTypedV1 *typed = control_fixture_mode(&record, true);
		RfSideOnlineOperationV1 *native = &side_fixture[1].operations[0];
		RfSideOnlineOperationV1 *space = &side_fixture[1].operations[2];
		xl_smgr_create create = { 0 };
		create.rlocator = space->space_key.locator;
		create.forkNum = MAIN_FORKNUM;
		*native = side_fixture[1].operations[1];
		native->kind = RF_SIDE_ONLINE_OPERATION_NATIVE_STRUCTURE;
		native->purpose = RF_SIDE_PLAN_RECOVERY;
		native->identity.record.read_rec_ptr = 0x800;
		native->identity.record.end_rec_ptr = 0x900;
		native->identity.record.rmid = RM_SMGR_ID;
		native->identity.record.info = XLOG_SMGR_CREATE;
		native->owned_payload = (const uint8 *)&create;
		native->owned_payload_length = sizeof(create);
		native->source_xid = space->source_xid = 3;
		space->route.normalized_info = XLOG_SMGR_SPACE_IDENTITY;
		control_native_pair_ok = fault != 1;
		if (fault == 2)
			native->source_xid++;
		if (fault == 3)
			space->owned_payload_length--;
		UT_ASSERT_EQ(consume_control(typed, &record), fault == 0);
		UT_ASSERT_EQ(typed->refusal == CLUSTER_COLD_OK, fault == 0);
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_control_dispatch_rejects_wrong_record_or_effect)
{
	for (int fault = 0; fault < 17; fault++) {
		ClusterColdRecordV1 record;
		ClusterColdTypedV1 *typed = control_fixture(&record);
		RfSideOnlineOperationV1 *operation = &side_fixture[1].operations[1];
		switch (fault) {
		case 0:
			record.record_crc++;
			break;
		case 1:
			record.read_rec_ptr++;
			break;
		case 2:
			record.end_rec_ptr++;
			break;
		case 3:
			record.rmid = RM_CLOG_ID;
			break;
		case 4:
			record.info = XLOG_NEXTOID;
			break;
		case 5:
			record.component_count = 1;
			break;
		case 6:
			record.space_count = 1;
			break;
		case 7:
			record.record_flags |= CLUSTER_COLD_RECORD_SIDE_UNOWNED;
			break;
		case 8:
			operation->history_only = true;
			break;
		case 9:
			operation->kind = RF_SIDE_ONLINE_OPERATION_XACT;
			break;
		case 10:
			operation->identity.record.system_identifier++;
			break;
		case 11:
			operation->identity.record.storage_uuid[15]++;
			break;
		case 12:
			operation->identity.record.origin_thread++;
			break;
		case 13:
			operation->identity.record.timeline_id++;
			break;
		case 14:
			operation->identity.participant_index = 1;
			break;
		case 15:
			operation->identity.reserved_zero = 1;
			break;
		case 16:
			side_fixture[1].operations[2] = *operation;
			break;
		}
		UT_ASSERT(!consume_control(typed, &record));
		UT_ASSERT_EQ(control_calls, 0);
		UT_ASSERT(typed->refusal != CLUSTER_COLD_OK);
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_control_dispatch_requires_original_source_and_cold_scope)
{
	for (int fault = 0; fault < 17; fault++) {
		ClusterColdRecordV1 record;
		ClusterColdTypedV1 *typed = control_fixture(&record);
		switch (fault) {
		case 0:
			cluster_cold_replay_window_leave_v1();
			break;
		case 1:
			fence_current = false;
			break;
		case 2:
			typed->side_capture_detail[1] = RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
			break;
		case 3:
			typed->side_plans[1] = NULL;
			break;
		case 4:
			control_borrow_ok = false;
			break;
		case 5:
			control_authority_ok = false;
			break;
		case 6:
			control_cover_ok = false;
			break;
		case 7:
			control_serial.mode = 0;
			break;
		case 8:
			control_root.validated_tail_lsn_exclusive++;
			break;
		case 9:
			control_token.file_txn_seq++;
			break;
		case 10:
			tokens[3].file_txn_seq++;
			break;
		case 11:
			typed->sources[1].claim.claim_sha256[31]++;
			break;
		case 12:
			control_epoch = 0;
			break;
		case 13:
			control_epoch = (uint64)UINT32_MAX + 1;
			break;
		case 14:
			typed->own_participant = 1;
			break;
		case 15:
			typed->replay_count = 1;
			break;
		case 16:
			typed->refusal = CLUSTER_COLD_SOURCE_GAP;
			break;
		}
		UT_ASSERT(!consume_control(typed, &record));
		UT_ASSERT_EQ(control_calls, 0);
		UT_ASSERT(typed->refusal != CLUSTER_COLD_OK);
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_control_dispatch_rejects_capture_or_interval_drift)
{
	for (int fault = 0; fault < 7; fault++) {
		ClusterColdRecordV1 record;
		ClusterColdTypedV1 *typed = control_fixture(&record);
		switch (fault) {
		case 0:
			side_fixture[1].cut.origin_owner_incarnation++;
			break;
		case 1:
			side_fixture[1].cut.scan_end_exclusive++;
			break;
		case 2:
			side_fixture[1].redo++;
			break;
		case 3:
			side_fixture[1].operation_count = 0;
			break;
		case 4:
			side_fixture[1].sealed = false;
			break;
		case 5:
			record.read_rec_ptr = 0x100;
			record.end_rec_ptr = 0x200;
			break;
		case 6:
			record.end_rec_ptr = 0x2001;
			break;
		}
		UT_ASSERT(!consume_control(typed, &record));
		UT_ASSERT_EQ(control_calls, 0);
		UT_ASSERT(typed->refusal != CLUSTER_COLD_OK);
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_control_dispatch_failure_and_late_drift_remain_refused)
{
	for (int fault = 1; fault <= 7; fault++) {
		ClusterColdRecordV1 record;
		ClusterColdTypedV1 *typed;
		if (fault == 5)
			continue;
		typed = control_fixture(&record);
		control_fault = fault;
		UT_ASSERT(!consume_control(typed, &record));
		UT_ASSERT_EQ(control_calls, fault < 5 ? 1 : 0);
		UT_ASSERT(typed->refusal != CLUSTER_COLD_OK);
		control_fault = 0;
		UT_ASSERT(!consume_control(typed, &record));
		UT_ASSERT_EQ(control_calls, fault < 5 ? 1 : 0);
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_control_dispatch_error_does_not_leave_success)
{
	ClusterColdRecordV1 record;
	ClusterColdTypedV1 *typed = control_fixture(&record);
	volatile bool caught = false;
	control_fault = 5;
	if (sigsetjmp(source_sync_exception, 0) == 0)
		(void)consume_control(typed, &record);
	else
		caught = true;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(control_calls, 1);
	UT_ASSERT(typed->refusal != CLUSTER_COLD_OK);
	control_fault = 0;
	UT_ASSERT(!consume_control(typed, &record));
	UT_ASSERT_EQ(control_calls, 1);
	cluster_cold_replay_window_leave_v1();
	cluster_cold_typed_destroy_v1(&typed);
}


static void
side_record_fixture(ClusterColdRecordV1 *record, uint8 rmid, uint8 info,
					RfSideOnlineOperationKindV1 kind)
{
	RfSideOnlineOperationV1 *operation = &side_fixture[1].operations[1];
	record->record_flags = CLUSTER_COLD_RECORD_FOREIGN_SIDE;
	record->rmid = rmid;
	record->info = info;
	operation->identity.record.rmid = rmid;
	operation->identity.record.info = info;
	operation->kind = kind;
}

UT_TEST(test_native_side_dispatch_selects_exact_existing_consumer)
{
	const uint8 rmids[] = { RM_CLOG_ID,		 RM_CLOG_ID,	  RM_MULTIXACT_ID, RM_MULTIXACT_ID,
							RM_MULTIXACT_ID, RM_MULTIXACT_ID, RM_XACT_ID,	   RM_STANDBY_ID };
	const uint8 infos[] = { CLOG_ZEROPAGE,
							CLOG_TRUNCATE,
							XLOG_MULTIXACT_ZERO_OFF_PAGE,
							XLOG_MULTIXACT_ZERO_MEM_PAGE,
							XLOG_MULTIXACT_CREATE_ID,
							XLOG_MULTIXACT_TRUNCATE_ID,
							XLOG_XACT_ASSIGNMENT,
							XLOG_INVALIDATIONS };
	for (uint32 i = 0; i < lengthof(rmids); i++) {
		ClusterColdRecordV1 record;
		ClusterColdTypedV1 *typed = control_fixture_mode(&record, true);
		bool accepted;
		side_record_fixture(&record, rmids[i], infos[i],
							i < 6 ? RF_SIDE_ONLINE_OPERATION_PROJECTION
								  : RF_SIDE_ONLINE_OPERATION_NATIVE_CONTROL);
		accepted = consume_control(typed, &record);
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
		UT_ASSERT(accepted);
		UT_ASSERT_EQ(control_calls, 1);
		UT_ASSERT_EQ(control_selected, 1);
	}
}

UT_TEST(test_native_side_dispatch_rejects_wrong_kind_and_family)
{
	for (uint32 fault = 0; fault < 9; fault++) {
		ClusterColdRecordV1 record;
		ClusterColdTypedV1 *typed;

		if (fault == 5)
			continue; /* native COMMIT is now covered by the positive route test */
		typed = control_fixture(&record);
		side_record_fixture(&record, RM_CLOG_ID, CLOG_ZEROPAGE,
							RF_SIDE_ONLINE_OPERATION_PROJECTION);
		switch (fault) {
		case 0:
			side_fixture[1].operations[1].kind = RF_SIDE_ONLINE_OPERATION_NATIVE_CONTROL;
			break;
		case 1:
			record.record_flags |= CLUSTER_COLD_RECORD_FOREIGN_CONTROL;
			break;
		case 2:
			record.record_flags |= CLUSTER_COLD_RECORD_SIDE_UNOWNED;
			break;
		case 3:
			record.record_crc++;
			break;
		case 4:
			record.info = 0xF0;
			side_fixture[1].operations[1].identity.record.info = record.info;
			break;
		case 5:
			side_record_fixture(&record, RM_XACT_ID, XLOG_XACT_COMMIT,
								RF_SIDE_ONLINE_OPERATION_XACT);
			break;
		case 6:
			side_record_fixture(&record, RM_XACT_ID, XLOG_XACT_INVALIDATIONS,
								RF_SIDE_ONLINE_OPERATION_XACT);
			break;
		case 7:
			side_record_fixture(&record, RM_XACT_ID, XLOG_XACT_ASSIGNMENT,
								RF_SIDE_ONLINE_OPERATION_PROJECTION);
			break;
		case 8:
			side_record_fixture(&record, RM_STANDBY_ID, XLOG_INVALIDATIONS,
								RF_SIDE_ONLINE_OPERATION_XACT);
			break;
		}
		UT_ASSERT(!consume_control(typed, &record));
		UT_ASSERT_EQ(control_calls, 0);
		UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SIDE_OWNER_MISSING);
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_native_side_failure_or_late_drift_is_sticky)
{
	for (uint32 fault = 1; fault <= 7; fault++) {
		ClusterColdRecordV1 record;
		ClusterColdTypedV1 *typed = control_fixture(&record);
		side_record_fixture(&record, RM_MULTIXACT_ID, XLOG_MULTIXACT_CREATE_ID,
							RF_SIDE_ONLINE_OPERATION_PROJECTION);
		control_fault = fault;
		if (sigsetjmp(source_sync_exception, 0) == 0) {
			UT_ASSERT(!consume_control(typed, &record));
			UT_ASSERT(fault != 5);
		} else
			UT_ASSERT_EQ(fault, 5);
		UT_ASSERT_EQ(control_calls, fault <= 5 ? 1 : 0);
		UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SIDE_OWNER_MISSING);
		control_fault = 0;
		UT_ASSERT(!consume_control(typed, &record));
		UT_ASSERT_EQ(control_calls, fault <= 5 ? 1 : 0);
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}


UT_TEST(test_undo_dispatch_selects_exact_record_and_keeps_space_proof)
{
	const uint8 infos[] = { XLOG_UNDO_BLOCK_WRITE,		   XLOG_UNDO_BLOCK_WRITE_MULTI,
							XLOG_UNDO_SEGMENT_INIT,		   XLOG_UNDO_SEGMENT_REUSE,
							XLOG_UNDO_SEGMENT_RECYCLE,	   XLOG_UNDO_TT_SLOT_BIND,
							XLOG_UNDO_TT_SLOT_SET_HEAD,	   XLOG_UNDO_TT_SLOT_ABORT,
							XLOG_UNDO_TT_SLOT_CTRC_RELEASE };
	for (uint32 i = 0; i < lengthof(infos); i++) {
		ClusterColdRecordV1 record;
		ClusterColdTypedV1 *typed = control_fixture_mode(&record, true);
		side_record_fixture(&record, RM_CLUSTER_UNDO_ID, infos[i], RF_SIDE_ONLINE_OPERATION_UNDO);
		if (infos[i] == XLOG_UNDO_TT_SLOT_ABORT) {
			side_fixture[1].operations[1].undo.kind = CLUSTER_UNDO_KIND_TT_ABORT;
			side_fixture[1].operations[1].undo.format_version = CLUSTER_UNDO_TT_ABORT_EXACT_VERSION;
		}
		if (infos[i] == XLOG_UNDO_TT_SLOT_CTRC_RELEASE) {
			side_fixture[1].operations[1].undo.kind = CLUSTER_UNDO_KIND_TT_CTRC_RELEASE;
			side_fixture[1].operations[1].undo.format_version
				= CLUSTER_UNDO_TT_CTRC_RELEASE_VERSION;
		}
		UT_ASSERT(consume_control(typed, &record));
		UT_ASSERT_EQ(control_calls, 1);
		UT_ASSERT_EQ(control_selected, 1);
		UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_undo_dispatch_rejects_wrong_identity_or_unowned_effect)
{
	for (uint32 fault = 0; fault < 10; fault++) {
		ClusterColdRecordV1 record;
		ClusterColdTypedV1 *typed = control_fixture(&record);
		side_record_fixture(&record, RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE,
							RF_SIDE_ONLINE_OPERATION_UNDO);
		switch (fault) {
		case 0:
			side_fixture[1].operations[1].kind = RF_SIDE_ONLINE_OPERATION_PROJECTION;
			break;
		case 1:
			record.record_crc++;
			break;
		case 2:
			record.info |= 1;
			side_fixture[1].operations[1].identity.record.info = record.info;
			break;
		case 3:
			side_record_fixture(&record, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_ABORT,
								RF_SIDE_ONLINE_OPERATION_UNDO);
			break;
		case 4:
			side_record_fixture(&record, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_COMMIT,
								RF_SIDE_ONLINE_OPERATION_UNDO);
			break;
		case 5:
			side_record_fixture(&record, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_CTRC_RELEASE,
								RF_SIDE_ONLINE_OPERATION_UNDO);
			side_fixture[1].operations[1].undo.kind = CLUSTER_UNDO_KIND_TT_CTRC_RELEASE;
			/* The complete captured record must retain the known version. */
			break;
		case 6:
			side_fixture[1].operations[1].history_only = true;
			break;
		case 7:
			record.record_flags |= CLUSTER_COLD_RECORD_FOREIGN_CONTROL;
			break;
		case 8:
			control_serial.mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
			break;
		case 9:
			tokens[2].file_txn_seq++;
			break;
		}
		UT_ASSERT(!consume_control(typed, &record));
		UT_ASSERT_EQ(control_calls, 0);
		UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SIDE_OWNER_MISSING);
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_undo_block_dispatch_owner_failure_and_drift_stay_refused)
{
	for (uint32 fault = 1; fault <= 7; fault++) {
		ClusterColdRecordV1 record;
		ClusterColdTypedV1 *typed = control_fixture(&record);
		side_record_fixture(&record, RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE_MULTI,
							RF_SIDE_ONLINE_OPERATION_UNDO);
		control_fault = fault;
		if (sigsetjmp(source_sync_exception, 0) == 0) {
			UT_ASSERT(!consume_control(typed, &record));
			UT_ASSERT(fault != 5);
		} else
			UT_ASSERT_EQ(fault, 5);
		UT_ASSERT_EQ(control_calls, fault <= 5 ? 1 : 0);
		UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SIDE_OWNER_MISSING);
		control_fault = 0;
		UT_ASSERT(!consume_control(typed, &record));
		UT_ASSERT_EQ(control_calls, fault <= 5 ? 1 : 0);
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}


UT_TEST(test_native_abort_dispatch_matches_exact_captured_kind)
{
	for (int fault = 0; fault < 5; fault++) {
		ClusterColdRecordV1 record;
		ClusterColdTypedV1 *typed = control_fixture_mode(&record, true);
		side_record_fixture(&record, RM_XACT_ID, XLOG_XACT_ABORT | XLOG_XACT_HAS_INFO,
							RF_SIDE_ONLINE_OPERATION_XACT);
		side_fixture[1].operations[1].xact.kind = RF_SIDE_XACT_ABORT;
		if (fault == 1) side_fixture[1].operations[1].kind = RF_SIDE_ONLINE_OPERATION_PROJECTION;
		if (fault == 2) side_fixture[1].operations[1].identity.record.rmid = RM_CLOG_ID;
		if (fault == 3) record.record_crc++;
		if (fault == 4) record.info |= 1;
		UT_ASSERT_EQ(consume_control(typed, &record), fault == 0);
		UT_ASSERT_EQ(control_calls, fault == 0 ? 1 : 0);
		UT_ASSERT_EQ(typed->refusal, fault == 0 ? CLUSTER_COLD_OK : CLUSTER_COLD_SIDE_OWNER_MISSING);
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_relation_abort_dispatch_requires_original_complete_selected_input)
{
	/* Payload parsing/deletion are tested with their real owners elsewhere;
	 * this boundary keeps the original Startup selection and refusal checks. */
	static const uint8 payload[16] = { 1 };
	for (int fault = 0; fault < 16; fault++) {
		ClusterColdRecordV1 record;
		ClusterColdTypedV1 *typed = control_fixture_mode(&record, true);
		RfSideOnlineOperationV1 *op = &side_fixture[1].operations[1];

		side_record_fixture(&record, RM_XACT_ID,
							XLOG_XACT_ABORT | XLOG_XACT_HAS_INFO | XLR_SPECIAL_REL_UPDATE,
							RF_SIDE_ONLINE_OPERATION_XACT);
		op->purpose = RF_SIDE_PLAN_RECOVERY;
		op->xact.kind = RF_SIDE_XACT_ABORT;
		op->xact.cold_completion_effects = true;
		op->xact.relation_count = 2;
		op->xact.completion_payload_length = sizeof(payload);
		op->owned_payload = payload;
		op->owned_payload_length = sizeof(payload);
		if (fault == 1)
			op->xact.cold_completion_effects = false;
		if (fault == 2)
			op->xact.space_drop_count = 1;
		if (fault == 3)
			op->xact.kind = RF_SIDE_XACT_COMMIT;
		if (fault == 4)
			op->owned_payload = NULL;
		if (fault == 5)
			op->owned_payload_length--;
		if (fault == 6)
			op->xact.completion_payload_length = op->owned_payload_length = 0;
		if (fault == 7)
			op->history_only = true;
		if (fault == 8)
			op->purpose = RF_SIDE_PLAN_PI_CONTRIBUTIONS;
		if (fault == 9)
			record.space_count = 1;
		if (fault == 10)
			record.info |= 0x02;
		if (fault == 11)
			record.record_crc++;
		if (fault == 12)
			op->xact.relation_count = 0;
		if (fault == 13)
			record.info = op->identity.record.info = XLOG_XACT_ABORT | XLOG_XACT_HAS_INFO;
		if (fault == 14)
			control_authority_ok = false;
		if (fault == 15)
			control_cover_ok = false;
		UT_ASSERT_EQ(consume_control(typed, &record), fault == 0);
		UT_ASSERT_EQ(control_calls, fault == 0 ? 1 : 0);
		UT_ASSERT_EQ(control_selected, fault == 0 ? 1 : UINT32_MAX);
		UT_ASSERT_EQ(typed->refusal,
					 fault == 0 ? CLUSTER_COLD_OK : CLUSTER_COLD_SIDE_OWNER_MISSING);
		if (fault != 0) {
			UT_ASSERT(!consume_control(typed, &record));
			UT_ASSERT_EQ(control_calls, 0);
		}
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_native_commit_dispatch_selects_exact_consumer)
{
	ClusterColdRecordV1 record;
	ClusterColdTypedV1 *typed = control_fixture_mode(&record, true);

	side_record_fixture(&record, RM_XACT_ID, XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO,
						RF_SIDE_ONLINE_OPERATION_XACT);
	side_fixture[1].operations[1].xact.kind = RF_SIDE_XACT_COMMIT;
	UT_ASSERT(consume_control(typed, &record));
	UT_ASSERT_EQ(control_calls, 1);
	UT_ASSERT_EQ(control_selected, 1);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	cluster_cold_replay_window_leave_v1();
	cluster_cold_typed_destroy_v1(&typed);
}

/* Actual decoder and storage adapters have independent component tests.
 * Here the real sealed plan and dispatcher must keep exact captured bytes. */
static ClusterColdTypedV1 *
compound_fixture(ClusterColdRecordV1 *record, ClusterColdSpaceOpV1 *op)
{
	ClusterColdTypedV1 *typed = control_fixture_kind(record, true, true);
	ClusterColdSpaceInputV1 input;
	RfSideOnlineOperationV1 *captured = &side_fixture[1].operations[1];
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	UT_ASSERT(cluster_cold_plan_space_input_v1(typed->plan, 0, 0, &input));
	side_record_fixture(record, RM_XACT_ID,
						XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO | XLR_SPECIAL_REL_UPDATE,
						RF_SIDE_ONLINE_OPERATION_XACT);
	record->read_rec_ptr = captured->identity.record.read_rec_ptr = input.read_rec_ptr;
	record->end_rec_ptr = captured->identity.record.end_rec_ptr = input.end_rec_ptr;
	captured->xact.kind = RF_SIDE_XACT_COMMIT;
	captured->xact.cold_completion_effects = true;
	captured->xact.relation_count = captured->xact.space_drop_count = 1;
	captured->owned_payload = input.payload;
	captured->owned_payload_length = input.payload_length;
	captured->xact.space_drop_offset = 0;
	side_fixture[1].operation_count = 2;
	memset(op, 0, sizeof(*op));
	op->kind = CLUSTER_COLD_SPACE_DROP;
	op->locator = (RelFileLocator){ 1663, 5, 300 };
	op->payload = input.payload;
	op->payload_length = input.payload_length;
	record->space_count = 1;
	record->space_ops = op;
	return typed;
}

UT_TEST(test_compound_dispatch_binds_complete_drop_array)
{
	for (int fault = 0; fault < 9; fault++) {
		ClusterColdRecordV1 record;
		ClusterColdSpaceOpV1 op;
		ClusterColdTypedV1 *typed = compound_fixture(&record, &op);
		char different[CLUSTER_SPACE_STRUCTURE_WAL_BYTES] = { 'X' };
		if (fault == 1)
			op.kind = CLUSTER_COLD_SPACE_CREATE;
		if (fault == 2)
			op.payload_length--;
		if (fault == 3)
			op.payload = different;
		if (fault == 4)
			side_fixture[1].operations[1].xact.space_drop_count++;
		if (fault == 5)
			side_fixture[1].operations[1].xact.space_drop_offset++;
		if (fault == 6)
			record.space_ops = NULL;
		if (fault == 7)
			record.info |= XLR_CHECK_CONSISTENCY;
		if (fault == 8)
			side_fixture[1].operations[1].xact.cold_completion_effects = false;
		UT_ASSERT_EQ(consume_control(typed, &record), fault == 0);
		UT_ASSERT_EQ(control_calls, fault == 0 ? 1 : 0);
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
void
pg_re_throw(void)
{
	UT_ASSERT_NOT_NULL(PG_exception_stack);
	siglongjmp(*PG_exception_stack, 1);
}
static ClusterUndoRecoveryScopeV1 *final_scope;
static unsigned final_scope_enters, final_scope_leaves, final_drop_syncs;
static int final_drop_fault;

bool
cluster_undo_recovery_scope_enter_v1(ClusterUndoRecoveryScopeV1 *scope,
									 const ClusterThreadRecoveryAuthorityV1 *authority,
									 const RfSideOnlinePlanV1 *plan)
{
	UT_ASSERT_NULL(final_scope);
	UT_ASSERT(plan == &side_fixture[1]);
	UT_ASSERT_EQ(cluster_thread_recovery_authority_revalidate_nowait_v1(authority),
				 CLUSTER_THREAD_AUTHORITY_OK);
	final_scope_enters++;
	if (final_drop_fault == 1)
		return false;
	final_scope = scope;
	return true;
}
void
cluster_undo_recovery_scope_leave_v1(ClusterUndoRecoveryScopeV1 *scope)
{
	UT_ASSERT(final_scope == scope);
	final_scope = NULL;
	final_scope_leaves++;
}
bool
cluster_space_cold_drop_foreign_sync_v1(const ClusterSpaceIdentityKey *key,
										const ClusterSpaceRecoveryInput *input,
										const ClusterSpaceColdSourceV1 *source)
{
	UT_ASSERT_NOT_NULL(final_scope);
	UT_ASSERT_EQ(key->system_identifier, 9);
	UT_ASSERT_EQ(key->database_incarnation, 5);
	UT_ASSERT_EQ(key->locator.relNumber, 300);
	UT_ASSERT_EQ(source->origin_thread, 2);
	UT_ASSERT_EQ(source->end_rec_ptr, 0xa00);
	UT_ASSERT_EQ(input->length, CLUSTER_SPACE_STRUCTURE_WAL_BYTES);
	UT_ASSERT_EQ(((const char *)input->data)[0], 'D');
	final_drop_syncs++;
	if (final_drop_fault == 3)
		siglongjmp(*PG_exception_stack, 1);
	if (final_drop_fault == 4)
		control_authority_ok = false;
	if (final_drop_fault == 5)
		tokens[1].file_txn_seq++;
	if (final_drop_fault == 6)
		control_epoch++;
	return final_drop_fault != 2;
}

UT_TEST(test_compound_final_rebinds_scope_and_rejects_failure_stickily)
{
	for (int fault = 0; fault < 11; fault++) {
		ClusterColdRecordV1 record;
		ClusterColdSpaceOpV1 op;
		ClusterColdTypedV1 *typed = compound_fixture(&record, &op);
		volatile bool result = false, caught = false;
		final_drop_fault = fault;
		final_scope_enters = final_scope_leaves = final_drop_syncs = 0;
		final_scope = NULL;
		if (fault == 7)
			control_serial.mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
		if (fault == 8)
			side_fixture[1].bound = false;
		if (fault == 9)
			control_token.file_txn_seq++;
		if (fault == 10)
			typed->replay_count = 1;
		PG_TRY();
		{
			result = cluster_cold_typed_drop_final_v1(
				typed, (ClusterRecoveryFencePlan *)&fixture_context, 0);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(result, fault == 0);
		UT_ASSERT_EQ(caught, fault == 3);
		UT_ASSERT_EQ(final_scope_enters, fault <= 6 ? 1 : 0);
		UT_ASSERT_EQ(final_scope_leaves, fault <= 6 && fault != 1 ? 1 : 0);
		UT_ASSERT_NULL(final_scope);
		if (fault) {
			UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SIDE_OWNER_MISSING);
			final_drop_fault = 0;
			UT_ASSERT(!cluster_cold_typed_drop_final_v1(
				typed, (ClusterRecoveryFencePlan *)&fixture_context, 0));
		}
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

static bool
source_drained_fixture(ClusterColdTypedV1 *typed, uint32 participant)
{
	ClusterColdReplayResultV1 progress = { 0 };
	const ClusterControlRootSnapshot *root = &roots[typed->participants[participant].thread_id];
	progress.steps_done = cluster_cold_plan_step_count_v1(typed->plan);
	progress.last_read[participant] = root->tail_last_record_lsn;
	progress.last_end[participant] = root->validated_tail_lsn_exclusive;
	progress.last_crc[participant] = root->tail_last_record_crc32c;
	return cluster_cold_typed_source_drained_v1(
		typed, (ClusterRecoveryFencePlan *)&fixture_context, participant, &progress,
		cluster_cold_plan_replay_record_count_v1(typed->plan, participant));
}

static ClusterColdTypedV1 *
c0_fixture(ClusterColdRecordV1 *record, uint32 target)
{
	ClusterColdTypedV1 *typed = control_fixture(record);
	RfSideOnlineOperationV1 *op = &side_fixture[1].operations[1];
	ClusterRecoveryTTAbortV1 *c0 = &op->recovery_abort;
	side_record_fixture(record, RM_CLUSTER_UNDO_ID, XLOG_UNDO_RECOVERY_TT_ABORT,
						RF_SIDE_ONLINE_OPERATION_RECOVERY_TT_ABORT);
	c0_mode = c0_target_borrow = c0_target_current = c0_target_cover = true;
	c0_calls = c0_fault = 0;
	c0_duty = roots[3].identity;
	c0_root = roots[3];
	c0_token = tokens[3];
	c0_serial = control_serial;
	op->recovery_target_source = target;
	op->purpose = RF_SIDE_PLAN_RECOVERY;
	c0->version = 1;
	c0->length = CLUSTER_RECOVERY_TT_ABORT_BYTES;
	c0->flags = CLUSTER_RECOVERY_TT_ABORT_TOP_KNOWN;
	c0->writer_thread = 2;
	c0->writer_incarnation = 12;
	c0->target_thread = target + 1;
	c0->target_incarnation = target + 11;
	c0->recovery_serial = 3;
	c0->target_epoch = 4;
	c0->target_full_xid = c0->top_full_xid = UINT64_C(0x700400102);
	memcpy(c0->target_claim_sha256, typed->sources[target].claim.claim_sha256, 32);
	c0->exact = (xl_undo_tt_slot_abort_exact){ .segment_id = target * 256 + 1,
											   .xid = 0x400102,
											   .slot_offset = 5,
											   .wrap = 9,
											   .instance = target + 1,
											   .format_version = 1 };
	c0->before.xid = c0->exact.xid;
	c0->before.wrap = 9;
	c0->before.status = TT_SLOT_ACTIVE;
	UT_ASSERT(cluster_recovery_tt_abort_valid_v1(c0));
	if (target != 1)
		UT_ASSERT(source_drained_fixture(typed, target));
	return typed;
}

static ClusterColdTypedV1 *
own_c0_fixture(ClusterColdRecordV1 *record)
{
	ClusterColdTypedV1 *typed = c0_fixture(record, 2);
	RfSideOnlineOperationV1 *op = &side_fixture[0].operations[0];

	*op = side_fixture[1].operations[1];
	op->identity.record.origin_thread = 1;
	op->recovery_abort.writer_thread = 1;
	op->recovery_abort.writer_incarnation = 11;
	side_fixture[0].operation_count = 1;
	/* Keep the fixture's foreign source free of a second C0 dependency. */
	side_fixture[1].operation_count = 0;
	restart_ref = typed->sources[0];
	return typed;
}

UT_TEST(test_own_c0_dispatch_uses_restart_source_and_original_target)
{
	ClusterColdRecordV1 record;
	ClusterColdTypedV1 *typed = own_c0_fixture(&record);
	uint32 target = CLUSTER_COLD_NO_INDEX;
	uint32 order[CLUSTER_COLD_MAX_PARTICIPANTS];

	UT_ASSERT(cluster_cold_typed_record_target_v1(typed,
		(ClusterRecoveryFencePlan *)&fixture_context, 0, &record, &target));
	UT_ASSERT_EQ(target, 2);
	UT_ASSERT(cluster_cold_typed_drain_order_v1(typed,
		(ClusterRecoveryFencePlan *)&fixture_context, order, typed->participant_count));
	UT_ASSERT_EQ(order[typed->participant_count - 1], 0);
	UT_ASSERT(cluster_cold_typed_side_record_v1(typed,
		(ClusterRecoveryFencePlan *)&fixture_context, 0, &record));
	UT_ASSERT_EQ(c0_calls, 1);
	UT_ASSERT_EQ(control_init_calls, 0);
	UT_ASSERT_EQ(control_calls, 0);
	cluster_cold_replay_window_leave_v1();
	cluster_cold_typed_destroy_v1(&typed);
}

UT_TEST(test_own_c0_source_or_target_refusal_never_calls_owner)
{
	for (int fault = 0; fault < 12; fault++) {
		ClusterColdRecordV1 record;
		ClusterColdTypedV1 *typed = own_c0_fixture(&record);
		switch (fault) {
		case 0: restart_ref_present = false; break;
		case 1: restart_ref.claim.claim_sha256[0] ^= 1; break;
		case 2: restart_ref.claim.identity.origin_owner_incarnation++; break;
		case 3: restart_ref.claim.database_incarnation++; break;
		case 4: restart_ref.claim.max_config_generation++; break;
		case 5: restart_ref.timeline++; break;
		case 6: side_fixture[0].cut.scan_begin_inclusive++; break;
		case 7: c0_target_current = false; break;
		case 8: typed->source_drained[2] = false; break;
		case 9: side_fixture[0].operations[0].recovery_target_source = 0; break;
		case 10: record.info = XLOG_UNDO_TT_SLOT_ABORT; break;
		case 11: fence_current = false; break;
		}
		UT_ASSERT(!cluster_cold_typed_side_record_v1(typed,
			(ClusterRecoveryFencePlan *)&fixture_context, 0, &record));
		UT_ASSERT_EQ(c0_calls, 0);
		UT_ASSERT(typed->refusal != CLUSTER_COLD_OK);
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_own_c0_rechecks_restart_and_target_after_owner)
{
	for (int fault = 1; fault <= 4; fault++) {
		ClusterColdRecordV1 record;
		ClusterColdTypedV1 *typed = own_c0_fixture(&record);
		volatile bool caught = false;
		c0_fault = fault;
		PG_TRY();
		{
			UT_ASSERT(!cluster_cold_typed_side_record_v1(typed,
				(ClusterRecoveryFencePlan *)&fixture_context, 0, &record));
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 2);
		UT_ASSERT_EQ(c0_calls, 1);
		UT_ASSERT(typed->refusal != CLUSTER_COLD_OK);
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_c0_dispatch_binds_writer_and_target_separately)
{
	for (uint32 target = 1; target <= 2; target++) {
		ClusterColdRecordV1 record;
		ClusterColdTypedV1 *typed = c0_fixture(&record, target);
		UT_ASSERT(consume_control(typed, &record));
		UT_ASSERT_EQ(c0_calls, 1);
		UT_ASSERT_EQ(control_init_calls, 0);
		UT_ASSERT_EQ(control_calls, 0);
		UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_c0_dispatch_target_does_not_borrow_its_side_plan)
{
	ClusterColdRecordV1 record;
	ClusterColdTypedV1 *typed = c0_fixture(&record, 2);
	RfSideOnlinePlanV1 *saved = typed->side_plans[2];
	typed->side_plans[2] = NULL;
	typed->side_capture_detail[2] = RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
	UT_ASSERT(consume_control(typed, &record));
	UT_ASSERT_EQ(c0_calls, 1);
	UT_ASSERT_EQ(control_init_calls, 0);
	typed->side_plans[2] = saved;
	cluster_cold_replay_window_leave_v1();
	cluster_cold_typed_destroy_v1(&typed);
}

UT_TEST(test_c0_dispatch_requires_full_bound_source_set)
{
	for (int fault = 0; fault < 13; fault++) {
		ClusterColdRecordV1 record;
		ClusterColdTypedV1 *typed = c0_fixture(&record, 2);
		ClusterWalSourceRef *target = &side_fixture[1].sources[2];
		switch (fault) {
		case 0:
			side_fixture[1].sources[1].claim.claim_sha256[31]++;
			break;
		case 1:
			target->claim.claim_sha256[31]++;
			break;
		case 2:
			target->claim.database_incarnation++;
			break;
		case 3:
			target->claim.max_config_generation++;
			break;
		case 4:
			target->timeline++;
			break;
		case 5:
			target->claim.identity.authority_uuid[15]++;
			break;
		case 6:
			target->claim.identity.origin_owner_incarnation++;
			break;
		case 7:
			side_fixture[1].operations[1].recovery_target_source = 0;
			break;
		case 8:
			side_fixture[1].source_count = 2;
			break;
		case 9:
			side_fixture[1].sealed = false;
			break;
		case 10:
			typed->sources[3] = typed->sources[2];
			typed->participant_count++;
			break;
		case 11:
			/* A retained history ref is not a current mutation authority. */
			typed->sources[3] = *target;
			typed->sources[3].claim.identity.origin_owner_incarnation--;
			*target = typed->sources[3];
			typed->participant_count++;
			break;
		case 12:
			side_fixture[1].bound = false;
			break;
		}
		UT_ASSERT(!consume_control(typed, &record));
		UT_ASSERT_EQ(c0_calls, 0);
		UT_ASSERT_EQ(control_calls, 0);
		UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SIDE_OWNER_MISSING);
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_c0_dispatch_requires_target_current_authority)
{
	for (int fault = 0; fault < 9; fault++) {
		ClusterColdRecordV1 record;
		ClusterColdTypedV1 *typed = c0_fixture(&record, 2);
		switch (fault) {
		case 0:
			c0_target_borrow = false;
			break;
		case 1:
			c0_target_current = false;
			break;
		case 2:
			c0_target_cover = false;
			break;
		case 3:
			c0_serial.mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
			break;
		case 4:
			c0_token.file_txn_seq++;
			break;
		case 5:
			c0_root.validated_tail_lsn_exclusive++;
			break;
		case 6:
			cluster_cold_replay_window_leave_v1();
			break;
		case 7:
			control_borrow_ok = false;
			break;
		case 8:
			control_epoch = 0;
			break;
		}
		UT_ASSERT(!consume_control(typed, &record));
		UT_ASSERT_EQ(c0_calls, 0);
		UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SIDE_OWNER_MISSING);
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_c0_dispatch_rejects_uncaptured_or_unowned_records)
{
	for (int fault = 0; fault < 8; fault++) {
		ClusterColdRecordV1 record;
		ClusterColdTypedV1 *typed = c0_fixture(&record, 2);
		RfSideOnlineOperationV1 *op = &side_fixture[1].operations[1];
		switch (fault) {
		case 0:
			op->history_only = true;
			break;
		case 1:
			op->identity.record.record_crc++;
			break;
		case 2:
			record.component_count = 1;
			break;
		case 3:
			record.space_count = 1;
			break;
		case 4:
			record.record_flags = CLUSTER_COLD_RECORD_UNSUPPORTED;
			break;
		case 5:
			side_fixture[1].operations[2] = *op;
			break;
		case 6:
			record.info |= XLR_SPECIAL_REL_UPDATE;
			break;
		case 7:
			record.read_rec_ptr = 0x100;
			break;
		}
		UT_ASSERT(!consume_control(typed, &record));
		UT_ASSERT_EQ(c0_calls, 0);
		UT_ASSERT_EQ(control_calls, 0);
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_c0_dispatch_failure_error_and_final_drift_stay_sticky)
{
	for (int fault = 1; fault <= 7; fault++) {
		ClusterColdRecordV1 record;
		ClusterColdTypedV1 *typed = c0_fixture(&record, 2);
		volatile bool accepted = false;
		volatile bool caught = false;
		c0_fault = fault;
		PG_TRY();
		{
			accepted = consume_control(typed, &record);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(!accepted);
		UT_ASSERT_EQ(caught, fault == 2);
		UT_ASSERT_EQ(c0_calls, 1);
		UT_ASSERT_EQ(control_init_calls, 0);
		UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SIDE_OWNER_MISSING);
		c0_fault = 0;
		UT_ASSERT(!consume_control(typed, &record));
		UT_ASSERT_EQ(c0_calls, 1);
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_c0_target_requires_its_own_completed_drain)
{
	ClusterColdRecordV1 record;
	ClusterColdTypedV1 *typed = c0_fixture(&record, 2);
	typed->source_drained[2] = false;
	/* The writer or another source completing cannot stand in for target 2. */
	typed->source_drained[0] = typed->source_drained[1] = true;
	UT_ASSERT(!consume_control(typed, &record));
	UT_ASSERT_EQ(c0_calls, 0);
	UT_ASSERT(typed->refusal != CLUSTER_COLD_OK);
	UT_ASSERT(!source_drained_fixture(typed, 2));
	cluster_cold_replay_window_leave_v1();
	cluster_cold_typed_destroy_v1(&typed);
}

UT_TEST(test_source_drain_rechecks_exact_tail_count_and_scope)
{
	for (int fault = 0; fault < 9; fault++) {
		ClusterColdRecordV1 record;
		ClusterColdTypedV1 *typed = control_fixture(&record);
		ClusterColdReplayResultV1 progress = { 0 };
		uint64 count = cluster_cold_plan_replay_record_count_v1(typed->plan, 2);
		progress.steps_done = cluster_cold_plan_step_count_v1(typed->plan);
		progress.last_read[2] = roots[3].tail_last_record_lsn;
		progress.last_end[2] = roots[3].validated_tail_lsn_exclusive;
		progress.last_crc[2] = roots[3].tail_last_record_crc32c;
		switch (fault) {
		case 0:
			progress.last_read[2]++;
			break;
		case 1:
			progress.last_end[2]--;
			break;
		case 2:
			progress.last_crc[2] ^= 1;
			break;
		case 3:
			count++;
			break;
		case 4:
			progress.steps_done--;
			break;
		case 5:
			progress.detail = CLUSTER_COLD_REPLAY_CALLBACK;
			break;
		case 6:
			tokens[1].file_txn_seq++;
			break;
		case 7:
			fence_current = false;
			break;
		case 8:
			cluster_cold_replay_window_leave_v1();
			break;
		}
		UT_ASSERT(!cluster_cold_typed_source_drained_v1(
			typed, (ClusterRecoveryFencePlan *)&fixture_context, 2, &progress, count));
		UT_ASSERT(!typed->source_drained[2]);
		UT_ASSERT(typed->refusal != CLUSTER_COLD_OK);
		UT_ASSERT(!source_drained_fixture(typed, 2));
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_source_drain_is_attempt_local_and_not_recovery_completion)
{
	ClusterColdRecordV1 record;
	ClusterColdTypedV1 *typed = control_fixture(&record);
	UT_ASSERT(source_drained_fixture(typed, 2));
	UT_ASSERT(typed->source_drained[2]);
	UT_ASSERT(!typed->source_drained[0] && !typed->source_drained[1]);
	typed->xid_next_at_drain[2] = 99;
	UT_ASSERT(
		cluster_cold_typed_sync_sources_v1(typed, (ClusterRecoveryFencePlan *)&fixture_context));
	UT_ASSERT(!typed->source_drained[2]);
	UT_ASSERT_EQ(typed->xid_next_at_drain[2], 0);
	UT_ASSERT_EQ(c0_calls, 0);
	cluster_cold_replay_window_leave_v1();
	cluster_cold_typed_destroy_v1(&typed);
}


static bool
source_order_fixture(ClusterColdTypedV1 *typed, uint32 *order)
{
	return cluster_cold_typed_drain_order_v1(typed, (ClusterRecoveryFencePlan *)&fixture_context,
											 order, typed->participant_count);
}

UT_TEST(test_c0_drain_order_exact_target_before_writer_without_completion)
{
	for (uint32 target = 1; target <= 2; target++) {
		ClusterColdRecordV1 record;
		ClusterColdTypedV1 *typed = c0_fixture(&record, target);
		uint32 order[3] = { UINT32_MAX, UINT32_MAX, UINT32_MAX };
		memset(typed->source_drained, 0, sizeof(typed->source_drained));
		UT_ASSERT(source_order_fixture(typed, order));
		UT_ASSERT_EQ(order[0], 0);
		UT_ASSERT_EQ(order[1], target == 1 ? 1 : 2);
		UT_ASSERT_EQ(order[2], target == 1 ? 2 : 1);
		UT_ASSERT(!typed->source_drained[0] && !typed->source_drained[1]
				  && !typed->source_drained[2]);
		UT_ASSERT_EQ(c0_calls, 0);
		UT_ASSERT_EQ(control_calls, 0);
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_drain_order_preserves_no_dependency_and_repeated_edges)
{
	ClusterColdRecordV1 record;
	ClusterColdTypedV1 *typed = control_fixture(&record);
	uint32 order[3] = { UINT32_MAX, UINT32_MAX, UINT32_MAX };
	UT_ASSERT(source_order_fixture(typed, order));
	UT_ASSERT_EQ(order[0], 0);
	UT_ASSERT_EQ(order[1], 1);
	UT_ASSERT_EQ(order[2], 2);
	cluster_cold_replay_window_leave_v1();
	cluster_cold_typed_destroy_v1(&typed);
	typed = c0_fixture(&record, 2);
	side_fixture[1].operations[2] = side_fixture[1].operations[1];
	side_fixture[1].operations[2].identity.record.read_rec_ptr++;
	side_fixture[1].operation_count = 3;
	UT_ASSERT(source_order_fixture(typed, order));
	UT_ASSERT_EQ(order[0], 0);
	UT_ASSERT_EQ(order[1], 2);
	UT_ASSERT_EQ(order[2], 1);
	cluster_cold_replay_window_leave_v1();
	cluster_cold_typed_destroy_v1(&typed);
}

UT_TEST(test_drain_order_rejects_cycle_without_partial_order)
{
	ClusterColdRecordV1 record;
	ClusterColdTypedV1 *typed = c0_fixture(&record, 2);
	uint32 order[3] = { 0, 1, 2 };
	side_fixture[2].operations[1] = side_fixture[1].operations[1];
	side_fixture[2].operations[1].recovery_target_source = 1;
	side_fixture[2].operation_count = 2;
	UT_ASSERT(!source_order_fixture(typed, order));
	for (uint32 p = 0; p < 3; p++)
		UT_ASSERT_EQ(order[p], UINT32_MAX);
	UT_ASSERT_NE(typed->refusal, CLUSTER_COLD_OK);
	side_fixture[2].operation_count = 0;
	UT_ASSERT(!source_order_fixture(typed, order));
	UT_ASSERT_EQ(c0_calls, 0);
	cluster_cold_replay_window_leave_v1();
	cluster_cold_typed_destroy_v1(&typed);
}

UT_TEST(test_drain_order_rejects_identity_capture_and_scope_drift)
{
	for (int fault = 0; fault < 9; fault++) {
		ClusterColdRecordV1 record;
		ClusterColdTypedV1 *typed = c0_fixture(&record, 2);
		uint32 order[3] = { 0, 1, 2 };
		switch (fault) {
		case 0:
			side_fixture[1].sources[2].claim.claim_sha256[31]++;
			break;
		case 1:
			side_fixture[1].sources[1].claim.claim_sha256[31]++;
			break;
		case 2:
			side_fixture[1].operations[1].history_only = true;
			break;
		case 3:
			side_fixture[1].cut.scan_end_exclusive++;
			break;
		case 4:
			side_fixture[1].redo++;
			break;
		case 5:
			side_fixture[1].sealed = false;
			break;
		case 6:
			fence_current = false;
			break;
		case 7:
			tokens[3].file_txn_seq++;
			break;
		case 8:
			cluster_cold_replay_window_leave_v1();
			break;
		}
		UT_ASSERT(!source_order_fixture(typed, order));
		for (uint32 p = 0; p < 3; p++)
			UT_ASSERT_EQ(order[p], UINT32_MAX);
		UT_ASSERT_NE(typed->refusal, CLUSTER_COLD_OK);
		UT_ASSERT_EQ(c0_calls, 0);
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_source_drain_before_unrelated_steps_is_not_global_completion)
{
	for (int fault = 0; fault < 5; fault++) {
		ClusterColdRecordV1 record;
		ClusterColdTypedV1 *typed = control_fixture(&record);
		ClusterColdReplayResultV1 progress = { 0 };
		ClusterColdStepV1 step;
		uint64 count = cluster_cold_plan_replay_record_count_v1(typed->plan, 1);
		UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(typed->plan), 3);
		UT_ASSERT(cluster_cold_plan_step_v1(typed->plan, 1, &step));
		UT_ASSERT_EQ(step.participant, 1);
		UT_ASSERT(cluster_cold_plan_step_v1(typed->plan, 2, &step));
		UT_ASSERT_EQ(step.participant, 2);
		progress.steps_done = 2;
		progress.last_read[1] = roots[2].tail_last_record_lsn;
		progress.last_end[1] = roots[2].validated_tail_lsn_exclusive;
		progress.last_crc[1] = roots[2].tail_last_record_crc32c;
		if (fault == 1)
			progress.steps_done = 1;
		if (fault == 2)
			progress.steps_done = 4;
		if (fault == 3)
			progress.last_crc[1] ^= 1;
		if (fault == 4)
			tokens[1].file_txn_seq++;
		UT_ASSERT_EQ(cluster_cold_typed_source_drained_v1(
						 typed, (ClusterRecoveryFencePlan *)&fixture_context, 1, &progress, count),
					 fault == 0);
		UT_ASSERT_EQ(typed->source_drained[1], fault == 0);
		UT_ASSERT(!typed->source_drained[0] && !typed->source_drained[2]);
		UT_ASSERT(!cluster_cold_completion_proven_v1(typed->plan, &roots[2], &progress, 1));
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_c0_dependency_resolves_captured_target_without_completing_it)
{
	for (uint32 same = 0; same < 2; same++) {
		ClusterColdRecordV1 record;
		ClusterColdTypedV1 *typed = c0_fixture(&record, same ? 1 : 2);
		uint32 target = 999;
		memset(typed->source_drained, 0, sizeof(typed->source_drained));
		UT_ASSERT(cluster_cold_typed_record_target_v1(
			typed, (ClusterRecoveryFencePlan *)&fixture_context, 1, &record, &target));
		UT_ASSERT_EQ(target, same ? 1 : 2);
		UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
		UT_ASSERT_EQ(c0_calls, 0);
		UT_ASSERT(!typed->source_drained[0] && !typed->source_drained[1]
				  && !typed->source_drained[2]);
		if (!same) {
			UT_ASSERT(!consume_control(typed, &record));
			UT_ASSERT_EQ(c0_calls, 0);
		}
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
	{
		ClusterColdRecordV1 record;
		ClusterColdTypedV1 *typed = control_fixture(&record);
		uint32 target = 999;
		UT_ASSERT(cluster_cold_typed_record_target_v1(
			typed, (ClusterRecoveryFencePlan *)&fixture_context, 1, &record, &target));
		UT_ASSERT_EQ(target, CLUSTER_COLD_NO_INDEX);
		UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_c0_dependency_refuses_changed_identity_scope_and_partial_output)
{
	for (int fault = 0; fault < 10; fault++) {
		ClusterColdRecordV1 record;
		ClusterColdTypedV1 *typed = c0_fixture(&record, 2);
		uint32 target = 999;
		switch (fault) {
		case 0:
			record.record_crc ^= 1;
			break;
		case 1:
			record.end_rec_ptr++;
			break;
		case 2:
			record.record_flags = 0;
			break;
		case 3:
			side_fixture[1].operations[1].history_only = true;
			break;
		case 4:
			side_fixture[1].operations[0] = side_fixture[1].operations[1];
			break;
		case 5:
			side_fixture[1].sources[2].claim.claim_sha256[31] ^= 1;
			break;
		case 6:
			typed->replay_count = 2;
			break;
		case 7:
			c0_target_current = false;
			break;
		case 8:
			tokens[1].file_txn_seq++;
			break;
		case 9:
			cluster_cold_replay_window_leave_v1();
			break;
		}
		UT_ASSERT(!cluster_cold_typed_record_target_v1(
			typed, (ClusterRecoveryFencePlan *)&fixture_context, 1, &record, &target));
		UT_ASSERT_EQ(target, CLUSTER_COLD_NO_INDEX);
		UT_ASSERT(typed->refusal != CLUSTER_COLD_OK);
		UT_ASSERT_EQ(c0_calls, 0);
		UT_ASSERT(!cluster_cold_typed_record_target_v1(
			typed, (ClusterRecoveryFencePlan *)&fixture_context, 1, &record, &target));
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_tt_base_capture_includes_history_before_wal_scan)
{
	ClusterColdTypedV1 *typed;
	Size total = (Size)cluster_cold_recovery_plan_memory * 1024;

	fixture();
	history_generation(2, 9, 0x40, 0x80);
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	UT_ASSERT_EQ(tt_base_captures, 1);
	UT_ASSERT_EQ(tt_base_participants, 4);
	UT_ASSERT_EQ(tt_base_budget, total / 4);
	UT_ASSERT(side_budget_total + tt_base_budget < total);
	UT_ASSERT(typed->plan->memory_budget + side_budget_total + tt_base_budget <= total);
	UT_ASSERT_EQ(tt_base_rechecks, 0);
	cluster_cold_typed_destroy_v1(&typed);
	UT_ASSERT_EQ(tt_base_destroys, 1);
}

UT_TEST(test_tt_base_capture_failure_stops_before_side_and_scan)
{
	ClusterColdTypedV1 *typed;

	fixture();
	tt_base_capture_result = CLUSTER_COLD_SOURCE_GAP;
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SOURCE_GAP);
	UT_ASSERT_EQ(tt_base_captures, 1);
	UT_ASSERT_EQ(side_creates, 0);
	UT_ASSERT_EQ(typed->scanned_records, 0);
	cluster_cold_typed_destroy_v1(&typed);
}

UT_TEST(test_tt_base_drift_refuses_pass_two_stickily)
{
	ClusterColdTypedV1 *typed;

	fixture();
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	typed->effects_receipt.plan = typed->plan;
	tt_base_recheck_result = CLUSTER_COLD_SOURCE_GAP;
	cluster_cold_replay_window_enter_v1();
	UT_ASSERT(!sync_sources(typed));
	UT_ASSERT_EQ(source_sync_calls, NROOTS);
	UT_ASSERT_EQ(tt_base_rechecks, 1);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SOURCE_GAP);
	UT_ASSERT_NULL(typed->effects_receipt.plan);
	tt_base_recheck_result = CLUSTER_COLD_OK;
	UT_ASSERT(!sync_sources(typed));
	UT_ASSERT_EQ(tt_base_rechecks, 1);
	cluster_cold_replay_window_leave_v1();
	cluster_cold_typed_destroy_v1(&typed);
}

UT_TEST(test_tt_base_drift_refuses_later_pass_two_consumer)
{
	ClusterColdTypedV1 *typed = endpoint_fixture(false);

	/* Source sync must have checked the retained original TT base once. */
	UT_ASSERT_EQ(tt_base_rechecks, 1);
	typed->effects_receipt.plan = typed->plan;
	tt_base_recheck_result = CLUSTER_COLD_SOURCE_GAP;
	/* A later endpoint consumer must not use a stale base snapshot. */
	UT_ASSERT(!endpoint_finish(typed));
	UT_ASSERT_EQ(tt_base_rechecks, 2);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SOURCE_GAP);
	UT_ASSERT_EQ(typed->xid_next_after_redo, 0);
	UT_ASSERT_NULL(typed->effects_receipt.plan);
	tt_base_recheck_result = CLUSTER_COLD_OK;
	UT_ASSERT(!endpoint_finish(typed));
	UT_ASSERT_EQ(tt_base_rechecks, 2);
	endpoint_destroy(&typed);
}

UT_TEST(test_c0_isolation_uses_original_foreign_target_authority)
{
	ClusterColdRecordV1 record;
	ClusterColdTypedV1 *typed = control_fixture(&record);
	ClusterRecoveryFencePlan *fence = (ClusterRecoveryFencePlan *)&fixture_context;
	unsigned syncs = source_sync_calls;

	UT_ASSERT(!cluster_cold_typed_old_writer_isolated_v1(NULL, fence, 1));
	UT_ASSERT(cluster_cold_typed_old_writer_isolated_v1(typed, fence, 1));
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	UT_ASSERT_EQ(source_sync_calls, syncs);
	UT_ASSERT_EQ(control_calls, 0);
	UT_ASSERT_EQ(control_init_calls, 0);
	cluster_cold_replay_window_leave_v1();
	cluster_cold_typed_destroy_v1(&typed);
}

UT_TEST(test_c0_isolation_refuses_missing_or_stale_original_target_authority)
{
	for (int fault = 0; fault < 15; fault++) {
		ClusterColdRecordV1 record;
		ClusterColdTypedV1 *typed = control_fixture(&record);
		ClusterRecoveryFencePlan *fence = (ClusterRecoveryFencePlan *)&fixture_context;
		uint32 participant = 1;

		UT_ASSERT(cluster_cold_typed_old_writer_isolated_v1(typed, fence, participant));
		switch (fault) {
		case 0:
			fence = NULL;
			break;
		case 1:
			participant = typed->own_participant;
			break;
		case 2:
			participant = typed->replay_count;
			break;
		case 3:
			control_borrow_ok = false;
			break;
		case 4:
			control_authority_ok = false;
			break;
		case 5:
			control_cover_ok = false;
			break;
		case 6:
			control_serial.mode = 0;
			break;
		case 7:
			control_root.validated_tail_lsn_exclusive++;
			break;
		case 8:
			control_token.file_txn_seq++;
			break;
		case 9:
			fence_current = false;
			break;
		case 10:
			cluster_cold_replay_window_leave_v1();
			break;
		case 11:
			typed->refusal = CLUSTER_COLD_SOURCE_GAP;
			break;
		case 12:
			tokens[3].file_txn_seq++;
			break;
		case 13:
			typed->sources[1].claim.claim_sha256[31]++;
			break;
		case 14:
			typed->own_participant = 1;
			break;
		}
		UT_ASSERT(!cluster_cold_typed_old_writer_isolated_v1(typed, fence, participant));
		UT_ASSERT_EQ(control_calls, 0);
		UT_ASSERT_EQ(control_init_calls, 0);
		cluster_cold_replay_window_leave_v1();
		cluster_cold_typed_destroy_v1(&typed);
	}
}

UT_TEST(test_c0_sources_current_uses_original_owner_without_refresh)
{
	ClusterColdTypedV1 *typed = endpoint_fixture(true);
	ClusterColdXidReceiptV1 receipt;
	uint64 maximum;
	unsigned syncs, history_syncs;

	UT_ASSERT(endpoint_finish(typed));
	receipt = typed->xid_receipt;
	maximum = typed->xid_next_after_redo;
	syncs = source_sync_calls;
	history_syncs = history_sync_calls;
	UT_ASSERT_NULL(typed->inputs);
	UT_ASSERT(
		cluster_cold_typed_sources_current_v1(typed, (ClusterRecoveryFencePlan *)&fixture_context));
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	UT_ASSERT_EQ(typed->xid_next_after_redo, maximum);
	UT_ASSERT(memcmp(&receipt, &typed->xid_receipt, sizeof(receipt)) == 0);
	UT_ASSERT_EQ(source_sync_calls, syncs);
	UT_ASSERT_EQ(history_sync_calls, history_syncs);
	endpoint_destroy(&typed);
}

UT_TEST(test_c0_sources_current_refuses_lost_original_inputs_stickily)
{
	for (int fault = 0; fault < 12; fault++) {
		ClusterColdTypedV1 *typed = endpoint_fixture(true);
		ClusterRecoveryFencePlan *fence = (ClusterRecoveryFencePlan *)&fixture_context;
		uint32 participant_count = typed->participant_count;
		uint32 root_count = typed->replay_root_count;

		UT_ASSERT(endpoint_finish(typed));
		UT_ASSERT(cluster_cold_typed_sources_current_v1(typed, fence));
		typed->effects_receipt.plan = typed->plan;
		switch (fault) {
		case 0:
			fence = NULL;
			break;
		case 1:
			fence_current = false;
			break;
		case 2:
			cluster_cold_replay_window_leave_v1();
			break;
		case 3:
			census_token.file_txn_seq++;
			break;
		case 4:
			native_checkpoint[1].checkpoint.oldestXid++;
			break;
		case 5:
			typed->sources[typed->replay_count].claim.claim_sha256[31]++;
			break;
		case 6:
			typed->own_participant = 1;
			break;
		case 7:
			typed->participant_count--;
			break;
		case 8:
			typed->replay_root_count--;
			break;
		case 9:
			history_checkpoint_fault = 1;
			break;
		case 10:
			((uint8 *)&tokens[2])[0] ^= 1;
			break;
		case 11:
			typed->side_capture_detail[typed->replay_count] = 1;
			break;
		}
		UT_ASSERT(!cluster_cold_typed_sources_current_v1(typed, fence));
		UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SOURCE_GAP);
		UT_ASSERT_NULL(typed->xid_receipt.plan);
		UT_ASSERT_NULL(typed->effects_receipt.plan);
		UT_ASSERT_EQ(typed->xid_next_after_redo, 0);
		UT_ASSERT_EQ(typed->xid_next_at_drain[0], 0);
		UT_ASSERT_EQ(typed->history_xid[typed->replay_count].next_xid, 0);
		UT_ASSERT(!typed->census_root_captured);
		/* Restoring inputs cannot revive a refused attempt. */
		fence_current = true;
		typed->own_participant = 0;
		typed->participant_count = participant_count;
		typed->replay_root_count = root_count;
		UT_ASSERT(!cluster_cold_typed_sources_current_v1(
			typed, (ClusterRecoveryFencePlan *)&fixture_context));
		endpoint_destroy(&typed);
	}
	UT_ASSERT(!cluster_cold_typed_sources_current_v1(NULL, NULL));
}

UT_TEST(test_c0_sources_current_error_clears_inputs_and_preserves_prior_refusal)
{
	ClusterColdTypedV1 *typed = endpoint_fixture(true);
	volatile bool caught = false;

	UT_ASSERT(endpoint_finish(typed));
	typed->effects_receipt.plan = typed->plan;
	history_checkpoint_fault = 2;
	PG_TRY();
	{
		(void)cluster_cold_typed_sources_current_v1(typed,
													(ClusterRecoveryFencePlan *)&fixture_context);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SOURCE_GAP);
	UT_ASSERT_NULL(typed->xid_receipt.plan);
	UT_ASSERT_NULL(typed->effects_receipt.plan);
	UT_ASSERT_EQ(typed->xid_next_after_redo, 0);
	UT_ASSERT(!typed->census_root_captured);
	typed->refusal = CLUSTER_COLD_SPACE_REFUSED;
	UT_ASSERT(!cluster_cold_typed_sources_current_v1(typed,
													 (ClusterRecoveryFencePlan *)&fixture_context));
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SPACE_REFUSED);
	endpoint_destroy(&typed);
}

UT_TEST(test_checkpoint_inputs_survive_census_for_every_generation)
{
	ClusterColdTypedV1 *typed;
	ClusterControlRootSnapshot root;
	ClusterWalRecoveryCheckpointV1 checkpoint;

	fixture();
	history_generation(2, 9, 0x40, 0x80);
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	UT_ASSERT_NULL(typed->inputs);
	for (uint32 i = 0; i < typed->participant_count; i++) {
		uint32 input = i < typed->replay_count ? i + 1 : 11;
		UT_ASSERT(cluster_cold_typed_checkpoint_v1(typed, i, &root, &checkpoint));
		UT_ASSERT_EQ(memcmp(&root, &census_inputs[input].checkpoint, sizeof(root)), 0);
		UT_ASSERT_EQ(memcmp(&checkpoint.source, &typed->sources[i], sizeof(checkpoint.source)), 0);
		UT_ASSERT_EQ(checkpoint.checkpoint_start, census_inputs[input].checkpoint_start);
		UT_ASSERT_EQ(memcmp(&checkpoint.checkpoint, &census_inputs[input].native_checkpoint,
							sizeof(checkpoint.checkpoint)),
					 0);
	}
	/* A history cut still has no replay interval. Its actual checkpoint redo
	 * is retained independently, with no access to the released census. */
	memset(&census_inputs[11], 0xa5, sizeof(census_inputs[11]));
	UT_ASSERT(cluster_cold_typed_checkpoint_v1(typed, 3, &root, &checkpoint));
	UT_ASSERT_EQ(checkpoint.checkpoint.redo, 0x40);
	UT_ASSERT_EQ(typed->participants[3].native_redo, 0x80);
	UT_ASSERT_EQ(root.identity.origin_owner_incarnation, 9);
	UT_ASSERT_EQ(typed->replay_root_count, 3);
	cluster_cold_typed_destroy_v1(&typed);
	UT_ASSERT_EQ(contexts_alive, 0);
}

UT_TEST(test_checkpoint_input_mismatch_stops_before_capture_and_scan)
{
	for (int fault = 1; fault <= 9; fault++) {
		ClusterColdTypedV1 *typed;
		fixture();
		history_generation(2, 9, 0x40, 0x80);
		census_input_fault = fault;
		census_fault_index = fault == 7 ? 2 : 11;
		typed = prepare(0x800);
		UT_ASSERT_NE(typed->refusal, CLUSTER_COLD_OK);
		UT_ASSERT_EQ(typed->scanned_records, 0);
		UT_ASSERT_EQ(tt_base_captures, 0);
		UT_ASSERT_EQ(side_creates, 0);
		cluster_cold_typed_destroy_v1(&typed);
		UT_ASSERT_EQ(contexts_alive, 0);
	}
}

UT_TEST(test_checkpoint_export_clears_unsealed_and_failed_output)
{
	ClusterColdTypedV1 *typed;
	ClusterControlRootSnapshot root, zero_root = { 0 };
	ClusterWalRecoveryCheckpointV1 checkpoint, zero_checkpoint = { 0 };
	ClusterColdPlanV1 *plan;

	fixture();
	typed = prepare(0x800);
	for (int fault = 0; fault < 5; fault++) {
		memset(&root, 0xa5, sizeof(root));
		memset(&checkpoint, 0xa5, sizeof(checkpoint));
		if (fault == 4)
			typed->refusal = CLUSTER_COLD_SOURCE_GAP;
		UT_ASSERT(!cluster_cold_typed_checkpoint_v1(
			fault == 0 ? NULL : typed, fault == 1 ? typed->participant_count : 0,
			fault == 2 ? NULL : &root, fault == 3 ? NULL : &checkpoint));
		if (fault != 2)
			UT_ASSERT_EQ(memcmp(&root, &zero_root, sizeof(root)), 0);
		if (fault != 3)
			UT_ASSERT_EQ(memcmp(&checkpoint, &zero_checkpoint, sizeof(checkpoint)), 0);
	}
	typed->refusal = CLUSTER_COLD_OK;
	plan = typed->plan;
	typed->plan = NULL; /* unsealed state, no exported authority */
	UT_ASSERT(!cluster_cold_typed_checkpoint_v1(typed, 0, &root, &checkpoint));
	typed->plan = plan;
	cluster_cold_typed_destroy_v1(&typed);
}

UT_TEST(test_prepare_accepts_native_v2_own_claim_without_legacy_crc)
{
	for (unsigned native_crc = 0; native_crc < 2; native_crc++) {
		ClusterColdTypedV1 *typed;
		fixture();
		cluster_shared_config = true;
		roots[1].identity.thread_claim_crc32c = 0;
		if (native_crc) {
			ClusterWalThreadClaimV2 claim = {0};
			uint8 bytes[CLUSTER_WAL_CLAIM_V2_BYTES];
			claim.identity = roots[1].identity;
			claim.database_incarnation = 42;
			claim.config_generation = 1;
			claim.claim_generation = 7;
			UT_ASSERT_EQ(cluster_wal_claim_v2_encode(&claim, bytes), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
			for (unsigned i = 0; i < 4; i++)
				roots[1].identity.thread_claim_crc32c |= (uint32)bytes[104 + i] << (8 * i);
			UT_ASSERT(roots[1].identity.thread_claim_crc32c != 0);
		}
		restart_ref.claim.identity = roots[1].identity;
		native_checkpoint[1].source.claim.identity = roots[1].identity;
		UT_ASSERT(!cluster_recovery_duty_key_valid_v1(&roots[1].identity));
		UT_ASSERT(cluster_recovery_duty_key_valid_for_claim(&roots[1].identity, true));
		typed = prepare(0x800);
		UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
		UT_ASSERT_NOT_NULL(typed->plan);
		cluster_cold_typed_destroy_v1(&typed);
		cluster_shared_config = false;
	}
}

UT_TEST(test_prepare_retains_legacy_own_claim_crc_requirement)
{
	ClusterColdTypedV1 *typed;
	fixture();
	cluster_shared_config = false;
	roots[1].identity.thread_claim_crc32c = 0;
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_PARTICIPANT_INVALID);
	UT_ASSERT_NULL(typed->plan);
	cluster_cold_typed_destroy_v1(&typed);
}

UT_TEST(test_prepare_native_v2_own_claim_still_requires_exact_physical_identity)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterColdTypedV1 *typed;
		fixture();
		cluster_shared_config = true;
		roots[1].identity.thread_claim_crc32c = 0;
		if (fault == 0) roots[1].identity.origin_owner_incarnation = 0;
		if (fault == 1) roots[1].identity.authority_uuid[6] = 0;
		if (fault == 2) roots[1].identity.origin_thread_id = 2;
		if (fault == 3) roots[1].identity.reserved60 = 1;
		typed = prepare(0x800);
		UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_PARTICIPANT_INVALID);
		UT_ASSERT_NULL(typed->plan);
		cluster_cold_typed_destroy_v1(&typed);
		cluster_shared_config = false;
	}
}

int
main(void)
{
	UT_PLAN(127);
	UT_RUN(test_prepare_accepts_native_v2_own_claim_without_legacy_crc);
	UT_RUN(test_prepare_retains_legacy_own_claim_crc_requirement);
	UT_RUN(test_prepare_native_v2_own_claim_still_requires_exact_physical_identity);
	UT_RUN(test_own_c0_dispatch_uses_restart_source_and_original_target);
	UT_RUN(test_own_c0_source_or_target_refusal_never_calls_owner);
	UT_RUN(test_own_c0_rechecks_restart_and_target_after_owner);
	UT_RUN(test_prepare_rechecks_physical_profiles_of_every_original_origin);
	UT_RUN(test_prepare_checks_empty_physical_profiles_including_history_only_origin);
	UT_RUN(test_endpoints_recheck_physical_profiles_and_discard_stale_inputs);
	UT_RUN(test_endpoints_capture_and_recheck_post_redo_tt_before_publication);
	UT_RUN(test_endpoints_incomplete_replay_never_captures_post_redo_tt);
	UT_RUN(test_endpoints_inventory_refusal_discards_all_attempt_inputs);
	UT_RUN(test_endpoints_inventory_rechecks_files_sources_and_cleans_error);
	UT_RUN(test_endpoints_post_redo_tt_refusal_discards_all_attempt_inputs);
	UT_RUN(test_endpoints_post_redo_tt_rechecks_sources_and_cleans_error);
	UT_RUN(test_endpoints_prior_refusal_resets_previous_post_redo_snapshot);
	UT_RUN(test_endpoints_receipt_requires_original_completion);
	UT_RUN(test_endpoints_receipt_refuses_changed_retained_inputs);
	UT_RUN(test_endpoints_maximum_uses_complete_full_width_inputs);
	UT_RUN(test_endpoints_include_every_history_generation_original_checkpoint_cut);
	UT_RUN(test_endpoints_reject_partial_or_conflicting_replay_counters);
	UT_RUN(test_endpoints_reject_missing_or_tail_only_history_inputs);
	UT_RUN(test_endpoints_require_whole_sequencer_completion_and_exact_tails);
	UT_RUN(test_endpoints_require_current_original_owner_and_all_sources);
	UT_RUN(test_endpoints_recheck_original_sources_before_publishing_maximum);
	UT_RUN(test_endpoints_error_discards_group_and_preserves_prior_refusal);
	UT_RUN(test_endpoints_new_sync_invalidates_previous_aggregate);
	UT_RUN(test_history_durability_failure_discards_provisional_inputs_stickily);
	UT_RUN(test_history_durability_rechecks_root_checkpoint_and_original_owner);
	UT_RUN(test_history_durability_error_clears_provisional_inputs);
	UT_RUN(test_history_execution_freshness_refuses_changed_input_before_sync);
	UT_RUN(test_history_execution_freshness_rechecks_after_last_source_sync);
	UT_RUN(test_history_execution_freshness_requires_original_owner_and_cleans_error);
	UT_RUN(test_execution_pin_includes_history_only_thread_once_after_census_release);
	UT_RUN(test_execution_pin_refuses_changed_census_before_retention);
	UT_RUN(test_execution_pin_acquire_failure_is_sticky_and_discards_counters);
	UT_RUN(test_census_recheck_keeps_current_closed_history);
	UT_RUN(test_census_root_capture_failure_discards_history);
	UT_RUN(test_census_recheck_preserves_original_history_inputs_only);
	UT_RUN(test_census_recheck_rejects_every_changed_root_token_byte);
	UT_RUN(test_census_recheck_rejects_changed_history_metadata);
	UT_RUN(test_census_recheck_scope_failures_discard_provisional_group);
	UT_RUN(test_census_recheck_refuses_missing_or_execution_state);

	UT_RUN(test_history_side_capture_refusals_keep_original_boundaries);
	UT_RUN(test_ready_refuses_incomplete_history_capture_before_ir);
	UT_RUN(test_ready_refuses_unsupported_history_capture_before_ir);
	UT_RUN(test_c0_isolation_uses_original_foreign_target_authority);
	UT_RUN(test_c0_isolation_refuses_missing_or_stale_original_target_authority);
	UT_RUN(test_c0_sources_current_uses_original_owner_without_refresh);
	UT_RUN(test_c0_sources_current_refuses_lost_original_inputs_stickily);
	UT_RUN(test_c0_sources_current_error_clears_inputs_and_preserves_prior_refusal);
	UT_RUN(test_checkpoint_inputs_survive_census_for_every_generation);
	UT_RUN(test_checkpoint_input_mismatch_stops_before_capture_and_scan);
	UT_RUN(test_checkpoint_export_clears_unsealed_and_failed_output);
	UT_RUN(test_tt_base_capture_includes_history_before_wal_scan);
	UT_RUN(test_tt_base_capture_failure_stops_before_side_and_scan);
	UT_RUN(test_tt_base_drift_refuses_pass_two_stickily);
	UT_RUN(test_tt_base_drift_refuses_later_pass_two_consumer);
	UT_RUN(test_source_barrier_rechecks_complete_native_checkpoint);
	UT_RUN(test_source_barrier_final_native_checkpoint_change_is_sticky);
	UT_RUN(test_source_drain_before_unrelated_steps_is_not_global_completion);
	UT_RUN(test_c0_dependency_resolves_captured_target_without_completing_it);
	UT_RUN(test_c0_dependency_refuses_changed_identity_scope_and_partial_output);
	UT_RUN(test_c0_drain_order_exact_target_before_writer_without_completion);
	UT_RUN(test_drain_order_preserves_no_dependency_and_repeated_edges);
	UT_RUN(test_drain_order_rejects_cycle_without_partial_order);
	UT_RUN(test_drain_order_rejects_identity_capture_and_scope_drift);
	UT_RUN(test_c0_target_requires_its_own_completed_drain);
	UT_RUN(test_source_drain_rechecks_exact_tail_count_and_scope);
	UT_RUN(test_source_drain_is_attempt_local_and_not_recovery_completion);
	UT_RUN(test_c0_dispatch_binds_writer_and_target_separately);
	UT_RUN(test_c0_dispatch_target_does_not_borrow_its_side_plan);
	UT_RUN(test_c0_dispatch_requires_full_bound_source_set);
	UT_RUN(test_c0_dispatch_requires_target_current_authority);
	UT_RUN(test_c0_dispatch_rejects_uncaptured_or_unowned_records);
	UT_RUN(test_c0_dispatch_failure_error_and_final_drift_stay_sticky);
	UT_RUN(test_side_capture_source_bind_failure_stops_before_scan);
	UT_RUN(test_compound_dispatch_binds_complete_drop_array);
	UT_RUN(test_compound_final_rebinds_scope_and_rejects_failure_stickily);
	UT_RUN(test_native_commit_dispatch_selects_exact_consumer);
	UT_RUN(test_native_abort_dispatch_matches_exact_captured_kind);
	UT_RUN(test_relation_abort_dispatch_requires_original_complete_selected_input);
	UT_RUN(test_undo_dispatch_selects_exact_record_and_keeps_space_proof);
	UT_RUN(test_undo_dispatch_rejects_wrong_identity_or_unowned_effect);
	UT_RUN(test_undo_block_dispatch_owner_failure_and_drift_stay_refused);
	UT_RUN(test_native_side_dispatch_selects_exact_existing_consumer);
	UT_RUN(test_native_side_dispatch_rejects_wrong_kind_and_family);
	UT_RUN(test_native_side_failure_or_late_drift_is_sticky);
	UT_RUN(test_control_dispatch_selects_current_record_only);
	UT_RUN(test_control_dispatch_keeps_original_space_preflight);
	UT_RUN(test_control_dispatch_native_create_borrows_exact_typed_pair);
	UT_RUN(test_control_dispatch_rejects_unbound_space_effect);
	UT_RUN(test_control_dispatch_rejects_wrong_record_or_effect);
	UT_RUN(test_control_dispatch_requires_original_source_and_cold_scope);
	UT_RUN(test_control_dispatch_rejects_capture_or_interval_drift);
	UT_RUN(test_control_dispatch_failure_and_late_drift_remain_refused);
	UT_RUN(test_control_dispatch_error_does_not_leave_success);
	UT_RUN(test_native_history_capture_gap_keeps_original_plan_but_no_side_authority);
	UT_RUN(test_prepare_seals_own_and_fenced_generations);
	UT_RUN(test_side_capture_bound_to_each_original_source);
	UT_RUN(test_side_capture_failure_never_exposes_a_partial_plan);
	UT_RUN(test_side_capture_keeps_original_scan_and_seal_refusals);
	UT_RUN(test_prepare_takes_history_generations_from_the_census);
	UT_RUN(test_prepare_refuses_what_the_census_refuses);
	UT_RUN(test_prepare_refuses_unsealed_own_generation);
	UT_RUN(test_prepare_refuses_restart_redo_mismatch);
	UT_RUN(test_prepare_binds_own_generation_to_restart_input);
	UT_RUN(test_prepare_refuses_unproven_origin_source);
	UT_RUN(test_prepare_reports_scan_and_seal_refusals);
	UT_RUN(test_redo_block_decisions);
	UT_RUN(test_route_shared_cold_merge_only_typed);
	UT_RUN(test_ready_requires_every_consumer_before_ir);
	UT_RUN(test_space_effects_checked_and_need_the_space_owner);
	UT_RUN(test_unshared_multi_thread_cold_merge_refused);
	UT_RUN(test_checksum_decides_torn_pages);
	UT_RUN(test_cold_replay_window_tracks_pass_two);
	UT_RUN(test_apply_step_brackets_native_redo);
	UT_RUN(test_observed_page_classification);
	UT_RUN(test_refusal_hints);
	UT_RUN(test_plan_memory_budget_parameter);
	UT_RUN(test_source_barrier_all_selected_generations);
	UT_RUN(test_history_source_barrier_rejects_non_history_side_effect);
	UT_RUN(test_source_barrier_refuses_sync_or_tail_failure_stickily);
	UT_RUN(test_source_barrier_final_whole_scope_recheck);
	UT_RUN(test_source_barrier_whole_source_preflight_before_sync);
	UT_RUN(test_source_barrier_requires_cold_window_and_original_scope);
	UT_RUN(test_source_barrier_error_does_not_leave_success);
	UT_DONE();
	return ut_failed_count != 0;
}
