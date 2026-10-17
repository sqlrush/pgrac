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

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# unexpected Assert: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

#include "test_cluster_cold_recovery_startup_boundary.h"

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
	cluster_cold_typed_destroy_v1(&typed);
	UT_ASSERT_EQ(census_releases, 1);
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

UT_TEST(test_source_barrier_all_replay_generations_only)
{
	ClusterColdTypedV1 *typed;

	fixture();
	history_generation(2, 9, 0x40, 0x80);
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	UT_ASSERT_EQ(source_sync_calls, 0); /* pass 1 remains read only */
	cluster_cold_replay_window_enter_v1();
	UT_ASSERT(sync_sources(typed));
	UT_ASSERT_EQ(source_sync_calls, 3);
	UT_ASSERT_EQ(source_sync_order[0], 1);
	UT_ASSERT_EQ(source_sync_order[1], 2);
	UT_ASSERT_EQ(source_sync_order[2], 3);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	cluster_cold_replay_window_leave_v1();
	cluster_cold_typed_destroy_v1(&typed);
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
			cluster_cold_replay_window_enter_v1();
			UT_ASSERT(!sync_sources(typed));
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


UT_TEST(test_side_capture_bound_to_each_original_replay_source)
{
	ClusterColdTypedV1 *typed;
	Size total_budget = (Size)cluster_cold_recovery_plan_memory * 1024;

	fixture();
	history_generation(2, 9, 0x40, 0x80);
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	UT_ASSERT_EQ(side_creates, NROOTS);
	UT_ASSERT_EQ(side_seals, NROOTS);
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
	}
	UT_ASSERT_NULL(typed->side_plans[3]);
	UT_ASSERT_EQ(history_scans, 1);
	cluster_cold_typed_destroy_v1(&typed);
	UT_ASSERT_EQ(side_destroys, NROOTS);
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

int
main(void)
{
	UT_PLAN(48);
	UT_RUN(test_compound_dispatch_binds_complete_drop_array);
	UT_RUN(test_compound_final_rebinds_scope_and_rejects_failure_stickily);
	UT_RUN(test_native_commit_dispatch_selects_exact_consumer);
	UT_RUN(test_native_abort_dispatch_matches_exact_captured_kind);
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
	UT_RUN(test_side_capture_bound_to_each_original_replay_source);
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
	UT_RUN(test_source_barrier_all_replay_generations_only);
	UT_RUN(test_source_barrier_refuses_sync_or_tail_failure_stickily);
	UT_RUN(test_source_barrier_final_whole_scope_recheck);
	UT_RUN(test_source_barrier_whole_source_preflight_before_sync);
	UT_RUN(test_source_barrier_requires_cold_window_and_original_scope);
	UT_RUN(test_source_barrier_error_does_not_leave_success);
	UT_DONE();
	return ut_failed_count != 0;
}
