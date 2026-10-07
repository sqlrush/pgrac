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

#include "access/xlogreader.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_cold_recovery_census.h"
#include "cluster/cluster_page_cold_redo.h"
#include "storage/checksum.h"
#include "storage/checksum_impl.h"
#include "cluster/cluster_recovery_duty.h"
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

int
main(void)
{
	UT_PLAN(19);
	UT_RUN(test_prepare_seals_own_and_fenced_generations);
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
	UT_DONE();
	return ut_failed_count != 0;
}
