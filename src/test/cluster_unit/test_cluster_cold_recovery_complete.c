/*-------------------------------------------------------------------------
 *
 * test_cluster_cold_recovery_complete.c
 *	  What typed cold-crash replay must establish before a replayed
 *	  generation is published recovered: pass 2 consumed exactly its
 *	  ROOT-sealed cut, and every file pass 2 changed is durable.
 *
 *	  The plan core and the I/O module are linked; storage, buffer writes,
 *	  fsync and the decoded WAL record are boundaries
 *	  (test_cluster_cold_recovery_io_boundary.h).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cold_recovery_complete.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <signal.h>
#include <unistd.h>

#include "access/xlog.h"
#include "access/xlogreader.h"
#include "catalog/storage_xlog.h"
#include "miscadmin.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_cold_recovery_census.h"
#include "cluster/cluster_control_root.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_space_identity.h"
#include "cluster/cluster_space_reservation.h"
#include "cluster/cluster_wal_restart_read.h"
#include "cluster/cluster_wal_tail.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/smgr.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# unexpected Assert: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

#include "test_cluster_cold_recovery_io_boundary.h"

/* ---- completion proof ---- */
static ClusterColdParticipantV1 cut;
static ClusterControlRootSnapshot root;
static ClusterColdReplayResultV1 result;

static bool
observe_nothing(void *arg pg_attribute_unused(), const RfPageIdentityV1 *page pg_attribute_unused(),
				ClusterColdDataV1 *out)
{
	memset(out, 0, sizeof(*out));
	out->kind = CLUSTER_COLD_DATA_INVALID;
	return true;
}

/* A generation whose whole cut ends before pass 2 replays anything; the
 * proof is about where pass 2 stopped, not what it applied. */
static ClusterColdPlanV1 *
proof_fixture(void)
{
	ClusterColdPlanV1 *plan = NULL;
	ClusterColdDiagV1 diag;

	memset(&cut, 0, sizeof(cut));
	cut.thread_id = 2;
	cut.timeline = 1;
	cut.owner_incarnation = 12;
	cut.physical_lower = cut.native_redo = cut.tail_end = 0x2000;
	UT_ASSERT_EQ(cluster_cold_plan_create_v1(&cut, 1, 1024 * 1024, &plan), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe_nothing, NULL, &diag), CLUSTER_COLD_OK);
	memset(&root, 0, sizeof(root));
	root.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	root.root_flags
		= CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID | CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID;
	root.tail_last_record_lsn = 0x1f00;
	root.tail_last_record_crc32c = 0xabcd;
	root.validated_tail_lsn_exclusive = 0x2000;
	memset(&result, 0, sizeof(result));
	result.detail = CLUSTER_COLD_REPLAY_OK;
	result.last_read[0] = 0x1f00;
	result.last_crc[0] = 0xabcd;
	result.last_end[0] = 0x2000;
	return plan;
}

/*
 * Published recovered only when pass 2 ended exactly at the root's
 * validated last record: same start, same CRC, ending at the validated
 * tail, with every step done.
 */
UT_TEST(test_completion_proof)
{
	ClusterColdPlanV1 *plan = proof_fixture();

	UT_ASSERT(cluster_cold_completion_proven_v1(plan, &root, &result, 0));
	result.last_read[0]--;
	UT_ASSERT(!cluster_cold_completion_proven_v1(plan, &root, &result, 0));
	result.last_read[0]++;
	result.last_crc[0] ^= 1;
	UT_ASSERT(!cluster_cold_completion_proven_v1(plan, &root, &result, 0));
	result.last_crc[0] ^= 1;
	result.last_end[0] = 0x1ff0;
	UT_ASSERT(!cluster_cold_completion_proven_v1(plan, &root, &result, 0));
	result.last_end[0] = 0x2000;
	result.detail = CLUSTER_COLD_REPLAY_CUT_DIFFERS;
	UT_ASSERT(!cluster_cold_completion_proven_v1(plan, &root, &result, 0));
	result.detail = CLUSTER_COLD_REPLAY_OK;
	result.steps_done = 1; /* the plan has none */
	UT_ASSERT(!cluster_cold_completion_proven_v1(plan, &root, &result, 0));
	result.steps_done = 0;
	/* Tail equality and step count do not certify the aggregate effect route. */
	result.pages_applied = 1;
	UT_ASSERT(!cluster_cold_completion_proven_v1(plan, &root, &result, 0));
	result.pages_applied = 0;
	result.pages_skipped = 1;
	UT_ASSERT(!cluster_cold_completion_proven_v1(plan, &root, &result, 0));
	result.pages_skipped = 0;
	result.space_steps = 1;
	UT_ASSERT(!cluster_cold_completion_proven_v1(plan, &root, &result, 0));
	result.space_steps = 0;
	result.space_relations_finished = 1;
	UT_ASSERT(!cluster_cold_completion_proven_v1(plan, &root, &result, 0));
	result.space_relations_finished = 0;
	/* a participant the plan does not have, whatever its slot holds */
	result.last_read[1] = result.last_read[0];
	result.last_crc[1] = result.last_crc[0];
	result.last_end[1] = result.last_end[0];
	UT_ASSERT(!cluster_cold_completion_proven_v1(plan, &root, &result, 1));
	root.root_flags &= ~CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID;
	UT_ASSERT(!cluster_cold_completion_proven_v1(plan, &root, &result, 0));
	root.root_flags |= CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID;
	root.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	UT_ASSERT(!cluster_cold_completion_proven_v1(plan, &root, &result, 0));
	root.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	/* nothing consumed proves nothing, even against a zeroed root */
	memset(&result.last_read, 0, sizeof(result.last_read));
	root.tail_last_record_lsn = 0;
	UT_ASSERT(!cluster_cold_completion_proven_v1(plan, &root, &result, 0));
	UT_ASSERT(!cluster_cold_completion_proven_v1(NULL, &root, &result, 0));
	cluster_cold_plan_destroy_v1(&plan);
}

/* ---- every fenced origin, before the handoff ---- */
#define FENCE ((const ClusterRecoveryFencePlan *)&fence_origin_count)

/* Founder (thread 1), two fenced origins (2, 3) and two history
 * generations of thread 2: each origin's pass 2 ended at its root's sealed
 * tail. */
static ClusterColdPlanV1 *
ready_fixture(ClusterColdTypedV1 *typed)
{
	ClusterColdPlanV1 *plan = NULL;
	ClusterColdDiagV1 diag;
	uint32 p;

	memset(typed, 0, sizeof(*typed));
	for (p = 0; p < 5; p++) {
		ClusterColdParticipantV1 *part = &typed->participants[p];

		part->thread_id = p < 3 ? (uint16)(p + 1) : 2;
		part->timeline = 1;
		part->owner_incarnation = p < 3 ? 10 + p : p + 2;
		part->physical_lower = part->native_redo = part->tail_end = 0x2000 + 0x1000 * p;
	}
	typed->participant_count = 5;
	typed->replay_count = 3;
	typed->own_participant = 0;
	UT_ASSERT_EQ(cluster_cold_plan_create_v1(typed->participants, 5, 1024 * 1024, &plan),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe_nothing, NULL, &diag), CLUSTER_COLD_OK);
	typed->plan = plan;
	memset(&result, 0, sizeof(result));
	result.detail = CLUSTER_COLD_REPLAY_OK;
	fence_origin_count = 2;
	for (p = 1; p < 3; p++) {
		ClusterControlRootSnapshot *r = &fence_origin_root[p - 1];

		fence_origin_thread[p - 1] = (uint16)(p + 1);
		memset(r, 0, sizeof(*r));
		r->identity.origin_thread_id = (uint16)(p + 1);
		r->identity.origin_owner_incarnation = 10 + p;
		r->lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
		r->root_flags = CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID;
		r->tail_last_record_lsn = typed->participants[p].tail_end - 0x80;
		r->tail_last_record_crc32c = 0x100 + p;
		r->validated_tail_lsn_exclusive = typed->participants[p].tail_end;
		result.last_read[p] = r->tail_last_record_lsn;
		result.last_crc[p] = r->tail_last_record_crc32c;
		result.last_end[p] = r->validated_tail_lsn_exclusive;
	}
	return plan;
}

/*
 * Every fenced origin must be a replayed participant -- not the founder,
 * not a history generation of the same thread, the same owner
 * incarnation -- whose pass 2 ended at its sealed tail.
 */
UT_TEST(test_completion_ready)
{
	ClusterColdTypedV1 typed;
	ClusterColdTouchedV1 touched;
	ClusterColdPlanV1 *plan = ready_fixture(&typed);
	uint16 thread = 99;

	memset(&touched, 0, sizeof(touched));
	UT_ASSERT(cluster_cold_completion_ready_v1(&typed, FENCE, &result, &touched, &thread));
	UT_ASSERT_EQ(thread, 0);
	UT_ASSERT_EQ(touched.count, 0); /* no SPACE relation in this plan */

	/* origin 3 stopped one record early */
	result.last_end[2] -= 0x10;
	UT_ASSERT(!cluster_cold_completion_ready_v1(&typed, FENCE, &result, &touched, &thread));
	UT_ASSERT_EQ(thread, 3);
	result.last_end[2] += 0x10;

	/* an origin the typed plan does not replay: only a history generation
	 * of its thread is here, even one whose cut would prove */
	typed.participants[2].thread_id = 4;
	typed.participants[4].thread_id = 3;
	typed.participants[4].owner_incarnation = 12;
	result.last_read[4] = result.last_read[2];
	result.last_crc[4] = result.last_crc[2];
	result.last_end[4] = result.last_end[2];
	UT_ASSERT(!cluster_cold_completion_ready_v1(&typed, FENCE, &result, &touched, &thread));
	UT_ASSERT_EQ(thread, 3);
	typed.participants[4].thread_id = 2;
	typed.participants[4].owner_incarnation = 6;
	/* not found is not the first history slot, whatever that slot holds */
	typed.participants[3].owner_incarnation = 12;
	result.last_read[3] = result.last_read[2];
	result.last_crc[3] = result.last_crc[2];
	result.last_end[3] = result.last_end[2];
	UT_ASSERT(!cluster_cold_completion_ready_v1(&typed, FENCE, &result, &touched, &thread));
	UT_ASSERT_EQ(thread, 3);
	typed.participants[2].thread_id = 3;
	typed.participants[3].owner_incarnation = 5;

	/* another incarnation of the origin's thread */
	typed.participants[1].owner_incarnation = 5;
	UT_ASSERT(!cluster_cold_completion_ready_v1(&typed, FENCE, &result, &touched, &thread));
	UT_ASSERT_EQ(thread, 2);
	typed.participants[1].owner_incarnation = 11;

	/* the founder's own generation never stands in for an origin */
	fence_origin_thread[0] = 1;
	fence_origin_root[0].identity.origin_thread_id = 1;
	result.last_read[0] = result.last_read[1];
	result.last_crc[0] = result.last_crc[1];
	result.last_end[0] = result.last_end[1];
	typed.participants[0].owner_incarnation = 11;
	UT_ASSERT(!cluster_cold_completion_ready_v1(&typed, FENCE, &result, &touched, &thread));
	UT_ASSERT_EQ(thread, 1);
	cluster_cold_plan_destroy_v1(&plan);

	/* an origin the fence plan cannot name */
	plan = ready_fixture(&typed);
	UT_ASSERT(!cluster_cold_completion_ready_v1(&typed, NULL, &result, &touched, &thread));
	fence_origin_count = 3; /* index 2 cannot be named */
	fence_origin_thread[2] = 0;
	UT_ASSERT(!cluster_cold_completion_ready_v1(&typed, FENCE, &result, &touched, &thread));
	cluster_cold_plan_destroy_v1(&plan);
}

/* ---- touched relations ---- */
static RelFileLocator
locator(RelFileNumber rel)
{
	RelFileLocator l;

	l.spcOid = 1663;
	l.dbOid = 5;
	l.relNumber = rel;
	return l;
}

static uint32
forks_of(const ClusterColdTouchedV1 *touched, RelFileNumber rel)
{
	uint32 i;

	for (i = 0; i < touched->count; i++)
		if (touched->rels[i].locator.relNumber == rel)
			return touched->rels[i].forks;
	return 0;
}

#define FORK(f) (UINT32_C(1) << (f))

/* One entry per relation, kept sorted, its forks merged. */
UT_TEST(test_touched_set)
{
	ClusterColdTouchedV1 touched;
	RelFileLocator l;
	uint32 i;

	memset(&touched, 0, sizeof(touched));
	l = locator(200);
	cluster_cold_touched_add_v1(&touched, &l, MAIN_FORKNUM);
	l = locator(100);
	cluster_cold_touched_add_v1(&touched, &l, FSM_FORKNUM);
	l = locator(200);
	cluster_cold_touched_add_v1(&touched, &l, VISIBILITYMAP_FORKNUM);
	l = locator(100);
	cluster_cold_touched_add_v1(&touched, &l, MAIN_FORKNUM);
	UT_ASSERT_EQ(touched.count, 2);
	UT_ASSERT_EQ(touched.rels[0].locator.relNumber, 100);
	UT_ASSERT_EQ(touched.rels[0].forks, FORK(MAIN_FORKNUM) | FORK(FSM_FORKNUM));
	UT_ASSERT_EQ(touched.rels[1].forks, FORK(MAIN_FORKNUM) | FORK(VISIBILITYMAP_FORKNUM));
	/* another database's relation of the same number is another relation */
	l = locator(100);
	l.dbOid = 4;
	cluster_cold_touched_add_v1(&touched, &l, MAIN_FORKNUM);
	UT_ASSERT_EQ(touched.count, 3);
	UT_ASSERT_EQ(touched.rels[0].locator.dbOid, 4);

	/* past the first allocation, descending input still ends up sorted */
	for (i = 0; i < 100; i++) {
		l = locator(10000 - i);
		cluster_cold_touched_add_v1(&touched, &l, MAIN_FORKNUM);
	}
	UT_ASSERT_EQ(touched.count, 103);
	for (i = 1; i < touched.count; i++) {
		const RelFileLocator *a = &touched.rels[i - 1].locator;
		const RelFileLocator *b = &touched.rels[i].locator;

		UT_ASSERT(a->dbOid < b->dbOid || (a->dbOid == b->dbOid && a->relNumber < b->relNumber));
	}
	pfree(touched.rels);
}

/* A record's block references and the files a creation or a truncation
 * changes. */
UT_TEST(test_touched_by_record)
{
	ClusterColdTouchedV1 touched;
	DecodedXLogRecord *decoded;
	XLogReaderState reader;
	xl_smgr_create create;
	xl_smgr_truncate truncate;

	memset(&touched, 0, sizeof(touched));
	decoded = calloc(1, offsetof(DecodedXLogRecord, blocks) + 3 * sizeof(DecodedBkpBlock));
	memset(&reader, 0, sizeof(reader));
	reader.record = decoded;
	decoded->header.xl_rmid = RM_HEAP_ID;
	decoded->max_block_id = 2;
	decoded->blocks[0].in_use = true;
	decoded->blocks[0].rlocator = locator(300);
	decoded->blocks[0].forknum = MAIN_FORKNUM;
	decoded->blocks[2].in_use = true;
	decoded->blocks[2].rlocator = locator(100);
	decoded->blocks[2].forknum = FSM_FORKNUM;
	cluster_cold_touched_add_record_v1(&touched, &reader);
	UT_ASSERT_EQ(touched.count, 2);
	UT_ASSERT_EQ(forks_of(&touched, 300), FORK(MAIN_FORKNUM));
	UT_ASSERT_EQ(forks_of(&touched, 100), FORK(FSM_FORKNUM));

	memset(&create, 0, sizeof(create));
	create.rlocator = locator(400);
	create.forkNum = INIT_FORKNUM;
	decoded->header.xl_rmid = RM_SMGR_ID;
	decoded->header.xl_info = XLOG_SMGR_CREATE;
	decoded->max_block_id = -1;
	decoded->main_data = (char *)&create;
	cluster_cold_touched_add_record_v1(&touched, &reader);
	UT_ASSERT_EQ(forks_of(&touched, 400), FORK(INIT_FORKNUM));

	memset(&truncate, 0, sizeof(truncate));
	truncate.rlocator = locator(500);
	decoded->header.xl_info = XLOG_SMGR_TRUNCATE;
	decoded->main_data = (char *)&truncate;
	cluster_cold_touched_add_record_v1(&touched, &reader);
	UT_ASSERT_EQ(forks_of(&touched, 500),
				 FORK(MAIN_FORKNUM) | FORK(FSM_FORKNUM) | FORK(VISIBILITYMAP_FORKNUM));
	UT_ASSERT_EQ(touched.count, 4);
	free(decoded);
	pfree(touched.rels);
}

/*
 * Every touched relation's dirty buffers are written in one pass, then each
 * touched fork that still exists is fsynced; a dropped relation has nothing
 * left to sync.
 */
UT_TEST(test_barrier_writes_then_syncs)
{
	ClusterColdTouchedV1 touched;
	RelFileLocator l;

	storage_reset();
	forks[0].exists = true; /* 100 MAIN */
	forks[1].exists = true; /* 100 SPACE */
	forks[2].exists = true; /* 200 MAIN */
	forks[3].exists = true; /* 200 SPACE, untouched: not synced */
	barrier_event_count = 0;
	memset(&touched, 0, sizeof(touched));
	UT_ASSERT(cluster_cold_durable_barrier_v1(&touched));
	UT_ASSERT(cluster_cold_durable_barrier_v1(NULL));
	UT_ASSERT_EQ(barrier_event_count, 0);

	l = locator(100);
	cluster_cold_touched_add_v1(&touched, &l, MAIN_FORKNUM);
	cluster_cold_touched_add_v1(&touched, &l, SPACE_FORKNUM);
	l = locator(999); /* dropped during pass 2 */
	cluster_cold_touched_add_v1(&touched, &l, MAIN_FORKNUM);
	l = locator(200);
	cluster_cold_touched_add_v1(&touched, &l, MAIN_FORKNUM);
	cluster_cold_touched_add_v1(&touched, &l, FSM_FORKNUM); /* never created */
	native_barrier_count = 0;
	UT_ASSERT(cluster_cold_durable_barrier_v1(&touched));
	UT_ASSERT_EQ(barrier_event_count, 6);
	UT_ASSERT(strcmp(barrier_events[0], "w100/-1") == 0);
	UT_ASSERT(strcmp(barrier_events[1], "w200/-1") == 0);
	UT_ASSERT(strcmp(barrier_events[2], "w999/-1") == 0);
	UT_ASSERT(strcmp(barrier_events[3], "s100/0") == 0);
	{
		char space[32];

		snprintf(space, sizeof(space), "s100/%d", (int)SPACE_FORKNUM);
		UT_ASSERT(strcmp(barrier_events[4], space) == 0);
	}
	UT_ASSERT(strcmp(barrier_events[5], "s200/0") == 0);
	UT_ASSERT(strcmp(native_barrier_events, "ctpmus") == 0);
	UT_ASSERT_EQ(native_sync_relation_events, 6);
	pfree(touched.rels);
}

UT_TEST(test_barrier_flushes_native_without_relation_files)
{
	ClusterColdTouchedV1 touched = { 0 };

	for (int null_input = 0; null_input <= 1; null_input++) {
		barrier_event_count = 0;
		native_barrier_count = 0;
		native_barrier_events[0] = '\0';
		native_sync_relation_events = -1;
		UT_ASSERT(cluster_cold_durable_barrier_v1(null_input ? NULL : &touched));
		UT_ASSERT(strcmp(native_barrier_events, "ctpmus") == 0);
		UT_ASSERT_EQ(native_sync_relation_events, 0);
		UT_ASSERT_EQ(barrier_event_count, 0);
	}
}

UT_TEST(test_barrier_refuses_unconfirmed_native_sync)
{
	ClusterColdTouchedV1 touched = { 0 };
	bool result;

	native_barrier_count = 0;
	native_sync_ok = false;
	result = cluster_cold_durable_barrier_v1(&touched);
	native_sync_ok = true;
	UT_ASSERT(!result);
	UT_ASSERT(strcmp(native_barrier_events, "ctpmus") == 0);
}

/* Real sealed graph and I/O observer, with storage/checksum boundaries.
 * The fixture supplies the replayed bytes; no native redo is claimed. */
static void
readback_fixture(ClusterColdTypedV1 *typed)
{
	ClusterColdParticipantV1 participant = { 0 };
	ClusterColdComponentV1 component = { 0 };
	ClusterColdRecordV1 record = { 0 };
	ClusterColdDiagV1 diag;

	storage_reset();
	forks[0].exists = true;
	forks[0].nblocks = 1;
	data_page(&forks[0], 0, 'P', 1);
	identity_page(&forks[1], 'L', 7);
	memset(typed, 0, sizeof(*typed));
	typed->observer.system_identifier = 99;
	typed->observer.database_incarnation = 42;
	memset(typed->observer.storage_uuid, 3, 16);
	participant.thread_id = 1;
	participant.timeline = 1;
	participant.owner_incarnation = 11;
	participant.physical_lower = participant.native_redo = 0x1000;
	participant.tail_end = 0x2000;
	UT_ASSERT_EQ(cluster_cold_plan_create_v1(&participant, 1, 1024 * 1024, &typed->plan),
				 CLUSTER_COLD_OK);
	component.page.system_identifier = 99;
	memset(component.page.storage_uuid, 3, 16);
	component.page.locator = locator(100);
	component.page.forknum = MAIN_FORKNUM;
	component.page_class = RF_PAGE_CLASS_ORDINARY;
	component.before_kind = component.result_kind = RF_PAGE_STATE_PRESENT;
	memset(component.before.segment_incarnation, 7, 16);
	memset(component.result.segment_incarnation, 7, 16);
	component.before.mutation_token = 1;
	component.result.mutation_token = 2;
	record.read_rec_ptr = 0x1000;
	record.end_rec_ptr = 0x2000;
	record.scn = record.record_crc = 1;
	record.rmid = RM_HEAP_ID;
	record.component_count = 1;
	record.components = &component;
	UT_ASSERT_EQ(cluster_cold_plan_feed_v1(typed->plan, 0, &record), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(typed->plan, cluster_cold_observe_data_v1,
										   &typed->observer, &diag),
				 CLUSTER_COLD_OK);
	UT_ASSERT(typed->observer.cached_valid);
	data_page(&forks[0], 0, 'P', 2);
}

UT_TEST(test_readback_uses_fresh_physical_data_and_space_identity)
{
	static const ClusterColdDetailV1 expected[] = { CLUSTER_COLD_OK,
													CLUSTER_COLD_ANCESTOR_MISSING,
													CLUSTER_COLD_INCARNATION_MISMATCH,
													CLUSTER_COLD_CONTENT_UNPROVEN,
													CLUSTER_COLD_CONTENT_UNPROVEN,
													CLUSTER_COLD_IDENTITY_MISSING,
													CLUSTER_COLD_CONTENT_UNPROVEN,
													CLUSTER_COLD_CONTENT_UNPROVEN,
													CLUSTER_COLD_OBSERVATION_FAILED,
													CLUSTER_COLD_IDENTITY_MISSING };
	ClusterColdTypedV1 typed;

	for (uint32 fault = 0; fault < lengthof(expected); fault++) {
		readback_fixture(&typed);
		switch (fault) {
		case 1:
			data_page(&forks[0], 0, 'P', 1);
			break;
		case 2:
			identity_page(&forks[1], 'L', 9);
			break;
		case 3:
			data_page(&forks[0], 0, 'I', 2);
			break;
		case 4:
			checksums_on = false;
			break;
		case 5:
			forks[1].exists = false;
			break;
		case 6:
			forks[0].exists = false;
			break;
		case 7:
			header_valid = false;
			break;
		case 8:
			data_page(&forks[0], 0, 'Z', 2);
			break;
		case 9:
			identity_page(&forks[1], 'T', 7);
			break;
		default:
			break;
		}
		space_reads = 0;
		UT_ASSERT_EQ(cluster_cold_data_readback_v1(&typed), fault == 0);
		UT_ASSERT_EQ(typed.refusal, expected[fault]);
		UT_ASSERT_EQ(typed.observer.pages_observed, 2);
		if (fault == 0 || fault == 2 || fault == 9)
			UT_ASSERT_EQ(space_reads, 1); /* never reuse the pass-1 cached identity */
		if (fault != 0) {
			UT_ASSERT(strstr(typed.refusal_detail, "PAGE readback") != NULL);
			data_page(&forks[0], 0, 'P', 2);
			identity_page(&forks[1], 'L', 7);
			UT_ASSERT(!cluster_cold_data_readback_v1(&typed)); /* refusal stays sticky */
			UT_ASSERT_EQ(typed.observer.pages_observed, 2);
		}
		cluster_cold_plan_destroy_v1(&typed.plan);
	}
	UT_ASSERT(!cluster_cold_data_readback_v1(NULL));
	memset(&typed, 0, sizeof(typed));
	UT_ASSERT(!cluster_cold_data_readback_v1(&typed));
	UT_ASSERT_EQ(typed.refusal, CLUSTER_COLD_INVALID_ARGUMENT);
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_completion_proof);
	UT_RUN(test_completion_ready);
	UT_RUN(test_touched_set);
	UT_RUN(test_touched_by_record);
	UT_RUN(test_barrier_writes_then_syncs);
	UT_RUN(test_barrier_flushes_native_without_relation_files);
	UT_RUN(test_barrier_refuses_unconfirmed_native_sync);
	UT_RUN(test_readback_uses_fresh_physical_data_and_space_identity);
	UT_DONE();
	return ut_failed_count != 0;
}
