/*-------------------------------------------------------------------------
 * test_cluster_cold_recovery_effects.c
 *    Cold effect routing through the real registry and native WAL parsers.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_cold_recovery_effects.c
 * NOTES
 *    PGRAC-original. Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1
#include "postgres.h"
#include <limits.h>
#include "access/commit_ts.h"
#include "access/clog.h"
#include "access/multixact.h"
#include "access/xact.h"
#include "access/xlogreader.h"
#include "catalog/pg_control.h"
#include "catalog/storage_xlog.h"
#include "cluster/cluster_block_apply.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_page_detached.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "cluster/cluster_undo_segment.h"
#include "storage/standbydefs.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# unexpected Assert: %s at %s:%d\n", condition, file, line);
	abort();
}

/* This test never applies a page. Registry, detached preflight, native
 * commit/abort parsing and bounded SIDE shape validation are product code. */
ClusterBlkApplyResult
cluster_block_apply_one(XLogReaderState *r pg_attribute_unused(), uint8 id pg_attribute_unused(),
						char *page pg_attribute_unused())
{
	abort();
}

bool
rf_page_version_present_v1(const RfPageVersionV1 *version)
{
	const uint8 zero[16] = { 0 };
	return version != NULL && version->mutation_token != 0
		   && memcmp(version->segment_incarnation, zero, sizeof(zero)) != 0;
}

typedef struct EffectRecord {
	XLogReaderState reader;
	union {
		uint64 aligned;
		char bytes[BLCKSZ + 128];
	} data;
	DecodedXLogRecord decoded;
} EffectRecord;

static const uint8 uuid[16] = { 1 };

static void
record_init(EffectRecord *r, uint8 rmid, uint8 info)
{
	memset(r, 0, sizeof(*r));
	r->reader.record = &r->decoded;
	r->reader.ReadRecPtr = 0x1000;
	r->reader.EndRecPtr = 0x1800;
	r->decoded.header.xl_rmid = rmid;
	r->decoded.header.xl_info = info;
	r->decoded.header.xl_xid = 100;
	r->decoded.header.xl_scn = 23;
	r->decoded.max_block_id = -1;
	r->decoded.main_data = r->data.bytes;
}

static void
append(EffectRecord *r, const void *bytes, Size len)
{
	Assert(r->decoded.main_data_len + len <= sizeof(r->data.bytes));
	memcpy(r->data.bytes + r->decoded.main_data_len, bytes, len);
	r->decoded.main_data_len += len;
}

static void
counted(EffectRecord *r, const void *bytes, int count, Size size)
{
	append(r, &count, sizeof(count));
	append(r, bytes, count * size);
}

static void
completion(EffectRecord *r, bool commit, uint32 xinfo)
{
	TimestampTz timestamp = 1;
	TransactionId children[2] = { 101, 102 };
	RelFileLocator relations[2] = { { 1663, 5, 123 }, { 1663, 5, 124 } };
	xl_xact_stats_item stats[2] = { { 0 } };
	SharedInvalidationMessage msgs[2] = { { 0 } };
	xl_xact_origin origin = { 0 };
	xl_xact_scn scn = { .scn = 23 };
	xl_xact_twophase prepared = { .xid = 100 };
	xl_xact_tt_commit tt = { .segment_id = 1,
							 .segment_generation = 0,
							 .xid = 100,
							 .slot_offset = 1,
							 .wrap = 1,
							 .instance = 1,
							 .format_version = CLUSTER_XACT_TT_COMMIT_VERSION,
							 .commit_scn = 23 };

	record_init(r, RM_XACT_ID, (commit ? XLOG_XACT_COMMIT : XLOG_XACT_ABORT) | XLOG_XACT_HAS_INFO);
	append(r, &timestamp, sizeof(timestamp));
	append(r, &xinfo, sizeof(xinfo));
	if (xinfo & XACT_XINFO_HAS_SUBXACTS)
		counted(r, children, 2, sizeof(children[0]));
	if (xinfo & XACT_XINFO_HAS_RELFILELOCATORS)
		counted(r, relations, 2, sizeof(relations[0]));
	if (xinfo & XACT_XINFO_HAS_DROPPED_STATS)
		counted(r, stats, 2, sizeof(stats[0]));
	if (xinfo & XACT_XINFO_HAS_INVALS)
		counted(r, msgs, 2, sizeof(msgs[0]));
	if (xinfo & XACT_XINFO_HAS_TWOPHASE)
		append(r, &prepared, sizeof(prepared));
	if (xinfo & XACT_XINFO_HAS_ORIGIN)
		append(r, &origin, sizeof(origin));
	if (xinfo & XACT_XINFO_HAS_SCN)
		append(r, &scn, sizeof(scn));
	if (xinfo & XACT_XINFO_HAS_TT_COMMIT)
		append(r, &tt, sizeof(tt));
}

static bool
partitioned(const ClusterColdEffectRoutesV1 *e)
{
	uint32 routes[] = { e->native_apply, e->page_apply, e->space_apply, e->no_effect, e->refused };
	uint32 seen = 0;

	for (Size i = 0; i < lengthof(routes); i++) {
		if (seen & routes[i])
			return false;
		seen |= routes[i];
	}
	return seen != 0 && seen == e->required;
}

#define ALL_XINFO                                                                                  \
	(XACT_XINFO_HAS_SUBXACTS | XACT_XINFO_HAS_RELFILELOCATORS | XACT_XINFO_HAS_DROPPED_STATS       \
	 | XACT_XINFO_HAS_INVALS | XACT_XINFO_HAS_ORIGIN | XACT_XINFO_HAS_SCN                          \
	 | XACT_XINFO_HAS_TT_COMMIT | XACT_XINFO_HAS_AE_LOCKS | XACT_COMPLETION_APPLY_FEEDBACK         \
	 | XACT_COMPLETION_FORCE_SYNC_COMMIT | XACT_COMPLETION_UPDATE_RELCACHE_FILE)
#define ALL_EFFECTS                                                                                \
	(CLUSTER_COLD_EFFECT_RECORD | CLUSTER_COLD_EFFECT_TERMINAL | CLUSTER_COLD_EFFECT_CHILDREN      \
	 | CLUSTER_COLD_EFFECT_RELATIONS | CLUSTER_COLD_EFFECT_STATS                                   \
	 | CLUSTER_COLD_EFFECT_INVALIDATIONS | CLUSTER_COLD_EFFECT_ORIGIN | CLUSTER_COLD_EFFECT_SCN    \
	 | CLUSTER_COLD_EFFECT_TT | CLUSTER_COLD_EFFECT_LOCKS | CLUSTER_COLD_EFFECT_FEEDBACK           \
	 | CLUSTER_COLD_EFFECT_SYNC | CLUSTER_COLD_EFFECT_RELCACHE)

UT_TEST(test_whole_commit_effects_keep_native_owner)
{
	EffectRecord r;
	ClusterColdDecodedV1 out = { 0 };
	completion(&r, true, ALL_XINFO);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.effects.required, ALL_EFFECTS);
	UT_ASSERT_EQ(out.effects.native_apply, ALL_EFFECTS);
	UT_ASSERT(partitioned(&out.effects));
}

UT_TEST(test_foreign_compound_cannot_drop_unowned_effects)
{
	EffectRecord r;
	ClusterColdDecodedV1 out = { 0 };
	completion(&r, true, ALL_XINFO);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.effects.refused, ALL_EFFECTS);
	UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_SIDE_UNOWNED);
	UT_ASSERT(partitioned(&out.effects));
}

UT_TEST(test_relcache_work_exists_with_zero_messages)
{
	EffectRecord r;
	ClusterColdDecodedV1 out = { 0 };
	completion(&r, true, XACT_COMPLETION_UPDATE_RELCACHE_FILE);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.effects.native_apply, CLUSTER_COLD_EFFECT_RECORD | CLUSTER_COLD_EFFECT_TERMINAL
											   | CLUSTER_COLD_EFFECT_RELCACHE);
	UT_ASSERT(partitioned(&out.effects));
}

UT_TEST(test_embedded_twophase_is_not_ordinary_commit)
{
	EffectRecord r;
	ClusterColdDecodedV1 out = { 0 };
	completion(&r, true, XACT_XINFO_HAS_TWOPHASE);
	for (int foreign = 0; foreign <= 1; foreign++) {
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, foreign, &out),
					 CLUSTER_COLD_OK);
		UT_ASSERT(out.record.record_flags & CLUSTER_COLD_RECORD_UNSUPPORTED);
		UT_ASSERT_EQ(out.effects.refused, out.effects.required);
		UT_ASSERT(partitioned(&out.effects));
	}
}

UT_TEST(test_commit_ts_is_unsupported_even_for_own_source)
{
	EffectRecord r;
	ClusterColdDecodedV1 out = { 0 };
	int64 page = 0;
	record_init(&r, RM_COMMIT_TS_ID, COMMIT_TS_ZEROPAGE);
	append(&r, &page, sizeof(page));
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT(out.record.record_flags & CLUSTER_COLD_RECORD_UNSUPPORTED);
	UT_ASSERT(partitioned(&out.effects));
}

UT_TEST(test_foreign_invalidation_is_not_no_effect)
{
	EffectRecord r;
	ClusterColdDecodedV1 out = { 0 };
	xl_invalidations inval = { 0 };
	inval.relcacheInitFileInval = true;
	record_init(&r, RM_STANDBY_ID, XLOG_INVALIDATIONS);
	append(&r, &inval, MinSizeOfInvalidations);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_FOREIGN_SIDE);
	UT_ASSERT_EQ(out.effects.no_effect, 0);
	UT_ASSERT_EQ(out.effects.refused, 0);
	UT_ASSERT(out.effects.native_apply & CLUSTER_COLD_EFFECT_RELCACHE);
	UT_ASSERT(cluster_cold_replay_effects_v1(&out, NULL, false));
	UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, true));
	UT_ASSERT(partitioned(&out.effects));
}

UT_TEST(test_assignment_requires_complete_counted_payload)
{
	EffectRecord r;
	ClusterColdDecodedV1 out = { 0 };
	xl_xact_assignment assignment = { .xtop = 100, .nsubxacts = 1 };
	TransactionId child = 101;
	record_init(&r, RM_XACT_ID, XLOG_XACT_ASSIGNMENT);
	append(&r, &assignment, MinSizeOfXactAssignment);
	append(&r, &child, sizeof(child));
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.effects.native_apply,
				 CLUSTER_COLD_EFFECT_RECORD | CLUSTER_COLD_EFFECT_CHILDREN);
	r.decoded.main_data_len--;
	UT_ASSERT(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out)
			  != CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.effects.required, 0);
}

UT_TEST(test_invalidations_require_complete_counted_payload)
{
	EffectRecord r;
	ClusterColdDecodedV1 out = { 0 };
	SharedInvalidationMessage msg = { 0 };
	record_init(&r, RM_XACT_ID, XLOG_XACT_INVALIDATIONS);
	counted(&r, &msg, 1, sizeof(msg));
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.effects.native_apply,
				 CLUSTER_COLD_EFFECT_RECORD | CLUSTER_COLD_EFFECT_INVALIDATIONS);
	r.decoded.main_data_len--;
	UT_ASSERT(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out)
			  != CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.effects.required, 0);
}

UT_TEST(test_abort_routes_all_native_effects)
{
	EffectRecord r;
	ClusterColdDecodedV1 out = { 0 };
	completion(&r, false,
			   XACT_XINFO_HAS_SUBXACTS | XACT_XINFO_HAS_DROPPED_STATS | XACT_XINFO_HAS_SCN);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.effects.native_apply, CLUSTER_COLD_EFFECT_RECORD | CLUSTER_COLD_EFFECT_TERMINAL
											   | CLUSTER_COLD_EFFECT_CHILDREN
											   | CLUSTER_COLD_EFFECT_STATS
											   | CLUSTER_COLD_EFFECT_SCN);
	UT_ASSERT(partitioned(&out.effects));
}

UT_TEST(test_unknown_or_truncated_commit_clears_routes)
{
	EffectRecord r;
	ClusterColdDecodedV1 out = { 0 };
	completion(&r, true, ALL_XINFO);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out),
				 CLUSTER_COLD_OK);
	r.decoded.main_data_len--;
	UT_ASSERT(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out)
			  != CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.effects.required, 0);
	completion(&r, true, 1U << 28);
	UT_ASSERT(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out)
			  != CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.effects.native_apply, 0);
}

UT_TEST(test_exact_foreign_noop_stays_no_effect)
{
	EffectRecord r;
	ClusterColdDecodedV1 out = { 0 };
	record_init(&r, RM_XLOG_ID, XLOG_NOOP);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.effects.no_effect, CLUSTER_COLD_EFFECT_RECORD);
	UT_ASSERT(partitioned(&out.effects));
}

UT_TEST(test_registry_unknown_and_blocks_still_refuse)
{
	EffectRecord r;
	ClusterColdDecodedV1 out = { 0 };
	record_init(&r, RM_XACT_ID, 0x70);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out),
				 CLUSTER_COLD_OPCODE_UNSUPPORTED);
	UT_ASSERT_EQ(out.effects.required, 0);
	record_init(&r, RM_MIN_CUSTOM_ID, 0);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out),
				 CLUSTER_COLD_OPCODE_UNSUPPORTED);
}

UT_TEST(test_folded_tt_unknown_format_is_not_native_apply)
{
	EffectRecord r;
	ClusterColdDecodedV1 out = { 0 };
	xl_xact_tt_commit tt;
	Size offset = MinSizeOfXactCommit + sizeof(xl_xact_xinfo) + sizeof(xl_xact_scn);

	for (int field = 0; field < 4; field++) {
		completion(&r, true, XACT_XINFO_HAS_SCN | XACT_XINFO_HAS_TT_COMMIT);
		memcpy(&tt, r.data.bytes + offset, sizeof(tt));
		if (field == 0)
			tt.format_version = 0;
		else if (field == 1)
			tt.format_version++;
		else if (field == 2)
			tt.flags = 1;
		else
			tt.reserved = 1;
		memcpy(r.data.bytes + offset, &tt, sizeof(tt));
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out),
					 CLUSTER_COLD_OPCODE_UNSUPPORTED);
		UT_ASSERT_EQ(out.effects.required, 0);
	}
}

UT_TEST(test_truncation_at_every_compound_boundary_refuses)
{
	EffectRecord r;
	ClusterColdDecodedV1 out = { 0 };
	uint32 full_len;
	completion(&r, true, ALL_XINFO);
	full_len = r.decoded.main_data_len;
	for (uint32 len = 0; len < full_len; len++) {
		r.decoded.main_data_len = len;
		UT_ASSERT(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out)
				  != CLUSTER_COLD_OK);
		UT_ASSERT_EQ(out.effects.required, 0);
	}
}

static ClusterColdStepV1
step_for(const ClusterColdDecodedV1 *out, uint8 kind)
{
	ClusterColdStepV1 step = { 0 };
	step.read_rec_ptr = out->record.read_rec_ptr;
	step.end_rec_ptr = out->record.end_rec_ptr;
	step.record_crc = out->record.record_crc;
	step.rmid = out->record.rmid;
	step.info = out->record.info;
	step.step_kind = kind;
	step.space_count = out->record.space_count;
	if (step.space_count)
		step.space_kind = out->record.space_ops[0].kind;
	return step;
}

UT_TEST(test_dispatch_whole_native_and_refused_compound)
{
	EffectRecord r;
	ClusterColdDecodedV1 out = { 0 };
	completion(&r, true, ALL_XINFO);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT(cluster_cold_replay_effects_v1(&out, NULL, true));
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, false));
	/* Clearing the coarse flag must not hide the unowned effects. */
	out.record.record_flags = 0;
	UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, false));
}

UT_TEST(test_dispatch_partition_must_be_complete_known_and_disjoint)
{
	EffectRecord r;
	ClusterColdDecodedV1 out = { 0 };
	ClusterColdEffectRoutesV1 good;
	completion(&r, true, ALL_XINFO);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out),
				 CLUSTER_COLD_OK);
	good = out.effects;
	for (uint32 bit = 1; bit <= CLUSTER_COLD_EFFECT_SCN; bit <<= 1) {
		if (!(good.required & bit))
			continue;
		out.effects = good;
		out.effects.native_apply &= ~bit;
		UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, true));
		out.effects = good;
		out.effects.no_effect |= bit;
		UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, true));
		out.effects.native_apply &= ~bit;
		UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, true));
	}
	out.effects = good;
	out.effects.required |= 1U << 31;
	out.effects.native_apply |= 1U << 31;
	UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, true));
	memset(&out.effects, 0, sizeof(out.effects));
	UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, true));
	UT_ASSERT(!cluster_cold_replay_effects_v1(NULL, NULL, true));
}

UT_TEST(test_dispatch_no_effect_only_for_original_foreign_control)
{
	EffectRecord r;
	ClusterColdDecodedV1 out = { 0 };
	record_init(&r, RM_XLOG_ID, XLOG_NOOP);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT(cluster_cold_replay_effects_v1(&out, NULL, false));
	UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, true));
	out.effects.required |= CLUSTER_COLD_EFFECT_INVALIDATIONS;
	out.effects.no_effect |= CLUSTER_COLD_EFFECT_INVALIDATIONS;
	UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, false));
}

UT_TEST(test_dispatch_page_skip_does_not_skip_other_consumers)
{
	ClusterColdDecodedV1 out = { 0 };
	ClusterColdStepV1 step;
	out.record.rmid = RM_HEAP_ID;
	out.record.component_count = 1;
	out.record.components = out.components;
	out.effects.required
		= CLUSTER_COLD_EFFECT_RECORD | CLUSTER_COLD_EFFECT_PAGE | CLUSTER_COLD_EFFECT_FSM;
	out.effects.page_apply = out.effects.required;
	step = step_for(&out, CLUSTER_COLD_STEP_PAGE);
	for (int skip = 0; skip <= 1; skip++) {
		step.all_skip = skip;
		UT_ASSERT(cluster_cold_replay_effects_v1(&out, &step, true));
		UT_ASSERT(cluster_cold_replay_effects_v1(&out, &step, false));
	}
	out.effects.required |= CLUSTER_COLD_EFFECT_TERMINAL;
	out.effects.native_apply = CLUSTER_COLD_EFFECT_TERMINAL;
	UT_ASSERT(!cluster_cold_replay_effects_v1(&out, &step, true));
	out.effects.required &= ~CLUSTER_COLD_EFFECT_TERMINAL;
	out.effects.native_apply = 0;
	for (int field = 0; field < 5; field++) {
		step = step_for(&out, CLUSTER_COLD_STEP_PAGE);
		if (field == 0)
			step.read_rec_ptr++;
		if (field == 1)
			step.end_rec_ptr++;
		if (field == 2)
			step.record_crc++;
		if (field == 3)
			step.rmid++;
		if (field == 4)
			step.info++;
		UT_ASSERT(!cluster_cold_replay_effects_v1(&out, &step, true));
	}
	UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, true));
}

UT_TEST(test_dispatch_fsm_only_uses_original_native_redo)
{
	ClusterColdDecodedV1 out = { 0 };
	ClusterColdStepV1 step = { 0 };
	out.effects.required = CLUSTER_COLD_EFFECT_RECORD | CLUSTER_COLD_EFFECT_FSM;
	out.effects.page_apply = out.effects.required;
	UT_ASSERT(cluster_cold_replay_effects_v1(&out, NULL, true));
	UT_ASSERT(cluster_cold_replay_effects_v1(&out, NULL, false));
	UT_ASSERT(!cluster_cold_replay_effects_v1(&out, &step, true));
	out.effects.page_apply = 0;
	out.effects.native_apply = out.effects.required;
	UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, true));
}

UT_TEST(test_dispatch_real_page_decoder_at_pass_two)
{
	EffectRecord *r = calloc(1, sizeof(*r) + sizeof(DecodedBkpBlock));
	ClusterColdDecodedV1 out = { 0 };
	ClusterColdStepV1 step;
	UT_ASSERT(r != NULL);
	for (int fsm = 0; fsm <= 1; fsm++) {
		RfPageVersionEdgeEntryV1 *edge;
		record_init(r, RM_XLOG_ID, XLOG_FPI);
		r->decoded.max_block_id = 0;
		r->decoded.blocks[0].in_use = true;
		r->decoded.blocks[0].forknum = fsm ? FSM_FORKNUM : MAIN_FORKNUM;
		r->decoded.has_page_version_edge = true;
		r->decoded.page_version_edge.entry_count = 1;
		r->decoded.page_version_edge.result_token = 9;
		edge = &r->decoded.page_version_edge.entries[0];
		edge->page_class = fsm ? RF_PAGE_CLASS_REBUILDABLE_FSM : RF_PAGE_CLASS_ORDINARY;
		memset(edge->result_incarnation, 1, sizeof(edge->result_incarnation));
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r->reader, 99, uuid, false, true, &out),
					 CLUSTER_COLD_OK);
		step = step_for(&out, CLUSTER_COLD_STEP_PAGE);
		step.all_skip = true;
		UT_ASSERT(cluster_cold_replay_effects_v1(&out, fsm ? NULL : &step, false));
	}
	free(r);
}

UT_TEST(test_dispatch_space_drop_keeps_entire_native_commit)
{
	EffectRecord r;
	ClusterColdDecodedV1 out = { 0 };
	ClusterColdSpaceOpV1 ops[2]
		= { { .kind = CLUSTER_COLD_SPACE_DROP }, { .kind = CLUSTER_COLD_SPACE_DROP } };
	ClusterColdStepV1 step;
	completion(&r, true, ALL_XINFO);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out),
				 CLUSTER_COLD_OK);
	/* Already decoded SPACE projections; original decoder's payload cases
	 * are exercised by test_cluster_cold_recovery_decode. */
	out.record.space_count = 2;
	out.record.space_ops = ops;
	out.effects.required |= CLUSTER_COLD_EFFECT_SPACE;
	out.effects.space_apply = CLUSTER_COLD_EFFECT_SPACE;
	step = step_for(&out, CLUSTER_COLD_STEP_SPACE);
	UT_ASSERT(cluster_cold_replay_effects_v1(&out, &step, true));
	UT_ASSERT(!cluster_cold_replay_effects_v1(&out, &step, false));
	UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, true));
	step.space_count--;
	UT_ASSERT(!cluster_cold_replay_effects_v1(&out, &step, true));
	step.space_count++;
	ops[1].kind = CLUSTER_COLD_SPACE_CREATE;
	UT_ASSERT(!cluster_cold_replay_effects_v1(&out, &step, true));
	ops[1].kind = CLUSTER_COLD_SPACE_DROP;
	/* Moving an XACT side effect to SPACE cannot make a complete dispatch. */
	out.effects.native_apply &= ~CLUSTER_COLD_EFFECT_SYNC;
	out.effects.space_apply |= CLUSTER_COLD_EFFECT_SYNC;
	UT_ASSERT(!cluster_cold_replay_effects_v1(&out, &step, true));
}

UT_TEST(test_dispatch_standalone_space_and_original_refusal_flags)
{
	ClusterColdDecodedV1 out = { 0 };
	ClusterColdSpaceOpV1 op = { .kind = CLUSTER_COLD_SPACE_CREATE };
	ClusterColdStepV1 step;
	out.record.rmid = RM_SMGR_ID;
	out.record.space_count = 1;
	out.record.space_ops = &op;
	out.effects.required = CLUSTER_COLD_EFFECT_RECORD | CLUSTER_COLD_EFFECT_SPACE;
	out.effects.space_apply = out.effects.required;
	step = step_for(&out, CLUSTER_COLD_STEP_SPACE);
	UT_ASSERT(cluster_cold_replay_effects_v1(&out, &step, true));
	UT_ASSERT(cluster_cold_replay_effects_v1(&out, &step, false));
	UT_ASSERT(cluster_cold_replay_effects_v1(&out, NULL, true));
	for (uint8 flag = 1; flag != 0; flag <<= 1) {
		out.record.record_flags = flag;
		UT_ASSERT(!cluster_cold_replay_effects_v1(&out, &step, true));
	}
	out.record.record_flags = 0;
	out.record.space_ops = NULL;
	UT_ASSERT(!cluster_cold_replay_effects_v1(&out, &step, true));
}


UT_TEST(test_foreign_clog_and_multixact_use_native_side_consumer)
{
	const uint8 rmids[] = { RM_CLOG_ID,		 RM_CLOG_ID,	  RM_MULTIXACT_ID,
							RM_MULTIXACT_ID, RM_MULTIXACT_ID, RM_MULTIXACT_ID };
	const uint8 infos[] = { CLOG_ZEROPAGE,
							CLOG_TRUNCATE,
							XLOG_MULTIXACT_ZERO_OFF_PAGE,
							XLOG_MULTIXACT_ZERO_MEM_PAGE,
							XLOG_MULTIXACT_CREATE_ID,
							XLOG_MULTIXACT_TRUNCATE_ID };
	for (uint32 i = 0; i < lengthof(rmids); i++) {
		EffectRecord r;
		ClusterColdDecodedV1 out = { 0 };
		int page = 0;
		xl_clog_truncate clog = { .pageno = 0, .oldestXact = 100, .oldestXactDb = 5 };
		xl_multixact_create create = { .mid = 5, .moff = 20, .nmembers = 1 };
		MultiXactMember member = { .xid = 100, .status = MultiXactStatusForUpdate };
		xl_multixact_truncate truncate = { .oldestMultiDB = 5,
										   .startTruncOff = 1,
										   .endTruncOff = 5,
										   .startTruncMemb = 1,
										   .endTruncMemb = 20 };
		record_init(&r, rmids[i], infos[i]);
		if (i == 1)
			append(&r, &clog, sizeof(clog));
		else if (i == 4) {
			append(&r, &create, SizeOfMultiXactCreate);
			append(&r, &member, sizeof(member));
		} else if (i == 5)
			append(&r, &truncate, SizeOfMultiXactTruncate);
		else
			append(&r, &page, sizeof(page));
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out),
					 CLUSTER_COLD_OK);
		UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_FOREIGN_SIDE);
		UT_ASSERT_EQ(out.effects.native_apply, CLUSTER_COLD_EFFECT_RECORD);
		UT_ASSERT_EQ(out.effects.no_effect, 0);
		UT_ASSERT_EQ(out.effects.refused, 0);
		UT_ASSERT(partitioned(&out.effects));
		UT_ASSERT_EQ(cluster_cold_unscheduled_v1(&out.record),
					 CLUSTER_COLD_UNSCHEDULED_FOREIGN_SIDE);
		UT_ASSERT(cluster_cold_replay_effects_v1(&out, NULL, false));
		UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, true));
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out),
					 CLUSTER_COLD_OK);
		UT_ASSERT_EQ(out.record.record_flags, 0);
		UT_ASSERT(cluster_cold_replay_effects_v1(&out, NULL, true));
	}
}

UT_TEST(test_foreign_assignment_keeps_native_parent_effect)
{
	EffectRecord r;
	ClusterColdDecodedV1 out = { 0 };
	xl_xact_assignment assignment = { .xtop = 100, .nsubxacts = 1 };
	TransactionId child = 101;
	record_init(&r, RM_XACT_ID, XLOG_XACT_ASSIGNMENT);
	append(&r, &assignment, MinSizeOfXactAssignment);
	append(&r, &child, sizeof(child));
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_FOREIGN_SIDE);
	UT_ASSERT_EQ(out.effects.native_apply,
				 CLUSTER_COLD_EFFECT_RECORD | CLUSTER_COLD_EFFECT_CHILDREN);
	UT_ASSERT(cluster_cold_replay_effects_v1(&out, NULL, false));
	out.effects.no_effect = out.effects.native_apply;
	out.effects.native_apply = 0;
	UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, false));
	r.decoded.main_data_len--;
	UT_ASSERT(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out)
			  != CLUSTER_COLD_OK);
}

UT_TEST(test_foreign_side_dispatch_rejects_mixed_flags_and_unowned_xact)
{
	ClusterColdDecodedV1 out = { 0 };
	EffectRecord r;
	out.record.record_flags = CLUSTER_COLD_RECORD_FOREIGN_SIDE;
	out.record.rmid = RM_CLOG_ID;
	out.record.info = CLOG_ZEROPAGE;
	out.effects.required = out.effects.native_apply = CLUSTER_COLD_EFFECT_RECORD;
	UT_ASSERT(cluster_cold_replay_effects_v1(&out, NULL, false));
	for (uint8 bit = 1; bit != 0; bit <<= 1) {
		if (bit == CLUSTER_COLD_RECORD_FOREIGN_SIDE)
			continue;
		out.record.record_flags = CLUSTER_COLD_RECORD_FOREIGN_SIDE | bit;
		UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, false));
		UT_ASSERT_EQ(cluster_cold_unscheduled_v1(&out.record), CLUSTER_COLD_UNSCHEDULED_REFUSE);
	}
	/* A terminal record is never a record-local no-effect control. */
	completion(&r, true, XACT_XINFO_HAS_SCN);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_SIDE_UNOWNED);
	UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, false));
}

UT_TEST(test_logical_xact_invalidations_keep_physical_no_effect)
{
	EffectRecord r;
	ClusterColdDecodedV1 out = { 0 };
	SharedInvalidationMessage msgs[2] = { 0 };
	msgs[0].rc.id = SHAREDINVALRELCACHE_ID;
	msgs[0].rc.dbId = 5;
	msgs[0].rc.relId = 123;
	msgs[1].cc.id = 0;
	msgs[1].cc.dbId = 5;
	for (int count = 0; count <= 2; count++) {
		record_init(&r, RM_XACT_ID, XLOG_XACT_INVALIDATIONS);
		counted(&r, msgs, count, sizeof(msgs[0]));
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out),
					 CLUSTER_COLD_OK);
		UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_FOREIGN_CONTROL);
		UT_ASSERT_EQ(out.effects.no_effect,
					 CLUSTER_COLD_EFFECT_RECORD | CLUSTER_COLD_EFFECT_INVALIDATIONS);
		UT_ASSERT_EQ(out.effects.native_apply | out.effects.refused, 0);
		UT_ASSERT(partitioned(&out.effects));
		UT_ASSERT(cluster_cold_replay_effects_v1(&out, NULL, false));
		UT_ASSERT_EQ(cluster_cold_unscheduled_v1(&out.record),
					 CLUSTER_COLD_UNSCHEDULED_FOREIGN_NOOP);
		out.record.rmid = RM_STANDBY_ID;
		out.record.info = XLOG_INVALIDATIONS;
		UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, false));
		out.record.rmid = RM_XACT_ID;
		out.record.info = XLOG_XACT_COMMIT;
		UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, false));
		out.record.info = XLOG_XACT_INVALIDATIONS | XLOG_XACT_HAS_INFO;
		UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, false));
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out),
					 CLUSTER_COLD_OK);
		UT_ASSERT_EQ(out.record.record_flags, 0);
		UT_ASSERT_EQ(out.effects.no_effect, 0);
		UT_ASSERT(cluster_cold_replay_effects_v1(&out, NULL, true));
	}
}

UT_TEST(test_logical_xact_invalidations_do_not_hide_bad_shape)
{
	for (int fault = 0; fault < 7; fault++) {
		EffectRecord r;
		ClusterColdDecodedV1 out = { 0 };
		SharedInvalidationMessage msg = { 0 };
		int count;
		record_init(&r, RM_XACT_ID, XLOG_XACT_INVALIDATIONS);
		counted(&r, &msg, 1, sizeof(msg));
		switch (fault) {
		case 0:
			r.decoded.main_data_len = MinSizeOfXactInvals - 1;
			break;
		case 1:
			r.decoded.main_data_len--;
			break;
		case 2:
			r.decoded.main_data_len++;
			break;
		case 3:
		case 4:
			count = fault == 3 ? -1 : INT_MAX;
			memcpy(r.data.bytes, &count, sizeof(count));
			break;
		case 5:
			r.decoded.header.xl_info |= XLOG_XACT_HAS_INFO;
			break;
		case 6:
			r.decoded.header.xl_info |= 1;
			break;
		}
		if (cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out)
			== CLUSTER_COLD_OK) {
			UT_ASSERT(out.record.record_flags != CLUSTER_COLD_RECORD_FOREIGN_CONTROL);
			UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, false));
		}
	}
}


/* Only route shapes here; actual prefix installation uses the production
 * owner and temporary files in test_cluster_side_xact. */
static void
undo_block_effect_record(EffectRecord *r, bool multi, bool fpi)
{
	char body[BLCKSZ] = { 0 };
	xl_undo_block_write single = { .segment_id = 257,
								   .block_no = 9,
								   .instance = 2,
								   .has_fpi = fpi,
								   .rec_off = fpi ? 0 : 128,
								   .rec_len = fpi ? 0 : 16,
								   .slot_off = fpi ? 0 : BLCKSZ - sizeof(UndoSlotDirEntry) };
	xl_undo_block_write_multi multiple
		= { .segment_id = 257,
			.block_no = 9,
			.instance = 2,
			.has_fpi = fpi,
			.rec_off = fpi ? 0 : 128,
			.rec_len = fpi ? 0 : 16,
			.slot_off = fpi ? 0 : BLCKSZ - 2 * sizeof(UndoSlotDirEntry),
			.slot_len = fpi ? 0 : 2 * sizeof(UndoSlotDirEntry) };
	Size slots = multi ? 2 * sizeof(UndoSlotDirEntry) : sizeof(UndoSlotDirEntry);

	record_init(r, RM_CLUSTER_UNDO_ID, multi ? XLOG_UNDO_BLOCK_WRITE_MULTI : XLOG_UNDO_BLOCK_WRITE);
	if (multi)
		append(r, &multiple, sizeof(multiple));
	else
		append(r, &single, sizeof(single));
	append(r, body, fpi ? BLCKSZ : UNDO_BLOCK_HDR_PREFIX_LEN + 16 + slots);
}

UT_TEST(test_foreign_undo_blocks_require_real_record_consumer)
{
	for (int multi = 0; multi < 2; multi++)
		for (int fpi = 0; fpi < 2; fpi++) {
			EffectRecord r;
			ClusterColdDecodedV1 out = { 0 };
			undo_block_effect_record(&r, multi, fpi);
			UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out),
						 CLUSTER_COLD_OK);
			UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_FOREIGN_SIDE);
			UT_ASSERT_EQ(out.effects.native_apply, CLUSTER_COLD_EFFECT_RECORD);
			UT_ASSERT_EQ(out.effects.no_effect | out.effects.refused, 0);
			UT_ASSERT(partitioned(&out.effects));
			UT_ASSERT_EQ(cluster_cold_unscheduled_v1(&out.record),
						 CLUSTER_COLD_UNSCHEDULED_FOREIGN_SIDE);
			UT_ASSERT(cluster_cold_replay_effects_v1(&out, NULL, false));
			UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, true));
			UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out),
						 CLUSTER_COLD_OK);
			UT_ASSERT_EQ(out.record.record_flags, 0);
			UT_ASSERT(cluster_cold_replay_effects_v1(&out, NULL, true));
		}
}

UT_TEST(test_foreign_undo_blocks_reject_malformed_or_unowned_shapes)
{
	const uint8 infos[]
		= { XLOG_UNDO_SEGMENT_INIT,		XLOG_UNDO_TT_SLOT_BIND,		   XLOG_UNDO_TT_SLOT_COMMIT,
			XLOG_UNDO_TT_SLOT_ABORT,	XLOG_UNDO_SEGMENT_RECYCLE,	   XLOG_UNDO_SEGMENT_REUSE,
			XLOG_UNDO_TT_SLOT_SET_HEAD, XLOG_UNDO_TT_SLOT_CTRC_RELEASE };
	for (int multi = 0; multi < 2; multi++)
		for (int fault = 0; fault < 12; fault++) {
			EffectRecord r;
			ClusterColdDecodedV1 out = { 0 };
			xl_undo_block_write *block;
			undo_block_effect_record(&r, multi, true);
			block = (xl_undo_block_write *)r.data.bytes;
			switch (fault) {
			case 0:
				r.decoded.main_data_len = sizeof(*block) - 1;
				break;
			case 1:
				r.decoded.main_data_len--;
				break;
			case 2:
				r.decoded.main_data_len++;
				break;
			case 3:
				block->has_fpi = 2;
				break;
			case 4:
				block->block_no = 0;
				break;
			case 5:
				block->instance = 0;
				break;
			case 6:
				block->segment_id = 1;
				break;
			case 7:
				block->rec_len = 1;
				break;
			case 8:
				block->rec_off = 1;
				break;
			case 9:
				r.decoded.header.xl_info |= 1;
				break;
			case 10:
				r.decoded.header.xl_info = 0xF0;
				break;
			case 11:
				r.decoded.main_data = NULL;
				break;
			}
			if (cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out)
				== CLUSTER_COLD_OK) {
				UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_SIDE_UNOWNED);
				UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, false));
			}
		}
	/* Empty header and TT carriers cannot acquire execution authority. */
	for (uint32 i = 0; i < lengthof(infos); i++) {
		EffectRecord r;
		ClusterColdDecodedV1 out = { 0 };
		record_init(&r, RM_CLUSTER_UNDO_ID, infos[i]);
		if (cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out)
			== CLUSTER_COLD_OK) {
			UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_SIDE_UNOWNED);
			UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, false));
		}
	}
}

static void
undo_header_effect_record(EffectRecord *r, uint8 info)
{
	char page[BLCKSZ] = { 0 };
	xl_cluster_undo_segment_init init = { .segment_id = 257, .instance = 2 };
	xl_undo_segment_reuse reuse
		= { .segment_id = 257, .instance = 2, .old_generation = 0, .new_generation = 1 };
	xl_undo_segment_recycle recycle = { .segment_id = 257,
										.instance = 2,
										.old_state = SEGMENT_COMMITTED,
										.new_state = SEGMENT_RECYCLABLE };
	xl_undo_tt_slot_bind bind = { .segment_id = 257,
								  .instance = 2,
								  .slot_offset = 4,
								  .xid = 802,
								  .wrap = 7,
								  .format_version = CLUSTER_UNDO_TT_BIND_VERSION };

	record_init(r, RM_CLUSTER_UNDO_ID, info);
	if (info == XLOG_UNDO_SEGMENT_INIT)
		append(r, &init, sizeof(init));
	else if (info == XLOG_UNDO_SEGMENT_REUSE)
		append(r, &reuse, sizeof(reuse));
	else if (info == XLOG_UNDO_SEGMENT_RECYCLE)
		append(r, &recycle, sizeof(recycle));
	else
		append(r, &bind, sizeof(bind));
	if (info == XLOG_UNDO_SEGMENT_INIT || info == XLOG_UNDO_SEGMENT_REUSE)
		append(r, page, sizeof(page));
}

UT_TEST(test_foreign_undo_header_routes_only_connected_record_consumers)
{
	const uint8 infos[] = { XLOG_UNDO_SEGMENT_INIT, XLOG_UNDO_SEGMENT_REUSE,
							XLOG_UNDO_SEGMENT_RECYCLE, XLOG_UNDO_TT_SLOT_BIND };
	for (uint32 i = 0; i < lengthof(infos); i++) {
		EffectRecord r;
		ClusterColdDecodedV1 out = { 0 };
		undo_header_effect_record(&r, infos[i]);
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out),
					 CLUSTER_COLD_OK);
		UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_FOREIGN_SIDE);
		UT_ASSERT_EQ(out.effects.native_apply, CLUSTER_COLD_EFFECT_RECORD);
		UT_ASSERT_EQ(out.effects.no_effect | out.effects.refused, 0);
		UT_ASSERT(cluster_cold_replay_effects_v1(&out, NULL, false));
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out),
					 CLUSTER_COLD_OK);
		UT_ASSERT_EQ(out.record.record_flags, 0);
		UT_ASSERT(cluster_cold_replay_effects_v1(&out, NULL, true));
	}
}

UT_TEST(test_foreign_undo_header_malformed_carriers_remain_refused)
{
	const uint8 infos[] = { XLOG_UNDO_SEGMENT_INIT, XLOG_UNDO_SEGMENT_REUSE,
							XLOG_UNDO_SEGMENT_RECYCLE, XLOG_UNDO_TT_SLOT_BIND };
	for (uint32 i = 0; i < lengthof(infos); i++)
		for (int fault = 0; fault < 6; fault++) {
			EffectRecord r;
			ClusterColdDecodedV1 out = { 0 };
			undo_header_effect_record(&r, infos[i]);
			switch (fault) {
			case 0:
				r.decoded.main_data_len--;
				break;
			case 1:
				r.decoded.main_data_len++;
				break;
			case 2:
				r.decoded.header.xl_info |= 1;
				break;
			case 3:
				r.decoded.main_data = NULL;
				break;
			case 4:
				memset(r.data.bytes, 0, sizeof(uint32));
				break;
			case 5:
				r.decoded.header.xl_info = 0xF0;
				break;
			}
			if (cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out)
				== CLUSTER_COLD_OK) {
				UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_SIDE_UNOWNED);
				UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, false));
			}
		}
}

int
main(void)
{
	UT_PLAN(31);
	UT_RUN(test_foreign_undo_header_routes_only_connected_record_consumers);
	UT_RUN(test_foreign_undo_header_malformed_carriers_remain_refused);
	UT_RUN(test_foreign_undo_blocks_require_real_record_consumer);
	UT_RUN(test_foreign_undo_blocks_reject_malformed_or_unowned_shapes);
	UT_RUN(test_logical_xact_invalidations_keep_physical_no_effect);
	UT_RUN(test_logical_xact_invalidations_do_not_hide_bad_shape);
	UT_RUN(test_foreign_clog_and_multixact_use_native_side_consumer);
	UT_RUN(test_foreign_assignment_keeps_native_parent_effect);
	UT_RUN(test_foreign_side_dispatch_rejects_mixed_flags_and_unowned_xact);
	UT_RUN(test_whole_commit_effects_keep_native_owner);
	UT_RUN(test_foreign_compound_cannot_drop_unowned_effects);
	UT_RUN(test_relcache_work_exists_with_zero_messages);
	UT_RUN(test_embedded_twophase_is_not_ordinary_commit);
	UT_RUN(test_commit_ts_is_unsupported_even_for_own_source);
	UT_RUN(test_foreign_invalidation_is_not_no_effect);
	UT_RUN(test_assignment_requires_complete_counted_payload);
	UT_RUN(test_invalidations_require_complete_counted_payload);
	UT_RUN(test_abort_routes_all_native_effects);
	UT_RUN(test_unknown_or_truncated_commit_clears_routes);
	UT_RUN(test_exact_foreign_noop_stays_no_effect);
	UT_RUN(test_registry_unknown_and_blocks_still_refuse);
	UT_RUN(test_folded_tt_unknown_format_is_not_native_apply);
	UT_RUN(test_truncation_at_every_compound_boundary_refuses);
	UT_RUN(test_dispatch_whole_native_and_refused_compound);
	UT_RUN(test_dispatch_partition_must_be_complete_known_and_disjoint);
	UT_RUN(test_dispatch_no_effect_only_for_original_foreign_control);
	UT_RUN(test_dispatch_page_skip_does_not_skip_other_consumers);
	UT_RUN(test_dispatch_fsm_only_uses_original_native_redo);
	UT_RUN(test_dispatch_real_page_decoder_at_pass_two);
	UT_RUN(test_dispatch_space_drop_keeps_entire_native_commit);
	UT_RUN(test_dispatch_standalone_space_and_original_refusal_flags);
	UT_DONE();
	return ut_failed_count != 0;
}
