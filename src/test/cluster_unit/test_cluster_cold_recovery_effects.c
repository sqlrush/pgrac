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
#include "access/twophase.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "catalog/pg_control.h"
#include "catalog/pg_tablespace_d.h"
#include "catalog/storage_xlog.h"
#include "cluster/cluster_block_apply.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_page_detached.h"
#include "cluster/cluster_side_online_plan.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_xid_stripe.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/cluster_recovery_tt_record.h"
#include "cluster/cluster_space_reservation.h"
#include "pgstat.h"
#include "replication/message.h"
#include "replication/origin.h"
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

/* Original xlog adapter; only the final already-tested authority owner is
 * a boundary. The decoder and effect validation below are real code. */
typedef struct ClusterColdTypedReplay {
	ClusterColdTypedV1 *typed;
	struct ClusterRecoveryFencePlan **fence_plan;
	XLogReaderState *current[CLUSTER_COLD_MAX_PARTICIPANTS];
	ClusterColdDecodedV1 decoded;
} ClusterColdTypedReplay;

static unsigned dependency_calls;
static bool dependency_owner_ok;
static uint8 dependency_fence;

bool
cluster_cold_typed_record_target_v1(ClusterColdTypedV1 *typed,
									struct ClusterRecoveryFencePlan *fence, uint32 participant,
									const ClusterColdRecordV1 *record, uint32 *target)
{
	dependency_calls++;
	UT_ASSERT_EQ(participant, 1);
	UT_ASSERT(typed != NULL && fence == (struct ClusterRecoveryFencePlan *)&dependency_fence);
	UT_ASSERT_EQ(record->record_flags, CLUSTER_COLD_RECORD_FOREIGN_SIDE);
	UT_ASSERT_EQ(record->rmid, RM_CLUSTER_UNDO_ID);
	UT_ASSERT_EQ(record->info, XLOG_UNDO_RECOVERY_TT_ABORT);
	if (dependency_owner_ok)
		*target = 2;
	return dependency_owner_ok;
}

#include "test_cluster_cold_dependency.inc"

static const uint8 uuid[16] = { 1 };

static void
record_init(EffectRecord *r, uint8 rmid, uint8 info)
{
	memset(r, 0, sizeof(*r));
	r->reader.record = &r->decoded;
	r->reader.ReadRecPtr = 0x1000;
	r->reader.EndRecPtr = 0x1800;
	/* The real cold reader binds this from the retained source claim. */
	r->reader.cluster_expected_thread_id = 1;
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
	xl_xact_stats_item stats[2] = { { .kind = PGSTAT_KIND_RELATION, .dboid = 5, .objoid = 123 },
									{ .kind = PGSTAT_KIND_FUNCTION, .dboid = 5, .objoid = 124 } };
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
	if (xinfo & XACT_XINFO_HAS_DBINFO) {
		xl_xact_dbinfo db = { .dbId = 5, .tsId = 1663 };
		append(r, &db, sizeof(db));
	}
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

static void
terminal_children(EffectRecord *r, bool commit, TransactionId top, int count,
				  const TransactionId *children)
{
	TimestampTz timestamp = 1;
	uint32 xinfo = XACT_XINFO_HAS_SUBXACTS | XACT_XINFO_HAS_SCN;
	xl_xact_scn scn = { .scn = 23 };
	xl_xact_tt_commit tt = { .segment_id = 1,
							 .segment_generation = 0,
							 .xid = top,
							 .slot_offset = 1,
							 .wrap = 1,
							 .instance = 1,
							 .format_version = CLUSTER_XACT_TT_COMMIT_VERSION,
							 .commit_scn = 23 };

	if (commit)
		xinfo |= XACT_XINFO_HAS_TT_COMMIT;
	record_init(r, RM_XACT_ID, (commit ? XLOG_XACT_COMMIT : XLOG_XACT_ABORT) | XLOG_XACT_HAS_INFO);
	r->decoded.header.xl_xid = top;
	append(r, &timestamp, sizeof(timestamp));
	append(r, &xinfo, sizeof(xinfo));
	counted(r, children, count, sizeof(*children));
	append(r, &scn, sizeof(scn));
	if (commit)
		append(r, &tt, sizeof(tt));
}

static void
check_bad_terminal_children(bool commit, bool foreign)
{
	static const struct {
		TransactionId top;
		TransactionId children[2];
	} bad[] = { { 100, { InvalidTransactionId, 102 } },
				{ 100, { BootstrapTransactionId, 102 } },
				{ 100, { FrozenTransactionId, 102 } },
				{ 100, { 100, 102 } },
				{ 100, { 101, 101 } },
				{ 100, { 103, 102 } },
				{ 100, { 99, 102 } },
				{ 100, { 100U + 0x80000000U, 102 } },
				{ InvalidTransactionId, { 101, 102 } },
				{ BootstrapTransactionId, { 101, 102 } },
				{ FrozenTransactionId, { 101, 102 } } };

	for (Size i = 0; i < lengthof(bad); i++) {
		EffectRecord r;
		ClusterColdDecodedV1 out = { 0 };

		terminal_children(&r, commit, bad[i].top, 2, bad[i].children);
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, foreign, &out),
					 CLUSTER_COLD_COMPONENT_INVALID);
		UT_ASSERT_EQ(out.effects.required, 0);
		UT_ASSERT_EQ(out.effects.native_apply | out.effects.page_apply | out.effects.space_apply
						 | out.effects.no_effect | out.effects.refused,
					 0);
	}
}

UT_TEST(test_own_commit_rejects_invalid_terminal_children)
{
	check_bad_terminal_children(true, false);
}

UT_TEST(test_own_abort_rejects_invalid_terminal_children)
{
	check_bad_terminal_children(false, false);
}

UT_TEST(test_foreign_commit_rejects_invalid_terminal_children)
{
	check_bad_terminal_children(true, true);
}

UT_TEST(test_foreign_abort_rejects_invalid_terminal_children)
{
	check_bad_terminal_children(false, true);
}

static void
check_terminal_children(TransactionId top, int count, const TransactionId *children)
{
	for (int commit = 0; commit <= 1; commit++) {
		for (int foreign = 0; foreign <= 1; foreign++) {
			EffectRecord r;
			ClusterColdDecodedV1 out = { 0 };

			terminal_children(&r, commit, top, count, children);
			UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, foreign, &out),
						 CLUSTER_COLD_OK);
			UT_ASSERT_EQ(out.record.record_flags, foreign ? CLUSTER_COLD_RECORD_FOREIGN_SIDE : 0);
			UT_ASSERT(out.effects.native_apply & CLUSTER_COLD_EFFECT_CHILDREN);
			UT_ASSERT_EQ(out.effects.refused | out.effects.no_effect, 0);
			UT_ASSERT(partitioned(&out.effects));
		}
	}
}

UT_TEST(test_terminal_children_allow_native_xid_wrap)
{
	TransactionId children[] = { UINT32_MAX, FirstNormalTransactionId };

	check_terminal_children(UINT32_MAX - 1, lengthof(children), children);
}

UT_TEST(test_terminal_children_are_not_limited_to_assignment_cache)
{
	TransactionId children[256];

	for (Size i = 0; i < lengthof(children); i++)
		children[i] = 101 + i;
	check_terminal_children(100, lengthof(children), children);
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

/* These are the original XLOG control carriers, not page/FPI records. */
static const struct {
	uint8 info;
	Size length;
} xlog_controls[] = { { XLOG_CHECKPOINT_SHUTDOWN, sizeof(CheckPoint) },
					  { XLOG_CHECKPOINT_ONLINE, sizeof(CheckPoint) },
					  { XLOG_NEXTOID, sizeof(Oid) },
					  { XLOG_PARAMETER_CHANGE, sizeof(xl_parameter_change) },
					  { XLOG_FPW_CHANGE, sizeof(bool) },
					  { XLOG_SWITCH, 0 },
					  { XLOG_NOOP, 4 },
					  { XLOG_BACKUP_END, sizeof(XLogRecPtr) },
					  { XLOG_RESTORE_POINT, sizeof(xl_restore_point) },
					  { XLOG_END_OF_RECOVERY, sizeof(xl_end_of_recovery) },
					  { XLOG_OVERWRITE_CONTRECORD, sizeof(xl_overwrite_contrecord) } };

static void
xlog_control(EffectRecord *r, Size index, int variant)
{
	union {
		CheckPoint checkpoint;
		xl_parameter_change params;
		xl_restore_point restore;
		xl_end_of_recovery end;
		xl_overwrite_contrecord overwrite;
		Oid oid;
		XLogRecPtr lsn;
		bool fpw;
		char bytes[sizeof(CheckPoint)];
	} payload = { 0 };
	bool enabled = (variant & 1) != 0;
	uint8 info = xlog_controls[index].info;

	record_init(r, RM_XLOG_ID, info);
	switch (info) {
	case XLOG_CHECKPOINT_SHUTDOWN:
	case XLOG_CHECKPOINT_ONLINE:
		payload.checkpoint = (CheckPoint){ .redo = 0x900,
										   .ThisTimeLineID = 2,
										   .PrevTimeLineID = 2,
										   .fullPageWrites = enabled,
										   .nextXid = FullTransactionIdFromU64(300),
										   .nextOid = 18000,
										   .nextMulti = 10,
										   .nextMultiOffset = 20,
										   .oldestXid = 100,
										   .oldestMulti = 1 };
		/* Native padding is not a new persistent field. */
		memset(payload.bytes + offsetof(CheckPoint, fullPageWrites) + sizeof(bool), 0xa5,
			   offsetof(CheckPoint, nextXid) - offsetof(CheckPoint, fullPageWrites) - sizeof(bool));
		break;
	case XLOG_NEXTOID:
		payload.oid = 18000;
		break;
	case XLOG_PARAMETER_CHANGE:
		payload.params = (xl_parameter_change){ .MaxConnections = 100,
												.max_worker_processes = 8,
												.max_wal_senders = 10,
												.max_prepared_xacts = 0,
												.max_locks_per_xact = 64,
												.wal_level = 1,
												.wal_log_hints = enabled,
												.track_commit_timestamp = !enabled };
		memset(payload.bytes + offsetof(xl_parameter_change, track_commit_timestamp) + sizeof(bool),
			   0xa5,
			   sizeof(xl_parameter_change) - offsetof(xl_parameter_change, track_commit_timestamp)
				   - sizeof(bool));
		break;
	case XLOG_FPW_CHANGE:
		payload.fpw = enabled;
		break;
	case XLOG_NOOP:
		memcpy(payload.bytes, "\0\xff\x01\x80", 4);
		break;
	case XLOG_BACKUP_END:
		payload.lsn = 0x900;
		break;
	case XLOG_RESTORE_POINT:
		payload.restore.rp_time = 123;
		memset(payload.restore.rp_name, 'r', sizeof(payload.restore.rp_name));
		payload.restore.rp_name[enabled ? MAXFNAMELEN - 1 : 0] = '\0';
		break;
	case XLOG_END_OF_RECOVERY:
		payload.end
			= (xl_end_of_recovery){ .end_time = 123, .ThisTimeLineID = 2, .PrevTimeLineID = 1 };
		break;
	case XLOG_OVERWRITE_CONTRECORD:
		payload.overwrite
			= (xl_overwrite_contrecord){ .overwritten_lsn = 0x900, .overwrite_time = 123 };
		break;
	}
	append(r, payload.bytes, xlog_controls[index].length);
	if (variant & 2) {
		memmove(r->data.bytes + 1, r->data.bytes, r->decoded.main_data_len);
		r->decoded.main_data = r->data.bytes + 1;
	} else if (r->decoded.main_data_len == 0)
		r->decoded.main_data = NULL;
}

static bool
xlog_control_refused(EffectRecord *r)
{
	ClusterColdDecodedV1 out = { 0 };
	for (int foreign = 0; foreign <= 1; foreign++) {
		if (cluster_cold_recovery_decode_v1(&r->reader, 99, uuid, true, foreign, &out)
				== CLUSTER_COLD_OK
			|| out.effects.required != 0 || out.effects.no_effect != 0
			|| out.effects.native_apply != 0
			|| cluster_cold_replay_effects_v1(&out, NULL, !foreign))
			return false;
	}
	return true;
}

UT_TEST(test_xlog_controls_keep_native_and_no_effect_routes)
{
	EffectRecord r;
	ClusterColdDecodedV1 out = { 0 };
	for (Size i = 0; i < lengthof(xlog_controls); i++) {
		for (int variant = 0; variant < 4; variant++) {
			xlog_control(&r, i, variant);
			for (int foreign = 0; foreign <= 1; foreign++) {
				UT_ASSERT_EQ(
					cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, foreign, &out),
					CLUSTER_COLD_OK);
				UT_ASSERT_EQ(out.record.record_flags,
							 foreign ? CLUSTER_COLD_RECORD_FOREIGN_CONTROL : 0);
				UT_ASSERT_EQ(out.effects.required, CLUSTER_COLD_EFFECT_RECORD);
				UT_ASSERT_EQ(out.effects.no_effect, foreign ? CLUSTER_COLD_EFFECT_RECORD : 0);
				UT_ASSERT_EQ(out.effects.native_apply, foreign ? 0 : CLUSTER_COLD_EFFECT_RECORD);
				UT_ASSERT_EQ(out.effects.refused, 0);
				UT_ASSERT(partitioned(&out.effects));
				UT_ASSERT(cluster_cold_replay_effects_v1(&out, NULL, !foreign));
				UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, foreign));
			}
		}
	}
}

UT_TEST(test_xlog_controls_reject_incomplete_envelopes)
{
	EffectRecord r;
	for (Size i = 0; i < lengthof(xlog_controls); i++) {
		if (xlog_controls[i].info != XLOG_NOOP) {
			for (Size length = 0; length <= xlog_controls[i].length + 1; length++) {
				if (length == xlog_controls[i].length)
					continue;
				xlog_control(&r, i, 0);
				r.decoded.main_data_len = length;
				UT_ASSERT(xlog_control_refused(&r));
			}
		}
		for (int fault = 0; fault < 6; fault++) {
			union {
				DecodedXLogRecord decoded;
				char bytes[sizeof(DecodedXLogRecord) + sizeof(DecodedBkpBlock)];
			} blocked = { 0 };
			xlog_control(&r, i, 0);
			if (fault == 0) {
				if (r.decoded.main_data_len == 0)
					continue;
				r.decoded.main_data = NULL;
			} else if (fault <= 4)
				r.decoded.header.xl_info |= 1 << (fault - 1);
			else {
				memcpy(&blocked.decoded, &r.decoded, sizeof(r.decoded));
				blocked.decoded.max_block_id = 0;
				blocked.decoded.blocks[0].in_use = true;
				r.reader.record = &blocked.decoded;
			}
			UT_ASSERT(xlog_control_refused(&r));
		}
	}
	record_init(&r, RM_XLOG_ID, 0xc0);
	UT_ASSERT(xlog_control_refused(&r));
}

UT_TEST(test_xlog_controls_reject_invalid_native_fields)
{
	EffectRecord r;
	static const struct {
		Size index;
		Size offset;
	} bools[] = { { 0, offsetof(CheckPoint, fullPageWrites) },
				  { 1, offsetof(CheckPoint, fullPageWrites) },
				  { 3, offsetof(xl_parameter_change, wal_log_hints) },
				  { 3, offsetof(xl_parameter_change, track_commit_timestamp) },
				  { 4, 0 } };
	static const unsigned char invalid[] = { 2, 128, 255 };
	for (int variant = 0; variant < 4; variant++) {
		for (Size i = 0; i < lengthof(bools); i++) {
			for (Size v = 0; v < lengthof(invalid); v++) {
				xlog_control(&r, bools[i].index, variant);
				r.decoded.main_data[bools[i].offset] = invalid[v];
				UT_ASSERT(xlog_control_refused(&r));
			}
		}
		xlog_control(&r, 8, variant);
		memset(r.decoded.main_data + offsetof(xl_restore_point, rp_name), 'x', MAXFNAMELEN);
		UT_ASSERT(xlog_control_refused(&r));
	}
}

static void
logical_message(EffectRecord *r, bool transactional, const char *prefix, const char *message,
				Size length)
{
	xl_logical_message header = { .dbId = 5,
								  .transactional = transactional,
								  .prefix_size = strlen(prefix) + 1,
								  .message_size = length };

	record_init(r, RM_LOGICALMSG_ID, XLOG_LOGICAL_MESSAGE);
	append(r, &header, SizeOfLogicalMessage);
	append(r, prefix, header.prefix_size);
	append(r, message, length);
}

UT_TEST(test_logical_message_keeps_complete_native_no_effect_carrier)
{
	const char message[] = { '\0', (char)0xff, 'x', '\0' };

	for (int variant = 0; variant < 32; variant++) {
		for (int foreign = 0; foreign <= 1; foreign++) {
			EffectRecord r;
			ClusterColdDecodedV1 out = { 0 };

			logical_message(&r, (variant & 1) != 0, variant & 2 ? "prefix" : "", message,
							variant & 4 ? sizeof(message) : 0);
			r.decoded.record_origin = variant & 8 ? 7 : InvalidRepOriginId;
			if (variant & 16) {
				memmove(r.data.bytes + 1, r.data.bytes, r.decoded.main_data_len);
				r.decoded.main_data++;
			}
			UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, foreign, &out),
						 CLUSTER_COLD_OK);
			UT_ASSERT_EQ(out.record.record_flags,
						 foreign ? CLUSTER_COLD_RECORD_FOREIGN_CONTROL : 0);
			UT_ASSERT_EQ(out.effects.required, CLUSTER_COLD_EFFECT_RECORD);
			UT_ASSERT_EQ(out.effects.native_apply, foreign ? 0 : CLUSTER_COLD_EFFECT_RECORD);
			UT_ASSERT_EQ(out.effects.no_effect, foreign ? CLUSTER_COLD_EFFECT_RECORD : 0);
			UT_ASSERT_EQ(out.effects.refused, 0);
			UT_ASSERT(partitioned(&out.effects));
			UT_ASSERT(cluster_cold_replay_effects_v1(&out, NULL, !foreign));
		}
	}
}

UT_TEST(test_logical_message_requires_every_native_payload_byte)
{
	const char message[] = { '\0', 'x', (char)0xff };
	EffectRecord complete;

	logical_message(&complete, true, "prefix", message, sizeof(message));
	for (uint32 length = 0; length < complete.decoded.main_data_len; length++) {
		for (int foreign = 0; foreign <= 1; foreign++) {
			EffectRecord r;
			ClusterColdDecodedV1 out = { 0 };

			logical_message(&r, true, "prefix", message, sizeof(message));
			r.decoded.main_data_len = length;
			UT_ASSERT(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, foreign, &out)
					  != CLUSTER_COLD_OK);
			UT_ASSERT_EQ(out.effects.required, 0);
			UT_ASSERT_EQ(out.effects.no_effect | out.effects.native_apply, 0);
			UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, !foreign));
		}
	}
}

UT_TEST(test_logical_message_rejects_nonproducer_carriers)
{
	const char message[] = { '\0', 'x', (char)0xff };

	for (int fault = 0; fault < 16; fault++) {
		for (int foreign = 0; foreign <= 1; foreign++) {
			EffectRecord r;
			ClusterColdDecodedV1 out = { 0 };
			Size size;
			union {
				DecodedXLogRecord decoded;
				char bytes[sizeof(DecodedXLogRecord) + sizeof(DecodedBkpBlock)];
			} blocked = { 0 };

			logical_message(&r, true, "prefix", message, sizeof(message));
			switch (fault) {
			case 0:
				r.decoded.main_data = NULL;
				break;
			case 1:
			case 2:
			case 3:
				size = fault == 1 ? 0 : fault == 2 ? r.decoded.main_data_len : SIZE_MAX;
				memcpy(r.data.bytes + offsetof(xl_logical_message, prefix_size), &size,
					   sizeof(size));
				break;
			case 4:
			case 5:
				size = fault == 4 ? SIZE_MAX : sizeof(message) - 1;
				memcpy(r.data.bytes + offsetof(xl_logical_message, message_size), &size,
					   sizeof(size));
				break;
			case 6:
				r.data.bytes[SizeOfLogicalMessage + strlen("prefix")] = 'x';
				break;
			case 7:
				r.data.bytes[SizeOfLogicalMessage + 1] = '\0';
				break;
			case 8:
				r.decoded.main_data_len++;
				break;
			case 9:
				r.data.bytes[offsetof(xl_logical_message, transactional)] = 2;
				break;
			case 10:
				r.decoded.header.xl_info = 0x10;
				break;
			case 11:
			case 12:
			case 13:
			case 14:
				r.decoded.header.xl_info |= 1 << (fault - 11);
				break;
			case 15:
				memcpy(&blocked.decoded, &r.decoded, sizeof(r.decoded));
				blocked.decoded.max_block_id = 0;
				blocked.decoded.blocks[0].in_use = true;
				r.reader.record = &blocked.decoded;
				break;
			}
			UT_ASSERT(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, foreign, &out)
					  != CLUSTER_COLD_OK);
			UT_ASSERT_EQ(out.effects.required, 0);
			UT_ASSERT_EQ(out.effects.no_effect | out.effects.native_apply, 0);
			UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, !foreign));
		}
	}
}

static void
smgr_record(EffectRecord *r, bool truncate, bool shared, ForkNumber fork)
{
	RelFileLocator locator = { .spcOid = shared ? GLOBALTABLESPACE_OID : DEFAULTTABLESPACE_OID,
							   .dbOid = shared ? InvalidOid : 5,
							   .relNumber = 42 };

	record_init(r, RM_SMGR_ID,
				(truncate ? XLOG_SMGR_TRUNCATE : XLOG_SMGR_CREATE) | XLR_SPECIAL_REL_UPDATE);
	if (truncate) {
		xl_smgr_truncate data = { .blkno = 0, .rlocator = locator, .flags = SMGR_TRUNCATE_ALL };

		append(r, &data, sizeof(data));
	} else {
		xl_smgr_create data = { .rlocator = locator, .forkNum = fork };

		append(r, &data, sizeof(data));
	}
}

UT_TEST(test_smgr_native_carriers_keep_existing_effect_owners)
{
	for (int truncate = 0; truncate <= 1; truncate++) {
		for (int foreign = 0; foreign <= 1; foreign++) {
			for (int shared = 0; shared <= 1; shared++) {
				for (ForkNumber fork = MAIN_FORKNUM; fork <= MAX_FORKNUM; fork++) {
					for (int consistency = 0; consistency <= 1; consistency++) {
						EffectRecord r;
						ClusterColdDecodedV1 out = { 0 };

						smgr_record(&r, truncate, shared, fork);
						if (consistency)
							r.decoded.header.xl_info |= XLR_CHECK_CONSISTENCY;
						UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true,
																	 foreign, &out),
									 CLUSTER_COLD_OK);
						UT_ASSERT(partitioned(&out.effects));
						UT_ASSERT_EQ(out.effects.required, CLUSTER_COLD_EFFECT_RECORD);
						UT_ASSERT_EQ(out.effects.native_apply,
									 truncate ? 0 : CLUSTER_COLD_EFFECT_RECORD);
						UT_ASSERT_EQ(out.effects.refused,
									 truncate ? CLUSTER_COLD_EFFECT_RECORD : 0);
						UT_ASSERT_EQ(out.record.record_flags,
									 truncate
										 ? CLUSTER_COLD_RECORD_STRUCTURAL
											   | (foreign ? CLUSTER_COLD_RECORD_SIDE_UNOWNED : 0)
										 : 0);
					}
				}
			}
		}
	}
}

static void
check_bad_smgr_carriers(bool truncate)
{
	for (int foreign = 0; foreign <= 1; foreign++) {
		for (int fault = 0; fault < 14; fault++) {
			EffectRecord r;
			ClusterColdDecodedV1 out = { 0 };
			xl_smgr_create create;
			xl_smgr_truncate shrink;
			RelFileLocator *locator;

			smgr_record(&r, truncate, false, MAIN_FORKNUM);
			if (truncate) {
				memcpy(&shrink, r.data.bytes, sizeof(shrink));
				locator = &shrink.rlocator;
			} else {
				memcpy(&create, r.data.bytes, sizeof(create));
				locator = &create.rlocator;
			}
			switch (fault) {
			case 0:
				r.decoded.main_data_len = 0;
				break;
			case 1:
				r.decoded.main_data_len--;
				break;
			case 2:
				r.decoded.main_data_len++;
				break;
			case 3:
				r.decoded.main_data = NULL;
				break;
			case 4:
				r.decoded.header.xl_info &= ~XLR_SPECIAL_REL_UPDATE;
				break;
			case 5:
				r.decoded.header.xl_info |= 0x08;
				break;
			case 6:
				locator->spcOid = InvalidOid;
				break;
			case 7:
				locator->relNumber = InvalidRelFileNumber;
				break;
			case 8:
				locator->dbOid = InvalidOid;
				break;
			case 9:
				locator->spcOid = GLOBALTABLESPACE_OID;
				break;
			case 10:
				if (truncate)
					shrink.flags = 0x08;
				else
					create.forkNum = MAX_FORKNUM + 1;
				break;
			case 11:
				if (truncate)
					shrink.blkno = InvalidBlockNumber;
				else
					create.forkNum = InvalidForkNumber;
				break;
			case 12:
				if (truncate)
					shrink.flags = 0;
				else
					r.decoded.main_data_len = UINT32_MAX;
				break;
			case 13:
				r.decoded.header.xl_info |= 0x04;
				break;
			}
			if (truncate)
				memcpy(r.data.bytes, &shrink, sizeof(shrink));
			else
				memcpy(r.data.bytes, &create, sizeof(create));
			UT_ASSERT(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, foreign, &out)
					  != CLUSTER_COLD_OK);
			UT_ASSERT_EQ(out.effects.required, 0);
			UT_ASSERT_EQ(out.effects.native_apply | out.effects.no_effect, 0);
			UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, !foreign));
		}
		/* A complete payload still cannot own a hidden page effect. */
		{
			EffectRecord r;
			ClusterColdDecodedV1 out = { 0 };
			union {
				DecodedXLogRecord decoded;
				char bytes[sizeof(DecodedXLogRecord) + sizeof(DecodedBkpBlock)];
			} blocked = { 0 };

			smgr_record(&r, truncate, false, MAIN_FORKNUM);
			memcpy(&blocked.decoded, &r.decoded, sizeof(r.decoded));
			blocked.decoded.max_block_id = 0;
			blocked.decoded.blocks[0].in_use = true;
			r.reader.record = &blocked.decoded;
			UT_ASSERT(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, foreign, &out)
					  != CLUSTER_COLD_OK);
			UT_ASSERT_EQ(out.effects.required, 0);
		}
	}
}

UT_TEST(test_smgr_create_rejects_incomplete_or_nonproducer_carriers)
{
	check_bad_smgr_carriers(false);
}

UT_TEST(test_smgr_truncate_rejects_incomplete_or_nonproducer_carriers)
{
	check_bad_smgr_carriers(true);
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

static void
check_bad_terminal_tt_delta(bool foreign)
{
	for (int field = 0; field < 16; field++) {
		EffectRecord r;
		ClusterColdDecodedV1 out = { 0 };
		xl_xact_tt_commit tt;
		bool missing_scn = field == 13 || field == 14;
		Size offset
			= MinSizeOfXactCommit + sizeof(xl_xact_xinfo) + (missing_scn ? 0 : sizeof(xl_xact_scn));

		completion(&r, true, XACT_XINFO_HAS_TT_COMMIT | (missing_scn ? 0 : XACT_XINFO_HAS_SCN));
		memcpy(&tt, r.data.bytes + offset, sizeof(tt));
		switch (field) {
		case 0:
			tt.instance = 0;
			break;
		case 1:
			tt.instance = 2;
			break;
		case 2:
			tt.segment_id = 0;
			break;
		case 3:
			tt.segment_id = CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
			break;
		case 4:
			tt.segment_generation = UINT32_MAX;
			break;
		case 5:
			tt.slot_offset = TT_SLOTS_PER_SEGMENT;
			break;
		case 6:
			tt.wrap = TT_WRAP_INVALID;
			break;
		case 7:
			tt.xid++;
			break;
		case 8:
			tt.commit_scn++;
			break;
		case 9:
			tt.xid = r.decoded.header.xl_xid = InvalidTransactionId;
			break;
		case 10:
			tt.xid = r.decoded.header.xl_xid = BootstrapTransactionId;
			break;
		case 11:
			tt.xid = r.decoded.header.xl_xid = FrozenTransactionId;
			break;
		case 12: {
			xl_xact_scn scn = { .scn = InvalidScn };

			memcpy(r.data.bytes + MinSizeOfXactCommit + sizeof(xl_xact_xinfo), &scn, sizeof(scn));
			tt.commit_scn = InvalidScn;
			break;
		}
		case 13:
			break; /* A valid delta cannot supply the missing record SCN. */
		case 14:
			tt.commit_scn = InvalidScn;
			break;
		case 15:
			r.reader.cluster_expected_thread_id = tt.instance = 129;
			tt.segment_id = 128 * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
			break;
		}
		memcpy(r.data.bytes + offset, &tt, sizeof(tt));
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, foreign, &out),
					 CLUSTER_COLD_COMPONENT_INVALID);
		UT_ASSERT_EQ(out.effects.required, 0);
		UT_ASSERT_EQ(out.effects.native_apply | out.effects.page_apply | out.effects.space_apply
						 | out.effects.no_effect | out.effects.refused,
					 0);
	}
}

UT_TEST(test_own_commit_rejects_invalid_tt_delta_binding)
{
	check_bad_terminal_tt_delta(false);
}

UT_TEST(test_foreign_commit_rejects_invalid_tt_delta_binding)
{
	check_bad_terminal_tt_delta(true);
}

UT_TEST(test_terminal_tt_delta_preserves_valid_physical_boundaries)
{
	for (int foreign = 0; foreign <= 1; foreign++) {
		for (int field = 0; field < 8; field++) {
			EffectRecord r;
			ClusterColdDecodedV1 out = { 0 };
			xl_xact_tt_commit tt;
			Size offset = MinSizeOfXactCommit + sizeof(xl_xact_xinfo) + sizeof(xl_xact_scn);

			completion(&r, true, XACT_XINFO_HAS_SCN | XACT_XINFO_HAS_TT_COMMIT);
			memcpy(&tt, r.data.bytes + offset, sizeof(tt));
			switch (field) {
			case 0:
				break; /* Generation zero is a valid original segment. */
			case 1:
				tt.segment_generation = UINT32_MAX - 1;
				break;
			case 2:
				tt.wrap = 0;
				break;
			case 3:
				tt.wrap = TT_WRAP_INVALID - 1;
				break;
			case 4:
				tt.slot_offset = 0;
				break;
			case 5:
				tt.slot_offset = TT_SLOTS_PER_SEGMENT - 1;
				break;
			case 6:
				r.reader.cluster_expected_thread_id = tt.instance = 128;
				tt.segment_id = 128 * CLUSTER_UNDO_SEGS_PER_INSTANCE;
				break;
			case 7:
				tt.xid = r.decoded.header.xl_xid = UINT32_MAX;
				break;
			}
			memcpy(r.data.bytes + offset, &tt, sizeof(tt));
			UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, foreign, &out),
						 CLUSTER_COLD_OK);
			UT_ASSERT_EQ(out.record.record_flags, foreign ? CLUSTER_COLD_RECORD_FOREIGN_SIDE : 0);
			UT_ASSERT(out.effects.native_apply & CLUSTER_COLD_EFFECT_TT);
			UT_ASSERT_EQ(out.effects.refused | out.effects.no_effect, 0);
			UT_ASSERT(partitioned(&out.effects));
		}
	}
}

UT_TEST(test_own_commit_without_folded_tt_keeps_native_route)
{
	uint32 variants[] = { 0, XACT_XINFO_HAS_SCN | XACT_XINFO_HAS_SUBXACTS };

	for (Size i = 0; i < lengthof(variants); i++) {
		EffectRecord r;
		ClusterColdDecodedV1 out = { 0 };

		completion(&r, true, variants[i]);
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out),
					 CLUSTER_COLD_OK);
		UT_ASSERT_EQ(out.record.record_flags, 0);
		UT_ASSERT(out.effects.native_apply & CLUSTER_COLD_EFFECT_TERMINAL);
		UT_ASSERT_EQ(out.effects.required & CLUSTER_COLD_EFFECT_TT, 0);
		UT_ASSERT(partitioned(&out.effects));
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


static void
slru_record(EffectRecord *r, uint32 kind)
{
	const uint8 rmids[] = { RM_CLOG_ID,		 RM_CLOG_ID,	  RM_MULTIXACT_ID,
							RM_MULTIXACT_ID, RM_MULTIXACT_ID, RM_MULTIXACT_ID };
	const uint8 infos[] = { CLOG_ZEROPAGE,
							CLOG_TRUNCATE,
							XLOG_MULTIXACT_ZERO_OFF_PAGE,
							XLOG_MULTIXACT_ZERO_MEM_PAGE,
							XLOG_MULTIXACT_CREATE_ID,
							XLOG_MULTIXACT_TRUNCATE_ID };
	int page = 0;
	xl_clog_truncate clog = { .pageno = 0, .oldestXact = 100, .oldestXactDb = 5 };
	xl_multixact_create create = { .mid = 5, .moff = 20, .nmembers = 1 };
	MultiXactMember member = { .xid = 100, .status = MultiXactStatusForUpdate };
	xl_multixact_truncate truncate = { .oldestMultiDB = 5,
									   .startTruncOff = 1,
									   .endTruncOff = 5,
									   .startTruncMemb = 1,
									   .endTruncMemb = 20 };

	record_init(r, rmids[kind], infos[kind]);
	if (kind == 1)
		append(r, &clog, sizeof(clog));
	else if (kind == 4) {
		append(r, &create, SizeOfMultiXactCreate);
		append(r, &member, sizeof(member));
	} else if (kind == 5)
		append(r, &truncate, SizeOfMultiXactTruncate);
	else
		append(r, &page, sizeof(page));
}

UT_TEST(test_foreign_clog_and_multixact_use_native_side_consumer)
{
	for (uint32 i = 0; i < 6; i++) {
		EffectRecord r;
		ClusterColdDecodedV1 out = { 0 };

		slru_record(&r, i);
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

UT_TEST(test_slru_native_carriers_preserve_consistency_and_member_range)
{
	for (uint32 kind = 0; kind < 6; kind++) {
		EffectRecord r;
		ClusterColdDecodedV1 out = { 0 };

		slru_record(&r, kind);
		r.decoded.header.xl_info |= XLR_CHECK_CONSISTENCY;
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out),
					 CLUSTER_COLD_OK);
		UT_ASSERT_EQ(out.record.record_flags, 0);
		UT_ASSERT_EQ(out.effects.native_apply, CLUSTER_COLD_EFFECT_RECORD);
		UT_ASSERT_EQ(out.effects.refused | out.effects.no_effect, 0);
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out),
					 CLUSTER_COLD_OK);
		UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_SIDE_UNOWNED);
		UT_ASSERT_EQ(out.effects.refused, out.effects.required);
	}
	for (int count = 256; count <= 257; count++) {
		EffectRecord r;
		ClusterColdDecodedV1 out = { 0 };
		xl_multixact_create create = { .mid = 5, .moff = 20, .nmembers = count };
		MultiXactMember member = { .xid = 101, .status = MultiXactStatusForKeyShare };

		record_init(&r, RM_MULTIXACT_ID, XLOG_MULTIXACT_CREATE_ID);
		append(&r, &create, SizeOfMultiXactCreate);
		for (int i = 0; i < count; i++)
			append(&r, &member, sizeof(member));
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out),
					 CLUSTER_COLD_OK);
		UT_ASSERT_EQ(out.effects.native_apply, CLUSTER_COLD_EFFECT_RECORD);
		UT_ASSERT_EQ(out.record.record_flags, 0);
		UT_ASSERT(out.xid_input.complete);
		UT_ASSERT_EQ(out.xid_input.payload_xid, 101);
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out),
					 CLUSTER_COLD_OK);
		UT_ASSERT_EQ(out.record.record_flags,
					 count == 256 ? CLUSTER_COLD_RECORD_FOREIGN_SIDE
								  : CLUSTER_COLD_RECORD_SIDE_UNOWNED);
	}
}

UT_TEST(test_slru_carriers_reject_incomplete_envelopes)
{
	for (int foreign = 0; foreign <= 1; foreign++) {
		for (uint32 kind = 0; kind < 6; kind++) {
			EffectRecord complete;

			slru_record(&complete, kind);
			/* Every missing suffix, a trailing byte, missing storage, and
			 * nonproducer flags must fail before native effect ownership. */
			for (uint32 fault = 0; fault <= complete.decoded.main_data_len + 4; fault++) {
				EffectRecord r;
				ClusterColdDecodedV1 out = { 0 };
				ClusterColdDetailV1 detail;
				uint32 length = complete.decoded.main_data_len;
				union {
					DecodedXLogRecord decoded;
					char bytes[sizeof(DecodedXLogRecord) + sizeof(DecodedBkpBlock)];
				} blocked = { 0 };

				slru_record(&r, kind);
				if (fault < length)
					r.decoded.main_data_len = fault;
				else if (fault == length)
					r.decoded.main_data_len++;
				else if (fault == length + 1)
					r.decoded.main_data = NULL;
				else if (fault == length + 2)
					r.decoded.header.xl_info |= XLR_SPECIAL_REL_UPDATE;
				else if (fault == length + 3)
					r.decoded.header.xl_info |= 0x08;
				else {
					memcpy(&blocked.decoded, &r.decoded, sizeof(r.decoded));
					blocked.decoded.max_block_id = 0;
					blocked.decoded.blocks[0].in_use = true;
					r.reader.record = &blocked.decoded;
				}
				detail = cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, foreign, &out);
				UT_ASSERT(detail != CLUSTER_COLD_OK);
				if (detail == CLUSTER_COLD_OK)
					return;
				UT_ASSERT_EQ(out.effects.required | out.effects.native_apply | out.effects.no_effect
								 | out.effects.refused,
							 0);
				UT_ASSERT(!out.xid_input.complete);
			}
		}
	}
}

UT_TEST(test_slru_carriers_reject_invalid_native_fields)
{
	for (int foreign = 0; foreign <= 1; foreign++) {
		for (uint32 kind = 0; kind < 6; kind++) {
			const int faults[] = { 1, 3, 1, 1, 10, 3 };

			for (int fault = 0; fault < faults[kind]; fault++) {
				EffectRecord r;
				ClusterColdDecodedV1 out = { 0 };
				ClusterColdDetailV1 detail;

				slru_record(&r, kind);
				if (kind == 1) {
					xl_clog_truncate clog;

					memcpy(&clog, r.data.bytes, sizeof(clog));
					if (fault == 0)
						clog.pageno = -1;
					else if (fault == 1)
						clog.oldestXact = FrozenTransactionId;
					else
						clog.oldestXactDb = InvalidOid;
					memcpy(r.data.bytes, &clog, sizeof(clog));
				} else if (kind == 4) {
					xl_multixact_create create;
					MultiXactMember member;

					memcpy(&create, r.data.bytes, SizeOfMultiXactCreate);
					memcpy(&member, r.data.bytes + SizeOfMultiXactCreate, sizeof(member));
					switch (fault) {
					case 0:
						create.nmembers = 0;
						break;
					case 1:
						create.nmembers = -1;
						break;
					case 2:
						create.nmembers = INT_MAX;
						break;
					case 3:
						create.mid = InvalidMultiXactId;
						break;
					case 4:
						create.moff = 0;
						break;
					case 5:
						member.xid = InvalidTransactionId;
						break;
					case 6:
						member.xid = FrozenTransactionId;
						break;
					case 7:
						member.status = -1;
						break;
					case 8:
						member.status = MaxMultiXactStatus + 1;
						break;
					case 9:
						create.nmembers = 2;
						break;
					}
					memcpy(r.data.bytes, &create, SizeOfMultiXactCreate);
					memcpy(r.data.bytes + SizeOfMultiXactCreate, &member, sizeof(member));
				} else if (kind == 5) {
					xl_multixact_truncate truncate;

					memcpy(&truncate, r.data.bytes, SizeOfMultiXactTruncate);
					if (fault == 0)
						truncate.oldestMultiDB = InvalidOid;
					else if (fault == 1)
						truncate.startTruncOff = InvalidMultiXactId;
					else
						truncate.endTruncOff = InvalidMultiXactId;
					memcpy(r.data.bytes, &truncate, SizeOfMultiXactTruncate);
				} else {
					int page = -1;

					memcpy(r.data.bytes, &page, sizeof(page));
				}
				detail = cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, foreign, &out);
				UT_ASSERT_EQ(detail, CLUSTER_COLD_COMPONENT_INVALID);
				if (detail != CLUSTER_COLD_COMPONENT_INVALID)
					return;
				UT_ASSERT_EQ(out.effects.required | out.effects.native_apply | out.effects.no_effect
								 | out.effects.refused,
							 0);
				UT_ASSERT(!out.xid_input.complete);
			}
		}
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

	xl_undo_tt_slot_set_head head
		= { .segment_id = 257,
			.instance = 2,
			.slot_offset = 4,
			.xid = 802,
			.wrap = 7,
			.first_undo_block = { .raw = { UINT64_C(257) | (UINT64_C(9) << 32), 4 } } };

	xl_undo_tt_slot_abort_exact abort = { .segment_id = 257,
										  .instance = 2,
										  .slot_offset = 4,
										  .xid = 802,
										  .wrap = 7,
										  .format_version = CLUSTER_UNDO_TT_ABORT_EXACT_VERSION };
	xl_undo_tt_slot_commit commit = { .segment_id = 257,
									 .instance = 2,
									 .slot_offset = 4,
									 .xid = 802,
									 .wrap = 7,
									 .commit_scn = 23 };

	record_init(r, RM_CLUSTER_UNDO_ID, info);
	if (info == XLOG_UNDO_TT_SLOT_ABORT)
		append(r, &abort, sizeof(abort));
	else if (info == XLOG_UNDO_TT_SLOT_COMMIT)
		append(r, &commit, sizeof(commit));
	else if (info == XLOG_UNDO_TT_SLOT_SET_HEAD)
		append(r, &head, sizeof(head));
	else if (info == XLOG_UNDO_SEGMENT_INIT)
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
	const uint8 infos[]
		= { XLOG_UNDO_SEGMENT_INIT, XLOG_UNDO_SEGMENT_REUSE,	XLOG_UNDO_SEGMENT_RECYCLE,
			XLOG_UNDO_TT_SLOT_BIND, XLOG_UNDO_TT_SLOT_ABORT };
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

UT_TEST(test_standalone_prepared_carriers_remain_refused)
{
	const uint8 infos[] = { XLOG_UNDO_TT_SLOT_COMMIT, XLOG_UNDO_TT_SLOT_SET_HEAD,
		XLOG_UNDO_TT_SLOT_ABORT };

	for (uint32 i = 0; i < lengthof(infos); i++)
		for (int foreign = 0; foreign <= 1; foreign++) {
			EffectRecord r;
			ClusterColdDecodedV1 out = { 0 };
			undo_header_effect_record(&r, infos[i]);
			if (infos[i] == XLOG_UNDO_TT_SLOT_ABORT) {
				xl_undo_tt_slot_abort legacy
					= { .segment_id = 257, .instance = 2, .slot_offset = 4, .xid = 802, .wrap = 7 };
				record_init(&r, RM_CLUSTER_UNDO_ID, infos[i]);
				append(&r, &legacy, sizeof(legacy));
			}
			UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, foreign, &out),
						 CLUSTER_COLD_OK);
			UT_ASSERT((out.record.record_flags & CLUSTER_COLD_RECORD_UNSUPPORTED) != 0);
			UT_ASSERT_EQ(out.effects.required, CLUSTER_COLD_EFFECT_RECORD);
			UT_ASSERT_EQ(out.effects.refused, out.effects.required);
			UT_ASSERT_EQ(out.effects.native_apply | out.effects.no_effect, 0);
			UT_ASSERT(partitioned(&out.effects));
			UT_ASSERT_EQ(cluster_cold_unscheduled_v1(&out.record), CLUSTER_COLD_UNSCHEDULED_REFUSE);
			UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, !foreign));
		}
}

UT_TEST(test_foreign_undo_header_malformed_carriers_remain_refused)
{
	const uint8 infos[]
		= { XLOG_UNDO_SEGMENT_INIT, XLOG_UNDO_SEGMENT_REUSE,	XLOG_UNDO_SEGMENT_RECYCLE,
			XLOG_UNDO_TT_SLOT_BIND, XLOG_UNDO_TT_SLOT_SET_HEAD, XLOG_UNDO_TT_SLOT_ABORT };
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
				UT_ASSERT((out.record.record_flags & CLUSTER_COLD_RECORD_SIDE_UNOWNED) != 0);
				UT_ASSERT_EQ(out.effects.required, CLUSTER_COLD_EFFECT_RECORD);
				UT_ASSERT_EQ(out.effects.refused, out.effects.required);
				UT_ASSERT_EQ(out.effects.native_apply | out.effects.no_effect, 0);
				UT_ASSERT(partitioned(&out.effects));
				UT_ASSERT_EQ(cluster_cold_unscheduled_v1(&out.record), CLUSTER_COLD_UNSCHEDULED_REFUSE);
				UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, false));
			}
		}
}

UT_TEST(test_foreign_abort_requires_exact_version_and_complete_carrier)
{
	for (int fault = 0; fault < 5; fault++) {
		EffectRecord r;
		ClusterColdDecodedV1 out = { 0 };
		xl_undo_tt_slot_abort_exact abort;
		undo_header_effect_record(&r, XLOG_UNDO_TT_SLOT_ABORT);
		memcpy(&abort, r.data.bytes, sizeof(abort));
		if (fault == 0) {
			xl_undo_tt_slot_abort legacy
				= { .segment_id = 257, .instance = 2, .slot_offset = 4, .xid = 802, .wrap = 7 };
			record_init(&r, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_ABORT);
			append(&r, &legacy, sizeof(legacy));
		} else {
			if (fault == 1)
				abort.format_version++;
			if (fault == 2)
				abort.flags = 1;
			if (fault == 3)
				abort.reserved = 1;
			if (fault == 4)
				abort.segment_generation = UINT32_MAX;
			memcpy(r.data.bytes, &abort, sizeof(abort));
		}
		if (cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out)
			== CLUSTER_COLD_OK) {
			UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_SIDE_UNOWNED
							| (fault == 0 ? CLUSTER_COLD_RECORD_UNSUPPORTED : 0));
			UT_ASSERT_EQ(out.effects.required, CLUSTER_COLD_EFFECT_RECORD);
			UT_ASSERT_EQ(out.effects.refused, out.effects.required);
			UT_ASSERT_EQ(out.effects.native_apply | out.effects.no_effect, 0);
			UT_ASSERT(partitioned(&out.effects));
			UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, false));
		}
	}
}


UT_TEST(test_foreign_native_abort_keeps_complete_record_effects)
{
	for (int variant = 0; variant < 8; variant++) {
		EffectRecord r;
		ClusterColdDecodedV1 out = { 0 };
		uint32 xinfo = XACT_XINFO_HAS_SCN;
		if (variant & 1)
			xinfo |= XACT_XINFO_HAS_DROPPED_STATS;
		if (variant & 2)
			xinfo |= XACT_XINFO_HAS_AE_LOCKS;
		if (variant & 4)
			xinfo |= XACT_XINFO_HAS_SUBXACTS;
		completion(&r, false, xinfo);
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out), CLUSTER_COLD_OK);
		UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_FOREIGN_SIDE);
		UT_ASSERT_EQ(out.effects.required & CLUSTER_COLD_EFFECT_CHILDREN,
					 (variant & 4) ? CLUSTER_COLD_EFFECT_CHILDREN : 0);
		UT_ASSERT_EQ(out.effects.native_apply, out.effects.required);
		UT_ASSERT_EQ(out.effects.refused | out.effects.no_effect, 0);
		UT_ASSERT(partitioned(&out.effects));
		UT_ASSERT(cluster_cold_replay_effects_v1(&out, NULL, false));
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out), CLUSTER_COLD_OK);
		UT_ASSERT_EQ(out.record.record_flags, 0);
		UT_ASSERT_EQ(out.effects.native_apply, out.effects.required);
		UT_ASSERT_EQ(out.effects.refused | out.effects.no_effect, 0);
		UT_ASSERT(cluster_cold_replay_effects_v1(&out, NULL, true));
	}
}

UT_TEST(test_foreign_relation_abort_routes_complete_native_record)
{
	for (int variant = 0; variant < 8; variant++) {
		EffectRecord r;
		ClusterColdDecodedV1 out = { 0 };
		uint32 xinfo = XACT_XINFO_HAS_SCN | XACT_XINFO_HAS_RELFILELOCATORS;

		if (variant & 1)
			xinfo |= XACT_XINFO_HAS_DROPPED_STATS;
		if (variant & 2)
			xinfo |= XACT_XINFO_HAS_AE_LOCKS;
		if (variant & 4)
			xinfo |= XACT_XINFO_HAS_SUBXACTS;
		completion(&r, false, xinfo);
		/* The original native producer sets this flag for pending deletes. */
		r.decoded.header.xl_info |= XLR_SPECIAL_REL_UPDATE;
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out),
					 CLUSTER_COLD_OK);
		UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_FOREIGN_SIDE);
		UT_ASSERT_EQ(out.record.space_count, 0);
		UT_ASSERT_EQ(out.effects.native_apply, out.effects.required);
		UT_ASSERT_EQ(out.effects.refused | out.effects.no_effect | out.effects.space_apply, 0);
		UT_ASSERT(partitioned(&out.effects));
		UT_ASSERT(cluster_cold_replay_effects_v1(&out, NULL, false));
		UT_ASSERT_EQ(cluster_cold_unscheduled_v1(&out.record),
					 CLUSTER_COLD_UNSCHEDULED_FOREIGN_SIDE);
		/* Own native redo has no selected cold deletion owner. */
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out),
					 CLUSTER_COLD_OK);
		UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_STRUCTURAL);
		UT_ASSERT_EQ(out.effects.refused, out.effects.required);
		UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, true));
	}
}

UT_TEST(test_foreign_relation_abort_refuses_unowned_frame_and_effects)
{
	for (int fault = 0; fault < 11; fault++) {
		EffectRecord r;
		ClusterColdDecodedV1 out = { 0 };
		ClusterColdDetailV1 detail;
		uint32 xinfo = XACT_XINFO_HAS_SCN | XACT_XINFO_HAS_RELFILELOCATORS;

		if (fault == 6)
			xinfo |= XACT_XINFO_HAS_ORIGIN;
		if (fault == 7)
			xinfo |= XACT_XINFO_HAS_TWOPHASE;
		completion(&r, false, xinfo);
		r.decoded.header.xl_info |= XLR_SPECIAL_REL_UPDATE;
		if (fault == 0)
			r.decoded.header.xl_info &= ~XLR_SPECIAL_REL_UPDATE;
		if (fault == 1)
			r.decoded.header.xl_info |= 0x02;
		if (fault == 2)
			r.decoded.has_page_version_edge = true;
		if (fault == 3)
			r.decoded.record_origin = 1;
		if (fault == 4)
			r.decoded.toplevel_xid = 100;
		if (fault == 5)
			r.reader.cluster_expected_thread_id = 0;
		if (fault == 8)
			r.decoded.main_data_len--;
		if (fault == 9)
			r.decoded.main_data_len++;
		if (fault == 10)
			r.decoded.main_data = NULL;
		detail = cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out);
		if (detail == CLUSTER_COLD_OK) {
			UT_ASSERT_EQ(out.record.record_flags & CLUSTER_COLD_RECORD_FOREIGN_SIDE, 0);
			UT_ASSERT_EQ(out.effects.refused, out.effects.required);
		} else
			UT_ASSERT_EQ(out.effects.required, 0);
	}
}

UT_TEST(test_foreign_native_xact_requires_bound_source_identity)
{
	for (uint16 origin = 1; origin <= 2; origin++)
		for (int commit = 0; commit <= 1; commit++) {
			EffectRecord r;
			ClusterColdDecodedV1 out = { 0 };
			uint32 xinfo = XACT_XINFO_HAS_SCN;

			if (commit)
				xinfo |= XACT_XINFO_HAS_TT_COMMIT;
			completion(&r, commit, xinfo);
			r.reader.cluster_expected_thread_id = origin;
			if (commit) {
				xl_xact_tt_commit tt;
				Size offset = r.decoded.main_data_len - sizeof(tt);

				memcpy(&tt, r.data.bytes + offset, sizeof(tt));
				tt.instance = origin;
				tt.segment_id = origin == 1 ? 1 : 257;
				memcpy(r.data.bytes + offset, &tt, sizeof(tt));
			}
			UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out),
						 CLUSTER_COLD_OK);
			UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_FOREIGN_SIDE);
			UT_ASSERT_EQ(out.effects.native_apply, out.effects.required);
			UT_ASSERT_EQ(out.effects.refused | out.effects.no_effect, 0);
			UT_ASSERT(cluster_cold_replay_effects_v1(&out, NULL, false));
			for (int fault = 0; fault < (commit ? 3 : 2); fault++) {
				r.reader.cluster_expected_thread_id = fault == 0   ? 0
													  : fault == 1 ? 129
																   : 3 - origin;
				if (commit) {
					UT_ASSERT_EQ(
						cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out),
						CLUSTER_COLD_COMPONENT_INVALID);
					UT_ASSERT_EQ(out.effects.required, 0);
					UT_ASSERT_EQ(out.effects.native_apply | out.effects.page_apply
									 | out.effects.space_apply | out.effects.no_effect
									 | out.effects.refused,
								 0);
					continue;
				}
				UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out),
							 CLUSTER_COLD_OK);
				UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_SIDE_UNOWNED);
				UT_ASSERT_EQ(out.effects.refused, out.effects.required);
				UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, false));
			}
		}
}

UT_TEST(test_native_completion_rejects_invalid_stats_kinds_before_routing)
{
	const PgStat_Kind invalid[]
		= { PGSTAT_KIND_INVALID,  (PgStat_Kind)-1,			PGSTAT_KIND_ARCHIVER,
			PGSTAT_KIND_BGWRITER, PGSTAT_KIND_CHECKPOINTER, PGSTAT_KIND_IO,
			PGSTAT_KIND_SLRU,	  PGSTAT_KIND_WAL,			(PgStat_Kind)(PGSTAT_KIND_LAST + 1),
			(PgStat_Kind)INT_MAX };

	for (int commit = 0; commit <= 1; commit++)
		for (int foreign = 0; foreign <= 1; foreign++)
			for (uint32 i = 0; i < lengthof(invalid); i++)
				for (int item = 0; item < 2; item++) {
					EffectRecord r;
					ClusterColdDecodedV1 out = { 0 };
					xl_xact_stats_item stats;
					uint32 xinfo = XACT_XINFO_HAS_SCN | XACT_XINFO_HAS_DROPPED_STATS;
					Size offset = (commit ? MinSizeOfXactCommit : MinSizeOfXactAbort)
								  + sizeof(xl_xact_xinfo) + MinSizeOfXactStatsItems
								  + item * sizeof(stats);

					if (commit)
						xinfo |= XACT_XINFO_HAS_TT_COMMIT;
					completion(&r, commit, xinfo);
					memcpy(&stats, r.data.bytes + offset, sizeof(stats));
					stats.kind = invalid[i];
					memcpy(r.data.bytes + offset, &stats, sizeof(stats));
					UT_ASSERT_EQ(
						cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, foreign, &out),
						CLUSTER_COLD_COMPONENT_INVALID);
					UT_ASSERT_EQ(out.effects.required, 0);
					UT_ASSERT_EQ(out.effects.native_apply | out.effects.page_apply
									 | out.effects.space_apply | out.effects.no_effect
									 | out.effects.refused,
								 0);
				}
}

UT_TEST(test_native_completion_preserves_variable_stats_kinds)
{
	const xl_xact_stats_item items[]
		= { { .kind = PGSTAT_KIND_DATABASE, .dboid = 5, .objoid = InvalidOid },
			{ .kind = PGSTAT_KIND_RELATION, .dboid = 5, .objoid = 123 },
			{ .kind = PGSTAT_KIND_FUNCTION, .dboid = 5, .objoid = 123 },
			{ .kind = PGSTAT_KIND_REPLSLOT, .dboid = InvalidOid, .objoid = 123 },
			{ .kind = PGSTAT_KIND_SUBSCRIPTION, .dboid = InvalidOid, .objoid = 123 } };

	for (int commit = 0; commit <= 1; commit++)
		for (uint32 i = 0; i < lengthof(items); i++) {
			EffectRecord r;
			ClusterColdDecodedV1 out = { 0 };
			uint8 original[sizeof(r.data.bytes)];
			uint32 xinfo = XACT_XINFO_HAS_SCN | XACT_XINFO_HAS_DROPPED_STATS;
			Size offset = (commit ? MinSizeOfXactCommit : MinSizeOfXactAbort)
						  + sizeof(xl_xact_xinfo) + MinSizeOfXactStatsItems;

			if (commit)
				xinfo |= XACT_XINFO_HAS_TT_COMMIT;
			completion(&r, commit, xinfo);
			memcpy(r.data.bytes + offset, &items[i], sizeof(items[i]));
			memcpy(original, r.data.bytes, r.decoded.main_data_len);
			UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out),
						 CLUSTER_COLD_OK);
			UT_ASSERT_EQ(out.record.record_flags, 0);
			UT_ASSERT((out.effects.required & CLUSTER_COLD_EFFECT_STATS) != 0);
			UT_ASSERT_EQ(out.effects.native_apply, out.effects.required);
			UT_ASSERT_EQ(out.effects.refused | out.effects.no_effect, 0);
			UT_ASSERT(partitioned(&out.effects));
			UT_ASSERT_EQ(memcmp(original, r.data.bytes, r.decoded.main_data_len), 0);
		}
}

UT_TEST(test_native_stats_kind_validation_does_not_expand_foreign_owner)
{
	const PgStat_Kind kinds[]
		= { PGSTAT_KIND_DATABASE, PGSTAT_KIND_REPLSLOT, PGSTAT_KIND_SUBSCRIPTION };

	for (int commit = 0; commit <= 1; commit++)
		for (uint32 i = 0; i < lengthof(kinds); i++) {
			EffectRecord r;
			ClusterColdDecodedV1 out = { 0 };
			xl_xact_stats_item stats = { .kind = kinds[i], .dboid = 5, .objoid = 123 };
			uint32 xinfo = XACT_XINFO_HAS_SCN | XACT_XINFO_HAS_DROPPED_STATS;
			Size offset = (commit ? MinSizeOfXactCommit : MinSizeOfXactAbort)
						  + sizeof(xl_xact_xinfo) + MinSizeOfXactStatsItems;

			if (commit)
				xinfo |= XACT_XINFO_HAS_TT_COMMIT;
			completion(&r, commit, xinfo);
			memcpy(r.data.bytes + offset, &stats, sizeof(stats));
			UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out),
						 CLUSTER_COLD_OK);
			UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_SIDE_UNOWNED);
			UT_ASSERT_EQ(out.effects.refused, out.effects.required);
			UT_ASSERT_EQ(out.effects.native_apply | out.effects.no_effect, 0);
			UT_ASSERT(partitioned(&out.effects));
			UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, false));
		}
}

UT_TEST(test_foreign_native_abort_rejects_commit_flags_and_unknown_stats)
{
	EffectRecord r;
	ClusterColdDecodedV1 out = { 0 };
	xl_xact_stats_item invalid[2] = { { 0 } };
	Size stats_offset = MinSizeOfXactAbort + sizeof(xl_xact_xinfo) + MinSizeOfXactStatsItems;

	/* XactLogAbortRecord emits AE_LOCKS but never FORCE_SYNC_COMMIT. */
	completion(&r, false,
			   XACT_XINFO_HAS_SCN | XACT_XINFO_HAS_DROPPED_STATS | XACT_XINFO_HAS_AE_LOCKS
				   | XACT_COMPLETION_FORCE_SYNC_COMMIT);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out),
				 CLUSTER_COLD_COMPONENT_INVALID);
	UT_ASSERT_EQ(out.effects.required, 0);
	completion(&r, false, XACT_XINFO_HAS_SCN | XACT_XINFO_HAS_DROPPED_STATS);
	memcpy(r.data.bytes + stats_offset, invalid, sizeof(invalid));
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out),
				 CLUSTER_COLD_COMPONENT_INVALID);
	UT_ASSERT_EQ(out.effects.required, 0);
	UT_ASSERT_EQ(out.effects.native_apply | out.effects.page_apply | out.effects.space_apply
					 | out.effects.no_effect | out.effects.refused,
				 0);
}

UT_TEST(test_foreign_native_xact_origin_effect_has_no_consumer)
{
	for (int commit = 0; commit <= 1; commit++) {
		EffectRecord r;
		ClusterColdDecodedV1 out = { 0 };
		xl_xact_origin origin = { .origin_lsn = 0x900, .origin_timestamp = 1 };
		uint32 xinfo = XACT_XINFO_HAS_SCN | XACT_XINFO_HAS_ORIGIN;
		Size offset = (commit ? MinSizeOfXactCommit : MinSizeOfXactAbort) + sizeof(xl_xact_xinfo);

		if (commit)
			xinfo |= XACT_XINFO_HAS_TT_COMMIT;
		completion(&r, commit, xinfo);
		memcpy(r.data.bytes + offset, &origin, sizeof(origin));
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out),
					 CLUSTER_COLD_OK);
		UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_SIDE_UNOWNED);
		UT_ASSERT((out.effects.required & CLUSTER_COLD_EFFECT_ORIGIN) != 0);
		UT_ASSERT_EQ(out.effects.refused, out.effects.required);
		UT_ASSERT_EQ(out.effects.native_apply | out.effects.no_effect, 0);
		UT_ASSERT_EQ(cluster_cold_unscheduled_v1(&out.record), CLUSTER_COLD_UNSCHEDULED_REFUSE);
		UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, false));
	}
}

UT_TEST(test_foreign_native_abort_unsupported_effects_are_not_consumers)
{
	const uint32 extra[] = { XACT_XINFO_HAS_RELFILELOCATORS,
		XACT_XINFO_HAS_TWOPHASE, XACT_XINFO_HAS_ORIGIN };
	for (uint32 i = 0; i < lengthof(extra); i++) {
		EffectRecord r;
		ClusterColdDecodedV1 out = { 0 };
		completion(&r, false, XACT_XINFO_HAS_SCN | extra[i]);
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out), CLUSTER_COLD_OK);
		UT_ASSERT_EQ(out.record.record_flags & CLUSTER_COLD_RECORD_FOREIGN_SIDE, 0);
		UT_ASSERT_EQ(out.effects.refused, out.effects.required);
	}
}

UT_TEST(test_foreign_ctrc_release_requires_original_complete_carrier)
{
	for (int fault = 0; fault < 10; fault++) {
		EffectRecord r;
		ClusterColdDecodedV1 out = { 0 };
		xl_undo_tt_slot_ctrc_release_v1 release
			= { .segment_id = 257,
				.xid = 802,
				.cluster_epoch = 11,
				.root_id = 12,
				.root_generation = 13,
				.formation_epoch = 14,
				.admission_record_generation = 15,
				.seal_generation = 16,
				.touched_nodes_low = 2,
				.slot_offset = 4,
				.slot_wrap = 7,
				.owner_instance = 2,
				.terminal_status = TT_SLOT_COMMITTED,
				.format_version = CLUSTER_UNDO_TT_CTRC_RELEASE_VERSION,
				.flags = CLUSTER_UNDO_TT_CTRC_RELEASE_ALL_TOUCHED_ACKED };
		uint8 bytes[CLUSTER_UNDO_TT_CTRC_RELEASE_BYTES];
		ClusterColdDetailV1 detail;

		memset(release.ack_set_digest, 0x5a, sizeof(release.ack_set_digest));
		UT_ASSERT(cluster_undo_tt_ctrc_release_encode(&release, bytes));
		record_init(&r, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_CTRC_RELEASE);
		append(&r, bytes, sizeof(bytes));
		if (fault == 1)
			r.decoded.main_data_len--;
		if (fault == 2)
			r.decoded.main_data_len++;
		if (fault == 3)
			r.data.bytes[offsetof(xl_undo_tt_slot_ctrc_release_v1, format_version)]++;
		if (fault == 4)
			r.data.bytes[offsetof(xl_undo_tt_slot_ctrc_release_v1, flags)] = 0;
		if (fault == 5)
			memset(r.data.bytes + offsetof(xl_undo_tt_slot_ctrc_release_v1, root_id), 0, 4);
		if (fault == 6)
			memset(r.data.bytes + offsetof(xl_undo_tt_slot_ctrc_release_v1, segment_generation),
				   0xff, 4);
		if (fault == 7)
			r.decoded.header.xl_info |= 1;
		if (fault == 8)
			r.decoded.main_data = NULL;
		if (fault == 9)
			r.data.bytes[offsetof(xl_undo_tt_slot_ctrc_release_v1, terminal_status)]
				= TT_SLOT_ACTIVE;
		detail = cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out);
		if (fault == 0) {
			UT_ASSERT_EQ(detail, CLUSTER_COLD_OK);
			UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_FOREIGN_SIDE);
			UT_ASSERT_EQ(out.effects.native_apply, CLUSTER_COLD_EFFECT_RECORD);
			UT_ASSERT_EQ(out.effects.refused | out.effects.no_effect, 0);
			UT_ASSERT(cluster_cold_replay_effects_v1(&out, NULL, false));
			UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out),
						 CLUSTER_COLD_OK);
			UT_ASSERT_EQ(out.record.record_flags, 0);
		} else if (detail == CLUSTER_COLD_OK) {
			UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_SIDE_UNOWNED);
			UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, false));
		}
	}
}

static void
compound_drop_record(EffectRecord *r)
{
	const uint32 flags = XACT_XINFO_HAS_DBINFO | XACT_XINFO_HAS_RELFILELOCATORS
						 | XACT_XINFO_HAS_SPACE_DROP | XACT_XINFO_HAS_SCN | XACT_XINFO_HAS_TT_COMMIT
						 | XACT_XINFO_HAS_DROPPED_STATS | XACT_XINFO_HAS_INVALS
						 | XACT_COMPLETION_FORCE_SYNC_COMMIT;
	uint32 count = 2;
	completion(r, true, flags);
	append(r, &count, sizeof(count));
	for (uint32 i = 0; i < count; i++) {
		ClusterSpaceStructureChange c = { 0 };
		char bytes[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
		c.identity.action = CLUSTER_SPACE_WAL_TOMBSTONE;
		c.identity.expected.key.system_identifier = 99;
		c.identity.expected.key.database_incarnation = 4;
		memcpy(c.identity.expected.key.storage_uuid, uuid, 16);
		c.identity.expected.key.locator = (RelFileLocator){ 1663, 5, 123 + i };
		memset(c.identity.expected.incarnation, 0x31, 16);
		c.identity.expected.sequence = 1;
		c.identity.expected.operation = 7;
		c.identity.expected.state = CLUSTER_SPACE_IDENTITY_LIVE;
		c.identity.result = c.identity.expected;
		c.identity.result.sequence++;
		c.identity.result.operation++;
		c.identity.result.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
		c.identity.before_token = 17;
		c.identity.result_token = 211;
		c.identity.nblocks = InvalidBlockNumber;
		c.reservation.action = CLUSTER_SPACE_RESERVATION_TOMBSTONE;
		c.reservation.before.identity = c.identity.expected;
		c.reservation.result.identity = c.identity.result;
		c.reservation.before.next_block = c.reservation.result.next_block = 10;
		c.reservation.before_token = 99;
		c.reservation.result_token = 211;
		UT_ASSERT(cluster_space_structure_wal_encode(&c, bytes, sizeof(bytes)));
		append(r, bytes, sizeof(bytes));
	}
}

UT_TEST(test_foreign_compound_uses_scheduled_whole_commit_consumer)
{
	for (int special = 0; special <= 1; special++) {
		EffectRecord r;
		ClusterColdDecodedV1 out = { 0 };
		ClusterColdStepV1 step;
		compound_drop_record(&r);
		if (special)
			r.decoded.header.xl_info |= XLR_SPECIAL_REL_UPDATE;
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out),
					 CLUSTER_COLD_OK);
		UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_FOREIGN_SIDE);
		UT_ASSERT_EQ(out.record.space_count, 2);
		UT_ASSERT_EQ(out.effects.refused, 0);
		UT_ASSERT_EQ(out.effects.space_apply, CLUSTER_COLD_EFFECT_SPACE);
		UT_ASSERT_EQ(out.effects.native_apply,
					 CLUSTER_COLD_EFFECT_RECORD | CLUSTER_COLD_EFFECT_TERMINAL
						 | CLUSTER_COLD_EFFECT_RELATIONS | CLUSTER_COLD_EFFECT_TT
						 | CLUSTER_COLD_EFFECT_SCN | CLUSTER_COLD_EFFECT_STATS
						 | CLUSTER_COLD_EFFECT_INVALIDATIONS | CLUSTER_COLD_EFFECT_SYNC);
		step = step_for(&out, CLUSTER_COLD_STEP_SPACE);
		UT_ASSERT(cluster_cold_replay_effects_v1(&out, &step, false));
		UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, false));
		UT_ASSERT(!cluster_cold_replay_effects_v1(&out, &step, true));
		step.space_count--;
		UT_ASSERT(!cluster_cold_replay_effects_v1(&out, &step, false));
		step.space_count++;
		out.space_ops[1].kind = CLUSTER_COLD_SPACE_CREATE;
		UT_ASSERT(!cluster_cold_replay_effects_v1(&out, &step, false));
		cluster_cold_decoded_release_v1(&out);
	}
}

UT_TEST(test_foreign_compound_rejects_unconsumed_frame_and_payload)
{
	for (int fault = 0; fault < 7; fault++) {
		EffectRecord r;
		ClusterColdDecodedV1 out = { 0 };
		ClusterColdDetailV1 detail;
		compound_drop_record(&r);
		if (fault == 0)
			r.decoded.record_origin = 1;
		if (fault == 1)
			r.decoded.toplevel_xid = 101;
		if (fault == 2)
			r.decoded.header.xl_info |= XLR_CHECK_CONSISTENCY;
		if (fault == 3)
			r.decoded.main_data_len--;
		if (fault == 4)
			r.reader.cluster_expected_thread_id = 2;
		if (fault == 5)
			r.decoded.header.xl_xid++;
		if (fault == 6)
			r.decoded.has_page_version_edge = true;
		detail = cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out);
		if (detail == CLUSTER_COLD_OK) {
			ClusterColdStepV1 step = step_for(&out, CLUSTER_COLD_STEP_SPACE);
			UT_ASSERT(out.record.record_flags != CLUSTER_COLD_RECORD_FOREIGN_SIDE);
			UT_ASSERT(!cluster_cold_replay_effects_v1(&out, &step, false));
		}
		cluster_cold_decoded_release_v1(&out);
	}
}

static void
recovery_abort_record(EffectRecord *r)
{
	ClusterRecoveryTTAbortV1 c0 = { 0 };
	record_init(r, RM_CLUSTER_UNDO_ID, XLOG_UNDO_RECOVERY_TT_ABORT);
	r->decoded.header.xl_xid = InvalidTransactionId;
	c0.version = 1;
	c0.length = 152;
	c0.flags = 1;
	c0.target_thread = 2;
	c0.writer_thread = 1;
	c0.target_incarnation = 22;
	c0.writer_incarnation = 11;
	c0.recovery_serial = 3;
	c0.target_epoch = 4;
	c0.target_full_xid = c0.top_full_xid = UINT64_C(0x700400102);
	memset(c0.target_claim_sha256, 0x21, 32);
	c0.exact = (xl_undo_tt_slot_abort_exact){ .segment_id = 257,
											  .segment_generation = 0,
											  .xid = 0x400102,
											  .slot_offset = 5,
											  .wrap = 9,
											  .instance = 2,
											  .format_version = 1 };
	c0.before.xid = c0.exact.xid;
	c0.before.wrap = 9;
	c0.before.status = TT_SLOT_ACTIVE;
	UT_ASSERT(cluster_recovery_tt_abort_encode_v1(&c0, (uint8 *)r->data.bytes, 152));
	r->decoded.main_data_len = 152;
}

UT_TEST(test_foreign_c0_routes_whole_record_to_exact_side_consumer)
{
	EffectRecord r;
	ClusterColdDecodedV1 out = { 0 };
	ClusterColdStepV1 step;
	recovery_abort_record(&r);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 9, uuid, false, true, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_FOREIGN_SIDE);
	UT_ASSERT_EQ(out.effects.required, CLUSTER_COLD_EFFECT_RECORD | CLUSTER_COLD_EFFECT_TT
										   | CLUSTER_COLD_EFFECT_TERMINAL);
	UT_ASSERT_EQ(out.effects.native_apply, out.effects.required);
	UT_ASSERT_EQ(out.effects.refused | out.effects.no_effect | out.effects.page_apply
					 | out.effects.space_apply,
				 0);
	UT_ASSERT_EQ(cluster_cold_unscheduled_v1(&out.record), CLUSTER_COLD_UNSCHEDULED_FOREIGN_SIDE);
	UT_ASSERT(cluster_cold_replay_effects_v1(&out, NULL, false));
	UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, true));
	step = step_for(&out, CLUSTER_COLD_STEP_PAGE);
	UT_ASSERT(!cluster_cold_replay_effects_v1(&out, &step, false));
	step = step_for(&out, CLUSTER_COLD_STEP_SPACE);
	UT_ASSERT(!cluster_cold_replay_effects_v1(&out, &step, false));
	cluster_cold_decoded_release_v1(&out);
}

UT_TEST(test_own_c0_is_never_native_or_history_no_effect)
{
	EffectRecord r;
	ClusterColdDecodedV1 out = { 0 };
	recovery_abort_record(&r);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 9, uuid, false, false, &out),
				 CLUSTER_COLD_OK);
	/* Own C0 still has no native consumer, including below native redo. */
	UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_UNSUPPORTED);
	UT_ASSERT_EQ(out.effects.required, CLUSTER_COLD_EFFECT_RECORD | CLUSTER_COLD_EFFECT_TT
										   | CLUSTER_COLD_EFFECT_TERMINAL);
	UT_ASSERT_EQ(out.effects.refused, out.effects.required);
	UT_ASSERT_EQ(out.effects.native_apply | out.effects.no_effect, 0);
	UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, true));
	UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, false));
	cluster_cold_decoded_release_v1(&out);
}

UT_TEST(test_c0_route_rejects_unowned_envelope_effects)
{
	for (int foreign = 0; foreign < 2; foreign++) {
		for (int fault = 0; fault < 8; fault++) {
			EffectRecord r;
			ClusterColdDecodedV1 out = { 0 };
			recovery_abort_record(&r);
			if (fault == 0)
				r.decoded.header.xl_xid = 23;
			if (fault == 1)
				r.decoded.toplevel_xid = 23;
			if (fault == 2)
				r.decoded.record_origin = 1;
			if (fault == 3)
				r.decoded.header.xl_info |= XLR_SPECIAL_REL_UPDATE;
			if (fault == 4)
				r.reader.cluster_expected_thread_id = 2;
			if (fault == 5) {
				r.decoded.max_block_id = 0;
				r.decoded.blocks[0].in_use = true;
			}
			if (fault == 6)
				r.decoded.has_page_version_edge = true;
			if (fault == 7)
				r.decoded.main_data = NULL;
			UT_ASSERT_NE(cluster_cold_recovery_decode_v1(&r.reader, 9, uuid, false, foreign, &out),
						 CLUSTER_COLD_OK);
			UT_ASSERT_EQ(out.effects.native_apply | out.effects.no_effect | out.effects.page_apply
							 | out.effects.space_apply,
						 0);
			cluster_cold_decoded_release_v1(&out);
		}
	}
}

UT_TEST(test_c0_route_keeps_complete_bounded_payload_validation)
{
	for (int fault = 0; fault < 7; fault++) {
		EffectRecord r;
		ClusterColdDecodedV1 out = { 0 };
		recovery_abort_record(&r);
		if (fault == 0)
			r.decoded.main_data_len = 0;
		if (fault == 1)
			r.decoded.main_data_len = 151;
		if (fault == 2)
			r.decoded.main_data_len = 153;
		if (fault == 3)
			r.data.bytes[0] = 2; /* unknown format */
		if (fault == 4)
			r.data.bytes[4] = 2; /* unsupported top identity flag */
		if (fault == 5)
			r.data.bytes[12] = 1; /* reserved envelope bytes */
		if (fault == 6)
			r.data.bytes[56] ^= 1; /* top FullXid differs from target */
		UT_ASSERT_NE(cluster_cold_recovery_decode_v1(&r.reader, 9, uuid, false, true, &out),
					 CLUSTER_COLD_OK);
		UT_ASSERT_EQ(out.effects.native_apply | out.effects.no_effect, 0);
		cluster_cold_decoded_release_v1(&out);
	}
}

UT_TEST(test_original_startup_dependency_decodes_before_target_owner)
{
	for (int fault = 0; fault < 12; fault++) {
		EffectRecord r;
		ClusterColdTypedV1 typed = { 0 };
		ClusterColdTypedReplay replay = { 0 };
		struct ClusterRecoveryFencePlan *fence = (void *)&dependency_fence;
		ClusterColdReplayRecordV1 record = { 0 };
		uint32 target = 999;
		recovery_abort_record(&r);
		typed.system_identifier = 99;
		typed.participant_count = 3;
		typed.own_participant = 0;
		memcpy(typed.sources[1].claim.identity.storage_uuid, uuid, 16);
		replay.typed = &typed;
		replay.fence_plan = &fence;
		replay.current[1] = &r.reader;
		record.read_rec_ptr = r.reader.ReadRecPtr;
		record.end_rec_ptr = r.reader.EndRecPtr;
		record.record_crc = r.decoded.header.xl_crc;
		record.rmid = r.decoded.header.xl_rmid;
		record.info = r.decoded.header.xl_info;
		dependency_calls = 0;
		dependency_owner_ok = fault != 1;
		switch (fault) {
		case 2:
			record.record_crc ^= 1;
			break;
		case 3:
			record.rmid = RM_XACT_ID;
			break;
		case 4:
			record.read_rec_ptr++;
			break;
		case 5:
			r.decoded.main_data_len--;
			break;
		case 6:
			typed.own_participant = 1;
			break;
		case 7:
			r.reader.cluster_expected_thread_id = 2;
			break;
		case 8:
			replay.current[1] = NULL;
			break;
		case 9:
			replay.fence_plan = NULL;
			break;
		case 10:
			typed.participant_count = 1;
			break;
		case 11:
			record.end_rec_ptr--;
			break;
		}
		UT_ASSERT_EQ(cluster_typed_replay_dependency(&replay, 1, &record, &target), fault == 0);
		UT_ASSERT_EQ(target, fault == 0 ? 2 : CLUSTER_COLD_NO_INDEX);
		UT_ASSERT_EQ(dependency_calls, fault <= 1 ? 1 : 0);
		UT_ASSERT(replay.current[1] == (fault == 8 ? NULL : &r.reader));
		cluster_cold_decoded_release_v1(&replay.decoded);
	}
}

UT_TEST(test_original_startup_dependency_leaves_non_c0_with_existing_dispatch)
{
	EffectRecord r;
	ClusterColdTypedV1 typed = { 0 };
	ClusterColdTypedReplay replay = { 0 };
	struct ClusterRecoveryFencePlan *fence = (void *)&dependency_fence;
	ClusterColdReplayRecordV1 record = { 0 };
	uint32 target = 999;
	record_init(&r, RM_XLOG_ID, XLOG_NOOP);
	typed.participant_count = 2;
	replay.typed = &typed;
	replay.fence_plan = &fence;
	replay.current[1] = &r.reader;
	record.read_rec_ptr = r.reader.ReadRecPtr;
	record.end_rec_ptr = r.reader.EndRecPtr;
	record.record_crc = r.decoded.header.xl_crc;
	record.rmid = RM_XLOG_ID;
	record.info = XLOG_NOOP;
	dependency_calls = 0;
	UT_ASSERT(cluster_typed_replay_dependency(&replay, 1, &record, &target));
	UT_ASSERT_EQ(target, CLUSTER_COLD_NO_INDEX);
	UT_ASSERT_EQ(dependency_calls, 0);
}

UT_TEST(test_foreign_terminals_refuse_unconsumed_frame_before_dispatch)
{
	for (int commit = 0; commit < 2; commit++)
		for (int extras = 0; extras < 2; extras++)
			for (int field = 0; field < 3; field++) {
				EffectRecord r;
				ClusterColdDecodedV1 out = { 0 };
				ClusterColdDetailV1 detail;
				uint32 xinfo = XACT_XINFO_HAS_SCN;

				if (commit)
					xinfo |= XACT_XINFO_HAS_TT_COMMIT;
				if (extras)
					xinfo |= XACT_XINFO_HAS_DROPPED_STATS | XACT_XINFO_HAS_AE_LOCKS
							 | XACT_XINFO_HAS_SUBXACTS;
				completion(&r, commit, xinfo);
				UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out),
							 CLUSTER_COLD_OK);
				UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_FOREIGN_SIDE);
				UT_ASSERT(cluster_cold_replay_effects_v1(&out, NULL, false));
				cluster_cold_decoded_release_v1(&out);
				if (field == 0)
					r.decoded.record_origin = 1;
				else if (field == 1)
					r.decoded.toplevel_xid = 101;
				else
					r.decoded.has_page_version_edge = true;
				detail = cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out);
				if (detail == CLUSTER_COLD_OK) {
					UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_SIDE_UNOWNED);
					UT_ASSERT_EQ(out.effects.refused, out.effects.required);
					UT_ASSERT_EQ(out.effects.native_apply | out.effects.page_apply
									 | out.effects.space_apply | out.effects.no_effect,
								 0);
					UT_ASSERT(partitioned(&out.effects));
					UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, false));
				} else
					UT_ASSERT_EQ(detail, CLUSTER_COLD_OPCODE_UNSUPPORTED);
				cluster_cold_decoded_release_v1(&out);
			}
}

#include "test_cluster_cold_abort_replay.inc"

int
main(void)
{
	UT_PLAN(78);
	UT_RUN(test_relation_abort_capture_replays_and_drains_complete_cut);
	UT_RUN(test_relation_abort_capture_refuses_missing_creation);
	UT_RUN(test_relation_abort_replay_failure_keeps_final_drain_closed);
	UT_RUN(test_foreign_terminals_refuse_unconsumed_frame_before_dispatch);
	UT_RUN(test_smgr_native_carriers_keep_existing_effect_owners);
	UT_RUN(test_smgr_create_rejects_incomplete_or_nonproducer_carriers);
	UT_RUN(test_smgr_truncate_rejects_incomplete_or_nonproducer_carriers);
	UT_RUN(test_original_startup_dependency_decodes_before_target_owner);
	UT_RUN(test_original_startup_dependency_leaves_non_c0_with_existing_dispatch);
	UT_RUN(test_foreign_c0_routes_whole_record_to_exact_side_consumer);
	UT_RUN(test_own_c0_is_never_native_or_history_no_effect);
	UT_RUN(test_c0_route_rejects_unowned_envelope_effects);
	UT_RUN(test_c0_route_keeps_complete_bounded_payload_validation);
	UT_RUN(test_foreign_compound_uses_scheduled_whole_commit_consumer);
	UT_RUN(test_foreign_compound_rejects_unconsumed_frame_and_payload);
	UT_RUN(test_foreign_ctrc_release_requires_original_complete_carrier);
	UT_RUN(test_foreign_native_abort_keeps_complete_record_effects);
	UT_RUN(test_foreign_relation_abort_routes_complete_native_record);
	UT_RUN(test_foreign_relation_abort_refuses_unowned_frame_and_effects);
	UT_RUN(test_foreign_native_xact_requires_bound_source_identity);
	UT_RUN(test_native_completion_rejects_invalid_stats_kinds_before_routing);
	UT_RUN(test_native_completion_preserves_variable_stats_kinds);
	UT_RUN(test_native_stats_kind_validation_does_not_expand_foreign_owner);
	UT_RUN(test_foreign_native_abort_rejects_commit_flags_and_unknown_stats);
	UT_RUN(test_foreign_native_xact_origin_effect_has_no_consumer);
	UT_RUN(test_foreign_native_abort_unsupported_effects_are_not_consumers);
	UT_RUN(test_foreign_abort_requires_exact_version_and_complete_carrier);
	UT_RUN(test_foreign_undo_header_routes_only_connected_record_consumers);
	UT_RUN(test_standalone_prepared_carriers_remain_refused);
	UT_RUN(test_foreign_undo_header_malformed_carriers_remain_refused);
	UT_RUN(test_foreign_undo_blocks_require_real_record_consumer);
	UT_RUN(test_foreign_undo_blocks_reject_malformed_or_unowned_shapes);
	UT_RUN(test_logical_xact_invalidations_keep_physical_no_effect);
	UT_RUN(test_logical_xact_invalidations_do_not_hide_bad_shape);
	UT_RUN(test_foreign_clog_and_multixact_use_native_side_consumer);
	UT_RUN(test_slru_native_carriers_preserve_consistency_and_member_range);
	UT_RUN(test_slru_carriers_reject_incomplete_envelopes);
	UT_RUN(test_slru_carriers_reject_invalid_native_fields);
	UT_RUN(test_foreign_assignment_keeps_native_parent_effect);
	UT_RUN(test_foreign_side_dispatch_rejects_mixed_flags_and_unowned_xact);
	UT_RUN(test_whole_commit_effects_keep_native_owner);
	UT_RUN(test_foreign_compound_cannot_drop_unowned_effects);
	UT_RUN(test_relcache_work_exists_with_zero_messages);
	UT_RUN(test_embedded_twophase_is_not_ordinary_commit);
	UT_RUN(test_commit_ts_is_unsupported_even_for_own_source);
	UT_RUN(test_foreign_invalidation_is_not_no_effect);
	UT_RUN(test_assignment_requires_complete_counted_payload);
	UT_RUN(test_own_commit_rejects_invalid_terminal_children);
	UT_RUN(test_own_abort_rejects_invalid_terminal_children);
	UT_RUN(test_foreign_commit_rejects_invalid_terminal_children);
	UT_RUN(test_foreign_abort_rejects_invalid_terminal_children);
	UT_RUN(test_terminal_children_allow_native_xid_wrap);
	UT_RUN(test_terminal_children_are_not_limited_to_assignment_cache);
	UT_RUN(test_invalidations_require_complete_counted_payload);
	UT_RUN(test_abort_routes_all_native_effects);
	UT_RUN(test_unknown_or_truncated_commit_clears_routes);
	UT_RUN(test_exact_foreign_noop_stays_no_effect);
	UT_RUN(test_xlog_controls_keep_native_and_no_effect_routes);
	UT_RUN(test_xlog_controls_reject_incomplete_envelopes);
	UT_RUN(test_xlog_controls_reject_invalid_native_fields);
	UT_RUN(test_logical_message_keeps_complete_native_no_effect_carrier);
	UT_RUN(test_logical_message_requires_every_native_payload_byte);
	UT_RUN(test_logical_message_rejects_nonproducer_carriers);
	UT_RUN(test_registry_unknown_and_blocks_still_refuse);
	UT_RUN(test_folded_tt_unknown_format_is_not_native_apply);
	UT_RUN(test_own_commit_rejects_invalid_tt_delta_binding);
	UT_RUN(test_foreign_commit_rejects_invalid_tt_delta_binding);
	UT_RUN(test_terminal_tt_delta_preserves_valid_physical_boundaries);
	UT_RUN(test_own_commit_without_folded_tt_keeps_native_route);
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
