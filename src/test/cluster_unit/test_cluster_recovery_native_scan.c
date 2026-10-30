/* Native producer bytes through the real bounded parsers and effect reader.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */
#include "postgres.h"
#include <limits.h>
#include "access/multixact.h"
#include "catalog/storage_xlog.h"
#include "cluster/cluster_recovery_native_scan.h"
#include "cluster/cluster_side_undo.h"
#include "cluster/cluster_uba.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "cluster/storage/cluster_undo_alloc.h"
#include "storage/standbydefs.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s at %s:%d\n", condition, file, line);
	abort();
}

#include "../../backend/cluster/cluster_recovery_native_scan.c"

static XLogReaderState reader;
static DecodedXLogRecord decoded;
static ClusterWalSourceRef source;
static RfContributorStreamCutV1 cut;
static union {
	uint64 align;
	char bytes[2 * BLCKSZ];
} payload;

static void
append(const void *bytes, size_t length)
{
	memcpy(payload.bytes + decoded.main_data_len, bytes, length);
	decoded.main_data_len += length;
	decoded.header.xl_tot_len = SizeOfXLogRecord + decoded.main_data_len;
	decoded.next_lsn = decoded.lsn + MAXALIGN(decoded.header.xl_tot_len);
	reader.EndRecPtr = decoded.next_lsn;
	cut.scan_end_exclusive = decoded.next_lsn;
}

static void
fixture(uint8 rmid, uint8 info)
{
	memset(&reader, 0, sizeof(reader));
	memset(&decoded, 0, sizeof(decoded));
	memset(&source, 0, sizeof(source));
	memset(&payload, 0, sizeof(payload));
	source.claim.identity.system_identifier = 817;
	source.claim.identity.storage_uuid[0] = 5;
	source.claim.identity.authority_uuid[0] = 6;
	source.claim.identity.origin_thread_id = 2;
	source.claim.identity.origin_node_id = 1;
	source.claim.identity.thread_claim_created_at = 103;
	source.claim.identity.origin_owner_incarnation = 71;
	source.claim.identity.root_lineage_seq = 3;
	source.claim.database_incarnation = 91;
	source.claim.max_config_generation = 2;
	source.claim.claim_sha256[0] = 18;
	source.timeline = 7;
	cut = (RfContributorStreamCutV1){ 0 };
	cut.failed_thread = 2;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	cut.timeline_id = 7;
	cut.origin_owner_incarnation = 71;
	cut.scan_begin_inclusive = 0x3000;
	cut.contributor_count = 1;
	reader.system_identifier = 817;
	reader.ReadRecPtr = decoded.lsn = 0x3000;
	reader.record = &decoded;
	decoded.header.xl_rmid = rmid;
	decoded.header.xl_info = info;
	decoded.header.xl_xid = 4195136;
	decoded.header.xl_scn = 876;
	decoded.header.xl_prev = 0x2000;
	decoded.header.xl_crc = 12345;
	decoded.record_origin = 17;
	decoded.toplevel_xid = 4195136;
	decoded.main_data = payload.bytes;
	decoded.max_block_id = -1;
}

static void
terminal(bool commit, uint32 xinfo)
{
	TimestampTz time = 8765;

	fixture(RM_XACT_ID, (commit ? XLOG_XACT_COMMIT : XLOG_XACT_ABORT) | XLOG_XACT_HAS_INFO);
	append(&time, sizeof(time));
	append(&xinfo, sizeof(xinfo));
}

static void
array(const void *bytes, int count, size_t item_size)
{
	append(&count, sizeof(count));
	if (count > 0)
		append(bytes, (size_t)count * item_size);
}

static void
full_commit(void)
{
	xl_xact_dbinfo db = { 5, 1663 };
	TransactionId children[] = { 4195137, 4195138 };
	RelFileLocator rels[] = { { 1663, 5, 16385 }, { 1664, 0, 1260 } };
	xl_xact_stats_item stats[] = { { 1, 5, 77 }, { 2, 0, 88 } };
	SharedInvalidationMessage msgs[2] = { 0 };
	xl_xact_origin origin = { 0x12340, 34567 };
	xl_xact_scn scn = { 5906 };
	xl_xact_tt_commit tt = { 0 };
	char drop[XACT_SPACE_DROP_RECORD_BYTES] = { 0 };
	uint32 drops = 1;

	terminal(true, XACT_XINFO_HAS_DBINFO | XACT_XINFO_HAS_SUBXACTS | XACT_XINFO_HAS_RELFILELOCATORS
					   | XACT_XINFO_HAS_DROPPED_STATS | XACT_XINFO_HAS_INVALS
					   | XACT_XINFO_HAS_ORIGIN | XACT_XINFO_HAS_SCN | XACT_XINFO_HAS_TT_COMMIT
					   | XACT_XINFO_HAS_SPACE_DROP | XACT_XINFO_HAS_AE_LOCKS
					   | XACT_COMPLETION_APPLY_FEEDBACK | XACT_COMPLETION_FORCE_SYNC_COMMIT
					   | XACT_COMPLETION_UPDATE_RELCACHE_FILE);
	msgs[0].id = SHAREDINVALRELCACHE_ID;
	msgs[1].id = SHAREDINVALCATALOG_ID;
	tt.segment_id = 257;
	tt.xid = decoded.header.xl_xid;
	tt.wrap = 1;
	tt.instance = 2;
	tt.format_version = CLUSTER_XACT_TT_COMMIT_VERSION;
	tt.commit_scn = scn.scn;
	drop[0] = 0x71; /* Raw typed bytes; decoding/apply belongs to SPACE. */
	append(&db, sizeof(db));
	array(children, 2, sizeof(children[0]));
	array(rels, 2, sizeof(rels[0]));
	array(stats, 2, sizeof(stats[0]));
	array(msgs, 2, sizeof(msgs[0]));
	append(&origin, sizeof(origin));
	append(&scn, sizeof(scn));
	append(&tt, sizeof(tt));
	append(&drops, sizeof(drops));
	append(drop, sizeof(drop));
}

static ClusterRecoveryNativeResult
begin(ClusterRecoveryNativeScan **scan)
{
	return cluster_recovery_native_scan_begin(&reader, &source, &cut, 65536, scan);
}

UT_TEST(test_commit_keeps_every_effect_and_owned_source)
{
	ClusterRecoveryNativeScan *scan = NULL;
	ClusterRecoveryNativeEffect effect;
	const ClusterRecoveryNativeRecord *record;
	unsigned counts[CLUSTER_RECOVERY_NATIVE_SPACE_DROP + 1] = { 0 };
	char saved[8192];
	uint32 length;

	full_commit();
	length = decoded.main_data_len;
	memcpy(saved, payload.bytes, length);
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
	if (scan == NULL)
		return;
	UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect), CLUSTER_RECOVERY_NATIVE_EFFECT);
	UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_TERMINAL);
	record = effect.record;
	UT_ASSERT_EQ((uintptr_t)record->payload % MAXIMUM_ALIGNOF, 0);
	UT_ASSERT_EQ(record->database, 5);
	UT_ASSERT_EQ(record->tablespace, 1663);
	UT_ASSERT_EQ(record->top_xid, 4195136);
	UT_ASSERT_EQ(record->terminal_scn, 5906);
	UT_ASSERT_EQ(record->origin.origin_lsn, 0x12340);
	UT_ASSERT(record->xinfo & XACT_COMPLETION_FORCE_SYNC_COMMIT);
	UT_ASSERT(record->xinfo & XACT_COMPLETION_UPDATE_RELCACHE_FILE);
	UT_ASSERT(record->xinfo & XACT_COMPLETION_APPLY_FEEDBACK);
	UT_ASSERT(record->xinfo & XACT_XINFO_HAS_AE_LOCKS);
	UT_ASSERT_EQ(record->record_origin, 17);
	UT_ASSERT_EQ(record->source.claim.identity.origin_owner_incarnation, 71);
	UT_ASSERT_EQ(record->read_lsn, 0x3000);
	UT_ASSERT_EQ(record->header.xl_crc, 12345);
	memset(payload.bytes, 0xa5, length);
	memset(&source, 0, sizeof(source));
	memset(&decoded, 0, sizeof(decoded));
	UT_ASSERT(memcmp(record->payload, saved, length) == 0);
	while (cluster_recovery_native_scan_next(scan, &effect) == CLUSTER_RECOVERY_NATIVE_EFFECT) {
		UT_ASSERT(effect.record == record);
		UT_ASSERT(effect.ordinal == counts[effect.kind]);
		counts[effect.kind]++;
		if (effect.kind == CLUSTER_RECOVERY_NATIVE_SUBXACT) {
			TransactionId child;
			memcpy(&child, effect.bytes, sizeof(child));
			UT_ASSERT_EQ(child, 4195137 + effect.ordinal);
		}
		if (effect.kind == CLUSTER_RECOVERY_NATIVE_SPACE_DROP) {
			UT_ASSERT_EQ(effect.length, XACT_SPACE_DROP_RECORD_BYTES);
			UT_ASSERT_EQ(*(const uint8 *)effect.bytes, 0x71);
		}
	}
	UT_ASSERT_EQ(counts[CLUSTER_RECOVERY_NATIVE_SUBXACT], 2);
	UT_ASSERT_EQ(counts[CLUSTER_RECOVERY_NATIVE_RELATION_DROP], 2);
	UT_ASSERT_EQ(counts[CLUSTER_RECOVERY_NATIVE_STATS_DROP], 2);
	UT_ASSERT_EQ(counts[CLUSTER_RECOVERY_NATIVE_INVALIDATION], 2);
	UT_ASSERT_EQ(counts[CLUSTER_RECOVERY_NATIVE_TT_COMMIT], 1);
	UT_ASSERT_EQ(counts[CLUSTER_RECOVERY_NATIVE_SPACE_DROP], 1);
	UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect), CLUSTER_RECOVERY_NATIVE_END);
	UT_ASSERT(effect.record == NULL && effect.bytes == NULL);
	cluster_recovery_native_scan_destroy(scan);
}

UT_TEST(test_abort_keeps_deletions_stats_and_subxact_membership)
{
	ClusterRecoveryNativeScan *scan = NULL;
	ClusterRecoveryNativeEffect effect;
	TransactionId child = 4195139;
	RelFileLocator rel = { 1663, 5, 16385 };
	xl_xact_stats_item stats = { 2, 5, 16385 };
	xl_xact_scn scn = { 5907 };
	const ClusterRecoveryNativeKind kinds[]
		= { CLUSTER_RECOVERY_NATIVE_TERMINAL, CLUSTER_RECOVERY_NATIVE_SUBXACT,
			CLUSTER_RECOVERY_NATIVE_RELATION_DROP, CLUSTER_RECOVERY_NATIVE_STATS_DROP };

	terminal(false, XACT_XINFO_HAS_SUBXACTS | XACT_XINFO_HAS_RELFILELOCATORS
						| XACT_XINFO_HAS_DROPPED_STATS | XACT_XINFO_HAS_SCN);
	array(&child, 1, sizeof(child));
	array(&rel, 1, sizeof(rel));
	array(&stats, 1, sizeof(stats));
	append(&scn, sizeof(scn));
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
	if (scan == NULL)
		return;
	for (unsigned i = 0; i < lengthof(kinds); i++) {
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect),
					 CLUSTER_RECOVERY_NATIVE_EFFECT);
		UT_ASSERT_EQ(effect.kind, kinds[i]);
		UT_ASSERT_EQ(effect.record->terminal_scn, 5907);
		UT_ASSERT_EQ(effect.record->header.xl_info & XLOG_XACT_OPMASK, XLOG_XACT_ABORT);
	}
	UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect), CLUSTER_RECOVERY_NATIVE_END);
	cluster_recovery_native_scan_destroy(scan);
}

UT_TEST(test_assignment_retains_real_top_membership)
{
	ClusterRecoveryNativeScan *scan = NULL;
	ClusterRecoveryNativeEffect effect;
	TransactionId top = 4195140;
	TransactionId children[] = { 4195141, 4195142 };

	fixture(RM_XACT_ID, XLOG_XACT_ASSIGNMENT);
	decoded.header.xl_xid = InvalidTransactionId;
	append(&top, sizeof(top));
	array(children, 2, sizeof(children[0]));
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
	if (scan == NULL)
		return;
	UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect), CLUSTER_RECOVERY_NATIVE_EFFECT);
	UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_ASSIGNMENT);
	UT_ASSERT_EQ(effect.record->top_xid, top);
	for (unsigned i = 0; i < 2; i++) {
		TransactionId child;
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect),
					 CLUSTER_RECOVERY_NATIVE_EFFECT);
		UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_SUBXACT);
		memcpy(&child, effect.bytes, sizeof(child));
		UT_ASSERT_EQ(child, children[i]);
	}
	UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect), CLUSTER_RECOVERY_NATIVE_END);
	cluster_recovery_native_scan_destroy(scan);
}

UT_TEST(test_zero_message_batch_is_an_obligation)
{
	ClusterRecoveryNativeScan *scan = NULL;
	ClusterRecoveryNativeEffect effect;
	xl_invalidations invalidations = { 0 };

	fixture(RM_XACT_ID, XLOG_XACT_INVALIDATIONS);
	invalidations.dbId = 5;
	invalidations.tsId = 1663;
	invalidations.relcacheInitFileInval = true;
	append(&invalidations, MinSizeOfInvalidations);
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
	if (scan == NULL)
		return;
	UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect), CLUSTER_RECOVERY_NATIVE_EFFECT);
	UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_INVALIDATION_BATCH);
	UT_ASSERT(effect.record->relcache_init_file);
	UT_ASSERT_EQ(effect.record->database, 5);
	UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect), CLUSTER_RECOVERY_NATIVE_END);
	cluster_recovery_native_scan_destroy(scan);
}

UT_TEST(test_create_before_space_has_exact_fork)
{
	ClusterRecoveryNativeScan *scan = NULL;
	ClusterRecoveryNativeEffect effect;
	xl_smgr_create create = { { 1664, 0, 1260 }, MAIN_FORKNUM };
	xl_smgr_create actual;

	fixture(RM_SMGR_ID, XLOG_SMGR_CREATE);
	append(&create, sizeof(create));
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
	if (scan == NULL)
		return;
	UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect), CLUSTER_RECOVERY_NATIVE_EFFECT);
	UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_CREATE);
	UT_ASSERT_EQ(effect.length, sizeof(create));
	memcpy(&actual, effect.bytes, sizeof(actual));
	UT_ASSERT(memcmp(&actual, &create, sizeof(create)) == 0);
	UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect), CLUSTER_RECOVERY_NATIVE_END);
	cluster_recovery_native_scan_destroy(scan);
}

UT_TEST(test_all_commit_truncations_and_trailing_bytes_refuse)
{
	ClusterRecoveryNativeScan *scan;
	uint32 length;

	full_commit();
	length = decoded.main_data_len;
	for (uint32 i = 0; i < length; i++) {
		decoded.main_data_len = i;
		scan = (void *)1;
		UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_SHAPE);
		UT_ASSERT(scan == NULL);
	}
	decoded.main_data_len = length + 1;
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_SHAPE);
	UT_ASSERT(scan == NULL);
}

UT_TEST(test_bad_counts_flags_and_abort_only_shapes)
{
	ClusterRecoveryNativeScan *scan = NULL;
	int bad_counts[] = { -1, INT_MAX };

	for (unsigned i = 0; i < lengthof(bad_counts); i++) {
		terminal(true, XACT_XINFO_HAS_SUBXACTS);
		append(&bad_counts[i], sizeof(int));
		UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_SHAPE);
		UT_ASSERT(scan == NULL);
	}
	terminal(true, UINT32_C(1) << 20);
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_SHAPE);
	terminal(false, XACT_XINFO_HAS_INVALS);
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_SHAPE);
	terminal(false, XACT_XINFO_HAS_TT_COMMIT);
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_SHAPE);
}

UT_TEST(test_short_abort_assignment_invalidation_create_refuse)
{
	ClusterRecoveryNativeScan *scan = NULL;
	int count = INT_MAX;
	TransactionId top = 4195136;
	xl_invalidations inval = { 0 };
	xl_smgr_create create = { { 1663, 5, 16385 }, MAIN_FORKNUM };

	terminal(false, XACT_XINFO_HAS_DROPPED_STATS);
	append(&count, sizeof(count));
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_SHAPE);
	fixture(RM_XACT_ID, XLOG_XACT_ASSIGNMENT);
	append(&top, sizeof(top));
	append(&count, sizeof(count));
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_SHAPE);
	fixture(RM_XACT_ID, XLOG_XACT_INVALIDATIONS);
	inval.nmsgs = -1;
	append(&inval, MinSizeOfInvalidations);
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_SHAPE);
	fixture(RM_SMGR_ID, XLOG_SMGR_CREATE);
	append(&create, sizeof(create) - 1);
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_SHAPE);
}

UT_TEST(test_prepared_and_unknown_are_not_empty)
{
	ClusterRecoveryNativeScan *scan = NULL;
	uint8 opcodes[] = { XLOG_XACT_COMMIT_PREPARED, XLOG_XACT_ABORT_PREPARED, 0x70 };

	for (unsigned i = 0; i < lengthof(opcodes); i++) {
		terminal(true, 0);
		decoded.header.xl_info = opcodes[i];
		UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_UNSUPPORTED);
		UT_ASSERT(scan == NULL);
	}
	terminal(true, 0);
	decoded.header.xl_rmid = RM_HEAP_ID;
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_UNSUPPORTED);
}

UT_TEST(test_bad_source_and_record_identity_refuse)
{
	ClusterRecoveryNativeScan *scan = NULL;

	terminal(true, 0);
	source.claim.identity.origin_owner_incarnation = 0;
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_INVALID);
	terminal(true, 0);
	source.timeline = 0;
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_INVALID);
	terminal(true, 0);
	reader.EndRecPtr = reader.ReadRecPtr;
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_INVALID);
	terminal(true, 0);
	decoded.lsn++;
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_INVALID);
	terminal(true, 0);
	reader.system_identifier++;
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_INVALID);
}

UT_TEST(test_capacity_and_block_components_never_drop_work)
{
	ClusterRecoveryNativeScan *scan = NULL;
	ClusterRecoveryNativeEffect effect;

	full_commit();
	UT_ASSERT_EQ(cluster_recovery_native_scan_begin(&reader, &source, &cut, 1, &scan),
				 CLUSTER_RECOVERY_NATIVE_CAPACITY);
	UT_ASSERT(scan == NULL);
	decoded.max_block_id = 0;
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_UNSUPPORTED);
	decoded.max_block_id = -1;
	decoded.has_page_version_edge = true;
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_UNSUPPORTED);
	memset(&effect, 0xa5, sizeof(effect));
	UT_ASSERT_EQ(cluster_recovery_native_scan_next(NULL, &effect), CLUSTER_RECOVERY_NATIVE_INVALID);
	UT_ASSERT(effect.bytes == NULL && effect.record == NULL);
	cluster_recovery_native_scan_destroy(NULL);
}

UT_TEST(test_unknown_message_bytes_are_not_interpreted_or_discarded)
{
	ClusterRecoveryNativeScan *scan = NULL;
	ClusterRecoveryNativeEffect effect;
	SharedInvalidationMessage message;

	memset(&message, 0xa5, sizeof(message));
	terminal(true, XACT_XINFO_HAS_INVALS);
	array(&message, 1, sizeof(message));
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
	if (scan == NULL)
		return;
	UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect), CLUSTER_RECOVERY_NATIVE_EFFECT);
	UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect), CLUSTER_RECOVERY_NATIVE_EFFECT);
	UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_INVALIDATION);
	UT_ASSERT_EQ(effect.length, sizeof(message));
	UT_ASSERT(memcmp(effect.bytes, &message, sizeof(message)) == 0);
	cluster_recovery_native_scan_destroy(scan);
}

UT_TEST(test_invalid_native_boolean_is_not_a_relcache_proof)
{
	ClusterRecoveryNativeScan *scan = NULL;
	xl_invalidations invalidations = { 0 };

	fixture(RM_XACT_ID, XLOG_XACT_INVALIDATIONS);
	append(&invalidations, MinSizeOfInvalidations);
	payload.bytes[offsetof(xl_invalidations, relcacheInitFileInval)] = 2;
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_SHAPE);
	UT_ASSERT(scan == NULL);
	cluster_recovery_native_scan_destroy(scan);
}

UT_TEST(test_record_must_belong_to_the_same_complete_cut)
{
	for (unsigned variant = 0; variant < 8; variant++) {
		ClusterRecoveryNativeScan *scan = NULL;

		full_commit();
		switch (variant) {
		case 0:
			cut.flags = 0;
			break;
		case 1:
			cut.flags |= RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY;
			break;
		case 2:
			cut.failed_thread++;
			break;
		case 3:
			cut.timeline_id++;
			break;
		case 4:
			cut.origin_owner_incarnation = 0;
			break;
		case 5:
			cut.origin_owner_incarnation++;
			break;
		case 6:
			cut.scan_begin_inclusive++;
			break;
		case 7:
			cut.scan_end_exclusive--;
			break;
		}
		UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_INVALID);
		UT_ASSERT(scan == NULL);
		cluster_recovery_native_scan_destroy(scan);
	}
}

UT_TEST(test_second_pass_binds_all_source_and_record_bytes)
{
	for (unsigned variant = 0; variant < 16; variant++) {
		ClusterRecoveryNativeScan *scan = NULL;
		ClusterRecoveryNativeEffect effect;
		ClusterRecoveryNativeResult result;

		full_commit();
		UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
		if (scan == NULL)
			return;
		UT_ASSERT_EQ(cluster_recovery_native_scan_recheck(scan, &reader, &source, &cut),
					 CLUSTER_RECOVERY_NATIVE_UNCHANGED);
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect),
					 CLUSTER_RECOVERY_NATIVE_EFFECT);
		UT_ASSERT_EQ(effect.record->cut.scan_end_exclusive, cut.scan_end_exclusive);
		switch (variant) {
		case 0:
			source.claim.claim_sha256[31]++;
			break;
		case 1:
			source.claim.identity.origin_owner_incarnation++;
			break;
		case 2:
			source.claim.identity.authority_uuid[15]++;
			break;
		case 3:
			source.claim.database_incarnation++;
			break;
		case 4:
			source.claim.identity.root_lineage_seq++;
			break;
		case 5:
			source.claim.max_config_generation++;
			break;
		case 6:
			cut.scan_end_exclusive++;
			break;
		case 7:
			cut.contributor_count++;
			break;
		case 8:
			decoded.header.xl_scn++;
			break;
		case 9:
			decoded.header.xl_crc++;
			break;
		case 10:
			decoded.record_origin++;
			break;
		case 11:
			decoded.toplevel_xid++;
			break;
		case 12:
			decoded.main_data[decoded.main_data_len - 1]++;
			break;
		case 13:
			decoded.max_block_id = 0;
			break;
		case 14:
			decoded.main_data_len--;
			break;
		case 15:
			decoded.header.xl_prev++;
			break;
		}
		result = cluster_recovery_native_scan_recheck(scan, &reader, &source, &cut);
		UT_ASSERT_EQ(result, CLUSTER_RECOVERY_NATIVE_CHANGED);
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect),
					 CLUSTER_RECOVERY_NATIVE_CHANGED);
		UT_ASSERT(effect.record == NULL && effect.bytes == NULL);
		/* Restoring the original bytes cannot resurrect a failed observation. */
		full_commit();
		UT_ASSERT_EQ(cluster_recovery_native_scan_recheck(scan, &reader, &source, &cut),
					 CLUSTER_RECOVERY_NATIVE_CHANGED);
		cluster_recovery_native_scan_destroy(scan);
	}
}

#include "test_cluster_recovery_undo_effects.inc"
#include "test_cluster_recovery_multixact_effects.inc"
#include "test_cluster_recovery_slru_effects.inc"
#include "test_cluster_recovery_relmap_effects.inc"
#include "test_cluster_recovery_prepare_effects.inc"

int
main(void)
{
	UT_PLAN(45);
	UT_RUN(test_commit_keeps_every_effect_and_owned_source);
	UT_RUN(test_abort_keeps_deletions_stats_and_subxact_membership);
	UT_RUN(test_assignment_retains_real_top_membership);
	UT_RUN(test_zero_message_batch_is_an_obligation);
	UT_RUN(test_create_before_space_has_exact_fork);
	UT_RUN(test_all_commit_truncations_and_trailing_bytes_refuse);
	UT_RUN(test_bad_counts_flags_and_abort_only_shapes);
	UT_RUN(test_short_abort_assignment_invalidation_create_refuse);
	UT_RUN(test_prepared_and_unknown_are_not_empty);
	UT_RUN(test_bad_source_and_record_identity_refuse);
	UT_RUN(test_capacity_and_block_components_never_drop_work);
	UT_RUN(test_unknown_message_bytes_are_not_interpreted_or_discarded);
	UT_RUN(test_invalid_native_boolean_is_not_a_relcache_proof);
	UT_RUN(test_record_must_belong_to_the_same_complete_cut);
	UT_RUN(test_second_pass_binds_all_source_and_record_bytes);
	UT_RUN(test_cold_undo_effects_own_all_original_shapes);
	UT_RUN(test_cold_undo_effects_reject_truncated_and_mixed_records);
	UT_RUN(test_cold_undo_effects_require_source_and_bounded_target);
	UT_RUN(test_cold_undo_effects_budget_and_recheck_are_sticky);
	UT_RUN(test_cold_undo_effect_keeps_delta_and_unknown_generation);
	UT_RUN(test_multixact_owns_every_member_and_full_source);
	UT_RUN(test_multixact_zero_and_truncate_preserve_distinct_raw_obligations);
	UT_RUN(test_multixact_bad_counts_truncation_and_member_bytes_refuse);
	UT_RUN(test_multixact_zero_page_bounds_and_truncate_shape_refuse);
	UT_RUN(test_multixact_cut_budget_and_final_recheck_keep_original_gates);
	UT_RUN(test_native_slru_owns_all_four_shapes_and_raw_cutoff);
	UT_RUN(test_native_slru_bad_page_cutoff_shape_and_unknown_refuse);
	UT_RUN(test_native_slru_original_cut_shape_and_allocation_gates);
	UT_RUN(test_native_slru_full_recheck_after_drain_is_sticky);
	UT_RUN(test_native_clog_full_birth_retains_exact_generation);
	UT_RUN(test_native_clog_full_birth_rejects_bad_generation_shapes);
	UT_RUN(test_native_clog_full_birth_recheck_cannot_rebase);
	UT_RUN(test_relmap_owns_empty_full_image_and_every_mapping);
	UT_RUN(test_relmap_rejects_every_truncation_and_bad_image);
	UT_RUN(test_relmap_preserves_original_scope_budget_and_block_gates);
	UT_RUN(test_relmap_recheck_retains_inactive_bytes_and_is_sticky);
	UT_RUN(test_prepare_retains_both_outcomes_and_every_owned_resource);
	UT_RUN(test_prepare_empty_and_max_gid_are_pending_not_terminal);
	UT_RUN(test_prepare_all_truncations_and_header_count_faults_refuse);
	UT_RUN(test_prepare_invalid_gid_boolean_resources_and_sentinel_refuse);
	UT_RUN(test_prepare_source_budget_blocks_and_strict_completion_gates);
	UT_RUN(test_prepare_full_bytes_rechecked_even_after_drain);
	UT_RUN(test_prepare_repeated_resources_have_no_projection_cap);
	UT_RUN(test_native_clog_full_truncate_keeps_exact_cutoff_image);
	UT_RUN(test_native_clog_full_truncate_refuses_raw_alias_and_incomplete_image);
	UT_DONE();
	return ut_failed_count != 0;
}
