/* PREPARE observations preserve both outcomes; none is a terminal decision.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#include "test_cluster_recovery_prepare_fixture.h"

static uint32
native_prepare_fixture(bool full, uint16 gidlen)
{
	char bytes[TEST_PREPARE_MAX];
	uint32 length = test_prepare_payload(bytes, full, gidlen);
	fixture(RM_XACT_ID, XLOG_XACT_PREPARE);
	append(bytes, length);
	return length;
}

UT_TEST(test_prepare_retains_both_outcomes_and_every_owned_resource)
{
	ClusterRecoveryNativeScan *scan = NULL;
	ClusterRecoveryNativeEffect effect;
	ClusterWalSourceRef saved_source;
	char saved[TEST_PREPARE_MAX];
	uint32 length = native_prepare_fixture(true, 13);
	ClusterRecoveryNativeKind kinds[] = { CLUSTER_RECOVERY_NATIVE_PREPARE_SUBXACT,
										  CLUSTER_RECOVERY_NATIVE_PREPARE_COMMIT_RELATION,
										  CLUSTER_RECOVERY_NATIVE_PREPARE_ABORT_RELATION,
										  CLUSTER_RECOVERY_NATIVE_PREPARE_COMMIT_STATS,
										  CLUSTER_RECOVERY_NATIVE_PREPARE_ABORT_STATS,
										  CLUSTER_RECOVERY_NATIVE_PREPARE_INVALIDATION };
	size_t widths[] = { sizeof(TransactionId),		sizeof(RelFileLocator),
						sizeof(RelFileLocator),		sizeof(xl_xact_stats_item),
						sizeof(xl_xact_stats_item), sizeof(SharedInvalidationMessage) };
	size_t at = MAXALIGN(sizeof(xl_xact_prepare)) + MAXALIGN(13);
	/* Independently compare all six segments against the original parser on
	 * this complete valid fixture. The production inspector bounds first. */
	xl_xact_parsed_prepare native;
	const void *arrays[6];
	size_t native_at = at;
	ParsePrepareRecord(XLOG_XACT_PREPARE, (xl_xact_prepare *)payload.bytes, &native);
	UT_ASSERT_EQ(native.twophase_xid, 4195136);
	UT_ASSERT_EQ(native.nsubxacts, 2);
	UT_ASSERT_EQ(native.nrels, 2);
	UT_ASSERT_EQ(native.nabortrels, 2);
	UT_ASSERT_EQ(native.nstats, 2);
	UT_ASSERT_EQ(native.nabortstats, 2);
	UT_ASSERT_EQ(native.nmsgs, 2);
	arrays[0] = native.subxacts;
	arrays[1] = native.xlocators;
	arrays[2] = native.abortlocators;
	arrays[3] = native.stats;
	arrays[4] = native.abortstats;
	arrays[5] = native.msgs;
	for (unsigned i = 0; i < lengthof(arrays); ++i) {
		UT_ASSERT((const char *)arrays[i] == payload.bytes + native_at);
		native_at += MAXALIGN(2 * widths[i]);
	}
	memcpy(saved, payload.bytes, length);
	saved_source = source;
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
	if (scan == NULL)
		return;
	memset(&payload, 0, sizeof(payload));
	memset(&decoded, 0, sizeof(decoded));
	memset(&source, 0, sizeof(source));
	UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect), CLUSTER_RECOVERY_NATIVE_EFFECT);
	UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_PREPARE);
	UT_ASSERT_EQ(effect.length, length);
	UT_ASSERT(memcmp(effect.bytes, saved, length) == 0);
	UT_ASSERT(memcmp(&effect.record->source, &saved_source, sizeof(saved_source)) == 0);
	UT_ASSERT_EQ(effect.record->top_xid, 4195136);
	UT_ASSERT_EQ(effect.record->terminal_scn, 0);
	UT_ASSERT_EQ(effect.record->xinfo, 0);
	UT_ASSERT_EQ(effect.record->database, 5);
	UT_ASSERT(effect.record->relcache_init_file);
	UT_ASSERT_EQ(effect.record->origin.origin_lsn, 0xabc);
	UT_ASSERT_EQ(effect.record->prepared_gid_offset, MAXALIGN(sizeof(xl_xact_prepare)));
	UT_ASSERT_EQ(effect.record->prepared_gid_length, 13);
	UT_ASSERT_EQ(effect.record->prepared_resource_count, TWOPHASE_RM_MAX_ID);
	for (unsigned region = 0; region < lengthof(kinds); ++region) {
		for (unsigned i = 0; i < 2; ++i) {
			UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect),
						 CLUSTER_RECOVERY_NATIVE_EFFECT);
			UT_ASSERT_EQ(effect.kind, kinds[region]);
			UT_ASSERT_EQ(effect.ordinal, i);
			UT_ASSERT_EQ(effect.length, widths[region]);
			UT_ASSERT(effect.bytes == effect.record->payload + at + i * widths[region]);
			UT_ASSERT(memcmp(effect.bytes, saved + at + i * widths[region], effect.length) == 0);
		}
		at += MAXALIGN(2 * widths[region]);
	}
	UT_ASSERT_EQ(effect.record->prepared_resources_offset, at);
	for (unsigned id = 1; id <= TWOPHASE_RM_MAX_ID; ++id) {
		TwoPhaseRecordOnDisk resource;
		memcpy(&resource, saved + at, sizeof(resource));
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect),
					 CLUSTER_RECOVERY_NATIVE_EFFECT);
		UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_PREPARE_RESOURCE);
		UT_ASSERT_EQ(effect.ordinal, id - 1);
		UT_ASSERT_EQ(effect.length, MAXALIGN(sizeof(resource)) + MAXALIGN(resource.len));
		UT_ASSERT(memcmp(effect.bytes, saved + at, effect.length) == 0);
		at += effect.length;
	}
	UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect), CLUSTER_RECOVERY_NATIVE_EFFECT);
	UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_PREPARE_END);
	UT_ASSERT_EQ(effect.length, MAXALIGN(sizeof(TwoPhaseRecordOnDisk)));
	UT_ASSERT_EQ(effect.ordinal, TWOPHASE_RM_MAX_ID);
	UT_ASSERT(memcmp(effect.bytes, saved + at, effect.length) == 0);
	UT_ASSERT_EQ(at + effect.length, length);
	UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect), CLUSTER_RECOVERY_NATIVE_END);
	UT_ASSERT(effect.record == NULL && effect.bytes == NULL);
	cluster_recovery_native_scan_destroy(scan);
}

UT_TEST(test_prepare_empty_and_max_gid_are_pending_not_terminal)
{
	for (unsigned gidlen = 1; gidlen <= GIDSIZE; gidlen += GIDSIZE - 1) {
		ClusterRecoveryNativeScan *scan = NULL;
		ClusterRecoveryNativeEffect effect;
		native_prepare_fixture(false, gidlen);
		UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
		if (scan == NULL)
			continue;
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect),
					 CLUSTER_RECOVERY_NATIVE_EFFECT);
		UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_PREPARE);
		UT_ASSERT_EQ(effect.record->prepared_resource_count, 0);
		UT_ASSERT_EQ(effect.record->prepared_gid_length, gidlen);
		UT_ASSERT_EQ(effect.record->nsubxacts, 0);
		UT_ASSERT_EQ(effect.record->terminal_scn, 0);
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect),
					 CLUSTER_RECOVERY_NATIVE_EFFECT);
		UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_PREPARE_END);
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect), CLUSTER_RECOVERY_NATIVE_END);
		cluster_recovery_native_scan_destroy(scan);
	}
}

UT_TEST(test_prepare_all_truncations_and_header_count_faults_refuse)
{
	uint32 length = native_prepare_fixture(true, 13);
	for (uint32 truncated = 0; truncated < length; ++truncated) {
		ClusterRecoveryNativeScan *scan = NULL;
		xl_xact_prepare header;
		native_prepare_fixture(true, 13);
		memcpy(&header, payload.bytes, sizeof(header));
		header.total_len = truncated + sizeof(pg_crc32c); /* reach internal bounds */
		memcpy(payload.bytes, &header, sizeof(header));
		decoded.main_data_len = truncated;
		UT_ASSERT(begin(&scan) != CLUSTER_RECOVERY_NATIVE_MORE);
		UT_ASSERT(scan == NULL);
	}
	for (unsigned fault = 0; fault < 20; ++fault) {
		ClusterRecoveryNativeScan *scan = NULL;
		xl_xact_prepare header;
		native_prepare_fixture(true, 13);
		memcpy(&header, payload.bytes, sizeof(header));
		switch (fault) {
		case 0:
			header.magic++;
			break;
		case 1:
			header.total_len++;
			break;
		case 2:
			header.xid = InvalidTransactionId;
			break;
		case 3:
			header.xid++;
			break;
		case 4:
			header.database = InvalidOid;
			break;
		case 5:
			header.owner = InvalidOid;
			break;
		case 6:
			header.gidlen = 0;
			break;
		case 7:
			header.gidlen = GIDSIZE + 1;
			break;
		case 8:
			header.nsubxacts = -1;
			break;
		case 9:
			header.ncommitrels = -1;
			break;
		case 10:
			header.nabortrels = -1;
			break;
		case 11:
			header.ncommitstats = -1;
			break;
		case 12:
			header.nabortstats = -1;
			break;
		case 13:
			header.ninvalmsgs = -1;
			break;
		case 14:
			header.nsubxacts = INT_MAX;
			break;
		case 15:
			header.ncommitrels = INT_MAX;
			break;
		case 16:
			header.nabortrels = INT_MAX;
			break;
		case 17:
			header.ncommitstats = INT_MAX;
			break;
		case 18:
			header.nabortstats = INT_MAX;
			break;
		case 19:
			header.ninvalmsgs = INT_MAX;
			break;
		}
		memcpy(payload.bytes, &header, sizeof(header));
		UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_SHAPE);
		UT_ASSERT(scan == NULL);
	}
}

UT_TEST(test_prepare_invalid_gid_boolean_resources_and_sentinel_refuse)
{
	for (unsigned fault = 0; fault < 10; ++fault) {
		ClusterRecoveryNativeScan *scan = NULL;
		xl_xact_prepare header;
		TwoPhaseRecordOnDisk resource;
		uint32 length = native_prepare_fixture(false, 13);
		size_t tail = length - MAXALIGN(sizeof(resource));
		memcpy(&header, payload.bytes, sizeof(header));
		memcpy(&resource, payload.bytes + tail, sizeof(resource));
		switch (fault) {
		case 0:
			payload.bytes[MAXALIGN(sizeof(header)) + 12] = 'x';
			break;
		case 1:
			payload.bytes[MAXALIGN(sizeof(header)) + 3] = '\0';
			break;
		case 2:
			payload.bytes[offsetof(xl_xact_prepare, initfileinval)] = 2;
			break;
		case 3:
			resource.len = 1;
			break;
		case 4:
			resource.info = 1;
			break;
		case 5:
			resource.rmid = TWOPHASE_RM_MAX_ID + 1;
			break;
		case 6:
			resource.rmid = TWOPHASE_RM_LOCK_ID;
			break; /* missing END */
		case 7:
			resource.rmid = TWOPHASE_RM_LOCK_ID;
			resource.len = UINT32_MAX;
			break;
		case 8:
			resource.rmid = TWOPHASE_RM_LOCK_ID;
			resource.len = 1;
			break;
		case 9:
			append(&resource, sizeof(resource));
			header.total_len = decoded.main_data_len + sizeof(pg_crc32c);
			memcpy(payload.bytes, &header, sizeof(header));
			break;
		}
		memcpy(payload.bytes + tail, &resource, sizeof(resource));
		UT_ASSERT_EQ(begin(&scan), fault == 5 ? CLUSTER_RECOVERY_NATIVE_UNSUPPORTED
											  : CLUSTER_RECOVERY_NATIVE_SHAPE);
		UT_ASSERT(scan == NULL);
	}
}

UT_TEST(test_prepare_source_budget_blocks_and_strict_completion_gates)
{
	ClusterRecoveryNativeScan *scan = NULL;
	native_prepare_fixture(true, 13);

	UT_ASSERT_EQ(cluster_recovery_native_scan_begin(&reader, &source, &cut, 1, &scan),
				 CLUSTER_RECOVERY_NATIVE_CAPACITY);
	UT_ASSERT(scan == NULL);
	decoded.max_block_id = 0;
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_UNSUPPORTED);
	decoded.max_block_id = -1;
	decoded.has_page_version_edge = true;
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_UNSUPPORTED);
	decoded.has_page_version_edge = false;
	cut.scan_end_exclusive--;
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_INVALID);
	native_prepare_fixture(true, 13);
	cut.origin_owner_incarnation++;
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_INVALID);
	for (unsigned mode = 0; mode < 3; ++mode) {
		native_prepare_fixture(true, 13);
		decoded.header.xl_info = mode == 0	 ? XLOG_XACT_COMMIT_PREPARED
								 : mode == 1 ? XLOG_XACT_ABORT_PREPARED
											 : XLOG_XACT_PREPARE | XLOG_XACT_HAS_INFO;
		UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_UNSUPPORTED);
		UT_ASSERT(scan == NULL);
	}
}

UT_TEST(test_prepare_full_bytes_rechecked_even_after_drain)
{
	uint32 length = native_prepare_fixture(true, 13);
	for (uint32 changed = 0; changed < length; ++changed) {
		ClusterRecoveryNativeScan *scan = NULL;
		ClusterRecoveryNativeEffect effect;
		native_prepare_fixture(true, 13);
		UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
		if (scan == NULL)
			break;
		while (cluster_recovery_native_scan_next(scan, &effect) == CLUSTER_RECOVERY_NATIVE_EFFECT) {
		}
		UT_ASSERT_EQ(cluster_recovery_native_scan_recheck(scan, &reader, &source, &cut),
					 CLUSTER_RECOVERY_NATIVE_UNCHANGED);
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect), CLUSTER_RECOVERY_NATIVE_END);
		payload.bytes[changed] ^= 1;
		UT_ASSERT_EQ(cluster_recovery_native_scan_recheck(scan, &reader, &source, &cut),
					 CLUSTER_RECOVERY_NATIVE_CHANGED);
		UT_ASSERT_EQ(cluster_recovery_native_scan_primary(scan, &effect),
					 CLUSTER_RECOVERY_NATIVE_CHANGED);
		UT_ASSERT(effect.record == NULL && effect.bytes == NULL);
		payload.bytes[changed] ^= 1;
		UT_ASSERT_EQ(cluster_recovery_native_scan_recheck(scan, &reader, &source, &cut),
					 CLUSTER_RECOVERY_NATIVE_CHANGED);
		cluster_recovery_native_scan_destroy(scan);
	}
}

UT_TEST(test_prepare_repeated_resources_have_no_projection_cap)
{
	ClusterRecoveryNativeScan *scan = NULL;
	ClusterRecoveryNativeEffect effect;
	xl_xact_prepare header;
	TwoPhaseRecordOnDisk resource = { .len = 3, .rmid = TWOPHASE_RM_LOCK_ID, .info = 17 };
	TwoPhaseRecordOnDisk end = { 0 };
	char data[8] = { 1, 2, 3, 4, 5, 6, 7, 8 };
	native_prepare_fixture(false, 2);
	decoded.main_data_len -= MAXALIGN(sizeof(end));
	for (unsigned i = 0; i < 300; ++i) {
		append(&resource, sizeof(resource));
		append(data, sizeof(data));
	}
	append(&end, sizeof(end));
	memcpy(&header, payload.bytes, sizeof(header));
	header.total_len = decoded.main_data_len + sizeof(pg_crc32c);
	memcpy(payload.bytes, &header, sizeof(header));
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
	if (scan == NULL)
		return;
	UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect), CLUSTER_RECOVERY_NATIVE_EFFECT);
	UT_ASSERT_EQ(effect.record->prepared_resource_count, 300);
	for (unsigned i = 0; i < 300; ++i) {
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect),
					 CLUSTER_RECOVERY_NATIVE_EFFECT);
		UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_PREPARE_RESOURCE);
		UT_ASSERT_EQ(effect.ordinal, i);
		UT_ASSERT_EQ(effect.length, 16);
		UT_ASSERT(
			memcmp((const char *)effect.bytes + MAXALIGN(sizeof(resource)), data, sizeof(data))
			== 0);
	}
	UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect), CLUSTER_RECOVERY_NATIVE_EFFECT);
	UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_PREPARE_END);
	UT_ASSERT_EQ(effect.ordinal, 300);
	UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect), CLUSTER_RECOVERY_NATIVE_END);
	cluster_recovery_native_scan_destroy(scan);
}
