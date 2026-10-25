/*-------------------------------------------------------------------------
 *
 * test_cluster_relmap_retained_wal.c
 *    Pending relmap against original ROOT/claim files and native XLogReader.
 *
 * Copyright (c) 2026, pgrac contributors
 * Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Copyright (c) 1994, The Regents of the University of California
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_retained_wal.c
 *
 * NOTES
 *    WALR/CF transport and recovery-role providers remain fixture boundaries.
 *
 *-------------------------------------------------------------------------
 */
int control_fixture_main(int argc, char **argv);
#define main control_fixture_main
#include "test_cluster_control_root.c"
#undef main
#include "cluster/cluster_relmap_owner.h"
#include "cluster/cluster_relmap_recovery.h"
#include "cluster/cluster_relmap_terminal_scan.h"
#include "cluster/cluster_relmap_retention_census.h"
#include "cluster/cluster_undo_recovery.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_relmap_lock.h"
#include "cluster/cluster_relmap_authority.h"
#include "cluster/cluster_sinval.h"
#include "cluster/cluster_conf.h"
#include "test_cluster_relmap_native_clog.h"
#include "test_cluster_relmap_native_storage.h"
#include "test_cluster_relmap_original_scope.h"
#include "catalog/pg_tablespace_d.h"
#include "catalog/storage_xlog.h"
#include "utils/relmapper.h"

/* This retained-WAL fixture grants no physical execution census ownership. */
bool cluster_shared_catalog;
static bool relmap_root_merger, relmap_root_namespace_x;
static bool cold_native_real_clog, cold_native_real_scope;
static bool cold_native_real_storage;
static void cold_original_scope_setup(void);
static void cold_original_scope_done(void);
static void cold_original_protection_setup(void);
static void cold_original_authority_postwrite(void);
static bool cold_original_cohort_bind(RfSideOnlinePlanV1 *plan,
									const ClusterWalSourceRef *selected);
static size_t cold_original_terminal_payload(bool aborted, uint16 thread, uint8 *out);
static TestRelmapNativeClogStats cold_native_clog_last;
bool
cluster_recovery_merge_claim_is_held(void)
{
	return relmap_root_merger;
}
bool
cluster_relmap_authority_x_held(void)
{
	return relmap_root_namespace_x;
}

static ClusterWalSourceRef relmap_test_source[2];
static WalTestRecord relmap_test_record[2];
static unsigned relmap_test_fault;
static bool relmap_test_side_error;
static unsigned relmap_test_side_errors;

ClusterRelmapPendingResult test_relmap_pending_collect_side(
	const ClusterRelmapV2Namespace *expected, const uint8 storage_uuid[16],
	const ClusterRelmapPendingSideSource *sources, size_t source_count,
	ClusterRelmapPendingDescriptor *out, size_t capacity, size_t *count_out);

/* The real collector can ERROR through the ABORT accessor's production
 * palloc0. Inject that boundary; all nonfault calls use the original collector. */
ClusterRelmapPendingResult
test_relmap_pending_collect_side(const ClusterRelmapV2Namespace *expected,
	const uint8 storage_uuid[16], const ClusterRelmapPendingSideSource *sources,
	size_t source_count, ClusterRelmapPendingDescriptor *out, size_t capacity,
	size_t *count_out)
{
	if (relmap_test_side_error) {
		relmap_test_side_error = false;
		relmap_test_side_errors++;
		ereport(ERROR, (errmsg("injected SIDE collector ERROR")));
	}
	return cluster_relmap_pending_collect_side(expected, storage_uuid, sources,
		source_count, out, capacity, count_out);
}

static void
relmap_test_recheck_fault(void)
{
	test_stop_share_hook = NULL;
	if (relmap_test_fault == 1)
		v2_checkpoint_root_race();
	else if (relmap_test_fault == 2)
		inputs_pin_current = false;
	else
		test_throw_root_read = true;
}

static void
relmap_test_crc(RelMapFile *map)
{
	INIT_CRC32C(map->crc);
	COMP_CRC32C(map->crc, map, offsetof(RelMapFile, crc));
	FIN_CRC32C(map->crc);
}

/* Encode one typed TRUNCATE; real retained tests put these bytes in WAL. */
static void
relmap_test_truncate(const ClusterRelmapV2Image *image, uint8 *wal)
{
	ClusterSpaceIdentityKey key = { 0 };
	ClusterSpaceReservation before = { 0 };
	ClusterSpaceStructureChange change = { 0 };

	key.system_identifier = image->identity.system_identifier;
	key.database_incarnation = image->identity.database_incarnation;
	memcpy(key.storage_uuid, v2_storage, 16);
	key.locator.spcOid = image->identity.shared ? GLOBALTABLESPACE_OID : DEFAULTTABLESPACE_OID;
	key.locator.dbOid = image->identity.dbid;
	key.locator.relNumber = 70000;
	before.identity.key = key;
	memset(before.identity.incarnation, 0x17, 16);
	before.identity.sequence = before.identity.operation = 1;
	before.identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	before.next_block = 7;
	change.identity.action = CLUSTER_SPACE_WAL_TRUNCATE;
	change.identity.nblocks = 2;
	change.identity.expected = change.identity.result = before.identity;
	memset(change.identity.result.incarnation, 0x29, 16);
	change.identity.result.sequence++;
	change.identity.result.operation++;
	change.identity.before_token = 123;
	change.identity.result_token = 70;
	change.reservation.action = CLUSTER_SPACE_RESERVATION_RESET;
	change.reservation.before = change.reservation.result = before;
	change.reservation.result.identity = change.identity.result;
	change.reservation.result.next_block = change.reservation.first_block = 2;
	change.reservation.before_token = 80;
	change.reservation.result_token = 70;
	UT_ASSERT(cluster_space_structure_wal_encode(&change, wal, CLUSTER_SPACE_STRUCTURE_WAL_BYTES));
}

/* Append actual short, single-page native records to the checkpoint segment. */
static WalTestRecord
relmap_test_append(const ClusterWalSourceRef *source, XLogRecPtr at,
	XLogRecPtr previous, RmgrId rmid, const void *payload, uint32 length)
{
	uint8 bytes[2048] = { 0 };
	XLogRecord record = { 0 };
	size_t framing = length <= UINT8_MAX ? 2 : 5;
	char path[MAXPGPATH];
	int fd;

	UT_ASSERT(length + SizeOfXLogRecord + framing < sizeof(bytes));
	record.xl_tot_len = SizeOfXLogRecord + framing + length;
	record.xl_prev = previous;
	record.xl_rmid = rmid;
	record.xl_info = rmid == RM_RELMAP_ID ? XLOG_RELMAP_UPDATE :
		rmid == RM_SMGR_ID ? XLOG_SMGR_SPACE_IDENTITY | XLR_SPECIAL_REL_UPDATE :
		rmid == RM_XACT_ID ? XLOG_XACT_ASSIGNMENT : XLOG_NOOP;
	record.xl_xid = 4195136;
	bytes[SizeOfXLogRecord] = length <= UINT8_MAX ? XLR_BLOCK_ID_DATA_SHORT : XLR_BLOCK_ID_DATA_LONG;
	if (length <= UINT8_MAX)
		bytes[SizeOfXLogRecord + 1] = length;
	else
		memcpy(bytes + SizeOfXLogRecord + 1, &length, sizeof(length));
	memcpy(bytes + SizeOfXLogRecord + framing, payload, length);
	INIT_CRC32C(record.xl_crc);
	COMP_CRC32C(record.xl_crc, bytes + SizeOfXLogRecord, framing + length);
	COMP_CRC32C(record.xl_crc, &record, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(record.xl_crc);
	memcpy(bytes, &record, SizeOfXLogRecord);
	v2_checkpoint_wal_path(&source->claim.identity, at, source->timeline, path);
	UT_ASSERT(at % XLOG_BLCKSZ + record.xl_tot_len < XLOG_BLCKSZ);
	fd = open(path, O_WRONLY);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pwrite(fd, bytes, record.xl_tot_len, at % wal_segment_size), record.xl_tot_len);
	UT_ASSERT_EQ(close(fd), 0);
	return (WalTestRecord){ 1, MAXALIGN(at + record.xl_tot_len), at, record.xl_crc };
}

static ClusterWalInputsV1 *
relmap_test_fixture_with_side(ClusterRelmapV2Image *image, unsigned selected, bool shared,
	bool cold, bool open_source, unsigned structural)
{
	uint8 bytes[66048], payload[MinSizeOfRelmapUpdate + sizeof(RelMapFile)];
	ClusterRecoveryAnchorV2 anchors[2];
	ClusterWalInputsV1 *inputs = NULL;
	ControlRootImage root;
	xl_relmap_update update = { 0 };

	inputs_claim_observe = inputs_checkpoint_observe = false;
	inputs_claim_fault = 0;
	inputs_closed_wal_fixture(bytes, anchors);
	UT_ASSERT_EQ(cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &root), 0);
	memset(image, 0, sizeof(*image));
	image->identity.system_identifier = TEST_SYSID;
	image->identity.database_incarnation = root.header.v2.database_incarnation;
	image->identity.shared = shared;
	image->identity.dbid = shared ? InvalidOid : 42;
	image->committed_generation = 7;
	image->pending_generation = 8;
	image->pending_owner.node = anchors[selected].identity.origin_node_id;
	image->pending_owner.admitted_incarnation = anchors[selected].identity.origin_owner_incarnation;
	image->pending_owner.top_full_xid = UINT64CONST(0x100000000) + 4195136;
	image->pending_owner.membership_epoch = 91;
	image->pending_owner.boot_incarnation = 202;
	image->committed.magic = RELMAPPER_FILEMAGIC;
	image->committed.num_mappings = 1;
	image->committed.mappings[0].mapoid = 1259;
	image->committed.mappings[0].mapfilenumber = 40000;
	relmap_test_crc(&image->committed);
	image->pending = image->committed;
	image->pending.mappings[0].mapfilenumber++;
	relmap_test_crc(&image->pending);
	update.dbid = image->identity.dbid;
	update.tsid = shared ? GLOBALTABLESPACE_OID : DEFAULTTABLESPACE_OID;
	update.nbytes = sizeof(RelMapFile);
	memcpy(payload, &update, MinSizeOfRelmapUpdate);
	memcpy(payload + MinSizeOfRelmapUpdate, &image->pending, sizeof(RelMapFile));
	for (unsigned i = 0; i < 2; i++) {
		ClusterControlRootSnapshot *snapshot = &root.records[i == 0 ? 0 : 127];
		ClusterWalSourceRef *source = &relmap_test_source[i];
		WalTestRecord tail;

		memset(source, 0, sizeof(*source));
		source->claim.identity = snapshot->identity;
		source->claim.database_incarnation = root.header.v2.database_incarnation;
		source->claim.max_config_generation = root.header.v2.config_generation;
		memcpy(source->claim.claim_sha256, root.refs[i == 0 ? 0 : 127].claim_sha256, 32);
		source->timeline = snapshot->tail_tli;
		tail = relmap_test_append(source, snapshot->validated_tail_lsn_exclusive,
			snapshot->tail_last_record_lsn, RM_RELMAP_ID, payload, sizeof(payload));
		relmap_test_record[i] = tail;
		if (i == selected)
			image->pending_owner.wal_end = tail.exclusive_end;
		/* A valid suffix after the match is still mandatory input. */
		tail = relmap_test_append(source, tail.exclusive_end, tail.record_start, RM_XLOG_ID, "abc", 3);
		if (structural == 1 && i == selected) {
			uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];

			relmap_test_truncate(image, wal);
			tail = relmap_test_append(source, tail.exclusive_end, tail.record_start,
				RM_SMGR_ID, wal, sizeof(wal));
		}
		if (structural == 2 && i == selected) {
			uint8 wal[MinSizeOfXactAssignment + sizeof(TransactionId)];
			xl_xact_assignment assignment = { 0 };
			TransactionId sub = 4195137;

			assignment.xtop = 4195136;
			assignment.nsubxacts = 1;
			memcpy(wal, &assignment, MinSizeOfXactAssignment);
			memcpy(wal + MinSizeOfXactAssignment, &sub, sizeof(sub));
			tail = relmap_test_append(source, tail.exclusive_end, tail.record_start,
				RM_XACT_ID, wal, sizeof(wal));
		}
		/* An omitted interior structural record still reaches the original
		 * sealed cut. Terminal coverage alone must not imply capture coverage. */
		if (structural && i == selected)
			tail = relmap_test_append(source, tail.exclusive_end, tail.record_start,
				RM_XLOG_ID, "abc", 3);
		snapshot->tail_last_record_lsn = tail.record_start;
		snapshot->tail_last_record_crc32c = tail.record_crc;
		snapshot->validated_tail_lsn_exclusive = tail.exclusive_end;
	}
	if (open_source)
		root.records[127].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0);
	v2_write_roots(root.bytes);
	test_inputs_startup_cf = cold;
	UT_ASSERT_EQ(inputs_begin_for_role(cold, &inputs), 0);
	return inputs;
}

static ClusterWalInputsV1 *
relmap_test_fixture(ClusterRelmapV2Image *image, unsigned selected, bool shared,
	bool cold, bool open_source)
{
	return relmap_test_fixture_with_side(image, selected, shared, cold, open_source, false);
}

UT_TEST(test_retained_relmap_matches_each_selected_namespace)
{
	for (unsigned i = 0; i < 4; i++) {
		ClusterRelmapV2Image image, before;
		ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, i % 2, i < 2, true, false);
		before = image;
		UT_ASSERT(inputs != NULL);
		if (inputs == NULL) return;
		for (unsigned pass = 0; pass < 2; pass++) {
			UT_ASSERT_EQ(cluster_relmap_owner_retained_wal_v2(&image, inputs), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
			UT_ASSERT_EQ(cluster_wal_inputs_revalidate_v1(inputs), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
			UT_ASSERT(inputs_pin_held && inputs_pin_current && test_actual_cf == NoLock);
			UT_ASSERT_EQ(memcmp(&image, &before, sizeof(image)), 0);
		}
		UT_ASSERT_EQ(inputs_peer_begins + inputs_native_begin_calls + inputs_native_flush_calls, 0);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_retained_relmap_requires_exact_pending)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, false, true, false);
		if (fault == 0) image.pending_owner.wal_end++;
		if (fault == 1) image.pending_owner.top_full_xid++;
		if (fault == 2) image.pending_owner.admitted_incarnation++;
		if (fault == 3) image.identity.database_incarnation++;
		if (fault == 4) { image.pending.mappings[0].mapfilenumber++; relmap_test_crc(&image.pending); }
		if (fault == 5) image.identity.dbid++;
		if (fault == 6) { image.identity.shared = true; image.identity.dbid = InvalidOid; }
		if (fault == 7) image.pending_owner.node = 64;
		UT_ASSERT_NE(cluster_relmap_owner_retained_wal_v2(&image, inputs), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(inputs_pin_held);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_retained_relmap_scans_suffix_and_other_source)
{
	for (unsigned selected = 0; selected < 2; selected++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, true, true, false);
		char path[MAXPGPATH];
		uint8 bad = 0x5a;
		int fd;
		v2_checkpoint_wal_path(&relmap_test_source[selected].claim.identity,
			relmap_test_record[selected].exclusive_end, relmap_test_source[selected].timeline, path);
		fd = open(path, O_WRONLY);
		UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(pwrite(fd, &bad, 1, relmap_test_record[selected].exclusive_end % wal_segment_size + SizeOfXLogRecord + 2), 1);
		UT_ASSERT_EQ(close(fd), 0);
		UT_ASSERT_NE(cluster_relmap_owner_retained_wal_v2(&image, inputs), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(inputs_pin_held);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_retained_relmap_waits_for_open_and_rejects_online)
{
	for (unsigned mode = 0; mode < 2; mode++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, true, mode == 0, mode == 0);
		inputs_claim_calls = 0;
		inputs_claim_observe = true;
		UT_ASSERT_EQ(cluster_relmap_owner_retained_wal_v2(&image, inputs), mode == 0 ? CLUSTER_CONTROL_ROOT_RECONFIG_WAIT : CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
		inputs_claim_observe = false;
		UT_ASSERT_EQ(inputs_claim_calls, 0);
		UT_ASSERT(inputs_pin_held);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_retained_relmap_original_recheck_and_error_keep_pin)
{
	for (unsigned fault = 1; fault <= 3; fault++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, true, true, false);
		volatile bool threw = false;

		/* Original cuts check, two pre/post visits, then whole-set recheck.
		 * Inject after the native records in both sources have been read. */
		relmap_test_fault = fault;
		test_stop_share_call = test_cf_lock_calls + 6;
		test_stop_share_hook = relmap_test_recheck_fault;
		PG_TRY();
		{
			UT_ASSERT_NE(cluster_relmap_owner_retained_wal_v2(&image, inputs), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		test_throw_root_read = false;
		UT_ASSERT(test_stop_share_hook == NULL);
		test_stop_share_hook = NULL;
		UT_ASSERT_EQ(threw, fault == 3);
		UT_ASSERT(inputs_pin_held && test_actual_cf == NoLock);
		UT_ASSERT_NE(cluster_wal_inputs_revalidate_v1(inputs), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_retained_relmap_invalid_input_is_read_only)
{
	ClusterRelmapV2Image image;
	ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, true, true, false);
	inputs_claim_calls = 0;
	inputs_claim_observe = true;
	UT_ASSERT_EQ(cluster_relmap_owner_retained_wal_v2(NULL, inputs), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_relmap_owner_retained_wal_v2(&image, NULL), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	image.pending.magic++;
	UT_ASSERT_EQ(cluster_relmap_owner_retained_wal_v2(&image, inputs), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	inputs_claim_observe = false;
	UT_ASSERT_EQ(inputs_claim_calls, 0);
	UT_ASSERT_EQ(cluster_wal_inputs_revalidate_v1(inputs), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	cold_census_done(&inputs);
}

typedef struct RelmapRetainedSideFixture
{
	ClusterRelmapPendingSideSource sources[2];
	RfSideOnlinePlanV1 *plans[2];
} RelmapRetainedSideFixture;

typedef struct RelmapRetainedSideFeed
{
	ClusterRelmapPendingSideSource *input;
	uint32 participant;
	unsigned fault;
	const ClusterRelmapV2Image *image;
} RelmapRetainedSideFeed;

/* Capture native checkpoint/NOOP/XACT and typed TRUNCATE with the original reader.
 * This seals the source/cut fixture, not a complete recovery capture: RELMAP
 * remains with its native owner. The API under test cannot certify omitted
 * operations or independently qualify these plans for recovery execution. */
static bool
relmap_retained_side_feed(XLogReaderState *reader, void *arg)
{
	const RelmapRetainedSideFeed *feed = arg;
	ClusterRelmapPendingSideSource *input = feed->input;
	RfDetachedRecordPlanV1 record = { 0 };
	RfPageOnlineRecordIdentityV1 identity = { 0 };
	RfPageProofDetailV1 result;
	DecodedXLogRecord *original = reader->record;
	DecodedXLogRecord changed;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	bool structural = XLogRecGetRmid(reader) == RM_SMGR_ID || XLogRecGetRmid(reader) == RM_XACT_ID;

	if (!((XLogRecGetRmid(reader) == RM_XLOG_ID &&
		((XLogRecGetInfo(reader) & ~XLR_INFO_MASK) == XLOG_NOOP ||
		 (XLogRecGetInfo(reader) & ~XLR_INFO_MASK) == XLOG_CHECKPOINT_SHUTDOWN ||
		 (XLogRecGetInfo(reader) & ~XLR_INFO_MASK) == XLOG_CHECKPOINT_ONLINE)) ||
		(XLogRecGetRmid(reader) == RM_SMGR_ID &&
		(XLogRecGetInfo(reader) & ~XLR_INFO_MASK) == XLOG_SMGR_SPACE_IDENTITY) ||
		XLogRecGetRmid(reader) == RM_XACT_ID))
		return true;
	if (reader->ReadRecPtr < input->cut.scan_begin_inclusive)
		return true;
	/* Controlled capture faults, never writes to the held native WAL. Each
	 * altered input must still pass the original decoder and seal. In fault 6
	 * keep the original CRC to require an exact owned-payload comparison too. */
	if (structural && feed->fault == 5)
		return true;
	if ((structural && feed->fault >= 6 && feed->fault <= 10) ||
		((feed->fault == 11 || feed->fault == 12) &&
		 input->source.claim.identity.origin_node_id == feed->image->pending_owner.node &&
		 (feed->fault == 11 ? reader->ReadRecPtr == feed->image->pending_owner.wal_end :
			reader->EndRecPtr == input->cut.scan_end_exclusive) &&
		 XLogRecGetRmid(reader) == RM_XLOG_ID &&
		 (XLogRecGetInfo(reader) & ~XLR_INFO_MASK) == XLOG_NOOP)) {
		changed = *original;
		reader->record = &changed;
		if (feed->fault == 6 && XLogRecGetRmid(reader) == RM_XACT_ID) {
			TransactionId sub = 4195138;

			memcpy(wal, XLogRecGetData(reader), XLogRecGetDataLen(reader));
			memcpy(wal + MinSizeOfXactAssignment, &sub, sizeof(sub));
			changed.main_data = (char *)wal;
		} else if (feed->fault == 6 || feed->fault == 11 || feed->fault == 12) {
			ClusterSpaceStructureChange change;

			relmap_test_truncate(feed->image, wal);
			UT_ASSERT(cluster_space_structure_wal_decode(wal, sizeof(wal), &change));
			change.identity.expected.key.locator.relNumber++;
			change.identity.result.key = change.identity.expected.key;
			change.reservation.before.identity.key = change.identity.expected.key;
			change.reservation.result.identity.key = change.identity.expected.key;
			UT_ASSERT(cluster_space_structure_wal_encode(&change, wal, sizeof(wal)));
			changed.main_data = (char *)wal;
			changed.main_data_len = sizeof(wal);
			changed.header.xl_rmid = RM_SMGR_ID;
			changed.header.xl_info = XLOG_SMGR_SPACE_IDENTITY | XLR_SPECIAL_REL_UPDATE;
		}
		if (feed->fault == 7) changed.header.xl_crc ^= 0x5a;
		if (feed->fault == 8) changed.header.xl_xid++;
		if (feed->fault == 9) changed.toplevel_xid = 4195137;
		if (feed->fault == 10) changed.header.xl_info ^= XLR_SPECIAL_REL_UPDATE;
	}
	record.source_record = reader;
	record.preflight_complete = true;
	UT_ASSERT_EQ(rf_opcode_route_lookup_v1(XLogRecGetRmid(reader), XLogRecGetInfo(reader),
		false, true, &record.route), RF_OPCODE_ROUTE_OK);
	identity.record.system_identifier = reader->system_identifier;
	memcpy(identity.record.storage_uuid, input->source.claim.identity.storage_uuid, 16);
	identity.record.origin_thread = input->cut.failed_thread;
	identity.record.timeline_id = input->cut.timeline_id;
	identity.record.read_rec_ptr = reader->ReadRecPtr;
	identity.record.end_rec_ptr = reader->EndRecPtr;
	identity.record.record_crc = reader->record->header.xl_crc;
	identity.record.rmid = XLogRecGetRmid(reader);
	identity.record.info = XLogRecGetInfo(reader);
	identity.participant_index = feed->participant;
	result = rf_side_online_plan_feed_record_v1((RfSideOnlinePlanV1 *)input->plan,
		&record, &identity);
	reader->record = original;
	UT_ASSERT_EQ(result, RF_PAGE_PROOF_DETAIL_OK);
	return result == RF_PAGE_PROOF_DETAIL_OK;
}

/* fault mutates an internally consistent sealed input, so refusal must come
 * from comparison with the held original source, not only self-validation. */
static void
relmap_retained_side(RelmapRetainedSideFixture *f, ClusterWalInputsV1 *inputs,
	const ClusterRelmapV2Image *image, bool split, unsigned fault, bool seal)
{
	RfContributorStreamCutV1 cuts[2];
	ClusterWalSourceRef sources[2];
	XLogRecPtr redo[2];

	memset(f, 0, sizeof(*f));
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(inputs), 2);
	for (unsigned i = 0; i < 2; i++) {
		const ClusterWalInputV1 *item = cluster_wal_inputs_at_v1(inputs, i);
		ClusterRelmapPendingSideSource *input = &f->sources[i];

		UT_ASSERT(item != NULL && item->kind == CLUSTER_WAL_INPUT_CHECKPOINT);
		if (item == NULL) return;
		input->source = item->source;
		input->cut.failed_thread = item->source.claim.identity.origin_thread_id;
		input->cut.origin_owner_incarnation = item->source.claim.identity.origin_owner_incarnation;
		input->cut.timeline_id = item->source.timeline;
		input->cut.scan_begin_inclusive = item->checkpoint.checkpoint_lower_lsn;
		input->cut.scan_end_exclusive = item->checkpoint.validated_tail_lsn_exclusive;
		input->cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		if (i == 1) {
			if (fault == 1) input->cut.scan_begin_inclusive += MAXIMUM_ALIGNOF;
			if (fault == 2) input->source.claim.claim_sha256[0] ^= 0x5a;
			if (fault == 3) input->source.claim.max_config_generation++;
			if (fault == 4) {
				input->cut.scan_end_exclusive = input->cut.scan_begin_inclusive;
				input->cut.flags |= RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY;
			}
		}
		redo[i] = input->cut.scan_end_exclusive;
		cuts[i] = input->cut;
		sources[i] = input->source;
	}
	for (unsigned p = 0; p < (split ? 2 : 1); p++) {
		RfSideOnlinePlanRequestV1 request = { 0 };
		unsigned first = split ? p : 0, count = split ? 1 : 2;

		request.system_identifier = image->identity.system_identifier;
		memcpy(request.storage_uuid, sources[0].claim.identity.storage_uuid, 16);
		request.physical_cuts = &cuts[first];
		request.redo_starts = &redo[first];
		request.participant_count = count;
		request.purpose = RF_SIDE_PLAN_RECOVERY;
		UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &f->plans[p]), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(rf_side_online_plan_bind_sources_v1(f->plans[p], &sources[first], count));
		for (unsigned j = 0; j < count; j++) {
			ClusterRelmapPendingSideSource *input = &f->sources[first + j];
			ClusterWalTailObservation observation;
			RelmapRetainedSideFeed feed = { input, j, fault, image };

			input->plan = f->plans[p];
			if (!(input->cut.flags & RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY))
				UT_ASSERT_EQ(cluster_wal_inputs_visit_retained_v1(inputs, first + j,
					relmap_retained_side_feed, &feed, &observation), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		}
		if (seal) {
			UT_ASSERT(rf_side_online_plan_bind_database_v1(f->plans[p], image->identity.database_incarnation));
			UT_ASSERT_EQ(rf_side_online_plan_seal_v1(f->plans[p]), RF_PAGE_PROOF_DETAIL_OK);
		}
	}
}

static void
relmap_retained_side_destroy(RelmapRetainedSideFixture *f)
{
	for (unsigned i = 0; i < 2; i++)
		rf_side_online_plan_destroy_v1(&f->plans[i]);
}

UT_TEST(test_retained_side_exact_complete_and_permuted)
{
	for (unsigned mode = 0; mode < 4; mode++) {
		ClusterRelmapV2Image image, before;
		ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, mode % 2, mode < 2, true, false);
		RelmapRetainedSideFixture f;
		ClusterRelmapPendingSideSource supplied[2], saved[2];

		relmap_retained_side(&f, inputs, &image, mode % 2 != 0, 0, true);
		before = image;
		for (unsigned pass = 0; pass < 2; pass++) {
			supplied[0] = f.sources[pass];
			supplied[1] = f.sources[1 - pass];
			/* Graph counts must not be mistaken for physical source identity. */
			supplied[0].cut.contributor_count = 987;
			memcpy(saved, supplied, sizeof(saved));
			UT_ASSERT_EQ(cluster_relmap_owner_retained_side_wal_v2(&image, inputs,
				supplied, 2), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
			UT_ASSERT_EQ(memcmp(&image, &before, sizeof(image)), 0);
			UT_ASSERT_EQ(memcmp(supplied, saved, sizeof(saved)), 0);
			UT_ASSERT_EQ(cluster_wal_inputs_revalidate_v1(inputs), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
			UT_ASSERT(inputs_pin_held && inputs_pin_current && test_actual_cf == NoLock);
		}
		UT_ASSERT_EQ(inputs_peer_begins + inputs_native_begin_calls + inputs_native_flush_calls, 0);
		relmap_retained_side_destroy(&f);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_retained_side_clean_pair_still_checks_structure)
{
	for (unsigned mode = 0; mode < 4; mode++) {
		ClusterRelmapV2Image image, before;
		ClusterWalInputsV1 *inputs = relmap_test_fixture_with_side(&image,
			mode % 2, mode < 2, true, false, mode % 2 + 1);
		RelmapRetainedSideFixture side;

		relmap_retained_side(&side, inputs, &image, mode % 2 != 0, 0, true);
		image.pending_generation = 0;
		memset(&image.pending_owner, 0, sizeof(image.pending_owner));
		memset(&image.pending, 0, sizeof(image.pending));
		before = image;
		/* No pending assertion exists, but the complete structural input does. */
		UT_ASSERT_EQ(cluster_relmap_owner_retained_wal_v2(&image, inputs),
			CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
		UT_ASSERT_EQ(cluster_relmap_owner_retained_side_wal_v2(&image, inputs,
			side.sources, 2), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(memcmp(&image, &before, sizeof(image)), 0);
		UT_ASSERT_EQ(cluster_wal_inputs_revalidate_v1(inputs), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(inputs_pin_held && inputs_pin_current && test_actual_cf == NoLock);
		relmap_retained_side_destroy(&side);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_retained_side_rejects_incomplete_or_invalid_vectors)
{
	for (unsigned fault = 0; fault < 11; fault++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, true, true, false);
		RelmapRetainedSideFixture f;
		ClusterRelmapPendingSideSource supplied[3];
		const ClusterRelmapPendingSideSource *argument = supplied;
		size_t count = 2;

		relmap_retained_side(&f, inputs, &image, fault != 0, 0, fault != 7);
		memcpy(supplied, f.sources, sizeof(f.sources));
		if (fault <= 1) count = 1; /* missing shared-plan participant / selected source */
		if (fault == 2) supplied[1] = supplied[0];
		if (fault == 3) { supplied[2] = supplied[0]; count = 3; }
		if (fault == 4) supplied[1].cut.scan_end_exclusive++;
		if (fault == 5) supplied[1].source.claim.claim_sha256[0] ^= 0x37;
		if (fault == 6) supplied[1].plan = NULL;
		if (fault == 8) argument = NULL;
		if (fault == 9) count = 0;
		if (fault == 10) count = CLUSTER_WAL_INPUTS_MAX + 1;
		UT_ASSERT_NE(cluster_relmap_owner_retained_side_wal_v2(&image, inputs,
			argument, count), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(inputs_pin_held && test_actual_cf == NoLock);
		UT_ASSERT_EQ(cluster_wal_inputs_revalidate_v1(inputs), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		relmap_retained_side_destroy(&f);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_retained_side_checks_actual_cut_claim_and_empty_source)
{
	for (unsigned fault = 1; fault <= 4; fault++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, false, true, false);
		RelmapRetainedSideFixture f;

		relmap_retained_side(&f, inputs, &image, true, fault, true);
		for (unsigned i = 0; i < 2; i++) {
			UT_ASSERT(rf_side_online_plan_recovery_source_matches_v1(f.sources[i].plan,
				image.identity.system_identifier, v2_storage, &f.sources[i].cut));
			UT_ASSERT(rf_side_online_plan_recovery_claim_matches_v1(f.sources[i].plan,
				&f.sources[i].source));
		}
		UT_ASSERT_NE(cluster_relmap_owner_retained_side_wal_v2(&image, inputs,
			f.sources, 2), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(inputs_pin_held && test_actual_cf == NoLock);
		relmap_retained_side_destroy(&f);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_retained_side_preserves_native_scan_and_final_rechecks)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, true, fault != 3, fault == 2);
		RelmapRetainedSideFixture f;

		/* OPEN cannot be visited: build the otherwise valid sealed input from
		 * its native tail fixture first, then restore the held OPEN scope. */
		if (fault == 2) {
			cold_census_done(&inputs);
			inputs = relmap_test_fixture(&image, 0, true, true, false);
		}
		relmap_retained_side(&f, inputs, &image, false, 0, true);
		if (fault == 2) {
			cold_census_done(&inputs);
			inputs = relmap_test_fixture(&image, 0, true, true, true);
		}
		if (fault <= 1) {
			char path[MAXPGPATH];
			uint8 bad = 0x5a;
			int fd;

			v2_checkpoint_wal_path(&relmap_test_source[fault].claim.identity,
				relmap_test_record[fault].exclusive_end, relmap_test_source[fault].timeline, path);
			fd = open(path, O_WRONLY);
			UT_ASSERT(fd >= 0);
			UT_ASSERT_EQ(pwrite(fd, &bad, 1, relmap_test_record[fault].exclusive_end % wal_segment_size + SizeOfXLogRecord + 2), 1);
			UT_ASSERT_EQ(close(fd), 0);
		}
		if (fault >= 4) {
			relmap_test_fault = fault - 3;
			test_stop_share_call = test_cf_lock_calls + 6;
			test_stop_share_hook = relmap_test_recheck_fault;
		}
		UT_ASSERT_NE(cluster_relmap_owner_retained_side_wal_v2(&image, inputs,
			f.sources, 2), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		if (fault >= 4) UT_ASSERT(test_stop_share_hook == NULL);
		test_stop_share_hook = NULL;
		UT_ASSERT(inputs_pin_held && test_actual_cf == NoLock);
		relmap_retained_side_destroy(&f);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_retained_side_requires_original_structural_operations)
{
	for (unsigned kind = 1; kind <= 2; kind++) {
		for (unsigned fault = 0; fault <= 12; fault++) {
			ClusterRelmapV2Image image, before;
			ClusterWalInputsV1 *inputs;
			RelmapRetainedSideFixture f;

			if ((fault > 0 && fault < 5) || (kind == 2 && fault == 10)) continue;
			inputs = relmap_test_fixture_with_side(&image, fault % 2,
				fault % 4 < 2, true, false, kind);
			relmap_retained_side(&f, inputs, &image, fault % 2 != 0, fault, true);
			before = image;
			/* All mutations passed the original capture and seal. The held WAL
			 * remains valid; only its structural SIDE correspondence differs. */
			UT_ASSERT_EQ(cluster_relmap_owner_retained_wal_v2(&image, inputs), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
			if (fault == 0)
				UT_ASSERT_EQ(cluster_relmap_owner_retained_side_wal_v2(&image, inputs,
					f.sources, 2), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
			else
				UT_ASSERT_NE(cluster_relmap_owner_retained_side_wal_v2(&image, inputs,
					f.sources, 2), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
			UT_ASSERT_EQ(memcmp(&image, &before, sizeof(image)), 0);
			if (fault != 0)
				UT_ASSERT_NE(cluster_wal_inputs_revalidate_v1(inputs), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
			UT_ASSERT(inputs_pin_held && inputs_pin_current && test_actual_cf == NoLock);
			relmap_retained_side_destroy(&f);
			cold_census_done(&inputs);
		}
	}
}

typedef struct RelmapCensusWalFixture
{
	char path[MAXPGPATH];
	int fd;
	ClusterRecoveryRootSet *roots;
	ClusterRelmapRetentionCensus *census;
	ClusterRelmapRetentionDirectory origin[2];
	ClusterRelmapRetentionDirectory *directories[2];
} RelmapCensusWalFixture;

static void
relmap_census_file(int fd, const char *path, const ClusterRelmapV2Image *image)
{
	uint8 wire[CLUSTER_RELMAP_V2_FILE_SIZE];
	int file;

	UT_ASSERT_EQ(cluster_relmap_v2_encode(image, wire, sizeof(wire)), CLUSTER_RELMAP_V2_VALID);
	file = openat(fd, path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	UT_ASSERT(file >= 0);
	UT_ASSERT_EQ(write(file, wire, sizeof(wire)), sizeof(wire));
	UT_ASSERT_EQ(close(file), 0);
}

/* Actual physical namespace and authority bytes, distinct from WAL ROOT files.
 * Caller qualification of floor/issued and isolation remains a fixture input. */
static void
relmap_census_begin(RelmapCensusWalFixture *f, const ClusterRelmapV2Image *pending,
	bool published)
{
	ClusterRelmapV2Image clean = *pending, primary = *pending;
	ClusterRecoveryRootsResult result = CLUSTER_RECOVERY_ROOTS_MORE;

	memset(f, 0, sizeof(*f));
	strlcpy(f->path, "/tmp/pgrac-relmap-census-wal-XXXXXX", sizeof(f->path));
	UT_ASSERT(mkdtemp(f->path) != NULL);
	f->fd = open(f->path, O_RDONLY | O_DIRECTORY);
	UT_ASSERT(f->fd >= 0);
	UT_ASSERT_EQ(mkdirat(f->fd, "global", 0700), 0);
	UT_ASSERT_EQ(mkdirat(f->fd, "base", 0700), 0);
	UT_ASSERT_EQ(mkdirat(f->fd, "base/42", 0700), 0);
	clean.identity.dbid = pending->identity.shared ? 42 : InvalidOid;
	clean.identity.shared = !pending->identity.shared;
	clean.pending_generation = 0;
	memset(&clean.pending_owner, 0, sizeof(clean.pending_owner));
	memset(&clean.pending, 0, sizeof(clean.pending));
	if (published) {
		primary.committed_generation = pending->pending_generation;
		primary.committed = pending->pending;
		primary.pending_generation = 0;
		memset(&primary.pending_owner, 0, sizeof(primary.pending_owner));
		memset(&primary.pending, 0, sizeof(primary.pending));
	}
	relmap_census_file(f->fd, "global/pgrac_relmap_authority",
		pending->identity.shared ? &primary : &clean);
	relmap_census_file(f->fd, "global/pgrac_relmap_authority.bak",
		pending->identity.shared ? pending : &clean);
	relmap_census_file(f->fd, "base/42/pgrac_relmap_authority",
		pending->identity.shared ? &clean : &primary);
	relmap_census_file(f->fd, "base/42/pgrac_relmap_authority.bak",
		pending->identity.shared ? &clean : pending);
	for (unsigned i = 0; i < 2; i++) {
		UT_ASSERT_EQ(cluster_relmap_retention_build_start(&f->origin[i], TEST_SYSID,
			pending->identity.database_incarnation,
			i == 0 ? 0 : (pending->pending_owner.node == 0 ? 127 : pending->pending_owner.node), 1,
			UINT64CONST(0x100000003), UINT64CONST(0x100800000)), CLUSTER_RELMAP_RETENTION_OK);
		f->directories[1 - i] = &f->origin[i];
	}
	UT_ASSERT_EQ(cluster_recovery_root_set_begin(f->fd, TEST_SYSID,
		pending->identity.database_incarnation, v2_storage, 4 * 1024 * 1024, &f->roots), result);
	for (unsigned i = 0; i < 20000 && (result == CLUSTER_RECOVERY_ROOTS_MORE
		|| result == CLUSTER_RECOVERY_ROOTS_ENTRY || result == CLUSTER_RECOVERY_ROOTS_BYTES); i++) {
		ClusterRecoveryRootsEvent event;
		result = cluster_recovery_root_set_step(f->roots, &event);
	}
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_ROOTS_OBSERVED);
	UT_ASSERT_EQ(cluster_relmap_retention_census_begin(f->roots, TEST_SYSID,
		pending->identity.database_incarnation, v2_storage, f->directories, 2,
		128 * 1024, &f->census), CLUSTER_RELMAP_CENSUS_MORE);
}

static ClusterRelmapCensusCommitted
relmap_census_emission(RelmapCensusWalFixture *f, Oid dbid)
{
	ClusterRelmapCensusCommitted out = { 0 };
	ClusterRelmapCensusResult result = CLUSTER_RELMAP_CENSUS_MORE;

	for (unsigned i = 0; i < 20000 && result == CLUSTER_RELMAP_CENSUS_MORE; i++) {
		result = cluster_relmap_retention_census_next_committed(f->census, &out);
		if (out.present && out.identity.dbid == dbid)
			return out;
	}
	UT_ASSERT(false);
	return out;
}

static ClusterRelmapCensusResult
relmap_census_drain(RelmapCensusWalFixture *f)
{
	ClusterRelmapCensusResult result = CLUSTER_RELMAP_CENSUS_MORE;

	for (unsigned i = 0; i < 20000 && result == CLUSTER_RELMAP_CENSUS_MORE; i++)
		result = cluster_relmap_retention_census_next(f->census);
	UT_ASSERT_NE(result, CLUSTER_RELMAP_CENSUS_MORE);
	return result;
}

static ClusterRelmapPendingDescriptor
relmap_census_descriptor(const ClusterRelmapV2Image *image)
{
	ClusterRelmapPendingDescriptor descriptor = { 0 };

	descriptor.kind = CLUSTER_RELMAP_PENDING_DROP;
	descriptor.identity.system_identifier = TEST_SYSID;
	descriptor.identity.database_incarnation = image->identity.database_incarnation;
	memcpy(descriptor.identity.storage_uuid, v2_storage, 16);
	descriptor.identity.locator = (RelFileLocator){ image->identity.shared
		? GLOBALTABLESPACE_OID : DEFAULTTABLESPACE_OID, image->identity.dbid, 70000 };
	return descriptor;
}

static void
relmap_census_pin_retained(RelmapCensusWalFixture *f, const ClusterRelmapV2Image *image,
	bool refused)
{
	unsigned pins = 0;
	uint8 digest[32];

	for (unsigned i = 0; i < 2; i++) {
		UT_ASSERT_EQ(f->origin[i].state, refused ? CLUSTER_RELMAP_RETENTION_UNKNOWN
			: CLUSTER_RELMAP_RETENTION_BUILDING);
		for (unsigned p = 0; p < lengthof(f->origin[i].pins); p++) {
			const ClusterRelmapRetentionPin *pin = &f->origin[i].pins[p];
			if (!pin->occupied)
				continue;
			pins++;
			UT_ASSERT_EQ(pin->key.origin, image->pending_owner.node);
			UT_ASSERT_EQ(pin->key.top_full_xid, image->pending_owner.top_full_xid);
			UT_ASSERT_EQ(pin->wal_end, image->pending_owner.wal_end);
		}
	}
	UT_ASSERT_EQ(pins, 1);
	UT_ASSERT_EQ(cluster_recovery_root_set_digest(f->roots, digest), !refused);
}

static void
relmap_census_end(RelmapCensusWalFixture *f)
{
	cluster_relmap_retention_census_destroy(f->census);
	cluster_recovery_root_set_destroy(f->roots);
	UT_ASSERT_EQ(unlinkat(f->fd, "global/pgrac_relmap_authority", 0), 0);
	UT_ASSERT_EQ(unlinkat(f->fd, "global/pgrac_relmap_authority.bak", 0), 0);
	UT_ASSERT_EQ(unlinkat(f->fd, "base/42/pgrac_relmap_authority", 0), 0);
	UT_ASSERT_EQ(unlinkat(f->fd, "base/42/pgrac_relmap_authority.bak", 0), 0);
	UT_ASSERT_EQ(unlinkat(f->fd, "base/42", AT_REMOVEDIR), 0);
	UT_ASSERT_EQ(unlinkat(f->fd, "base", AT_REMOVEDIR), 0);
	UT_ASSERT_EQ(unlinkat(f->fd, "global", AT_REMOVEDIR), 0);
	UT_ASSERT_EQ(close(f->fd), 0);
	UT_ASSERT_EQ(rmdir(f->path), 0);
}

static ClusterRelmapRetentionDirectory *relmap_census_drift_directory;

static void
relmap_census_borrowed_drift(void)
{
	test_stop_share_hook = NULL;
	relmap_census_drift_directory->generation++;
}

UT_TEST(test_census_wal_rebuild_stage_and_publish_cuts)
{
	for (unsigned cut = 0; cut < 8; cut++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, cut % 2, cut % 4 < 2, true, false);
		RelmapCensusWalFixture f;
		ClusterRelmapPendingSet pending;
		ClusterRelmapPendingDescriptor descriptor = relmap_census_descriptor(&image);
		ClusterRelmapCensusCommitted out;

		relmap_census_begin(&f, &image, cut >= 4);
		out = relmap_census_emission(&f, image.identity.dbid);
		UT_ASSERT_EQ(out.generation, cut >= 4 ? image.pending_generation : image.committed_generation);
		UT_ASSERT_EQ(out.map.mappings[0].mapfilenumber, cut >= 4 ? 40001 : 40000);
		cluster_relmap_pending_init(&pending);
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_wal(f.census,
			&pending, inputs, &descriptor, 1), CLUSTER_RELMAP_PENDING_OK);
		UT_ASSERT(cluster_relmap_pending_contains(&pending, descriptor.kind, &descriptor.identity));
		UT_ASSERT(pending.entries[0].main_placeholder && !pending.entries[0].copies_cleared_readback
			&& !pending.entries[0].native_terminal && !pending.entries[0].tt_complete
			&& !pending.entries[0].space_complete);
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_wal(f.census,
			&pending, inputs, &descriptor, 1), CLUSTER_RELMAP_PENDING_NOT_READY);
		UT_ASSERT_EQ(relmap_census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
		relmap_census_pin_retained(&f, &image, false);
		UT_ASSERT(inputs_pin_held && inputs_pin_current && test_actual_cf == NoLock);
		relmap_census_end(&f);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_census_wal_mismatch_is_atomic_and_sticky)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 1, false, true, false);
		RelmapCensusWalFixture f;
		ClusterRelmapPendingSet pending, before;
		ClusterRelmapPendingDescriptor descriptor = relmap_census_descriptor(&image);

		if (fault == 0) image.pending_owner.wal_end++;
		if (fault == 1) image.pending_owner.top_full_xid++;
		if (fault == 2) image.pending_owner.admitted_incarnation++;
		if (fault == 3) { image.pending.mappings[0].mapfilenumber++; relmap_test_crc(&image.pending); }
		relmap_census_begin(&f, &image, true);
		(void)relmap_census_emission(&f, image.identity.dbid);
		cluster_relmap_pending_init(&pending);
		before = pending;
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_wal(f.census,
			&pending, inputs, &descriptor, 1), CLUSTER_RELMAP_PENDING_NOT_READY);
		UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
		relmap_census_pin_retained(&f, &image, true);
		UT_ASSERT_EQ(relmap_census_drain(&f), CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending(f.census,
			&pending, &descriptor, 1), CLUSTER_RELMAP_PENDING_NOT_READY);
		UT_ASSERT(inputs_pin_held && test_actual_cf == NoLock);
		relmap_census_end(&f);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_census_wal_wait_online_and_missing_scope_refuse)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, true, fault != 1, fault == 0);
		RelmapCensusWalFixture f;
		ClusterRelmapPendingSet pending, before;

		relmap_census_begin(&f, &image, false);
		(void)relmap_census_emission(&f, image.identity.dbid);
		cluster_relmap_pending_init(&pending);
		before = pending;
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_wal(f.census,
			&pending, fault == 2 ? NULL : inputs, NULL, 0), CLUSTER_RELMAP_PENDING_NOT_READY);
		UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
		relmap_census_pin_retained(&f, &image, true);
		UT_ASSERT(inputs_pin_held && test_actual_cf == NoLock);
		relmap_census_end(&f);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_census_wal_original_recheck_error_retains_both_pins)
{
	for (unsigned fault = 1; fault <= 3; fault++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, false, true, false);
		RelmapCensusWalFixture f;
		ClusterRelmapPendingSet pending, before;
		volatile bool threw = false;

		relmap_census_begin(&f, &image, false);
		(void)relmap_census_emission(&f, image.identity.dbid);
		cluster_relmap_pending_init(&pending);
		before = pending;
		relmap_test_fault = fault;
		test_stop_share_call = test_cf_lock_calls + 6;
		test_stop_share_hook = relmap_test_recheck_fault;
		PG_TRY();
		{
			UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_wal(f.census,
				&pending, inputs, NULL, 0), CLUSTER_RELMAP_PENDING_NOT_READY);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		test_throw_root_read = false;
		UT_ASSERT(test_stop_share_hook == NULL);
		test_stop_share_hook = NULL;
		UT_ASSERT_EQ(threw, fault == 3);
		UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
		relmap_census_pin_retained(&f, &image, true);
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending(f.census,
			&pending, NULL, 0), CLUSTER_RELMAP_PENDING_NOT_READY);
		UT_ASSERT(inputs_pin_held && test_actual_cf == NoLock);
		relmap_census_end(&f);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_census_wal_structural_conflict_leaves_destination)
{
	ClusterRelmapV2Image image;
	ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, false, true, false);
	RelmapCensusWalFixture f;
	ClusterRelmapPendingSet pending, before;
	ClusterRelmapPendingDescriptor descriptors[2];

	descriptors[0] = relmap_census_descriptor(&image);
	descriptors[1] = descriptors[0];
	relmap_census_begin(&f, &image, true);
	(void)relmap_census_emission(&f, image.identity.dbid);
	cluster_relmap_pending_init(&pending);
	before = pending;
	UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_wal(f.census,
		&pending, inputs, descriptors, 2), CLUSTER_RELMAP_PENDING_CONFLICT);
	UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
	relmap_census_pin_retained(&f, &image, true);
	UT_ASSERT(inputs_pin_held);
	relmap_census_end(&f);
	cold_census_done(&inputs);
}

UT_TEST(test_census_wal_rechecks_borrowed_directory_after_scan)
{
	ClusterRelmapV2Image image;
	ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, false, true, false);
	RelmapCensusWalFixture f;
	ClusterRelmapPendingSet pending, before;
	ClusterRelmapPendingDescriptor descriptor = relmap_census_descriptor(&image);

	relmap_census_begin(&f, &image, false);
	(void)relmap_census_emission(&f, image.identity.dbid);
	cluster_relmap_pending_init(&pending);
	before = pending;
	relmap_census_drift_directory = &f.origin[1];
	test_stop_share_call = test_cf_lock_calls + 6;
	test_stop_share_hook = relmap_census_borrowed_drift;
	UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_wal(f.census,
		&pending, inputs, &descriptor, 1), CLUSTER_RELMAP_PENDING_NOT_READY);
	UT_ASSERT(test_stop_share_hook == NULL);
	test_stop_share_hook = NULL;
	relmap_census_drift_directory = NULL;
	UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
	UT_ASSERT_EQ(relmap_census_drain(&f), CLUSTER_RELMAP_CENSUS_CHANGED);
	relmap_census_pin_retained(&f, &image, true);
	UT_ASSERT_EQ(cluster_wal_inputs_revalidate_v1(inputs), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(inputs_pin_held);
	relmap_census_end(&f);
	cold_census_done(&inputs);
}

UT_TEST(test_census_wal_physical_root_recheck_still_required)
{
	ClusterRelmapV2Image image;
	ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, false, true, false);
	RelmapCensusWalFixture f;
	ClusterRelmapPendingSet pending;
	ClusterRelmapPendingDescriptor descriptor = relmap_census_descriptor(&image);

	relmap_census_begin(&f, &image, false);
	(void)relmap_census_emission(&f, image.identity.dbid);
	cluster_relmap_pending_init(&pending);
	UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_wal(f.census,
		&pending, inputs, &descriptor, 1), CLUSTER_RELMAP_PENDING_OK);
	UT_ASSERT(cluster_relmap_pending_contains(&pending, descriptor.kind, &descriptor.identity));
	image.pending_owner.boot_incarnation++;
	relmap_census_file(f.fd, "base/42/pgrac_relmap_authority.bak", &image);
	UT_ASSERT_NE(relmap_census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
	relmap_census_pin_retained(&f, &image, true);
	/* The caller must discard the provisional set after that refusal. */
	UT_ASSERT(!pending.entries[0].copies_cleared_readback && !pending.entries[0].native_terminal);
	UT_ASSERT(inputs_pin_held);
	relmap_census_end(&f);
	cold_census_done(&inputs);
}

UT_TEST(test_census_wal_clean_pair_and_missing_window)
{
	ClusterRelmapV2Image image;
	ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, false, true, false);
	RelmapCensusWalFixture f;
	ClusterRelmapPendingSet pending, before;
	ClusterRelmapPendingDescriptor descriptor;

	relmap_census_begin(&f, &image, false);
	cluster_relmap_pending_init(&pending);
	before = pending;
	UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_wal(f.census,
		&pending, inputs, NULL, 0), CLUSTER_RELMAP_PENDING_NOT_READY);
	UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
	/* The other namespace is clean, but structural descriptors remain independent. */
	(void)relmap_census_emission(&f, InvalidOid);
	image.identity.dbid = InvalidOid;
	image.identity.shared = true;
	descriptor = relmap_census_descriptor(&image);
	UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_wal(f.census,
		&pending, inputs, &descriptor, 1), CLUSTER_RELMAP_PENDING_OK);
	UT_ASSERT(cluster_relmap_pending_contains(&pending, descriptor.kind, &descriptor.identity));
	UT_ASSERT_EQ(relmap_census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
	UT_ASSERT(inputs_pin_held && inputs_pin_current);
	relmap_census_end(&f);
	cold_census_done(&inputs);
}

typedef struct RelmapCensusSideFixture
{
	ClusterRelmapPendingSideSource source;
	RfSideOnlinePlanV1 *plan;
	ClusterSpaceIdentityKey key;
	RelmapRetainedSideFixture retained;
	const ClusterRelmapPendingSideSource *selected;
	size_t count;
} RelmapCensusSideFixture;

/* Controlled decoded SIDE record, owned and sealed by the original plan.
 * Source selection/cut correspondence to the held WAL scope is still the
 * original caller's qualification, not a claim made by this fixture. */
static void
relmap_census_side(RelmapCensusSideFixture *f, const ClusterRelmapV2Image *image,
	unsigned selected, bool seal)
{
	RfSideOnlinePlanRequestV1 request = { 0 };
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	XLogReaderState reader = { 0 };
	DecodedXLogRecord decoded = { 0 };
	RfPageOnlineRecordIdentityV1 identity = { 0 };
	RfDetachedRecordPlanV1 record = { 0 };
	XLogRecPtr redo = 200;

	memset(f, 0, sizeof(*f));
	f->selected = &f->source;
	f->count = 1;
	f->source.source = relmap_test_source[selected];
	f->source.cut.failed_thread = f->source.source.claim.identity.origin_thread_id;
	f->source.cut.timeline_id = f->source.source.timeline;
	f->source.cut.origin_owner_incarnation =
		f->source.source.claim.identity.origin_owner_incarnation;
	f->source.cut.scan_begin_inclusive = 100;
	f->source.cut.scan_end_exclusive = 200;
	f->source.cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	request.system_identifier = image->identity.system_identifier;
	memcpy(request.storage_uuid, v2_storage, 16);
	request.physical_cuts = &f->source.cut;
	request.participant_count = 1;
	request.purpose = RF_SIDE_PLAN_RECOVERY;
	request.redo_starts = &redo;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &f->plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_bind_sources_v1(f->plan, &f->source.source, 1));
	f->source.plan = f->plan;
	f->key = relmap_census_descriptor(image).identity;
	relmap_test_truncate(image, wal);
	decoded.header.xl_rmid = RM_SMGR_ID;
	decoded.header.xl_info = XLOG_SMGR_SPACE_IDENTITY | XLR_SPECIAL_REL_UPDATE;
	decoded.header.xl_crc = 0xabc123;
	decoded.header.xl_xid = 4195136;
	decoded.main_data = (char *)wal;
	decoded.main_data_len = sizeof(wal);
	decoded.max_block_id = -1;
	decoded.lsn = reader.ReadRecPtr = 100;
	decoded.next_lsn = reader.EndRecPtr = 200;
	reader.system_identifier = image->identity.system_identifier;
	reader.record = &decoded;
	identity.record.system_identifier = reader.system_identifier;
	memcpy(identity.record.storage_uuid, v2_storage, 16);
	identity.record.origin_thread = f->source.cut.failed_thread;
	identity.record.timeline_id = f->source.cut.timeline_id;
	identity.record.read_rec_ptr = 100;
	identity.record.end_rec_ptr = 200;
	identity.record.record_crc = decoded.header.xl_crc;
	identity.record.rmid = decoded.header.xl_rmid;
	identity.record.info = decoded.header.xl_info;
	record.source_record = &reader;
	record.route.rmid = RM_SMGR_ID;
	record.route.normalized_info = XLOG_SMGR_SPACE_IDENTITY;
	record.route.record_owner = RF_ROUTE_OWNER_SIDE_TYPED;
	record.route.block_policy = RF_ROUTE_BLOCKS_FORBIDDEN;
	record.route.codec_id = RF_ROUTE_CODEC_SIDE_STANDARD;
	record.preflight_complete = true;
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(f->plan, &record, &identity),
		RF_PAGE_PROOF_DETAIL_OK);
	memset(wal, 0xee, sizeof(wal));
	if (seal) {
		UT_ASSERT(rf_side_online_plan_bind_database_v1(f->plan, image->identity.database_incarnation));
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(f->plan), RF_PAGE_PROOF_DETAIL_OK);
		/* This operation is retained history, not a redo execution target. */
		UT_ASSERT_EQ(rf_side_online_plan_space_target_count_v1(f->plan), 0);
	}
}

/* Full original held-source vector; history starts at each actual retained end. */
static void
relmap_census_side_bound(RelmapCensusSideFixture *f, ClusterWalInputsV1 *inputs,
	const ClusterRelmapV2Image *image, bool split, unsigned fault)
{
	memset(f, 0, sizeof(*f));
	f->key = relmap_census_descriptor(image).identity;
	relmap_retained_side(&f->retained, inputs, image, split, fault, true);
	f->selected = f->retained.sources;
	f->count = 2;
	for (unsigned i = 0; i < 2; i++)
		UT_ASSERT_EQ(rf_side_online_plan_space_target_count_v1(f->retained.sources[i].plan), 0);
}

static void
relmap_census_side_destroy(RelmapCensusSideFixture *f)
{
	rf_side_online_plan_destroy_v1(&f->plan);
	relmap_retained_side_destroy(&f->retained);
}

UT_TEST(test_census_side_wal_structural_capture_refusal_is_atomic)
{
	for (unsigned fault = 5; fault <= 12; fault++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture_with_side(&image,
			fault % 2, fault % 4 < 2, true, false, 1);
		RelmapCensusWalFixture f;
		RelmapCensusSideFixture side;
		ClusterRelmapPendingSet pending, before;

		relmap_census_side_bound(&side, inputs, &image, fault % 2 != 0, fault);
		relmap_census_begin(&f, &image, false);
		(void)relmap_census_emission(&f, image.identity.dbid);
		cluster_relmap_pending_init(&pending);
		before = pending;
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_side_wal(f.census,
			&pending, inputs, side.selected, side.count), CLUSTER_RELMAP_PENDING_NOT_READY);
		UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
		relmap_census_pin_retained(&f, &image, true);
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending(f.census,
			&pending, NULL, 0), CLUSTER_RELMAP_PENDING_NOT_READY);
		UT_ASSERT(inputs_pin_held && inputs_pin_current && test_actual_cf == NoLock);
		relmap_census_side_destroy(&side);
		relmap_census_end(&f);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_census_side_wal_same_window_stage_publish_and_history)
{
	for (unsigned cut = 0; cut < 8; cut++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture_with_side(&image, cut % 2, cut % 4 < 2, true, false, true);
		RelmapCensusWalFixture f;
		RelmapCensusSideFixture side;
		ClusterRelmapPendingSet pending, installed;

		relmap_census_side_bound(&side, inputs, &image, cut % 2 != 0, 0);
		relmap_census_begin(&f, &image, cut >= 4);
		(void)relmap_census_emission(&f, image.identity.dbid);
		cluster_relmap_pending_init(&pending);
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_side_wal(f.census,
			&pending, inputs, side.selected, side.count), CLUSTER_RELMAP_PENDING_OK);
		UT_ASSERT(cluster_relmap_pending_contains(&pending, CLUSTER_RELMAP_PENDING_TRUNCATE, &side.key));
		UT_ASSERT(pending.entries[0].main_placeholder && !pending.entries[0].copies_cleared_readback
			&& !pending.entries[0].native_terminal && !pending.entries[0].tt_complete
			&& !pending.entries[0].space_complete);
		UT_ASSERT(!cluster_relmap_pending_can_release(&pending, CLUSTER_RELMAP_PENDING_TRUNCATE, &side.key));
		installed = pending;
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_side_wal(f.census,
			&pending, inputs, side.selected, side.count), CLUSTER_RELMAP_PENDING_NOT_READY);
		UT_ASSERT_EQ(memcmp(&pending, &installed, sizeof(pending)), 0);
		UT_ASSERT_EQ(relmap_census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
		relmap_census_pin_retained(&f, &image, false);
		UT_ASSERT(inputs_pin_held && inputs_pin_current && test_actual_cf == NoLock);
		relmap_census_side_destroy(&side);
		relmap_census_end(&f);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_census_side_wal_side_refusal_precedes_scan)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, false, true, false);
		RelmapCensusWalFixture f;
		RelmapCensusSideFixture side;
		ClusterRelmapPendingSet pending, before;

		relmap_census_side(&side, &image, 0, fault != 1);
		if (fault == 2) side.source.source.claim.claim_sha256[0]++;
		if (fault == 3) side.source.cut.scan_end_exclusive++;
		relmap_census_begin(&f, &image, false);
		(void)relmap_census_emission(&f, image.identity.dbid);
		cluster_relmap_pending_init(&pending);
		UT_ASSERT_EQ(cluster_relmap_pending_add(&pending, CLUSTER_RELMAP_PENDING_DROP,
			&side.key, true), CLUSTER_RELMAP_PENDING_OK);
		before = pending;
		inputs_claim_calls = 0;
		inputs_claim_observe = true;
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_side_wal(f.census,
			&pending, inputs, fault == 0 ? NULL : side.selected, fault == 0 ? 0 : side.count),
			CLUSTER_RELMAP_PENDING_NOT_READY);
		UT_ASSERT_EQ(inputs_claim_calls, 0);
		inputs_claim_observe = false;
		UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
		relmap_census_pin_retained(&f, &image, true);
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending(f.census,
			&pending, NULL, 0), CLUSTER_RELMAP_PENDING_NOT_READY);
		UT_ASSERT(inputs_pin_held && test_actual_cf == NoLock);
		relmap_census_side_destroy(&side);
		relmap_census_end(&f);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_census_side_wal_wal_refusal_never_installs_side)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture_with_side(&image, 1, true, fault != 3, false, true);
		RelmapCensusWalFixture f;
		RelmapCensusSideFixture side;
		ClusterRelmapPendingSet pending, before;

		relmap_census_side_bound(&side, inputs, &image, false, 0);
		if (fault == 2) {
			cold_census_done(&inputs);
			inputs = relmap_test_fixture_with_side(&image, 1, true, true, true, true);
		}
		if (fault == 0) image.pending_owner.wal_end++;
		relmap_census_begin(&f, &image, true);
		(void)relmap_census_emission(&f, image.identity.dbid);
		cluster_relmap_pending_init(&pending);
		before = pending;
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_side_wal(f.census,
			&pending, fault == 1 ? NULL : inputs, side.selected, side.count), CLUSTER_RELMAP_PENDING_NOT_READY);
		UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
		relmap_census_pin_retained(&f, &image, true);
		UT_ASSERT(inputs_pin_held && test_actual_cf == NoLock);
		relmap_census_side_destroy(&side);
		relmap_census_end(&f);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_census_side_wal_recheck_and_error_keep_window_invalid)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture_with_side(&image, 0, false, true, false, true);
		RelmapCensusWalFixture f;
		RelmapCensusSideFixture side;
		ClusterRelmapPendingSet pending, before;
		volatile bool threw = false;

		relmap_census_side_bound(&side, inputs, &image, false, 0);
		relmap_census_begin(&f, &image, false);
		(void)relmap_census_emission(&f, image.identity.dbid);
		cluster_relmap_pending_init(&pending);
		before = pending;
		relmap_census_drift_directory = &f.origin[1];
		relmap_test_fault = fault == 1 ? 2 : 3;
		test_stop_share_call = test_cf_lock_calls + 6;
		test_stop_share_hook = fault == 0 ? relmap_census_borrowed_drift : relmap_test_recheck_fault;
		PG_TRY();
		{
			UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_side_wal(f.census,
				&pending, inputs, side.selected, side.count), CLUSTER_RELMAP_PENDING_NOT_READY);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		test_throw_root_read = false;
		UT_ASSERT(test_stop_share_hook == NULL);
		test_stop_share_hook = NULL;
		relmap_census_drift_directory = NULL;
		UT_ASSERT_EQ(threw, fault == 2);
		UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
		relmap_census_pin_retained(&f, &image, true);
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending(f.census,
			&pending, NULL, 0), CLUSTER_RELMAP_PENDING_NOT_READY);
		UT_ASSERT(inputs_pin_held && test_actual_cf == NoLock);
		relmap_census_side_destroy(&side);
		relmap_census_end(&f);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_census_side_wal_late_physical_root_rejects_provisional)
{
	ClusterRelmapV2Image image;
	ClusterWalInputsV1 *inputs = relmap_test_fixture_with_side(&image, 0, false, true, false, true);
	RelmapCensusWalFixture f;
	RelmapCensusSideFixture side;
	ClusterRelmapPendingSet pending;

	relmap_census_side_bound(&side, inputs, &image, false, 0);
	relmap_census_begin(&f, &image, false);
	(void)relmap_census_emission(&f, image.identity.dbid);
	cluster_relmap_pending_init(&pending);
	UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_side_wal(f.census,
		&pending, inputs, side.selected, side.count), CLUSTER_RELMAP_PENDING_OK);
	UT_ASSERT(cluster_relmap_pending_contains(&pending, CLUSTER_RELMAP_PENDING_TRUNCATE, &side.key));
	image.pending_owner.boot_incarnation++;
	relmap_census_file(f.fd, "base/42/pgrac_relmap_authority.bak", &image);
	UT_ASSERT_NE(relmap_census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
	relmap_census_pin_retained(&f, &image, true);
	UT_ASSERT(!cluster_relmap_pending_can_release(&pending, CLUSTER_RELMAP_PENDING_TRUNCATE, &side.key));
	UT_ASSERT(inputs_pin_held);
	relmap_census_side_destroy(&side);
	relmap_census_end(&f);
	cold_census_done(&inputs);
}

UT_TEST(test_census_side_wal_collector_error_invalidates_before_wal)
{
	ClusterRelmapV2Image image;
	ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, false, true, false);
	RelmapCensusWalFixture f;
	RelmapCensusSideFixture side;
	ClusterRelmapPendingSet pending, before;
	volatile bool threw = false;

	relmap_census_side(&side, &image, 0, true);
	relmap_census_begin(&f, &image, false);
	(void)relmap_census_emission(&f, image.identity.dbid);
	cluster_relmap_pending_init(&pending);
	UT_ASSERT_EQ(cluster_relmap_pending_add(&pending, CLUSTER_RELMAP_PENDING_DROP,
		&side.key, true), CLUSTER_RELMAP_PENDING_OK);
	before = pending;
	inputs_claim_calls = relmap_test_side_errors = 0;
	inputs_claim_observe = relmap_test_side_error = true;
	test_last_error_level = 0;
	test_capture_error_level = true;
	PG_TRY();
	{
		(void)cluster_relmap_retention_census_rebuild_pending_side_wal(f.census,
			&pending, inputs, side.selected, side.count);
	}
	PG_CATCH();
	{
		threw = true;
	}
	PG_END_TRY();
	test_capture_error_level = false;
	UT_ASSERT(threw && !relmap_test_side_error);
	UT_ASSERT_EQ(test_last_error_level, ERROR);
	UT_ASSERT_EQ(relmap_test_side_errors, 1);
	UT_ASSERT_EQ(inputs_claim_calls, 0);
	inputs_claim_observe = relmap_test_side_error = false;
	UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
	relmap_census_pin_retained(&f, &image, true);
	UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending(f.census,
		&pending, NULL, 0), CLUSTER_RELMAP_PENDING_NOT_READY);
	UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
	UT_ASSERT_EQ(cluster_wal_inputs_revalidate_v1(inputs), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(inputs_pin_held && inputs_pin_current && test_actual_cf == NoLock);
	relmap_census_side_destroy(&side);
	relmap_census_end(&f);
	cold_census_done(&inputs);
}

UT_TEST(test_census_side_wal_window_clean_pair_and_destination)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture_with_side(&image, 0, false, true, false, true);
		RelmapCensusWalFixture f;
		RelmapCensusSideFixture side;
		ClusterRelmapPendingSet pending, before;

		relmap_census_begin(&f, &image, false);
		cluster_relmap_pending_init(&pending);
		before = pending;
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_side_wal(f.census,
			&pending, inputs, NULL, 0), CLUSTER_RELMAP_PENDING_NOT_READY);
		UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_side_wal(NULL,
			&pending, inputs, NULL, 0), CLUSTER_RELMAP_PENDING_INVALID_INPUT);
		(void)relmap_census_emission(&f, fault == 0 ? InvalidOid : image.identity.dbid);
		if (fault == 0) {
			image.identity.dbid = InvalidOid;
			image.identity.shared = true;
		}
		if (fault == 0)
			relmap_census_side(&side, &image, 0, true);
		else
			relmap_census_side_bound(&side, inputs, &image, false, 0);
		if (fault == 1)
			UT_ASSERT_EQ(cluster_relmap_pending_add(&pending, CLUSTER_RELMAP_PENDING_DROP,
				&side.key, true), CLUSTER_RELMAP_PENDING_OK);
		before = pending;
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_side_wal(f.census,
			fault == 2 ? NULL : &pending, fault == 0 ? NULL : inputs, side.selected, side.count),
			fault == 0 ? CLUSTER_RELMAP_PENDING_NOT_READY : fault == 1 ? CLUSTER_RELMAP_PENDING_CONFLICT
			: CLUSTER_RELMAP_PENDING_INVALID_INPUT);
		if (fault == 0) {
			/* Clean authority does not exempt selected SIDE from native checks. */
			UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
			UT_ASSERT_EQ(cluster_relmap_retention_census_next(f.census), CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
			for (unsigned i = 0; i < 2; i++)
				UT_ASSERT_EQ(f.origin[i].state, CLUSTER_RELMAP_RETENTION_UNKNOWN);
		} else {
			UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
			relmap_census_pin_retained(&f, &image, true);
		}
		UT_ASSERT(inputs_pin_held);
		relmap_census_side_destroy(&side);
		relmap_census_end(&f);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_census_side_wal_clean_pair_rebuilds_original_structure)
{
	for (unsigned mode = 0; mode < 4; mode++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture_with_side(&image,
			mode % 2, mode < 2, true, false, true);
		RelmapCensusWalFixture f;
		RelmapCensusSideFixture side;
		ClusterRelmapPendingSet pending;

		relmap_census_side_bound(&side, inputs, &image, mode % 2 != 0, 0);
		image.pending_generation = 0;
		memset(&image.pending_owner, 0, sizeof(image.pending_owner));
		memset(&image.pending, 0, sizeof(image.pending));
		relmap_census_begin(&f, &image, false);
		(void)relmap_census_emission(&f, image.identity.dbid);
		cluster_relmap_pending_init(&pending);
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_side_wal(f.census,
			&pending, inputs, side.selected, side.count), CLUSTER_RELMAP_PENDING_OK);
		UT_ASSERT(cluster_relmap_pending_contains(&pending, CLUSTER_RELMAP_PENDING_TRUNCATE, &side.key));
		UT_ASSERT(!cluster_relmap_pending_can_release(&pending, CLUSTER_RELMAP_PENDING_TRUNCATE, &side.key));
		UT_ASSERT_EQ(relmap_census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
		for (unsigned i = 0; i < 2; i++)
			UT_ASSERT_EQ(f.origin[i].state, CLUSTER_RELMAP_RETENTION_BUILDING);
		UT_ASSERT_EQ(cluster_wal_inputs_revalidate_v1(inputs), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(inputs_pin_held && inputs_pin_current && test_actual_cf == NoLock);
		relmap_census_side_destroy(&side);
		relmap_census_end(&f);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_census_side_wal_clean_pair_refusal_is_atomic_and_sticky)
{
	for (unsigned fault = 0; fault < 11; fault++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture_with_side(&image,
			0, fault % 2 != 0, true, false, true);
		RelmapCensusWalFixture f;
		RelmapCensusSideFixture side;
		ClusterRelmapPendingSet pending, before;
		volatile bool threw = false;
		unsigned capture_fault = fault == 2 ? 2 : fault == 3 ? 1
			: fault == 4 ? 5 : fault == 5 ? 6 : 0;

		relmap_census_side_bound(&side, inputs, &image, true, capture_fault);
		if (fault == 1) side.count = 1;
		if (fault >= 9) {
			cold_census_done(&inputs);
			inputs = relmap_test_fixture_with_side(&image, 0, fault % 2 != 0,
				fault == 9, fault == 9, true);
		}
		if (fault == 6) {
			char path[MAXPGPATH];
			uint8 bad = 0x5a;
			int fd;

			v2_checkpoint_wal_path(&relmap_test_source[1].claim.identity,
				relmap_test_record[1].exclusive_end, relmap_test_source[1].timeline, path);
			fd = open(path, O_WRONLY);
			UT_ASSERT(fd >= 0);
			UT_ASSERT_EQ(pwrite(fd, &bad, 1, relmap_test_record[1].exclusive_end
				% wal_segment_size + SizeOfXLogRecord + 2), 1);
			UT_ASSERT_EQ(close(fd), 0);
		}
		image.pending_generation = 0;
		memset(&image.pending_owner, 0, sizeof(image.pending_owner));
		memset(&image.pending, 0, sizeof(image.pending));
		relmap_census_begin(&f, &image, false);
		(void)relmap_census_emission(&f, image.identity.dbid);
		cluster_relmap_pending_init(&pending);
		before = pending;
		if (fault == 7 || fault == 8) {
			relmap_test_fault = fault == 7 ? 2 : 3;
			test_stop_share_call = test_cf_lock_calls + 6;
			test_stop_share_hook = relmap_test_recheck_fault;
		}
		PG_TRY();
		{
			UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_side_wal(f.census,
				&pending, fault == 0 ? NULL : inputs, side.selected, side.count),
				CLUSTER_RELMAP_PENDING_NOT_READY);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		test_throw_root_read = false;
		if (fault == 7 || fault == 8) UT_ASSERT(test_stop_share_hook == NULL);
		test_stop_share_hook = NULL;
		UT_ASSERT_EQ(threw, fault == 8);
		UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
		for (unsigned i = 0; i < 2; i++)
			UT_ASSERT_EQ(f.origin[i].state, CLUSTER_RELMAP_RETENTION_UNKNOWN);
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending(f.census,
			&pending, NULL, 0), CLUSTER_RELMAP_PENDING_NOT_READY);
		UT_ASSERT_EQ(cluster_relmap_retention_census_next(f.census), CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
		UT_ASSERT(inputs_pin_held && test_actual_cf == NoLock);
		relmap_census_side_destroy(&side);
		relmap_census_end(&f);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_census_side_wal_checks_original_full_source_vector)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture_with_side(&image, 0, false, true, false, true);
		RelmapCensusWalFixture f;
		RelmapCensusSideFixture side;
		ClusterRelmapPendingSet pending, before;

		relmap_census_side_bound(&side, inputs, &image, true, fault);
		if (fault == 0) side.count = 1; /* Complete individual plan, missing other selected origin. */
		relmap_census_begin(&f, &image, false);
		(void)relmap_census_emission(&f, image.identity.dbid);
		cluster_relmap_pending_init(&pending);
		before = pending;
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_side_wal(f.census,
			&pending, inputs, side.selected, side.count), CLUSTER_RELMAP_PENDING_NOT_READY);
		UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
		relmap_census_pin_retained(&f, &image, true);
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending(f.census,
			&pending, NULL, 0), CLUSTER_RELMAP_PENDING_NOT_READY);
		UT_ASSERT(inputs_pin_held && test_actual_cf == NoLock);
		relmap_census_side_destroy(&side);
		relmap_census_end(&f);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_census_side_collector_error_invalidates_window)
{
	ClusterRelmapV2Image image;
	ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, false, true, false);
	RelmapCensusWalFixture f;
	RelmapCensusSideFixture side;
	ClusterRelmapPendingSet pending, before;
	volatile bool threw = false;

	relmap_census_side(&side, &image, 0, true);
	relmap_census_begin(&f, &image, false);
	(void)relmap_census_emission(&f, image.identity.dbid);
	cluster_relmap_pending_init(&pending);
	UT_ASSERT_EQ(cluster_relmap_pending_add(&pending, CLUSTER_RELMAP_PENDING_DROP,
		&side.key, true), CLUSTER_RELMAP_PENDING_OK);
	before = pending;
	inputs_claim_calls = relmap_test_side_errors = 0;
	inputs_claim_observe = relmap_test_side_error = true;
	test_last_error_level = 0;
	test_capture_error_level = true;
	PG_TRY();
	{
		(void)cluster_relmap_retention_census_rebuild_pending_side(f.census,
			&pending, side.selected, side.count);
	}
	PG_CATCH();
	{
		threw = true;
	}
	PG_END_TRY();
	test_capture_error_level = false;
	UT_ASSERT(threw && !relmap_test_side_error);
	UT_ASSERT_EQ(test_last_error_level, ERROR);
	UT_ASSERT_EQ(relmap_test_side_errors, 1);
	UT_ASSERT_EQ(inputs_claim_calls, 0);
	inputs_claim_observe = relmap_test_side_error = false;
	UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
	relmap_census_pin_retained(&f, &image, true);
	UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending(f.census,
		&pending, NULL, 0), CLUSTER_RELMAP_PENDING_NOT_READY);
	UT_ASSERT_EQ(cluster_relmap_retention_census_next(f.census), CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
	UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
	UT_ASSERT_EQ(cluster_wal_inputs_revalidate_v1(inputs), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(inputs_pin_held && inputs_pin_current && test_actual_cf == NoLock);
	relmap_census_side_destroy(&side);
	relmap_census_end(&f);
	cold_census_done(&inputs);
}


/* A genuine initializer terminal has a source completion but no records. Keep
 * its original predecessor/history, and put RELMAP on independent origin 3.
 * Transport, isolation and floor/issued qualification remain fixture inputs. */
typedef struct RelmapEmptySideFixture
{
	ClusterRecoverySerialGuard serial;
	ClusterRelmapPendingSideSource sources[3];
	RfSideOnlinePlanV1 *plans[3];
	unsigned empty;
} RelmapEmptySideFixture;

static ClusterWalInputsV1 *
relmap_empty_initializer_fixture(RelmapEmptySideFixture *f,
	ClusterRelmapV2Image *image, bool shared, bool split)
{
	ClusterThreadRecLaunchEligibility eligibility;
	ClusterWalStartupImage op;
	ClusterWalInputsV1 *inputs = NULL;
	ControlRootImage root;
	ControlFileData control;
	ClusterControlRootFileToken token;
	ClusterWalSourceRef source = { 0 }, sources[3];
	RfContributorStreamCutV1 cuts[3];
	XLogRecPtr redo[3];
	uint32 input_index[3];
	ClusterControlRootSnapshot *snapshot;
	ClusterRecoveryAnchorRefV2 anchor_ref = { 0 };
	ClusterRecoveryAnchorV2 anchor;
	ControlFileData peer = { 0 };
	uint8 anchor_bytes[512];
	char path[MAXPGPATH], hex[65];
	uint8 payload[MinSizeOfRelmapUpdate + sizeof(RelMapFile)];
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	xl_relmap_update update = { 0 };
	WalTestRecord tail;
	unsigned empty_count = 0, nonempty_count = 0;

	memset(f, 0, sizeof(*f));
	op = pending_worker_fixture(&f->serial, &eligibility);
	if (ut_current_failed) return NULL;
	test_worker_pin_held = false;
	test_history_sync_count = 0;
	test_history_fail_sync = UINT_MAX;
	UT_ASSERT_EQ(thread_recovery_worker_run(&eligibility), CLUSTER_THREADREC_DONE);
	test_history_fail_sync = 0;
	test_actual_cf = test_cf_mode = ShareLock;
	UT_ASSERT_EQ(cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID,
		&root, &control, &token), 0);
	/* As in the original terminal visitor fixture, remove only the unrelated
	 * uncompleted peer reservation. This edit grants no runtime admission. */
	memset(&root.startup[3], 0, sizeof(root.startup[3]));
	snapshot = &root.records[3];
	UT_ASSERT_EQ(snapshot->identity.origin_node_id, 3);
	UT_ASSERT_NE(snapshot->validated_tail_lsn_exclusive, InvalidXLogRecPtr);
	/* The older peer fixture has nextXid zero. Before opening the cold scope,
	 * seed a normal native checkpoint and matching anchor on this independent
	 * peer only; the worker terminal and its predecessor remain untouched. */
	anchor_ref.identity = snapshot->identity;
	anchor_ref.database_incarnation = root.header.v2.database_incarnation;
	anchor_ref.max_config_generation = root.header.v2.config_generation;
	anchor_ref.anchor_generation = root.refs[3].anchor_generation;
	memcpy(anchor_ref.anchor_sha256, root.refs[3].anchor_sha256, 32);
	memcpy(anchor_ref.claim_sha256, root.refs[3].claim_sha256, 32);
	for (unsigned i = 0; i < 32; i++)
		snprintf(hex + i * 2, 3, "%02x", anchor_ref.anchor_sha256[i]);
	snprintf(path, sizeof(path), "%s/global/anchor_images/thread_%u/generation_" UINT64_FORMAT
		"/anchor_" UINT64_FORMAT "-%s.bin", test_root, snapshot->identity.origin_thread_id,
		snapshot->identity.origin_owner_incarnation, anchor_ref.anchor_generation, hex);
	read_all_or_abort(path, anchor_bytes, sizeof(anchor_bytes));
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_decode(anchor_bytes, sizeof(anchor_bytes), &anchor_ref, &anchor), 0);
	anchor.checkpoint_copy.nextXid = FullTransactionIdFromU64(103);
	v2_anchor_object(root.bytes, &anchor, &snapshot->identity, path);
	memcpy(root.refs[3].anchor_sha256, root.bytes + 512 + 3 * 512 + 264, 32);
	peer.state = DB_SHUTDOWNED;
	peer.checkPoint = anchor.checkpoint;
	peer.checkPointCopy = anchor.checkpoint_copy;
	v2_checkpoint_wal_record(&snapshot->identity, &peer, 0);
	snapshot->checkpoint_record_crc32c = snapshot->tail_last_record_crc32c = test_checkpoint_crc;
	snapshot->validated_tail_lsn_exclusive = test_checkpoint_end;
	memset(image, 0, sizeof(*image));
	image->identity.system_identifier = TEST_SYSID;
	image->identity.database_incarnation = root.header.v2.database_incarnation;
	image->identity.shared = shared;
	image->identity.dbid = shared ? InvalidOid : 42;
	image->committed_generation = 7;
	image->pending_generation = 8;
	image->pending_owner.node = snapshot->identity.origin_node_id;
	image->pending_owner.admitted_incarnation = snapshot->identity.origin_owner_incarnation;
	image->pending_owner.top_full_xid = UINT64CONST(0x100000000) + 4195136;
	image->pending_owner.membership_epoch = 91;
	image->pending_owner.boot_incarnation = 202;
	image->committed.magic = RELMAPPER_FILEMAGIC;
	image->committed.num_mappings = 1;
	image->committed.mappings[0].mapoid = 1259;
	image->committed.mappings[0].mapfilenumber = 40000;
	relmap_test_crc(&image->committed);
	image->pending = image->committed;
	image->pending.mappings[0].mapfilenumber++;
	relmap_test_crc(&image->pending);
	source.claim.identity = snapshot->identity;
	source.claim.database_incarnation = root.header.v2.database_incarnation;
	source.claim.max_config_generation = root.header.v2.config_generation;
	memcpy(source.claim.claim_sha256, root.refs[3].claim_sha256, 32);
	source.timeline = snapshot->tail_tli;
	update.dbid = image->identity.dbid;
	update.tsid = shared ? GLOBALTABLESPACE_OID : DEFAULTTABLESPACE_OID;
	update.nbytes = sizeof(RelMapFile);
	memcpy(payload, &update, MinSizeOfRelmapUpdate);
	memcpy(payload + MinSizeOfRelmapUpdate, &image->pending, sizeof(RelMapFile));
	tail = relmap_test_append(&source, snapshot->validated_tail_lsn_exclusive,
		snapshot->tail_last_record_lsn, RM_RELMAP_ID, payload, sizeof(payload));
	image->pending_owner.wal_end = tail.exclusive_end;
	relmap_test_truncate(image, wal);
	tail = relmap_test_append(&source, tail.exclusive_end, tail.record_start,
		RM_SMGR_ID, wal, sizeof(wal));
	snapshot->tail_last_record_lsn = tail.record_start;
	snapshot->tail_last_record_crc32c = tail.record_crc;
	snapshot->validated_tail_lsn_exclusive = tail.exclusive_end;
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0);
	v2_write_roots(root.bytes);
	test_actual_cf = test_cf_mode = NoLock;
	test_inputs_startup_cf = true;
	UT_ASSERT_EQ(inputs_begin_for_role(true, &inputs), 0);
	if (inputs == NULL) return NULL;
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(inputs), lengthof(f->sources));
	if (cluster_wal_inputs_count_v1(inputs) != lengthof(f->sources)) return inputs;
	for (unsigned i = 0; i < lengthof(f->sources); i++) {
		const ClusterWalInputV1 *item = cluster_wal_inputs_at_v1(inputs, i);
		unsigned target = item->kind == CLUSTER_WAL_INPUT_INITIALIZER_TERMINAL
			? 2 : nonempty_count++;
		ClusterRelmapPendingSideSource *input = &f->sources[target];

		input_index[target] = i;
		input->source = item->source;
		input->cut.failed_thread = item->source.claim.identity.origin_thread_id;
		input->cut.origin_owner_incarnation = item->source.claim.identity.origin_owner_incarnation;
		input->cut.timeline_id = item->source.timeline;
		input->cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		if (item->kind == CLUSTER_WAL_INPUT_INITIALIZER_TERMINAL) {
			UT_ASSERT_EQ(item->terminal.tail.records, 0);
			UT_ASSERT(cluster_control_root_identity_equal(&item->source.claim.identity,
				&op.claim.identity));
			input->cut.scan_begin_inclusive = item->first_segment + SizeOfXLogLongPHD;
			input->cut.scan_end_exclusive = input->cut.scan_begin_inclusive;
			input->cut.flags |= RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY;
			f->empty = target;
			empty_count++;
		} else {
			UT_ASSERT_EQ(item->kind, CLUSTER_WAL_INPUT_CHECKPOINT);
			input->cut.scan_begin_inclusive = item->checkpoint.checkpoint_lower_lsn;
			input->cut.scan_end_exclusive = item->checkpoint.validated_tail_lsn_exclusive;
		}
		cuts[target] = input->cut;
		sources[target] = input->source;
		redo[target] = input->cut.scan_end_exclusive;
	}
	UT_ASSERT_EQ(empty_count, 1);
	UT_ASSERT_EQ(nonempty_count, 2);
	for (unsigned p = 0; p < (split ? lengthof(f->plans) : 2); p++) {
		RfSideOnlinePlanRequestV1 request = { 0 };
		unsigned first = split ? p : (p == 0 ? 0 : 2);
		unsigned count = split || p != 0 ? 1 : 2;

		request.system_identifier = TEST_SYSID;
		memcpy(request.storage_uuid, v2_storage, 16);
		request.physical_cuts = &cuts[first];
		request.redo_starts = &redo[first];
		request.participant_count = count;
		request.purpose = RF_SIDE_PLAN_RECOVERY;
		UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &f->plans[p]), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(rf_side_online_plan_bind_sources_v1(f->plans[p], &sources[first], count));
		for (unsigned j = 0; j < count; j++) {
			ClusterRelmapPendingSideSource *input = &f->sources[first + j];
			RelmapRetainedSideFeed feed = { input, j, 0, image };
			ClusterWalTailObservation observation;

			input->plan = f->plans[p];
			if (!(input->cut.flags & RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY))
				UT_ASSERT_EQ(cluster_wal_inputs_visit_retained_v1(inputs, input_index[first + j],
					relmap_retained_side_feed, &feed, &observation), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		}
		UT_ASSERT(rf_side_online_plan_bind_database_v1(f->plans[p], image->identity.database_incarnation));
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(f->plans[p]), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_space_target_count_v1(f->plans[p]), 0);
	}
	return inputs;
}

/* mode exercises both namespaces and shared/individual sealed plans. Refusal
 * either omits the real empty source or replaces it with a nonempty duplicate. */
static void
relmap_empty_initializer_rebuild(bool refuse)
{
	int saved_node = cluster_node_id;
	uint64 saved_incarnation = test_self_incarnation;

	for (unsigned mode = 0; mode < 4; mode++) {
		ClusterRelmapV2Image image = { 0 };
		RelmapEmptySideFixture side;
		ClusterWalInputsV1 *inputs = relmap_empty_initializer_fixture(&side, &image,
			mode < 2, mode % 2 != 0);
		RelmapCensusWalFixture f;
		ClusterRelmapPendingSet pending, before;
		ClusterRelmapPendingSideSource supplied[3];
		ClusterRelmapPendingDescriptor key = relmap_census_descriptor(&image);
		unsigned count = 0;

		if (ut_current_failed || inputs == NULL) goto cleanup;
		for (unsigned i = 0; i < lengthof(side.sources); i++)
			if (!refuse || i != side.empty) supplied[count++] = side.sources[i];
		if (refuse && mode >= 2) supplied[count++] = supplied[0];
		UT_ASSERT_EQ(cluster_relmap_owner_retained_side_wal_v2(&image, inputs,
			supplied, count), refuse ? CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH : CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(cluster_wal_inputs_revalidate_v1(inputs), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		relmap_census_begin(&f, &image, false);
		(void)relmap_census_emission(&f, image.identity.dbid);
		cluster_relmap_pending_init(&pending);
		before = pending;
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_side_wal(f.census,
			&pending, inputs, supplied, count), refuse ? CLUSTER_RELMAP_PENDING_NOT_READY
			: CLUSTER_RELMAP_PENDING_OK);
		if (refuse) {
			UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
			UT_ASSERT_EQ(cluster_relmap_retention_census_next(f.census), CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
		} else {
			UT_ASSERT(cluster_relmap_pending_contains(&pending, CLUSTER_RELMAP_PENDING_TRUNCATE, &key.identity));
			UT_ASSERT(pending.entries[0].main_placeholder && !pending.entries[0].copies_cleared_readback
				&& !pending.entries[0].native_terminal && !pending.entries[0].tt_complete
				&& !pending.entries[0].space_complete);
			UT_ASSERT(!cluster_relmap_pending_can_release(&pending, CLUSTER_RELMAP_PENDING_TRUNCATE, &key.identity));
			UT_ASSERT_EQ(relmap_census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
		}
		relmap_census_pin_retained(&f, &image, refuse);
		UT_ASSERT_EQ(inputs_peer_begins + inputs_native_begin_calls + inputs_native_flush_calls, 0);
		UT_ASSERT(inputs_pin_held && inputs_pin_current && test_actual_cf == NoLock);
		relmap_census_end(&f);
cleanup:
		for (unsigned i = 0; i < lengthof(side.plans); i++)
			rf_side_online_plan_destroy_v1(&side.plans[i]);
		cold_census_done(&inputs);
		test_reserve_mode = false;
		cluster_node_id = saved_node;
		test_self_incarnation = saved_incarnation;
		if (ut_current_failed) return;
	}
}

UT_TEST(test_census_side_wal_keeps_real_empty_initializer)
{
	relmap_empty_initializer_rebuild(false);
}

UT_TEST(test_census_side_wal_requires_real_empty_initializer)
{
	relmap_empty_initializer_rebuild(true);
}

UT_TEST(test_terminal_prefix_scan_requires_original_retained_owner)
{
	ClusterRelmapTerminalScanV1 *scan = (void *)1;
	UT_ASSERT(!cluster_relmap_recovery_terminal_prefix_scan_v2(
		NULL, 0, FullTransactionIdFromU64(99), &scan));
	UT_ASSERT(scan == NULL);
	cluster_relmap_recovery_terminal_scan_release_v1(&scan);
}
UT_TEST(test_terminal_scan_requires_original_retained_owner)
{
	ClusterRelmapTerminalScanV1 *scan = (void *)1;
	UT_ASSERT(
		!cluster_relmap_recovery_terminal_scan_v1(NULL, 0, FullTransactionIdFromU64(99), &scan));
	UT_ASSERT(scan == NULL);
	cluster_relmap_recovery_terminal_scan_release_v1(&scan);
	UT_ASSERT(scan == NULL);
}

/* Real native WAL bytes and original ROOT/anchor/claim/read scope. */
static WalTestRecord
terminal_test_record(const ClusterWalSourceRef *source, XLogRecPtr at, XLogRecPtr prev, RmgrId rmid,
					 uint8 info, TransactionId xid, const void *data, uint32 len)
{
	WalTestRecord result = relmap_test_append(source, at, prev, rmid, data, len);
	uint8 bytes[2048];
	XLogRecord record;
	char path[MAXPGPATH];
	int fd;
	v2_checkpoint_wal_path(&source->claim.identity, at, source->timeline, path);
	fd = open(path, O_RDWR);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pread(fd, &record, SizeOfXLogRecord, at % wal_segment_size), SizeOfXLogRecord);
	UT_ASSERT_EQ(pread(fd, bytes, record.xl_tot_len, at % wal_segment_size), record.xl_tot_len);
	record.xl_info = info;
	record.xl_xid = xid;
	INIT_CRC32C(record.xl_crc);
	COMP_CRC32C(record.xl_crc, bytes + SizeOfXLogRecord, record.xl_tot_len - SizeOfXLogRecord);
	COMP_CRC32C(record.xl_crc, &record, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(record.xl_crc);
	UT_ASSERT_EQ(pwrite(fd, &record, SizeOfXLogRecord, at % wal_segment_size), SizeOfXLogRecord);
	UT_ASSERT_EQ(close(fd), 0);
	result.record_crc = record.xl_crc;
	return result;
}

static ClusterWalInputsV1 *
terminal_test_fixture(unsigned selected, bool aborting, unsigned fault)
{
	uint8 bytes[66048];
	ClusterRecoveryAnchorV2 anchors[2];
	ControlRootImage root;
	ClusterRelmapCheckpointWindow window = { 0 };
	ClusterWalSourceRef source = { 0 };
	ClusterWalInputsV1 *inputs = NULL;
	TimestampTz timestamp = 123;
	WalTestRecord tail;
	ClusterControlRootSnapshot *snapshot;
	char path[MAXPGPATH];
	unsigned node = selected ? 127 : 0;
	inputs_closed_wal_fixture(bytes, anchors);
	UT_ASSERT_EQ(
		cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &root), 0);
	window.system_identifier = TEST_SYSID;
	window.database_incarnation = anchors[selected].database_incarnation;
	window.origin = node;
	window.admitted_incarnation = anchors[selected].identity.origin_owner_incarnation;
	window.boot_incarnation = 411;
	window.epoch = 97;
	window.native_floor = 80;
	window.last_issued = 99;
	window.allocation_next = 100;
	UT_ASSERT(cluster_relmap_checkpoint_window_encode_v1(
		&window, &anchors[selected].checkpoint_copy, anchors[selected].checkpoint,
		root.records[node].validated_tail_lsn_exclusive + (fault == 2),
		anchors[selected].native_window));
	if (fault == 1)
		memset(anchors[selected].native_window, 0, 160);
	v2_anchor_object(bytes, &anchors[selected], &anchors[selected].identity, path);
	UT_ASSERT_EQ(
		cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &root), 0);
	snapshot = &root.records[node];
	source.claim.identity = snapshot->identity;
	source.claim.database_incarnation = root.header.v2.database_incarnation;
	source.claim.max_config_generation = root.header.v2.config_generation;
	memcpy(source.claim.claim_sha256, root.refs[node].claim_sha256, 32);
	source.timeline = snapshot->tail_tli;
	tail = terminal_test_record(&source, snapshot->validated_tail_lsn_exclusive,
								snapshot->tail_last_record_lsn, RM_XACT_ID,
								aborting ? XLOG_XACT_ABORT : XLOG_XACT_COMMIT, fault == 3 ? 98 : 99,
								&timestamp, fault == 4 ? 1 : sizeof(timestamp));
	if (fault == 5 || fault == 6)
		tail = terminal_test_record(&source, tail.exclusive_end, tail.record_start, RM_XACT_ID,
									fault == 5 ? XLOG_XACT_ABORT : XLOG_XACT_COMMIT, 99, &timestamp,
									sizeof(timestamp));
	if (fault == 10) {
		uint8 payload[sizeof(timestamp) + sizeof(xl_xact_xinfo) + MinSizeOfXactSubxacts
					  + sizeof(TransactionId)];
		xl_xact_xinfo xinfo = { XACT_XINFO_HAS_SUBXACTS };
		int nsubs = 1;
		TransactionId child = 99;
		memcpy(payload, &timestamp, sizeof(timestamp));
		memcpy(payload + sizeof(timestamp), &xinfo, sizeof(xinfo));
		memcpy(payload + sizeof(timestamp) + sizeof(xinfo), &nsubs, sizeof(nsubs));
		memcpy(payload + sizeof(timestamp) + sizeof(xinfo) + MinSizeOfXactSubxacts, &child,
			   sizeof(child));
		tail = terminal_test_record(&source, tail.exclusive_end, tail.record_start, RM_XACT_ID,
									XLOG_XACT_ABORT | XLOG_XACT_HAS_INFO, 98, payload,
									sizeof(payload));
	}
	if (fault == 11 || fault == 12)
		tail = terminal_test_record(&source, tail.exclusive_end, tail.record_start, RM_XACT_ID,
									fault == 11 ? XLOG_XACT_PREPARE : XLOG_XACT_COMMIT, 98,
									&timestamp, fault == 12 ? 1 : sizeof(timestamp));
	tail = terminal_test_record(&source, tail.exclusive_end, tail.record_start, RM_XLOG_ID,
								XLOG_NOOP, fault == 7 ? 99 : 0, "abc", 3);
	if (fault == 8)
		snapshot->lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	snapshot->tail_last_record_lsn = tail.record_start;
	snapshot->tail_last_record_crc32c = tail.record_crc;
	snapshot->validated_tail_lsn_exclusive = tail.exclusive_end;
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0);
	v2_write_roots(root.bytes);
	if (fault == 9) {
		int fd;
		uint8 b;
		v2_checkpoint_wal_path(&source.claim.identity, tail.record_start, source.timeline, path);
		fd = open(path, O_RDWR);
		UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(pread(fd, &b, 1, tail.record_start % wal_segment_size + SizeOfXLogRecord + 2),
					 1);
		b ^= 1;
		UT_ASSERT_EQ(pwrite(fd, &b, 1, tail.record_start % wal_segment_size + SizeOfXLogRecord + 2),
					 1);
		UT_ASSERT_EQ(close(fd), 0);
	}
	test_inputs_startup_cf = true;
	UT_ASSERT_EQ(inputs_begin_for_role(true, &inputs), 0);
	return inputs;
}

static XLogRecPtr prefix_test_checkpoint_end;
static ClusterRelmapV2Image custody_image;
static bool custody_shared;
static uint64 custody_scope = 1;
static unsigned custody_scope_reads, custody_scope_error_after;
static unsigned origin_build_scope_change_after;
static void (*origin_build_recheck_hook)(void);
static unsigned custody_terminal_calls, custody_terminal_fault;
static unsigned custody_inventory_fault, custody_inventory_calls, custody_inventory_expected = 1;
static bool custody_extra_terminal;
static void
custody_image_init(const ClusterControlRootSnapshot *snapshot, uint64 dbinc)
{
	memset(&custody_image, 0, sizeof(custody_image));
	custody_image.identity
		= (ClusterRelmapV2Namespace){ TEST_SYSID, dbinc, custody_shared ? InvalidOid : 42,
									  custody_shared };
	custody_image.committed_generation = 7;
	custody_image.pending_generation = 8;
	custody_image.pending_owner
		= (ClusterRelmapV2Owner){ 0, 99, 97, snapshot->identity.origin_owner_incarnation, 0, 411 };
	custody_image.committed.magic = RELMAPPER_FILEMAGIC;
	custody_image.committed.num_mappings = 1;
	custody_image.committed.mappings[0].mapoid = 1259;
	custody_image.committed.mappings[0].mapfilenumber = 40000;
	relmap_test_crc(&custody_image.committed);
	custody_image.pending = custody_image.committed;
	custody_image.pending.mappings[0].mapfilenumber++;
	relmap_test_crc(&custody_image.pending);
}
static uint64 custody_era_offset, custody_next_override;
static ClusterWalInputsV1 *
terminal_prefix_fixture(bool aborting, unsigned fault)
{
	uint8 bytes[66048];
	ClusterRecoveryAnchorV2 anchors[2];
	ClusterRecoveryAnchorV2 *a = &anchors[0];
	ClusterRelmapCheckpointWindow window = { 0 };
	ClusterWalSourceRef source = { 0 };
	ControlRootImage root;
	ClusterControlRootSnapshot *snapshot;
	ControlFileData native = { 0 };
	ClusterWalInputsV1 *inputs = NULL;
	TimestampTz timestamp = 123;
	uint8 native_terminal[sizeof(xl_xact_commit) + sizeof(xl_xact_xinfo)
		+ sizeof(xl_xact_scn) + sizeof(xl_xact_tt_commit)];
	const void *terminal_data = &timestamp;
	size_t terminal_bytes = sizeof(timestamp);
	uint8 terminal_info = aborting ? XLOG_XACT_ABORT : XLOG_XACT_COMMIT;
	WalTestRecord terminal, checkpoint, tail;
	XLogRecPtr lower, at;
	char path[MAXPGPATH];
	inputs_closed_wal_fixture(bytes, anchors);
	if (cold_native_real_scope) {
		terminal_bytes = cold_original_terminal_payload(aborting, a->identity.origin_thread_id,
			native_terminal);
		terminal_data = native_terminal;
		terminal_info |= XLOG_XACT_HAS_INFO;
	}
	a->checkpoint_copy.nextXid = FullTransactionIdFromU64(custody_next_override != 0
		? custody_next_override
		: U64FromFullTransactionId(a->checkpoint_copy.nextXid) + custody_era_offset);
	UT_ASSERT_EQ(
		cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &root), 0);
	snapshot = &root.records[0];
	lower = snapshot->checkpoint_lower_lsn;
	at = lower + MAXALIGN(SizeOfXLogRecord + 2 + terminal_bytes);
	if (fault >= 29 && fault <= 32)
		at += MAXALIGN(SizeOfXLogRecord + 5 + MinSizeOfRelmapUpdate + sizeof(RelMapFile));
	if (fault == 4 || fault == 5 || fault == 6)
		at += MAXALIGN(SizeOfXLogRecord + 2 + sizeof(timestamp));
	if (fault == 14 || fault == 27)
		at += MAXALIGN(SizeOfXLogRecord + 2 + sizeof(int));
	if (fault == 28)
		at += MAXALIGN(SizeOfXLogRecord + 2 + sizeof(xl_clog_truncate));
	a->state = DB_IN_PRODUCTION;
	a->checkpoint = at;
	a->checkpoint_copy.redo
		= fault == 6 ? lower + MAXALIGN(SizeOfXLogRecord + 2 + sizeof(timestamp)) : lower;
	native.state = a->state;
	native.checkPoint = a->checkpoint;
	native.checkPointCopy = a->checkpoint_copy;
	snprintf(test_checkpoint_prefix_path, sizeof(test_checkpoint_prefix_path),
			 "%s/thread_%u/generation_" UINT64_FORMAT "/durable_prefix/current",
			 cluster_wal_threads_dir, a->identity.origin_thread_id,
			 a->identity.origin_owner_incarnation);
	v2_checkpoint_wal_record(&a->identity, &native, 0);
	prefix_test_checkpoint_end = test_checkpoint_end;
	source.claim.identity = snapshot->identity;
	source.claim.database_incarnation = root.header.v2.database_incarnation;
	source.claim.max_config_generation = root.header.v2.config_generation;
	memcpy(source.claim.claim_sha256, root.refs[0].claim_sha256, 32);
	source.timeline = snapshot->tail_tli;
	if (fault >= 29 && fault <= 32) {
		uint8 payload[MinSizeOfRelmapUpdate + sizeof(RelMapFile)];
		xl_relmap_update update = { 0 };
		WalTestRecord pending;
		custody_image_init(snapshot, root.header.v2.database_incarnation);
		custody_image.pending_owner.top_full_xid += custody_era_offset;
		update.dbid = custody_image.identity.dbid;
		update.tsid = custody_shared ? GLOBALTABLESPACE_OID : DEFAULTTABLESPACE_OID;
		update.nbytes = sizeof(RelMapFile);
		memcpy(payload, &update, MinSizeOfRelmapUpdate);
		memcpy(payload + MinSizeOfRelmapUpdate, &custody_image.pending, sizeof(RelMapFile));
		pending = terminal_test_record(&source, lower, 0, RM_RELMAP_ID, XLOG_RELMAP_UPDATE, 99,
									   payload, sizeof(payload));
		custody_image.pending_owner.wal_end = pending.exclusive_end;
		terminal = terminal_test_record(&source, pending.exclusive_end, pending.record_start,
										RM_XACT_ID, terminal_info, 99, terminal_data, terminal_bytes);
	} else if (fault == 14 || fault == 28) {
		int page = 0;
		xl_clog_truncate trunc = { .pageno = 0, .oldestXact = 80, .oldestXactDb = 123 };
		WalTestRecord before = terminal_test_record(
			&source, lower, 0, RM_CLOG_ID, fault == 14 ? CLOG_ZEROPAGE : CLOG_TRUNCATE, 0,
			fault == 14 ? (const void *)&page : (const void *)&trunc,
			fault == 14 ? sizeof(page) : sizeof(trunc));
		terminal = terminal_test_record(&source, before.exclusive_end, before.record_start,
										RM_XACT_ID, aborting ? XLOG_XACT_ABORT : XLOG_XACT_COMMIT,
										99, &timestamp, sizeof(timestamp));
	} else
		terminal = terminal_test_record(
			&source, lower, 0, RM_XACT_ID, aborting ? XLOG_XACT_ABORT : XLOG_XACT_COMMIT,
			fault == 2 || fault == 3 ? 98 : 99, &timestamp, sizeof(timestamp));
	if (fault == 4 || fault == 5 || fault == 6)
		terminal = terminal_test_record(&source, terminal.exclusive_end, terminal.record_start,
										fault == 6 ? RM_XLOG_ID : RM_XACT_ID,
										fault == 6	 ? XLOG_NOOP
										: fault == 4 ? XLOG_XACT_ABORT
													 : XLOG_XACT_COMMIT,
										fault == 6 ? 0 : 99, &timestamp, sizeof(timestamp));
	if (fault == 27) {
		int page = 0;
		terminal = terminal_test_record(&source, terminal.exclusive_end, terminal.record_start,
										RM_CLOG_ID, CLOG_ZEROPAGE, 0, &page, sizeof(page));
	}
	checkpoint = terminal_test_record(&source, a->checkpoint, terminal.record_start, RM_XLOG_ID,
									  XLOG_CHECKPOINT_ONLINE, 0, &a->checkpoint_copy,
									  sizeof(a->checkpoint_copy));
	UT_ASSERT_EQ(checkpoint.exclusive_end, prefix_test_checkpoint_end);
	tail = checkpoint;
	if (fault == 31 || fault == 32) {
		int page = 0;
		xl_clog_truncate trunc = { .pageno = 0, .oldestXact = 80, .oldestXactDb = 123 };
		tail = terminal_test_record(&source, tail.exclusive_end, tail.record_start, RM_CLOG_ID,
									fault == 31 ? CLOG_ZEROPAGE : CLOG_TRUNCATE, 0,
									fault == 31 ? (const void *)&page : (const void *)&trunc,
									fault == 31 ? sizeof(page) : sizeof(trunc));
	}
	if (fault == 30)
		tail = terminal_test_record(&source, tail.exclusive_end, tail.record_start, RM_XACT_ID,
									XLOG_XACT_COMMIT, 98, &timestamp, sizeof(timestamp));
	if (fault == 2)
		tail = terminal_test_record(&source, tail.exclusive_end, tail.record_start, RM_XACT_ID,
									XLOG_XACT_COMMIT, 99, &timestamp, sizeof(timestamp));
	if (fault == 8) {
		uint8 payload[sizeof(timestamp) + sizeof(xl_xact_xinfo) + MinSizeOfXactSubxacts
					  + sizeof(TransactionId)];
		xl_xact_xinfo info = { XACT_XINFO_HAS_SUBXACTS };
		int nsubs = 1;
		TransactionId child = 99;
		memcpy(payload, &timestamp, sizeof(timestamp));
		memcpy(payload + sizeof(timestamp), &info, sizeof(info));
		memcpy(payload + sizeof(timestamp) + sizeof(info), &nsubs, sizeof(nsubs));
		memcpy(payload + sizeof(timestamp) + sizeof(info) + MinSizeOfXactSubxacts, &child,
			   sizeof(child));
		tail = terminal_test_record(&source, tail.exclusive_end, tail.record_start, RM_XACT_ID,
									XLOG_XACT_ABORT | XLOG_XACT_HAS_INFO, 98, payload,
									sizeof(payload));
	}
	if (fault == 11)
		tail = terminal_test_record(&source, tail.exclusive_end, tail.record_start, RM_XACT_ID,
									XLOG_XACT_PREPARE, 98, &timestamp, sizeof(timestamp));
	if (fault >= 12 && fault <= 26 && fault != 14) {
		int page = fault == 13 ? 1 : fault == 18 ? -1 : fault == 19 ? 131072 : 0;
		xl_clog_truncate trunc = { .pageno = fault == 22 ? 1 : 0,
								   .oldestXact = fault == 15   ? 100
												 : fault == 21 ? 2
															   : 80,
								   .oldestXactDb = 123 };
		bool truncate = fault == 15 || fault == 16 || (fault >= 20 && fault <= 22);
		uint8 info = fault == 23   ? 0x20
					 : fault == 26 ? 1
					 : truncate	   ? CLOG_TRUNCATE
								   : CLOG_ZEROPAGE;
		tail = terminal_test_record(
			&source, tail.exclusive_end, tail.record_start, RM_CLOG_ID, info, fault == 24 ? 98 : 0,
			truncate || fault == 25 ? (const void *)&trunc : (const void *)&page,
			fault == 25 ? sizeof(trunc)
			: truncate	? sizeof(trunc) - (fault == 20)
						: sizeof(page) - (fault == 17));
	}
	tail = terminal_test_record(&source, tail.exclusive_end, tail.record_start, RM_XLOG_ID,
								XLOG_NOOP, fault == 7 ? 99 : 0, "abc", 3);
	window.system_identifier = TEST_SYSID;
	window.database_incarnation = a->database_incarnation;
	window.origin = 0;
	window.admitted_incarnation = a->identity.origin_owner_incarnation;
	window.boot_incarnation = 411;
	window.epoch = 97;
	window.native_floor = custody_era_offset + 80;
	window.last_issued = custody_era_offset + 99;
	window.allocation_next = custody_era_offset + 100;
	if (fault == 1)
		UT_ASSERT(
			cluster_relmap_checkpoint_window_encode_v1(&window, &a->checkpoint_copy, a->checkpoint,
													   checkpoint.exclusive_end, a->native_window));
	else
		UT_ASSERT(cluster_relmap_checkpoint_window_encode_v2(
			&window, &a->checkpoint_copy, a->checkpoint, checkpoint.exclusive_end + (fault == 10),
			custody_era_offset + 105, a->native_window));
	v2_anchor_object(bytes, a, &a->identity, path);
	UT_ASSERT_EQ(
		cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &root), 0);
	snapshot = &root.records[0];
	/* An online checkpoint is a recovery input, never a clean CLOSED vote. */
	snapshot->lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	snapshot->checkpoint_record_crc32c = checkpoint.record_crc;
	snapshot->tail_last_record_lsn = tail.record_start;
	snapshot->tail_last_record_crc32c = tail.record_crc;
	snapshot->validated_tail_lsn_exclusive = tail.exclusive_end;
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0);
	v2_write_roots(root.bytes);
	if (fault == 9) {
		int fd;
		uint8 b;
		v2_checkpoint_wal_path(&source.claim.identity, tail.record_start, source.timeline, path);
		fd = open(path, O_RDWR);
		UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(pread(fd, &b, 1, tail.record_start % wal_segment_size + SizeOfXLogRecord + 2),
					 1);
		b ^= 1;
		UT_ASSERT_EQ(pwrite(fd, &b, 1, tail.record_start % wal_segment_size + SizeOfXLogRecord + 2),
					 1);
		UT_ASSERT_EQ(close(fd), 0);
	}
	test_inputs_startup_cf = true;
	UT_ASSERT_EQ(inputs_begin_for_role(true, &inputs), 0);
	return inputs;
}
/* Native disk I/O and the cold owner are controlled leaf boundaries here.
 * Actual owner/source matching and actual CLOG disk reads have separate tests. */
static ClusterWalInputV1 prefix_base_expected;
static unsigned prefix_base_fault, prefix_base_reads, prefix_base_scope_checks;
static XidStatus prefix_base_status;
static uint8 prefix_base_last_byte;

bool
cluster_undo_recovery_cold_source_matches_v1(const ClusterWalSourceRef *source,
											 const ClusterControlRootSnapshot *checkpoint,
											 XLogRecPtr native_redo)
{
	if (cold_native_real_scope)
		return test_relmap_scope_source_matches(source, checkpoint, native_redo);
	prefix_base_scope_checks++;
	return prefix_base_fault != 1 && !(prefix_base_fault == 9 && prefix_base_scope_checks > 1)
		   && memcmp(source, &prefix_base_expected.source, sizeof(*source)) == 0
		   && memcmp(checkpoint, &prefix_base_expected.checkpoint, sizeof(*checkpoint)) == 0
		   && native_redo == prefix_base_expected.native_redo;
}

uint64
cluster_undo_recovery_cold_scope_id_v1(const ClusterWalSourceRef *source,
									   const ClusterControlRootSnapshot *checkpoint,
									   XLogRecPtr native_redo)
{
	if (cold_native_real_scope)
		return test_relmap_scope_id(source, checkpoint, native_redo);
	if (++custody_scope_reads == custody_scope_error_after)
		ereport(ERROR, (errmsg("injected original scope identity ERROR")));
	if (custody_scope_reads == origin_build_scope_change_after)
		custody_scope++;
	if (custody_scope_reads == 2 && origin_build_recheck_hook != NULL)
		origin_build_recheck_hook();
	return cluster_undo_recovery_cold_source_matches_v1(source, checkpoint, native_redo)
			   ? custody_scope
			   : 0;
}

uint32
cluster_undo_recovery_cold_terminal_count_v1(const ClusterWalSourceRef *source,
											 const ClusterControlRootSnapshot *checkpoint,
											 XLogRecPtr redo, uint64 serial)
{
	if (cold_native_real_scope)
		return test_relmap_scope_terminal_count(source, checkpoint, redo, serial);
	custody_inventory_calls++;
	if (custody_inventory_fault == 8)
		ereport(ERROR, (errmsg("injected original inventory ERROR")));
	if (serial != custody_scope || serial == 0
		|| memcmp(source, &prefix_base_expected.source, sizeof(*source)) != 0
		|| memcmp(checkpoint, &prefix_base_expected.checkpoint, sizeof(*checkpoint)) != 0
		|| redo != prefix_base_expected.native_redo || custody_inventory_fault == 1)
		return UINT32_MAX;
	if (custody_inventory_fault == 2)
		return 0;
	if (custody_inventory_fault == 3
		|| (custody_inventory_fault == 5 && custody_inventory_calls > 1))
		return custody_inventory_expected + 1;
	if (custody_inventory_fault == 4)
		return custody_inventory_expected - 1;
	return custody_inventory_expected;
}

bool
cluster_undo_recovery_cold_terminal_matches_v1(const ClusterWalSourceRef *source,
											   const ClusterControlRootSnapshot *checkpoint,
											   XLogRecPtr redo, uint64 serial,
											   const XLogReaderState *reader)
{
	TransactionId xid = reader != NULL ? XLogRecGetXid(reader) : 0;
	if (cold_native_real_scope)
		return test_relmap_scope_terminal_matches(source, checkpoint, redo, serial, reader);
	custody_terminal_calls++;
	if (custody_terminal_fault == 2 || (custody_inventory_fault == 7 && xid == 98))
		ereport(ERROR, (errmsg("injected original terminal source ERROR")));
	return custody_terminal_fault == 0 && !(custody_inventory_fault == 6 && xid == 98)
		   && serial == custody_scope && serial != 0
		   && memcmp(source, &prefix_base_expected.source, sizeof(*source)) == 0
		   && memcmp(checkpoint, &prefix_base_expected.checkpoint, sizeof(*checkpoint)) == 0
		   && redo == prefix_base_expected.native_redo && reader != NULL
		   && reader->ReadRecPtr >= redo
		   && reader->EndRecPtr
				  <= (xid == 99 ? prefix_base_expected.checkpoint_start
								: prefix_base_expected.checkpoint.validated_tail_lsn_exclusive)
		   && XLogRecGetRmid(reader) == RM_XACT_ID
		   && (xid == 99 || (custody_extra_terminal && xid == 98));
}

bool
cluster_clog_native_recovery_base_read_v1(int origin, TransactionId xid,
										  ClusterClogNativeBaseV1 *out)
{
	prefix_base_reads++;
	if (cold_native_real_clog)
		return test_relmap_native_clog_read(origin, xid, out, PG_exception_stack);
	memset(out, 0, sizeof(*out));
	if (origin == cluster_node_id || prefix_base_fault == 2)
		return false;
	out->origin = prefix_base_fault == 3 ? origin + 1 : origin;
	out->xid = prefix_base_fault == 4 ? xid + 1 : xid;
	out->status = prefix_base_fault == 5	? TRANSACTION_STATUS_IN_PROGRESS
				  : prefix_base_fault == 14 ? TRANSACTION_STATUS_ABORTED
											: prefix_base_status;
	out->page_number = xid / (BLCKSZ * 4) + (prefix_base_fault == 10);
	memset(out->image, 0x42, sizeof(out->image));
	/* Model the original native reader's status/image agreement. */
	out->image[(xid / 4) % BLCKSZ] &= ~(3 << ((xid % 4) * 2));
	out->image[(xid / 4) % BLCKSZ] |= prefix_base_status << ((xid % 4) * 2);
	out->image[BLCKSZ - 1] = prefix_base_last_byte;
	if (prefix_base_fault == 6)
		inputs_pin_current = false;
	if (prefix_base_fault == 7)
		v2_checkpoint_root_race();
	if (prefix_base_fault == 8)
		ereport(ERROR, (errmsg("injected native base ERROR")));
	return true;
}

static ClusterRelmapTerminalScanV1 *
prefix_base_setup_fault(ClusterWalInputsV1 **inputs, bool aborted, bool prefix,
						unsigned source_fault)
{
	ClusterRelmapTerminalScanV1 *scan = NULL;
	*inputs = prefix ? terminal_prefix_fixture(aborted, source_fault)
					 : terminal_test_fixture(0, aborted, 0);
	prefix_base_expected = *cluster_wal_inputs_at_v1(*inputs, 0);
	prefix_base_fault = prefix_base_reads = prefix_base_scope_checks = 0;
	prefix_base_status = aborted ? TRANSACTION_STATUS_ABORTED : TRANSACTION_STATUS_COMMITTED;
	prefix_base_last_byte = 0x24;
	if (!(prefix ? cluster_relmap_recovery_terminal_prefix_scan_v2(
					   *inputs, 0, FullTransactionIdFromU64(99), &scan)
				 : cluster_relmap_recovery_terminal_scan_v1(*inputs, 0,
															FullTransactionIdFromU64(99), &scan)))
		abort();
	return scan;
}

static ClusterRelmapTerminalScanV1 *
prefix_base_setup(ClusterWalInputsV1 **inputs, bool aborted, bool prefix)
{
	return prefix_base_setup_fault(inputs, aborted, prefix, 0);
}

UT_TEST(test_terminal_native_base_retains_only_undestroyed_original_page)
{
	const unsigned cases[] = { 12, 13, 14, 15, 16, 27, 28 };
	int saved = cluster_node_id;
	cluster_node_id = 1;
	for (unsigned i = 0; i < lengthof(cases); i++) {
		unsigned fault = cases[i];
		bool allowed = fault == 13 || fault == 14 || fault == 28;
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRelmapTerminalScanV1 *scan = prefix_base_setup_fault(&inputs, i % 2, true, fault);
		ClusterRelmapTerminalBaseObservationV1 out, zero = { 0 };
		ClusterRelmapTerminalObservationV1 terminal;
		/* A valid terminal observation is still available before requesting
		 * a native base; page lifetime is an additional independent gate. */
		UT_ASSERT(cluster_relmap_recovery_terminal_scan_get_v1(scan, &terminal));
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT_EQ(cluster_relmap_recovery_terminal_prefix_base_read_v1(scan, &out), allowed);
		UT_ASSERT_EQ(prefix_base_reads, allowed ? 1 : 0);
		if (allowed) {
			UT_ASSERT_EQ(out.terminal.requested_top_full_xid, 99);
			UT_ASSERT_EQ(out.native_base.status, prefix_base_status);
			UT_ASSERT(cluster_relmap_recovery_terminal_scan_get_v1(scan, &terminal));
		} else {
			UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
			UT_ASSERT(!cluster_relmap_recovery_terminal_prefix_base_read_v1(scan, &out));
			UT_ASSERT(!cluster_relmap_recovery_terminal_scan_get_v1(scan, &terminal));
		}
		cluster_relmap_recovery_terminal_scan_release_v1(&scan);
		UT_ASSERT(inputs_pin_held);
		cold_census_done(&inputs);
	}
	cluster_node_id = saved;
}

UT_TEST(test_terminal_native_base_refuses_unknown_or_malformed_clog_carriers)
{
	const unsigned cases[] = { 17, 18, 19, 20, 21, 22, 23, 24, 25, 26 };
	int saved = cluster_node_id;
	cluster_node_id = 1;
	for (unsigned i = 0; i < lengthof(cases); i++) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRelmapTerminalScanV1 *scan = prefix_base_setup_fault(&inputs, false, true, cases[i]);
		ClusterRelmapTerminalBaseObservationV1 out, zero = { 0 };
		ClusterRelmapTerminalObservationV1 terminal;
		UT_ASSERT(cluster_relmap_recovery_terminal_scan_get_v1(scan, &terminal));
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT(!cluster_relmap_recovery_terminal_prefix_base_read_v1(scan, &out));
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0 && prefix_base_reads == 0);
		UT_ASSERT(!cluster_relmap_recovery_terminal_scan_get_v1(scan, &terminal));
		cluster_relmap_recovery_terminal_scan_release_v1(&scan);
		UT_ASSERT(inputs_pin_held);
		cold_census_done(&inputs);
	}
	cluster_node_id = saved;
}

UT_TEST(test_terminal_prefix_native_base_binds_full_image_and_source)
{
	int saved = cluster_node_id;
	cluster_node_id = 1;
	for (unsigned aborted = 0; aborted < 2; aborted++) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRelmapTerminalScanV1 *scan = prefix_base_setup(&inputs, aborted, true);
		ClusterRelmapTerminalBaseObservationV1 out, again;
		uint8 transcript[sizeof("PGRAC-RELMAP-PREFIX-NATIVE-BASE-V1") + 32 + 13 + BLCKSZ];
		uint8 expected[32];
		size_t at = 0;
		UT_ASSERT(!cluster_relmap_recovery_terminal_prefix_base_read_v1(scan, (void *)scan));
		UT_ASSERT(cluster_relmap_recovery_terminal_prefix_base_read_v1(scan, &out));
		UT_ASSERT_EQ(prefix_base_reads, 1);
		UT_ASSERT_EQ(prefix_base_scope_checks, 2);
		UT_ASSERT_EQ(out.terminal.requested_top_full_xid, 99);
		UT_ASSERT_EQ(out.terminal.post_flush_issued, 105);
		UT_ASSERT_EQ(out.native_base.origin, 0);
		UT_ASSERT_EQ(out.native_base.xid, 99);
		UT_ASSERT_EQ(out.native_base.page_number, 0);
		UT_ASSERT_EQ(out.native_base.status, out.terminal.status);
		memcpy(transcript, "PGRAC-RELMAP-PREFIX-NATIVE-BASE-V1",
			   sizeof("PGRAC-RELMAP-PREFIX-NATIVE-BASE-V1"));
		at += sizeof("PGRAC-RELMAP-PREFIX-NATIVE-BASE-V1");
		memcpy(transcript + at, out.terminal.fingerprint, 32);
		at += 32;
		memset(transcript + at, 0, 13);
		transcript[at + 4] = 99;
		transcript[at + 12] = out.terminal.status;
		at += 13;
		memcpy(transcript + at, out.native_base.image, BLCKSZ);
		at += BLCKSZ;
		sha256_bytes(transcript, at, expected);
		UT_ASSERT(memcmp(out.fingerprint, expected, 32) == 0);
		prefix_base_last_byte ^= 1;
		UT_ASSERT(cluster_relmap_recovery_terminal_prefix_base_read_v1(scan, &again));
		UT_ASSERT(memcmp(out.terminal.fingerprint, again.terminal.fingerprint, 32) == 0);
		UT_ASSERT(memcmp(out.fingerprint, again.fingerprint, 32) != 0);
		cluster_relmap_recovery_terminal_scan_release_v1(&scan);
		UT_ASSERT(inputs_pin_held);
		cold_census_done(&inputs);
	}
	cluster_node_id = saved;
}

UT_TEST(test_terminal_prefix_native_base_rejects_scope_and_native_mismatch_sticky)
{
	int saved = cluster_node_id;
	cluster_node_id = 1;
	for (unsigned fault = 1; fault <= 13; fault++) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRelmapTerminalScanV1 *scan = prefix_base_setup(&inputs, false, fault != 11);
		ClusterRelmapTerminalBaseObservationV1 out, zero = { 0 };
		ClusterRelmapTerminalObservationV1 terminal;
		prefix_base_fault = fault;
		if (fault == 8)
			prefix_base_fault = 14; /* Opposite terminal, ERROR tested separately. */
		if (fault == 12)
			inputs_pin_current = false;
		if (fault == 13)
			cluster_node_id = 0; /* Original reader rejects own origin. */
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT(!cluster_relmap_recovery_terminal_prefix_base_read_v1(scan, &out));
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		if (fault == 1 || fault == 11 || fault == 12)
			UT_ASSERT_EQ(prefix_base_reads, 0);
		prefix_base_fault = 0;
		inputs_pin_current = true;
		cluster_node_id = 1;
		UT_ASSERT(!cluster_relmap_recovery_terminal_prefix_base_read_v1(scan, &out));
		UT_ASSERT(!cluster_relmap_recovery_terminal_scan_get_v1(scan, &terminal));
		cluster_relmap_recovery_terminal_scan_release_v1(&scan);
		UT_ASSERT(inputs_pin_held);
		cold_census_done(&inputs);
	}
	cluster_node_id = saved;
}

UT_TEST(test_terminal_prefix_native_base_error_keeps_output_and_original_pins)
{
	int saved = cluster_node_id;
	cluster_node_id = 1;
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRelmapTerminalScanV1 *scan = prefix_base_setup(&inputs, false, true);
		ClusterRelmapTerminalBaseObservationV1 out, zero = { 0 };
		volatile bool threw = false;
		prefix_base_fault = fault == 1 ? 8 : 0;
		test_capture_error_level = fault == 1;
		test_last_error_level = 0;
		test_throw_root_read = fault == 0;
		if (fault == 2)
			CritSectionCount++;
		memset(&out, 0xa5, sizeof(out));
		PG_TRY();
		{
			UT_ASSERT(!cluster_relmap_recovery_terminal_prefix_base_read_v1(scan, &out));
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		if (fault == 2)
			CritSectionCount--;
		test_capture_error_level = false;
		test_throw_root_read = false;
		UT_ASSERT_EQ(threw, fault != 2);
		if (fault == 1)
			UT_ASSERT_EQ(test_last_error_level, ERROR);
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		UT_ASSERT(inputs_pin_held && test_actual_cf == NoLock);
		prefix_base_fault = 0;
		UT_ASSERT(!cluster_relmap_recovery_terminal_prefix_base_read_v1(scan, &out));
		cluster_relmap_recovery_terminal_scan_release_v1(&scan);
		cold_census_done(&inputs);
	}
	cluster_node_id = saved;
}

UT_TEST(test_terminal_prefix_scan_binds_actual_checkpoint_end_and_issued_cap)
{
	for (unsigned observer = 0; observer < 2; observer++)
		for (unsigned aborted = 0; aborted < 2; aborted++) {
			int saved = cluster_node_id;
			ClusterWalInputsV1 *inputs;
			ClusterRelmapTerminalScanV1 *scan = NULL;
			ClusterRelmapTerminalObservationV1 out;
			bool accepted;
			cluster_node_id = observer;
			inputs = terminal_prefix_fixture(aborted, 0);
			accepted = cluster_relmap_recovery_terminal_prefix_scan_v2(
				inputs, 0, FullTransactionIdFromU64(99), &scan);
			if (!accepted) {
				cold_census_done(&inputs);
				cluster_node_id = saved;
			}
			UT_ASSERT(accepted);
			if (!accepted)
				return;
			UT_ASSERT(cluster_relmap_recovery_terminal_scan_get_v1(scan, &out));
			UT_ASSERT_EQ(out.status,
						 aborted ? TRANSACTION_STATUS_ABORTED : TRANSACTION_STATUS_COMMITTED);
			UT_ASSERT_EQ(out.requested_top_full_xid, 99);
			UT_ASSERT_EQ(out.post_flush_issued, 105);
			UT_ASSERT_EQ(out.prefix_end, prefix_test_checkpoint_end);
			UT_ASSERT(out.terminal_start >= cluster_wal_inputs_at_v1(inputs, 0)->native_redo);
			UT_ASSERT(out.terminal_end <= cluster_wal_inputs_at_v1(inputs, 0)->checkpoint_start);
			UT_ASSERT(out.prefix_end < out.cut && out.records == 3);
			UT_ASSERT(!v2_zero(out.fingerprint, 32));
			cluster_relmap_recovery_terminal_scan_release_v1(&scan);
			UT_ASSERT(inputs_pin_held);
			cold_census_done(&inputs);
			cluster_node_id = saved;
		}
}
UT_TEST(test_terminal_prefix_scan_refuses_unbounded_or_nonmatching_sources)
{
	for (unsigned fault = 1; fault <= 11; fault++) {
		ClusterWalInputsV1 *inputs = terminal_prefix_fixture(false, fault);
		ClusterRelmapTerminalScanV1 *scan = (void *)1;
		if (fault == 2) {
			/* Check the old mode before a refused visit invalidates this scope. */
			UT_ASSERT(cluster_relmap_recovery_terminal_scan_v1(
				inputs, 0, FullTransactionIdFromU64(99), &scan));
			cluster_relmap_recovery_terminal_scan_release_v1(&scan);
			scan = (void *)1;
		}
		UT_ASSERT(!cluster_relmap_recovery_terminal_prefix_scan_v2(
			inputs, 0, FullTransactionIdFromU64(99), &scan));
		UT_ASSERT(scan == NULL && inputs_pin_held);
		cold_census_done(&inputs);
	}
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterWalInputsV1 *inputs = terminal_prefix_fixture(false, 0);
		ClusterRelmapTerminalScanV1 *scan = (void *)1;
		uint64 full = fault == 0   ? 79
					  : fault == 1 ? 100
					  : fault == 2 ? UINT64CONST(0x100000063)
								   : 99;
		UT_ASSERT(!cluster_relmap_recovery_terminal_prefix_scan_v2(
			inputs, fault == 3 ? 2 : 0, FullTransactionIdFromU64(full), &scan));
		UT_ASSERT(scan == NULL && inputs_pin_held);
		cold_census_done(&inputs);
	}
}
UT_TEST(test_terminal_prefix_scan_getter_invalidates_cap_and_prefix_together)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		ClusterWalInputsV1 *inputs = terminal_prefix_fixture(false, 0);
		ClusterRelmapTerminalScanV1 *scan = NULL;
		ClusterRelmapTerminalObservationV1 out, zero = { 0 };
		bool accepted = cluster_relmap_recovery_terminal_prefix_scan_v2(
			inputs, 0, FullTransactionIdFromU64(99), &scan);
		if (!accepted)
			cold_census_done(&inputs);
		UT_ASSERT(accepted);
		if (!accepted)
			return;
		UT_ASSERT(!cluster_relmap_recovery_terminal_scan_get_v1(scan, (void *)scan));
		UT_ASSERT(cluster_relmap_recovery_terminal_scan_get_v1(scan, &out));
		if (fault)
			v2_checkpoint_root_race();
		else
			inputs_pin_current = false;
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT(!cluster_relmap_recovery_terminal_scan_get_v1(scan, &out));
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		inputs_pin_current = true;
		UT_ASSERT(!cluster_relmap_recovery_terminal_scan_get_v1(scan, &out));
		cluster_relmap_recovery_terminal_scan_release_v1(&scan);
		UT_ASSERT(inputs_pin_held);
		cold_census_done(&inputs);
	}
}
UT_TEST(test_terminal_scan_full_native_commit_and_abort_sources)
{
	for (unsigned selected = 0; selected < 2; selected++)
		for (unsigned aborted = 0; aborted < 2; aborted++) {
			int saved_node = cluster_node_id;
			ClusterWalInputsV1 *inputs;
			ClusterRelmapTerminalScanV1 *scan = NULL;
			ClusterRelmapTerminalObservationV1 out, again;
			/* Native-window codec supports origin0..63. Original generic
			 * ROOT fixture's second origin127 stays legacy/unqualified.
			 * Exercise both own and foreign observer contexts for origin0. */
			cluster_node_id = selected ? 1 : 0;
			inputs = terminal_test_fixture(0, aborted, 0);
			UT_ASSERT(cluster_relmap_recovery_terminal_scan_v1(
				inputs, 0, FullTransactionIdFromU64(99), &scan));
			UT_ASSERT(scan != NULL);
			UT_ASSERT(!cluster_relmap_recovery_terminal_scan_get_v1(scan, (void *)scan));
			UT_ASSERT(cluster_relmap_recovery_terminal_scan_get_v1(scan, &out));
			UT_ASSERT_EQ(out.status,
						 aborted ? TRANSACTION_STATUS_ABORTED : TRANSACTION_STATUS_COMMITTED);
			UT_ASSERT_EQ(out.source.claim.identity.origin_node_id, 0);
			UT_ASSERT_EQ(out.requested_top_full_xid, 99);
			UT_ASSERT(out.post_flush_issued == 0 && out.prefix_end == 0);
			UT_ASSERT_EQ(out.records, 3);
			UT_ASSERT(out.terminal_start < out.terminal_end && out.terminal_end < out.cut);
			UT_ASSERT(!v2_zero(out.fingerprint, sizeof(out.fingerprint)));
			UT_ASSERT(cluster_relmap_recovery_terminal_scan_get_v1(scan, &again));
			UT_ASSERT(memcmp(&out, &again, sizeof(out)) == 0);
			cluster_relmap_recovery_terminal_scan_release_v1(&scan);
			cluster_relmap_recovery_terminal_scan_release_v1(&scan);
			UT_ASSERT(scan == NULL && inputs_pin_held);
			cold_census_done(&inputs);
			cluster_node_id = saved_node;
		}
}

UT_TEST(test_terminal_scan_refuses_invalid_window_target_and_suffix)
{
	for (unsigned fault = 1; fault <= 12; fault++) {
		ClusterWalInputsV1 *inputs = terminal_test_fixture(0, false, fault);
		ClusterRelmapTerminalScanV1 *scan = (void *)1;
		UT_ASSERT(!cluster_relmap_recovery_terminal_scan_v1(inputs, 0, FullTransactionIdFromU64(99),
															&scan));
		UT_ASSERT(scan == NULL && inputs_pin_held);
		cold_census_done(&inputs);
	}
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterWalInputsV1 *inputs = terminal_test_fixture(0, false, 0);
		ClusterRelmapTerminalScanV1 *scan = (void *)1;
		uint64 xid = fault == 0	  ? 79
					 : fault == 1 ? 100
					 : fault == 2 ? UINT64CONST(0x100000063)
								  : 99;
		UT_ASSERT(!cluster_relmap_recovery_terminal_scan_v1(inputs, fault == 3 ? 2 : 0,
															FullTransactionIdFromU64(xid), &scan));
		UT_ASSERT(scan == NULL && inputs_pin_held);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_terminal_scan_getter_keeps_original_owner_pins_and_zeroes_refusal)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		ClusterWalInputsV1 *inputs = terminal_test_fixture(0, false, 0);
		ClusterRelmapTerminalScanV1 *scan = NULL;
		ClusterRelmapTerminalObservationV1 out, zero = { 0 };
		UT_ASSERT(cluster_relmap_recovery_terminal_scan_v1(inputs, 0, FullTransactionIdFromU64(99),
														   &scan));
		if (fault == 0)
			inputs_pin_current = false;
		else
			v2_checkpoint_root_race();
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT(!cluster_relmap_recovery_terminal_scan_get_v1(scan, &out));
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0 && inputs_pin_held);
		cluster_relmap_recovery_terminal_scan_release_v1(&scan);
		UT_ASSERT(inputs_pin_held);
		cold_census_done(&inputs);
	}
}


UT_TEST(test_terminal_scan_error_frees_only_local_state_and_keeps_pin)
{
	for (unsigned getter = 0; getter < 2; getter++) {
		ClusterWalInputsV1 *inputs = terminal_test_fixture(0, false, 0);
		ClusterRelmapTerminalScanV1 *scan = NULL;
		ClusterRelmapTerminalObservationV1 out, zero = { 0 };
		volatile bool threw = false;
		if (getter)
			UT_ASSERT(cluster_relmap_recovery_terminal_scan_v1(
				inputs, 0, FullTransactionIdFromU64(99), &scan));
		test_throw_root_read = true;
		PG_TRY();
		{
			if (getter)
				(void)cluster_relmap_recovery_terminal_scan_get_v1(scan, &out);
			else
				(void)cluster_relmap_recovery_terminal_scan_v1(inputs, 0,
															   FullTransactionIdFromU64(99), &scan);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		test_throw_root_read = false;
		UT_ASSERT(threw && inputs_pin_held && test_actual_cf == NoLock);
		if (!getter)
			UT_ASSERT(scan == NULL);
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT(!cluster_relmap_recovery_terminal_scan_get_v1(scan, &out));
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		cluster_relmap_recovery_terminal_scan_release_v1(&scan);
		UT_ASSERT(scan == NULL && inputs_pin_held);
		cold_census_done(&inputs);
	}
}

static ClusterRelmapV2View
custody_view(bool published)
{
	ClusterRelmapV2View view = { 0 };
	view.committed = published ? custody_image.pending : custody_image.committed;
	view.committed_generation = published ? 8 : 7;
	view.pending_complete = view.has_pending = true;
	view.pending_generation = 8;
	view.pending_owner = custody_image.pending_owner;
	view.pending = custody_image.pending;
	view.pending_committed_generation = 7;
	view.pending_committed = custody_image.committed;
	return view;
}
static int custody_saved_node;
static ClusterWalInputsV1 *
custody_setup(bool shared, bool aborted)
{
	ClusterWalInputsV1 *inputs;
	custody_saved_node = cluster_node_id;
	cluster_node_id = 1; /* Original cold owner is foreign origin zero. */
	custody_shared = shared;
	custody_scope = 7;
	custody_scope_reads = custody_scope_error_after = 0;
	custody_terminal_calls = custody_terminal_fault = 0;
	custody_inventory_calls = custody_inventory_fault = 0;
	custody_inventory_expected = custody_extra_terminal ? 2 : 1;
	inputs = terminal_prefix_fixture(aborted, custody_extra_terminal ? 30 : 29);
	prefix_base_expected = *cluster_wal_inputs_at_v1(inputs, 0);
	prefix_base_fault = prefix_base_scope_checks = prefix_base_reads = 0;
	prefix_base_status = aborted ? TRANSACTION_STATUS_ABORTED : TRANSACTION_STATUS_COMMITTED;
	prefix_base_last_byte = 0x24;
	return inputs;
}
static void
custody_done(ClusterWalInputsV1 **inputs)
{
	cold_census_done(inputs);
	cluster_node_id = custody_saved_node;
	custody_extra_terminal = false;
	custody_era_offset = custody_next_override = 0;
}
UT_TEST(test_recovery_custody_binds_both_namespaces_and_pair_cuts)
{
	for (unsigned shared = 0; shared < 2; shared++)
		for (unsigned cut = 0; cut < 3; cut++) {
			ClusterWalInputsV1 *inputs = custody_setup(shared, cut % 2);
			ClusterRelmapRecoveryHandle *handle = NULL;
			ClusterRelmapV2View view = custody_view(cut == 1);
			ClusterRelmapV2Namespace identity = custody_image.identity;
			ClusterRelmapV2View saved_view = view;
			ClusterRelmapV2Namespace saved_identity = identity;
			UT_ASSERT(!cluster_relmap_recovery_prepare_v1(inputs, &identity, &view, (void *)&view));
			UT_ASSERT(memcmp(&view, &saved_view, sizeof(view)) == 0);
			UT_ASSERT(
				!cluster_relmap_recovery_prepare_v1(inputs, &identity, &view, (void *)&identity));
			UT_ASSERT(memcmp(&identity, &saved_identity, sizeof(identity)) == 0);
			UT_ASSERT(cluster_relmap_recovery_prepare_v1(inputs, &identity, &view, &handle));
			UT_ASSERT(handle != NULL && inputs_pin_held);
			UT_ASSERT(!cluster_relmap_recovery_handle_matches_v1(handle, (void *)handle, &view));
			UT_ASSERT(
				!cluster_relmap_recovery_handle_matches_v1(handle, &identity, (void *)handle));
			UT_ASSERT(cluster_relmap_recovery_handle_matches_v1(handle, &identity, &view));
			UT_ASSERT_EQ(prefix_base_reads, 2); /* prepare + successful match */
			cluster_relmap_recovery_handle_release_v1(&handle);
			UT_ASSERT(handle == NULL && inputs_pin_held);
			custody_done(&inputs);
		}
}
UT_TEST(test_recovery_custody_rejects_pending_owner_window_and_view_corruption)
{
	for (unsigned fault = 0; fault < 19; fault++) {
		ClusterWalInputsV1 *inputs = custody_setup(false, false);
		ClusterRelmapRecoveryHandle *handle = (void *)1;
		ClusterRelmapV2View view = custody_view(false);
		ClusterRelmapV2Namespace identity = custody_image.identity;
		if (fault == 0)
			view.pending_complete = false;
		if (fault == 1)
			view.has_pending = false;
		if (fault == 2)
			view.pending_owner.membership_epoch++;
		if (fault == 3)
			view.pending_owner.boot_incarnation++;
		if (fault == 4)
			view.pending_owner.admitted_incarnation++;
		if (fault == 5)
			view.pending_owner.top_full_xid += UINT64CONST(0x100000000);
		if (fault == 6)
			view.pending_owner.wal_end++;
		if (fault == 7)
			view.pending_owner.node = 1;
		if (fault == 8) {
			view.pending.mappings[0].mapfilenumber++;
			relmap_test_crc(&view.pending);
		}
		if (fault == 9)
			view.pending_generation++;
		if (fault == 10)
			identity.database_incarnation++;
		if (fault == 11)
			identity.dbid++;
		if (fault == 12)
			view.committed_generation++;
		if (fault == 13) {
			view.committed.mappings[0].mapfilenumber++;
			relmap_test_crc(&view.committed);
		}
		if (fault == 14)
			view.pending_committed.crc++;
		if (fault == 15)
			custody_scope = 0;
		if (fault == 16)
			inputs_pin_current = false;
		if (fault == 17)
			prefix_base_fault = 1;
		if (fault == 18)
			view.used_backup = true;
		UT_ASSERT(!cluster_relmap_recovery_prepare_v1(inputs, &identity, &view, &handle));
		UT_ASSERT(handle == NULL && inputs_pin_held);
		UT_ASSERT_EQ(prefix_base_reads, 0);
		custody_done(&inputs);
	}
}
UT_TEST(test_recovery_custody_refusal_is_sticky_and_scope_aba_is_not_current)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		ClusterWalInputsV1 *inputs = custody_setup(false, false);
		ClusterRelmapRecoveryHandle *handle = NULL;
		ClusterRelmapV2View view = custody_view(false), changed = view;
		ClusterRelmapV2Namespace identity = custody_image.identity, changed_ns = identity;
		UT_ASSERT(cluster_relmap_recovery_prepare_v1(inputs, &identity, &view, &handle));
		if (fault == 0)
			custody_scope++;
		if (fault == 1)
			changed.pending_owner.wal_end++;
		if (fault == 2)
			changed_ns.dbid++;
		if (fault == 3)
			inputs_pin_current = false;
		if (fault == 4)
			v2_checkpoint_root_race();
		if (fault == 5)
			prefix_base_fault = 9;
		UT_ASSERT(!cluster_relmap_recovery_handle_matches_v1(handle, &changed_ns, &changed));
		custody_scope = 7;
		inputs_pin_current = true;
		prefix_base_fault = 0;
		UT_ASSERT(!cluster_relmap_recovery_handle_matches_v1(handle, &identity, &view));
		cluster_relmap_recovery_handle_release_v1(&handle);
		UT_ASSERT(handle == NULL && inputs_pin_held);
		custody_done(&inputs);
	}
}

UT_TEST(test_recovery_custody_error_preserves_pins_and_invalidates_handle)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		ClusterWalInputsV1 *inputs = custody_setup(false, false);
		ClusterRelmapRecoveryHandle *handle = NULL;
		ClusterRelmapV2View view = custody_view(false);
		ClusterRelmapV2Namespace identity = custody_image.identity;
		volatile bool threw = false;
		if (fault)
			UT_ASSERT(cluster_relmap_recovery_prepare_v1(inputs, &identity, &view, &handle));
		custody_scope_error_after = fault ? custody_scope_reads + 1 : 2;
		test_capture_error_level = true;
		test_last_error_level = 0;
		PG_TRY();
		{
			if (fault)
				(void)cluster_relmap_recovery_handle_matches_v1(handle, &identity, &view);
			else
				(void)cluster_relmap_recovery_prepare_v1(inputs, &identity, &view, &handle);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		test_capture_error_level = false;
		custody_scope_error_after = 0;
		UT_ASSERT(threw && test_last_error_level == ERROR);
		UT_ASSERT(inputs_pin_held && test_actual_cf == NoLock);
		if (fault)
			UT_ASSERT(!cluster_relmap_recovery_handle_matches_v1(handle, &identity, &view));
		else
			UT_ASSERT(handle == NULL);
		cluster_relmap_recovery_handle_release_v1(&handle);
		custody_done(&inputs);
	}
}

UT_TEST(test_recovery_custody_requires_original_sealed_terminal_source)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterWalInputsV1 *inputs = custody_setup(false, fault % 2);
		ClusterRelmapRecoveryHandle *handle = NULL;
		ClusterRelmapV2View view = custody_view(false);
		ClusterRelmapV2Namespace identity = custody_image.identity;
		volatile bool threw = false;
		custody_terminal_fault = fault;
		test_capture_error_level = fault == 2;
		PG_TRY();
		{
			UT_ASSERT_EQ(cluster_relmap_recovery_prepare_v1(inputs, &identity, &view, &handle),
						 fault == 0);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		test_capture_error_level = false;
		custody_terminal_fault = 0;
		UT_ASSERT_EQ(threw, fault == 2);
		UT_ASSERT_EQ(custody_terminal_calls, 1);
		UT_ASSERT_EQ(handle != NULL, fault == 0);
		UT_ASSERT(inputs_pin_held && test_actual_cf == NoLock);
		cluster_relmap_recovery_handle_release_v1(&handle);
		custody_done(&inputs);
	}
}

UT_TEST(test_recovery_custody_reads_original_native_base_before_publication)
{
	static const unsigned faults[] = { 0, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
	for (unsigned aborted = 0; aborted < 2; aborted++)
		for (unsigned i = 0; i < lengthof(faults); i++) {
			unsigned fault = faults[i];
			ClusterWalInputsV1 *inputs = custody_setup(i % 2, aborted);
			ClusterRelmapRecoveryHandle *handle = NULL;
			ClusterRelmapV2View view = custody_view(false);
			volatile bool threw = false;
			prefix_base_fault = fault;
			test_capture_error_level = fault == 8;
			PG_TRY();
			{
				UT_ASSERT_EQ(cluster_relmap_recovery_prepare_v1(inputs, &custody_image.identity,
																&view, &handle),
							 fault == 0);
			}
			PG_CATCH();
			{
				threw = true;
			}
			PG_END_TRY();
			test_capture_error_level = false;
			prefix_base_fault = 0;
			UT_ASSERT_EQ(threw, fault == 8);
			/* Fault 9 invalidates the source in the pre-I/O scope check. */
			UT_ASSERT_EQ(prefix_base_reads, fault == 9 ? 0 : 1);
			UT_ASSERT_EQ(handle != NULL, fault == 0);
			UT_ASSERT(inputs_pin_held && test_actual_cf == NoLock);
			cluster_relmap_recovery_handle_release_v1(&handle);
			custody_done(&inputs);
		}
}

UT_TEST(test_recovery_custody_base_drift_and_io_error_are_sticky)
{
	for (unsigned shared = 0; shared < 2; shared++)
		for (unsigned fault = 0; fault < 4; fault++) {
			ClusterWalInputsV1 *inputs = custody_setup(shared, fault % 2);
			ClusterRelmapRecoveryHandle *handle = NULL;
			ClusterRelmapV2View view = custody_view(false);
			volatile bool threw = false;
			unsigned reads;
			uint8 original = prefix_base_last_byte;
			UT_ASSERT(cluster_relmap_recovery_prepare_v1(inputs, &custody_image.identity, &view,
														 &handle));
			UT_ASSERT_EQ(prefix_base_reads, 1);
			UT_ASSERT(
				cluster_relmap_recovery_handle_matches_v1(handle, &custody_image.identity, &view));
			UT_ASSERT_EQ(prefix_base_reads, 2);
			if (fault == 0)
				prefix_base_last_byte ^= 1; /* Unrelated native page byte. */
			if (fault == 1)
				prefix_base_fault = 8;
			if (fault == 2)
				prefix_base_fault = 5;
			if (fault == 3)
				prefix_base_fault = 2;
			test_capture_error_level = fault == 1;
			PG_TRY();
			{
				UT_ASSERT(!cluster_relmap_recovery_handle_matches_v1(
					handle, &custody_image.identity, &view));
			}
			PG_CATCH();
			{
				threw = true;
			}
			PG_END_TRY();
			test_capture_error_level = false;
			UT_ASSERT_EQ(threw, fault == 1);
			UT_ASSERT_EQ(prefix_base_reads, 3);
			reads = prefix_base_reads;
			prefix_base_last_byte = original;
			prefix_base_fault = 0;
			UT_ASSERT(
				!cluster_relmap_recovery_handle_matches_v1(handle, &custody_image.identity, &view));
			UT_ASSERT_EQ(prefix_base_reads, reads);
			UT_ASSERT(inputs_pin_held && inputs_pin_current && test_actual_cf == NoLock);
			cluster_relmap_recovery_handle_release_v1(&handle);
			UT_ASSERT_EQ(prefix_base_reads, reads);
			custody_done(&inputs);
		}
}

UT_TEST(test_recovery_custody_requires_complete_original_native_terminal_set)
{
	for (unsigned shared = 0; shared < 2; shared++)
		for (unsigned fault = 0; fault < 9; fault++) {
			ClusterWalInputsV1 *inputs;
			ClusterRelmapRecoveryHandle *handle = NULL;
			ClusterRelmapV2View view;
			volatile bool threw = false;
			custody_extra_terminal = true;
			inputs = custody_setup(shared, fault % 2);
			view = custody_view(false);
			custody_inventory_fault = fault;
			test_capture_error_level = fault == 7 || fault == 8;
			PG_TRY();
			{
				UT_ASSERT_EQ(cluster_relmap_recovery_prepare_v1(inputs, &custody_image.identity,
																&view, &handle),
							 fault == 0);
			}
			PG_CATCH();
			{
				threw = true;
			}
			PG_END_TRY();
			test_capture_error_level = false;
			UT_ASSERT_EQ(threw, fault == 7 || fault == 8);
			UT_ASSERT_EQ(handle != NULL, fault == 0);
			UT_ASSERT_EQ(custody_terminal_calls, fault == 1 || fault == 2 || fault == 8 ? 0 : 2);
			UT_ASSERT_EQ(prefix_base_reads, fault == 0 ? 1 : 0);
			UT_ASSERT(inputs_pin_held && test_actual_cf == NoLock);
			cluster_relmap_recovery_handle_release_v1(&handle);
			custody_done(&inputs);
		}
}

UT_TEST(test_terminal_read_exact_origin_namespace_era_and_outcome)
{
	for (unsigned shared = 0; shared < 2; shared++)
		for (unsigned era = 0; era < 2; era++)
			for (unsigned aborted = 0; aborted < 2; aborted++) {
				ClusterWalInputsV1 *inputs;
				ClusterRelmapRecoveryHandle *handle = NULL;
				ClusterRelmapRecoveryTerminalV1 out;
				ClusterRelmapV2View view;
				custody_era_offset = (uint64)era << 32;
				inputs = custody_setup(shared, aborted);
				view = custody_view(false);
				UT_ASSERT(cluster_relmap_recovery_prepare_v1(inputs, &custody_image.identity,
					&view, &handle));
				UT_ASSERT(cluster_relmap_recovery_terminal_read(handle, &custody_image.identity,
					&view, &out));
				UT_ASSERT_EQ(out.top_full_xid, custody_era_offset + 99);
				UT_ASSERT_EQ(out.bounds.native_floor, custody_era_offset + 80);
				UT_ASSERT_EQ(out.bounds.last_issued, custody_era_offset + 99);
				UT_ASSERT_EQ(out.bounds.origin, 0);
				UT_ASSERT_EQ(out.bounds.system_identifier, custody_image.identity.system_identifier);
				UT_ASSERT_EQ(out.bounds.database_incarnation, custody_image.identity.database_incarnation);
				UT_ASSERT_EQ(out.bounds.membership_epoch, view.pending_owner.membership_epoch);
				UT_ASSERT_EQ(out.bounds.boot_incarnation, view.pending_owner.boot_incarnation);
				UT_ASSERT_EQ(out.bounds.admitted_incarnation, view.pending_owner.admitted_incarnation);
				UT_ASSERT_EQ(out.status, aborted ? TRANSACTION_STATUS_ABORTED : TRANSACTION_STATUS_COMMITTED);
				UT_ASSERT(out.terminal_start >= view.pending_owner.wal_end);
				UT_ASSERT(out.terminal_start < out.terminal_end && out.terminal_end < out.source_cut);
				UT_ASSERT(!v2_zero(out.fingerprint, 32));
				UT_ASSERT_EQ(prefix_base_reads, 2);
				UT_ASSERT(inputs_pin_held && inputs_pin_current && test_actual_cf == NoLock);
				cluster_relmap_recovery_handle_release_v1(&handle);
				custody_done(&inputs);
			}
}

UT_TEST(test_terminal_read_checkpoint_half_space_boundary_is_not_issuance)
{
	for (unsigned boundary = 0; boundary < 3; boundary++) {
		ClusterWalInputsV1 *inputs;
		ClusterRelmapRecoveryHandle *handle = NULL;
		ClusterRelmapRecoveryTerminalV1 out, zero = { 0 };
		ClusterRelmapV2View view;
		custody_next_override = UINT64CONST(0x80000000) + 79 + boundary;
		inputs = custody_setup(false, false);
		view = custody_view(false);
		UT_ASSERT(cluster_relmap_recovery_prepare_v1(inputs, &custody_image.identity, &view, &handle));
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT_EQ(cluster_relmap_recovery_terminal_read(handle, &custody_image.identity,
			&view, &out), boundary == 0);
		if (boundary == 0)
			UT_ASSERT_EQ(out.bounds.last_issued, 99);
		else {
			UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
			UT_ASSERT(!cluster_relmap_recovery_handle_matches_v1(handle, &custody_image.identity, &view));
		}
		UT_ASSERT(inputs_pin_held);
		cluster_relmap_recovery_handle_release_v1(&handle);
		custody_done(&inputs);
	}
}

UT_TEST(test_terminal_read_drift_and_wrong_era_are_sticky_without_pin_release)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		ClusterWalInputsV1 *inputs = custody_setup(false, false);
		ClusterRelmapRecoveryHandle *handle = NULL;
		ClusterRelmapRecoveryTerminalV1 out, zero = { 0 };
		ClusterRelmapV2View view = custody_view(false), original = view;
		UT_ASSERT(cluster_relmap_recovery_prepare_v1(inputs, &custody_image.identity, &view, &handle));
		if (fault == 0) view.pending_owner.top_full_xid += UINT64CONST(0x100000000);
		if (fault == 1) prefix_base_last_byte ^= 1;
		if (fault == 2) prefix_base_fault = 14;
		if (fault == 3) inputs_pin_current = false;
		if (fault == 4) custody_scope++;
		if (fault == 5) prefix_base_fault = 2;
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT(!cluster_relmap_recovery_terminal_read(handle, &custody_image.identity, &view, &out));
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		UT_ASSERT(inputs_pin_held && test_actual_cf == NoLock);
		prefix_base_fault = 0;
		inputs_pin_current = true;
		UT_ASSERT(!cluster_relmap_recovery_terminal_read(handle, &custody_image.identity, &original, &out));
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		cluster_relmap_recovery_handle_release_v1(&handle);
		custody_done(&inputs);
	}
}

UT_TEST(test_terminal_read_alias_refusal_keeps_original_custody)
{
	ClusterWalInputsV1 *inputs = custody_setup(true, true);
	ClusterRelmapRecoveryHandle *handle = NULL;
	ClusterRelmapRecoveryTerminalV1 out, zero = { 0 };
	ClusterRelmapV2View view = custody_view(false), original = view;
	ClusterRelmapV2Namespace identity = custody_image.identity, saved = identity;
	UT_ASSERT(cluster_relmap_recovery_prepare_v1(inputs, &identity, &view, &handle));
	UT_ASSERT(!cluster_relmap_recovery_terminal_read(handle, &identity, &view, (void *)&view));
	UT_ASSERT(!cluster_relmap_recovery_terminal_read(handle, &identity, &view, (void *)&identity));
	UT_ASSERT(!cluster_relmap_recovery_terminal_read(handle, &identity, &view, (void *)handle));
	UT_ASSERT(memcmp(&view, &original, sizeof(view)) == 0 && memcmp(&identity, &saved, sizeof(identity)) == 0);
	UT_ASSERT(!cluster_relmap_recovery_terminal_read(handle, &identity, &view, NULL));
	UT_ASSERT(cluster_relmap_recovery_terminal_read(handle, &identity, &view, &out));
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!cluster_relmap_recovery_terminal_read(NULL, &identity, &view, &out));
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0 && inputs_pin_held);
	cluster_relmap_recovery_handle_release_v1(&handle);
	custody_done(&inputs);
}

UT_TEST(test_terminal_read_native_error_keeps_output_zero_and_custody_dead)
{
	ClusterWalInputsV1 *inputs = custody_setup(false, true);
	ClusterRelmapRecoveryHandle *handle = NULL;
	ClusterRelmapRecoveryTerminalV1 out, zero = { 0 };
	ClusterRelmapV2View view = custody_view(false);
	volatile bool threw = false;
	unsigned reads;
	UT_ASSERT(cluster_relmap_recovery_prepare_v1(inputs, &custody_image.identity, &view, &handle));
	prefix_base_fault = 8;
	test_capture_error_level = true;
	memset(&out, 0xa5, sizeof(out));
	PG_TRY();
	{
		(void)cluster_relmap_recovery_terminal_read(handle, &custody_image.identity, &view, &out);
	}
	PG_CATCH();
	{
		threw = true;
	}
	PG_END_TRY();
	test_capture_error_level = false;
	UT_ASSERT(threw && inputs_pin_held && test_actual_cf == NoLock);
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	reads = prefix_base_reads;
	prefix_base_fault = 0;
	UT_ASSERT(!cluster_relmap_recovery_terminal_read(handle, &custody_image.identity, &view, &out));
	UT_ASSERT_EQ(prefix_base_reads, reads);
	cluster_relmap_recovery_handle_release_v1(&handle);
	custody_done(&inputs);
}

#include "test_cluster_relmap_origin_build.inc"
#include "test_cluster_relmap_execution_custody.inc"
#include "test_cluster_relmap_execution_targets.inc"
#include "test_cluster_relmap_execution_observe.inc"
#include "test_cluster_relmap_census_open.inc"
#include "test_cluster_relmap_cold_native.inc"
#include "test_cluster_relmap_original_scope.inc"
#include "test_cluster_relmap_original_authority.inc"
#include "test_cluster_relmap_original_cohort.inc"
#include "test_cluster_relmap_native_file.inc"
#include "test_cluster_relmap_original_storage.inc"

int
main(int argc, char **argv)
{
	(void)argc;
	setvbuf(stdout, NULL, _IOLBF, 0);
	test_program_path = argv[0];
	setup_fixture();
	UT_PLAN(137);
	UT_RUN(test_cold_original_storage_full_native_chain_and_fd_custody);
	UT_RUN(test_cold_original_storage_rejects_unqualified_sentinel_before_native);
	UT_RUN(test_cold_original_storage_midread_change_and_error_preserve_pending);
	UT_RUN(test_cold_native_replaced_file_refuses_before_pair_write);
	UT_RUN(test_cold_native_replaced_file_after_pair_sync_keeps_debt);
	UT_RUN(test_cold_native_replaced_file_permanently_invalidates_old_handle);
	UT_RUN(test_cold_cohort_other_origins_preserve_selected_consumer);
	UT_RUN(test_cold_cohort_same_origin_generations_refuse_before_native);
	UT_RUN(test_cold_cohort_foreign_pending_requires_original_directory);
	UT_RUN(test_cold_original_authority_rejects_broken_provenance_before_storage);
	UT_RUN(test_cold_original_authority_postwrite_loss_keeps_original_debt);
	UT_RUN(test_cold_original_authority_late_provider_refusal_or_error_keeps_pending);
	UT_RUN(test_cold_original_scope_native_commit_abort);
	UT_RUN(test_cold_original_scope_reentry_rejects_old_cursor);
	UT_RUN(test_cold_original_scope_refuses_non_cold_or_stale_owner);
	UT_RUN(test_cold_native_actual_clog_commit_abort_and_full_page);
	UT_RUN(test_cold_native_actual_clog_missing_short_and_io_failures);
	UT_RUN(test_cold_native_actual_clog_late_base_change_or_error);
	UT_RUN(test_cold_native_commit_abort_actual_pair_and_fresh_committed);
	UT_RUN(test_cold_native_clean_does_not_read_clog_or_write_pairs);
	UT_RUN(test_cold_native_unknown_and_native_error_keep_pending);
	UT_RUN(test_cold_native_each_sync_cut_returns_no_observation_or_retry);
	UT_RUN(test_cold_native_postwrite_scope_loss_or_error_keeps_original_debt);
	UT_RUN(test_cold_committed_original_wal_factory_and_all_physical_namespaces);
	UT_RUN(test_cold_committed_original_wal_source_and_other_namespace_drift_refuse);
	UT_RUN(test_census_open_original_wal_physical_pairs_and_owned_fd);
	UT_RUN(test_census_open_initial_gates_output_and_budgets);
	UT_RUN(test_census_open_checkpoint_refusal_and_error_cleanup);
	UT_RUN(test_census_open_storage_refusal_and_error_cleanup);
	UT_RUN(test_census_open_all_physical_passes_refuse_drift_and_errors);
	UT_RUN(test_census_open_later_missing_pair_source_or_error_keeps_fd_until_destroy);
	UT_RUN(test_census_open_early_destroy_owns_only_new_custody);
	UT_RUN(test_execution_observe_complete_wal_and_physical_union);
	UT_RUN(test_execution_observe_missing_gates_and_invalid_source);
	UT_RUN(test_execution_observe_fresh_output_aliases_and_budgets);
	UT_RUN(test_execution_observe_storage_refusal_and_error_cleanup);
	UT_RUN(test_execution_observe_wal_refusal_and_error_cleanup);
	UT_RUN(test_execution_observe_physical_read_and_owner_drift_cleanup);
	UT_RUN(test_execution_observe_physical_change_never_publishes);
	UT_RUN(test_execution_observe_feeds_original_cold_census);
	UT_RUN(test_execution_targets_preserve_real_wal_missing_and_short_files);
	UT_RUN(test_execution_targets_missing_initial_gates_leave_root_unchanged);
	UT_RUN(test_execution_targets_invalid_source_leaves_unstarted_root);
	UT_RUN(test_execution_targets_late_visit_refusal_or_error_revokes_partial_root);
	UT_RUN(test_execution_targets_payload_refusal_preserves_original_owner);
	UT_RUN(test_execution_targets_late_context_or_input_drift_revokes_root);
	UT_RUN(test_execution_targets_alias_and_started_root_refuse_safely);
	UT_RUN(test_custody_source_observes_original_tuple_without_native_page_io);
	UT_RUN(test_custody_source_refusal_and_error_are_sticky);
	UT_RUN(test_custody_source_alias_refusal_keeps_handle_usable);
	UT_RUN(test_execution_pending_original_source_and_native_terminal_read);
	UT_RUN(test_execution_pending_requires_original_owner_and_single_source);
	UT_RUN(test_execution_pending_refuses_wrong_complete_view_and_era);
	UT_RUN(test_execution_pending_keeps_native_inventory_and_lifetime_gates);
	UT_RUN(test_execution_pending_drift_is_sticky_and_keeps_original_owner);
	UT_RUN(test_execution_pending_error_releases_only_local_custody);
	UT_RUN(test_execution_pending_alias_refusal_preserves_inputs);
	UT_RUN(test_execution_root_fd_original_source_and_owner);
	UT_RUN(test_execution_root_fd_missing_initial_gates_does_no_storage_io);
	UT_RUN(test_execution_root_fd_post_io_drift_or_error_closes_only_new_fd);
	UT_RUN(test_execution_root_fd_invalid_provenance_cannot_open_storage);
	UT_RUN(test_execution_build_source_publishes_same_original_provenance);
	UT_RUN(test_execution_build_source_refusal_and_error_keep_both_outputs_atomic);
	UT_RUN(test_execution_build_source_rejects_nonzero_overlap_and_missing_outputs);
	UT_RUN(test_execution_build_uses_original_inputs_without_census_read_scope);
	UT_RUN(test_execution_build_refuses_wrong_role_and_original_owner);
	UT_RUN(test_execution_build_validates_checkpoint_window_and_complete_suffix);
	UT_RUN(test_execution_build_never_resets_old_storage);
	UT_RUN(test_execution_build_late_refusal_and_error_publish_nothing);
	UT_RUN(test_origin_build_uses_original_checkpoint_and_round);
	UT_RUN(test_origin_build_clean_checkpoint_needs_no_terminal_or_pending);
	UT_RUN(test_origin_build_refuses_wrong_owner_and_scope);
	UT_RUN(test_origin_build_refuses_unsupported_or_changed_inputs);
	UT_RUN(test_origin_build_cannot_reset_or_overlap_storage);
	UT_RUN(test_origin_build_late_round_root_and_pin_refusal_is_atomic);
	UT_RUN(test_origin_build_error_keeps_output_and_original_pins);
	UT_RUN(test_terminal_read_exact_origin_namespace_era_and_outcome);
	UT_RUN(test_terminal_read_checkpoint_half_space_boundary_is_not_issuance);
	UT_RUN(test_terminal_read_drift_and_wrong_era_are_sticky_without_pin_release);
	UT_RUN(test_terminal_read_alias_refusal_keeps_original_custody);
	UT_RUN(test_terminal_read_native_error_keeps_output_zero_and_custody_dead);
	UT_RUN(test_recovery_custody_requires_complete_original_native_terminal_set);
	UT_RUN(test_recovery_custody_base_drift_and_io_error_are_sticky);
	UT_RUN(test_recovery_custody_reads_original_native_base_before_publication);
	UT_RUN(test_recovery_custody_requires_original_sealed_terminal_source);
	UT_RUN(test_recovery_custody_error_preserves_pins_and_invalidates_handle);
	UT_RUN(test_recovery_custody_binds_both_namespaces_and_pair_cuts);
	UT_RUN(test_recovery_custody_rejects_pending_owner_window_and_view_corruption);
	UT_RUN(test_recovery_custody_refusal_is_sticky_and_scope_aba_is_not_current);
	UT_RUN(test_retained_relmap_matches_each_selected_namespace);
	UT_RUN(test_retained_relmap_requires_exact_pending);
	UT_RUN(test_retained_relmap_scans_suffix_and_other_source);
	UT_RUN(test_retained_relmap_waits_for_open_and_rejects_online);
	UT_RUN(test_retained_relmap_original_recheck_and_error_keep_pin);
	UT_RUN(test_retained_relmap_invalid_input_is_read_only);
	UT_RUN(test_retained_side_exact_complete_and_permuted);
	UT_RUN(test_retained_side_clean_pair_still_checks_structure);
	UT_RUN(test_retained_side_rejects_incomplete_or_invalid_vectors);
	UT_RUN(test_retained_side_checks_actual_cut_claim_and_empty_source);
	UT_RUN(test_retained_side_preserves_native_scan_and_final_rechecks);
	UT_RUN(test_retained_side_requires_original_structural_operations);
	UT_RUN(test_census_wal_rebuild_stage_and_publish_cuts);
	UT_RUN(test_census_wal_mismatch_is_atomic_and_sticky);
	UT_RUN(test_census_wal_wait_online_and_missing_scope_refuse);
	UT_RUN(test_census_wal_original_recheck_error_retains_both_pins);
	UT_RUN(test_census_wal_structural_conflict_leaves_destination);
	UT_RUN(test_census_wal_rechecks_borrowed_directory_after_scan);
	UT_RUN(test_census_wal_physical_root_recheck_still_required);
	UT_RUN(test_census_wal_clean_pair_and_missing_window);
	UT_RUN(test_census_side_wal_same_window_stage_publish_and_history);
	UT_RUN(test_census_side_wal_structural_capture_refusal_is_atomic);
	UT_RUN(test_census_side_wal_side_refusal_precedes_scan);
	UT_RUN(test_census_side_wal_wal_refusal_never_installs_side);
	UT_RUN(test_census_side_wal_recheck_and_error_keep_window_invalid);
	UT_RUN(test_census_side_wal_late_physical_root_rejects_provisional);
	UT_RUN(test_census_side_wal_collector_error_invalidates_before_wal);
	UT_RUN(test_census_side_wal_window_clean_pair_and_destination);
	UT_RUN(test_census_side_wal_clean_pair_rebuilds_original_structure);
	UT_RUN(test_census_side_wal_clean_pair_refusal_is_atomic_and_sticky);
	UT_RUN(test_census_side_wal_checks_original_full_source_vector);
	UT_RUN(test_census_side_collector_error_invalidates_window);
	UT_RUN(test_census_side_wal_keeps_real_empty_initializer);
	UT_RUN(test_census_side_wal_requires_real_empty_initializer);
	UT_RUN(test_terminal_native_base_retains_only_undestroyed_original_page);
	UT_RUN(test_terminal_native_base_refuses_unknown_or_malformed_clog_carriers);
	UT_RUN(test_terminal_prefix_native_base_binds_full_image_and_source);
	UT_RUN(test_terminal_prefix_native_base_rejects_scope_and_native_mismatch_sticky);
	UT_RUN(test_terminal_prefix_native_base_error_keeps_output_and_original_pins);
	UT_RUN(test_terminal_prefix_scan_requires_original_retained_owner);
	UT_RUN(test_terminal_prefix_scan_binds_actual_checkpoint_end_and_issued_cap);
	UT_RUN(test_terminal_prefix_scan_refuses_unbounded_or_nonmatching_sources);
	UT_RUN(test_terminal_prefix_scan_getter_invalidates_cap_and_prefix_together);
	UT_RUN(test_terminal_scan_requires_original_retained_owner);
	UT_RUN(test_terminal_scan_full_native_commit_and_abort_sources);
	UT_RUN(test_terminal_scan_refuses_invalid_window_target_and_suffix);
	UT_RUN(test_terminal_scan_getter_keeps_original_owner_pins_and_zeroes_refusal);
	UT_RUN(test_terminal_scan_error_frees_only_local_state_and_keeps_pin);
	UT_DONE();
	return ut_failed_count != 0;
}
