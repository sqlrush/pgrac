/* PGRAC: relmap owner tests using the original native WAL writer above.
 * Transaction/context, TT receipt and lock providers are fixture boundaries.
 * Native RELMAP records below use DecodeXLogRecord, not a physical retained
 * visit. No source authentication, TT publication or live completion claimed.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#include "access/xact.h"
#include "cluster/cluster_relmap_authority.h"
#include "cluster/cluster_relmap_lock.h"
#include "cluster/cluster_relmap_owner.h"
#include "cluster/cluster_relmap_retention.h"
#include "cluster/cluster_tt_slot.h"
#include "catalog/pg_tablespace_d.h"
#include "utils/relmapper.h"

static bool relmap_global_x, relmap_local_lock, relmap_in_transaction;
static uint64 relmap_top_full;
static unsigned relmap_assign_calls;
static bool relmap_advance_epoch_on_assign;
static bool relmap_have_binding, relmap_binding_drift;
static bool relmap_active_owner;
static bool relmap_stage_lock_window;
static unsigned relmap_binding_owner_fault;
static unsigned relmap_binding_reads;
static ClusterCanonicalTxnBinding relmap_binding;
/* This fixture uses the real native writer, with controlled conversion I/O. */
static ClusterRelmapV2Namespace relmap_conversion_identity;
static unsigned relmap_conversion_calls, relmap_conversion_drift;
static bool relmap_conversion_ok;

static void
relmap_retention_lock_enter(void)
{
	if ((!relmap_global_x && !relmap_active_owner && !AmStartupProcess()
		 && !(relmap_xidgen_held && relmap_xidgen_mode == LW_EXCLUSIVE))
		|| (relmap_local_lock && !relmap_stage_lock_window) || CritSectionCount != 0)
		abort();
}

FullTransactionId
GetTopFullTransactionId(void)
{
	if (relmap_local_lock || CritSectionCount != 0 || !relmap_global_x)
		abort();
	relmap_assign_calls++;
	if (relmap_advance_epoch_on_assign)
		epoch++;
	return FullTransactionIdFromU64(relmap_top_full);
}

FullTransactionId
GetTopFullTransactionIdRelmap(uint64 system_identifier, uint64 database_incarnation, uint16 origin)
{
	if (system_identifier != ref.claim.identity.system_identifier
		|| database_incarnation != ref.claim.database_incarnation
		|| origin != ref.claim.identity.origin_node_id)
		return InvalidFullTransactionId;
	return GetTopFullTransactionId(); /* Controlled xact leaf in this writer fixture. */
}

FullTransactionId
GetTopFullTransactionIdIfAny(void)
{
	if (relmap_retention_held)
		abort();
	return FullTransactionIdFromU64(relmap_top_full);
}

bool
IsTransactionState(void)
{
	return relmap_in_transaction;
}

TimeLineID
GetWALInsertionTimeLine(void)
{
	return native_ctl.InsertTimeLineID;
}

bool
LWLockHeldByMe(LWLock *lock)
{
	if (lock == XidGenLock)
		return relmap_xidgen_held;
	if (lock == &relmap_retention_lock || lock == relmap_runtime_test_lock)
		return relmap_retention_held;
	return lock == RelationMappingLock && relmap_local_lock;
}

bool
cluster_tt_local_get_published_binding(TransactionId top_xid, ClusterCanonicalTxnBinding *out)
{
	if (relmap_retention_held || relmap_local_lock || (!relmap_global_x && !relmap_active_owner)
		|| CritSectionCount != 0 || top_xid != (uint32) relmap_top_full)
		abort();
	relmap_binding_reads++;
	memset(out, 0, sizeof(*out));
	if (relmap_have_binding)
		*out = relmap_binding;
	if (relmap_binding_drift)
		relmap_top_full += UINT64CONST(0x100000000);
	if (relmap_binding_owner_fault == 1) epoch++;
	if (relmap_binding_owner_fault == 2) ref.claim.claim_sha256[0]++;
	if (relmap_binding_owner_fault == 3) native_ctl.InsertTimeLineID++;
	if (relmap_binding_owner_fault == 4) ShutdownRequestPending = true;
	if (relmap_binding_owner_fault == 5) cluster_node_id++;
	if (relmap_binding_owner_fault == 6) have_ref = false;
	if (relmap_binding_owner_fault == 7) relmap_admission_delta = 1;
	return relmap_have_binding;
}

bool
cluster_relmap_authority_x_held(void)
{
	return relmap_global_x;
}

bool
cluster_relmap_authority_v2_convert_clean(const ClusterRelmapV2Namespace *identity)
{
	if (relmap_local_lock || CritSectionCount != 0 || !relmap_global_x)
		abort();
	relmap_conversion_calls++;
	relmap_conversion_identity = *identity;
	if (relmap_conversion_drift == 1) epoch++;
	if (relmap_conversion_drift == 2) relmap_top_full += UINT64CONST(0x100000000);
	if (relmap_conversion_drift == 3) MyDatabaseId++;
	if (relmap_conversion_drift == 4) relmap_global_x = false;
	if (relmap_conversion_drift == 5) relmap_local_lock = true;
	if (relmap_conversion_drift == 6) relmap_in_transaction = false;
	if (relmap_conversion_drift == 7) ShutdownRequestPending = true;
	if (relmap_conversion_drift == 8) relmap_admission_delta = 1;
	if (relmap_conversion_drift == 9) ref.claim.claim_sha256[0]++;
	return relmap_conversion_ok;
}

#include "../../backend/cluster/cluster_relmap_owner.c"

typedef struct RelmapNativeWalFixture {
	ClusterRelmapV2Image image;
	ClusterWalSourceRef source;
	XLogReaderState reader;
} RelmapNativeWalFixture;

static void
relmap_wal_map_crc(RelMapFile *map)
{
	INIT_CRC32C(map->crc);
	COMP_CRC32C(map->crc, map, offsetof(RelMapFile, crc));
	FIN_CRC32C(map->crc);
}

/* Decode actual native bytes. A real retained visit must also authenticate
 * the source/CRC, cover the whole input and revalidate it; this fixture does
 * not replace those independent duties. No transaction/context providers. */
static bool
relmap_wal_fixture(RelmapNativeWalFixture *f, bool shared, bool with_top)
{
	uint8 wire[CLUSTER_RELMAP_V2_FILE_SIZE];
	ClusterRelmapV2Namespace expected;
	xl_relmap_update update = { 0 };
	XLogRecord header = { 0 };
	uint32 payload_length = MinSizeOfRelmapUpdate + sizeof(RelMapFile);
	size_t total = SizeOfXLogRecord + 5 + payload_length + (with_top ? 5 : 0);
	char *raw = calloc(1, total);
	uint8 *cursor;
	char *error = NULL;

	memset(f, 0, sizeof(*f));
	if (raw == NULL)
		return false;
	f->source.claim.identity.system_identifier = UINT64CONST(0x1122334455667788);
	memset(f->source.claim.identity.storage_uuid, 3, 16);
	memset(f->source.claim.identity.authority_uuid, 5, 16);
	f->source.claim.identity.origin_thread_id = 3;
	f->source.claim.identity.origin_node_id = 2;
	f->source.claim.identity.thread_claim_created_at = 23;
	f->source.claim.identity.origin_owner_incarnation = 101;
	f->source.claim.identity.root_lineage_seq = 9;
	f->source.claim.database_incarnation = 7;
	f->source.claim.max_config_generation = 5;
	memset(f->source.claim.claim_sha256, 7, 32);
	f->source.timeline = 2;
	f->image.identity.system_identifier = f->source.claim.identity.system_identifier;
	f->image.identity.database_incarnation = 7;
	f->image.identity.shared = shared;
	f->image.identity.dbid = shared ? InvalidOid : 42;
	f->image.committed_generation = 7;
	f->image.pending_generation = 8;
	f->image.pending_owner.node = 2;
	f->image.pending_owner.top_full_xid = UINT64CONST(0x400000123);
	f->image.pending_owner.membership_epoch = 91;
	f->image.pending_owner.admitted_incarnation = 101;
	f->image.pending_owner.boot_incarnation = 202;
	f->image.committed.magic = RELMAPPER_FILEMAGIC;
	f->image.committed.num_mappings = 1;
	f->image.committed.mappings[0].mapoid = 1259;
	f->image.committed.mappings[0].mapfilenumber = 40000;
	relmap_wal_map_crc(&f->image.committed);
	f->image.pending = f->image.committed;
	f->image.pending.mappings[0].mapfilenumber++;
	relmap_wal_map_crc(&f->image.pending);
	f->reader.ReadRecPtr = UINT64CONST(0x101000);
	f->reader.EndRecPtr = f->reader.ReadRecPtr + MAXALIGN(total);
	f->image.pending_owner.wal_end = f->reader.EndRecPtr;
	expected = f->image.identity;
	if (cluster_relmap_v2_encode(&f->image, wire, sizeof(wire)) != CLUSTER_RELMAP_V2_VALID
		|| cluster_relmap_v2_decode(wire, sizeof(wire), &expected, &f->image)
		   != CLUSTER_RELMAP_V2_VALID) {
		free(raw);
		return false;
	}
	update.dbid = f->image.identity.dbid;
	update.tsid = shared ? GLOBALTABLESPACE_OID : DEFAULTTABLESPACE_OID;
	update.nbytes = sizeof(RelMapFile);
	header.xl_tot_len = total;
	header.xl_xid = (uint32) f->image.pending_owner.top_full_xid;
	header.xl_rmid = RM_RELMAP_ID;
	header.xl_info = XLOG_RELMAP_UPDATE;
	cursor = (uint8 *) raw + SizeOfXLogRecord;
	if (with_top) {
		*cursor++ = XLR_BLOCK_ID_TOPLEVEL_XID;
		memcpy(cursor, &header.xl_xid, sizeof(TransactionId));
		cursor += sizeof(TransactionId);
	}
	*cursor++ = XLR_BLOCK_ID_DATA_LONG;
	memcpy(cursor, &payload_length, sizeof(payload_length));
	cursor += sizeof(payload_length);
	memcpy(cursor, &update, MinSizeOfRelmapUpdate);
	memcpy(cursor + MinSizeOfRelmapUpdate, &f->image.pending, sizeof(RelMapFile));
	INIT_CRC32C(header.xl_crc);
	COMP_CRC32C(header.xl_crc, raw + SizeOfXLogRecord, total - SizeOfXLogRecord);
	COMP_CRC32C(header.xl_crc, &header, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(header.xl_crc);
	memcpy(raw, &header, SizeOfXLogRecord);
	f->reader.record = calloc(1, DecodeXLogRecordRequiredSpace(total));
	if (f->reader.record == NULL) {
		free(raw);
		return false;
	}
	if (!DecodeXLogRecord(&f->reader, f->reader.record, (XLogRecord *) raw,
		f->reader.ReadRecPtr, &error)) {
		free(raw);
		free(f->reader.record);
		return false;
	}
	free(raw);
	f->reader.record->next_lsn = f->reader.EndRecPtr;
	f->reader.system_identifier = f->source.claim.identity.system_identifier;
	f->reader.cluster_expected_thread_id = f->source.claim.identity.origin_thread_id;
	f->reader.seg.ws_tli = f->source.timeline;
	return true;
}

UT_TEST(test_relmap_native_wal_matches_shared_and_database_pending)
{
	for (unsigned variant = 0; variant < 4; variant++) {
		RelmapNativeWalFixture f;
		UT_ASSERT(relmap_wal_fixture(&f, variant & 1, variant & 2));
		UT_ASSERT(cluster_relmap_owner_wal_matches_v2(&f.image, &f.source, &f.reader));
		free(f.reader.record);
	}
}

UT_TEST(test_relmap_native_wal_rejects_foreign_or_unqualified_source)
{
	for (unsigned fault = 0; fault < 15; fault++) {
		RelmapNativeWalFixture f;
		UT_ASSERT(relmap_wal_fixture(&f, false, false));
		if (fault == 0) f.source.claim.identity.system_identifier++;
		if (fault == 1) f.source.claim.database_incarnation++;
		if (fault == 2) f.source.claim.identity.origin_node_id++;
		if (fault == 3) f.source.claim.identity.origin_owner_incarnation++;
		if (fault == 4) f.source.claim.identity.origin_thread_id++;
		if (fault == 5) f.source.timeline++;
		if (fault == 6) f.reader.system_identifier++;
		if (fault == 7) f.reader.cluster_expected_thread_id++;
		if (fault == 8) f.reader.seg.ws_tli++;
		if (fault == 9) memset(f.source.claim.claim_sha256, 0, 32);
		if (fault == 10) memset(f.source.claim.identity.storage_uuid, 0, 16);
		if (fault == 11) memset(f.source.claim.identity.authority_uuid, 0, 16);
		if (fault == 12) f.source.claim.identity.root_lineage_seq = 0;
		if (fault == 13) f.source.claim.max_config_generation = 0;
		if (fault == 14) f.source.timeline = f.reader.seg.ws_tli = 0;
		UT_ASSERT(!cluster_relmap_owner_wal_matches_v2(&f.image, &f.source, &f.reader));
		free(f.reader.record);
	}
}

UT_TEST(test_relmap_native_wal_requires_exact_record_range_and_kind)
{
	for (unsigned fault = 0; fault < 11; fault++) {
		RelmapNativeWalFixture f;
		UT_ASSERT(relmap_wal_fixture(&f, false, false));
		if (fault == 0) f.reader.ReadRecPtr = 0;
		if (fault == 1) f.reader.EndRecPtr = f.reader.ReadRecPtr;
		if (fault == 2) f.reader.EndRecPtr = f.reader.ReadRecPtr - 1;
		if (fault == 3) f.reader.record->lsn++;
		if (fault == 4) f.reader.record->next_lsn++;
		if (fault == 5) f.image.pending_owner.wal_end++;
		if (fault == 6) f.reader.record->header.xl_rmid = RM_XLOG_ID;
		if (fault == 7) f.reader.record->header.xl_info = 0x10;
		if (fault == 8) f.reader.record->max_block_id = 0;
		if (fault == 9) f.reader.record->has_page_version_edge = true;
		if (fault == 10) f.reader.record->header.xl_tot_len =
			f.reader.EndRecPtr - f.reader.ReadRecPtr + 1;
		UT_ASSERT(!cluster_relmap_owner_wal_matches_v2(&f.image, &f.source, &f.reader));
		free(f.reader.record);
	}
}

UT_TEST(test_relmap_native_wal_requires_original_raw_and_top_xid)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		RelmapNativeWalFixture f;
		UT_ASSERT(relmap_wal_fixture(&f, false, true));
		if (fault == 0) f.reader.record->header.xl_xid++;
		if (fault == 1) f.reader.record->header.xl_xid = InvalidTransactionId;
		if (fault == 2) f.reader.record->header.xl_xid = FrozenTransactionId;
		if (fault == 3) f.reader.record->toplevel_xid++;
		if (fault == 4) f.reader.record->toplevel_xid = FrozenTransactionId;
		UT_ASSERT(!cluster_relmap_owner_wal_matches_v2(&f.image, &f.source, &f.reader));
		free(f.reader.record);
	}
}

UT_TEST(test_relmap_native_wal_requires_exact_namespace_and_native_length)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		RelmapNativeWalFixture f;
		xl_relmap_update update;
		UT_ASSERT(relmap_wal_fixture(&f, fault == 3, false));
		memcpy(&update, XLogRecGetData(&f.reader), MinSizeOfRelmapUpdate);
		if (fault == 0) update.dbid++;
		if (fault == 1) update.tsid = InvalidOid;
		if (fault == 2) update.tsid = GLOBALTABLESPACE_OID;
		if (fault == 3) update.tsid = DEFAULTTABLESPACE_OID;
		if (fault == 4) update.nbytes--;
		if (fault == 5) update.nbytes = -1;
		memcpy(XLogRecGetData(&f.reader), &update, MinSizeOfRelmapUpdate);
		if (fault == 6) f.reader.record->main_data_len--;
		if (fault == 7) f.reader.record->main_data_len++;
		UT_ASSERT(!cluster_relmap_owner_wal_matches_v2(&f.image, &f.source, &f.reader));
		free(f.reader.record);
	}
	{
		RelmapNativeWalFixture f;
		xl_relmap_update update;
		UT_ASSERT(relmap_wal_fixture(&f, false, false));
		memcpy(&update, XLogRecGetData(&f.reader), MinSizeOfRelmapUpdate);
		update.tsid = 7777; /* Native local map can use a custom tablespace. */
		memcpy(XLogRecGetData(&f.reader), &update, MinSizeOfRelmapUpdate);
		UT_ASSERT(cluster_relmap_owner_wal_matches_v2(&f.image, &f.source, &f.reader));
		free(f.reader.record);
	}
}

UT_TEST(test_relmap_native_wal_compares_every_native_map_byte)
{
	RelmapNativeWalFixture f;
	char *map;
	UT_ASSERT(relmap_wal_fixture(&f, false, false));
	map = XLogRecGetData(&f.reader) + MinSizeOfRelmapUpdate;
	for (size_t offset = 0; offset < sizeof(RelMapFile); offset++) {
		map[offset] ^= 1;
		UT_ASSERT(!cluster_relmap_owner_wal_matches_v2(&f.image, &f.source, &f.reader));
		map[offset] ^= 1;
	}
	/* A different valid map is also rejected, including inactive slot bytes. */
	for (unsigned fault = 0; fault < 2; fault++) {
		RelMapFile different = f.image.pending;
		different.mappings[fault == 0 ? 0 : MAX_MAPPINGS - 1].mapfilenumber++;
		relmap_wal_map_crc(&different);
		memcpy(map, &different, sizeof(different));
		UT_ASSERT(!cluster_relmap_owner_wal_matches_v2(&f.image, &f.source, &f.reader));
	}
	free(f.reader.record);
}

UT_TEST(test_relmap_native_wal_rejects_invalid_or_absent_pending_image)
{
	for (unsigned fault = 0; fault < 12; fault++) {
		RelmapNativeWalFixture f;
		UT_ASSERT(relmap_wal_fixture(&f, false, false));
		if (fault == 0) f.image.pending_generation = 0;
		if (fault == 1) f.image.pending_generation++;
		if (fault == 2) f.image.committed_generation = 0;
		if (fault == 3) f.image.pending_owner.top_full_xid = 2;
		if (fault == 4) f.image.pending_owner.admitted_incarnation = 0;
		if (fault == 5) f.image.pending_owner.boot_incarnation = 0;
		if (fault == 6) f.image.pending_owner.wal_end = 0;
		if (fault == 7) f.image.pending.crc++;
		if (fault == 8) f.image.committed.crc++;
		if (fault == 9) f.image.identity.shared = true;
		if (fault == 10) f.image.identity.system_identifier = 0;
		if (fault == 11) f.image.identity.database_incarnation = 0;
		UT_ASSERT(!cluster_relmap_owner_wal_matches_v2(&f.image, &f.source, &f.reader));
		free(f.reader.record);
	}
}

UT_TEST(test_relmap_native_wal_null_inputs_never_match)
{
	RelmapNativeWalFixture f;
	DecodedXLogRecord *record;
	char *data;
	UT_ASSERT(relmap_wal_fixture(&f, false, false));
	UT_ASSERT(!cluster_relmap_owner_wal_matches_v2(NULL, &f.source, &f.reader));
	UT_ASSERT(!cluster_relmap_owner_wal_matches_v2(&f.image, NULL, &f.reader));
	UT_ASSERT(!cluster_relmap_owner_wal_matches_v2(&f.image, &f.source, NULL));
	record = f.reader.record;
	f.reader.record = NULL;
	UT_ASSERT(!cluster_relmap_owner_wal_matches_v2(&f.image, &f.source, &f.reader));
	f.reader.record = record;
	data = record->main_data;
	record->main_data = NULL;
	UT_ASSERT(!cluster_relmap_owner_wal_matches_v2(&f.image, &f.source, &f.reader));
	record->main_data = data;
	free(record);
}

UT_TEST(test_relmap_native_wal_is_readonly_and_does_not_qualify_missing_era)
{
	RelmapNativeWalFixture f, before;
	void *record_before;
	UT_ASSERT(relmap_wal_fixture(&f, false, true));
	before = f;
	record_before = malloc(f.reader.record->size);
	UT_ASSERT_NOT_NULL(record_before);
	memcpy(record_before, f.reader.record, f.reader.record->size);
	UT_ASSERT(cluster_relmap_owner_wal_matches_v2(&f.image, &f.source, &f.reader));
	UT_ASSERT_EQ(memcmp(&f, &before, sizeof(f)), 0);
	UT_ASSERT_EQ(memcmp(f.reader.record, record_before, f.reader.record->size), 0);
	/* These independent fields are not present in this native record/claim.
	 * Matching raw XID cannot certify which original FullXid era owns it. */
	f.image.pending_owner.top_full_xid += UINT64CONST(0x100000000);
	f.image.pending_owner.membership_epoch++;
	f.image.pending_owner.boot_incarnation++;
	UT_ASSERT(cluster_relmap_owner_wal_matches_v2(&f.image, &f.source, &f.reader));
	free(record_before);
	free(f.reader.record);
}

static void
relmap_owner_fixture(void)
{
	fixture();
	CritSectionCount = 0;
	MyProcPid = 79;
	MyDatabaseId = 42;
	native_ctl.InsertTimeLineID = 1;
	relmap_global_x = relmap_in_transaction = true;
	relmap_local_lock = flush_probe_recovery = false;
	relmap_assign_calls = 0;
	relmap_advance_epoch_on_assign = false;
	relmap_top_full = UINT64CONST(0x400000123);
	relmap_retention_held = relmap_have_binding = relmap_binding_drift = false;
	relmap_active_owner = false;
	relmap_stage_lock_window = false;
	relmap_binding_owner_fault = 0;
	relmap_conversion_calls = relmap_conversion_drift = 0;
	relmap_conversion_ok = true;
	memset(&relmap_conversion_identity, 0, sizeof(relmap_conversion_identity));
	relmap_retention_acquires = relmap_binding_reads = 0;
	memset(&relmap_binding, 0, sizeof(relmap_binding));
	relmap_binding.segment_id = 12;
	relmap_binding.xid = (uint32) relmap_top_full;
	relmap_binding.slot_offset = 3;
	relmap_binding.origin_instance = 1;
	relmap_binding.publish_state = CLUSTER_CANONICAL_TXN_PUBLISHED;
	relmap_binding.active_lsn = 456;
}

UT_TEST(test_relmap_conversion_uses_original_claim_and_actual_namespace)
{
	for (int shared = 0; shared < 2; shared++) {
		ClusterRelmapOwnerSampleV2 sample, before;
		relmap_owner_fixture();
		/* Shared conversion has no dependency on an attached database. */
		if (shared) MyDatabaseId = InvalidOid;
		UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
		before = sample;
		UT_ASSERT(cluster_relmap_owner_convert_clean_v2(&sample, shared));
		UT_ASSERT_EQ(relmap_conversion_calls, 1);
		UT_ASSERT_EQ(relmap_conversion_identity.system_identifier,
			sample.writer.ref.claim.identity.system_identifier);
		UT_ASSERT_EQ(relmap_conversion_identity.database_incarnation,
			sample.writer.ref.claim.database_incarnation);
		UT_ASSERT_EQ(relmap_conversion_identity.dbid, shared ? InvalidOid : MyDatabaseId);
		UT_ASSERT_EQ(relmap_conversion_identity.shared, shared);
		UT_ASSERT_EQ(relmap_assign_calls, 1);
		UT_ASSERT_EQ(memcmp(&sample, &before, sizeof(sample)), 0);
	}
}

UT_TEST(test_relmap_conversion_refuses_before_storage_in_unsafe_or_changed_owner)
{
	for (unsigned fault = 0; fault < 12; fault++) {
		ClusterRelmapOwnerSampleV2 sample;
		relmap_owner_fixture();
		UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
		if (fault == 0) relmap_local_lock = true;
		if (fault == 1) CritSectionCount = 1;
		if (fault == 2) relmap_global_x = false;
		if (fault == 3) relmap_in_transaction = false;
		if (fault == 4) MyBackendType = B_BG_WRITER;
		if (fault == 5) epoch++;
		if (fault == 6) relmap_top_full += UINT64CONST(0x100000000);
		if (fault == 7) flush_probe_recovery = true;
		if (fault == 8) MyDatabaseId = InvalidOid;
		if (fault == 9) relmap_admission_delta = 1;
		if (fault == 10) sample.producer_pid++;
		UT_ASSERT(!cluster_relmap_owner_convert_clean_v2(fault == 11 ? NULL : &sample, false));
		UT_ASSERT_EQ(relmap_conversion_calls, 0);
		UT_ASSERT_EQ(relmap_assign_calls, 1);
	}
}

UT_TEST(test_relmap_conversion_storage_refusal_is_not_permission)
{
	ClusterRelmapOwnerSampleV2 sample;
	relmap_owner_fixture();
	UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
	relmap_conversion_ok = false;
	UT_ASSERT(!cluster_relmap_owner_convert_clean_v2(&sample, true));
	UT_ASSERT_EQ(relmap_conversion_calls, 1);
	UT_ASSERT_EQ(relmap_assign_calls, 1);
}

UT_TEST(test_relmap_conversion_rechecks_owner_and_database_after_storage)
{
	for (unsigned drift = 1; drift <= 9; drift++) {
		ClusterRelmapOwnerSampleV2 sample;
		relmap_owner_fixture();
		UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
		relmap_conversion_drift = drift;
		UT_ASSERT(!cluster_relmap_owner_convert_clean_v2(&sample, false));
		UT_ASSERT_EQ(relmap_conversion_calls, 1);
		UT_ASSERT_EQ(relmap_assign_calls, 1);
	}
}

UT_TEST(test_relmap_capture_preserves_original_full_identity)
{
	ClusterRelmapOwnerSampleV2 sample;
	relmap_owner_fixture();
	UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
	UT_ASSERT_EQ(relmap_assign_calls, 1);
	UT_ASSERT_EQ(sample.owner.top_full_xid, UINT64CONST(0x400000123));
	UT_ASSERT_EQ(sample.owner.node, 0);
	UT_ASSERT_EQ(sample.owner.membership_epoch, 7);
	UT_ASSERT_EQ(sample.owner.boot_incarnation, 99);
	UT_ASSERT_EQ(sample.owner.admitted_incarnation, 99);
	UT_ASSERT_EQ(sample.owner.wal_end, 0);
	UT_ASSERT_EQ(sample.producer_pid, MyProcPid);
	UT_ASSERT_EQ(memcmp(&sample.writer.ref, &ref, sizeof(ref)), 0);
	UT_ASSERT(cluster_relmap_owner_matches_v2(&sample));
	UT_ASSERT_EQ(relmap_assign_calls, 1);
	UT_ASSERT_EQ(sync_calls + rename_calls, 0);
}

UT_TEST(test_relmap_capture_refuses_unsafe_context_before_assigning)
{
	for (unsigned fault = 0; fault < 12; fault++) {
		ClusterRelmapOwnerSampleV2 sample, zero = { 0 };
		relmap_owner_fixture();
		if (fault == 0)
			CritSectionCount = 1;
		if (fault == 1)
			relmap_local_lock = true;
		if (fault == 2)
			relmap_global_x = false;
		if (fault == 3)
			relmap_in_transaction = false;
		if (fault == 4)
			MyBackendType = B_BG_WRITER;
		if (fault == 5)
			ShutdownRequestPending = true;
		if (fault == 6)
			cluster_enabled = false;
		if (fault == 7)
			cluster_shared_config = false;
		if (fault == 8)
			cluster_node_id = -1;
		if (fault == 9)
			cluster_node_id = CLUSTER_MAX_NODES;
		if (fault == 10)
			MyProcPid = 0;
		if (fault == 11)
			flush_probe_recovery = true;
		memset(&sample, 0x55, sizeof(sample));
		UT_ASSERT(!cluster_relmap_owner_capture_v2(&sample));
		UT_ASSERT_EQ(memcmp(&sample, &zero, sizeof(zero)), 0);
		UT_ASSERT_EQ(relmap_assign_calls, 0);
		cluster_node_id = 0;
	}
	relmap_owner_fixture();
	UT_ASSERT(!cluster_relmap_owner_capture_v2(NULL));
	UT_ASSERT_EQ(relmap_assign_calls, 0);
}

UT_TEST(test_relmap_writer_is_sampled_after_original_top_assignment)
{
	ClusterRelmapOwnerSampleV2 sample;
	relmap_owner_fixture();
	relmap_advance_epoch_on_assign = true;
	UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
	UT_ASSERT_EQ(sample.writer.epoch, 8);
	UT_ASSERT_EQ(sample.owner.membership_epoch, 8);
	UT_ASSERT_EQ(relmap_assign_calls, 1);
}

UT_TEST(test_relmap_invalid_top_or_unqualified_writer_cannot_be_captured)
{
	for (unsigned fault = 0; fault < 9; fault++) {
		ClusterRelmapOwnerSampleV2 sample, zero = { 0 };
		relmap_owner_fixture();
		if (fault == 0)
			relmap_top_full = 0;
		if (fault == 1)
			relmap_top_full = UINT64CONST(0x400000002);
		if (fault == 2)
			have_ref = false;
		if (fault == 3)
			member = false;
		if (fault == 4)
			self_fenced = true;
		if (fault == 5)
			ref.claim.identity.origin_owner_incarnation++;
		if (fault == 6)
			relmap_admission_delta = 1;
		if (fault == 7)
			epoch = 0; /* native writer refuses; codec still allows zero */
		if (fault == 8)
			active = false;
		memset(&sample, 0x55, sizeof(sample));
		UT_ASSERT(!cluster_relmap_owner_capture_v2(&sample));
		UT_ASSERT_EQ(memcmp(&sample, &zero, sizeof(zero)), 0);
		UT_ASSERT_EQ(relmap_assign_calls, 1);
	}
}

UT_TEST(test_relmap_revalidation_never_adopts_new_writer_or_reassigns_top)
{
	for (unsigned fault = 0; fault < 14; fault++) {
		ClusterRelmapOwnerSampleV2 sample, before;
		relmap_owner_fixture();
		UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
		before = sample;
		if (fault == 0)
			epoch++;
		if (fault == 1)
			ref.claim.claim_sha256[0]++;
		if (fault == 2)
			ref.claim.database_incarnation++;
		if (fault == 3)
			native_ctl.InsertTimeLineID++;
		if (fault == 4)
			incarnation++;
		if (fault == 5)
			relmap_admission_delta = 1;
		if (fault == 6)
			relmap_top_full += UINT64CONST(0x100000000); /* same raw XID */
		if (fault == 7)
			MyProcPid++;
		if (fault == 8)
			relmap_global_x = false;
		if (fault == 9)
			self_fenced = true;
		if (fault == 10)
			member = false;
		if (fault == 11)
			have_ref = false;
		if (fault == 12)
			CritSectionCount = 1;
		if (fault == 13)
			ShutdownRequestPending = true;
		UT_ASSERT(!cluster_relmap_owner_matches_v2(&sample));
		UT_ASSERT_EQ(memcmp(&sample, &before, sizeof(sample)), 0);
		UT_ASSERT_EQ(relmap_assign_calls, 1);
		UT_ASSERT_EQ(sync_calls + rename_calls, 0);
	}
}

UT_TEST(test_relmap_sample_fields_and_original_lock_context_are_exact)
{
	ClusterRelmapOwnerSampleV2 sample;

	for (unsigned fault = 0; fault < 7; fault++) {
		relmap_owner_fixture();
		UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
		if (fault == 0)
			sample.owner.node++;
		if (fault == 1)
			sample.owner.boot_incarnation++;
		if (fault == 2)
			sample.owner.admitted_incarnation++;
		if (fault == 3)
			sample.owner.membership_epoch++;
		if (fault == 4)
			sample.owner.wal_end = 42;
		if (fault == 5)
			sample.writer.startup_first_lsn = 42;
		if (fault == 6)
			sample.owner.top_full_xid++;
		UT_ASSERT(!cluster_relmap_owner_matches_v2(&sample));
	}
	relmap_owner_fixture();
	UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
	relmap_local_lock = true; /* recheck must not allocate under this lock */
	UT_ASSERT(cluster_relmap_owner_matches_v2(&sample));
	UT_ASSERT_EQ(relmap_assign_calls, 1);
	UT_ASSERT(!cluster_relmap_owner_matches_v2(NULL));
}

static ClusterRelmapV2Namespace
relmap_owner_namespace(void)
{
	ClusterRelmapV2Namespace identity = { 0 };
	identity.system_identifier = ref.claim.identity.system_identifier;
	identity.database_incarnation = ref.claim.database_incarnation;
	identity.dbid = MyDatabaseId;
	return identity;
}

static void
relmap_owner_ready(ClusterRelmapRetentionDirectory *directory)
{
	memset(directory, 0, sizeof(*directory));
	UT_ASSERT_EQ(cluster_relmap_retention_build_start(directory,
		ref.claim.identity.system_identifier, ref.claim.database_incarnation, 0, 7,
		relmap_top_full - 100, relmap_top_full + 100), CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT_EQ(cluster_relmap_retention_build_finish(directory), CLUSTER_RELMAP_RETENTION_OK);
}

UT_TEST(test_relmap_owner_retention_uses_original_key_and_published_receipt)
{
	ClusterRelmapOwnerSampleV2 sample;
	ClusterRelmapRetentionDirectory directory;
	ClusterRelmapV2Namespace identity;
	const ClusterRelmapRetentionPin *pin;

	relmap_owner_fixture();
	UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
	identity = relmap_owner_namespace();
	relmap_owner_ready(&directory);
	relmap_have_binding = true;
	UT_ASSERT_EQ(cluster_relmap_owner_retention_begin_v2(&sample, &identity, 8,
		&directory, &relmap_retention_lock), CLUSTER_RELMAP_RETENTION_OK);
	pin = &directory.pins[0];
	UT_ASSERT(pin->occupied && pin->has_tt_binding && !pin->may_be_staged);
	UT_ASSERT_EQ(pin->wal_end, 0);
	UT_ASSERT_EQ(pin->key.top_full_xid, sample.owner.top_full_xid);
	UT_ASSERT_EQ(pin->key.membership_epoch, sample.owner.membership_epoch);
	UT_ASSERT_EQ(pin->key.admitted_incarnation, sample.owner.admitted_incarnation);
	UT_ASSERT_EQ(pin->key.boot_incarnation, sample.owner.boot_incarnation);
	UT_ASSERT_EQ(pin->key.origin, sample.owner.node);
	UT_ASSERT_EQ(pin->key.generation, 8);
	UT_ASSERT_EQ(pin->key.identity.system_identifier, identity.system_identifier);
	UT_ASSERT_EQ(pin->key.identity.database_incarnation, identity.database_incarnation);
	UT_ASSERT_EQ(pin->key.identity.dbid, MyDatabaseId);
	UT_ASSERT(!pin->key.identity.shared);
	UT_ASSERT_EQ(memcmp(&pin->binding, &relmap_binding, sizeof(relmap_binding)), 0);
	UT_ASSERT_EQ(relmap_assign_calls, 1);
	UT_ASSERT_EQ(relmap_binding_reads, 1);
	UT_ASSERT_EQ(relmap_retention_acquires, 1);
	UT_ASSERT(!relmap_retention_held);
	UT_ASSERT_EQ(sync_calls + rename_calls, 0);
	identity.shared = true;
	identity.dbid = InvalidOid;
	UT_ASSERT_EQ(cluster_relmap_owner_retention_begin_v2(&sample, &identity, 8,
		&directory, &relmap_retention_lock), CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT(directory.pins[1].key.identity.shared && directory.pins[1].has_tt_binding);
}

UT_TEST(test_relmap_owner_retention_absent_binding_stays_unresolved)
{
	ClusterRelmapOwnerSampleV2 sample;
	ClusterRelmapRetentionDirectory directory;
	ClusterRelmapV2Namespace identity;

	relmap_owner_fixture();
	UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
	identity = relmap_owner_namespace();
	relmap_owner_ready(&directory);
	UT_ASSERT_EQ(cluster_relmap_owner_retention_begin_v2(&sample, &identity, 8,
		&directory, &relmap_retention_lock), CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT(directory.pins[0].occupied && !directory.pins[0].has_tt_binding);
	UT_ASSERT_EQ(cluster_relmap_retention_tt_release_check(&directory, &relmap_binding),
		CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
	UT_ASSERT_EQ(relmap_binding_reads, 1);
	UT_ASSERT_EQ(relmap_assign_calls, 1);
}

UT_TEST(test_relmap_owner_retention_refuses_stale_or_unsafe_owner)
{
	for (unsigned fault = 0; fault < 12; fault++) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapRetentionDirectory directory, before;
		ClusterRelmapV2Namespace identity;

		relmap_owner_fixture();
		UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
		identity = relmap_owner_namespace();
		relmap_owner_ready(&directory);
		before = directory;
		if (fault == 0) relmap_local_lock = true;
		if (fault == 1) relmap_retention_held = true;
		if (fault == 2) CritSectionCount = 1;
		if (fault == 3) relmap_global_x = false;
		if (fault == 4) relmap_in_transaction = false;
		if (fault == 5) ShutdownRequestPending = true;
		if (fault == 6) MyProcPid++;
		if (fault == 7) epoch++;
		if (fault == 8) ref.claim.claim_sha256[0]++;
		if (fault == 9) relmap_top_full += UINT64CONST(0x100000000);
		if (fault == 10) flush_probe_recovery = true;
		if (fault == 11) MyBackendType = B_BG_WRITER;
		UT_ASSERT_NE(cluster_relmap_owner_retention_begin_v2(&sample, &identity, 8,
			&directory, &relmap_retention_lock), CLUSTER_RELMAP_RETENTION_OK);
		UT_ASSERT_EQ(memcmp(&directory, &before, sizeof(directory)), 0);
		UT_ASSERT_EQ(relmap_binding_reads + relmap_retention_acquires, 0);
		UT_ASSERT_EQ(relmap_assign_calls, 1);
	}
}

UT_TEST(test_relmap_owner_retention_rejects_foreign_namespace_and_arguments)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapRetentionDirectory directory, before;
		ClusterRelmapV2Namespace identity;
		const ClusterRelmapOwnerSampleV2 *sample_arg = &sample;
		const ClusterRelmapV2Namespace *identity_arg = &identity;
		ClusterRelmapRetentionDirectory *directory_arg = &directory;
		LWLock *lock_arg = &relmap_retention_lock;

		relmap_owner_fixture();
		UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
		identity = relmap_owner_namespace();
		relmap_owner_ready(&directory);
		before = directory;
		if (fault == 0) identity.system_identifier++;
		if (fault == 1) identity.database_incarnation++;
		if (fault == 2) identity.dbid++;
		if (fault == 3) identity.shared = true;
		if (fault == 4) identity.dbid = InvalidOid;
		if (fault == 5) sample_arg = NULL;
		if (fault == 6) identity_arg = NULL;
		if (fault == 7) directory_arg = NULL;
		if (fault == 8) lock_arg = NULL;
		if (fault == 9) lock_arg = RelationMappingLock;
		UT_ASSERT_NE(cluster_relmap_owner_retention_begin_v2(sample_arg, identity_arg, 8,
			directory_arg, lock_arg), CLUSTER_RELMAP_RETENTION_OK);
		UT_ASSERT_EQ(memcmp(&directory, &before, sizeof(directory)), 0);
		UT_ASSERT_EQ(relmap_binding_reads + relmap_retention_acquires, 0);
	}
}

UT_TEST(test_relmap_owner_retention_rechecks_after_original_binding_read)
{
	ClusterRelmapOwnerSampleV2 sample;
	ClusterRelmapRetentionDirectory directory, before;
	ClusterRelmapV2Namespace identity;

	relmap_owner_fixture();
	UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
	identity = relmap_owner_namespace();
	relmap_owner_ready(&directory);
	before = directory;
	relmap_have_binding = relmap_binding_drift = true;
	UT_ASSERT_EQ(cluster_relmap_owner_retention_begin_v2(&sample, &identity, 8,
		&directory, &relmap_retention_lock), CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
	UT_ASSERT_EQ(memcmp(&directory, &before, sizeof(directory)), 0);
	UT_ASSERT_EQ(relmap_binding_reads, 1);
	UT_ASSERT_EQ(relmap_retention_acquires, 0);
	UT_ASSERT_EQ(relmap_assign_calls, 1);
}

UT_TEST(test_relmap_owner_retention_preserves_core_refusals)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapRetentionDirectory directory, before;
		ClusterRelmapV2Namespace identity;
		uint64 generation = 8;
		ClusterRelmapRetentionResult expected;

		relmap_owner_fixture();
		UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
		identity = relmap_owner_namespace();
		relmap_owner_ready(&directory);
		expected = CLUSTER_RELMAP_RETENTION_UNAVAILABLE;
		if (fault == 0) directory.state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
		if (fault == 1) directory.state = CLUSTER_RELMAP_RETENTION_BUILDING;
		if (fault == 2) { directory.origin++; expected = CLUSTER_RELMAP_RETENTION_INVALID; }
		if (fault == 3) { directory.native_floor = relmap_top_full + 1; expected = CLUSTER_RELMAP_RETENTION_NATIVE_GONE; }
		if (fault == 4) { directory.last_issued = relmap_top_full - 1; expected = CLUSTER_RELMAP_RETENTION_INVALID; }
		if (fault == 5) { generation = 1; expected = CLUSTER_RELMAP_RETENTION_INVALID; }
		if (fault == 6) { relmap_have_binding = true; relmap_binding.xid++; expected = CLUSTER_RELMAP_RETENTION_INVALID; }
		if (fault == 7) {
			UT_ASSERT_EQ(cluster_relmap_owner_retention_begin_v2(&sample, &identity, 8,
				&directory, &relmap_retention_lock), CLUSTER_RELMAP_RETENTION_OK);
			directory.pins[0].may_be_staged = true;
			directory.pins[0].wal_end = 600;
			expected = CLUSTER_RELMAP_RETENTION_RETAINED;
		}
		if (fault == 8) {
			directory.last_issued = relmap_top_full + UINT64CONST(0x80000000);
			expected = CLUSTER_RELMAP_RETENTION_XID_WINDOW;
		}
		if (fault == 9) {
			ClusterRelmapRetentionTruncate reservation;
			UT_ASSERT_EQ(cluster_relmap_retention_truncate_begin(&directory, relmap_top_full + 1,
				&reservation), CLUSTER_RELMAP_RETENTION_OK);
			expected = CLUSTER_RELMAP_RETENTION_NATIVE_GONE;
		}
		before = directory;
		UT_ASSERT_EQ(cluster_relmap_owner_retention_begin_v2(&sample, &identity, generation,
			&directory, &relmap_retention_lock), expected);
		UT_ASSERT_EQ(memcmp(&directory, &before, sizeof(directory)), 0);
		UT_ASSERT(!relmap_retention_held);
		UT_ASSERT_EQ(relmap_assign_calls, 1);
	}
}
