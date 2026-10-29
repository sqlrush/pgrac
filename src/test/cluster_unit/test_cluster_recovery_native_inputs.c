/* Native effects through real ROOT/claim/anchor files and XLogReader.
 * Only the between-pass fault injector wraps a call boundary; both passes
 * still execute the original complete cold census.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */
int control_fixture_main(int argc, char **argv);
#define main control_fixture_main
#include "test_cluster_control_root.c"
#undef main
#include "catalog/storage_xlog.h"
#include "access/multixact.h"
#include "access/clog.h"
#include "access/commit_ts.h"
#include "cluster/cluster_native_startup.h"
#include "cluster/cluster_recovery_native_inventory.h"
#include "cluster/cluster_side_undo.h"
#include "cluster/cluster_recovery_record_hash.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "cluster/storage/cluster_undo_alloc.h"
#include "../../backend/cluster/cluster_uba.c"
#include "test_cluster_heap_small_scn.inc"
#include "test_cluster_recovery_relmap_fixture.h"
#include "test_cluster_recovery_prepare_fixture.h"

static unsigned inventory_census_calls;
static void (*inventory_between_passes)(void);
extern bool cluster_wal_inputs_relmap_clog_applied_v1(
	const ClusterRecoveryRelmapSourceV1 *source, const XLogReaderState *reader);
extern bool cluster_recovery_relmap_history_pending_applied_pages_v1(
	ClusterRecoveryRelmapHistory *history, const ClusterRelmapV2Image *pending,
	ClusterRecoveryRelmapPendingNativePagesV1 *out, ClusterRecoveryNativeInventoryDiag *diag);
static bool episode_applied_match(const ClusterRecoveryRelmapSourceV1 *source,
	const XLogReaderState *reader);

static ClusterControlRootResult
inventory_test_census(ClusterWalInputsV1 *inputs, ClusterWalCensusVisitorV1 visitor,
					  ClusterWalCensusSourceVisitorV1 source_visitor, void *arg, uint64 *records,
					  RfPageProofDetailV1 *detail)
{
	ClusterControlRootResult result;

	inventory_census_calls++;
	result
		= cluster_wal_inputs_cold_census_v1(inputs, visitor, source_visitor, arg, records, detail);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY && inventory_census_calls == 1
		&& inventory_between_passes != NULL)
		inventory_between_passes();
	return result;
}

#define cluster_wal_inputs_cold_census_v1 inventory_test_census
#define cluster_wal_inputs_relmap_clog_applied_v1 episode_applied_match
#include "../../backend/cluster/cluster_recovery_native_inventory.c"
#undef cluster_wal_inputs_relmap_clog_applied_v1
#undef cluster_wal_inputs_cold_census_v1
static ClusterRecoveryRootsResult census_current_read(ClusterRecoveryRootSet *roots,
	uint32 index, uint64 offset, void *buffer, size_t length);
static void census_namespace_interrupts(void);
static ClusterRecoveryGraphResult census_tx_test_at(const ClusterRecoveryReferenceGraph *graph,
	uint32 index, ClusterRecoveryGraphRecord *out);
static ClusterRecoveryGraphResult census_tx_test_recheck_at(ClusterRecoveryReferenceGraph *, uint32);
static ClusterRecoveryGraphResult census_dependency_test_at(const ClusterRecoveryReferenceGraph *,
 uint32,ClusterRecoveryGraphDependency *);
#define cluster_recovery_reference_graph_dependency_at census_dependency_test_at
#define cluster_recovery_reference_graph_at census_tx_test_at
#define cluster_recovery_reference_graph_recheck_at census_tx_test_recheck_at
#define HandleStartupProcInterrupts census_namespace_interrupts
#define cluster_recovery_root_set_read_current_relation census_current_read
static ClusterRecoveryGraphResult census_ref_test_find(const ClusterRecoveryReferenceGraph *,
 const ClusterRecoveryTxnKey *,UBA,ClusterRecoveryGraphDependency *);
#define cluster_recovery_reference_graph_dependency_find census_ref_test_find
static ClusterRecoveryNativeRecordObservationV1 census_native_test_record(
 ClusterRecoveryNativeInventory *, uint64);
#define cluster_recovery_native_inventory_record_v1 census_native_test_record
#include "../../backend/cluster/cluster_recovery_census.c"
#undef cluster_recovery_native_inventory_record_v1
#undef cluster_recovery_reference_graph_dependency_find
#undef cluster_recovery_root_set_read_current_relation
#undef HandleStartupProcInterrupts
#undef cluster_recovery_reference_graph_at
#undef cluster_recovery_reference_graph_recheck_at
#undef cluster_recovery_reference_graph_dependency_at

static ClusterWalSourceRef inventory_fault_source;
static XLogRecPtr inventory_commit_start, inventory_commit_previous;
static unsigned inventory_fault;
static unsigned inventory_extra;
static unsigned inventory_binding_case;
static XLogRecPtr inventory_extra_start, inventory_extra_previous;
static XLogRecPtr inventory_mx_start, inventory_mx_previous;
static XLogRecPtr inventory_slru_start[4], inventory_slru_previous[4];
static unsigned inventory_slru_fault;
static unsigned inventory_relmap_fault;
static unsigned inventory_prepare_fault;
static unsigned inventory_terminal_mode;
static WalTestRecord inventory_write_relmap_fold_sequence(const ClusterWalSourceRef *source,
	WalTestRecord next, unsigned node);

/* These small records stay within the original checkpoint's first WAL page.
 * Header, main-data framing and CRC are actual native-reader inputs. */
static WalTestRecord
inventory_write_record(const ClusterWalSourceRef *source, XLogRecPtr at, XLogRecPtr previous,
					   RmgrId rmid, uint8 info, const void *payload, uint32 length)
{
	uint8 bytes[2048] = { 0 };
	size_t framing = length <= UINT8_MAX ? 2 : 5;
	XLogRecord record = { 0 };
	char path[MAXPGPATH];
	int fd;

	UT_ASSERT(length <= sizeof(bytes) - SizeOfXLogRecord - framing);
	record.xl_tot_len = SizeOfXLogRecord + framing + length;
	record.xl_prev = previous;
	record.xl_rmid = rmid;
	record.xl_info = info;
	record.xl_xid = 4195136;
	record.xl_scn = 5906;
	bytes[SizeOfXLogRecord]
		= length <= UINT8_MAX ? XLR_BLOCK_ID_DATA_SHORT : XLR_BLOCK_ID_DATA_LONG;
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

/* A real FSM record, including the original fixed PageVersion wire edge. */
static WalTestRecord
inventory_write_fsm(const ClusterWalSourceRef *source, XLogRecPtr at, XLogRecPtr previous,
					BlockNumber block)
{
	uint8 bytes[256] = { 0 };
	uint8 *edge = bytes + SizeOfXLogRecord;
	XLogRecord record = { 0 };
	XLogRecordBlockHeader header = { .id = 0, .fork_flags = FSM_FORKNUM, .data_length = 0 };
	RelFileLocator locator = { 1663, 5, 16386 };
	size_t used = SizeOfXLogRecord + 64;
	char path[MAXPGPATH];
	int fd;

	edge[0] = XLR_BLOCK_ID_PAGE_VERSION_EDGE;
	edge[1] = XLR_PAGE_VERSION_EDGE_FORMAT_V1;
	edge[2] = 1;
	edge[3] = XLR_PAGE_VERSION_EDGE_ENTRY_SIZE;
	edge[8] = 31; /* LE result_token */
	edge[17] = RF_PAGE_CLASS_REBUILDABLE_FSM;
	edge[18] = edge[19] = RF_PAGE_STATE_REBUILDABLE;
	memcpy(bytes + used, &header, SizeOfXLogRecordBlockHeader);
	used += SizeOfXLogRecordBlockHeader;
	memcpy(bytes + used, &locator, sizeof(locator));
	used += sizeof(locator);
	memcpy(bytes + used, &block, sizeof(block));
	used += sizeof(block);
	record.xl_tot_len = used;
	record.xl_prev = previous;
	record.xl_rmid = RM_GENERIC_ID;
	record.xl_scn = 5906;
	INIT_CRC32C(record.xl_crc);
	COMP_CRC32C(record.xl_crc, bytes + SizeOfXLogRecord, used - SizeOfXLogRecord);
	COMP_CRC32C(record.xl_crc, &record, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(record.xl_crc);
	memcpy(bytes, &record, SizeOfXLogRecord);
	v2_checkpoint_wal_path(&source->claim.identity, at, source->timeline, path);
	UT_ASSERT(at % XLOG_BLCKSZ + used < XLOG_BLCKSZ);
	fd = open(path, O_WRONLY);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pwrite(fd, bytes, used, at % wal_segment_size), used);
	UT_ASSERT_EQ(close(fd), 0);
	return (WalTestRecord){ 1, MAXALIGN(at + used), at, record.xl_crc };
}

static WalTestRecord
inventory_write_commit(const ClusterWalSourceRef *source, XLogRecPtr at, XLogRecPtr previous,
					   TimestampTz time, bool unsupported)
{
	uint8 payload[128] = { 0 };
	uint32 xinfo = XACT_XINFO_HAS_INVALS | XACT_XINFO_HAS_SCN | XACT_COMPLETION_FORCE_SYNC_COMMIT;
	SharedInvalidationMessage msgs[2] = { 0 };
	xl_xact_scn scn = { 5906 };
	int nmsgs = 2;
	size_t used = 0;
	if (inventory_terminal_mode == 21)
		xinfo |= XACT_XINFO_HAS_TT_COMMIT;

	msgs[0].id = SHAREDINVALRELCACHE_ID;
	msgs[0].rc.dbId = 5;
	msgs[0].rc.relId = 16385;
	msgs[1].id = SHAREDINVALCATALOG_ID;
	memcpy(payload + used, &time, sizeof(time));
	used += sizeof(time);
	memcpy(payload + used, &xinfo, sizeof(xinfo));
	used += sizeof(xinfo);
	memcpy(payload + used, &nmsgs, sizeof(nmsgs));
	used += sizeof(nmsgs);
	memcpy(payload + used, msgs, sizeof(msgs));
	used += sizeof(msgs);
	memcpy(payload + used, &scn, sizeof(scn));
	used += sizeof(scn);
	if (inventory_terminal_mode == 21) {
		xl_xact_tt_commit tt = { 0 };
		tt.instance = source->claim.identity.origin_thread_id;
		tt.segment_id = (tt.instance - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
		tt.xid = 4195136;
		tt.wrap = 1;
		tt.commit_scn = 5906;
		tt.format_version = CLUSTER_XACT_TT_COMMIT_VERSION;
		memcpy(payload + used, &tt, sizeof(tt));
		used += sizeof(tt);
	}
	return inventory_write_record(
		source, at, previous, RM_XACT_ID,
		unsupported ? XLOG_XACT_PREPARE : XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO, payload, used);
}

static WalTestRecord
inventory_write_bound_head(const ClusterWalSourceRef *source, WalTestRecord previous, unsigned node)
{
	xl_undo_tt_slot_bind binding = { 0 };
	xl_undo_tt_slot_set_head head = { 0 };
	WalTestRecord next = previous;
	bool missing = inventory_binding_case == 1 || (inventory_binding_case == 8 && node == 127);

	binding.segment_id = node * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	binding.instance = node + 1;
	binding.xid = 4195136;
	binding.wrap = 1;
	binding.format_version = CLUSTER_UNDO_TT_BIND_VERSION;
	head.segment_id = binding.segment_id;
	head.instance = binding.instance;
	head.xid = binding.xid;
	head.wrap = binding.wrap;
	head.first_undo_block = uba_encode(binding.segment_id, 1, 0, 0);
	if (inventory_binding_case == 7)
		next = inventory_write_record(source, next.exclusive_end, next.record_start,
									  RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_SET_HEAD, &head,
									  sizeof(head));
	if (!missing)
		next = inventory_write_record(source, next.exclusive_end, next.record_start,
									  RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_BIND, &binding,
									  sizeof(binding));
	if (inventory_binding_case == 2 || inventory_binding_case == 3) {
		if (inventory_binding_case == 2)
			binding.xid++;
		else
			binding.wrap++;
		next = inventory_write_record(source, next.exclusive_end, next.record_start,
									  RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_BIND, &binding,
									  sizeof(binding));
	}
	if (inventory_binding_case == 4) {
		xl_undo_segment_recycle recycle = { 0 };
		recycle.segment_id = binding.segment_id;
		recycle.instance = binding.instance;
		recycle.old_state = SEGMENT_COMMITTED;
		recycle.new_state = SEGMENT_RECYCLABLE;
		next = inventory_write_record(source, next.exclusive_end, next.record_start,
									  RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_RECYCLE, &recycle,
									  sizeof(recycle));
	}
	return inventory_write_record(source, next.exclusive_end, next.record_start, RM_CLUSTER_UNDO_ID,
								  XLOG_UNDO_TT_SLOT_SET_HEAD, &head, sizeof(head));
}

/* Four original TT terminal carriers plus native-only ABORT. Conflicting
 * endpoints intentionally coexist: observation must never choose a winner. */
static WalTestRecord
inventory_write_terminal_sequence(const ClusterWalSourceRef *source, WalTestRecord next,
								  unsigned node)
{
	uint32 segment = node * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	uint8 payload[128] = { 0 };
	TimestampTz time = 8765;
	uint32 xinfo = XACT_XINFO_HAS_SCN | XACT_XINFO_HAS_TT_COMMIT;
	xl_xact_scn scn = { 5906 };
	xl_xact_tt_commit folded = { 0 };
	xl_undo_tt_slot_bind bind = { 0 };
	xl_undo_tt_slot_commit commit = { 0 };
	xl_undo_tt_slot_abort abort_record = { 0 };
	xl_undo_tt_slot_abort_exact exact = { 0 };
	size_t used = 0;
	if (inventory_terminal_mode == 20) {
		bind.segment_id = segment;
		bind.instance = node + 1;
		bind.xid = 4195136;
		bind.wrap = 1;
		bind.format_version = CLUSTER_UNDO_TT_BIND_VERSION;
		for (unsigned i = 0; i < 72; i++)
			next = inventory_write_record(source, next.exclusive_end, next.record_start,
										  RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_BIND, &bind,
										  sizeof(bind));
	}
	folded.segment_id = segment;
	folded.xid = 4195136;
	folded.wrap = 1;
	folded.instance = node + 1;
	folded.format_version = CLUSTER_XACT_TT_COMMIT_VERSION;
	folded.commit_scn = scn.scn;
	switch (inventory_terminal_mode) {
	case 6:
		folded.instance = node == 0 ? 2 : 1;
		break;
	case 7:
		folded.segment_id = node == 0 ? 257 : 1;
		break;
	case 8:
		folded.segment_generation = UINT32_MAX;
		break;
	case 9:
		folded.slot_offset = TT_SLOTS_PER_SEGMENT;
		break;
	case 10:
		folded.wrap = TT_WRAP_INVALID;
		break;
	case 11:
		folded.xid++;
		break;
	case 12:
		folded.format_version++;
		break;
	case 13:
		folded.flags = 1;
		break;
	case 14:
		folded.reserved = 1;
		break;
	case 15:
		folded.commit_scn++;
		break;
	default:
		break;
	}
	memcpy(payload + used, &time, sizeof(time));
	used += sizeof(time);
	memcpy(payload + used, &xinfo, sizeof(xinfo));
	used += sizeof(xinfo);
	memcpy(payload + used, &scn, sizeof(scn));
	used += sizeof(scn);
	memcpy(payload + used, &folded, sizeof(folded));
	used += sizeof(folded);
	if (inventory_terminal_mode != 18)
		next = inventory_write_record(source, next.exclusive_end, next.record_start, RM_XACT_ID,
									  XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO, payload, used);
	if (inventory_terminal_mode == 2 || (inventory_terminal_mode == 5 && node == 127)) {
		xl_undo_segment_recycle recycle = { 0 };
		recycle.segment_id = segment;
		recycle.instance = node + 1;
		recycle.old_state = SEGMENT_COMMITTED;
		recycle.new_state = SEGMENT_RECYCLABLE;
		next = inventory_write_record(source, next.exclusive_end, next.record_start,
									  RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_RECYCLE, &recycle,
									  sizeof(recycle));
	}
	if (inventory_terminal_mode == 3 || inventory_terminal_mode == 4) {
		bind.segment_id = segment;
		bind.instance = node + 1;
		bind.xid = 4195136 + (inventory_terminal_mode == 3);
		bind.wrap = 1 + (inventory_terminal_mode == 4);
		bind.format_version = CLUSTER_UNDO_TT_BIND_VERSION;
		next = inventory_write_record(source, next.exclusive_end, next.record_start,
									  RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_BIND, &bind,
									  sizeof(bind));
	}
	commit.segment_id = segment;
	commit.instance = node + 1;
	commit.xid = 4195136;
	commit.wrap = 1;
	commit.commit_scn = inventory_terminal_mode == 17 ? 5907 : 5906;
	if (inventory_terminal_mode != 18)
		next = inventory_write_record(source, next.exclusive_end, next.record_start,
									  RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_COMMIT, &commit,
									  sizeof(commit));
	if (inventory_terminal_mode == 16 || inventory_terminal_mode == 17
		|| inventory_terminal_mode == 21)
		return next;
	abort_record.segment_id = segment;
	abort_record.instance = node + 1;
	abort_record.xid = 4195136;
	abort_record.wrap = 1;
	next = inventory_write_record(source, next.exclusive_end, next.record_start, RM_CLUSTER_UNDO_ID,
								  XLOG_UNDO_TT_SLOT_ABORT, &abort_record, sizeof(abort_record));
	exact.segment_id = segment;
	exact.instance = node + 1;
	exact.xid = 4195136;
	exact.wrap = 1;
	exact.format_version = CLUSTER_UNDO_TT_ABORT_EXACT_VERSION;
	next = inventory_write_record(source, next.exclusive_end, next.record_start, RM_CLUSTER_UNDO_ID,
								  XLOG_UNDO_TT_SLOT_ABORT, &exact, sizeof(exact));
	return inventory_write_record(source, next.exclusive_end, next.record_start, RM_XACT_ID,
								  XLOG_XACT_ABORT, &time, sizeof(time));
}

static void
inventory_mx_payload(char *bytes, bool invalid, bool changed)
{
	xl_multixact_create create = { 0 };
	MultiXactMember members[2]
		= { { 4195136, MultiXactStatusForShare }, { 4195138, MultiXactStatusUpdate } };

	create.mid = 42;
	create.moff = invalid ? 0 : 108;
	create.nmembers = 2;
	if (changed)
		members[1].xid++;
	memcpy(bytes, &create, SizeOfMultiXactCreate);
	memcpy(bytes + SizeOfMultiXactCreate, members, sizeof(members));
}

static WalTestRecord
inventory_write_mx_sequence(const ClusterWalSourceRef *source, WalTestRecord next, unsigned node)
{
	char bytes[SizeOfMultiXactCreate + 2 * sizeof(MultiXactMember)];
	xl_multixact_truncate trunc = { 5, 1, 40, 1, 106 };
	int page = 0;

	inventory_mx_payload(bytes, inventory_extra == 7, false);
	if (node == 0) {
		inventory_mx_start = next.exclusive_end;
		inventory_mx_previous = next.record_start;
	}
	next = inventory_write_record(source, next.exclusive_end, next.record_start, RM_MULTIXACT_ID,
								  inventory_extra == 8 ? 0x40 : XLOG_MULTIXACT_CREATE_ID, bytes,
								  sizeof(bytes));
	next = inventory_write_record(source, next.exclusive_end, next.record_start, RM_MULTIXACT_ID,
								  XLOG_MULTIXACT_ZERO_OFF_PAGE, &page, sizeof(page));
	next = inventory_write_record(source, next.exclusive_end, next.record_start, RM_MULTIXACT_ID,
								  XLOG_MULTIXACT_ZERO_MEM_PAGE, &page, sizeof(page));
	return inventory_write_record(source, next.exclusive_end, next.record_start, RM_MULTIXACT_ID,
								  XLOG_MULTIXACT_TRUNCATE_ID, &trunc, sizeof(trunc));
}

static uint8
inventory_slru_payload(unsigned shape, char *bytes, bool changed)
{
	int page = 33;
	xl_clog_truncate clog = { page, page * CLUSTER_NATIVE_CLOG_PER_PAGE, changed ? 6 : 5 };
	xl_commit_ts_truncate cts
		= { page, page * CLUSTER_NATIVE_COMMIT_TS_PER_PAGE + (changed ? 1 : 0) };
	if (shape % 2 == 0) {
		memcpy(bytes, &page, sizeof(page));
		return sizeof(page);
	}
	if (shape == 1) {
		memcpy(bytes, &clog, sizeof(clog));
		return sizeof(clog);
	}
	memcpy(bytes, &cts, SizeOfCommitTsTruncate);
	return SizeOfCommitTsTruncate;
}

static WalTestRecord
inventory_write_slru_sequence(const ClusterWalSourceRef *source, WalTestRecord next, unsigned node)
{
	unsigned first = inventory_extra == 10 ? 0 : inventory_extra == 11 ? 2 : 3;
	unsigned limit = inventory_extra == 10 ? 2 : first + 1;
	for (unsigned shape = first; shape < limit; shape++) {
		char bytes[sizeof(xl_clog_truncate)];
		uint8 length = inventory_slru_payload(shape, bytes, false);
		uint8 opcode = shape % 2 ? 0x10 : 0;
		if (node == 0) {
			inventory_slru_start[shape] = next.exclusive_end;
			inventory_slru_previous[shape] = next.record_start;
		}
		if ((inventory_slru_fault == 1 && shape == 0) || (inventory_slru_fault == 2 && shape == 2))
			opcode = 0xF0;
		if ((inventory_slru_fault == 3 && shape == 1) || (inventory_slru_fault == 4 && shape == 3))
			length--;
		next = inventory_write_record(source, next.exclusive_end, next.record_start,
									  shape < 2 ? RM_CLOG_ID : RM_COMMIT_TS_ID, opcode, bytes,
									  length);
	}
	return next;
}

/* A mixed ordinary FPI and rebuildable component, with an unused block ID.
 * The native parser, rather than a fabricated decoded fixture, owns framing. */
static WalTestRecord
inventory_write_page(const ClusterWalSourceRef *source, XLogRecPtr at, XLogRecPtr previous,
	bool changed)
{
	uint8 bytes[512] = {0};
	uint8 *wire = bytes + SizeOfXLogRecord;
	XLogRecord record = {0};
	XLogRecordBlockHeader block = {.id = 0, .fork_flags = MAIN_FORKNUM | BKPBLOCK_HAS_IMAGE};
	XLogRecordBlockImageHeader image = {.length = 64, .hole_offset = 64,
		.bimg_info = BKPIMAGE_HAS_HOLE | BKPIMAGE_APPLY};
	RelFileLocator locator = {1663, 5, 16386};
	BlockNumber number = 7;
	PGAlignedBlock page = {0};
	PageHeader header = (PageHeader)page.data;
	size_t used = SizeOfXLogRecord + XLR_PAGE_VERSION_EDGE_HEADER_SIZE
		+ 2 * XLR_PAGE_VERSION_EDGE_ENTRY_SIZE;
	char path[MAXPGPATH];
	int fd;

	wire[0] = XLR_BLOCK_ID_PAGE_VERSION_EDGE; wire[1] = XLR_PAGE_VERSION_EDGE_FORMAT_V1;
	wire[2] = 2; wire[3] = XLR_PAGE_VERSION_EDGE_ENTRY_SIZE; wire[8] = 77;
	wire += XLR_PAGE_VERSION_EDGE_HEADER_SIZE;
	wire[1] = RF_PAGE_CLASS_ORDINARY;
	wire[2] = wire[3] = RF_PAGE_STATE_PRESENT;
	wire[4] = RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE;
	memset(wire + 8, 0x21, 16); wire[24] = 70; memset(wire + 32, 0x21, 16);
	wire += XLR_PAGE_VERSION_EDGE_ENTRY_SIZE;
	wire[0] = 2; wire[1] = RF_PAGE_CLASS_REBUILDABLE_FSM;
	wire[2] = wire[3] = RF_PAGE_STATE_REBUILDABLE; wire[6] = 1;
	memcpy(bytes + used, &block, SizeOfXLogRecordBlockHeader); used += SizeOfXLogRecordBlockHeader;
	memcpy(bytes + used, &image, SizeOfXLogRecordBlockImageHeader); used += SizeOfXLogRecordBlockImageHeader;
	memcpy(bytes + used, &locator, sizeof(locator)); used += sizeof(locator);
	memcpy(bytes + used, &number, sizeof(number)); used += sizeof(number);
	block = (XLogRecordBlockHeader){.id = 2, .fork_flags = FSM_FORKNUM | BKPBLOCK_HAS_DATA,
		.data_length = 4};
	locator.relNumber++; number = 31;
	memcpy(bytes + used, &block, SizeOfXLogRecordBlockHeader); used += SizeOfXLogRecordBlockHeader;
	memcpy(bytes + used, &locator, sizeof(locator)); used += sizeof(locator);
	memcpy(bytes + used, &number, sizeof(number)); used += sizeof(number);
	bytes[used++] = XLR_BLOCK_ID_DATA_SHORT; bytes[used++] = 4;
	header->pd_lower = SizeOfPageHeaderData; header->pd_upper = header->pd_special = BLCKSZ;
	header->pd_block_scn = 77;
	PageSetPageSizeAndVersion(page.data, BLCKSZ, PG_PAGE_LAYOUT_VERSION);
	memcpy(bytes + used, page.data, 64); used += 64;
	memcpy(bytes + used, "edge", 4); used += 4;
	memcpy(bytes + used, changed ? "MAIN" : "main", 4); used += 4;
	record.xl_tot_len = used; record.xl_prev = previous;
	record.xl_rmid = RM_GENERIC_ID; record.xl_scn = 5906;
	INIT_CRC32C(record.xl_crc);
	COMP_CRC32C(record.xl_crc, bytes + SizeOfXLogRecord, used - SizeOfXLogRecord);
	COMP_CRC32C(record.xl_crc, &record, offsetof(XLogRecord, xl_crc)); FIN_CRC32C(record.xl_crc);
	memcpy(bytes, &record, SizeOfXLogRecord);
	v2_checkpoint_wal_path(&source->claim.identity, at, source->timeline, path);
	UT_ASSERT(at % XLOG_BLCKSZ + used < XLOG_BLCKSZ);
	fd = open(path, O_WRONLY); UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pwrite(fd, bytes, used, at % wal_segment_size), used);
	UT_ASSERT_EQ(close(fd), 0);
	return (WalTestRecord){1, MAXALIGN(at + used), at, record.xl_crc};
}

static ClusterWalInputsV1 *
inventory_fixture(bool unsupported, bool open_source)
{
	uint8 bytes[66048];
	ClusterRecoveryAnchorV2 anchors[2];
	ClusterWalInputsV1 *inputs = NULL;
	ControlRootImage root;

	inputs_closed_wal_fixture(bytes, anchors);
	UT_ASSERT_EQ(
		cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &root), 0);
	for (unsigned i = 0; i < 2; i++) {
		unsigned node = i == 0 ? 0 : 127;
		ClusterWalSourceRef source = { 0 };
		ClusterControlRootSnapshot *snapshot = &root.records[node];
		WalTestRecord commit, extra, create;
		xl_smgr_create payload = { .rlocator = { 1663, 5, 16385 }, .forkNum = MAIN_FORKNUM };

		source.claim.identity = snapshot->identity;
		source.timeline = snapshot->tail_tli;
		commit = inventory_write_commit(&source, snapshot->validated_tail_lsn_exclusive,
										snapshot->tail_last_record_lsn, 8765, unsupported);
		extra = commit;
		if (inventory_extra == 1)
			extra = inventory_write_record(&source, commit.exclusive_end, commit.record_start,
										   RM_XLOG_ID, XLOG_NOOP, "abc", 3);
		else if (inventory_extra == 2)
			extra = inventory_write_fsm(&source, commit.exclusive_end, commit.record_start, 31);
		else if (inventory_extra == 3 || inventory_extra == 4) {
			xl_undo_tt_slot_bind binding = { 0 };

			binding.segment_id = node * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
			binding.instance = node + 1;
			if (inventory_extra == 4) {
				binding.instance = node == 0 ? 128 : 1;
				binding.segment_id = node == 0 ? 32513 : 1;
			}
			binding.xid = 4195136;
			binding.wrap = 1;
			binding.format_version = CLUSTER_UNDO_TT_BIND_VERSION;
			extra = inventory_write_record(&source, commit.exclusive_end, commit.record_start,
										   RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_BIND, &binding,
										   sizeof(binding));
		}
		if (inventory_extra == 5 || inventory_extra == 9 || inventory_extra == 16)
			extra = inventory_write_bound_head(&source, commit, node);
		if ((inventory_extra >= 6 && inventory_extra <= 9) || inventory_extra == 16)
			extra = inventory_write_mx_sequence(&source, extra, node);
		if (inventory_extra >= 10 && inventory_extra <= 12)
			extra = inventory_write_slru_sequence(&source, extra, node);
		if (inventory_extra == 13) {
			/* A separate assigned transaction, not a terminal/parent proof. */
			char bytes[MinSizeOfXactAssignment + 2 * sizeof(TransactionId)] = { 0 };
			TransactionId top = 4196000, children[] = { 4196001, 4196352 };
			int count = 2;
			memcpy(bytes, &top, sizeof(top));
			memcpy(bytes + sizeof(top), &count, sizeof(count));
			memcpy(bytes + MinSizeOfXactAssignment, children, sizeof(children));
			extra = inventory_write_record(&source, extra.exclusive_end, extra.record_start,
										   RM_XACT_ID, XLOG_XACT_ASSIGNMENT, bytes, sizeof(bytes));
		}
		if (inventory_extra == 14) {
			char bytes[TEST_RELMAP_LENGTH];
			uint8 opcode = inventory_relmap_fault == 1 ? 0x10 : XLOG_RELMAP_UPDATE;
			test_relmap_payload(bytes, node == 0, 2);
			if (inventory_relmap_fault == 2)
				bytes[TEST_RELMAP_LENGTH - 1] ^= 1;
			extra = inventory_write_record(&source, extra.exclusive_end, extra.record_start,
										   RM_RELMAP_ID, opcode, bytes, sizeof(bytes));
		}
		if (inventory_extra == 15) {
			char bytes[TEST_PREPARE_MAX];
			uint32 length = test_prepare_payload(bytes, true, 13);
			uint8 opcode = XLOG_XACT_PREPARE;
			if (inventory_prepare_fault == 1)
				bytes[length - MAXALIGN(sizeof(TwoPhaseRecordOnDisk))
					  + offsetof(TwoPhaseRecordOnDisk, rmid)] = 0xff;
			if (inventory_prepare_fault == 2) {
				int bad = -1;
				memcpy(bytes + offsetof(xl_xact_prepare, nabortrels), &bad, sizeof(bad));
			}
			if (inventory_prepare_fault == 3)
				opcode = XLOG_XACT_COMMIT_PREPARED;
			extra = inventory_write_record(&source, extra.exclusive_end, extra.record_start,
										   RM_XACT_ID, opcode, bytes, length);
		}
		if (inventory_extra == 16)
			extra = inventory_write_terminal_sequence(&source, extra, node);
		if (inventory_extra == 17)
			extra = inventory_write_page(&source, extra.exclusive_end, extra.record_start, false);
		if (inventory_extra == 18)
			extra = inventory_write_relmap_fold_sequence(&source, extra, node);
		create = inventory_write_record(&source, extra.exclusive_end, extra.record_start,
										RM_SMGR_ID, XLOG_SMGR_CREATE, &payload, sizeof(payload));
		if (i == 0) {
			inventory_fault_source = source;
			inventory_commit_start = commit.record_start;
			inventory_extra_start = extra.record_start;
			inventory_extra_previous = commit.record_start;
			inventory_commit_previous = snapshot->tail_last_record_lsn;
		}
		snapshot->tail_last_record_lsn = create.record_start;
		snapshot->tail_last_record_crc32c = create.record_crc;
		snapshot->validated_tail_lsn_exclusive = create.exclusive_end;
	}
	if (open_source)
		root.records[127].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0);
	v2_write_roots(root.bytes);
	test_inputs_startup_cf = true;
	UT_ASSERT_EQ(inputs_begin_for_role(true, &inputs), 0);
	inventory_census_calls = 0;
	inventory_between_passes = NULL;
	return inputs;
}

static void
inventory_drift(void)
{
	char path[MAXPGPATH];

	switch (inventory_fault) {
	case 0:
		/* Non-tail record: its new CRC is valid; ROOT's last-record CRC is
		 * unchanged. Full native bytes must still detect this rewrite. */
		(void)inventory_write_commit(&inventory_fault_source, inventory_commit_start,
									 inventory_commit_previous, 9876, false);
		break;
	case 1:
		v2_checkpoint_wal_path(&inventory_fault_source.claim.identity, inventory_commit_start,
							   inventory_fault_source.timeline, path);
		UT_ASSERT_EQ(unlink(path), 0);
		break;
	case 2:
		v2_checkpoint_root_race();
		break;
	case 3:
		inputs_pin_current = false;
		break;
	case 4:
		pg_re_throw();
		break;
	case 6:
		(void)inventory_write_record(&inventory_fault_source, inventory_extra_start,
									 inventory_extra_previous, RM_XLOG_ID, XLOG_NOOP, "abd", 3);
		break;
	case 7:
		(void)inventory_write_fsm(&inventory_fault_source, inventory_extra_start,
								  inventory_extra_previous, 32);
		break;
	case 5: {
		/* Same ordinal and byte length, but no longer this native family.
		 * Record-count equality must not erase the previously seen effect. */
		uint8 bytes[128];
		uint8 length;
		int fd;

		v2_checkpoint_wal_path(&inventory_fault_source.claim.identity, inventory_commit_start,
							   inventory_fault_source.timeline, path);
		fd = open(path, O_RDONLY);
		UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(
			pread(fd, &length, 1, inventory_commit_start % wal_segment_size + SizeOfXLogRecord + 1),
			1);
		UT_ASSERT(length <= sizeof(bytes));
		UT_ASSERT_EQ(pread(fd, bytes, length,
						   inventory_commit_start % wal_segment_size + SizeOfXLogRecord + 2),
					 length);
		UT_ASSERT_EQ(close(fd), 0);
		(void)inventory_write_record(&inventory_fault_source, inventory_commit_start,
									 inventory_commit_previous, RM_XLOG_ID, XLOG_NOOP, bytes,
									 length);
		break;
	}
	}
}

UT_TEST(test_native_inventory_uses_two_real_cold_passes)
{
	ClusterWalInputsV1 *inputs = inventory_fixture(false, false);
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryNativeEffect effect;
	unsigned effects = 0, terminals = 0, messages = 0, creates = 0;
	ClusterRecoveryNativeInventoryResult result;

	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	UT_ASSERT_EQ(inventory_census_calls, 2);
	UT_ASSERT_EQ(diag.sources, 2);
	UT_ASSERT_EQ(diag.records, 6);
	UT_ASSERT_EQ(diag.native_records, 4);
	UT_ASSERT_EQ(diag.other_records, 2); /* checkpoints remain other obligations */
	if (inventory != NULL) {
		while ((result = cluster_recovery_native_inventory_next(inventory, &effect))
			   == CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT) {
			effects++;
			UT_ASSERT_EQ(effect.record->source.claim.identity.origin_thread_id,
						 effect.record->cut.failed_thread);
			UT_ASSERT(effect.record->read_lsn >= effect.record->cut.scan_begin_inclusive);
			if (effect.kind == CLUSTER_RECOVERY_NATIVE_TERMINAL) {
				terminals++;
				UT_ASSERT(effect.record->xinfo & XACT_COMPLETION_FORCE_SYNC_COMMIT);
				UT_ASSERT_EQ(effect.record->xact_time, 8765);
				UT_ASSERT_EQ(effect.record->terminal_scn, 5906);
			} else if (effect.kind == CLUSTER_RECOVERY_NATIVE_INVALIDATION)
				messages++;
			else if (effect.kind == CLUSTER_RECOVERY_NATIVE_CREATE)
				creates++;
		}
		UT_ASSERT_EQ(result, CLUSTER_RECOVERY_NATIVE_INVENTORY_END);
		UT_ASSERT_EQ(effects, 8);
		UT_ASSERT_EQ(terminals, 2);
		UT_ASSERT_EQ(messages, 4);
		UT_ASSERT_EQ(creates, 2);
	}
	UT_ASSERT_EQ(inputs_peer_begins + inputs_native_begin_calls + inputs_native_flush_calls, 0);
	cluster_recovery_native_inventory_destroy(inventory);
	cold_census_done(&inputs);
}

UT_TEST(test_native_inventory_never_returns_partial_on_drift_io_or_error)
{
	for (inventory_fault = 0; inventory_fault < 6; inventory_fault++) {
		ClusterWalInputsV1 *inputs = inventory_fixture(false, false);
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		bool threw = false;

		inventory_between_passes = inventory_drift;
		PG_TRY();
		{
			UT_ASSERT_NE(
				cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
				CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
			if (inventory_fault == 0)
				UT_ASSERT_EQ(diag.native_result, CLUSTER_RECOVERY_NATIVE_CHANGED);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(threw, inventory_fault == 4);
		UT_ASSERT(inventory == NULL);
		inventory_between_passes = NULL;
		inputs_pin_current = true;
		cold_census_done(&inputs);
	}
}

UT_TEST(test_native_inventory_capacity_is_not_partial_success)
{
	size_t required = 0;

	for (unsigned mode = 0; mode < 3; mode++) {
		ClusterWalInputsV1 *inputs = inventory_fixture(false, false);
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		size_t budget = mode == 0 ? 1024 * 1024 : required - (mode == 1);

		UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, budget, &inventory, &diag),
					 mode == 1 ? CLUSTER_RECOVERY_NATIVE_INVENTORY_CAPACITY
							   : CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
		if (mode == 0) {
			required = diag.owned_bytes;
			UT_ASSERT(required > 0 && required < budget);
		} else if (mode == 1)
			UT_ASSERT(inventory == NULL);
		else
			UT_ASSERT_EQ(diag.owned_bytes, budget);
		cluster_recovery_native_inventory_destroy(inventory);
		cold_census_done(&inputs);
		if (required == 0)
			break;
	}
}

UT_TEST(test_native_inventory_open_unknown_and_online_never_observed)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		ClusterWalInputsV1 *inputs = inventory_fixture(mode == 1, mode == 0);
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;

		if (mode == 2) {
			cold_census_done(&inputs);
			UT_ASSERT_EQ(inputs_begin_for_role(false, &inputs), 0);
		}
		UT_ASSERT_NE(
			cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
			CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
		UT_ASSERT(inventory == NULL);
		if (mode == 0)
			UT_ASSERT_EQ(diag.input_result, CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		if (mode == 1) /* COMMIT bytes under a now-recognized PREPARE opcode */
			UT_ASSERT_EQ(diag.native_result, CLUSTER_RECOVERY_NATIVE_SHAPE);
		cold_census_done(&inputs);
	}
}

static ClusterRecoveryRootSet *inventory_roots(ClusterWalInputsV1 *inputs, char path[MAXPGPATH],
											   int *fd, bool mismatch);
static void inventory_roots_done(ClusterRecoveryRootSet *roots, int fd, const char *path);

UT_TEST(test_native_inventory_keeps_real_empty_sources_and_other_obligations)
{
	int saved_node = cluster_node_id;
	uint64 saved_incarnation = test_self_incarnation;
	ClusterRecoverySerialGuard serial;
	ClusterThreadRecLaunchEligibility eligibility;
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryNativeEffect effect;
	ClusterRecoveryRootSet *roots = NULL;
	char root_path[MAXPGPATH];
	int root_fd = -1;
	ControlRootImage root;
	ControlFileData control;
	ClusterControlRootFileToken token;
	uint32 selected;

	(void)pending_worker_fixture(&serial, &eligibility);
	if (ut_current_failed)
		goto cleanup;
	test_worker_pin_held = false;
	test_history_sync_count = 0;
	test_history_fail_sync = UINT_MAX;
	UT_ASSERT_EQ(thread_recovery_worker_run(&eligibility), CLUSTER_THREADREC_DONE);
	test_history_fail_sync = 0;
	test_actual_cf = test_cf_mode = ShareLock;
	UT_ASSERT_EQ(cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID, &root,
															 &control, &token),
				 0);
	memset(&root.startup[3], 0, sizeof(root.startup[3]));
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0);
	v2_write_roots(root.bytes);
	test_actual_cf = test_cf_mode = NoLock;
	test_inputs_startup_cf = true;
	UT_ASSERT_EQ(inputs_begin_for_role(true, &inputs), 0);
	if (inputs == NULL)
		goto cleanup;
	selected = cluster_wal_inputs_count_v1(inputs);
	inventory_census_calls = 0;
	inventory_between_passes = NULL;
	roots = inventory_roots(inputs, root_path, &root_fd, false);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_collect_roots(inputs, roots, 1024 * 1024,
																 &inventory, &diag),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	UT_ASSERT_EQ(inventory_census_calls, 2);
	UT_ASSERT_EQ(diag.sources, selected);
	UT_ASSERT_EQ(diag.native_records, 0);
	UT_ASSERT_EQ(diag.records, diag.other_records);
	UT_ASSERT(diag.other_records > 0);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_next(inventory, &effect),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_END);
	/* The empty terminal is represented, not conflated with a missing
	 * source or with an all-producer recovery-complete graph. */
	if (inventory != NULL) {
		unsigned empty = 0;
		for (uint32 i = 0; i < selected; i++)
			empty += (inventory->sources[i].cut.flags & RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY) != 0;
		UT_ASSERT_EQ(empty, 1);
	}
cleanup:
	if (roots != NULL)
		inventory_roots_done(roots, root_fd, root_path);
	cluster_recovery_native_inventory_destroy(inventory);
	cold_census_done(&inputs);
	test_reserve_mode = false;
	cluster_node_id = saved_node;
	test_self_incarnation = saved_incarnation;
}

#include "test_cluster_recovery_native_transcript.inc"
#include "test_cluster_recovery_wal_tt_roots.inc"
#include "test_cluster_recovery_multixact_inputs.inc"
#include "test_cluster_recovery_slru_inputs.inc"
#include "test_cluster_recovery_xact_slru_inputs.inc"
#include "test_cluster_recovery_multixact_join_inputs.inc"
#include "test_cluster_recovery_census_inputs.inc"
#include "test_cluster_recovery_xact_join_inputs.inc"
#include "test_cluster_recovery_relmap_inputs.inc"
#include "test_cluster_recovery_relmap_physical_inputs.inc"
#include "test_cluster_recovery_prepare_inputs.inc"
#include "test_cluster_recovery_manifest_inputs.inc"
#include "test_cluster_recovery_terminal_inputs.inc"
#include "test_cluster_recovery_tt_terminal_inputs.inc"
#include "test_cluster_recovery_page_roots_inputs.inc"
#include "test_cluster_recovery_page_wal_inputs.inc"
#include "test_cluster_recovery_wal_sources.inc"
#include "test_cluster_recovery_page_graph.inc"
#include "test_cluster_recovery_census_page_plan.inc"
#include "test_cluster_recovery_reference_inputs.inc"

UT_TEST(test_owned_ctrc_census_borrows_namespace_and_revokes_changed_work)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryCensus *census = NULL;
	ClusterRecoveryCensusEntry entry, zero = {0};
	ClusterRecoveryCensusManifest manifest, empty = {0};
	int fd = -1;

	ctrc_inputs_fixture();
	UT_ASSERT_EQ(ctrc_inputs_begin(&inputs), 0);
	UT_ASSERT_EQ(cluster_recovery_census_begin_ctrc(inputs, 8 * 1024 * 1024, &census),
		CLUSTER_RECOVERY_CENSUS_MORE);
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_namespace_v1(inputs, &fd), 0);
	UT_ASSERT_EQ(cluster_recovery_census_next(census, &entry), CLUSTER_RECOVERY_CENSUS_MORE);
	tx_inputs_typed.ctrc_work_generation++;
	memset(&entry, 0xa5, sizeof(entry));
	UT_ASSERT_EQ(cluster_recovery_census_next(census, &entry), CLUSTER_RECOVERY_CENSUS_REFUSED);
	UT_ASSERT(memcmp(&entry, &zero, sizeof(entry)) == 0);
	memset(&manifest, 0xa5, sizeof(manifest));
	UT_ASSERT(!cluster_recovery_census_manifest(census, &manifest));
	UT_ASSERT(memcmp(&manifest, &empty, sizeof(manifest)) == 0);
	cluster_recovery_census_destroy(census);
	UT_ASSERT(fcntl(fd, F_GETFD) >= 0);
	cluster_wal_inputs_release_v1(&inputs);
	UT_ASSERT_EQ(fcntl(fd, F_GETFD), -1);
	test_inputs_startup_cf = false;
}

UT_TEST(test_owned_ctrc_census_rejects_other_purpose_and_namespace_switch)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryCensus *census = NULL;
		ClusterRecoveryCensusEntry entry, zero = {0};
		char *selected;

		ctrc_inputs_fixture();
		if (fault == 0) {
			inputs_native_recovery = true;
			UT_ASSERT_EQ(cluster_wal_inputs_cold_begin_v1(v2_storage, TEST_SYSID, &inputs), 0);
			UT_ASSERT_EQ(cluster_recovery_census_begin_ctrc(inputs, 8 * 1024 * 1024, &census),
				CLUSTER_RECOVERY_CENSUS_REFUSED);
			UT_ASSERT_NULL(census);
		} else {
			UT_ASSERT_EQ(ctrc_inputs_begin(&inputs), 0);
			UT_ASSERT_EQ(cluster_recovery_census_begin_ctrc(inputs, 8 * 1024 * 1024, &census),
				CLUSTER_RECOVERY_CENSUS_MORE);
			selected = cluster_shared_data_dir;
			cluster_shared_data_dir = "";
			memset(&entry, 0xa5, sizeof(entry));
			UT_ASSERT_EQ(cluster_recovery_census_next(census, &entry), CLUSTER_RECOVERY_CENSUS_REFUSED);
			cluster_shared_data_dir = selected;
			UT_ASSERT(memcmp(&entry, &zero, sizeof(entry)) == 0);
		}
		cluster_recovery_census_destroy(census);
		cluster_wal_inputs_release_v1(&inputs);
	}
	test_inputs_startup_cf = false;
}

static void
ctrc_inventory_append_after_first_pass(void)
{
	test_flush = test_insert = ctrc_local_append(ctrc_local_checkpoint_end, test_insert);
}

UT_TEST(test_ctrc_inventory_keeps_original_local_cut_and_native_terminal_bytes)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryNativeEffect effect;
	ClusterWalSourceRef selected = { 0 };
	TimestampTz time = 12345;
	WalTestRecord terminal;
	XLogRecPtr fixed;

	ctrc_local_fixture(false);
	terminal = inventory_write_record(&ctrc_local_source, ctrc_local_checkpoint_end,
		ctrc_local_checkpoint, RM_XACT_ID, XLOG_XACT_ABORT, &time, sizeof(time));
	fixed = test_insert = terminal.exclusive_end;
	UT_ASSERT_EQ(ctrc_inputs_begin(&inputs), 0);
	for (uint32 i = 0; i < cluster_wal_inputs_count_v1(inputs); i++) {
		const ClusterWalInputV1 *item = cluster_wal_inputs_at_v1(inputs, i);
		if (item->current && cluster_control_root_identity_equal(&item->source.claim.identity,
			&ctrc_local_source.claim.identity))
			selected = item->source;
	}
	UT_ASSERT(selected.claim.database_incarnation != 0);
	inventory_census_calls = 0;
	inventory_between_passes = ctrc_inventory_append_after_first_pass;
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
		CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	inventory_between_passes = NULL;
	UT_ASSERT_EQ(inventory_census_calls, 2);
	UT_ASSERT_EQ(diag.sources, 4);
	UT_ASSERT_EQ(diag.native_records, 1);
	UT_ASSERT(diag.records >= 5);
	UT_ASSERT_EQ(ctrc_local_flushes, 1);
	UT_ASSERT_EQ(ctrc_local_requested, fixed);
	UT_ASSERT(test_insert > fixed);
	if (inventory != NULL) {
		UT_ASSERT_EQ(cluster_recovery_native_inventory_next(inventory, &effect),
			CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT);
		UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_TERMINAL);
		UT_ASSERT_EQ(effect.record->read_lsn, terminal.record_start);
		UT_ASSERT_EQ(effect.record->end_lsn, fixed);
		UT_ASSERT_EQ(effect.record->cut.scan_end_exclusive, fixed);
		UT_ASSERT_EQ(effect.record->header.xl_info & ~XLR_INFO_MASK, XLOG_XACT_ABORT);
		UT_ASSERT_EQ(effect.record->xact_time, time);
		UT_ASSERT_EQ(memcmp(&effect.record->source, &selected, sizeof(selected)), 0);
		UT_ASSERT_EQ(cluster_recovery_native_inventory_next(inventory, &effect),
			CLUSTER_RECOVERY_NATIVE_INVENTORY_END);
		UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag),
			CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
		/* A CRC-valid rewrite within the fixed prefix revokes the inventory;
		 * bytes appended after the cut above cannot hide that change. */
		time++;
		inventory_write_record(&ctrc_local_source, terminal.record_start, ctrc_local_checkpoint,
			RM_XACT_ID, XLOG_XACT_ABORT, &time, sizeof(time));
		UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag),
			CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED);
	}
	cluster_recovery_native_inventory_destroy(inventory);
	ctrc_local_done(&inputs);
}

#include "test_cluster_recovery_episode_history.inc"
#include "test_cluster_recovery_relmap_fold.inc"
#include "test_cluster_recovery_episode_pending.inc"
#include "test_cluster_recovery_episode_clog.h"
static size_t episode_controlled_clog_bytes(void);
static bool episode_controlled_clog_read(const ClusterRecoveryRelmapSourceV1 *source,
	uint64 full, ClusterRelmapClogBaseV1 *out, ClusterRelmapClogBaseHandleV1 **held);
static void episode_controlled_clog_release(ClusterRelmapClogBaseHandleV1 **held);
#define cluster_clog_relmap_base_custody_bytes_v1 episode_controlled_clog_bytes
#define cluster_clog_relmap_base_read_v1 episode_controlled_clog_read
#define cluster_clog_relmap_base_release_v1 episode_controlled_clog_release
#include "test_cluster_recovery_episode_base.inc"
#undef cluster_clog_relmap_base_custody_bytes_v1
#undef cluster_clog_relmap_base_read_v1
#undef cluster_clog_relmap_base_release_v1

static bool episode_native_clog;
static unsigned episode_native_clog_calls;

size_t
cluster_clog_relmap_base_custody_bytes_v1(void)
{
	return episode_native_clog ? test_episode_native_clog_bytes()
		: episode_controlled_clog_bytes();
}

bool
cluster_clog_relmap_base_read_v1(const ClusterRecoveryRelmapSourceV1 *source,
	uint64 full, ClusterRelmapClogBaseV1 *out, ClusterRelmapClogBaseHandleV1 **held)
{
	if (episode_native_clog) {
		void *native = *held;
		bool ok;
		episode_native_clog_calls++;
		ok = test_episode_native_clog_read(source, full, out, &native);
		*held = native;
		return ok;
	}
	return episode_controlled_clog_read(source, full, out, held);
}

void
cluster_clog_relmap_base_release_v1(ClusterRelmapClogBaseHandleV1 **held)
{
	if (episode_native_clog) {
		void *native = *held;
		test_episode_native_clog_release(&native);
		*held = native;
	} else
		episode_controlled_clog_release(held);
}
#include "test_cluster_recovery_episode_sources.inc"
#include "test_cluster_recovery_episode_pages.inc"
#include "test_cluster_recovery_episode_native_clog.inc"
#include "test_cluster_recovery_episode_terminal.inc"
#include "test_cluster_recovery_episode_xid.inc"
#include "test_cluster_recovery_episode_clog_history.inc"
#include "test_cluster_recovery_episode_clog_birth.inc"
#include "test_cluster_recovery_episode_birth_match.inc"
#include "test_cluster_recovery_episode_birth_terminal.inc"
#include "test_cluster_recovery_episode_clog_side.inc"
#include "test_cluster_recovery_episode_birth_halfspace.inc"
#include "test_cluster_recovery_episode_all_clog.inc"
#include "test_cluster_recovery_episode_native_pages.inc"
#include "test_cluster_recovery_episode_applied_pages.inc"
#include "test_cluster_recovery_episode_side_owner.h"
#include "test_cluster_recovery_episode_applied_owner.inc"
#include "test_cluster_recovery_episode_consumed_pages.inc"
#include "test_cluster_recovery_episode_local_bounds.inc"
#include "test_cluster_recovery_episode_native_bounds.inc"
#include "test_cluster_recovery_episode_issued_clog.inc"
#include "test_cluster_recovery_episode_resident_birth.inc"
#include "test_cluster_recovery_episode_initialized_pages.inc"
#include "test_cluster_recovery_episode_multipage_init.inc"
#include "test_cluster_recovery_episode_page_history.inc"
#include "test_cluster_recovery_episode_page_sources.inc"
#include "test_cluster_recovery_consumed_native_redo.inc"
#include "test_cluster_recovery_episode_generation_xid.inc"
#include "test_cluster_recovery_clog_full_roots.inc"

#include "test_cluster_recovery_census_current_pages.inc"
#include "test_cluster_recovery_census_namespace.inc"

#include "test_cluster_recovery_census_tx_records.inc"
#include "test_cluster_recovery_census_native_records.inc"
#include "test_cluster_recovery_census_tx_dependencies.inc"
#include "test_cluster_recovery_census_tx_reference_edges.inc"
#include "test_cluster_recovery_census_tx_recheck.inc"

int
main(int argc, char **argv)
{
	(void)argc;
	setvbuf(stdout, NULL, _IOLBF, 0);
	test_program_path = argv[0];
	setup_fixture();
	if (getenv("PGRAC_PRE2_TEST_CENSUS_TX_RECHECK") != NULL) {
		UT_PLAN(3);
		UT_RUN(test_census_tx_final_recheck_accepts_original_complete_files_and_empty_end);
		UT_RUN(test_census_tx_final_recheck_detects_same_inode_late_predecessor_rewrite);
		UT_RUN(test_census_tx_final_recheck_does_not_enter_under_locks_or_stale_source);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_CENSUS_TX_REFERENCE_EDGES") != NULL) {
		UT_PLAN(3);
		UT_RUN(test_census_tx_references_join_all_original_current_and_history_items);
		UT_RUN(test_census_tx_references_prefix_empty_lock_and_range_are_distinct);
		UT_RUN(test_census_tx_references_late_source_graph_generation_and_error_are_sticky);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_CENSUS_TX_DEPENDENCIES") != NULL) {
		UT_PLAN(3);
		UT_RUN(test_census_tx_dependencies_construct_original_roots_and_cross_relation_edge);
		UT_RUN(test_census_tx_dependencies_prefix_lock_and_range_do_not_enter);
		UT_RUN(test_census_tx_dependencies_late_source_count_error_revoke_whole_forest);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_CENSUS_TX_RECORDS") != NULL) {
		UT_PLAN(4);
		UT_RUN(test_census_tx_records_include_real_cross_relation_predecessor);
		UT_RUN(test_census_tx_records_preconditions_and_range_do_not_advance);
		UT_RUN(test_census_tx_records_late_source_graph_error_and_count_are_sticky);
		UT_RUN(test_census_tx_records_original_second_read_refuses_file_rewrite);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_CENSUS_NAMESPACE") != NULL) {
		UT_PLAN(3);
	UT_RUN(test_census_namespace_complete_set_keeps_original_pages_and_manifest);
	UT_RUN(test_census_namespace_added_object_revokes_private_census_not_original_pin);
	UT_RUN(test_census_namespace_preflight_and_entered_source_or_error_keep_pin);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_MULTIPAGE_INIT") != NULL) {
		UT_PLAN(4);
		UT_RUN(test_episode_multipage_init_keeps_both_actual_foreign_births);
		UT_RUN(test_episode_multipage_init_wrong_foreign_epoch_closes_whole_group);
		UT_RUN(test_episode_multipage_init_real_eviction_stays_unknown);
		UT_RUN(test_episode_multipage_init_late_equal_bytes_keep_original_custody);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_INITIALIZED_PAGES") != NULL) {
		UT_PLAN(5);
		UT_RUN(test_episode_initialized_pages_uses_both_actual_producers);
		UT_RUN(test_episode_initialized_pages_refuses_each_unknown_origin);
		UT_RUN(test_episode_initialized_pages_refuses_equal_bytes_wrong_birth);
		UT_RUN(test_episode_initialized_pages_cache_loss_keeps_old_custody);
		UT_RUN(test_episode_initialized_pages_alias_has_no_native_effect);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_CURRENT_PAGES") != NULL) {
		UT_PLAN(4);
		UT_RUN(test_census_current_pages_keep_initial_baseline_after_owner_write);
		UT_RUN(test_census_current_pages_refuse_replaced_short_or_unsafe_objects);
		UT_RUN(test_census_current_pages_preconditions_do_not_enter_or_poison);
		UT_RUN(test_census_current_pages_final_source_error_and_index_drift_are_sticky);
		setup_fixture(); UT_DONE(); return ut_failed_count != 0;
	}

	if (getenv("PGRAC_PRE2_TEST_RESIDENT_BIRTH") != NULL) {
		UT_PLAN(3);
		UT_RUN(test_episode_resident_birth_reaches_original_page_and_query);
		UT_RUN(test_episode_resident_birth_does_not_borrow_wal_or_query_epoch);
		UT_RUN(test_episode_resident_birth_loss_poisoned_history_keeps_custody);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_ISSUED_CLOG") != NULL) {
		UT_PLAN(3);
		UT_RUN(test_episode_issued_clog_bytes_reach_original_strict_reader);
		UT_RUN(test_episode_issued_clog_raw_route_cannot_supply_full_record);
		UT_RUN(test_episode_issued_clog_wal_error_never_publishes_candidate);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_NATIVE_LOCAL_BOUNDS") != NULL) {
		UT_PLAN(3);
		UT_RUN(test_episode_native_bounds_advance_before_observation);
		UT_RUN(test_episode_native_bounds_counter_drift_keeps_custody);
		UT_RUN(test_episode_native_bounds_horizon_equality_and_crossing);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_LOCAL_CLOG_BOUNDS") != NULL) {
		UT_PLAN(4);
		UT_RUN(test_episode_local_bounds_keeps_original_reads_and_foreign_unknown);
		UT_RUN(test_episode_local_bounds_refuses_counter_tail_and_halfcycle);
		UT_RUN(test_episode_local_bounds_refuses_raw_horizon_without_rebasing);
		UT_RUN(test_episode_local_bounds_alias_and_late_failure_keep_custody);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_CONSUMED_NATIVE") != NULL) {
		UT_PLAN(4);
		UT_RUN(test_episode_consumed_native_redo_reaches_original_sync_and_whole_page);
		UT_RUN(test_episode_consumed_native_dirty_status_is_not_a_disk_commit);
		UT_RUN(test_episode_consumed_native_redo_cannot_rebase_an_earlier_held_page);
		UT_RUN(test_episode_consumed_native_redo_late_error_keeps_attempt_and_custody);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_CONSUMED_PAGES") != NULL) {
		UT_PLAN(5);
		UT_RUN(test_episode_consumed_pages_fresh_carriers_have_no_consumption);
		UT_RUN(test_episode_consumed_pages_native_founder_and_cold_foreign_share_original_reads);
		UT_RUN(test_episode_consumed_pages_foreign_late_failure_retains_all_custody);
		UT_RUN(test_episode_consumed_pages_foreign_native_receipt_cannot_replace_cold_apply);
		UT_RUN(test_episode_consumed_pages_alias_and_old_modes_keep_original_contracts);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_APPLIED_OWNER") != NULL) {
		UT_PLAN(4);
		UT_RUN(test_episode_applied_owner_whole_source_preflight_precedes_native_mutation);
		UT_RUN(test_episode_applied_owner_equivalent_plan_does_not_replace_original_attempt);
		UT_RUN(test_episode_applied_owner_scope_false_and_error_do_not_retry);
		UT_RUN(test_episode_applied_owner_stale_source_refuses_before_original_attempt);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_APPLIED_PAGES") != NULL) {
		UT_PLAN(4);
		UT_RUN(test_episode_applied_pages_fresh_carriers_cannot_supply_execution);
		UT_RUN(test_episode_applied_pages_joins_all_sources_in_original_pass);
		UT_RUN(test_episode_applied_pages_foreign_failure_keeps_whole_custody);
		UT_RUN(test_episode_applied_pages_opt_out_and_alias_keep_original_inputs);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_NATIVE_PAGES") != NULL) {
		UT_PLAN(4);
		UT_RUN(test_episode_native_pages_holds_all_origins_on_original_descriptors);
		UT_RUN(test_episode_native_pages_partial_failure_keeps_all_open_custody);
		UT_RUN(test_episode_native_pages_whole_capacity_budget_and_duplicate_preflight);
		UT_RUN(test_episode_native_pages_alias_error_and_final_recheck_keep_debt);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_EPISODE_BIRTH_TERMINAL") != NULL) {
		UT_PLAN(4);
		UT_RUN(test_episode_birth_terminal_joins_source_counter_and_both_polarities_in_one_pass);
		UT_RUN(test_episode_birth_terminal_refuses_old_terminal_without_exact_birth);
		UT_RUN(test_episode_birth_terminal_keeps_original_side_and_native_status_refusals);
		UT_RUN(test_episode_birth_terminal_whole_output_and_owner_error_keep_custody);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_EPISODE_BIRTH_MATCH") != NULL) {
		UT_PLAN(5);
		UT_RUN(test_episode_birth_match_retains_original_source_counter_and_complete_input);
		UT_RUN(test_episode_birth_match_refuses_absent_legacy_wrong_era_and_other_page);
		UT_RUN(test_episode_birth_match_rejects_duplicate_late_birth_reset_and_truncate);
		UT_RUN(test_episode_birth_match_preconditions_preserve_original_query_and_custody);
		UT_RUN(test_episode_birth_match_owner_error_keeps_original_held_fd_and_pins);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_EPISODE_CLOG_BIRTH") != NULL) {
		UT_PLAN(4);
		UT_RUN(test_episode_clog_birth_retains_full_native_image_beside_legacy_history);
		UT_RUN(test_episode_clog_birth_keeps_native_epochs_separate_from_counter_candidates);
		UT_RUN(test_episode_clog_birth_capacity_never_publishes_a_partial_history);
		UT_RUN(test_episode_clog_birth_late_error_keeps_original_held_fd_and_all_protections);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_CLOG_FULL_ROOTS") != NULL) {
		UT_PLAN(4);
		UT_RUN(test_clog_full_roots_keep_original_origin_and_page_extent);
		UT_RUN(test_clog_full_roots_reject_malformed_or_detached_births_stickily);
		UT_RUN(test_clog_full_roots_read_whole_original_file_and_revoke_on_drift);
		UT_RUN(test_clog_full_roots_keep_original_source_and_unstarted_union_guards);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_EPISODE_GENERATION_XID") != NULL) {
		UT_PLAN(4);
		UT_RUN(test_episode_generation_xid_keeps_every_original_source_and_counter);
		UT_RUN(test_episode_generation_xid_sibling_frames_cannot_hide_behind_the_map);
		UT_RUN(test_episode_generation_xid_sibling_half_range_refuses_without_rebase);
		UT_RUN(test_episode_generation_xid_late_error_retains_original_native_and_pins);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_EPISODE_CLOG_HISTORY") != NULL) {
		UT_PLAN(5);
		UT_RUN(test_episode_clog_history_keeps_every_original_zero_and_truncate);
		UT_RUN(test_episode_clog_history_keeps_unrelated_page_and_raw_horizon);
		UT_RUN(test_episode_clog_history_capacity_refuses_without_partial_output);
		UT_RUN(test_episode_clog_history_prefix_and_other_generation_remain_unobserved);
		UT_RUN(test_episode_clog_history_late_error_discards_output_keeps_original_custody);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_EPISODE_XID") != NULL) {
		UT_PLAN(7);
		UT_RUN(test_episode_xid_unrelated_relmap_retains_original_frame_identity_refusal);
		UT_RUN(test_episode_xid_keeps_original_checkpoint_map_and_complete_tail);
		UT_RUN(test_episode_xid_tail_header_and_checkpoint_use_original_counter_rules);
		UT_RUN(test_episode_xid_full_counter_bounds_keep_wrap_and_exact_half_range_distinct);
		UT_RUN(test_episode_xid_error_clears_private_pass_and_retains_original_native_fd);
		UT_RUN(test_episode_xid_missing_checkpoint_or_window_fails_original_episode);
		UT_RUN(test_episode_xid_output_and_caller_preconditions_preserve_original_custody);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_EPISODE_NATIVE_CLOG") != NULL) {
		UT_PLAN(3);
		UT_RUN(test_episode_native_clog_keeps_real_fd_and_all_status_observations);
		UT_RUN(test_episode_native_clog_inode_and_neighbor_change_revoke_same_episode);
		UT_RUN(test_episode_native_clog_error_retains_fd_and_original_protection);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_EPISODE_PAGES") != NULL) {
		UT_PLAN(2);
		UT_RUN(test_episode_pages_borrowed_output_preserves_complete_baseline);
		UT_RUN(test_episode_pages_changed_payload_revokes_copied_source);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_EPISODE_SOURCES") != NULL) {
		UT_PLAN(4);
	UT_RUN(test_episode_sources_keep_complete_zero_page_generations_by_value);
	UT_RUN(test_episode_sources_base_keeps_full_tail_after_native_map);
	UT_RUN(test_episode_sources_reject_borrowed_output_without_changing_baseline);
	UT_RUN(test_episode_sources_sibling_metadata_drift_revokes_complete_view);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_CENSUS_PAGE_PLAN") != NULL) {
		UT_PLAN(4);
	UT_RUN(test_census_page_graph_preserves_original_full_manifest_and_owned_plan);
	UT_RUN(test_census_page_graph_never_advances_prefix_or_enters_under_caller_lock);
	UT_RUN(test_census_page_graph_factory_failure_and_final_manifest_drift_are_sticky);
	UT_RUN(test_census_page_graph_error_invalidates_observation_without_releasing_input);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_EPISODE_HISTORY") != NULL) {
		UT_PLAN(8);
		UT_RUN(test_episode_history_selected_window_drift_invalidates_inventory);
		UT_RUN(test_episode_history_uses_all_original_sources_and_actual_window);
		UT_RUN(test_episode_history_requires_declaration_and_complete_original_holders);
		UT_RUN(test_episode_history_refusal_revokes_episode_and_keeps_protection);
		UT_RUN(test_episode_history_first_current_error_is_sticky);
		UT_RUN(test_episode_history_stops_before_second_pass_after_sibling_loss);
		UT_RUN(test_episode_history_retains_history_and_current_generations_together);
		UT_RUN(test_episode_history_budget_index_and_alias_refusals_do_not_rebase);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_CTRC_WAL_SOURCES") != NULL) {
		UT_PLAN(7);
		UT_RUN(test_wal_sources_include_all_records_without_page_inputs);
		UT_RUN(test_wal_sources_keep_explicit_empty_and_every_original_generation);
		UT_RUN(test_wal_sources_preserve_page_and_native_effect_cursors);
		UT_RUN(test_wal_sources_missing_incomplete_and_crc_valid_rewrite_refuse);
		UT_RUN(test_wal_sources_census_requires_full_manifest_and_retains_zero_page_sources);
		UT_RUN(test_wal_sources_census_scope_loss_or_original_rewrite_is_sticky);
		UT_RUN(test_wal_sources_census_full_source_bytes_remain_bound_to_original_manifest);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_CTRC_PAGE_WAL") != NULL) {
		UT_PLAN(8);
		UT_RUN(test_page_wal_census_requires_complete_original_observation);
		UT_RUN(test_page_wal_census_original_scope_loss_never_rebases);
		UT_RUN(test_page_wal_inputs_keep_all_components_payloads_and_original_sources);
		UT_RUN(test_page_wal_inputs_do_not_reset_native_effect_cursor_or_fixed_cut);
		UT_RUN(test_page_wal_inputs_empty_is_complete_and_partial_or_failed_is_not);
		UT_RUN(test_page_wal_inputs_snapshot_changes_refuse_without_following_rebound_pointers);
		UT_RUN(test_page_wal_inputs_crc_valid_original_rewrite_refuses_both_passes);
		UT_RUN(test_page_wal_inputs_charge_original_budget_and_never_export_a_prefix);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_CTRC_PAGE_INVENTORY") != NULL) {
		UT_PLAN(6);
	UT_RUN(test_owned_page_inventory_retains_every_space_and_raw_main_page);
	UT_RUN(test_owned_page_inventory_keeps_original_heap_layout_and_space_identity);
	UT_RUN(test_owned_page_inventory_never_exports_or_advances_an_observation_prefix);
	UT_RUN(test_owned_page_inventory_mutation_revokes_the_original_complete_view);
	UT_RUN(test_owned_page_inventory_scope_loss_is_sticky_without_rebasing_inputs);
	UT_RUN(test_owned_page_inventory_capacity_never_silently_omits_physical_pages);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_CTRC_PAGE_GRAPH") != NULL) {
		UT_PLAN(9);
		UT_RUN(test_page_graph_original_full_cuts_order_cross_thread_page_edges);
		UT_RUN(test_page_graph_no_page_sources_and_original_native_cursor_remain);
		UT_RUN(test_page_graph_keeps_explicit_empty_and_historical_generations);
		UT_RUN(test_page_graph_budget_refuses_without_exporting_a_prefix);
		UT_RUN(test_page_graph_crc_valid_original_rewrite_is_sticky);
		UT_RUN(test_page_graph_bad_codec_and_cycle_keep_original_debt);
		UT_RUN(test_page_graph_changed_native_checkpoint_never_rebases);
		UT_RUN(test_page_graph_native_error_cleans_private_state_only);
		UT_RUN(test_page_graph_refuses_caller_locks_before_any_original_io);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_EPISODE_BASE") != NULL) {
		UT_PLAN(9);
		UT_RUN(test_episode_base_requires_drain_and_retains_actual_query_source);
		UT_RUN(test_episode_base_rejects_unqualified_or_ambiguous_native_source);
		UT_RUN(test_episode_base_bound_query_cannot_restart_or_replace_custody);
		UT_RUN(test_episode_base_final_physical_reread_keeps_the_original_baseline);
		UT_RUN(test_episode_base_first_and_existing_errors_never_drop_protection);
		UT_RUN(test_episode_base_after_read_source_drift_discards_only_local_output);
		UT_RUN(test_episode_base_output_lock_and_budget_preconditions_do_not_poison);
		UT_RUN(test_episode_base_preserves_original_borrowed_page_storage);
		UT_RUN(test_episode_base_checks_original_page_bytes_before_and_after_native_read);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_EPISODE_TERMINAL") != NULL) {
		UT_PLAN(6);
		UT_RUN(test_episode_terminal_matches_actual_sealed_side_and_same_held_page);
		UT_RUN(test_episode_terminal_rejects_lifecycle_discontinuity_before_new_native_io);
		UT_RUN(test_episode_terminal_missing_conflicting_child_prepare_or_alias_is_not_terminal);
		UT_RUN(test_episode_terminal_raw_status_or_side_failure_retains_observed_native_custody);
		UT_RUN(test_episode_terminal_error_is_sticky_and_clears_only_transient_pass_state);
		UT_RUN(test_episode_terminal_preconditions_preserve_original_base_and_query);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_CLOG_SIDE") != NULL) {
		UT_PLAN(4);
		UT_RUN(test_episode_clog_side_matches_all_physical_generations_in_one_original_pass);
		UT_RUN(test_episode_clog_side_refuses_complete_native_header_and_payload_mismatches);
		UT_RUN(test_episode_clog_side_requires_every_original_plan_before_and_after_reread);
		UT_RUN(test_episode_clog_side_whole_output_owner_error_retains_original_custody);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_BIRTH_HALFSPACE") != NULL) {
		UT_PLAN(3);
		UT_RUN(test_episode_birth_halfspace_accepts_last_unambiguous_whole_page_position);
		UT_RUN(test_episode_birth_halfspace_refuses_exact_whole_page_half_cycle);
		UT_RUN(test_episode_birth_halfspace_refuses_later_tail_with_legal_query_distance);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_ALL_CLOG") != NULL) {
		UT_PLAN(4);
		UT_RUN(test_episode_all_clog_retains_every_source_without_changing_query_input);
		UT_RUN(test_episode_all_clog_keeps_foreign_full_birth_and_its_own_counter);
		UT_RUN(test_episode_all_clog_capacity_is_for_whole_set_not_query_origin);
		UT_RUN(test_episode_all_clog_whole_output_and_late_error_preserve_custody);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_PAGE_HISTORY") != NULL) {
		UT_PLAN(4);
		UT_RUN(test_episode_page_history_checks_later_nonquery_full_eras);
		UT_RUN(test_episode_page_history_refuses_postbirth_zero_and_truncate);
		UT_RUN(test_episode_page_history_uses_each_sources_whole_page_halfspace);
		UT_RUN(test_episode_page_history_preserves_prebirth_legacy_input);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_PAGE_SOURCE_SET") != NULL) {
		UT_PLAN(3);
		UT_RUN(test_episode_page_sources_refuse_same_origin_without_clog);
		UT_RUN(test_episode_page_sources_refuse_same_origin_without_counter);
		UT_RUN(test_episode_page_sources_preserve_unrelated_eventless_generation);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_CENSUS_REFERENCES") != NULL) {
		UT_PLAN(4);
		UT_RUN(test_census_references_own_every_untouched_heap_and_inverse_history_emission);
		UT_RUN(test_census_references_complete_empty_is_not_prefix_or_lost_scope);
		UT_RUN(test_census_references_reject_modified_metadata_tuple_and_rebound_pointer);
		UT_RUN(test_census_references_bound_metadata_and_tuple_storage_before_publication);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_CENSUS_NATIVE_RECORDS") != NULL) {
		UT_PLAN(4);
	UT_RUN(test_census_native_records_keep_entire_payload_source_and_drained_cursor);
	UT_RUN(test_census_native_records_prefix_locks_and_range_do_not_enter);
	UT_RUN(test_census_native_records_late_current_shape_and_error_are_sticky);
	UT_RUN(test_census_native_records_original_final_wal_rewrite_refuses_whole_view);
		UT_DONE(); return ut_failed_count != 0;
	}
	UT_PLAN(281);
	UT_RUN(test_census_native_records_keep_entire_payload_source_and_drained_cursor);
	UT_RUN(test_census_native_records_prefix_locks_and_range_do_not_enter);
	UT_RUN(test_census_native_records_late_current_shape_and_error_are_sticky);
	UT_RUN(test_census_native_records_original_final_wal_rewrite_refuses_whole_view);

	UT_RUN(test_census_tx_final_recheck_accepts_original_complete_files_and_empty_end);
	UT_RUN(test_census_tx_final_recheck_detects_same_inode_late_predecessor_rewrite);
	UT_RUN(test_census_tx_final_recheck_does_not_enter_under_locks_or_stale_source);
	UT_RUN(test_census_tx_references_join_all_original_current_and_history_items);
	UT_RUN(test_census_tx_references_prefix_empty_lock_and_range_are_distinct);
	UT_RUN(test_census_tx_references_late_source_graph_generation_and_error_are_sticky);
	UT_RUN(test_census_tx_dependencies_construct_original_roots_and_cross_relation_edge);
	UT_RUN(test_census_tx_dependencies_prefix_lock_and_range_do_not_enter);
	UT_RUN(test_census_tx_dependencies_late_source_count_error_revoke_whole_forest);

	UT_RUN(test_census_tx_records_include_real_cross_relation_predecessor);
	UT_RUN(test_census_tx_records_preconditions_and_range_do_not_advance);
	UT_RUN(test_census_tx_records_late_source_graph_error_and_count_are_sticky);
	UT_RUN(test_census_tx_records_original_second_read_refuses_file_rewrite);
	UT_RUN(test_episode_multipage_init_keeps_both_actual_foreign_births);
	UT_RUN(test_episode_multipage_init_wrong_foreign_epoch_closes_whole_group);
	UT_RUN(test_episode_multipage_init_real_eviction_stays_unknown);
	UT_RUN(test_episode_multipage_init_late_equal_bytes_keep_original_custody);
	UT_RUN(test_census_namespace_complete_set_keeps_original_pages_and_manifest);
	UT_RUN(test_census_namespace_added_object_revokes_private_census_not_original_pin);
	UT_RUN(test_census_namespace_preflight_and_entered_source_or_error_keep_pin);

	UT_RUN(test_episode_initialized_pages_uses_both_actual_producers);
	UT_RUN(test_episode_initialized_pages_refuses_each_unknown_origin);
	UT_RUN(test_episode_initialized_pages_refuses_equal_bytes_wrong_birth);
	UT_RUN(test_episode_initialized_pages_cache_loss_keeps_old_custody);
	UT_RUN(test_episode_initialized_pages_alias_has_no_native_effect);
	UT_RUN(test_census_current_pages_keep_initial_baseline_after_owner_write);
	UT_RUN(test_census_current_pages_refuse_replaced_short_or_unsafe_objects);
	UT_RUN(test_census_current_pages_preconditions_do_not_enter_or_poison);
	UT_RUN(test_census_current_pages_final_source_error_and_index_drift_are_sticky);
	UT_RUN(test_episode_resident_birth_reaches_original_page_and_query);
	UT_RUN(test_episode_resident_birth_does_not_borrow_wal_or_query_epoch);
	UT_RUN(test_episode_resident_birth_loss_poisoned_history_keeps_custody);
	UT_RUN(test_episode_issued_clog_bytes_reach_original_strict_reader);
	UT_RUN(test_episode_issued_clog_raw_route_cannot_supply_full_record);
	UT_RUN(test_episode_issued_clog_wal_error_never_publishes_candidate);

	UT_RUN(test_episode_native_bounds_advance_before_observation);
	UT_RUN(test_episode_native_bounds_counter_drift_keeps_custody);
	UT_RUN(test_episode_native_bounds_horizon_equality_and_crossing);
	UT_RUN(test_episode_local_bounds_keeps_original_reads_and_foreign_unknown);
	UT_RUN(test_episode_local_bounds_refuses_counter_tail_and_halfcycle);
	UT_RUN(test_episode_local_bounds_refuses_raw_horizon_without_rebasing);
	UT_RUN(test_episode_local_bounds_alias_and_late_failure_keep_custody);
	UT_RUN(test_episode_page_sources_refuse_same_origin_without_clog);
	UT_RUN(test_episode_page_sources_refuse_same_origin_without_counter);
	UT_RUN(test_episode_page_sources_preserve_unrelated_eventless_generation);
	UT_RUN(test_episode_page_history_checks_later_nonquery_full_eras);
	UT_RUN(test_episode_page_history_refuses_postbirth_zero_and_truncate);
	UT_RUN(test_episode_page_history_uses_each_sources_whole_page_halfspace);
	UT_RUN(test_episode_page_history_preserves_prebirth_legacy_input);
	UT_RUN(test_episode_consumed_native_redo_reaches_original_sync_and_whole_page);
	UT_RUN(test_episode_consumed_native_dirty_status_is_not_a_disk_commit);
	UT_RUN(test_episode_consumed_native_redo_cannot_rebase_an_earlier_held_page);
	UT_RUN(test_episode_consumed_native_redo_late_error_keeps_attempt_and_custody);
	UT_RUN(test_episode_consumed_pages_fresh_carriers_have_no_consumption);
	UT_RUN(test_episode_consumed_pages_native_founder_and_cold_foreign_share_original_reads);
	UT_RUN(test_episode_consumed_pages_foreign_late_failure_retains_all_custody);
	UT_RUN(test_episode_consumed_pages_foreign_native_receipt_cannot_replace_cold_apply);
	UT_RUN(test_episode_consumed_pages_alias_and_old_modes_keep_original_contracts);

	UT_RUN(test_episode_applied_owner_whole_source_preflight_precedes_native_mutation);
	UT_RUN(test_episode_applied_owner_equivalent_plan_does_not_replace_original_attempt);
	UT_RUN(test_episode_applied_owner_scope_false_and_error_do_not_retry);
	UT_RUN(test_episode_applied_owner_stale_source_refuses_before_original_attempt);

	UT_RUN(test_episode_native_pages_holds_all_origins_on_original_descriptors);
	UT_RUN(test_episode_native_pages_partial_failure_keeps_all_open_custody);
	UT_RUN(test_episode_native_pages_whole_capacity_budget_and_duplicate_preflight);
	UT_RUN(test_episode_native_pages_alias_error_and_final_recheck_keep_debt);
	UT_RUN(test_episode_applied_pages_fresh_carriers_cannot_supply_execution);
	UT_RUN(test_episode_applied_pages_joins_all_sources_in_original_pass);
	UT_RUN(test_episode_applied_pages_foreign_failure_keeps_whole_custody);
	UT_RUN(test_episode_applied_pages_opt_out_and_alias_keep_original_inputs);
	UT_RUN(test_episode_all_clog_retains_every_source_without_changing_query_input);
	UT_RUN(test_episode_all_clog_keeps_foreign_full_birth_and_its_own_counter);
	UT_RUN(test_episode_all_clog_capacity_is_for_whole_set_not_query_origin);
	UT_RUN(test_episode_all_clog_whole_output_and_late_error_preserve_custody);
	UT_RUN(test_episode_birth_halfspace_accepts_last_unambiguous_whole_page_position);
	UT_RUN(test_episode_birth_halfspace_refuses_exact_whole_page_half_cycle);
	UT_RUN(test_episode_birth_halfspace_refuses_later_tail_with_legal_query_distance);
	UT_RUN(test_episode_clog_side_matches_all_physical_generations_in_one_original_pass);
	UT_RUN(test_episode_clog_side_refuses_complete_native_header_and_payload_mismatches);
	UT_RUN(test_episode_clog_side_requires_every_original_plan_before_and_after_reread);
	UT_RUN(test_episode_clog_side_whole_output_owner_error_retains_original_custody);
	UT_RUN(test_episode_birth_terminal_joins_source_counter_and_both_polarities_in_one_pass);
	UT_RUN(test_episode_birth_terminal_refuses_old_terminal_without_exact_birth);
	UT_RUN(test_episode_birth_terminal_keeps_original_side_and_native_status_refusals);
	UT_RUN(test_episode_birth_terminal_whole_output_and_owner_error_keep_custody);
	UT_RUN(test_clog_full_roots_keep_original_origin_and_page_extent);
	UT_RUN(test_clog_full_roots_reject_malformed_or_detached_births_stickily);
	UT_RUN(test_clog_full_roots_read_whole_original_file_and_revoke_on_drift);
	UT_RUN(test_clog_full_roots_keep_original_source_and_unstarted_union_guards);
	UT_RUN(test_episode_clog_history_keeps_every_original_zero_and_truncate);
	UT_RUN(test_episode_clog_history_keeps_unrelated_page_and_raw_horizon);
	UT_RUN(test_episode_clog_history_capacity_refuses_without_partial_output);
	UT_RUN(test_episode_clog_history_prefix_and_other_generation_remain_unobserved);
	UT_RUN(test_episode_clog_history_late_error_discards_output_keeps_original_custody);
	UT_RUN(test_episode_clog_birth_retains_full_native_image_beside_legacy_history);
	UT_RUN(test_episode_clog_birth_keeps_native_epochs_separate_from_counter_candidates);
	UT_RUN(test_episode_clog_birth_capacity_never_publishes_a_partial_history);
	UT_RUN(test_episode_clog_birth_late_error_keeps_original_held_fd_and_all_protections);
	UT_RUN(test_episode_birth_match_retains_original_source_counter_and_complete_input);
	UT_RUN(test_episode_birth_match_refuses_absent_legacy_wrong_era_and_other_page);
	UT_RUN(test_episode_birth_match_rejects_duplicate_late_birth_reset_and_truncate);
	UT_RUN(test_episode_birth_match_preconditions_preserve_original_query_and_custody);
	UT_RUN(test_episode_birth_match_owner_error_keeps_original_held_fd_and_pins);
	UT_RUN(test_episode_generation_xid_keeps_every_original_source_and_counter);
	UT_RUN(test_episode_generation_xid_sibling_frames_cannot_hide_behind_the_map);
	UT_RUN(test_episode_generation_xid_sibling_half_range_refuses_without_rebase);
	UT_RUN(test_episode_generation_xid_late_error_retains_original_native_and_pins);
	UT_RUN(test_episode_xid_unrelated_relmap_retains_original_frame_identity_refusal);
	UT_RUN(test_episode_xid_keeps_original_checkpoint_map_and_complete_tail);
	UT_RUN(test_episode_xid_tail_header_and_checkpoint_use_original_counter_rules);
	UT_RUN(test_episode_xid_full_counter_bounds_keep_wrap_and_exact_half_range_distinct);
	UT_RUN(test_episode_xid_error_clears_private_pass_and_retains_original_native_fd);
	UT_RUN(test_episode_xid_missing_checkpoint_or_window_fails_original_episode);
	UT_RUN(test_episode_xid_output_and_caller_preconditions_preserve_original_custody);
	UT_RUN(test_episode_terminal_matches_actual_sealed_side_and_same_held_page);
	UT_RUN(test_episode_terminal_rejects_lifecycle_discontinuity_before_new_native_io);
	UT_RUN(test_episode_terminal_missing_conflicting_child_prepare_or_alias_is_not_terminal);
	UT_RUN(test_episode_terminal_raw_status_or_side_failure_retains_observed_native_custody);
	UT_RUN(test_episode_terminal_error_is_sticky_and_clears_only_transient_pass_state);
	UT_RUN(test_episode_terminal_preconditions_preserve_original_base_and_query);
	UT_RUN(test_episode_native_clog_keeps_real_fd_and_all_status_observations);
	UT_RUN(test_episode_native_clog_inode_and_neighbor_change_revoke_same_episode);
	UT_RUN(test_episode_native_clog_error_retains_fd_and_original_protection);
	UT_RUN(test_census_page_graph_preserves_original_full_manifest_and_owned_plan);
	UT_RUN(test_census_page_graph_never_advances_prefix_or_enters_under_caller_lock);
	UT_RUN(test_census_page_graph_factory_failure_and_final_manifest_drift_are_sticky);
	UT_RUN(test_census_page_graph_error_invalidates_observation_without_releasing_input);
	UT_RUN(test_page_graph_original_full_cuts_order_cross_thread_page_edges);
	UT_RUN(test_page_graph_no_page_sources_and_original_native_cursor_remain);
	UT_RUN(test_page_graph_keeps_explicit_empty_and_historical_generations);
	UT_RUN(test_page_graph_budget_refuses_without_exporting_a_prefix);
	UT_RUN(test_page_graph_crc_valid_original_rewrite_is_sticky);
	UT_RUN(test_page_graph_bad_codec_and_cycle_keep_original_debt);
	UT_RUN(test_page_graph_changed_native_checkpoint_never_rebases);
	UT_RUN(test_page_graph_native_error_cleans_private_state_only);
	UT_RUN(test_page_graph_refuses_caller_locks_before_any_original_io);
	UT_RUN(test_wal_sources_include_all_records_without_page_inputs);
	UT_RUN(test_wal_sources_keep_explicit_empty_and_every_original_generation);
	UT_RUN(test_wal_sources_preserve_page_and_native_effect_cursors);
	UT_RUN(test_wal_sources_missing_incomplete_and_crc_valid_rewrite_refuse);
	UT_RUN(test_wal_sources_census_requires_full_manifest_and_retains_zero_page_sources);
	UT_RUN(test_wal_sources_census_scope_loss_or_original_rewrite_is_sticky);
	UT_RUN(test_wal_sources_census_full_source_bytes_remain_bound_to_original_manifest);
	UT_RUN(test_page_wal_census_requires_complete_original_observation);
	UT_RUN(test_page_wal_census_original_scope_loss_never_rebases);
	UT_RUN(test_page_wal_inputs_keep_all_components_payloads_and_original_sources);
	UT_RUN(test_page_wal_inputs_do_not_reset_native_effect_cursor_or_fixed_cut);
	UT_RUN(test_page_wal_inputs_empty_is_complete_and_partial_or_failed_is_not);
	UT_RUN(test_page_wal_inputs_snapshot_changes_refuse_without_following_rebound_pointers);
	UT_RUN(test_page_wal_inputs_crc_valid_original_rewrite_refuses_both_passes);
	UT_RUN(test_page_wal_inputs_charge_original_budget_and_never_export_a_prefix);
	UT_RUN(test_owned_page_inventory_retains_every_space_and_raw_main_page);
	UT_RUN(test_owned_page_inventory_keeps_original_heap_layout_and_space_identity);
	UT_RUN(test_owned_page_inventory_never_exports_or_advances_an_observation_prefix);
	UT_RUN(test_owned_page_inventory_mutation_revokes_the_original_complete_view);
	UT_RUN(test_owned_page_inventory_scope_loss_is_sticky_without_rebasing_inputs);
	UT_RUN(test_owned_page_inventory_capacity_never_silently_omits_physical_pages);

	UT_RUN(test_ctrc_inventory_keeps_original_local_cut_and_native_terminal_bytes);
	UT_RUN(test_owned_ctrc_census_borrows_namespace_and_revokes_changed_work);
	UT_RUN(test_owned_ctrc_census_rejects_other_purpose_and_namespace_switch);
	UT_RUN(test_native_inventory_uses_two_real_cold_passes);
	UT_RUN(test_native_inventory_never_returns_partial_on_drift_io_or_error);
	UT_RUN(test_native_inventory_capacity_is_not_partial_success);
	UT_RUN(test_native_inventory_open_unknown_and_online_never_observed);
	UT_RUN(test_native_inventory_keeps_real_empty_sources_and_other_obligations);
	UT_RUN(test_native_inventory_detects_non_native_rewrite);
	UT_RUN(test_native_inventory_collects_real_fsm_and_all_physical_roots);
	UT_RUN(test_native_inventory_poison_targets_on_error_and_drift);
	UT_RUN(test_record_transcript_full_fields_and_storage_independence);
	UT_RUN(test_record_transcript_rejects_unsafe_bounds);
	UT_RUN(test_final_input_recheck_preserves_effect_cursor_and_all_sources);
	UT_RUN(test_final_input_recheck_is_sticky_on_bytes_scope_io_and_error);
	UT_RUN(test_native_inventory_collects_undo_targets_from_real_retained_wal);
	UT_RUN(test_cold_inventory_owns_undo_records_and_rechecks_after_export);
	UT_RUN(test_cold_inventory_does_not_hide_invalid_undo_in_other_records);
	UT_RUN(test_wal_heads_seed_reused_slots_through_real_files);
	UT_RUN(test_wal_heads_require_prior_same_source_identity);
	UT_RUN(test_wal_head_seeding_preserves_effect_cursor);
	UT_RUN(test_wal_head_final_recheck_and_destroy_revoke_graph);
	UT_RUN(test_wal_heads_reject_late_graph_and_partial_capacity);
	UT_RUN(test_wal_head_bounded_search_and_lifecycle_barriers);
	UT_RUN(test_cold_multixact_records_keep_all_shapes_and_origin_sources);
	UT_RUN(test_cold_multixact_second_and_final_reads_revoke_observations);
	UT_RUN(test_cold_multixact_unknown_and_bad_shapes_never_become_other);
	UT_RUN(test_cold_mx_join_visits_all_saved_creates_and_preserves_effects);
	UT_RUN(test_cold_mx_join_final_wal_drift_and_destroy_revoke_roots);
	UT_RUN(test_cold_mx_join_refusal_poisons_inventory_and_partial_roots);
	UT_RUN(test_owned_census_page_roots_without_tt_or_wal_heads);
	UT_RUN(test_page_root_deferred_base_preserves_independent_wal_forest);
	UT_RUN(test_page_root_deferred_base_never_replaces_later_coverage);
	UT_RUN(test_owned_census_real_inputs_all_observation_families);
	UT_RUN(test_owned_census_final_rereads_refuse_changes);
	UT_RUN(test_owned_census_scope_loss_and_drained_invalidation);
	UT_RUN(test_owned_census_missing_and_short_files_refuse);
	UT_RUN(test_owned_census_error_releases_children);
	UT_RUN(test_owned_census_open_input_and_budget_refuse);
	UT_RUN(test_owned_census_destroy_each_phase);
	UT_RUN(test_owned_census_target_obligations_keep_absence_extent_and_unsupported_forks);
	UT_RUN(test_owned_census_target_join_and_final_observation_revoke_changed_inputs);
	UT_RUN(test_owned_census_enumerates_raw_native_slru_slots);
	UT_RUN(test_owned_census_native_slru_drift_and_scope_loss_revoke_outputs);
	UT_RUN(test_cold_slru_records_keep_all_shapes_and_origin_targets);
	UT_RUN(test_cold_slru_second_and_final_reads_revoke_the_union);
	UT_RUN(test_cold_slru_bad_records_never_become_other);
	UT_RUN(test_cold_terminal_assignment_target_union_without_zero);
	UT_RUN(test_cold_child_record_drift_revokes_all_targets);
	UT_RUN(test_owned_census_missing_or_short_terminal_clog_refuses);
	UT_RUN(test_cold_commit_ts_keeps_original_profile_refusal);
	UT_RUN(test_cold_xact_join_keeps_every_source_and_raw_effect);
	UT_RUN(test_cold_xact_join_final_wal_roots_destroy_revoke_outputs);
	UT_RUN(test_owned_xact_join_still_requires_final_whole_inputs);
	UT_RUN(test_cold_relmap_owns_images_entries_and_all_target_paths);
	UT_RUN(test_cold_relmap_second_and_final_reads_revoke_observations);
	UT_RUN(test_cold_relmap_malformed_or_unknown_never_becomes_other);
	UT_RUN(test_owned_census_maps_keep_all_copy_edges);
	UT_RUN(test_owned_census_maps_final_root_and_wal_rechecks);
	UT_RUN(test_owned_census_bad_map_revokes_all_children);
	UT_RUN(test_cold_prepare_full_payload_and_polarities_remain_unowned);
	UT_RUN(test_cold_prepare_two_pass_and_final_drift_revoke_roots);
	UT_RUN(test_cold_prepare_invalid_or_completion_records_never_become_other);
	UT_RUN(test_census_manifest_requires_original_final_reads);
	UT_RUN(test_inventory_digest_binds_every_source_cut_tail_and_transcript);
	UT_RUN(test_census_manifest_revokes_after_scope_or_dependency_loss);
	UT_RUN(test_census_manifest_final_changes_never_export_partial);
	UT_RUN(test_inventory_digest_canonical_sha256_and_failed_output);
	UT_RUN(test_terminal_edges_keep_every_exact_and_native_carrier);
	UT_RUN(test_terminal_edges_never_guess_missing_or_reused_bindings);
	UT_RUN(test_terminal_edges_preserve_effects_and_final_revocation);
	UT_RUN(test_terminal_binding_search_is_bounded);
	UT_RUN(test_terminal_census_compares_original_physical_slots);
	UT_RUN(test_terminal_census_final_changes_revoke_output);
	UT_RUN(test_terminal_bad_exact_fields_refuse_and_revoke);
	UT_RUN(test_terminal_lookup_exact_identity_and_unbound_records);
	UT_RUN(test_terminal_lookup_consistent_and_conflicting_endpoints);
	UT_RUN(test_terminal_lookup_bounded_query_and_key_change);
	UT_RUN(test_terminal_lookup_final_input_drift_revokes);
	UT_RUN(test_physical_tt_terminal_census_keeps_every_slot);
	UT_RUN(test_physical_tt_terminal_census_final_rechecks);
	UT_RUN(test_terminal_lookup_invalid_keys_and_incomplete_cursor_refuse);
	UT_RUN(test_episode_history_selected_window_drift_invalidates_inventory);
	UT_RUN(test_episode_history_uses_all_original_sources_and_actual_window);
	UT_RUN(test_episode_history_requires_declaration_and_complete_original_holders);
	UT_RUN(test_episode_history_refusal_revokes_episode_and_keeps_protection);
	UT_RUN(test_episode_history_first_current_error_is_sticky);
	UT_RUN(test_episode_history_stops_before_second_pass_after_sibling_loss);
	UT_RUN(test_episode_history_retains_history_and_current_generations_together);
	UT_RUN(test_episode_history_budget_index_and_alias_refusals_do_not_rebase);
	UT_RUN(test_relmap_fold_matches_full_native_images_and_preserves_cursors);
	UT_RUN(test_relmap_fold_keeps_terminals_children_and_lifecycle_obligations);
	UT_RUN(test_relmap_fold_never_discards_historical_raw_aliases);
	UT_RUN(test_relmap_fold_is_bounded_and_pins_the_complete_query);
	UT_RUN(test_relmap_fold_refusal_and_final_drift_revoke_dependents);
	UT_RUN(test_relmap_fold_counts_empty_origins_and_refuses_invalid_queries);
	UT_RUN(test_episode_pending_keeps_full_history_and_lifecycle_obligations);
	UT_RUN(test_episode_pending_bounds_ticks_and_rejects_changed_query_or_holder);
	UT_RUN(test_episode_pending_first_current_error_retains_original_protection);
	UT_RUN(test_episode_pending_final_reread_drift_never_revives_the_query);
	UT_RUN(test_episode_pending_output_and_caller_lock_preconditions_preserve_custody);
	UT_RUN(test_episode_base_requires_drain_and_retains_actual_query_source);
	UT_RUN(test_episode_base_rejects_unqualified_or_ambiguous_native_source);
	UT_RUN(test_episode_base_bound_query_cannot_restart_or_replace_custody);
	UT_RUN(test_episode_base_final_physical_reread_keeps_the_original_baseline);
	UT_RUN(test_episode_base_first_and_existing_errors_never_drop_protection);
	UT_RUN(test_episode_base_after_read_source_drift_discards_only_local_output);
	UT_RUN(test_episode_base_output_lock_and_budget_preconditions_do_not_poison);
	UT_RUN(test_episode_base_preserves_original_borrowed_page_storage);
	UT_RUN(test_episode_base_checks_original_page_bytes_before_and_after_native_read);
	UT_RUN(test_episode_sources_keep_complete_zero_page_generations_by_value);
	UT_RUN(test_episode_sources_base_keeps_full_tail_after_native_map);
	UT_RUN(test_episode_sources_reject_borrowed_output_without_changing_baseline);
	UT_RUN(test_episode_sources_sibling_metadata_drift_revokes_complete_view);
	UT_RUN(test_episode_pages_borrowed_output_preserves_complete_baseline);
	UT_RUN(test_episode_pages_changed_payload_revokes_copied_source);
	UT_RUN(test_census_references_own_every_untouched_heap_and_inverse_history_emission);
	UT_RUN(test_census_references_complete_empty_is_not_prefix_or_lost_scope);
	UT_RUN(test_census_references_reject_modified_metadata_tuple_and_rebound_pointer);
	UT_RUN(test_census_references_bound_metadata_and_tuple_storage_before_publication);
	UT_DONE();
	return ut_failed_count != 0;
}
