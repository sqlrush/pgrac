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
#include "cluster/cluster_recovery_native_inventory.h"
#include "cluster/cluster_side_undo.h"
#include "cluster/cluster_recovery_record_hash.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "cluster/storage/cluster_undo_alloc.h"
#include "../../backend/cluster/cluster_uba.c"
#include "test_cluster_heap_small_scn.inc"

static unsigned inventory_census_calls;
static void (*inventory_between_passes)(void);

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
#include "../../backend/cluster/cluster_recovery_native_inventory.c"
#undef cluster_wal_inputs_cold_census_v1

static ClusterWalSourceRef inventory_fault_source;
static XLogRecPtr inventory_commit_start, inventory_commit_previous;
static unsigned inventory_fault;
static unsigned inventory_extra;
static unsigned inventory_binding_case;
static XLogRecPtr inventory_extra_start, inventory_extra_previous;
static XLogRecPtr inventory_mx_start, inventory_mx_previous;

/* These small records stay within the original checkpoint's first WAL page.
 * Header, main-data framing and CRC are actual native-reader inputs. */
static WalTestRecord
inventory_write_record(const ClusterWalSourceRef *source, XLogRecPtr at, XLogRecPtr previous,
					   RmgrId rmid, uint8 info, const void *payload, uint8 length)
{
	uint8 bytes[512] = { 0 };
	XLogRecord record = { 0 };
	char path[MAXPGPATH];
	int fd;

	record.xl_tot_len = SizeOfXLogRecord + 2 + length;
	record.xl_prev = previous;
	record.xl_rmid = rmid;
	record.xl_info = info;
	record.xl_xid = 4195136;
	record.xl_scn = 5906;
	bytes[SizeOfXLogRecord] = XLR_BLOCK_ID_DATA_SHORT;
	bytes[SizeOfXLogRecord + 1] = length;
	memcpy(bytes + SizeOfXLogRecord + 2, payload, length);
	INIT_CRC32C(record.xl_crc);
	COMP_CRC32C(record.xl_crc, bytes + SizeOfXLogRecord, 2 + length);
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
		if (inventory_extra == 5)
			extra = inventory_write_bound_head(&source, commit, node);
		if (inventory_extra >= 6 && inventory_extra <= 8)
			extra = inventory_write_mx_sequence(&source, commit, node);
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
		if (mode == 1)
			UT_ASSERT_EQ(diag.native_result, CLUSTER_RECOVERY_NATIVE_UNSUPPORTED);
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

int
main(int argc, char **argv)
{
	(void)argc;
	setvbuf(stdout, NULL, _IOLBF, 0);
	test_program_path = argv[0];
	setup_fixture();
	UT_PLAN(24);
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
	UT_DONE();
	return ut_failed_count != 0;
}
