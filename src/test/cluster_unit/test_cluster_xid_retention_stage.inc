/*-------------------------------------------------------------------------
 * test_cluster_xid_retention_stage.inc
 *     Original retained transaction and native FULL CLOG through v2 stage.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_xid_retention_stage.inc
 * NOTES
 *     Qualified directory, authority, locks, WAL transport and pair I/O are
 *     controlled. Native xact, allocator, CLOG/SLRU and stage bodies execute.
 *-------------------------------------------------------------------------
 */
#include "utils/relmapper.h"
#include "catalog/pg_tablespace_d.h"

Oid MyDatabaseTableSpace = DEFAULTTABLESPACE_OID;
bool allowSystemTableMods;
static RelMapFile stage_base, stage_updates, stage_wal_image;
static xl_relmap_update stage_wal_header;
static unsigned stage_pair_reads, stage_pair_writes, stage_inserts, stage_flushes;
static unsigned stage_registered, stage_fault;
static uint64 stage_committed[2];
static ClusterRelmapV2Owner stage_written[2];

FullTransactionId
GetCurrentFullTransactionIdIfAny(void)
{
	return CurrentTransactionState->fullTransactionId;
}

ClusterRelmapV2PairResult
cluster_relmap_authority_v2_read(const ClusterRelmapV2Namespace *identity,
	ClusterRelmapV2View *out)
{
	unsigned slot = identity->shared ? 0 : 1;

	UT_ASSERT(caller_global_x && !held && !retention_held && CritSectionCount == 0);
	UT_ASSERT_EQ(identity->system_identifier, retention_sysid);
	UT_ASSERT_EQ(identity->database_incarnation, retention_dbinc);
	UT_ASSERT_EQ(identity->dbid, identity->shared ? InvalidOid : MyDatabaseId);
	stage_pair_reads++;
	memset(out, 0, sizeof(*out));
	out->committed = stage_base;
	out->committed_generation = stage_committed[slot];
	out->pending_complete = true;
	if (stage_fault == 1) caller_boot++;
	return CLUSTER_RELMAP_V2_PAIR_VALID;
}

void
XLogBeginInsert(void)
{
	XidNativeClogState state;

	test_xid_native_clog_state(&state);
	UT_ASSERT(retention_mapping_held && !held && !retention_held && CritSectionCount == 1);
	UT_ASSERT_EQ(U64FromFullTransactionId(XactTopFullTransactionId),
		U64FromFullTransactionId(variables.clusterLastIssuedXid));
	UT_ASSERT_EQ(relmap_runtime_shmem->directories[retention_origin].last_issued,
		U64FromFullTransactionId(XactTopFullTransactionId));
	UT_ASSERT_EQ(proc.xid, XidFromFullTransactionId(XactTopFullTransactionId));
	UT_ASSERT(state.inserts != 0 && state.page_zero && !state.xid_held && !state.slru_held);
	stage_registered = 0;
}

void
XLogRegisterData(char *data, uint32 length)
{
	if (stage_registered++ == 0) {
		UT_ASSERT_EQ(length, MinSizeOfRelmapUpdate);
		memcpy(&stage_wal_header, data, length);
	} else {
		UT_ASSERT_EQ(stage_registered, 2);
		UT_ASSERT_EQ(length, sizeof(stage_wal_image));
		memcpy(&stage_wal_image, data, length);
	}
}

XLogRecPtr
XLogInsert(RmgrId rmid, uint8 info)
{
	UT_ASSERT(retention_mapping_held && CritSectionCount == 1);
	UT_ASSERT_EQ(rmid, RM_RELMAP_ID);
	UT_ASSERT_EQ(info, XLOG_RELMAP_UPDATE);
	UT_ASSERT_EQ(stage_registered, 2);
	UT_ASSERT_EQ(stage_wal_header.nbytes, sizeof(RelMapFile));
	stage_inserts++;
	return 16384 + stage_inserts;
}

void
XLogFlush(XLogRecPtr end)
{
	UT_ASSERT(retention_mapping_held && !held && !retention_held && CritSectionCount == 1);
	UT_ASSERT_EQ(end, 16384 + stage_inserts);
	stage_flushes++;
}

bool
cluster_relmap_authority_v2_write_pending(const ClusterRelmapV2Namespace *identity,
	uint64 generation, const ClusterRelmapV2Owner *owner, const RelMapFile *pending,
	ClusterRelmapRetentionDirectory *directory, LWLock *lock)
{
	ClusterRelmapRetentionKey key = { 0 };
	unsigned slot = identity->shared ? 0 : 1;

	UT_ASSERT(retention_mapping_held && !held && !retention_held && CritSectionCount == 0);
	UT_ASSERT(lock == &relmap_runtime_shmem->lock);
	UT_ASSERT(directory == &relmap_runtime_shmem->directories[retention_origin]);
	UT_ASSERT_EQ(stage_flushes, stage_inserts);
	UT_ASSERT_EQ(owner->wal_end, 16384 + stage_inserts);
	UT_ASSERT_EQ(generation, stage_committed[slot] + 1);
	UT_ASSERT_EQ(stage_wal_header.dbid, identity->dbid);
	UT_ASSERT_EQ(stage_wal_header.tsid,
		identity->shared ? GLOBALTABLESPACE_OID : MyDatabaseTableSpace);
	UT_ASSERT_EQ(memcmp(pending, &stage_wal_image, sizeof(*pending)), 0);
	key.identity = *identity; key.generation = generation; key.origin = owner->node;
	key.top_full_xid = owner->top_full_xid; key.membership_epoch = owner->membership_epoch;
	key.admitted_incarnation = owner->admitted_incarnation;
	key.boot_incarnation = owner->boot_incarnation;
	LWLockAcquire(lock, LW_EXCLUSIVE);
	UT_ASSERT_EQ(cluster_relmap_retention_stage_start(directory, &key, owner->wal_end),
		CLUSTER_RELMAP_RETENTION_OK);
	LWLockRelease(lock);
	stage_pair_writes++;
	stage_written[slot] = *owner;
	if (stage_fault == 2) return false;
	if (stage_fault == 3)
		ereport(ERROR, (errcode(ERRCODE_DISK_FULL), errmsg("controlled pending pair error")));
	return true;
}

#include "test_cluster_xid_retention_stage_product.inc"

static void
retained_stage_start(uint64 first, bool striped, bool fail)
{
	retained_xact_start(first, striped, fail);
	caller_retained_reader = retained_capture_allocate;
	retained_capture_drift = 0;
	retention_owner_begin = true;
	stage_pair_reads = stage_pair_writes = stage_inserts = stage_flushes = 0;
	stage_registered = stage_fault = 0;
	stage_committed[0] = 7; stage_committed[1] = 8;
	memset(stage_written, 0, sizeof(stage_written));
	memset(&stage_base, 0, sizeof(stage_base));
	stage_base.magic = RELMAPPER_FILEMAGIC;
	stage_base.num_mappings = 1;
	stage_base.mappings[0].mapoid = 1259;
	stage_base.mappings[0].mapfilenumber = 800;
	INIT_CRC32C(stage_base.crc);
	COMP_CRC32C(stage_base.crc, &stage_base, offsetof(RelMapFile, crc));
	FIN_CRC32C(stage_base.crc);
	stage_updates = stage_base;
	stage_updates.mappings[0].mapfilenumber = 900;
	allowSystemTableMods = false;
	MyDatabaseTableSpace = DEFAULTTABLESPACE_OID;
}

static bool
retained_stage_error(ClusterRelmapOwnerSampleV2 *owner, ClusterRelmapV2Image *image)
{
	volatile bool caught = false;

	retention_error_expected = true;
	PG_TRY();
	{
		(void)cluster_relmap_producer_stage_v2(true, &stage_updates, owner, image);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	/* Simulate only the original outer native ERROR lock cleanup. */
	if (held) { HOLD_INTERRUPTS(); LWLockRelease(XidGenLock); }
	if (retention_mapping_held) { HOLD_INTERRUPTS(); LWLockRelease(RelationMappingLock); }
	retention_error_expected = false;
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT(!retention_held);
	return caught;
}

static void
retained_stage_pin(uint64 full_xid)
{
	retained_clog_pin(full_xid);
	/* Preserve an unrelated database's obligation while staging either local
	 * namespace. An existing pin in the same namespace correctly conflicts. */
	relmap_runtime_shmem->directories[retention_origin].pins[0].key.identity = caller_namespace(false);
	relmap_runtime_shmem->directories[retention_origin].pins[0].key.identity.dbid = MyDatabaseId + 1;
}

UT_TEST(retained_stage_native_gap_and_rollover_precede_relmap_wal)
{
	for (unsigned which = 0; which < 4; which++) {
		uint64 first = which & 2 ? UINT64_C(0x7fffffffe) : UINT64_C(0x500007ffe);
		uint64 top = which & 2 ? UINT64_C(0x800000004) : UINT64_C(0x500008004);
		ClusterRelmapOwnerSampleV2 owner = { 0 };
		ClusterRelmapV2Image image = { 0 };
		ClusterRelmapRetentionPin old;
		XidNativeClogState state;
		XidNativeClogReplayState replay;

		retained_stage_start(first, true, false);
		retained_stage_pin(first - 1);
		old = relmap_runtime_shmem->directories[4].pins[0];
		UT_ASSERT(cluster_relmap_producer_stage_v2((which & 1) != 0, &stage_updates, &owner, &image));
		test_xid_native_clog_state(&state);
		retained_clog_birth(&state, which & 2 ? 4 : 7,
			which & 2 ? UINT64_C(0x800000003) : UINT64_C(0x500008000));
		UT_ASSERT_EQ(owner.owner.top_full_xid, top);
		UT_ASSERT_EQ(image.pending_owner.top_full_xid, top);
		UT_ASSERT_EQ(image.pending.mappings[0].mapfilenumber, 900);
		UT_ASSERT_EQ(stage_pair_reads, 2);
		UT_ASSERT_EQ(stage_inserts, 1); UT_ASSERT_EQ(stage_flushes, 1); UT_ASSERT_EQ(stage_pair_writes, 1);
		UT_ASSERT_EQ(memcmp(&old, relmap_runtime_shmem->directories[4].pins, sizeof(old)), 0);
		UT_ASSERT_EQ(relmap_runtime_shmem->directories[4].pins[1].key.top_full_xid, top);
		UT_ASSERT(relmap_runtime_shmem->directories[4].pins[1].may_be_staged);
		UT_ASSERT_EQ(relmap_runtime_shmem->directories[4].pins[1].wal_end, image.pending_owner.wal_end);
		UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), top + 1);
		test_xid_native_clog_replay(XID_CLOG_REPLAY_VALID, &replay);
		UT_ASSERT(replay.io_ok && !replay.caught && replay.target_zero && replay.neighbor_sentinel);
		UT_ASSERT_EQ(replay.writes, 1);
		UT_ASSERT_EQ(caller_owner_lock_errors | lock_errors | native_publish_order_errors, 0);
		UT_ASSERT(!held && !retention_held && !retention_mapping_held);
		retained_clog_stop();
	}
}

UT_TEST(retained_stage_native_two_namespaces_keep_top_across_child)
{
	TransactionStateData child = { 0 };
	ClusterRelmapOwnerSampleV2 owner[2] = { 0 };
	ClusterRelmapV2Image image[2] = { 0 };
	ClusterRelmapRetentionPin pins[3];
	XidNativeClogState before, after;

	retained_stage_start(UINT64_C(0x500008000), false, false);
	retained_stage_pin(UINT64_C(0x500007fff));
	UT_ASSERT(cluster_relmap_producer_stage_v2(true, &stage_updates, &owner[0], &image[0]));
	UT_ASSERT(cluster_relmap_producer_stage_v2(false, &stage_updates, &owner[1], &image[1]));
	UT_ASSERT_EQ(owner[0].owner.top_full_xid, UINT64_C(0x500008000));
	UT_ASSERT_EQ(owner[1].owner.top_full_xid, owner[0].owner.top_full_xid);
	UT_ASSERT_EQ(stage_inserts, 2); UT_ASSERT_EQ(stage_flushes, 2); UT_ASSERT_EQ(stage_pair_writes, 2);
	UT_ASSERT_EQ(stage_written[0].top_full_xid, stage_written[1].top_full_xid);
	memcpy(pins, relmap_runtime_shmem->directories[4].pins, sizeof(pins));
	test_xid_native_clog_state(&before);
	child.state = TRANS_INPROGRESS; child.nestingLevel = 2;
	child.parent = &TopTransactionStateData; child.curTransactionOwner = (ResourceOwner)(uintptr_t)12;
	CurrentTransactionState = &child;
	UT_ASSERT_EQ(U64FromFullTransactionId(xact_source_current()), UINT64_C(0x500008001));
	test_xid_native_clog_state(&after);
	UT_ASSERT_EQ(after.inserts, before.inserts);
	UT_ASSERT_EQ(after.calls, before.calls + 1);
	UT_ASSERT_EQ(xact_parent[0], (uint32)owner[0].owner.top_full_xid);
	UT_ASSERT_EQ(memcmp(pins, relmap_runtime_shmem->directories[4].pins, sizeof(pins)), 0);
	UT_ASSERT_EQ(relmap_runtime_shmem->directories[4].last_issued, UINT64_C(0x500008001));
	UT_ASSERT_EQ(full_clog_fixture.raw_calls | caller_owner_lock_errors | lock_errors, 0);
	retained_clog_stop();
}

UT_TEST(retained_stage_native_early_guards_preserve_all_native_state)
{
	for (unsigned which = 0; which < 4; which++) {
		ClusterRelmapOwnerSampleV2 owner = { 0 }, saved_owner;
		ClusterRelmapV2Image image = { 0 }, saved_image;
		ClusterRelmapRetentionDirectory saved;
		VariableCacheData native;
		XidNativeClogState state;

		retained_stage_start(UINT64_C(0x500008000), false, false);
		retained_stage_pin(UINT64_C(0x500007fff));
		if (which == 0) caller_writer_ready = false;
		if (which == 1) caller_global_x = false;
		if (which == 2) relmap_runtime_shmem->directories[4].state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
		if (which == 3) relmap_runtime_shmem->directories[4].pins[0].key.top_full_xid -= UINT64_C(0x80000000) - 1;
		saved = relmap_runtime_shmem->directories[4]; native = variables;
		memset(&owner, 0x5a, sizeof(owner)); saved_owner = owner;
		memset(&image, 0x3c, sizeof(image)); saved_image = image;
		if (which < 2)
			UT_ASSERT(!cluster_relmap_producer_stage_v2(true, &stage_updates, &owner, &image));
		else
			UT_ASSERT(retained_stage_error(&owner, &image));
		test_xid_native_clog_state(&state);
		UT_ASSERT_EQ(state.calls | state.inserts | stage_inserts | stage_pair_reads | stage_pair_writes, 0);
		UT_ASSERT_EQ(memcmp(&saved, &relmap_runtime_shmem->directories[4], sizeof(saved)), 0);
		UT_ASSERT_EQ(memcmp(&native, &variables, sizeof(native)), 0);
		UT_ASSERT_EQ(memcmp(&owner, &saved_owner, sizeof(owner)), 0);
		UT_ASSERT_EQ(memcmp(&image, &saved_image, sizeof(image)), 0);
		retained_clog_stop();
	}
}

static void
retained_stage_allocation_failure(bool native_error)
{
	ClusterRelmapOwnerSampleV2 owner, saved_owner;
	ClusterRelmapV2Image image, saved_image;
	ClusterRelmapRetentionPin old;
	XidNativeClogState first, again;

	retained_stage_start(UINT64_C(0x500008000), false, native_error);
	retained_stage_pin(UINT64_C(0x500007fff));
	old = relmap_runtime_shmem->directories[4].pins[0];
	retained_clog_invalidate = !native_error;
	memset(&owner, 0x5a, sizeof(owner)); saved_owner = owner;
	memset(&image, 0x3c, sizeof(image)); saved_image = image;
	UT_ASSERT(retained_stage_error(&owner, &image));
	test_xid_native_clog_state(&first);
	UT_ASSERT_EQ(first.inserts, 1);
	UT_ASSERT_EQ(first.dirty_pages, 1);
	UT_ASSERT_EQ(native_publish_count | proc.xid | stage_inserts | stage_pair_reads | stage_pair_writes, 0);
	UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), UINT64_C(0x500008000));
	UT_ASSERT_EQ(memcmp(&old, relmap_runtime_shmem->directories[4].pins, sizeof(old)), 0);
	UT_ASSERT_EQ(memcmp(&owner, &saved_owner, sizeof(owner)), 0);
	UT_ASSERT_EQ(memcmp(&image, &saved_image, sizeof(image)), 0);
	/* Repairing the controlled gate cannot retry this failed xact allocation. */
	retained_clog_invalidate = false;
	relmap_runtime_shmem->directories[4].state = CLUSTER_RELMAP_RETENTION_COMPLETE;
	UT_ASSERT(retained_stage_error(&owner, &image));
	test_xid_native_clog_state(&again);
	UT_ASSERT_EQ(again.calls, first.calls); UT_ASSERT_EQ(again.inserts, first.inserts);
	UT_ASSERT_EQ(stage_inserts | stage_pair_reads | stage_pair_writes, 0);
	retained_clog_stop();
}

UT_TEST(retained_stage_native_wal_error_keeps_original_unissued_debt)
{
	retained_stage_allocation_failure(true);
}

UT_TEST(retained_stage_native_late_guard_keeps_birth_without_relmap)
{
	retained_stage_allocation_failure(false);
}

UT_TEST(retained_stage_native_writer_drift_keeps_actual_issuance_only)
{
	ClusterRelmapOwnerSampleV2 owner, saved_owner;
	ClusterRelmapV2Image image, saved_image;
	ClusterRelmapRetentionPin old;
	XidNativeClogState state;

	retained_stage_start(UINT64_C(0x500008000), false, false);
	retained_stage_pin(UINT64_C(0x500007fff));
	old = relmap_runtime_shmem->directories[4].pins[0];
	memset(&owner, 0x5a, sizeof(owner)); saved_owner = owner;
	memset(&image, 0x3c, sizeof(image)); saved_image = image;
	stage_fault = 1;
	UT_ASSERT(!cluster_relmap_producer_stage_v2(true, &stage_updates, &owner, &image));
	test_xid_native_clog_state(&state);
	UT_ASSERT_EQ(state.inserts, 1); UT_ASSERT_EQ(native_publish_count, 1);
	UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), UINT64_C(0x500008001));
	UT_ASSERT_EQ(stage_pair_reads, 1);
	UT_ASSERT_EQ(stage_inserts | stage_pair_writes, 0);
	UT_ASSERT_EQ(memcmp(&old, relmap_runtime_shmem->directories[4].pins, sizeof(old)), 0);
	UT_ASSERT_EQ(memcmp(&owner, &saved_owner, sizeof(owner)), 0);
	UT_ASSERT_EQ(memcmp(&image, &saved_image, sizeof(image)), 0);
	retained_clog_stop();
}

UT_TEST(retained_stage_native_pair_refusal_keeps_staged_pins_and_outputs)
{
	for (unsigned fault = 2; fault <= 4; fault++) {
		ClusterRelmapOwnerSampleV2 owner, saved_owner;
		ClusterRelmapV2Image image, saved_image;
		ClusterRelmapRetentionPin old, *pin;
		XidNativeClogState state;

		retained_stage_start(UINT64_C(0x500008000), false, false);
		retained_stage_pin(UINT64_C(0x500007fff));
		if (fault == 4) /* Preserve the original same-namespace conflict. */
			relmap_runtime_shmem->directories[4].pins[0].key.identity = caller_namespace(true);
		old = relmap_runtime_shmem->directories[4].pins[0];
		memset(&owner, 0x5a, sizeof(owner)); saved_owner = owner;
		memset(&image, 0x3c, sizeof(image)); saved_image = image;
		stage_fault = fault;
		if (fault == 3)
			UT_ASSERT(retained_stage_error(&owner, &image));
		else
			UT_ASSERT(!cluster_relmap_producer_stage_v2(true, &stage_updates, &owner, &image));
		test_xid_native_clog_state(&state);
		UT_ASSERT_EQ(state.inserts, 1); UT_ASSERT_EQ(native_publish_count, 1);
		if (fault == 4) {
			UT_ASSERT_EQ(stage_pair_reads, 1);
			UT_ASSERT_EQ(stage_inserts | stage_flushes | stage_pair_writes, 0);
			UT_ASSERT(!relmap_runtime_shmem->directories[4].pins[1].occupied);
			UT_ASSERT_EQ(memcmp(&old, relmap_runtime_shmem->directories[4].pins, sizeof(old)), 0);
			UT_ASSERT_EQ(memcmp(&owner, &saved_owner, sizeof(owner)), 0);
			UT_ASSERT_EQ(memcmp(&image, &saved_image, sizeof(image)), 0);
			UT_ASSERT(!held && !retention_held && !retention_mapping_held);
			UT_ASSERT_EQ(caller_owner_lock_errors | lock_errors, 0);
			retained_clog_stop();
			continue;
		}
		UT_ASSERT_EQ(stage_inserts, 1); UT_ASSERT_EQ(stage_flushes, 1); UT_ASSERT_EQ(stage_pair_writes, 1);
		pin = &relmap_runtime_shmem->directories[4].pins[1];
		UT_ASSERT(pin->occupied && pin->may_be_staged);
		UT_ASSERT_EQ(pin->key.top_full_xid, UINT64_C(0x500008000));
		UT_ASSERT_EQ(pin->wal_end, 16385);
		UT_ASSERT_EQ(memcmp(&old, relmap_runtime_shmem->directories[4].pins, sizeof(old)), 0);
		UT_ASSERT_EQ(memcmp(&owner, &saved_owner, sizeof(owner)), 0);
		UT_ASSERT_EQ(memcmp(&image, &saved_image, sizeof(image)), 0);
		UT_ASSERT(!held && !retention_held && !retention_mapping_held);
		UT_ASSERT_EQ(caller_owner_lock_errors | lock_errors, 0);
		retained_clog_stop();
	}
}
