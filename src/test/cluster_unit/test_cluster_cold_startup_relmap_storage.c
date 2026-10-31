/*-------------------------------------------------------------------------
 * test_cluster_cold_startup_relmap_storage.c
 *     Original relmap storage and pins for declared Startup replay.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_cold_startup_relmap_storage.c
 * NOTES
 *     Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
#include "test_cluster_cold_startup_relmap_storage_namespace.h"
int main(void);
#include "test_cluster_relmap_v2_storage.c"
#include "test_cluster_cold_startup_relmap_storage.h"

static unsigned producer_reads;
static void (*producer_read_hook)(unsigned);
static bool producer_pin_error;
static unsigned producer_pin_error_after;

/* The original Startup exception owner, outside the isolated fixture stack. */
#undef PG_exception_stack
extern PGDLLIMPORT sigjmp_buf *PG_exception_stack;

void
test_cold_relmap_storage_begin(ClusterColdRelmapInputV1 *input)
{
	const ClusterRelmapV2Owner *pending = &input->staged.pending_owner;

	setup(&input->staged.identity);
	/* Replace the setup-only fixture pin before any replay attempt. */
	if (cluster_relmap_retention_cancel_prepare(&writer_retention, &writer_key)
		!= CLUSTER_RELMAP_RETENTION_OK)
		abort();
	/* The old fixture is now empty; construct this test's original-owner input. */
	memset(&writer_retention, 0, sizeof(writer_retention));
	if (cluster_relmap_retention_build_start(&writer_retention,
		input->staged.identity.system_identifier, input->staged.identity.database_incarnation,
		pending->node, 1, pending->top_full_xid - 16, pending->top_full_xid + 16)
		!= CLUSTER_RELMAP_RETENTION_OK
		|| cluster_relmap_retention_build_finish(&writer_retention) != CLUSTER_RELMAP_RETENTION_OK)
		abort();
	writer_key = (ClusterRelmapRetentionKey){0};
	writer_key.identity = input->staged.identity;
	writer_key.generation = input->staged.pending_generation;
	writer_key.origin = pending->node;
	writer_key.top_full_xid = pending->top_full_xid;
	writer_key.membership_epoch = pending->membership_epoch;
	writer_key.admitted_incarnation = pending->admitted_incarnation;
	writer_key.boot_incarnation = pending->boot_incarnation;
	LWLockAcquire(&retention_lock, LW_EXCLUSIVE);
	if (cluster_relmap_retention_begin(&writer_retention, &writer_key)
		!= CLUSTER_RELMAP_RETENTION_OK)
		abort();
	LWLockRelease(&retention_lock);
	input->staged.committed = map_image(40000);
	input->staged.pending = map_image(40001);
	input->retention = &writer_retention;
	input->retention_lock = &retention_lock;
	relmap_x_held = true;
	producer_reads = 0;
	producer_read_hook = NULL;
	producer_pin_error = false;
	producer_pin_error_after = 0;
}

void
test_cold_relmap_storage_end(void)
{
	producer_read_hook = NULL;
	cleanup();
}

void
test_cold_relmap_storage_read_hook(void (*hook)(unsigned))
{
	producer_read_hook = hook;
}

ssize_t
test_cold_relmap_storage_read(int fd, void *bytes, size_t len)
{
	ssize_t result = relmap_test_read(fd, bytes, len);
	++producer_reads;
	if (producer_read_hook != NULL)
		producer_read_hook(producer_reads);
	return result;
}

void
test_cold_relmap_storage_pin_error(void)
{
	producer_pin_error = true;
}

void
test_cold_relmap_storage_pin_error_after(unsigned acquisitions)
{
	producer_pin_error_after = acquisitions;
}

static ClusterRelmapRetentionKey
producer_pin_key(const ClusterColdRelmapWalV1 *wal)
{
	ClusterRelmapRetentionKey key = {0};
	key.identity = wal->identity;
	key.generation = wal->generation;
	key.origin = wal->owner.node;
	key.top_full_xid = wal->owner.top_full_xid;
	key.membership_epoch = wal->owner.membership_epoch;
	key.admitted_incarnation = wal->owner.admitted_incarnation;
	key.boot_incarnation = wal->owner.boot_incarnation;
	return key;
}

void
test_cold_relmap_storage_add_namespace(const ClusterColdRelmapWalV1 *wal)
{
	ClusterRelmapRetentionKey key = producer_pin_key(wal);
	ClusterRelmapV2Image clean = {0};
	unsigned char bytes[CLUSTER_RELMAP_V2_FILE_SIZE];
	char original[MAXPGPATH];
	clean.identity = wal->identity;
	clean.committed_generation = wal->generation - 1;
	clean.committed = map_image(40000);
	if (clean.identity.shared || clean.identity.dbid != 42
		|| cluster_relmap_v2_encode(&clean, bytes, sizeof(bytes)) != CLUSTER_RELMAP_V2_VALID)
		abort();
	strcpy(original, directory);
	snprintf(directory, sizeof(directory), "%s/base/42", root);
	write_copy(0, bytes, sizeof(bytes));
	write_copy(1, bytes, sizeof(bytes));
	strcpy(directory, original);
	LWLockAcquire(wal->retention_lock, LW_EXCLUSIVE);
	if (cluster_relmap_retention_begin(wal->retention, &key) != CLUSTER_RELMAP_RETENTION_OK)
		abort();
	LWLockRelease(wal->retention_lock);
}

ClusterRelmapRetentionResult
test_cold_relmap_storage_cancel(const ClusterColdRelmapWalV1 *wal)
{
	ClusterRelmapRetentionKey key = producer_pin_key(wal);
	ClusterRelmapRetentionResult result;
	LWLockAcquire(wal->retention_lock, LW_EXCLUSIVE);
	result = cluster_relmap_retention_cancel_prepare(wal->retention, &key);
	LWLockRelease(wal->retention_lock);
	return result;
}

/* Preserve the exact original key while changing its retained context. */
void
test_cold_relmap_storage_pin_drift(unsigned fault)
{
	ClusterCanonicalTxnBinding binding = {0};

	LWLockAcquire(&retention_lock, LW_EXCLUSIVE);
	switch (fault) {
	case 0: ++writer_retention.generation; break;
	case 1: ++writer_retention.native_floor; break;
	case 2: ++writer_retention.last_issued; break;
	case 3: ++writer_retention.truncate_serial; break;
	case 4:
		writer_retention.inflight = (ClusterRelmapRetentionTruncate){
			writer_retention.generation, 1, writer_retention.native_floor};
		break;
	case 5:
		binding.segment_id = 17;
		binding.xid = (TransactionId)writer_key.top_full_xid;
		binding.slot_wrap = 0;
		binding.origin_instance = writer_key.origin + 1;
		binding.publish_state = CLUSTER_CANONICAL_TXN_PUBLISHED;
		binding.active_lsn = 0x4567;
		if (cluster_relmap_retention_bind(&writer_retention, writer_key.top_full_xid,
			&binding) != CLUSTER_RELMAP_RETENTION_OK)
			abort();
		break;
	case 6:
	case 7:
		writer_retention.pins[1] = writer_retention.pins[0];
		if (fault == 7) ++writer_retention.pins[1].key.boot_incarnation;
		break;
	case 8: ++writer_retention.pins[0].binding.active_lsn; break;
	default: abort();
	}
	LWLockRelease(&retention_lock);
}

bool
test_cold_relmap_storage_acquire(LWLock *lock, LWLockMode mode)
{
	if (producer_pin_error || (producer_pin_error_after != 0 && --producer_pin_error_after == 0)) {
		producer_pin_error = false;
		siglongjmp(*PG_exception_stack, 1);
	}
	return LWLockAcquire(lock, mode);
}

void
test_cold_relmap_storage_fault(unsigned write, void (*hook)(bool))
{
	fail_write = write;
	post_sync_hook = hook;
}

TestColdRelmapStorageStats
test_cold_relmap_storage_stats(void)
{
	TestColdRelmapStorageStats stats = {writes, io_under_retention_lock,
		writer_retention.pins[0].occupied, writer_retention.pins[0].may_be_staged, producer_reads, 0, 0};
	for (unsigned i = 0; i < CLUSTER_RELMAP_RETENTION_CAPACITY; ++i)
		if (writer_retention.pins[i].occupied) {
			++stats.pins;
			stats.armed_pins += writer_retention.pins[i].may_be_staged;
		}
	return stats;
}

bool
test_cold_relmap_storage_matches(const ClusterColdRelmapInputV1 *input, bool pending)
{
	ClusterRelmapV2View view;
	return cluster_relmap_authority_v2_read(&input->staged.identity, &view)
		== CLUSTER_RELMAP_V2_PAIR_VALID && view.has_pending == pending
		&& view.committed_generation == input->staged.committed_generation
		&& memcmp(&view.committed, &input->staged.committed, sizeof(RelMapFile)) == 0
		&& (!pending || memcmp(&view.pending, &input->staged.pending, sizeof(RelMapFile)) == 0);
}
