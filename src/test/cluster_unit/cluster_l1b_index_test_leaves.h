/* Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors.
 * Buffer/scheduler/WAL/visibility leaves; native VACUUM decisions are extracted.
 */
#ifndef CLUSTER_L1B_INDEX_TEST_LEAVES_H
#define CLUSTER_L1B_INDEX_TEST_LEAVES_H
#define BYPASS_THRESHOLD_PAGES 0.02
#define ParallelVacuumIsActive(vacrel) ((vacrel)->pvs != NULL)

#undef BufferGetPage
#define BufferGetPage(buffer) ((Page)pages[(buffer) - 1].data)

static bool
heap_page_is_all_visible(LVRelState *vacrel, Buffer buffer,
						TransactionId *cutoff, bool *all_frozen)
{
	(void)vacrel;
	(void)buffer;
	(void)cutoff;
	(void)all_frozen;
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(heap_locks, 1);
	return false; /* VM publication is a separate consumer, not a grant here. */
}

bool
vacuum_xid_failsafe_check(const struct VacuumCutoffs *cutoffs)
{
	(void)cutoffs;
	return failsafe_at == (int)failsafe_checks++;
}

void
vacuum_delay_point(void)
{
	UT_ASSERT_EQ(heap_locks + heap_pins, 0);
	delay_calls++;
}

void
pgstat_progress_update_param(int index, int64 value)
{
	(void)index;
	(void)value;
}

void
parallel_vacuum_bulkdel_all_indexes(ParallelVacuumState *pvs, long tuples, int scans)
{
	(void)pvs;
	(void)tuples;
	(void)scans;
	abort(); /* Parallel worker execution is not modeled as success. */
}

void
visibilitymap_pin(Relation rel, BlockNumber block, Buffer *buffer)
{
	(void)rel;
	UT_ASSERT(block < lengthof(pages));
	UT_ASSERT_EQ(heap_locks + heap_pins, 0);
	if (*buffer == InvalidBuffer) {
		*buffer = 100;
		vm_pins++;
	}
}

Buffer
ReadBufferExtended(Relation rel, ForkNumber fork, BlockNumber block,
				   ReadBufferMode mode, BufferAccessStrategy strategy)
{
	(void)rel;
	(void)strategy;
	UT_ASSERT_EQ(fork, MAIN_FORKNUM);
	UT_ASSERT_EQ(mode, RBM_NORMAL);
	UT_ASSERT(block < lengthof(pages));
	UT_ASSERT_EQ(heap_locks + heap_pins, 0);
	heap_reads++;
	heap_pins++;
	return block + 1;
}

void
LockBuffer(Buffer buffer, int mode)
{
	UT_ASSERT(buffer > 0 && buffer <= (Buffer)lengthof(pages));
	UT_ASSERT_EQ(mode, BUFFER_LOCK_EXCLUSIVE);
	UT_ASSERT_EQ(heap_pins, 1);
	UT_ASSERT_EQ(heap_locks, 0);
	heap_locks++;
}

void
UnlockReleaseBuffer(Buffer buffer)
{
	UT_ASSERT(buffer > 0 && buffer <= (Buffer)lengthof(pages));
	UT_ASSERT_EQ(heap_locks, 1);
	UT_ASSERT_EQ(heap_pins, 1);
	heap_locks--;
	heap_pins--;
}

void
ReleaseBuffer(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 100);
	UT_ASSERT_EQ(vm_pins, 1);
	vm_pins--;
}

void
MarkBufferDirty(Buffer buffer)
{
	UT_ASSERT(buffer > 0 && buffer <= (Buffer)lengthof(pages));
	UT_ASSERT_EQ(heap_locks, 1);
	UT_ASSERT_EQ(CritSectionCount, 1);
	heap_dirties++;
}

void
RecordPageWithFreeSpace(Relation rel, BlockNumber block, Size free_space)
{
	(void)rel;
	UT_ASSERT_EQ(heap_locks + heap_pins, 0);
	UT_ASSERT_EQ(free_space, PageGetHeapFreeSpace((Page)pages[block].data));
	fsm_writes++;
}

void
visibilitymap_set(Relation rel, BlockNumber block, Buffer heap, XLogRecPtr lsn,
				  Buffer vm, TransactionId cutoff, uint8 flags,
				  const ClusterSpaceIdentity *identity)
{
	(void)rel; (void)block; (void)heap; (void)lsn;
	(void)vm; (void)cutoff; (void)flags; (void)identity;
	abort();
}

bool
cluster_space_prepare_buffer_versions(const ClusterSpaceIdentity *identity,
	const Buffer *buffers, const uint8 *ids, uint8 count, RfPageProducerBatchV1 *batch)
{
	(void)identity; (void)buffers; (void)ids; (void)count; (void)batch;
	abort(); /* No shared positive authority. */
}

bool
rf_page_producer_stamp_v1(RfPageProducerBatchV1 *batch)
{
	(void)batch;
	abort();
}

bool
rf_page_producer_register_wal_v1(const RfPageProducerBatchV1 *batch)
{
	(void)batch;
	abort();
}
#endif
