/* Actual VM/FSM mutation bodies, heap requalification and SQL consumer glue.
 * Transport, physical lookup and WAL sinks are controlled edges. This does
 * not claim to replay the distributed micro or replace its six live phases. */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/visibilitymap.h"
#include "catalog/pg_class.h"
#include "cluster/cluster_pcm_lock.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "portability/instr_time.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "storage/fsm_internals.h"
#include "storage/smgr.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
int NBuffers = 3;
int NLocBuffer = 0;
bool InRecovery = false;
bool cluster_recmerge_window_active = false;
bool cluster_recmerge_apply_foreign = false;
uint64 cluster_recmerge_window_scn = 0;
uint64 cluster_recmerge_window_own_lsn = 0;
int cluster_node_id = 0;

#include "test_cluster_pcm_aux_page_space.inc"

static PGAlignedBlock pages[3];
static BufferTag tags[3];
static int refs[3];
static bool locks[3];
static RelationData relation_data;
static FormData_pg_class relation_class;
static SMgrRelationData smgr_data;
static sigjmp_buf failure_jump;
static int current_aux;
static int aux_writes, heap_writes, wal_writes, aux_locks, aux_unlocks;
static int clear_failures, set_failures, recent_races;
static int old_handle_releases, vm_reads_under_heap, heap_reentries;
static int operation_begins, operation_ends, operation_sequences;
static int errors, cancel_on_retry, invalidate_proof_on_retry;
static Size fsm_available;
static uint64 last_deadline;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "assertion: %s at %s:%d\n", condition, file, line);
	abort();
}

static void
fixture_error(void)
{
	errors++;
	siglongjmp(failure_jump, 1);
}

static Page
fixture_page(Buffer buffer)
{
	UT_ASSERT(buffer >= 1 && buffer <= 3);
	UT_ASSERT(refs[buffer - 1] > 0);
	UT_ASSERT(tags[buffer - 1].relNumber == 16393);
	return pages[buffer - 1].data;
}

static BlockNumber
fixture_block(Buffer buffer)
{
	UT_ASSERT(refs[buffer - 1] > 0);
	UT_ASSERT(tags[buffer - 1].relNumber == 16393);
	return tags[buffer - 1].blockNum;
}

static void
fixture_release(Buffer buffer)
{
	if (refs[buffer - 1] == 0 || tags[buffer - 1].relNumber != 16393) {
		old_handle_releases++;
		fixture_error();
	}
	refs[buffer - 1]--;
}

static void
fixture_lock(Buffer buffer, int mode)
{
	UT_ASSERT(refs[buffer - 1] > 0);
	if (mode == BUFFER_LOCK_UNLOCK) {
		UT_ASSERT(locks[buffer - 1]);
		locks[buffer - 1] = false;
		if (buffer != 3)
			aux_unlocks++;
	} else {
		UT_ASSERT_EQ(mode, BUFFER_LOCK_EXCLUSIVE);
		UT_ASSERT(!locks[buffer - 1]);
		locks[buffer - 1] = true;
		if (buffer == 3) {
			heap_reentries++;
			if (heap_reentries > 1 && invalidate_proof_on_retry) {
				ItemId item = PageGetItemId(pages[2].data, FirstOffsetNumber);
				HeapTupleHeader tuple = (HeapTupleHeader)PageGetItem(pages[2].data, item);
				tuple->t_infomask &= ~HEAP_XMIN_FROZEN;
			}
		} else
			aux_locks++;
	}
}

static void
fixture_unlock_release(Buffer buffer)
{
	fixture_lock(buffer, BUFFER_LOCK_UNLOCK);
	fixture_release(buffer);
}

static void
fixture_dirty(Buffer buffer)
{
	UT_ASSERT(locks[buffer - 1]);
	(void)fixture_page(buffer);
	if (buffer == 3)
		heap_writes++;
	else
		aux_writes++;
}

static void
fixture_replace(Buffer buffer)
{
	int old = buffer - 1;
	int next = 1 - old;

	UT_ASSERT(old < 2 && refs[old] == 1 && !locks[old]);
	refs[old] = 0; /* Actual handoff was executed separately in the owner suite. */
	memcpy(pages[next].data, pages[old].data, BLCKSZ);
	tags[next] = tags[old];
	tags[old].relNumber = 999;
	current_aux = next;
}

static bool
fixture_aux_lock(Buffer *buffer, ResourceXAuxiliaryAcquireContext *context)
{
	bool fail = aux_writes == 0 ? clear_failures > 0 : set_failures > 0;

	UT_ASSERT(locks[2]);
	UT_ASSERT_EQ(refs[*buffer - 1], 1);
	if (!context->active) {
		context->active = true;
		context->absolute_deadline_us = 9000;
	} else
		UT_ASSERT_EQ(context->absolute_deadline_us, last_deadline);
	last_deadline = context->absolute_deadline_us;
	if (fail) {
		if (aux_writes == 0)
			clear_failures--;
		else
			set_failures--;
		fixture_replace(*buffer);
		*buffer = InvalidBuffer;
		context->reobserve = true;
		return false;
	}
	fixture_lock(*buffer, BUFFER_LOCK_EXCLUSIVE);
	memset(context, 0, sizeof(*context));
	return true;
}

static Buffer
fixture_vm_read(Relation rel, BlockNumber block, bool extend)
{
	UT_ASSERT(rel == &relation_data && block == 0 && extend);
	if (locks[2]) {
		vm_reads_under_heap++;
		fixture_error();
	}
	refs[current_aux]++;
	return current_aux + 1;
}

static bool
fixture_recent(RelFileLocator locator, ForkNumber fork, BlockNumber block, Buffer recent)
{
	BufferTag expected;

	InitBufferTag(&expected, &locator, fork, block);
	UT_ASSERT(locks[2]);
	if (recent_races > 0 && recent == current_aux + 1) {
		recent_races--;
		refs[recent - 1] = 1;
		fixture_replace(recent);
		return false;
	}
	if (!BufferTagsEqual(&expected, &tags[recent - 1]))
		return false;
	refs[recent - 1]++;
	return true;
}

static XLogRecPtr
fixture_log_visible(Relation rel, Buffer heap, Buffer vm, TransactionId cutoff, uint8 flags)
{
	UT_ASSERT(rel == &relation_data && heap == 3 && vm == current_aux + 1);
	UT_ASSERT(locks[2] && locks[vm - 1]);
	UT_ASSERT_EQ(cutoff, InvalidTransactionId);
	UT_ASSERT_EQ(flags, VISIBILITYMAP_VALID_BITS);
	wal_writes++;
	return 12345;
}

static Buffer
fixture_read_heap(Relation rel, BlockNumber block)
{
	UT_ASSERT(rel == &relation_data && block == 0);
	refs[2]++;
	return 3;
}

static void
fixture_interrupt(void)
{
	if (cancel_on_retry && (clear_failures == 0 || set_failures == 0))
		fixture_error();
}

#undef BufferGetPage
#define BufferGetPage fixture_page
#define BufferGetBlockNumber fixture_block
#define ReleaseBuffer fixture_release
#define LockBuffer fixture_lock
#define UnlockReleaseBuffer fixture_unlock_release
#define MarkBufferDirty fixture_dirty
#define MarkBufferDirtyHint(buffer, standard) ((void)(standard), fixture_dirty(buffer))
#define ClusterLockBufferExclusiveAuxiliaryAware fixture_aux_lock
#define vm_readbuf fixture_vm_read
#define ReadRecentBuffer fixture_recent
#define RelationGetSmgr(rel) ((void)(rel), &smgr_data)
#undef RelationNeedsWAL
#define RelationNeedsWAL(rel) ((void)(rel), true)
#define log_heap_visible fixture_log_visible
#undef XLogHintBitIsNeeded
#define XLogHintBitIsNeeded() false
#undef START_CRIT_SECTION
#define START_CRIT_SECTION() ((void)0)
#undef END_CRIT_SECTION
#define END_CRIT_SECTION() ((void)0)
#undef CHECK_FOR_INTERRUPTS
#define CHECK_FOR_INTERRUPTS() fixture_interrupt()
#undef elog
#define elog(...) fixture_error()
#undef ereport
#define ereport(...) fixture_error()

static void visibilitymap_set_locked(Relation rel, BlockNumber heapBlk, Buffer heapBuf,
									 XLogRecPtr recptr, Buffer vmBuf, TransactionId cutoff_xid,
									 uint8 flags);
#include "test_cluster_pcm_aux_vm_mutation.inc"
#include "test_cluster_pcm_aux_heap_repin.inc"

typedef struct FSMAddress {
	int level;
	int logpageno;
} FSMAddress;
static FSMAddress
fixture_fsm_location(BlockNumber heap, uint16 *slot)
{
	FSMAddress addr = { 0, 0 };
	UT_ASSERT_EQ(heap, 0);
	*slot = 0;
	return addr;
}
static Buffer
fixture_fsm_read(Relation rel, FSMAddress addr, bool extend)
{
	UT_ASSERT(rel == &relation_data && addr.logpageno == 0 && extend);
	refs[current_aux]++;
	return current_aux + 1;
}
static bool
fixture_fsm_set(Page page, uint16 slot, uint8 value)
{
	UT_ASSERT(page == pages[current_aux].data && slot == 0 && locks[current_aux]);
	fsm_available = (Size)value * (BLCKSZ / 256);
	return true;
}
#define fsm_space_avail_to_cat(value) ((value) / (BLCKSZ / 256))
#define fsm_get_location fixture_fsm_location
#define fsm_readbuf fixture_fsm_read
#define fsm_set_avail fixture_fsm_set
#define fsm_search_avail(...) (fixture_error(), -1)
#define FSM_BOTTOM_LEVEL 0
static int fsm_set_and_search(Relation rel, FSMAddress addr, uint16 slot, uint8 newValue,
							  uint8 minValue);
#include "test_cluster_pcm_aux_fsm_mutation.inc"

static Relation
fixture_table_open(Oid oid, LOCKMODE mode)
{
	UT_ASSERT_EQ(oid, 16393);
	UT_ASSERT_EQ(mode, ShareLock);
	return &relation_data;
}
static void
fixture_operation(uint16 phase, uint64 sequence, uint64 count)
{
	UT_ASSERT_EQ(sequence, 1);
	if (phase == RESOURCE_X_TRACE_OPERATION_BEGIN) {
		operation_begins++;
		UT_ASSERT_EQ(count, 0);
	} else {
		UT_ASSERT_EQ(phase, RESOURCE_X_TRACE_OPERATION_DONE);
		UT_ASSERT_EQ(count, 2);
		operation_ends++;
	}
}

#define superuser() true
#define table_open fixture_table_open
#define table_close(rel, mode) UT_ASSERT((rel) == &relation_data && (mode) == ShareLock)
#undef RelationGetNumberOfBlocks
#define RelationGetNumberOfBlocks(rel) ((void)(rel), 16)
#define ReadBuffer fixture_read_heap
#define probe_next_operation() (++operation_sequences)
#define probe_operation(rel, fork, block, phase, sequence, count)                                  \
	((void)(rel), (void)(fork), (void)(block), fixture_operation(phase, sequence, count))
#define GetRecordedFreeSpace(rel, block) ((void)(rel), (void)(block), fsm_available)
#define psprintf(...) ((char *)NULL)
#define cstring_to_text(value) ((void)(value), (void *)NULL)
#include "test_cluster_pcm_aux_mutation_consumer.inc"

static void
fixture_reset(ForkNumber fork)
{
	PageHeader header;
	ItemId item;
	HeapTupleHeader tuple;

	memset(pages, 0, sizeof(pages));
	memset(tags, 0, sizeof(tags));
	memset(refs, 0, sizeof(refs));
	memset(locks, 0, sizeof(locks));
	memset(&relation_data, 0, sizeof(relation_data));
	memset(&relation_class, 0, sizeof(relation_class));
	relation_data.rd_rel = &relation_class;
	relation_class.relkind = RELKIND_RELATION;
	relation_class.relpersistence = RELPERSISTENCE_PERMANENT;
	strcpy(NameStr(relation_class.relname), "aux_mutate");
	for (int i = 0; i < 3; i++) {
		tags[i].spcOid = 1663;
		tags[i].dbOid = 5;
		tags[i].relNumber = 16393;
		tags[i].forkNum = i == 2 ? MAIN_FORKNUM : fork;
		header = (PageHeader)pages[i].data;
		header->pd_lower = SizeOfPageHeaderData;
		header->pd_upper = BLCKSZ;
		header->pd_special = BLCKSZ;
	}
	smgr_data.smgr_rlocator.locator = BufTagGetRelFileLocator(&tags[0]);
	PageSetAllVisible(pages[2].data);
	header = (PageHeader)pages[2].data;
	header->pd_lower += sizeof(ItemIdData);
	header->pd_upper = BLCKSZ - MAXALIGN(SizeofHeapTupleHeader);
	item = PageGetItemId(pages[2].data, FirstOffsetNumber);
	ItemIdSetNormal(item, header->pd_upper, SizeofHeapTupleHeader);
	tuple = (HeapTupleHeader)PageGetItem(pages[2].data, item);
	HeapTupleHeaderSetXmin(tuple, FrozenTransactionId);
	HeapTupleHeaderSetXminFrozen(tuple);
	tuple->t_infomask |= HEAP_XMAX_INVALID;
	((char *)PageGetContents(pages[0].data))[0] = VISIBILITYMAP_VALID_BITS;
	current_aux = 0;
	aux_writes = heap_writes = wal_writes = aux_locks = aux_unlocks = 0;
	clear_failures = set_failures = recent_races = 0;
	old_handle_releases = vm_reads_under_heap = heap_reentries = 0;
	operation_begins = operation_ends = operation_sequences = errors = 0;
	cancel_on_retry = invalidate_proof_on_retry = 0;
	fsm_available = BLCKSZ / 256;
	last_deadline = 0;
}

static void
run_cycle(ForkNumber fork)
{
	LOCAL_FCINFO(fcinfo, 3);

	InitFunctionCallInfoData(*fcinfo, NULL, 3, InvalidOid, NULL, NULL);
	fcinfo->args[0].value = ObjectIdGetDatum(16393);
	fcinfo->args[1].value = Int32GetDatum(0);
	fcinfo->args[2].value = Int32GetDatum(fork);
	(void)aux_cycle_internal(fcinfo, false);
}

UT_TEST(actual_vm_consumer_requalifies_both_stages_without_replaying_clear)
{
	fixture_reset(VISIBILITYMAP_FORKNUM);
	clear_failures = 1;
	set_failures = 2;
	recent_races = 1;
	if (sigsetjmp(failure_jump, 1) != 0) {
		UT_ASSERT(false);
		return;
	}
	run_cycle(VISIBILITYMAP_FORKNUM);
	UT_ASSERT_EQ(aux_writes, 2);
	UT_ASSERT_EQ(wal_writes, 1);
	UT_ASSERT_EQ(aux_locks, 2);
	UT_ASSERT_EQ(aux_unlocks, 2);
	UT_ASSERT_EQ(operation_sequences, 1);
	UT_ASSERT_EQ(operation_begins, 1);
	UT_ASSERT_EQ(operation_ends, 1);
	UT_ASSERT_EQ(old_handle_releases, 0);
	UT_ASSERT_EQ(vm_reads_under_heap, 0);
	UT_ASSERT_EQ(refs[0] + refs[1] + refs[2], 0);
	UT_ASSERT(heap_reentries >= 5);
}

UT_TEST(actual_vm_unchanged_clear_is_completed_not_retry)
{
	ResourceXAuxiliaryAcquireContext context = { 0 };
	Buffer buffer = 1;
	bool cleared = true;

	fixture_reset(VISIBILITYMAP_FORKNUM);
	refs[0] = refs[2] = 1;
	locks[2] = true;
	((char *)PageGetContents(pages[0].data))[0] = 0;
	UT_ASSERT(visibilitymap_clear_retry_aware(&relation_data, 0, &buffer, VISIBILITYMAP_VALID_BITS,
											  &context, &cleared));
	UT_ASSERT(!cleared);
	UT_ASSERT_EQ(aux_writes, 0);
	UT_ASSERT_EQ(wal_writes, 0);
	UT_ASSERT_EQ(buffer, 1);
}

UT_TEST(actual_consumer_rejects_stale_heap_proof_before_restoring_vm)
{
	fixture_reset(VISIBILITYMAP_FORKNUM);
	set_failures = 1;
	invalidate_proof_on_retry = 1;
	if (sigsetjmp(failure_jump, 1) == 0) {
		run_cycle(VISIBILITYMAP_FORKNUM);
		UT_ASSERT(false);
	}
	UT_ASSERT_EQ(aux_writes, 1);
	UT_ASSERT_EQ(wal_writes, 0);
	UT_ASSERT_EQ(operation_ends, 0);
	UT_ASSERT_EQ(errors, 1);
	UT_ASSERT_EQ(old_handle_releases, 0);
}

UT_TEST(actual_consumer_cancellation_does_not_report_completion)
{
	fixture_reset(VISIBILITYMAP_FORKNUM);
	clear_failures = 1;
	cancel_on_retry = 1;
	if (sigsetjmp(failure_jump, 1) == 0) {
		run_cycle(VISIBILITYMAP_FORKNUM);
		UT_ASSERT(false);
	}
	UT_ASSERT_EQ(aux_writes, 0);
	UT_ASSERT_EQ(wal_writes, 0);
	UT_ASSERT_EQ(operation_ends, 0);
	UT_ASSERT(!locks[2]);
	UT_ASSERT_EQ(refs[0] + refs[1], 0);
	UT_ASSERT_EQ(refs[2], 1); /* Existing ResourceOwner owns the heap pin at ERROR. */
}

UT_TEST(actual_fsm_consumer_keeps_original_mutation_and_restore)
{
	fixture_reset(FSM_FORKNUM);
	if (sigsetjmp(failure_jump, 1) != 0) {
		UT_ASSERT(false);
		return;
	}
	run_cycle(FSM_FORKNUM);
	UT_ASSERT_EQ(aux_writes, 2);
	UT_ASSERT_EQ(wal_writes, 0);
	UT_ASSERT_EQ(aux_locks, 2);
	UT_ASSERT_EQ(aux_unlocks, 2);
	UT_ASSERT_EQ(fsm_available, BLCKSZ / 256);
	UT_ASSERT_EQ(operation_sequences, 1);
	UT_ASSERT_EQ(operation_ends, 1);
	UT_ASSERT_EQ(old_handle_releases, 0);
	UT_ASSERT_EQ(refs[0] + refs[1] + refs[2], 0);
	UT_ASSERT_EQ(last_deadline, 0); /* FSM never entered Resource-X retry. */
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(actual_vm_consumer_requalifies_both_stages_without_replaying_clear);
	UT_RUN(actual_vm_unchanged_clear_is_completed_not_retry);
	UT_RUN(actual_consumer_rejects_stale_heap_proof_before_restoring_vm);
	UT_RUN(actual_consumer_cancellation_does_not_report_completion);
	UT_RUN(actual_fsm_consumer_keeps_original_mutation_and_restore);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
