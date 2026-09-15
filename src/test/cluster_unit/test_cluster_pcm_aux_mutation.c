/* Author: SqlRush <sqlrush@gmail.com> */
/* Actual VM/FSM mutation bodies, heap requalification and SQL consumer glue.
 * Transport, physical lookup and WAL sinks are controlled edges. This does
 * not claim to replay the distributed micro or replace its six live phases. */
#include "postgres.h"

#include "access/heapam.h"
#include "access/hio.h"
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

/* Execute the real INSERT pre-critical bracket. The raw X edge rejects the
 * same unready image that the explicit ownership adapter can return to its
 * caller. This is not a mock of receipt APPLY or the INSERT body. */
static int insert_entries, insert_retries;

static void
insert_raw_lock(Buffer buffer, int mode)
{
	if (buffer != 3 && mode == BUFFER_LOCK_EXCLUSIVE)
		fixture_error();
	fixture_lock(buffer, mode);
}

static bool
insert_aux_lock(Buffer *buffer, Buffer *alias, const ClusterBufferBarrierSiteId *site,
				ResourceXAuxiliaryAcquireContext *context)
{
	UT_ASSERT(alias == NULL && site == NULL);
	return fixture_aux_lock(buffer, context);
}

#undef LockBuffer
#define LockBuffer insert_raw_lock
#define ClusterLockBufferExclusiveAuxiliaryAliasAware insert_aux_lock
/* Retarget safety is executed with the real owner/prepare in aux_reobserve.
 * This fixture keeps one logical VM page while its physical slot changes. */
#define cluster_heap_vm_retarget_after_repick(buffer, context) ((void)(buffer), (void)(context))
static void
run_insert_vm_bracket(void)
{
	Buffer buffer = 3, vmbuffer = 1;
	bool vm_locked = false;
	ResourceXAuxiliaryAcquireContext cluster_vm_context = { 0 };

cluster_heap_insert_retry:
	if (insert_entries++ != 0) {
		insert_retries++;
		UT_ASSERT(!locks[2]);
		UT_ASSERT_EQ(refs[0] + refs[1] + refs[2], 0);
		refs[2] = 1;
		vmbuffer = fixture_vm_read(&relation_data, 0, true);
		fixture_lock(buffer, BUFFER_LOCK_EXCLUSIVE);
	}
#include "test_cluster_heap_vm_insert.inc"
	UT_ASSERT(vm_locked && locks[2] && locks[vmbuffer - 1]);
	fixture_lock(vmbuffer, BUFFER_LOCK_UNLOCK);
	fixture_unlock_release(buffer);
	fixture_release(vmbuffer);
}

UT_TEST(actual_insert_vm_loss_returns_to_target_qualification_before_mutation)
{
	fixture_reset(VISIBILITYMAP_FORKNUM);
	refs[0] = refs[2] = 1;
	locks[2] = true;
	clear_failures = 1;
	insert_entries = insert_retries = 0;
	if (sigsetjmp(failure_jump, 1) == 0)
		run_insert_vm_bracket();
	else
		UT_ASSERT(false);
	UT_ASSERT_EQ(insert_retries, 1);
	UT_ASSERT_EQ(old_handle_releases, 0);
	UT_ASSERT_EQ(aux_writes + heap_writes + wal_writes, 0);
	UT_ASSERT_EQ(refs[0] + refs[1] + refs[2], 0);
}

/* Tuple-lock bracket: the following external acquire edge handles both old
 * and new signatures so RED is a runtime rejection, not a compile failure. */
static bool
tuple_vm_lock(Buffer buffer, bool barrier, ClusterBufferBarrierSiteId site, bool *replaced,
			  int *first, int *second, ...)
{
	ResourceXAuxiliaryAcquireContext *context;
	Buffer *alias;
	Buffer target = buffer;
	bool acquired;
	va_list args;

	if (replaced == NULL)
		fixture_error();
	va_start(args, second);
	context = va_arg(args, ResourceXAuxiliaryAcquireContext *);
	alias = va_arg(args, Buffer *);
	va_end(args);
	acquired = insert_aux_lock(&target, alias, NULL, context);
	*replaced = !BufferIsValid(target);
	return acquired;
}

static void
tuple_vm_warm(Buffer *buffer, Buffer *alias, ResourceXAuxiliaryAcquireContext *context)
{
	UT_ASSERT(!locks[2]);
	if (BufferIsValid(*buffer)) {
		fixture_lock(*buffer, BUFFER_LOCK_EXCLUSIVE);
		fixture_lock(*buffer, BUFFER_LOCK_UNLOCK);
	}
}

#define cluster_current_mx_stamp_lock_buffer tuple_vm_lock
#define cluster_current_mx_stamp_cancel(plan) (*(plan) = 0)
#define cluster_current_mx_operation_restart(plan) (*(plan) = 0)
#define cluster_heap_vm_barrier_warm tuple_vm_warm
static void
run_tuple_vm_bracket(void)
{
	Buffer selected = 3, *buffer = &selected, vmbuffer = 1;
	Relation relation = &relation_data;
	Page page = pages[2].data;
	ItemId lp;
	HeapTupleData data = { 0 }, *tuple = &data;
	ItemPointerData tid_data;
	ItemPointer tid = &tid_data;
	BlockNumber block = 0;
	ResourceXAuxiliaryAcquireContext cluster_vm_context = { 0 };
	int cluster_current_mx_lock_plan = 1, cluster_current_mx_operation = 1;
	bool cluster_did_lock_stamp = true, cluster_lock_needs_itl_slot = true;
	bool cluster_lock_undo_ready = true, vm_locked = false;
	int cluster_lock_slot_idx = 2;

	ItemPointerSet(tid, 0, FirstOffsetNumber);
#include "test_cluster_heap_vm_tuple_lock.inc"
	fixture_error(); /* This fixture must reenter, not reach publication. */
l3:
	UT_ASSERT(!vm_locked && locks[2]);
	UT_ASSERT_EQ(cluster_current_mx_lock_plan, 0);
	UT_ASSERT_EQ(cluster_current_mx_operation, 0);
	UT_ASSERT(!cluster_did_lock_stamp && !cluster_lock_needs_itl_slot && !cluster_lock_undo_ready);
	UT_ASSERT_EQ(cluster_lock_slot_idx, CLUSTER_ITL_SLOT_UNALLOCATED);
	UT_ASSERT(tuple->t_data == (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 1)));
	UT_ASSERT_EQ(tuple->t_len, SizeofHeapTupleHeader);
	fixture_unlock_release(*buffer);
	fixture_release(vmbuffer);
}

UT_TEST(actual_tuple_lock_vm_loss_reloads_tuple_before_l3)
{
	fixture_reset(VISIBILITYMAP_FORKNUM);
	refs[0] = refs[2] = 1;
	locks[2] = true;
	clear_failures = 1;
	if (sigsetjmp(failure_jump, 1) == 0)
		run_tuple_vm_bracket();
	else
		UT_ASSERT(false);
	UT_ASSERT_EQ(old_handle_releases, 0);
	UT_ASSERT_EQ(aux_writes + heap_writes + wal_writes, 0);
	UT_ASSERT_EQ(refs[0] + refs[1] + refs[2], 0);
}

/* Actual UPDATE pair bracket and its two caller-owned unwind branches. Only
 * distributed acquisition/lookup and MX cancellation are controlled edges;
 * alias counts, ordering, lock release and the selected reentry execute from
 * heapam.c. The adapter's own pin/ledger behavior is in aux_reobserve. */
static PGAlignedBlock dml_pages[4];
static BufferTag dml_tags[4];
static int dml_refs[4], dml_acquires[4];
static bool dml_locks[4];
static int dml_nacquires, dml_fail_at, dml_reads, dml_releases;
static bool dml_replace;
static Buffer dml_old_vm;

static void
dml_lock(Buffer buffer, int mode)
{
	UT_ASSERT(dml_refs[buffer - 1] > 0);
	UT_ASSERT_EQ(dml_locks[buffer - 1], mode == BUFFER_LOCK_UNLOCK);
	dml_locks[buffer - 1] = mode != BUFFER_LOCK_UNLOCK;
}

static void
dml_release(Buffer buffer)
{
	UT_ASSERT(!dml_locks[buffer - 1] && dml_refs[buffer - 1] > 0);
	dml_refs[buffer - 1]--;
	dml_releases++;
}

static void
dml_tag(Buffer buffer, RelFileLocator *locator, ForkNumber *fork, BlockNumber *block)
{
	UT_ASSERT(dml_refs[buffer - 1] > 0);
	*locator = BufTagGetRelFileLocator(&dml_tags[buffer - 1]);
	*fork = dml_tags[buffer - 1].forkNum;
	*block = dml_tags[buffer - 1].blockNum;
}

static bool
dml_aux_lock(Buffer *buffer, Buffer *alias, const ClusterBufferBarrierSiteId *site,
			 ResourceXAuxiliaryAcquireContext *context)
{
	bool equal_alias = alias != NULL && alias != buffer && *alias == *buffer;
	int index = *buffer - 1;

	UT_ASSERT(!dml_locks[index]);
	UT_ASSERT_EQ(dml_refs[index], equal_alias ? 2 : 1);
	UT_ASSERT(dml_nacquires < 4);
	dml_acquires[dml_nacquires++] = *buffer;
	if (dml_nacquires == dml_fail_at) {
		if (dml_replace) {
			dml_refs[index] = 0;
			dml_tags[index].relNumber = 999;
			*buffer = InvalidBuffer;
			if (equal_alias)
				*alias = InvalidBuffer;
			context->active = context->reobserve = true;
		}
		return false;
	}
	dml_lock(*buffer, BUFFER_LOCK_EXCLUSIVE);
	return true;
}

static bool
dml_stamp_lock(Buffer buffer, bool barrier, ClusterBufferBarrierSiteId site, bool *replaced,
			   int *first, int *second, ResourceXAuxiliaryAcquireContext *context, Buffer *alias)
{
	bool result = dml_aux_lock(&buffer, alias, &site, context);

	*replaced = !BufferIsValid(buffer);
	return result;
}

static void
dml_pin(Relation relation, BlockNumber heap, Buffer *vm)
{
	UT_ASSERT(!dml_locks[2] && !dml_locks[3]);
	UT_ASSERT(!BufferIsValid(*vm));
	UT_ASSERT_EQ(dml_refs[dml_old_vm - 1], 0);
	dml_reads++;
	*vm = dml_old_vm;
	dml_tags[*vm - 1].relNumber = 16393;
	dml_refs[*vm - 1]++;
}

static bool
dml_recent(Relation relation, BlockNumber heap, Buffer recent, Buffer *vm)
{
	UT_ASSERT(dml_locks[2] && !dml_locks[3]);
	UT_ASSERT_EQ(recent, dml_old_vm);
	UT_ASSERT_EQ(dml_refs[recent - 1], 0);
	*vm = recent;
	dml_refs[recent - 1]++;
	return true;
}

static void
dml_observe(ClusterBufferBarrierPhase phase)
{
	if (phase == CLUSTER_BUFFER_BARRIER_PHASE_CALLER_POST)
		UT_ASSERT(!dml_locks[0] && !dml_locks[1] && !dml_locks[2] && !dml_locks[3]);
}

#undef LockBuffer
#undef ReleaseBuffer
#undef BufferGetPage
#undef BufferGetTag
#undef ClusterLockBufferExclusiveAuxiliaryAliasAware
#undef cluster_current_mx_stamp_lock_buffer
#undef cluster_heap_vm_barrier_warm
#define LockBuffer dml_lock
#define ReleaseBuffer dml_release
#define BufferGetPage(buffer) (dml_pages[(buffer) - 1].data)
#define BufferGetTag dml_tag
#define ClusterLockBufferExclusiveAuxiliaryAliasAware dml_aux_lock
#define cluster_current_mx_stamp_lock_buffer dml_stamp_lock
#define cluster_heap_vm_barrier_warm dml_warm
#define cluster_heap_lock_with_vm_repin dml_repin
#define visibilitymap_pin dml_pin
#define visibilitymap_pin_recent dml_recent
#define ClusterObserveBufferBarrierReceipt(site, phase, locator, fork, block, outcome, proof)      \
	((void)(site), (void)(locator), (void)(fork), (void)(block), (void)(outcome), (void)(proof),   \
	 dml_observe(phase))
#define heap_freetuple(tuple) fixture_error()
void dml_repin(Relation relation, BlockNumber heap, Buffer buffer, Buffer *vm);
#include "test_cluster_heap_vm_unwind_helpers.inc"

static int
run_update_vm_bracket(bool same_vm, bool reverse, bool temp_locked)
{
	Relation relation = &relation_data;
	Buffer buffer = 3, newbuf = 4;
	Buffer vmbuffer = reverse ? 2 : 1, vmbuffer_new = same_vm ? vmbuffer : (reverse ? 1 : 2);
	bool vm_locked = false, vm_locked_new = false, old_tuple_temp_locked = temp_locked;
	HeapTupleData new_data = { 0 }, oldtup = { 0 };
	HeapTuple newtup = &new_data, heaptup = newtup, old_key_tuple = NULL;
	bool old_key_copied = false, iscombo = true;
	CommandId cid = 2, pgrac_entry_cid = 1;
	ResourceXAuxiliaryAcquireContext cluster_vm_context = { 0 }, cluster_vm_new_context = { 0 };
	int cluster_current_mx_old_plan = 1, cluster_current_mx_new_plan = 1;
	Page page;
	ItemId lp;
	ItemPointerData tid_data;
	ItemPointer otid = &tid_data;
	BlockNumber block = 0;

	dml_old_vm = vmbuffer;
	dml_refs[vmbuffer - 1]++;
	dml_refs[vmbuffer_new - 1]++;
	dml_refs[2] = dml_refs[3] = 1;
	dml_locks[2] = dml_locks[3] = true;
	ItemPointerSet(otid, 0, 1);
#include "test_cluster_heap_vm_update.inc"
	UT_ASSERT(vm_locked && (same_vm ? !vm_locked_new : vm_locked_new));
	UT_ASSERT(dml_locks[vmbuffer - 1] && dml_locks[vmbuffer_new - 1]);
	return 0;
l2:
	UT_ASSERT(!vm_locked && !vm_locked_new);
	UT_ASSERT_EQ(cid, 1);
	UT_ASSERT(!iscombo);
	UT_ASSERT_EQ(cluster_current_mx_old_plan + cluster_current_mx_new_plan, 0);
	UT_ASSERT(oldtup.t_data == (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 1)));
	UT_ASSERT_EQ(oldtup.t_len, SizeofHeapTupleHeader);
	UT_ASSERT(dml_locks[2] && !dml_locks[3] && !dml_locks[0] && !dml_locks[1]);
	UT_ASSERT_EQ(dml_refs[0] + dml_refs[1], 1);
	UT_ASSERT_EQ(dml_refs[2], 1);
	UT_ASSERT_EQ(dml_refs[3], 0);
	UT_ASSERT_EQ(vmbuffer_new, InvalidBuffer);
	UT_ASSERT_EQ(dml_reads, 1);
	return 1;
l_pgrac_reacquire:
	UT_ASSERT(!vm_locked && !vm_locked_new);
	UT_ASSERT_EQ(cluster_current_mx_old_plan + cluster_current_mx_new_plan, 0);
	UT_ASSERT(!dml_locks[2] && !dml_locks[3] && !dml_locks[0] && !dml_locks[1]);
	UT_ASSERT_EQ(dml_refs[0] + dml_refs[1], 0);
	UT_ASSERT_EQ(dml_refs[2], 1);
	UT_ASSERT_EQ(dml_refs[3], 0);
	UT_ASSERT_EQ(vmbuffer, InvalidBuffer);
	UT_ASSERT_EQ(vmbuffer_new, InvalidBuffer);
	return 2;
}

static void
reset_update_vm(int fail_at, bool replace)
{
	fixture_reset(VISIBILITYMAP_FORKNUM);
	memset(dml_refs, 0, sizeof(dml_refs));
	memset(dml_locks, 0, sizeof(dml_locks));
	dml_reads = dml_releases = dml_nacquires = 0;
	dml_fail_at = fail_at;
	dml_replace = replace;
	for (int i = 0; i < 4; i++) {
		memcpy(dml_pages[i].data, pages[2].data, BLCKSZ);
		dml_tags[i] = tags[i < 2 ? i : 2];
		dml_tags[i].blockNum = i;
	}
}

UT_TEST(actual_update_pair_orders_distinct_locks_and_locks_alias_once)
{
	for (int shape = 0; shape < 3; shape++) {
		reset_update_vm(0, false);
		UT_ASSERT_EQ(run_update_vm_bracket(shape == 0, shape == 2, false), 0);
		UT_ASSERT_EQ(dml_nacquires, shape == 0 ? 1 : 2);
		UT_ASSERT_EQ(dml_acquires[0], 1);
		if (shape != 0)
			UT_ASSERT_EQ(dml_acquires[1], 2);
	}
}

UT_TEST(actual_update_second_vm_refusal_unwinds_first_lock_for_both_reentries)
{
	for (int bits = 0; bits < 8; bits++) {
		reset_update_vm(2, (bits & 1) != 0);
		if (sigsetjmp(failure_jump, 1) == 0)
			UT_ASSERT_EQ(run_update_vm_bracket(false, (bits & 2) != 0, (bits & 4) != 0),
						 (bits & 4) ? 2 : 1);
		else
			UT_ASSERT(false);
		UT_ASSERT_EQ(dml_acquires[0], 1);
		UT_ASSERT_EQ(dml_acquires[1], 2);
	}
}

UT_TEST(actual_update_shared_vm_loss_invalidates_both_aliases)
{
	for (int temp = 0; temp < 2; temp++) {
		reset_update_vm(1, true);
		if (sigsetjmp(failure_jump, 1) == 0)
			UT_ASSERT_EQ(run_update_vm_bracket(true, false, temp), temp ? 2 : 1);
		else
			UT_ASSERT(false);
	}
}

#undef PG_TRY
#undef PG_CATCH
#undef PG_END_TRY
#undef PG_RE_THROW
#define PG_TRY()                                                                                   \
	do {                                                                                           \
		if (true) {
#define PG_CATCH()                                                                                 \
	}                                                                                              \
	else                                                                                           \
	{
#define PG_END_TRY()                                                                               \
	}                                                                                              \
	}                                                                                              \
	while (0)
#define PG_RE_THROW() fixture_error()
static void
run_delete_vm_bracket(void)
{
	Relation relation = &relation_data;
	Buffer buffer = 3, vmbuffer = 1;
	Page page = dml_pages[2].data;
	HeapTupleData tp = { 0 };
	HeapTuple old_key_tuple = NULL;
	ItemId lp;
	ItemPointerData tid_data;
	ItemPointer tid = &tid_data;
	BlockNumber block = 0;
	bool vm_locked = false, old_key_copied = false, iscombo = true;
	CommandId cid = 2, pgrac_entry_cid = 1;
	int cluster_current_mx_plan = 1;
	ResourceXAuxiliaryAcquireContext cluster_vm_context = { 0 };

	dml_old_vm = 1;
	dml_refs[0] = dml_refs[2] = 1;
	dml_locks[2] = true;
	ItemPointerSet(tid, 0, 1);
#include "test_cluster_heap_vm_delete.inc"
	fixture_error();
l1:
	UT_ASSERT_EQ(cluster_current_mx_plan, 0);
	UT_ASSERT(!vm_locked && !iscombo);
	UT_ASSERT_EQ(cid, 1);
	UT_ASSERT(tp.t_data == (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 1)));
	UT_ASSERT_EQ(tp.t_len, SizeofHeapTupleHeader);
	UT_ASSERT(dml_locks[2] && !dml_locks[0]);
}

static void
run_pretoast_vm_bracket(void)
{
	Relation relation = &relation_data;
	Buffer buffer = 3, vmbuffer = 1, vmbuffer_new = InvalidBuffer;
	Page page = dml_pages[2].data;
	HeapTupleData oldtup = { 0 };
	ItemId lp;
	ItemPointerData tid_data;
	ItemPointer otid = &tid_data;
	BlockNumber block = 0;
	bool vm_locked = false, iscombo = true, cluster_current_mx_recomposed = true;
	CommandId cid = 2, pgrac_entry_cid = 1;
	int cluster_current_mx_temp_lock_plan = 1, cluster_current_mx_operation = 1;
	ResourceXAuxiliaryAcquireContext cluster_vm_context = { 0 };

	dml_old_vm = 1;
	dml_refs[0] = dml_refs[2] = 1;
	dml_locks[2] = true;
	ItemPointerSet(otid, 0, 1);
#include "test_cluster_heap_vm_pretoast.inc"
	fixture_error();
l2:
	UT_ASSERT_EQ(cluster_current_mx_temp_lock_plan + cluster_current_mx_operation, 0);
	UT_ASSERT(!vm_locked && !iscombo);
	UT_ASSERT_EQ(cid, 1);
	UT_ASSERT(oldtup.t_data == (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 1)));
	UT_ASSERT_EQ(oldtup.t_len, SizeofHeapTupleHeader);
	UT_ASSERT(dml_locks[2] && !dml_locks[0]);
}

UT_TEST(actual_delete_vm_retry_refreshes_tuple_before_l1)
{
	reset_update_vm(1, true);
	run_delete_vm_bracket();
}

UT_TEST(actual_pretoast_vm_retry_refreshes_predecessor_before_l2)
{
	reset_update_vm(1, true);
	run_pretoast_vm_bracket();
}

#include "test_cluster_heap_hio_vm.h"

int
main(void)
{
	UT_PLAN(20);
	UT_RUN(actual_vm_consumer_requalifies_both_stages_without_replaying_clear);
	UT_RUN(actual_vm_unchanged_clear_is_completed_not_retry);
	UT_RUN(actual_consumer_rejects_stale_heap_proof_before_restoring_vm);
	UT_RUN(actual_consumer_cancellation_does_not_report_completion);
	UT_RUN(actual_fsm_consumer_keeps_original_mutation_and_restore);
	UT_RUN(actual_insert_vm_loss_returns_to_target_qualification_before_mutation);
	UT_RUN(actual_tuple_lock_vm_loss_reloads_tuple_before_l3);
	UT_RUN(actual_update_pair_orders_distinct_locks_and_locks_alias_once);
	UT_RUN(actual_update_second_vm_refusal_unwinds_first_lock_for_both_reentries);
	UT_RUN(actual_update_shared_vm_loss_invalidates_both_aliases);
	UT_RUN(actual_delete_vm_retry_refreshes_tuple_before_l1);
	UT_RUN(actual_pretoast_vm_retry_refreshes_predecessor_before_l2);
	UT_RUN(actual_hio_single_relock_carries_no_vm_pin);
	UT_RUN(actual_hio_pair_relock_carries_neither_vm_alias);
	UT_RUN(actual_hio_repin_failure_releases_partial_result);
	UT_RUN(actual_hio_newly_visible_and_frozen_require_exact_maps);
	UT_RUN(actual_hio_declared_pin_release_and_alias_counts);
	UT_RUN(actual_hio_disabled_local_and_cancellation_boundaries);
	UT_RUN(actual_hio_allocation_reads_and_locks_without_incoming_vm_pins);
	UT_RUN(actual_hio_rejected_candidate_and_frozen_extension_recheck_maps);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
