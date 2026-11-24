/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_hot_prune_ahead.c
 *	  R4 holder-built FULL images as consumed by the scratch HOT walker.
 *
 *	  The holder step of cluster_cr.c, the page primitives of bufpage.c and
 *	  the scratch walker of heapam.c are the real product functions.  Undo
 *	  record I/O and the per-tuple scratch visibility verdict are the only
 *	  controlled boundaries: records are served from a private table and a
 *	  transaction is visible exactly when its commit SCN is at or before the
 *	  read SCN.
 *
 *	  Page/record shapes are constructed by this fixture, not by the heap
 *	  writer or pruner.  In particular ut_prune bypasses the live retention
 *	  predicate; it tests a sufficient refusal shape, not its reachability.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_r4_hot_prune_ahead.c
 *
 * NOTES
 *	  This is a pgrac-original file.  An unchanged redirect reaches the
 *	  restored predecessor.  A manually advanced redirect is refused rather
 *	  than answered; these tests do not prove the live floor protects readers.
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"
#include "miscadmin.h"
#include "utils/timestamp.h"

#include <setjmp.h>
#include <stdarg.h>
#include <string.h>

#include "access/htup_details.h"
#include "storage/bufpage.h"
#include "utils/rel.h"
#include "utils/snapshot.h"

#include "cluster/cluster_cr.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_cr_coordinator_stat.h"
#include "cluster/cluster_itl_slot.h"
#include "cluster/cluster_tx_resolve.h"
#include "cluster/cluster_undo_record.h"
#include "cluster/cluster_undo_record_api.h"
#include "cluster/cluster_uba.h"
#include "../../backend/access/heap/heapam_r4_private.h"

#undef printf
#undef fprintf
#undef snprintf
#undef vsnprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/* ---------- controlled process boundaries of the linked product ---------- */

sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
int			cluster_cross_instance_cr_coordinator = CR_COORD_MODE_OFF;
bool		cluster_cross_instance_cr_probe;
int			cluster_node_id = 0;
int			cluster_cr_chain_walk_max_steps = 4096;
bool		cluster_trace_normal_stop = false;
volatile uint32 CritSectionCount;
int			MyProcPid = 4242;
MemoryContext TopMemoryContext = (MemoryContext) 0x1;

static char ut_last_log[4096];
static char ut_logs[65536];
static unsigned ut_log_count;

ClusterCrCoordOriginClass
cluster_cr_coordinator_classify_origin(NodeId origin)
{
	UT_ASSERT_EQ(origin, 0);
	return CR_COORD_ORIGIN_OWN;
}

void
cluster_cr_coordinator_stat_bump(ClusterCrCoordCounter counter pg_attribute_unused())
{
	UT_ASSERT(false);
}

TimestampTz GetCurrentTimestamp(void) { return 12500; }
void		pg_re_throw(void) { abort(); }
void		pfree(void *pointer) { free(pointer); }

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

void *
palloc(Size size)
{
	void	   *result = malloc(size);

	if (result == NULL)
		abort();
	return result;
}

void *
MemoryContextAllocZero(MemoryContext context pg_attribute_unused(), Size size)
{
	void	   *pointer = malloc(size);

	if (pointer == NULL)
		abort();
	memset(pointer, 0, size);
	return pointer;
}

/* Only LOG is emitted by a well-formed build; anything stronger is a failure. */
bool
errstart(int level, const char *domain pg_attribute_unused())
{
	UT_ASSERT(level <= LOG);
	return level == LOG;
}

bool
errstart_cold(int level, const char *domain pg_attribute_unused())
{
	UT_ASSERT(level < ERROR);
	if (level >= ERROR)
		abort();
	return false;
}

/* Message text other than the internal LOG line is not examined here. */
int			errcode(int code pg_attribute_unused()) { return 0; }
int			errmsg(const char *format pg_attribute_unused(),...) { return 0; }
int			errdetail(const char *format pg_attribute_unused(),...) { return 0; }
int			errhint(const char *format pg_attribute_unused(),...) { return 0; }

int
errmsg_internal(const char *format,...)
{
	va_list		args;

	va_start(args, format);
	vsnprintf(ut_last_log, sizeof(ut_last_log), format, args);
	va_end(args);
	return 0;
}

void
errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
		  const char *func pg_attribute_unused())
{
	ut_log_count++;
	strncat(ut_logs, ut_last_log, sizeof(ut_logs) - strlen(ut_logs) - 2);
	strncat(ut_logs, "\n", sizeof(ut_logs) - strlen(ut_logs) - 1);
}

int
scn_time_cmp(SCN a, SCN b)
{
	uint64		la = scn_local(a);
	uint64		lb = scn_local(b);

	if (la < lb)
		return -1;
	if (la > lb)
		return 1;
	return 0;
}

/* ---------- private undo table: exact UBA lookup, nothing else ---------- */

#define UT_RECORDS 8

static PGAlignedBlock ut_records[UT_RECORDS];
static UBA	ut_record_ubas[UT_RECORDS];
static bool ut_record_used[UT_RECORDS];
static unsigned ut_record_reads;

size_t
cluster_undo_get_record(UBA uba, void *out_buffer, size_t buffer_size)
{
	unsigned	i;

	ut_record_reads++;
	for (i = 0; i < UT_RECORDS; i++)
	{
		const UndoRecordHeader *header = (const UndoRecordHeader *) ut_records[i].data;
		size_t		length;

		if (!ut_record_used[i] || !cluster_undo_record_uba_equal(ut_record_ubas[i], uba))
			continue;
		length = sizeof(*header) + header->payload_length;
		if (out_buffer == NULL || length > buffer_size)
			return 0;
		memcpy(out_buffer, ut_records[i].data, length);
		return length;
	}
	return 0;
}

/* ---------- controlled transaction outcomes for the scratch verdict ---------- */

#define UT_XACTS 8

typedef struct UtXact
{
	TransactionId xid;
	SCN			commit_scn;		/* InvalidScn: never committed */
} UtXact;

static UtXact ut_xacts[UT_XACTS];
static unsigned ut_xact_count;
static unsigned ut_verdicts;

static void
ut_xact_set(TransactionId xid, SCN commit_scn)
{
	unsigned	i;

	for (i = 0; i < ut_xact_count; i++)
	{
		if (ut_xacts[i].xid == xid)
		{
			ut_xacts[i].commit_scn = commit_scn;
			return;
		}
	}
	UT_ASSERT(ut_xact_count < UT_XACTS);
	ut_xacts[ut_xact_count].xid = xid;
	ut_xacts[ut_xact_count].commit_scn = commit_scn;
	ut_xact_count++;
}

static bool
ut_committed_at(TransactionId xid, SCN read_scn)
{
	unsigned	i;

	for (i = 0; i < ut_xact_count; i++)
	{
		if (ut_xacts[i].xid == xid)
			return SCN_VALID(ut_xacts[i].commit_scn)
				&& scn_time_cmp(ut_xacts[i].commit_scn, read_scn) <= 0;
	}
	UT_ASSERT(false);			/* every tuple names a declared transaction */
	return false;
}

bool
HeapTupleSatisfiesMVCCScratch(HeapTuple htup, Snapshot snapshot,
							  const ClusterR4HotScratchTestContext *context)
{
	HeapTupleHeader tuple = htup->t_data;

	UT_ASSERT(context->already_full && !context->allow_hint && !context->allow_cleanout);
	UT_ASSERT_EQ(context->read_scn, snapshot->read_scn);
	ut_verdicts++;
	if (!ut_committed_at(HeapTupleHeaderGetRawXmin(tuple), snapshot->read_scn))
		return false;
	if ((tuple->t_infomask & HEAP_XMAX_INVALID) != 0
		|| HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
		return true;
	return !ut_committed_at(HeapTupleHeaderGetRawXmax(tuple), snapshot->read_scn);
}

/* ---------- the real scratch walker and its two refusal exits ---------- */

static sigjmp_buf ut_walker_exit;
static const char *ut_refusal;
static bool ut_too_old;

static pg_attribute_noreturn() void
heap_hot_r4_unknown(const char *reason)
{
	ut_refusal = reason;
	siglongjmp(ut_walker_exit, 1);
}

static pg_attribute_noreturn() void
heap_hot_r4_snapshot_too_old(SCN read_scn pg_attribute_unused(),
							 SCN recycle_scn pg_attribute_unused())
{
	ut_too_old = true;
	siglongjmp(ut_walker_exit, 1);
}

#include "test_cluster_heap_cr_reuse_scratch.inc"

/* ---------- page producers ---------- */

#define UT_BLOCK ((BlockNumber) 37)
#define UT_TUPLE_LENGTH 32
#define UT_WRAP ((uint16) 7)
#define UT_SEGMENT ((uint32) 1)
#define UT_TT_OFFSET ((uint16) 3)
#define UT_UNDO_BLOCK ((uint32) 7)

#define XID_FIRST ((TransactionId) 700)		/* creator of the pruned-to version */
#define XID_NEXT ((TransactionId) 800)		/* later updater / locker */

typedef enum UtWalk
{
	UT_WALK_FOUND,
	UT_WALK_NOT_FOUND,
	UT_WALK_REFUSED,
	UT_WALK_TOO_OLD
} UtWalk;

static PGAlignedBlock ut_page;
static BufferTag ut_tag;
static RelationData ut_relation;
static SnapshotData ut_snapshot;
static HeapHotSearchResult ut_result;
static uint64 ut_generation = 9000;

static UBA
ut_uba(unsigned record_index)
{
	return uba_encode(UT_SEGMENT, UT_UNDO_BLOCK, UT_TT_OFFSET, (uint16) (record_index + 1));
}

static ClusterItlSlotData *
ut_slot(uint8 slot_index)
{
	return &ClusterPageGetItlSlots((Page) ut_page.data)[slot_index];
}

static HeapTupleHeader
ut_tuple(OffsetNumber offset)
{
	ItemId		item = PageGetItemId((Page) ut_page.data, offset);

	UT_ASSERT(ItemIdIsNormal(item));
	return (HeapTupleHeader) PageGetItem((Page) ut_page.data, item);
}

static void
ut_reset(void)
{
	PageHeader	header = (PageHeader) ut_page.data;

	memset(&ut_page, 0, sizeof(ut_page));
	PageSetPageSizeAndVersion((Page) ut_page.data, BLCKSZ, PG_PAGE_LAYOUT_VERSION);
	header->pd_lower = SizeOfPageHeaderData;
	header->pd_upper = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
	header->pd_special = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
	header->pd_flags = PD_HAS_ITL;

	memset(ut_records, 0, sizeof(ut_records));
	memset(ut_record_ubas, 0, sizeof(ut_record_ubas));
	memset(ut_record_used, 0, sizeof(ut_record_used));
	memset(ut_xacts, 0, sizeof(ut_xacts));
	ut_xact_count = ut_record_reads = ut_verdicts = ut_log_count = 0;
	cluster_trace_normal_stop = false;
	ut_logs[0] = '\0';

	memset(&ut_tag, 0, sizeof(ut_tag));
	ut_tag.spcOid = 1663;
	ut_tag.dbOid = 5;
	ut_tag.relNumber = 20000;
	ut_tag.forkNum = MAIN_FORKNUM;
	ut_tag.blockNum = UT_BLOCK;
	memset(&ut_relation, 0, sizeof(ut_relation));
	ut_relation.rd_id = 16384;
}

/* Insert one fixed-length version at an exact, currently free offset. */
static void
ut_put_version(OffsetNumber offset, TransactionId xmin, uint8 slot_index, bool heap_only,
			   char marker)
{
	char		image[UT_TUPLE_LENGTH];
	HeapTupleHeader tuple = (HeapTupleHeader) image;

	memset(image, marker, sizeof(image));
	memset(image, 0, SizeofHeapTupleHeader);
	tuple->t_hoff = SizeofHeapTupleHeader;
	HeapTupleHeaderSetXmin(tuple, xmin);
	HeapTupleHeaderSetXmax(tuple, InvalidTransactionId);
	tuple->t_infomask = HEAP_XMAX_INVALID;
	tuple->t_infomask2 = heap_only ? HEAP_ONLY_TUPLE : 0;
	tuple->t_itl_slot_idx = slot_index;
	ItemPointerSet(&tuple->t_ctid, UT_BLOCK, offset);
	UT_ASSERT_EQ(PageAddItemExtended((Page) ut_page.data, (Item) image, sizeof(image), offset,
									 PAI_OVERWRITE | PAI_IS_HEAP),
				 offset);
}

/* A root whose first versions were pruned earlier: redirect, then its target. */
static void
ut_start_redirected_chain(void)
{
	PageHeader	header = (PageHeader) ut_page.data;
	ClusterItlSlotData *creator = ut_slot(1);

	header->pd_lower = SizeOfPageHeaderData + sizeof(ItemIdData);
	ItemIdSetRedirect(PageGetItemId((Page) ut_page.data, 1), 2);
	ut_put_version(2, XID_FIRST, 1, true, 'a');

	/* The creator finished long before any reader in these tests. */
	creator->xid = XID_FIRST;
	creator->wrap = UT_WRAP;
	creator->flags = ITL_FLAG_COMMITTED;
	creator->write_scn = (SCN) 80;
	creator->commit_scn = (SCN) 90;
	creator->undo_segment_head = uba_encode(UT_SEGMENT, UT_UNDO_BLOCK, UT_TT_OFFSET, 100);
	ut_xact_set(XID_FIRST, (SCN) 90);
}

static UndoRecordHeader *
ut_begin_record(unsigned record_index, uint8 record_type, TransactionId xid, SCN write_scn,
				OffsetNumber target_offset, UBA previous)
{
	UndoRecordHeader *record = (UndoRecordHeader *) ut_records[record_index].data;

	UT_ASSERT(record_index < UT_RECORDS && !ut_record_used[record_index]);
	memset(ut_records[record_index].data, 0, BLCKSZ);
	record->record_type = record_type;
	record->flags = UNDO_REC_FLAG_HAS_ITL_HISTORY
		| (UBA_is_invalid(previous) ? UNDO_REC_FLAG_FIRST_IN_TX : 0);
	record->xid = xid;
	record->origin_node_id = 0;
	record->tt_slot_segment_id = UT_SEGMENT;
	record->tt_slot_id = UT_TT_OFFSET + 1;
	record->tt_wrap_plus1 = UT_WRAP + 1;
	record->write_scn = write_scn + 1;	/* the record is consumed after the stamp */
	record->prev_uba = previous;
	record->target_locator.spcOid = ut_tag.spcOid;
	record->target_locator.dbOid = ut_tag.dbOid;
	record->target_locator.relNumber = ut_tag.relNumber;
	record->target_fork = MAIN_FORKNUM;
	record->target_block = UT_BLOCK;
	record->target_offset = target_offset;
	ut_record_ubas[record_index] = ut_uba(record_index);
	ut_record_used[record_index] = true;
	return record;
}

static void
ut_finish_record(UndoRecordHeader *record, Size body_length, uint8 slot_index, uint8 after_kind,
				 SCN write_scn)
{
	UndoItlHistoryTrailer history;

	memset(&history, 0, sizeof(history));
	history.magic = UNDO_ITL_HISTORY_MAGIC;
	history.version = UNDO_ITL_HISTORY_VERSION;
	history.count = 1;
	history.entries[0].block = UT_BLOCK;
	history.entries[0].slot_index = slot_index;
	history.entries[0].after_kind = after_kind;
	history.entries[0].after_write_scn = write_scn;
	history.entries[0].prior = *ut_slot(slot_index);
	memcpy((char *) record + sizeof(*record) + body_length, &history, sizeof(history));
	record->payload_length = (uint16) (body_length + sizeof(history));
}

/* Stamp the slot exactly as the DATA and row-lock stamps do. */
static void
ut_stamp_slot(uint8 slot_index, uint8 kind, TransactionId xid, SCN write_scn, UBA head)
{
	ClusterItlSlotData *slot = ut_slot(slot_index);

	if (slot->flags != ITL_FLAG_FREE && !(slot->flags == kind && slot->xid == xid))
		slot->wrap++;
	slot->xid = xid;
	slot->flags = kind;
	slot->commit_scn = InvalidScn;
	slot->write_scn = write_scn;
	slot->undo_segment_head = head;
}

/* Same-page HOT update: one UPDATE record, old header rewritten, successor added. */
static void
ut_hot_update(unsigned record_index, uint8 slot_index, TransactionId xid, SCN write_scn,
			  OffsetNumber old_offset, OffsetNumber new_offset, UBA previous, char marker)
{
	HeapTupleHeader old_tuple = ut_tuple(old_offset);
	UndoRecordHeader *record
		= ut_begin_record(record_index, UNDO_RECORD_UPDATE, xid, write_scn, old_offset, previous);
	UndoUpdatePayload *update = (UndoUpdatePayload *) ((char *) record + sizeof(*record));

	update->new_block = UT_BLOCK;
	update->new_offset = new_offset;
	update->old_tuple_length = UT_TUPLE_LENGTH;
	update->old_tuple_offset = sizeof(*update);
	memcpy((char *) update + sizeof(*update), old_tuple, UT_TUPLE_LENGTH);
	ut_finish_record(record, sizeof(*update) + UT_TUPLE_LENGTH, slot_index, ITL_FLAG_ACTIVE,
					 write_scn);

	ut_stamp_slot(slot_index, ITL_FLAG_ACTIVE, xid, write_scn, ut_uba(record_index));
	old_tuple->t_infomask &= ~HEAP_XMAX_BITS;
	old_tuple->t_infomask2 &= ~HEAP_KEYS_UPDATED;
	old_tuple->t_infomask2 |= HEAP_HOT_UPDATED;
	HeapTupleHeaderSetXmax(old_tuple, xid);
	old_tuple->t_itl_slot_idx = slot_index;
	ItemPointerSet(&old_tuple->t_ctid, UT_BLOCK, new_offset);
	ut_put_version(new_offset, xid, slot_index, true, marker);
}

/* Plain row lock, as taken on the predecessor before an UPDATE that must wait. */
static void
ut_lock_row(unsigned record_index, uint8 slot_index, TransactionId xid, SCN write_scn,
			OffsetNumber offset)
{
	HeapTupleHeader tuple = ut_tuple(offset);
	ClusterItlSlotData *slot = ut_slot(slot_index);
	UndoRecordHeader *record
		= ut_begin_record(record_index, UNDO_RECORD_ITL, xid, write_scn, offset,
						  (UBA) InvalidUba_init);
	UndoItlPayload lock;

	memset(&lock, 0, sizeof(lock));
	lock.itl_slot_idx = slot_index;
	lock.prev_flags = slot->flags;
	lock.new_flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
	lock.lock_xid = xid;
	lock.prev_xmax = HeapTupleHeaderGetRawXmax(tuple);
	lock.prev_infomask = tuple->t_infomask;
	lock.prev_infomask2 = tuple->t_infomask2;
	lock.prev_commit_scn = slot->commit_scn;
	lock.prev_undo_segment_head = slot->undo_segment_head;
	memcpy((char *) record + sizeof(*record), &lock, sizeof(lock));
	ut_finish_record(record, sizeof(lock), slot_index, ITL_FLAG_LOCK_ONLY_ACTIVE, write_scn);

	ut_stamp_slot(slot_index, ITL_FLAG_LOCK_ONLY_ACTIVE, xid, write_scn, ut_uba(record_index));
	tuple->t_infomask &= ~HEAP_XMAX_BITS;
	tuple->t_infomask2 &= ~(HEAP_KEYS_UPDATED | HEAP_HOT_UPDATED);
	tuple->t_infomask |= HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK;
	HeapTupleHeaderSetXmax(tuple, xid);
	ItemPointerSet(&tuple->t_ctid, UT_BLOCK, offset);
}

static void
ut_commit(uint8 slot_index, TransactionId xid, SCN commit_scn)
{
	ClusterItlSlotData *slot = ut_slot(slot_index);

	UT_ASSERT_EQ(slot->xid, xid);
	slot->flags = ITL_FLAG_COMMITTED;
	slot->commit_scn = commit_scn;
	ut_xact_set(xid, commit_scn);
}

/* What a HOT prune does to the line pointers; it writes no undo. */
static void
ut_prune(OffsetNumber root, OffsetNumber survivor, OffsetNumber removed)
{
	ItemIdSetRedirect(PageGetItemId((Page) ut_page.data, root), survivor);
	ItemIdSetUnused(PageGetItemId((Page) ut_page.data, removed));
	PageRepairFragmentation((Page) ut_page.data);
}

/* ---------- the two product entries under test ---------- */

static ClusterR4CrBuildStepResult
ut_build(SCN read_scn, ClusterCrBuildReason *reason, uint32 *steps)
{
	PGAlignedBlock foreign;
	ClusterR4CrSlotExtension extension;
	ClusterR4CrBuildStepResult result;
	uint64		generation = ++ut_generation;

	memset(&foreign, 0, sizeof(foreign));
	memset(&extension, 0, sizeof(extension));
	extension.slot_generation = generation;
	extension.owner.builder_incarnation = generation + 1;
	extension.route_proof.read_scn = read_scn;
	extension.route_proof.tag = ut_tag;
	result = cluster_cr_build_on_holder_step(0, generation, false, &extension, ut_page.data,
											 foreign.data, reason);
	*steps = extension.build_steps;
	cluster_cr_build_on_holder_forget(0, generation);
	return result;
}

static UtWalk
ut_walk(OffsetNumber root_offset, SCN read_scn, bool scoped, OffsetNumber *found_offset)
{
	ItemPointerData root;
	volatile UtWalk outcome = UT_WALK_REFUSED;

	ItemPointerSet(&root, UT_BLOCK, root_offset);
	memset(&ut_snapshot, 0, sizeof(ut_snapshot));
	ut_snapshot.snapshot_type = SNAPSHOT_MVCC;
	ut_snapshot.cluster_source = SNAPSHOT_SOURCE_CLUSTER;
	ut_snapshot.read_scn = read_scn;
	memset(&ut_result, 0, sizeof(ut_result));
	memcpy(ut_result.scratch_page, ut_page.data, BLCKSZ);
	ut_refusal = NULL;
	ut_too_old = false;
	*found_offset = InvalidOffsetNumber;

	if (sigsetjmp(ut_walker_exit, 0) == 0)
	{
		if (heap_hot_r4_search_scratch(&ut_tag, &root, &ut_relation, &ut_snapshot, &ut_result,
									   scoped))
		{
			*found_offset = ItemPointerGetOffsetNumber(&ut_result.tuple.t_self);
			outcome = UT_WALK_FOUND;
		}
		else
			outcome = UT_WALK_NOT_FOUND;
	}
	else
		outcome = ut_too_old ? UT_WALK_TOO_OLD : UT_WALK_REFUSED;
	return outcome;
}

/* ---------- tests ---------- */

/*
 * The updater wrote after the reader's SCN and the page was never pruned
 * past the reader.  The holder removes the successor, restores the redirect
 * target byte for byte, and the walker reaches it through the redirect.
 */
UT_TEST(test_unchanged_redirect_reaches_restored_target)
{
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	char		before[UT_TUPLE_LENGTH];
	uint32		steps = 0;
	OffsetNumber found;
	int			scoped;

	ut_reset();
	ut_start_redirected_chain();
	memcpy(before, ut_tuple(2), sizeof(before));
	ut_hot_update(0, 0, XID_NEXT, (SCN) 200, 2, 3, (UBA) InvalidUba_init, 'b');
	ut_xact_set(XID_NEXT, (SCN) 210);

	UT_ASSERT_EQ(ut_build((SCN) 100, &reason, &steps), CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(steps, 1);
	UT_ASSERT(ItemIdIsRedirected(PageGetItemId((Page) ut_page.data, 1)));
	UT_ASSERT_EQ(ItemIdGetRedirect(PageGetItemId((Page) ut_page.data, 1)), 2);
	UT_ASSERT(!ItemIdIsUsed(PageGetItemId((Page) ut_page.data, 3)));
	UT_ASSERT_EQ(memcmp(ut_tuple(2), before, sizeof(before)), 0);
	UT_ASSERT_EQ(ut_slot(0)->flags, ITL_FLAG_FREE);

	for (scoped = 0; scoped < 2; scoped++)
	{
		UT_ASSERT_EQ(ut_walk(1, (SCN) 100, scoped != 0, &found), UT_WALK_FOUND);
		UT_ASSERT_EQ(found, 2);
	}
	UT_ASSERT_EQ(ut_log_count, 0);
}

/*
 * A row lock taken on the predecessor is not a row version.  With the lock
 * and the following HOT update both after the reader, the holder peels the
 * update and then the lock, and the predecessor comes back exactly as it was
 * before either.  With only the update after the reader, the still-locked
 * predecessor is the reader's row.
 */
UT_TEST(test_row_lock_before_hot_update_is_peeled_as_a_lock)
{
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	char		before_lock[UT_TUPLE_LENGTH];
	char		locked[UT_TUPLE_LENGTH];
	uint32		steps = 0;
	OffsetNumber found;
	int			between;

	for (between = 0; between < 2; between++)
	{
		SCN			read_scn = between ? (SCN) 160 : (SCN) 100;

		ut_reset();
		ut_start_redirected_chain();
		memcpy(before_lock, ut_tuple(2), sizeof(before_lock));
		ut_lock_row(0, 2, XID_NEXT, (SCN) 150, 2);
		memcpy(locked, ut_tuple(2), sizeof(locked));
		ut_hot_update(1, 0, XID_NEXT, (SCN) 200, 2, 3, ut_uba(0), 'b');
		ut_xact_set(XID_NEXT, (SCN) 210);

		UT_ASSERT_EQ(ut_build(read_scn, &reason, &steps), CLUSTER_R4_CR_STEP_FULL);
		UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
		UT_ASSERT_EQ(steps, between ? 1 : 2);
		UT_ASSERT(!ItemIdIsUsed(PageGetItemId((Page) ut_page.data, 3)));
		UT_ASSERT_EQ(memcmp(ut_tuple(2), between ? locked : before_lock, UT_TUPLE_LENGTH), 0);
		UT_ASSERT_EQ(ut_slot(0)->flags, ITL_FLAG_FREE);
		UT_ASSERT_EQ(ut_slot(2)->flags, between ? ITL_FLAG_LOCK_ONLY_ACTIVE : ITL_FLAG_FREE);

		UT_ASSERT_EQ(ut_walk(1, read_scn, false, &found), UT_WALK_FOUND);
		UT_ASSERT_EQ(found, 2);
		UT_ASSERT_EQ(ut_log_count, 0);
	}
}

/*
 * The page was pruned as if the updater had committed below every reader,
 * while this reader's SCN precedes the update.  The holder still removes the
 * successor and restores the predecessor from undo, but the root keeps the
 * redirect a prune gave it.  The image is not a row the walker can prove:
 * it must refuse, in both the per-fetch and the scan-scoped consumer, and it
 * must never answer "no such row".
 */
UT_TEST(test_manually_advanced_redirect_is_refused)
{
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	char		before[UT_TUPLE_LENGTH];
	uint32		steps = 0;
	OffsetNumber found;
	int			scoped;

	ut_reset();
	ut_start_redirected_chain();
	memcpy(before, ut_tuple(2), sizeof(before));
	ut_hot_update(0, 0, XID_NEXT, (SCN) 200, 2, 3, (UBA) InvalidUba_init, 'b');
	ut_commit(0, XID_NEXT, (SCN) 210);
	ut_prune(1, 3, 2);

	UT_ASSERT_EQ(ut_build((SCN) 100, &reason, &steps), CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(steps, 1);
	/* the reader's version is back, yet nothing leads to it */
	UT_ASSERT_EQ(memcmp(ut_tuple(2), before, sizeof(before)), 0);
	UT_ASSERT(ItemIdIsRedirected(PageGetItemId((Page) ut_page.data, 1)));
	UT_ASSERT_EQ(ItemIdGetRedirect(PageGetItemId((Page) ut_page.data, 1)), 3);
	UT_ASSERT(!ItemIdIsUsed(PageGetItemId((Page) ut_page.data, 3)));

	for (scoped = 0; scoped < 2; scoped++)
	{
		UT_ASSERT_EQ(ut_walk(1, (SCN) 100, scoped != 0, &found), UT_WALK_REFUSED);
		UT_ASSERT(ut_refusal != NULL
				  && strcmp(ut_refusal, "HOT root or member is unused or unprovable") == 0);
		UT_ASSERT_EQ(ut_verdicts, 0);	/* refused before any visibility verdict */
	}
	UT_ASSERT_EQ(ut_log_count, 0); /* diagnostics stay off on the ordinary path */
}

/*
 * The holder cannot repair the root, but it knows which record removed the
 * redirect target.  It retains that identity once, without changing the image
 * or its result.
 */
UT_TEST(test_manually_advanced_redirect_is_logged_when_tracing)
{
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	uint32		steps = 0;
	const char *observation;

	ut_reset();
	ut_start_redirected_chain();
	ut_hot_update(0, 0, XID_NEXT, (SCN) 200, 2, 3, (UBA) InvalidUba_init, 'b');
	ut_commit(0, XID_NEXT, (SCN) 210);
	ut_prune(1, 3, 2);
	cluster_trace_normal_stop = true;

	UT_ASSERT_EQ(ut_build((SCN) 100, &reason, &steps), CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	observation = strstr(ut_logs, "PGRAC_FAMILY=R4_HISTORY_ORPHAN_ROOT");
	UT_ASSERT(observation != NULL);
	UT_ASSERT(observation == NULL
			  || strstr(observation + 1, "PGRAC_FAMILY=R4_HISTORY_ORPHAN_ROOT") == NULL);
	UT_ASSERT(strstr(ut_logs, "tag=1663/5/20000/0/37 read_scn=100 ") != NULL);
	UT_ASSERT(strstr(ut_logs, " root=1 successor=3 predecessor=2 xid=800 ") != NULL);
	UT_ASSERT(strstr(ut_logs, " record_write_scn=201 ") != NULL);
}

int
main(int argc pg_attribute_unused(), char **argv pg_attribute_unused())
{
	UT_PLAN(4);
	UT_RUN(test_unchanged_redirect_reaches_restored_target);
	UT_RUN(test_row_lock_before_hot_update_is_peeled_as_a_lock);
	UT_RUN(test_manually_advanced_redirect_is_refused);
	UT_RUN(test_manually_advanced_redirect_is_logged_when_tracing);

	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
