/*-------------------------------------------------------------------------
 *
 * test_pgrac_aux_consumer.c
 *    Pin a VM page through its real relation-level consumer for a TAP probe.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_tap/aux_probe/test_pgrac_aux_consumer.c
 *
 * NOTES
 *    Test-only, superuser-only module. No direct buffer/PCM state mutation,
 *    synthetic grant, page initialization, timeout change or injected return.
 *    A holder can keep the real pin until the test creates its release marker.
 *    Normal resource-owner cleanup still owns errors and cancellation.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>

#include "access/htup_details.h"
#include "access/table.h"
#include "access/visibilitymap.h"
#include "catalog/pg_class.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "portability/instr_time.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "storage/latch.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/wait_event.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(test_pgrac_aux_vm_pin);
PG_FUNCTION_INFO_V1(test_pgrac_aux_cycle);

Datum
test_pgrac_aux_vm_pin(PG_FUNCTION_ARGS)
{
	Relation relation;
	Buffer buffer = InvalidBuffer;
	Page page;
	PageHeader header;
	uint64 digest = UINT64CONST(14695981039346656037);
	bool hold = PG_GETARG_BOOL(1);
	char *release_path = NULL;
	char *result;
	int i;

	if (!superuser())
		ereport(ERROR, (errmsg("auxiliary consumer probe requires superuser")));
	if (hold) {
		release_path = psprintf("%s/aux-probe-release", DataDir);
		if (access(release_path, F_OK) == 0 || errno != ENOENT)
			ereport(ERROR, (errmsg("auxiliary consumer requires a fresh release marker path")));
	}
	relation = table_open(PG_GETARG_OID(0), AccessShareLock);
	if (relation->rd_rel->relkind != RELKIND_RELATION
		|| relation->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT)
		ereport(ERROR, (errmsg("auxiliary consumer probe requires a permanent table")));

	/* The production map reader owns both initialization and its locked
	 * PageIsNew recheck. This fixture neither initializes nor marks VM bits. */
	visibilitymap_pin(relation, 0, &buffer);
	if (!BufferIsValid(buffer) || BufferIsLocal(buffer))
		ereport(ERROR, (errmsg("auxiliary consumer did not return a shared pin")));
	page = BufferGetPage(buffer);
	header = (PageHeader)page;
	if (PageIsNew(page))
		ereport(ERROR, (errmsg("auxiliary consumer returned an uninitialized VM page")));
	for (i = SizeOfPageHeaderData; i < BLCKSZ; i++) {
		digest ^= (unsigned char)page[i];
		digest *= UINT64CONST(1099511628211);
	}
	result = psprintf("pid=%d rel=%u page_new=0 lower=%u upper=%u special=%u bitmap=%llu",
					  MyProcPid, RelationGetRelid(relation), header->pd_lower, header->pd_upper,
					  header->pd_special, (unsigned long long)digest);
	ereport(NOTICE, (errmsg("aux probe pinned: %s hold=%s", result, hold ? "true" : "false")));

	while (hold && access(release_path, F_OK) != 0) {
		if (errno != ENOENT)
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not read auxiliary probe release marker: %m")));
		CHECK_FOR_INTERRUPTS();
		(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 1L,
						PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
	}
	ReleaseBuffer(buffer);
	table_close(relation, AccessShareLock);
	PG_RETURN_TEXT_P(cstring_to_text(result));
}

/* Finite legal mutation, not a claim that a remote handoff took place.  The
 * caller assigns different frozen heap pages to concurrent workers; all may
 * share one auxiliary map page.  Table ShareLock excludes ordinary DML, and
 * content-X also protects the heap LSN written by visibilitymap_set(). */
Datum
test_pgrac_aux_cycle(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	int32 block_arg = PG_GETARG_INT32(1);
	int32 fork_arg = PG_GETARG_INT32(2);
	Relation relation;
	BlockNumber block;
	Buffer heap_buffer;
	Buffer vm_buffer = InvalidBuffer;
	Page heap_page;
	OffsetNumber offset;
	Size before;
	Size after;
	Size available;
	Size alternate;
	instr_time started;
	instr_time elapsed;
	char *result;

	if (!superuser())
		ereport(ERROR, (errmsg("auxiliary consumer probe requires superuser")));
	if (block_arg < 0 || (fork_arg != VISIBILITYMAP_FORKNUM && fork_arg != FSM_FORKNUM))
		ereport(ERROR, (errmsg("invalid auxiliary mutation block or fork")));
	INSTR_TIME_SET_CURRENT(started);
	block = (BlockNumber)block_arg;
	relation = table_open(relid, ShareLock);
	if (relation->rd_rel->relkind != RELKIND_RELATION
		|| relation->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT
		|| strcmp(RelationGetRelationName(relation), "aux_mutate") != 0)
		ereport(ERROR, (errmsg("auxiliary mutation requires the permanent aux_mutate fixture")));
	if (block >= RelationGetNumberOfBlocks(relation))
		ereport(ERROR, (errmsg("auxiliary mutation block is outside the existing heap")));
	if (fork_arg == VISIBILITYMAP_FORKNUM)
		visibilitymap_pin(relation, block, &vm_buffer);
	heap_buffer = ReadBuffer(relation, block);
	LockBuffer(heap_buffer, BUFFER_LOCK_EXCLUSIVE);
	heap_page = BufferGetPage(heap_buffer);
	if (PageIsNew(heap_page) || !PageIsAllVisible(heap_page))
		ereport(ERROR,
				(errmsg("auxiliary mutation requires an initialized all-visible heap page")));
	for (offset = FirstOffsetNumber; offset <= PageGetMaxOffsetNumber(heap_page); offset++) {
		ItemId item = PageGetItemId(heap_page, offset);
		HeapTupleHeader tuple;

		if (!ItemIdIsUsed(item))
			continue;
		if (!ItemIdIsNormal(item))
			ereport(ERROR, (errmsg("auxiliary mutation refuses non-normal heap items")));
		tuple = (HeapTupleHeader)PageGetItem(heap_page, item);
		if (!HeapTupleHeaderXminFrozen(tuple) || !(tuple->t_infomask & HEAP_XMAX_INVALID))
			ereport(ERROR, (errmsg("auxiliary mutation requires every tuple frozen without xmax")));
	}

	if (fork_arg == VISIBILITYMAP_FORKNUM) {
		before = visibilitymap_get_status(relation, block, &vm_buffer);
		if (before != VISIBILITYMAP_VALID_BITS)
			ereport(ERROR, (errmsg("auxiliary mutation requires a pre-frozen visibility map")));
		if (!visibilitymap_clear(relation, block, vm_buffer, VISIBILITYMAP_VALID_BITS)
			|| visibilitymap_get_status(relation, block, &vm_buffer) != 0)
			ereport(ERROR, (errmsg("auxiliary mutation did not clear the exact visibility bits")));
		/* The page is independently proven frozen under content-X.  Clearing
		 * is conservative; only the existing WAL-producing set API restores it. */
		MarkBufferDirty(heap_buffer);
		visibilitymap_set(relation, block, heap_buffer, InvalidXLogRecPtr, vm_buffer,
						  InvalidTransactionId, VISIBILITYMAP_VALID_BITS);
		after = visibilitymap_get_status(relation, block, &vm_buffer);
		ReleaseBuffer(vm_buffer);
	} else {
		available = PageGetHeapFreeSpace(heap_page);
		before = GetRecordedFreeSpace(relation, block);
		if (available < BLCKSZ / 256 || before > available)
			ereport(ERROR, (errmsg("auxiliary mutation lacks a conservative FSM range")));
		alternate = before == 0 ? BLCKSZ / 256 : 0;
		RecordPageWithFreeSpace(relation, block, alternate);
		if (GetRecordedFreeSpace(relation, block) != alternate)
			ereport(ERROR, (errmsg("auxiliary mutation did not change the exact FSM slot")));
		RecordPageWithFreeSpace(relation, block, before);
		after = GetRecordedFreeSpace(relation, block);
	}
	if (after != before)
		ereport(ERROR, (errmsg("auxiliary mutation failed to restore its conservative value")));
	UnlockReleaseBuffer(heap_buffer);
	table_close(relation, ShareLock);
	INSTR_TIME_SET_CURRENT(elapsed);
	INSTR_TIME_SUBTRACT(elapsed, started);
	result = psprintf("{\"pid\":%d,\"heap_block\":%u,\"fork\":%d,\"changes\":2,"
					  "\"before\":%zu,\"after\":%zu,\"operation_us\":%llu}",
					  MyProcPid, block, fork_arg, before, after,
					  (unsigned long long)INSTR_TIME_GET_MICROSEC(elapsed));
	PG_RETURN_TEXT_P(cstring_to_text(result));
}
