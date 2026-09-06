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
#include "catalog/pg_type.h"
#include "cluster/cluster_pcm_lock.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "portability/instr_time.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "storage/latch.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/wait_event.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(test_pgrac_aux_vm_pin);
PG_FUNCTION_INFO_V1(test_pgrac_aux_cycle);
PG_FUNCTION_INFO_V1(test_pgrac_aux_step);
PG_FUNCTION_INFO_V1(test_pgrac_micro_heap_step);
PG_FUNCTION_INFO_V1(test_pgrac_rx_trace_begin);
PG_FUNCTION_INFO_V1(test_pgrac_rx_trace_seal);
PG_FUNCTION_INFO_V1(test_pgrac_rx_trace_stats);
PG_FUNCTION_INFO_V1(test_pgrac_rx_trace_export);
PG_FUNCTION_INFO_V1(test_pgrac_rx_trace_release);

static uint64 operation_sequence;

static void
probe_superuser(void)
{
	if (!superuser())
		ereport(ERROR, (errmsg("consumer observation requires superuser")));
}

static void
probe_operation(Relation relation, ForkNumber fork, BlockNumber block, uint16 kind, uint64 sequence,
				uint64 argument)
{
	ResourceXTraceEvent event = { 0 };

	InitBufferTag(&event.assertion.resource, &relation->rd_locator, fork, block);
	event.assertion.requester_node = cluster_node_id;
	event.kind = kind;
	event.value[0] = sequence;
	event.value[1] = argument;
	cluster_pcm_lock_resource_x_trace_note(&event);
}

static uint64
probe_next_operation(void)
{
	if (operation_sequence == UINT64_MAX)
		ereport(ERROR, (errmsg("consumer observation operation sequence exhausted")));
	return ++operation_sequence;
}

static Datum aux_cycle_internal(FunctionCallInfo fcinfo, bool describe);

Datum
test_pgrac_aux_cycle(PG_FUNCTION_ARGS)
{
	return aux_cycle_internal(fcinfo, true);
}

Datum
test_pgrac_aux_step(PG_FUNCTION_ARGS)
{
	/* The finite diagnostic uses the first 16 distinct heap blocks. These
	 * map to VM block 0 and the first FSM leaf (physical block 2 at 8 KiB).
	 * No per-operation JSON or observation SQL is on the timed path. */
	if (PG_GETARG_INT32(1) < 0 || PG_GETARG_INT32(1) >= 16 || BLCKSZ != 8192)
		ereport(ERROR, (errmsg("auxiliary timed step requires one of the first 16 heap blocks")));
	return aux_cycle_internal(fcinfo, false);
}

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
static Datum
aux_cycle_internal(FunctionCallInfo fcinfo, bool describe)
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
	uint64 sequence;
	BlockNumber observed_block;

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
	sequence = probe_next_operation();
	observed_block = block < 16 ? (fork_arg == VISIBILITYMAP_FORKNUM ? 0 : 2) : InvalidBlockNumber;
	probe_operation(relation, fork_arg, observed_block, RESOURCE_X_TRACE_OPERATION_BEGIN, sequence,
					block);
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
	probe_operation(relation, fork_arg, observed_block, RESOURCE_X_TRACE_OPERATION_DONE, sequence,
					2);
	table_close(relation, ShareLock);
	if (!describe)
		PG_RETURN_VOID();
	INSTR_TIME_SET_CURRENT(elapsed);
	INSTR_TIME_SUBTRACT(elapsed, started);
	result = psprintf("{\"pid\":%d,\"heap_block\":%u,\"fork\":%d,\"changes\":2,"
					  "\"before\":%zu,\"after\":%zu,\"operation_us\":%llu}",
					  MyProcPid, block, fork_arg, before, after,
					  (unsigned long long)INSTR_TIME_GET_MICROSEC(elapsed));
	PG_RETURN_TEXT_P(cstring_to_text(result));
}

/* A real SQL UPDATE, including its ordinary TT/undo/WAL and commit costs.
 * Sixteen immutable worker IDs share the one measured existing heap block.
 * A moved tuple is an error, not a successful single-block observation. */
Datum
test_pgrac_micro_heap_step(PG_FUNCTION_ARGS)
{
	Relation relation;
	Oid types[1] = { INT4OID };
	Datum values[1];
	char *query;
	uint64 sequence;
	bool isnull;
	Datum tid;
	int result;

	probe_superuser();
	if (PG_GETARG_INT32(1) < 1 || PG_GETARG_INT32(1) > 16)
		ereport(ERROR, (errmsg("heap timed step requires a fixed worker id from 1 to 16")));
	relation = table_open(PG_GETARG_OID(0), RowExclusiveLock);
	if (relation->rd_rel->relkind != RELKIND_RELATION
		|| relation->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT
		|| strcmp(RelationGetRelationName(relation), "micro_heap") != 0
		|| RelationGetNumberOfBlocks(relation) != 1)
		ereport(ERROR,
				(errmsg("heap timed step requires the existing single-block micro_heap fixture")));
	query = psprintf("UPDATE %s SET value=value # 1 WHERE id=$1 RETURNING ctid",
					 quote_qualified_identifier(get_namespace_name(RelationGetNamespace(relation)),
												RelationGetRelationName(relation)));
	values[0] = PG_GETARG_DATUM(1);
	sequence = probe_next_operation();
	probe_operation(relation, MAIN_FORKNUM, 0, RESOURCE_X_TRACE_OPERATION_BEGIN, sequence,
					PG_GETARG_INT32(1));
	if (SPI_connect() != SPI_OK_CONNECT)
		ereport(ERROR, (errmsg("heap timed step could not connect SPI")));
	result = SPI_execute_with_args(query, 1, types, values, NULL, false, 0);
	if (result != SPI_OK_UPDATE_RETURNING || SPI_processed != 1 || SPI_tuptable == NULL)
		ereport(ERROR, (errmsg("heap timed step did not update exactly one row")));
	tid = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
	if (isnull || ItemPointerGetBlockNumber((ItemPointer)DatumGetPointer(tid)) != 0)
		ereport(ERROR, (errmsg("heap timed step left the exact measured block")));
	if (SPI_finish() != SPI_OK_FINISH)
		ereport(ERROR, (errmsg("heap timed step could not finish SPI")));
	probe_operation(relation, MAIN_FORKNUM, 0, RESOURCE_X_TRACE_OPERATION_DONE, sequence, 1);
	table_close(relation, RowExclusiveLock);
	PG_RETURN_VOID();
}

Datum
test_pgrac_rx_trace_begin(PG_FUNCTION_ARGS)
{
	Relation relation;
	BufferTag tag;
	bool accepted;
	int32 fork = PG_GETARG_INT32(1);
	int32 block = PG_GETARG_INT32(2);
	int64 epoch = PG_GETARG_INT64(3);

	probe_superuser();
	if (fork < MAIN_FORKNUM || fork > VISIBILITYMAP_FORKNUM || block < 0 || epoch <= 0)
		ereport(ERROR, (errmsg("invalid exact observation selector or epoch")));
	relation = table_open(PG_GETARG_OID(0), AccessShareLock);
	if (relation->rd_rel->relkind != RELKIND_RELATION
		|| relation->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT)
		ereport(ERROR, (errmsg("observation selector requires a permanent table")));
	InitBufferTag(&tag, &relation->rd_locator, fork, block);
	accepted = cluster_pcm_lock_resource_x_trace_begin(&tag, epoch);
	table_close(relation, AccessShareLock);
	PG_RETURN_BOOL(accepted);
}

Datum
test_pgrac_rx_trace_seal(PG_FUNCTION_ARGS)
{
	probe_superuser();
	PG_RETURN_BOOL(cluster_pcm_lock_resource_x_trace_seal(PG_GETARG_INT64(0)));
}

Datum
test_pgrac_rx_trace_release(PG_FUNCTION_ARGS)
{
	probe_superuser();
	PG_RETURN_BOOL(cluster_pcm_lock_resource_x_trace_release(PG_GETARG_INT64(0)));
}

Datum
test_pgrac_rx_trace_stats(PG_FUNCTION_ARGS)
{
	ResourceXTraceStats stats;
	char *result;

	probe_superuser();
	if (!cluster_pcm_lock_resource_x_trace_snapshot(&stats))
		ereport(ERROR, (errmsg("bounded observation is unavailable")));
	result = psprintf("{\"tag\":[%u,%u,%u,%d,%u],\"state\":%u,\"epoch\":%llu,"
					  "\"count\":%llu,\"overflow\":%llu,\"late_events\":%llu,\"exported\":%llu,"
					  "\"started_us\":%llu,\"sealed_us\":%llu,\"capacity\":%u,\"owner_pid\":%d,"
					  "\"record_bytes\":%zu}",
					  stats.tag.spcOid, stats.tag.dbOid, stats.tag.relNumber, stats.tag.forkNum,
					  stats.tag.blockNum, stats.state, (unsigned long long)stats.epoch,
					  (unsigned long long)stats.count, (unsigned long long)stats.overflow,
					  (unsigned long long)stats.late_events, (unsigned long long)stats.exported,
					  (unsigned long long)stats.started_us, (unsigned long long)stats.sealed_us,
					  stats.capacity, stats.owner_pid, sizeof(ResourceXTraceEvent));
	PG_RETURN_TEXT_P(cstring_to_text(result));
}

Datum
test_pgrac_rx_trace_export(PG_FUNCTION_ARGS)
{
	ResourceXTraceStats stats;
	uint64 epoch = PG_GETARG_INT64(0);
	uint64 cursor = 0;
	Size bytes;
	bytea *result;

	probe_superuser();
	if (!cluster_pcm_lock_resource_x_trace_snapshot(&stats)
		|| stats.state != RESOURCE_X_TRACE_SEALED || stats.epoch != epoch
		|| stats.owner_pid != MyProcPid || stats.count > RESOURCE_X_TRACE_MAX_EVENTS)
		ereport(ERROR, (errmsg("observation export requires the exact sealed owner epoch")));
	bytes = stats.count * sizeof(ResourceXTraceEvent);
	result = palloc(bytes + VARHDRSZ);
	SET_VARSIZE(result, bytes + VARHDRSZ);
	while (cursor < stats.count) {
		/* At most 8 KiB under the observation lock. Formatting, allocation,
		 * interrupts and protocol output are all outside that lock. */
		ResourceXTraceEvent batch[64];
		uint32 copied = cluster_pcm_lock_resource_x_trace_read(epoch, cursor, batch, 64);

		if (copied == 0)
			ereport(ERROR, (errmsg("observation export lost its exact sealed epoch")));
		memcpy(VARDATA(result) + cursor * sizeof(*batch), batch, copied * sizeof(*batch));
		cursor += copied;
		CHECK_FOR_INTERRUPTS();
	}
	PG_RETURN_BYTEA_P(result);
}
