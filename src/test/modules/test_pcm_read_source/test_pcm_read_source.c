/* Native buffer operations for the first-read race TAP; not linked in postgres. */
#include "postgres.h"
#include "access/relation.h"
#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_guc.h"
#endif
#include "fmgr.h"
#include "varatt.h"
#include "miscadmin.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "utils/fmgrprotos.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(test_pcm_read_source_cached);
PG_FUNCTION_INFO_V1(test_pcm_read_source_page);
PG_FUNCTION_INFO_V1(test_pcm_read_source_flush);

static int test_pcm_read_source_hold_shared_ms;

PGDLLEXPORT void _PG_init(void);

void
_PG_init(void)
{
	DefineCustomIntVariable("test_pcm_read_source.hold_shared_ms",
							"Pause a native test page read while holding SHARE.",
							NULL, &test_pcm_read_source_hold_shared_ms,
							0, 0, 30000, PGC_SUSET, 0, NULL, NULL, NULL);
}

static Relation
open_test_relation(Oid oid)
{
	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("superuser required")));
	return relation_open(oid, AccessShareLock);
}

Datum
test_pcm_read_source_cached(PG_FUNCTION_ARGS)
{
	Relation rel = open_test_relation(PG_GETARG_OID(0));
	BufferTag tag;
	bool found = false;
	int i;
	InitBufferTag(&tag, &rel->rd_locator, MAIN_FORKNUM, 0);
	for (i = 0; i < NBuffers; i++) {
		BufferDesc *buf = GetBufferDescriptor(i);
		uint32 state = LockBufHdr(buf);
		found = (state & BM_TAG_VALID) != 0 && BufferTagsEqual(&buf->tag, &tag);
		UnlockBufHdr(buf, state);
		if (found)
			break;
	}
	relation_close(rel, AccessShareLock);
	PG_RETURN_BOOL(found);
}

static bytea *
read_test_page(Oid oid, bool flush)
{
	Relation rel = open_test_relation(oid);
	Buffer buffer = ReadBuffer(rel, 0);
	bytea *result = palloc(VARHDRSZ + BLCKSZ);
	SET_VARSIZE(result, VARHDRSZ + BLCKSZ);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	if (flush)
		FlushOneBuffer(buffer);
	else if (test_pcm_read_source_hold_shared_ms > 0) {
		int node = -1;

#ifdef USE_PGRAC_CLUSTER
		node = cluster_node_id;
#endif
		ereport(LOG, (errmsg_internal("TEST_PCM_READ_SOURCE phase=SHARED_HELD time=" INT64_FORMAT
			" node=%d pid=%d tag=%u/%u/%u/0/0 buffer=%d hold_ms=%d",
			GetCurrentTimestamp(), node, MyProcPid, rel->rd_locator.spcOid,
			rel->rd_locator.dbOid, rel->rd_locator.relNumber, buffer,
			test_pcm_read_source_hold_shared_ms)));
		/* Native pg_sleep handles interrupts and early latch/signal wakeups. */
		(void)DirectFunctionCall1(pg_sleep,
			Float8GetDatum(test_pcm_read_source_hold_shared_ms / 1000.0));
		ereport(LOG, (errmsg_internal("TEST_PCM_READ_SOURCE phase=SHARED_RELEASE time=" INT64_FORMAT
			" node=%d pid=%d tag=%u/%u/%u/0/0 buffer=%d hold_ms=%d",
			GetCurrentTimestamp(), node, MyProcPid, rel->rd_locator.spcOid,
			rel->rd_locator.dbOid, rel->rd_locator.relNumber, buffer,
			test_pcm_read_source_hold_shared_ms)));
	}
	memcpy(VARDATA(result), BufferGetPage(buffer), BLCKSZ);
	UnlockReleaseBuffer(buffer);
	relation_close(rel, AccessShareLock);
	return result;
}

Datum
test_pcm_read_source_page(PG_FUNCTION_ARGS)
{
	PG_RETURN_BYTEA_P(read_test_page(PG_GETARG_OID(0), false));
}

Datum
test_pcm_read_source_flush(PG_FUNCTION_ARGS)
{
	PG_RETURN_BYTEA_P(read_test_page(PG_GETARG_OID(0), true));
}
