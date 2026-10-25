/* Native buffer operations for the first-read race TAP; not linked in postgres. */
#include "postgres.h"
#include "access/relation.h"
#include "fmgr.h"
#include "varatt.h"
#include "miscadmin.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(test_pcm_read_source_cached);
PG_FUNCTION_INFO_V1(test_pcm_read_source_page);
PG_FUNCTION_INFO_V1(test_pcm_read_source_flush);

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
