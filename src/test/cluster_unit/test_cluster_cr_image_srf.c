/*-------------------------------------------------------------------------
 *
 * test_cluster_cr_image_srf.c
 *    Actual SRF acquisition, refusal and row materialization choreography.
 *
 * SQL context, relation/buffer ownership, tuple deformation and target
 * transport are controlled boundaries. No backend, MVCC or native protocol
 * is simulated.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_cr_image_srf.c
 *
 * NOTES
 *    This is a pgrac-original test; production entry point names are stubbed
 *    at their SQL, buffer and transport boundaries.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "access/relation.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_type.h"
#include "cluster/cluster_cr.h"
#include "cluster/cluster_cr_server.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "storage/bufpage.h"
#include "utils/rel.h"
#include "utils/relcache.h"

#undef printf
#undef fprintf
#undef snprintf
#undef vsnprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
volatile uint32 InterruptHoldoffCount;
int NBuffers = 16;
int NLocBuffer;
static RelationData relation;
static FormData_pg_class rel_class;
static TupleDesc rel_desc, out_desc;
static PGAlignedBlock full_page, own_page;
static int fetched, constructed, reads, locked, pinned, closes, deforms, rows;
static int last_code;
static bool allowed, fetch_error, construct_error;
static ClusterCrBuildResult fetch_result;
static ClusterCrBuildReason fetch_reason;
static Datum output[3];
static void *allocations[64];
static int allocation_count;

void ExceptionalCondition(const char *c pg_attribute_unused(),
						  const char *f pg_attribute_unused(), int l pg_attribute_unused())
{ abort(); }
void pg_re_throw(void)
{ if (PG_exception_stack) siglongjmp(*PG_exception_stack, 1); abort(); }
bool errstart(int level, const char *domain pg_attribute_unused()) { return level >= ERROR; }
bool errstart_cold(int level, const char *domain) { return errstart(level, domain); }
int errcode(int code) { last_code = code; return 0; }
int errmsg(const char *fmt pg_attribute_unused(), ...) { return 0; }
int errdetail(const char *fmt pg_attribute_unused(), ...) { return 0; }
int errhint(const char *fmt pg_attribute_unused(), ...) { return 0; }
void errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
			   const char *func pg_attribute_unused()) { pg_re_throw(); }
bool superuser(void) { return allowed; }
void *palloc(Size size)
{
	void *p = calloc(1, size);
	if (!p || allocation_count == lengthof(allocations)) abort();
	allocations[allocation_count++] = p;
	return p;
}
void InitMaterializedSRF(FunctionCallInfo fcinfo, bits32 flags)
{
	ReturnSetInfo *rs = (ReturnSetInfo *)fcinfo->resultinfo;
	if (flags != 0) abort();
	rs->setDesc = out_desc;
	rs->setResult = (Tuplestorestate *)1;
}
Relation relation_open(Oid oid, LOCKMODE mode)
{
	if (oid != 16385 || mode != AccessShareLock) abort();
	return &relation;
}
void relation_close(Relation rel, LOCKMODE mode)
{
	if (rel != &relation || mode != AccessShareLock || locked || pinned) abort();
	closes++;
}
Buffer ReadBuffer(Relation rel, BlockNumber block)
{
	if (rel != &relation || block != 137 || pinned) abort();
	reads++; pinned = 1;
	return 7;
}
void LockBuffer(Buffer buffer, int mode)
{
	if (buffer != 7 || !pinned) abort();
	if (mode == BUFFER_LOCK_SHARE && !locked) locked = 1;
	else if (mode == BUFFER_LOCK_UNLOCK && locked) locked = 0;
	else abort();
}
void ReleaseBuffer(Buffer buffer)
{ if (buffer != 7 || locked || !pinned) abort(); pinned = 0; }
const char *cluster_cr_construct_block(Buffer buffer, SCN scn)
{
	if (buffer != 7 || !locked || scn != 4097) abort();
	constructed++;
	if (construct_error) { last_code = ERRCODE_CLUSTER_CR_CROSS_INSTANCE_UNSUPPORTED; pg_re_throw(); }
	return own_page.data;
}
ClusterCrBuildResult
cluster_gcs_block_cr_fetch_and_wait(BufferTag tag, SCN scn, char dst[BLCKSZ],
								   ClusterCrBuildReason *reason)
{
	if (locked || pinned || tag.spcOid != 1663 || tag.dbOid != 5
		|| tag.relNumber != 16385 || tag.forkNum != MAIN_FORKNUM
		|| tag.blockNum != 137 || scn != 4097) abort();
	fetched++;
	if (fetch_error) { last_code = ERRCODE_QUERY_CANCELED; pg_re_throw(); }
	/* Even a refusing boundary can have touched dst; it must not be consumed. */
	memcpy(dst, full_page.data, BLCKSZ);
	*reason = fetch_reason;
	return fetch_result;
}
void heap_deform_tuple(HeapTuple tuple, TupleDesc desc, Datum *values, bool *nulls)
{
	int32 value;
	if (desc != rel_desc || locked || pinned || tuple->t_tableOid != 16385
		|| ItemPointerGetBlockNumber(&tuple->t_self) != 137
		|| ItemPointerGetOffsetNumber(&tuple->t_self) != 3) abort();
	memcpy(&value, (char *)tuple->t_data + tuple->t_data->t_hoff, sizeof(value));
	values[0] = Int32GetDatum(value);
	values[1] = Int32GetDatum(42);
	nulls[0] = nulls[1] = false;
	deforms++;
}
void tuplestore_putvalues(Tuplestorestate *store, TupleDesc desc, Datum *values,
						 bool *nulls)
{
	if (store != (Tuplestorestate *)1 || desc != out_desc
		|| nulls[0] || nulls[1] || nulls[2]) abort();
	memcpy(output, values, sizeof(output));
	rows++;
}

Datum cluster_cr_test_image(PG_FUNCTION_ARGS);
Datum cluster_cr_test_target_image(PG_FUNCTION_ARGS);
Datum fixture_disabled_target(PG_FUNCTION_ARGS);

#include "test_cluster_cr_image_srf.inc"

static void page_fixture(char *page, int32 value)
{
	PageHeader h = (PageHeader)page;
	HeapTupleHeader tuple;
	memset(page, 0, BLCKSZ);
	h->pd_lower = SizeOfPageHeaderData + 3 * sizeof(ItemIdData);
	h->pd_special = BLCKSZ;
	h->pd_upper = BLCKSZ - MAXALIGN(SizeofHeapTupleHeader + sizeof(value));
	PageSetPageSizeAndVersion(page, BLCKSZ, PG_PAGE_LAYOUT_VERSION);
	ItemIdSetRedirect(PageGetItemId(page, 1), 3);
	ItemIdSetUnused(PageGetItemId(page, 2));
	ItemIdSetNormal(PageGetItemId(page, 3), h->pd_upper, SizeofHeapTupleHeader + sizeof(value));
	tuple = (HeapTupleHeader)(page + h->pd_upper);
	tuple->t_hoff = SizeofHeapTupleHeader;
	memcpy((char *)tuple + tuple->t_hoff, &value, sizeof(value));
}
static void reset_fixture(void)
{
	for (int i = 0; i < allocation_count; i++) free(allocations[i]);
	allocation_count = 0;
	memset(&relation, 0, sizeof(relation));
	memset(&rel_class, 0, sizeof(rel_class));
	rel_desc = palloc(offsetof(struct TupleDescData, attrs) + 2 * sizeof(FormData_pg_attribute));
	out_desc = palloc(offsetof(struct TupleDescData, attrs) + 3 * sizeof(FormData_pg_attribute));
	rel_desc->natts = 2; out_desc->natts = 3;
	TupleDescAttr(out_desc, 0)->atttypid = INT2OID;
	for (int i = 0; i < 2; i++) {
		TupleDescAttr(rel_desc, i)->atttypid = INT4OID;
		TupleDescAttr(rel_desc, i)->atttypmod = -1;
		*TupleDescAttr(out_desc, i + 1) = *TupleDescAttr(rel_desc, i);
	}
	rel_class.relkind = RELKIND_RELATION; rel_class.relam = HEAP_TABLE_AM_OID;
	relation.rd_rel = &rel_class; relation.rd_att = rel_desc;
	relation.rd_locator.spcOid = 1663; relation.rd_locator.dbOid = 5;
	relation.rd_locator.relNumber = 16385;
	page_fixture(full_page.data, 64); page_fixture(own_page.data, 99);
	fetched = constructed = reads = locked = pinned = closes = deforms = rows = last_code = 0;
	InterruptHoldoffCount = 0;
	allowed = true; fetch_error = construct_error = false;
	fetch_result = CLUSTER_CR_BUILD_FULL; fetch_reason = CLUSTER_CR_BUILD_NONE;
}
static bool run(bool target, bool disabled, int32 block, int64 scn)
{
	LOCAL_FCINFO(fcinfo, 3);
	ReturnSetInfo rs;
	volatile bool caught = false;
	memset(fcinfo, 0, SizeForFunctionCallInfo(3));
	memset(&rs, 0, sizeof(rs));
	fcinfo->resultinfo = (Node *)&rs;
	fcinfo->args[0].value = ObjectIdGetDatum(16385);
	fcinfo->args[1].value = Int32GetDatum(block);
	fcinfo->args[2].value = Int64GetDatum(scn);
	PG_TRY(); {
		if (disabled) (void)fixture_disabled_target(fcinfo);
		else if (target) (void)fixture_target(fcinfo);
		else (void)cluster_cr_test_image(fcinfo);
	}
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	return !caught;
}

UT_TEST(target_full_consumes_owned_page_without_local_buffer)
{
	const char kinds[] = {RELKIND_RELATION, RELKIND_MATVIEW};
	for (int i = 0; i < lengthof(kinds); i++) {
		reset_fixture(); rel_class.relkind = kinds[i];
		UT_ASSERT(run(true, false, 137, 4097));
		UT_ASSERT(fetched == 1 && constructed == 0 && reads == 0);
		UT_ASSERT(rows == 1 && deforms == 1 && closes == 1);
		UT_ASSERT(DatumGetInt16(output[0]) == 3 && DatumGetInt32(output[1]) == 64);
	}
}
UT_TEST(own_entry_preserves_share_copy_and_release)
{
	reset_fixture();
	UT_ASSERT(run(false, false, 137, 4097));
	UT_ASSERT(fetched == 0 && constructed == 1 && reads == 1);
	UT_ASSERT(rows == 1 && closes == 1 && !locked && !pinned);
	UT_ASSERT_EQ(DatumGetInt32(output[1]), 99);
}
UT_TEST(non_full_and_inconsistent_pairs_never_materialize)
{
	const struct { ClusterCrBuildResult result; ClusterCrBuildReason reason; } cases[] = {
		{CLUSTER_CR_BUILD_RETRYABLE, CLUSTER_CR_BUILD_TARGET_DISABLED},
		{CLUSTER_CR_BUILD_RETRYABLE, CLUSTER_CR_BUILD_RF_DEFERRED},
		{CLUSTER_CR_BUILD_FAIL_CLOSED, CLUSTER_CR_BUILD_BAD_UNDO},
		{CLUSTER_CR_BUILD_FAIL_CLOSED, CLUSTER_CR_BUILD_SNAPSHOT_TOO_OLD},
		{CLUSTER_CR_BUILD_FULL, CLUSTER_CR_BUILD_PROTOCOL},
		{CLUSTER_CR_BUILD_RETRYABLE, CLUSTER_CR_BUILD_NONE},
		{(ClusterCrBuildResult)99, CLUSTER_CR_BUILD_NONE},
		{CLUSTER_CR_BUILD_FULL, (ClusterCrBuildReason)99}
	};
	for (int i = 0; i < lengthof(cases); i++) {
		reset_fixture(); fetch_result = cases[i].result; fetch_reason = cases[i].reason;
		UT_ASSERT(!run(true, false, 137, 4097));
		UT_ASSERT_EQ(last_code, ERRCODE_CLUSTER_TT_STATUS_UNKNOWN);
		UT_ASSERT(fetched == 1 && reads == 0 && constructed == 0);
		UT_ASSERT(rows == 0 && deforms == 0 && closes == 1);
	}
}
UT_TEST(target_exception_propagates_without_fallback_or_retry)
{
	reset_fixture(); fetch_error = true;
	UT_ASSERT(!run(true, false, 137, 4097));
	UT_ASSERT_EQ(last_code, ERRCODE_QUERY_CANCELED);
	UT_ASSERT(fetched == 1 && reads == 0 && constructed == 0 && closes == 1);
	UT_ASSERT(rows == 0 && deforms == 0);
}
UT_TEST(own_error_releases_buffer_before_rethrow)
{
	reset_fixture(); construct_error = true;
	UT_ASSERT(!run(false, false, 137, 4097));
	UT_ASSERT_EQ(last_code, ERRCODE_CLUSTER_CR_CROSS_INSTANCE_UNSUPPORTED);
	UT_ASSERT(!locked && !pinned && closes == 1 && rows == 0);
}
UT_TEST(non_superuser_cannot_acquire_or_materialize)
{
	reset_fixture(); allowed = false;
	UT_ASSERT(!run(true, false, 137, 4097));
	UT_ASSERT_EQ(last_code, ERRCODE_INSUFFICIENT_PRIVILEGE);
	UT_ASSERT(fetched == 0 && reads == 0 && closes == 0 && rows == 0);
}
UT_TEST(noncluster_entry_is_unsupported)
{
	reset_fixture();
	UT_ASSERT(!run(true, true, 137, 4097));
	UT_ASSERT_EQ(last_code, ERRCODE_FEATURE_NOT_SUPPORTED);
	UT_ASSERT(fetched == 0 && reads == 0 && rows == 0);
}
UT_TEST(output_shape_error_precedes_target_acquisition)
{
	reset_fixture(); TupleDescAttr(out_desc, 0)->atttypid = INT4OID;
	UT_ASSERT(!run(true, false, 137, 4097));
	UT_ASSERT_EQ(last_code, ERRCODE_INVALID_PARAMETER_VALUE);
	UT_ASSERT(fetched == 0 && reads == 0 && closes == 1 && rows == 0);
}
UT_TEST(non_heap_relation_cannot_be_deformed_as_heap)
{
	reset_fixture(); rel_class.relkind = RELKIND_INDEX;
	UT_ASSERT(!run(true, false, 137, 4097));
	UT_ASSERT(fetched == 0 && reads == 0 && closes == 1 && rows == 0);
	reset_fixture(); rel_class.relam = InvalidOid;
	UT_ASSERT(!run(true, false, 137, 4097));
	UT_ASSERT(fetched == 0 && reads == 0 && closes == 1 && rows == 0);
}

int main(void)
{
	UT_PLAN(9);
	UT_RUN(target_full_consumes_owned_page_without_local_buffer);
	UT_RUN(own_entry_preserves_share_copy_and_release);
	UT_RUN(non_full_and_inconsistent_pairs_never_materialize);
	UT_RUN(target_exception_propagates_without_fallback_or_retry);
	UT_RUN(own_error_releases_buffer_before_rethrow);
	UT_RUN(non_superuser_cannot_acquire_or_materialize);
	UT_RUN(noncluster_entry_is_unsupported);
	UT_RUN(output_shape_error_precedes_target_acquisition);
	UT_RUN(non_heap_relation_cannot_be_deformed_as_heap);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
