/*-------------------------------------------------------------------------
 *
 * test_cluster_page_abi.c
 *    RF-PAGE PGDEL-10 — ABI-stability guards: the RF-PAGE layers must
 *    never modify the catalog/page/WAL/wire ABI until the stable-base
 *    STOP and the product plan are approved (spec §8.2-4).
 *
 *    The guarded sizes are the PGRAC HEAD baselines (PageHeaderData
 *    already carries the 8-byte pd_block_scn from stage 1.4; XLogRecord
 *    already carries the 8-byte xl_scn from spec-4.5).  These compile-
 *    time assertions pin the baseline so a future RF-PAGE change that
 *    accidentally resizes a page header, a WAL record header, a buffer
 *    tag or a relfilelocator fails the build instead of silently
 *    breaking the frozen ABI.
 *
 *    §8.2 compatibility disposition (documented, not code):
 *      1. exact-f076 lacks the PageVersion before/result producer — the
 *         existing replay behavior is NOT claimed compatible with the
 *         new contract (G3; the PAGE layers are judgement-only);
 *      2. a mixed-version peer that cannot produce/verify an exact
 *         proof fails closed (every PAGE validator fails on missing
 *         facts);
 *      3. no correctness authority is migrated from old private
 *         artifacts — the PAGE recovery set is in-memory and rebuilt
 *         from canonical inputs (D3-prime);
 *      4. NO catalog/page/WAL/wire ABI change ships with RF-PAGE — the
 *         guards below enforce the baseline;
 *      5. rollback never deletes retained origin redo nor relaxes the
 *         version/source/class gates (the §7.4 handoff denies removal);
 *      6. checksum on/off only changes the detector — the PAGE
 *         judgements take integrity as a declared fact and never read
 *         the checksum GUC (pure functions, no GUC dependency).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlogrecord.h"
#include "storage/buf_internals.h"
#include "storage/bufpage.h"
#include "storage/relfilelocator.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/* PGRAC HEAD ABI baselines — see the header comment.  A change to any of
 * these sizes breaks the frozen page/WAL/wire ABI and must fail here. */
StaticAssertDecl(sizeof(PageHeaderData) == 32,
				 "PageHeaderData ABI baseline (24-byte PG16 header + 8-byte pd_block_scn)");
StaticAssertDecl(sizeof(XLogRecord) == 32,
				 "XLogRecord ABI baseline (24-byte PG16 header + 8-byte xl_scn)");
StaticAssertDecl(sizeof(BufferTag) == 20, "BufferTag ABI baseline");
StaticAssertDecl(sizeof(RelFileLocator) == 12, "RelFileLocator ABI baseline");
StaticAssertDecl(offsetof(XLogRecord, xl_scn) == 16, "spec-4.5 xl_scn at offset 16");

UT_TEST(abi_baseline)
{
	/* The compile-time guards above are the real test: if this binary
	 * links, the ABI baseline holds. */
	UT_ASSERT(true);
}

int
main(void)
{
	UT_PLAN(1);
	UT_RUN(abi_baseline);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
