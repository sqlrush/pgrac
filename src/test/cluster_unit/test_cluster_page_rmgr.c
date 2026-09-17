/*-------------------------------------------------------------------------
 *
 * test_cluster_page_rmgr.c
 *    RF-PAGE PGDEL-02 focused unit tests: the rmgr census (closed-table
 *    row assignment + byte-for-byte differential evidence state), the
 *    census -> classifier wiring, and cluster_page_redo_decode (identity
 *    + §3.1 hints extraction; facts only, never a version judgement).
 *
 *    RED mapping (spec §10.1 + §4.1/§4.2 + G1/G9):
 *      - unknown rmgr/opcode -> no census row -> decode false (BLOCKED);
 *      - rows WITHOUT differential evidence never register into the
 *        classifier known-set -> the record classifies UNKNOWN (BLOCKED);
 *      - rows WITH t/256 evidence (heap INSERT/DELETE/UPDATE/HOT_UPDATE,
 *        generic) register -> classify NORMAL;
 *      - heap INIT_PAGE row carries the will_init ATTRIBUTE but no §4.6
 *        full-init rule -> classifier still returns UNKNOWN (PU-17);
 *      - SLRU/relmap/undo rows are HEADER-class (typed owner) and never
 *        known_delta: generic page replay must not touch them (§4.5);
 *      - decoder_registered is false for every row (G3: the §3.4
 *        deterministic-mutation declaration stays RED).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam_xlog.h" /* XLOG_HEAP_* opcodes */
#include "access/rmgr.h"
#include "access/xlogrecord.h"
#include "cluster/cluster_page_rmgr.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

#include <stdio.h>

/* ----------
 * Record fabrication (mirrors test_cluster_block_apply.c): a minimal
 * XLogReaderState wrapping a DecodedXLogRecord with one block reference.
 * ----------
 */
typedef struct FakeRecord {
	XLogReaderState st;
	union {
		DecodedXLogRecord dec;
		/* cppcheck-suppress unusedStructMember */
		char pad[sizeof(DecodedXLogRecord) + 2 * sizeof(DecodedBkpBlock)];
	} u;
} FakeRecord;

static XLogReaderState *
make_record(FakeRecord *fr, RmgrId rmid, uint8 info, uint64 xl_scn, bool has_ref, bool has_image,
			bool apply_image, uint8 fork_flags, XLogRecPtr endlsn)
{
	DecodedXLogRecord *dec = &fr->u.dec;

	memset(fr, 0, sizeof(*fr));
	dec->header.xl_rmid = rmid;
	dec->header.xl_info = info;
	dec->header.xl_scn = xl_scn;
	dec->max_block_id = has_ref ? 0 : -1;
	if (has_ref) {
		dec->blocks[0].in_use = true;
		dec->blocks[0].rlocator.spcOid = 1;
		dec->blocks[0].rlocator.dbOid = 2;
		dec->blocks[0].rlocator.relNumber = 3;
		dec->blocks[0].forknum = MAIN_FORKNUM;
		dec->blocks[0].blkno = 42;
		dec->blocks[0].flags = fork_flags;
		dec->blocks[0].has_image = has_image;
		dec->blocks[0].apply_image = apply_image;
	}
	fr->st.record = dec;
	fr->st.EndRecPtr = endlsn;
	return &fr->st;
}

/* ==========================================================================
 * A. census: closed-table rows
 * ========================================================================== */

UT_TEST(test_census_heap_delta_rows)
{
	ClusterPageRmgrCensusEntry e;
	int opcodes[] = { XLOG_HEAP_INSERT, XLOG_HEAP_DELETE, XLOG_HEAP_UPDATE, XLOG_HEAP_HOT_UPDATE };
	int i;

	/* The t/256-differenced heap deltas: NORMAL, page-affecting, known. */
	for (i = 0; i < (int)lengthof(opcodes); i++) {
		UT_ASSERT(cluster_page_rmgr_census_lookup(RM_HEAP_ID, (uint16)opcodes[i], &e));
		UT_ASSERT_EQ((int)e.page_class, (int)CLUSTER_PAGE_CLASS_NORMAL);
		UT_ASSERT(e.affects_page);
		UT_ASSERT(e.known_delta);
		/* G3: no deterministic-mutation declaration exists yet. */
		UT_ASSERT(!e.decoder_registered);
	}
}

UT_TEST(test_census_heap_unsupported_deltas_not_known)
{
	ClusterPageRmgrCensusEntry e;

	/* LOCK/CONFIRM/INPLACE are page-affecting NORMAL rows WITHOUT
	 * differential evidence (the matrix fails them closed, 8.A/R11). */
	UT_ASSERT(cluster_page_rmgr_census_lookup(RM_HEAP_ID, XLOG_HEAP_LOCK, &e));
	UT_ASSERT_EQ((int)e.page_class, (int)CLUSTER_PAGE_CLASS_NORMAL);
	UT_ASSERT(e.affects_page);
	UT_ASSERT(!e.known_delta);

	/* The whole RM_HEAP2 family is page-affecting but unproven. */
	UT_ASSERT(cluster_page_rmgr_census_lookup(RM_HEAP2_ID, 0, &e));
	UT_ASSERT(e.affects_page);
	UT_ASSERT(!e.known_delta);

	/* Index data pages: NORMAL row, no evidence yet. */
	UT_ASSERT(cluster_page_rmgr_census_lookup(RM_BTREE_ID, 0, &e));
	UT_ASSERT_EQ((int)e.page_class, (int)CLUSTER_PAGE_CLASS_NORMAL);
	UT_ASSERT(e.affects_page);
	UT_ASSERT(!e.known_delta);
}

UT_TEST(test_census_header_class_typed_owner)
{
	ClusterPageRmgrCensusEntry e;

	/* SLRU/relmap pages are HEADER-class with a typed owner: generic page
	 * replay must never touch them (§4.5), and there is no differential
	 * evidence. */
	UT_ASSERT(cluster_page_rmgr_census_lookup(RM_CLOG_ID, 0, &e));
	UT_ASSERT_EQ((int)e.page_class, (int)CLUSTER_PAGE_CLASS_HEADER);
	UT_ASSERT(e.affects_page);
	UT_ASSERT(!e.known_delta);
	UT_ASSERT(cluster_page_rmgr_census_lookup(RM_RELMAP_ID, 0, &e));
	UT_ASSERT_EQ((int)e.page_class, (int)CLUSTER_PAGE_CLASS_HEADER);
}

UT_TEST(test_census_non_page_rmgrs)
{
	ClusterPageRmgrCensusEntry e;

	/* Control/transaction/file-level records are not page-affecting. */
	UT_ASSERT(cluster_page_rmgr_census_lookup(RM_XLOG_ID, 0, &e));
	UT_ASSERT(!e.affects_page);
	UT_ASSERT(cluster_page_rmgr_census_lookup(RM_XACT_ID, 0, &e));
	UT_ASSERT(!e.affects_page);
	UT_ASSERT(cluster_page_rmgr_census_lookup(RM_SMGR_ID, 0, &e));
	UT_ASSERT(!e.affects_page);
	UT_ASSERT(cluster_page_rmgr_census_lookup(RM_STANDBY_ID, 0, &e));
	UT_ASSERT(!e.affects_page);
}

UT_TEST(test_census_unknown_rmgr_no_row)
{
	ClusterPageRmgrCensusEntry e;

	/* Unknown rmgr ID: no census row -> fail closed. */
	UT_ASSERT(!cluster_page_rmgr_census_lookup(0xFE, 0x10, &e));
	UT_ASSERT(!cluster_page_rmgr_census_lookup(0xFE, 0, NULL));
}

UT_TEST(test_census_heap_init_page_attribute_row)
{
	ClusterPageRmgrCensusEntry e;

	/* INIT_PAGE: NEW-class row carrying the will_init ATTRIBUTE — but no
	 * §4.6 full-init rule (expected class state + result-version proof),
	 * so the classifier must still return UNKNOWN for it (PU-17). */
	UT_ASSERT(cluster_page_rmgr_census_lookup(RM_HEAP_ID, XLOG_HEAP_INIT_PAGE, &e));
	UT_ASSERT_EQ((int)e.page_class, (int)CLUSTER_PAGE_CLASS_NEW);
	UT_ASSERT(e.will_init);
	UT_ASSERT(!e.known_delta);
}

/* ==========================================================================
 * B. census -> classifier wiring
 * ========================================================================== */

UT_TEST(test_populate_known_set_wires_only_proven_deltas)
{
	ClusterPageClassifyInput in;

	/* Fresh process: the classifier registry starts empty. */
	UT_ASSERT(!cluster_page_class_is_known_opcode(RM_HEAP_ID, XLOG_HEAP_INSERT));
	UT_ASSERT(!cluster_page_class_is_known_opcode(RM_HEAP_ID, XLOG_HEAP_LOCK));

	cluster_page_rmgr_populate_known_set();

	/* Proven deltas register -> NORMAL. */
	UT_ASSERT(cluster_page_class_is_known_opcode(RM_HEAP_ID, XLOG_HEAP_INSERT));
	UT_ASSERT(cluster_page_class_is_known_opcode(RM_HEAP_ID, XLOG_HEAP_UPDATE));
	UT_ASSERT(cluster_page_class_is_known_opcode(RM_GENERIC_ID, 0));
	/* Unproven opcodes do NOT register. */
	UT_ASSERT(!cluster_page_class_is_known_opcode(RM_HEAP_ID, XLOG_HEAP_LOCK));
	UT_ASSERT(!cluster_page_class_is_known_opcode(RM_HEAP_ID, XLOG_HEAP_INIT_PAGE));
	UT_ASSERT(!cluster_page_class_is_known_opcode(RM_HEAP2_ID, 0));
	UT_ASSERT(!cluster_page_class_is_known_opcode(RM_BTREE_ID, 0));

	memset(&in, 0, sizeof(in));
	in.rmid = RM_HEAP_ID;
	in.opcode = XLOG_HEAP_INSERT;
	in.forknum = MAIN_FORKNUM;
	in.header_owner = CLUSTER_PAGE_HEADER_OWNER_NONE;
	UT_ASSERT_EQ((int)cluster_page_classify(&in), (int)CLUSTER_PAGE_CLASS_NORMAL);
	/* Lock stays UNKNOWN (BLOCKED) until differential evidence lands. */
	in.opcode = XLOG_HEAP_LOCK;
	UT_ASSERT_EQ((int)cluster_page_classify(&in), (int)CLUSTER_PAGE_CLASS_UNKNOWN);
	/* INIT_PAGE keeps the WILL_INIT attribute: UNKNOWN without the
	 * full-init rule (PU-17). */
	in.opcode = XLOG_HEAP_INIT_PAGE;
	UT_ASSERT_EQ((int)cluster_page_classify(&in), (int)CLUSTER_PAGE_CLASS_UNKNOWN);
}

/* ==========================================================================
 * C. cluster_page_redo_decode — facts only
 * ========================================================================== */

UT_TEST(test_decode_heap_insert_extracts_facts)
{
	FakeRecord fr;
	XLogReaderState *rec;
	ClusterPageRedoDecoded out;

	rec = make_record(&fr, RM_HEAP_ID, XLOG_HEAP_INSERT, UINT64_C(0x123456789), true, false, false,
					  0, UINT64_C(0x1000));
	UT_ASSERT(cluster_page_redo_decode(rec, 0, &out));
	UT_ASSERT_EQ((int)out.page_class, (int)CLUSTER_PAGE_CLASS_NORMAL);
	UT_ASSERT_EQ((unsigned long long)out.identity.rlocator.spcOid, 1ULL);
	UT_ASSERT_EQ((unsigned long long)out.identity.blocknum, 42ULL);
	UT_ASSERT_EQ((int)out.identity.forknum, (int)MAIN_FORKNUM);
	/* §3.1 hints: record-side facts only. */
	UT_ASSERT_EQ((unsigned long long)out.hints.record_scn,
				 (unsigned long long)UINT64_C(0x123456789));
	UT_ASSERT_EQ((unsigned long long)out.hints.record_lsn, (unsigned long long)UINT64_C(0x1000));
	UT_ASSERT(!out.full_image);
	UT_ASSERT(!out.will_init);
	/* Page-side hints are the caller's to fill; they start zero. */
	UT_ASSERT_EQ((unsigned long long)out.hints.page_scn, 0ULL);
	UT_ASSERT_EQ((unsigned long long)out.hints.page_lsn, 0ULL);
}

UT_TEST(test_decode_fpi_and_will_init_attributes)
{
	FakeRecord fr;
	XLogReaderState *rec;
	ClusterPageRedoDecoded out;

	/* FPI present and applied. */
	rec = make_record(&fr, RM_HEAP_ID, XLOG_HEAP_INSERT, 5, true, true, true, 0, 0x2000);
	UT_ASSERT(cluster_page_redo_decode(rec, 0, &out));
	UT_ASSERT(out.full_image);

	/* BKPBLOCK_WILL_INIT attribute rides the block fork flags. */
	rec = make_record(&fr, RM_HEAP_ID, XLOG_HEAP_INIT_PAGE, 6, true, false, false,
					  BKPBLOCK_WILL_INIT, 0x3000);
	UT_ASSERT(cluster_page_redo_decode(rec, 0, &out));
	UT_ASSERT(out.will_init);
	UT_ASSERT_EQ((int)out.page_class, (int)CLUSTER_PAGE_CLASS_NEW);
}

UT_TEST(test_decode_fail_closed_paths)
{
	FakeRecord fr;
	XLogReaderState *rec;
	ClusterPageRedoDecoded out;

	/* No block reference: nothing to decode. */
	rec = make_record(&fr, RM_HEAP_ID, XLOG_HEAP_INSERT, 1, false, false, false, 0, 0x1000);
	UT_ASSERT(!cluster_page_redo_decode(rec, 0, &out));

	/* Non-page-affecting rmgr (SMGR): census row says not page-affecting. */
	rec = make_record(&fr, RM_SMGR_ID, 0, 1, true, false, false, 0, 0x1000);
	UT_ASSERT(!cluster_page_redo_decode(rec, 0, &out));

	/* Unknown rmgr: no census row. */
	rec = make_record(&fr, 0xFE, 0x10, 1, true, false, false, 0, 0x1000);
	UT_ASSERT(!cluster_page_redo_decode(rec, 0, &out));

	/* NULL inputs. */
	UT_ASSERT(!cluster_page_redo_decode(NULL, 0, &out));
}

int
main(void)
{
	UT_PLAN(10);

	UT_RUN(test_census_heap_delta_rows);
	UT_RUN(test_census_heap_unsupported_deltas_not_known);
	UT_RUN(test_census_header_class_typed_owner);
	UT_RUN(test_census_non_page_rmgrs);
	UT_RUN(test_census_unknown_rmgr_no_row);
	UT_RUN(test_census_heap_init_page_attribute_row);
	UT_RUN(test_populate_known_set_wires_only_proven_deltas);
	UT_RUN(test_decode_heap_insert_extracts_facts);
	UT_RUN(test_decode_fpi_and_will_init_attributes);
	UT_RUN(test_decode_fail_closed_paths);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
