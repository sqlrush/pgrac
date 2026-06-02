/*-------------------------------------------------------------------------
 *
 * test_cluster_tt_durable.c
 *	  cluster_unit tests for spec-3.11 durable TT slot (D2).
 *
 *	  Covers the I/O-free decision logic + the lookup / by-xid scan logic with
 *	  a mocked cluster_undo_smgr (canned slot / block).  The real file-I/O
 *	  behavior (write -> read -> redo -> restart survival) is end-to-end in
 *	  cluster_tap t/219 (spec-3.11 §4; same pure-unit + e2e-IO split as
 *	  spec-3.9 / spec-3.10 CR -- undo segment I/O needs a real $PGDATA).
 *
 *	  U1  byte-layout (sizeof TTSlot==32, xl_undo_tt_slot_commit==24)
 *	  U2  cluster_tt_durable_redo_decide -- all 5 outcomes (wrap table §2.3)
 *	  U3  cluster_tt_durable_slot_match -- exact / wrong-xid / UNUSED /
 *	      invalid-scn (xid-match; recycle stamps a new owner xid)
 *	  U4  cluster_tt_slot_durable_lookup -- match / wrong-xid / unused /
 *	      read-fail (mocked smgr)
 *	  U5  cluster_tt_slot_durable_lookup_by_xid -- 0 / 1 / >1 matches (mocked
 *	      block); ambiguity fail-closed (规则 8.A)
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.11-durable-tt-slot.md (§4.1)
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_tt_durable.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "access/transam.h"
#include "storage/bufpage.h"

#include "cluster/cluster_scn.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/storage/cluster_undo_xlog.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();


/* ============================================================
 *	ereport / Assert stubs (cluster_tt_durable.c ereports on I/O fail)
 * ============================================================ */
static int last_ereport_errcode = 0;

bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return true;
}
bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}
void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	abort(); /* ERROR path not exercised by these tests */
}
int
errcode(int sqlerrcode)
{
	last_ereport_errcode = sqlerrcode;
	return 0;
}
int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}


/* ============================================================
 *	Mocked cluster_undo_smgr + emit + cluster_node_id
 * ============================================================ */
int cluster_node_id = 0;

static TTSlot g_canned_slot;	  /* returned by read_header_bytes */
static bool g_read_hdr_ok = true; /* read_header_bytes success flag */

static char g_canned_block[BLCKSZ];		  /* returned by read_block for segment 1 */
static uint32 g_canned_block_segment = 1; /* which segment_id the canned block answers */

XLogRecPtr
cluster_undo_emit_tt_slot_commit(uint8 instance pg_attribute_unused(),
								 uint32 segment_id pg_attribute_unused(),
								 uint16 slot_offset pg_attribute_unused(),
								 uint16 wrap pg_attribute_unused(),
								 TransactionId xid pg_attribute_unused(),
								 SCN commit_scn pg_attribute_unused())
{
	return InvalidXLogRecPtr;
}

bool
cluster_undo_smgr_read_header_bytes(uint32 segment_id pg_attribute_unused(),
									uint8 owner_instance pg_attribute_unused(), uint32 offset,
									char *buf, uint32 len)
{
	if (!g_read_hdr_ok || buf == NULL || len != sizeof(TTSlot))
		return false;
	(void)offset;
	memcpy(buf, &g_canned_slot, sizeof(TTSlot));
	return true;
}

bool
cluster_undo_smgr_write_header_bytes(uint32 segment_id pg_attribute_unused(),
									 uint8 owner_instance pg_attribute_unused(),
									 uint32 offset pg_attribute_unused(), const char *buf,
									 uint32 len)
{
	return buf != NULL && len == sizeof(TTSlot);
}

bool
cluster_undo_smgr_read_block(uint32 segment_id, uint8 owner_instance pg_attribute_unused(),
							 uint32 block_no, char *buf)
{
	if (buf == NULL || block_no != 0 || segment_id != g_canned_block_segment)
		return false; /* other segments "don't exist" -> by-xid skips them */
	memcpy(buf, g_canned_block, BLCKSZ);
	return true;
}


/* helper: seed g_canned_block's TT slot i with (status, xid, commit_scn). */
static void
seed_block_slot(int i, uint8 status, TransactionId xid, SCN commit_scn)
{
	UndoSegmentHeaderData *hdr = (UndoSegmentHeaderData *)g_canned_block;

	hdr->tt_slots[i].status = status;
	hdr->tt_slots[i].xid = xid;
	hdr->tt_slots[i].commit_scn = commit_scn;
}


/* ============================================================
 *	U1: byte layout
 * ============================================================ */
UT_TEST(test_layout_sizes)
{
	UT_ASSERT_EQ((int)sizeof(TTSlot), 32);
	UT_ASSERT_EQ((int)sizeof(xl_undo_tt_slot_commit), 24);
}


/* ============================================================
 *	U2: redo wrap-comparison table (§2.3)
 * ============================================================ */
UT_TEST(test_redo_decide_newer_wrap_applies)
{
	/* rec.wrap > slot.wrap -> APPLY (recycle-then-commit normal path). */
	UT_ASSERT_EQ((int)cluster_tt_durable_redo_decide(TT_SLOT_COMMITTED, 100, 5, 200, 6),
				 (int)CLUSTER_TT_REDO_APPLY);
}
UT_TEST(test_redo_decide_same_wrap_same_xid_idempotent)
{
	UT_ASSERT_EQ((int)cluster_tt_durable_redo_decide(TT_SLOT_COMMITTED, 100, 5, 100, 5),
				 (int)CLUSTER_TT_REDO_APPLY);
}
UT_TEST(test_redo_decide_same_wrap_diff_xid_corrupt)
{
	UT_ASSERT_EQ((int)cluster_tt_durable_redo_decide(TT_SLOT_COMMITTED, 100, 5, 999, 5),
				 (int)CLUSTER_TT_REDO_CORRUPT);
}
UT_TEST(test_redo_decide_older_wrap_skips)
{
	UT_ASSERT_EQ((int)cluster_tt_durable_redo_decide(TT_SLOT_COMMITTED, 100, 6, 100, 5),
				 (int)CLUSTER_TT_REDO_SKIP);
}
UT_TEST(test_redo_decide_bad_status)
{
	/* status > TT_SLOT_RECYCLABLE (4) -> BADSTATUS even if wrap would apply. */
	UT_ASSERT_EQ((int)cluster_tt_durable_redo_decide(99, 100, 5, 200, 6),
				 (int)CLUSTER_TT_REDO_BADSTATUS);
}


/* ============================================================
 *	U3: slot_match predicate (C5)
 * ============================================================ */
UT_TEST(test_slot_match_exact)
{
	UT_ASSERT_EQ((int)cluster_tt_durable_slot_match(TT_SLOT_COMMITTED, 100, scn_encode(1, 42), 100),
				 1);
}
UT_TEST(test_slot_match_wrong_xid)
{
	/* recycle stamps a new owner xid -> xid mismatch is the recycle detector. */
	UT_ASSERT_EQ((int)cluster_tt_durable_slot_match(TT_SLOT_COMMITTED, 999, scn_encode(1, 42), 100),
				 0);
}
UT_TEST(test_slot_match_unused)
{
	UT_ASSERT_EQ((int)cluster_tt_durable_slot_match(TT_SLOT_UNUSED, 100, scn_encode(1, 42), 100),
				 0);
}
UT_TEST(test_slot_match_invalid_scn)
{
	UT_ASSERT_EQ((int)cluster_tt_durable_slot_match(TT_SLOT_COMMITTED, 100, InvalidScn, 100), 0);
}


/* ============================================================
 *	U4: cluster_tt_slot_durable_lookup (mocked smgr)
 * ============================================================ */
UT_TEST(test_lookup_match)
{
	SCN got = InvalidScn;

	g_read_hdr_ok = true;
	memset(&g_canned_slot, 0, sizeof(g_canned_slot));
	g_canned_slot.status = TT_SLOT_COMMITTED;
	g_canned_slot.xid = 100;
	g_canned_slot.wrap = 5;
	g_canned_slot.commit_scn = scn_encode(1, 42);

	UT_ASSERT_EQ((int)cluster_tt_slot_durable_lookup(1, 0, 100, &got), 1);
	UT_ASSERT_EQ((int)(scn_local(got)), 42);
}
UT_TEST(test_lookup_wrong_xid_miss)
{
	SCN got = InvalidScn;

	g_read_hdr_ok = true;
	memset(&g_canned_slot, 0, sizeof(g_canned_slot));
	g_canned_slot.status = TT_SLOT_COMMITTED;
	g_canned_slot.xid = 999; /* slot recycled to a new owner xid */
	g_canned_slot.commit_scn = scn_encode(1, 42);

	UT_ASSERT_EQ((int)cluster_tt_slot_durable_lookup(1, 0, 100, &got), 0);
}
UT_TEST(test_lookup_unused_miss)
{
	SCN got = InvalidScn;

	g_read_hdr_ok = true;
	memset(&g_canned_slot, 0, sizeof(g_canned_slot)); /* status=UNUSED(0) */

	UT_ASSERT_EQ((int)cluster_tt_slot_durable_lookup(1, 0, 100, &got), 0);
}
UT_TEST(test_lookup_read_fail_miss)
{
	SCN got = InvalidScn;

	g_read_hdr_ok = false; /* segment absent / I/O error */
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_lookup(1, 0, 100, &got), 0);
}


/* ============================================================
 *	U5: cluster_tt_slot_durable_lookup_by_xid (mocked block)
 * ============================================================ */
UT_TEST(test_by_xid_zero_match_miss)
{
	SCN got = InvalidScn;

	cluster_node_id = 0;
	memset(g_canned_block, 0, sizeof(g_canned_block)); /* all UNUSED */
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_lookup_by_xid(12345, &got), 0);
}
UT_TEST(test_by_xid_one_match)
{
	SCN got = InvalidScn;

	cluster_node_id = 0;
	memset(g_canned_block, 0, sizeof(g_canned_block));
	seed_block_slot(3, TT_SLOT_COMMITTED, 12345, scn_encode(1, 77));
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_lookup_by_xid(12345, &got), 1);
	UT_ASSERT_EQ((int)(scn_local(got)), 77);
}
UT_TEST(test_by_xid_two_match_ambiguous_failclosed)
{
	SCN got = InvalidScn;

	cluster_node_id = 0;
	memset(g_canned_block, 0, sizeof(g_canned_block));
	seed_block_slot(3, TT_SLOT_COMMITTED, 12345, scn_encode(1, 77));
	seed_block_slot(9, TT_SLOT_COMMITTED, 12345, scn_encode(1, 88)); /* duplicate xid */
	/* 规则 8.A: >1 match -> fail-closed (never first-match). */
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_lookup_by_xid(12345, &got), 0);
}


int
main(int argc, char **argv)
{
	UT_PLAN(17);

	UT_RUN(test_layout_sizes);

	UT_RUN(test_redo_decide_newer_wrap_applies);
	UT_RUN(test_redo_decide_same_wrap_same_xid_idempotent);
	UT_RUN(test_redo_decide_same_wrap_diff_xid_corrupt);
	UT_RUN(test_redo_decide_older_wrap_skips);
	UT_RUN(test_redo_decide_bad_status);

	UT_RUN(test_slot_match_exact);
	UT_RUN(test_slot_match_wrong_xid);
	UT_RUN(test_slot_match_unused);
	UT_RUN(test_slot_match_invalid_scn);

	UT_RUN(test_lookup_match);
	UT_RUN(test_lookup_wrong_xid_miss);
	UT_RUN(test_lookup_unused_miss);
	UT_RUN(test_lookup_read_fail_miss);

	UT_RUN(test_by_xid_zero_match_miss);
	UT_RUN(test_by_xid_one_match);
	UT_RUN(test_by_xid_two_match_ambiguous_failclosed);

	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
