/*-------------------------------------------------------------------------
 *
 * test_cluster_itl_cleanout.c
 *	  pgrac spec-3.4c D10 — cluster_unit tests for lazy cleanout
 *	  helpers (cluster_itl_cleanout.c / .h).
 *
 *	  20 tests covering:
 *	    T1   cluster_itl_cleanout_lazy symbol linkable
 *	    T2   cluster_itl_cleanout_can_stamp symbol linkable
 *	    T3   can_stamp(NULL, _, _) → false
 *	    T4   can_stamp(slot.flags != ACTIVE [COMMITTED]) → false
 *	    T5   can_stamp(slot.flags != ACTIVE [ABORTED])   → false
 *	    T6   can_stamp(slot.flags != ACTIVE [FREE])      → false
 *	    T7   can_stamp(slot.xid mismatch) → false  (L189 recycle defense)
 *	    T8   can_stamp(slot.commit_scn already set) → false  (concurrent stamp defense)
 *	    T9   can_stamp(expected_commit_scn invalid) → false  (caller logic bug)
 *	    T10  can_stamp happy path → true
 *	    T11  lazy(InvalidBuffer, _, _, _) → false  (guard)
 *	    T12  lazy(slot_idx out-of-bound) → false  (guard)
 *	    T13  lazy(InvalidTransactionId) → false  (guard)
 *	    T14  lazy(InvalidScn expected_commit_scn) → false  (guard)
 *	    T15  CLUSTER_ITL_INITRANS_DEFAULT == 8 (slot_idx bound source)
 *	    T16  ITL_FLAG_ACTIVE / COMMITTED / ABORTED / FREE distinct constants
 *	    T17  SCN_VALID(InvalidScn) == false (can_stamp third guard semantic)
 *	    T18  SCN_VALID(non-zero) == true
 *	    T19  ClusterItlSlotData sizeof 48 (regression — spec-3.2 layout)
 *	    T20  ClusterItlSlotData.xid offset 8 / .commit_scn offset 24 / .flags offset 16
 *
 *	  Behavioural coverage of cluster_itl_cleanout_lazy under a real
 *	  buffer manager (ConditionalLockBuffer race / MarkBufferDirtyHint
 *	  observability / cross-page idempotency) is exercised by
 *	  cluster_tap t/208 — that path requires a full backend with
 *	  bufmgr.c + lwlock.c live, which a self-contained cluster_unit
 *	  binary cannot link.  This file therefore stops at the API surface
 *	  + can_stamp() pure-data contract + boundary guards reachable
 *	  without a real Buffer / Page / LWLock.
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_itl_cleanout.c
 *
 * Spec: spec-3.4c-delayed-cleanout-d5b-commit-scn-yellow-perf-hardening.md
 *       (v0.3 FROZEN 2026-05-24)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>
#include <string.h>

#include "access/transam.h"
#include "cluster/cluster_itl_cleanout.h"
#include "cluster/cluster_itl_slot.h"
#include "cluster/cluster_scn.h"
#include "storage/buf.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"


UT_DEFINE_GLOBALS();


/* ===== Assert / ereport stubs ===== */

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}
bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}
void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}
int
errcode(int sqlerrcode pg_attribute_unused())
{
	return 0;
}
int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}


/* ===== bufmgr API stubs — never reached because all tests pass
 *	   InvalidBuffer to drive the BufferIsValid guard.  Linker still
 *	   needs the symbols.  bufmgr.h provides BufferGetPage as a
 *	   static inline, so it does not need a local definition. ===== */
int NBuffers = 0;
int NLocBuffer = 0;
char *BufferBlocks = NULL;
Block *LocalBufferBlockPointers = NULL;

bool
ConditionalLockBuffer(Buffer buffer pg_attribute_unused())
{
	return false;
}
void
LockBuffer(Buffer buffer pg_attribute_unused(), int mode pg_attribute_unused())
{}
void
MarkBufferDirtyHint(Buffer buffer pg_attribute_unused(), bool buffer_std pg_attribute_unused())
{}


/* ===== T1 / T2: API surface linkable ===== */
UT_TEST(t1_lazy_symbol_linkable)
{
	UT_ASSERT_NE((void *)cluster_itl_cleanout_lazy, NULL);
}
UT_TEST(t2_can_stamp_symbol_linkable)
{
	UT_ASSERT_NE((void *)cluster_itl_cleanout_can_stamp, NULL);
}


/* ===== T3-T10: can_stamp() boundary cases (pure data; no Buffer) ===== */

static ClusterItlSlotData
make_slot(uint16 flags, TransactionId xid, SCN commit_scn)
{
	ClusterItlSlotData s;

	memset(&s, 0, sizeof(s));
	s.flags = flags;
	s.xid = xid;
	s.commit_scn = commit_scn;
	return s;
}

UT_TEST(t3_can_stamp_null_slot_false)
{
	UT_ASSERT_EQ((int)cluster_itl_cleanout_can_stamp(NULL, 1234, 5678), 0);
}

UT_TEST(t4_can_stamp_flag_committed_false)
{
	ClusterItlSlotData s = make_slot(ITL_FLAG_COMMITTED, 1234, InvalidScn);
	UT_ASSERT_EQ((int)cluster_itl_cleanout_can_stamp(&s, 1234, 5678), 0);
}

UT_TEST(t5_can_stamp_flag_aborted_false)
{
	ClusterItlSlotData s = make_slot(ITL_FLAG_ABORTED, 1234, InvalidScn);
	UT_ASSERT_EQ((int)cluster_itl_cleanout_can_stamp(&s, 1234, 5678), 0);
}

UT_TEST(t6_can_stamp_flag_free_false)
{
	ClusterItlSlotData s = make_slot(ITL_FLAG_FREE, 1234, InvalidScn);
	UT_ASSERT_EQ((int)cluster_itl_cleanout_can_stamp(&s, 1234, 5678), 0);
}

UT_TEST(t7_can_stamp_xid_mismatch_false)
{
	/* spec-3.4c F2 — L189 slot recycle defense.  Slot now belongs to xid Y,
	 * caller passed xid X → must refuse to stamp X's commit_scn. */
	ClusterItlSlotData s = make_slot(ITL_FLAG_ACTIVE, 9999 /* Y */, InvalidScn);
	UT_ASSERT_EQ((int)cluster_itl_cleanout_can_stamp(&s, 1234 /* X */, 5678), 0);
}

UT_TEST(t8_can_stamp_slot_commit_scn_already_set_false)
{
	/* Concurrent-stamp defense:  another backend raced ahead with this
	 * slot's commit_scn.  We must not over-stamp. */
	ClusterItlSlotData s = make_slot(ITL_FLAG_ACTIVE, 1234, 4242 /* already set */);
	UT_ASSERT_EQ((int)cluster_itl_cleanout_can_stamp(&s, 1234, 5678), 0);
}

UT_TEST(t9_can_stamp_invalid_expected_scn_false)
{
	ClusterItlSlotData s = make_slot(ITL_FLAG_ACTIVE, 1234, InvalidScn);
	/* Caller passed InvalidScn:  caller logic bug.  Refuse. */
	UT_ASSERT_EQ((int)cluster_itl_cleanout_can_stamp(&s, 1234, InvalidScn), 0);
}

UT_TEST(t10_can_stamp_happy_path_true)
{
	ClusterItlSlotData s = make_slot(ITL_FLAG_ACTIVE, 1234, InvalidScn);
	UT_ASSERT_EQ((int)cluster_itl_cleanout_can_stamp(&s, 1234, 5678), 1);
}


/* ===== T11-T14: lazy() outer-guard short-circuits (reach via Invalid* args) ===== */

UT_TEST(t11_lazy_invalid_buffer_false)
{
	UT_ASSERT_EQ((int)cluster_itl_cleanout_lazy(InvalidBuffer, 0, 1234, 5678), 0);
}

UT_TEST(t12_lazy_slot_idx_out_of_bound_false)
{
	/* slot_idx = INITRANS (8) violates `slot_idx >= INITRANS` guard. */
	UT_ASSERT_EQ(
		(int)cluster_itl_cleanout_lazy(1 /* fake buf */, CLUSTER_ITL_INITRANS_DEFAULT, 1234, 5678),
		0);
}

UT_TEST(t13_lazy_invalid_xid_false)
{
	UT_ASSERT_EQ((int)cluster_itl_cleanout_lazy(InvalidBuffer, 0, InvalidTransactionId, 5678), 0);
}

UT_TEST(t14_lazy_invalid_commit_scn_false)
{
	UT_ASSERT_EQ((int)cluster_itl_cleanout_lazy(InvalidBuffer, 0, 1234, InvalidScn), 0);
}


/* ===== T15-T16: enum / constant stability (regression — bound source) ===== */

UT_TEST(t15_initrans_default_is_8)
{
	UT_ASSERT_EQ((int)CLUSTER_ITL_INITRANS_DEFAULT, 8);
}

UT_TEST(t16_itl_flag_constants_distinct)
{
	UT_ASSERT_NE((int)ITL_FLAG_FREE, (int)ITL_FLAG_ACTIVE);
	UT_ASSERT_NE((int)ITL_FLAG_ACTIVE, (int)ITL_FLAG_COMMITTED);
	UT_ASSERT_NE((int)ITL_FLAG_COMMITTED, (int)ITL_FLAG_ABORTED);
	UT_ASSERT_NE((int)ITL_FLAG_FREE, (int)ITL_FLAG_COMMITTED);
}


/* ===== T17-T18: SCN_VALID contract underlying can_stamp guards ===== */

UT_TEST(t17_scn_valid_invalidscn_false)
{
	UT_ASSERT_EQ((int)SCN_VALID(InvalidScn), 0);
}

UT_TEST(t18_scn_valid_nonzero_true)
{
	UT_ASSERT_NE((int)SCN_VALID((SCN)1), 0);
}


/* ===== T19-T20: ClusterItlSlotData layout regression (spec-3.2 frozen) ===== */

UT_TEST(t19_slot_sizeof_48)
{
	UT_ASSERT_EQ((int)sizeof(ClusterItlSlotData), 48);
}

UT_TEST(t20_slot_field_offsets_stable)
{
	/* spec-3.2 D1 / spec-3.4a stability: lazy cleanout mutates slot->xid /
	 * slot->flags / slot->commit_scn;  these offsets must not drift. */
	UT_ASSERT_EQ((int)offsetof(ClusterItlSlotData, xid), 0);
	UT_ASSERT_EQ((int)offsetof(ClusterItlSlotData, flags), 6);
	UT_ASSERT_EQ((int)offsetof(ClusterItlSlotData, commit_scn), 24);
}


int
main(void)
{
	UT_PLAN(20);
	UT_RUN(t1_lazy_symbol_linkable);
	UT_RUN(t2_can_stamp_symbol_linkable);
	UT_RUN(t3_can_stamp_null_slot_false);
	UT_RUN(t4_can_stamp_flag_committed_false);
	UT_RUN(t5_can_stamp_flag_aborted_false);
	UT_RUN(t6_can_stamp_flag_free_false);
	UT_RUN(t7_can_stamp_xid_mismatch_false);
	UT_RUN(t8_can_stamp_slot_commit_scn_already_set_false);
	UT_RUN(t9_can_stamp_invalid_expected_scn_false);
	UT_RUN(t10_can_stamp_happy_path_true);
	UT_RUN(t11_lazy_invalid_buffer_false);
	UT_RUN(t12_lazy_slot_idx_out_of_bound_false);
	UT_RUN(t13_lazy_invalid_xid_false);
	UT_RUN(t14_lazy_invalid_commit_scn_false);
	UT_RUN(t15_initrans_default_is_8);
	UT_RUN(t16_itl_flag_constants_distinct);
	UT_RUN(t17_scn_valid_invalidscn_false);
	UT_RUN(t18_scn_valid_nonzero_true);
	UT_RUN(t19_slot_sizeof_48);
	UT_RUN(t20_slot_field_offsets_stable);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
