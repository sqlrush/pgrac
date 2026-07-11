/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_resid.c
 *	  Unit tests for the shared-undo block resource identity pure layer
 *	  (spec-5.22a D1).
 *
 *	  U2 pins the undo resid class byte (0xF9) above every PG LockTagType
 *	  and distinct from every header-visible resid class (SQ 0xF0 / CF 0xF1
 *	  / HW 0xF2 / DL 0xF3 / TT 0xF4 / IR 0xF5 / KO 0xF6 / OID 0xF7 /
 *	  RELMAP 0xF8), plus the backend-local raw-layout 0xF3 value that the
 *	  header cross-assert net cannot see.  The real owner-as-master data
 *	  plane (grant / PI / shipping) is D2+; this file pins the identity
 *	  contract only.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_undo_resid.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.22a-undo-block-resource-identity.md (D1, §4.1)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_scn.h" /* SCN_MAX_VALID_NODE_ID */
#include "cluster/cluster_undo_resid.h"
#include "storage/lock.h"
#include "utils/errcodes.h" /* ERRCODE_CLUSTER_UNDO_RESID_INVALID (guard-fire) */

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/*
 * spec-5.22d Hardening — fail-closed guard-fire trampoline (mirrors
 * test_cluster_grd.c / test_cluster_fence.c).  When armed, an Assert trip
 * (assert builds) jumps back with rc 2, and an ereport(ERROR) jumps back with
 * rc 1 after the errcode stub captured the SQLSTATE.  The derived-primitive
 * guards are runtime ereports (NOT Asserts), so rc 1 is the pass arm in BOTH
 * cassert and production builds (an Assert trip, rc 2, means the guard is still
 * the pre-fix no-op-in-production Assert).
 */
static sigjmp_buf ut_trap_jump;
static bool ut_trap_jump_armed = false;
static int ut_ereport_last_errcode = 0;
static int ut_current_elevel = 0;

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	if (ut_trap_jump_armed)
		siglongjmp(ut_trap_jump, 2);
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

bool
errstart(int elevel, const char *d pg_attribute_unused())
{
	ut_current_elevel = elevel;
	/* PG: ERROR = 21 (elog.h);  >= ERROR runs the ereport body so the errcode
	 * stub can capture the SQLSTATE before errfinish jumps. */
	return elevel >= 21;
}

bool
errstart_cold(int elevel, const char *d)
{
	return errstart(elevel, d);
}

void
errfinish(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		  const char *fn pg_attribute_unused())
{
	if (ut_current_elevel >= 21 && ut_trap_jump_armed)
		siglongjmp(ut_trap_jump, 1);
}

int
errcode(int s)
{
	if (ut_trap_jump_armed)
		ut_ereport_last_errcode = s;
	return 0;
}

int
errmsg(const char *f pg_attribute_unused(), ...)
{
	return 0;
}

int
errmsg_internal(const char *f pg_attribute_unused(), ...)
{
	return 0;
}

int
errdetail(const char *f pg_attribute_unused(), ...)
{
	return 0;
}

int
errhint(const char *f pg_attribute_unused(), ...)
{
	return 0;
}

sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

/* ======================================================================
 * U2 -- class byte: 0xF9, above PG lock types, no collision with any
 * existing resid class (spec-5.22a §2.1)
 * ====================================================================== */
UT_TEST(test_undo_resid_class_byte)
{
	/* 0xF9 is the next free slot after the contiguous 0xF0-0xF8 run */
	UT_ASSERT_EQ(CLUSTER_UNDO_RESID_TYPE, 0xF9);
	UT_ASSERT(CLUSTER_UNDO_RESID_TYPE > LOCKTAG_LAST_TYPE);

	/* distinct from every header-visible resid class */
	UT_ASSERT_NE(CLUSTER_UNDO_RESID_TYPE, CLUSTER_SQ_RESID_TYPE);
	UT_ASSERT_NE(CLUSTER_UNDO_RESID_TYPE, CLUSTER_CF_RESID_TYPE);
	UT_ASSERT_NE(CLUSTER_UNDO_RESID_TYPE, CLUSTER_HW_RESID_TYPE);
	UT_ASSERT_NE(CLUSTER_UNDO_RESID_TYPE, CLUSTER_DL_RESID_TYPE);
	UT_ASSERT_NE(CLUSTER_UNDO_RESID_TYPE, CLUSTER_TT_RESID_TYPE);
	UT_ASSERT_NE(CLUSTER_UNDO_RESID_TYPE, CLUSTER_IR_RESID_TYPE);
	UT_ASSERT_NE(CLUSTER_UNDO_RESID_TYPE, CLUSTER_KO_RESID_TYPE);
	UT_ASSERT_NE(CLUSTER_UNDO_RESID_TYPE, CLUSTER_OID_RESID_TYPE);
	UT_ASSERT_NE(CLUSTER_UNDO_RESID_TYPE, CLUSTER_RELMAP_RESID_TYPE);

	/*
	 * Namespace audit beyond the header net: CLUSTER_RAW_LAYOUT_RESID_TYPE
	 * (0xF3) is a backend-local define in cluster_shared_fs_block_device.c,
	 * not header-visible, so the header StaticAssert net cannot name it.
	 * Pin the raw value here so the undo class also stays clear of it.
	 * NB: 0xF3 itself is double-booked today (DL + raw layout); cleanup is
	 * a registered follow-up outside this spec.
	 */
	UT_ASSERT_NE(CLUSTER_UNDO_RESID_TYPE, 0xF3);
}

/* ======================================================================
 * U1 -- encode/decode round-trip: all four identity dimensions survive
 * ====================================================================== */
UT_TEST(test_undo_resid_encode_decode_roundtrip)
{
	ClusterResId r;
	int32 owner_node;
	uint32 undo_segment;
	uint32 block_no;
	uint32 generation;

	memset(&r, 0xEE, sizeof(r));
	cluster_undo_resid_encode(2, 7, 129, 3, &r);

	UT_ASSERT_EQ(r.field1, 7);	 /* undo_segment */
	UT_ASSERT_EQ(r.field2, 129); /* block_no */
	UT_ASSERT_EQ(r.field3, 3);	 /* generation */
	UT_ASSERT_EQ(r.field4, 2);	 /* owner_node */
	UT_ASSERT_EQ(r.type, CLUSTER_UNDO_RESID_TYPE);
	UT_ASSERT_EQ(r.lockmethodid, DEFAULT_LOCKMETHOD);

	owner_node = -1;
	undo_segment = 0xDEADBEEF;
	block_no = 0xDEADBEEF;
	generation = 0xDEADBEEF;
	cluster_undo_resid_decode(&r, &owner_node, &undo_segment, &block_no, &generation);
	UT_ASSERT_EQ(owner_node, 2);
	UT_ASSERT_EQ(undo_segment, 7);
	UT_ASSERT_EQ(block_no, 129);
	UT_ASSERT_EQ(generation, 3);
}

/* block 0 (the segment TT header block) is a valid block_no: DATA and TT
 * blocks share the one undo class */
UT_TEST(test_undo_resid_tt_header_block_zero)
{
	ClusterResId r;
	int32 owner_node;
	uint32 undo_segment;
	uint32 block_no;
	uint32 generation;

	cluster_undo_resid_encode(1, 42, 0, 9, &r);
	cluster_undo_resid_decode(&r, &owner_node, &undo_segment, &block_no, &generation);
	UT_ASSERT_EQ(block_no, 0);
	UT_ASSERT_EQ(undo_segment, 42);
}

/* ======================================================================
 * U3 -- class discriminator: true for undo, false for other classes
 * ====================================================================== */
UT_TEST(test_undo_resid_is_undo)
{
	ClusterResId undo_r;
	ClusterResId hw_r;
	ClusterResId cf_r;
	RelFileLocator rloc;

	cluster_undo_resid_encode(0, 1, 2, 0, &undo_r);
	UT_ASSERT(cluster_undo_resid_is_undo(&undo_r));

	/* a real HW resid (encoder linked) is not an undo resid */
	rloc.spcOid = 1663;
	rloc.dbOid = 5;
	rloc.relNumber = 16384;
	cluster_hw_resid_encode(rloc, MAIN_FORKNUM, &hw_r);
	UT_ASSERT(!cluster_undo_resid_is_undo(&hw_r));

	/* CF resid constructed by hand (the CF encoder lives in a backend
	 * object; only the type byte matters to the discriminator) */
	memset(&cf_r, 0, sizeof(cf_r));
	cf_r.type = CLUSTER_CF_RESID_TYPE;
	cf_r.lockmethodid = DEFAULT_LOCKMETHOD;
	UT_ASSERT(!cluster_undo_resid_is_undo(&cf_r));
}

/* ======================================================================
 * U6 -- wire ABI: ClusterResId stays 16 bytes
 * ====================================================================== */
UT_TEST(test_undo_resid_wire_abi_16_bytes)
{
	UT_ASSERT_EQ(sizeof(ClusterResId), 16);
}

/* ======================================================================
 * U7 -- owner_node boundary encoding (0 / 15 / SCN_MAX_VALID_NODE_ID)
 * ====================================================================== */
UT_TEST(test_undo_resid_owner_bounds)
{
	static const int32 owners[] = { 0, 15, SCN_MAX_VALID_NODE_ID };
	int i;

	for (i = 0; i < (int)lengthof(owners); i++) {
		ClusterResId r;
		int32 owner_node = -1;
		uint32 undo_segment;
		uint32 block_no;
		uint32 generation;

		cluster_undo_resid_encode(owners[i], 3, 5, 1, &r);
		cluster_undo_resid_decode(&r, &owner_node, &undo_segment, &block_no, &generation);
		UT_ASSERT_EQ(owner_node, owners[i]);
	}
}

/* ======================================================================
 * U4 -- owner-as-master routing: the master IS the encoded owner_node,
 * never a hash-derived node (spec-5.22a §3.1)
 * ====================================================================== */
UT_TEST(test_undo_resid_master_is_owner)
{
	ClusterResId r;

	cluster_undo_resid_encode(3, 11, 200, 5, &r);
	UT_ASSERT_EQ(cluster_undo_resid_master(&r), 3);

	/* the master must not vary with the non-owner identity dimensions
	 * (a shard-hash master would) */
	cluster_undo_resid_encode(3, 9999, 123456, 42, &r);
	UT_ASSERT_EQ(cluster_undo_resid_master(&r), 3);

	cluster_undo_resid_encode(0, 1, 1, 1, &r);
	UT_ASSERT_EQ(cluster_undo_resid_master(&r), 0);

	cluster_undo_resid_encode(SCN_MAX_VALID_NODE_ID, 1, 1, 1, &r);
	UT_ASSERT_EQ(cluster_undo_resid_master(&r), SCN_MAX_VALID_NODE_ID);
}

/* ======================================================================
 * U5 -- anti-ABA generation predicate: mismatch means stale reference
 * (caller must fail closed, never treat as a match)
 * ====================================================================== */
UT_TEST(test_undo_resid_generation_matches)
{
	ClusterResId r;

	cluster_undo_resid_encode(1, 7, 129, 3, &r);
	UT_ASSERT(cluster_undo_resid_generation_matches(&r, 3));
	UT_ASSERT(!cluster_undo_resid_generation_matches(&r, 2));
	UT_ASSERT(!cluster_undo_resid_generation_matches(&r, 4));

	/* generation 0 (never-reused segment) matches only 0 */
	cluster_undo_resid_encode(1, 7, 129, 0, &r);
	UT_ASSERT(cluster_undo_resid_generation_matches(&r, 0));
	UT_ASSERT(!cluster_undo_resid_generation_matches(&r, 1));
}

/* ======================================================================
 * spec-5.22d Hardening — derived-primitive fail-closed guards.  A non-undo
 * class resid passed to decode / master / generation_matches, or an
 * out-of-range owner_node passed to encode, must ereport(ERROR) 53R9R (a real
 * runtime guard, symmetric with the GRD-side 53R9Q hash-route guard), NEVER a
 * silently misrouted answer.  Pre-fix these were Assert-only (a no-op under
 * --disable-cassert): the trampoline requires rc 1 (the ereport arm), so a
 * cassert-only Assert trip (rc 2) fails the test -- proving the guard now fires
 * in production too.
 * ====================================================================== */
static void
make_non_undo_resid(ClusterResId *r)
{
	/* a real other class (CF), never the undo class -- only the type byte
	 * matters to the discriminator guard */
	memset(r, 0, sizeof(*r));
	r->type = CLUSTER_CF_RESID_TYPE;
	r->lockmethodid = DEFAULT_LOCKMETHOD;
	r->field4 = 1;
}

UT_TEST(test_undo_resid_master_rejects_non_undo_class)
{
	ClusterResId r;
	int rc;

	make_non_undo_resid(&r);
	ut_ereport_last_errcode = 0;
	rc = sigsetjmp(ut_trap_jump, 0);
	if (rc == 0) {
		ut_trap_jump_armed = true;
		(void)cluster_undo_resid_master(&r);
		ut_trap_jump_armed = false;
		UT_ASSERT(false); /* reaching here = the misroute was not refused */
	}
	ut_trap_jump_armed = false;
	UT_ASSERT_EQ(rc, 1); /* ereport arm, not the cassert-only Assert (rc 2) */
	UT_ASSERT_EQ(ut_ereport_last_errcode, ERRCODE_CLUSTER_UNDO_RESID_INVALID);
}

UT_TEST(test_undo_resid_decode_rejects_non_undo_class)
{
	ClusterResId r;
	int32 owner = 0;
	uint32 seg = 0;
	uint32 block = 0;
	uint32 gen = 0;
	int rc;

	make_non_undo_resid(&r);
	ut_ereport_last_errcode = 0;
	rc = sigsetjmp(ut_trap_jump, 0);
	if (rc == 0) {
		ut_trap_jump_armed = true;
		cluster_undo_resid_decode(&r, &owner, &seg, &block, &gen);
		ut_trap_jump_armed = false;
		UT_ASSERT(false);
	}
	ut_trap_jump_armed = false;
	UT_ASSERT_EQ(rc, 1);
	UT_ASSERT_EQ(ut_ereport_last_errcode, ERRCODE_CLUSTER_UNDO_RESID_INVALID);
}

UT_TEST(test_undo_resid_generation_matches_rejects_non_undo_class)
{
	ClusterResId r;
	int rc;

	make_non_undo_resid(&r);
	ut_ereport_last_errcode = 0;
	rc = sigsetjmp(ut_trap_jump, 0);
	if (rc == 0) {
		ut_trap_jump_armed = true;
		(void)cluster_undo_resid_generation_matches(&r, 0);
		ut_trap_jump_armed = false;
		UT_ASSERT(false);
	}
	ut_trap_jump_armed = false;
	UT_ASSERT_EQ(rc, 1);
	UT_ASSERT_EQ(ut_ereport_last_errcode, ERRCODE_CLUSTER_UNDO_RESID_INVALID);
}

UT_TEST(test_undo_resid_encode_rejects_out_of_range_owner)
{
	ClusterResId r;
	int rc;

	ut_ereport_last_errcode = 0;
	rc = sigsetjmp(ut_trap_jump, 0);
	if (rc == 0) {
		ut_trap_jump_armed = true;
		/* one past the valid node range would truncate into the uint16 field4 */
		cluster_undo_resid_encode(SCN_MAX_VALID_NODE_ID + 1, 3, 5, 1, &r);
		ut_trap_jump_armed = false;
		UT_ASSERT(false);
	}
	ut_trap_jump_armed = false;
	UT_ASSERT_EQ(rc, 1);
	UT_ASSERT_EQ(ut_ereport_last_errcode, ERRCODE_CLUSTER_UNDO_RESID_INVALID);
}

int
main(void)
{
	UT_PLAN(12);
	UT_RUN(test_undo_resid_class_byte);
	UT_RUN(test_undo_resid_encode_decode_roundtrip);
	UT_RUN(test_undo_resid_tt_header_block_zero);
	UT_RUN(test_undo_resid_is_undo);
	UT_RUN(test_undo_resid_wire_abi_16_bytes);
	UT_RUN(test_undo_resid_owner_bounds);
	UT_RUN(test_undo_resid_master_is_owner);
	UT_RUN(test_undo_resid_generation_matches);
	UT_RUN(test_undo_resid_master_rejects_non_undo_class);
	UT_RUN(test_undo_resid_decode_rejects_non_undo_class);
	UT_RUN(test_undo_resid_generation_matches_rejects_non_undo_class);
	UT_RUN(test_undo_resid_encode_rejects_out_of_range_owner);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
