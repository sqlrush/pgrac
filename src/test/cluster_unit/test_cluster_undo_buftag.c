/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_buftag.c
 *	  pgrac spec-3.27 D1 / §2.1-A / §4 — buffer-backed undo identity +
 *	  BufferTag collision-freedom unit tests.
 *
 *	  Proves the reserved-OID isolation that lets undo blocks borrow the PG
 *	  BufTable namespace without ever colliding with a real relation:
 *	    T1  reserved OID constants + range invariants (spc != built-in
 *	        tablespaces;  dbOid range below FirstNormalObjectId and above the
 *	        built-in db OIDs)
 *	    T2  encode: cluster_undo_relfilelocator field mapping
 *	    T3  reverse-decode round-trips (owner_instance, segment_id)
 *	    T4  distinct (instance, segment) -> distinct locators (uniqueness)
 *	    T5  undo locator never equals a real relation locator (any tablespace)
 *	    T6  is_undo predicate: true for undo, false for real relations
 *	    T7  full valid dbOid range never overlaps a real database OID
 *	    T8  segment_id spans the full 32-bit relNumber space intact
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_undo_buftag.c
 *
 * NOTES
 *	  pgrac-original file.
 *	  Spec: spec-3.27-undo-buffer-backed-model.md (FROZEN v1.0, D1 / §4)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include "cluster/cluster_undo_buftag.h"

/* Un-remap the port.h printf family so this standalone test links against the
 * C library (no libpgport), matching the other simple cluster_unit tests. */
#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/* Build a "real relation" locator for the collision tests. */
static inline RelFileLocator
real_locator(Oid spc, Oid db, RelFileNumber rel)
{
	RelFileLocator rl;

	rl.spcOid = spc;
	rl.dbOid = db;
	rl.relNumber = rel;
	return rl;
}

UT_TEST(test_t1_reserved_oid_invariants)
{
	/* The whole reserved dbOid range must sit strictly below the
	 * dynamically-allocated floor and strictly above the built-in db OIDs. */
	UT_ASSERT(CLUSTER_UNDO_DB_BASE + UNDO_OWNER_INSTANCE_MAX < (Oid)FirstNormalObjectId);
	UT_ASSERT(CLUSTER_UNDO_DB_BASE > (Oid)PostgresDbOid);
	UT_ASSERT(CLUSTER_UNDO_DB_BASE > (Oid)Template0DbOid);
	UT_ASSERT(CLUSTER_UNDO_DB_BASE > (Oid)Template1DbOid);

	/* Reserved tablespace must not be a built-in tablespace and must stay
	 * below the dynamically-allocated tablespace floor. */
	UT_ASSERT(CLUSTER_UNDO_REL_SPCOID != (Oid)DEFAULTTABLESPACE_OID);
	UT_ASSERT(CLUSTER_UNDO_REL_SPCOID != (Oid)GLOBALTABLESPACE_OID);
	UT_ASSERT(CLUSTER_UNDO_REL_SPCOID < (Oid)FirstNormalObjectId);
}

UT_TEST(test_t2_encode_fields)
{
	RelFileLocator rl = cluster_undo_relfilelocator(3, 42);

	UT_ASSERT_EQ(rl.spcOid, (Oid)CLUSTER_UNDO_REL_SPCOID);
	UT_ASSERT_EQ(rl.dbOid, (Oid)(CLUSTER_UNDO_DB_BASE + 3));
	UT_ASSERT_EQ(rl.relNumber, (RelFileNumber)42);
}

UT_TEST(test_t3_decode_roundtrip)
{
	int inst;

	/* Sweep the full valid owner-instance range and a spread of segment ids. */
	for (inst = 1; inst <= UNDO_OWNER_INSTANCE_MAX; inst++) {
		RelFileNumber segs[5] = { 0, 1, 1000, 65535, 4294967295U };
		int s;

		for (s = 0; s < 5; s++) {
			RelFileLocator rl = cluster_undo_relfilelocator((uint8)inst, segs[s]);

			UT_ASSERT(cluster_undo_locator_is_undo(rl));
			UT_ASSERT_EQ((int)cluster_undo_locator_owner_instance(rl), inst);
			UT_ASSERT_EQ(cluster_undo_locator_segment_id(rl), segs[s]);
		}
	}
}

UT_TEST(test_t4_distinct_inputs_distinct_locators)
{
	RelFileLocator a = cluster_undo_relfilelocator(1, 10);
	RelFileLocator b = cluster_undo_relfilelocator(2, 10); /* different instance */
	RelFileLocator c = cluster_undo_relfilelocator(1, 11); /* different segment */

	/* Distinct instance and distinct segment both yield unequal locators —
	 * so their BufferTags (locator + fork + block) can never collide. */
	UT_ASSERT(!RelFileLocatorEquals(a, b));
	UT_ASSERT(!RelFileLocatorEquals(a, c));
	UT_ASSERT(!RelFileLocatorEquals(b, c));
	UT_ASSERT(RelFileLocatorEquals(a, a));
}

UT_TEST(test_t5_never_equals_real_relation)
{
	RelFileLocator undo = cluster_undo_relfilelocator(1, 5);

	/* Real relations always carry spcOid in {1663, 1664, >= 16384}.  Even a
	 * pathological real locator sharing our dbOid/relNumber differs in spcOid,
	 * so RelFileLocatorEquals (which compares spc, db, rel) is never true. */
	RelFileLocator r_default = real_locator(DEFAULTTABLESPACE_OID, 16400, 16500);
	RelFileLocator r_global = real_locator(GLOBALTABLESPACE_OID, 0, 1262);
	RelFileLocator r_user = real_locator(16384, 16400, 16500);
	RelFileLocator r_alias = real_locator(DEFAULTTABLESPACE_OID, undo.dbOid, undo.relNumber);

	UT_ASSERT(!RelFileLocatorEquals(undo, r_default));
	UT_ASSERT(!RelFileLocatorEquals(undo, r_global));
	UT_ASSERT(!RelFileLocatorEquals(undo, r_user));
	UT_ASSERT(!RelFileLocatorEquals(undo, r_alias));
}

UT_TEST(test_t6_is_undo_predicate)
{
	RelFileLocator undo = cluster_undo_relfilelocator(7, 700);
	RelFileLocator r_default = real_locator(DEFAULTTABLESPACE_OID, 16400, 16500);
	RelFileLocator r_global = real_locator(GLOBALTABLESPACE_OID, 0, 1262);
	RelFileLocator r_user = real_locator(16384, 16400, 16500);

	UT_ASSERT(cluster_undo_locator_is_undo(undo));
	UT_ASSERT(!cluster_undo_locator_is_undo(r_default));
	UT_ASSERT(!cluster_undo_locator_is_undo(r_global));
	UT_ASSERT(!cluster_undo_locator_is_undo(r_user));
}

UT_TEST(test_t7_dboid_range_no_real_db_overlap)
{
	int inst;

	/* Every dbOid the identity layer can produce must be neither a built-in
	 * database OID (1/4/5) nor a dynamically-allocatable one (>= 16384). */
	for (inst = 1; inst <= UNDO_OWNER_INSTANCE_MAX; inst++) {
		Oid db = CLUSTER_UNDO_DB_BASE + (Oid)inst;

		UT_ASSERT(db != (Oid)Template1DbOid);
		UT_ASSERT(db != (Oid)Template0DbOid);
		UT_ASSERT(db != (Oid)PostgresDbOid);
		UT_ASSERT(db < (Oid)FirstNormalObjectId);
	}
}

UT_TEST(test_t8_segment_id_full_width)
{
	/* segment_id uses the whole 32-bit relNumber — no bit-packing loss. */
	RelFileNumber hi = 0xFFFFFFFFU;
	RelFileLocator rl = cluster_undo_relfilelocator(1, hi);

	UT_ASSERT_EQ(rl.relNumber, hi);
	UT_ASSERT_EQ(cluster_undo_locator_segment_id(rl), hi);
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_t1_reserved_oid_invariants);
	UT_RUN(test_t2_encode_fields);
	UT_RUN(test_t3_decode_roundtrip);
	UT_RUN(test_t4_distinct_inputs_distinct_locators);
	UT_RUN(test_t5_never_equals_real_relation);
	UT_RUN(test_t6_is_undo_predicate);
	UT_RUN(test_t7_dboid_range_no_real_db_overlap);
	UT_RUN(test_t8_segment_id_full_width);
	UT_DONE();
	return 0;
}
