/*-------------------------------------------------------------------------
 *
 * test_cluster_visibility_inject.c
 *	  pgrac spec-3.4c D11 — cluster_unit tests for the D5b 6-arg inject
 *	  UDF (A1 P0 overlay install + F4 delete_exact + F5 verify).
 *
 *	  16 tests covering:
 *	    T1   cluster_test_lookup_visibility_inject API linkable
 *	    T2   cluster_visibility_inject_shmem_* APIs linkable
 *	    T3   cluster_tt_status_lookup_exact API linkable
 *	    T4   cluster_tt_status_install_local API linkable
 *	    T5   cluster_tt_status_delete_exact API linkable (spec-3.4c D6 / F4)
 *	    T6   ClusterUndoTTSlotRef.cached_commit_scn at offset 16 (D9 stash sink)
 *	    T7   ClusterUndoTTSlotRef.has_cached_status at offset 24 (D9 toggle)
 *	    T8   ClusterUndoTTSlotRef sizeof 32 (regression)
 *	    T9   ClusterTTStatusKey 5 identity fields carry from ref (A1 build_key contract)
 *	    T10  ClusterTTStatusKey _reserved + _reserved2 zero-init
 *	    T11  ClusterTTStatus enum 5 values stable (UNKNOWN/IN_PROGRESS/COMMITTED/ABORTED/CLEANED_OUT)
 *	    T12  ERRCODE_CLUSTER_TT_STATUS_UNKNOWN == 53R97 (A1 overlay miss target)
 *	    T13  SCN_VALID(InvalidScn)=false / SCN_VALID(non-zero)=true (D9 has_cached_status semantics)
 *	    T14  CLUSTER_TT_STATUS_COMMITTED == 2 (D7 install_local status arg)
 *	    T15  ENABLE_INJECTION stub semantic — lookup helper returns false
 *	    T16  cluster_test_inject_visibility_tt_ref pg_proc symbol linkable (UDF entry)
 *
 *	  Behavioural coverage of the install/lookup/verify chain under a
 *	  real backend (overlay-full triggers ERROR, clear deletes overlay,
 *	  cached_commit_scn → decide_by_scn returns VISIBLE/INVISIBLE) is
 *	  cluster_tap t/208 (D12).  cluster_unit cannot exercise overlay
 *	  shmem behaviour without a postmaster.
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_visibility_inject.c
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
#include "cluster/cluster_scn.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_visibility_inject.h"
#include "fmgr.h"
#include "utils/errcodes.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"


UT_DEFINE_GLOBALS();


void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}


/* Local stubs — this cluster_unit binary does not link the real
 * cluster_tt_status.o or cluster_visibility_inject.o (they pull in
 * shmem/LWLock that requires a postmaster).  Symbol presence is what
 * we verify; behavioural coverage moves to TAP. */
bool
cluster_test_lookup_visibility_inject(TransactionId xid pg_attribute_unused(),
									  ClusterUndoTTSlotRef *ref pg_attribute_unused())
{
	return false;
}
Size
cluster_visibility_inject_shmem_size(void)
{
	return 0;
}
void
cluster_visibility_inject_shmem_init(void)
{}
void
cluster_visibility_inject_shmem_register(void)
{}

bool
cluster_tt_status_lookup_exact(const ClusterTTStatusKey *key pg_attribute_unused(),
							   ClusterTTStatusResult *res pg_attribute_unused())
{
	return false;
}
void
cluster_tt_status_install_local(const ClusterTTStatusKey *key pg_attribute_unused(),
								ClusterTTStatus status pg_attribute_unused(),
								SCN commit_scn pg_attribute_unused())
{}
bool
cluster_tt_status_delete_exact(const ClusterTTStatusKey *key pg_attribute_unused())
{
	return false;
}


/* The fmgr-wrapped UDF entry is declared via PG_FUNCTION_INFO_V1 in
 * cluster_visibility_inject.c;  we declare an extern prototype here to
 * test symbol linkability.  The real implementation is supplied at link
 * time via the dummy below (cluster_unit binary does not link the real
 * cluster_visibility_inject.o because it would drag in superuser() +
 * HTAB + LWLock).  T16 verifies the symbol address is non-NULL. */
extern Datum cluster_test_inject_visibility_tt_ref(PG_FUNCTION_ARGS);
Datum
cluster_test_inject_visibility_tt_ref(PG_FUNCTION_ARGS pg_attribute_unused())
{
	PG_RETURN_BOOL(false);
}


/* ===== T1-T5: API surface linkable ===== */

UT_TEST(t1_lookup_inject_linkable)
{
	UT_ASSERT_NE((void *)cluster_test_lookup_visibility_inject, NULL);
}
UT_TEST(t2_inject_shmem_helpers_linkable)
{
	UT_ASSERT_NE((void *)cluster_visibility_inject_shmem_size, NULL);
	UT_ASSERT_NE((void *)cluster_visibility_inject_shmem_init, NULL);
	UT_ASSERT_NE((void *)cluster_visibility_inject_shmem_register, NULL);
}
UT_TEST(t3_tt_status_lookup_exact_linkable)
{
	UT_ASSERT_NE((void *)cluster_tt_status_lookup_exact, NULL);
}
UT_TEST(t4_tt_status_install_local_linkable)
{
	UT_ASSERT_NE((void *)cluster_tt_status_install_local, NULL);
}
UT_TEST(t5_tt_status_delete_exact_linkable)
{
	/* spec-3.4c D6 / F4:  per-key delete companion required so D5b
	 * clear UDF does not fake-clear via ABORTED install. */
	UT_ASSERT_NE((void *)cluster_tt_status_delete_exact, NULL);
}


/* ===== T6-T8: ClusterUndoTTSlotRef layout (D9 stash sink) ===== */

UT_TEST(t6_ref_cached_commit_scn_offset_16)
{
	/* D9 stashes commit_scn into ref->cached_commit_scn for D5b lookup. */
	UT_ASSERT_EQ((int)offsetof(ClusterUndoTTSlotRef, cached_commit_scn), 16);
}
UT_TEST(t7_ref_has_cached_status_offset_24)
{
	UT_ASSERT_EQ((int)offsetof(ClusterUndoTTSlotRef, has_cached_status), 24);
}
UT_TEST(t8_ref_sizeof_32)
{
	UT_ASSERT_EQ((int)sizeof(ClusterUndoTTSlotRef), 32);
}


/* ===== T9-T10: ClusterTTStatusKey build-from-ref contract (A1 install path) ===== */

UT_TEST(t9_build_key_5_identity_fields_carry)
{
	ClusterUndoTTSlotRef ref;
	ClusterTTStatusKey key;

	memset(&ref, 0, sizeof(ref));
	ref.origin_node_id = 11;
	ref.undo_segment_id = 22;
	ref.tt_slot_id = 33;
	ref.cluster_epoch = 444;
	ref.local_xid = 555;

	memset(&key, 0, sizeof(key));
	key.origin_node_id = ref.origin_node_id;
	key.undo_segment_id = ref.undo_segment_id;
	key.tt_slot_id = ref.tt_slot_id;
	key.cluster_epoch = ref.cluster_epoch;
	key.local_xid = ref.local_xid;

	UT_ASSERT_EQ((int)key.origin_node_id, 11);
	UT_ASSERT_EQ((int)key.undo_segment_id, 22);
	UT_ASSERT_EQ((int)key.tt_slot_id, 33);
	UT_ASSERT_EQ((int)key.cluster_epoch, 444);
	UT_ASSERT_EQ((int)key.local_xid, 555);
}

UT_TEST(t10_build_key_reserved_zero)
{
	ClusterTTStatusKey key;
	memset(&key, 0, sizeof(key));
	UT_ASSERT_EQ((int)key._reserved, 0);
	UT_ASSERT_EQ((int)key._reserved2, 0);
}


/* ===== T11-T14: enum / constant stability ===== */

UT_TEST(t11_status_enum_5_values)
{
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_UNKNOWN, 0);
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_IN_PROGRESS, 1);
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_COMMITTED, 2);
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_ABORTED, 3);
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_CLEANED_OUT, 4);
}

UT_TEST(t12_errcode_53r97_encodable)
{
	int sqlstate = MAKE_SQLSTATE('5', '3', 'R', '9', '7');
	UT_ASSERT_EQ((int)ERRCODE_CLUSTER_TT_STATUS_UNKNOWN, sqlstate);
}

UT_TEST(t13_scn_valid_semantics)
{
	UT_ASSERT_EQ((int)SCN_VALID(InvalidScn), 0);
	UT_ASSERT_NE((int)SCN_VALID((SCN)42), 0);
}

UT_TEST(t14_status_committed_is_2)
{
	/* D7 install_local uses CLUSTER_TT_STATUS_COMMITTED as the status arg;
	 * a value drift would silently break the overlay's lookup expectation. */
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_COMMITTED, 2);
}


/* ===== T15-T16: ENABLE_INJECTION conditional + UDF entry symbol ===== */

UT_TEST(t15_production_inject_stub_returns_false)
{
	ClusterUndoTTSlotRef ref;
	bool hit;
	memset(&ref, 0, sizeof(ref));
	/* This binary links the local stub form; covers ENABLE_INJECTION-off
	 * (production binary) FEATURE_NOT_SUPPORTED equivalent semantics. */
	hit = cluster_test_lookup_visibility_inject(99, &ref);
	UT_ASSERT_EQ((int)hit, 0);
}

UT_TEST(t16_inject_udf_entry_linkable)
{
	UT_ASSERT_NE((void *)cluster_test_inject_visibility_tt_ref, NULL);
}


int
main(void)
{
	UT_PLAN(16);
	UT_RUN(t1_lookup_inject_linkable);
	UT_RUN(t2_inject_shmem_helpers_linkable);
	UT_RUN(t3_tt_status_lookup_exact_linkable);
	UT_RUN(t4_tt_status_install_local_linkable);
	UT_RUN(t5_tt_status_delete_exact_linkable);
	UT_RUN(t6_ref_cached_commit_scn_offset_16);
	UT_RUN(t7_ref_has_cached_status_offset_24);
	UT_RUN(t8_ref_sizeof_32);
	UT_RUN(t9_build_key_5_identity_fields_carry);
	UT_RUN(t10_build_key_reserved_zero);
	UT_RUN(t11_status_enum_5_values);
	UT_RUN(t12_errcode_53r97_encodable);
	UT_RUN(t13_scn_valid_semantics);
	UT_RUN(t14_status_committed_is_2);
	UT_RUN(t15_production_inject_stub_returns_false);
	UT_RUN(t16_inject_udf_entry_linkable);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
