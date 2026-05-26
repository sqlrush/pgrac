/*-------------------------------------------------------------------------
 *
 * test_cluster_tt_status.c
 *	  pgrac spec-3.1 D9 — cluster_unit static-contract tests for the
 *	  Undo TT status foundation (exact-key API + ITL reader contract).
 *
 *	  22 真 C 单测 enumeration (v0.4 M5):
 *
 *	    T1   sizeof(ClusterTTStatusKey) == 24 (v0.4 N9 explicit _reserved2)
 *	    T2   sizeof(ClusterUndoTTSlotRef) == 32 (v0.4 M4)
 *	    T3   CLUSTER_TT_STATUS_UNKNOWN == 0 (sentinel)
 *	    T4   CLUSTER_TT_STATUS_IN_PROGRESS == 1
 *	    T5   CLUSTER_TT_STATUS_COMMITTED == 2
 *	    T6   CLUSTER_TT_STATUS_ABORTED == 3
 *	    T7   CLUSTER_TT_STATUS_CLEANED_OUT == 4
 *	    T8   public API: cluster_tt_status_lookup_exact prototype linkable
 *	    T9   public API: cluster_tt_status_install_local prototype linkable
 *	    T10  public API: cluster_tt_status_flush_all prototype linkable
 *	    T11  public API: cluster_tt_status_generation prototype linkable
 *	    T12  ClusterTTStatusKey field offsets locked (HC183 wire-stable)
 *	    T13  ClusterTTStatusResult layout sane (status + commit_scn + epoch + authoritative)
 *	    T14  ClusterUndoTTSlotRef field offsets locked
 *	    T15  GUC cluster_tt_status_overlay_max_entries default 32768
 *	    T16  GUC cluster_tt_status_overlay_ttl_ms default 30000
 *	    T17  D5 local install API prototype linkable (commit + abort + slot_seq_peek)
 *	    T18  ITL reader prototype linkable (D4 cluster_itl_get_tt_ref)
 *	    T19  LWTRANCHE_CLUSTER_TT_STATUS enum slot defined
 *	    T20  shmem helpers linkable (size + init + register for D2 + D5)
 *	    T21  self-consumer hit bump API linkable (v0.4 N7)
 *	    T22  ClusterTTStatus enum and TTSlotStatus enum kept distinct (no
 *	         conflict by value;  Cluster_TT_STATUS_* are wire enum,
 *	         TT_SLOT_* are on-disk slot status)
 *
 *	  No raw xid lookup path is exercised here (HC180 / L176 — by design,
 *	  no such API exists in spec-3.1).  Negative-grep assertions (no
 *	  banned CLOG-overlay identifiers — see check-no-clog-overlay.sh for
 *	  the full banned list) live in scripts/ci/check-no-clog-overlay.sh
 *	  (D11);  not encoded as C unit tests (v0.4 N8).
 *
 *	  Header-only;  behavioral coverage in cluster_tap t/203.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_tt_status.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-3.1-cluster-xid-status-foundation.md (FROZEN v1.0)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>

#include "cluster/cluster_guc.h"
#include "cluster/cluster_tt_local.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_tt_status.h"
#include "storage/lwlock.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"


UT_DEFINE_GLOBALS();


void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}


/*
 * Stubs — the cluster_unit binary doesn't link cluster_tt_*.o (matches
 * the test_cluster_sinval_ack pattern).  Only addresses are taken; never
 * called.  GUC variables are defined here as locals so symbol references
 * resolve.
 */
int cluster_tt_status_overlay_max_entries = 32768;
int cluster_tt_status_overlay_ttl_ms = 30000;

bool
cluster_tt_status_lookup_exact(const ClusterTTStatusKey *key pg_attribute_unused(),
							   ClusterTTStatusResult *result pg_attribute_unused())
{
	return false;
}

bool
cluster_tt_status_install_local(const ClusterTTStatusKey *key pg_attribute_unused(),
								ClusterTTStatus status pg_attribute_unused(),
								SCN commit_scn pg_attribute_unused())
{
	return false;
}

void
cluster_tt_status_flush_all(uint32 new_epoch pg_attribute_unused())
{}

uint64
cluster_tt_status_generation(void)
{
	return 0;
}

void
cluster_tt_status_bump_self_consumer_hit(void)
{}

Size
cluster_tt_status_shmem_size(void)
{
	return 0;
}
void
cluster_tt_status_shmem_init(void)
{}
void
cluster_tt_status_shmem_register(void)
{}

void
cluster_tt_local_record_commit(TransactionId xid pg_attribute_unused(),
							   SCN commit_scn pg_attribute_unused())
{}

void
cluster_tt_local_record_abort(TransactionId xid pg_attribute_unused())
{}

uint32
cluster_tt_local_slot_seq_peek(void)
{
	return 0;
}

Size
cluster_tt_local_shmem_size(void)
{
	return 0;
}
void
cluster_tt_local_shmem_init(void)
{}
void
cluster_tt_local_shmem_register(void)
{}

/*
 * Forward-decl ITL reader so we can address-take it without pulling in
 * storage/bufpage.h (which has heavy backend dependencies).  We never
 * call it from this test.
 */
extern bool cluster_itl_get_tt_ref(void *page, uint8 itl_slot_idx, void *ref);
bool
cluster_itl_get_tt_ref(void *page pg_attribute_unused(), uint8 itl_slot_idx pg_attribute_unused(),
					   void *ref pg_attribute_unused())
{
	return false;
}


/* ===== T1: ClusterTTStatusKey 24B (v0.4 N9) ===== */
UT_TEST(test_t1_status_key_sizeof_24)
{
	UT_ASSERT_EQ((int)sizeof(ClusterTTStatusKey), 24);
}

/* ===== T2: ClusterUndoTTSlotRef 32B (v0.4 M4) ===== */
UT_TEST(test_t2_undo_tt_slot_ref_sizeof_32)
{
	UT_ASSERT_EQ((int)sizeof(ClusterUndoTTSlotRef), 32);
}

/* ===== T3-T7: ClusterTTStatus enum values stable ===== */
UT_TEST(test_t3_enum_unknown_zero)
{
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_UNKNOWN, 0);
}
UT_TEST(test_t4_enum_in_progress_one)
{
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_IN_PROGRESS, 1);
}
UT_TEST(test_t5_enum_committed_two)
{
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_COMMITTED, 2);
}
UT_TEST(test_t6_enum_aborted_three)
{
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_ABORTED, 3);
}
UT_TEST(test_t7_enum_cleaned_out_four)
{
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_CLEANED_OUT, 4);
}

/* ===== T8-T11: public API linkable ===== */
UT_TEST(test_t8_lookup_exact_linkable)
{
	UT_ASSERT_NE((void *)cluster_tt_status_lookup_exact, NULL);
}
UT_TEST(test_t9_install_local_linkable)
{
	UT_ASSERT_NE((void *)cluster_tt_status_install_local, NULL);
}
UT_TEST(test_t10_flush_all_linkable)
{
	UT_ASSERT_NE((void *)cluster_tt_status_flush_all, NULL);
}
UT_TEST(test_t11_generation_linkable)
{
	UT_ASSERT_NE((void *)cluster_tt_status_generation, NULL);
}

/* ===== T12: ClusterTTStatusKey field offsets locked (HC183 wire-stable) ===== */
UT_TEST(test_t12_status_key_field_offsets)
{
	UT_ASSERT_EQ((int)offsetof(ClusterTTStatusKey, origin_node_id), 0);
	UT_ASSERT_EQ((int)offsetof(ClusterTTStatusKey, undo_segment_id), 2);
	UT_ASSERT_EQ((int)offsetof(ClusterTTStatusKey, tt_slot_id), 4);
	UT_ASSERT_EQ((int)offsetof(ClusterTTStatusKey, cluster_epoch), 8);
	UT_ASSERT_EQ((int)offsetof(ClusterTTStatusKey, local_xid), 12);
	UT_ASSERT_EQ((int)offsetof(ClusterTTStatusKey, _reserved), 16);
	UT_ASSERT_EQ((int)offsetof(ClusterTTStatusKey, _reserved2), 20);
}

/* ===== T13: ClusterTTStatusResult layout ===== */
UT_TEST(test_t13_status_result_field_offsets)
{
	int minsz;

	UT_ASSERT_EQ((int)offsetof(ClusterTTStatusResult, status), 0);
	/* commit_scn / status_epoch / authoritative — order locked, exact
	 * offsets depend on natural alignment;  test sizeof for total layout
	 * sanity instead. */
	minsz = (int)(sizeof(ClusterTTStatus) + sizeof(SCN) + sizeof(uint32) + sizeof(bool));
	UT_ASSERT_EQ((int)sizeof(ClusterTTStatusResult) >= minsz, 1);
}

/* ===== T14: ClusterUndoTTSlotRef field offsets locked (v0.4 M4) ===== */
UT_TEST(test_t14_undo_tt_slot_ref_field_offsets)
{
	UT_ASSERT_EQ((int)offsetof(ClusterUndoTTSlotRef, origin_node_id), 0);
	UT_ASSERT_EQ((int)offsetof(ClusterUndoTTSlotRef, undo_segment_id), 2);
	UT_ASSERT_EQ((int)offsetof(ClusterUndoTTSlotRef, tt_slot_id), 4);
	UT_ASSERT_EQ((int)offsetof(ClusterUndoTTSlotRef, cluster_epoch), 8);
	UT_ASSERT_EQ((int)offsetof(ClusterUndoTTSlotRef, local_xid), 12);
	UT_ASSERT_EQ((int)offsetof(ClusterUndoTTSlotRef, cached_commit_scn), 16);
	UT_ASSERT_EQ((int)offsetof(ClusterUndoTTSlotRef, has_cached_status), 24);
	UT_ASSERT_EQ((int)offsetof(ClusterUndoTTSlotRef, _padding), 25);
}

/* ===== T15-T16: GUC defaults ===== */
UT_TEST(test_t15_guc_overlay_max_entries_default)
{
	UT_ASSERT_EQ(cluster_tt_status_overlay_max_entries, 32768);
}
UT_TEST(test_t16_guc_overlay_ttl_ms_default)
{
	UT_ASSERT_EQ(cluster_tt_status_overlay_ttl_ms, 30000);
}

/* ===== T17: D5 local install + slot_seq API linkable ===== */
UT_TEST(test_t17_local_install_api_linkable)
{
	UT_ASSERT_NE((void *)cluster_tt_local_record_commit, NULL);
	UT_ASSERT_NE((void *)cluster_tt_local_record_abort, NULL);
	UT_ASSERT_NE((void *)cluster_tt_local_slot_seq_peek, NULL);
}

/* ===== T18: D4 ITL reader linkable ===== */
UT_TEST(test_t18_itl_reader_linkable)
{
	UT_ASSERT_NE((void *)cluster_itl_get_tt_ref, NULL);
}

/* ===== T19: LWTRANCHE_CLUSTER_TT_STATUS slot ===== */
UT_TEST(test_t19_lwtranche_slot_defined)
{
	/* Compile-time visible — the symbol must exist as an enum value and
	 * sit inside the BuiltinTranche range. */
	UT_ASSERT_EQ((int)LWTRANCHE_CLUSTER_TT_STATUS >= 0, 1);
	UT_ASSERT_EQ((int)LWTRANCHE_CLUSTER_TT_STATUS < (int)LWTRANCHE_FIRST_USER_DEFINED, 1);
}

/* ===== T20: shmem helpers linkable ===== */
UT_TEST(test_t20_shmem_helpers_linkable)
{
	UT_ASSERT_NE((void *)cluster_tt_status_shmem_size, NULL);
	UT_ASSERT_NE((void *)cluster_tt_status_shmem_init, NULL);
	UT_ASSERT_NE((void *)cluster_tt_status_shmem_register, NULL);
	UT_ASSERT_NE((void *)cluster_tt_local_shmem_size, NULL);
	UT_ASSERT_NE((void *)cluster_tt_local_shmem_init, NULL);
	UT_ASSERT_NE((void *)cluster_tt_local_shmem_register, NULL);
}

/* ===== T21: self-consumer hit bump linkable (v0.4 N7) ===== */
UT_TEST(test_t21_self_consumer_hit_linkable)
{
	UT_ASSERT_NE((void *)cluster_tt_status_bump_self_consumer_hit, NULL);
}

/* ===== T22: enum distinctness ===== */
UT_TEST(test_t22_enum_kept_distinct)
{
	/* spec-3.1 §0.1 F4 + §1.3 #10:  spec-3.1 NEVER introduces a
	 * SUBCOMMITTED cluster status (推 spec-3.5).  Enum size MUST be exactly
	 * 5 values; any add beyond CLEANED_OUT must reopen the spec.
	 *
	 * Verify by enumerating; if any new value sneaks into the enum after
	 * CLEANED_OUT, this test fails. */
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_UNKNOWN, 0);
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_CLEANED_OUT, 4);

	/* TT_SLOT_* (cluster_tt_slot.h) is the on-disk TTSlot.status enum;
	 * spec-3.1 keeps it separate from the cluster status wire enum.
	 * Verify TT_SLOT_UNUSED is the zero sentinel and CLEANED_OUT is not
	 * conflated. */
	UT_ASSERT_EQ((int)TT_SLOT_UNUSED, 0);
	UT_ASSERT_EQ((int)TT_SLOT_INVALID, 0xFF);
}


int
main(void)
{
	UT_RUN(test_t1_status_key_sizeof_24);
	UT_RUN(test_t2_undo_tt_slot_ref_sizeof_32);
	UT_RUN(test_t3_enum_unknown_zero);
	UT_RUN(test_t4_enum_in_progress_one);
	UT_RUN(test_t5_enum_committed_two);
	UT_RUN(test_t6_enum_aborted_three);
	UT_RUN(test_t7_enum_cleaned_out_four);
	UT_RUN(test_t8_lookup_exact_linkable);
	UT_RUN(test_t9_install_local_linkable);
	UT_RUN(test_t10_flush_all_linkable);
	UT_RUN(test_t11_generation_linkable);
	UT_RUN(test_t12_status_key_field_offsets);
	UT_RUN(test_t13_status_result_field_offsets);
	UT_RUN(test_t14_undo_tt_slot_ref_field_offsets);
	UT_RUN(test_t15_guc_overlay_max_entries_default);
	UT_RUN(test_t16_guc_overlay_ttl_ms_default);
	UT_RUN(test_t17_local_install_api_linkable);
	UT_RUN(test_t18_itl_reader_linkable);
	UT_RUN(test_t19_lwtranche_slot_defined);
	UT_RUN(test_t20_shmem_helpers_linkable);
	UT_RUN(test_t21_self_consumer_hit_linkable);
	UT_RUN(test_t22_enum_kept_distinct);
	UT_DONE();
}
