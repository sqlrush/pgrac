/*-------------------------------------------------------------------------
 *
 * test_cluster_snapshot_source.c
 *	  pgrac spec-3.3 D12 — cluster_unit static + ABI tests for the
 *	  SnapshotData explicit 24B cluster tail (D1) and ClusterTTStatusHint
 *	  V1/V2 wire layout (D8).
 *
 *	  18 tests (spec-3.3 §1.2 D12):
 *	    T1   SnapshotSource enum values stable (LOCAL=0, CLUSTER=1)
 *	    T2   SnapshotData has read_scn / read_epoch / cluster_source /
 *	         _pad members (compile-time presence)
 *	    T3   read_scn is SCN (uint64) size
 *	    T4   read_epoch is uint64 (R9 P2: no uint32 wrap alias)
 *	    T5   cluster_source is uint8
 *	    T6   _pad[6] is 6 bytes (R4 P1 explicit padding; spec-3.24 D1 took 1B)
 *	    T7   cluster tail total occupies 24 bytes when measured
 *	         offsetof(_pad)+sizeof(_pad) - offsetof(read_scn)
 *	    T8   sizeof(SnapshotData) is a multiple of 8 bytes (R4 P1 no
 *	         hidden compiler padding inside the tail)
 *	    T9   LOCAL and CLUSTER source values are mutually distinct
 *	    T10  default-zeroed SnapshotData yields LOCAL semantics
 *	         (cluster_source=0, read_scn=0=InvalidScn, read_epoch=0)
 *	    T11  ClusterTTStatusHintMsgV1 sizeof = 32B
 *	    T12  ClusterTTStatusHintMsgV1 key offset = 8
 *	    T13  ClusterTTStatusHintMsgV2 sizeof = 40B (D8 wire ABI lock)
 *	    T14  ClusterTTStatusHintMsgV2 key offset = 8
 *	    T15  ClusterTTStatusHintMsgV2 commit_scn offset = 32 (D8 wire
 *	         ABI lock)
 *	    T16  ClusterTTStatusHintVersion has V1=1 and V2=2 stable
 *	    T17  Legacy typedef ClusterTTStatusHintMsg == V1 (32B)
 *	    T18  Snapshot CLUSTER+InvalidScn pair is representable (manual
 *	         construction does not overflow uint8 cluster_source)
 *	    T19  cluster_snapshot_session_local is uint8 (spec-3.24 D1)
 *
 *	  Standalone executable; no PG backend required.
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_snapshot_source.c
 *
 * Spec: spec-3.3-snapshot-consistency-cross-node.md (v1.0 FROZEN 2026-05-23)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>
#include <string.h>

#include "cluster/cluster_scn.h"
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_tt_status_hint.h"
#include "utils/snapshot.h"

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


UT_TEST(test_t1_snapshot_source_enum_stable)
{
	UT_ASSERT_EQ((int)SNAPSHOT_SOURCE_LOCAL, 0);
	UT_ASSERT_EQ((int)SNAPSHOT_SOURCE_CLUSTER, 1);
}

UT_TEST(test_t2_snapshot_cluster_tail_members_present)
{
	SnapshotData s;

	memset(&s, 0, sizeof(s));
	/*
	 * Touch each field to require it at compile time. Any future rename
	 * or removal fails the build before this test runs.
	 */
	s.read_scn = InvalidScn;
	s.read_epoch = 0;
	s.cluster_source = (uint8)SNAPSHOT_SOURCE_LOCAL;
	s.cluster_snapshot_session_local = 0;
	s._pad[0] = 0;
	s._pad[5] = 0;
	UT_ASSERT_EQ((int)s.cluster_source, 0);
}

UT_TEST(test_t3_read_scn_is_scn)
{
	UT_ASSERT_EQ((int)sizeof(((SnapshotData *)0)->read_scn), (int)sizeof(SCN));
	UT_ASSERT_EQ((int)sizeof(SCN), 8);
}

UT_TEST(test_t4_read_epoch_is_uint64)
{
	UT_ASSERT_EQ((int)sizeof(((SnapshotData *)0)->read_epoch), 8);
}

UT_TEST(test_t5_cluster_source_is_uint8)
{
	UT_ASSERT_EQ((int)sizeof(((SnapshotData *)0)->cluster_source), 1);
}

UT_TEST(test_t6_pad_is_six_bytes)
{
	/* spec-3.24 D1 repurposed one _pad byte to cluster_snapshot_session_local. */
	UT_ASSERT_EQ((int)sizeof(((SnapshotData *)0)->_pad), 6);
}

UT_TEST(test_t19_session_local_is_uint8)
{
	UT_ASSERT_EQ((int)sizeof(((SnapshotData *)0)->cluster_snapshot_session_local), 1);
}

UT_TEST(test_t7_cluster_tail_spans_24_bytes)
{
	int start = (int)offsetof(SnapshotData, read_scn);
	int pad_end = (int)(offsetof(SnapshotData, _pad) + sizeof(((SnapshotData *)0)->_pad));

	UT_ASSERT_EQ(pad_end - start, 24);
}

UT_TEST(test_t8_snapshot_struct_multiple_of_8)
{
	UT_ASSERT_EQ((int)(sizeof(SnapshotData) % 8), 0);
}

UT_TEST(test_t9_local_cluster_distinct)
{
	UT_ASSERT_NE((int)SNAPSHOT_SOURCE_LOCAL, (int)SNAPSHOT_SOURCE_CLUSTER);
}

UT_TEST(test_t10_zeroed_snapshot_is_local)
{
	SnapshotData s;

	memset(&s, 0, sizeof(s));
	UT_ASSERT_EQ((int)s.cluster_source, (int)SNAPSHOT_SOURCE_LOCAL);
	UT_ASSERT_EQ((int)s.read_scn, (int)InvalidScn);
	UT_ASSERT_EQ((int)s.read_epoch, 0);
}

UT_TEST(test_t11_hint_v1_sizeof_32)
{
	UT_ASSERT_EQ((int)sizeof(ClusterTTStatusHintMsgV1), 32);
}

UT_TEST(test_t12_hint_v1_key_offset_8)
{
	UT_ASSERT_EQ((int)offsetof(ClusterTTStatusHintMsgV1, key), 8);
}

UT_TEST(test_t13_hint_v2_sizeof_40)
{
	UT_ASSERT_EQ((int)sizeof(ClusterTTStatusHintMsgV2), 40);
}

UT_TEST(test_t14_hint_v2_key_offset_8)
{
	UT_ASSERT_EQ((int)offsetof(ClusterTTStatusHintMsgV2, key), 8);
}

UT_TEST(test_t15_hint_v2_commit_scn_offset_32)
{
	UT_ASSERT_EQ((int)offsetof(ClusterTTStatusHintMsgV2, commit_scn), 32);
}

UT_TEST(test_t16_hint_version_values_stable)
{
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_HINT_V1, 1);
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_HINT_V2, 2);
}

UT_TEST(test_t17_legacy_typedef_is_v1)
{
	UT_ASSERT_EQ((int)sizeof(ClusterTTStatusHintMsg), (int)sizeof(ClusterTTStatusHintMsgV1));
}

UT_TEST(test_t18_cluster_local_invalidscn_representable)
{
	SnapshotData s;

	memset(&s, 0, sizeof(s));
	s.cluster_source = (uint8)SNAPSHOT_SOURCE_CLUSTER;
	s.read_scn = InvalidScn;
	s.read_epoch = (uint64)1;
	UT_ASSERT_EQ((int)s.cluster_source, 1);
	UT_ASSERT_EQ((int)s.read_scn, (int)InvalidScn);
	UT_ASSERT_EQ((int)s.read_epoch, 1);
}


int
main(void)
{
	UT_RUN(test_t1_snapshot_source_enum_stable);
	UT_RUN(test_t2_snapshot_cluster_tail_members_present);
	UT_RUN(test_t3_read_scn_is_scn);
	UT_RUN(test_t4_read_epoch_is_uint64);
	UT_RUN(test_t5_cluster_source_is_uint8);
	UT_RUN(test_t6_pad_is_six_bytes);
	UT_RUN(test_t19_session_local_is_uint8);
	UT_RUN(test_t7_cluster_tail_spans_24_bytes);
	UT_RUN(test_t8_snapshot_struct_multiple_of_8);
	UT_RUN(test_t9_local_cluster_distinct);
	UT_RUN(test_t10_zeroed_snapshot_is_local);
	UT_RUN(test_t11_hint_v1_sizeof_32);
	UT_RUN(test_t12_hint_v1_key_offset_8);
	UT_RUN(test_t13_hint_v2_sizeof_40);
	UT_RUN(test_t14_hint_v2_key_offset_8);
	UT_RUN(test_t15_hint_v2_commit_scn_offset_32);
	UT_RUN(test_t16_hint_version_values_stable);
	UT_RUN(test_t17_legacy_typedef_is_v1);
	UT_RUN(test_t18_cluster_local_invalidscn_representable);
	UT_DONE();
}
