/*-------------------------------------------------------------------------
 *
 * test_cluster_sf_dep.c
 *	  spec-6.2 Smart Fusion dependency-vector unit tests.
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>

#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_sf_dep.h"
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

UT_TEST(test_vec_set_union_and_clear)
{
	ClusterSfDepVec a;
	ClusterSfDepVec b;

	cluster_sf_dep_vec_reset(&a);
	cluster_sf_dep_vec_reset(&b);
	UT_ASSERT(cluster_sf_dep_vec_is_empty(&a));

	UT_ASSERT(cluster_sf_dep_vec_set(&a, 1, (XLogRecPtr)100));
	UT_ASSERT(!cluster_sf_dep_vec_is_empty(&a));
	UT_ASSERT_EQ((uint64)a.required[1], (uint64)100);

	UT_ASSERT(cluster_sf_dep_vec_set(&a, 1, (XLogRecPtr)90));
	UT_ASSERT_EQ((uint64)a.required[1], (uint64)100);
	UT_ASSERT(cluster_sf_dep_vec_set(&a, 1, (XLogRecPtr)120));
	UT_ASSERT_EQ((uint64)a.required[1], (uint64)120);

	UT_ASSERT(cluster_sf_dep_vec_set(&b, 2, (XLogRecPtr)55));
	UT_ASSERT(cluster_sf_dep_vec_set(&b, 1, (XLogRecPtr)110));
	UT_ASSERT(cluster_sf_dep_vec_union(&a, &b));
	UT_ASSERT_EQ((uint64)a.required[1], (uint64)120);
	UT_ASSERT_EQ((uint64)a.required[2], (uint64)55);

	UT_ASSERT(!cluster_sf_dep_vec_clear_durable(&a, 1, (XLogRecPtr)119));
	UT_ASSERT_EQ((uint64)a.required[1], (uint64)120);
	UT_ASSERT(cluster_sf_dep_vec_clear_durable(&a, 1, (XLogRecPtr)120));
	UT_ASSERT(XLogRecPtrIsInvalid(a.required[1]));
	UT_ASSERT(!cluster_sf_dep_vec_is_empty(&a));
	UT_ASSERT(cluster_sf_dep_vec_clear_durable(&a, 2, (XLogRecPtr)56));
	UT_ASSERT(cluster_sf_dep_vec_is_empty(&a));
}

UT_TEST(test_vec_rejects_invalid_origin_and_lsn)
{
	ClusterSfDepVec v;

	cluster_sf_dep_vec_reset(&v);
	UT_ASSERT(!cluster_sf_dep_vec_set(&v, -1, (XLogRecPtr)1));
	UT_ASSERT(!cluster_sf_dep_vec_set(&v, CLUSTER_SF_DEP_MAX_ORIGINS, (XLogRecPtr)1));
	UT_ASSERT(!cluster_sf_dep_vec_set(&v, 0, InvalidXLogRecPtr));
	UT_ASSERT(cluster_sf_dep_vec_is_empty(&v));
}

UT_TEST(test_smart_fusion_lwlock_tranche)
{
	UT_ASSERT_EQ((int)LWTRANCHE_CLUSTER_SMART_FUSION, (int)LWTRANCHE_CLUSTER_IC_RDMA + 1);
	UT_ASSERT((int)LWTRANCHE_CLUSTER_SMART_FUSION < (int)LWTRANCHE_FIRST_USER_DEFINED);
}

UT_TEST(test_gcs_block_reply_v2_layout)
{
	UT_ASSERT_EQ((int)sizeof(GcsBlockReplyHeader), 48);
	UT_ASSERT_EQ((int)offsetof(GcsBlockReplyHeaderV2, sf_flags), 48);
	UT_ASSERT_EQ((int)offsetof(GcsBlockReplyHeaderV2, sf_dep), 56);
	UT_ASSERT_EQ((int)sizeof(GcsBlockReplySfDep), 16);
	UT_ASSERT_EQ((int)sizeof(GcsBlockReplyHeaderV2), 56 + CLUSTER_SF_DEP_MAX_ORIGINS * 16);
}

UT_TEST(test_gcs_block_reply_v2_dep_extract_valid)
{
	GcsBlockReplyHeaderV2 hdr;
	ClusterSfDepVec vec;

	memset(&hdr, 0, sizeof(hdr));
	hdr.sf_flags = GCS_BLOCK_REPLY_SF_EARLY_TRANSFER | GCS_BLOCK_REPLY_SF_HAS_DEP_VEC;
	hdr.sf_dep_count = 2;
	hdr.sf_dep[0].origin_node = 1;
	hdr.sf_dep[0].required_redo_lsn = 100;
	hdr.sf_dep[1].origin_node = 3;
	hdr.sf_dep[1].required_redo_lsn = 90;

	UT_ASSERT(cluster_gcs_block_reply_v2_extract_dep_vec(&hdr, &vec));
	UT_ASSERT_EQ((uint64)vec.required[1], (uint64)100);
	UT_ASSERT_EQ((uint64)vec.required[3], (uint64)90);
	UT_ASSERT(XLogRecPtrIsInvalid(vec.required[0]));
}

UT_TEST(test_gcs_block_reply_v2_dep_extract_rejects_malformed)
{
	GcsBlockReplyHeaderV2 hdr;
	ClusterSfDepVec vec;

	memset(&hdr, 0, sizeof(hdr));
	hdr.sf_flags = GCS_BLOCK_REPLY_SF_HAS_DEP_VEC;
	hdr.sf_dep_count = 1;
	hdr.sf_dep[0].origin_node = 1;
	hdr.sf_dep[0].required_redo_lsn = 100;
	UT_ASSERT(!cluster_gcs_block_reply_v2_extract_dep_vec(&hdr, &vec));

	memset(&hdr, 0, sizeof(hdr));
	hdr.sf_flags = GCS_BLOCK_REPLY_SF_EARLY_TRANSFER | GCS_BLOCK_REPLY_SF_HAS_DEP_VEC | 0x80;
	hdr.sf_dep_count = 1;
	hdr.sf_dep[0].origin_node = 1;
	hdr.sf_dep[0].required_redo_lsn = 100;
	UT_ASSERT(!cluster_gcs_block_reply_v2_extract_dep_vec(&hdr, &vec));

	memset(&hdr, 0, sizeof(hdr));
	hdr.sf_flags = GCS_BLOCK_REPLY_SF_EARLY_TRANSFER | GCS_BLOCK_REPLY_SF_HAS_DEP_VEC;
	hdr.sf_dep_count = 2;
	hdr.sf_dep[0].origin_node = 1;
	hdr.sf_dep[0].required_redo_lsn = 100;
	hdr.sf_dep[1].origin_node = 1;
	hdr.sf_dep[1].required_redo_lsn = 101;
	UT_ASSERT(!cluster_gcs_block_reply_v2_extract_dep_vec(&hdr, &vec));

	memset(&hdr, 0, sizeof(hdr));
	hdr.sf_flags = GCS_BLOCK_REPLY_SF_EARLY_TRANSFER | GCS_BLOCK_REPLY_SF_HAS_DEP_VEC;
	hdr.sf_dep_count = 1;
	hdr.sf_dep[1].origin_node = 2;
	hdr.sf_dep[1].required_redo_lsn = 100;
	UT_ASSERT(!cluster_gcs_block_reply_v2_extract_dep_vec(&hdr, &vec));
}

UT_TEST(test_gcs_block_reply_v2_dep_extract_accepts_empty_v2_no_dep)
{
	GcsBlockReplyHeaderV2 hdr;
	ClusterSfDepVec vec;

	memset(&hdr, 0, sizeof(hdr));
	UT_ASSERT(cluster_gcs_block_reply_v2_extract_dep_vec(&hdr, &vec));
	UT_ASSERT(cluster_sf_dep_vec_is_empty(&vec));
}

int
main(void)
{
	UT_RUN(test_vec_set_union_and_clear);
	UT_RUN(test_vec_rejects_invalid_origin_and_lsn);
	UT_RUN(test_smart_fusion_lwlock_tranche);
	UT_RUN(test_gcs_block_reply_v2_layout);
	UT_RUN(test_gcs_block_reply_v2_dep_extract_valid);
	UT_RUN(test_gcs_block_reply_v2_dep_extract_rejects_malformed);
	UT_RUN(test_gcs_block_reply_v2_dep_extract_accepts_empty_v2_no_dep);
	UT_DONE();
}
