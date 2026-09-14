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

#include "access/xlog.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_grd_outbound.h"
#include "cluster/cluster_sf_dep.h"
#include "storage/lwlock.h"
#include "utils/timestamp.h"

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

#define TEST_SF_CAP_PEER 7
#define TEST_SF_SHMEM_BYTES 8192
#define TEST_R4_REQUIRED_CAPS                                                                  \
	(PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1 | PGRAC_IC_HELLO_CAP_R4_SYNC_CR_V1              \
	 | PGRAC_IC_HELLO_CAP_CANDIDATE2_CORRECTED_A1_V1                                          \
	 | PGRAC_IC_HELLO_CAP_UNDO_ROOT_DESCRIPTOR_V1)
#define TEST_STAGE8_ACK_REQUIRED_CAPS                                                         \
	(TEST_R4_REQUIRED_CAPS | PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_ACK_V1)

typedef union TestSfShmemStorage {
	LWLock align;
	uint64 atomic_align;
	uint8 bytes[TEST_SF_SHMEM_BYTES];
} TestSfShmemStorage;

static TestSfShmemStorage test_sf_shmem;

ProcessingMode Mode = NormalProcessing;
bool cluster_enabled = true;
bool cluster_smart_fusion = false;
int NBuffers = 0;
int NLocBuffer = 0;
int cluster_node_id = 0;
int cluster_smart_fusion_origin_durable_gossip_ms = 50;
static XLogRecPtr test_sf_flush = 3500;
static bool test_sf_recovery;
static unsigned test_sf_flush_reads;
static unsigned test_sf_sends;
static bool test_sf_declared = true;
static bool test_sf_enqueue_ok = true;
static ClusterSfDurableGossipMsg test_sf_message;
static TimestampTz test_sf_now;

TimestampTz
GetCurrentTimestamp(void)
{
	return test_sf_now;
}

XLogRecPtr
GetFlushRecPtr(TimeLineID *timeline)
{
	test_sf_flush_reads++;
	UT_ASSERT(!test_sf_recovery);
	if (timeline != NULL)
		*timeline = 1;
	return test_sf_flush;
}

bool
RecoveryInProgress(void)
{
	return test_sf_recovery;
}

const ClusterNodeInfo *
cluster_conf_lookup_node(int32 node_id)
{
	static const ClusterNodeInfo peer = { 0 };
	return test_sf_declared && node_id == TEST_SF_CAP_PEER ? &peer : NULL;
}

bool
cluster_grd_outbound_enqueue_backend_msg(uint8 type, uint32 destination, const void *payload,
										 uint16 length)
{
	Assert(type == PGRAC_IC_MSG_SMART_FUSION_DURABLE);
	Assert(destination == TEST_SF_CAP_PEER && length == sizeof(test_sf_message));
	memcpy(&test_sf_message, payload, length);
	test_sf_sends++;
	return test_sf_enqueue_ok;
}

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *foundPtr)
{
	UT_ASSERT(size <= sizeof(test_sf_shmem.bytes));
	*foundPtr = false;
	return test_sf_shmem.bytes;
}

void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}

bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	return true;
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{}

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	fprintf(stderr, "assertion %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

static void
test_sf_cap_store_reset(void)
{
	memset(&test_sf_shmem, 0, sizeof(test_sf_shmem));
	cluster_sf_dep_shmem_init();
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

/*
 * spec-2.2 additive amendment (spec-5.22e D5 prereq, B4): the per-peer HELLO
 * capability record is generation-bound.  A clear only applies when the
 * caller's connection generation matches the generation recorded at learn
 * time, so a defensive close of a failed dial or of an OLDER connection can
 * never wipe the surviving connection's capability record.
 */
UT_TEST(test_peer_cap_gen_note_query_invalidate)
{
	ClusterSfPeerCap cap;

	memset(&cap, 0, sizeof(cap));

	/* an unset record reads as "no capability" (UNKNOWN) */
	UT_ASSERT_EQ(cluster_sf_peer_cap_bits(&cap), (uint32)0);

	/* note stamps bits + generation and turns the record valid */
	cluster_sf_peer_cap_note(&cap, (uint32)0x0E, (uint32)3);
	UT_ASSERT_EQ(cluster_sf_peer_cap_bits(&cap), (uint32)0x0E);

	/* mismatched-generation invalidate must NOT clear the record */
	UT_ASSERT(!cluster_sf_peer_cap_invalidate_gen(&cap, (uint32)2));
	UT_ASSERT_EQ(cluster_sf_peer_cap_bits(&cap), (uint32)0x0E);
	UT_ASSERT(!cluster_sf_peer_cap_invalidate_gen(&cap, (uint32)4));
	UT_ASSERT_EQ(cluster_sf_peer_cap_bits(&cap), (uint32)0x0E);

	/* matching generation clears exactly once */
	UT_ASSERT(cluster_sf_peer_cap_invalidate_gen(&cap, (uint32)3));
	UT_ASSERT_EQ(cluster_sf_peer_cap_bits(&cap), (uint32)0);
	UT_ASSERT(!cluster_sf_peer_cap_invalidate_gen(&cap, (uint32)3));
}

UT_TEST(test_peer_cap_gen_renote_after_reconnect)
{
	ClusterSfPeerCap cap;

	memset(&cap, 0, sizeof(cap));

	/* connection gen 3 learns, closes (matched clear), gen 4 relearns */
	cluster_sf_peer_cap_note(&cap, (uint32)0x0E, (uint32)3);
	UT_ASSERT(cluster_sf_peer_cap_invalidate_gen(&cap, (uint32)3));
	cluster_sf_peer_cap_note(&cap, (uint32)0x04, (uint32)4);
	UT_ASSERT_EQ(cluster_sf_peer_cap_bits(&cap), (uint32)0x04);

	/* a straggler clear for the OLD generation must not touch gen 4 */
	UT_ASSERT(!cluster_sf_peer_cap_invalidate_gen(&cap, (uint32)3));
	UT_ASSERT_EQ(cluster_sf_peer_cap_bits(&cap), (uint32)0x04);

	/* same-generation re-note (HELLO then CAPS_REPLY on one connection)
	 * is a plain overwrite, not an error */
	cluster_sf_peer_cap_note(&cap, (uint32)0x0E, (uint32)4);
	UT_ASSERT_EQ(cluster_sf_peer_cap_bits(&cap), (uint32)0x0E);
	UT_ASSERT(cluster_sf_peer_cap_invalidate_gen(&cap, (uint32)4));
	UT_ASSERT_EQ(cluster_sf_peer_cap_bits(&cap), (uint32)0);
}

UT_TEST(test_pcm_x_capability_does_not_alias_idle_horizon)
{
	ClusterSfPeerCap cap;

	memset(&cap, 0, sizeof(cap));
	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_UNDO_HORIZON_IDLE_V1, (uint32)0x00000100U);
	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_PCM_X_CONVERT_V1, (uint32)0x00000200U);
	UT_ASSERT((PGRAC_IC_HELLO_CAP_UNDO_HORIZON_IDLE_V1 & PGRAC_IC_HELLO_CAP_PCM_X_CONVERT_V1) == 0);
	cluster_sf_peer_cap_note(&cap, PGRAC_IC_HELLO_CAP_UNDO_HORIZON_IDLE_V1, 7);
	UT_ASSERT((cluster_sf_peer_cap_bits(&cap) & PGRAC_IC_HELLO_CAP_PCM_X_CONVERT_V1) == 0);
	cluster_sf_peer_cap_note(&cap, PGRAC_IC_HELLO_CAP_PCM_X_CONVERT_V1, 8);
	UT_ASSERT((cluster_sf_peer_cap_bits(&cap) & PGRAC_IC_HELLO_CAP_PCM_X_CONVERT_V1) != 0);
}

UT_TEST(test_pcm_x_capability_generation_snapshot_is_exact)
{
	ClusterSfPeerCap cap;
	uint32 generation = UINT32_MAX;

	memset(&cap, 0, sizeof(cap));
	UT_ASSERT(!cluster_sf_peer_cap_generation_for_bits(&cap, PGRAC_IC_HELLO_CAP_PCM_X_CONVERT_V1,
													   &generation));
	UT_ASSERT_EQ(generation, (uint32)0);

	cluster_sf_peer_cap_note(&cap, PGRAC_IC_HELLO_CAP_UNDO_HORIZON_IDLE_V1, 7);
	generation = UINT32_MAX;
	UT_ASSERT(!cluster_sf_peer_cap_generation_for_bits(&cap, PGRAC_IC_HELLO_CAP_PCM_X_CONVERT_V1,
													   &generation));
	UT_ASSERT_EQ(generation, (uint32)0);

	cluster_sf_peer_cap_note(&cap, PGRAC_IC_HELLO_CAP_PCM_X_CONVERT_V1, 8);
	UT_ASSERT(cluster_sf_peer_cap_generation_for_bits(&cap, PGRAC_IC_HELLO_CAP_PCM_X_CONVERT_V1,
													  &generation));
	UT_ASSERT_EQ(generation, (uint32)8);

	UT_ASSERT(cluster_sf_peer_cap_invalidate_gen(&cap, 8));
	generation = UINT32_MAX;
	UT_ASSERT(!cluster_sf_peer_cap_generation_for_bits(&cap, PGRAC_IC_HELLO_CAP_PCM_X_CONVERT_V1,
													   &generation));
	UT_ASSERT_EQ(generation, (uint32)0);
}

/* review P0-2: the family sample returns required-bit support, the optional
 * bit and the record generation from one record read; a reconnect renote is
 * visible as a generation move together with its bits. */
UT_TEST(test_pcm_x_capability_family_sample_is_record_coherent)
{
	ClusterSfPeerCap cap;
	uint32 generation = UINT32_MAX;
	bool rebase = true;

	memset(&cap, 0, sizeof(cap));
	UT_ASSERT(!cluster_sf_peer_cap_family_sample(&cap, PGRAC_IC_HELLO_CAP_PCM_X_CONVERT_V1,
												 PGRAC_IC_HELLO_CAP_PCM_X_REBASE_V1, &rebase,
												 &generation));
	UT_ASSERT(!rebase);
	UT_ASSERT_EQ(generation, (uint32)0);

	/* CONVERT without REBASE: supported, optional bit false. */
	cluster_sf_peer_cap_note(&cap, PGRAC_IC_HELLO_CAP_PCM_X_CONVERT_V1, 7);
	rebase = true;
	UT_ASSERT(cluster_sf_peer_cap_family_sample(&cap, PGRAC_IC_HELLO_CAP_PCM_X_CONVERT_V1,
												PGRAC_IC_HELLO_CAP_PCM_X_REBASE_V1, &rebase,
												&generation));
	UT_ASSERT(!rebase);
	UT_ASSERT_EQ(generation, (uint32)7);

	/* Reconnect renote with both bits: the generation moves with the bits. */
	cluster_sf_peer_cap_note(
		&cap, PGRAC_IC_HELLO_CAP_PCM_X_CONVERT_V1 | PGRAC_IC_HELLO_CAP_PCM_X_REBASE_V1, 8);
	UT_ASSERT(cluster_sf_peer_cap_family_sample(&cap, PGRAC_IC_HELLO_CAP_PCM_X_CONVERT_V1,
												PGRAC_IC_HELLO_CAP_PCM_X_REBASE_V1, &rebase,
												&generation));
	UT_ASSERT(rebase);
	UT_ASSERT_EQ(generation, (uint32)8);

	/* REBASE alone never satisfies the family requirement. */
	cluster_sf_peer_cap_note(&cap, PGRAC_IC_HELLO_CAP_PCM_X_REBASE_V1, 9);
	rebase = true;
	UT_ASSERT(!cluster_sf_peer_cap_family_sample(&cap, PGRAC_IC_HELLO_CAP_PCM_X_CONVERT_V1,
												 PGRAC_IC_HELLO_CAP_PCM_X_REBASE_V1, &rebase,
												 &generation));
	UT_ASSERT(!rebase);
	UT_ASSERT_EQ(generation, (uint32)0);

	/* Disconnect invalidation clears the whole family. */
	cluster_sf_peer_cap_note(
		&cap, PGRAC_IC_HELLO_CAP_PCM_X_CONVERT_V1 | PGRAC_IC_HELLO_CAP_PCM_X_REBASE_V1, 10);
	UT_ASSERT(cluster_sf_peer_cap_invalidate_gen(&cap, 10));
	rebase = true;
	UT_ASSERT(!cluster_sf_peer_cap_family_sample(&cap, PGRAC_IC_HELLO_CAP_PCM_X_CONVERT_V1,
												 PGRAC_IC_HELLO_CAP_PCM_X_REBASE_V1, &rebase,
												 &generation));
	UT_ASSERT(!rebase);
}

UT_TEST(test_pcm_x_source_floor_capability_guard_is_generation_exact)
{
	ClusterSfPeerCap cap;
	const uint32 family
		= PGRAC_IC_HELLO_CAP_PCM_X_CONVERT_V1 | PGRAC_IC_HELLO_CAP_PCM_X_SOURCE_FLOOR_V1;

	memset(&cap, 0, sizeof(cap));
	cluster_sf_peer_cap_note(&cap, family, 41);
	UT_ASSERT(cluster_sf_peer_cap_generation_matches_exact(
		&cap, PGRAC_IC_HELLO_CAP_PCM_X_SOURCE_FLOOR_V1, 41));
	UT_ASSERT(!cluster_sf_peer_cap_generation_matches_exact(
		&cap, PGRAC_IC_HELLO_CAP_PCM_X_SOURCE_FLOOR_V1, 42));
	cluster_sf_peer_cap_note(&cap, PGRAC_IC_HELLO_CAP_PCM_X_CONVERT_V1, 42);
	UT_ASSERT(!cluster_sf_peer_cap_generation_matches_exact(
		&cap, PGRAC_IC_HELLO_CAP_PCM_X_SOURCE_FLOOR_V1, 42));
}

UT_TEST(test_r4_exported_family_sample_requires_both_bits_and_canonicalizes_outputs)
{
	static const struct {
		uint32 bits;
		uint32 noted_generation;
		uint32 required;
		uint32 optional;
		bool want_supported;
		bool want_done;
		uint32 want_generation;
	} cases[] = {
		{PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1, 11, TEST_R4_REQUIRED_CAPS,
		 PGRAC_IC_HELLO_CAP_GCS_DONE_V1, false, false, 0},
		{PGRAC_IC_HELLO_CAP_R4_SYNC_CR_V1, 12, TEST_R4_REQUIRED_CAPS,
		 PGRAC_IC_HELLO_CAP_GCS_DONE_V1, false, false, 0},
		{PGRAC_IC_HELLO_CAP_GCS_DONE_V1, 13, TEST_R4_REQUIRED_CAPS,
		 PGRAC_IC_HELLO_CAP_GCS_DONE_V1, false, false, 0},
		{TEST_R4_REQUIRED_CAPS, 14, TEST_R4_REQUIRED_CAPS,
		 PGRAC_IC_HELLO_CAP_GCS_DONE_V1, true, false, 14},
		{TEST_R4_REQUIRED_CAPS | PGRAC_IC_HELLO_CAP_GCS_DONE_V1, 15,
		 TEST_R4_REQUIRED_CAPS, PGRAC_IC_HELLO_CAP_GCS_DONE_V1, true, true, 15},
		{TEST_R4_REQUIRED_CAPS | PGRAC_IC_HELLO_CAP_GCS_DONE_V1, 16, 0,
		 PGRAC_IC_HELLO_CAP_GCS_DONE_V1, false, false, 0},
		{TEST_R4_REQUIRED_CAPS | PGRAC_IC_HELLO_CAP_GCS_DONE_V1, 17,
		 TEST_R4_REQUIRED_CAPS, 0, true, false, 17},
	};
	bool done = true;
	uint32 generation = UINT32_MAX;
	Size i;

	test_sf_cap_store_reset();
	UT_ASSERT(!cluster_sf_peer_capability_family_sample(
		TEST_SF_CAP_PEER, TEST_R4_REQUIRED_CAPS, PGRAC_IC_HELLO_CAP_GCS_DONE_V1, &done,
		&generation));
	UT_ASSERT(!done);
	UT_ASSERT_EQ(generation, (uint32)0);

	for (i = 0; i < lengthof(cases); i++) {
		cluster_sf_note_peer_hello_capabilities_gen(TEST_SF_CAP_PEER, cases[i].bits,
												 cases[i].noted_generation);
		done = !cases[i].want_done;
		generation = UINT32_MAX;
		UT_ASSERT_EQ(cluster_sf_peer_capability_family_sample(
						 TEST_SF_CAP_PEER, cases[i].required, cases[i].optional, &done, &generation),
					 cases[i].want_supported);
		UT_ASSERT_EQ(done, cases[i].want_done);
		UT_ASSERT_EQ(generation, cases[i].want_generation);
	}

	done = true;
	generation = UINT32_MAX;
	UT_ASSERT(!cluster_sf_peer_capability_family_sample(
		-1, TEST_R4_REQUIRED_CAPS, PGRAC_IC_HELLO_CAP_GCS_DONE_V1, &done, &generation));
	UT_ASSERT(!done);
	UT_ASSERT_EQ(generation, (uint32)0);
}

UT_TEST(test_r4_exported_family_sample_accepts_registered_generation_zero)
{
	bool done = false;
	uint32 generation = UINT32_MAX;

	/* RF-ROOT P9 verification (verified implementation): the generation is the peer's
	 * tier1 reconnect_count, which is 0 for the FIRST verified connection;
	 * the semantic-activation gates require a NONZERO capability generation
	 * (0 = no verified record), so note() maps the first verified connection
	 * (registered 0) to generation 1. */
	test_sf_cap_store_reset();
	cluster_sf_note_peer_hello_capabilities_gen(
		TEST_SF_CAP_PEER, TEST_R4_REQUIRED_CAPS | PGRAC_IC_HELLO_CAP_GCS_DONE_V1, 0);
	UT_ASSERT(cluster_sf_peer_capability_family_sample(
		TEST_SF_CAP_PEER, TEST_R4_REQUIRED_CAPS, PGRAC_IC_HELLO_CAP_GCS_DONE_V1, &done,
		&generation));
	UT_ASSERT(done);
	UT_ASSERT_EQ(generation, (uint32)1);
	UT_ASSERT(cluster_sf_peer_capability_generation_matches(TEST_SF_CAP_PEER,
													 TEST_R4_REQUIRED_CAPS, 1));
}

UT_TEST(test_r4_exported_family_sample_reconnect_generation_is_exact)
{
	bool done = false;
	uint32 generation = UINT32_MAX;

	test_sf_cap_store_reset();
	cluster_sf_note_peer_hello_capabilities_gen(
		TEST_SF_CAP_PEER, TEST_R4_REQUIRED_CAPS | PGRAC_IC_HELLO_CAP_GCS_DONE_V1, 21);
	UT_ASSERT(cluster_sf_peer_capability_family_sample(
		TEST_SF_CAP_PEER, TEST_R4_REQUIRED_CAPS, PGRAC_IC_HELLO_CAP_GCS_DONE_V1, &done,
		&generation));
	UT_ASSERT(done);
	UT_ASSERT_EQ(generation, (uint32)21);
	UT_ASSERT(cluster_sf_peer_capability_generation_matches(TEST_SF_CAP_PEER,
													 TEST_R4_REQUIRED_CAPS, 21));

	cluster_sf_note_peer_hello_capabilities_gen(
		TEST_SF_CAP_PEER, TEST_R4_REQUIRED_CAPS | PGRAC_IC_HELLO_CAP_GCS_DONE_V1, 22);
	UT_ASSERT(!cluster_sf_peer_capability_generation_matches(TEST_SF_CAP_PEER,
													  TEST_R4_REQUIRED_CAPS, 21));
	UT_ASSERT(cluster_sf_peer_capability_generation_matches(TEST_SF_CAP_PEER,
													 TEST_R4_REQUIRED_CAPS, 22));
	done = false;
	generation = UINT32_MAX;
	UT_ASSERT(cluster_sf_peer_capability_family_sample(
		TEST_SF_CAP_PEER, TEST_R4_REQUIRED_CAPS, PGRAC_IC_HELLO_CAP_GCS_DONE_V1, &done,
		&generation));
	UT_ASSERT(done);
	UT_ASSERT_EQ(generation, (uint32)22);

	cluster_sf_note_peer_hello_capabilities_gen(TEST_SF_CAP_PEER,
											 PGRAC_IC_HELLO_CAP_GCS_DONE_V1, 23);
	done = true;
	generation = UINT32_MAX;
	UT_ASSERT(!cluster_sf_peer_capability_family_sample(
		TEST_SF_CAP_PEER, TEST_R4_REQUIRED_CAPS, PGRAC_IC_HELLO_CAP_GCS_DONE_V1, &done,
		&generation));
	UT_ASSERT(!done);
	UT_ASSERT_EQ(generation, (uint32)0);
	UT_ASSERT(!cluster_sf_peer_capability_generation_matches(TEST_SF_CAP_PEER,
													  TEST_R4_REQUIRED_CAPS, 23));

	cluster_sf_note_peer_disconnected_gen(TEST_SF_CAP_PEER, 22);
	UT_ASSERT(cluster_sf_peer_supports_gcs_done(TEST_SF_CAP_PEER));
	cluster_sf_note_peer_disconnected_gen(TEST_SF_CAP_PEER, 23);
	UT_ASSERT(!cluster_sf_peer_supports_gcs_done(TEST_SF_CAP_PEER));
}

UT_TEST(test_stage8_ack_full_word_sample_is_record_coherent)
{
	const uint32 full_word
		= TEST_STAGE8_ACK_REQUIRED_CAPS
		  | PGRAC_IC_HELLO_CAP_GCS_DONE_V1;
	uint32 sampled_word = UINT32_MAX;
	uint32 generation = UINT32_MAX;

	test_sf_cap_store_reset();
	UT_ASSERT(!cluster_sf_peer_capability_word_sample(
		TEST_SF_CAP_PEER, TEST_STAGE8_ACK_REQUIRED_CAPS,
		&sampled_word, &generation));
	UT_ASSERT_EQ(sampled_word, (uint32)0);
	UT_ASSERT_EQ(generation, (uint32)0);

	cluster_sf_note_peer_hello_capabilities_gen(
		TEST_SF_CAP_PEER, full_word, 41);
	UT_ASSERT(cluster_sf_peer_capability_word_sample(
		TEST_SF_CAP_PEER, TEST_STAGE8_ACK_REQUIRED_CAPS,
		&sampled_word, &generation));
	UT_ASSERT_EQ(sampled_word, full_word);
	UT_ASSERT_EQ(generation, (uint32)41);

	cluster_sf_note_peer_hello_capabilities_gen(
		TEST_SF_CAP_PEER,
		TEST_STAGE8_ACK_REQUIRED_CAPS
			& ~PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_ACK_V1,
		42);
	sampled_word = UINT32_MAX;
	generation = UINT32_MAX;
	UT_ASSERT(!cluster_sf_peer_capability_word_sample(
		TEST_SF_CAP_PEER, TEST_STAGE8_ACK_REQUIRED_CAPS,
		&sampled_word, &generation));
	UT_ASSERT_EQ(sampled_word, (uint32)0);
	UT_ASSERT_EQ(generation, (uint32)0);

	sampled_word = UINT32_MAX;
	generation = UINT32_MAX;
	UT_ASSERT(!cluster_sf_peer_capability_word_sample(
		-1, TEST_STAGE8_ACK_REQUIRED_CAPS,
		&sampled_word, &generation));
	UT_ASSERT_EQ(sampled_word, (uint32)0);
	UT_ASSERT_EQ(generation, (uint32)0);
}

UT_TEST(test_current_mx_capability_generation_sample_is_connection_exact)
{
	uint32 generation = UINT32_MAX;

	test_sf_cap_store_reset();
	UT_ASSERT(!cluster_sf_peer_multixact_current_capability_generation(
		TEST_SF_CAP_PEER, &generation));
	UT_ASSERT_EQ(generation, (uint32)0);

	/* The former 0x00001000 allocation belongs to semantic activation and
	 * must not admit the migrated Current-MX transport. */
	cluster_sf_note_peer_hello_capabilities_gen(
		TEST_SF_CAP_PEER, PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1, 72);
	generation = UINT32_MAX;
	UT_ASSERT(!cluster_sf_peer_multixact_current_capability_generation(
		TEST_SF_CAP_PEER, &generation));
	UT_ASSERT_EQ(generation, (uint32)0);

	cluster_sf_note_peer_hello_capabilities_gen(
		TEST_SF_CAP_PEER, PGRAC_IC_HELLO_CAP_MULTIXACT_CURRENT_V1, 73);
	UT_ASSERT(cluster_sf_peer_multixact_current_capability_generation(
		TEST_SF_CAP_PEER, &generation));
	UT_ASSERT_EQ(generation, (uint32)73);

	/* A reconnect that withdraws the bit invalidates both authority and the
	 * previously sampled generation. */
	cluster_sf_note_peer_hello_capabilities_gen(
		TEST_SF_CAP_PEER, PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1, 74);
	generation = UINT32_MAX;
	UT_ASSERT(!cluster_sf_peer_multixact_current_capability_generation(
		TEST_SF_CAP_PEER, &generation));
	UT_ASSERT_EQ(generation, (uint32)0);
}

UT_TEST(test_a89_capability_record_snapshot_distinguishes_unavailable_and_drift)
{
	ClusterSfPeerCap sample;

	memset(&sample, 0xff, sizeof(sample));
	UT_ASSERT(!cluster_sf_peer_capability_record_snapshot(TEST_SF_CAP_PEER, &sample));
	UT_ASSERT(!sample.valid);
	UT_ASSERT_EQ(sample.generation, 0);
	test_sf_cap_store_reset();
	UT_ASSERT(cluster_sf_peer_capability_record_snapshot(TEST_SF_CAP_PEER, &sample));
	UT_ASSERT(!sample.valid);
	cluster_sf_note_peer_hello_capabilities_gen(TEST_SF_CAP_PEER,
												PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1, 73);
	UT_ASSERT(cluster_sf_peer_capability_record_snapshot(TEST_SF_CAP_PEER, &sample));
	UT_ASSERT(sample.valid);
	UT_ASSERT_EQ(sample.generation, 73);
	UT_ASSERT_EQ(sample.bits, PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1);
	cluster_sf_note_peer_disconnected_gen(TEST_SF_CAP_PEER, 73);
	UT_ASSERT(cluster_sf_peer_capability_record_snapshot(TEST_SF_CAP_PEER, &sample));
	UT_ASSERT(!sample.valid);
	UT_ASSERT_EQ(sample.generation, 73); /* Invalidated is not a fresh incarnation. */
	cluster_sf_note_peer_hello_capabilities_gen(TEST_SF_CAP_PEER,
												PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1, 74);
	UT_ASSERT(cluster_sf_peer_capability_record_snapshot(TEST_SF_CAP_PEER, &sample));
	UT_ASSERT(sample.valid);
	UT_ASSERT_EQ(sample.generation, 74);
	UT_ASSERT_EQ(sample.bits, PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1);
	UT_ASSERT(!cluster_sf_peer_capability_record_snapshot(-1, &sample));
	UT_ASSERT(!sample.valid);
	UT_ASSERT_EQ(sample.generation, 0);
	UT_ASSERT(!cluster_sf_peer_capability_record_snapshot(TEST_SF_CAP_PEER, NULL));
}

UT_TEST(test_ordinary_ctrc_peer_receives_actual_durability_with_sf_off)
{
	ClusterICEnvelope envelope = { 0 };
	const uint32 caps
		= PGRAC_IC_HELLO_CAP_MULTIXACT_CURRENT_V1 | PGRAC_IC_HELLO_CAP_MULTIXACT_CTRC_V1;
	test_sf_cap_store_reset();
	cluster_smart_fusion = false;
	test_sf_sends = 0;
	cluster_sf_note_peer_hello_capabilities_gen(TEST_SF_CAP_PEER, caps, 1);
	UT_ASSERT(!cluster_sf_peer_supports_reply_v2(TEST_SF_CAP_PEER));
	cluster_sf_publish_origin_durable_lsn();
	UT_ASSERT_EQ(test_sf_sends, 1);
	UT_ASSERT_EQ(test_sf_message.durable_lsn, test_sf_flush);
	UT_ASSERT_EQ(test_sf_message.origin_node, cluster_node_id);
	UT_ASSERT_EQ(test_sf_message.flags, 0);
	/* Reinitialize the actual store as a receiver, then deliver the captured
	 * original message. This is a C producer/receiver test, not network I/O. */
	test_sf_cap_store_reset();
	UT_ASSERT(XLogRecPtrIsInvalid(cluster_sf_observed_origin_durable_lsn(0)));
	envelope.source_node_id = 0;
	envelope.payload_length = sizeof(test_sf_message);
	cluster_sf_handle_durable_gossip(&envelope, &test_sf_message);
	UT_ASSERT_EQ(cluster_sf_observed_origin_durable_lsn(0), test_sf_flush);
	UT_ASSERT(!cluster_smart_fusion);
}

UT_TEST(test_durable_message_preserves_sender_and_monotonicity)
{
	ClusterICEnvelope envelope = { 0 };
	ClusterSfDurableGossipMsg msg = { CLUSTER_SF_DURABLE_GOSSIP_VERSION, 0, 1, 4000 };
	test_sf_cap_store_reset();
	envelope.source_node_id = 2;
	envelope.payload_length = sizeof(msg);
	cluster_sf_handle_durable_gossip(&envelope, &msg);
	UT_ASSERT(XLogRecPtrIsInvalid(cluster_sf_observed_origin_durable_lsn(1)));
	envelope.source_node_id = 1;
	envelope.payload_length--;
	cluster_sf_handle_durable_gossip(&envelope, &msg);
	UT_ASSERT(XLogRecPtrIsInvalid(cluster_sf_observed_origin_durable_lsn(1)));
	envelope.payload_length++;
	msg.msg_version++;
	cluster_sf_handle_durable_gossip(&envelope, &msg);
	UT_ASSERT(XLogRecPtrIsInvalid(cluster_sf_observed_origin_durable_lsn(1)));
	msg.msg_version--;
	cluster_sf_handle_durable_gossip(&envelope, &msg);
	msg.durable_lsn = 3999;
	cluster_sf_handle_durable_gossip(&envelope, &msg);
	UT_ASSERT_EQ(cluster_sf_observed_origin_durable_lsn(1), 4000);
}

static void
test_sf_publish_setup(uint32 caps)
{
	cluster_smart_fusion = false;
	cluster_enabled = true;
	cluster_node_id = 0;
	test_sf_declared = true;
	test_sf_enqueue_ok = true;
	test_sf_flush = 3500;
	test_sf_recovery = false;
	test_sf_flush_reads = 0;
	test_sf_sends = 0;
	test_sf_cap_store_reset();
	cluster_sf_note_peer_hello_capabilities_gen(TEST_SF_CAP_PEER, caps, 1);
}

UT_TEST(test_durable_publisher_requires_declared_current_capability)
{
	const uint32 current = PGRAC_IC_HELLO_CAP_MULTIXACT_CURRENT_V1;
	const uint32 ctrc = PGRAC_IC_HELLO_CAP_MULTIXACT_CTRC_V1;
	for (unsigned leg = 0; leg < 7; leg++) {
		test_sf_publish_setup(leg == 0 ? 0 : leg == 1 ? current : leg == 2 ? ctrc : current | ctrc);
		if (leg == 3)
			cluster_sf_note_peer_disconnected_gen(TEST_SF_CAP_PEER, 1);
		if (leg == 4)
			test_sf_declared = false;
		if (leg == 5)
			cluster_node_id = -1;
		if (leg == 6)
			cluster_enabled = false;
		cluster_sf_publish_origin_durable_lsn();
		UT_ASSERT_EQ(test_sf_sends, 0);
	}
}

UT_TEST(test_durable_tick_retries_idle_loss_without_changing_deadlines)
{
	test_sf_publish_setup(PGRAC_IC_HELLO_CAP_MULTIXACT_CURRENT_V1
						  | PGRAC_IC_HELLO_CAP_MULTIXACT_CTRC_V1);
	test_sf_now = INT64CONST(100000000);
	cluster_sf_origin_durable_lmon_tick();
	UT_ASSERT_EQ(test_sf_sends, 1);
	test_sf_now += 49999;
	cluster_sf_origin_durable_lmon_tick();
	UT_ASSERT_EQ(test_sf_sends, 1);
	test_sf_now++;
	cluster_sf_origin_durable_lmon_tick();
	UT_ASSERT_EQ(test_sf_sends, 2);
	UT_ASSERT_EQ(test_sf_message.durable_lsn, 3500);
	test_sf_enqueue_ok = false;
	test_sf_now += 50000;
	cluster_sf_origin_durable_lmon_tick();
	UT_ASSERT_EQ(test_sf_sends, 3);
	test_sf_enqueue_ok = true;
	test_sf_now += 50000;
	cluster_sf_origin_durable_lmon_tick();
	UT_ASSERT_EQ(test_sf_sends, 4);
	test_sf_now--; /* Clock correction affects rate observation, not proof. */
	cluster_sf_origin_durable_lmon_tick();
	UT_ASSERT_EQ(test_sf_sends, 5);
	UT_ASSERT_EQ(test_sf_message.durable_lsn, 3500);
	UT_ASSERT(!cluster_smart_fusion);
}

UT_TEST(test_durable_tick_observes_background_flush_without_commit)
{
	test_sf_publish_setup(PGRAC_IC_HELLO_CAP_MULTIXACT_CURRENT_V1
						  | PGRAC_IC_HELLO_CAP_MULTIXACT_CTRC_V1);
	test_sf_now = INT64CONST(200000000);
	cluster_sf_origin_durable_lmon_tick();
	UT_ASSERT_EQ(test_sf_message.durable_lsn, 3500);
	test_sf_flush = 4700;
	test_sf_now += 50000;
	cluster_sf_origin_durable_lmon_tick();
	UT_ASSERT_EQ(test_sf_sends, 2);
	UT_ASSERT_EQ(test_sf_message.durable_lsn, 4700);
	UT_ASSERT_EQ(cluster_sf_observed_origin_durable_lsn(0), 4700);
}

UT_TEST(test_durable_zero_flush_does_not_send_positive_proof)
{
	test_sf_publish_setup(PGRAC_IC_HELLO_CAP_MULTIXACT_CURRENT_V1
						  | PGRAC_IC_HELLO_CAP_MULTIXACT_CTRC_V1);
	test_sf_flush = InvalidXLogRecPtr;
	cluster_sf_publish_origin_durable_lsn();
	UT_ASSERT_EQ(test_sf_sends, 0);
	UT_ASSERT(XLogRecPtrIsInvalid(cluster_sf_observed_origin_durable_lsn(0)));
}

UT_TEST(test_existing_smart_fusion_peer_still_receives_durability)
{
	test_sf_publish_setup(PGRAC_IC_HELLO_CAP_SMART_FUSION_REPLY_V2);
	cluster_smart_fusion = true;
	cluster_sf_publish_origin_durable_lsn();
	UT_ASSERT_EQ(test_sf_sends, 1);
	UT_ASSERT_EQ(test_sf_message.durable_lsn, 3500);
	cluster_smart_fusion = false;
}

UT_TEST(test_publisher_waits_for_recovery_before_reading_flush)
{
	for (unsigned leg = 0; leg < 2; leg++) {
		test_sf_publish_setup(leg == 0 ? PGRAC_IC_HELLO_CAP_MULTIXACT_CURRENT_V1
											 | PGRAC_IC_HELLO_CAP_MULTIXACT_CTRC_V1
									   : PGRAC_IC_HELLO_CAP_SMART_FUSION_REPLY_V2);
		cluster_smart_fusion = leg != 0;
		test_sf_recovery = true;
		cluster_sf_publish_origin_durable_lsn();
		UT_ASSERT_EQ(test_sf_flush_reads, 0);
		UT_ASSERT_EQ(test_sf_sends, 0);
		UT_ASSERT(XLogRecPtrIsInvalid(cluster_sf_observed_origin_durable_lsn(0)));
		test_sf_recovery = false;
		cluster_sf_publish_origin_durable_lsn();
		UT_ASSERT_EQ(test_sf_flush_reads, 1);
		UT_ASSERT_EQ(test_sf_sends, 1);
		UT_ASSERT_EQ(cluster_sf_observed_origin_durable_lsn(0), test_sf_flush);
	}
	cluster_smart_fusion = false;
}

UT_TEST(test_lmon_tick_continues_after_recovery_without_commit)
{
	test_sf_publish_setup(PGRAC_IC_HELLO_CAP_MULTIXACT_CURRENT_V1
						  | PGRAC_IC_HELLO_CAP_MULTIXACT_CTRC_V1);
	test_sf_now = INT64CONST(300000000);
	test_sf_recovery = true;
	cluster_sf_origin_durable_lmon_tick();
	test_sf_now += 50000;
	cluster_sf_origin_durable_lmon_tick();
	UT_ASSERT_EQ(test_sf_flush_reads, 0);
	UT_ASSERT_EQ(test_sf_sends, 0);
	UT_ASSERT(XLogRecPtrIsInvalid(cluster_sf_observed_origin_durable_lsn(0)));
	test_sf_recovery = false;
	test_sf_now += 50000;
	cluster_sf_origin_durable_lmon_tick();
	UT_ASSERT_EQ(test_sf_flush_reads, 1);
	UT_ASSERT_EQ(test_sf_sends, 1);
	UT_ASSERT_EQ(cluster_sf_observed_origin_durable_lsn(0), test_sf_flush);
	UT_ASSERT(!cluster_smart_fusion);
}

int
main(void)
{
	UT_PLAN(28);
	UT_RUN(test_a89_capability_record_snapshot_distinguishes_unavailable_and_drift);
	UT_RUN(test_vec_set_union_and_clear);
	UT_RUN(test_vec_rejects_invalid_origin_and_lsn);
	UT_RUN(test_smart_fusion_lwlock_tranche);
	UT_RUN(test_gcs_block_reply_v2_layout);
	UT_RUN(test_gcs_block_reply_v2_dep_extract_valid);
	UT_RUN(test_gcs_block_reply_v2_dep_extract_rejects_malformed);
	UT_RUN(test_gcs_block_reply_v2_dep_extract_accepts_empty_v2_no_dep);
	UT_RUN(test_peer_cap_gen_note_query_invalidate);
	UT_RUN(test_peer_cap_gen_renote_after_reconnect);
	UT_RUN(test_pcm_x_capability_does_not_alias_idle_horizon);
	UT_RUN(test_pcm_x_capability_generation_snapshot_is_exact);
	UT_RUN(test_pcm_x_capability_family_sample_is_record_coherent);
	UT_RUN(test_pcm_x_source_floor_capability_guard_is_generation_exact);
	UT_RUN(test_r4_exported_family_sample_requires_both_bits_and_canonicalizes_outputs);
	UT_RUN(test_r4_exported_family_sample_accepts_registered_generation_zero);
	UT_RUN(test_r4_exported_family_sample_reconnect_generation_is_exact);
	UT_RUN(test_stage8_ack_full_word_sample_is_record_coherent);
	UT_RUN(test_current_mx_capability_generation_sample_is_connection_exact);
	UT_RUN(test_ordinary_ctrc_peer_receives_actual_durability_with_sf_off);
	UT_RUN(test_durable_message_preserves_sender_and_monotonicity);
	UT_RUN(test_durable_publisher_requires_declared_current_capability);
	UT_RUN(test_durable_tick_retries_idle_loss_without_changing_deadlines);
	UT_RUN(test_durable_tick_observes_background_flush_without_commit);
	UT_RUN(test_durable_zero_flush_does_not_send_positive_proof);
	UT_RUN(test_existing_smart_fusion_peer_still_receives_durability);
	UT_RUN(test_publisher_waits_for_recovery_before_reading_flush);
	UT_RUN(test_lmon_tick_continues_after_recovery_without_commit);
	UT_DONE();
}
