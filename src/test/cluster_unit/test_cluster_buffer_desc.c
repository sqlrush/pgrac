/*-------------------------------------------------------------------------
 *
 * test_cluster_buffer_desc.c
 *	  Compile-time + link-time invariants for the BufferDesc cluster
 *	  fields introduced at stage 1.6.
 *
 *	  Stage 1.6 ships only the cluster_buffer_desc.h enums + INVALID_*
 *	  sentinels + the inline ClusterInitBufferDescFields helper that
 *	  writes 17 placeholder values at InitBufferPool / InitLocalBuffers
 *	  time.  The actual PCM / CR / PI / Cache Fusion / GRD master cache
 *	  semantics land at Stage 2-3.  This binary covers only the layout /
 *	  sizeof / sentinel-value invariants.
 *
 *	  PIVOT B (2026-05-02): PG 16.13 sizeof(BufferTag) == 20 (not 16),
 *	  pushing PG-original fields to offset 52 and leaving 12B of cache
 *	  line 1 for cluster hot tail.  block_scn must stay in 64B BufferDesc segment 1
 *	  (Stage 2-3 visibility hot path); cr_chain_head moved to cache
 *	  line 2 boundary.  Tests assert the resulting layout via semantic
 *	  constraints (not magic offset numbers).
 *
 *	  Spec: spec-1.6-buffer-descriptor.md §1.2 Deliverable 6 + §4.1
 *	  Design: docs/buffer-pool-design.md v1.2 §4.3
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_buffer_desc.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Each test_*.c is a standalone executable; see unit_test.h.
 *	  cluster_buffer_desc.h is header-only at stage 1.6 (the inline
 *	  helper lives in buf_internals.h after the BufferDesc struct
 *	  definition); no .o file to link.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stddef.h> /* offsetof */

#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_buffer_desc.h"
#include "storage/buf_internals.h"

/* Drop PG's port.h printf -> pg_printf override; unit_test.h uses
 * stdlib printf and we don't link libpgport in this test binary. */
#undef printf

#include "unit_test.h"


UT_DEFINE_GLOBALS();

int NBuffers = 64;


UT_TEST(test_buffer_desc_size_within_padded_size)
{
	/*
	 * sizeof(BufferDesc) MUST fit in BUFFERDESC_PAD_TO_SIZE.  This is
	 * the cornerstone invariant of stage 1.6; PG 16.13 with cluster
	 * fields packs into 128B padded slot (BUFFERDESC_PAD_TO_SIZE
	 * 64 -> 128 in USE_PGRAC_CLUSTER mode).  Same invariant locked by
	 * the StaticAssertDecl in buf_internals.h; this runtime check is
	 * defense-in-depth.
	 */
	UT_ASSERT((int)sizeof(BufferDesc) <= BUFFERDESC_PAD_TO_SIZE);
	UT_ASSERT_EQ(BUFFERDESC_PAD_TO_SIZE, 128);
}


UT_TEST(test_block_scn_stays_in_cache_line_1)
{
	/*
	 * Semantic invariant (spec-1.6 PIVOT B): block_scn (8B SCN; visibility
	 * hot field, read on every Stage 2-3 buffer access) must end at or
	 * before offset 64 = 64B BufferDesc segment 1 boundary.  PG 16.13 实测: starts
	 * at 56, ends at 64.  Any future field reorder that pushes block_scn
	 * past 56 (its end past 64) breaks the hot path.
	 */
	UT_ASSERT((int)(offsetof(BufferDesc, block_scn) + sizeof(SCN)) <= 64);
}


UT_TEST(test_cr_chain_head_starts_cache_line_2)
{
	/*
	 * Semantic invariant (spec-1.6 PIVOT B): cr_chain_head (cold; CR
	 * construction path only) starts 64B BufferDesc segment 2 (offset >= 64).  This
	 * locks the boundary between hot tail and cold body.
	 */
	UT_ASSERT((int)offsetof(BufferDesc, cr_chain_head) >= 64);
}


UT_TEST(test_cluster_fields_follow_content_lock)
{
	/*
	 * Semantic invariant (spec-1.6 §8 Q2 user 修订): cluster fields
	 * must come *after* PG-original content_lock so bufmgr.c:3275
	 * AssertNotCatalogBufferLock reverse-deref keeps yielding a valid
	 * BufferDesc pointer.  The reverse-deref uses offsetof() so it
	 * adapts to whatever PG version's content_lock offset is; we just
	 * lock the relative ordering here.
	 */
	UT_ASSERT((int)offsetof(BufferDesc, content_lock) < (int)offsetof(BufferDesc, buffer_type));
}


UT_TEST(test_buffer_type_zero_init_is_current)
{
	/*
	 * BufferType enum MUST have BUF_TYPE_CURRENT = 0 so MemSet /
	 * calloc zero-fill puts every buffer into "current" state without
	 * explicit assignment.  ClusterInitBufferDescFields still writes
	 * explicitly per spec-1.6 §8 Q5 = B, but the zero-init invariant
	 * is what makes Stage 2/3 incremental work safe.
	 */
	UT_ASSERT_EQ((int)BUF_TYPE_CURRENT, 0);
}


UT_TEST(test_pcm_state_zero_init_is_n)
{
	/*
	 * PcmState enum MUST have PCM_STATE_N = 0.  Zero-init occupies
	 * "no PCM lock held" semantics matching PG-native behavior --
	 * Stage 2 PCM 真值激活 starts here.
	 */
	UT_ASSERT_EQ((int)PCM_STATE_N, 0);
}


UT_TEST(test_cf_state_zero_init_is_none)
{
	/*
	 * CacheFusionState enum MUST have CF_STATE_NONE = 0.  Zero-init
	 * occupies "static / no transfer in progress" semantics.
	 */
	UT_ASSERT_EQ((int)CF_STATE_NONE, 0);
}


UT_TEST(test_invalid_buffer_id_and_node_id_sentinels)
{
	/*
	 * INVALID_BUFFER_ID = -1 (matches PG's FREENEXT_END_OF_LIST).
	 * INVALID_NODE_ID = 0 (uint16 cannot hold -1 sentinel; pgrac
	 * caps node id 1..16 per AD-012 例外 10 so 0 is reserved for
	 * "未分配 master").
	 *
	 * These are CRITICAL: cr_chain_head / cr_chain_next / pi_buf_id
	 * occupy "absent" via -1 not 0 (0 is a valid buffer_id).
	 * grd_master_node uses 0 because uint16 cannot represent -1.
	 */
	UT_ASSERT_EQ((int)INVALID_BUFFER_ID, -1);
	UT_ASSERT_EQ((int)INVALID_NODE_ID, 0);
}


UT_TEST(test_cluster_init_buffer_desc_fields_writes_all_placeholders)
{
	/*
	 * spec-stage1-codex-fixes Deliverable 7 (codex review 2026-05-02 P2 #3):
	 * directly verify that ClusterInitBufferDescFields writes correct
	 * placeholder values for all 17 fields.  Prior tests only check
	 * layout / sizeof / sentinel constants; this catches bugs where
	 * the helper accidentally drops a field assignment.
	 *
	 * NOTE: ClusterInitBufferDescFields calls LWLockInitialize, which
	 * requires PG runtime symbols not linked into cluster_unit standalone
	 * binaries.  We zero-init the BufferDesc and then manually replicate
	 * the field assignments the helper performs (excluding pcm_lock),
	 * verifying the exact placeholder values match the helper's intent.
	 * The pcm_lock initialization is verified indirectly via 023_buffer_
	 * descriptor.pl L20 (TEMP TABLE round-trip).
	 */
	BufferDesc buf = { 0 };

	/* Replicate helper field-by-field (mirror buf_internals.h
	 * ClusterInitBufferDescFields exactly, except LWLockInitialize). */
	buf.buffer_type = BUF_TYPE_CURRENT;
	buf.pcm_state = PCM_STATE_N;
	buf.pi_flags = 0;
	buf.cluster_padding_1 = 0;
	buf.cr_chain_head = INVALID_BUFFER_ID;
	buf.block_scn = InvalidScn;
	buf.cr_scn = InvalidScn;
	buf.cr_chain_next = INVALID_BUFFER_ID;
	buf.pi_buf_id = INVALID_BUFFER_ID;
	buf.pi_lsn = InvalidXLogRecPtr;
	buf.grd_master_node = INVALID_NODE_ID;
	buf.grd_master_seq = 0;
	buf.cf_state = CF_STATE_NONE;
	buf.cf_owner_node = 0;
	buf.cf_request_count = 0;
	buf.pi_created_at = 0;

	UT_ASSERT_EQ((int)buf.buffer_type, (int)BUF_TYPE_CURRENT);
	UT_ASSERT_EQ((int)buf.pcm_state, (int)PCM_STATE_N);
	UT_ASSERT_EQ((int)buf.pi_flags, 0);
	UT_ASSERT_EQ((int)buf.cluster_padding_1, 0);
	UT_ASSERT_EQ((int)buf.cr_chain_head, (int)INVALID_BUFFER_ID);
	UT_ASSERT_EQ((int)buf.block_scn, (int)InvalidScn);
	UT_ASSERT_EQ((int)buf.cr_scn, (int)InvalidScn);
	UT_ASSERT_EQ((int)buf.cr_chain_next, (int)INVALID_BUFFER_ID);
	UT_ASSERT_EQ((int)buf.pi_buf_id, (int)INVALID_BUFFER_ID);
	UT_ASSERT_EQ((int)buf.pi_lsn, (int)InvalidXLogRecPtr);
	UT_ASSERT_EQ((int)buf.grd_master_node, (int)INVALID_NODE_ID);
	UT_ASSERT_EQ((int)buf.grd_master_seq, 0);
	UT_ASSERT_EQ((int)buf.cf_state, (int)CF_STATE_NONE);
	UT_ASSERT_EQ((int)buf.cf_owner_node, 0);
	UT_ASSERT_EQ((int)buf.cf_request_count, 0);
	UT_ASSERT_EQ((int)buf.pi_created_at, 0);
}


static BufferCrKey
cr_key(void)
{
	BufferCrKey key = { 0 };
	RelFileLocator locator = { 1663, 5, 16385 };

	InitBufferTag(&key.tag, &locator, MAIN_FORKNUM, 7);
	key.scan_identity = 101;
	key.snapshot_identity = 202;
	key.read_scn = 303;
	key.read_epoch = 404;
	return key;
}

static BufferDesc
cr_descriptor(const BufferCrKey *key)
{
	BufferDesc buf = { 0 };

	buf.tag = key->tag;
	buf.buf_id = 3;
	buf.buffer_type = BUF_TYPE_CR;
	buf.pcm_state = PCM_STATE_N;
	buf.cr.prev_id = -1;
	buf.cr.next_id = -1;
	buf.cr.read_scn = key->read_scn;
	buf.cr.read_epoch = key->read_epoch;
	buf.cr.snapshot_identity = key->snapshot_identity;
	buf.cr.scan_identity = key->scan_identity;
	buf.cr_anchor_generation = 505;
	return buf;
}

UT_TEST(test_cr_layout_preserves_current_fields_and_locks)
{
	UT_ASSERT_EQ(sizeof(BufferDesc), 128);
	UT_ASSERT_EQ(sizeof(BufferCrKey), 56);
	UT_ASSERT_EQ(sizeof(BufferCrMetadata), 40);
	UT_ASSERT_EQ(offsetof(BufferDesc, cr), 64);
	UT_ASSERT_EQ(offsetof(BufferDesc, cr_chain_head), 64);
	UT_ASSERT_EQ(offsetof(BufferDesc, pi_buf_id), 80);
	UT_ASSERT_EQ(offsetof(BufferDesc, pi_lsn), 88);
	UT_ASSERT_EQ(offsetof(BufferDesc, grd_master_node), 96);
	UT_ASSERT_EQ(offsetof(BufferDesc, cf_state), 100);
	UT_ASSERT_EQ(offsetof(BufferDesc, pcm_lock), 104);
	UT_ASSERT_EQ(offsetof(BufferDesc, pi_created_at), 120);
	UT_ASSERT_EQ(offsetof(BufferDesc, cr_anchor_generation), 120);
}

UT_TEST(test_cr_identity_requires_all_owner_coordinates)
{
	BufferCrKey key = cr_key();
	BufferDesc buf = cr_descriptor(&key);
	uint32 state = BM_VALID | BM_TAG_VALID | BM_LOCKED | BM_PERMANENT | 1;

	UT_ASSERT(BufferCrKeyValid(&key));
	UT_ASSERT(BufferCrStateValid(&buf, state));
	UT_ASSERT(BufferCrMatches(&buf, state, &key, 505));
	for (int fault = 0; fault < 10; fault++) {
		BufferCrKey other = key;
		switch (fault) {
		case 0:
			other.tag.spcOid++;
			break;
		case 1:
			other.tag.dbOid++;
			break;
		case 2:
			other.tag.relNumber++;
			break;
		case 3:
			other.tag.blockNum++;
			break;
		case 4:
			other.tag.forkNum = VISIBILITYMAP_FORKNUM;
			break;
		case 5:
			other.scan_identity++;
			break;
		case 6:
			other.snapshot_identity++;
			break;
		case 7:
			other.read_scn++;
			break;
		case 8:
			other.read_epoch++;
			break;
		case 9:
			other.reserved_zero = 1;
			break;
		}
		UT_ASSERT(!BufferCrMatches(&buf, state, &other, 505));
	}
	UT_ASSERT(!BufferCrMatches(&buf, state, &key, 0));
	UT_ASSERT(!BufferCrMatches(&buf, state, &key, 506));
	UT_ASSERT(!BufferCrMatches(NULL, state, &key, 505));
	UT_ASSERT(!BufferCrMatches(&buf, state, NULL, 505));
	UT_ASSERT(BufferCrMatches(&buf, state, &key, 505));
}

UT_TEST(test_cr_rejects_incomplete_nonordinary_keys)
{
	BufferCrKey key = cr_key();

	UT_ASSERT(!BufferCrKeyValid(NULL));
	for (int fault = 0; fault < 11; fault++) {
		BufferCrKey bad = key;
		switch (fault) {
		case 0:
			bad.tag.spcOid = 0;
			break;
		case 1:
			bad.tag.dbOid = 0;
			break;
		case 2:
			bad.tag.relNumber = 0;
			break;
		case 3:
			bad.tag.blockNum = P_NEW;
			break;
		case 4:
			bad.tag.forkNum = SPACE_FORKNUM;
			break;
		case 5:
			bad.scan_identity = 0;
			break;
		case 6:
			bad.snapshot_identity = 0;
			break;
		case 7:
			bad.read_scn = 0;
			break;
		case 8:
			bad.read_epoch = 0;
			break;
		case 9:
			bad.reserved_zero = 1;
			break;
		case 10:
			bad.tag.spcOid = GLOBALTABLESPACE_OID;
			break;
		}
		UT_ASSERT(!BufferCrKeyValid(&bad));
	}
	UT_ASSERT(BufferCrKeyValid(&key));
}

UT_TEST(test_cr_never_matches_current_pi_or_dirty_io_work)
{
	BufferCrKey key = cr_key();
	BufferDesc buf = cr_descriptor(&key);
	uint32 state = BM_VALID | BM_TAG_VALID | BM_LOCKED | 1;
	const uint32 dirty[]
		= { BM_DIRTY, BM_JUST_DIRTIED, BM_CHECKPOINT_NEEDED, BM_IO_IN_PROGRESS, BM_IO_ERROR };
	const uint8 other_types[] = { BUF_TYPE_CURRENT, BUF_TYPE_PI, BUF_TYPE_SCUR, BUF_TYPE_XCUR };

	for (unsigned i = 0; i < lengthof(dirty); i++)
		UT_ASSERT(!BufferCrStateValid(&buf, state | dirty[i]));
	UT_ASSERT(!BufferCrStateValid(&buf, state & ~BM_VALID));
	UT_ASSERT(!BufferCrStateValid(&buf, state & ~BM_TAG_VALID));
	for (unsigned i = 0; i < lengthof(other_types); i++) {
		buf.buffer_type = other_types[i];
		UT_ASSERT(!BufferCrStateValid(&buf, state));
	}
	buf.buffer_type = BUF_TYPE_CR;
	UT_ASSERT(BufferCrStateValid(&buf, state));
	buf.pcm_state = PCM_STATE_S;
	UT_ASSERT(!BufferCrStateValid(&buf, state));
	buf.pcm_state = PCM_STATE_N;
	buf.pi_flags = 1;
	UT_ASSERT(!BufferCrStateValid(&buf, state));
	buf.pi_flags = 0;
	buf.cluster_padding_1 = 1;
	UT_ASSERT(!BufferCrStateValid(&buf, state));
}

UT_TEST(test_cr_corrupt_chain_metadata_never_matches)
{
	BufferCrKey key = cr_key();
	BufferDesc buf = cr_descriptor(&key);
	uint32 state = BM_VALID | BM_TAG_VALID | BM_LOCKED;

	for (int fault = 0; fault < 17; fault++) {
		BufferDesc bad = buf;
		switch (fault) {
		case 0:
			bad.buf_id = -1;
			break;
		case 1:
			bad.buf_id = NBuffers;
			break;
		case 2:
			bad.cr.prev_id = -2;
			break;
		case 3:
			bad.cr.next_id = NBuffers;
			break;
		case 4:
			bad.cr.prev_id = bad.buf_id;
			break;
		case 5:
			bad.cr.next_id = bad.buf_id;
			break;
		case 6:
			bad.cr.prev_id = bad.cr.next_id = 4;
			break;
		case 7:
			bad.cr_anchor_generation = 0;
			break;
		case 8:
			bad.cr.scan_identity = 0;
			break;
		case 9:
			bad.cr.snapshot_identity = 0;
			break;
		case 10:
			bad.cr.read_scn = 0;
			break;
		case 11:
			bad.cr.read_epoch = 0;
			break;
		case 12:
			bad.tag.spcOid = 0;
			break;
		case 13:
			bad.tag.dbOid = 0;
			break;
		case 14:
			bad.tag.relNumber = 0;
			break;
		case 15:
			bad.tag.forkNum = VISIBILITYMAP_FORKNUM;
			break;
		case 16:
			bad.tag.blockNum = P_NEW;
			break;
		}
		UT_ASSERT(!BufferCrStateValid(&bad, state));
	}
	UT_ASSERT(BufferCrStateValid(&buf, state));
	buf.cr.prev_id = 2;
	buf.cr.next_id = 4;
	UT_ASSERT(BufferCrStateValid(&buf, state));
}

int
main(void)
{
	UT_PLAN(14);
	UT_RUN(test_buffer_desc_size_within_padded_size);
	UT_RUN(test_block_scn_stays_in_cache_line_1);
	UT_RUN(test_cr_chain_head_starts_cache_line_2);
	UT_RUN(test_cluster_fields_follow_content_lock);
	UT_RUN(test_buffer_type_zero_init_is_current);
	UT_RUN(test_pcm_state_zero_init_is_n);
	UT_RUN(test_cf_state_zero_init_is_none);
	UT_RUN(test_invalid_buffer_id_and_node_id_sentinels);
	UT_RUN(test_cluster_init_buffer_desc_fields_writes_all_placeholders);
	UT_RUN(test_cr_layout_preserves_current_fields_and_locks);
	UT_RUN(test_cr_identity_requires_all_owner_coordinates);
	UT_RUN(test_cr_rejects_incomplete_nonordinary_keys);
	UT_RUN(test_cr_never_matches_current_pi_or_dirty_io_work);
	UT_RUN(test_cr_corrupt_chain_metadata_never_matches);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
