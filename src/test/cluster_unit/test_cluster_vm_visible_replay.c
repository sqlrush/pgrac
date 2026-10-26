/*-------------------------------------------------------------------------
 *
 * test_cluster_vm_visible_replay.c
 *    Replay the VM image captured from the native empty-page publisher.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_vm_visible_replay.c
 *
 * NOTES
 *    Reuses the publication fixture and complete native VM redo bodies.
 *    WAL registration/decoding, buffers and restart identity are controlled;
 *    RestoreBlockImage is real. No heap redo or physical restart is modeled.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/xlogutils.h"
#include "cluster/cluster_page_cold_redo.h"

/* Keep the original fixture and its tests intact, using its providers once. */
int publication_fixture_main(void);
#define main publication_fixture_main
#include "test_cluster_vm_visible_version.c"
#undef main

static XLogReaderState replay_reader;
static DecodedXLogRecord *replay_decoded;
static PGAlignedBlock replay_result;
static PGAlignedBlock replay_heap;
static bool replay_identity_ok;
static unsigned replay_calls, replay_reads, replay_identity_reads;
bool cluster_page_cold_redo_active_v1;

bool
cluster_cold_redo_block_decision_v1(XLogReaderState *record, uint8 id, ClusterColdRedoBlockV1 *out)
{
	UT_ASSERT(record == &replay_reader && id == 0);
	memset(out, 0, sizeof(*out));
	out->action = CLUSTER_COLD_REDO_NATIVE;
	return true;
}

bool
cluster_page_cold_redo_vm_image_applied_v1(XLogReaderState *record, uint8 id)
{
	(void)record;
	(void)id;
	abort(); /* No cold-driver decision is supplied by this fixture. */
}

Buffer
XLogReadBufferExtended(RelFileLocator locator, ForkNumber forknum, BlockNumber block,
					   ReadBufferMode mode, Buffer recent)
{
	UT_ASSERT(RelFileLocatorEquals(locator, relation.rd_locator));
	UT_ASSERT(forknum == VISIBILITYMAP_FORKNUM && block == 0);
	UT_ASSERT(mode == RBM_NORMAL && recent == InvalidBuffer);
	UT_ASSERT(InRecovery && !pinned[0] && !pinned[1] && !held[0] && !held[1]);
	pinned[1] = true;
	replay_reads++;
	return 2;
}

XLogRedoAction
XLogReadBufferForRedoExtended(XLogReaderState *record, uint8 id, ReadBufferMode mode, bool cleanup,
							  Buffer *buffer)
{
	(void)record;
	(void)id;
	(void)mode;
	(void)cleanup;
	(void)buffer;
	abort(); /* Native VM replay must check the base before restoring it. */
}

static bool
replay_read_identity(RelFileLocator locator, ClusterSpaceIdentity *out)
{
	UT_ASSERT(InRecovery && RelFileLocatorEquals(locator, relation.rd_locator));
	UT_ASSERT(!held[0] && !held[1] && !pinned[0] && !pinned[1]);
	replay_identity_reads++;
	if (replay_identity_ok)
		*out = identity;
	return replay_identity_ok;
}

/* The real storage object remains linked for the producer. Substitute only
 * the redo entry's already-selected restart identity, not its runtime gates. */
#define cluster_space_relation_read_redo_identity replay_read_identity
#include "test_cluster_vm_visible_replay.inc"
#undef cluster_space_relation_read_redo_identity

static bool
replay_apply(void)
{
	replay_calls++;
	return visibilitymap_set_versioned_redo(&replay_reader, relation.rd_locator, 0,
											VISIBILITYMAP_VALID_BITS);
}

static void
replay_prepare(OffsetNumber unused, bool share)
{
	LVRelState state;
	Buffer vm = 2;

	MyBackendType = B_BACKEND;
	state = empty_reset(false, 12);
	if (unused != 0)
		empty_unused_slots(unused);
	memset(&published_visible_payload, 0xa5, sizeof(published_visible_payload));
	UT_ASSERT(lazy_scan_new_or_empty(&state, 1, 0, pages[0].data, share, &vm));
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(edge_count, 2);
	UT_ASSERT_EQ(registered[0], 1);
	UT_ASSERT(register_flags[0] & REGBUF_FORCE_IMAGE);
	UT_ASSERT_EQ(published_visible_payload.flags, VISIBILITYMAP_VALID_BITS);
	UT_ASSERT_EQ(published_visible_payload.snapshotConflictHorizon, InvalidTransactionId);
	UT_ASSERT(!held[0] && !held[1] && !pinned[0] && pinned[1]);
	ReleaseBuffer(vm);
	replay_result = pages[1];
	replay_heap = pages[0];
	pages[1] = before[1];
	free(replay_decoded);
	replay_decoded = calloc(1, offsetof(DecodedXLogRecord, blocks) + 2 * sizeof(DecodedBkpBlock));
	UT_ASSERT(replay_decoded != NULL);
	memset(&replay_reader, 0, sizeof(replay_reader));
	replay_reader.record = replay_decoded;
	replay_reader.EndRecPtr = PageGetLSN(replay_result.data);
	replay_decoded->header.xl_rmid = RM_HEAP2_ID;
	replay_decoded->header.xl_info = XLOG_HEAP2_VISIBLE;
	replay_decoded->main_data = (char *)&published_visible_payload;
	replay_decoded->main_data_len = SizeOfHeapVisible;
	replay_decoded->has_page_version_edge = true;
	replay_decoded->page_version_edge.result_token = edge_token;
	replay_decoded->page_version_edge.entry_count = edge_count;
	memcpy(replay_decoded->page_version_edge.entries, edges, sizeof(edges));
	/* Registration precedes assembly. The native assembler derives this
	 * decoded anchor from the forced image; raw registered edges have no
	 * image flags yet. Framing/CRC/decoding remain controlled here. */
	UT_ASSERT(XLogPageVersionImageRequiredV1(register_flags[0], false, PageGetLSN(images[0].data),
											 InvalidXLogRecPtr));
	UT_ASSERT_EQ(edges[0].edge_flags, 0);
	replay_decoded->page_version_edge.entries[0].edge_flags
		= RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE;
	replay_decoded->max_block_id = 1;
	for (int id = 0; id < 2; id++) {
		DecodedBkpBlock *block = &replay_decoded->blocks[id];
		int buffer = id == 0 ? 1 : 0;

		block->in_use = true;
		block->rlocator = fixture_locators[buffer];
		block->forknum = fixture_forks[buffer];
		block->blkno = fixture_blocks[buffer];
		block->component_ordinal = id;
	}
	/* VM FORCE_IMAGE has no standard-page hole. Do not invent a heap FPI. */
	replay_decoded->blocks[0].has_image = true;
	replay_decoded->blocks[0].apply_image = true;
	replay_decoded->blocks[0].bimg_len = BLCKSZ;
	replay_decoded->blocks[0].bimg_info = BKPIMAGE_APPLY;
	replay_decoded->blocks[0].bkp_image = images[0].data;
	replay_identity_ok = true;
	replay_calls = replay_reads = replay_identity_reads = 0;
	MyBackendType = B_STARTUP;
	InRecovery = true;
}

UT_TEST(test_published_empty_and_unused_vm_images_replay_exactly)
{
	static const OffsetNumber counts[] = { 0, 1, 8, MaxHeapTuplesPerPage };

	for (unsigned n = 0; n < lengthof(counts); n++) {
		for (int share = 0; share < 2; share++) {
			replay_prepare(counts[n], share);
			UT_ASSERT(replay_apply());
			UT_ASSERT_EQ(replay_calls, 1);
			UT_ASSERT_EQ(replay_identity_reads, 1);
			UT_ASSERT_EQ(replay_reads, 1);
			UT_ASSERT(memcmp(pages[1].data, replay_result.data, BLCKSZ) == 0);
			UT_ASSERT(memcmp(pages[0].data, replay_heap.data, BLCKSZ) == 0);
			UT_ASSERT_EQ((uint8)*PageGetContents(pages[1].data), 15);
			UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, edge_token);
			UT_ASSERT(!pinned[0] && !pinned[1] && !held[0] && !held[1]);
			UT_ASSERT_EQ(inserts, 1);
			UT_ASSERT_EQ(next_token, edge_token);
		}
	}
}

UT_TEST(test_published_vm_result_is_idempotent_without_new_wal)
{
	replay_prepare(8, false);
	UT_ASSERT(replay_apply());
	UT_ASSERT(replay_apply());
	UT_ASSERT_EQ(replay_calls, 2);
	UT_ASSERT_EQ(replay_reads, 2);
	UT_ASSERT(memcmp(pages[1].data, replay_result.data, BLCKSZ) == 0);
	UT_ASSERT(memcmp(pages[0].data, replay_heap.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(dirty[1], 2); /* One original publication, one redo apply. */
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(next_token, edge_token);
	UT_ASSERT(!pinned[0] && !pinned[1] && !held[0] && !held[1]);
}

UT_TEST(test_published_vm_replay_refuses_changed_base_image_and_identity)
{
	for (int variant = 0; variant < 8; variant++) {
		PGAlignedBlock unchanged;
		volatile bool caught = false;

		replay_prepare(8, true);
		switch (variant) {
		case 0:
			((PageHeader)pages[1].data)->pd_block_scn++;
			break;
		case 1:
			replay_identity_ok = false;
			break;
		case 2:
			replay_decoded->page_version_edge.entries[0].before.segment_incarnation[0]++;
			break;
		case 3:
			replay_decoded->blocks[0].apply_image = false;
			break;
		case 4:
			PageGetContents(images[0].data)[0] &= ~VISIBILITYMAP_VALID_BITS;
			break;
		case 5:
			((PageHeader)images[0].data)->pd_block_scn++;
			break;
		case 6:
			replay_decoded->blocks[1].blkno++;
			break;
		case 7:
			pages[1] = replay_result;
			PageGetContents(pages[1].data)[0] ^= (VISIBILITYMAP_VALID_BITS << 2);
			break;
		}
		unchanged = pages[1];
		expecting_error = true;
		if (setjmp(error_jump) == 0)
			(void)replay_apply();
		else
			caught = true;
		expecting_error = false;
		UT_ASSERT(caught);
		UT_ASSERT_EQ(replay_calls, 1);
		UT_ASSERT(memcmp(pages[1].data, unchanged.data, BLCKSZ) == 0);
		UT_ASSERT(memcmp(pages[0].data, replay_heap.data, BLCKSZ) == 0);
		UT_ASSERT(!pinned[0] && !pinned[1] && !held[0] && !held[1]);
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT_EQ(next_token, edge_token);
	}
}

UT_TEST(test_qualified_noop_produces_no_replay_record)
{
	LVRelState state;
	Buffer vm = 2;

	MyBackendType = B_BACKEND;
	state = empty_reset(true, 15);
	empty_unused_slots(8);
	UT_ASSERT(lazy_scan_new_or_empty(&state, 1, 0, pages[0].data, false, &vm));
	UT_ASSERT_EQ(inserts + edge_count + registered[0] + registered[1], 0);
	UT_ASSERT_EQ(empty_publication_metric("vm_empty_publish_noop_count"), 1);
	UT_ASSERT(memcmp(pages, before, sizeof(pages)) == 0);
	ReleaseBuffer(vm);
}

int
main(void)
{
	UT_PLAN(4);
	UT_RUN(test_published_empty_and_unused_vm_images_replay_exactly);
	UT_RUN(test_published_vm_result_is_idempotent_without_new_wal);
	UT_RUN(test_published_vm_replay_refuses_changed_base_image_and_identity);
	UT_RUN(test_qualified_noop_produces_no_replay_record);
	free(replay_decoded);
	UT_DONE();
	return ut_failed_count != 0;
}
