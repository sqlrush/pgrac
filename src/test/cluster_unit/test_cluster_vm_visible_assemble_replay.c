/*-------------------------------------------------------------------------
 *
 * test_cluster_vm_visible_assemble_replay.c
 *    Assemble captured visible publications before native two-page replay.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_vm_visible_assemble_replay.c
 *
 * NOTES
 *    Captured registrations bridge the publisher and private WAL assembly.
 *    The final insert header and WAL page framing remain controlled.
 *    Selected recovery and physical I/O reuse the heap replay fixture.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "cluster/cluster_page_anchor_cache.h"
#include "common/pg_lzcompress.h"
#include "replication/origin.h"
#ifdef USE_LZ4
#include <lz4.h>
#endif
#ifdef USE_ZSTD
#include <zstd.h>
#endif

int assembly_heap_fixture_main(void);
#define VM_HEAP_REPLAY_MAIN assembly_heap_fixture_main
#include "test_cluster_vm_visible_heap_replay.c"

#include "test_cluster_vm_visible_assemble_state.inc"
static bool XLogCompressBackupBlock(char *page, uint16 hole_offset, uint16 hole_length, char *dest,
									uint16 *dlen);

/* These private registrations must not replace the publisher's capture leaves. */
#define XLogBeginInsert assembly_begin
#define XLogResetInsertion assembly_reset
#define XLogRegisterBlock assembly_block
#define XLogRegisterData assembly_data
#define XLogRegisterPageVersionEdge assembly_edge
#include "test_cluster_vm_visible_assemble.inc"
#undef XLogBeginInsert
#undef XLogResetInsertion
#undef XLogRegisterBlock
#undef XLogRegisterData
#undef XLogRegisterPageVersionEdge

static bool assembly_consistency[RM_MAX_ID + 1];
bool *wal_consistency_checking = assembly_consistency;
int wal_compression = WAL_COMPRESSION_NONE;
RepOriginId replorigin_session_origin = InvalidRepOriginId;

int
errdetail_internal(const char *format, ...)
{
	(void)format;
	UT_ASSERT(expecting_error && CritSectionCount == 0);
	return 0;
}

bool
XLogInsertAllowed(void)
{
	return !InRecovery;
}

bool
IsSubxactTopXidLogPending(void)
{
	return false; /* The captured VACUUM has no child transaction. */
}

TransactionId
GetCurrentTransactionIdIfAny(void)
{
	return InvalidTransactionId; /* No transaction assignment is modeled. */
}

TransactionId
GetTopTransactionIdIfAny(void)
{
	abort(); /* No top-XID record is requested by this fixture. */
}

bool
rf_page_anchor_cache_incarnation_matches_v1(const RfPageAnchorCacheKeyV1 *key,
											const uint8 incarnation[16])
{
	(void)key;
	(void)incarnation;
	abort(); /* Private block registrations do not supply anchor-cache keys. */
}

static void
assembly_register_captured(void)
{
	static registered_buffer buffers[2];
	static XLogRecData data[2];
	static char header[HEADER_SCRATCH_SIZE] pg_attribute_aligned(MAXIMUM_ALIGNOF);

	assembly_reset();
	memset(buffers, 0, sizeof(buffers));
	memset(data, 0, sizeof(data));
	memset(header, 0, sizeof(header));
	registered_buffers = buffers;
	max_registered_buffers = lengthof(buffers);
	rdatas = data;
	max_rdatas = lengthof(data);
	hdr_scratch = header;
	/* The original publisher already finished. Register its owned copies,
	 * never the restored-before recovery pages or backend buffer pointers. */
	InRecovery = false;
	assembly_begin();
	InRecovery = true;
	assembly_block(0, &fixture_locators[1], fixture_forks[1], fixture_blocks[1], images[0].data,
				   register_flags[0]);
	assembly_block(1, &fixture_locators[0], fixture_forks[0], fixture_blocks[0], images[1].data,
				   register_flags[1]);
	assembly_data((char *)&published_visible_payload, SizeOfHeapVisible);
	assembly_edge(edge_token, edges, edge_count);
}

/* Re-entry preserves the registered pages and native rdata nodes. Only the
 * explicitly injected semantic inputs are restored before each attempt. */
static void
assembly_emit(bool full_page_writes, unsigned fault)
{
	XLogRecData *chain;
	XLogRecPtr fpw_lsn;
	int num_fpi = 0;
	bool topxid = false;
	Size size = 0;
	char *dest;
	pg_crc32c crc;

	if (fault > 6)
		abort();
	memcpy(registered_page_version_entries, edges, edge_count * sizeof(edges[0]));
	registered_page_version_entry_count = edge_count;
	registered_page_version_result_token = edge_token;
	switch (fault) {
	case 1:
		registered_page_version_entry_count = 1;
		break;
	case 2:
		registered_page_version_entries[0].block_id = 1;
		break;
	case 3:
		registered_page_version_entries[1].component_ordinal = 0;
		break;
	case 4:
		registered_page_version_result_token = 0;
		break;
	case 5:
		registered_page_version_entries[0].result_incarnation[0] ^= 1;
		break;
	case 6:
		registered_page_version_entries[2] = edges[1];
		registered_page_version_entries[2].block_id = 2;
		registered_page_version_entries[2].component_ordinal = 2;
		registered_page_version_entry_count = 3;
		break;
	}
	chain = XLogRecordAssemble(RM_HEAP2_ID, XLOG_HEAP2_VISIBLE, UINT64_MAX, full_page_writes,
							   &fpw_lsn, &num_fpi, &topxid);
	UT_ASSERT_EQ(num_fpi, full_page_writes ? 2 : 1);
	UT_ASSERT(!topxid);
	for (XLogRecData *part = chain; part != NULL; part = part->next) {
		if (part->len > XLogRecordMaxSize - size)
			abort();
		size += part->len;
	}
	UT_ASSERT_EQ(size, ((XLogRecord *)hdr_scratch)->xl_tot_len);
	free(replay_raw);
	replay_raw = malloc(size);
	if (replay_raw == NULL)
		abort();
	dest = (char *)replay_raw;
	for (XLogRecData *part = chain; part != NULL; part = part->next) {
		memcpy(dest, part->data, part->len);
		dest += part->len;
	}
	/* XLogInsertRecord owns this last step in production. Retain the native
	 * assembler's partial data CRC, then add only the final insert header. */
	crc = replay_raw->xl_crc;
	COMP_CRC32C(crc, replay_raw, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(crc);
	replay_raw->xl_crc = crc;
}

static void
assembly_prepare(OffsetNumber unused, bool share, bool full_page_writes)
{
	heap_replay_prepare(unused, share, 0);
	assembly_register_captured();
	assembly_emit(full_page_writes, 0);
	UT_ASSERT(replay_read_raw(0));
}

UT_TEST(test_assembly_keeps_forced_vm_image_with_full_page_writes_off)
{
	static const OffsetNumber counts[] = { 0, 1, 8, MaxHeapTuplesPerPage };

	for (unsigned n = 0; n < lengthof(counts); n++) {
		for (unsigned share = 0; share < 2; share++) {
			assembly_prepare(counts[n], share != 0, false);
			UT_ASSERT(XLogRecHasBlockImage(replay_reader, 0));
			UT_ASSERT(XLogRecBlockImageApply(replay_reader, 0));
			UT_ASSERT(!XLogRecHasBlockImage(replay_reader, 1));
			UT_ASSERT_EQ(replay_decoded->page_version_edge.entries[0].edge_flags,
						 RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE);
			UT_ASSERT_EQ(replay_decoded->page_version_edge.entries[1].edge_flags, 0);
			heap_replay_apply();
			heap_replay_assert_result();
		}
	}
}

UT_TEST(test_assembly_binds_heap_image_with_full_page_writes_on)
{
	static const OffsetNumber counts[] = { 0, 1, 8, MaxHeapTuplesPerPage };

	for (unsigned n = 0; n < lengthof(counts); n++) {
		for (unsigned share = 0; share < 2; share++) {
			assembly_prepare(counts[n], share != 0, true);
			UT_ASSERT(XLogRecHasBlockImage(replay_reader, 1));
			UT_ASSERT(XLogRecBlockImageApply(replay_reader, 1));
			UT_ASSERT_EQ(replay_decoded->page_version_edge.entries[1].edge_flags,
						 RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE);
			heap_replay_apply();
			heap_replay_assert_result();
			UT_ASSERT_EQ(heap_replay_fsm, 1);
		}
	}
}

UT_TEST(test_reassembly_rebinds_the_same_registration_image_choices)
{
	assembly_prepare(8, true, false);
	for (unsigned attempt = 0; attempt < 4; attempt++) {
		bool full_page_writes = attempt % 2 == 0;

		assembly_emit(full_page_writes, 0);
		UT_ASSERT(replay_read_raw(0));
		UT_ASSERT(XLogRecHasBlockImage(replay_reader, 0));
		UT_ASSERT_EQ(XLogRecHasBlockImage(replay_reader, 1), full_page_writes);
		UT_ASSERT_EQ(replay_decoded->page_version_edge.result_token, edge_token);
	}
	heap_replay_apply();
	heap_replay_assert_result();
	UT_ASSERT_EQ(registered[0], 1);
	UT_ASSERT_EQ(registered[1], 1);
}

UT_TEST(test_assembly_rejects_inconsistent_version_bindings_before_redo)
{
	for (unsigned fault = 1; fault <= 6; fault++) {
		volatile bool caught = false;
		PGAlignedBlock saved[2];

		assembly_prepare(8, false, false);
		memcpy(saved, pages, sizeof(saved));
		expecting_error = true;
		if (setjmp(error_jump) == 0)
			assembly_emit(false, fault);
		else
			caught = true;
		expecting_error = false;
		UT_ASSERT(caught);
		UT_ASSERT(memcmp(saved, pages, sizeof(saved)) == 0);
		UT_ASSERT_EQ(heap_replay_reads[0] + heap_replay_reads[1] + heap_replay_fsm, 0);
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT_EQ(next_token, edge_token);
		UT_ASSERT(!pinned[0] && !pinned[1] && !held[0] && !held[1]);
	}
}

UT_TEST(test_refused_assembly_allows_reentry_without_another_publication)
{
	static const unsigned faults[] = { 1, 2, 4 };

	assembly_prepare(8, true, false);
	for (unsigned n = 0; n < lengthof(faults); n++) {
		volatile bool caught = false;

		expecting_error = true;
		if (setjmp(error_jump) == 0)
			assembly_emit(true, faults[n]);
		else
			caught = true;
		expecting_error = false;
		UT_ASSERT(caught);
	}
	/* Restoring fixture inputs is explicit; no backend error cleanup is modeled. */
	assembly_emit(true, 0);
	UT_ASSERT(replay_read_raw(0));
	UT_ASSERT(XLogRecHasBlockImage(replay_reader, 1));
	heap_replay_apply();
	heap_replay_assert_result();
	UT_ASSERT_EQ(registered[0], 1);
	UT_ASSERT_EQ(registered[1], 1);
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_assembly_keeps_forced_vm_image_with_full_page_writes_off);
	UT_RUN(test_assembly_binds_heap_image_with_full_page_writes_on);
	UT_RUN(test_reassembly_rebinds_the_same_registration_image_choices);
	UT_RUN(test_assembly_rejects_inconsistent_version_bindings_before_redo);
	UT_RUN(test_refused_assembly_allows_reentry_without_another_publication);
	XLogReaderFree(replay_reader);
	free(replay_raw);
	UT_DONE();
	return ut_failed_count != 0;
}
