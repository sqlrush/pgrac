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
 *    Registration, record/page assembly, buffers and restart identity are
 *    controlled. PageVersion encoding, the complete core WAL reader (including
 *    CRC and decoding), VM redo and RestoreBlockImage are real. No heap redo,
 *    native XLogRecordAssemble, physical restart or durability is modeled.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/xlog_internal.h"
#include "access/xlogutils.h"
#include "cluster/cluster_page_cold_redo.h"

/* Keep the original fixture and its tests intact, using its providers once. */
int publication_fixture_main(void);
#define main publication_fixture_main
#include "test_cluster_vm_visible_version.c"
#undef main

#define REPLAY_SEGMENT_SIZE (16 * 1024 * 1024)
#define REPLAY_SEGMENT_START ((XLogRecPtr)REPLAY_SEGMENT_SIZE)

static XLogReaderState *replay_reader;
static DecodedXLogRecord *replay_decoded;
static PGAlignedBlock replay_result;
static PGAlignedBlock replay_heap;
static bool replay_identity_ok;
static unsigned replay_calls, replay_reads, replay_identity_reads;
static XLogRecord *replay_raw;
static size_t replay_vm_header_offset, replay_image_offset, replay_main_header_offset;
static unsigned replay_wire_reads;
static bool replay_crc_failed;
static char replay_error[1001];
static PGAlignedXLogBlock replay_wal[3];
static XLogRecPtr replay_start, replay_first_page;
static unsigned replay_wal_fault;
bool cluster_page_cold_redo_active_v1;

static void
replay_seal_raw(void)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, (char *)replay_raw + SizeOfXLogRecord,
				replay_raw->xl_tot_len - SizeOfXLogRecord);
	COMP_CRC32C(crc, replay_raw, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(crc);
	replay_raw->xl_crc = crc;
}

static int
replay_read_page(XLogReaderState *reader, XLogRecPtr page, int required, XLogRecPtr target,
				 char *out)
{
	unsigned slot;

	(void)target;
	UT_ASSERT(required > 0 && required <= XLOG_BLCKSZ);
	reader->seg.ws_tli = 1;
	replay_wire_reads++;
	if (page == REPLAY_SEGMENT_START)
		slot = 0;
	else if (page == replay_first_page)
		slot = 1;
	else if (page == replay_first_page + XLOG_BLCKSZ)
		slot = 2;
	else
		return -1;
	if (slot == 2 && replay_wal_fault == 4)
		return -1;
	memcpy(out, replay_wal[slot].data, XLOG_BLCKSZ);
	return XLOG_BLCKSZ;
}

static bool
replay_read_raw(unsigned fault)
{
	const uint64 system_id = UINT64_C(0x1020304050607080);
	XLogLongPageHeader first = (XLogLongPageHeader)replay_wal[0].data;
	XLogPageHeader head = (XLogPageHeader)replay_wal[1].data;
	XLogPageHeader tail = (XLogPageHeader)replay_wal[2].data;
	size_t first_part, remaining;
	char *error = NULL;
	bool ok;

	if (replay_reader != NULL)
		XLogReaderFree(replay_reader);
	replay_decoded = NULL;
	replay_wire_reads = 0;
	replay_wal_fault = fault;
	replay_error[0] = '\0';
	memset(replay_wal, 0, sizeof(replay_wal));
	/* Preserve the original publication leaf's assigned end LSN.
	 * The record straddles two short-header pages; page zero supplies the
	 * segment's independently validated long header. No reader cursor is faked. */
	replay_start
		= PageGetLSN(replay_result.data) - MAXALIGN(replay_raw->xl_tot_len) - SizeOfXLogShortPHD;
	replay_first_page = replay_start - replay_start % XLOG_BLCKSZ;
	first_part = XLOG_BLCKSZ - replay_start % XLOG_BLCKSZ;
	remaining = replay_raw->xl_tot_len - first_part;
	if (replay_first_page <= REPLAY_SEGMENT_START || replay_start % XLOG_BLCKSZ < SizeOfXLogShortPHD
		|| first_part >= replay_raw->xl_tot_len || remaining > XLOG_BLCKSZ - SizeOfXLogShortPHD)
		abort();
	first->std.xlp_magic = XLOG_PAGE_MAGIC;
	first->std.xlp_info = XLP_LONG_HEADER;
	first->std.xlp_tli = 1;
	first->std.xlp_pageaddr = REPLAY_SEGMENT_START;
	first->std.xlp_thread_id = 3;
	first->std.xlp_cluster_flags = XLP_CLUSTER_FLAGS_RESERVED;
	first->xlp_sysid = system_id;
	first->xlp_seg_size = REPLAY_SEGMENT_SIZE;
	first->xlp_xlog_blcksz = XLOG_BLCKSZ;
	head->xlp_magic = tail->xlp_magic = XLOG_PAGE_MAGIC;
	head->xlp_tli = tail->xlp_tli = 1;
	head->xlp_thread_id = tail->xlp_thread_id = 3;
	head->xlp_cluster_flags = tail->xlp_cluster_flags = XLP_CLUSTER_FLAGS_RESERVED;
	head->xlp_pageaddr = replay_first_page;
	tail->xlp_pageaddr = replay_first_page + XLOG_BLCKSZ;
	tail->xlp_info = XLP_FIRST_IS_CONTRECORD;
	tail->xlp_rem_len = remaining;
	memcpy(replay_wal[1].data + replay_start % XLOG_BLCKSZ, replay_raw, first_part);
	memcpy(replay_wal[2].data + SizeOfXLogShortPHD, (char *)replay_raw + first_part, remaining);
	if (fault == 1)
		tail->xlp_rem_len++;
	else if (fault == 2)
		tail->xlp_info &= ~XLP_FIRST_IS_CONTRECORD;
	else if (fault == 3)
		tail->xlp_pageaddr++;
	replay_reader = XLogReaderAllocate(REPLAY_SEGMENT_SIZE, NULL,
									   XL_ROUTINE(.page_read = replay_read_page), NULL);
	if (replay_reader == NULL)
		abort();
	replay_reader->system_identifier = system_id;
	replay_reader->cluster_expected_thread_id = 3;
	XLogBeginRead(replay_reader, replay_start);
	ok = XLogReadRecord(replay_reader, &error) != NULL;
	replay_crc_failed = replay_reader->cluster_record_crc_failed;
	if (error != NULL)
		strlcpy(replay_error, error, sizeof(replay_error));
	if (ok) {
		replay_decoded = replay_reader->record;
		UT_ASSERT_EQ(replay_reader->EndRecPtr, PageGetLSN(replay_result.data));
	}
	return ok;
}

static void
replay_build_raw(void)
{
	RfPageVersionEdgeEntryV1 wire_edges[2];
	uint8 edge_wire[XLR_PAGE_VERSION_EDGE_MAX_SIZE];
	XLogRecordBlockHeader block = { 0 };
	XLogRecordBlockImageHeader image = { 0 };
	size_t edge_size, size;
	uint8 *target;

	UT_ASSERT_EQ(edge_count, lengthof(wire_edges));
	memcpy(wire_edges, edges, sizeof(wire_edges));
	/* Explicit fixture assembly: full_page_writes is off. The actual
	 * native image-selection helper still selects the forced VM image. */
	UT_ASSERT(XLogPageVersionImageRequiredV1(register_flags[0], false, PageGetLSN(images[0].data),
											 InvalidXLogRecPtr));
	UT_ASSERT(!XLogPageVersionImageRequiredV1(register_flags[1], false, PageGetLSN(images[1].data),
											  InvalidXLogRecPtr));
	UT_ASSERT_EQ(wire_edges[0].edge_flags, 0);
	wire_edges[0].edge_flags = RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE;
	if (!XLogEncodePageVersionEdgeV1(edge_wire, sizeof(edge_wire), edge_token, wire_edges,
									 lengthof(wire_edges), &edge_size))
		abort();
	size = SizeOfXLogRecord + edge_size + 2 * SizeOfXLogRecordBlockHeader
		   + SizeOfXLogRecordBlockImageHeader + sizeof(RelFileLocator) + 2 * sizeof(BlockNumber)
		   + SizeOfXLogRecordDataHeaderShort + BLCKSZ + SizeOfHeapVisible;
	free(replay_raw);
	replay_raw = calloc(1, size);
	if (replay_raw == NULL || SizeOfHeapVisible > UINT8_MAX)
		abort();
	replay_raw->xl_tot_len = size;
	replay_raw->xl_rmid = RM_HEAP2_ID;
	replay_raw->xl_info = XLOG_HEAP2_VISIBLE;
	target = (uint8 *)replay_raw + SizeOfXLogRecord;
	memcpy(target, edge_wire, edge_size);
	target += edge_size;
	replay_vm_header_offset = target - (uint8 *)replay_raw;
	block.fork_flags = fixture_forks[1] | BKPBLOCK_HAS_IMAGE;
	memcpy(target, &block, SizeOfXLogRecordBlockHeader);
	target += SizeOfXLogRecordBlockHeader;
	image.length = BLCKSZ;
	image.bimg_info = BKPIMAGE_APPLY;
	memcpy(target, &image, SizeOfXLogRecordBlockImageHeader);
	target += SizeOfXLogRecordBlockImageHeader;
	memcpy(target, &fixture_locators[1], sizeof(RelFileLocator));
	target += sizeof(RelFileLocator);
	memcpy(target, &fixture_blocks[1], sizeof(BlockNumber));
	target += sizeof(BlockNumber);
	UT_ASSERT(RelFileLocatorEquals(fixture_locators[0], fixture_locators[1]));
	block.id = 1;
	block.fork_flags = fixture_forks[0] | BKPBLOCK_SAME_REL;
	memcpy(target, &block, SizeOfXLogRecordBlockHeader);
	target += SizeOfXLogRecordBlockHeader;
	memcpy(target, &fixture_blocks[0], sizeof(BlockNumber));
	target += sizeof(BlockNumber);
	replay_main_header_offset = target - (uint8 *)replay_raw;
	*target++ = XLR_BLOCK_ID_DATA_SHORT;
	*target++ = SizeOfHeapVisible;
	replay_image_offset = target - (uint8 *)replay_raw;
	memcpy(target, images[0].data, BLCKSZ);
	target += BLCKSZ;
	memcpy(target, &published_visible_payload, SizeOfHeapVisible);
	target += SizeOfHeapVisible;
	UT_ASSERT_EQ((size_t)(target - (uint8 *)replay_raw), size);
	replay_seal_raw();
}

bool
cluster_cold_redo_block_decision_v1(XLogReaderState *record, uint8 id, ClusterColdRedoBlockV1 *out)
{
	UT_ASSERT(record == replay_reader && id == 0);
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
	return visibilitymap_set_versioned_redo(replay_reader, relation.rd_locator, 0,
											VISIBILITYMAP_VALID_BITS);
}

static void
replay_prepare(OffsetNumber unused, bool share)
{
	LVRelState state;
	Buffer vm = 2;

	MyBackendType = B_BACKEND;
	insert_end_lsn = REPLAY_SEGMENT_START + UINT64_C(0x9000);
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
	replay_identity_ok = true;
	replay_calls = replay_reads = replay_identity_reads = 0;
	MyBackendType = B_STARTUP;
	InRecovery = true;
	replay_build_raw();
	if (!replay_read_raw(0)) {
		fprintf(stderr, "native visible WAL read failed: %s\n", replay_error);
		abort();
	}
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
			PageGetContents(replay_decoded->blocks[0].bkp_image)[0] &= ~VISIBILITYMAP_VALID_BITS;
			break;
		case 5:
			((PageHeader)replay_decoded->blocks[0].bkp_image)->pd_block_scn++;
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

UT_TEST(test_published_record_crosses_native_wal_reader_before_redo)
{
	replay_prepare(8, true);
	UT_ASSERT(replay_raw != NULL);
	if (replay_raw == NULL)
		return;
	UT_ASSERT(replay_raw->xl_tot_len > XLOG_BLCKSZ);
	UT_ASSERT_EQ(replay_wire_reads, 3);
	UT_ASSERT(!replay_crc_failed && replay_error[0] == '\0');
	UT_ASSERT_EQ(replay_decoded->header.xl_tot_len, replay_raw->xl_tot_len);
	UT_ASSERT_EQ(replay_decoded->header.xl_crc, replay_raw->xl_crc);
	UT_ASSERT_EQ(replay_decoded->main_data_len, SizeOfHeapVisible);
	UT_ASSERT(memcmp(replay_decoded->main_data, &published_visible_payload, SizeOfHeapVisible)
			  == 0);
	UT_ASSERT_EQ(replay_decoded->max_block_id, 1);
	UT_ASSERT(replay_decoded->has_page_version_edge);
	UT_ASSERT_EQ(replay_decoded->page_version_edge.entry_count, 2);
	UT_ASSERT(replay_apply());
	UT_ASSERT(memcmp(pages[1].data, replay_result.data, BLCKSZ) == 0);
	UT_ASSERT(memcmp(pages[0].data, replay_heap.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT(!pinned[0] && !pinned[1] && !held[0] && !held[1]);
}

UT_TEST(test_native_reader_rejects_corrupt_record_before_vm_access)
{
	for (unsigned variant = 0; variant < 5; variant++) {
		static const size_t header_offsets[]
			= { offsetof(XLogRecord, xl_scn), offsetof(XLogRecord, xl_crc) };
		size_t offset;

		replay_prepare(8, false);
		UT_ASSERT(replay_raw != NULL);
		if (replay_raw == NULL)
			return;
		if (variant < 2)
			offset = header_offsets[variant];
		else if (variant == 2)
			offset = SizeOfXLogRecord + XLR_PAGE_VERSION_EDGE_HEADER_SIZE;
		else if (variant == 3)
			offset = replay_image_offset + BLCKSZ - 1;
		else
			offset = replay_raw->xl_tot_len - 1;
		((uint8 *)replay_raw)[offset] ^= 1;
		UT_ASSERT(!replay_read_raw(0));
		UT_ASSERT(replay_crc_failed);
		UT_ASSERT(strstr(replay_error, "checksum") != NULL);
		UT_ASSERT_EQ(replay_calls + replay_reads + replay_identity_reads, 0);
		UT_ASSERT(memcmp(pages[1].data, before[1].data, BLCKSZ) == 0);
		UT_ASSERT(memcmp(pages[0].data, replay_heap.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT(!pinned[0] && !pinned[1] && !held[0] && !held[1]);
	}
}

UT_TEST(test_valid_crc_does_not_admit_malformed_visible_record)
{
	for (unsigned variant = 0; variant < 7; variant++) {
		uint8 *wire;
		uint16 short_image = BLCKSZ - 1;

		replay_prepare(8, true);
		UT_ASSERT(replay_raw != NULL);
		if (replay_raw == NULL)
			return;
		wire = (uint8 *)replay_raw;
		switch (variant) {
		case 0:
			wire[SizeOfXLogRecord] = XLR_BLOCK_ID_TOPLEVEL_XID;
			break;
		case 1:
			wire[SizeOfXLogRecord + XLR_PAGE_VERSION_EDGE_HEADER_SIZE] = 2;
			break;
		case 2:
			wire[replay_vm_header_offset] = 1;
			break;
		case 3:
			wire[replay_vm_header_offset + 1] &= ~BKPBLOCK_HAS_IMAGE;
			break;
		case 4:
			memcpy(wire + replay_vm_header_offset + SizeOfXLogRecordBlockHeader, &short_image,
				   sizeof(short_image));
			break;
		case 5:
			wire[replay_vm_header_offset + 1] |= BKPBLOCK_SAME_REL;
			break;
		case 6:
			wire[replay_main_header_offset + 1]++;
			break;
		}
		replay_seal_raw();
		UT_ASSERT(!replay_read_raw(0));
		UT_ASSERT(!replay_crc_failed && replay_error[0] != '\0');
		UT_ASSERT_EQ(replay_calls + replay_reads + replay_identity_reads, 0);
		UT_ASSERT(memcmp(pages[1].data, before[1].data, BLCKSZ) == 0);
		UT_ASSERT(memcmp(pages[0].data, replay_heap.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT(!pinned[0] && !pinned[1] && !held[0] && !held[1]);
	}
}

UT_TEST(test_native_decoded_image_and_payload_own_their_bytes)
{
	replay_prepare(8, true);
	UT_ASSERT(replay_raw != NULL);
	if (replay_raw == NULL)
		return;
	UT_ASSERT(replay_decoded->main_data != (char *)&published_visible_payload);
	UT_ASSERT(replay_decoded->blocks[0].bkp_image != images[0].data);
	memset(replay_raw, 0xa5, replay_raw->xl_tot_len);
	memset(&published_visible_payload, 0xa5, sizeof(published_visible_payload));
	memset(images, 0xa5, sizeof(images));
	UT_ASSERT(replay_apply());
	UT_ASSERT(memcmp(pages[1].data, replay_result.data, BLCKSZ) == 0);
	UT_ASSERT(memcmp(pages[0].data, replay_heap.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT(!pinned[0] && !pinned[1] && !held[0] && !held[1]);
}

UT_TEST(test_native_reader_rejects_broken_wal_continuation)
{
	for (unsigned fault = 1; fault <= 4; fault++) {
		replay_prepare(8, false);
		UT_ASSERT(replay_raw != NULL);
		if (replay_raw == NULL)
			return;
		UT_ASSERT(!replay_read_raw(fault));
		UT_ASSERT(!replay_crc_failed);
		UT_ASSERT_EQ(replay_calls + replay_reads + replay_identity_reads, 0);
		UT_ASSERT(memcmp(pages[1].data, before[1].data, BLCKSZ) == 0);
		UT_ASSERT(memcmp(pages[0].data, replay_heap.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT(!pinned[0] && !pinned[1] && !held[0] && !held[1]);
	}
}

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(test_published_empty_and_unused_vm_images_replay_exactly);
	UT_RUN(test_published_vm_result_is_idempotent_without_new_wal);
	UT_RUN(test_published_vm_replay_refuses_changed_base_image_and_identity);
	UT_RUN(test_qualified_noop_produces_no_replay_record);
	UT_RUN(test_published_record_crosses_native_wal_reader_before_redo);
	UT_RUN(test_native_reader_rejects_corrupt_record_before_vm_access);
	UT_RUN(test_valid_crc_does_not_admit_malformed_visible_record);
	UT_RUN(test_native_decoded_image_and_payload_own_their_bytes);
	UT_RUN(test_native_reader_rejects_broken_wal_continuation);
	XLogReaderFree(replay_reader);
	free(replay_raw);
	UT_DONE();
	return ut_failed_count != 0;
}
