/*-------------------------------------------------------------------------
 *
 * test_cluster_generic_version.c
 *    Internal generic WAL retains exact before/result page versions.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_generic_version.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <setjmp.h>

#include "access/generic_xlog.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/storage/cluster_smgr.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool cluster_enabled = true, cluster_shared_config = true;
BackendType MyBackendType = B_BACKEND;
static ClusterNormalStartState fixture_start_state = CLUSTER_NORMAL_START_SOURCE_ZERO;
static bool fixture_pristine = true;
int cluster_node_id = 0, NBuffers = 4, NLocBuffer;
char *BufferBlocks;
Block *LocalBufferBlockPointers;
volatile uint32 CritSectionCount;
bool cluster_recmerge_window_active, cluster_recmerge_apply_foreign;
uint64 cluster_recmerge_window_scn, cluster_recmerge_window_own_lsn;

static PGAlignedBlock pages[4], before[4], wal_images[4];
static char deltas[4][BLCKSZ + 16];
static Size delta_lengths[4];
static ClusterSpaceIdentity identities[4];
static RfPageVersionEdgeEntryV1 entries[4];
static unsigned edge_count, inserts, dirties;
static unsigned raw_blocks;
static RelFileLocator raw_locator;
static ForkNumber raw_fork;
static BlockNumber raw_block;
static uint8 raw_flags;
static uint64 next_token, wal_token;
static bool begun, expecting_error;
static jmp_buf error_jump;

ClusterNormalStartState
cluster_semantic_normal_start_state(void)
{
	return fixture_start_state;
}

bool
cluster_semantic_activation_phase1_pristine(void)
{
	return fixture_pristine;
}

bool
cluster_semantic_activation_recovered_open_current(void)
{
	return false;
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# assertion %s at %s:%d\n", condition, file, line);
	abort();
}

void *
palloc_aligned(Size size, Size alignment, int flags)
{
	void *ptr = NULL;
	(void)flags;
	if (posix_memalign(&ptr, alignment, size) != 0)
		abort();
	return ptr;
}

void
pfree(void *ptr)
{
	free(ptr);
}

bool
RecoveryInProgress(void)
{
	return false;
}

bool
cluster_wal_thread_current_v2_ref(ClusterWalSourceRef *out)
{
	memset(out, 0, sizeof(*out));
	out->claim.identity.system_identifier = 11;
	out->claim.database_incarnation = 12;
	memset(out->claim.identity.storage_uuid, 13, 16);
	return true;
}

bool
cluster_wal_thread_restart_v2_ref(ClusterWalSourceRef *out)
{
	(void)out;
	abort();
}

int
cluster_smgr_which_for(RelFileLocator locator, BackendId backend)
{
	if (backend != InvalidBackendId || locator.dbOid != 5)
		abort();
	return 1;
}

void
BufferGetTag(Buffer buffer, RelFileLocator *locator, ForkNumber *forknum, BlockNumber *block)
{
	*locator = (RelFileLocator){ 1663, 5, 900 + buffer - 1 };
	*forknum = MAIN_FORKNUM;
	*block = buffer - 1;
}

bool
BufferIsPermanent(Buffer buffer)
{
	return buffer > 0 && buffer <= 4;
}

SCN
cluster_scn_advance(void)
{
	return ++next_token;
}

void
XLogBeginInsert(void)
{
	if (begun)
		abort();
	begun = true;
}

void
MarkBufferDirty(Buffer buffer)
{
	if (!BufferIsValid(buffer) || CritSectionCount == 0)
		abort();
	dirties++;
}

void
XLogRegisterBuffer(uint8 id, Buffer buffer, uint8 flags)
{
	(void)flags;
	if (!begun || id >= 4 || buffer != id + 1)
		abort();
	memcpy(wal_images[id].data, BufferGetPage(buffer), BLCKSZ);
}

void
XLogRegisterBufData(uint8 id, char *data, uint32 length)
{
	if (!begun || id >= 4 || length > sizeof(deltas[id]))
		abort();
	memcpy(deltas[id], data, length);
	delta_lengths[id] = length;
}

void
XLogRegisterBlock(uint8 id, RelFileLocator *locator, ForkNumber fork_number,
				  BlockNumber block_number, char *page, uint8 flags)
{
	if (!begun || id != 0 || CritSectionCount == 0)
		abort();
	raw_blocks++;
	raw_locator = *locator;
	raw_fork = fork_number;
	raw_block = block_number;
	raw_flags = flags;
	memcpy(wal_images[id].data, page, BLCKSZ);
}

void
XLogRegisterPageVersionEdge(uint64 token, const RfPageVersionEdgeEntryV1 *edges, uint8 count)
{
	if (!begun || count > 4)
		abort();
	wal_token = token;
	edge_count = count;
	memcpy(entries, edges, count * sizeof(*edges));
}

XLogRecPtr
XLogInsert(RmgrId rmid, uint8 info)
{
	if (!begun || rmid != RM_GENERIC_ID || info != 0 || CritSectionCount == 0)
		abort();
	begun = false;
	inserts++;
	return 800;
}

bool
errstart(int level, const char *domain)
{
	(void)domain;
	/* Native ARM CRC feature selection emits DEBUG1 on Linux. */
	if (level == DEBUG1)
		return false;
	if (expecting_error && level == ERROR)
		return true;
	abort();
}

int
errmsg_internal(const char *format, ...)
{
	(void)format;
	if (expecting_error)
		longjmp(error_jump, 1);
	abort();
}

int
errmsg(const char *format, ...)
{
	(void)format;
	return 0;
}

int
errcode(int code)
{
	(void)code;
	return 0;
}

void
errfinish(const char *file, int line, const char *func)
{
	(void)file;
	(void)line;
	(void)func;
	if (expecting_error)
		longjmp(error_jump, 1);
	abort();
}

static void
reset(void)
{
	unsigned i;

	BufferBlocks = pages[0].data;
	memset(pages, 0, sizeof(pages));
	memset(identities, 0, sizeof(identities));
	memset(delta_lengths, 0, sizeof(delta_lengths));
	for (i = 0; i < 4; i++) {
		PageHeader page = (PageHeader)pages[i].data;
		page->pd_lower = SizeOfPageHeaderData + 16;
		page->pd_upper = BLCKSZ - 16;
		page->pd_special = BLCKSZ;
		page->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
		page->pd_block_scn = 20 + i;
		identities[i].key.system_identifier = 11;
		identities[i].key.database_incarnation = 12;
		memset(identities[i].key.storage_uuid, 13, 16);
		identities[i].key.locator = (RelFileLocator){ 1663, 5, 900 + i };
		memset(identities[i].incarnation, 30 + i, 16);
		identities[i].sequence = 1;
		identities[i].operation = 40 + i;
		identities[i].state = CLUSTER_SPACE_IDENTITY_LIVE;
	}
	memcpy(before, pages, sizeof(pages));
	cluster_shared_config = true;
	MyBackendType = B_BACKEND;
	fixture_start_state = CLUSTER_NORMAL_START_SOURCE_ZERO;
	fixture_pristine = true;
	CritSectionCount = 0;
	edge_count = inserts = dirties = 0;
	raw_blocks = 0;
	wal_token = 0;
	next_token = 100;
	begun = expecting_error = false;
}

/* The original RED used the native unversioned interface at these seams. */
static GenericXLogState *
start_internal(void)
{
	return GenericXLogStartInternal(true, GENERIC_XLOG_ITL_FINISH);
}

static Page
register_internal(GenericXLogState *state, Buffer buffer, int flags)
{
	return GenericXLogRegisterBufferVersioned(state, buffer, flags, &identities[buffer - 1]);
}

UT_TEST(test_delta_carries_result_version)
{
	GenericXLogState *state;
	Page image;
	PGAlignedBlock replay;
	Size pos;

	reset();
	state = start_internal();
	image = register_internal(state, 1, 0);
	image[SizeOfPageHeaderData] = 77;
	UT_ASSERT_EQ(GenericXLogFinish(state), 800);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(edge_count, 1);
	UT_ASSERT_EQ(entries[0].before.mutation_token, 20);
	UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 101);
	/* Replay only the actual emitted delta on an independent predecessor. */
	replay = before[0];
	for (pos = 0; pos < delta_lengths[0];) {
		OffsetNumber offset, length;
		memcpy(&offset, deltas[0] + pos, sizeof(offset));
		pos += sizeof(offset);
		memcpy(&length, deltas[0] + pos, sizeof(length));
		pos += sizeof(length);
		UT_ASSERT(pos + length <= delta_lengths[0] && offset + length <= BLCKSZ);
		memcpy(replay.data + offset, deltas[0] + pos, length);
		pos += length;
	}
	UT_ASSERT_EQ(((PageHeader)replay.data)->pd_block_scn, 101);
	UT_ASSERT_EQ(replay.data[SizeOfPageHeaderData], 77);
}

UT_TEST(test_multiple_relations_share_one_batch_token)
{
	GenericXLogState *state;
	unsigned i;

	reset();
	state = start_internal();
	for (i = 0; i < 4; i++)
		register_internal(state, i + 1, GENERIC_XLOG_FULL_IMAGE)[SizeOfPageHeaderData] = 80 + i;
	GenericXLogFinish(state);
	UT_ASSERT_EQ(edge_count, 4);
	UT_ASSERT_EQ(next_token, 101);
	for (i = 0; i < 4; i++) {
		UT_ASSERT_EQ(entries[i].block_id, i);
		UT_ASSERT_EQ(entries[i].before.mutation_token, 20 + i);
		UT_ASSERT(memcmp(entries[i].result_incarnation, identities[i].incarnation, 16) == 0);
		UT_ASSERT_EQ(((PageHeader)wal_images[i].data)->pd_block_scn, 101);
		UT_ASSERT_EQ(wal_images[i].data[SizeOfPageHeaderData], 80 + i);
	}
}

UT_TEST(test_abort_preserves_all_page_bytes)
{
	GenericXLogState *state;

	reset();
	state = start_internal();
	register_internal(state, 1, 0)[SizeOfPageHeaderData] = 77;
	GenericXLogAbort(state);
	UT_ASSERT(memcmp(pages, before, sizeof(pages)) == 0);
	UT_ASSERT_EQ(inserts, 0);
	UT_ASSERT_EQ(next_token, 100);
}

UT_TEST(test_nonshared_native_generic_unchanged)
{
	GenericXLogState *state;

	reset();
	cluster_shared_config = false;
	state = GenericXLogStartLogged(true);
	GenericXLogRegisterBuffer(state, 1, 0)[SizeOfPageHeaderData] = 77;
	GenericXLogFinish(state);
	UT_ASSERT_EQ(edge_count, 0);
	UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 20);
	UT_ASSERT_EQ(pages[0].data[SizeOfPageHeaderData], 77);
}

UT_TEST(test_unrecognized_owner_and_public_entry_refuse)
{
	int which;
	for (which = 0; which < 2; which++) {
		volatile bool caught = false;
		reset();
		expecting_error = true;
		if (setjmp(error_jump) == 0) {
			GenericXLogState *state
				= which == 0 ? GenericXLogStartLogged(true)
							 : GenericXLogStartInternal(true, (GenericXLogInternalOwner)99);
			GenericXLogAbort(state);
		} else
			caught = true;
		expecting_error = false;
		UT_ASSERT(caught);
		UT_ASSERT_EQ(inserts, 0);
		UT_ASSERT(memcmp(pages, before, sizeof(pages)) == 0);
	}
}

UT_TEST(test_wrong_namespace_missing_identity_and_zero_before_refuse)
{
	int fault;
	for (fault = 0; fault < 5; fault++) {
		GenericXLogState *state;
		volatile bool caught = false;
		reset();
		state = start_internal();
		switch (fault) {
		case 0:
			identities[0].key.database_incarnation++;
			break;
		case 1:
			identities[0].key.locator.relNumber++;
			break;
		case 2:
			identities[0].state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
			break;
		case 3:
			((PageHeader)pages[0].data)->pd_block_scn = 0;
			break;
		}
		memcpy(before, pages, sizeof(pages));
		expecting_error = true;
		if (setjmp(error_jump) == 0) {
			if (fault == 4)
				GenericXLogRegisterBuffer(state, 1, 0);
			else
				register_internal(state, 1, 0);
		} else
			caught = true;
		expecting_error = false;
		GenericXLogAbort(state);
		UT_ASSERT(caught);
		UT_ASSERT_EQ(next_token, 100);
		UT_ASSERT(memcmp(pages, before, sizeof(pages)) == 0);
	}
}

UT_TEST(test_second_page_drift_refuses_before_any_publication)
{
	int fault;
	for (fault = 0; fault < 3; fault++) {
		GenericXLogState *state;
		Page image;
		volatile bool caught = false;
		reset();
		state = start_internal();
		register_internal(state, 1, 0)[SizeOfPageHeaderData] = 77;
		image = register_internal(state, 2, 0);
		if (fault == 0)
			((PageHeader)pages[1].data)->pd_block_scn++;
		else if (fault == 1)
			((PageHeader)image)->pd_block_scn++;
		else
			((PageHeader)image)->pd_lower = BLCKSZ;
		memcpy(before, pages, sizeof(pages));
		expecting_error = true;
		if (setjmp(error_jump) == 0)
			GenericXLogFinish(state);
		else
			caught = true;
		expecting_error = false;
		if (caught)
			GenericXLogAbort(state);
		UT_ASSERT(caught);
		UT_ASSERT_EQ(dirties, 0);
		UT_ASSERT_EQ(inserts, 0);
		UT_ASSERT_EQ(CritSectionCount, 0);
		UT_ASSERT(memcmp(pages, before, sizeof(pages)) == 0);
	}
}

UT_TEST(test_all_three_internal_owners_and_duplicate_registration)
{
	int owner;
	for (owner = GENERIC_XLOG_ITL_FINISH; owner <= GENERIC_XLOG_CTRC_ITL; owner++) {
		GenericXLogState *state;
		Page image;
		reset();
		state = GenericXLogStartInternal(true, (GenericXLogInternalOwner)owner);
		image = register_internal(state, 1, 0);
		image[SizeOfPageHeaderData] = 77;
		UT_ASSERT(register_internal(state, 1, 0) == image);
		GenericXLogFinish(state);
		UT_ASSERT_EQ(edge_count, 1);
		UT_ASSERT_EQ(next_token, 101);
		UT_ASSERT_EQ(pages[0].data[SizeOfPageHeaderData], 77);
	}
}

UT_TEST(test_recovery_full_image_frame_uses_native_block_without_publication)
{
	for (int owner = GENERIC_XLOG_CTRC_MX; owner <= GENERIC_XLOG_CTRC_ITL; owner++) {
		PGAlignedBlock changed;
		GenericXLogRecoveryImageV1 frame;
		reset();
		MyBackendType = B_STARTUP;
		fixture_start_state = CLUSTER_NORMAL_START_EXISTING_OTHER;
		fixture_pristine = false;
		changed = pages[0];
		changed.data[SizeOfPageHeaderData] = 77;
		memset(changed.data + ((PageHeader)changed.data)->pd_lower, 0xa5,
			((PageHeader)changed.data)->pd_upper - ((PageHeader)changed.data)->pd_lower);
		UT_ASSERT(GenericXLogRecoveryImagePrepare((GenericXLogInternalOwner)owner,
			&identities[0], MAIN_FORKNUM, 7, pages[0].data, changed.data, &frame));
		UT_ASSERT(memcmp(pages, before, sizeof(pages)) == 0);
		UT_ASSERT_EQ(changed.data[SizeOfPageHeaderData], 77);
		UT_ASSERT_EQ((unsigned char)changed.data[((PageHeader)changed.data)->pd_lower], 0xa5);
		UT_ASSERT_EQ(frame.result_token, 101);
		UT_ASSERT_EQ(frame.version.before.mutation_token, 20);
		UT_ASSERT_EQ(((PageHeader)frame.image.data)->pd_block_scn, 101);
		for (unsigned i = ((PageHeader)frame.image.data)->pd_lower;
			 i < ((PageHeader)frame.image.data)->pd_upper; i++)
			UT_ASSERT_EQ(frame.image.data[i], 0);
		UT_ASSERT_EQ(raw_blocks, 0);
		UT_ASSERT_EQ(inserts, 0);
		XLogBeginInsert();
		CritSectionCount++;
		UT_ASSERT(GenericXLogRecoveryImageRegister(&frame));
		CritSectionCount--;
		UT_ASSERT_EQ(raw_blocks, 1);
		UT_ASSERT(memcmp(&raw_locator, &identities[0].key.locator, sizeof(raw_locator)) == 0);
		UT_ASSERT_EQ(raw_fork, MAIN_FORKNUM);
		UT_ASSERT_EQ(raw_block, 7);
		UT_ASSERT_EQ(raw_flags, REGBUF_FORCE_IMAGE | REGBUF_STANDARD);
		UT_ASSERT_EQ(edge_count, 1);
		UT_ASSERT_EQ(wal_token, 101);
		UT_ASSERT(memcmp(wal_images[0].data, frame.image.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(inserts, 0);
		UT_ASSERT_EQ(dirties, 0);
		UT_ASSERT(memcmp(pages, before, sizeof(pages)) == 0);
	}
}

UT_TEST(test_recovery_frame_descriptor_retains_exact_target_version_and_digest)
{
	GenericXLogRecoveryImageV1 frame, saved;
	GenericXLogRecoveryImageDescriptorV1 descriptor, again;
	PGAlignedBlock changed;
	uint8 identity[CLUSTER_SPACE_IDENTITY_BYTES];
	reset();
	changed = pages[0];
	changed.data[SizeOfPageHeaderData] = 77;
	UT_ASSERT(GenericXLogRecoveryImagePrepare(GENERIC_XLOG_CTRC_ITL, &identities[0],
		MAIN_FORKNUM, 7, pages[0].data, changed.data, &frame));
	saved = frame;
	UT_ASSERT(GenericXLogRecoveryImageDescribe(&frame, &descriptor));
	UT_ASSERT(GenericXLogRecoveryImageDescribe(&frame, &again));
	UT_ASSERT(memcmp(&descriptor, &again, sizeof(descriptor)) == 0);
	UT_ASSERT(cluster_space_identity_encode(&identities[0], identity, sizeof(identity)));
	UT_ASSERT(memcmp(descriptor.space_identity, identity, sizeof(identity)) == 0);
	UT_ASSERT_EQ(descriptor.owner, GENERIC_XLOG_CTRC_ITL);
	UT_ASSERT_EQ(descriptor.fork_number, MAIN_FORKNUM);
	UT_ASSERT_EQ(descriptor.block_number, 7);
	UT_ASSERT_EQ(descriptor.result_token, frame.result_token);
	UT_ASSERT(memcmp(&descriptor.version, &frame.version, sizeof(frame.version)) == 0);
	UT_ASSERT(GenericXLogRecoveryImageMatchesDescriptor(&frame, &descriptor));
	frame.image.data[SizeOfPageHeaderData] ^= 1;
	UT_ASSERT(!GenericXLogRecoveryImageMatchesDescriptor(&frame, &descriptor));
	frame = saved;
	frame.block_number++;
	UT_ASSERT(!GenericXLogRecoveryImageMatchesDescriptor(&frame, &descriptor));
	frame = saved;
	descriptor.after_sha256[31] ^= 1;
	UT_ASSERT(!GenericXLogRecoveryImageMatchesDescriptor(&frame, &descriptor));
	UT_ASSERT(memcmp(&frame, &saved, sizeof(frame)) == 0);
	UT_ASSERT_EQ(raw_blocks + inserts + dirties, 0);
}

UT_TEST(test_recovery_descriptor_requires_valid_image_outside_insertion)
{
	GenericXLogRecoveryImageV1 frame;
	GenericXLogRecoveryImageDescriptorV1 descriptor, saved;
	reset();
	UT_ASSERT(GenericXLogRecoveryImagePrepare(GENERIC_XLOG_CTRC_MX, &identities[0],
		MAIN_FORKNUM, 7, pages[0].data, pages[0].data, &frame));
	memset(&descriptor, 0xa5, sizeof(descriptor));
	saved = descriptor;
	CritSectionCount++;
	UT_ASSERT(!GenericXLogRecoveryImageDescribe(&frame, &descriptor));
	CritSectionCount--;
	UT_ASSERT(memcmp(&descriptor, &saved, sizeof(descriptor)) == 0);
	frame.version.before.mutation_token = 0;
	UT_ASSERT(!GenericXLogRecoveryImageDescribe(&frame, &descriptor));
	UT_ASSERT(memcmp(&descriptor, &saved, sizeof(descriptor)) == 0);
	UT_ASSERT(!GenericXLogRecoveryImageDescribe(NULL, &descriptor));
	UT_ASSERT(!GenericXLogRecoveryImageDescribe(&frame, NULL));
	UT_ASSERT(!GenericXLogRecoveryImageMatchesDescriptor(&frame, NULL));
	UT_ASSERT_EQ(raw_blocks + inserts + dirties, 0);
}

UT_TEST(test_recovery_frame_refuses_bad_inputs_before_token_or_registration)
{
	for (unsigned fault = 0; fault < 9; fault++) {
		GenericXLogRecoveryImageV1 frame, saved;
		PGAlignedBlock changed;
		reset();
		changed = pages[0];
		memset(&frame, 0xa5, sizeof(frame));
		saved = frame;
		if (fault == 1) memset(identities[0].incarnation, 0, 16);
		if (fault == 2) ((PageHeader)pages[0].data)->pd_block_scn = 0;
		if (fault == 3) ((PageHeader)changed.data)->pd_block_scn++;
		if (fault == 4) ((PageHeader)changed.data)->pd_lower = BLCKSZ;
		if (fault == 5) identities[0].state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
		if (fault == 8) fixture_start_state = CLUSTER_NORMAL_START_EXISTING_OTHER;
		UT_ASSERT(!GenericXLogRecoveryImagePrepare(
			fault == 0 ? GENERIC_XLOG_ITL_FINISH : GENERIC_XLOG_CTRC_ITL,
			fault == 6 ? NULL : &identities[0], fault == 7 ? FSM_FORKNUM : MAIN_FORKNUM,
			7, pages[0].data, changed.data, &frame));
		UT_ASSERT(memcmp(&frame, &saved, sizeof(frame)) == 0);
		UT_ASSERT_EQ(next_token, 100);
		UT_ASSERT_EQ(raw_blocks, 0);
		UT_ASSERT_EQ(edge_count, 0);
		UT_ASSERT_EQ(inserts, 0);
	}
}

UT_TEST(test_recovery_frame_registration_rejects_bad_metadata_before_native_calls)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		GenericXLogRecoveryImageV1 frame;
		reset();
		UT_ASSERT(GenericXLogRecoveryImagePrepare(GENERIC_XLOG_CTRC_ITL,
			&identities[0], MAIN_FORKNUM, 7, pages[0].data, pages[0].data, &frame));
		if (fault == 0) frame.owner = GENERIC_XLOG_ITL_FINISH;
		if (fault == 1) frame.result_token = 0;
		if (fault == 2) frame.version.block_id = 1;
		if (fault == 3) frame.version.result_incarnation[0] ^= 1;
		if (fault == 4) ((PageHeader)frame.image.data)->pd_block_scn++;
		if (fault == 6) frame.image.data[((PageHeader)frame.image.data)->pd_lower] = 1;
		XLogBeginInsert();
		if (fault != 5) CritSectionCount++;
		UT_ASSERT(!GenericXLogRecoveryImageRegister(&frame));
		if (fault != 5) CritSectionCount--;
		UT_ASSERT_EQ(raw_blocks, 0);
		UT_ASSERT_EQ(edge_count, 0);
		UT_ASSERT_EQ(inserts, 0);
		UT_ASSERT_EQ(dirties, 0);
	}
}

UT_TEST(test_recovery_frame_output_overlap_leaves_both_images_unchanged)
{
	for (unsigned leg = 0; leg < 4; leg++) {
		PGAlignedBlock storage[3] = { 0 }, saved[3];
		Page input = (Page)storage + (leg & 1 ? MAXALIGN(1) : 0);
		GenericXLogRecoveryImageV1 *out
			= (GenericXLogRecoveryImageV1 *)((char *)storage + (leg & 1 ? 0 : MAXALIGN(1)));
		reset();
		memcpy(input, pages[0].data, BLCKSZ);
		memcpy(saved, storage, sizeof(storage));
		UT_ASSERT(!GenericXLogRecoveryImagePrepare(GENERIC_XLOG_CTRC_ITL, &identities[0],
			MAIN_FORKNUM, 7, leg < 2 ? input : pages[0].data,
			leg < 2 ? pages[0].data : input, out));
		UT_ASSERT(memcmp(storage, saved, sizeof(storage)) == 0);
		UT_ASSERT_EQ(next_token, 100);
		UT_ASSERT_EQ(raw_blocks, 0);
	}
}

int
main(void)
{
	UT_PLAN(14);
	UT_RUN(test_delta_carries_result_version);
	UT_RUN(test_multiple_relations_share_one_batch_token);
	UT_RUN(test_abort_preserves_all_page_bytes);
	UT_RUN(test_nonshared_native_generic_unchanged);
	UT_RUN(test_unrecognized_owner_and_public_entry_refuse);
	UT_RUN(test_wrong_namespace_missing_identity_and_zero_before_refuse);
	UT_RUN(test_second_page_drift_refuses_before_any_publication);
	UT_RUN(test_all_three_internal_owners_and_duplicate_registration);
	UT_RUN(test_recovery_full_image_frame_uses_native_block_without_publication);
	UT_RUN(test_recovery_frame_descriptor_retains_exact_target_version_and_digest);
	UT_RUN(test_recovery_descriptor_requires_valid_image_outside_insertion);
	UT_RUN(test_recovery_frame_refuses_bad_inputs_before_token_or_registration);
	UT_RUN(test_recovery_frame_registration_rejects_bad_metadata_before_native_calls);
	UT_RUN(test_recovery_frame_output_overlap_leaves_both_images_unchanged);
	UT_DONE();
	return ut_failed_count != 0;
}
