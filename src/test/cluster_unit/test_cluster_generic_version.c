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
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/storage/cluster_smgr.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool cluster_enabled = true, cluster_shared_config = true;
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
static uint64 next_token, wal_token;
static bool begun, expecting_error;
static jmp_buf error_jump;

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
	CritSectionCount = 0;
	edge_count = inserts = dirties = 0;
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

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_delta_carries_result_version);
	UT_RUN(test_multiple_relations_share_one_batch_token);
	UT_RUN(test_abort_preserves_all_page_bytes);
	UT_RUN(test_nonshared_native_generic_unchanged);
	UT_RUN(test_unrecognized_owner_and_public_entry_refuse);
	UT_RUN(test_wrong_namespace_missing_identity_and_zero_before_refuse);
	UT_RUN(test_second_page_drift_refuses_before_any_publication);
	UT_RUN(test_all_three_internal_owners_and_duplicate_registration);
	UT_DONE();
	return ut_failed_count != 0;
}
