/*-------------------------------------------------------------------------
 *
 * test_cluster_native_disk_read.c
 *    Exercise exact first-fill authority across synchronous disk boundaries.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_native_disk_read.c
 *
 * NOTES
 *    Native/PCM/codec/ResourceOwner and page verification/checksum are real.
 *    KO/process/storage handles are explicit leaves; storage uses a host
 *    temporary file. Staged bytes are not installed or native completion.
 *
 *-------------------------------------------------------------------------
 */
#include "test_cluster_native_acquire_execution_fixture.inc"
#include "storage/checksum.h"
#include "storage/checksum_impl.h"
#include "storage/shmem.h"

static PGIOAlignedBlock disk_bytes, disk_output, disk_shared;
static FILE *disk_file;
static unsigned disk_enters, disk_ends, disk_opens, disk_reads, disk_bad_pages, disk_traces;
static bool disk_enter_ok, disk_end_ok, disk_open_error, disk_read_error;
static void (*disk_on_enter)(void), (*disk_on_open)(void), (*disk_on_read)(void),
	(*disk_on_end)(void);
static XLogRecPtr disk_lsn;
static uint64 disk_scn;

bool
DataChecksumsEnabled(void)
{
	return true;
} /* Configuration leaf only. */
void
pgstat_report_checksum_failure(void)
{
	disk_bad_pages++;
}
bool
ShmemAddrIsValid(const void *address)
{
	uintptr_t p = (uintptr_t)address, b = (uintptr_t)&disk_shared;
	return p >= b && p < b + sizeof(disk_shared);
}
static void
disk_idle(void)
{
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	UT_ASSERT((pg_atomic_read_u32(&request_buf.state) & BM_LOCKED) == 0);
}
static bool
disk_enter(RelFileLocator locator, ClusterKoDirectoryIoV3 *out)
{
	disk_idle();
	UT_ASSERT(RelFileLocatorEquals(locator, BufTagGetRelFileLocator(&request_base.tag)));
	disk_enters++;
	if (disk_on_enter != NULL)
		disk_on_enter();
	memset(out, 0, sizeof(*out));
	return disk_enter_ok;
}
static bool
disk_end(const ClusterKoDirectoryIoV3 *call)
{
	disk_idle();
	disk_ends++;
	if (disk_on_end != NULL)
		disk_on_end();
	return disk_end_ok;
}
static SMgrRelation
disk_open(RelFileLocator locator, BackendId backend)
{
	disk_idle();
	disk_opens++;
	UT_ASSERT(RelFileLocatorEquals(locator, BufTagGetRelFileLocator(&request_base.tag)));
	UT_ASSERT_EQ(backend, InvalidBackendId);
	if (disk_on_open != NULL)
		disk_on_open();
	if (disk_open_error)
		pg_re_throw();
	return (SMgrRelation)disk_file;
}
static void
disk_read(SMgrRelation rel, ForkNumber fork, BlockNumber block, void *page)
{
	disk_idle();
	disk_reads++;
	UT_ASSERT(rel == (SMgrRelation)disk_file);
	UT_ASSERT_EQ(fork, request_base.tag.forkNum);
	UT_ASSERT_EQ(block, request_base.tag.blockNum);
	if (disk_read_error)
		pg_re_throw();
	rewind(disk_file);
	UT_ASSERT_EQ(fread(page, 1, BLCKSZ, disk_file), BLCKSZ);
	if (disk_on_read != NULL)
		disk_on_read();
}
static void
disk_trace(BufferTag tag, const char *page, const char *stage, uint16 expected, uint16 actual)
{
	UT_ASSERT(BufferTagsEqual(&tag, &request_base.tag));
	UT_ASSERT(memcmp(page, disk_bytes.data, BLCKSZ) == 0);
	UT_ASSERT(strcmp(stage, "DISK_ACQUIRE") == 0);
	disk_traces++;
}
#define cluster_ko_directory_io_begin_v3 disk_enter
#define cluster_ko_directory_io_end_v3 disk_end
#define smgropen disk_open
#define smgrread disk_read
#define cluster_block_io_trace_failed disk_trace
#define LockBufHdr stage_header_lock
#define GetPrivateRefCount(buffer) request_private_pin(buffer)
#include "test_cluster_native_disk_read.inc"
#undef GetPrivateRefCount
#undef LockBufHdr
#undef cluster_block_io_trace_failed
#undef smgrread
#undef smgropen
#undef cluster_ko_directory_io_end_v3
#undef cluster_ko_directory_io_begin_v3

static void
disk_store(void)
{
	if (disk_file != NULL)
		fclose(disk_file);
	disk_file = tmpfile();
	UT_ASSERT(disk_file != NULL);
	UT_ASSERT_EQ(fwrite(disk_bytes.data, 1, BLCKSZ, disk_file), BLCKSZ);
	UT_ASSERT_EQ(fflush(disk_file), 0);
}
static bool
disk_start(EvidenceFixture *f, int profile)
{
	if (!execution_start(f, profile, -1) || execution_enter() != RESOURCE_X_APPLY_APPLIED)
		return false;
	disk_enters = disk_ends = disk_opens = disk_reads = disk_bad_pages = disk_traces = 0;
	disk_enter_ok = disk_end_ok = true;
	disk_open_error = disk_read_error = false;
	disk_on_enter = disk_on_open = disk_on_read = disk_on_end = NULL;
	memset(&disk_bytes, 0x53, sizeof(disk_bytes));
	memset(disk_bytes.data, 0, SizeOfPageHeaderData);
	((PageHeader)disk_bytes.data)->pd_lower = SizeOfPageHeaderData;
	((PageHeader)disk_bytes.data)->pd_upper = BLCKSZ;
	((PageHeader)disk_bytes.data)->pd_special = BLCKSZ;
	((PageHeader)disk_bytes.data)->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	PageSetLSNPreserveOrigin((Page)disk_bytes.data, 12345);
	((PageHeader)disk_bytes.data)->pd_block_scn = 93;
	((PageHeader)disk_bytes.data)->pd_checksum
		= pg_checksum_page(disk_bytes.data, request_base.tag.blockNum);
	disk_store();
	memset(&disk_output, 0xA5, sizeof(disk_output));
	disk_lsn = UINT64_C(0xA5A5A5A5A5A5A5A5);
	disk_scn = disk_lsn;
	request_rechecks = 0;
	request_hook = NULL;
	return true;
}
static ResourceXApplyResult
disk_run(void)
{
	return cluster_bufmgr_pcm_own_acquire_read_disk_exact(
		&request_buf, &request_base, &prepared_episode, &request_admission, &execution_claim,
		&disk_output, &disk_lsn, &disk_scn);
}
static void
disk_preserved(void)
{
	PGIOAlignedBlock canary;
	memset(&canary, 0xA5, sizeof(canary));
	UT_ASSERT(memcmp(&canary, &disk_output, sizeof(canary)) == 0);
	UT_ASSERT_EQ(disk_lsn, UINT64_C(0xA5A5A5A5A5A5A5A5));
	UT_ASSERT_EQ(disk_scn, disk_lsn);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
				 RESOURCE_X_DELIVERY_ACQUIRE_EXECUTING);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 1);
}
static void
disk_lose_io(void)
{
	ResourceOwnerForgetBufferIO(original_io_owner, 1);
}
static void
disk_cancel(void)
{
	request_entry()->resource_x_bootstrap_round.cancelled_attempt_floor = 1;
}
static void
disk_turnover(void)
{
	pg_atomic_write_u64(&request_own.generation, 99);
}
static void
disk_change_owner(void)
{
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "successor");
}

UT_TEST(test_disk_only_original_full_disk_S_and_known_X_can_read)
{
	for (int profile = 0; profile < 2; profile++) {
		EvidenceFixture f;
		ClusterPcmOwnEntry own;
		uint32 state;
		ClusterPcmResourceXBootstrapRound round;
		UT_ASSERT(disk_start(&f, profile));
		own = request_own;
		state = pg_atomic_read_u32(&request_buf.state);
		round = request_entry()->resource_x_bootstrap_round;
		UT_ASSERT_EQ(disk_run(), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(disk_reads, 1);
		UT_ASSERT_EQ(disk_ends, 1);
		UT_ASSERT(memcmp(&disk_output, &disk_bytes, sizeof(disk_bytes)) == 0);
		UT_ASSERT_EQ(disk_lsn, 12345);
		UT_ASSERT_EQ(disk_scn, 93);
		request_native_unchanged(&own, state);
		UT_ASSERT(memcmp(&round, &request_entry()->resource_x_bootstrap_round, sizeof(round)) == 0);
		UT_ASSERT(ResourceOwnerHasBufferIO(original_io_owner, 1));
	}
}
UT_TEST(test_disk_remote_local_current_or_resident_never_reads_disk)
{
	for (int profile = 2; profile < 8; profile++) {
		EvidenceFixture f;
		UT_ASSERT(disk_start(&f, profile));
		UT_ASSERT(disk_run() != RESOURCE_X_APPLY_APPLIED);
		disk_preserved();
		UT_ASSERT_EQ(disk_enters, 0);
		UT_ASSERT_EQ(disk_reads, 0);
	}
}
UT_TEST(test_disk_stale_execution_or_episode_cannot_read)
{
	for (int n = 0; n < 4; n++) {
		EvidenceFixture f;
		UT_ASSERT(disk_start(&f, 0));
		if (n == 0)
			execution_claim.executor_sequence++;
		if (n == 1)
			execution_claim.purpose = RESOURCE_X_DELIVERY_ACQUIRE_PREPARED;
		if (n == 2)
			prepared_episode.backend_generation++;
		if (n == 3)
			execution_claim.reserved[0] = 1;
		UT_ASSERT(disk_run() != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(disk_enters, 0);
		UT_ASSERT_EQ(disk_reads, 0);
	}
}
UT_TEST(test_disk_original_io_owner_is_required_before_KO)
{
	EvidenceFixture f;
	UT_ASSERT(disk_start(&f, 0));
	disk_lose_io();
	UT_ASSERT(disk_run() != RESOURCE_X_APPLY_APPLIED);
	disk_preserved();
	UT_ASSERT_EQ(disk_enters, 0);
	UT_ASSERT_EQ(disk_reads, 0);
}
UT_TEST(test_disk_KO_provider_loses_io_before_open)
{
	EvidenceFixture f;
	UT_ASSERT(disk_start(&f, 0));
	disk_on_enter = disk_lose_io;
	UT_ASSERT(disk_run() != RESOURCE_X_APPLY_APPLIED);
	disk_preserved();
	UT_ASSERT_EQ(disk_ends, 1);
	UT_ASSERT_EQ(disk_opens, 0);
	UT_ASSERT_EQ(disk_reads, 0);
}
UT_TEST(test_disk_open_cancel_prevents_any_read)
{
	EvidenceFixture f;
	UT_ASSERT(disk_start(&f, 0));
	disk_on_open = disk_cancel;
	UT_ASSERT(disk_run() != RESOURCE_X_APPLY_APPLIED);
	disk_preserved();
	UT_ASSERT_EQ(disk_ends, 1);
	UT_ASSERT_EQ(disk_opens, 1);
	UT_ASSERT_EQ(disk_reads, 0);
}
UT_TEST(test_disk_open_turnover_prevents_any_read)
{
	EvidenceFixture f;
	UT_ASSERT(disk_start(&f, 0));
	disk_on_open = disk_turnover;
	UT_ASSERT(disk_run() != RESOURCE_X_APPLY_APPLIED);
	disk_preserved();
	UT_ASSERT_EQ(disk_reads, 0);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 99);
}
UT_TEST(test_disk_read_cancel_discards_scratch_and_keeps_original_debt)
{
	EvidenceFixture f;
	UT_ASSERT(disk_start(&f, 0));
	disk_on_read = disk_cancel;
	UT_ASSERT(disk_run() != RESOURCE_X_APPLY_APPLIED);
	disk_preserved();
	UT_ASSERT_EQ(disk_reads, 1);
	UT_ASSERT_EQ(disk_ends, 1);
}
UT_TEST(test_disk_read_ERROR_ends_KO_and_keeps_original_IO)
{
	EvidenceFixture f;
	volatile bool caught = false;
	UT_ASSERT(disk_start(&f, 0));
	disk_read_error = true;
	PG_TRY();
	{
		(void)disk_run();
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(disk_ends, 1);
	disk_preserved();
	UT_ASSERT(ResourceOwnerHasBufferIO(original_io_owner, 1));
}
UT_TEST(test_disk_KO_refusal_opens_nothing_and_preserves_execution)
{
	EvidenceFixture f;
	UT_ASSERT(disk_start(&f, 0));
	disk_enter_ok = false;
	UT_ASSERT(disk_run() != RESOURCE_X_APPLY_APPLIED);
	disk_preserved();
	UT_ASSERT_EQ(disk_opens, 0);
	UT_ASSERT_EQ(disk_reads, 0);
	UT_ASSERT_EQ(disk_ends, 0);
}
UT_TEST(test_disk_KO_end_unknown_cannot_export_bytes)
{
	EvidenceFixture f;
	UT_ASSERT(disk_start(&f, 0));
	disk_end_ok = false;
	UT_ASSERT(disk_run() != RESOURCE_X_APPLY_APPLIED);
	disk_preserved();
	UT_ASSERT_EQ(disk_reads, 1);
	UT_ASSERT_EQ(disk_ends, 1);
}
UT_TEST(test_disk_open_ERROR_still_ends_original_KO)
{
	EvidenceFixture f;
	volatile bool caught = false;
	UT_ASSERT(disk_start(&f, 0));
	disk_open_error = true;
	PG_TRY();
	{
		(void)disk_run();
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(disk_ends, 1);
	UT_ASSERT_EQ(disk_reads, 0);
	disk_preserved();
}
UT_TEST(test_disk_actual_checksum_failure_never_exports)
{
	EvidenceFixture f;
	UT_ASSERT(disk_start(&f, 0));
	disk_bytes.data[BLCKSZ - 1] ^= 1;
	disk_store();
	UT_ASSERT(disk_run() != RESOURCE_X_APPLY_APPLIED);
	disk_preserved();
	UT_ASSERT_EQ(disk_bad_pages, 1);
	UT_ASSERT_EQ(disk_traces, 1);
}
UT_TEST(test_disk_shape_failure_with_valid_checksum_never_exports)
{
	EvidenceFixture f;
	UT_ASSERT(disk_start(&f, 0));
	((PageHeader)disk_bytes.data)->pd_upper = 1;
	((PageHeader)disk_bytes.data)->pd_checksum
		= pg_checksum_page(disk_bytes.data, request_base.tag.blockNum);
	disk_store();
	UT_ASSERT(disk_run() != RESOURCE_X_APPLY_APPLIED);
	disk_preserved();
	UT_ASSERT_EQ(disk_bad_pages, 0);
	UT_ASSERT_EQ(disk_traces, 1);
}
UT_TEST(test_disk_content_holder_refused_before_provider_or_entry)
{
	EvidenceFixture f;
	LWLock *lock;
	UT_ASSERT(disk_start(&f, 0));
	lock = BufferDescriptorGetContentLock(&request_buf);
	LWLockAcquire(lock, LW_EXCLUSIVE);
	UT_ASSERT(disk_run() != RESOURCE_X_APPLY_APPLIED);
	LWLockRelease(lock);
	disk_preserved();
	UT_ASSERT_EQ(disk_enters, 0);
}
UT_TEST(test_disk_shared_destination_is_not_private_staging)
{
	EvidenceFixture f;
	UT_ASSERT(disk_start(&f, 0));
	UT_ASSERT(cluster_bufmgr_pcm_own_acquire_read_disk_exact(
				  &request_buf, &request_base, &prepared_episode, &request_admission,
				  &execution_claim, &disk_shared, &disk_lsn, &disk_scn)
			  != RESOURCE_X_APPLY_APPLIED);
	disk_preserved();
	UT_ASSERT_EQ(disk_enters, 0);
}
UT_TEST(test_disk_KO_end_owner_change_cannot_export_or_abort_old_IO)
{
	EvidenceFixture f;
	UT_ASSERT(disk_start(&f, 0));
	disk_on_end = disk_change_owner;
	UT_ASSERT(disk_run() != RESOURCE_X_APPLY_APPLIED);
	disk_preserved();
	UT_ASSERT(ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT(!ResourceOwnerHasBufferIO(CurrentResourceOwner, 1));
}
UT_TEST(test_disk_overlapping_outputs_cannot_rewrite_staged_bytes)
{
	EvidenceFixture f;
	UT_ASSERT(disk_start(&f, 0));
	UT_ASSERT(cluster_bufmgr_pcm_own_acquire_read_disk_exact(
				  &request_buf, &request_base, &prepared_episode, &request_admission,
				  &execution_claim, &disk_output, (XLogRecPtr *)disk_output.data, &disk_scn)
			  != RESOURCE_X_APPLY_APPLIED);
	disk_preserved();
	UT_ASSERT_EQ(disk_enters, 0);
}
UT_TEST(test_disk_original_all_zero_page_semantics_are_preserved)
{
	EvidenceFixture f;
	UT_ASSERT(disk_start(&f, 0));
	memset(&disk_bytes, 0, sizeof(disk_bytes));
	disk_store();
	UT_ASSERT_EQ(disk_run(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&disk_output, &disk_bytes, sizeof(disk_bytes)) == 0);
	UT_ASSERT_EQ(disk_lsn, 0);
	UT_ASSERT_EQ(disk_scn, 0);
	UT_ASSERT_EQ(disk_bad_pages, 0);
}
UT_TEST(test_disk_wrong_typed_fork_with_valid_checksum_is_rejected)
{
	EvidenceFixture f;
	UT_ASSERT(disk_start(&f, 0));
	((PageHeader)disk_bytes.data)->pd_flags = PD_SPACE_METADATA;
	((PageHeader)disk_bytes.data)->pd_checksum
		= pg_checksum_page(disk_bytes.data, request_base.tag.blockNum);
	disk_store();
	UT_ASSERT(disk_run() != RESOURCE_X_APPLY_APPLIED);
	disk_preserved();
	UT_ASSERT_EQ(disk_bad_pages, 0);
	UT_ASSERT_EQ(disk_traces, 1);
}
int
main(void)
{
	UT_PLAN(20);
	UT_RUN(test_disk_only_original_full_disk_S_and_known_X_can_read);
	UT_RUN(test_disk_remote_local_current_or_resident_never_reads_disk);
	UT_RUN(test_disk_stale_execution_or_episode_cannot_read);
	UT_RUN(test_disk_original_io_owner_is_required_before_KO);
	UT_RUN(test_disk_KO_provider_loses_io_before_open);
	UT_RUN(test_disk_open_cancel_prevents_any_read);
	UT_RUN(test_disk_open_turnover_prevents_any_read);
	UT_RUN(test_disk_read_cancel_discards_scratch_and_keeps_original_debt);
	UT_RUN(test_disk_read_ERROR_ends_KO_and_keeps_original_IO);
	UT_RUN(test_disk_KO_refusal_opens_nothing_and_preserves_execution);
	UT_RUN(test_disk_KO_end_unknown_cannot_export_bytes);
	UT_RUN(test_disk_open_ERROR_still_ends_original_KO);
	UT_RUN(test_disk_actual_checksum_failure_never_exports);
	UT_RUN(test_disk_shape_failure_with_valid_checksum_never_exports);
	UT_RUN(test_disk_content_holder_refused_before_provider_or_entry);
	UT_RUN(test_disk_shared_destination_is_not_private_staging);
	UT_RUN(test_disk_KO_end_owner_change_cannot_export_or_abort_old_IO);
	UT_RUN(test_disk_overlapping_outputs_cannot_rewrite_staged_bytes);
	UT_RUN(test_disk_original_all_zero_page_semantics_are_preserved);
	UT_RUN(test_disk_wrong_typed_fork_with_valid_checksum_is_rejected);
	if (disk_file != NULL)
		fclose(disk_file);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
