/* Actual native insert/stamp and target installer, with real-file SMGR I/O. */
int recovery_fpi_insert_fixture_main(void);
#define recovery_fpi_hash_create recovery_fpi_insert_hash_create
#define ClusterStartupInstalledRecoveryWriterV1 recovery_fpi_insert_installed_writer
#define cluster_scn_advance recovery_fpi_insert_scn_advance
#define RECOVERY_FPI_INSERT_MAIN recovery_fpi_insert_fixture_main
#include "test_cluster_recovery_fpi_insert.c"
#undef recovery_fpi_hash_create
#undef ClusterStartupInstalledRecoveryWriterV1
#undef cluster_scn_advance

#include "cluster/cluster_page_install.h"
#include "access/xlogrecovery.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_space_reservation.h"
#include "storage/checksum.h"
#include "storage/smgr.h"
#include <fcntl.h>
#include <unistd.h>

bool cluster_recmerge_window_active, cluster_recmerge_apply_foreign;
uint64 cluster_recmerge_window_scn, cluster_recmerge_window_own_lsn;

static SMgrRelationData install_relation;
static ClusterRecoveryFpiInsertV1 inserted;
static int install_fd = -1;
static char install_path[64];
static bool installing, checksums, postread_corrupt;
static unsigned reads, writes, syncs, opens, install_hashes;
static unsigned throw_read_at;
static bool throw_write, throw_sync, drift_after_read, drift_after_sync, drift_after_postread;
static bool writer_drift_after_read, missing_target;
static XLogRecPtr native_flush_end;
static TimeLineID native_flush_timeline;
static bool reading_input, input_current;
static unsigned input_reads, input_throw_at, input_drift_at, input_drift_kind;
static unsigned input_current_checks;
static int input_space_fd = -1;
static char input_space_path[64], input_start_object;
static ClusterColdTypedV1 input_typed;
static PGAlignedBlock input_work_storage;
static ClusterRecoveryCheckpointOwnerV1 input_owner;
static BlockNumber input_space_blocks;
static bool input_missing_space;
static RfPageRecoveryCtrcItlSelectionV1 input_itls[CLUSTER_ITL_INITRANS_DEFAULT + 1];
static unsigned input_token_fault;
static bool preparing_input_frame;
#define input_start ((struct ClusterRecoveryStartV1 *)&input_start_object)
#define input_work ((ClusterColdCtrcWorkV1 *)input_work_storage.data)

/* Original opaque Start/work admission is a controlled boundary here; its
 * native implementation is tested separately by test_cluster_cold_tx_start. */
bool
ClusterColdRecoveryCtrcWorkCurrent(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work)
{
	input_current_checks++;
	return input_current && start == input_start && work == input_work;
}

const ClusterColdTypedV1 *
ClusterColdRecoveryTxInputs(const struct ClusterRecoveryStartV1 *start)
{
	return input_current && start == input_start ? &input_typed : NULL;
}

bool
ClusterColdRecoveryTxCheckpointOwnerV1(const struct ClusterRecoveryStartV1 *start,
	ClusterRecoveryCheckpointOwnerV1 *out)
{
	memset(out, 0, sizeof(*out));
	if (!input_current || start != input_start)
		return false;
	*out = input_owner;
	return true;
}

pg_cryptohash_ctx *recovery_fpi_hash_create(pg_cryptohash_type type);
bool ClusterStartupInstalledRecoveryWriterV1(ClusterWalWriterToken *writer,
	uint8 operation_uuid[16], uint64 *generation);
bool
ClusterStartupInstalledRecoveryWriterV1(ClusterWalWriterToken *writer,
	uint8 operation_uuid[16], uint64 *generation)
{
	if (!installing)
		return recovery_fpi_insert_installed_writer(writer, operation_uuid, generation);
	UT_ASSERT(promoted && CritSectionCount == 0);
	if (!installed)
		return false;
	*writer = installed_writer;
	memset(operation_uuid, 8, 16);
	*generation = 9;
	return true;
}

pg_cryptohash_ctx *
recovery_fpi_hash_create(pg_cryptohash_type type)
{
	if (!installing)
		return recovery_fpi_insert_hash_create(type);
	UT_ASSERT(promoted && CritSectionCount == 0);
	install_hashes++;
	return pg_cryptohash_create(type);
}

bool DataChecksumsEnabled(void) { return checksums; }
XLogRecPtr
GetFlushRecPtr(TimeLineID *timeline)
{
	UT_ASSERT_EQ(CritSectionCount, 0);
	if (timeline != NULL)
		*timeline = native_flush_timeline;
	return native_flush_end;
}

static void
install_io_check(void)
{
	if (reading_input) {
		UT_ASSERT_EQ(CritSectionCount, 0);
		return;
	}
	UT_ASSERT(installing && promoted && flushes == 1);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_INSERTED);
	UT_ASSERT(!region.thread_replay[2].stamp.failed);
	UT_ASSERT(!inserted.target_synced);
	UT_ASSERT(fpi_serial.held);
}

SMgrRelation
smgropen(RelFileLocator locator, BackendId backend)
{
	install_io_check();
	UT_ASSERT(RelFileLocatorEquals(locator, frame.identity.key.locator));
	UT_ASSERT_EQ(backend, InvalidBackendId);
	opens++;
	return &install_relation;
}

bool
smgrexists(SMgrRelation relation, ForkNumber forknum)
{
	install_io_check();
	if (reading_input && forknum == SPACE_FORKNUM) {
		UT_ASSERT(relation == &install_relation);
		return !input_missing_space;
	}
	UT_ASSERT(relation == &install_relation && forknum == MAIN_FORKNUM);
	return !missing_target;
}

BlockNumber
smgrnblocks(SMgrRelation relation, ForkNumber forknum)
{
	install_io_check();
	if (reading_input && forknum == SPACE_FORKNUM) {
		UT_ASSERT(relation == &install_relation);
		return input_space_blocks;
	}
	UT_ASSERT(relation == &install_relation && forknum == MAIN_FORKNUM);
	return 8;
}

void
smgrread(SMgrRelation relation, ForkNumber forknum, BlockNumber block, void *buffer)
{
	install_io_check();
	if (reading_input) {
		int fd = forknum == SPACE_FORKNUM ? input_space_fd : install_fd;
		UT_ASSERT(relation == &install_relation);
		UT_ASSERT((forknum == SPACE_FORKNUM && block < 2)
			|| (forknum == MAIN_FORKNUM && block == 7));
		input_reads++;
		if (input_reads == input_throw_at)
			ereport(ERROR, (errmsg("test recovery page input read error")));
		UT_ASSERT_EQ(pread(fd, buffer, BLCKSZ, (off_t)block * BLCKSZ), BLCKSZ);
		if (input_reads == input_drift_at) {
			switch (input_drift_kind) {
			case 0: input_current = false; break;
			case 1: input_work->generation++; break;
			case 2: input_owner.install_generation++; break;
			case 3: input_owner.writer.epoch++; break;
			case 4: input_owner.operation_uuid[15]++; break;
			case 5: fpi_protection_current = false; break;
			case 6: fpi_serial.duty.root_lineage_seq++; break;
			case 7: input_typed.sources[0].claim.identity.root_lineage_seq++; break;
			case 8: input_space_blocks++; break;
			case 9: ((char *)buffer)[BLCKSZ - 1] ^= 1; break;
			case 10: input_work->counts.unresolved_count++; break;
			case 11: input_work->entries[0].slot.commit_scn++; break;
			case 12: input_itls[0].subject_index++; break;
			case 13: input_work->entries[0].key.segment_generation++; break;
			case 14: input_itls[0].expected_itl_wrap++; break;
			}
		}
		return;
	}
	UT_ASSERT(relation == &install_relation && forknum == MAIN_FORKNUM && block == 7);
	reads++;
	if (reads == throw_read_at)
		ereport(ERROR, (errmsg("test target read error")));
	UT_ASSERT_EQ(pread(install_fd, buffer, BLCKSZ, (off_t)block * BLCKSZ), BLCKSZ);
	if (reads == 1 && drift_after_read)
		fpi_protection_current = false;
	if (reads == 1 && writer_drift_after_read)
		stale_writer_at = writer_checks + 1;
	if (reads == 2 && postread_corrupt)
		((char *)buffer)[SizeOfPageHeaderData] ^= 1;
	if (reads == 2 && drift_after_postread)
		fpi_protection_current = false;
}

void
smgrwrite(SMgrRelation relation, ForkNumber forknum, BlockNumber block, const void *buffer,
	bool skip_fsync)
{
	install_io_check();
	UT_ASSERT(relation == &install_relation && forknum == MAIN_FORKNUM && block == 7);
	UT_ASSERT(!skip_fsync && reads == 1);
	writes++;
	if (throw_write)
		ereport(ERROR, (errmsg("test target write error")));
	UT_ASSERT_EQ(pwrite(install_fd, buffer, BLCKSZ, (off_t)block * BLCKSZ), BLCKSZ);
}

void
smgrimmedsync(SMgrRelation relation, ForkNumber forknum)
{
	install_io_check();
	UT_ASSERT(relation == &install_relation && forknum == MAIN_FORKNUM);
	syncs++;
	if (throw_sync)
		ereport(ERROR, (errmsg("test target sync error")));
	UT_ASSERT_EQ(fsync(install_fd), 0);
	if (drift_after_sync)
		fpi_protection_current = false;
}

/* This entry must never extend or create an unproven target. */
void
smgrextend(SMgrRelation relation, ForkNumber forknum, BlockNumber block, const void *buffer,
	bool skip_fsync)
{
	(void)relation; (void)forknum; (void)block; (void)buffer; (void)skip_fsync;
	abort();
}

static void
install_file_close(void)
{
	if (input_space_fd >= 0) {
		close(input_space_fd);
		unlink(input_space_path);
		input_space_fd = -1;
	}
	if (install_fd >= 0) {
		close(install_fd);
		unlink(install_path);
		install_fd = -1;
	}
}

static void
install_file_write(PGAlignedBlock *page)
{
	((PageHeader)page->data)->pd_checksum = 0;
	/* Native checksum requires an initialized page; preserve the zero-page
	 * negative input so the product installer, rather than this fixture, refuses it. */
	if (checksums && !PageIsNew((Page)page->data))
		((PageHeader)page->data)->pd_checksum = pg_checksum_page(page->data, 7);
	UT_ASSERT_EQ(pwrite(install_fd, page->data, BLCKSZ, (off_t)7 * BLCKSZ), BLCKSZ);
	UT_ASSERT_EQ(fsync(install_fd), 0);
}

static void
reset_install(bool enable_checksums)
{
	PGAlignedBlock before;
	install_file_close();
	reading_input = false;
	installing = false;
	reset_insert();
	UT_ASSERT(finish_image(&inserted));
	checksums = enable_checksums;
	reads = writes = syncs = opens = install_hashes = 0;
	throw_read_at = 0;
	throw_write = throw_sync = postread_corrupt = false;
	drift_after_read = drift_after_sync = drift_after_postread = false;
	writer_drift_after_read = missing_target = false;
	native_flush_end = inserted.record_end;
	native_flush_timeline = installed_writer.ref.timeline;
	strlcpy(install_path, "/tmp/pgrac-fpi-install-XXXXXX", sizeof(install_path));
	install_fd = mkstemp(install_path);
	UT_ASSERT(install_fd >= 0);
	UT_ASSERT_EQ(ftruncate(install_fd, (off_t)8 * BLCKSZ), 0);
	before = predecessor;
	install_file_write(&before);
}

static bool
install_image(void)
{
	bool result;
	installing = true;
	result = rf_page_recovery_fpi_install_smgr_v1(&inserted, &fpi_serial,
		(ClusterWalRetentionPin *)&fpi_pin_object);
	installing = false;
	return result;
}

static void
assert_install_pending(bool failed)
{
	UT_ASSERT(inserted.guard == (RfPageAuthorityGuardV1 *)&fpi_guard_object);
	UT_ASSERT(fpi_serial.held);
	UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_INSERTED);
	UT_ASSERT_EQ(region.thread_replay[2].stamp.failed, failed);
	UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&inserted.window));
	UT_ASSERT(!cluster_recovery_stamp_clear_v1(&inserted.window));
}

UT_TEST(fpi_install_syncs_exact_native_lsn_and_checksum_without_retiring_window)
{
	for (unsigned checksum = 0; checksum < 2; checksum++) {
		PGAlignedBlock expected, actual;
		GenericXLogRecoveryImageV1 saved;
		reset_install(checksum != 0);
		saved = inserted.image;
		UT_ASSERT(install_image());
		UT_ASSERT(inserted.target_synced);
		UT_ASSERT_EQ(opens, 1); UT_ASSERT_EQ(reads, 2);
		UT_ASSERT_EQ(writes, 1); UT_ASSERT_EQ(syncs, 1);
		UT_ASSERT_EQ(install_hashes, 1);
		expected = saved.image;
		PageSetLSN(expected.data, inserted.record_end);
		((PageHeader)expected.data)->pd_checksum = 0;
		if (checksums)
			((PageHeader)expected.data)->pd_checksum = pg_checksum_page(expected.data, 7);
		UT_ASSERT_EQ(pread(install_fd, actual.data, BLCKSZ, (off_t)7 * BLCKSZ), BLCKSZ);
		UT_ASSERT(memcmp(actual.data, expected.data, BLCKSZ) == 0);
		UT_ASSERT(memcmp(&saved, &inserted.image, sizeof(saved)) == 0);
		assert_install_pending(false);
	}
}

UT_TEST(fpi_install_exact_result_retries_sync_and_postread_without_rewriting)
{
	reset_install(true);
	UT_ASSERT(install_image());
	UT_ASSERT(install_image());
	UT_ASSERT_EQ(writes, 1); UT_ASSERT_EQ(syncs, 2); UT_ASSERT_EQ(reads, 4);
	UT_ASSERT(inserted.target_synced);
	assert_install_pending(false);
}

UT_TEST(fpi_install_refuses_wrong_window_flush_frame_actor_or_serial_before_io)
{
	for (unsigned fault = 0; fault < 14; fault++) {
		reset_install(false);
		if (fault == 0) inserted.wal_flushed = false;
		if (fault == 1) native_flush_end--;
		if (fault == 2) inserted.record_start++;
		if (fault == 3) inserted.record_end++;
		if (fault == 4) inserted.window.payload[0] ^= 1;
		if (fault == 5) inserted.image.image.data[SizeOfPageHeaderData] ^= 1;
		if (fault == 6) fpi_serial.held = false;
		if (fault == 7) fpi_serial.mode = CLUSTER_RECOVERY_SERIAL_INPUT_SEAL;
		if (fault == 8) MyBackendType = B_BACKEND;
		if (fault == 9) ShutdownRequestPending = 1;
		if (fault == 10) recovering = true;
		if (fault == 11) region.thread_replay[2].stamp.failed = true;
		if (fault == 12) native_flush_timeline++;
		if (fault == 13) installed = false;
		UT_ASSERT(!install_image());
		UT_ASSERT(!inserted.target_synced);
		UT_ASSERT_EQ(opens, 0); UT_ASSERT_EQ(reads, 0); UT_ASSERT_EQ(writes, 0);
		UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_INSERTED);
		UT_ASSERT(inserted.guard != NULL);
	}
}

UT_TEST(fpi_install_refuses_missing_torn_third_version_and_changed_result)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		PGAlignedBlock bad, before, after;
		reset_install(true);
		bad = predecessor;
		if (fault == 0) missing_target = true;
		if (fault == 1) ((PageHeader)bad.data)->pd_block_scn = 777;
		if (fault == 2) {
			bad = inserted.image.image;
			PageSetLSN(bad.data, inserted.record_end);
			bad.data[SizeOfPageHeaderData] ^= 1;
		}
		if (fault == 3) memset(&bad, 0, sizeof(bad));
		install_file_write(&bad);
		if (fault == 4) {
			bad.data[SizeOfPageHeaderData] ^= 1;
			UT_ASSERT_EQ(pwrite(install_fd, bad.data, BLCKSZ, (off_t)7 * BLCKSZ), BLCKSZ);
		}
		UT_ASSERT_EQ(pread(install_fd, before.data, BLCKSZ, (off_t)7 * BLCKSZ), BLCKSZ);
		UT_ASSERT(!install_image());
		UT_ASSERT_EQ(writes, 0); UT_ASSERT_EQ(syncs, 0);
		UT_ASSERT_EQ(pread(install_fd, after.data, BLCKSZ, (off_t)7 * BLCKSZ), BLCKSZ);
		UT_ASSERT(memcmp(before.data, after.data, BLCKSZ) == 0);
		UT_ASSERT(!inserted.target_synced);
		assert_install_pending(true);
	}
}

UT_TEST(fpi_install_authority_or_writer_drift_never_claims_completion)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		reset_install(true);
		drift_after_read = fault == 0;
		writer_drift_after_read = fault == 1;
		drift_after_sync = fault == 2;
		drift_after_postread = fault == 3;
		postread_corrupt = fault == 4;
		UT_ASSERT(!install_image());
		UT_ASSERT(!inserted.target_synced);
		UT_ASSERT_EQ(writes, fault < 2 ? 0 : 1);
		assert_install_pending(true);
	}
}

UT_TEST(fpi_install_io_exceptions_rethrow_and_keep_inserted_debt)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		volatile bool caught = false;
		reset_install(true);
		throw_read_at = fault == 0 ? 1 : fault == 3 ? 2 : 0;
		throw_write = fault == 1;
		throw_sync = fault == 2;
		PG_TRY();
		{
			(void)install_image();
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(raised_level, ERROR);
		UT_ASSERT_EQ(CritSectionCount, 0);
		UT_ASSERT(!inserted.target_synced);
		assert_install_pending(true);
	}
}

UT_TEST(fpi_inserted_observer_is_exact_and_never_advances_window)
{
	ClusterRecoveryStampHandleV1 changed_handle;
	reset_install(false);
	changed_handle = inserted.window;
	UT_ASSERT(cluster_recovery_stamp_fpi_inserted_current_v1(&inserted.window,
		inserted.record_start, inserted.record_end));
	changed_handle.key.window_seq++;
	UT_ASSERT(!cluster_recovery_stamp_fpi_inserted_current_v1(&changed_handle,
		inserted.record_start, inserted.record_end));
	UT_ASSERT(!cluster_recovery_stamp_fpi_inserted_current_v1(&inserted.window,
		inserted.record_start, inserted.record_end + 1));
	UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_INSERTED);
	UT_ASSERT(!region.thread_replay[2].stamp.failed);
	region.thread_replay[2].stamp.phase = CLUSTER_RECOVERY_STAMP_ADOPT_PENDING;
	region.thread_replay[2].stamp.adopt_from = CLUSTER_RECOVERY_STAMP_INSERTED;
	UT_ASSERT(!cluster_recovery_stamp_fpi_inserted_current_v1(&inserted.window,
		inserted.record_start, inserted.record_end));
	UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_ADOPT_PENDING);
}

#include "test_cluster_recovery_ctrc_read.inc"
#include "test_cluster_recovery_ctrc_itl.inc"
#include "test_cluster_recovery_ctrc_fpi.inc"

int
main(void)
{
	atexit(install_file_close);
	UT_PLAN(30);
	UT_RUN(fpi_install_syncs_exact_native_lsn_and_checksum_without_retiring_window);
	UT_RUN(fpi_install_exact_result_retries_sync_and_postread_without_rewriting);
	UT_RUN(fpi_install_refuses_wrong_window_flush_frame_actor_or_serial_before_io);
	UT_RUN(fpi_install_refuses_missing_torn_third_version_and_changed_result);
	UT_RUN(fpi_install_authority_or_writer_drift_never_claims_completion);
	UT_RUN(fpi_install_io_exceptions_rethrow_and_keep_inserted_debt);
	UT_RUN(fpi_inserted_observer_is_exact_and_never_advances_window);
	UT_RUN(ctrc_input_reads_exact_native_page_without_mutation);
	UT_RUN(ctrc_input_refuses_wrong_original_scope_before_io);
	UT_RUN(ctrc_input_refuses_wrong_space_or_page);
	UT_RUN(ctrc_input_rechecks_scope_at_every_read);
	UT_RUN(ctrc_input_rechecks_namespace_bytes_after_main);
	UT_RUN(ctrc_input_errors_preserve_output_and_debt);
	UT_RUN(ctrc_input_refuses_known_output_aliases);
	UT_RUN(ctrc_input_accepts_only_original_replay_serial);
	UT_RUN(ctrc_input_refuses_absent_or_foreign_handles);
	UT_RUN(ctrc_itl_input_projects_only_original_selected_entries);
	UT_RUN(ctrc_itl_input_reports_idempotence_without_writing_storage);
	UT_RUN(ctrc_itl_input_late_refusal_preserves_complete_output);
	UT_RUN(ctrc_itl_input_refuses_bounds_duplicates_and_aliases_before_io);
	UT_RUN(ctrc_itl_input_refuses_wrong_actor_before_borrowing_work);
	UT_RUN(ctrc_itl_input_rechecks_original_scope_and_selected_bytes);
	UT_RUN(ctrc_itl_input_errors_preserve_output_and_owners);
	UT_RUN(ctrc_itl_fpi_prepares_exact_canonical_frame_from_original_input);
	UT_RUN(ctrc_itl_fpi_unchanged_does_not_mint_a_version);
	UT_RUN(ctrc_itl_fpi_ignores_native_space_padding);
	UT_RUN(ctrc_itl_fpi_refusals_keep_output_and_original_debt);
	UT_RUN(ctrc_itl_fpi_rechecks_original_inputs_after_token_preparation);
	UT_RUN(ctrc_itl_fpi_refuses_output_aliases_and_bounds_before_io);
	UT_RUN(ctrc_itl_fpi_errors_keep_output_and_original_debt);
	UT_DONE();
	return ut_failed_count != 0;
}
