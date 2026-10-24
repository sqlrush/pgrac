/* PGRAC: real Generic frame/producer and stamp lifecycle, with controlled
 * writer/PAGE and native WAL boundaries. No physical recovery I/O proof.
 * Author: SqlRush <sqlrush@gmail.com> */
int recovery_stamp_fixture_main(void);
#define main recovery_stamp_fixture_main
#include "test_cluster_recovery_stamp.c"
#undef main

#include "cluster/cluster_semantic_activation.h"
#include "postmaster/interrupt.h"

static GenericXLogRecoveryImageV1 frame;
static PGAlignedBlock predecessor, changed, registered_image;
static RfPageVersionEdgeEntryV1 registered_edge;
static ClusterWalWriterToken installed_writer;
static char preflight_object;
static unsigned capacity_calls, hash_calls, promotes, revalidates, writer_checks;
static unsigned begins, blocks, edges, inserts, flushes;
static uint64 next_token;
static bool installed, recovering, promote_ok, promoted, begun;
static bool flush_error, insert_error, registering_error, mutate_input;
static bool corrupt_registered_token, invalid_insert_range;
static unsigned stale_writer_at, stale_protection_at;
static int raised_level;
pg_cryptohash_ctx *recovery_fpi_hash_create(pg_cryptohash_type type);
volatile sig_atomic_t ShutdownRequestPending;
XLogRecPtr ProcLastRecPtr;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

void
pg_re_throw(void)
{
	if (PG_exception_stack == NULL)
		abort();
	siglongjmp(*PG_exception_stack, 1);
}

bool
errstart(int level, const char *domain)
{
	(void)domain;
	if (level == DEBUG1)
		return false;
	raised_level = level;
	return true;
}

int errmsg(const char *format, ...) { (void)format; return 0; }
int errmsg_internal(const char *format, ...) { (void)format; return 0; }
void
errfinish(const char *file, int line, const char *function)
{
	(void)file; (void)line; (void)function;
	pg_re_throw();
}

ClusterNormalStartState
cluster_semantic_normal_start_state(void) { return CLUSTER_NORMAL_START_EXISTING_OTHER; }
bool cluster_semantic_activation_recovered_open_current(void) { return false; }
SCN cluster_scn_advance(void) { return ++next_token; }
bool RecoveryInProgress(void) { return recovering; }

/* The separately compiled actual Generic code uses this observation hook,
 * forwarding to the real hash implementation. Allocation after promotion
 * or inside the insertion critical section is a contract failure. */
pg_cryptohash_ctx *
recovery_fpi_hash_create(pg_cryptohash_type type)
{
	UT_ASSERT(!promoted);
	UT_ASSERT_EQ(CritSectionCount, 0);
	hash_calls++;
	return pg_cryptohash_create(type);
}

bool
ClusterStartupInstalledRecoveryWriterV1(ClusterWalWriterToken *writer,
	uint8 operation_uuid[16], uint64 *generation)
{
	UT_ASSERT(!promoted);
	UT_ASSERT_EQ(CritSectionCount, 0);
	if (!installed)
		return false;
	*writer = installed_writer;
	memset(operation_uuid, 8, 16);
	*generation = 9;
	return true;
}

ClusterControlRootResult
cluster_wal_writer_check(const ClusterWalWriterToken *writer)
{
	writer_checks++;
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT(memcmp(writer, &installed_writer, sizeof(*writer)) == 0);
	return writer_checks == stale_writer_at ? CLUSTER_CONTROL_ROOT_STALE_TOKEN
		: CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

RfPageAuthorityVerdictV1
rf_page_authority_batch_promote_nowait_v1(RfPageAuthorityPreflightV1 *preflight,
	ClusterRecoverySerialGuard *serial, RfPageAuthorityGuardV1 **out)
{
	UT_ASSERT(preflight == (RfPageAuthorityPreflightV1 *)&preflight_object);
	UT_ASSERT(serial == &fpi_serial);
	UT_ASSERT_EQ(capacity_calls, 1);
	UT_ASSERT_EQ(hash_calls, 1);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_EMPTY);
	promotes++;
	*out = NULL;
	if (!promote_ok)
		return RF_PAGE_AUTHORITY_WOULD_BLOCK;
	*out = (RfPageAuthorityGuardV1 *)&fpi_guard_object;
	promoted = true;
	if (mutate_input)
		frame.image.data[SizeOfPageHeaderData] ^= 1;
	return RF_PAGE_AUTHORITY_OK;
}

RfPageAuthorityVerdictV1
rf_page_authority_batch_revalidate_nowait_v1(const RfPageAuthorityGuardV1 *guard,
	ClusterRecoverySerialGuard *serial)
{
	UT_ASSERT(promoted && guard == (RfPageAuthorityGuardV1 *)&fpi_guard_object);
	UT_ASSERT(serial == &fpi_serial);
	UT_ASSERT_EQ(CritSectionCount, 0);
	revalidates++;
	return revalidates == stale_protection_at ? RF_PAGE_AUTHORITY_RETENTION_STALE
		: RF_PAGE_AUTHORITY_OK;
}

void
XLogEnsureRecordSpace(int max_block_id, int ndatas)
{
	UT_ASSERT_EQ(max_block_id, 0);
	UT_ASSERT_EQ(ndatas, 0);
	UT_ASSERT(!promoted && !begun);
	UT_ASSERT_EQ(CritSectionCount, 0);
	capacity_calls++;
}

static void
assert_window_pending(ClusterRecoveryStampPhaseV1 phase)
{
	ClusterRecoveryStampCaptureV1 capture;
	UT_ASSERT(promoted);
	UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, phase);
	for (unsigned i = 0; i <= CLUSTER_RECOVERY_STAMP_SLOTS; i++)
		UT_ASSERT(SpinLockFree(&region.thread_replay[i].stamp.lock));
	/* Capture/codec checks are deliberately outside insertion critical. */
	if (CritSectionCount == 0) {
		UT_ASSERT(cluster_recovery_stamp_capture_v1(&capture));
		UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&capture), CLUSTER_RECOVERY_STAMP_PENDING);
	}
}

void
XLogBeginInsert(void)
{
	UT_ASSERT_EQ(CritSectionCount, 1);
	assert_window_pending(CLUSTER_RECOVERY_STAMP_INSERTING);
	UT_ASSERT(!begun);
	begun = true;
	begins++;
}

void
XLogRegisterBlock(uint8 id, RelFileLocator *locator, ForkNumber fork,
	BlockNumber block, char *page, uint8 flags)
{
	UT_ASSERT(begun);
	UT_ASSERT_EQ(CritSectionCount, 1);
	UT_ASSERT_EQ(id, 0);
	UT_ASSERT_EQ(locator->spcOid, 1663);
	UT_ASSERT_EQ(locator->dbOid, 8);
	UT_ASSERT_EQ(locator->relNumber, 900);
	UT_ASSERT_EQ(fork, MAIN_FORKNUM);
	UT_ASSERT_EQ(block, 7);
	UT_ASSERT_EQ(flags, REGBUF_FORCE_IMAGE | REGBUF_STANDARD);
	memcpy(registered_image.data, page, BLCKSZ);
	blocks++;
	if (registering_error)
		pg_re_throw();
	if (corrupt_registered_token)
		((PageHeader)page)->pd_block_scn++;
}

void
XLogRegisterPageVersionEdge(uint64 token, const RfPageVersionEdgeEntryV1 *entry, uint8 count)
{
	UT_ASSERT(begun && blocks == 1);
	UT_ASSERT_EQ(CritSectionCount, 1);
	UT_ASSERT_EQ(token, 101);
	UT_ASSERT_EQ(count, 1);
	registered_edge = *entry;
	edges++;
}

XLogRecPtr
XLogInsert(RmgrId rmid, uint8 info)
{
	UT_ASSERT(begun && blocks == 1 && edges == 1);
	UT_ASSERT_EQ(rmid, RM_GENERIC_ID);
	UT_ASSERT_EQ(info, 0);
	UT_ASSERT_EQ(CritSectionCount, 1);
	assert_window_pending(CLUSTER_RECOVERY_STAMP_INSERTING);
	inserts++;
	if (insert_error)
		pg_re_throw();
	begun = false;
	ProcLastRecPtr = 0x2000040;
	return invalid_insert_range ? ProcLastRecPtr : 0x2000240;
}

void
XLogFlush(XLogRecPtr end)
{
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(end, 0x2000240);
	UT_ASSERT_EQ(region.thread_replay[2].stamp.record_start, 0x2000040);
	UT_ASSERT_EQ(region.thread_replay[2].stamp.record_end, end);
	assert_window_pending(CLUSTER_RECOVERY_STAMP_INSERTED);
	flushes++;
	if (flush_error)
		pg_re_throw();
}

static void
reset_insert(void)
{
	GenericXLogRecoveryImageDescriptorV1 descriptor;
	ClusterSpaceIdentity identity;
	ClusterSpaceIdentityKey key = {0};
	fresh_fpi(&descriptor);
	key.system_identifier = 99;
	key.database_incarnation = 8;
	memset(key.storage_uuid, 1, 16);
	key.locator = (RelFileLocator){1663, 8, 900};
	UT_ASSERT(cluster_space_identity_decode(descriptor.space_identity, 128, &key, &identity));
	memset(&predecessor, 0, sizeof(predecessor));
	((PageHeader)predecessor.data)->pd_lower = SizeOfPageHeaderData + 16;
	((PageHeader)predecessor.data)->pd_upper = BLCKSZ - 16;
	((PageHeader)predecessor.data)->pd_special = BLCKSZ;
	((PageHeader)predecessor.data)->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	((PageHeader)predecessor.data)->pd_block_scn = 20;
	changed = predecessor;
	changed.data[SizeOfPageHeaderData] = 42;
	next_token = 100;
	promoted = false;
	UT_ASSERT(GenericXLogRecoveryImagePrepare(GENERIC_XLOG_CTRC_ITL, &identity,
		MAIN_FORKNUM, 7, predecessor.data, changed.data, &frame));
	installed_writer = owner.writer;
	installed = promote_ok = true;
	recovering = begun = flush_error = insert_error = registering_error = mutate_input = false;
	corrupt_registered_token = invalid_insert_range = false;
	ShutdownRequestPending = 0;
	capacity_calls = hash_calls = promotes = revalidates = writer_checks = 0;
	begins = blocks = edges = inserts = flushes = 0;
	stale_writer_at = stale_protection_at = 0;
	ProcLastRecPtr = 0;
	raised_level = 0;
}

static bool
finish_image(ClusterRecoveryFpiInsertV1 *out)
{
	return GenericXLogRecoveryImageFinish(&frame, &owner,
		(RfPageAuthorityPreflightV1 *)&preflight_object, &fpi_serial,
		(ClusterWalRetentionPin *)&fpi_pin_object, out);
}

UT_TEST(fpi_native_insert_retains_exact_image_and_pending_window)
{
	ClusterRecoveryFpiInsertV1 result;
	GenericXLogRecoveryImageV1 saved;
	reset_insert();
	saved = frame;
	mutate_input = true;
	UT_ASSERT(finish_image(&result));
	UT_ASSERT(result.guard == (RfPageAuthorityGuardV1 *)&fpi_guard_object);
	UT_ASSERT(result.wal_flushed);
	UT_ASSERT_EQ(result.record_start, 0x2000040);
	UT_ASSERT_EQ(result.record_end, 0x2000240);
	UT_ASSERT(memcmp(&result.image, &saved, sizeof(saved)) == 0);
	UT_ASSERT(memcmp(registered_image.data, saved.image.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(registered_image.data[SizeOfPageHeaderData], 42);
	UT_ASSERT_EQ(registered_edge.before.mutation_token, 20);
	UT_ASSERT_EQ(registered_edge.result_incarnation[15], 6);
	UT_ASSERT_EQ(((PageHeader)registered_image.data)->pd_block_scn, 101);
	UT_ASSERT_EQ(((PageHeader)predecessor.data)->pd_block_scn, 20);
	UT_ASSERT_EQ(changed.data[SizeOfPageHeaderData], 42);
	UT_ASSERT_EQ(flushes, 1);
	assert_window_pending(CLUSTER_RECOVERY_STAMP_INSERTED);
	UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&result.window));
}

UT_TEST(fpi_native_insert_refuses_uninstalled_wrong_actor_or_frame_without_promotion)
{
	for (unsigned fault = 0; fault < 9; fault++) {
		ClusterRecoveryFpiInsertV1 result;
		reset_insert();
		if (fault == 0) installed = false;
		if (fault == 1) installed_writer.epoch++;
		if (fault == 2) MyBackendType = B_BACKEND;
		if (fault == 3) fpi_serial.mode = CLUSTER_RECOVERY_SERIAL_INPUT_SEAL;
		if (fault == 4) frame.owner = GENERIC_XLOG_ITL_FINISH;
		if (fault == 5) recovering = true;
		if (fault == 6) ShutdownRequestPending = 1;
		if (fault == 7) CritSectionCount = 1;
		if (fault == 8) owner.executor_pid++;
		UT_ASSERT(!finish_image(&result));
		UT_ASSERT(result.guard == NULL && result.window.key.window_seq == 0);
		UT_ASSERT(!result.wal_flushed && result.record_end == 0);
		UT_ASSERT_EQ(promotes + inserts + flushes, 0);
	}
}

UT_TEST(fpi_native_insert_preinsert_refusal_cancels_only_unwritten_window)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterRecoveryFpiInsertV1 result;
		reset_insert();
		if (fault == 0) promote_ok = false;
		if (fault == 1) fpi_protection_current = false;
		if (fault == 2) stale_writer_at = 1;
		if (fault == 3) stale_protection_at = 1;
		UT_ASSERT(!finish_image(&result));
		UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_EMPTY);
		UT_ASSERT_EQ(result.window.key.window_seq, 0);
		UT_ASSERT_EQ(begins + inserts + flushes, 0);
		UT_ASSERT_EQ(result.guard != NULL, fault != 0);
	}
}

UT_TEST(fpi_native_insert_refuses_aliases_without_erasing_inputs)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		Size size = sizeof(ClusterRecoveryFpiInsertV1) + MAXALIGN(1);
		char *storage = calloc(1, size), *saved = malloc(size);
		ClusterRecoveryFpiInsertV1 *result = (ClusterRecoveryFpiInsertV1 *)
			(storage + (fault & 1 ? MAXALIGN(1) : 0));
		char *overlap = storage + (fault & 1 ? 0 : MAXALIGN(1));
		const GenericXLogRecoveryImageV1 *input = &frame;
		const ClusterRecoveryStampOwnerV1 *input_owner = &owner;
		ClusterRecoverySerialGuard *input_serial = &fpi_serial;
		reset_insert();
		if (fault / 2 == 0) {
			memcpy(overlap, &frame, sizeof(frame));
			input = (GenericXLogRecoveryImageV1 *)overlap;
		} else if (fault / 2 == 1) {
			memcpy(overlap, &owner, sizeof(owner));
			input_owner = (ClusterRecoveryStampOwnerV1 *)overlap;
		} else {
			memcpy(overlap, &fpi_serial, sizeof(fpi_serial));
			input_serial = (ClusterRecoverySerialGuard *)overlap;
		}
		memcpy(saved, storage, size);
		UT_ASSERT(!GenericXLogRecoveryImageFinish(input, input_owner,
			(RfPageAuthorityPreflightV1 *)&preflight_object, input_serial,
			(ClusterWalRetentionPin *)&fpi_pin_object, result));
		UT_ASSERT(memcmp(saved, storage, size) == 0);
		UT_ASSERT_EQ(capacity_calls + hash_calls + promotes + inserts, 0);
		free(saved);
		free(storage);
	}
}

UT_TEST(fpi_native_insert_postflush_drift_preserves_failed_debt)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		ClusterRecoveryFpiInsertV1 result;
		ClusterRecoveryStampCaptureV1 capture;
		reset_insert();
		if (fault == 0) stale_writer_at = 2;
		if (fault == 1) stale_protection_at = 2;
		UT_ASSERT(!finish_image(&result));
		UT_ASSERT(result.guard != NULL && !result.wal_flushed);
		UT_ASSERT_EQ(result.record_end, 0x2000240);
		UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_INSERTED);
		UT_ASSERT(region.thread_replay[2].stamp.failed);
		UT_ASSERT(cluster_recovery_stamp_capture_v1(&capture));
		UT_ASSERT_EQ(capture.count, 1);
		UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&capture), CLUSTER_RECOVERY_STAMP_INVALID);
		UT_ASSERT(!cluster_recovery_stamp_durable_v1(&result.window));
		UT_ASSERT(!cluster_recovery_stamp_clear_v1(&result.window));
	}
}

UT_TEST(fpi_native_insert_exceptions_keep_guard_and_exact_uncertain_phase)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		ClusterRecoveryFpiInsertV1 *result = calloc(1, sizeof(*result));
		volatile bool caught = false;
		reset_insert();
		flush_error = fault == 0;
		insert_error = fault == 1;
		registering_error = fault == 2;
		corrupt_registered_token = fault == 3;
		invalid_insert_range = fault == 4;
		PG_TRY();
		{
			(void)finish_image(result);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT(result->guard != NULL && !result->wal_flushed);
		UT_ASSERT(region.thread_replay[2].stamp.failed);
		UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, fault == 0
			? CLUSTER_RECOVERY_STAMP_INSERTED : CLUSTER_RECOVERY_STAMP_INSERTING);
		UT_ASSERT_EQ(CritSectionCount, fault == 0 ? 0 : 1);
		if (fault >= 3)
			UT_ASSERT_EQ(raised_level, PANIC);
		CritSectionCount = 0; /* Fixture unwinding only; real critical errors PANIC. */
		UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&result->window));
		UT_ASSERT(!cluster_recovery_stamp_durable_v1(&result->window));
		free(result);
	}
}

#ifndef RECOVERY_FPI_INSERT_MAIN
#define RECOVERY_FPI_INSERT_MAIN main
#endif
int
RECOVERY_FPI_INSERT_MAIN(void)
{
	UT_PLAN(6);
	UT_RUN(fpi_native_insert_retains_exact_image_and_pending_window);
	UT_RUN(fpi_native_insert_refuses_uninstalled_wrong_actor_or_frame_without_promotion);
	UT_RUN(fpi_native_insert_preinsert_refusal_cancels_only_unwritten_window);
	UT_RUN(fpi_native_insert_refuses_aliases_without_erasing_inputs);
	UT_RUN(fpi_native_insert_postflush_drift_preserves_failed_debt);
	UT_RUN(fpi_native_insert_exceptions_keep_guard_and_exact_uncertain_phase);
	UT_DONE();
	return ut_failed_count != 0;
}
