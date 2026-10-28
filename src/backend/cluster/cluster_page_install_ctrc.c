/*-------------------------------------------------------------------------
 *
 * cluster_page_install_ctrc.c
 *     Protected storage input under original native CTRC replay owners.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/backend/cluster/cluster_page_install_ctrc.c
 *
 * NOTES
 *     Reuse native SMGR/SPACE/checksum validation without RR impersonation.
 *     All original grants and their cleanup remain owned by the caller.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_page_install_ctrc.h"
#include "cluster/cluster_page_buffer_ctrc.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogrecovery.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_recovery_plan.h"
#include "cluster/cluster_space_reservation.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "cluster/cluster_wal_inputs.h"
#include "cluster/storage/cluster_shared_fs_page.h"
#include "utils/memutils.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "storage/checksum.h"
#include "storage/lwlock.h"
#include "storage/smgr.h"

typedef struct CtrcNativeStepImagesV1 {
	PGAlignedBlock page, space0, space1;
	bool ready;
} CtrcNativeStepImagesV1;

typedef struct CtrcNativeStepV1 {
	const struct ClusterRecoveryStartV1 *start;
	const ClusterColdCtrcWorkV1 *work, *snapshot;
	const ClusterRecoveryUndoEntry *original_physical, *physical;
	const ClusterRecoveryCtrcReplaySetSourceV1 *original_source;
	ClusterRecoveryCtrcReplaySetSourceV1 source;
	const ClusterSpaceIdentity *original_space;
	ClusterSpaceIdentity space;
	uint8 space_identity[CLUSTER_SPACE_IDENTITY_BYTES];
	const RfPageVersionV1 *original_before;
	RfPageVersionV1 before;
	ClusterRecoveryTxResultObservationV1 accepted;
	Size work_bytes, physical_bytes;
	CtrcNativeStepImagesV1 images;
	const char *original_canonical;
	PGAlignedBlock canonical;
	ClusterSharedFsPageV1 *storage;
	bool publish_durable;
	bool sync_unchanged;
	bool cache_retired;
	const RfPageCtrcAuthorityV1 *cache_authority;
	BlockNumber cache_block;
} CtrcNativeStepV1;

typedef struct CtrcNativeReadV1 {
	const struct ClusterRecoveryStartV1 *start;
	const ClusterColdCtrcWorkV1 *work;
	const ClusterColdTypedV1 *typed;
	const RfPageCtrcAuthorityV1 *authority;
	struct ClusterWalInputsV1 *scope;
	const ClusterSpaceIdentity *original_space;
	const RfPageVersionV1 *original_before;
	uint64 work_generation;
	const ClusterColdPlanV1 *plan;
	const struct ClusterColdTopTxCensusV1 *top_tx_census;
	ClusterColdCtrcReleaseCensusV1 counts;
	ClusterRecoveryCheckpointOwnerV1 owner;
	ClusterSpaceIdentityKey key;
	uint8 space_identity[CLUSTER_SPACE_IDENTITY_BYTES];
	RfPageIdentityV1 identity;
	RfPageVersionV1 before;
	bool checksums;
	const CtrcNativeStepV1 *step;
	bool (*step_current)(const CtrcNativeStepV1 *step);
	CtrcNativeStepImagesV1 *capture;
} CtrcNativeReadV1;

static bool
ctrc_native_read_actor(void)
{
	return cluster_enabled && cluster_shared_config && IsUnderPostmaster
		&& MyBackendType == B_STARTUP && MyProcPid > 0 && CritSectionCount == 0
		&& !ShutdownRequestPending && !RecoveryInProgress();
}

static bool
ctrc_native_step_current(const CtrcNativeStepV1 *step)
{
	ClusterRecoveryTxResultObservationV1 accepted;
	uint8 encoded[CLUSTER_SPACE_IDENTITY_BYTES];

	if (step == NULL) return true;
	return ctrc_native_read_actor()
		&& (step->storage == NULL || cluster_shared_fs_page_current_v1(step->storage))
		&& ClusterColdRecoveryCtrcWorkCurrent(step->start, step->work)
		&& memcmp(step->work, step->snapshot, step->work_bytes) == 0
		&& (step->physical_bytes == 0
			|| memcmp(step->original_physical, step->physical, step->physical_bytes) == 0)
		&& memcmp(step->original_source, &step->source, sizeof(step->source)) == 0
		&& cluster_space_identity_encode(step->original_space, encoded, sizeof(encoded))
		&& memcmp(encoded, step->space_identity, sizeof(encoded)) == 0
		&& memcmp(step->original_before, &step->before, sizeof(step->before)) == 0
		&& (step->original_canonical == NULL
			|| memcmp(step->original_canonical, step->canonical.data, BLCKSZ) == 0)
		&& ClusterColdRecoveryCtrcScope(step->start, step->work) == step->source.scope
		&& cluster_recovery_ctrc_terminal_result_v1(step->start, step->work, &accepted)
		&& memcmp(&accepted, &step->accepted, sizeof(accepted)) == 0
		&& (!step->cache_retired || cluster_bufmgr_ctrc_page_cache_v1(step->start,
			step->work, step->cache_authority, step->original_space, step->cache_block,
			step->original_before, NULL) == CLUSTER_CTRC_PAGE_CACHE_ABSENT);
}

static bool
ctrc_native_read_step_current(const CtrcNativeReadV1 *context)
{
	/* Ordinary reader callers do not depend on the terminal projection leg. */
	return context->step == NULL
		|| (context->step_current != NULL && context->step_current(context->step));
}

static bool
ctrc_native_read_current(const CtrcNativeReadV1 *context)
{
	ClusterRecoveryCheckpointOwnerV1 owner;
	const ClusterColdTypedV1 *typed;
	uint8 encoded[CLUSTER_SPACE_IDENTITY_BYTES];

	if (!ctrc_native_read_actor() || !ctrc_native_read_step_current(context)
		|| !ClusterColdRecoveryCtrcWorkCurrent(context->start, context->work)
		|| context->work->generation != context->work_generation
		|| context->work->plan != context->plan
		|| context->work->top_tx_census != context->top_tx_census
		|| memcmp(&context->work->counts, &context->counts, sizeof(context->counts)) != 0)
		return false;
	typed = ClusterColdRecoveryTxInputs(context->start);
	return typed != NULL && typed == context->typed && typed->ctrc_work == context->work
		&& typed->ctrc_work_generation == context->work_generation
		&& typed->participant_count <= CLUSTER_COLD_MAX_PARTICIPANTS
		&& typed->replay_count != 0 && typed->replay_count <= typed->participant_count
		&& ClusterColdRecoveryTxCheckpointOwnerV1(context->start, &owner)
		&& memcmp(&owner, &context->owner, sizeof(owner)) == 0
		&& ClusterColdRecoveryCtrcScope(context->start, context->work) == context->scope
		&& cluster_space_identity_encode(context->original_space, encoded, sizeof(encoded))
		&& memcmp(encoded, context->space_identity, sizeof(encoded)) == 0
		&& memcmp(context->original_before, &context->before, sizeof(context->before)) == 0
		&& DataChecksumsEnabled() == context->checksums
		&& rf_page_authority_ctrc_replays_scope_nowait_v1(context->authority, context->scope)
		&& rf_page_authority_ctrc_replays_covers_nowait_v1(context->authority,
			&context->identity, &context->before)
		&& ctrc_native_read_step_current(context);
}

static bool
ctrc_native_read_storage(SMgrRelation relation, BlockNumber blockno)
{
	return smgrexists(relation, SPACE_FORKNUM) && smgrnblocks(relation, SPACE_FORKNUM) == 2
		&& smgrexists(relation, MAIN_FORKNUM) && blockno < smgrnblocks(relation, MAIN_FORKNUM);
}

static bool
ctrc_native_read_target_current(const CtrcNativeReadV1 *context, SMgrRelation relation)
{
	return context->step != NULL
		? context->step->storage != NULL && cluster_shared_fs_page_current_v1(context->step->storage)
		: ctrc_native_read_storage(relation, context->identity.blockno);
}

static bool
ctrc_native_read_page(const CtrcNativeReadV1 *context, SMgrRelation relation,
	ForkNumber forknum, BlockNumber blockno, PGAlignedBlock *out)
{
	if (!ctrc_native_read_current(context)
		|| !ctrc_native_read_target_current(context, relation)
		|| !ctrc_native_read_current(context))
		return false;
	if (context->step != NULL) {
		if (!cluster_shared_fs_page_read_v1(context->step->storage, forknum, blockno, out->data))
			return false;
	} else
		smgrread(relation, forknum, blockno, out->data);
	return ctrc_native_read_current(context)
		&& ctrc_native_read_target_current(context, relation)
		&& ctrc_native_read_current(context);
}

static bool
ctrc_native_read_integrity(const CtrcNativeReadV1 *context,
	PGAlignedBlock *image, BlockNumber blockno)
{
	Page page = image->data;
	PageHeader header = (PageHeader)page;

	/* pg_checksum_page requires a non-new page even in cassert builds. */
	if (PageIsNew(page) || PageGetPageSize(page) != BLCKSZ
		|| PageGetPageLayoutVersion(page) != PG_PAGE_LAYOUT_VERSION
		|| header->pd_lower < SizeOfPageHeaderData || header->pd_lower > header->pd_upper
		|| header->pd_upper > header->pd_special || header->pd_special > BLCKSZ)
		return false;
	return context->checksums ? header->pd_checksum == pg_checksum_page(page, blockno)
		: header->pd_checksum == 0;
}

static bool
ctrc_native_read_bind(CtrcNativeReadV1 *context,
	const struct ClusterRecoveryStartV1 *start, const ClusterColdCtrcWorkV1 *work,
	const RfPageCtrcAuthorityV1 *authority, const ClusterSpaceIdentity *space,
	BlockNumber blockno, const RfPageVersionV1 *expected_before)
{
	if (start == NULL || work == NULL || authority == NULL || space == NULL
		|| expected_before == NULL || !ctrc_native_read_actor()
		|| !ClusterColdRecoveryCtrcWorkCurrent(start, work) || work->generation == 0)
		return false;
	context->typed = ClusterColdRecoveryTxInputs(start);
	if (context->typed == NULL || context->typed->ctrc_work != work
		|| context->typed->ctrc_work_generation != work->generation
		|| context->typed->participant_count > CLUSTER_COLD_MAX_PARTICIPANTS
		|| context->typed->replay_count == 0
		|| context->typed->replay_count > context->typed->participant_count
		|| !ClusterColdRecoveryTxCheckpointOwnerV1(start, &context->owner)
		|| context->owner.owner_pid != MyProcPid || context->owner.owner_boot == 0
		|| context->owner.install_generation == 0 || context->owner.start_generation == 0
		|| context->owner.start_serial == 0
		|| space->state != CLUSTER_SPACE_IDENTITY_LIVE
		|| !cluster_space_identity_encode(space, context->space_identity, sizeof(context->space_identity))
		|| !rf_page_version_present_v1(expected_before)
		|| memcmp(space->incarnation, expected_before->segment_incarnation, 16) != 0
		|| space->key.system_identifier != context->owner.writer.ref.claim.identity.system_identifier
		|| space->key.database_incarnation != context->owner.writer.ref.claim.database_incarnation
		|| memcmp(space->key.storage_uuid, context->owner.writer.ref.claim.identity.storage_uuid, 16) != 0)
		return false;
	context->start = start; context->work = work; context->authority = authority;
	context->work_generation = work->generation;
	context->plan = work->plan; context->top_tx_census = work->top_tx_census;
	context->counts = work->counts;
	context->scope = ClusterColdRecoveryCtrcScope(start, work);
	context->original_space = space; context->original_before = expected_before;
	context->key = space->key; context->before = *expected_before;
	context->identity.system_identifier = space->key.system_identifier;
	memcpy(context->identity.storage_uuid, space->key.storage_uuid, 16);
	context->identity.locator = space->key.locator; context->identity.forknum = MAIN_FORKNUM;
	context->identity.blockno = blockno; context->checksums = DataChecksumsEnabled();
	if (context->scope == NULL || !rf_page_identity_valid_v1(&context->identity)
		|| !ctrc_native_read_current(context))
		return false;
	return true;
}

/* Reuse this original binding, including across allocation/token callbacks. */
static bool
ctrc_native_read_image(const CtrcNativeReadV1 *context, PGAlignedBlock *out)
{
	ClusterSpaceIdentity identity;
	ClusterSpaceReservation reservation;
	uint8 encoded[CLUSTER_SPACE_IDENTITY_BYTES];
	PGAlignedBlock space0, space1, page, verify;
	SMgrRelation relation;
	uint64 token;
	BlockNumber blockno = context->identity.blockno;

	relation = context->step == NULL ? smgropen(context->identity.locator, InvalidBackendId) : NULL;
	if (!ctrc_native_read_page(context, relation, SPACE_FORKNUM, 0, &space0)
		|| !ctrc_native_read_integrity(context, &space0, 0)
		|| !cluster_space_identity_page_decode(space0.data, BLCKSZ, SPACE_FORKNUM, 0,
			&context->key, &identity, &token)
		|| !cluster_space_identity_encode(&identity, encoded, sizeof(encoded))
		|| memcmp(encoded, context->space_identity, sizeof(encoded)) != 0
		|| !ctrc_native_read_page(context, relation, SPACE_FORKNUM, 1, &space1)
		|| !ctrc_native_read_integrity(context, &space1, 1)
		|| !cluster_space_reservation_page_decode(space1.data, BLCKSZ, SPACE_FORKNUM, 1,
			&context->key, &reservation, &token)
		|| reservation.next_block <= blockno
		|| !cluster_space_identity_encode(&reservation.identity, encoded, sizeof(encoded))
		|| memcmp(encoded, context->space_identity, sizeof(encoded)) != 0
		|| !ctrc_native_read_page(context, relation, MAIN_FORKNUM, blockno, &page)
		|| !ctrc_native_read_integrity(context, &page, blockno)
		|| ((PageHeader)page.data)->pd_block_scn != context->before.mutation_token
		|| !ctrc_native_read_page(context, relation, SPACE_FORKNUM, 0, &verify)
		|| memcmp(verify.data, space0.data, BLCKSZ) != 0
		|| !ctrc_native_read_page(context, relation, SPACE_FORKNUM, 1, &verify)
		|| memcmp(verify.data, space1.data, BLCKSZ) != 0
		|| !ctrc_native_read_current(context))
		return false;
	memcpy(out, &page, sizeof(page));
	if (context->capture != NULL) {
		context->capture->page = page;
		context->capture->space0 = space0;
		context->capture->space1 = space1;
		context->capture->ready = true;
	}
	return true;
}

RfPageCtrcReadV1
rf_page_recovery_ctrc_replays_read_smgr_v1(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, const RfPageCtrcAuthorityV1 *authority,
	const ClusterSpaceIdentity *space, BlockNumber blockno, const RfPageVersionV1 *expected_before)
{
	RfPageCtrcReadV1 result = {0};
	CtrcNativeReadV1 context = {0};

	if (ctrc_native_read_bind(&context, start, work, authority, space, blockno, expected_before)
		&& ctrc_native_read_image(&context, &result.image))
		result.ready = true;
	return result;
}

typedef struct CtrcNativeInsertV1 {
	CtrcNativeReadV1 read;
	const struct ClusterRecoveryStartV1 *start;
	const ClusterColdCtrcWorkV1 *work;
	const ClusterColdCtrcWorkV1 *snapshot;
	Size work_bytes;
	const ClusterRecoveryCtrcReplaySetSourceV1 *original_source;
	ClusterRecoveryCtrcReplaySetSourceV1 source;
	const GenericXLogRecoveryImageV1 *original_image;
	ClusterRecoveryTxResultObservationV1 accepted;
	GenericXLogRecoveryImageDescriptorV1 descriptor;
	RfPageAuthorityVerdictV1 promotion;
} CtrcNativeInsertV1;

static void
ctrc_native_insert_count_lock(LWLock *lock, LWLockMode mode, void *context)
{
	(void)lock; (void)mode;
	*(uint32 *)context += 1;
}

static bool
ctrc_native_insert_inputs_current(const CtrcNativeInsertV1 *context,
	const RfPageCtrcFpiInsertV1 *result)
{
	ClusterRecoveryTxResultObservationV1 accepted;

	return ctrc_native_read_actor() && ctrc_native_step_current(context->read.step)
		&& ClusterColdRecoveryCtrcWorkCurrent(context->start, context->work)
		&& memcmp(context->work, context->snapshot, context->work_bytes) == 0
		&& memcmp(context->original_image, &result->image, sizeof(result->image)) == 0
		&& memcmp(context->original_source, &context->source, sizeof(context->source)) == 0
		&& cluster_recovery_ctrc_terminal_result_v1(context->start, context->work, &accepted)
		&& memcmp(&accepted, &context->accepted, sizeof(accepted)) == 0
		&& ClusterColdRecoveryCtrcScope(context->start, context->work) == context->source.scope
		&& ctrc_native_step_current(context->read.step);
}

static bool
ctrc_native_insert_gap_current(CtrcNativeReadV1 *read)
{
	CtrcNativeStepImagesV1 observed = {0};
	const CtrcNativeStepImagesV1 *original;
	bool ready;

	if (read->step == NULL) return true;
	original = &read->step->images;
	if (!original->ready || !ctrc_native_step_current(read->step)) return false;
	read->capture = &observed;
	ready = ctrc_native_read_image(read, &observed.page);
	read->capture = NULL;
	return ready && observed.ready
		&& memcmp(&observed.page, &original->page, sizeof(observed.page)) == 0
		&& memcmp(&observed.space0, &original->space0, sizeof(observed.space0)) == 0
		&& memcmp(&observed.space1, &original->space1, sizeof(observed.space1)) == 0
		&& ctrc_native_step_current(read->step);
}

/* No hashing, allocation, ROOT/WAL I/O or ownership acquisition. Original
 * short shared snapshots still validate the same installed writer/holders.
 * A step also rechecks its already-held target namespace outside critical
 * sections; this never reopens a file or acquires another resource. */
static bool
ctrc_native_insert_current(const CtrcNativeInsertV1 *context,
	const RfPageCtrcFpiInsertV1 *result)
{
	ClusterWalWriterToken writer;
	uint8 operation_uuid[16];
	uint64 generation;
	ClusterRecoveryStampFpiV1 envelope;
	RfPageCtrcReferenceV1 reference;
	const ClusterRecoveryCheckpointOwnerV1 *owner = &context->accepted.checkpoint.owner;

	if (!ctrc_native_insert_inputs_current(context, result)
		|| !ClusterStartupInstalledRecoveryWriterV1(&writer, operation_uuid, &generation)
		|| memcmp(&writer, &owner->writer, sizeof(writer)) != 0
		|| memcmp(operation_uuid, owner->operation_uuid, sizeof(operation_uuid)) != 0
		|| generation != owner->install_generation
		|| !ctrc_native_read_current(&context->read)
		|| !cluster_wal_inputs_ctrc_writer_retained_v1(&context->source, &owner->writer)
		|| result->window.kind != CLUSTER_RECOVERY_STAMP_FPI
		|| result->window.payload_length != sizeof(envelope))
		return false;
	memcpy(&envelope, result->window.payload, sizeof(envelope));
	reference = rf_page_authority_ctrc_replays_reference_nowait_v1(context->read.authority,
		&context->source, &context->read.identity, &context->read.before);
	return reference.ready
		&& memcmp(&envelope.image, &context->descriptor, sizeof(envelope.image)) == 0
		&& memcmp(&envelope.protection, &reference.reference, sizeof(envelope.protection)) == 0
		&& memcmp(&result->window.owner.writer, &owner->writer, sizeof(owner->writer)) == 0
		&& memcmp(&result->window.owner.target, &context->source.selected.source.claim,
			sizeof(result->window.owner.target)) == 0
		&& result->window.owner.executor_pid == owner->owner_pid
		&& result->window.owner.executor_boot == owner->owner_boot
		&& result->window.owner.recovery_serial == owner->start_serial
		&& ctrc_native_insert_inputs_current(context, result);
}

static void
ctrc_native_insert_wal(CtrcNativeInsertV1 *context, RfPageCtrcAuthorityV1 *authority,
	Size subject_count, Size stamp_budget, RfPageCtrcFpiInsertV1 *result)
{
	if (!ctrc_native_insert_inputs_current(context, result)
		|| !GenericXLogRecoveryImageDescribe(&result->image, &context->descriptor)
		|| !ctrc_native_insert_inputs_current(context, result))
		return;
	XLogEnsureRecordSpace(0, 0);
	if (!ctrc_native_insert_inputs_current(context, result)) return;
	context->read.identity.system_identifier = result->image.identity.key.system_identifier;
	memcpy(context->read.identity.storage_uuid, result->image.identity.key.storage_uuid, 16);
	context->read.identity.locator = result->image.identity.key.locator;
	context->read.identity.forknum = result->image.fork_number;
	context->read.identity.blockno = result->image.block_number;
	context->promotion = rf_page_authority_ctrc_replays_promote_fpi_nowait_v1(authority,
		&context->read.identity, &result->image.version.before);
	if (context->promotion != RF_PAGE_AUTHORITY_OK
		|| !ctrc_native_insert_inputs_current(context, result)
		|| !ctrc_native_read_bind(&context->read, context->start, context->work, authority,
			&context->original_image->identity, result->image.block_number, &context->original_image->version.before)
		|| memcmp(&context->read.owner, &context->accepted.checkpoint.owner, sizeof(context->read.owner)) != 0
		|| !ctrc_native_insert_gap_current(&context->read))
		return;
	result->window = cluster_recovery_stamp_prepare_ctrc_fpi_v1(context->start, context->work,
		context->original_source, authority, &context->read.identity, &context->descriptor,
		subject_count, stamp_budget);
	if (result->window.key.window_seq == 0) return;
	if (cluster_wal_writer_check(&result->window.owner.writer) != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| !ctrc_native_insert_current(context, result)) {
		if (cluster_recovery_stamp_cancel_v1(&result->window))
			memset(&result->window, 0, sizeof(result->window));
		else
			(void)cluster_recovery_stamp_fail_v1(&result->window);
		return;
	}
	START_CRIT_SECTION();
	if (!cluster_recovery_stamp_inserting_v1(&result->window))
		ereport(PANIC, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			errmsg("could not begin native recovery image insertion"),
			errhint("Retain the recovery window for the next recovery owner.")));
	XLogBeginInsert();
	if (!GenericXLogRecoveryImageRegister(&result->image))
		ereport(PANIC, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			errmsg("could not register native recovery image"),
			errhint("Retain the recovery window for the next recovery owner.")));
	result->record_end = XLogInsert(RM_GENERIC_ID, 0);
	result->record_start = ProcLastRecPtr;
	if (!cluster_recovery_stamp_inserted_v1(&result->window, result->record_start, result->record_end))
		ereport(PANIC, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			errmsg("could not record native recovery image insertion"),
			errhint("Retain the recovery window for the next recovery owner.")));
	END_CRIT_SECTION();
	XLogFlush(result->record_end);
	if (cluster_wal_writer_check(&result->window.owner.writer) == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& ctrc_native_insert_current(context, result)
		&& cluster_recovery_stamp_fpi_inserted_current_v1(&result->window, result->record_start, result->record_end))
		result->wal_flushed = true;
	else
		(void)cluster_recovery_stamp_fail_v1(&result->window);
}

/* The output and binding belong to the calling frame, outside this setjmp.
 * On ERROR the exact published window remains defined for failure marking. */
static void
ctrc_native_insert_attempt(CtrcNativeInsertV1 *context, const ClusterColdCtrcWorkV1 *header,
	RfPageCtrcAuthorityV1 *authority, Size subject_count, Size memory_budget,
	RfPageCtrcFpiInsertV1 *result)
{
	void *volatile snapshot = NULL;

	PG_TRY();
	{
		snapshot = MemoryContextAllocZero(CurrentMemoryContext, context->work_bytes);
		if (ClusterColdRecoveryCtrcWorkCurrent(context->start, context->work)
			&& memcmp(context->work, header, offsetof(ClusterColdCtrcWorkV1, entries)) == 0) {
			memcpy((void *)snapshot, context->work, context->work_bytes);
			context->snapshot = (const ClusterColdCtrcWorkV1 *)snapshot;
			ctrc_native_insert_wal(context, authority, subject_count,
				memory_budget - context->work_bytes, result);
		}
	}
	PG_CATCH();
	{
		if (result->window.key.window_seq != 0) (void)cluster_recovery_stamp_fail_v1(&result->window);
		if (snapshot != NULL) pfree((void *)snapshot);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (snapshot != NULL) pfree((void *)snapshot);
}

static RfPageCtrcFpiInsertV1
ctrc_native_fpi_insert(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, const ClusterRecoveryCtrcReplaySetSourceV1 *source,
	RfPageCtrcAuthorityV1 *authority, const GenericXLogRecoveryImageV1 *image,
	Size subject_count, Size memory_budget, const CtrcNativeStepV1 *step,
	RfPageAuthorityVerdictV1 *promotion)
{
	RfPageCtrcFpiInsertV1 result = {0};
	CtrcNativeInsertV1 context = {0};
	ClusterColdCtrcWorkV1 header;
	uint32 held = 0;
	Size bytes;

	if (!ctrc_native_read_actor() || start == NULL || work == NULL || source == NULL
		|| authority == NULL || image == NULL)
		return result;
	/* Reject a full caller-held list before any original owner snapshot. */
	ForEachLWLockHeldByMe(ctrc_native_insert_count_lock, &held);
	if (held != 0 || !ClusterColdRecoveryCtrcWorkCurrent(start, work)
		|| subject_count != work->counts.subject_count
		|| subject_count > (MaxAllocSize - offsetof(ClusterColdCtrcWorkV1, entries)) / sizeof(work->entries[0])
		|| source->scope == NULL || ClusterColdRecoveryCtrcScope(start, work) != source->scope)
		return result;
	bytes = offsetof(ClusterColdCtrcWorkV1, entries) + subject_count * sizeof(work->entries[0]);
	/* The unchanged F4 producer temporarily keeps its own complete snapshot. */
	if (bytes > memory_budget / 2) return result;
	memcpy(&header, work, offsetof(ClusterColdCtrcWorkV1, entries));
	context.start = start; context.work = work; context.work_bytes = bytes;
	context.original_source = source; context.source = *source;
	context.original_image = image; result.image = *image;
	context.read.step = step; context.promotion = RF_PAGE_AUTHORITY_INVALID_ARGUMENT;
	context.read.step_current = step == NULL ? NULL : ctrc_native_step_current;
	if (!cluster_recovery_ctrc_terminal_result_v1(start, work, &context.accepted)) return result;
	ctrc_native_insert_attempt(&context, &header, authority, subject_count, memory_budget, &result);
	if (promotion != NULL) *promotion = context.promotion;
	return result;
}

RfPageCtrcFpiInsertV1
rf_page_recovery_ctrc_replays_fpi_insert_v1(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, const ClusterRecoveryCtrcReplaySetSourceV1 *source,
	RfPageCtrcAuthorityV1 *authority, const GenericXLogRecoveryImageV1 *image,
	Size subject_count, Size memory_budget)
{
	return ctrc_native_fpi_insert(start, work, source, authority, image,
		subject_count, memory_budget, NULL, NULL);
}

typedef struct CtrcNativeInstallV1 {
	CtrcNativeInsertV1 binding;
	const RfPageCtrcFpiInsertV1 *original;
	RfPageCtrcFpiInsertV1 fixed;
	PGAlignedBlock space0, space1;
	bool publish_durable;
} CtrcNativeInstallV1;

static bool
ctrc_native_install_current(const CtrcNativeInstallV1 *context)
{
	const RfPageCtrcFpiInsertV1 *fixed = &context->fixed;
	TimeLineID timeline;

	if (memcmp(context->original, fixed, sizeof(*fixed)) != 0 || !fixed->wal_flushed
		|| !ctrc_native_insert_current(&context->binding, fixed)
		|| !cluster_recovery_stamp_fpi_inserted_current_v1(&fixed->window,
			fixed->record_start, fixed->record_end)
		|| cluster_wal_writer_check(&fixed->window.owner.writer) != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| GetFlushRecPtr(&timeline) < fixed->record_end
		|| timeline != fixed->window.owner.writer.ref.timeline)
		return false;
	return memcmp(context->original, fixed, sizeof(*fixed)) == 0
		&& ctrc_native_insert_current(&context->binding, fixed);
}

static bool
ctrc_native_install_read_page(const CtrcNativeInstallV1 *context, SMgrRelation relation,
	ForkNumber fork, BlockNumber block, PGAlignedBlock *out)
{
	return ctrc_native_install_current(context)
		&& ctrc_native_read_page(&context->binding.read, relation, fork, block, out)
		&& ctrc_native_install_current(context);
}

/* SPACE is verified on both sides of each target observation, and across
 * installation. Result-version acceptance never widens the PAGE authority. */
static bool
ctrc_native_install_observe(CtrcNativeInstallV1 *context, SMgrRelation relation,
	bool first, PGAlignedBlock *out)
{
	const CtrcNativeReadV1 *read = &context->binding.read;
	ClusterSpaceIdentity identity;
	ClusterSpaceReservation reservation;
	uint8 encoded[CLUSTER_SPACE_IDENTITY_BYTES];
	PGAlignedBlock space0, space1, page, verify;
	BlockNumber block = read->identity.blockno;
	uint64 token;

	if (!ctrc_native_install_read_page(context, relation, SPACE_FORKNUM, 0, &space0)
		|| !ctrc_native_read_integrity(read, &space0, 0)
		|| !cluster_space_identity_page_decode(space0.data, BLCKSZ, SPACE_FORKNUM, 0,
			&read->key, &identity, &token)
		|| !cluster_space_identity_encode(&identity, encoded, sizeof(encoded))
		|| memcmp(encoded, read->space_identity, sizeof(encoded)) != 0
		|| !ctrc_native_install_read_page(context, relation, SPACE_FORKNUM, 1, &space1)
		|| !ctrc_native_read_integrity(read, &space1, 1)
		|| !cluster_space_reservation_page_decode(space1.data, BLCKSZ, SPACE_FORKNUM, 1,
			&read->key, &reservation, &token)
		|| reservation.next_block <= block
		|| !cluster_space_identity_encode(&reservation.identity, encoded, sizeof(encoded))
		|| memcmp(encoded, read->space_identity, sizeof(encoded)) != 0
		|| (!first && (memcmp(&space0, &context->space0, sizeof(space0)) != 0
			|| memcmp(&space1, &context->space1, sizeof(space1)) != 0))
		|| !ctrc_native_install_read_page(context, relation, MAIN_FORKNUM, block, &page)
		|| !ctrc_native_read_integrity(read, &page, block)
		|| !ctrc_native_install_read_page(context, relation, SPACE_FORKNUM, 0, &verify)
		|| memcmp(&verify, &space0, sizeof(verify)) != 0
		|| !ctrc_native_install_read_page(context, relation, SPACE_FORKNUM, 1, &verify)
		|| memcmp(&verify, &space1, sizeof(verify)) != 0
		|| !ctrc_native_install_current(context))
		return false;
	if (first) { context->space0 = space0; context->space1 = space1; }
	*out = page;
	return true;
}

static bool
ctrc_native_install_storage_current(const CtrcNativeInstallV1 *context, SMgrRelation relation)
{
	return ctrc_native_install_current(context)
		&& ctrc_native_read_target_current(&context->binding.read, relation)
		&& ctrc_native_install_current(context);
}

/* Called only inside the original successful install attempt: both full-work
 * snapshots, exact PAGE and pre-WAL raw descriptors are still alive. A later
 * saved target_synced value cannot enter this path. Keep the DURABLE window
 * pending so late loss or descriptor-close failure retains exact failed debt. */
static bool
ctrc_native_install_publish_durable(const CtrcNativeInstallV1 *context)
{
	const RfPageCtrcFpiInsertV1 *fixed = &context->fixed;
	TimeLineID timeline;

	if (context->binding.read.step == NULL || context->binding.read.step->storage == NULL
		|| !ctrc_native_install_storage_current(context, NULL)
		|| !cluster_recovery_stamp_durable_v1(&fixed->window))
		return false;
	return memcmp(context->original, fixed, sizeof(*fixed)) == 0 && fixed->wal_flushed
		&& ctrc_native_insert_current(&context->binding, fixed)
		&& cluster_recovery_stamp_fpi_durable_current_v1(&fixed->window,
			fixed->record_start, fixed->record_end)
		&& cluster_wal_writer_check(&fixed->window.owner.writer) == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& GetFlushRecPtr(&timeline) >= fixed->record_end
		&& timeline == fixed->window.owner.writer.ref.timeline
		&& ctrc_native_read_target_current(&context->binding.read, NULL)
		&& memcmp(context->original, fixed, sizeof(*fixed)) == 0
		&& ctrc_native_insert_current(&context->binding, fixed)
		&& cluster_recovery_stamp_fpi_durable_current_v1(&fixed->window,
			fixed->record_start, fixed->record_end);
}

static bool
ctrc_native_install_image(CtrcNativeInstallV1 *context, const RfPageCtrcAuthorityV1 *authority)
{
	CtrcNativeInsertV1 *binding = &context->binding;
	const RfPageCtrcFpiInsertV1 *fixed = &context->fixed;
	const ClusterControlRootIdentity *writer = &fixed->window.owner.writer.ref.claim.identity;
	PGAlignedBlock canonical, current;
	SMgrRelation relation;
	uint64 token;

	if (memcmp(context->original, fixed, sizeof(*fixed)) != 0
		|| !ctrc_native_insert_inputs_current(binding, fixed)
		|| !GenericXLogRecoveryImageDescribe(&fixed->image, &binding->descriptor)
		|| memcmp(context->original, fixed, sizeof(*fixed)) != 0
		|| !ctrc_native_insert_inputs_current(binding, fixed)
		|| !ctrc_native_read_bind(&binding->read, binding->start, binding->work, authority,
			&context->original->image.identity, fixed->image.block_number,
			&context->original->image.version.before)
		|| memcmp(&binding->read.owner, &binding->accepted.checkpoint.owner, sizeof(binding->read.owner)) != 0
		|| !ctrc_native_install_current(context)
		|| writer->origin_node_id > PGRAC_PAGE_LSN_ORIGIN_MAX
		|| writer->origin_thread_id != writer->origin_node_id + 1)
		return false;
	canonical = fixed->image.image;
	/* This is the actual newly inserted writer's coordinate. Never relabel
	 * it as the failed replay source, or let a foreign merge window clamp it. */
	PageSetLSNPreserveOrigin(canonical.data, fixed->record_end);
	if (!PageSetLSNOrigin(canonical.data, writer->origin_node_id)) return false;
	((PageHeader)canonical.data)->pd_checksum = 0;
	if (binding->read.checksums)
		((PageHeader)canonical.data)->pd_checksum = pg_checksum_page(canonical.data, fixed->image.block_number);
	if (!ctrc_native_read_integrity(&binding->read, &canonical, fixed->image.block_number)
		|| ((PageHeader)canonical.data)->pd_block_scn != fixed->image.result_token
		|| !ctrc_native_install_current(context))
		return false;
	relation = binding->read.step == NULL ? smgropen(binding->read.identity.locator, InvalidBackendId) : NULL;
	if (!ctrc_native_install_current(context)
		|| !ctrc_native_install_observe(context, relation, true, &current))
		return false;
	token = ((PageHeader)current.data)->pd_block_scn;
	if (token == fixed->image.result_token) {
		if (memcmp(&current, &canonical, sizeof(current)) != 0) return false;
	} else if (token == fixed->image.version.before.mutation_token) {
		if (!ctrc_native_install_storage_current(context, relation)) return false;
		if (binding->read.step != NULL) {
			if (!cluster_shared_fs_page_write_v1(binding->read.step->storage, canonical.data)) return false;
		} else
			smgrwrite(relation, MAIN_FORKNUM, fixed->image.block_number, canonical.data, false);
		if (!ctrc_native_install_storage_current(context, relation)) return false;
	} else
		return false;
	if (!ctrc_native_install_storage_current(context, relation)) return false;
	if (binding->read.step != NULL) {
		if (!cluster_shared_fs_page_sync_v1(binding->read.step->storage)) return false;
	} else
		smgrimmedsync(relation, MAIN_FORKNUM);
	return ctrc_native_install_storage_current(context, relation)
		&& ctrc_native_install_observe(context, relation, false, &current)
		&& memcmp(&current, &canonical, sizeof(current)) == 0
		&& ctrc_native_install_current(context);
}

/* Refuse additional caller locks before any borrowed owner/current snapshot.
 * The only held lock must be the original target PAGE, subsequently matched
 * by the unchanged native reference. Native storage/flush snapshots retain
 * ample headroom without raising the native LWLock ceiling. */
static bool
ctrc_native_install_bind(CtrcNativeInstallV1 *context,
	const struct ClusterRecoveryStartV1 *start, const ClusterColdCtrcWorkV1 *work,
	const ClusterRecoveryCtrcReplaySetSourceV1 *source, const RfPageCtrcAuthorityV1 *authority,
	Size subject_count, Size memory_budget, ClusterColdCtrcWorkV1 *header)
{
	CtrcNativeInsertV1 *binding = &context->binding;
	uint32 held = 0;

	if (!ctrc_native_read_actor() || start == NULL || work == NULL || source == NULL || authority == NULL)
		return false;
	ForEachLWLockHeldByMe(ctrc_native_insert_count_lock, &held);
	if (held != 1 || !ClusterColdRecoveryCtrcWorkCurrent(start, work)
		|| subject_count != work->counts.subject_count
		|| subject_count > (MaxAllocSize - offsetof(ClusterColdCtrcWorkV1, entries)) / sizeof(work->entries[0])
		|| source->scope == NULL || ClusterColdRecoveryCtrcScope(start, work) != source->scope)
		return false;
	binding->work_bytes = offsetof(ClusterColdCtrcWorkV1, entries)
		+ subject_count * sizeof(work->entries[0]);
	if (binding->work_bytes > memory_budget) return false;
	memcpy(header, work, offsetof(ClusterColdCtrcWorkV1, entries));
	binding->start = start; binding->work = work;
	binding->original_source = source; memcpy(&binding->source, source, sizeof(binding->source));
	binding->original_image = &context->original->image;
	return cluster_recovery_ctrc_terminal_result_v1(start, work, &binding->accepted);
}

/* Fixed input/binding live in the caller's frame, outside this setjmp. */
static bool
ctrc_native_install_attempt(CtrcNativeInstallV1 *context,
	const struct ClusterRecoveryStartV1 *start, const ClusterColdCtrcWorkV1 *work,
	const ClusterRecoveryCtrcReplaySetSourceV1 *source, const RfPageCtrcAuthorityV1 *authority,
	Size subject_count, Size memory_budget)
{
	void *volatile snapshot = NULL;
	volatile bool completed = false;

	PG_TRY();
	{
		ClusterColdCtrcWorkV1 header;
		if (ctrc_native_install_bind(context, start, work, source, authority, subject_count, memory_budget, &header)) {
			snapshot = MemoryContextAllocZero(CurrentMemoryContext, context->binding.work_bytes);
			if (ClusterColdRecoveryCtrcWorkCurrent(start, work)
				&& memcmp(work, &header, offsetof(ClusterColdCtrcWorkV1, entries)) == 0) {
				memcpy((void *)snapshot, work, context->binding.work_bytes);
				context->binding.snapshot = (const ClusterColdCtrcWorkV1 *)snapshot;
				completed = ctrc_native_install_image(context, authority);
				if (completed && context->publish_durable)
					completed = ctrc_native_install_publish_durable(context);
			}
		}
		if (!completed) (void)cluster_recovery_stamp_fail_v1(&context->fixed.window);
	}
	PG_CATCH();
	{
		(void)cluster_recovery_stamp_fail_v1(&context->fixed.window);
		if (snapshot != NULL) pfree((void *)snapshot);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (snapshot != NULL) pfree((void *)snapshot);
	return completed;
}

bool
rf_page_recovery_ctrc_replays_fpi_install_smgr_v1(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, const ClusterRecoveryCtrcReplaySetSourceV1 *source,
	const RfPageCtrcAuthorityV1 *authority, const RfPageCtrcFpiInsertV1 *inserted,
	Size subject_count, Size memory_budget)
{
	CtrcNativeInstallV1 context = {0};

	if (inserted == NULL) return false;
	context.original = inserted; memcpy(&context.fixed, inserted, sizeof(context.fixed));
	return ctrc_native_install_attempt(&context, start, work, source, authority, subject_count, memory_budget);
}

typedef struct CtrcNativeFpiV1 {
	CtrcNativeReadV1 read;
	ClusterSpaceIdentity space;
	const ClusterRecoveryUndoEntry *original_physical;
	Size count;
	Size subject_bytes;
	Size physical_bytes;
	ClusterColdTtSlotInputV1 *originals;
	ClusterRecoveryUndoEntry *physical;
	ClusterColdTtSlotInputV1 *terminals;
	ClusterRecoveryTxResultObservationV1 accepted;
} CtrcNativeFpiV1;

static bool
ctrc_native_fpi_inputs_current(const CtrcNativeFpiV1 *context)
{
	return ctrc_native_read_current(&context->read)
		&& (context->count == 0
			|| (memcmp(context->read.work->entries, context->originals, context->subject_bytes) == 0
				&& memcmp(context->original_physical, context->physical, context->physical_bytes) == 0));
}

/* Re-run only the original canonical factories, never infer a terminal. */
static bool
ctrc_native_fpi_current(const CtrcNativeFpiV1 *context)
{
	ClusterRecoveryTxResultObservationV1 accepted;
	ClusterColdTtSlotInputV1 terminal;

	if (!ctrc_native_fpi_inputs_current(context)
		|| !cluster_recovery_ctrc_terminal_result_v1(context->read.start, context->read.work, &accepted)
		|| memcmp(&accepted, &context->accepted, sizeof(accepted)) != 0)
		return false;
	for (Size i = 0; i < context->count; i++)
		if (!ctrc_native_fpi_inputs_current(context)
			|| !cluster_recovery_ctrc_terminal_subject_v1(context->read.start, context->read.work,
				i, &context->physical[i], &terminal)
			|| memcmp(&terminal, &context->terminals[i], sizeof(terminal)) != 0)
			return false;
	return ctrc_native_fpi_inputs_current(context)
		&& cluster_recovery_ctrc_terminal_result_v1(context->read.start, context->read.work, &accepted)
		&& memcmp(&accepted, &context->accepted, sizeof(accepted)) == 0
		&& ctrc_native_fpi_inputs_current(context);
}

/* The storage checksum was verified by the original reader. Every other
 * byte, including the exact thread-qualified LSN, must match the fixed graph. */
static bool
ctrc_native_graph_image(const CtrcNativeStepV1 *step, const PGAlignedBlock *image)
{
	const Size offset = offsetof(PageHeaderData, pd_checksum);
	const Size end = offset + sizeof(((PageHeader)0)->pd_checksum);

	return step == NULL || step->original_canonical == NULL
		|| (memcmp(image->data, step->canonical.data, offset) == 0
			&& memcmp(image->data + end, step->canonical.data + end, BLCKSZ - end) == 0);
}

static void
ctrc_native_fpi_prepare(CtrcNativeFpiV1 *context, void *snapshot, RfPageCtrcFpiV1 *out)
{
	PGAlignedBlock before, after;
	GenericXLogRecoveryImageV1 frame;
	ClusterCtrcItlCleanoutApplyResult projected;

	if (!ctrc_native_read_current(&context->read))
		return;
	if (context->count != 0) {
		context->originals = snapshot;
		context->physical = (void *)((char *)snapshot + MAXALIGN(context->subject_bytes));
		context->terminals = (void *)((char *)context->physical + MAXALIGN(context->physical_bytes));
		memcpy(context->originals, context->read.work->entries, context->subject_bytes);
		memcpy(context->physical, context->original_physical, context->physical_bytes);
	}
	if (!ctrc_native_fpi_inputs_current(context)
		|| !cluster_recovery_ctrc_terminal_result_v1(context->read.start, context->read.work,
			&context->accepted))
		return;
	for (Size i = 0; i < context->count; i++)
		if (!ctrc_native_fpi_inputs_current(context)
			|| !cluster_recovery_ctrc_terminal_subject_v1(context->read.start, context->read.work,
				i, &context->physical[i], &context->terminals[i]))
			return;
	if (!ctrc_native_fpi_current(context)
		|| !ctrc_native_read_image(&context->read, &before)
		|| !ctrc_native_graph_image(context->read.step, &before)
		|| !ctrc_native_fpi_current(context))
		return;
	projected = cluster_ctrc_recovery_itl_page_image_v1(context->terminals, context->count,
		before.data, after.data);
	if ((projected != CLUSTER_CTRC_ITL_CLEANOUT_REWRITTEN
			&& projected != CLUSTER_CTRC_ITL_CLEANOUT_ALREADY_TERMINAL)
		|| !ctrc_native_fpi_current(context))
		return;
	if (projected == CLUSTER_CTRC_ITL_CLEANOUT_REWRITTEN) {
		if (!GenericXLogRecoveryImagePrepare(GENERIC_XLOG_CTRC_ITL, &context->space,
				MAIN_FORKNUM, context->read.identity.blockno, before.data, after.data, &frame)
			|| !ctrc_native_fpi_current(context))
			return;
		out->frame = frame;
		out->rewritten = true;
	}
	out->ready = true;
}

static RfPageCtrcFpiV1
ctrc_native_itl_fpi(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, const RfPageCtrcAuthorityV1 *authority,
	const ClusterSpaceIdentity *space, BlockNumber blockno, const RfPageVersionV1 *expected_before,
	const ClusterRecoveryUndoEntry *physical, Size physical_count, Size memory_budget,
	const CtrcNativeStepV1 *step, CtrcNativeStepImagesV1 *capture)
{
	RfPageCtrcFpiV1 result = {0};
	CtrcNativeFpiV1 context = {0};
	void *volatile snapshot = NULL;
	Size bytes;
	const Size per_subject = 2 * MAXALIGN(sizeof(ClusterColdTtSlotInputV1))
		+ MAXALIGN(sizeof(ClusterRecoveryUndoEntry));

	context.read.step = step; context.read.capture = capture;
	context.read.step_current = step == NULL ? NULL : ctrc_native_step_current;
	if (!ctrc_native_read_bind(&context.read, start, work, authority, space, blockno, expected_before)
		|| physical_count != context.read.counts.subject_count
		|| context.read.counts.unqualified_count != 0
		|| physical_count > MaxAllocSize / per_subject
		|| (physical_count != 0 && physical == NULL))
		return result;
	context.count = physical_count;
	context.subject_bytes = physical_count * sizeof(ClusterColdTtSlotInputV1);
	context.physical_bytes = physical_count * sizeof(ClusterRecoveryUndoEntry);
	bytes = 2 * MAXALIGN(context.subject_bytes) + MAXALIGN(context.physical_bytes);
	if (bytes > memory_budget)
		return result;
	context.original_physical = physical;
	context.space = *space;
	PG_TRY();
	{
		if (bytes != 0)
			snapshot = MemoryContextAllocZero(CurrentMemoryContext, bytes);
		ctrc_native_fpi_prepare(&context, (void *)snapshot, &result);
	}
	PG_CATCH();
	{
		if (snapshot != NULL)
			pfree((void *)snapshot);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (snapshot != NULL)
		pfree((void *)snapshot);
	return result;
}

RfPageCtrcFpiV1
rf_page_recovery_ctrc_replays_itl_fpi_smgr_v1(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, const RfPageCtrcAuthorityV1 *authority,
	const ClusterSpaceIdentity *space, BlockNumber blockno, const RfPageVersionV1 *expected_before,
	const ClusterRecoveryUndoEntry *physical, Size physical_count, Size memory_budget)
{
	return ctrc_native_itl_fpi(start, work, authority, space, blockno, expected_before,
		physical, physical_count, memory_budget, NULL, NULL);
}

/* Preparation already proved the complete graph/ITL terminal image. Keep its
 * PAGE and original raw descriptors while making that observation durable;
 * no stamp/WAL mutation is needed when every byte remains unchanged. */
static bool
ctrc_native_step_sync_unchanged(CtrcNativeStepV1 *context,
	const RfPageCtrcAuthorityV1 *authority, BlockNumber blockno)
{
	CtrcNativeReadV1 read = {0};
	CtrcNativeStepImagesV1 verified = {0};

	read.step = context; read.step_current = ctrc_native_step_current;
	read.capture = &verified;
	if (context->storage == NULL || !context->images.ready || !ctrc_native_step_current(context)
		|| !ctrc_native_read_bind(&read, context->start, context->work, authority,
			context->original_space, blockno, context->original_before)
		|| !ctrc_native_read_current(&read) || !ctrc_native_read_target_current(&read, NULL)
		|| !cluster_shared_fs_page_sync_v1(context->storage)
		|| !ctrc_native_read_image(&read, &verified.page) || !verified.ready
		|| memcmp(verified.page.data, context->images.page.data, BLCKSZ) != 0
		|| memcmp(verified.space0.data, context->images.space0.data, BLCKSZ) != 0
		|| memcmp(verified.space1.data, context->images.space1.data, BLCKSZ) != 0)
		return false;
	return ctrc_native_read_current(&read) && ctrc_native_read_target_current(&read, NULL)
		&& ctrc_native_step_current(context);
}

static void
ctrc_native_step_execute(CtrcNativeStepV1 *context, RfPageCtrcAuthorityV1 *authority,
	BlockNumber blockno, Size count, Size memory_budget, RfPageCtrcStepResultV1 *result)
{
	RfPageIdentityV1 identity = {0};
	RfPageAuthorityVerdictV1 promotion = RF_PAGE_AUTHORITY_INVALID_ARGUMENT;
	RfPageCtrcFpiV1 prepared;
	CtrcNativeInstallV1 install = {0};

	if (!ctrc_native_step_current(context)) return;
	identity.system_identifier = context->space.key.system_identifier;
	memcpy(identity.storage_uuid, context->space.key.storage_uuid, sizeof(identity.storage_uuid));
	identity.locator = context->space.key.locator;
	identity.forknum = MAIN_FORKNUM; identity.blockno = blockno;
	promotion = rf_page_authority_ctrc_replays_promote_fpi_nowait_v1(authority, &identity, &context->before);
	if (promotion == RF_PAGE_AUTHORITY_WOULD_BLOCK) { result->phase = RF_PAGE_CTRC_STEP_WAIT; return; }
	if (promotion != RF_PAGE_AUTHORITY_OK || !ctrc_native_step_current(context)) return;
	prepared = ctrc_native_itl_fpi(context->start, context->work, authority,
		context->original_space, blockno, context->original_before,
		context->original_physical, count, memory_budget, context, &context->images);
	if (!prepared.ready || !ctrc_native_step_current(context)) return;
	/* Unchanged sync also precedes Startup progress. Resolve the same native
	 * cache duties first and keep absence under the original current gates. */
	if (context->sync_unchanged) {
		ClusterCtrcPageCacheResultV1 cache = cluster_bufmgr_ctrc_page_cache_v1(context->start,
			context->work, authority, context->original_space, blockno, context->original_before,
			context->images.page.data);
		if (cache == CLUSTER_CTRC_PAGE_CACHE_WAIT) { result->phase = RF_PAGE_CTRC_STEP_WAIT; return; }
		if (cache != CLUSTER_CTRC_PAGE_CACHE_ABSENT) return;
		context->cache_authority = authority; context->cache_block = blockno;
		context->cache_retired = true;
	}
	if (!prepared.rewritten) {
		if (!context->sync_unchanged) result->phase = RF_PAGE_CTRC_STEP_UNCHANGED;
		else if (ctrc_native_step_sync_unchanged(context, authority, blockno))
			result->phase = RF_PAGE_CTRC_STEP_UNCHANGED_SYNCED;
		return;
	}
	if (!rf_page_authority_ctrc_replays_unpromote_v1(authority)
		|| !ctrc_native_step_current(context))
		return;
	promotion = RF_PAGE_AUTHORITY_INVALID_ARGUMENT;
	result->inserted = ctrc_native_fpi_insert(context->start, context->work,
		context->original_source, authority, &prepared.frame, count, memory_budget, context, &promotion);
	if (!result->inserted.wal_flushed) {
		if (promotion == RF_PAGE_AUTHORITY_WOULD_BLOCK) result->phase = RF_PAGE_CTRC_STEP_WAIT;
		return;
	}
	install.original = &result->inserted;
	memcpy(&install.fixed, &result->inserted, sizeof(install.fixed));
	install.binding.read.step = context;
	install.binding.read.step_current = ctrc_native_step_current;
	install.publish_durable = context->publish_durable;
	if (ctrc_native_install_attempt(&install, context->start, context->work,
			context->original_source, authority, count, memory_budget)
		&& ctrc_native_step_current(context))
		result->phase = context->publish_durable ? RF_PAGE_CTRC_STEP_FPI_DURABLE
			: RF_PAGE_CTRC_STEP_TARGET_SYNCED;
	else if (context->publish_durable)
		(void)cluster_recovery_stamp_fail_v1(&result->inserted.window);
}

/* Binding/result belong to the caller's frame, outside this setjmp. */
static void
ctrc_native_step_attempt(CtrcNativeStepV1 *context, const ClusterColdCtrcWorkV1 *header,
	RfPageCtrcAuthorityV1 *authority, BlockNumber blockno, Size count, Size bytes,
	Size memory_budget, RfPageCtrcStepResultV1 *result)
{
	void *volatile snapshot = NULL;

	PG_TRY();
	{
		snapshot = MemoryContextAllocZero(CurrentMemoryContext, bytes);
		if (ClusterColdRecoveryCtrcWorkCurrent(context->start, context->work)
			&& memcmp(context->work, header, offsetof(ClusterColdCtrcWorkV1, entries)) == 0) {
			context->snapshot = snapshot;
			context->physical = (const void *)((const char *)snapshot + MAXALIGN(context->work_bytes));
			memcpy((void *)context->snapshot, context->work, context->work_bytes);
			if (context->physical_bytes != 0)
				memcpy((void *)context->physical, context->original_physical, context->physical_bytes);
			/* Fix namespace/inode/extent and raw descriptors before the first
			 * PAGE. No VFD reopen is permitted in the subsequent WAL window. */
			if (ctrc_native_step_current(context)) {
				context->storage = cluster_shared_fs_page_open_v1(context->space.key.locator,
					blockno, memory_budget - bytes);
				if (context->storage != NULL && ctrc_native_step_current(context))
					ctrc_native_step_execute(context, authority, blockno, count,
						memory_budget - bytes - cluster_shared_fs_page_bytes_v1(), result);
			}
		}
	}
	PG_CATCH();
	{
		if (result->inserted.window.key.window_seq != 0)
			(void)cluster_recovery_stamp_fail_v1(&result->inserted.window);
		(void)cluster_shared_fs_page_close_v1(context->storage); context->storage = NULL;
		if (snapshot != NULL) pfree((void *)snapshot);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (!cluster_shared_fs_page_close_v1(context->storage)) {
		result->phase = RF_PAGE_CTRC_STEP_REFUSED;
		if (result->inserted.window.key.window_seq != 0)
			(void)cluster_recovery_stamp_fail_v1(&result->inserted.window);
	}
	context->storage = NULL;
	if (snapshot != NULL) pfree((void *)snapshot);
}

static RfPageCtrcStepResultV1
ctrc_native_itl_step(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, const ClusterRecoveryCtrcReplaySetSourceV1 *source,
	RfPageCtrcAuthorityV1 *authority, const ClusterSpaceIdentity *space,
	BlockNumber blockno, const RfPageVersionV1 *expected_before, const char *canonical,
	const ClusterRecoveryUndoEntry *physical, Size physical_count, Size memory_budget,
	bool publish_durable, bool sync_unchanged)
{
	RfPageCtrcStepResultV1 result = {0};
	CtrcNativeStepV1 context = {0};
	ClusterColdCtrcWorkV1 header;
	Size bytes, prepare_bytes, storage_bytes = cluster_shared_fs_page_bytes_v1();
	uint32 held = 0;
	const Size per_subject = 2 * MAXALIGN(sizeof(ClusterColdTtSlotInputV1))
		+ MAXALIGN(sizeof(ClusterRecoveryUndoEntry));

	if (!ctrc_native_read_actor() || start == NULL || work == NULL || source == NULL
		|| authority == NULL || space == NULL || expected_before == NULL)
		return result;
	ForEachLWLockHeldByMe(ctrc_native_insert_count_lock, &held);
	if (held != 0 || !ClusterColdRecoveryCtrcWorkCurrent(start, work)
		|| physical_count != work->counts.subject_count || work->counts.unqualified_count != 0
		|| physical_count > (MaxAllocSize - offsetof(ClusterColdCtrcWorkV1, entries)) / sizeof(work->entries[0])
		|| physical_count > MaxAllocSize / per_subject
		|| (physical_count != 0 && physical == NULL) || !rf_page_version_present_v1(expected_before)
		|| source->scope == NULL || ClusterColdRecoveryCtrcScope(start, work) != source->scope)
		return result;
	context.work_bytes = offsetof(ClusterColdCtrcWorkV1, entries) + physical_count * sizeof(work->entries[0]);
	context.physical_bytes = physical_count * sizeof(*physical);
	if (context.work_bytes > MaxAllocSize - (MAXIMUM_ALIGNOF - 1)
		|| context.physical_bytes > MaxAllocSize - MAXALIGN(context.work_bytes))
		return result;
	bytes = MAXALIGN(context.work_bytes) + context.physical_bytes;
	prepare_bytes = 2 * MAXALIGN(physical_count * sizeof(work->entries[0])) + MAXALIGN(context.physical_bytes);
	/* Reserve the peak before any PAGE lock, including the original F4 copy. */
	if (storage_bytes > memory_budget || bytes > memory_budget - storage_bytes
		|| prepare_bytes > memory_budget - storage_bytes - bytes
		|| context.work_bytes > (memory_budget - storage_bytes - bytes) / 2)
		return result;
	memcpy(&header, work, offsetof(ClusterColdCtrcWorkV1, entries));
	context.start = start; context.work = work;
	context.publish_durable = publish_durable;
	context.sync_unchanged = sync_unchanged;
	context.original_source = source; memcpy(&context.source, source, sizeof(context.source));
	context.original_space = space; memcpy(&context.space, space, sizeof(context.space));
	if (space->state != CLUSTER_SPACE_IDENTITY_LIVE
		|| !cluster_space_identity_encode(space, context.space_identity, sizeof(context.space_identity)))
		return result;
	context.original_before = expected_before; memcpy(&context.before, expected_before, sizeof(context.before));
	context.original_physical = physical;
	context.original_canonical = canonical;
	if (canonical != NULL) memcpy(context.canonical.data, canonical, BLCKSZ);
	if (!cluster_recovery_ctrc_terminal_result_v1(start, work, &context.accepted)) return result;
	ctrc_native_step_attempt(&context, &header, authority, blockno, physical_count, bytes, memory_budget, &result);
	return result;
}

RfPageCtrcStepResultV1
rf_page_recovery_ctrc_replays_itl_step_smgr_v1(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, const ClusterRecoveryCtrcReplaySetSourceV1 *source,
	RfPageCtrcAuthorityV1 *authority, const ClusterSpaceIdentity *space,
	BlockNumber blockno, const RfPageVersionV1 *expected_before,
	const ClusterRecoveryUndoEntry *physical, Size physical_count, Size memory_budget)
{
	return ctrc_native_itl_step(start, work, source, authority, space, blockno,
		expected_before, NULL, physical, physical_count, memory_budget, false, false);
}

RfPageCtrcStepResultV1
rf_page_recovery_ctrc_replays_itl_graph_step_smgr_v1(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, const ClusterRecoveryCtrcReplaySetSourceV1 *source,
	RfPageCtrcAuthorityV1 *authority, const ClusterSpaceIdentity *space,
	BlockNumber blockno, const RfPageVersionV1 *expected_before, const char canonical[BLCKSZ],
	const ClusterRecoveryUndoEntry *physical, Size physical_count, Size memory_budget)
{
	if (canonical == NULL) return (RfPageCtrcStepResultV1){0};
	return ctrc_native_itl_step(start, work, source, authority, space, blockno,
		expected_before, canonical, physical, physical_count, memory_budget, false, false);
}

RfPageCtrcStepResultV1
rf_page_recovery_ctrc_replays_itl_graph_durable_smgr_v1(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, const ClusterRecoveryCtrcReplaySetSourceV1 *source,
	RfPageCtrcAuthorityV1 *authority, const ClusterSpaceIdentity *space,
	BlockNumber blockno, const RfPageVersionV1 *expected_before, const char canonical[BLCKSZ],
	const ClusterRecoveryUndoEntry *physical, Size physical_count, Size memory_budget)
{
	if (canonical == NULL) return (RfPageCtrcStepResultV1){0};
	return ctrc_native_itl_step(start, work, source, authority, space, blockno,
		expected_before, canonical, physical, physical_count, memory_budget, true, false);
}

RfPageCtrcStepResultV1
rf_page_recovery_ctrc_replays_itl_graph_sync_smgr_v1(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, const ClusterRecoveryCtrcReplaySetSourceV1 *source,
	RfPageCtrcAuthorityV1 *authority, const ClusterSpaceIdentity *space,
	BlockNumber blockno, const RfPageVersionV1 *expected_before, const char canonical[BLCKSZ],
	const ClusterRecoveryUndoEntry *physical, Size physical_count, Size memory_budget)
{
	if (canonical == NULL) return (RfPageCtrcStepResultV1){0};
	return ctrc_native_itl_step(start, work, source, authority, space, blockno,
		expected_before, canonical, physical, physical_count, memory_budget, true, true);
}
#endif /* USE_PGRAC_CLUSTER */
