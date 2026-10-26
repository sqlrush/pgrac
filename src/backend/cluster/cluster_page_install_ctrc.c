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

#include "cluster/cluster_page_install_ctrc.h"

#ifdef USE_PGRAC_CLUSTER
#include "access/xlog.h"
#include "access/xlogrecovery.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_recovery_plan.h"
#include "cluster/cluster_space_reservation.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "utils/memutils.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "storage/checksum.h"
#include "storage/smgr.h"

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
} CtrcNativeReadV1;

static bool
ctrc_native_read_actor(void)
{
	return cluster_enabled && cluster_shared_config && IsUnderPostmaster
		&& MyBackendType == B_STARTUP && MyProcPid > 0 && CritSectionCount == 0
		&& !ShutdownRequestPending && !RecoveryInProgress();
}

static bool
ctrc_native_read_current(const CtrcNativeReadV1 *context)
{
	ClusterRecoveryCheckpointOwnerV1 owner;
	const ClusterColdTypedV1 *typed;
	uint8 encoded[CLUSTER_SPACE_IDENTITY_BYTES];

	if (!ctrc_native_read_actor()
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
			&context->identity, &context->before);
}

static bool
ctrc_native_read_storage(SMgrRelation relation, BlockNumber blockno)
{
	return smgrexists(relation, SPACE_FORKNUM) && smgrnblocks(relation, SPACE_FORKNUM) == 2
		&& smgrexists(relation, MAIN_FORKNUM) && blockno < smgrnblocks(relation, MAIN_FORKNUM);
}

static bool
ctrc_native_read_page(const CtrcNativeReadV1 *context, SMgrRelation relation,
	ForkNumber forknum, BlockNumber blockno, PGAlignedBlock *out)
{
	if (!ctrc_native_read_current(context)
		|| !ctrc_native_read_storage(relation, context->identity.blockno)
		|| !ctrc_native_read_current(context))
		return false;
	smgrread(relation, forknum, blockno, out->data);
	return ctrc_native_read_current(context)
		&& ctrc_native_read_storage(relation, context->identity.blockno)
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

	relation = smgropen(context->identity.locator, InvalidBackendId);
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

RfPageCtrcFpiV1
rf_page_recovery_ctrc_replays_itl_fpi_smgr_v1(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, const RfPageCtrcAuthorityV1 *authority,
	const ClusterSpaceIdentity *space, BlockNumber blockno, const RfPageVersionV1 *expected_before,
	const ClusterRecoveryUndoEntry *physical, Size physical_count, Size memory_budget)
{
	RfPageCtrcFpiV1 result = {0};
	CtrcNativeFpiV1 context = {0};
	void *volatile snapshot = NULL;
	Size bytes;
	const Size per_subject = 2 * MAXALIGN(sizeof(ClusterColdTtSlotInputV1))
		+ MAXALIGN(sizeof(ClusterRecoveryUndoEntry));

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
#endif /* USE_PGRAC_CLUSTER */
