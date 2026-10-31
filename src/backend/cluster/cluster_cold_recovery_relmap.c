/*-------------------------------------------------------------------------
 * cluster_cold_recovery_relmap.c
 *     One-shot relmap participant at the original cold replay position.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/backend/cluster/cluster_cold_recovery_relmap.c
 * NOTES
 *     Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER
#include "access/xlogreader.h"
#include "cluster/cluster_cold_recovery_plan_internal.h"
#include "cluster/cluster_relmap_authority.h"
#include "cluster/cluster_relmap_retention.h"
#include "storage/lwlock.h"
#include "utils/relmapper.h"
#include "miscadmin.h"
#include "catalog/pg_tablespace_d.h"
#include "replication/origin.h"

static bool
relmap_current(ClusterColdTypedV1 *typed, struct ClusterRecoveryFencePlan *fence,
	const ClusterColdRelmapInputV1 *input)
{
	const ClusterColdPlanV1 *plan = typed->plan;
	uint32 p = input->participant;

	return typed->refusal == CLUSTER_COLD_OK && p < typed->replay_count
		&& typed->participant_count == plan->participant_count
		&& typed->system_identifier == input->staged.identity.system_identifier
		&& cold_relmap_cut_equal(&input->cut, &typed->participants[p])
		&& memcmp(&input->source, &typed->sources[p], sizeof(input->source)) == 0
		&& cluster_cold_typed_sources_current_v1(typed, fence);
}

static bool
relmap_pin(const ClusterColdRelmapInputV1 *input, bool staged)
{
	const ClusterRelmapV2Image *image = &input->staged;
	const ClusterRelmapV2Owner *owner = &image->pending_owner;
	ClusterRelmapRetentionKey key = {0};
	volatile bool locked = false;
	volatile bool ok = false;

	if (LWLockHeldByMe(input->retention_lock))
		return false;
	key.identity = image->identity;
	key.generation = image->pending_generation;
	key.origin = owner->node;
	key.top_full_xid = owner->top_full_xid;
	key.membership_epoch = owner->membership_epoch;
	key.admitted_incarnation = owner->admitted_incarnation;
	key.boot_incarnation = owner->boot_incarnation;
	PG_TRY();
	{
		LWLockAcquire(input->retention_lock, LW_EXCLUSIVE);
		locked = true;
		ok = (staged
			? cluster_relmap_retention_stage_check(input->retention, &key, owner->wal_end)
			: cluster_relmap_retention_prepare_check(input->retention, &key))
			== CLUSTER_RELMAP_RETENTION_OK;
		LWLockRelease(input->retention_lock);
		locked = false;
	}
	PG_CATCH();
	{
		HOLD_INTERRUPTS();
		if (locked)
			LWLockRelease(input->retention_lock);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return ok;
}

static bool
relmap_record_matches(const ClusterColdRelmapInputV1 *input, XLogReaderState *reader)
{
	xl_relmap_update header;
	const ClusterColdRecordFrameV1 *frame = &input->frame;
	const DecodedXLogRecord *record;

	if (reader == NULL || reader->record == NULL)
		return false;
	record = reader->record;
	if (reader->ReadRecPtr != frame->read_rec_ptr || reader->EndRecPtr != frame->end_rec_ptr
		|| record->header.xl_crc != frame->record_crc || record->header.xl_rmid != frame->rmid
		|| record->header.xl_info != frame->info || record->header.xl_xid != frame->source_xid
		|| record->toplevel_xid != frame->source_top_xid || XLogRecHasAnyBlockRefs(reader)
		|| record->has_page_version_edge || XLogRecGetOrigin(reader) != InvalidRepOriginId
		|| XLogRecGetData(reader) == NULL
		|| XLogRecGetDataLen(reader) != MinSizeOfRelmapUpdate + sizeof(RelMapFile))
		return false;
	memcpy(&header, XLogRecGetData(reader), MinSizeOfRelmapUpdate);
	return header.dbid == input->staged.identity.dbid
		&& header.tsid == (input->staged.identity.shared ? GLOBALTABLESPACE_OID : DEFAULTTABLESPACE_OID)
		&& header.nbytes == sizeof(RelMapFile)
		&& memcmp(XLogRecGetData(reader) + MinSizeOfRelmapUpdate, &input->staged.pending,
			sizeof(RelMapFile)) == 0;
}

bool
cluster_cold_typed_relmap_record_v1(ClusterColdTypedV1 *typed,
	struct ClusterRecoveryFencePlan *fence, const ClusterColdStepV1 *step, XLogReaderState *reader)
{
	ClusterColdPlanV1 *plan;
	ClusterColdStepV1 expected;
	const ColdRecord *record;
	ColdRelmapInput *entry;
	const ClusterColdRelmapInputV1 *input;
	ClusterRelmapV2View base;
	unsigned char encoded[CLUSTER_RELMAP_V2_FILE_SIZE];
	bool ok = false;

	if (typed == NULL || fence == NULL || step == NULL || !plan_valid(typed->plan))
		return false;
	plan = typed->plan;
	if (plan->relmap_failed || !cluster_cold_plan_step_v1(plan, step->step_index, &expected)
		|| expected.step_kind != CLUSTER_COLD_STEP_RELMAP || memcmp(step, &expected, sizeof(expected)) != 0)
		return false;
	record = cold_record(plan, plan->schedule[step->step_index]);
	if (record->first_component >= plan->relmap_count)
		return false;
	entry = &plan->relmap_inputs[record->first_component];
	input = &entry->input;
	if (entry->attempted && !entry->applied)
		return false;
	/* Fail sticky before any provider, pin or I/O can throw. Applied repeats
	 * only verify the old receipt and complete pair; they never enter stage. */
	entry->attempted = true;
	plan->relmap_failed = true;
	PG_TRY();
	{
		if (entry->captured && relmap_current(typed, fence, input)
			&& relmap_record_matches(input, reader)
			&& cluster_relmap_v2_encode(&input->staged, encoded, sizeof(encoded)) == CLUSTER_RELMAP_V2_VALID
			&& relmap_pin(input, entry->applied)) {
			if (entry->applied)
				ok = cluster_relmap_authority_v2_check_staged(&input->staged);
			else if (cluster_relmap_authority_v2_read(&input->staged.identity, &base)
					== CLUSTER_RELMAP_V2_PAIR_VALID && !base.has_pending
				&& base.committed_generation == input->staged.committed_generation
				&& memcmp(&base.committed, &input->staged.committed, sizeof(RelMapFile)) == 0
				&& relmap_current(typed, fence, input) && relmap_pin(input, false))
				ok = cluster_relmap_authority_v2_write_pending(&input->staged.identity,
					input->staged.pending_generation, &input->staged.pending_owner,
					&input->staged.pending, input->retention, input->retention_lock)
					&& cluster_relmap_authority_v2_check_staged(&input->staged);
			ok = ok && relmap_current(typed, fence, input) && relmap_pin(input, true);
		}
	}
	PG_CATCH();
	{
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (ok) {
		entry->applied = true;
		plan->relmap_failed = false;
	}
	return ok;
}

bool
cluster_cold_typed_relmap_ready_v1(ClusterColdTypedV1 *typed, struct ClusterRecoveryFencePlan *fence)
{
	ClusterColdPlanV1 *plan;
	bool ok = true;

	if (typed == NULL || fence == NULL || !plan_valid(typed->plan))
		return false;
	plan = typed->plan;
	if (plan->phase != COLD_PHASE_SEALED || plan->relmap_failed)
		return false;
	for (uint32 i = 0; i < plan->relmap_count; ++i)
		if (!plan->relmap_inputs[i].captured || !plan->relmap_inputs[i].applied)
			return false;
	/* Querying an incomplete batch grants nothing but does not consume it.
	 * Once all receipts exist, even an ERROR during revalidation is sticky. */
	if (plan->relmap_count == 0)
		return true;
	plan->relmap_failed = true;
	for (uint32 i = 0; i < plan->relmap_count && ok; ++i) {
		const ClusterColdRelmapInputV1 *input = &plan->relmap_inputs[i].input;
		ok = relmap_current(typed, fence, input) && relmap_pin(input, true)
			&& cluster_relmap_authority_v2_check_staged(&input->staged)
			&& relmap_current(typed, fence, input) && relmap_pin(input, true);
	}
	if (ok)
		plan->relmap_failed = false;
	return ok;
}
#endif
