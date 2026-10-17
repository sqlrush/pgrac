/*-------------------------------------------------------------------------
 *
 * cluster_cold_recovery_startup.c
 *	  Startup-process driver state for typed cold-crash replay.
 *
 *	  Pass 1 runs here, with every foreign origin's external admission held
 *	  and before the serial set (retention pin, then IR), so its ROOT/CF
 *	  reads and WAL scans never run under IR; the caller recommits the
 *	  unchanged ROOT tokens under the serial set and pass 2 matches every
 *	  record against pass 1.  Participants are the founder's own sealed
 *	  generation (its restart input), every fenced origin and, from the
 *	  participant census, every other generation with retained WAL as
 *	  history.  Any refusal returns before the first mutation.
 *
 *	  Pass 2 runs in xlogrecovery.c; the per-block verdicts of the record
 *	  it applies are published here for XLogReadBufferForRedoExtended.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cold_recovery_startup.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *	  Spec: spec-s9p2-03-shared-wal-and-checkpoint.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/xlogreader.h"
#include "access/clog.h"
#include "access/multixact.h"
#include "access/xact.h"
#include "storage/standbydefs.h"
#include "catalog/storage_xlog.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_cold_recovery_census.h"
#include "cluster/cluster_side_online_plan.h"
#include "cluster/cluster_side_online_owner.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_undo_recovery.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_control_root.h"
#include "cluster/cluster_page_cold_redo.h"
#include "cluster/cluster_recovery_duty.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_wal_tail.h"
#include "cluster/cluster_wal_thread.h"
#include "storage/bufpage.h"
#include "storage/checksum.h"
#include "utils/memutils.h"

/* cluster.cold_recovery_plan_memory, kB; registered with the cluster GUCs. */
int cluster_cold_recovery_plan_memory = CLUSTER_COLD_PLAN_MEMORY_DEFAULT_KB;

#define COLD_REQUIRED_ROOT_FLAGS                                                                   \
	(CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID            \
	 | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID)

typedef struct ClusterColdReplayRootV1 {
	ClusterControlRootSnapshot root;
	ClusterControlRootReadToken token;
} ColdRoot;

static const char *
cold_detail_name(ClusterColdDetailV1 detail)
{
	static const char *const names[] = {
		"ok",
		"invalid argument",
		"participant invalid",
		"source gap",
		"component invalid",
		"edge branch",
		"edge cycle",
		"chain ambiguous",
		"ancestor missing",
		"incarnation mismatch",
		"history gap",
		"anchor missing",
		"observation failed",
		"deadlock",
		"capacity",
		"out of memory",
		"state",
		"lifecycle record unsupported",
		"opcode unsupported",
		"side owner missing",
		"page content unproven",
		"SPACE effects invalid",
		"SPACE identity missing",
		"SPACE effects refused",
	};

	return (int)detail >= 0 && (Size)detail < lengthof(names) ? names[detail] : "unknown";
}

static void cold_refuse(ClusterColdTypedV1 *typed, ClusterColdDetailV1 detail, const char *format,
						...) pg_attribute_printf(3, 4);

static void
cold_refuse(ClusterColdTypedV1 *typed, ClusterColdDetailV1 detail, const char *format, ...)
{
	va_list args;

	typed->refusal = detail;
	va_start(args, format);
	(void)pg_vsnprintf(typed->refusal_detail, sizeof(typed->refusal_detail), format, args);
	va_end(args);
	if (detail == CLUSTER_COLD_CAPACITY) {
		Size used = strlen(typed->refusal_detail);

		(void)snprintf(typed->refusal_detail + used, sizeof(typed->refusal_detail) - used,
					   " (cluster.cold_recovery_plan_memory is %d kB)",
					   cluster_cold_recovery_plan_memory);
	}
}

/* The founder's previous generation must be sealed like every peer. */
static bool
cold_own_root(ClusterColdTypedV1 *typed, uint16 own_thread, ColdRoot *out)
{
	ClusterControlRootIdentity identity;
	ClusterControlRootResult result;

	result = cluster_control_root_lookup_owner_by_node_runtime((int32)own_thread - 1, &identity,
															   &out->root, &out->token);
	if ((result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		 && result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		|| !cluster_recovery_duty_key_valid_v1(&identity) || identity.origin_thread_id != own_thread
		|| cluster_recovery_duty_key_compare(&identity, &out->root.identity)
			   != CLUSTER_RECOVERY_DUTY_COMPARE_EXACT) {
		cold_refuse(typed, CLUSTER_COLD_PARTICIPANT_INVALID,
					"own thread %u root is unreadable (result %d)", (unsigned)own_thread,
					(int)result);
		return false;
	}
	if (out->root.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
		|| (out->root.root_flags & COLD_REQUIRED_ROOT_FLAGS) != COLD_REQUIRED_ROOT_FLAGS) {
		cold_refuse(typed, CLUSTER_COLD_PARTICIPANT_INVALID,
					"own thread %u previous generation is not sealed for recovery "
					"(lifecycle %d, flags 0x%x)",
					(unsigned)own_thread, (int)out->root.lifecycle, out->root.root_flags);
		return false;
	}
	return true;
}

/* Exact source, native redo and cut of one sealed root, from its token. */
static bool
cold_participant(ClusterColdTypedV1 *typed, const ColdRoot *root, uint32 index)
{
	ClusterColdParticipantV1 *participant = &typed->participants[index];
	ClusterControlRootResult result;
	XLogRecPtr native_redo = InvalidXLogRecPtr;

	result = cluster_control_root_recovery_source_v1(&root->root, &root->token,
													 &typed->sources[index], &native_redo);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY || native_redo < root->root.checkpoint_lower_lsn
		|| native_redo > root->root.validated_tail_lsn_exclusive) {
		cold_refuse(typed, CLUSTER_COLD_PARTICIPANT_INVALID,
					"thread %u native checkpoint input is unproven (result %d)",
					(unsigned)root->root.identity.origin_thread_id, (int)result);
		return false;
	}
	memset(participant, 0, sizeof(*participant));
	participant->thread_id = root->root.identity.origin_thread_id;
	participant->timeline = root->root.checkpoint_tli;
	participant->owner_incarnation = root->root.identity.origin_owner_incarnation;
	participant->physical_lower = root->root.checkpoint_lower_lsn;
	participant->native_redo = native_redo;
	participant->tail_end = root->root.validated_tail_lsn_exclusive;
	return true;
}

/* The founder replays the generation it restarts from: the restart input
 * its bootstrap validated names the claim of its sealed root, and native
 * startup chose the same redo. */
static bool
cold_own_restart(ClusterColdTypedV1 *typed, uint16 own_thread, XLogRecPtr own_redo)
{
	const ClusterWalSourceRef *source = &typed->sources[typed->own_participant];
	XLogRecPtr native_redo = typed->participants[typed->own_participant].native_redo;
	ClusterWalSourceRef restart;

	if (!cluster_wal_thread_restart_v2_ref(&restart)
		|| memcmp(&restart.claim.identity, &source->claim.identity, sizeof(restart.claim.identity))
			   != 0
		|| memcmp(restart.claim.claim_sha256, source->claim.claim_sha256,
				  sizeof(restart.claim.claim_sha256))
			   != 0) {
		cold_refuse(typed, CLUSTER_COLD_PARTICIPANT_INVALID,
					"own thread %u sealed generation is not this node's restart input",
					(unsigned)own_thread);
		return false;
	}
	if (native_redo != own_redo) {
		cold_refuse(typed, CLUSTER_COLD_PARTICIPANT_INVALID,
					"own thread %u native redo %X/%X differs from the restart redo %X/%X",
					(unsigned)own_thread, LSN_FORMAT_ARGS(native_redo), LSN_FORMAT_ARGS(own_redo));
		return false;
	}
	return true;
}

static bool
cold_collect_roots(ClusterColdTypedV1 *typed, ClusterRecoveryFencePlan *fence, uint16 own_thread,
				   ColdRoot *roots)
{
	uint16 count = cluster_recovery_merge_fence_plan_origin_count(fence);
	uint16 i;

	if (count == 0 || (uint32)count + 1 > CLUSTER_COLD_MAX_PARTICIPANTS) {
		cold_refuse(typed, CLUSTER_COLD_PARTICIPANT_INVALID, "fence plan names %u origins",
					(unsigned)count);
		return false;
	}
	if (!cold_own_root(typed, own_thread, &roots[0]))
		return false;
	for (i = 0; i < count; i++) {
		uint16 thread = 0;

		if (!cluster_recovery_merge_fence_plan_origin(fence, i, &thread, &roots[i + 1].root,
													  &roots[i + 1].token)
			|| thread == own_thread) {
			cold_refuse(typed, CLUSTER_COLD_PARTICIPANT_INVALID, "fence plan origin %u is unusable",
						(unsigned)i);
			return false;
		}
	}
	typed->participant_count = (uint32)count + 1;
	typed->own_participant = 0;
	return true;
}

static void
cold_refuse_diag(ClusterColdTypedV1 *typed, ClusterColdDetailV1 detail, const char *stage,
				 const ClusterColdDiagV1 *diag)
{
	uint16 thread = diag->has_record && diag->participant < typed->participant_count
						? typed->participants[diag->participant].thread_id
						: 0;

	cold_refuse(
		typed, detail,
		"%s refused: %s; thread %u record %X/%X; page %u/%u/%u fork %u block %u; "
		"version token " UINT64_FORMAT "; waits for thread %u record %X/%X",
		stage, cold_detail_name(detail), (unsigned)thread,
		LSN_FORMAT_ARGS(diag->has_record ? diag->read_rec_ptr : InvalidXLogRecPtr),
		diag->has_page ? diag->page.locator.spcOid : 0,
		diag->has_page ? diag->page.locator.dbOid : 0,
		diag->has_page ? diag->page.locator.relNumber : 0, diag->has_page ? diag->page.forknum : 0,
		diag->has_page ? diag->page.blockno : 0, diag->version.mutation_token,
		diag->has_dependency && diag->dependency_participant < typed->participant_count
			? (unsigned)typed->participants[diag->dependency_participant].thread_id
			: 0,
		LSN_FORMAT_ARGS(diag->has_dependency ? diag->dependency_read_rec_ptr : InvalidXLogRecPtr));
}

/* SPACE effects are keyed by the namespace every participant must share:
 * the cluster's identity and the database incarnation of their claims. */
static bool
cold_space_namespace(ClusterColdTypedV1 *typed)
{
	const ClusterWalSourceRef *own = &typed->sources[typed->own_participant];
	ClusterColdObserverV1 *observer = &typed->observer;
	uint32 i;

	for (i = 0; i < typed->participant_count; i++) {
		const ClusterWalSourceRef *source = &typed->sources[i];
		bool other_storage
			= memcmp(source->claim.identity.storage_uuid, own->claim.identity.storage_uuid, 16)
			  != 0;

		if (source->claim.database_incarnation != own->claim.database_incarnation
			|| source->claim.identity.system_identifier != own->claim.identity.system_identifier
			|| other_storage) {
			cold_refuse(
				typed, CLUSTER_COLD_PARTICIPANT_INVALID,
				"thread %u claims system " UINT64_FORMAT ", database incarnation " UINT64_FORMAT
				"%s; thread %u claims system " UINT64_FORMAT ", database incarnation " UINT64_FORMAT
				"; generations share no namespace",
				(unsigned)typed->participants[i].thread_id,
				source->claim.identity.system_identifier, source->claim.database_incarnation,
				other_storage ? ", another storage uuid" : "",
				(unsigned)typed->participants[typed->own_participant].thread_id,
				own->claim.identity.system_identifier, own->claim.database_incarnation);
			return false;
		}
	}
	observer->system_identifier = own->claim.identity.system_identifier;
	observer->database_incarnation = own->claim.database_incarnation;
	memcpy(observer->storage_uuid, own->claim.identity.storage_uuid, 16);
	return true;
}

static const char *
cold_census_name(ClusterColdCensusDetailV1 detail)
{
	switch (detail) {
	case CLUSTER_COLD_CENSUS_LIVE_WRITER:
		return "a current generation is still OPEN";
	case CLUSTER_COLD_CENSUS_LIFECYCLE:
		return "unknown generation lifecycle";
	case CLUSTER_COLD_CENSUS_TERMINAL:
		return "initializer terminal with records";
	case CLUSTER_COLD_CENSUS_RANGE:
		return "retained range invalid";
	case CLUSTER_COLD_CENSUS_UNCOVERED:
		return "crashed generation missing from the fence plan";
	case CLUSTER_COLD_CENSUS_ORIGIN_MISSING:
		return "fence origin is not a crashed generation";
	case CLUSTER_COLD_CENSUS_CAPACITY:
		return "more generations than participants";
	default:
		return "invalid census input";
	}
}

/* The census's crashed generations are exactly the roots already taken, with
 * the same cut; append its history-only generations as participants. */
static bool
cold_census_take(ClusterColdTypedV1 *typed, const ColdRoot *roots,
				 const ClusterColdCensusEntryV1 *entries, uint32 count)
{
	uint32 crashed = typed->participant_count;
	uint32 i;
	uint32 k;

	for (i = 0; i < count; i++) {
		const ClusterColdCensusEntryV1 *entry = &entries[i];

		if (entry->role == CLUSTER_COLD_CENSUS_REPLAY) {
			for (k = 0; k < crashed; k++)
				if (cluster_control_root_identity_equal(&roots[k].root.identity, &entry->identity))
					break;
			if (k == crashed
				|| memcmp(&typed->participants[k], &entry->cut, sizeof(entry->cut)) != 0) {
				cold_refuse(typed, CLUSTER_COLD_PARTICIPANT_INVALID,
							"thread %u census cut differs from its ROOT source (native redo %X/%X)",
							(unsigned)entry->cut.thread_id,
							LSN_FORMAT_ARGS(entry->cut.native_redo));
				return false;
			}
			continue;
		}
		if (typed->participant_count >= CLUSTER_COLD_MAX_PARTICIPANTS) {
			cold_refuse(typed, CLUSTER_COLD_CAPACITY, "more than %d retained generations",
						CLUSTER_COLD_MAX_PARTICIPANTS);
			return false;
		}
		typed->participants[typed->participant_count] = entry->cut;
		typed->sources[typed->participant_count] = entry->source;
		typed->input_index[typed->participant_count] = entry->input_index;
		typed->participant_count++;
	}
	return true;
}

/* Complete participant census: every generation with retained WAL under one
 * cold read scope, held until pass 1 has scanned the history-only ones. */
static bool
cold_census(ClusterColdTypedV1 *typed, const ColdRoot *roots)
{
	const ClusterControlRootIdentity *own = &roots[0].root.identity;
	const ClusterWalInputV1 **inputs;
	ClusterColdCensusEntryV1 *entries;
	ClusterControlRootSnapshot *crashed;
	ClusterControlRootResult result;
	ClusterColdCensusDetailV1 detail;
	uint32 count;
	uint32 kept = 0;
	uint32 bad = 0;
	uint16 thread = 0;
	uint32 i;
	bool taken = false;

	typed->replay_count = typed->participant_count;
	result = cluster_wal_inputs_cold_begin_v1(own->storage_uuid, own->system_identifier,
											  &typed->inputs);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		cold_refuse(typed, CLUSTER_COLD_PARTICIPANT_INVALID,
					"participant census unavailable (root result %d)", (int)result);
		return false;
	}
	count = cluster_wal_inputs_count_v1(typed->inputs);
	inputs = (const ClusterWalInputV1 **)palloc0(sizeof(*inputs) * Max(count, 1));
	entries = (ClusterColdCensusEntryV1 *)palloc0(sizeof(*entries) * CLUSTER_COLD_MAX_PARTICIPANTS);
	crashed = (ClusterControlRootSnapshot *)palloc0(sizeof(*crashed) * typed->participant_count);
	for (i = 0; i < count; i++)
		inputs[i] = cluster_wal_inputs_at_v1(typed->inputs, i);
	for (i = 0; i < typed->participant_count; i++)
		crashed[i] = roots[i].root;
	detail = cluster_cold_census_select_v1(inputs, count, entries, CLUSTER_COLD_MAX_PARTICIPANTS,
										   &kept, &bad);
	if (detail == CLUSTER_COLD_CENSUS_OK)
		detail = cluster_cold_census_cover_v1(entries, kept, crashed, typed->participant_count,
											  &thread);
	if (detail != CLUSTER_COLD_CENSUS_OK)
		cold_refuse(typed, CLUSTER_COLD_PARTICIPANT_INVALID,
					"participant census refused: %s (input %u, thread %u)",
					cold_census_name(detail), bad, (unsigned)thread);
	else
		taken = cold_census_take(typed, roots, entries, kept);
	pfree(crashed);
	pfree(entries);
	pfree(inputs);
	return taken;
}

/* The history-only generations, under the census scope; then release it. */
static bool
cold_scan_history(ClusterColdTypedV1 *typed)
{
	ClusterControlRootResult result;
	uint32 i;

	for (i = typed->replay_count; i < typed->participant_count; i++) {
		ClusterColdScanResultV1 scan;
		ClusterColdDetailV1 detail = cluster_cold_scan_input_v1(
			typed->plan, i, typed->inputs, typed->input_index[i], false, true, &scan);

		typed->scanned_records += scan.records;
		if (detail != CLUSTER_COLD_OK) {
			cold_refuse(typed, detail,
						"pass-1 scan refused: %s; history thread %u record %X/%X (root result %d)",
						cold_detail_name(detail), (unsigned)typed->participants[i].thread_id,
						LSN_FORMAT_ARGS(scan.failed_read_rec_ptr), scan.root_result);
			return false;
		}
	}
	result = cluster_wal_inputs_revalidate_v1(typed->inputs);
	cluster_wal_inputs_release_v1(&typed->inputs);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP,
					"participant census changed while it was scanned (root result %d)",
					(int)result);
		return false;
	}
	return true;
}


static bool cold_sources_match(ClusterColdTypedV1 *typed, ClusterRecoveryFencePlan *fence);

/* Reserve a bounded part of the existing plan budget for original SIDE
 * payload ownership. The PAGE and SIDE budgets together never exceed it. */
static Size
cold_side_budget(const ClusterColdTypedV1 *typed)
{
	Size total = (Size)cluster_cold_recovery_plan_memory * 1024;
	return typed->replay_count == 0
			   ? 0
			   : Min((Size)RF_SIDE_ONLINE_PLAN_MAX_BYTES, total / 2 / typed->replay_count);
}

static void
cold_side_destroy(ClusterColdTypedV1 *typed)
{
	for (uint32 i = 0; i < CLUSTER_COLD_MAX_PARTICIPANTS; i++)
		rf_side_online_plan_destroy_v1(&typed->side_plans[i]);
}

static bool
cold_side_create(ClusterColdTypedV1 *typed)
{
	for (uint32 i = 0; i < typed->replay_count; i++) {
		const ClusterColdParticipantV1 *participant = &typed->participants[i];
		RfContributorStreamCutV1 cut = { 0 };
		RfSideOnlinePlanRequestV1 request = { 0 };
		RfPageProofDetailV1 detail;

		cut.failed_thread = participant->thread_id;
		cut.timeline_id = participant->timeline;
		cut.origin_owner_incarnation = participant->owner_incarnation;
		cut.scan_begin_inclusive = participant->physical_lower;
		cut.scan_end_exclusive = participant->tail_end;
		cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		if (cut.scan_begin_inclusive == cut.scan_end_exclusive)
			cut.flags |= RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY;
		request.system_identifier = typed->system_identifier;
		memcpy(request.storage_uuid, typed->observer.storage_uuid, 16);
		request.physical_cuts = &cut;
		request.participant_count = 1;
		request.redo_starts = &participant->native_redo;
		request.memory_budget = cold_side_budget(typed);
		request.purpose = RF_SIDE_PLAN_RECOVERY;
		if (request.memory_budget == 0)
			detail = RF_PAGE_PROOF_DETAIL_CAPACITY;
		else
			detail = rf_side_online_plan_create_v1(&request, &typed->side_plans[i]);
		/* The stream is local to this capture; a C0 target need not be.
		 * Preserve every original census claim, including history, before
		 * any feed. These copies do not replace the live input/target scope. */
		if (detail == RF_PAGE_PROOF_DETAIL_OK
			&& !rf_side_online_plan_bind_sources_v1(typed->side_plans[i], typed->sources,
													typed->participant_count))
			detail = RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
		if (detail != RF_PAGE_PROOF_DETAIL_OK) {
			cold_refuse(typed,
						detail == RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH
							? CLUSTER_COLD_PARTICIPANT_INVALID
							: CLUSTER_COLD_CAPACITY,
						"thread %u SIDE plan creation refused (detail %u)",
						(unsigned)participant->thread_id, (unsigned)detail);
			return false;
		}
	}
	return true;
}

static bool
cold_scan_and_seal(ClusterColdTypedV1 *typed, const ColdRoot *roots,
				   ClusterRecoveryFencePlan *fence)
{
	ClusterColdDiagV1 diag;
	ClusterColdDetailV1 detail;
	uint32 i;

	if (!cold_space_namespace(typed) || !cold_side_create(typed))
		return false;
	detail = cluster_cold_plan_set_space_check_v1(typed->plan, cluster_cold_space_check_v1,
												  &typed->observer);
	if (detail != CLUSTER_COLD_OK) {
		cold_refuse(typed, detail, "SPACE check refused: %s", cold_detail_name(detail));
		return false;
	}

	for (i = 0; i < typed->replay_count; i++) {
		ClusterColdScanResultV1 scan;

		detail
			= cluster_cold_scan_root_v1(typed->plan, i, &roots[i].root, &roots[i].token, false,
										i != typed->own_participant, typed->side_plans[i], &scan);
		typed->scanned_records += scan.records;
		if (detail != CLUSTER_COLD_OK) {
			cold_refuse(typed, detail,
						"pass-1 scan refused: %s; thread %u record %X/%X (root result %d, "
						"route detail %u)",
						cold_detail_name(detail), (unsigned)typed->participants[i].thread_id,
						LSN_FORMAT_ARGS(scan.failed_read_rec_ptr), scan.root_result,
						(unsigned)scan.route_detail);
			return false;
		}
		if (scan.side_capture_detail != 0) {
			typed->side_capture_detail[i] = scan.side_capture_detail;
			typed->side_capture_read_rec_ptr[i] = scan.side_capture_read_rec_ptr;
			rf_side_online_plan_destroy_v1(&typed->side_plans[i]);
		}
	}
	if (!cold_scan_history(typed))
		return false;
	detail = cluster_cold_plan_seal_v1(typed->plan, cluster_cold_observe_data_v1, &typed->observer,
									   &diag);
	if (detail != CLUSTER_COLD_OK) {
		cold_refuse_diag(typed, detail, "plan seal", &diag);
		return false;
	}
	/* No provisional payload plan survives a later source or ROOT failure. */
	if (!cold_sources_match(typed, fence)) {
		cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP, "SIDE capture source changed before seal");
		return false;
	}
	for (uint32 i = 0; i < typed->replay_count; i++) {
		RfPageProofDetailV1 side_detail;
		if (typed->side_plans[i] == NULL)
			continue;
		if (!rf_side_online_plan_bind_database_v1(typed->side_plans[i],
												  typed->observer.database_incarnation))
			side_detail = RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
		else
			side_detail = rf_side_online_plan_seal_v1(typed->side_plans[i]);
		if (side_detail == RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE
			|| side_detail == RF_PAGE_PROOF_DETAIL_OPCODE_UNSUPPORTED) {
			/* Original native/history coverage can exceed this execution
			 * profile. Keep its classification, never a partial SIDE seal. */
			typed->side_capture_detail[i] = (uint8)side_detail;
			rf_side_online_plan_destroy_v1(&typed->side_plans[i]);
		} else if (side_detail != RF_PAGE_PROOF_DETAIL_OK) {
			cold_refuse(typed, CLUSTER_COLD_SIDE_OWNER_MISSING,
						"thread %u SIDE seal refused (detail %u)",
						(unsigned)typed->participants[i].thread_id, (unsigned)side_detail);
			return false;
		}
	}
	if (!cold_sources_match(typed, fence)) {
		cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP, "SIDE capture source changed after seal");
		return false;
	}
	return true;
}

ClusterColdTypedV1 *
cluster_cold_typed_prepare_v1(ClusterRecoveryFencePlan *fence, uint16 own_thread,
							  XLogRecPtr own_redo)
{
	MemoryContext context;
	MemoryContext old_context;
	ClusterColdTypedV1 *typed;
	ColdRoot *roots;
	ClusterColdDetailV1 detail;

	context = AllocSetContextCreate(TopMemoryContext, "cluster cold replay plan",
									ALLOCSET_DEFAULT_SIZES);
	old_context = MemoryContextSwitchTo(context);
	typed = (ClusterColdTypedV1 *)palloc0(sizeof(*typed));
	typed->context = context;
	roots = (ColdRoot *)palloc0(sizeof(ColdRoot) * CLUSTER_COLD_MAX_PARTICIPANTS);
	typed->replay_roots = roots;
	if (!cold_collect_roots(typed, fence, own_thread, roots))
		goto done;
	typed->replay_root_count = typed->participant_count;
	for (uint32 i = 0; i < typed->participant_count; i++)
		if (!cold_participant(typed, &roots[i], i))
			goto done;
	if (!cold_own_restart(typed, own_thread, own_redo) || !cold_census(typed, roots))
		goto done;
	typed->system_identifier = roots[0].root.identity.system_identifier;
	detail = cluster_cold_plan_create_v1(typed->participants, typed->participant_count,
										 (Size)cluster_cold_recovery_plan_memory * 1024
											 - cold_side_budget(typed) * typed->replay_count,
										 &typed->plan);
	if (detail != CLUSTER_COLD_OK) {
		cold_refuse(typed, detail, "plan creation refused: %s", cold_detail_name(detail));
		goto done;
	}
	if (cold_scan_and_seal(typed, roots, fence))
		typed->refusal = CLUSTER_COLD_OK;

done:
	if (typed->refusal != CLUSTER_COLD_OK)
		cold_side_destroy(typed);
	MemoryContextSwitchTo(old_context);
	return typed;
}

void
cluster_cold_typed_destroy_v1(ClusterColdTypedV1 **typed_address)
{
	ClusterColdTypedV1 *typed;

	if (typed_address == NULL || *typed_address == NULL)
		return;
	typed = *typed_address;
	*typed_address = NULL;
	if (typed->inputs != NULL)
		cluster_wal_inputs_release_v1(&typed->inputs);
	cold_side_destroy(typed);
	if (typed->plan != NULL)
		cluster_cold_plan_destroy_v1(&typed->plan);
	if (typed->replay_roots != NULL)
		pfree(typed->replay_roots);
	MemoryContextDelete(typed->context);
}

/* Re-select through each original ROOT token, never through a newer root. */
static bool
cold_sources_match(ClusterColdTypedV1 *typed, ClusterRecoveryFencePlan *fence)
{
	if (fence == NULL
		|| (uint32)cluster_recovery_merge_fence_plan_origin_count(fence) + 1 != typed->replay_count)
		return false;
	for (uint32 i = 0; i < typed->replay_count; i++) {
		const ColdRoot *selected = &typed->replay_roots[i];
		const ClusterWalSourceRef *expected = &typed->sources[i];
		const ClusterColdParticipantV1 *participant = &typed->participants[i];
		ClusterWalSourceRef source;
		XLogRecPtr redo;

		if (i != typed->own_participant) {
			ColdRoot origin;
			uint16 thread;

			/* prepare() keeps the founder first, then the fence-plan order. */
			if (!cluster_recovery_merge_fence_plan_origin(fence, i - 1, &thread, &origin.root,
														  &origin.token)
				|| thread != participant->thread_id
				|| memcmp(&origin.root, &selected->root, sizeof(origin.root)) != 0
				|| memcmp(&origin.token, &selected->token, sizeof(origin.token)) != 0)
				return false;
		}
		if (cluster_control_root_recovery_source_v1(&selected->root, &selected->token, &source,
													&redo)
				!= CLUSTER_CONTROL_ROOT_OK_PRIMARY
			|| !cluster_control_root_identity_equal(&source.claim.identity,
													&expected->claim.identity)
			|| source.claim.database_incarnation != expected->claim.database_incarnation
			|| source.claim.max_config_generation != expected->claim.max_config_generation
			|| memcmp(source.claim.claim_sha256, expected->claim.claim_sha256, 32) != 0
			|| source.timeline != expected->timeline
			|| participant->thread_id != selected->root.identity.origin_thread_id
			|| participant->owner_incarnation != selected->root.identity.origin_owner_incarnation
			|| participant->timeline != selected->root.checkpoint_tli
			|| participant->physical_lower != selected->root.checkpoint_lower_lsn
			|| participant->tail_end != selected->root.validated_tail_lsn_exclusive
			|| participant->native_redo != redo)
			return false;
	}
	return true;
}

/* Pass 1 has not acquired the serial set. Execution revalidation belongs
 * only to pass 2, around the same complete original-source comparison. */
static bool
cold_replay_sources_current(ClusterColdTypedV1 *typed, ClusterRecoveryFencePlan *fence)
{
	return cluster_recovery_merge_fence_plan_revalidate_nowait(fence)
		   && cold_sources_match(typed, fence)
		   && cluster_recovery_merge_fence_plan_revalidate_nowait(fence);
}

bool
cluster_cold_typed_sync_sources_v1(ClusterColdTypedV1 *typed, ClusterRecoveryFencePlan *fence)
{
	if (typed == NULL || typed->refusal != CLUSTER_COLD_OK)
		return false;
	/* ERROR from an original source owner must not leave a reusable OK plan. */
	cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP, "pass-2 source durability is incomplete");
	memset(typed->source_drained, 0, sizeof(typed->source_drained));
	if (fence == NULL || !cluster_cold_replay_window_active_v1() || typed->plan == NULL
		|| typed->replay_roots == NULL || typed->own_participant != 0 || typed->replay_count == 0
		|| typed->replay_count > typed->participant_count
		|| typed->participant_count > CLUSTER_COLD_MAX_PARTICIPANTS
		|| typed->replay_root_count != typed->replay_count
		|| cluster_cold_plan_participant_count_v1(typed->plan) != typed->participant_count
		|| !cold_replay_sources_current(typed, fence))
		return false;

	/* The original source owner releases CF before syncing WAL/claim/dirs,
	 * then rechecks its complete cut. No target consumer has run yet. */
	for (uint32 i = 0; i < typed->replay_count; i++) {
		const ColdRoot *selected = &typed->replay_roots[i];
		ClusterWalTailObservation observed;
		ClusterControlRootResult result;

		result
			= cluster_control_root_recovery_sync_v1(&selected->root, &selected->token, &observed);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
			|| observed.complete_end != selected->root.validated_tail_lsn_exclusive
			|| observed.last_record_start != selected->root.tail_last_record_lsn
			|| observed.last_record_crc != selected->root.tail_last_record_crc32c
			|| observed.database_incarnation != typed->sources[i].claim.database_incarnation
			|| !cluster_recovery_merge_fence_plan_revalidate_nowait(fence)) {
			cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP,
						"thread %u pass-2 source sync refused (root result %d)",
						(unsigned)typed->participants[i].thread_id, (int)result);
			return false;
		}
	}
	/* A later source's IO must not hide earlier source or scope drift. */
	if (!cold_replay_sources_current(typed, fence))
		return false;
	typed->refusal = CLUSTER_COLD_OK;
	typed->refusal_detail[0] = '\0';
	return true;
}

/* Only the original sequencer calls this after its counted drain. Keep
 * the captured ROOT's exact tail proof; EOF or a source number is not one. */
bool
cluster_cold_typed_source_drained_v1(ClusterColdTypedV1 *typed, ClusterRecoveryFencePlan *fence,
									 uint32 participant, const ClusterColdReplayResultV1 *progress,
									 uint64 records)
{
	if (typed == NULL || typed->refusal != CLUSTER_COLD_OK)
		return false;
	cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP, "pass-2 source drain is incomplete");
	if (fence == NULL || !cluster_cold_replay_window_active_v1() || typed->plan == NULL
		|| typed->replay_roots == NULL || typed->own_participant != 0 || typed->replay_count == 0
		|| typed->replay_count > typed->participant_count
		|| typed->participant_count > CLUSTER_COLD_MAX_PARTICIPANTS
		|| typed->replay_root_count != typed->replay_count
		|| cluster_cold_plan_participant_count_v1(typed->plan) != typed->participant_count
		|| participant >= typed->participant_count || progress == NULL
		|| progress->detail != CLUSTER_COLD_REPLAY_OK
		|| progress->steps_done != cluster_cold_plan_step_count_v1(typed->plan)
		|| records != cluster_cold_plan_replay_record_count_v1(typed->plan, participant)
		|| !cold_replay_sources_current(typed, fence))
		return false;
	/* Founder/history/empty inputs do not acquire target mutation or base
	 * coverage here. Their existing final completion owners remain required. */
	if (participant < typed->replay_count && participant != typed->own_participant
		&& records != 0) {
		if (!cluster_cold_completion_proven_v1(typed->plan, &typed->replay_roots[participant].root,
											   progress, participant))
			return false;
		typed->source_drained[participant] = true;
	}
	typed->refusal = CLUSTER_COLD_OK;
	typed->refusal_detail[0] = '\0';
	return true;
}

ClusterColdRouteV1
cluster_cold_route_v1(bool shared_config, bool merge_engaged)
{
	if (!merge_engaged)
		return CLUSTER_COLD_ROUTE_NATIVE;
	return shared_config ? CLUSTER_COLD_ROUTE_TYPED : CLUSTER_COLD_ROUTE_REFUSE;
}

static bool
cold_control_authority_current(void *arg)
{
	return cluster_thread_recovery_authority_revalidate_nowait_v1(
			   (const ClusterThreadRecoveryAuthorityV1 *)arg)
		   == CLUSTER_THREAD_AUTHORITY_OK;
}

typedef struct ColdControlSpace {
	ClusterColdTypedV1 *typed;
	ClusterRecoveryFencePlan *fence;
	uint32 participant;
} ColdControlSpace;

/* Borrow the original sealed, whole-cohort SPACE proof, not a partial
 * per-source reconstruction. Its original ordered owner still revalidates
 * physical bytes and performs all installs at the scheduled SPACE step. */
static bool
cold_control_space_input(const ColdControlSpace *context, const RfSideOnlineOperationV1 *operation)
{
	const ClusterColdTypedV1 *typed = context->typed;
	uint32 relations = cluster_cold_plan_space_relation_count_v1(typed->plan);

	if (operation->kind != RF_SIDE_ONLINE_OPERATION_SPACE || operation->history_only
		|| operation->purpose != RF_SIDE_PLAN_RECOVERY || operation->owned_payload == NULL
		|| operation->owned_payload_length == 0
		|| operation->space_key.system_identifier != typed->observer.system_identifier
		|| operation->space_key.database_incarnation != typed->observer.database_incarnation
		|| memcmp(operation->space_key.storage_uuid, typed->observer.storage_uuid, 16) != 0)
		return false;
	for (uint32 r = 0; r < relations; r++) {
		RelFileLocator locator;
		uint32 count;
		if (!cluster_cold_plan_space_relation_v1(typed->plan, r, &locator, &count))
			return false;
		if (!RelFileLocatorEquals(locator, operation->space_key.locator))
			continue;
		for (uint32 i = 0; i < count; i++) {
			ClusterColdSpaceInputV1 input;
			if (!cluster_cold_plan_space_input_v1(typed->plan, r, i, &input))
				return false;
			if (input.participant == context->participant
				&& input.read_rec_ptr == operation->identity.record.read_rec_ptr
				&& input.end_rec_ptr == operation->identity.record.end_rec_ptr
				&& input.payload_length == operation->owned_payload_length && input.payload != NULL
				&& memcmp(input.payload, operation->owned_payload, input.payload_length) == 0)
				return true;
		}
		return false;
	}
	return false;
}

static bool
cold_control_space_preflight(void *arg, const RfSideOnlineOperationV1 *operation)
{
	ColdControlSpace *context = arg;
	const RfSideOnlinePlanV1 *plan;
	bool matched = false;

	if (context == NULL || operation == NULL || operation->history_only
		|| operation->purpose != RF_SIDE_PLAN_RECOVERY || operation->identity.participant_index != 0
		|| operation->identity.record.origin_thread
			   != context->typed->participants[context->participant].thread_id
		|| !cold_replay_sources_current(context->typed, context->fence))
		return false;
	plan = context->typed->side_plans[context->participant];
	if (operation->kind == RF_SIDE_ONLINE_OPERATION_SPACE)
		matched = cold_control_space_input(context, operation);
	else if (operation->kind == RF_SIDE_ONLINE_OPERATION_NATIVE_STRUCTURE) {
		uint32 count = rf_side_online_plan_operation_count_v1(plan);
		for (uint32 i = 0; i < count; i++) {
			RfSideOnlineOperationV1 candidate;
			ClusterSpaceStructureChange creation;
			if (!rf_side_online_plan_operation_v1(plan, i, &candidate))
				return false;
			if (memcmp(&candidate.identity, &operation->identity, sizeof(candidate.identity)) != 0)
				continue;
			/*
			 * The existing decoder proves the complete native CREATE and
			 * its first same-source/xid typed CREATE pair.
			 */
			if (!rf_side_online_plan_native_creation_v1(plan, i, &creation))
				return false;
			for (uint32 j = i + 1; j < count; j++) {
				if (!rf_side_online_plan_operation_v1(plan, j, &candidate))
					return false;
				if (candidate.identity.participant_index != operation->identity.participant_index
					|| candidate.kind != RF_SIDE_ONLINE_OPERATION_SPACE
					|| candidate.route.normalized_info != XLOG_SMGR_SPACE_IDENTITY
					|| !RelFileLocatorEquals(candidate.space_key.locator,
											 creation.identity.result.key.locator))
					continue;
				matched = cold_control_space_input(context, &candidate);
				break;
			}
			break;
		}
	}
	return matched && cold_replay_sources_current(context->typed, context->fence);
}

static bool
cold_control_space_not_executed(void *arg pg_attribute_unused(),
								const RfSideOnlineOperationV1 *operation pg_attribute_unused())
{
	/* A control callback must never execute or mark a later SPACE effect. */
	return false;
}

/* Match the original immutable SIDE operation kind as well as its full
 * WAL identity. No XACT/UNDO whole-source preparation is dispatched here. */
static RfSideOnlineOperationKindV1
cold_record_side_kind(const ClusterColdRecordV1 *record)
{
	uint8 info = record->rmid == RM_XACT_ID ? record->info & XLOG_XACT_OPMASK
										   : record->info & ~XLR_INFO_MASK;

	if (record->record_flags == CLUSTER_COLD_RECORD_FOREIGN_CONTROL)
		return RF_SIDE_ONLINE_OPERATION_NATIVE_CONTROL;
	if (record->record_flags != CLUSTER_COLD_RECORD_FOREIGN_SIDE
		|| ((record->info & XLR_INFO_MASK) != 0
			&& !(record->rmid == RM_XACT_ID && info == XLOG_XACT_COMMIT && record->space_count > 0
				 && (record->info & XLR_INFO_MASK) == XLR_SPECIAL_REL_UPDATE)))
		return RF_SIDE_ONLINE_OPERATION_INVALID;
	if (record->rmid == RM_CLUSTER_UNDO_ID && info == XLOG_UNDO_RECOVERY_TT_ABORT)
		return RF_SIDE_ONLINE_OPERATION_RECOVERY_TT_ABORT;
	if (record->rmid == RM_CLUSTER_UNDO_ID
		&& (info == XLOG_UNDO_BLOCK_WRITE || info == XLOG_UNDO_BLOCK_WRITE_MULTI
			|| info == XLOG_UNDO_SEGMENT_INIT || info == XLOG_UNDO_SEGMENT_REUSE
			|| info == XLOG_UNDO_SEGMENT_RECYCLE || info == XLOG_UNDO_TT_SLOT_BIND
			|| info == XLOG_UNDO_TT_SLOT_SET_HEAD || info == XLOG_UNDO_TT_SLOT_ABORT
			|| info == XLOG_UNDO_TT_SLOT_CTRC_RELEASE))
		return RF_SIDE_ONLINE_OPERATION_UNDO;
	/* Ordered native XACT completion is consumed by the existing cold owner;
	 * prepared/2PC variants stay outside this route and fail closed. */
	if (record->rmid == RM_XACT_ID
		&& (info == XLOG_XACT_COMMIT || info == XLOG_XACT_ABORT))
		return RF_SIDE_ONLINE_OPERATION_XACT;
	if ((record->rmid == RM_CLOG_ID && (info == CLOG_ZEROPAGE || info == CLOG_TRUNCATE))
		|| (record->rmid == RM_MULTIXACT_ID
			&& (info == XLOG_MULTIXACT_ZERO_OFF_PAGE || info == XLOG_MULTIXACT_ZERO_MEM_PAGE
				|| info == XLOG_MULTIXACT_CREATE_ID || info == XLOG_MULTIXACT_TRUNCATE_ID)))
		return RF_SIDE_ONLINE_OPERATION_PROJECTION;
	if ((record->rmid == RM_XACT_ID && info == XLOG_XACT_ASSIGNMENT)
		|| (record->rmid == RM_STANDBY_ID && info == XLOG_INVALIDATIONS))
		return RF_SIDE_ONLINE_OPERATION_NATIVE_CONTROL;
	return RF_SIDE_ONLINE_OPERATION_INVALID;
}

/* A target's current authority is independent of its SIDE capture. */
static bool
cold_participant_authority(ClusterColdTypedV1 *typed, ClusterRecoveryFencePlan *fence,
						   uint32 participant, ClusterThreadRecoveryAuthorityV1 *authority)
{
	const ClusterColdParticipantV1 *source;
	const ColdRoot *selected;
	if (fence == NULL || !cluster_cold_replay_window_active_v1() || typed->plan == NULL
		|| typed->replay_roots == NULL || typed->own_participant != 0 || typed->replay_count == 0
		|| typed->replay_count > typed->participant_count
		|| typed->participant_count > CLUSTER_COLD_MAX_PARTICIPANTS
		|| typed->replay_root_count != typed->replay_count || participant >= typed->replay_count
		|| participant == typed->own_participant || !cold_replay_sources_current(typed, fence))
		return false;
	source = &typed->participants[participant];
	selected = &typed->replay_roots[participant];
	return cluster_recovery_merge_fence_plan_authority(fence, source->thread_id, authority)
		   && authority->duty != NULL && authority->root_snapshot != NULL
		   && authority->root_token != NULL && authority->serial_guard != NULL
		   && authority->serial_guard->mode == CLUSTER_RECOVERY_SERIAL_COLD_FORMED
		   && memcmp(authority->root_snapshot, &selected->root, sizeof(selected->root)) == 0
		   && memcmp(authority->root_token, &selected->token, sizeof(selected->token)) == 0
		   && cold_control_authority_current(authority)
		   && cluster_thread_recovery_authority_covers_window_v1(
			   authority, source->thread_id, source->physical_lower, source->tail_end);
}

/* Rebind the same immutable whole-source authority for record execution
 * and for the sequencer's post-drain physical durability check. */
static bool
cold_side_source_authority(ClusterColdTypedV1 *typed, ClusterRecoveryFencePlan *fence,
						   uint32 participant, ClusterThreadRecoveryAuthorityV1 *authority)
{
	const ClusterColdParticipantV1 *source;
	RfContributorStreamCutV1 cut = { 0 };
	if (participant >= CLUSTER_COLD_MAX_PARTICIPANTS || typed->side_capture_detail[participant] != 0
		|| typed->side_plans[participant] == NULL
		|| !cold_participant_authority(typed, fence, participant, authority))
		return false;
	source = &typed->participants[participant];
	cut.failed_thread = source->thread_id;
	cut.timeline_id = source->timeline;
	cut.origin_owner_incarnation = source->owner_incarnation;
	cut.scan_begin_inclusive = source->physical_lower;
	cut.scan_end_exclusive = source->tail_end;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	return rf_side_online_plan_source_matches_v1(typed->side_plans[participant],
												 typed->system_identifier,
												 typed->observer.storage_uuid, &cut)
		   && rf_side_online_plan_replay_start_matches_v1(
			   typed->side_plans[participant], source->thread_id, source->owner_incarnation,
			   typed->observer.database_incarnation, source->native_redo);
}

static bool
cold_source_ref_matches(const ClusterWalSourceRef *a, const ClusterWalSourceRef *b)
{
	return cluster_control_root_identity_equal(&a->claim.identity, &b->claim.identity)
		   && a->claim.database_incarnation == b->claim.database_incarnation
		   && a->claim.max_config_generation == b->claim.max_config_generation
		   && memcmp(a->claim.claim_sha256, b->claim.claim_sha256, 32) == 0
		   && a->timeline == b->timeline;
}

static bool
cold_recovery_abort_record(ClusterColdTypedV1 *typed, ClusterRecoveryFencePlan *fence,
						   uint32 participant, uint32 index,
						   const RfPageOnlineRecordIdentityV1 *identity,
						   const ClusterThreadRecoveryAuthorityV1 *writer)
{
	const RfSideOnlinePlanV1 *plan = typed->side_plans[participant];
	ClusterWalSourceRef bound_writer, bound_target;
	ClusterThreadRecoveryAuthorityV1 target;
	uint32 match = UINT32_MAX;

	if (!rf_side_online_plan_recovery_abort_sources_v1(plan, index, &bound_writer, &bound_target)
		|| !cold_source_ref_matches(&bound_writer, &typed->sources[participant]))
		return false;
	for (uint32 i = 0; i < typed->participant_count; i++) {
		if (!cold_source_ref_matches(&bound_target, &typed->sources[i]))
			continue;
		if (match != UINT32_MAX)
			return false;
		match = i;
	}
	/* A history claim or own restart input is not a borrowed current
	 * mutation authority. Do not substitute a newer generation of the thread. */
	/* Same-source C0 follows its original stream prefix. A different source
	 * must have consumed its complete captured cut first; never compare LSNs
	 * from different streams or treat the writer checkpoint as target cover. */
	return match != UINT32_MAX && (match == participant || typed->source_drained[match])
		   && cold_participant_authority(typed, fence, match, &target)
		   && cluster_undo_recovery_abort_replay_v1(plan, index, identity, writer, &target)
		   && cold_control_authority_current(&target);
}

/* A compound dispatch must retain every originally decoded SPACE payload.
 * No partial array, newly reconstructed COMMIT, or bare physical absence
 * can substitute for the selected full captured operation. */
static bool
cold_record_drops_match(const ClusterColdRecordV1 *record, const RfSideOnlineOperationV1 *operation)
{
	uint32 offset = operation->xact.space_drop_offset;
	if (record->space_count == 0)
		return operation->kind != RF_SIDE_ONLINE_OPERATION_XACT
			   || (operation->xact.space_drop_count == 0 && operation->xact.relation_count == 0);
	if (operation->kind != RF_SIDE_ONLINE_OPERATION_XACT
		|| operation->xact.kind != RF_SIDE_XACT_COMMIT || !operation->xact.cold_completion_effects
		|| record->space_ops == NULL || operation->xact.space_drop_count != record->space_count
		|| operation->xact.relation_count != record->space_count || operation->owned_payload == NULL
		|| offset > operation->owned_payload_length
		|| record->space_count
			   > (operation->owned_payload_length - offset) / CLUSTER_SPACE_STRUCTURE_WAL_BYTES)
		return false;
	for (uint32 i = 0; i < record->space_count; i++) {
		const ClusterColdSpaceOpV1 *op = &record->space_ops[i];
		if (op->kind != CLUSTER_COLD_SPACE_DROP || op->payload == NULL
			|| op->payload_length != CLUSTER_SPACE_STRUCTURE_WAL_BYTES
			|| memcmp(op->payload, operation->owned_payload + offset, op->payload_length) != 0)
			return false;
		offset += CLUSTER_SPACE_STRUCTURE_WAL_BYTES;
	}
	return true;
}

/* The original sequencer owns record order and final completion. Never
 * dispatch the whole-source plan before intervening PAGE/SPACE records. */
bool
cluster_cold_typed_side_record_v1(ClusterColdTypedV1 *typed, ClusterRecoveryFencePlan *fence,
								  uint32 participant, const ClusterColdRecordV1 *record)
{
	const ClusterColdParticipantV1 *source;
	const RfSideOnlinePlanV1 *plan;
	RfPageOnlineRecordIdentityV1 identity = { 0 };
	ClusterThreadRecoveryAuthorityV1 authority;
	RfSideOnlineProductionOwnerV1 owner;
	ColdControlSpace space = { typed, fence, participant };
	RfSideOnlineOperationKindV1 kind;
	uint32 match = UINT32_MAX;
	uint32 count;
	uint64 epoch;

	if (typed == NULL || typed->refusal != CLUSTER_COLD_OK)
		return false;
	/*
	 * ERROR from an original owner must not leave a reusable prepared-success
	 * state. No field is restored on an exceptional exit.
	 */
	cold_refuse(typed, CLUSTER_COLD_SIDE_OWNER_MISSING,
				"pass-2 foreign SIDE record consumption is incomplete");
	if (record == NULL || (kind = cold_record_side_kind(record)) == RF_SIDE_ONLINE_OPERATION_INVALID
		|| record->component_count != 0
		|| !cold_side_source_authority(typed, fence, participant, &authority))
		return false;
	source = &typed->participants[participant];
	plan = typed->side_plans[participant];
	if (record->read_rec_ptr < source->native_redo || record->end_rec_ptr <= record->read_rec_ptr
		|| record->end_rec_ptr > source->tail_end)
		return false;
	identity.record.system_identifier = typed->system_identifier;
	memcpy(identity.record.storage_uuid, typed->observer.storage_uuid, 16);
	identity.record.origin_thread = source->thread_id;
	identity.record.timeline_id = source->timeline;
	identity.record.read_rec_ptr = record->read_rec_ptr;
	identity.record.end_rec_ptr = record->end_rec_ptr;
	identity.record.record_crc = record->record_crc;
	identity.record.rmid = record->rmid;
	identity.record.info = record->info;
	count = rf_side_online_plan_operation_count_v1(plan);
	for (uint32 i = 0; i < count; i++) {
		RfSideOnlineOperationV1 operation;
		if (!rf_side_online_plan_operation_v1(plan, i, &operation))
			return false;
		if (operation.identity.record.read_rec_ptr != record->read_rec_ptr)
			continue;
		if (match != UINT32_MAX || operation.history_only || operation.kind != kind
			|| memcmp(&operation.identity, &identity, sizeof(identity)) != 0
			|| !cold_record_drops_match(record, &operation))
			return false;
		/* Keep the decoder's exact terminal carrier boundary at dispatch;
		 * an older ABORT without segment generation is not this consumer. */
		if (record->rmid == RM_CLUSTER_UNDO_ID && record->info == XLOG_UNDO_TT_SLOT_ABORT
			&& (operation.undo.kind != CLUSTER_UNDO_KIND_TT_ABORT
				|| operation.undo.format_version != CLUSTER_UNDO_TT_ABORT_EXACT_VERSION))
			return false;
		if (record->rmid == RM_CLUSTER_UNDO_ID && record->info == XLOG_UNDO_TT_SLOT_CTRC_RELEASE
			&& (operation.undo.kind != CLUSTER_UNDO_KIND_TT_CTRC_RELEASE
				|| operation.undo.format_version != CLUSTER_UNDO_TT_CTRC_RELEASE_VERSION))
			return false;
		match = i;
	}
	epoch = cluster_epoch_get_current();
	if (match == UINT32_MAX || epoch == 0 || epoch > UINT32_MAX)
		return false;
	if (kind == RF_SIDE_ONLINE_OPERATION_RECOVERY_TT_ABORT) {
		if (!cold_recovery_abort_record(typed, fence, participant, match, &identity, &authority))
			return false;
	} else {
		if (!rf_side_online_production_owner_init_v1(
				&owner, &authority, cold_control_authority_current, (uint32)epoch, true)
			|| !rf_side_online_production_bind_undo_v1(&owner, &authority))
			return false;
		owner.space_arg = &space;
		owner.preflight_space = cold_control_space_preflight;
		owner.apply_space = cold_control_space_not_executed;
		if (rf_side_online_production_apply_record_v1(plan, match, &identity, &owner)
			!= RF_PAGE_PROOF_DETAIL_OK)
			return false;
	}
	if (cluster_epoch_get_current() != epoch || !cold_control_authority_current(&authority)
		|| !cold_replay_sources_current(typed, fence))
		return false;
	typed->refusal = CLUSTER_COLD_OK;
	typed->refusal_detail[0] = '\0';
	return true;
}

/* Called only after the original sequencer executed every record and drained
 * every source. This checks durable deletion, never replays absent SPACE. */
bool
cluster_cold_typed_drop_final_v1(ClusterColdTypedV1 *typed, ClusterRecoveryFencePlan *fence,
								 uint32 relation)
{
	RelFileLocator locator;
	uint32 count = 0;
	ClusterColdSpaceInputV1 last;
	const ClusterColdParticipantV1 *participant;
	ClusterThreadRecoveryAuthorityV1 authority;
	ClusterUndoRecoveryScopeV1 scope;
	ClusterSpaceIdentityKey key = { 0 };
	ClusterSpaceRecoveryInput input;
	ClusterSpaceColdSourceV1 source;
	uint64 epoch;
	bool ok = false;

	if (typed == NULL || typed->refusal != CLUSTER_COLD_OK)
		return false;
	cold_refuse(typed, CLUSTER_COLD_SIDE_OWNER_MISSING,
				"pass-2 foreign DROP durability is incomplete");
	if (typed->plan == NULL
		|| !cluster_cold_plan_space_relation_v1(typed->plan, relation, &locator, &count)
		|| count == 0 || !cluster_cold_plan_space_input_v1(typed->plan, relation, count - 1, &last)
		|| last.kind != CLUSTER_COLD_SPACE_DROP
		|| !cold_side_source_authority(typed, fence, last.participant, &authority))
		return false;
	participant = &typed->participants[last.participant];
	epoch = cluster_epoch_get_current();
	if (epoch == 0 || epoch > UINT32_MAX || XLogRecPtrIsInvalid(last.read_rec_ptr)
		|| last.read_rec_ptr < participant->native_redo || last.end_rec_ptr <= last.read_rec_ptr
		|| last.end_rec_ptr > participant->tail_end)
		return false;
	key.system_identifier = typed->observer.system_identifier;
	key.database_incarnation = typed->observer.database_incarnation;
	memcpy(key.storage_uuid, typed->observer.storage_uuid, sizeof(key.storage_uuid));
	key.locator = locator;
	input.data = last.payload;
	input.length = last.payload_length;
	source.origin_thread = participant->thread_id;
	source.end_rec_ptr = last.end_rec_ptr;
	if (!cluster_undo_recovery_scope_enter_v1(&scope, &authority,
											  typed->side_plans[last.participant]))
		return false;
	PG_TRY();
	{
		ok = cluster_space_cold_drop_foreign_sync_v1(&key, &input, &source)
			 && cluster_epoch_get_current() == epoch && cold_control_authority_current(&authority)
			 && cold_replay_sources_current(typed, fence);
	}
	PG_FINALLY();
	{
		cluster_undo_recovery_scope_leave_v1(&scope);
	}
	PG_END_TRY();
	if (!ok)
		return false;
	typed->refusal = CLUSTER_COLD_OK;
	typed->refusal_detail[0] = '\0';
	return true;
}

static void
cold_reason_append(char *reason, Size reason_size, const char *text)
{
	Size used = strlen(reason);

	(void)snprintf(reason + used, reason_size - used, "%s%s", used > 0 ? "; " : "", text);
}

bool
cluster_cold_typed_ready_v1(const ClusterColdTypedV1 *typed,
							const ClusterColdHandshakeV1 *handshake, char *reason, Size reason_size)
{
	uint32 steps;
	uint32 applied = 0;
	uint32 i;

	if (reason == NULL || reason_size == 0)
		return false;
	reason[0] = '\0';
	if (typed == NULL || handshake == NULL || typed->refusal != CLUSTER_COLD_OK
		|| typed->plan == NULL) {
		cold_reason_append(reason, reason_size, "no sealed typed cold plan");
		return false;
	}
	if (!handshake->redo_block_hook)
		cold_reason_append(reason, reason_size, "no typed redo-block consultation (R-A2)");
	if (!handshake->participant_census)
		cold_reason_append(reason, reason_size, "no complete participant census (R-A4)");
	if (!handshake->side_owners)
		cold_reason_append(reason, reason_size,
						   "no typed side owners or XID/OID/MX/SCN bound merge (R-A5)");
	for (i = 0; i < typed->replay_count; i++) {
		if (typed->side_capture_detail[i] != 0 || typed->side_plans[i] == NULL) {
			char text[128];
			(void)snprintf(text, sizeof(text),
						   "thread %u has no complete SIDE capture (detail %u, record %X/%X)",
						   (unsigned)typed->participants[i].thread_id,
						   (unsigned)typed->side_capture_detail[i],
						   LSN_FORMAT_ARGS(typed->side_capture_read_rec_ptr[i]));
			cold_reason_append(reason, reason_size, text);
		}
	}
	if (!handshake->completion_publish)
		cold_reason_append(reason, reason_size, "no recovery completion publication (R-A7)");
	if (!handshake->restartpoint_hold)
		cold_reason_append(reason, reason_size,
						   "no restartpoint hold while the cold cut is replayed (R-A9)");
	if (cluster_cold_plan_space_relation_count_v1(typed->plan) > 0 && !handshake->space_owner) {
		char text[96];

		(void)snprintf(text, sizeof(text), "%u SPACE relations need the cold SPACE owner (R-A11)",
					   cluster_cold_plan_space_relation_count_v1(typed->plan));
		cold_reason_append(reason, reason_size, text);
	}
	steps = cluster_cold_plan_step_count_v1(typed->plan);
	for (i = 0; i < steps; i++) {
		ClusterColdStepV1 step;

		if (!cluster_cold_plan_step_v1(typed->plan, i, &step)) {
			cold_reason_append(reason, reason_size, "unusable plan step");
			return false;
		}
		if (!step.all_skip)
			applied++;
	}
	if (applied > 0 && !handshake->redo_block_hook) {
		char text[96];

		(void)snprintf(text, sizeof(text), "%u page records need the redo-block consultation",
					   applied);
		cold_reason_append(reason, reason_size, text);
	}
	return reason[0] == '\0';
}

static bool cold_replay_window = false;

void
cluster_cold_replay_window_enter_v1(void)
{
	cold_replay_window = true;
}

void
cluster_cold_replay_window_leave_v1(void)
{
	cold_replay_window = false;
}

bool
cluster_cold_replay_window_active_v1(void)
{
	return cold_replay_window;
}

/* Pass-2 verdicts of the one record being applied, startup process only. */
static struct {
	bool active;
	XLogRecPtr read_rec_ptr;
	ClusterColdStepV1 step;
} cold_redo_step;

void
cluster_cold_redo_step_enter_v1(const ClusterColdStepV1 *step)
{
	Assert(!cold_redo_step.active);
	cold_redo_step.step = *step;
	cold_redo_step.read_rec_ptr = step->read_rec_ptr;
	cold_redo_step.active = true;
}

void
cluster_cold_redo_step_leave_v1(void)
{
	memset(&cold_redo_step, 0, sizeof(cold_redo_step));
}

bool
cluster_cold_redo_block_decision_v1(XLogReaderState *record, uint8 block_id,
									ClusterColdRedoBlockV1 *out)
{
	const ClusterColdBlockStepV1 *block;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	out->action = CLUSTER_COLD_REDO_NATIVE;
	if (!cold_redo_step.active)
		return true;
	/* A different record inside the window is a driver bug; refuse it. */
	if (record == NULL || record->ReadRecPtr != cold_redo_step.read_rec_ptr
		|| block_id > XLR_MAX_BLOCK_ID)
		return false;
	block = &cold_redo_step.step.blocks[block_id];
	switch (block->verdict) {
	case CLUSTER_COLD_BLOCK_NONE:
		return true; /* rebuildable FSM block: unchanged native path */
	case CLUSTER_COLD_BLOCK_SKIP:
		out->action = CLUSTER_COLD_REDO_SKIP;
		break;
	case CLUSTER_COLD_BLOCK_APPLY_DELTA:
	case CLUSTER_COLD_BLOCK_APPLY_IMAGE:
	case CLUSTER_COLD_BLOCK_APPLY_INIT:
		out->action = CLUSTER_COLD_REDO_APPLY;
		break;
	default:
		return false;
	}
	out->expected_kind = block->expected_kind;
	out->expected_before = block->expected_before;
	out->result = block->result;
	return true;
}

static bool
cold_page_all_zero(const char *page)
{
	Size i;

	for (i = 0; i < BLCKSZ; i++)
		if (page[i] != 0)
			return false;
	return true;
}

/* The stored data checksum matches the content (pg_checksum_page scratches
 * pd_checksum, so it runs on a copy). */
static bool
cold_page_checksum_matches(const char *page, BlockNumber blkno)
{
	PGAlignedBlock copy;

	memcpy(copy.data, page, BLCKSZ);
	return pg_checksum_page(copy.data, blkno) == ((const PageHeaderData *)page)->pd_checksum;
}

bool
cluster_cold_classify_page_v1(const char *page, BlockNumber blkno, bool header_valid,
							  bool checksums, ClusterColdDataV1 *out)
{
	memset(out, 0, sizeof(*out));
	if (PageIsNew((Page)page)) {
		/* Only an entirely zero page is unformatted; anything else is torn. */
		if (!cold_page_all_zero(page)) {
			out->kind = CLUSTER_COLD_DATA_INVALID;
			return true;
		}
		out->kind = CLUSTER_COLD_DATA_UNFORMATTED;
		out->flags = CLUSTER_COLD_DATA_FLAG_CONTENT_VERIFIED;
		return true;
	}
	if (!header_valid || (checksums && !cold_page_checksum_matches(page, blkno))) {
		out->kind = CLUSTER_COLD_DATA_INVALID;
		return true;
	}
	/* A formatted page without a version token cannot be placed on a chain;
	 * refuse rather than treat it as replaceable. */
	if (((const PageHeaderData *)page)->pd_block_scn == 0)
		return false;
	out->kind = CLUSTER_COLD_DATA_PRESENT;
	out->version.mutation_token = (uint64)((const PageHeaderData *)page)->pd_block_scn;
	if (checksums)
		out->flags = CLUSTER_COLD_DATA_FLAG_CONTENT_VERIFIED;
	return true;
}

const char *
cluster_cold_refusal_hint_v1(ClusterColdDetailV1 detail)
{
	switch (detail) {
	case CLUSTER_COLD_CONTENT_UNPROVEN:
		return "Shared mode requires data checksums (initdb -k, or pgrac-init); without them "
			   "the named page's content cannot be proven, and no full-page image after its "
			   "last checkpointed change can rebuild it. Preserve all original thread WAL and "
			   "DATA; do not force recovery.";
	case CLUSTER_COLD_ANCHOR_MISSING:
		return "The named page is unreadable or failed verification, and no full-page image "
			   "after its last checkpointed change can rebuild it. Preserve all original "
			   "thread WAL and DATA; do not force recovery.";
	case CLUSTER_COLD_CAPACITY:
		return "The typed cold plan did not fit cluster.cold_recovery_plan_memory. Nothing was "
			   "modified; raise the parameter and restart.";
	case CLUSTER_COLD_OOM:
		return "The server could not allocate the typed cold plan. Nothing was modified; free "
			   "memory on this host and restart.";
	case CLUSTER_COLD_SPACE_INVALID:
		return "The retained WAL holds malformed or contradictory SPACE changes at the named "
			   "record. Nothing was modified; preserve all original thread WAL and DATA.";
	case CLUSTER_COLD_SPACE_REFUSED:
		return "The SPACE owner could not order the named record's relation changes from its "
			   "current SPACE pages (a change is missing or branches, or the pages are "
			   "unreadable). Nothing was modified; preserve all original thread WAL and DATA.";
	case CLUSTER_COLD_IDENTITY_MISSING:
		return "The named page's relation has no readable SPACE identity and was not created "
			   "inside the replayed WAL. Nothing was modified; preserve all original thread WAL "
			   "and DATA; do not force recovery.";
	default:
		return "Preserve all original thread WAL and shared configuration. Shared mode "
			   "recovers every retained writer generation through the typed cold plan and "
			   "never falls back to single-stream replay.";
	}
}

/*
 * The native consumer reads this record's verdicts when it begins and
 * ends, so the step is published first and withdrawn last.  Any refusal
 * inside ends the startup process; nothing here needs unwinding.
 */
void
cluster_cold_apply_step_v1(const ClusterColdStepV1 *step, XLogReaderState *record,
						   ClusterColdApplyRecordV1 apply, void *arg)
{
	cluster_cold_redo_step_enter_v1(step);
	cluster_page_cold_redo_begin_v1(record);
	apply(record, arg);
	cluster_page_cold_redo_end_v1(record);
	cluster_cold_redo_step_leave_v1();
}

#endif /* USE_PGRAC_CLUSTER */
