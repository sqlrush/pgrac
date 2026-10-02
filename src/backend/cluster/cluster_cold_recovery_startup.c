/*-------------------------------------------------------------------------
 *
 * cluster_cold_recovery_startup.c
 *	  Startup-process driver state for typed cold-crash replay.
 *
 *	  Pass 1 runs here, with every foreign origin's external admission held
 *	  and before the serial set (retention pin, then IR) is taken, so its
 *	  ROOT/CF reads and WAL scans never run under IR.  Its results are
 *	  provisional: the caller recommits the unchanged ROOT tokens under the
 *	  serial set and pass 2 matches every record against pass 1.  Every participant is a RECOVERY_REQUIRED writer generation:
 *	  the founder's own previous generation plus every fenced origin.  Each
 *	  root's exact source and native redo come from one ROOT token; its whole
 *	  retained cut is scanned, DATA is observed and the plan is sealed.  Any
 *	  refusal returns before the first mutation with an exact reason.
 *
 *	  Pass 2 runs in xlogrecovery.c.  While it applies one scheduled record,
 *	  the record's per-block verdicts are published here for the typed redo
 *	  block consultation in XLogReadBufferForRedoExtended.
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
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_control_root.h"
#include "cluster/cluster_page_cold_redo.h"
#include "cluster/cluster_recovery_duty.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_wal_tail.h"
#include "utils/memutils.h"

/* Upper bound on everything one cold plan may own (records and versions,
 * never WAL payload).  Exceeding it refuses startup with CAPACITY. */
#define CLUSTER_COLD_PLAN_MEMORY_BUDGET ((Size)1024 * 1024 * 1024)

#define COLD_REQUIRED_ROOT_FLAGS                                                                   \
	(CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID            \
	 | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID)

typedef struct ColdRoot {
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

static bool
cold_scan_and_seal(ClusterColdTypedV1 *typed, const ColdRoot *roots)
{
	ClusterColdDiagV1 diag;
	ClusterColdDetailV1 detail;
	uint32 i;

	for (i = 0; i < typed->participant_count; i++) {
		ClusterColdScanResultV1 scan;

		detail = cluster_cold_scan_root_v1(typed->plan, i, &roots[i].root, &roots[i].token, false,
										   i != typed->own_participant, &scan);
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
	}
	detail = cluster_cold_plan_seal_v1(typed->plan, cluster_cold_observe_data_v1, &typed->observer,
									   &diag);
	if (detail != CLUSTER_COLD_OK) {
		cold_refuse_diag(typed, detail, "plan seal", &diag);
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
	if (!cold_collect_roots(typed, fence, own_thread, roots))
		goto done;
	for (uint32 i = 0; i < typed->participant_count; i++)
		if (!cold_participant(typed, &roots[i], i))
			goto done;
	if (typed->participants[typed->own_participant].native_redo != own_redo) {
		cold_refuse(typed, CLUSTER_COLD_PARTICIPANT_INVALID,
					"own thread %u native redo %X/%X differs from the restart redo %X/%X",
					(unsigned)own_thread,
					LSN_FORMAT_ARGS(typed->participants[typed->own_participant].native_redo),
					LSN_FORMAT_ARGS(own_redo));
		goto done;
	}
	typed->system_identifier = roots[0].root.identity.system_identifier;
	detail = cluster_cold_plan_create_v1(typed->participants, typed->participant_count,
										 CLUSTER_COLD_PLAN_MEMORY_BUDGET, &typed->plan);
	if (detail != CLUSTER_COLD_OK) {
		cold_refuse(typed, detail, "plan creation refused: %s", cold_detail_name(detail));
		goto done;
	}
	if (cold_scan_and_seal(typed, roots))
		typed->refusal = CLUSTER_COLD_OK;

done:
	pfree(roots);
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
	if (typed->plan != NULL)
		cluster_cold_plan_destroy_v1(&typed->plan);
	MemoryContextDelete(typed->context);
}

ClusterColdRouteV1
cluster_cold_route_v1(bool shared_config, bool merge_engaged)
{
	if (!merge_engaged)
		return CLUSTER_COLD_ROUTE_NATIVE;
	return shared_config ? CLUSTER_COLD_ROUTE_TYPED : CLUSTER_COLD_ROUTE_REFUSE;
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
	if (!handshake->completion_publish)
		cold_reason_append(reason, reason_size, "no recovery completion publication (R-A7)");
	if (!handshake->restartpoint_hold)
		cold_reason_append(reason, reason_size,
						   "no restartpoint hold while the cold cut is replayed (R-A9)");
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

bool
cluster_cold_checksum_proves_content_v1(bool checksums_enabled, bool ignore_checksum_failure)
{
	return checksums_enabled && !ignore_checksum_failure;
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
