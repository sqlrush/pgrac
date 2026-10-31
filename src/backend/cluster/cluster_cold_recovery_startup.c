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

#include "miscadmin.h"
#include "access/xlog.h"
#include "access/xlogreader.h"
#include "access/clog.h"
#include "access/multixact.h"
#include "access/xact.h"
#include "storage/standbydefs.h"
#include "catalog/storage_xlog.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_cold_relmap.h"
#include "cluster/cluster_cold_recovery_census.h"
#include "cluster/cluster_side_online_plan.h"
#include "cluster/cluster_side_online_owner.h"
#include "cluster_side_online_plan_internal.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_undo_recovery.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_control_root.h"
#include "cluster/cluster_grd.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_page_cold_redo.h"
#include "cluster/cluster_recovery_duty.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_startup_phase.h"
#include "cluster/cluster_relmap_checkpoint.h"
#include "cluster/cluster_relmap_retention_runtime.h"
#include "cluster/cluster_wal_retention.h"
#include "cluster/cluster_wal_tail.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/cluster_xid_stripe_xlog.h"
#include "storage/bufpage.h"
#include "storage/checksum.h"
#include "postmaster/interrupt.h"
#include "postmaster/startup.h"
#include "portability/instr_time.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/wait_event.h"
#include "cluster_control_bootstrap_private.h"

/* cluster.cold_recovery_plan_memory, kB; registered with the cluster GUCs. */
int cluster_cold_recovery_plan_memory = CLUSTER_COLD_PLAN_MEMORY_DEFAULT_KB;

#define COLD_REQUIRED_ROOT_FLAGS                                                                   \
	(CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID            \
	 | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID)

typedef struct ClusterColdReplayRootV1 {
	ClusterControlRootSnapshot root;
	ClusterControlRootReadToken token;
	ClusterWalSourceRef source;
	XLogRecPtr checkpoint_start;
	CheckPoint native_checkpoint;
	uint8 native_window[160];
	bool checkpoint_captured;
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

/* Diagnostics only: the original claim validator decides admission. Explain
 * its failed format constraint without taking a new observation or treating a
 * native v2 record CRC as a reconstructed legacy CRC. */
static void
cold_own_key_refuse(ClusterColdTypedV1 *typed, uint16 thread,
					const ClusterControlRootIdentity *key)
{
	static const uint8 zero_uuid[16] = {0};
	const char *claim = cluster_shared_config ? "v2" : "v1";
	ClusterWalThreadClaim legacy;

#define COLD_KEY_BAD(condition, field, format, ...) \
	do { \
		if (condition) { \
			cold_refuse(typed, CLUSTER_COLD_PARTICIPANT_INVALID, \
				"own thread %u root predicate=KEY_FORMAT claim=%s field=" #field " " format, \
				(unsigned)thread, claim, __VA_ARGS__); \
			return; \
		} \
	} while (0)
	COLD_KEY_BAD(key->system_identifier == 0, system_identifier,
		"expected=nonzero actual=" UINT64_FORMAT, key->system_identifier);
	COLD_KEY_BAD(memcmp(key->storage_uuid, zero_uuid, 16) == 0, storage_uuid,
		"expected=nonzero_UUID actual=%s", "00000000000000000000000000000000");
	COLD_KEY_BAD(memcmp(key->authority_uuid, zero_uuid, 16) == 0, authority_uuid,
		"expected=nonzero_UUID actual=%s", "00000000000000000000000000000000");
	COLD_KEY_BAD((key->authority_uuid[6] & 0xf0) != 0x40, authority_uuid[6],
		"expected=(byte&0xf0)=0x40 actual=0x%02x", (unsigned)key->authority_uuid[6]);
	COLD_KEY_BAD((key->authority_uuid[8] & 0xc0) != 0x80, authority_uuid[8],
		"expected=(byte&0xc0)=0x80 actual=0x%02x", (unsigned)key->authority_uuid[8]);
	COLD_KEY_BAD(key->origin_thread_id == 0
		|| key->origin_thread_id > CLUSTER_CONTROL_ROOT_RECORD_COUNT, origin_thread_id,
		"expected=1..%u actual=%u", (unsigned)CLUSTER_CONTROL_ROOT_RECORD_COUNT,
		(unsigned)key->origin_thread_id);
	COLD_KEY_BAD(key->origin_node_id < 0
		|| key->origin_node_id >= CLUSTER_CONTROL_ROOT_RECORD_COUNT, origin_node_id,
		"expected=0..%u actual=%d", (unsigned)CLUSTER_CONTROL_ROOT_RECORD_COUNT - 1,
		(int)key->origin_node_id);
	COLD_KEY_BAD(key->origin_thread_id != (uint16)(key->origin_node_id + 1), origin_node_thread,
		"expected=%u actual=%u", (unsigned)key->origin_node_id + 1,
		(unsigned)key->origin_thread_id);
	COLD_KEY_BAD(key->reserved42 != 0, reserved42,
		"expected=0 actual=%u", (unsigned)key->reserved42);
	COLD_KEY_BAD(key->thread_claim_created_at == 0
		|| (cluster_shared_config && key->thread_claim_created_at < 0), thread_claim_created_at,
		"expected=%s actual=" INT64_FORMAT, cluster_shared_config ? ">0" : "nonzero",
		key->thread_claim_created_at);
	COLD_KEY_BAD(key->reserved60 != 0, reserved60,
		"expected=0 actual=%u", key->reserved60);
	COLD_KEY_BAD(key->origin_owner_incarnation == 0, origin_owner_incarnation,
		"expected=nonzero actual=" UINT64_FORMAT, key->origin_owner_incarnation);
	COLD_KEY_BAD(key->root_lineage_seq == 0, root_lineage_seq,
		"expected=nonzero actual=" UINT64_FORMAT, key->root_lineage_seq);
	if (!cluster_shared_config) {
		cluster_wal_thread_claim_fill(&legacy, key->origin_thread_id, key->origin_node_id,
			key->thread_claim_created_at);
		COLD_KEY_BAD(key->thread_claim_crc32c == 0 || key->thread_claim_crc32c != legacy.crc,
			thread_claim_crc32c, "expected=%u actual=%u", (unsigned)legacy.crc,
			key->thread_claim_crc32c);
	}
#undef COLD_KEY_BAD
	/* Keep an explicit refusal if the original validator gains a new rule. */
	cold_refuse(typed, CLUSTER_COLD_PARTICIPANT_INVALID,
		"own thread %u root predicate=KEY_FORMAT claim=%s field=validator expected=valid actual=invalid",
		(unsigned)thread, claim);
}

static void
cold_identity_uuid_hex(const uint8 uuid[16], char out[33])
{
	static const char digits[] = "0123456789abcdef";

	for (unsigned i = 0; i < 16; i++) {
		out[2 * i] = digits[uuid[i] >> 4];
		out[2 * i + 1] = digits[uuid[i] & 15];
	}
	out[32] = '\0';
}

/* Report the first different field in the two values already read, including
 * generation/lineage and complete UUIDs. Never re-read a mutable ROOT here. */
static void
cold_own_identity_refuse(ClusterColdTypedV1 *typed, uint16 thread,
		const ClusterControlRootIdentity *expected, const ClusterControlRootIdentity *actual,
		ClusterRecoveryDutyCompare compare)
{
	char expected_hex[33], actual_hex[33];
	const uint8 *expected_uuid[] = { expected->storage_uuid, expected->authority_uuid };
	const uint8 *actual_uuid[] = { actual->storage_uuid, actual->authority_uuid };
	const char *uuid_field[] = { "storage_uuid", "authority_uuid" };

#define COLD_ID_DIFFERENT(field, format, type) \
	do { \
		if (expected->field != actual->field) { \
			cold_refuse(typed, CLUSTER_COLD_PARTICIPANT_INVALID, \
				"own thread %u root predicate=ROOT_IDENTITY field=" #field \
				" expected=" format " actual=" format \
				" expected_source=lookup actual_source=root compare=%d", \
				(unsigned)thread, (type)expected->field, (type)actual->field, (int)compare); \
			return; \
		} \
	} while (0)
	COLD_ID_DIFFERENT(system_identifier, UINT64_FORMAT, uint64);
	for (unsigned i = 0; i < lengthof(expected_uuid); i++) {
		if (memcmp(expected_uuid[i], actual_uuid[i], 16) != 0) {
			cold_identity_uuid_hex(expected_uuid[i], expected_hex);
			cold_identity_uuid_hex(actual_uuid[i], actual_hex);
			cold_refuse(typed, CLUSTER_COLD_PARTICIPANT_INVALID,
				"own thread %u root predicate=ROOT_IDENTITY field=%s expected=%s actual=%s "
				"expected_source=lookup actual_source=root compare=%d",
				(unsigned)thread, uuid_field[i], expected_hex, actual_hex, (int)compare);
			return;
		}
	}
	COLD_ID_DIFFERENT(origin_thread_id, UINT64_FORMAT, uint64);
	COLD_ID_DIFFERENT(reserved42, UINT64_FORMAT, uint64);
	COLD_ID_DIFFERENT(origin_node_id, INT64_FORMAT, int64);
	COLD_ID_DIFFERENT(thread_claim_created_at, INT64_FORMAT, int64);
	COLD_ID_DIFFERENT(thread_claim_crc32c, UINT64_FORMAT, uint64);
	COLD_ID_DIFFERENT(reserved60, UINT64_FORMAT, uint64);
	COLD_ID_DIFFERENT(origin_owner_incarnation, UINT64_FORMAT, uint64);
	COLD_ID_DIFFERENT(root_lineage_seq, UINT64_FORMAT, uint64);
#undef COLD_ID_DIFFERENT
	cold_refuse(typed, CLUSTER_COLD_PARTICIPANT_INVALID,
		"own thread %u root predicate=ROOT_IDENTITY field=validator expected=%d actual=%d",
		(unsigned)thread, (int)CLUSTER_RECOVERY_DUTY_COMPARE_EXACT, (int)compare);
}

typedef struct ColdOwnRootCfWait {
	PGPROC *proc;
	Latch *latch;
	ResourceOwner owner;
	ClusterColdTypedV1 *typed;
	MemoryContext context;
	struct ClusterColdReplayRootV1 *roots;
	ClusterFormationSnapshotV1 cohort;
	ClusterWalSourceRef restart;
	int pid, node;
	uint16 thread;
	uint64 epoch, generation, boot, system_identifier;
	bool recovery, captured;
} ColdOwnRootCfWait;

static void
cold_own_root_cf_capture(ColdOwnRootCfWait *original, ClusterColdTypedV1 *typed, uint16 thread)
{
	memset(original, 0, sizeof(*original));
	original->proc = MyProc;
	original->latch = MyLatch;
	original->owner = CurrentResourceOwner;
	original->typed = typed;
	original->context = typed->context;
	original->roots = typed->replay_roots;
	original->pid = MyProcPid;
	original->node = cluster_node_id;
	original->thread = thread;
	original->epoch = cluster_epoch_get_current();
	original->generation = cluster_grd_redeclare_generation();
	original->boot = cluster_qvotec_get_self_incarnation();
	original->system_identifier = GetSystemIdentifier();
	original->recovery = InRecovery;
	original->captured
		= thread == (uint16)(cluster_node_id + 1)
		  && cluster_reconfig_capture_formation_snapshot_v1(thread, &original->cohort)
		  && original->cohort.startup_formation_generation != 0
		  && cluster_wal_thread_restart_v2_ref(&original->restart);
}

static bool
cold_own_root_cf_current(const ColdOwnRootCfWait *original)
{
	ClusterFormationSnapshotV1 cohort;
	ClusterWalSourceRef restart;

	return original->captured && original->proc != NULL && original->latch != NULL
		   && original->owner != NULL && original->context != NULL && original->roots != NULL
		   && MyProc == original->proc && MyLatch == original->latch && MyProcPid == original->pid
		   && CurrentResourceOwner == original->owner && MyBackendType == B_STARTUP
		   && AmStartupProcess() && !ShutdownRequestPending && CritSectionCount == 0
		   && !LWLockHeldByMe(ControlFileLock) && cluster_enabled && cluster_shared_config
		   && cluster_controlfile_shared_authority && cluster_node_id == original->node
		   && original->thread == (uint16)(cluster_node_id + 1) && original->epoch != 0
		   && original->generation != 0 && original->boot != 0 && original->system_identifier != 0
		   && original->typed->context == original->context
		   && original->typed->replay_roots == original->roots
		   && GetSystemIdentifier() == original->system_identifier
		   && InRecovery == original->recovery && cluster_epoch_get_current() == original->epoch
		   && cluster_grd_redeclare_generation() == original->generation
		   && cluster_qvotec_get_self_incarnation() == original->boot
		   && cluster_recovery_transport_components_current()
		   && cluster_reconfig_capture_formation_snapshot_v1(original->thread, &cohort)
		   && cluster_formation_snapshot_matches_v1(&original->cohort, &cohort)
		   && cluster_wal_thread_restart_v2_ref(&restart)
		   && memcmp(&original->restart, &restart, sizeof(restart)) == 0;
}

static bool
cold_own_root_monotonic_ns(uint64 *out)
{
	struct timespec now;

	if (clock_gettime(CLOCK_MONOTONIC, &now) != 0 || now.tv_sec < 0 || now.tv_nsec < 0
		|| now.tv_nsec >= NS_PER_S || (uint64)now.tv_sec > (UINT64_MAX - NS_PER_S) / NS_PER_S)
		return false;
	*out = (uint64)now.tv_sec * NS_PER_S + (uint64)now.tv_nsec;
	return true;
}

static bool
cold_own_root_deadline_current(const uint64 *deadline_ns)
{
	uint64 now;

	return deadline_ns == NULL || *deadline_ns == 0
		   || (cold_own_root_monotonic_ns(&now) && now < *deadline_ns);
}

/* NULL preserves the ordinary caller policy; zero permits no added wait.
 * The Startup deadline was created by the original merge plan, not here. */
static int
cold_own_root_cf_budget(const uint64 *deadline_ns)
{
	uint64 now;
	uint64 remaining_ms;

	if (deadline_ns == NULL)
		return 20;
	if (*deadline_ns == 0 || !cold_own_root_monotonic_ns(&now) || now >= *deadline_ns)
		return 0;
	remaining_ms = (*deadline_ns - now) / NS_PER_MS;
	return (int)Min(remaining_ms, UINT64_C(100));
}

/* No serial set is held in pass 1. Retire only this lookup's failed request;
 * neither its cleanup receipt nor the read below supplies replay authority. */
static void
cold_own_root_cf_wait(const ColdOwnRootCfWait *original, const ClusterCfAttemptObservation *attempt,
					  const uint64 *deadline_ns)
{
	ClusterCfRetirementSnapshot retiring;
	bool empty = attempt->owner_state_at_return == CLUSTER_LOCK_OWNER_EMPTY;
	bool empty_waited = false;
	const char *reason = "CONTEXT_CHANGED_AFTER_WAIT_SLICE";

	if (!cold_own_root_cf_current(original) || cluster_cf_held(ExclusiveLock))
		goto refuse;
	reason = "CF_WAIT_IDENTITY_OR_COMPLETION";
	if (!empty
		&& (!cluster_cf_retirement_snapshot(ShareLock, NULL, &retiring)
			|| retiring.cookie != attempt->control_owner_id
			|| retiring.registry.message.key.holder.request_id != attempt->request_id
			|| retiring.registry.message.key.holder.cluster_epoch != attempt->request_epoch))
		goto refuse;
	for (;;) {
		ClusterCfRetirementSnapshot current;
		bool held;
		int wait_ms;

		HandleStartupProcInterrupts();
		CHECK_FOR_INTERRUPTS();
		reason = "CONTEXT_CHANGED_AFTER_WAIT_SLICE";
		if (!cold_own_root_cf_current(original))
			goto refuse;
		reason = "MERGE_DEADLINE_OR_ZERO_ATTEMPT";
		if (deadline_ns != NULL
			&& (*deadline_ns == 0 || !cold_own_root_deadline_current(deadline_ns)))
			goto refuse;
		ResetLatch(MyLatch);
		held = cluster_cf_held(ShareLock);
		if (!held && !cluster_cf_held(ExclusiveLock)) {
			if (!empty && cluster_cf_release_completed(ShareLock, retiring.cookie))
				return;
			if (empty) {
				if (empty_waited)
					return;
				empty_waited = true;
				wait_ms = cold_own_root_cf_budget(deadline_ns);
				if (wait_ms <= 0)
					goto refuse;
				(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, wait_ms,
								WAIT_EVENT_CLUSTER_STARTUP_PHASE_3);
				continue;
			}
		}
		reason = "CF_WAIT_IDENTITY_OR_COMPLETION";
		if (empty || !held || cluster_cf_held(ExclusiveLock)
			|| !cluster_cf_retirement_snapshot(ShareLock, NULL, &current)
			|| current.cookie != retiring.cookie
			|| current.registry.handle.slot != retiring.registry.handle.slot
			|| current.registry.handle.generation != retiring.registry.handle.generation
			|| memcmp(&current.registry.owner, &retiring.registry.owner,
					  sizeof(current.registry.owner))
				   != 0
			|| memcmp(&current.registry.message.key, &retiring.registry.message.key,
					  sizeof(current.registry.message.key))
				   != 0)
			goto refuse;
		cluster_lock_owners_service_poll();
		cluster_cf_retirement_poll();
		if (cluster_cf_held(ShareLock)) {
			reason = "MERGE_DEADLINE_OR_ZERO_ATTEMPT";
			wait_ms = cold_own_root_cf_budget(deadline_ns);
			if (wait_ms <= 0)
				goto refuse;
			(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, wait_ms,
							WAIT_EVENT_CLUSTER_STARTUP_PHASE_3);
		}
	}
refuse:
	ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
					errmsg("cold own-root lookup cannot continue its control-file wait"),
					errdetail("PGRAC_FAMILY=STARTUP_CONTROL consumer=cold_own_root reason=%s "
							  "cookie=" UINT64_FORMAT " request=" UINT64_FORMAT,
							  reason, attempt->control_owner_id, attempt->request_id)));
}

/* The founder's previous generation must be sealed like every peer. */
static bool
cold_own_root(ClusterColdTypedV1 *typed, uint16 own_thread, ColdRoot *out,
			  const uint64 *deadline_ns)
{
	ClusterControlRootIdentity identity;
	ClusterControlRootResult result;
	ClusterRecoveryDutyCompare compare;
	ColdOwnRootCfWait original;
	bool startup = cluster_shared_config && MyBackendType == B_STARTUP && AmStartupProcess();
	bool waited = false;

	if (startup)
		cold_own_root_cf_capture(&original, typed, own_thread);
	for (;;) {
		ClusterCfAttemptObservation attempt;
		uint64 before = startup ? cluster_cf_attempt_sequence() : 0;

		if (!cold_own_root_deadline_current(deadline_ns))
			ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
							errmsg("cold own-root lookup exceeded its original merge deadline"),
							errdetail("stage=merge_deadline before=own_root_read")));
		result = deadline_ns != NULL
					 ? cluster_control_root_lookup_owner_deadline_v1(
						   (int32)own_thread - 1, &identity, &out->root, &out->token, *deadline_ns)
					 : cluster_control_root_lookup_owner_by_node_runtime(
						   (int32)own_thread - 1, &identity, &out->root, &out->token);
		if (!cold_own_root_deadline_current(deadline_ns)) {
			memset(&out->root, 0, sizeof(out->root));
			memset(&out->token, 0, sizeof(out->token));
			ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
							errmsg("cold own-root lookup exceeded its original merge deadline"),
							errdetail("stage=merge_deadline after=own_root_read")));
		}
		/* The acquire can yield. Reject a fresh read from a changed context
		 * before its provisional outputs reach any original admission gate. */
		if (waited && !cold_own_root_cf_current(&original)) {
			memset(&out->root, 0, sizeof(out->root));
			memset(&out->token, 0, sizeof(out->token));
			cold_refuse(
				typed, CLUSTER_COLD_PARTICIPANT_INVALID,
				"own thread %u root predicate=WAIT_CONTEXT expected=original actual=changed",
				(unsigned)own_thread);
			return false;
		}
		if (result != CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE || !startup
			|| !cluster_cf_last_attempt_since(before, &attempt)
			|| !cluster_cf_attempt_wait_slice(&attempt) || attempt.mode != ShareLock)
			break;
		cold_own_root_cf_wait(&original, &attempt, deadline_ns);
		waited = true;
	}
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
		cold_refuse(typed, CLUSTER_COLD_PARTICIPANT_INVALID,
					"own thread %u root predicate=LOOKUP_RESULT expected=%d|%d actual=%d",
					(unsigned)own_thread, (int)CLUSTER_CONTROL_ROOT_OK_PRIMARY,
					(int)CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED, (int)result);
		return false;
	}
	/* PRE2's canonical reader authenticates its native v2 record CRC, which
	 * differs from the legacy claim CRC. Retain the original claim domain. */
	if (!cluster_recovery_duty_key_valid_for_claim(&identity, cluster_shared_config)) {
		cold_own_key_refuse(typed, own_thread, &identity);
		return false;
	}
	if (identity.origin_thread_id != own_thread) {
		cold_refuse(typed, CLUSTER_COLD_PARTICIPANT_INVALID,
			"own thread %u root predicate=OWN_THREAD field=origin_thread_id expected=%u actual=%u",
			(unsigned)own_thread, (unsigned)own_thread, (unsigned)identity.origin_thread_id);
		return false;
	}
	compare = cluster_recovery_duty_key_compare_for_claim(&identity, &out->root.identity,
		cluster_shared_config);
	if (compare != CLUSTER_RECOVERY_DUTY_COMPARE_EXACT) {
		cold_own_identity_refuse(typed, own_thread, &identity, &out->root.identity, compare);
		return false;
	}
	if (out->root.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED) {
		cold_refuse(typed, CLUSTER_COLD_PARTICIPANT_INVALID,
			"own thread %u previous generation is not sealed for recovery "
			"predicate=SEALED_LIFECYCLE expected=%u actual=%u",
			(unsigned)own_thread, (unsigned)CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED,
			(unsigned)out->root.lifecycle);
		return false;
	}
	if ((out->root.root_flags & COLD_REQUIRED_ROOT_FLAGS) != COLD_REQUIRED_ROOT_FLAGS) {
		cold_refuse(typed, CLUSTER_COLD_PARTICIPANT_INVALID,
					"own thread %u previous generation is not sealed for recovery "
					"predicate=SEALED_FLAGS expected=0x%x actual=0x%x missing=0x%x",
					(unsigned)own_thread, COLD_REQUIRED_ROOT_FLAGS, out->root.root_flags,
					COLD_REQUIRED_ROOT_FLAGS & ~out->root.root_flags);
		return false;
	}
	return true;
}

/* Exact source, complete native checkpoint and cut from the original token.
 * This binds input only, not a TT slot's era or a winner/loser decision. */
static void
cold_prepare_deadline_check(const uint64 *deadline)
{
	if (!cold_own_root_deadline_current(deadline))
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
						errmsg("cold preparation exceeded its original merge deadline")));
}

static bool
cold_participant(ClusterColdTypedV1 *typed, ColdRoot *root, uint32 index, const uint64 *deadline)
{
	ClusterColdParticipantV1 *participant = &typed->participants[index];
	ClusterControlRootResult result;
	ClusterWalRecoveryCheckpointV1 selected;

	cold_prepare_deadline_check(deadline);
	result
		= deadline != NULL
			  ? cluster_control_root_recovery_checkpoint_deadline_v1(&root->root, &root->token,
																	 &selected, *deadline)
			  : cluster_control_root_recovery_checkpoint_v1(&root->root, &root->token, &selected);
	cold_prepare_deadline_check(deadline);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| selected.checkpoint.redo < root->root.checkpoint_lower_lsn
		|| selected.checkpoint.redo > root->root.validated_tail_lsn_exclusive) {
		cold_refuse(typed, CLUSTER_COLD_PARTICIPANT_INVALID,
					"thread %u native checkpoint input is unproven (result %d)",
					(unsigned)root->root.identity.origin_thread_id, (int)result);
		return false;
	}
	typed->sources[index] = selected.source;
	root->source = selected.source;
	root->checkpoint_start = selected.checkpoint_start;
	memcpy(&root->native_checkpoint, &selected.checkpoint, sizeof(root->native_checkpoint));
	memcpy(root->native_window, selected.native_window, sizeof(root->native_window));
	root->checkpoint_captured = true;
	memset(participant, 0, sizeof(*participant));
	participant->thread_id = root->root.identity.origin_thread_id;
	participant->timeline = root->root.checkpoint_tli;
	participant->owner_incarnation = root->root.identity.origin_owner_incarnation;
	participant->physical_lower = root->root.checkpoint_lower_lsn;
	participant->native_redo = selected.checkpoint.redo;
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
	uint64 deadline_ns;
	const uint64 *deadline = NULL;

	if (count == 0 || (uint32)count + 1 > CLUSTER_COLD_MAX_PARTICIPANTS) {
		cold_refuse(typed, CLUSTER_COLD_PARTICIPANT_INVALID, "fence plan names %u origins",
					(unsigned)count);
		return false;
	}
	if (cluster_shared_config && MyBackendType == B_STARTUP && AmStartupProcess()) {
		if (!cluster_recovery_merge_fence_plan_deadline_v1(fence, &deadline_ns)) {
			cold_refuse(typed, CLUSTER_COLD_PARTICIPANT_INVALID,
						"stage=merge_deadline reason=ORIGINAL_PLAN_INVALID");
			return false;
		}
		deadline = &deadline_ns;
	}
	if (!cold_own_root(typed, own_thread, &roots[0], deadline))
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
	if (!cold_own_root_deadline_current(deadline))
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
						errmsg("cold root collection exceeded its original merge deadline")));
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
cold_census_take(ClusterColdTypedV1 *typed, ColdRoot *roots, const ClusterWalInputV1 *const *inputs,
				 uint32 input_count, const ClusterColdCensusEntryV1 *entries, uint32 count)
{
	uint32 crashed = typed->participant_count;
	uint32 i;
	uint32 k;

	for (i = 0; i < count; i++) {
		const ClusterColdCensusEntryV1 *entry = &entries[i];
		const ClusterWalInputV1 *input
			= entry->input_index < input_count ? inputs[entry->input_index] : NULL;

		/* Preserve the complete checkpoint read by the original retained-WAL
		 * owner, before its scope is released. A history cut intentionally has
		 * native_redo == tail; its checkpoint's actual redo is a separate input. */
		if (input == NULL || input->kind != CLUSTER_WAL_INPUT_CHECKPOINT
			|| !cluster_control_root_identity_equal(&input->checkpoint.identity, &entry->identity)
			|| !cluster_control_root_identity_equal(&input->source.claim.identity, &entry->identity)
			|| memcmp(&input->source, &entry->source, sizeof(input->source)) != 0
			|| input->source.timeline != entry->cut.timeline
			|| input->checkpoint.checkpoint_tli != entry->cut.timeline
			|| input->checkpoint.checkpoint_lower_lsn != entry->cut.physical_lower
			|| input->checkpoint.validated_tail_lsn_exclusive != entry->cut.tail_end
			|| XLogRecPtrIsInvalid(input->checkpoint_start)
			|| input->native_checkpoint.ThisTimeLineID != entry->cut.timeline
			|| input->native_checkpoint.redo != input->native_redo
			|| input->native_redo < entry->cut.physical_lower
			|| input->native_redo > entry->cut.tail_end) {
			cold_refuse(typed, CLUSTER_COLD_PARTICIPANT_INVALID,
						"thread %u census native checkpoint input differs from its selected source",
						(unsigned)entry->cut.thread_id);
			return false;
		}

		if (entry->role == CLUSTER_COLD_CENSUS_REPLAY) {
			for (k = 0; k < crashed; k++)
				if (cluster_control_root_identity_equal(&roots[k].root.identity, &entry->identity))
					break;
			if (k == crashed
				|| memcmp(&typed->participants[k], &entry->cut, sizeof(entry->cut)) != 0
				|| memcmp(&roots[k].root, &input->checkpoint, sizeof(input->checkpoint)) != 0
				|| memcmp(&typed->sources[k], &input->source, sizeof(input->source)) != 0
				|| roots[k].checkpoint_start != input->checkpoint_start
				|| memcmp(&roots[k].native_checkpoint, &input->native_checkpoint,
						  sizeof(input->native_checkpoint))
					   != 0
				|| memcmp(roots[k].native_window, input->native_window,
						  sizeof(input->native_window)) != 0) {
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
		roots[typed->participant_count].root = input->checkpoint;
		roots[typed->participant_count].source = input->source;
		roots[typed->participant_count].checkpoint_start = input->checkpoint_start;
		memcpy(&roots[typed->participant_count].native_checkpoint, &input->native_checkpoint,
			   sizeof(input->native_checkpoint));
		memcpy(roots[typed->participant_count].native_window, input->native_window,
			   sizeof(input->native_window));
		roots[typed->participant_count].checkpoint_captured = true;
		typed->participant_count++;
	}
	return true;
}

/* Complete participant census: every generation with retained WAL under one
 * cold read scope, held until pass 1 has scanned the history-only ones. */
static bool
cold_census(ClusterColdTypedV1 *typed, ColdRoot *roots, const uint64 *deadline)
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
	cold_prepare_deadline_check(deadline);
	result = deadline != NULL
				 ? cluster_wal_inputs_cold_begin_deadline_v1(
					   own->storage_uuid, own->system_identifier, &typed->inputs, *deadline)
				 : cluster_wal_inputs_cold_begin_v1(own->storage_uuid, own->system_identifier,
													&typed->inputs);
	cold_prepare_deadline_check(deadline);
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
		taken = cold_census_take(typed, roots, inputs, count, entries, kept);
	pfree(crashed);
	pfree(entries);
	pfree(inputs);
	cold_prepare_deadline_check(deadline);
	return taken;
}

/* The history-only generations, under the census scope; then release it. */
static bool
cold_scan_history(ClusterColdTypedV1 *typed)
{
	ClusterControlRootResult result;
	uint32 i;

	memset(typed->history_xid, 0, sizeof(typed->history_xid));
	typed->census_root_captured = false;
	for (i = typed->replay_count; i < typed->participant_count; i++) {
		ClusterColdScanResultV1 scan;
		ClusterColdDetailV1 detail
			= cluster_cold_scan_input_v1(typed->plan, i, typed->inputs, typed->input_index[i],
										 false, true, typed->side_plans[i], &scan);

		typed->scanned_records += scan.records;
		if (detail == CLUSTER_COLD_OK
			&& memcmp(scan.native_window, typed->replay_roots[i].native_window,
					  sizeof(scan.native_window)) != 0)
			detail = CLUSTER_COLD_SOURCE_GAP;
		if (detail != CLUSTER_COLD_OK) {
			cold_refuse(typed, detail,
						"pass-1 scan refused: %s; history thread %u record %X/%X (root result %d, "
						"record seen %u, rmid %u, info 0x%02X, length %u, data length %u)",
						cold_detail_name(detail), (unsigned)typed->participants[i].thread_id,
						LSN_FORMAT_ARGS(scan.failed_read_rec_ptr), scan.root_result,
						(unsigned)scan.failed_record_present, (unsigned)scan.failed_record_rmid,
						(unsigned)scan.failed_record_info, (unsigned)scan.failed_record_length,
						(unsigned)scan.failed_record_data_length);
			memset(typed->history_xid, 0, sizeof(typed->history_xid));
			return false;
		}
		typed->history_xid[i] = scan.history_xid;
		if (scan.side_capture_detail != 0) {
			typed->side_capture_detail[i] = scan.side_capture_detail;
			typed->side_capture_read_rec_ptr[i] = scan.side_capture_read_rec_ptr;
			rf_side_online_plan_destroy_v1(&typed->side_plans[i]);
		}
	}
	result = cluster_wal_inputs_revalidate_v1(typed->inputs);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		result = cluster_wal_inputs_root_token_v1(typed->inputs, &typed->census_root);
	cluster_wal_inputs_release_v1(&typed->inputs);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP,
					"participant census changed while it was scanned (root result %d)",
					(int)result);
		memset(typed->history_xid, 0, sizeof(typed->history_xid));
		return false;
	}
	typed->census_root_captured = true;
	return true;
}


/* This scope owns only provisional metadata reads. It must be released
 * before the caller acquires the execution pin/IR set; it cannot replace
 * that set's later retention, source-durability and freshness checks. */
static bool
cold_census_matches(ClusterColdTypedV1 *typed)
{
	ClusterControlRootFileToken token;
	uint32 count = cluster_wal_inputs_count_v1(typed->inputs);
	ClusterControlRootResult result;

	result = cluster_wal_inputs_root_token_v1(typed->inputs, &token);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP,
					"pre-execution census refused: stage=census_root_read result=%d", (int)result);
		return false;
	}
	if (memcmp(&token, &typed->census_root, sizeof(token)) != 0) {
		const unsigned char *expected = (const unsigned char *)&typed->census_root;
		const unsigned char *observed = (const unsigned char *)&token;
		const char *field = "padding";
		size_t byte = 0;

		/* Diagnose the same original comparison; do not reread or rebase the
		 * selected ROOT. The byte index also identifies array elements. */
		while (byte < sizeof(token) && expected[byte] == observed[byte])
			byte++;
#define CENSUS_TOKEN_FIELD(member)                                                                 \
	if (byte >= offsetof(ClusterControlRootFileToken, member)                                      \
		&& byte < offsetof(ClusterControlRootFileToken, member) + sizeof(token.member))            \
	field = #member
		CENSUS_TOKEN_FIELD(authority_uuid);
		CENSUS_TOKEN_FIELD(file_txn_seq);
		CENSUS_TOKEN_FIELD(body_crc32c);
		CENSUS_TOKEN_FIELD(header_crc32c);
		CENSUS_TOKEN_FIELD(activation_state);
		CENSUS_TOKEN_FIELD(format_version);
		CENSUS_TOKEN_FIELD(record_count);
		CENSUS_TOKEN_FIELD(system_identifier);
		CENSUS_TOKEN_FIELD(image_sha256);
#undef CENSUS_TOKEN_FIELD
		cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP,
					"pre-execution census refused: stage=census_root_changed field=%s byte=%zu "
					"expected_byte=%u observed_byte=%u expected_txn=" UINT64_FORMAT
					" observed_txn=" UINT64_FORMAT,
					field, byte, expected[byte], observed[byte], typed->census_root.file_txn_seq,
					token.file_txn_seq);
		return false;
	}
	for (uint32 i = typed->replay_count; i < typed->participant_count; i++) {
		const ColdRoot *selected = &typed->replay_roots[i];
		const ClusterWalInputV1 *input
			= typed->input_index[i] < count
				  ? cluster_wal_inputs_at_v1(typed->inputs, typed->input_index[i])
				  : NULL;
		const char *field = NULL;

		/* The whole ROOT token also fixes input ordering and claim/anchor
		 * references. Compare the actual checkpoint bytes read by that owner,
		 * including the full-width seed used for the preserved history fold.
		 * Preserve the original census role: current CLOSED/RECOVERY_COMPLETE
		 * inputs can also be history. */
		if (input == NULL)
			field = "missing";
		else if (input->kind != CLUSTER_WAL_INPUT_CHECKPOINT)
			field = "kind";
		else if (!selected->checkpoint_captured)
			field = "checkpoint_captured";
		else if (memcmp(&input->checkpoint, &selected->root, sizeof(input->checkpoint)) != 0)
			field = "root";
		else if (memcmp(&input->source, &typed->sources[i], sizeof(input->source)) != 0)
			field = "source";
		else if (input->checkpoint_start != selected->checkpoint_start)
			field = "checkpoint_start";
		else if (input->native_redo != selected->native_checkpoint.redo)
			field = "native_redo";
		else if (memcmp(&input->native_checkpoint, &selected->native_checkpoint,
						sizeof(input->native_checkpoint)) != 0)
			field = "native_checkpoint";
		else if (memcmp(input->native_window, selected->native_window,
						sizeof(input->native_window)) != 0)
			field = "native_window";
		if (field != NULL) {
			cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP,
						"pre-execution census refused: stage=census_history participant=%u "
						"thread=%u input_index=%u field=%s",
						i, typed->participants[i].thread_id, typed->input_index[i], field);
			return false;
		}
	}
	result = cluster_wal_inputs_revalidate_v1(typed->inputs);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP,
					"pre-execution census refused: stage=census_revalidate result=%d", (int)result);
	return result == CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

bool
cluster_cold_typed_recheck_inputs_v1(ClusterColdTypedV1 *typed)
{
	bool matched = false;

	if (typed == NULL)
		return false;
	if (typed->refusal != CLUSTER_COLD_OK || typed->plan == NULL || typed->inputs != NULL
		|| !typed->census_root_captured || typed->replay_roots == NULL
		|| typed->own_participant != 0 || typed->replay_count == 0
		|| typed->replay_count > typed->participant_count
		|| typed->participant_count > CLUSTER_COLD_MAX_PARTICIPANTS
		|| typed->replay_root_count != typed->replay_count
		|| cluster_cold_plan_participant_count_v1(typed->plan) != typed->participant_count
		|| cluster_cold_replay_window_active_v1()) {
		cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP, "pre-execution census state is invalid");
		memset(typed->history_xid, 0, sizeof(typed->history_xid));
		typed->census_root_captured = false;
		return false;
	}
	/* ERROR must also leave this attempt refused, with no surviving inputs. */
	cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP, "pre-execution census freshness is incomplete");
	PG_TRY();
	{
		const ClusterControlRootIdentity *own = &typed->replay_roots[0].root.identity;
		ClusterControlRootResult result;

		result = cluster_wal_inputs_cold_begin_v1(own->storage_uuid, typed->system_identifier,
												 &typed->inputs);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			matched = cold_census_matches(typed);
		else
			cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP,
						"pre-execution census refused: stage=census_begin result=%d", (int)result);
		if (typed->inputs != NULL)
			cluster_wal_inputs_release_v1(&typed->inputs);
	}
	PG_CATCH();
	{
		memset(typed->history_xid, 0, sizeof(typed->history_xid));
		typed->census_root_captured = false;
		if (typed->inputs != NULL)
			cluster_wal_inputs_release_v1(&typed->inputs);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (!matched) {
		memset(typed->history_xid, 0, sizeof(typed->history_xid));
		typed->census_root_captured = false;
		return false;
	}
	typed->refusal = CLUSTER_COLD_OK;
	typed->refusal_detail[0] = '\0';
	return true;
}

/* Copies the same physical union for initial retention and commit retry.
 * This reads only the original local plan; it never opens a census under IR. */
static bool
cold_execution_input_threads(ClusterColdTypedV1 *typed, uint16 *threads, uint16 *count)
{
	bool selected[CLUSTER_WAL_RETENTION_MAX_THREADS + 1] = { false };

	*count = 0;
	if (typed == NULL)
		return false;
	if (typed->refusal != CLUSTER_COLD_OK)
		return false;
	if (!typed->census_root_captured || typed->inputs != NULL || typed->replay_roots == NULL
		|| typed->participant_count == 0 || typed->participant_count > CLUSTER_COLD_MAX_PARTICIPANTS
		|| cluster_cold_replay_window_active_v1()) {
		cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP,
					"execution input retention refused: stage=input_state");
		return false;
	}
	for (uint32 i = 0; i < typed->participant_count; i++) {
		uint16 thread = typed->participants[i].thread_id;

		if (thread == 0 || thread > CLUSTER_WAL_RETENTION_MAX_THREADS
			|| thread != typed->replay_roots[i].root.identity.origin_thread_id
			|| thread != typed->sources[i].claim.identity.origin_thread_id) {
			cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP,
						"execution input retention refused: stage=input_thread participant=%u "
						"thread=%u root_thread=%u source_thread=%u",
						i, thread, typed->replay_roots[i].root.identity.origin_thread_id,
						typed->sources[i].claim.identity.origin_thread_id);
			return false;
		}
		selected[thread] = true;
	}
	for (uint16 thread = 1; thread <= CLUSTER_WAL_RETENTION_MAX_THREADS; thread++) {
		if (selected[thread])
			threads[(*count)++] = thread;
	}
	return *count != 0;
}

bool
cluster_cold_typed_acquire_serial_v1(ClusterColdTypedV1 *typed, ClusterRecoveryFencePlan *fence)
{
	uint16 threads[CLUSTER_WAL_RETENTION_MAX_THREADS];
	uint16 count;
	bool acquired = false;

	if (typed == NULL)
		return false;
	if (fence == NULL) {
		cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP,
					"execution input retention refused: stage=fence");
		goto refused;
	}
	if (!cluster_cold_typed_recheck_inputs_v1(typed)
		|| !cold_execution_input_threads(typed, threads, &count))
		goto refused;
	cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP, "execution input retention is incomplete");
	PG_TRY();
	{
		acquired = cluster_recovery_merge_fence_plan_acquire_serial_inputs_v1(fence, threads, count);
	}
	PG_CATCH();
	{
		memset(&typed->history_xid, 0, sizeof(typed->history_xid));
		typed->census_root_captured = false;
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (!acquired) {
		cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP,
					"execution input retention refused: stage=serial_inputs input_threads=%u", count);
		goto refused;
	}
	typed->refusal = CLUSTER_COLD_OK;
	typed->refusal_detail[0] = '\0';
	return true;

refused:
	memset(&typed->history_xid, 0, sizeof(typed->history_xid));
	typed->census_root_captured = false;
	return false;
}

bool
cluster_cold_typed_commit_serial_v1(ClusterColdTypedV1 *typed, ClusterRecoveryFencePlan *fence)
{
	uint16 threads[CLUSTER_WAL_RETENTION_MAX_THREADS];
	uint16 count;
	bool committed = false;

	if (typed == NULL)
		return false;
	if (fence == NULL) {
		cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP, "execution input commit refused: stage=fence");
		goto refused;
	}
	if (!cold_execution_input_threads(typed, threads, &count))
		goto refused;
	cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP, "execution input commit is incomplete");
	PG_TRY();
	{
		committed = cluster_recovery_merge_commit_plan_inputs_v1(fence, threads, count);
	}
	PG_CATCH();
	{
		memset(&typed->history_xid, 0, sizeof(typed->history_xid));
		typed->census_root_captured = false;
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (!committed) {
		cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP,
					"execution input commit refused: stage=commit_inputs input_threads=%u", count);
		goto refused;
	}
	typed->refusal = CLUSTER_COLD_OK;
	typed->refusal_detail[0] = '\0';
	return true;

refused:
	memset(&typed->history_xid, 0, sizeof(typed->history_xid));
	typed->census_root_captured = false;
	return false;
}

static bool cold_sources_match(ClusterColdTypedV1 *typed, ClusterRecoveryFencePlan *fence,
							   const uint64 *deadline);

/* PAGE, SIDE and original TT inputs share the existing plan budget. */
static Size
cold_tt_budget(void)
{
	return (Size)cluster_cold_recovery_plan_memory * 1024 / 4;
}

static Size
cold_side_budget(const ClusterColdTypedV1 *typed)
{
	Size total = (Size)cluster_cold_recovery_plan_memory * 1024;
	return typed->participant_count == 0
			   ? 0
			   : Min((Size)RF_SIDE_ONLINE_PLAN_MAX_BYTES, total / 2 / typed->participant_count);
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
	for (uint32 i = 0; i < typed->participant_count; i++) {
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

/* Bootstrap's unsupported physical-profile check is provisional. Recheck
 * each original origin, including history-only inputs, under this owner;
 * generations share one physical directory and confer no new authority. */
static bool
cold_native_profiles_match(const ClusterColdTypedV1 *typed)
{
	bool seen[CLUSTER_CONTROL_ROOT_RECORD_COUNT] = { false };

	for (uint32 p = 0; p < typed->participant_count; p++) {
		uint32 node = typed->sources[p].claim.identity.origin_node_id;

		if (node >= CLUSTER_CONTROL_ROOT_RECORD_COUNT
			|| typed->participants[p].thread_id != node + 1)
			return false;
		if (seen[node])
			continue;
		if (cluster_control_native_commit_ts_profile_v1(cluster_shared_data_dir, node)
			!= CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return false;
		seen[node] = true;
	}
	return true;
}

static bool
cold_scan_and_seal(ClusterColdTypedV1 *typed, const ColdRoot *roots,
				   ClusterRecoveryFencePlan *fence, const uint64 *deadline)
{
	ClusterColdDiagV1 diag;
	ClusterColdDetailV1 detail;
	uint32 i;
	bool profiles_current;

	cold_prepare_deadline_check(deadline);
	profiles_current = cold_native_profiles_match(typed);
	cold_prepare_deadline_check(deadline);
	if (!profiles_current) {
		cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP,
					"original commit_ts physical profile is unsupported or unavailable");
		return false;
	}
	cold_prepare_deadline_check(deadline);
	if (!cold_space_namespace(typed))
		return false;
	detail = cluster_cold_tt_base_capture_v1(typed, cold_tt_budget());
	cold_prepare_deadline_check(deadline);
	if (detail != CLUSTER_COLD_OK) {
		cold_refuse(typed, detail, "original TT base capture refused: %s",
					cold_detail_name(detail));
		return false;
	}
	if (!cold_side_create(typed))
		return false;
	detail = cluster_cold_plan_set_space_check_v1(typed->plan, cluster_cold_space_check_v1,
												  &typed->observer);
	if (detail != CLUSTER_COLD_OK) {
		cold_refuse(typed, detail, "SPACE check refused: %s", cold_detail_name(detail));
		return false;
	}

	for (i = 0; i < typed->replay_count; i++) {
		ClusterColdScanResultV1 scan;

		cold_prepare_deadline_check(deadline);
		detail = deadline != NULL
					 ? cluster_cold_scan_root_deadline_v1(
						   typed->plan, i, &roots[i].root, &roots[i].token, false,
						   i != typed->own_participant, typed->side_plans[i], &scan, *deadline)
					 : cluster_cold_scan_root_v1(typed->plan, i, &roots[i].root, &roots[i].token,
												 false, i != typed->own_participant,
												 typed->side_plans[i], &scan);
		cold_prepare_deadline_check(deadline);
		typed->scanned_records += scan.records;
		if (detail == CLUSTER_COLD_OK
			&& memcmp(scan.native_window, roots[i].native_window, sizeof(scan.native_window)) != 0)
			detail = CLUSTER_COLD_SOURCE_GAP;
		if (detail != CLUSTER_COLD_OK) {
			cold_refuse(typed, detail,
						"pass-1 scan refused: %s; thread %u record %X/%X (root result %d, "
						"route detail %u, record seen %u, rmid %u, info 0x%02X, length %u, "
						"data length %u)",
						cold_detail_name(detail), (unsigned)typed->participants[i].thread_id,
						LSN_FORMAT_ARGS(scan.failed_read_rec_ptr), scan.root_result,
						(unsigned)scan.route_detail, (unsigned)scan.failed_record_present,
						(unsigned)scan.failed_record_rmid, (unsigned)scan.failed_record_info,
						(unsigned)scan.failed_record_length, (unsigned)scan.failed_record_data_length);
			return false;
		}
		if (scan.side_capture_detail != 0) {
			typed->side_capture_detail[i] = scan.side_capture_detail;
			typed->side_capture_read_rec_ptr[i] = scan.side_capture_read_rec_ptr;
			rf_side_online_plan_destroy_v1(&typed->side_plans[i]);
		}
	}
	if (!cold_scan_history(typed)) {
		cold_prepare_deadline_check(deadline);
		return false;
	}
	cold_prepare_deadline_check(deadline);
	detail = cluster_cold_plan_seal_v1(typed->plan, cluster_cold_observe_data_v1, &typed->observer,
									   &diag);
	cold_prepare_deadline_check(deadline);
	if (detail != CLUSTER_COLD_OK) {
		cold_refuse_diag(typed, detail, "plan seal", &diag);
		return false;
	}
	/* No provisional payload plan survives a later source or ROOT failure. */
	if (!cold_sources_match(typed, fence, deadline)) {
		cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP, "SIDE capture source changed before seal");
		return false;
	}
	for (uint32 i = 0; i < typed->participant_count; i++) {
		RfPageProofDetailV1 side_detail;
		if (typed->side_plans[i] == NULL)
			continue;
		if (!rf_side_online_plan_bind_database_v1(typed->side_plans[i],
												  typed->observer.database_incarnation))
			side_detail = RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
		else
			side_detail = rf_side_online_plan_seal_v1(typed->side_plans[i]);
		cold_prepare_deadline_check(deadline);
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
	if (!cold_sources_match(typed, fence, deadline)) {
		cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP, "SIDE capture source changed after seal");
		return false;
	}
	cold_prepare_deadline_check(deadline);
	return true;
}

static ClusterColdTypedV1 *
cold_typed_prepare(ClusterRecoveryFencePlan *fence, uint16 own_thread,
	XLogRecPtr own_redo, const ClusterColdRelmapInputV1 *relmap, uint32 relmap_count)
{
	MemoryContext context;
	MemoryContext old_context;
	ClusterColdTypedV1 *typed;
	ColdRoot *roots;
	ClusterColdDetailV1 detail;
	uint64 deadline_ns;
	const uint64 *deadline = NULL;

	context = AllocSetContextCreate(TopMemoryContext, "cluster cold replay plan",
									ALLOCSET_DEFAULT_SIZES);
	old_context = MemoryContextSwitchTo(context);
	typed = (ClusterColdTypedV1 *)palloc0(sizeof(*typed));
	typed->context = context;
	roots = (ColdRoot *)palloc0(sizeof(ColdRoot) * CLUSTER_COLD_MAX_PARTICIPANTS);
	typed->replay_roots = roots;
	if (cluster_shared_config && MyBackendType == B_STARTUP && AmStartupProcess()) {
		if (!cluster_recovery_merge_fence_plan_deadline_v1(fence, &deadline_ns)) {
			cold_refuse(typed, CLUSTER_COLD_PARTICIPANT_INVALID,
						"original prepare plan is invalid");
			goto done;
		}
		deadline = &deadline_ns;
		cold_prepare_deadline_check(deadline);
	}
	if (!cluster_relmap_retention_runtime_recovery_invalidate_v2()) {
		cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP, "shared relmap retention invalidation refused");
		goto done;
	}
	if (!cold_collect_roots(typed, fence, own_thread, roots))
		goto done;
	typed->replay_root_count = typed->participant_count;
	for (uint32 i = 0; i < typed->participant_count; i++)
		if (!cold_participant(typed, &roots[i], i, deadline))
			goto done;
	if (!cold_own_restart(typed, own_thread, own_redo) || !cold_census(typed, roots, deadline))
		goto done;
	typed->system_identifier = roots[0].root.identity.system_identifier;
	detail = cluster_cold_plan_create_v1(typed->participants, typed->participant_count,
										 (Size)cluster_cold_recovery_plan_memory * 1024
											 - cold_side_budget(typed) * typed->participant_count
											 - cold_tt_budget(),
										 &typed->plan);
	if (detail != CLUSTER_COLD_OK) {
		cold_refuse(typed, detail, "plan creation refused: %s", cold_detail_name(detail));
		goto done;
	}
	if (relmap_count != 0) {
		detail = cluster_cold_plan_relmap_declare_v1(typed->plan, relmap, relmap_count);
		if (detail != CLUSTER_COLD_OK) {
			cold_refuse(typed, detail, "declared relmap input refused");
			goto done;
		}
		for (uint32 i = 0; i < relmap_count; ++i) {
			uint32 p = relmap[i].participant;
			if (p >= typed->replay_count
				|| memcmp(&relmap[i].source, &typed->sources[p], sizeof(relmap[i].source)) != 0) {
				cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP, "declared relmap source changed");
				goto done;
			}
		}
	}
	if (cold_scan_and_seal(typed, roots, fence, deadline))
		typed->refusal = CLUSTER_COLD_OK;

done:
	if (typed->refusal != CLUSTER_COLD_OK) {
		cold_side_destroy(typed);
		cluster_cold_tt_base_destroy_v1(typed);
	}
	MemoryContextSwitchTo(old_context);
	return typed;
}

ClusterColdTypedV1 *
cluster_cold_typed_prepare_v1(ClusterRecoveryFencePlan *fence, uint16 own_thread, XLogRecPtr own_redo)
{
	return cold_typed_prepare(fence, own_thread, own_redo, NULL, 0);
}

ClusterColdTypedV1 *
cluster_cold_typed_prepare_relmap_v1(ClusterRecoveryFencePlan *fence, uint16 own_thread,
	XLogRecPtr own_redo, const ClusterColdRelmapInputV1 *inputs, uint32 count)
{
	/* Invalid declaration never falls back to the ordinary undeclared mode. */
	if (inputs == NULL || count == 0 || count > CLUSTER_COLD_RELMAP_MAX_INPUTS)
		return NULL;
	return cold_typed_prepare(fence, own_thread, own_redo, inputs, count);
}

void
cluster_cold_typed_destroy_v1(ClusterColdTypedV1 **typed_address)
{
	ClusterColdTypedV1 *typed;

	if (typed_address == NULL || *typed_address == NULL)
		return;
	typed = *typed_address;
	if (typed->inputs != NULL)
		cluster_wal_inputs_release_v1(&typed->inputs);
	if (typed->tx_fence != NULL) {
		cluster_recovery_merge_fence_plan_destroy(&typed->tx_fence);
		if (typed->tx_fence != NULL)
			elog(FATAL, "could not release original cold transaction fence owner");
	}
	*typed_address = NULL;
	cold_side_destroy(typed);
	cluster_cold_tt_base_destroy_v1(typed);
	if (typed->plan != NULL)
		cluster_cold_plan_destroy_v1(&typed->plan);
	if (typed->replay_roots != NULL)
		pfree(typed->replay_roots);
	MemoryContextDelete(typed->context);
}

bool
cluster_cold_typed_checkpoint_v1(const ClusterColdTypedV1 *typed, uint32 participant,
								 ClusterControlRootSnapshot *out_root,
								 ClusterWalRecoveryCheckpointV1 *out_checkpoint)
{
	const ColdRoot *selected;

	if (out_root != NULL)
		memset(out_root, 0, sizeof(*out_root));
	if (out_checkpoint != NULL)
		memset(out_checkpoint, 0, sizeof(*out_checkpoint));
	if (out_root == NULL || out_checkpoint == NULL || typed == NULL
		|| typed->refusal != CLUSTER_COLD_OK || typed->plan == NULL || typed->replay_roots == NULL
		|| typed->participant_count > CLUSTER_COLD_MAX_PARTICIPANTS
		|| participant >= typed->participant_count)
		return false;
	selected = &typed->replay_roots[participant];
	if (!selected->checkpoint_captured
		|| !cluster_control_root_identity_equal(&selected->root.identity,
												&typed->sources[participant].claim.identity))
		return false;
	*out_root = selected->root;
	out_checkpoint->source = typed->sources[participant];
	out_checkpoint->checkpoint_start = selected->checkpoint_start;
	memcpy(&out_checkpoint->checkpoint, &selected->native_checkpoint,
		   sizeof(out_checkpoint->checkpoint));
	memcpy(out_checkpoint->native_window, selected->native_window,
		   sizeof(out_checkpoint->native_window));
	return true;
}

/* Re-select through each original ROOT token, never through a newer root. */
static bool
cold_sources_match(ClusterColdTypedV1 *typed, ClusterRecoveryFencePlan *fence,
				   const uint64 *deadline)
{
	bool profiles_current;
	if (fence == NULL
		|| (uint32)cluster_recovery_merge_fence_plan_origin_count(fence) + 1 != typed->replay_count)
		return false;
	for (uint32 i = 0; i < typed->replay_count; i++) {
		const ColdRoot *selected = &typed->replay_roots[i];
		const ClusterWalSourceRef *expected = &typed->sources[i];
		const ClusterColdParticipantV1 *participant = &typed->participants[i];
		ClusterWalRecoveryCheckpointV1 observed;
		ClusterControlRootResult result;
		const ClusterWalSourceRef *source = &observed.source;

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
		cold_prepare_deadline_check(deadline);
		result = deadline != NULL ? cluster_control_root_recovery_checkpoint_deadline_v1(
										&selected->root, &selected->token, &observed, *deadline)
								  : cluster_control_root_recovery_checkpoint_v1(
										&selected->root, &selected->token, &observed);
		cold_prepare_deadline_check(deadline);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
			|| !cluster_control_root_identity_equal(&source->claim.identity,
													&expected->claim.identity)
			|| source->claim.database_incarnation != expected->claim.database_incarnation
			|| source->claim.max_config_generation != expected->claim.max_config_generation
			|| memcmp(source->claim.claim_sha256, expected->claim.claim_sha256, 32) != 0
			|| source->timeline != expected->timeline
			|| participant->thread_id != selected->root.identity.origin_thread_id
			|| participant->owner_incarnation != selected->root.identity.origin_owner_incarnation
			|| participant->timeline != selected->root.checkpoint_tli
			|| participant->physical_lower != selected->root.checkpoint_lower_lsn
			|| participant->tail_end != selected->root.validated_tail_lsn_exclusive
			|| participant->native_redo != observed.checkpoint.redo
			|| selected->checkpoint_start != observed.checkpoint_start
			|| memcmp(&selected->native_checkpoint, &observed.checkpoint,
					  sizeof(observed.checkpoint))
				   != 0
			|| memcmp(selected->native_window, observed.native_window,
					  sizeof(observed.native_window))
				   != 0)
			return false;
	}
	profiles_current = cold_native_profiles_match(typed);
	cold_prepare_deadline_check(deadline);
	return profiles_current;
}

/* A retained history participant has no native redo to apply.  Its sealed
 * SIDE plan is still part of the original source qualification: every
 * retained operation must remain history-only, and the plan must match the
 * same physical cut and replay start before pass 2 can use that source. */
static bool
cold_history_side_effects_match(const ClusterColdTypedV1 *typed, uint32 participant)
{
	const ClusterColdParticipantV1 *source;
	const RfSideOnlinePlanV1 *plan;
	RfContributorStreamCutV1 cut = {0};
	uint32 operations;

	if (typed->plan == NULL || participant >= typed->participant_count
		|| typed->side_capture_detail[participant] != 0
		|| (plan = typed->side_plans[participant]) == NULL)
		return false;
	source = &typed->participants[participant];
	cut.failed_thread = source->thread_id;
	cut.timeline_id = source->timeline;
	cut.origin_owner_incarnation = source->owner_incarnation;
	cut.scan_begin_inclusive = source->physical_lower;
	cut.scan_end_exclusive = source->tail_end;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE
				| (source->physical_lower == source->tail_end
					   ? RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY
					   : 0);
	if (!rf_side_online_plan_source_matches_v1(plan, typed->system_identifier,
												  typed->observer.storage_uuid, &cut)
		|| !rf_side_online_plan_replay_start_matches_v1(
			plan, source->thread_id, source->owner_incarnation,
			typed->observer.database_incarnation, source->native_redo)
		|| !cluster_cold_plan_participant_matches_v1(typed->plan, participant, source)
		|| cluster_cold_plan_replay_record_count_v1(typed->plan, participant) != 0)
		return false;
	operations = rf_side_online_plan_operation_count_v1(plan);
	for (uint32 i = 0; i < operations; i++) {
		RfSideOnlineOperationV1 operation;

		if (!rf_side_online_plan_operation_v1(plan, i, &operation)
			|| !operation.history_only || operation.purpose != RF_SIDE_PLAN_RECOVERY)
			return false;
	}
	return true;
}

/* Independent prepared copies, checked only under the original owners.
 * Physical source drift is checked after exact IR release, never here. */
static bool
cold_captured_sources_match(const ClusterColdTypedV1 *typed, ClusterRecoveryFencePlan *fence)
{
	if (fence == NULL || typed->replay_roots == NULL || typed->own_participant != 0
		|| typed->replay_count == 0 || typed->replay_count > typed->participant_count
		|| typed->participant_count > CLUSTER_COLD_MAX_PARTICIPANTS
		|| typed->replay_root_count != typed->replay_count || !typed->census_root_captured
		|| typed->inputs != NULL || typed->plan == NULL
		|| cluster_cold_plan_participant_count_v1(typed->plan) != typed->participant_count
		|| typed->replay_count != (uint32)cluster_recovery_merge_fence_plan_origin_count(fence) + 1)
		return false;
	for (uint32 i = 0; i < typed->participant_count; ++i) {
		const ColdRoot *selected = &typed->replay_roots[i];
		const ClusterColdParticipantV1 *participant = &typed->participants[i];
		if (!selected->checkpoint_captured
			|| memcmp(&selected->source, &typed->sources[i], sizeof(selected->source)) != 0
			|| !cluster_control_root_identity_equal(&selected->source.claim.identity,
													&selected->root.identity)
			|| selected->source.timeline != selected->root.checkpoint_tli
			|| participant->thread_id != selected->root.identity.origin_thread_id
			|| participant->owner_incarnation != selected->root.identity.origin_owner_incarnation
			|| participant->timeline != selected->root.checkpoint_tli
			|| participant->physical_lower != selected->root.checkpoint_lower_lsn
			|| participant->tail_end != selected->root.validated_tail_lsn_exclusive
			|| (i < typed->replay_count
				&& participant->native_redo != selected->native_checkpoint.redo)
			|| !cluster_cold_plan_participant_matches_v1(typed->plan, i, participant))
			return false;
		if (i > 0 && i < typed->replay_count) {
			ClusterControlRootSnapshot root;
			ClusterControlRootReadToken token;
			uint16 thread;
			if (!cluster_recovery_merge_fence_plan_origin(fence, i - 1, &thread, &root, &token)
				|| thread != participant->thread_id
				|| memcmp(&root, &selected->root, sizeof(root)) != 0
				|| memcmp(&token, &selected->token, sizeof(token)) != 0)
				return false;
		}
		if (i >= typed->replay_count
			&& (participant->native_redo != participant->tail_end
				|| !cold_history_side_effects_match(typed, i)))
			return false;
	}
	return true;
}

static void
cold_execution_inputs_failed(ClusterColdTypedV1 *typed)
{
	memset(&typed->xid_receipt, 0, sizeof(typed->xid_receipt));
	memset(&typed->effects_receipt, 0, sizeof(typed->effects_receipt));
	cluster_cold_tt_after_redo_reset_v1(typed);
	if (typed->refusal == CLUSTER_COLD_OK)
		cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP, "pass-2 original input freshness is incomplete");
	memset(typed->history_xid, 0, sizeof(typed->history_xid));
	memset(typed->xid_next_at_drain, 0, sizeof(typed->xid_next_at_drain));
	typed->xid_next_after_redo = 0;
	typed->census_root_captured = false;
}

/* Pass 1 has not acquired the serial set. Execution revalidation belongs
 * only to pass 2, around the same complete original-source comparison. */
static bool
cold_replay_sources_current(ClusterColdTypedV1 *typed, ClusterRecoveryFencePlan *fence)
{
	bool matched = false;

	PG_TRY();
	{
		matched = cluster_recovery_merge_fence_plan_revalidate_nowait(fence)
				  && cold_captured_sources_match(typed, fence) && cold_native_profiles_match(typed)
				  && cluster_recovery_merge_fence_plan_revalidate_nowait(fence);
	}
	PG_CATCH();
	{
		cold_execution_inputs_failed(typed);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (!matched)
		cold_execution_inputs_failed(typed);
	return matched;
}

/* C0 input consumption must use the original execution owner, not a cached
 * source receipt or a caller's isolation flag. It cannot outlive pass 2. */
bool
cluster_cold_typed_sources_current_v1(ClusterColdTypedV1 *typed, ClusterRecoveryFencePlan *fence)
{
	if (typed == NULL || typed->refusal != CLUSTER_COLD_OK)
		return false;
	if (fence == NULL || !cluster_cold_replay_window_active_v1() || typed->plan == NULL
		|| typed->replay_roots == NULL || typed->own_participant != 0 || typed->replay_count == 0
		|| typed->replay_count > typed->participant_count
		|| typed->participant_count > CLUSTER_COLD_MAX_PARTICIPANTS
		|| typed->replay_root_count != typed->replay_count
		|| cluster_cold_plan_participant_count_v1(typed->plan) != typed->participant_count) {
		cold_execution_inputs_failed(typed);
		return false;
	}
	return cold_replay_sources_current(typed, fence);
}

/* Original merge caller, after exact IR release and before publication.
 * Waits use original Startup/CF ownership, without any replay retry/budget. */
bool
cluster_cold_typed_sources_readback_v1(void *arg, ClusterRecoveryFencePlan *fence)
{
	ClusterColdTypedV1 *typed = arg;
	bool matched = false;
	if (typed == NULL || typed->refusal != CLUSTER_COLD_OK)
		return false;
	PG_TRY();
	{
		matched = cluster_recovery_merge_fence_plan_publish_current_v1(fence)
				  && cold_captured_sources_match(typed, fence);
		for (uint32 i = 0; matched && i < typed->participant_count; ++i) {
			const ColdRoot *selected = &typed->replay_roots[i];
			ClusterWalRecoveryCheckpointV1 observed;
			matched = cluster_wal_inputs_cold_checkpoint_readback_v1(&selected->root,
																	 &typed->census_root, &observed)
						  == CLUSTER_CONTROL_ROOT_OK_PRIMARY
					  && memcmp(&observed.source, &selected->source, sizeof(observed.source)) == 0
					  && observed.checkpoint_start == selected->checkpoint_start
					  && memcmp(&observed.checkpoint, &selected->native_checkpoint,
								sizeof(observed.checkpoint))
							 == 0
					  && memcmp(observed.native_window, selected->native_window,
								sizeof(observed.native_window))
							 == 0
					  && cluster_recovery_merge_fence_plan_publish_current_v1(fence);
		}
		matched = matched && cold_captured_sources_match(typed, fence)
				  && cold_native_profiles_match(typed)
				  && cluster_recovery_merge_fence_plan_publish_current_v1(fence);
	}
	PG_CATCH();
	{
		cold_execution_inputs_failed(typed);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (!matched)
		cold_execution_inputs_failed(typed);
	return matched;
}

bool
cluster_cold_typed_sync_sources_v1(ClusterColdTypedV1 *typed, ClusterRecoveryFencePlan *fence)
{
	volatile bool synced = true;

	if (typed == NULL || typed->refusal != CLUSTER_COLD_OK)
		return false;
	/* ERROR from an original source owner must not leave a reusable OK plan. */
	cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP, "pass-2 source durability is incomplete");
	memset(&typed->xid_receipt, 0, sizeof(typed->xid_receipt));
	memset(&typed->effects_receipt, 0, sizeof(typed->effects_receipt));
	cluster_cold_tt_after_redo_reset_v1(typed);
	typed->xid_next_after_redo = 0;
	memset(typed->source_drained, 0, sizeof(typed->source_drained));
	memset(typed->xid_next_at_drain, 0, sizeof(typed->xid_next_at_drain));
	if (fence == NULL || !cluster_cold_replay_window_active_v1() || typed->plan == NULL
		|| typed->replay_roots == NULL || typed->own_participant != 0 || typed->replay_count == 0
		|| typed->replay_count > typed->participant_count
		|| typed->participant_count > CLUSTER_COLD_MAX_PARTICIPANTS
		|| typed->replay_root_count != typed->replay_count
		|| cluster_cold_plan_participant_count_v1(typed->plan) != typed->participant_count
		|| !cold_replay_sources_current(typed, fence))
		return false;

	PG_TRY();
	{
		/* Every input uses the original owner, with physical I/O outside CF.
		 * History is read-only input, not a replay frontier or recovery duty. */
		for (uint32 i = 0; i < typed->participant_count; i++) {
			const ColdRoot *selected = &typed->replay_roots[i];
			ClusterWalTailObservation observed;
			ClusterControlRootResult result;

			result = cluster_wal_tail_sync_sealed_v1(cluster_wal_threads_dir, &selected->source,
													 wal_segment_size, &selected->root,
													 selected->checkpoint_start, &observed);
			if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
				|| observed.complete_end != selected->root.validated_tail_lsn_exclusive
				|| observed.last_record_start != selected->root.tail_last_record_lsn
				|| observed.last_record_crc != selected->root.tail_last_record_crc32c
				|| observed.database_incarnation != typed->sources[i].claim.database_incarnation
				|| !cluster_recovery_merge_fence_plan_revalidate_nowait(fence)) {
				cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP,
							"thread %u pass-2 source sync refused (root result %d)",
							(unsigned)typed->participants[i].thread_id, (int)result);
				synced = false;
				break;
			}
		}
		/* A later source's IO must not hide earlier source or scope drift. */
		if (synced)
			synced = cluster_cold_tt_base_recheck_v1(typed) == CLUSTER_COLD_OK
					 && cold_replay_sources_current(typed, fence);
	}
	PG_CATCH();
	{
		cold_execution_inputs_failed(typed);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (!synced) {
		cold_execution_inputs_failed(typed);
		return false;
	}
	typed->refusal = CLUSTER_COLD_OK;
	typed->refusal_detail[0] = '\0';
	return true;
}

/* The complete original scan already matched these exact bytes against the
 * actual checkpoint record and its end. Recheck that retained input here;
 * parsing its end is not an independent WAL or source qualification. */
static bool
cold_xid_window_minimum(const ClusterColdTypedV1 *typed, uint32 p, uint64 *next)
{
	const ColdRoot *selected = &typed->replay_roots[p];
	const ClusterWalSourceRef *source = &typed->sources[p];
	const uint8 *bytes = selected->native_window;
	const uint8 zero[160] = { 0 };
	ClusterRelmapCheckpointWindow window;
	XLogRecPtr end = 0;
	uint64 issued;
	FullTransactionId after;

	if (memcmp(bytes, zero, sizeof(zero)) == 0)
		return true;
	for (unsigned i = 0; i < 8; i++)
		end |= (uint64)bytes[88 + i] << (8 * i);
	if (selected->checkpoint_start < selected->root.checkpoint_lower_lsn
		|| selected->checkpoint_start >= end || end > selected->root.validated_tail_lsn_exclusive)
		return false;
	if (memcmp(bytes, "RMWIN002", 8) != 0)
		return cluster_relmap_checkpoint_window_decode_v1(
			bytes, &selected->native_checkpoint, selected->checkpoint_start, end,
			&source->claim.identity, source->claim.database_incarnation, &window);
	if (!cluster_relmap_checkpoint_window_decode_v2(
			bytes, &selected->native_checkpoint, selected->checkpoint_start, end,
			&source->claim.identity, source->claim.database_incarnation, &window, &issued)
		|| issued == UINT64_MAX)
		return false;
	/* This actual post-flush issuance can be later than checkpoint nextXid.
	 * Keep the original counted WAL fold, adding only a monotonic minimum. */
	after = FullTransactionIdFromU64(issued);
	FullTransactionIdAdvance(&after);
	if (!TransactionIdIsNormal(XidFromFullTransactionId(after))
		|| U64FromFullTransactionId(after) <= issued)
		return false;
	*next = Max(*next, U64FromFullTransactionId(after));
	return true;
}

static bool
cold_xid_endpoint(const ClusterColdTypedV1 *typed, const ClusterColdReplayResultV1 *result,
				  const ClusterColdXidProgressV1 *progress, uint32 p, uint64 *next)
{
	const ColdRoot *selected = &typed->replay_roots[p];
	const ClusterControlRootSnapshot *root = &selected->root;
	ClusterColdParticipantV1 source = typed->participants[p];
	const ClusterColdXidProgressV1 *xid = &progress[p];
	uint64 records = cluster_cold_plan_replay_record_count_v1(typed->plan, p);

	if (!selected->checkpoint_captured
		|| !cluster_cold_plan_participant_matches_v1(typed->plan, p, &source))
		return false;
	if (p >= typed->replay_count) {
		/* A zero-record tail mutation cut cannot supply the original
		 * checkpoint-to-tail counter coverage of a history generation. */
		if (records != 0 || result->last_read[p] != InvalidXLogRecPtr
			|| result->last_end[p] != InvalidXLogRecPtr || result->last_crc[p] != 0)
			return false;
		source.native_redo = selected->native_checkpoint.redo;
		xid = &typed->history_xid[p];
		if (source.native_redo != source.tail_end) {
			if (progress[p].begun || progress[p].failed || typed->xid_next_at_drain[p] != 0)
				return false;
		} else {
			uint64 drained;
			if (progress[p].expected_records != 0 || progress[p].last_read != InvalidXLogRecPtr
				|| !cluster_cold_xid_candidate_v1(&progress[p], &source, &drained)
				|| drained != typed->xid_next_at_drain[p] || drained != xid->next_xid)
				return false;
		}
	} else if (selected->native_checkpoint.redo != source.native_redo
			   || xid->expected_records != records || xid->last_read != result->last_read[p]
			   || result->last_end[p] != (records == 0 ? InvalidXLogRecPtr : xid->last_end)
			   || typed->xid_next_at_drain[p] != xid->next_xid
			   || (records == 0 && result->last_crc[p] != 0)
			   || (records != 0 && result->last_crc[p] != root->tail_last_record_crc32c))
		return false;
	if (!cluster_cold_xid_candidate_v1(xid, &source, next))
		return false;
	if (source.native_redo == source.tail_end)
		return xid->records == 0 && xid->last_read == InvalidXLogRecPtr;
	return xid->records != 0
		   && (root->root_flags & CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID) != 0
		   && xid->last_read == root->tail_last_record_lsn;
}

bool
cluster_cold_typed_xid_endpoints_v1(ClusterColdTypedV1 *typed, ClusterRecoveryFencePlan *fence,
									const ClusterColdReplayResultV1 *result,
									const ClusterColdXidProgressV1 *progress)
{
	uint64 maximum = 0;
	volatile bool ready = false;

	if (typed == NULL)
		return false;
	memset(&typed->xid_receipt, 0, sizeof(typed->xid_receipt));
	cluster_cold_tt_after_redo_reset_v1(typed);
	typed->xid_next_after_redo = 0;
	if (typed->refusal != CLUSTER_COLD_OK)
		return false;
	cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP, "pass-2 original counter endpoints are incomplete");
	if (fence == NULL || !cluster_cold_replay_window_active_v1() || result == NULL
		|| progress == NULL || typed->plan == NULL || typed->replay_roots == NULL
		|| typed->own_participant != 0 || typed->replay_count == 0
		|| typed->replay_count > typed->participant_count
		|| typed->participant_count > CLUSTER_COLD_MAX_PARTICIPANTS
		|| typed->replay_root_count != typed->replay_count
		|| cluster_cold_plan_participant_count_v1(typed->plan) != typed->participant_count
		|| result->detail != CLUSTER_COLD_REPLAY_OK
		|| result->steps_done != cluster_cold_plan_step_count_v1(typed->plan)
		|| result->space_relations_finished
			   != cluster_cold_plan_space_relation_count_v1(typed->plan)
		|| result->own_read != result->last_read[0] || result->own_end != result->last_end[0]) {
		cold_execution_inputs_failed(typed);
		return false;
	}
	PG_TRY();
	{
		/* Endpoint consumers run after source sync. Revalidate the retained
		 * original TT base before deriving post-redo endpoint inputs. */
		ready = cluster_cold_tt_base_recheck_v1(typed) == CLUSTER_COLD_OK
				&& cold_replay_sources_current(typed, fence);
		for (uint32 p = 0; ready && p < typed->participant_count; p++) {
			uint64 next;
			ready = cold_xid_endpoint(typed, result, progress, p, &next)
					&& cold_xid_window_minimum(typed, p, &next);
			if (ready) {
				typed->xid_receipt.next_xid[p] = next;
				if (next > maximum)
					maximum = next;
			}
		}
		if (ready)
			ready = cold_replay_sources_current(typed, fence);
		/* The original executor has completed redo and its physical durability
		 * barrier. Retain separate raw TT bytes, then recheck both files and
		 * every original ROOT/claim/checkpoint before exposing attempt inputs. */
		if (ready)
			ready = cluster_cold_tt_after_redo_capture_v1(typed) == CLUSTER_COLD_OK
					&& cold_replay_sources_current(typed, fence);
		if (ready) {
			/* Guarded capture readers require OK. Scope that access to this
			 * original owner's inventory check; final readback is still due.
			 * ERROR takes the same attempt-input cleanup path below. */
			typed->refusal = CLUSTER_COLD_OK;
			ready = cluster_cold_typed_tt_redo_inventory_v1(typed);
			cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP,
						"pass-2 TT inventory final readback is incomplete");
		}
		if (ready)
			ready = cluster_cold_tt_after_redo_recheck_v1(typed) == CLUSTER_COLD_OK
					&& cold_replay_sources_current(typed, fence);
	}
	PG_CATCH();
	{
		cold_execution_inputs_failed(typed);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (!ready) {
		cold_execution_inputs_failed(typed);
		return false;
	}
	typed->xid_next_after_redo = maximum;
	memcpy(typed->xid_receipt.sources, typed->sources,
		   typed->participant_count * sizeof(typed->sources[0]));
	typed->xid_receipt.system_identifier = typed->system_identifier;
	typed->xid_receipt.participant_count = typed->participant_count;
	typed->xid_receipt.replay_count = typed->replay_count;
	/* Publish last, after the original owner's final source/TT readback.
	 * The receipt survives EOR only as input, without the released fence. */
	typed->xid_receipt.plan = typed->plan;
	typed->refusal = CLUSTER_COLD_OK;
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
		|| progress->steps_done > cluster_cold_plan_step_count_v1(typed->plan)
		|| records != cluster_cold_plan_replay_record_count_v1(typed->plan, participant)
		|| !cold_replay_sources_current(typed, fence))
		return false;
	/* Only this source may drain early. The original global completion
	 * predicate still requires every scheduled step and final durability. */
	for (uint32 i = progress->steps_done; i < cluster_cold_plan_step_count_v1(typed->plan); i++) {
		ClusterColdStepV1 step;
		if (!cluster_cold_plan_step_v1(typed->plan, i, &step) || step.participant == participant)
			return false;
	}
	/* Founder/history/empty inputs do not acquire target mutation or base
	 * coverage here. Their existing final completion owners remain required. */
	if (participant < typed->replay_count && participant != typed->own_participant
		&& records != 0) {
		const ClusterControlRootSnapshot *root = &typed->replay_roots[participant].root;
		if (root->lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
			|| (root->root_flags & CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID) == 0
			|| root->tail_last_record_lsn == InvalidXLogRecPtr
			|| progress->last_read[participant] != root->tail_last_record_lsn
			|| progress->last_crc[participant] != root->tail_last_record_crc32c
			|| progress->last_end[participant] != root->validated_tail_lsn_exclusive)
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
			&& !(record->rmid == RM_XACT_ID
				 && ((info == XLOG_XACT_COMMIT && record->space_count > 0)
					 || (info == XLOG_XACT_ABORT && record->space_count == 0))
				 && (record->info & XLR_INFO_MASK) == XLR_SPECIAL_REL_UPDATE)))
		return RF_SIDE_ONLINE_OPERATION_INVALID;
	if (record->rmid == RM_CLUSTER_UNDO_ID && info == XLOG_UNDO_RECOVERY_TT_ABORT)
		return RF_SIDE_ONLINE_OPERATION_RECOVERY_TT_ABORT;
	if (record->rmid == RM_CLUSTER_XID_STRIPE_ID && info == XLOG_CLUSTER_XID_STRIPE_JOIN)
		return RF_SIDE_ONLINE_OPERATION_NATIVE_CONTROL;
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
	if ((record->rmid == RM_CLOG_ID
		 && (info == CLOG_ZEROPAGE || info == CLOG_TRUNCATE || info == CLOG_ZEROPAGE_FULL
			 || info == CLOG_TRUNCATE_FULL))
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

bool
cluster_cold_typed_old_writer_isolated_v1(ClusterColdTypedV1 *typed,
										  ClusterRecoveryFencePlan *fence, uint32 participant)
{
	ClusterThreadRecoveryAuthorityV1 authority = { 0 };

	if (typed == NULL || typed->refusal != CLUSTER_COLD_OK)
		return false;
	/* Reuse the original real isolation authority, never self-seal or a
	 * source-current receipt. This borrows no authority for own/history. */
	return cold_participant_authority(typed, fence, participant, &authority);
}

/* Match the original complete SIDE capture independently of writer kind. */
static bool
cold_side_source_matches(ClusterColdTypedV1 *typed, uint32 participant)
{
	const ClusterColdParticipantV1 *source;
	RfContributorStreamCutV1 cut = { 0 };
	if (participant >= typed->participant_count || participant >= CLUSTER_COLD_MAX_PARTICIPANTS
		|| typed->side_capture_detail[participant] != 0 || typed->side_plans[participant] == NULL)
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

/* Rebind the same immutable whole-source authority for record execution
 * and for the sequencer's post-drain physical durability check. */
static bool
cold_side_source_authority(ClusterColdTypedV1 *typed, ClusterRecoveryFencePlan *fence,
	uint32 participant, ClusterThreadRecoveryAuthorityV1 *authority)
{
	return cold_side_source_matches(typed, participant)
		&& cold_participant_authority(typed, fence, participant, authority);
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

/* A retained C0 in our restart stream is not a foreign duty. Its writer
 * remains the original restart claim/ROOT; only the target borrows the
 * independent cold authority. Ordinary local SIDE records keep their owner. */
static bool
cold_c0_source_authority(ClusterColdTypedV1 *typed, ClusterRecoveryFencePlan *fence,
	uint32 participant, ClusterThreadRecoveryAuthorityV1 *authority)
{
	ClusterWalSourceRef restart;

	if (participant != typed->own_participant)
		return cold_side_source_authority(typed, fence, participant, authority);
	memset(authority, 0, sizeof(*authority));
	return fence != NULL && cluster_cold_replay_window_active_v1() && typed->plan != NULL
		&& typed->replay_roots != NULL && typed->own_participant == 0 && typed->replay_count != 0
		&& typed->replay_count <= typed->participant_count
		&& typed->participant_count <= CLUSTER_COLD_MAX_PARTICIPANTS
		&& typed->replay_root_count == typed->replay_count
		&& cold_replay_sources_current(typed, fence) && cold_side_source_matches(typed, participant)
		&& cluster_wal_thread_restart_v2_ref(&restart)
		&& cold_source_ref_matches(&restart, &typed->sources[participant])
		&& cold_replay_sources_current(typed, fence);
}

typedef struct ColdNativeClogReplay {
	ClusterColdTypedV1 *typed;
	ClusterRecoveryFencePlan *fence;
	const RfSideOnlinePlanV1 *plan;
	ClusterWalSourceRef source;
	ClusterColdParticipantV1 participant;
	XLogReaderState *reader;
	const DecodedXLogRecord *record;
	void (*redo)(XLogReaderState *, void *);
	void *arg;
} ColdNativeClogReplay;

static ColdNativeClogReplay *cold_native_clog_active;

/* Only the original callback can supply this read-only record context. Full
 * source/ROOT I/O stays in the original lock-free bracket below; native CLOG
 * may check these immutable bytes while holding its existing short locks. */
bool
cluster_cold_typed_clog_native_record_current_v1(int origin, const XLogReaderState *reader)
{
	const ColdNativeClogReplay *replay = cold_native_clog_active;
	const ClusterColdTypedV1 *typed;

	if (replay == NULL || reader == NULL || replay->reader != reader
		|| reader->record == NULL || replay->record != reader->record)
		return false;
	typed = replay->typed;
	return origin >= 0 && origin <= SCN_MAX_VALID_NODE_ID && origin == cluster_node_id
		&& typed->refusal == CLUSTER_COLD_SIDE_OWNER_MISSING
		&& replay->fence != NULL && cluster_cold_replay_window_active_v1()
		&& typed->plan != NULL && typed->replay_roots != NULL
		&& typed->own_participant == 0 && typed->replay_count != 0
		&& typed->replay_count <= typed->participant_count
		&& typed->participant_count <= CLUSTER_COLD_MAX_PARTICIPANTS
		&& typed->replay_root_count == typed->replay_count
		&& replay->plan != NULL && typed->side_plans[0] == replay->plan
		&& typed->side_capture_detail[0] == 0
		&& replay->source.claim.identity.origin_node_id == origin
		&& replay->source.claim.identity.origin_thread_id == origin + 1
		&& memcmp(&typed->sources[0], &replay->source, sizeof(replay->source)) == 0
		&& memcmp(&typed->participants[0], &replay->participant, sizeof(replay->participant)) == 0
		&& rf_side_online_plan_clog_matches_v1(replay->plan, &replay->source, reader);
}

/* This is the founder's existing native path, not foreign SIDE/C0 authority.
 * Its original restart generation and complete held source set must survive
 * the native call unchanged. No independent recovery scope is acquired. */
static bool
cold_native_clog_current(void *arg)
{
	ColdNativeClogReplay *replay = arg;
	ClusterColdTypedV1 *typed = replay->typed;
	ClusterWalSourceRef restart;

	return typed->refusal == CLUSTER_COLD_SIDE_OWNER_MISSING
		&& replay->fence != NULL && cluster_cold_replay_window_active_v1()
		&& typed->plan != NULL && typed->replay_roots != NULL
		&& typed->own_participant == 0 && typed->replay_count != 0
		&& typed->replay_count <= typed->participant_count
		&& typed->participant_count <= CLUSTER_COLD_MAX_PARTICIPANTS
		&& typed->replay_root_count == typed->replay_count
		&& typed->side_plans[0] == replay->plan && replay->plan != NULL
		&& typed->side_capture_detail[0] == 0 && cluster_node_id >= 0
		&& cluster_node_id <= SCN_MAX_VALID_NODE_ID
		&& replay->source.claim.identity.origin_node_id == cluster_node_id
		&& replay->source.claim.identity.origin_thread_id == cluster_node_id + 1
		&& memcmp(&typed->sources[0], &replay->source, sizeof(replay->source)) == 0
		&& memcmp(&typed->participants[0], &replay->participant, sizeof(replay->participant)) == 0
		&& cold_replay_sources_current(typed, replay->fence)
		&& cold_side_source_matches(typed, 0)
		&& cluster_wal_thread_restart_v2_ref(&restart)
		&& cold_source_ref_matches(&restart, &replay->source)
		&& cold_replay_sources_current(typed, replay->fence);
}

static void
cold_native_clog_redo(XLogReaderState *reader, void *arg)
{
	ColdNativeClogReplay *replay = arg;

	if (cold_native_clog_active != NULL || reader != replay->reader)
		ereport(ERROR, (errmsg("native CLOG replay callback context changed")));
	cold_native_clog_active = replay;
	PG_TRY();
	{
		replay->redo(reader, replay->arg);
	}
	PG_FINALLY();
	{
		cold_native_clog_active = NULL;
	}
	PG_END_TRY();
}

bool
cluster_cold_typed_clog_native_v1(ClusterColdTypedV1 *typed,
	ClusterRecoveryFencePlan *fence, XLogReaderState *reader,
	void (*redo)(XLogReaderState *, void *), void *arg)
{
	ColdNativeClogReplay replay = { .typed = typed, .fence = fence, .redo = redo, .arg = arg };

	if (typed == NULL || typed->refusal != CLUSTER_COLD_OK || cold_native_clog_active != NULL)
		return false;
	/* Preserve the whole episode on ERROR or a late false, just as the
	 * original foreign record owner does. This is never COMPLETE. */
	cold_refuse(typed, CLUSTER_COLD_SIDE_OWNER_MISSING,
		"pass-2 native CLOG record consumption is incomplete");
	if (typed->own_participant != 0 || typed->participant_count == 0
		|| typed->participant_count > CLUSTER_COLD_MAX_PARTICIPANTS
		|| reader == NULL || reader->record == NULL || redo == NULL
		|| XLogRecGetRmid(reader) != RM_CLOG_ID)
		return false;
	replay.plan = typed->side_plans[0];
	replay.reader = reader;
	replay.record = reader->record;
	memcpy(&replay.source, &typed->sources[0], sizeof(replay.source));
	memcpy(&replay.participant, &typed->participants[0], sizeof(replay.participant));
	if (!side_plan_clog_native_replay(replay.plan, &replay.source, reader,
			cold_native_clog_redo, cold_native_clog_current, &replay))
		return false;
	typed->refusal = CLUSTER_COLD_OK;
	typed->refusal_detail[0] = '\0';
	return true;
}

/* Resolve the original bound identities, never source-array proximity. */
static uint32
cold_recovery_abort_target(const ClusterColdTypedV1 *typed, uint32 participant, uint32 index)
{
	ClusterWalSourceRef writer, target;
	uint32 match = UINT32_MAX;

	if (!rf_side_online_plan_recovery_abort_sources_v1(typed->side_plans[participant], index,
													   &writer, &target)
		|| !cold_source_ref_matches(&writer, &typed->sources[participant]))
		return UINT32_MAX;
	for (uint32 i = 0; i < typed->participant_count; i++) {
		if (!cold_source_ref_matches(&target, &typed->sources[i]))
			continue;
		if (match != UINT32_MAX)
			return UINT32_MAX;
		match = i;
	}
	return match;
}

/* Resolve only a complete captured record. The sequencer may use the
 * index to finish a ready target, but execution still rebinds both owners. */
bool
cluster_cold_typed_record_target_v1(ClusterColdTypedV1 *typed, ClusterRecoveryFencePlan *fence,
									uint32 participant, const ClusterColdRecordV1 *record,
									uint32 *target)
{
	const ClusterColdParticipantV1 *source;
	const RfSideOnlinePlanV1 *plan;
	ClusterThreadRecoveryAuthorityV1 authority;
	RfPageOnlineRecordIdentityV1 identity = { 0 };
	uint32 match = UINT32_MAX, resolved;

	if (target != NULL)
		*target = CLUSTER_COLD_NO_INDEX;
	if (typed == NULL || typed->refusal != CLUSTER_COLD_OK)
		return false;
	if (target == NULL || record == NULL || participant >= typed->participant_count) {
		cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP, "pass-2 record dependency is invalid");
		return false;
	}
	if (record->rmid != RM_CLUSTER_UNDO_ID
		|| (record->info & ~XLR_INFO_MASK) != XLOG_UNDO_RECOVERY_TT_ABORT)
		return true;
	cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP, "pass-2 C0 target dependency is incomplete");
	if (cold_record_side_kind(record) != RF_SIDE_ONLINE_OPERATION_RECOVERY_TT_ABORT
		|| record->component_count != 0 || record->space_count != 0
		|| !cold_c0_source_authority(typed, fence, participant, &authority))
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
	for (uint32 i = 0; i < rf_side_online_plan_operation_count_v1(plan); i++) {
		RfSideOnlineOperationV1 operation;
		if (!rf_side_online_plan_operation_v1(plan, i, &operation))
			return false;
		if (operation.identity.record.read_rec_ptr != record->read_rec_ptr)
			continue;
		if (match != UINT32_MAX || operation.history_only
			|| operation.kind != RF_SIDE_ONLINE_OPERATION_RECOVERY_TT_ABORT
			|| operation.purpose != RF_SIDE_PLAN_RECOVERY
			|| memcmp(&operation.identity, &identity, sizeof(identity)) != 0
			|| !cluster_recovery_tt_abort_valid_v1(&operation.recovery_abort))
			return false;
		match = i;
	}
	if (match == UINT32_MAX
		|| (resolved = cold_recovery_abort_target(typed, participant, match)) == UINT32_MAX
		|| !cold_participant_authority(typed, fence, resolved, &authority)
		|| !cold_replay_sources_current(typed, fence))
		return false;
	*target = resolved;
	typed->refusal = CLUSTER_COLD_OK;
	typed->refusal_detail[0] = '\0';
	return true;
}

/* Order the original remainder drains. The sequencer may also drain an
 * exact C0 target whose scheduled steps already ran; PAGE/SPACE order and
 * each source's record order remain unchanged. */
bool
cluster_cold_typed_drain_order_v1(ClusterColdTypedV1 *typed, ClusterRecoveryFencePlan *fence,
								  uint32 *order, uint32 count)
{
	bool depends[CLUSTER_COLD_MAX_PARTICIPANTS][CLUSTER_COLD_MAX_PARTICIPANTS] = { 0 };
	bool emitted[CLUSTER_COLD_MAX_PARTICIPANTS] = { 0 };
	uint32 result[CLUSTER_COLD_MAX_PARTICIPANTS];

	if (order != NULL && count <= CLUSTER_COLD_MAX_PARTICIPANTS)
		for (uint32 i = 0; i < count; i++)
			order[i] = UINT32_MAX;
	if (typed == NULL || typed->refusal != CLUSTER_COLD_OK)
		return false;
	cold_refuse(typed, CLUSTER_COLD_SOURCE_GAP, "pass-2 C0 source ordering is incomplete");
	if (order == NULL || count == 0 || count > CLUSTER_COLD_MAX_PARTICIPANTS
		|| count != typed->participant_count || typed->plan == NULL
		|| cluster_cold_plan_participant_count_v1(typed->plan) != count
		|| typed->own_participant != 0 || typed->replay_count == 0 || typed->replay_count > count
		|| typed->replay_roots == NULL || typed->replay_root_count != typed->replay_count
		|| fence == NULL || !cluster_cold_replay_window_active_v1()
		|| !cold_replay_sources_current(typed, fence))
		return false;
	for (uint32 p = 0; p < typed->replay_count; p++) {
		const ClusterColdParticipantV1 *source = &typed->participants[p];
		const RfSideOnlinePlanV1 *plan = typed->side_plans[p];
		RfContributorStreamCutV1 cut = { 0 };
		uint32 operations;

		cut.failed_thread = source->thread_id;
		cut.timeline_id = source->timeline;
		cut.origin_owner_incarnation = source->owner_incarnation;
		cut.scan_begin_inclusive = source->physical_lower;
		cut.scan_end_exclusive = source->tail_end;
		cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		if (plan == NULL || typed->side_capture_detail[p] != 0
			|| !rf_side_online_plan_source_matches_v1(plan, typed->system_identifier,
													  typed->observer.storage_uuid, &cut)
			|| !rf_side_online_plan_replay_start_matches_v1(
				plan, source->thread_id, source->owner_incarnation,
				typed->observer.database_incarnation, source->native_redo))
			return false;
		operations = rf_side_online_plan_operation_count_v1(plan);
		for (uint32 i = 0; i < operations; i++) {
			RfSideOnlineOperationV1 operation;
			uint32 target;

			if (!rf_side_online_plan_operation_v1(plan, i, &operation))
				return false;
			if (operation.kind != RF_SIDE_ONLINE_OPERATION_RECOVERY_TT_ABORT)
				continue;
			target = cold_recovery_abort_target(typed, p, i);
			/* The existing target owner grants neither founder nor history
			 * mutation. Ordering cannot manufacture that authority or use
			 * writer checkpoint coverage to skip a retained C0. */
			if (operation.history_only || target >= typed->replay_count
				|| target == typed->own_participant
				|| operation.identity.record.read_rec_ptr < source->native_redo
				|| operation.identity.record.end_rec_ptr <= operation.identity.record.read_rec_ptr
				|| operation.identity.record.end_rec_ptr > source->tail_end)
				return false;
			if (target != p)
				depends[p][target] = true;
		}
	}
	/* Stable ordinal choice among ready sources; at most 128 sources, no
	 * new queue or persistent progress. Repeated edges count only once. */
	for (uint32 slot = 0; slot < count; slot++) {
		uint32 choice = UINT32_MAX;
		for (uint32 p = 0; p < count; p++) {
			bool ready = !emitted[p];
			for (uint32 target = 0; target < count && ready; target++)
				if (depends[p][target] && !emitted[target])
					ready = false;
			if (ready) {
				choice = p;
				break;
			}
		}
		if (choice == UINT32_MAX)
			return false;
		result[slot] = choice;
		emitted[choice] = true;
	}
	if (!cold_replay_sources_current(typed, fence))
		return false;
	memcpy(order, result, count * sizeof(*order));
	typed->refusal = CLUSTER_COLD_OK;
	typed->refusal_detail[0] = '\0';
	return true;
}

static bool
cold_recovery_abort_record(ClusterColdTypedV1 *typed, ClusterRecoveryFencePlan *fence,
						   uint32 participant, uint32 index,
						   const RfPageOnlineRecordIdentityV1 *identity,
						   const ClusterThreadRecoveryAuthorityV1 *writer)
{
	const RfSideOnlinePlanV1 *plan = typed->side_plans[participant];
	ClusterThreadRecoveryAuthorityV1 target;
	uint32 match = cold_recovery_abort_target(typed, participant, index);
	/* A history claim or own restart input is not a borrowed current
	 * mutation authority. Do not substitute a newer generation of the thread. */
	/* Same-source C0 follows its original stream prefix. A different source
	 * must have consumed its complete captured cut first; never compare LSNs
	 * from different streams or treat the writer checkpoint as target cover. */
	if (match == UINT32_MAX || (match != participant && !typed->source_drained[match])
		|| !cold_participant_authority(typed, fence, match, &target))
		return false;
	if (participant == typed->own_participant) {
		const ColdRoot *selected = &typed->replay_roots[participant];
		if (!cluster_undo_recovery_abort_replay_native_v1(
				plan, index, identity, &selected->root, &selected->token, &target))
			return false;
	} else if (!cluster_undo_recovery_abort_replay_v1(plan, index, identity, writer, &target))
		return false;
	return cold_control_authority_current(&target);
}

/* A compound dispatch must retain every originally decoded SPACE payload.
 * No partial array, newly reconstructed terminal, or bare physical absence
 * can substitute for the selected full captured operation. */
static bool
cold_record_drops_match(const ClusterColdRecordV1 *record, const RfSideOnlineOperationV1 *operation)
{
	uint32 offset = operation->xact.space_drop_offset;
	if (record->space_count == 0) {
		if (operation->kind != RF_SIDE_ONLINE_OPERATION_XACT)
			return true;
		if (operation->xact.space_drop_count != 0)
			return false;
		if (operation->xact.relation_count == 0)
			return (record->info & XLR_INFO_MASK) == 0;
		/* Native ABORT has raw pending-delete locators, not typed SPACE
		 * tombstones. Only the original complete recovery operation may
		 * reach its selected cold owner and native deletion continuation. */
		return operation->purpose == RF_SIDE_PLAN_RECOVERY
			   && operation->xact.kind == RF_SIDE_XACT_ABORT
			   && operation->xact.cold_completion_effects && record->rmid == RM_XACT_ID
			   && (record->info & XLOG_XACT_OPMASK) == XLOG_XACT_ABORT
			   && (record->info & XLR_INFO_MASK) == XLR_SPECIAL_REL_UPDATE
			   && operation->owned_payload != NULL && operation->owned_payload_length > 0
			   && operation->owned_payload_length == operation->xact.completion_payload_length;
	}
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
		|| !(kind == RF_SIDE_ONLINE_OPERATION_RECOVERY_TT_ABORT
			 ? cold_c0_source_authority(typed, fence, participant, &authority)
			 : cold_side_source_authority(typed, fence, participant, &authority)))
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
	if (cluster_epoch_get_current() != epoch
		|| !(kind == RF_SIDE_ONLINE_OPERATION_RECOVERY_TT_ABORT
			 ? cold_c0_source_authority(typed, fence, participant, &authority)
			 : cold_control_authority_current(&authority))
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
	/* Required retained history must also have complete capture before replay. */
	for (i = 0; i < typed->participant_count; i++) {
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
