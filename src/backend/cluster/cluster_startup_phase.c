/*-------------------------------------------------------------------------
 *
 * cluster_startup_phase.c
 *	  pgrac postmaster startup phase machinery (Stage 1.10 skeleton).
 *
 *	  Implements the Phase 0 -> 1 -> 2 -> 3 -> 4 -> RUNNING state
 *	  machine that splits the previously single cluster_init() entry
 *	  into named, observable, timeout-bounded transitions.
 *
 *	  See cluster_startup_phase.h for the architectural overview and
 *	  HC1-HC5 hard constraints; spec-1.10-postmaster-startup-phase-
 *	  skeleton.md for the full design.
 *
 *	  Driver / handler split (HC3):
 *
 *	    The driver in cluster_run_startup_sequence() owns the phase
 *	    transition (advance + log + wait event + history + timeout +
 *	    inject points).  Phase handlers (phase_1_handler, phase_2_
 *	    handler, ..., phase_4_handler) only do their phase's work and
 *	    return PhaseRunResult.  The driver decides whether to advance.
 *
 *	    Stage 1.10 phase handlers 1-3 are no-op stubs returning
 *	    PHASE_RUN_OK; phase 4 handler delegates to PG's existing
 *	    walwriter / bgwriter / etc. spawn paths (no new process).
 *	    Stage 1.11-1.14 / Stage 2-4 replace handler bodies without
 *	    breaking the driver loop.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_startup_phase.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Compiled only in --enable-cluster builds.
 *	  Spec: spec-1.10-postmaster-startup-phase-skeleton.md (frozen
 *	  2026-05-03 v1.1 with 5 user hard-constraint refinements).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "miscadmin.h" /* IsUnderPostmaster (HC1) */
#include "port/atomics.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/elog.h"
#include "utils/timestamp.h"

#include "cluster/cluster_elog.h" /* cluster_phase legacy mirror (HC2) */
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_hw_remaster.h" /* contract: worker CF(S) admission */
#include "cluster/cluster_cf_phase2.h"	 /* RF-ROOT P6: storage contract verify */
#include "cluster/cluster_cssd.h"		 /* cluster_cssd_start / wait_for_ready (2.5 Sprint A) */
#include "cluster/cluster_qvotec.h" /* cluster_qvotec_start / wait_for_ready (spec-2.6 Step 3 D8) */
#include "cluster/cluster_diag.h"	/* cluster_diag_start / wait_for_ready (1.13 Sprint A) */
#include "cluster/cluster_epoch.h"	/* cluster_epoch_get_current (RF-ROOT P6 diag) */
#include "cluster/cluster_guc.h"	/* cluster_phase{1..4}_timeout (D2 F2) */
#include "cluster/cluster_grd.h"
#include "cluster/cluster_stats.h"	/* cluster_stats_start / wait_for_ready (1.14 Sprint A) */
#include "cluster/cluster_inject.h" /* CLUSTER_INJECTION_POINT */
#include "cluster/cluster_lck.h"	/* cluster_lck_start / wait_for_ready (1.12 Sprint A) */
#include "cluster/cluster_lms.h"	/* cluster_lms_start / wait_for_ready (spec-2.18 Sprint A) */
#include "cluster/cluster_lmon.h"	/* cluster_lmon_start / wait_for_ready (1.11 Sprint A) */
#include "cluster/cluster_membership.h"
#include "cluster/cluster_recovery_duty.h" /* live formation witness (RF-ROOT P6) */
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_scn.h"   /* SCN_NODE_ID_VALID (spec-1.16 D13) */
#include "cluster/cluster_shmem.h" /* cluster_shmem_register_region */
#include "cluster/cluster_startup_phase.h"
#include "cluster/cluster_wal_state.h" /* spec-4.2 publish_active (phase->RUNNING) */
#include "cluster/cluster_wal_retention.h"
#include "cluster/cluster_write_fence.h" /* spec-4.12 D6 rejoin self-fence gate (Q5=C) */
#include "storage/proc.h"


/*
 * Phase enum ↔ string lookup table.  Position in the array MUST line
 * up with ClusterStartupPhase enum values; out-of-range returns
 * "(unknown)".
 */
static const char *const cluster_phase_strings[] = {
	"pre_init",		   /* CLUSTER_PHASE_PRE_INIT  = 0 */
	"phase0_base",	   /* CLUSTER_PHASE_0_BASE    = 1 */
	"phase1_cluster",  /* CLUSTER_PHASE_1_CLUSTER = 2 */
	"phase2_lock",	   /* CLUSTER_PHASE_2_LOCK    = 3 */
	"phase3_recovery", /* CLUSTER_PHASE_3_RECOVERY= 4 */
	"phase4_normal",   /* CLUSTER_PHASE_4_NORMAL  = 5 */
	"running",		   /* CLUSTER_PHASE_RUNNING   = 6 */
	"shutdown"		   /* CLUSTER_PHASE_SHUTDOWN  = 7 */
};

/*
 * Phase state in shared memory (spec-1.10.1 D1 F1 hardening).
 *
 *	Five static globals (current_phase / phase_start_times[] /
 *	phase_history[] / count / head) used to live here.  EXEC_BACKEND/
 *	Windows children re-execed and re-ran their static initializers,
 *	so they observed the PRE_INIT seed regardless of postmaster's
 *	actual state.  Migrating to shmem gives every process a coherent
 *	view backed by ShmemInitStruct, with LWLock LWTRANCHE_CLUSTER_
 *	STARTUP_PHASE guarding writes (postmaster, LW_EXCLUSIVE) and
 *	reads (any backend, LW_SHARED).
 *
 *	cluster_phase_state is set by cluster_phase_shmem_init() during
 *	postmaster startup and (on EXEC_BACKEND children) during
 *	SubPostmasterMain shmem rebind.  It stays NULL only inside the
 *	cluster_unit test harness when the helper is not invoked --
 *	accessors all early-return safe defaults in that case.
 */
static ClusterPhaseSharedState *cluster_phase_state = NULL;
static bool phase3_join_readonly_deferred = false;

static bool
cluster_phase_state_lock_acquire(LWLockMode mode)
{
	if (MyProc == NULL)
		return LWLockConditionalAcquire(&cluster_phase_state->lwlock, mode);
	LWLockAcquire(&cluster_phase_state->lwlock, mode);
	return true;
}

typedef struct ClusterAuthorityBindingLocal {
	ClusterAuthorityReadiness state;
	uint16 origin_thread;
	uint64 boot_incarnation;
	uint64 lms_generation;
	ClusterFenceAuthorityProof authority;
	ClusterFormationSnapshotV1 formation;
} ClusterAuthorityBindingLocal;


/* ============================================================
 * Public accessors (read-only; callable from any backend)
 * ============================================================ */

const char *
cluster_startup_phase_to_string(ClusterStartupPhase phase)
{
	if ((int)phase < 0 || (int)phase > CLUSTER_PHASE_LAST)
		return "(unknown)";
	return cluster_phase_strings[(int)phase];
}


ClusterStartupPhase
cluster_current_phase(void)
{
	/* AD-023 A1: lock-free atomic read; the phase word is written only under
	 * the phase-state lwlock, so concurrent readers never block and never see
	 * a torn value. */
	if (cluster_phase_state == NULL)
		return CLUSTER_PHASE_PRE_INIT;
	return (ClusterStartupPhase)pg_atomic_read_u32(&cluster_phase_state->current_phase);
}

ClusterAuthorityReadiness
cluster_authority_readiness_get(void)
{
	/* AD-023 A1: lock-free atomic read; the single word is written only under
	 * the phase-state lwlock, so a concurrent reader sees either the old or
	 * the new value, never a torn mix. */
	if (cluster_phase_state == NULL)
		return CLUSTER_AUTHORITY_OFF;
	return (ClusterAuthorityReadiness)pg_atomic_read_u32(&cluster_phase_state->authority_readiness);
}

bool
cluster_authority_readiness_managed(void)
{
	/* Same lock-free discipline: this runs on every GES grant and must not
	 * contend on the hot phase-state lwlock. */
	if (cluster_phase_state == NULL)
		return false;
	return pg_atomic_read_u32(&cluster_phase_state->authority_managed) != 0;
}

static bool
cluster_authority_binding_copy(ClusterAuthorityBindingLocal *out)
{
	if (cluster_phase_state == NULL || out == NULL)
		return false;
	if (!cluster_phase_state_lock_acquire(LW_SHARED))
		return false;
	if (pg_atomic_read_u32(&cluster_phase_state->authority_managed) == 0
		|| (ClusterAuthorityReadiness)pg_atomic_read_u32(&cluster_phase_state->authority_readiness)
			   == CLUSTER_AUTHORITY_OFF) {
		LWLockRelease(&cluster_phase_state->lwlock);
		return false;
	}
	out->state
		= (ClusterAuthorityReadiness)pg_atomic_read_u32(&cluster_phase_state->authority_readiness);
	out->origin_thread = cluster_phase_state->authority_origin_thread;
	out->boot_incarnation = cluster_phase_state->authority_boot_incarnation;
	out->lms_generation = cluster_phase_state->authority_lms_generation;
	out->authority = cluster_phase_state->authority_fence;
	out->formation = cluster_phase_state->authority_formation;
	LWLockRelease(&cluster_phase_state->lwlock);
	return true;
}

static bool
cluster_authority_clear_matching_internal(const ClusterAuthorityBindingLocal *binding,
										  const char *caller, bool preserve_handoff_identity)
{
	bool cleared = false;

	if (cluster_phase_state == NULL || binding == NULL)
		return false;
	if (!cluster_phase_state_lock_acquire(LW_EXCLUSIVE))
		return false;
	if (pg_atomic_read_u32(&cluster_phase_state->authority_managed) != 0
		&& (ClusterAuthorityReadiness)pg_atomic_read_u32(&cluster_phase_state->authority_readiness)
			   == binding->state
		&& cluster_phase_state->authority_origin_thread == binding->origin_thread
		&& cluster_phase_state->authority_boot_incarnation == binding->boot_incarnation
		&& cluster_phase_state->authority_lms_generation == binding->lms_generation
		&& memcmp(&cluster_phase_state->authority_fence, &binding->authority,
				  sizeof(binding->authority))
			   == 0
		&& memcmp(&cluster_phase_state->authority_formation, &binding->formation,
				  sizeof(binding->formation))
			   == 0) {
		pg_atomic_write_u32(&cluster_phase_state->authority_readiness, CLUSTER_AUTHORITY_OFF);
		/* Managed is a boot-lifetime fail-closed latch.  Losing a bound
		 * generation invalidates readiness; it must never reactivate the
		 * legacy one-dimensional LMS/native fallback in the same postmaster. */
		cluster_phase_state->authority_origin_thread
			= preserve_handoff_identity ? binding->origin_thread : 0;
		cluster_phase_state->authority_boot_incarnation
			= preserve_handoff_identity ? binding->boot_incarnation : 0;
		cluster_phase_state->authority_lms_generation = 0;
		memset(&cluster_phase_state->authority_fence, 0,
			   sizeof(cluster_phase_state->authority_fence));
		memset(&cluster_phase_state->authority_formation, 0,
			   sizeof(cluster_phase_state->authority_formation));
		if (preserve_handoff_identity) {
			int32 origin_node = (int32)binding->origin_thread - 1;

			cluster_phase_state->authority_formation.membership.membership_state[origin_node]
				= CLUSTER_MEMBER_MEMBER;
			cluster_phase_state->authority_formation.membership
				.last_admitted_incarnation[origin_node]
				= binding->formation.membership.last_admitted_incarnation[origin_node];
		}
		cleared = true;
	}
	LWLockRelease(&cluster_phase_state->lwlock);
	return cleared;
}

static bool
cluster_authority_clear_matching(const ClusterAuthorityBindingLocal *binding, const char *caller)
{
	return cluster_authority_clear_matching_internal(binding, caller, false);
}

/* The exact pivot clears authority readiness and every authority-bearing
 * byte, but keeps the already-captured boot/predecessor tuple in existing
 * phase-state storage.  OFF remains non-authoritative; the carrier exists
 * solely so the forked LMON can reject floor or incarnation drift. */
static bool
cluster_authority_clear_matching_for_handoff(const ClusterAuthorityBindingLocal *binding)
{
	return cluster_authority_clear_matching_internal(binding, "phase3_join_readonly_pivot", true);
}

bool
cluster_authority_handoff_identity_current(uint64 expected_self_incarnation,
										   uint64 expected_predecessor_floor)
{
	ClusterFenceAuthorityProof zero_fence;
	bool current = false;
	int32 origin_node;

	if (cluster_phase_state == NULL || expected_self_incarnation == 0
		|| (expected_predecessor_floor != 0
			&& expected_predecessor_floor >= expected_self_incarnation)
		|| !cluster_phase_state_lock_acquire(LW_SHARED))
		return false;
	origin_node = cluster_node_id;
	memset(&zero_fence, 0, sizeof(zero_fence));
	if (origin_node >= 0 && origin_node < CLUSTER_MAX_NODES
		&& pg_atomic_read_u32(&cluster_phase_state->authority_managed) != 0
		&& (ClusterAuthorityReadiness)pg_atomic_read_u32(&cluster_phase_state->authority_readiness)
			   == CLUSTER_AUTHORITY_OFF
		&& cluster_phase_state->authority_origin_thread == (uint16)(origin_node + 1)
		&& cluster_phase_state->authority_boot_incarnation == expected_self_incarnation
		&& cluster_phase_state->authority_lms_generation == 0
		&& memcmp(&cluster_phase_state->authority_fence, &zero_fence, sizeof(zero_fence)) == 0
		&& cluster_phase_state->authority_formation.membership.membership_state[origin_node]
			   == CLUSTER_MEMBER_MEMBER
		&& cluster_phase_state->authority_formation.membership
				   .last_admitted_incarnation[origin_node]
			   == expected_predecessor_floor)
		current = true;
	LWLockRelease(&cluster_phase_state->lwlock);
	return current;
}

void
cluster_authority_readiness_clear(void)
{
	if (cluster_phase_state == NULL)
		return;
	if (!cluster_phase_state_lock_acquire(LW_EXCLUSIVE))
		return;
	pg_atomic_write_u32(&cluster_phase_state->authority_readiness, CLUSTER_AUTHORITY_OFF);
	/* Preserve authority_managed once set; shmem reinitialization is the only
	 * transition back to an unmanaged boot. */
	cluster_phase_state->authority_origin_thread = 0;
	cluster_phase_state->authority_boot_incarnation = 0;
	cluster_phase_state->authority_lms_generation = 0;
	memset(&cluster_phase_state->authority_fence, 0, sizeof(cluster_phase_state->authority_fence));
	memset(&cluster_phase_state->authority_formation, 0,
		   sizeof(cluster_phase_state->authority_formation));
	LWLockRelease(&cluster_phase_state->lwlock);
}

static bool
cluster_authority_setup_phase_current(void)
{
	ClusterStartupPhase phase = cluster_current_phase();

	return phase == CLUSTER_PHASE_3_RECOVERY
		   || (phase == CLUSTER_PHASE_4_NORMAL && phase3_join_readonly_deferred);
}

static bool
cluster_authority_binding_preseal_current(const ClusterAuthorityBindingLocal *binding)
{
	uint64 formation_floor;
	uint64 live_floor;

	if (binding == NULL || binding->origin_thread == 0
		|| binding->origin_thread > CLUSTER_MAX_NODES)
		return false;
	formation_floor
		= binding->formation.membership.last_admitted_incarnation[binding->origin_thread - 1];
	live_floor = cluster_membership_get_last_admitted_incarnation(cluster_node_id);
	return binding != NULL && binding->boot_incarnation != 0 && binding->lms_generation != 0
		   && cluster_authority_setup_phase_current()
		   && cluster_cssd_get_status() == CLUSTER_CSSD_READY
		   && cluster_qvotec_get_status() == CLUSTER_QVOTEC_READY && cluster_qvotec_in_quorum()
		   && cluster_qvotec_get_self_incarnation() == binding->boot_incarnation
		   && binding->formation.membership.membership_state[binding->origin_thread - 1]
				  == CLUSTER_MEMBER_MEMBER
		   && formation_floor == live_floor
		   && (live_floor == binding->boot_incarnation
			   || (!cluster_reconfig_self_join_admitted()
				   && live_floor < binding->boot_incarnation))
		   && cluster_lms_get_lms_restart_generation() == binding->lms_generation
		   && cluster_lms_is_recovery_ready()
		   && cluster_formation_classification_revalidate_nowait(
				  binding->origin_thread, &binding->authority, &binding->formation)
				  == CLUSTER_FORMATION_WITNESS_READY;
}

/* A sealed serving generation must continue to match the live formation, but
 * must not consume the finite IR-held recovery-duty fence cache.  The GRD seal,
 * QVOTEC incarnation and LMS generation are checked by the caller. */
static bool
cluster_serving_formation_current(const ClusterAuthorityBindingLocal *binding)
{
	ClusterFormationSnapshotV1 current;

	return binding != NULL && cluster_reconfig_self_join_admitted()
		   && cluster_reconfig_capture_formation_snapshot_v1(binding->origin_thread, &current)
		   && cluster_formation_snapshot_matches_v1(&binding->formation, &current);
}

/* The formation and GRD seal may be replaced only by LMON after the ordinary
 * reconfig barrier closes.  Keep the boot/LMS binding while that recoverable
 * mismatch is fenced, but never retain it across a real generation loss. */
static bool
cluster_serving_generation_current(const ClusterAuthorityBindingLocal *binding)
{
	ClusterStartupPhase phase = cluster_current_phase();

	return binding != NULL && binding->state == CLUSTER_AUTHORITY_SERVING_READY
		   && binding->boot_incarnation != 0 && binding->lms_generation != 0
		   && phase >= CLUSTER_PHASE_4_NORMAL && phase < CLUSTER_PHASE_SHUTDOWN
		   && cluster_cssd_get_status() == CLUSTER_CSSD_READY
		   && cluster_qvotec_get_status() == CLUSTER_QVOTEC_READY && cluster_qvotec_in_quorum()
		   && cluster_qvotec_get_self_incarnation() == binding->boot_incarnation
		   && cluster_membership_get_last_admitted_incarnation(cluster_node_id)
				  == binding->boot_incarnation
		   && cluster_lms_get_lms_restart_generation() == binding->lms_generation
		   && cluster_lms_is_ready();
}

/* AD-023 §3: component drift (CSSD/QVOTEC/quorum/incarnation/formation/LMS
 * generation/GRD) is the invalidation trigger.  The phase/state gate is
 * deliberately NOT part of this predicate so callers can distinguish "the
 * allowlist phase gate rejected this request" from "the binding itself is
 * stale". */
static bool
cluster_authority_binding_components_current_internal(const ClusterAuthorityBindingLocal *binding,
													  bool serving, bool require_seal,
													  bool require_member)
{
	if (binding == NULL || binding->boot_incarnation == 0 || binding->lms_generation == 0
		|| cluster_cssd_get_status() != CLUSTER_CSSD_READY
		|| cluster_qvotec_get_status() != CLUSTER_QVOTEC_READY || !cluster_qvotec_in_quorum()
		|| cluster_qvotec_get_self_incarnation() != binding->boot_incarnation
		|| cluster_membership_get_last_admitted_incarnation(cluster_node_id)
			   != binding->boot_incarnation
		|| cluster_lms_get_lms_restart_generation() != binding->lms_generation
		|| (require_member && !cluster_membership_is_member(cluster_node_id))
		|| (serving ? !cluster_serving_formation_current(binding)
					: cluster_formation_classification_revalidate_nowait(
						  binding->origin_thread, &binding->authority, &binding->formation)
						  != CLUSTER_FORMATION_WITNESS_READY)
		|| (require_seal
			&& !cluster_grd_recovery_authority_is_current(binding->boot_incarnation,
														  binding->lms_generation)))
		return false;
	return true;
}

static bool
cluster_authority_binding_components_current(const ClusterAuthorityBindingLocal *binding,
											 bool serving)
{
	return cluster_authority_binding_components_current_internal(binding, serving, true, true);
}

static bool
cluster_authority_binding_external_current(const ClusterAuthorityBindingLocal *binding,
										   bool serving)
{
	ClusterStartupPhase phase = cluster_current_phase();

	if (!cluster_authority_binding_components_current(binding, serving))
		return false;
	if (serving)
		return binding->state == CLUSTER_AUTHORITY_SERVING_READY && phase >= CLUSTER_PHASE_4_NORMAL
			   && phase < CLUSTER_PHASE_SHUTDOWN && cluster_lms_is_ready();
	return binding->state == CLUSTER_AUTHORITY_RECOVERY_READY && phase == CLUSTER_PHASE_3_RECOVERY
		   && cluster_lms_is_recovery_ready();
}

bool
cluster_authority_readiness_begin(uint16 origin_thread, const ClusterFenceAuthorityProof *authority,
								  const ClusterFormationSnapshotV1 *formation)
{
	uint64 boot_incarnation;
	uint64 formation_floor;
	uint64 live_floor;
	int32 origin_node;

	if (cluster_phase_state == NULL || authority == NULL || formation == NULL || origin_thread == 0
		|| origin_thread > CLUSTER_MAX_NODES || !cluster_authority_setup_phase_current())
		return false;
	origin_node = (int32)origin_thread - 1;
	boot_incarnation = cluster_qvotec_get_self_incarnation();
	formation_floor = formation->membership.last_admitted_incarnation[origin_node];
	live_floor = cluster_membership_get_last_admitted_incarnation(origin_node);
	if (origin_node != cluster_node_id || boot_incarnation == 0
		|| formation->membership.membership_state[origin_node] != CLUSTER_MEMBER_MEMBER
		|| formation_floor != live_floor
		|| (live_floor != boot_incarnation
			&& (cluster_reconfig_self_join_admitted() || live_floor >= boot_incarnation))
		|| cluster_formation_classification_revalidate_nowait(origin_thread, authority, formation)
			   != CLUSTER_FORMATION_WITNESS_READY)
		return false;

	if (!cluster_phase_state_lock_acquire(LW_EXCLUSIVE))
		return false;
	if ((ClusterAuthorityReadiness)pg_atomic_read_u32(&cluster_phase_state->authority_readiness)
		!= CLUSTER_AUTHORITY_OFF) {
		LWLockRelease(&cluster_phase_state->lwlock);
		return false;
	}
	pg_atomic_write_u32(&cluster_phase_state->authority_managed, 1);
	pg_atomic_write_u32(&cluster_phase_state->authority_readiness, CLUSTER_AUTHORITY_STARTING);
	cluster_phase_state->authority_origin_thread = origin_thread;
	cluster_phase_state->authority_boot_incarnation = boot_incarnation;
	cluster_phase_state->authority_lms_generation = 0;
	cluster_phase_state->authority_fence = *authority;
	cluster_phase_state->authority_formation = *formation;
	LWLockRelease(&cluster_phase_state->lwlock);
	return true;
}

bool
cluster_authority_readiness_bind_recovery_generation(uint64 lms_generation)
{
	ClusterAuthorityBindingLocal binding;
	bool valid;

	if (cluster_phase_state == NULL || lms_generation == 0) {
		return false;
	}
	if (!cluster_phase_state_lock_acquire(LW_EXCLUSIVE)) {
		return false;
	}
	if (pg_atomic_read_u32(&cluster_phase_state->authority_managed) == 0
		|| (ClusterAuthorityReadiness)pg_atomic_read_u32(&cluster_phase_state->authority_readiness)
			   != CLUSTER_AUTHORITY_STARTING
		|| !cluster_authority_setup_phase_current()
		|| (cluster_phase_state->authority_lms_generation != 0
			&& cluster_phase_state->authority_lms_generation != lms_generation)) {
		LWLockRelease(&cluster_phase_state->lwlock);
		return false;
	}
	cluster_phase_state->authority_lms_generation = lms_generation;
	LWLockRelease(&cluster_phase_state->lwlock);

	if (!cluster_authority_binding_copy(&binding)) {
		return false;
	}
	valid = binding.state == CLUSTER_AUTHORITY_STARTING
			&& cluster_authority_binding_preseal_current(&binding);
	if (!valid && cluster_authority_binding_copy(&binding)) {
		cluster_authority_clear_matching(&binding, "bind_preseal_fail");
	}
	return valid;
}

bool
cluster_authority_readiness_publish_recovery(uint64 lms_generation)
{
	ClusterAuthorityBindingLocal binding;
	bool valid;

	if (cluster_phase_state == NULL || lms_generation == 0)
		return false;
	if (!cluster_phase_state_lock_acquire(LW_EXCLUSIVE))
		return false;
	if (pg_atomic_read_u32(&cluster_phase_state->authority_managed) == 0
		|| (ClusterAuthorityReadiness)pg_atomic_read_u32(&cluster_phase_state->authority_readiness)
			   != CLUSTER_AUTHORITY_STARTING
		|| !cluster_authority_setup_phase_current()) {
		LWLockRelease(&cluster_phase_state->lwlock);
		return false;
	}
	if (cluster_phase_state->authority_lms_generation != lms_generation) {
		LWLockRelease(&cluster_phase_state->lwlock);
		return false;
	}
	LWLockRelease(&cluster_phase_state->lwlock);

	/* STARTING uses the same external proof, with the state transition checked
	 * below instead of the steady recovery predicate. */
	if (!cluster_authority_binding_copy(&binding))
		return false;
	valid = binding.state == CLUSTER_AUTHORITY_STARTING && cluster_authority_setup_phase_current()
			&& cluster_cssd_get_status() == CLUSTER_CSSD_READY
			&& cluster_qvotec_get_status() == CLUSTER_QVOTEC_READY && cluster_qvotec_in_quorum()
			&& cluster_qvotec_get_self_incarnation() == binding.boot_incarnation
			&& cluster_membership_get_last_admitted_incarnation(cluster_node_id)
				   == binding.boot_incarnation
			&& cluster_lms_get_lms_restart_generation() == lms_generation
			&& cluster_lms_is_recovery_ready()
			&& cluster_formation_classification_revalidate_nowait(
				   binding.origin_thread, &binding.authority, &binding.formation)
				   == CLUSTER_FORMATION_WITNESS_READY
			&& cluster_grd_recovery_authority_is_current(binding.boot_incarnation, lms_generation);
	if (!valid) {
		cluster_authority_clear_matching(&binding, "publish_recovery_fail");
		return false;
	}
	if (!cluster_phase_state_lock_acquire(LW_EXCLUSIVE))
		return false;
	if ((ClusterAuthorityReadiness)pg_atomic_read_u32(&cluster_phase_state->authority_readiness)
			== CLUSTER_AUTHORITY_STARTING
		&& cluster_phase_state->authority_lms_generation == lms_generation)
		pg_atomic_write_u32(&cluster_phase_state->authority_readiness,
							CLUSTER_AUTHORITY_RECOVERY_READY);
	else
		valid = false;
	LWLockRelease(&cluster_phase_state->lwlock);
	return valid;
}

bool
cluster_recovery_transport_is_current(void)
{
	ClusterAuthorityBindingLocal binding;
	bool current;

	if (!cluster_authority_binding_copy(&binding))
		return false;
	if (binding.state == CLUSTER_AUTHORITY_RECOVERY_READY)
		return cluster_recovery_authority_is_current();
	if (binding.state != CLUSTER_AUTHORITY_STARTING)
		return false;
	current = cluster_authority_binding_preseal_current(&binding);
	if (!current) {
		/* Mirror the recovery_authority discipline: the STARTING preseal
		 * carries the same phase-3 gate, and a phase-4 request must not
		 * destroy a binding on the phase gate alone. */
		/*
		 * RF-ROOT P6 (STOP-01 contract): a STARTING
		 * binding with lms_generation == 0 is the postmaster's mid-bind
		 * window — begin() can only create the binding before the LMS
		 * process exists (live generation still 0), and the phase-3 loop
		 * binds the generation on its next iteration.  The preseal fails
		 * on that window by construction (lms_generation != 0 is one of
		 * its terms), but destroying the binding here forces a full
		 * re-begin on every peer DONE ingress and starves the loop;
		 * the phase-3 loop owns that binding and clears it itself on its
		 * own failure paths (bounded by the phase-3 deadline).  Only a
		 * bound generation that drifted from the live formation is
		 * genuinely stale.
		 */
		if (cluster_authority_setup_phase_current() && binding.lms_generation != 0)
			cluster_authority_clear_matching(&binding, "recovery_transport_stale");
	}
	return current;
}

/*
 * cluster_recovery_transport_components_current -- RF-ROOT P6 (crash-rejoin).
 *
 *	The components-only transport proof:  identical to the strict
 *	cluster_recovery_transport_is_current EXCEPT that it does not require
 *	the GRD recovery-authority seal.  The seal is stamped only when the
 *	phase-3 recovery-authority barrier reaches terminal SUCCESS, and the
 *	barrier's cluster-wide convergence input is the survivor's inbound
 *	REDECLARE_DONE key (the authority-axis done slots) — gating that
 *	ingress on the seal is structurally circular on a rejoiner:
 *
 *	  boot_decided=0 -> self JOINING -> barrier request-current fails
 *	  -> seal never stamped -> transport not current -> REDECLARE_DONE
 *	  dropped -> join view never rebuilt -> boot_decided stays 0 ...
 *
 *	The REDECLARE_DONE arm of ges_readiness_allows_early_opcode is the
 *	single consumer of this predicate;  every other opcode keeps the
 *	strict seal requirement.  Safety:  a REDECLARE_DONE frame only
 *	mutates the monotonic done arrays (and the authority-axis slots,
 *	which additionally require an exact {epoch, dead-bitmap-hash} match
 *	against the published request) — it has no serving-side effect, so
 *	accepting it on component currency alone cannot open the serve gate.
 */
bool
cluster_recovery_transport_components_current(void)
{
	ClusterAuthorityBindingLocal binding;

	if (!cluster_authority_binding_copy(&binding))
		return false;
	if (binding.state == CLUSTER_AUTHORITY_RECOVERY_READY
		|| binding.state == CLUSTER_AUTHORITY_STARTING) {
		/*
		 * The durable admission (self_join_admitted, set by the quorum-
		 * majority COMMITTED marker + publish-proof) is the membership
		 * proof for the transport:  the LMON self-state byte can
		 * transiently read JOINING while the boot-decided latch is still
		 * held, and dropping the barrier-building DONE ingress during
		 * those windows re-closes the deadlock this predicate exists to
		 * break.  The formation revalidation is deliberately NOT part of
		 * this proof either:  the admission path invalidates the fence
		 * cache on every real membership flip, so a revalidation would
		 * fail exactly while the re-declare barrier is converging — the
		 * very frames it must admit.  REDECLARE_DONE only mutates the
		 * monotonic done arrays (and the authority-axis slots, which
		 * additionally require an exact {epoch, dead-bitmap-hash} match
		 * against the published request);  it has no serving-side effect,
		 * so component currency (cssd/qvotec/quorum/incarnation/LMS
		 * generation/admission) is the correct strength for its gate.
		 */
		bool boot_ok = binding.boot_incarnation != 0;
		bool lmsgen_ok = binding.lms_generation != 0;
		bool cssd_ok = cluster_cssd_get_status() == CLUSTER_CSSD_READY;
		bool qvotec_ok = cluster_qvotec_get_status() == CLUSTER_QVOTEC_READY;
		bool quorum_ok = cluster_qvotec_in_quorum();
		bool inc_ok = cluster_qvotec_get_self_incarnation() == binding.boot_incarnation;
		bool admitted_ok = cluster_membership_get_last_admitted_incarnation(cluster_node_id)
						   == binding.boot_incarnation;
		bool lms_match_ok = cluster_lms_get_lms_restart_generation() == binding.lms_generation;
		bool lms_rcv_ok = cluster_lms_is_recovery_ready();
		bool member_ok = cluster_membership_is_member(cluster_node_id)
						 || cluster_reconfig_self_join_admitted();

		return boot_ok && lmsgen_ok && cssd_ok && qvotec_ok && quorum_ok && inc_ok && admitted_ok
			   && lms_match_ok && lms_rcv_ok && member_ok;
	}
	return false;
}

bool
cluster_recovery_authority_is_current(void)
{
	ClusterAuthorityBindingLocal binding;
	bool current;

	if (!cluster_authority_binding_copy(&binding))
		return false;
	if (binding.state != CLUSTER_AUTHORITY_RECOVERY_READY)
		return false;
	current = cluster_authority_binding_external_current(&binding, false);
	if (!current) {
		/* AD-023 §3: only a real component loss invalidates the binding.
		 * The recovery allowlist additionally gates on phase == PHASE_3;
		 * once StartupXLOG advances to phase 4 the binding is the
		 * postmaster's pending SERVING upgrade and must survive requesters
		 * that only fail the phase gate.  Clearing on the phase gate alone
		 * stranded phase 4 with an OFF binding that nothing re-binds
		 * (begin() is phase-3 gated), guaranteeing the phase4
		 * serving-publication timeout. */
		if (!cluster_authority_binding_components_current(&binding, false))
			cluster_authority_clear_matching(&binding, "recovery_authority_stale");
	}
	return current;
}

bool
cluster_authority_readiness_publish_serving(void)
{
	ClusterAuthorityBindingLocal binding;
	ClusterFormationWitnessResult formation_result;
	bool cssd_ready;
	bool qvotec_ready;
	bool in_quorum;
	bool lms_ready;
	bool grd_current;
	uint64 self_incarnation;
	uint64 admitted_incarnation;
	uint64 lms_generation;
	bool valid;

	if (!cluster_authority_binding_copy(&binding)
		|| binding.state != CLUSTER_AUTHORITY_RECOVERY_READY
		|| cluster_current_phase() != CLUSTER_PHASE_4_NORMAL)
		return false;
	/* Validate every generation component while service is still unpublished. */
	cssd_ready = cluster_cssd_get_status() == CLUSTER_CSSD_READY;
	qvotec_ready = cluster_qvotec_get_status() == CLUSTER_QVOTEC_READY;
	in_quorum = cluster_qvotec_in_quorum();
	self_incarnation = cluster_qvotec_get_self_incarnation();
	admitted_incarnation = cluster_membership_get_last_admitted_incarnation(cluster_node_id);
	lms_generation = cluster_lms_get_lms_restart_generation();
	lms_ready = cluster_lms_is_ready();
	formation_result = cluster_formation_classification_revalidate_nowait(
		binding.origin_thread, &binding.authority, &binding.formation);
	grd_current = cluster_grd_recovery_authority_is_current(binding.boot_incarnation,
															binding.lms_generation);
	valid = cssd_ready && qvotec_ready && in_quorum && self_incarnation == binding.boot_incarnation
			&& admitted_incarnation == binding.boot_incarnation
			&& lms_generation == binding.lms_generation && lms_ready
			&& formation_result == CLUSTER_FORMATION_WITNESS_READY && grd_current
			&& cluster_reconfig_self_join_admitted();
	if (!valid) {
		ereport(
			LOG,
			(errmsg("cluster phase 4: serving authority diagnostic"),
			 errdetail("cssd_ready=%d qvotec_ready=%d in_quorum=%d "
					   "self_incarnation=%llu/%llu admitted_incarnation=%llu "
					   "lms_generation=%llu/%llu lms_ready=%d formation_result=%d "
					   "grd_current=%d",
					   cssd_ready, qvotec_ready, in_quorum, (unsigned long long)self_incarnation,
					   (unsigned long long)binding.boot_incarnation,
					   (unsigned long long)admitted_incarnation, (unsigned long long)lms_generation,
					   (unsigned long long)binding.lms_generation, lms_ready, (int)formation_result,
					   grd_current)));
		cluster_authority_clear_matching(&binding, "publish_serving_stale");
		return false;
	}
	LWLockAcquire(&cluster_phase_state->lwlock, LW_EXCLUSIVE);
	if ((ClusterAuthorityReadiness)pg_atomic_read_u32(&cluster_phase_state->authority_readiness)
			== CLUSTER_AUTHORITY_RECOVERY_READY
		&& cluster_phase_state->authority_lms_generation == binding.lms_generation)
		pg_atomic_write_u32(&cluster_phase_state->authority_readiness,
							CLUSTER_AUTHORITY_SERVING_READY);
	else
		valid = false;
	LWLockRelease(&cluster_phase_state->lwlock);
	return valid;
}

bool
cluster_serving_ready_is_current(void)
{
	ClusterAuthorityBindingLocal binding;
	bool current;

	if (!cluster_authority_binding_copy(&binding))
		return false;
	if (binding.state != CLUSTER_AUTHORITY_SERVING_READY)
		return false;
	current = cluster_authority_binding_external_current(&binding, true);
	/* A current boot/LMS generation whose formation moved stays unavailable,
	 * but keeps its immutable binding so the survivor LMON can replace it only
	 * after the existing GRD recovery/re-declare barrier closes.  Every data-
	 * plane caller still observes false during that interval.  A same-formation
	 * GRD loss is not a reconfig transition and remains terminal for this boot. */
	if (!current
		&& (!cluster_serving_generation_current(&binding)
			|| cluster_serving_formation_current(&binding)))
		cluster_authority_clear_matching(&binding, "serving_ready_stale");
	return current;
}

bool
cluster_authority_serving_rebind_lmon(void)
{
	ClusterAuthorityBindingLocal binding;
	ClusterFormationSnapshotV1 current;
	ClusterFormationSnapshotV1 verify;
	bool rebound = false;
	bool ext_cur;
	bool gen_cur;
	bool capture_ok;
	bool moved;
	bool grd_rebind_ok;
	bool verify_ok;

	if (!cluster_authority_binding_copy(&binding)
		|| binding.state != CLUSTER_AUTHORITY_SERVING_READY)
		return false;
	ext_cur = cluster_authority_binding_external_current(&binding, true);
	if (ext_cur)
		return true;
	gen_cur = cluster_serving_generation_current(&binding);
	capture_ok = cluster_reconfig_capture_formation_snapshot_v1(binding.origin_thread, &current);
	moved = capture_ok && !cluster_formation_snapshot_matches_v1(&binding.formation, &current);
	if (!gen_cur || !capture_ok || !moved)
		return false;

	/* The GRD helper accepts only the exact event already closed by the ordinary
	 * LMON P0-P7 recovery driver; it does not start a second barrier. */
	grd_rebind_ok = cluster_grd_serving_authority_rebind_lmon(&current, binding.boot_incarnation,
															  binding.lms_generation);
	verify_ok = cluster_reconfig_capture_formation_snapshot_v1(binding.origin_thread, &verify)
				&& memcmp(&current, &verify, sizeof(current)) == 0;
	if (!grd_rebind_ok || !verify_ok)
		return false;

	if (!cluster_phase_state_lock_acquire(LW_EXCLUSIVE))
		return false;
	if ((ClusterAuthorityReadiness)pg_atomic_read_u32(&cluster_phase_state->authority_readiness)
			== CLUSTER_AUTHORITY_SERVING_READY
		&& cluster_phase_state->authority_origin_thread == binding.origin_thread
		&& cluster_phase_state->authority_boot_incarnation == binding.boot_incarnation
		&& cluster_phase_state->authority_lms_generation == binding.lms_generation
		&& memcmp(&cluster_phase_state->authority_formation, &binding.formation,
				  sizeof(binding.formation))
			   == 0) {
		cluster_phase_state->authority_formation = current;
		rebound = true;
	}
	LWLockRelease(&cluster_phase_state->lwlock);

	return rebound && cluster_serving_ready_is_current();
}

/*
 * cluster_authority_serving_rebind_leaver -- RF-ROOT P6 (L5 shutdown
 * handoff): the committed LEAVER's serving rebind.  The survivor rebind
 * above requires the ordinary P0-P7 episode to close; the departed node
 * never arms one for its own departure, so its serving binding would stay
 * stale forever and its shutdown checkpoint / THREAD_CLEAN_CLOSE CF
 * acquires would fail closed.  The GRD leaver rebind re-stamps the seal
 * from the leaver's own applied CLEAN_LEAVE evidence; this wrapper then
 * re-captures the binding formation under the phase-state lock, exactly
 * like the survivor path.
 */
bool
cluster_authority_serving_rebind_leaver(void)
{
	ClusterAuthorityBindingLocal binding;
	ClusterFormationSnapshotV1 current;
	ClusterFormationSnapshotV1 verify;
	bool rebound = false;

	if (!cluster_authority_binding_copy(&binding)
		|| binding.state != CLUSTER_AUTHORITY_SERVING_READY)
		return false;
	if (cluster_authority_binding_external_current(&binding, true))
		return true;
	if (!cluster_serving_generation_current(&binding)
		|| !cluster_reconfig_capture_formation_snapshot_v1(binding.origin_thread, &current)
		|| cluster_formation_snapshot_matches_v1(&binding.formation, &current)) {
		return false;
	}

	/* Only the committed leaver re-binds this way (the GRD helper re-checks
	 * the applied CLEAN_LEAVE evidence + all fail-closed components). */
	if (!cluster_grd_serving_authority_rebind_leaver(&current, binding.boot_incarnation,
													 binding.lms_generation)
		|| !cluster_reconfig_capture_formation_snapshot_v1(binding.origin_thread, &verify)
		|| memcmp(&current, &verify, sizeof(current)) != 0) {
		return false;
	}

	if (!cluster_phase_state_lock_acquire(LW_EXCLUSIVE))
		return false;
	if ((ClusterAuthorityReadiness)pg_atomic_read_u32(&cluster_phase_state->authority_readiness)
			== CLUSTER_AUTHORITY_SERVING_READY
		&& cluster_phase_state->authority_origin_thread == binding.origin_thread
		&& cluster_phase_state->authority_boot_incarnation == binding.boot_incarnation
		&& cluster_phase_state->authority_lms_generation == binding.lms_generation
		&& memcmp(&cluster_phase_state->authority_formation, &binding.formation,
				  sizeof(binding.formation))
			   == 0) {
		cluster_phase_state->authority_formation = current;
		rebound = true;
	}
	LWLockRelease(&cluster_phase_state->lwlock);

	return rebound && cluster_serving_ready_is_current();
}

bool
cluster_recovery_authority_resid_mode_allowed(const ClusterResId *resid, LOCKMODE mode)
{
	if (resid == NULL)
		return false;
	if (resid->type == CLUSTER_CF_RESID_TYPE)
		return mode == ShareLock && resid->field1 == 0 && resid->field2 == 0 && resid->field3 == 0
			   && resid->field4 == 0 && resid->lockmethodid == DEFAULT_LOCKMETHOD;
	if (resid->type == CLUSTER_WAL_RETENTION_RESID_TYPE)
		return mode == ExclusiveLock && resid->field1 > 0
			   && resid->field1 <= CLUSTER_WAL_RETENTION_MAX_THREADS && resid->field2 == 0
			   && resid->field3 == 0 && resid->field4 == 0
			   && resid->lockmethodid == DEFAULT_LOCKMETHOD;
	return false;
}

bool
cluster_recovery_authority_request_allowed(const ClusterResId *resid, LOCKMODE mode,
										   bool startup_process)
{
	/*
	 * RF-ROOT P6 (clean-reopen / THREAD_OPEN): the recovery-time lock
	 * admission accepts the components-only transport proof too.  The
	 * strict authority proof requires the GRD seal, which the phase-3
	 * recovery-authority barrier only stamps AFTER it converges — and the
	 * THREAD_OPEN root reopen that feeds the survivor's join chain (and
	 * therefore the barrier's convergence) itself needs the phase-3
	 * clusterwide CF share-lock first.  Only the StartupProcess during
	 * phase 3 is ever admitted, still restricted to the frozen
	 * CF(S)/WALR(X) allowlist below.
	 */
	/*
	 * Stage 8 contract (verified implementation): the hw-remaster rebuild bgworker
	 * runs on the survivor during its GRD recovery episode (phase 4 normal,
	 * serving readiness not current) and must STRONG-read the dead origin's
	 * canonical root to rebuild the adopted HWM — which needs the
	 * clusterwide CF share lock.  It is a real backend with a normal S6
	 * release path (AD-023 §4's ghost-holder lesson was about the
	 * postmaster, which has no PGPROC release), so admit it to the SAME
	 * frozen CF(S)/WALR(X) allowlist without the StartupProcess/phase-3
	 * condition.  No other process is affected.
	 */
	if (cluster_hw_remaster_worker_active())
		return cluster_recovery_authority_resid_mode_allowed(resid, mode);
	return startup_process && cluster_current_phase() == CLUSTER_PHASE_3_RECOVERY
		   && (cluster_recovery_transport_components_current()
			   || cluster_recovery_authority_is_current())
		   && cluster_recovery_authority_resid_mode_allowed(resid, mode);
}


TimestampTz
cluster_phase_started_at(ClusterStartupPhase phase)
{
	TimestampTz result;

	if ((int)phase < 0 || (int)phase > CLUSTER_PHASE_LAST)
		return 0;
	if (cluster_phase_state == NULL)
		return 0;

	LWLockAcquire(&cluster_phase_state->lwlock, LW_SHARED);
	result = cluster_phase_state->phase_start_times[(int)phase];
	LWLockRelease(&cluster_phase_state->lwlock);
	return result;
}


int64
cluster_phase_elapsed_seconds(void)
{
	ClusterStartupPhase phase;
	TimestampTz started;
	long secs;
	int usecs;

	if (cluster_phase_state == NULL)
		return 0;

	LWLockAcquire(&cluster_phase_state->lwlock, LW_SHARED);
	phase = (ClusterStartupPhase)pg_atomic_read_u32(&cluster_phase_state->current_phase);
	started = cluster_phase_state->phase_start_times[(int)phase];
	LWLockRelease(&cluster_phase_state->lwlock);

	if (started == 0)
		return 0;

	TimestampDifference(started, GetCurrentTimestamp(), &secs, &usecs);
	return (int64)secs;
}


void
cluster_phase_history_format(char *buf, size_t size)
{
	int start;
	int i;
	int emit_count;
	size_t offset = 0;
	int local_count;
	int local_head;
	PhaseHistoryEntry local_history[CLUSTER_PHASE_HISTORY_RING_SIZE];

	if (buf == NULL || size == 0)
		return;
	buf[0] = '\0';

	if (cluster_phase_state == NULL)
		return;

	/*
	 * Snapshot the ring under LW_SHARED so the formatter can iterate
	 * without holding the lock during the (potentially many) snprintf
	 * calls.  Eight entries fit comfortably on the stack.
	 */
	LWLockAcquire(&cluster_phase_state->lwlock, LW_SHARED);
	local_count = cluster_phase_state->phase_history_count;
	local_head = cluster_phase_state->phase_history_head;
	memcpy(local_history, cluster_phase_state->phase_history, sizeof(local_history));
	LWLockRelease(&cluster_phase_state->lwlock);

	emit_count = (local_count < CLUSTER_PHASE_HISTORY_RING_SIZE) ? local_count
																 : CLUSTER_PHASE_HISTORY_RING_SIZE;

	if (emit_count == 0)
		return;

	/*
	 * Walk in chronological order: oldest entry is at head when the
	 * ring is full, otherwise at slot 0.
	 */
	start = (local_count < CLUSTER_PHASE_HISTORY_RING_SIZE) ? 0 : local_head;

	for (i = 0; i < emit_count; i++) {
		int idx = (start + i) % CLUSTER_PHASE_HISTORY_RING_SIZE;
		const PhaseHistoryEntry *entry = &local_history[idx];
		const char *phase_str = cluster_startup_phase_to_string(entry->phase);
		const char *ts_str = timestamptz_to_str(entry->entered_at);
		int n;

		n = snprintf(buf + offset, size - offset, "%s%s@%s", (i > 0) ? "," : "", phase_str, ts_str);
		if (n < 0 || (size_t)n >= size - offset) {
			/* Truncate cleanly; the ring is bounded so this rarely fires. */
			break;
		}
		offset += (size_t)n;
	}
}


/* ============================================================
 * Phase shmem region helpers (spec-1.10.1 D1 F1)
 * ============================================================ */

Size
cluster_phase_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterPhaseSharedState));
}


void
cluster_phase_shmem_init(void)
{
	bool found;

	cluster_phase_state = (ClusterPhaseSharedState *)ShmemInitStruct(
		"pgrac cluster startup phase", sizeof(ClusterPhaseSharedState), &found);

	if (!found) {
		/*
		 * First attach (postmaster on POSIX fork; postmaster on
		 * EXEC_BACKEND too -- the EXEC_BACKEND child takes the found=true
		 * branch).  Initialise everything to the PRE_INIT seed and
		 * register the LWLock with its dedicated tranche.
		 */
		memset(cluster_phase_state, 0, sizeof(*cluster_phase_state));
		LWLockInitialize(&cluster_phase_state->lwlock, LWTRANCHE_CLUSTER_STARTUP_PHASE);
		pg_atomic_init_u32(&cluster_phase_state->authority_readiness, CLUSTER_AUTHORITY_OFF);
		pg_atomic_init_u32(&cluster_phase_state->authority_managed, 0);
		pg_atomic_init_u32(&cluster_phase_state->current_phase, CLUSTER_PHASE_PRE_INIT);
	}
}


/*
 * cluster_phase_shmem_region -- spec-1.3 shmem registry descriptor.
 *
 *	Registered from cluster_init_shmem_module() in cluster_shmem.c so
 *	the registry is the single dispatch path; cluster_request_shmem
 *	iterates and calls cluster_phase_shmem_size().
 */
static const ClusterShmemRegion cluster_phase_region = {
	.name = "pgrac cluster startup phase",
	.size_fn = cluster_phase_shmem_size,
	.init_fn = cluster_phase_shmem_init,
	.lwlock_count = 1, /* the embedded ClusterPhaseSharedState.lwlock */
	.owner_subsys = "cluster_startup_phase",
	.reserved_flags = 0,
};


void
cluster_phase_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_phase_region);
}


/* ============================================================
 * Phase advance (driver-internal API; HC2 SSOT, HC1 postmaster-only)
 * ============================================================ */

void
cluster_advance_phase(ClusterStartupPhase target)
{
	ClusterStartupPhase prev;
	TimestampTz now;
	int slot;

	/*
	 * HC1: postmaster-only.  This is the ONLY function that mutates
	 * the shmem-backed phase state; calling it from a child backend
	 * would corrupt the postmaster's view of its own startup.
	 */
	Assert(!IsUnderPostmaster);

	/*
	 * spec-1.10.1 D1 F1: phase state lives in shmem now.  This guards
	 * against early callers (cluster_init_shmem_module not yet run)
	 * by failing loudly rather than dereferencing NULL.
	 */
	if (cluster_phase_state == NULL)
		ereport(FATAL,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("cluster_advance_phase called before phase shmem state was attached"),
				 errhint("cluster_phase_shmem_init() must run during "
						 "CreateSharedMemoryAndSemaphores().")));

	prev = (ClusterStartupPhase)pg_atomic_read_u32(&cluster_phase_state->current_phase);

	/*
	 * Strict transition rules.  The only legitimate transitions are:
	 *   prev + 1 == target          (forward step)
	 *   target == CLUSTER_PHASE_SHUTDOWN  (any phase can enter shutdown)
	 * Everything else is a programming error -> ereport(FATAL).
	 *
	 * Validate before taking the lock so the FATAL stack is shorter.
	 */
	if (target == CLUSTER_PHASE_SHUTDOWN) {
		/* allowed from any current phase */
	} else if ((int)target == (int)prev + 1) {
		/* allowed forward step */
	} else {
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_PHASE_PRECONDITION_FAILED),
						errmsg("invalid cluster phase transition: %s -> %s",
							   cluster_startup_phase_to_string(prev),
							   cluster_startup_phase_to_string(target)),
						errdetail("Cluster startup phases must advance strictly +1 or "
								  "transition to SHUTDOWN.  Backward transitions and "
								  "skipped phases indicate a programming error in "
								  "cluster_run_startup_sequence() driver loop.")));
	}

	/*
	 * Fire the prev phase's "-exit" injection point before switching,
	 * unless we're transitioning out of PRE_INIT (no exit for the
	 * sentinel) or into SHUTDOWN (the prev phase may not have a
	 * meaningful exit -- shutdown is a special transition).
	 *
	 * Inject points run outside the LWLock to avoid Assert(!locked)
	 * paths inside ereport in fault-injected sleep modes.
	 */
	if (prev != CLUSTER_PHASE_PRE_INIT && target != CLUSTER_PHASE_SHUTDOWN) {
		switch (prev) {
		case CLUSTER_PHASE_0_BASE:
			CLUSTER_INJECTION_POINT("cluster-startup-phase-0-exit");
			break;
		case CLUSTER_PHASE_1_CLUSTER:
			CLUSTER_INJECTION_POINT("cluster-startup-phase-1-exit");
			break;
		case CLUSTER_PHASE_2_LOCK:
			CLUSTER_INJECTION_POINT("cluster-startup-phase-2-exit");
			break;
		case CLUSTER_PHASE_3_RECOVERY:
			CLUSTER_INJECTION_POINT("cluster-startup-phase-3-exit");
			break;
		case CLUSTER_PHASE_4_NORMAL:
			CLUSTER_INJECTION_POINT("cluster-startup-phase-4-exit");
			break;
		default:
			break;
		}
	}

	now = GetCurrentTimestamp();

	/* Commit the transition under LW_EXCLUSIVE (HC2 SSOT mutate). */
	LWLockAcquire(&cluster_phase_state->lwlock, LW_EXCLUSIVE);
	pg_atomic_write_u32(&cluster_phase_state->current_phase, target);
	cluster_phase_state->phase_start_times[(int)target] = now;

	/* Append to fixed-size history ring (HC5). */
	slot = cluster_phase_state->phase_history_head;
	cluster_phase_state->phase_history[slot].phase = target;
	cluster_phase_state->phase_history[slot].entered_at = now;
	cluster_phase_state->phase_history_head = (slot + 1) % CLUSTER_PHASE_HISTORY_RING_SIZE;
	cluster_phase_state->phase_history_count++;
	LWLockRelease(&cluster_phase_state->lwlock);

	/*
	 * Update the legacy cluster_phase const char * mirror (HC2: this
	 * is the ONLY writer in the codebase).  cluster_startup_phase_to_
	 * string returns a pointer to a static string literal so no
	 * lifetime concerns; child backends inherit this pointer at fork
	 * time.  Note: under EXEC_BACKEND the legacy mirror starts at
	 * "pre_init" again in each child.  Backends needing the live phase
	 * value should call cluster_current_phase() (shmem-backed) and
	 * cluster_startup_phase_to_string(); the mirror is retained for
	 * cluster_elog.c log decorations only.
	 */
	cluster_phase = cluster_startup_phase_to_string(target);

	/*
	 * Phase enter logging.  LOG so it's visible at default verbosity
	 * (postmaster startup is the only realistic observation channel
	 * when phase machinery is mid-flight; pg_cluster_state and
	 * pg_stat_activity require SQL access which is not yet up).
	 */
	ereport(LOG, (errmsg("cluster startup: %s -> %s", cluster_startup_phase_to_string(prev),
						 cluster_startup_phase_to_string(target))));

	/* Fire the new phase's "-enter" injection point. */
	switch (target) {
	case CLUSTER_PHASE_0_BASE:
		CLUSTER_INJECTION_POINT("cluster-startup-phase-0-enter");
		break;
	case CLUSTER_PHASE_1_CLUSTER:
		CLUSTER_INJECTION_POINT("cluster-startup-phase-1-enter");
		break;
	case CLUSTER_PHASE_2_LOCK:
		CLUSTER_INJECTION_POINT("cluster-startup-phase-2-enter");
		break;
	case CLUSTER_PHASE_3_RECOVERY:
		CLUSTER_INJECTION_POINT("cluster-startup-phase-3-enter");
		break;
	case CLUSTER_PHASE_4_NORMAL:
		CLUSTER_INJECTION_POINT("cluster-startup-phase-4-enter");
		break;
	default:
		break;
	}
}


/* ============================================================
 * Phase handlers (HC3 driver/handler split; handlers DO NOT call
 * cluster_advance_phase()).
 *
 *	Stage 1.10 skeleton: phase 1-3 handlers are no-op stubs.  Phase 4
 *	is also a stub at this stage -- the actual walwriter / bgwriter /
 *	checkpointer / autovacuum / etc. spawn happens in PG's PostmasterMain
 *	later in the startup sequence (between cluster_run_startup_sequence
 *	and the ServerLoop entry).  Handler bodies are placeholders for
 *	1.11-1.14 / Stage 2-4 replacement.
 * ============================================================ */

/* Forward decls used by multi-child phase handlers. */
static int cluster_phase_timeout_for(ClusterStartupPhase phase);
static int cluster_phase_remaining_budget_ms(TimestampTz deadline, int driver_buffer_ms);
static bool cluster_phase4_wal_state_configured(void);
static bool cluster_phase4_wait_for_quorum(TimestampTz deadline);
static int phase3_cssd_pid = 0;
static int phase3_qvotec_pid = 0;
static int phase3_lms_pid = 0;
static TimestampTz phase3_recovery_deadline = 0;

/*
 * A recovery-control founder can discover the already-running cluster only
 * after its first GRD barrier attempt.  The approved pivot is legal solely
 * while the exact pre-publish STARTING binding is still reversible.  It
 * neither owns admission nor creates or refreshes a deadline.
 */
static bool
cluster_phase3_try_pre_publish_join_readonly_pivot(TimestampTz deadline, uint64 lms_generation)
{
	ClusterAuthorityBindingLocal binding;
	uint64 predecessor_floor;

	if (phase3_join_readonly_deferred || deadline == 0 || lms_generation == 0
		|| GetCurrentTimestamp() >= deadline || !cluster_cf_phase2_peer_verified()
		|| cluster_reconfig_self_join_admitted()
		|| !cluster_reconfig_epoch0_late_founder_evidence_current()
		|| !cluster_authority_binding_copy(&binding) || binding.state != CLUSTER_AUTHORITY_STARTING
		|| binding.lms_generation != lms_generation
		|| cluster_lms_get_lms_restart_generation() != binding.lms_generation
		|| binding.boot_incarnation == 0
		|| cluster_qvotec_get_self_incarnation() != binding.boot_incarnation)
		return false;
	predecessor_floor = cluster_membership_get_last_admitted_incarnation(cluster_node_id);
	if (predecessor_floor != 0 && predecessor_floor >= binding.boot_incarnation)
		return false;

	if (!cluster_authority_clear_matching_for_handoff(&binding)
		|| cluster_authority_readiness_get() != CLUSTER_AUTHORITY_OFF
		|| !cluster_authority_readiness_managed()
		|| cluster_lms_get_lms_restart_generation() != binding.lms_generation
		|| cluster_qvotec_get_self_incarnation() != binding.boot_incarnation
		|| cluster_membership_get_last_admitted_incarnation(cluster_node_id) != predecessor_floor
		|| GetCurrentTimestamp() >= deadline || !cluster_cf_phase2_peer_verified()
		|| cluster_reconfig_self_join_admitted()
		|| !cluster_reconfig_epoch0_late_founder_evidence_current()
		|| !cluster_reconfig_stage_pre_publish_join_handoff(binding.boot_incarnation,
															predecessor_floor))
		return false;

	phase3_join_readonly_deferred = true;
	elog(LOG, "cluster phase 3: exact pre-publish recovery binding pivoted to JOIN_READONLY");
	return true;
}

static PhaseRunResult
phase_1_handler(PhaseRunFailContext *fail_ctx)
{
	int lmon_pid;
	bool ready;
	int wait_budget_ms;

	Assert(!IsUnderPostmaster);
	Assert(fail_ctx != NULL);

	if (!cluster_enabled) {
		elog(DEBUG1, "cluster phase 1: cluster.enabled=false; skipping LMON "
					 "spawn (degraded to spec-1.10 stub behavior)");
		return PHASE_RUN_OK;
	}

	lmon_pid = cluster_lmon_start();
	if (lmon_pid == 0) {
		/*
		 * Spec-1.11.1 F13 (codex round 4 P2/P3 fix): write the LMON-
		 * specific SQLSTATE into fail_ctx so the driver's FATAL exit
		 * carries 53R0A (was generic 53R09 before F13).  Sprint B's
		 * LOG-only ereport is removed -- single FATAL path is cleaner
		 * for external supervisors / TAP tests.
		 */
		fail_ctx->errcode = ERRCODE_CLUSTER_LMON_SPAWN_FAILED;
		fail_ctx->errmsg = "cluster phase 1: failed to spawn LMON aux process";
		fail_ctx->errhint = "Check fork() / system limits (ulimit -u) and "
							"postmaster log for LMON child startup errors.";
		return PHASE_RUN_FATAL;
	}

	wait_budget_ms = (cluster_phase1_timeout - 5) * 1000;
	if (wait_budget_ms < 1000)
		wait_budget_ms = 1000;

	ready = cluster_lmon_wait_for_ready(wait_budget_ms);
	if (!ready) {
		fail_ctx->errcode = ERRCODE_CLUSTER_LMON_NOT_READY;
		fail_ctx->errmsg = "cluster phase 1: LMON did not publish READY in time";
		fail_ctx->errhint = "Increase cluster.phase1_timeout or check LMON log "
							"for stuck startup; LMON status sticks at SPAWNING "
							"when the child crashed during initialization.";
		return PHASE_RUN_FATAL;
	}

	elog(DEBUG1,
		 "cluster phase 1: LMON ready (pid %d); interconnect listener / "
		 "heartbeat consumer remain stubs (Stage 1.15+)",
		 lmon_pid);
	return PHASE_RUN_OK;
}


static PhaseRunResult
phase_2_handler(PhaseRunFailContext *fail_ctx)
{
	int lck_pid;
	bool ready;
	int wait_budget_ms;

	Assert(!IsUnderPostmaster);
	Assert(fail_ctx != NULL);

	/* Spec-1.12 HC4: cluster.enabled=false 退化 stub (与 phase_1 对称). */
	if (!cluster_enabled) {
		elog(DEBUG1, "cluster phase 2: cluster.enabled=false; skipping LCK "
					 "spawn (degraded to spec-1.10 stub behavior)");
		return PHASE_RUN_OK;
	}

	/* Stage 1.12 Sprint A: spawn LCK and wait for READY. */
	lck_pid = cluster_lck_start();
	if (lck_pid == 0) {
		fail_ctx->errcode = ERRCODE_CLUSTER_LCK_SPAWN_FAILED;
		fail_ctx->errmsg = "cluster phase 2: failed to spawn LCK aux process";
		fail_ctx->errhint = "Check fork() / system limits (ulimit -u) and "
							"postmaster log for LCK child startup errors.";
		return PHASE_RUN_FATAL;
	}

	wait_budget_ms = (cluster_phase2_timeout - 5) * 1000;
	if (wait_budget_ms < 1000)
		wait_budget_ms = 1000;

	ready = cluster_lck_wait_for_ready(wait_budget_ms);
	if (!ready) {
		fail_ctx->errcode = ERRCODE_CLUSTER_LCK_NOT_READY;
		fail_ctx->errmsg = "cluster phase 2: LCK did not publish READY in time";
		fail_ctx->errhint = "Increase cluster.phase2_timeout or check LCK log "
							"for stuck startup; LCK status sticks at SPAWNING "
							"when the child crashed during initialization.";
		return PHASE_RUN_FATAL;
	}

	elog(DEBUG1,
		 "cluster phase 2: LCK ready (pid %d); LMS starts under PM_RUN "
		 "ServerLoop respawn, LMD remains stub (Stage 2+ GES feature)",
		 lck_pid);
	return PHASE_RUN_OK;
}


static bool
cluster_phase3_wait_for_live_formation(TimestampTz deadline, bool allow_join_readonly,
									   ClusterFormationWitnessResult *out_result,
									   uint16 *out_origin_thread,
									   ClusterFenceAuthorityProof *out_authority,
									   ClusterFormationSnapshotV1 *out_snapshot)
{
	ClusterFormationWitnessResult result = CLUSTER_FORMATION_WITNESS_UNSTABLE;
	uint16 thread_id = cluster_wal_thread_id();

	if (thread_id == XLP_THREAD_ID_LEGACY) {
		if (out_result != NULL)
			*out_result = CLUSTER_FORMATION_WITNESS_BAD_ARGUMENT;
		return false;
	}

	for (;;) {
		ClusterFormationWitnessV1 *witness = NULL;
		int attempt_ms;

		if (GetCurrentTimestamp() >= deadline)
			break;
		/* spec-5.6 JOIN_READONLY: an exact peer-attaching node may finish
		 * StartupXLOG without shared-authority writes.  This is a one-way,
		 * fail-closed deferral: only the initial phase-3 wait may select it,
		 * and only while the fresh phase-2 peer proof and exact same-boot
		 * late-founder evidence are both current.  A transient JOINING byte
		 * can also be produced by the founding bootstrap stripe HOLD; it is
		 * not evidence that another database instance is already running. */
		if (allow_join_readonly && cluster_cf_phase2_peer_verified()
			&& !cluster_reconfig_self_join_admitted()
			&& cluster_reconfig_epoch0_late_founder_evidence_current()) {
			phase3_join_readonly_deferred = true;
			if (out_result != NULL)
				*out_result = CLUSTER_FORMATION_WITNESS_UNSTABLE;
			return true;
		}
		attempt_ms = cluster_phase_remaining_budget_ms(deadline, 5000);
		if (attempt_ms > 100)
			attempt_ms = 100;
		result = cluster_formation_witness_build_recovery_control_wait(thread_id, attempt_ms,
																	   &witness);
		if (result == CLUSTER_FORMATION_WITNESS_READY) {
			if (!cluster_formation_witness_copy_classification_v1(witness, out_origin_thread,
																  out_authority, out_snapshot))
				result = CLUSTER_FORMATION_WITNESS_CORRUPT;
			else
				result = cluster_formation_witness_revalidate_nowait(witness);
			cluster_formation_witness_destroy(&witness);
			if (result == CLUSTER_FORMATION_WITNESS_READY) {
				if (out_result != NULL)
					*out_result = result;
				return true;
			}
		}
		cluster_formation_witness_destroy(&witness);

		/* OWNER_MISMATCH is transient here while LMON publishes the exact
		 * admitted-incarnation floor.  CAPABILITY_UNAVAILABLE is also transient
		 * for this Postmaster/no-PGPROC caller: A1's conditional formation-
		 * snapshot acquisition reports lock contention through that result and
		 * this loop retries it only under the existing phase-3 deadline.
		 * Structural/corrupt results still fail immediately. */
		if (result != CLUSTER_FORMATION_WITNESS_OWNER_MISMATCH
			&& result != CLUSTER_FORMATION_WITNESS_UNSTABLE
			&& result != CLUSTER_FORMATION_WITNESS_MARKER_UNPROVEN
			&& result != CLUSTER_FORMATION_WITNESS_IO_FAILED
			&& result != CLUSTER_FORMATION_WITNESS_CAPABILITY_UNAVAILABLE)
			break;
		pg_usleep(1000L);
	}

	if (out_result != NULL)
		*out_result = result;
	return false;
}

/*
 * Refresh the finite durable-marker cache after StartupXLOG and the exact
 * xid-stripe admission have completed.  The phase-3 recovery-control proof
 * is an immutable authority binding, not a timeless cache lease: ordinary
 * service may reuse it only after a fresh live witness proves the same
 * marker and the same formation (apart from the frozen 0 -> 1 admission
 * edge).  This does not replace the binding or create a second authority.
 */
static ClusterFormationWitnessResult
cluster_phase4_refresh_serving_formation(int timeout_ms)
{
	ClusterAuthorityBindingLocal binding;
	ClusterFormationWitnessV1 *witness = NULL;
	ClusterFenceAuthorityProof fresh_authority;
	ClusterFormationSnapshotV1 fresh_formation;
	ClusterFormationWitnessResult result;
	uint16 fresh_origin_thread = 0;

	if (timeout_ms <= 0 || !cluster_authority_binding_copy(&binding)
		|| binding.state != CLUSTER_AUTHORITY_RECOVERY_READY
		|| !cluster_reconfig_self_join_admitted())
		return CLUSTER_FORMATION_WITNESS_CAPABILITY_UNAVAILABLE;

	result = cluster_formation_witness_build_live_wait(binding.origin_thread, timeout_ms, &witness);
	if (result == CLUSTER_FORMATION_WITNESS_READY) {
		if (!cluster_formation_witness_copy_classification_v1(witness, &fresh_origin_thread,
															  &fresh_authority, &fresh_formation))
			result = CLUSTER_FORMATION_WITNESS_CORRUPT;
		else if (fresh_origin_thread != binding.origin_thread
				 || fresh_authority.agree_disk_count != binding.authority.agree_disk_count
				 || fresh_authority.total_disk_count != binding.authority.total_disk_count
				 || !cluster_fence_marker_semantic_equal(&fresh_authority.marker,
														 &binding.authority.marker)
				 || !cluster_formation_snapshot_matches_v1(&binding.formation, &fresh_formation))
			result = CLUSTER_FORMATION_WITNESS_UNSTABLE;
		else
			result = cluster_formation_classification_revalidate_nowait(
				binding.origin_thread, &binding.authority, &binding.formation);
	}
	cluster_formation_witness_destroy(&witness);
	return result;
}

static PhaseRunResult
phase_3_handler(PhaseRunFailContext *fail_ctx)
{
	ClusterFenceAuthorityProof formation_authority;
	ClusterFormationSnapshotV1 formation_snapshot;
	ClusterFormationWitnessResult formation_result;
	uint16 formation_origin_thread = 0;
	uint64 boot_incarnation;
	uint64 lms_generation;
	int lms_remaining_ms;
	int remaining_ms;
	bool bind_failed;
	bool barrier_failed;
	bool resume_join_readonly = phase3_join_readonly_deferred;

	Assert(!IsUnderPostmaster);
	Assert(fail_ctx != NULL);

	if (!cluster_enabled) {
		elog(DEBUG1, "cluster phase 3: cluster.enabled=false; skipping CSSD + "
					 "QVOTEC pre-recovery formation gate");
		return PHASE_RUN_OK;
	}

	/* The post-StartupXLOG half of JOIN_READONLY reuses the exact phase-3
	 * children.  Its authority-only budget is created after ordinary admission;
	 * it cannot respawn a child or establish authority before that edge. */
	if (resume_join_readonly) {
		if (phase3_recovery_deadline == 0 || GetCurrentTimestamp() >= phase3_recovery_deadline
			|| cluster_cssd_get_status() != CLUSTER_CSSD_READY
			|| cluster_qvotec_get_status() != CLUSTER_QVOTEC_READY || !cluster_qvotec_in_quorum()
			|| !cluster_cf_phase2_peer_verified() || !cluster_reconfig_self_join_admitted()) {
			fail_ctx->errcode = ERRCODE_CLUSTER_WAL_RETENTION_BLOCKED;
			fail_ctx->errmsg = "cluster phase 3: JOIN_READONLY admission is not current";
			fail_ctx->errhint = "The fresh peer proof, quorum, original phase-3 deadline, "
								"and exact post-recovery admission must all remain current.";
			return PHASE_RUN_FATAL;
		}
		goto establish_recovery_authority;
	}

	/*
	 * RF-ROOT P6 (L5/cast-leg contract wedge):  verify the phase-2
	 * cross-node storage contract HERE, in postmaster context, BEFORE the
	 * formation wait.  The phase-3 THREAD_OPEN root publish and the
	 * cast-leg bootstrap CF role gate fail closed on an unverified
	 * contract, while the StartupXLOG-side verify runs in the startup
	 * process — which this postmaster phase machine does not fork until
	 * phase 3 completes, so the startup verify can never precede the
	 * formation wait.  Postmaster context has the loaded topology, so the
	 * fresh nonce+ack rendezvous runs here; the StartupXLOG call remains
	 * (idempotent — a second fresh rendezvous re-confirms and rewrites
	 * the same CROSSNODE_VERIFIED state).
	 */
	if (cluster_phase4_wal_state_configured())
		cluster_cf_phase2_verify_or_fail(DataDir);

	/*
	 * RF-ROOT P6 E2: recovery-time WAL retirement needs the same live
	 * formation authority as normal operation.  Establish CSSD then QVOTEC
	 * before StartupXLOG, using one bounded phase-3 deadline for both READY
	 * waits and the configured multi-node quorum cut.
	 */
	phase3_recovery_deadline = TimestampTzPlusMilliseconds(
		GetCurrentTimestamp(), cluster_phase_timeout_for(CLUSTER_PHASE_3_RECOVERY) * 1000);

	phase3_cssd_pid = cluster_cssd_start();
	if (phase3_cssd_pid <= 0) {
		fail_ctx->errcode = ERRCODE_CLUSTER_CSSD_SPAWN_FAILED;
		fail_ctx->errmsg = "cluster phase 3: failed to spawn CSSD aux process";
		fail_ctx->errhint = "Check postmaster log for fork() error and confirm OS "
							"process limits leave room for the CSSD aux process.";
		return PHASE_RUN_FATAL;
	}

	remaining_ms = cluster_phase_remaining_budget_ms(phase3_recovery_deadline, 5000);
	if (!cluster_cssd_wait_for_ready(remaining_ms)) {
		fail_ctx->errcode = ERRCODE_CLUSTER_CSSD_NOT_READY;
		fail_ctx->errmsg = "cluster phase 3: CSSD did not publish READY in time";
		fail_ctx->errhint = "Check postmaster log for CSSD-side errors.  If CSSD is "
							"slow on this hardware, raise cluster.phase3_timeout.";
		return PHASE_RUN_FATAL;
	}

	phase3_qvotec_pid = cluster_qvotec_start();
	if (phase3_qvotec_pid <= 0) {
		fail_ctx->errcode = ERRCODE_CLUSTER_QVOTEC_SPAWN_FAILED;
		fail_ctx->errmsg = "cluster phase 3: failed to spawn QVOTEC aux process";
		fail_ctx->errhint = "Check postmaster log for fork() error and confirm OS "
							"process limits leave room for the QVOTEC aux process.";
		return PHASE_RUN_FATAL;
	}

	remaining_ms = cluster_phase_remaining_budget_ms(phase3_recovery_deadline, 5000);
	if (!cluster_qvotec_wait_for_ready(remaining_ms)) {
		fail_ctx->errcode = ERRCODE_CLUSTER_QVOTEC_NOT_READY;
		fail_ctx->errmsg = "cluster phase 3: QVOTEC did not publish READY in time";
		fail_ctx->errhint = "Check postmaster log for QVOTEC-side errors.  If QVOTEC "
							"is slow on this hardware, raise cluster.phase3_timeout.";
		return PHASE_RUN_FATAL;
	}

	if (cluster_phase4_wal_state_configured() && cluster_conf_node_count() > 1
		&& !cluster_phase4_wait_for_quorum(phase3_recovery_deadline)) {
		fail_ctx->errcode = ERRCODE_CLUSTER_QUORUM_LOST;
		fail_ctx->errmsg = "cluster phase 3: QVOTEC did not establish quorum in time";
		fail_ctx->errhint = "Restore a voting-disk majority and verify the lease-aware "
							"quorum state before retrying startup.";
		return PHASE_RUN_FATAL;
	}

establish_recovery_authority:
	if (cluster_phase4_wal_state_configured()
		&& !cluster_phase3_wait_for_live_formation(phase3_recovery_deadline, !resume_join_readonly,
												   &formation_result, &formation_origin_thread,
												   &formation_authority, &formation_snapshot)) {
		ereport(LOG, (errmsg("cluster phase 3: live formation wait failed with witness result %u",
							 (uint32)formation_result)));
		fail_ctx->errcode = ERRCODE_CLUSTER_WAL_RETENTION_BLOCKED;
		fail_ctx->errmsg = "cluster phase 3: live formation did not become ready before recovery";
		fail_ctx->errhint = "Verify exact self MEMBER admission, the current QVOTEC "
							"incarnation floor, and durable voting-disk authority before "
							"retrying startup.";
		return PHASE_RUN_FATAL;
	}
	if (!resume_join_readonly && phase3_join_readonly_deferred) {
		elog(LOG,
			 "cluster phase 3: deferring recovery authority for exact JOIN_READONLY StartupXLOG");
		/* The ordinary stripe/JCMK state machine owns admission timing.  The
		 * pre-StartupXLOG phase budget must not become, or be mistaken for, a
		 * second admission deadline. */
		phase3_recovery_deadline = 0;
		return PHASE_RUN_OK;
	}

	if (cluster_phase4_wal_state_configured()) {
		if (!cluster_lms_enabled) {
			fail_ctx->errcode = ERRCODE_CLUSTER_LMS_UNAVAILABLE;
			fail_ctx->errmsg = "cluster phase 3: LMS is disabled for recovery authority";
			fail_ctx->errhint = "Set cluster.lms_enabled=on; a formed WAL registry has "
								"no PG-native recovery-authority fallback.";
			return PHASE_RUN_FATAL;
		}
		if (!cluster_authority_readiness_begin(formation_origin_thread, &formation_authority,
											   &formation_snapshot)) {
			fail_ctx->errcode = ERRCODE_CLUSTER_WAL_RETENTION_BLOCKED;
			fail_ctx->errmsg = "cluster phase 3: live formation could not bind this boot";
			fail_ctx->errhint = "Verify the current QVOTEC incarnation equals the admitted "
								"membership floor and retry startup.";
			return PHASE_RUN_FATAL;
		}

		phase3_lms_pid = cluster_lms_start();
		if (phase3_lms_pid <= 0) {
			cluster_authority_readiness_clear();
			fail_ctx->errcode = ERRCODE_CLUSTER_LMS_UNAVAILABLE;
			fail_ctx->errmsg = "cluster phase 3: failed to spawn recovery LMS";
			fail_ctx->errhint = "Check postmaster log and OS process limits.";
			return PHASE_RUN_FATAL;
		}
		lms_remaining_ms = cluster_phase_remaining_budget_ms(phase3_recovery_deadline, 5000);
		if (!cluster_lms_wait_for_recovery_ready(lms_remaining_ms)) {
			cluster_authority_readiness_clear();
			fail_ctx->errcode = ERRCODE_CLUSTER_LMS_UNAVAILABLE;
			fail_ctx->errmsg = "cluster phase 3: LMS did not publish recovery readiness";
			fail_ctx->errhint = "Inspect LMS startup diagnostics; ordinary service remains "
								"closed until phase 4.";
			return PHASE_RUN_FATAL;
		}
		/*
		 * Bind the recovery LMS generation, then complete the GRD
		 * recovery-authority barrier (AD-023 A2).  Both legs share one
		 * bounded loop: fail-closed clear paths (a transient stale
		 * formation proof, a lost conditional phase-state read, or a
		 * REDECLARE/REDECLARE_DONE witness re-validating the binding from
		 * a backend) can drop the STARTING/RECOVERY_READY binding, and
		 * begin() only accepts OFF.  Every retry therefore reacquires the
		 * complete live formation, re-reads the current LMS generation,
		 * and re-binds before the next barrier attempt -- a one-shot
		 * frozen snapshot can never match the post-rejoin REDECLARE_DONE
		 * composite key (the join advances the epoch and dead bitmap).
		 * The whole sequence still fails closed at the phase-3 deadline.
		 */
		bind_failed = false;
		barrier_failed = false;
		for (;;) {
			lms_generation = cluster_lms_get_lms_restart_generation();
			/*
			 * The clean-reopen mainline (STOP-01 frozen THREAD_OPEN) is
			 * executed by the JOIN COMMIT path:  the coordinator's commit
			 * re-vet (cluster_recovery_owner_rejoin_v1) issues the frozen
			 * CLOSED -> OPEN CAS under the THREAD_OPEN reason when the
			 * root is clean-closed (Stage 8 contract, corrected
			 * design).  The postmaster phase-3 driver has no PGPROC and
			 * its S1 admission fails closed (r=10), and the startup
			 * process is forked only AFTER phase-3 — running the reopen
			 * there deadlocks the phase-3 barrier, which waits on the
			 * survivor's join commit, which re-vets the root.  The
			 * components-only transport proof (which the S1 recovery lock
			 * admission uses for the clusterwide CF share-lock) reads the
			 * bound generation, and the phase-2 cross-node storage
			 * contract can verify late (the survivor's cssd publishes its
			 * probe response on the heartbeat cadence).
			 */
			if (cluster_phase4_wal_state_configured()) {
				(void)cluster_authority_readiness_bind_recovery_generation(lms_generation);
			}
			if (!cluster_authority_readiness_bind_recovery_generation(lms_generation)) {
				/* begin() only accepts OFF, so drop any stale STARTING
				 * binding before reacquiring the exact live formation. */
				cluster_authority_readiness_clear();
				if (GetCurrentTimestamp() >= phase3_recovery_deadline
					|| !cluster_phase3_wait_for_live_formation(
						phase3_recovery_deadline, false, &formation_result,
						&formation_origin_thread, &formation_authority, &formation_snapshot)
					|| !cluster_authority_readiness_begin(
						formation_origin_thread, &formation_authority, &formation_snapshot)) {
					bind_failed = true;
					break;
				}
				pg_usleep(20000L);
				continue;
			}
			boot_incarnation = cluster_qvotec_get_self_incarnation();
			lms_remaining_ms = cluster_phase_remaining_budget_ms(phase3_recovery_deadline, 5000);
			if (cluster_grd_recovery_authority_barrier_wait(&formation_snapshot, boot_incarnation,
															lms_generation, lms_remaining_ms)) {
				if (cluster_authority_readiness_publish_recovery(lms_generation))
					break;
			} else if (cluster_phase3_try_pre_publish_join_readonly_pivot(phase3_recovery_deadline,
																		  lms_generation)) {
				return PHASE_RUN_OK;
			}
			if (GetCurrentTimestamp() >= phase3_recovery_deadline) {
				barrier_failed = true;
				break;
			}
			/* Re-fetch the live formation and re-bind before the next
			 * barrier attempt; begin() only accepts OFF, so drop the
			 * stale binding first. */
			cluster_authority_readiness_clear();
			if (!cluster_phase3_wait_for_live_formation(phase3_recovery_deadline, false,
														&formation_result, &formation_origin_thread,
														&formation_authority, &formation_snapshot)
				|| !cluster_authority_readiness_begin(formation_origin_thread, &formation_authority,
													  &formation_snapshot)) {
				bind_failed = true;
				break;
			}
			boot_incarnation = cluster_qvotec_get_self_incarnation();
			/* Pace the retry so a persistently unavailable barrier cannot
			 * spin the Postmaster without yielding; the phase-3 deadline
			 * still bounds the whole loop. */
			pg_usleep(20000L);
		}

		if (bind_failed || barrier_failed
			|| cluster_authority_readiness_get() != CLUSTER_AUTHORITY_RECOVERY_READY) {
			cluster_authority_readiness_clear();
			fail_ctx->errcode = ERRCODE_CLUSTER_LMS_UNAVAILABLE;
			if (barrier_failed) {
				fail_ctx->errmsg = "cluster phase 3: authoritative recovery GRD is unavailable";
				fail_ctx->errhint
					= "Recovery requires an explicit current-generation holder "
					  "authority seal; an empty or uninitialized GRD is insufficient.";
			} else {
				fail_ctx->errmsg = "cluster phase 3: recovery LMS generation could not be bound";
				fail_ctx->errhint = "The LMS recovery generation, live formation, and admitted "
									"incarnation must remain exact before holder remastering.";
			}
			return PHASE_RUN_FATAL;
		}
	}
	phase3_join_readonly_deferred = false;

	elog(DEBUG1,
		 "cluster phase 3: CSSD ready (pid %d) + QVOTEC ready (pid %d); "
		 "PG-native recovery starts with formation authority available",
		 phase3_cssd_pid, phase3_qvotec_pid);
	return PHASE_RUN_OK;
}


/* Complete a peer-attaching JOIN_READONLY boot inside the one phase-4
 * publication budget.  StartupXLOG has already returned and ordinary
 * stripe/JOIN-WAL/JCMK admission is terminal-positive.  The readiness FSM is
 * unchanged: OFF -> STARTING -> RECOVERY_READY, followed by the existing
 * phase-4 RECOVERY_READY -> SERVING_READY transition. */
static PhaseRunResult
cluster_phase4_establish_join_readonly_authority(TimestampTz deadline,
												 PhaseRunFailContext *fail_ctx)
{
	ClusterFenceAuthorityProof formation_authority;
	ClusterFormationSnapshotV1 formation_snapshot;
	ClusterFormationWitnessResult formation_result;
	uint16 formation_origin_thread = 0;
	uint64 boot_incarnation;
	uint64 lms_generation;
	int lms_remaining_ms;
	bool bind_failed = false;
	bool barrier_failed = false;

	if (!phase3_join_readonly_deferred || cluster_current_phase() != CLUSTER_PHASE_4_NORMAL
		|| !cluster_cf_phase2_peer_verified() || !cluster_reconfig_self_join_admitted()) {
		fail_ctx->errcode = ERRCODE_CLUSTER_WAL_RETENTION_BLOCKED;
		fail_ctx->errmsg = "cluster phase 4: JOIN_READONLY admission is not current";
		fail_ctx->errhint = "The fresh peer proof and exact stripe/JOIN-WAL/JCMK "
							"admission must remain current before authority publication.";
		return PHASE_RUN_FATAL;
	}
	if (!cluster_phase3_wait_for_live_formation(deadline, false, &formation_result,
												&formation_origin_thread, &formation_authority,
												&formation_snapshot)) {
		fail_ctx->errcode = ERRCODE_CLUSTER_WAL_RETENTION_BLOCKED;
		fail_ctx->errmsg = "cluster phase 4: admitted JOIN_READONLY formation is unavailable";
		fail_ctx->errhint = "The current admitted formation and durable voting "
							"authority must become exact within the phase-4 deadline.";
		return PHASE_RUN_FATAL;
	}
	if (!cluster_lms_enabled) {
		fail_ctx->errcode = ERRCODE_CLUSTER_LMS_UNAVAILABLE;
		fail_ctx->errmsg = "cluster phase 4: LMS is disabled for admitted JOIN_READONLY";
		fail_ctx->errhint = "Set cluster.lms_enabled=on; no native serving fallback exists.";
		return PHASE_RUN_FATAL;
	}
	if (!cluster_authority_readiness_begin(formation_origin_thread, &formation_authority,
										   &formation_snapshot)) {
		fail_ctx->errcode = ERRCODE_CLUSTER_WAL_RETENTION_BLOCKED;
		fail_ctx->errmsg = "cluster phase 4: admitted JOIN_READONLY formation could not bind";
		fail_ctx->errhint = "The admission incarnation and formation must remain exact.";
		return PHASE_RUN_FATAL;
	}

	if (phase3_lms_pid <= 0 || cluster_lms_get_pid() != phase3_lms_pid)
		phase3_lms_pid = cluster_lms_start();
	if (phase3_lms_pid <= 0) {
		cluster_authority_readiness_clear();
		fail_ctx->errcode = ERRCODE_CLUSTER_LMS_UNAVAILABLE;
		fail_ctx->errmsg = "cluster phase 4: failed to spawn admitted JOIN_READONLY LMS";
		fail_ctx->errhint = "Check postmaster diagnostics and process limits.";
		return PHASE_RUN_FATAL;
	}
	lms_remaining_ms = cluster_phase_remaining_budget_ms(deadline, 5000);
	if (!cluster_lms_wait_for_recovery_ready(lms_remaining_ms)) {
		cluster_authority_readiness_clear();
		fail_ctx->errcode = ERRCODE_CLUSTER_LMS_UNAVAILABLE;
		fail_ctx->errmsg = "cluster phase 4: admitted JOIN_READONLY LMS is not recovery-ready";
		fail_ctx->errhint = "The LMS generation must become ready within the phase-4 deadline.";
		return PHASE_RUN_FATAL;
	}

	for (;;) {
		lms_generation = cluster_lms_get_lms_restart_generation();
		if (!cluster_authority_readiness_bind_recovery_generation(lms_generation)) {
			cluster_authority_readiness_clear();
			if (GetCurrentTimestamp() >= deadline
				|| !cluster_phase3_wait_for_live_formation(
					deadline, false, &formation_result, &formation_origin_thread,
					&formation_authority, &formation_snapshot)
				|| !cluster_authority_readiness_begin(formation_origin_thread, &formation_authority,
													  &formation_snapshot)) {
				bind_failed = true;
				break;
			}
			pg_usleep(20000L);
			continue;
		}

		boot_incarnation = cluster_qvotec_get_self_incarnation();
		lms_remaining_ms = cluster_phase_remaining_budget_ms(deadline, 5000);
		if (cluster_grd_recovery_authority_barrier_wait(&formation_snapshot, boot_incarnation,
														lms_generation, lms_remaining_ms)
			&& cluster_authority_readiness_publish_recovery(lms_generation))
			break;
		if (GetCurrentTimestamp() >= deadline) {
			barrier_failed = true;
			break;
		}
		cluster_authority_readiness_clear();
		if (!cluster_phase3_wait_for_live_formation(deadline, false, &formation_result,
													&formation_origin_thread, &formation_authority,
													&formation_snapshot)
			|| !cluster_authority_readiness_begin(formation_origin_thread, &formation_authority,
												  &formation_snapshot)) {
			bind_failed = true;
			break;
		}
		pg_usleep(20000L);
	}

	if (bind_failed || barrier_failed
		|| cluster_authority_readiness_get() != CLUSTER_AUTHORITY_RECOVERY_READY) {
		cluster_authority_readiness_clear();
		fail_ctx->errcode = ERRCODE_CLUSTER_LMS_UNAVAILABLE;
		fail_ctx->errmsg
			= barrier_failed
				  ? "cluster phase 4: admitted JOIN_READONLY GRD barrier is unavailable"
				  : "cluster phase 4: admitted JOIN_READONLY LMS generation could not bind";
		fail_ctx->errhint = "Formation, admission, LMS generation, and GRD seal "
							"must remain exact within the phase-4 deadline.";
		return PHASE_RUN_FATAL;
	}

	phase3_join_readonly_deferred = false;
	return PHASE_RUN_OK;
}


/*
 * cluster_phase_remaining_budget_ms -- single-deadline remaining budget.
 *
 *	Returns max(deadline - now - driver_buffer_ms, 100ms floor).  Each
 *	child wait inside phase_4_handler computes its budget off the same
 *	phase4_deadline so the sum of all children's wait times cannot
 *	exceed cluster.phase4_timeout.  Without this, two serial waits
 *	(DIAG + Cluster Stats) each of (phase4_timeout - 5s) would total
 *	2 * (30s - 5s) = 50s, breaching the 30s contract.
 *
 *	Driver buffer (5s default) is reserved for the outer driver's
 *	TimestampDifferenceExceeds check so 53R09 PHASE_TRANSITION_TIMEOUT
 *	can still trip cleanly if a child wait runs to its full slice.
 *	Min 100ms floor guarantees at least one polling iteration so
 *	wait_for_ready does not spuriously fail on a tight overflow.
 */
static int
cluster_phase_remaining_budget_ms(TimestampTz deadline, int driver_buffer_ms)
{
	long secs;
	int microsecs;
	long remaining_ms;

	TimestampDifference(GetCurrentTimestamp(), deadline, &secs, &microsecs);
	remaining_ms = secs * 1000 + microsecs / 1000 - driver_buffer_ms;
	return remaining_ms > 100 ? (int)remaining_ms : 100;
}


static bool
cluster_phase4_wal_state_configured(void)
{
	return cluster_enabled && cluster_wal_threads_dir != NULL && cluster_wal_threads_dir[0] != '\0';
}


static bool
cluster_phase4_wait_for_quorum(TimestampTz deadline)
{
	for (;;) {
		if (cluster_qvotec_in_quorum())
			return true;
		if (GetCurrentTimestamp() >= deadline)
			return false;
		pg_usleep(100000L);
	}
}


static void
cluster_validate_running_configuration(void)
{
	if (cluster_phase4_wal_state_configured() && !cluster_controlfile_shared_authority)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_CONTROLFILE_AUTHORITY_UNAVAILABLE),
						errmsg("formed WAL registry requires shared control-file authority"),
						errhint("Set cluster.controlfile_shared_authority=on and restart all "
								"members on the same RF A1 binary.")));

	if (cluster_enabled && !SCN_NODE_ID_VALID(cluster_node_id)) {
		if (cluster_allow_single_node) {
			ereport(WARNING, (errcode(ERRCODE_WARNING),
							  errmsg("cluster.node_id (%d) is outside the valid range 0..%d; "
									 "cluster SCN advance will silently skip",
									 cluster_node_id, SCN_MAX_VALID_NODE_ID),
							  errhint("Set cluster.node_id in postgresql.conf to an integer 0..127 "
									  "to enable SCN advance, or set cluster.enabled = off for "
									  "vanilla PG behaviour.  Currently running in single-node "
									  "compatibility mode (cluster.allow_single_node = on).  Set "
									  "cluster.allow_single_node = off to enforce strict mode.")));
		} else {
			ereport(FATAL, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("cluster.node_id (%d) is outside the valid range 0..%d",
								   cluster_node_id, SCN_MAX_VALID_NODE_ID),
							errhint("Set cluster.node_id in postgresql.conf to an integer 0..127, "
									"or set cluster.allow_single_node = on for single-node "
									"compatibility mode.")));
		}
	}

	if (cluster_enabled && cluster_conf_node_count() > 1 && !cluster_allow_single_node) {
		const char *vd = cluster_voting_disks;
		bool empty = true;

		if (vd != NULL) {
			while (*vd) {
				if (*vd != ' ' && *vd != '\t' && *vd != ',') {
					empty = false;
					break;
				}
				vd++;
			}
		}

		if (empty)
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("multi-node cluster requires cluster.voting_disks to be "
							"configured when cluster.allow_single_node=off"),
					 errdetail("pgrac.conf declares %d nodes but cluster.voting_disks is "
							   "empty.  Without voting disks the cluster has no quorum "
							   "protocol and backends would silently fail-open under partition.",
							   cluster_conf_node_count()),
					 errhint("Set cluster.voting_disks in postgresql.conf to a comma-"
							 "separated list of pre-formatted voting-disk file paths "
							 "(odd majority recommended: 1 / 3 / 5 / 7 disks across "
							 "distinct failure domains), or set cluster.allow_single_node = on "
							 "for single-node development mode.")));
	}
}


static PhaseRunResult
cluster_phase4_start_stats(PhaseRunFailContext *fail_ctx, TimestampTz deadline, int *stats_pid)
{
	int remaining_ms;

	*stats_pid = cluster_stats_start();
	if (*stats_pid <= 0) {
		fail_ctx->errcode = ERRCODE_CLUSTER_STATS_SPAWN_FAILED;
		fail_ctx->errmsg = "cluster phase 4: failed to spawn Cluster Stats aux process";
		fail_ctx->errhint = "Check postmaster log for fork() error.  Confirm OS process "
							"limits (ulimit -u) leave room for the Cluster Stats aux "
							"process; if the limit is exhausted, raise it via ulimit "
							"or systemd LimitNPROC and restart postmaster.";
		return PHASE_RUN_FATAL;
	}

	remaining_ms = cluster_phase_remaining_budget_ms(deadline, 5000);
	if (!cluster_stats_wait_for_ready(remaining_ms)) {
		fail_ctx->errcode = ERRCODE_CLUSTER_STATS_NOT_READY;
		fail_ctx->errmsg = "cluster phase 4: Cluster Stats did not publish READY in time";
		fail_ctx->errhint = "Check postmaster log for Cluster Stats-side errors.  If "
							"Cluster Stats is slow on this hardware, raise "
							"cluster.phase4_timeout (PGC_SIGHUP).";
		return PHASE_RUN_FATAL;
	}

	return PHASE_RUN_OK;
}


static PhaseRunResult
phase_4_handler(PhaseRunFailContext *fail_ctx)
{
	int diag_pid;
	int stats_pid;
	int cssd_pid;
	int qvotec_pid;
	int diag_remaining_ms;
	int lms_pid = 0;
	int lms_remaining_ms;
	TimestampTz phase4_start;
	TimestampTz phase4_deadline;
	bool registry_configured;

	Assert(!IsUnderPostmaster);

	/*
	 * HC4 (spec-1.13 §1.4 #4 / spec-1.14 §1.4): if cluster.enabled =
	 * false, phase 4 degrades to spec-1.10 stub behavior — no DIAG
	 * spawn AND no Cluster Stats spawn, no FATAL.  Tested by 063 L10
	 * (DIAG-only) + 064 L10 (DIAG + Cluster Stats双 process disabled).
	 */
	if (!cluster_enabled) {
		elog(DEBUG1, "cluster phase 4: cluster.enabled=false; skipping DIAG + "
					 "Cluster Stats + CSSD + QVOTEC spawn (degraded to spec-1.10 "
					 "stub behavior).  PG-native walwriter / bgwriter / "
					 "checkpointer / autovacuum spawn unchanged.");
		return PHASE_RUN_OK;
	}

	/*
	 * spec-1.14 Q3 user 修订: phase 4 single deadline pattern.
	 *
	 *	Both child waits below compute remaining budget off the same
	 *	phase4_deadline (= phase4_start + cluster.phase4_timeout).
	 *	This caps the sum of all child waits so phase 4 cannot run
	 *	past cluster.phase4_timeout regardless of how many children
	 *	live in this phase (1.14: DIAG + Cluster Stats; Stage 2+ may
	 *	add Sinval Broadcaster etc.).
	 */
	phase4_start = GetCurrentTimestamp();
	phase4_deadline = TimestampTzPlusMilliseconds(
		phase4_start, cluster_phase_timeout_for(CLUSTER_PHASE_4_NORMAL) * 1000);
	registry_configured = cluster_phase4_wal_state_configured();

	/*
	 * CSSD and QVOTEC are phase-3 children.  Phase 4 may only reuse their
	 * existing READY state; a lost pre-recovery authority must fail closed
	 * rather than spawning a second child generation.
	 */
	cssd_pid = phase3_cssd_pid;
	if (cluster_cssd_get_status() != CLUSTER_CSSD_READY) {
		fail_ctx->errcode = ERRCODE_CLUSTER_CSSD_NOT_READY;
		fail_ctx->errmsg = "cluster phase 4: pre-recovery CSSD is no longer READY";
		fail_ctx->errhint = "Inspect the CSSD child failure and restart after cluster "
							"membership authority is available.";
		return PHASE_RUN_FATAL;
	}

	qvotec_pid = phase3_qvotec_pid;
	if (cluster_qvotec_get_status() != CLUSTER_QVOTEC_READY) {
		fail_ctx->errcode = ERRCODE_CLUSTER_QVOTEC_NOT_READY;
		fail_ctx->errmsg = "cluster phase 4: pre-recovery QVOTEC is no longer READY";
		fail_ctx->errhint = "Inspect the QVOTEC child failure and restart after voting "
							"authority is available.";
		return PHASE_RUN_FATAL;
	}

	if (registry_configured && cluster_conf_node_count() > 1
		&& !cluster_phase4_wait_for_quorum(phase4_deadline)) {
		fail_ctx->errcode = ERRCODE_CLUSTER_QUORUM_LOST;
		fail_ctx->errmsg = "cluster phase 4: QVOTEC quorum was lost after recovery";
		fail_ctx->errhint = "Restore a voting-disk majority and verify the lease-aware "
							"quorum state before retrying startup.";
		return PHASE_RUN_FATAL;
	}

	/* A peer-attaching JOIN_READONLY boot deliberately reached StartupXLOG
	 * without recovery authority.  After the unchanged admission terminal,
	 * build that authority here under this same phase-4 absolute deadline;
	 * the ordinary phase-3 path and all already-admitted boots are unchanged. */
	if (registry_configured && phase3_join_readonly_deferred
		&& cluster_phase4_establish_join_readonly_authority(phase4_deadline, fail_ctx)
			   == PHASE_RUN_FATAL)
		return PHASE_RUN_FATAL;

	/* ----------
	 * spec-1.13 D6: DIAG spawn + sync wait ready (first phase 4 child).
	 * ----------
	 */
	diag_pid = cluster_diag_start();
	if (diag_pid <= 0) {
		fail_ctx->errcode = ERRCODE_CLUSTER_DIAG_SPAWN_FAILED;
		fail_ctx->errmsg = "cluster phase 4: failed to spawn DIAG aux process";
		fail_ctx->errhint = "Check postmaster log for fork() error.  Confirm OS process "
							"limits (ulimit -u) leave room for the DIAG aux process; if "
							"the limit is exhausted, raise it via ulimit or systemd "
							"LimitNPROC and restart postmaster.";
		return PHASE_RUN_FATAL;
	}

	diag_remaining_ms = cluster_phase_remaining_budget_ms(phase4_deadline, 5000);
	if (!cluster_diag_wait_for_ready(diag_remaining_ms)) {
		fail_ctx->errcode = ERRCODE_CLUSTER_DIAG_NOT_READY;
		fail_ctx->errmsg = "cluster phase 4: DIAG did not publish READY in time";
		fail_ctx->errhint = "Check postmaster log for DIAG-side errors.  If DIAG is "
							"slow on this hardware, raise cluster.phase4_timeout (PGC_SIGHUP).";
		return PHASE_RUN_FATAL;
	}

	/* A flat/unconfigured registry keeps the pre-RF child order unchanged. */
	if (!registry_configured
		&& cluster_phase4_start_stats(fail_ctx, phase4_deadline, &stats_pid) == PHASE_RUN_FATAL)
		return PHASE_RUN_FATAL;

	/*
	 * A formed registry requires an exact READY LMS.  The legacy LMS
	 * wait helper treats DISABLED as ready-or-skip, so reject the GUC
	 * state first and re-check the exact predicate after the wait.
	 */
	if (registry_configured) {
		if (!cluster_lms_enabled) {
			fail_ctx->errcode = ERRCODE_CLUSTER_LMS_UNAVAILABLE;
			fail_ctx->errmsg = "cluster phase 4: LMS is disabled for a formed WAL registry";
			fail_ctx->errhint = "Set cluster.lms_enabled=on and restart all members on the "
								"same RF A1 binary.";
			return PHASE_RUN_FATAL;
		}

		lms_pid = phase3_lms_pid;
		if (lms_pid <= 0) {
			fail_ctx->errcode = ERRCODE_CLUSTER_LMS_UNAVAILABLE;
			fail_ctx->errmsg = "cluster phase 4: recovery LMS authority is unavailable";
			fail_ctx->errhint = "Restart after the phase-3 LMS generation and authority "
								"binding are available.";
			return PHASE_RUN_FATAL;
		}

		/*
		 * AD-023 A1 contract: every Postmaster read of the volatile phase
		 * state is a conditional acquire that must never queue.  A transient
		 * contention therefore shows up as an unavailable read, not a hang,
		 * and the contract requires retrying inside the existing phase4
		 * deadline instead of treating one miss as terminal.
		 */
		for (;;) {
			if (cluster_authority_readiness_get() == CLUSTER_AUTHORITY_RECOVERY_READY)
				break;
			if (GetCurrentTimestamp() >= phase4_deadline) {
				fail_ctx->errcode = ERRCODE_CLUSTER_LMS_UNAVAILABLE;
				fail_ctx->errmsg = "cluster phase 4: recovery LMS authority is unavailable";
				fail_ctx->errhint = "Restart after the phase-3 LMS generation and authority "
									"binding are available.";
				return PHASE_RUN_FATAL;
			}
			pg_usleep(20000L);
		}
		/* AD-023 recovery-control authority is sufficient for StartupXLOG,
		 * never for ordinary service.  LMON keeps retrying the original
		 * stripe join gate after recovery; wait for that exact admission byte
		 * before issuing the existing LMS SERVING request. */
		for (;;) {
			if (cluster_reconfig_self_join_admitted())
				break;
			if (GetCurrentTimestamp() >= phase4_deadline) {
				fail_ctx->errcode = ERRCODE_CLUSTER_LMS_UNAVAILABLE;
				fail_ctx->errmsg = "cluster phase 4: xid stripe admission is unavailable";
				fail_ctx->errhint = "Recovery control authority cannot publish ordinary "
									"service until the post-recovery xid stripe floor is durable.";
				return PHASE_RUN_FATAL;
			}
			pg_usleep(20000L);
		}
		/* The phase-3 durable-marker cache is intentionally finite and can
		 * expire while StartupXLOG completes.  Re-read the current durable
		 * authority only after the exact stripe gate opens, and accept it only
		 * when it is the same immutable marker/formation already bound to this
		 * recovery generation. */
		for (;;) {
			ClusterFormationWitnessResult refresh_result;
			int refresh_ms = cluster_phase_remaining_budget_ms(phase4_deadline, 5000);

			if (refresh_ms > 100)
				refresh_ms = 100;
			refresh_result = cluster_phase4_refresh_serving_formation(refresh_ms);
			if (refresh_result == CLUSTER_FORMATION_WITNESS_READY)
				break;
			if ((refresh_result != CLUSTER_FORMATION_WITNESS_OWNER_MISMATCH
				 && refresh_result != CLUSTER_FORMATION_WITNESS_UNSTABLE
				 && refresh_result != CLUSTER_FORMATION_WITNESS_MARKER_UNPROVEN
				 && refresh_result != CLUSTER_FORMATION_WITNESS_IO_FAILED
				 && refresh_result != CLUSTER_FORMATION_WITNESS_CAPABILITY_UNAVAILABLE)
				|| GetCurrentTimestamp() >= phase4_deadline) {
				cluster_authority_readiness_clear();
				fail_ctx->errcode = ERRCODE_CLUSTER_LMS_UNAVAILABLE;
				fail_ctx->errmsg = "cluster phase 4: serving formation proof is unavailable";
				fail_ctx->errhint = "The post-recovery durable marker and exact admitted "
									"formation must still match the phase-3 authority binding.";
				return PHASE_RUN_FATAL;
			}
			pg_usleep(20000L);
		}
		if (!cluster_lms_request_serving()) {
			cluster_authority_readiness_clear();
			fail_ctx->errcode = ERRCODE_CLUSTER_LMS_UNAVAILABLE;
			fail_ctx->errmsg = "cluster phase 4: could not upgrade recovery LMS to service";
			fail_ctx->errhint = "The exact phase-3 LMS generation must remain recovery-ready.";
			return PHASE_RUN_FATAL;
		}
		lms_remaining_ms = cluster_phase_remaining_budget_ms(phase4_deadline, 5000);
		if (!cluster_lms_wait_for_ready(lms_remaining_ms) || !cluster_lms_is_ready()) {
			cluster_authority_readiness_clear();
			fail_ctx->errcode = ERRCODE_CLUSTER_LMS_UNAVAILABLE;
			fail_ctx->errmsg = "cluster phase 4: LMS did not publish exact READY in time";
			fail_ctx->errhint = "Inspect LMS startup diagnostics; DISABLED or any non-READY "
								"state cannot authorize a formed-registry CF update.";
			return PHASE_RUN_FATAL;
		}
		/*
		 * Same A1 retry discipline for the serving publication: retry the
		 * conditional phase-state read inside the phase4 deadline, then fail
		 * closed only when the budget is exhausted or the authoritative
		 * predicate itself reports stale.
		 */
		for (;;) {
			if (cluster_authority_readiness_publish_serving())
				break;
			if (GetCurrentTimestamp() >= phase4_deadline) {
				cluster_authority_readiness_clear();
				fail_ctx->errcode = ERRCODE_CLUSTER_LMS_UNAVAILABLE;
				fail_ctx->errmsg = "cluster phase 4: serving authority publication failed";
				fail_ctx->errhint = "A stale formation, QVOTEC incarnation, LMS generation, or "
									"GRD seal cannot publish ordinary GES/GCS service.";
				return PHASE_RUN_FATAL;
			}
			pg_usleep(20000L);
		}

		cluster_validate_running_configuration();
	}

	/*
	 * With a formed registry Stats is deliberately last.  Its child owns W2
	 * and publishes READY only after ACTIVE post-read and checkpoint return.
	 */
	if (registry_configured
		&& cluster_phase4_start_stats(fail_ctx, phase4_deadline, &stats_pid) == PHASE_RUN_FATAL)
		return PHASE_RUN_FATAL;

	if (registry_configured)
		elog(DEBUG1,
			 "cluster phase 4: DIAG ready (pid %d) + CSSD ready (pid %d) + "
			 "QVOTEC ready (pid %d) + LMS ready (pid %d) + Cluster Stats ready "
			 "(pid %d).  PG-native processes unchanged.",
			 diag_pid, cssd_pid, qvotec_pid, lms_pid, stats_pid);
	else
		elog(DEBUG1,
			 "cluster phase 4: DIAG ready (pid %d) + Cluster Stats ready (pid %d) + "
			 "CSSD ready (pid %d) + QVOTEC ready (pid %d).  PG-native "
			 "processes unchanged.",
			 diag_pid, stats_pid, cssd_pid, qvotec_pid);

	return PHASE_RUN_OK;
}


/* ============================================================
 * Sequence drivers (HC1 postmaster-only)
 * ============================================================ */

/*
 * Static dispatch table from phase to handler.  Indexed by
 * ClusterStartupPhase enum value.  PRE_INIT / 0_BASE / RUNNING /
 * SHUTDOWN have NULL because they don't run a phase handler -- their
 * transitions are driven directly by cluster_advance_phase().
 */
typedef PhaseRunResult (*ClusterPhaseHandler)(PhaseRunFailContext *fail_ctx);

static const ClusterPhaseHandler phase_handlers[CLUSTER_PHASE_LAST + 1]
	= { [CLUSTER_PHASE_PRE_INIT] = NULL,
		[CLUSTER_PHASE_0_BASE] = NULL,
		[CLUSTER_PHASE_1_CLUSTER] = phase_1_handler,
		[CLUSTER_PHASE_2_LOCK] = phase_2_handler,
		[CLUSTER_PHASE_3_RECOVERY] = phase_3_handler,
		[CLUSTER_PHASE_4_NORMAL] = phase_4_handler,
		[CLUSTER_PHASE_RUNNING] = NULL,
		[CLUSTER_PHASE_SHUTDOWN] = NULL };


/*
 * cluster_phase_timeout_for -- read the GUC seconds value for a phase.
 *
 *	Spec-1.10.1 D2 F2 / Q2=D: driver synchronous elapsed check uses
 *	this helper to fetch the deadline that was promised by GUC help
 *	text.  Phase 0 has no GUC (skeleton trivial); only phases 1..4
 *	are bounded.
 */
static int
cluster_phase_timeout_for(ClusterStartupPhase phase)
{
	switch (phase) {
	case CLUSTER_PHASE_1_CLUSTER:
		return cluster_phase1_timeout;
	case CLUSTER_PHASE_2_LOCK:
		return cluster_phase2_timeout;
	case CLUSTER_PHASE_3_RECOVERY:
		return cluster_phase3_timeout;
	case CLUSTER_PHASE_4_NORMAL:
		return cluster_phase4_timeout;
	default:
		/* phases without timeout GUC (PRE_INIT / 0_BASE / RUNNING / SHUTDOWN) */
		return 0;
	}
}


/*
 * cluster_phase_fail_inject -- fire the per-phase "-fail" inject point.
 *
 *	Spec-1.10.1 D6 F6: the driver invokes this on every FATAL exit
 *	path (handler PHASE_RUN_FATAL + driver elapsed timeout) so the
 *	previously dead cluster-startup-phase-N-fail injection points are
 *	reachable from tests.  Inject framework treats this as a no-op
 *	when no fault is armed.
 */
static void
cluster_phase_fail_inject(ClusterStartupPhase phase)
{
	switch (phase) {
	case CLUSTER_PHASE_1_CLUSTER:
		CLUSTER_INJECTION_POINT("cluster-startup-phase-1-fail");
		break;
	case CLUSTER_PHASE_2_LOCK:
		CLUSTER_INJECTION_POINT("cluster-startup-phase-2-fail");
		break;
	case CLUSTER_PHASE_3_RECOVERY:
		CLUSTER_INJECTION_POINT("cluster-startup-phase-3-fail");
		break;
	case CLUSTER_PHASE_4_NORMAL:
		CLUSTER_INJECTION_POINT("cluster-startup-phase-4-fail");
		break;
	default:
		break;
	}
}


void
cluster_run_startup_sequence(void)
{
	ClusterStartupPhase phase;

	/*
	 * HC1 PostmasterMain-only.  This must be called from PostmasterMain
	 * function body, NOT from inside CreateSharedMemoryAndSemaphores
	 * (the latter is also called by SubPostmasterMain on EXEC_BACKEND
	 * children; running phase machinery there would violate Postmaster-
	 * once semantics, CLAUDE.md rule 16 §Postmaster-once).
	 */
	Assert(!IsUnderPostmaster);

	CLUSTER_INJECTION_POINT("cluster-run-startup-top");

	/*
	 * Driver loop.  Walk Phase 0 -> 1 -> 2 -> 3 -> 4.  The driver
	 * advances; handlers only do work + return status (HC3).  Phase
	 * RUNNING is NOT advanced from this driver -- spec-1.10.1 D4 F4
	 * pushes that transition to cluster_finalize_startup_running()
	 * called from PostmasterMain just before ServerLoop() so phase=
	 * running accurately reflects "PostgreSQL ready to accept
	 * connections", not just "pgrac skeleton finished".
	 *
	 * Phase 0 entry is the "post-shmem ready" point reached by the
	 * caller (PostmasterMain after CreateSharedMemoryAndSemaphores +
	 * cluster_init).  We advance into 0_BASE here as the explicit
	 * skeleton starting point.
	 */
	phase3_join_readonly_deferred = false;
	phase3_recovery_deadline = 0;
	cluster_advance_phase(CLUSTER_PHASE_0_BASE);

	/*
	 * Spec-1.13 v0.2 Q2 A': cluster_run_startup_sequence() walks
	 * phase 1 -> 3 ONLY.  Phase 4 (the post-recovery / post-PM_RUN
	 * normal-running phase that DIAG and Cluster Stats spawn into)
	 * is driven by cluster_run_phase4_sequence() below, which the
	 * postmaster reaper invokes after the startup process has finished
	 * recovery and pmState transitions to PM_RUN.  Splitting the
	 * driver moves DIAG spawn from "pre-recovery" (the original 1.10
	 * skeleton timing) to "post-recovery / DB OPEN" (correct Oracle
	 * DIAG semantics).
	 */
	for (phase = CLUSTER_PHASE_1_CLUSTER; phase <= CLUSTER_PHASE_3_RECOVERY; phase++) {
		PhaseRunResult result;
		ClusterPhaseHandler handler;
		TimestampTz started;
		TimestampTz now;
		long elapsed_secs;
		int microsecs;
		int timeout_secs;

		/*
		 * Spec-1.10.1 D2 F2 / Q2=D: driver synchronous elapsed check.
		 *
		 *	Record the phase wall-clock start before cluster_advance_
		 *	phase() so the measured interval covers both the transition
		 *	overhead (including any -enter inject point sleeps used by
		 *	tests to simulate a stuck phase) AND the handler body.
		 *	After handler returns the driver compares elapsed against
		 *	the per-phase GUC timeout and ereport(FATAL) on overrun.
		 *
		 *	Handlers MUST self-bound any blocking wait via WaitLatch
		 *	(..., timeout_ms, WAIT_EVENT_CLUSTER_STARTUP_PHASE_N) per
		 *	the contract in cluster_startup_phase.h; a handler that
		 *	hangs without using WaitLatch+timeout will hang the entire
		 *	postmaster, defeating this enforcement.
		 */
		PhaseRunFailContext fail_ctx = { 0 };

		started = GetCurrentTimestamp();
		cluster_advance_phase(phase);

		handler = phase_handlers[(int)phase];
		/*
		 * Every iterated phase (1..4) has a handler defined in
		 * phase_handlers[].  If a future amend leaves a slot NULL we
		 * fail loudly rather than dereference it (cppcheck flagged
		 * the prior Assert-only form as a potential null deref).
		 */
		if (handler == NULL)
			ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("cluster startup phase %s has no handler in dispatch table",
								   cluster_startup_phase_to_string(phase))));

		/*
		 * Spec-1.11.1 F13 (codex round 4 P2/P3 fix): handler writes
		 * PHASE_RUN_FATAL diagnostic into fail_ctx (errcode/errmsg/
		 * errhint).  Driver loop's FATAL path uses fail_ctx values
		 * when non-zero/non-NULL, otherwise falls back to generic
		 * 53R09 PHASE_PRECONDITION_FAILED + standard message.
		 */
		result = handler(&fail_ctx);

		/*
		 * Spec-1.10.2 F8 (2026-05-04 codex review fix): use
		 * TimestampDifferenceExceeds for millisecond-precision boundary
		 * checking.  The prior comparison `elapsed_secs > timeout_secs`
		 * floored sub-second elapsed and yielded false negatives when
		 * elapsed was 1.0s..1.999s with timeout_secs == 1 (boundary
		 * leak).  TimestampDifferenceExceeds(start, stop, ms) returns
		 * true iff (stop - start) exceeds ms, with full us precision.
		 *
		 * Capture `now` once so the FATAL errmsg reports the same
		 * sample the deadline was checked against.
		 */
		now = GetCurrentTimestamp();
		timeout_secs = cluster_phase_timeout_for(phase);

		if (timeout_secs > 0 && TimestampDifferenceExceeds(started, now, timeout_secs * 1000)) {
			TimestampDifference(started, now, &elapsed_secs, &microsecs);
			cluster_phase_fail_inject(phase);
			ereport(FATAL, (errcode(ERRCODE_CLUSTER_PHASE_TRANSITION_TIMEOUT),
							errmsg("cluster startup phase %s exceeded timeout (%ld.%03d s > %d s)",
								   cluster_startup_phase_to_string(phase), elapsed_secs,
								   microsecs / 1000, timeout_secs),
							errhint("Increase cluster.phase%d_timeout GUC or "
									"fix handler hang (handler must self-bound "
									"blocking waits via WaitLatch+timeout per the "
									"phase handler contract).",
									(int)phase - 1)));
		}

		/*
		 * Spec-1.10.1 D3 F3: explicit switch on PhaseRunResult.  The
		 * earlier Assert(result == PHASE_RUN_OK) tripped only in debug
		 * builds, leaving production builds to silently advance on
		 * unknown values.  Handle every case explicitly.
		 */
		switch (result) {
		case PHASE_RUN_OK:
			break;

		case PHASE_RUN_FATAL:
			cluster_phase_fail_inject(phase);
			/*
			 * Spec-1.11.1 F13: use handler-supplied SQLSTATE / errmsg
			 * / errhint when present (e.g. 53R0A LMON_SPAWN_FAILED for
			 * phase_1_handler), else fall back to generic 53R09
			 * PHASE_PRECONDITION_FAILED.
			 */
			ereport(FATAL,
					(errcode(fail_ctx.errcode != 0 ? fail_ctx.errcode
												   : ERRCODE_CLUSTER_PHASE_PRECONDITION_FAILED),
					 errmsg("cluster startup phase %s failed: %s",
							cluster_startup_phase_to_string(phase),
							fail_ctx.errmsg != NULL ? fail_ctx.errmsg
													: "see postmaster log for diagnostics"),
					 errhint("%s",
							 fail_ctx.errhint != NULL
								 ? fail_ctx.errhint
								 : "spec-1.10 / spec-1.11+ document the phase handler contract.")));
			break;

		case PHASE_RUN_RETRY:
			/*
			 * RETRY enum value is reserved for spec-1.11+ retry semantics
			 * (per-phase retry count + backoff schedule).  At spec-1.10.1
			 * the driver does not implement retry, so a RETRY return is a
			 * programming error: ereport(FATAL) rather than silently
			 * advancing.
			 */
			cluster_phase_fail_inject(phase);
			ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("cluster startup phase %s returned PHASE_RUN_RETRY "
								   "but driver does not implement retry yet",
								   cluster_startup_phase_to_string(phase)),
							errhint("Future spec must define per-phase retry "
									"count + backoff before handlers may return "
									"PHASE_RUN_RETRY.")));
			break;

		default:
			cluster_phase_fail_inject(phase);
			ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("cluster startup phase %s handler returned unknown "
								   "PhaseRunResult %d",
								   cluster_startup_phase_to_string(phase), (int)result)));
			break;
		}
	}

	/* Driver leaves phase machinery at CLUSTER_PHASE_3_RECOVERY.
	 * cluster_run_phase4_sequence() advances to CLUSTER_PHASE_4_NORMAL
	 * later from the reaper PM_RUN transition path. */
}


/*
 * cluster_run_phase4_sequence -- spec-1.13 v0.2 Q2 A' driver for the
 * post-recovery / post-PM_RUN phase 4 transition.
 *
 *	Walks just CLUSTER_PHASE_4_NORMAL by invoking phase_4_handler.
 *	Same per-phase timeout / FATAL / SQLSTATE machinery as
 *	cluster_run_startup_sequence().  Caller (postmaster reaper at
 *	PM_STARTUP -> PM_RUN transition) invokes this AFTER the startup
 *	process has succeeded.  cluster_finalize_startup_running() runs
 *	immediately after to advance phase machinery to RUNNING.
 *
 *	Why split: spec-1.10 originally walked phase 1->4 inside
 *	cluster_run_startup_sequence() in PostmasterMain, before
 *	StartupDataBase().  That made phase 4 fire pre-recovery, which
 *	would mis-position DIAG (1.13) and Cluster Stats (1.14) into the
 *	WAL replay window.  Round 5 (user codex review) caught this; the
 *	fix is the driver split: phase 1-3 stay in startup_sequence;
 *	phase 4 lives here.
 */
void
cluster_run_phase4_sequence(void)
{
	PhaseRunResult result;
	ClusterPhaseHandler handler;
	TimestampTz started;
	TimestampTz now;
	long elapsed_secs;
	int microsecs;
	int timeout_secs;
	const ClusterStartupPhase phase = CLUSTER_PHASE_4_NORMAL;
	PhaseRunFailContext fail_ctx = { 0 };

	/* JOIN_READONLY runs StartupXLOG with every shared-authority mutation gate
	 * closed.  Before crossing into phase 4, observe the unchanged
	 * stripe/JOIN-WAL/JCMK owner until it admits or rejects this boot.  This
	 * loop owns no deadline: classification/WAIT_EVIDENCE must not consume an
	 * admission budget, and LMON remains the sole join-timeout owner. */
	if (phase3_join_readonly_deferred) {
		while (!cluster_reconfig_self_join_admitted()) {
			ClusterMembershipState self_state = cluster_membership_get_state(cluster_node_id);

			if (self_state == CLUSTER_MEMBER_REJECTED || self_state == CLUSTER_MEMBER_REMOVED)
				break;
			pg_usleep(20000L);
		}
		if (!cluster_reconfig_self_join_admitted())
			ereport(FATAL,
					(errcode(ERRCODE_CLUSTER_JOIN_REJECTED_STALE),
					 errmsg("cluster startup phase %s failed: JOIN_READONLY admission rejected",
							cluster_startup_phase_to_string(CLUSTER_PHASE_4_NORMAL)),
					 errhint("The ordinary stripe/JOIN-WAL/JCMK owner reached its "
							 "existing fail-closed terminal before phase 4.")));
	}

	Assert(!IsUnderPostmaster);

	CLUSTER_INJECTION_POINT("cluster-run-phase4-top");

	started = GetCurrentTimestamp();
	cluster_advance_phase(phase);

	handler = phase_handlers[(int)phase];
	if (handler == NULL)
		ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster startup phase %s has no handler in dispatch table",
							   cluster_startup_phase_to_string(phase))));

	result = handler(&fail_ctx);

	now = GetCurrentTimestamp();
	timeout_secs = cluster_phase_timeout_for(phase);

	if (timeout_secs > 0 && TimestampDifferenceExceeds(started, now, timeout_secs * 1000)) {
		TimestampDifference(started, now, &elapsed_secs, &microsecs);
		cluster_phase_fail_inject(phase);
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_PHASE_TRANSITION_TIMEOUT),
						errmsg("cluster startup phase %s exceeded timeout (%ld.%03d s > %d s)",
							   cluster_startup_phase_to_string(phase), elapsed_secs,
							   microsecs / 1000, timeout_secs),
						errhint("Increase cluster.phase%d_timeout GUC or "
								"fix handler hang.",
								(int)phase - 1)));
	}

	switch (result) {
	case PHASE_RUN_OK:
		break;

	case PHASE_RUN_FATAL:
		cluster_phase_fail_inject(phase);
		ereport(FATAL, (errcode(fail_ctx.errcode != 0 ? fail_ctx.errcode
													  : ERRCODE_CLUSTER_PHASE_PRECONDITION_FAILED),
						errmsg("cluster startup phase %s failed: %s",
							   cluster_startup_phase_to_string(phase),
							   fail_ctx.errmsg != NULL ? fail_ctx.errmsg
													   : "see postmaster log for diagnostics"),
						errhint("%s", fail_ctx.errhint != NULL
										  ? fail_ctx.errhint
										  : "spec-1.13 documents the phase 4 handler contract.")));
		break;

	case PHASE_RUN_RETRY:
		cluster_phase_fail_inject(phase);
		ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster startup phase %s returned PHASE_RUN_RETRY "
							   "but driver does not implement retry yet",
							   cluster_startup_phase_to_string(phase))));
		break;

	default:
		cluster_phase_fail_inject(phase);
		ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster startup phase %s handler returned unknown "
							   "PhaseRunResult %d",
							   cluster_startup_phase_to_string(phase), (int)result)));
		break;
	}
}


void
cluster_finalize_startup_running(void)
{
	/*
	 * Spec-1.10.1 D4 F4: explicit RUNNING transition is now the
	 * responsibility of PostmasterMain (just before ServerLoop()) so
	 * that "phase=running" reflects PG-ready, not just pgrac-skeleton-
	 * finished.  HC1 postmaster-only.
	 *
	 * Spec-1.11.1 F9: idempotent guard.  Crash reinit also calls this
	 * to advance phase 4 -> RUNNING after the reinit cycle completes;
	 * normal startup calls it once from PostmasterMain.  An already-
	 * RUNNING state is a benign no-op rather than a strict +1
	 * cluster_advance_phase failure.
	 */
	Assert(!IsUnderPostmaster);

	if (cluster_current_phase() == CLUSTER_PHASE_RUNNING)
		return;

	/* Formed-registry startup ran these validators and self-check in Stats. */
	if (!cluster_phase4_wal_state_configured()) {
		cluster_validate_running_configuration();
		if (cluster_write_fence_startup_self_check())
			ereport(WARNING,
					(errcode(ERRCODE_CLUSTER_WRITE_FENCED),
					 errmsg("this node is fenced by a membership reconfiguration; "
							"entering non-serving mode"),
					 errdetail("A durable quorum-majority voting-disk marker still lists "
							   "this node as dead.  All shared-storage writes are rejected "
							   "(53R51) and this node publishes no serving authority."),
					 errhint("Recover only via the controlled rejoin / cold-admin procedure "
							 "once the cluster confirms this node may rejoin; never clear the "
							 "fence marker manually while the cluster is live.")));
	}

	cluster_advance_phase(CLUSTER_PHASE_RUNNING);
}


void
cluster_run_shutdown_sequence(void)
{
	Assert(!IsUnderPostmaster);

	CLUSTER_INJECTION_POINT("cluster-run-shutdown-top");

	/*
	 * Spec-1.10.1 D5 F5: this entry is now wired into pmdie() in
	 * postmaster.c after children have been reaped.  pmdie may invoke
	 * the shutdown sequence more than once if a smart shutdown is
	 * upgraded to fast / immediate shutdown; cluster_advance_phase()
	 * permits any -> SHUTDOWN transition (including SHUTDOWN -> SHUTDOWN
	 * is rejected by the strict +1 check, so guard against re-entry).
	 *
	 * Stage 1.10 stub: directly transition to SHUTDOWN.  Reverse-order
	 * graceful tear-down (RUNNING -> 4 -> 3 -> 2 -> 1 -> SHUTDOWN) is
	 * deferred to 1.11-1.14 / Stage 6 once the per-phase background
	 * processes that need graceful stop are spawned.
	 */
	if (cluster_current_phase() == CLUSTER_PHASE_SHUTDOWN)
		return;

	cluster_authority_readiness_clear();
	cluster_advance_phase(CLUSTER_PHASE_SHUTDOWN);
}

#endif /* USE_PGRAC_CLUSTER */
