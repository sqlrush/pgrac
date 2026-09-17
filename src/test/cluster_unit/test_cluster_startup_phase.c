/*-------------------------------------------------------------------------
 *
 * test_cluster_startup_phase.c
 *	  Compile-time / link-level invariants for the cluster startup
 *	  phase machinery shipped at stage 1.10.
 *
 *	  Locks:
 *	    - ClusterStartupPhase enum values (PRE_INIT=0, 0_BASE=1, ...,
 *	      SHUTDOWN=7) are frozen.  Spec-1.10 §1.5 HC2 SSOT relies on
 *	      these specific integer values.
 *	    - CLUSTER_PHASE_LAST == CLUSTER_PHASE_SHUTDOWN (the array
 *	      bounds in cluster_phase_start_times depend on this).
 *	    - CLUSTER_PHASE_HISTORY_RING_SIZE == 8 (HC5 fixed-size ring;
 *	      avoids unbounded string accumulation under reconfig phase
 *	      reentry in Stage 6).
 *	    - cluster_startup_phase_to_string() returns non-null for every
 *	      enum value and "(unknown)" for out-of-range values.
 *	    - Public symbols cluster_advance_phase / cluster_run_startup_
 *	      sequence / cluster_run_shutdown_sequence resolve at link
 *	      time.
 *
 *	  Behavior-level tests (transition rejection of backward / skip,
 *	  Postmaster-once Assert(!IsUnderPostmaster) firing) live in TAP
 *	  t/060_postmaster_phases.pl because they depend on postmaster
 *	  startup orchestration that is not reproducible at unit test level.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_startup_phase.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Linking cluster_startup_phase.o
 *	  standalone pulls in references to ereport, GetCurrentTimestamp,
 *	  timestamptz_to_str, the cluster_phase legacy mirror, and the
 *	  cluster_inject framework; the test stubs every one of those
 *	  because we only take addresses + read enum constants.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_cssd.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_recovery_duty.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_startup_phase.h"
#include "cluster/cluster_wal_retention.h"
#include "storage/proc.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include <setjmp.h>

#include "unit_test.h"

#ifndef STARTUP_PHASE_SOURCE_PATH
#error "STARTUP_PHASE_SOURCE_PATH must identify production cluster_startup_phase.c"
#endif

static char *
read_startup_phase_source(void)
{
	FILE *file;
	char *source;
	long length;

	file = fopen(STARTUP_PHASE_SOURCE_PATH, "rb");
	if (file == NULL)
		return NULL;
	if (fseek(file, 0, SEEK_END) != 0 || (length = ftell(file)) < 0
		|| fseek(file, 0, SEEK_SET) != 0) {
		fclose(file);
		return NULL;
	}
	source = malloc((size_t)length + 1);
	if (source == NULL) {
		fclose(file);
		return NULL;
	}
	if (fread(source, 1, (size_t)length, file) != (size_t)length) {
		free(source);
		fclose(file);
		return NULL;
	}
	source[length] = '\0';
	fclose(file);
	return source;
}


/* ----------
 * Stubs needed to link cluster_startup_phase.o standalone.  None of
 * these paths run during the unit test -- we only take addresses and
 * exercise pure-function APIs (cluster_startup_phase_to_string).
 * ----------
 */

#include "cluster/cluster_inject.h"
int cluster_injection_armed_count = 0;
char *cluster_injection_points = NULL;
void
cluster_injection_run(const char *name pg_attribute_unused())
{}

/* miscadmin: HC1 Assert reads IsUnderPostmaster. */
bool IsUnderPostmaster = false;
PGPROC *MyProc = NULL;

/* cluster_phase legacy mirror (HC2 derived).  Real backend gets it from
 * cluster_elog.o; the unit test provides a local writable storage so
 * cluster_advance_phase's mirror update has somewhere to land. */
const char *cluster_phase = "pre_init";

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/* ereport machinery; the quorum-negative test captures the expected FATAL. */
static jmp_buf phase4_fatal_jump;
static bool phase4_capture_fatal = false;
static int phase4_last_elevel = 0;

bool
errstart(int e, const char *d pg_attribute_unused())
{
	phase4_last_elevel = e;
	return phase4_capture_fatal;
}

/* Stage 8 contract (verified implementation): worker identity for the recovery
 * lock-admission gate — this binary never runs inside the hw-remaster
 * bgworker, so the worker window is always closed here. */
bool
cluster_hw_remaster_worker_active(void)
{
	return false;
}
bool
errstart_cold(int e, const char *d pg_attribute_unused())
{
	phase4_last_elevel = e;
	return phase4_capture_fatal;
}
void
errfinish(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		  const char *fn pg_attribute_unused())
{
	if (phase4_capture_fatal && phase4_last_elevel >= ERROR)
		longjmp(phase4_fatal_jump, 1);
}
int
errcode(int s pg_attribute_unused())
{
	return 0;
}
int
errmsg(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
int
errmsg_internal(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
int
errdetail(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
int
errhint(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
void
elog_start(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		   const char *fn pg_attribute_unused())
{}
void
elog_finish(int e pg_attribute_unused(), const char *f pg_attribute_unused(), ...)
{}
void
pre_format_elog_string(int n pg_attribute_unused(), const char *d pg_attribute_unused())
{}
char *
format_elog_string(const char *f pg_attribute_unused(), ...)
{
	return NULL;
}

/* Test clock: pg_usleep advances it without wall-clock delay. */
static TimestampTz phase4_test_now = 0;

TimestampTz
GetCurrentTimestamp(void)
{
	return phase4_test_now;
}
void
TimestampDifference(TimestampTz start_time pg_attribute_unused(),
					TimestampTz stop_time pg_attribute_unused(), long *secs, int *microsecs)
{
	*secs = 0;
	*microsecs = 0;
}

bool
TimestampDifferenceExceeds(TimestampTz start_time pg_attribute_unused(),
						   TimestampTz stop_time pg_attribute_unused(),
						   int msec pg_attribute_unused())
{
	return false;
}
const char *
timestamptz_to_str(TimestampTz dt pg_attribute_unused())
{
	return "(stub)";
}

void
pg_usleep(long microsec)
{
	phase4_test_now += microsec;
}

/* pg_snprintf: cluster_startup_phase.c uses snprintf (macro'd to
 * pg_snprintf in PG).  Forward to libc vsnprintf in unit test. */
#include <stdarg.h>
int
pg_snprintf(char *str, size_t count, const char *fmt, ...)
{
	int n;
	va_list ap;

	va_start(ap, fmt);
	n = vsnprintf(str, count, fmt, ap);
	va_end(ap);
	return n;
}


/*
 * Spec-1.10.1 D1 F1 stubs: cluster_startup_phase.o now references
 * LWLock + ShmemInitStruct (phase state migrated to shmem) +
 * cluster.phase{1..4}_timeout GUC variables (D2 F2 driver elapsed
 * check) + cluster_shmem_register_region (registry).  The runtime
 * tests below only exercise pure-function APIs; the stubs are
 * address-only / no-op.
 */
#include "storage/lwlock.h"
#include "storage/shmem.h"
static bool phase_lwlock_conditional_result = true;
static int phase_lwlock_blocking_calls = 0;
static int phase_lwlock_conditional_calls = 0;

void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}

bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	phase_lwlock_blocking_calls++;
	return true;
}

bool
LWLockConditionalAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	phase_lwlock_conditional_calls++;
	return phase_lwlock_conditional_result;
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{}

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size pg_attribute_unused(),
				bool *foundPtr)
{
	/* AD-023 A1: the phase state now embeds pg_atomic_uint32 words; the
	 * fake region must be maximally aligned so the atomic loads/stores
	 * never touch an under-aligned address (arm64 traps on that). */
	static char fake_shmem[4096] pg_attribute_aligned(MAXIMUM_ALIGNOF);

	memset(fake_shmem, 0, sizeof(fake_shmem));
	if (foundPtr != NULL)
		*foundPtr = false;
	return fake_shmem;
}

int cluster_phase1_timeout = 60;
int cluster_phase2_timeout = 30;
int cluster_phase3_timeout = 600;
int cluster_phase4_timeout = 30;
/* Spec-1.11 Sprint B: cluster_startup_phase.c references cluster_enabled */
bool cluster_enabled = true;
bool cluster_lms_enabled = true;
bool cluster_controlfile_shared_authority = true;
char *cluster_wal_threads_dir = "/rf-a1/formed";
/* Spec-1.16 D13: cluster_finalize_startup_running references cluster_node_id
 * for SCN_NODE_ID_VALID validation.  Pin to 0 (valid) so unit test does
 * not trip the FATAL ereport path; behavioral test lives in TAP 060 L19. */
int cluster_node_id = 0;
/* Spec-2.1 D1: cluster_finalize_startup_running references allow_single_node;
 * stub matches storage default (true) so WARNING/FATAL dual path stays
 * on WARNING side -- unit test pins node_id = 0 (valid) anyway so this
 * stub value is moot.  Behavioral validation in TAP 072. */
bool cluster_allow_single_node = true;
/* spec-2.6 Q7 validator: cluster_startup_phase.c reads cluster_voting_disks */
char *cluster_voting_disks = NULL;
/* spec-2.6 Q7 validator: cluster_startup_phase.c reads cluster_conf_node_count */
int
cluster_conf_node_count(void)
{
	return 4;
}

#include "cluster/cluster_shmem.h"
void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}

/* spec-4.12 D6 stub: cluster_startup_phase.o references the rejoin self-fence gate. */
static int startup_self_check_calls = 0;
bool cluster_write_fence_startup_self_check(void);
bool
cluster_write_fence_startup_self_check(void)
{
	startup_self_check_calls++;
	return false;
}

static char phase4_events[512];
static int phase4_event_count = 0;
static bool phase4_test_in_quorum = true;
static int phase4_quorum_check_calls = 0;
static ClusterCssdStatus phase_test_cssd_status = CLUSTER_CSSD_DOWN;
static ClusterQvotecStatus phase_test_qvotec_status = CLUSTER_QVOTEC_DOWN;
static bool phase_test_cssd_spawn_ok = true;
static bool phase_test_cssd_ready_ok = true;
static bool phase_test_qvotec_spawn_ok = true;
static bool phase_test_qvotec_ready_ok = true;
static ClusterFormationWitnessResult phase_test_formation_result = CLUSTER_FORMATION_WITNESS_READY;
static int phase_test_formation_unavailable_attempts = 0;
static int64 phase_test_formation_unavailable_advance_us = 0;
static bool phase_test_classification_current = true;
static bool phase_test_grd_authority_ok = true;
static bool phase_test_lms_recovery_ready_ok = true;
static uint64 phase_test_lms_generation = 7;
static int phase_test_lms_start_calls = 0;
static int phase_test_lms_pid = 0;
static uint8 phase_test_formation_epoch = 1;
static bool phase_test_expire_formation_during_lms = false;
static bool phase_test_formation_expired = false;
static int phase_test_live_formation_calls = 0;
static int phase_test_recovery_control_formation_calls = 0;
static bool phase_test_live_refreshes_classification = false;
static bool phase_test_cf_peer_verified = false;
static bool phase_test_epoch0_late_founder_evidence = false;
static bool phase_test_late_founder_after_grd_barrier = false;
static bool phase_test_publish_recovery_after_grd_barrier = false;
static bool phase_test_drift_lms_generation_after_grd_barrier = false;
static int phase_test_grd_barrier_calls = 0;
static int phase_test_join_readonly_handoff_calls = 0;
static uint64 phase_test_join_readonly_handoff_incarnation = 0;
static uint64 phase_test_join_readonly_handoff_predecessor_floor = 0;
static uint64 phase_test_last_admitted_incarnation = 11;
/* RF-ROOT P6 (L4/L5 wiring): steady-state defaults for the membership /
 * reconfig / GRD accessor stubs added below. */
static bool phase_test_membership_member = true;
static ClusterMembershipState phase_test_nonmember_state = CLUSTER_MEMBER_JOINING;
static bool phase_test_self_join_admitted = false;
static uint64 phase_test_episode_epoch = 0;
static bool phase_test_join_remaster = false;
/* RF-ROOT P6 (contract-verify-early): the phase-3 handler passes DataDir to
 * the verify stub; the pure unit harness has no data directory. */
char *DataDir = NULL;

static void
record_phase4_event(char event)
{
	UT_ASSERT(phase4_event_count < (int)sizeof(phase4_events) - 1);
	phase4_events[phase4_event_count++] = event;
	phase4_events[phase4_event_count] = '\0';
}

/* Spec-1.11 Sprint A stubs (cluster_startup_phase.o references). */
int
cluster_lmon_start(void)
{
	return 9;
}

bool
cluster_lmon_wait_for_ready(int timeout_ms pg_attribute_unused())
{
	return true;
}

/* Spec-1.12 stubs. */
int
cluster_lck_start(void)
{
	return 10;
}
bool
cluster_lck_wait_for_ready(int timeout_ms pg_attribute_unused())
{
	return true;
}

/* Spec-1.13 stubs. */
int
cluster_diag_start(void)
{
	record_phase4_event('D');
	return 11;
}
bool
cluster_diag_wait_for_ready(int timeout_ms pg_attribute_unused())
{
	record_phase4_event('d');
	return true;
}

/* Spec-1.14 stubs. */
int
cluster_stats_start(void)
{
	record_phase4_event('S');
	return 15;
}
bool
cluster_stats_wait_for_ready(int timeout_ms pg_attribute_unused())
{
	record_phase4_event('s');
	return true;
}

/* Spec-2.5 stubs. */
int
cluster_cssd_start(void)
{
	record_phase4_event('C');
	if (!phase_test_cssd_spawn_ok)
		return 0;
	phase_test_cssd_status = CLUSTER_CSSD_STARTING;
	return 12;
}
bool
cluster_cssd_wait_for_ready(int timeout_ms pg_attribute_unused())
{
	record_phase4_event('c');
	if (!phase_test_cssd_ready_ok)
		return false;
	phase_test_cssd_status = CLUSTER_CSSD_READY;
	return true;
}
ClusterCssdStatus
cluster_cssd_get_status(void)
{
	return phase_test_cssd_status;
}
pid_t
cluster_cssd_get_pid(void)
{
	return phase_test_cssd_status == CLUSTER_CSSD_DOWN ? 0 : 12;
}

/* spec-2.6 Sprint A Step 3 D7 stubs. */
pid_t
cluster_qvotec_start(void)
{
	record_phase4_event('Q');
	if (!phase_test_qvotec_spawn_ok)
		return 0;
	phase_test_qvotec_status = CLUSTER_QVOTEC_STARTING;
	return 13;
}
bool
cluster_qvotec_wait_for_ready(int timeout_ms pg_attribute_unused())
{
	record_phase4_event('q');
	if (!phase_test_qvotec_ready_ok)
		return false;
	phase_test_qvotec_status = CLUSTER_QVOTEC_READY;
	return true;
}
int
cluster_qvotec_get_status(void)
{
	return phase_test_qvotec_status;
}
int
cluster_qvotec_get_pid(void)
{
	return phase_test_qvotec_status == CLUSTER_QVOTEC_DOWN ? 0 : 13;
}
bool
cluster_qvotec_in_quorum(void)
{
	phase4_quorum_check_calls++;
	if (phase4_quorum_check_calls == 1)
		record_phase4_event('V');
	return phase4_test_in_quorum;
}

uint16
cluster_wal_thread_id(void)
{
	return 1;
}

ClusterFormationWitnessResult
cluster_formation_witness_build_live_wait(uint16 origin_thread pg_attribute_unused(),
										  int timeout_ms pg_attribute_unused(),
										  ClusterFormationWitnessV1 **out)
{
	phase_test_live_formation_calls++;
	record_phase4_event('F');
	if (phase_test_live_refreshes_classification)
		phase_test_classification_current = true;
	if (phase_test_formation_unavailable_attempts > 0) {
		phase_test_formation_unavailable_attempts--;
		phase4_test_now += phase_test_formation_unavailable_advance_us;
		*out = NULL;
		return CLUSTER_FORMATION_WITNESS_CAPABILITY_UNAVAILABLE;
	}
	*out = phase_test_formation_result == CLUSTER_FORMATION_WITNESS_READY
			   ? (ClusterFormationWitnessV1 *)(uintptr_t)1
			   : NULL;
	return phase_test_formation_result;
}

ClusterFormationWitnessResult
cluster_formation_witness_build_recovery_control_wait(uint16 origin_thread pg_attribute_unused(),
													  int timeout_ms pg_attribute_unused(),
													  ClusterFormationWitnessV1 **out)
{
	phase_test_recovery_control_formation_calls++;
	record_phase4_event('F');
	if (phase_test_formation_unavailable_attempts > 0) {
		phase_test_formation_unavailable_attempts--;
		phase4_test_now += phase_test_formation_unavailable_advance_us;
		*out = NULL;
		return CLUSTER_FORMATION_WITNESS_CAPABILITY_UNAVAILABLE;
	}
	*out = phase_test_formation_result == CLUSTER_FORMATION_WITNESS_READY
			   ? (ClusterFormationWitnessV1 *)(uintptr_t)1
			   : NULL;
	return phase_test_formation_result;
}

void
cluster_formation_witness_destroy(ClusterFormationWitnessV1 **witness)
{
	*witness = NULL;
}

bool
cluster_formation_witness_copy_classification_v1(
	const ClusterFormationWitnessV1 *witness pg_attribute_unused(), uint16 *origin_thread,
	ClusterFenceAuthorityProof *authority, ClusterFormationSnapshotV1 *snapshot)
{
	record_phase4_event('X');
	memset(authority, 0, sizeof(*authority));
	memset(snapshot, 0, sizeof(*snapshot));
	*origin_thread = 1;
	snapshot->membership.membership_state[0] = CLUSTER_MEMBER_MEMBER;
	snapshot->membership.last_admitted_incarnation[0] = phase_test_last_admitted_incarnation;
	snapshot->reserved[0] = phase_test_formation_epoch;
	return true;
}

ClusterFormationWitnessResult
cluster_formation_witness_revalidate_nowait(
	const ClusterFormationWitnessV1 *witness pg_attribute_unused())
{
	return phase_test_classification_current ? CLUSTER_FORMATION_WITNESS_READY
											 : CLUSTER_FORMATION_WITNESS_UNSTABLE;
}

ClusterFormationWitnessResult
cluster_formation_classification_revalidate_nowait(
	uint16 origin_thread pg_attribute_unused(),
	const ClusterFenceAuthorityProof *authority pg_attribute_unused(),
	const ClusterFormationSnapshotV1 *snapshot pg_attribute_unused())
{
	return phase_test_classification_current && snapshot->reserved[0] == phase_test_formation_epoch
			   ? CLUSTER_FORMATION_WITNESS_READY
			   : CLUSTER_FORMATION_WITNESS_UNSTABLE;
}

bool
cluster_formation_snapshot_matches_v1(const ClusterFormationSnapshotV1 *expected,
									  const ClusterFormationSnapshotV1 *observed)
{
	return expected != NULL && observed != NULL
		   && memcmp(expected, observed, sizeof(*expected)) == 0;
}

bool
cluster_reconfig_capture_formation_snapshot_v1(uint16 origin_thread,
											   ClusterFormationSnapshotV1 *snapshot)
{
	if (origin_thread != 1 || snapshot == NULL)
		return false;
	memset(snapshot, 0, sizeof(*snapshot));
	snapshot->membership.membership_state[0] = CLUSTER_MEMBER_MEMBER;
	snapshot->membership.last_admitted_incarnation[0] = phase_test_last_admitted_incarnation;
	snapshot->reserved[0] = phase_test_formation_epoch;
	return true;
}

uint64
cluster_qvotec_get_self_incarnation(void)
{
	return 11;
}

uint64
cluster_membership_get_last_admitted_incarnation(int32 node_id pg_attribute_unused())
{
	return phase_test_last_admitted_incarnation;
}

bool
cluster_grd_recovery_authority_barrier_wait(
	const ClusterFormationSnapshotV1 *formation pg_attribute_unused(), uint64 boot_incarnation,
	uint64 lms_generation, int timeout_ms pg_attribute_unused())
{
	bool saved_grd_authority_ok;

	record_phase4_event('G');
	phase_test_grd_barrier_calls++;
	if (phase_test_late_founder_after_grd_barrier)
		phase_test_epoch0_late_founder_evidence = true;
	if (phase_test_publish_recovery_after_grd_barrier) {
		saved_grd_authority_ok = phase_test_grd_authority_ok;
		phase_test_grd_authority_ok = true;
		UT_ASSERT(cluster_authority_readiness_publish_recovery(lms_generation));
		phase_test_grd_authority_ok = saved_grd_authority_ok;
	}
	if (phase_test_drift_lms_generation_after_grd_barrier)
		phase_test_lms_generation++;
	return phase_test_grd_authority_ok && boot_incarnation == 11
		   && lms_generation == phase_test_lms_generation;
}

bool
cluster_grd_recovery_authority_is_current(uint64 boot_incarnation, uint64 lms_generation)
{
	return phase_test_grd_authority_ok && boot_incarnation == 11
		   && lms_generation == phase_test_lms_generation;
}

bool
cluster_grd_serving_authority_rebind_lmon(const ClusterFormationSnapshotV1 *formation,
										  uint64 boot_incarnation, uint64 lms_generation)
{
	return phase_test_grd_authority_ok && formation != NULL
		   && formation->reserved[0] == phase_test_formation_epoch && boot_incarnation == 11
		   && lms_generation == phase_test_lms_generation;
}

/* RF-ROOT P6 (L5 leaver serving rebind): cluster_startup_phase.o references
 * the leaver-side GRD rebind; the pure unit pins it to the same authority
 * gate as the LMON rebind above. */
bool
cluster_grd_serving_authority_rebind_leaver(const ClusterFormationSnapshotV1 *formation,
											uint64 boot_incarnation, uint64 lms_generation)
{
	return phase_test_grd_authority_ok && formation != NULL
		   && formation->reserved[0] == phase_test_formation_epoch && boot_incarnation == 11
		   && lms_generation == phase_test_lms_generation;
}

/* RF-ROOT P6 (L4/L5 wiring): unit stubs for the membership / reconfig / GRD
 * accessors the startup-phase predicates consult.  Deterministic, mirroring
 * the production semantics in the pure unit's single-process harness. */
bool
cluster_membership_is_member(int32 node_id pg_attribute_unused())
{
	return phase_test_membership_member;
}

ClusterMembershipState
cluster_membership_get_state(int32 node_id pg_attribute_unused())
{
	return phase_test_membership_member ? CLUSTER_MEMBER_MEMBER : phase_test_nonmember_state;
}

bool
cluster_reconfig_self_join_admitted(void)
{
	return phase_test_self_join_admitted;
}

bool
cluster_reconfig_epoch0_late_founder_evidence_current(void)
{
	return phase_test_epoch0_late_founder_evidence;
}

bool
cluster_reconfig_stage_pre_publish_join_handoff(uint64 expected_self_incarnation,
												uint64 expected_predecessor_floor)
{
	phase_test_join_readonly_handoff_calls++;
	phase_test_join_readonly_handoff_incarnation = expected_self_incarnation;
	phase_test_join_readonly_handoff_predecessor_floor = expected_predecessor_floor;
	phase_test_membership_member = false;
	phase_test_nonmember_state = CLUSTER_MEMBER_JOINING;
	return expected_self_incarnation == 11 && expected_predecessor_floor == 10;
}

uint64
cluster_grd_recovery_episode_epoch_value(void)
{
	return phase_test_episode_epoch;
}

bool
cluster_grd_join_remaster_in_progress(void)
{
	return phase_test_join_remaster;
}

/* RF-ROOT P6 (contract-verify-early + THREAD_OPEN wiring): unit stubs for the
 * phase-3 handler's new collaborators.  Deliberate no-ops — the pure unit
 * harness has no DataDir/control-root/epoch subsystem, and the phase4 event-
 * sequence assertions above are exact strings. */
void
cluster_cf_phase2_verify_or_fail(const char *datadir pg_attribute_unused())
{}

bool
cluster_cf_phase2_peer_verified(void)
{
	return phase_test_cf_peer_verified;
}

bool
cluster_control_root_thread_open_publish(uint64 boot_incarnation pg_attribute_unused())
{
	return false;
}

uint64
cluster_epoch_get_current(void)
{
	return phase_test_formation_epoch;
}

/* spec-2.18 Sprint A stubs. */
int
cluster_lms_start(void)
{
	phase_test_lms_start_calls++;
	phase_test_lms_pid = 14;
	record_phase4_event('L');
	return phase_test_lms_pid;
}
pid_t
cluster_lms_get_pid(void)
{
	return phase_test_lms_pid;
}
bool
cluster_lms_wait_for_recovery_ready(int timeout_ms pg_attribute_unused())
{
	record_phase4_event('r');
	if (phase_test_expire_formation_during_lms && !phase_test_formation_expired) {
		phase_test_formation_epoch++;
		phase_test_formation_expired = true;
	}
	return phase_test_lms_recovery_ready_ok;
}
bool
cluster_lms_is_recovery_ready(void)
{
	return phase_test_lms_recovery_ready_ok;
}
uint64
cluster_lms_get_lms_restart_generation(void)
{
	return phase_test_lms_generation;
}
void
cluster_lms_wakeup(int worker_id)
{
	UT_ASSERT_EQ(worker_id, 0);
	record_phase4_event('W');
}
bool
cluster_lms_request_serving(void)
{
	cluster_lms_wakeup(0);
	return phase_test_lms_recovery_ready_ok;
}
bool
cluster_lms_wait_for_ready(int timeout_ms pg_attribute_unused())
{
	record_phase4_event('l');
	return true;
}
bool
cluster_lms_is_ready(void)
{
	record_phase4_event('E');
	return true;
}

static void
reset_phase_service_fixture(bool formed_registry)
{
	phase4_event_count = 0;
	phase4_events[0] = '\0';
	phase4_test_now = 0;
	phase4_test_in_quorum = true;
	phase4_quorum_check_calls = 0;
	phase_test_cssd_status = CLUSTER_CSSD_DOWN;
	phase_test_qvotec_status = CLUSTER_QVOTEC_DOWN;
	phase_test_cssd_spawn_ok = true;
	phase_test_cssd_ready_ok = true;
	phase_test_qvotec_spawn_ok = true;
	phase_test_qvotec_ready_ok = true;
	phase_test_formation_result = CLUSTER_FORMATION_WITNESS_READY;
	phase_test_formation_unavailable_attempts = 0;
	phase_test_formation_unavailable_advance_us = 0;
	phase_test_classification_current = true;
	phase_test_grd_authority_ok = true;
	phase_test_lms_recovery_ready_ok = true;
	phase_test_lms_generation = 7;
	phase_test_lms_start_calls = 0;
	phase_test_lms_pid = 0;
	phase_test_formation_epoch = 1;
	phase_test_expire_formation_during_lms = false;
	phase_test_formation_expired = false;
	phase_test_live_formation_calls = 0;
	phase_test_recovery_control_formation_calls = 0;
	phase_test_live_refreshes_classification = false;
	phase_test_cf_peer_verified = false;
	phase_test_epoch0_late_founder_evidence = false;
	phase_test_late_founder_after_grd_barrier = false;
	phase_test_publish_recovery_after_grd_barrier = false;
	phase_test_drift_lms_generation_after_grd_barrier = false;
	phase_test_grd_barrier_calls = 0;
	phase_test_join_readonly_handoff_calls = 0;
	phase_test_join_readonly_handoff_incarnation = 0;
	phase_test_join_readonly_handoff_predecessor_floor = 0;
	phase_test_last_admitted_incarnation = 11;
	phase_test_membership_member = true;
	phase_test_nonmember_state = CLUSTER_MEMBER_JOINING;
	phase_test_self_join_admitted = true;
	cluster_enabled = true;
	cluster_wal_threads_dir = formed_registry ? "/rf-a1/formed" : NULL;
	cluster_phase_shmem_init();
}


UT_DEFINE_GLOBALS();


/* spec-4.2 D3 stub: cluster_startup_phase.c publishes ACTIVE to the
 * WAL-state registry at the phase->RUNNING transition; the registry
 * module is not linked here (L104). */
static int postmaster_publish_active_calls = 0;
void cluster_wal_state_publish_active(void);
void
cluster_wal_state_publish_active(void)
{
	postmaster_publish_active_calls++;
}


/* ============================================================
 * Compile-time anchors
 * ============================================================ */

UT_TEST(test_phase_enum_values_frozen)
{
	/*
	 * Spec-1.10 §1.5 HC2 SSOT: these specific integer values are
	 * relied upon by pg_cluster_state.phase.phase_enum_value (int4
	 * field exposed to user) and by 030 acceptance §S.  Changing them
	 * is a public-facing breakage.
	 */
	UT_ASSERT_EQ((int)CLUSTER_PHASE_PRE_INIT, 0);
	UT_ASSERT_EQ((int)CLUSTER_PHASE_0_BASE, 1);
	UT_ASSERT_EQ((int)CLUSTER_PHASE_1_CLUSTER, 2);
	UT_ASSERT_EQ((int)CLUSTER_PHASE_2_LOCK, 3);
	UT_ASSERT_EQ((int)CLUSTER_PHASE_3_RECOVERY, 4);
	UT_ASSERT_EQ((int)CLUSTER_PHASE_4_NORMAL, 5);
	UT_ASSERT_EQ((int)CLUSTER_PHASE_RUNNING, 6);
	UT_ASSERT_EQ((int)CLUSTER_PHASE_SHUTDOWN, 7);
}


UT_TEST(test_phase_last_is_shutdown)
{
	/*
	 * Spec-1.10 enum total at Stage 1.10 is 8 values; CLUSTER_PHASE_
	 * LAST is used as the upper bound when sizing per-phase arrays
	 * (e.g. cluster_phase_start_times in cluster_startup_phase.c).
	 */
	UT_ASSERT_EQ((int)CLUSTER_PHASE_LAST, (int)CLUSTER_PHASE_SHUTDOWN);
	UT_ASSERT_EQ((int)CLUSTER_PHASE_LAST, 7);
}


UT_TEST(test_phase_history_ring_size_is_eight)
{
	/*
	 * Spec-1.10 §1.5 HC5: fixed-size ring at 8 entries to bound
	 * pg_cluster_state.phase.phase_history string length and prevent
	 * Stage 6 reconfig phase reentry from causing unbounded growth.
	 */
	UT_ASSERT_EQ((int)CLUSTER_PHASE_HISTORY_RING_SIZE, 8);
}


UT_TEST(test_phase_string_lookup_returns_non_null_for_each_value)
{
	int i;

	for (i = 0; i <= (int)CLUSTER_PHASE_LAST; i++) {
		const char *s = cluster_startup_phase_to_string((ClusterStartupPhase)i);

		UT_ASSERT_NOT_NULL(s);
		/*
		 * Defensive: cppcheck doesn't model UT_ASSERT_NOT_NULL's abort
		 * semantics, so the explicit `s != NULL` keeps the static
		 * analyser happy without relaxing the assertion's meaning.
		 */
		if (s != NULL)
			UT_ASSERT(s[0] != '\0');
	}
}


UT_TEST(test_phase_string_lookup_invalid_returns_unknown)
{
	const char *neg = cluster_startup_phase_to_string((ClusterStartupPhase)-1);
	const char *over
		= cluster_startup_phase_to_string((ClusterStartupPhase)((int)CLUSTER_PHASE_LAST + 1));

	UT_ASSERT_STR_EQ(neg, "(unknown)");
	UT_ASSERT_STR_EQ(over, "(unknown)");
}


/* ============================================================
 * Public symbol linkability
 *
 *	If any of these unresolves at link time, this test binary will
 *	fail to build.  Taking the address (cast to void *) is enough to
 *	pin link-time presence without invoking the body.
 * ============================================================ */

UT_TEST(test_public_symbols_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_startup_phase_to_string);
	UT_ASSERT_NOT_NULL((void *)cluster_current_phase);
	UT_ASSERT_NOT_NULL((void *)cluster_phase_started_at);
	UT_ASSERT_NOT_NULL((void *)cluster_phase_elapsed_seconds);
	UT_ASSERT_NOT_NULL((void *)cluster_phase_history_format);
	UT_ASSERT_NOT_NULL((void *)cluster_advance_phase);
	UT_ASSERT_NOT_NULL((void *)cluster_run_startup_sequence);
	UT_ASSERT_NOT_NULL((void *)cluster_run_shutdown_sequence);
}


/* ============================================================
 * Spec-1.10.1 D1 F1 / D4 F4 anchors
 * ============================================================ */

UT_TEST(test_phase_shmem_state_size_under_4kb)
{
	/*
	 * Spec-1.10.1 D1 F1: ClusterPhaseSharedState lives in shmem now.
	 * The struct is small by design (LWLock + 1 enum + 8 timestamps +
	 * 8-entry ring + 2 ints).  Bound it well below 4 KiB so a future
	 * field accidentally bloating the layout is caught early; on
	 * macOS arm64 the current size is ~256 bytes.
	 */
	UT_ASSERT(sizeof(ClusterPhaseSharedState) < 4096);
}


UT_TEST(test_phase_shmem_register_init_linkable)
{
	/*
	 * Spec-1.10.1 D1 F1: cluster_phase_shmem_register +
	 * cluster_phase_shmem_init are part of the public surface and must
	 * resolve at link time.  cluster_finalize_startup_running (D4 F4)
	 * is the new public entry that PostmasterMain calls before
	 * ServerLoop.  All three are address-only here -- the runtime
	 * paths require real shmem and PG init that the unit harness lacks.
	 */
	UT_ASSERT_NOT_NULL((void *)cluster_phase_shmem_register);
	UT_ASSERT_NOT_NULL((void *)cluster_phase_shmem_init);
	UT_ASSERT_NOT_NULL((void *)cluster_phase_shmem_size);
	UT_ASSERT_NOT_NULL((void *)cluster_finalize_startup_running);
}


UT_TEST(test_rf_a1_no_pgproc_phase_reads_never_block)
{
	PGPROC fake_proc;

	cluster_phase_shmem_init();
	MyProc = NULL;
	phase_lwlock_conditional_result = false;
	phase_lwlock_blocking_calls = 0;
	phase_lwlock_conditional_calls = 0;
	UT_ASSERT_EQ((int)cluster_current_phase(), (int)CLUSTER_PHASE_PRE_INIT);
	UT_ASSERT_EQ((int)cluster_authority_readiness_get(), (int)CLUSTER_AUTHORITY_OFF);
	UT_ASSERT(!cluster_authority_readiness_managed());
	/*
	 * AD-023 A1 (lock-free branch): the three hot phase-state words are
	 * read through pg_atomic and never take the LWLock at all, so a
	 * no-PGPROC caller cannot block and cannot be starved by the
	 * per-grant serving gates; the conditional-acquire discipline only
	 * remains for the large binding copy.
	 */
	UT_ASSERT_EQ(phase_lwlock_blocking_calls, 0);
	UT_ASSERT_EQ(phase_lwlock_conditional_calls, 0);

	phase_lwlock_conditional_result = true;
	phase_lwlock_blocking_calls = 0;
	phase_lwlock_conditional_calls = 0;
	UT_ASSERT_EQ((int)cluster_current_phase(), (int)CLUSTER_PHASE_PRE_INIT);
	UT_ASSERT_EQ(phase_lwlock_blocking_calls, 0);
	UT_ASSERT_EQ(phase_lwlock_conditional_calls, 0);

	memset(&fake_proc, 0, sizeof(fake_proc));
	MyProc = &fake_proc;
	phase_lwlock_blocking_calls = 0;
	phase_lwlock_conditional_calls = 0;
	UT_ASSERT_EQ((int)cluster_current_phase(), (int)CLUSTER_PHASE_PRE_INIT);
	UT_ASSERT_EQ(phase_lwlock_blocking_calls, 0);
	UT_ASSERT_EQ(phase_lwlock_conditional_calls, 0);
	MyProc = NULL;
}


UT_TEST(test_rf_a1_phase3_establishes_formation_and_phase4_does_not_respawn)
{
	reset_phase_service_fixture(true);
	cluster_allow_single_node = false;
	cluster_voting_disks = "disk1,disk2,disk3";

	cluster_run_startup_sequence();
	UT_ASSERT_EQ((int)cluster_current_phase(), (int)CLUSTER_PHASE_3_RECOVERY);
	UT_ASSERT_STR_EQ(phase4_events, "CcQqVFXLrG");
	UT_ASSERT_EQ((int)cluster_authority_readiness_get(), (int)CLUSTER_AUTHORITY_RECOVERY_READY);
	UT_ASSERT(cluster_recovery_authority_is_current());
	UT_ASSERT(!cluster_serving_ready_is_current());
	UT_ASSERT_EQ(phase_test_lms_start_calls, 1);
	UT_ASSERT_EQ(phase_test_recovery_control_formation_calls, 1);
	UT_ASSERT_EQ(phase_test_live_formation_calls, 0);

	cluster_run_phase4_sequence();

	UT_ASSERT_STR_EQ(phase4_events, "CcQqVFXLrGDdFXWlEESs");
	UT_ASSERT_EQ(phase_test_lms_start_calls, 1);
	UT_ASSERT_EQ(phase_test_live_formation_calls, 1);
	UT_ASSERT_EQ((int)cluster_authority_readiness_get(), (int)CLUSTER_AUTHORITY_SERVING_READY);
	UT_ASSERT(cluster_serving_ready_is_current());
}

UT_TEST(test_phase3_join_readonly_defers_authority_until_post_startup_admission)
{
	bool caught_fatal = false;

	reset_phase_service_fixture(true);
	cluster_allow_single_node = false;
	cluster_voting_disks = "disk1,disk2,disk3";
	phase_test_cf_peer_verified = true;
	phase_test_membership_member = false;
	phase_test_self_join_admitted = false;
	phase_test_epoch0_late_founder_evidence = true;

	/* A peer-attaching node may finish StartupXLOG under the existing
	 * JOIN_READONLY contract, but it must not create recovery or serving
	 * authority before its exact post-recovery admission. */
	cluster_run_startup_sequence();
	UT_ASSERT_STR_EQ(phase4_events, "CcQqV");
	UT_ASSERT_EQ(phase_test_recovery_control_formation_calls, 0);
	UT_ASSERT_EQ(phase_test_lms_start_calls, 0);
	UT_ASSERT_EQ((int)cluster_authority_readiness_get(), (int)CLUSTER_AUTHORITY_OFF);

	/* StartupXLOG has returned.  The unchanged stripe/JOIN-WAL/JCMK path now
	 * admits this boot; only then may the same phase-3 recovery state machine
	 * complete before phase 4 publishes ordinary serving. */
	phase4_test_now = (TimestampTz)(cluster_phase3_timeout + 60) * INT64CONST(1000000);
	phase_test_membership_member = true;
	phase_test_self_join_admitted = true;
	phase4_capture_fatal = true;
	if (setjmp(phase4_fatal_jump) == 0)
		cluster_run_phase4_sequence();
	else
		caught_fatal = true;
	phase4_capture_fatal = false;
	UT_ASSERT(!caught_fatal);
	UT_ASSERT_STR_EQ(phase4_events, "CcQqVFXLrGDdFXWlEESs");
	UT_ASSERT_EQ(phase_test_recovery_control_formation_calls, 1);
	UT_ASSERT_EQ(phase_test_lms_start_calls, 1);
	UT_ASSERT_EQ((int)cluster_authority_readiness_get(), (int)CLUSTER_AUTHORITY_SERVING_READY);
}

/* A transient bootstrap HOLD may expose JOINING before any peer from the
 * current boot has published WAL ACTIVE.  JOINING is not late-founder
 * evidence: without the exact same-boot peer proof phase 3 must retain the
 * recovery-control owner and establish GRD authority normally. */
UT_TEST(test_phase3_joining_without_exact_peer_active_stays_recovery_control)
{
	reset_phase_service_fixture(true);
	cluster_allow_single_node = false;
	cluster_voting_disks = "disk1,disk2,disk3";
	phase_test_cf_peer_verified = true;
	phase_test_membership_member = false;
	phase_test_self_join_admitted = false;
	phase_test_epoch0_late_founder_evidence = false;

	cluster_run_startup_sequence();
	UT_ASSERT_STR_EQ(phase4_events, "CcQqVFXLrG");
	UT_ASSERT_EQ(phase_test_recovery_control_formation_calls, 1);
	UT_ASSERT_EQ(phase_test_grd_barrier_calls, 1);
	UT_ASSERT_EQ((int)cluster_authority_readiness_get(), (int)CLUSTER_AUTHORITY_RECOVERY_READY);
}

UT_TEST(test_phase3_epoch0_late_founder_defers_only_with_exact_current_evidence)
{
	reset_phase_service_fixture(true);
	cluster_allow_single_node = false;
	cluster_voting_disks = "disk1,disk2,disk3";
	phase_test_cf_peer_verified = true;
	/* AD-023 control membership is not service admission.  The approved
	 * epoch-0 late-founder edge may use it only together with the new exact
	 * current peer evidence, and only to finish StartupXLOG read-only. */
	phase_test_membership_member = true;
	phase_test_self_join_admitted = false;
	phase_test_epoch0_late_founder_evidence = true;

	cluster_run_startup_sequence();
	UT_ASSERT_STR_EQ(phase4_events, "CcQqV");
	UT_ASSERT_EQ(phase_test_recovery_control_formation_calls, 0);
	UT_ASSERT_EQ(phase_test_lms_start_calls, 0);
	UT_ASSERT_EQ((int)cluster_authority_readiness_get(), (int)CLUSTER_AUTHORITY_OFF);

	/* Missing exact epoch-0 peer evidence preserves the existing recovery-
	 * control path; CFP2 alone is never admission or a stripe owner. */
	reset_phase_service_fixture(true);
	cluster_allow_single_node = false;
	cluster_voting_disks = "disk1,disk2,disk3";
	phase_test_cf_peer_verified = true;
	phase_test_membership_member = true;
	phase_test_self_join_admitted = false;
	phase_test_epoch0_late_founder_evidence = false;
	cluster_run_startup_sequence();
	UT_ASSERT_STR_EQ(phase4_events, "CcQqVFXLrG");
}

/* A founder can select recovery-control before the first same-boot peer has
 * published WAL ACTIVE.  If its GRD barrier is still pre-publish and the exact
 * late-founder evidence arrives after that failed attempt, the approved pivot
 * clears the matching STARTING binding once and finishes StartupXLOG as
 * JOIN_READONLY.  The already-running, authority-free LMS is reused only after
 * ordinary stripe/JCMK admission; neither deadline nor worker is duplicated. */
UT_TEST(test_phase3_pre_published_recovery_pivots_once_to_join_readonly)
{
	bool caught_fatal = false;
	int saved_phase3_timeout = cluster_phase3_timeout;

	reset_phase_service_fixture(true);
	cluster_allow_single_node = false;
	cluster_voting_disks = "disk1,disk2,disk3";
	phase_test_cf_peer_verified = true;
	phase_test_membership_member = true;
	phase_test_self_join_admitted = false;
	phase_test_grd_authority_ok = false;
	phase_test_late_founder_after_grd_barrier = true;
	phase_test_last_admitted_incarnation = 10;
	cluster_phase3_timeout = 1;

	phase4_capture_fatal = true;
	if (setjmp(phase4_fatal_jump) == 0)
		cluster_run_startup_sequence();
	else
		caught_fatal = true;
	phase4_capture_fatal = false;
	cluster_phase3_timeout = saved_phase3_timeout;

	UT_ASSERT(!caught_fatal);
	UT_ASSERT_STR_EQ(phase4_events, "CcQqVFXLrG");
	UT_ASSERT_EQ(phase_test_grd_barrier_calls, 1);
	UT_ASSERT_EQ(phase_test_join_readonly_handoff_calls, 1);
	UT_ASSERT_EQ(phase_test_join_readonly_handoff_incarnation, 11);
	UT_ASSERT_EQ(phase_test_join_readonly_handoff_predecessor_floor, 10);
	UT_ASSERT(cluster_authority_handoff_identity_current(11, 10));
	UT_ASSERT_EQ(phase_test_recovery_control_formation_calls, 1);
	UT_ASSERT_EQ(phase_test_lms_start_calls, 1);
	UT_ASSERT_EQ((int)cluster_authority_readiness_get(), (int)CLUSTER_AUTHORITY_OFF);

	/* The pivot owns no admission.  Only the existing terminal ordinary join
	 * permits Phase 4 to rebuild authority, reusing the same idle LMS. */
	phase_test_membership_member = true;
	phase_test_self_join_admitted = true;
	phase_test_last_admitted_incarnation = 11;
	phase_test_grd_authority_ok = true;
	phase4_capture_fatal = true;
	if (setjmp(phase4_fatal_jump) == 0)
		cluster_run_phase4_sequence();
	else
		caught_fatal = true;
	phase4_capture_fatal = false;
	UT_ASSERT(!caught_fatal);
	UT_ASSERT_EQ(phase_test_lms_start_calls, 1);
	UT_ASSERT_EQ((int)cluster_authority_readiness_get(), (int)CLUSTER_AUTHORITY_SERVING_READY);
}

/* The handoff owns only the exact reversible STARTING generation captured by
 * phase 3.  A concurrently published RECOVERY_READY binding or a changed LMS
 * generation is not this caller's transition and must never reach JOINING. */
UT_TEST(test_phase3_pivot_rejects_published_or_generation_drift)
{
	bool caught_fatal = false;
	int saved_phase3_timeout = cluster_phase3_timeout;

	reset_phase_service_fixture(true);
	cluster_allow_single_node = false;
	cluster_voting_disks = "disk1,disk2,disk3";
	phase_test_cf_peer_verified = true;
	phase_test_membership_member = true;
	phase_test_self_join_admitted = false;
	phase_test_grd_authority_ok = false;
	phase_test_late_founder_after_grd_barrier = true;
	phase_test_publish_recovery_after_grd_barrier = true;
	/* A genuinely published recovery binding already has current admission
	 * lineage; it is not the predecessor-floor handoff case. */
	phase_test_last_admitted_incarnation = 11;
	cluster_phase3_timeout = 1;
	phase4_capture_fatal = true;
	if (setjmp(phase4_fatal_jump) == 0)
		cluster_run_startup_sequence();
	else
		caught_fatal = true;
	phase4_capture_fatal = false;
	UT_ASSERT(caught_fatal);
	UT_ASSERT_EQ(phase_test_join_readonly_handoff_calls, 0);

	caught_fatal = false;
	reset_phase_service_fixture(true);
	cluster_allow_single_node = false;
	cluster_voting_disks = "disk1,disk2,disk3";
	phase_test_cf_peer_verified = true;
	phase_test_membership_member = true;
	phase_test_self_join_admitted = false;
	phase_test_grd_authority_ok = false;
	phase_test_late_founder_after_grd_barrier = true;
	phase_test_drift_lms_generation_after_grd_barrier = true;
	phase_test_last_admitted_incarnation = 10;
	phase4_capture_fatal = true;
	if (setjmp(phase4_fatal_jump) == 0)
		cluster_run_startup_sequence();
	else
		caught_fatal = true;
	phase4_capture_fatal = false;
	cluster_phase3_timeout = saved_phase3_timeout;
	UT_ASSERT(caught_fatal);
	UT_ASSERT_EQ(phase_test_join_readonly_handoff_calls, 0);
}

UT_TEST(test_phase3_join_readonly_obeys_existing_join_failure_terminal)
{
	bool caught_fatal = false;

	reset_phase_service_fixture(true);
	cluster_allow_single_node = false;
	cluster_voting_disks = "disk1,disk2,disk3";
	phase_test_cf_peer_verified = true;
	phase_test_membership_member = false;
	phase_test_self_join_admitted = false;
	phase_test_epoch0_late_founder_evidence = true;
	cluster_run_startup_sequence();
	UT_ASSERT_STR_EQ(phase4_events, "CcQqV");

	/* LMON owns admission failure through its frozen stripe/JCMK deadline.
	 * The postmaster observes that terminal and must not create another timer
	 * or any recovery/serving authority. */
	phase_test_nonmember_state = CLUSTER_MEMBER_REJECTED;
	phase4_capture_fatal = true;
	if (setjmp(phase4_fatal_jump) == 0)
		cluster_run_phase4_sequence();
	else
		caught_fatal = true;
	phase4_capture_fatal = false;
	UT_ASSERT(caught_fatal);
	UT_ASSERT_STR_EQ(phase4_events, "CcQqV");
	UT_ASSERT_EQ(phase_test_lms_start_calls, 0);
	UT_ASSERT_EQ((int)cluster_authority_readiness_get(), (int)CLUSTER_AUTHORITY_OFF);
}

UT_TEST(test_rf_a1_phase4_waits_for_stripe_admission_before_serving)
{
	bool caught_fatal = false;
	int saved_timeout = cluster_phase4_timeout;

	reset_phase_service_fixture(true);
	phase_test_self_join_admitted = false;
	cluster_phase4_timeout = 1;
	cluster_run_startup_sequence();
	UT_ASSERT_EQ((int)cluster_authority_readiness_get(), (int)CLUSTER_AUTHORITY_RECOVERY_READY);

	phase4_capture_fatal = true;
	if (setjmp(phase4_fatal_jump) == 0)
		cluster_run_phase4_sequence();
	else
		caught_fatal = true;
	phase4_capture_fatal = false;
	cluster_phase4_timeout = saved_timeout;

	UT_ASSERT(caught_fatal);
	UT_ASSERT(strchr(phase4_events, 'W') == NULL);
	UT_ASSERT_EQ(phase_test_live_formation_calls, 0);
	UT_ASSERT_EQ((int)cluster_authority_readiness_get(), (int)CLUSTER_AUTHORITY_RECOVERY_READY);
}


UT_TEST(test_rf_a1_phase4_refreshes_expired_recovery_proof_before_serving)
{
	bool caught_fatal = false;

	reset_phase_service_fixture(true);
	cluster_run_startup_sequence();
	UT_ASSERT_EQ((int)cluster_authority_readiness_get(), (int)CLUSTER_AUTHORITY_RECOVERY_READY);

	/* The phase-3 cache can expire while StartupXLOG and the exact stripe
	 * admission complete.  A post-admission live reread must refresh the same
	 * durable proof before ASSERTING ordinary LMS service. */
	phase_test_classification_current = false;
	phase_test_live_refreshes_classification = true;
	phase4_capture_fatal = true;
	if (setjmp(phase4_fatal_jump) == 0)
		cluster_run_phase4_sequence();
	else
		caught_fatal = true;
	phase4_capture_fatal = false;

	UT_ASSERT(!caught_fatal);
	UT_ASSERT_EQ(phase_test_live_formation_calls, 1);
	UT_ASSERT(strstr(phase4_events, "DdFXW") != NULL);
	UT_ASSERT_EQ((int)cluster_authority_readiness_get(), (int)CLUSTER_AUTHORITY_SERVING_READY);
}


UT_TEST(test_rf_a2_serving_does_not_consume_recovery_duty_cache)
{
	reset_phase_service_fixture(true);
	cluster_run_startup_sequence();
	cluster_run_phase4_sequence();

	/* The finite IR-held recovery cache expires while the admitted
	 * formation and sealed serving generation remain current. */
	phase_test_classification_current = false;
	UT_ASSERT(cluster_serving_ready_is_current());
	UT_ASSERT_EQ((int)cluster_authority_readiness_get(), (int)CLUSTER_AUTHORITY_SERVING_READY);
}


UT_TEST(test_rf_a2_serving_rebinds_only_after_lmon_closes_recovery)
{
	reset_phase_service_fixture(true);
	cluster_run_startup_sequence();
	cluster_run_phase4_sequence();

	phase_test_formation_epoch++;
	UT_ASSERT(!cluster_serving_ready_is_current());
	UT_ASSERT_EQ((int)cluster_authority_readiness_get(), (int)CLUSTER_AUTHORITY_SERVING_READY);
	UT_ASSERT(cluster_authority_serving_rebind_lmon());
	UT_ASSERT(cluster_serving_ready_is_current());
}

UT_TEST(test_rf_a1_readiness_is_monotone_and_generation_bound)
{
	ClusterResId recovery_resid;

	reset_phase_service_fixture(true);
	UT_ASSERT_EQ((int)cluster_authority_readiness_get(), (int)CLUSTER_AUTHORITY_OFF);
	UT_ASSERT(!cluster_authority_readiness_publish_serving());

	cluster_run_startup_sequence();
	UT_ASSERT(cluster_recovery_authority_is_current());
	memset(&recovery_resid, 0, sizeof(recovery_resid));
	recovery_resid.type = CLUSTER_CF_RESID_TYPE;
	recovery_resid.lockmethodid = DEFAULT_LOCKMETHOD;
	UT_ASSERT(cluster_recovery_authority_request_allowed(&recovery_resid, ShareLock, true));
	recovery_resid.field1 = 1;
	UT_ASSERT(!cluster_recovery_authority_request_allowed(&recovery_resid, ShareLock, true));
	memset(&recovery_resid, 0, sizeof(recovery_resid));
	recovery_resid.field1 = 1;
	recovery_resid.type = CLUSTER_WAL_RETENTION_RESID_TYPE;
	recovery_resid.lockmethodid = DEFAULT_LOCKMETHOD;
	UT_ASSERT(cluster_recovery_authority_request_allowed(&recovery_resid, ExclusiveLock, true));
	recovery_resid.field1 = 0;
	UT_ASSERT(!cluster_recovery_authority_request_allowed(&recovery_resid, ExclusiveLock, true));
	phase_test_lms_generation++;
	UT_ASSERT(!cluster_recovery_authority_is_current());
	UT_ASSERT(!cluster_serving_ready_is_current());
	/* Once this boot enters the managed readiness lifecycle, loss of its
	 * generation must leave the boot fail-closed.  OFF is a readiness level,
	 * not permission to fall back to the legacy one-dimensional LMS gate. */
	UT_ASSERT(cluster_authority_readiness_managed());
}

UT_TEST(test_rf_a1_transport_stale_clear_skips_mid_bind_gen_zero)
{
	ClusterFenceAuthorityProof authority;
	ClusterFormationSnapshotV1 formation;

	reset_phase_service_fixture(true);
	/* The preseal gates require the aux services READY (the full startup
	 * sequence normally publishes these). */
	phase_test_cssd_status = CLUSTER_CSSD_READY;
	phase_test_qvotec_status = CLUSTER_QVOTEC_READY;
	/* Strict forward transitions only (prev + 1 == target). */
	cluster_advance_phase(CLUSTER_PHASE_0_BASE);
	cluster_advance_phase(CLUSTER_PHASE_1_CLUSTER);
	cluster_advance_phase(CLUSTER_PHASE_2_LOCK);
	cluster_advance_phase(CLUSTER_PHASE_3_RECOVERY);

	/* begin(): STARTING binding with lms_generation = 0 — the postmaster's
	 * mid-bind window (the LMS process does not exist yet, so the live
	 * generation is bound one loop iteration later). */
	memset(&authority, 0, sizeof(authority));
	memset(&formation, 0, sizeof(formation));
	formation.membership.membership_state[0] = CLUSTER_MEMBER_MEMBER;
	formation.membership.last_admitted_incarnation[0] = 11;
	formation.reserved[0] = phase_test_formation_epoch;
	UT_ASSERT(cluster_authority_readiness_begin(1, &authority, &formation));
	UT_ASSERT_EQ((int)cluster_authority_readiness_get(), (int)CLUSTER_AUTHORITY_STARTING);

	/* The transport check fails on the unbound generation, but the binding
	 * is mid-bind, NOT stale: the stale-clear is gated on
	 * lms_generation != 0 (STOP-01 contract), so a peer DONE ingress
	 * during the window cannot destroy the STARTING binding. */
	UT_ASSERT(!cluster_recovery_transport_is_current());
	UT_ASSERT_EQ((int)cluster_authority_readiness_get(), (int)CLUSTER_AUTHORITY_STARTING);

	/* Once the loop binds the generation, the same check passes... */
	UT_ASSERT(cluster_authority_readiness_bind_recovery_generation(phase_test_lms_generation));
	UT_ASSERT(cluster_recovery_transport_is_current());
	UT_ASSERT_EQ((int)cluster_authority_readiness_get(), (int)CLUSTER_AUTHORITY_STARTING);

	/* ... and a BOUND generation whose formation drifted from the live
	 * formation IS stale: the transport check clears it (fail-closed). */
	phase_test_classification_current = false;
	UT_ASSERT(!cluster_recovery_transport_is_current());
	UT_ASSERT_EQ((int)cluster_authority_readiness_get(), (int)CLUSTER_AUTHORITY_OFF);
	phase_test_classification_current = true;
}

UT_TEST(test_rf_a1_missing_authoritative_grd_stops_before_recovery)
{
	bool caught_fatal = false;
	int saved_phase3_timeout = cluster_phase3_timeout;

	reset_phase_service_fixture(true);
	phase_test_grd_authority_ok = false;
	cluster_phase3_timeout = 1; /* A2 retry loop must fail closed fast */
	phase4_capture_fatal = true;
	if (setjmp(phase4_fatal_jump) == 0)
		cluster_run_startup_sequence();
	else
		caught_fatal = true;
	phase4_capture_fatal = false;
	cluster_phase3_timeout = saved_phase3_timeout;

	UT_ASSERT(caught_fatal);
	/* The A2 barrier retry repeats the formation re-fetch until the
	 * deadline, so the event stream keeps the phase-3 prefix and adds
	 * repeated witness/classification/barrier cycles before the FATAL. */
	UT_ASSERT(strncmp(phase4_events, "CcQqVFXLrG", 10) == 0);
	UT_ASSERT_EQ((int)cluster_authority_readiness_get(), (int)CLUSTER_AUTHORITY_OFF);
}


UT_TEST(test_rf_a1_refreshes_formation_if_proof_expires_during_lms_start)
{
	bool caught_fatal = false;

	reset_phase_service_fixture(true);
	phase_test_expire_formation_during_lms = true;
	phase4_capture_fatal = true;
	if (setjmp(phase4_fatal_jump) == 0)
		cluster_run_startup_sequence();
	else
		caught_fatal = true;
	phase4_capture_fatal = false;

	UT_ASSERT(!caught_fatal);
	UT_ASSERT_STR_EQ(phase4_events, "CcQqVFXLrFXG");
	UT_ASSERT_EQ((int)cluster_authority_readiness_get(), (int)CLUSTER_AUTHORITY_RECOVERY_READY);
}


UT_TEST(test_rf_a1_phase3_quorum_cannot_be_downgraded_by_compat_guc)
{
	bool caught_fatal = false;

	reset_phase_service_fixture(true);
	phase4_test_in_quorum = false;
	cluster_allow_single_node = true;

	phase4_capture_fatal = true;
	if (setjmp(phase4_fatal_jump) == 0)
		cluster_run_startup_sequence();
	else
		caught_fatal = true;
	phase4_capture_fatal = false;

	UT_ASSERT(caught_fatal);
	UT_ASSERT_STR_EQ(phase4_events, "CcQqV");
	UT_ASSERT(phase4_quorum_check_calls > 0);
	UT_ASSERT_EQ((int)cluster_current_phase(), (int)CLUSTER_PHASE_3_RECOVERY);
	phase4_test_in_quorum = true;
}


UT_TEST(test_rf_a1_phase3_service_failures_stop_before_recovery)
{
	bool caught_fatal;

	reset_phase_service_fixture(true);
	phase_test_cssd_spawn_ok = false;
	caught_fatal = false;
	phase4_capture_fatal = true;
	if (setjmp(phase4_fatal_jump) == 0)
		cluster_run_startup_sequence();
	else
		caught_fatal = true;
	phase4_capture_fatal = false;
	UT_ASSERT(caught_fatal);
	UT_ASSERT_STR_EQ(phase4_events, "C");
	UT_ASSERT_EQ((int)cluster_current_phase(), (int)CLUSTER_PHASE_3_RECOVERY);

	reset_phase_service_fixture(true);
	phase_test_cssd_ready_ok = false;
	caught_fatal = false;
	phase4_capture_fatal = true;
	if (setjmp(phase4_fatal_jump) == 0)
		cluster_run_startup_sequence();
	else
		caught_fatal = true;
	phase4_capture_fatal = false;
	UT_ASSERT(caught_fatal);
	UT_ASSERT_STR_EQ(phase4_events, "Cc");
	UT_ASSERT_EQ((int)cluster_current_phase(), (int)CLUSTER_PHASE_3_RECOVERY);

	reset_phase_service_fixture(true);
	phase_test_qvotec_spawn_ok = false;
	caught_fatal = false;
	phase4_capture_fatal = true;
	if (setjmp(phase4_fatal_jump) == 0)
		cluster_run_startup_sequence();
	else
		caught_fatal = true;
	phase4_capture_fatal = false;
	UT_ASSERT(caught_fatal);
	UT_ASSERT_STR_EQ(phase4_events, "CcQ");
	UT_ASSERT_EQ((int)cluster_current_phase(), (int)CLUSTER_PHASE_3_RECOVERY);

	reset_phase_service_fixture(true);
	phase_test_qvotec_ready_ok = false;
	caught_fatal = false;
	phase4_capture_fatal = true;
	if (setjmp(phase4_fatal_jump) == 0)
		cluster_run_startup_sequence();
	else
		caught_fatal = true;
	phase4_capture_fatal = false;
	UT_ASSERT(caught_fatal);
	UT_ASSERT_STR_EQ(phase4_events, "CcQq");
	UT_ASSERT_EQ((int)cluster_current_phase(), (int)CLUSTER_PHASE_3_RECOVERY);
}


UT_TEST(test_rf_a1_phase3_requires_live_formation_before_recovery)
{
	bool caught_fatal = false;

	reset_phase_service_fixture(true);
	phase_test_formation_result = CLUSTER_FORMATION_WITNESS_CORRUPT;
	phase4_capture_fatal = true;
	if (setjmp(phase4_fatal_jump) == 0)
		cluster_run_startup_sequence();
	else
		caught_fatal = true;
	phase4_capture_fatal = false;

	UT_ASSERT(caught_fatal);
	UT_ASSERT_STR_EQ(phase4_events, "CcQqVF");
	UT_ASSERT_EQ((int)cluster_current_phase(), (int)CLUSTER_PHASE_3_RECOVERY);
}


UT_TEST(test_rf_a1_phase3_retries_no_pgproc_snapshot_contention)
{
	bool caught_fatal = false;

	reset_phase_service_fixture(true);
	phase_test_formation_unavailable_attempts = 1;
	phase4_capture_fatal = true;
	if (setjmp(phase4_fatal_jump) == 0)
		cluster_run_startup_sequence();
	else
		caught_fatal = true;
	phase4_capture_fatal = false;

	UT_ASSERT(!caught_fatal);
	UT_ASSERT_STR_EQ(phase4_events, "CcQqVFFXLrG");
	UT_ASSERT_EQ((int)cluster_authority_readiness_get(), (int)CLUSTER_AUTHORITY_RECOVERY_READY);
}


UT_TEST(test_rf_a1_phase3_snapshot_contention_expires_fail_closed)
{
	bool caught_fatal = false;
	int saved_timeout = cluster_phase3_timeout;

	reset_phase_service_fixture(true);
	cluster_phase3_timeout = 1;
	phase_test_formation_unavailable_attempts = 1;
	phase_test_formation_unavailable_advance_us = INT64CONST(2000000);
	phase4_capture_fatal = true;
	if (setjmp(phase4_fatal_jump) == 0)
		cluster_run_startup_sequence();
	else
		caught_fatal = true;
	phase4_capture_fatal = false;
	cluster_phase3_timeout = saved_timeout;

	UT_ASSERT(caught_fatal);
	UT_ASSERT_STR_EQ(phase4_events, "CcQqVF");
	UT_ASSERT_EQ((int)cluster_authority_readiness_get(), (int)CLUSTER_AUTHORITY_OFF);
}


UT_TEST(test_rf_a1_cluster_disabled_keeps_phase3_and_phase4_empty)
{
	reset_phase_service_fixture(true);
	cluster_enabled = false;

	cluster_run_startup_sequence();
	cluster_run_phase4_sequence();

	UT_ASSERT_STR_EQ(phase4_events, "");
	UT_ASSERT_EQ((int)cluster_current_phase(), (int)CLUSTER_PHASE_4_NORMAL);
	cluster_enabled = true;
}


UT_TEST(test_rf_a1_unconfigured_registry_keeps_legacy_phase4_order)
{
	reset_phase_service_fixture(false);
	cluster_run_startup_sequence();

	cluster_run_phase4_sequence();

	UT_ASSERT_STR_EQ(phase4_events, "CcQqDdSs");
	startup_self_check_calls = 0;
	postmaster_publish_active_calls = 0;
	cluster_finalize_startup_running();
	UT_ASSERT_EQ(startup_self_check_calls, 1);
	UT_ASSERT_EQ(postmaster_publish_active_calls, 0);
	cluster_wal_threads_dir = "/rf-a1/formed";
}


UT_TEST(test_rf_a1_phase4_refuses_lost_prestartup_service_without_respawn)
{
	bool caught_fatal = false;

	reset_phase_service_fixture(true);
	cluster_allow_single_node = false;
	cluster_voting_disks = "disk1,disk2,disk3";
	cluster_run_startup_sequence();
	UT_ASSERT_STR_EQ(phase4_events, "CcQqVFXLrG");
	phase_test_cssd_status = CLUSTER_CSSD_DOWN;

	phase4_capture_fatal = true;
	if (setjmp(phase4_fatal_jump) == 0)
		cluster_run_phase4_sequence();
	else
		caught_fatal = true;
	phase4_capture_fatal = false;

	UT_ASSERT(caught_fatal);
	UT_ASSERT_STR_EQ(phase4_events, "CcQqVFXLrG");
}


UT_TEST(test_rf_a1_finalize_never_runs_self_fence_or_active_from_postmaster)
{
	postmaster_publish_active_calls = 0;
	startup_self_check_calls = 0;
	cluster_finalize_startup_running();

	UT_ASSERT_EQ(postmaster_publish_active_calls, 0);
	UT_ASSERT_EQ(startup_self_check_calls, 0);
}


/* The post-admission JOIN_READONLY rebuild owns one exact LMS generation
 * bind per loop iteration.  A duplicate conditional lock transaction can
 * observe an unrelated contender between identical calls and spuriously
 * discard an already-bound STARTING tuple. */
UT_TEST(test_join_readonly_rebuild_binds_generation_once_per_iteration)
{
	const char *function_end;
	const char *function_start;
	const char *match;
	const char *needle = "cluster_authority_readiness_bind_recovery_generation(";
	char *source = read_startup_phase_source();
	int count = 0;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	function_start = strstr(source, "\ncluster_phase4_establish_join_readonly_authority(");
	function_end = function_start == NULL
					   ? NULL
					   : strstr(function_start, "\n\n/*\n * cluster_phase_remaining_budget_ms");
	UT_ASSERT_NOT_NULL(function_start);
	UT_ASSERT_NOT_NULL(function_end);
	if (function_start != NULL && function_end != NULL) {
		match = function_start;
		while ((match = strstr(match, needle)) != NULL && match < function_end) {
			count++;
			match += strlen(needle);
		}
	}
	UT_ASSERT_EQ(count, 1);
	free(source);
}


/* ============================================================
 * Test runner
 * ============================================================ */

int
main(void)
{
	UT_PLAN(34);
	UT_RUN(test_phase_enum_values_frozen);
	UT_RUN(test_phase_last_is_shutdown);
	UT_RUN(test_phase_history_ring_size_is_eight);
	UT_RUN(test_phase_string_lookup_returns_non_null_for_each_value);
	UT_RUN(test_phase_string_lookup_invalid_returns_unknown);
	UT_RUN(test_public_symbols_linkable);
	UT_RUN(test_phase_shmem_state_size_under_4kb);
	UT_RUN(test_phase_shmem_register_init_linkable);
	UT_RUN(test_rf_a1_no_pgproc_phase_reads_never_block);
	UT_RUN(test_rf_a1_phase3_establishes_formation_and_phase4_does_not_respawn);
	UT_RUN(test_phase3_join_readonly_defers_authority_until_post_startup_admission);
	UT_RUN(test_phase3_joining_without_exact_peer_active_stays_recovery_control);
	UT_RUN(test_phase3_epoch0_late_founder_defers_only_with_exact_current_evidence);
	UT_RUN(test_phase3_pre_published_recovery_pivots_once_to_join_readonly);
	UT_RUN(test_phase3_pivot_rejects_published_or_generation_drift);
	UT_RUN(test_phase3_join_readonly_obeys_existing_join_failure_terminal);
	UT_RUN(test_rf_a1_phase4_waits_for_stripe_admission_before_serving);
	UT_RUN(test_rf_a1_phase4_refreshes_expired_recovery_proof_before_serving);
	UT_RUN(test_rf_a2_serving_does_not_consume_recovery_duty_cache);
	UT_RUN(test_rf_a2_serving_rebinds_only_after_lmon_closes_recovery);
	UT_RUN(test_rf_a1_finalize_never_runs_self_fence_or_active_from_postmaster);
	UT_RUN(test_rf_a1_readiness_is_monotone_and_generation_bound);
	UT_RUN(test_rf_a1_transport_stale_clear_skips_mid_bind_gen_zero);
	UT_RUN(test_rf_a1_missing_authoritative_grd_stops_before_recovery);
	UT_RUN(test_rf_a1_refreshes_formation_if_proof_expires_during_lms_start);
	UT_RUN(test_rf_a1_unconfigured_registry_keeps_legacy_phase4_order);
	UT_RUN(test_rf_a1_phase3_quorum_cannot_be_downgraded_by_compat_guc);
	UT_RUN(test_rf_a1_phase3_service_failures_stop_before_recovery);
	UT_RUN(test_rf_a1_phase3_requires_live_formation_before_recovery);
	UT_RUN(test_rf_a1_phase3_retries_no_pgproc_snapshot_contention);
	UT_RUN(test_rf_a1_phase3_snapshot_contention_expires_fail_closed);
	UT_RUN(test_rf_a1_cluster_disabled_keeps_phase3_and_phase4_empty);
	UT_RUN(test_rf_a1_phase4_refuses_lost_prestartup_service_without_respawn);
	UT_RUN(test_join_readonly_rebuild_binds_generation_once_per_iteration);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
