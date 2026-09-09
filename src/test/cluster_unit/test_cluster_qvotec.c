/*-------------------------------------------------------------------------
 *
 * test_cluster_qvotec.c
 *	  Compile-time / link-level invariants for spec-2.6 D1+D2 (Sprint A
 *	  Step 1 — initial scaffolding).
 *
 *	  Step 1 scope (this file):
 *	    T-1 ClusterVotingSlot byte layout — size == 512 + per-field
 *	        offset (magic@0 / node_id@8 / incarnation@16 /
 *	        heartbeat_ts_us@24 / current_epoch@32 / flags@40 /
 *	        generation@56 / _alive_bitmap@64 / crc32c@508)
 *	    T-2 ClusterQvotecShmem byte layout — size == 448, with the exact
 *	        spec-5.15A §2.1A.4 320-byte mailbox appended at offset 128
 *	    T-3 lifecycle accessor surface — all 7 dump-key accessors
 *	        symbol-resolve at link time;NULL-safe (return defaults
 *	        before shmem_init)
 *	    T-4 cluster_qvotec_in_quorum lease-aware semantics — pre-init
 *	        returns false;cluster_writes_frozen=1 returns false
 *	        (regardless of state)
 *	    T-5 cluster_freeze_writes_set / _thaw_writes_set / _currently
 *	        _frozen round-trip
 *	    T-6 ClusterQvotecMain symbol resolves at link time (postmaster
 *	        reaper wiring lands Step 3 D7;test just verifies linker)
 *	    T-7 4 enum (QvotecStatus / QuorumState / VotingDiskIoState /
 *	        CollisionDetectionState) numeric values frozen + name
 *	        round-trip
 *
 *	  Step 1 explicitly DEFERS:
 *	    - Real poll cycle (Step 2 D3+D4)
 *	    - Boot-time epoch recovery body (Step 2 — needs disk I/O)
 *	    - 4 GUC default+range (Step 4 D12)
 *	    - PROCSIG flag set/clear via real ProcSignal (Step 3 D5)
 *	    - Disk I/O failure path / fanout LMON-only Assert (Step 2 D3)
 *	    - quorum_view atomic update under transitions (Step 2 D4)
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_qvotec.c
 *
 * NOTES
 *	  pgrac-original file.  Spec: spec-2.6-voting-disk-quorum-lite.md
 *	  (frozen v0.2 2026-05-09 Q1-Q10 user approve).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <signal.h>
#include <stddef.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_gcs_block_dedup.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_ic_tier1.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_reconfig.h" /* ReconfigEvent for spec-4.12b D2 stub */
#include "cluster/cluster_control_root.h" /* B′ bit22 cutover references */
#include "cluster/cluster_replacement_request.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/cluster_undo_root_descriptor.h"
#include "cluster/cluster_write_fence.h" /* ClusterFenceMarker for D2/D4 stubs */
#include "cluster/storage/cluster_undo_block0_current.h"

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

#include "unit_test.h"

#ifndef QVOTEC_SOURCE_PATH
#error "QVOTEC_SOURCE_PATH must identify cluster_qvotec.c"
#endif
#ifndef LMON_SOURCE_PATH
#error "LMON_SOURCE_PATH must identify cluster_lmon.c"
#endif
#ifndef SHMEM_SOURCE_PATH
#error "SHMEM_SOURCE_PATH must identify cluster_shmem.c"
#endif
#ifndef SEMANTIC_SOURCE_PATH
#error "SEMANTIC_SOURCE_PATH must identify cluster_semantic_activation.c"
#endif
#ifndef SEMANTIC_HEADER_PATH
#error "SEMANTIC_HEADER_PATH must identify cluster_semantic_activation.h"
#endif
#ifndef CLUSTER_MAKEFILE_PATH
#error "CLUSTER_MAKEFILE_PATH must identify the backend cluster Makefile"
#endif

/* Test-only linkage; deliberately absent from every product header/ABI. */
extern ClusterSemanticActivationResult cluster_qvotec_test_semantic_activation_record_cas_write(
	const int *fds, int n_disks, uint64 expected_generation,
	uint64 expected_source_feature_bitmap,
	const uint8 desired_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES]);
extern ClusterSemanticActivationResult cluster_qvotec_test_semantic_activation_record_read(
	const int *fds, int n_disks,
	uint8 selected_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES],
	bool *implicit_open);
extern bool cluster_qvotec_test_join_marker_ack_proven(
	const int *fds, int n_disks, int32 target_node,
	const uint8 *staged_slot, uint32 writes_ok);
extern bool cluster_qvotec_test_join_marker_verify_committed_closed(
	const int *fds, int n_disks, int32 target_node,
	uint8 verified_image96[CLUSTER_JCMK_REPLACEMENT_BYTES]);
extern ClusterQvotecMailboxResult cluster_qvotec_test_epoch_ballot_recover_head(
	const int *fds, int n_disks, uint64 system_identifier,
	const uint64 admitted_incarnations[CLUSTER_MAX_NODES],
	uint8 out_value[CLUSTER_QVOTEC_AUTHORITY_VALUE_BYTES],
	uint8 out_ballot[CLUSTER_QVOTEC_BALLOT_BYTES],
	uint8 *out_observed_disk_bitmap);
extern bool cluster_qvotec_test_epoch_ballot_phase1_promise(
	const int *fds, int n_disks, uint64 system_identifier,
	const uint64 admitted_incarnations[CLUSTER_MAX_NODES],
	int32 proposer_node_id, const ClusterEpochBallotId *ballot,
	uint8 *out_completed_disk_bitmap);
extern bool cluster_qvotec_test_epoch_ballot_formation_attested(
	const int *fds, int n_disks);
extern bool cluster_qvotec_test_undo_root_descriptor_formation_attested(
	const int *fds, int n_disks);
extern bool cluster_qvotec_test_undo_root_descriptor_provision(
	const int *fds, int n_disks, uint64 system_identifier,
	const uint8 desired[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES],
	uint8 *out_completed_disk_bitmap);
extern ClusterUndoRootDescriptorState
cluster_qvotec_test_undo_root_descriptor_read(
	const int *fds, int n_disks, uint64 system_identifier,
	uint8 root_kind, int32 owner_node, ClusterUndoRootDescriptorV1 *out,
	uint8 *out_observed_disk_bitmap);
extern ClusterReplacementRequestSlotState
cluster_qvotec_test_replacement_request_preserve(
	ClusterVotingSlot *next, const ClusterVotingSlot *prior);
extern long cluster_qvotec_test_poll_wait_timeout_ms(
	uint64 elapsed_us, int poll_interval_ms);
#ifdef __APPLE__
extern void cluster_qvotec_test_poll_pre_injection(void) __attribute__((weak_import));
extern void cluster_qvotec_test_publish_poll_lease(uint64 now_us) __attribute__((weak_import));
#else
extern void cluster_qvotec_test_poll_pre_injection(void) __attribute__((weak));
extern void cluster_qvotec_test_publish_poll_lease(uint64 now_us) __attribute__((weak));
#endif


/* ============================================================
 * Stubs — link cluster_qvotec.o standalone.
 * ============================================================ */

bool IsUnderPostmaster = false;
volatile sig_atomic_t ConfigReloadPending = false;
volatile sig_atomic_t ShutdownRequestPending = false;
volatile uint32 InterruptHoldoffCount = 0;
int MyProcPid = 0;
int cluster_node_id = 0;
char *cluster_shared_data_dir = NULL;

/* implementation (contract §C): cluster_semantic_activation.o consults the runtime
 * census at the latch apply; this binary does not link cluster_wal_state.o.
 * GREEN stub — the RED refusal path is covered in
 * test_cluster_r4_activation_fsm test_130. */
bool
cluster_wal_state_correctness_census_ok(void)
{
	return true;
}

bool
cluster_ctrc_shmem_ready(void)
{
	return true;
}

uint32
cluster_grd_recovery_state_value(void)
{
	return 0;
}

bool
cluster_reconfig_snapshot_initial_clean_formation(
	ClusterInitialCleanFormationSnapshot *out)
{
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	return false;
}

const struct ClusterSemanticActivationCallbackBundle *
cluster_pcm_x_resource_x_activation_callbacks(void)
{
	return NULL;
}

bool
cluster_pcm_lock_resource_x_gate_snapshot(ResourceXGateSnapshot *out)
{
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	return false;
}

bool
cluster_pcm_lock_resource_x_cutover_gate_snapshot_exact(ResourceXGateSnapshot *out)
{
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	return false;
}

bool
cluster_pcm_lock_resource_x_cutover_current_proof_digest_exact(
	bool thawed pg_attribute_unused(), ResourceXReconfigToken *token_out,
	uint64 *digest_out)
{
	if (token_out != NULL)
		memset(token_out, 0, sizeof(*token_out));
	if (digest_out != NULL)
		*digest_out = 0;
	return false;
}

bool
cluster_reconfig_get_observed_slot(int32 node_id pg_attribute_unused(),
	uint64 *incarnation, uint64 *generation)
{
	if (incarnation != NULL)
		*incarnation = 0;
	if (generation != NULL)
		*generation = 0;
	return false;
}

uint64
cluster_reconfig_get_observed_epoch(int32 node_id pg_attribute_unused())
{
	return 0;
}

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

static char quorum_admission_log[512];
static unsigned int quorum_admission_log_count;

bool
errstart(int e, const char *d pg_attribute_unused())
{
	return e == LOG;
}
bool
errstart_cold(int e pg_attribute_unused(), const char *d pg_attribute_unused())
{
	return false;
}
void
errfinish(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		  const char *fn pg_attribute_unused())
{}
int
errcode(int s pg_attribute_unused())
{
	return 0;
}
int
errcode_for_file_access(void)
{
	return 0;
}
int
errmsg(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
int
errmsg_internal(const char *f, ...)
{
	va_list args;

	va_start(args, f);
	vsnprintf(quorum_admission_log, sizeof(quorum_admission_log), f, args);
	va_end(args);
	quorum_admission_log_count++;
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

#include "storage/shmem.h"
/* ShmemInitStruct stub: hand back a writable buffer for shmem_init(). */
static char shmem_storage[512] __attribute__((aligned(64)));
static bool shmem_init_done = false;
void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *foundPtr)
{
	if (foundPtr != NULL)
		*foundPtr = shmem_init_done;
	if (size > sizeof(shmem_storage))
		return NULL;
	shmem_init_done = true;
	return (void *)shmem_storage;
}

#include "datatype/timestamp.h"
static TimestampTz mock_now = 1700000000000000LL;
TimestampTz
GetCurrentTimestamp(void)
{
	return mock_now;
}

void
proc_exit(int code pg_attribute_unused())
{
	abort();
}

#include "miscadmin.h"
volatile sig_atomic_t InterruptPending = false;
BackendType MyBackendType = B_INVALID;
struct Latch *MyLatch = NULL;

void
ProcessInterrupts(void)
{}
void
ResetLatch(struct Latch *latch pg_attribute_unused())
{}
int
WaitLatch(struct Latch *latch pg_attribute_unused(), int wakeEvents pg_attribute_unused(),
		  long timeout pg_attribute_unused(), uint32 wait_event_info pg_attribute_unused())
{
	return 0;
}
static int injected_sleeps;
static long injected_sleep_us;
void
pg_usleep(long microsec)
{
	injected_sleeps++;
	injected_sleep_us = microsec;
}

#include "cluster/cluster_shmem.h"
void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}

uint32
cluster_ic_local_capability_word(void)
{
	return 0;
}

ClusterICSendResult
cluster_ic_send_envelope(uint8 msg_type pg_attribute_unused(),
	int32 dest_node_id pg_attribute_unused(),
	const void *payload pg_attribute_unused(),
	uint32 payload_len pg_attribute_unused())
{
	return CLUSTER_IC_SEND_NOT_ADMITTED;
}

void
cluster_ic_tier1_close_peer(int32 peer_id pg_attribute_unused(),
	const char *reason pg_attribute_unused())
{}

bool
cluster_sf_peer_capability_word_sample(
	int32 peer_id pg_attribute_unused(),
	uint32 required_capabilities pg_attribute_unused(),
	uint32 *capability_word_out, uint32 *generation_out)
{
	if (capability_word_out != NULL)
		*capability_word_out = 0;
	if (generation_out != NULL)
		*generation_out = 0;
	return false;
}

void
cluster_write_fence_authority_cache_invalidate(void)
{}

#include "cluster/cluster_elog.h"
void
cluster_elog_init(void)
{}

#include "cluster/cluster_inject.h"
static bool poll_injection_armed;
int cluster_injection_armed_count;
static uint64 poll_injection_hits;
static uint64 poll_injection_param = 75000;

bool
cluster_injection_is_armed(const char *name)
{
	return poll_injection_armed && strcmp(name, "cluster-qvotec-poll-pre") == 0;
}

int
cluster_injection_get_count(void)
{
	return 1;
}

bool
cluster_injection_get_state_at(int idx, const char **name, ClusterInjectFaultType *type,
							   uint64 *hits)
{
	if (idx != 0)
		return false;
	*name = "cluster-qvotec-poll-pre";
	*type = poll_injection_armed ? CLUSTER_FAULT_SLEEP : CLUSTER_FAULT_NONE;
	*hits = poll_injection_hits;
	return true;
}

bool
cluster_cr_injection_armed(const char *name, uint64 *out_param)
{
	if (!cluster_injection_is_armed(name))
		return false;
	if (out_param != NULL)
		*out_param = poll_injection_param;
	poll_injection_hits++;
	return true;
}

/* Step 3 D7 stubs: signal/ps_display/procsignal symbols not linked
 * here (cluster_qvotec.c references them for ClusterQvotecMain;
 * unit test never invokes Main, just address-takes for T-6). */
sigset_t UnBlockSig;
typedef void (*pqsigfunc)(int);
pqsigfunc
pqsignal(int signum pg_attribute_unused(), pqsigfunc handler pg_attribute_unused())
{
	return handler;
}
void
SignalHandlerForConfigReload(int sig pg_attribute_unused())
{}
void
SignalHandlerForShutdownRequest(int sig pg_attribute_unused())
{}
void
init_ps_display(const char *fixed_part pg_attribute_unused())
{}
void
procsignal_sigusr1_handler(int sig pg_attribute_unused())
{}
void
ProcessConfigFile(int context pg_attribute_unused())
{}

/* P1.3 step 1-4 stubs — voting disk fd helpers + on_shmem_exit + slot
 * I/O + quorum decision + pgstat counters + memory context.  None of
 * these are exercised by the unit harness; we only need symbols to
 * resolve at link time. */
#include "storage/ipc.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "cluster/cluster_voting_disk_io.h"
#include "cluster/cluster_quorum_decision.h"
#include "cluster/cluster_pgstat.h"
#include "cluster/cluster_adg.h"
#include "cluster/cluster_mrp.h"

#ifndef CLUSTER_QVOTEC_PGSA_UNIT_TEST
int
cluster_voting_disk_open(const char *path pg_attribute_unused(),
						 bool create_if_missing pg_attribute_unused())
{
	return -1;
}
void
cluster_voting_disk_close(int fd pg_attribute_unused())
{}
ClusterVotingDiskIoState
cluster_voting_disk_read_slot(int fd pg_attribute_unused(),
							  int expected_disk_index pg_attribute_unused(),
							  uint32 node_id pg_attribute_unused(),
							  ClusterVotingSlot *out pg_attribute_unused())
{
	return CLUSTER_VOTING_DISK_IO_NOT_TRIED;
}
ClusterVotingDiskIoState
cluster_voting_disk_write_slot(int fd pg_attribute_unused(),
							   ClusterVotingSlot *slot pg_attribute_unused())
{
	return CLUSTER_VOTING_DISK_IO_NOT_TRIED;
}
ClusterVotingDiskIoState
cluster_voting_disk_write_leave_slot(int fd pg_attribute_unused(),
									 uint32 node_id pg_attribute_unused(),
									 const void *in_slot512 pg_attribute_unused())
{
	return CLUSTER_VOTING_DISK_IO_NOT_TRIED;
}
/* spec-5.15 D4: qvotec poll writes the join-commit marker to region 3. */
ClusterVotingDiskIoState cluster_voting_disk_write_join_slot(int fd, uint32 node_id,
															 const void *in_slot512);
ClusterVotingDiskIoState
cluster_voting_disk_write_join_slot(int fd pg_attribute_unused(),
									uint32 node_id pg_attribute_unused(),
									const void *in_slot512 pg_attribute_unused())
{
	return CLUSTER_VOTING_DISK_IO_NOT_TRIED;
}
ClusterVotingDiskIoState
cluster_voting_disk_read_apply_lease_global_slot(int fd pg_attribute_unused(),
												 void *out_slot512 pg_attribute_unused())
{
	return CLUSTER_VOTING_DISK_IO_NOT_TRIED;
}
ClusterVotingDiskIoState
cluster_voting_disk_write_apply_lease_global_slot(int fd pg_attribute_unused(),
												  const void *in_slot512 pg_attribute_unused())
{
	return CLUSTER_VOTING_DISK_IO_NOT_TRIED;
}
#endif
bool
cluster_adg_apply_master_lease_valid(const ClusterAdgApplyMasterLease *lease pg_attribute_unused())
{
	return false;
}
bool
cluster_adg_apply_master_lease_pack(void *slot512 pg_attribute_unused(),
									const ClusterAdgApplyMasterLease *lease pg_attribute_unused())
{
	return false;
}
bool
cluster_adg_apply_master_lease_unpack(const void *slot512 pg_attribute_unused(),
									  ClusterAdgApplyMasterLease *lease pg_attribute_unused())
{
	return false;
}
bool
cluster_adg_apply_master_lease_quorum(
	const ClusterAdgApplyMasterLease leases[] pg_attribute_unused(),
	const bool valid[] pg_attribute_unused(), int lease_count pg_attribute_unused(),
	int quorum pg_attribute_unused(), ClusterAdgApplyMasterLeaseQuorum *out)
{
	if (out != NULL) {
		memset(out, 0, sizeof(*out));
		out->owner_node_id = -1;
	}
	return true;
}
ClusterAdgApplyMasterLeaseCasVerdict
cluster_adg_apply_master_lease_cas_verdict(
	const ClusterAdgApplyMasterLeaseQuorum *current pg_attribute_unused(),
	const ClusterAdgApplyMasterLease *desired pg_attribute_unused(),
	int64 now_ms pg_attribute_unused(), int64 takeover_grace_ms pg_attribute_unused())
{
	return CLUSTER_ADG_APPLY_LEASE_CAS_STALE;
}
int32
cluster_adg_apply_master_candidate_node(const uint8 *alive_bitmap, int bitmap_bytes)
{
	int byte;

	if (alive_bitmap == NULL || bitmap_bytes <= 0)
		return -1;
	for (byte = 0; byte < bitmap_bytes; byte++) {
		int bit;

		if (alive_bitmap[byte] == 0)
			continue;
		for (bit = 0; bit < 8; bit++) {
			if ((alive_bitmap[byte] & (uint8)(1u << bit)) != 0)
				return (int32)(byte * 8 + bit);
		}
	}
	return -1;
}
bool
cluster_adg_apply_master_candidate_allows_owner(const uint8 *alive_bitmap, int bitmap_bytes,
												int32 owner_node_id)
{
	int32 candidate_node;

	candidate_node = cluster_adg_apply_master_candidate_node(alive_bitmap, bitmap_bytes);
	return candidate_node >= 0 && candidate_node == owner_node_id;
}
void
cluster_mrp_publish_qvotec_latch(struct Latch *latch pg_attribute_unused())
{}
bool
cluster_mrp_qvotec_poll_apply_lease_request(ClusterAdgApplyMasterLease *out pg_attribute_unused())
{
	return false;
}
void
cluster_mrp_qvotec_complete_apply_lease_request(
	ClusterMrpApplyLeaseSubmitResult result pg_attribute_unused(),
	const ClusterAdgApplyMasterLeaseQuorum *winner pg_attribute_unused())
{}
ClusterQvotecQuorumState
decide_quorum_view(const ClusterVotingSlot *slots pg_attribute_unused(),
				   const ClusterVotingDiskIoState *io_states pg_attribute_unused(),
				   uint32 n_disks pg_attribute_unused(), uint32 n_max_nodes pg_attribute_unused(),
				   uint32 self_node_id pg_attribute_unused(),
				   uint64 self_incarnation pg_attribute_unused(),
				   uint64 now_us pg_attribute_unused(),
				   uint64 heartbeat_timeout_us pg_attribute_unused(),
				   ClusterQuorumDecision *out pg_attribute_unused())
{
	return CLUSTER_QVOTEC_QUORUM_LOST;
}
ClusterPgstatCounter *
cluster_pgstat_lookup(const char *name pg_attribute_unused())
{
	return NULL;
}
void
cluster_pgstat_inc(ClusterPgstatCounter *c pg_attribute_unused())
{}
uint64
cluster_pgstat_read(const ClusterPgstatCounter *c pg_attribute_unused())
{
	return 0;
}
void
on_shmem_exit(pg_on_exit_callback function pg_attribute_unused(), Datum arg pg_attribute_unused())
{}
MemoryContext TopMemoryContext = NULL;
void *
MemoryContextAllocZero(MemoryContext context pg_attribute_unused(), Size size)
{
	return calloc(1, size);
}
int cluster_quorum_poll_interval_ms = 2000;
int cluster_voting_disk_io_timeout_ms = 5000;
int cluster_adg_lease_takeover_grace_ms = 5000;

/* spec-4.12 D2/D4 stubs: cluster_qvotec.o references the write-fence marker
 * scan / token refresh / submit-mailbox helpers + the lease GUC; cluster_write_
 * fence.o + cluster_guc.o are not linked here.  poll_pending returns "no pending
 * submit" so the qvotec poll path is unchanged in this unit harness. */
int cluster_write_fence_lease_ms = 6000;
void cluster_write_fence_refresh_from_marker(const ClusterFenceMarker *m, uint64 lease_expire_us);
void
cluster_write_fence_refresh_from_marker(const ClusterFenceMarker *m pg_attribute_unused(),
										uint64 lease_expire_us pg_attribute_unused())
{}
void cluster_write_fence_note_minority_marker(void);
void
cluster_write_fence_note_minority_marker(void)
{}
void cluster_write_fence_publish_qvotec_latch(struct Latch *latch);
void
cluster_write_fence_publish_qvotec_latch(struct Latch *latch pg_attribute_unused())
{}
bool cluster_write_fence_qvotec_poll_pending(ClusterFenceMarker *out);
bool
cluster_write_fence_qvotec_poll_pending(ClusterFenceMarker *out pg_attribute_unused())
{
	return false;
}
void cluster_write_fence_qvotec_complete(bool acked);
void
cluster_write_fence_qvotec_complete(bool acked pg_attribute_unused())
{}

/* spec-5.13 §2.5 stubs: cluster_qvotec.o now also references the clean-leave
 * marker submit handshake + startup rebuild; cluster_clean_leave.o is not linked
 * here.  poll_pending returns "no pending submit" so the qvotec poll path under
 * test is unchanged; rebuild is a no-op. */
void cluster_clean_leave_publish_qvotec_latch(struct Latch *latch);
void
cluster_clean_leave_publish_qvotec_latch(struct Latch *latch pg_attribute_unused())
{}
bool cluster_clean_leave_qvotec_poll_pending(void *out_slot512);
bool
cluster_clean_leave_qvotec_poll_pending(void *out_slot512 pg_attribute_unused())
{
	return false;
}
void cluster_clean_leave_qvotec_complete(bool acked);
void
cluster_clean_leave_qvotec_complete(bool acked pg_attribute_unused())
{}
void cluster_clean_leave_rebuild_from_disks(const int *fds, int n_disks);
void
cluster_clean_leave_rebuild_from_disks(const int *fds pg_attribute_unused(),
									   int n_disks pg_attribute_unused())
{}

/* spec-5.18 D8 stubs: cluster_qvotec.o now references the node-removal marker
 * mailbox + carry-forward + the removed-bitmap snapshot; cluster_node_remove.o /
 * cluster_node_remove_policy.o / cluster_reconfig.o are not linked here.  The
 * poll returns "no pending submit" so the qvotec poll path under test is unchanged;
 * pack/preserve/snapshot are inert. */
#include "cluster/cluster_node_remove.h"
bool
cluster_node_remove_qvotec_poll_pending(ClusterRemovalMarker *out pg_attribute_unused())
{
	return false;
}
void
cluster_node_remove_qvotec_complete(bool acked pg_attribute_unused())
{}
void
cluster_node_remove_publish_qvotec_latch(struct Latch *latch pg_attribute_unused())
{}
void
cluster_node_remove_rebuild_from_disks(const int *fds pg_attribute_unused(),
									   int n_disks pg_attribute_unused())
{}
void
cluster_removal_marker_pack(uint8 *reserved1 pg_attribute_unused(),
							const ClusterRemovalMarker *m pg_attribute_unused())
{}
void
cluster_removal_marker_preserve_per_disk(uint8 *new_reserved1 pg_attribute_unused(),
										 const uint8 *prior pg_attribute_unused())
{}
void
cluster_reconfig_snapshot_removed_bitmap(uint8 *out)
{
	if (out != NULL)
		memset(out, 0, 16); /* no removed nodes in the unit harness */
}
uint64
cluster_reconfig_get_removed_count(void)
{
	return 0; /* no removed nodes in the unit harness -> fence baseline path unchanged */
}

/* spec-4.12b D2/D4/D6 stubs: cluster_qvotec.o now references the enforcement GUC
 * (D2 author gate), the applied-membership snapshot (D2 baseline build), the
 * current-epoch upper-bound Assert (cassert), and the D6 baseline observability
 * note.  cluster_write_fence.o / cluster_guc.o / cluster_reconfig.o / cluster_epoch.o
 * are not linked here -- provide stubs.  enforcement OFF keeps the baseline-author
 * branch disabled, so the poll path under test is unchanged. */
int cluster_write_fence_enforcement = CLUSTER_WRITE_FENCE_ENFORCE_OFF;
void cluster_write_fence_note_baseline_published(bool is_leader, bool published);
void
cluster_write_fence_note_baseline_published(bool is_leader pg_attribute_unused(),
											bool published pg_attribute_unused())
{}
void cluster_write_fence_note_baseline_stale(void);
void
cluster_write_fence_note_baseline_stale(void)
{}
void
cluster_reconfig_get_last_event(ReconfigEvent *out)
{
	memset(out, 0, sizeof(*out)); /* pristine (event_id == 0): never applied */
}
/* RF-ROOT P6 (clean-departed epoch floor): cluster_qvotec.o references the
 * clean-departed epoch for the fence baseline floor; the unit harness has no
 * departed nodes -> floor 0 (pristine path unchanged). */
uint64
cluster_reconfig_get_clean_departed_epoch(int32 node_id pg_attribute_unused())
{
	return 0;
}
/* spec-5.15 D1/D4: qvotec poll publishes observed slots into the reconfig region
 * and mediates the join-commit marker handshake; stub all the reconfig symbols
 * qvotec.o now references (cluster_reconfig.o is not linked into this test). */
void cluster_reconfig_record_observed_slot(int32 node_id, uint64 incarnation, uint64 generation,
										   uint64 epoch);
void
cluster_reconfig_record_observed_slot(int32 node_id pg_attribute_unused(),
									  uint64 incarnation pg_attribute_unused(),
									  uint64 generation pg_attribute_unused(),
									  uint64 epoch pg_attribute_unused())
{}
/* spec-5.15 Hardening v1.3: qvotec.o now also publishes per-node fresh-alive. */
void cluster_reconfig_record_observed_fresh_alive(int32 node_id, bool fresh_alive);
void
cluster_reconfig_record_observed_fresh_alive(int32 node_id pg_attribute_unused(),
											 bool fresh_alive pg_attribute_unused())
{}
/* RF-ROOT P9 verification (cold-formation): qvotec.o now also drives the
 * cold-formation marker mailbox (region 7) and the bootstrap-observation
 * window (per-node observed incarnation/generation/epoch + fresh-alive +
 * same-round in-quorum snapshot inside one seqlock window).  cluster_
 * reconfig.o is not linked into this binary — stub the B′ surfaces like
 * the other reconfig symbols.  The formation-marker tests live in
 * test_cluster_formation_marker / test_cluster_reconfig. */
bool
cluster_reconfig_formation_qvotec_poll_pending(
	ClusterFormationMarkerSubmitRequest *out pg_attribute_unused())
{
	return false;
}
void
cluster_reconfig_formation_qvotec_complete(bool success pg_attribute_unused())
{}
void
cluster_reconfig_formation_qvotec_note_max_generation(
	uint64 generation pg_attribute_unused())
{}
void
cluster_reconfig_formation_qvotec_publish_observed(
	const ClusterFormationCommitMarker *marker pg_attribute_unused(),
	const uint64 *incarnation_by_node pg_attribute_unused())
{}
void
cluster_reconfig_formation_qvotec_clear_observed(void)
{}
void
cluster_reconfig_publish_formation_qvotec_latch(struct Latch *latch pg_attribute_unused())
{}
void
cluster_reconfig_bootstrap_publish_begin(void)
{}
void
cluster_reconfig_bootstrap_publish_in_quorum(bool in_quorum pg_attribute_unused())
{}
void
cluster_reconfig_bootstrap_publish_end(void)
{}
/* RF-ROOT P9 verification (cold-formation): cluster_semantic_activation.o
 * (linked here) references the control-root + formation-marker surfaces of
 * the bit22 cutover chain; cluster_control_root.o / cluster_formation_
 * marker.o are not linked into this binary.  GREEN link stubs — the
 * cutover-chain behavior tests live in test_cluster_r4_activation_fsm /
 * test_cluster_formation_marker / test_cluster_reconfig. */
ClusterControlRootResult
cluster_control_root_bootstrap_validate_active_round_fields(
	uint64 transition_epoch pg_attribute_unused(),
	uint64 prepare_generation pg_attribute_unused(),
	uint64 source_feature_bitmap pg_attribute_unused(),
	uint64 target_feature_bitmap pg_attribute_unused())
{
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
ClusterControlRootResult
cluster_control_root_create_prepared(
	const ClusterControlRootMigrationImage *image pg_attribute_unused(),
	const ClusterControlRootMigrationRoundV1 *round pg_attribute_unused(),
	ClusterControlRootFileToken *out_token)
{
	if (out_token != NULL)
		memset(out_token, 0, sizeof(*out_token));
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
ClusterControlRootResult
cluster_control_root_activate_prepared(
	const ClusterControlRootFileToken *expected_token pg_attribute_unused(),
	const uint8 expected_round_sha256[PG_SHA256_DIGEST_LENGTH] pg_attribute_unused(),
	const ClusterControlRootMigrationRoundV1 *round pg_attribute_unused(),
	ClusterControlRootFileToken *out_token)
{
	if (out_token != NULL)
		memset(out_token, 0, sizeof(*out_token));
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
bool
cluster_control_root_round_sha256(
	const ClusterControlRootMigrationRoundV1 *round pg_attribute_unused(),
	uint8 out_sha[PG_SHA256_DIGEST_LENGTH])
{
	if (out_sha != NULL)
		memset(out_sha, 0x11, PG_SHA256_DIGEST_LENGTH);
	return true;
}
ClusterControlRootResult
cluster_control_root_build_migration_image(
	const ClusterControlRootMigrationRoundV1 *round pg_attribute_unused(),
	ClusterControlRootMigrationImage *out)
{
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
bool
cluster_formation_marker_decode(
	const uint8 slot_bytes[CLUSTER_VOTING_SLOT_BYTES] pg_attribute_unused(),
	ClusterFormationCommitMarker *marker pg_attribute_unused(),
	uint64 *incarnation_by_node pg_attribute_unused())
{
	return false;
}
bool
cluster_formation_marker_validate(
	const uint8 slot_bytes[CLUSTER_VOTING_SLOT_BYTES] pg_attribute_unused(),
	ClusterFormationCommitMarker *out_decoded pg_attribute_unused(),
	uint64 *out_incarnations pg_attribute_unused())
{
	return false;
}
/* The SQL entry pgrac_r4_bit22_cutover_begin references superuser(); this
 * binary does not link the backend superuser machinery. */
bool
superuser(void)
{
	return true;
}
/* spec-5.16: qvotec.o also publishes each peer's durable COMMITTED join marker and
 * supersedes a stale write-fence on self-admit; stub both for the standalone link. */
void cluster_reconfig_record_observed_committed_join(int32 node_id, uint64 incarnation,
													 uint64 epoch);
void
cluster_reconfig_record_observed_committed_join(int32 node_id pg_attribute_unused(),
												uint64 incarnation pg_attribute_unused(),
												uint64 epoch pg_attribute_unused())
{}
void cluster_write_fence_supersede_by_admit(uint64 admitted_epoch);
void
cluster_write_fence_supersede_by_admit(uint64 admitted_epoch pg_attribute_unused())
{}
bool
cluster_reconfig_join_qvotec_poll_pending(
	ClusterJoinMarkerMailboxOperationV1 *operation_out,
	int32 *out_target_node, void *out_slot512 pg_attribute_unused())
{
	if (operation_out != NULL)
		*operation_out = CLUSTER_JOIN_MARKER_MAILBOX_WRITE_EXACT;
	if (out_target_node != NULL)
		*out_target_node = -1;
	return false;
}
void
cluster_reconfig_join_qvotec_complete(
	ClusterJoinMarkerMailboxOperationV1 operation pg_attribute_unused(),
	bool acked pg_attribute_unused(),
	const uint8 *verified_image96 pg_attribute_unused())
{}
bool
cluster_reconfig_qvotec_lifecycle_transition(
	ClusterQvotecMailbox *authority_mailbox,
	pg_atomic_uint32 *qvotec_status, ClusterQvotecStatus next_status)
{
	if (authority_mailbox == NULL || qvotec_status == NULL)
		return false;
	pg_atomic_write_u32(qvotec_status, (uint32)next_status);
	if (next_status == CLUSTER_QVOTEC_STARTING
		|| next_status == CLUSTER_QVOTEC_SHUTTING_DOWN)
		cluster_qvotec_mailbox_restart_reset(authority_mailbox);
	return true;
}
bool
cluster_replacement_phase3_handoff_poll_local(
	ClusterReplacementPhase3HandoffItem *out pg_attribute_unused())
{
	return false;
}
bool
cluster_reconfig_lmon_observe_replacement_ready(
	const ClusterReplacementPhase3HandoffItem *item pg_attribute_unused())
{
	return false;
}
bool
cluster_replacement_episode_is_valid(
	const ClusterReplacementEpisode *episode pg_attribute_unused())
{
	return false;
}
bool
cluster_reconfig_lmon_snapshot_replacement_admitted(
	ClusterReplacementEpisode *out_episode pg_attribute_unused(),
	ClusterReplacementCommitMarkerV3 *out_marker pg_attribute_unused())
{
	return false;
}
bool
cluster_reconfig_lmon_snapshot_admitted_membership(
	uint64 *out_members_lo, uint64 *out_members_hi,
	uint64 *out_formation_epoch)
{
	if (out_members_lo != NULL)
		*out_members_lo = 0;
	if (out_members_hi != NULL)
		*out_members_hi = 0;
	if (out_formation_epoch != NULL)
		*out_formation_epoch = 0;
	return false;
}
ClusterR4PrerequisiteSnapshot
cluster_reconfig_r4_prerequisite_snapshot(void)
{
	return (ClusterR4PrerequisiteSnapshot){
		.status = CLUSTER_R4_PREREQUISITE_RF_DEFERRED,
		.target_node_id = -1,
	};
}
bool
cluster_reconfig_r4_publish_ready(
	const ClusterR4PrerequisiteSnapshot *expected pg_attribute_unused())
{
	return false;
}
ClusterLmsSharedState *
cluster_lms_shared_state(void)
{
	return NULL;
}
bool
cluster_lms_r4_drain_request(
	ClusterLmsSharedState *state pg_attribute_unused(),
	uint64 generation pg_attribute_unused(),
	uint64 *worker_incarnation pg_attribute_unused())
{
	return false;
}
void
cluster_lms_wakeup(int worker_id pg_attribute_unused())
{}
bool
cluster_cr_server_r4_lmon_reclaim_closed(
	uint64 worker_incarnation pg_attribute_unused(),
	uint64 generation pg_attribute_unused())
{
	return false;
}
uint64
cluster_gcs_block_dedup_r4_route_purge_closed(void)
{
	return 0;
}
uint64
cluster_gcs_block_dedup_r4_route_count(void)
{
	return 0;
}
uint64
cluster_gcs_block_r4_requester_count(void)
{
	return 0;
}
ClusterUndoSmgrRootMirrorState
cluster_undo_smgr_root_descriptor_read_candidate(
	const char *root_directory pg_attribute_unused(),
	uint8 observed[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES] pg_attribute_unused())
{
	return CLUSTER_UNDO_SMGR_ROOT_MIRROR_ABSENT;
}
ClusterUndoSmgrRootMirrorState
cluster_undo_smgr_root_descriptor_publish(
	const char *root_directory pg_attribute_unused(),
	const uint8 image[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES] pg_attribute_unused())
{
	return CLUSTER_UNDO_SMGR_ROOT_MIRROR_IO_ERROR;
}
bool
cluster_undo_block0_current_startup_fenced_owned(void)
{
	return false;
}
void cluster_reconfig_publish_join_qvotec_latch(struct Latch *latch);
void
cluster_reconfig_publish_join_qvotec_latch(struct Latch *latch pg_attribute_unused())
{}
void cluster_membership_seed_last_admitted_from_voting_disk(const int *fds, int n_disks);
void
cluster_membership_seed_last_admitted_from_voting_disk(const int *fds pg_attribute_unused(),
													   int n_disks pg_attribute_unused())
{}
/* spec-6.15 D5b: xid stripe region-5 scan + mailbox service (not
 * exercised here; the stripe face has its own truth tables). */
#include "cluster/cluster_xid_stripe_boot.h"
void
cluster_xid_stripe_scan_disks(const int *fds pg_attribute_unused(),
							  int n_disks pg_attribute_unused())
{}
void
cluster_xid_stripe_service_seed(const int *fds pg_attribute_unused(),
								int n_disks pg_attribute_unused())
{}
ClusterXidStripeDiskState
cluster_xid_stripe_disk_state(void)
{
	return CLUSTER_XID_STRIPE_DISK_UNKNOWN;
}
void
cluster_xid_stripe_herding_tick(const int *fds pg_attribute_unused(),
								int n_disks pg_attribute_unused())
{}
#include "cluster/cluster_membership.h" /* ClusterJoinCommitMarker (D5 self-admit) */
void cluster_reconfig_note_self_admitted(uint64 admitted_epoch);
void
cluster_reconfig_note_self_admitted(uint64 admitted_epoch pg_attribute_unused())
{}
bool cluster_reconfig_qvotec_observe_replacement_admitted(
	const int *fds, int n_disks, uint64 live_incarnation);
bool
cluster_reconfig_qvotec_observe_replacement_admitted(
	const int *fds pg_attribute_unused(), int n_disks pg_attribute_unused(),
	uint64 live_incarnation pg_attribute_unused())
{
	return false;
}
/* Hardening v1.1: self-admit now groups by commit identity (HF-3) and gates on
 * the publish-proof (HF-1).  The is_committed_basis stub returns false so the
 * collection loop is empty and neither runs, but the linker needs the symbols. */
bool cluster_reconfig_join_publish_proven(uint64 admitted_epoch);
bool
cluster_reconfig_join_publish_proven(uint64 admitted_epoch pg_attribute_unused())
{
	return false;
}
#ifndef CLUSTER_QVOTEC_PGSA_UNIT_TEST
ClusterVotingDiskIoState cluster_voting_disk_read_join_slot(int fd, uint32 node_id,
																void *out_slot512);
ClusterVotingDiskIoState
cluster_voting_disk_read_join_slot(int fd pg_attribute_unused(),
								   uint32 node_id pg_attribute_unused(),
								   void *out_slot512 pg_attribute_unused())
{
	return CLUSTER_VOTING_DISK_IO_FAILED;
}
#endif
uint64 cluster_epoch_get_current(void);
uint64
cluster_epoch_get_current(void)
{
	return 0;
}
void cluster_undo_horizon_note_self_member(void);
void
cluster_undo_horizon_note_self_member(void)
{}
uint64
GetSystemIdentifier(void)
{
	return UINT64_C(1);
}
bool cluster_sf_peer_capability_generation_matches(int32 peer_id, uint32 required_capabilities,
											uint32 expected_generation);
bool
cluster_sf_peer_capability_generation_matches(int32 peer_id pg_attribute_unused(),
											uint32 required_capabilities pg_attribute_unused(),
											uint32 expected_generation pg_attribute_unused())
{
	return false;
}
#ifndef CLUSTER_QVOTEC_PGSA_UNIT_TEST
void
cluster_voting_disk_io_install_timeout_handler(void)
{}
void
cluster_voting_disk_io_set_timeout_ms(int timeout_ms pg_attribute_unused())
{}
#endif

/* spec-2.6 Sprint A Step 3 D7 stub: postmaster spawn wrapper.
 * Real impl in postmaster.c (file-static StartChildProcess);unit
 * test never spawns so stub returns 0 (failure). */
pid_t
cluster_postmaster_start_qvotec(void)
{
	return 0;
}

bool cluster_enabled = true;

/* spec-2.6 D15 stubs: PG SRF machinery referenced from cluster_qvotec.o
 * SRF bodies (cluster_get_quorum_state / cluster_get_voting_disks).
 * The unit test never invokes the SRFs — symbols only need to resolve. */
#include "funcapi.h"
#include "utils/builtins.h"

char *cluster_voting_disks = NULL;

void
InitMaterializedSRF(FunctionCallInfo fcinfo pg_attribute_unused(),
					bits32 flags pg_attribute_unused())
{}
struct varlena *
cstring_to_text(const char *s pg_attribute_unused())
{
	return NULL;
}
void *
palloc(Size size pg_attribute_unused())
{
	return NULL;
}
void
pfree(void *p pg_attribute_unused())
{}
void
tuplestore_putvalues(Tuplestorestate *state pg_attribute_unused(),
					 TupleDesc tdesc pg_attribute_unused(), Datum *values pg_attribute_unused(),
					 bool *isnull pg_attribute_unused())
{}


UT_DEFINE_GLOBALS();


/* ============================================================
 * T-1: ClusterVotingSlot byte layout — size 512 + per-field offsets.
 * ============================================================ */

UT_TEST(test_voting_slot_size_512)
{
	UT_ASSERT_EQ(sizeof(ClusterVotingSlot), 512);
}

UT_TEST(test_voting_slot_field_offsets)
{
	UT_ASSERT_EQ(offsetof(ClusterVotingSlot, magic), 0);
	UT_ASSERT_EQ(offsetof(ClusterVotingSlot, version), 4);
	UT_ASSERT_EQ(offsetof(ClusterVotingSlot, node_id), 8);
	UT_ASSERT_EQ(offsetof(ClusterVotingSlot, incarnation), 16);
	UT_ASSERT_EQ(offsetof(ClusterVotingSlot, heartbeat_ts_us), 24);
	UT_ASSERT_EQ(offsetof(ClusterVotingSlot, current_epoch), 32);
	UT_ASSERT_EQ(offsetof(ClusterVotingSlot, flags), 40);
	UT_ASSERT_EQ(offsetof(ClusterVotingSlot, disk_index), 48);
	UT_ASSERT_EQ(offsetof(ClusterVotingSlot, generation), 56);
	UT_ASSERT_EQ(offsetof(ClusterVotingSlot, _alive_bitmap), 64);
	UT_ASSERT_EQ(offsetof(ClusterVotingSlot, crc32c), 508);
}

UT_TEST(test_qvotec_preserves_replacement_request_per_disk_fail_closed)
{
	ClusterReplacementRequestMarker marker;
	ClusterVotingSlot prior;
	ClusterVotingSlot next;
	ClusterVotingSlot before;

	memset(&marker, 0, sizeof(marker));
	marker.magic = CLUSTER_REPLACEMENT_MARKER_MAGIC;
	marker.version = CLUSTER_REPLACEMENT_MARKER_VERSION;
	marker.phase = CLUSTER_REPLACEMENT_MARKER_PHASE_REQUESTED;
	marker.target_node_id = 3;
	marker.baseline_epoch = 9;
	marker.old_admitted_incarnation = 40;
	marker.fresh_incarnation = 41;
	marker.request_nonce = 42;
	marker.grammar_fingerprint
		= CLUSTER_REPLACEMENT_EPISODE_GRAMMAR_FINGERPRINT;

	memset(&prior, 0, sizeof(prior));
	prior.node_id = 3;
	prior.incarnation = 41;
	prior.flags = CLUSTER_VOTING_SLOT_FLAG_REPLACEMENT_REQUESTED;
	UT_ASSERT(cluster_replacement_request_pack(prior._reserved1, &marker));
	memset(&next, 0x5a, sizeof(next));
	next.node_id = 3;
	next.incarnation = 41;
	next.flags = CLUSTER_VOTING_SLOT_FLAG_ALIVE;
	UT_ASSERT_EQ(cluster_qvotec_test_replacement_request_preserve(&next, &prior),
		CLUSTER_REPLACEMENT_REQUEST_SLOT_VALID);
	UT_ASSERT((next.flags & CLUSTER_VOTING_SLOT_FLAG_REPLACEMENT_REQUESTED) != 0);
	UT_ASSERT_EQ(memcmp(
		next._reserved1 + CLUSTER_REPLACEMENT_MARKER_RESERVED1_OFFSET,
		prior._reserved1 + CLUSTER_REPLACEMENT_MARKER_RESERVED1_OFFSET,
		CLUSTER_REPLACEMENT_MARKER_BYTES), 0);

	memset(&prior, 0, sizeof(prior));
	memset(&next, 0x3c, sizeof(next));
	next.node_id = 3;
	next.incarnation = 41;
	next.flags = CLUSTER_VOTING_SLOT_FLAG_ALIVE;
	cluster_replacement_request_clear(next._reserved1);
	before = next;
	UT_ASSERT_EQ(cluster_qvotec_test_replacement_request_preserve(&next, &prior),
		CLUSTER_REPLACEMENT_REQUEST_SLOT_CLEAR);
	UT_ASSERT_EQ(memcmp(&next, &before, sizeof(next)), 0);

	prior.node_id = 3;
	prior.incarnation = 41;
	UT_ASSERT(cluster_replacement_request_pack(prior._reserved1, &marker));
	before = next;
	UT_ASSERT_EQ(cluster_qvotec_test_replacement_request_preserve(&next, &prior),
		CLUSTER_REPLACEMENT_REQUEST_SLOT_HOLD);
	UT_ASSERT_EQ(memcmp(&next, &before, sizeof(next)), 0);

	prior.flags = CLUSTER_VOTING_SLOT_FLAG_REPLACEMENT_REQUESTED;
	next.incarnation = 42;
	before = next;
	UT_ASSERT_EQ(cluster_qvotec_test_replacement_request_preserve(&next, &prior),
		CLUSTER_REPLACEMENT_REQUEST_SLOT_HOLD);
	UT_ASSERT_EQ(memcmp(&next, &before, sizeof(next)), 0);
}


/* ============================================================
 * T-2: ClusterQvotecShmem byte layout — existing 128-byte prefix plus
 *      the exact 320-byte spec-5.15A §2.1A.4 SPSC mailbox.
 *
 *	Public test cannot reach private struct sizeof, so verify
 *	indirectly via cluster_qvotec_shmem_size().
 * ============================================================ */

UT_TEST(test_qvotec_shmem_and_mailbox_layout)
{
	UT_ASSERT_EQ(cluster_qvotec_shmem_size(), 448);
	UT_ASSERT_EQ(sizeof(ClusterQvotecMailbox), 320);
	UT_ASSERT_EQ(offsetof(ClusterQvotecMailbox, request_seq), 0);
	UT_ASSERT_EQ(offsetof(ClusterQvotecMailbox, completion_seq), 8);
	UT_ASSERT_EQ(offsetof(ClusterQvotecMailbox, request_opcode), 16);
	UT_ASSERT_EQ(offsetof(ClusterQvotecMailbox, completion_result), 20);
	UT_ASSERT_EQ(offsetof(ClusterQvotecMailbox, request_value), 24);
	UT_ASSERT_EQ(offsetof(ClusterQvotecMailbox, completion_value), 152);
	UT_ASSERT_EQ(offsetof(ClusterQvotecMailbox, completion_ballot), 280);
	UT_ASSERT_EQ(offsetof(ClusterQvotecMailbox, observed_disk_bitmap), 312);
	UT_ASSERT_EQ(offsetof(ClusterQvotecMailbox, actor_phase), 313);
	UT_ASSERT_EQ(offsetof(ClusterQvotecMailbox, detail), 314);
	UT_ASSERT_EQ(offsetof(ClusterQvotecMailbox, reserved), 316);
	UT_ASSERT_EQ(CLUSTER_QVOTEC_MAILBOX_NONE, 0);
	UT_ASSERT_EQ(CLUSTER_QVOTEC_MAILBOX_RECOVER_HEAD, 1);
	UT_ASSERT_EQ(CLUSTER_QVOTEC_MAILBOX_PROPOSE_VALUE, 2);
	UT_ASSERT_EQ(CLUSTER_QVOTEC_MAILBOX_RESULT_NONE, 0);
	UT_ASSERT_EQ(CLUSTER_QVOTEC_MAILBOX_CHOSEN, 1);
	UT_ASSERT_EQ(CLUSTER_QVOTEC_MAILBOX_ADOPTED_OTHER, 2);
	UT_ASSERT_EQ(CLUSTER_QVOTEC_MAILBOX_HOLD, 3);
}

UT_TEST(test_qvotec_mailbox_reset_discards_volatile_handoff)
{
	ClusterQvotecMailbox mailbox;
	const uint8 zero[sizeof(mailbox)] = {0};

	memset(&mailbox, 0xa5, sizeof(mailbox));
	cluster_qvotec_mailbox_restart_reset(&mailbox);

	UT_ASSERT_EQ(memcmp(&mailbox, zero, sizeof(mailbox)), 0);
}

UT_TEST(test_qvotec_mailbox_recover_head_is_even_stable_and_single_outstanding)
{
	ClusterQvotecMailbox mailbox;
	ClusterQvotecMailboxRequest request;
	uint8 zero_value[CLUSTER_QVOTEC_AUTHORITY_VALUE_BYTES] = {0};
	uint64 request_seq = 0;

	cluster_qvotec_mailbox_restart_reset(&mailbox);
	UT_ASSERT_EQ(cluster_qvotec_mailbox_lmon_submit(
					 &mailbox, CLUSTER_QVOTEC_MAILBOX_RECOVER_HEAD, zero_value, &request_seq),
				 CLUSTER_QVOTEC_MAILBOX_SUBMIT_ACCEPTED);
	UT_ASSERT_EQ(request_seq, 2);
	UT_ASSERT_EQ(pg_atomic_read_u64(&mailbox.request_seq), 2);
	UT_ASSERT_EQ(pg_atomic_read_u64(&mailbox.completion_seq), 0);
	UT_ASSERT(cluster_qvotec_mailbox_qvotec_poll(&mailbox, &request));
	UT_ASSERT_EQ(request.request_seq, 2);
	UT_ASSERT_EQ(request.opcode, CLUSTER_QVOTEC_MAILBOX_RECOVER_HEAD);
	UT_ASSERT_EQ(memcmp(request.request_value, zero_value, sizeof(zero_value)), 0);
	UT_ASSERT_EQ(cluster_qvotec_mailbox_lmon_submit(
					 &mailbox, CLUSTER_QVOTEC_MAILBOX_RECOVER_HEAD, zero_value, &request_seq),
				 CLUSTER_QVOTEC_MAILBOX_SUBMIT_BUSY);
}

UT_TEST(test_qvotec_mailbox_actor_completion_round_trip)
{
	ClusterQvotecMailbox mailbox;
	ClusterQvotecMailboxRequest request;
	ClusterQvotecMailboxCompletion completion;
	ClusterQvotecMailboxCompletion observed;
	uint8 request_value[CLUSTER_QVOTEC_AUTHORITY_VALUE_BYTES];
	uint64 request_seq = 0;
	int i;

	for (i = 0; i < (int)sizeof(request_value); i++)
		request_value[i] = (uint8)(i + 1);
	cluster_qvotec_mailbox_restart_reset(&mailbox);
	UT_ASSERT_EQ(cluster_qvotec_mailbox_lmon_submit(
					 &mailbox, CLUSTER_QVOTEC_MAILBOX_PROPOSE_VALUE, request_value, &request_seq),
				 CLUSTER_QVOTEC_MAILBOX_SUBMIT_ACCEPTED);
	UT_ASSERT(cluster_qvotec_mailbox_qvotec_poll(&mailbox, &request));
	UT_ASSERT_EQ(memcmp(request.request_value, request_value, sizeof(request_value)), 0);
	UT_ASSERT(!cluster_qvotec_mailbox_lmon_poll_completion(&mailbox, request_seq, &observed));

	memset(&completion, 0, sizeof(completion));
	completion.request_seq = request_seq;
	completion.result = CLUSTER_QVOTEC_MAILBOX_CHOSEN;
	memcpy(completion.completion_value, request_value, sizeof(request_value));
	for (i = 0; i < (int)sizeof(completion.completion_ballot); i++)
		completion.completion_ballot[i] = (uint8)(0x80 + i);
	completion.observed_disk_bitmap = UINT8_C(0x05);
	completion.actor_phase = CLUSTER_QVOTEC_ACTOR_SETTLE_WRITE;
	completion.detail = UINT16_C(0x1234);

	completion.request_seq += 2;
	UT_ASSERT(!cluster_qvotec_mailbox_qvotec_complete(&mailbox, UINT8_C(0x07), &completion));
	completion.request_seq = request_seq;
	UT_ASSERT(!cluster_qvotec_mailbox_qvotec_complete(&mailbox, UINT8_C(0x03), &completion));
	UT_ASSERT(cluster_qvotec_mailbox_qvotec_complete(&mailbox, UINT8_C(0x07), &completion));
	UT_ASSERT_EQ(pg_atomic_read_u64(&mailbox.completion_seq), request_seq);
	UT_ASSERT(cluster_qvotec_mailbox_lmon_poll_completion(&mailbox, request_seq, &observed));
	UT_ASSERT_EQ(observed.request_seq, request_seq);
	UT_ASSERT_EQ(observed.result, CLUSTER_QVOTEC_MAILBOX_CHOSEN);
	UT_ASSERT_EQ(memcmp(observed.completion_value, request_value, sizeof(request_value)), 0);
	UT_ASSERT_EQ(memcmp(observed.completion_ballot, completion.completion_ballot,
						 sizeof(completion.completion_ballot)),
				 0);
	UT_ASSERT_EQ(observed.observed_disk_bitmap, UINT8_C(0x05));
	UT_ASSERT_EQ(observed.actor_phase, CLUSTER_QVOTEC_ACTOR_SETTLE_WRITE);
	UT_ASSERT_EQ(observed.detail, UINT16_C(0x1234));

	UT_ASSERT_EQ(cluster_qvotec_mailbox_lmon_submit(
					 &mailbox, CLUSTER_QVOTEC_MAILBOX_PROPOSE_VALUE, request_value, &request_seq),
				 CLUSTER_QVOTEC_MAILBOX_SUBMIT_ACCEPTED);
	UT_ASSERT_EQ(request_seq, 4);
}

UT_TEST(test_qvotec_mailbox_rejects_invalid_and_holds_on_sequence_overflow)
{
	ClusterQvotecMailbox mailbox;
	uint8 zero_value[CLUSTER_QVOTEC_AUTHORITY_VALUE_BYTES] = {0};
	uint8 nonzero_value[CLUSTER_QVOTEC_AUTHORITY_VALUE_BYTES] = {1};
	uint64 request_seq = 0;

	cluster_qvotec_mailbox_restart_reset(&mailbox);
	UT_ASSERT_EQ(cluster_qvotec_mailbox_lmon_submit(
					 &mailbox, CLUSTER_QVOTEC_MAILBOX_NONE, zero_value, &request_seq),
				 CLUSTER_QVOTEC_MAILBOX_SUBMIT_INVALID);
	UT_ASSERT_EQ(cluster_qvotec_mailbox_lmon_submit(
					 &mailbox, CLUSTER_QVOTEC_MAILBOX_RECOVER_HEAD, nonzero_value, &request_seq),
				 CLUSTER_QVOTEC_MAILBOX_SUBMIT_INVALID);

	pg_atomic_write_u64(&mailbox.request_seq, UINT64_MAX - 1);
	pg_atomic_write_u64(&mailbox.completion_seq, UINT64_MAX - 1);
	UT_ASSERT_EQ(cluster_qvotec_mailbox_lmon_submit(
					 &mailbox, CLUSTER_QVOTEC_MAILBOX_RECOVER_HEAD, zero_value, &request_seq),
				 CLUSTER_QVOTEC_MAILBOX_SUBMIT_HOLD);
	UT_ASSERT_EQ(pg_atomic_read_u64(&mailbox.request_seq), UINT64_MAX - 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&mailbox.completion_seq), UINT64_MAX - 1);
}

UT_TEST(test_qvotec_mailbox_terminal_hold_completion)
{
	ClusterQvotecMailbox mailbox;
	ClusterQvotecMailboxCompletion completion;
	ClusterQvotecMailboxCompletion observed;
	uint8 zero_value[CLUSTER_QVOTEC_AUTHORITY_VALUE_BYTES] = {0};
	uint64 request_seq = 0;

	cluster_qvotec_mailbox_restart_reset(&mailbox);
	UT_ASSERT_EQ(cluster_qvotec_mailbox_lmon_submit(
					 &mailbox, CLUSTER_QVOTEC_MAILBOX_RECOVER_HEAD, zero_value, &request_seq),
				 CLUSTER_QVOTEC_MAILBOX_SUBMIT_ACCEPTED);
	memset(&completion, 0, sizeof(completion));
	completion.request_seq = request_seq;
	completion.result = CLUSTER_QVOTEC_MAILBOX_HOLD;
	completion.actor_phase = CLUSTER_QVOTEC_ACTOR_HOLD;
	UT_ASSERT(cluster_qvotec_mailbox_qvotec_complete(&mailbox, UINT8_C(0x07), &completion));
	UT_ASSERT(cluster_qvotec_mailbox_lmon_poll_completion(&mailbox, request_seq, &observed));
	UT_ASSERT_EQ(observed.result, CLUSTER_QVOTEC_MAILBOX_HOLD);
	UT_ASSERT_EQ(observed.actor_phase, CLUSTER_QVOTEC_ACTOR_HOLD);
}


/* ============================================================
 * T-3: 7 lifecycle / dump-key accessor surface — pre-shmem-init
 *      returns sane defaults (NULL-safe contract per F11).
 * ============================================================ */

UT_TEST(test_qvotec_accessors_null_safe_pre_init)
{
	UT_ASSERT_EQ(cluster_qvotec_get_pid(), 0);
	UT_ASSERT_STR_EQ(cluster_qvotec_get_status_name(), "(uninitialised)");
	UT_ASSERT_STR_EQ(cluster_qvotec_get_quorum_state_name(), "(uninitialised)");
	UT_ASSERT_EQ(cluster_qvotec_get_disks_ok_count(), 0);
	UT_ASSERT_EQ(cluster_qvotec_get_disks_total_count(), 0);
	UT_ASSERT_EQ(cluster_qvotec_get_current_epoch_at_boot(), 0);
	UT_ASSERT_EQ(cluster_qvotec_get_self_incarnation(), 0);
	UT_ASSERT_STR_EQ(cluster_qvotec_get_collision_state_name(), "(uninitialised)");
}

UT_TEST(test_qvotec_accessors_post_init)
{
	cluster_qvotec_shmem_init();

	UT_ASSERT_EQ(cluster_qvotec_get_pid(), 0); /* Main not entered */
	UT_ASSERT_STR_EQ(cluster_qvotec_get_status_name(), "starting");
	UT_ASSERT_STR_EQ(cluster_qvotec_get_quorum_state_name(), "initializing");
	UT_ASSERT_EQ(cluster_qvotec_get_disks_ok_count(), 0);
	UT_ASSERT_EQ(cluster_qvotec_get_disks_total_count(), 0);
	UT_ASSERT_EQ(cluster_qvotec_get_current_epoch_at_boot(), 0);
	UT_ASSERT_EQ(cluster_qvotec_get_self_incarnation(), 0);
	cluster_qvotec_publish_self_incarnation(919);
	UT_ASSERT_EQ(cluster_qvotec_get_self_incarnation(), 919);
	UT_ASSERT_STR_EQ(cluster_qvotec_get_collision_state_name(), "none");
}


/* ============================================================
 * T-4: cluster_qvotec_in_quorum() lease-aware semantics (Q4 v0.2).
 *
 *	Pre-init / quorum_state != OK / lease expired → all return false.
 * ============================================================ */

UT_TEST(test_in_quorum_missing_shmem_diagnostic_is_once_and_still_false)
{
	quorum_admission_log_count = 0;
	UT_ASSERT(!cluster_qvotec_in_quorum());
	UT_ASSERT(strstr(quorum_admission_log, "PGRAC_REASON=SHMEM_UNAVAILABLE") != NULL);
	UT_ASSERT_EQ(quorum_admission_log_count, 1);
	UT_ASSERT(!cluster_qvotec_in_quorum());
	UT_ASSERT_EQ(quorum_admission_log_count, 1);
}

UT_TEST(test_in_quorum_diagnostics_preserve_exact_state_and_lease_decisions)
{
	const uint32 states[] = { CLUSTER_QVOTEC_QUORUM_INITIALIZING, CLUSTER_QVOTEC_QUORUM_UNCERTAIN,
							  CLUSTER_QVOTEC_QUORUM_LOST, UINT32_MAX };
	const char *reasons[] = { "PGRAC_REASON=STATE_INITIALIZING", "PGRAC_REASON=STATE_UNCERTAIN",
							  "PGRAC_REASON=STATE_LOST", "PGRAC_REASON=STATE_INVALID" };
	TimestampTz saved_now = mock_now;
	char before[sizeof(shmem_storage)];
	unsigned int i;

	/* Set only the frozen quorum@4 and lease@32 fields of the actual region. */
	pg_atomic_write_u32((pg_atomic_uint32 *)(shmem_storage + 4), CLUSTER_QVOTEC_QUORUM_OK);
	pg_atomic_write_u64((pg_atomic_uint64 *)(shmem_storage + 32), 5000000);
	memcpy(before, shmem_storage, sizeof(before));
	mock_now = 4999999;
	quorum_admission_log_count = 0;
	UT_ASSERT(cluster_qvotec_in_quorum());
	UT_ASSERT_EQ(quorum_admission_log_count, 0);
	cluster_freeze_writes_set();
	UT_ASSERT(!cluster_qvotec_in_quorum());
	UT_ASSERT(strstr(quorum_admission_log, "PGRAC_REASON=WRITES_FROZEN") != NULL);
	UT_ASSERT_EQ(quorum_admission_log_count, 1);
	UT_ASSERT(!cluster_qvotec_in_quorum());
	UT_ASSERT_EQ(quorum_admission_log_count, 1);
	UT_ASSERT_EQ(memcmp(before, shmem_storage, sizeof(before)), 0);
	cluster_thaw_writes_set();
	UT_ASSERT(cluster_qvotec_in_quorum());

	for (i = 0; i < lengthof(states); i++) {
		pg_atomic_write_u32((pg_atomic_uint32 *)(shmem_storage + 4), states[i]);
		memcpy(before, shmem_storage, sizeof(before));
		quorum_admission_log_count = 0;
		quorum_admission_log[0] = '\0';
		UT_ASSERT(!cluster_qvotec_in_quorum());
		UT_ASSERT(strstr(quorum_admission_log, reasons[i]) != NULL);
		UT_ASSERT_EQ(quorum_admission_log_count, 1);
		UT_ASSERT(!cluster_qvotec_in_quorum());
		UT_ASSERT_EQ(quorum_admission_log_count, 1);
		UT_ASSERT_EQ(memcmp(before, shmem_storage, sizeof(before)), 0);
	}

	pg_atomic_write_u32((pg_atomic_uint32 *)(shmem_storage + 4), CLUSTER_QVOTEC_QUORUM_OK);
	memcpy(before, shmem_storage, sizeof(before));
	quorum_admission_log_count = 0;
	mock_now = 5000000;
	UT_ASSERT(!cluster_qvotec_in_quorum());
	UT_ASSERT(strstr(quorum_admission_log, "PGRAC_REASON=LEASE_EXPIRED") != NULL);
	UT_ASSERT(strstr(quorum_admission_log, "sampled_expiry_us=5000000 sampled_now_us=5000000")
			  != NULL);
	UT_ASSERT_EQ(quorum_admission_log_count, 1);
	mock_now++;
	UT_ASSERT(!cluster_qvotec_in_quorum());
	UT_ASSERT_EQ(quorum_admission_log_count, 1);
	UT_ASSERT_EQ(memcmp(before, shmem_storage, sizeof(before)), 0);
	pg_atomic_write_u64((pg_atomic_uint64 *)(shmem_storage + 32), 6000000);
	UT_ASSERT(cluster_qvotec_in_quorum());
	UT_ASSERT_EQ(quorum_admission_log_count, 1);
	pg_atomic_write_u32((pg_atomic_uint32 *)(shmem_storage + 4),
						CLUSTER_QVOTEC_QUORUM_INITIALIZING);
	mock_now = saved_now;
}

UT_TEST(test_passive_quorum_observation_neither_renews_nor_logs)
{
	ClusterQvotecObservation observed;
	void (*observe)(ClusterQvotecObservation *) = cluster_qvotec_observe;
	char before[sizeof(shmem_storage)];
	char saved[sizeof(shmem_storage)];
	TimestampTz saved_now = mock_now;

	UT_ASSERT_NOT_NULL((void *)observe);
	if (observe == NULL)
		return;
	memcpy(saved, shmem_storage, sizeof(saved));
	pg_atomic_write_u32((pg_atomic_uint32 *)(shmem_storage + 4), CLUSTER_QVOTEC_QUORUM_LOST);
	pg_atomic_write_u32((pg_atomic_uint32 *)(shmem_storage + 8), 1);
	pg_atomic_write_u32((pg_atomic_uint32 *)(shmem_storage + 12), 3);
	pg_atomic_write_u64((pg_atomic_uint64 *)(shmem_storage + 24), 1000);
	pg_atomic_write_u64((pg_atomic_uint64 *)(shmem_storage + 32), 5000);
	mock_now = 7000;
	memcpy(before, shmem_storage, sizeof(before));
	quorum_admission_log_count = 0;
	observe(&observed);
	UT_ASSERT(observed.attached);
	UT_ASSERT_EQ(observed.quorum_state, CLUSTER_QVOTEC_QUORUM_LOST);
	UT_ASSERT_EQ(observed.disks_ok, 1);
	UT_ASSERT_EQ(observed.disks_total, 3);
	UT_ASSERT_EQ(observed.last_poll_us, 1000);
	UT_ASSERT_EQ(observed.expiry_us, 5000);
	UT_ASSERT_EQ(observed.now_us, 7000);
	UT_ASSERT_EQ(quorum_admission_log_count, 0);
	UT_ASSERT_EQ(memcmp(before, shmem_storage, sizeof(before)), 0);
	memcpy(shmem_storage, saved, sizeof(saved));
	mock_now = saved_now;
}

UT_TEST(test_in_quorum_pre_shmem_init_false)
{
	/* Reset shmem stub */
	shmem_init_done = false;

	UT_ASSERT(!(cluster_qvotec_in_quorum()));
}

UT_TEST(test_quorum_lease_six_polls_preserves_exact_expiry_and_fail_closed)
{
	void (*publish)(uint64) = cluster_qvotec_test_publish_poll_lease;
	TimestampTz saved_now = mock_now;
	int saved_poll = cluster_quorum_poll_interval_ms;
	char saved[sizeof(shmem_storage)];
	char published[sizeof(shmem_storage)];
	const uint32 denied_states[]
		= { CLUSTER_QVOTEC_QUORUM_INITIALIZING, CLUSTER_QVOTEC_QUORUM_UNCERTAIN,
			CLUSTER_QVOTEC_QUORUM_LOST, UINT32_MAX };
	unsigned int i;

	UT_ASSERT_NOT_NULL((void *)publish);
	if (publish == NULL)
		return;
	memcpy(saved, shmem_storage, sizeof(saved));
	cluster_quorum_poll_interval_ms = 2000;
	pg_atomic_write_u32((pg_atomic_uint32 *)(shmem_storage + 4), CLUSTER_QVOTEC_QUORUM_OK);
	publish(1000000);
	UT_ASSERT_EQ(pg_atomic_read_u64((pg_atomic_uint64 *)(shmem_storage + 24)), 1000000);
	UT_ASSERT_EQ(pg_atomic_read_u64((pg_atomic_uint64 *)(shmem_storage + 32)), 13000000);
	memcpy(published, shmem_storage, sizeof(published));

	/* The old four-second boundary is inside the approved live window. */
	mock_now = 5000000;
	UT_ASSERT(cluster_qvotec_in_quorum());
	mock_now = 12999999;
	UT_ASSERT(cluster_qvotec_in_quorum());
	mock_now = 13000000;
	UT_ASSERT(!cluster_qvotec_in_quorum());
	mock_now = 13000001;
	UT_ASSERT(!cluster_qvotec_in_quorum());
	UT_ASSERT_EQ(memcmp(published, shmem_storage, sizeof(published)), 0);

	mock_now = 1000001;
	cluster_freeze_writes_set();
	UT_ASSERT(!cluster_qvotec_in_quorum());
	UT_ASSERT_EQ(memcmp(published, shmem_storage, sizeof(published)), 0);
	cluster_thaw_writes_set();
	for (i = 0; i < lengthof(denied_states); i++) {
		pg_atomic_write_u32((pg_atomic_uint32 *)(shmem_storage + 4), denied_states[i]);
		UT_ASSERT(!cluster_qvotec_in_quorum());
	}

	/* Only the existing publisher renews; it does not turn a LOST state OK. */
	pg_atomic_write_u32((pg_atomic_uint32 *)(shmem_storage + 4), CLUSTER_QVOTEC_QUORUM_LOST);
	publish(14000000);
	UT_ASSERT_EQ(pg_atomic_read_u64((pg_atomic_uint64 *)(shmem_storage + 32)), 26000000);
	UT_ASSERT_EQ(cluster_qvotec_get_quorum_state(), CLUSTER_QVOTEC_QUORUM_LOST);
	UT_ASSERT(!cluster_qvotec_in_quorum());
	pg_atomic_write_u32((pg_atomic_uint32 *)(shmem_storage + 4), CLUSTER_QVOTEC_QUORUM_OK);
	mock_now = 14000000;
	UT_ASSERT(cluster_qvotec_in_quorum());
	cluster_quorum_poll_interval_ms = saved_poll;
	mock_now = saved_now;
	memcpy(shmem_storage, saved, sizeof(saved));
}

UT_TEST(test_in_quorum_initializing_state_false)
{
	cluster_qvotec_shmem_init();

	/* state == INITIALIZING (default after shmem_init), lease not set */
	UT_ASSERT(!(cluster_qvotec_in_quorum()));
}

UT_TEST(test_in_quorum_frozen_flag_overrides_to_false)
{
	cluster_qvotec_shmem_init();

	/* Even if state were OK + lease live, frozen flag should win.
	 * Test the flag arm + helper return. */
	cluster_freeze_writes_set();
	UT_ASSERT(!(cluster_qvotec_in_quorum()));

	cluster_thaw_writes_set();
	/* state is INITIALIZING so still false even after thaw */
	UT_ASSERT(!(cluster_qvotec_in_quorum()));
}


/* ============================================================
 * T-5: ProcSignal flag round-trip.
 * ============================================================ */

UT_TEST(test_freeze_thaw_round_trip)
{
	UT_ASSERT(!(cluster_writes_currently_frozen()));

	cluster_freeze_writes_set();
	UT_ASSERT(cluster_writes_currently_frozen());

	cluster_thaw_writes_set();
	UT_ASSERT(!(cluster_writes_currently_frozen()));
}


/* ============================================================
 * T-6: ClusterQvotecMain symbol resolves at link time.
 *
 *	Postmaster reaper wiring lands Step 3 D7;here we just verify
 *	the function symbol exists for linker (address-take only — never
 *	invoke).
 * ============================================================ */

UT_TEST(test_qvotec_main_symbol_link_resolves)
{
	void (*p_main)(void) = ClusterQvotecMain;
	UT_ASSERT_NOT_NULL((void *)p_main);
}

UT_TEST(test_qvotec_poll_cadence_subtracts_cycle_work)
{
	UT_ASSERT_EQ(cluster_qvotec_test_poll_wait_timeout_ms(0, 500), 500);
	UT_ASSERT_EQ(cluster_qvotec_test_poll_wait_timeout_ms(125000, 500), 375);
	UT_ASSERT_EQ(cluster_qvotec_test_poll_wait_timeout_ms(499001, 500), 1);
	UT_ASSERT_EQ(cluster_qvotec_test_poll_wait_timeout_ms(500000, 500), 0);
	UT_ASSERT_EQ(cluster_qvotec_test_poll_wait_timeout_ms(900000, 500), 0);
	UT_ASSERT_EQ(cluster_qvotec_test_poll_wait_timeout_ms(0, 0), 0);
}

UT_TEST(test_qvotec_poll_pre_injection_only_target_and_once)
{
	void (*poll_pre)(void) = cluster_qvotec_test_poll_pre_injection;

	UT_ASSERT_NOT_NULL((void *)poll_pre);
	if (poll_pre == NULL)
		return;
	MyBackendType = B_QVOTEC;
	poll_injection_armed = false;
	poll_pre();
	UT_ASSERT_EQ(injected_sleeps, 0);
	UT_ASSERT_EQ(poll_injection_hits, 0);
	poll_injection_armed = true;
	cluster_injection_armed_count = 1;
	MyBackendType = B_BACKEND;
	poll_pre();
	UT_ASSERT_EQ(injected_sleeps, 0);
	UT_ASSERT_EQ(poll_injection_hits, 0);
	MyBackendType = B_QVOTEC;
	poll_pre();
	poll_pre();
	UT_ASSERT_EQ(injected_sleeps, 1);
	UT_ASSERT_EQ(injected_sleep_us, 75000);
	UT_ASSERT_EQ(poll_injection_hits, 1);
	poll_injection_armed = false;
	poll_pre();
	UT_ASSERT_EQ(injected_sleeps, 1);
	poll_injection_param = 6000000;
	poll_injection_armed = true;
	poll_pre();
	UT_ASSERT_EQ(injected_sleeps, 2);
	UT_ASSERT_EQ(injected_sleep_us, 6000000);
	UT_ASSERT_EQ(poll_injection_hits, 2);
	poll_injection_armed = false;
	poll_pre();
	poll_injection_param = 14000000;
	poll_injection_armed = true;
	poll_pre();
	poll_pre();
	UT_ASSERT_EQ(injected_sleeps, 3);
	UT_ASSERT_EQ(injected_sleep_us, 14000000);
	UT_ASSERT_EQ(poll_injection_hits, 3);
	poll_injection_armed = false;
	poll_pre();
	poll_injection_param = 14000001;
	poll_injection_armed = true;
	poll_pre();
	poll_pre();
	UT_ASSERT_EQ(injected_sleeps, 3);
	poll_injection_armed = false;
	poll_pre();
	MyBackendType = B_INVALID;
	cluster_injection_armed_count = 0;
}


/* ============================================================
 * T-7: 4 enum numeric values frozen + accessor name round-trip.
 *
 *	SQL views (Step 5) observe these values;preserve the mapping.
 * ============================================================ */

UT_TEST(test_qvotec_status_enum_values)
{
	UT_ASSERT_EQ(CLUSTER_QVOTEC_STARTING, 0);
	UT_ASSERT_EQ(CLUSTER_QVOTEC_READY, 1);
	UT_ASSERT_EQ(CLUSTER_QVOTEC_SHUTTING_DOWN, 2);
	UT_ASSERT_EQ(CLUSTER_QVOTEC_DOWN, 3);
	UT_ASSERT_EQ(CLUSTER_QVOTEC_FAILED, 4);
}

UT_TEST(test_quorum_state_enum_values)
{
	UT_ASSERT_EQ(CLUSTER_QVOTEC_QUORUM_INITIALIZING, 0);
	UT_ASSERT_EQ(CLUSTER_QVOTEC_QUORUM_OK, 1);
	UT_ASSERT_EQ(CLUSTER_QVOTEC_QUORUM_UNCERTAIN, 2);
	UT_ASSERT_EQ(CLUSTER_QVOTEC_QUORUM_LOST, 3);
}

UT_TEST(test_voting_disk_io_state_enum_values)
{
	UT_ASSERT_EQ(CLUSTER_VOTING_DISK_IO_OK, 0);
	UT_ASSERT_EQ(CLUSTER_VOTING_DISK_IO_TORN, 1);
	UT_ASSERT_EQ(CLUSTER_VOTING_DISK_IO_FAILED, 2);
	UT_ASSERT_EQ(CLUSTER_VOTING_DISK_IO_NOT_TRIED, 3);
}

UT_TEST(test_collision_state_enum_values)
{
	UT_ASSERT_EQ(CLUSTER_COLLISION_NONE, 0);
	UT_ASSERT_EQ(CLUSTER_COLLISION_OBSERVED_OLDER, 1);
	UT_ASSERT_EQ(CLUSTER_COLLISION_FATAL_NEWER_SELF, 2);
}

#define PGSA_TEST_DISKS 3

typedef struct PgsaDiskSet {
	int fds[PGSA_TEST_DISKS];
	char paths[PGSA_TEST_DISKS][MAXPGPATH];
} PgsaDiskSet;

static void pgsa_disk_set_close(PgsaDiskSet *set);

static bool
pgsa_disk_set_open(PgsaDiskSet *set)
{
	int i;

	memset(set, 0, sizeof(*set));
	for (i = 0; i < PGSA_TEST_DISKS; i++)
		set->fds[i] = -1;
	for (i = 0; i < PGSA_TEST_DISKS; i++) {
		int n = snprintf(set->paths[i], sizeof(set->paths[i]),
						 "/tmp/pgrac-pgsa-%ld-%d-XXXXXX", (long)getpid(), i);

		if (n < 0 || n >= (int)sizeof(set->paths[i]))
			goto fail;
		set->fds[i] = mkstemp(set->paths[i]);
		if (set->fds[i] < 0
			|| ftruncate(set->fds[i], CLUSTER_VOTING_FILE_BYTES_MIN) != 0)
			goto fail;
	}
	return true;

fail:
	pgsa_disk_set_close(set);
	return false;
}

static void
pgsa_disk_set_close(PgsaDiskSet *set)
{
	int i;

	for (i = 0; i < PGSA_TEST_DISKS; i++) {
		if (set->fds[i] >= 0)
			(void)close(set->fds[i]);
		if (set->paths[i][0] != '\0')
			(void)unlink(set->paths[i]);
		set->fds[i] = -1;
	}
}

static bool
pgrd_test_image(uint8 root_kind, int32 owner_node, uint8 uuid_marker,
				 uint8 out[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES])
{
	ClusterUndoRootDescriptorV1 descriptor;
	uint32 root_ordinal;

	memset(&descriptor, 0, sizeof(descriptor));
	root_ordinal = root_kind == CLUSTER_UNDO_ROOT_KIND_SHARED
					   ? 0
					   : (uint32)owner_node + 1;
	descriptor.descriptor_incarnation = 1;
	descriptor.root_kind = root_kind;
	descriptor.owner_node = owner_node;
	descriptor.root_ordinal = root_ordinal;
	memset(descriptor.root_uuid, uuid_marker,
		   sizeof(descriptor.root_uuid));
	descriptor.system_identifier = UINT64_C(0x0123456789abcdef);
	if (!cluster_undo_root_namespace_id(1, root_ordinal,
										&descriptor.namespace_id))
		return false;
	return cluster_undo_root_descriptor_encode(&descriptor, out);
}

static int
pgrd_count_image(const PgsaDiskSet *set, off_t offset,
				  const uint8 expected[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES])
{
	uint8 actual[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	int count = 0;
	int i;

	for (i = 0; i < PGSA_TEST_DISKS; i++) {
		memset(actual, 0, sizeof(actual));
		if (cluster_voting_disk_read_raw_slot_at(set->fds[i], offset, actual)
				== CLUSTER_VOTING_DISK_RAW_READ_FULL
			&& memcmp(actual, expected, sizeof(actual)) == 0)
			count++;
	}
	return count;
}

UT_TEST(test_pgrd_initial_shared_eof_provisions_exact_majority)
{
	PgsaDiskSet set;
	uint8 desired[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	uint8 completed = 0;

	if (!pgsa_disk_set_open(&set)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT(pgrd_test_image(CLUSTER_UNDO_ROOT_KIND_SHARED, -1, 0x31,
						   desired));
	UT_ASSERT(cluster_qvotec_test_undo_root_descriptor_provision(
		set.fds, PGSA_TEST_DISKS, UINT64_C(0x0123456789abcdef), desired,
		&completed));
	UT_ASSERT_EQ(completed, UINT8_C(0x07));
	UT_ASSERT_EQ(pgrd_count_image(
		&set, CLUSTER_UNDO_ROOT_DESCRIPTOR_SHARED_OFFSET, desired),
		PGSA_TEST_DISKS);
	pgsa_disk_set_close(&set);
}

UT_TEST(test_pgrd_exact_partial_retry_is_idempotent)
{
	PgsaDiskSet set;
	uint8 desired[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	uint8 completed = 0;

	if (!pgsa_disk_set_open(&set)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT(pgrd_test_image(CLUSTER_UNDO_ROOT_KIND_SHARED, -1, 0x42,
						   desired));
	UT_ASSERT_EQ(cluster_voting_disk_write_raw_slot_at(
					 set.fds[0], CLUSTER_UNDO_ROOT_DESCRIPTOR_SHARED_OFFSET,
					 desired),
				 CLUSTER_VOTING_DISK_IO_OK);
	UT_ASSERT(cluster_qvotec_test_undo_root_descriptor_provision(
		set.fds, PGSA_TEST_DISKS, UINT64_C(0x0123456789abcdef), desired,
		&completed));
	UT_ASSERT_EQ(completed, UINT8_C(0x07));
	UT_ASSERT_EQ(pgrd_count_image(
		&set, CLUSTER_UNDO_ROOT_DESCRIPTOR_SHARED_OFFSET, desired),
		PGSA_TEST_DISKS);
	pgsa_disk_set_close(&set);
}

UT_TEST(test_pgrd_reachable_conflict_holds_without_mutation)
{
	PgsaDiskSet set;
	uint8 desired[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	uint8 conflict[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	uint8 observed[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	uint8 completed = UINT8_C(0xa5);
	struct stat st;

	if (!pgsa_disk_set_open(&set)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT(pgrd_test_image(CLUSTER_UNDO_ROOT_KIND_SHARED, -1, 0x53,
						   desired));
	UT_ASSERT(pgrd_test_image(CLUSTER_UNDO_ROOT_KIND_SHARED, -1, 0x64,
						   conflict));
	UT_ASSERT_EQ(cluster_voting_disk_write_raw_slot_at(
					 set.fds[0], CLUSTER_UNDO_ROOT_DESCRIPTOR_SHARED_OFFSET,
					 conflict),
				 CLUSTER_VOTING_DISK_IO_OK);
	UT_ASSERT(!cluster_qvotec_test_undo_root_descriptor_provision(
		set.fds, PGSA_TEST_DISKS, UINT64_C(0x0123456789abcdef), desired,
		&completed));
	UT_ASSERT_EQ(completed, UINT8_C(0xa5));
	UT_ASSERT_EQ(cluster_voting_disk_read_raw_slot_at(
					 set.fds[0], CLUSTER_UNDO_ROOT_DESCRIPTOR_SHARED_OFFSET,
					 observed),
				 CLUSTER_VOTING_DISK_RAW_READ_FULL);
	UT_ASSERT_EQ(memcmp(observed, conflict, sizeof(observed)), 0);
	UT_ASSERT_EQ(fstat(set.fds[1], &st), 0);
	UT_ASSERT_EQ(st.st_size, CLUSTER_UNDO_ROOT_DESCRIPTOR_SHARED_OFFSET);
	UT_ASSERT_EQ(fstat(set.fds[2], &st), 0);
	UT_ASSERT_EQ(st.st_size, CLUSTER_UNDO_ROOT_DESCRIPTOR_SHARED_OFFSET);
	pgsa_disk_set_close(&set);
}

UT_TEST(test_pgrd_short_read_holds_despite_clean_majority)
{
	PgsaDiskSet set;
	uint8 desired[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	uint8 completed = UINT8_C(0x96);

	if (!pgsa_disk_set_open(&set)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT(pgrd_test_image(CLUSTER_UNDO_ROOT_KIND_SHARED, -1, 0x75,
						   desired));
	UT_ASSERT_EQ(ftruncate(
		set.fds[0], CLUSTER_UNDO_ROOT_DESCRIPTOR_SHARED_OFFSET + 17), 0);
	UT_ASSERT(!cluster_qvotec_test_undo_root_descriptor_provision(
		set.fds, PGSA_TEST_DISKS, UINT64_C(0x0123456789abcdef), desired,
		&completed));
	UT_ASSERT_EQ(completed, UINT8_C(0x96));
	UT_ASSERT_EQ(pgrd_count_image(
		&set, CLUSTER_UNDO_ROOT_DESCRIPTOR_SHARED_OFFSET, desired), 0);
	pgsa_disk_set_close(&set);
}

UT_TEST(test_pgrd_postwrite_one_of_three_exact_does_not_commit)
{
	PgsaDiskSet set;
	uint8 desired[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	uint8 completed = UINT8_C(0x87);
	int i;

	if (!pgsa_disk_set_open(&set)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT(pgrd_test_image(CLUSTER_UNDO_ROOT_KIND_SHARED, -1, 0x80,
						   desired));
	for (i = 1; i < PGSA_TEST_DISKS; i++) {
		UT_ASSERT_EQ(close(set.fds[i]), 0);
		set.fds[i] = open(set.paths[i], O_RDONLY | PG_BINARY);
		UT_ASSERT(set.fds[i] >= 0);
	}
	UT_ASSERT(!cluster_qvotec_test_undo_root_descriptor_provision(
		set.fds, PGSA_TEST_DISKS, UINT64_C(0x0123456789abcdef), desired,
		&completed));
	UT_ASSERT_EQ(completed, UINT8_C(0x87));
	UT_ASSERT_EQ(pgrd_count_image(
		&set, CLUSTER_UNDO_ROOT_DESCRIPTOR_SHARED_OFFSET, desired), 1);
	pgsa_disk_set_close(&set);
}

UT_TEST(test_pgrd_local_node_127_uses_last_frozen_slot)
{
	PgsaDiskSet set;
	uint8 desired[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	uint8 zero[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES] = { 0 };
	uint8 observed[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	uint8 completed = 0;
	struct stat st;
	int i;

	if (!pgsa_disk_set_open(&set)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT(pgrd_test_image(CLUSTER_UNDO_ROOT_KIND_LOCAL, 127, 0x86,
						   desired));
	UT_ASSERT(cluster_qvotec_test_undo_root_descriptor_provision(
		set.fds, PGSA_TEST_DISKS, UINT64_C(0x0123456789abcdef), desired,
		&completed));
	UT_ASSERT_EQ(completed, UINT8_C(0x07));
	for (i = 0; i < PGSA_TEST_DISKS; i++) {
		UT_ASSERT_EQ(fstat(set.fds[i], &st), 0);
		/* B′ P0: the attested capacity (CLUSTER_VOTING_PGRD_FILE_BYTES_MIN,
		 * 8N+3 slots) now covers region 7, but the runtime file still
		 * materializes regions lazily — the node-127 write extends the
		 * base file exactly through the last descriptor slot. */
		UT_ASSERT_EQ(st.st_size,
					 CLUSTER_UNDO_ROOT_DESCRIPTOR_LOCAL_OFFSET(127)
					 + CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES);
		UT_ASSERT_EQ(cluster_voting_disk_read_raw_slot_at(
						 set.fds[i],
						 CLUSTER_UNDO_ROOT_DESCRIPTOR_SHARED_OFFSET, observed),
					 CLUSTER_VOTING_DISK_RAW_READ_FULL);
		UT_ASSERT_EQ(memcmp(observed, zero, sizeof(observed)), 0);
	}
	UT_ASSERT_EQ(pgrd_count_image(
		&set, CLUSTER_UNDO_ROOT_DESCRIPTOR_LOCAL_OFFSET(127), desired),
		PGSA_TEST_DISKS);
	pgsa_disk_set_close(&set);
}

UT_TEST(test_pgrd_majority_read_requires_two_exact_images)
{
	PgsaDiskSet set;
	ClusterUndoRootDescriptorV1 observed;
	uint8 desired[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	uint8 observed_bitmap = UINT8_C(0xa5);
	int i;

	if (!pgsa_disk_set_open(&set)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT(pgrd_test_image(CLUSTER_UNDO_ROOT_KIND_SHARED, -1, 0x91,
						   desired));
	UT_ASSERT_EQ(cluster_voting_disk_write_raw_slot_at(
					 set.fds[0], CLUSTER_UNDO_ROOT_DESCRIPTOR_SHARED_OFFSET,
					 desired),
				 CLUSTER_VOTING_DISK_IO_OK);
	UT_ASSERT_EQ(close(set.fds[1]), 0);
	set.fds[1] = -1;
	UT_ASSERT_EQ(close(set.fds[2]), 0);
	set.fds[2] = -1;
	memset(&observed, 0, sizeof(observed));
	UT_ASSERT_EQ(cluster_qvotec_test_undo_root_descriptor_read(
					 set.fds, PGSA_TEST_DISKS,
					 UINT64_C(0x0123456789abcdef),
					 CLUSTER_UNDO_ROOT_KIND_SHARED, -1, &observed,
					 &observed_bitmap),
				 CLUSTER_UNDO_ROOT_DESCRIPTOR_HOLD);

	set.fds[1] = open(set.paths[1], O_RDWR | PG_BINARY);
	UT_ASSERT(set.fds[1] >= 0);
	set.fds[2] = open(set.paths[2], O_RDWR | PG_BINARY);
	UT_ASSERT(set.fds[2] >= 0);
	UT_ASSERT_EQ(cluster_voting_disk_write_raw_slot_at(
					 set.fds[1], CLUSTER_UNDO_ROOT_DESCRIPTOR_SHARED_OFFSET,
					 desired),
				 CLUSTER_VOTING_DISK_IO_OK);
	UT_ASSERT_EQ(cluster_qvotec_test_undo_root_descriptor_read(
					 set.fds, PGSA_TEST_DISKS,
					 UINT64_C(0x0123456789abcdef),
					 CLUSTER_UNDO_ROOT_KIND_SHARED, -1, &observed,
					 &observed_bitmap),
				 CLUSTER_UNDO_ROOT_DESCRIPTOR_VALID);
	UT_ASSERT_EQ(observed_bitmap, UINT8_C(0x03));
	UT_ASSERT_EQ(observed.descriptor_incarnation, UINT64_C(1));
	UT_ASSERT_EQ(observed.root_kind, CLUSTER_UNDO_ROOT_KIND_SHARED);
	UT_ASSERT_EQ(observed.owner_node, -1);
	UT_ASSERT_EQ(observed.root_ordinal, UINT32_C(0));
	for (i = 0; i < CLUSTER_UNDO_ROOT_UUID_BYTES; i++)
		UT_ASSERT_EQ(observed.root_uuid[i], UINT8_C(0x91));
	UT_ASSERT_EQ(observed.namespace_id, UINT64_C(1));
	UT_ASSERT_EQ(observed.system_identifier,
				 UINT64_C(0x0123456789abcdef));
	pgsa_disk_set_close(&set);
}

UT_TEST(test_pgrd_majority_read_short_member_holds)
{
	PgsaDiskSet set;
	ClusterUndoRootDescriptorV1 observed;
	ClusterUndoRootDescriptorV1 zero;
	uint8 desired[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	uint8 observed_bitmap = UINT8_C(0xa5);

	if (!pgsa_disk_set_open(&set)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT(pgrd_test_image(CLUSTER_UNDO_ROOT_KIND_SHARED, -1, 0x92,
						   desired));
	UT_ASSERT_EQ(cluster_voting_disk_write_raw_slot_at(
					 set.fds[0], CLUSTER_UNDO_ROOT_DESCRIPTOR_SHARED_OFFSET,
					 desired),
				 CLUSTER_VOTING_DISK_IO_OK);
	UT_ASSERT_EQ(cluster_voting_disk_write_raw_slot_at(
					 set.fds[1], CLUSTER_UNDO_ROOT_DESCRIPTOR_SHARED_OFFSET,
					 desired),
				 CLUSTER_VOTING_DISK_IO_OK);
	UT_ASSERT_EQ(ftruncate(
		set.fds[2], CLUSTER_UNDO_ROOT_DESCRIPTOR_SHARED_OFFSET + 17), 0);
	memset(&observed, 0xa5, sizeof(observed));
	memset(&zero, 0, sizeof(zero));
	UT_ASSERT_EQ(cluster_qvotec_test_undo_root_descriptor_read(
					 set.fds, PGSA_TEST_DISKS,
					 UINT64_C(0x0123456789abcdef),
					 CLUSTER_UNDO_ROOT_KIND_SHARED, -1, &observed,
					 &observed_bitmap),
				 CLUSTER_UNDO_ROOT_DESCRIPTOR_HOLD);
	UT_ASSERT_EQ(memcmp(&observed, &zero, sizeof(observed)), 0);
	UT_ASSERT_EQ(observed_bitmap, UINT8_C(0));
	pgsa_disk_set_close(&set);
}

UT_TEST(test_pgrd_majority_read_same_incarnation_conflict_holds)
{
	PgsaDiskSet set;
	ClusterUndoRootDescriptorV1 observed;
	ClusterUndoRootDescriptorV1 zero;
	uint8 desired[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	uint8 conflict[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	uint8 observed_bitmap = UINT8_C(0xa5);
	int i;

	if (!pgsa_disk_set_open(&set)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT(pgrd_test_image(CLUSTER_UNDO_ROOT_KIND_SHARED, -1, 0x93,
						   desired));
	UT_ASSERT(pgrd_test_image(CLUSTER_UNDO_ROOT_KIND_SHARED, -1, 0x94,
						   conflict));
	for (i = 0; i < PGSA_TEST_DISKS; i++)
		UT_ASSERT_EQ(cluster_voting_disk_write_raw_slot_at(
						 set.fds[i], CLUSTER_UNDO_ROOT_DESCRIPTOR_SHARED_OFFSET,
						 i == 2 ? conflict : desired),
					 CLUSTER_VOTING_DISK_IO_OK);
	memset(&observed, 0xa5, sizeof(observed));
	memset(&zero, 0, sizeof(zero));
	UT_ASSERT_EQ(cluster_qvotec_test_undo_root_descriptor_read(
					 set.fds, PGSA_TEST_DISKS,
					 UINT64_C(0x0123456789abcdef),
					 CLUSTER_UNDO_ROOT_KIND_SHARED, -1, &observed,
					 &observed_bitmap),
				 CLUSTER_UNDO_ROOT_DESCRIPTOR_HOLD);
	UT_ASSERT_EQ(memcmp(&observed, &zero, sizeof(observed)), 0);
	UT_ASSERT_EQ(observed_bitmap, UINT8_C(0));
	pgsa_disk_set_close(&set);
}

static ClusterSemanticActivationRecord
pgsa_record(ClusterSemanticActivationPhase phase, uint64 generation, uint64 source, uint64 target)
{
	ClusterSemanticActivationRecord record;

	memset(&record, 0, sizeof(record));
	record.source_feature_bitmap = source;
	record.target_feature_bitmap = target;
	record.transition_epoch = UINT64_C(0x101);
	record.record_generation = generation;
	record.admitted_members_lo = UINT64_C(0x0f);
	record.capability_sample_digest = UINT64_C(0x202);
	record.coordinator_incarnation = UINT64_C(0x303);
	record.coordinator_node = 1;
	record.phase = phase;
	return record;
}

static bool
pgsa_encode(ClusterSemanticActivationRecord record,
			uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES])
{
	memset(bytes, 0, CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES);
	return cluster_semantic_activation_record_encode(&record, bytes);
}

static bool
pgsa_write_image(int fd, const uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES])
{
	return cluster_voting_disk_write_raw_tail_slot(fd, bytes) == CLUSTER_VOTING_DISK_IO_OK;
}

static bool
pgsa_read_image(int fd, uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES])
{
	memset(bytes, 0, CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES);
	return cluster_voting_disk_read_raw_tail_slot(fd, bytes)
		   == CLUSTER_VOTING_DISK_RAW_READ_FULL;
}

static bool
pgsa_snapshot(const PgsaDiskSet *set,
			  uint8 images[PGSA_TEST_DISKS][CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES])
{
	int i;

	for (i = 0; i < PGSA_TEST_DISKS; i++) {
		if (!pgsa_read_image(set->fds[i], images[i]))
			return false;
	}
	return true;
}

static bool
pgsa_snapshot_matches(
	const PgsaDiskSet *set,
	const uint8 expected[PGSA_TEST_DISKS][CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES])
{
	uint8 actual[PGSA_TEST_DISKS][CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	int i;

	if (!pgsa_snapshot(set, actual))
		return false;
	for (i = 0; i < PGSA_TEST_DISKS; i++) {
		if (memcmp(actual[i], expected[i], sizeof(actual[i])) != 0)
			return false;
	}
	return true;
}

static int
pgsa_count_image(const PgsaDiskSet *set,
				 const uint8 expected[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES])
{
	uint8 actual[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	int count = 0;
	int i;

	for (i = 0; i < PGSA_TEST_DISKS; i++) {
		if (pgsa_read_image(set->fds[i], actual)
			&& memcmp(actual, expected, sizeof(actual)) == 0)
			count++;
	}
	return count;
}

static ClusterSemanticActivationResult
pgsa_cas(const PgsaDiskSet *set, uint64 expected_generation, uint64 expected_source,
		 const uint8 desired[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES])
{
	return cluster_qvotec_test_semantic_activation_record_cas_write(
		set->fds, PGSA_TEST_DISKS, expected_generation, expected_source, desired);
}

UT_TEST(test_pgsa_01_expected_majority_plus_stale_commits_desired)
{
	PgsaDiskSet set;
	uint8 current[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 stale[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 desired[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	if (!pgsa_disk_set_open(&set)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT(pgsa_encode(pgsa_record(CLUSTER_SEMANTIC_PHASE_PREPARE, 7, 0x11, 0x22),
						  current));
	UT_ASSERT(pgsa_encode(pgsa_record(CLUSTER_SEMANTIC_PHASE_OPEN, 6, 0x11, 0x11), stale));
	UT_ASSERT(pgsa_encode(pgsa_record(CLUSTER_SEMANTIC_PHASE_COMMIT, 8, 0x11, 0x22),
						  desired));
	UT_ASSERT(pgsa_write_image(set.fds[0], current));
	UT_ASSERT(pgsa_write_image(set.fds[1], current));
	UT_ASSERT(pgsa_write_image(set.fds[2], stale));
	UT_ASSERT_EQ(pgsa_cas(&set, 7, 0x11, desired), CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT(pgsa_count_image(&set, desired) >= 2);
	pgsa_disk_set_close(&set);
}

UT_TEST(test_pgsa_02_generation_mismatch_is_conflict_without_mutation)
{
	PgsaDiskSet set;
	uint8 current[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 desired[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 before[PGSA_TEST_DISKS][CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	int i;

	if (!pgsa_disk_set_open(&set)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT(pgsa_encode(pgsa_record(CLUSTER_SEMANTIC_PHASE_COMMIT, 7, 0x11, 0x22),
						  current));
	UT_ASSERT(pgsa_encode(pgsa_record(CLUSTER_SEMANTIC_PHASE_OPEN, 7, 0x11, 0x22), desired));
	for (i = 0; i < PGSA_TEST_DISKS; i++)
		UT_ASSERT(pgsa_write_image(set.fds[i], current));
	UT_ASSERT(pgsa_snapshot(&set, before));
	UT_ASSERT_EQ(pgsa_cas(&set, 6, 0x11, desired),
				 CLUSTER_SEMANTIC_ACTIVATION_RECORD_CONFLICT);
	UT_ASSERT(pgsa_snapshot_matches(&set, before));
	pgsa_disk_set_close(&set);
}

UT_TEST(test_pgsa_03_source_mismatch_is_conflict_without_mutation)
{
	PgsaDiskSet set;
	uint8 current[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 desired[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 before[PGSA_TEST_DISKS][CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	int i;

	if (!pgsa_disk_set_open(&set)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT(pgsa_encode(pgsa_record(CLUSTER_SEMANTIC_PHASE_COMMIT, 7, 0x11, 0x22),
						  current));
	UT_ASSERT(pgsa_encode(pgsa_record(CLUSTER_SEMANTIC_PHASE_OPEN, 8, 0x44, 0x22), desired));
	for (i = 0; i < PGSA_TEST_DISKS; i++)
		UT_ASSERT(pgsa_write_image(set.fds[i], current));
	UT_ASSERT(pgsa_snapshot(&set, before));
	UT_ASSERT_EQ(pgsa_cas(&set, 7, 0x44, desired),
				 CLUSTER_SEMANTIC_ACTIVATION_RECORD_CONFLICT);
	UT_ASSERT(pgsa_snapshot_matches(&set, before));
	pgsa_disk_set_close(&set);
}

UT_TEST(test_pgsa_04_commit_to_open_requires_explicit_prior_source)
{
	PgsaDiskSet set;
	uint8 current[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 desired[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 before[PGSA_TEST_DISKS][CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	int i;

	if (!pgsa_disk_set_open(&set)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT(pgsa_encode(pgsa_record(CLUSTER_SEMANTIC_PHASE_COMMIT, 7, 0x11, 0x22),
						  current));
	UT_ASSERT(pgsa_encode(pgsa_record(CLUSTER_SEMANTIC_PHASE_OPEN, 8, 0x11, 0x22), desired));
	for (i = 0; i < PGSA_TEST_DISKS; i++)
		UT_ASSERT(pgsa_write_image(set.fds[i], current));
	UT_ASSERT(pgsa_snapshot(&set, before));
	UT_ASSERT_EQ(pgsa_cas(&set, 7, 0x22, desired),
				 CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
	UT_ASSERT(pgsa_snapshot_matches(&set, before));
	UT_ASSERT_EQ(pgsa_cas(&set, 7, 0x11, desired), CLUSTER_SEMANTIC_ACTIVATION_OK);
	pgsa_disk_set_close(&set);
}

UT_TEST(test_pgsa_04a_open_to_next_prepare_uses_open_target_as_current_source)
{
	PgsaDiskSet set;
	uint8 current[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 desired[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	int i;

	if (!pgsa_disk_set_open(&set)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT(pgsa_encode(pgsa_record(
		CLUSTER_SEMANTIC_PHASE_OPEN, 7, 0x11, 0x22), current));
	UT_ASSERT(pgsa_encode(pgsa_record(
		CLUSTER_SEMANTIC_PHASE_PREPARE, 8, 0x22, 0x422), desired));
	for (i = 0; i < PGSA_TEST_DISKS; i++)
		UT_ASSERT(pgsa_write_image(set.fds[i], current));
	UT_ASSERT_EQ(pgsa_cas(&set, 7, 0x22, desired),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT_EQ(pgsa_count_image(&set, desired), PGSA_TEST_DISKS);
	pgsa_disk_set_close(&set);
}

UT_TEST(test_pgsa_04b_desired_source_mismatch_is_bad_state_before_io)
{
	uint8 desired[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(pgsa_encode(pgsa_record(
		CLUSTER_SEMANTIC_PHASE_COMMIT, 8, 0x44, 0x55), desired));
	UT_ASSERT_EQ(cluster_qvotec_test_semantic_activation_record_cas_write(
				 NULL, 0, 7, 0x11, desired),
				 CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
}

UT_TEST(test_pgsa_04c_rollback_complete_projects_target_as_current_source)
{
	PgsaDiskSet set;
	uint8 current[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 desired[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	int i;

	if (!pgsa_disk_set_open(&set)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT(pgsa_encode(pgsa_record(
		CLUSTER_SEMANTIC_PHASE_ROLLBACK_COMPLETE, 7, 0x11, 0x22),
		current));
	UT_ASSERT(pgsa_encode(pgsa_record(
		CLUSTER_SEMANTIC_PHASE_PREPARE, 8, 0x22, 0x422), desired));
	for (i = 0; i < PGSA_TEST_DISKS; i++)
		UT_ASSERT(pgsa_write_image(set.fds[i], current));
	UT_ASSERT_EQ(pgsa_cas(&set, 7, 0x22, desired),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT_EQ(pgsa_count_image(&set, desired), PGSA_TEST_DISKS);
	pgsa_disk_set_close(&set);
}

UT_TEST(test_pgsa_04d_unknown_current_phase_holds_without_mutation)
{
	PgsaDiskSet set;
	uint8 current[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 desired[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 before[PGSA_TEST_DISKS][CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	int i;

	if (!pgsa_disk_set_open(&set)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT(pgsa_encode(pgsa_record(
		CLUSTER_SEMANTIC_PHASE_COMMIT, 7, 0x11, 0x22), current));
	current[16] = (uint8)(CLUSTER_SEMANTIC_PHASE_ROLLBACK_COMPLETE + 1);
	UT_ASSERT(pgsa_encode(pgsa_record(
		CLUSTER_SEMANTIC_PHASE_OPEN, 8, 0x11, 0x22), desired));
	for (i = 0; i < PGSA_TEST_DISKS; i++)
		UT_ASSERT(pgsa_write_image(set.fds[i], current));
	UT_ASSERT(pgsa_snapshot(&set, before));
	UT_ASSERT_EQ(pgsa_cas(&set, 7, 0x11, desired),
				 CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD);
	UT_ASSERT(pgsa_snapshot_matches(&set, before));
	pgsa_disk_set_close(&set);
}

UT_TEST(test_pgsa_05_lost_completion_replay_is_idempotent_ok)
{
	PgsaDiskSet set;
	uint8 current[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 desired[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	int i;

	if (!pgsa_disk_set_open(&set)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT(pgsa_encode(pgsa_record(CLUSTER_SEMANTIC_PHASE_PREPARE, 7, 0x11, 0x22),
						  current));
	UT_ASSERT(pgsa_encode(pgsa_record(CLUSTER_SEMANTIC_PHASE_COMMIT, 8, 0x11, 0x22),
						  desired));
	for (i = 0; i < PGSA_TEST_DISKS; i++)
		UT_ASSERT(pgsa_write_image(set.fds[i], current));
	UT_ASSERT_EQ(pgsa_cas(&set, 7, 0x11, desired), CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT_EQ(pgsa_cas(&set, 7, 0x11, desired), CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT_EQ(pgsa_count_image(&set, desired), PGSA_TEST_DISKS);
	pgsa_disk_set_close(&set);
}

UT_TEST(test_pgsa_06_split_or_no_disks_holds_without_mutation)
{
	PgsaDiskSet set;
	uint8 images[PGSA_TEST_DISKS][CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 desired[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 before[PGSA_TEST_DISKS][CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	int i;

	if (!pgsa_disk_set_open(&set)) {
		UT_ASSERT(false);
		return;
	}
	for (i = 0; i < PGSA_TEST_DISKS; i++) {
		UT_ASSERT(pgsa_encode(pgsa_record(CLUSTER_SEMANTIC_PHASE_COMMIT, 7, 0x11,
												 0x20 + (uint64)i),
							  images[i]));
		UT_ASSERT(pgsa_write_image(set.fds[i], images[i]));
	}
	UT_ASSERT(pgsa_encode(pgsa_record(CLUSTER_SEMANTIC_PHASE_OPEN, 8, 0x11, 0x22), desired));
	UT_ASSERT(pgsa_snapshot(&set, before));
	UT_ASSERT_EQ(pgsa_cas(&set, 7, 0x11, desired),
				 CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD);
	UT_ASSERT(pgsa_snapshot_matches(&set, before));
	UT_ASSERT_EQ(cluster_qvotec_test_semantic_activation_record_cas_write(
					 NULL, 0, 7, 0x11, desired),
				 CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD);
	pgsa_disk_set_close(&set);
}

UT_TEST(test_pgsa_07_postwrite_one_of_three_desired_holds)
{
	PgsaDiskSet set;
	uint8 current[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 desired[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	int i;

	if (!pgsa_disk_set_open(&set)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT(pgsa_encode(pgsa_record(CLUSTER_SEMANTIC_PHASE_PREPARE, 7, 0x11, 0x22),
						  current));
	UT_ASSERT(pgsa_encode(pgsa_record(CLUSTER_SEMANTIC_PHASE_COMMIT, 8, 0x11, 0x22),
						  desired));
	for (i = 0; i < PGSA_TEST_DISKS; i++)
		UT_ASSERT(pgsa_write_image(set.fds[i], current));
	for (i = 1; i < PGSA_TEST_DISKS; i++) {
		(void)close(set.fds[i]);
		set.fds[i] = open(set.paths[i], O_RDONLY);
		UT_ASSERT(set.fds[i] >= 0);
	}
	UT_ASSERT_EQ(pgsa_cas(&set, 7, 0x11, desired),
				 CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD);
	UT_ASSERT_EQ(pgsa_count_image(&set, desired), 1);
	pgsa_disk_set_close(&set);
}

UT_TEST(test_pgsa_08_clean_eof_zero_pair_accepts_generation_one)
{
	PgsaDiskSet set;
	uint8 desired[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	if (!pgsa_disk_set_open(&set)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT(pgsa_encode(pgsa_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1, 0, 0), desired));
	UT_ASSERT_EQ(pgsa_cas(&set, 0, 0, desired), CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT_EQ(pgsa_count_image(&set, desired), PGSA_TEST_DISKS);
	pgsa_disk_set_close(&set);
}

UT_TEST(test_pgsa_09_invalid_desired_and_overflow_are_bad_state_no_mutation)
{
	PgsaDiskSet set;
	uint8 current[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 invalid[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES] = { 0 };
	uint8 desired[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 before[PGSA_TEST_DISKS][CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	int i;

	if (!pgsa_disk_set_open(&set)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT(pgsa_encode(pgsa_record(CLUSTER_SEMANTIC_PHASE_OPEN, 7, 0x11, 0x11), current));
	UT_ASSERT(pgsa_encode(pgsa_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1, 0x11, 0x11), desired));
	for (i = 0; i < PGSA_TEST_DISKS; i++)
		UT_ASSERT(pgsa_write_image(set.fds[i], current));
	UT_ASSERT(pgsa_snapshot(&set, before));
	UT_ASSERT_EQ(pgsa_cas(&set, 7, 0x11, invalid),
				 CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
	UT_ASSERT_EQ(cluster_qvotec_test_semantic_activation_record_cas_write(
					 set.fds, PGSA_TEST_DISKS, 7, 0x11, NULL),
				 CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
	UT_ASSERT_EQ(pgsa_cas(&set, UINT64_MAX, 0x11, desired),
				 CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
	UT_ASSERT(pgsa_snapshot_matches(&set, before));
	pgsa_disk_set_close(&set);
}

UT_TEST(test_pgsa_10_read_selects_exact_majority_and_reports_conflict)
{
	PgsaDiskSet set;
	uint8 first[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 second[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 selected[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	bool implicit_open = false;
	int i;

	if (!pgsa_disk_set_open(&set)) {
		UT_ASSERT(false);
		return;
	}
	memset(selected, 0xa5, sizeof(selected));
	UT_ASSERT_EQ(cluster_qvotec_test_semantic_activation_record_read(
					 set.fds, PGSA_TEST_DISKS, selected, &implicit_open),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT(implicit_open);
	for (i = 0; i < CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES; i++)
		UT_ASSERT_EQ(selected[i], 0);

	UT_ASSERT(pgsa_encode(
		pgsa_record(CLUSTER_SEMANTIC_PHASE_OPEN, 7, 0x11, 0x11), first));
	UT_ASSERT(pgsa_write_image(set.fds[0], first));
	UT_ASSERT(pgsa_write_image(set.fds[1], first));
	memset(selected, 0, sizeof(selected));
	implicit_open = true;
	UT_ASSERT_EQ(cluster_qvotec_test_semantic_activation_record_read(
					 set.fds, PGSA_TEST_DISKS, selected, &implicit_open),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT(!implicit_open);
	UT_ASSERT_EQ(memcmp(selected, first, sizeof(first)), 0);

	UT_ASSERT(pgsa_encode(
		pgsa_record(CLUSTER_SEMANTIC_PHASE_OPEN, 7, 0x22, 0x22), second));
	UT_ASSERT(pgsa_write_image(set.fds[1], second));
	memset(selected, 0xa5, sizeof(selected));
	implicit_open = true;
	UT_ASSERT_EQ(cluster_qvotec_test_semantic_activation_record_read(
					 set.fds, PGSA_TEST_DISKS, selected, &implicit_open),
				 CLUSTER_SEMANTIC_ACTIVATION_RECORD_CONFLICT);
	UT_ASSERT(!implicit_open);
	for (i = 0; i < CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES; i++)
		UT_ASSERT_EQ(selected[i], 0);
	pgsa_disk_set_close(&set);
}

UT_TEST(test_jcmk_v3_write_tally_cannot_ack_without_exact_readback)
{
	uint8 staged[CLUSTER_VOTING_SLOT_BYTES] = { 0 };
	int unreadable_fds[3] = { -1, -1, -1 };

	/* Exact little-endian JCMK-v3 discriminator at byte offset 4. */
	staged[0] = 'K';
	staged[1] = 'M';
	staged[2] = 'C';
	staged[3] = 'J';
	staged[4] = CLUSTER_JCMK_REPLACEMENT_VERSION;
	UT_ASSERT(!cluster_qvotec_test_join_marker_ack_proven(
		unreadable_fds, 3, 1, staged, 3));
}

UT_TEST(test_jcmk_v3_exact_readback_majority_acks)
{
	PgsaDiskSet set;
	uint8 staged[CLUSTER_VOTING_SLOT_BYTES];
	int i;

	if (!pgsa_disk_set_open(&set)) {
		UT_ASSERT(false);
		return;
	}
	for (i = 0; i < CLUSTER_VOTING_SLOT_BYTES; i++)
		staged[i] = (uint8)(i ^ 0x5a);
	for (i = 0; i < PGSA_TEST_DISKS; i++)
		UT_ASSERT_EQ(cluster_voting_disk_write_join_slot(
						 set.fds[i], 1, staged),
					 CLUSTER_VOTING_DISK_IO_OK);
	UT_ASSERT(cluster_qvotec_test_join_marker_ack_proven(
		set.fds, PGSA_TEST_DISKS, 1, staged, PGSA_TEST_DISKS));
	pgsa_disk_set_close(&set);
}

UT_TEST(test_jcmk_v3_split_readback_cannot_form_false_majority)
{
	PgsaDiskSet set;
	uint8 staged[CLUSTER_VOTING_SLOT_BYTES];
	uint8 other[CLUSTER_VOTING_SLOT_BYTES];
	int i;

	if (!pgsa_disk_set_open(&set)) {
		UT_ASSERT(false);
		return;
	}
	for (i = 0; i < CLUSTER_VOTING_SLOT_BYTES; i++) {
		staged[i] = (uint8)(i ^ 0x5a);
		other[i] = (uint8)(i ^ 0xa5);
	}
	UT_ASSERT_EQ(cluster_voting_disk_write_join_slot(set.fds[0], 1, staged),
				 CLUSTER_VOTING_DISK_IO_OK);
	for (i = 1; i < PGSA_TEST_DISKS; i++)
		UT_ASSERT_EQ(cluster_voting_disk_write_join_slot(
						 set.fds[i], 1, other),
					 CLUSTER_VOTING_DISK_IO_OK);
	UT_ASSERT(!cluster_qvotec_test_join_marker_ack_proven(
		set.fds, PGSA_TEST_DISKS, 1, staged, PGSA_TEST_DISKS));
	pgsa_disk_set_close(&set);
}

UT_TEST(test_jcmk_v3_verify_reads_configured_majority_without_writes)
{
	PgsaDiskSet set;
	ClusterReplacementCommitMarkerV3 marker;
	ClusterReplacementCommitMarkerV3 other;
	uint8 image[CLUSTER_JCMK_REPLACEMENT_BYTES];
	uint8 other_image[CLUSTER_JCMK_REPLACEMENT_BYTES];
	uint8 slot[CLUSTER_VOTING_SLOT_BYTES];
	uint8 other_slot[CLUSTER_VOTING_SLOT_BYTES];
	uint8 before[PGSA_TEST_DISKS][CLUSTER_VOTING_SLOT_BYTES];
	uint8 after[PGSA_TEST_DISKS][CLUSTER_VOTING_SLOT_BYTES];
	uint8 verified[CLUSTER_JCMK_REPLACEMENT_BYTES];
	int one_read_failure[PGSA_TEST_DISKS];
	int i;

	if (!pgsa_disk_set_open(&set)) {
		UT_ASSERT(false);
		return;
	}
	memset(&marker, 0, sizeof(marker));
	marker.magic = CLUSTER_JCMK_MAGIC;
	marker.version = CLUSTER_JCMK_REPLACEMENT_VERSION;
	marker.target_node_id = 7;
	marker.phase = CLUSTER_JCMK_REPLACEMENT_PHASE_COMMITTED_CLOSED;
	marker.generation = UINT64_C(11);
	marker.old_admitted_incarnation = UINT64_C(20);
	marker.fresh_incarnation = UINT64_C(21);
	marker.baseline_epoch = UINT64_C(30);
	marker.reserved_or_committed_epoch = UINT64_C(31);
	marker.request_nonce = UINT64_C(40);
	marker.expected_purge_survivors[0] = UINT8_C(0x06);
	marker.grammar_fingerprint = UINT64_C(50);
	other = marker;
	other.generation++;
	UT_ASSERT(cluster_replacement_marker_v3_encode(&marker, image));
	UT_ASSERT(cluster_replacement_marker_v3_encode(&other, other_image));
	memset(slot, 0, sizeof(slot));
	memset(other_slot, 0, sizeof(other_slot));
	memcpy(slot, image, sizeof(image));
	memcpy(other_slot, other_image, sizeof(other_image));
	for (i = 0; i < PGSA_TEST_DISKS; i++)
		UT_ASSERT_EQ(cluster_voting_disk_write_join_slot(
						 set.fds[i], (uint32)marker.target_node_id,
						 i < 2 ? slot : other_slot),
					 CLUSTER_VOTING_DISK_IO_OK);
	for (i = 0; i < PGSA_TEST_DISKS; i++)
		UT_ASSERT_EQ(cluster_voting_disk_read_join_slot(
						 set.fds[i], (uint32)marker.target_node_id, before[i]),
					 CLUSTER_VOTING_DISK_IO_OK);

	memset(verified, 0xa5, sizeof(verified));
	UT_ASSERT(cluster_qvotec_test_join_marker_verify_committed_closed(
		set.fds, PGSA_TEST_DISKS, marker.target_node_id, verified));
	UT_ASSERT_EQ(memcmp(verified, image, sizeof(image)), 0);
	for (i = 0; i < PGSA_TEST_DISKS; i++) {
		UT_ASSERT_EQ(cluster_voting_disk_read_join_slot(
						 set.fds[i], (uint32)marker.target_node_id, after[i]),
					 CLUSTER_VOTING_DISK_IO_OK);
		UT_ASSERT_EQ(memcmp(before[i], after[i], sizeof(before[i])), 0);
	}

	/* A read failure remains in the configured denominator: one exact image
	 * plus one different image is not a majority of three. */
	one_read_failure[0] = set.fds[0];
	one_read_failure[1] = -1;
	one_read_failure[2] = set.fds[2];
	memset(verified, 0xa5, sizeof(verified));
	UT_ASSERT(!cluster_qvotec_test_join_marker_verify_committed_closed(
		one_read_failure, PGSA_TEST_DISKS, marker.target_node_id, verified));
	for (i = 0; i < CLUSTER_JCMK_REPLACEMENT_BYTES; i++)
		UT_ASSERT_EQ(verified[i], 0);

	/* Even a byte-identical majority is ineligible in the ADMITTED phase. */
	marker.phase = CLUSTER_JCMK_REPLACEMENT_PHASE_ADMITTED;
	marker.ready_state_generation = UINT32_C(9);
	UT_ASSERT(cluster_replacement_marker_v3_encode(&marker, image));
	memset(slot, 0, sizeof(slot));
	memcpy(slot, image, sizeof(image));
	for (i = 0; i < 2; i++)
		UT_ASSERT_EQ(cluster_voting_disk_write_join_slot(
						 set.fds[i], (uint32)marker.target_node_id, slot),
					 CLUSTER_VOTING_DISK_IO_OK);
	memset(verified, 0xa5, sizeof(verified));
	UT_ASSERT(!cluster_qvotec_test_join_marker_verify_committed_closed(
		set.fds, PGSA_TEST_DISKS, marker.target_node_id, verified));
	for (i = 0; i < CLUSTER_JCMK_REPLACEMENT_BYTES; i++)
		UT_ASSERT_EQ(verified[i], 0);
	pgsa_disk_set_close(&set);
}

UT_TEST(test_epoch_ballot_recover_head_requires_exact_settled_majority)
{
	PgsaDiskSet set;
	ClusterEpochAuthorityValue value;
	ClusterEpochAuthorityValue recovered_value;
	ClusterEpochBallotId ballot;
	ClusterEpochBallotId recovered_ballot;
	ClusterEpochBallotLane lane;
	uint64 admitted[CLUSTER_MAX_NODES] = { 0 };
	uint8 lane_image[CLUSTER_EPOCH_BALLOT_LANE_BYTES];
	uint8 value_image[CLUSTER_QVOTEC_AUTHORITY_VALUE_BYTES];
	uint8 ballot_image[CLUSTER_QVOTEC_BALLOT_BYTES];
	uint8 observed = 0;
	const uint64 sysid = UINT64_C(0x3132333435363738);

	if (!pgsa_disk_set_open(&set)) {
		UT_ASSERT(false);
		return;
	}
	memset(&value, 0, sizeof(value));
	value.value_version = CLUSTER_EPOCH_AUTHORITY_VALUE_VERSION;
	value.transition = CLUSTER_EPOCH_AUTHORITY_GENESIS;
	value.event_kind = CLUSTER_EPOCH_EVENT_GENESIS;
	value.request_origin_node = 0;
	value.target_node_id = 0;
	value.authority_generation = 1;
	value.baseline_epoch = 0;
	value.reserved_epoch = 0;
	value.authority_member_bitmap[0] = UINT8_C(0x03);
	value.grammar_fingerprint = CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT;

	memset(&ballot, 0, sizeof(ballot));
	ballot.counter = UINT64_C(7);
	ballot.proposer_node_id = 1;
	ballot.proposer_admitted_incarnation = UINT64_C(111);
	ballot.nonce = UINT64_C(0x5152535455565758);

	memset(&lane, 0, sizeof(lane));
	lane.magic = CLUSTER_EPOCH_BALLOT_MAGIC;
	lane.version = CLUSTER_EPOCH_BALLOT_VERSION;
	lane.last_write_phase = CLUSTER_EPOCH_BALLOT_PHASE_SETTLED;
	lane.proposer_node_id = 1;
	lane.configured_disk_count = PGSA_TEST_DISKS;
	lane.proposer_admitted_incarnation
		= ballot.proposer_admitted_incarnation;
	lane.lane_generation = UINT64_C(5);
	lane.system_identifier = sysid;
	lane.grammar_fingerprint = CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT;
	lane.promised_ballot = ballot;
	lane.accepted_ballot = ballot;
	lane.accepted_value = value;
	lane.settled_ballot = ballot;
	lane.settled_value = value;
	UT_ASSERT(cluster_epoch_ballot_lane_encode(
		&lane, 1, PGSA_TEST_DISKS, ballot.proposer_admitted_incarnation,
		sysid, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT, lane_image));
	admitted[1] = ballot.proposer_admitted_incarnation;

	UT_ASSERT_EQ(cluster_voting_disk_write_epoch_ballot_slot(
					 set.fds[0], 1, lane_image),
				 CLUSTER_VOTING_DISK_IO_OK);
	UT_ASSERT_EQ((int)cluster_qvotec_test_epoch_ballot_recover_head(
		set.fds, PGSA_TEST_DISKS, sysid, admitted, value_image,
		ballot_image, &observed),
				 (int)CLUSTER_QVOTEC_MAILBOX_HOLD);
	UT_ASSERT_EQ((int)observed, 0);

	UT_ASSERT_EQ(cluster_voting_disk_write_epoch_ballot_slot(
					 set.fds[1], 1, lane_image),
				 CLUSTER_VOTING_DISK_IO_OK);
	UT_ASSERT_EQ((int)cluster_qvotec_test_epoch_ballot_recover_head(
		set.fds, PGSA_TEST_DISKS, sysid, admitted, value_image,
		ballot_image, &observed),
				 (int)CLUSTER_QVOTEC_MAILBOX_CHOSEN);
	UT_ASSERT_EQ((int)observed, (int)UINT8_C(0x03));
	UT_ASSERT(cluster_epoch_authority_value_decode(
		value_image, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT,
		&recovered_value));
	UT_ASSERT(cluster_epoch_ballot_id_decode(
		ballot_image, &recovered_ballot));
	UT_ASSERT_EQ(memcmp(&recovered_value, &value, sizeof(value)), 0);
	UT_ASSERT_EQ(memcmp(&recovered_ballot, &ballot, sizeof(ballot)), 0);
	pgsa_disk_set_close(&set);
}

UT_TEST(test_epoch_ballot_phase1_preserves_history_and_observed_promise_floor)
{
	PgsaDiskSet set;
	ClusterEpochAuthorityValue value;
	ClusterEpochBallotId settled_ballot;
	ClusterEpochBallotId promised_ballot;
	ClusterEpochBallotId higher_peer_ballot;
	ClusterEpochBallotId stale_attempt;
	ClusterEpochBallotLane lane;
	ClusterEpochBallotLane decoded;
	uint64 admitted[CLUSTER_MAX_NODES] = { 0 };
	uint8 lane_image[CLUSTER_EPOCH_BALLOT_LANE_BYTES];
	uint8 observed = 0;
	const uint64 sysid = UINT64_C(0x4142434445464748);
	int i;

	if (!pgsa_disk_set_open(&set)) {
		UT_ASSERT(false);
		return;
	}
	memset(&value, 0, sizeof(value));
	value.value_version = CLUSTER_EPOCH_AUTHORITY_VALUE_VERSION;
	value.transition = CLUSTER_EPOCH_AUTHORITY_GENESIS;
	value.event_kind = CLUSTER_EPOCH_EVENT_GENESIS;
	value.request_origin_node = 0;
	value.target_node_id = 0;
	value.authority_generation = 1;
	value.authority_member_bitmap[0] = UINT8_C(0x07);
	value.grammar_fingerprint = CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT;

	memset(&settled_ballot, 0, sizeof(settled_ballot));
	settled_ballot.counter = UINT64_C(7);
	settled_ballot.proposer_node_id = 1;
	settled_ballot.proposer_admitted_incarnation = UINT64_C(111);
	settled_ballot.nonce = UINT64_C(0x5152535455565758);
	memset(&lane, 0, sizeof(lane));
	lane.magic = CLUSTER_EPOCH_BALLOT_MAGIC;
	lane.version = CLUSTER_EPOCH_BALLOT_VERSION;
	lane.last_write_phase = CLUSTER_EPOCH_BALLOT_PHASE_SETTLED;
	lane.proposer_node_id = 1;
	lane.configured_disk_count = PGSA_TEST_DISKS;
	lane.proposer_admitted_incarnation = settled_ballot.proposer_admitted_incarnation;
	lane.lane_generation = UINT64_C(5);
	lane.system_identifier = sysid;
	lane.grammar_fingerprint = CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT;
	lane.promised_ballot = settled_ballot;
	lane.accepted_ballot = settled_ballot;
	lane.accepted_value = value;
	lane.settled_ballot = settled_ballot;
	lane.settled_value = value;
	UT_ASSERT(cluster_epoch_ballot_lane_encode(
		&lane, 1, PGSA_TEST_DISKS, settled_ballot.proposer_admitted_incarnation,
		sysid, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT, lane_image));
	for (i = 0; i < 2; i++)
		UT_ASSERT_EQ(cluster_voting_disk_write_epoch_ballot_slot(
					 set.fds[i], 1, lane_image),
				 CLUSTER_VOTING_DISK_IO_OK);
	admitted[1] = settled_ballot.proposer_admitted_incarnation;
	admitted[2] = UINT64_C(222);

	promised_ballot = settled_ballot;
	promised_ballot.counter = UINT64_C(8);
	promised_ballot.nonce = UINT64_C(0x6162636465666768);
	UT_ASSERT(cluster_qvotec_test_epoch_ballot_phase1_promise(
		set.fds, PGSA_TEST_DISKS, sysid, admitted, 1,
		&promised_ballot, &observed));
	UT_ASSERT_EQ((int)observed, (int)UINT8_C(0x07));
	for (i = 0; i < PGSA_TEST_DISKS; i++) {
		UT_ASSERT_EQ(cluster_voting_disk_read_epoch_ballot_slot(
					 set.fds[i], 1, lane_image),
				 CLUSTER_VOTING_DISK_IO_OK);
		UT_ASSERT(cluster_epoch_ballot_lane_decode(
			lane_image, 1, PGSA_TEST_DISKS,
			settled_ballot.proposer_admitted_incarnation, sysid,
			CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT, &decoded));
		UT_ASSERT_EQ((int)decoded.last_write_phase,
					 (int)CLUSTER_EPOCH_BALLOT_PHASE_PROMISED);
		UT_ASSERT_EQ(decoded.lane_generation, UINT64_C(6));
		UT_ASSERT_EQ(memcmp(&decoded.promised_ballot, &promised_ballot,
						 sizeof(promised_ballot)), 0);
		UT_ASSERT_EQ(memcmp(&decoded.accepted_ballot, &settled_ballot,
						 sizeof(settled_ballot)), 0);
		UT_ASSERT_EQ(memcmp(&decoded.accepted_value, &value, sizeof(value)), 0);
		UT_ASSERT_EQ(memcmp(&decoded.settled_ballot, &settled_ballot,
						 sizeof(settled_ballot)), 0);
		UT_ASSERT_EQ(memcmp(&decoded.settled_value, &value, sizeof(value)), 0);
	}

	memset(&lane, 0, sizeof(lane));
	higher_peer_ballot.counter = UINT64_C(10);
	higher_peer_ballot.proposer_node_id = 2;
	higher_peer_ballot.reserved = 0;
	higher_peer_ballot.proposer_admitted_incarnation = admitted[2];
	higher_peer_ballot.nonce = UINT64_C(0x7172737475767778);
	lane.magic = CLUSTER_EPOCH_BALLOT_MAGIC;
	lane.version = CLUSTER_EPOCH_BALLOT_VERSION;
	lane.last_write_phase = CLUSTER_EPOCH_BALLOT_PHASE_PROMISED;
	lane.proposer_node_id = 2;
	lane.configured_disk_count = PGSA_TEST_DISKS;
	lane.proposer_admitted_incarnation = admitted[2];
	lane.lane_generation = 1;
	lane.system_identifier = sysid;
	lane.grammar_fingerprint = CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT;
	lane.promised_ballot = higher_peer_ballot;
	UT_ASSERT(cluster_epoch_ballot_lane_encode(
		&lane, 2, PGSA_TEST_DISKS, admitted[2], sysid,
		CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT, lane_image));
	UT_ASSERT_EQ(cluster_voting_disk_write_epoch_ballot_slot(
				 set.fds[0], 2, lane_image),
			 CLUSTER_VOTING_DISK_IO_OK);
	stale_attempt = promised_ballot;
	stale_attempt.counter = UINT64_C(9);
	stale_attempt.nonce = UINT64_C(0x8182838485868788);
	observed = UINT8_C(0xff);
	UT_ASSERT(!cluster_qvotec_test_epoch_ballot_phase1_promise(
		set.fds, PGSA_TEST_DISKS, sysid, admitted, 1,
		&stale_attempt, &observed));
	UT_ASSERT_EQ((int)observed, 0);
	for (i = 0; i < PGSA_TEST_DISKS; i++) {
		UT_ASSERT_EQ(cluster_voting_disk_read_epoch_ballot_slot(
					 set.fds[i], 1, lane_image),
				 CLUSTER_VOTING_DISK_IO_OK);
		UT_ASSERT(cluster_epoch_ballot_lane_decode(
			lane_image, 1, PGSA_TEST_DISKS,
			settled_ballot.proposer_admitted_incarnation, sysid,
			CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT, &decoded));
		UT_ASSERT_EQ(decoded.lane_generation, UINT64_C(6));
		UT_ASSERT_EQ(memcmp(&decoded.promised_ballot, &promised_ballot,
						 sizeof(promised_ballot)), 0);
	}
	pgsa_disk_set_close(&set);
}

UT_TEST(test_epoch_ballot_formation_rejects_fixture_disks)
{
	PgsaDiskSet set;

	if (!pgsa_disk_set_open(&set)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT(!cluster_qvotec_test_epoch_ballot_formation_attested(
		set.fds, PGSA_TEST_DISKS));
	UT_ASSERT(!cluster_qvotec_test_epoch_ballot_formation_attested(NULL, 0));
	UT_ASSERT(!cluster_qvotec_test_epoch_ballot_formation_attested(set.fds, 2));
	pgsa_disk_set_close(&set);
}

UT_TEST(test_pgrd_formation_accepts_only_bounded_nonlinux_development_disks)
{
	PgsaDiskSet set;
	int i;

	if (!pgsa_disk_set_open(&set)) {
		UT_ASSERT(false);
		return;
	}
	for (i = 0; i < PGSA_TEST_DISKS; i++)
		UT_ASSERT_EQ(ftruncate(
			set.fds[i], CLUSTER_UNDO_ROOT_DESCRIPTOR_FILE_BYTES_MIN), 0);
#ifndef __linux__
	UT_ASSERT(cluster_qvotec_test_undo_root_descriptor_formation_attested(
		set.fds, PGSA_TEST_DISKS));
#else
	UT_ASSERT(!cluster_qvotec_test_undo_root_descriptor_formation_attested(
		set.fds, PGSA_TEST_DISKS));
#endif
	UT_ASSERT_EQ(ftruncate(
		set.fds[2], CLUSTER_UNDO_ROOT_DESCRIPTOR_FILE_BYTES_MIN - 1), 0);
	UT_ASSERT(!cluster_qvotec_test_undo_root_descriptor_formation_attested(
		set.fds, PGSA_TEST_DISKS));
	UT_ASSERT_EQ(ftruncate(
		set.fds[2], CLUSTER_UNDO_ROOT_DESCRIPTOR_FILE_BYTES_MIN), 0);
	UT_ASSERT(!cluster_qvotec_test_undo_root_descriptor_formation_attested(
		NULL, 0));
	UT_ASSERT(!cluster_qvotec_test_undo_root_descriptor_formation_attested(
		set.fds, 2));
	pgsa_disk_set_close(&set);
}

static char *
pgsa_read_source(const char *path)
{
	FILE *fp = fopen(path, "rb");
	char *contents;
	long length;

	if (fp == NULL)
		return NULL;
	if (fseek(fp, 0, SEEK_END) != 0) {
		fclose(fp);
		return NULL;
	}
	length = ftell(fp);
	if (length < 0 || fseek(fp, 0, SEEK_SET) != 0) {
		fclose(fp);
		return NULL;
	}
	contents = malloc((size_t)length + 1);
	if (contents == NULL) {
		fclose(fp);
		return NULL;
	}
	if (fread(contents, 1, (size_t)length, fp) != (size_t)length) {
		free(contents);
		fclose(fp);
		return NULL;
	}
	contents[length] = '\0';
	fclose(fp);
	return contents;
}

static int
pgsa_count_substring(const char *source, const char *needle)
{
	int count = 0;
	const char *cursor = source;

	while (cursor != NULL && (cursor = strstr(cursor, needle)) != NULL) {
		count++;
		cursor += strlen(needle);
	}
	return count;
}

static bool
pgsa_lmon_tick_order_is_exact(const char *source)
{
	const char *cursor = source;
	int count = 0;

	while ((cursor = strstr(cursor, "cluster_reconfig_lmon_tick();")) != NULL) {
		const char *semantic = strstr(cursor, "cluster_semantic_activation_lmon_tick();");
		const char *recovery = strstr(cursor, "cluster_grd_recovery_lmon_tick();");

		if (semantic == NULL || (recovery != NULL && semantic > recovery))
			return false;
		count++;
		cursor += strlen("cluster_reconfig_lmon_tick();");
	}
	return count == 2;
}

UT_TEST(test_pgsa_source_graph_and_test_linkage_are_exact)
{
	char *qvotec = pgsa_read_source(QVOTEC_SOURCE_PATH);
	char *lmon = pgsa_read_source(LMON_SOURCE_PATH);
	char *shmem = pgsa_read_source(SHMEM_SOURCE_PATH);
	char *semantic = pgsa_read_source(SEMANTIC_SOURCE_PATH);
	char *header = pgsa_read_source(SEMANTIC_HEADER_PATH);
	char *makefile = pgsa_read_source(CLUSTER_MAKEFILE_PATH);
	const char *submit;
	const char *submit_end;

	UT_ASSERT_NOT_NULL(qvotec);
	UT_ASSERT_NOT_NULL(lmon);
	UT_ASSERT_NOT_NULL(shmem);
	UT_ASSERT_NOT_NULL(semantic);
	UT_ASSERT_NOT_NULL(header);
	UT_ASSERT_NOT_NULL(makefile);
	if (qvotec != NULL) {
		UT_ASSERT_NOT_NULL(strstr(qvotec, "qvotec_semantic_activation_record_cas_write_fds"));
		UT_ASSERT_NOT_NULL(strstr(qvotec, "cluster_semantic_activation_record_cas_write"));
		UT_ASSERT_NOT_NULL(strstr(qvotec, "CLUSTER_QVOTEC_PGSA_UNIT_TEST"));
		UT_ASSERT_NOT_NULL(
			strstr(qvotec, "cluster_qvotec_test_semantic_activation_record_cas_write"));
		UT_ASSERT_NOT_NULL(
			strstr(qvotec, "cluster_semantic_activation_qvotec_poll_record_cas"));
		UT_ASSERT_NOT_NULL(
			strstr(qvotec, "cluster_semantic_activation_qvotec_complete_record_cas"));
		UT_ASSERT_NULL(strstr(qvotec, "cluster_qvotec_set_semantic_activation_fds"));
	}
	if (lmon != NULL) {
		UT_ASSERT_EQ(pgsa_count_substring(lmon, "cluster_semantic_activation_lmon_tick();"), 2);
		UT_ASSERT(pgsa_lmon_tick_order_is_exact(lmon));
	}
	if (shmem != NULL) {
		UT_ASSERT_NOT_NULL(strstr(shmem, "pgrac cluster semantic activation"));
		UT_ASSERT_NOT_NULL(strstr(shmem, ".owner_subsys = \"semantic_activation\""));
		UT_ASSERT_NOT_NULL(strstr(shmem, "cluster_semantic_activation_shmem_size"));
		UT_ASSERT_NOT_NULL(strstr(shmem, "cluster_semantic_activation_shmem_init"));
	}
	if (semantic != NULL) {
		UT_ASSERT_NOT_NULL(strstr(semantic, "semantic_activation_record_cas_mailbox_submit"));
		UT_ASSERT_NOT_NULL(
			strstr(semantic, "semantic_activation_record_cas_mailbox_poll_completion"));
		UT_ASSERT_NOT_NULL(strstr(semantic, "pg_write_barrier();"));
		UT_ASSERT_NOT_NULL(strstr(semantic, "pg_read_barrier();"));
		submit = strstr(semantic, "cluster_semantic_activation_submit(");
		submit_end = submit == NULL
					 ? NULL
					 : strstr(submit, "cluster_semantic_activation_r4_descriptor(");
		UT_ASSERT_NOT_NULL(submit);
		UT_ASSERT_NOT_NULL(submit_end);
		if (submit != NULL && submit_end != NULL) {
			const char *publication
				= strstr(submit, "semantic_activation_record_cas_mailbox_submit(");

			UT_ASSERT(publication == NULL || publication >= submit_end);
		}
	}
	if (header != NULL)
		UT_ASSERT_NULL(strstr(header, "cluster_qvotec_test_"));
	if (makefile != NULL)
		UT_ASSERT_NOT_NULL(strstr(makefile, "cluster_semantic_activation.o"));
	free(qvotec);
	free(lmon);
	free(shmem);
	free(semantic);
	free(header);
	free(makefile);
}


int
main(void)
{
	UT_PLAN(58);
	UT_RUN(test_voting_slot_size_512);
	UT_RUN(test_voting_slot_field_offsets);
	UT_RUN(test_qvotec_preserves_replacement_request_per_disk_fail_closed);
	UT_RUN(test_qvotec_shmem_and_mailbox_layout);
	UT_RUN(test_qvotec_mailbox_reset_discards_volatile_handoff);
	UT_RUN(test_qvotec_mailbox_recover_head_is_even_stable_and_single_outstanding);
	UT_RUN(test_qvotec_mailbox_actor_completion_round_trip);
	UT_RUN(test_qvotec_mailbox_rejects_invalid_and_holds_on_sequence_overflow);
	UT_RUN(test_qvotec_mailbox_terminal_hold_completion);
	UT_RUN(test_qvotec_accessors_null_safe_pre_init);
	UT_RUN(test_in_quorum_missing_shmem_diagnostic_is_once_and_still_false);
	UT_RUN(test_qvotec_accessors_post_init);
	UT_RUN(test_in_quorum_diagnostics_preserve_exact_state_and_lease_decisions);
	UT_RUN(test_passive_quorum_observation_neither_renews_nor_logs);
	UT_RUN(test_quorum_lease_six_polls_preserves_exact_expiry_and_fail_closed);
	UT_RUN(test_in_quorum_pre_shmem_init_false);
	UT_RUN(test_in_quorum_initializing_state_false);
	UT_RUN(test_in_quorum_frozen_flag_overrides_to_false);
	UT_RUN(test_freeze_thaw_round_trip);
	UT_RUN(test_qvotec_main_symbol_link_resolves);
	UT_RUN(test_qvotec_poll_cadence_subtracts_cycle_work);
	UT_RUN(test_qvotec_poll_pre_injection_only_target_and_once);
	UT_RUN(test_qvotec_status_enum_values);
	UT_RUN(test_quorum_state_enum_values);
	UT_RUN(test_voting_disk_io_state_enum_values);
	UT_RUN(test_collision_state_enum_values);
	UT_RUN(test_pgrd_initial_shared_eof_provisions_exact_majority);
	UT_RUN(test_pgrd_exact_partial_retry_is_idempotent);
	UT_RUN(test_pgrd_reachable_conflict_holds_without_mutation);
	UT_RUN(test_pgrd_short_read_holds_despite_clean_majority);
	UT_RUN(test_pgrd_postwrite_one_of_three_exact_does_not_commit);
	UT_RUN(test_pgrd_local_node_127_uses_last_frozen_slot);
	UT_RUN(test_pgrd_majority_read_requires_two_exact_images);
	UT_RUN(test_pgrd_majority_read_short_member_holds);
	UT_RUN(test_pgrd_majority_read_same_incarnation_conflict_holds);
	UT_RUN(test_pgsa_01_expected_majority_plus_stale_commits_desired);
	UT_RUN(test_pgsa_02_generation_mismatch_is_conflict_without_mutation);
	UT_RUN(test_pgsa_03_source_mismatch_is_conflict_without_mutation);
	UT_RUN(test_pgsa_04_commit_to_open_requires_explicit_prior_source);
	UT_RUN(test_pgsa_04a_open_to_next_prepare_uses_open_target_as_current_source);
	UT_RUN(test_pgsa_04b_desired_source_mismatch_is_bad_state_before_io);
	UT_RUN(test_pgsa_04c_rollback_complete_projects_target_as_current_source);
	UT_RUN(test_pgsa_04d_unknown_current_phase_holds_without_mutation);
	UT_RUN(test_pgsa_05_lost_completion_replay_is_idempotent_ok);
	UT_RUN(test_pgsa_06_split_or_no_disks_holds_without_mutation);
	UT_RUN(test_pgsa_07_postwrite_one_of_three_desired_holds);
	UT_RUN(test_pgsa_08_clean_eof_zero_pair_accepts_generation_one);
	UT_RUN(test_pgsa_09_invalid_desired_and_overflow_are_bad_state_no_mutation);
	UT_RUN(test_pgsa_10_read_selects_exact_majority_and_reports_conflict);
	UT_RUN(test_jcmk_v3_write_tally_cannot_ack_without_exact_readback);
	UT_RUN(test_jcmk_v3_exact_readback_majority_acks);
	UT_RUN(test_jcmk_v3_split_readback_cannot_form_false_majority);
	UT_RUN(test_jcmk_v3_verify_reads_configured_majority_without_writes);
	UT_RUN(test_epoch_ballot_recover_head_requires_exact_settled_majority);
	UT_RUN(test_epoch_ballot_phase1_preserves_history_and_observed_promise_floor);
	UT_RUN(test_epoch_ballot_formation_rejects_fixture_disks);
	UT_RUN(test_pgrd_formation_accepts_only_bounded_nonlinux_development_disks);
	UT_RUN(test_pgsa_source_graph_and_test_linkage_are_exact);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
