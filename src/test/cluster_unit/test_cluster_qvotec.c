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
 *	    T-2 ClusterQvotecShmem byte layout — size == 128 + per-field
 *	        sanity (state @0 / quorum_state @4 / lease_expire_at_us
 *	        offset within first cache line)
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

#include <signal.h>
#include <stddef.h>

#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_reconfig.h"	 /* ReconfigEvent for spec-4.12b D2 stub */
#include "cluster/cluster_write_fence.h" /* ClusterFenceMarker for D2/D4 stubs */

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


/* ============================================================
 * Stubs — link cluster_qvotec.o standalone.
 * ============================================================ */

bool IsUnderPostmaster = false;
volatile sig_atomic_t ConfigReloadPending = false;
volatile sig_atomic_t ShutdownRequestPending = false;
int MyProcPid = 0;
int cluster_node_id = 0;

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

bool
errstart(int e pg_attribute_unused(), const char *d pg_attribute_unused())
{
	return false;
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

#include "storage/shmem.h"
/* ShmemInitStruct stub: hand back a writable buffer for shmem_init().
 * ClusterQvotecShmem is 128 byte;buffer at 256 byte for headroom. */
static char shmem_storage[256] __attribute__((aligned(64)));
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
void
pg_usleep(long microsec pg_attribute_unused())
{}

#include "cluster/cluster_shmem.h"
void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}

#include "cluster/cluster_elog.h"
void
cluster_elog_init(void)
{}

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
bool cluster_reconfig_join_qvotec_poll_pending(int32 *out_target_node, void *out_slot512);
bool
cluster_reconfig_join_qvotec_poll_pending(int32 *out_target_node,
										  void *out_slot512 pg_attribute_unused())
{
	if (out_target_node != NULL)
		*out_target_node = -1;
	return false;
}
void cluster_reconfig_join_qvotec_complete(bool acked);
void
cluster_reconfig_join_qvotec_complete(bool acked pg_attribute_unused())
{}
void cluster_reconfig_publish_join_qvotec_latch(struct Latch *latch);
void
cluster_reconfig_publish_join_qvotec_latch(struct Latch *latch pg_attribute_unused())
{}
void cluster_membership_seed_last_admitted_from_voting_disk(const int *fds, int n_disks);
void
cluster_membership_seed_last_admitted_from_voting_disk(const int *fds pg_attribute_unused(),
													   int n_disks pg_attribute_unused())
{}
#include "cluster/cluster_membership.h" /* ClusterJoinCommitMarker (D5 self-admit) */
bool cluster_join_marker_is_committed_basis(const ClusterJoinCommitMarker *m, int32 expected_node);
bool
cluster_join_marker_is_committed_basis(const ClusterJoinCommitMarker *m pg_attribute_unused(),
									   int32 expected_node pg_attribute_unused())
{
	return false;
}
void cluster_reconfig_note_self_admitted(uint64 admitted_epoch);
void
cluster_reconfig_note_self_admitted(uint64 admitted_epoch pg_attribute_unused())
{}
/* Hardening v1.1: self-admit now groups by commit identity (HF-3) and gates on
 * the publish-proof (HF-1).  The is_committed_basis stub returns false so the
 * collection loop is empty and neither runs, but the linker needs the symbols. */
bool cluster_join_marker_same_commit(const ClusterJoinCommitMarker *a,
									 const ClusterJoinCommitMarker *b);
bool
cluster_join_marker_same_commit(const ClusterJoinCommitMarker *a pg_attribute_unused(),
								const ClusterJoinCommitMarker *b pg_attribute_unused())
{
	return false;
}
bool cluster_reconfig_join_publish_proven(uint64 admitted_epoch);
bool
cluster_reconfig_join_publish_proven(uint64 admitted_epoch pg_attribute_unused())
{
	return false;
}
ClusterVotingDiskIoState cluster_voting_disk_read_join_slot(int fd, uint32 node_id,
															void *out_slot512);
ClusterVotingDiskIoState
cluster_voting_disk_read_join_slot(int fd pg_attribute_unused(),
								   uint32 node_id pg_attribute_unused(),
								   void *out_slot512 pg_attribute_unused())
{
	return CLUSTER_VOTING_DISK_IO_FAILED;
}
uint64 cluster_epoch_get_current(void);
uint64
cluster_epoch_get_current(void)
{
	return 0;
}
void
cluster_voting_disk_io_install_timeout_handler(void)
{}
void
cluster_voting_disk_io_set_timeout_ms(int timeout_ms pg_attribute_unused())
{}

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


/* ============================================================
 * T-2: ClusterQvotecShmem byte layout — size 128 (cache-line × 2).
 *
 *	Public test cannot reach private struct sizeof, so verify
 *	indirectly via cluster_qvotec_shmem_size().
 * ============================================================ */

UT_TEST(test_qvotec_shmem_size_128)
{
	/* ClusterQvotecShmem is private to cluster_qvotec.c;
	 * cluster_qvotec_shmem_size() returns sizeof(ClusterQvotecShmem) by
	 * contract.  v0.2 amend per Q4 — 128 byte (2 cache lines). */
	UT_ASSERT_EQ(cluster_qvotec_shmem_size(), 128);
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
	UT_ASSERT_STR_EQ(cluster_qvotec_get_collision_state_name(), "none");
}


/* ============================================================
 * T-4: cluster_qvotec_in_quorum() lease-aware semantics (Q4 v0.2).
 *
 *	Pre-init / quorum_state != OK / lease expired → all return false.
 * ============================================================ */

UT_TEST(test_in_quorum_pre_shmem_init_false)
{
	/* Reset shmem stub */
	shmem_init_done = false;

	UT_ASSERT(!(cluster_qvotec_in_quorum()));
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


int
main(void)
{
	UT_PLAN(14);
	UT_RUN(test_voting_slot_size_512);
	UT_RUN(test_voting_slot_field_offsets);
	UT_RUN(test_qvotec_shmem_size_128);
	UT_RUN(test_qvotec_accessors_null_safe_pre_init);
	UT_RUN(test_qvotec_accessors_post_init);
	UT_RUN(test_in_quorum_pre_shmem_init_false);
	UT_RUN(test_in_quorum_initializing_state_false);
	UT_RUN(test_in_quorum_frozen_flag_overrides_to_false);
	UT_RUN(test_freeze_thaw_round_trip);
	UT_RUN(test_qvotec_main_symbol_link_resolves);
	UT_RUN(test_qvotec_status_enum_values);
	UT_RUN(test_quorum_state_enum_values);
	UT_RUN(test_voting_disk_io_state_enum_values);
	UT_RUN(test_collision_state_enum_values);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
