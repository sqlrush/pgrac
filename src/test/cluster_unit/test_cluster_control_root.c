/*-------------------------------------------------------------------------
 *
 * test_cluster_control_root.c
 *	  RF-ROOT P1 tests for the survivor-readable control-root carrier.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog.h"
#include "catalog/catversion.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_cf_authority.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_shared_config.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_cf_stats.h"
#include "cluster/cluster_cf_storage.h"
#include "cluster/cluster_control_root.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_recovery_plan.h"
#include "cluster/cluster_startup_phase.h"
#include "cluster/cluster_stats.h"
#include "cluster/cluster_wal_retention.h"
#include "cluster/cluster_wal_state.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/storage/cluster_shared_fs.h"
#include "common/cryptohash.h"
#include "common/sha2.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"

#include "../../backend/cluster/cluster_control_root_private.h"
#include "../../backend/cluster/cluster_control_bootstrap_private.h"
#include "../../backend/cluster/cluster_recovery_anchor_private.h"

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
#include "postmaster/interrupt.h"
#include "cluster/cluster_ir.h"
#include "cluster/cluster_external_fence.h"
#include "cluster/cluster_thread_recovery.h"
#include "cluster/cluster_thread_recovery_authority.h"
#include "cluster/cluster_write_fence.h"
#include "cluster/cluster_grd.h"

/* backend global provided by xlog.c in a real server; the cluster_unit
 * fixture uses the default 16MiB segment size (segment 1 covers
 * [0x1000000, 0x2000000) — the build_source_wal_state fixture's
 * checkpoint LSN 0x1000000 therefore lives in segment 1). */
int wal_segment_size = XLOG_BLCKSZ * 2048;


UT_DEFINE_GLOBALS();

#define TEST_SYSID UINT64_C(0x0123456789abcdef)

char *cluster_shared_data_dir = NULL;
char *cluster_wal_threads_dir = NULL;
char *DataDir = NULL;
int cluster_node_id = 0;
bool enableFsync = true;
bool cluster_enabled = true;
bool cluster_controlfile_shared_authority = true;
bool cluster_shared_config = false;
ResourceOwner CurrentResourceOwner = (ResourceOwner)(uintptr_t)1;
BackgroundWorker *MyBgworkerEntry;

/* PGRAC: facts, not an authorization stub; real publisher performs checks.
 * Author: SqlRush <sqlrush@gmail.com>
 */
AuxProcType MyAuxProcType = NotAnAuxProcess;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static bool test_checkpoint_mode;
static uint64 test_self_incarnation;
static uint64 test_epoch;
BackendType MyBackendType = B_INVALID;
volatile uint32 CritSectionCount;
static uint64 test_cf_cookie, test_cf_completed_cookie;
static uint64 test_control_generation = 1;
static bool test_cf_release_cut_change;
static unsigned test_epoch_reads, test_change_epoch_read;
static unsigned test_throw_epoch_read;
static bool test_capture_error_level;
static int test_last_error_level;
static bool test_serving, test_fence, test_prebump, test_wal_validated;
static ClusterMembershipState test_member_state;
static XLogRecPtr test_flush;
static XLogRecPtr test_insert;
volatile sig_atomic_t ShutdownRequestPending;
volatile sig_atomic_t InterruptPending;
static TimeLineID test_flush_tli;
static void (*test_checkpoint_x_hook)(void);
static void (*test_checkpoint_published_hook)(void);
static void (*test_startup_sync_hook)(void);
static unsigned test_startup_prefix_syncs;
static void (*test_stop_share_hook)(void);
static unsigned test_stop_share_call;
static bool test_fence_after_primary, test_release_after_primary;
static bool test_checkpoint_outer_cf;
static bool test_projection_sync_fault, test_projection_observe;
static unsigned test_projection_syncs;
static LOCKMODE test_actual_cf;
static unsigned test_history_sync_count, test_history_fail_sync;
static bool test_throw_root_read;
static bool test_close_owned;
static bool test_reserve_mode, test_reserve_provider, test_reserve_quorum;
static ClusterStartupExitResult test_reserve_evidence;
static ClusterFormationSnapshotV1 test_reserve_formation;
static ClusterStartupExitCut test_reserve_cut;
static int test_reserve_fault;
static bool test_startup_bound;
static ClusterWalStartupImage test_startup_operation;

bool
cluster_wal_durable_startup_matches(const ClusterControlRootIdentity *self,
									const uint8 operation_uuid[16], XLogRecPtr first_segment)
{
	return test_startup_bound && MyBackendType == B_STARTUP
		   && cluster_control_root_identity_equal(self, &test_startup_operation.claim.identity)
		   && memcmp(operation_uuid, test_startup_operation.operation_uuid, 16) == 0
		   && first_segment == test_startup_operation.first_segment_lsn;
}

/* Explicit distributed-owner boundaries. Actual root, configuration, claim,
 * anchor, native WAL and PGWG persistence below remain production code. */
bool
cluster_external_fence_runtime_active(void)
{
	return test_reserve_mode && test_reserve_provider;
}
bool
cluster_qvotec_in_quorum(void)
{
	return test_reserve_mode && test_reserve_quorum;
}
bool
cluster_write_fence_enforcing(void)
{
	return test_reserve_mode && test_fence;
}
ClusterFenceAuthorityReadResult
cluster_write_fence_read_durable_authority(ClusterFenceAuthorityProof *out)
{
	memset(out, 0, sizeof(*out));
	if (!test_reserve_mode || !test_fence)
		return CLUSTER_FENCE_AUTHORITY_IO_UNAVAILABLE;
	out->marker.fence_epoch = test_epoch;
	if (test_reserve_fault == 1)
		out->marker.fence_epoch++;
	if (test_reserve_fault == 2)
		out->marker.fenced_dead_bitmap[0] = 8;
	return CLUSTER_FENCE_AUTHORITY_OK;
}
bool
cluster_reconfig_capture_formation_snapshot_v1(uint16 thread, ClusterFormationSnapshotV1 *out)
{
	if (!test_reserve_mode || thread != cluster_node_id + 1)
		return false;
	*out = test_reserve_formation;
	return true;
}
ClusterStartupExitResult
cluster_startup_exit_request(const ClusterStartupExitCut *cut, uint8 digest[32])
{
	memset(digest, 0, 32);
	if (!test_reserve_mode || memcmp(cut, &test_reserve_cut, sizeof(*cut)) != 0)
		return CLUSTER_STARTUP_EXIT_UNAVAILABLE;
	if (test_reserve_evidence == CLUSTER_STARTUP_EXIT_READY)
		memset(digest, 0x79, 32);
	return test_reserve_evidence;
}

bool
cluster_normal_stop_durable_close_owned(const ClusterPhase1FullStopPlan *plan)
{
	return test_close_owned && plan != NULL;
}

/* PGRAC: root-publisher tests inject formation/provider owners at their API
 * boundary. Their real implementations have separate C suites. Physical
 * v2 root/config/claim/anchor/WAL I/O is never stubbed here.
 * Author: SqlRush <sqlrush@gmail.com> */
static bool test_failure_formation, test_failure_needs, test_failure_admissions;
static PgracExternalFenceNeedV1 test_failure_need;

bool
cluster_recovery_duty_digest_for_claim(const ClusterRecoveryDutyKey *key, bool v2,
									   ClusterRecoveryDutyDigest *out)
{
	/* Exact-key SHA encoding is tested in test_cluster_recovery_duty. */
	if (!cluster_recovery_duty_key_valid_for_claim(key, v2))
		return false;
	memset(out, 0, sizeof(*out));
	memcpy(out->bytes, &key->origin_owner_incarnation, 8);
	memcpy(out->bytes + 8, &key->root_lineage_seq, 8);
	memcpy(out->bytes + 16, &key->thread_claim_created_at, 8);
	memcpy(out->bytes + 24, &key->thread_claim_crc32c, 4);
	return true;
}

ClusterFormationWitnessResult
cluster_formation_witness_revalidate_nowait(const ClusterFormationWitnessV1 *witness)
{
	return test_failure_formation && witness == (const ClusterFormationWitnessV1 *)(uintptr_t)1
			   ? CLUSTER_FORMATION_WITNESS_READY
			   : CLUSTER_FORMATION_WITNESS_UNSTABLE;
}

bool
cluster_external_fence_need_set_revalidate_nowait(const PgracExternalFenceNeedSetV1 *needs,
												  const ClusterFormationWitnessV1 *formation,
												  PgracExternalFenceDenyReason *reason)
{
	*reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	return test_failure_needs && needs == (const PgracExternalFenceNeedSetV1 *)(uintptr_t)2
		   && cluster_formation_witness_revalidate_nowait(formation)
				  == CLUSTER_FORMATION_WITNESS_READY;
}

bool
cluster_external_fence_revalidate_set_nowait(const PgracExternalFenceAdmissionSetV1 *admissions,
											 const PgracExternalFenceNeedSetV1 *needs,
											 const ClusterFormationWitnessV1 *formation,
											 PgracExternalFenceDenyReason *reason)
{
	return test_failure_admissions
		   && admissions == (const PgracExternalFenceAdmissionSetV1 *)(uintptr_t)3
		   && cluster_external_fence_need_set_revalidate_nowait(needs, formation, reason);
}

uint32
cluster_external_fence_need_set_count(const PgracExternalFenceNeedSetV1 *needs)
{
	return needs == (const PgracExternalFenceNeedSetV1 *)(uintptr_t)2 ? 1 : 0;
}

const PgracExternalFenceNeedV1 *
cluster_external_fence_need_set_at(const PgracExternalFenceNeedSetV1 *needs, uint32 index)
{
	return cluster_external_fence_need_set_count(needs) == 1 && index == 0 ? &test_failure_need
																		   : NULL;
}

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

void
ProcessInterrupts(void)
{
	InterruptPending = false;
	pg_re_throw();
}

uint64
cluster_epoch_get_current(void)
{
	if (!test_checkpoint_mode)
		abort();
	if (++test_epoch_reads == test_throw_epoch_read)
		pg_re_throw();
	if (test_epoch_reads == test_change_epoch_read)
		++test_epoch;
	return test_epoch;
}

bool
cluster_serving_ready_is_current(void)
{
	if (!test_checkpoint_mode)
		abort();
	return test_serving;
}

bool
cluster_reconfig_has_pending_prebump_stage(void)
{
	if (!test_checkpoint_mode)
		abort();
	return test_prebump;
}

XLogRecPtr
GetFlushRecPtr(TimeLineID *tli)
{
	if (!test_checkpoint_mode)
		abort();
	if (tli != NULL)
		*tli = test_flush_tli;
	return test_flush;
}

XLogRecPtr
GetXLogInsertRecPtr(void)
{
	if (!test_checkpoint_mode)
		abort();
	/* This API is the next record START, not the reserved exclusive END.
	 * The production sampler/converters are tested by xlog_insert_end. */
	if (test_insert % XLOG_BLCKSZ == 0)
		return test_insert
			   + (test_insert % wal_segment_size == 0 ? SizeOfXLogLongPHD : SizeOfXLogShortPHD);
	return test_insert;
}

XLogRecPtr
GetXLogInsertEndRecPtr(void)
{
	if (!test_checkpoint_mode)
		abort();
	return test_insert;
}

/* PGRAC: linked legacy anchor writer dependencies must not be used by this
 * read-only v2 integration. Abort if a new path accidentally calls them.
 * Author: SqlRush <sqlrush@gmail.com>
 */
ClusterMembershipState
cluster_membership_get_state(int32 node)
{
	(void)node;
	if (test_checkpoint_mode)
		return test_member_state;
	abort();
}
ClusterStartupPhase
cluster_current_phase(void)
{
	abort();
}
TimestampTz
cluster_phase_started_at(ClusterStartupPhase phase)
{
	(void)phase;
	abort();
}
ClusterStatsStatus
cluster_stats_status(void)
{
	abort();
}
TimestampTz
cluster_stats_spawned_at(void)
{
	abort();
}
uint64
cluster_qvotec_get_self_incarnation(void)
{
	if (test_checkpoint_mode)
		return test_self_incarnation;
	abort();
}
uint16
cluster_wal_thread_dump_thread_id(void)
{
	abort();
}
bool
cluster_wal_thread_dir_configured(void)
{
	abort();
}
bool
cluster_wal_thread_dir_validated(void)
{
	if (test_checkpoint_mode)
		return test_wal_validated;
	abort();
}
bool
cluster_wal_state_registry_ready(void)
{
	abort();
}
ClusterWalSlotVerdict
cluster_wal_state_read_slot(uint16 thread, ClusterWalStateSlot *slot)
{
	(void)thread;
	(void)slot;
	abort();
}
bool
cluster_cf_exactly_one_declared_node(void)
{
	abort();
}
bool
cluster_cf_held(LOCKMODE mode)
{
	(void)mode;
	if (MyBackendType == B_LMON || MyBackendType == B_LMS)
		return test_actual_cf == mode;
	if (test_checkpoint_mode)
		return test_checkpoint_outer_cf;
	abort();
}
bool
cluster_cf_owner_eor_local_active(void)
{
	abort();
}
bool
cluster_cf_held_is_usable(LOCKMODE mode pg_attribute_unused())
{
	/* Native clean-seed publication is outside this root fixture. */
	abort();
}
bool
cluster_write_fence_allowed(void)
{
	if (test_checkpoint_mode)
		return test_fence;
	abort();
}
void
cluster_write_fence_reject_if_fenced(const char *op)
{
	(void)op;
	abort();
}

static char test_root[MAXPGPATH];
static char test_wal_root[MAXPGPATH];
static uint64 test_system_identifier = TEST_SYSID;
static ControlFileData *test_sysid_control;
static char test_storage_uuid_text[33] = "00112233445566778899aabbccddeeff";
static ClusterCfContractState test_contract = CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED;
static int test_node_count = 4;
static bool test_local_probe = true;
static bool test_cf_grant = true;
static bool test_cf_clusterwide = true;
static LOCKMODE test_cf_mode = NoLock;
static bool test_cf_release_confirmed = true;
static int test_cf_lock_calls = 0;
static int test_durable_rename_calls = 0;
static bool test_fail_primary_rename = false;
static bool test_fail_after_primary_rename = false;
static bool test_fail_global_sync = false;
static unsigned test_global_syncs;
static bool test_create_authorized = true;
static uint16 test_own_thread = 1;
/* RF-ROOT P9 verification (contract): stub state for the bit22 latch
 * cross-restart restore (cluster_control_root_restore_bit22_latch_if_active
 * links the semantic_activation entry points; the unit harness stands in
 * for the shmem latch with plain scalars). */
static bool test_bit22_latch_active;
static bool test_bit22_latch_apply_ok = true;
static uint64 test_bit22_latch_apply_epoch;
static uint64 test_bit22_latch_apply_generation;
static int test_bit22_latch_apply_calls;
/* RF-ROOT P9 verification: durable-OPEN restore stub state — the harness
 * stands in for the voting-disk majority OPEN(P+2) record. */
static bool test_qvotec_open_present;
static uint64 test_qvotec_open_epoch;
static uint64 test_qvotec_open_generation;
static int test_qvotec_bootstrap_calls;
static bool test_activate_authorized = true;
static bool test_publish_authorized = true;
static ClusterWalPinResult test_walr_begin_result = CLUSTER_WAL_PIN_OK;
static ClusterWalrReleaseResult test_walr_end_result = CLUSTER_WALR_RELEASE_CONFIRMED;
static int test_walr_begin_calls = 0;
static int test_walr_end_calls = 0;
static bool test_walr_sealed_required;
static bool test_walr_sealed_current = true;
static uint16 test_walr_thread = 0;
static int test_order_seq = 0;
static int test_walr_begin_order = 0;
static int test_cf_acquire_order = 0;
static int test_cf_release_order = 0;
static int test_last_rename_order = 0;
static ClusterRecoverySerialAcquireResult test_input_acquire = CLUSTER_RECOVERY_SERIAL_GRANTED;
static bool test_input_current = true, test_input_release = true;
static int test_input_acquires, test_input_releases, test_input_acquire_order,
	test_input_release_order;
static int test_worker_replays, test_worker_pins, test_worker_normal_ir;
static int test_worker_initializer_ir, test_worker_formations;
static bool test_worker_pin_held;
static bool test_worker_pin_release = true;
static ClusterWalRetentionPinThreadRequest test_worker_pin_request;
static ReconfigEvent test_worker_event;
static bool test_control_barrier_ready = true;
static bool test_launch_fixture;
static unsigned test_launch_stamps, test_launch_registered, test_launch_legacy_pins;
static bool test_worker_window_consumer;
static unsigned test_legacy_projection_reads;
static bool thread_recovery_root_projection(uint16 thread, uint64 epoch,
											const ClusterThreadRecoveryAuthorityV1 *authority,
											ClusterControlRootReadToken *token, uint64 *tail,
											uint64 *lower, uint64 *lifecycle, uint32 *tail_tli,
											uint32 *checkpoint_tli);
int cluster_external_fence_acquire_timeout_ms = 5000;
static TimestampTz test_now = INT64_C(1700000000000000);

typedef struct ClusterWalRootPublishGuard ClusterWalRootPublishGuard;

extern ClusterWalPinResult
cluster_wal_retention_root_publish_begin_exact(const ClusterControlRootReadToken *expected_root,
											   bool require_sealed_pin,
											   ClusterWalRootPublishGuard **out_guard);
extern ClusterWalrReleaseResult
cluster_wal_retention_root_publish_end(ClusterWalRootPublishGuard **guard);

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

int
OpenTransientFile(const char *fileName, int fileFlags)
{
	if (test_throw_root_read && test_actual_cf == ShareLock)
		pg_re_throw();
	return open(fileName, fileFlags, 0600);
}

/* linked by xlogreader_fs.o via libpgport_srv.a path.o (make_absolute_path
 * error paths) — the unit harness never raises; plain open() suffices. */
int
BasicOpenFile(const char *file_name, int fileFlags)
{
	return open(file_name, fileFlags, 0);
}

int
errcode(int sqlerrcode pg_attribute_unused())
{
	return 0;
}

/* PGRAC: legacy native-control error/fallback symbols are linked but must
 * never select a fallback in the v2 view tests. Author: SqlRush <sqlrush@gmail.com>
 */
int
errcode_for_file_access(void)
{
	return 0;
}

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	if (test_capture_error_level && elevel >= ERROR) {
		test_last_error_level = elevel;
		pg_re_throw();
	}
	if (elevel >= ERROR)
		abort();
	return false;
}

bool
cluster_cf_bak_checkpoint_recoverable(const ControlFileData *bak pg_attribute_unused())
{
	return false;
}

void
cluster_cf_counter_inc(ClusterCfCounter which pg_attribute_unused())
{}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

bool
errstart_cold(int elevel, const char *domain)
{
	if (test_capture_error_level)
		return errstart(elevel, domain);
	return false;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

int
BasicOpenFilePerm(const char *fileName, int fileFlags, mode_t fileMode)
{
	return open(fileName, fileFlags, fileMode);
}

int
CloseTransientFile(int fd)
{
	return close(fd);
}

int
pg_fsync(int fd)
{
	struct stat st;
	struct stat global_st;
	char global_path[MAXPGPATH];
	snprintf(global_path, sizeof(global_path), "%s/global", test_root);
	if (fstat(fd, &st) == 0 && S_ISDIR(st.st_mode) && stat(global_path, &global_st) == 0
		&& st.st_dev == global_st.st_dev && st.st_ino == global_st.st_ino) {
		++test_global_syncs;
		if (test_fail_global_sync) {
			errno = EIO;
			return -1;
		}
	}
	if (test_startup_sync_hook != NULL)
		test_startup_sync_hook();
	if (fstat(fd, &st) == 0 && S_ISREG(st.st_mode)
		&& st.st_size == CLUSTER_WAL_DURABLE_PREFIX_BYTES)
		test_startup_prefix_syncs++;
	if (test_history_fail_sync != 0 && ++test_history_sync_count == test_history_fail_sync) {
		errno = EIO;
		return -1;
	}
	if (test_projection_observe && fstat(fd, &st) == 0 && S_ISREG(st.st_mode)
		&& st.st_size == PG_CONTROL_FILE_SIZE) {
		/* The native projection write is after the root's durable rename.
		 * Immutable common images are not written by ordinary checkpoint. */
		UT_ASSERT(test_durable_rename_calls > 0);
		UT_ASSERT(test_actual_cf == ExclusiveLock);
		test_projection_syncs++;
		if (test_projection_sync_fault) {
			errno = EIO;
			return -1;
		}
	}
	return fsync(fd);
}

int
durable_rename(const char *oldfile, const char *newfile, int elevel pg_attribute_unused())
{
	int result;
	bool primary
		= strstr(newfile, CLUSTER_CONTROL_ROOT_REL_PATH) != NULL && strstr(newfile, ".bak") == NULL;
	test_durable_rename_calls++;
	test_last_rename_order = ++test_order_seq;
	if (test_fail_primary_rename && strstr(newfile, CLUSTER_CONTROL_ROOT_REL_PATH) != NULL
		&& strstr(newfile, ".bak") == NULL) {
		errno = EIO;
		return -1;
	}
	if (strstr(newfile, CLUSTER_CONTROL_ROOT_REL_PATH) != NULL && strstr(newfile, ".bak") == NULL) {
		if (test_fence_after_primary)
			test_fence = false;
		if (test_release_after_primary)
			test_cf_release_confirmed = false;
	}
	result = rename(oldfile, newfile);
	if (result == 0 && primary && test_checkpoint_published_hook != NULL)
		test_checkpoint_published_hook();
	/* Native durable_rename can fail in its post-rename fsync steps even
	 * though the new primary is already visible to this process. */
	if (result == 0 && primary && test_fail_after_primary_rename) {
		errno = EIO;
		return -1;
	}
	return result;
}

bool
pg_strong_random(void *buf, size_t len)
{
	static uint8 seed = 0x31;
	uint8 *bytes = buf;
	size_t i;

	for (i = 0; i < len; i++)
		bytes[i] = seed++;
	return true;
}

TimestampTz
GetCurrentTimestamp(void)
{
	return ++test_now;
}

static uint64 test_membership_incarnation = UINT64_C(0x1020304050607080);

uint64
cluster_membership_get_last_admitted_incarnation(int32 node_id)
{
	(void)node_id;
	return test_membership_incarnation;
}

uint16
cluster_wal_thread_id(void)
{
	return test_own_thread;
}

bool
cluster_r4_bit22_cutover_active(void)
{
	return test_bit22_latch_active;
}

bool
cluster_r4_bit22_source_writer_enter(void)
{
	return true;
}

void
cluster_r4_bit22_source_writer_leave(void)
{}

bool
cluster_r4_bit22_source_close_begin(uint64 transition_epoch pg_attribute_unused(),
									uint64 prepare_generation pg_attribute_unused())
{
	return true;
}

static bool test_source_close_current_ok;

bool
cluster_r4_bit22_source_close_current(uint64 transition_epoch pg_attribute_unused(),
									  uint64 prepare_generation pg_attribute_unused())
{
	return test_source_close_current_ok;
}

bool
cluster_r4_bit22_cutover_latch_apply(uint64 transition_epoch, uint64 round_generation)
{
	test_bit22_latch_apply_calls++;
	if (!test_bit22_latch_apply_ok)
		return false;
	test_bit22_latch_active = true;
	test_bit22_latch_apply_epoch = transition_epoch;
	test_bit22_latch_apply_generation = round_generation;
	return true;
}

ClusterSemanticActivationResult
cluster_qvotec_bootstrap_read_semantic_activation(
	uint8 selected[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES], bool *implicit_open)
{
	test_qvotec_bootstrap_calls++;
	if (selected != NULL)
		memset(selected, 0, CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES);
	if (implicit_open != NULL)
		*implicit_open = false;
	if (!test_qvotec_open_present)
		return CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD;
	if (selected != NULL)
		selected[0] = 0x5a; /* non-zero: restore decodes via the stub below */
	if (implicit_open != NULL)
		*implicit_open = true;
	return CLUSTER_SEMANTIC_ACTIVATION_OK;
}

static ClusterSemanticActivationRecord test_decoded_open;

bool
cluster_semantic_activation_record_decode(
	const uint8 bytes[512] pg_attribute_unused(),
	ClusterSemanticActivationRecord *record pg_attribute_unused(),
	ClusterSemanticActivationRefusal *refusal pg_attribute_unused())
{
	if (record != NULL)
		*record = test_decoded_open;
	return true;
}

bool
cluster_r4_bit22_cutover_latch_verify(void)
{
	return test_bit22_latch_active;
}

uint64
GetSystemIdentifier(void)
{
	return test_sysid_control != NULL ? test_sysid_control->system_identifier
									  : test_system_identifier;
}

int
cluster_conf_node_count(void)
{
	return test_node_count;
}

void
cluster_shared_fs_get_storage_uuid(char *out, size_t outlen)
{
	strlcpy(out, test_storage_uuid_text, outlen);
}

ClusterCfContractState
cluster_cf_contract_load(const char *pgdata pg_attribute_unused())
{
	return test_contract;
}

bool
cluster_cf_storage_write_allowed(ClusterCfContractState state, bool multi_node)
{
	return !multi_node || state == CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED;
}

bool
cluster_cf_storage_probe_local(void)
{
	return test_local_probe;
}

bool
cluster_cf_lock(LOCKMODE mode pg_attribute_unused())
{
	test_cf_lock_calls++;
	test_cf_acquire_order = ++test_order_seq;
	if (mode == ShareLock && test_stop_share_hook != NULL
		&& test_cf_lock_calls == test_stop_share_call)
		test_stop_share_hook();
	if (mode == ExclusiveLock && test_checkpoint_x_hook != NULL)
		test_checkpoint_x_hook();
	if (test_cf_grant) {
		test_actual_cf = mode;
		test_cf_cookie++;
	}
	return test_cf_grant;
}

bool
cluster_cf_lock_poll(LOCKMODE mode)
{
	return cluster_cf_lock(mode);
}
bool
cluster_cf_acquire_pending(LOCKMODE mode pg_attribute_unused())
{
	return false;
}
uint64
cluster_cf_owner_cookie(LOCKMODE mode)
{
	return test_actual_cf == mode ? test_cf_cookie : 0;
}
bool
cluster_cf_release_completed(LOCKMODE mode pg_attribute_unused(), uint64 cookie)
{
	return cookie != 0 && cookie == test_cf_completed_cookie;
}
void
cluster_cf_retirement_poll(void)
{
	if (test_cf_release_confirmed && test_actual_cf != NoLock) {
		test_cf_completed_cookie = test_cf_cookie;
		test_actual_cf = NoLock;
	}
}
uint64
cluster_grd_redeclare_generation(void)
{
	return test_control_generation;
}
int32
cluster_grd_lookup_master_gen(const ClusterResId *resid pg_attribute_unused(), uint64 *generation)
{
	*generation = test_control_generation;
	return 0;
}

bool
cluster_cf_held_is_clusterwide(LOCKMODE mode)
{
	if (test_reserve_mode)
		return test_cf_grant && test_cf_clusterwide && test_actual_cf == mode;
	return test_cf_grant && test_cf_clusterwide && (test_cf_mode == NoLock || test_cf_mode == mode);
}

ClusterCfReleaseResult
cluster_cf_unlock_confirmed(LOCKMODE mode pg_attribute_unused())
{
	test_cf_release_order = ++test_order_seq;
	if (test_cf_release_cut_change)
		test_control_generation++;
	if (test_cf_release_confirmed) {
		test_actual_cf = NoLock;
		test_cf_completed_cookie = test_cf_cookie;
	}
	return test_cf_release_confirmed ? CLUSTER_CF_RELEASE_CONFIRMED
									 : CLUSTER_CF_RELEASE_UNCONFIRMED;
}

ClusterWalPinResult
cluster_wal_retention_root_publish_begin_exact(const ClusterControlRootReadToken *expected_root,
											   bool require_sealed_pin,
											   ClusterWalRootPublishGuard **out_guard)
{
	/* Match the real WALR precondition; SHMEM-only workers start without it. */
	if (CurrentResourceOwner == NULL)
		return CLUSTER_WAL_PIN_INVALID;
	test_walr_begin_calls++;
	test_walr_sealed_required = require_sealed_pin;
	test_walr_thread = expected_root->origin_thread_id;
	test_walr_begin_order = ++test_order_seq;
	if (test_walr_begin_result != CLUSTER_WAL_PIN_OK)
		return test_walr_begin_result;
	*out_guard = (ClusterWalRootPublishGuard *)(uintptr_t)0x1;
	return CLUSTER_WAL_PIN_OK;
}

ClusterWalrReleaseResult
cluster_wal_retention_root_publish_end(ClusterWalRootPublishGuard **guard)
{
	test_walr_end_calls++;
	++test_order_seq;
	if (test_walr_end_result == CLUSTER_WALR_RELEASE_CONFIRMED)
		*guard = NULL;
	return test_walr_end_result;
}

bool
cluster_wal_retention_root_publish_sealed_current(const ClusterWalRootPublishGuard *guard,
												  const ClusterControlRootReadToken *expected_root)
{
	return guard == (const ClusterWalRootPublishGuard *)(uintptr_t)1 && expected_root != NULL
		   && test_walr_sealed_current;
}

/* PGRAC: inject the distributed IR owner, not the physical root/WAL reader.
 * The production IR implementation is exercised by recovery_serial tests.
 * Author: SqlRush <sqlrush@gmail.com> */
ClusterRecoverySerialAcquireResult
cluster_recovery_serial_acquire(const ClusterRecoverySerialRequest *request,
								ClusterRecoverySerialGuard *guard)
{
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	if (request->mode == CLUSTER_RECOVERY_SERIAL_INPUT_SEAL) {
		test_input_acquires++;
		test_input_acquire_order = ++test_order_seq;
		UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls + 1);
	} else if (request->mode == CLUSTER_RECOVERY_SERIAL_INITIALIZER) {
		test_worker_initializer_ir++;
		UT_ASSERT(test_worker_pin_held);
		UT_ASSERT_EQ(
			memcmp(&request->pending, &test_worker_pin_request.pending, sizeof(request->pending)),
			0);
	} else {
		test_worker_normal_ir++;
		UT_ASSERT_EQ(request->mode, CLUSTER_RECOVERY_SERIAL_ONLINE);
		UT_ASSERT(test_worker_pin_held);
		UT_ASSERT_EQ(test_input_acquires, test_input_releases);
		UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
		UT_ASSERT_EQ(memcmp(&request->expected_root_token, &test_worker_pin_request.root_read,
							sizeof(request->expected_root_token)),
					 0);
	}
	memset(guard, 0, sizeof(*guard));
	if (test_input_acquire == CLUSTER_RECOVERY_SERIAL_GRANTED) {
		guard->held = true;
		guard->mode = request->mode;
		guard->duty = request->duty;
		guard->root_read_token = request->expected_root_token;
		guard->pending = request->pending;
		guard->formation = request->formation;
		guard->fence_need_set = request->fence_need_set;
		guard->fence_admission_set = request->fence_admission_set;
	}
	return test_input_acquire;
}

ClusterRecoverySerialRevalidateResult
cluster_recovery_serial_input_revalidate(ClusterRecoverySerialGuard *guard)
{
	return guard->held && guard->mode == CLUSTER_RECOVERY_SERIAL_INPUT_SEAL && test_input_current
			   ? CLUSTER_RECOVERY_SERIAL_CURRENT
			   : CLUSTER_RECOVERY_SERIAL_CAPABILITY_STALE;
}

ClusterRecoverySerialRevalidateResult
cluster_recovery_serial_initializer_revalidate(ClusterRecoverySerialGuard *guard)
{
	PgracExternalFenceDenyReason reason;
	return guard->held && !guard->release_uncertain
				   && guard->mode == CLUSTER_RECOVERY_SERIAL_INITIALIZER && test_input_current
				   && cluster_formation_witness_revalidate_nowait(guard->formation)
						  == CLUSTER_FORMATION_WITNESS_READY
				   && cluster_external_fence_need_set_revalidate_nowait(guard->fence_need_set,
																		guard->formation, &reason)
				   && cluster_external_fence_revalidate_set_nowait(
					   guard->fence_admission_set, guard->fence_need_set, guard->formation, &reason)
			   ? CLUSTER_RECOVERY_SERIAL_CURRENT
			   : CLUSTER_RECOVERY_SERIAL_CAPABILITY_STALE;
}

ClusterRecoverySerialReleaseResult
cluster_recovery_serial_release(ClusterRecoverySerialGuard *guard)
{
	if (guard->mode == CLUSTER_RECOVERY_SERIAL_INPUT_SEAL)
		test_input_releases++;
	test_input_release_order = ++test_order_seq;
	if (!test_input_release) {
		guard->release_uncertain = true;
		return CLUSTER_RECOVERY_SERIAL_RELEASE_UNCONFIRMED;
	}
	memset(guard, 0, sizeof(*guard));
	return CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED;
}

/* PGRAC: the extracted worker/eligibility bodies below execute unchanged
 * production code against real root/WAL files. Only external process/lock
 * owners and the subsequent DATA replay boundary are replaced here.
 * Author: SqlRush <sqlrush@gmail.com> */
ClusterFormationWitnessResult
cluster_formation_witness_build_wait(uint16 thread, bool opening, int timeout,
									 ClusterFormationWitnessV1 **out)
{
	++test_worker_formations;
	UT_ASSERT_EQ(thread, 1);
	UT_ASSERT(!opening);
	UT_ASSERT_EQ(timeout, 5000);
	*out = (ClusterFormationWitnessV1 *)(uintptr_t)1;
	return cluster_formation_witness_revalidate_nowait(*out);
}

void
cluster_formation_witness_destroy(ClusterFormationWitnessV1 **out)
{
	*out = NULL;
}

PgracExternalFenceNeedSetResult
cluster_external_fence_need_set_build(const ClusterRecoveryDutyKey *duty,
									  const ClusterFormationWitnessV1 *formation,
									  PgracExternalFenceNeedSetV1 **out)
{
	UT_ASSERT_EQ(duty->origin_thread_id, 1);
	UT_ASSERT_EQ(cluster_formation_witness_revalidate_nowait(formation),
				 CLUSTER_FORMATION_WITNESS_READY);
	*out = (PgracExternalFenceNeedSetV1 *)(uintptr_t)2;
	return PGRAC_EXTERNAL_FENCE_NEED_SET_OK;
}

PgracExternalFenceVerdict
cluster_external_fence_admit_set_wait(const PgracExternalFenceNeedSetV1 *needs,
									  const ClusterFormationWitnessV1 *formation, int timeout,
									  PgracExternalFenceAdmissionSetV1 **out)
{
	PgracExternalFenceDenyReason reason;
	UT_ASSERT_EQ(timeout, 5000);
	UT_ASSERT(cluster_external_fence_need_set_revalidate_nowait(needs, formation, &reason));
	*out = (PgracExternalFenceAdmissionSetV1 *)(uintptr_t)3;
	return test_failure_admissions ? PGRAC_EXTERNAL_FENCE_WRITE_EXCLUDED
								   : PGRAC_EXTERNAL_FENCE_UNKNOWN;
}

void
cluster_external_fence_need_set_release(PgracExternalFenceNeedSetV1 **out)
{
	*out = NULL;
}
void
cluster_external_fence_admission_set_release(PgracExternalFenceAdmissionSetV1 **out)
{
	*out = NULL;
}

bool
cluster_thread_recovery_replay_read(uint16 thread, ClusterThreadRecReplayState *state,
									uint64 *epoch)
{
	UT_ASSERT_EQ(thread, 1);
	*state
		= test_launch_fixture ? CLUSTER_THREADREC_REPLAY_IDLE : CLUSTER_THREADREC_REPLAY_REPLAYING;
	*epoch = 123;
	return true;
}

bool
cluster_thread_recovery_replay_mark_replaying(uint16 thread, uint64 epoch)
{
	UT_ASSERT(test_launch_fixture && thread == 1 && epoch == 123);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	test_launch_stamps++;
	return true;
}

bool
cluster_thread_recovery_pin_projection(uint16 thread, uint64 epoch)
{
	UT_ASSERT(thread == 1 && epoch == 123);
	test_launch_legacy_pins++;
	return false; /* A second service-side read is not synchronously ready. */
}

bool
cluster_thread_recovery_projection_current(
	uint16 thread pg_attribute_unused(), uint64 epoch pg_attribute_unused(),
	ClusterControlRootReadToken *token pg_attribute_unused(), uint64 *tail pg_attribute_unused(),
	uint64 *lower pg_attribute_unused(), uint64 *lifecycle pg_attribute_unused(),
	uint32 *tail_tli pg_attribute_unused(), uint32 *checkpoint_tli pg_attribute_unused())
{
	test_legacy_projection_reads++;
	return false; /* No prelaunch projection of the newly sealed input exists. */
}

static bool
register_one_worker(const ClusterThreadRecLaunchEligibility *eligibility,
					BackgroundWorkerHandle **handle)
{
	UT_ASSERT(test_launch_fixture && eligibility->origin_thread == 1);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	test_launch_registered++;
	*handle = (BackgroundWorkerHandle *)(uintptr_t)1;
	return true;
}

uint64
cluster_grd_redeclare_episode_epoch(void)
{
	return 123;
}
void
cluster_reconfig_get_last_event(ReconfigEvent *out)
{
	*out = test_worker_event;
}

/* PGRAC: the GRD binary drives the real protocol FSM. This boundary lets the
 * real launch consumer below exercise canonical root I/O and refuse it before
 * that barrier. It is not a claim of live GES admission. */
bool
cluster_grd_recovery_control_snapshot(uint16 origin_thread,
									  ClusterGrdRecoveryControlSnapshotV1 *out)
{
	memset(out, 0, sizeof(*out));
	if (!test_control_barrier_ready || origin_thread != 1)
		return false;
	out->event_id = test_worker_event.event_id;
	out->episode_epoch = test_worker_event.new_epoch;
	out->dead_bitmap_hash = 1;
	out->redeclare_generation = 1;
	out->master_map_refresh = 1;
	out->routing_generation = 1;
	memcpy(out->dead_bitmap, test_worker_event.dead_bitmap, sizeof(out->dead_bitmap));
	out->survivor_bitmap[15] = 0x80;
	return true;
}
void
cluster_write_fence_note_external_mutation_gate_blocked(void)
{}

ClusterWalPinResult
cluster_wal_retention_pin_acquire(const ClusterWalRetentionPinThreadRequest *request, uint16 count,
								  ClusterWalRetentionPin **out)
{
	test_worker_pins++;
	UT_ASSERT_EQ(count, 1);
	if (request->pending.generation != 0) {
		UT_ASSERT_EQ(request->nintervals, 0);
		UT_ASSERT(request->intervals == NULL);
		UT_ASSERT(cluster_control_pending_token_matches(&request->pending, &request->duty));
	} else {
		UT_ASSERT_EQ(request->nintervals, 1);
		UT_ASSERT(request->intervals[0].end_lsn > request->intervals[0].start_lsn);
		UT_ASSERT((request->root_read.root_flags & CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID) != 0);
	}
	test_worker_pin_request = *request;
	test_worker_pin_held = true;
	*out = (ClusterWalRetentionPin *)(uintptr_t)4;
	return CLUSTER_WAL_PIN_OK;
}

ClusterWalPinResult
cluster_wal_retention_pin_bind_one(ClusterWalRetentionPin *pin, ClusterRecoverySerialGuard *guard)
{
	UT_ASSERT(pin == (ClusterWalRetentionPin *)(uintptr_t)4 && guard->held);
	UT_ASSERT(guard->mode == CLUSTER_RECOVERY_SERIAL_ONLINE
			  || guard->mode == CLUSTER_RECOVERY_SERIAL_INITIALIZER);
	return CLUSTER_WAL_PIN_OK;
}

ClusterWalPinResult
cluster_wal_retention_pin_revalidate(ClusterWalRetentionPin *pin)
{
	return pin == (ClusterWalRetentionPin *)(uintptr_t)4 && test_worker_pin_held
			   ? CLUSTER_WAL_PIN_OK
			   : CLUSTER_WAL_PIN_STALE;
}

ClusterWalrReleaseResult
cluster_wal_retention_pin_release(ClusterWalRetentionPin **pin)
{
	UT_ASSERT(*pin == (ClusterWalRetentionPin *)(uintptr_t)4);
	if (!test_worker_pin_release)
		return CLUSTER_WALR_RELEASE_UNCONFIRMED;
	*pin = NULL;
	test_worker_pin_held = false;
	return CLUSTER_WALR_RELEASE_CONFIRMED;
}

ClusterThreadRecoveryAuthorityResultV1
cluster_thread_recovery_authority_revalidate_nowait_v1(
	const ClusterThreadRecoveryAuthorityV1 *authority)
{
	UT_ASSERT(authority->serial_guard->held && test_worker_pin_held);
	UT_ASSERT_EQ(authority->serial_guard->mode, CLUSTER_RECOVERY_SERIAL_ONLINE);
	return CLUSTER_THREAD_AUTHORITY_OK;
}

ClusterThreadRecResult
cluster_thread_recovery_replay_one(uint16 thread, uint64 epoch,
								   const ClusterThreadRecoveryAuthorityV1 *authority)
{
	test_worker_replays++;
	UT_ASSERT_EQ(thread, 1);
	UT_ASSERT_EQ(epoch, 123);
	UT_ASSERT_EQ(authority->root_snapshot->lifecycle,
				 CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED);
	UT_ASSERT(authority->root_snapshot->validated_tail_lsn_exclusive
			  > authority->root_snapshot->checkpoint_lower_lsn);
	UT_ASSERT_EQ(authority->serial_guard->mode, CLUSTER_RECOVERY_SERIAL_ONLINE);
	if (test_worker_window_consumer) {
		ClusterControlRootReadToken token = { 0 };
		uint64 tail = 0, lower = 0, lifecycle = 0;
		uint32 tail_tli = 0, checkpoint_tli = 0;

		UT_ASSERT(thread_recovery_root_projection(thread, epoch, authority, &token, &tail, &lower,
												  &lifecycle, &tail_tli, &checkpoint_tli));
		UT_ASSERT_EQ(memcmp(&token, authority->root_token, sizeof(token)), 0);
		UT_ASSERT_EQ(tail, authority->root_snapshot->validated_tail_lsn_exclusive);
		UT_ASSERT_EQ(lower, authority->root_snapshot->checkpoint_lower_lsn);
		UT_ASSERT(tail > lower && lower != 0);
		UT_ASSERT(!thread_recovery_root_projection(thread, epoch + 1, authority, &token, &tail,
												   &lower, &lifecycle, &tail_tli, &checkpoint_tli));
	}
	return CLUSTER_THREADREC_DEFERRED; /* No DATA replay is claimed by this fixture. */
}

/* PGRAC: process setup/exit primitives are the boundary; the generated main
 * and run bodies, root publishers and physical files remain production code.
 * Native ResourceOwner destruction is not claimed by this process-local test.
 * Author: SqlRush <sqlrush@gmail.com> */
void
CreateAuxProcessResourceOwner(void)
{
	UT_ASSERT(CurrentResourceOwner == NULL);
	CurrentResourceOwner = (ResourceOwner)(uintptr_t)2;
}

void
cluster_grd_cleanup_on_backend_exit_callback(int code pg_attribute_unused(),
											 Datum arg pg_attribute_unused())
{}

void
before_shmem_exit(pg_on_exit_callback function, Datum arg)
{
	UT_ASSERT(function == cluster_grd_cleanup_on_backend_exit_callback);
	UT_ASSERT_EQ(arg, 0);
}

void
BackgroundWorkerUnblockSignals(void)
{}

ClusterThreadReplayMatchResult
cluster_thread_recovery_replay_transition_if_match(
	uint16 thread, uint64 stamp, ClusterThreadRecReplayState expected,
	ClusterThreadRecReplayState target pg_attribute_unused())
{
	UT_ASSERT_EQ(thread, 1);
	UT_ASSERT_EQ(stamp, 123);
	UT_ASSERT_EQ(expected, CLUSTER_THREADREC_REPLAY_REPLAYING);
	return CLUSTER_THREADREC_MATCH_CHANGED;
}

#include "test_cluster_control_root_worker.inc"

bool
cluster_control_root_create_authority_current_v1(
	const ClusterControlRootMigrationImage *image pg_attribute_unused(),
	const ClusterControlRootMigrationRoundV1 *round pg_attribute_unused())
{
	return test_create_authorized;
}

bool
cluster_control_root_activate_authority_current_v1(
	const ClusterControlRootFileToken *expected_token pg_attribute_unused(),
	const uint8 expected_round_sha256[32] pg_attribute_unused(),
	const ClusterControlRootMigrationRoundV1 *round pg_attribute_unused())
{
	return test_activate_authorized;
}

bool
cluster_control_root_publish_authority_current_v1(
	const ClusterControlRootReadToken *expected_token pg_attribute_unused(),
	const ClusterControlRootPatch *patch pg_attribute_unused(),
	ClusterControlRootPublishReason reason pg_attribute_unused())
{
	return test_publish_authorized;
}

static void
put_u16_le(uint8 *dst, uint16 value)
{
	dst[0] = (uint8)value;
	dst[1] = (uint8)(value >> 8);
}

static void
put_u32_le(uint8 *dst, uint32 value)
{
	dst[0] = (uint8)value;
	dst[1] = (uint8)(value >> 8);
	dst[2] = (uint8)(value >> 16);
	dst[3] = (uint8)(value >> 24);
}

static void
put_u64_le(uint8 *dst, uint64 value)
{
	int i;

	for (i = 0; i < 8; i++) {
		dst[i] = (uint8)value;
		value >>= 8;
	}
}

static uint32
image_crc(const uint8 *bytes, size_t len)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, len);
	FIN_CRC32C(crc);
	return (uint32)crc;
}

static void
sha256_bytes(const uint8 *bytes, size_t len, uint8 out[PG_SHA256_DIGEST_LENGTH])
{
	pg_cryptohash_ctx *ctx = pg_cryptohash_create(PG_SHA256);

	if (ctx == NULL || pg_cryptohash_init(ctx) < 0 || pg_cryptohash_update(ctx, bytes, len) < 0
		|| pg_cryptohash_final(ctx, out, PG_SHA256_DIGEST_LENGTH) < 0)
		abort();
	pg_cryptohash_free(ctx);
}

static void
round_sha256(const ClusterControlRootMigrationRoundV1 *round, uint8 out[PG_SHA256_DIGEST_LENGTH])
{
	uint8 bytes[80];

	memset(bytes, 0, sizeof(bytes));
	memcpy(bytes, "PCRM", 4);
	put_u16_le(bytes + 4, 1);
	put_u16_le(bytes + 6, 80);
	put_u64_le(bytes + 8, round->prepare_generation);
	put_u64_le(bytes + 16, round->transition_epoch);
	put_u64_le(bytes + 24, round->source_feature_bitmap);
	put_u64_le(bytes + 32, round->target_feature_bitmap);
	put_u64_le(bytes + 40, round->admitted_bitmap_low);
	put_u64_le(bytes + 48, round->admitted_bitmap_high);
	put_u64_le(bytes + 56, round->capability_sample_digest);
	put_u64_le(bytes + 64, round->coordinator_incarnation);
	put_u32_le(bytes + 72, round->coordinator_node_id);
	sha256_bytes(bytes, sizeof(bytes), out);
}

static void
path_for(char *dst, size_t dstlen, const char *rel)
{
	snprintf(dst, dstlen, "%s/%s", test_root, rel);
}

static void
wipe_root_files(void)
{
	char path[MAXPGPATH];
	test_reserve_mode = false;

	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	unlink(path);
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_BAK_REL_PATH);
	unlink(path);
	test_contract = CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED;
	test_node_count = 4;
	test_local_probe = true;
	test_cf_grant = true;
	test_cf_clusterwide = true;
	test_cf_mode = NoLock;
	test_cf_release_confirmed = true;
	test_cf_lock_calls = 0;
	test_durable_rename_calls = 0;
	test_fail_primary_rename = false;
	test_fail_after_primary_rename = test_fail_global_sync = false;
	test_global_syncs = 0;
	test_create_authorized = true;
	test_activate_authorized = true;
	test_publish_authorized = true;
	test_walr_begin_result = CLUSTER_WAL_PIN_OK;
	test_walr_end_result = CLUSTER_WALR_RELEASE_CONFIRMED;
	test_walr_begin_calls = 0;
	test_walr_end_calls = 0;
	test_walr_sealed_required = false;
	test_walr_sealed_current = true;
	test_walr_thread = 0;
	test_order_seq = 0;
	test_walr_begin_order = 0;
	test_cf_acquire_order = 0;
	test_cf_release_order = 0;
	test_last_rename_order = 0;
	test_input_acquire = CLUSTER_RECOVERY_SERIAL_GRANTED;
	test_input_current = test_input_release = true;
	test_input_acquires = test_input_releases = test_input_acquire_order = test_input_release_order
		= 0;
	test_worker_replays = test_worker_pins = test_worker_normal_ir = 0;
	test_worker_initializer_ir = test_worker_formations = 0;
	test_worker_pin_held = false;
	CurrentResourceOwner = (ResourceOwner)(uintptr_t)1;
	test_worker_pin_release = true;
	MyBgworkerEntry = NULL;
	test_checkpoint_mode = false;
	test_projection_observe = false;
	cluster_shared_config = false;
	test_checkpoint_x_hook = NULL;
	test_checkpoint_published_hook = NULL;
	test_stop_share_hook = NULL;
	test_stop_share_call = 0;
	test_fence_after_primary = test_release_after_primary = false;
	test_throw_root_read = false;
}

static void
write_all_or_abort(const char *path, const void *buf, size_t len)
{
	const uint8 *bytes = buf;
	size_t done = 0;
	int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);

	if (fd < 0)
		abort();
	while (done < len) {
		ssize_t n = write(fd, bytes + done, len - done);

		if (n <= 0)
			abort();
		done += (size_t)n;
	}
	close(fd);
}

static void
read_all_or_abort(const char *path, void *buf, size_t len)
{
	uint8 *bytes = buf;
	size_t done = 0;
	int fd = open(path, O_RDONLY);

	if (fd < 0)
		abort();
	while (done < len) {
		ssize_t n = read(fd, bytes + done, len - done);

		if (n <= 0)
			abort();
		done += (size_t)n;
	}
	close(fd);
}

/*
 * write_minimal_checkpoint_segment -- RF-ROOT P9 verification (contract): build
 * a minimal real WAL segment for thread 1 (tli 1, seg 1 — the
 * build_source_wal_state fixture's checkpoint_redo lives at 0x1000000,
 * segment offset 0) containing one CheckPoint record, so the migration
 * image scan can extract the checkpoint record CRC.  XLogRecord encoding
 * follows the on-disk format (header + payload + CRC over everything but
 * the xl_crc field).
 */
static void
write_minimal_checkpoint_segment(const char *thread_dir)
{
	char path[MAXPGPATH];
	uint8 page[XLOG_BLCKSZ];
	XLogLongPageHeaderData longhdr;
	XLogRecord rec;
	pg_crc32c crc;
	int off;

	memset(page, 0, sizeof(page));
	memset(&longhdr, 0, sizeof(longhdr));
	/* Segment page 0 must carry the long header (offset==0 forces
	 * XLP_LONG_HEADER in XLogReaderValidatePageHeader).  The reader's
	 * system_identifier is 0 in the unit harness, so xlp_sysid stays 0;
	 * segment size and block size must match the reader's. */
	longhdr.std.xlp_magic = XLOG_PAGE_MAGIC;
	longhdr.std.xlp_info = XLP_LONG_HEADER;
	longhdr.std.xlp_tli = 1;
	longhdr.std.xlp_pageaddr = UINT64_C(0x1000000);
	longhdr.xlp_sysid = UINT64_C(0);
	longhdr.xlp_seg_size = wal_segment_size;
	longhdr.xlp_xlog_blcksz = XLOG_BLCKSZ;
	memcpy(page, &longhdr, sizeof(longhdr));

	off = SizeOfXLogLongPHD + SizeOfXLogRecord;
	/* Payload follows the XLogInsert encoding for a pure main-data
	 * record: XLogRecordDataHeaderShort (0xFF + len) + CheckPoint bytes.
	 * The record reader parses these headers, so zeros alone would be
	 * misread as block ids. */
	page[off] = XLR_BLOCK_ID_DATA_SHORT;
	page[off + 1] = (uint8)sizeof(CheckPoint);
	memset(page + off + 2, 0, sizeof(CheckPoint));

	memset(&rec, 0, sizeof(rec));
	rec.xl_tot_len = SizeOfXLogRecord + 2 + sizeof(CheckPoint);
	rec.xl_xid = 1;
	rec.xl_prev = UINT64_C(0x1000000);
	rec.xl_info = XLOG_CHECKPOINT_SHUTDOWN;
	rec.xl_rmid = RM_XLOG_ID;
	/* ValidXLogRecord order: payload first, then header up to (not
	 * including) xl_crc. */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, page + off, 2 + sizeof(CheckPoint));
	COMP_CRC32C(crc, (uint8 *)&rec, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(crc);
	rec.xl_crc = (uint32)crc;
	memcpy(page + SizeOfXLogLongPHD, &rec, sizeof(rec));

	snprintf(path, sizeof(path), "%s/%s", thread_dir, "000000010000000000000001");
	write_all_or_abort(path, page, sizeof(page));
}

static void
build_source_wal_state(void)
{
	uint8 bytes[CLUSTER_WAL_STATE_FILE_SIZE];
	ClusterWalStateHeader header;
	ClusterWalStateSlot slot;
	ClusterWalThreadClaim claim;
	char path[MAXPGPATH];
	char thread_dir[MAXPGPATH];

	memset(bytes, 0, sizeof(bytes));
	cluster_wal_state_header_fill(&header, INT64_C(1699999999000000));
	memcpy(bytes, &header, sizeof(header));
	cluster_wal_state_slot_fill(&slot, 1, 0, CLUSTER_WAL_SLOT_STATE_STOPPED, 1,
								INT64_C(1699999999000001), INT64_C(1699999999000002),
								UINT64_C(0x1000000), 1);
	slot.checkpoint_redo_lsn = UINT64_C(0x1000000);
	slot.crc = cluster_wal_state_block_crc(&slot);
	memcpy(bytes + CLUSTER_WAL_STATE_SLOT_OFFSET(1), &slot, sizeof(slot));
	snprintf(path, sizeof(path), "%s/%s", test_wal_root, CLUSTER_WAL_STATE_FILENAME);
	write_all_or_abort(path, bytes, sizeof(bytes));
	cluster_wal_thread_claim_fill(&claim, 1, 0, INT64_C(1699999999000001));
	snprintf(thread_dir, sizeof(thread_dir), "%s/thread_1", test_wal_root);
	if (mkdir(thread_dir, 0700) != 0 && errno != EEXIST)
		abort();
	snprintf(path, sizeof(path), "%s/%s", thread_dir, CLUSTER_WAL_THREAD_CLAIM_FILENAME);
	write_all_or_abort(path, &claim, sizeof(claim));
	write_minimal_checkpoint_segment(thread_dir);
}

static void
fill_identity(ClusterControlRootIdentity *identity)
{
	ClusterWalThreadClaim claim;
	int i;

	memset(identity, 0, sizeof(*identity));
	identity->system_identifier = TEST_SYSID;
	for (i = 0; i < 16; i++) {
		identity->storage_uuid[i] = (uint8)(i * 0x11);
		identity->authority_uuid[i] = (uint8)(0xa0 + i);
	}
	identity->authority_uuid[6] = 0x46;
	identity->authority_uuid[8] = 0x8a;
	identity->origin_thread_id = 1;
	identity->origin_node_id = 0;
	cluster_wal_thread_claim_fill(&claim, 1, 0, INT64_C(1699999999000001));
	identity->thread_claim_created_at = claim.created_at;
	identity->thread_claim_crc32c = claim.crc;
	identity->origin_owner_incarnation = UINT64_C(0x1122334455667788);
	identity->root_lineage_seq = 1;
}

static void
build_migration(ClusterControlRootMigrationImage *image, ClusterControlRootMigrationRoundV1 *round)
{
	ClusterControlRootSnapshot *record;

	memset(image, 0, sizeof(*image));
	image->system_identifier = TEST_SYSID;
	fill_identity(&image->records[0].identity);
	memcpy(image->storage_uuid, image->records[0].identity.storage_uuid, 16);
	memcpy(image->authority_uuid, image->records[0].identity.authority_uuid, 16);
	image->created_at_usec = INT64_C(1699999999000002);
	image->assigned_record_count = 1;
	record = &image->records[0];
	record->lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	record->root_flags
		= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID
		  | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID;
	record->root_publish_seq = 1;
	record->checkpoint_tli = 1;
	record->tail_tli = 1;
	record->recovered_tli = 1;
	record->checkpoint_source_kind = CLUSTER_CONTROL_ROOT_CHECKPOINT_NATIVE_V1;
	record->tail_validation_kind = CLUSTER_CONTROL_ROOT_TAIL_WAL_RECORD_SCAN_V1;
	record->checkpoint_lower_lsn = UINT64_C(0x1000000);
	record->validated_tail_lsn_exclusive = UINT64_C(0x1000000);
	record->recovered_through_lsn_exclusive = UINT64_C(0x1000000);
	record->published_at_usec = image->created_at_usec;
	record->checkpoint_record_crc32c = UINT32_C(0x33445566);
	record->lifecycle_reason = CLUSTER_CONTROL_ROOT_PUBLISH_MIGRATION_IMPORT;

	memset(round, 0, sizeof(*round));
	memcpy(round->magic, "PCRM", 4);
	round->version = 1;
	round->bytes = sizeof(*round);
	round->prepare_generation = 1;
	round->transition_epoch = 7;
	round->target_feature_bitmap = PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1;
	round->admitted_bitmap_low = 1;
	round->capability_sample_digest = UINT64_C(0x8877665544332211);
	round->coordinator_incarnation = UINT64_C(0x7766554433221100);
	round->coordinator_node_id = 0;
}

static bool
parse_u64_arg(const char *text, uint64 *out)
{
	char *end = NULL;
	unsigned long long value;

	if (text == NULL || text[0] == '\0' || text[0] == '-')
		return false;
	errno = 0;
	value = strtoull(text, &end, 10);
	if (errno != 0 || end == text || *end != '\0')
		return false;
	*out = (uint64)value;
	return true;
}

static int
hex_digit(unsigned char ch)
{
	if (ch >= '0' && ch <= '9')
		return ch - '0';
	if (ch >= 'a' && ch <= 'f')
		return ch - 'a' + 10;
	if (ch >= 'A' && ch <= 'F')
		return ch - 'A' + 10;
	return -1;
}

static bool
parse_uuid_hex(const char *text, uint8 out[16])
{
	int i;

	if (text == NULL || strlen(text) != 32)
		return false;
	for (i = 0; i < 16; i++) {
		int high = hex_digit((unsigned char)text[i * 2]);
		int low = hex_digit((unsigned char)text[i * 2 + 1]);

		if (high < 0 || low < 0)
			return false;
		out[i] = (uint8)((high << 4) | low);
	}
	return true;
}

static bool
read_exact_file(const char *path, void *buf, size_t len)
{
	uint8 *bytes = buf;
	struct stat st;
	size_t done = 0;
	int fd = open(path, O_RDONLY | PG_BINARY);

	if (fd < 0 || fstat(fd, &st) != 0 || st.st_size != (off_t)len) {
		if (fd >= 0)
			close(fd);
		return false;
	}
	while (done < len) {
		ssize_t n = read(fd, bytes + done, len - done);

		if (n <= 0) {
			close(fd);
			return false;
		}
		done += (size_t)n;
	}
	return close(fd) == 0;
}

static bool
write_exact_durable(const char *path, const void *buf, size_t len)
{
	const uint8 *bytes = buf;
	size_t done = 0;
	int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY, 0600);

	if (fd < 0)
		return false;
	while (done < len) {
		ssize_t n = write(fd, bytes + done, len - done);

		if (n <= 0) {
			close(fd);
			return false;
		}
		done += (size_t)n;
	}
	if (fsync(fd) != 0 || close(fd) != 0)
		return false;
	return true;
}

static bool
fixture_seed_source(uint32 tli, uint64 checkpoint_lsn, uint64 tail_lsn,
					ClusterWalThreadClaim *out_claim)
{
	uint8 bytes[CLUSTER_WAL_STATE_FILE_SIZE];
	ClusterWalStateSlot slot;
	uint16 bad_thread = 0;
	const char *reason = NULL;
	char path[MAXPGPATH];
	char thread_dir[MAXPGPATH];
	int64 claim_created_at = INT64_C(1700000000000001);

	if (snprintf(path, sizeof(path), "%s/%s", test_wal_root, CLUSTER_WAL_STATE_FILENAME) <= 0
		|| !read_exact_file(path, bytes, sizeof(bytes))
		|| !cluster_wal_state_image_validate(bytes, sizeof(bytes), &bad_thread, &reason))
		return false;
	if (!cluster_wal_state_slot_is_zero(
			(ClusterWalStateSlot *)(bytes + CLUSTER_WAL_STATE_SLOT_OFFSET(1))))
		return false;

	cluster_wal_state_slot_fill(&slot, 1, 0, CLUSTER_WAL_SLOT_STATE_STOPPED, tli, claim_created_at,
								claim_created_at + 1, tail_lsn, 1);
	slot.checkpoint_redo_lsn = checkpoint_lsn;
	slot.crc = cluster_wal_state_block_crc(&slot);
	memcpy(bytes + CLUSTER_WAL_STATE_SLOT_OFFSET(1), &slot, sizeof(slot));
	if (!write_exact_durable(path, bytes, sizeof(bytes)))
		return false;

	cluster_wal_thread_claim_fill(out_claim, 1, 0, claim_created_at);
	if (snprintf(thread_dir, sizeof(thread_dir), "%s/thread_1", test_wal_root) <= 0)
		return false;
	if (mkdir(thread_dir, 0700) != 0 && errno != EEXIST)
		return false;
	if (snprintf(path, sizeof(path), "%s/%s", thread_dir, CLUSTER_WAL_THREAD_CLAIM_FILENAME) <= 0
		|| !write_exact_durable(path, out_claim, sizeof(*out_claim)))
		return false;
	return true;
}

static int
fixture_root_main(int argc, char **argv)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken prepared;
	ClusterControlRootFileToken active;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootIdentity expected_identity;
	ClusterControlRootReadToken read_token;
	ClusterWalThreadClaim claim;
	uint8 round_sha[PG_SHA256_DIGEST_LENGTH];
	uint64 sysid;
	uint64 tli64;
	uint64 checkpoint_lsn;
	uint64 tail_lsn;
	uint32 lifecycle;
	int i;

	if (argc != 10 || strcmp(argv[1], "--fixture-root") != 0 || !parse_u64_arg(argv[4], &sysid)
		|| sysid == 0 || !parse_u64_arg(argv[7], &tli64) || tli64 == 0 || tli64 > UINT32_MAX
		|| !parse_u64_arg(argv[8], &checkpoint_lsn) || checkpoint_lsn == 0
		|| !parse_u64_arg(argv[9], &tail_lsn) || tail_lsn < checkpoint_lsn
		|| strlen(argv[2]) >= sizeof(test_root) || strlen(argv[3]) >= sizeof(test_wal_root)
		|| strlen(argv[5]) != 32) {
		fprintf(stderr, "invalid --fixture-root arguments\n");
		return 2;
	}
	if (strcmp(argv[6], "OPEN") == 0)
		lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	else if (strcmp(argv[6], "RECOVERY_REQUIRED") == 0)
		lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	else if (strcmp(argv[6], "RECOVERY_COMPLETE") == 0)
		lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	else {
		fprintf(stderr, "unsupported fixture lifecycle\n");
		return 2;
	}

	strlcpy(test_root, argv[2], sizeof(test_root));
	strlcpy(test_wal_root, argv[3], sizeof(test_wal_root));
	strlcpy(test_storage_uuid_text, argv[5], sizeof(test_storage_uuid_text));
	test_system_identifier = sysid;
	cluster_shared_data_dir = test_root;
	cluster_wal_threads_dir = test_wal_root;
	DataDir = test_root;
	test_node_count = 1;
	test_local_probe = true;
	if (!fixture_seed_source((uint32)tli64, checkpoint_lsn, tail_lsn, &claim)) {
		fprintf(stderr, "cannot seed canonical stopped WAL source\n");
		return 1;
	}

	memset(&image, 0, sizeof(image));
	image.system_identifier = sysid;
	if (!parse_uuid_hex(argv[5], image.storage_uuid)) {
		fprintf(stderr, "invalid storage UUID\n");
		return 2;
	}
	for (i = 0; i < 16; i++)
		image.authority_uuid[i] = (uint8)(0xa0 + i);
	image.authority_uuid[6] = 0x46;
	image.authority_uuid[8] = 0x8a;
	image.created_at_usec = INT64_C(1700000000000002);
	image.assigned_record_count = 1;

	snapshot = (ClusterControlRootSnapshot){ 0 };
	snapshot.identity.system_identifier = sysid;
	memcpy(snapshot.identity.storage_uuid, image.storage_uuid, 16);
	memcpy(snapshot.identity.authority_uuid, image.authority_uuid, 16);
	snapshot.identity.origin_thread_id = 1;
	snapshot.identity.origin_node_id = 0;
	snapshot.identity.thread_claim_created_at = claim.created_at;
	snapshot.identity.thread_claim_crc32c = claim.crc;
	snapshot.identity.origin_owner_incarnation = UINT64_C(0x1122334455667788);
	snapshot.identity.root_lineage_seq = 1;
	snapshot.lifecycle = lifecycle;
	snapshot.root_flags = CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID
						  | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID
						  | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID;
	snapshot.root_publish_seq = 1;
	snapshot.checkpoint_tli = (uint32)tli64;
	snapshot.tail_tli = (uint32)tli64;
	snapshot.checkpoint_source_kind = CLUSTER_CONTROL_ROOT_CHECKPOINT_NATIVE_V1;
	snapshot.tail_validation_kind = CLUSTER_CONTROL_ROOT_TAIL_WAL_RECORD_SCAN_V1;
	snapshot.checkpoint_lower_lsn = checkpoint_lsn;
	snapshot.validated_tail_lsn_exclusive = tail_lsn;
	snapshot.checkpoint_record_crc32c = UINT32_C(0x33445566);
	if (tail_lsn > checkpoint_lsn) {
		snapshot.root_flags |= CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID;
		snapshot.tail_last_record_lsn = tail_lsn - 1;
		snapshot.tail_last_record_crc32c = UINT32_C(0x55667788);
	}
	if (lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED) {
		snapshot.root_flags |= CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID;
		snapshot.recovered_tli = (uint32)tli64;
		snapshot.recovered_through_lsn_exclusive = checkpoint_lsn;
	}
	snapshot.published_at_usec = image.created_at_usec;
	snapshot.lifecycle_reason = CLUSTER_CONTROL_ROOT_PUBLISH_MIGRATION_IMPORT;
	image.records[0] = snapshot;

	memset(&round, 0, sizeof(round));
	memcpy(round.magic, "PCRM", 4);
	round.version = 1;
	round.bytes = sizeof(round);
	round.prepare_generation = 1;
	round.transition_epoch = 1;
	round.target_feature_bitmap = PGRAC_CONTROL_ROOT_FEATURE_WAL_REUSE_V1
								  | PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1;
	round.admitted_bitmap_low = 1;
	round.capability_sample_digest = UINT64_C(0x8877665544332211);
	round.coordinator_incarnation = UINT64_C(0x7766554433221100);
	round.coordinator_node_id = 0;

	wipe_root_files();
	if (cluster_control_root_create_prepared(&image, &round, &prepared)
		!= CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		fprintf(stderr, "control-root prepare failed\n");
		return 1;
	}
	round_sha256(&round, round_sha);
	if (cluster_control_root_activate_prepared(&prepared, round_sha, &round, &active)
			!= CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| active.activation_state != CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE) {
		fprintf(stderr, "control-root activation verification failed\n");
		return 1;
	}
	expected_identity = snapshot.identity;
	if (cluster_control_root_read_canonical(1, &expected_identity, CLUSTER_CONTROL_ROOT_READ_STRONG,
											&snapshot, &read_token)
		!= CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		fprintf(stderr, "control-root activation verification failed\n");
		return 1;
	}
	return 0;
}

/*
 * RF-ROOT P6 pair cast (t/243 setup producer).
 *
 * Unlike the synthetic --fixture-root mode, this mode never rewrites the
 * wal-state registry or claim files.  It reads the REAL stopped slots for
 * threads 1 and 2 and the REAL claim files, and mints a canonical control
 * root whose two records mirror the whole registry:
 *
 *   record[0] = thread 1 / node 0, lifecycle argv[9]
 *   record[1] = thread 2 / node 1, lifecycle argv[7]
 *
 * argv: --fixture-root-cast <shared_root> <wal_root> <sysid>
 *       <storage_uuid_hex32> <authority_uuid_hex32> <lifecycle2> <inc2>
 *       <lifecycle1> <inc1>
 */
static bool
fixture_cast_load_thread(uint16 thread_id, int32 node_id, uint32 *out_tli, uint64 *out_ckpt,
						 uint64 *out_tail, ClusterWalThreadClaim *out_claim)
{
	uint8 bytes[CLUSTER_WAL_STATE_FILE_SIZE];
	ClusterWalStateSlot slot;
	ClusterWalThreadClaim disk_claim;
	ClusterWalThreadClaim expected_claim;
	uint16 bad_thread = 0;
	const char *reason = NULL;
	char path[MAXPGPATH];
	char thread_dir[MAXPGPATH];

	if (snprintf(path, sizeof(path), "%s/%s", test_wal_root, CLUSTER_WAL_STATE_FILENAME) <= 0
		|| !read_exact_file(path, bytes, sizeof(bytes))
		|| !cluster_wal_state_image_validate(bytes, sizeof(bytes), &bad_thread, &reason))
		return false;
	memcpy(&slot, bytes + CLUSTER_WAL_STATE_SLOT_OFFSET(thread_id), sizeof(slot));
	if (cluster_wal_state_slot_classify(&slot, thread_id, -1, NULL) != CLUSTER_WAL_SLOT_OK
		|| slot.state != CLUSTER_WAL_SLOT_STATE_STOPPED || slot.node_id != node_id || slot.tli == 0
		|| slot.checkpoint_redo_lsn == 0 || slot.highest_lsn == 0
		|| slot.highest_lsn < slot.checkpoint_redo_lsn || slot.merge_recovered_lsn != 0)
		return false;

	if (snprintf(thread_dir, sizeof(thread_dir), "%s/thread_%u", test_wal_root, thread_id) <= 0
		|| snprintf(path, sizeof(path), "%s/%s", thread_dir, CLUSTER_WAL_THREAD_CLAIM_FILENAME) <= 0
		|| !read_exact_file(path, (uint8 *)&disk_claim, sizeof(disk_claim)))
		return false;
	cluster_wal_thread_claim_fill(&expected_claim, thread_id, node_id, disk_claim.created_at);
	if (disk_claim.magic != expected_claim.magic || disk_claim.version != expected_claim.version
		|| disk_claim.thread_id != thread_id || disk_claim.node_id != node_id
		|| disk_claim.created_at == 0 || disk_claim.crc != expected_claim.crc)
		return false;

	*out_tli = slot.tli;
	*out_ckpt = slot.checkpoint_redo_lsn;
	*out_tail = slot.highest_lsn;
	*out_claim = expected_claim;
	return true;
}

static void
fixture_cast_fill_record(ClusterControlRootSnapshot *snapshot, uint64 sysid,
						 const uint8 storage_uuid[16], const uint8 authority_uuid[16],
						 uint16 thread_id, int32 node_id, const ClusterWalThreadClaim *claim,
						 uint32 lifecycle, uint64 owner_incarnation, uint32 tli, uint64 ckpt,
						 uint64 tail)
{
	*snapshot = (ClusterControlRootSnapshot){ 0 };
	snapshot->identity.system_identifier = sysid;
	memcpy(snapshot->identity.storage_uuid, storage_uuid, 16);
	memcpy(snapshot->identity.authority_uuid, authority_uuid, 16);
	snapshot->identity.origin_thread_id = thread_id;
	snapshot->identity.origin_node_id = node_id;
	snapshot->identity.thread_claim_created_at = claim->created_at;
	snapshot->identity.thread_claim_crc32c = claim->crc;
	snapshot->identity.origin_owner_incarnation = owner_incarnation;
	snapshot->identity.root_lineage_seq = 1;
	snapshot->lifecycle = lifecycle;
	snapshot->lifecycle_reason = CLUSTER_CONTROL_ROOT_PUBLISH_MIGRATION_IMPORT;
	snapshot->root_flags = CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID
						   | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID
						   | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID;
	snapshot->root_publish_seq = 1;
	snapshot->checkpoint_tli = tli;
	snapshot->tail_tli = tli;
	snapshot->checkpoint_source_kind = CLUSTER_CONTROL_ROOT_CHECKPOINT_NATIVE_V1;
	snapshot->tail_validation_kind = CLUSTER_CONTROL_ROOT_TAIL_WAL_RECORD_SCAN_V1;
	snapshot->checkpoint_lower_lsn = ckpt;
	snapshot->validated_tail_lsn_exclusive = tail;
	snapshot->checkpoint_record_crc32c = UINT32_C(0x33445566);
	if (tail > ckpt) {
		snapshot->root_flags |= CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID;
		snapshot->tail_last_record_lsn = tail - 1;
		snapshot->tail_last_record_crc32c = UINT32_C(0x55667788);
	}
}

static int
fixture_cast_main(int argc, char **argv)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken prepared;
	ClusterControlRootFileToken active;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootIdentity expected_identity;
	ClusterControlRootReadToken read_token;
	ClusterWalThreadClaim claim1;
	ClusterWalThreadClaim claim2;
	ClusterControlRootResult result_cast_prepare;
	uint8 round_sha[PG_SHA256_DIGEST_LENGTH];
	uint64 sysid;
	uint64 inc1;
	uint64 inc2;
	uint32 lifecycle1;
	uint32 lifecycle2;
	uint32 tli1;
	uint32 tli2;
	uint64 ckpt1;
	uint64 ckpt2;
	uint64 tail1;
	uint64 tail2;
	uint8 storage_uuid[16];
	uint8 authority_uuid[16];

	if (argc != 11 || strcmp(argv[1], "--fixture-root-cast") != 0 || !parse_u64_arg(argv[4], &sysid)
		|| sysid == 0 || strlen(argv[2]) >= sizeof(test_root)
		|| strlen(argv[3]) >= sizeof(test_wal_root) || strlen(argv[5]) != 32
		|| strlen(argv[6]) != 32 || !parse_uuid_hex(argv[5], storage_uuid)
		|| !parse_uuid_hex(argv[6], authority_uuid) || (authority_uuid[6] & 0xf0) != 0x40
		|| (authority_uuid[8] & 0xc0) != 0x80 || !parse_u64_arg(argv[8], &inc2) || inc2 == 0
		|| !parse_u64_arg(argv[10], &inc1) || inc1 == 0) {
		fprintf(stderr, "invalid --fixture-root-cast arguments\n");
		return 2;
	}
	if (strcmp(argv[7], "RECOVERY_COMPLETE") == 0)
		lifecycle2 = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	else if (strcmp(argv[7], "OPEN") == 0)
		lifecycle2 = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	else if (strcmp(argv[7], "RECOVERY_REQUIRED") == 0)
		lifecycle2 = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	else {
		fprintf(stderr, "unsupported cast lifecycle 2\n");
		return 2;
	}
	if (strcmp(argv[9], "OPEN") == 0)
		lifecycle1 = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	else if (strcmp(argv[9], "RECOVERY_COMPLETE") == 0)
		lifecycle1 = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	else {
		fprintf(stderr, "unsupported cast lifecycle 1\n");
		return 2;
	}

	strlcpy(test_root, argv[2], sizeof(test_root));
	strlcpy(test_wal_root, argv[3], sizeof(test_wal_root));
	strlcpy(test_storage_uuid_text, argv[5], sizeof(test_storage_uuid_text));
	test_system_identifier = sysid;
	cluster_shared_data_dir = test_root;
	cluster_wal_threads_dir = test_wal_root;
	DataDir = test_root;
	test_node_count = 2;
	test_local_probe = true;

	if (!fixture_cast_load_thread(1, 0, &tli1, &ckpt1, &tail1, &claim1)) {
		fprintf(stderr, "cannot load real thread-1 source\n");
		return 1;
	}
	if (!fixture_cast_load_thread(2, 1, &tli2, &ckpt2, &tail2, &claim2)) {
		fprintf(stderr, "cannot load real thread-2 source\n");
		return 1;
	}

	memset(&image, 0, sizeof(image));
	image.system_identifier = sysid;
	memcpy(image.storage_uuid, storage_uuid, 16);
	memcpy(image.authority_uuid, authority_uuid, 16);
	image.created_at_usec = INT64_C(1700000000000002);
	image.assigned_record_count = 2;
	fixture_cast_fill_record(&image.records[0], sysid, storage_uuid, authority_uuid, 1, 0, &claim1,
							 lifecycle1, inc1, tli1, ckpt1, tail1);
	fixture_cast_fill_record(&image.records[1], sysid, storage_uuid, authority_uuid, 2, 1, &claim2,
							 lifecycle2, inc2, tli2, ckpt2, tail2);

	memset(&round, 0, sizeof(round));
	memcpy(round.magic, "PCRM", 4);
	round.version = 1;
	round.bytes = sizeof(round);
	round.prepare_generation = 1;
	round.transition_epoch = 1;
	round.target_feature_bitmap = PGRAC_CONTROL_ROOT_FEATURE_WAL_REUSE_V1
								  | PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1;
	round.admitted_bitmap_low = 3;
	round.capability_sample_digest = UINT64_C(0x8877665544332211);
	round.coordinator_incarnation = UINT64_C(0x7766554433221100);
	round.coordinator_node_id = 0;

	wipe_root_files();
	result_cast_prepare = cluster_control_root_create_prepared(&image, &round, &prepared);
	if (result_cast_prepare != CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		fprintf(stderr, "control-root cast prepare failed (result %d)\n", (int)result_cast_prepare);
		return 1;
	}
	round_sha256(&round, round_sha);
	if (cluster_control_root_activate_prepared(&prepared, round_sha, &round, &active)
			!= CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| active.activation_state != CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE) {
		fprintf(stderr, "control-root cast activation failed\n");
		return 1;
	}
	expected_identity = image.records[0].identity;
	if (cluster_control_root_read_canonical(1, &expected_identity, CLUSTER_CONTROL_ROOT_READ_STRONG,
											&snapshot, &read_token)
		!= CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		fprintf(stderr, "control-root cast thread-1 readback failed\n");
		return 1;
	}
	expected_identity = image.records[1].identity;
	if (cluster_control_root_read_canonical(2, &expected_identity, CLUSTER_CONTROL_ROOT_READ_STRONG,
											&snapshot, &read_token)
		!= CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		fprintf(stderr, "control-root cast thread-2 readback failed\n");
		return 1;
	}
	return 0;
}

static ClusterControlRootResult
create_prepared(ClusterControlRootMigrationImage *image, ClusterControlRootMigrationRoundV1 *round,
				ClusterControlRootFileToken *token)
{
	build_migration(image, round);
	return cluster_control_root_create_prepared(image, round, token);
}

static void
force_first_record_lineage(uint64 lineage)
{
	uint8 bytes[CLUSTER_CONTROL_ROOT_FILE_BYTES];
	uint8 *record = bytes + CLUSTER_CONTROL_ROOT_HEADER_BYTES;
	char primary[MAXPGPATH];
	char bak[MAXPGPATH];

	path_for(primary, sizeof(primary), CLUSTER_CONTROL_ROOT_REL_PATH);
	path_for(bak, sizeof(bak), CLUSTER_CONTROL_ROOT_BAK_REL_PATH);
	read_all_or_abort(primary, bytes, sizeof(bytes));
	put_u64_le(record + 24, lineage);
	put_u32_le(record + 504, image_crc(record, 504));
	put_u32_le(bytes + 96, image_crc(bytes + CLUSTER_CONTROL_ROOT_HEADER_BYTES,
									 sizeof(bytes) - CLUSTER_CONTROL_ROOT_HEADER_BYTES));
	put_u32_le(bytes + 504, image_crc(bytes, 504));
	write_all_or_abort(primary, bytes, sizeof(bytes));
	write_all_or_abort(bak, bytes, sizeof(bytes));
}

static void
build_owner_rejoin_patch(const ClusterControlRootSnapshot *snapshot, uint64 new_incarnation,
						 uint64 new_lineage, ClusterControlRootPatch *patch)
{
	memset(patch, 0, sizeof(*patch));
	patch->mask = UINT64_C(0x3b);
	patch->expected_lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	patch->desired.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	patch->desired.identity.origin_owner_incarnation = new_incarnation;
	patch->desired.identity.root_lineage_seq = new_lineage;
	patch->desired.root_flags = snapshot->root_flags;
	patch->desired.checkpoint_tli = snapshot->checkpoint_tli;
	patch->desired.checkpoint_source_kind = snapshot->checkpoint_source_kind;
	patch->desired.checkpoint_lower_lsn = snapshot->checkpoint_lower_lsn;
	patch->desired.checkpoint_record_crc32c = snapshot->checkpoint_record_crc32c;
	patch->desired.tail_tli = snapshot->tail_tli;
	patch->desired.tail_validation_kind = snapshot->tail_validation_kind;
	patch->desired.validated_tail_lsn_exclusive = snapshot->validated_tail_lsn_exclusive;
	patch->desired.tail_last_record_lsn = snapshot->tail_last_record_lsn;
	patch->desired.tail_last_record_crc32c = snapshot->tail_last_record_crc32c;
	patch->desired.recovered_tli = snapshot->recovered_tli;
	patch->desired.recovered_through_lsn_exclusive = snapshot->recovered_through_lsn_exclusive;
	patch->desired.recovered_last_record_lsn = snapshot->recovered_last_record_lsn;
	patch->desired.recovered_last_record_crc32c = snapshot->recovered_last_record_crc32c;
}

static void
setup_fixture(void)
{
	char tmpl[MAXPGPATH];
	char path[MAXPGPATH];

	strlcpy(tmpl, "/tmp/pgrac_control_root_XXXXXX", sizeof(tmpl));
	if (mkdtemp(tmpl) == NULL)
		abort();
	strlcpy(test_root, tmpl, sizeof(test_root));
	cluster_shared_data_dir = test_root;
	DataDir = test_root;

	snprintf(path, sizeof(path), "%s/global", test_root);
	if (mkdir(path, 0700) != 0)
		abort();
	snprintf(test_wal_root, sizeof(test_wal_root), "%s/wal", test_root);
	if (mkdir(test_wal_root, 0700) != 0)
		abort();
	cluster_wal_threads_dir = test_wal_root;
	build_source_wal_state();
}

UT_TEST(test_abi_identity_and_features)
{
	ClusterControlRootIdentity left;
	ClusterControlRootIdentity right;
	uint64 known = (UINT64_C(1) << 0) | PGRAC_CONTROL_ROOT_FEATURE_WAL_REUSE_V1
				   | PGRAC_CONTROL_ROOT_FEATURE_PAGE_STABLE_BASE_V1
				   | PGRAC_CONTROL_ROOT_FEATURE_SPACE_METADATA_V1
				   | PGRAC_CONTROL_ROOT_FEATURE_CONSERVATIVE_COMMIT_SCN_V1
				   | PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1
				   | PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_SERIAL_V1
				   | PGRAC_CONTROL_ROOT_FEATURE_EXTERNAL_FENCE_V1;

	UT_ASSERT_EQ(CLUSTER_CONTROL_ROOT_FILE_BYTES, 66048);
	UT_ASSERT_EQ(CLUSTER_CONTROL_ROOT_FORMAT_FLAGS_V1, UINT64_C(0x0d));
	UT_ASSERT_EQ(CLUSTER_CONTROL_ROOT_FLAGS_V1, UINT32_C(0x1fd));
	UT_ASSERT_EQ(CLUSTER_CONTROL_ROOT_PATCH_ALL_V1, UINT64_C(0xfb));
	UT_ASSERT_EQ(PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1, UINT64_C(0x00400000));
	UT_ASSERT_EQ(PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_SERIAL_V1, UINT64_C(0x00800000));
	UT_ASSERT_EQ(PGRAC_CONTROL_ROOT_FEATURE_EXTERNAL_FENCE_V1, UINT64_C(0x01000000));
	UT_ASSERT_EQ(PGRAC_CONTROL_ROOT_FEATURE_KNOWN_MASK_V1, UINT64_C(0x01ee0001));
	UT_ASSERT_EQ(known, PGRAC_CONTROL_ROOT_FEATURE_KNOWN_MASK_V1);
	fill_identity(&left);
	right = left;
	UT_ASSERT(cluster_control_root_identity_equal(&left, &right));
	right.root_lineage_seq++;
	UT_ASSERT(!cluster_control_root_identity_equal(&left, &right));
	UT_ASSERT(cluster_control_root_feature_bitmap_is_known(known));
	UT_ASSERT(!cluster_control_root_feature_bitmap_is_known(UINT64_C(1) << 63));
}

UT_TEST(test_invalid_argument_precedes_authority_io)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken token;

	wipe_root_files();
	build_migration(&image, &round);
	round.reserved76 = 1;
	memset(&token, 0xee, sizeof(token));
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &token),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
	UT_ASSERT_EQ(token.file_txn_seq, 0);
}

UT_TEST(test_external_fence_bit24_activation_is_forbidden_without_provider)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken token;

	wipe_root_files();
	build_migration(&image, &round);
	round.target_feature_bitmap |= PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_SERIAL_V1
								   | PGRAC_CONTROL_ROOT_FEATURE_EXTERNAL_FENCE_V1;
	memset(&token, 0xee, sizeof(token));
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &token),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
	UT_ASSERT_EQ(token.file_txn_seq, 0);
}

UT_TEST(test_create_and_read_primary)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken read_token;
	uint8 primary[CLUSTER_CONTROL_ROOT_FILE_BYTES];
	uint8 bak[CLUSTER_CONTROL_ROOT_FILE_BYTES];
	char primary_path[MAXPGPATH];
	char bak_path[MAXPGPATH];

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &file_token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(file_token.file_txn_seq, 1);
	UT_ASSERT_EQ(file_token.activation_state, CLUSTER_CONTROL_ROOT_ACTIVATION_PREPARED);
	path_for(primary_path, sizeof(primary_path), CLUSTER_CONTROL_ROOT_REL_PATH);
	path_for(bak_path, sizeof(bak_path), CLUSTER_CONTROL_ROOT_BAK_REL_PATH);
	read_all_or_abort(primary_path, primary, sizeof(primary));
	read_all_or_abort(bak_path, bak, sizeof(bak));
	UT_ASSERT(memcmp(primary, bak, sizeof(primary)) == 0);
	UT_ASSERT_EQ(image_crc(primary + CLUSTER_CONTROL_ROOT_HEADER_BYTES,
						   sizeof(primary) - CLUSTER_CONTROL_ROOT_HEADER_BYTES),
				 file_token.body_crc32c);

	memset(&snapshot, 0xee, sizeof(snapshot));
	memset(&read_token, 0xee, sizeof(read_token));
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &image.records[0].identity,
													 CLUSTER_CONTROL_ROOT_READ_STRONG, &snapshot,
													 &read_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(cluster_control_root_identity_equal(&snapshot.identity, &image.records[0].identity));
	UT_ASSERT_EQ(read_token.file_txn_seq, 1);
	UT_ASSERT_EQ(read_token.origin_thread_id, 1);
}

UT_TEST(test_bootstrap_read_never_returns_authority_token)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken read_token;

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &file_token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	test_cf_lock_calls = 0;
	memset(&read_token, 0xee, sizeof(read_token));
	UT_ASSERT_EQ(cluster_control_root_read_canonical(
					 1, NULL, CLUSTER_CONTROL_ROOT_READ_BOOTSTRAP_VALIDATE, &snapshot, &read_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
	UT_ASSERT_EQ(read_token.file_txn_seq, 0);
}

/* RF-ROOT P7 (contract §A / follow-up E1): the NULL-identity bug class — a
 * STRONG read with expected_identity == NULL must stay INVALID_ARGUMENT=23
 * (the G1b step-4 sites' inertness signature).  The legal no-prior-identity
 * path is the two-step discovered read below. */
UT_TEST(test_round_sha256_is_deterministic_and_matches_create)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken token;
	uint8 sha_a[PG_SHA256_DIGEST_LENGTH];
	uint8 sha_b[PG_SHA256_DIGEST_LENGTH];

	wipe_root_files();
	build_migration(&image, &round);
	UT_ASSERT(cluster_control_root_round_sha256(&round, sha_a));
	UT_ASSERT(cluster_control_root_round_sha256(&round, sha_b));
	UT_ASSERT(memcmp(sha_a, sha_b, sizeof(sha_a)) == 0);
	/* create_prepared must succeed with the same round (its header stores
	 * the same wire-encoded sha). */
	UT_ASSERT_EQ(create_prepared(&image, &round, &token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
}

UT_TEST(test_build_migration_image_maps_registry_and_claims)
{
	ClusterControlRootMigrationImage image;

	wipe_root_files();
	build_source_wal_state(); /* registry slot 1 STOPPED + thread_1 claim */
	test_membership_incarnation = UINT64_C(0x1020304050607080);
	UT_ASSERT_EQ(cluster_control_root_build_migration_image(NULL, &image),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(image.assigned_record_count, 1);
	/* the checkpoint record CRC must come from the real WAL stream scan */
	UT_ASSERT(image.records[0].checkpoint_record_crc32c != 0);
	UT_ASSERT_EQ(image.records[0].identity.origin_thread_id, 1);
	UT_ASSERT_EQ(image.records[0].identity.origin_node_id, 0);
	UT_ASSERT_EQ(image.records[0].identity.origin_owner_incarnation, UINT64_C(0x1020304050607080));
	UT_ASSERT_EQ(image.records[0].identity.thread_claim_created_at, INT64_C(1699999999000001));
	UT_ASSERT_EQ(image.records[0].lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED);
	UT_ASSERT_EQ(image.records[0].checkpoint_lower_lsn, UINT64_C(0x1000000));
	UT_ASSERT_EQ(image.records[0].validated_tail_lsn_exclusive, UINT64_C(0x1000000));
	UT_ASSERT((image.records[0].root_flags & CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID) != 0);
	UT_ASSERT(memcmp(image.storage_uuid, image.records[0].identity.storage_uuid, 16) == 0);
	build_source_wal_state(); /* restore the shared fixture for later tests */
}

UT_TEST(test_build_migration_image_accepts_frozen_active_slot)
{
	/* RF-ROOT P9 verification (implementation): the online first-open round freezes
	 * every member's wal-state writers first; an ACTIVE slot is then
	 * provably quiesced and acceptable as migration input. */
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	uint8 bytes[CLUSTER_WAL_STATE_FILE_SIZE];
	ClusterWalStateSlot slot;

	wipe_root_files();
	memset(bytes, 0, sizeof(bytes));
	cluster_wal_state_header_fill((ClusterWalStateHeader *)bytes, INT64_C(1699999999000000));
	cluster_wal_state_slot_fill(&slot, 1, 0, CLUSTER_WAL_SLOT_STATE_ACTIVE, 1,
								INT64_C(1699999999000001), INT64_C(1699999999000002),
								UINT64_C(0x1000000), 1);
	slot.checkpoint_redo_lsn = UINT64_C(0x1000000);
	slot.crc = cluster_wal_state_block_crc(&slot);
	memcpy(bytes + CLUSTER_WAL_STATE_SLOT_OFFSET(1), &slot, sizeof(slot));
	{
		char path[MAXPGPATH];

		snprintf(path, sizeof(path), "%s/%s", test_wal_root, CLUSTER_WAL_STATE_FILENAME);
		write_all_or_abort(path, bytes, sizeof(bytes));
	}
	/* claim + minimal WAL segment for the scan */
	{
		char thread_dir[MAXPGPATH];
		char path[MAXPGPATH];
		ClusterWalThreadClaim claim;

		snprintf(thread_dir, sizeof(thread_dir), "%s/thread_1", test_wal_root);
		if (mkdir(thread_dir, 0700) != 0 && errno != EEXIST)
			abort();
		cluster_wal_thread_claim_fill(&claim, 1, 0, INT64_C(1699999999000001));
		snprintf(path, sizeof(path), "%s/%s", thread_dir, CLUSTER_WAL_THREAD_CLAIM_FILENAME);
		write_all_or_abort(path, &claim, sizeof(claim));
		write_minimal_checkpoint_segment(thread_dir);
	}
	test_membership_incarnation = UINT64_C(0x1020304050607080);
	memset(&round, 0, sizeof(round));
	round.transition_epoch = 7;
	round.prepare_generation = 5;

	/* ACTIVE without the round's freeze -> refused. */
	test_source_close_current_ok = false;
	UT_ASSERT_EQ(cluster_control_root_build_migration_image(&round, &image),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);

	/* ACTIVE frozen by this exact round -> accepted. */
	test_source_close_current_ok = true;
	UT_ASSERT_EQ(cluster_control_root_build_migration_image(&round, &image),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(image.assigned_record_count, 1);
	UT_ASSERT_EQ(image.records[0].identity.origin_thread_id, 1);
	UT_ASSERT(image.records[0].checkpoint_record_crc32c != 0);
	test_source_close_current_ok = false;
}

UT_TEST(test_build_migration_image_rejects_non_stopped_slot)
{
	ClusterControlRootMigrationImage image;
	ClusterWalStateSlot slot;
	char path[MAXPGPATH];
	int fd;

	wipe_root_files();
	build_source_wal_state();
	/* flip slot 1 to ACTIVE — the W6 CLOSED precondition is violated */
	path_for(path, sizeof(path), ""); /* reuse: write into the wal root */
	snprintf(path, sizeof(path), "%s/%s", test_wal_root, CLUSTER_WAL_STATE_FILENAME);
	fd = open(path, O_RDWR);
	UT_ASSERT(fd >= 0);
	memcpy(&slot, (void *)0, 0); /* noop to keep compiler quiet */
	{
		ClusterWalStateSlot s;

		if (pread(fd, &s, sizeof(s), CLUSTER_WAL_STATE_SLOT_OFFSET(1)) != (ssize_t)sizeof(s))
			abort();
		s.state = CLUSTER_WAL_SLOT_STATE_ACTIVE;
		s.crc = cluster_wal_state_block_crc(&s);
		if (pwrite(fd, &s, sizeof(s), CLUSTER_WAL_STATE_SLOT_OFFSET(1)) != (ssize_t)sizeof(s))
			abort();
	}
	close(fd);
	UT_ASSERT_EQ(cluster_control_root_build_migration_image(NULL, &image),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	build_source_wal_state(); /* restore the STOPPED fixture */
}

UT_TEST(test_strong_read_null_identity_stays_invalid_argument)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken read_token;

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &file_token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	memset(&snapshot, 0xee, sizeof(snapshot));
	memset(&read_token, 0xee, sizeof(read_token));
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, NULL, CLUSTER_CONTROL_ROOT_READ_STRONG,
													 &snapshot, &read_token),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(snapshot.identity.system_identifier, 0);
	UT_ASSERT_EQ(read_token.file_txn_seq, 0);
}

UT_TEST(test_discovered_read_binds_identity_and_mints_token)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken read_token;

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &file_token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	memset(&snapshot, 0xee, sizeof(snapshot));
	memset(&read_token, 0xee, sizeof(read_token));
	UT_ASSERT_EQ(cluster_control_root_read_canonical_discovered(1, &snapshot, &read_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(cluster_control_root_identity_equal(&snapshot.identity, &image.records[0].identity));
	UT_ASSERT_EQ(snapshot.checkpoint_lower_lsn, UINT64_C(0x1000000));
	/* The STRONG step mints the authority token (BOOTSTRAP never does). */
	UT_ASSERT_EQ(read_token.file_txn_seq, 1);
	UT_ASSERT_EQ(read_token.origin_thread_id, 1);
}

UT_TEST(test_discovered_read_absent_thread_fails_closed)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken read_token;

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &file_token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	memset(&snapshot, 0xee, sizeof(snapshot));
	memset(&read_token, 0xee, sizeof(read_token));
	/* The fixture mints record[0] only; tid 2 was never present. */
	UT_ASSERT_EQ(cluster_control_root_read_canonical_discovered(2, &snapshot, &read_token),
				 CLUSTER_CONTROL_ROOT_ABSENT);
	UT_ASSERT_EQ(snapshot.identity.system_identifier, 0);
	UT_ASSERT_EQ(read_token.file_txn_seq, 0);
}

UT_TEST(test_valid_bak_blocks_corrupt_primary)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken read_token;
	char path[MAXPGPATH];
	int fd;
	uint8 byte;

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &file_token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	fd = open(path, O_RDWR);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pread(fd, &byte, 1, 0), 1);
	byte ^= 0xff;
	UT_ASSERT_EQ(pwrite(fd, &byte, 1, 0), 1);
	close(fd);
	memset(&snapshot, 0xee, sizeof(snapshot));
	memset(&read_token, 0xee, sizeof(read_token));
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &image.records[0].identity,
													 CLUSTER_CONTROL_ROOT_READ_STRONG, &snapshot,
													 &read_token),
				 CLUSTER_CONTROL_ROOT_OK_BAK_BLOCKED);
	UT_ASSERT_EQ(snapshot.identity.system_identifier, 0);
	UT_ASSERT_EQ(read_token.file_txn_seq, 0);
}

UT_TEST(test_storage_contract_fails_before_cf_or_file_io)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken token;

	wipe_root_files();
	test_contract = CLUSTER_CF_CONTRACT_UNVERIFIED;
	UT_ASSERT_EQ(create_prepared(&image, &round, &token),
				 CLUSTER_CONTROL_ROOT_STORAGE_CONTRACT_UNVERIFIED);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
	UT_ASSERT_EQ(test_durable_rename_calls, 0);
}

UT_TEST(test_single_node_local_probe_fails_before_cf_or_file_io)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken token;

	wipe_root_files();
	test_node_count = 1;
	test_local_probe = false;
	UT_ASSERT_EQ(create_prepared(&image, &round, &token),
				 CLUSTER_CONTROL_ROOT_STORAGE_CONTRACT_UNVERIFIED);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
	UT_ASSERT_EQ(test_durable_rename_calls, 0);
}

UT_TEST(test_activate_and_stale_token)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken prepared;
	ClusterControlRootFileToken active;
	ClusterControlRootFileToken stale_out;
	uint8 round_sha[PG_SHA256_DIGEST_LENGTH];

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &prepared), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	round_sha256(&round, round_sha);
	UT_ASSERT_EQ(cluster_control_root_activate_prepared(&prepared, round_sha, &round, &active),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(active.activation_state, CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE);
	UT_ASSERT_EQ(active.file_txn_seq, 2);
	memset(&stale_out, 0xee, sizeof(stale_out));
	UT_ASSERT_EQ(cluster_control_root_activate_prepared(&prepared, round_sha, &round, &stale_out),
				 CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT_EQ(stale_out.file_txn_seq, 0);
}

UT_TEST(test_restore_bit22_latch_from_active_root)
{
	/* RF-ROOT P9 verification (implementation): the latch restores only on the
	 * DURABLE Target OPEN proof — a strict-majority OPEN(P+2) record on
	 * the voting disks cross-matched to the ACTIVE canonical root's round
	 * identity (root migration_transition_epoch == OPEN.transition_epoch
	 * AND root migration_prepare_generation + 2 == OPEN.record_generation).
	 * No record-lifecycle axis participates.  The apply lands at
	 * TARGET_BOOTSTRAP; a refused apply (census RED) fails closed. */
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken prepared;
	ClusterControlRootFileToken active;
	uint8 round_sha[PG_SHA256_DIGEST_LENGTH];

	/* No durable OPEN record -> no restore. */
	wipe_root_files();
	test_bit22_latch_active = false;
	test_bit22_latch_apply_calls = 0;
	test_qvotec_open_present = false;
	UT_ASSERT(!cluster_control_root_restore_bit22_latch_if_active());
	UT_ASSERT_EQ(test_bit22_latch_apply_calls, 0);

	/* OPEN record present but no root -> no restore. */
	test_qvotec_open_present = true;
	test_qvotec_open_epoch = 7;
	test_qvotec_open_generation = 7;
	test_decoded_open.phase = CLUSTER_SEMANTIC_PHASE_OPEN;
	test_decoded_open.transition_epoch = 7;
	test_decoded_open.record_generation = 7;
	UT_ASSERT(!cluster_control_root_restore_bit22_latch_if_active());
	UT_ASSERT_EQ(test_bit22_latch_apply_calls, 0);

	/* PREPARED root (create only) -> no restore (not ACTIVE). */
	UT_ASSERT_EQ(create_prepared(&image, &round, &prepared), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	test_bit22_latch_apply_calls = 0;
	UT_ASSERT(!cluster_control_root_restore_bit22_latch_if_active());
	UT_ASSERT_EQ(test_bit22_latch_apply_calls, 0);

	/* ACTIVE root but the OPEN record does not cross-match the round
	 * identity -> no restore. */
	round_sha256(&round, round_sha);
	UT_ASSERT_EQ(cluster_control_root_activate_prepared(&prepared, round_sha, &round, &active),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(active.activation_state, CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE);
	test_decoded_open.transition_epoch = round.transition_epoch + 1;
	test_decoded_open.record_generation = round.prepare_generation + 2; /* epoch mismatch */
	test_bit22_latch_apply_calls = 0;
	UT_ASSERT(!cluster_control_root_restore_bit22_latch_if_active());
	UT_ASSERT_EQ(test_bit22_latch_apply_calls, 0);
	UT_ASSERT(!test_bit22_latch_active);

	/* ACTIVE root + exact cross-match -> restored with the OPEN record's
	 * round identity (TARGET_BOOTSTRAP). */
	test_decoded_open.transition_epoch = round.transition_epoch;
	test_decoded_open.record_generation = round.prepare_generation + 2;
	test_bit22_latch_apply_calls = 0;
	UT_ASSERT(cluster_control_root_restore_bit22_latch_if_active());
	UT_ASSERT_EQ(test_bit22_latch_apply_calls, 1);
	UT_ASSERT_EQ(test_bit22_latch_apply_epoch, round.transition_epoch);
	UT_ASSERT_EQ(test_bit22_latch_apply_generation, round.prepare_generation + 2);
	UT_ASSERT(test_bit22_latch_active);

	/* Already armed -> no second apply. */
	test_bit22_latch_apply_calls = 0;
	UT_ASSERT(cluster_control_root_restore_bit22_latch_if_active());
	UT_ASSERT_EQ(test_bit22_latch_apply_calls, 0);

	/* Refused apply (census RED stand-in) -> fail-closed, gate stays off. */
	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &prepared), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	round_sha256(&round, round_sha);
	UT_ASSERT_EQ(cluster_control_root_activate_prepared(&prepared, round_sha, &round, &active),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	test_decoded_open.transition_epoch = round.transition_epoch;
	test_decoded_open.record_generation = round.prepare_generation + 2;
	test_bit22_latch_active = false;
	test_bit22_latch_apply_ok = false;
	test_bit22_latch_apply_calls = 0;
	UT_ASSERT(!cluster_control_root_restore_bit22_latch_if_active());
	UT_ASSERT_EQ(test_bit22_latch_apply_calls, 1);
	UT_ASSERT(!test_bit22_latch_active);
	test_bit22_latch_apply_ok = true;
}

UT_TEST(test_unbound_cutover_mutators_fail_before_cf_and_preserve_prepared_root)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken prepared;
	ClusterControlRootFileToken active;
	uint8 round_sha[PG_SHA256_DIGEST_LENGTH];
	int lock_calls_before;
	int rename_calls_before;

	wipe_root_files();
	build_migration(&image, &round);
	memset(&prepared, 0xee, sizeof(prepared));
	test_create_authorized = false;
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &prepared),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
	UT_ASSERT_EQ(test_durable_rename_calls, 0);
	UT_ASSERT_EQ(prepared.file_txn_seq, 0);

	test_create_authorized = true;
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &prepared),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	round_sha256(&round, round_sha);
	lock_calls_before = test_cf_lock_calls;
	rename_calls_before = test_durable_rename_calls;
	memset(&active, 0xee, sizeof(active));
	test_activate_authorized = false;
	UT_ASSERT_EQ(cluster_control_root_activate_prepared(&prepared, round_sha, &round, &active),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(test_cf_lock_calls, lock_calls_before);
	UT_ASSERT_EQ(test_durable_rename_calls, rename_calls_before);
	UT_ASSERT_EQ(active.file_txn_seq, 0);

	/* The refused attempt cannot consume or mutate the PREPARED image. */
	test_activate_authorized = true;
	UT_ASSERT_EQ(cluster_control_root_activate_prepared(&prepared, round_sha, &round, &active),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(active.activation_state, CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE);
	UT_ASSERT_EQ(active.file_txn_seq, prepared.file_txn_seq + 1);
}

UT_TEST(test_native_cf_hold_cannot_authorize_strong_read)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken token;

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &file_token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	test_cf_clusterwide = false;
	memset(&snapshot, 0xee, sizeof(snapshot));
	memset(&token, 0xee, sizeof(token));
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &image.records[0].identity,
													 CLUSTER_CONTROL_ROOT_READ_STRONG, &snapshot,
													 &token),
				 CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	UT_ASSERT_EQ(snapshot.identity.system_identifier, 0);
	UT_ASSERT_EQ(token.file_txn_seq, 0);
}

UT_TEST(test_activation_rejects_changed_source_wal_bytes)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken prepared;
	ClusterControlRootFileToken active;
	ClusterWalStateHeader header;
	uint8 round_sha[PG_SHA256_DIGEST_LENGTH];
	char path[MAXPGPATH];
	int fd;

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &prepared), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	snprintf(path, sizeof(path), "%s/%s", test_wal_root, CLUSTER_WAL_STATE_FILENAME);
	fd = open(path, O_RDWR);
	UT_ASSERT(fd >= 0);
	cluster_wal_state_header_fill(&header, INT64_C(1700000000000999));
	UT_ASSERT_EQ(pwrite(fd, &header, sizeof(header), 0), sizeof(header));
	close(fd);
	round_sha256(&round, round_sha);
	memset(&active, 0xee, sizeof(active));
	UT_ASSERT_EQ(cluster_control_root_activate_prepared(&prepared, round_sha, &round, &active),
				 CLUSTER_CONTROL_ROOT_HASH_MISMATCH);
	UT_ASSERT_EQ(active.file_txn_seq, 0);
	build_source_wal_state();
}

UT_TEST(test_activation_rejects_same_node_thread_claim_drift)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken prepared;
	ClusterControlRootFileToken active;
	ClusterWalThreadClaim claim;
	uint8 round_sha[PG_SHA256_DIGEST_LENGTH];
	char path[MAXPGPATH];

	wipe_root_files();
	build_source_wal_state();
	UT_ASSERT_EQ(create_prepared(&image, &round, &prepared), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	cluster_wal_thread_claim_fill(&claim, 1, 0, INT64_C(1699999999000999));
	snprintf(path, sizeof(path), "%s/thread_1/%s", test_wal_root,
			 CLUSTER_WAL_THREAD_CLAIM_FILENAME);
	write_all_or_abort(path, &claim, sizeof(claim));
	round_sha256(&round, round_sha);
	memset(&active, 0xee, sizeof(active));
	UT_ASSERT_EQ(cluster_control_root_activate_prepared(&prepared, round_sha, &round, &active),
				 CLUSTER_CONTROL_ROOT_HASH_MISMATCH);
	UT_ASSERT_EQ(active.file_txn_seq, 0);
	build_source_wal_state();
}

UT_TEST(test_forbidden_patch_rejected_before_cf_and_file_io)
{
	ClusterControlRootReadToken token;
	ClusterControlRootPatch patch;
	ClusterControlRootSnapshot snapshot;

	wipe_root_files();
	memset(&token, 0, sizeof(token));
	token.source = 1;
	token.origin_thread_id = 1;
	memset(&patch, 0, sizeof(patch));
	patch.mask = UINT64_C(0x04);
	patch.expected_lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	test_cf_lock_calls = 0;
	UT_ASSERT_EQ(cluster_control_root_compare_and_publish(
					 &token, &patch, CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_RETIRE, &snapshot, NULL),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
}

UT_TEST(test_lookup_and_revalidate_use_exact_primary_identity)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootIdentity identity;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken token;
	ClusterControlRootReadToken lookup_token;

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &file_token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &image.records[0].identity,
													 CLUSTER_CONTROL_ROOT_READ_STRONG, &snapshot,
													 &token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cluster_control_root_revalidate(&token, &image.records[0].identity, &snapshot),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(
		cluster_control_root_lookup_owner_by_node_runtime(0, &identity, &snapshot, &lookup_token),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(cluster_control_root_identity_equal(&identity, &image.records[0].identity));
	UT_ASSERT_EQ(lookup_token.file_txn_seq, token.file_txn_seq);
}

UT_TEST(test_lifecycle_publish_exact_token_cas)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootSnapshot published;
	ClusterControlRootReadToken read_token;
	ClusterControlRootReadToken new_token;
	ClusterControlRootPatch patch;

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &file_token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &image.records[0].identity,
													 CLUSTER_CONTROL_ROOT_READ_STRONG, &snapshot,
													 &read_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	memset(&patch, 0, sizeof(patch));
	patch.mask = CLUSTER_CONTROL_ROOT_PATCH_LIFECYCLE;
	patch.expected_lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	patch.desired.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RETIRED;
	UT_ASSERT_EQ(cluster_control_root_compare_and_publish(
					 &read_token, &patch, CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_RETIRE, &published,
					 &new_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(published.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_RETIRED);
	UT_ASSERT_EQ(published.root_publish_seq, snapshot.root_publish_seq + 1);
	UT_ASSERT_EQ(new_token.file_txn_seq, read_token.file_txn_seq + 1);
	UT_ASSERT_EQ(cluster_control_root_compare_and_publish(
					 &read_token, &patch, CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_RETIRE, &published,
					 &new_token),
				 CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT_EQ(test_walr_begin_calls, 0);
	UT_ASSERT_EQ(test_walr_end_calls, 0);
}

UT_TEST(test_retention_expanding_publish_refuses_before_cf_without_walr)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootSnapshot published;
	ClusterControlRootReadToken read_token;
	ClusterControlRootReadToken new_token;
	ClusterControlRootPatch patch;

	wipe_root_files();
	build_migration(&image, &round);
	image.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &file_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &image.records[0].identity,
													 CLUSTER_CONTROL_ROOT_READ_STRONG, &snapshot,
													 &read_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	build_owner_rejoin_patch(&snapshot, snapshot.identity.origin_owner_incarnation + 1,
							 snapshot.identity.root_lineage_seq + 1, &patch);
	test_cf_lock_calls = 0;
	test_durable_rename_calls = 0;
	test_walr_begin_result = CLUSTER_WAL_PIN_UNAVAILABLE;
	memset(&published, 0xee, sizeof(published));
	memset(&new_token, 0xee, sizeof(new_token));
	UT_ASSERT_EQ(cluster_control_root_compare_and_publish(&read_token, &patch,
														  CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN,
														  &published, &new_token),
				 CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	UT_ASSERT_EQ(test_walr_begin_calls, 1);
	UT_ASSERT_EQ(test_walr_thread, 1);
	UT_ASSERT_EQ(test_walr_end_calls, 0);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
	UT_ASSERT_EQ(test_durable_rename_calls, 0);
	UT_ASSERT_EQ(published.identity.system_identifier, 0);
	UT_ASSERT_EQ(new_token.file_txn_seq, 0);
}

UT_TEST(test_retention_expanding_publish_holds_walr_around_cf_and_readback)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootSnapshot published;
	ClusterControlRootReadToken read_token;
	ClusterControlRootReadToken new_token;
	ClusterControlRootPatch patch;

	wipe_root_files();
	build_migration(&image, &round);
	image.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &file_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &image.records[0].identity,
													 CLUSTER_CONTROL_ROOT_READ_STRONG, &snapshot,
													 &read_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	build_owner_rejoin_patch(&snapshot, snapshot.identity.origin_owner_incarnation + 1,
							 snapshot.identity.root_lineage_seq + 1, &patch);
	test_order_seq = 0;
	test_walr_begin_order = 0;
	test_cf_acquire_order = 0;
	test_cf_release_order = 0;
	test_last_rename_order = 0;
	UT_ASSERT_EQ(cluster_control_root_compare_and_publish(&read_token, &patch,
														  CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN,
														  &published, &new_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(test_walr_begin_calls, 1);
	UT_ASSERT_EQ(test_walr_end_calls, 1);
	UT_ASSERT(test_walr_begin_order < test_cf_acquire_order);
	UT_ASSERT(test_cf_acquire_order < test_last_rename_order);
	UT_ASSERT(test_last_rename_order < test_cf_release_order);
	UT_ASSERT(test_cf_release_order < test_order_seq);
}

UT_TEST(test_unbound_publisher_fails_before_cf_and_preserves_root)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot before;
	ClusterControlRootSnapshot after;
	ClusterControlRootSnapshot published;
	ClusterControlRootReadToken before_token;
	ClusterControlRootReadToken after_token;
	ClusterControlRootReadToken published_token;
	ClusterControlRootPatch patch;
	int lock_calls_before_publish;

	wipe_root_files();
	test_publish_authorized = true;
	UT_ASSERT_EQ(create_prepared(&image, &round, &file_token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &image.records[0].identity,
													 CLUSTER_CONTROL_ROOT_READ_STRONG, &before,
													 &before_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	memset(&patch, 0, sizeof(patch));
	patch.mask = CLUSTER_CONTROL_ROOT_PATCH_LIFECYCLE;
	patch.expected_lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	patch.desired.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RETIRED;
	memset(&published, 0xee, sizeof(published));
	memset(&published_token, 0xee, sizeof(published_token));
	lock_calls_before_publish = test_cf_lock_calls;
	test_publish_authorized = false;
	UT_ASSERT_EQ(cluster_control_root_compare_and_publish(
					 &before_token, &patch, CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_RETIRE, &published,
					 &published_token),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(test_cf_lock_calls, lock_calls_before_publish);
	UT_ASSERT_EQ(published.identity.system_identifier, 0);
	UT_ASSERT_EQ(published_token.file_txn_seq, 0);

	test_publish_authorized = true;
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &image.records[0].identity,
													 CLUSTER_CONTROL_ROOT_READ_STRONG, &after,
													 &after_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(after.lifecycle, before.lifecycle);
	UT_ASSERT_EQ(after.root_publish_seq, before.root_publish_seq);
	UT_ASSERT_EQ(after_token.file_txn_seq, before_token.file_txn_seq);
}

UT_TEST(test_owner_rejoin_rejects_non_new_incarnation)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootSnapshot published;
	ClusterControlRootReadToken read_token;
	ClusterControlRootReadToken new_token;
	ClusterControlRootPatch patch;

	wipe_root_files();
	build_migration(&image, &round);
	image.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &file_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &image.records[0].identity,
													 CLUSTER_CONTROL_ROOT_READ_STRONG, &snapshot,
													 &read_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	memset(&patch, 0, sizeof(patch));
	patch.mask = UINT64_C(0x3b);
	patch.expected_lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	patch.desired.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	patch.desired.identity.origin_owner_incarnation = snapshot.identity.origin_owner_incarnation;
	patch.desired.identity.root_lineage_seq = snapshot.identity.root_lineage_seq + 1;
	patch.desired.root_flags
		= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID;
	patch.desired.checkpoint_tli = snapshot.checkpoint_tli;
	patch.desired.checkpoint_source_kind = snapshot.checkpoint_source_kind;
	patch.desired.checkpoint_lower_lsn = snapshot.checkpoint_lower_lsn;
	patch.desired.checkpoint_record_crc32c = snapshot.checkpoint_record_crc32c;
	patch.desired.recovered_through_lsn_exclusive = snapshot.checkpoint_lower_lsn;
	memset(&published, 0xee, sizeof(published));
	memset(&new_token, 0xee, sizeof(new_token));
	UT_ASSERT_EQ(cluster_control_root_compare_and_publish(&read_token, &patch,
														  CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN,
														  &published, &new_token),
				 CLUSTER_CONTROL_ROOT_CAS_CONFLICT);
	UT_ASSERT_EQ(published.identity.system_identifier, 0);
	UT_ASSERT_EQ(new_token.file_txn_seq, 0);
}

UT_TEST(test_owner_rejoin_advances_exact_lineage_and_exhausts_at_max)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootIdentity identity;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootSnapshot published;
	ClusterControlRootReadToken read_token;
	ClusterControlRootReadToken new_token;
	ClusterControlRootPatch patch;
	uint64 new_incarnation;

	wipe_root_files();
	build_migration(&image, &round);
	image.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &file_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(
		cluster_control_root_lookup_owner_by_node_runtime(0, &identity, &snapshot, &read_token),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	new_incarnation = snapshot.identity.origin_owner_incarnation + 1;
	build_owner_rejoin_patch(&snapshot, new_incarnation, snapshot.identity.root_lineage_seq + 1,
							 &patch);
	UT_ASSERT_EQ(cluster_control_root_compare_and_publish(&read_token, &patch,
														  CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN,
														  &published, &new_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(published.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN);
	UT_ASSERT_EQ(published.identity.origin_owner_incarnation, new_incarnation);
	UT_ASSERT_EQ(published.identity.root_lineage_seq, 2);

	/* Rebuild an otherwise valid RECOVERY_COMPLETE root at the terminal
	 * lineage.  OWNER_REJOIN must fail closed; UINT64_MAX never wraps. */
	wipe_root_files();
	build_migration(&image, &round);
	image.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &file_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	force_first_record_lineage(UINT64_MAX);
	UT_ASSERT_EQ(
		cluster_control_root_lookup_owner_by_node_runtime(0, &identity, &snapshot, &read_token),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(snapshot.identity.root_lineage_seq, UINT64_MAX);
	build_owner_rejoin_patch(&snapshot, snapshot.identity.origin_owner_incarnation + 1, UINT64_C(1),
							 &patch);
	memset(&published, 0xee, sizeof(published));
	memset(&new_token, 0xee, sizeof(new_token));
	UT_ASSERT_EQ(cluster_control_root_compare_and_publish(&read_token, &patch,
														  CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN,
														  &published, &new_token),
				 CLUSTER_CONTROL_ROOT_CAS_CONFLICT);
	UT_ASSERT_EQ(published.identity.system_identifier, 0);
	UT_ASSERT_EQ(new_token.file_txn_seq, 0);
}

UT_TEST(test_lifecycle_frozen_shape_matrix)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootIdentity identity;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootSnapshot published;
	ClusterControlRootReadToken read_token;
	ClusterControlRootReadToken new_token;
	ClusterControlRootPatch patch;
	uint64 new_incarnation;

	/* ① OWNER_REJOIN from RECOVERY_COMPLETE -> OPEN succeeds (frozen
	 * crash-rejoin mainline). */
	wipe_root_files();
	build_migration(&image, &round);
	image.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &file_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(
		cluster_control_root_lookup_owner_by_node_runtime(0, &identity, &snapshot, &read_token),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	new_incarnation = snapshot.identity.origin_owner_incarnation + 1;
	build_owner_rejoin_patch(&snapshot, new_incarnation, snapshot.identity.root_lineage_seq + 1,
							 &patch);
	memset(&published, 0xee, sizeof(published));
	memset(&new_token, 0xee, sizeof(new_token));
	UT_ASSERT_EQ(cluster_control_root_compare_and_publish(&read_token, &patch,
														  CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN,
														  &published, &new_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(published.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN);
	UT_ASSERT_EQ(published.identity.origin_owner_incarnation, new_incarnation);
	UT_ASSERT_EQ(published.identity.root_lineage_seq, snapshot.identity.root_lineage_seq + 1);

	/* ② OWNER_REJOIN from OPEN is rejected by patch_shape_valid BEFORE any
	 * CF / file I/O (STOP-02 §17.4: pre-lifecycle must be
	 * RECOVERY_COMPLETE). */
	wipe_root_files();
	build_migration(&image, &round);
	image.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &file_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(
		cluster_control_root_lookup_owner_by_node_runtime(0, &identity, &snapshot, &read_token),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	build_owner_rejoin_patch(&snapshot, snapshot.identity.origin_owner_incarnation + 1,
							 snapshot.identity.root_lineage_seq + 1, &patch);
	patch.expected_lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	test_cf_lock_calls = 0;
	test_durable_rename_calls = 0;
	memset(&published, 0xee, sizeof(published));
	memset(&new_token, 0xee, sizeof(new_token));
	UT_ASSERT_EQ(cluster_control_root_compare_and_publish(&read_token, &patch,
														  CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN,
														  &published, &new_token),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(published.identity.system_identifier, 0);
	UT_ASSERT_EQ(new_token.file_txn_seq, 0);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
	UT_ASSERT_EQ(test_durable_rename_calls, 0);

	/* ②' OWNER_REJOIN from CLOSED is rejected the same way: the
	 * clean-reopen mainline is THREAD_OPEN (CLOSED -> OPEN), never the
	 * OWNER_REJOIN CAS (increment-13 allowance removed). */
	wipe_root_files();
	build_migration(&image, &round);
	image.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &file_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(
		cluster_control_root_lookup_owner_by_node_runtime(0, &identity, &snapshot, &read_token),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	build_owner_rejoin_patch(&snapshot, snapshot.identity.origin_owner_incarnation + 1,
							 snapshot.identity.root_lineage_seq + 1, &patch);
	patch.expected_lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	test_cf_lock_calls = 0;
	test_durable_rename_calls = 0;
	memset(&published, 0xee, sizeof(published));
	memset(&new_token, 0xee, sizeof(new_token));
	UT_ASSERT_EQ(cluster_control_root_compare_and_publish(&read_token, &patch,
														  CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN,
														  &published, &new_token),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(published.identity.system_identifier, 0);
	UT_ASSERT_EQ(new_token.file_txn_seq, 0);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
	UT_ASSERT_EQ(test_durable_rename_calls, 0);

	/* ③ THREAD_OPEN CLOSED -> OPEN succeeds with owner re-stamp +
	 * lineage+1 (the frozen clean-reopen mainline). */
	wipe_root_files();
	build_migration(&image, &round);
	image.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &file_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(
		cluster_control_root_lookup_owner_by_node_runtime(0, &identity, &snapshot, &read_token),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	new_incarnation = snapshot.identity.origin_owner_incarnation + 1;
	memset(&patch, 0, sizeof(patch));
	patch.mask = UINT64_C(0x3b);
	patch.expected_lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	patch.desired.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	patch.desired.identity.origin_owner_incarnation = new_incarnation;
	patch.desired.identity.root_lineage_seq = snapshot.identity.root_lineage_seq + 1;
	patch.desired.root_flags = snapshot.root_flags;
	patch.desired.checkpoint_tli = snapshot.checkpoint_tli;
	patch.desired.checkpoint_source_kind = snapshot.checkpoint_source_kind;
	patch.desired.checkpoint_lower_lsn = snapshot.checkpoint_lower_lsn;
	patch.desired.checkpoint_record_crc32c = snapshot.checkpoint_record_crc32c;
	patch.desired.tail_tli = snapshot.tail_tli;
	patch.desired.tail_validation_kind = snapshot.tail_validation_kind;
	patch.desired.validated_tail_lsn_exclusive = snapshot.validated_tail_lsn_exclusive;
	patch.desired.tail_last_record_lsn = snapshot.tail_last_record_lsn;
	patch.desired.tail_last_record_crc32c = snapshot.tail_last_record_crc32c;
	patch.desired.recovered_tli = snapshot.recovered_tli;
	patch.desired.recovered_through_lsn_exclusive = snapshot.recovered_through_lsn_exclusive;
	patch.desired.recovered_last_record_lsn = snapshot.recovered_last_record_lsn;
	patch.desired.recovered_last_record_crc32c = snapshot.recovered_last_record_crc32c;
	memset(&published, 0xee, sizeof(published));
	memset(&new_token, 0xee, sizeof(new_token));
	UT_ASSERT_EQ(cluster_control_root_compare_and_publish(&read_token, &patch,
														  CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_OPEN,
														  &published, &new_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(published.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN);
	UT_ASSERT_EQ(published.identity.origin_owner_incarnation, new_incarnation);
	UT_ASSERT_EQ(published.identity.root_lineage_seq, snapshot.identity.root_lineage_seq + 1);
}

UT_TEST(test_initial_migration_requires_lineage_one)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken token;

	wipe_root_files();
	build_migration(&image, &round);
	image.records[0].identity.root_lineage_seq = 2;
	memset(&token, 0xee, sizeof(token));
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &token),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(token.file_txn_seq, 0);
}

UT_TEST(test_unconfirmed_release_returns_no_authority)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken token;

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &file_token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	test_cf_release_confirmed = false;
	memset(&snapshot, 0xee, sizeof(snapshot));
	memset(&token, 0xee, sizeof(token));
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &image.records[0].identity,
													 CLUSTER_CONTROL_ROOT_READ_STRONG, &snapshot,
													 &token),
				 CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN);
	UT_ASSERT_EQ(snapshot.identity.system_identifier, 0);
	UT_ASSERT_EQ(token.file_txn_seq, 0);
}

UT_TEST(test_primary_rename_failure_is_not_success)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken token;
	char path[MAXPGPATH];
	struct stat st;

	wipe_root_files();
	test_fail_primary_rename = true;
	memset(&token, 0xee, sizeof(token));
	UT_ASSERT_EQ(create_prepared(&image, &round, &token), CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT_EQ(token.file_txn_seq, 0);
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	UT_ASSERT(lstat(path, &st) != 0 && errno == ENOENT);
}

UT_TEST(test_reserved_bytes_and_symlink_fail_closed)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken token;
	ClusterControlRootSnapshot snapshot;
	uint8 bytes[CLUSTER_CONTROL_ROOT_FILE_BYTES];
	char primary[MAXPGPATH];
	char bak[MAXPGPATH];

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	path_for(primary, sizeof(primary), CLUSTER_CONTROL_ROOT_REL_PATH);
	path_for(bak, sizeof(bak), CLUSTER_CONTROL_ROOT_BAK_REL_PATH);
	read_all_or_abort(primary, bytes, sizeof(bytes));
	bytes[196] = 1;
	put_u32_le(bytes + 504, image_crc(bytes, 504));
	write_all_or_abort(primary, bytes, sizeof(bytes));
	unlink(bak);
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &image.records[0].identity,
													 CLUSTER_CONTROL_ROOT_READ_STRONG, &snapshot,
													 NULL),
				 CLUSTER_CONTROL_ROOT_BAD_RESERVED);

	wipe_root_files();
	UT_ASSERT_EQ(symlink("/tmp/foreign-pgrac-root", primary), 0);
	UT_ASSERT_EQ(create_prepared(&image, &round, &token), CLUSTER_CONTROL_ROOT_IO_ERROR);
}

/* PGRAC: root-v2 fixtures are independently assembled bytes.  In particular,
 * no production encoder computes the expected image or its field offsets.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static const uint8 v2_storage[16] = { 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
									  0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff };

static bool
v2_zero(const void *ptr, size_t len)
{
	const uint8 *p = ptr;
	size_t i;

	for (i = 0; i < len; ++i)
		if (p[i] != 0)
			return false;
	return true;
}

static void
v2_root_checksums(uint8 *bytes)
{
	put_u32_le(bytes + 96, image_crc(bytes + 512, 65536));
	put_u32_le(bytes + 504, image_crc(bytes, 504));
}

static void
v2_checksums(uint8 *bytes)
{
	int i;

	for (i = 0; i < 128; ++i) {
		uint8 *record = bytes + 512 + 512 * i;

		if (!v2_zero(record, 512))
			put_u32_le(record + 504, image_crc(record, 504));
	}
	v2_root_checksums(bytes);
}

static void
v2_fixture(uint8 bytes[66048])
{
	int node;

	memset(bytes, 0, 66048);
	memcpy(bytes, "PGCH", 4);
	put_u16_le(bytes + 4, 2);
	put_u16_le(bytes + 6, 512);
	put_u16_le(bytes + 8, 512);
	put_u16_le(bytes + 10, 128);
	put_u32_le(bytes + 12, UINT32_C(0x01020304));
	put_u64_le(bytes + 16, 7);
	put_u64_le(bytes + 24, TEST_SYSID);
	memcpy(bytes + 32, v2_storage, 16);
	memset(bytes + 48, 0xab, 16);
	bytes[54] = 0x4b;
	bytes[56] = 0x8b;
	put_u64_le(bytes + 64, 0x0d);
	put_u16_le(bytes + 72, 2);
	put_u16_le(bytes + 74, 2);
	put_u32_le(bytes + 76, 2);
	put_u64_le(bytes + 80, 111);
	put_u64_le(bytes + 88, 222);
	memset(bytes + 100, 0x11, 32);
	memset(bytes + 132, 0x22, 32);
	put_u64_le(bytes + 164, 3);
	put_u64_le(bytes + 172, 4);
	put_u64_le(bytes + 180, 1);
	put_u64_le(bytes + 188, UINT64_C(0x400001));
	put_u32_le(bytes + 196, 3);
	put_u64_le(bytes + 200, 41);
	put_u64_le(bytes + 208, 43);
	put_u64_le(bytes + 216, 1);
	put_u64_le(bytes + 224, UINT64_C(1) << 63);
	put_u64_le(bytes + 232, 1);
	put_u64_le(bytes + 240, UINT64_C(1) << 63);
	put_u64_le(bytes + 248, 47);
	memset(bytes + 256, 0x33, 32);
	put_u64_le(bytes + 288, 53);
	memset(bytes + 296, 0x44, 32);
	put_u64_le(bytes + 328, 59);
	put_u64_le(bytes + 336, 61);
	memset(bytes + 344, 0x55, 32);
	for (node = 0; node <= 127; node += 127) {
		uint8 *r = bytes + 512 + node * 512;

		memcpy(r, "PGRT", 4);
		put_u16_le(r + 4, 2);
		put_u16_le(r + 6, 512);
		put_u16_le(r + 8, node + 1);
		r[10] = 1;
		put_u32_le(r + 12, node);
		put_u64_le(r + 16, 10 + node);
		put_u64_le(r + 24, 11 + node);
		put_u64_le(r + 32, TEST_SYSID);
		memcpy(r + 40, bytes + 32, 32);
		put_u64_le(r + 72, 12345 + node);
		put_u64_le(r + 80, 99 + node);
		put_u32_le(r + 96, 1);
		put_u32_le(r + 108, 5);
		put_u64_le(r + 112, UINT64_C(0x1000000) + node * 4096);
		put_u64_le(r + 144, 777);
		put_u32_le(r + 152, 2);
		put_u32_le(r + 156, 11);
		put_u64_le(r + 160, 333);
		put_u32_le(r + 168, 1233 + node);
		put_u32_le(r + 172, 1234 + node);
		put_u16_le(r + 194, 2);
		if (node == 127) {
			put_u64_le(r + 216, 44);
			memset(r + 224, 0x66, 32);
		}
		put_u64_le(r + 256, 66 + node);
		memset(r + 264, 0x77, 32);
		memset(r + 296, 0x88, 32);
	}
	v2_checksums(bytes);
}

/* PGRAC: literal retained-input bytes, not a codec-generated expectation.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static size_t
history_fixture(uint8 bytes[65604], ControlRootImage *root, uint32 node, uint32 count)
{
	uint8 raw_root[66048];
	size_t length = 64 + count * 512 + 4;

	v2_fixture(raw_root);
	if (cluster_control_root_v2_decode(raw_root, sizeof(raw_root), v2_storage, TEST_SYSID, root))
		abort();
	memset(bytes, 0, 65604);
	memcpy(bytes, "PGWH", 4);
	put_u16_le(bytes + 4, 1);
	put_u16_le(bytes + 6, 64);
	put_u32_le(bytes + 8, count);
	put_u32_le(bytes + 12, 512);
	put_u64_le(bytes + 16, count * 512);
	put_u64_le(bytes + 24, TEST_SYSID);
	memcpy(bytes + 32, v2_storage, 16);
	memcpy(bytes + 48, raw_root + 48, 16);
	for (uint32 i = 0; i < count; i++) {
		uint8 *record = bytes + 64 + i * 512;
		memcpy(record, raw_root + 512 + node * 512, 512);
		/* Deliberately unlike the current writer; no numeric age inference. */
		put_u64_le(record + 80, 1000 + i);
		put_u64_le(record + 24, 2000 + i);
		put_u64_le(record + 72, 3000 + i);
		put_u32_le(record + 168, 4000 + i);
		record[10] = 1 + i % 5;
		memset(record + 216, 0, 40);
		put_u32_le(record + 504, image_crc(record, 504));
	}
	put_u32_le(bytes + length - 4, image_crc(bytes, length - 4));
	root->refs[node].history_generation = 123;
	sha256_bytes(bytes, length, root->refs[node].history_sha256);
	return length;
}

static void
history_outer_checksum(uint8 *bytes, size_t len, ControlRootImage *root, uint32 node)
{
	put_u32_le(bytes + len - 4, image_crc(bytes, len - 4));
	sha256_bytes(bytes, len, root->refs[node].history_sha256);
}

static ClusterControlRootResult
history_refused(const uint8 *bytes, size_t len, const ControlRootImage *root, uint32 node)
{
	ClusterWalHistoryImage out;
	ClusterControlRootResult result;
	memset(&out, 0xa5, sizeof(out));
	result = cluster_control_root_v2_history_decode(bytes, len, root, node, &out);
	UT_ASSERT(result != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	return result;
}

UT_TEST(test_history_exact_empty_and_full_set_preserves_inputs)
{
	uint8 bytes[65604];
	ControlRootImage root;
	ClusterWalHistoryImage out;
	const uint32 counts[] = { 0, 2, 128 };
	for (uint32 node = 0; node <= 127; node += 127)
		for (size_t c = 0; c < lengthof(counts); c++) {
			uint8 hash[32];
			size_t len = history_fixture(bytes, &root, node, counts[c]);
			ClusterControlRootResult result
				= cluster_control_root_v2_history_decode(bytes, len, &root, node, &out);
			UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
			if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
				continue;
			UT_ASSERT_EQ(out.count, counts[c]);
			for (uint32 i = 0; i < out.count; i++) {
				const ClusterWalHistoryRecord *record = &out.records[i];
				UT_ASSERT_EQ(record->snapshot.identity.origin_node_id, node);
				UT_ASSERT_EQ(record->snapshot.identity.origin_thread_id, node + 1);
				UT_ASSERT_EQ(record->snapshot.identity.origin_owner_incarnation, 1000 + i);
				UT_ASSERT_EQ(record->snapshot.identity.root_lineage_seq, 2000 + i);
				UT_ASSERT_EQ(record->snapshot.identity.thread_claim_created_at, 3000 + i);
				UT_ASSERT_EQ(record->snapshot.identity.thread_claim_crc32c, 4000 + i);
				UT_ASSERT_EQ(record->snapshot.lifecycle, 1 + i % 5);
				UT_ASSERT_EQ(record->snapshot.checkpoint_lower_lsn,
							 UINT64_C(0x1000000) + node * 4096);
				UT_ASSERT_EQ(record->publisher_incarnation, 777);
				UT_ASSERT_EQ(record->publisher_node, 2);
				UT_ASSERT_EQ(record->refs.anchor_generation, 66 + node);
				UT_ASSERT_EQ(record->refs.history_generation, 0);
				UT_ASSERT(memcmp(record->refs.anchor_sha256, bytes + 64 + i * 512 + 264, 32) == 0);
				UT_ASSERT(memcmp(record->refs.claim_sha256, bytes + 64 + i * 512 + 296, 32) == 0);
				UT_ASSERT_EQ(record->record_crc32c, image_crc(bytes + 64 + i * 512, 504));
			}
			UT_ASSERT(v2_zero(&out.records[counts[c]], (128 - counts[c]) * sizeof(out.records[0])));
			/* Exact hash remains bound; decoder does not canonicalize inputs. */
			sha256_bytes(bytes, len, hash);
			UT_ASSERT(memcmp(hash, root.refs[node].history_sha256, 32) == 0);
		}
}

UT_TEST(test_history_rejects_outer_shape_crc_hash_and_identity)
{
	uint8 bytes[65604];
	ControlRootImage root;
	size_t len;
	for (int fault = 0; fault < 13; fault++) {
		size_t case_len = history_fixture(bytes, &root, 0, 2);
		ClusterControlRootResult expected = CLUSTER_CONTROL_ROOT_BAD_SIZE;
		switch (fault) {
		case 0:
			bytes[0] ^= 1;
			expected = CLUSTER_CONTROL_ROOT_BAD_MAGIC;
			break;
		case 1:
			put_u16_le(bytes + 4, 2);
			expected = CLUSTER_CONTROL_ROOT_BAD_VERSION;
			break;
		case 2:
			put_u16_le(bytes + 6, 63);
			break;
		case 3:
			put_u32_le(bytes + 8, 129);
			break;
		case 4:
			put_u32_le(bytes + 8, UINT32_MAX);
			break;
		case 5:
			put_u32_le(bytes + 12, 511);
			break;
		case 6:
			put_u64_le(bytes + 16, 1023);
			break;
		case 7:
			bytes[24] ^= 1;
			expected = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
			break;
		case 8:
			bytes[32] ^= 1;
			expected = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
			break;
		case 9:
			bytes[48] ^= 1;
			expected = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
			break;
		case 10:
			case_len--;
			break;
		case 11:
			case_len++;
			break;
		case 12:
			expected = CLUSTER_CONTROL_ROOT_BAD_BODY_CRC;
			break;
		}
		history_outer_checksum(bytes, case_len, &root, 0);
		if (fault == 12) {
			bytes[case_len - 1] ^= 1;
			sha256_bytes(bytes, case_len, root.refs[0].history_sha256);
		}
		UT_ASSERT_EQ(history_refused(bytes, case_len, &root, 0), expected);
	}
	len = history_fixture(bytes, &root, 0, 2);
	root.refs[0].history_sha256[0] ^= 1;
	UT_ASSERT_EQ(history_refused(bytes, len, &root, 0), CLUSTER_CONTROL_ROOT_HASH_MISMATCH);
}

UT_TEST(test_history_rejects_bad_records_and_namespace_aliases)
{
	uint8 bytes[65604];
	ControlRootImage root;
	for (int fault = 0; fault < 14; fault++) {
		size_t len = history_fixture(bytes, &root, 0, 2);
		uint8 *record = bytes + 64 + 512;
		ClusterControlRootResult expected = CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		switch (fault) {
		case 0:
			record[0] ^= 1;
			expected = CLUSTER_CONTROL_ROOT_BAD_MAGIC;
			break;
		case 1:
			put_u16_le(record + 4, 1);
			expected = CLUSTER_CONTROL_ROOT_BAD_VERSION;
			break;
		case 2:
			record[328] = 1;
			expected = CLUSTER_CONTROL_ROOT_BAD_RESERVED;
			break;
		case 3:
			put_u16_le(record + 8, 2);
			expected = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
			break;
		case 4:
			put_u32_le(record + 12, 1);
			expected = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
			break;
		case 5:
			put_u64_le(record + 32, TEST_SYSID + 1);
			expected = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
			break;
		case 6:
			put_u64_le(record + 80, 1000);
			break; /* Different claim, same namespace. */
		case 7:
			put_u64_le(record + 80, 999);
			break; /* Not canonical order. */
		case 8:
			put_u64_le(record + 80, 99);
			expected = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
			break;
		case 9:
			put_u64_le(record + 216, 45);
			memset(record + 224, 1, 32);
			break;
		case 10:
			put_u64_le(record + 144, 0);
			expected = CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
			break;
		case 11:
			put_u64_le(record + 112, 0);
			break;
		case 12:
			expected = CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC;
			break;
		case 13:
			memset(record, 0, 512);
			break;
		}
		if (fault != 13)
			put_u32_le(record + 504, image_crc(record, 504));
		if (fault == 12)
			record[504] ^= 1;
		history_outer_checksum(bytes, len, &root, 0);
		UT_ASSERT_EQ(history_refused(bytes, len, &root, 0), expected);
	}
}

UT_TEST(test_history_refuses_unselected_and_invalid_arguments)
{
	uint8 bytes[65604];
	ControlRootImage root;
	size_t len = history_fixture(bytes, &root, 0, 2);
	const size_t sizes[] = { 0, 4, 63, 67, 65605, SIZE_MAX };
	UT_ASSERT_EQ(history_refused(NULL, len, &root, 0), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(history_refused(bytes, len, NULL, 0), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(history_refused(bytes, len, &root, 128), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_control_root_v2_history_decode(bytes, len, &root, 0, NULL),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	for (size_t i = 0; i < lengthof(sizes); i++)
		UT_ASSERT_EQ(history_refused(bytes, sizes[i], &root, 0), CLUSTER_CONTROL_ROOT_BAD_SIZE);
	root.present[0] = false;
	UT_ASSERT_EQ(history_refused(bytes, len, &root, 0), CLUSTER_CONTROL_ROOT_ABSENT);
	root.present[0] = true;
	root.refs[0].history_generation = 0;
	UT_ASSERT_EQ(history_refused(bytes, len, &root, 0), CLUSTER_CONTROL_ROOT_RANGE_INVALID);
	memset(root.refs[0].history_sha256, 0, 32);
	UT_ASSERT_EQ(history_refused(bytes, len, &root, 0), CLUSTER_CONTROL_ROOT_ABSENT);
	len = history_fixture(bytes, &root, 0, 2);
	root.header.format_version = 1;
	UT_ASSERT_EQ(history_refused(bytes, len, &root, 0), CLUSTER_CONTROL_ROOT_BAD_VERSION);
}

UT_TEST(test_history_refuses_alias_before_clearing_output)
{
	union {
		uint8 bytes[65604];
		ControlRootImage root;
		ClusterWalHistoryImage out;
	} storage;
	uint8 bytes[65604];
	ControlRootImage root;
	size_t len = history_fixture(storage.bytes, &root, 0, 2);
	UT_ASSERT_EQ(cluster_control_root_v2_history_decode(storage.bytes, len, &root, 0, &storage.out),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(v2_zero(&storage.out, sizeof(storage.out)));
	len = history_fixture(bytes, &storage.root, 0, 2);
	UT_ASSERT_EQ(cluster_control_root_v2_history_decode(bytes, len, &storage.root, 0, &storage.out),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(v2_zero(&storage.out, sizeof(storage.out)));
}

UT_TEST(test_v2_decodes_exact_common_and_two_thread_fields)
{
	uint8 bytes[66048];
	ControlRootImage out;
	int node;

	v2_fixture(bytes);
	UT_ASSERT_EQ(cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(out.header.format_version, 2);
	UT_ASSERT_EQ(out.header.file_txn_seq, 7);
	UT_ASSERT_EQ(out.header.v2.database_state, CLUSTER_CONTROL_ROOT_DATABASE_OPEN);
	UT_ASSERT_EQ(out.header.v2.database_incarnation, 41);
	UT_ASSERT_EQ(out.header.v2.formation_seq, 43);
	UT_ASSERT_EQ(out.header.v2.configured[0], 1);
	UT_ASSERT_EQ(out.header.v2.configured[1], UINT64_C(1) << 63);
	UT_ASSERT_EQ(out.header.v2.serving[0], 1);
	UT_ASSERT_EQ(out.header.v2.serving[1], UINT64_C(1) << 63);
	UT_ASSERT_EQ(out.header.v2.config_generation, 47);
	UT_ASSERT_EQ(out.header.v2.control_image_generation, 53);
	UT_ASSERT_EQ(out.header.v2.catalog_manifest_generation, 59);
	UT_ASSERT_EQ(out.header.v2.global_scn_high_water, 61);
	UT_ASSERT(memcmp(out.header.v2.config_sha256, bytes + 256, 32) == 0);
	UT_ASSERT(memcmp(out.header.v2.control_image_sha256, bytes + 296, 32) == 0);
	UT_ASSERT(memcmp(out.header.v2.catalog_manifest_sha256, bytes + 344, 32) == 0);
	for (node = 0; node <= 127; node += 127) {
		UT_ASSERT(out.present[node]);
		UT_ASSERT_EQ(out.records[node].identity.origin_thread_id, node + 1);
		UT_ASSERT_EQ(out.records[node].identity.origin_node_id, node);
		UT_ASSERT_EQ(out.records[node].identity.origin_owner_incarnation, 99 + node);
		UT_ASSERT_EQ(out.records[node].checkpoint_lower_lsn, UINT64_C(0x1000000) + node * 4096);
		UT_ASSERT_EQ(out.publisher_incarnation[node], 777);
		UT_ASSERT_EQ(out.publisher_node[node], 2);
		UT_ASSERT_EQ(out.refs[node].anchor_generation, 66 + node);
		UT_ASSERT(memcmp(out.refs[node].anchor_sha256, bytes + 512 + node * 512 + 264, 32) == 0);
		UT_ASSERT(memcmp(out.refs[node].claim_sha256, bytes + 512 + node * 512 + 296, 32) == 0);
	}
	UT_ASSERT_EQ(out.refs[127].history_generation, 44);
	UT_ASSERT(memcmp(out.refs[127].history_sha256, bytes + 512 + 127 * 512 + 224, 32) == 0);
	UT_ASSERT(v2_zero(&out.refs[0].history_sha256, 32));
	UT_ASSERT_EQ(out.refs[0].history_generation, 0);
	UT_ASSERT(!out.present[1] && v2_zero(&out.records[1], sizeof(out.records[1])));
	UT_ASSERT(memcmp(out.bytes, bytes, sizeof(bytes)) == 0);
}

UT_TEST(test_v2_encoder_preserves_exact_bytes_and_publishers)
{
	uint8 bytes[66048];
	ControlRootImage out;

	v2_fixture(bytes);
	UT_ASSERT_EQ(cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	memset(out.bytes, 0xee, sizeof(out.bytes));
	UT_ASSERT_EQ(cluster_control_root_v2_encode(&out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(memcmp(out.bytes, bytes, sizeof(bytes)) == 0);
	out.header.v2.config_generation = 48;
	put_u64_le(bytes + 248, 48);
	v2_checksums(bytes);
	UT_ASSERT_EQ(cluster_control_root_v2_encode(&out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(memcmp(out.bytes, bytes, sizeof(bytes)) == 0);
}

UT_TEST(test_v2_versions_are_independent_and_strict)
{
	static const size_t offsets[] = { 4, 72, 74, 512 + 4, 512 + 127 * 512 + 4 };
	uint8 bytes[66048];
	ControlRootImage out;
	size_t i;
	uint16 version;

	for (i = 0; i < lengthof(offsets); ++i)
		for (version = 1; version <= 3; version += 2) {
			v2_fixture(bytes);
			put_u16_le(bytes + offsets[i], version);
			v2_checksums(bytes);
			memset(&out, 0xee, sizeof(out));
			UT_ASSERT_EQ(
				cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
				CLUSTER_CONTROL_ROOT_BAD_VERSION);
			UT_ASSERT(v2_zero(&out, sizeof(out)));
		}
}

UT_TEST(test_v2_checks_all_three_crc_layers)
{
	uint8 bytes[66048];
	ControlRootImage out;
	int i;
	const ClusterControlRootResult expected[]
		= { CLUSTER_CONTROL_ROOT_BAD_HEADER_CRC, CLUSTER_CONTROL_ROOT_BAD_BODY_CRC,
			CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC };

	for (i = 0; i < 3; ++i) {
		v2_fixture(bytes);
		bytes[i == 0 ? 196 : 512 + 216] ^= 1;
		if (i == 2)
			v2_root_checksums(bytes);
		memset(&out, 0xee, sizeof(out));
		UT_ASSERT_EQ(
			cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
			expected[i]);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
	}
}

UT_TEST(test_v2_reserved_bytes_and_partial_holes_are_rejected)
{
	static const size_t offsets[]
		= { 376,	   503,		  508,		 511,		512 + 11,  512 + 88, 512 + 95,
			512 + 198, 512 + 207, 512 + 328, 512 + 503, 512 + 508, 512 + 511 };
	uint8 bytes[66048];
	ControlRootImage out;
	size_t i;

	for (i = 0; i < lengthof(offsets); ++i) {
		v2_fixture(bytes);
		bytes[offsets[i]] = 1;
		v2_checksums(bytes);
		UT_ASSERT_EQ(
			cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
			CLUSTER_CONTROL_ROOT_BAD_RESERVED);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
	}
	v2_fixture(bytes);
	bytes[1024 + 296] = 1;
	v2_checksums(bytes);
	UT_ASSERT_EQ(cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
				 CLUSTER_CONTROL_ROOT_BAD_MAGIC);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
}

UT_TEST(test_v2_requires_object_references_and_bound_membership)
{
	static const struct {
		size_t offset;
		size_t len;
	} clear[] = { { 200, 8 },
				  { 208, 8 },
				  { 248, 8 },
				  { 256, 32 },
				  { 288, 8 },
				  { 296, 32 },
				  { 328, 8 },
				  { 336, 8 },
				  { 344, 32 },
				  { 512 + 256, 8 },
				  { 512 + 264, 32 },
				  { 512 + 296, 32 },
				  { 512 + 127 * 512 + 216, 8 },
				  { 512 + 127 * 512 + 224, 32 },
				  { 224, 8 },
				  { 216, 8 } };
	uint8 bytes[66048];
	ControlRootImage out;
	size_t i;

	for (i = 0; i < lengthof(clear); ++i) {
		v2_fixture(bytes);
		memset(bytes + clear[i].offset, 0, clear[i].len);
		v2_checksums(bytes);
		UT_ASSERT_EQ(
			cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
			CLUSTER_CONTROL_ROOT_RANGE_INVALID);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
	}
	v2_fixture(bytes);
	memset(bytes + 216, 0, 32);
	v2_checksums(bytes);
	UT_ASSERT_EQ(cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
				 CLUSTER_CONTROL_ROOT_RANGE_INVALID);
}

UT_TEST(test_v2_identity_and_node_thread_cannot_be_substituted)
{
	static const size_t offsets[]
		= { 24, 32, 48, 512 + 32, 512 + 40, 512 + 56, 512 + 8, 512 + 12, 512 + 127 * 512 + 12 };
	uint8 bytes[66048];
	ControlRootImage out;
	size_t i;

	for (i = 0; i < lengthof(offsets); ++i) {
		v2_fixture(bytes);
		if (offsets[i] == 48)
			memset(bytes + 48, 0, 16);
		else
			bytes[offsets[i]] ^= 1;
		v2_checksums(bytes);
		UT_ASSERT_EQ(
			cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
			CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
	}
}

UT_TEST(test_v2_database_state_validation_is_not_an_open_decision)
{
	uint8 bytes[66048];
	ControlRootImage out;
	uint32 state;

	for (state = 0; state <= 7; ++state) {
		v2_fixture(bytes);
		put_u32_le(bytes + 196, state);
		v2_checksums(bytes);
		UT_ASSERT_EQ(
			cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
			state >= 1 && state <= 6 ? CLUSTER_CONTROL_ROOT_OK_PRIMARY
									 : CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID);
		if (state >= 1 && state <= 6)
			UT_ASSERT_EQ(out.header.v2.database_state, state);
		else
			UT_ASSERT(v2_zero(&out, sizeof(out)));
	}
}

UT_TEST(test_v2_bad_arguments_and_size_clear_output)
{
	uint8 bytes[66048];
	ControlRootImage out;
	size_t i;
	static const size_t sizes[] = { 0, 512, 66047, 66049 };

	v2_fixture(bytes);
	for (i = 0; i < lengthof(sizes); ++i) {
		memset(&out, 0xee, sizeof(out));
		UT_ASSERT_EQ(cluster_control_root_v2_decode(bytes, sizes[i], v2_storage, TEST_SYSID, &out),
					 CLUSTER_CONTROL_ROOT_BAD_SIZE);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
	}
	UT_ASSERT_EQ(cluster_control_root_v2_decode(NULL, sizeof(bytes), v2_storage, TEST_SYSID, &out),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_control_root_v2_decode(bytes, sizeof(bytes), NULL, TEST_SYSID, &out),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, 0, &out),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, NULL),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_control_root_v2_encode(NULL), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
}

UT_TEST(test_v2_encoder_refuses_bad_logical_fields_without_bytes)
{
	uint8 bytes[66048];
	ControlRootImage out;
	int mutation;

	v2_fixture(bytes);
	for (mutation = 0; mutation < 11; ++mutation) {
		UT_ASSERT_EQ(
			cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
			CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		switch (mutation) {
		case 0:
			out.header.v2.reserved4 = 1;
			break;
		case 1:
			out.header.v2.config_generation = 0;
			break;
		case 2:
			out.publisher_incarnation[127] = 0;
			break;
		case 3:
			out.publisher_node[127] = 128;
			break;
		case 4:
			out.records[127].identity.origin_node_id = 126;
			break;
		case 5:
			out.records[127].reserved208 = 1;
			break;
		case 6:
			out.refs[127].history_generation = 0;
			break;
		case 7:
			out.header.format_version = 1;
			break;
		case 8:
			out.refs[1].anchor_generation = 1;
			break;
		case 9:
			out.present[127] = false;
			break;
		case 10:
			memset(out.header.storage_uuid, 0, 16);
			memset(out.records[0].identity.storage_uuid, 0, 16);
			memset(out.records[127].identity.storage_uuid, 0, 16);
			break;
		}
		UT_ASSERT(cluster_control_root_v2_encode(&out) != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(v2_zero(out.bytes, sizeof(out.bytes)));
	}
}

UT_TEST(test_v2_max_generations_remain_readable_without_advancement)
{
	uint8 bytes[66048];
	ControlRootImage out;
	static const size_t offsets[]
		= { 16, 200, 208, 248, 288, 328, 512 + 256, 512 + 127 * 512 + 216 };
	size_t i;

	v2_fixture(bytes);
	for (i = 0; i < lengthof(offsets); ++i)
		put_u64_le(bytes + offsets[i], UINT64_MAX);
	v2_checksums(bytes);
	UT_ASSERT_EQ(cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(out.header.v2.config_generation, UINT64_MAX);
	UT_ASSERT_EQ(cluster_control_root_v2_encode(&out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(memcmp(bytes, out.bytes, sizeof(bytes)) == 0);
}

/* PGRAC: independent startup-capable root bytes. The pending reference must
 * survive even though the old writer is closed and the new one is not serving.
 * Author: SqlRush <sqlrush@gmail.com> */
static void
v3_fixture(uint8 bytes[66048], bool pending)
{
	v2_fixture(bytes);
	put_u16_le(bytes + 4, 3);
	put_u16_le(bytes + 72, 3);
	put_u16_le(bytes + 74, 3);
	for (uint32 node = 0; node <= 127; node += 127)
		put_u16_le(bytes + 512 + node * 512 + 4, 3);
	if (pending) {
		uint8 *record = bytes + 512;

		put_u64_le(bytes + 232, 0);
		record[10] = 4;
		put_u64_le(record + 328, 71);
		memset(record + 336, 0x99, 32);
	}
	v2_checksums(bytes);
}

static void
v3_exit_fixture(uint8 bytes[66048], ClusterFormationSnapshotV1 *formation)
{
	v3_fixture(bytes, false);
	put_u32_le(bytes + 196, CLUSTER_CONTROL_ROOT_DATABASE_CLOSED);
	bytes[512 + 10] = bytes[512 + 127 * 512 + 10] = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	v2_checksums(bytes);
	memset(formation, 0, sizeof(*formation));
	formation->local_epoch = 51;
	formation->membership.membership_state[0] = CLUSTER_MEMBER_MEMBER;
	formation->membership.membership_state[127] = CLUSTER_MEMBER_MEMBER;
	formation->membership.last_admitted_incarnation[0] = 101;
	formation->membership.last_admitted_incarnation[127] = 228;
}

UT_TEST(test_v3_clean_exit_cut_keeps_complete_root_roster)
{
	uint8 bytes[66048];
	ClusterFormationSnapshotV1 formation;
	ClusterStartupExitCut cut, changed;
	v3_exit_fixture(bytes, &formation);
	UT_ASSERT_EQ(cluster_control_root_v3_clean_exit_cut(bytes, sizeof(bytes), v2_storage,
														TEST_SYSID, &formation, &cut),
				 0);
	UT_ASSERT_EQ(cut.required[0], 1);
	UT_ASSERT_EQ(cut.required[1], UINT64_C(1) << 63);
	UT_ASSERT_EQ(cut.predecessor[0], 99);
	UT_ASSERT_EQ(cut.predecessor[127], 226);
	UT_ASSERT_EQ(cut.observer[0], 101);
	UT_ASSERT_EQ(cut.observer[127], 228);
	UT_ASSERT_EQ(cut.key.coordinator, 0);
	UT_ASSERT_EQ(cut.key.coordinator_incarnation, 101);
	UT_ASSERT_EQ(cut.key.epoch, 51);
	UT_ASSERT_EQ(cut.key.database_incarnation, 41);
	UT_ASSERT_EQ(cut.key.config_generation, 47);
	UT_ASSERT_EQ(cut.key.root_sequence, 7);
	UT_ASSERT(memcmp(cut.key.storage_uuid, v2_storage, 16) == 0);
	UT_ASSERT(!v2_zero(cut.key.root_sha256, 32));
	put_u64_le(bytes + 16, 8);
	v2_checksums(bytes);
	UT_ASSERT_EQ(cluster_control_root_v3_clean_exit_cut(bytes, sizeof(bytes), v2_storage,
														TEST_SYSID, &formation, &changed),
				 0);
	UT_ASSERT(memcmp(changed.key.root_sha256, cut.key.root_sha256, 32) != 0);
}

UT_TEST(test_v3_clean_exit_cut_never_shrinks_missing_members)
{
	for (unsigned fault = 0; fault < 15; ++fault) {
		uint8 bytes[66048];
		ClusterFormationSnapshotV1 formation;
		ClusterStartupExitCut cut;
		v3_exit_fixture(bytes, &formation);
		switch (fault) {
		case 0:
			formation.membership.membership_state[127] = CLUSTER_MEMBER_DEAD;
			break;
		case 1:
			formation.membership.membership_state[127] = CLUSTER_MEMBER_ABSENT;
			break;
		case 2:
			formation.membership.last_admitted_incarnation[127] = 226;
			break;
		case 3:
			formation.membership.last_admitted_incarnation[127] = 0;
			break;
		case 4:
			formation.local_epoch = 0;
			break;
		case 5:
			formation.prebump_sync_active = 1;
			break;
		case 6:
			formation.self_join_failed = 1;
			break;
		case 7:
			formation.pending_join_bitmap[0] = 1;
			break;
		case 8:
			formation.excluded_bitmap[15] = 0x80;
			break;
		case 9:
			formation.membership.membership_state[3] = CLUSTER_MEMBER_MEMBER;
			break;
		case 10:
			bytes[512 + 127 * 512 + 10] = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
			break;
		case 11:
			put_u32_le(bytes + 196, CLUSTER_CONTROL_ROOT_DATABASE_OPEN);
			break;
		case 12:
			put_u32_le(bytes + 76, CLUSTER_CONTROL_ROOT_ACTIVATION_PREPARED);
			break;
		case 13:
			formation.reserved[1] = 1;
			break;
		case 14:
			bytes[4] = 2;
			break;
		}
		v2_checksums(bytes);
		memset(&cut, 0xff, sizeof(cut));
		UT_ASSERT(cluster_control_root_v3_clean_exit_cut(bytes, sizeof(bytes), v2_storage,
														 TEST_SYSID, &formation, &cut)
				  != 0);
		UT_ASSERT(v2_zero(&cut, sizeof(cut)));
	}
}

UT_TEST(test_v3_preserves_pending_initialization_in_exact_root)
{
	uint8 bytes[66048];
	ControlRootImage out;
	ClusterControlRootResult result;

	v3_fixture(bytes, true);
	result = cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out);
	UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return;
	UT_ASSERT_EQ(out.header.format_version, 3);
	UT_ASSERT_EQ(out.records[0].lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED);
	UT_ASSERT_EQ(out.header.v2.serving[0], 0);
	UT_ASSERT_EQ(out.refs[127].history_generation, 44);
	UT_ASSERT_EQ(out.startup[0].generation, 71);
	UT_ASSERT(memcmp(out.startup[0].sha256, bytes + 512 + 336, 32) == 0);
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(memcmp(out.bytes, bytes, sizeof(bytes)) == 0);
}

UT_TEST(test_v3_empty_pending_does_not_implicitly_downgrade_format)
{
	uint8 bytes[66048];
	ControlRootImage out;
	ClusterControlRootResult result;

	v3_fixture(bytes, false);
	result = cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out);
	UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return;
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(memcmp(out.bytes, bytes, sizeof(bytes)) == 0);
	UT_ASSERT_EQ(cluster_control_root_v2_encode(&out), CLUSTER_CONTROL_ROOT_BAD_VERSION);
	UT_ASSERT(v2_zero(out.bytes, sizeof(out.bytes)));
}

UT_TEST(test_v2_reader_still_refuses_startup_capable_root)
{
	uint8 bytes[66048];
	ControlRootImage out;

	v3_fixture(bytes, true);
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT_EQ(cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
				 CLUSTER_CONTROL_ROOT_BAD_VERSION);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
}

UT_TEST(test_v3_refuses_incomplete_or_serving_pending_ownership)
{
	uint8 bytes[66048];
	ControlRootImage out;
	static const ClusterControlRootResult reasons[]
		= { CLUSTER_CONTROL_ROOT_RANGE_INVALID,		CLUSTER_CONTROL_ROOT_RANGE_INVALID,
			CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID, CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH,
			CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID, CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID,
			CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID, CLUSTER_CONTROL_ROOT_BAD_VERSION };

	for (size_t i = 0; i < lengthof(reasons); i++) {
		v3_fixture(bytes, true);
		switch (i) {
		case 0:
			put_u64_le(bytes + 512 + 328, 0);
			break;
		case 1:
			memset(bytes + 512 + 336, 0, 32);
			break;
		case 2:
			put_u64_le(bytes + 232, 1);
			break;
		case 3:
			put_u64_le(bytes + 216, 0);
			break;
		case 4:
			bytes[512 + 10] = 1;
			break;
		case 5:
			put_u32_le(bytes + 196, 5);
			break;
		case 6:
			put_u32_le(bytes + 196, 6);
			break;
		case 7:
			put_u16_le(bytes + 512 + 4, 2);
			break;
		}
		v2_checksums(bytes);
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT_EQ(
			cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
			reasons[i]);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
	}
}

UT_TEST(test_v3_all_reserved_and_crc_boundaries_remain_strict)
{
	uint8 bytes[66048];
	ControlRootImage out;
	const size_t reserved[] = { 376,
								503,
								508,
								511,
								512 + 368,
								512 + 503,
								512 + 508,
								512 + 511,
								512 + 127 * 512 + 368,
								512 + 127 * 512 + 503 };

	for (size_t i = 0; i < lengthof(reserved); i++) {
		v3_fixture(bytes, true);
		bytes[reserved[i]] = 1;
		v2_checksums(bytes);
		UT_ASSERT_EQ(
			cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
			CLUSTER_CONTROL_ROOT_BAD_RESERVED);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
	}
	for (unsigned layer = 0; layer < 3; layer++) {
		v3_fixture(bytes, true);
		if (layer == 0)
			bytes[504] ^= 1;
		else {
			bytes[512 + 336] ^= 1;
			if (layer == 2)
				v2_root_checksums(bytes);
		}
		UT_ASSERT_EQ(
			cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
			layer == 0	 ? CLUSTER_CONTROL_ROOT_BAD_HEADER_CRC
			: layer == 1 ? CLUSTER_CONTROL_ROOT_BAD_BODY_CRC
						 : CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
	}
}

UT_TEST(test_v3_encoder_cannot_erase_pending_by_downcast_or_hole)
{
	uint8 bytes[66048];
	ControlRootImage out;

	for (unsigned mutation = 0; mutation < 5; mutation++) {
		v3_fixture(bytes, true);
		UT_ASSERT_EQ(
			cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
			CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		switch (mutation) {
		case 0:
			out.header.format_version = 2;
			UT_ASSERT_EQ(cluster_control_root_v2_encode(&out), CLUSTER_CONTROL_ROOT_BAD_RESERVED);
			break;
		case 1:
			out.startup[1] = out.startup[0];
			UT_ASSERT_EQ(cluster_control_root_v3_encode(&out), CLUSTER_CONTROL_ROOT_BAD_RESERVED);
			break;
		case 2:
			out.header.v2.serving[0] = 1;
			UT_ASSERT_EQ(cluster_control_root_v3_encode(&out),
						 CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID);
			break;
		case 3:
			out.header.v2.database_state = CLUSTER_CONTROL_ROOT_DATABASE_CLOSED;
			UT_ASSERT_EQ(cluster_control_root_v3_encode(&out),
						 CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID);
			break;
		case 4:
			out.startup[0].generation = 0;
			UT_ASSERT_EQ(cluster_control_root_v3_encode(&out), CLUSTER_CONTROL_ROOT_RANGE_INVALID);
			break;
		}
		UT_ASSERT(v2_zero(out.bytes, sizeof(out.bytes)));
	}
}

UT_TEST(test_v3_pending_high_origin_and_recovered_input_are_preserved)
{
	uint8 bytes[66048];
	ControlRootImage out;
	uint8 *record = bytes + 512 + 127 * 512;

	v3_fixture(bytes, false);
	put_u64_le(bytes + 240, 0);
	record[10] = 3;
	put_u64_le(record + 328, UINT64_MAX);
	memset(record + 336, 0x91, 32);
	v2_checksums(bytes);
	memcpy(out.bytes, bytes, sizeof(bytes));
	UT_ASSERT_EQ(
		cluster_control_root_v3_decode(out.bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(out.startup[127].generation, UINT64_MAX);
	UT_ASSERT_EQ(out.records[127].lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE);
	UT_ASSERT_EQ(out.header.v2.serving[0], 1);
	UT_ASSERT(v2_zero(&out.startup[0], sizeof(out.startup[0])));
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(memcmp(out.bytes, bytes, sizeof(bytes)) == 0);
}

UT_TEST(test_v3_size_identity_versions_and_alias_failure_clear_output)
{
	uint8 bytes[66048];
	ControlRootImage out;
	const size_t lengths[] = { 0, 512, 66047, 66049 };

	v3_fixture(bytes, true);
	for (size_t i = 0; i < lengthof(lengths); i++) {
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT_EQ(
			cluster_control_root_v3_decode(bytes, lengths[i], v2_storage, TEST_SYSID, &out),
			CLUSTER_CONTROL_ROOT_BAD_SIZE);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
	}
	UT_ASSERT_EQ(
		cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID + 1, &out),
		CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	for (unsigned offset = 4; offset <= 74; offset += offset == 4 ? 68 : 2) {
		v3_fixture(bytes, true);
		put_u16_le(bytes + offset, 2);
		v2_checksums(bytes);
		memcpy(out.bytes, bytes, sizeof(bytes));
		UT_ASSERT_EQ(
			cluster_control_root_v3_decode(out.bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
			CLUSTER_CONTROL_ROOT_BAD_VERSION);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
	}
}

/* PGRAC: literal operation bytes; the production encoder is not used to
 * construct its own expected input. Author: SqlRush <sqlrush@gmail.com> */
static void
startup_fixture(uint8 bytes[1536], ControlRootImage *root, uint32 phase)
{
	uint8 root_bytes[66048];
	uint8 *record = root_bytes + 512;
	uint8 *claim = bytes + 1280;
	uint8 claim_hash[32];

	v3_fixture(root_bytes, true);
	put_u64_le(root_bytes + 16, 80);
	put_u32_le(record + 100, 1);
	put_u32_le(record + 108, 0x8d);
	put_u64_le(record + 112, 0x1000028);
	put_u64_le(record + 120, 0x1000100);
	put_u64_le(record + 176, 0x1000028);
	put_u16_le(record + 192, 1);
	v2_checksums(root_bytes);
	if (cluster_control_root_v3_decode(root_bytes, sizeof(root_bytes), v2_storage, TEST_SYSID,
									   root))
		abort();
	memset(bytes, 0, 1536);
	memcpy(bytes, "PGWG", 4);
	put_u16_le(bytes + 4, 1);
	put_u16_le(bytes + 6, 1536);
	put_u32_le(bytes + 8, phase);
	put_u32_le(bytes + 12, 1);
	memset(bytes + 16, 0x11, 16);
	put_u64_le(bytes + 32, 41);
	put_u64_le(bytes + 40, 47);
	put_u64_le(bytes + 48, 43);
	put_u64_le(bytes + 56, 7);
	memset(bytes + 64, 0x22, 32);
	put_u64_le(bytes + 96, 71);
	put_u64_le(bytes + 104, 0x2000000);
	put_u32_le(bytes + 112, 1);
	put_u32_le(bytes + 116, 0x1000000);
	memset(bytes + 120, 0x33, 32);
	put_u64_le(bytes + 152, 0x1000028);
	put_u64_le(bytes + 160, 0x1000100);
	put_u32_le(bytes + 168, 1234);
	put_u32_le(bytes + 172, 1);
	put_u64_le(bytes + 176, 0x1000100);
	memcpy(bytes + 256, record, 512);
	put_u16_le(bytes + 256 + 4, 2);
	memset(bytes + 256 + 328, 0, 176);
	put_u32_le(bytes + 256 + 504, image_crc(bytes + 256, 504));

	put_u32_le(claim, 0x50475443);
	put_u16_le(claim + 4, 2);
	put_u16_le(claim + 6, 1);
	put_u64_le(claim + 16, TEST_SYSID);
	put_u64_le(claim + 24, 41);
	put_u64_le(claim + 32, 199);
	memcpy(claim + 40, root_bytes + 32, 32);
	put_u64_le(claim + 72, 111);
	put_u64_le(claim + 80, 12346);
	put_u64_le(claim + 88, 47);
	put_u64_le(claim + 96, 71);
	put_u32_le(claim + 104, image_crc(claim, 104));
	sha256_bytes(claim, 112, claim_hash);
	if (phase == 3) {
		uint8 *successor = bytes + 768;

		memcpy(successor, bytes + 256, 512);
		successor[10] = 1;
		put_u64_le(successor + 16, 72);
		put_u64_le(successor + 24, 111);
		put_u64_le(successor + 72, 12346);
		put_u64_le(successor + 80, 199);
		put_u64_le(successor + 112, 0x2000028);
		put_u64_le(successor + 120, 0x2000100);
		put_u32_le(successor + 168, image_crc(claim, 104));
		put_u32_le(successor + 172, 4321);
		put_u64_le(successor + 176, 0x2000028);
		put_u32_le(successor + 184, 4321);
		put_u64_le(successor + 256, 72);
		memset(successor + 264, 0x44, 32);
		memcpy(successor + 296, claim_hash, 32);
		put_u32_le(successor + 504, image_crc(successor, 504));
		put_u64_le(bytes + 1392, 2);
		put_u64_le(bytes + 1400, 0x2000100);
		put_u64_le(bytes + 1408, 0x2000028);
		put_u32_le(bytes + 1416, 4321);
		put_u32_le(bytes + 1420, 1);
	}
	put_u32_le(bytes + 1532, image_crc(bytes, 1532));
	sha256_bytes(bytes, 1536, root->startup[0].sha256);
}

UT_TEST(test_startup_encoder_matches_literal_phases_without_publishing)
{
	uint8 expected[1536], actual[1536];
	ControlRootImage root, before;
	ClusterWalStartupImage input;
	ControlRootStartupRefV3 ref, selected;

	for (uint32 phase = 1; phase <= 3; phase++) {
		startup_fixture(expected, &root, phase);
		UT_ASSERT_EQ(
			cluster_control_root_v3_startup_decode(expected, sizeof(expected), &root, 0, &input),
			0);
		selected = root.startup[0];
		memset(&root.startup[0], 0, sizeof(root.startup[0]));
		before = root;
		memset(actual, 0xa5, sizeof(actual));
		memset(&ref, 0xa5, sizeof(ref));
		UT_ASSERT_EQ(cluster_control_root_v3_startup_encode(&root, 0, &input, actual, &ref), 0);
		UT_ASSERT(memcmp(actual, expected, sizeof(actual)) == 0);
		UT_ASSERT(memcmp(&ref, &selected, sizeof(ref)) == 0);
		UT_ASSERT(memcmp(&root, &before, sizeof(root)) == 0);
	}
}

/* Independent literal terminal, including its original initialization and
 * exact EMPTY durable promise. No producer is used to manufacture expectations. */
static void
terminal_fixture(uint8 bytes[CLUSTER_WAL_TERMINAL_BYTES], ControlRootImage *root,
				 ClusterWalHistoryImage *history, ClusterWalTerminalRef *ref, bool parameters)
{
	uint8 *prefix = bytes + 1792;
	memset(bytes, 0, CLUSTER_WAL_TERMINAL_BYTES);
	startup_fixture(bytes + 256, root, CLUSTER_WAL_STARTUP_INITIALIZING);
	memset(history, 0, sizeof(*history));
	memcpy(bytes, "PGWG", 4);
	put_u16_le(bytes + 4, 2);
	put_u16_le(bytes + 6, 2304);
	put_u32_le(bytes + 8, 4);
	put_u32_le(bytes + 12, 1);
	memcpy(bytes + 16, bytes + 256 + 16, 16);
	put_u64_le(bytes + 32, 81);
	put_u64_le(bytes + 40, 41);
	put_u64_le(bytes + 48, 80);
	memset(bytes + 56, 0x35, 32);
	put_u64_le(bytes + 88, 90);
	put_u64_le(bytes + 96, 999);
	put_u32_le(bytes + 104, 2);
	put_u64_le(bytes + 112, 12345);
	put_u64_le(bytes + 120, root->startup[0].generation);
	memcpy(bytes + 128, root->startup[0].sha256, 32);
	memset(bytes + 160, 0x45, 32);
	memset(bytes + 192, 0x55, 32);
	memcpy(prefix, "PGWP", 4);
	put_u16_le(prefix + 4, 1);
	put_u16_le(prefix + 6, 256);
	put_u64_le(prefix + 8, TEST_SYSID);
	put_u64_le(prefix + 16, 41);
	memcpy(prefix + 24, root->header.storage_uuid, 16);
	memcpy(prefix + 40, root->header.authority_uuid, 16);
	put_u32_le(prefix + 56, 0);
	put_u32_le(prefix + 60, 1);
	put_u64_le(prefix + 64, 199);
	put_u32_le(prefix + 72, 1);
	sha256_bytes(bytes + 256 + 1280, 112, prefix + 80);
	put_u64_le(prefix + 112, 1);
	put_u32_le(prefix + 252, image_crc(prefix, 252));
	put_u32_le(bytes + 2068, 1);
	if (parameters) {
		put_u64_le(bytes + 2048, 0x2000100);
		put_u64_le(bytes + 2056, 0x2000028);
		put_u32_le(bytes + 2064, 1234);
		put_u64_le(bytes + 2072, 1);
		put_u64_le(bytes + 2088, 1);
		put_u32_le(bytes + 2096, 900);
		put_u32_le(bytes + 2100, 40);
		put_u32_le(bytes + 2104, 20);
		put_u32_le(bytes + 2108, 10);
		put_u32_le(bytes + 2112, 90);
	}
	put_u32_le(bytes + 2300, image_crc(bytes, 2300));
	ref->incarnation = 199;
	ref->generation = 81;
	sha256_bytes(bytes, 2304, ref->sha256);
	history->terminal_count = 1;
	history->terminals[0] = *ref;
	root->header.file_txn_seq = 81;
	memset(&root->startup[0], 0, sizeof(root->startup[0]));
	/* The enclosing manifest changed; original PGWG bytes must not change. */
	root->refs[0].history_generation = 81;
	memset(root->refs[0].history_sha256, 0x65, 32);
}

UT_TEST(test_terminal_literal_is_distinct_from_checkpoint_or_active_writer)
{
	for (int parameters = 0; parameters < 2; parameters++) {
		uint8 bytes[CLUSTER_WAL_TERMINAL_BYTES], encoded[CLUSTER_WAL_TERMINAL_BYTES];
		ControlRootImage root;
		ClusterWalHistoryImage history;
		ClusterWalTerminalRef ref, encoded_ref;
		ClusterWalTerminalImage out;
		ClusterWalStartupImage active;
		terminal_fixture(bytes, &root, &history, &ref, parameters != 0);
		UT_ASSERT_EQ(
			cluster_wal_terminal_decode(bytes, sizeof(bytes), &root, 0, &history, &ref, &out), 0);
		if (ut_current_failed)
			return;
		UT_ASSERT_EQ(out.initialization.claim.identity.origin_owner_incarnation, 199);
		UT_ASSERT_EQ(out.initialization.predecessor.snapshot.identity.origin_owner_incarnation, 99);
		UT_ASSERT_EQ(out.observation.tail.records, parameters);
		UT_ASSERT_EQ(out.observation.checkpoint_records, 0);
		UT_ASSERT_EQ(out.observation.max_connections, parameters ? 900 : 0);
		UT_ASSERT_EQ(out.observation.tail.durable_prefix.exclusive_end, 0);
		UT_ASSERT_EQ(cluster_wal_terminal_encode(&root, 0, &history, &out, encoded, &encoded_ref),
					 0);
		UT_ASSERT(memcmp(encoded, bytes, sizeof(bytes)) == 0);
		UT_ASSERT(memcmp(&ref, &encoded_ref, sizeof(ref)) == 0);
		UT_ASSERT_EQ(
			cluster_control_root_v3_startup_decode(bytes, sizeof(bytes), &root, 0, &active),
			CLUSTER_CONTROL_ROOT_BAD_SIZE);
		UT_ASSERT(v2_zero(&active, sizeof(active)));
		out.observation.unsupported_records = 1;
		UT_ASSERT_EQ(cluster_wal_terminal_encode(&root, 0, &history, &out, encoded, &encoded_ref),
					 CLUSTER_CONTROL_ROOT_RANGE_INVALID);
		UT_ASSERT(v2_zero(encoded, sizeof(encoded)));
		UT_ASSERT(v2_zero(&encoded_ref, sizeof(encoded_ref)));
	}
}

UT_TEST(test_terminal_recoverer_uses_node_and_incarnation_identity)
{
	for (unsigned same_node = 0; same_node < 2; same_node++)
		for (unsigned same_incarnation = 0; same_incarnation < 2; same_incarnation++) {
			uint8 bytes[CLUSTER_WAL_TERMINAL_BYTES], encoded[CLUSTER_WAL_TERMINAL_BYTES];
			ControlRootImage root;
			ClusterWalHistoryImage history;
			ClusterWalTerminalRef ref, encoded_ref;
			ClusterWalTerminalImage out;
			terminal_fixture(bytes, &root, &history, &ref, false);
			put_u64_le(bytes + 96, same_incarnation ? 199 : 999);
			put_u32_le(bytes + 104, same_node ? 0 : 2);
			put_u32_le(bytes + 2300, image_crc(bytes, 2300));
			sha256_bytes(bytes, sizeof(bytes), ref.sha256);
			history.terminals[0] = ref;
			if (same_node && same_incarnation) {
				UT_ASSERT_EQ(cluster_wal_terminal_decode(bytes, sizeof(bytes), &root, 0, &history,
														 &ref, &out),
							 CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
				UT_ASSERT(v2_zero(&out, sizeof(out)));
			} else {
				UT_ASSERT_EQ(cluster_wal_terminal_decode(bytes, sizeof(bytes), &root, 0, &history,
														 &ref, &out),
							 0);
				if (ut_current_failed)
					return;
				UT_ASSERT_EQ(
					cluster_wal_terminal_encode(&root, 0, &history, &out, encoded, &encoded_ref),
					0);
				UT_ASSERT(memcmp(bytes, encoded, sizeof(bytes)) == 0);
			}
		}
}

UT_TEST(test_terminal_rejects_unproven_or_inconsistent_fields)
{
	for (unsigned fault = 0; fault < 24; fault++) {
		uint8 bytes[CLUSTER_WAL_TERMINAL_BYTES];
		ControlRootImage root;
		ClusterWalHistoryImage history;
		ClusterWalTerminalRef ref;
		ClusterWalTerminalImage out;
		terminal_fixture(bytes, &root, &history, &ref, true);
		switch (fault) {
		case 0:
			bytes[108] = 1;
			break;
		case 1:
			bytes[224] = 1;
			break;
		case 2:
			bytes[2120] = 1;
			break;
		case 3:
			put_u32_le(bytes + 8, 3);
			break;
		case 4:
			put_u32_le(bytes + 12, 0);
			break;
		case 5:
			put_u64_le(bytes + 40, 99);
			break;
		case 6:
			put_u64_le(bytes + 48, 82);
			break;
		case 7:
			memset(bytes + 56, 0, 32);
			break;
		case 8:
			put_u64_le(bytes + 88, 0);
			break;
		case 9:
			put_u64_le(bytes + 96, 199);
			put_u32_le(bytes + 104, 0);
			break;
		case 10:
			put_u32_le(bytes + 104, 128);
			break;
		case 11:
			put_u64_le(bytes + 112, 0);
			break;
		case 12:
			memset(bytes + 160, 0, 32);
			break;
		case 13:
			memset(bytes + 192, 0, 32);
			break;
		case 14:
			bytes[128] ^= 1;
			break;
		case 15:
			bytes[16] ^= 1;
			break;
		case 16:
			put_u32_le(bytes + 2068, 2);
			break;
		case 17:
			put_u64_le(bytes + 2072, 0);
			break;
		case 18:
			put_u64_le(bytes + 2080, UINT64_MAX);
			break;
		case 19:
			put_u32_le(bytes + 2096, 0);
			break;
		case 20:
			put_u32_le(bytes + 2116, 2);
			break;
		case 21:
			put_u32_le(bytes + 2108, UINT32_MAX);
			break;
		case 22:
			put_u64_le(bytes + 2056, 8);
			break;
		case 23:
			put_u64_le(bytes + 2048, 0);
			break;
		}
		put_u32_le(bytes + 2300, image_crc(bytes, 2300));
		sha256_bytes(bytes, sizeof(bytes), ref.sha256);
		history.terminals[0] = ref;
		UT_ASSERT(cluster_wal_terminal_decode(bytes, sizeof(bytes), &root, 0, &history, &ref, &out)
				  != 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
	}
}

UT_TEST(test_terminal_requires_original_predecessor_in_flat_current_union)
{
	uint8 bytes[CLUSTER_WAL_TERMINAL_BYTES];
	ControlRootImage root;
	ClusterWalHistoryImage history;
	ClusterWalTerminalRef ref;
	ClusterWalTerminalImage out;
	terminal_fixture(bytes, &root, &history, &ref, false);
	UT_ASSERT_EQ(cluster_wal_terminal_decode(bytes, sizeof(bytes), &root, 0, &history, &ref, &out),
				 0);
	if (ut_current_failed)
		return;
	history.count = 1;
	history.records[0] = out.initialization.predecessor;
	history.records[0].refs.history_generation = 0;
	memset(history.records[0].refs.history_sha256, 0, 32);
	root.records[0].identity.origin_owner_incarnation = 301;
	root.records[0].identity.root_lineage_seq = 401;
	root.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	root.header.v2.serving[0] |= 1;
	UT_ASSERT_EQ(cluster_wal_terminal_decode(bytes, sizeof(bytes), &root, 0, &history, &ref, &out),
				 0);
	history.records[0].refs.anchor_sha256[0] ^= 1;
	UT_ASSERT_EQ(cluster_wal_terminal_decode(bytes, sizeof(bytes), &root, 0, &history, &ref, &out),
				 CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	history.count = 0;
	UT_ASSERT_EQ(cluster_wal_terminal_decode(bytes, sizeof(bytes), &root, 0, &history, &ref, &out),
				 CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
}

UT_TEST(test_startup_encoder_refuses_invalid_logical_input_without_partial_bytes)
{
	uint8 bytes[1536], actual[1536];
	ControlRootImage root;
	ClusterWalStartupImage input;
	ControlRootStartupRefV3 ref;

	for (unsigned fault = 0; fault < 13; fault++) {
		startup_fixture(bytes, &root, fault >= 10 ? 3 : 1);
		UT_ASSERT_EQ(cluster_control_root_v3_startup_decode(bytes, sizeof(bytes), &root, 0, &input),
					 0);
		switch (fault) {
		case 0:
			input.generation = 0;
			break;
		case 1:
			input.phase = 0;
			break;
		case 2:
			input.predecessor.snapshot.identity.reserved42 = 1;
			break;
		case 3:
			input.predecessor.snapshot.checkpoint_record_crc32c++;
			break;
		case 4:
			input.claim.identity.reserved60 = 1;
			break;
		case 5:
			input.successor.snapshot.root_publish_seq = 1;
			break;
		case 6:
			input.prefix.sequence = 1;
			break;
		case 7:
			input.prefix_timeline = 1;
			break;
		case 8:
			input.input_record_end = input.input_record_start;
			break;
		case 9:
			root.header.format_version = 2;
			break;
		case 10:
			input.successor.snapshot.identity.reserved42 = 1;
			break;
		case 11:
			input.successor.snapshot.identity.origin_thread_id = 257;
			break;
		case 12:
			input.successor.snapshot.lifecycle = 257;
			break;
		}
		memset(actual, 0xa5, sizeof(actual));
		memset(&ref, 0xa5, sizeof(ref));
		UT_ASSERT(cluster_control_root_v3_startup_encode(&root, 0, &input, actual, &ref) != 0);
		UT_ASSERT(v2_zero(actual, sizeof(actual)));
		UT_ASSERT(v2_zero(&ref, sizeof(ref)));
	}
}

UT_TEST(test_startup_encoder_arguments_and_aliases_clear_outputs)
{
	uint8 bytes[1536], actual[1536];
	ControlRootImage root;
	ClusterWalStartupImage input;
	ControlRootStartupRefV3 ref;
	union {
		ControlRootImage root;
		ClusterWalStartupImage startup;
		uint8 bytes[1536];
		ControlRootStartupRefV3 ref;
	} alias;

	startup_fixture(bytes, &root, 1);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_decode(bytes, sizeof(bytes), &root, 0, &input), 0);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_encode(NULL, 0, &input, actual, &ref),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_encode(&root, 128, &input, actual, &ref),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_encode(&root, 0, NULL, actual, &ref),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_encode(&root, 0, &input, NULL, &ref),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(v2_zero(&ref, sizeof(ref)));
	UT_ASSERT_EQ(cluster_control_root_v3_startup_encode(&root, 0, &input, actual, NULL),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(v2_zero(actual, sizeof(actual)));
	alias.root = root;
	UT_ASSERT_EQ(cluster_control_root_v3_startup_encode(&alias.root, 0, &input, alias.bytes, &ref),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(v2_zero(alias.bytes, sizeof(alias.bytes)));
	alias.startup = input;
	UT_ASSERT_EQ(
		cluster_control_root_v3_startup_encode(&root, 0, &alias.startup, actual, &alias.ref),
		CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(v2_zero(actual, sizeof(actual)));
	UT_ASSERT(v2_zero(&alias.ref, sizeof(alias.ref)));
	UT_ASSERT_EQ(cluster_control_root_v3_startup_encode(&root, 0, &input, alias.bytes, &alias.ref),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(v2_zero(alias.bytes, sizeof(alias.bytes)));
}

UT_TEST(test_startup_selected_phases_retain_exact_old_and_new_writers)
{
	uint8 bytes[1536];
	ControlRootImage root;
	ClusterWalStartupImage out;

	for (uint32 phase = 1; phase <= 3; phase++) {
		ClusterControlRootResult result;

		startup_fixture(bytes, &root, phase);
		result = cluster_control_root_v3_startup_decode(bytes, sizeof(bytes), &root, 0, &out);
		UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			continue;
		UT_ASSERT_EQ(out.phase, phase);
		UT_ASSERT_EQ(out.predecessor.snapshot.identity.origin_owner_incarnation, 99);
		UT_ASSERT_EQ(out.claim.identity.origin_owner_incarnation, 199);
		UT_ASSERT_EQ(out.first_segment_lsn, 0x2000000);
		UT_ASSERT_EQ(out.sealed_input_end, 0x1000100);
		UT_ASSERT_EQ(out.generation, 71);
		if (phase == 3) {
			UT_ASSERT_EQ(out.successor.snapshot.identity.origin_owner_incarnation, 199);
			UT_ASSERT_EQ(out.prefix.exclusive_end, 0x2000100);
			UT_ASSERT_EQ(out.prefix.record_crc, 4321);
		} else {
			UT_ASSERT(v2_zero(&out.successor, sizeof(out.successor)));
			UT_ASSERT(v2_zero(&out.prefix, sizeof(out.prefix)));
		}
	}
}

static void
startup_checksum(uint8 bytes[1536], ControlRootImage *root)
{
	put_u32_le(bytes + 1532, image_crc(bytes, 1532));
	sha256_bytes(bytes, 1536, root->startup[0].sha256);
}

static ClusterControlRootResult
startup_refused(const uint8 *bytes, size_t len, const ControlRootImage *root, uint32 node)
{
	ClusterWalStartupImage out;
	ClusterControlRootResult result;

	memset(&out, 0xa5, sizeof(out));
	result = cluster_control_root_v3_startup_decode(bytes, len, root, node, &out);
	UT_ASSERT(result != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	return result;
}

UT_TEST(test_startup_envelope_integrity_and_selection_are_required)
{
	uint8 bytes[1536];
	ControlRootImage root;
	static const size_t reserved[] = { 184, 255, 1424, 1531 };
	static const size_t lengths[] = { 0, 256, 1535, 1537 };

	for (size_t i = 0; i < lengthof(reserved); i++) {
		startup_fixture(bytes, &root, 1);
		bytes[reserved[i]] = 1;
		startup_checksum(bytes, &root);
		UT_ASSERT_EQ(startup_refused(bytes, sizeof(bytes), &root, 0),
					 CLUSTER_CONTROL_ROOT_BAD_RESERVED);
	}
	startup_fixture(bytes, &root, 1);
	for (size_t i = 0; i < lengthof(lengths); i++)
		UT_ASSERT_EQ(startup_refused(bytes, lengths[i], &root, 0), CLUSTER_CONTROL_ROOT_BAD_SIZE);
	bytes[0] ^= 1;
	startup_checksum(bytes, &root);
	UT_ASSERT_EQ(startup_refused(bytes, sizeof(bytes), &root, 0), CLUSTER_CONTROL_ROOT_BAD_MAGIC);
	startup_fixture(bytes, &root, 1);
	put_u16_le(bytes + 4, 2);
	startup_checksum(bytes, &root);
	UT_ASSERT_EQ(startup_refused(bytes, sizeof(bytes), &root, 0), CLUSTER_CONTROL_ROOT_BAD_VERSION);
	startup_fixture(bytes, &root, 1);
	put_u16_le(bytes + 6, 1535);
	startup_checksum(bytes, &root);
	UT_ASSERT_EQ(startup_refused(bytes, sizeof(bytes), &root, 0), CLUSTER_CONTROL_ROOT_BAD_SIZE);
	startup_fixture(bytes, &root, 1);
	bytes[1532] ^= 1;
	UT_ASSERT_EQ(startup_refused(bytes, sizeof(bytes), &root, 0),
				 CLUSTER_CONTROL_ROOT_BAD_BODY_CRC);
	startup_fixture(bytes, &root, 1);
	root.startup[0].sha256[0] ^= 1;
	UT_ASSERT_EQ(startup_refused(bytes, sizeof(bytes), &root, 0),
				 CLUSTER_CONTROL_ROOT_HASH_MISMATCH);
	startup_fixture(bytes, &root, 1);
	root.startup[0].generation++;
	UT_ASSERT_EQ(startup_refused(bytes, sizeof(bytes), &root, 0),
				 CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
}

UT_TEST(test_startup_operation_identity_and_phase_cannot_be_invented)
{
	uint8 bytes[1536];
	ControlRootImage root;
	const size_t nonzero[] = { 16, 48, 56, 64, 120 };
	const size_t widths[] = { 16, 8, 8, 32, 32 };

	for (size_t i = 0; i < lengthof(nonzero); i++) {
		startup_fixture(bytes, &root, 1);
		memset(bytes + nonzero[i], 0, widths[i]);
		startup_checksum(bytes, &root);
		UT_ASSERT_EQ(startup_refused(bytes, sizeof(bytes), &root, 0),
					 CLUSTER_CONTROL_ROOT_RANGE_INVALID);
	}
	for (unsigned field = 8; field <= 12; field += 4)
		for (unsigned value = 0; value <= 4; value += 4) {
			startup_fixture(bytes, &root, 1);
			put_u32_le(bytes + field, value);
			startup_checksum(bytes, &root);
			UT_ASSERT_EQ(startup_refused(bytes, sizeof(bytes), &root, 0),
						 CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID);
		}
	startup_fixture(bytes, &root, 1);
	put_u64_le(bytes + 56, 80);
	startup_checksum(bytes, &root);
	UT_ASSERT_EQ(startup_refused(bytes, sizeof(bytes), &root, 0),
				 CLUSTER_CONTROL_ROOT_RANGE_INVALID);
	startup_fixture(bytes, &root, 1);
	put_u64_le(bytes + 32, 42);
	startup_checksum(bytes, &root);
	UT_ASSERT_EQ(startup_refused(bytes, sizeof(bytes), &root, 0),
				 CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
	startup_fixture(bytes, &root, 1);
	put_u64_le(bytes + 40, 48);
	startup_checksum(bytes, &root);
	UT_ASSERT_EQ(startup_refused(bytes, sizeof(bytes), &root, 0),
				 CLUSTER_CONTROL_ROOT_RANGE_INVALID);
}

UT_TEST(test_startup_predecessor_is_exact_not_just_same_origin)
{
	uint8 bytes[1536];
	ControlRootImage root;
	const size_t fields[] = { 16, 24, 72, 80, 144, 160, 256, 264, 296 };

	for (size_t i = 0; i < lengthof(fields); i++) {
		startup_fixture(bytes, &root, 1);
		bytes[256 + fields[i]] ^= 2;
		put_u32_le(bytes + 256 + 504, image_crc(bytes + 256, 504));
		startup_checksum(bytes, &root);
		UT_ASSERT_EQ(startup_refused(bytes, sizeof(bytes), &root, 0),
					 CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
	}
	startup_fixture(bytes, &root, 1);
	bytes[256 + 328] = 1;
	put_u32_le(bytes + 256 + 504, image_crc(bytes + 256, 504));
	startup_checksum(bytes, &root);
	UT_ASSERT_EQ(startup_refused(bytes, sizeof(bytes), &root, 0),
				 CLUSTER_CONTROL_ROOT_BAD_RESERVED);
	startup_fixture(bytes, &root, 1);
	bytes[256 + 504] ^= 1;
	startup_checksum(bytes, &root);
	UT_ASSERT_EQ(startup_refused(bytes, sizeof(bytes), &root, 0),
				 CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC);
}

UT_TEST(test_startup_input_and_fresh_segment_boundaries)
{
	uint8 bytes[1536];
	ControlRootImage root;
	ClusterWalStartupImage out;
	const size_t fields[] = { 104, 112, 116, 152, 160, 168, 172, 176 };

	for (size_t i = 0; i < lengthof(fields); i++) {
		startup_fixture(bytes, &root, 1);
		bytes[fields[i]] ^= 1;
		startup_checksum(bytes, &root);
		UT_ASSERT_EQ(startup_refused(bytes, sizeof(bytes), &root, 0),
					 CLUSTER_CONTROL_ROOT_RANGE_INVALID);
	}
	/* A sealed segment boundary stays that boundary, rather than skipping a
	 * segment. The rounded overflow is refused, not wrapped into old WAL. */
	startup_fixture(bytes, &root, 1);
	put_u64_le(bytes + 176, 0x2000000);
	put_u64_le(bytes + 256 + 120, 0x2000000);
	root.records[0].validated_tail_lsn_exclusive = 0x2000000;
	put_u32_le(bytes + 256 + 504, image_crc(bytes + 256, 504));
	startup_checksum(bytes, &root);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_decode(bytes, sizeof(bytes), &root, 0, &out), 0);
	UT_ASSERT_EQ(out.first_segment_lsn, 0x2000000);
	put_u64_le(bytes + 176, UINT64_MAX);
	put_u64_le(bytes + 256 + 120, UINT64_MAX);
	root.records[0].validated_tail_lsn_exclusive = UINT64_MAX;
	put_u32_le(bytes + 256 + 504, image_crc(bytes + 256, 504));
	startup_checksum(bytes, &root);
	UT_ASSERT_EQ(startup_refused(bytes, sizeof(bytes), &root, 0),
				 CLUSTER_CONTROL_ROOT_RANGE_INVALID);
}

UT_TEST(test_startup_new_claim_requires_same_database_and_fresh_writer)
{
	uint8 bytes[1536];
	ControlRootImage root;
	const size_t fields[] = { 6, 8, 16, 24, 40, 56, 88 };

	for (size_t i = 0; i < lengthof(fields); i++) {
		startup_fixture(bytes, &root, 1);
		bytes[1280 + fields[i]] ^= 2;
		put_u32_le(bytes + 1280 + 104, image_crc(bytes + 1280, 104));
		startup_checksum(bytes, &root);
		startup_refused(bytes, sizeof(bytes), &root, 0);
	}
	for (unsigned identity = 0; identity < 2; identity++) {
		startup_fixture(bytes, &root, 1);
		put_u64_le(bytes + 1280 + (identity == 0 ? 32 : 72), identity == 0 ? 99 : 11);
		put_u32_le(bytes + 1280 + 104, image_crc(bytes + 1280, 104));
		startup_checksum(bytes, &root);
		UT_ASSERT_EQ(startup_refused(bytes, sizeof(bytes), &root, 0),
					 CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
	}
	startup_fixture(bytes, &root, 1);
	bytes[1280 + 104] ^= 1;
	startup_checksum(bytes, &root);
	UT_ASSERT_EQ(startup_refused(bytes, sizeof(bytes), &root, 0),
				 CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC);
}

UT_TEST(test_startup_unfinished_phase_has_no_successor_or_prefix)
{
	uint8 bytes[1536];
	ControlRootImage root;
	const size_t forbidden[] = { 768, 1279, 1392, 1400, 1408, 1416, 1420 };

	for (unsigned phase = 1; phase <= 2; phase++)
		for (size_t i = 0; i < lengthof(forbidden); i++) {
			startup_fixture(bytes, &root, phase);
			bytes[forbidden[i]] = 1;
			startup_checksum(bytes, &root);
			UT_ASSERT_EQ(startup_refused(bytes, sizeof(bytes), &root, 0),
						 CLUSTER_CONTROL_ROOT_BAD_RESERVED);
		}
}

UT_TEST(test_startup_durable_prefix_must_bind_new_checkpoint_and_claim)
{
	uint8 bytes[1536];
	ControlRootImage root;
	ClusterWalStartupImage out;
	const size_t fields[]
		= { 768 + 24, 768 + 72, 768 + 80, 768 + 112, 768 + 296, 1392, 1400, 1408, 1416, 1420 };

	for (size_t i = 0; i < lengthof(fields); i++) {
		startup_fixture(bytes, &root, 3);
		if (fields[i] == 1392)
			put_u64_le(bytes + 1392, 0);
		else
			bytes[fields[i]] ^= 2;
		put_u32_le(bytes + 768 + 504, image_crc(bytes + 768, 504));
		startup_checksum(bytes, &root);
		startup_refused(bytes, sizeof(bytes), &root, 0);
	}
	startup_fixture(bytes, &root, 3);
	memset(bytes + 768, 0, 512);
	startup_checksum(bytes, &root);
	UT_ASSERT_EQ(startup_refused(bytes, sizeof(bytes), &root, 0),
				 CLUSTER_CONTROL_ROOT_RANGE_INVALID);
	/* Real CRC32C can be zero. Presence comes from validated fields/phase. */
	startup_fixture(bytes, &root, 3);
	put_u32_le(bytes + 768 + 172, 0);
	put_u32_le(bytes + 768 + 184, 0);
	put_u32_le(bytes + 1416, 0);
	put_u32_le(bytes + 768 + 504, image_crc(bytes + 768, 504));
	startup_checksum(bytes, &root);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_decode(bytes, sizeof(bytes), &root, 0, &out), 0);
	UT_ASSERT_EQ(out.prefix.record_crc, 0);
}

UT_TEST(test_startup_recovery_import_and_old_config_remain_distinct)
{
	uint8 bytes[1536];
	ControlRootImage root;
	ClusterWalStartupImage out;

	startup_fixture(bytes, &root, 1);
	put_u32_le(bytes + 12, 3);
	startup_checksum(bytes, &root);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_decode(bytes, sizeof(bytes), &root, 0, &out), 0);
	UT_ASSERT_EQ(out.input_kind, CLUSTER_WAL_STARTUP_IMPORTED);
	/* Reading an old pending operation after configuration advances is not
	 * permission to resume that old initializer under a new configuration. */
	root.header.v2.config_generation++;
	UT_ASSERT_EQ(cluster_control_root_v3_startup_decode(bytes, sizeof(bytes), &root, 0, &out), 0);
	UT_ASSERT_EQ(out.config_generation, 47);
	put_u32_le(bytes + 12, 2);
	startup_checksum(bytes, &root);
	UT_ASSERT_EQ(startup_refused(bytes, sizeof(bytes), &root, 0),
				 CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID);
	bytes[256 + 10] = 3;
	root.records[0].lifecycle = 3;
	put_u32_le(bytes + 256 + 504, image_crc(bytes + 256, 504));
	startup_checksum(bytes, &root);
	UT_ASSERT_EQ(startup_refused(bytes, sizeof(bytes), &root, 0),
				 CLUSTER_CONTROL_ROOT_RANGE_INVALID);
	put_u32_le(bytes + 256 + 108, 0x19d);
	put_u32_le(bytes + 256 + 104, 1);
	put_u64_le(bytes + 256 + 128, 0x1000100);
	put_u64_le(bytes + 256 + 208, 0x1000028);
	root.records[0].root_flags = 0x19d;
	root.records[0].recovered_tli = 1;
	root.records[0].recovered_through_lsn_exclusive = 0x1000100;
	root.records[0].recovered_last_record_lsn = 0x1000028;
	put_u32_le(bytes + 256 + 504, image_crc(bytes + 256, 504));
	startup_checksum(bytes, &root);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_decode(bytes, sizeof(bytes), &root, 0, &out), 0);
	UT_ASSERT_EQ(out.input_kind, CLUSTER_WAL_STARTUP_RECOVERED);
}

UT_TEST(test_startup_invalid_arguments_and_aliases_cannot_leave_partial_input)
{
	uint8 bytes[1536];
	ControlRootImage root;
	union {
		ControlRootImage root;
		uint8 bytes[1536];
		ClusterWalStartupImage out;
	} storage;

	startup_fixture(bytes, &root, 1);
	UT_ASSERT_EQ(startup_refused(NULL, sizeof(bytes), &root, 0),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(startup_refused(bytes, sizeof(bytes), NULL, 0),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(startup_refused(bytes, sizeof(bytes), &root, 128),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(startup_refused(bytes, sizeof(bytes), &root, 1), CLUSTER_CONTROL_ROOT_ABSENT);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_decode(bytes, sizeof(bytes), &root, 0, NULL),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	memcpy(storage.bytes, bytes, sizeof(bytes));
	UT_ASSERT_EQ(cluster_control_root_v3_startup_decode(storage.bytes, sizeof(bytes), &root, 0,
														&storage.out),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(v2_zero(&storage.out, sizeof(storage.out)));
	storage.root = root;
	UT_ASSERT_EQ(cluster_control_root_v3_startup_decode(bytes, sizeof(bytes), &storage.root, 0,
														&storage.out),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(v2_zero(&storage.out, sizeof(storage.out)));
}

UT_TEST(test_v1_io_does_not_silently_consume_or_convert_v2)
{
	uint8 bytes[66048];
	ControlRootImage out;
	ClusterControlRootMigrationImage legacy;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken token;
	ClusterControlRootSnapshot snapshot;
	char primary[MAXPGPATH];
	char bak[MAXPGPATH];

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&legacy, &round, &token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	path_for(primary, sizeof(primary), CLUSTER_CONTROL_ROOT_REL_PATH);
	path_for(bak, sizeof(bak), CLUSTER_CONTROL_ROOT_BAK_REL_PATH);
	read_all_or_abort(primary, bytes, sizeof(bytes));
	UT_ASSERT_EQ(cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
				 CLUSTER_CONTROL_ROOT_BAD_VERSION);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	v2_fixture(bytes);
	write_all_or_abort(primary, bytes, sizeof(bytes));
	unlink(bak);
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &legacy.records[0].identity,
													 CLUSTER_CONTROL_ROOT_READ_STRONG, &snapshot,
													 NULL),
				 CLUSTER_CONTROL_ROOT_BAD_VERSION);
	read_all_or_abort(primary, out.bytes, sizeof(out.bytes));
	UT_ASSERT(memcmp(out.bytes, bytes, sizeof(bytes)) == 0);
}

/* PGRAC: real root/immutable-object integration; only CF/storage facts are
 * controlled. The root fixture is independently encoded, not published by a
 * bypass of a production authority guard. Author: SqlRush <sqlrush@gmail.com>
 */
static void
v2_install_native(ControlFileData *native, ClusterCfImageStage *stage)
{
	static uint8 operation = 1;
	uint8 uuid[16] = { 0 };
	char dir[MAXPGPATH];

	path_for(dir, sizeof(dir), "global/control_images");
	if (mkdir(dir, 0700) != 0 && errno != EEXIST)
		abort();
	path_for(dir, sizeof(dir), "global/control_images/.staging");
	if (mkdir(dir, 0700) != 0 && errno != EEXIST)
		abort();
	uuid[0] = operation++;
	uuid[6] = 0x42;
	uuid[8] = 0x82;
	if (cluster_cf_control_image_prepare(native, 53, uuid, stage) != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| cluster_cf_control_image_install(stage) != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		abort();
}

/* PGRAC: root configuration references name real canonical immutable files,
 * never a fake validator returning success. Author: SqlRush <sqlrush@gmail.com>
 */
static void
v2_install_config(uint8 bytes[66048], bool foreign)
{
	ControlRootImage decoded;
	ClusterSharedConfigIdentity id;
	ClusterSharedConfigEntry entry = { -1, "cluster.enabled", "on" };
	char config[1024], path[MAXPGPATH], hex[65];
	uint8 hash[32];
	size_t len;

	if (cluster_control_root_v2_decode(bytes, 66048, v2_storage, TEST_SYSID, &decoded) != 0)
		abort();
	memset(&id, 0, sizeof(id));
	id.system_identifier = TEST_SYSID + (foreign ? 1 : 0);
	id.database_incarnation = decoded.header.v2.database_incarnation;
	id.generation = decoded.header.v2.config_generation;
	id.configured[0] = decoded.header.v2.configured[0];
	id.configured[1] = decoded.header.v2.configured[1];
	memcpy(id.storage_uuid, decoded.header.storage_uuid, 16);
	memcpy(id.authority_uuid, decoded.header.authority_uuid, 16);
	if (cluster_shared_config_encode(&id, &entry, 1, config, sizeof(config), &len, hash) != 0)
		abort();
	path_for(path, sizeof(path), "global/config_images");
	if (mkdir(path, 0700) != 0 && errno != EEXIST)
		abort();
	for (int i = 0; i < 32; ++i)
		snprintf(hex + 2 * i, 3, "%02x", hash[i]);
	snprintf(path, sizeof(path), "%s/global/config_images/47-%s.conf", test_root, hex);
	write_all_or_abort(path, (const uint8 *)config, len);
	memcpy(bytes + 256, hash, 32);
	v2_checksums(bytes);
}

static void
v2_view_fixture(uint8 bytes[66048], ControlFileData *native, ClusterCfImageStage *stage)
{
	static pg_time_t native_time = 900;
	char path[MAXPGPATH];

	wipe_root_files();
	memset(native, 0, sizeof(*native));
	native->system_identifier = TEST_SYSID;
	native->pg_control_version = PG_CONTROL_VERSION;
	native->catalog_version_no = CATALOG_VERSION_NO;
	native->time = native_time++;
	native->state = DB_SHUTDOWNED;
	native->checkPoint = UINT64_C(0x9000000);
	v2_install_native(native, stage);
	v2_fixture(bytes);
	memcpy(bytes + 296, stage->image_sha256, 32);
	v2_checksums(bytes);
	v2_install_config(bytes, false);
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	write_all_or_abort(path, bytes, 66048);
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_BAK_REL_PATH);
	write_all_or_abort(path, bytes, 66048);
}

static bool
v2_outputs_zero(const ControlRootImage *root, const ControlFileData *common,
				const ClusterControlRootFileToken *token)
{
	return v2_zero(root, sizeof(*root)) && v2_zero(common, sizeof(*common))
		   && v2_zero(token, sizeof(*token));
}

UT_TEST(test_v2_view_selects_exact_hash_not_decoy_or_projection)
{
	uint8 bytes[66048], hash[32];
	ControlRootImage root;
	ControlFileData native, out, decoy;
	ClusterCfImageStage stage, other;
	ClusterControlRootFileToken token;

	v2_view_fixture(bytes, &native, &stage);
	decoy = native;
	decoy.time += 99;
	v2_install_native(&decoy, &other);
	cluster_cf_authority_write(&decoy);
	test_cf_mode = ShareLock;
	UT_ASSERT_EQ(
		cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &token),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(token.format_version, 2);
	UT_ASSERT_EQ(token.file_txn_seq, 7);
	UT_ASSERT_EQ(out.time, native.time);
	UT_ASSERT_EQ(out.checkPoint, native.checkPoint);
	UT_ASSERT(memcmp(root.bytes, bytes, sizeof(bytes)) == 0);
	sha256_bytes(bytes, sizeof(bytes), hash);
	UT_ASSERT(memcmp(token.image_sha256, hash, sizeof(hash)) == 0);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
	/* Common native state is NOT the database/thread admission state. */
	UT_ASSERT_EQ(out.state, DB_SHUTDOWNED);
	UT_ASSERT_EQ(root.header.v2.database_state, CLUSTER_CONTROL_ROOT_DATABASE_OPEN);
}

UT_TEST(test_v2_view_requires_clusterwide_lock_and_verified_storage)
{
	uint8 bytes[66048], foreign[16];
	ControlRootImage root;
	ControlFileData native, out;
	ClusterCfImageStage stage;
	ClusterControlRootFileToken token;
	int fault;

	v2_view_fixture(bytes, &native, &stage);
	for (fault = 0; fault < 3; ++fault) {
		test_cf_grant = fault != 0;
		test_cf_clusterwide = fault != 1;
		test_contract
			= fault == 2 ? CLUSTER_CF_CONTRACT_UNVERIFIED : CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED;
		memset(&root, 0xee, sizeof(root));
		memset(&out, 0xee, sizeof(out));
		memset(&token, 0xee, sizeof(token));
		UT_ASSERT(
			cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &token)
			!= CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(v2_outputs_zero(&root, &out, &token));
	}
	test_contract = CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED;
	memcpy(foreign, v2_storage, sizeof(foreign));
	foreign[0]++;
	UT_ASSERT_EQ(
		cluster_control_root_v2_read_control_locked(foreign, TEST_SYSID, &root, &out, &token),
		CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
	UT_ASSERT_EQ(cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID + 1, &root,
															 &out, &token),
				 CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
}

UT_TEST(test_v2_view_valid_backup_never_substitutes_for_current)
{
	uint8 bytes[66048];
	ControlRootImage root;
	ControlFileData native, out;
	ClusterCfImageStage stage;
	ClusterControlRootFileToken token;
	char primary[MAXPGPATH];

	v2_view_fixture(bytes, &native, &stage);
	path_for(primary, sizeof(primary), CLUSTER_CONTROL_ROOT_REL_PATH);
	bytes[200] ^= 1;
	write_all_or_abort(primary, bytes, sizeof(bytes));
	UT_ASSERT_EQ(
		cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &token),
		CLUSTER_CONTROL_ROOT_OK_BAK_BLOCKED);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
	UT_ASSERT(unlink(primary) == 0);
	UT_ASSERT_EQ(
		cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &token),
		CLUSTER_CONTROL_ROOT_OK_BAK_BLOCKED);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
}

UT_TEST(test_v2_view_backup_divergence_and_degraded_are_distinct)
{
	uint8 bytes[66048];
	ControlRootImage root;
	ControlFileData native, out;
	ClusterCfImageStage stage;
	ClusterControlRootFileToken token;
	char bak[MAXPGPATH];

	v2_view_fixture(bytes, &native, &stage);
	path_for(bak, sizeof(bak), CLUSTER_CONTROL_ROOT_BAK_REL_PATH);
	put_u64_le(bytes + 16, 8);
	v2_checksums(bytes);
	write_all_or_abort(bak, bytes, sizeof(bytes));
	UT_ASSERT_EQ(
		cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &token),
		CLUSTER_CONTROL_ROOT_COPY_DIVERGENT);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
	put_u64_le(bytes + 16, 7);
	put_u64_le(bytes + 336, 73);
	v2_checksums(bytes);
	write_all_or_abort(bak, bytes, sizeof(bytes));
	UT_ASSERT_EQ(
		cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &token),
		CLUSTER_CONTROL_ROOT_COPY_DIVERGENT);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
	UT_ASSERT(unlink(bak) == 0);
	UT_ASSERT_EQ(
		cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &token),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED);
	UT_ASSERT_EQ(out.time, native.time);
	UT_ASSERT_EQ(token.format_version, 2);
}

UT_TEST(test_v2_view_selected_object_failure_clears_valid_root)
{
	uint8 bytes[66048];
	ControlRootImage root;
	ControlFileData native, out;
	ClusterCfImageStage stage;
	ClusterControlRootFileToken token;
	char hash[65], object[MAXPGPATH];
	int i;

	v2_view_fixture(bytes, &native, &stage);
	for (i = 0; i < 32; ++i)
		snprintf(hash + 2 * i, 3, "%02x", stage.image_sha256[i]);
	snprintf(object, sizeof(object), "%s/global/control_images/53-%s.bin", test_root, hash);
	memset(bytes, 0x5a, PG_CONTROL_FILE_SIZE);
	write_all_or_abort(object, bytes, PG_CONTROL_FILE_SIZE);
	UT_ASSERT_EQ(
		cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &token),
		CLUSTER_CONTROL_ROOT_HASH_MISMATCH);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
	UT_ASSERT(unlink(object) == 0);
	UT_ASSERT_EQ(
		cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &token),
		CLUSTER_CONTROL_ROOT_ABSENT);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
}

UT_TEST(test_v2_view_token_covers_whole_root_not_only_control_hash)
{
	uint8 bytes[66048];
	ControlRootImage root;
	ControlFileData native, out;
	ClusterCfImageStage stage;
	ClusterControlRootFileToken before, after;
	char primary[MAXPGPATH];

	v2_view_fixture(bytes, &native, &stage);
	UT_ASSERT_EQ(
		cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &before),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	put_u64_le(bytes + 16, 8);
	put_u64_le(bytes + 336, 65);
	v2_checksums(bytes);
	path_for(primary, sizeof(primary), CLUSTER_CONTROL_ROOT_REL_PATH);
	write_all_or_abort(primary, bytes, sizeof(bytes));
	UT_ASSERT_EQ(
		cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &after),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(after.file_txn_seq, before.file_txn_seq + 1);
	UT_ASSERT(memcmp(before.image_sha256, after.image_sha256, 32) != 0);
	UT_ASSERT_EQ(out.time, native.time);
}

UT_TEST(test_v2_view_rejects_v1_and_invalid_input_without_conversion)
{
	uint8 bytes[66048];
	ControlRootImage root;
	ControlFileData out;
	ClusterControlRootMigrationImage legacy;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken token;
	char primary[MAXPGPATH];

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&legacy, &round, &token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	path_for(primary, sizeof(primary), CLUSTER_CONTROL_ROOT_REL_PATH);
	read_all_or_abort(primary, bytes, sizeof(bytes));
	UT_ASSERT_EQ(
		cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &token),
		CLUSTER_CONTROL_ROOT_BAD_VERSION);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
	read_all_or_abort(primary, root.bytes, sizeof(root.bytes));
	UT_ASSERT(memcmp(root.bytes, bytes, sizeof(bytes)) == 0);
	UT_ASSERT_EQ(cluster_control_root_v2_read_control_locked(NULL, TEST_SYSID, &root, &out, &token),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
}

UT_TEST(test_v2_view_single_node_does_not_bypass_shared_storage_qualification)
{
	uint8 bytes[66048];
	ControlRootImage root;
	ControlFileData native, out;
	ClusterCfImageStage stage;
	ClusterControlRootFileToken token;

	v2_view_fixture(bytes, &native, &stage);
	test_node_count = 1;
	test_contract = CLUSTER_CF_CONTRACT_UNVERIFIED;
	UT_ASSERT_EQ(
		cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &token),
		CLUSTER_CONTROL_ROOT_STORAGE_CONTRACT_UNVERIFIED);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
	test_contract = CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED;
	UT_ASSERT_EQ(
		cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &token),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(out.time, native.time);
}

/* PGRAC: composition consumes independently encoded roots and actual anchor
 * objects. The anchor's standalone tests pin its independent byte fixture.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static void
v2_anchor_object(uint8 bytes[66048], const ClusterRecoveryAnchorV2 *anchor,
				 const ClusterControlRootIdentity *path_identity, char path[MAXPGPATH])
{
	uint8 image[512], hash[32];
	char dir[MAXPGPATH], hex[65];
	size_t i;
	int node = path_identity->origin_node_id;

	UT_ASSERT_EQ(cluster_recovery_anchor_v2_encode(anchor, image), 0);
	sha256_bytes(image, sizeof(image), hash);
	for (i = 0; i < 32; ++i)
		snprintf(hex + i * 2, 3, "%02x", hash[i]);
	path_for(dir, sizeof(dir), "global/anchor_images");
	UT_ASSERT(mkdir(dir, 0700) == 0 || errno == EEXIST);
	snprintf(dir, sizeof(dir), "%s/global/anchor_images/thread_%u", cluster_shared_data_dir,
			 path_identity->origin_thread_id);
	UT_ASSERT(mkdir(dir, 0700) == 0 || errno == EEXIST);
	snprintf(dir, sizeof(dir), "%s/global/anchor_images/thread_%u/generation_" UINT64_FORMAT,
			 cluster_shared_data_dir, path_identity->origin_thread_id,
			 path_identity->origin_owner_incarnation);
	UT_ASSERT(mkdir(dir, 0700) == 0 || errno == EEXIST);
	snprintf(path, MAXPGPATH, "%s/anchor_" UINT64_FORMAT "-%s.bin", dir, anchor->anchor_generation,
			 hex);
	write_all_or_abort(path, image, sizeof(image));
	memcpy(bytes + 512 + node * 512 + 264, hash, 32);
	v2_checksums(bytes);
}

static void
v2_write_roots(uint8 bytes[66048])
{
	char path[MAXPGPATH];

	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	write_all_or_abort(path, bytes, 66048);
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_BAK_REL_PATH);
	write_all_or_abort(path, bytes, 66048);
}

static void
v2_claim_path(const ClusterControlRootIdentity *id, char path[MAXPGPATH])
{
	snprintf(path, MAXPGPATH, "%s/thread_%u/generation_" UINT64_FORMAT "/pgrac_thread.claim",
			 cluster_wal_threads_dir, id->origin_thread_id, id->origin_owner_incarnation);
}

/* PGRAC: independently create the selected claim before constructing anchors.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static void
v2_claim_object(uint8 bytes[66048], int node, ControlRootImage *root)
{
	uint8 image[112] = { 0 }, hash[32];
	ClusterControlRootIdentity *id = &root->records[node].identity;
	char dir[MAXPGPATH], path[MAXPGPATH];

	put_u32_le(image, UINT32_C(0x50475443));
	put_u16_le(image + 4, 2);
	put_u16_le(image + 6, id->origin_thread_id);
	put_u32_le(image + 8, id->origin_node_id);
	put_u64_le(image + 16, id->system_identifier);
	put_u64_le(image + 24, root->header.v2.database_incarnation);
	put_u64_le(image + 32, id->origin_owner_incarnation);
	memcpy(image + 40, id->storage_uuid, 16);
	memcpy(image + 56, id->authority_uuid, 16);
	put_u64_le(image + 72, id->root_lineage_seq);
	put_u64_le(image + 80, id->thread_claim_created_at);
	put_u64_le(image + 88, root->header.v2.config_generation - 1);
	put_u64_le(image + 96, 1);
	id->thread_claim_crc32c = image_crc(image, 104);
	put_u32_le(image + 104, id->thread_claim_crc32c);
	sha256_bytes(image, sizeof(image), hash);
	memcpy(root->refs[node].claim_sha256, hash, 32);
	memcpy(bytes + 512 + node * 512 + 296, hash, 32);
	put_u32_le(bytes + 512 + node * 512 + 168, id->thread_claim_crc32c);
	v2_checksums(bytes);
	snprintf(dir, sizeof(dir), "%s/thread_%u", cluster_wal_threads_dir, id->origin_thread_id);
	UT_ASSERT(mkdir(dir, 0700) == 0 || errno == EEXIST);
	snprintf(dir, sizeof(dir), "%s/thread_%u/generation_" UINT64_FORMAT, cluster_wal_threads_dir,
			 id->origin_thread_id, id->origin_owner_incarnation);
	UT_ASSERT(mkdir(dir, 0700) == 0 || errno == EEXIST);
	v2_claim_path(id, path);
	write_all_or_abort(path, image, sizeof(image));
}

static void
v2_thread_fixture(uint8 bytes[66048], ClusterRecoveryAnchorV2 anchors[2])
{
	ControlFileData native;
	ClusterCfImageStage stage;
	ControlRootImage root;
	char path[MAXPGPATH];
	int i;

	v2_view_fixture(bytes, &native, &stage);
	native.MaxConnections = 300;
	native.checkPointCopy.nextOid = 60001;
	v2_install_native(&native, &stage);
	memcpy(bytes + 296, stage.image_sha256, 32);
	v2_checksums(bytes);
	UT_ASSERT_EQ(cluster_control_root_v2_decode(bytes, 66048, v2_storage, TEST_SYSID, &root), 0);
	memset(anchors, 0, 2 * sizeof(*anchors));
	for (i = 0; i < 2; ++i) {
		int node = i == 0 ? 0 : 127;
		ClusterRecoveryAnchorV2 *a = &anchors[i];

		v2_claim_object(bytes, node, &root);
		a->identity = root.records[node].identity;
		a->database_incarnation = root.header.v2.database_incarnation;
		a->config_generation = root.header.v2.config_generation;
		a->anchor_generation = root.refs[node].anchor_generation;
		memcpy(a->claim_sha256, root.refs[node].claim_sha256, 32);
		a->state = DB_IN_PRODUCTION;
		a->checkpoint_copy.redo = root.records[node].checkpoint_lower_lsn;
		a->checkpoint = a->checkpoint_copy.redo + 128;
		a->write_time = 1001 + node;
		a->checkpoint_copy.ThisTimeLineID = root.records[node].checkpoint_tli;
		a->checkpoint_copy.PrevTimeLineID = root.records[node].checkpoint_tli;
		a->checkpoint_copy.nextOid = 40 + node; /* deliberately not common */
		a->min_recovery_point = a->checkpoint_copy.redo + 64;
		a->min_recovery_tli = root.records[node].checkpoint_tli;
		a->unlogged_lsn = UINT64_C(0x2000000) + node;
		a->max_connections = 300 + node;
		a->wal_level = 1;
		a->max_worker_processes = 16;
		a->max_wal_senders = 5;
		a->max_locks_per_xact = 64;
		v2_anchor_object(bytes, a, &a->identity, path);
	}
	v2_write_roots(bytes);
}

UT_TEST(test_v2_thread_view_selects_each_exact_thread)
{
	uint8 bytes[66048];
	ClusterRecoveryAnchorV2 anchors[2];
	ControlRootImage root;
	ControlFileData out;
	ClusterControlRootFileToken token;
	pg_crc32c crc;
	int i;

	v2_thread_fixture(bytes, anchors);
	test_cf_mode = ShareLock;
	for (i = 0; i < 2; ++i) {
		UT_ASSERT_EQ(
			cluster_control_root_v2_read_thread_locked(&anchors[i].identity, &root, &out, &token),
			0);
		UT_ASSERT_EQ(out.checkPoint, anchors[i].checkpoint);
		UT_ASSERT_EQ(out.minRecoveryPoint, anchors[i].min_recovery_point);
		UT_ASSERT_EQ(out.unloggedLSN, anchors[i].unlogged_lsn);
		UT_ASSERT_EQ(out.state, DB_IN_PRODUCTION);
		UT_ASSERT_EQ(out.MaxConnections, i == 0 ? 300 : 427);
		UT_ASSERT_EQ(out.checkPointCopy.nextOid, 60001);
		UT_ASSERT_EQ(token.file_txn_seq, 7);
		UT_ASSERT_EQ(token.format_version, 2);
		INIT_CRC32C(crc);
		COMP_CRC32C(crc, &out, offsetof(ControlFileData, crc));
		FIN_CRC32C(crc);
		UT_ASSERT(EQ_CRC32C(crc, out.crc));
	}
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
}

UT_TEST(test_v2_thread_view_uses_lifecycle_not_old_clean_anchor)
{
	for (int life = 1; life <= 5; ++life) {
		uint8 bytes[66048];
		ClusterRecoveryAnchorV2 anchors[2];
		ControlRootImage root;
		ControlFileData out;
		ClusterControlRootFileToken token;
		char path[MAXPGPATH];
		ClusterControlRootResult result;

		v2_thread_fixture(bytes, anchors);
		anchors[0].state = DB_SHUTDOWNED;
		anchors[0].min_recovery_point = 0;
		anchors[0].min_recovery_tli = 0;
		v2_anchor_object(bytes, &anchors[0], &anchors[0].identity, path);
		bytes[512 + 10] = life;
		v2_checksums(bytes);
		v2_write_roots(bytes);
		result
			= cluster_control_root_v2_read_thread_locked(&anchors[0].identity, &root, &out, &token);
		if (life == CLUSTER_CONTROL_ROOT_LIFECYCLE_RETIRED) {
			UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID);
			UT_ASSERT(v2_outputs_zero(&root, &out, &token));
		} else {
			DBState expected = life == CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN ? DB_IN_PRODUCTION
							   : life == CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED
								   ? DB_SHUTDOWNED
								   : DB_IN_CRASH_RECOVERY;
			pg_crc32c crc;
			UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
			UT_ASSERT_EQ(out.state, expected);
			UT_ASSERT_EQ(out.checkPoint, anchors[0].checkpoint);
			UT_ASSERT_EQ(root.records[127].lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN);
			UT_ASSERT_EQ(root.header.v2.database_state, CLUSTER_CONTROL_ROOT_DATABASE_OPEN);
			INIT_CRC32C(crc);
			COMP_CRC32C(crc, &out, offsetof(ControlFileData, crc));
			FIN_CRC32C(crc);
			UT_ASSERT(EQ_CRC32C(crc, out.crc));
		}
	}
}

UT_TEST(test_v2_thread_view_rejects_false_clean_or_unsupported_native_state)
{
	for (int fault = 0; fault < 5; ++fault) {
		uint8 bytes[66048];
		ClusterRecoveryAnchorV2 anchors[2];
		ControlRootImage root;
		ControlFileData out;
		ClusterControlRootFileToken token;
		char path[MAXPGPATH];
		v2_thread_fixture(bytes, anchors);
		if (fault < 2) {
			bytes[512 + 10] = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
			anchors[0].state = fault == 0 ? DB_IN_PRODUCTION : DB_SHUTDOWNED;
		} else {
			const DBState forbidden[]
				= { DB_STARTUP, DB_SHUTDOWNED_IN_RECOVERY, DB_IN_ARCHIVE_RECOVERY };
			anchors[0].state = forbidden[fault - 2];
		}
		v2_anchor_object(bytes, &anchors[0], &anchors[0].identity, path);
		v2_checksums(bytes);
		v2_write_roots(bytes);
		UT_ASSERT_EQ(
			cluster_control_root_v2_read_thread_locked(&anchors[0].identity, &root, &out, &token),
			CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID);
		UT_ASSERT(v2_outputs_zero(&root, &out, &token));
	}
}

UT_TEST(test_v2_thread_view_rejects_stale_caller_and_absent_record)
{
	uint8 bytes[66048];
	ClusterRecoveryAnchorV2 anchors[2];
	ClusterControlRootIdentity self;
	ControlRootImage root;
	ControlFileData out;
	ClusterControlRootFileToken token;

	v2_thread_fixture(bytes, anchors);
	self = anchors[0].identity;
	self.origin_owner_incarnation++;
	UT_ASSERT_EQ(cluster_control_root_v2_read_thread_locked(&self, &root, &out, &token),
				 CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
	self = anchors[0].identity;
	self.origin_thread_id = 2;
	self.origin_node_id = 1;
	UT_ASSERT_EQ(cluster_control_root_v2_read_thread_locked(&self, &root, &out, &token),
				 CLUSTER_CONTROL_ROOT_ABSENT);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
}

UT_TEST(test_v2_thread_view_clears_root_after_missing_anchor)
{
	uint8 bytes[66048];
	ClusterRecoveryAnchorV2 anchors[2];
	ControlRootImage root;
	ControlFileData out;
	ClusterControlRootFileToken token;
	char path[MAXPGPATH];

	v2_thread_fixture(bytes, anchors);
	v2_anchor_object(bytes, &anchors[0], &anchors[0].identity, path);
	UT_ASSERT_EQ(unlink(path), 0);
	UT_ASSERT_EQ(
		cluster_control_root_v2_read_thread_locked(&anchors[0].identity, &root, &out, &token),
		CLUSTER_CONTROL_ROOT_ABSENT);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
}

UT_TEST(test_v2_thread_view_rejects_selected_foreign_or_inconsistent_anchor)
{
	uint8 bytes[66048];
	ClusterRecoveryAnchorV2 anchors[2];
	ClusterControlRootIdentity self;
	ControlRootImage root;
	ControlFileData out;
	ClusterControlRootFileToken token;
	char path[MAXPGPATH];
	int fault;

	for (fault = 0; fault < 3; ++fault) {
		v2_thread_fixture(bytes, anchors);
		self = anchors[0].identity;
		if (fault == 0)
			anchors[0].database_incarnation++;
		else if (fault == 1)
			anchors[0].checkpoint_copy.redo += 8192;
		else
			anchors[0].checkpoint_copy.ThisTimeLineID++;
		v2_anchor_object(bytes, &anchors[0], &self, path);
		v2_write_roots(bytes);
		UT_ASSERT_EQ(cluster_control_root_v2_read_thread_locked(&self, &root, &out, &token),
					 CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
		UT_ASSERT(v2_outputs_zero(&root, &out, &token));
	}
}

UT_TEST(test_v2_thread_view_requires_physical_selected_claim)
{
	for (int fault = 0; fault < 5; ++fault) {
		uint8 bytes[66048], claim[113];
		ClusterRecoveryAnchorV2 anchors[2];
		ControlRootImage root;
		ControlFileData out;
		ClusterControlRootFileToken token;
		char path[MAXPGPATH];
		ClusterControlRootResult expected;

		v2_thread_fixture(bytes, anchors);
		v2_claim_path(&anchors[0].identity, path);
		read_all_or_abort(path, claim, 112);
		if (fault == 0) {
			UT_ASSERT_EQ(unlink(path), 0);
			expected = CLUSTER_CONTROL_ROOT_ABSENT;
		} else if (fault == 1) {
			claim[32] ^= 1;
			write_all_or_abort(path, claim, 112);
			expected = CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC;
		} else if (fault == 2) {
			claim[4] = 1;
			put_u32_le(claim + 104, image_crc(claim, 104));
			write_all_or_abort(path, claim, 112);
			expected = CLUSTER_CONTROL_ROOT_BAD_VERSION;
		} else if (fault == 3) {
			claim[112] = 0;
			write_all_or_abort(path, claim, 113);
			expected = CLUSTER_CONTROL_ROOT_BAD_SIZE;
		} else {
			++claim[32];
			put_u32_le(claim + 104, image_crc(claim, 104));
			write_all_or_abort(path, claim, 112);
			expected = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
		}
		memset(&root, 0xa5, sizeof(root));
		memset(&out, 0xa5, sizeof(out));
		memset(&token, 0xa5, sizeof(token));
		UT_ASSERT_EQ(
			cluster_control_root_v2_read_thread_locked(&anchors[0].identity, &root, &out, &token),
			expected);
		UT_ASSERT(v2_outputs_zero(&root, &out, &token));
	}
}

/* PGRAC: exact-filesystem checkpoint publication, no fake positive publisher.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static XLogRecPtr test_checkpoint_end;
static uint32 test_checkpoint_crc;
static ControlFileData test_checkpoint_output;
static ClusterWalDurablePrefixRef test_checkpoint_prefix_ref;
static ClusterWalDurablePrefix test_checkpoint_prefix;
static char test_checkpoint_prefix_path[MAXPGPATH];
static ClusterWalDurablePrefixRef test_restart_ref;
static bool test_restart_ref_valid;

bool
cluster_wal_thread_restart_v2_ref(ClusterWalDurablePrefixRef *out)
{
	*out = test_restart_ref;
	return test_restart_ref_valid;
}

bool
cluster_wal_thread_current_v2_ref(ClusterWalDurablePrefixRef *out)
{
	*out = test_checkpoint_prefix_ref;
	return test_wal_validated;
}

static void
v2_checkpoint_prefix_write(void)
{
	uint8 bytes[CLUSTER_WAL_DURABLE_PREFIX_BYTES];
	UT_ASSERT_EQ(cluster_wal_durable_prefix_encode(&test_checkpoint_prefix_ref,
												   &test_checkpoint_prefix, bytes),
				 0);
	write_all_or_abort(test_checkpoint_prefix_path, bytes, sizeof(bytes));
}

static void
v2_checkpoint_wal_path(const ClusterControlRootIdentity *self, XLogRecPtr position, TimeLineID tli,
					   char path[MAXPGPATH])
{
	char filename[MAXFNAMELEN];
	XLogSegNo segno;
	XLByteToSeg(position, segno, wal_segment_size);
	XLogFileName(filename, tli, segno, wal_segment_size);
	snprintf(path, MAXPGPATH, "%s/thread_%u/generation_" UINT64_FORMAT "/%s",
			 cluster_wal_threads_dir, self->origin_thread_id, self->origin_owner_incarnation,
			 filename);
}

/* Actual native record framing, including page and segment continuation. No
 * replacement of XLogReader or its record/CRC validation. Faults alter bytes. */
static void
v2_checkpoint_wal_record(const ClusterControlRootIdentity *self, ControlFileData *candidate,
						 int fault)
{
	uint8 record_bytes[SizeOfXLogRecord + 2 + sizeof(CheckPoint)];
	XLogRecord record;
	CheckPoint checkpoint = candidate->checkPointCopy;
	XLogRecPtr position = candidate->checkPoint;
	XLogSegNo previous_segment = UINT64_MAX;
	size_t used = 0;
	pg_crc32c crc;

	memset(&record, 0, sizeof(record));
	memset(record_bytes, 0, sizeof(record_bytes));
	if (fault == 6)
		checkpoint.time++;
	record.xl_tot_len = sizeof(record_bytes);
	record.xl_prev
		= fault == 8 ? 0 : Min(candidate->checkPointCopy.redo, candidate->checkPoint - 8);
	record.xl_info = (candidate->state == DB_SHUTDOWNED) != (fault == 5) ? XLOG_CHECKPOINT_SHUTDOWN
																		 : XLOG_CHECKPOINT_ONLINE;
	record.xl_rmid = RM_XLOG_ID;
	record_bytes[SizeOfXLogRecord] = XLR_BLOCK_ID_DATA_SHORT;
	record_bytes[SizeOfXLogRecord + 1] = sizeof(CheckPoint);
	memcpy(record_bytes + SizeOfXLogRecord + 2, &checkpoint, sizeof(checkpoint));
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, record_bytes + SizeOfXLogRecord, sizeof(record_bytes) - SizeOfXLogRecord);
	COMP_CRC32C(crc, &record, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(crc);
	/* PGRAC: a real zero-CRC WAL record, not a substituted decoder result.
	 * Solve the 32-bit linear CRC delta in the diagnostic checkpoint time.
	 * Author: SqlRush <sqlrush@gmail.com>
	 */
	if (fault == 7) {
		uint32 basis[32] = { 0 }, masks[32] = { 0 }, solution = 0;
		uint8 *patch = record_bytes + SizeOfXLogRecord + 2 + offsetof(CheckPoint, time);
		uint32 target = crc;
		for (unsigned bit = 0; bit < 32; ++bit) {
			pg_crc32c changed;
			uint32 delta, mask = UINT32_C(1) << bit;
			patch[bit / 8] ^= 1U << (bit % 8);
			INIT_CRC32C(changed);
			COMP_CRC32C(changed, record_bytes + SizeOfXLogRecord,
						sizeof(record_bytes) - SizeOfXLogRecord);
			COMP_CRC32C(changed, &record, offsetof(XLogRecord, xl_crc));
			FIN_CRC32C(changed);
			patch[bit / 8] ^= 1U << (bit % 8);
			delta = changed ^ crc;
			for (int pivot = 31; pivot >= 0; --pivot) {
				if ((delta & (UINT32_C(1) << pivot)) == 0)
					continue;
				if (basis[pivot] != 0) {
					delta ^= basis[pivot];
					mask ^= masks[pivot];
				} else {
					basis[pivot] = delta;
					masks[pivot] = mask;
					break;
				}
			}
		}
		for (int pivot = 31; pivot >= 0; --pivot) {
			if ((target & (UINT32_C(1) << pivot)) != 0) {
				UT_ASSERT(basis[pivot] != 0);
				target ^= basis[pivot];
				solution ^= masks[pivot];
			}
		}
		UT_ASSERT_EQ(target, 0);
		for (unsigned bit = 0; bit < 32; ++bit)
			if ((solution & (UINT32_C(1) << bit)) != 0)
				patch[bit / 8] ^= 1U << (bit % 8);
		memcpy(&candidate->checkPointCopy, record_bytes + SizeOfXLogRecord + 2, sizeof(CheckPoint));
		INIT_CRC32C(candidate->crc);
		COMP_CRC32C(candidate->crc, candidate, offsetof(ControlFileData, crc));
		FIN_CRC32C(candidate->crc);
		INIT_CRC32C(crc);
		COMP_CRC32C(crc, record_bytes + SizeOfXLogRecord, sizeof(record_bytes) - SizeOfXLogRecord);
		COMP_CRC32C(crc, &record, offsetof(XLogRecord, xl_crc));
		FIN_CRC32C(crc);
		UT_ASSERT_EQ(crc, 0);
	}
	record.xl_crc = crc;
	test_checkpoint_crc = crc;
	memcpy(record_bytes, &record, SizeOfXLogRecord);
	if (fault == 4)
		record_bytes[sizeof(record_bytes) - 1] ^= 1;
	while (used < sizeof(record_bytes)) {
		uint8 page[XLOG_BLCKSZ];
		XLogLongPageHeaderData header;
		XLogRecPtr page_start = position - position % XLOG_BLCKSZ;
		XLogSegNo segno;
		size_t offset = position % XLOG_BLCKSZ, count;
		char path[MAXPGPATH];
		int fd;
		XLByteToSeg(position, segno, wal_segment_size);
		v2_checkpoint_wal_path(self, position, candidate->checkPointCopy.ThisTimeLineID, path);
		fd = open(path, O_RDWR | O_CREAT | (segno != previous_segment ? O_TRUNC : 0), 0600);
		if (fd < 0 || ftruncate(fd, wal_segment_size) != 0)
			abort();
		memset(&header, 0, sizeof(header));
		header.std.xlp_magic = XLOG_PAGE_MAGIC;
		header.std.xlp_info = XLP_LONG_HEADER;
		header.std.xlp_tli = candidate->checkPointCopy.ThisTimeLineID + (fault == 2);
		header.std.xlp_thread_id = fault == 1 ? self->origin_thread_id + 1 : self->origin_thread_id;
		header.std.xlp_pageaddr = segno * wal_segment_size;
		header.xlp_sysid = self->system_identifier + (fault == 3);
		header.xlp_seg_size = wal_segment_size;
		header.xlp_xlog_blcksz = XLOG_BLCKSZ;
		if (segno != previous_segment) {
			memset(page, 0, sizeof(page));
			memcpy(page, &header, SizeOfXLogLongPHD);
			if (pwrite(fd, page, sizeof(page), 0) != sizeof(page))
				abort();
		}
		memset(page, 0, sizeof(page));
		header.std.xlp_pageaddr = page_start;
		if (page_start % wal_segment_size != 0)
			header.std.xlp_info = 0;
		if (used != 0) {
			header.std.xlp_info |= XLP_FIRST_IS_CONTRECORD;
			header.std.xlp_rem_len = sizeof(record_bytes) - used;
			offset = page_start % wal_segment_size == 0 ? SizeOfXLogLongPHD : SizeOfXLogShortPHD;
		}
		memcpy(page, &header,
			   page_start % wal_segment_size == 0 ? SizeOfXLogLongPHD : SizeOfXLogShortPHD);
		count = Min(sizeof(record_bytes) - used, XLOG_BLCKSZ - offset);
		memcpy(page + offset, record_bytes + used, count);
		if (pwrite(fd, page, sizeof(page), page_start % wal_segment_size) != sizeof(page)
			|| close(fd) != 0)
			abort();
		used += count;
		position = page_start + offset + count;
		previous_segment = segno;
	}
	test_checkpoint_end = MAXALIGN(position);
	test_flush = test_checkpoint_end;
	test_insert = test_checkpoint_end;
	test_checkpoint_prefix
		= (ClusterWalDurablePrefix){ 11, test_checkpoint_end, candidate->checkPoint,
									 test_checkpoint_crc };
	v2_checkpoint_prefix_write();
}

static void
v2_checkpoint_fixture(uint8 before[66048], ClusterControlRootIdentity *self,
					  ControlFileData *candidate)
{
	ClusterRecoveryAnchorV2 anchors[2];
	ControlRootImage root;
	ClusterControlRootFileToken token;
	char dir[MAXPGPATH];

	v2_thread_fixture(before, anchors);
	*self = anchors[0].identity;
	UT_ASSERT_EQ(cluster_control_root_v2_read_thread_locked(self, &root, candidate, &token), 0);
	memset(&test_checkpoint_prefix_ref, 0, sizeof(test_checkpoint_prefix_ref));
	test_checkpoint_prefix_ref.claim.identity = *self;
	test_checkpoint_prefix_ref.claim.database_incarnation = root.header.v2.database_incarnation;
	test_checkpoint_prefix_ref.claim.max_config_generation = root.header.v2.config_generation;
	memcpy(test_checkpoint_prefix_ref.claim.claim_sha256, root.refs[0].claim_sha256, 32);
	test_checkpoint_prefix_ref.timeline = candidate->checkPointCopy.ThisTimeLineID;
	snprintf(dir, sizeof(dir), "%s/thread_%u/generation_" UINT64_FORMAT "/durable_prefix",
			 cluster_wal_threads_dir, self->origin_thread_id, self->origin_owner_incarnation);
	UT_ASSERT(mkdir(dir, 0700) == 0 || errno == EEXIST);
	snprintf(test_checkpoint_prefix_path, sizeof(test_checkpoint_prefix_path), "%s/current", dir);
	candidate->checkPoint += 8192;
	candidate->checkPointCopy.redo += 8192;
	candidate->time++;
	candidate->checkPointCopy.time++;
	INIT_CRC32C(candidate->crc);
	COMP_CRC32C(candidate->crc, candidate, offsetof(ControlFileData, crc));
	FIN_CRC32C(candidate->crc);
	snprintf(dir, sizeof(dir), "%s/global/anchor_images/thread_1/generation_99/.staging",
			 test_root);
	UT_ASSERT(mkdir(dir, 0700) == 0 || errno == EEXIST);
	test_checkpoint_mode = true;
	MyAuxProcType = CheckpointerProcess;
	cluster_node_id = 0;
	cluster_enabled = cluster_controlfile_shared_authority = true;
	test_self_incarnation = test_membership_incarnation = self->origin_owner_incarnation;
	test_own_thread = self->origin_thread_id;
	test_member_state = CLUSTER_MEMBER_MEMBER;
	test_epoch = 97;
	test_serving = test_fence = test_wal_validated = true;
	test_prebump = false;
	v2_checkpoint_wal_record(self, candidate, 0);
	test_flush_tli = candidate->checkPointCopy.ThisTimeLineID;
	test_cf_mode = NoLock;
	test_cf_lock_calls = test_durable_rename_calls = 0;
	test_checkpoint_outer_cf = false;
	cluster_shared_config = true;
	test_projection_sync_fault = false;
	test_projection_syncs = 0;
}

static ClusterControlRootResult
v2_checkpoint_publish(const ClusterControlRootIdentity *self, const ControlFileData *candidate,
					  ClusterControlRootSnapshot *out, ClusterControlRootFileToken *token)
{
	ClusterControlRootResult result;
	memset(&test_checkpoint_output, 0xa5, sizeof(test_checkpoint_output));
	result = cluster_control_root_v2_checkpoint_publish(self, candidate, test_checkpoint_end, out,
														token, &test_checkpoint_output);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		UT_ASSERT(v2_zero(&test_checkpoint_output, sizeof(test_checkpoint_output)));
	return result;
}

static void
v2_assert_primary_unchanged(const uint8 before[66048])
{
	uint8 bytes[66048];
	char path[MAXPGPATH];

	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	read_all_or_abort(path, bytes, sizeof(bytes));
	UT_ASSERT(memcmp(before, bytes, sizeof(bytes)) == 0);
}

static void
v2_assert_anchor_staging_empty(void)
{
	char path[MAXPGPATH];
	DIR *dir;
	struct dirent *entry;
	int count = 0;

	snprintf(path, sizeof(path), "%s/global/anchor_images/thread_1/generation_99/.staging",
			 test_root);
	dir = opendir(path);
	UT_ASSERT(dir != NULL);
	if (dir == NULL)
		return;
	while ((entry = readdir(dir)) != NULL)
		if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0)
			count++;
	UT_ASSERT_EQ(closedir(dir), 0);
	UT_ASSERT_EQ(count, 0);
}

UT_TEST(test_v2_checkpoint_advances_one_thread_and_preserves_common)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	ControlRootImage root;
	ControlFileData candidate, view;

	v2_checkpoint_fixture(before, &self, &candidate);
	UT_ASSERT_EQ(v2_checkpoint_publish(&self, &candidate, &out, &token), 0);
	UT_ASSERT_EQ(out.checkpoint_lower_lsn, candidate.checkPointCopy.redo);
	UT_ASSERT_EQ(out.validated_tail_lsn_exclusive, test_checkpoint_end);
	UT_ASSERT_EQ(token.file_txn_seq, 8);
	UT_ASSERT_EQ(token.format_version, 2);
	UT_ASSERT_EQ(test_walr_begin_calls, 1);
	UT_ASSERT_EQ(test_walr_end_calls, 1);
	UT_ASSERT(test_walr_begin_order < test_cf_acquire_order);
	UT_ASSERT(test_last_rename_order < test_cf_release_order);
	UT_ASSERT_EQ(cluster_control_root_v2_read_thread_locked(&self, &root, &view, &token), 0);
	UT_ASSERT_EQ(view.checkPoint, candidate.checkPoint);
	UT_ASSERT_EQ(root.refs[0].anchor_generation, 67);
	UT_ASSERT(memcmp(root.bytes + 196, before + 196, 180) == 0);
	UT_ASSERT(memcmp(root.bytes + 1024, before + 1024, 66048 - 1024) == 0);
	UT_ASSERT_EQ(view.checkPointCopy.nextOid, 60001);
	UT_ASSERT(memcmp(&test_checkpoint_output, &view, sizeof(view)) == 0);
	UT_ASSERT_EQ(root.records[0].root_publish_seq, 11);
	UT_ASSERT_EQ(root.records[0].identity.origin_owner_incarnation, 99);
	v2_assert_anchor_staging_empty();
}

static void
v2_shutdown_checkpoint_fixture(uint8 before[66048], ClusterControlRootIdentity *self,
							   ControlFileData *candidate)
{
	v2_checkpoint_fixture(before, self, candidate);
	candidate->state = DB_SHUTDOWNED;
	candidate->checkPointCopy.redo = candidate->checkPoint;
	INIT_CRC32C(candidate->crc);
	COMP_CRC32C(candidate->crc, candidate, offsetof(ControlFileData, crc));
	FIN_CRC32C(candidate->crc);
	v2_checkpoint_wal_record(self, candidate, 0);
}

static ClusterControlRootResult
v2_shutdown_checkpoint_publish(const ClusterControlRootIdentity *self,
							   const ControlFileData *candidate, ClusterControlRootSnapshot *out,
							   ClusterControlRootFileToken *token)
{
	ClusterControlRootResult result;
	memset(&test_checkpoint_output, 0xa5, sizeof(test_checkpoint_output));
	result = cluster_control_root_v2_shutdown_checkpoint_publish(
		self, candidate, test_checkpoint_end, out, token, &test_checkpoint_output);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		UT_ASSERT(v2_zero(&test_checkpoint_output, sizeof(test_checkpoint_output)));
	return result;
}

UT_TEST(test_v2_shutdown_checkpoint_evidence_is_not_clean_close)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	ControlRootImage root;
	ClusterRecoveryAnchorRefV2 ref = { 0 };
	ControlFileData candidate, view, raw;

	v2_shutdown_checkpoint_fixture(before, &self, &candidate);
	UT_ASSERT_EQ(v2_shutdown_checkpoint_publish(&self, &candidate, &out, &token), 0);
	UT_ASSERT_EQ(out.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN);
	UT_ASSERT_EQ(out.checkpoint_lower_lsn, candidate.checkPoint);
	UT_ASSERT_EQ(out.validated_tail_lsn_exclusive, test_checkpoint_end);
	UT_ASSERT_EQ(cluster_control_root_v2_read_thread_locked(&self, &root, &view, &token), 0);
	UT_ASSERT_EQ(view.state, DB_IN_PRODUCTION);
	UT_ASSERT_EQ(test_checkpoint_output.state, DB_IN_PRODUCTION);
	UT_ASSERT_EQ(root.header.v2.database_state, CLUSTER_CONTROL_ROOT_DATABASE_OPEN);
	UT_ASSERT(memcmp(root.bytes + 196, before + 196, 180) == 0);
	UT_ASSERT(memcmp(root.bytes + 1024, before + 1024, 66048 - 1024) == 0);
	ref.identity = self;
	ref.database_incarnation = root.header.v2.database_incarnation;
	ref.max_config_generation = root.header.v2.config_generation;
	ref.anchor_generation = root.refs[0].anchor_generation;
	memcpy(ref.anchor_sha256, root.refs[0].anchor_sha256, 32);
	memcpy(ref.claim_sha256, root.refs[0].claim_sha256, 32);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_read_locked(&ref, &view, &raw), 0);
	UT_ASSERT_EQ(raw.state, DB_SHUTDOWNED);
	UT_ASSERT_EQ(raw.checkPoint, candidate.checkPoint);
	UT_ASSERT_EQ(raw.checkPointCopy.redo, candidate.checkPoint);
	UT_ASSERT_EQ(test_walr_begin_calls, 1);
	UT_ASSERT_EQ(test_walr_end_calls, 1);
	v2_assert_anchor_staging_empty();
}

/* PGRAC: execute the real root/anchor/claim/WAL consumer. A compatibility
 * projection, legacy STOPPED slot or caller boolean is never stop evidence.
 * Author: SqlRush <sqlrush@gmail.com> */
static void
v2_stop_observation_fixture(uint8 before[66048], ClusterControlRootIdentity *self,
							ControlFileData *candidate)
{
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	char path[MAXPGPATH];
	v2_shutdown_checkpoint_fixture(before, self, candidate);
	UT_ASSERT_EQ(v2_shutdown_checkpoint_publish(self, candidate, &out, &token), 0);
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	read_all_or_abort(path, before, 66048);
	ShutdownRequestPending = true;
}

UT_TEST(test_v2_stop_observation_does_not_close_or_publish)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ControlFileData candidate;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	int writes;

	v2_stop_observation_fixture(before, &self, &candidate);
	if (ut_current_failed)
		return;
	writes = test_durable_rename_calls;
	UT_ASSERT_EQ(
		cluster_control_root_v2_shutdown_observe(&test_checkpoint_prefix_ref, &out, &token), 0);
	UT_ASSERT_EQ(out.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN);
	UT_ASSERT_EQ(out.validated_tail_lsn_exclusive, test_checkpoint_end);
	UT_ASSERT_EQ(token.file_txn_seq, 8);
	UT_ASSERT_EQ(test_durable_rename_calls, writes);
	UT_ASSERT_EQ(test_cf_mode, NoLock);
	UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
	v2_assert_primary_unchanged(before);
}

UT_TEST(test_v2_stop_observation_rejects_late_wal_and_changed_owner)
{
	for (int fault = 0; fault < 13; ++fault) {
		uint8 before[66048];
		ClusterControlRootIdentity self;
		ControlFileData candidate;
		ClusterControlRootSnapshot out;
		ClusterControlRootFileToken token;
		ClusterWalDurablePrefixRef ref;
		v2_stop_observation_fixture(before, &self, &candidate);
		if (ut_current_failed)
			return;
		ref = test_checkpoint_prefix_ref;
		switch (fault) {
		case 0:
			ShutdownRequestPending = false;
			break;
		case 1:
			test_insert += 8;
			break;
		case 2:
			test_insert -= 8;
			break;
		case 3:
			test_fence = false;
			break;
		case 4:
			test_prebump = true;
			break;
		case 5:
			MyAuxProcType = NotAnAuxProcess;
			break;
		case 6:
			ref.claim.database_incarnation++;
			break;
		case 7:
			ref.claim.claim_sha256[0] ^= 1;
			break;
		case 8:
			ref.timeline++;
			break;
		case 9:
			test_self_incarnation++;
			break;
		case 10:
			test_cf_release_confirmed = false;
			break;
		case 11:
			test_walr_end_result = CLUSTER_WALR_RELEASE_UNCONFIRMED;
			break;
		case 12:
			test_checkpoint_prefix.sequence++;
			test_checkpoint_prefix.record_start = test_checkpoint_end;
			test_checkpoint_prefix.exclusive_end += 80;
			v2_checkpoint_prefix_write();
			break;
		}
		memset(&out, 0xa5, sizeof(out));
		memset(&token, 0xa5, sizeof(token));
		UT_ASSERT(cluster_control_root_v2_shutdown_observe(&ref, &out, &token) != 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
		UT_ASSERT(v2_zero(&token, sizeof(token)));
		v2_assert_primary_unchanged(before);
	}
}

UT_TEST(test_v2_stop_observation_accepts_exact_page_and_segment_end)
{
	for (int segment = 0; segment < 2; ++segment) {
		uint8 before[66048];
		ClusterControlRootIdentity self;
		ControlFileData candidate;
		ClusterControlRootSnapshot out;
		ClusterControlRootFileToken token;
		XLogRecPtr boundary;
		uint64 unit = segment ? wal_segment_size : XLOG_BLCKSZ;
		char path[MAXPGPATH];
		v2_shutdown_checkpoint_fixture(before, &self, &candidate);
		boundary = (candidate.checkPoint / unit + 1) * unit;
		candidate.checkPoint = boundary - MAXALIGN(SizeOfXLogRecord + 2 + sizeof(CheckPoint));
		candidate.checkPointCopy.redo = candidate.checkPoint;
		INIT_CRC32C(candidate.crc);
		COMP_CRC32C(candidate.crc, &candidate, offsetof(ControlFileData, crc));
		FIN_CRC32C(candidate.crc);
		v2_checkpoint_wal_record(&self, &candidate, 0);
		UT_ASSERT_EQ(test_checkpoint_end, boundary);
		UT_ASSERT_EQ(v2_shutdown_checkpoint_publish(&self, &candidate, &out, &token), 0);
		if (ut_current_failed)
			return;
		path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
		read_all_or_abort(path, before, sizeof(before));
		ShutdownRequestPending = true;
		UT_ASSERT_EQ(
			cluster_control_root_v2_shutdown_observe(&test_checkpoint_prefix_ref, &out, &token), 0);
		UT_ASSERT_EQ(out.validated_tail_lsn_exclusive, boundary);
		UT_ASSERT_EQ(out.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN);
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
		v2_assert_primary_unchanged(before);
	}
}

UT_TEST(test_v2_checkpoint_preserves_historical_parameter_requirements)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	ControlRootImage root;
	ControlFileData candidate, view;

	v2_checkpoint_fixture(before, &self, &candidate);
	for (int lower = 0; lower < 2; ++lower) {
		candidate.MaxConnections = lower ? 100 : 811;
		candidate.max_worker_processes = lower ? 1 : 28;
		candidate.max_wal_senders = lower ? 0 : 19;
		candidate.max_prepared_xacts = lower ? 0 : 13;
		candidate.max_locks_per_xact = lower ? 1 : 259;
		INIT_CRC32C(candidate.crc);
		COMP_CRC32C(candidate.crc, &candidate, offsetof(ControlFileData, crc));
		FIN_CRC32C(candidate.crc);
		UT_ASSERT_EQ(v2_checkpoint_publish(&self, &candidate, &out, &token), 0);
		UT_ASSERT_EQ(cluster_control_root_v2_read_thread_locked(&self, &root, &view, &token), 0);
		UT_ASSERT_EQ(view.MaxConnections, 811);
		UT_ASSERT_EQ(view.max_worker_processes, 28);
		UT_ASSERT_EQ(view.max_wal_senders, 19);
		UT_ASSERT_EQ(view.max_prepared_xacts, 13);
		UT_ASSERT_EQ(view.max_locks_per_xact, 259);
		UT_ASSERT_EQ(view.wal_level, 1);
		UT_ASSERT(!view.wal_log_hints && !view.track_commit_timestamp);
		UT_ASSERT(memcmp(root.bytes + 196, before + 196, 180) == 0);
		UT_ASSERT(memcmp(root.bytes + 1024, before + 1024, 66048 - 1024) == 0);
		v2_assert_anchor_staging_empty();
		candidate = view;
		candidate.checkPoint += 8192;
		candidate.checkPointCopy.redo += 8192;
		v2_checkpoint_wal_record(&self, &candidate, 0);
	}
}

UT_TEST(test_v2_checkpoint_accepts_actual_zero_crc)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	ControlFileData candidate, view;
	ControlRootImage root;
	v2_checkpoint_fixture(before, &self, &candidate);
	v2_checkpoint_wal_record(&self, &candidate, 7);
	UT_ASSERT_EQ(test_checkpoint_crc, 0);
	UT_ASSERT_EQ(v2_checkpoint_publish(&self, &candidate, &out, &token), 0);
	UT_ASSERT_EQ(out.checkpoint_record_crc32c, 0);
	UT_ASSERT_EQ(out.tail_last_record_crc32c, 0);
	UT_ASSERT_EQ(cluster_control_root_v2_read_thread_locked(&self, &root, &view, &token), 0);
	UT_ASSERT_EQ(view.checkPoint, candidate.checkPoint);
	UT_ASSERT_EQ(view.checkPointCopy.time, candidate.checkPointCopy.time);
}

UT_TEST(test_v2_checkpoint_requires_actual_wal_record)
{
	for (int fault = 0; fault < 10; ++fault) {
		uint8 before[66048];
		ClusterControlRootIdentity self;
		ClusterControlRootSnapshot out;
		ClusterControlRootFileToken token;
		ControlFileData candidate;
		char path[MAXPGPATH];
		v2_checkpoint_fixture(before, &self, &candidate);
		v2_checkpoint_wal_path(&self, candidate.checkPoint, candidate.checkPointCopy.ThisTimeLineID,
							   path);
		if (fault == 0)
			UT_ASSERT_EQ(unlink(path), 0);
		else if (fault <= 6)
			v2_checkpoint_wal_record(&self, &candidate, fault);
		else if (fault == 7)
			UT_ASSERT_EQ(truncate(path, 1), 0);
		else if (fault == 8) {
			test_checkpoint_prefix.record_crc ^= 1;
			v2_checkpoint_prefix_write();
		} else {
			test_checkpoint_end += 8;
			test_flush = test_checkpoint_end;
		}
		UT_ASSERT(v2_checkpoint_publish(&self, &candidate, &out, &token) != 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
		UT_ASSERT(v2_zero(&token, sizeof(token)));
		v2_assert_primary_unchanged(before);
		v2_assert_anchor_staging_empty();
		UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
		UT_ASSERT_EQ(test_cf_mode, NoLock);
	}
}

UT_TEST(test_v2_checkpoint_requires_exact_durable_prefix)
{
	for (int fault = 0; fault < 10; ++fault) {
		uint8 before[66048], bytes[CLUSTER_WAL_DURABLE_PREFIX_BYTES];
		ClusterControlRootIdentity self;
		ClusterControlRootSnapshot out;
		ClusterControlRootFileToken token;
		ControlFileData candidate;
		char moved[MAXPGPATH];
		v2_checkpoint_fixture(before, &self, &candidate);
		if (fault == 0)
			UT_ASSERT_EQ(unlink(test_checkpoint_prefix_path), 0);
		else if (fault == 1) {
			read_all_or_abort(test_checkpoint_prefix_path, bytes, sizeof(bytes));
			bytes[252] ^= 1;
			write_all_or_abort(test_checkpoint_prefix_path, bytes, sizeof(bytes));
		} else if (fault == 2) {
			test_checkpoint_prefix_ref.timeline++;
			v2_checkpoint_prefix_write();
		} else if (fault == 3) {
			test_checkpoint_prefix = (ClusterWalDurablePrefix){ 1, 0, 0, 0 };
			v2_checkpoint_prefix_write();
		} else if (fault == 4) {
			test_checkpoint_prefix.exclusive_end -= 8;
			v2_checkpoint_prefix_write();
		} else if (fault == 5)
			UT_ASSERT_EQ(truncate(test_checkpoint_prefix_path, 12), 0);
		else if (fault == 6) {
			snprintf(moved, sizeof(moved), "%s.moved", test_checkpoint_prefix_path);
			UT_ASSERT_EQ(rename(test_checkpoint_prefix_path, moved), 0);
			UT_ASSERT_EQ(symlink(moved, test_checkpoint_prefix_path), 0);
		} else {
			if (fault == 7)
				test_checkpoint_prefix.record_crc ^= 1;
			else if (fault == 8)
				test_checkpoint_prefix.record_start += 8;
			else
				test_checkpoint_prefix.exclusive_end += 128;
			v2_checkpoint_prefix_write();
		}
		UT_ASSERT(v2_checkpoint_publish(&self, &candidate, &out, &token) != 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
		v2_assert_primary_unchanged(before);
		v2_assert_anchor_staging_empty();
		UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
		UT_ASSERT_EQ(test_cf_mode, NoLock);
		if (fault == 6) {
			UT_ASSERT_EQ(unlink(test_checkpoint_prefix_path), 0);
			UT_ASSERT_EQ(rename(moved, test_checkpoint_prefix_path), 0);
		}
	}
}

static int test_prefix_race;

static void
v2_checkpoint_prefix_race(void)
{
	test_checkpoint_x_hook = NULL;
	if (test_prefix_race == 0)
		test_checkpoint_prefix.sequence--;
	else if (test_prefix_race == 1)
		test_checkpoint_prefix.record_crc ^= 1;
	else if (test_prefix_race == 2)
		test_checkpoint_prefix.sequence++;
	else if (test_prefix_race == 3) {
		test_checkpoint_prefix.sequence++;
		test_checkpoint_prefix.exclusive_end += 128;
	} else if (test_prefix_race == 4) {
		UT_ASSERT_EQ(unlink(test_checkpoint_prefix_path), 0);
		return;
	} else {
		/* A checkpoint need not consume these later records: it must only
		 * retain coverage of its own independently verified native record. */
		test_checkpoint_prefix.sequence += 3;
		test_checkpoint_prefix.record_start = test_checkpoint_prefix.exclusive_end + 16;
		test_checkpoint_prefix.exclusive_end += 128;
	}
	v2_checkpoint_prefix_write();
}

UT_TEST(test_v2_checkpoint_rechecks_durable_prefix)
{
	for (int fault = 0; fault < 7; ++fault) {
		uint8 before[66048];
		ClusterControlRootIdentity self;
		ClusterControlRootSnapshot out;
		ClusterControlRootFileToken token;
		ControlFileData candidate;
		v2_checkpoint_fixture(before, &self, &candidate);
		test_prefix_race = fault;
		test_checkpoint_x_hook = v2_checkpoint_prefix_race;
		if (fault == 6) {
			v2_checkpoint_prefix_race();
			/* An already-ahead, nonoverlapping promise is legitimate too. */
		}
		if (fault >= 5) {
			UT_ASSERT_EQ(v2_checkpoint_publish(&self, &candidate, &out, &token), 0);
			UT_ASSERT_EQ(out.validated_tail_lsn_exclusive, test_checkpoint_end);
		} else {
			UT_ASSERT(v2_checkpoint_publish(&self, &candidate, &out, &token) != 0);
			UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
			v2_assert_primary_unchanged(before);
		}
		v2_assert_anchor_staging_empty();
		UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
		UT_ASSERT_EQ(test_cf_mode, NoLock);
	}
}

UT_TEST(test_v2_checkpoint_wal_continuation)
{
	for (int segment = 0; segment < 2; ++segment) {
		uint8 before[66048];
		ClusterControlRootIdentity self;
		ClusterControlRootSnapshot out;
		ClusterControlRootFileToken token;
		ControlFileData candidate;
		v2_checkpoint_fixture(before, &self, &candidate);
		candidate.checkPoint
			= segment ? (candidate.checkPoint / wal_segment_size + 2) * wal_segment_size - 48
					  : (candidate.checkPoint / XLOG_BLCKSZ + 2) * XLOG_BLCKSZ - 48;
		INIT_CRC32C(candidate.crc);
		COMP_CRC32C(candidate.crc, &candidate, offsetof(ControlFileData, crc));
		FIN_CRC32C(candidate.crc);
		v2_checkpoint_wal_record(&self, &candidate, 0);
		UT_ASSERT_EQ(v2_checkpoint_publish(&self, &candidate, &out, &token), 0);
		UT_ASSERT_EQ(out.tail_last_record_lsn, candidate.checkPoint);
		UT_ASSERT_EQ(out.validated_tail_lsn_exclusive, test_checkpoint_end);
		UT_ASSERT_EQ(out.tail_last_record_crc32c, test_checkpoint_crc);
		v2_assert_anchor_staging_empty();
	}
}

UT_TEST(test_v2_checkpoint_rejects_invalid_parameter_before_max)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	ControlFileData candidate;

	for (int fault = 0; fault < 8; ++fault) {
		v2_checkpoint_fixture(before, &self, &candidate);
		switch (fault) {
		case 0:
			candidate.MaxConnections = 0;
			break;
		case 1:
			candidate.MaxConnections = -1;
			break;
		case 2:
			candidate.max_worker_processes = -1;
			break;
		case 3:
			candidate.max_wal_senders = -1;
			break;
		case 4:
			candidate.max_prepared_xacts = -1;
			break;
		case 5:
			candidate.max_locks_per_xact = 0;
			break;
		case 6:
			candidate.wal_level = -1;
			break;
		case 7:
			candidate.wal_level = 3;
			break;
		}
		INIT_CRC32C(candidate.crc);
		COMP_CRC32C(candidate.crc, &candidate, offsetof(ControlFileData, crc));
		FIN_CRC32C(candidate.crc);
		UT_ASSERT_EQ(v2_checkpoint_publish(&self, &candidate, &out, &token),
					 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
		UT_ASSERT(v2_zero(&token, sizeof(token)));
		UT_ASSERT_EQ(test_cf_lock_calls, 0);
		v2_assert_primary_unchanged(before);
		v2_assert_anchor_staging_empty();
	}
}

UT_TEST(test_v2_checkpoint_cannot_invent_parameter_transition_proof)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	ControlFileData candidate;

	for (int fault = 0; fault < 4; ++fault) {
		v2_checkpoint_fixture(before, &self, &candidate);
		switch (fault) {
		case 0:
			candidate.wal_level = 0;
			break;
		case 1:
			candidate.wal_level = 2;
			break;
		case 2:
			candidate.wal_log_hints = true;
			break;
		case 3:
			candidate.track_commit_timestamp = true;
			break;
		}
		INIT_CRC32C(candidate.crc);
		COMP_CRC32C(candidate.crc, &candidate, offsetof(ControlFileData, crc));
		FIN_CRC32C(candidate.crc);
		UT_ASSERT_EQ(v2_checkpoint_publish(&self, &candidate, &out, &token),
					 CLUSTER_CONTROL_ROOT_RANGE_INVALID);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
		UT_ASSERT(v2_zero(&token, sizeof(token)));
		v2_assert_primary_unchanged(before);
		v2_assert_anchor_staging_empty();
	}
}

UT_TEST(test_v2_checkpoint_rejects_non_owner_facts_before_io)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	ControlFileData candidate;
	int fault;

	for (fault = 0; fault < 12; ++fault) {
		v2_checkpoint_fixture(before, &self, &candidate);
		switch (fault) {
		case 0:
			MyAuxProcType = NotAnAuxProcess;
			break;
		case 1:
			test_self_incarnation++;
			break;
		case 2:
			test_membership_incarnation++;
			break;
		case 3:
			test_member_state = CLUSTER_MEMBER_ABSENT;
			break;
		case 4:
			test_fence = false;
			break;
		case 5:
			test_serving = false;
			break;
		case 6:
			test_prebump = true;
			break;
		case 7:
			test_wal_validated = false;
			break;
		case 8:
			cluster_enabled = false;
			break;
		case 9:
			cluster_controlfile_shared_authority = false;
			break;
		case 10:
			self.origin_node_id = 127;
			break;
		case 11:
			test_checkpoint_outer_cf = true;
			break;
		}
		memset(&out, 0xab, sizeof(out));
		memset(&token, 0xcd, sizeof(token));
		UT_ASSERT(v2_checkpoint_publish(&self, &candidate, &out, &token) != 0);
		UT_ASSERT_EQ(test_cf_lock_calls, 0);
		UT_ASSERT_EQ(test_durable_rename_calls, 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
		v2_assert_primary_unchanged(before);
	}
}

UT_TEST(test_v2_checkpoint_rejects_unflushed_wrong_tli_and_bad_inputs)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	ControlFileData candidate;
	int fault;

	for (fault = 0; fault < 9; ++fault) {
		v2_checkpoint_fixture(before, &self, &candidate);
		switch (fault) {
		case 0:
			test_flush--;
			break;
		case 1:
			test_flush_tli++;
			break;
		case 2:
			candidate.state = DB_SHUTDOWNED;
			break;
		case 3:
			candidate.backupEndRequired = true;
			break;
		case 4:
			candidate.backupStartPoint = 1;
			break;
		case 5:
			candidate.minRecoveryPoint = 0;
			break;
		case 6:
			candidate.checkPointCopy.redo -= 8192;
			break;
		case 7:
			candidate.system_identifier++;
			break;
		case 8:
			candidate.checkPointCopy.ThisTimeLineID++;
			test_flush_tli++;
			break;
		}
		/* Do not let a stale CRC mask the intended semantic rejection. */
		INIT_CRC32C(candidate.crc);
		COMP_CRC32C(candidate.crc, &candidate, offsetof(ControlFileData, crc));
		FIN_CRC32C(candidate.crc);
		UT_ASSERT(v2_checkpoint_publish(&self, &candidate, &out, &token) != 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
		v2_assert_primary_unchanged(before);
		v2_assert_anchor_staging_empty();
	}
}

static void
v2_checkpoint_replace_wal_generation(void)
{
	char generation[MAXPGPATH], moved[MAXPGPATH], claim[MAXPGPATH];
	uint8 bytes[112];
	test_checkpoint_x_hook = NULL;
	snprintf(generation, sizeof(generation), "%s/thread_1/generation_99", cluster_wal_threads_dir);
	snprintf(moved, sizeof(moved), "%s/thread_1/generation_99.moved", cluster_wal_threads_dir);
	snprintf(claim, sizeof(claim), "%s/%s", generation, CLUSTER_WAL_THREAD_CLAIM_FILENAME);
	read_all_or_abort(claim, bytes, sizeof(bytes));
	if (rename(generation, moved) != 0 || mkdir(generation, 0700) != 0)
		abort();
	/* Same exact claim passes the final root read, but its WAL is not the
	 * pinned directory whose bytes were verified. */
	write_all_or_abort(claim, bytes, sizeof(bytes));
}

UT_TEST(test_v2_checkpoint_rejects_replaced_wal_directory)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	ControlFileData candidate;
	char generation[MAXPGPATH], moved[MAXPGPATH], claim[MAXPGPATH];
	v2_checkpoint_fixture(before, &self, &candidate);
	test_checkpoint_x_hook = v2_checkpoint_replace_wal_generation;
	UT_ASSERT_EQ(v2_checkpoint_publish(&self, &candidate, &out, &token),
				 CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	v2_assert_primary_unchanged(before);
	v2_assert_anchor_staging_empty();
	UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
	UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
	UT_ASSERT_EQ(test_cf_mode, NoLock);
	snprintf(generation, sizeof(generation), "%s/thread_1/generation_99", cluster_wal_threads_dir);
	snprintf(moved, sizeof(moved), "%s/thread_1/generation_99.moved", cluster_wal_threads_dir);
	snprintf(claim, sizeof(claim), "%s/%s", generation, CLUSTER_WAL_THREAD_CLAIM_FILENAME);
	UT_ASSERT_EQ(unlink(claim), 0);
	UT_ASSERT_EQ(rmdir(generation), 0);
	UT_ASSERT_EQ(rename(moved, generation), 0);
}

static char v2_wal_segment_path[MAXPGPATH];
static char v2_wal_segment_moved[MAXPGPATH];

static void
v2_checkpoint_replace_wal_segment(void)
{
	uint8 *copy = malloc(wal_segment_size);
	test_checkpoint_x_hook = NULL;
	if (copy == NULL)
		abort();
	read_all_or_abort(v2_wal_segment_path, copy, wal_segment_size);
	copy[offsetof(XLogLongPageHeaderData, xlp_sysid)] ^= 1;
	if (rename(v2_wal_segment_path, v2_wal_segment_moved) != 0)
		abort();
	write_all_or_abort(v2_wal_segment_path, copy, wal_segment_size);
	free(copy);
}

UT_TEST(test_v2_checkpoint_rejects_replaced_wal_segment)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	ControlFileData candidate;
	v2_checkpoint_fixture(before, &self, &candidate);
	v2_checkpoint_wal_path(&self, candidate.checkPoint, candidate.checkPointCopy.ThisTimeLineID,
						   v2_wal_segment_path);
	snprintf(v2_wal_segment_moved, sizeof(v2_wal_segment_moved), "%s.moved", v2_wal_segment_path);
	test_checkpoint_x_hook = v2_checkpoint_replace_wal_segment;
	UT_ASSERT_EQ(v2_checkpoint_publish(&self, &candidate, &out, &token),
				 CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	v2_assert_primary_unchanged(before);
	v2_assert_anchor_staging_empty();
	UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
	UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
	UT_ASSERT_EQ(test_cf_mode, NoLock);
	UT_ASSERT_EQ(unlink(v2_wal_segment_path), 0);
	UT_ASSERT_EQ(rename(v2_wal_segment_moved, v2_wal_segment_path), 0);
}

static uint8 v2_race_winner[66048];

static void
v2_checkpoint_root_race(void)
{
	char path[MAXPGPATH];

	test_checkpoint_x_hook = NULL;
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	read_all_or_abort(path, v2_race_winner, sizeof(v2_race_winner));
	put_u64_le(v2_race_winner + 16, 8);
	put_u64_le(v2_race_winner + 336, 99);
	v2_checksums(v2_race_winner);
	v2_write_roots(v2_race_winner);
}

static void
v2_checkpoint_epoch_race(void)
{
	test_checkpoint_x_hook = NULL;
	test_epoch++;
}

UT_TEST(test_v2_checkpoint_cas_and_epoch_races_do_not_overwrite)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	ControlFileData candidate;

	v2_checkpoint_fixture(before, &self, &candidate);
	test_checkpoint_x_hook = v2_checkpoint_root_race;
	UT_ASSERT_EQ(v2_checkpoint_publish(&self, &candidate, &out, &token),
				 CLUSTER_CONTROL_ROOT_CAS_CONFLICT);
	v2_assert_primary_unchanged(v2_race_winner);
	UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
	v2_assert_anchor_staging_empty();
	v2_checkpoint_fixture(before, &self, &candidate);
	test_checkpoint_x_hook = v2_checkpoint_epoch_race;
	UT_ASSERT_EQ(v2_checkpoint_publish(&self, &candidate, &out, &token),
				 CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	v2_assert_primary_unchanged(before);
	v2_assert_anchor_staging_empty();
}

UT_TEST(test_v2_checkpoint_boundaries_refuse_without_mutation)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	ControlFileData candidate;
	int fault;

	for (fault = 0; fault < 7; ++fault) {
		v2_checkpoint_fixture(before, &self, &candidate);
		switch (fault) {
		case 0:
			put_u64_le(before + 16, UINT64_MAX);
			break;
		case 1:
			put_u64_le(before + 512 + 16, UINT64_MAX);
			break;
		case 2: {
			ClusterRecoveryAnchorV2 anchors[2];
			char path[MAXPGPATH];

			v2_thread_fixture(before, anchors);
			test_checkpoint_mode = true;
			cluster_shared_config = true;
			anchors[0].anchor_generation = UINT64_MAX;
			put_u64_le(before + 512 + 256, UINT64_MAX);
			v2_anchor_object(before, &anchors[0], &anchors[0].identity, path);
			break;
		}
		case 3:
			put_u32_le(before + 196, 4);
			break;
		case 4:
			put_u64_le(before + 232, 0);
			break;
		case 5:
			test_walr_begin_result = CLUSTER_WAL_PIN_UNAVAILABLE;
			break;
		case 6:
			test_contract = CLUSTER_CF_CONTRACT_UNVERIFIED;
			break;
		}
		v2_checksums(before);
		v2_write_roots(before);
		if (fault < 3)
			UT_ASSERT_EQ(v2_checkpoint_publish(&self, &candidate, &out, &token),
						 CLUSTER_CONTROL_ROOT_SEQUENCE_EXHAUSTED);
		else
			UT_ASSERT(v2_checkpoint_publish(&self, &candidate, &out, &token) != 0);
		UT_ASSERT_EQ(test_durable_rename_calls, 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
		v2_assert_primary_unchanged(before);
		v2_assert_anchor_staging_empty();
	}
}

UT_TEST(test_v2_checkpoint_root_io_failure_keeps_old_selection)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	ControlFileData candidate;

	v2_checkpoint_fixture(before, &self, &candidate);
	test_fail_primary_rename = true;
	UT_ASSERT_EQ(v2_checkpoint_publish(&self, &candidate, &out, &token),
				 CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
	v2_assert_primary_unchanged(before);
	v2_assert_anchor_staging_empty();
}

UT_TEST(test_v2_checkpoint_postwrite_failure_keeps_fact_but_no_success)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	ControlFileData candidate, view;
	ControlRootImage root;
	int fault;

	for (fault = 0; fault < 3; ++fault) {
		v2_checkpoint_fixture(before, &self, &candidate);
		if (fault == 0)
			test_fence_after_primary = true;
		else if (fault == 1)
			test_release_after_primary = true;
		else
			test_walr_end_result = CLUSTER_WALR_RELEASE_UNCONFIRMED;
		UT_ASSERT(v2_checkpoint_publish(&self, &candidate, &out, &token) != 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
		UT_ASSERT_EQ(cluster_control_root_v2_read_thread_locked(&self, &root, &view, &token), 0);
		UT_ASSERT_EQ(view.checkPoint, candidate.checkPoint);
		UT_ASSERT_EQ(token.file_txn_seq, 8);
		v2_assert_anchor_staging_empty();
	}
}

UT_TEST(test_v2_checkpoint_projection_follows_root_and_cannot_roll_it_back)
{
	uint8 before[66048], actual[PG_CONTROL_FILE_SIZE], expected[PG_CONTROL_FILE_SIZE];
	ClusterControlRootIdentity self;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	ControlFileData candidate, view;
	ControlRootImage root;
	char path[MAXPGPATH];

	for (int fault = 0; fault < 2; fault++) {
		v2_checkpoint_fixture(before, &self, &candidate);
		path_for(path, sizeof(path), "global/pg_control");
		/* A damaged old projection is not an input authority. */
		memset(actual, 0xa5, sizeof(actual));
		write_all_or_abort(path, actual, sizeof(actual));
		test_projection_sync_fault = fault != 0;
		test_projection_observe = true;
		if (fault == 0)
			UT_ASSERT_EQ(v2_checkpoint_publish(&self, &candidate, &out, &token), 0);
		else {
			UT_ASSERT_EQ(v2_checkpoint_publish(&self, &candidate, &out, &token),
						 CLUSTER_CONTROL_ROOT_IO_ERROR);
			UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
		}
		UT_ASSERT_EQ(test_projection_syncs, 1);
		test_projection_sync_fault = false;
		test_projection_observe = false;
		UT_ASSERT_EQ(cluster_control_root_v2_read_thread_locked(&self, &root, &view, &token), 0);
		UT_ASSERT_EQ(token.file_txn_seq, 8);
		UT_ASSERT_EQ(view.checkPoint, candidate.checkPoint);
		read_all_or_abort(path, actual, sizeof(actual));
		if (fault == 0) {
			UT_ASSERT_EQ(cluster_cf_control_image_encode(&view, expected), 0);
			UT_ASSERT(memcmp(expected, actual, sizeof(actual)) == 0);
		} else {
			memset(expected, 0xa5, sizeof(expected));
			UT_ASSERT(memcmp(expected, actual, sizeof(actual)) == 0);
		}
		v2_assert_anchor_staging_empty();
	}
}

static void
v2_checkpoint_throw_on_x(void)
{
	test_checkpoint_x_hook = NULL;
	pg_re_throw();
}

UT_TEST(test_v2_stop_observation_requires_real_shutdown_wal)
{
	for (int fault = 0; fault < 5; ++fault) {
		uint8 before[66048];
		ClusterControlRootIdentity self;
		ControlFileData candidate;
		ClusterControlRootSnapshot out;
		ClusterControlRootFileToken token;
		char path[MAXPGPATH];

		if (fault == 0) {
			v2_checkpoint_fixture(before, &self, &candidate);
			UT_ASSERT_EQ(v2_checkpoint_publish(&self, &candidate, &out, &token), 0);
			ShutdownRequestPending = true;
			path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
			read_all_or_abort(path, before, sizeof(before));
		} else {
			v2_stop_observation_fixture(before, &self, &candidate);
			if (fault == 1 || fault == 2)
				v2_checkpoint_wal_record(&self, &candidate, fault == 1 ? 4 : 5);
			else if (fault == 3) {
				v2_checkpoint_wal_path(&self, candidate.checkPoint,
									   candidate.checkPointCopy.ThisTimeLineID, path);
				UT_ASSERT_EQ(unlink(path), 0);
			} else
				UT_ASSERT_EQ(unlink(test_checkpoint_prefix_path), 0);
		}
		if (ut_current_failed)
			return;
		UT_ASSERT(
			cluster_control_root_v2_shutdown_observe(&test_checkpoint_prefix_ref, &out, &token)
			!= 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
		v2_assert_primary_unchanged(before);
	}
}

UT_TEST(test_v2_stop_observation_rechecks_root_and_pinned_wal)
{
	for (int fault = 0; fault < 4; ++fault) {
		uint8 before[66048];
		ClusterControlRootIdentity self;
		ControlFileData candidate;
		ClusterControlRootSnapshot out;
		ClusterControlRootFileToken token;
		ClusterControlRootResult result;
		char generation[MAXPGPATH], moved[MAXPGPATH], claim[MAXPGPATH];

		v2_stop_observation_fixture(before, &self, &candidate);
		if (ut_current_failed)
			return;
		test_stop_share_call = test_cf_lock_calls + 2;
		switch (fault) {
		case 0:
			test_stop_share_hook = v2_checkpoint_root_race;
			break;
		case 1:
			test_stop_share_hook = v2_checkpoint_epoch_race;
			break;
		case 2:
			test_stop_share_hook = v2_checkpoint_replace_wal_generation;
			break;
		case 3:
			v2_checkpoint_wal_path(&self, candidate.checkPoint,
								   candidate.checkPointCopy.ThisTimeLineID, v2_wal_segment_path);
			snprintf(v2_wal_segment_moved, sizeof(v2_wal_segment_moved), "%s.moved",
					 v2_wal_segment_path);
			test_stop_share_hook = v2_checkpoint_replace_wal_segment;
			break;
		}
		result
			= cluster_control_root_v2_shutdown_observe(&test_checkpoint_prefix_ref, &out, &token);
		/* The replacement generation deliberately contains only its claim.
		 * Its missing PGWP is detected before the final pinned-path recheck. */
		UT_ASSERT_EQ(result, fault == 0	  ? CLUSTER_CONTROL_ROOT_CAS_CONFLICT
							 : fault == 2 ? CLUSTER_CONTROL_ROOT_ABSENT
										  : CLUSTER_CONTROL_ROOT_STALE_TOKEN);
		UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
		v2_assert_primary_unchanged(fault == 0 ? v2_race_winner : before);
		if (fault == 2) {
			snprintf(generation, sizeof(generation), "%s/thread_1/generation_99",
					 cluster_wal_threads_dir);
			snprintf(moved, sizeof(moved), "%s.moved", generation);
			snprintf(claim, sizeof(claim), "%s/%s", generation, CLUSTER_WAL_THREAD_CLAIM_FILENAME);
			UT_ASSERT_EQ(unlink(claim), 0);
			UT_ASSERT_EQ(rmdir(generation), 0);
			UT_ASSERT_EQ(rename(moved, generation), 0);
		} else if (fault == 3) {
			UT_ASSERT_EQ(unlink(v2_wal_segment_path), 0);
			UT_ASSERT_EQ(rename(v2_wal_segment_moved, v2_wal_segment_path), 0);
		}
	}
}

UT_TEST(test_v2_stop_observation_error_releases_file_and_walr_ownership)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ControlFileData candidate;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	volatile bool caught = false;
	int open_before = 0, open_after = 0;

	v2_stop_observation_fixture(before, &self, &candidate);
	if (ut_current_failed)
		return;
	test_stop_share_call = test_cf_lock_calls + 2;
	test_stop_share_hook = v2_checkpoint_throw_on_x;
	for (int fd = 0; fd < 256; ++fd)
		if (fcntl(fd, F_GETFD) >= 0)
			++open_before;
	PG_TRY();
	{
		(void)cluster_control_root_v2_shutdown_observe(&test_checkpoint_prefix_ref, &out, &token);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	for (int fd = 0; fd < 256; ++fd)
		if (fcntl(fd, F_GETFD) >= 0)
			++open_after;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(open_before, open_after);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
	UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
	v2_assert_primary_unchanged(before);
}

UT_TEST(test_v2_checkpoint_error_unwind_releases_owned_work)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	ControlFileData candidate;
	volatile bool caught = false;
	int open_before = 0, open_after = 0;

	v2_checkpoint_fixture(before, &self, &candidate);
	for (int fd = 0; fd < 256; fd++)
		if (fcntl(fd, F_GETFD) >= 0)
			open_before++;
	test_checkpoint_x_hook = v2_checkpoint_throw_on_x;
	PG_TRY();
	{
		(void)v2_checkpoint_publish(&self, &candidate, &out, &token);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(test_walr_begin_calls, 1);
	UT_ASSERT_EQ(test_walr_end_calls, 1);
	v2_assert_primary_unchanged(before);
	v2_assert_anchor_staging_empty();
	for (int fd = 0; fd < 256; fd++)
		if (fcntl(fd, F_GETFD) >= 0)
			open_after++;
	UT_ASSERT_EQ(open_after, open_before);
}

UT_TEST(test_v2_shutdown_checkpoint_rejects_wrong_purpose_or_evidence)
{
	for (int fault = 0; fault < 10; ++fault) {
		uint8 before[66048];
		ClusterControlRootIdentity self;
		ClusterControlRootSnapshot out;
		ClusterControlRootFileToken token;
		ControlFileData candidate;
		ClusterControlRootResult result;

		v2_shutdown_checkpoint_fixture(before, &self, &candidate);
		switch (fault) {
		case 0:
			candidate.state = DB_IN_PRODUCTION;
			break;
		case 1:
			break; /* Correct shutdown input, wrong online entry. */
		case 2:
			candidate.checkPointCopy.redo -= 8;
			break;
		case 3:
			v2_checkpoint_wal_record(&self, &candidate, 5);
			break;
		case 4:
			v2_checkpoint_wal_record(&self, &candidate, 4);
			break;
		case 5:
			UT_ASSERT_EQ(unlink(test_checkpoint_prefix_path), 0);
			break;
		case 6:
			test_prefix_race = 5;
			v2_checkpoint_prefix_race();
			break;
		case 7:
			cluster_shared_config = false;
			break;
		case 8:
			test_self_incarnation++;
			break;
		case 9:
			candidate.minRecoveryPoint = candidate.checkPoint;
			break;
		}
		INIT_CRC32C(candidate.crc);
		COMP_CRC32C(candidate.crc, &candidate, offsetof(ControlFileData, crc));
		FIN_CRC32C(candidate.crc);
		result = fault == 1 ? v2_checkpoint_publish(&self, &candidate, &out, &token)
							: v2_shutdown_checkpoint_publish(&self, &candidate, &out, &token);
		UT_ASSERT(result != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
		v2_assert_primary_unchanged(before);
		v2_assert_anchor_staging_empty();
		UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
		UT_ASSERT_EQ(test_cf_mode, NoLock);
	}
}

UT_TEST(test_v2_shutdown_checkpoint_accepts_real_zero_crc)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	ControlFileData candidate;

	v2_shutdown_checkpoint_fixture(before, &self, &candidate);
	v2_checkpoint_wal_record(&self, &candidate, 7);
	UT_ASSERT_EQ(test_checkpoint_crc, 0);
	UT_ASSERT_EQ(v2_shutdown_checkpoint_publish(&self, &candidate, &out, &token), 0);
	UT_ASSERT_EQ(out.checkpoint_record_crc32c, 0);
	UT_ASSERT_EQ(out.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN);
	UT_ASSERT_EQ(test_checkpoint_output.state, DB_IN_PRODUCTION);
}

UT_TEST(test_v2_shutdown_checkpoint_races_preserve_durable_fact_not_close)
{
	for (int fault = 0; fault < 6; ++fault) {
		uint8 before[66048];
		ClusterControlRootIdentity self;
		ClusterControlRootSnapshot out;
		ClusterControlRootFileToken token;
		ControlFileData candidate, view;
		ControlRootImage root;
		ClusterControlRootResult result;

		v2_shutdown_checkpoint_fixture(before, &self, &candidate);
		if (fault == 0)
			test_checkpoint_x_hook = v2_checkpoint_root_race;
		else if (fault == 1)
			test_checkpoint_x_hook = v2_checkpoint_epoch_race;
		else if (fault == 2 || fault == 3) {
			test_prefix_race = 5;
			if (fault == 2)
				test_checkpoint_x_hook = v2_checkpoint_prefix_race;
			else
				test_checkpoint_published_hook = v2_checkpoint_prefix_race;
		} else if (fault == 4)
			test_fail_primary_rename = true;
		else {
			test_projection_sync_fault = true;
			test_projection_observe = true;
		}
		result = v2_shutdown_checkpoint_publish(&self, &candidate, &out, &token);
		UT_ASSERT_EQ(result, fault == 0	  ? CLUSTER_CONTROL_ROOT_CAS_CONFLICT
							 : fault == 1 ? CLUSTER_CONTROL_ROOT_STALE_TOKEN
							 : fault < 4  ? CLUSTER_CONTROL_ROOT_RANGE_INVALID
										  : CLUSTER_CONTROL_ROOT_IO_ERROR);
		test_checkpoint_published_hook = NULL;
		test_projection_sync_fault = false;
		test_projection_observe = false;
		if (fault == 5)
			UT_ASSERT_EQ(test_projection_syncs, 1);
		UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
		if (fault == 0)
			v2_assert_primary_unchanged(v2_race_winner);
		else if (fault == 3 || fault == 5) {
			UT_ASSERT_EQ(cluster_control_root_v2_read_thread_locked(&self, &root, &view, &token),
						 0);
			UT_ASSERT_EQ(view.checkPoint, candidate.checkPoint);
			UT_ASSERT_EQ(view.state, DB_IN_PRODUCTION);
			UT_ASSERT_EQ(root.records[0].lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN);
			UT_ASSERT_EQ(root.header.v2.database_state, CLUSTER_CONTROL_ROOT_DATABASE_OPEN);
		} else
			v2_assert_primary_unchanged(before);
		v2_assert_anchor_staging_empty();
		UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
		UT_ASSERT_EQ(test_cf_mode, NoLock);
	}
}

/* PGRAC: native CF entry consumes the real root/claim/anchor stack, not a
 * stubbed success. The compatibility image is valid but has a different
 * checkpoint so a fallback cannot accidentally satisfy these assertions.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static void root_fixture_version3(uint8 bytes[66048]);

static void
runtime_fixture_version3(uint8 bytes[66048])
{
	root_fixture_version3(bytes);
	v2_write_roots(bytes);
}

static void
v2_runtime_fixture(uint8 bytes[66048], ClusterControlRootIdentity *self, ControlFileData *candidate)
{
	v2_checkpoint_fixture(bytes, self, candidate);
	cluster_shared_config = false;
	cluster_cf_authority_write(candidate);
	cluster_shared_config = true;
	test_cf_mode = ShareLock;
	test_checkpoint_outer_cf = true;
	MyAuxProcType = NotAnAuxProcess;
	test_epoch_reads = test_change_epoch_read = 0;
	test_throw_epoch_read = 0;
}

/* PGRAC: actual CF/root/config/claim/anchor retention reader, never a native
 * projection fallback. Author: SqlRush <sqlrush@gmail.com>
 */
static void
v2_retention_fixture(uint8 bytes[66048], ClusterControlRootIdentity *self,
					 ControlFileData *candidate)
{
	v2_runtime_fixture(bytes, self, candidate);
	test_cf_mode = NoLock;
	test_checkpoint_outer_cf = false;
	test_actual_cf = NoLock;
}

/* PGRAC: existing recovery owners use these public canonical readers, not
 * the own-writer checkpoint accessor. Author: SqlRush <sqlrush@gmail.com> */
UT_TEST(test_runtime_v3_canonical_strong_reads_recovery_required_peer)
{
	uint8 bytes[66048];
	ClusterControlRootIdentity self;
	ControlFileData candidate;
	ClusterControlRootSnapshot out;
	ClusterControlRootReadToken token;
	int latch_calls;
	v2_retention_fixture(bytes, &self, &candidate);
	runtime_fixture_version3(bytes);
	bytes[512 + 10] = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	put_u64_le(bytes + 232, 0); /* No serving claim for this failed origin. */
	v2_checksums(bytes);
	v2_write_roots(bytes);
	cluster_node_id = 1; /* Observer is not the failed origin. */
	latch_calls = test_bit22_latch_apply_calls;
	UT_ASSERT_EQ(cluster_control_root_read_canonical(
					 self.origin_thread_id, &self, CLUSTER_CONTROL_ROOT_READ_STRONG, &out, &token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(out.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED);
	UT_ASSERT_EQ(memcmp(&out.identity, &self, sizeof(self)), 0);
	UT_ASSERT_EQ(token.lifecycle, out.lifecycle);
	UT_ASSERT_EQ(token.origin_thread_id, self.origin_thread_id);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	UT_ASSERT_EQ(test_bit22_latch_apply_calls, latch_calls);
	v2_assert_primary_unchanged(bytes);
	cluster_shared_config = false;
	cluster_node_id = 0;
}

UT_TEST(test_runtime_v3_canonical_discovery_lookup_and_revalidate)
{
	uint8 bytes[66048];
	ClusterControlRootIdentity self, discovered;
	ControlFileData candidate;
	ClusterControlRootSnapshot out, lookup;
	ClusterControlRootReadToken token, looked_up;
	v2_retention_fixture(bytes, &self, &candidate);
	runtime_fixture_version3(bytes);
	UT_ASSERT_EQ(
		cluster_control_root_read_canonical_discovered(self.origin_thread_id, &out, &token), 0);
	UT_ASSERT_EQ(cluster_control_root_lookup_owner_by_node_runtime(self.origin_node_id, &discovered,
																   &lookup, &looked_up),
				 0);
	UT_ASSERT_EQ(memcmp(&discovered, &self, sizeof(self)), 0);
	UT_ASSERT_EQ(memcmp(&out, &lookup, sizeof(out)), 0);
	UT_ASSERT_EQ(memcmp(&token, &looked_up, sizeof(token)), 0);
	UT_ASSERT_EQ(cluster_control_root_revalidate(&token, &self, &lookup), 0);
	/* Another publisher changes the whole-root sequence; same thread bytes
	 * are not permission to keep consuming the previous token. */
	put_u64_le(bytes + 16, token.file_txn_seq + 1);
	v2_checksums(bytes);
	v2_write_roots(bytes);
	UT_ASSERT_EQ(cluster_control_root_revalidate(&token, &self, &lookup),
				 CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT(v2_zero(&lookup, sizeof(lookup)));
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	cluster_shared_config = false;
}

UT_TEST(test_runtime_v3_canonical_no_missing_claim_or_lockfree_fallback)
{
	uint8 bytes[66048];
	char path[MAXPGPATH];
	ClusterControlRootIdentity self;
	ControlFileData candidate;
	ClusterControlRootSnapshot out;
	ClusterControlRootReadToken token;
	v2_retention_fixture(bytes, &self, &candidate);
	runtime_fixture_version3(bytes);
	UT_ASSERT_EQ(cluster_control_root_read_canonical(self.origin_thread_id, NULL,
													 CLUSTER_CONTROL_ROOT_READ_BOOTSTRAP_VALIDATE,
													 &out, &token),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_control_root_read_canonical_dead_origin(self.origin_thread_id, &out),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
	v2_claim_path(&self, path);
	UT_ASSERT_EQ(unlink(path), 0);
	memset(&out, 0x5a, sizeof(out));
	memset(&token, 0x5a, sizeof(token));
	UT_ASSERT_NE(cluster_control_root_read_canonical(
					 self.origin_thread_id, &self, CLUSTER_CONTROL_ROOT_READ_STRONG, &out, &token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	UT_ASSERT(v2_zero(&token, sizeof(token)));
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	cluster_shared_config = false;
}

UT_TEST(test_v2_stop_phase_uses_selected_raw_anchor_without_closing)
{
	for (int stopped = 0; stopped < 2; stopped++) {
		uint8 before[66048];
		ClusterControlRootIdentity self;
		ControlFileData candidate;
		ClusterControlRootStopObservation out;
		int writes;

		if (stopped)
			v2_stop_observation_fixture(before, &self, &candidate);
		else
			v2_retention_fixture(before, &self, &candidate);
		writes = test_durable_rename_calls;
		UT_ASSERT_EQ(cluster_control_root_v2_stop_phase_read(self.origin_thread_id,
															 self.origin_owner_incarnation, &out),
					 0);
		UT_ASSERT_EQ(out.phase, stopped ? CLUSTER_CONTROL_ROOT_STOP_CHECKPOINT
										: CLUSTER_CONTROL_ROOT_STOP_ACTIVE);
		UT_ASSERT(cluster_control_root_identity_equal(&out.snapshot.identity, &self));
		UT_ASSERT_EQ(out.snapshot.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN);
		UT_ASSERT_EQ(out.members[0].phase, out.phase);
		UT_ASSERT_EQ(out.members[127].phase, CLUSTER_CONTROL_ROOT_STOP_ACTIVE);
		UT_ASSERT_EQ(out.members[127].incarnation, 226);
		UT_ASSERT_EQ(out.members[127].claim_created_at, 12472);
		UT_ASSERT_EQ(out.members[1].phase, CLUSTER_CONTROL_ROOT_STOP_UNKNOWN);
		UT_ASSERT(!v2_zero(&out.token, sizeof(out.token)));
		UT_ASSERT_EQ(test_durable_rename_calls, writes);
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		v2_assert_primary_unchanged(before);
	}
}

/* Remove the fixture's deliberately retained recovery obligation, not a
 * production safety check. Re-encode an independent immutable anchor. */
static ClusterRecoveryAnchorV2
v2_stop_clean_anchor(uint8 bytes[66048], const ClusterControlRootIdentity *self)
{
	ControlRootImage root;
	ControlFileData view;
	ClusterControlRootFileToken token;
	ClusterRecoveryAnchorV2 anchor;
	ClusterRecoveryAnchorRefV2 ref = { 0 };
	uint8 encoded[512];
	char hex[65], path[MAXPGPATH];

	UT_ASSERT_EQ(cluster_control_root_v2_read_thread_locked(self, &root, &view, &token), 0);
	ref.identity = *self;
	ref.database_incarnation = root.header.v2.database_incarnation;
	ref.max_config_generation = root.header.v2.config_generation;
	ref.anchor_generation = root.refs[self->origin_node_id].anchor_generation;
	memcpy(ref.anchor_sha256, root.refs[self->origin_node_id].anchor_sha256, 32);
	memcpy(ref.claim_sha256, root.refs[self->origin_node_id].claim_sha256, 32);
	for (int i = 0; i < 32; ++i)
		snprintf(hex + i * 2, 3, "%02x", ref.anchor_sha256[i]);
	snprintf(path, sizeof(path),
			 "%s/global/anchor_images/thread_%u/generation_" UINT64_FORMAT "/anchor_" UINT64_FORMAT
			 "-%s.bin",
			 test_root, self->origin_thread_id, self->origin_owner_incarnation,
			 ref.anchor_generation, hex);
	read_all_or_abort(path, encoded, sizeof(encoded));
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_decode(encoded, sizeof(encoded), &ref, &anchor), 0);
	anchor.min_recovery_point = InvalidXLogRecPtr;
	anchor.min_recovery_tli = 0;
	v2_anchor_object(bytes, &anchor, self, path);
	v2_write_roots(bytes);
	return anchor;
}

static void
v2_close_fixture(uint8 before[66048], ClusterPhase1FullStopPlan *plan)
{
	ClusterControlRootIdentity self;
	ClusterControlRootFileToken token;
	ControlRootImage root;
	ControlFileData candidate, view;
	v2_stop_observation_fixture(before, &self, &candidate);
	v2_stop_clean_anchor(before, &self);
	UT_ASSERT_EQ(cluster_control_root_v2_read_thread_locked(&self, &root, &view, &token), 0);
	root.header.v2.configured[1] = root.header.v2.serving[1] = 0;
	root.present[127] = false;
	memset(&root.records[127], 0, sizeof(root.records[127]));
	memset(&root.refs[127], 0, sizeof(root.refs[127]));
	root.publisher_incarnation[127] = root.publisher_node[127] = 0;
	UT_ASSERT_EQ(cluster_control_root_v2_encode(&root), 0);
	memcpy(before, root.bytes, sizeof(root.bytes));
	v2_install_config(before, false);
	v2_write_roots(before);
	memset(plan, 0, sizeof(*plan));
	plan->valid = plan->pre2_root_observed = true;
	plan->epoch = test_epoch;
	plan->member_incarnations[0] = self.origin_owner_incarnation;
	plan->own_wal_started_at = self.thread_claim_created_at;
	plan->pre2_member_phase[0] = CLUSTER_CONTROL_ROOT_STOP_CHECKPOINT;
	test_close_owned = true;
}

static ClusterWalDurablePrefixRef
v2_close_add_peer(uint8 before[66048], int node, ClusterPhase1FullStopPlan *plan)
{
	ClusterWalDurablePrefixRef saved = test_checkpoint_prefix_ref, peer;
	ClusterRecoveryAnchorV2 anchor = v2_stop_clean_anchor(before, &saved.claim.identity);
	ControlRootImage root;
	ControlFileData candidate = { 0 };
	char saved_path[MAXPGPATH], path[MAXPGPATH];

	strlcpy(saved_path, test_checkpoint_prefix_path, sizeof(saved_path));
	UT_ASSERT_EQ(cluster_control_root_v2_decode(before, 66048, v2_storage, TEST_SYSID, &root), 0);
	root.header.v2.configured[0] |= UINT64_C(1) << node;
	root.header.v2.serving[0] |= UINT64_C(1) << node;
	root.present[node] = true;
	root.records[node] = root.records[0];
	root.refs[node] = root.refs[0];
	root.refs[node].history_generation = 0;
	memset(root.refs[node].history_sha256, 0, 32);
	root.records[node].identity.origin_node_id = node;
	root.records[node].identity.origin_thread_id = node + 1;
	root.records[node].identity.origin_owner_incarnation += node;
	root.records[node].identity.thread_claim_created_at += node;
	root.publisher_node[node] = node;
	root.publisher_incarnation[node] = root.records[node].identity.origin_owner_incarnation;
	UT_ASSERT_EQ(cluster_control_root_v2_encode(&root), 0);
	memcpy(before, root.bytes, 66048);
	v2_claim_object(before, node, &root);
	anchor.identity = root.records[node].identity;
	memcpy(anchor.claim_sha256, root.refs[node].claim_sha256, 32);
	v2_anchor_object(before, &anchor, &anchor.identity, path);
	v2_install_config(before, false);
	v2_write_roots(before);
	peer = saved;
	peer.claim.identity = anchor.identity;
	memcpy(peer.claim.claim_sha256, anchor.claim_sha256, 32);
	test_checkpoint_prefix_ref = peer;
	snprintf(path, sizeof(path), "%s/thread_%u/generation_" UINT64_FORMAT "/durable_prefix",
			 cluster_wal_threads_dir, peer.claim.identity.origin_thread_id,
			 peer.claim.identity.origin_owner_incarnation);
	UT_ASSERT(mkdir(path, 0700) == 0 || errno == EEXIST);
	snprintf(test_checkpoint_prefix_path, sizeof(test_checkpoint_prefix_path), "%s/current", path);
	candidate.state = DB_SHUTDOWNED;
	candidate.checkPoint = anchor.checkpoint;
	candidate.checkPointCopy = anchor.checkpoint_copy;
	v2_checkpoint_wal_record(&peer.claim.identity, &candidate, 0);
	UT_ASSERT_EQ(cluster_control_root_v2_decode(before, 66048, v2_storage, TEST_SYSID, &root), 0);
	root.records[node].checkpoint_record_crc32c = test_checkpoint_crc;
	root.records[node].tail_last_record_crc32c = test_checkpoint_crc;
	root.records[node].validated_tail_lsn_exclusive = test_checkpoint_end;
	UT_ASSERT_EQ(cluster_control_root_v2_encode(&root), 0);
	memcpy(before, root.bytes, 66048);
	v2_write_roots(before);
	plan->member_incarnations[node] = peer.claim.identity.origin_owner_incarnation;
	plan->pre2_member_phase[node] = CLUSTER_CONTROL_ROOT_STOP_CHECKPOINT;
	test_checkpoint_prefix_ref = saved;
	strlcpy(test_checkpoint_prefix_path, saved_path, sizeof(test_checkpoint_prefix_path));
	return peer;
}

static void
v3_reserve_fixture(uint8 before[66048], ControlRootImage *root)
{
	ClusterPhase1FullStopPlan plan;
	char path[MAXPGPATH];
	const char *dirs[] = { "global/wal_startup", "global/wal_startup/thread_1",
						   "global/wal_startup/thread_1/.staging", "global/wal_startup/thread_4",
						   "global/wal_startup/thread_4/.staging" };
	enableFsync = true;
	test_reserve_mode = false;
	v2_close_fixture(before, &plan);
	(void)v2_close_add_peer(before, 3, &plan);
	UT_ASSERT_EQ(cluster_control_root_v2_decode(before, 66048, v2_storage, TEST_SYSID, root), 0);
	root->header.format_version = 3;
	root->header.v2.database_state = CLUSTER_CONTROL_ROOT_DATABASE_CLOSED;
	root->records[0].lifecycle = root->records[3].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	UT_ASSERT_EQ(cluster_control_root_v3_encode(root), 0);
	memcpy(before, root->bytes, 66048);
	v2_write_roots(before);
	memset(&test_reserve_formation, 0, sizeof(test_reserve_formation));
	test_reserve_formation.local_epoch = test_epoch;
	test_reserve_formation.self_join_admitted = 1;
	for (int node = 0; node <= 3; node += 3) {
		test_reserve_formation.membership.membership_state[node] = CLUSTER_MEMBER_MEMBER;
		test_reserve_formation.membership.last_admitted_incarnation[node]
			= root->records[node].identity.origin_owner_incarnation + 10;
	}
	UT_ASSERT_EQ(cluster_control_root_v3_clean_exit_cut(before, 66048, v2_storage, TEST_SYSID,
														&test_reserve_formation, &test_reserve_cut),
				 0);
	test_reserve_mode = test_reserve_provider = test_reserve_quorum = true;
	test_reserve_fault = 0;
	test_history_fail_sync = test_history_sync_count = 0;
	test_reserve_evidence = CLUSTER_STARTUP_EXIT_READY;
	test_self_incarnation = test_membership_incarnation = test_reserve_cut.observer[0];
	MyBackendType = B_STARTUP;
	ShutdownRequestPending = false;
	test_cf_mode = NoLock;
	for (unsigned i = 0; i < lengthof(dirs); ++i) {
		path_for(path, sizeof(path), dirs[i]);
		UT_ASSERT(mkdir(path, 0700) == 0 || errno == EEXIST);
	}
}

UT_TEST(test_v3_reserve_clean_publishes_all_sparse_targets_without_serving)
{
	uint8 before[66048], actual[66048];
	ControlRootImage old, root;
	ClusterControlRootFileToken token;
	char path[MAXPGPATH];
	v3_reserve_fixture(before, &old);
	if (ut_current_failed)
		return;
	UT_ASSERT_EQ(cluster_control_root_v3_reserve_clean(&test_reserve_cut, &token), 0);
	if (ut_current_failed)
		return;
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	read_all_or_abort(path, actual, sizeof(actual));
	UT_ASSERT_EQ(
		cluster_control_root_v3_decode(actual, sizeof(actual), v2_storage, TEST_SYSID, &root), 0);
	UT_ASSERT_EQ(root.header.file_txn_seq, old.header.file_txn_seq + 1);
	UT_ASSERT_EQ(root.header.v2.database_state, CLUSTER_CONTROL_ROOT_DATABASE_MOUNTED);
	UT_ASSERT_EQ(root.header.v2.serving[0], 0);
	UT_ASSERT_EQ(root.header.v2.serving[1], 0);
	UT_ASSERT_EQ(root.header.v2.configured[0], 9);
	UT_ASSERT_EQ(token.file_txn_seq, root.header.file_txn_seq);
	test_cf_mode = ShareLock;
	test_actual_cf = ShareLock;
	for (int node = 0; node <= 3; node += 3) {
		ClusterWalStartupImage startup;
		UT_ASSERT(memcmp(&root.records[node], &old.records[node], sizeof(root.records[node])) == 0);
		UT_ASSERT(memcmp(&root.refs[node], &old.refs[node], sizeof(root.refs[node])) == 0);
		UT_ASSERT_EQ(cluster_wal_startup_read_locked(&root, node, &startup), 0);
		UT_ASSERT_EQ(startup.phase, CLUSTER_WAL_STARTUP_RESERVED);
		UT_ASSERT_EQ(startup.predecessor_file_sequence, old.header.file_txn_seq);
		UT_ASSERT(memcmp(startup.predecessor_file_sha256, test_reserve_cut.key.root_sha256, 32)
				  == 0);
		UT_ASSERT_EQ(startup.claim.identity.origin_owner_incarnation,
					 test_reserve_cut.observer[node]);
		UT_ASSERT_EQ(startup.claim.identity.root_lineage_seq,
					 old.records[node].identity.root_lineage_seq + 1);
		UT_ASSERT_EQ(startup.first_segment_lsn % wal_segment_size, 0);
		UT_ASSERT(startup.first_segment_lsn >= startup.sealed_input_end);
		UT_ASSERT(v2_zero(&startup.successor, sizeof(startup.successor)));
		UT_ASSERT(v2_zero(&startup.prefix, sizeof(startup.prefix)));
	}
	test_cf_mode = NoLock;
	test_actual_cf = NoLock;
	test_reserve_mode = false;
}

static ClusterWalStartupImage
v3_target_fixture(uint8 before[66048], ControlRootImage *root, unsigned node)
{
	static uint64 boot = 1000;
	ClusterControlRootFileToken token;
	ClusterWalStartupImage op = { 0 };
	char path[MAXPGPATH];
	cluster_node_id = 0;
	test_startup_sync_hook = NULL;
	v3_reserve_fixture(before, root);
	/* Each fault case is a different boot, not permission to overwrite the
	 * prior case's deliberately retained selected namespace. */
	boot += 10;
	for (unsigned target = 0; target <= 3; target += 3)
		test_reserve_formation.membership.last_admitted_incarnation[target] += boot;
	UT_ASSERT_EQ(cluster_control_root_v3_clean_exit_cut(before, 66048, v2_storage, TEST_SYSID,
														&test_reserve_formation, &test_reserve_cut),
				 0);
	test_self_incarnation = test_membership_incarnation = test_reserve_cut.observer[0];
	UT_ASSERT_EQ(cluster_control_root_v3_reserve_clean(&test_reserve_cut, &token), 0);
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	read_all_or_abort(path, before, 66048);
	UT_ASSERT_EQ(cluster_control_root_v3_decode(before, 66048, v2_storage, TEST_SYSID, root), 0);
	test_actual_cf = test_cf_mode = ShareLock;
	UT_ASSERT_EQ(cluster_wal_startup_read_locked(root, node, &op), 0);
	test_actual_cf = test_cf_mode = NoLock;
	cluster_node_id = node;
	test_self_incarnation = test_membership_incarnation = test_reserve_cut.observer[node];
	return op;
}

static void
v3_target_path(char path[MAXPGPATH], const ClusterWalStartupImage *op, const char *suffix)
{
	snprintf(path, MAXPGPATH, "%s/thread_%u/generation_" UINT64_FORMAT "%s",
			 cluster_wal_threads_dir, op->claim.identity.origin_thread_id,
			 op->claim.identity.origin_owner_incarnation, suffix);
}

UT_TEST(test_v3_target_creates_only_exact_empty_successor)
{
	for (unsigned node = 0; node <= 3; node += 3) {
		uint8 before[66048], encoded[112];
		ControlRootImage root;
		ClusterWalStartupImage op = v3_target_fixture(before, &root, node), observed;
		ClusterWalThreadClaimV2 claim;
		ClusterWalDurablePrefixRef ref = { 0 };
		ClusterWalDurablePrefix prefix;
		if (ut_current_failed)
			return;
		UT_ASSERT_EQ(cluster_control_root_v3_startup_prepare_target(&op.claim.identity,
																	op.operation_uuid, &observed),
					 0);
		if (ut_current_failed)
			return;
		UT_ASSERT(memcmp(&op, &observed, sizeof(op)) == 0);
		UT_ASSERT_EQ(cluster_wal_claim_v2_encode(&op.claim, encoded), 0);
		ref.claim.identity = op.claim.identity;
		ref.claim.database_incarnation = op.database_incarnation;
		ref.claim.max_config_generation = op.config_generation;
		ref.timeline = op.timeline;
		sha256_bytes(encoded, sizeof(encoded), ref.claim.claim_sha256);
		UT_ASSERT_EQ(cluster_wal_claim_v2_read(cluster_wal_threads_dir, &ref.claim, &claim), 0);
		UT_ASSERT_EQ(cluster_wal_durable_prefix_read(cluster_wal_threads_dir, &ref, &prefix), 0);
		UT_ASSERT_EQ(prefix.sequence, 1);
		UT_ASSERT_EQ(prefix.exclusive_end, 0);
		UT_ASSERT_EQ(prefix.record_start, 0);
		UT_ASSERT_EQ(prefix.record_crc, 0);
		UT_ASSERT_EQ(cluster_control_root_v3_startup_prepare_target(&op.claim.identity,
																	op.operation_uuid, &observed),
					 0);
		v2_assert_primary_unchanged(before);
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		cluster_node_id = 0;
		test_reserve_mode = false;
	}
}

static ClusterWalDurablePrefixRef
v3_driver_restart(const ControlRootImage *root, unsigned node)
{
	ClusterWalDurablePrefixRef ref = { 0 };
	ref.claim.identity = root->records[node].identity;
	ref.claim.database_incarnation = root->header.v2.database_incarnation;
	ref.claim.max_config_generation = root->header.v2.config_generation;
	memcpy(ref.claim.claim_sha256, root->refs[node].claim_sha256, 32);
	ref.timeline = root->records[node].checkpoint_tli;
	test_restart_ref = ref;
	test_restart_ref_valid = true;
	cluster_node_id = node;
	test_self_incarnation = test_membership_incarnation = test_reserve_cut.observer[node];
	return ref;
}

UT_TEST(test_v3_native_driver_prepares_sparse_targets_then_returns_initializing)
{
	uint8 before[66048];
	ControlRootImage root;
	ClusterWalStartupImage op = v3_target_fixture(before, &root, 0), observed;
	ClusterWalDurablePrefixRef restart = v3_driver_restart(&root, 0);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &observed),
				 CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT(v2_zero(&observed, sizeof(observed)));
	v2_assert_primary_unchanged(before);
	/* The peer can prepare its namespace but cannot publish INITIALIZING. */
	restart = v3_driver_restart(&root, 3);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &observed),
				 CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	v2_assert_primary_unchanged(before);
	restart = v3_driver_restart(&root, 0);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &observed),
				 CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT(v2_zero(&observed, sizeof(observed)));
	UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &observed), 0);
	UT_ASSERT_EQ(observed.phase, CLUSTER_WAL_STARTUP_INITIALIZING);
	UT_ASSERT(memcmp(observed.operation_uuid, op.operation_uuid, 16) == 0);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	restart = v3_driver_restart(&root, 3);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &observed), 0);
	UT_ASSERT_EQ(observed.claim.identity.origin_node_id, 3);
	UT_ASSERT_EQ(observed.phase, CLUSTER_WAL_STARTUP_INITIALIZING);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	cluster_node_id = 0;
	test_reserve_mode = false;
}

UT_TEST(test_v3_native_driver_rejects_foreign_input_and_owner_without_mutation)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		uint8 before[66048];
		ControlRootImage root;
		ClusterWalStartupImage op = v3_target_fixture(before, &root, 0), observed;
		ClusterWalDurablePrefixRef restart = v3_driver_restart(&root, 0);
		(void)op;
		if (fault == 0)
			restart.claim.claim_sha256[0] ^= 1;
		if (fault == 1)
			test_reserve_provider = false;
		if (fault == 2)
			MyBackendType = B_BACKEND;
		if (fault == 3)
			ShutdownRequestPending = true;
		if (fault == 4)
			test_restart_ref_valid = false;
		UT_ASSERT(cluster_control_root_v3_startup_advance_clean(&restart, &observed) != 0);
		UT_ASSERT(v2_zero(&observed, sizeof(observed)));
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		v2_assert_primary_unchanged(before);
		test_reserve_mode = false;
	}
}

UT_TEST(test_v3_native_driver_reserves_closed_cohort_only_on_coordinator)
{
	uint8 before[66048], after[66048];
	ControlRootImage root, selected;
	ClusterWalStartupImage observed;
	ClusterWalDurablePrefixRef restart;
	char path[MAXPGPATH];
	v3_reserve_fixture(before, &root);
	restart = v3_driver_restart(&root, 3);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &observed),
				 CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	v2_assert_primary_unchanged(before);
	restart = v3_driver_restart(&root, 0);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &observed),
				 CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT(v2_zero(&observed, sizeof(observed)));
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	read_all_or_abort(path, after, sizeof(after));
	UT_ASSERT_EQ(
		cluster_control_root_v3_decode(after, sizeof(after), v2_storage, TEST_SYSID, &selected), 0);
	UT_ASSERT_EQ(selected.header.file_txn_seq, root.header.file_txn_seq + 1);
	UT_ASSERT_EQ(selected.header.v2.database_state, CLUSTER_CONTROL_ROOT_DATABASE_MOUNTED);
	UT_ASSERT_EQ(selected.header.v2.configured[0], 9);
	UT_ASSERT_EQ(selected.header.v2.serving[0] | selected.header.v2.serving[1], 0);
	UT_ASSERT(selected.startup[0].generation != 0 && selected.startup[3].generation != 0);
	test_reserve_mode = false;
}

UT_TEST(test_v3_native_driver_missing_selected_or_partial_namespace_is_not_wait)
{
	for (unsigned fault = 0; fault < 6; ++fault) {
		uint8 before[66048];
		ControlRootImage root;
		ClusterWalStartupImage peer = v3_target_fixture(before, &root, 3), observed;
		ClusterWalDurablePrefixRef restart;
		ClusterControlRootResult result;
		char path[MAXPGPATH], moved[MAXPGPATH], digest[65];
		UT_ASSERT_EQ(cluster_control_root_v3_startup_prepare_target(&peer.claim.identity,
																	peer.operation_uuid, &observed),
					 0);
		switch (fault) {
		case 0:
			v3_target_path(path, &peer, "/" CLUSTER_WAL_THREAD_CLAIM_FILENAME);
			break;
		case 1:
			v3_target_path(path, &peer, "/durable_prefix/current");
			break;
		case 2:
			v3_target_path(path, &peer, "/archive_status");
			break;
		case 3:
			snprintf(path, sizeof(path),
					 "%s/global/anchor_images/thread_4/generation_" UINT64_FORMAT, test_root,
					 peer.claim.identity.origin_owner_incarnation);
			break;
		case 4:
			snprintf(path, sizeof(path), "%s/thread_4", cluster_wal_threads_dir);
			break;
		default:
			for (unsigned i = 0; i < 32; ++i)
				snprintf(digest + i * 2, 3, "%02x", root.startup[3].sha256[i]);
			snprintf(path, sizeof(path),
					 "%s/global/wal_startup/thread_4/startup_" UINT64_FORMAT "-%s.bin", test_root,
					 root.startup[3].generation, digest);
			break;
		}
		snprintf(moved, sizeof(moved), "%s.moved", path);
		UT_ASSERT_EQ(rename(path, moved), 0);
		restart = v3_driver_restart(&root, 0);
		result = cluster_control_root_v3_startup_advance_clean(&restart, &observed);
		UT_ASSERT_EQ(rename(moved, path), 0);
		UT_ASSERT(result != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(result != CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		UT_ASSERT(v2_zero(&observed, sizeof(observed)));
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		v2_assert_primary_unchanged(before);
		test_reserve_mode = false;
	}
}

UT_TEST(test_v3_native_driver_uncertain_observation_release_does_not_advance)
{
	uint8 before[66048];
	ControlRootImage root;
	ClusterWalStartupImage op = v3_target_fixture(before, &root, 0), observed;
	ClusterWalDurablePrefixRef restart = v3_driver_restart(&root, 0);
	ClusterControlRootResult result;
	char path[MAXPGPATH];
	struct stat st;
	test_cf_release_confirmed = false;
	result = cluster_control_root_v3_startup_advance_clean(&restart, &observed);
	test_cf_release_confirmed = true;
	UT_ASSERT(result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
			  && result != CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT(v2_zero(&observed, sizeof(observed)));
	v2_assert_primary_unchanged(before);
	v3_target_path(path, &op, "");
	UT_ASSERT(lstat(path, &st) < 0 && errno == ENOENT);
	/* Unconfirmed release is not reported as NoLock or retried by the driver. */
	UT_ASSERT_EQ(test_actual_cf, ShareLock);
	test_actual_cf = test_cf_mode = NoLock;
	test_reserve_mode = false;
}

UT_TEST(test_v3_begin_requires_all_declared_empty_targets_before_native_mutation)
{
	uint8 before[66048], after[66048];
	ControlRootImage root, selected;
	ControlFileData common;
	ClusterWalStartupImage self = v3_target_fixture(before, &root, 0), peer, observed;
	ClusterControlRootFileToken token, advanced;
	char path[MAXPGPATH];
	UT_ASSERT_EQ(cluster_control_root_v3_startup_prepare_target(&self.claim.identity,
																self.operation_uuid, &observed),
				 0);
	test_actual_cf = test_cf_mode = ShareLock;
	UT_ASSERT_EQ(cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID, &selected,
															 &common, &token),
				 0);
	UT_ASSERT_EQ(cluster_wal_startup_read_locked(&root, 3, &peer), 0);
	test_actual_cf = test_cf_mode = NoLock;
	UT_ASSERT(cluster_control_root_v3_startup_read_writer(&self.claim.identity, self.operation_uuid,
														  &observed)
			  != 0);
	UT_ASSERT(v2_zero(&observed, sizeof(observed)));
	UT_ASSERT(cluster_control_root_v3_startup_begin_clean(&self.claim.identity, self.operation_uuid,
														  &token, &advanced)
			  != 0);
	UT_ASSERT(v2_zero(&advanced, sizeof(advanced)));
	v2_assert_primary_unchanged(before);
	cluster_node_id = 3;
	test_self_incarnation = test_membership_incarnation
		= peer.claim.identity.origin_owner_incarnation;
	UT_ASSERT_EQ(cluster_control_root_v3_startup_prepare_target(&peer.claim.identity,
																peer.operation_uuid, &observed),
				 0);
	/* Physical preparation is target-owned; publishing the roster is not. */
	UT_ASSERT(cluster_control_root_v3_startup_begin_clean(&peer.claim.identity, peer.operation_uuid,
														  &token, &advanced)
			  != 0);
	cluster_node_id = 0;
	test_self_incarnation = test_membership_incarnation
		= self.claim.identity.origin_owner_incarnation;
	test_startup_prefix_syncs = 0;
	UT_ASSERT_EQ(cluster_control_root_v3_startup_begin_clean(
					 &self.claim.identity, self.operation_uuid, &token, &advanced),
				 0);
	UT_ASSERT_EQ(test_startup_prefix_syncs, 2);
	if (ut_current_failed)
		return;
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	read_all_or_abort(path, after, sizeof(after));
	UT_ASSERT_EQ(
		cluster_control_root_v3_decode(after, sizeof(after), v2_storage, TEST_SYSID, &selected), 0);
	UT_ASSERT_EQ(selected.header.file_txn_seq, root.header.file_txn_seq + 1);
	UT_ASSERT_EQ(selected.header.v2.database_state, CLUSTER_CONTROL_ROOT_DATABASE_MOUNTED);
	UT_ASSERT_EQ(selected.header.v2.serving[0], 0);
	UT_ASSERT_EQ(advanced.file_txn_seq, selected.header.file_txn_seq);
	test_actual_cf = test_cf_mode = ShareLock;
	for (unsigned node = 0; node <= 3; node += 3) {
		UT_ASSERT_EQ(cluster_wal_startup_read_locked(&selected, node, &observed), 0);
		UT_ASSERT_EQ(observed.phase, CLUSTER_WAL_STARTUP_INITIALIZING);
		UT_ASSERT_EQ(observed.claim.claim_generation, root.startup[node].generation);
		UT_ASSERT(memcmp(&root.records[node], &selected.records[node], sizeof(root.records[node]))
				  == 0);
		UT_ASSERT(memcmp(&root.refs[node], &selected.refs[node], sizeof(root.refs[node])) == 0);
	}
	test_actual_cf = test_cf_mode = NoLock;
	UT_ASSERT(cluster_control_root_v3_startup_prepare_target(&self.claim.identity,
															 self.operation_uuid, &observed)
			  != 0);
	UT_ASSERT(v2_zero(&observed, sizeof(observed)));
	/* Native writer admission reads the actual selected INITIALIZING object,
	 * not the predecessor's CLOSED current record or a caller-made token. */
	UT_ASSERT_EQ(cluster_control_root_v3_startup_read_writer(&self.claim.identity,
															 self.operation_uuid, &observed),
				 0);
	UT_ASSERT_EQ(observed.phase, CLUSTER_WAL_STARTUP_INITIALIZING);
	UT_ASSERT_EQ(observed.first_segment_lsn, self.first_segment_lsn);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	for (int fault = 0; fault < 9; ++fault) {
		uint8 uuid[16];
		memcpy(uuid, self.operation_uuid, sizeof(uuid));
		switch (fault) {
		case 0:
			MyBackendType = B_BACKEND;
			break;
		case 1:
			test_reserve_provider = false;
			break;
		case 2:
			++test_self_incarnation;
			break;
		case 3:
			uuid[0] ^= 1;
			break;
		case 4:
			ShutdownRequestPending = true;
			break;
		case 5:
			CritSectionCount = 1;
			break;
		case 6:
			test_actual_cf = test_cf_mode = ShareLock;
			break;
		case 7:
			++test_reserve_formation.membership.last_admitted_incarnation[3];
			break;
		case 8:
			test_reserve_formation.prebump_sync_active = 1;
			break;
		}
		UT_ASSERT(cluster_control_root_v3_startup_read_writer(&self.claim.identity, uuid, &observed)
				  != 0);
		UT_ASSERT(v2_zero(&observed, sizeof(observed)));
		UT_ASSERT_EQ(test_actual_cf, fault == 6 ? ShareLock : NoLock);
		MyBackendType = B_STARTUP;
		test_reserve_provider = true;
		test_self_incarnation = self.claim.identity.origin_owner_incarnation;
		ShutdownRequestPending = false;
		CritSectionCount = 0;
		test_actual_cf = test_cf_mode = NoLock;
		test_reserve_formation.membership.last_admitted_incarnation[3]
			= peer.claim.identity.origin_owner_incarnation;
		test_reserve_formation.prebump_sync_active = 0;
	}
	/* Even an INITIALIZING root does not authorize adopting bytes left by a
	 * different native execution. This API is for the still-EMPTY cut only. */
	v3_target_path(path, &self, "/unexpected-wal");
	write_all_or_abort(path, "old", 3);
	UT_ASSERT(cluster_control_root_v3_startup_read_writer(&self.claim.identity, self.operation_uuid,
														  &observed)
			  != 0);
	UT_ASSERT(v2_zero(&observed, sizeof(observed)));
	v2_assert_primary_unchanged(after);
	test_reserve_mode = false;
}

UT_TEST(test_v3_target_rejects_wrong_owner_before_creation)
{
	for (int fault = 0; fault < 12; ++fault) {
		uint8 before[66048];
		ControlRootImage root;
		ClusterWalStartupImage op = v3_target_fixture(before, &root, 0), observed;
		char path[MAXPGPATH];
		struct stat st;
		if (fault == 0)
			MyBackendType = B_LMON;
		if (fault == 1)
			test_reserve_provider = false;
		if (fault == 2)
			op.operation_uuid[0] ^= 1;
		if (fault == 3)
			test_self_incarnation++;
		if (fault == 4)
			ShutdownRequestPending = true;
		if (fault == 5)
			enableFsync = false;
		if (fault == 6)
			test_reserve_formation.membership.last_admitted_incarnation[3]++;
		if (fault == 7)
			cluster_node_id = 3;
		if (fault == 8)
			test_reserve_fault = 2;
		if (fault == 9)
			test_reserve_formation.excluded_bitmap[0] |= 8;
		if (fault == 10)
			test_reserve_formation.pending_join_bitmap[0] |= 8;
		if (fault == 11)
			test_reserve_formation.self_join_failed = 1;
		memset(&observed, 0xff, sizeof(observed));
		UT_ASSERT(cluster_control_root_v3_startup_prepare_target(&op.claim.identity,
																 op.operation_uuid, &observed)
				  != 0);
		UT_ASSERT(v2_zero(&observed, sizeof(observed)));
		v3_target_path(path, &op, "");
		UT_ASSERT(lstat(path, &st) != 0 && errno == ENOENT);
		v2_assert_primary_unchanged(before);
		cluster_node_id = 0;
		test_reserve_mode = false;
	}
}

UT_TEST(test_v3_target_never_adopts_foreign_or_nonempty_namespace)
{
	for (int fault = 0; fault < 7; ++fault) {
		uint8 before[66048];
		ControlRootImage root;
		ClusterWalStartupImage op = v3_target_fixture(before, &root, 0), observed;
		char path[MAXPGPATH], saved[MAXPGPATH];
		struct stat st;
		int fd;
		if (fault == 0) {
			v3_target_path(path, &op, "");
			UT_ASSERT_EQ(mkdir(path, 0700), 0);
		} else {
			UT_ASSERT_EQ(cluster_control_root_v3_startup_prepare_target(
							 &op.claim.identity, op.operation_uuid, &observed),
						 0);
			if (fault == 1)
				v3_target_path(path, &op, "/000000010000000000000007");
			if (fault == 2)
				v3_target_path(path, &op, "/pgrac_thread.claim");
			if (fault == 3)
				v3_target_path(path, &op, "/durable_prefix/current");
			if (fault == 4)
				v3_target_path(path, &op, "/archive_status/000000010000000000000007.ready");
			if (fault <= 4) {
				fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);
				UT_ASSERT(fd >= 0 && write(fd, "FOREIGN", 7) == 7 && close(fd) == 0);
			}
			if (fault == 5 || fault == 6) {
				v3_target_path(path, &op, fault == 5 ? "" : "/durable_prefix/current");
				snprintf(saved, sizeof(saved), "%s.kept", path);
				UT_ASSERT_EQ(rename(path, saved), 0);
				UT_ASSERT_EQ(symlink(saved, path), 0);
			}
		}
		UT_ASSERT(cluster_control_root_v3_startup_prepare_target(&op.claim.identity,
																 op.operation_uuid, &observed)
				  != 0);
		UT_ASSERT(v2_zero(&observed, sizeof(observed)));
		if (fault == 0) {
			v3_target_path(path, &op, "/pgrac_thread.claim");
			UT_ASSERT(lstat(path, &st) != 0 && errno == ENOENT);
		} else if (fault <= 4) {
			char bytes[7];
			read_all_or_abort(path, bytes, sizeof(bytes));
			UT_ASSERT(memcmp(bytes, "FOREIGN", 7) == 0);
		} else {
			UT_ASSERT(lstat(path, &st) == 0 && S_ISLNK(st.st_mode));
		}
		v2_assert_primary_unchanged(before);
		test_reserve_mode = false;
	}
}

UT_TEST(test_v3_target_sync_failures_never_grant_initialization)
{
	for (unsigned cut = 1; cut <= 13; ++cut) {
		uint8 before[66048];
		ControlRootImage root;
		ClusterWalStartupImage op = v3_target_fixture(before, &root, 0), observed;
		test_history_sync_count = 0;
		test_history_fail_sync = cut;
		UT_ASSERT(cluster_control_root_v3_startup_prepare_target(&op.claim.identity,
																 op.operation_uuid, &observed)
				  != 0);
		UT_ASSERT(v2_zero(&observed, sizeof(observed)));
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		v2_assert_primary_unchanged(before);
		test_history_fail_sync = 0;
		UT_ASSERT_EQ(cluster_control_root_v3_startup_prepare_target(&op.claim.identity,
																	op.operation_uuid, &observed),
					 0);
		UT_ASSERT_EQ(observed.phase, CLUSTER_WAL_STARTUP_RESERVED);
		v2_assert_primary_unchanged(before);
		test_reserve_mode = false;
	}
}

static void
v3_target_late_change(void)
{
	test_startup_sync_hook = NULL;
	if (test_reserve_fault == 3)
		test_reserve_formation.membership.last_admitted_incarnation[3]++;
	else
		test_reserve_provider = false;
}

UT_TEST(test_v3_target_reobserves_formation_and_provider_after_files)
{
	for (int fault = 3; fault <= 4; ++fault) {
		uint8 before[66048];
		ControlRootImage root;
		ClusterWalStartupImage op = v3_target_fixture(before, &root, 0), observed;
		test_reserve_fault = fault;
		test_startup_sync_hook = v3_target_late_change;
		UT_ASSERT(cluster_control_root_v3_startup_prepare_target(&op.claim.identity,
																 op.operation_uuid, &observed)
				  != 0);
		UT_ASSERT(v2_zero(&observed, sizeof(observed)));
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		v2_assert_primary_unchanged(before);
		test_reserve_mode = false;
	}
}

static ClusterWalStartupImage
v3_begin_fixture(uint8 before[66048], ControlRootImage *root, ClusterControlRootFileToken *token)
{
	ClusterWalStartupImage op = v3_target_fixture(before, root, 0), peer, observed;
	ControlFileData common;
	UT_ASSERT_EQ(cluster_control_root_v3_startup_prepare_target(&op.claim.identity,
																op.operation_uuid, &observed),
				 0);
	test_actual_cf = test_cf_mode = ShareLock;
	UT_ASSERT_EQ(cluster_wal_startup_read_locked(root, 3, &peer), 0);
	UT_ASSERT_EQ(
		cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID, root, &common, token),
		0);
	test_actual_cf = test_cf_mode = NoLock;
	cluster_node_id = 3;
	test_self_incarnation = test_membership_incarnation
		= peer.claim.identity.origin_owner_incarnation;
	UT_ASSERT_EQ(cluster_control_root_v3_startup_prepare_target(&peer.claim.identity,
																peer.operation_uuid, &observed),
				 0);
	cluster_node_id = 0;
	test_self_incarnation = test_membership_incarnation
		= op.claim.identity.origin_owner_incarnation;
	return op;
}

/* The mirror is an explicit process boundary; root, PGWG, claims, prefix and
 * symlink operations below are actual production objects and physical files. */
static ClusterWalStartupImage
v3_route_fixture(uint8 before[66048], ControlRootImage *root, char pgdata[MAXPGPATH])
{
	ClusterControlRootFileToken token, advanced;
	ControlFileData common;
	ClusterWalStartupImage op = v3_begin_fixture(before, root, &token);
	char pgwal[MAXPGPATH], input[MAXPGPATH];
	UT_ASSERT_EQ(cluster_control_root_v3_startup_begin_clean(&op.claim.identity, op.operation_uuid,
															 &token, &advanced),
				 0);
	test_actual_cf = test_cf_mode = ShareLock;
	UT_ASSERT_EQ(
		cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID, root, &common, &token),
		0);
	UT_ASSERT_EQ(cluster_wal_startup_read_locked(root, 0, &op), 0);
	test_actual_cf = test_cf_mode = NoLock;
	memcpy(before, root->bytes, 66048);
	memset(&test_restart_ref, 0, sizeof(test_restart_ref));
	test_restart_ref.claim.identity = op.predecessor.snapshot.identity;
	test_restart_ref.claim.database_incarnation = op.database_incarnation;
	test_restart_ref.claim.max_config_generation = op.config_generation;
	memcpy(test_restart_ref.claim.claim_sha256, op.predecessor.refs.claim_sha256, 32);
	test_restart_ref.timeline = op.predecessor.snapshot.checkpoint_tli;
	test_restart_ref_valid = true;
	strlcpy(pgdata, "/tmp/pgrac-startup-route.XXXXXX", MAXPGPATH);
	UT_ASSERT(mkdtemp(pgdata) != NULL);
	DataDir = pgdata;
	snprintf(pgwal, sizeof(pgwal), "%s/pg_wal", pgdata);
	snprintf(input, sizeof(input), "%s/thread_%u/generation_" UINT64_FORMAT,
			 cluster_wal_threads_dir, test_restart_ref.claim.identity.origin_thread_id,
			 test_restart_ref.claim.identity.origin_owner_incarnation);
	UT_ASSERT_EQ(symlink(input, pgwal), 0);
	return op;
}

UT_TEST(test_v3_startup_route_switches_only_exact_empty_generation)
{
	uint8 before[66048];
	ControlRootImage root;
	char pgdata[MAXPGPATH], pgwal[MAXPGPATH], target[MAXPGPATH];
	ClusterWalStartupImage op = v3_route_fixture(before, &root, pgdata), out;
	struct stat oldlink, newlink, routed, expected;
	if (ut_current_failed)
		return;
	snprintf(pgwal, sizeof(pgwal), "%s/pg_wal", pgdata);
	v3_target_path(target, &op, "");
	UT_ASSERT_EQ(lstat(pgwal, &oldlink), 0);
	UT_ASSERT_EQ(
		cluster_control_root_v3_startup_route_writer(&op.claim.identity, op.operation_uuid, &out),
		0);
	if (!ut_current_failed) {
		UT_ASSERT(memcmp(&out, &op, sizeof(op)) == 0);
		UT_ASSERT_EQ(stat(pgwal, &routed), 0);
		UT_ASSERT_EQ(stat(target, &expected), 0);
		UT_ASSERT(routed.st_dev == expected.st_dev && routed.st_ino == expected.st_ino);
		UT_ASSERT_EQ(lstat(pgwal, &newlink), 0);
		UT_ASSERT(S_ISLNK(newlink.st_mode));
		UT_ASSERT(oldlink.st_ino != newlink.st_ino);
		UT_ASSERT_EQ(cluster_control_root_v3_startup_route_writer(&op.claim.identity,
																  op.operation_uuid, &out),
					 0);
		UT_ASSERT_EQ(lstat(pgwal, &routed), 0);
		UT_ASSERT(routed.st_dev == newlink.st_dev && routed.st_ino == newlink.st_ino);
	}
	v2_assert_primary_unchanged(before);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	UT_ASSERT_EQ(unlink(pgwal), 0);
	UT_ASSERT_EQ(rmdir(pgdata), 0);
	DataDir = test_root;
	test_restart_ref_valid = test_reserve_mode = false;
}

UT_TEST(test_v3_startup_route_requires_exact_owner_and_restart_input)
{
	for (unsigned fault = 0; fault < 14; ++fault) {
		uint8 before[66048], uuid[16];
		ControlRootImage root;
		char pgdata[MAXPGPATH], pgwal[MAXPGPATH];
		ClusterWalStartupImage op = v3_route_fixture(before, &root, pgdata), out;
		struct stat original, observed;
		if (ut_current_failed)
			return;
		memcpy(uuid, op.operation_uuid, 16);
		snprintf(pgwal, sizeof(pgwal), "%s/pg_wal", pgdata);
		UT_ASSERT_EQ(lstat(pgwal, &original), 0);
		switch (fault) {
		case 0:
			MyBackendType = B_BACKEND;
			break;
		case 1:
			test_restart_ref_valid = false;
			break;
		case 2:
			test_restart_ref.claim.identity.origin_owner_incarnation++;
			break;
		case 3:
			test_restart_ref.claim.claim_sha256[0] ^= 1;
			break;
		case 4:
			test_restart_ref.claim.max_config_generation++;
			break;
		case 5:
			test_restart_ref.timeline++;
			break;
		case 6:
			uuid[0] ^= 1;
			break;
		case 7:
			ShutdownRequestPending = true;
			break;
		case 8:
			enableFsync = false;
			break;
		case 9:
			test_reserve_provider = false;
			break;
		case 10:
			test_self_incarnation++;
			break;
		case 11:
			test_reserve_formation.membership.last_admitted_incarnation[3]++;
			break;
		case 12:
			test_actual_cf = test_cf_mode = ShareLock;
			break;
		case 13:
			CritSectionCount = 1;
			break;
		}
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT(cluster_control_root_v3_startup_route_writer(&op.claim.identity, uuid, &out)
				  != 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
		UT_ASSERT_EQ(lstat(pgwal, &observed), 0);
		UT_ASSERT(original.st_dev == observed.st_dev && original.st_ino == observed.st_ino);
		v2_assert_primary_unchanged(before);
		UT_ASSERT_EQ(test_actual_cf, fault == 12 ? ShareLock : NoLock);
		UT_ASSERT_EQ(unlink(pgwal), 0);
		UT_ASSERT_EQ(rmdir(pgdata), 0);
		DataDir = test_root;
		test_actual_cf = test_cf_mode = NoLock;
		CritSectionCount = 0;
		ShutdownRequestPending = false;
		test_restart_ref_valid = test_reserve_mode = false;
	}
}

UT_TEST(test_v3_startup_route_refuses_unsafe_or_nonempty_namespaces)
{
	unsigned fds_before = 0, fds_after = 0;
	for (int fd = 0; fd < 256; ++fd)
		if (fcntl(fd, F_GETFD) >= 0)
			++fds_before;
	for (unsigned fault = 0; fault < 10; ++fault) {
		uint8 before[66048];
		ControlRootImage root;
		char pgdata[MAXPGPATH], pgwal[MAXPGPATH], path[MAXPGPATH], moved[MAXPGPATH];
		ClusterWalStartupImage op = v3_route_fixture(before, &root, pgdata), out;
		if (ut_current_failed)
			return;
		snprintf(pgwal, sizeof(pgwal), "%s/pg_wal", pgdata);
		if (fault <= 2) {
			UT_ASSERT_EQ(unlink(pgwal), 0);
			if (fault == 1)
				UT_ASSERT_EQ(mkdir(pgwal, 0700), 0);
			if (fault == 2)
				UT_ASSERT_EQ(symlink(cluster_wal_threads_dir, pgwal), 0);
		} else if (fault == 3 || fault == 4) {
			v2_claim_path(fault == 3 ? &op.predecessor.snapshot.identity : &op.claim.identity,
						  path);
			write_all_or_abort(path, "bad", 3);
		} else if (fault == 5) {
			v3_target_path(path, &op, "/unexpected-wal");
			write_all_or_abort(path, "not empty", 9);
		} else if (fault == 6) {
			v3_target_path(path, &op, "/durable_prefix/current");
			write_all_or_abort(path, "bad", 3);
		} else if (fault == 7) {
			v3_target_path(path, &op, "");
			UT_ASSERT_EQ(chmod(path, 0777), 0);
		} else if (fault == 8) {
			v3_target_path(path, &op, "");
			snprintf(moved, sizeof(moved), "%s.saved", path);
			UT_ASSERT_EQ(rename(path, moved), 0);
			UT_ASSERT_EQ(symlink(moved, path), 0);
		} else {
			snprintf(path, sizeof(path),
					 "%s/thread_1/generation_" UINT64_FORMAT "/durable_prefix/current",
					 cluster_wal_threads_dir,
					 op.predecessor.snapshot.identity.origin_owner_incarnation);
			write_all_or_abort(path, "bad", 3);
		}
		UT_ASSERT(cluster_control_root_v3_startup_route_writer(&op.claim.identity,
															   op.operation_uuid, &out)
				  != 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
		v2_assert_primary_unchanged(before);
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		if (fault == 7)
			UT_ASSERT_EQ(chmod(path, 0700), 0);
		if (fault == 8) {
			UT_ASSERT_EQ(unlink(path), 0);
			UT_ASSERT_EQ(rename(moved, path), 0);
		}
		if (fault == 1)
			UT_ASSERT_EQ(rmdir(pgwal), 0);
		else if (fault != 0)
			UT_ASSERT_EQ(unlink(pgwal), 0);
		/* Also proves that a rejected operation left no owned temporary name. */
		UT_ASSERT_EQ(rmdir(pgdata), 0);
		DataDir = test_root;
		test_restart_ref_valid = test_reserve_mode = false;
	}
	for (int fd = 0; fd < 256; ++fd)
		if (fcntl(fd, F_GETFD) >= 0)
			++fds_after;
	UT_ASSERT_EQ(fds_after, fds_before);
}

UT_TEST(test_v3_startup_route_uncertain_rename_requires_directory_sync)
{
	uint8 before[66048], old_bytes[XLOG_BLCKSZ], after_bytes[XLOG_BLCKSZ];
	ControlRootImage root;
	char pgdata[MAXPGPATH], pgwal[MAXPGPATH], wal[MAXPGPATH];
	ClusterWalStartupImage op = v3_route_fixture(before, &root, pgdata), out;
	struct stat original, switched = { 0 }, retried;
	if (ut_current_failed)
		return;
	snprintf(pgwal, sizeof(pgwal), "%s/pg_wal", pgdata);
	v2_checkpoint_wal_path(&op.predecessor.snapshot.identity, op.input_record_start,
						   op.input_timeline, wal);
	read_all_or_abort(wal, old_bytes, sizeof(old_bytes));
	UT_ASSERT_EQ(lstat(pgwal, &original), 0);
	for (unsigned attempt = 0; attempt < 3; ++attempt) {
		test_history_sync_count = 0;
		test_history_fail_sync = attempt < 2 ? 1 : 0;
		UT_ASSERT_EQ(cluster_control_root_v3_startup_route_writer(&op.claim.identity,
																  op.operation_uuid, &out),
					 attempt < 2 ? CLUSTER_CONTROL_ROOT_IO_ERROR : CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		if (attempt < 2) {
			UT_ASSERT(v2_zero(&out, sizeof(out)));
			UT_ASSERT_EQ(test_history_sync_count, 1);
		}
		UT_ASSERT_EQ(lstat(pgwal, &retried), 0);
		if (attempt == 0)
			switched = retried;
		UT_ASSERT(original.st_ino != retried.st_ino);
		UT_ASSERT(switched.st_dev == retried.st_dev && switched.st_ino == retried.st_ino);
		v2_assert_primary_unchanged(before);
		UT_ASSERT_EQ(test_actual_cf, NoLock);
	}
	read_all_or_abort(wal, after_bytes, sizeof(after_bytes));
	UT_ASSERT(memcmp(old_bytes, after_bytes, sizeof(old_bytes)) == 0);
	UT_ASSERT_EQ(unlink(pgwal), 0);
	UT_ASSERT_EQ(rmdir(pgdata), 0);
	DataDir = test_root;
	test_restart_ref_valid = test_reserve_mode = false;
}

static unsigned test_route_cut;
static char test_route_cut_path[MAXPGPATH], test_route_cut_moved[MAXPGPATH];

static void
v3_route_late_change(void)
{
	test_startup_sync_hook = NULL;
	switch (test_route_cut) {
	case 0:
		test_reserve_provider = false;
		break;
	case 1:
		test_reserve_formation.membership.last_admitted_incarnation[3]++;
		break;
	case 2:
		UT_ASSERT_EQ(unlink(test_route_cut_path), 0);
		UT_ASSERT_EQ(symlink(cluster_wal_threads_dir, test_route_cut_path), 0);
		break;
	case 3:
	case 4:
		UT_ASSERT_EQ(rename(test_route_cut_path, test_route_cut_moved), 0);
		UT_ASSERT_EQ(mkdir(test_route_cut_path, 0700), 0);
		break;
	case 5:
		ShutdownRequestPending = true;
		break;
	case 6:
		test_cf_release_confirmed = false;
		break;
	}
}

UT_TEST(test_v3_startup_route_rechecks_names_and_owner_after_exchange)
{
	for (test_route_cut = 0; test_route_cut < 7; ++test_route_cut) {
		uint8 before[66048];
		ControlRootImage root;
		char pgdata[MAXPGPATH], pgwal[MAXPGPATH];
		ClusterWalStartupImage op = v3_route_fixture(before, &root, pgdata), out;
		if (ut_current_failed)
			return;
		snprintf(pgwal, sizeof(pgwal), "%s/pg_wal", pgdata);
		strlcpy(test_route_cut_path, test_route_cut == 3 ? pgdata : pgwal,
				sizeof(test_route_cut_path));
		if (test_route_cut == 4)
			v3_target_path(test_route_cut_path, &op, "");
		snprintf(test_route_cut_moved, sizeof(test_route_cut_moved), "%s.moved",
				 test_route_cut_path);
		test_startup_sync_hook = v3_route_late_change;
		UT_ASSERT(cluster_control_root_v3_startup_route_writer(&op.claim.identity,
															   op.operation_uuid, &out)
				  != 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
		UT_ASSERT(test_startup_sync_hook == NULL);
		v2_assert_primary_unchanged(before);
		if (test_route_cut == 3 || test_route_cut == 4) {
			UT_ASSERT_EQ(rmdir(test_route_cut_path), 0);
			UT_ASSERT_EQ(rename(test_route_cut_moved, test_route_cut_path), 0);
		}
		UT_ASSERT_EQ(unlink(pgwal), 0);
		UT_ASSERT_EQ(rmdir(pgdata), 0);
		DataDir = test_root;
		ShutdownRequestPending = false;
		test_cf_release_confirmed = true;
		test_restart_ref_valid = test_reserve_mode = false;
	}
}

static void
v3_route_binding(const ControlRootImage *root, const char *pgdata)
{
	PgracControlBinding binding = { 0 };
	uint8 bytes[256];
	char path[MAXPGPATH];
	binding.system_identifier = root->header.system_identifier;
	memcpy(binding.storage_uuid, root->header.storage_uuid, 16);
	memcpy(binding.authority_uuid, root->header.authority_uuid, 16);
	binding.database_incarnation = root->header.v2.database_incarnation;
	binding.node_id = 0;
	memset(binding.operation_uuid, 0x41, 16);
	memset(binding.source_cold_sha256, 0x42, 32);
	memset(binding.target_qualification_sha256, 0x43, 32);
	memcpy(binding.migration_round_sha256, root->header.migration_round_sha256, 32);
	memcpy(binding.source_wal_state_sha256, root->header.source_wal_state_sha256, 32);
	binding.migration_prepare_generation = root->header.migration_prepare_generation;
	binding.migration_transition_epoch = root->header.migration_transition_epoch;
	UT_ASSERT(pgrac_control_binding_encode(&binding, bytes, sizeof(bytes)));
	snprintf(path, sizeof(path), "%s/global", pgdata);
	UT_ASSERT_EQ(mkdir(path, 0700), 0);
	snprintf(path, sizeof(path), "%s/global/%s", pgdata, PGRAC_CONTROL_BINDING_NAME);
	write_all_or_abort(path, bytes, sizeof(bytes));
}

UT_TEST(test_bootstrap_pending_route_keeps_immutable_restart_input)
{
	uint8 before[66048];
	ControlRootImage root;
	char pgdata[MAXPGPATH], path[MAXPGPATH];
	ClusterWalStartupImage op = v3_route_fixture(before, &root, pgdata), observed;
	ClusterControlBootstrapObservation out;
	v3_route_binding(&root, pgdata);
	for (unsigned routed = 0; routed < 2; ++routed) {
		if (routed)
			UT_ASSERT_EQ(cluster_control_root_v3_startup_route_writer(&op.claim.identity,
																	  op.operation_uuid, &observed),
						 0);
		UT_ASSERT_EQ(cluster_control_bootstrap_read(pgdata, test_root, test_wal_root, 0, &out), 0);
		if (ut_current_failed)
			break;
		UT_ASSERT(out.snapshot.pending_wal_valid);
		UT_ASSERT(cluster_control_root_identity_equal(&out.snapshot.wal.claim.identity,
													  &op.predecessor.snapshot.identity));
		UT_ASSERT(cluster_control_root_identity_equal(&out.snapshot.pending_wal.claim.identity,
													  &op.claim.identity));
		UT_ASSERT_EQ(
			cluster_control_bootstrap_wal_startup_route(pgdata, test_wal_root, &out.snapshot), 0);
		if (routed)
			UT_ASSERT(cluster_control_bootstrap_wal_route(pgdata, test_wal_root, &out.snapshot.wal)
					  != 0);
		pfree(out.config_bytes);
	}
	v2_assert_primary_unchanged(before);
	snprintf(path, sizeof(path), "%s/pg_wal", pgdata);
	UT_ASSERT_EQ(unlink(path), 0);
	snprintf(path, sizeof(path), "%s/global/%s", pgdata, PGRAC_CONTROL_BINDING_NAME);
	UT_ASSERT_EQ(unlink(path), 0);
	snprintf(path, sizeof(path), "%s/global", pgdata);
	UT_ASSERT_EQ(rmdir(path), 0);
	UT_ASSERT_EQ(rmdir(pgdata), 0);
	DataDir = test_root;
	test_restart_ref_valid = test_reserve_mode = false;
}

/* Build a real independent successor stream after the production all-member
 * INITIALIZING CAS. The native checkpoint and PGWP files are not mocked. */
static void history_stage_dirs(uint32 node);

static void
v3_startup_native_record(const ClusterWalStartupImage *op, ControlFileData *candidate)
{
	uint8 claim[112];
	UT_ASSERT_EQ(cluster_wal_claim_v2_encode(&op->claim, claim), 0);
	memset(&test_checkpoint_prefix_ref, 0, sizeof(test_checkpoint_prefix_ref));
	test_checkpoint_prefix_ref.claim.identity = op->claim.identity;
	test_checkpoint_prefix_ref.claim.database_incarnation = op->database_incarnation;
	test_checkpoint_prefix_ref.claim.max_config_generation = op->config_generation;
	sha256_bytes(claim, sizeof(claim), test_checkpoint_prefix_ref.claim.claim_sha256);
	test_checkpoint_prefix_ref.timeline = op->timeline;
	v3_target_path(test_checkpoint_prefix_path, op, "/durable_prefix/current");
	candidate->state = DB_SHUTDOWNED;
	candidate->checkPoint = op->first_segment_lsn + SizeOfXLogLongPHD;
	candidate->checkPointCopy.redo = candidate->checkPoint;
	candidate->time++;
	candidate->checkPointCopy.time++;
	candidate->minRecoveryPoint = 0;
	candidate->minRecoveryPointTLI = 0;
	INIT_CRC32C(candidate->crc);
	COMP_CRC32C(candidate->crc, candidate, offsetof(ControlFileData, crc));
	FIN_CRC32C(candidate->crc);
	test_flush_tli = op->timeline;
	v2_checkpoint_wal_record(&op->claim.identity, candidate, 8);
	test_startup_operation = *op;
	test_startup_bound = true;
}

static ClusterWalStartupImage
v3_startup_checkpoint_fixture(uint8 before[66048], ControlRootImage *root,
							  ControlFileData *candidate)
{
	ClusterControlRootFileToken token, advanced;
	ClusterWalStartupImage op = v3_begin_fixture(before, root, &token);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_begin_clean(&op.claim.identity, op.operation_uuid,
															 &token, &advanced),
				 0);
	test_actual_cf = test_cf_mode = ShareLock;
	UT_ASSERT_EQ(cluster_control_root_v3_read_thread_locked(&op.predecessor.snapshot.identity, root,
															candidate, &token),
				 0);
	UT_ASSERT_EQ(cluster_wal_startup_read_locked(root, 0, &op), 0);
	test_actual_cf = test_cf_mode = NoLock;
	memcpy(before, root->bytes, 66048);
	v3_startup_native_record(&op, candidate);
	return op;
}

UT_TEST(test_v3_startup_checkpoint_publishes_only_actual_new_durability)
{
	uint8 before[66048];
	ControlRootImage root, selected;
	ControlFileData candidate, common;
	ClusterControlRootFileToken token;
	ClusterWalStartupImage op = v3_startup_checkpoint_fixture(before, &root, &candidate), observed;
	if (ut_current_failed)
		return;
	UT_ASSERT_EQ(cluster_control_root_v3_startup_checkpoint(&op.claim.identity, op.operation_uuid,
															&candidate, test_checkpoint_end,
															&observed),
				 0);
	if (ut_current_failed)
		return;
	UT_ASSERT_EQ(observed.phase, CLUSTER_WAL_STARTUP_DURABLE);
	UT_ASSERT_EQ(observed.successor.snapshot.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN);
	UT_ASSERT_EQ(observed.successor.snapshot.checkpoint_lower_lsn, candidate.checkPoint);
	UT_ASSERT_EQ(observed.prefix.exclusive_end, test_checkpoint_end);
	UT_ASSERT_EQ(observed.prefix.record_crc, test_checkpoint_crc);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	test_actual_cf = test_cf_mode = ShareLock;
	UT_ASSERT_EQ(cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID, &selected,
															 &common, &token),
				 0);
	UT_ASSERT_EQ(selected.header.file_txn_seq, root.header.file_txn_seq + 1);
	UT_ASSERT_EQ(selected.header.v2.database_state, CLUSTER_CONTROL_ROOT_DATABASE_MOUNTED);
	UT_ASSERT_EQ(selected.header.v2.serving[0], 0);
	UT_ASSERT(memcmp(root.records, selected.records, sizeof(root.records)) == 0);
	UT_ASSERT(memcmp(root.refs, selected.refs, sizeof(root.refs)) == 0);
	UT_ASSERT(memcmp(&root.startup[3], &selected.startup[3], sizeof(root.startup[3])) == 0);
	test_actual_cf = test_cf_mode = NoLock;
	/* A same-process retry reads the exact selected durable result without
	 * generating another root/anchor or granting ordinary writer access. */
	memcpy(before, selected.bytes, sizeof(before));
	UT_ASSERT_EQ(cluster_control_root_v3_startup_checkpoint(&op.claim.identity, op.operation_uuid,
															&candidate, test_checkpoint_end,
															&observed),
				 0);
	v2_assert_primary_unchanged(before);
	test_startup_bound = false;
	UT_ASSERT(cluster_control_root_v3_startup_checkpoint(&op.claim.identity, op.operation_uuid,
														 &candidate, test_checkpoint_end, &observed)
			  != 0);
	UT_ASSERT(v2_zero(&observed, sizeof(observed)));
	test_reserve_mode = false;
}

UT_TEST(test_v3_startup_checkpoint_retry_requires_selected_root_durability)
{
	uint8 before[66048], selected_bytes[66048], backup[66048];
	ControlRootImage root;
	ControlFileData candidate;
	ClusterWalStartupImage op = v3_startup_checkpoint_fixture(before, &root, &candidate), out;
	char path[MAXPGPATH];
	unsigned syncs, renames;
	if (ut_current_failed)
		return;
	test_fail_after_primary_rename = true;
	UT_ASSERT_EQ(cluster_control_root_v3_startup_checkpoint(&op.claim.identity, op.operation_uuid,
															&candidate, test_checkpoint_end, &out),
				 CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	read_all_or_abort(path, selected_bytes, sizeof(selected_bytes));
	UT_ASSERT(memcmp(before, selected_bytes, sizeof(before)) != 0);
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_BAK_REL_PATH);
	read_all_or_abort(path, backup, sizeof(backup));
	UT_ASSERT(memcmp(before, backup, sizeof(before)) == 0);
	test_fail_after_primary_rename = false;
	test_fail_global_sync = true;
	syncs = test_global_syncs;
	UT_ASSERT_EQ(cluster_control_root_v3_startup_checkpoint(&op.claim.identity, op.operation_uuid,
															&candidate, test_checkpoint_end, &out),
				 CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	UT_ASSERT(test_global_syncs > syncs);
	v2_assert_primary_unchanged(selected_bytes);
	test_fail_global_sync = false;
	syncs = test_global_syncs;
	renames = test_durable_rename_calls;
	UT_ASSERT_EQ(cluster_control_root_v3_startup_checkpoint(&op.claim.identity, op.operation_uuid,
															&candidate, test_checkpoint_end, &out),
				 0);
	UT_ASSERT_EQ(out.phase, CLUSTER_WAL_STARTUP_DURABLE);
	UT_ASSERT(test_global_syncs > syncs);
	UT_ASSERT_EQ(test_durable_rename_calls, renames + 1);
	v2_assert_primary_unchanged(selected_bytes);
	read_all_or_abort(path, before, sizeof(before));
	UT_ASSERT(memcmp(before, backup, sizeof(before)) == 0);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
	test_reserve_mode = false;
}

UT_TEST(test_v3_startup_install_retains_predecessor_without_serving)
{
	uint8 before[66048];
	ControlRootImage root, selected;
	ControlFileData candidate, common;
	ClusterControlRootFileToken token;
	ClusterWalStartupImage op = v3_startup_checkpoint_fixture(before, &root, &candidate), durable;
	ClusterWalDurablePrefixRef writer;
	ClusterWalHistoryImage history;
	if (ut_current_failed)
		return;
	history_stage_dirs(0);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_checkpoint(&op.claim.identity, op.operation_uuid,
															&candidate, test_checkpoint_end,
															&durable),
				 0);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_install_writer(&durable, &writer), 0);
	if (ut_current_failed)
		return;
	UT_ASSERT(cluster_control_root_identity_equal(&writer.claim.identity, &op.claim.identity));
	UT_ASSERT_EQ(writer.timeline, op.timeline);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	test_actual_cf = test_cf_mode = ShareLock;
	UT_ASSERT_EQ(cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID, &selected,
															 &common, &token),
				 0);
	UT_ASSERT_EQ(selected.header.file_txn_seq, root.header.file_txn_seq + 2);
	UT_ASSERT_EQ(selected.header.v2.database_state, CLUSTER_CONTROL_ROOT_DATABASE_MOUNTED);
	UT_ASSERT_EQ(selected.header.v2.serving[0], 0);
	UT_ASSERT(v2_zero(&selected.startup[0], sizeof(selected.startup[0])));
	UT_ASSERT(memcmp(&selected.startup[3], &root.startup[3], sizeof(root.startup[3])) == 0);
	UT_ASSERT(memcmp(&selected.records[3], &root.records[3], sizeof(root.records[3])) == 0);
	UT_ASSERT(memcmp(&selected.records[0], &durable.successor.snapshot, sizeof(selected.records[0]))
			  == 0);
	UT_ASSERT_EQ(cluster_wal_history_read_locked(&selected, 0, &history), 0);
	UT_ASSERT_EQ(history.count, 1);
	UT_ASSERT(memcmp(&history.records[0].snapshot, &durable.predecessor.snapshot,
					 sizeof(durable.predecessor.snapshot))
			  == 0);
	UT_ASSERT_EQ(history.records[0].refs.history_generation, 0);
	memcpy(before, selected.bytes, sizeof(before));
	test_actual_cf = test_cf_mode = NoLock;
	UT_ASSERT_EQ(cluster_control_root_v3_startup_install_writer(&durable, &writer), 0);
	v2_assert_primary_unchanged(before);
	test_startup_bound = false;
	UT_ASSERT(cluster_control_root_v3_startup_install_writer(&durable, &writer) != 0);
	UT_ASSERT(v2_zero(&writer, sizeof(writer)));
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
	test_reserve_mode = false;
}

static ClusterWalStartupImage
v3_install_fixture(uint8 before[66048], ControlRootImage *root)
{
	ControlFileData candidate, common;
	ClusterControlRootFileToken token;
	ClusterWalStartupImage op = v3_startup_checkpoint_fixture(before, root, &candidate), durable;
	history_stage_dirs(0);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_checkpoint(&op.claim.identity, op.operation_uuid,
															&candidate, test_checkpoint_end,
															&durable),
				 0);
	test_actual_cf = test_cf_mode = ShareLock;
	UT_ASSERT_EQ(
		cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID, root, &common, &token),
		0);
	memcpy(before, root->bytes, 66048);
	test_actual_cf = test_cf_mode = NoLock;
	return durable;
}

UT_TEST(test_v3_startup_install_refuses_wrong_owner_and_intent)
{
	for (unsigned installed = 0; installed < 2; ++installed)
		for (unsigned fault = 0; fault < 17; ++fault) {
			uint8 before[66048];
			char path[MAXPGPATH];
			ControlRootImage root;
			ClusterWalStartupImage op = v3_install_fixture(before, &root), expected = op;
			ClusterWalDurablePrefixRef writer;
			if (ut_current_failed)
				return;
			if (installed) {
				UT_ASSERT_EQ(cluster_control_root_v3_startup_install_writer(&op, &writer), 0);
				path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
				read_all_or_abort(path, before, sizeof(before));
			}
			switch (fault) {
			case 0:
				test_startup_bound = false;
				break;
			case 1:
				MyBackendType = B_BACKEND;
				break;
			case 2:
				ShutdownRequestPending = true;
				break;
			case 3:
				CritSectionCount = 1;
				break;
			case 4:
				test_reserve_provider = false;
				break;
			case 5:
				++test_self_incarnation;
				break;
			case 6:
				++test_epoch;
				break;
			case 7:
				++test_insert;
				break;
			case 8:
				expected.operation_uuid[0] ^= 1;
				break;
			case 9:
				++expected.successor.snapshot.checkpoint_lower_lsn;
				break;
			case 10:
				expected.predecessor.refs.claim_sha256[0] ^= 1;
				break;
			case 11:
				expected.phase = CLUSTER_WAL_STARTUP_INITIALIZING;
				break;
			case 12:
				enableFsync = false;
				break;
			case 13:
				++test_reserve_formation.membership.last_admitted_incarnation[3];
				break;
			case 14:
				test_actual_cf = test_cf_mode = ShareLock;
				test_checkpoint_outer_cf = true;
				break;
			case 15:
				++expected.successor.publisher_incarnation;
				break;
			case 16:
				expected.prefix.record_crc ^= 1;
				break;
			}
			memset(&writer, 0xa5, sizeof(writer));
			UT_ASSERT(cluster_control_root_v3_startup_install_writer(&expected, &writer) != 0);
			UT_ASSERT(v2_zero(&writer, sizeof(writer)));
			v2_assert_primary_unchanged(before);
			UT_ASSERT_EQ(test_actual_cf, fault == 14 ? ShareLock : NoLock);
			UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
			CritSectionCount = 0;
			test_checkpoint_outer_cf = false;
			test_actual_cf = test_cf_mode = NoLock;
			test_reserve_mode = false;
		}
}

UT_TEST(test_v3_startup_install_requires_actual_old_and_new_files)
{
	for (unsigned installed = 0; installed < 2; ++installed)
		for (unsigned fault = 0; fault < 7; ++fault) {
			uint8 before[66048];
			char path[MAXPGPATH], hex[65];
			ControlRootImage root;
			ClusterWalStartupImage op = v3_install_fixture(before, &root);
			ClusterWalDurablePrefixRef writer;
			if (ut_current_failed)
				return;
			if (installed) {
				UT_ASSERT_EQ(cluster_control_root_v3_startup_install_writer(&op, &writer), 0);
				path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
				read_all_or_abort(path, before, sizeof(before));
			}
			if (fault == 0)
				v2_claim_path(&op.predecessor.snapshot.identity, path);
			if (fault == 1)
				v2_claim_path(&op.claim.identity, path);
			if (fault == 2)
				v3_target_path(path, &op, "/durable_prefix/current");
			if (fault == 3)
				v2_checkpoint_wal_path(&op.claim.identity, op.prefix.record_start, op.timeline,
									   path);
			if (fault >= 4 && fault <= 5) {
				const ClusterWalHistoryRecord *r = fault == 4 ? &op.predecessor : &op.successor;
				for (unsigned i = 0; i < 32; ++i)
					snprintf(hex + 2 * i, 3, "%02x", r->refs.anchor_sha256[i]);
				snprintf(path, sizeof(path),
						 "%s/global/anchor_images/thread_1/generation_" UINT64_FORMAT
						 "/anchor_" UINT64_FORMAT "-%s.bin",
						 test_root, r->snapshot.identity.origin_owner_incarnation,
						 r->refs.anchor_generation, hex);
			}
			if (fault == 6)
				snprintf(path, sizeof(path),
						 "%s/thread_1/generation_" UINT64_FORMAT "/durable_prefix/current",
						 cluster_wal_threads_dir,
						 op.predecessor.snapshot.identity.origin_owner_incarnation);
			UT_ASSERT_EQ(unlink(path), 0);
			UT_ASSERT(cluster_control_root_v3_startup_install_writer(&op, &writer) != 0);
			UT_ASSERT(v2_zero(&writer, sizeof(writer)));
			v2_assert_primary_unchanged(before);
			UT_ASSERT_EQ(test_actual_cf, NoLock);
			UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
			test_reserve_mode = false;
		}
}

UT_TEST(test_v3_startup_install_uncertain_rename_is_not_success)
{
	uint8 before[66048], installed[66048];
	char path[MAXPGPATH];
	ControlRootImage root;
	ClusterWalStartupImage op = v3_install_fixture(before, &root);
	ClusterWalDurablePrefixRef writer;
	if (ut_current_failed)
		return;
	test_fail_after_primary_rename = true;
	UT_ASSERT_EQ(cluster_control_root_v3_startup_install_writer(&op, &writer),
				 CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT(v2_zero(&writer, sizeof(writer)));
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	read_all_or_abort(path, installed, sizeof(installed));
	UT_ASSERT_EQ(
		cluster_control_root_v3_decode(installed, sizeof(installed), v2_storage, TEST_SYSID, &root),
		0);
	UT_ASSERT_EQ(root.startup[0].generation, 0);
	UT_ASSERT_EQ(root.records[0].identity.origin_owner_incarnation,
				 op.claim.identity.origin_owner_incarnation);
	UT_ASSERT_EQ(root.header.v2.serving[0], 0);
	test_fail_after_primary_rename = false;
	test_fail_global_sync = true;
	UT_ASSERT_EQ(cluster_control_root_v3_startup_install_writer(&op, &writer),
				 CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT(v2_zero(&writer, sizeof(writer)));
	v2_assert_primary_unchanged(installed);
	test_fail_global_sync = false;
	UT_ASSERT_EQ(cluster_control_root_v3_startup_install_writer(&op, &writer), 0);
	v2_assert_primary_unchanged(installed);
	/* The previous DURABLE root is still the backup, not the retry image. */
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_BAK_REL_PATH);
	read_all_or_abort(path, installed, sizeof(installed));
	UT_ASSERT(memcmp(before, installed, sizeof(before)) == 0);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
	test_reserve_mode = false;
}

static void v3_startup_checkpoint_late_change(void);

UT_TEST(test_v3_startup_install_all_sync_and_late_cuts_retain_obligations)
{
	unsigned sync_count;
	uint8 before[66048];
	ControlRootImage root;
	ClusterWalStartupImage op = v3_install_fixture(before, &root);
	ClusterWalDurablePrefixRef writer;
	if (ut_current_failed)
		return;
	test_history_sync_count = 0;
	test_history_fail_sync = UINT_MAX;
	UT_ASSERT_EQ(cluster_control_root_v3_startup_install_writer(&op, &writer), 0);
	sync_count = test_history_sync_count;
	UT_ASSERT(sync_count > 0 && sync_count < 32);
	test_history_fail_sync = 0;
	for (unsigned cut = 1; cut <= sync_count + 5; ++cut) {
		uint8 after[66048];
		char path[MAXPGPATH];
		op = v3_install_fixture(before, &root);
		if (ut_current_failed)
			return;
		test_history_sync_count = 0;
		if (cut <= sync_count)
			test_history_fail_sync = cut;
		else if (cut == sync_count + 1)
			test_checkpoint_x_hook = v3_startup_checkpoint_late_change;
		else if (cut == sync_count + 2)
			test_checkpoint_published_hook = v3_startup_checkpoint_late_change;
		else if (cut == sync_count + 3)
			test_fence_after_primary = true;
		else if (cut == sync_count + 4)
			test_release_after_primary = true;
		else
			test_walr_end_result = CLUSTER_WALR_RELEASE_UNCONFIRMED;
		UT_ASSERT(cluster_control_root_v3_startup_install_writer(&op, &writer) != 0);
		UT_ASSERT(v2_zero(&writer, sizeof(writer)));
		path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
		read_all_or_abort(path, after, sizeof(after));
		UT_ASSERT_EQ(
			cluster_control_root_v3_decode(after, sizeof(after), v2_storage, TEST_SYSID, &root), 0);
		UT_ASSERT_EQ(root.header.v2.serving[0], 0);
		UT_ASSERT_EQ(root.header.v2.database_state, CLUSTER_CONTROL_ROOT_DATABASE_MOUNTED);
		if (root.startup[0].generation != 0)
			v2_assert_primary_unchanged(before);
		else {
			ClusterWalHistoryImage history;
			test_actual_cf = test_cf_mode = ShareLock;
			UT_ASSERT_EQ(cluster_wal_history_read_locked(&root, 0, &history), 0);
			UT_ASSERT_EQ(history.count, 1);
			UT_ASSERT_EQ(history.records[0].snapshot.identity.origin_owner_incarnation,
						 op.predecessor.snapshot.identity.origin_owner_incarnation);
		}
		test_history_fail_sync = 0;
		test_cf_release_confirmed = true;
		test_walr_end_result = CLUSTER_WALR_RELEASE_CONFIRMED;
		test_actual_cf = test_cf_mode = NoLock;
		test_reserve_mode = false;
	}
}

/* Independently construct a prior flat history with real claim, native WAL,
 * prefix and anchor files, then bind that input into the DURABLE fixture. */
static void
v3_install_add_history(ControlRootImage *root, ClusterWalStartupImage *op,
					   ClusterWalHistoryImage *history, unsigned count, bool with_terminal)
{
	static uint64 old_generation = 500000;
	ClusterRecoveryAnchorRefV2 ref = { 0 };
	ClusterRecoveryAnchorV2 anchor;
	ClusterWalHistoryStage stage;
	ClusterWalStartupStage operation_stage;
	ClusterWalDurablePrefixRef saved_ref = test_checkpoint_prefix_ref;
	ClusterWalDurablePrefix saved_prefix = test_checkpoint_prefix;
	XLogRecPtr saved_insert = test_insert, saved_flush = test_flush,
			   saved_end = test_checkpoint_end;
	uint32 saved_crc = test_checkpoint_crc;
	char path[MAXPGPATH], saved_path[MAXPGPATH], hex[65];
	uint8 encoded[512], bytes[66048], uuid[16] = { 0x61 };
	ControlRootImage old;
	strlcpy(saved_path, test_checkpoint_prefix_path, sizeof(saved_path));
	ref.identity = op->predecessor.snapshot.identity;
	ref.database_incarnation = op->database_incarnation;
	ref.max_config_generation = op->config_generation;
	ref.anchor_generation = op->predecessor.refs.anchor_generation;
	memcpy(ref.anchor_sha256, op->predecessor.refs.anchor_sha256, 32);
	memcpy(ref.claim_sha256, op->predecessor.refs.claim_sha256, 32);
	for (unsigned i = 0; i < 32; ++i)
		snprintf(hex + 2 * i, 3, "%02x", ref.anchor_sha256[i]);
	snprintf(path, sizeof(path),
			 "%s/global/anchor_images/thread_1/generation_" UINT64_FORMAT "/anchor_" UINT64_FORMAT
			 "-%s.bin",
			 test_root, ref.identity.origin_owner_incarnation, ref.anchor_generation, hex);
	read_all_or_abort(path, encoded, sizeof(encoded));
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_decode(encoded, sizeof(encoded), &ref, &anchor), 0);
	memset(history, 0, sizeof(*history));
	history->count = count;
	for (unsigned i = 0; i < count; ++i) {
		ClusterRecoveryAnchorV2 historical = anchor;
		ControlFileData native = { 0 };
		old = *root;
		memset(old.startup, 0, sizeof(old.startup));
		old.header.format_version = 2;
		old.records[0].identity.origin_owner_incarnation = ++old_generation;
		old.records[0].identity.thread_claim_created_at += old_generation;
		old.publisher_incarnation[0] = old_generation;
		UT_ASSERT_EQ(cluster_control_root_v2_encode(&old), 0);
		memcpy(bytes, old.bytes, sizeof(bytes));
		v2_claim_object(bytes, 0, &old);
		historical.identity = old.records[0].identity;
		memcpy(historical.claim_sha256, old.refs[0].claim_sha256, 32);
		v2_anchor_object(bytes, &historical, &historical.identity, path);
		UT_ASSERT_EQ(
			cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &old), 0);
		test_checkpoint_prefix_ref = saved_ref;
		test_checkpoint_prefix_ref.claim.identity = historical.identity;
		memcpy(test_checkpoint_prefix_ref.claim.claim_sha256, historical.claim_sha256, 32);
		snprintf(path, sizeof(path), "%s/thread_1/generation_" UINT64_FORMAT "/durable_prefix",
				 cluster_wal_threads_dir, historical.identity.origin_owner_incarnation);
		UT_ASSERT_EQ(mkdir(path, 0700), 0);
		snprintf(test_checkpoint_prefix_path, sizeof(test_checkpoint_prefix_path), "%s/current",
				 path);
		native.state = DB_SHUTDOWNED;
		native.checkPoint = historical.checkpoint;
		native.checkPointCopy = historical.checkpoint_copy;
		v2_checkpoint_wal_record(&historical.identity, &native, 0);
		old.records[0].checkpoint_record_crc32c = test_checkpoint_crc;
		old.records[0].tail_last_record_crc32c = test_checkpoint_crc;
		old.records[0].validated_tail_lsn_exclusive = test_checkpoint_end;
		history->records[i].snapshot = old.records[0];
		history->records[i].refs = old.refs[0];
		history->records[i].publisher_incarnation = old.publisher_incarnation[0];
		history->records[i].publisher_node = 0;
	}
	test_checkpoint_prefix_ref = saved_ref;
	test_checkpoint_prefix = saved_prefix;
	test_insert = saved_insert;
	test_flush = saved_flush;
	test_checkpoint_end = saved_end;
	test_checkpoint_crc = saved_crc;
	strlcpy(test_checkpoint_prefix_path, saved_path, sizeof(test_checkpoint_prefix_path));
	if (with_terminal) {
		ClusterWalStartupImage interrupted = *op;
		ClusterWalTerminalImage terminal = { 0 };
		ClusterWalStartupStage terminal_stage;
		uint8 claim_bytes[CLUSTER_WAL_CLAIM_V2_BYTES];
		interrupted.phase = CLUSTER_WAL_STARTUP_INITIALIZING;
		memset(&interrupted.successor, 0, sizeof(interrupted.successor));
		memset(&interrupted.prefix, 0, sizeof(interrupted.prefix));
		interrupted.prefix_timeline = 0;
		interrupted.generation = ++old_generation;
		interrupted.claim.identity.origin_owner_incarnation = old_generation;
		interrupted.claim.identity.root_lineage_seq = old_generation;
		interrupted.claim.identity.thread_claim_crc32c = 0;
		interrupted.claim.claim_generation = old_generation;
		UT_ASSERT_EQ(cluster_wal_claim_v2_encode(&interrupted.claim, claim_bytes), 0);
		interrupted.claim.identity.thread_claim_crc32c = image_crc(claim_bytes, 104);
		put_u64_le(interrupted.operation_uuid, old_generation);
		UT_ASSERT_EQ(cluster_control_root_v3_startup_encode(
						 root, 0, &interrupted, terminal.original, &terminal.original_ref),
					 0);
		terminal.closure_version = 1;
		memcpy(terminal.operation_uuid, interrupted.operation_uuid, 16);
		terminal.generation = ++old_generation;
		terminal.database_incarnation = op->database_incarnation;
		terminal.sealing_sequence = root->header.file_txn_seq;
		sha256_bytes(root->bytes, sizeof(root->bytes), terminal.sealing_sha256);
		terminal.formation_epoch = op->formation_epoch;
		terminal.recoverer_node = 2;
		terminal.recoverer_incarnation = 777;
		terminal.ir_request_id = old_generation;
		memset(terminal.isolation_sha256, 0x51, 32);
		memset(terminal.closure_sha256, 0x61, 32);
		terminal.observation.tail.durable_prefix.sequence = 1;
		test_actual_cf = test_cf_mode = ExclusiveLock;
		UT_ASSERT_EQ(cluster_wal_terminal_prepare(root, 0, history, &terminal, &terminal_stage), 0);
		UT_ASSERT_EQ(cluster_wal_terminal_install(&terminal_stage), 0);
		if (ut_current_failed)
			return;
		history->terminal_count = 1;
		history->terminals[0].incarnation = interrupted.claim.identity.origin_owner_incarnation;
		history->terminals[0].generation = terminal_stage.generation;
		memcpy(history->terminals[0].sha256, terminal_stage.sha256, 32);
		UT_ASSERT_EQ(cluster_wal_terminal_discard(&terminal_stage), 0);
	}
	put_u64_le(uuid, ++old_generation);
	root->header.file_txn_seq++;
	test_actual_cf = test_cf_mode = ExclusiveLock;
	UT_ASSERT_EQ(
		cluster_wal_history_prepare(root, 0, history, root->header.file_txn_seq, uuid, &stage), 0);
	UT_ASSERT_EQ(cluster_wal_history_install(&stage), 0);
	root->refs[0].history_generation = stage.generation;
	memcpy(root->refs[0].history_sha256, stage.sha256, 32);
	op->predecessor.refs = root->refs[0];
	op->generation = root->header.file_txn_seq;
	UT_ASSERT_EQ(cluster_wal_startup_prepare(root, 0, op, &operation_stage), 0);
	UT_ASSERT_EQ(cluster_wal_startup_install(&operation_stage), 0);
	root->startup[0].generation = operation_stage.generation;
	memcpy(root->startup[0].sha256, operation_stage.sha256, 32);
	UT_ASSERT_EQ(cluster_control_root_v3_encode(root), 0);
	v2_write_roots(root->bytes);
	UT_ASSERT_EQ(cluster_wal_startup_read_locked(root, 0, op), 0);
	UT_ASSERT_EQ(cluster_wal_history_discard(&stage), 0);
	UT_ASSERT_EQ(cluster_wal_startup_discard(&operation_stage), 0);
	test_actual_cf = test_cf_mode = NoLock;
}

UT_TEST(test_v3_startup_install_keeps_full_history_and_refuses_overflow)
{
	for (unsigned scenario = 0; scenario < 4; ++scenario) {
		bool with_terminal = scenario >= 2;
		unsigned count = scenario % 2 == 0 ? 2 : with_terminal ? 127 : 128;
		uint8 before[66048];
		ControlRootImage root, selected;
		ControlFileData common;
		ClusterControlRootFileToken token;
		ClusterWalStartupImage op = v3_install_fixture(before, &root);
		ClusterWalHistoryImage old_history, actual;
		ClusterWalDurablePrefixRef writer;
		v3_install_add_history(&root, &op, &old_history, count, with_terminal);
		if (ut_current_failed)
			return;
		memcpy(before, root.bytes, sizeof(before));
		if (count + old_history.terminal_count == 128) {
			UT_ASSERT_EQ(cluster_control_root_v3_startup_install_writer(&op, &writer),
						 CLUSTER_CONTROL_ROOT_RANGE_INVALID);
			UT_ASSERT(v2_zero(&writer, sizeof(writer)));
			v2_assert_primary_unchanged(before);
			continue;
		}
		UT_ASSERT_EQ(cluster_control_root_v3_startup_install_writer(&op, &writer), 0);
		test_actual_cf = test_cf_mode = ShareLock;
		UT_ASSERT_EQ(cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID, &selected,
																 &common, &token),
					 0);
		UT_ASSERT_EQ(cluster_wal_history_read_locked(&selected, 0, &actual), 0);
		UT_ASSERT_EQ(actual.count, 3);
		UT_ASSERT_EQ(actual.terminal_count, with_terminal ? 1 : 0);
		UT_ASSERT(memcmp(actual.terminals, old_history.terminals, sizeof(actual.terminals)) == 0);
		for (unsigned i = 0; i < count; ++i) {
			UT_ASSERT(memcmp(&old_history.records[i].snapshot, &actual.records[i + 1].snapshot,
							 sizeof(actual.records[0].snapshot))
					  == 0);
			UT_ASSERT(memcmp(&old_history.records[i].refs, &actual.records[i + 1].refs,
							 sizeof(actual.records[0].refs))
					  == 0);
		}
		UT_ASSERT_EQ(actual.records[0].refs.history_generation, 0);
		UT_ASSERT_EQ(actual.records[0].snapshot.identity.origin_owner_incarnation,
					 op.predecessor.snapshot.identity.origin_owner_incarnation);
		test_actual_cf = test_cf_mode = NoLock;
		UT_ASSERT_EQ(cluster_control_root_v3_startup_install_writer(&op, &writer), 0);
		if (with_terminal) {
			char path[MAXPGPATH], hex[65];
			const ClusterWalTerminalRef *terminal = &actual.terminals[0];
			for (unsigned i = 0; i < 32; ++i)
				snprintf(hex + 2 * i, 3, "%02x", terminal->sha256[i]);
			snprintf(path, sizeof(path),
					 "%s/global/wal_startup/thread_1/startup_" UINT64_FORMAT "-%s.bin", test_root,
					 terminal->generation, hex);
			UT_ASSERT_EQ(unlink(path), 0);
			UT_ASSERT_EQ(cluster_control_root_v3_startup_install_writer(&op, &writer),
						 CLUSTER_CONTROL_ROOT_ABSENT);
			UT_ASSERT(v2_zero(&writer, sizeof(writer)));
			v2_assert_primary_unchanged(selected.bytes);
			continue;
		}
		/* A missing retained claim cannot be silently skipped on retry. */
		{
			char path[MAXPGPATH];
			v2_claim_path(&old_history.records[0].snapshot.identity, path);
			UT_ASSERT_EQ(unlink(path), 0);
			UT_ASSERT(cluster_control_root_v3_startup_install_writer(&op, &writer) != 0);
			UT_ASSERT(v2_zero(&writer, sizeof(writer)));
			v2_assert_primary_unchanged(selected.bytes);
		}
	}
	test_reserve_mode = false;
}

UT_TEST(test_v3_startup_install_sparse_pair_has_no_four_member_assumption)
{
	uint8 before[66048];
	ControlRootImage root, selected;
	ControlFileData common, candidate;
	ClusterControlRootFileToken token;
	ClusterWalStartupImage own = v3_install_fixture(before, &root), peer, durable;
	ClusterWalDurablePrefixRef writer;
	if (ut_current_failed)
		return;
	UT_ASSERT_EQ(cluster_control_root_v3_startup_install_writer(&own, &writer), 0);
	test_actual_cf = test_cf_mode = ShareLock;
	UT_ASSERT_EQ(
		cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID, &root, &common, &token),
		0);
	UT_ASSERT_EQ(cluster_wal_startup_read_locked(&root, 3, &peer), 0);
	UT_ASSERT_EQ(cluster_control_root_v3_read_thread_locked(&peer.predecessor.snapshot.identity,
															&selected, &candidate, &token),
				 0);
	test_actual_cf = test_cf_mode = NoLock;
	cluster_node_id = 3;
	test_self_incarnation = test_membership_incarnation
		= peer.claim.identity.origin_owner_incarnation;
	v3_startup_native_record(&peer, &candidate);
	history_stage_dirs(3);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_checkpoint(&peer.claim.identity,
															peer.operation_uuid, &candidate,
															test_checkpoint_end, &durable),
				 0);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_install_writer(&durable, &writer), 0);
	UT_ASSERT_EQ(writer.claim.identity.origin_thread_id, 4);
	test_actual_cf = test_cf_mode = ShareLock;
	UT_ASSERT_EQ(cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID, &selected,
															 &common, &token),
				 0);
	UT_ASSERT_EQ(selected.header.v2.configured[0], 9);
	UT_ASSERT_EQ(selected.header.v2.serving[0], 0);
	UT_ASSERT_EQ(selected.header.v2.database_state, CLUSTER_CONTROL_ROOT_DATABASE_MOUNTED);
	UT_ASSERT_EQ(selected.startup[0].generation, 0);
	UT_ASSERT_EQ(selected.startup[3].generation, 0);
	UT_ASSERT(memcmp(&selected.records[0], &root.records[0], sizeof(root.records[0])) == 0);
	UT_ASSERT(memcmp(&selected.refs[0], &root.refs[0], sizeof(root.refs[0])) == 0);
	UT_ASSERT(memcmp(&selected.records[3], &durable.successor.snapshot, sizeof(selected.records[3]))
			  == 0);
	test_actual_cf = test_cf_mode = NoLock;
	cluster_node_id = 0;
	test_reserve_mode = false;
}

UT_TEST(test_v3_startup_checkpoint_refuses_runtime_and_control_drift)
{
	for (int fault = 0; fault < 18; ++fault) {
		uint8 before[66048], uuid[16];
		ControlRootImage root;
		ControlFileData candidate;
		ClusterWalStartupImage op = v3_startup_checkpoint_fixture(before, &root, &candidate), out;
		if (ut_current_failed)
			return;
		memcpy(uuid, op.operation_uuid, 16);
		switch (fault) {
		case 0:
			MyBackendType = B_CHECKPOINTER;
			break;
		case 1:
			test_startup_bound = false;
			break;
		case 2:
			ShutdownRequestPending = true;
			break;
		case 3:
			CritSectionCount = 1;
			break;
		case 4:
			test_reserve_provider = false;
			break;
		case 5:
			++test_self_incarnation;
			break;
		case 6:
			++test_epoch;
			break;
		case 7:
			++test_insert;
			break;
		case 8:
			uuid[0] ^= 1;
			break;
		case 9:
			candidate.state = DB_IN_PRODUCTION;
			break;
		case 10:
			candidate.checkPointCopy.redo -= 8;
			break;
		case 11:
			candidate.minRecoveryPoint = 1;
			break;
		case 12:
			candidate.backupStartPoint = 1;
			break;
		case 13:
			++candidate.wal_level;
			break;
		case 14:
			candidate.checkPointCopy.fullPageWrites = !candidate.checkPointCopy.fullPageWrites;
			break;
		case 15:
			++test_reserve_formation.membership.last_admitted_incarnation[3];
			break;
		case 16:
			enableFsync = false;
			break;
		case 17:
			test_actual_cf = test_cf_mode = ShareLock;
			test_checkpoint_outer_cf = true;
			break;
		}
		INIT_CRC32C(candidate.crc);
		COMP_CRC32C(candidate.crc, &candidate, offsetof(ControlFileData, crc));
		FIN_CRC32C(candidate.crc);
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT(cluster_control_root_v3_startup_checkpoint(&op.claim.identity, uuid, &candidate,
															 test_checkpoint_end, &out)
				  != 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
		v2_assert_primary_unchanged(before);
		UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
		UT_ASSERT_EQ(test_actual_cf, fault == 17 ? ShareLock : NoLock);
		CritSectionCount = 0;
		test_checkpoint_outer_cf = false;
		test_actual_cf = test_cf_mode = NoLock;
		test_reserve_mode = false;
	}
}

UT_TEST(test_v3_startup_checkpoint_requires_real_successor_bytes)
{
	for (int fault = 0; fault < 10; ++fault) {
		uint8 before[66048];
		ControlRootImage root;
		ControlFileData candidate;
		ClusterWalStartupImage op = v3_startup_checkpoint_fixture(before, &root, &candidate), out;
		char path[MAXPGPATH];
		if (ut_current_failed)
			return;
		v2_checkpoint_wal_path(&op.claim.identity, candidate.checkPoint, op.timeline, path);
		if (fault < 6)
			v2_checkpoint_wal_record(&op.claim.identity, &candidate, fault + 1);
		if (fault == 6)
			UT_ASSERT_EQ(unlink(path), 0);
		if (fault == 7) {
			test_checkpoint_prefix = (ClusterWalDurablePrefix){ 1, 0, 0, 0 };
			v2_checkpoint_prefix_write();
		}
		if (fault == 8) {
			test_checkpoint_prefix.record_crc ^= 1;
			v2_checkpoint_prefix_write();
		}
		if (fault == 9) {
			v2_claim_path(&op.claim.identity, path);
			UT_ASSERT_EQ(unlink(path), 0);
		}
		UT_ASSERT(cluster_control_root_v3_startup_checkpoint(&op.claim.identity, op.operation_uuid,
															 &candidate, test_checkpoint_end, &out)
				  != 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
		v2_assert_primary_unchanged(before);
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
		test_reserve_mode = false;
	}
}

static void
v3_startup_checkpoint_late_change(void)
{
	test_checkpoint_x_hook = test_checkpoint_published_hook = NULL;
	++test_reserve_formation.membership.last_admitted_incarnation[3];
}

UT_TEST(test_v3_startup_checkpoint_persistence_and_late_races_keep_obligations)
{
	for (int fault = 0; fault < 10; ++fault) {
		uint8 before[66048], after[66048];
		ControlRootImage root;
		ControlFileData candidate;
		ClusterWalStartupImage op = v3_startup_checkpoint_fixture(before, &root, &candidate), out;
		char path[MAXPGPATH];
		if (ut_current_failed)
			return;
		test_history_sync_count = 0;
		if (fault < 4)
			test_history_fail_sync = fault + 1;
		if (fault == 4)
			test_fail_primary_rename = true;
		if (fault == 5)
			test_checkpoint_x_hook = v3_startup_checkpoint_late_change;
		if (fault == 6)
			test_checkpoint_published_hook = v3_startup_checkpoint_late_change;
		if (fault == 7)
			test_fence_after_primary = true;
		if (fault == 8)
			test_release_after_primary = true;
		if (fault == 9)
			test_walr_end_result = CLUSTER_WALR_RELEASE_UNCONFIRMED;
		UT_ASSERT(cluster_control_root_v3_startup_checkpoint(&op.claim.identity, op.operation_uuid,
															 &candidate, test_checkpoint_end, &out)
				  != 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
		path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
		read_all_or_abort(path, after, sizeof(after));
		UT_ASSERT_EQ(
			cluster_control_root_v3_decode(after, sizeof(after), v2_storage, TEST_SYSID, &root), 0);
		UT_ASSERT_EQ(root.header.v2.serving[0], 0);
		UT_ASSERT(memcmp(after + 512, before + 512, 328) == 0);
		UT_ASSERT(root.startup[0].generation != 0);
		if (fault <= 5)
			v2_assert_primary_unchanged(before);
		else {
			/* The root may already have changed. No rollback or erased object
			 * is allowed simply because the caller could not receive success. */
			UT_ASSERT(root.header.file_txn_seq > test_reserve_cut.key.root_sequence);
		}
		test_cf_release_confirmed = true;
		test_actual_cf = test_cf_mode = NoLock;
		test_walr_end_result = CLUSTER_WALR_RELEASE_CONFIRMED;
		test_history_fail_sync = 0;
		test_reserve_mode = false;
	}
}

UT_TEST(test_v3_begin_failures_preserve_selected_obligations_without_permission)
{
	for (int fault = 0; fault < 11; ++fault) {
		uint8 before[66048], after[66048];
		ControlRootImage root, selected;
		ClusterControlRootFileToken token, advanced;
		ClusterWalStartupImage op = v3_begin_fixture(before, &root, &token);
		char path[MAXPGPATH];
		if (ut_current_failed)
			return;
		if (fault == 0)
			token.image_sha256[0] ^= 1;
		if (fault == 1)
			op.operation_uuid[0] ^= 1;
		if (fault == 2)
			MyBackendType = B_LMON;
		if (fault == 3)
			test_reserve_provider = false;
		if (fault == 4) {
			snprintf(path, sizeof(path),
					 "%s/thread_4/generation_" UINT64_FORMAT "/durable_prefix/current",
					 cluster_wal_threads_dir, test_reserve_cut.observer[3]);
			write_all_or_abort(path, "BAD", 3);
		}
		if (fault == 5) {
			test_history_sync_count = 0;
			test_history_fail_sync = 1;
		}
		if (fault == 6)
			test_fail_primary_rename = true;
		if (fault == 7)
			test_fence_after_primary = true;
		if (fault == 8)
			test_release_after_primary = true;
		if (fault == 9 || fault == 10) {
			test_reserve_fault = 3;
			if (fault == 9)
				test_startup_sync_hook = v3_target_late_change;
			else
				test_checkpoint_published_hook = v3_target_late_change;
		}
		memset(&advanced, 0xff, sizeof(advanced));
		UT_ASSERT(cluster_control_root_v3_startup_begin_clean(&op.claim.identity, op.operation_uuid,
															  &token, &advanced)
				  != 0);
		UT_ASSERT(v2_zero(&advanced, sizeof(advanced)));
		if (fault <= 6 || fault == 9)
			v2_assert_primary_unchanged(before);
		else {
			path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
			read_all_or_abort(path, after, sizeof(after));
			UT_ASSERT_EQ(cluster_control_root_v3_decode(after, sizeof(after), v2_storage,
														TEST_SYSID, &selected),
						 0);
			UT_ASSERT_EQ(selected.header.v2.serving[0], 0);
			test_actual_cf = test_cf_mode = ShareLock;
			for (unsigned node = 0; node <= 3; node += 3) {
				ClusterWalStartupImage pending;
				UT_ASSERT_EQ(cluster_wal_startup_read_locked(&selected, node, &pending), 0);
				UT_ASSERT_EQ(pending.phase, CLUSTER_WAL_STARTUP_INITIALIZING);
				UT_ASSERT(
					memcmp(&root.records[node], &selected.records[node], sizeof(root.records[node]))
					== 0);
			}
		}
		test_history_fail_sync = 0;
		test_cf_release_confirmed = true;
		test_actual_cf = test_cf_mode = NoLock;
		test_startup_sync_hook = test_checkpoint_published_hook = NULL;
		test_reserve_mode = false;
	}
}

UT_TEST(test_v3_reserve_requires_actual_collective_evidence_and_owner)
{
	for (int fault = 0; fault < 13; ++fault) {
		uint8 before[66048];
		ControlRootImage root;
		ClusterControlRootFileToken token;
		v3_reserve_fixture(before, &root);
		memset(&token, 0xff, sizeof(token));
		if (fault == 0)
			test_reserve_evidence = CLUSTER_STARTUP_EXIT_WAITING;
		if (fault == 1)
			test_reserve_evidence = CLUSTER_STARTUP_EXIT_UNAVAILABLE;
		if (fault == 2)
			test_reserve_provider = false;
		if (fault == 3)
			test_reserve_quorum = false;
		if (fault == 4)
			test_fence = false;
		if (fault == 5)
			test_reserve_fault = 1;
		if (fault == 6)
			test_reserve_fault = 2;
		if (fault == 7)
			test_reserve_formation.membership.last_admitted_incarnation[3]++;
		if (fault == 8)
			MyBackendType = B_BACKEND;
		if (fault == 9)
			ShutdownRequestPending = true;
		if (fault == 10)
			test_self_incarnation++;
		if (fault == 11)
			enableFsync = false;
		if (fault == 12)
			test_system_identifier++;
		UT_ASSERT(cluster_control_root_v3_reserve_clean(&test_reserve_cut, &token) != 0);
		UT_ASSERT(v2_zero(&token, sizeof(token)));
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
		v2_assert_primary_unchanged(before);
		if (fault == 12)
			test_system_identifier--;
		test_reserve_mode = false;
	}
}

static void
v3_reserve_late_change(void)
{
	test_checkpoint_x_hook = NULL;
	if (test_reserve_fault == 3)
		test_reserve_formation.membership.last_admitted_incarnation[3]++;
	else if (test_reserve_fault == 4)
		test_reserve_provider = false;
	else if (test_reserve_fault == 5)
		test_reserve_evidence = CLUSTER_STARTUP_EXIT_WAITING;
	else if (test_reserve_fault == 6)
		v2_checkpoint_root_race();
}

UT_TEST(test_v3_reserve_rechecks_cut_after_wal_scan)
{
	for (int fault = 3; fault <= 6; ++fault) {
		uint8 before[66048];
		ControlRootImage root;
		ClusterControlRootFileToken token;
		v3_reserve_fixture(before, &root);
		test_reserve_fault = fault;
		test_checkpoint_x_hook = v3_reserve_late_change;
		UT_ASSERT(cluster_control_root_v3_reserve_clean(&test_reserve_cut, &token) != 0);
		UT_ASSERT(v2_zero(&token, sizeof(token)));
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
		v2_assert_primary_unchanged(fault == 6 ? v2_race_winner : before);
		test_reserve_mode = false;
	}
}

UT_TEST(test_v3_reserve_consumes_real_nonlocal_files)
{
	for (int fault = 0; fault < 4; ++fault) {
		uint8 before[66048];
		ControlRootImage root;
		ClusterControlRootFileToken token;
		char path[MAXPGPATH], saved[MAXPGPATH];
		v3_reserve_fixture(before, &root);
		if (fault == 0)
			v2_checkpoint_wal_path(&root.records[3].identity, root.records[3].checkpoint_lower_lsn,
								   root.records[3].checkpoint_tli, path);
		else if (fault == 1)
			snprintf(path, sizeof(path), "%s/thread_4/generation_" UINT64_FORMAT "/%s",
					 cluster_wal_threads_dir, root.records[3].identity.origin_owner_incarnation,
					 CLUSTER_WAL_THREAD_CLAIM_FILENAME);
		else if (fault == 2)
			snprintf(path, sizeof(path),
					 "%s/thread_4/generation_" UINT64_FORMAT "/durable_prefix/current",
					 cluster_wal_threads_dir, root.records[3].identity.origin_owner_incarnation);
		else {
			char digest[65];
			for (int i = 0; i < 32; i++)
				snprintf(digest + i * 2, 3, "%02x", root.refs[3].anchor_sha256[i]);
			snprintf(path, sizeof(path),
					 "%s/global/anchor_images/thread_4/generation_" UINT64_FORMAT
					 "/anchor_" UINT64_FORMAT "-%s.bin",
					 test_root, root.records[3].identity.origin_owner_incarnation,
					 root.refs[3].anchor_generation, digest);
		}
		snprintf(saved, sizeof(saved), "%s.reserve-test-saved", path);
		UT_ASSERT_EQ(rename(path, saved), 0);
		UT_ASSERT(cluster_control_root_v3_reserve_clean(&test_reserve_cut, &token) != 0);
		UT_ASSERT(v2_zero(&token, sizeof(token)));
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
		v2_assert_primary_unchanged(before);
		UT_ASSERT_EQ(rename(saved, path), 0);
		test_reserve_mode = false;
	}
}

UT_TEST(test_v3_reserve_uncertain_publication_never_returns_permission)
{
	for (int fault = 0; fault < 6; ++fault) {
		uint8 before[66048], actual[66048];
		ControlRootImage root;
		ClusterControlRootFileToken token;
		char path[MAXPGPATH];
		v3_reserve_fixture(before, &root);
		if (fault == 0)
			test_fail_primary_rename = true;
		if (fault == 1)
			test_fence_after_primary = true;
		if (fault == 2)
			test_release_after_primary = true;
		if (fault == 3)
			test_history_fail_sync = 1;
		if (fault == 4) {
			test_reserve_fault = 3;
			test_checkpoint_published_hook = v3_reserve_late_change;
		}
		if (fault == 5) {
			test_reserve_fault = 5;
			test_checkpoint_published_hook = v3_reserve_late_change;
		}
		UT_ASSERT(cluster_control_root_v3_reserve_clean(&test_reserve_cut, &token) != 0);
		UT_ASSERT(v2_zero(&token, sizeof(token)));
		if (fault == 0 || fault == 3)
			v2_assert_primary_unchanged(before);
		else {
			path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
			read_all_or_abort(path, actual, sizeof(actual));
			UT_ASSERT_EQ(cluster_control_root_v3_decode(actual, sizeof(actual), v2_storage,
														TEST_SYSID, &root),
						 0);
			UT_ASSERT_EQ(root.header.v2.serving[0], 0);
			UT_ASSERT(root.startup[0].generation != 0 && root.startup[3].generation != 0);
		}
		test_history_fail_sync = 0;
		test_cf_release_confirmed = true;
		test_actual_cf = NoLock;
		test_reserve_mode = false;
	}
}

UT_TEST(test_v3_reserve_error_cleanup_keeps_old_authority)
{
	uint8 before[66048];
	ControlRootImage root;
	ClusterControlRootFileToken token;
	volatile bool caught = false;
	int open_before = 0, open_after = 0;
	v3_reserve_fixture(before, &root);
	test_stop_share_call = test_cf_lock_calls + 2;
	test_stop_share_hook = v2_checkpoint_throw_on_x;
	for (int fd = 0; fd < 256; ++fd)
		if (fcntl(fd, F_GETFD) >= 0)
			++open_before;
	PG_TRY();
	{
		(void)cluster_control_root_v3_reserve_clean(&test_reserve_cut, &token);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	for (int fd = 0; fd < 256; ++fd)
		if (fcntl(fd, F_GETFD) >= 0)
			++open_after;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(open_before, open_after);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
	UT_ASSERT(v2_zero(&token, sizeof(token)));
	v2_assert_primary_unchanged(before);
	test_reserve_mode = false;
}

UT_TEST(test_v2_normal_close_pair_waits_for_last_thread_and_preserves_roster)
{
	const int peers[] = { 1, 3 };
	for (unsigned i = 0; i < lengthof(peers); i++) {
		uint8 before[66048];
		ClusterPhase1FullStopPlan plan;
		ClusterWalDurablePrefixRef peer;
		ClusterControlRootStopObservation out;
		bool complete = true;
		v2_close_fixture(before, &plan);
		peer = v2_close_add_peer(before, peers[i], &plan);
		UT_ASSERT_EQ(cluster_control_root_v2_normal_stop_close(&plan, &complete), 0);
		UT_ASSERT(!complete);
		UT_ASSERT_EQ(cluster_control_root_v2_stop_phase_read(1, plan.member_incarnations[0], &out),
					 0);
		UT_ASSERT_EQ(out.members[0].phase, CLUSTER_CONTROL_ROOT_STOP_CLOSED);
		UT_ASSERT_EQ(out.members[peers[i]].phase, CLUSTER_CONTROL_ROOT_STOP_CHECKPOINT);
		UT_ASSERT_EQ(cluster_control_root_v2_normal_stop_close(&plan, &complete), 0);
		UT_ASSERT(!complete);
		cluster_node_id = peers[i];
		test_own_thread = peers[i] + 1;
		test_self_incarnation = test_membership_incarnation
			= peer.claim.identity.origin_owner_incarnation;
		test_checkpoint_prefix_ref = peer;
		plan.own_wal_started_at = peer.claim.identity.thread_claim_created_at;
		UT_ASSERT_EQ(cluster_control_root_v2_normal_stop_close(&plan, &complete), 0);
		UT_ASSERT(complete);
		UT_ASSERT_EQ(cluster_control_root_v2_stop_phase_read(1, plan.member_incarnations[0], &out),
					 0);
		UT_ASSERT_EQ(out.members[0].phase, CLUSTER_CONTROL_ROOT_STOP_CLOSED);
		UT_ASSERT_EQ(out.members[peers[i]].phase, CLUSTER_CONTROL_ROOT_STOP_CLOSED);
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
	}
}

UT_TEST(test_v2_normal_close_publishes_exact_durable_root_not_voting_exit)
{
	uint8 before[66048];
	ClusterPhase1FullStopPlan plan;
	ClusterControlRootStopObservation selected;
	bool all_closed = false;
	int writes;
	v2_close_fixture(before, &plan);
	UT_ASSERT_EQ(cluster_control_root_v2_normal_stop_close(&plan, &all_closed), 0);
	UT_ASSERT(all_closed);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
	UT_ASSERT_EQ(cluster_control_root_v2_stop_phase_read(1, plan.member_incarnations[0], &selected),
				 0);
	UT_ASSERT_EQ(selected.phase, CLUSTER_CONTROL_ROOT_STOP_CLOSED);
	UT_ASSERT_EQ(selected.snapshot.root_publish_seq, 12);
	writes = test_durable_rename_calls;
	all_closed = false;
	UT_ASSERT_EQ(cluster_control_root_v2_normal_stop_close(&plan, &all_closed), 0);
	UT_ASSERT(all_closed);
	UT_ASSERT_EQ(test_durable_rename_calls, writes);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
}

UT_TEST(test_v2_normal_close_requires_real_controller_and_exact_native_cut)
{
	for (int fault = 0; fault < 6; fault++) {
		uint8 before[66048];
		ClusterPhase1FullStopPlan plan;
		bool all_closed = true;
		v2_close_fixture(before, &plan);
		if (fault == 0)
			test_close_owned = false;
		if (fault == 1)
			test_insert += 8;
		if (fault == 2)
			plan.member_incarnations[0]++;
		if (fault == 3)
			plan.member_incarnations[3] = 102;
		if (fault == 4)
			plan.epoch++;
		if (fault == 5)
			test_fence = false;
		UT_ASSERT(cluster_control_root_v2_normal_stop_close(&plan, &all_closed) != 0);
		UT_ASSERT(!all_closed);
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		v2_assert_primary_unchanged(before);
	}
}

UT_TEST(test_v2_normal_close_never_returns_success_after_uncertain_release)
{
	for (int fault = 0; fault < 3; fault++) {
		uint8 before[66048];
		ClusterPhase1FullStopPlan plan;
		bool complete = true;
		v2_close_fixture(before, &plan);
		if (fault == 0)
			test_cf_release_confirmed = false;
		if (fault == 1)
			test_walr_end_result = CLUSTER_WALR_RELEASE_UNCONFIRMED;
		if (fault == 2)
			test_projection_sync_fault = test_projection_observe = true;
		UT_ASSERT(cluster_control_root_v2_normal_stop_close(&plan, &complete) != 0);
		UT_ASSERT(!complete);
		test_projection_sync_fault = false;
		test_projection_observe = false;
		test_cf_release_confirmed = true;
		test_walr_end_result = CLUSTER_WALR_RELEASE_CONFIRMED;
	}
}

UT_TEST(test_v2_stop_phase_accepts_durable_closed_successor_not_active)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ControlFileData candidate, view;
	ClusterControlRootStopObservation out;
	int writes;

	v2_stop_observation_fixture(before, &self, &candidate);
	v2_stop_clean_anchor(before, &self);
	/* Selected durable lifecycle changes; the authenticated raw shutdown
	 * anchor, exact prefix and original serving roster do not. */
	before[512 + 10] = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	v2_checksums(before);
	v2_write_roots(before);
	writes = test_durable_rename_calls;
	UT_ASSERT_EQ(cluster_control_root_v2_stop_phase_read(self.origin_thread_id,
														 self.origin_owner_incarnation, &out),
				 0);
	UT_ASSERT_EQ(out.phase, CLUSTER_CONTROL_ROOT_STOP_CLOSED);
	UT_ASSERT_EQ(out.members[0].phase, out.phase);
	UT_ASSERT_EQ(out.members[127].phase, CLUSTER_CONTROL_ROOT_STOP_ACTIVE);
	UT_ASSERT_EQ(out.snapshot.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED);
	UT_ASSERT_EQ(test_durable_rename_calls, writes);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	/* The stop consumer must not widen ordinary writer admission. */
	test_cf_mode = ExclusiveLock;
	UT_ASSERT(cluster_control_root_v2_read_runtime_local_locked(&view) != 0);
	test_cf_mode = NoLock;
	v2_assert_primary_unchanged(before);
}

UT_TEST(test_v2_stop_phase_refuses_wrong_owner_missing_claim_or_late_prefix)
{
	for (int fault = 0; fault < 5; fault++) {
		uint8 before[66048];
		ClusterControlRootIdentity self;
		ControlFileData candidate;
		ClusterControlRootStopObservation out;
		char path[MAXPGPATH];
		uint64 incarnation;

		v2_stop_observation_fixture(before, &self, &candidate);
		incarnation = self.origin_owner_incarnation;
		if (fault == 0)
			incarnation++;
		else if (fault == 1) {
			v2_claim_path(&self, path);
			UT_ASSERT_EQ(unlink(path), 0);
		} else if (fault == 2) {
			test_checkpoint_prefix.sequence++;
			test_checkpoint_prefix.record_start = test_checkpoint_end;
			test_checkpoint_prefix.exclusive_end += 80;
			v2_checkpoint_prefix_write();
		} else if (fault == 3) {
			/* A recovery-required writer is not an active stop participant. */
			before[512 + 10] = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
			v2_checksums(before);
			v2_write_roots(before);
		} else {
			ClusterControlRootIdentity peer = self;
			peer.origin_thread_id = 128;
			peer.origin_owner_incarnation = 226;
			v2_claim_path(&peer, path);
			UT_ASSERT_EQ(unlink(path), 0);
		}
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT_NE(
			cluster_control_root_v2_stop_phase_read(self.origin_thread_id, incarnation, &out),
			CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		v2_assert_primary_unchanged(before);
	}
}

UT_TEST(test_runtime_v3_stop_phase_service_release_is_input_kind_and_cut_bound)
{
	for (int mutation = 0; mutation < 5; mutation++) {
		uint8 before[66048];
		ClusterControlRootIdentity self;
		ControlFileData candidate;
		ClusterControlRootStopObservation out;
		ClusterControlRootSnapshot ordinary;
		ClusterControlRootReadToken token;
		int locks;
		uint64 incarnation;

		v2_stop_observation_fixture(before, &self, &candidate);
		runtime_fixture_version3(before);
		MyBackendType = B_LMON;
		test_cf_release_confirmed = false;
		incarnation = self.origin_owner_incarnation;
		if (mutation == 4) {
			UT_ASSERT_EQ(cluster_control_root_read_canonical_discovered(self.origin_thread_id,
																		&ordinary, &token),
						 CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
		} else {
			UT_ASSERT_EQ(
				cluster_control_root_v3_stop_phase_read(self.origin_thread_id, incarnation, &out),
				CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
			UT_ASSERT(v2_zero(&out, sizeof(out)));
		}
		locks = test_cf_lock_calls;
		test_cf_release_confirmed = true;
		if (mutation == 1)
			incarnation++;
		if (mutation == 2)
			test_control_generation++;
		if (mutation == 3) {
			UT_ASSERT_EQ(cluster_control_root_read_canonical_discovered(self.origin_thread_id,
																		&ordinary, &token),
						 CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
			UT_ASSERT(v2_zero(&ordinary, sizeof(ordinary)) && v2_zero(&token, sizeof(token)));
		} else {
			UT_ASSERT_EQ(
				cluster_control_root_v3_stop_phase_read(self.origin_thread_id, incarnation, &out),
				mutation == 0 ? CLUSTER_CONTROL_ROOT_OK_PRIMARY
							  : CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
			if (mutation == 0)
				UT_ASSERT_EQ(out.phase, CLUSTER_CONTROL_ROOT_STOP_CHECKPOINT);
			else
				UT_ASSERT(v2_zero(&out, sizeof(out)));
		}
		UT_ASSERT_EQ(test_cf_lock_calls, locks);
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		MyBackendType = B_INVALID;
		cluster_shared_config = false;
	}
}

UT_TEST(test_v2_stop_phase_error_cleans_cf_and_cannot_return_evidence)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ControlFileData candidate;
	static ClusterControlRootStopObservation out;
	volatile bool caught = false;
	v2_retention_fixture(before, &self, &candidate);
	test_throw_root_read = true;
	memset(&out, 0xa5, sizeof(out));
	PG_TRY();
	{
		(void)cluster_control_root_v2_stop_phase_read(self.origin_thread_id,
													  self.origin_owner_incarnation, &out);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	test_throw_root_read = false;
	UT_ASSERT(caught);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	v2_assert_primary_unchanged(before);
	cluster_shared_config = false;
}

UT_TEST(test_runtime_v3_service_read_waits_for_exact_retirement_before_output)
{
	uint8 bytes[66048];
	ClusterControlRootIdentity self;
	ControlFileData candidate;
	static ClusterControlRootSnapshot out;
	static ClusterControlRootReadToken token;
	volatile bool caught = false;
	volatile ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	int locks;

	v2_retention_fixture(bytes, &self, &candidate);
	runtime_fixture_version3(bytes);
	MyBackendType = B_LMON;
	test_cf_release_confirmed = false;
	test_capture_error_level = true;
	PG_TRY();
	{
		result = cluster_control_root_read_canonical(
			self.origin_thread_id, &self, CLUSTER_CONTROL_ROOT_READ_STRONG, &out, &token);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	test_capture_error_level = false;
	UT_ASSERT(!caught);
	UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
	locks = test_cf_lock_calls;
	if (!caught) {
		UT_ASSERT_EQ(cluster_control_root_read_canonical(self.origin_thread_id, &self,
														 CLUSTER_CONTROL_ROOT_READ_STRONG, &out,
														 &token),
					 CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
		test_cf_release_confirmed = true;
		UT_ASSERT_EQ(cluster_control_root_read_canonical(self.origin_thread_id, &self,
														 CLUSTER_CONTROL_ROOT_READ_STRONG, &out,
														 &token),
					 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(test_cf_lock_calls, locks);
		UT_ASSERT(cluster_control_root_identity_equal(&out.identity, &self));
		UT_ASSERT(!v2_zero(&token, sizeof(token)));
	}
	MyBackendType = B_INVALID;
	cluster_shared_config = false;
}

UT_TEST(test_runtime_v3_service_pending_observation_is_bound_to_input_and_cut)
{
	for (int mutation = 0; mutation < 2; mutation++) {
		uint8 bytes[66048];
		ClusterControlRootIdentity self, wrong;
		ControlFileData candidate;
		ClusterControlRootSnapshot out;
		ClusterControlRootReadToken token;

		v2_retention_fixture(bytes, &self, &candidate);
		runtime_fixture_version3(bytes);
		MyBackendType = B_LMON;
		test_cf_release_confirmed = false;
		UT_ASSERT_EQ(cluster_control_root_read_canonical(self.origin_thread_id, &self,
														 CLUSTER_CONTROL_ROOT_READ_STRONG, &out,
														 &token),
					 CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
		test_cf_release_confirmed = true;
		wrong = self;
		if (mutation == 0)
			test_control_generation++;
		else
			wrong.origin_owner_incarnation++;
		UT_ASSERT_EQ(cluster_control_root_read_canonical(self.origin_thread_id, &wrong,
														 CLUSTER_CONTROL_ROOT_READ_STRONG, &out,
														 &token),
					 CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
		UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
		MyBackendType = B_INVALID;
		cluster_shared_config = false;
	}
}

UT_TEST(test_runtime_v3_service_immediate_release_also_rechecks_observation_cut)
{
	uint8 bytes[66048];
	ClusterControlRootIdentity self;
	ControlFileData candidate;
	ClusterControlRootSnapshot out;
	ClusterControlRootReadToken token;

	v2_retention_fixture(bytes, &self, &candidate);
	runtime_fixture_version3(bytes);
	MyBackendType = B_LMON;
	test_cf_release_cut_change = true;
	UT_ASSERT_EQ(cluster_control_root_read_canonical(
					 self.origin_thread_id, &self, CLUSTER_CONTROL_ROOT_READ_STRONG, &out, &token),
				 CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
	test_cf_release_cut_change = false;
	MyBackendType = B_INVALID;
	cluster_shared_config = false;
}

UT_TEST(test_runtime_v3_canonical_unconfirmed_release_is_fatal)
{
	uint8 bytes[66048];
	ClusterControlRootIdentity self;
	ControlFileData candidate;
	static ClusterControlRootSnapshot out;
	static ClusterControlRootReadToken token;
	volatile bool caught = false;
	v2_retention_fixture(bytes, &self, &candidate);
	runtime_fixture_version3(bytes);
	test_cf_release_confirmed = false;
	test_capture_error_level = true;
	test_last_error_level = 0;
	PG_TRY();
	{
		(void)cluster_control_root_read_canonical(self.origin_thread_id, &self,
												  CLUSTER_CONTROL_ROOT_READ_STRONG, &out, &token);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	test_capture_error_level = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(test_last_error_level, FATAL);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	UT_ASSERT(v2_zero(&token, sizeof(token)));
	UT_ASSERT_EQ(test_actual_cf, ShareLock);
	cluster_shared_config = false;
}

/* PGRAC: a checkpoint's observed tail must not be reused as a failed
 * writer's sealed final input. Physical root/claim/anchor I/O is real.
 * Author: SqlRush <sqlrush@gmail.com> */
UT_TEST(test_v2_failure_open_invalidates_old_tail_and_keeps_other_threads)
{
	uint8 before[66048], after[66048];
	ClusterControlRootIdentity self;
	ControlFileData candidate;
	ClusterControlRootSnapshot old, out;
	ClusterControlRootReadToken token;
	ClusterRecoverySerialRequest request;
	ControlRootImage root;
	ClusterControlRootFileToken file_token;

	v2_retention_fixture(before, &self, &candidate);
	UT_ASSERT_EQ(cluster_control_root_v2_read_canonical(self.origin_thread_id, &self, &old, &token),
				 0);
	memset(&request, 0, sizeof(request));
	request.mode = CLUSTER_RECOVERY_SERIAL_INPUT_SEAL;
	request.duty = self;
	request.expected_root_token = token;
	request.formation = (const ClusterFormationWitnessV1 *)(uintptr_t)1;
	request.fence_need_set = (const PgracExternalFenceNeedSetV1 *)(uintptr_t)2;
	request.fence_admission_set = (const PgracExternalFenceAdmissionSetV1 *)(uintptr_t)3;
	request.acquire_timeout_ms = request.release_timeout_ms = 5000;
	test_failure_formation = test_failure_needs = test_failure_admissions = true;
	memset(&test_failure_need, 0, sizeof(test_failure_need));
	test_failure_need.system_identifier = self.system_identifier;
	test_failure_need.victim_node_id = self.origin_node_id;
	test_failure_need.victim_incarnation = self.origin_owner_incarnation;
	UT_ASSERT(cluster_recovery_duty_digest_for_claim(&self, true,
													 &test_failure_need.canonical_duty_digest));
	cluster_node_id = 127;
	test_self_incarnation = 991;
	UT_ASSERT_EQ(cluster_control_root_v2_failure_open_publish(&request, &out, &token), 0);
	UT_ASSERT_EQ(out.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED);
	UT_ASSERT_EQ(out.root_flags, CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID
									 | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID);
	UT_ASSERT_EQ(out.validated_tail_lsn_exclusive, 0);
	UT_ASSERT_EQ(out.checkpoint_lower_lsn, old.checkpoint_lower_lsn);
	UT_ASSERT_EQ(out.root_publish_seq, old.root_publish_seq + 1);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	/* Read through the real root-selected native view after publication. */
	test_cf_mode = test_actual_cf = ShareLock;
	UT_ASSERT_EQ(cluster_control_root_v2_read_thread_locked(&self, &root, &candidate, &file_token),
				 0);
	memcpy(after, root.bytes, sizeof(after));
	UT_ASSERT_EQ(candidate.state, DB_IN_CRASH_RECOVERY);
	UT_ASSERT_EQ(root.header.v2.serving[0] & UINT64_C(1), 0);
	UT_ASSERT(memcmp(before + 512 + 512, after + 512 + 512, sizeof(before) - 1024) == 0);
	cluster_shared_config = false;
}

static void
v2_failure_fixture(uint8 before[66048], ClusterRecoverySerialRequest *request, bool open_failure)
{
	ControlFileData candidate;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken file_token;
	ClusterControlRootIdentity self;
	char primary[MAXPGPATH];

	v2_shutdown_checkpoint_fixture(before, &self, &candidate);
	UT_ASSERT_EQ(v2_shutdown_checkpoint_publish(&self, &candidate, &out, &file_token), 0);
	memset(request, 0, sizeof(*request));
	request->mode = CLUSTER_RECOVERY_SERIAL_INPUT_SEAL;
	request->duty = self;
	request->formation = (const ClusterFormationWitnessV1 *)(uintptr_t)1;
	request->fence_need_set = (const PgracExternalFenceNeedSetV1 *)(uintptr_t)2;
	request->fence_admission_set = (const PgracExternalFenceAdmissionSetV1 *)(uintptr_t)3;
	request->acquire_timeout_ms = request->release_timeout_ms = 5000;
	UT_ASSERT_EQ(
		cluster_control_root_v2_read_canonical(1, &self, &out, &request->expected_root_token), 0);
	test_failure_formation = test_failure_needs = test_failure_admissions = true;
	memset(&test_failure_need, 0, sizeof(test_failure_need));
	test_failure_need.system_identifier = self.system_identifier;
	test_failure_need.victim_node_id = self.origin_node_id;
	test_failure_need.victim_incarnation = self.origin_owner_incarnation;
	UT_ASSERT(cluster_recovery_duty_digest_for_claim(&self, true,
													 &test_failure_need.canonical_duty_digest));
	cluster_node_id = 127;
	test_self_incarnation = 991;
	if (open_failure)
		UT_ASSERT_EQ(cluster_control_root_v2_failure_open_publish(request, &out,
																  &request->expected_root_token),
					 0);
	path_for(primary, sizeof(primary), CLUSTER_CONTROL_ROOT_REL_PATH);
	read_all_or_abort(primary, before, 66048);
}

static void
v2_failure_seal_fixture(uint8 before[66048], ClusterRecoverySerialRequest *request)
{
	v2_failure_fixture(before, request, true);
}

UT_TEST(test_runtime_v3_failure_launch_accepts_open_but_not_clean_thread)
{
	uint8 before[66048];
	ClusterRecoverySerialRequest request;
	ClusterThreadRecLaunchEligibility eligibility;
	v2_failure_fixture(before, &request, false);
	runtime_fixture_version3(before);
	memset(&test_worker_event, 0, sizeof(test_worker_event));
	test_worker_event.reconfig_kind = RECONFIG_KIND_FAIL_STOP;
	test_worker_event.event_id = 12;
	test_worker_event.new_epoch = 123;
	test_worker_event.dead_bitmap[0] = 1;
	test_control_barrier_ready = false;
	test_cf_acquire_order = 0;
	UT_ASSERT(!cluster_reconfig_thread_recovery_eligibility_consume(1, &eligibility));
	UT_ASSERT_EQ(test_cf_acquire_order, 0);
	UT_ASSERT_EQ(eligibility.attempt_stamp, 0);
	test_control_barrier_ready = true;
	UT_ASSERT(cluster_reconfig_thread_recovery_eligibility_consume(1, &eligibility));
	UT_ASSERT_EQ(eligibility.attempt_stamp, 123);
	UT_ASSERT_EQ(memcmp(&eligibility.duty, &request.duty, sizeof(request.duty)), 0);
	test_worker_event.dead_bitmap[0] = 0;
	UT_ASSERT(!cluster_reconfig_thread_recovery_eligibility_consume(1, &eligibility));
	test_worker_event.dead_bitmap[0] = 1;
	before[512 + 10] = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	v2_checksums(before);
	v2_write_roots(before);
	UT_ASSERT(!cluster_reconfig_thread_recovery_eligibility_consume(1, &eligibility));
	cluster_shared_config = false;
}

UT_TEST(test_runtime_v3_failure_launch_selects_interrupted_initializer_not_predecessor)
{
	uint8 before[66048];
	ControlRootImage root;
	ClusterControlRootFileToken token, advanced;
	ClusterThreadRecLaunchEligibility eligibility;
	ClusterWalStartupImage op = v3_begin_fixture(before, &root, &token);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_begin_clean(&op.claim.identity, op.operation_uuid,
															 &token, &advanced),
				 0);
	if (ut_current_failed)
		return;
	cluster_node_id = 127;
	test_self_incarnation = 991;
	memset(&test_worker_event, 0, sizeof(test_worker_event));
	test_worker_event.reconfig_kind = RECONFIG_KIND_FAIL_STOP;
	test_worker_event.event_id = 12;
	test_worker_event.new_epoch = 123;
	test_worker_event.dead_bitmap[0] = 1;
	test_control_barrier_ready = true;
	UT_ASSERT(cluster_reconfig_thread_recovery_eligibility_consume(1, &eligibility));
	UT_ASSERT_EQ(memcmp(&eligibility.duty, &op.claim.identity, sizeof(op.claim.identity)), 0);
	UT_ASSERT(eligibility.duty.origin_owner_incarnation
			  != op.predecessor.snapshot.identity.origin_owner_incarnation);
	UT_ASSERT_EQ(eligibility.subject_kind, CLUSTER_CONTROL_RECOVERY_PENDING_INITIALIZER);
	UT_ASSERT(memcmp(eligibility.selected_root_sha256, advanced.image_sha256, 32) == 0);
	UT_ASSERT_EQ(sizeof(eligibility), BGW_EXTRALEN);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	test_reserve_mode = false;
}

UT_TEST(test_runtime_pending_observation_is_separate_and_requires_actual_inputs)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		uint8 before[66048];
		ControlRootImage root;
		ClusterControlRootFileToken token, advanced;
		ClusterControlRecoverySubject subject;
		ClusterControlRootSnapshot ordinary;
		ClusterControlRootReadToken ordinary_token;
		char path[MAXPGPATH];
		ClusterWalStartupImage op = v3_begin_fixture(before, &root, &token);
		UT_ASSERT_EQ(cluster_control_root_v3_startup_begin_clean(
						 &op.claim.identity, op.operation_uuid, &token, &advanced),
					 0);
		if (ut_current_failed)
			return;
		cluster_node_id = 127;
		test_self_incarnation = 991;
		UT_ASSERT_EQ(cluster_control_root_read_recovery_subject(1, &op.claim.identity, &subject),
					 0);
		UT_ASSERT_EQ(subject.kind, CLUSTER_CONTROL_RECOVERY_PENDING_INITIALIZER);
		UT_ASSERT(v2_zero(&subject.current, sizeof(subject.current)));
		UT_ASSERT(v2_zero(&subject.current_token, sizeof(subject.current_token)));
		UT_ASSERT_EQ(cluster_control_root_v3_read_canonical(1, NULL, &ordinary, &ordinary_token),
					 CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID);
		if (fault == 0) {
			UT_ASSERT_EQ(cluster_control_root_read_recovery_subject(
							 1, &op.predecessor.snapshot.identity, &subject),
						 CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
		} else {
			if (fault == 1)
				v2_claim_path(&op.claim.identity, path);
			else if (fault == 2)
				v3_target_path(path, &op, "/durable_prefix/current");
			else {
				char hash[65];
				for (unsigned i = 0; i < 32; i++)
					snprintf(hash + 2 * i, 3, "%02x", subject.pending.sha256[i]);
				snprintf(path, sizeof(path),
						 "%s/global/wal_startup/thread_1/startup_" UINT64_FORMAT "-%s.bin",
						 test_root, subject.pending.generation, hash);
			}
			UT_ASSERT_EQ(unlink(path), 0);
			UT_ASSERT(cluster_control_root_read_recovery_subject(1, &op.claim.identity, &subject)
					  != 0);
		}
		UT_ASSERT(v2_zero(&subject, sizeof(subject)));
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		test_reserve_mode = false;
	}
}

/* PGRAC: execute the actual background-worker body. The pending W1 must obtain
 * its own isolation/WALR/IR route, never use W0's ordinary DATA replay owners.
 * Author: SqlRush <sqlrush@gmail.com> */
UT_TEST(test_runtime_pending_worker_owns_exact_subject_before_inspection)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		uint8 before[66048];
		ControlRootImage root;
		ClusterControlRootFileToken token, advanced;
		ClusterThreadRecLaunchEligibility eligibility = { 0 };
		ClusterControlRecoverySubject observed;
		ClusterWalStartupImage op = v3_begin_fixture(before, &root, &token);
		UT_ASSERT_EQ(cluster_control_root_v3_startup_begin_clean(
						 &op.claim.identity, op.operation_uuid, &token, &advanced),
					 0);
		if (ut_current_failed)
			return;
		cluster_node_id = 127;
		test_self_incarnation = 991;
		eligibility.origin_thread = 1;
		eligibility.attempt_stamp = 123;
		eligibility.subject_kind = CLUSTER_CONTROL_RECOVERY_PENDING_INITIALIZER;
		eligibility.duty = op.claim.identity;
		memcpy(eligibility.selected_root_sha256, advanced.image_sha256, 32);
		test_failure_formation = test_failure_needs = test_failure_admissions = true;
		memset(&test_failure_need, 0, sizeof(test_failure_need));
		test_failure_need.system_identifier = op.claim.identity.system_identifier;
		test_failure_need.victim_node_id = op.claim.identity.origin_node_id;
		test_failure_need.victim_incarnation = op.claim.identity.origin_owner_incarnation;
		UT_ASSERT(cluster_recovery_duty_digest_for_claim(&op.claim.identity, true,
														 &test_failure_need.canonical_duty_digest));
		if (fault == 1)
			eligibility.selected_root_sha256[0] ^= 1;
		if (fault == 2)
			test_failure_admissions = false;
		if (fault == 3) {
			char path[MAXPGPATH];
			v2_claim_path(&op.claim.identity, path);
			UT_ASSERT_EQ(unlink(path), 0);
		}
		if (fault == 4)
			test_input_current = false;
		if (fault == 5)
			test_input_release = false;
		if (fault == 6)
			test_worker_pin_release = false;
		UT_ASSERT_EQ(thread_recovery_worker_run(&eligibility), fault == 2 || fault >= 5
																   ? CLUSTER_THREADREC_BLOCKED
																   : CLUSTER_THREADREC_DEFERRED);
		UT_ASSERT_EQ(test_worker_formations, fault == 1 || fault == 3 ? 0 : 1);
		UT_ASSERT_EQ(test_worker_pins, fault == 0 || fault >= 4 ? 1 : 0);
		UT_ASSERT_EQ(test_worker_initializer_ir, fault == 0 || fault >= 4 ? 1 : 0);
		UT_ASSERT_EQ(test_worker_normal_ir, 0);
		UT_ASSERT_EQ(test_worker_replays, 0);
		UT_ASSERT_EQ(test_worker_pin_held, fault >= 5);
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		if (fault == 0) {
			UT_ASSERT_EQ(
				cluster_control_root_read_recovery_subject(1, &op.claim.identity, &observed), 0);
			UT_ASSERT_EQ(observed.kind, CLUSTER_CONTROL_RECOVERY_PENDING_INITIALIZER);
			UT_ASSERT_EQ(memcmp(observed.pending.file.image_sha256, advanced.image_sha256, 32), 0);
		}
		test_reserve_mode = false;
	}
}

static ClusterWalStartupImage
pending_inspection_fixture(ClusterRecoverySerialGuard *serial, bool checkpoint)
{
	uint8 before[66048];
	ControlRootImage root;
	ControlFileData control;
	ClusterControlRootFileToken token, advanced;
	ClusterControlRecoverySubject subject;
	ClusterWalStartupImage op;
	if (checkpoint) {
		op = v3_startup_checkpoint_fixture(before, &root, &control);
		/* The older anchor-codec fixture leaves nextXid at zero. This test
		 * reads native checkpoint payloads, which require a normal nextXid. */
		control.checkPointCopy.nextXid = FullTransactionIdFromU64(103);
		v3_startup_native_record(&op, &control);
	} else {
		op = v3_begin_fixture(before, &root, &token);
		UT_ASSERT_EQ(cluster_control_root_v3_startup_begin_clean(
						 &op.claim.identity, op.operation_uuid, &token, &advanced),
					 0);
	}
	cluster_node_id = 127;
	test_self_incarnation = 991;
	UT_ASSERT_EQ(cluster_control_root_read_recovery_subject(1, &op.claim.identity, &subject), 0);
	memset(serial, 0, sizeof(*serial));
	serial->held = true;
	serial->mode = CLUSTER_RECOVERY_SERIAL_INITIALIZER;
	serial->duty = op.claim.identity;
	serial->pending = subject.pending;
	serial->formation = (const ClusterFormationWitnessV1 *)(uintptr_t)1;
	serial->fence_need_set = (const PgracExternalFenceNeedSetV1 *)(uintptr_t)2;
	serial->fence_admission_set = (const PgracExternalFenceAdmissionSetV1 *)(uintptr_t)3;
	test_failure_formation = test_failure_needs = test_failure_admissions = true;
	test_worker_pin_held = true;
	memset(&test_failure_need, 0, sizeof(test_failure_need));
	test_failure_need.system_identifier = op.claim.identity.system_identifier;
	test_failure_need.victim_node_id = op.claim.identity.origin_node_id;
	test_failure_need.victim_incarnation = op.claim.identity.origin_owner_incarnation;
	UT_ASSERT(cluster_recovery_duty_digest_for_claim(&op.claim.identity, true,
													 &test_failure_need.canonical_duty_digest));
	return op;
}

UT_TEST(test_runtime_pending_owner_reads_actual_empty_and_checkpoint_wal)
{
	for (unsigned scenario = 0; scenario < 3; scenario++) {
		ClusterRecoverySerialGuard serial;
		ClusterWalInitializerInput input;
		ClusterWalStartupImage op = pending_inspection_fixture(&serial, scenario != 0);
		if (ut_current_failed)
			return;
		if (scenario == 2) {
			char wal[MAXPGPATH];
			v2_checkpoint_wal_path(&op.claim.identity, op.first_segment_lsn, op.timeline, wal);
			UT_ASSERT_EQ(unlink(wal), 0);
		}
		if (scenario == 2) {
			UT_ASSERT(cluster_control_root_v3_initializer_observe(
						  &serial, (ClusterWalRetentionPin *)(uintptr_t)4, &input)
					  != 0);
			UT_ASSERT(v2_zero(&input, sizeof(input)));
		} else {
			UT_ASSERT_EQ(cluster_control_root_v3_initializer_observe(
							 &serial, (ClusterWalRetentionPin *)(uintptr_t)4, &input),
						 0);
			UT_ASSERT_EQ(input.startup.phase, CLUSTER_WAL_STARTUP_INITIALIZING);
			UT_ASSERT_EQ(input.observation.checkpoint_records, scenario);
			UT_ASSERT_EQ(input.observation.tail.records, scenario);
			UT_ASSERT_EQ(input.observation.unsupported_records, 0);
			if (scenario != 0) {
				UT_ASSERT_EQ(input.observation.checkpoint_start,
							 op.first_segment_lsn + SizeOfXLogLongPHD);
				UT_ASSERT(input.observation.checkpoint_start
						  != op.predecessor.snapshot.checkpoint_lower_lsn);
			}
		}
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		UT_ASSERT(serial.held && test_worker_pin_held);
		test_reserve_mode = false;
	}
}

UT_TEST(test_runtime_pending_inspection_refuses_stale_or_unowned_input)
{
	for (unsigned fault = 0; fault < 12; fault++) {
		ClusterRecoverySerialGuard serial;
		ClusterWalInitializerInput input;
		ClusterWalStartupImage op = pending_inspection_fixture(&serial, false);
		if (ut_current_failed)
			return;
		switch (fault) {
		case 0:
			serial.pending.operation_uuid[0] ^= 1;
			break;
		case 1:
			serial.pending.sha256[0] ^= 1;
			break;
		case 2:
			serial.pending.file.image_sha256[0] ^= 1;
			break;
		case 3:
			serial.release_uncertain = true;
			break;
		case 4:
			serial.mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
			break;
		case 5:
			test_worker_pin_held = false;
			break;
		case 6:
			test_failure_admissions = false;
			break;
		case 7:
			test_failure_need.victim_incarnation++;
			break;
		case 8:
			cluster_node_id = op.claim.identity.origin_node_id;
			test_self_incarnation = op.claim.identity.origin_owner_incarnation;
			break;
		case 9:
			test_stop_share_hook = v2_checkpoint_root_race;
			test_stop_share_call = test_cf_lock_calls + 2;
			break;
		case 10:
			test_failure_formation = false;
			break;
		case 11:
			test_cf_release_confirmed = false;
			break;
		}
		UT_ASSERT(cluster_control_root_v3_initializer_observe(
					  &serial, (ClusterWalRetentionPin *)(uintptr_t)4, &input)
				  != 0);
		UT_ASSERT(v2_zero(&input, sizeof(input)));
		UT_ASSERT(serial.held);
		if (fault != 11)
			UT_ASSERT_EQ(test_actual_cf, NoLock);
		test_reserve_mode = false;
	}
}

UT_TEST(test_runtime_v3_lmon_launch_continues_only_after_exact_cf_retirement)
{
	uint8 before[66048];
	ClusterRecoverySerialRequest request;
	ClusterThreadRecLaunchEligibility eligibility;
	int locks;

	v2_failure_fixture(before, &request, false);
	runtime_fixture_version3(before);
	memset(&test_worker_event, 0, sizeof(test_worker_event));
	test_worker_event.reconfig_kind = RECONFIG_KIND_FAIL_STOP;
	test_worker_event.event_id = 12;
	test_worker_event.new_epoch = 123;
	test_worker_event.dead_bitmap[0] = 1;
	test_control_barrier_ready = true;
	test_launch_fixture = true;
	test_launch_stamps = test_launch_registered = test_launch_legacy_pins = 0;
	test_bit22_latch_active = true;
	MyBackendType = B_LMON;
	test_cf_release_confirmed = false;
	UT_ASSERT(!cluster_reconfig_thread_recovery_eligibility_consume(1, &eligibility));
	UT_ASSERT_EQ(eligibility.attempt_stamp, 0);
	UT_ASSERT_EQ(test_launch_stamps, 0);
	locks = test_cf_lock_calls;
	test_cf_release_confirmed = true;
	UT_ASSERT(cluster_reconfig_thread_recovery_eligibility_consume(1, &eligibility));
	UT_ASSERT_EQ(test_cf_lock_calls, locks);
	thread_recovery_launch_one(&eligibility);
	UT_ASSERT_EQ(test_launch_stamps, 1);
	UT_ASSERT_EQ(test_launch_registered, 1);
	UT_ASSERT_EQ(test_launch_legacy_pins, 0);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	memset(thread_recovery_owned, 0, sizeof(thread_recovery_owned));
	test_launch_fixture = false;
	test_bit22_latch_active = false;
	MyBackendType = B_INVALID;
	cluster_shared_config = false;
}

UT_TEST(test_runtime_v3_failure_worker_seals_then_acquires_fresh_replay_owners)
{
	uint8 before[66048];
	ClusterRecoverySerialRequest request;
	ClusterThreadRecLaunchEligibility eligibility;
	ClusterControlRootSnapshot out;
	ClusterControlRootReadToken token;
	v2_failure_fixture(before, &request, false);
	runtime_fixture_version3(before);
	memset(&eligibility, 0, sizeof(eligibility));
	eligibility.origin_thread = 1;
	eligibility.attempt_stamp = 123;
	eligibility.duty = request.duty;
	UT_ASSERT_EQ(thread_recovery_worker_run(&eligibility), CLUSTER_THREADREC_DEFERRED);
	UT_ASSERT_EQ(test_input_acquires, 1);
	UT_ASSERT_EQ(test_input_releases, 1);
	UT_ASSERT_EQ(test_worker_pins, 1);
	UT_ASSERT_EQ(test_worker_normal_ir, 1);
	UT_ASSERT_EQ(test_worker_replays, 1);
	UT_ASSERT(!test_worker_pin_held);
	UT_ASSERT_EQ(cluster_control_root_read_canonical(
					 1, &request.duty, CLUSTER_CONTROL_ROOT_READ_STRONG, &out, &token),
				 0);
	UT_ASSERT_EQ(out.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED);
	UT_ASSERT_EQ(out.validated_tail_lsn_exclusive, test_checkpoint_end);
	/* A second worker resumes sealed input without republishing its tail. */
	UT_ASSERT_EQ(thread_recovery_worker_run(&eligibility), CLUSTER_THREADREC_DEFERRED);
	UT_ASSERT_EQ(test_input_acquires, 1);
	UT_ASSERT_EQ(test_worker_normal_ir, 2);
	UT_ASSERT_EQ(test_worker_replays, 2);
	cluster_shared_config = false;
}

UT_TEST(test_runtime_v3_worker_window_consumes_its_sealed_authority_not_legacy_projection)
{
	uint8 before[66048];
	ClusterRecoverySerialRequest request;
	ClusterThreadRecLaunchEligibility eligibility = { 0 };

	v2_failure_fixture(before, &request, false);
	runtime_fixture_version3(before);
	eligibility.origin_thread = 1;
	eligibility.attempt_stamp = 123;
	eligibility.duty = request.duty;
	test_worker_window_consumer = true;
	test_legacy_projection_reads = 0;
	UT_ASSERT_EQ(thread_recovery_worker_run(&eligibility), CLUSTER_THREADREC_DEFERRED);
	UT_ASSERT_EQ(test_worker_replays, 1);
	UT_ASSERT_EQ(test_legacy_projection_reads, 0);
	test_worker_window_consumer = false;
	cluster_shared_config = false;
}

UT_TEST(test_v2_failure_tail_publishes_real_input_not_terminal)
{
	uint8 before[66048];
	ClusterRecoverySerialRequest request;
	ClusterControlRootSnapshot out;
	ClusterControlRootReadToken token;

	v2_failure_seal_fixture(before, &request);
	UT_ASSERT_EQ(cluster_control_root_v2_failure_tail_publish(&request, &out, &token), 0);
	UT_ASSERT_EQ(out.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED);
	UT_ASSERT_EQ(out.validated_tail_lsn_exclusive, test_checkpoint_end);
	UT_ASSERT_EQ(out.tail_last_record_crc32c, test_checkpoint_crc);
	UT_ASSERT_EQ(out.recovered_through_lsn_exclusive, out.checkpoint_lower_lsn);
	UT_ASSERT_EQ(out.root_publish_seq, request.expected_root_token.root_publish_seq + 1);
	UT_ASSERT_EQ(out.lifecycle_reason, CLUSTER_CONTROL_ROOT_PUBLISH_FAILURE_TAIL_VALIDATED);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
	UT_ASSERT(test_walr_begin_order < test_input_acquire_order);
	UT_ASSERT(test_input_acquire_order < test_cf_acquire_order);
	UT_ASSERT(test_last_rename_order < test_cf_release_order);
	UT_ASSERT(test_cf_release_order < test_input_release_order);
	UT_ASSERT(test_input_release_order < test_order_seq);
	cluster_shared_config = false;
}

UT_TEST(test_runtime_v3_failure_worker_entry_establishes_resource_owner)
{
	uint8 before[66048];
	ClusterRecoverySerialRequest request;
	ClusterThreadRecLaunchEligibility eligibility;
	ClusterControlRootSnapshot out;
	ClusterControlRootReadToken token;
	BackgroundWorker worker;

	v2_failure_fixture(before, &request, false);
	runtime_fixture_version3(before);
	memset(&eligibility, 0, sizeof(eligibility));
	eligibility.origin_thread = 1;
	eligibility.attempt_stamp = 123;
	eligibility.duty = request.duty;
	memset(&worker, 0, sizeof(worker));
	memcpy(worker.bgw_extra, &eligibility, sizeof(eligibility));
	MyBgworkerEntry = &worker;
	CurrentResourceOwner = NULL;
	/* Invalid launches cannot enter publication or manufacture an owner. */
	cluster_thread_recovery_worker_main(Int32GetDatum(0));
	UT_ASSERT_NULL(CurrentResourceOwner);
	v2_assert_primary_unchanged(before);
	cluster_thread_recovery_worker_main(Int32GetDatum(1));
	UT_ASSERT_NOT_NULL(CurrentResourceOwner);
	UT_ASSERT_EQ(test_worker_replays, 1);
	UT_ASSERT_EQ(cluster_control_root_read_canonical(
					 1, &request.duty, CLUSTER_CONTROL_ROOT_READ_STRONG, &out, &token),
				 0);
	UT_ASSERT_EQ(out.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED);
	UT_ASSERT_EQ(out.validated_tail_lsn_exclusive, test_checkpoint_end);
	UT_ASSERT(!test_worker_pin_held);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	CurrentResourceOwner = (ResourceOwner)(uintptr_t)1;
	MyBgworkerEntry = NULL;
	cluster_shared_config = false;
}

UT_TEST(test_v2_failure_input_preserves_fpw_was_off_history)
{
	uint8 before[66048];
	ControlRootImage root;
	ClusterRecoverySerialRequest request;
	ClusterControlRootSnapshot out;
	ClusterControlRootReadToken token;

	v2_failure_fixture(before, &request, false);
	UT_ASSERT_EQ(cluster_control_root_v2_decode(before, sizeof(before), request.duty.storage_uuid,
												request.duty.system_identifier, &root),
				 0);
	root.records[0].root_flags |= CLUSTER_CONTROL_ROOT_FLAG_FPW_WAS_OFF;
	UT_ASSERT_EQ(cluster_control_root_v2_encode(&root), 0);
	v2_write_roots(root.bytes);
	UT_ASSERT_EQ(cluster_control_root_v2_read_canonical(1, &request.duty, &out,
														&request.expected_root_token),
				 0);
	UT_ASSERT_EQ(cluster_control_root_v2_failure_open_publish(&request, &out, &token), 0);
	UT_ASSERT((out.root_flags & CLUSTER_CONTROL_ROOT_FLAG_FPW_WAS_OFF) != 0);
	request.expected_root_token = token;
	UT_ASSERT_EQ(cluster_control_root_v2_failure_tail_publish(&request, &out, &token), 0);
	UT_ASSERT((out.root_flags & CLUSTER_CONTROL_ROOT_FLAG_FPW_WAS_OFF) != 0);
	UT_ASSERT_EQ(out.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED);
	UT_ASSERT_EQ(out.recovered_through_lsn_exclusive, out.checkpoint_lower_lsn);
	UT_ASSERT_EQ(out.validated_tail_lsn_exclusive, test_checkpoint_end);
	cluster_shared_config = false;
}

UT_TEST(test_v2_failure_open_refuses_unproven_owners)
{
	for (int i = 0; i < 10; i++) {
		uint8 before[66048];
		ClusterRecoverySerialRequest request;
		ClusterControlRootSnapshot out;
		ClusterControlRootReadToken token;
		v2_failure_fixture(before, &request, false);
		switch (i) {
		case 0:
			test_failure_formation = false;
			break;
		case 1:
			test_failure_needs = false;
			break;
		case 2:
			test_failure_admissions = false;
			break;
		case 3:
			test_failure_need.victim_incarnation++;
			break;
		case 4:
			test_failure_need.canonical_duty_digest.bytes[0] ^= 1;
			break;
		case 5:
			request.expected_root_token.file_txn_seq++;
			break;
		case 6:
			request.duty.origin_owner_incarnation++;
			break;
		case 7:
			cluster_node_id = request.duty.origin_node_id;
			test_self_incarnation = request.duty.origin_owner_incarnation;
			break;
		case 8:
			test_cf_grant = false;
			break;
		case 9:
			test_fail_primary_rename = true;
			break;
		}
		memset(&out, 0xa5, sizeof(out));
		memset(&token, 0xa5, sizeof(token));
		UT_ASSERT(cluster_control_root_v2_failure_open_publish(&request, &out, &token) != 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
		v2_assert_primary_unchanged(before);
		cluster_shared_config = false;
	}
}

UT_TEST(test_v2_failure_tail_refuses_unproven_or_borrowed_owners)
{
	for (int i = 0; i < 10; i++) {
		uint8 before[66048];
		ClusterRecoverySerialRequest request;
		ClusterControlRootSnapshot out;
		ClusterControlRootReadToken token;
		v2_failure_seal_fixture(before, &request);
		switch (i) {
		case 0:
			test_failure_formation = false;
			break;
		case 1:
			test_failure_admissions = false;
			break;
		case 2:
			test_input_acquire = CLUSTER_RECOVERY_SERIAL_BUSY;
			break;
		case 3:
			test_input_current = false;
			break;
		case 4:
			request.expected_root_token.file_txn_seq++;
			break;
		case 5:
			test_cf_clusterwide = false;
			break;
		case 6:
			test_walr_begin_result = CLUSTER_WAL_PIN_UNAVAILABLE;
			break;
		case 7:
			request.mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
			break;
		case 8:
			enableFsync = false;
			break;
		case 9:
			test_checkpoint_outer_cf = true;
			test_cf_mode = test_actual_cf = ShareLock;
			break;
		}
		UT_ASSERT(cluster_control_root_v2_failure_tail_publish(&request, &out, &token) != 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
		UT_ASSERT_EQ(test_actual_cf, i == 9 ? ShareLock : NoLock);
		v2_assert_primary_unchanged(before);
		enableFsync = true;
		cluster_shared_config = false;
	}
}

static int test_failure_late_change;
static void
v2_failure_late_change(void)
{
	uint8 bytes[66048];
	char primary[MAXPGPATH];
	if (test_failure_late_change == 0)
		test_failure_admissions = false;
	else if (test_failure_late_change == 1)
		test_input_current = false;
	else if (test_failure_late_change == 2) {
		test_checkpoint_prefix.sequence++;
		v2_checkpoint_prefix_write();
	} else {
		path_for(primary, sizeof(primary), CLUSTER_CONTROL_ROOT_REL_PATH);
		read_all_or_abort(primary, bytes, sizeof(bytes));
		/* Another publisher changed the common sequence while CF was free. */
		bytes[16]++;
		v2_checksums(bytes);
		v2_write_roots(bytes);
	}
}

UT_TEST(test_v2_failure_tail_rechecks_root_fence_serial_and_promise)
{
	for (int i = 0; i < 4; i++) {
		uint8 before[66048];
		ClusterRecoverySerialRequest request;
		ClusterControlRootSnapshot out;
		ClusterControlRootReadToken token;
		v2_failure_seal_fixture(before, &request);
		test_failure_late_change = i;
		test_checkpoint_x_hook = v2_failure_late_change;
		UT_ASSERT(cluster_control_root_v2_failure_tail_publish(&request, &out, &token) != 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
		if (i == 3) {
			before[16]++;
			v2_checksums(before);
		}
		v2_assert_primary_unchanged(before);
		UT_ASSERT_EQ(test_input_acquires, test_input_releases);
		UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
		test_checkpoint_x_hook = NULL;
		cluster_shared_config = false;
	}
}

UT_TEST(test_v2_failure_tail_requires_real_promised_input)
{
	for (int i = 0; i < 4; i++) {
		uint8 before[66048];
		char wal[MAXPGPATH];
		ClusterRecoverySerialRequest request;
		ClusterControlRootSnapshot out;
		ClusterControlRootReadToken token;
		ClusterControlRootResult result;
		v2_failure_seal_fixture(before, &request);
		v2_checkpoint_wal_path(&request.duty, test_checkpoint_end, 1, wal);
		if (i == 0)
			UT_ASSERT_EQ(unlink(test_checkpoint_prefix_path), 0);
		else if (i == 1)
			UT_ASSERT_EQ(unlink(wal), 0);
		else if (i == 2) {
			/* Cut record payload, not the padding up to its aligned EndRecPtr. */
			UT_ASSERT(test_checkpoint_prefix.record_start % XLOG_BLCKSZ + SizeOfXLogRecord + 2
						  + sizeof(CheckPoint)
					  < XLOG_BLCKSZ);
			UT_ASSERT_EQ(truncate(wal, test_checkpoint_prefix.record_start % wal_segment_size
										   + SizeOfXLogRecord + 2 + sizeof(CheckPoint) - 1),
						 0);
		} else {
			test_checkpoint_prefix.record_crc ^= 1;
			v2_checkpoint_prefix_write();
		}
		result = cluster_control_root_v2_failure_tail_publish(&request, &out, &token);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			printf("# promised-input fault %d accepted; record=" UINT64_FORMAT
				   "+%zu end=" UINT64_FORMAT "\n",
				   i, test_checkpoint_prefix.record_start,
				   (size_t)(SizeOfXLogRecord + 2 + sizeof(CheckPoint)), test_checkpoint_end);
		UT_ASSERT(result != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
		v2_assert_primary_unchanged(before);
		UT_ASSERT_EQ(test_input_acquires, test_input_releases);
		UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
		cluster_shared_config = false;
	}
}

static bool
v2_failure_tail_catches_error(const ClusterRecoverySerialRequest *request,
							  ClusterControlRootSnapshot *out, ClusterControlRootReadToken *token)
{
	volatile bool caught = false;

	PG_TRY();
	{
		(void)cluster_control_root_v2_failure_tail_publish(request, out, token);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	return caught;
}

UT_TEST(test_v2_failure_tail_cancellation_and_uncertain_cleanup)
{
	for (int i = 0; i < 5; i++) {
		uint8 before[66048];
		ClusterRecoverySerialRequest request;
		static ClusterControlRootSnapshot out;
		static ClusterControlRootReadToken token;
		v2_failure_seal_fixture(before, &request);
		if (i == 0)
			test_throw_root_read = true;
		else
			InterruptPending = true;
		if (i == 2)
			test_cf_release_confirmed = false;
		else if (i == 3)
			test_input_release = false;
		else if (i == 4)
			test_walr_end_result = CLUSTER_WALR_RELEASE_UNCONFIRMED;
		test_capture_error_level = true;
		test_last_error_level = 0;
		UT_ASSERT(v2_failure_tail_catches_error(&request, &out, &token));
		if (i >= 2)
			UT_ASSERT_EQ(test_last_error_level, FATAL);
		UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
		v2_assert_primary_unchanged(before);
		UT_ASSERT_EQ(test_input_acquires, test_input_releases);
		UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
		test_capture_error_level = test_throw_root_read = false;
		InterruptPending = false;
		cluster_shared_config = false;
	}
}

UT_TEST(test_v2_failure_worker_without_fence_never_changes_root)
{
	uint8 before[66048];
	ClusterRecoverySerialRequest request;
	ClusterThreadRecLaunchEligibility eligibility;
	v2_failure_fixture(before, &request, false);
	memset(&eligibility, 0, sizeof(eligibility));
	eligibility.origin_thread = 1;
	eligibility.attempt_stamp = 123;
	eligibility.duty = request.duty;
	test_failure_admissions = false;
	UT_ASSERT_EQ(thread_recovery_worker_run(&eligibility), CLUSTER_THREADREC_BLOCKED);
	UT_ASSERT_EQ(test_worker_replays, 0);
	UT_ASSERT_EQ(test_input_acquires, 0);
	v2_assert_primary_unchanged(before);
	cluster_shared_config = false;
}

static int test_failure_release_fault;
static void
v2_failure_release_fault(void)
{
	if (test_failure_release_fault == 0)
		test_cf_release_confirmed = false;
	else if (test_failure_release_fault == 1)
		test_input_release = false;
	else
		test_walr_end_result = CLUSTER_WALR_RELEASE_UNCONFIRMED;
}

UT_TEST(test_v2_failure_sealed_tail_release_uncertainty_never_returns_authority)
{
	for (int i = 0; i < 3; i++) {
		uint8 before[66048];
		char primary[MAXPGPATH];
		ControlRootImage root;
		ClusterRecoverySerialRequest request;
		static ClusterControlRootSnapshot out;
		static ClusterControlRootReadToken token;
		v2_failure_seal_fixture(before, &request);
		test_failure_release_fault = i;
		test_checkpoint_x_hook = v2_failure_release_fault;
		test_capture_error_level = true;
		test_last_error_level = 0;
		UT_ASSERT(v2_failure_tail_catches_error(&request, &out, &token));
		UT_ASSERT_EQ(test_last_error_level, FATAL);
		UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
		path_for(primary, sizeof(primary), CLUSTER_CONTROL_ROOT_REL_PATH);
		read_all_or_abort(primary, root.bytes, sizeof(root.bytes));
		UT_ASSERT_EQ(cluster_control_root_v2_decode(root.bytes, sizeof(root.bytes),
													request.duty.storage_uuid,
													request.duty.system_identifier, &root),
					 0);
		UT_ASSERT_EQ(root.records[0].validated_tail_lsn_exclusive, test_checkpoint_end);
		UT_ASSERT_EQ(root.records[0].lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED);
		UT_ASSERT_EQ(root.header.v2.serving[0] & UINT64_C(1), 0);
		UT_ASSERT_EQ(test_input_acquires, test_input_releases);
		UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
		test_checkpoint_x_hook = NULL;
		test_capture_error_level = false;
		cluster_shared_config = false;
	}
}

UT_TEST(test_runtime_v3_canonical_foreign_absent_and_cf_refusals)
{
	uint8 bytes[66048];
	ClusterControlRootIdentity self, foreign;
	ControlFileData candidate;
	ClusterControlRootSnapshot out;
	ClusterControlRootReadToken token;
	int i;

	for (i = 0; i < 5; i++) {
		v2_retention_fixture(bytes, &self, &candidate);
		runtime_fixture_version3(bytes);
		foreign = self;
		if (i == 0)
			foreign.origin_owner_incarnation++;
		else if (i == 1)
			test_cf_grant = false;
		else if (i == 2)
			test_cf_clusterwide = false;
		else if (i == 3) {
			test_checkpoint_outer_cf = true;
			test_cf_mode = ShareLock;
			test_actual_cf = ShareLock;
		}
		memset(&out, 0x5a, sizeof(out));
		memset(&token, 0x5a, sizeof(token));
		if (i == 4)
			UT_ASSERT_EQ(cluster_control_root_read_canonical_discovered(2, &out, &token),
						 CLUSTER_CONTROL_ROOT_ABSENT);
		else
			UT_ASSERT_EQ(cluster_control_root_read_canonical(self.origin_thread_id, &foreign,
															 CLUSTER_CONTROL_ROOT_READ_STRONG, &out,
															 &token),
						 i == 0 ? CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH
								: CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
		UT_ASSERT(v2_zero(&token, sizeof(token)));
		UT_ASSERT_EQ(test_actual_cf, i == 3 ? ShareLock : NoLock);
		if (i == 3)
			UT_ASSERT_EQ(test_cf_lock_calls, 0);
		v2_assert_primary_unchanged(bytes);
		cluster_shared_config = false;
	}
}

UT_TEST(test_runtime_v3_canonical_error_releases_owned_lock)
{
	uint8 bytes[66048];
	ClusterControlRootIdentity self;
	ControlFileData candidate;
	static ClusterControlRootSnapshot out;
	static ClusterControlRootReadToken token;
	volatile bool caught = false;
	v2_retention_fixture(bytes, &self, &candidate);
	runtime_fixture_version3(bytes);
	test_throw_root_read = true;
	PG_TRY();
	{
		(void)cluster_control_root_read_canonical(self.origin_thread_id, &self,
												  CLUSTER_CONTROL_ROOT_READ_STRONG, &out, &token);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	test_throw_root_read = false;
	UT_ASSERT(caught);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	UT_ASSERT(v2_zero(&token, sizeof(token)));
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	cluster_shared_config = false;
}

UT_TEST(test_runtime_v3_canonical_error_unconfirmed_cleanup_is_fatal)
{
	uint8 bytes[66048];
	ClusterControlRootIdentity self;
	ControlFileData candidate;
	static ClusterControlRootSnapshot out;
	static ClusterControlRootReadToken token;
	volatile bool caught = false;

	v2_retention_fixture(bytes, &self, &candidate);
	runtime_fixture_version3(bytes);
	test_throw_root_read = true;
	test_cf_release_confirmed = false;
	test_capture_error_level = true;
	test_last_error_level = 0;
	PG_TRY();
	{
		(void)cluster_control_root_read_canonical_discovered(self.origin_thread_id, &out, &token);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	test_throw_root_read = false;
	test_capture_error_level = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(test_last_error_level, FATAL);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	UT_ASSERT(v2_zero(&token, sizeof(token)));
	UT_ASSERT_EQ(test_actual_cf, ShareLock);
	cluster_shared_config = false;
}

UT_TEST(test_v2_retention_reader_owns_exact_live_thread_and_cf)
{
	uint8 bytes[66048];
	ClusterControlRootIdentity self;
	ControlFileData candidate;
	ClusterControlRootSnapshot out;
	ClusterControlRootReadToken token;
	int latch_calls;

	v2_retention_fixture(bytes, &self, &candidate);
	latch_calls = test_bit22_latch_apply_calls;
	UT_ASSERT_EQ(cluster_control_root_v2_read_retention_current(&self, &out, &token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(memcmp(&out.identity, &self, sizeof(self)), 0);
	UT_ASSERT_EQ(out.checkpoint_lower_lsn, candidate.checkPointCopy.redo - 8192);
	UT_ASSERT_EQ(out.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN);
	UT_ASSERT_EQ(token.origin_thread_id, self.origin_thread_id);
	UT_ASSERT_EQ(token.lifecycle, out.lifecycle);
	UT_ASSERT(token.file_txn_seq != 0);
	UT_ASSERT_EQ(test_cf_lock_calls, 1);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	UT_ASSERT_EQ(test_bit22_latch_apply_calls, latch_calls);
	cluster_shared_config = false;
}

UT_TEST(test_v2_retention_refusals_clear_all_outputs)
{
	for (int fault = 0; fault < 12; ++fault) {
		uint8 bytes[66048];
		ClusterControlRootIdentity self;
		ControlFileData candidate;
		ClusterControlRootSnapshot out;
		ClusterControlRootReadToken token;
		char path[MAXPGPATH];
		volatile ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
		volatile bool caught = false;

		v2_retention_fixture(bytes, &self, &candidate);
		switch (fault) {
		case 0:
			self.origin_owner_incarnation++;
			break;
		case 1:
			test_member_state = CLUSTER_MEMBER_DEAD;
			break;
		case 2:
			test_change_epoch_read = 3;
			break;
		case 3:
			test_serving = false;
			break;
		case 4:
			test_fence = false;
			break;
		case 5:
			test_cf_clusterwide = false;
			break;
		case 6:
			test_cf_release_confirmed = false;
			break;
		case 7:
			v2_claim_path(&self, path);
			UT_ASSERT_EQ(unlink(path), 0);
			break;
		case 8:
			test_checkpoint_outer_cf = true;
			break;
		case 9:
			put_u32_le(bytes + 196, CLUSTER_CONTROL_ROOT_DATABASE_CLOSED);
			break;
		case 10:
			put_u64_le(bytes + 232, 0);
			break;
		case 11:
			bytes[512 + 10] = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
			break;
		}
		if (fault >= 9) {
			v2_checksums(bytes);
			v2_write_roots(bytes);
		}
		memset(&out, 0x5a, sizeof(out));
		memset(&token, 0x5a, sizeof(token));
		test_capture_error_level = fault == 6;
		test_last_error_level = 0;
		PG_TRY();
		{
			result = cluster_control_root_v2_read_retention_current(&self, &out, &token);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		test_capture_error_level = false;
		UT_ASSERT(result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
				  && result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
		UT_ASSERT(v2_zero(&token, sizeof(token)));
		if (fault == 6) {
			UT_ASSERT(caught);
			UT_ASSERT_EQ(test_last_error_level, FATAL);
			UT_ASSERT_EQ(test_actual_cf, ShareLock);
		} else {
			UT_ASSERT(!caught);
			UT_ASSERT_EQ(test_actual_cf, NoLock);
		}
		cluster_shared_config = false;
	}
}

UT_TEST(test_v2_retention_exception_releases_owned_cf)
{
	uint8 bytes[66048];
	ClusterControlRootIdentity self;
	ControlFileData candidate;
	ClusterControlRootSnapshot out;
	ClusterControlRootReadToken token;
	volatile bool caught = false;

	v2_retention_fixture(bytes, &self, &candidate);
	test_throw_epoch_read = 3; /* Post-read revalidation, with CF-S owned. */
	memset(&out, 0x5a, sizeof(out));
	memset(&token, 0x5a, sizeof(token));
	PG_TRY();
	{
		(void)cluster_control_root_v2_read_retention_current(&self, &out, &token);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	test_throw_epoch_read = 0;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(test_cf_lock_calls, 1);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	UT_ASSERT(v2_zero(&token, sizeof(token)));
	cluster_shared_config = false;
}

UT_TEST(test_v2_retention_exception_unconfirmed_release_is_fatal)
{
	uint8 bytes[66048];
	ClusterControlRootIdentity self;
	ControlFileData candidate;
	ClusterControlRootSnapshot out;
	ClusterControlRootReadToken token;
	volatile bool caught = false;

	v2_retention_fixture(bytes, &self, &candidate);
	test_throw_epoch_read = 3;
	test_cf_release_confirmed = false;
	test_capture_error_level = true;
	test_last_error_level = 0;
	PG_TRY();
	{
		(void)cluster_control_root_v2_read_retention_current(&self, &out, &token);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	test_capture_error_level = false;
	test_throw_epoch_read = 0;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(test_last_error_level, FATAL);
	UT_ASSERT_EQ(test_actual_cf, ShareLock);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	UT_ASSERT(v2_zero(&token, sizeof(token)));
	cluster_shared_config = false;
}

UT_TEST(test_runtime_v3_runtime_native_reader_selects_own_thread)
{
	uint8 bytes[66048];
	ClusterControlRootIdentity self;
	ControlFileData candidate, out;

	v2_runtime_fixture(bytes, &self, &candidate);
	runtime_fixture_version3(bytes);
	UT_ASSERT(cluster_cf_authority_read(&out));
	UT_ASSERT_EQ(out.checkPoint, candidate.checkPoint - 8192);
	UT_ASSERT_EQ(out.checkPointCopy.nextOid, 60001);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
	cluster_node_id = 127;
	test_self_incarnation = test_membership_incarnation = 226;
	test_own_thread = 128;
	UT_ASSERT(cluster_cf_authority_read(&out));
	UT_ASSERT_EQ(out.checkPoint, UINT64_C(0x1000000) + 127 * 4096 + 128);
	UT_ASSERT_EQ(out.checkPointCopy.nextOid, 60001);
	cluster_shared_config = false;
}

UT_TEST(test_runtime_v3_runtime_reader_never_uses_projection_for_bad_facts)
{
	for (int fault = 0; fault < 16; ++fault) {
		uint8 bytes[66048];
		ClusterControlRootIdentity self;
		ControlFileData candidate, out;
		char path[MAXPGPATH];

		v2_runtime_fixture(bytes, &self, &candidate);
		runtime_fixture_version3(bytes);
		switch (fault) {
		case 0:
			test_cf_clusterwide = false;
			break;
		case 1:
			test_self_incarnation++;
			break;
		case 2:
			test_membership_incarnation++;
			break;
		case 3:
			test_member_state = CLUSTER_MEMBER_DEAD;
			break;
		case 4:
			test_serving = false;
			break;
		case 5:
			test_fence = false;
			break;
		case 6:
			test_prebump = true;
			break;
		case 7:
			test_wal_validated = false;
			break;
		case 8:
			test_own_thread = 2;
			break;
		case 9:
			test_change_epoch_read = 3;
			break;
		case 10:
			v2_claim_path(&self, path);
			UT_ASSERT_EQ(unlink(path), 0);
			break;
		case 11:
			cluster_controlfile_shared_authority = false;
			break;
		case 12:
			put_u32_le(bytes + 196, CLUSTER_CONTROL_ROOT_DATABASE_CLOSED);
			break;
		case 13:
			put_u64_le(bytes + 232, 0);
			break;
		case 14:
			bytes[512 + 10] = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
			break;
		case 15:
			put_u32_le(bytes + 76, CLUSTER_CONTROL_ROOT_ACTIVATION_PREPARED);
			break;
		}
		if (fault >= 12) {
			ControlRootImage root;

			v2_checksums(bytes);
			UT_ASSERT_EQ(
				cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &root),
				0);
			v2_write_roots(bytes);
		}
		out = candidate;
		UT_ASSERT(!cluster_cf_authority_read(&out));
		UT_ASSERT_EQ(memcmp(&out, &candidate, sizeof(out)), 0);
		/* Private decoder scratch clears on failure; native shared output is
		 * untouched, not treated as a successful fallback or new authority.
		 */
		test_epoch_reads = 0;
		if (fault == 9)
			--test_epoch;
		UT_ASSERT(cluster_control_root_v3_read_runtime_local_locked(&out)
				  != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
		cluster_shared_config = false;
	}
}

UT_TEST(test_runtime_v3_runtime_native_inplace_identity_is_never_cleared)
{
	uint8 bytes[66048];
	ClusterControlRootIdentity self;
	ControlFileData candidate, before;

	v2_runtime_fixture(bytes, &self, &candidate);
	runtime_fixture_version3(bytes);
	/* Actual xlog.c shape: CF read's output is also GetSystemIdentifier's
	 * backing storage. It must never be zeroed while validating a new view.
	 */
	test_sysid_control = &candidate;
	UT_ASSERT(cluster_cf_authority_read(&candidate));
	UT_ASSERT_EQ(candidate.system_identifier, TEST_SYSID);
	UT_ASSERT_EQ(candidate.checkPoint, UINT64_C(0x1000000) + 128);
	before = candidate;
	test_fence = false;
	UT_ASSERT(!cluster_cf_authority_read(&candidate));
	UT_ASSERT_EQ(memcmp(&candidate, &before, sizeof(candidate)), 0);
	test_sysid_control = NULL;
	cluster_shared_config = false;
}

UT_TEST(test_v2_view_requires_exact_config_object)
{
	uint8 bytes[66048];
	ControlFileData native, out;
	ClusterCfImageStage stage;
	ControlRootImage root;
	ClusterControlRootFileToken token;
	char object[MAXPGPATH], path[MAXPGPATH], hex[65];

	for (int n = 0; n < 3; ++n) {
		v2_view_fixture(bytes, &native, &stage);
		for (int i = 0; i < 32; ++i)
			snprintf(hex + 2 * i, 3, "%02x", bytes[256 + i]);
		snprintf(object, sizeof(object), "%s/global/config_images/47-%s.conf", test_root, hex);
		if (n == 0)
			UT_ASSERT_EQ(unlink(object), 0);
		else if (n == 1)
			write_all_or_abort(object, (const uint8 *)"corrupt", 7);
		else {
			/* Even an exact hash cannot substitute a foreign identity. */
			v2_install_config(bytes, true);
			path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
			write_all_or_abort(path, bytes, sizeof(bytes));
			path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_BAK_REL_PATH);
			write_all_or_abort(path, bytes, sizeof(bytes));
		}
		memset(&root, 0xa5, sizeof(root));
		memset(&out, 0xa5, sizeof(out));
		memset(&token, 0xa5, sizeof(token));
		UT_ASSERT(
			cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &token)
			!= 0);
		UT_ASSERT(v2_outputs_zero(&root, &out, &token));
	}
}

/* PGRAC: exact byte composition cannot turn a valid individual object into
 * startup permission. All input bytes below came from the real codecs/files.
 * Author: SqlRush <sqlrush@gmail.com>
 */
typedef struct BootstrapFixture {
	ClusterControlBootstrapInput input;
	uint8 binding[256];
	uint8 before[66048];
	uint8 after[66048];
	uint8 common[PG_CONTROL_FILE_SIZE];
	uint8 config[4096];
	uint8 claim[112];
	uint8 anchor[512];
	ClusterRecoveryAnchorV2 local_anchor;
	ClusterRecoveryAnchorV2 current_anchors[2];
} BootstrapFixture;

static void
bootstrap_hex(const uint8 hash[32], char hex[65])
{
	for (int i = 0; i < 32; i++)
		snprintf(hex + 2 * i, 3, "%02x", hash[i]);
}

static void
bootstrap_fixture(BootstrapFixture *f, int node)
{
	ClusterRecoveryAnchorV2 anchors[2];
	ControlRootImage root;
	PgracControlBinding binding;
	char path[MAXPGPATH], hex[65];
	struct stat st;

	memset(f, 0, sizeof(*f));
	v2_thread_fixture(f->before, anchors);
	/* This baseline has no retained writers; history fixtures install real
	 * selected objects rather than relying on the codec-only placeholder. */
	memset(f->before + 512 + 127 * 512 + 216, 0, 40);
	v2_checksums(f->before);
	v2_write_roots(f->before);
	memcpy(f->current_anchors, anchors, sizeof(anchors));
	if (cluster_control_root_v2_decode(f->before, sizeof(f->before), v2_storage, TEST_SYSID, &root)
		!= 0)
		abort();
	f->local_anchor = anchors[node == 0 ? 0 : 1];
	memset(&binding, 0, sizeof(binding));
	binding.system_identifier = TEST_SYSID;
	memcpy(binding.storage_uuid, v2_storage, 16);
	memcpy(binding.authority_uuid, root.header.authority_uuid, 16);
	binding.database_incarnation = 41;
	binding.node_id = node;
	memset(binding.operation_uuid, 0x41, 16);
	memset(binding.source_cold_sha256, 0x42, 32);
	memset(binding.target_qualification_sha256, 0x43, 32);
	memcpy(binding.migration_round_sha256, root.header.migration_round_sha256, 32);
	memcpy(binding.source_wal_state_sha256, root.header.source_wal_state_sha256, 32);
	binding.migration_prepare_generation = 3;
	binding.migration_transition_epoch = 4;
	if (!pgrac_control_binding_encode(&binding, f->binding, sizeof(f->binding)))
		abort();

	bootstrap_hex(root.header.v2.control_image_sha256, hex);
	snprintf(path, sizeof(path), "%s/global/control_images/53-%s.bin", test_root, hex);
	read_all_or_abort(path, f->common, sizeof(f->common));
	bootstrap_hex(root.header.v2.config_sha256, hex);
	snprintf(path, sizeof(path), "%s/global/config_images/47-%s.conf", test_root, hex);
	if (stat(path, &st) != 0 || st.st_size <= 0 || st.st_size > sizeof(f->config))
		abort();
	read_all_or_abort(path, f->config, st.st_size);
	f->input.config.data = f->config;
	f->input.config.len = st.st_size;
	v2_claim_path(&f->local_anchor.identity, path);
	read_all_or_abort(path, f->claim, sizeof(f->claim));
	bootstrap_hex(root.refs[node].anchor_sha256, hex);
	snprintf(path, sizeof(path),
			 "%s/global/anchor_images/thread_%u/generation_" UINT64_FORMAT "/anchor_" UINT64_FORMAT
			 "-%s.bin",
			 test_root, f->local_anchor.identity.origin_thread_id,
			 f->local_anchor.identity.origin_owner_incarnation, f->local_anchor.anchor_generation,
			 hex);
	read_all_or_abort(path, f->anchor, sizeof(f->anchor));
	memcpy(f->after, f->before, sizeof(f->after));
	f->input.node_id = node;
#define BOOT_INPUT(field, bytes)                                                                   \
	f->input.field.data = f->bytes;                                                                \
	f->input.field.len = sizeof(f->bytes)
	BOOT_INPUT(binding, binding);
	BOOT_INPUT(root_before, before);
	BOOT_INPUT(root_after, after);
	BOOT_INPUT(common, common);
	BOOT_INPUT(claim, claim);
	BOOT_INPUT(anchor, anchor);
#undef BOOT_INPUT
	/* Composition must not consult these unrelated runtime authorities. */
	test_cf_grant = false;
	test_cf_lock_calls = 0;
	test_contract = CLUSTER_CF_CONTRACT_UNVERIFIED;
}

static ClusterControlRootResult
bootstrap_refused(const ClusterControlBootstrapInput *input)
{
	ClusterControlBootstrapSnapshot out;
	ClusterControlRootResult result;

	memset(&out, 0xa5, sizeof(out));
	result = cluster_control_bootstrap_decode(input, &out);
	UT_ASSERT(result != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	return result;
}

UT_TEST(test_bootstrap_composes_exact_threads_without_admission)
{
	BootstrapFixture f;
	ClusterControlBootstrapSnapshot out;
	uint8 hash[32];

	for (int node = 0; node <= 127; node += 127) {
		bootstrap_fixture(&f, node);
		UT_ASSERT_EQ(cluster_control_bootstrap_decode(&f.input, &out), 0);
		UT_ASSERT_EQ(out.binding.node_id, node);
		UT_ASSERT_EQ(out.thread.origin_thread_id, node + 1);
		UT_ASSERT_EQ(out.thread.origin_owner_incarnation,
					 f.local_anchor.identity.origin_owner_incarnation);
		UT_ASSERT_EQ(out.control.checkPoint, f.local_anchor.checkpoint);
		UT_ASSERT_EQ(out.control.minRecoveryPoint, f.local_anchor.min_recovery_point);
		UT_ASSERT_EQ(out.control.MaxConnections, 300 + node);
		UT_ASSERT_EQ(out.control.checkPointCopy.nextOid, 60001);
		UT_ASSERT_EQ(out.config.identity.generation, 47);
		UT_ASSERT_EQ(out.root_sequence, 7);
		UT_ASSERT_EQ(out.database_state, CLUSTER_CONTROL_ROOT_DATABASE_OPEN);
		sha256_bytes(f.before, sizeof(f.before), hash);
		UT_ASSERT(memcmp(hash, out.root_sha256, 32) == 0);
		UT_ASSERT_EQ(test_cf_lock_calls, 0);
	}
}

UT_TEST(test_bootstrap_every_local_root_identity_must_match)
{
	BootstrapFixture f;
	PgracControlBinding binding;

	bootstrap_fixture(&f, 0);
	for (int fault = 0; fault < 9; fault++) {
		UT_ASSERT(pgrac_control_binding_decode(f.binding, sizeof(f.binding), &binding));
		switch (fault) {
		case 0:
			binding.system_identifier++;
			break;
		case 1:
			binding.storage_uuid[0] ^= 1;
			break;
		case 2:
			binding.authority_uuid[0] ^= 1;
			break;
		case 3:
			binding.database_incarnation++;
			break;
		case 4:
			binding.node_id = 127;
			break;
		case 5:
			binding.migration_round_sha256[0] ^= 1;
			break;
		case 6:
			binding.source_wal_state_sha256[0] ^= 1;
			break;
		case 7:
			binding.migration_prepare_generation++;
			break;
		case 8:
			binding.migration_transition_epoch++;
			break;
		}
		UT_ASSERT(pgrac_control_binding_encode(&binding, f.binding, sizeof(f.binding)));
		UT_ASSERT_EQ(bootstrap_refused(&f.input), CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
		bootstrap_fixture(&f, 0);
	}
}

UT_TEST(test_bootstrap_changed_root_is_not_a_partial_success)
{
	BootstrapFixture f;

	bootstrap_fixture(&f, 0);
	put_u64_le(f.after + 16, 8);
	v2_checksums(f.after);
	UT_ASSERT_EQ(bootstrap_refused(&f.input), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	/* A revoked after-image is terminal, not a retry that could reopen it. */
	put_u32_le(f.after + 196, CLUSTER_CONTROL_ROOT_DATABASE_MIGRATION_REVOKED);
	v2_checksums(f.after);
	UT_ASSERT_EQ(bootstrap_refused(&f.input), CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID);
	f.after[200] ^= 1;
	UT_ASSERT_EQ(bootstrap_refused(&f.input), CLUSTER_CONTROL_ROOT_BAD_HEADER_CRC);
}

UT_TEST(test_bootstrap_absent_unconfigured_retired_or_revoked)
{
	BootstrapFixture f;

	for (int fault = 0; fault < 4; fault++) {
		bootstrap_fixture(&f, 0);
		if (fault == 0)
			memset(f.before + 512, 0, 512);
		else if (fault == 1) {
			put_u64_le(f.before + 216, 0);
			put_u64_le(f.before + 232, 0);
		} else if (fault == 2)
			f.before[512 + 10] = CLUSTER_CONTROL_ROOT_LIFECYCLE_RETIRED;
		else
			put_u32_le(f.before + 196, CLUSTER_CONTROL_ROOT_DATABASE_MIGRATION_REVOKED);
		v2_checksums(f.before);
		memcpy(f.after, f.before, sizeof(f.after));
		bootstrap_refused(&f.input);
	}
}

UT_TEST(test_bootstrap_every_selected_object_is_required)
{
	BootstrapFixture f;
	uint8 *objects[5];

	for (int fault = 0; fault < 5; fault++) {
		bootstrap_fixture(&f, 0);
		objects[0] = f.binding;
		objects[1] = f.common;
		objects[2] = f.config;
		objects[3] = f.claim;
		objects[4] = f.anchor;
		objects[fault][20] ^= 1;
		bootstrap_refused(&f.input);
	}
	bootstrap_fixture(&f, 0);
	put_u16_le(f.before + 4, 1);
	v2_checksums(f.before);
	memcpy(f.after, f.before, sizeof(f.after));
	UT_ASSERT_EQ(bootstrap_refused(&f.input), CLUSTER_CONTROL_ROOT_BAD_VERSION);
}

UT_TEST(test_bootstrap_bad_input_lengths_and_alias_clear_output)
{
	BootstrapFixture f;
	ClusterControlBootstrapBytes *objects[7];
	ClusterControlBootstrapSnapshot out;

	bootstrap_fixture(&f, 0);
	objects[0] = &f.input.binding;
	objects[1] = &f.input.root_before;
	objects[2] = &f.input.root_after;
	objects[3] = &f.input.common;
	objects[4] = &f.input.config;
	objects[5] = &f.input.claim;
	objects[6] = &f.input.anchor;
	UT_ASSERT_EQ(bootstrap_refused(NULL), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_control_bootstrap_decode(&f.input, NULL),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	for (int i = 0; i < 7; i++) {
		ClusterControlBootstrapBytes saved = *objects[i];

		objects[i]->data = NULL;
		bootstrap_refused(&f.input);
		*objects[i] = saved;
		objects[i]->len--;
		bootstrap_refused(&f.input);
		*objects[i] = saved;
		objects[i]->len = SIZE_MAX;
		bootstrap_refused(&f.input);
		*objects[i] = saved;
	}
	f.input.node_id = 128;
	bootstrap_refused(&f.input);
	f.input.node_id = 0;
	UT_ASSERT_EQ(
		cluster_control_bootstrap_decode(&f.input, (ClusterControlBootstrapSnapshot *)f.before),
		CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(v2_zero(f.before, sizeof(out)));
}

static void
bootstrap_replace_anchor(BootstrapFixture *f)
{
	uint8 hash[32];

	UT_ASSERT_EQ(cluster_recovery_anchor_v2_encode(&f->local_anchor, f->anchor), 0);
	sha256_bytes(f->anchor, sizeof(f->anchor), hash);
	memcpy(f->before + 512 + 264, hash, 32);
	v2_checksums(f->before);
	memcpy(f->after, f->before, sizeof(f->after));
}

UT_TEST(test_bootstrap_thread_lifecycle_overrides_old_clean_anchor)
{
	for (int life = 1; life <= 5; ++life) {
		BootstrapFixture f;
		ClusterControlBootstrapSnapshot out;
		bootstrap_fixture(&f, 0);
		f.local_anchor.state = DB_SHUTDOWNED;
		f.local_anchor.min_recovery_point = 0;
		f.local_anchor.min_recovery_tli = 0;
		f.before[512 + 10] = life;
		bootstrap_replace_anchor(&f);
		if (life == CLUSTER_CONTROL_ROOT_LIFECYCLE_RETIRED) {
			UT_ASSERT_EQ(bootstrap_refused(&f.input), CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID);
		} else {
			DBState expected = life == CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN ? DB_IN_PRODUCTION
							   : life == CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED
								   ? DB_SHUTDOWNED
								   : DB_IN_CRASH_RECOVERY;
			UT_ASSERT_EQ(cluster_control_bootstrap_decode(&f.input, &out), 0);
			UT_ASSERT_EQ(out.control.state, expected);
			UT_ASSERT_EQ(out.database_state, CLUSTER_CONTROL_ROOT_DATABASE_OPEN);
			UT_ASSERT_EQ(test_cf_lock_calls, 0);
		}
	}
}

UT_TEST(test_bootstrap_closed_thread_requires_clean_anchor)
{
	BootstrapFixture f;
	bootstrap_fixture(&f, 0);
	f.before[512 + 10] = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	bootstrap_replace_anchor(&f);
	UT_ASSERT_EQ(bootstrap_refused(&f.input), CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID);
}

UT_TEST(test_bootstrap_anchor_requires_exact_redo_and_no_backup)
{
	BootstrapFixture f;

	bootstrap_fixture(&f, 0);
	f.local_anchor.checkpoint_copy.redo += 4096;
	f.local_anchor.checkpoint += 4096;
	f.local_anchor.min_recovery_point += 4096;
	bootstrap_replace_anchor(&f);
	UT_ASSERT_EQ(bootstrap_refused(&f.input), CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
	bootstrap_fixture(&f, 0);
	f.local_anchor.backup_start = UINT64_C(0x1000100);
	f.local_anchor.backup_end = UINT64_C(0x1000200);
	f.local_anchor.backup_end_required = false;
	bootstrap_replace_anchor(&f);
	UT_ASSERT_EQ(bootstrap_refused(&f.input), CLUSTER_CONTROL_ROOT_RANGE_INVALID);
}

UT_TEST(test_bootstrap_prepared_observation_is_not_open)
{
	BootstrapFixture f;
	ClusterControlBootstrapSnapshot out;

	for (int state = 1; state <= 5; state++) {
		bootstrap_fixture(&f, 0);
		put_u32_le(f.before + 76, CLUSTER_CONTROL_ROOT_ACTIVATION_PREPARED);
		put_u32_le(f.before + 196, state);
		v2_checksums(f.before);
		memcpy(f.after, f.before, sizeof(f.after));
		UT_ASSERT_EQ(cluster_control_bootstrap_decode(&f.input, &out), 0);
		UT_ASSERT_EQ(out.activation_state, CLUSTER_CONTROL_ROOT_ACTIVATION_PREPARED);
		UT_ASSERT_EQ(out.database_state, state);
		UT_ASSERT_EQ(out.control.state, DB_IN_PRODUCTION);
	}
}

/* PGRAC: real filesystem collection, with syscall-timed mutation only.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static char bootstrap_local[MAXPGPATH];
static int bootstrap_race;
static int bootstrap_root_opens;
static int bootstrap_race_at;
static int bootstrap_close_calls;
static int bootstrap_close_fail_at;
static bool bootstrap_wal_route_race;
static uint8 bootstrap_replacement[66048];
static uint8 bootstrap_binding_replacement[256];
static bool bootstrap_side_race;
static char bootstrap_side_replace[MAXPGPATH], bootstrap_side_saved[MAXPGPATH];
static bool bootstrap_inputs_race, bootstrap_inputs_race_file;
static char bootstrap_inputs_replace[MAXPGPATH], bootstrap_inputs_saved[MAXPGPATH];

/* Native input qualification reads these actual files, never a mock result. */
static const char *const native_input_dirs[]
	= { "pg_twophase", "pg_replslot", "pg_logical", "pg_logical/snapshots", "pg_logical/mappings" };

static void
native_input_fixture(char *root)
{
	char path[MAXPGPATH];
	strlcpy(root, "/tmp/pgrac-native-inputs.XXXXXX", MAXPGPATH);
	UT_ASSERT(mkdtemp(root) != NULL);
	for (size_t i = 0; i < lengthof(native_input_dirs); ++i) {
		snprintf(path, sizeof(path), "%s/%s", root, native_input_dirs[i]);
		UT_ASSERT_EQ(mkdir(path, 0700), 0);
	}
	bootstrap_close_calls = bootstrap_close_fail_at = 0;
}

static void
native_input_cleanup(const char *root)
{
	char path[MAXPGPATH];
	for (int i = lengthof(native_input_dirs) - 1; i >= 0; --i) {
		snprintf(path, sizeof(path), "%s/%s", root, native_input_dirs[i]);
		UT_ASSERT_EQ(rmdir(path), 0);
	}
	UT_ASSERT_EQ(rmdir(root), 0);
}

UT_TEST(test_native_inputs_accept_only_empty_native_progress)
{
	char root[MAXPGPATH], path[MAXPGPATH];
	uint32 magic = UINT32_C(0x1257DADE);
	pg_crc32c crc;
	uint8 bytes[9], observed[8];

	native_input_fixture(root);
	UT_ASSERT_EQ(cluster_control_bootstrap_native_inputs(root), 0);
	snprintf(path, sizeof(path), "%s/pg_logical/replorigin_checkpoint", root);
	memcpy(bytes, &magic, sizeof(magic));
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, sizeof(magic));
	FIN_CRC32C(crc);
	memcpy(bytes + sizeof(magic), &crc, sizeof(crc));
	write_all_or_abort(path, bytes, 8);
	UT_ASSERT_EQ(cluster_control_bootstrap_native_inputs(root), 0);
	read_all_or_abort(path, observed, sizeof(observed));
	UT_ASSERT(memcmp(bytes, observed, sizeof(observed)) == 0);
	for (unsigned i = 0; i < 8; ++i) {
		bytes[i] ^= 1;
		write_all_or_abort(path, bytes, 8);
		UT_ASSERT(cluster_control_bootstrap_native_inputs(root) != 0);
		bytes[i] ^= 1;
	}
	bytes[8] = 1;
	for (unsigned size = 0; size <= 9; ++size) {
		if (size == 8)
			continue;
		write_all_or_abort(path, bytes, size);
		UT_ASSERT(cluster_control_bootstrap_native_inputs(root) != 0);
	}
	UT_ASSERT_EQ(unlink(path), 0);
	UT_ASSERT_EQ(mkfifo(path, 0600), 0);
	UT_ASSERT(cluster_control_bootstrap_native_inputs(root) != 0);
	UT_ASSERT_EQ(unlink(path), 0);
	UT_ASSERT_EQ(symlink("absent", path), 0);
	UT_ASSERT(cluster_control_bootstrap_native_inputs(root) != 0);
	UT_ASSERT_EQ(unlink(path), 0);
	native_input_cleanup(root);
}

UT_TEST(test_native_inputs_refuse_all_recovery_signals_without_cleanup)
{
	static const char *const signals[] = { "backup_label",
										   "tablespace_map",
										   "recovery.signal",
										   "standby.signal",
										   "recovery.conf",
										   "recovery.done",
										   "pg_logical/replorigin_checkpoint.tmp" };
	char root[MAXPGPATH], path[MAXPGPATH];
	uint8 bytes[17] = "retained-input", observed[17];
	struct stat st;
	native_input_fixture(root);
	for (size_t i = 0; i < lengthof(signals); ++i) {
		snprintf(path, sizeof(path), "%s/%s", root, signals[i]);
		write_all_or_abort(path, bytes, sizeof(bytes));
		UT_ASSERT(cluster_control_bootstrap_native_inputs(root) != 0);
		read_all_or_abort(path, observed, sizeof(observed));
		UT_ASSERT(memcmp(bytes, observed, sizeof(bytes)) == 0);
		UT_ASSERT_EQ(unlink(path), 0);
		UT_ASSERT_EQ(symlink("absent", path), 0);
		UT_ASSERT(cluster_control_bootstrap_native_inputs(root) != 0);
		UT_ASSERT_EQ(lstat(path, &st), 0);
		UT_ASSERT(S_ISLNK(st.st_mode));
		UT_ASSERT_EQ(unlink(path), 0);
		UT_ASSERT_EQ(mkfifo(path, 0600), 0);
		UT_ASSERT(cluster_control_bootstrap_native_inputs(root) != 0);
		UT_ASSERT_EQ(unlink(path), 0);
	}
	UT_ASSERT_EQ(cluster_control_bootstrap_native_inputs(root), 0);
	native_input_cleanup(root);
}

UT_TEST(test_native_inputs_preserve_slots_and_prepared_files)
{
	char root[MAXPGPATH], path[MAXPGPATH], child[MAXPGPATH], saved[MAXPGPATH];
	uint8 bytes[17] = "retained-progress", observed[17];
	native_input_fixture(root);
	for (size_t i = 0; i < lengthof(native_input_dirs); ++i) {
		/* pg_logical necessarily contains its two native subdirectories. */
		if (i == 2)
			continue;
		snprintf(path, sizeof(path), "%s/%s", root, native_input_dirs[i]);
		snprintf(child, sizeof(child), "%s/retained.tmp", path);
		write_all_or_abort(child, bytes, sizeof(bytes));
		UT_ASSERT(cluster_control_bootstrap_native_inputs(root) != 0);
		read_all_or_abort(child, observed, sizeof(observed));
		UT_ASSERT(memcmp(bytes, observed, sizeof(bytes)) == 0);
		UT_ASSERT_EQ(unlink(child), 0);
		UT_ASSERT_EQ(mkdir(child, 0700), 0);
		UT_ASSERT(cluster_control_bootstrap_native_inputs(root) != 0);
		UT_ASSERT_EQ(rmdir(child), 0);
		UT_ASSERT_EQ(chmod(path, 0770), 0);
		UT_ASSERT(cluster_control_bootstrap_native_inputs(root) != 0);
		UT_ASSERT_EQ(chmod(path, 0700), 0);
		snprintf(saved, sizeof(saved), "%s.saved", path);
		UT_ASSERT_EQ(rename(path, saved), 0);
		UT_ASSERT(cluster_control_bootstrap_native_inputs(root) != 0);
		UT_ASSERT_EQ(symlink(saved, path), 0);
		UT_ASSERT(cluster_control_bootstrap_native_inputs(root) != 0);
		UT_ASSERT_EQ(unlink(path), 0);
		UT_ASSERT_EQ(rename(saved, path), 0);
	}
	UT_ASSERT_EQ(cluster_control_bootstrap_native_inputs(root), 0);
	native_input_cleanup(root);
}

UT_TEST(test_native_inputs_recheck_namespace_and_release_fds)
{
	char root[MAXPGPATH], checkpoint[MAXPGPATH];
	uint32 magic = UINT32_C(0x1257DADE);
	pg_crc32c crc;
	uint8 bytes[8];
	int before = 0, after = 0, closes;
	const char *bad[] = { NULL, "relative", "/", "/tmp/../tmp", "/tmp//input" };
	for (size_t i = 0; i < lengthof(bad); ++i)
		UT_ASSERT(cluster_control_bootstrap_native_inputs(bad[i]) != 0);
	native_input_fixture(root);
	snprintf(checkpoint, sizeof(checkpoint), "%s/pg_logical/replorigin_checkpoint", root);
	memcpy(bytes, &magic, sizeof(magic));
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, sizeof(magic));
	FIN_CRC32C(crc);
	memcpy(bytes + sizeof(magic), &crc, sizeof(crc));
	write_all_or_abort(checkpoint, bytes, sizeof(bytes));
	for (int fd = 0; fd < 256; ++fd)
		if (fcntl(fd, F_GETFD) >= 0)
			++before;
	UT_ASSERT_EQ(cluster_control_bootstrap_native_inputs(root), 0);
	closes = bootstrap_close_calls;
	UT_ASSERT(closes > 0);
	for (int i = 1; i <= closes; ++i) {
		bootstrap_close_calls = 0;
		bootstrap_close_fail_at = i;
		UT_ASSERT_EQ(cluster_control_bootstrap_native_inputs(root), CLUSTER_CONTROL_ROOT_IO_ERROR);
	}
	bootstrap_close_fail_at = 0;
	for (int i = -1; i < (int)lengthof(native_input_dirs); ++i) {
		if (i < 0)
			strlcpy(bootstrap_inputs_replace, root, sizeof(bootstrap_inputs_replace));
		else
			snprintf(bootstrap_inputs_replace, sizeof(bootstrap_inputs_replace), "%s/%s", root,
					 native_input_dirs[i]);
		snprintf(bootstrap_inputs_saved, sizeof(bootstrap_inputs_saved), "%s.saved",
				 bootstrap_inputs_replace);
		bootstrap_inputs_race = true;
		bootstrap_inputs_race_file = false;
		UT_ASSERT(cluster_control_bootstrap_native_inputs(root) != 0);
		UT_ASSERT(!bootstrap_inputs_race);
		UT_ASSERT_EQ(rmdir(bootstrap_inputs_replace), 0);
		UT_ASSERT_EQ(rename(bootstrap_inputs_saved, bootstrap_inputs_replace), 0);
	}
	strlcpy(bootstrap_inputs_replace, checkpoint, sizeof(bootstrap_inputs_replace));
	snprintf(bootstrap_inputs_saved, sizeof(bootstrap_inputs_saved), "%s.saved", checkpoint);
	bootstrap_inputs_race = bootstrap_inputs_race_file = true;
	UT_ASSERT(cluster_control_bootstrap_native_inputs(root) != 0);
	UT_ASSERT(!bootstrap_inputs_race);
	UT_ASSERT_EQ(unlink(checkpoint), 0);
	UT_ASSERT_EQ(rename(bootstrap_inputs_saved, checkpoint), 0);
	UT_ASSERT_EQ(cluster_control_bootstrap_native_inputs(root), 0);
	for (int fd = 0; fd < 256; ++fd)
		if (fcntl(fd, F_GETFD) >= 0)
			++after;
	UT_ASSERT_EQ(before, after);
	UT_ASSERT_EQ(unlink(checkpoint), 0);
	native_input_cleanup(root);
}

/* Physical aliases only: not a native-side migration or ownership grant. */
static const char *const side_names[]
	= { "pg_xact", "pg_subtrans", "pg_multixact", "pg_commit_ts" };
typedef struct BootstrapSideFixture {
	char root[MAXPGPATH];
	char local[MAXPGPATH];
	char shared[MAXPGPATH];
	char origin[MAXPGPATH];
} BootstrapSideFixture;

static void
bootstrap_side_fixture(BootstrapSideFixture *f, uint32 node)
{
	char path[MAXPGPATH], alias[MAXPGPATH];
	memset(f, 0, sizeof(*f));
	strlcpy(f->root, "/tmp/pgrac-native-side.XXXXXX", sizeof(f->root));
	UT_ASSERT(mkdtemp(f->root) != NULL);
	snprintf(f->local, sizeof(f->local), "%s/local", f->root);
	snprintf(f->shared, sizeof(f->shared), "%s/shared", f->root);
	UT_ASSERT_EQ(mkdir(f->local, 0700), 0);
	UT_ASSERT_EQ(mkdir(f->shared, 0700), 0);
	snprintf(path, sizeof(path), "%s/native_side", f->shared);
	UT_ASSERT_EQ(mkdir(path, 0700), 0);
	snprintf(f->origin, sizeof(f->origin), "%s/native_side/origin_%u", f->shared, node);
	UT_ASSERT_EQ(mkdir(f->origin, 0700), 0);
	for (size_t i = 0; i < lengthof(side_names); ++i) {
		snprintf(path, sizeof(path), "%s/%s", f->origin, side_names[i]);
		snprintf(alias, sizeof(alias), "%s/%s", f->local, side_names[i]);
		UT_ASSERT_EQ(mkdir(path, 0700), 0);
		UT_ASSERT_EQ(symlink(path, alias), 0);
	}
	snprintf(path, sizeof(path), "%s/pg_multixact/offsets", f->origin);
	UT_ASSERT_EQ(mkdir(path, 0700), 0);
	snprintf(path, sizeof(path), "%s/pg_multixact/members", f->origin);
	UT_ASSERT_EQ(mkdir(path, 0700), 0);
	bootstrap_close_calls = bootstrap_close_fail_at = 0;
}

static void
bootstrap_side_cleanup(const BootstrapSideFixture *f)
{
	char path[MAXPGPATH];
	snprintf(path, sizeof(path), "%s/pg_multixact/offsets", f->origin);
	UT_ASSERT_EQ(rmdir(path), 0);
	snprintf(path, sizeof(path), "%s/pg_multixact/members", f->origin);
	UT_ASSERT_EQ(rmdir(path), 0);
	for (size_t i = 0; i < lengthof(side_names); ++i) {
		snprintf(path, sizeof(path), "%s/%s", f->local, side_names[i]);
		UT_ASSERT_EQ(unlink(path), 0);
		snprintf(path, sizeof(path), "%s/%s", f->origin, side_names[i]);
		UT_ASSERT_EQ(rmdir(path), 0);
	}
	UT_ASSERT_EQ(rmdir(f->origin), 0);
	snprintf(path, sizeof(path), "%s/native_side", f->shared);
	UT_ASSERT_EQ(rmdir(path), 0);
	UT_ASSERT_EQ(rmdir(f->local), 0);
	UT_ASSERT_EQ(rmdir(f->shared), 0);
	UT_ASSERT_EQ(rmdir(f->root), 0);
}

UT_TEST(test_bootstrap_side_routes_require_exact_origin)
{
	for (unsigned node = 0; node <= 127; node += 127) {
		BootstrapSideFixture f;
		char path[MAXPGPATH];
		uint8 stored[17], marker[17] = "retained-status";
		bootstrap_side_fixture(&f, node);
		if (ut_current_failed)
			return;
		snprintf(path, sizeof(path), "%s/pg_subtrans/0000", f.origin);
		write_all_or_abort(path, marker, sizeof(marker));
		UT_ASSERT_EQ(cluster_control_bootstrap_side_route(f.local, f.shared, node), 0);
		read_all_or_abort(path, stored, sizeof(stored));
		UT_ASSERT(memcmp(marker, stored, sizeof(marker)) == 0);
		UT_ASSERT_EQ(unlink(path), 0);
		bootstrap_side_cleanup(&f);
	}
}

UT_TEST(test_bootstrap_side_rejects_local_foreign_and_unsafe_aliases)
{
	BootstrapSideFixture f;
	char path[MAXPGPATH], alias[MAXPGPATH], saved[MAXPGPATH];
	bootstrap_side_fixture(&f, 3);
	if (ut_current_failed)
		return;
	for (size_t i = 0; i < lengthof(side_names); ++i) {
		snprintf(path, sizeof(path), "%s/%s", f.origin, side_names[i]);
		snprintf(alias, sizeof(alias), "%s/%s", f.local, side_names[i]);
		for (unsigned fault = 0; fault < 5; ++fault) {
			UT_ASSERT_EQ(unlink(alias), 0);
			if (fault == 1)
				UT_ASSERT_EQ(mkdir(alias, 0700), 0);
			else if (fault == 2)
				UT_ASSERT_EQ(symlink(f.shared, alias), 0);
			else if (fault == 3)
				UT_ASSERT_EQ(mkfifo(alias, 0600), 0);
			else if (fault == 4) {
				UT_ASSERT_EQ(symlink(path, alias), 0);
				UT_ASSERT_EQ(chmod(path, 0777), 0);
			}
			UT_ASSERT(cluster_control_bootstrap_side_route(f.local, f.shared, 3) != 0);
			if (fault == 1)
				UT_ASSERT_EQ(rmdir(alias), 0);
			else if (fault != 0)
				UT_ASSERT_EQ(unlink(alias), 0);
			if (fault == 4)
				UT_ASSERT_EQ(chmod(path, 0700), 0);
			UT_ASSERT_EQ(symlink(path, alias), 0);
		}
		/* Even a same-inode route is invalid when its canonical family is
		 * itself a symlink outside the origin namespace. */
		snprintf(saved, sizeof(saved), "%s.saved", path);
		UT_ASSERT_EQ(rename(path, saved), 0);
		UT_ASSERT_EQ(symlink(saved, path), 0);
		UT_ASSERT(cluster_control_bootstrap_side_route(f.local, f.shared, 3) != 0);
		UT_ASSERT_EQ(unlink(path), 0);
		UT_ASSERT_EQ(rename(saved, path), 0);
	}
	UT_ASSERT(cluster_control_bootstrap_side_route(f.local, f.shared, 0) != 0);
	UT_ASSERT_EQ(cluster_control_bootstrap_side_route(f.local, f.shared, 3), 0);
	bootstrap_side_cleanup(&f);
}

UT_TEST(test_bootstrap_side_children_parents_and_arguments_are_strict)
{
	BootstrapSideFixture f;
	char path[MAXPGPATH], saved[MAXPGPATH];
	const char *bad[] = { NULL, "", ".", "/", "/tmp/", "/tmp//x", "/tmp/./x", "/tmp/../x" };
	bootstrap_side_fixture(&f, 0);
	if (ut_current_failed)
		return;
	for (size_t i = 0; i < lengthof(bad); ++i) {
		UT_ASSERT(cluster_control_bootstrap_side_route(bad[i], f.shared, 0) != 0);
		UT_ASSERT(cluster_control_bootstrap_side_route(f.local, bad[i], 0) != 0);
	}
	UT_ASSERT(cluster_control_bootstrap_side_route(f.local, f.shared, 128) != 0);
	for (unsigned object = 0; object < 6; ++object) {
		if (object == 0)
			strlcpy(path, f.local, sizeof(path));
		else if (object == 1)
			strlcpy(path, f.shared, sizeof(path));
		else if (object == 2)
			snprintf(path, sizeof(path), "%s/native_side", f.shared);
		else if (object == 3)
			strlcpy(path, f.origin, sizeof(path));
		else
			snprintf(path, sizeof(path), "%s/pg_multixact/%s", f.origin,
					 object == 4 ? "offsets" : "members");
		for (unsigned fault = 0; fault < 3; ++fault) {
			if (fault == 0)
				UT_ASSERT_EQ(chmod(path, 0777), 0);
			else {
				snprintf(saved, sizeof(saved), "%s.saved", path);
				UT_ASSERT_EQ(rename(path, saved), 0);
				if (fault == 2)
					UT_ASSERT_EQ(symlink(saved, path), 0);
			}
			UT_ASSERT(cluster_control_bootstrap_side_route(f.local, f.shared, 0) != 0);
			if (fault == 0)
				UT_ASSERT_EQ(chmod(path, 0700), 0);
			else {
				if (fault == 2)
					UT_ASSERT_EQ(unlink(path), 0);
				UT_ASSERT_EQ(rename(saved, path), 0);
			}
		}
	}
	bootstrap_side_cleanup(&f);
}

UT_TEST(test_bootstrap_side_reobserves_every_namespace_and_closes_all_fds)
{
	BootstrapSideFixture f;
	unsigned before = 0, after = 0;
	int closes;
	bootstrap_side_fixture(&f, 0);
	if (ut_current_failed)
		return;
	UT_ASSERT_EQ(cluster_control_bootstrap_side_route(f.local, f.shared, 0), 0);
	closes = bootstrap_close_calls;
	UT_ASSERT(closes > 0);
	for (int fd = 0; fd < 256; ++fd)
		if (fcntl(fd, F_GETFD) >= 0)
			++before;
	for (int i = 1; i <= closes; ++i) {
		bootstrap_close_calls = 0;
		bootstrap_close_fail_at = i;
		UT_ASSERT_EQ(cluster_control_bootstrap_side_route(f.local, f.shared, 0),
					 CLUSTER_CONTROL_ROOT_IO_ERROR);
		UT_ASSERT_EQ(bootstrap_close_calls, closes);
	}
	bootstrap_close_fail_at = 0;
	for (unsigned object = 0; object < 10; ++object) {
		if (object == 0)
			strlcpy(bootstrap_side_replace, f.local, sizeof(bootstrap_side_replace));
		else if (object == 1)
			strlcpy(bootstrap_side_replace, f.shared, sizeof(bootstrap_side_replace));
		else if (object == 2)
			snprintf(bootstrap_side_replace, sizeof(bootstrap_side_replace), "%s/native_side",
					 f.shared);
		else if (object == 3)
			strlcpy(bootstrap_side_replace, f.origin, sizeof(bootstrap_side_replace));
		else if (object < 8)
			snprintf(bootstrap_side_replace, sizeof(bootstrap_side_replace), "%s/%s", f.origin,
					 side_names[object - 4]);
		else
			snprintf(bootstrap_side_replace, sizeof(bootstrap_side_replace), "%s/pg_multixact/%s",
					 f.origin, object == 8 ? "offsets" : "members");
		snprintf(bootstrap_side_saved, sizeof(bootstrap_side_saved), "%s.saved",
				 bootstrap_side_replace);
		bootstrap_side_race = true;
		UT_ASSERT(cluster_control_bootstrap_side_route(f.local, f.shared, 0) != 0);
		UT_ASSERT(!bootstrap_side_race);
		UT_ASSERT_EQ(rmdir(bootstrap_side_replace), 0);
		UT_ASSERT_EQ(rename(bootstrap_side_saved, bootstrap_side_replace), 0);
	}
	for (int fd = 0; fd < 256; ++fd)
		if (fcntl(fd, F_GETFD) >= 0)
			++after;
	UT_ASSERT_EQ(before, after);
	UT_ASSERT_EQ(cluster_control_bootstrap_side_route(f.local, f.shared, 0), 0);
	bootstrap_side_cleanup(&f);
}

int unit_bootstrap_openat(int dir, const char *name, int flags, ...);
int unit_bootstrap_close(int fd);

int
unit_bootstrap_close(int fd)
{
	int result = close(fd);

	if (++bootstrap_close_calls == bootstrap_close_fail_at) {
		errno = EIO;
		return -1;
	}
	return result;
}

int
unit_bootstrap_openat(int dir, const char *name, int flags, ...)
{
	char primary[MAXPGPATH], staging[MAXPGPATH], binding[MAXPGPATH];
	bool root_open = strcmp(name, "pgrac_control_root") == 0;
	bool missing_object = bootstrap_race == 2 && strstr(name, ".bin") != NULL;
	if (bootstrap_inputs_race && strcmp(name, "replorigin_checkpoint") == 0) {
		int fd = openat(dir, name, flags);
		bootstrap_inputs_race = false;
		if (fd < 0 || rename(bootstrap_inputs_replace, bootstrap_inputs_saved) != 0)
			abort();
		if (bootstrap_inputs_race_file) {
			uint8 bytes[8];
			read_all_or_abort(bootstrap_inputs_saved, bytes, sizeof(bytes));
			write_all_or_abort(bootstrap_inputs_replace, bytes, sizeof(bytes));
		} else if (mkdir(bootstrap_inputs_replace, 0700) != 0)
			abort();
		return fd;
	}
	if (bootstrap_side_race && strcmp(name, "members") == 0) {
		int fd = openat(dir, name, flags);
		bootstrap_side_race = false;
		if (fd < 0 || rename(bootstrap_side_replace, bootstrap_side_saved) != 0
			|| mkdir(bootstrap_side_replace, 0700) != 0)
			abort();
		return fd;
	}
	if (bootstrap_wal_route_race && strcmp(name, "pg_wal") == 0) {
		int fd = openat(dir, name, flags);
		bootstrap_wal_route_race = false;
		if (fd < 0 || unlinkat(dir, name, 0) != 0
			|| symlinkat(cluster_wal_threads_dir, dir, name) != 0)
			abort();
		return fd;
	}

	if (root_open)
		bootstrap_root_opens++;
	if ((root_open && bootstrap_root_opens == bootstrap_race_at && bootstrap_race != 0)
		|| missing_object) {
		path_for(primary, sizeof(primary), CLUSTER_CONTROL_ROOT_REL_PATH);
		path_for(staging, sizeof(staging), "global/bootstrap-test-new-root");
		if (bootstrap_race == 6) {
			path_for(primary, sizeof(primary), "global");
			path_for(staging, sizeof(staging), "global.bootstrap-saved");
			if (rename(primary, staging) != 0 || mkdir(primary, 0700) != 0)
				abort();
		} else if (bootstrap_race == 5) {
			snprintf(binding, sizeof(binding), "%s/global/%s", bootstrap_local,
					 PGRAC_CONTROL_BINDING_NAME);
			write_all_or_abort(binding, bootstrap_binding_replacement, 256);
		} else {
			write_all_or_abort(staging, bootstrap_replacement, 66048);
			if (rename(staging, primary) != 0)
				abort();
		}
		bootstrap_race = 0;
		if (missing_object) {
			/* Retiring an old object after publishing a new root is real I/O. */
			if (unlinkat(dir, name, 0) != 0)
				abort();
		}
	}
	return openat(dir, name, flags);
}

static void
bootstrap_read_fixture(BootstrapFixture *f, int node)
{
	char path[MAXPGPATH];

	bootstrap_race = bootstrap_root_opens = 0;
	bootstrap_race_at = 2;
	bootstrap_close_calls = bootstrap_close_fail_at = 0;
	bootstrap_fixture(f, node);
	if (bootstrap_local[0] == '\0') {
		strlcpy(bootstrap_local, "/tmp/pgrac-bootstrap-local.XXXXXX", sizeof(bootstrap_local));
		if (mkdtemp(bootstrap_local) == NULL)
			abort();
		snprintf(path, sizeof(path), "%s/global", bootstrap_local);
		if (mkdir(path, 0700) != 0)
			abort();
	}
	snprintf(path, sizeof(path), "%s/global/%s", bootstrap_local, PGRAC_CONTROL_BINDING_NAME);
	write_all_or_abort(path, f->binding, sizeof(f->binding));
}

UT_TEST(test_bootstrap_wal_route_exact_generation_and_refusals)
{
	BootstrapFixture f;
	ClusterControlBootstrapSnapshot snapshot;
	ClusterWalDurablePrefix prefix = { .sequence = 1 };
	char generation[MAXPGPATH], prefix_dir[MAXPGPATH], current[MAXPGPATH];
	char pgwal[MAXPGPATH], moved[MAXPGPATH];
	uint8 bytes[CLUSTER_WAL_DURABLE_PREFIX_BYTES];
	int before = 0, after = 0;

	for (int fd = 0; fd < 256; fd++)
		if (fcntl(fd, F_GETFD) >= 0)
			before++;
	for (int fault = 0; fault < 11; fault++) {
		bootstrap_read_fixture(&f, fault == 0 ? 127 : 0);
		UT_ASSERT_EQ(cluster_control_bootstrap_decode(&f.input, &snapshot), 0);
		v2_claim_path(&snapshot.thread, generation);
		*strrchr(generation, '/') = '\0';
		snprintf(prefix_dir, sizeof(prefix_dir), "%s/durable_prefix", generation);
		UT_ASSERT(mkdir(prefix_dir, 0700) == 0 || errno == EEXIST);
		snprintf(current, sizeof(current), "%s/current", prefix_dir);
		UT_ASSERT_EQ(cluster_wal_durable_prefix_encode(&snapshot.wal, &prefix, bytes), 0);
		if (fault == 4)
			bytes[140] ^= 1;
		if (fault == 5) {
			ClusterWalDurablePrefixRef foreign = snapshot.wal;
			foreign.timeline++;
			UT_ASSERT_EQ(cluster_wal_durable_prefix_encode(&foreign, &prefix, bytes), 0);
		}
		write_all_or_abort(current, bytes, sizeof(bytes));
		snprintf(pgwal, sizeof(pgwal), "%s/pg_wal", bootstrap_local);
		UT_ASSERT(unlink(pgwal) == 0 || errno == ENOENT);
		UT_ASSERT_EQ(symlink(fault == 1 ? cluster_wal_threads_dir : generation, pgwal), 0);
		if (fault == 2)
			UT_ASSERT_EQ(unlink(pgwal), 0);
		if (fault == 3 || fault == 8)
			UT_ASSERT_EQ(unlink(current), 0);
		if (fault == 6)
			UT_ASSERT_EQ(chmod(generation, 0777), 0);
		if (fault == 7) {
			snprintf(moved, sizeof(moved), "%s.saved", generation);
			UT_ASSERT_EQ(rename(generation, moved), 0);
			UT_ASSERT_EQ(symlink(moved, generation), 0);
		}
		if (fault == 8)
			UT_ASSERT_EQ(mkfifo(current, 0600), 0);
		bootstrap_wal_route_race = fault == 9;
		bootstrap_close_fail_at = fault == 10 ? 1 : 0;
		if (fault == 0)
			UT_ASSERT_EQ(cluster_control_bootstrap_wal_route(
							 bootstrap_local, cluster_wal_threads_dir, &snapshot.wal),
						 0);
		else
			UT_ASSERT(cluster_control_bootstrap_wal_route(bootstrap_local, cluster_wal_threads_dir,
														  &snapshot.wal)
					  != 0);
		bootstrap_close_fail_at = 0;
		if (fault == 6)
			UT_ASSERT_EQ(chmod(generation, 0700), 0);
		if (fault == 7) {
			UT_ASSERT_EQ(unlink(generation), 0);
			UT_ASSERT_EQ(rename(moved, generation), 0);
		}
		if (fault == 8)
			UT_ASSERT_EQ(unlink(current), 0);
	}
	UT_ASSERT(cluster_control_bootstrap_wal_route(NULL, cluster_wal_threads_dir, &snapshot.wal)
			  != 0);
	UT_ASSERT(cluster_control_bootstrap_wal_route(bootstrap_local, cluster_wal_threads_dir, NULL)
			  != 0);
	for (int fd = 0; fd < 256; fd++)
		if (fcntl(fd, F_GETFD) >= 0)
			after++;
	UT_ASSERT_EQ(after, before);
}

static void
bootstrap_selected_path(const BootstrapFixture *f, int object, char path[MAXPGPATH])
{
	char hex[65];
	ControlRootImage root;
	uint32 node = f->input.node_id;

	if (cluster_control_root_v2_decode(f->before, sizeof(f->before), v2_storage, TEST_SYSID, &root))
		abort();
	switch (object) {
	case 0:
		bootstrap_hex(root.header.v2.control_image_sha256, hex);
		snprintf(path, MAXPGPATH, "%s/global/control_images/53-%s.bin", test_root, hex);
		break;
	case 1:
		bootstrap_hex(root.header.v2.config_sha256, hex);
		snprintf(path, MAXPGPATH, "%s/global/config_images/47-%s.conf", test_root, hex);
		break;
	case 2:
		v2_claim_path(&f->local_anchor.identity, path);
		break;
	case 3:
		bootstrap_hex(root.refs[node].anchor_sha256, hex);
		snprintf(path, MAXPGPATH,
				 "%s/global/anchor_images/thread_%u/generation_" UINT64_FORMAT
				 "/anchor_" UINT64_FORMAT "-%s.bin",
				 test_root, f->local_anchor.identity.origin_thread_id,
				 f->local_anchor.identity.origin_owner_incarnation,
				 f->local_anchor.anchor_generation, hex);
		break;
	case 4:
		path_for(path, MAXPGPATH, CLUSTER_CONTROL_ROOT_REL_PATH);
		break;
	case 5:
		snprintf(path, MAXPGPATH, "%s/global/%s", bootstrap_local, PGRAC_CONTROL_BINDING_NAME);
		break;
	default:
		abort();
	}
}

static ClusterControlRootResult
bootstrap_read_refused(uint32 node)
{
	ClusterControlBootstrapObservation out;
	ClusterControlRootResult result;

	memset(&out, 0xa5, sizeof(out));
	result = cluster_control_bootstrap_read(bootstrap_local, test_root, test_wal_root, node, &out);
	UT_ASSERT(result != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY && out.config_bytes != NULL)
		pfree(out.config_bytes);
	return result;
}

UT_TEST(test_bootstrap_read_exact_files_and_owned_config)
{
	BootstrapFixture f;
	ClusterControlBootstrapObservation out;

	for (int node = 0; node <= 127; node += 127) {
		ClusterControlRootResult result;

		bootstrap_read_fixture(&f, node);
		result
			= cluster_control_bootstrap_read(bootstrap_local, test_root, test_wal_root, node, &out);
		UT_ASSERT_EQ(result, 0);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY || out.config_bytes == NULL)
			continue;
		UT_ASSERT_EQ(out.snapshot.thread.origin_thread_id, node + 1);
		UT_ASSERT_EQ(out.snapshot.control.checkPoint, f.local_anchor.checkpoint);
		UT_ASSERT_EQ(out.snapshot.control.MaxConnections, 300 + node);
		UT_ASSERT_EQ(out.config_len, f.input.config.len);
		UT_ASSERT(memcmp(out.config_bytes, f.config, out.config_len) == 0);
		UT_ASSERT_EQ(out.config_bytes[out.config_len], '\0');
		UT_ASSERT_EQ(test_cf_lock_calls, 0);
		UT_ASSERT_EQ(test_durable_rename_calls, 0);
		UT_ASSERT_EQ(bootstrap_root_opens, 3);
		v2_assert_primary_unchanged(f.before);
		pfree(out.config_bytes);
	}
}

UT_TEST(test_bootstrap_read_requires_independent_binding_and_node)
{
	BootstrapFixture f;
	char path[MAXPGPATH];

	bootstrap_read_fixture(&f, 0);
	bootstrap_selected_path(&f, 5, path);
	UT_ASSERT_EQ(unlink(path), 0);
	UT_ASSERT_EQ(bootstrap_read_refused(0), CLUSTER_CONTROL_ROOT_ABSENT);
	UT_ASSERT_EQ(bootstrap_root_opens, 0);
	bootstrap_read_fixture(&f, 0);
	UT_ASSERT_EQ(bootstrap_read_refused(127), CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
	UT_ASSERT_EQ(bootstrap_root_opens, 0);
	UT_ASSERT_EQ(bootstrap_read_refused(128), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
}

UT_TEST(test_bootstrap_read_never_falls_back_to_valid_bak)
{
	BootstrapFixture f;
	char path[MAXPGPATH];

	for (int fault = 0; fault < 3; fault++) {
		bootstrap_read_fixture(&f, 0);
		bootstrap_selected_path(&f, 4, path);
		if (fault == 0)
			UT_ASSERT_EQ(unlink(path), 0);
		else {
			if (fault == 1)
				f.before[200] ^= 1;
			else {
				put_u16_le(f.before + 4, 1);
				v2_checksums(f.before);
			}
			write_all_or_abort(path, f.before, sizeof(f.before));
		}
		UT_ASSERT_EQ(bootstrap_read_refused(0), fault == 0	 ? CLUSTER_CONTROL_ROOT_ABSENT
												: fault == 1 ? CLUSTER_CONTROL_ROOT_BAD_HEADER_CRC
															 : CLUSTER_CONTROL_ROOT_BAD_VERSION);
	}
}

UT_TEST(test_bootstrap_read_rejects_every_bad_selected_file)
{
	BootstrapFixture f;
	char path[MAXPGPATH];
	uint8 bytes[66049];
	struct stat st;

	for (int object = 0; object < 6; object++)
		for (int fault = 0; fault < 4; fault++) {
			bootstrap_read_fixture(&f, 0);
			bootstrap_selected_path(&f, object, path);
			UT_ASSERT_EQ(stat(path, &st), 0);
			read_all_or_abort(path, bytes, st.st_size);
			if (fault == 0)
				UT_ASSERT_EQ(unlink(path), 0);
			else if (fault == 1) {
				bytes[20] ^= 1;
				write_all_or_abort(path, bytes, st.st_size);
			} else if (fault == 2)
				UT_ASSERT_EQ(truncate(path, st.st_size - 1), 0);
			else {
				bytes[st.st_size] = 0;
				write_all_or_abort(path, bytes, st.st_size + 1);
			}
			bootstrap_read_refused(0);
		}
}

UT_TEST(test_bootstrap_read_unsafe_leaves_do_not_block_or_leak)
{
	BootstrapFixture f;
	char path[MAXPGPATH], saved[MAXPGPATH];
	int open_before = 0, open_after = 0;

	for (int fd = 0; fd < 256; fd++)
		if (fcntl(fd, F_GETFD) >= 0)
			open_before++;
	for (int object = 0; object < 6; object++)
		for (int fault = 0; fault < 3; fault++) {
			bootstrap_read_fixture(&f, 0);
			bootstrap_selected_path(&f, object, path);
			if (fault == 0)
				UT_ASSERT_EQ(chmod(path, 0666), 0);
			else {
				snprintf(saved, sizeof(saved), "%s.saved", path);
				UT_ASSERT_EQ(rename(path, saved), 0);
				if (fault == 1)
					UT_ASSERT_EQ(symlink(saved, path), 0);
				else
					UT_ASSERT_EQ(mkfifo(path, 0600), 0);
			}
			bootstrap_read_refused(0);
			if (fault == 0)
				UT_ASSERT_EQ(chmod(path, 0600), 0);
			else {
				UT_ASSERT_EQ(unlink(path), 0);
				UT_ASSERT_EQ(rename(saved, path), 0);
			}
		}
	for (int fd = 0; fd < 256; fd++)
		if (fcntl(fd, F_GETFD) >= 0)
			open_after++;
	UT_ASSERT_EQ(open_after, open_before);
}

UT_TEST(test_bootstrap_read_unsafe_directories_are_refused)
{
	BootstrapFixture f;
	char path[MAXPGPATH], saved[MAXPGPATH];

	for (int component = 0; component < 9; component++)
		for (int fault = 0; fault < 2; fault++) {
			bootstrap_read_fixture(&f, 0);
			switch (component) {
			case 0:
				strlcpy(path, test_root, sizeof(path));
				break;
			case 1:
				path_for(path, sizeof(path), "global");
				break;
			case 2:
				path_for(path, sizeof(path), "global/control_images");
				break;
			case 3:
				path_for(path, sizeof(path), "global/config_images");
				break;
			case 4:
				path_for(path, sizeof(path), "global/anchor_images");
				break;
			case 5:
				path_for(path, sizeof(path), "global/anchor_images/thread_1");
				break;
			case 6:
				path_for(path, sizeof(path), "global/anchor_images/thread_1/generation_99");
				break;
			case 7:
				strlcpy(path, test_wal_root, sizeof(path));
				break;
			case 8:
				snprintf(path, sizeof(path), "%s/thread_1/generation_99", test_wal_root);
				break;
			}
			if (fault == 0)
				UT_ASSERT_EQ(chmod(path, 0777), 0);
			else {
				snprintf(saved, sizeof(saved), "%s.saved", path);
				UT_ASSERT_EQ(rename(path, saved), 0);
				UT_ASSERT_EQ(symlink(saved, path), 0);
			}
			bootstrap_read_refused(0);
			if (fault == 0)
				UT_ASSERT_EQ(chmod(path, 0700), 0);
			else {
				UT_ASSERT_EQ(unlink(path), 0);
				UT_ASSERT_EQ(rename(saved, path), 0);
			}
		}
}

UT_TEST(test_bootstrap_read_real_root_replacement_and_binding_races)
{
	BootstrapFixture f;
	PgracControlBinding binding;

	for (int race = 1; race <= 5; race++) {
		bootstrap_read_fixture(&f, 0);
		memcpy(bootstrap_replacement, f.before, sizeof(f.before));
		put_u64_le(bootstrap_replacement + 16, 8);
		if (race == 3)
			put_u32_le(bootstrap_replacement + 196,
					   CLUSTER_CONTROL_ROOT_DATABASE_MIGRATION_REVOKED);
		else if (race == 4)
			put_u64_le(bootstrap_replacement + 200, 42);
		v2_checksums(bootstrap_replacement);
		UT_ASSERT(pgrac_control_binding_decode(f.binding, sizeof(f.binding), &binding));
		binding.target_qualification_sha256[0] ^= 1;
		UT_ASSERT(pgrac_control_binding_encode(&binding, bootstrap_binding_replacement, 256));
		bootstrap_race = race;
		UT_ASSERT_EQ(bootstrap_read_refused(0), race == 3 ? CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID
												: race == 4 || race == 5
													? CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH
													: CLUSTER_CONTROL_ROOT_STALE_TOKEN);
		UT_ASSERT_EQ(bootstrap_race, 0);
		UT_ASSERT_EQ(bootstrap_root_opens, 2);
	}
}

UT_TEST(test_bootstrap_read_invalid_paths_outputs_and_alias)
{
	BootstrapFixture f;
	ClusterControlBootstrapObservation out;
	const char *bad[] = { NULL, "", ".", "/", "/tmp/", "/tmp//x", "/tmp/./x", "/tmp/../x" };

	bootstrap_read_fixture(&f, 0);
	for (int i = 0; i < lengthof(bad); i++)
		for (int field = 0; field < 3; field++) {
			const char *paths[3] = { bootstrap_local, test_root, test_wal_root };
			paths[field] = bad[i];
			memset(&out, 0xa5, sizeof(out));
			UT_ASSERT_EQ(cluster_control_bootstrap_read(paths[0], paths[1], paths[2], 0, &out),
						 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
			UT_ASSERT(v2_zero(&out, sizeof(out)));
		}
	UT_ASSERT_EQ(cluster_control_bootstrap_read(bootstrap_local, test_root, test_wal_root, 0, NULL),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	memset(&out, 0, sizeof(out));
	strlcpy((char *)&out, bootstrap_local, sizeof(out));
	UT_ASSERT_EQ(cluster_control_bootstrap_read((char *)&out, test_root, test_wal_root, 0, &out),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	UT_ASSERT_EQ(bootstrap_root_opens, 0);
}

UT_TEST(test_bootstrap_read_pinned_directory_is_not_replacement)
{
	BootstrapFixture f;
	char current[MAXPGPATH], saved[MAXPGPATH];

	bootstrap_read_fixture(&f, 0);
	bootstrap_race = 6;
	UT_ASSERT_EQ(bootstrap_read_refused(0), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT_EQ(bootstrap_race, 0);
	path_for(current, sizeof(current), "global");
	path_for(saved, sizeof(saved), "global.bootstrap-saved");
	/* The replacement is our exact newly created empty test directory. */
	UT_ASSERT_EQ(rmdir(current), 0);
	UT_ASSERT_EQ(rename(saved, current), 0);
}

static void bootstrap_history_files(BootstrapFixture *f, uint32 node, uint32 count,
									char paths[3][MAXPGPATH]);

UT_TEST(test_bootstrap_read_close_failure_never_returns_partial_success)
{
	BootstrapFixture f;
	char paths[3][MAXPGPATH];
	ClusterControlBootstrapObservation out;
	ClusterControlRootResult result;
	int closes, open_before = 0, open_after = 0;

	bootstrap_read_fixture(&f, 0);
	bootstrap_history_files(&f, 127, 2, paths);
	result = cluster_control_bootstrap_read(bootstrap_local, test_root, test_wal_root, 0, &out);
	UT_ASSERT_EQ(result, 0);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return;
	closes = bootstrap_close_calls;
	UT_ASSERT(closes > 10);
	pfree(out.config_bytes);
	for (int fd = 0; fd < 256; fd++)
		if (fcntl(fd, F_GETFD) >= 0)
			open_before++;
	for (int i = 1; i <= closes; i++) {
		bootstrap_read_fixture(&f, 0);
		bootstrap_history_files(&f, 127, 2, paths);
		bootstrap_close_fail_at = i;
		UT_ASSERT_EQ(bootstrap_read_refused(0), CLUSTER_CONTROL_ROOT_IO_ERROR);
		UT_ASSERT(bootstrap_close_calls >= i);
	}
	for (int fd = 0; fd < 256; fd++)
		if (fcntl(fd, F_GETFD) >= 0)
			open_after++;
	UT_ASSERT_EQ(open_after, open_before);
	bootstrap_close_fail_at = 0;
}

/* PGRAC: real selected current/history files exercise complete sizing input.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static void
bootstrap_source_paths(const uint8 root_bytes[66048], uint32 node, char claim[MAXPGPATH],
					   char anchor[MAXPGPATH])
{
	ControlRootImage root;
	char hex[65];
	if (cluster_control_root_v2_decode(root_bytes, 66048, v2_storage, TEST_SYSID, &root))
		abort();
	v2_claim_path(&root.records[node].identity, claim);
	bootstrap_hex(root.refs[node].anchor_sha256, hex);
	snprintf(anchor, MAXPGPATH,
			 "%s/global/anchor_images/thread_%u/generation_" UINT64_FORMAT "/anchor_" UINT64_FORMAT
			 "-%s.bin",
			 test_root, node + 1, root.records[node].identity.origin_owner_incarnation,
			 root.refs[node].anchor_generation, hex);
}

static void
bootstrap_history_files(BootstrapFixture *f, uint32 node, uint32 count, char paths[3][MAXPGPATH])
{
	uint8 bytes[65604], source[66048];
	ControlRootImage root;
	char dir[MAXPGPATH], hex[65];
	size_t len = 64 + count * 512 + 4;

	memset(bytes, 0, sizeof(bytes));
	memcpy(bytes, "PGWH", 4);
	put_u16_le(bytes + 4, 1);
	put_u16_le(bytes + 6, 64);
	put_u32_le(bytes + 8, count);
	put_u32_le(bytes + 12, 512);
	put_u64_le(bytes + 16, count * 512);
	put_u64_le(bytes + 24, TEST_SYSID);
	memcpy(bytes + 32, f->before + 32, 32);
	for (uint32 i = 0; i < count; i++) {
		ClusterRecoveryAnchorV2 a = f->current_anchors[node == 0 ? 0 : 1];
		uint8 *record = source + 512 + node * 512;
		memcpy(source, f->before, sizeof(source));
		put_u64_le(record + 80, 1000 + i);
		put_u64_le(record + 24, 2000 + i);
		put_u64_le(record + 72, 3000 + i);
		memset(record + 216, 0, 40);
		record[10] = 1 + i % 5;
		v2_checksums(source);
		if (cluster_control_root_v2_decode(source, sizeof(source), v2_storage, TEST_SYSID, &root))
			abort();
		v2_claim_object(source, node, &root);
		a.identity = root.records[node].identity;
		memcpy(a.claim_sha256, root.refs[node].claim_sha256, 32);
		a.max_connections = 600 + i;
		a.max_worker_processes = 20 + i;
		a.max_wal_senders = 8 + i;
		a.max_prepared_xacts = i;
		a.max_locks_per_xact = 100 + i;
		/* Modes are not quantities to fold into a maximum. */
		a.wal_level = i % 3;
		a.wal_log_hints = (i % 2) != 0;
		a.track_commit_timestamp = !a.wal_log_hints;
		v2_anchor_object(source, &a, &a.identity, paths[2]);
		bootstrap_source_paths(source, node, paths[1], paths[2]);
		memcpy(bytes + 64 + i * 512, record, 512);
	}
	put_u32_le(bytes + len - 4, image_crc(bytes, len - 4));
	put_u64_le(f->before + 512 + node * 512 + 216, 123);
	sha256_bytes(bytes, len, f->before + 512 + node * 512 + 224);
	bootstrap_hex(f->before + 512 + node * 512 + 224, hex);
	path_for(dir, sizeof(dir), "global/wal_history");
	UT_ASSERT(mkdir(dir, 0700) == 0 || errno == EEXIST);
	snprintf(dir, sizeof(dir), "%s/global/wal_history/thread_%u", test_root, node + 1);
	UT_ASSERT(mkdir(dir, 0700) == 0 || errno == EEXIST);
	snprintf(paths[0], MAXPGPATH, "%s/history_123-%s.bin", dir, hex);
	write_all_or_abort(paths[0], bytes, len);
	v2_checksums(f->before);
	v2_write_roots(f->before);
}

UT_TEST(test_bootstrap_capacity_includes_every_current_and_retained_source)
{
	BootstrapFixture f;
	ClusterControlBootstrapObservation out;
	const uint32 counts[] = { 0, 2, 128 };
	char paths[3][MAXPGPATH];

	for (size_t i = 0; i < lengthof(counts); i++) {
		ClusterControlRootResult result;
		bootstrap_read_fixture(&f, 0);
		bootstrap_history_files(&f, 127, counts[i], paths);
		/* An unreferenced decoy may be broken and must not be enumerated. */
		path_for(paths[0], MAXPGPATH, "global/wal_history/thread_128/history_999-decoy.bin");
		write_all_or_abort(paths[0], (const uint8 *)"bad", 3);
		result = cluster_control_bootstrap_read(bootstrap_local, test_root, test_wal_root, 0, &out);
		UT_ASSERT_EQ(result, 0);
		if (result != 0)
			continue;
		UT_ASSERT_EQ(out.required.current_sources, 2);
		UT_ASSERT_EQ(out.required.history_sources, counts[i]);
		UT_ASSERT_EQ(out.required.max_connections, counts[i] == 0 ? 427 : 599 + counts[i]);
		UT_ASSERT_EQ(out.required.max_worker_processes, counts[i] == 0 ? 16 : 19 + counts[i]);
		UT_ASSERT_EQ(out.required.max_wal_senders, counts[i] == 0 ? 5 : 7 + counts[i]);
		UT_ASSERT_EQ(out.required.max_prepared_xacts, counts[i] == 0 ? 0 : counts[i] - 1);
		UT_ASSERT_EQ(out.required.max_locks_per_xact, counts[i] == 0 ? 64 : 99 + counts[i]);
		UT_ASSERT_EQ(out.snapshot.control.MaxConnections, 300);
		UT_ASSERT_EQ(out.snapshot.control.wal_level, 1);
		UT_ASSERT_EQ(test_cf_lock_calls, 0);
		UT_ASSERT_EQ(test_durable_rename_calls, 0);
		v2_assert_primary_unchanged(f.before);
		pfree(out.config_bytes);
	}
}

UT_TEST(test_bootstrap_capacity_does_not_skip_retired_or_unconfigured_origin)
{
	BootstrapFixture f;
	ClusterControlBootstrapObservation out;
	for (int state = 1; state <= 5; state++) {
		ClusterControlRootResult result;
		bootstrap_read_fixture(&f, 0);
		f.before[512 + 127 * 512 + 10] = state;
		put_u64_le(f.before + 224, 0); /* configured excludes remote origin */
		put_u64_le(f.before + 240, 0); /* serving excludes it too */
		v2_checksums(f.before);
		v2_install_config(f.before, false);
		v2_write_roots(f.before);
		result = cluster_control_bootstrap_read(bootstrap_local, test_root, test_wal_root, 0, &out);
		UT_ASSERT_EQ(result, 0);
		if (result == 0) {
			UT_ASSERT_EQ(out.required.current_sources, 2);
			UT_ASSERT_EQ(out.required.max_connections, 427);
			pfree(out.config_bytes);
		}
	}
}

UT_TEST(test_bootstrap_capacity_requires_nonlocal_and_history_objects)
{
	BootstrapFixture f;
	char paths[3][MAXPGPATH], saved[MAXPGPATH];
	for (int retained = 0; retained <= 1; retained++)
		for (int object = retained ? 0 : 1; object < 3; object++)
			for (int fault = 0; fault < 5; fault++) {
				bootstrap_read_fixture(&f, 0);
				if (retained)
					bootstrap_history_files(&f, 127, 2, paths);
				else
					bootstrap_source_paths(f.before, 127, paths[1], paths[2]);
				if (fault == 0)
					UT_ASSERT_EQ(unlink(paths[object]), 0);
				else if (fault == 1)
					write_all_or_abort(paths[object], (const uint8 *)"bad", 3);
				else if (fault == 2)
					UT_ASSERT_EQ(chmod(paths[object], 0666), 0);
				else {
					snprintf(saved, sizeof(saved), "%s.saved", paths[object]);
					UT_ASSERT_EQ(rename(paths[object], saved), 0);
					if (fault == 3)
						UT_ASSERT_EQ(symlink(saved, paths[object]), 0);
					else
						UT_ASSERT_EQ(mkfifo(paths[object], 0600), 0);
				}
				bootstrap_read_refused(0);
				if (fault == 2)
					UT_ASSERT_EQ(chmod(paths[object], 0600), 0);
				if (fault >= 3) {
					UT_ASSERT_EQ(unlink(paths[object]), 0);
					UT_ASSERT_EQ(rename(saved, paths[object]), 0);
				}
			}
}

UT_TEST(test_bootstrap_capacity_reobserves_after_collection)
{
	BootstrapFixture f;
	PgracControlBinding binding;
	char paths[3][MAXPGPATH];
	for (int object_failure = 0; object_failure <= 1; object_failure++)
		for (int race = 1; race <= 5; race++) {
			if (race == 2)
				continue; /* earlier missing-object hook already tested */
			bootstrap_read_fixture(&f, 0);
			bootstrap_history_files(&f, 127, 2, paths);
			if (object_failure)
				UT_ASSERT_EQ(unlink(paths[2]), 0);
			memcpy(bootstrap_replacement, f.before, sizeof(f.before));
			put_u64_le(bootstrap_replacement + 16, 8);
			if (race == 3)
				put_u32_le(bootstrap_replacement + 196,
						   CLUSTER_CONTROL_ROOT_DATABASE_MIGRATION_REVOKED);
			else if (race == 4)
				put_u64_le(bootstrap_replacement + 200, 42);
			v2_checksums(bootstrap_replacement);
			UT_ASSERT(pgrac_control_binding_decode(f.binding, sizeof(f.binding), &binding));
			binding.target_qualification_sha256[0] ^= 1;
			UT_ASSERT(pgrac_control_binding_encode(&binding, bootstrap_binding_replacement, 256));
			bootstrap_race = race;
			bootstrap_race_at = 3;
			UT_ASSERT_EQ(bootstrap_read_refused(0),
						 race == 3				  ? CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID
						 : race == 4 || race == 5 ? CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH
												  : CLUSTER_CONTROL_ROOT_STALE_TOKEN);
			UT_ASSERT_EQ(bootstrap_race, 0);
			UT_ASSERT_EQ(bootstrap_root_opens, 3);
		}
}

UT_TEST(test_bootstrap_capacity_checks_nonlocal_content_not_just_file_presence)
{
	BootstrapFixture f;
	char path[MAXPGPATH], paths[3][MAXPGPATH];
	for (int fault = 0; fault < 8; fault++) {
		ClusterRecoveryAnchorV2 anchor;
		bootstrap_read_fixture(&f, 0);
		anchor = f.current_anchors[1];
		switch (fault) {
		case 0:
			anchor.checkpoint_copy.redo++;
			break;
		case 1:
			anchor.checkpoint_copy.ThisTimeLineID++;
			break;
		case 2:
			anchor.backup_start = 1;
			break;
		case 3:
			anchor.backup_end = 1;
			break;
		case 4:
			/* The production encoder already refuses this unsupported input.
			 * Mutate encoded bytes below to exercise the consumer as well. */
			break;
		case 5:
			anchor.identity.system_identifier++;
			break;
		case 6:
			anchor.identity.origin_owner_incarnation++;
			break;
		case 7:
			anchor.claim_sha256[0] ^= 1;
			break;
		}
		v2_anchor_object(f.before, &anchor, &f.current_anchors[1].identity, path);
		if (fault == 4) {
			uint8 bytes[512];
			read_all_or_abort(path, bytes, sizeof(bytes));
			bytes[288] = 1;
			put_u32_le(bytes + 508, image_crc(bytes, 508));
			sha256_bytes(bytes, sizeof(bytes), f.before + 512 + 127 * 512 + 264);
			v2_checksums(f.before);
			bootstrap_source_paths(f.before, 127, paths[1], path);
			write_all_or_abort(path, bytes, sizeof(bytes));
		}
		v2_write_roots(f.before);
		UT_ASSERT_EQ(bootstrap_read_refused(0), fault >= 2 && fault <= 4
													? CLUSTER_CONTROL_ROOT_RANGE_INVALID
													: CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
	}
	for (int object = 0; object < 3; object++) {
		uint8 bytes[1092];
		size_t len = object == 0 ? sizeof(bytes) : object == 1 ? 112 : 512;
		bootstrap_read_fixture(&f, 0);
		bootstrap_history_files(&f, 127, 2, paths);
		read_all_or_abort(paths[object], bytes, len);
		bytes[40] ^= 1;
		write_all_or_abort(paths[object], bytes, len);
		bootstrap_read_refused(0);
	}
	bootstrap_read_fixture(&f, 0);
	bootstrap_source_paths(f.before, 127, paths[1], paths[2]);
	/* A native v1-length claim is not auto-upgraded using current identity. */
	write_all_or_abort(paths[1], f.claim, 40);
	UT_ASSERT_EQ(bootstrap_read_refused(0), CLUSTER_CONTROL_ROOT_BAD_SIZE);
}

UT_TEST(test_bootstrap_capacity_final_directory_replacement_is_not_pinned_success)
{
	BootstrapFixture f;
	char saved[MAXPGPATH], path[MAXPGPATH];
	bootstrap_read_fixture(&f, 0);
	bootstrap_race = 6;
	bootstrap_race_at = 3;
	UT_ASSERT_EQ(bootstrap_read_refused(0), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT_EQ(bootstrap_race, 0);
	path_for(path, sizeof(path), "global");
	path_for(saved, sizeof(saved), "global.bootstrap-saved");
	UT_ASSERT_EQ(rmdir(path), 0);
	UT_ASSERT_EQ(rename(saved, path), 0);
}

/* PGRAC: canonical history producer and actual immutable-file lifecycle.
 * Independent literal bytes above remain the expected encoding.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static void
startup_stage_fixture(ControlRootImage *root, ClusterWalStartupImage *input, uint8 bytes[1536],
					  uint32 phase)
{
	static uint64 sequence = 80000;
	char path[MAXPGPATH];
	const char *dirs[] = { "global/wal_startup", "global/wal_startup/thread_1",
						   "global/wal_startup/thread_1/.staging" };

	startup_fixture(bytes, root, phase);
	put_u64_le(bytes + 16, ++sequence);
	put_u64_le(bytes + 96, sequence);
	root->startup[0].generation = sequence;
	startup_checksum(bytes, root);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_decode(bytes, 1536, root, 0, input), 0);
	for (unsigned i = 0; i < lengthof(dirs); i++) {
		path_for(path, sizeof(path), dirs[i]);
		if (mkdir(path, 0700) != 0 && errno != EEXIST)
			abort();
	}
	test_cf_grant = test_cf_clusterwide = true;
	test_cf_mode = ExclusiveLock;
	test_checkpoint_mode = false;
	enableFsync = true;
	test_history_fail_sync = test_history_sync_count = 0;
}

static void
startup_stage_paths(const ClusterWalStartupStage *stage, char formal[MAXPGPATH],
					char temp[MAXPGPATH])
{
	char digest[65], uuid[33];
	for (unsigned i = 0; i < 32; i++)
		snprintf(digest + i * 2, 3, "%02x", stage->sha256[i]);
	for (unsigned i = 0; i < 16; i++)
		snprintf(uuid + i * 2, 3, "%02x", stage->operation_uuid[i]);
	snprintf(formal, MAXPGPATH, "%s/global/wal_startup/thread_%u/startup_%llu-%s.bin", test_root,
			 stage->origin_node + 1, (unsigned long long)stage->generation, digest);
	snprintf(temp, MAXPGPATH, "%s/global/wal_startup/thread_%u/.staging/%s.tmp", test_root,
			 stage->origin_node + 1, uuid);
}

UT_TEST(test_startup_file_install_preserves_literal_phases_without_selecting_root)
{
	for (uint32 phase = 1; phase <= 3; phase++) {
		ControlRootImage root, before;
		ClusterWalStartupImage input, got;
		ClusterWalStartupStage stage;
		uint8 bytes[1536], disk[1536];
		char formal[MAXPGPATH], temp[MAXPGPATH];

		startup_stage_fixture(&root, &input, bytes, phase);
		memset(&root.startup[0], 0, sizeof(root.startup[0]));
		before = root;
		UT_ASSERT_EQ(cluster_wal_startup_prepare(&root, 0, &input, &stage), 0);
		if (ut_current_failed)
			return;
		startup_stage_paths(&stage, formal, temp);
		UT_ASSERT_EQ(access(temp, F_OK), 0);
		UT_ASSERT_EQ(access(formal, F_OK), -1);
		UT_ASSERT_EQ(cluster_wal_history_install(&stage), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
		test_cf_mode = ShareLock;
		UT_ASSERT_EQ(cluster_wal_startup_install(&stage), CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
		test_cf_mode = ExclusiveLock;
		UT_ASSERT_EQ(cluster_wal_startup_install(&stage), 0);
		UT_ASSERT_EQ(cluster_wal_startup_install(&stage), 0);
		UT_ASSERT_EQ(access(temp, F_OK), -1);
		read_all_or_abort(formal, disk, sizeof(disk));
		UT_ASSERT(memcmp(disk, bytes, sizeof(bytes)) == 0);
		UT_ASSERT(memcmp(&root, &before, sizeof(root)) == 0);
		UT_ASSERT_EQ(cluster_wal_startup_read_locked(&root, 0, &got), CLUSTER_CONTROL_ROOT_ABSENT);
		UT_ASSERT(v2_zero(&got, sizeof(got)));
		root.startup[0].generation = stage.generation;
		memcpy(root.startup[0].sha256, stage.sha256, 32);
		UT_ASSERT_EQ(cluster_wal_startup_read_locked(&root, 0, &got), 0);
		UT_ASSERT(memcmp(&got, &input, sizeof(got)) == 0);
		UT_ASSERT_EQ(cluster_wal_startup_discard(&stage), 0);
		UT_ASSERT(v2_zero(&stage, sizeof(stage)));
		UT_ASSERT_EQ(access(formal, F_OK), 0);
	}
}

UT_TEST(test_startup_file_prepare_and_install_durability_cuts_keep_owned_evidence)
{
	for (unsigned cut = 1; cut <= 5; cut++) {
		ControlRootImage root;
		ClusterWalStartupImage input;
		ClusterWalStartupStage stage;
		uint8 bytes[1536];
		char formal[MAXPGPATH], temp[MAXPGPATH];

		startup_stage_fixture(&root, &input, bytes, 2);
		if (cut <= 2) {
			test_history_fail_sync = cut;
			UT_ASSERT_EQ(cluster_wal_startup_prepare(&root, 0, &input, &stage),
						 CLUSTER_CONTROL_ROOT_IO_ERROR);
			UT_ASSERT(v2_zero(&stage, sizeof(stage)));
			test_history_fail_sync = 0;
		}
		UT_ASSERT_EQ(cluster_wal_startup_prepare(&root, 0, &input, &stage), 0);
		if (ut_current_failed)
			return;
		startup_stage_paths(&stage, formal, temp);
		if (cut > 2) {
			test_history_sync_count = 0;
			test_history_fail_sync = cut - 2;
			UT_ASSERT_EQ(cluster_wal_startup_install(&stage), CLUSTER_CONTROL_ROOT_IO_ERROR);
			UT_ASSERT_EQ(access(formal, F_OK), 0);
			test_history_fail_sync = 0;
			if (cut != 4)
				UT_ASSERT_EQ(cluster_wal_startup_install(&stage), 0);
		}
		UT_ASSERT_EQ(cluster_wal_startup_discard(&stage), 0);
		UT_ASSERT_EQ(access(temp, F_OK), -1);
		UT_ASSERT_EQ(access(formal, F_OK), cut > 2 ? 0 : -1);
	}
}

UT_TEST(test_startup_file_read_only_needs_no_staging_or_fsync)
{
	ControlRootImage root;
	ClusterWalStartupImage input, got;
	ClusterWalStartupStage stage;
	uint8 bytes[1536];
	char formal[MAXPGPATH], temp[MAXPGPATH], dir[MAXPGPATH], saved[MAXPGPATH];

	startup_stage_fixture(&root, &input, bytes, 3);
	UT_ASSERT_EQ(cluster_wal_startup_prepare(&root, 0, &input, &stage), 0);
	if (ut_current_failed)
		return;
	UT_ASSERT_EQ(cluster_wal_startup_install(&stage), 0);
	startup_stage_paths(&stage, formal, temp);
	path_for(dir, sizeof(dir), "global/wal_startup/thread_1/.staging");
	path_for(saved, sizeof(saved), "global/wal_startup/thread_1/.staging.saved");
	UT_ASSERT_EQ(rename(dir, saved), 0);
	UT_ASSERT_EQ(chmod(formal, 0400), 0);
	test_cf_mode = ShareLock;
	test_history_sync_count = 0;
	test_history_fail_sync = 1;
	UT_ASSERT_EQ(cluster_wal_startup_read_locked(&root, 0, &got), 0);
	UT_ASSERT(memcmp(&got, &input, sizeof(got)) == 0);
	UT_ASSERT_EQ(test_history_sync_count, 0);
	test_history_fail_sync = 0;
	UT_ASSERT_EQ(chmod(formal, 0600), 0);
	UT_ASSERT_EQ(rename(saved, dir), 0);
	UT_ASSERT_EQ(cluster_wal_startup_discard(&stage), 0);
}

UT_TEST(test_startup_file_rejects_unselected_unsafe_and_changed_objects)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		ControlRootImage root;
		ClusterWalStartupImage input, got;
		ClusterWalStartupStage stage;
		uint8 bytes[1536];
		char formal[MAXPGPATH], temp[MAXPGPATH];

		startup_stage_fixture(&root, &input, bytes, 2);
		UT_ASSERT_EQ(cluster_wal_startup_prepare(&root, 0, &input, &stage), 0);
		if (ut_current_failed)
			return;
		UT_ASSERT_EQ(cluster_wal_startup_install(&stage), 0);
		startup_stage_paths(&stage, formal, temp);
		switch (fault) {
		case 0:
			root.startup[0].generation++;
			break;
		case 1:
			root.startup[0].sha256[0] ^= 1;
			break;
		case 2:
			root.present[0] = false;
			break;
		case 3:
			root.header.format_version = 2;
			break;
		case 4:
			test_cf_grant = false;
			break;
		case 5:
			test_cf_clusterwide = false;
			break;
		case 6:
			UT_ASSERT_EQ(chmod(formal, 0660), 0);
			break;
		case 7:
			UT_ASSERT_EQ(truncate(formal, 1535), 0);
			break;
		case 8:
			bytes[48] ^= 1;
			write_all_or_abort(formal, bytes, sizeof(bytes));
			break;
		case 9:
			UT_ASSERT_EQ(unlink(formal), 0);
			UT_ASSERT_EQ(symlink("missing-evidence", formal), 0);
			break;
		}
		memset(&got, 0xa5, sizeof(got));
		UT_ASSERT(cluster_wal_startup_read_locked(&root, 0, &got) != 0);
		UT_ASSERT(v2_zero(&got, sizeof(got)));
		test_cf_clusterwide = true;
		UT_ASSERT_EQ(cluster_wal_startup_discard(&stage), 0);
	}
}

UT_TEST(test_startup_file_replaced_stage_and_directory_cannot_be_deleted_or_adopted)
{
	ControlRootImage root;
	ClusterWalStartupImage input;
	ClusterWalStartupStage stage;
	uint8 bytes[1536];
	char formal[MAXPGPATH], temp[MAXPGPATH], saved[MAXPGPATH], dir[MAXPGPATH];

	startup_stage_fixture(&root, &input, bytes, 1);
	UT_ASSERT_EQ(cluster_wal_startup_prepare(&root, 0, &input, &stage), 0);
	if (ut_current_failed)
		return;
	startup_stage_paths(&stage, formal, temp);
	snprintf(saved, sizeof(saved), "%s.saved", temp);
	UT_ASSERT_EQ(rename(temp, saved), 0);
	write_all_or_abort(temp, bytes, sizeof(bytes));
	UT_ASSERT_EQ(cluster_wal_startup_install(&stage), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT_EQ(cluster_wal_startup_discard(&stage), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT_EQ(access(temp, F_OK), 0);
	UT_ASSERT_EQ(access(formal, F_OK), -1);
	UT_ASSERT_EQ(unlink(temp), 0);
	UT_ASSERT_EQ(rename(saved, temp), 0);
	path_for(dir, sizeof(dir), "global/wal_startup/thread_1/.staging");
	snprintf(saved, sizeof(saved), "%s.saved", dir);
	UT_ASSERT_EQ(rename(dir, saved), 0);
	UT_ASSERT_EQ(mkdir(dir, 0700), 0);
	UT_ASSERT_EQ(cluster_wal_startup_install(&stage), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT_EQ(cluster_wal_startup_discard(&stage), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT_EQ(rmdir(dir), 0);
	UT_ASSERT_EQ(rename(saved, dir), 0);
	/* Same formal name with different contents is not replaceable. */
	bytes[100] ^= 1;
	write_all_or_abort(formal, bytes, sizeof(bytes));
	UT_ASSERT_EQ(cluster_wal_startup_install(&stage), CLUSTER_CONTROL_ROOT_HASH_MISMATCH);
	UT_ASSERT_EQ(cluster_wal_startup_discard(&stage), 0);
	UT_ASSERT_EQ(access(formal, F_OK), 0);
}

UT_TEST(test_startup_file_invalid_owner_and_aliases_cannot_publish)
{
	ControlRootImage root;
	ClusterWalStartupImage input, got;
	ClusterWalStartupStage stage, other;
	uint8 bytes[1536];

	startup_stage_fixture(&root, &input, bytes, 1);
	UT_ASSERT_EQ(cluster_wal_startup_prepare(NULL, 0, &input, &stage),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_startup_prepare(&root, 128, &input, &stage),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_startup_prepare(&root, 0, NULL, &stage),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_startup_prepare(&root, 0, &input, NULL),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(v2_zero(&stage, sizeof(stage)));
	UT_ASSERT_EQ(cluster_wal_startup_read_locked(NULL, 0, &got),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_startup_read_locked(&root, 128, &got),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_startup_read_locked(&root, 0, NULL),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(v2_zero(&got, sizeof(got)));
	UT_ASSERT_EQ(cluster_wal_startup_prepare(&root, 0, &input, &stage), 0);
	if (ut_current_failed)
		return;
	UT_ASSERT_EQ(cluster_wal_startup_prepare(&root, 0, &input, &other),
				 CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT(v2_zero(&other, sizeof(other)));
	stage.owner_pid++;
	UT_ASSERT_EQ(cluster_wal_startup_install(&stage), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_startup_discard(&stage), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	stage.owner_pid--;
	enableFsync = false;
	UT_ASSERT_EQ(cluster_wal_startup_install(&stage), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_startup_discard(&stage), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	enableFsync = true;
	UT_ASSERT_EQ(cluster_wal_startup_prepare(&root, 0, &input,
											 (ClusterWalStartupStage *)input.operation_uuid),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(v2_zero(input.operation_uuid, sizeof(stage)));
	UT_ASSERT_EQ(cluster_wal_startup_discard(&stage), 0);
	UT_ASSERT_EQ(cluster_wal_startup_read_locked(&root, 0, (ClusterWalStartupImage *)root.bytes),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(v2_zero(root.bytes, sizeof(got)));
}

static void
history_stage_dirs(uint32 node)
{
	char path[MAXPGPATH];
	path_for(path, sizeof(path), "global/wal_history");
	if (mkdir(path, 0700) != 0 && errno != EEXIST)
		abort();
	snprintf(path, sizeof(path), "%s/global/wal_history/thread_%u", test_root, node + 1);
	if (mkdir(path, 0700) != 0 && errno != EEXIST)
		abort();
	strlcat(path, "/.staging", sizeof(path));
	if (mkdir(path, 0700) != 0 && errno != EEXIST)
		abort();
}

static void
history_stage_paths(const ClusterWalHistoryStage *stage, char formal[MAXPGPATH],
					char temp[MAXPGPATH])
{
	char digest[65], uuid[33];
	for (unsigned i = 0; i < 32; i++)
		snprintf(digest + i * 2, 3, "%02x", stage->sha256[i]);
	for (unsigned i = 0; i < 16; i++)
		snprintf(uuid + i * 2, 3, "%02x", stage->operation_uuid[i]);
	snprintf(formal, MAXPGPATH, "%s/global/wal_history/thread_%u/history_%llu-%s.bin", test_root,
			 stage->origin_node + 1, (unsigned long long)stage->generation, digest);
	snprintf(temp, MAXPGPATH, "%s/global/wal_history/thread_%u/.staging/%s.tmp", test_root,
			 stage->origin_node + 1, uuid);
}

static void
history_prepare_fixture(ControlRootImage *root, ClusterWalHistoryImage *history, uint8 uuid[16],
						uint8 bytes[CLUSTER_WAL_HISTORY_MAX_BYTES], uint32 count)
{
	static uint64 sequence = 10000;
	size_t len = history_fixture(bytes, root, 0, count);
	UT_ASSERT_EQ(cluster_control_root_v2_history_decode(bytes, len, root, 0, history), 0);
	memset(uuid, 0, 16);
	put_u64_le(uuid, ++sequence);
	history_stage_dirs(0);
	test_cf_grant = test_cf_clusterwide = true;
	test_cf_mode = ExclusiveLock;
	enableFsync = true;
	test_history_fail_sync = test_history_sync_count = 0;
}

UT_TEST(test_history_encoder_matches_literal_empty_and_full)
{
	ControlRootImage root;
	ClusterWalHistoryImage history;
	uint8 expected[CLUSTER_WAL_HISTORY_MAX_BYTES], actual[CLUSTER_WAL_HISTORY_MAX_BYTES];
	uint32 counts[] = { 0, 2, 128 };
	for (uint32 node = 0; node <= 127; node += 127)
		for (unsigned i = 0; i < lengthof(counts); i++) {
			size_t len, used = 777;
			memset(expected, 0, sizeof(expected));
			len = history_fixture(expected, &root, node, counts[i]);
			UT_ASSERT_EQ(
				cluster_control_root_v2_history_decode(expected, len, &root, node, &history), 0);
			memset(actual, 0xa5, sizeof(actual));
			UT_ASSERT_EQ(
				cluster_control_root_v2_history_encode(&root, node, &history, actual, &used), 0);
			UT_ASSERT_EQ(used, len);
			UT_ASSERT(memcmp(expected, actual, sizeof(actual)) == 0);
		}
}

/* Literal version-2 manifest: normal inputs and checkpoint-less terminal
 * references are disjoint, sorted sets, not one fake checkpoint sequence. */
static size_t
terminal_history_fixture(uint8 bytes[CLUSTER_WAL_HISTORY_MAX_BYTES], ControlRootImage *root)
{
	(void)history_fixture(bytes, root, 0, 1);
	root->header.format_version = 3;
	memmove(bytes + 96, bytes + 64, 512);
	memset(bytes + 64, 0, 32);
	put_u16_le(bytes + 4, 2);
	put_u16_le(bytes + 6, 96);
	put_u64_le(bytes + 16, 512 + 48);
	put_u32_le(bytes + 64, 1);
	put_u32_le(bytes + 68, 48);
	put_u64_le(bytes + 608, 2000);
	put_u64_le(bytes + 616, 1234);
	memset(bytes + 624, 0x36, 32);
	history_outer_checksum(bytes, 660, root, 0);
	return 660;
}

UT_TEST(test_terminal_history_literal_union_and_old_reader_refusal)
{
	uint8 bytes[CLUSTER_WAL_HISTORY_MAX_BYTES], encoded[CLUSTER_WAL_HISTORY_MAX_BYTES];
	ControlRootImage root;
	ClusterWalHistoryImage out;
	size_t used, len = terminal_history_fixture(bytes, &root);
	UT_ASSERT_EQ(cluster_control_root_v3_history_decode(bytes, len, &root, 0, &out), 0);
	if (ut_current_failed)
		return;
	UT_ASSERT_EQ(out.count, 1);
	UT_ASSERT_EQ(out.records[0].snapshot.identity.origin_owner_incarnation, 1000);
	UT_ASSERT_EQ(out.terminal_count, 1);
	UT_ASSERT_EQ(out.terminals[0].incarnation, 2000);
	UT_ASSERT_EQ(out.terminals[0].generation, 1234);
	UT_ASSERT(memcmp(out.terminals[0].sha256, bytes + 624, 32) == 0);
	UT_ASSERT_EQ(cluster_control_root_v3_history_encode(&root, 0, &out, encoded, &used), 0);
	UT_ASSERT_EQ(used, len);
	UT_ASSERT(memcmp(encoded, bytes, len) == 0);
	root.header.format_version = 2;
	UT_ASSERT_EQ(cluster_control_root_v2_history_decode(bytes, len, &root, 0, &out),
				 CLUSTER_CONTROL_ROOT_BAD_VERSION);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
}

UT_TEST(test_terminal_history_refuses_aliases_and_malformed_sets)
{
	for (unsigned fault = 0; fault < 11; fault++) {
		uint8 bytes[CLUSTER_WAL_HISTORY_MAX_BYTES];
		ControlRootImage root;
		ClusterWalHistoryImage out;
		size_t len = terminal_history_fixture(bytes, &root);
		switch (fault) {
		case 0:
			put_u64_le(bytes + 608, 1000);
			break;
		case 1:
			put_u64_le(bytes + 608, root.records[0].identity.origin_owner_incarnation);
			break;
		case 2:
			put_u64_le(bytes + 608, 0);
			break;
		case 3:
			put_u64_le(bytes + 616, 0);
			break;
		case 4:
			memset(bytes + 624, 0, 32);
			break;
		case 5:
			put_u32_le(bytes + 64, 128);
			break;
		case 6:
			put_u32_le(bytes + 68, 40);
			break;
		case 7:
			bytes[72] = 1;
			break;
		case 8:
			put_u64_le(bytes + 16, 512);
			break;
		case 9:
			put_u32_le(bytes + 64, 0);
			break;
		case 10:
			memcpy(bytes + 656, bytes + 608, 48);
			put_u32_le(bytes + 64, 2);
			put_u64_le(bytes + 16, 512 + 96);
			len += 48;
			break;
		}
		history_outer_checksum(bytes, len, &root, 0);
		UT_ASSERT(cluster_control_root_v3_history_decode(bytes, len, &root, 0, &out) != 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
	}
}

UT_TEST(test_terminal_history_real_file_install_and_selected_read)
{
	ControlRootImage root;
	ClusterWalHistoryImage history, observed;
	ClusterWalHistoryStage stage;
	uint8 uuid[16], bytes[CLUSTER_WAL_HISTORY_MAX_BYTES];
	history_prepare_fixture(&root, &history, uuid, bytes, 1);
	root.header.format_version = 3;
	history.terminal_count = 1;
	history.terminals[0].incarnation = 2000;
	history.terminals[0].generation = 1234;
	memset(history.terminals[0].sha256, 0x36, 32);
	UT_ASSERT_EQ(cluster_wal_history_prepare(&root, 0, &history, 7020, uuid, &stage), 0);
	UT_ASSERT_EQ(stage.length, 660);
	UT_ASSERT_EQ(cluster_wal_history_install(&stage), 0);
	if (ut_current_failed)
		return;
	root.refs[0].history_generation = stage.generation;
	memcpy(root.refs[0].history_sha256, stage.sha256, 32);
	UT_ASSERT_EQ(cluster_wal_history_read_locked(&root, 0, &observed), 0);
	UT_ASSERT_EQ(observed.count, 1);
	UT_ASSERT_EQ(observed.terminal_count, 1);
	UT_ASSERT(memcmp(observed.terminals, history.terminals, sizeof(history.terminals)) == 0);
	UT_ASSERT_EQ(cluster_wal_history_install(&stage), 0);
	UT_ASSERT_EQ(cluster_wal_history_discard(&stage), 0);
	UT_ASSERT_EQ(cluster_wal_history_read_locked(&root, 0, &observed), 0);
	{
		ClusterWalOriginInputs all;
		/* Metadata decoding alone cannot silently omit the selected terminal
		 * file. The complete-input consumer must actually read it. */
		UT_ASSERT_EQ(cluster_wal_origin_inputs_read_locked(&root, 0, &all),
					 CLUSTER_CONTROL_ROOT_ABSENT);
		UT_ASSERT(v2_zero(&all, sizeof(all)));
	}
}

UT_TEST(test_terminal_history_combined_capacity_does_not_evict_inputs)
{
	ControlRootImage root;
	ClusterWalHistoryImage history, readback;
	uint8 bytes[CLUSTER_WAL_HISTORY_MAX_BYTES];
	size_t used, len = history_fixture(bytes, &root, 0, 127);
	UT_ASSERT_EQ(cluster_control_root_v2_history_decode(bytes, len, &root, 0, &history), 0);
	root.header.format_version = 3;
	history.terminal_count = 1;
	history.terminals[0].incarnation = 2000;
	history.terminals[0].generation = 1234;
	memset(history.terminals[0].sha256, 0x36, 32);
	UT_ASSERT_EQ(cluster_control_root_v3_history_encode(&root, 0, &history, bytes, &used), 0);
	history_outer_checksum(bytes, used, &root, 0);
	UT_ASSERT_EQ(cluster_control_root_v3_history_decode(bytes, used, &root, 0, &readback), 0);
	UT_ASSERT_EQ(readback.count, 127);
	UT_ASSERT_EQ(readback.terminal_count, 1);
	history.terminal_count = 2;
	history.terminals[1] = history.terminals[0];
	history.terminals[1].incarnation++;
	UT_ASSERT_EQ(cluster_control_root_v3_history_encode(&root, 0, &history, bytes, &used),
				 CLUSTER_CONTROL_ROOT_RANGE_INVALID);
	UT_ASSERT_EQ(used, 0);
	UT_ASSERT(v2_zero(bytes, sizeof(bytes)));
	UT_ASSERT_EQ(history.count, 127);
	UT_ASSERT_EQ(history.terminal_count, 2);
	root.header.format_version = 2;
	UT_ASSERT_EQ(cluster_control_root_v2_history_encode(&root, 0, &history, bytes, &used),
				 CLUSTER_CONTROL_ROOT_BAD_VERSION);
}

UT_TEST(test_history_encoder_rejects_invalid_records_without_partial_output)
{
	ControlRootImage root;
	ClusterWalHistoryImage history;
	uint8 bytes[CLUSTER_WAL_HISTORY_MAX_BYTES];
	for (unsigned fault = 0; fault < 10; fault++) {
		size_t len = history_fixture(bytes, &root, 0, 2), used = 777;
		UT_ASSERT_EQ(cluster_control_root_v2_history_decode(bytes, len, &root, 0, &history), 0);
		switch (fault) {
		case 0:
			history.count = 129;
			break;
		case 1:
			history.records[1].snapshot.identity = history.records[0].snapshot.identity;
			break;
		case 2:
			history.records[0].snapshot.identity.origin_owner_incarnation = 99;
			break;
		case 3:
			history.records[0].refs.history_generation = 123;
			break;
		case 4:
			history.records[0].snapshot.identity.origin_node_id = 1;
			break;
		case 5:
			history.records[0].publisher_incarnation = 0;
			break;
		case 6:
			history.records[0].snapshot.lifecycle = 0;
			break;
		case 7:
			memset(history.records[0].refs.claim_sha256, 0, 32);
			break;
		case 8:
			root.header.format_version = 1;
			break;
		case 9:
			root.records[0].identity.origin_node_id = 1;
			break;
		}
		memset(bytes, 0xa5, sizeof(bytes));
		UT_ASSERT(cluster_control_root_v2_history_encode(&root, 0, &history, bytes, &used) != 0);
		UT_ASSERT_EQ(used, 0);
		UT_ASSERT(v2_zero(bytes, sizeof(bytes)));
	}
}

UT_TEST(test_history_stage_installs_exact_readable_object)
{
	ControlRootImage root;
	ClusterWalHistoryImage history, readback;
	ClusterWalHistoryStage stage;
	uint8 uuid[16], bytes[CLUSTER_WAL_HISTORY_MAX_BYTES], got[CLUSTER_WAL_HISTORY_MAX_BYTES];
	char formal[MAXPGPATH], temp[MAXPGPATH];
	history_prepare_fixture(&root, &history, uuid, bytes, 128);
	UT_ASSERT_EQ(cluster_wal_history_prepare(&root, 0, &history, 7001, uuid, &stage), 0);
	if (ut_current_failed)
		return;
	UT_ASSERT_EQ(stage.length, CLUSTER_WAL_HISTORY_HEADER_BYTES + 128 * 512 + 4);
	history_stage_paths(&stage, formal, temp);
	UT_ASSERT_EQ(access(temp, F_OK), 0);
	UT_ASSERT_EQ(access(formal, F_OK), -1);
	UT_ASSERT_EQ(cluster_wal_history_install(&stage), 0);
	if (ut_current_failed)
		return;
	UT_ASSERT_EQ(access(temp, F_OK), -1);
	read_all_or_abort(formal, got, stage.length);
	UT_ASSERT(memcmp(bytes, got, stage.length) == 0);
	root.refs[0].history_generation = stage.generation;
	memcpy(root.refs[0].history_sha256, stage.sha256, 32);
	UT_ASSERT_EQ(cluster_control_root_v2_history_decode(got, stage.length, &root, 0, &readback), 0);
	UT_ASSERT_EQ(readback.count, 128);
	UT_ASSERT_EQ(cluster_wal_history_install(&stage), 0);
	UT_ASSERT_EQ(cluster_wal_history_discard(&stage), 0);
	UT_ASSERT(v2_zero(&stage, sizeof(stage)));
	UT_ASSERT_EQ(access(formal, F_OK), 0);
}

UT_TEST(test_history_stage_lock_and_no_clobber)
{
	ControlRootImage root;
	ClusterWalHistoryImage history;
	ClusterWalHistoryStage first, second, duplicate;
	uint8 uuid[16], bytes[CLUSTER_WAL_HISTORY_MAX_BYTES];
	char first_path[MAXPGPATH], second_path[MAXPGPATH], temp[MAXPGPATH];
	history_prepare_fixture(&root, &history, uuid, bytes, 2);
	UT_ASSERT_EQ(cluster_wal_history_prepare(&root, 0, &history, 7002, uuid, &first), 0);
	if (ut_current_failed)
		return;
	test_cf_mode = ShareLock;
	UT_ASSERT_EQ(cluster_wal_history_install(&first), CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	test_cf_mode = ExclusiveLock;
	UT_ASSERT_EQ(cluster_wal_history_install(&first), 0);
	uuid[15]++;
	UT_ASSERT_EQ(cluster_wal_history_prepare(&root, 0, &history, 7002, uuid, &duplicate), 0);
	UT_ASSERT_EQ(cluster_wal_history_install(&duplicate), 0);
	uuid[15]++;
	history.records[1].snapshot.published_at_usec++;
	UT_ASSERT_EQ(cluster_wal_history_prepare(&root, 0, &history, 7002, uuid, &second), 0);
	UT_ASSERT_EQ(cluster_wal_history_install(&second), 0);
	history_stage_paths(&first, first_path, temp);
	history_stage_paths(&second, second_path, temp);
	UT_ASSERT(strcmp(first_path, second_path) != 0);
	UT_ASSERT_EQ(access(first_path, F_OK), 0);
	UT_ASSERT_EQ(access(second_path, F_OK), 0);
	UT_ASSERT_EQ(cluster_wal_history_discard(&first), 0);
	UT_ASSERT_EQ(cluster_wal_history_discard(&second), 0);
	UT_ASSERT_EQ(cluster_wal_history_discard(&duplicate), 0);
}

UT_TEST(test_history_stage_uncertain_sync_keeps_formal_and_can_finish)
{
	ControlRootImage root;
	ClusterWalHistoryImage history;
	ClusterWalHistoryStage stage;
	uint8 uuid[16], bytes[CLUSTER_WAL_HISTORY_MAX_BYTES];
	char formal[MAXPGPATH], temp[MAXPGPATH];
	for (unsigned cut = 1; cut <= 3; cut++) {
		history_prepare_fixture(&root, &history, uuid, bytes, 2);
		UT_ASSERT_EQ(cluster_wal_history_prepare(&root, 0, &history, 7100 + cut, uuid, &stage), 0);
		if (ut_current_failed)
			return;
		history_stage_paths(&stage, formal, temp);
		test_history_fail_sync = cut;
		UT_ASSERT_EQ(cluster_wal_history_install(&stage), CLUSTER_CONTROL_ROOT_IO_ERROR);
		UT_ASSERT_EQ(access(formal, F_OK), 0);
		test_history_fail_sync = 0;
		UT_ASSERT_EQ(cluster_wal_history_install(&stage), 0);
		UT_ASSERT_EQ(cluster_wal_history_discard(&stage), 0);
		UT_ASSERT_EQ(access(formal, F_OK), 0);
	}
}

UT_TEST(test_history_encoder_and_prepare_refuse_aliases)
{
	union {
		ControlRootImage root;
		uint8 bytes[CLUSTER_WAL_HISTORY_MAX_BYTES];
	} storage;
	ControlRootImage root;
	ClusterWalHistoryImage history;
	uint8 uuid[16], bytes[CLUSTER_WAL_HISTORY_MAX_BYTES];
	size_t used = 999;
	history_prepare_fixture(&root, &history, uuid, bytes, 2);
	storage.root = root;
	UT_ASSERT_EQ(
		cluster_control_root_v2_history_encode(&storage.root, 0, &history, storage.bytes, &used),
		CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(used, 0);
	UT_ASSERT_EQ(cluster_wal_history_prepare(&root, 0, &history, 7200, uuid,
											 (ClusterWalHistoryStage *)&history.records[3]),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	uuid[15]++;
	UT_ASSERT_EQ(cluster_wal_history_prepare(&root, 0, &history, 7201, uuid,
											 (ClusterWalHistoryStage *)root.bytes),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
}

UT_TEST(test_history_stage_refuses_corrupt_or_unsafe_formal_without_overwrite)
{
	ControlRootImage root;
	ClusterWalHistoryImage history;
	ClusterWalHistoryStage stage;
	uint8 uuid[16], bytes[CLUSTER_WAL_HISTORY_MAX_BYTES], got[CLUSTER_WAL_HISTORY_MAX_BYTES];
	char formal[MAXPGPATH], temp[MAXPGPATH];
	for (unsigned fault = 0; fault < 4; fault++) {
		history_prepare_fixture(&root, &history, uuid, bytes, 2);
		UT_ASSERT_EQ(cluster_wal_history_prepare(&root, 0, &history, 7210 + fault, uuid, &stage),
					 0);
		if (ut_current_failed)
			return;
		history_stage_paths(&stage, formal, temp);
		if (fault == 0) {
			bytes[0] ^= 1;
			write_all_or_abort(formal, bytes, stage.length);
		} else if (fault == 1) {
			UT_ASSERT_EQ(symlink(temp, formal), 0);
		} else if (fault == 2) {
			UT_ASSERT_EQ(mkfifo(formal, 0600), 0);
		} else {
			write_all_or_abort(formal, bytes, stage.length);
			UT_ASSERT_EQ(chmod(formal, 0660), 0);
		}
		UT_ASSERT(cluster_wal_history_install(&stage) != 0);
		UT_ASSERT_EQ(access(temp, F_OK), 0);
		if (fault == 0) {
			read_all_or_abort(formal, got, stage.length);
			UT_ASSERT(memcmp(got, bytes, stage.length) == 0);
		}
		UT_ASSERT_EQ(cluster_wal_history_discard(&stage), 0);
		/* Only this test's deliberately unsafe entry; never a product cleanup. */
		UT_ASSERT_EQ(unlink(formal), 0);
	}
}

UT_TEST(test_history_stage_refuses_replaced_temporary_and_directory)
{
	ControlRootImage root;
	ClusterWalHistoryImage history;
	ClusterWalHistoryStage stage;
	uint8 uuid[16], bytes[CLUSTER_WAL_HISTORY_MAX_BYTES];
	char formal[MAXPGPATH], temp[MAXPGPATH], saved[MAXPGPATH], dir[MAXPGPATH];
	history_prepare_fixture(&root, &history, uuid, bytes, 2);
	UT_ASSERT_EQ(cluster_wal_history_prepare(&root, 0, &history, 7220, uuid, &stage), 0);
	if (ut_current_failed)
		return;
	history_stage_paths(&stage, formal, temp);
	snprintf(saved, sizeof(saved), "%s.saved", temp);
	UT_ASSERT_EQ(rename(temp, saved), 0);
	write_all_or_abort(temp, bytes, stage.length);
	UT_ASSERT_EQ(cluster_wal_history_install(&stage), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT_EQ(cluster_wal_history_discard(&stage), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT_EQ(access(temp, F_OK), 0);
	UT_ASSERT_EQ(unlink(temp), 0);
	UT_ASSERT_EQ(rename(saved, temp), 0);
	path_for(dir, sizeof(dir), "global/wal_history/thread_1/.staging");
	snprintf(saved, sizeof(saved), "%s.saved", dir);
	UT_ASSERT_EQ(rename(dir, saved), 0);
	UT_ASSERT_EQ(mkdir(dir, 0700), 0);
	UT_ASSERT_EQ(cluster_wal_history_install(&stage), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT_EQ(cluster_wal_history_discard(&stage), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT_EQ(rmdir(dir), 0);
	UT_ASSERT_EQ(rename(saved, dir), 0);
	UT_ASSERT_EQ(cluster_wal_history_install(&stage), 0);
	UT_ASSERT_EQ(cluster_wal_history_discard(&stage), 0);
}

UT_TEST(test_history_stage_bad_owner_duplicate_and_disabled_sync)
{
	ControlRootImage root;
	ClusterWalHistoryImage history;
	ClusterWalHistoryStage stage, duplicate;
	uint8 uuid[16], bytes[CLUSTER_WAL_HISTORY_MAX_BYTES];
	char formal[MAXPGPATH], temp[MAXPGPATH];
	history_prepare_fixture(&root, &history, uuid, bytes, 2);
	UT_ASSERT_EQ(cluster_wal_history_prepare(&root, 0, &history, 7230, uuid, &stage), 0);
	if (ut_current_failed)
		return;
	history_stage_paths(&stage, formal, temp);
	UT_ASSERT_EQ(cluster_wal_history_prepare(&root, 0, &history, 7231, uuid, &duplicate),
				 CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT(v2_zero(&duplicate, sizeof(duplicate)));
	UT_ASSERT_EQ(access(temp, F_OK), 0);
	stage.owner_pid++;
	UT_ASSERT_EQ(cluster_wal_history_install(&stage), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_history_discard(&stage), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	stage.owner_pid--;
	enableFsync = false;
	UT_ASSERT_EQ(cluster_wal_history_install(&stage), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_history_discard(&stage), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	enableFsync = true;
	UT_ASSERT_EQ(cluster_wal_history_discard(&stage), 0);
	UT_ASSERT_EQ(access(temp, F_OK), -1);
}

UT_TEST(test_history_prepare_refuses_unsafe_dirs_and_invalid_parameters)
{
	ControlRootImage root;
	ClusterWalHistoryImage history;
	ClusterWalHistoryStage stage;
	uint8 uuid[16], bytes[CLUSTER_WAL_HISTORY_MAX_BYTES], zero[16] = { 0 };
	char dir[MAXPGPATH], saved[MAXPGPATH];
	history_prepare_fixture(&root, &history, uuid, bytes, 2);
	UT_ASSERT_EQ(cluster_wal_history_prepare(&root, 0, &history, 0, uuid, &stage),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_history_prepare(&root, 0, &history, 7235, zero, &stage),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_history_prepare(NULL, 0, &history, 7235, uuid, &stage),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_history_prepare(&root, 128, &history, 7235, uuid, &stage),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	enableFsync = false;
	UT_ASSERT_EQ(cluster_wal_history_prepare(&root, 0, &history, 7235, uuid, &stage),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	enableFsync = true;
	path_for(dir, sizeof(dir), "global/wal_history/thread_1/.staging");
	snprintf(saved, sizeof(saved), "%s.saved", dir);
	UT_ASSERT_EQ(chmod(dir, 0770), 0);
	UT_ASSERT(cluster_wal_history_prepare(&root, 0, &history, 7235, uuid, &stage) != 0);
	UT_ASSERT(v2_zero(&stage, sizeof(stage)));
	UT_ASSERT_EQ(chmod(dir, 0700), 0);
	UT_ASSERT_EQ(rename(dir, saved), 0);
	UT_ASSERT_EQ(symlink(saved, dir), 0);
	UT_ASSERT(cluster_wal_history_prepare(&root, 0, &history, 7235, uuid, &stage) != 0);
	UT_ASSERT_EQ(unlink(dir), 0);
	UT_ASSERT_EQ(rename(saved, dir), 0);
	UT_ASSERT_EQ(cluster_wal_history_prepare(&root, 0, &history, 7235, uuid, &stage), 0);
	UT_ASSERT_EQ(cluster_wal_history_discard(&stage), 0);
}

UT_TEST(test_history_stage_changed_bytes_refuse_without_install)
{
	ControlRootImage root;
	ClusterWalHistoryImage history;
	ClusterWalHistoryStage stage;
	uint8 uuid[16], bytes[CLUSTER_WAL_HISTORY_MAX_BYTES];
	char formal[MAXPGPATH], temp[MAXPGPATH];
	history_prepare_fixture(&root, &history, uuid, bytes, 2);
	UT_ASSERT_EQ(cluster_wal_history_prepare(&root, 0, &history, 7236, uuid, &stage), 0);
	if (ut_current_failed)
		return;
	history_stage_paths(&stage, formal, temp);
	bytes[100] ^= 1;
	write_all_or_abort(temp, bytes, stage.length);
	UT_ASSERT_EQ(cluster_wal_history_install(&stage), CLUSTER_CONTROL_ROOT_HASH_MISMATCH);
	UT_ASSERT_EQ(access(formal, F_OK), -1);
	UT_ASSERT_EQ(cluster_wal_history_discard(&stage), 0);
}

UT_TEST(test_history_discard_sync_failure_cannot_resurrect_install)
{
	ControlRootImage root;
	ClusterWalHistoryImage history;
	ClusterWalHistoryStage stage;
	uint8 uuid[16], bytes[CLUSTER_WAL_HISTORY_MAX_BYTES];
	char formal[MAXPGPATH], temp[MAXPGPATH];
	history_prepare_fixture(&root, &history, uuid, bytes, 0);
	UT_ASSERT_EQ(cluster_wal_history_prepare(&root, 0, &history, 7240, uuid, &stage), 0);
	if (ut_current_failed)
		return;
	history_stage_paths(&stage, formal, temp);
	test_history_fail_sync = 1;
	UT_ASSERT_EQ(cluster_wal_history_discard(&stage), CLUSTER_CONTROL_ROOT_IO_ERROR);
	test_history_fail_sync = 0;
	UT_ASSERT_EQ(cluster_wal_history_install(&stage), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(access(temp, F_OK), -1);
	UT_ASSERT_EQ(access(formal, F_OK), -1);
	UT_ASSERT_EQ(cluster_wal_history_discard(&stage), 0);
}

UT_TEST(test_history_prepare_sync_failure_clears_output_and_owned_temp)
{
	ControlRootImage root;
	ClusterWalHistoryImage history;
	ClusterWalHistoryStage stage;
	uint8 uuid[16], bytes[CLUSTER_WAL_HISTORY_MAX_BYTES];
	char formal[MAXPGPATH], temp[MAXPGPATH];
	for (unsigned cut = 1; cut <= 2; cut++) {
		history_prepare_fixture(&root, &history, uuid, bytes, 2);
		memset(&stage, 0xa5, sizeof(stage));
		test_history_fail_sync = cut;
		UT_ASSERT_EQ(cluster_wal_history_prepare(&root, 0, &history, 7250 + cut, uuid, &stage),
					 CLUSTER_CONTROL_ROOT_IO_ERROR);
		UT_ASSERT(v2_zero(&stage, sizeof(stage)));
		test_history_fail_sync = 0;
		/* Same operation UUID succeeds only if its own failed temp was removed. */
		UT_ASSERT_EQ(cluster_wal_history_prepare(&root, 0, &history, 7250 + cut, uuid, &stage), 0);
		if (ut_current_failed)
			return;
		history_stage_paths(&stage, formal, temp);
		UT_ASSERT_EQ(cluster_wal_history_discard(&stage), 0);
		UT_ASSERT_EQ(access(temp, F_OK), -1);
	}
}

UT_TEST(test_history_produced_file_reaches_actual_bootstrap_consumer)
{
	BootstrapFixture f;
	ClusterControlBootstrapObservation out;
	ControlRootImage root;
	ClusterWalHistoryImage history;
	ClusterWalHistoryStage stage;
	uint8 bytes[CLUSTER_WAL_HISTORY_MAX_BYTES], uuid[16] = { 0x91 };
	char paths[3][MAXPGPATH];
	size_t len = 64 + 2 * 512 + 4;
	bootstrap_read_fixture(&f, 0);
	bootstrap_history_files(&f, 127, 2, paths);
	read_all_or_abort(paths[0], bytes, len);
	UT_ASSERT_EQ(
		cluster_control_root_v2_decode(f.before, sizeof(f.before), v2_storage, TEST_SYSID, &root),
		0);
	UT_ASSERT_EQ(cluster_control_root_v2_history_decode(bytes, len, &root, 127, &history), 0);
	/* Remove only our raw fixture so the reader must consume the real producer. */
	UT_ASSERT_EQ(unlink(paths[0]), 0);
	history_stage_dirs(127);
	test_cf_grant = test_cf_clusterwide = true;
	test_cf_mode = ExclusiveLock;
	UT_ASSERT_EQ(cluster_wal_history_prepare(&root, 127, &history, 123, uuid, &stage), 0);
	if (ut_current_failed)
		return;
	UT_ASSERT(memcmp(stage.sha256, root.refs[127].history_sha256, 32) == 0);
	UT_ASSERT_EQ(cluster_wal_history_install(&stage), 0);
	UT_ASSERT_EQ(cluster_wal_history_discard(&stage), 0);
	test_cf_grant = false;
	test_cf_mode = NoLock;
	UT_ASSERT_EQ(cluster_control_bootstrap_read(bootstrap_local, test_root, test_wal_root, 0, &out),
				 0);
	UT_ASSERT_EQ(out.required.history_sources, 2);
	test_cf_grant = true;
}

/* PGRAC: retained inputs have their own physical claim, clean anchor and
 * persistent prefix. No decoder or close predicate is replaced by a stub.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static void
v2_close_history_fixture(uint8 before[66048], ClusterPhase1FullStopPlan *plan,
						 ClusterWalHistoryImage *history, ClusterWalHistoryStage *stage,
						 uint32 count)
{
	ControlRootImage root, old;
	ClusterRecoveryAnchorV2 anchor;
	ClusterWalDurablePrefixRef saved;
	ClusterWalDurablePrefix saved_prefix;
	uint8 bytes[66048], uuid[16] = { 0 };
	char saved_path[MAXPGPATH], path[MAXPGPATH];
	static uint64 generation = 30000;

	v2_close_fixture(before, plan);
	saved = test_checkpoint_prefix_ref;
	saved_prefix = test_checkpoint_prefix;
	strlcpy(saved_path, test_checkpoint_prefix_path, sizeof(saved_path));
	anchor = v2_stop_clean_anchor(before, &saved.claim.identity);
	UT_ASSERT_EQ(cluster_control_root_v2_decode(before, 66048, v2_storage, TEST_SYSID, &root), 0);
	memset(history, 0, sizeof(*history));
	history->count = count;
	for (uint32 i = 0; i < count; i++) {
		ClusterRecoveryAnchorV2 historical = anchor;
		ControlFileData native = { 0 };
		old = root;
		old.records[0].identity.origin_owner_incarnation -= count - i;
		old.records[0].identity.thread_claim_created_at -= count - i;
		old.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
		old.records[0].lifecycle_reason = CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_CLEAN_CLOSE;
		old.publisher_incarnation[0] = old.records[0].identity.origin_owner_incarnation;
		UT_ASSERT_EQ(cluster_control_root_v2_encode(&old), 0);
		memcpy(bytes, old.bytes, sizeof(bytes));
		v2_claim_object(bytes, 0, &old);
		historical.identity = old.records[0].identity;
		memcpy(historical.claim_sha256, old.refs[0].claim_sha256, 32);
		v2_anchor_object(bytes, &historical, &historical.identity, path);
		UT_ASSERT_EQ(
			cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &old), 0);
		test_checkpoint_prefix_ref = saved;
		test_checkpoint_prefix_ref.claim.identity = historical.identity;
		memcpy(test_checkpoint_prefix_ref.claim.claim_sha256, historical.claim_sha256, 32);
		snprintf(path, sizeof(path), "%s/thread_1/generation_" UINT64_FORMAT "/durable_prefix",
				 cluster_wal_threads_dir, historical.identity.origin_owner_incarnation);
		UT_ASSERT(mkdir(path, 0700) == 0 || errno == EEXIST);
		snprintf(test_checkpoint_prefix_path, sizeof(test_checkpoint_prefix_path), "%s/current",
				 path);
		native.state = DB_SHUTDOWNED;
		native.checkPoint = historical.checkpoint;
		native.checkPointCopy = historical.checkpoint_copy;
		v2_checkpoint_wal_record(&historical.identity, &native, 0);
		old.records[0].checkpoint_record_crc32c = test_checkpoint_crc;
		old.records[0].tail_last_record_crc32c = test_checkpoint_crc;
		old.records[0].validated_tail_lsn_exclusive = test_checkpoint_end;
		history->records[i].snapshot = old.records[0];
		history->records[i].refs = old.refs[0];
		history->records[i].publisher_incarnation = old.publisher_incarnation[0];
		history->records[i].publisher_node = 0;
	}
	test_checkpoint_prefix_ref = saved;
	test_checkpoint_prefix = saved_prefix;
	strlcpy(test_checkpoint_prefix_path, saved_path, sizeof(test_checkpoint_prefix_path));
	history_stage_dirs(0);
	put_u64_le(uuid, ++generation);
	test_actual_cf = ExclusiveLock;
	UT_ASSERT_EQ(cluster_wal_history_prepare(&root, 0, history, generation, uuid, stage), 0);
	UT_ASSERT_EQ(cluster_wal_history_install(stage), 0);
	root.refs[0].history_generation = stage->generation;
	memcpy(root.refs[0].history_sha256, stage->sha256, 32);
	UT_ASSERT_EQ(cluster_control_root_v2_encode(&root), 0);
	memcpy(before, root.bytes, sizeof(root.bytes));
	v2_write_roots(before);
	test_actual_cf = NoLock;
}

UT_TEST(test_v2_normal_close_authenticates_retained_clean_generations)
{
	for (uint32 count = 0; count <= 2; count += 2) {
		uint8 before[66048];
		ClusterPhase1FullStopPlan plan;
		ClusterWalHistoryImage history;
		ClusterWalHistoryStage stage;
		ControlRootImage root;
		ClusterControlRootFileToken token;
		ControlFileData view;
		bool complete = false;
		v2_close_history_fixture(before, &plan, &history, &stage, count);
		UT_ASSERT_EQ(cluster_control_root_v2_normal_stop_close(&plan, &complete), 0);
		UT_ASSERT(complete);
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		test_actual_cf = ShareLock;
		UT_ASSERT_EQ(cluster_control_root_v2_read_thread_locked(
						 &test_checkpoint_prefix_ref.claim.identity, &root, &view, &token),
					 0);
		UT_ASSERT_EQ(root.refs[0].history_generation, stage.generation);
		UT_ASSERT(memcmp(root.refs[0].history_sha256, stage.sha256, 32) == 0);
		test_actual_cf = NoLock;
	}
}

UT_TEST(test_v2_normal_close_rechecks_closed_peer_persistent_tail)
{
	uint8 before[66048], primary[66048], prefix_bytes[CLUSTER_WAL_DURABLE_PREFIX_BYTES];
	ClusterPhase1FullStopPlan plan;
	ClusterWalDurablePrefixRef peer;
	ClusterWalDurablePrefix prefix;
	char path[MAXPGPATH];
	bool complete = true;
	v2_close_fixture(before, &plan);
	peer = v2_close_add_peer(before, 1, &plan);
	UT_ASSERT_EQ(cluster_control_root_v2_normal_stop_close(&plan, &complete), 0);
	UT_ASSERT(!complete);
	/* The first publisher has already closed node0. A later valid PGWP must
	 * not be ignored when node1 considers the global-close fold. */
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	read_all_or_abort(path, primary, sizeof(primary));
	UT_ASSERT_EQ(cluster_wal_durable_prefix_read(cluster_wal_threads_dir,
												 &test_checkpoint_prefix_ref, &prefix),
				 0);
	prefix.sequence++;
	prefix.exclusive_end += 8;
	UT_ASSERT_EQ(
		cluster_wal_durable_prefix_encode(&test_checkpoint_prefix_ref, &prefix, prefix_bytes), 0);
	write_all_or_abort(test_checkpoint_prefix_path, prefix_bytes, sizeof(prefix_bytes));
	cluster_node_id = 1;
	test_own_thread = 2;
	test_self_incarnation = test_membership_incarnation
		= peer.claim.identity.origin_owner_incarnation;
	test_checkpoint_prefix_ref = peer;
	plan.own_wal_started_at = peer.claim.identity.thread_claim_created_at;
	UT_ASSERT(cluster_control_root_v2_normal_stop_close(&plan, &complete) != 0);
	UT_ASSERT(!complete);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	v2_assert_primary_unchanged(primary);
}

UT_TEST(test_history_selected_reader_is_read_only_and_needs_no_staging)
{
	uint32 counts[] = { 0, 2, 128 };
	for (unsigned i = 0; i < lengthof(counts); i++) {
		ControlRootImage root;
		ClusterWalHistoryImage expected, got;
		ClusterWalHistoryStage stage;
		uint8 uuid[16], bytes[CLUSTER_WAL_HISTORY_MAX_BYTES];
		char formal[MAXPGPATH], temp[MAXPGPATH], dir[MAXPGPATH], saved[MAXPGPATH];
		unsigned syncs;
		test_checkpoint_mode = false;
		history_prepare_fixture(&root, &expected, uuid, bytes, counts[i]);
		UT_ASSERT_EQ(cluster_wal_history_prepare(&root, 0, &expected, 40000 + i, uuid, &stage), 0);
		UT_ASSERT_EQ(cluster_wal_history_install(&stage), 0);
		root.refs[0].history_generation = stage.generation;
		memcpy(root.refs[0].history_sha256, stage.sha256, 32);
		history_stage_paths(&stage, formal, temp);
		path_for(dir, sizeof(dir), "global/wal_history/thread_1/.staging");
		path_for(saved, sizeof(saved), "global/wal_history/thread_1/.staging-reader-test");
		UT_ASSERT_EQ(rename(dir, saved), 0);
		UT_ASSERT_EQ(chmod(formal, 0400), 0);
		test_cf_mode = ShareLock;
		syncs = test_history_sync_count;
		UT_ASSERT_EQ(cluster_wal_history_read_locked(&root, 0, &got), 0);
		UT_ASSERT_EQ(got.count, counts[i]);
		UT_ASSERT(memcmp(&expected, &got, sizeof(got)) == 0);
		UT_ASSERT_EQ(test_history_sync_count, syncs);
		UT_ASSERT_EQ(chmod(formal, 0600), 0);
		UT_ASSERT_EQ(rename(saved, dir), 0);
		test_cf_mode = ExclusiveLock;
		UT_ASSERT_EQ(cluster_wal_history_discard(&stage), 0);
	}
}

UT_TEST(test_history_selected_reader_refuses_unsafe_or_unselected_inputs)
{
	for (int fault = 0; fault < 9; fault++) {
		ControlRootImage root;
		ClusterWalHistoryImage expected, got;
		ClusterWalHistoryStage stage;
		uint8 uuid[16], bytes[CLUSTER_WAL_HISTORY_MAX_BYTES];
		char formal[MAXPGPATH], temp[MAXPGPATH];
		test_checkpoint_mode = false;
		history_prepare_fixture(&root, &expected, uuid, bytes, 2);
		UT_ASSERT_EQ(cluster_wal_history_prepare(&root, 0, &expected, 40100 + fault, uuid, &stage),
					 0);
		UT_ASSERT_EQ(cluster_wal_history_install(&stage), 0);
		root.refs[0].history_generation = stage.generation;
		memcpy(root.refs[0].history_sha256, stage.sha256, 32);
		history_stage_paths(&stage, formal, temp);
		switch (fault) {
		case 0:
			UT_ASSERT_EQ(unlink(formal), 0);
			break;
		case 1:
			UT_ASSERT_EQ(truncate(formal, CLUSTER_WAL_HISTORY_MAX_BYTES + 1), 0);
			break;
		case 2:
			UT_ASSERT_EQ(unlink(formal), 0);
			UT_ASSERT_EQ(symlink("missing-evidence", formal), 0);
			break;
		case 3:
			bytes[stage.length - 1] ^= 1;
			write_all_or_abort(formal, bytes, stage.length);
			break;
		case 4:
			root.refs[0].history_generation++;
			break;
		case 5:
			test_cf_clusterwide = false;
			break;
		case 6:
			UT_ASSERT_EQ(chmod(formal, 0660), 0);
			break;
		case 7:
			root.present[0] = false;
			break;
		case 8:
			root.refs[0].history_generation = 0;
			break;
		}
		memset(&got, 0xa5, sizeof(got));
		UT_ASSERT(cluster_wal_history_read_locked(&root, 0, &got) != 0);
		UT_ASSERT(v2_zero(&got, sizeof(got)));
		test_cf_clusterwide = true;
		if (fault == 6)
			UT_ASSERT_EQ(chmod(formal, 0600), 0);
		UT_ASSERT_EQ(cluster_wal_history_discard(&stage), 0);
	}
}

UT_TEST(test_v2_normal_close_refuses_missing_or_nonterminal_history)
{
	for (int fault = 0; fault < 8; fault++) {
		uint8 before[66048], prefix_bytes[CLUSTER_WAL_DURABLE_PREFIX_BYTES];
		ClusterPhase1FullStopPlan plan;
		ClusterWalHistoryImage history;
		ClusterWalHistoryStage stage;
		ClusterWalHistoryRecord *old;
		ClusterWalDurablePrefixRef ref;
		ClusterWalDurablePrefix prefix;
		char path[MAXPGPATH], temp[MAXPGPATH], hex[65];
		bool complete = true;
		v2_close_history_fixture(before, &plan, &history, &stage, 2);
		old = &history.records[0];
		ref = test_checkpoint_prefix_ref;
		ref.claim.identity = old->snapshot.identity;
		memcpy(ref.claim.claim_sha256, old->refs.claim_sha256, 32);
		if (fault == 0) {
			history_stage_paths(&stage, path, temp);
			UT_ASSERT_EQ(unlink(path), 0);
		} else if (fault == 1) {
			v2_claim_path(&old->snapshot.identity, path);
			UT_ASSERT_EQ(unlink(path), 0);
		} else if (fault == 2) {
			for (int i = 0; i < 32; i++)
				snprintf(hex + i * 2, 3, "%02x", old->refs.anchor_sha256[i]);
			snprintf(path, sizeof(path),
					 "%s/global/anchor_images/thread_1/generation_" UINT64_FORMAT
					 "/anchor_" UINT64_FORMAT "-%s.bin",
					 test_root, old->snapshot.identity.origin_owner_incarnation,
					 old->refs.anchor_generation, hex);
			UT_ASSERT_EQ(unlink(path), 0);
		} else if (fault < 6) {
			snprintf(path, sizeof(path),
					 "%s/thread_1/generation_" UINT64_FORMAT "/durable_prefix/current",
					 cluster_wal_threads_dir, old->snapshot.identity.origin_owner_incarnation);
			if (fault == 3)
				UT_ASSERT_EQ(unlink(path), 0);
			else {
				UT_ASSERT_EQ(
					cluster_wal_durable_prefix_read(cluster_wal_threads_dir, &ref, &prefix), 0);
				if (fault == 5)
					prefix.exclusive_end += 8;
				UT_ASSERT_EQ(cluster_wal_durable_prefix_encode(&ref, &prefix, prefix_bytes), 0);
				if (fault == 4)
					prefix_bytes[sizeof(prefix_bytes) - 1] ^= 1;
				write_all_or_abort(path, prefix_bytes, sizeof(prefix_bytes));
			}
		} else {
			ControlRootImage root;
			uint8 uuid[16] = { 0 };
			uint64 generation = stage.generation + 100000;
			old->snapshot.lifecycle = fault == 6 ? CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
												 : CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
			UT_ASSERT_EQ(cluster_control_root_v2_decode(before, sizeof(before), v2_storage,
														TEST_SYSID, &root),
						 0);
			put_u64_le(uuid, generation);
			test_actual_cf = ExclusiveLock;
			UT_ASSERT_EQ(cluster_wal_history_prepare(&root, 0, &history, generation, uuid, &stage),
						 0);
			UT_ASSERT_EQ(cluster_wal_history_install(&stage), 0);
			root.refs[0].history_generation = generation;
			memcpy(root.refs[0].history_sha256, stage.sha256, 32);
			UT_ASSERT_EQ(cluster_control_root_v2_encode(&root), 0);
			memcpy(before, root.bytes, sizeof(before));
			v2_write_roots(before);
			test_actual_cf = NoLock;
		}
		UT_ASSERT(cluster_control_root_v2_normal_stop_close(&plan, &complete) != 0);
		UT_ASSERT(!complete);
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		v2_assert_primary_unchanged(before);
	}
}

UT_TEST(test_v2_normal_close_pair_preserves_other_origin_history)
{
	uint8 before[66048];
	ClusterPhase1FullStopPlan plan;
	ClusterWalHistoryImage history;
	ClusterWalHistoryStage stage;
	ClusterWalDurablePrefixRef peer;
	bool complete = true;
	v2_close_history_fixture(before, &plan, &history, &stage, 2);
	peer = v2_close_add_peer(before, 3, &plan);
	UT_ASSERT_EQ(cluster_control_root_v2_normal_stop_close(&plan, &complete), 0);
	UT_ASSERT(!complete);
	cluster_node_id = 3;
	test_own_thread = 4;
	test_self_incarnation = test_membership_incarnation
		= peer.claim.identity.origin_owner_incarnation;
	test_checkpoint_prefix_ref = peer;
	plan.own_wal_started_at = peer.claim.identity.thread_claim_created_at;
	UT_ASSERT_EQ(cluster_control_root_v2_normal_stop_close(&plan, &complete), 0);
	UT_ASSERT(complete);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
}

/* Explicit format conversion exists only in this fixture. Production needs
 * the separately qualified cold migration, never a decoder-side upgrade. */
static void
root_fixture_version3(uint8 bytes[66048])
{
	put_u16_le(bytes + 4, 3);
	put_u16_le(bytes + 72, 3);
	put_u16_le(bytes + 74, 3);
	for (unsigned node = 0; node < 128; node++)
		if (!v2_zero(bytes + 512 + node * 512, 512))
			put_u16_le(bytes + 512 + node * 512 + 4, 3);
	v2_checksums(bytes);
}

UT_TEST(test_v3_history_keeps_flat_input_separate_from_pending_operation)
{
	const uint32 counts[] = { 0, 2, 128 };
	for (unsigned i = 0; i < lengthof(counts); i++) {
		uint8 bytes[CLUSTER_WAL_HISTORY_MAX_BYTES], encoded[CLUSTER_WAL_HISTORY_MAX_BYTES];
		ControlRootImage root, before;
		ClusterWalHistoryImage expected, got;
		size_t len = history_fixture(bytes, &root, 0, counts[i]), used = 0;

		UT_ASSERT_EQ(cluster_control_root_v2_history_decode(bytes, len, &root, 0, &expected), 0);
		root.header.format_version = 3;
		root.header.v2.serving[0] &= ~UINT64_C(1);
		root.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
		root.startup[0].generation = 500;
		memset(root.startup[0].sha256, 0x55, 32);
		before = root;
		UT_ASSERT_EQ(cluster_control_root_v3_history_decode(bytes, len, &root, 0, &got), 0);
		UT_ASSERT(memcmp(&got, &expected, sizeof(got)) == 0);
		UT_ASSERT_EQ(cluster_control_root_v3_history_encode(&root, 0, &expected, encoded, &used),
					 0);
		UT_ASSERT_EQ(used, len);
		UT_ASSERT(memcmp(bytes, encoded, len) == 0);
		UT_ASSERT(v2_zero(encoded + len, sizeof(encoded) - len));
		UT_ASSERT(memcmp(&before, &root, sizeof(root)) == 0);
		UT_ASSERT_EQ(cluster_control_root_v2_history_decode(bytes, len, &root, 0, &got),
					 CLUSTER_CONTROL_ROOT_BAD_VERSION);
		UT_ASSERT(v2_zero(&got, sizeof(got)));
		root.header.format_version = 2;
		UT_ASSERT_EQ(cluster_control_root_v3_history_decode(bytes, len, &root, 0, &got),
					 CLUSTER_CONTROL_ROOT_BAD_VERSION);
		UT_ASSERT_EQ(cluster_control_root_v3_history_encode(&root, 0, &expected, encoded, &used),
					 CLUSTER_CONTROL_ROOT_BAD_VERSION);
		UT_ASSERT_EQ(used, 0);
		UT_ASSERT(v2_zero(encoded, sizeof(encoded)));
	}
}

UT_TEST(test_v3_history_files_preserve_pending_and_reject_unknown_format)
{
	ControlRootImage root, before;
	ClusterWalHistoryImage expected, got;
	ClusterWalHistoryStage stage;
	uint8 uuid[16], bytes[CLUSTER_WAL_HISTORY_MAX_BYTES];
	char formal[MAXPGPATH], temp[MAXPGPATH];

	history_prepare_fixture(&root, &expected, uuid, bytes, 2);
	root.header.format_version = 3;
	root.startup[0].generation = 70001;
	memset(root.startup[0].sha256, 0x71, 32);
	before = root;
	UT_ASSERT_EQ(cluster_wal_history_prepare(&root, 0, &expected, 70002, uuid, &stage), 0);
	if (ut_current_failed)
		return;
	history_stage_paths(&stage, formal, temp);
	UT_ASSERT_EQ(cluster_wal_history_install(&stage), 0);
	UT_ASSERT(memcmp(&root, &before, sizeof(root)) == 0);
	root.refs[0].history_generation = stage.generation;
	memcpy(root.refs[0].history_sha256, stage.sha256, 32);
	test_cf_mode = ShareLock;
	UT_ASSERT_EQ(cluster_wal_history_read_locked(&root, 0, &got), 0);
	UT_ASSERT(memcmp(&got, &expected, sizeof(got)) == 0);
	UT_ASSERT_EQ(root.startup[0].generation, before.startup[0].generation);
	UT_ASSERT(memcmp(root.startup[0].sha256, before.startup[0].sha256, 32) == 0);
	UT_ASSERT_EQ(cluster_wal_history_discard(&stage), 0);
	UT_ASSERT_EQ(access(formal, F_OK), 0);
	for (unsigned version = 0; version <= 4; version++) {
		if (version == 2 || version == 3)
			continue;
		root.header.format_version = version;
		UT_ASSERT_EQ(cluster_wal_history_read_locked(&root, 0, &got),
					 CLUSTER_CONTROL_ROOT_BAD_VERSION);
		UT_ASSERT(v2_zero(&got, sizeof(got)));
		UT_ASSERT_EQ(cluster_wal_history_prepare(&root, 0, &expected, 70003, uuid, &stage),
					 CLUSTER_CONTROL_ROOT_BAD_VERSION);
		UT_ASSERT(v2_zero(&stage, sizeof(stage)));
	}
}

UT_TEST(test_v3_locked_read_preserves_pending_and_current_projection)
{
	uint8 bytes[66048], hash[32];
	ClusterRecoveryAnchorV2 anchors[2];
	ControlRootImage root;
	ControlFileData out;
	ClusterControlRootFileToken token;
	uint8 *peer = bytes + 512 + 127 * 512;

	v2_thread_fixture(bytes, anchors);
	root_fixture_version3(bytes);
	/* The unrelated predecessor is non-serving, yet its pending reference
	 * must survive this ordinary current-thread observation and token. */
	put_u64_le(bytes + 240, 0); /* serving high half; configured roster unchanged */
	peer[10] = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	put_u64_le(peer + 328, 501);
	memset(peer + 336, 0x66, 32);
	v2_checksums(bytes);
	UT_ASSERT_EQ(
		cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &root), 0);
	v2_write_roots(bytes);
	test_cf_mode = ShareLock;
	UT_ASSERT_EQ(
		cluster_control_root_v3_read_thread_locked(&anchors[0].identity, &root, &out, &token), 0);
	UT_ASSERT_EQ(out.checkPoint, anchors[0].checkpoint);
	UT_ASSERT_EQ(out.MaxConnections, 300);
	UT_ASSERT_EQ(token.format_version, 3);
	UT_ASSERT_EQ(root.startup[127].generation, 501);
	UT_ASSERT_EQ(root.header.v2.serving[1], 0);
	UT_ASSERT(memcmp(root.bytes, bytes, sizeof(bytes)) == 0);
	sha256_bytes(bytes, sizeof(bytes), hash);
	UT_ASSERT(memcmp(token.image_sha256, hash, 32) == 0);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
	UT_ASSERT_EQ(
		cluster_control_root_v2_read_thread_locked(&anchors[0].identity, &root, &out, &token),
		CLUSTER_CONTROL_ROOT_BAD_VERSION);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
}

UT_TEST(test_v3_locked_read_requires_exact_format_primary_and_objects)
{
	uint8 bytes[66048];
	ClusterRecoveryAnchorV2 anchors[2];
	ControlRootImage root;
	ControlFileData out;
	ClusterControlRootFileToken token;
	char path[MAXPGPATH];

	v2_thread_fixture(bytes, anchors);
	UT_ASSERT_EQ(
		cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &token),
		CLUSTER_CONTROL_ROOT_BAD_VERSION);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
	root_fixture_version3(bytes);
	v2_write_roots(bytes);
	UT_ASSERT_EQ(
		cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &token),
		0);
	if (ut_current_failed)
		return;
	test_cf_grant = false;
	UT_ASSERT_EQ(
		cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &token),
		CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
	test_cf_grant = true;
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	bytes[300] ^= 1;
	write_all_or_abort(path, bytes, sizeof(bytes));
	UT_ASSERT_EQ(
		cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &token),
		CLUSTER_CONTROL_ROOT_OK_BAK_BLOCKED);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
	bytes[300] ^= 1;
	v2_write_roots(bytes);
	v2_claim_path(&anchors[0].identity, path);
	UT_ASSERT_EQ(unlink(path), 0);
	UT_ASSERT_EQ(
		cluster_control_root_v3_read_thread_locked(&anchors[0].identity, &root, &out, &token),
		CLUSTER_CONTROL_ROOT_ABSENT);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
}

UT_TEST(test_bootstrap_v3_reads_exact_current_and_flat_history)
{
	BootstrapFixture f;
	ClusterControlBootstrapSnapshot snapshot;
	ClusterControlBootstrapObservation out;
	char paths[3][MAXPGPATH];
	uint8 hash[32];

	bootstrap_read_fixture(&f, 0);
	bootstrap_history_files(&f, 127, 2, paths);
	root_fixture_version3(f.before);
	memcpy(f.after, f.before, sizeof(f.after));
	v2_write_roots(f.before);
	UT_ASSERT_EQ(cluster_control_bootstrap_decode(&f.input, &snapshot), 0);
	sha256_bytes(f.before, sizeof(f.before), hash);
	UT_ASSERT(memcmp(hash, snapshot.root_sha256, 32) == 0);
	UT_ASSERT_EQ(snapshot.control.checkPoint, f.local_anchor.checkpoint);
	UT_ASSERT_EQ(cluster_control_bootstrap_read(bootstrap_local, test_root, test_wal_root, 0, &out),
				 0);
	if (ut_current_failed)
		return;
	UT_ASSERT_EQ(out.required.current_sources, 2);
	UT_ASSERT_EQ(out.required.history_sources, 2);
	UT_ASSERT_EQ(out.required.pending_sources, 0);
	UT_ASSERT_EQ(out.required.max_connections, 601);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
	pfree(out.config_bytes);
	/* No reinterpretation of a new version through an older second observation. */
	put_u16_le(f.after + 4, 4);
	v2_checksums(f.after);
	UT_ASSERT_EQ(bootstrap_refused(&f.input), CLUSTER_CONTROL_ROOT_BAD_VERSION);
}

/* Actual producer/consumer integration. Independent PGWG fixtures above test
 * the representation; here real selected claims/anchors are not stubbed. */
static void
bootstrap_pending_fixture(BootstrapFixture *f, uint32 phase, char paths[3][MAXPGPATH])
{
	ControlRootImage root, next;
	ClusterWalStartupImage operation = { 0 };
	ClusterWalStartupStage stage;
	ClusterWalThreadClaimRefV2 claim_ref = { 0 };
	ClusterRecoveryAnchorV2 anchor;
	uint8 bytes[66048];
	char temp[MAXPGPATH];
	char history_paths[3][MAXPGPATH];
	static uint64 generation = 90000;

	bootstrap_read_fixture(f, 0);
	bootstrap_history_files(f, 127, 2, history_paths);
	/* A foreign, non-serving predecessor remains selected until INSTALL. */
	f->before[512 + 127 * 512 + 10] = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	put_u32_le(f->before + 512 + 127 * 512 + 108, 0x8d);
	put_u32_le(f->before + 512 + 127 * 512 + 100, 1);
	put_u64_le(f->before + 512 + 127 * 512 + 120, f->current_anchors[1].checkpoint_copy.redo + 128);
	put_u64_le(f->before + 512 + 127 * 512 + 176, f->current_anchors[1].checkpoint_copy.redo);
	put_u16_le(f->before + 512 + 127 * 512 + 192, 1);
	put_u64_le(f->before + 240, 0);
	v2_checksums(f->before);
	UT_ASSERT_EQ(
		cluster_control_root_v2_decode(f->before, sizeof(f->before), v2_storage, TEST_SYSID, &root),
		0);
	operation.sealed_input_end = root.records[127].validated_tail_lsn_exclusive;
	operation.segment_size = 0x1000000;
	operation.first_segment_lsn = (operation.sealed_input_end + 0xffffff) & ~UINT64_C(0xffffff);
	operation.timeline = operation.input_timeline = root.records[127].checkpoint_tli;
	next = root;
	next.records[127].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	next.records[127].identity.origin_owner_incarnation = ++generation;
	next.records[127].identity.root_lineage_seq = generation;
	next.records[127].identity.thread_claim_created_at = generation;
	next.records[127].checkpoint_lower_lsn = operation.first_segment_lsn + SizeOfXLogLongPHD;
	next.records[127].validated_tail_lsn_exclusive = next.records[127].checkpoint_lower_lsn + 128;
	next.records[127].tail_last_record_lsn = next.records[127].checkpoint_lower_lsn;
	next.records[127].root_flags = 0x8d;
	next.records[127].tail_tli = operation.timeline;
	next.records[127].tail_validation_kind = CLUSTER_CONTROL_ROOT_TAIL_WAL_RECORD_SCAN_V1;
	next.refs[127].history_generation = 0;
	memset(next.refs[127].history_sha256, 0, 32);
	next.refs[127].anchor_generation = generation;
	UT_ASSERT_EQ(cluster_control_root_v2_encode(&next), 0);
	memcpy(bytes, next.bytes, sizeof(bytes));
	v2_claim_object(bytes, 127, &next);
	claim_ref.identity = next.records[127].identity;
	claim_ref.database_incarnation = next.header.v2.database_incarnation;
	claim_ref.max_config_generation = next.header.v2.config_generation;
	memcpy(claim_ref.claim_sha256, next.refs[127].claim_sha256, 32);
	UT_ASSERT_EQ(cluster_wal_claim_v2_read(test_wal_root, &claim_ref, &operation.claim), 0);
	v2_claim_path(&claim_ref.identity, paths[1]);
	anchor = f->current_anchors[1];
	anchor.identity = claim_ref.identity;
	anchor.anchor_generation = generation;
	memcpy(anchor.claim_sha256, claim_ref.claim_sha256, 32);
	anchor.checkpoint = anchor.checkpoint_copy.redo = next.records[127].checkpoint_lower_lsn;
	anchor.min_recovery_point = 0;
	anchor.min_recovery_tli = 0;
	anchor.max_connections = 901;
	v2_anchor_object(bytes, &anchor, &anchor.identity, paths[2]);
	UT_ASSERT_EQ(
		cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &next), 0);
	root_fixture_version3(f->before);
	sha256_bytes(f->before, sizeof(f->before), operation.predecessor_file_sha256);
	put_u64_le(f->before + 16, root.header.file_txn_seq + 1);
	v2_checksums(f->before);
	UT_ASSERT_EQ(
		cluster_control_root_v3_decode(f->before, sizeof(f->before), v2_storage, TEST_SYSID, &root),
		0);
	operation.phase = phase;
	operation.input_kind = CLUSTER_WAL_STARTUP_CLEAN;
	memset(operation.operation_uuid, 0x73, 16);
	put_u64_le(operation.operation_uuid, generation);
	operation.database_incarnation = root.header.v2.database_incarnation;
	operation.config_generation = operation.claim.config_generation;
	operation.formation_epoch = root.header.v2.formation_seq;
	operation.predecessor_file_sequence = root.header.file_txn_seq - 1;
	operation.generation = generation;
	memset(operation.predecessor_evidence_sha256, 0x47, 32);
	operation.input_record_start = root.records[127].checkpoint_lower_lsn;
	operation.input_record_end = operation.input_record_start + 128;
	operation.input_record_crc = root.records[127].checkpoint_record_crc32c;
	operation.predecessor.snapshot = root.records[127];
	operation.predecessor.refs = root.refs[127];
	operation.predecessor.publisher_node = root.publisher_node[127];
	operation.predecessor.publisher_incarnation = root.publisher_incarnation[127];
	if (phase == CLUSTER_WAL_STARTUP_DURABLE) {
		operation.successor.snapshot = next.records[127];
		operation.successor.refs = next.refs[127];
		operation.successor.publisher_node = next.publisher_node[127];
		operation.successor.publisher_incarnation = next.publisher_incarnation[127];
		operation.prefix.sequence = 2;
		operation.prefix.record_start = next.records[127].tail_last_record_lsn;
		operation.prefix.exclusive_end = next.records[127].validated_tail_lsn_exclusive;
		operation.prefix.record_crc = next.records[127].tail_last_record_crc32c;
		operation.prefix_timeline = operation.timeline;
	}
	path_for(temp, sizeof(temp), "global/wal_startup");
	UT_ASSERT(mkdir(temp, 0700) == 0 || errno == EEXIST);
	path_for(temp, sizeof(temp), "global/wal_startup/thread_128");
	UT_ASSERT(mkdir(temp, 0700) == 0 || errno == EEXIST);
	path_for(temp, sizeof(temp), "global/wal_startup/thread_128/.staging");
	UT_ASSERT(mkdir(temp, 0700) == 0 || errno == EEXIST);
	test_cf_grant = test_cf_clusterwide = true;
	test_cf_mode = ExclusiveLock;
	UT_ASSERT_EQ(cluster_wal_startup_prepare(&root, 127, &operation, &stage), 0);
	if (ut_current_failed)
		return;
	UT_ASSERT_EQ(cluster_wal_startup_install(&stage), 0);
	startup_stage_paths(&stage, paths[0], temp);
	root.startup[127].generation = stage.generation;
	memcpy(root.startup[127].sha256, stage.sha256, 32);
	UT_ASSERT_EQ(cluster_wal_startup_discard(&stage), 0);
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0);
	memcpy(f->before, root.bytes, sizeof(f->before));
	memcpy(f->after, f->before, sizeof(f->after));
	v2_write_roots(f->before);
	test_cf_grant = false;
	test_cf_mode = NoLock;
}

/* PGRAC: actual selected immutable files, not synthetic recovery authority.
 * Author: SqlRush <sqlrush@gmail.com> */
UT_TEST(test_origin_input_union_preserves_current_history_and_pending)
{
	for (uint32 phase = 1; phase <= 3; ++phase) {
		BootstrapFixture f;
		ControlRootImage root, before;
		ClusterWalOriginInputs out;
		ClusterWalHistoryImage retained;
		ClusterWalStartupImage pending;
		char paths[3][MAXPGPATH];
		bootstrap_pending_fixture(&f, phase, paths);
		UT_ASSERT_EQ(cluster_control_root_v3_decode(f.before, sizeof(f.before), v2_storage,
													TEST_SYSID, &root),
					 0);
		test_cf_grant = true;
		test_cf_mode = ShareLock;
		before = root;
		UT_ASSERT_EQ(cluster_wal_history_read_locked(&root, 127, &retained), 0);
		UT_ASSERT_EQ(cluster_wal_startup_read_locked(&root, 127, &pending), 0);
		UT_ASSERT_EQ(cluster_wal_origin_inputs_read_locked(&root, 127, &out), 0);
		if (ut_current_failed)
			return;
		UT_ASSERT_EQ(out.current.snapshot.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED);
		UT_ASSERT(memcmp(&out.current.snapshot, &root.records[127], sizeof(out.current.snapshot))
				  == 0);
		UT_ASSERT_EQ(out.current.publisher_node, root.publisher_node[127]);
		UT_ASSERT(memcmp(&out.history, &retained, sizeof(retained)) == 0);
		UT_ASSERT_EQ(out.history.count, 2);
		UT_ASSERT(out.has_pending);
		UT_ASSERT(memcmp(&out.pending, &pending, sizeof(pending)) == 0);
		UT_ASSERT_EQ(out.pending.phase, phase);
		UT_ASSERT(out.pending.claim.identity.origin_owner_incarnation
				  != out.current.snapshot.identity.origin_owner_incarnation);
		if (phase != CLUSTER_WAL_STARTUP_DURABLE)
			UT_ASSERT(v2_zero(&out.pending.successor, sizeof(out.pending.successor)));
		UT_ASSERT(memcmp(&root, &before, sizeof(root)) == 0);
		/* Current-only node0 does not inherit another origin's obligation. */
		UT_ASSERT_EQ(cluster_wal_origin_inputs_read_locked(&root, 0, &out), 0);
		UT_ASSERT(!out.has_pending && out.history.count == 0);
		UT_ASSERT(v2_zero(&out.pending, sizeof(out.pending)));
	}
}

UT_TEST(test_origin_input_union_refuses_partial_or_unowned_metadata)
{
	for (int fault = 0; fault < 8; ++fault) {
		BootstrapFixture f;
		ControlRootImage root;
		ClusterWalOriginInputs out;
		char paths[3][MAXPGPATH], history_path[MAXPGPATH], digest[65];
		bootstrap_pending_fixture(&f, CLUSTER_WAL_STARTUP_INITIALIZING, paths);
		UT_ASSERT_EQ(cluster_control_root_v3_decode(f.before, sizeof(f.before), v2_storage,
													TEST_SYSID, &root),
					 0);
		test_cf_grant = true;
		test_cf_mode = ShareLock;
		switch (fault) {
		case 0:
			test_cf_grant = false;
			break;
		case 1:
			root.header.format_version = 2;
			break;
		case 2:
			root.present[127] = false;
			break;
		case 3:
			UT_ASSERT_EQ(unlink(paths[0]), 0);
			break;
		case 4:
			root.startup[127].sha256[0] ^= 1;
			break;
		case 5:
			root.startup[127].generation = 0;
			break;
		case 6:
			root.refs[127].history_generation = 0;
			break;
		case 7:
			bootstrap_hex(root.refs[127].history_sha256, digest);
			snprintf(history_path, sizeof(history_path),
					 "%s/global/wal_history/thread_128/history_" UINT64_FORMAT "-%s.bin", test_root,
					 root.refs[127].history_generation, digest);
			UT_ASSERT_EQ(unlink(history_path), 0);
			break;
		}
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT(cluster_wal_origin_inputs_read_locked(&root, 127, &out) != 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
	}
}

UT_TEST(test_origin_input_union_alias_and_bounds_clear_all_output)
{
	BootstrapFixture f;
	union {
		ControlRootImage root;
		ClusterWalOriginInputs out;
	} storage;
	ClusterWalOriginInputs out;
	char paths[3][MAXPGPATH];
	bootstrap_pending_fixture(&f, CLUSTER_WAL_STARTUP_DURABLE, paths);
	UT_ASSERT_EQ(cluster_control_root_v3_decode(f.before, sizeof(f.before), v2_storage, TEST_SYSID,
												&storage.root),
				 0);
	test_cf_grant = true;
	test_cf_mode = ShareLock;
	UT_ASSERT_EQ(cluster_wal_origin_inputs_read_locked(NULL, 0, &out),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	UT_ASSERT_EQ(cluster_wal_origin_inputs_read_locked(&storage.root, 128, &out),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	UT_ASSERT_EQ(cluster_wal_origin_inputs_read_locked(&storage.root, 127, NULL),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_origin_inputs_read_locked(&storage.root, 127, &storage.out),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(v2_zero(&storage.out, sizeof(storage.out)));
}

static void
bootstrap_initializing_wal_fixture(BootstrapFixture *f, bool parameters, char paths[3][MAXPGPATH])
{
	uint8 operation_bytes[CLUSTER_WAL_STARTUP_BYTES], page[XLOG_BLCKSZ] = { 0 };
	ControlRootImage root;
	ClusterWalStartupImage operation;
	ClusterWalDurablePrefixRef ref = { 0 };
	ClusterWalDurablePrefix prefix = { 1, 0, 0, 0 };
	uint8 encoded[CLUSTER_WAL_DURABLE_PREFIX_BYTES];
	char dir[MAXPGPATH], path[MAXPGPATH];
	bootstrap_pending_fixture(f, CLUSTER_WAL_STARTUP_INITIALIZING, paths);
	UT_ASSERT_EQ(
		cluster_control_root_v3_decode(f->before, sizeof(f->before), v2_storage, TEST_SYSID, &root),
		0);
	read_all_or_abort(paths[0], operation_bytes, sizeof(operation_bytes));
	UT_ASSERT_EQ(cluster_control_root_v3_startup_decode(operation_bytes, sizeof(operation_bytes),
														&root, 127, &operation),
				 0);
	ref.claim.identity = operation.claim.identity;
	ref.claim.database_incarnation = operation.database_incarnation;
	ref.claim.max_config_generation = operation.config_generation;
	sha256_bytes(operation_bytes + 1280, CLUSTER_WAL_CLAIM_V2_BYTES, ref.claim.claim_sha256);
	ref.timeline = operation.timeline;
	snprintf(dir, sizeof(dir), "%s/thread_128/generation_" UINT64_FORMAT "/durable_prefix",
			 test_wal_root, ref.claim.identity.origin_owner_incarnation);
	UT_ASSERT(mkdir(dir, 0700) == 0 || errno == EEXIST);
	if (parameters) {
		XLogLongPageHeaderData header = { 0 };
		XLogRecord record = { 0 };
		xl_parameter_change payload = { 1401, 31, 17, 9, 101, WAL_LEVEL_REPLICA, false, false };
		uint8 *bytes = page + SizeOfXLogLongPHD;
		header.std.xlp_magic = XLOG_PAGE_MAGIC;
		header.std.xlp_info = XLP_LONG_HEADER;
		header.std.xlp_tli = ref.timeline;
		header.std.xlp_thread_id = 128;
		header.std.xlp_pageaddr = operation.first_segment_lsn;
		header.xlp_sysid = TEST_SYSID;
		header.xlp_seg_size = operation.segment_size;
		header.xlp_xlog_blcksz = XLOG_BLCKSZ;
		memcpy(page, &header, SizeOfXLogLongPHD);
		record.xl_tot_len = SizeOfXLogRecord + 2 + sizeof(payload);
		record.xl_info = XLOG_PARAMETER_CHANGE;
		record.xl_rmid = RM_XLOG_ID;
		bytes[SizeOfXLogRecord] = XLR_BLOCK_ID_DATA_SHORT;
		bytes[SizeOfXLogRecord + 1] = sizeof(payload);
		memcpy(bytes + SizeOfXLogRecord + 2, &payload, sizeof(payload));
		INIT_CRC32C(record.xl_crc);
		COMP_CRC32C(record.xl_crc, bytes + SizeOfXLogRecord, 2 + sizeof(payload));
		COMP_CRC32C(record.xl_crc, &record, offsetof(XLogRecord, xl_crc));
		FIN_CRC32C(record.xl_crc);
		memcpy(bytes, &record, SizeOfXLogRecord);
		wal_segment_size = operation.segment_size;
		v2_checkpoint_wal_path(&ref.claim.identity, operation.first_segment_lsn, ref.timeline,
							   path);
		write_all_or_abort(path, page, sizeof(page));
	}
	UT_ASSERT_EQ(cluster_wal_durable_prefix_encode(&ref, &prefix, encoded), 0);
	snprintf(path, sizeof(path), "%s/current", dir);
	write_all_or_abort(path, encoded, sizeof(encoded));
}

UT_TEST(test_bootstrap_initializing_counts_actual_wal_not_unselected_anchor)
{
	for (int has_parameters = 0; has_parameters < 2; has_parameters++) {
		BootstrapFixture f;
		ClusterControlBootstrapObservation out;
		char paths[3][MAXPGPATH];
		bootstrap_initializing_wal_fixture(&f, has_parameters != 0, paths);
		if (ut_current_failed)
			return;
		/* This higher anchor is deliberately unselected before DURABLE. */
		UT_ASSERT_EQ(unlink(paths[2]), 0);
		UT_ASSERT_EQ(
			cluster_control_bootstrap_read(bootstrap_local, test_root, test_wal_root, 0, &out), 0);
		if (ut_current_failed)
			return;
		UT_ASSERT_EQ(out.required.pending_sources, 1);
		UT_ASSERT_EQ(out.required.max_connections, has_parameters ? 1401 : 601);
		UT_ASSERT_EQ(test_cf_lock_calls, 0);
		pfree(out.config_bytes);
		UT_ASSERT_EQ(unlink(paths[1]), 0);
		UT_ASSERT_EQ(bootstrap_read_refused(0), CLUSTER_CONTROL_ROOT_ABSENT);
	}
}

UT_TEST(test_bootstrap_initializing_refuses_malformed_native_metadata)
{
	for (int fault = 0; fault < 2; ++fault) {
		BootstrapFixture f;
		ControlRootImage root;
		ClusterWalStartupImage operation;
		uint8 startup[CLUSTER_WAL_STARTUP_BYTES], page[XLOG_BLCKSZ];
		XLogRecord record;
		char paths[3][MAXPGPATH], path[MAXPGPATH];
		uint8 *bytes = page + SizeOfXLogLongPHD;
		bootstrap_initializing_wal_fixture(&f, true, paths);
		UT_ASSERT_EQ(cluster_control_root_v3_decode(f.before, sizeof(f.before), v2_storage,
													TEST_SYSID, &root),
					 0);
		read_all_or_abort(paths[0], startup, sizeof(startup));
		UT_ASSERT_EQ(cluster_control_root_v3_startup_decode(startup, sizeof(startup), &root, 127,
															&operation),
					 0);
		v2_checkpoint_wal_path(&operation.claim.identity, operation.first_segment_lsn,
							   operation.timeline, path);
		read_all_or_abort(path, page, sizeof(page));
		memcpy(&record, bytes, sizeof(record));
		/* Preserve native physical CRC validity. The actual bootstrap consumer
		 * must reject the wrong metadata size even with an EMPTY promise. */
		record.xl_info = fault == 0 ? XLOG_FPW_CHANGE : XLOG_CHECKPOINT_SHUTDOWN;
		INIT_CRC32C(record.xl_crc);
		COMP_CRC32C(record.xl_crc, bytes + SizeOfXLogRecord, record.xl_tot_len - SizeOfXLogRecord);
		COMP_CRC32C(record.xl_crc, &record, offsetof(XLogRecord, xl_crc));
		FIN_CRC32C(record.xl_crc);
		memcpy(bytes, &record, sizeof(record));
		write_all_or_abort(path, page, sizeof(page));
		UT_ASSERT_EQ(bootstrap_read_refused(0), CLUSTER_CONTROL_ROOT_RANGE_INVALID);
		UT_ASSERT_EQ(test_cf_lock_calls, 0);
	}
}

static void
bootstrap_terminal_fixture(BootstrapFixture *f, bool parameters, char paths[4][MAXPGPATH])
{
	ControlRootImage root;
	ClusterWalTerminalImage terminal = { 0 };
	ClusterWalHistoryImage history;
	ClusterWalStartupStage stage;
	ClusterWalHistoryStage manifest;
	ClusterWalDurablePrefixRef wal = { 0 };
	char temp[MAXPGPATH];
	bootstrap_initializing_wal_fixture(f, parameters, paths);
	UT_ASSERT_EQ(
		cluster_control_root_v3_decode(f->before, sizeof(f->before), v2_storage, TEST_SYSID, &root),
		0);
	read_all_or_abort(paths[0], terminal.original, sizeof(terminal.original));
	UT_ASSERT_EQ(cluster_control_root_v3_startup_decode(terminal.original,
														sizeof(terminal.original), &root, 127,
														&terminal.initialization),
				 0);
	terminal.closure_version = 1;
	memcpy(terminal.operation_uuid, terminal.initialization.operation_uuid, 16);
	terminal.generation = terminal.initialization.generation + 1;
	terminal.database_incarnation = terminal.initialization.database_incarnation;
	terminal.sealing_sequence = root.header.file_txn_seq;
	sha256_bytes(f->before, sizeof(f->before), terminal.sealing_sha256);
	terminal.formation_epoch = terminal.initialization.formation_epoch;
	terminal.recoverer_incarnation = 777;
	terminal.recoverer_node = 0;
	terminal.ir_request_id = 12345;
	terminal.original_ref = root.startup[127];
	memset(terminal.isolation_sha256, 0x55, 32);
	memset(terminal.closure_sha256, 0x66, 32);
	wal.claim.identity = terminal.initialization.claim.identity;
	wal.claim.database_incarnation = terminal.database_incarnation;
	wal.claim.max_config_generation = terminal.initialization.config_generation;
	sha256_bytes(terminal.original + 1280, 112, wal.claim.claim_sha256);
	wal.timeline = terminal.initialization.timeline;
	UT_ASSERT_EQ(cluster_wal_startup_observe(
					 test_wal_root, &wal, terminal.initialization.segment_size,
					 terminal.initialization.first_segment_lsn, &terminal.observation),
				 0);
	test_actual_cf = test_cf_mode = ExclusiveLock;
	test_cf_grant = test_cf_clusterwide = true;
	UT_ASSERT_EQ(cluster_wal_history_read_locked(&root, 127, &history), 0);
	UT_ASSERT_EQ(cluster_wal_terminal_prepare(&root, 127, &history, &terminal, &stage), 0);
	UT_ASSERT_EQ(cluster_wal_startup_install(&stage), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_terminal_install(&stage), 0);
	if (ut_current_failed)
		return;
	UT_ASSERT_EQ(cluster_wal_terminal_install(&stage), 0);
	startup_stage_paths(&stage, paths[3], temp);
	history.terminal_count = 1;
	history.terminals[0].incarnation = wal.claim.identity.origin_owner_incarnation;
	history.terminals[0].generation = stage.generation;
	memcpy(history.terminals[0].sha256, stage.sha256, 32);
	history_stage_dirs(127);
	UT_ASSERT_EQ(cluster_wal_history_prepare(&root, 127, &history, stage.generation,
											 terminal.operation_uuid, &manifest),
				 0);
	UT_ASSERT_EQ(cluster_wal_history_install(&manifest), 0);
	if (ut_current_failed)
		return;
	root.refs[127].history_generation = manifest.generation;
	memcpy(root.refs[127].history_sha256, manifest.sha256, 32);
	memset(&root.startup[127], 0, sizeof(root.startup[127]));
	root.header.file_txn_seq++;
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0);
	memcpy(f->before, root.bytes, sizeof(f->before));
	v2_write_roots(f->before);
	UT_ASSERT_EQ(cluster_wal_terminal_discard(&stage), 0);
	UT_ASSERT_EQ(cluster_wal_history_discard(&manifest), 0);
	/* The terminal must stand on its embedded selected input, not this file. */
	UT_ASSERT_EQ(unlink(paths[0]), 0);
	test_actual_cf = test_cf_mode = NoLock;
}

UT_TEST(test_bootstrap_terminal_preserves_actual_wal_capacity_without_active_intent)
{
	for (unsigned parameters = 0; parameters < 2; parameters++) {
		BootstrapFixture f;
		ClusterControlBootstrapObservation out;
		char paths[4][MAXPGPATH];
		bootstrap_terminal_fixture(&f, parameters != 0, paths);
		if (ut_current_failed)
			return;
		UT_ASSERT_EQ(
			cluster_control_bootstrap_read(bootstrap_local, test_root, test_wal_root, 0, &out), 0);
		if (ut_current_failed)
			return;
		UT_ASSERT_EQ(out.required.pending_sources, 0);
		UT_ASSERT_EQ(out.required.history_sources, 3);
		UT_ASSERT_EQ(out.required.max_connections, parameters ? 1401 : 601);
		UT_ASSERT(!out.snapshot.pending_wal_valid);
		UT_ASSERT_EQ(test_cf_lock_calls, 0);
		pfree(out.config_bytes);
		UT_ASSERT_EQ(unlink(paths[3]), 0);
		UT_ASSERT_EQ(bootstrap_read_refused(0), CLUSTER_CONTROL_ROOT_ABSENT);
	}
}

UT_TEST(test_terminal_selected_reader_and_bootstrap_refuse_incomplete_evidence)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		BootstrapFixture f;
		ControlRootImage root;
		ClusterWalTerminalImage terminal;
		ClusterWalOriginInputs inputs;
		char paths[4][MAXPGPATH], path[MAXPGPATH];
		bootstrap_terminal_fixture(&f, true, paths);
		if (ut_current_failed)
			return;
		UT_ASSERT_EQ(cluster_control_root_v3_decode(f.before, sizeof(f.before), v2_storage,
													TEST_SYSID, &root),
					 0);
		test_actual_cf = test_cf_mode = ShareLock;
		UT_ASSERT_EQ(cluster_wal_origin_inputs_read_locked(&root, 127, &inputs), 0);
		UT_ASSERT_EQ(inputs.history.terminal_count, 1);
		UT_ASSERT(!inputs.has_pending);
		UT_ASSERT_EQ(cluster_wal_terminal_read_locked(&root, 127, 0, &terminal), 0);
		if (ut_current_failed)
			return;
		if (fault == 0)
			UT_ASSERT_EQ(unlink(paths[1]), 0);
		else if (fault == 1) {
			snprintf(path, sizeof(path),
					 "%s/thread_128/generation_" UINT64_FORMAT "/durable_prefix/current",
					 test_wal_root,
					 terminal.initialization.claim.identity.origin_owner_incarnation);
			UT_ASSERT_EQ(unlink(path), 0);
		} else if (fault == 2) {
			uint8 page[XLOG_BLCKSZ];
			XLogRecord record;
			uint8 *bytes = page + SizeOfXLogLongPHD;
			v2_checkpoint_wal_path(&terminal.initialization.claim.identity,
								   terminal.initialization.first_segment_lsn,
								   terminal.initialization.timeline, path);
			read_all_or_abort(path, page, sizeof(page));
			memcpy(&record, bytes, sizeof(record));
			record.xl_info = XLOG_NOOP;
			INIT_CRC32C(record.xl_crc);
			COMP_CRC32C(record.xl_crc, bytes + SizeOfXLogRecord,
						record.xl_tot_len - SizeOfXLogRecord);
			COMP_CRC32C(record.xl_crc, &record, offsetof(XLogRecord, xl_crc));
			FIN_CRC32C(record.xl_crc);
			memcpy(bytes, &record, sizeof(record));
			write_all_or_abort(path, page, sizeof(page));
		} else if (fault == 3)
			UT_ASSERT_EQ(unlink(paths[3]), 0);
		else if (fault == 4)
			UT_ASSERT_EQ(chmod(paths[3], 0660), 0);
		else if (fault == 5) {
			UT_ASSERT_EQ(unlink(paths[3]), 0);
			UT_ASSERT_EQ(symlink("missing-terminal", paths[3]), 0);
		} else
			write_all_or_abort(paths[3], "bad", 3);
		if (fault >= 3) {
			UT_ASSERT(cluster_wal_terminal_read_locked(&root, 127, 0, &terminal) != 0);
			UT_ASSERT(v2_zero(&terminal, sizeof(terminal)));
			UT_ASSERT(cluster_wal_origin_inputs_read_locked(&root, 127, &inputs) != 0);
			UT_ASSERT(v2_zero(&inputs, sizeof(inputs)));
		}
		test_actual_cf = test_cf_mode = NoLock;
		UT_ASSERT(bootstrap_read_refused(0) != 0);
		v2_assert_primary_unchanged(f.before);
	}
}

UT_TEST(test_bootstrap_v3_pending_is_never_missing_from_capacity)
{
	BootstrapFixture f;
	ClusterControlBootstrapObservation out;
	char paths[3][MAXPGPATH];

	bootstrap_pending_fixture(&f, CLUSTER_WAL_STARTUP_DURABLE, paths);
	if (ut_current_failed)
		return;
	UT_ASSERT_EQ(cluster_control_bootstrap_read(bootstrap_local, test_root, test_wal_root, 0, &out),
				 0);
	if (ut_current_failed)
		return;
	UT_ASSERT_EQ(out.required.current_sources, 2);
	UT_ASSERT_EQ(out.required.history_sources, 2);
	UT_ASSERT_EQ(out.required.pending_sources, 1);
	UT_ASSERT_EQ(out.required.max_connections, 901);
	/* Another origin's initialization never changes this node's route. */
	UT_ASSERT(!out.snapshot.pending_wal_valid);
	UT_ASSERT(v2_zero(&out.snapshot.pending_wal, sizeof(out.snapshot.pending_wal)));
	pfree(out.config_bytes);
	for (int object = 0; object < 3; object++) {
		bootstrap_pending_fixture(&f, CLUSTER_WAL_STARTUP_DURABLE, paths);
		UT_ASSERT_EQ(unlink(paths[object]), 0);
		UT_ASSERT_EQ(bootstrap_read_refused(0), CLUSTER_CONTROL_ROOT_ABSENT);
	}
	bootstrap_pending_fixture(&f, CLUSTER_WAL_STARTUP_INITIALIZING, paths);
	/* Missing real PGWP is not an EMPTY prefix, even with an unselected anchor. */
	UT_ASSERT_EQ(bootstrap_read_refused(0), CLUSTER_CONTROL_ROOT_ABSENT);
}

static void
bootstrap_route_cleanup(const char *pgdata)
{
	char path[MAXPGPATH];
	snprintf(path, sizeof(path), "%s/pg_wal", pgdata);
	UT_ASSERT_EQ(unlink(path), 0);
	snprintf(path, sizeof(path), "%s/global/%s", pgdata, PGRAC_CONTROL_BINDING_NAME);
	UT_ASSERT_EQ(unlink(path), 0);
	snprintf(path, sizeof(path), "%s/global", pgdata);
	UT_ASSERT_EQ(rmdir(path), 0);
	UT_ASSERT_EQ(rmdir(pgdata), 0);
	DataDir = test_root;
	test_restart_ref_valid = test_reserve_mode = false;
}

UT_TEST(test_bootstrap_pending_route_never_masks_bad_inputs_or_namespace)
{
	unsigned before_fds = 0, after_fds = 0;
	for (int fd = 0; fd < 256; ++fd)
		if (fcntl(fd, F_GETFD) >= 0)
			before_fds++;
	for (unsigned routed = 0; routed < 2; ++routed)
		for (unsigned fault = 0; fault < 9; ++fault) {
			uint8 before[66048];
			ControlRootImage root;
			char pgdata[MAXPGPATH], path[MAXPGPATH], pgwal[MAXPGPATH];
			ClusterWalStartupImage op = v3_route_fixture(before, &root, pgdata), observed;
			ClusterControlBootstrapObservation out;
			v3_route_binding(&root, pgdata);
			if (routed)
				UT_ASSERT_EQ(cluster_control_root_v3_startup_route_writer(
								 &op.claim.identity, op.operation_uuid, &observed),
							 0);
			UT_ASSERT_EQ(cluster_control_bootstrap_read(pgdata, test_root, test_wal_root, 0, &out),
						 0);
			if (ut_current_failed)
				return;
			snprintf(pgwal, sizeof(pgwal), "%s/pg_wal", pgdata);
			if (fault < 2) {
				v2_claim_path(fault == 0 ? &op.predecessor.snapshot.identity : &op.claim.identity,
							  path);
				write_all_or_abort(path, "bad", 3);
			} else if (fault < 4) {
				const ClusterControlRootIdentity *identity
					= fault == 2 ? &op.predecessor.snapshot.identity : &op.claim.identity;
				snprintf(path, sizeof(path),
						 "%s/thread_1/generation_" UINT64_FORMAT "/durable_prefix/current",
						 test_wal_root, identity->origin_owner_incarnation);
				UT_ASSERT_EQ(unlink(path), 0);
			} else if (fault == 4) {
				UT_ASSERT_EQ(unlink(pgwal), 0);
				UT_ASSERT_EQ(symlink(test_wal_root, pgwal), 0);
			} else if (fault == 5)
				bootstrap_wal_route_race = true;
			else if (fault == 6) {
				bootstrap_close_calls = 0;
				bootstrap_close_fail_at = 1;
			} else if (fault == 7)
				out.snapshot.pending_wal.claim.identity.origin_node_id++;
			else
				out.snapshot.pending_wal.claim.identity.origin_owner_incarnation
					= out.snapshot.wal.claim.identity.origin_owner_incarnation;
			UT_ASSERT(
				cluster_control_bootstrap_wal_startup_route(pgdata, test_wal_root, &out.snapshot)
				!= 0);
			if (fault == 5)
				UT_ASSERT(!bootstrap_wal_route_race);
			bootstrap_close_fail_at = 0;
			pfree(out.config_bytes);
			v2_assert_primary_unchanged(before);
			bootstrap_route_cleanup(pgdata);
		}
	for (int fd = 0; fd < 256; ++fd)
		if (fcntl(fd, F_GETFD) >= 0)
			after_fds++;
	UT_ASSERT_EQ(after_fds, before_fds);
}

UT_TEST(test_bootstrap_pending_route_tracks_durable_and_install_not_authority)
{
	uint8 before[66048];
	ControlRootImage root;
	ControlFileData candidate;
	ClusterControlRootFileToken token;
	char pgdata[MAXPGPATH];
	ClusterWalStartupImage op = v3_route_fixture(before, &root, pgdata), observed;
	ClusterWalDurablePrefixRef writer;
	ClusterControlBootstrapObservation out;
	v3_route_binding(&root, pgdata);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_route_writer(&op.claim.identity, op.operation_uuid,
															  &observed),
				 0);
	test_actual_cf = test_cf_mode = ShareLock;
	UT_ASSERT_EQ(cluster_control_root_v3_read_thread_locked(&op.predecessor.snapshot.identity,
															&root, &candidate, &token),
				 0);
	test_actual_cf = test_cf_mode = NoLock;
	v3_startup_native_record(&op, &candidate);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_checkpoint(&op.claim.identity, op.operation_uuid,
															&candidate, test_checkpoint_end,
															&observed),
				 0);
	UT_ASSERT_EQ(cluster_control_bootstrap_read(pgdata, test_root, test_wal_root, 0, &out), 0);
	if (ut_current_failed)
		return;
	UT_ASSERT(out.snapshot.pending_wal_valid);
	UT_ASSERT(cluster_control_root_identity_equal(&out.snapshot.wal.claim.identity,
												  &op.predecessor.snapshot.identity));
	UT_ASSERT_EQ(cluster_control_bootstrap_wal_startup_route(pgdata, test_wal_root, &out.snapshot),
				 0);
	pfree(out.config_bytes);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_install_writer(&observed, &writer), 0);
	UT_ASSERT_EQ(cluster_control_bootstrap_read(pgdata, test_root, test_wal_root, 0, &out), 0);
	if (ut_current_failed)
		return;
	UT_ASSERT(!out.snapshot.pending_wal_valid);
	UT_ASSERT(v2_zero(&out.snapshot.pending_wal, sizeof(out.snapshot.pending_wal)));
	UT_ASSERT(
		cluster_control_root_identity_equal(&out.snapshot.wal.claim.identity, &op.claim.identity));
	UT_ASSERT_EQ(out.snapshot.database_state, CLUSTER_CONTROL_ROOT_DATABASE_MOUNTED);
	UT_ASSERT_EQ(cluster_control_bootstrap_wal_startup_route(pgdata, test_wal_root, &out.snapshot),
				 0);
	pfree(out.config_bytes);
	bootstrap_route_cleanup(pgdata);
}

UT_TEST(test_bootstrap_reserved_does_not_offer_a_successor_route)
{
	uint8 before[66048];
	ControlRootImage root;
	ClusterWalStartupImage op = v3_target_fixture(before, &root, 0);
	ClusterControlBootstrapObservation out;
	char pgdata[MAXPGPATH] = "/tmp/pgrac-bootstrap-reserved.XXXXXX";
	char pgwal[MAXPGPATH], generation[MAXPGPATH];
	UT_ASSERT(mkdtemp(pgdata) != NULL);
	v3_route_binding(&root, pgdata);
	snprintf(pgwal, sizeof(pgwal), "%s/pg_wal", pgdata);
	snprintf(generation, sizeof(generation), "%s/thread_1/generation_" UINT64_FORMAT, test_wal_root,
			 op.predecessor.snapshot.identity.origin_owner_incarnation);
	UT_ASSERT_EQ(symlink(generation, pgwal), 0);
	UT_ASSERT_EQ(cluster_control_bootstrap_read(pgdata, test_root, test_wal_root, 0, &out), 0);
	if (ut_current_failed)
		return;
	UT_ASSERT(!out.snapshot.pending_wal_valid);
	UT_ASSERT(v2_zero(&out.snapshot.pending_wal, sizeof(out.snapshot.pending_wal)));
	UT_ASSERT_EQ(cluster_control_bootstrap_wal_startup_route(pgdata, test_wal_root, &out.snapshot),
				 0);
	pfree(out.config_bytes);
	bootstrap_route_cleanup(pgdata);
}

UT_TEST(test_bootstrap_v3_pending_objects_and_reread_remain_exact)
{
	BootstrapFixture f;
	ClusterControlBootstrapObservation out;
	char paths[3][MAXPGPATH], saved[MAXPGPATH];
	uint8 bytes[CLUSTER_WAL_STARTUP_BYTES];
	const size_t lengths[] = { CLUSTER_WAL_STARTUP_BYTES, 112, 512 };

	for (int object = 0; object < 3; object++)
		for (int fault = 0; fault < 4; fault++) {
			bootstrap_pending_fixture(&f, CLUSTER_WAL_STARTUP_DURABLE, paths);
			if (ut_current_failed)
				return;
			if (fault == 0) {
				read_all_or_abort(paths[object], bytes, lengths[object]);
				bytes[20] ^= 1;
				write_all_or_abort(paths[object], bytes, lengths[object]);
			} else if (fault == 1)
				UT_ASSERT_EQ(truncate(paths[object], lengths[object] - 1), 0);
			else if (fault == 2)
				UT_ASSERT_EQ(chmod(paths[object], 0666), 0);
			else {
				snprintf(saved, sizeof(saved), "%s.saved", paths[object]);
				UT_ASSERT_EQ(rename(paths[object], saved), 0);
				UT_ASSERT_EQ(symlink(saved, paths[object]), 0);
			}
			bootstrap_read_refused(0);
			if (fault == 2)
				UT_ASSERT_EQ(chmod(paths[object], 0600), 0);
			else if (fault == 3) {
				UT_ASSERT_EQ(unlink(paths[object]), 0);
				UT_ASSERT_EQ(rename(saved, paths[object]), 0);
			}
		}
	bootstrap_pending_fixture(&f, CLUSTER_WAL_STARTUP_RESERVED, paths);
	/* RESERVED grants no native mutation. It is counted, not guessed durable. */
	UT_ASSERT_EQ(cluster_control_bootstrap_read(bootstrap_local, test_root, test_wal_root, 0, &out),
				 0);
	if (ut_current_failed)
		return;
	UT_ASSERT_EQ(out.required.pending_sources, 1);
	UT_ASSERT_EQ(out.required.max_connections, 601);
	pfree(out.config_bytes);
	bootstrap_pending_fixture(&f, CLUSTER_WAL_STARTUP_DURABLE, paths);
	memcpy(bootstrap_replacement, f.before, sizeof(f.before));
	put_u64_le(bootstrap_replacement + 16, 9);
	v2_checksums(bootstrap_replacement);
	bootstrap_race = 1;
	bootstrap_race_at = 3;
	UT_ASSERT_EQ(bootstrap_read_refused(0), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT_EQ(bootstrap_race, 0);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
}

UT_TEST(test_v3_runtime_retention_and_canonical_use_exact_new_root)
{
	uint8 bytes[66048];
	ClusterControlRootIdentity self;
	ControlFileData candidate, view;
	ClusterControlRootSnapshot out;
	ClusterControlRootReadToken token;
	ClusterControlRootStopObservation stop;

	v2_runtime_fixture(bytes, &self, &candidate);
	root_fixture_version3(bytes);
	v2_write_roots(bytes);
	UT_ASSERT_EQ(cluster_control_root_v3_read_runtime_local_locked(&view), 0);
	UT_ASSERT_EQ(view.checkPoint, candidate.checkPoint - 8192);
	UT_ASSERT_EQ(cluster_control_root_v2_read_runtime_local_locked(&view),
				 CLUSTER_CONTROL_ROOT_BAD_VERSION);
	UT_ASSERT(v2_zero(&view, sizeof(view)));
	test_cf_mode = NoLock;
	test_checkpoint_outer_cf = false;
	test_actual_cf = NoLock;
	UT_ASSERT_EQ(cluster_control_root_v3_read_retention_current(&self, &out, &token), 0);
	UT_ASSERT(cluster_control_root_identity_equal(&out.identity, &self));
	UT_ASSERT_EQ(token.origin_thread_id, self.origin_thread_id);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	UT_ASSERT_EQ(cluster_control_root_v3_read_canonical(self.origin_thread_id, &self, &out, &token),
				 0);
	UT_ASSERT_EQ(cluster_control_root_v3_read_canonical(self.origin_thread_id, NULL, &out, &token),
				 0);
	UT_ASSERT(cluster_control_root_identity_equal(&out.identity, &self));
	UT_ASSERT_EQ(cluster_control_root_v3_stop_phase_read(self.origin_thread_id,
														 self.origin_owner_incarnation, &stop),
				 0);
	UT_ASSERT_EQ(stop.phase, CLUSTER_CONTROL_ROOT_STOP_ACTIVE);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	v2_assert_primary_unchanged(bytes);
	cluster_shared_config = false;
}

/* The production runtime entrypoints must consume v3, not merely expose a
 * separate decoder that none of their callers use. Author: SqlRush. */
UT_TEST(test_shared_runtime_dispatches_only_startup_capable_root)
{
	uint8 bytes[66048], legacy[66048];
	ClusterControlRootIdentity self, discovered;
	ControlFileData candidate, view;
	ClusterControlRootSnapshot out, checked;
	ClusterControlRootReadToken token;

	v2_runtime_fixture(bytes, &self, &candidate);
	memcpy(legacy, bytes, sizeof(legacy));
	root_fixture_version3(bytes);
	v2_write_roots(bytes);
	UT_ASSERT(cluster_cf_authority_read(&view));
	UT_ASSERT_EQ(view.checkPoint, candidate.checkPoint - 8192);
	test_cf_mode = test_actual_cf = NoLock;
	test_checkpoint_outer_cf = false;
	UT_ASSERT_EQ(cluster_control_root_read_canonical(
					 self.origin_thread_id, &self, CLUSTER_CONTROL_ROOT_READ_STRONG, &out, &token),
				 0);
	UT_ASSERT_EQ(
		cluster_control_root_read_canonical_discovered(self.origin_thread_id, &out, &token), 0);
	UT_ASSERT_EQ(cluster_control_root_lookup_owner_by_node_runtime(self.origin_node_id, &discovered,
																   &out, &token),
				 0);
	UT_ASSERT(cluster_control_root_identity_equal(&discovered, &self));
	UT_ASSERT_EQ(cluster_control_root_revalidate(&token, &self, &checked), 0);
	UT_ASSERT(memcmp(&checked, &out, sizeof(out)) == 0);
	/* Old format remains a valid explicit fixture, never a runtime fallback. */
	memcpy(bytes, legacy, sizeof(bytes));
	v2_write_roots(bytes);
	UT_ASSERT_EQ(
		cluster_control_root_read_canonical_discovered(self.origin_thread_id, &out, &token),
		CLUSTER_CONTROL_ROOT_BAD_VERSION);
	UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
	cluster_shared_config = false;
}

UT_TEST(test_v3_runtime_pending_cannot_be_clean_or_retention_authority)
{
	uint8 bytes[66048];
	ClusterControlRootIdentity self;
	ControlFileData candidate;
	ClusterControlRootSnapshot out;
	ClusterControlRootReadToken token;
	ClusterControlRootStopObservation stop;

	v2_retention_fixture(bytes, &self, &candidate);
	root_fixture_version3(bytes);
	bytes[512 + 10] = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	put_u64_le(bytes + 232, 0);
	put_u64_le(bytes + 512 + 328, 705);
	memset(bytes + 512 + 336, 0x57, 32);
	v2_checksums(bytes);
	v2_write_roots(bytes);
	UT_ASSERT_EQ(cluster_control_root_v3_read_canonical(self.origin_thread_id, &self, &out, &token),
				 CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID);
	UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
	UT_ASSERT_EQ(cluster_control_root_read_canonical(
					 self.origin_thread_id, &self, CLUSTER_CONTROL_ROOT_READ_STRONG, &out, &token),
				 CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID);
	UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
	UT_ASSERT_EQ(
		cluster_control_root_read_canonical_discovered(self.origin_thread_id, &out, &token),
		CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID);
	UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
	UT_ASSERT_EQ(
		cluster_control_root_lookup_owner_by_node_runtime(self.origin_node_id, NULL, &out, &token),
		CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID);
	UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
	UT_ASSERT_EQ(cluster_control_root_v3_read_retention_current(&self, &out, &token),
				 CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID);
	UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
	/* The other origin is still serving: full-stop evidence cannot silently
	 * ignore this selected, non-serving pending obligation. */
	UT_ASSERT_EQ(cluster_control_root_v3_stop_phase_read(128, 226, &stop),
				 CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID);
	UT_ASSERT(v2_zero(&stop, sizeof(stop)));
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	cluster_shared_config = false;
}

UT_TEST(test_v3_service_cannot_consume_cached_v2_observation)
{
	uint8 bytes[66048];
	ClusterControlRootIdentity self;
	ControlFileData candidate;
	ClusterControlRootSnapshot out;
	ClusterControlRootReadToken token;

	v2_retention_fixture(bytes, &self, &candidate);
	MyBackendType = B_LMON;
	test_cf_release_confirmed = false;
	UT_ASSERT_EQ(cluster_control_root_v2_read_canonical(1, &self, &out, &token),
				 CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	test_cf_release_confirmed = true;
	root_fixture_version3(bytes);
	v2_write_roots(bytes);
	UT_ASSERT_EQ(cluster_control_root_v3_read_canonical(1, &self, &out, &token),
				 CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
	UT_ASSERT_EQ(cluster_control_root_v3_read_canonical(1, &self, &out, &token), 0);
	UT_ASSERT(cluster_control_root_identity_equal(&out.identity, &self));
	MyBackendType = B_INVALID;
	cluster_shared_config = false;
}

/* A publisher must preserve an unrelated pending origin verbatim, not
 * interpret it as the old clean thread or erase it as reserved padding. */
static void
v3_mark_pending(uint8 bytes[66048], unsigned node)
{
	uint8 *record = bytes + 512 + node * 512;
	record[10] = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	bytes[232 + node / 8] &= ~(1u << (node % 8));
	put_u64_le(record + 328, 705);
	memset(record + 336, 0x57, 32);
	v2_checksums(bytes);
}

static void
v3_complete_fixture(uint8 before[66048], ClusterControlRootSnapshot *input,
					ClusterControlRootReadToken *token, ClusterControlRootPatch *patch)
{
	ClusterRecoverySerialRequest request;
	char path[MAXPGPATH];
	v2_failure_fixture(before, &request, true);
	root_fixture_version3(before);
	v3_mark_pending(before, 127);
	v2_write_roots(before);
	UT_ASSERT_EQ(cluster_control_root_v3_read_canonical(1, &request.duty, input,
														&request.expected_root_token),
				 0);
	UT_ASSERT_EQ(cluster_control_root_v3_failure_tail_publish(&request, input, token), 0);
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	read_all_or_abort(path, before, 66048);
	memset(patch, 0, sizeof(*patch));
	patch->mask
		= CLUSTER_CONTROL_ROOT_PATCH_LIFECYCLE | CLUSTER_CONTROL_ROOT_PATCH_RECOVERY_PROGRESS;
	patch->expected_lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	patch->expected_flags_mask = patch->expected_flags_value = input->root_flags;
	patch->desired.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	patch->desired.root_flags = input->root_flags | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID
								| CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_LAST_RECORD_VALID;
	patch->desired.recovered_tli = input->tail_tli;
	patch->desired.recovered_through_lsn_exclusive = input->validated_tail_lsn_exclusive;
	patch->desired.recovered_last_record_lsn = input->tail_last_record_lsn;
	patch->desired.recovered_last_record_crc32c = input->tail_last_record_crc32c;
	/* Use the actual held-mode fixture, not the legacy wildcard CF seam. */
	test_reserve_mode = true;
}

static ClusterControlRootResult
v3_complete_publish(const ClusterControlRootReadToken *token, const ClusterControlRootPatch *patch,
					ClusterControlRootSnapshot *out, ClusterControlRootReadToken *published)
{
	return cluster_control_root_compare_and_publish(
		token, patch, CLUSTER_CONTROL_ROOT_PUBLISH_RECOVERY_COMPLETE, out, published);
}

UT_TEST(test_v3_recovery_complete_preserves_exact_selected_inputs)
{
	uint8 before[66048], after[66048];
	ClusterControlRootSnapshot input, out;
	ClusterControlRootReadToken token, published, observed;
	ClusterControlRootPatch patch;
	char path[MAXPGPATH];
	v3_complete_fixture(before, &input, &token, &patch);
	UT_ASSERT_EQ(v3_complete_publish(&token, &patch, &out, &published), 0);
	if (ut_current_failed)
		return;
	UT_ASSERT_EQ(out.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE);
	UT_ASSERT_EQ(out.recovered_through_lsn_exclusive, input.validated_tail_lsn_exclusive);
	UT_ASSERT_EQ(out.recovered_last_record_crc32c, input.tail_last_record_crc32c);
	UT_ASSERT_EQ(memcmp(&out.identity, &input.identity, sizeof(input.identity)), 0);
	UT_ASSERT_EQ(out.root_publish_seq, input.root_publish_seq + 1);
	UT_ASSERT_EQ(published.file_txn_seq, token.file_txn_seq + 1);
	UT_ASSERT(test_walr_sealed_required);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	read_all_or_abort(path, after, sizeof(after));
	UT_ASSERT_EQ(after[4], 3);
	UT_ASSERT_EQ(memcmp(before + 196, after + 196, 180), 0);
	UT_ASSERT_EQ(memcmp(before + 512 + 216, after + 512 + 216, 152), 0);
	UT_ASSERT_EQ(memcmp(before + 1024, after + 1024, sizeof(after) - 1024), 0);
	UT_ASSERT_EQ(cluster_control_root_read_canonical(
					 1, &input.identity, CLUSTER_CONTROL_ROOT_READ_STRONG, &out, &observed),
				 0);
	UT_ASSERT_EQ(memcmp(&observed, &published, sizeof(observed)), 0);
}

UT_TEST(test_v3_recovery_complete_never_accepts_partial_or_unowned_terminal)
{
	for (int fault = 0; fault < 10; ++fault) {
		uint8 before[66048];
		ClusterControlRootSnapshot input, out;
		ClusterControlRootReadToken token, published;
		ClusterControlRootPatch patch;
		v3_complete_fixture(before, &input, &token, &patch);
		if (fault == 0)
			patch.desired.recovered_through_lsn_exclusive--;
		else if (fault == 1)
			patch.desired.recovered_tli++;
		else if (fault == 2)
			patch.desired.recovered_last_record_lsn++;
		else if (fault == 3)
			patch.desired.recovered_last_record_crc32c++;
		else if (fault == 4)
			test_publish_authorized = false;
		else if (fault == 5)
			test_walr_begin_result = CLUSTER_WAL_PIN_STALE;
		else if (fault == 6)
			test_walr_sealed_current = false;
		else if (fault == 7)
			token.file_txn_seq++;
		else if (fault == 8) {
			ControlRootImage decoded;
			v3_mark_pending(before, 0);
			v2_write_roots(before);
			UT_ASSERT_EQ(cluster_control_root_v3_decode(before, sizeof(before),
														input.identity.storage_uuid,
														input.identity.system_identifier, &decoded),
						 0);
			token.record_crc32c = decoded.record_crc32c[0];
		} else
			UT_ASSERT_EQ(unlink(test_checkpoint_prefix_path), 0);
		memset(&out, 0xa5, sizeof(out));
		memset(&published, 0xa5, sizeof(published));
		UT_ASSERT(v3_complete_publish(&token, &patch, &out, &published) != 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&published, sizeof(published)));
		v2_assert_primary_unchanged(before);
		UT_ASSERT_EQ(test_actual_cf, NoLock);
	}
}

static void
v3_complete_late_pin_loss(void)
{
	test_walr_sealed_current = false;
}

UT_TEST(test_v3_recovery_complete_late_loss_never_returns_permission)
{
	uint8 before[66048];
	ClusterControlRootSnapshot input, out;
	ClusterControlRootReadToken token, published;
	ClusterControlRootPatch patch;
	v3_complete_fixture(before, &input, &token, &patch);
	test_checkpoint_published_hook = v3_complete_late_pin_loss;
	UT_ASSERT_EQ(v3_complete_publish(&token, &patch, &out, &published),
				 CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&published, sizeof(published)));
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
	test_checkpoint_published_hook = NULL;
	/* Publication may have persisted; no rollback to the earlier root. */
	UT_ASSERT_EQ(cluster_control_root_read_canonical(
					 1, &input.identity, CLUSTER_CONTROL_ROOT_READ_STRONG, &out, &published),
				 0);
	UT_ASSERT_EQ(out.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE);
}

UT_TEST(test_v3_recovery_complete_errors_unwind_without_reverting_published_fact)
{
	for (int fault = 0; fault < 5; ++fault) {
		uint8 before[66048];
		ClusterControlRootSnapshot input, out;
		ClusterControlRootReadToken token, published;
		ClusterControlRootPatch patch;
		volatile bool caught = false;
		volatile ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
		v3_complete_fixture(before, &input, &token, &patch);
		if (fault == 0)
			test_fail_primary_rename = true;
		else if (fault == 1)
			test_fail_after_primary_rename = true;
		else if (fault == 2)
			test_release_after_primary = true;
		else if (fault == 3)
			test_walr_end_result = CLUSTER_WALR_RELEASE_UNCONFIRMED;
		else
			test_checkpoint_published_hook = v2_checkpoint_throw_on_x;
		memset(&out, 0xa5, sizeof(out));
		memset(&published, 0xa5, sizeof(published));
		PG_TRY();
		{
			result = v3_complete_publish(&token, &patch, &out, &published);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(fault == 4 ? caught : result != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&published, sizeof(published)));
		UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
		test_checkpoint_published_hook = NULL;
		test_fail_primary_rename = test_fail_after_primary_rename = false;
		test_release_after_primary = false;
		test_cf_release_confirmed = true;
		test_walr_end_result = CLUSTER_WALR_RELEASE_CONFIRMED;
		cluster_cf_retirement_poll();
		UT_ASSERT_EQ(cluster_control_root_read_canonical(
						 1, &input.identity, CLUSTER_CONTROL_ROOT_READ_STRONG, &out, &published),
					 0);
		UT_ASSERT_EQ(out.lifecycle, fault == 0 ? CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
											   : CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE);
	}
}

UT_TEST(test_v3_recovery_complete_alias_refuses_before_authority)
{
	uint8 before[66048];
	ClusterControlRootSnapshot input, out;
	ClusterControlRootReadToken token;
	ClusterControlRootPatch patch;
	int calls;
	v3_complete_fixture(before, &input, &token, &patch);
	calls = test_walr_begin_calls;
	UT_ASSERT_EQ(v3_complete_publish(&token, &patch, &out, &token),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
	UT_ASSERT_EQ(test_walr_begin_calls, calls);
	v2_assert_primary_unchanged(before);
}

UT_TEST(test_v3_failure_publishers_preserve_pending_and_authenticate_native_input)
{
	for (int sealed = 0; sealed < 2; sealed++) {
		uint8 before[66048], after[66048];
		ClusterRecoverySerialRequest request;
		ClusterControlRootSnapshot out;
		ClusterControlRootReadToken token;
		char path[MAXPGPATH];
		v2_failure_fixture(before, &request, sealed != 0);
		root_fixture_version3(before);
		v3_mark_pending(before, 127);
		v2_write_roots(before);
		UT_ASSERT_EQ(cluster_control_root_v3_read_canonical(1, &request.duty, &out,
															&request.expected_root_token),
					 0);
		UT_ASSERT_EQ(sealed ? cluster_control_root_v3_failure_tail_publish(&request, &out, &token)
							: cluster_control_root_v3_failure_open_publish(&request, &out, &token),
					 0);
		if (ut_current_failed)
			return;
		UT_ASSERT_EQ(out.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED);
		UT_ASSERT_EQ(out.validated_tail_lsn_exclusive, sealed ? test_checkpoint_end : 0);
		UT_ASSERT_EQ(out.recovered_through_lsn_exclusive, sealed ? out.checkpoint_lower_lsn : 0);
		path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
		read_all_or_abort(path, after, sizeof(after));
		UT_ASSERT_EQ(after[4], 3);
		UT_ASSERT(memcmp(before + 1024, after + 1024, 66048 - 1024) == 0);
		UT_ASSERT_EQ(cluster_control_root_v3_read_canonical(1, &request.duty, &out,
															&request.expected_root_token),
					 0);
		UT_ASSERT(memcmp(&request.expected_root_token, &token, sizeof(token)) == 0);
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
	}
}

UT_TEST(test_v3_failure_publication_retains_old_authority_and_no_fallback)
{
	for (int fault = 0; fault < 6; fault++) {
		uint8 before[66048];
		ClusterRecoverySerialRequest request;
		ClusterControlRootSnapshot out;
		ClusterControlRootReadToken token;
		v2_failure_fixture(before, &request, true);
		if (fault != 0) {
			root_fixture_version3(before);
			v3_mark_pending(before, 127);
			v2_write_roots(before);
			UT_ASSERT_EQ(cluster_control_root_v3_read_canonical(1, &request.duty, &out,
																&request.expected_root_token),
						 0);
		}
		if (fault == 2)
			test_failure_admissions = false;
		else if (fault == 3)
			UT_ASSERT_EQ(unlink(test_checkpoint_prefix_path), 0);
		else if (fault == 4)
			request.expected_root_token.file_txn_seq++;
		else if (fault == 5)
			test_self_incarnation = request.duty.origin_owner_incarnation;
		if (fault == 5)
			cluster_node_id = request.duty.origin_node_id;
		UT_ASSERT((fault == 1
					   ? cluster_control_root_v2_failure_tail_publish(&request, &out, &token)
					   : cluster_control_root_v3_failure_tail_publish(&request, &out, &token))
				  != 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
		v2_assert_primary_unchanged(before);
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
	}
}

UT_TEST(test_v3_checkpoint_publish_preserves_pending_and_real_wal_guards)
{
	for (int shutdown = 0; shutdown < 2; shutdown++) {
		uint8 before[66048];
		ClusterControlRootIdentity self;
		ClusterControlRootSnapshot out;
		ClusterControlRootFileToken token;
		ControlRootImage root;
		ControlFileData candidate, view;
		if (shutdown)
			v2_shutdown_checkpoint_fixture(before, &self, &candidate);
		else
			v2_checkpoint_fixture(before, &self, &candidate);
		root_fixture_version3(before);
		v3_mark_pending(before, 127);
		v2_write_roots(before);
		UT_ASSERT_EQ(shutdown ? cluster_control_root_v3_shutdown_checkpoint_publish(
									&self, &candidate, test_checkpoint_end, &out, &token, &view)
							  : cluster_control_root_v3_checkpoint_publish(
									&self, &candidate, test_checkpoint_end, &out, &token, &view),
					 0);
		if (ut_current_failed)
			return;
		UT_ASSERT_EQ(token.format_version, 3);
		UT_ASSERT_EQ(out.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN);
		UT_ASSERT_EQ(view.state, DB_IN_PRODUCTION);
		UT_ASSERT_EQ(out.validated_tail_lsn_exclusive, test_checkpoint_end);
		UT_ASSERT_EQ(cluster_control_root_v3_read_thread_locked(&self, &root, &view, &token), 0);
		UT_ASSERT(memcmp(root.bytes + 1024, before + 1024, 66048 - 1024) == 0);
		UT_ASSERT_EQ(root.startup[127].generation, 705);
		UT_ASSERT_EQ(root.header.v2.database_state, CLUSTER_CONTROL_ROOT_DATABASE_OPEN);
		UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		v2_assert_anchor_staging_empty();
		if (shutdown) {
			ShutdownRequestPending = true;
			UT_ASSERT_EQ(
				cluster_control_root_v3_shutdown_observe(&test_checkpoint_prefix_ref, &out, &token),
				0);
			UT_ASSERT_EQ(token.format_version, 3);
			UT_ASSERT_EQ(out.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN);
		}
	}
}

UT_TEST(test_v3_publishers_do_not_fallback_or_weaken_physical_checks)
{
	for (int fault = 0; fault < 5; fault++) {
		uint8 before[66048];
		ClusterControlRootIdentity self;
		ClusterControlRootSnapshot out;
		ClusterControlRootFileToken token;
		ControlFileData candidate, view;
		v2_checkpoint_fixture(before, &self, &candidate);
		if (fault != 0) {
			root_fixture_version3(before);
			v3_mark_pending(before, 127);
			v2_write_roots(before);
		}
		if (fault == 1)
			test_self_incarnation++;
		else if (fault == 2)
			v2_checkpoint_wal_record(&self, &candidate, 4);
		else if (fault == 3)
			UT_ASSERT_EQ(unlink(test_checkpoint_prefix_path), 0);
		UT_ASSERT((fault == 4 ? cluster_control_root_v2_checkpoint_publish(
									&self, &candidate, test_checkpoint_end, &out, &token, &view)
							  : cluster_control_root_v3_checkpoint_publish(
									&self, &candidate, test_checkpoint_end, &out, &token, &view))
				  != 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token))
				  && v2_zero(&view, sizeof(view)));
		v2_assert_primary_unchanged(before);
		UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
		UT_ASSERT_EQ(test_actual_cf, NoLock);
	}
}

UT_TEST(test_v3_normal_close_sparse_pair_preserves_exact_roster)
{
	uint8 before[66048];
	ClusterPhase1FullStopPlan plan;
	ClusterWalDurablePrefixRef peer;
	ClusterControlRootStopObservation out;
	bool complete = true;
	v2_close_fixture(before, &plan);
	peer = v2_close_add_peer(before, 3, &plan);
	root_fixture_version3(before);
	v2_write_roots(before);
	UT_ASSERT_EQ(cluster_control_root_v3_normal_stop_close(&plan, &complete), 0);
	if (ut_current_failed)
		return;
	UT_ASSERT(!complete);
	UT_ASSERT_EQ(cluster_control_root_v3_stop_phase_read(1, plan.member_incarnations[0], &out), 0);
	UT_ASSERT_EQ(out.members[0].phase, CLUSTER_CONTROL_ROOT_STOP_CLOSED);
	UT_ASSERT_EQ(out.members[3].phase, CLUSTER_CONTROL_ROOT_STOP_CHECKPOINT);
	cluster_node_id = 3;
	test_own_thread = 4;
	test_self_incarnation = test_membership_incarnation
		= peer.claim.identity.origin_owner_incarnation;
	test_checkpoint_prefix_ref = peer;
	plan.own_wal_started_at = peer.claim.identity.thread_claim_created_at;
	UT_ASSERT_EQ(cluster_control_root_v3_normal_stop_close(&plan, &complete), 0);
	UT_ASSERT(complete);
	UT_ASSERT_EQ(cluster_control_root_v3_normal_stop_close(&plan, &complete), 0);
	UT_ASSERT(complete);
	UT_ASSERT_EQ(cluster_control_root_v3_stop_phase_read(1, plan.member_incarnations[0], &out), 0);
	UT_ASSERT_EQ(out.members[3].phase, CLUSTER_CONTROL_ROOT_STOP_CLOSED);
	UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
}

UT_TEST(test_v3_normal_close_cannot_discard_foreign_pending_initialization)
{
	uint8 before[66048], after[66048];
	ClusterPhase1FullStopPlan plan;
	ControlRootImage root;
	ControlFileData view;
	ClusterControlRootFileToken token;
	char path[MAXPGPATH];
	bool complete = true;
	v2_close_fixture(before, &plan);
	(void)v2_close_add_peer(before, 3, &plan);
	plan.member_incarnations[3] = 0;
	plan.pre2_member_phase[3] = CLUSTER_CONTROL_ROOT_STOP_UNKNOWN;
	root_fixture_version3(before);
	v3_mark_pending(before, 3);
	v2_write_roots(before);
	UT_ASSERT_EQ(cluster_control_root_v3_normal_stop_close(&plan, &complete), 0);
	if (ut_current_failed)
		return;
	UT_ASSERT(!complete);
	UT_ASSERT_EQ(cluster_control_root_v3_read_thread_locked(
					 &test_checkpoint_prefix_ref.claim.identity, &root, &view, &token),
				 0);
	UT_ASSERT_EQ(root.records[0].lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED);
	UT_ASSERT_EQ(root.header.v2.database_state, CLUSTER_CONTROL_ROOT_DATABASE_OPEN);
	UT_ASSERT(memcmp(root.bytes + 2048, before + 2048, 512) == 0);
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	read_all_or_abort(path, after, sizeof(after));
	UT_ASSERT_EQ(cluster_control_root_v3_normal_stop_close(&plan, &complete), 0);
	UT_ASSERT(!complete);
	v2_assert_primary_unchanged(after);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
}

int
main(int argc, char **argv)
{
	if (argc > 1 && strcmp(argv[1], "--fixture-root-cast") == 0)
		return fixture_cast_main(argc, argv);
	if (argc > 1)
		return fixture_root_main(argc, argv);
	setup_fixture();

	if (getenv("PGRAC_PRE2_TEST_ROOT_POLL_RED") != NULL) {
		UT_PLAN(1);
		UT_RUN(test_runtime_v3_service_read_waits_for_exact_retirement_before_output);
		UT_DONE();
		return ut_failed_count ? 1 : 0;
	}
	UT_PLAN(302);
	UT_RUN(test_native_inputs_accept_only_empty_native_progress);
	UT_RUN(test_native_inputs_refuse_all_recovery_signals_without_cleanup);
	UT_RUN(test_native_inputs_preserve_slots_and_prepared_files);
	UT_RUN(test_native_inputs_recheck_namespace_and_release_fds);
	UT_RUN(test_bootstrap_side_routes_require_exact_origin);
	UT_RUN(test_bootstrap_side_rejects_local_foreign_and_unsafe_aliases);
	UT_RUN(test_bootstrap_side_children_parents_and_arguments_are_strict);
	UT_RUN(test_bootstrap_side_reobserves_every_namespace_and_closes_all_fds);
	UT_RUN(test_v3_reserve_clean_publishes_all_sparse_targets_without_serving);
	UT_RUN(test_v3_target_creates_only_exact_empty_successor);
	UT_RUN(test_v3_native_driver_prepares_sparse_targets_then_returns_initializing);
	UT_RUN(test_v3_native_driver_rejects_foreign_input_and_owner_without_mutation);
	UT_RUN(test_v3_native_driver_reserves_closed_cohort_only_on_coordinator);
	UT_RUN(test_v3_native_driver_missing_selected_or_partial_namespace_is_not_wait);
	UT_RUN(test_v3_native_driver_uncertain_observation_release_does_not_advance);
	UT_RUN(test_v3_startup_checkpoint_publishes_only_actual_new_durability);
	UT_RUN(test_v3_startup_checkpoint_retry_requires_selected_root_durability);
	UT_RUN(test_v3_startup_install_retains_predecessor_without_serving);
	UT_RUN(test_v3_startup_install_refuses_wrong_owner_and_intent);
	UT_RUN(test_v3_startup_install_requires_actual_old_and_new_files);
	UT_RUN(test_v3_startup_install_uncertain_rename_is_not_success);
	UT_RUN(test_v3_startup_install_all_sync_and_late_cuts_retain_obligations);
	UT_RUN(test_v3_startup_install_keeps_full_history_and_refuses_overflow);
	UT_RUN(test_v3_startup_install_sparse_pair_has_no_four_member_assumption);
	UT_RUN(test_v3_startup_checkpoint_refuses_runtime_and_control_drift);
	UT_RUN(test_v3_startup_checkpoint_requires_real_successor_bytes);
	UT_RUN(test_v3_startup_checkpoint_persistence_and_late_races_keep_obligations);
	UT_RUN(test_v3_begin_requires_all_declared_empty_targets_before_native_mutation);
	UT_RUN(test_v3_startup_route_switches_only_exact_empty_generation);
	UT_RUN(test_v3_startup_route_requires_exact_owner_and_restart_input);
	UT_RUN(test_v3_startup_route_refuses_unsafe_or_nonempty_namespaces);
	UT_RUN(test_v3_startup_route_uncertain_rename_requires_directory_sync);
	UT_RUN(test_v3_startup_route_rechecks_names_and_owner_after_exchange);
	UT_RUN(test_bootstrap_pending_route_keeps_immutable_restart_input);
	UT_RUN(test_v3_target_rejects_wrong_owner_before_creation);
	UT_RUN(test_v3_target_never_adopts_foreign_or_nonempty_namespace);
	UT_RUN(test_v3_target_sync_failures_never_grant_initialization);
	UT_RUN(test_v3_target_reobserves_formation_and_provider_after_files);
	UT_RUN(test_v3_begin_failures_preserve_selected_obligations_without_permission);
	UT_RUN(test_v3_reserve_requires_actual_collective_evidence_and_owner);
	UT_RUN(test_v3_reserve_rechecks_cut_after_wal_scan);
	UT_RUN(test_v3_reserve_consumes_real_nonlocal_files);
	UT_RUN(test_v3_reserve_uncertain_publication_never_returns_permission);
	UT_RUN(test_v3_reserve_error_cleanup_keeps_old_authority);
	UT_RUN(test_v3_clean_exit_cut_keeps_complete_root_roster);
	UT_RUN(test_v3_clean_exit_cut_never_shrinks_missing_members);
	UT_RUN(test_bootstrap_initializing_counts_actual_wal_not_unselected_anchor);
	UT_RUN(test_bootstrap_initializing_refuses_malformed_native_metadata);
	UT_RUN(test_bootstrap_terminal_preserves_actual_wal_capacity_without_active_intent);
	UT_RUN(test_terminal_selected_reader_and_bootstrap_refuse_incomplete_evidence);
	UT_RUN(test_v3_failure_publishers_preserve_pending_and_authenticate_native_input);
	UT_RUN(test_v3_recovery_complete_preserves_exact_selected_inputs);
	UT_RUN(test_v3_recovery_complete_never_accepts_partial_or_unowned_terminal);
	UT_RUN(test_v3_recovery_complete_late_loss_never_returns_permission);
	UT_RUN(test_v3_recovery_complete_errors_unwind_without_reverting_published_fact);
	UT_RUN(test_v3_recovery_complete_alias_refuses_before_authority);
	UT_RUN(test_v3_failure_publication_retains_old_authority_and_no_fallback);
	UT_RUN(test_v3_checkpoint_publish_preserves_pending_and_real_wal_guards);
	UT_RUN(test_v3_publishers_do_not_fallback_or_weaken_physical_checks);
	UT_RUN(test_v3_normal_close_sparse_pair_preserves_exact_roster);
	UT_RUN(test_v3_normal_close_cannot_discard_foreign_pending_initialization);
	UT_RUN(test_v3_runtime_retention_and_canonical_use_exact_new_root);
	UT_RUN(test_shared_runtime_dispatches_only_startup_capable_root);
	UT_RUN(test_v3_runtime_pending_cannot_be_clean_or_retention_authority);
	UT_RUN(test_v3_service_cannot_consume_cached_v2_observation);
	UT_RUN(test_bootstrap_v3_pending_objects_and_reread_remain_exact);
	UT_RUN(test_origin_input_union_preserves_current_history_and_pending);
	UT_RUN(test_origin_input_union_refuses_partial_or_unowned_metadata);
	UT_RUN(test_origin_input_union_alias_and_bounds_clear_all_output);
	UT_RUN(test_bootstrap_v3_reads_exact_current_and_flat_history);
	UT_RUN(test_bootstrap_v3_pending_is_never_missing_from_capacity);
	UT_RUN(test_bootstrap_pending_route_never_masks_bad_inputs_or_namespace);
	UT_RUN(test_bootstrap_pending_route_tracks_durable_and_install_not_authority);
	UT_RUN(test_bootstrap_reserved_does_not_offer_a_successor_route);
	UT_RUN(test_v3_history_files_preserve_pending_and_reject_unknown_format);
	UT_RUN(test_v3_history_keeps_flat_input_separate_from_pending_operation);
	UT_RUN(test_v3_locked_read_preserves_pending_and_current_projection);
	UT_RUN(test_v3_locked_read_requires_exact_format_primary_and_objects);
	UT_RUN(test_startup_file_replaced_stage_and_directory_cannot_be_deleted_or_adopted);
	UT_RUN(test_startup_file_invalid_owner_and_aliases_cannot_publish);
	UT_RUN(test_startup_file_install_preserves_literal_phases_without_selecting_root);
	UT_RUN(test_startup_file_prepare_and_install_durability_cuts_keep_owned_evidence);
	UT_RUN(test_startup_file_read_only_needs_no_staging_or_fsync);
	UT_RUN(test_startup_file_rejects_unselected_unsafe_and_changed_objects);
	UT_RUN(test_startup_encoder_matches_literal_phases_without_publishing);
	UT_RUN(test_terminal_literal_is_distinct_from_checkpoint_or_active_writer);
	UT_RUN(test_terminal_recoverer_uses_node_and_incarnation_identity);
	UT_RUN(test_terminal_rejects_unproven_or_inconsistent_fields);
	UT_RUN(test_terminal_requires_original_predecessor_in_flat_current_union);
	UT_RUN(test_startup_encoder_refuses_invalid_logical_input_without_partial_bytes);
	UT_RUN(test_startup_encoder_arguments_and_aliases_clear_outputs);
	UT_RUN(test_history_encoder_matches_literal_empty_and_full);
	UT_RUN(test_terminal_history_literal_union_and_old_reader_refusal);
	UT_RUN(test_terminal_history_refuses_aliases_and_malformed_sets);
	UT_RUN(test_terminal_history_real_file_install_and_selected_read);
	UT_RUN(test_terminal_history_combined_capacity_does_not_evict_inputs);
	UT_RUN(test_history_encoder_rejects_invalid_records_without_partial_output);
	UT_RUN(test_history_stage_installs_exact_readable_object);
	UT_RUN(test_history_stage_lock_and_no_clobber);
	UT_RUN(test_history_stage_uncertain_sync_keeps_formal_and_can_finish);
	UT_RUN(test_history_encoder_and_prepare_refuse_aliases);
	UT_RUN(test_history_stage_refuses_corrupt_or_unsafe_formal_without_overwrite);
	UT_RUN(test_history_stage_refuses_replaced_temporary_and_directory);
	UT_RUN(test_history_stage_bad_owner_duplicate_and_disabled_sync);
	UT_RUN(test_history_prepare_refuses_unsafe_dirs_and_invalid_parameters);
	UT_RUN(test_history_stage_changed_bytes_refuse_without_install);
	UT_RUN(test_history_discard_sync_failure_cannot_resurrect_install);
	UT_RUN(test_history_prepare_sync_failure_clears_output_and_owned_temp);
	UT_RUN(test_history_produced_file_reaches_actual_bootstrap_consumer);
	UT_RUN(test_history_selected_reader_is_read_only_and_needs_no_staging);
	UT_RUN(test_history_selected_reader_refuses_unsafe_or_unselected_inputs);
	test_history_fail_sync = 0;
	test_cf_mode = NoLock;
	UT_RUN(test_abi_identity_and_features);
	UT_RUN(test_invalid_argument_precedes_authority_io);
	UT_RUN(test_external_fence_bit24_activation_is_forbidden_without_provider);
	UT_RUN(test_create_and_read_primary);
	UT_RUN(test_bootstrap_read_never_returns_authority_token);
	UT_RUN(test_round_sha256_is_deterministic_and_matches_create);
	UT_RUN(test_build_migration_image_maps_registry_and_claims);
	UT_RUN(test_build_migration_image_accepts_frozen_active_slot);
	UT_RUN(test_build_migration_image_rejects_non_stopped_slot);
	UT_RUN(test_strong_read_null_identity_stays_invalid_argument);
	UT_RUN(test_discovered_read_binds_identity_and_mints_token);
	UT_RUN(test_discovered_read_absent_thread_fails_closed);
	UT_RUN(test_valid_bak_blocks_corrupt_primary);
	UT_RUN(test_storage_contract_fails_before_cf_or_file_io);
	UT_RUN(test_single_node_local_probe_fails_before_cf_or_file_io);
	UT_RUN(test_activate_and_stale_token);
	UT_RUN(test_restore_bit22_latch_from_active_root);
	UT_RUN(test_unbound_cutover_mutators_fail_before_cf_and_preserve_prepared_root);
	UT_RUN(test_native_cf_hold_cannot_authorize_strong_read);
	UT_RUN(test_activation_rejects_changed_source_wal_bytes);
	UT_RUN(test_activation_rejects_same_node_thread_claim_drift);
	UT_RUN(test_forbidden_patch_rejected_before_cf_and_file_io);
	UT_RUN(test_lookup_and_revalidate_use_exact_primary_identity);
	UT_RUN(test_lifecycle_publish_exact_token_cas);
	UT_RUN(test_retention_expanding_publish_refuses_before_cf_without_walr);
	UT_RUN(test_retention_expanding_publish_holds_walr_around_cf_and_readback);
	UT_RUN(test_unbound_publisher_fails_before_cf_and_preserves_root);
	UT_RUN(test_owner_rejoin_rejects_non_new_incarnation);
	UT_RUN(test_owner_rejoin_advances_exact_lineage_and_exhausts_at_max);
	UT_RUN(test_lifecycle_frozen_shape_matrix);
	UT_RUN(test_initial_migration_requires_lineage_one);
	UT_RUN(test_unconfirmed_release_returns_no_authority);
	UT_RUN(test_primary_rename_failure_is_not_success);
	UT_RUN(test_reserved_bytes_and_symlink_fail_closed);
	UT_RUN(test_history_exact_empty_and_full_set_preserves_inputs);
	UT_RUN(test_history_rejects_outer_shape_crc_hash_and_identity);
	UT_RUN(test_history_rejects_bad_records_and_namespace_aliases);
	UT_RUN(test_history_refuses_unselected_and_invalid_arguments);
	UT_RUN(test_history_refuses_alias_before_clearing_output);
	UT_RUN(test_v2_decodes_exact_common_and_two_thread_fields);
	UT_RUN(test_v2_encoder_preserves_exact_bytes_and_publishers);
	UT_RUN(test_v2_versions_are_independent_and_strict);
	UT_RUN(test_v2_checks_all_three_crc_layers);
	UT_RUN(test_v2_reserved_bytes_and_partial_holes_are_rejected);
	UT_RUN(test_v2_requires_object_references_and_bound_membership);
	UT_RUN(test_v2_identity_and_node_thread_cannot_be_substituted);
	UT_RUN(test_v2_database_state_validation_is_not_an_open_decision);
	UT_RUN(test_v2_bad_arguments_and_size_clear_output);
	UT_RUN(test_v2_encoder_refuses_bad_logical_fields_without_bytes);
	UT_RUN(test_v2_max_generations_remain_readable_without_advancement);
	UT_RUN(test_v1_io_does_not_silently_consume_or_convert_v2);
	UT_RUN(test_v3_preserves_pending_initialization_in_exact_root);
	UT_RUN(test_v3_empty_pending_does_not_implicitly_downgrade_format);
	UT_RUN(test_v2_reader_still_refuses_startup_capable_root);
	UT_RUN(test_v3_refuses_incomplete_or_serving_pending_ownership);
	UT_RUN(test_v3_all_reserved_and_crc_boundaries_remain_strict);
	UT_RUN(test_v3_encoder_cannot_erase_pending_by_downcast_or_hole);
	UT_RUN(test_v3_pending_high_origin_and_recovered_input_are_preserved);
	UT_RUN(test_v3_size_identity_versions_and_alias_failure_clear_output);
	UT_RUN(test_startup_selected_phases_retain_exact_old_and_new_writers);
	UT_RUN(test_startup_envelope_integrity_and_selection_are_required);
	UT_RUN(test_startup_operation_identity_and_phase_cannot_be_invented);
	UT_RUN(test_startup_predecessor_is_exact_not_just_same_origin);
	UT_RUN(test_startup_input_and_fresh_segment_boundaries);
	UT_RUN(test_startup_new_claim_requires_same_database_and_fresh_writer);
	UT_RUN(test_startup_unfinished_phase_has_no_successor_or_prefix);
	UT_RUN(test_startup_durable_prefix_must_bind_new_checkpoint_and_claim);
	UT_RUN(test_startup_recovery_import_and_old_config_remain_distinct);
	UT_RUN(test_startup_invalid_arguments_and_aliases_cannot_leave_partial_input);
	UT_RUN(test_v2_view_selects_exact_hash_not_decoy_or_projection);
	UT_RUN(test_v2_view_requires_clusterwide_lock_and_verified_storage);
	UT_RUN(test_v2_view_valid_backup_never_substitutes_for_current);
	UT_RUN(test_v2_view_backup_divergence_and_degraded_are_distinct);
	UT_RUN(test_v2_view_selected_object_failure_clears_valid_root);
	UT_RUN(test_v2_view_token_covers_whole_root_not_only_control_hash);
	UT_RUN(test_v2_view_rejects_v1_and_invalid_input_without_conversion);
	UT_RUN(test_v2_view_single_node_does_not_bypass_shared_storage_qualification);
	UT_RUN(test_v2_thread_view_selects_each_exact_thread);
	UT_RUN(test_v2_thread_view_uses_lifecycle_not_old_clean_anchor);
	UT_RUN(test_v2_thread_view_rejects_false_clean_or_unsupported_native_state);
	UT_RUN(test_v2_thread_view_rejects_stale_caller_and_absent_record);
	UT_RUN(test_v2_thread_view_clears_root_after_missing_anchor);
	UT_RUN(test_v2_thread_view_rejects_selected_foreign_or_inconsistent_anchor);
	UT_RUN(test_v2_checkpoint_advances_one_thread_and_preserves_common);
	UT_RUN(test_v2_shutdown_checkpoint_evidence_is_not_clean_close);
	UT_RUN(test_v2_stop_observation_does_not_close_or_publish);
	UT_RUN(test_v2_stop_observation_rejects_late_wal_and_changed_owner);
	UT_RUN(test_v2_stop_observation_accepts_exact_page_and_segment_end);
	UT_RUN(test_v2_stop_observation_requires_real_shutdown_wal);
	UT_RUN(test_v2_stop_observation_rechecks_root_and_pinned_wal);
	UT_RUN(test_v2_stop_observation_error_releases_file_and_walr_ownership);
	UT_RUN(test_v2_shutdown_checkpoint_rejects_wrong_purpose_or_evidence);
	UT_RUN(test_v2_shutdown_checkpoint_accepts_real_zero_crc);
	UT_RUN(test_v2_shutdown_checkpoint_races_preserve_durable_fact_not_close);
	UT_RUN(test_v2_checkpoint_requires_actual_wal_record);
	UT_RUN(test_v2_checkpoint_accepts_actual_zero_crc);
	UT_RUN(test_v2_checkpoint_requires_exact_durable_prefix);
	UT_RUN(test_v2_checkpoint_rechecks_durable_prefix);
	UT_RUN(test_v2_checkpoint_wal_continuation);
	UT_RUN(test_v2_checkpoint_rejects_replaced_wal_directory);
	UT_RUN(test_v2_checkpoint_rejects_replaced_wal_segment);
	UT_RUN(test_v2_checkpoint_preserves_historical_parameter_requirements);
	UT_RUN(test_v2_checkpoint_rejects_invalid_parameter_before_max);
	UT_RUN(test_v2_checkpoint_cannot_invent_parameter_transition_proof);
	UT_RUN(test_v2_checkpoint_rejects_non_owner_facts_before_io);
	UT_RUN(test_v2_checkpoint_rejects_unflushed_wrong_tli_and_bad_inputs);
	UT_RUN(test_v2_checkpoint_cas_and_epoch_races_do_not_overwrite);
	UT_RUN(test_v2_checkpoint_boundaries_refuse_without_mutation);
	UT_RUN(test_v2_checkpoint_root_io_failure_keeps_old_selection);
	UT_RUN(test_v2_checkpoint_postwrite_failure_keeps_fact_but_no_success);
	UT_RUN(test_v2_checkpoint_projection_follows_root_and_cannot_roll_it_back);
	UT_RUN(test_v2_checkpoint_error_unwind_releases_owned_work);
	UT_RUN(test_v2_thread_view_requires_physical_selected_claim);
	UT_RUN(test_runtime_v3_runtime_native_reader_selects_own_thread);
	UT_RUN(test_v2_retention_reader_owns_exact_live_thread_and_cf);
	UT_RUN(test_v2_retention_refusals_clear_all_outputs);
	UT_RUN(test_v2_retention_exception_releases_owned_cf);
	UT_RUN(test_v2_retention_exception_unconfirmed_release_is_fatal);
	UT_RUN(test_runtime_v3_canonical_strong_reads_recovery_required_peer);
	UT_RUN(test_runtime_v3_canonical_discovery_lookup_and_revalidate);
	UT_RUN(test_runtime_v3_canonical_no_missing_claim_or_lockfree_fallback);
	UT_RUN(test_runtime_v3_canonical_unconfirmed_release_is_fatal);
	UT_RUN(test_runtime_v3_service_read_waits_for_exact_retirement_before_output);
	UT_RUN(test_v2_stop_phase_uses_selected_raw_anchor_without_closing);
	UT_RUN(test_v2_stop_phase_accepts_durable_closed_successor_not_active);
	UT_RUN(test_v2_normal_close_publishes_exact_durable_root_not_voting_exit);
	UT_RUN(test_v2_normal_close_requires_real_controller_and_exact_native_cut);
	UT_RUN(test_v2_normal_close_pair_waits_for_last_thread_and_preserves_roster);
	UT_RUN(test_v2_normal_close_never_returns_success_after_uncertain_release);
	UT_RUN(test_v2_normal_close_authenticates_retained_clean_generations);
	UT_RUN(test_v2_normal_close_rechecks_closed_peer_persistent_tail);
	UT_RUN(test_v2_normal_close_refuses_missing_or_nonterminal_history);
	UT_RUN(test_v2_normal_close_pair_preserves_other_origin_history);
	UT_RUN(test_v2_stop_phase_refuses_wrong_owner_missing_claim_or_late_prefix);
	UT_RUN(test_runtime_v3_stop_phase_service_release_is_input_kind_and_cut_bound);
	UT_RUN(test_v2_stop_phase_error_cleans_cf_and_cannot_return_evidence);
	UT_RUN(test_runtime_v3_service_pending_observation_is_bound_to_input_and_cut);
	UT_RUN(test_runtime_v3_service_immediate_release_also_rechecks_observation_cut);
	UT_RUN(test_runtime_v3_canonical_foreign_absent_and_cf_refusals);
	UT_RUN(test_runtime_v3_canonical_error_releases_owned_lock);
	UT_RUN(test_runtime_v3_canonical_error_unconfirmed_cleanup_is_fatal);
	UT_RUN(test_v2_failure_open_invalidates_old_tail_and_keeps_other_threads);
	UT_RUN(test_v2_failure_tail_publishes_real_input_not_terminal);
	UT_RUN(test_runtime_v3_failure_launch_accepts_open_but_not_clean_thread);
	UT_RUN(test_runtime_v3_failure_launch_selects_interrupted_initializer_not_predecessor);
	UT_RUN(test_runtime_pending_observation_is_separate_and_requires_actual_inputs);
	UT_RUN(test_runtime_pending_worker_owns_exact_subject_before_inspection);
	UT_RUN(test_runtime_pending_owner_reads_actual_empty_and_checkpoint_wal);
	UT_RUN(test_runtime_pending_inspection_refuses_stale_or_unowned_input);
	UT_RUN(test_runtime_v3_lmon_launch_continues_only_after_exact_cf_retirement);
	UT_RUN(test_runtime_v3_failure_worker_seals_then_acquires_fresh_replay_owners);
	UT_RUN(test_runtime_v3_worker_window_consumes_its_sealed_authority_not_legacy_projection);
	UT_RUN(test_runtime_v3_failure_worker_entry_establishes_resource_owner);
	UT_RUN(test_v2_failure_input_preserves_fpw_was_off_history);
	UT_RUN(test_v2_failure_open_refuses_unproven_owners);
	UT_RUN(test_v2_failure_tail_refuses_unproven_or_borrowed_owners);
	UT_RUN(test_v2_failure_tail_rechecks_root_fence_serial_and_promise);
	UT_RUN(test_v2_failure_tail_requires_real_promised_input);
	UT_RUN(test_v2_failure_tail_cancellation_and_uncertain_cleanup);
	UT_RUN(test_v2_failure_worker_without_fence_never_changes_root);
	UT_RUN(test_v2_failure_sealed_tail_release_uncertainty_never_returns_authority);
	UT_RUN(test_runtime_v3_runtime_reader_never_uses_projection_for_bad_facts);
	UT_RUN(test_runtime_v3_runtime_native_inplace_identity_is_never_cleared);
	UT_RUN(test_v2_view_requires_exact_config_object);
	UT_RUN(test_bootstrap_composes_exact_threads_without_admission);
	UT_RUN(test_bootstrap_thread_lifecycle_overrides_old_clean_anchor);
	UT_RUN(test_bootstrap_closed_thread_requires_clean_anchor);
	UT_RUN(test_bootstrap_every_local_root_identity_must_match);
	UT_RUN(test_bootstrap_changed_root_is_not_a_partial_success);
	UT_RUN(test_bootstrap_absent_unconfigured_retired_or_revoked);
	UT_RUN(test_bootstrap_every_selected_object_is_required);
	UT_RUN(test_bootstrap_bad_input_lengths_and_alias_clear_output);
	UT_RUN(test_bootstrap_anchor_requires_exact_redo_and_no_backup);
	UT_RUN(test_bootstrap_prepared_observation_is_not_open);
	UT_RUN(test_bootstrap_read_exact_files_and_owned_config);
	UT_RUN(test_bootstrap_read_requires_independent_binding_and_node);
	UT_RUN(test_bootstrap_read_never_falls_back_to_valid_bak);
	UT_RUN(test_bootstrap_read_rejects_every_bad_selected_file);
	UT_RUN(test_bootstrap_read_unsafe_leaves_do_not_block_or_leak);
	UT_RUN(test_bootstrap_wal_route_exact_generation_and_refusals);
	UT_RUN(test_bootstrap_read_unsafe_directories_are_refused);
	UT_RUN(test_bootstrap_read_real_root_replacement_and_binding_races);
	UT_RUN(test_bootstrap_read_invalid_paths_outputs_and_alias);
	UT_RUN(test_bootstrap_read_pinned_directory_is_not_replacement);
	UT_RUN(test_bootstrap_read_close_failure_never_returns_partial_success);
	UT_RUN(test_bootstrap_capacity_includes_every_current_and_retained_source);
	UT_RUN(test_bootstrap_capacity_does_not_skip_retired_or_unconfigured_origin);
	UT_RUN(test_bootstrap_capacity_requires_nonlocal_and_history_objects);
	UT_RUN(test_bootstrap_capacity_reobserves_after_collection);
	UT_RUN(test_bootstrap_capacity_checks_nonlocal_content_not_just_file_presence);
	UT_RUN(test_bootstrap_capacity_final_directory_replacement_is_not_pinned_success);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
