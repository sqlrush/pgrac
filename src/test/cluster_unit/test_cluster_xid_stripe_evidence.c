/*-------------------------------------------------------------------------
 *
 * test_cluster_xid_stripe_evidence.c
 *    Read actual stripe slot bytes through the production selection policy.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_xid_stripe_evidence.c
 *
 * NOTES
 *    PGRAC-original tests. Spec: spec-s9p2-06-online-membership.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "cluster/cluster_voting_disk_io.h"
#include "cluster/cluster_xid_stripe.h"
#include "cluster/cluster_xid_stripe_boot.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_heap_horizon.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_inject.h"
#include "storage/condition_variable.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "access/twophase.h"
#include "test_cluster_pgproc_active_types.inc"
#include "storage/pmsignal.h"
#include "miscadmin.h"
#include "utils/wait_event.h"
#undef printf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool cluster_enabled;
bool cluster_xid_striping;
bool cluster_shared_catalog;
bool cluster_shared_config;
int cluster_node_id;
int cluster_xid_herding_slack = 4194304;
static uint64 local_next = 2052;
/* Only the new composition cases use these native PGPROC/cache inputs. */
static VariableCacheData variables;
VariableCache ShmemVariableCache = &variables;
static PROC_HDR procs;
PROC_HDR *ProcGlobal = &procs;
static char active_array_storage[offsetof(ProcArrayStruct, pgprocnos) + 8 * sizeof(int)]
	pg_attribute_aligned(MAXIMUM_ALIGNOF);
#define active_array (*(ProcArrayStruct *) active_array_storage)
static ProcArrayStruct *procArray = &active_array;
static TransactionId active_xids[8];
static PGPROC active_procs[8];
static bool active_sampling, active_recovery;
static LWLock *active_locks[3];
static unsigned active_depth, active_lock_errors, active_array_reads;
static void (*after_future_hook)(void);

bool
RecoveryInProgress(void)
{
	return active_recovery;
}
static bool stall_herding;
bool IsUnderPostmaster;
volatile sig_atomic_t InterruptPending;
/* Linux's fast path must consult this fixture's actual liveness observation,
 * including its parent-death case; a zero flag would skip that check. */
volatile sig_atomic_t postmaster_possibly_dead = 1;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static int error_level, error_code;
static unsigned prepares, sleeps, cancels, wait_action;
static bool postmaster_alive = true;
static uint64 incarnation = 81;
static bool outside_qvotec;
static ClusterXidStripeSampleFence sample_fence;
static bool in_quorum = true;
static bool xid_lock_held, change_inc_on_release;
static LWLockPadded locks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = locks;
static unsigned allowed_writes = 3, writes, premature_grants, wakeups;
static unsigned read_errors;
static unsigned history_writes, allowed_history_writes = 3;
static unsigned history_read_errors;
static unsigned history_write_errors, source_sync_writes, allowed_source_sync_writes = 3;
static int disks[3];
static FullTransactionId pending_candidate;
static ClusterSemanticActiveSampleFence semantic_fence;
static ClusterHeapFreezeCohortObservation cohort;
static ClusterHeapFreezeCohortStatus cohort_status;
static unsigned cohort_reads, sync_writes, tracking_writes, premature_floor_arms;
static uint64 forbidden_floor;
static void (*slot_write_hook)(void);
static void (*exclusive_release_hook)(void);
static bool exclusive_lock;

bool
cluster_semantic_activation_active_sample_current(const ClusterSemanticActiveSampleFence *expected)
{
	return expected != NULL && memcmp(expected, &semantic_fence, sizeof(*expected)) == 0;
}

ClusterHeapFreezeCohortStatus
cluster_heap_freeze_cohort_observe(ClusterHeapFreezeCohortObservation *out)
{
	cohort_reads++;
	if (cohort_status == CLUSTER_HEAP_FREEZE_COHORT_READY)
		*out = cohort;
	return cohort_status;
}
uint64
cluster_qvotec_self_incarnation_value(void)
{
	return outside_qvotec ? 0 : incarnation;
}
uint64
cluster_qvotec_get_self_incarnation(void)
{
	return incarnation;
}
bool
cluster_qvotec_in_quorum(void)
{
	return in_quorum;
}
void
ConditionVariableBroadcast(ConditionVariable *cv)
{
	wakeups++;
}
void
cluster_xid_stripe_lazy_latch(void)
{}
FullTransactionId
ReadNextFullTransactionId(void)
{
	return FullTransactionIdFromU64(local_next);
}
bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	if (active_sampling) {
		if (mode != LW_SHARED || active_depth >= lengthof(active_locks))
			abort();
		if (lock == ProcArrayLock) {
			active_array_reads++;
			if (active_depth != 0 || xid_lock_held)
				active_lock_errors++;
		}
		if (lock == XidGenLock) {
			if (active_depth != 0)
				active_lock_errors++;
			xid_lock_held = true;
		}
		active_locks[active_depth++] = lock;
	}
	exclusive_lock = mode == LW_EXCLUSIVE;
	return true;
}
bool
LWLockHeldByMe(LWLock *lock)
{
	return lock == XidGenLock && xid_lock_held;
}
void
LWLockRelease(LWLock *lock)
{
	if (active_sampling) {
		if (active_depth == 0 || active_locks[--active_depth] != lock)
			abort();
		if (lock == XidGenLock) {
			xid_lock_held = false;
			if (after_future_hook != NULL) {
				void (*hook)(void) = after_future_hook;
				after_future_hook = NULL;
				hook();
			}
		}
	}
	if (exclusive_lock && exclusive_release_hook != NULL) {
		void (*hook)(void) = exclusive_release_hook;
		exclusive_release_hook = NULL;
		hook();
	}
	exclusive_lock = false;
	if (change_inc_on_release) {
		incarnation++;
		change_inc_on_release = false;
	}
}
const ClusterNodeInfo *
cluster_conf_lookup_node(int32 node)
{
	static ClusterNodeInfo info;
	return node == cluster_node_id ? &info : NULL;
}
bool
cluster_cr_injection_armed(const char *name, uint64 *param)
{
	return stall_herding;
}
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	abort();
}
#undef errstart
#undef errstart_cold
bool
errstart(int level, const char *domain)
{
	error_level = level;
	return level >= ERROR;
}
bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}
int
errmsg_internal(const char *fmt, ...)
{
	return 0;
}
int
errmsg(const char *fmt, ...)
{
	return 0;
}
void
errfinish(const char *file, int line, const char *func)
{
	if (error_level >= ERROR)
		pg_re_throw();
}
int
errcode(int code)
{
	error_code = code;
	return 0;
}
void
pg_re_throw(void)
{
	if (PG_exception_stack)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}
void
ProcessInterrupts(void)
{
	error_code = ERRCODE_QUERY_CANCELED;
	pg_re_throw();
}
bool
PostmasterIsAliveInternal(void)
{
	return postmaster_alive;
}

/* Only replace the timed/O_DIRECT transport; CRC, short-read classification,
 * generation selection and all comparisons are actual product functions. */
ClusterVotingDiskIoState
cluster_voting_disk_read_stripe_slot(int fd, uint32 node, void *bytes)
{
	for (int i = 0; i < 3; i++)
		if (fd == disks[i] && (read_errors & (1u << i)))
			return CLUSTER_VOTING_DISK_IO_FAILED;
	return pread(fd, bytes, CLUSTER_VOTING_SLOT_BYTES, CLUSTER_VOTING_STRIPE_SLOT_OFFSET(node))
				   == CLUSTER_VOTING_SLOT_BYTES
			   ? CLUSTER_VOTING_DISK_IO_OK
			   : CLUSTER_VOTING_DISK_IO_FAILED;
}

ClusterVotingDiskIoState
cluster_voting_disk_write_stripe_slot(int fd, uint32 node, const void *bytes)
{
	if (cluster_xid_stripe_lease_ready(pending_candidate))
		premature_grants++;
	if (++writes > allowed_writes)
		return CLUSTER_VOTING_DISK_IO_FAILED;
	if (pwrite(fd, bytes, CLUSTER_VOTING_SLOT_BYTES, CLUSTER_VOTING_STRIPE_SLOT_OFFSET(node))
			!= CLUSTER_VOTING_SLOT_BYTES
		|| fsync(fd) != 0)
		abort();
	return CLUSTER_VOTING_DISK_IO_OK;
}
ClusterVotingDiskIoState
cluster_voting_disk_write_stripe_slot_ex(int fd, uint32 node, const void *bytes, bool sync)
{
	const ClusterXidStripeSlotRecord *record = bytes;
	if (sync)
		sync_writes++;
	else
		tracking_writes++;
	if (forbidden_floor != 0
		&& U64FromFullTransactionId(cluster_xid_stripe_herding_floor()) >= forbidden_floor)
		premature_floor_arms++;
	if (slot_write_hook != NULL) {
		void (*hook)(void) = slot_write_hook;
		slot_write_hook = NULL;
		hook();
	}
	if (sync && record->lease_incarnation != incarnation) {
		if (++source_sync_writes > allowed_source_sync_writes)
			return CLUSTER_VOTING_DISK_IO_FAILED;
		if (pwrite(fd, bytes, CLUSTER_VOTING_SLOT_BYTES, CLUSTER_VOTING_STRIPE_SLOT_OFFSET(node))
				!= CLUSTER_VOTING_SLOT_BYTES
			|| fsync(fd) != 0)
			abort();
		return CLUSTER_VOTING_DISK_IO_OK;
	}
	return cluster_voting_disk_write_stripe_slot(fd, node, bytes);
}

ClusterVotingDiskRawReadState
cluster_voting_disk_read_raw_slot_at(int fd, off_t offset, void *bytes)
{
	ssize_t n;
	for (int i = 0; i < 3; i++)
		if (fd == disks[i] && ((read_errors | history_read_errors) & (1u << i)))
			return CLUSTER_VOTING_DISK_RAW_READ_IO_FAILED;
	n = pread(fd, bytes, CLUSTER_VOTING_SLOT_BYTES, offset);
	return n == CLUSTER_VOTING_SLOT_BYTES ? CLUSTER_VOTING_DISK_RAW_READ_FULL
		   : n == 0						  ? CLUSTER_VOTING_DISK_RAW_READ_CLEAN_EOF
		   : n > 0						  ? CLUSTER_VOTING_DISK_RAW_READ_SHORT
										  : CLUSTER_VOTING_DISK_RAW_READ_IO_FAILED;
}

ClusterVotingDiskIoState
cluster_voting_disk_write_raw_slot_at(int fd, off_t offset, const void *bytes)
{
	for (int i = 0; i < 3; i++)
		if (fd == disks[i] && (history_write_errors & (1u << i)))
			return CLUSTER_VOTING_DISK_IO_FAILED;
	if (++history_writes > allowed_history_writes)
		return CLUSTER_VOTING_DISK_IO_FAILED;
	if (pwrite(fd, bytes, CLUSTER_VOTING_SLOT_BYTES, offset) != CLUSTER_VOTING_SLOT_BYTES
		|| fsync(fd) != 0)
		abort();
	return CLUSTER_VOTING_DISK_IO_OK;
}

#include "test_cluster_xid_stripe_evidence.inc"

/* The original policy predicate now consumes the original stripe-current
 * implementation already compiled above; no bool fence stub intervenes. */
#define cluster_heap_freeze_policy_sample_current_v1 actual_saved_policy_current
#include "test_cluster_heap_freeze_policy_current.inc"
#undef cluster_heap_freeze_policy_sample_current_v1

/* Only the bounded published-watermark observation remains a leaf here.
 * Its caller must already have released the original XidGenLock. */
static bool policy_watermark_available;
static uint64 policy_watermark_min;
static int policy_watermark_revoke;
static unsigned policy_watermark_reads;

bool
cluster_heap_freeze_watermark_sample(ClusterHeapFreezeWatermark *out)
{
	UT_ASSERT(active_sampling && !xid_lock_held && !exclusive_lock);
	UT_ASSERT_EQ(active_depth, 0);
	policy_watermark_reads++;
	switch (policy_watermark_revoke) {
	case 1: incarnation++; break;
	case 2: in_quorum = false; break;
	case 3: pg_atomic_write_u32(&StripeLease->failed, 1); break;
	case 4: StripeBootShmem->generation++; break;
	}
	if (!policy_watermark_available) return false;
	memset(out, 0, sizeof(*out));
	out->active_full_xid_min = policy_watermark_min;
	return true;
}

#define cluster_heap_freeze_policy_sample_v1 actual_saved_policy_sample
#include "test_cluster_heap_freeze_policy_sample.inc"
#undef cluster_heap_freeze_policy_sample_v1
#include "test_cluster_xid_stripe_active.inc"

static ClusterXidStripeSlotRecord rows[3];
static ClusterXidStripeLease lease;
static ClusterXidStripeBootShmem boot;

void
ConditionVariablePrepareToSleep(ConditionVariable *cv)
{
	prepares++;
}
bool
ConditionVariableTimedSleep(ConditionVariable *cv, long timeout, uint32 event)
{
	UT_ASSERT_EQ(event, PG_WAIT_EXTENSION);
	UT_ASSERT_EQ(timeout, 1000);
	if (++sleeps > 5)
		abort();
	if (sleeps == 3) {
		if (wait_action == 1)
			InterruptPending = true;
		else if (wait_action == 2)
			in_quorum = false;
		else if (wait_action == 3)
			postmaster_alive = false;
		else {
			pg_atomic_write_u64(&lease.floor_full, 4100);
			pg_atomic_write_u64(&lease.limit_full, 8196);
		}
	}
	return true; /* timeout notification, not a transaction error */
}
bool
ConditionVariableCancelSleep(void)
{
	cancels++;
	return true;
}

/* These fixtures only supply boot publication and local lock plumbing. The
 * actual claim/retire/herding functions and their disk policy run above. */
bool
cluster_xid_stripe_get_activation(uint64 *floor, uint64 *epoch, uint64 *generation)
{
	if (floor)
		*floor = boot.floor_full;
	return true;
}
ClusterXidStripeSlotState
cluster_xid_stripe_slot_state(void)
{
	return boot.slot_state;
}
ClusterXidStripeDiskState
cluster_xid_stripe_disk_state(void)
{
	return boot.disk_state;
}
void
cluster_xid_stripe_scan_disks(const int *fds, int n_disks)
{}

static void
reset_lease(void)
{
	active_sampling = active_recovery = false;
	after_future_hook = NULL;
	memset(&lease, 0, sizeof(lease));
	pg_atomic_init_u64(&lease.floor_full, 0);
	pg_atomic_init_u64(&lease.limit_full, 0);
	pg_atomic_init_u64(&lease.published_incarnation, 0);
	pg_atomic_init_u32(&lease.failed, 0);
	StripeLease = &lease;
	incarnation++;
	outside_qvotec = false;
	xid_lock_held = true;
	change_inc_on_release = false;
	in_quorum = cluster_enabled = cluster_shared_catalog = cluster_shared_config = cluster_xid_striping = true;
	cluster_node_id = 4;
	allowed_writes = 3;
	writes = premature_grants = wakeups = 0;
	read_errors = 0;
	history_read_errors = 0;
	history_write_errors = source_sync_writes = 0;
	allowed_source_sync_writes = 3;
	local_next = 2052;
	stall_herding = false;
	prepares = sleeps = cancels = wait_action = 0;
	error_level = error_code = 0;
	InterruptPending = false;
	IsUnderPostmaster = postmaster_alive = true;
	memset(&boot, 0, sizeof(boot));
	boot.floor_full = boot.my_slot_floor_full = 1024;
	boot.epoch = boot.generation = 1;
	boot.my_slot_claimed = true;
	boot.my_owner_incarnation = 71; /* The immutable claim survives restarts. */
	boot.disk_state = CLUSTER_XID_STRIPE_DISK_PUBLISHED;
	boot.slot_state = CLUSTER_XID_STRIPE_SLOT_MINE;
	pg_atomic_init_u64(&boot.herding_floor_full, 0);
	pg_atomic_init_u64(&boot.cluster_min_active_hwm, 0);
	pg_atomic_init_u64(&boot.cluster_max_active_hwm, 0);
	pg_atomic_init_u32(&boot.wrap_barrier_done, 1);
	StripeBootShmem = &boot;
	pending_candidate = FullTransactionIdFromU64(2052);
	MemSet(&semantic_fence, 0, sizeof(semantic_fence));
	MemSet(&cohort, 0, sizeof(cohort));
	cohort_status = CLUSTER_HEAP_FREEZE_COHORT_READY;
	cohort_reads = sync_writes = tracking_writes = premature_floor_arms = 0;
	forbidden_floor = 0;
	slot_write_hook = exclusive_release_hook = NULL;
	exclusive_lock = false;
}

static void
reset_rows(void)
{
	history_writes = 0;
	allowed_history_writes = 3;
	memset(rows, 0, sizeof(rows));
	for (int i = 0; i < 3; i++) {
		UT_ASSERT_EQ(ftruncate(disks[i], 0), 0);
		rows[i].magic = CLUSTER_PGXS_MAGIC;
		rows[i].version = CLUSTER_PGXS_VERSION;
		rows[i].node_id = 4;
		rows[i].owner_incarnation = 71;
		rows[i].floor_full = 1024;
		rows[i].next_xid_hwm_full = 2048;
		rows[i].stride_mode_epoch = 1;
		rows[i].generation = 1;
	}
}

static StripeSlotReadClass
read_rows(ClusterXidStripeSlotRecord *out, bool reverse)
{
	int order[3];
	for (int i = 0; i < 3; i++) {
		char bytes[CLUSTER_VOTING_SLOT_BYTES] = { 0 };
		cluster_xid_stripe_slot_record_compute_crc(&rows[i]);
		memcpy(bytes, &rows[i], sizeof(rows[i]));
		if (pwrite(disks[i], bytes, sizeof(bytes), CLUSTER_VOTING_STRIPE_SLOT_OFFSET(4))
			!= sizeof(bytes))
			abort();
		order[i] = disks[reverse ? 2 - i : i];
	}
	memset(out, 0, sizeof(*out));
	return stripe_read_slot_all(order, 3, 4, out, STRIPE_READ_EVIDENCE);
}

UT_TEST(coherent_tracking_and_retirement_choose_latest)
{
	ClusterXidStripeSlotRecord out;
	reset_rows();
	rows[1].generation = 2;
	rows[1].next_xid_hwm_full = 4096;
	rows[2] = rows[1];
	rows[2].generation = 3;
	rows[2].retired = 1;
	for (int reverse = 0; reverse < 2; reverse++) {
		UT_ASSERT_EQ(read_rows(&out, reverse), STRIPE_READ_VALID);
		UT_ASSERT_EQ(out.generation, 3);
		UT_ASSERT_EQ(out.next_xid_hwm_full, 4096);
		UT_ASSERT_EQ(out.retired, 1);
	}
}

UT_TEST(equal_generation_disagreement_refuses_in_either_order)
{
	ClusterXidStripeSlotRecord out;
	reset_rows();
	rows[1].next_xid_hwm_full++;
	for (int reverse = 0; reverse < 2; reverse++)
		UT_ASSERT_EQ(read_rows(&out, reverse), STRIPE_READ_CORRUPT);
}

UT_TEST(newer_record_cannot_replace_claim_identity)
{
	ClusterXidStripeSlotRecord out;
	for (int field = 0; field < 3; field++) {
		reset_rows();
		rows[1].generation = 2;
		if (field == 0)
			rows[1].owner_incarnation++;
		else if (field == 1)
			rows[1].floor_full++;
		else
			rows[1].stride_mode_epoch++;
		for (int reverse = 0; reverse < 2; reverse++)
			UT_ASSERT_EQ(read_rows(&out, reverse), STRIPE_READ_CORRUPT);
	}
}

UT_TEST(newer_record_cannot_lower_hwm_or_clear_retired)
{
	ClusterXidStripeSlotRecord out;
	for (int field = 0; field < 2; field++) {
		reset_rows();
		rows[1].generation = 2;
		if (field == 0)
			rows[1].next_xid_hwm_full--;
		else
			rows[0].retired = rows[2].retired = 1;
		for (int reverse = 0; reverse < 2; reverse++)
			UT_ASSERT_EQ(read_rows(&out, reverse), STRIPE_READ_CORRUPT);
	}
}

UT_TEST(unsupported_nonempty_copy_is_not_an_absent_disk)
{
	ClusterXidStripeSlotRecord out;
	reset_rows();
	rows[1].version++;
	for (int reverse = 0; reverse < 2; reverse++)
		UT_ASSERT_EQ(read_rows(&out, reverse), STRIPE_READ_CORRUPT);
}

UT_TEST(absent_unreadable_and_partial_sectors_remain_distinct)
{
	ClusterXidStripeSlotRecord out;
	int unavailable[3] = { -1, -1, -1 };
	char fragment[17] = { 1 };
	reset_rows();
	UT_ASSERT_EQ(read_rows(&out, false), STRIPE_READ_VALID);
	for (int i = 0; i < 3; i++)
		UT_ASSERT_EQ(ftruncate(disks[i], 0), 0);
	UT_ASSERT_EQ(stripe_read_slot_all(disks, 3, 4, &out, STRIPE_READ_EVIDENCE), STRIPE_READ_ABSENT);
	UT_ASSERT_EQ(stripe_read_slot_all(unavailable, 3, 4, &out, STRIPE_READ_EVIDENCE),
				 STRIPE_READ_UNREADABLE);
	UT_ASSERT_EQ(pwrite(disks[0], fragment, sizeof(fragment), CLUSTER_VOTING_STRIPE_SLOT_OFFSET(4)),
				 sizeof(fragment));
	UT_ASSERT_EQ(stripe_read_slot_all(disks, 3, 4, &out, STRIPE_READ_EVIDENCE),
				 STRIPE_READ_CORRUPT);
}

UT_TEST(reservation_waits_for_durable_majority_and_retries_exact_range)
{
	ClusterXidStripeSlotRecord out;
	uint64 pending_limit;
	reset_lease();
	reset_rows();
	UT_ASSERT_EQ(read_rows(&out, false), STRIPE_READ_VALID);
	allowed_writes = 1;
	stripe_lease_tick(disks, 3, 2052);
	UT_ASSERT(!cluster_xid_stripe_lease_ready(pending_candidate));
	UT_ASSERT_EQ(wakeups, 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&lease.published_incarnation), 0);
	UT_ASSERT_EQ(stripe_read_slot_all(disks, 3, 4, &out, STRIPE_READ_EVIDENCE), STRIPE_READ_VALID);
	pending_limit = out.issued_limit_full;
	UT_ASSERT(pending_limit > 2052);
	allowed_writes = 3;
	writes = 0;
	stripe_lease_tick(disks, 3, 2052);
	UT_ASSERT_EQ(premature_grants, 0);
	UT_ASSERT(cluster_xid_stripe_lease_ready(pending_candidate));
	UT_ASSERT_EQ(pg_atomic_read_u64(&lease.limit_full), pending_limit);
	UT_ASSERT_EQ(pg_atomic_read_u64(&lease.published_incarnation), incarnation);
	UT_ASSERT_EQ(wakeups, 1);
	UT_ASSERT(!cluster_xid_stripe_lease_ready(FullTransactionIdFromU64(pending_limit)));
}

UT_TEST(restart_burns_unused_interval_and_refuses_old_candidates)
{
	uint64 prior_limit;
	ClusterXidStripeSlotRecord out;
	reset_lease();
	reset_rows();
	UT_ASSERT_EQ(read_rows(&out, false), STRIPE_READ_VALID);
	stripe_lease_tick(disks, 3, 2052);
	prior_limit = pg_atomic_read_u64(&lease.limit_full);
	UT_ASSERT(prior_limit > 2052);
	reset_lease();
	stripe_lease_tick(disks, 3, 2052);
	UT_ASSERT_EQ(pg_atomic_read_u64(&lease.floor_full), prior_limit);
	UT_ASSERT(!cluster_xid_stripe_lease_ready(FullTransactionIdFromU64(prior_limit - 16)));
	UT_ASSERT(cluster_xid_stripe_lease_ready(FullTransactionIdFromU64(prior_limit)));
	UT_ASSERT_EQ(premature_grants, 0);
}

UT_TEST(repeated_generations_keep_durable_history_before_new_grants)
{
	ClusterXidStripeSlotRecord current, archived;
	uint8 first[CLUSTER_VOTING_SLOT_BYTES], again[CLUSTER_VOTING_SLOT_BYTES];
	uint64 old_incarnation, ceiling;
	off_t offset = CLUSTER_VOTING_STRIPE_HISTORY_BASE + 4 * CLUSTER_VOTING_SLOT_BYTES;
	reset_lease();
	reset_rows();
	UT_ASSERT_EQ(read_rows(&current, false), STRIPE_READ_VALID);
	stripe_lease_tick(disks, 3, 2052);
	for (int generation = 1; generation <= 2; generation++) {
		old_incarnation = incarnation;
		ceiling = pg_atomic_read_u64(&lease.limit_full);
		reset_lease();
		history_writes = 0;
		stripe_lease_tick(disks, 3, 2052);
		UT_ASSERT_EQ(stripe_read_slot_all(disks, 3, 4, &current, STRIPE_READ_UPDATE),
					 STRIPE_READ_VALID);
		UT_ASSERT_EQ(current.history_generation, generation);
		UT_ASSERT_EQ(pread(disks[0], again, sizeof(again), offset), sizeof(again));
		memcpy(&archived, again, sizeof(archived));
		UT_ASSERT(cluster_xid_stripe_slot_record_valid(&archived, 4));
		UT_ASSERT_EQ(archived.lease_incarnation, old_incarnation);
		UT_ASSERT_EQ(archived.issued_limit_full, ceiling);
		UT_ASSERT_EQ(archived.history_generation, generation - 1);
		UT_ASSERT(current.lease_floor_full >= archived.issued_limit_full);
		if (generation == 1)
			memcpy(first, again, sizeof(first));
		offset += CLUSTER_XID_STRIDE * CLUSTER_VOTING_SLOT_BYTES;
	}
	offset = CLUSTER_VOTING_STRIPE_HISTORY_BASE + 4 * CLUSTER_VOTING_SLOT_BYTES;
	UT_ASSERT_EQ(pread(disks[0], again, sizeof(again), offset), sizeof(again));
	UT_ASSERT_EQ(memcmp(first, again, sizeof(first)), 0);
}

UT_TEST(history_majority_precedes_activation_and_preserves_old_mapping)
{
	ClusterXidStripeSlotRecord current, history;
	uint64 old_incarnation, old_limit, resolved = 999;
	uint8 bytes[CLUSTER_VOTING_SLOT_BYTES];
	off_t offset = CLUSTER_VOTING_STRIPE_HISTORY_BASE + 4 * CLUSTER_VOTING_SLOT_BYTES;
	reset_lease();
	reset_rows();
	UT_ASSERT_EQ(read_rows(&current, false), STRIPE_READ_VALID);
	stripe_lease_tick(disks, 3, 2052);
	old_incarnation = incarnation;
	old_limit = pg_atomic_read_u64(&lease.limit_full);
	reset_lease();
	allowed_history_writes = 1;
	stripe_lease_tick(disks, 3, 2052);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&lease.limit_full), 0);
	/* Visible identical bytes from the failed round are not fsync receipts. */
	history_writes = 0;
	source_sync_writes = 0;
	stripe_lease_tick(disks, 3, 2052);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&lease.limit_full), 0);
	history_writes = 0;
	source_sync_writes = 0;
	allowed_history_writes = 2;
	stripe_lease_tick(disks, 3, 2052);
	UT_ASSERT_EQ(pg_atomic_read_u64(&lease.floor_full), old_limit);
	UT_ASSERT(cluster_xid_stripe_lookup_incarnation_fds(disks, 3, FullTransactionIdFromU64(2052),
														&resolved));
	UT_ASSERT_EQ(resolved, old_incarnation);
	UT_ASSERT(cluster_xid_stripe_lookup_incarnation_fds(
		disks, 3, FullTransactionIdFromU64(old_limit), &resolved));
	UT_ASSERT_EQ(resolved, incarnation);
	/* The exact predecessor is bound by current quorum evidence. One
	 * durable copy plus a readable lagging replica still supplies that image. */
	read_errors = 1;
	UT_ASSERT(cluster_xid_stripe_lookup_incarnation_fds(
		disks, 3, FullTransactionIdFromU64(old_limit - 16), &resolved));
	UT_ASSERT_EQ(resolved, old_incarnation);
	read_errors = 0;
	UT_ASSERT_EQ(pread(disks[0], bytes, sizeof(bytes), offset), sizeof(bytes));
	memcpy(&history, bytes, sizeof(history));
	history.lease_incarnation++;
	cluster_xid_stripe_slot_record_compute_crc(&history);
	memcpy(bytes, &history, sizeof(history));
	UT_ASSERT_EQ(pwrite(disks[0], bytes, sizeof(bytes), offset), sizeof(bytes));
	resolved = 999;
	UT_ASSERT(!cluster_xid_stripe_lookup_incarnation_fds(disks, 3, FullTransactionIdFromU64(2052),
														 &resolved));
	UT_ASSERT_EQ(resolved, 999);
}

UT_TEST(history_offset_and_nonmatching_image_fail_closed)
{
	ClusterXidStripeSlotRecord current;
	off_t offset = -7;
	uint8 garbage[CLUSTER_VOTING_SLOT_BYTES] = { 1 };
	UT_ASSERT(!stripe_history_offset(4, UINT64_MAX, &offset));
	UT_ASSERT(!stripe_history_offset(16, 1, &offset));
	UT_ASSERT(!stripe_history_offset(4, 0, &offset));
	UT_ASSERT_EQ(offset, -7);
	reset_lease();
	reset_rows();
	UT_ASSERT_EQ(read_rows(&current, false), STRIPE_READ_VALID);
	stripe_lease_tick(disks, 3, 2052);
	reset_lease();
	UT_ASSERT(stripe_history_offset(4, 1, &offset));
	UT_ASSERT_EQ(pwrite(disks[2], garbage, sizeof(garbage), offset), sizeof(garbage));
	stripe_lease_tick(disks, 3, 2052);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(history_writes, 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&lease.limit_full), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&lease.failed), 1);
}

UT_TEST(first_lease_requires_readable_history_capacity)
{
	ClusterXidStripeSlotRecord current;
	reset_lease();
	reset_rows();
	UT_ASSERT_EQ(read_rows(&current, false), STRIPE_READ_VALID);
	history_read_errors = 6;
	stripe_lease_tick(disks, 3, 2052);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&lease.limit_full), 0);
	/* A regular test file may grow; unreadable raw capacity is not absence. */
	history_read_errors = 0;
	stripe_lease_tick(disks, 3, 2052);
	UT_ASSERT(cluster_xid_stripe_lease_ready(pending_candidate));
}

UT_TEST(partial_archive_freezes_herding_image_until_activation)
{
	ClusterXidStripeSlotRecord current, previous;
	uint64 ceiling;
	reset_lease();
	reset_rows();
	UT_ASSERT_EQ(read_rows(&current, false), STRIPE_READ_VALID);
	stripe_lease_tick(disks, 3, 2052);
	UT_ASSERT_EQ(stripe_read_slot_all(disks, 3, 4, &previous, STRIPE_READ_UPDATE),
				 STRIPE_READ_VALID);
	ceiling = previous.issued_limit_full;
	reset_lease();
	local_next = ceiling + 16;
	allowed_source_sync_writes = 1;
	cluster_xid_stripe_herding_tick(disks, 3);
	UT_ASSERT_EQ(history_writes, 0);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&lease.limit_full), 0);
	allowed_source_sync_writes = 3;
	source_sync_writes = 0;
	allowed_history_writes = 1;
	cluster_xid_stripe_herding_tick(disks, 3);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&lease.limit_full), 0);
	UT_ASSERT_EQ(stripe_read_slot_all(disks, 3, 4, &current, STRIPE_READ_UPDATE),
				 STRIPE_READ_VALID);
	UT_ASSERT_EQ(memcmp(&current, &previous, sizeof(current)), 0);
	/* A second owner resumes the exact archive and burns the prior ceiling. */
	reset_lease();
	history_writes = 0;
	allowed_history_writes = 3;
	cluster_xid_stripe_herding_tick(disks, 3);
	UT_ASSERT_EQ(pg_atomic_read_u32(&lease.failed), 0);
	UT_ASSERT(cluster_xid_stripe_lease_ready(FullTransactionIdFromU64(ceiling)));
}

UT_TEST(partial_current_after_archive_is_burned_after_restart)
{
	ClusterXidStripeSlotRecord current;
	uint64 first_incarnation, abandoned_incarnation, ceiling, resolved;
	reset_lease();
	reset_rows();
	UT_ASSERT_EQ(read_rows(&current, false), STRIPE_READ_VALID);
	stripe_lease_tick(disks, 3, 2052);
	first_incarnation = incarnation;
	ceiling = pg_atomic_read_u64(&lease.limit_full);
	reset_lease();
	abandoned_incarnation = incarnation;
	allowed_writes = 1;
	cluster_xid_stripe_herding_tick(disks, 3);
	UT_ASSERT_EQ(pg_atomic_read_u64(&lease.limit_full), 0);
	UT_ASSERT_EQ(stripe_read_slot_all(disks, 3, 4, &current, STRIPE_READ_UPDATE),
				 STRIPE_READ_VALID);
	UT_ASSERT_EQ(current.lease_floor_full, ceiling);
	ceiling = current.issued_limit_full;
	reset_lease();
	history_writes = 0;
	cluster_xid_stripe_herding_tick(disks, 3);
	UT_ASSERT_EQ(pg_atomic_read_u64(&lease.floor_full), ceiling);
	UT_ASSERT(cluster_xid_stripe_lookup_incarnation_fds(disks, 3, FullTransactionIdFromU64(2052),
														&resolved));
	UT_ASSERT_EQ(resolved, first_incarnation);
	UT_ASSERT(cluster_xid_stripe_lookup_incarnation_fds(
		disks, 3, FullTransactionIdFromU64(ceiling - 16), &resolved));
	UT_ASSERT_EQ(resolved, abandoned_incarnation);
}

UT_TEST(archived_source_survives_loss_of_its_original_newest_copy)
{
	ClusterXidStripeSlotRecord current;
	uint8 bytes[CLUSTER_VOTING_SLOT_BYTES] = { 0 };
	uint64 ceiling;
	reset_lease();
	reset_rows();
	UT_ASSERT_EQ(read_rows(&current, false), STRIPE_READ_VALID);
	stripe_lease_tick(disks, 3, 2052);
	UT_ASSERT_EQ(stripe_read_slot_all(disks, 3, 4, &current, STRIPE_READ_UPDATE),
				 STRIPE_READ_VALID);
	ceiling = current.issued_limit_full;
	/* A legal tracking update reached only D0 before the old owner died. */
	current.next_xid_hwm_full += 16;
	current.generation++;
	cluster_xid_stripe_slot_record_compute_crc(&current);
	memcpy(bytes, &current, sizeof(current));
	UT_ASSERT_EQ(pwrite(disks[0], bytes, sizeof(bytes), CLUSTER_VOTING_STRIPE_SLOT_OFFSET(4)),
				 sizeof(bytes));
	reset_lease();
	history_write_errors = 1;
	allowed_writes = 0;
	cluster_xid_stripe_herding_tick(disks, 3);
	UT_ASSERT_EQ(pg_atomic_read_u64(&lease.limit_full), 0);
	UT_ASSERT_EQ(history_writes, 2);
	reset_lease();
	history_writes = 0;
	read_errors = 1;
	cluster_xid_stripe_herding_tick(disks, 3);
	UT_ASSERT_EQ(pg_atomic_read_u32(&lease.failed), 0);
	UT_ASSERT(cluster_xid_stripe_lease_ready(FullTransactionIdFromU64(ceiling)));
}

UT_TEST(renewal_preserves_floor_and_does_not_sync_per_xid)
{
	uint64 floor, limit;
	ClusterXidStripeSlotRecord out;
	reset_lease();
	reset_rows();
	UT_ASSERT_EQ(read_rows(&out, false), STRIPE_READ_VALID);
	stripe_lease_tick(disks, 3, 2052);
	floor = pg_atomic_read_u64(&lease.floor_full);
	limit = pg_atomic_read_u64(&lease.limit_full);
	writes = 0;
	stripe_lease_tick(disks, 3, 2068);
	UT_ASSERT_EQ(writes, 0);
	pending_candidate = FullTransactionIdFromU64(limit);
	stripe_lease_tick(disks, 3, limit - 16);
	UT_ASSERT_EQ(writes, 3);
	UT_ASSERT_EQ(pg_atomic_read_u64(&lease.floor_full), floor);
	UT_ASSERT(pg_atomic_read_u64(&lease.limit_full) > limit);
	UT_ASSERT_EQ(premature_grants, 0);
}

UT_TEST(unavailable_retired_and_overflow_cannot_grant)
{
	ClusterXidStripeSlotRecord out;
	reset_lease();
	reset_rows();
	UT_ASSERT_EQ(read_rows(&out, false), STRIPE_READ_VALID);
	in_quorum = false;
	stripe_lease_tick(disks, 3, 2052);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT(!cluster_xid_stripe_lease_ready(pending_candidate));
	in_quorum = true;
	for (int i = 0; i < 3; i++)
		rows[i].retired = 1;
	UT_ASSERT_EQ(read_rows(&out, false), STRIPE_READ_VALID);
	stripe_lease_tick(disks, 3, 2052);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&lease.failed), 1);
	reset_lease();
	reset_rows();
	UT_ASSERT_EQ(read_rows(&out, false), STRIPE_READ_VALID);
	stripe_lease_tick(disks, 3, UINT64_MAX - 10);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&lease.failed), 1);
}

UT_TEST(missing_read_majority_cannot_overwrite_unknown_reservations)
{
	ClusterXidStripeSlotRecord out;
	reset_lease();
	reset_rows();
	UT_ASSERT_EQ(read_rows(&out, false), STRIPE_READ_VALID);
	/* Reads fail while writes would still work: one stale readable copy
	 * cannot authorize overwriting the two unseen allocation ceilings. */
	read_errors = 6;
	stripe_lease_tick(disks, 3, 2052);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT(!cluster_xid_stripe_lease_ready(pending_candidate));
	read_errors = 0;
	stripe_lease_tick(disks, 3, 2052);
	UT_ASSERT(cluster_xid_stripe_lease_ready(pending_candidate));
}

UT_TEST(cached_reservation_cannot_hide_changed_claim)
{
	ClusterXidStripeSlotRecord out;
	reset_lease();
	reset_rows();
	UT_ASSERT_EQ(read_rows(&out, false), STRIPE_READ_VALID);
	stripe_lease_tick(disks, 3, 2052);
	UT_ASSERT(cluster_xid_stripe_lease_ready(pending_candidate));
	UT_ASSERT_EQ(stripe_read_slot_all(disks, 3, 4, &out, STRIPE_READ_EVIDENCE), STRIPE_READ_VALID);
	out.owner_incarnation++;
	out.generation++;
	for (int i = 0; i < 3; i++)
		rows[i] = out;
	UT_ASSERT_EQ(read_rows(&out, false), STRIPE_READ_VALID);
	writes = 0;
	stripe_lease_tick(disks, 3, 2068);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&lease.failed), 1);
	UT_ASSERT(!cluster_xid_stripe_lease_ready(pending_candidate));
}

UT_TEST(all_slot_writers_require_read_majority)
{
	ClusterXidStripeSlotRecord out;
	reset_lease();
	reset_rows();
	UT_ASSERT_EQ(read_rows(&out, false), STRIPE_READ_VALID);
	read_errors = 6;
	cluster_xid_stripe_herding_tick(disks, 3);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(stripe_service_retire(disks, 3, 4, 71), 0);
	UT_ASSERT_EQ(writes, 0);
	/* A single empty copy cannot prove that the slot was never claimed. */
	UT_ASSERT_EQ(ftruncate(disks[0], 0), 0);
	boot.slot_state = CLUSTER_XID_STRIPE_SLOT_ABSENT;
	UT_ASSERT_EQ(stripe_service_claim(disks, 3), 0);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(stripe_service_retire(disks, 3, 4, 71), 0);
	UT_ASSERT_EQ(writes, 0);
}

UT_TEST(partial_retirement_requires_durable_retry)
{
	ClusterXidStripeSlotRecord out;
	reset_lease();
	reset_rows();
	UT_ASSERT_EQ(read_rows(&out, false), STRIPE_READ_VALID);
	allowed_writes = 1;
	UT_ASSERT_EQ(stripe_service_retire(disks, 3, 4, 71), 0);
	writes = 0;
	UT_ASSERT_EQ(stripe_service_retire(disks, 3, 4, 71), 0);
	allowed_writes = 3;
	writes = 0;
	UT_ASSERT_EQ(stripe_service_retire(disks, 3, 4, 71), 1);
	UT_ASSERT_EQ(writes, 3);
}

UT_TEST(partial_first_claim_with_proven_empty_peers_can_resume)
{
	reset_lease();
	for (int i = 0; i < 3; i++)
		UT_ASSERT_EQ(ftruncate(disks[i], 0), 0);
	boot.slot_state = CLUSTER_XID_STRIPE_SLOT_ABSENT;
	allowed_writes = 1;
	UT_ASSERT_EQ(stripe_service_claim(disks, 3), 0);
	UT_ASSERT(!cluster_xid_stripe_lease_ready(pending_candidate));
	/* Boot can see MINE from the partial claim. Two confirmed empty slots
	 * carry read evidence; unlike two I/O failures, they hide no ceiling. */
	boot.slot_state = CLUSTER_XID_STRIPE_SLOT_MINE;
	allowed_writes = 3;
	writes = 0;
	stall_herding = true;
	cluster_xid_stripe_herding_tick(disks, 3);
	UT_ASSERT(!cluster_xid_stripe_lease_ready(pending_candidate));
	UT_ASSERT_EQ(writes, 3);
	writes = 0;
	cluster_xid_stripe_herding_tick(disks, 3);
	UT_ASSERT(cluster_xid_stripe_lease_ready(pending_candidate));
	UT_ASSERT_EQ(premature_grants, 0);
	/* The exception cannot repair a later allocation record from one copy. */
	reset_lease();
	for (int i = 1; i < 3; i++)
		UT_ASSERT_EQ(ftruncate(disks[i], 0), 0);
	stripe_lease_tick(disks, 3, 2052);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT(!cluster_xid_stripe_lease_ready(pending_candidate));
}

UT_TEST(herding_services_reservation_and_preserves_ceiling)
{
	ClusterXidStripeSlotRecord out;
	uint64 ceiling;
	reset_lease();
	reset_rows();
	UT_ASSERT_EQ(read_rows(&out, false), STRIPE_READ_VALID);
	cluster_xid_stripe_herding_tick(disks, 3);
	UT_ASSERT(cluster_xid_stripe_lease_ready(pending_candidate));
	ceiling = pg_atomic_read_u64(&lease.limit_full);
	reset_lease();
	/* Injection stalls herding, not durable allocator range service. */
	stall_herding = true;
	cluster_xid_stripe_herding_tick(disks, 3);
	UT_ASSERT_EQ(U64FromFullTransactionId(cluster_xid_stripe_herding_floor()), ceiling);
	UT_ASSERT(cluster_xid_stripe_lease_ready(FullTransactionIdFromU64(ceiling)));
}

static bool
try_wait(void)
{
	sigjmp_buf outer;
	bool success;

	PG_exception_stack = &outer;
	if (sigsetjmp(outer, 0) == 0) {
		cluster_xid_stripe_wait_lease(FullTransactionIdFromU64(2052));
		success = true;
	} else
		success = false;
	PG_exception_stack = NULL;
	return success;
}

UT_TEST(wait_timeouts_retry_and_new_floor_returns_for_rederivation)
{
	reset_lease();
	UT_ASSERT(try_wait());
	UT_ASSERT_EQ(sleeps, 3);
	UT_ASSERT_EQ(prepares, 1);
	UT_ASSERT_EQ(cancels, 1);
	UT_ASSERT_EQ(error_code, 0);
	UT_ASSERT(!cluster_xid_stripe_lease_ready(FullTransactionIdFromU64(2052)));
}

UT_TEST(wait_cancellation_quorum_loss_and_parent_death_cleanup)
{
	for (unsigned action = 1; action <= 3; action++) {
		reset_lease();
		wait_action = action;
		UT_ASSERT(!try_wait());
		UT_ASSERT_EQ(prepares, 1);
		UT_ASSERT_EQ(cancels, 1);
		UT_ASSERT_EQ(sleeps, 3);
		if (action == 1)
			UT_ASSERT_EQ(error_code, ERRCODE_QUERY_CANCELED);
		else if (action == 2)
			UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE);
		else
			UT_ASSERT_EQ(error_level, FATAL);
	}
}

/* Publish through the original majority-durable owner, not a ready stub. */
static void
prepare_future_lease(void)
{
	ClusterXidStripeSlotRecord out;

	reset_lease();
	reset_rows();
	UT_ASSERT_EQ(read_rows(&out, false), STRIPE_READ_VALID);
	stripe_lease_tick(disks, 3, local_next);
}

UT_TEST(future_requires_majority_and_current_lease_incarnation)
{
	FullTransactionId future = FullTransactionIdFromU64(99);
	ClusterXidStripeSlotRecord out;
	unsigned writes_before;

	reset_lease();
	reset_rows();
	UT_ASSERT_EQ(read_rows(&out, false), STRIPE_READ_VALID);
	allowed_writes = 1;
	stripe_lease_tick(disks, 3, local_next);
	UT_ASSERT(!cluster_xid_stripe_sample_future(FullTransactionIdFromU64(local_next), &future,
												&sample_fence));
	UT_ASSERT(!FullTransactionIdIsValid(future));
	allowed_writes = 3;
	writes = 0;
	stripe_lease_tick(disks, 3, local_next);
	writes_before = writes;
	UT_ASSERT(cluster_xid_stripe_sample_future(FullTransactionIdFromU64(local_next), &future,
											   &sample_fence));
	UT_ASSERT_EQ(U64FromFullTransactionId(future), 2052);
	UT_ASSERT_EQ(writes, writes_before);
	UT_ASSERT_EQ(local_next, 2052);
	UT_ASSERT(boot.my_owner_incarnation != incarnation);
	incarnation++;
	UT_ASSERT(!cluster_xid_stripe_sample_future(FullTransactionIdFromU64(local_next), &future,
												&sample_fence));
	UT_ASSERT(!FullTransactionIdIsValid(future));
	writes = 0;
	stripe_lease_tick(disks, 3, local_next);
	UT_ASSERT(cluster_xid_stripe_sample_future(FullTransactionIdFromU64(local_next), &future,
											   &sample_fence));
	UT_ASSERT_EQ(U64FromFullTransactionId(future), pg_atomic_read_u64(&lease.floor_full));
}

UT_TEST(future_uses_shared_incarnation_outside_qvotec)
{
	FullTransactionId future;

	prepare_future_lease();
	outside_qvotec = true;
	UT_ASSERT_EQ(cluster_qvotec_self_incarnation_value(), 0);
	UT_ASSERT_EQ(cluster_qvotec_get_self_incarnation(), incarnation);
	UT_ASSERT(cluster_xid_stripe_sample_future(FullTransactionIdFromU64(local_next), &future,
											   &sample_fence));
	UT_ASSERT_EQ(U64FromFullTransactionId(future), local_next);
}

UT_TEST(future_clamps_original_floors_without_changing_allocation)
{
	FullTransactionId future;
	unsigned writes_before;

	prepare_future_lease();
	writes_before = writes;
	boot.floor_full = 2055;
	UT_ASSERT(
		cluster_xid_stripe_sample_future(FullTransactionIdFromU64(2052), &future, &sample_fence));
	UT_ASSERT_EQ(U64FromFullTransactionId(future), 2068);
	boot.my_slot_floor_full = 2080;
	UT_ASSERT(
		cluster_xid_stripe_sample_future(FullTransactionIdFromU64(2052), &future, &sample_fence));
	UT_ASSERT_EQ(U64FromFullTransactionId(future), 2084);
	pg_atomic_write_u64(&boot.herding_floor_full, 2101);
	UT_ASSERT(
		cluster_xid_stripe_sample_future(FullTransactionIdFromU64(2052), &future, &sample_fence));
	UT_ASSERT_EQ(U64FromFullTransactionId(future), 2116);
	UT_ASSERT(
		cluster_xid_stripe_sample_future(FullTransactionIdFromU64(2120), &future, &sample_fence));
	UT_ASSERT_EQ(U64FromFullTransactionId(future), 2132);
	UT_ASSERT_EQ(local_next, 2052);
	UT_ASSERT_EQ(writes, writes_before);
}

UT_TEST(future_refuses_missing_or_changing_authority)
{
	for (unsigned fault = 0; fault < 22; fault++) {
		FullTransactionId future = FullTransactionIdFromU64(99);

		prepare_future_lease();
		switch (fault) {
		case 0:
			cluster_enabled = false;
			break;
		case 1:
			cluster_shared_catalog = false;
			break;
		case 2:
			cluster_xid_striping = false;
			break;
		case 3:
			cluster_node_id = -1;
			break;
		case 4:
			cluster_node_id = CLUSTER_XID_STRIDE;
			break;
		case 5:
			StripeBootShmem = NULL;
			break;
		case 6:
			StripeLease = NULL;
			break;
		case 7:
			in_quorum = false;
			break;
		case 8:
			incarnation = 0;
			break;
		case 9:
			pg_atomic_write_u64(&lease.published_incarnation, 0);
			break;
		case 10:
			pg_atomic_write_u32(&lease.failed, 1);
			break;
		case 11:
			boot.disk_state = CLUSTER_XID_STRIPE_DISK_UNKNOWN;
			break;
		case 12:
			boot.slot_state = CLUSTER_XID_STRIPE_SLOT_RETIRED;
			break;
		case 13:
			boot.my_slot_claimed = false;
			break;
		case 14:
			boot.my_owner_incarnation = 0;
			break;
		case 15:
			boot.epoch = 0;
			break;
		case 16:
			boot.generation = 0;
			break;
		case 17:
			boot.floor_full = 0;
			break;
		case 18:
			boot.my_slot_floor_full = 0;
			break;
		case 19:
			pg_atomic_write_u64(&lease.floor_full, 0);
			break;
		case 20:
			xid_lock_held = false;
			break;
		case 21:
			change_inc_on_release = true;
			break;
		}
		UT_ASSERT(!cluster_xid_stripe_sample_future(FullTransactionIdFromU64(local_next), &future,
													&sample_fence));
		UT_ASSERT(!FullTransactionIdIsValid(future));
	}
	prepare_future_lease();
	UT_ASSERT(!cluster_xid_stripe_sample_future(FullTransactionIdFromU64(local_next), NULL,
												&sample_fence));
}

UT_TEST(future_retains_lease_bounds_wrap_and_window_guards)
{
	FullTransactionId future;

	prepare_future_lease();
	UT_ASSERT(!cluster_xid_stripe_sample_future(
		FullTransactionIdFromU64(pg_atomic_read_u64(&lease.limit_full)), &future, &sample_fence));
	UT_ASSERT(!FullTransactionIdIsValid(future));
	UT_ASSERT(!cluster_xid_stripe_sample_future(FullTransactionIdFromU64(UINT64_MAX), &future,
												&sample_fence));
	UT_ASSERT(!FullTransactionIdIsValid(future));
	for (uint64 special = 0; special < FirstNormalTransactionId; special++) {
		UT_ASSERT(!cluster_xid_stripe_sample_future(FullTransactionIdFromU64(special), &future,
													&sample_fence));
		UT_ASSERT(!FullTransactionIdIsValid(future));
	}
	pg_atomic_write_u64(&boot.cluster_min_active_hwm, 1);
	cluster_xid_herding_slack = 1;
	UT_ASSERT(!cluster_xid_stripe_sample_future(FullTransactionIdFromU64(local_next), &future,
												&sample_fence));
	UT_ASSERT(!FullTransactionIdIsValid(future));
	cluster_xid_herding_slack = 4194304;
	pg_atomic_write_u64(&lease.floor_full, (uint64)UINT32_MAX - 32);
	pg_atomic_write_u64(&lease.limit_full, (uint64)UINT32_MAX + 32);
	pg_atomic_write_u32(&boot.wrap_barrier_done, 0);
	UT_ASSERT(!cluster_xid_stripe_sample_future(FullTransactionIdFromU64(UINT32_MAX - 1), &future,
												&sample_fence));
	UT_ASSERT(!FullTransactionIdIsValid(future));
	pg_atomic_write_u32(&boot.wrap_barrier_done, 1);
	pg_atomic_write_u64(&boot.cluster_min_active_hwm, 0);
	UT_ASSERT(cluster_xid_stripe_sample_future(FullTransactionIdFromU64(UINT32_MAX - 1), &future,
											   &sample_fence));
	UT_ASSERT_EQ(U64FromFullTransactionId(future), (UINT64CONST(1) << 32) + 4);
}

UT_TEST(sample_fence_keeps_original_durable_interval_across_renewal)
{
	FullTransactionId future;
	ClusterXidStripeSampleFence saved;
	unsigned writes_before;

	prepare_future_lease();
	outside_qvotec = true;
	UT_ASSERT(cluster_xid_stripe_sample_future(FullTransactionIdFromU64(local_next), &future,
											   &sample_fence));
	saved = sample_fence;
	UT_ASSERT_EQ(saved.incarnation, incarnation);
	UT_ASSERT_EQ(saved.activation_generation, boot.generation);
	UT_ASSERT_EQ(saved.slot_owner_incarnation, boot.my_owner_incarnation);
	UT_ASSERT_EQ(saved.lease_limit_full, pg_atomic_read_u64(&lease.limit_full));
	writes_before = writes;
	UT_ASSERT(cluster_xid_stripe_sample_fence_current(&saved));
	UT_ASSERT_EQ(writes, writes_before);
	/* Use the real sole publisher to extend the same durable promise. */
	outside_qvotec = false;
	writes = 0;
	stripe_lease_tick(disks, 3, saved.lease_limit_full);
	UT_ASSERT(pg_atomic_read_u64(&lease.limit_full) > saved.lease_limit_full);
	UT_ASSERT(cluster_xid_stripe_sample_fence_current(&saved));
	UT_ASSERT_EQ(memcmp(&sample_fence, &saved, sizeof(saved)), 0);
}

UT_TEST(sample_fence_rejects_replaced_or_revoked_authority)
{
	for (unsigned fault = 0; fault < 21; fault++) {
		FullTransactionId future;
		ClusterXidStripeSampleFence saved;

		prepare_future_lease();
		UT_ASSERT(cluster_xid_stripe_sample_future(FullTransactionIdFromU64(local_next), &future,
												   &saved));
		switch (fault) {
		case 0:
			incarnation++;
			break;
		case 1:
			in_quorum = false;
			break;
		case 2:
			pg_atomic_write_u32(&lease.failed, 1);
			break;
		case 3:
			pg_atomic_write_u64(&lease.limit_full, 0);
			break;
		case 4:
			pg_atomic_write_u64(&lease.limit_full, saved.lease_limit_full - 1);
			break;
		case 5:
			pg_atomic_write_u64(&lease.floor_full, saved.lease_floor_full + 16);
			break;
		case 6:
			pg_atomic_write_u64(&lease.published_incarnation, incarnation + 1);
			break;
		case 7:
			boot.floor_full++;
			break;
		case 8:
			boot.epoch++;
			break;
		case 9:
			boot.generation++;
			break;
		case 10:
			boot.my_slot_floor_full++;
			break;
		case 11:
			boot.my_owner_incarnation++;
			break;
		case 12:
			boot.my_slot_claimed = false;
			break;
		case 13:
			boot.slot_state = CLUSTER_XID_STRIPE_SLOT_ABSENT;
			break;
		case 14:
			boot.disk_state = CLUSTER_XID_STRIPE_DISK_ABSENT;
			break;
		case 15:
			cluster_node_id++;
			break;
		case 16:
			cluster_enabled = false;
			break;
		case 17:
			cluster_shared_catalog = false;
			break;
		case 18:
			cluster_xid_striping = false;
			break;
		case 19:
			StripeBootShmem = NULL;
			break;
		case 20:
			StripeLease = NULL;
			break;
		}
		UT_ASSERT(!cluster_xid_stripe_sample_fence_current(&saved));
	}
}

UT_TEST(sample_fence_failure_clears_outputs_and_refuses_empty_fence)
{
	FullTransactionId future;
	ClusterXidStripeSampleFence empty = { 0 };

	prepare_future_lease();
	UT_ASSERT(cluster_xid_stripe_sample_future(FullTransactionIdFromU64(local_next), &future,
											   &sample_fence));
	in_quorum = false;
	UT_ASSERT(!cluster_xid_stripe_sample_future(FullTransactionIdFromU64(local_next), &future,
												&sample_fence));
	UT_ASSERT(!FullTransactionIdIsValid(future));
	UT_ASSERT_EQ(memcmp(&sample_fence, &empty, sizeof(empty)), 0);
	in_quorum = true;
	UT_ASSERT(!cluster_xid_stripe_sample_fence_current(NULL));
	UT_ASSERT(!cluster_xid_stripe_sample_fence_current(&empty));
	UT_ASSERT(
		!cluster_xid_stripe_sample_future(FullTransactionIdFromU64(local_next), &future, NULL));
	UT_ASSERT(!FullTransactionIdIsValid(future));
}


/* The cohort/semantic dependencies are controlled here. Their real complete
 * fixed-set observer has its own fixture; IO/selection/lease/JUMP run above. */
static void
prepare_maintenance(void)
{
	FullTransactionId future;

	prepare_future_lease();
	writes = 0;
	cluster_xid_stripe_herding_tick(disks, 3);
	UT_ASSERT(cluster_xid_stripe_sample_future(FullTransactionIdFromU64(local_next), &future,
											   &sample_fence));
	xid_lock_held = false;
	semantic_fence.system_identifier = 300;
	semantic_fence.admission_publication_seq = 2;
	semantic_fence.active_bits = 1;
	semantic_fence.record_generation = cohort.record_generation = 7;
	semantic_fence.formation_epoch = cohort.formation_epoch = 9;
	semantic_fence.membership_cut_generation = 5;
	semantic_fence.local_boot_incarnation = incarnation;
	semantic_fence.local_node_id = cluster_node_id;
	semantic_fence.admitted_members_lo = cohort.admitted_members_lo = 17;
	semantic_fence.admitted_members_hi = cohort.admitted_members_hi = 2;
	semantic_fence.admitted_incarnation[4] = incarnation;
	semantic_fence.admitted_incarnation[65] = 105;
	cohort.future_full_xid_max = 8000004;
	cohort.active_full_xid_min = 100;
	cohort.horizon_scn = 20;
	writes = sync_writes = tracking_writes = cohort_reads = 0;
}

static ClusterXidStripeMaintenanceResult
request_maintenance(uint64 target)
{
	ClusterSemanticActiveSampleFence saved_identity = semantic_fence;
	ClusterXidStripeSampleFence saved_stripe = sample_fence;

	return cluster_xid_stripe_request_maintenance(&saved_identity, &saved_stripe,
												  FullTransactionIdFromU64(target));
}

UT_TEST(maintenance_stages_then_syncs_before_arming_original_floor)
{
	ClusterXidStripeSlotRecord out;
	uint64 old_limit;

	prepare_maintenance();
	old_limit = pg_atomic_read_u64(&lease.limit_full);
	forbidden_floor = 8196;
	UT_ASSERT_EQ(request_maintenance(8196), CLUSTER_XID_STRIPE_MAINTENANCE_PENDING);
	UT_ASSERT(boot.maintenance_pending);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT(U64FromFullTransactionId(cluster_xid_stripe_herding_floor()) < 8196);
	cluster_xid_stripe_herding_tick(disks, 3);
	UT_ASSERT_EQ(sync_writes, 3);
	UT_ASSERT_EQ(tracking_writes, 0);
	UT_ASSERT_EQ(premature_floor_arms, 0);
	UT_ASSERT_EQ(U64FromFullTransactionId(cluster_xid_stripe_herding_floor()), 8196);
	UT_ASSERT(!boot.maintenance_pending);
	UT_ASSERT_EQ(local_next, 2052); /* old allocations/ACTIVE identities unchanged */
	UT_ASSERT_EQ(stripe_read_slot_all(disks, 3, 4, &out, STRIPE_READ_UPDATE), STRIPE_READ_VALID);
	UT_ASSERT_EQ(out.version, 3);
	UT_ASSERT_EQ(out.next_xid_hwm_full, 8196);
	UT_ASSERT_EQ(out.issued_limit_full, old_limit);
	UT_ASSERT_EQ(out.lease_incarnation, incarnation);
	UT_ASSERT_EQ(request_maintenance(8196), CLUSTER_XID_STRIPE_MAINTENANCE_DURABLE);
	UT_ASSERT_EQ(writes, 3);
	UT_ASSERT(cohort_reads >= 2);
}

UT_TEST(maintenance_syncs_even_when_tracking_hwm_already_covers_request)
{
	prepare_maintenance();
	local_next = 16388;
	cluster_xid_stripe_herding_tick(disks, 3);
	UT_ASSERT_EQ(tracking_writes, 3);
	UT_ASSERT_EQ(boot.my_hwm_on_disk, 16388);
	UT_ASSERT(U64FromFullTransactionId(cluster_xid_stripe_herding_floor()) < 8196);
	writes = sync_writes = tracking_writes = 0;
	UT_ASSERT_EQ(request_maintenance(8196), CLUSTER_XID_STRIPE_MAINTENANCE_PENDING);
	cluster_xid_stripe_herding_tick(disks, 3);
	UT_ASSERT_EQ(sync_writes, 3);
	UT_ASSERT_EQ(tracking_writes, 0);
	UT_ASSERT_EQ(U64FromFullTransactionId(cluster_xid_stripe_herding_floor()), 8196);
	UT_ASSERT_EQ(boot.my_hwm_on_disk, 16388);
}

UT_TEST(maintenance_partial_write_retries_without_arming)
{
	prepare_maintenance();
	forbidden_floor = 8196;
	UT_ASSERT_EQ(request_maintenance(8196), CLUSTER_XID_STRIPE_MAINTENANCE_PENDING);
	allowed_writes = 1;
	cluster_xid_stripe_herding_tick(disks, 3);
	UT_ASSERT(boot.maintenance_pending);
	UT_ASSERT(U64FromFullTransactionId(cluster_xid_stripe_herding_floor()) < 8196);
	UT_ASSERT_EQ(premature_floor_arms, 0);
	UT_ASSERT_EQ(request_maintenance(8196), CLUSTER_XID_STRIPE_MAINTENANCE_PENDING);
	allowed_writes = 3;
	writes = sync_writes = 0;
	cluster_xid_stripe_herding_tick(disks, 3);
	UT_ASSERT_EQ(sync_writes, 3);
	UT_ASSERT_EQ(premature_floor_arms, 0);
	UT_ASSERT_EQ(U64FromFullTransactionId(cluster_xid_stripe_herding_floor()), 8196);
	UT_ASSERT(!boot.maintenance_pending);
}

UT_TEST(maintenance_requires_complete_current_cohort_before_staging)
{
	for (unsigned fault = 0; fault < 9; fault++) {
		ClusterXidStripeMaintenanceResult result;

		prepare_maintenance();
		if (fault < 5)
			cohort_status = CLUSTER_HEAP_FREEZE_COHORT_INCOMPLETE + fault;
		else if (fault == 5)
			cohort.record_generation++;
		else if (fault == 6)
			cohort.formation_epoch++;
		else if (fault == 7)
			cohort.admitted_members_lo ^= 1;
		else
			cohort.admitted_members_hi = 0;
		result = request_maintenance(8196);
		UT_ASSERT(result != CLUSTER_XID_STRIPE_MAINTENANCE_PENDING
				  && result != CLUSTER_XID_STRIPE_MAINTENANCE_DURABLE);
		UT_ASSERT(!boot.maintenance_pending);
		UT_ASSERT_EQ(writes, 0);
	}
}

UT_TEST(maintenance_refuses_unbounded_or_illegal_targets_without_io)
{
	for (unsigned fault = 0; fault < 12; fault++) {
		uint64 target = 8196;

		prepare_maintenance();
		switch (fault) {
		case 0:
			target = 0;
			break;
		case 1:
			target = 2;
			break;
		case 2:
			target++;
			break; /* another origin */
		case 3:
			cohort.future_full_xid_max = target - 1;
			break;
		case 4:
			target = local_next + cluster_xid_herding_slack + CLUSTER_XID_STRIDE;
			break;
		case 5:
			local_next = 400000004;
			target = cohort.future_full_xid_max = local_next + 16;
			pg_atomic_write_u64(&boot.cluster_min_active_hwm, 1024);
			break;
		case 6:
			local_next = UINT64_C(0x100002004);
			target = cohort.future_full_xid_max = local_next + 16;
			pg_atomic_write_u32(&boot.wrap_barrier_done, 0);
			break;
		case 7:
			local_next = UINT64_MAX - 27;
			target = cohort.future_full_xid_max = local_next + 16;
			break;
		case 8:
			xid_lock_held = true;
			break;
		case 9:
			boot.maintenance_seq = UINT64_MAX;
			break;
		case 10:
			cluster_shared_catalog = false;
			break;
		case 11:
			pg_atomic_write_u32(&lease.failed, 1);
			break;
		}
		UT_ASSERT_EQ(request_maintenance(target), CLUSTER_XID_STRIPE_MAINTENANCE_UNAVAILABLE);
		UT_ASSERT(!boot.maintenance_pending);
		UT_ASSERT_EQ(writes, 0);
	}
}

UT_TEST(maintenance_rechecks_cohort_and_retires_changed_identity)
{
	prepare_maintenance();
	UT_ASSERT_EQ(request_maintenance(8196), CLUSTER_XID_STRIPE_MAINTENANCE_PENDING);
	cohort_status = CLUSTER_HEAP_FREEZE_COHORT_INCOMPLETE;
	cluster_xid_stripe_herding_tick(disks, 3);
	UT_ASSERT(boot.maintenance_pending);
	UT_ASSERT_EQ(sync_writes, 0);
	cohort_status = CLUSTER_HEAP_FREEZE_COHORT_READY;
	semantic_fence.admitted_incarnation[65]++;
	cluster_xid_stripe_herding_tick(disks, 3);
	UT_ASSERT(!boot.maintenance_pending);
	UT_ASSERT_EQ(sync_writes, 0);
	UT_ASSERT(U64FromFullTransactionId(cluster_xid_stripe_herding_floor()) < 8196);
}

static void
change_maintenance_identity(void)
{
	semantic_fence.membership_cut_generation++;
}

UT_TEST(maintenance_identity_change_during_io_cannot_arm_on_later_tick)
{
	prepare_maintenance();
	UT_ASSERT_EQ(request_maintenance(8196), CLUSTER_XID_STRIPE_MAINTENANCE_PENDING);
	slot_write_hook = change_maintenance_identity;
	cluster_xid_stripe_herding_tick(disks, 3);
	UT_ASSERT_EQ(sync_writes, 3);
	UT_ASSERT(!boot.maintenance_pending);
	UT_ASSERT(U64FromFullTransactionId(cluster_xid_stripe_herding_floor()) < 8196);
	cluster_xid_stripe_herding_tick(disks, 3);
	UT_ASSERT(U64FromFullTransactionId(cluster_xid_stripe_herding_floor()) < 8196);
	UT_ASSERT_EQ(sync_writes, 3);
}

static void
stage_later_maintenance(void)
{
	UT_ASSERT_EQ(request_maintenance(24580), CLUSTER_XID_STRIPE_MAINTENANCE_PENDING);
}

UT_TEST(maintenance_coalesces_max_and_old_io_cannot_clear_new_request)
{
	uint64 seq;

	prepare_maintenance();
	UT_ASSERT_EQ(request_maintenance(8196), CLUSTER_XID_STRIPE_MAINTENANCE_PENDING);
	seq = boot.maintenance_seq;
	UT_ASSERT_EQ(request_maintenance(4100), CLUSTER_XID_STRIPE_MAINTENANCE_PENDING);
	UT_ASSERT_EQ(boot.maintenance_seq, seq);
	UT_ASSERT_EQ(request_maintenance(16388), CLUSTER_XID_STRIPE_MAINTENANCE_PENDING);
	UT_ASSERT(boot.maintenance_seq > seq);
	slot_write_hook = stage_later_maintenance;
	cluster_xid_stripe_herding_tick(disks, 3);
	UT_ASSERT_EQ(U64FromFullTransactionId(cluster_xid_stripe_herding_floor()), 16388);
	UT_ASSERT(boot.maintenance_pending);
	UT_ASSERT_EQ(boot.maintenance_requested_full, 24580);
	writes = 0;
	cluster_xid_stripe_herding_tick(disks, 3);
	UT_ASSERT_EQ(U64FromFullTransactionId(cluster_xid_stripe_herding_floor()), 24580);
	UT_ASSERT(!boot.maintenance_pending);
}

UT_TEST(maintenance_extends_only_original_lease_and_respects_stall)
{
	ClusterXidStripeSlotRecord out;
	uint64 old_limit, target;

	prepare_maintenance();
	UT_ASSERT_EQ(cohort_reads, 0); /* no observer on ordinary polls */
	old_limit = pg_atomic_read_u64(&lease.limit_full);
	target = old_limit + 16;
	UT_ASSERT_EQ(request_maintenance(target), CLUSTER_XID_STRIPE_MAINTENANCE_PENDING);
	stall_herding = true;
	cluster_xid_stripe_herding_tick(disks, 3);
	UT_ASSERT(boot.maintenance_pending);
	UT_ASSERT_EQ(writes, 0);
	stall_herding = false;
	allowed_writes = 6;
	cluster_xid_stripe_herding_tick(disks, 3);
	UT_ASSERT(!boot.maintenance_pending);
	UT_ASSERT_EQ(writes, 6); /* original legal lease majority, then JUMP majority */
	UT_ASSERT_EQ(sync_writes, 3);
	UT_ASSERT_EQ(U64FromFullTransactionId(cluster_xid_stripe_herding_floor()), target);
	UT_ASSERT(pg_atomic_read_u64(&lease.limit_full) > target);
	UT_ASSERT(cluster_xid_stripe_sample_fence_current(&sample_fence));
	UT_ASSERT_EQ(stripe_read_slot_all(disks, 3, 4, &out, STRIPE_READ_UPDATE), STRIPE_READ_VALID);
	UT_ASSERT_EQ(out.lease_floor_full, sample_fence.lease_floor_full);
	UT_ASSERT_EQ(out.history_generation, 0);
	UT_ASSERT_EQ(out.lease_incarnation, incarnation);
}


UT_TEST(maintenance_staging_and_durable_reply_recheck_identity_after_unlock)
{
	for (unsigned durable = 0; durable < 2; durable++) {
		prepare_maintenance();
		exclusive_release_hook = change_maintenance_identity;
		UT_ASSERT_EQ(request_maintenance(durable ? 2052 : 8196),
					 CLUSTER_XID_STRIPE_MAINTENANCE_IDENTITY_CHANGED);
		UT_ASSERT_EQ(writes, 0);
		cluster_xid_stripe_herding_tick(disks, 3);
		UT_ASSERT(!boot.maintenance_pending);
		UT_ASSERT_EQ(writes, 0);
	}
}

static void
lose_maintenance_cohort(void)
{
	cohort_status = CLUSTER_HEAP_FREEZE_COHORT_INCOMPLETE;
}

UT_TEST(maintenance_durable_io_still_requires_current_proof_before_arm)
{
	prepare_maintenance();
	UT_ASSERT_EQ(request_maintenance(8196), CLUSTER_XID_STRIPE_MAINTENANCE_PENDING);
	slot_write_hook = lose_maintenance_cohort;
	cluster_xid_stripe_herding_tick(disks, 3);
	UT_ASSERT_EQ(sync_writes, 3);
	UT_ASSERT(boot.maintenance_pending);
	UT_ASSERT(U64FromFullTransactionId(cluster_xid_stripe_herding_floor()) < 8196);
	cohort_status = CLUSTER_HEAP_FREEZE_COHORT_READY;
	writes = sync_writes = 0;
	cluster_xid_stripe_herding_tick(disks, 3);
	UT_ASSERT_EQ(sync_writes, 3);
	UT_ASSERT(!boot.maintenance_pending);
	UT_ASSERT_EQ(U64FromFullTransactionId(cluster_xid_stripe_herding_floor()), 8196);
}

/* Compose the original durable lease/future producer and native F sampler.
 * PGPROC membership, atomics and lock scheduling remain controlled leaves. */
static void
prepare_active_sample(void)
{
	prepare_future_lease();
	MemSet(&variables, 0, sizeof(variables));
	MemSet(&procs, 0, sizeof(procs));
	MemSet(active_array_storage, 0, sizeof(active_array_storage));
	MemSet(active_xids, 0, sizeof(active_xids));
	MemSet(active_procs, 0, sizeof(active_procs));
	variables.nextXid = FullTransactionIdFromU64(local_next);
	procs.xids = active_xids;
	procs.allProcs = active_procs;
	active_array.maxProcs = lengthof(active_xids);
	for (unsigned i = 0; i < lengthof(active_procs); i++)
		pg_atomic_init_u64(&active_procs[i].cluster_top_full_xid_atomic, 0);
	active_depth = active_lock_errors = active_array_reads = 0;
	xid_lock_held = false;
	active_sampling = true;
}

UT_TEST(active_sample_composes_real_floors_and_original_pgproc_identity)
{
	FullTransactionId reference, future, minimum;
	ClusterXidStripeSampleFence fence;
	VariableCacheData before;
	unsigned writes_before;

	prepare_active_sample();
	boot.floor_full = 2055;
	pg_atomic_write_u64(&boot.herding_floor_full, 2069);
	active_array.numProcs = 3;
	active_array.pgprocnos[0] = 6;
	active_array.pgprocnos[1] = 1;
	active_array.pgprocnos[2] = 4;
	/* Same raw value, different original epoch: never collapse the identities. */
	active_xids[0] = 2050;
	pg_atomic_write_u64(&active_procs[6].cluster_top_full_xid_atomic,
		U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(1, 2050)));
	active_xids[1] = 2051;
	pg_atomic_write_u64(&active_procs[1].cluster_top_full_xid_atomic, 2051);
	/* An idle compact entry's stale full value is not an active producer. */
	pg_atomic_write_u64(&active_procs[4].cluster_top_full_xid_atomic, 3);
	before = variables;
	writes_before = writes;
	UT_ASSERT(ProcArrayGetActiveFullXidSample(&reference, &future, &minimum, &fence));
	UT_ASSERT_EQ(U64FromFullTransactionId(reference), 2052);
	UT_ASSERT_EQ(U64FromFullTransactionId(future), 2084);
	UT_ASSERT_EQ(U64FromFullTransactionId(minimum), 2051);
	UT_ASSERT_EQ(fence.activation_floor_full, 2055);
	UT_ASSERT_EQ(fence.slot_floor_full, boot.my_slot_floor_full);
	UT_ASSERT_EQ(fence.slot_owner_incarnation, boot.my_owner_incarnation);
	UT_ASSERT_EQ(fence.incarnation, incarnation);
	UT_ASSERT_EQ(fence.slot, 4);
	UT_ASSERT_EQ(fence.lease_floor_full, pg_atomic_read_u64(&lease.floor_full));
	UT_ASSERT_EQ(fence.lease_limit_full, pg_atomic_read_u64(&lease.limit_full));
	UT_ASSERT(cluster_xid_stripe_sample_fence_current(&fence));
	UT_ASSERT_EQ(memcmp(&variables, &before, sizeof(before)), 0);
	UT_ASSERT_EQ(writes, writes_before);
	UT_ASSERT_EQ(active_array_reads, 1);
	UT_ASSERT_EQ(active_lock_errors, 0);
	UT_ASSERT_EQ(active_depth, 0);
	UT_ASSERT(!xid_lock_held);
}

UT_TEST(active_sample_clears_outputs_when_real_source_or_identity_is_unknown)
{
	for (unsigned scenario = 0; scenario < 10; scenario++) {
		FullTransactionId reference = FullTransactionIdFromU64(99);
		FullTransactionId future = reference, minimum = reference;
		ClusterXidStripeSampleFence fence, empty = {0};
		VariableCacheData before;
		unsigned writes_before;

		prepare_active_sample();
		MemSet(&fence, 0x5a, sizeof(fence));
		switch (scenario) {
		case 0: pg_atomic_write_u32(&lease.failed, 1); break;
		case 1: pg_atomic_write_u64(&lease.published_incarnation, incarnation + 1); break;
		case 2: in_quorum = false; break;
		case 3: boot.my_slot_claimed = false; break;
		case 4: boot.my_owner_incarnation = 0; break;
		case 5: cluster_xid_striping = false; break;
		case 6:
			pg_atomic_write_u64(&lease.limit_full, pg_atomic_read_u64(&lease.floor_full));
			break;
		case 7: variables.nextXid = FullTransactionIdFromEpochAndXid(1, 1); break;
		case 8: variables.nextXid = FullTransactionIdFromU64(UINT64_MAX); break;
		case 9:
			active_array.numProcs = 1;
			active_array.pgprocnos[0] = 6;
			active_xids[0] = 2051; /* visible raw identity lacks original FullXid */
			break;
		}
		before = variables;
		writes_before = writes;
		UT_ASSERT(!ProcArrayGetActiveFullXidSample(&reference, &future, &minimum, &fence));
		UT_ASSERT(!FullTransactionIdIsValid(reference));
		UT_ASSERT(!FullTransactionIdIsValid(future));
		UT_ASSERT(!FullTransactionIdIsValid(minimum));
		UT_ASSERT_EQ(memcmp(&fence, &empty, sizeof(empty)), 0);
		UT_ASSERT_EQ(memcmp(&variables, &before, sizeof(before)), 0);
		UT_ASSERT_EQ(writes, writes_before);
		UT_ASSERT_EQ(active_array_reads, scenario == 9 ? 1 : 0);
		UT_ASSERT_EQ(active_lock_errors, 0);
		UT_ASSERT_EQ(active_depth, 0);
		UT_ASSERT(!xid_lock_held);
	}
}

static void
publish_active_after_future(void)
{
	variables.nextXid = FullTransactionIdFromU64(2053);
	active_array.numProcs = 1;
	active_array.pgprocnos[0] = 6;
	pg_atomic_write_u64(&active_procs[6].cluster_top_full_xid_atomic, 2052);
	active_xids[0] = 2052;
}

UT_TEST(active_sample_keeps_original_cut_through_publication_and_durable_renewal)
{
	FullTransactionId reference, future, minimum;
	ClusterXidStripeSampleFence fence, saved;

	prepare_active_sample();
	after_future_hook = publish_active_after_future;
	UT_ASSERT(ProcArrayGetActiveFullXidSample(&reference, &future, &minimum, &fence));
	UT_ASSERT(after_future_hook == NULL);
	UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), 2053);
	UT_ASSERT_EQ(U64FromFullTransactionId(reference), 2052);
	UT_ASSERT_EQ(U64FromFullTransactionId(future), 2052);
	UT_ASSERT_EQ(U64FromFullTransactionId(minimum), 2052);
	saved = fence;
	UT_ASSERT(cluster_xid_stripe_sample_fence_current(&saved));
	/* The real publisher extends the promise without replacing the old cut.
	 * Its storage leaf gives each publication the original three-write budget. */
	active_sampling = false;
	writes = 0;
	stripe_lease_tick(disks, 3, saved.lease_limit_full);
	active_sampling = true;
	UT_ASSERT(pg_atomic_read_u64(&lease.limit_full) > saved.lease_limit_full);
	UT_ASSERT(cluster_xid_stripe_sample_fence_current(&saved));
	UT_ASSERT_EQ(memcmp(&fence, &saved, sizeof(saved)), 0);
	UT_ASSERT_EQ(U64FromFullTransactionId(minimum), 2052);
	UT_ASSERT_EQ(active_array_reads, 1);
	UT_ASSERT_EQ(active_lock_errors, 0);
	UT_ASSERT_EQ(active_depth, 0);
	UT_ASSERT(!xid_lock_held);
}

static unsigned active_revoke_kind;
static void
revoke_source_after_future(void)
{
	switch (active_revoke_kind) {
	case 0: pg_atomic_write_u32(&lease.failed, 1); break;
	case 1: incarnation++; break;
	case 2: boot.generation++; break;
	case 3: boot.my_slot_claimed = false; break;
	}
}

UT_TEST(active_sample_does_not_turn_old_cut_into_current_authority)
{
	for (active_revoke_kind = 0; active_revoke_kind < 4; active_revoke_kind++) {
		FullTransactionId reference, future, minimum;
		ClusterXidStripeSampleFence fence;
		unsigned writes_before;

		prepare_active_sample();
		after_future_hook = revoke_source_after_future;
		writes_before = writes;
		/* A local sample can finish after its lease/source was revoked. */
		UT_ASSERT(ProcArrayGetActiveFullXidSample(&reference, &future, &minimum, &fence));
		UT_ASSERT(after_future_hook == NULL);
		UT_ASSERT_EQ(U64FromFullTransactionId(reference), 2052);
		UT_ASSERT_EQ(U64FromFullTransactionId(future), 2052);
		UT_ASSERT_EQ(U64FromFullTransactionId(minimum), 2052);
		/* The existing consumer recheck must still reject that exact fence. */
		UT_ASSERT(!cluster_xid_stripe_sample_fence_current(&fence));
		UT_ASSERT_EQ(writes, writes_before);
		UT_ASSERT_EQ(active_array_reads, 1);
		UT_ASSERT_EQ(active_lock_errors, 0);
		UT_ASSERT_EQ(active_depth, 0);
		UT_ASSERT(!xid_lock_held);
	}
}

/* This constructs a candidate from the original sampler, not a new F or
 * retention certificate. Only these source-current composition groups use it. */
static void
original_policy_from_real_stripe(ClusterHeapFreezePolicySampleV1 *sample)
{
	FullTransactionId future;

	prepare_future_lease();
	outside_qvotec = true;
	UT_ASSERT(cluster_xid_stripe_sample_future(FullTransactionIdFromU64(local_next),
		&future, &sample_fence));
	memset(sample, 0, sizeof(*sample));
	sample->legal_next = sample->freeze_limit = future;
	sample->allocation_fence = sample_fence;
	/* The original source-current contract does not require XidGenLock. */
	xid_lock_held = false;
}

UT_TEST(original_policy_current_keeps_real_lease_across_original_renewal)
{
	ClusterHeapFreezePolicySampleV1 sample, before;
	unsigned writes_before;

	original_policy_from_real_stripe(&sample); before = sample;
	writes_before = writes;
	UT_ASSERT(actual_saved_policy_current(&sample));
	UT_ASSERT_EQ(writes, writes_before);
	outside_qvotec = false; writes = 0;
	stripe_lease_tick(disks, 3, sample.allocation_fence.lease_limit_full);
	UT_ASSERT(pg_atomic_read_u64(&lease.limit_full) > sample.allocation_fence.lease_limit_full);
	writes_before = writes;
	UT_ASSERT(actual_saved_policy_current(&sample));
	UT_ASSERT_EQ(writes, writes_before);
	UT_ASSERT(memcmp(&sample, &before, sizeof(sample)) == 0);
	UT_ASSERT(!xid_lock_held && !exclusive_lock);
}

UT_TEST(original_policy_current_refuses_revoked_real_source_without_mutation)
{
	for (int fault = 0; fault < 11; fault++) {
		ClusterHeapFreezePolicySampleV1 sample, before;
		unsigned writes_before;

		original_policy_from_real_stripe(&sample); before = sample;
		switch (fault) {
		case 0: incarnation++; break;
		case 1: in_quorum = false; break;
		case 2: pg_atomic_write_u32(&lease.failed, 1); break;
		case 3: pg_atomic_write_u64(&lease.limit_full, 0); break;
		case 4: pg_atomic_write_u64(&lease.floor_full, sample.allocation_fence.lease_floor_full + 16); break;
		case 5: pg_atomic_write_u64(&lease.published_incarnation, incarnation + 1); break;
		case 6: boot.generation++; break;
		case 7: boot.my_slot_claimed = false; break;
		case 8: boot.my_owner_incarnation++; break;
		case 9: boot.floor_full++; break;
		case 10: pg_atomic_write_u64(&lease.limit_full, sample.allocation_fence.lease_limit_full - 1); break;
		}
		writes_before = writes;
		UT_ASSERT(!actual_saved_policy_current(&sample));
		UT_ASSERT_EQ(writes, writes_before);
		UT_ASSERT(memcmp(&sample, &before, sizeof(sample)) == 0);
		UT_ASSERT(!xid_lock_held && !exclusive_lock);
	}
}

UT_TEST(original_policy_current_cannot_replace_captured_fence_with_valid_numbers)
{
	ClusterHeapFreezePolicySampleV1 sample, before;
	unsigned writes_before;

	original_policy_from_real_stripe(&sample);
	memset(&sample.allocation_fence, 0, sizeof(sample.allocation_fence));
	before = sample; writes_before = writes;
	UT_ASSERT(!actual_saved_policy_current(&sample));
	UT_ASSERT_EQ(writes, writes_before);
	UT_ASSERT(memcmp(&sample, &before, sizeof(sample)) == 0);
	UT_ASSERT(!xid_lock_held && !exclusive_lock);
}

/* Run the original full policy sampler against the original durable lease
 * fixture. This does not qualify the independent published watermark. */
static void
original_policy_sampling_fixture(void)
{
	prepare_future_lease();
	outside_qvotec = true;
	variables.nextXid = FullTransactionIdFromU64(local_next);
	xid_lock_held = false;
	active_sampling = true;
	active_depth = active_lock_errors = 0;
	policy_watermark_available = false;
	policy_watermark_min = 0;
	policy_watermark_revoke = 0;
	policy_watermark_reads = 0;
}

UT_TEST(original_policy_sampler_uses_real_legal_future_and_native_age)
{
	ClusterHeapFreezePolicySampleV1 sample;
	unsigned writes_before;

	original_policy_sampling_fixture();
	/* Floor 4096 is normalized to origin slot 4 (4100), above raw nextXid. */
	pg_atomic_write_u64(&boot.herding_floor_full, 4096);
	writes_before = writes;
	UT_ASSERT(actual_saved_policy_sample(4050, 1000, 100, 1000, &sample));
	UT_ASSERT_EQ(U64FromFullTransactionId(sample.legal_next), 4100);
	UT_ASSERT_EQ(U64FromFullTransactionId(sample.freeze_limit), 4000);
	UT_ASSERT(sample.aggressive);
	UT_ASSERT(actual_saved_policy_current(&sample));
	UT_ASSERT_EQ(local_next, 2052);
	UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), 2052);
	UT_ASSERT_EQ(policy_watermark_reads, 1);
	UT_ASSERT_EQ(writes, writes_before);
	UT_ASSERT(!xid_lock_held && !exclusive_lock);
	UT_ASSERT_EQ(active_depth + active_lock_errors, 0);
}

UT_TEST(original_policy_sampler_treats_watermark_as_conservative_ceiling)
{
	const uint64 minima[] = { 1800, 4000, 2, 0 };

	for (unsigned i = 0; i < lengthof(minima); i++) {
		ClusterHeapFreezePolicySampleV1 sample, before;
		unsigned writes_before;

		original_policy_sampling_fixture();
		policy_watermark_available = true;
		policy_watermark_min = minima[i];
		memset(&sample, 0xA5, sizeof(sample)); before = sample;
		writes_before = writes;
		if (i < 2) {
			UT_ASSERT(actual_saved_policy_sample(2000, 1000, 100, 2000, &sample));
			UT_ASSERT_EQ(U64FromFullTransactionId(sample.legal_next), 2052);
			UT_ASSERT_EQ(U64FromFullTransactionId(sample.freeze_limit), i == 0 ? 1800 : 1952);
			UT_ASSERT(!sample.aggressive);
			UT_ASSERT(actual_saved_policy_current(&sample));
		} else {
			UT_ASSERT(!actual_saved_policy_sample(2000, 1000, 100, 2000, &sample));
			UT_ASSERT(memcmp(&sample, &before, sizeof(sample)) == 0);
		}
		UT_ASSERT_EQ(policy_watermark_reads, 1);
		UT_ASSERT_EQ(writes, writes_before);
		UT_ASSERT(!xid_lock_held && !exclusive_lock);
		UT_ASSERT_EQ(active_depth + active_lock_errors, 0);
	}
}

UT_TEST(original_policy_sampler_rejects_actual_source_revoked_by_reader)
{
	for (int fault = 1; fault <= 4; fault++) {
		ClusterHeapFreezePolicySampleV1 sample, before;
		unsigned writes_before;

		original_policy_sampling_fixture();
		policy_watermark_available = true;
		policy_watermark_min = 1800;
		policy_watermark_revoke = fault;
		memset(&sample, 0xA5, sizeof(sample)); before = sample;
		writes_before = writes;
		UT_ASSERT(!actual_saved_policy_sample(2000, 1000, 100, 2000, &sample));
		UT_ASSERT(memcmp(&sample, &before, sizeof(sample)) == 0);
		UT_ASSERT_EQ(policy_watermark_reads, 1);
		UT_ASSERT_EQ(writes, writes_before);
		UT_ASSERT(!xid_lock_held && !exclusive_lock);
		UT_ASSERT_EQ(active_depth + active_lock_errors, 0);
	}
}

UT_TEST(original_policy_sampler_refuses_before_reader_without_current_lease)
{
	for (int fault = 0; fault < 4; fault++) {
		ClusterHeapFreezePolicySampleV1 sample, before;
		unsigned writes_before;

		original_policy_sampling_fixture();
		switch (fault) {
		case 0: pg_atomic_write_u32(&lease.failed, 1); break;
		case 1: pg_atomic_write_u64(&lease.limit_full, 0); break;
		case 2: pg_atomic_write_u64(&lease.published_incarnation, incarnation + 1); break;
		case 3: boot.my_slot_claimed = false; break;
		}
		memset(&sample, 0xA5, sizeof(sample)); before = sample;
		writes_before = writes;
		UT_ASSERT(!actual_saved_policy_sample(2000, 1000, 100, 2000, &sample));
		UT_ASSERT(memcmp(&sample, &before, sizeof(sample)) == 0);
		UT_ASSERT_EQ(policy_watermark_reads, 0);
		UT_ASSERT_EQ(writes, writes_before);
		UT_ASSERT(!xid_lock_held && !exclusive_lock);
		UT_ASSERT_EQ(active_depth + active_lock_errors, 0);
	}
}

int
main(void)
{
	for (int i = 0; i < 3; i++) {
		char path[] = "/tmp/pgrac-stripe-evidence-XXXXXX";
		disks[i] = mkstemp(path);
		if (disks[i] < 0 || unlink(path) != 0)
			abort();
	}
	UT_PLAN(55);
	UT_RUN(original_policy_sampler_uses_real_legal_future_and_native_age);
	UT_RUN(original_policy_sampler_treats_watermark_as_conservative_ceiling);
	UT_RUN(original_policy_sampler_rejects_actual_source_revoked_by_reader);
	UT_RUN(original_policy_sampler_refuses_before_reader_without_current_lease);
	UT_RUN(original_policy_current_keeps_real_lease_across_original_renewal);
	UT_RUN(original_policy_current_refuses_revoked_real_source_without_mutation);
	UT_RUN(original_policy_current_cannot_replace_captured_fence_with_valid_numbers);
	UT_RUN(coherent_tracking_and_retirement_choose_latest);
	UT_RUN(equal_generation_disagreement_refuses_in_either_order);
	UT_RUN(newer_record_cannot_replace_claim_identity);
	UT_RUN(newer_record_cannot_lower_hwm_or_clear_retired);
	UT_RUN(unsupported_nonempty_copy_is_not_an_absent_disk);
	UT_RUN(absent_unreadable_and_partial_sectors_remain_distinct);
	UT_RUN(reservation_waits_for_durable_majority_and_retries_exact_range);
	UT_RUN(restart_burns_unused_interval_and_refuses_old_candidates);
	UT_RUN(repeated_generations_keep_durable_history_before_new_grants);
	UT_RUN(history_majority_precedes_activation_and_preserves_old_mapping);
	UT_RUN(history_offset_and_nonmatching_image_fail_closed);
	UT_RUN(first_lease_requires_readable_history_capacity);
	UT_RUN(partial_archive_freezes_herding_image_until_activation);
	UT_RUN(partial_current_after_archive_is_burned_after_restart);
	UT_RUN(archived_source_survives_loss_of_its_original_newest_copy);
	UT_RUN(renewal_preserves_floor_and_does_not_sync_per_xid);
	UT_RUN(unavailable_retired_and_overflow_cannot_grant);
	UT_RUN(missing_read_majority_cannot_overwrite_unknown_reservations);
	UT_RUN(cached_reservation_cannot_hide_changed_claim);
	UT_RUN(all_slot_writers_require_read_majority);
	UT_RUN(partial_retirement_requires_durable_retry);
	UT_RUN(partial_first_claim_with_proven_empty_peers_can_resume);
	UT_RUN(herding_services_reservation_and_preserves_ceiling);
	UT_RUN(wait_timeouts_retry_and_new_floor_returns_for_rederivation);
	UT_RUN(wait_cancellation_quorum_loss_and_parent_death_cleanup);
	UT_RUN(future_requires_majority_and_current_lease_incarnation);
	UT_RUN(future_uses_shared_incarnation_outside_qvotec);
	UT_RUN(future_clamps_original_floors_without_changing_allocation);
	UT_RUN(future_refuses_missing_or_changing_authority);
	UT_RUN(future_retains_lease_bounds_wrap_and_window_guards);
	UT_RUN(sample_fence_keeps_original_durable_interval_across_renewal);
	UT_RUN(sample_fence_rejects_replaced_or_revoked_authority);
	UT_RUN(sample_fence_failure_clears_outputs_and_refuses_empty_fence);
	UT_RUN(maintenance_stages_then_syncs_before_arming_original_floor);
	UT_RUN(maintenance_syncs_even_when_tracking_hwm_already_covers_request);
	UT_RUN(maintenance_partial_write_retries_without_arming);
	UT_RUN(maintenance_requires_complete_current_cohort_before_staging);
	UT_RUN(maintenance_refuses_unbounded_or_illegal_targets_without_io);
	UT_RUN(maintenance_rechecks_cohort_and_retires_changed_identity);
	UT_RUN(maintenance_identity_change_during_io_cannot_arm_on_later_tick);
	UT_RUN(maintenance_coalesces_max_and_old_io_cannot_clear_new_request);
	UT_RUN(maintenance_extends_only_original_lease_and_respects_stall);
	UT_RUN(maintenance_staging_and_durable_reply_recheck_identity_after_unlock);
	UT_RUN(maintenance_durable_io_still_requires_current_proof_before_arm);
	UT_RUN(active_sample_composes_real_floors_and_original_pgproc_identity);
	UT_RUN(active_sample_clears_outputs_when_real_source_or_identity_is_unknown);
	UT_RUN(active_sample_keeps_original_cut_through_publication_and_durable_renewal);
	UT_RUN(active_sample_does_not_turn_old_cut_into_current_authority);
	for (int i = 0; i < 3; i++)
		close(disks[i]);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
