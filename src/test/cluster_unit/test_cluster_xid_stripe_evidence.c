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
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_inject.h"
#include "storage/condition_variable.h"
#include "storage/lwlock.h"
#include "storage/pmsignal.h"
#include "miscadmin.h"
#include "utils/wait_event.h"
#undef printf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool cluster_enabled;
bool cluster_xid_striping;
bool cluster_shared_catalog;
int cluster_node_id;
int cluster_xid_herding_slack = 4194304;
static uint64 local_next = 2052;
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
	in_quorum = cluster_enabled = cluster_shared_catalog = cluster_xid_striping = true;
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
	UT_ASSERT(!cluster_xid_stripe_sample_future(FullTransactionIdFromU64(local_next), &future));
	UT_ASSERT(!FullTransactionIdIsValid(future));
	allowed_writes = 3;
	writes = 0;
	stripe_lease_tick(disks, 3, local_next);
	writes_before = writes;
	UT_ASSERT(cluster_xid_stripe_sample_future(FullTransactionIdFromU64(local_next), &future));
	UT_ASSERT_EQ(U64FromFullTransactionId(future), 2052);
	UT_ASSERT_EQ(writes, writes_before);
	UT_ASSERT_EQ(local_next, 2052);
	UT_ASSERT(boot.my_owner_incarnation != incarnation);
	incarnation++;
	UT_ASSERT(!cluster_xid_stripe_sample_future(FullTransactionIdFromU64(local_next), &future));
	UT_ASSERT(!FullTransactionIdIsValid(future));
	writes = 0;
	stripe_lease_tick(disks, 3, local_next);
	UT_ASSERT(cluster_xid_stripe_sample_future(FullTransactionIdFromU64(local_next), &future));
	UT_ASSERT_EQ(U64FromFullTransactionId(future), pg_atomic_read_u64(&lease.floor_full));
}

UT_TEST(future_uses_shared_incarnation_outside_qvotec)
{
	FullTransactionId future;

	prepare_future_lease();
	outside_qvotec = true;
	UT_ASSERT_EQ(cluster_qvotec_self_incarnation_value(), 0);
	UT_ASSERT_EQ(cluster_qvotec_get_self_incarnation(), incarnation);
	UT_ASSERT(cluster_xid_stripe_sample_future(FullTransactionIdFromU64(local_next), &future));
	UT_ASSERT_EQ(U64FromFullTransactionId(future), local_next);
}

UT_TEST(future_clamps_original_floors_without_changing_allocation)
{
	FullTransactionId future;
	unsigned writes_before;

	prepare_future_lease();
	writes_before = writes;
	boot.floor_full = 2055;
	UT_ASSERT(cluster_xid_stripe_sample_future(FullTransactionIdFromU64(2052), &future));
	UT_ASSERT_EQ(U64FromFullTransactionId(future), 2068);
	boot.my_slot_floor_full = 2080;
	UT_ASSERT(cluster_xid_stripe_sample_future(FullTransactionIdFromU64(2052), &future));
	UT_ASSERT_EQ(U64FromFullTransactionId(future), 2084);
	pg_atomic_write_u64(&boot.herding_floor_full, 2101);
	UT_ASSERT(cluster_xid_stripe_sample_future(FullTransactionIdFromU64(2052), &future));
	UT_ASSERT_EQ(U64FromFullTransactionId(future), 2116);
	UT_ASSERT(cluster_xid_stripe_sample_future(FullTransactionIdFromU64(2120), &future));
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
		UT_ASSERT(!cluster_xid_stripe_sample_future(FullTransactionIdFromU64(local_next), &future));
		UT_ASSERT(!FullTransactionIdIsValid(future));
	}
	prepare_future_lease();
	UT_ASSERT(!cluster_xid_stripe_sample_future(FullTransactionIdFromU64(local_next), NULL));
}

UT_TEST(future_retains_lease_bounds_wrap_and_window_guards)
{
	FullTransactionId future;

	prepare_future_lease();
	UT_ASSERT(!cluster_xid_stripe_sample_future(
		FullTransactionIdFromU64(pg_atomic_read_u64(&lease.limit_full)), &future));
	UT_ASSERT(!FullTransactionIdIsValid(future));
	UT_ASSERT(!cluster_xid_stripe_sample_future(FullTransactionIdFromU64(UINT64_MAX), &future));
	UT_ASSERT(!FullTransactionIdIsValid(future));
	for (uint64 special = 0; special < FirstNormalTransactionId; special++) {
		UT_ASSERT(!cluster_xid_stripe_sample_future(FullTransactionIdFromU64(special), &future));
		UT_ASSERT(!FullTransactionIdIsValid(future));
	}
	pg_atomic_write_u64(&boot.cluster_min_active_hwm, 1);
	cluster_xid_herding_slack = 1;
	UT_ASSERT(!cluster_xid_stripe_sample_future(FullTransactionIdFromU64(local_next), &future));
	UT_ASSERT(!FullTransactionIdIsValid(future));
	cluster_xid_herding_slack = 4194304;
	pg_atomic_write_u64(&lease.floor_full, (uint64)UINT32_MAX - 32);
	pg_atomic_write_u64(&lease.limit_full, (uint64)UINT32_MAX + 32);
	pg_atomic_write_u32(&boot.wrap_barrier_done, 0);
	UT_ASSERT(!cluster_xid_stripe_sample_future(FullTransactionIdFromU64(UINT32_MAX - 1), &future));
	UT_ASSERT(!FullTransactionIdIsValid(future));
	pg_atomic_write_u32(&boot.wrap_barrier_done, 1);
	pg_atomic_write_u64(&boot.cluster_min_active_hwm, 0);
	UT_ASSERT(cluster_xid_stripe_sample_future(FullTransactionIdFromU64(UINT32_MAX - 1), &future));
	UT_ASSERT_EQ(U64FromFullTransactionId(future), (UINT64CONST(1) << 32) + 4);
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
	UT_PLAN(30);
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
	for (int i = 0; i < 3; i++)
		close(disks[i]);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
