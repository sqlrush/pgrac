/*-------------------------------------------------------------------------
 *
 * cluster_mrp.c
 *	  ADG Managed Recovery Process lifecycle skeleton.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_mrp.c
 *
 * NOTES
 *	  This is a pgrac-original file for spec-6.4 ADG apply master/MRP.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include <errno.h>
#include <signal.h>
#include <string.h>

#include "access/xlogrecovery.h"
#include "cluster/cluster_adg.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_mrp.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_voting_disk_io.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "port/pg_bitutils.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/shmem.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/ps_status.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#define MRP_IDLE_TIMEOUT_MS 100
#define MRP_APPLY_LEASE_MIN_MS 1000
#define MRP_APPLY_MASTER_NONE UINT32_MAX
#define MRP_THREAD_BITMAP_WORDS 2

static ClusterMrpSharedState *cluster_mrp_state = NULL;
static int cluster_mrp_voting_fds[CLUSTER_MAX_VOTING_DISKS];
static int cluster_mrp_voting_disk_count = 0;
static bool cluster_mrp_voting_disks_opened = false;
static uint64 cluster_mrp_held_term = 0;
static int64 cluster_mrp_held_term_expires_at_ms = 0;
static uint64 cluster_mrp_lease_generation = 0;
static uint64 cluster_mrp_owner_incarnation = 0;
static uint64 cluster_mrp_qvotec_last_processed_apply_lease_seq = 0;
static uint64 cluster_mrp_qvotec_inflight_apply_lease_seq = 0;

typedef struct ClusterMrpLeaseScan {
	int disks_total;
	int disks_ok;
	int quorum;
	bool attached;
	uint64 durable_term;
	int32 owner_node_id;
	int64 lease_expires_at_ms;
	uint64 generation;
	uint64 lease_epoch;
	uint64 owner_incarnation;
} ClusterMrpLeaseScan;

typedef struct ClusterMrpApplyMasterSnapshot {
	uint32 owner_node_id;
	uint32 valid;
	uint64 term;
	uint64 generation;
	uint64 lease_epoch;
	uint64 owner_incarnation;
	uint64 valid_until_ms;
	uint64 lost_at_ms;
} ClusterMrpApplyMasterSnapshot;

static const ClusterShmemRegion cluster_mrp_region = {
	.name = "pgrac cluster mrp",
	.size_fn = cluster_mrp_shmem_size,
	.init_fn = cluster_mrp_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "cluster_mrp",
	.reserved_flags = 0,
};

static void
cluster_mrp_set_state(ClusterMrpState state)
{
	if (cluster_mrp_state != NULL)
		pg_atomic_write_u32(&cluster_mrp_state->mrp_state, (uint32)state);
}

static int64
cluster_mrp_now_ms(void)
{
	return (int64)(GetCurrentTimestamp() / 1000);
}

static void
cluster_mrp_wakeup_recovery(void)
{
	if (IsUnderPostmaster)
		WakeupRecovery();
}

static void
cluster_mrp_apply_master_token_begin_update(void)
{
	if (cluster_mrp_state == NULL)
		return;

	for (;;) {
		uint64 seq;
		uint64 next;

		seq = pg_atomic_read_u64(&cluster_mrp_state->apply_master_token_seq);
		if ((seq & UINT64CONST(1)) != 0) {
			pg_usleep(1000);
			continue;
		}
		next = seq + 1;
		if (next == 0)
			next = 1;
		if (pg_atomic_compare_exchange_u64(&cluster_mrp_state->apply_master_token_seq, &seq, next))
			break;
	}
	pg_write_barrier();
}

static void
cluster_mrp_apply_master_token_end_update(void)
{
	uint64 seq;

	if (cluster_mrp_state == NULL)
		return;

	pg_write_barrier();
	seq = pg_atomic_read_u64(&cluster_mrp_state->apply_master_token_seq);
	if ((seq & UINT64CONST(1)) != 0) {
		uint64 next = seq + 1;

		if ((next & UINT64CONST(1)) != 0)
			next++;
		pg_atomic_write_u64(&cluster_mrp_state->apply_master_token_seq, next);
	}
}

static bool
cluster_mrp_read_apply_master_snapshot(ClusterMrpApplyMasterSnapshot *snap)
{
	if (cluster_mrp_state == NULL || snap == NULL)
		return false;

	for (int attempt = 0; attempt < 3; attempt++) {
		uint64 seq1;
		uint64 seq2;

		seq1 = pg_atomic_read_u64(&cluster_mrp_state->apply_master_token_seq);
		if ((seq1 & UINT64CONST(1)) != 0)
			return false;
		pg_read_barrier();
		snap->owner_node_id = pg_atomic_read_u32(&cluster_mrp_state->apply_master_node_id);
		snap->term = pg_atomic_read_u64(&cluster_mrp_state->apply_master_term);
		snap->generation = pg_atomic_read_u64(&cluster_mrp_state->apply_master_generation);
		snap->lease_epoch = pg_atomic_read_u64(&cluster_mrp_state->apply_master_epoch);
		snap->owner_incarnation
			= pg_atomic_read_u64(&cluster_mrp_state->apply_master_owner_incarnation);
		snap->valid_until_ms
			= pg_atomic_read_u64(&cluster_mrp_state->apply_master_term_valid_until_ms);
		snap->lost_at_ms = pg_atomic_read_u64(&cluster_mrp_state->apply_master_lost_at_ms);
		snap->valid = pg_atomic_read_u32(&cluster_mrp_state->apply_master_term_valid);
		pg_read_barrier();
		seq2 = pg_atomic_read_u64(&cluster_mrp_state->apply_master_token_seq);
		if (seq1 == seq2 && (seq2 & UINT64CONST(1)) == 0)
			return true;
	}
	return false;
}

static int
cluster_mrp_apply_lease_duration_ms(void)
{
	return Max(cluster_quorum_poll_interval_ms * 2, MRP_APPLY_LEASE_MIN_MS);
}

static int
cluster_mrp_apply_lease_renew_interval_ms(void)
{
	return Max(MRP_IDLE_TIMEOUT_MS,
			   Min(cluster_mrp_apply_lease_duration_ms() / 2, MRP_APPLY_LEASE_MIN_MS));
}

static bool
cluster_mrp_valid_real_thread_id(uint16 thread_id)
{
	return thread_id >= XLP_THREAD_ID_FIRST_REAL && thread_id <= CLUSTER_WAL_THREAD_MAX;
}

static void
cluster_mrp_mark_primary_thread_observed(uint16 thread_id)
{
	uint16 bit;
	uint16 word;
	uint64 mask;

	if (cluster_mrp_state == NULL || !cluster_mrp_valid_real_thread_id(thread_id))
		return;

	bit = thread_id - XLP_THREAD_ID_FIRST_REAL;
	word = bit / 64;
	if (word >= MRP_THREAD_BITMAP_WORDS)
		return;
	mask = UINT64CONST(1) << (bit % 64);
	pg_atomic_fetch_or_u64(&cluster_mrp_state->primary_thread_bitmap[word], mask);
}

static bool
cluster_mrp_primary_thread_observed(uint16 thread_id)
{
	uint16 bit;
	uint16 word;
	uint64 mask;

	if (cluster_mrp_state == NULL || !cluster_mrp_valid_real_thread_id(thread_id))
		return false;

	bit = thread_id - XLP_THREAD_ID_FIRST_REAL;
	word = bit / 64;
	if (word >= MRP_THREAD_BITMAP_WORDS)
		return false;
	mask = UINT64CONST(1) << (bit % 64);
	return (pg_atomic_read_u64(&cluster_mrp_state->primary_thread_bitmap[word]) & mask) != 0;
}

static int
cluster_mrp_primary_thread_count(void)
{
	uint32 count;

	if (cluster_mrp_state == NULL)
		return 0;
	count = pg_atomic_read_u32(&cluster_mrp_state->primary_thread_count);
	if (count == 0 || count > CLUSTER_WAL_THREAD_MAX)
		return 0;
	return (int)count;
}

static int
cluster_mrp_observed_primary_thread_count(void)
{
	int count = 0;

	if (cluster_mrp_state == NULL)
		return 0;

	for (int i = 0; i < MRP_THREAD_BITMAP_WORDS; i++)
		count += pg_popcount64(pg_atomic_read_u64(&cluster_mrp_state->primary_thread_bitmap[i]));
	return count;
}

void
cluster_mrp_note_primary_thread_count(uint16 primary_thread_count)
{
	uint32 old_count;

	if (cluster_mrp_state == NULL || primary_thread_count == 0
		|| primary_thread_count > CLUSTER_WAL_THREAD_MAX)
		return;

	old_count = pg_atomic_read_u32(&cluster_mrp_state->primary_thread_count);
	while (old_count < primary_thread_count) {
		if (pg_atomic_compare_exchange_u32(&cluster_mrp_state->primary_thread_count, &old_count,
										   primary_thread_count))
			break;
	}
}

static void
cluster_mrp_reset_voting_fds(void)
{
	int i;

	for (i = 0; i < CLUSTER_MAX_VOTING_DISKS; i++)
		cluster_mrp_voting_fds[i] = -1;
	cluster_mrp_voting_disk_count = 0;
}

static void
cluster_mrp_close_voting_disks(void)
{
	int i;

	for (i = 0; i < cluster_mrp_voting_disk_count; i++) {
		if (cluster_mrp_voting_fds[i] >= 0)
			cluster_voting_disk_close(cluster_mrp_voting_fds[i]);
		cluster_mrp_voting_fds[i] = -1;
	}
	cluster_mrp_voting_disk_count = 0;
}

static void
cluster_mrp_close_voting_disks_atexit(int code pg_attribute_unused(),
									  Datum arg pg_attribute_unused())
{
	cluster_mrp_close_voting_disks();
}

static void
cluster_mrp_open_voting_disks(void)
{
	const char *csv = cluster_voting_disks;
	const char *p;

	if (cluster_mrp_voting_disks_opened)
		return;

	cluster_mrp_reset_voting_fds();
	cluster_mrp_voting_disks_opened = true;

	if (csv == NULL || csv[0] == '\0')
		return;

	p = csv;
	while (*p) {
		const char *start = p;
		const char *end;
		char path[MAXPGPATH];
		size_t len;
		int fd;

		while (*p && *p != ',')
			p++;
		end = p;

		while (start < end && (*start == ' ' || *start == '\t'))
			start++;
		while (end > start && (end[-1] == ' ' || end[-1] == '\t'))
			end--;

		len = (size_t)(end - start);
		if (len == 0) {
			if (*p == ',')
				p++;
			continue;
		}
		if (len >= MAXPGPATH) {
			cluster_mrp_close_voting_disks();
			ereport(FATAL,
					(errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("cluster.voting_disks path too long (>%d bytes)", MAXPGPATH - 1)));
		}
		if (cluster_mrp_voting_disk_count >= CLUSTER_MAX_VOTING_DISKS) {
			cluster_mrp_close_voting_disks();
			ereport(FATAL, (errcode(ERRCODE_CONFIG_FILE_ERROR),
							errmsg("cluster.voting_disks declares more than %d entries",
								   CLUSTER_MAX_VOTING_DISKS),
							errhint("Reduce cluster.voting_disks to an odd-majority list "
									"(1 / 3 / 5 / 7 disks recommended).")));
		}

		memcpy(path, start, len);
		path[len] = '\0';

		fd = cluster_voting_disk_open(path, /*create_if_missing*/ false);
		if (fd < 0) {
			int saved_errno = errno;

			cluster_mrp_close_voting_disks();
			errno = saved_errno;
			ereport(FATAL, (errcode_for_file_access(),
							errmsg("cluster.voting_disks: cannot open \"%s\": %m", path),
							errhint("Voting disk files must exist and include the ADG "
									"apply-master lease region.")));
		}

		cluster_mrp_voting_fds[cluster_mrp_voting_disk_count++] = fd;

		if (*p == ',')
			p++;
	}
}

static void
cluster_mrp_clear_apply_master(void)
{
	int64 now_ms;

	cluster_mrp_held_term = 0;
	cluster_mrp_held_term_expires_at_ms = 0;

	if (cluster_mrp_state == NULL)
		return;

	now_ms = cluster_mrp_now_ms();
	cluster_mrp_apply_master_token_begin_update();
	pg_atomic_write_u32(&cluster_mrp_state->apply_master_term_valid, 0);
	pg_atomic_write_u32(&cluster_mrp_state->apply_master_node_id, MRP_APPLY_MASTER_NONE);
	pg_atomic_write_u64(&cluster_mrp_state->apply_master_term, 0);
	pg_atomic_write_u64(&cluster_mrp_state->apply_master_generation, 0);
	pg_atomic_write_u64(&cluster_mrp_state->apply_master_epoch, 0);
	pg_atomic_write_u64(&cluster_mrp_state->apply_master_owner_incarnation, 0);
	pg_atomic_write_u64(&cluster_mrp_state->apply_master_term_valid_until_ms, 0);
	pg_atomic_write_u64(&cluster_mrp_state->apply_master_lost_at_ms, (uint64)now_ms);
	cluster_mrp_apply_master_token_end_update();
	cluster_mrp_wakeup_recovery();
}

static void
cluster_mrp_publish_apply_master(uint32 owner_node_id, uint64 term, uint64 generation,
								 uint64 lease_epoch, uint64 owner_incarnation,
								 int64 lease_expires_at_ms)
{
	if (cluster_mrp_state == NULL)
		return;
	cluster_mrp_apply_master_token_begin_update();
	pg_atomic_write_u32(&cluster_mrp_state->apply_master_node_id, owner_node_id);
	pg_atomic_write_u64(&cluster_mrp_state->apply_master_term, term);
	pg_atomic_write_u64(&cluster_mrp_state->apply_master_generation, generation);
	pg_atomic_write_u64(&cluster_mrp_state->apply_master_epoch, lease_epoch);
	pg_atomic_write_u64(&cluster_mrp_state->apply_master_owner_incarnation, owner_incarnation);
	pg_atomic_write_u64(&cluster_mrp_state->apply_master_term_valid_until_ms,
						(uint64)lease_expires_at_ms);
	pg_atomic_write_u64(&cluster_mrp_state->apply_master_lost_at_ms, 0);
	pg_atomic_write_u32(&cluster_mrp_state->apply_master_term_valid, 1);
	cluster_mrp_apply_master_token_end_update();
	cluster_mrp_wakeup_recovery();
}

static bool
cluster_mrp_apply_lease_scan(ClusterMrpLeaseScan *scan)
{
	int disk;
	ClusterAdgApplyMasterLease leases[CLUSTER_MAX_VOTING_DISKS];
	bool valid[CLUSTER_MAX_VOTING_DISKS];
	ClusterAdgApplyMasterLeaseQuorum result;

	if (scan == NULL)
		return false;

	memset(scan, 0, sizeof(*scan));
	scan->owner_node_id = -1;
	memset(leases, 0, sizeof(leases));
	memset(valid, 0, sizeof(valid));

	cluster_mrp_open_voting_disks();
	scan->disks_total = cluster_mrp_voting_disk_count;
	if (scan->disks_total <= 0)
		return false;
	scan->quorum = scan->disks_total / 2 + 1;

	for (disk = 0; disk < cluster_mrp_voting_disk_count; disk++) {
		uint8 slot[CLUSTER_VOTING_SLOT_BYTES];

		if (cluster_voting_disk_read_apply_lease_global_slot(cluster_mrp_voting_fds[disk], slot)
			!= CLUSTER_VOTING_DISK_IO_OK)
			continue;

		scan->disks_ok++;
		valid[disk] = cluster_adg_apply_master_lease_unpack(slot, &leases[disk]);
		if (valid[disk] && leases[disk].owner_node_id >= CLUSTER_MAX_NODES)
			valid[disk] = false;
	}

	if (scan->disks_ok < scan->quorum)
		return false;

	if (!cluster_adg_apply_master_lease_quorum(leases, valid, cluster_mrp_voting_disk_count,
											   scan->quorum, &result))
		return false;
	if (result.attached) {
		scan->attached = true;
		scan->durable_term = result.durable_term;
		scan->owner_node_id = result.owner_node_id;
		scan->lease_expires_at_ms = result.lease_expires_at_ms;
		scan->generation = result.generation;
		scan->lease_epoch = result.lease_epoch;
		scan->owner_incarnation = result.owner_incarnation;
	}

	return true;
}

static void
cluster_mrp_apply_lease_publish_result(ClusterMrpApplyLeaseSubmitResult result)
{
	if (cluster_mrp_state == NULL)
		return;
	pg_atomic_write_u32(&cluster_mrp_state->apply_lease_result, (uint32)result);
	pg_write_barrier();
	pg_atomic_write_u64(&cluster_mrp_state->apply_lease_completion_seq,
						cluster_mrp_qvotec_inflight_apply_lease_seq);
	cluster_mrp_qvotec_last_processed_apply_lease_seq = cluster_mrp_qvotec_inflight_apply_lease_seq;
}

static ClusterMrpApplyLeaseSubmitResult
cluster_mrp_submit_apply_lease_cas(const ClusterAdgApplyMasterLease *lease)
{
	uint64 seq;
	Latch *qlatch;
	uint64 deadline_us;

	if (cluster_mrp_state == NULL || lease == NULL)
		return CLUSTER_MRP_APPLY_LEASE_SUBMIT_FAILED;
	if (!cluster_adg_apply_master_lease_valid(lease))
		return CLUSTER_MRP_APPLY_LEASE_SUBMIT_INVALID;

	seq = pg_atomic_read_u64(&cluster_mrp_state->apply_lease_request_seq) + 1;
	if (seq == 0)
		seq = 1;

	pg_atomic_write_u64(&cluster_mrp_state->pending_apply_lease_seq, (seq << 1) | 1);
	pg_write_barrier();
	memcpy(&cluster_mrp_state->pending_apply_lease, lease, sizeof(*lease));
	pg_write_barrier();
	pg_atomic_write_u64(&cluster_mrp_state->pending_apply_lease_seq, seq << 1);
	pg_write_barrier();
	pg_atomic_write_u64(&cluster_mrp_state->apply_lease_request_seq, seq);

	qlatch = cluster_mrp_state->apply_lease_qvotec_latch;
	if (qlatch != NULL)
		SetLatch(qlatch);

	deadline_us
		= (uint64)GetCurrentTimestamp() + (uint64)cluster_mrp_apply_lease_duration_ms() * 1000ULL;
	for (;;) {
		if (pg_atomic_read_u64(&cluster_mrp_state->apply_lease_completion_seq) == seq) {
			pg_read_barrier();
			return (ClusterMrpApplyLeaseSubmitResult)pg_atomic_read_u32(
				&cluster_mrp_state->apply_lease_result);
		}
		if ((uint64)GetCurrentTimestamp() >= deadline_us)
			return CLUSTER_MRP_APPLY_LEASE_SUBMIT_TIMEOUT;
		pg_usleep(2 * 1000);
	}
}

static bool
cluster_mrp_snapshot_allows_apply(const ClusterMrpApplyMasterSnapshot *snap, int64 now_ms,
								  uint64 held_term)
{
	uint64 local_incarnation = 0;

	if (snap == NULL)
		return false;

	if (AmMrpProcess())
		local_incarnation = cluster_mrp_owner_incarnation;
	return cluster_adg_apply_master_token_allows_apply(
		snap->owner_node_id, snap->valid, snap->term, snap->generation, snap->lease_epoch,
		snap->owner_incarnation, snap->valid_until_ms, cluster_node_id, cluster_epoch_get_current(),
		local_incarnation, held_term, now_ms);
}

bool
cluster_mrp_apply_master_can_apply(void)
{
	ClusterMrpApplyMasterSnapshot snap;

	if (!cluster_mrp_should_start())
		return true;
	if (!cluster_mrp_read_apply_master_snapshot(&snap))
		return false;
	return cluster_mrp_snapshot_allows_apply(&snap, cluster_mrp_now_ms(), 0);
}

bool
cluster_mrp_apply_master_term_still_valid(uint64 held_term)
{
	ClusterMrpApplyMasterSnapshot snap;

	if (!cluster_mrp_should_start())
		return true;
	if (held_term == 0)
		return false;
	if (!cluster_mrp_read_apply_master_snapshot(&snap))
		return false;
	return cluster_mrp_snapshot_allows_apply(&snap, cluster_mrp_now_ms(), held_term);
}

bool
cluster_mrp_read_service_available(void)
{
	ClusterMrpApplyMasterSnapshot snap;
	int64 now_ms;

	if (!cluster_mrp_should_start())
		return true;
	if (!cluster_mrp_read_apply_master_snapshot(&snap))
		return false;

	now_ms = cluster_mrp_now_ms();
	if (snap.valid != 0 && snap.term != 0 && snap.generation != 0 && snap.lease_epoch != 0
		&& snap.owner_incarnation != 0 && snap.valid_until_ms != 0
		&& now_ms < (int64)snap.valid_until_ms)
		return true;

	if (snap.lost_at_ms == 0 || cluster_apply_master_switch_drain_ms <= 0)
		return false;
	return now_ms >= (int64)snap.lost_at_ms
		   && now_ms - (int64)snap.lost_at_ms <= cluster_apply_master_switch_drain_ms;
}

static SCN
cluster_mrp_compute_consistent_scn(void)
{
	int expected_threads;
	int observed_threads = 0;
	ClusterAdgScnTracker tracker;
	uint16 thread_id;

	expected_threads = cluster_mrp_primary_thread_count();
	if (expected_threads <= 0 || expected_threads > CLUSTER_WAL_THREAD_MAX)
		return InvalidScn;
	if (!cluster_adg_scn_tracker_init(&tracker, CLUSTER_WAL_THREAD_MAX))
		return InvalidScn;

	for (thread_id = XLP_THREAD_ID_FIRST_REAL; thread_id <= CLUSTER_WAL_THREAD_MAX; thread_id++) {
		SCN thread_scn;
		uint64 receive_lsn;
		uint64 apply_lsn;
		uint64 barrier_lsn;
		bool observed;

		if (!cluster_mrp_primary_thread_observed(thread_id))
			continue;

		receive_lsn = pg_atomic_read_u64(&cluster_mrp_state->thread_receive_lsn[thread_id]);
		apply_lsn = pg_atomic_read_u64(&cluster_mrp_state->thread_apply_lsn[thread_id]);
		barrier_lsn = pg_atomic_read_u64(&cluster_mrp_state->thread_barrier_lsn[thread_id]);
		thread_scn = (SCN)pg_atomic_read_u64(&cluster_mrp_state->thread_barrier_scn[thread_id]);
		observed = receive_lsn != 0 || apply_lsn != 0 || barrier_lsn != 0 || SCN_VALID(thread_scn);
		if (!observed)
			continue;

		observed_threads++;
		if (!SCN_VALID(thread_scn) || barrier_lsn == 0)
			return InvalidScn;
		if (apply_lsn < barrier_lsn)
			return InvalidScn;
		if (receive_lsn != 0
			&& !cluster_adg_mark_received(&tracker, thread_id, (XLogRecPtr)receive_lsn, 0))
			return InvalidScn;
		if (!cluster_adg_apply_thread_barrier(&tracker, thread_id, (XLogRecPtr)barrier_lsn,
											  thread_scn, 0))
			return InvalidScn;
	}

	if (observed_threads < expected_threads
		|| cluster_mrp_observed_primary_thread_count() < expected_threads)
		return InvalidScn;
	return cluster_adg_scn_tracker_consistent_scn(&tracker);
}

void
cluster_mrp_apply_thread_barrier(uint16 thread_id, XLogRecPtr barrier_lsn, SCN thread_safe_scn,
								 uint16 primary_thread_count)
{
	SCN old_thread_scn;
	SCN old_consistent_scn;
	SCN new_consistent_scn;

	if (cluster_mrp_state == NULL)
		return;
	if (thread_id < XLP_THREAD_ID_FIRST_REAL || thread_id > CLUSTER_WAL_THREAD_MAX
		|| XLogRecPtrIsInvalid(barrier_lsn) || !SCN_VALID(thread_safe_scn)) {
		pg_atomic_fetch_add_u64(&cluster_mrp_state->error_count, 1);
		return;
	}
	if (!cluster_mrp_apply_master_can_apply()) {
		pg_atomic_fetch_add_u64(&cluster_mrp_state->error_count, 1);
		return;
	}
	cluster_mrp_note_primary_thread_count(primary_thread_count);
	cluster_mrp_mark_primary_thread_observed(thread_id);

	old_thread_scn = (SCN)pg_atomic_read_u64(&cluster_mrp_state->thread_barrier_scn[thread_id]);
	if (SCN_VALID(old_thread_scn) && scn_time_cmp(thread_safe_scn, old_thread_scn) < 0)
		return;

	pg_atomic_write_u64(&cluster_mrp_state->thread_barrier_scn[thread_id], (uint64)thread_safe_scn);
	pg_atomic_write_u64(&cluster_mrp_state->thread_barrier_lsn[thread_id], (uint64)barrier_lsn);
	if (pg_atomic_read_u64(&cluster_mrp_state->thread_apply_lsn[thread_id]) < (uint64)barrier_lsn) {
		pg_atomic_write_u64(&cluster_mrp_state->thread_apply_lsn[thread_id], (uint64)barrier_lsn);
		pg_atomic_write_u64(&cluster_mrp_state->thread_apply_time_us[thread_id],
							(uint64)GetCurrentTimestamp());
	}

	old_consistent_scn = cluster_mrp_standby_consistent_scn();
	new_consistent_scn = cluster_mrp_compute_consistent_scn();
	if (SCN_VALID(new_consistent_scn) && SCN_VALID(old_consistent_scn)
		&& scn_time_cmp(new_consistent_scn, old_consistent_scn) < 0)
		new_consistent_scn = old_consistent_scn;
	if (!SCN_VALID(new_consistent_scn))
		new_consistent_scn = old_consistent_scn;

	cluster_mrp_publish_watermarks(barrier_lsn, barrier_lsn, (uint64)new_consistent_scn);
}

static bool
cluster_mrp_apply_master_acquire_or_renew(void)
{
	ClusterMrpLeaseScan scan;
	uint64 term;
	uint64 generation;
	int64 now_ms;
	int64 expires_at_ms;
	bool can_take_lease;

	if (!SCN_NODE_ID_VALID(cluster_node_id)) {
		cluster_mrp_clear_apply_master();
		return false;
	}

	now_ms = cluster_mrp_now_ms();
	expires_at_ms = now_ms + cluster_mrp_apply_lease_duration_ms();

	if (!cluster_apply_master_election) {
		cluster_mrp_held_term = 1;
		cluster_mrp_held_term_expires_at_ms = expires_at_ms;
		cluster_mrp_publish_apply_master((uint32)cluster_node_id, 1, 1, cluster_epoch_get_current(),
										 cluster_mrp_owner_incarnation, expires_at_ms);
		return true;
	}

	if (!cluster_mrp_apply_lease_scan(&scan)) {
		cluster_mrp_clear_apply_master();
		return false;
	}

	can_take_lease = !scan.attached || scan.owner_node_id == cluster_node_id
					 || now_ms >= scan.lease_expires_at_ms;
	if (!can_take_lease) {
		cluster_mrp_publish_apply_master((uint32)scan.owner_node_id, scan.durable_term,
										 scan.generation, scan.lease_epoch, scan.owner_incarnation,
										 scan.lease_expires_at_ms);
		return false;
	}

	if (scan.attached && scan.owner_node_id == cluster_node_id
		&& cluster_mrp_held_term == scan.durable_term && now_ms < scan.lease_expires_at_ms)
		term = scan.durable_term;
	else
		term = cluster_adg_apply_master_next_term(scan.durable_term);

	if (term == 0) {
		cluster_mrp_clear_apply_master();
		return false;
	}

	generation = Max(scan.generation, cluster_mrp_lease_generation) + 1;
	if (generation == 0) {
		cluster_mrp_clear_apply_master();
		return false;
	}

	{
		ClusterAdgApplyMasterLease desired;
		ClusterMrpApplyLeaseSubmitResult result;

		cluster_adg_apply_master_lease_init_full(&desired, term, cluster_node_id, expires_at_ms,
												 generation, cluster_epoch_get_current(),
												 cluster_mrp_owner_incarnation);
		result = cluster_mrp_submit_apply_lease_cas(&desired);
		if (result != CLUSTER_MRP_APPLY_LEASE_SUBMIT_ACK) {
			cluster_mrp_held_term = 0;
			cluster_mrp_held_term_expires_at_ms = 0;
			if (result == CLUSTER_MRP_APPLY_LEASE_SUBMIT_TIMEOUT)
				cluster_mrp_clear_apply_master();
			return false;
		}
	}

	cluster_mrp_held_term = term;
	cluster_mrp_held_term_expires_at_ms = expires_at_ms;
	cluster_mrp_lease_generation = generation;

	return true;
}

bool
cluster_mrp_config_allows_start(int dg_role, bool enable_adg)
{
	return enable_adg && dg_role == CLUSTER_DG_ROLE_STANDBY;
}

bool
cluster_mrp_should_start(void)
{
	return cluster_enabled && cluster_mrp_config_allows_start(cluster_dg_role, cluster_enable_adg);
}

Size
cluster_mrp_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterMrpSharedState));
}

void
cluster_mrp_shmem_init(void)
{
	bool found;

	cluster_mrp_state = (ClusterMrpSharedState *)ShmemInitStruct("pgrac cluster mrp",
																 cluster_mrp_shmem_size(), &found);
	if (!found) {
		memset(cluster_mrp_state, 0, sizeof(*cluster_mrp_state));
		pg_atomic_init_u32(&cluster_mrp_state->mrp_state, cluster_mrp_should_start()
															  ? CLUSTER_MRP_NOT_STARTED
															  : CLUSTER_MRP_DISABLED);
		pg_atomic_init_u32(&cluster_mrp_state->pid, 0);
		pg_atomic_init_u32(&cluster_mrp_state->apply_master_node_id, UINT32_MAX);
		pg_atomic_init_u32(&cluster_mrp_state->apply_master_term_valid, 0);
		pg_atomic_init_u64(&cluster_mrp_state->apply_master_token_seq, 0);
		pg_atomic_init_u64(&cluster_mrp_state->apply_master_term, 0);
		pg_atomic_init_u64(&cluster_mrp_state->apply_master_generation, 0);
		pg_atomic_init_u64(&cluster_mrp_state->apply_master_epoch, 0);
		pg_atomic_init_u64(&cluster_mrp_state->apply_master_owner_incarnation, 0);
		pg_atomic_init_u64(&cluster_mrp_state->apply_master_term_valid_until_ms, 0);
		pg_atomic_init_u64(&cluster_mrp_state->apply_master_lost_at_ms, 0);
		pg_atomic_init_u64(&cluster_mrp_state->receive_lsn, 0);
		pg_atomic_init_u64(&cluster_mrp_state->apply_lsn, 0);
		pg_atomic_init_u64(&cluster_mrp_state->standby_consistent_scn, 0);
		pg_atomic_init_u64(&cluster_mrp_state->started_count, 0);
		pg_atomic_init_u64(&cluster_mrp_state->idle_count, 0);
		pg_atomic_init_u64(&cluster_mrp_state->apply_batch_count, 0);
		pg_atomic_init_u64(&cluster_mrp_state->error_count, 0);
		pg_atomic_init_u64(&cluster_mrp_state->ready_at_us, 0);
		pg_atomic_init_u64(&cluster_mrp_state->stopped_at_us, 0);
		cluster_mrp_state->apply_lease_qvotec_latch = NULL;
		pg_atomic_init_u64(&cluster_mrp_state->apply_lease_request_seq, 0);
		pg_atomic_init_u64(&cluster_mrp_state->pending_apply_lease_seq, 0);
		pg_atomic_init_u64(&cluster_mrp_state->apply_lease_completion_seq, 0);
		pg_atomic_init_u32(&cluster_mrp_state->apply_lease_result,
						   CLUSTER_MRP_APPLY_LEASE_SUBMIT_FAILED);
		pg_atomic_init_u64(&cluster_mrp_state->apply_lease_write_failed, 0);
		memset(&cluster_mrp_state->pending_apply_lease, 0,
			   sizeof(cluster_mrp_state->pending_apply_lease));
		pg_atomic_init_u32(&cluster_mrp_state->primary_thread_count, 0);
		for (int i = 0; i < MRP_THREAD_BITMAP_WORDS; i++)
			pg_atomic_init_u64(&cluster_mrp_state->primary_thread_bitmap[i], 0);
		for (int i = 0; i <= CLUSTER_WAL_THREAD_MAX; i++) {
			pg_atomic_init_u64(&cluster_mrp_state->thread_receive_lsn[i], 0);
			pg_atomic_init_u64(&cluster_mrp_state->thread_start_lsn[i], 0);
			pg_atomic_init_u64(&cluster_mrp_state->thread_receive_time_us[i], 0);
			pg_atomic_init_u64(&cluster_mrp_state->thread_apply_lsn[i], 0);
			pg_atomic_init_u64(&cluster_mrp_state->thread_apply_time_us[i], 0);
			pg_atomic_init_u64(&cluster_mrp_state->thread_barrier_scn[i], 0);
			pg_atomic_init_u64(&cluster_mrp_state->thread_barrier_lsn[i], 0);
		}
	}
}

static void
cluster_mrp_clear_qvotec_latch(int code pg_attribute_unused(), Datum arg pg_attribute_unused())
{
	if (cluster_mrp_state != NULL)
		cluster_mrp_state->apply_lease_qvotec_latch = NULL;
}

void
cluster_mrp_publish_qvotec_latch(struct Latch *latch)
{
	if (cluster_mrp_state == NULL)
		return;
	cluster_mrp_state->apply_lease_qvotec_latch = latch;
	on_shmem_exit(cluster_mrp_clear_qvotec_latch, (Datum)0);
}

bool
cluster_mrp_qvotec_poll_apply_lease_request(ClusterAdgApplyMasterLease *out)
{
	uint64 req;
	uint64 pending_seq;
	uint64 pending_seq2;
	uint64 req2;

	if (cluster_mrp_state == NULL || out == NULL)
		return false;

	req = pg_atomic_read_u64(&cluster_mrp_state->apply_lease_request_seq);
	if (req == cluster_mrp_qvotec_last_processed_apply_lease_seq)
		return false;

	pending_seq = pg_atomic_read_u64(&cluster_mrp_state->pending_apply_lease_seq);
	if ((pending_seq & UINT64CONST(1)) != 0 || pending_seq != (req << 1))
		return false;
	pg_read_barrier();
	memcpy(out, &cluster_mrp_state->pending_apply_lease, sizeof(*out));
	pg_read_barrier();
	pending_seq2 = pg_atomic_read_u64(&cluster_mrp_state->pending_apply_lease_seq);
	req2 = pg_atomic_read_u64(&cluster_mrp_state->apply_lease_request_seq);
	if (pending_seq2 != pending_seq || req2 != req)
		return false;
	cluster_mrp_qvotec_inflight_apply_lease_seq = req;
	return true;
}

void
cluster_mrp_qvotec_complete_apply_lease_request(ClusterMrpApplyLeaseSubmitResult result,
												const ClusterAdgApplyMasterLeaseQuorum *winner)
{
	if (cluster_mrp_state == NULL)
		return;

	if (result != CLUSTER_MRP_APPLY_LEASE_SUBMIT_ACK)
		pg_atomic_fetch_add_u64(&cluster_mrp_state->apply_lease_write_failed, 1);

	if (winner != NULL && winner->attached)
		cluster_mrp_publish_apply_master((uint32)winner->owner_node_id, winner->durable_term,
										 winner->generation, winner->lease_epoch,
										 winner->owner_incarnation, winner->lease_expires_at_ms);
	else if (result == CLUSTER_MRP_APPLY_LEASE_SUBMIT_NO_QUORUM
			 || result == CLUSTER_MRP_APPLY_LEASE_SUBMIT_FAILED)
		cluster_mrp_clear_apply_master();

	cluster_mrp_apply_lease_publish_result(result);
}

void
cluster_mrp_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_mrp_region);
}

ClusterMrpSharedState *
cluster_mrp_shared_state(void)
{
	return cluster_mrp_state;
}

ClusterMrpState
cluster_mrp_get_state(void)
{
	if (cluster_mrp_state == NULL)
		return CLUSTER_MRP_NOT_STARTED;
	return (ClusterMrpState)pg_atomic_read_u32(&cluster_mrp_state->mrp_state);
}

const char *
cluster_mrp_state_to_string(ClusterMrpState state)
{
	switch (state) {
	case CLUSTER_MRP_NOT_STARTED:
		return "not_started";
	case CLUSTER_MRP_STARTING:
		return "starting";
	case CLUSTER_MRP_READY:
		return "ready";
	case CLUSTER_MRP_DRAINING:
		return "draining";
	case CLUSTER_MRP_STOPPED:
		return "stopped";
	case CLUSTER_MRP_DISABLED:
		return "disabled";
	default:
		return "unknown";
	}
}

SCN
cluster_mrp_standby_consistent_scn(void)
{
	if (cluster_mrp_state == NULL)
		return InvalidScn;
	return (SCN)pg_atomic_read_u64(&cluster_mrp_state->standby_consistent_scn);
}

int64
cluster_mrp_apply_lag_ms(void)
{
	uint64 receive_time_us = 0;
	uint64 apply_time_us = 0;
	uint16 thread_id;

	if (cluster_mrp_state == NULL)
		return 0;

	for (thread_id = 0; thread_id <= CLUSTER_WAL_THREAD_MAX; thread_id++) {
		uint64 thread_receive_time_us
			= pg_atomic_read_u64(&cluster_mrp_state->thread_receive_time_us[thread_id]);
		uint64 thread_apply_time_us
			= pg_atomic_read_u64(&cluster_mrp_state->thread_apply_time_us[thread_id]);

		receive_time_us = Max(receive_time_us, thread_receive_time_us);
		apply_time_us = Max(apply_time_us, thread_apply_time_us);
	}

	if (receive_time_us <= apply_time_us)
		return 0;
	return (int64)((receive_time_us - apply_time_us) / 1000);
}

uint64
cluster_mrp_apply_master_term(void)
{
	ClusterMrpApplyMasterSnapshot snap;

	if (!cluster_mrp_read_apply_master_snapshot(&snap))
		return 0;
	return snap.valid != 0 ? snap.term : 0;
}

uint32
cluster_mrp_apply_master_node_id(void)
{
	ClusterMrpApplyMasterSnapshot snap;

	if (!cluster_mrp_read_apply_master_snapshot(&snap))
		return UINT32_MAX;
	return snap.valid != 0 ? snap.owner_node_id : UINT32_MAX;
}

int
cluster_mrp_streaming_snapshot(uint64 bitmap[2], XLogRecPtr start_lsn[], XLogRecPtr receive_lsn[],
							   XLogRecPtr barrier_lsn[], SCN barrier_scn[])
{
	int count = 0;
	int expected_threads;

	if (bitmap != NULL) {
		bitmap[0] = 0;
		bitmap[1] = 0;
	}
	if (barrier_lsn != NULL || barrier_scn != NULL) {
		for (uint16 thread_id = 0; thread_id <= CLUSTER_WAL_THREAD_MAX; thread_id++) {
			if (barrier_lsn != NULL)
				barrier_lsn[thread_id] = InvalidXLogRecPtr;
			if (barrier_scn != NULL)
				barrier_scn[thread_id] = InvalidScn;
		}
	}
	if (cluster_mrp_state == NULL)
		return 0;
	expected_threads = cluster_mrp_primary_thread_count();
	if (expected_threads <= 0)
		return 0;

	for (uint16 thread_id = XLP_THREAD_ID_FIRST_REAL; thread_id <= CLUSTER_WAL_THREAD_MAX;
		 thread_id++) {
		uint64 start = pg_atomic_read_u64(&cluster_mrp_state->thread_start_lsn[thread_id]);
		uint64 receive = pg_atomic_read_u64(&cluster_mrp_state->thread_receive_lsn[thread_id]);
		uint16 bit;
		uint16 word;

		if (!cluster_mrp_primary_thread_observed(thread_id))
			continue;
		if (start == 0 || receive == 0 || receive < start)
			return 0;

		bit = thread_id - XLP_THREAD_ID_FIRST_REAL;
		word = bit / 64;
		if (word >= MRP_THREAD_BITMAP_WORDS)
			return 0;
		if (bitmap != NULL)
			bitmap[word] |= UINT64CONST(1) << (bit % 64);
		if (start_lsn != NULL)
			start_lsn[thread_id] = (XLogRecPtr)start;
		if (receive_lsn != NULL)
			receive_lsn[thread_id] = (XLogRecPtr)receive;
		if (barrier_lsn != NULL)
			barrier_lsn[thread_id]
				= (XLogRecPtr)pg_atomic_read_u64(&cluster_mrp_state->thread_barrier_lsn[thread_id]);
		if (barrier_scn != NULL)
			barrier_scn[thread_id]
				= (SCN)pg_atomic_read_u64(&cluster_mrp_state->thread_barrier_scn[thread_id]);
		count++;
		if (count > expected_threads)
			return 0;
	}

	return count == expected_threads ? count : 0;
}

void
cluster_mrp_mark_thread_received_span(uint16 thread_id, XLogRecPtr start_lsn,
									  XLogRecPtr receive_lsn)
{
	uint64 old_start;

	if (cluster_mrp_state == NULL)
		return;
	if (thread_id < XLP_THREAD_ID_FIRST_REAL || thread_id > CLUSTER_WAL_THREAD_MAX
		|| XLogRecPtrIsInvalid(start_lsn) || XLogRecPtrIsInvalid(receive_lsn)
		|| receive_lsn < start_lsn) {
		pg_atomic_fetch_add_u64(&cluster_mrp_state->error_count, 1);
		return;
	}

	cluster_mrp_mark_primary_thread_observed(thread_id);
	old_start = pg_atomic_read_u64(&cluster_mrp_state->thread_start_lsn[thread_id]);
	while (old_start == 0 || old_start > (uint64)start_lsn) {
		if (pg_atomic_compare_exchange_u64(&cluster_mrp_state->thread_start_lsn[thread_id],
										   &old_start, (uint64)start_lsn))
			break;
	}
	cluster_mrp_mark_thread_received(thread_id, receive_lsn);
}

void
cluster_mrp_mark_thread_received(uint16 thread_id, XLogRecPtr receive_lsn)
{
	uint64 old_lsn;

	if (cluster_mrp_state == NULL)
		return;
	if (thread_id > CLUSTER_WAL_THREAD_MAX || XLogRecPtrIsInvalid(receive_lsn)) {
		pg_atomic_fetch_add_u64(&cluster_mrp_state->error_count, 1);
		return;
	}
	cluster_mrp_mark_primary_thread_observed(thread_id);

	old_lsn = pg_atomic_read_u64(&cluster_mrp_state->thread_receive_lsn[thread_id]);
	if (old_lsn > (uint64)receive_lsn)
		return;

	pg_atomic_write_u64(&cluster_mrp_state->thread_receive_lsn[thread_id], (uint64)receive_lsn);
	pg_atomic_write_u64(&cluster_mrp_state->thread_receive_time_us[thread_id],
						(uint64)GetCurrentTimestamp());
	old_lsn = pg_atomic_read_u64(&cluster_mrp_state->receive_lsn);
	if (old_lsn <= (uint64)receive_lsn)
		pg_atomic_write_u64(&cluster_mrp_state->receive_lsn, (uint64)receive_lsn);
}

void
cluster_mrp_mark_thread_applied(uint16 thread_id, XLogRecPtr apply_lsn)
{
	uint64 old_lsn;

	if (cluster_mrp_state == NULL)
		return;
	if (thread_id > CLUSTER_WAL_THREAD_MAX || XLogRecPtrIsInvalid(apply_lsn)) {
		pg_atomic_fetch_add_u64(&cluster_mrp_state->error_count, 1);
		return;
	}
	cluster_mrp_mark_primary_thread_observed(thread_id);

	old_lsn = pg_atomic_read_u64(&cluster_mrp_state->thread_apply_lsn[thread_id]);
	if (old_lsn > (uint64)apply_lsn)
		return;

	pg_atomic_write_u64(&cluster_mrp_state->thread_apply_lsn[thread_id], (uint64)apply_lsn);
	pg_atomic_write_u64(&cluster_mrp_state->thread_apply_time_us[thread_id],
						(uint64)GetCurrentTimestamp());
	old_lsn = pg_atomic_read_u64(&cluster_mrp_state->apply_lsn);
	if (old_lsn <= (uint64)apply_lsn)
		pg_atomic_write_u64(&cluster_mrp_state->apply_lsn, (uint64)apply_lsn);
}

void
cluster_mrp_mark_child_exit(void)
{
	ClusterMrpState state;

	if (cluster_mrp_state == NULL)
		return;

	state = cluster_mrp_get_state();
	pg_atomic_write_u32(&cluster_mrp_state->pid, 0);
	if (state != CLUSTER_MRP_DISABLED)
		cluster_mrp_set_state(CLUSTER_MRP_STOPPED);
}

void
cluster_mrp_publish_watermarks(XLogRecPtr receive_lsn, XLogRecPtr apply_lsn,
							   uint64 standby_consistent_scn)
{
	SCN candidate_scn = (SCN)standby_consistent_scn;

	if (cluster_mrp_state == NULL)
		return;
	if (SCN_VALID(candidate_scn) && !cluster_mrp_apply_master_can_apply()) {
		pg_atomic_fetch_add_u64(&cluster_mrp_state->error_count, 1);
		return;
	}
	if (!XLogRecPtrIsInvalid(receive_lsn) && !XLogRecPtrIsInvalid(apply_lsn)
		&& apply_lsn > receive_lsn)
		return;
	if (!XLogRecPtrIsInvalid(receive_lsn))
		pg_atomic_write_u64(&cluster_mrp_state->receive_lsn, (uint64)receive_lsn);
	if (!XLogRecPtrIsInvalid(apply_lsn))
		pg_atomic_write_u64(&cluster_mrp_state->apply_lsn, (uint64)apply_lsn);
	if (SCN_VALID(candidate_scn)) {
		uint64 old_raw;

		for (;;) {
			SCN old_scn;

			old_raw = pg_atomic_read_u64(&cluster_mrp_state->standby_consistent_scn);
			old_scn = (SCN)old_raw;
			if (SCN_VALID(old_scn)) {
				int cmp = scn_time_cmp(candidate_scn, old_scn);

				if (cmp < 0 || (cmp == 0 && scn_total_cmp(candidate_scn, old_scn) <= 0))
					break;
			}
			if (pg_atomic_compare_exchange_u64(&cluster_mrp_state->standby_consistent_scn, &old_raw,
											   standby_consistent_scn))
				break;
		}
	}
}

void
MrpMain(void)
{
	int64 next_lease_check_ms;

	if (!IsUnderPostmaster)
		elog(FATAL, "MRP must run under postmaster");

	MyBackendType = B_MRP;
	init_ps_display(NULL);

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, SignalHandlerForShutdownRequest);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, SIG_IGN);
	pqsignal(SIGUSR2, SIG_IGN);
	pqsignal(SIGCHLD, SIG_DFL);
	sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);

	cluster_voting_disk_io_install_timeout_handler();
	cluster_voting_disk_io_set_timeout_ms(cluster_voting_disk_io_timeout_ms);

	if (cluster_mrp_state == NULL)
		ereport(FATAL,
				(errcode(ERRCODE_INTERNAL_ERROR), errmsg("cluster_mrp shmem region not attached"),
				 errhint("cluster_mrp_shmem_init() must run during "
						 "CreateSharedMemoryAndSemaphores().")));

	if (!cluster_mrp_should_start()) {
		cluster_mrp_set_state(CLUSTER_MRP_DISABLED);
		proc_exit(0);
	}

	cluster_mrp_set_state(CLUSTER_MRP_STARTING);
	cluster_mrp_owner_incarnation = (uint64)GetCurrentTimestamp();
	if (cluster_mrp_owner_incarnation == 0)
		cluster_mrp_owner_incarnation = 1;
	pg_atomic_write_u32(&cluster_mrp_state->pid, (uint32)MyProcPid);
	pg_atomic_fetch_add_u64(&cluster_mrp_state->started_count, 1);
	cluster_mrp_open_voting_disks();
	if (cluster_apply_master_election && cluster_mrp_voting_disk_count <= 0)
		ereport(FATAL, (errcode(ERRCODE_CONFIG_FILE_ERROR),
						errmsg("cluster.voting_disks is required for ADG apply-master election"),
						errhint("Set cluster.voting_disks to a shared voting-disk majority, "
								"or set cluster.apply_master_election=off for a single-node "
								"test standby.")));
	on_shmem_exit(cluster_mrp_close_voting_disks_atexit, (Datum)0);

	cluster_mrp_set_state(CLUSTER_MRP_READY);
	pg_atomic_write_u64(&cluster_mrp_state->ready_at_us, (uint64)GetCurrentTimestamp());
	next_lease_check_ms = 0;

	for (;;) {
		int64 now_ms;

		CHECK_FOR_INTERRUPTS();

		if (ConfigReloadPending) {
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
			cluster_voting_disk_io_set_timeout_ms(cluster_voting_disk_io_timeout_ms);
		}

		if (ShutdownRequestPending)
			break;

		now_ms = cluster_mrp_now_ms();
		if (now_ms >= next_lease_check_ms) {
			if (!cluster_mrp_apply_master_acquire_or_renew())
				pg_atomic_fetch_add_u64(&cluster_mrp_state->error_count, 1);
			next_lease_check_ms = now_ms + cluster_mrp_apply_lease_renew_interval_ms();
		}

		pg_atomic_fetch_add_u64(&cluster_mrp_state->idle_count, 1);
		(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						MRP_IDLE_TIMEOUT_MS, WAIT_EVENT_ADG_MRP_APPLY_WAIT);
		ResetLatch(MyLatch);
	}

	cluster_mrp_set_state(CLUSTER_MRP_DRAINING);
	cluster_mrp_clear_apply_master();
	pg_atomic_write_u32(&cluster_mrp_state->pid, 0);
	pg_atomic_write_u64(&cluster_mrp_state->stopped_at_us, (uint64)GetCurrentTimestamp());
	cluster_mrp_set_state(CLUSTER_MRP_STOPPED);

	proc_exit(0);
}

#endif /* USE_PGRAC_CLUSTER */
