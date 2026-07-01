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

#include "cluster/cluster_adg.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_mrp.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_voting_disk_io.h"
#include "cluster/cluster_wal_state.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
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

static ClusterMrpSharedState *cluster_mrp_state = NULL;
static int cluster_mrp_voting_fds[CLUSTER_MAX_VOTING_DISKS];
static int cluster_mrp_voting_disk_count = 0;
static bool cluster_mrp_voting_disks_opened = false;
static uint64 cluster_mrp_held_term = 0;
static int64 cluster_mrp_held_term_expires_at_ms = 0;
static uint64 cluster_mrp_lease_generation = 0;

typedef struct ClusterMrpLeaseScan {
	int disks_total;
	int disks_ok;
	int quorum;
	bool attached;
	uint64 durable_term;
	int32 owner_node_id;
	int64 lease_expires_at_ms;
	uint64 generation;
} ClusterMrpLeaseScan;

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
	cluster_mrp_held_term = 0;
	cluster_mrp_held_term_expires_at_ms = 0;

	if (cluster_mrp_state == NULL)
		return;

	pg_atomic_write_u32(&cluster_mrp_state->apply_master_node_id, MRP_APPLY_MASTER_NONE);
	pg_atomic_write_u64(&cluster_mrp_state->apply_master_term, 0);
	pg_atomic_write_u64(&cluster_mrp_state->standby_consistent_scn, 0);
}

static bool
cluster_mrp_apply_lease_scan(ClusterMrpLeaseScan *scan)
{
	int disk;

	if (scan == NULL)
		return false;

	memset(scan, 0, sizeof(*scan));
	scan->owner_node_id = -1;

	cluster_mrp_open_voting_disks();
	scan->disks_total = cluster_mrp_voting_disk_count;
	if (scan->disks_total <= 0)
		return false;
	scan->quorum = scan->disks_total / 2 + 1;

	for (disk = 0; disk < cluster_mrp_voting_disk_count; disk++) {
		bool disk_ok = false;
		uint32 node_id;

		for (node_id = 0; node_id < CLUSTER_MAX_NODES; node_id++) {
			uint8 slot[CLUSTER_VOTING_SLOT_BYTES];
			ClusterAdgApplyMasterLease lease;

			if (cluster_voting_disk_read_apply_lease_slot(cluster_mrp_voting_fds[disk], node_id,
														  slot)
				!= CLUSTER_VOTING_DISK_IO_OK)
				continue;

			disk_ok = true;
			if (!cluster_adg_apply_master_lease_unpack(slot, &lease))
				continue;

			if (!scan->attached || lease.term > scan->durable_term
				|| (lease.term == scan->durable_term && lease.generation > scan->generation)) {
				scan->attached = true;
				scan->durable_term = lease.term;
				scan->owner_node_id = lease.owner_node_id;
				scan->lease_expires_at_ms = lease.lease_expires_at_ms;
				scan->generation = lease.generation;
			}
		}

		if (disk_ok)
			scan->disks_ok++;
	}

	return scan->disks_ok >= scan->quorum;
}

static bool
cluster_mrp_apply_lease_write(uint64 term, int64 lease_expires_at_ms, uint64 generation)
{
	ClusterAdgApplyMasterLease lease;
	uint8 slot[CLUSTER_VOTING_SLOT_BYTES];
	int success = 0;
	int i;

	if (!SCN_NODE_ID_VALID(cluster_node_id) || cluster_mrp_voting_disk_count <= 0)
		return false;

	cluster_adg_apply_master_lease_init(&lease, term, cluster_node_id, lease_expires_at_ms,
										generation);
	if (!cluster_adg_apply_master_lease_pack(slot, &lease))
		return false;

	for (i = 0; i < cluster_mrp_voting_disk_count; i++) {
		if (cluster_voting_disk_write_apply_lease_slot(cluster_mrp_voting_fds[i],
													   (uint32)cluster_node_id, slot)
			== CLUSTER_VOTING_DISK_IO_OK)
			success++;
	}

	return success >= (cluster_mrp_voting_disk_count / 2 + 1);
}

static bool
cluster_mrp_shared_apply_master_term_valid(int64 now_ms)
{
	ClusterMrpLeaseScan scan;
	bool lease_attached;
	uint32 owner_node_id;
	uint64 term;

	if (cluster_mrp_state == NULL)
		return false;

	owner_node_id = pg_atomic_read_u32(&cluster_mrp_state->apply_master_node_id);
	term = pg_atomic_read_u64(&cluster_mrp_state->apply_master_term);
	if (!SCN_NODE_ID_VALID(cluster_node_id) || owner_node_id != (uint32)cluster_node_id
		|| term == 0)
		return false;

	if (!cluster_apply_master_election) {
		return term == 1;
	}

	if (!cluster_mrp_apply_lease_scan(&scan))
		return false;

	lease_attached = scan.attached && scan.owner_node_id == cluster_node_id;
	return cluster_adg_apply_master_lease_check(term, scan.durable_term, lease_attached, true,
												now_ms, scan.lease_expires_at_ms)
		   == CLUSTER_ADG_LEASE_VALID;
}

bool
cluster_mrp_apply_master_can_apply(void)
{
	if (!cluster_mrp_should_start())
		return true;
	return cluster_mrp_shared_apply_master_term_valid(cluster_mrp_now_ms());
}

static SCN
cluster_mrp_compute_consistent_scn(void)
{
	bool required[CLUSTER_WAL_THREAD_MAX + 1];
	bool have_required = false;
	bool have_any_barrier = false;
	SCN min_scn = InvalidScn;
	uint16 thread_id;

	memset(required, 0, sizeof(required));

	for (thread_id = XLP_THREAD_ID_FIRST_REAL; thread_id <= CLUSTER_WAL_THREAD_MAX; thread_id++) {
		ClusterWalStateSlot slot;

		if (cluster_wal_state_read_slot(thread_id, &slot) == CLUSTER_WAL_SLOT_OK
			&& slot.state == CLUSTER_WAL_SLOT_STATE_ACTIVE) {
			required[thread_id] = true;
			have_required = true;
		}
	}

	for (thread_id = XLP_THREAD_ID_FIRST_REAL; thread_id <= CLUSTER_WAL_THREAD_MAX; thread_id++) {
		SCN thread_scn;

		if (have_required && !required[thread_id])
			continue;

		thread_scn = (SCN)pg_atomic_read_u64(&cluster_mrp_state->thread_barrier_scn[thread_id]);
		if (!SCN_VALID(thread_scn)) {
			if (have_required)
				return InvalidScn;
			continue;
		}

		have_any_barrier = true;
		if (!SCN_VALID(min_scn) || scn_time_cmp(thread_scn, min_scn) < 0)
			min_scn = thread_scn;
	}

	return have_any_barrier ? min_scn : InvalidScn;
}

void
cluster_mrp_apply_thread_barrier(uint16 thread_id, XLogRecPtr barrier_lsn, SCN thread_safe_scn)
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
	if (!cluster_mrp_shared_apply_master_term_valid(cluster_mrp_now_ms())) {
		cluster_mrp_clear_apply_master();
		pg_atomic_fetch_add_u64(&cluster_mrp_state->error_count, 1);
		return;
	}

	old_thread_scn = (SCN)pg_atomic_read_u64(&cluster_mrp_state->thread_barrier_scn[thread_id]);
	if (SCN_VALID(old_thread_scn) && scn_time_cmp(thread_safe_scn, old_thread_scn) < 0)
		return;

	pg_atomic_write_u64(&cluster_mrp_state->thread_barrier_scn[thread_id], (uint64)thread_safe_scn);
	pg_atomic_write_u64(&cluster_mrp_state->thread_barrier_lsn[thread_id], (uint64)barrier_lsn);

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
		if (cluster_mrp_state != NULL) {
			pg_atomic_write_u32(&cluster_mrp_state->apply_master_node_id, (uint32)cluster_node_id);
			pg_atomic_write_u64(&cluster_mrp_state->apply_master_term, 1);
		}
		return true;
	}

	if (!cluster_mrp_apply_lease_scan(&scan)) {
		cluster_mrp_clear_apply_master();
		return false;
	}

	can_take_lease = !scan.attached || scan.owner_node_id == cluster_node_id
					 || now_ms >= scan.lease_expires_at_ms;
	if (!can_take_lease) {
		cluster_mrp_clear_apply_master();
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

	if (!cluster_mrp_apply_lease_write(term, expires_at_ms, generation)) {
		cluster_mrp_clear_apply_master();
		return false;
	}

	cluster_mrp_held_term = term;
	cluster_mrp_held_term_expires_at_ms = expires_at_ms;
	cluster_mrp_lease_generation = generation;

	if (cluster_mrp_state != NULL) {
		pg_atomic_write_u32(&cluster_mrp_state->apply_master_node_id, (uint32)cluster_node_id);
		pg_atomic_write_u64(&cluster_mrp_state->apply_master_term, term);
	}

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
		pg_atomic_init_u32(&cluster_mrp_state->reserved32, 0);
		pg_atomic_init_u64(&cluster_mrp_state->apply_master_term, 0);
		pg_atomic_init_u64(&cluster_mrp_state->receive_lsn, 0);
		pg_atomic_init_u64(&cluster_mrp_state->apply_lsn, 0);
		pg_atomic_init_u64(&cluster_mrp_state->standby_consistent_scn, 0);
		pg_atomic_init_u64(&cluster_mrp_state->started_count, 0);
		pg_atomic_init_u64(&cluster_mrp_state->idle_count, 0);
		pg_atomic_init_u64(&cluster_mrp_state->apply_batch_count, 0);
		pg_atomic_init_u64(&cluster_mrp_state->error_count, 0);
		pg_atomic_init_u64(&cluster_mrp_state->ready_at_us, 0);
		pg_atomic_init_u64(&cluster_mrp_state->stopped_at_us, 0);
		for (int i = 0; i <= CLUSTER_WAL_THREAD_MAX; i++) {
			pg_atomic_init_u64(&cluster_mrp_state->thread_receive_lsn[i], 0);
			pg_atomic_init_u64(&cluster_mrp_state->thread_receive_time_us[i], 0);
			pg_atomic_init_u64(&cluster_mrp_state->thread_apply_lsn[i], 0);
			pg_atomic_init_u64(&cluster_mrp_state->thread_apply_time_us[i], 0);
			pg_atomic_init_u64(&cluster_mrp_state->thread_barrier_scn[i], 0);
			pg_atomic_init_u64(&cluster_mrp_state->thread_barrier_lsn[i], 0);
		}
	}
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

uint64
cluster_mrp_apply_master_term(void)
{
	if (cluster_mrp_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_mrp_state->apply_master_term);
}

uint32
cluster_mrp_apply_master_node_id(void)
{
	if (cluster_mrp_state == NULL)
		return UINT32_MAX;
	return pg_atomic_read_u32(&cluster_mrp_state->apply_master_node_id);
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
	if (cluster_mrp_state == NULL)
		return;
	if (SCN_VALID((SCN)standby_consistent_scn)
		&& !cluster_mrp_shared_apply_master_term_valid(cluster_mrp_now_ms())) {
		cluster_mrp_clear_apply_master();
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
	pg_atomic_write_u64(&cluster_mrp_state->standby_consistent_scn, standby_consistent_scn);
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
