/*-------------------------------------------------------------------------
 *
 * cluster_wal_retention.c
 *	  Control-root-aware WAL retention and reuse fencing (RF-ROOT P6).
 *
 * This first slice is deliberately authority-free.  It validates exact
 * process-local identities and folds outputs that the control-root owner has
 * already validated.  Live root reads, WALR grants, pins, guards, and every
 * destructive caller are added by later ordered P6 slices.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog.h"
#include "miscadmin.h"

#include "cluster/cluster_guc.h"
#include "cluster/cluster_lock_acquire.h"
#include "cluster/cluster_recovery_duty.h"
#include "cluster/cluster_wal_retention.h"
#include "cluster/cluster_wal_thread.h"
#include "portability/instr_time.h"
#include "storage/lock.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#define CLUSTER_WAL_PIN_MAGIC UINT32_C(0x57414c50)
#define CLUSTER_WAL_ROOT_PUBLISH_MAGIC UINT32_C(0x57525047)
#define CLUSTER_WALR_NATIVE_TAG_MAGIC UINT32_C(0x57414c52)

#define CLUSTER_WAL_E1_STATE_COARSE_HELD UINT8_C(1)
#define CLUSTER_WAL_E1_STATE_FILES UINT8_C(2)

typedef enum ClusterWalPinState {
	CLUSTER_WAL_PIN_STATE_ACQUIRED_UNBOUND = 1,
	CLUSTER_WAL_PIN_STATE_BOUND_ONE = 2,
	CLUSTER_WAL_PIN_STATE_BOUND_SET = 3,
	CLUSTER_WAL_PIN_STATE_SEALED = 4
} ClusterWalPinState;

typedef struct ClusterWalPinLock {
	ClusterLockAcquireRequest request;
	bool held;
	bool release_uncertain;
} ClusterWalPinLock;

typedef struct ClusterWalPinThread {
	ClusterWalRetentionInterval *intervals;
	uint32 nintervals;
	ClusterRecoveryDutyKey duty;
	ClusterControlRootReadToken root_read;
	const ClusterFormationWitnessV1 *formation;
	const PgracExternalFenceNeedSetV1 *needs;
	const PgracExternalFenceAdmissionSetV1 *admissions;
	ClusterRecoverySerialGuard *serial;
	ClusterWalPinLock walr;
} ClusterWalPinThread;

struct ClusterWalRetentionPin {
	uint32 magic;
	int32 owner_pid;
	ResourceOwner owner;
	uint16 nthreads;
	uint8 state;
	bool poisoned;
	ClusterRecoverySerialGuardSet *serial_set;
	ClusterWalPinThread *threads;
};

struct ClusterWalRootPublishGuard {
	uint32 magic;
	int32 owner_pid;
	ResourceOwner owner;
	uint16 thread_id;
	bool borrowed_from_pin;
	ClusterWalPinLock walr;
};

static ClusterWalRetentionPin *active_pin;
static ClusterWalRootPublishGuard *active_root_publish_guard;
static ClusterWalReuseActionGuard *active_reuse_guard;
static ClusterWalRetentionE1Context *active_e1_context;
static ClusterWalRetentionE1Context *active_action_context;
static bool walr_resource_callback_registered;

static void walr_resource_release_callback(ResourceReleasePhase phase, bool isCommit,
										   bool isTopLevel, void *arg);

static void
wal_reuse_guard_close_handles(ClusterWalReuseActionGuard *guard)
{
#ifndef WIN32
	if (guard->fork_source_handle >= 0)
		close((int)guard->fork_source_handle);
	if (guard->source_handle >= 0)
		close((int)guard->source_handle);
	if (guard->source_dir_handle >= 0)
		close((int)guard->source_dir_handle);
#endif
	guard->fork_source_handle = (intptr_t)-1;
	guard->source_handle = (intptr_t)-1;
	guard->source_dir_handle = (intptr_t)-1;
}

static void
walr_resource_ensure_callback(void)
{
	if (!walr_resource_callback_registered) {
		RegisterResourceReleaseCallback(walr_resource_release_callback, NULL);
		walr_resource_callback_registered = true;
	}
}

static void
fold_unknown(ClusterWalRootFold *fold)
{
	memset(fold, 0, sizeof(*fold));
	fold->result = CLUSTER_WAL_FOLD_UNKNOWN;
}

static bool
control_root_read_ready(ClusterControlRootResult result)
{
	return result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		   || result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED;
}

bool
cluster_wal_thread_directory_parse(const char *basename, uint16 *out_thread_id)
{
	const char *digits;
	uint32 value = 0;

	if (out_thread_id == NULL)
		return false;
	*out_thread_id = 0;
	if (basename == NULL || strncmp(basename, "thread_", 7) != 0)
		return false;
	digits = basename + 7;
	if (*digits < '1' || *digits > '9')
		return false;
	for (; *digits != '\0'; digits++) {
		if (*digits < '0' || *digits > '9')
			return false;
		value = value * 10 + (uint32)(*digits - '0');
		if (value > CLUSTER_WAL_RETENTION_MAX_THREADS)
			return false;
	}
	if (value == 0)
		return false;
	*out_thread_id = (uint16)value;
	return true;
}

bool
cluster_wal_file_identity_valid(const ClusterWalFileIdentity *identity, int wal_segsz_bytes)
{
	uint64 segments_per_id;

	if (identity == NULL || !IsValidWalSegSize(wal_segsz_bytes) || identity->thread_id == 0
		|| identity->thread_id > CLUSTER_WAL_RETENTION_MAX_THREADS
		|| (identity->kind != CLUSTER_WAL_FILE_NORMAL && identity->kind != CLUSTER_WAL_FILE_PARTIAL)
		|| identity->reserved_zero != 0 || identity->tli == 0)
		return false;
	segments_per_id = XLogSegmentsPerXLogId(wal_segsz_bytes);
	return identity->segno / segments_per_id <= UINT32_MAX;
}

bool
cluster_wal_file_identity_parse(const char *basename, uint16 thread_id, int wal_segsz_bytes,
								ClusterWalFileIdentity *out_identity)
{
	ClusterWalFileIdentity identity;
	char canonical[MAXFNAMELEN];
	char normal[XLOG_FNAME_LEN + 1];
	uint32 tli;
	uint32 log;
	uint32 seg;
	uint64 segments_per_id;
	bool partial;

	if (out_identity == NULL)
		return false;
	memset(out_identity, 0, sizeof(*out_identity));
	if (basename == NULL || !IsValidWalSegSize(wal_segsz_bytes) || thread_id == 0
		|| thread_id > CLUSTER_WAL_RETENTION_MAX_THREADS)
		return false;
	partial = IsPartialXLogFileName(basename);
	if (!partial && !IsXLogFileName(basename))
		return false;
	memcpy(normal, basename, XLOG_FNAME_LEN);
	normal[XLOG_FNAME_LEN] = '\0';
	if (sscanf(normal, "%8X%8X%8X", &tli, &log, &seg) != 3 || tli == 0)
		return false;
	segments_per_id = XLogSegmentsPerXLogId(wal_segsz_bytes);
	if ((uint64)seg >= segments_per_id)
		return false;
	memset(&identity, 0, sizeof(identity));
	identity.thread_id = thread_id;
	identity.kind = partial ? CLUSTER_WAL_FILE_PARTIAL : CLUSTER_WAL_FILE_NORMAL;
	identity.tli = (TimeLineID)tli;
	identity.segno = (XLogSegNo)((uint64)log * segments_per_id + seg);
	if (!cluster_wal_file_identity_valid(&identity, wal_segsz_bytes))
		return false;
	XLogFileName(canonical, identity.tli, identity.segno, wal_segsz_bytes);
	if (memcmp(canonical, normal, XLOG_FNAME_LEN + 1) != 0)
		return false;
	*out_identity = identity;
	return true;
}

bool
cluster_wal_file_long_header_matches(const ClusterWalFileIdentity *identity,
									 const XLogLongPageHeaderData *header, uint64 system_identifier,
									 int wal_segsz_bytes)
{
	XLogRecPtr expected_pageaddr;

	if (!cluster_wal_file_identity_valid(identity, wal_segsz_bytes) || header == NULL
		|| system_identifier == 0)
		return false;
	expected_pageaddr = (XLogRecPtr)(identity->segno * (uint64)wal_segsz_bytes);
	return header->std.xlp_magic == XLOG_PAGE_MAGIC && (header->std.xlp_info & ~XLP_ALL_FLAGS) == 0
		   && (header->std.xlp_info & XLP_LONG_HEADER) != 0 && header->std.xlp_tli == identity->tli
		   && header->std.xlp_pageaddr == expected_pageaddr
		   && header->std.xlp_thread_id == identity->thread_id
		   && header->std.xlp_cluster_flags == XLP_CLUSTER_FLAGS_RESERVED
		   && header->xlp_sysid == system_identifier
		   && header->xlp_seg_size == (uint32)wal_segsz_bytes
		   && header->xlp_xlog_blcksz == XLOG_BLCKSZ;
}

bool
cluster_wal_retention_interval_segment_bounds(const ClusterWalRetentionInterval *interval,
											  int wal_segsz_bytes, XLogSegNo *out_first,
											  XLogSegNo *out_last)
{
	if (out_first == NULL || out_last == NULL)
		return false;
	*out_first = 0;
	*out_last = 0;
	if (interval == NULL || !IsValidWalSegSize(wal_segsz_bytes) || interval->thread_id == 0
		|| interval->thread_id > CLUSTER_WAL_RETENTION_MAX_THREADS || interval->reserved_zero != 0
		|| interval->tli == 0 || interval->start_lsn == InvalidXLogRecPtr
		|| interval->end_lsn <= interval->start_lsn)
		return false;
	*out_first = interval->start_lsn / (uint64)wal_segsz_bytes;
	*out_last = (interval->end_lsn - 1) / (uint64)wal_segsz_bytes;
	return true;
}

bool
cluster_wal_retention_interval_intersects_file(const ClusterWalRetentionInterval *interval,
											   const ClusterWalFileIdentity *identity,
											   int wal_segsz_bytes)
{
	XLogSegNo first;
	XLogSegNo last;

	if (!cluster_wal_file_identity_valid(identity, wal_segsz_bytes)
		|| !cluster_wal_retention_interval_segment_bounds(interval, wal_segsz_bytes, &first, &last))
		return false;
	return interval->thread_id == identity->thread_id && interval->tli == identity->tli
		   && identity->segno >= first && identity->segno <= last;
}

bool
cluster_wal_retention_resid_encode(uint16 thread_id, ClusterResId *out_resid)
{
	if (out_resid == NULL)
		return false;
	memset(out_resid, 0, sizeof(*out_resid));
	if (thread_id == 0 || thread_id > CLUSTER_WAL_RETENTION_MAX_THREADS)
		return false;
	out_resid->field1 = thread_id;
	out_resid->type = CLUSTER_WAL_RETENTION_RESID_TYPE;
	out_resid->lockmethodid = DEFAULT_LOCKMETHOD;
	return true;
}

/*
 * WALR is outside the public PG locktag domain, but a NEED_PG_NATIVE_LOCK
 * result still requires the local leg of the existing seven-step contract.
 * Reserve one otherwise-unused USERLOCK namespace for that process-local
 * conflict.  The cross-node identity remains the frozen 0xFA ClusterResId;
 * this tag is never serialized or used as GES authority.
 */
static void
walr_native_locktag_init(uint16 thread_id, LOCKTAG *tag)
{
	memset(tag, 0, sizeof(*tag));
	tag->locktag_field1 = CLUSTER_WALR_NATIVE_TAG_MAGIC;
	tag->locktag_field2 = thread_id;
	tag->locktag_field3 = CLUSTER_WAL_RETENTION_RESID_TYPE;
	tag->locktag_type = LOCKTAG_USERLOCK;
	tag->locktag_lockmethodid = DEFAULT_LOCKMETHOD;
}

static bool
walr_request_has_native_lock(const ClusterLockAcquireRequest *request)
{
	return request->locktag.locktag_field1 == CLUSTER_WALR_NATIVE_TAG_MAGIC
		   && request->locktag.locktag_field2 == request->resid.field1
		   && request->locktag.locktag_field3 == CLUSTER_WAL_RETENTION_RESID_TYPE
		   && request->locktag.locktag_field4 == 0
		   && request->locktag.locktag_type == LOCKTAG_USERLOCK
		   && request->locktag.locktag_lockmethodid == DEFAULT_LOCKMETHOD;
}

static void
walr_native_lock_release_or_fatal(const ClusterLockAcquireRequest *request)
{
	if (walr_request_has_native_lock(request)
		&& !LockRelease(&request->locktag, request->lockmode, request->sessionLock))
		elog(FATAL, "could not release WALR PG-native lock");
}

/* Complete the PG-native leg before promoting a WALR reservation to a GES
 * holder.  A native refusal cancels the reservation and can never be recorded
 * as a coordinated grant. */
static ClusterLockAcquireResult
walr_request_acquire_actual(ClusterLockAcquireRequest *request)
{
	ClusterLockAcquireResult result;

	result = cluster_lock_acquire_seven_step(request);
	if (result == CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK) {
		LockAcquireResult native_result;

		walr_native_locktag_init((uint16)request->resid.field1, &request->locktag);
		native_result = LockAcquire(&request->locktag, request->lockmode, request->sessionLock,
									request->dontwait);
		if (native_result != LOCKACQUIRE_OK) {
			if (native_result == LOCKACQUIRE_ALREADY_CLEAR
				|| native_result == LOCKACQUIRE_ALREADY_HELD)
				walr_native_lock_release_or_fatal(request);
			(void)cluster_lock_acquire_s7_cleanup(request);
			return CLUSTER_LOCK_ACQUIRE_NOT_AVAIL;
		}
		result = cluster_lock_acquire_s5_promote(request);
		if (result != CLUSTER_LOCK_ACQUIRE_OK_GRANTED
			&& result != CLUSTER_LOCK_ACQUIRE_OK_CONVERTED)
			walr_native_lock_release_or_fatal(request);
	}
	return result;
}

/* Convert one existing WALR holder without creating a second GES holder.
 * preserve_request_id is used by X->S: keeping the confirmed X holder key
 * makes cleanup exact whether the downgrade reply is received or lost. */
static ClusterLockAcquireResult
walr_request_convert_actual(ClusterLockAcquireRequest *request, uint64 preserve_request_id,
							bool *out_cleanup_required)
{
	ClusterLockAcquireResult result;
	LockAcquireResult native_result;

	if (out_cleanup_required != NULL)
		*out_cleanup_required = false;
	result = cluster_lock_acquire_seven_step(request);
	if (result != CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK)
		return result;
	if (preserve_request_id != 0) {
		request->request_id = preserve_request_id;
		request->holder.request_id = preserve_request_id;
	}
	walr_native_locktag_init((uint16)request->resid.field1, &request->locktag);
	native_result = LockAcquire(&request->locktag, request->lockmode, request->sessionLock,
								request->dontwait);
	if (native_result != LOCKACQUIRE_OK && native_result != LOCKACQUIRE_ALREADY_HELD) {
		if (native_result == LOCKACQUIRE_ALREADY_CLEAR)
			walr_native_lock_release_or_fatal(request);
		return CLUSTER_LOCK_ACQUIRE_NOT_AVAIL;
	}
	result = cluster_lock_acquire_s5_promote(request);
	if (result != CLUSTER_LOCK_ACQUIRE_OK_CONVERTED) {
		if (request->lockmode == ExclusiveLock && result != CLUSTER_LOCK_ACQUIRE_NOT_AVAIL) {
			if (out_cleanup_required != NULL)
				*out_cleanup_required = true;
		} else
			walr_native_lock_release_or_fatal(request);
	}
	return result;
}

/* S6 confirmation and the matching local USERLOCK release are one recorded
 * WALR release.  On S6 uncertainty the local leg remains held for cleanup. */
static ClusterLockAcquireResult
walr_request_release_actual(const ClusterLockAcquireRequest *request)
{
	ClusterLockAcquireResult result;

	result = cluster_lock_acquire_s6_release(request);
	if (result == CLUSTER_LOCK_ACQUIRE_OK_GRANTED)
		walr_native_lock_release_or_fatal(request);
	return result;
}

static bool
root_token_matches_identity(const ClusterControlRootReadToken *token,
							const ClusterRecoveryDutyKey *duty)
{
	uint32 required_flags
		= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID;

	return token != NULL && duty != NULL && token->origin_thread_id == duty->origin_thread_id
		   && token->root_lineage_seq == duty->root_lineage_seq
		   && memcmp(token->authority_uuid, duty->authority_uuid, sizeof(token->authority_uuid))
				  == 0
		   && token->source != 0 && token->lifecycle >= CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
		   && token->lifecycle <= CLUSTER_CONTROL_ROOT_LIFECYCLE_RETIRED && token->reserved20 == 0
		   && token->reserved32 == 0 && token->file_txn_seq != 0 && token->root_publish_seq != 0
		   && token->record_crc32c != 0 && (token->root_flags & required_flags) == required_flags
		   && (token->root_flags & ~CLUSTER_CONTROL_ROOT_FLAGS_V1) == 0;
}

static bool
root_token_matches_recovery_duty(const ClusterControlRootReadToken *token,
								 const ClusterRecoveryDutyKey *duty)
{
	uint32 required_flags
		= CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID;

	return root_token_matches_identity(token, duty)
		   && token->lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
		   && (token->root_flags & required_flags) == required_flags;
}

static bool
pin_request_valid(const ClusterWalRetentionPinThreadRequest *request, int wal_segsz_bytes,
				  Size *total_bytes)
{
	uint32 i;
	Size interval_bytes;

	if (request == NULL || request->intervals == NULL || request->nintervals == 0
		|| !cluster_recovery_duty_key_valid_v1(&request->duty)
		|| !root_token_matches_recovery_duty(&request->root_read, &request->duty)
		|| request->formation == NULL || request->needs == NULL || request->admissions == NULL
		|| request->nintervals > MaxAllocSize / sizeof(*request->intervals))
		return false;
	interval_bytes = (Size)request->nintervals * sizeof(*request->intervals);
	if (*total_bytes > MaxAllocSize - interval_bytes)
		return false;
	*total_bytes += interval_bytes;

	for (i = 0; i < request->nintervals; i++) {
		const ClusterWalRetentionInterval *interval = &request->intervals[i];
		XLogSegNo first;
		XLogSegNo last;

		if (interval->thread_id != request->duty.origin_thread_id
			|| !cluster_wal_retention_interval_segment_bounds(interval, wal_segsz_bytes, &first,
															  &last))
			return false;
		if (i > 0) {
			const ClusterWalRetentionInterval *previous = &request->intervals[i - 1];

			if (previous->tli > interval->tli
				|| (previous->tli == interval->tli && previous->end_lsn >= interval->start_lsn))
				return false;
		}
	}
	return true;
}

static bool
pin_valid(const ClusterWalRetentionPin *pin)
{
	return pin != NULL && pin == active_pin && pin->magic == CLUSTER_WAL_PIN_MAGIC
		   && pin->owner_pid == MyProcPid && pin->owner == CurrentResourceOwner && pin->nthreads > 0
		   && pin->nthreads <= CLUSTER_WAL_RETENTION_MAX_THREADS && pin->threads != NULL
		   && pin->state >= CLUSTER_WAL_PIN_STATE_ACQUIRED_UNBOUND
		   && pin->state <= CLUSTER_WAL_PIN_STATE_SEALED;
}

static void
pin_free(ClusterWalRetentionPin *pin)
{
	uint16 i;

	if (pin == NULL)
		return;
	if (pin->threads != NULL) {
		for (i = 0; i < pin->nthreads; i++)
			if (pin->threads[i].intervals != NULL)
				pfree(pin->threads[i].intervals);
		pfree(pin->threads);
	}
	explicit_bzero(pin, sizeof(*pin));
	pfree(pin);
}

static ClusterWalrReleaseResult
pin_release_locks(ClusterWalRetentionPin *pin)
{
	uint16 i;
	bool unconfirmed = false;

	for (i = pin->nthreads; i > 0; i--) {
		ClusterWalPinLock *walr = &pin->threads[i - 1].walr;
		ClusterLockAcquireResult result;

		if (!walr->held && !walr->release_uncertain)
			continue;
		result = walr_request_release_actual(&walr->request);
		if (result == CLUSTER_LOCK_ACQUIRE_OK_GRANTED) {
			walr->held = false;
			walr->release_uncertain = false;
		} else {
			walr->held = true;
			walr->release_uncertain = true;
			unconfirmed = true;
		}
	}
	return unconfirmed ? CLUSTER_WALR_RELEASE_UNCONFIRMED : CLUSTER_WALR_RELEASE_CONFIRMED;
}

static ClusterWalPinResult
pin_fail_after_locks(ClusterWalRetentionPin *pin, ClusterWalPinResult result,
					 ClusterWalRetentionPin **out_pin)
{
	if (pin_release_locks(pin) == CLUSTER_WALR_RELEASE_UNCONFIRMED) {
		pin->poisoned = true;
		*out_pin = pin;
		return CLUSTER_WAL_PIN_RELEASE_UNCERTAIN;
	}
	active_pin = NULL;
	pin_free(pin);
	return result;
}

static ClusterWalPinResult
pin_current_after_grants(ClusterWalRetentionPin *pin)
{
	uint16 i;

	for (i = 0; i < pin->nthreads; i++) {
		ClusterWalPinThread *thread = &pin->threads[i];
		ClusterControlRootSnapshot snapshot;
		ClusterControlRootResult result;
		PgracExternalFenceDenyReason reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;

		result = cluster_control_root_revalidate(&thread->root_read, &thread->duty, &snapshot);
		if (!control_root_read_ready(result)
			|| memcmp(&snapshot.identity, &thread->duty, sizeof(thread->duty)) != 0
			|| snapshot.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
			|| snapshot.root_flags != thread->root_read.root_flags
			|| cluster_formation_witness_revalidate_nowait(thread->formation)
				   != CLUSTER_FORMATION_WITNESS_READY
			|| !cluster_external_fence_need_set_revalidate_nowait(thread->needs, thread->formation,
																  &reason)
			|| !cluster_external_fence_revalidate_set_nowait(thread->admissions, thread->needs,
															 thread->formation, &reason))
			return CLUSTER_WAL_PIN_STALE;
	}
	return CLUSTER_WAL_PIN_OK;
}

ClusterWalPinResult
cluster_wal_retention_pin_acquire(const ClusterWalRetentionPinThreadRequest *requests,
								  uint16 nthreads, ClusterWalRetentionPin **out_pin)
{
	ClusterWalRetentionPin *pin;
	Size total_bytes;
	uint16 i;

	if (out_pin == NULL || *out_pin != NULL || requests == NULL || CurrentResourceOwner == NULL
		|| nthreads == 0 || nthreads > CLUSTER_WAL_RETENTION_MAX_THREADS)
		return CLUSTER_WAL_PIN_INVALID;
	if (active_pin != NULL)
		return CLUSTER_WAL_PIN_CAPACITY;
	total_bytes = sizeof(*pin);
	if (nthreads > (MaxAllocSize - total_bytes) / sizeof(ClusterWalPinThread))
		return CLUSTER_WAL_PIN_INVALID;
	total_bytes += (Size)nthreads * sizeof(ClusterWalPinThread);
	for (i = 0; i < nthreads; i++) {
		if (!pin_request_valid(&requests[i], wal_segment_size, &total_bytes)
			|| (i > 0
				&& requests[i - 1].duty.origin_thread_id >= requests[i].duty.origin_thread_id))
			return CLUSTER_WAL_PIN_INVALID;
	}
	walr_resource_ensure_callback();

	pin = palloc0(sizeof(*pin));
	pin->threads = palloc0((Size)nthreads * sizeof(*pin->threads));
	pin->magic = CLUSTER_WAL_PIN_MAGIC;
	pin->owner_pid = MyProcPid;
	pin->owner = CurrentResourceOwner;
	pin->nthreads = nthreads;
	pin->state = CLUSTER_WAL_PIN_STATE_ACQUIRED_UNBOUND;
	for (i = 0; i < nthreads; i++) {
		ClusterWalPinThread *thread = &pin->threads[i];
		Size bytes = (Size)requests[i].nintervals * sizeof(*thread->intervals);

		thread->intervals = palloc0(bytes);
		memcpy(thread->intervals, requests[i].intervals, bytes);
		thread->nintervals = requests[i].nintervals;
		thread->duty = requests[i].duty;
		thread->root_read = requests[i].root_read;
		thread->formation = requests[i].formation;
		thread->needs = requests[i].needs;
		thread->admissions = requests[i].admissions;
	}
	active_pin = pin;

	for (i = 0; i < nthreads; i++) {
		ClusterWalPinLock *walr = &pin->threads[i].walr;
		ClusterLockAcquireResult result;

		memset(&walr->request, 0, sizeof(walr->request));
		if (!cluster_wal_retention_resid_encode(pin->threads[i].duty.origin_thread_id,
												&walr->request.resid))
			return pin_fail_after_locks(pin, CLUSTER_WAL_PIN_INVALID, out_pin);
		walr->request.lockmode = ShareLock;
		walr->request.op = CLUSTER_LOCK_OP_REQUEST;
		walr->request.current_mode = NoLock;
		walr->request.lockmethod_id = DEFAULT_LOCKMETHOD;
		walr->request.dontwait = true;
		walr->request.sessionLock = false;
		walr->request.caller_local_start_ts_ms = (uint64)(GetCurrentTimestamp() / 1000);
		walr->request.timeout_ms = 1;
		walr->request.wait_event = WAIT_EVENT_CLUSTER_GES_REPLY_WAIT;
		result = walr_request_acquire_actual(&walr->request);
		if (result != CLUSTER_LOCK_ACQUIRE_OK_GRANTED)
			return pin_fail_after_locks(pin, CLUSTER_WAL_PIN_UNAVAILABLE, out_pin);
		walr->held = true;
	}
	if (pin_current_after_grants(pin) != CLUSTER_WAL_PIN_OK)
		return pin_fail_after_locks(pin, CLUSTER_WAL_PIN_STALE, out_pin);
	*out_pin = pin;
	return CLUSTER_WAL_PIN_OK;
}

static bool
serial_matches_thread(ClusterRecoverySerialGuard *serial, const ClusterWalPinThread *thread)
{
	return serial != NULL && serial->held && !serial->release_uncertain
		   && memcmp(&serial->duty, &thread->duty, sizeof(thread->duty)) == 0
		   && memcmp(&serial->root_read_token, &thread->root_read, sizeof(thread->root_read)) == 0
		   && serial->formation == thread->formation && serial->fence_need_set == thread->needs
		   && serial->fence_admission_set == thread->admissions
		   && cluster_recovery_serial_revalidate(serial) == CLUSTER_RECOVERY_SERIAL_CURRENT;
}

static ClusterWalPinThread *
pin_find_thread(ClusterWalRetentionPin *pin, uint16 thread_id)
{
	uint16 i;

	if (!pin_valid(pin))
		return NULL;
	for (i = 0; i < pin->nthreads; i++)
		if (pin->threads[i].duty.origin_thread_id == thread_id)
			return &pin->threads[i];
	return NULL;
}

ClusterWalPinResult
cluster_wal_retention_pin_bind_one(ClusterWalRetentionPin *pin,
								   ClusterRecoverySerialGuard *held_serial)
{
	if (!pin_valid(pin))
		return CLUSTER_WAL_PIN_INVALID;
	if (pin->poisoned)
		return CLUSTER_WAL_PIN_RELEASE_UNCERTAIN;
	if (pin->state != CLUSTER_WAL_PIN_STATE_ACQUIRED_UNBOUND || pin->nthreads != 1)
		return CLUSTER_WAL_PIN_INVALID;
	if (!serial_matches_thread(held_serial, &pin->threads[0]))
		return CLUSTER_WAL_PIN_STALE;
	pin->threads[0].serial = held_serial;
	pin->state = CLUSTER_WAL_PIN_STATE_BOUND_ONE;
	return CLUSTER_WAL_PIN_OK;
}

ClusterWalPinResult
cluster_wal_retention_pin_bind_set(ClusterWalRetentionPin *pin,
								   ClusterRecoverySerialGuardSet *held_set)
{
	uint16 i;

	if (!pin_valid(pin))
		return CLUSTER_WAL_PIN_INVALID;
	if (pin->poisoned)
		return CLUSTER_WAL_PIN_RELEASE_UNCERTAIN;
	if (pin->state != CLUSTER_WAL_PIN_STATE_ACQUIRED_UNBOUND || pin->nthreads <= 1
		|| held_set == NULL || held_set->count != pin->nthreads)
		return CLUSTER_WAL_PIN_INVALID;
	for (i = 0; i < pin->nthreads; i++)
		if (!serial_matches_thread(&held_set->guards[i], &pin->threads[i]))
			return CLUSTER_WAL_PIN_STALE;
	for (i = 0; i < pin->nthreads; i++)
		pin->threads[i].serial = &held_set->guards[i];
	pin->serial_set = held_set;
	pin->state = CLUSTER_WAL_PIN_STATE_BOUND_SET;
	return CLUSTER_WAL_PIN_OK;
}

static ClusterWalPinResult
pin_revalidate_bound(ClusterWalRetentionPin *pin)
{
	uint16 i;

	if (pin->state != CLUSTER_WAL_PIN_STATE_BOUND_ONE
		&& pin->state != CLUSTER_WAL_PIN_STATE_BOUND_SET)
		return CLUSTER_WAL_PIN_INVALID;
	for (i = 0; i < pin->nthreads; i++)
		if (!serial_matches_thread(pin->threads[i].serial, &pin->threads[i])) {
			pin->poisoned = true;
			return CLUSTER_WAL_PIN_STALE;
		}
	return CLUSTER_WAL_PIN_OK;
}

ClusterWalPinResult
cluster_wal_retention_pin_preflight_revalidate_wait_v1(ClusterWalRetentionPin *pin)
{
	ClusterWalPinResult result;

	if (!pin_valid(pin) || pin->state != CLUSTER_WAL_PIN_STATE_ACQUIRED_UNBOUND)
		return CLUSTER_WAL_PIN_INVALID;
	if (pin->poisoned)
		return CLUSTER_WAL_PIN_STALE;
	result = pin_current_after_grants(pin);
	if (result != CLUSTER_WAL_PIN_OK)
		pin->poisoned = true;
	return result;
}

ClusterWalPinResult
cluster_wal_retention_pin_revalidate(ClusterWalRetentionPin *pin)
{
	if (!pin_valid(pin))
		return CLUSTER_WAL_PIN_INVALID;
	if (pin->poisoned)
		return CLUSTER_WAL_PIN_STALE;
	return pin_revalidate_bound(pin);
}

ClusterWalPinResult
cluster_wal_retention_pin_seal_for_root_publish(ClusterWalRetentionPin *pin)
{
	ClusterWalPinResult result;

	if (!pin_valid(pin))
		return CLUSTER_WAL_PIN_INVALID;
	if (pin->poisoned)
		return CLUSTER_WAL_PIN_STALE;
	result = pin_revalidate_bound(pin);
	if (result != CLUSTER_WAL_PIN_OK)
		return result;
	pin->state = CLUSTER_WAL_PIN_STATE_SEALED;
	return CLUSTER_WAL_PIN_OK;
}

static bool
pin_root_token_matches_snapshot(const ClusterControlRootReadToken *token,
								const ClusterControlRootSnapshot *snapshot,
								const ClusterRecoveryDutyKey *duty)
{
	return token != NULL && snapshot != NULL && duty != NULL
		   && memcmp(&snapshot->identity, duty, sizeof(*duty)) == 0
		   && memcmp(token->authority_uuid, duty->authority_uuid, 16) == 0
		   && token->origin_thread_id == duty->origin_thread_id && token->source != 0
		   && token->lifecycle == snapshot->lifecycle
		   && token->root_lineage_seq == duty->root_lineage_seq && token->reserved20 == 0
		   && token->reserved32 == 0 && token->file_txn_seq != 0 && token->root_publish_seq != 0
		   && token->record_crc32c != 0 && token->root_flags == snapshot->root_flags
		   && (token->root_flags & ~CLUSTER_CONTROL_ROOT_FLAGS_V1) == 0;
}

static bool
pin_root_immutable_tuple_equal(const ClusterControlRootSnapshot *left,
							   const ClusterControlRootSnapshot *right)
{
	return memcmp(&left->identity, &right->identity, sizeof(left->identity)) == 0
		   && left->checkpoint_tli == right->checkpoint_tli
		   && left->checkpoint_source_kind == right->checkpoint_source_kind
		   && left->checkpoint_lower_lsn == right->checkpoint_lower_lsn
		   && left->checkpoint_record_crc32c == right->checkpoint_record_crc32c
		   && left->tail_tli == right->tail_tli
		   && left->tail_validation_kind == right->tail_validation_kind
		   && left->validated_tail_lsn_exclusive == right->validated_tail_lsn_exclusive
		   && left->tail_last_record_lsn == right->tail_last_record_lsn
		   && left->tail_last_record_crc32c == right->tail_last_record_crc32c;
}

ClusterWalPinResult
cluster_wal_retention_pin_adopt_root_readback_v1(
	ClusterWalRetentionPin *pin, const ClusterControlRootSnapshot *expected_snapshot,
	const ClusterControlRootReadToken *expected_token,
	const ClusterControlRootSnapshot *observed_snapshot,
	const ClusterControlRootReadToken *observed_token)
{
	ClusterWalPinThread *thread;
	const ClusterWalRetentionInterval *interval;

	if (!pin_valid(pin) || expected_snapshot == NULL || expected_token == NULL
		|| observed_snapshot == NULL || observed_token == NULL)
		return CLUSTER_WAL_PIN_INVALID;
	if (pin->poisoned || pin->state != CLUSTER_WAL_PIN_STATE_SEALED)
		return CLUSTER_WAL_PIN_STALE;
	thread = pin_find_thread(pin, expected_token->origin_thread_id);
	if (thread == NULL || thread->serial == NULL || thread->serial->held
		|| thread->serial->release_uncertain || thread->nintervals != 1
		|| memcmp(&thread->root_read, expected_token, sizeof(*expected_token)) != 0
		|| !pin_root_token_matches_snapshot(expected_token, expected_snapshot, &thread->duty)
		|| !pin_root_token_matches_snapshot(observed_token, observed_snapshot, &thread->duty))
		return CLUSTER_WAL_PIN_STALE;
	interval = &thread->intervals[0];
	if (expected_snapshot->lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
		|| (observed_snapshot->lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
			&& observed_snapshot->lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE)
		|| interval->thread_id != thread->duty.origin_thread_id
		|| interval->tli != expected_snapshot->checkpoint_tli
		|| interval->tli != expected_snapshot->tail_tli
		|| interval->start_lsn != expected_snapshot->checkpoint_lower_lsn
		|| interval->end_lsn != expected_snapshot->validated_tail_lsn_exclusive
		|| !pin_root_immutable_tuple_equal(expected_snapshot, observed_snapshot)
		|| observed_token->file_txn_seq < expected_token->file_txn_seq
		|| observed_token->root_publish_seq < expected_token->root_publish_seq)
		return CLUSTER_WAL_PIN_STALE;
	thread->root_read = *observed_token;
	return CLUSTER_WAL_PIN_OK;
}

ClusterWalrReleaseResult
cluster_wal_retention_pin_release(ClusterWalRetentionPin **pin)
{
	ClusterWalrReleaseResult result;

	if (pin == NULL)
		return CLUSTER_WALR_RELEASE_INVALID;
	if (*pin == NULL)
		return CLUSTER_WALR_RELEASE_NOT_HELD;
	if (!pin_valid(*pin))
		return CLUSTER_WALR_RELEASE_INVALID;
	result = pin_release_locks(*pin);
	if (result != CLUSTER_WALR_RELEASE_CONFIRMED) {
		(*pin)->poisoned = true;
		return result;
	}
	active_pin = NULL;
	pin_free(*pin);
	*pin = NULL;
	return CLUSTER_WALR_RELEASE_CONFIRMED;
}

static bool
walr_share_request_init(uint16 thread_id, ClusterLockAcquireRequest *request)
{
	memset(request, 0, sizeof(*request));
	/* RF-ROOT P9 verification (contract / follow-up): the resid encode must not
	 * live inside Assert — a release build would ship a zeroed resid into
	 * the GES lock request.  Explicit fail-closed instead (AGENTS.md). */
	if (!cluster_wal_retention_resid_encode(thread_id, &request->resid))
		return false;
	request->lockmode = ShareLock;
	request->op = CLUSTER_LOCK_OP_REQUEST;
	request->current_mode = NoLock;
	request->lockmethod_id = DEFAULT_LOCKMETHOD;
	request->dontwait = true;
	request->sessionLock = false;
	request->caller_local_start_ts_ms = (uint64)(GetCurrentTimestamp() / 1000);
	request->timeout_ms = 1;
	request->wait_event = WAIT_EVENT_CLUSTER_GES_REPLY_WAIT;
	return true;
}

ClusterWalPinResult
cluster_wal_retention_root_publish_begin_exact(const ClusterControlRootReadToken *expected_root,
											   bool require_sealed_pin,
											   ClusterWalRootPublishGuard **out_guard)
{
	ClusterWalRootPublishGuard *guard;
	uint16 thread_id;
	uint16 i;

	if (out_guard == NULL || *out_guard != NULL || expected_root == NULL
		|| CurrentResourceOwner == NULL || expected_root->origin_thread_id == 0
		|| expected_root->origin_thread_id > CLUSTER_WAL_RETENTION_MAX_THREADS
		|| expected_root->source == 0
		|| expected_root->lifecycle < CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
		|| expected_root->lifecycle > CLUSTER_CONTROL_ROOT_LIFECYCLE_RETIRED
		|| expected_root->reserved20 != 0 || expected_root->reserved32 != 0
		|| expected_root->root_lineage_seq == 0 || expected_root->file_txn_seq == 0
		|| expected_root->root_publish_seq == 0 || expected_root->record_crc32c == 0
		|| (expected_root->root_flags & ~CLUSTER_CONTROL_ROOT_FLAGS_V1) != 0)
		return CLUSTER_WAL_PIN_INVALID;
	thread_id = expected_root->origin_thread_id;
	if (active_root_publish_guard != NULL)
		return CLUSTER_WAL_PIN_CAPACITY;
	walr_resource_ensure_callback();

	guard = palloc0(sizeof(*guard));
	guard->magic = CLUSTER_WAL_ROOT_PUBLISH_MAGIC;
	guard->owner_pid = MyProcPid;
	guard->owner = CurrentResourceOwner;
	guard->thread_id = thread_id;
	if (active_pin != NULL) {
		if (!pin_valid(active_pin) || active_pin->poisoned
			|| active_pin->state != CLUSTER_WAL_PIN_STATE_SEALED) {
			pfree(guard);
			return CLUSTER_WAL_PIN_STALE;
		}
		for (i = 0; i < active_pin->nthreads; i++)
			if (active_pin->threads[i].duty.origin_thread_id == thread_id)
				break;
		if (i == active_pin->nthreads
			|| memcmp(&active_pin->threads[i].root_read, expected_root, sizeof(*expected_root)) != 0
			|| !active_pin->threads[i].walr.held || active_pin->threads[i].walr.release_uncertain) {
			pfree(guard);
			return CLUSTER_WAL_PIN_STALE;
		}
		guard->borrowed_from_pin = true;
	} else {
		ClusterLockAcquireResult result;

		if (require_sealed_pin) {
			pfree(guard);
			return CLUSTER_WAL_PIN_STALE;
		}
		if (!walr_share_request_init(thread_id, &guard->walr.request)) {
			pfree(guard);
			return CLUSTER_WAL_PIN_UNAVAILABLE;
		}
		result = walr_request_acquire_actual(&guard->walr.request);
		if (result != CLUSTER_LOCK_ACQUIRE_OK_GRANTED) {
			pfree(guard);
			return CLUSTER_WAL_PIN_UNAVAILABLE;
		}
		guard->walr.held = true;
	}
	active_root_publish_guard = guard;
	*out_guard = guard;
	return CLUSTER_WAL_PIN_OK;
}

ClusterWalrReleaseResult
cluster_wal_retention_root_publish_end(ClusterWalRootPublishGuard **guard)
{
	ClusterWalrReleaseResult result = CLUSTER_WALR_RELEASE_CONFIRMED;

	if (guard == NULL)
		return CLUSTER_WALR_RELEASE_INVALID;
	if (*guard == NULL)
		return CLUSTER_WALR_RELEASE_NOT_HELD;
	if (*guard != active_root_publish_guard || (*guard)->magic != CLUSTER_WAL_ROOT_PUBLISH_MAGIC
		|| (*guard)->owner_pid != MyProcPid || (*guard)->owner != CurrentResourceOwner)
		return CLUSTER_WALR_RELEASE_INVALID;
	if (!(*guard)->borrowed_from_pin) {
		ClusterLockAcquireResult lock_result;

		lock_result = walr_request_release_actual(&(*guard)->walr.request);
		if (lock_result != CLUSTER_LOCK_ACQUIRE_OK_GRANTED) {
			(*guard)->walr.release_uncertain = true;
			result = CLUSTER_WALR_RELEASE_UNCONFIRMED;
		} else
			(*guard)->walr.held = false;
	}
	if (result == CLUSTER_WALR_RELEASE_CONFIRMED) {
		active_root_publish_guard = NULL;
		explicit_bzero(*guard, sizeof(**guard));
		pfree(*guard);
		*guard = NULL;
	}
	return result;
}

static void
walr_resource_release_callback(ResourceReleasePhase phase, bool isCommit, bool isTopLevel,
							   void *arg)
{
	(void)isCommit;
	(void)isTopLevel;
	(void)arg;
	if (phase != RESOURCE_RELEASE_BEFORE_LOCKS)
		return;

	if (active_reuse_guard != NULL && active_reuse_guard->owner == CurrentResourceOwner) {
		ClusterLockAcquireResult lock_result;
		ClusterWalPinThread *pin_thread = NULL;

		if (active_reuse_guard->walr.converted_from_pin) {
			pin_thread = pin_find_thread(active_reuse_guard->pin_or_null,
										 active_reuse_guard->duty.origin_thread_id);
			if (pin_thread == NULL || !pin_thread->walr.held
				|| pin_thread->serial != active_reuse_guard->serial_or_null)
				elog(FATAL, "lost WALR retained-source holder during action cleanup");
		}

		lock_result = walr_request_release_actual(&active_reuse_guard->walr.acquire_request);
		if (lock_result != CLUSTER_LOCK_ACQUIRE_OK_GRANTED)
			elog(FATAL, "could not confirm WALR action-guard cleanup");
		active_reuse_guard->walr.held = false;
		if (pin_thread != NULL) {
			/* The S->X conversion mutated the same GES holder, so the S6
			 * above is the sole coordinated release.  Drop only the original
			 * process-local S leg here and prevent pin cleanup from issuing a
			 * second S6 for the same holder. */
			walr_native_lock_release_or_fatal(&pin_thread->walr.request);
			pin_thread->walr.held = false;
			pin_thread->walr.release_uncertain = false;
		}
		wal_reuse_guard_close_handles(active_reuse_guard);
		active_reuse_guard = NULL;
	}
	if (active_e1_context != NULL && active_e1_context->owner == CurrentResourceOwner) {
		ClusterLockAcquireResult lock_result;

		lock_result = walr_request_release_actual(&active_e1_context->coarse_walr.acquire_request);
		if (lock_result != CLUSTER_LOCK_ACQUIRE_OK_GRANTED)
			elog(FATAL, "could not confirm coarse WAL-retention cleanup");
		active_e1_context->coarse_walr.held = false;
		cluster_formation_witness_destroy(&active_e1_context->formation);
		explicit_bzero(active_e1_context, sizeof(*active_e1_context));
		active_e1_context = NULL;
	}
	if (active_action_context != NULL && active_action_context->owner == CurrentResourceOwner) {
		cluster_formation_witness_destroy(&active_action_context->formation);
		explicit_bzero(active_action_context, sizeof(*active_action_context));
		active_action_context = NULL;
	}
	if (active_root_publish_guard != NULL
		&& active_root_publish_guard->owner == CurrentResourceOwner) {
		ClusterWalRootPublishGuard *guard = active_root_publish_guard;

		if (cluster_wal_retention_root_publish_end(&guard) != CLUSTER_WALR_RELEASE_CONFIRMED)
			elog(FATAL, "could not confirm WALR root-publisher cleanup");
	}
	if (active_pin != NULL && active_pin->owner == CurrentResourceOwner) {
		ClusterWalRetentionPin *pin = active_pin;

		if (pin_release_locks(pin) != CLUSTER_WALR_RELEASE_CONFIRMED)
			elog(FATAL, "could not confirm WALR retained-source cleanup");
		active_pin = NULL;
		pin_free(pin);
	}
}

static bool
wal_reuse_guard_valid(const ClusterWalReuseActionGuard *guard)
{
	return guard != NULL && guard->magic == CLUSTER_WAL_REUSE_GUARD_MAGIC
		   && guard->version == CLUSTER_WAL_REUSE_GUARD_VERSION && guard->owner_pid == MyProcPid
		   && guard->self_address == (uintptr_t)guard && guard->owner == CurrentResourceOwner
		   && CurrentResourceOwner != NULL && guard->state <= CLUSTER_WAL_GUARD_BOOKKEPT
		   && (guard->flags & ~CLUSTER_WAL_GUARD_F_KNOWN_MASK) == 0;
}

ClusterWalReuseGuardResult
cluster_wal_reuse_guard_init(ClusterWalReuseActionGuard *guard,
							 ClusterWalReuseDenyReason *out_reason)
{
	static const ClusterWalReuseActionGuard zero_guard;

	if (out_reason == NULL)
		return CLUSTER_WAL_GUARD_INVALID;
	*out_reason = CLUSTER_WAL_DENY_NONE;
	if (guard == NULL || CurrentResourceOwner == NULL
		|| memcmp(guard, &zero_guard, sizeof(*guard)) != 0) {
		*out_reason = CLUSTER_WAL_DENY_GUARD_STATE;
		return CLUSTER_WAL_GUARD_INVALID;
	}
	guard->magic = CLUSTER_WAL_REUSE_GUARD_MAGIC;
	guard->version = CLUSTER_WAL_REUSE_GUARD_VERSION;
	guard->state = CLUSTER_WAL_GUARD_EMPTY;
	guard->owner_pid = MyProcPid;
	guard->self_address = (uintptr_t)guard;
	guard->source_dir_handle = (intptr_t)-1;
	guard->source_handle = (intptr_t)-1;
	guard->fork_source_handle = (intptr_t)-1;
	guard->owner = CurrentResourceOwner;
	return CLUSTER_WAL_GUARD_OK;
}

static bool
wal_reuse_request_has_no_source(const ClusterWalReuseGuardRequest *request)
{
	static const ClusterWalFileIdentity zero_file;

	return request->source_kind == CLUSTER_WAL_INSTALL_SOURCE_NONE
		   && memcmp(&request->source_carrier, &zero_file, sizeof(zero_file)) == 0
		   && request->fork_lsn == InvalidXLogRecPtr
		   && request->source_coverage_start == InvalidXLogRecPtr
		   && request->source_coverage_end == InvalidXLogRecPtr;
}

static bool
wal_reuse_request_source_valid(const ClusterWalReuseGuardRequest *request)
{
	XLogRecPtr segment_start;
	XLogRecPtr segment_end;

	if (request->file.segno > UINT64_MAX / (uint64)wal_segment_size)
		return false;
	segment_start = (XLogRecPtr)(request->file.segno * (uint64)wal_segment_size);
	if (segment_start > UINT64_MAX - (uint64)wal_segment_size)
		return false;
	segment_end = segment_start + (uint64)wal_segment_size;
	if (request->entry == CLUSTER_WAL_REUSE_E3_ARCHIVE_END)
		return request->source_kind == CLUSTER_WAL_INSTALL_SOURCE_FORK_COPY_TEMP
			   && cluster_wal_file_identity_valid(&request->source_carrier, wal_segment_size)
			   && request->source_carrier.kind == CLUSTER_WAL_FILE_NORMAL
			   && request->source_carrier.segno == request->file.segno
			   && request->source_carrier.tli != request->file.tli
			   && request->fork_lsn > segment_start && request->fork_lsn < segment_end
			   && request->source_coverage_start == segment_start
			   && request->source_coverage_end == request->fork_lsn;
	if (request->entry == CLUSTER_WAL_REUSE_E6_ARCHIVE_KEEP)
		return request->source_kind == CLUSTER_WAL_INSTALL_SOURCE_ARCHIVE_STAGE
			   && memcmp(&request->source_carrier, &request->file, sizeof(request->file)) == 0
			   && request->fork_lsn == InvalidXLogRecPtr
			   && request->source_coverage_start >= segment_start
			   && request->source_coverage_start < request->source_coverage_end
			   && request->source_coverage_end <= segment_end;
	return false;
}

static bool
wal_reuse_request_shape_valid(const ClusterWalReuseGuardRequest *request)
{
	bool no_source;

	if (request == NULL || !cluster_wal_file_identity_valid(&request->file, wal_segment_size)
		|| !cluster_recovery_duty_key_valid_v1(&request->duty)
		|| !root_token_matches_identity(&request->root_read, &request->duty)
		|| request->file.thread_id != request->duty.origin_thread_id || request->formation == NULL)
		return false;
	no_source = wal_reuse_request_has_no_source(request);
	switch (request->entry) {
	case CLUSTER_WAL_REUSE_E1_CHECKPOINT_RESTARTPOINT:
	case CLUSTER_WAL_REUSE_E2_APPLY_TIMELINE_SWITCH:
		return request->action == CLUSTER_WAL_ACTION_RETIRE_RECYCLE_OR_REMOVE && no_source;
	case CLUSTER_WAL_REUSE_E3_ARCHIVE_END:
		if (request->action == CLUSTER_WAL_ACTION_RETIRE_RECYCLE_OR_REMOVE)
			return no_source;
		return (request->action == CLUSTER_WAL_ACTION_FORCE_REPLACE
				|| request->action == CLUSTER_WAL_ACTION_CREATE_ABSENT)
			   && wal_reuse_request_source_valid(request);
	case CLUSTER_WAL_REUSE_E4_PARTIAL_RENAME:
		return request->action == CLUSTER_WAL_ACTION_RENAME_PARTIAL && no_source;
	case CLUSTER_WAL_REUSE_E6_ARCHIVE_KEEP:
		return (request->action == CLUSTER_WAL_ACTION_ARCHIVE_REPLACE
				|| request->action == CLUSTER_WAL_ACTION_CREATE_ABSENT)
			   && wal_reuse_request_source_valid(request);
	case CLUSTER_WAL_REUSE_E5_RESTORE_STAGING:
	case CLUSTER_WAL_REUSE_E7_EXTERNAL_CLEANUP:
	default:
		return false;
	}
}

#ifndef WIN32
static int64
wal_reuse_stat_mtime_ns(const struct stat *st)
{
#ifdef __APPLE__
	return (int64)st->st_mtimespec.tv_sec * INT64_C(1000000000) + st->st_mtimespec.tv_nsec;
#else
	return (int64)st->st_mtim.tv_sec * INT64_C(1000000000) + st->st_mtim.tv_nsec;
#endif
}

static int64
wal_reuse_stat_ctime_ns(const struct stat *st)
{
#ifdef __APPLE__
	return (int64)st->st_ctimespec.tv_sec * INT64_C(1000000000) + st->st_ctimespec.tv_nsec;
#else
	return (int64)st->st_ctim.tv_sec * INT64_C(1000000000) + st->st_ctim.tv_nsec;
#endif
}

static bool
wal_reuse_same_leaf_stat(const struct stat *left, const struct stat *right)
{
	return left->st_dev == right->st_dev && left->st_ino == right->st_ino
		   && left->st_mode == right->st_mode && left->st_size == right->st_size
		   && wal_reuse_stat_mtime_ns(left) == wal_reuse_stat_mtime_ns(right)
		   && wal_reuse_stat_ctime_ns(left) == wal_reuse_stat_ctime_ns(right);
}
#endif

static bool
wal_reuse_target_basename(const ClusterWalFileIdentity *file, char *basename, Size basename_size)
{
	char normal[XLOG_FNAME_LEN + 1];
	int written;

	XLogFileName(normal, file->tli, file->segno, wal_segment_size);
	if (file->kind == CLUSTER_WAL_FILE_PARTIAL)
		written = snprintf(basename, basename_size, "%s.partial", normal);
	else
		written = snprintf(basename, basename_size, "%s", normal);
	return written > 0 && written < (int)basename_size;
}

static bool
wal_reuse_stamp_held_target(const ClusterWalFileIdentity *file, uint64 system_identifier, int dirfd,
							int fd, ClusterWalFileObjectStamp *out_stamp)
{
#ifndef WIN32
	char basename[MAXFNAMELEN];
	uint8 header_bytes[SizeOfXLogLongPHD];
	XLogLongPageHeaderData header;
	ClusterWalFileIdentity parsed;
	struct stat dir_stat;
	struct stat before;
	struct stat named_before;
	struct stat after;
	struct stat named_after;
	int at_flags;

	memset(out_stamp, 0, sizeof(*out_stamp));
#ifdef AT_SYMLINK_NOFOLLOW
	at_flags = AT_SYMLINK_NOFOLLOW;
#else
	return false;
#endif
	if (dirfd < 0 || fd < 0 || !wal_reuse_target_basename(file, basename, sizeof(basename))
		|| fstat(dirfd, &dir_stat) != 0 || !S_ISDIR(dir_stat.st_mode) || fstat(fd, &before) != 0
		|| !S_ISREG(before.st_mode) || before.st_size < (off_t)SizeOfXLogLongPHD
		|| fstatat(dirfd, basename, &named_before, at_flags) != 0
		|| !wal_reuse_same_leaf_stat(&before, &named_before)
		|| pread(fd, header_bytes, sizeof(header_bytes), 0) != (ssize_t)sizeof(header_bytes)
		|| fstat(fd, &after) != 0 || fstatat(dirfd, basename, &named_after, at_flags) != 0
		|| !wal_reuse_same_leaf_stat(&before, &after)
		|| !wal_reuse_same_leaf_stat(&after, &named_after)
		|| !cluster_wal_file_identity_parse(basename, file->thread_id, wal_segment_size, &parsed)
		|| memcmp(&parsed, file, sizeof(parsed)) != 0)
		return false;
	memset(&header, 0, sizeof(header));
	memcpy(&header, header_bytes, Min(sizeof(header), sizeof(header_bytes)));
	if (!cluster_wal_file_long_header_matches(&parsed, &header, system_identifier,
											  wal_segment_size))
		return false;
	out_stamp->platform = CLUSTER_WAL_OBJECT_POSIX;
	out_stamp->directory_device = (uint64)dir_stat.st_dev;
	out_stamp->directory_file_id_lo = (uint64)dir_stat.st_ino;
	out_stamp->file_device = (uint64)after.st_dev;
	out_stamp->file_id_lo = (uint64)after.st_ino;
	out_stamp->size_bytes = (uint64)after.st_size;
	out_stamp->mtime_ns = wal_reuse_stat_mtime_ns(&after);
	out_stamp->ctime_ns = wal_reuse_stat_ctime_ns(&after);
	out_stamp->mode_bits = (uint32)after.st_mode;
	out_stamp->parsed_identity = parsed;
	out_stamp->long_header = header;
	return true;
#else
	(void)file;
	(void)system_identifier;
	(void)dirfd;
	(void)fd;
	memset(out_stamp, 0, sizeof(*out_stamp));
	return false;
#endif
}

static bool
wal_reuse_open_target(const ClusterWalFileIdentity *file, uint64 system_identifier,
					  ClusterWalFileObjectStamp *out_stamp, intptr_t *out_dir_handle,
					  intptr_t *out_handle)
{
#ifndef WIN32
	char dirname[32];
	char dirpath[MAXPGPATH];
	char basename[MAXFNAMELEN];
	int dirfd;
	int fd;
	int dir_flags = O_RDONLY;
	int open_flags = O_RDONLY;

	*out_dir_handle = (intptr_t)-1;
	*out_handle = (intptr_t)-1;
	if (cluster_wal_threads_dir == NULL || cluster_wal_threads_dir[0] == '\0')
		return false;
	cluster_wal_thread_dir_name(file->thread_id, dirname, sizeof(dirname));
	if (dirname[0] == '\0'
		|| snprintf(dirpath, sizeof(dirpath), "%s/%s", cluster_wal_threads_dir, dirname)
			   >= (int)sizeof(dirpath)
		|| !wal_reuse_target_basename(file, basename, sizeof(basename)))
		return false;
#ifdef O_DIRECTORY
	dir_flags |= O_DIRECTORY;
#endif
#ifdef O_CLOEXEC
	dir_flags |= O_CLOEXEC;
	open_flags |= O_CLOEXEC;
#endif
#ifdef O_NOFOLLOW
	open_flags |= O_NOFOLLOW;
#endif
	dirfd = open(dirpath, dir_flags);
	if (dirfd < 0)
		return false;
	fd = openat(dirfd, basename, open_flags);
	if (fd < 0 || !wal_reuse_stamp_held_target(file, system_identifier, dirfd, fd, out_stamp)) {
		if (fd >= 0)
			close(fd);
		close(dirfd);
		return false;
	}
	*out_dir_handle = (intptr_t)dirfd;
	*out_handle = (intptr_t)fd;
	return true;
#else
	(void)file;
	(void)system_identifier;
	memset(out_stamp, 0, sizeof(*out_stamp));
	*out_dir_handle = (intptr_t)-1;
	*out_handle = (intptr_t)-1;
	return false;
#endif
}

static ClusterWalReuseGuardResult
wal_reuse_preflight_roots(const ClusterWalReuseGuardRequest *request, ClusterWalRootFold *fold,
						  ClusterControlRootSnapshot *target_root,
						  ClusterWalReuseDenyReason *out_reason)
{
	ClusterFormationSnapshotV1 formation_snapshot;
	ClusterFenceAuthorityProof authority;
	ClusterWalRootFoldInput inputs[CLUSTER_WAL_RETENTION_MAX_THREADS];
	uint16 witness_thread = 0;
	uint16 i;

	memset(&formation_snapshot, 0, sizeof(formation_snapshot));
	memset(&authority, 0, sizeof(authority));
	memset(inputs, 0, sizeof(inputs));
	if (!cluster_formation_witness_copy_classification_v1(request->formation, &witness_thread,
														  &authority, &formation_snapshot)
		|| witness_thread != request->duty.origin_thread_id
		|| cluster_formation_witness_revalidate_nowait(request->formation)
			   != CLUSTER_FORMATION_WITNESS_READY) {
		*out_reason = CLUSTER_WAL_DENY_FORMATION_STALE;
		return CLUSTER_WAL_GUARD_BLOCKED;
	}
	for (i = 0; i < CLUSTER_WAL_RETENTION_MAX_THREADS; i++) {
		ClusterMembershipState state
			= (ClusterMembershipState)formation_snapshot.membership.membership_state[i];
		ClusterControlRootIdentity discovered_identity;
		ClusterControlRootReadToken token;
		ClusterControlRootResult result;

		if (state == CLUSTER_MEMBER_ABSENT)
			continue;
		if (state < CLUSTER_MEMBER_DEAD || state > CLUSTER_MEMBER_REMOVED) {
			*out_reason = CLUSTER_WAL_DENY_FORMATION_STALE;
			return CLUSTER_WAL_GUARD_BLOCKED;
		}
		inputs[i].configured = true;
		memset(&token, 0, sizeof(token));
		if (i + 1 == request->duty.origin_thread_id) {
			result = cluster_control_root_read_canonical(i + 1, &request->duty,
														 CLUSTER_CONTROL_ROOT_READ_STRONG,
														 &inputs[i].snapshot, &token);
		} else {
			ClusterControlRootSnapshot bootstrap;

			result = cluster_control_root_read_canonical(
				i + 1, NULL, CLUSTER_CONTROL_ROOT_READ_BOOTSTRAP_VALIDATE, &bootstrap, NULL);
			if (!control_root_read_ready(result)) {
				inputs[i].read_result = result;
				continue;
			}
			discovered_identity = bootstrap.identity;
			result = cluster_control_root_read_canonical(i + 1, &discovered_identity,
														 CLUSTER_CONTROL_ROOT_READ_STRONG,
														 &inputs[i].snapshot, &token);
		}
		inputs[i].read_result = result;
		if (i + 1 == request->duty.origin_thread_id) {
			if (!control_root_read_ready(result)) {
				*out_reason = CLUSTER_WAL_DENY_ROOT_UNAVAILABLE;
				return CLUSTER_WAL_GUARD_BLOCKED;
			}
			if (memcmp(&token, &request->root_read, sizeof(token)) != 0) {
				*out_reason = CLUSTER_WAL_DENY_ROOT_STALE;
				return CLUSTER_WAL_GUARD_BLOCKED;
			}
			*target_root = inputs[i].snapshot;
		}
	}
	if (cluster_formation_witness_revalidate_nowait(request->formation)
		!= CLUSTER_FORMATION_WITNESS_READY) {
		*out_reason = CLUSTER_WAL_DENY_FORMATION_STALE;
		return CLUSTER_WAL_GUARD_BLOCKED;
	}
	if (!cluster_wal_retention_fold_validated_roots(inputs, CLUSTER_WAL_RETENTION_MAX_THREADS,
													wal_segment_size, fold)
		|| fold->result == CLUSTER_WAL_FOLD_UNKNOWN) {
		*out_reason = CLUSTER_WAL_DENY_ROOT_REQUIRED;
		return CLUSTER_WAL_GUARD_BLOCKED;
	}
	return CLUSTER_WAL_GUARD_OK;
}

ClusterWalReuseGuardResult
cluster_wal_reuse_guard_preflight(ClusterWalReuseActionGuard *guard,
								  const ClusterWalReuseGuardRequest *request,
								  PgracExternalFenceNeedSetV1 **out_needs,
								  ClusterWalReuseDenyReason *out_reason)
{
	ClusterWalFileObjectStamp target_stamp;
	ClusterWalRootFold fold;
	ClusterControlRootSnapshot target_root;
	PgracExternalFenceNeedSetV1 *needs = NULL;
	ClusterWalReuseGuardResult result;
	PgracExternalFenceDenyReason fence_reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	uint32 i;

	if (out_reason == NULL)
		return CLUSTER_WAL_GUARD_INVALID;
	*out_reason = CLUSTER_WAL_DENY_NONE;
	if (!wal_reuse_guard_valid(guard) || guard->state != CLUSTER_WAL_GUARD_EMPTY
		|| out_needs == NULL || *out_needs != NULL) {
		*out_reason = CLUSTER_WAL_DENY_GUARD_STATE;
		return CLUSTER_WAL_GUARD_INVALID;
	}
	if (!wal_reuse_request_shape_valid(request)) {
		*out_reason = CLUSTER_WAL_DENY_INVALID_IDENTITY;
		return CLUSTER_WAL_GUARD_INVALID;
	}
	if (request->source_kind != CLUSTER_WAL_INSTALL_SOURCE_NONE) {
		*out_reason = CLUSTER_WAL_DENY_INSTALL_SOURCE_UNPROVEN;
		return CLUSTER_WAL_GUARD_BLOCKED;
	}
	if (!wal_reuse_open_target(&request->file, request->duty.system_identifier, &target_stamp,
							   &guard->source_dir_handle, &guard->source_handle)) {
		*out_reason = CLUSTER_WAL_DENY_OBJECT_STALE;
		return CLUSTER_WAL_GUARD_BLOCKED;
	}
	memset(&fold, 0, sizeof(fold));
	memset(&target_root, 0, sizeof(target_root));
	result = wal_reuse_preflight_roots(request, &fold, &target_root, out_reason);
	if (result != CLUSTER_WAL_GUARD_OK)
		return result;
	for (i = 0; i < fold.nintervals; i++)
		if (cluster_wal_retention_interval_intersects_file(&fold.intervals[i], &request->file,
														   wal_segment_size)) {
			*out_reason = CLUSTER_WAL_DENY_ROOT_REQUIRED;
			return CLUSTER_WAL_GUARD_BLOCKED;
		}
	if (target_root.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED) {
		if (cluster_external_fence_need_set_build(&request->duty, request->formation, &needs)
				!= PGRAC_EXTERNAL_FENCE_NEED_SET_OK
			|| needs == NULL || cluster_external_fence_need_set_count(needs) == 0
			|| cluster_external_fence_need_set_count(needs) > CLUSTER_MAX_NODES
			|| cluster_external_fence_need_set_digest(needs) == NULL
			|| !cluster_external_fence_need_set_revalidate_nowait(needs, request->formation,
																  &fence_reason)) {
			if (needs != NULL)
				cluster_external_fence_need_set_release(&needs);
			*out_reason = CLUSTER_WAL_DENY_FENCE_UNAVAILABLE;
			return CLUSTER_WAL_GUARD_BLOCKED;
		}
	}
	guard->entry = request->entry;
	guard->action = request->action;
	guard->file = request->file;
	guard->pre_action_stamp = target_stamp;
	guard->source_kind = request->source_kind;
	guard->source_carrier = request->source_carrier;
	guard->fork_lsn = request->fork_lsn;
	guard->source_coverage_start = request->source_coverage_start;
	guard->source_coverage_end = request->source_coverage_end;
	guard->duty = request->duty;
	guard->root_read = request->root_read;
	guard->formation = request->formation;
	guard->needs_or_null = needs;
	guard->state = CLUSTER_WAL_GUARD_PREFLIGHTED;
	*out_needs = needs;
	return CLUSTER_WAL_GUARD_OK;
}

ClusterWalReuseGuardResult
cluster_wal_reuse_guard_preflight_active_recovery(ClusterWalReuseActionGuard *guard,
												  const ClusterWalFileIdentity *file,
												  ClusterWalReuseEntry entry,
												  ClusterRecoverySerialGuard **out_serial,
												  ClusterWalRetentionPin **out_pin,
												  ClusterWalReuseDenyReason *out_reason)
{
	ClusterWalPinThread *thread;
	ClusterWalFileObjectStamp target_stamp;
	uint32 i;

	if (out_reason == NULL)
		return CLUSTER_WAL_GUARD_INVALID;
	*out_reason = CLUSTER_WAL_DENY_NONE;
	if (out_serial == NULL || out_pin == NULL || *out_serial != NULL || *out_pin != NULL
		|| !wal_reuse_guard_valid(guard) || guard->state != CLUSTER_WAL_GUARD_EMPTY || file == NULL
		|| !cluster_wal_file_identity_valid(file, wal_segment_size)
		|| entry != CLUSTER_WAL_REUSE_E2_APPLY_TIMELINE_SWITCH || !pin_valid(active_pin)
		|| active_pin->poisoned
		|| (active_pin->state != CLUSTER_WAL_PIN_STATE_BOUND_ONE
			&& active_pin->state != CLUSTER_WAL_PIN_STATE_BOUND_SET)) {
		*out_reason = CLUSTER_WAL_DENY_GUARD_STATE;
		return CLUSTER_WAL_GUARD_INVALID;
	}
	thread = pin_find_thread(active_pin, file->thread_id);
	if (thread == NULL || thread->serial == NULL || thread->formation == NULL
		|| thread->needs == NULL || thread->admissions == NULL || !thread->walr.held
		|| thread->walr.release_uncertain
		|| !root_token_matches_recovery_duty(&thread->root_read, &thread->duty)
		|| cluster_wal_retention_pin_revalidate(active_pin) != CLUSTER_WAL_PIN_OK) {
		*out_reason = CLUSTER_WAL_DENY_SERIAL_STALE;
		return CLUSTER_WAL_GUARD_BLOCKED;
	}
	for (i = 0; i < thread->nintervals; i++)
		if (cluster_wal_retention_interval_intersects_file(&thread->intervals[i], file,
														   wal_segment_size)) {
			*out_reason = CLUSTER_WAL_DENY_ROOT_REQUIRED;
			return CLUSTER_WAL_GUARD_BLOCKED;
		}
	if (!wal_reuse_open_target(file, thread->duty.system_identifier, &target_stamp,
							   &guard->source_dir_handle, &guard->source_handle)) {
		*out_reason = CLUSTER_WAL_DENY_OBJECT_STALE;
		return CLUSTER_WAL_GUARD_BLOCKED;
	}
	guard->entry = entry;
	guard->action = CLUSTER_WAL_ACTION_RETIRE_RECYCLE_OR_REMOVE;
	guard->file = *file;
	guard->pre_action_stamp = target_stamp;
	guard->source_kind = CLUSTER_WAL_INSTALL_SOURCE_NONE;
	guard->duty = thread->duty;
	guard->root_read = thread->root_read;
	guard->formation = thread->formation;
	guard->needs_or_null = thread->needs;
	guard->admissions_or_null = thread->admissions;
	guard->state = CLUSTER_WAL_GUARD_FENCE_ADMITTED_OR_NA;
	*out_serial = thread->serial;
	*out_pin = active_pin;
	return CLUSTER_WAL_GUARD_OK;
}

ClusterWalReuseGuardResult
cluster_wal_reuse_guard_fence_admitted_nowait(
	ClusterWalReuseActionGuard *guard, const PgracExternalFenceAdmissionSetV1 *admissions_or_null,
	ClusterWalReuseDenyReason *out_reason)
{
	const PgracExternalFenceWriterSetDigest *need_digest;
	const PgracExternalFenceWriterSetDigest *admission_digest;
	PgracExternalFenceDenyReason fence_reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	uint32 need_count;
	uint32 admission_count;

	if (out_reason == NULL)
		return CLUSTER_WAL_GUARD_INVALID;
	*out_reason = CLUSTER_WAL_DENY_NONE;
	if (!wal_reuse_guard_valid(guard) || guard->state != CLUSTER_WAL_GUARD_PREFLIGHTED
		|| guard->formation == NULL) {
		*out_reason = CLUSTER_WAL_DENY_GUARD_STATE;
		return CLUSTER_WAL_GUARD_INVALID;
	}
	if (cluster_formation_witness_revalidate_nowait(guard->formation)
		!= CLUSTER_FORMATION_WITNESS_READY) {
		*out_reason = CLUSTER_WAL_DENY_FORMATION_STALE;
		return CLUSTER_WAL_GUARD_BLOCKED;
	}
	if (guard->needs_or_null == NULL) {
		if (admissions_or_null != NULL) {
			*out_reason = CLUSTER_WAL_DENY_FENCE_UNAVAILABLE;
			return CLUSTER_WAL_GUARD_BLOCKED;
		}
		guard->state = CLUSTER_WAL_GUARD_FENCE_ADMITTED_OR_NA;
		return CLUSTER_WAL_GUARD_OK;
	}
	if (admissions_or_null == NULL) {
		*out_reason = CLUSTER_WAL_DENY_FENCE_UNAVAILABLE;
		return CLUSTER_WAL_GUARD_BLOCKED;
	}
	need_count = cluster_external_fence_need_set_count(guard->needs_or_null);
	admission_count = cluster_external_fence_admission_set_count(admissions_or_null);
	need_digest = cluster_external_fence_need_set_digest(guard->needs_or_null);
	admission_digest = cluster_external_fence_admission_set_digest(admissions_or_null);
	if (need_count == 0 || need_count > CLUSTER_MAX_NODES || admission_count != need_count
		|| need_digest == NULL || admission_digest == NULL
		|| memcmp(need_digest, admission_digest, sizeof(*need_digest)) != 0) {
		*out_reason = CLUSTER_WAL_DENY_FENCE_UNAVAILABLE;
		return CLUSTER_WAL_GUARD_BLOCKED;
	}
	if (!cluster_external_fence_need_set_revalidate_nowait(guard->needs_or_null, guard->formation,
														   &fence_reason)
		|| !cluster_external_fence_revalidate_set_nowait(admissions_or_null, guard->needs_or_null,
														 guard->formation, &fence_reason)) {
		*out_reason = CLUSTER_WAL_DENY_FENCE_STALE;
		return CLUSTER_WAL_GUARD_BLOCKED;
	}
	guard->admissions_or_null = admissions_or_null;
	guard->state = CLUSTER_WAL_GUARD_FENCE_ADMITTED_OR_NA;
	return CLUSTER_WAL_GUARD_OK;
}

static uint64
wal_retention_monotonic_us(void)
{
	instr_time now;
	int64 ns;

	INSTR_TIME_SET_CURRENT(now);
	ns = INSTR_TIME_GET_NANOSEC(now);
	return ns <= 0 ? 0 : (uint64)ns / UINT64_C(1000);
}

static bool
wal_retention_e1_context_valid(const ClusterWalRetentionE1Context *context)
{
	return context != NULL && context->magic == CLUSTER_WAL_E1_CONTEXT_MAGIC
		   && context->thread_id > 0 && context->thread_id <= CLUSTER_WAL_RETENTION_MAX_THREADS
		   && context->reserved == 0 && context->owner_pid == MyProcPid
		   && context->owner == CurrentResourceOwner && context->formation != NULL;
}

static bool
wal_retention_e1_read_root(ClusterWalRetentionE1Context *context, bool discover_identity,
						   ClusterControlRootSnapshot *out_snapshot,
						   ClusterControlRootReadToken *out_token)
{
	ClusterControlRootIdentity identity;
	ClusterControlRootSnapshot bootstrap;
	ClusterControlRootResult result;

	if (discover_identity) {
		result = cluster_control_root_read_canonical(context->thread_id, NULL,
													 CLUSTER_CONTROL_ROOT_READ_BOOTSTRAP_VALIDATE,
													 &bootstrap, NULL);
		if (!control_root_read_ready(result))
			return false;
		identity = bootstrap.identity;
	} else
		identity = context->duty;
	result = cluster_control_root_read_canonical(
		context->thread_id, &identity, CLUSTER_CONTROL_ROOT_READ_STRONG, out_snapshot, out_token);
	if (!control_root_read_ready(result)
		|| memcmp(&out_snapshot->identity, &identity, sizeof(identity)) != 0
		|| !root_token_matches_identity(out_token, &identity))
		return false;
	context->duty = identity;
	context->root_read = *out_token;
	return true;
}

ClusterWalReuseGuardResult
cluster_wal_retention_e1_coarse_begin(ClusterWalRetentionE1Context *context, uint16 thread_id,
									  ClusterWalRootFoldResult *out_fold_result,
									  XLogSegNo *out_floor_segno,
									  ClusterWalReuseDenyReason *out_reason)
{
	static const ClusterWalRetentionE1Context zero_context;
	ClusterWalReuseGuardRequest request;
	ClusterWalRootFold fold;
	ClusterControlRootSnapshot own_root;
	ClusterControlRootSnapshot target_root;
	ClusterControlRootReadToken own_token;
	ClusterLockAcquireRequest lock_request;
	ClusterLockAcquireResult lock_result;
	ClusterWalReuseGuardResult result;
	ClusterWalReuseDenyReason saved_reason;
	ClusterFormationWitnessResult formation_result;

	if (out_fold_result != NULL)
		*out_fold_result = CLUSTER_WAL_FOLD_UNKNOWN;
	if (out_floor_segno != NULL)
		*out_floor_segno = 0;
	if (out_reason == NULL)
		return CLUSTER_WAL_GUARD_INVALID;
	*out_reason = CLUSTER_WAL_DENY_NONE;
	if (context == NULL || out_fold_result == NULL || out_floor_segno == NULL
		|| CurrentResourceOwner == NULL || thread_id == 0
		|| thread_id > CLUSTER_WAL_RETENTION_MAX_THREADS
		|| memcmp(context, &zero_context, sizeof(*context)) != 0 || active_e1_context != NULL
		|| active_action_context != NULL || active_reuse_guard != NULL || active_pin != NULL
		|| active_root_publish_guard != NULL) {
		*out_reason = CLUSTER_WAL_DENY_GUARD_STATE;
		return CLUSTER_WAL_GUARD_INVALID;
	}
	context->magic = CLUSTER_WAL_E1_CONTEXT_MAGIC;
	context->thread_id = thread_id;
	context->owner_pid = MyProcPid;
	context->owner = CurrentResourceOwner;
	formation_result = cluster_formation_witness_build_live_wait(
		thread_id, CLUSTER_WAL_REUSE_FENCE_TIMEOUT_MS, &context->formation);
	if (formation_result != CLUSTER_FORMATION_WITNESS_READY) {
		*out_reason = CLUSTER_WAL_DENY_FORMATION_STALE;
		explicit_bzero(context, sizeof(*context));
		return CLUSTER_WAL_GUARD_BLOCKED;
	}
	memset(&own_root, 0, sizeof(own_root));
	memset(&own_token, 0, sizeof(own_token));
	if (!wal_retention_e1_read_root(context, true, &own_root, &own_token)) {
		cluster_formation_witness_destroy(&context->formation);
		explicit_bzero(context, sizeof(*context));
		*out_reason = CLUSTER_WAL_DENY_ROOT_UNAVAILABLE;
		return CLUSTER_WAL_GUARD_BLOCKED;
	}

	memset(&lock_request, 0, sizeof(lock_request));
	if (!cluster_wal_retention_resid_encode(thread_id, &lock_request.resid)) {
		cluster_formation_witness_destroy(&context->formation);
		explicit_bzero(context, sizeof(*context));
		*out_reason = CLUSTER_WAL_DENY_INVALID_IDENTITY;
		return CLUSTER_WAL_GUARD_INVALID;
	}
	lock_request.lockmode = ExclusiveLock;
	lock_request.op = CLUSTER_LOCK_OP_REQUEST;
	lock_request.current_mode = NoLock;
	lock_request.lockmethod_id = DEFAULT_LOCKMETHOD;
	lock_request.dontwait = true;
	lock_request.sessionLock = false;
	lock_request.caller_local_start_ts_ms = (uint64)(GetCurrentTimestamp() / 1000);
	lock_request.timeout_ms = 1;
	lock_request.wait_event = WAIT_EVENT_CLUSTER_GES_REPLY_WAIT;
	walr_resource_ensure_callback();
	lock_result = walr_request_acquire_actual(&lock_request);
	if (lock_result != CLUSTER_LOCK_ACQUIRE_OK_GRANTED) {
		cluster_formation_witness_destroy(&context->formation);
		explicit_bzero(context, sizeof(*context));
		*out_reason = CLUSTER_WAL_DENY_GES_UNAVAILABLE;
		return CLUSTER_WAL_GUARD_BLOCKED;
	}
	context->coarse_walr.resid = lock_request.resid;
	context->coarse_walr.mode = ExclusiveLock;
	context->coarse_walr.held = true;
	context->coarse_walr.coordinated = true;
	context->coarse_walr.acquire_request = lock_request;
	context->coarse_walr.owner = CurrentResourceOwner;
	context->state = CLUSTER_WAL_E1_STATE_COARSE_HELD;
	active_e1_context = context;

	memset(&request, 0, sizeof(request));
	request.duty = context->duty;
	request.root_read = context->root_read;
	request.formation = context->formation;
	memset(&fold, 0, sizeof(fold));
	memset(&target_root, 0, sizeof(target_root));
	result = wal_reuse_preflight_roots(&request, &fold, &target_root, out_reason);
	if (result == CLUSTER_WAL_GUARD_OK && fold.result == CLUSTER_WAL_FOLD_BOUNDED
		&& fold.floor_by_thread[thread_id - 1] == 0) {
		*out_reason = CLUSTER_WAL_DENY_ROOT_REQUIRED;
		result = CLUSTER_WAL_GUARD_BLOCKED;
	}
	if (result != CLUSTER_WAL_GUARD_OK) {
		saved_reason = *out_reason;
		if (cluster_wal_retention_e1_coarse_release(context, out_reason)
			!= CLUSTER_WALR_RELEASE_CONFIRMED) {
			*out_reason = CLUSTER_WAL_DENY_RELEASE_UNCERTAIN;
			return CLUSTER_WAL_GUARD_RELEASE_UNCERTAIN;
		}
		cluster_wal_retention_e1_finish(context);
		*out_reason = saved_reason;
		return result;
	}
	*out_fold_result = fold.result;
	if (fold.result == CLUSTER_WAL_FOLD_BOUNDED)
		*out_floor_segno = fold.floor_by_thread[thread_id - 1];
	return CLUSTER_WAL_GUARD_OK;
}

ClusterWalrReleaseResult
cluster_wal_retention_e1_coarse_release(ClusterWalRetentionE1Context *context,
										ClusterWalReuseDenyReason *out_reason)
{
	ClusterLockAcquireResult lock_result;
	uint64 now_us;

	if (out_reason == NULL)
		return CLUSTER_WALR_RELEASE_INVALID;
	*out_reason = CLUSTER_WAL_DENY_NONE;
	if (!wal_retention_e1_context_valid(context)
		|| context->state != CLUSTER_WAL_E1_STATE_COARSE_HELD || active_e1_context != context
		|| !context->coarse_walr.held) {
		*out_reason = CLUSTER_WAL_DENY_GUARD_STATE;
		return CLUSTER_WALR_RELEASE_INVALID;
	}
	lock_result = walr_request_release_actual(&context->coarse_walr.acquire_request);
	if (lock_result != CLUSTER_LOCK_ACQUIRE_OK_GRANTED) {
		context->coarse_walr.release_uncertain = true;
		*out_reason = CLUSTER_WAL_DENY_RELEASE_UNCERTAIN;
		return CLUSTER_WALR_RELEASE_UNCONFIRMED;
	}
	context->coarse_walr.held = false;
	context->coarse_walr.coordinated = false;
	context->coarse_walr.mode = NoLock;
	memset(&context->coarse_walr.acquire_request, 0, sizeof(context->coarse_walr.acquire_request));
	active_e1_context = NULL;
	context->state = CLUSTER_WAL_E1_STATE_FILES;
	now_us = wal_retention_monotonic_us();
	if (now_us == 0
		|| now_us > UINT64_MAX - (uint64)CLUSTER_WAL_REUSE_FENCE_TIMEOUT_MS * UINT64_C(1000)) {
		*out_reason = CLUSTER_WAL_DENY_FENCE_UNAVAILABLE;
		return CLUSTER_WALR_RELEASE_INVALID;
	}
	context->provider_deadline_us
		= now_us + (uint64)CLUSTER_WAL_REUSE_FENCE_TIMEOUT_MS * UINT64_C(1000);
	return CLUSTER_WALR_RELEASE_CONFIRMED;
}

bool
cluster_wal_retention_active_pin_present(void)
{
	return active_pin != NULL;
}

ClusterWalReuseGuardResult
cluster_wal_retention_action_begin(ClusterWalRetentionE1Context *context, uint16 thread_id,
								   ClusterWalReuseDenyReason *out_reason)
{
	static const ClusterWalRetentionE1Context zero_context;
	ClusterControlRootSnapshot own_root;
	ClusterControlRootReadToken own_token;
	ClusterFormationWitnessResult formation_result;
	uint64 now_us;

	if (out_reason == NULL)
		return CLUSTER_WAL_GUARD_INVALID;
	*out_reason = CLUSTER_WAL_DENY_NONE;
	if (context == NULL || CurrentResourceOwner == NULL || thread_id == 0
		|| thread_id > CLUSTER_WAL_RETENTION_MAX_THREADS
		|| memcmp(context, &zero_context, sizeof(*context)) != 0 || active_action_context != NULL
		|| active_e1_context != NULL || active_reuse_guard != NULL || active_pin != NULL
		|| active_root_publish_guard != NULL) {
		*out_reason = CLUSTER_WAL_DENY_GUARD_STATE;
		return CLUSTER_WAL_GUARD_INVALID;
	}
	context->magic = CLUSTER_WAL_E1_CONTEXT_MAGIC;
	context->thread_id = thread_id;
	context->state = CLUSTER_WAL_E1_STATE_FILES;
	context->owner_pid = MyProcPid;
	context->owner = CurrentResourceOwner;
	formation_result = cluster_formation_witness_build_live_wait(
		thread_id, CLUSTER_WAL_REUSE_FENCE_TIMEOUT_MS, &context->formation);
	if (formation_result != CLUSTER_FORMATION_WITNESS_READY) {
		*out_reason = CLUSTER_WAL_DENY_FORMATION_STALE;
		explicit_bzero(context, sizeof(*context));
		return CLUSTER_WAL_GUARD_BLOCKED;
	}
	memset(&own_root, 0, sizeof(own_root));
	memset(&own_token, 0, sizeof(own_token));
	if (!wal_retention_e1_read_root(context, true, &own_root, &own_token)) {
		cluster_formation_witness_destroy(&context->formation);
		explicit_bzero(context, sizeof(*context));
		*out_reason = CLUSTER_WAL_DENY_ROOT_UNAVAILABLE;
		return CLUSTER_WAL_GUARD_BLOCKED;
	}
	now_us = wal_retention_monotonic_us();
	if (now_us == 0
		|| now_us > UINT64_MAX - (uint64)CLUSTER_WAL_REUSE_FENCE_TIMEOUT_MS * UINT64_C(1000)) {
		cluster_formation_witness_destroy(&context->formation);
		explicit_bzero(context, sizeof(*context));
		*out_reason = CLUSTER_WAL_DENY_FENCE_UNAVAILABLE;
		return CLUSTER_WAL_GUARD_BLOCKED;
	}
	context->provider_deadline_us
		= now_us + (uint64)CLUSTER_WAL_REUSE_FENCE_TIMEOUT_MS * UINT64_C(1000);
	walr_resource_ensure_callback();
	active_action_context = context;
	return CLUSTER_WAL_GUARD_OK;
}

ClusterWalReuseGuardResult
cluster_wal_retention_action_preflight(ClusterWalRetentionE1Context *context,
									   const ClusterWalFileIdentity *file,
									   ClusterWalReuseEntry entry,
									   ClusterWalReuseActionGuard *guard,
									   PgracExternalFenceNeedSetV1 **out_needs,
									   ClusterWalReuseDenyReason *out_reason)
{
	ClusterControlRootSnapshot own_root;
	ClusterControlRootReadToken own_token;
	ClusterWalReuseGuardRequest request;

	if (out_reason == NULL)
		return CLUSTER_WAL_GUARD_INVALID;
	*out_reason = CLUSTER_WAL_DENY_NONE;
	if (!wal_retention_e1_context_valid(context) || active_action_context != context
		|| context->state != CLUSTER_WAL_E1_STATE_FILES || context->coarse_walr.held || file == NULL
		|| guard == NULL || entry != CLUSTER_WAL_REUSE_E2_APPLY_TIMELINE_SWITCH || out_needs == NULL
		|| *out_needs != NULL) {
		*out_reason = CLUSTER_WAL_DENY_GUARD_STATE;
		return CLUSTER_WAL_GUARD_INVALID;
	}
	memset(&own_root, 0, sizeof(own_root));
	memset(&own_token, 0, sizeof(own_token));
	if (!wal_retention_e1_read_root(context, false, &own_root, &own_token)) {
		*out_reason = CLUSTER_WAL_DENY_ROOT_STALE;
		return CLUSTER_WAL_GUARD_BLOCKED;
	}
	memset(&request, 0, sizeof(request));
	request.file = *file;
	request.entry = entry;
	request.action = CLUSTER_WAL_ACTION_RETIRE_RECYCLE_OR_REMOVE;
	request.source_kind = CLUSTER_WAL_INSTALL_SOURCE_NONE;
	request.duty = context->duty;
	request.root_read = context->root_read;
	request.formation = context->formation;
	return cluster_wal_reuse_guard_preflight(guard, &request, out_needs, out_reason);
}

void
cluster_wal_retention_action_finish(ClusterWalRetentionE1Context *context)
{
	if (context == NULL || context->magic == 0)
		return;
	if (!wal_retention_e1_context_valid(context) || active_action_context != context
		|| context->coarse_walr.held)
		return;
	cluster_formation_witness_destroy(&context->formation);
	explicit_bzero(context, sizeof(*context));
	active_action_context = NULL;
}

ClusterWalReuseGuardResult
cluster_wal_retention_e1_preflight(ClusterWalRetentionE1Context *context,
								   const ClusterWalFileIdentity *file,
								   ClusterWalReuseActionGuard *guard,
								   PgracExternalFenceNeedSetV1 **out_needs,
								   ClusterWalReuseDenyReason *out_reason)
{
	ClusterControlRootSnapshot own_root;
	ClusterControlRootReadToken own_token;
	ClusterWalReuseGuardRequest request;

	if (out_reason == NULL)
		return CLUSTER_WAL_GUARD_INVALID;
	*out_reason = CLUSTER_WAL_DENY_NONE;
	if (!wal_retention_e1_context_valid(context) || context->state != CLUSTER_WAL_E1_STATE_FILES
		|| context->coarse_walr.held || file == NULL || guard == NULL || out_needs == NULL
		|| *out_needs != NULL) {
		*out_reason = CLUSTER_WAL_DENY_GUARD_STATE;
		return CLUSTER_WAL_GUARD_INVALID;
	}
	memset(&own_root, 0, sizeof(own_root));
	memset(&own_token, 0, sizeof(own_token));
	if (!wal_retention_e1_read_root(context, false, &own_root, &own_token)) {
		*out_reason = CLUSTER_WAL_DENY_ROOT_STALE;
		return CLUSTER_WAL_GUARD_BLOCKED;
	}
	memset(&request, 0, sizeof(request));
	request.file = *file;
	request.entry = CLUSTER_WAL_REUSE_E1_CHECKPOINT_RESTARTPOINT;
	request.action = CLUSTER_WAL_ACTION_RETIRE_RECYCLE_OR_REMOVE;
	request.source_kind = CLUSTER_WAL_INSTALL_SOURCE_NONE;
	request.duty = context->duty;
	request.root_read = context->root_read;
	request.formation = context->formation;
	return cluster_wal_reuse_guard_preflight(guard, &request, out_needs, out_reason);
}

ClusterWalReuseGuardResult
cluster_wal_retention_e1_fence_wait(ClusterWalRetentionE1Context *context,
									ClusterWalReuseActionGuard *guard,
									PgracExternalFenceNeedSetV1 *needs,
									PgracExternalFenceAdmissionSetV1 **out_admissions,
									ClusterWalReuseDenyReason *out_reason)
{
	PgracExternalFenceVerdict verdict;
	uint64 now_us;
	uint64 remaining_us;
	int timeout_ms;

	if (out_reason == NULL)
		return CLUSTER_WAL_GUARD_INVALID;
	*out_reason = CLUSTER_WAL_DENY_NONE;
	if (!wal_retention_e1_context_valid(context) || context->state != CLUSTER_WAL_E1_STATE_FILES
		|| guard == NULL || out_admissions == NULL || *out_admissions != NULL
		|| guard->needs_or_null != needs) {
		*out_reason = CLUSTER_WAL_DENY_GUARD_STATE;
		return CLUSTER_WAL_GUARD_INVALID;
	}
	if (needs == NULL)
		return cluster_wal_reuse_guard_fence_admitted_nowait(guard, NULL, out_reason);
	now_us = wal_retention_monotonic_us();
	if (now_us == 0 || now_us >= context->provider_deadline_us) {
		*out_reason = CLUSTER_WAL_DENY_FENCE_UNAVAILABLE;
		return CLUSTER_WAL_GUARD_BLOCKED;
	}
	remaining_us = context->provider_deadline_us - now_us;
	timeout_ms = (int)Min((remaining_us + UINT64_C(999)) / UINT64_C(1000),
						  (uint64)CLUSTER_WAL_REUSE_FENCE_TIMEOUT_MS);
	if (timeout_ms < 1)
		timeout_ms = 1;
	verdict = cluster_external_fence_admit_set_wait(needs, context->formation, timeout_ms,
													out_admissions);
	if (verdict != PGRAC_EXTERNAL_FENCE_WRITE_EXCLUDED || *out_admissions == NULL) {
		*out_reason = CLUSTER_WAL_DENY_FENCE_UNAVAILABLE;
		return CLUSTER_WAL_GUARD_BLOCKED;
	}
	return cluster_wal_reuse_guard_fence_admitted_nowait(guard, *out_admissions, out_reason);
}

void
cluster_wal_retention_e1_finish(ClusterWalRetentionE1Context *context)
{
	ClusterWalReuseDenyReason reason;

	if (context == NULL || context->magic == 0)
		return;
	if (!wal_retention_e1_context_valid(context))
		return;
	if (context->coarse_walr.held
		&& cluster_wal_retention_e1_coarse_release(context, &reason)
			   != CLUSTER_WALR_RELEASE_CONFIRMED)
		elog(FATAL, "could not confirm coarse WAL-retention release");
	cluster_formation_witness_destroy(&context->formation);
	explicit_bzero(context, sizeof(*context));
}

static bool
wal_reuse_guard_fence_current(const ClusterWalReuseActionGuard *guard,
							  ClusterWalReuseDenyReason *out_reason)
{
	const PgracExternalFenceWriterSetDigest *need_digest;
	const PgracExternalFenceWriterSetDigest *admission_digest;
	PgracExternalFenceDenyReason fence_reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	uint32 need_count;
	uint32 admission_count;

	if (cluster_formation_witness_revalidate_nowait(guard->formation)
		!= CLUSTER_FORMATION_WITNESS_READY) {
		*out_reason = CLUSTER_WAL_DENY_FORMATION_STALE;
		return false;
	}
	if (guard->needs_or_null == NULL) {
		if (guard->admissions_or_null != NULL) {
			*out_reason = CLUSTER_WAL_DENY_FENCE_UNAVAILABLE;
			return false;
		}
		return true;
	}
	if (guard->admissions_or_null == NULL) {
		*out_reason = CLUSTER_WAL_DENY_FENCE_UNAVAILABLE;
		return false;
	}
	need_count = cluster_external_fence_need_set_count(guard->needs_or_null);
	admission_count = cluster_external_fence_admission_set_count(guard->admissions_or_null);
	need_digest = cluster_external_fence_need_set_digest(guard->needs_or_null);
	admission_digest = cluster_external_fence_admission_set_digest(guard->admissions_or_null);
	if (need_count == 0 || need_count > CLUSTER_MAX_NODES || admission_count != need_count
		|| need_digest == NULL || admission_digest == NULL
		|| memcmp(need_digest, admission_digest, sizeof(*need_digest)) != 0) {
		*out_reason = CLUSTER_WAL_DENY_FENCE_UNAVAILABLE;
		return false;
	}
	if (!cluster_external_fence_need_set_revalidate_nowait(guard->needs_or_null, guard->formation,
														   &fence_reason)
		|| !cluster_external_fence_revalidate_set_nowait(
			guard->admissions_or_null, guard->needs_or_null, guard->formation, &fence_reason)) {
		*out_reason = CLUSTER_WAL_DENY_FENCE_STALE;
		return false;
	}
	return true;
}

static ClusterWalReuseGuardResult
wal_reuse_guard_rollback_x(ClusterWalReuseActionGuard *guard, ClusterWalReuseGuardResult result,
						   ClusterWalReuseDenyReason reason, ClusterWalReuseDenyReason *out_reason)
{
	ClusterLockAcquireResult lock_result;

	lock_result = walr_request_release_actual(&guard->walr.acquire_request);
	if (lock_result != CLUSTER_LOCK_ACQUIRE_OK_GRANTED) {
		guard->walr.release_uncertain = true;
		*out_reason = CLUSTER_WAL_DENY_RELEASE_UNCERTAIN;
		return CLUSTER_WAL_GUARD_RELEASE_UNCERTAIN;
	}
	guard->walr.held = false;
	guard->walr.coordinated = false;
	memset(&guard->walr.acquire_request, 0, sizeof(guard->walr.acquire_request));
	memset(&guard->walr.resid, 0, sizeof(guard->walr.resid));
	guard->walr.mode = NoLock;
	guard->state = CLUSTER_WAL_GUARD_FENCE_ADMITTED_OR_NA;
	active_reuse_guard = NULL;
	*out_reason = reason;
	return result;
}

ClusterWalReuseGuardResult
cluster_wal_reuse_guard_arm(ClusterWalReuseActionGuard *guard,
							ClusterRecoverySerialGuard *held_serial_or_null,
							ClusterWalRetentionPin *held_pin_or_null,
							ClusterWalReuseDenyReason *out_reason)
{
	ClusterWalReuseGuardRequest request;
	ClusterWalFileObjectStamp current_stamp;
	ClusterWalRootFold fold;
	ClusterControlRootSnapshot target_root;
	ClusterLockAcquireRequest lock_request;
	ClusterLockAcquireResult lock_result;
	ClusterWalReuseGuardResult result;
	uint32 i;

	if (out_reason == NULL)
		return CLUSTER_WAL_GUARD_INVALID;
	*out_reason = CLUSTER_WAL_DENY_NONE;
	if (!wal_reuse_guard_valid(guard) || guard->state != CLUSTER_WAL_GUARD_FENCE_ADMITTED_OR_NA
		|| active_reuse_guard != NULL) {
		*out_reason = CLUSTER_WAL_DENY_GUARD_STATE;
		return CLUSTER_WAL_GUARD_INVALID;
	}
	if ((held_serial_or_null == NULL) != (held_pin_or_null == NULL)) {
		*out_reason = CLUSTER_WAL_DENY_INVALID_IDENTITY;
		return CLUSTER_WAL_GUARD_INVALID;
	}
	if (held_serial_or_null != NULL) {
		ClusterWalPinThread *thread;
		ClusterResId expected_resid;
		bool cleanup_required = false;

		thread = pin_find_thread(held_pin_or_null, guard->duty.origin_thread_id);
		if (thread == NULL || held_pin_or_null->poisoned
			|| (held_pin_or_null->state != CLUSTER_WAL_PIN_STATE_BOUND_ONE
				&& held_pin_or_null->state != CLUSTER_WAL_PIN_STATE_BOUND_SET)
			|| active_root_publish_guard != NULL || thread->serial != held_serial_or_null
			|| memcmp(&thread->duty, &guard->duty, sizeof(guard->duty)) != 0
			|| memcmp(&thread->root_read, &guard->root_read, sizeof(guard->root_read)) != 0
			|| thread->formation != guard->formation || thread->needs != guard->needs_or_null
			|| thread->admissions != guard->admissions_or_null
			|| !serial_matches_thread(held_serial_or_null, thread)
			|| cluster_wal_retention_pin_revalidate(held_pin_or_null) != CLUSTER_WAL_PIN_OK
			|| !thread->walr.held || thread->walr.release_uncertain
			|| thread->walr.request.lockmode != ShareLock
			|| thread->walr.request.holder.request_id != thread->walr.request.request_id
			|| !walr_request_has_native_lock(&thread->walr.request)
			|| !cluster_wal_retention_resid_encode(guard->duty.origin_thread_id, &expected_resid)
			|| memcmp(&thread->walr.request.resid, &expected_resid, sizeof(expected_resid)) != 0) {
			*out_reason = CLUSTER_WAL_DENY_INVALID_IDENTITY;
			return CLUSTER_WAL_GUARD_BLOCKED;
		}

		lock_request = thread->walr.request;
		lock_request.lockmode = ExclusiveLock;
		lock_request.op = CLUSTER_LOCK_OP_CONVERT;
		lock_request.current_mode = ShareLock;
		lock_request.convert_old_request_id = thread->walr.request.request_id;
		lock_request.dontwait = true;
		lock_request.timeout_ms = 1;
		lock_request.wait_event = WAIT_EVENT_CLUSTER_GES_REPLY_WAIT;
		walr_resource_ensure_callback();
		lock_result = walr_request_convert_actual(&lock_request, 0, &cleanup_required);
		if (lock_result != CLUSTER_LOCK_ACQUIRE_OK_CONVERTED) {
			if (cleanup_required) {
				guard->serial_or_null = held_serial_or_null;
				guard->pin_or_null = held_pin_or_null;
				guard->walr.resid = lock_request.resid;
				guard->walr.mode = ExclusiveLock;
				guard->walr.held = true;
				guard->walr.coordinated = true;
				guard->walr.converted_from_pin = true;
				guard->walr.release_uncertain = true;
				guard->walr.acquire_request = lock_request;
				guard->walr.owner = CurrentResourceOwner;
				guard->state = CLUSTER_WAL_GUARD_WALR_X_HELD;
				held_pin_or_null->poisoned = true;
				active_reuse_guard = guard;
				*out_reason = CLUSTER_WAL_DENY_RELEASE_UNCERTAIN;
				return CLUSTER_WAL_GUARD_RELEASE_UNCERTAIN;
			}
			if (lock_result != CLUSTER_LOCK_ACQUIRE_NOT_AVAIL)
				held_pin_or_null->poisoned = true;
			*out_reason = CLUSTER_WAL_DENY_GES_UNAVAILABLE;
			return CLUSTER_WAL_GUARD_BLOCKED;
		}

		guard->serial_or_null = held_serial_or_null;
		guard->pin_or_null = held_pin_or_null;
		guard->walr.resid = lock_request.resid;
		guard->walr.mode = ExclusiveLock;
		guard->walr.held = true;
		guard->walr.coordinated = true;
		guard->walr.converted_from_pin = true;
		guard->walr.acquire_request = lock_request;
		guard->walr.owner = CurrentResourceOwner;
		guard->state = CLUSTER_WAL_GUARD_ARMED;
		active_reuse_guard = guard;
		return CLUSTER_WAL_GUARD_OK;
	}
	if (active_pin != NULL || active_root_publish_guard != NULL) {
		*out_reason = CLUSTER_WAL_DENY_PINNED;
		return CLUSTER_WAL_GUARD_BLOCKED;
	}
	memset(&lock_request, 0, sizeof(lock_request));
	if (!cluster_wal_retention_resid_encode(guard->file.thread_id, &lock_request.resid)) {
		*out_reason = CLUSTER_WAL_DENY_INVALID_IDENTITY;
		return CLUSTER_WAL_GUARD_INVALID;
	}
	lock_request.lockmode = ExclusiveLock;
	lock_request.op = CLUSTER_LOCK_OP_REQUEST;
	lock_request.current_mode = NoLock;
	lock_request.lockmethod_id = DEFAULT_LOCKMETHOD;
	lock_request.dontwait = true;
	lock_request.sessionLock = false;
	lock_request.caller_local_start_ts_ms = (uint64)(GetCurrentTimestamp() / 1000);
	lock_request.timeout_ms = 1;
	lock_request.wait_event = WAIT_EVENT_CLUSTER_GES_REPLY_WAIT;
	walr_resource_ensure_callback();
	lock_result = walr_request_acquire_actual(&lock_request);
	if (lock_result != CLUSTER_LOCK_ACQUIRE_OK_GRANTED) {
		*out_reason = CLUSTER_WAL_DENY_GES_UNAVAILABLE;
		return CLUSTER_WAL_GUARD_BLOCKED;
	}
	guard->walr.resid = lock_request.resid;
	guard->walr.mode = ExclusiveLock;
	guard->walr.held = true;
	guard->walr.coordinated = true;
	guard->walr.acquire_request = lock_request;
	guard->walr.owner = CurrentResourceOwner;
	guard->state = CLUSTER_WAL_GUARD_WALR_X_HELD;
	active_reuse_guard = guard;

	if (!wal_reuse_stamp_held_target(&guard->file, guard->duty.system_identifier,
									 (int)guard->source_dir_handle, (int)guard->source_handle,
									 &current_stamp)
		|| memcmp(&current_stamp, &guard->pre_action_stamp, sizeof(current_stamp)) != 0)
		return wal_reuse_guard_rollback_x(guard, CLUSTER_WAL_GUARD_BLOCKED,
										  CLUSTER_WAL_DENY_OBJECT_STALE, out_reason);
	memset(&request, 0, sizeof(request));
	request.file = guard->file;
	request.entry = guard->entry;
	request.action = guard->action;
	request.source_kind = guard->source_kind;
	request.source_carrier = guard->source_carrier;
	request.fork_lsn = guard->fork_lsn;
	request.source_coverage_start = guard->source_coverage_start;
	request.source_coverage_end = guard->source_coverage_end;
	request.duty = guard->duty;
	request.root_read = guard->root_read;
	request.formation = guard->formation;
	memset(&fold, 0, sizeof(fold));
	memset(&target_root, 0, sizeof(target_root));
	result = wal_reuse_preflight_roots(&request, &fold, &target_root, out_reason);
	if (result != CLUSTER_WAL_GUARD_OK)
		return wal_reuse_guard_rollback_x(guard, result, *out_reason, out_reason);
	for (i = 0; i < fold.nintervals; i++)
		if (cluster_wal_retention_interval_intersects_file(&fold.intervals[i], &guard->file,
														   wal_segment_size))
			return wal_reuse_guard_rollback_x(guard, CLUSTER_WAL_GUARD_BLOCKED,
											  CLUSTER_WAL_DENY_ROOT_REQUIRED, out_reason);
	if (!wal_reuse_guard_fence_current(guard, out_reason))
		return wal_reuse_guard_rollback_x(guard, CLUSTER_WAL_GUARD_BLOCKED, *out_reason,
										  out_reason);
	guard->state = CLUSTER_WAL_GUARD_ARMED;
	return CLUSTER_WAL_GUARD_OK;
}

static bool
wal_reuse_primary_physical_legal(const ClusterWalReuseActionGuard *guard,
								 ClusterWalReusePhysicalAction physical)
{
	switch (guard->action) {
	case CLUSTER_WAL_ACTION_RETIRE_RECYCLE_OR_REMOVE:
		return physical == CLUSTER_WAL_PHYSICAL_RECYCLE || physical == CLUSTER_WAL_PHYSICAL_REMOVE;
	case CLUSTER_WAL_ACTION_FORCE_REPLACE:
	case CLUSTER_WAL_ACTION_ARCHIVE_REPLACE:
		return physical == CLUSTER_WAL_PHYSICAL_REPLACE;
	case CLUSTER_WAL_ACTION_RENAME_PARTIAL:
		return physical == CLUSTER_WAL_PHYSICAL_RENAME_PARTIAL;
	case CLUSTER_WAL_ACTION_CREATE_ABSENT:
		return physical == CLUSTER_WAL_PHYSICAL_CREATE_ABSENT;
	default:
		return false;
	}
}

ClusterWalReuseGuardResult
cluster_wal_reuse_guard_l3_begin(ClusterWalReuseActionGuard *guard,
								 ClusterWalReusePhysicalAction physical,
								 ClusterWalReuseDenyReason *out_reason)
{
	ClusterWalFileObjectStamp current_stamp;
	ClusterWalPinThread *pin_thread;
	bool fallback;

	if (out_reason == NULL)
		return CLUSTER_WAL_GUARD_INVALID;
	*out_reason = CLUSTER_WAL_DENY_NONE;
	if (!wal_reuse_guard_valid(guard) || guard->state != CLUSTER_WAL_GUARD_ARMED
		|| active_reuse_guard != guard || !guard->walr.held || !guard->walr.coordinated
		|| guard->walr.mode != ExclusiveLock || guard->walr.release_uncertain
		|| !root_token_matches_identity(&guard->root_read, &guard->duty)) {
		*out_reason = CLUSTER_WAL_DENY_GUARD_STATE;
		return CLUSTER_WAL_GUARD_INVALID;
	}
	if (guard->walr.converted_from_pin) {
		pin_thread = pin_find_thread(guard->pin_or_null, guard->duty.origin_thread_id);
		if (pin_thread == NULL
			|| (guard->pin_or_null->state != CLUSTER_WAL_PIN_STATE_BOUND_ONE
				&& guard->pin_or_null->state != CLUSTER_WAL_PIN_STATE_BOUND_SET)
			|| pin_thread->serial != guard->serial_or_null
			|| memcmp(&pin_thread->duty, &guard->duty, sizeof(guard->duty)) != 0
			|| memcmp(&pin_thread->root_read, &guard->root_read, sizeof(guard->root_read)) != 0
			|| pin_thread->formation != guard->formation
			|| pin_thread->needs != guard->needs_or_null
			|| pin_thread->admissions != guard->admissions_or_null || !pin_thread->walr.held
			|| pin_thread->walr.release_uncertain
			|| cluster_wal_retention_pin_revalidate(guard->pin_or_null) != CLUSTER_WAL_PIN_OK
			|| !serial_matches_thread(guard->serial_or_null, pin_thread)) {
			*out_reason = CLUSTER_WAL_DENY_SERIAL_STALE;
			return CLUSTER_WAL_GUARD_BLOCKED;
		}
	}
	fallback = (guard->flags & CLUSTER_WAL_GUARD_F_PRIMARY_L3) != 0;
	if (!fallback) {
		if (!wal_reuse_primary_physical_legal(guard, physical)) {
			*out_reason = CLUSTER_WAL_DENY_INVALID_IDENTITY;
			return CLUSTER_WAL_GUARD_INVALID;
		}
	} else if (guard->action != CLUSTER_WAL_ACTION_RETIRE_RECYCLE_OR_REMOVE
			   || physical != CLUSTER_WAL_PHYSICAL_REMOVE
			   || (guard->flags
				   & (CLUSTER_WAL_GUARD_F_PRIMARY_RECYCLE | CLUSTER_WAL_GUARD_F_PRIMARY_ZERO))
					  != (CLUSTER_WAL_GUARD_F_PRIMARY_RECYCLE | CLUSTER_WAL_GUARD_F_PRIMARY_ZERO)
			   || (guard->flags & CLUSTER_WAL_GUARD_F_FALLBACK_L3) != 0) {
		*out_reason = CLUSTER_WAL_DENY_GUARD_STATE;
		return CLUSTER_WAL_GUARD_INVALID;
	}
	if (!wal_reuse_guard_fence_current(guard, out_reason))
		return CLUSTER_WAL_GUARD_BLOCKED;
	if (!wal_reuse_stamp_held_target(&guard->file, guard->duty.system_identifier,
									 (int)guard->source_dir_handle, (int)guard->source_handle,
									 &current_stamp)
		|| memcmp(&current_stamp, &guard->pre_action_stamp, sizeof(current_stamp)) != 0) {
		*out_reason = CLUSTER_WAL_DENY_OBJECT_STALE;
		return CLUSTER_WAL_GUARD_BLOCKED;
	}
	if (fallback)
		guard->flags |= CLUSTER_WAL_GUARD_F_FALLBACK_L3;
	else {
		guard->flags |= CLUSTER_WAL_GUARD_F_PRIMARY_L3;
		if (physical == CLUSTER_WAL_PHYSICAL_RECYCLE)
			guard->flags |= CLUSTER_WAL_GUARD_F_PRIMARY_RECYCLE;
	}
	return CLUSTER_WAL_GUARD_OK;
}

static bool
wal_reuse_guard_primary_physical(const ClusterWalReuseActionGuard *guard,
								 ClusterWalReusePhysicalAction *physical)
{
	switch (guard->action) {
	case CLUSTER_WAL_ACTION_RETIRE_RECYCLE_OR_REMOVE:
		*physical = (guard->flags & CLUSTER_WAL_GUARD_F_PRIMARY_RECYCLE) != 0
						? CLUSTER_WAL_PHYSICAL_RECYCLE
						: CLUSTER_WAL_PHYSICAL_REMOVE;
		return true;
	case CLUSTER_WAL_ACTION_FORCE_REPLACE:
	case CLUSTER_WAL_ACTION_ARCHIVE_REPLACE:
		*physical = CLUSTER_WAL_PHYSICAL_REPLACE;
		return true;
	case CLUSTER_WAL_ACTION_RENAME_PARTIAL:
		*physical = CLUSTER_WAL_PHYSICAL_RENAME_PARTIAL;
		return true;
	case CLUSTER_WAL_ACTION_CREATE_ABSENT:
		*physical = CLUSTER_WAL_PHYSICAL_CREATE_ABSENT;
		return true;
	default:
		return false;
	}
}

static ClusterWalTerminalOutcome
wal_reuse_terminal_for_physical(ClusterWalReusePhysicalAction physical)
{
	switch (physical) {
	case CLUSTER_WAL_PHYSICAL_REMOVE:
		return CLUSTER_WAL_TERMINAL_REMOVED;
	case CLUSTER_WAL_PHYSICAL_RECYCLE:
		return CLUSTER_WAL_TERMINAL_RECYCLED;
	case CLUSTER_WAL_PHYSICAL_REPLACE:
		return CLUSTER_WAL_TERMINAL_REPLACED;
	case CLUSTER_WAL_PHYSICAL_RENAME_PARTIAL:
		return CLUSTER_WAL_TERMINAL_RENAMED_PARTIAL;
	case CLUSTER_WAL_PHYSICAL_CREATE_ABSENT:
		return CLUSTER_WAL_TERMINAL_CREATED;
	default:
		return CLUSTER_WAL_TERMINAL_UNCHANGED;
	}
}

ClusterWalReuseGuardResult
cluster_wal_reuse_guard_confirm_zero_mutation(ClusterWalReuseActionGuard *guard,
											  ClusterWalReusePhysicalAction physical,
											  ClusterWalReuseDenyReason *out_reason)
{
	ClusterWalFileObjectStamp current_stamp;
	ClusterWalPinThread *pin_thread;

	if (out_reason == NULL)
		return CLUSTER_WAL_GUARD_INVALID;
	*out_reason = CLUSTER_WAL_DENY_NONE;
	if (!wal_reuse_guard_valid(guard) || guard->state != CLUSTER_WAL_GUARD_ARMED
		|| active_reuse_guard != guard || !guard->walr.held || !guard->walr.coordinated
		|| guard->walr.mode != ExclusiveLock || guard->walr.release_uncertain
		|| !root_token_matches_identity(&guard->root_read, &guard->duty)) {
		*out_reason = CLUSTER_WAL_DENY_GUARD_STATE;
		return CLUSTER_WAL_GUARD_INVALID;
	}
	if (guard->walr.converted_from_pin) {
		pin_thread = pin_find_thread(guard->pin_or_null, guard->duty.origin_thread_id);
		if (pin_thread == NULL
			|| (guard->pin_or_null->state != CLUSTER_WAL_PIN_STATE_BOUND_ONE
				&& guard->pin_or_null->state != CLUSTER_WAL_PIN_STATE_BOUND_SET)
			|| pin_thread->serial != guard->serial_or_null
			|| memcmp(&pin_thread->duty, &guard->duty, sizeof(guard->duty)) != 0
			|| memcmp(&pin_thread->root_read, &guard->root_read, sizeof(guard->root_read)) != 0
			|| pin_thread->formation != guard->formation
			|| pin_thread->needs != guard->needs_or_null
			|| pin_thread->admissions != guard->admissions_or_null || !pin_thread->walr.held
			|| pin_thread->walr.release_uncertain
			|| cluster_wal_retention_pin_revalidate(guard->pin_or_null) != CLUSTER_WAL_PIN_OK
			|| !serial_matches_thread(guard->serial_or_null, pin_thread)) {
			*out_reason = CLUSTER_WAL_DENY_SERIAL_STALE;
			return CLUSTER_WAL_GUARD_BLOCKED;
		}
	}
	if (!wal_reuse_guard_fence_current(guard, out_reason))
		return CLUSTER_WAL_GUARD_BLOCKED;
	if (!wal_reuse_stamp_held_target(&guard->file, guard->duty.system_identifier,
									 (int)guard->source_dir_handle, (int)guard->source_handle,
									 &current_stamp)
		|| memcmp(&current_stamp, &guard->pre_action_stamp, sizeof(current_stamp)) != 0) {
		*out_reason = CLUSTER_WAL_DENY_OBJECT_STALE;
		return CLUSTER_WAL_GUARD_BLOCKED;
	}
	return cluster_wal_reuse_guard_note_zero_mutation(guard, physical, out_reason);
}

ClusterWalReuseGuardResult
cluster_wal_reuse_guard_note_zero_mutation(ClusterWalReuseActionGuard *guard,
										   ClusterWalReusePhysicalAction physical,
										   ClusterWalReuseDenyReason *out_reason)
{
	ClusterWalReusePhysicalAction expected;

	if (out_reason == NULL)
		return CLUSTER_WAL_GUARD_INVALID;
	*out_reason = CLUSTER_WAL_DENY_NONE;
	if (!wal_reuse_guard_valid(guard) || guard->state != CLUSTER_WAL_GUARD_ARMED
		|| (guard->flags & CLUSTER_WAL_GUARD_F_PRIMARY_L3) == 0) {
		*out_reason = CLUSTER_WAL_DENY_GUARD_STATE;
		return CLUSTER_WAL_GUARD_INVALID;
	}
	if ((guard->flags & CLUSTER_WAL_GUARD_F_FALLBACK_L3) != 0) {
		if (physical != CLUSTER_WAL_PHYSICAL_REMOVE
			|| (guard->flags
				& (CLUSTER_WAL_GUARD_F_PRIMARY_RECYCLE | CLUSTER_WAL_GUARD_F_PRIMARY_ZERO))
				   != (CLUSTER_WAL_GUARD_F_PRIMARY_RECYCLE | CLUSTER_WAL_GUARD_F_PRIMARY_ZERO)
			|| (guard->flags & CLUSTER_WAL_GUARD_F_FALLBACK_ZERO) != 0) {
			*out_reason = CLUSTER_WAL_DENY_GUARD_STATE;
			return CLUSTER_WAL_GUARD_INVALID;
		}
		guard->flags |= CLUSTER_WAL_GUARD_F_FALLBACK_ZERO;
		return CLUSTER_WAL_GUARD_OK;
	}
	if ((guard->flags & CLUSTER_WAL_GUARD_F_PRIMARY_ZERO) != 0
		|| !wal_reuse_guard_primary_physical(guard, &expected) || physical != expected) {
		*out_reason = CLUSTER_WAL_DENY_GUARD_STATE;
		return CLUSTER_WAL_GUARD_INVALID;
	}
	guard->flags |= CLUSTER_WAL_GUARD_F_PRIMARY_ZERO;
	return CLUSTER_WAL_GUARD_OK;
}

ClusterWalReuseGuardResult
cluster_wal_reuse_guard_terminal_durable(ClusterWalReuseActionGuard *guard,
										 ClusterWalTerminalOutcome outcome,
										 ClusterWalReuseDenyReason *out_reason)
{
	ClusterWalReusePhysicalAction physical;
	ClusterWalTerminalOutcome expected;

	if (out_reason == NULL)
		return CLUSTER_WAL_GUARD_INVALID;
	*out_reason = CLUSTER_WAL_DENY_NONE;
	if (!wal_reuse_guard_valid(guard) || guard->state != CLUSTER_WAL_GUARD_ARMED
		|| (guard->flags & CLUSTER_WAL_GUARD_F_PRIMARY_L3) == 0) {
		*out_reason = CLUSTER_WAL_DENY_GUARD_STATE;
		return CLUSTER_WAL_GUARD_INVALID;
	}
	if ((guard->flags & CLUSTER_WAL_GUARD_F_FALLBACK_L3) != 0) {
		if ((guard->flags & CLUSTER_WAL_GUARD_F_FALLBACK_ZERO) != 0) {
			*out_reason = CLUSTER_WAL_DENY_GUARD_STATE;
			return CLUSTER_WAL_GUARD_INVALID;
		}
		physical = CLUSTER_WAL_PHYSICAL_REMOVE;
	} else {
		if ((guard->flags & CLUSTER_WAL_GUARD_F_PRIMARY_ZERO) != 0
			|| !wal_reuse_guard_primary_physical(guard, &physical)) {
			*out_reason = CLUSTER_WAL_DENY_GUARD_STATE;
			return CLUSTER_WAL_GUARD_INVALID;
		}
	}
	expected = wal_reuse_terminal_for_physical(physical);
	if (outcome == CLUSTER_WAL_TERMINAL_UNCHANGED || outcome != expected) {
		*out_reason = CLUSTER_WAL_DENY_GUARD_STATE;
		return CLUSTER_WAL_GUARD_INVALID;
	}
	guard->outcome = outcome;
	guard->state = CLUSTER_WAL_GUARD_TERMINAL_DURABLE;
	return CLUSTER_WAL_GUARD_OK;
}

ClusterWalReuseGuardResult
cluster_wal_reuse_guard_remove(ClusterWalReuseActionGuard *guard,
							   ClusterWalReuseDenyReason *out_reason)
{
#ifndef WIN32
	ClusterWalFileObjectStamp current_stamp;
	ClusterWalReusePhysicalAction physical;
	ClusterWalReuseGuardResult result;
	char basename[MAXFNAMELEN];
	struct stat post_action;
	int dirfd;
	int at_flags;

	if (out_reason == NULL)
		return CLUSTER_WAL_GUARD_INVALID;
	*out_reason = CLUSTER_WAL_DENY_NONE;
	if (!wal_reuse_guard_valid(guard) || guard->state != CLUSTER_WAL_GUARD_ARMED
		|| active_reuse_guard != guard || !guard->walr.held || guard->walr.release_uncertain
		|| (guard->flags & CLUSTER_WAL_GUARD_F_PRIMARY_L3) == 0
		/* STOP-05 §15.5: the single recycle-to-remove fallback is the one
		 * legal shape where PRIMARY_ZERO may be set -- the recycle attempt
		 * changed zero bytes/names and the caller re-armed the same guard
		 * through the fallback L3.  A zero-mutation note on the fallback
		 * itself (FALLBACK_ZERO) or a PRIMARY_ZERO without the fallback L3
		 * is ambiguous and stays GUARD_STATE. */
		|| (guard->flags & CLUSTER_WAL_GUARD_F_FALLBACK_ZERO) != 0
		|| ((guard->flags & CLUSTER_WAL_GUARD_F_PRIMARY_ZERO) != 0
			&& (guard->flags & CLUSTER_WAL_GUARD_F_FALLBACK_L3) == 0)) {
		*out_reason = CLUSTER_WAL_DENY_GUARD_STATE;
		return CLUSTER_WAL_GUARD_INVALID;
	}
	if ((guard->flags & CLUSTER_WAL_GUARD_F_FALLBACK_L3) != 0)
		physical = CLUSTER_WAL_PHYSICAL_REMOVE;
	else if (!wal_reuse_guard_primary_physical(guard, &physical)) {
		*out_reason = CLUSTER_WAL_DENY_GUARD_STATE;
		return CLUSTER_WAL_GUARD_INVALID;
	}
	if (physical != CLUSTER_WAL_PHYSICAL_REMOVE
		|| !wal_reuse_target_basename(&guard->file, basename, sizeof(basename))) {
		*out_reason = CLUSTER_WAL_DENY_GUARD_STATE;
		return CLUSTER_WAL_GUARD_INVALID;
	}
	dirfd = (int)guard->source_dir_handle;
#ifdef AT_SYMLINK_NOFOLLOW
	at_flags = AT_SYMLINK_NOFOLLOW;
#else
	*out_reason = CLUSTER_WAL_DENY_OBJECT_STALE;
	return CLUSTER_WAL_GUARD_BLOCKED;
#endif
	/* This is the mutation-adjacent identity check.  It deliberately uses
	 * the exact preflight handles and performs no pathname reconstruction. */
	if (!wal_reuse_stamp_held_target(&guard->file, guard->duty.system_identifier, dirfd,
									 (int)guard->source_handle, &current_stamp)
		|| memcmp(&current_stamp, &guard->pre_action_stamp, sizeof(current_stamp)) != 0) {
		*out_reason = CLUSTER_WAL_DENY_OBJECT_STALE;
		return CLUSTER_WAL_GUARD_BLOCKED;
	}
	if (unlinkat(dirfd, basename, 0) != 0) {
		result = cluster_wal_reuse_guard_confirm_zero_mutation(guard, CLUSTER_WAL_PHYSICAL_REMOVE,
															   out_reason);
		if (result != CLUSTER_WAL_GUARD_OK)
			return result;
		*out_reason = CLUSTER_WAL_DENY_OBJECT_STALE;
		return CLUSTER_WAL_GUARD_BLOCKED;
	}
	if (fsync(dirfd) != 0 || fstatat(dirfd, basename, &post_action, at_flags) == 0
		|| errno != ENOENT) {
		*out_reason = CLUSTER_WAL_DENY_RELEASE_UNCERTAIN;
		return CLUSTER_WAL_GUARD_RELEASE_UNCERTAIN;
	}
	return cluster_wal_reuse_guard_terminal_durable(guard, CLUSTER_WAL_TERMINAL_REMOVED,
													out_reason);
#else
	if (out_reason != NULL)
		*out_reason = CLUSTER_WAL_DENY_OBJECT_STALE;
	return out_reason == NULL ? CLUSTER_WAL_GUARD_INVALID : CLUSTER_WAL_GUARD_BLOCKED;
#endif
}

ClusterWalReuseGuardResult
cluster_wal_reuse_guard_recycle(ClusterWalReuseActionGuard *guard,
								const ClusterWalFileIdentity *destination,
								ClusterWalReuseDenyReason *out_reason)
{
#ifndef WIN32
	ClusterWalFileObjectStamp current_stamp;
	ClusterWalReusePhysicalAction physical;
	ClusterWalReuseGuardResult result;
	char source_basename[MAXFNAMELEN];
	char destination_basename[MAXFNAMELEN];
	struct stat source_after;
	struct stat destination_after;
	int dirfd;
	int at_flags;
	int stat_result;

	if (out_reason == NULL)
		return CLUSTER_WAL_GUARD_INVALID;
	*out_reason = CLUSTER_WAL_DENY_NONE;
	if (!wal_reuse_guard_valid(guard) || guard->state != CLUSTER_WAL_GUARD_ARMED
		|| active_reuse_guard != guard || !guard->walr.held || guard->walr.release_uncertain
		|| (guard->flags & CLUSTER_WAL_GUARD_F_PRIMARY_L3) == 0
		|| (guard->flags
			& (CLUSTER_WAL_GUARD_F_PRIMARY_ZERO | CLUSTER_WAL_GUARD_F_FALLBACK_L3
			   | CLUSTER_WAL_GUARD_F_FALLBACK_ZERO))
			   != 0
		|| !wal_reuse_guard_primary_physical(guard, &physical)
		|| physical != CLUSTER_WAL_PHYSICAL_RECYCLE || destination == NULL
		|| !cluster_wal_file_identity_valid(destination, wal_segment_size)
		|| destination->kind != CLUSTER_WAL_FILE_NORMAL
		|| destination->thread_id != guard->file.thread_id
		|| memcmp(destination, &guard->file, sizeof(*destination)) == 0
		|| !wal_reuse_target_basename(&guard->file, source_basename, sizeof(source_basename))
		|| !wal_reuse_target_basename(destination, destination_basename,
									  sizeof(destination_basename))) {
		*out_reason = CLUSTER_WAL_DENY_GUARD_STATE;
		return CLUSTER_WAL_GUARD_INVALID;
	}
	dirfd = (int)guard->source_dir_handle;
#ifdef AT_SYMLINK_NOFOLLOW
	at_flags = AT_SYMLINK_NOFOLLOW;
#else
	*out_reason = CLUSTER_WAL_DENY_OBJECT_STALE;
	return CLUSTER_WAL_GUARD_BLOCKED;
#endif
	if (!wal_reuse_stamp_held_target(&guard->file, guard->duty.system_identifier, dirfd,
									 (int)guard->source_handle, &current_stamp)
		|| memcmp(&current_stamp, &guard->pre_action_stamp, sizeof(current_stamp)) != 0) {
		*out_reason = CLUSTER_WAL_DENY_OBJECT_STALE;
		return CLUSTER_WAL_GUARD_BLOCKED;
	}
	stat_result = fstatat(dirfd, destination_basename, &destination_after, at_flags);
	if (stat_result == 0 || errno != ENOENT) {
		result = cluster_wal_reuse_guard_note_zero_mutation(guard, CLUSTER_WAL_PHYSICAL_RECYCLE,
															out_reason);
		if (result != CLUSTER_WAL_GUARD_OK)
			return result;
		*out_reason = CLUSTER_WAL_DENY_OBJECT_STALE;
		return CLUSTER_WAL_GUARD_BLOCKED;
	}
	if (renameat(dirfd, source_basename, dirfd, destination_basename) != 0) {
		result = cluster_wal_reuse_guard_confirm_zero_mutation(guard, CLUSTER_WAL_PHYSICAL_RECYCLE,
															   out_reason);
		if (result != CLUSTER_WAL_GUARD_OK)
			return result;
		*out_reason = CLUSTER_WAL_DENY_OBJECT_STALE;
		return CLUSTER_WAL_GUARD_BLOCKED;
	}
	if (fsync(dirfd) != 0 || fstatat(dirfd, source_basename, &source_after, at_flags) == 0
		|| errno != ENOENT || fstat((int)guard->source_handle, &source_after) != 0
		|| fstatat(dirfd, destination_basename, &destination_after, at_flags) != 0
		|| !wal_reuse_same_leaf_stat(&source_after, &destination_after)) {
		*out_reason = CLUSTER_WAL_DENY_RELEASE_UNCERTAIN;
		return CLUSTER_WAL_GUARD_RELEASE_UNCERTAIN;
	}
	return cluster_wal_reuse_guard_terminal_durable(guard, CLUSTER_WAL_TERMINAL_RECYCLED,
													out_reason);
#else
	(void)destination;
	if (out_reason != NULL)
		*out_reason = CLUSTER_WAL_DENY_OBJECT_STALE;
	return out_reason == NULL ? CLUSTER_WAL_GUARD_INVALID : CLUSTER_WAL_GUARD_BLOCKED;
#endif
}

ClusterWalReuseGuardResult
cluster_wal_reuse_guard_bookkeep(ClusterWalReuseActionGuard *guard,
								 ClusterWalReuseDenyReason *out_reason)
{
	if (out_reason == NULL)
		return CLUSTER_WAL_GUARD_INVALID;
	*out_reason = CLUSTER_WAL_DENY_NONE;
	if (!wal_reuse_guard_valid(guard) || guard->state != CLUSTER_WAL_GUARD_TERMINAL_DURABLE
		|| guard->outcome == CLUSTER_WAL_TERMINAL_UNCHANGED) {
		*out_reason = CLUSTER_WAL_DENY_GUARD_STATE;
		return CLUSTER_WAL_GUARD_INVALID;
	}
	guard->state = CLUSTER_WAL_GUARD_BOOKKEPT;
	return CLUSTER_WAL_GUARD_OK;
}

ClusterWalrReleaseResult
cluster_wal_reuse_guard_finish(ClusterWalReuseActionGuard *guard,
							   ClusterWalTerminalOutcome *out_outcome,
							   ClusterWalReuseDenyReason *out_reason)
{
	if (out_outcome != NULL)
		*out_outcome = CLUSTER_WAL_TERMINAL_UNCHANGED;
	if (out_reason != NULL)
		*out_reason = CLUSTER_WAL_DENY_NONE;
	if (out_outcome == NULL || out_reason == NULL || !wal_reuse_guard_valid(guard)) {
		if (out_reason != NULL)
			*out_reason = CLUSTER_WAL_DENY_GUARD_STATE;
		return CLUSTER_WALR_RELEASE_INVALID;
	}
	if (guard->state == CLUSTER_WAL_GUARD_BOOKKEPT) {
		if (guard->outcome == CLUSTER_WAL_TERMINAL_UNCHANGED) {
			*out_reason = CLUSTER_WAL_DENY_GUARD_STATE;
			return CLUSTER_WALR_RELEASE_INVALID;
		}
		*out_outcome = guard->outcome;
	} else if (guard->state <= CLUSTER_WAL_GUARD_ARMED) {
		if (guard->outcome != CLUSTER_WAL_TERMINAL_UNCHANGED
			|| ((guard->flags & CLUSTER_WAL_GUARD_F_PRIMARY_L3) != 0
				&& (guard->flags & CLUSTER_WAL_GUARD_F_PRIMARY_ZERO) == 0)
			|| ((guard->flags & CLUSTER_WAL_GUARD_F_FALLBACK_L3) != 0
				&& (guard->flags & CLUSTER_WAL_GUARD_F_FALLBACK_ZERO) == 0)
			|| ((guard->flags & CLUSTER_WAL_GUARD_F_FALLBACK_L3) != 0
				&& (guard->flags & CLUSTER_WAL_GUARD_F_PRIMARY_RECYCLE) == 0)) {
			*out_reason = CLUSTER_WAL_DENY_GUARD_STATE;
			return CLUSTER_WALR_RELEASE_INVALID;
		}
	} else {
		*out_reason = CLUSTER_WAL_DENY_GUARD_STATE;
		return CLUSTER_WALR_RELEASE_INVALID;
	}
	if (guard->walr.release_uncertain) {
		*out_reason = CLUSTER_WAL_DENY_RELEASE_UNCERTAIN;
		return CLUSTER_WALR_RELEASE_UNCONFIRMED;
	}
	if (guard->walr.held) {
		ClusterLockAcquireResult lock_result;

		if (active_reuse_guard != guard) {
			*out_reason = CLUSTER_WAL_DENY_GUARD_STATE;
			return CLUSTER_WALR_RELEASE_INVALID;
		}
		if (guard->walr.converted_from_pin) {
			ClusterWalPinThread *thread;
			ClusterLockAcquireRequest downgrade;
			bool cleanup_required = false;

			thread = pin_find_thread(guard->pin_or_null, guard->duty.origin_thread_id);
			if (thread == NULL || !thread->walr.held || thread->serial != guard->serial_or_null
				|| guard->walr.acquire_request.lockmode != ExclusiveLock
				|| guard->walr.mode != ExclusiveLock) {
				*out_reason = CLUSTER_WAL_DENY_GUARD_STATE;
				return CLUSTER_WALR_RELEASE_INVALID;
			}
			downgrade = guard->walr.acquire_request;
			downgrade.lockmode = ShareLock;
			downgrade.op = CLUSTER_LOCK_OP_CONVERT;
			downgrade.current_mode = ExclusiveLock;
			downgrade.convert_old_request_id = 0;
			downgrade.dontwait = true;
			lock_result = walr_request_convert_actual(
				&downgrade, guard->walr.acquire_request.request_id, &cleanup_required);
			if (lock_result != CLUSTER_LOCK_ACQUIRE_OK_CONVERTED) {
				guard->walr.release_uncertain = true;
				guard->pin_or_null->poisoned = true;
				*out_reason = CLUSTER_WAL_DENY_RELEASE_UNCERTAIN;
				return CLUSTER_WALR_RELEASE_UNCONFIRMED;
			}

			/* Drop only the temporary native X and extra S conversion refs;
			 * the pin's original native S remains.  The master holder keeps
			 * the confirmed converted request id, now in ShareLock mode. */
			walr_native_lock_release_or_fatal(&guard->walr.acquire_request);
			walr_native_lock_release_or_fatal(&downgrade);
			thread->walr.request = downgrade;
			thread->walr.held = true;
			thread->walr.release_uncertain = false;
			guard->walr.held = false;
			active_reuse_guard = NULL;
			wal_reuse_guard_close_handles(guard);
			explicit_bzero(guard, sizeof(*guard));
			return CLUSTER_WALR_RELEASE_CONFIRMED;
		}
		lock_result = walr_request_release_actual(&guard->walr.acquire_request);
		if (lock_result != CLUSTER_LOCK_ACQUIRE_OK_GRANTED) {
			guard->walr.release_uncertain = true;
			*out_reason = CLUSTER_WAL_DENY_RELEASE_UNCERTAIN;
			return CLUSTER_WALR_RELEASE_UNCONFIRMED;
		}
		guard->walr.held = false;
		active_reuse_guard = NULL;
		wal_reuse_guard_close_handles(guard);
		explicit_bzero(guard, sizeof(*guard));
		return CLUSTER_WALR_RELEASE_CONFIRMED;
	}
	wal_reuse_guard_close_handles(guard);
	explicit_bzero(guard, sizeof(*guard));
	return CLUSTER_WALR_RELEASE_NOT_HELD;
}

bool
cluster_wal_retention_fold_validated_roots(const ClusterWalRootFoldInput *inputs, uint16 nslots,
										   int wal_segsz_bytes, ClusterWalRootFold *out_fold)
{
	uint16 i;

	if (out_fold == NULL)
		return false;
	fold_unknown(out_fold);
	if (inputs == NULL || nslots == 0 || nslots > CLUSTER_WAL_RETENTION_MAX_THREADS
		|| !IsValidWalSegSize(wal_segsz_bytes))
		return false;

	for (i = 0; i < nslots; i++) {
		const ClusterWalRootFoldInput *input = &inputs[i];
		const ClusterControlRootSnapshot *root = &input->snapshot;
		ClusterWalRetentionInterval interval;
		XLogSegNo first;
		XLogSegNo last;

		if (!input->configured)
			continue;
		if (!control_root_read_ready(input->read_result) || root->identity.origin_thread_id != i + 1
			|| root->identity.system_identifier == 0 || root->identity.root_lineage_seq == 0
			|| (root->root_flags & ~CLUSTER_CONTROL_ROOT_FLAGS_V1) != 0
			|| (root->root_flags & CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID) == 0) {
			fold_unknown(out_fold);
			return true;
		}

		switch (root->lifecycle) {
		case CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN:
		case CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED:
			if ((root->root_flags & CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID) == 0
				|| (root->root_flags & CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID) == 0
				|| root->checkpoint_tli == 0 || root->checkpoint_tli != root->tail_tli
				|| root->checkpoint_lower_lsn == InvalidXLogRecPtr
				|| root->validated_tail_lsn_exclusive < root->checkpoint_lower_lsn) {
				fold_unknown(out_fold);
				return true;
			}
			if (root->validated_tail_lsn_exclusive == root->checkpoint_lower_lsn)
				continue;
			memset(&interval, 0, sizeof(interval));
			interval.thread_id = i + 1;
			interval.tli = root->checkpoint_tli;
			interval.start_lsn = root->checkpoint_lower_lsn;
			interval.end_lsn = root->validated_tail_lsn_exclusive;
			if (!cluster_wal_retention_interval_segment_bounds(&interval, wal_segsz_bytes, &first,
															   &last)
				|| out_fold->nintervals >= CLUSTER_WAL_RETENTION_MAX_THREADS) {
				fold_unknown(out_fold);
				return true;
			}
			out_fold->intervals[out_fold->nintervals++] = interval;
			out_fold->floor_by_thread[i] = first;
			break;
		case CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE:
		case CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED:
		case CLUSTER_CONTROL_ROOT_LIFECYCLE_RETIRED:
			break;
		case CLUSTER_CONTROL_ROOT_LIFECYCLE_UNUSED:
		default:
			fold_unknown(out_fold);
			return true;
		}
	}
	out_fold->result
		= out_fold->nintervals > 0 ? CLUSTER_WAL_FOLD_BOUNDED : CLUSTER_WAL_FOLD_UNCONSTRAINED;
	return true;
}
