/* PGRAC: full ROOT-selected input census; never replay or PI retirement.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include <unistd.h>

#include "access/xlog.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_wal_inputs.h"
#include "cluster/cluster_wal_retention.h"
#include "cluster/cluster_wal_writer.h"
#include "common/cryptohash.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "utils/resowner.h"

#include "cluster_control_root_private.h"
#include "cluster_recovery_anchor_private.h"

struct ClusterWalInputsV1 {
	uint64 system_identifier;
	uint8 storage_uuid[16];
	ClusterControlRootFileToken token;
	ClusterWalReadPinV1 *pin;
	ResourceOwner owner;
	pid_t pid;
	bool stale;
	uint16 thread_count;
	uint16 threads[CLUSTER_WAL_RETENTION_MAX_THREADS];
	uint32 count;
	ClusterWalInputV1 items[CLUSTER_WAL_INPUTS_MAX];
	/* Job-local minimum sampled after its directory cut, not a Flush promise. */
	ClusterWalWriterToken local_writer;
	XLogRecPtr local_minimum;
	uint32 local_index;
};

typedef struct WalInputsWork {
	ControlRootImage root;
	ControlFileData common;
	ClusterWalOriginInputs origin;
	ClusterWalTerminalImage terminal;
	bool cf_attempted;
} WalInputsWork;

/* This I/O job uses native CF/WALR waits, whose CONTROL dispatch runs in
 * other processes. It must never execute inside LMON/LMS dispatch itself. */
static bool
inputs_io_role(void)
{
	return MyBackendType == B_BG_WORKER || MyBackendType == B_BG_WRITER
		   || MyBackendType == B_CHECKPOINTER;
}

static bool
inputs_owned(const ClusterWalInputsV1 *inputs)
{
	return inputs != NULL && inputs->pid == getpid() && CurrentResourceOwner != NULL
		   && inputs->owner == CurrentResourceOwner;
}

static bool
inputs_current(ClusterWalInputsV1 *inputs)
{
	return cluster_enabled && cluster_shared_config && inputs_io_role() && inputs_owned(inputs)
		   && !inputs->stale && inputs->thread_count > 0
		   && inputs->thread_count <= CLUSTER_WAL_RETENTION_MAX_THREADS
		   && cluster_wal_read_pin_covers_v1(inputs->pin, inputs->threads[0]);
}

static void
inputs_unlock(WalInputsWork *work)
{
	if (work->cf_attempted) {
		ClusterCfReleaseResult released = cluster_cf_unlock_confirmed(ShareLock);
		if (released != CLUSTER_CF_RELEASE_CONFIRMED && released != CLUSTER_CF_RELEASE_NOT_HELD)
			elog(FATAL, "could not release WAL input CF read owner");
		work->cf_attempted = false;
	}
}

void
cluster_wal_inputs_release_v1(ClusterWalInputsV1 **inputs)
{
	if (inputs == NULL || *inputs == NULL)
		return;
	if (!inputs_owned(*inputs))
		elog(ERROR, "WAL input scope belongs to another resource owner");
	if ((*inputs)->pin != NULL
		&& cluster_wal_read_pin_release_v1(&(*inputs)->pin) != CLUSTER_WALR_RELEASE_CONFIRMED)
		elog(FATAL, "could not release WAL input retention owner");
	explicit_bzero(*inputs, sizeof(**inputs));
	pfree(*inputs);
	*inputs = NULL;
}

static ClusterControlRootResult
inputs_root(ClusterWalInputsV1 *inputs, WalInputsWork *work, bool first)
{
	ClusterControlRootFileToken token;
	ClusterControlRootResult result;

	if (cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	work->cf_attempted = true;
	if (!cluster_cf_lock(ShareLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	result = cluster_control_root_v3_read_control_locked(
		inputs->storage_uuid, inputs->system_identifier, &work->root, &work->common, &token);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (work->root.header.activation_state != CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE
		|| work->root.header.v2.database_state < CLUSTER_CONTROL_ROOT_DATABASE_MOUNTED
		|| work->root.header.v2.database_state > CLUSTER_CONTROL_ROOT_DATABASE_CLOSED)
		return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
	if (first)
		inputs->token = token;
	else if (memcmp(&inputs->token, &token, sizeof(token)) != 0)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static ClusterControlRootResult
inputs_checkpoint(ClusterWalInputsV1 *inputs, const WalInputsWork *work,
				  const ClusterWalHistoryRecord *record, bool current)
{
	ClusterWalInputV1 *item;
	ClusterWalThreadClaimV2 claim;
	ClusterRecoveryAnchorRefV2 anchor = { 0 };
	ControlFileData native;
	ClusterControlRootResult result;

	if (inputs->count >= CLUSTER_WAL_INPUTS_MAX)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	item = &inputs->items[inputs->count];
	item->kind = CLUSTER_WAL_INPUT_CHECKPOINT;
	item->current = current;
	item->checkpoint = record->snapshot;
	item->source.claim.identity = record->snapshot.identity;
	item->source.claim.database_incarnation = work->root.header.v2.database_incarnation;
	item->source.claim.max_config_generation = work->root.header.v2.config_generation;
	memcpy(item->source.claim.claim_sha256, record->refs.claim_sha256, 32);
	item->source.timeline = record->snapshot.checkpoint_tli;
	result = cluster_wal_claim_v2_read(cluster_wal_threads_dir, &item->source.claim, &claim);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	anchor.identity = record->snapshot.identity;
	anchor.database_incarnation = item->source.claim.database_incarnation;
	anchor.max_config_generation = item->source.claim.max_config_generation;
	anchor.anchor_generation = record->refs.anchor_generation;
	memcpy(anchor.anchor_sha256, record->refs.anchor_sha256, 32);
	memcpy(anchor.claim_sha256, record->refs.claim_sha256, 32);
	result = cluster_recovery_anchor_v2_read_native_locked(&anchor, &work->common, &native);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		result = cluster_recovery_anchor_v2_thread_state(&anchor, &record->snapshot, &native);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	item->checkpoint_start = native.checkPoint;
	item->native_redo = native.checkPointCopy.redo;
	inputs->count++;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static ClusterControlRootResult
inputs_terminal(ClusterWalInputsV1 *inputs, WalInputsWork *work, uint32 node, uint32 index)
{
	ClusterWalInputV1 *item;
	ClusterWalThreadClaimV2 claim;
	uint8 bytes[CLUSTER_WAL_CLAIM_V2_BYTES];
	pg_cryptohash_ctx *hash;
	ClusterControlRootResult result;
	bool hashed;

	if (inputs->count >= CLUSTER_WAL_INPUTS_MAX)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	result = cluster_wal_terminal_read_locked(&work->root, node, index, &work->terminal);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	item = &inputs->items[inputs->count];
	item->kind = CLUSTER_WAL_INPUT_INITIALIZER_TERMINAL;
	item->source.claim.identity = work->terminal.initialization.claim.identity;
	item->source.claim.database_incarnation = work->root.header.v2.database_incarnation;
	item->source.claim.max_config_generation = work->root.header.v2.config_generation;
	item->source.timeline = work->terminal.initialization.timeline;
	result = cluster_wal_claim_v2_encode(&work->terminal.initialization.claim, bytes);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	hash = pg_cryptohash_create(PG_SHA256);
	if (hash == NULL)
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	hashed = pg_cryptohash_init(hash) >= 0 && pg_cryptohash_update(hash, bytes, sizeof(bytes)) >= 0
			 && pg_cryptohash_final(hash, item->source.claim.claim_sha256, 32) >= 0;
	pg_cryptohash_free(hash);
	if (!hashed)
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	result = cluster_wal_claim_v2_read(cluster_wal_threads_dir, &item->source.claim, &claim);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	item->first_segment = work->terminal.initialization.first_segment_lsn;
	item->terminal = work->terminal.observation;
	inputs->count++;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static ClusterControlRootResult
inputs_collect(ClusterWalInputsV1 *inputs, WalInputsWork *work)
{
	ClusterControlRootResult result;

	for (uint32 node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; node++) {
		if (!work->root.present[node])
			continue;
		if (!cluster_wal_read_pin_covers_v1(inputs->pin, node + 1))
			return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		result = cluster_wal_origin_inputs_read_locked(&work->root, node, &work->origin);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
		if (work->origin.has_pending)
			return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
		if (work->origin.history.count + work->origin.history.terminal_count + 1
			> CLUSTER_WAL_INPUTS_MAX - inputs->count)
			return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		for (uint32 i = 0; i < work->origin.history.count; i++) {
			result = inputs_checkpoint(inputs, work, &work->origin.history.records[i], false);
			if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
				return result;
		}
		for (uint32 i = 0; i < work->origin.history.terminal_count; i++) {
			result = inputs_terminal(inputs, work, node, i);
			if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
				return result;
		}
		result = inputs_checkpoint(inputs, work, &work->origin.current, true);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
	}
	return inputs_current(inputs) ? CLUSTER_CONTROL_ROOT_OK_PRIMARY
								  : CLUSTER_CONTROL_ROOT_STALE_TOKEN;
}

typedef struct WalInputsVisit {
	ClusterWalInputsV1 *inputs;
	ClusterWalRecordVisitor visitor;
	void *arg;
} WalInputsVisit;

static bool
inputs_visit_record(struct XLogReaderState *reader, void *arg)
{
	WalInputsVisit *visit = arg;
	if (!inputs_current(visit->inputs) || cluster_cf_held(ShareLock)
		|| cluster_cf_held(ExclusiveLock))
		return false;
	if (visit->visitor != NULL && !visit->visitor(reader, visit->arg))
		return false;
	return inputs_current(visit->inputs) && !cluster_cf_held(ShareLock)
		   && !cluster_cf_held(ExclusiveLock);
}

/* Terminal codec admits PARAMETER_CHANGE/FPW only. Compare logical fields,
 * not native structure padding or just the final LSN. A later checkpoint or
 * any newly observed side effect invalidates this selected terminal. */
static bool
inputs_terminal_same(const ClusterWalStartupObservation *a, const ClusterWalStartupObservation *b)
{
	return a->tail.complete_end == b->tail.complete_end
		   && a->tail.last_record_start == b->tail.last_record_start
		   && a->tail.last_record_crc == b->tail.last_record_crc
		   && a->tail.records == b->tail.records
		   && a->tail.database_incarnation == b->tail.database_incarnation
		   && a->checkpoint_records == 0 && b->checkpoint_records == 0
		   && a->unsupported_records == 0 && b->unsupported_records == 0
		   && a->fpw_records == b->fpw_records && a->parameter_records == b->parameter_records
		   && a->fpw_disabled == b->fpw_disabled && a->max_connections == b->max_connections
		   && a->max_worker_processes == b->max_worker_processes
		   && a->max_wal_senders == b->max_wal_senders
		   && a->max_prepared_xacts == b->max_prepared_xacts
		   && a->max_locks_per_xact == b->max_locks_per_xact;
}

ClusterControlRootResult
cluster_wal_inputs_visit_retained_v1(ClusterWalInputsV1 *inputs, uint32 index,
									 ClusterWalRecordVisitor visitor, void *arg,
									 ClusterWalTailObservation *out)
{
	ClusterWalTailObservation observed = { 0 };
	WalInputsVisit visit = { inputs, visitor, arg };
	ClusterControlRootResult result;
	const ClusterWalInputV1 *item;

	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(out, 0, sizeof(*out));
	if (!inputs_current(inputs) || index >= inputs->count)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	item = &inputs->items[index];
	PG_TRY();
	{
		result = cluster_wal_inputs_revalidate_v1(inputs);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
			if (item->kind == CLUSTER_WAL_INPUT_CHECKPOINT)
				result = cluster_wal_retained_visit_v1(
					cluster_wal_threads_dir, &item->source, wal_segment_size, &item->checkpoint,
					item->checkpoint_start, inputs_visit_record, &visit, &observed);
			else if (item->kind == CLUSTER_WAL_INPUT_INITIALIZER_TERMINAL) {
				ClusterWalStartupObservation startup;
				result = cluster_wal_startup_visit_v1(cluster_wal_threads_dir, &item->source,
													  wal_segment_size, item->first_segment,
													  inputs_visit_record, &visit, &startup);
				if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
					if (!inputs_terminal_same(&startup, &item->terminal))
						result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
					else
						observed = startup.tail;
				}
			} else
				result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
		}
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			result = cluster_wal_inputs_revalidate_v1(inputs);
	}
	PG_CATCH();
	{
		inputs->stale = true;
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		*out = observed;
	else
		inputs->stale = true;
	return result;
}

static ClusterControlRootResult
inputs_live_sample(ClusterWalInputsV1 *inputs, uint32 index, ClusterWalWriterFlushV1 *out)
{
	const ClusterWalInputV1 *item = &inputs->items[index];
	ClusterWalWriterToken current;
	ClusterControlRootResult result;

	result = cluster_wal_writer_begin(item->source.timeline, &current);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	/* ROOT's later configuration ceiling may cover this same immutable
	 * native claim. The original writer token itself remains byte-exact. */
	if (!cluster_control_root_identity_equal(&current.ref.claim.identity,
											 &item->source.claim.identity)
		|| current.ref.claim.database_incarnation != item->source.claim.database_incarnation
		|| current.ref.claim.max_config_generation == 0
		|| current.ref.claim.max_config_generation > item->source.claim.max_config_generation
		|| memcmp(current.ref.claim.claim_sha256, item->source.claim.claim_sha256, 32) != 0
		|| current.ref.timeline != item->source.timeline || current.startup_first_lsn != 0)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (inputs->local_minimum == InvalidXLogRecPtr) {
		XLogRecPtr minimum = GetXLogInsertEndRecPtr();
		result = cluster_wal_writer_check(&current);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
		if (minimum <= item->checkpoint_start
			|| minimum < item->checkpoint.validated_tail_lsn_exclusive)
			return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		inputs->local_writer = current;
		inputs->local_minimum = minimum;
		inputs->local_index = index;
	} else if (inputs->local_index != index
			   || memcmp(&current, &inputs->local_writer, sizeof(current)) != 0)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	result = cluster_wal_writer_flushed_v1(out);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (memcmp(&out->writer, &inputs->local_writer, sizeof(out->writer)) != 0)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (out->complete_end == InvalidXLogRecPtr || out->flushed_end < out->complete_end)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	/* The process-local sampler may have completed an earlier job's pending
	 * reservation. Consuming that sample is safe, using it for this job is
	 * not. The next poll samples anew without moving our fixed minimum. */
	return out->complete_end < inputs->local_minimum ? CLUSTER_CONTROL_ROOT_RECONFIG_WAIT
													 : CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_wal_inputs_visit_live_local_v1(ClusterWalInputsV1 *inputs, uint32 index,
									   ClusterWalRecordVisitor visitor, void *arg,
									   ClusterWalTailObservation *out)
{
	ClusterWalTailObservation observed = { 0 };
	ClusterWalWriterFlushV1 native;
	WalInputsVisit visit = { inputs, visitor, arg };
	const ClusterWalInputV1 *item;
	ClusterControlRootResult result;
	bool visited = false;

	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(out, 0, sizeof(*out));
	if (!inputs_current(inputs) || index >= inputs->count || CritSectionCount != 0
		|| ShutdownRequestPending || RecoveryInProgress())
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	item = &inputs->items[index];
	if (!item->current || item->kind != CLUSTER_WAL_INPUT_CHECKPOINT
		|| item->checkpoint.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	PG_TRY();
	{
		result = cluster_wal_inputs_revalidate_v1(inputs);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
			if (item->source.claim.identity.origin_node_id != cluster_node_id)
				result = CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
			else
				result = inputs_live_sample(inputs, index, &native);
		}
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
			visited = true;
			result = cluster_wal_flushed_prefix_visit(
				cluster_wal_threads_dir, &item->source, wal_segment_size,
				item->checkpoint.checkpoint_lower_lsn, inputs->local_minimum, native.complete_end,
				native.flushed_end, item->checkpoint_start,
				item->checkpoint.checkpoint_record_crc32c, inputs_visit_record, &visit, &observed);
		}
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			result = cluster_wal_writer_check(&native.writer);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			result = cluster_wal_inputs_revalidate_v1(inputs);
	}
	PG_CATCH();
	{
		inputs->stale = true;
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		*out = observed;
	else if (visited || result != CLUSTER_CONTROL_ROOT_RECONFIG_WAIT)
		inputs->stale = true;
	return result;
}

ClusterControlRootResult
cluster_wal_inputs_begin_v1(const uint8 storage_uuid[16], uint64 system_identifier,
							ClusterWalInputsV1 **out)
{
	ClusterWalInputsV1 *inputs;
	WalInputsWork *work;
	ClusterControlRootResult result;
	ClusterWalPinResult pinned;

	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	*out = NULL;
	if (storage_uuid == NULL || system_identifier == 0 || !cluster_enabled || !cluster_shared_config
		|| !cluster_controlfile_shared_authority || CritSectionCount != 0
		|| CurrentResourceOwner == NULL || system_identifier != GetSystemIdentifier()
		|| !inputs_io_role())
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	inputs = palloc0(sizeof(*inputs));
	inputs->pid = getpid();
	inputs->owner = CurrentResourceOwner;
	inputs->system_identifier = system_identifier;
	memcpy(inputs->storage_uuid, storage_uuid, 16);
	work = palloc0(sizeof(*work));
	PG_TRY();
	{
		result = inputs_root(inputs, work, true);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
			for (uint32 node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; node++)
				if (work->root.present[node])
					inputs->threads[inputs->thread_count++] = node + 1;
			if (inputs->thread_count == 0)
				result = CLUSTER_CONTROL_ROOT_ABSENT;
		}
		inputs_unlock(work);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
			pinned = cluster_wal_read_pin_acquire_v1(inputs->threads, inputs->thread_count,
													 &inputs->pin);
			if (pinned != CLUSTER_WAL_PIN_OK)
				result = pinned == CLUSTER_WAL_PIN_RELEASE_UNCERTAIN
							 ? CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN
							 : CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
		}
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			result = inputs_root(inputs, work, false);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			result = inputs_collect(inputs, work);
		inputs_unlock(work);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY && !inputs_current(inputs))
			result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	}
	PG_CATCH();
	{
		inputs_unlock(work);
		cluster_wal_inputs_release_v1(&inputs);
		pfree(work);
		PG_RE_THROW();
	}
	PG_END_TRY();
	pfree(work);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		cluster_wal_inputs_release_v1(&inputs);
	else
		*out = inputs;
	return result;
}

ClusterControlRootResult
cluster_wal_inputs_revalidate_v1(ClusterWalInputsV1 *inputs)
{
	WalInputsWork *work;
	ClusterControlRootResult result;

	if (!inputs_current(inputs))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	work = palloc0(sizeof(*work));
	PG_TRY();
	{
		result = inputs_root(inputs, work, false);
		inputs_unlock(work);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY && !inputs_current(inputs))
			result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	}
	PG_CATCH();
	{
		inputs->stale = true;
		inputs_unlock(work);
		pfree(work);
		PG_RE_THROW();
	}
	PG_END_TRY();
	pfree(work);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE)
		inputs->stale = true;
	return result;
}

uint32
cluster_wal_inputs_count_v1(ClusterWalInputsV1 *inputs)
{
	return inputs_current(inputs) ? inputs->count : 0;
}

const ClusterWalInputV1 *
cluster_wal_inputs_at_v1(ClusterWalInputsV1 *inputs, uint32 index)
{
	return inputs_current(inputs) && index < inputs->count ? &inputs->items[index] : NULL;
}

#endif
