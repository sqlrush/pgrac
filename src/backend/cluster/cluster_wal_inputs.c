/* PGRAC: full ROOT-selected input census; never replay or PI retirement.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include <unistd.h>

#include "access/xlog.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_wal_inputs.h"
#include "cluster/cluster_wal_retention.h"
#include "cluster/cluster_wal_writer.h"
#include "cluster/cluster_wal_cut.h"
#include "common/cryptohash.h"
#include "miscadmin.h"
#include "postmaster/bgwriter.h"
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
	bool suspended;
	bool cold;
	uint16 thread_count;
	uint16 threads[CLUSTER_WAL_RETENTION_MAX_THREADS];
	uint32 count;
	ClusterWalInputV1 items[CLUSTER_WAL_INPUTS_MAX];
	/* Job-local minimum sampled after its directory cut, not a Flush promise. */
	ClusterWalWriterToken local_writer;
	XLogRecPtr local_minimum;
	uint32 local_index;
	ClusterWalWriterFlushV1 local_cut;
	ClusterWalCutV1 *remote[CLUSTER_WAL_INPUTS_MAX];
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
inputs_io_role(bool cold)
{
	if (cold)
		return MyBackendType == B_STARTUP && RecoveryInProgress();
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
	return cluster_enabled && cluster_shared_config && inputs_owned(inputs)
		   && inputs_io_role(inputs->cold) && !inputs->stale && inputs->thread_count > 0
		   && !inputs->suspended && inputs->thread_count <= CLUSTER_WAL_RETENTION_MAX_THREADS
		   && cluster_wal_read_pin_covers_v1(inputs->pin, inputs->threads[0]);
}

static bool
inputs_source_selected(const ClusterWalSourceRef *source, const ClusterWalSourceRef *selected)
{
	return cluster_wal_claim_v2_ref_valid(&source->claim)
		   && cluster_control_root_identity_equal(&source->claim.identity,
												  &selected->claim.identity)
		   && source->claim.database_incarnation == selected->claim.database_incarnation
		   && source->claim.max_config_generation <= selected->claim.max_config_generation
		   && memcmp(source->claim.claim_sha256, selected->claim.claim_sha256, 32) == 0
		   && source->timeline == selected->timeline;
}

bool
cluster_wal_inputs_recovered_prefix_v1(ClusterWalInputsV1 *inputs,
									   const ClusterWalSourceRef *source, XLogRecPtr *out_end)
{
	const uint32 required
		= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID
		  | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID | CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID
		  | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID
		  | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_LAST_RECORD_VALID;

	if (out_end != NULL)
		*out_end = InvalidXLogRecPtr;
	if (out_end == NULL || source == NULL || !inputs_current(inputs) || inputs->cold
		|| source->claim.identity.origin_owner_incarnation == 0)
		return false;
	for (uint32 i = 0; i < inputs->count; i++) {
		const ClusterWalInputV1 *item = &inputs->items[i];
		const ClusterControlRootSnapshot *root = &item->checkpoint;

		if (item->kind != CLUSTER_WAL_INPUT_CHECKPOINT
			|| !inputs_source_selected(source, &item->source))
			continue;
		/* CLOSED certifies DATA, not executor exit. Only the original recovery
		 * terminal combines the isolated writer with fully durable replay. */
		if (root->lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE
			|| (root->root_flags & required) != required || root->checkpoint_tli != source->timeline
			|| root->tail_tli != source->timeline || root->recovered_tli != source->timeline
			|| root->checkpoint_lower_lsn == InvalidXLogRecPtr
			|| root->validated_tail_lsn_exclusive <= root->checkpoint_lower_lsn
			|| root->recovered_through_lsn_exclusive != root->validated_tail_lsn_exclusive
			|| root->tail_last_record_lsn < root->checkpoint_lower_lsn
			|| root->tail_last_record_lsn >= root->validated_tail_lsn_exclusive
			|| root->recovered_last_record_lsn != root->tail_last_record_lsn
			|| root->recovered_last_record_crc32c != root->tail_last_record_crc32c)
			return false;
		*out_end = root->recovered_through_lsn_exclusive;
		return true;
	}
	return false;
}

bool
cluster_wal_inputs_recovered_owner_v1(ClusterWalInputsV1 *inputs, int32 node,
									ClusterWalSourceRef *out)
{
	const ClusterWalInputV1 *current = NULL;
	bool seen[CLUSTER_WAL_INPUTS_MAX] = { false };
	uint32 count = 0;
	XLogRecPtr end;
	const uint32 closed_flags = CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID
		| CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID
		| CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out == NULL || !inputs_current(inputs) || inputs->cold || node < 0
		|| node >= CLUSTER_CONTROL_ROOT_RECORD_COUNT)
		return false;
	for (uint32 i = 0; i < inputs->count; i++) {
		const ClusterWalInputV1 *item = &inputs->items[i];
		if (item->source.claim.identity.origin_node_id != node)
			continue;
		if (item->kind == CLUSTER_WAL_INPUT_INITIALIZER_TERMINAL) {
			/* The original selected terminal reader already qualified durable
			 * isolation/closure. It cannot stand in for an installed writer. */
			if (item->current || item->terminal.checkpoint_records != 0
				|| item->terminal.unsupported_records != 0)
				return false;
			continue;
		}
		if (item->kind != CLUSTER_WAL_INPUT_CHECKPOINT)
			return false;
		count++;
		if (item->current) {
			if (current != NULL)
				return false;
			current = item;
		}
	}
	if (current == NULL
		|| !cluster_wal_inputs_recovered_prefix_v1(inputs, &current->source, &end))
		return false;
	for (uint32 i = 0; i < inputs->count; i++) {
		const ClusterWalInputV1 *item = &inputs->items[i];
		const ClusterControlRootSnapshot *root = &item->checkpoint;
		uint64 offset;
		if (item->source.claim.identity.origin_node_id != node
			|| item->kind != CLUSTER_WAL_INPUT_CHECKPOINT)
			continue;
		if (item->source.claim.identity.root_lineage_seq == 0
			|| item->source.claim.identity.root_lineage_seq
				   > current->source.claim.identity.root_lineage_seq)
			return false;
		offset = current->source.claim.identity.root_lineage_seq
			- item->source.claim.identity.root_lineage_seq;
		if (offset >= count || seen[offset])
			return false;
		seen[offset] = true;
		if (item == current)
			continue;
		/* A selected successor exists for every retained predecessor. Native
		 * INSTALL retained this chain only after the original exit/recovery
		 * proof. CLOSED without that successor is deliberately insufficient. */
		if (root->lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED) {
			if (root->checkpoint_source_kind != CLUSTER_CONTROL_ROOT_CHECKPOINT_NATIVE_V1
				|| (root->root_flags & closed_flags) != closed_flags
				|| root->tail_tli != item->source.timeline
				|| root->checkpoint_lower_lsn == InvalidXLogRecPtr
				|| root->tail_last_record_lsn != item->checkpoint_start
				|| root->tail_last_record_lsn < root->checkpoint_lower_lsn
				|| root->tail_last_record_lsn >= root->validated_tail_lsn_exclusive)
				return false;
		} else if (!cluster_wal_inputs_recovered_prefix_v1(inputs, &item->source, &end))
			return false;
	}
	*out = current->source;
	return true;
}

bool
cluster_wal_inputs_local_predecessor_retired_v1(ClusterWalInputsV1 *inputs,
												const ClusterWalSourceRef *predecessor,
												const ClusterWalSourceRef *writer)
{
	const ClusterWalInputV1 *current = NULL, *old = NULL;
	ClusterQvotecPriorExitObservation exit;
	uint64 lineage, lower;
	bool exit_observed = false;

	if (!inputs_current(inputs) || inputs->cold || predecessor == NULL || writer == NULL
		|| writer->claim.identity.origin_node_id != cluster_node_id
		|| writer->claim.identity.origin_owner_incarnation == 0
		|| writer->claim.identity.origin_owner_incarnation != cluster_qvotec_get_self_incarnation()
		|| predecessor->claim.identity.origin_node_id != cluster_node_id
		|| predecessor->claim.identity.origin_owner_incarnation == 0
		|| predecessor->claim.identity.origin_owner_incarnation
			   == writer->claim.identity.origin_owner_incarnation
		|| cluster_wal_inputs_revalidate_v1(inputs) != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return false;
	for (uint32 i = 0; i < inputs->count; i++) {
		const ClusterWalInputV1 *item = &inputs->items[i];
		if (item->current && inputs_source_selected(writer, &item->source))
			current = item;
		if (!item->current && inputs_source_selected(predecessor, &item->source))
			old = item;
	}
	if (current == NULL || old == NULL || current->kind != CLUSTER_WAL_INPUT_CHECKPOINT
		|| current->checkpoint.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN)
		return false;
	/* This alternative was constructed only by the original selected
	 * initializer-terminal consumer, which validates durable isolation and
	 * closure. A terminal is never inferred from an absent checkpoint. */
	if (old->kind == CLUSTER_WAL_INPUT_INITIALIZER_TERMINAL)
		return old->terminal.checkpoint_records == 0 && old->terminal.unsupported_records == 0;
	lineage = current->source.claim.identity.root_lineage_seq;
	lower = old->source.claim.identity.root_lineage_seq;
	if (lower == 0 || lower >= lineage || lineage - lower > inputs->count)
		return false;
	/* Native INSTALL atomically selects the successor and retains the full
	 * CLOSED predecessor union after consuming the original clean exit.
	 * Anchor that chain at the actual prior-exit witness for this boot; no
	 * numeric boot comparison or unselected archival file is a proof. */
	while (lineage > lower) {
		const ClusterWalInputV1 *parent = NULL;
		lineage--;
		for (uint32 i = 0; i < inputs->count; i++) {
			const ClusterWalInputV1 *item = &inputs->items[i];
			if (item->current || item->kind != CLUSTER_WAL_INPUT_CHECKPOINT
				|| item->source.claim.identity.origin_node_id != cluster_node_id
				|| item->source.claim.identity.root_lineage_seq != lineage)
				continue;
			if (parent != NULL)
				return false;
			parent = item;
		}
		if (parent == NULL || parent->checkpoint.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED
			|| parent->checkpoint.checkpoint_source_kind
				   != CLUSTER_CONTROL_ROOT_CHECKPOINT_NATIVE_V1
			|| (lineage == lower && parent != old))
			return false;
		if (!exit_observed) {
			if (!cluster_qvotec_prior_exit_observe(
					cluster_node_id, parent->source.claim.identity.origin_owner_incarnation,
					writer->claim.identity.origin_owner_incarnation, &exit))
				return false;
			exit_observed = true;
		}
	}
	return exit_observed && inputs_current(inputs);
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
	for (uint32 i = 0; i < CLUSTER_WAL_INPUTS_MAX; i++)
		cluster_wal_cut_release_v1(&(*inputs)->remote[i]);
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

/* Claim paths are immutable within the held WALR set. Their physical I/O
 * must not extend the cluster-wide CF interval; the caller rechecks the
 * complete ROOT after this scan before publishing any assembled input. */
static ClusterControlRootResult
inputs_claims(ClusterWalInputsV1 *inputs)
{
	if (cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	for (uint32 i = 0; i < inputs->count; i++) {
		ClusterWalThreadClaimV2 claim;
		ClusterControlRootResult result;
		if (!inputs_current(inputs))
			return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		result = cluster_wal_claim_v2_read(cluster_wal_threads_dir, &inputs->items[i].source.claim,
										   &claim);
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
	uint64 records;
} WalInputsVisit;

static bool
inputs_visit_record(struct XLogReaderState *reader, void *arg)
{
	WalInputsVisit *visit = arg;
	if (!inputs_current(visit->inputs) || cluster_cf_held(ShareLock)
		|| cluster_cf_held(ExclusiveLock))
		return false;
	if (MyBackendType == B_CHECKPOINTER && (visit->records++ % 64) == 0)
		AbsorbSyncRequests();
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
	/* An OPEN source belongs to the live writer's fixed-cut reader. Merely
	 * choosing the retained visitor must not poison this still-held scope. */
	if (item->kind == CLUSTER_WAL_INPUT_CHECKPOINT
		&& item->checkpoint.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN)
		return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
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
	ClusterWalWriterSampleV1 sample;
	ClusterControlRootResult result;

	result = cluster_wal_writer_begin(item->source.timeline, &current);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (inputs->local_cut.complete_end != InvalidXLogRecPtr) {
		if (inputs->local_index != index
			|| memcmp(&current, &inputs->local_cut.writer, sizeof(current)) != 0)
			return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		result = cluster_wal_writer_check(&inputs->local_cut.writer);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			*out = inputs->local_cut;
		return result;
	}
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
	sample.writer = inputs->local_writer;
	sample.reserved_end = inputs->local_minimum;
	result = cluster_wal_writer_flush_sample_v1(&sample, out);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (memcmp(&out->writer, &inputs->local_writer, sizeof(out->writer)) != 0)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (out->complete_end == InvalidXLogRecPtr || out->flushed_end < out->complete_end)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	if (out->complete_end != inputs->local_minimum)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	inputs->local_cut = *out;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_wal_inputs_prepare_live_local_v1(ClusterWalInputsV1 *inputs, uint32 index,
										 XLogRecPtr *out_complete_end)
{
	ClusterWalWriterFlushV1 native;
	const ClusterWalInputV1 *item;
	ClusterControlRootResult result;

	if (out_complete_end == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	*out_complete_end = InvalidXLogRecPtr;
	if (!inputs_current(inputs) || inputs->cold || index >= inputs->count || CritSectionCount != 0
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
		*out_complete_end = native.complete_end;
	else if (result != CLUSTER_CONTROL_ROOT_RECONFIG_WAIT)
		inputs->stale = true;
	return result;
}

ClusterControlRootResult
cluster_wal_inputs_visit_live_local_v1(ClusterWalInputsV1 *inputs, uint32 index,
									   ClusterWalRecordVisitor visitor, void *arg,
									   ClusterWalTailObservation *out)
{
	ClusterWalTailObservation observed = { 0 };
	WalInputsVisit visit = { inputs, visitor, arg };
	ClusterControlRootResult result;
	const ClusterWalInputV1 *item;
	XLogRecPtr end;

	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(out, 0, sizeof(*out));
	result = cluster_wal_inputs_prepare_live_local_v1(inputs, index, &end);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	item = &inputs->items[index];
	PG_TRY();
	{
		result = cluster_wal_flushed_prefix_visit(
			cluster_wal_threads_dir, &item->source, wal_segment_size,
			item->checkpoint.checkpoint_lower_lsn, inputs->local_minimum, end,
			inputs->local_cut.flushed_end, item->checkpoint_start,
			item->checkpoint.checkpoint_record_crc32c, inputs_visit_record, &visit, &observed);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			result = cluster_wal_writer_check(&inputs->local_cut.writer);
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
inputs_remote_sample(ClusterWalInputsV1 *inputs, uint32 index, ClusterWalWriterFlushV1 *out)
{
	const ClusterWalInputV1 *item = &inputs->items[index];
	ClusterControlRootResult result;
	ClusterWalSourceRef selected;
	if (inputs->remote[index] == NULL) {
		result = cluster_wal_cut_begin_v1(&item->source, &inputs->remote[index]);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
	}
	result = cluster_wal_cut_poll_v1(inputs->remote[index], out);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	selected = out->writer.ref;
	if (selected.claim.max_config_generation == 0
		|| selected.claim.max_config_generation > item->source.claim.max_config_generation)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	selected.claim.max_config_generation = item->source.claim.max_config_generation;
	if (memcmp(&selected, &item->source, sizeof(selected)) != 0
		|| out->writer.startup_first_lsn != 0 || out->writer.epoch == 0)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (out->complete_end <= item->checkpoint_start
		|| out->complete_end < item->checkpoint.validated_tail_lsn_exclusive
		|| out->flushed_end < out->complete_end)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_wal_inputs_prepare_live_v1(ClusterWalInputsV1 *inputs, uint32 index,
								   XLogRecPtr *out_complete_end)
{
	const ClusterWalInputV1 *item;
	ClusterWalWriterFlushV1 native;
	ClusterControlRootResult result;
	if (out_complete_end == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	*out_complete_end = 0;
	if (!inputs_current(inputs) || inputs->cold || index >= inputs->count || CritSectionCount != 0
		|| ShutdownRequestPending || RecoveryInProgress())
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	item = &inputs->items[index];
	if (!item->current || item->kind != CLUSTER_WAL_INPUT_CHECKPOINT
		|| item->checkpoint.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (item->source.claim.identity.origin_node_id == cluster_node_id)
		return cluster_wal_inputs_prepare_live_local_v1(inputs, index, out_complete_end);
	PG_TRY();
	{
		result = cluster_wal_inputs_revalidate_v1(inputs);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			result = inputs_remote_sample(inputs, index, &native);
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
		*out_complete_end = native.complete_end;
	else if (result != CLUSTER_CONTROL_ROOT_RECONFIG_WAIT)
		inputs->stale = true;
	return result;
}

ClusterControlRootResult
cluster_wal_inputs_visit_live_v1(ClusterWalInputsV1 *inputs, uint32 index,
								 ClusterWalRecordVisitor visitor, void *arg,
								 ClusterWalTailObservation *out)
{
	ClusterWalTailObservation observed = { 0 };
	ClusterWalWriterFlushV1 native, after;
	WalInputsVisit visit = { inputs, visitor, arg };
	ClusterControlRootResult result;
	const ClusterWalInputV1 *item;
	XLogRecPtr end;
	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(out, 0, sizeof(*out));
	if (inputs_current(inputs) && index < inputs->count
		&& inputs->items[index].source.claim.identity.origin_node_id == cluster_node_id)
		return cluster_wal_inputs_visit_live_local_v1(inputs, index, visitor, arg, out);
	result = cluster_wal_inputs_prepare_live_v1(inputs, index, &end);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	item = &inputs->items[index];
	PG_TRY();
	{
		result = inputs_remote_sample(inputs, index, &native);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY && native.complete_end != end)
			result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			result = cluster_wal_flushed_prefix_visit(
				cluster_wal_threads_dir, &item->source, wal_segment_size,
				item->checkpoint.checkpoint_lower_lsn, end, end, native.flushed_end,
				item->checkpoint_start, item->checkpoint.checkpoint_record_crc32c,
				inputs_visit_record, &visit, &observed);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			result = inputs_remote_sample(inputs, index, &after);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
			&& memcmp(&native, &after, sizeof(native)) != 0)
			result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
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

typedef struct WalContributionWork {
	ClusterThreadRecoveryFabricPlanV1 *plan;
	ClusterWalCensusVisitorV1 census;
	void *census_arg;
	RfContributorStreamCutV1 cuts[CLUSTER_WAL_INPUTS_MAX];
	ClusterWalSourceRef sources[CLUSTER_WAL_INPUTS_MAX];
	uint32 indices[CLUSTER_WAL_INPUTS_MAX];
	uint32 count;
	uint16 participant;
	uint64 records;
	RfPageProofDetailV1 detail;
} WalContributionWork;

static ClusterControlRootResult
inputs_contribution_cuts(ClusterWalInputsV1 *inputs, WalContributionWork *work)
{
	ClusterControlRootResult result = cluster_wal_inputs_revalidate_v1(inputs);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	work->count = inputs->count;
	for (uint32 i = 0; i < work->count; i++) {
		const ClusterWalInputV1 *item = &inputs->items[i];
		RfContributorStreamCutV1 cut = { 0 };
		uint32 at = i;
		cut.failed_thread = item->source.claim.identity.origin_thread_id;
		cut.origin_owner_incarnation = item->source.claim.identity.origin_owner_incarnation;
		cut.timeline_id = item->source.timeline;
		cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		if (cut.origin_owner_incarnation == 0)
			return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
		if (item->kind == CLUSTER_WAL_INPUT_CHECKPOINT) {
			cut.scan_begin_inclusive = item->checkpoint.checkpoint_lower_lsn;
			cut.scan_end_exclusive = item->checkpoint.validated_tail_lsn_exclusive;
			if (item->checkpoint.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN) {
				result = cluster_wal_inputs_prepare_live_v1(inputs, i, &cut.scan_end_exclusive);
				if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
					return result;
			}
		} else if (item->kind == CLUSTER_WAL_INPUT_INITIALIZER_TERMINAL) {
			cut.scan_begin_inclusive = item->first_segment + SizeOfXLogLongPHD;
			cut.scan_end_exclusive = item->terminal.tail.complete_end;
			if (item->terminal.tail.records == 0) {
				cut.flags |= RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY;
				cut.scan_end_exclusive = cut.scan_begin_inclusive;
			}
		} else
			return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
		/* History and checkpoint-less terminal lists can interleave writer
		 * incarnations. Keep every input, in the graph's full-source order. */
		while (at > 0 && rf_contributor_cut_precedes_v1(&cut, &work->cuts[at - 1])) {
			work->cuts[at] = work->cuts[at - 1];
			work->sources[at] = work->sources[at - 1];
			work->indices[at] = work->indices[at - 1];
			at--;
		}
		work->cuts[at] = cut;
		work->sources[at] = item->source;
		work->indices[at] = i;
	}
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static bool
inputs_contribution_record(XLogReaderState *reader, void *arg)
{
	WalContributionWork *work = arg;
	if ((work->records % 64) == 0) {
		CHECK_FOR_INTERRUPTS();
		if (ShutdownRequestPending) {
			work->detail = RF_PAGE_PROOF_DETAIL_CANCELLED;
			return false;
		}
	}
	if (work->records == UINT64_MAX) {
		work->detail = RF_PAGE_PROOF_DETAIL_CAPACITY;
		return false;
	}
	work->detail = work->census != NULL
					   ? work->census(reader, &work->sources[work->participant],
									  &work->cuts[work->participant], work->census_arg)
					   : cluster_thread_recovery_fabric_plan_feed_record_v1(work->plan, reader,
																			work->participant);
	if (work->detail != RF_PAGE_PROOF_DETAIL_OK)
		return false;
	work->records++;
	return true;
}

static ClusterControlRootResult
inputs_contribution_build(ClusterWalInputsV1 *inputs, bool space_active, WalContributionWork *work)
{
	ClusterThreadRecoveryFabricPlanRequestV1 request = { 0 };
	ClusterControlRootResult result = inputs_contribution_cuts(inputs, work);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	for (uint32 i = 0; i < work->count; i++) {
		const ClusterWalSourceRef *source = &work->sources[i];
		if (!cluster_wal_claim_v2_ref_valid(&source->claim)
			|| source->claim.identity.system_identifier != inputs->system_identifier
			|| memcmp(source->claim.identity.storage_uuid, inputs->storage_uuid, 16) != 0
			|| source->claim.database_incarnation == 0
			|| source->claim.database_incarnation != work->sources[0].claim.database_incarnation) {
			work->detail = RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
			return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
		}
	}
	request.system_identifier = inputs->system_identifier;
	memcpy(request.storage_uuid, inputs->storage_uuid, 16);
	request.physical_cuts = work->cuts;
	request.sources = work->sources;
	request.participant_count = work->count;
	request.retention_binding_cookie = (uint64)(uintptr_t)inputs;
	request.space_active = space_active;
	/* NULL redo_starts deliberately keeps every retained contribution.
	 * This graph cannot authorize replay; native recovery has a different
	 * owner and must select its same-anchor actual redo start. */
	if (work->census == NULL) {
		work->detail = cluster_thread_recovery_fabric_plan_create_v1(&request, &work->plan);
		if (work->detail != RF_PAGE_PROOF_DETAIL_OK)
			return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		if (!cluster_thread_recovery_fabric_bind_database_v1(
				work->plan, work->sources[0].claim.database_incarnation)) {
			work->detail = RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
			return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
		}
	}
	for (uint32 i = 0; i < work->count; i++) {
		uint32 index = work->indices[i];
		const ClusterWalInputV1 *item = &inputs->items[index];
		ClusterWalTailObservation observed;
		uint64 previous = work->records;
		bool empty = (work->cuts[i].flags & RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY) != 0;
		work->participant = i;
		if (item->kind == CLUSTER_WAL_INPUT_CHECKPOINT
			&& item->checkpoint.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN)
			result = cluster_wal_inputs_visit_live_v1(inputs, index, inputs_contribution_record,
													  work, &observed);
		else
			result = cluster_wal_inputs_visit_retained_v1(inputs, index, inputs_contribution_record,
														  work, &observed);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
		if (observed.records != work->records - previous
			|| (empty ? observed.records != 0 || observed.complete_end != 0
					  : observed.records == 0
							|| observed.complete_end != work->cuts[i].scan_end_exclusive)
			|| (observed.records != 0
				&& observed.database_incarnation != work->sources[i].claim.database_incarnation)) {
			work->detail = RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
			return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		}
	}
	result = cluster_wal_inputs_revalidate_v1(inputs);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (work->census == NULL) {
		work->detail = cluster_thread_recovery_fabric_plan_seal_v1(work->plan);
		if (work->detail != RF_PAGE_PROOF_DETAIL_OK)
			return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	}
	result = cluster_wal_inputs_revalidate_v1(inputs);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& inputs->local_cut.complete_end != InvalidXLogRecPtr)
		result = cluster_wal_writer_check(&inputs->local_cut.writer);
	for (uint32 i = 0; i < inputs->count && result == CLUSTER_CONTROL_ROOT_OK_PRIMARY; i++) {
		if (inputs->remote[i] != NULL) {
			ClusterWalWriterFlushV1 current;
			result = inputs_remote_sample(inputs, i, &current);
		}
	}
	return result;
}

static ClusterControlRootResult
inputs_scan_contributions(ClusterWalInputsV1 *inputs, bool space_active,
						  ClusterWalCensusVisitorV1 census, void *census_arg,
						  ClusterThreadRecoveryFabricPlanV1 **out_plan, uint64 *out_record_count,
						  RfPageProofDetailV1 *out_detail)
{
	WalContributionWork *work;
	ClusterControlRootResult result;
	if (out_plan == NULL || out_record_count == NULL || out_detail == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	*out_plan = NULL;
	*out_record_count = 0;
	*out_detail = RF_PAGE_PROOF_DETAIL_OK;
	if (!inputs_current(inputs) || inputs->cold || CritSectionCount != 0 || ShutdownRequestPending)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	work = palloc0(sizeof(*work));
	work->census = census;
	work->census_arg = census_arg;
	PG_TRY();
	{
		result = inputs_contribution_build(inputs, space_active, work);
	}
	PG_CATCH();
	{
		cluster_thread_recovery_fabric_plan_destroy_v1(&work->plan);
		pfree(work);
		inputs->stale = true;
		PG_RE_THROW();
	}
	PG_END_TRY();
	*out_detail = work->detail;
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		*out_plan = work->plan;
		*out_record_count = work->records;
	} else {
		cluster_thread_recovery_fabric_plan_destroy_v1(&work->plan);
		if (result != CLUSTER_CONTROL_ROOT_RECONFIG_WAIT)
			inputs->stale = true;
	}
	pfree(work);
	return result;
}

ClusterControlRootResult
cluster_wal_inputs_contributions_v1(ClusterWalInputsV1 *inputs, bool space_active,
									ClusterThreadRecoveryFabricPlanV1 **out_plan,
									uint64 *out_record_count, RfPageProofDetailV1 *out_detail)
{
	return inputs_scan_contributions(inputs, space_active, NULL, NULL, out_plan, out_record_count,
									 out_detail);
}

ClusterControlRootResult
cluster_wal_inputs_census_v1(ClusterWalInputsV1 *inputs, ClusterWalCensusVisitorV1 visitor,
							 void *arg, uint64 *out_record_count, RfPageProofDetailV1 *out_detail)
{
	ClusterThreadRecoveryFabricPlanV1 *unused = NULL;
	if (visitor == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	return inputs_scan_contributions(inputs, true, visitor, arg, &unused, out_record_count,
									 out_detail);
}

static ClusterControlRootResult
inputs_begin(const uint8 storage_uuid[16], uint64 system_identifier, bool cold,
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
		|| !inputs_io_role(cold))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	inputs = palloc0(sizeof(*inputs));
	inputs->pid = getpid();
	inputs->owner = CurrentResourceOwner;
	inputs->cold = cold;
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
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			result = inputs_claims(inputs);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			result = inputs_root(inputs, work, false);
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
cluster_wal_inputs_begin_v1(const uint8 storage_uuid[16], uint64 system_identifier,
							ClusterWalInputsV1 **out)
{
	return inputs_begin(storage_uuid, system_identifier, false, out);
}

ClusterControlRootResult
cluster_wal_inputs_cold_begin_v1(const uint8 storage_uuid[16], uint64 system_identifier,
								 ClusterWalInputsV1 **out)
{
	return inputs_begin(storage_uuid, system_identifier, true, out);
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

ClusterControlRootResult
cluster_wal_inputs_suspend_v1(ClusterWalInputsV1 *inputs)
{
	if (!inputs_owned(inputs) || !inputs_io_role(inputs->cold) || inputs->stale)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	if (inputs->suspended)
		return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	if (!inputs_current(inputs))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (cluster_wal_read_pin_release_v1(&inputs->pin) != CLUSTER_WALR_RELEASE_CONFIRMED)
		elog(FATAL, "could not suspend WAL input retention owner");
	inputs->suspended = true;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_wal_inputs_wait_failed_v1(ClusterWalInputsV1 *inputs)
{
	WalInputsWork *work;
	ClusterControlRootResult result;
	if (!inputs_owned(inputs) || !inputs_io_role(inputs->cold) || CritSectionCount != 0
		|| !cluster_enabled || !cluster_shared_config)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	/* Failure remains terminal even when the ROOT still matches. No WAL
	 * or plan consumer can use the scope after this call. */
	inputs->stale = true;
	if (cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	for (uint32 i = 0; i < CLUSTER_WAL_INPUTS_MAX; i++)
		cluster_wal_cut_release_v1(&inputs->remote[i]);
	if (inputs->pin != NULL
		&& cluster_wal_read_pin_release_v1(&inputs->pin) != CLUSTER_WALR_RELEASE_CONFIRMED)
		elog(FATAL, "could not release failed WAL input retention owner");
	inputs->suspended = true;
	work = palloc0(sizeof(*work));
	PG_TRY();
	{
		result = inputs_root(inputs, work, false);
		inputs_unlock(work);
	}
	PG_CATCH();
	{
		inputs_unlock(work);
		pfree(work);
		PG_RE_THROW();
	}
	PG_END_TRY();
	pfree(work);
	return result;
}

ClusterControlRootResult
cluster_wal_inputs_resume_v1(ClusterWalInputsV1 *inputs)
{
	ClusterWalPinResult pinned;
	ClusterControlRootResult result;
	if (!inputs_owned(inputs) || !inputs_io_role(inputs->cold) || inputs->stale)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (!inputs->suspended)
		return cluster_wal_inputs_revalidate_v1(inputs);
	if (!cluster_enabled || !cluster_shared_config || cluster_cf_held(ShareLock)
		|| cluster_cf_held(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	PG_TRY();
	{
		pinned
			= cluster_wal_read_pin_acquire_v1(inputs->threads, inputs->thread_count, &inputs->pin);
		if (pinned != CLUSTER_WAL_PIN_OK)
			result = pinned == CLUSTER_WAL_PIN_RELEASE_UNCERTAIN
						 ? CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN
						 : CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
		else {
			inputs->suspended = false;
			result = cluster_wal_inputs_revalidate_v1(inputs);
		}
	}
	PG_CATCH();
	{
		inputs->stale = true;
		if (inputs->pin != NULL
			&& cluster_wal_read_pin_release_v1(&inputs->pin) != CLUSTER_WALR_RELEASE_CONFIRMED)
			elog(FATAL, "could not release resumed WAL input retention owner");
		PG_RE_THROW();
	}
	PG_END_TRY();
	/* A refused resume must not turn waiting for CF or a changed ROOT into
	 * another long-lived WALR-S grant. No result can consume the old plan. */
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		if (inputs->pin != NULL
			&& cluster_wal_read_pin_release_v1(&inputs->pin) != CLUSTER_WALR_RELEASE_CONFIRMED)
			elog(FATAL, "could not release refused WAL input retention owner");
		inputs->suspended = true;
	}
	return result;
}

ClusterControlRootResult
cluster_wal_inputs_root_token_v1(ClusterWalInputsV1 *inputs, ClusterControlRootFileToken *out)
{
	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(out, 0, sizeof(*out));
	if (!inputs_current(inputs))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	*out = inputs->token;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
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
