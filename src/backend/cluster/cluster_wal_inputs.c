/* PGRAC: full ROOT-selected input census; never replay or PI retirement.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog.h"
#include "access/xlogrecovery.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ir.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_recovery_plan.h"
#include "cluster/cluster_recovery_tt_record.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_wal_inputs.h"
#include "cluster/cluster_wal_retention.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/cluster_wal_writer.h"
#include "cluster/cluster_wal_cut.h"
#include "cluster/storage/cluster_shared_fs.h"
#include "common/cryptohash.h"
#include "miscadmin.h"
#include "postmaster/bgwriter.h"
#include "postmaster/interrupt.h"
#include "utils/resowner.h"

#include "cluster_control_root_private.h"
#include "cluster_recovery_anchor_private.h"

typedef enum WalInputsPurpose {
	WAL_INPUTS_ONLINE,
	WAL_INPUTS_COLD_REPLAY,
	WAL_INPUTS_TX_CLOSURE,
	WAL_INPUTS_CTRC_CENSUS
} WalInputsPurpose;

typedef struct ClusterRecoveryTxIoScopeV1 {
	const struct ClusterRecoveryStartV1 *start;
	const ClusterColdTypedV1 *inputs;
	const ClusterColdTopTxCensusEntryV1 *subject;
	uint64 generation, serial;
	ClusterColdTopTxCensusEntryV1 version;
	ClusterControlRootFileToken root;
	ClusterRecoverySerialGuard ir;
	bool ir_ready;
	bool ir_revoked;
} ClusterRecoveryTxIoScopeV1;

typedef struct ClusterRecoveryCtrcIoScopeV1 {
	const struct ClusterRecoveryStartV1 *start;
	const ClusterColdTypedV1 *inputs;
	const ClusterColdCtrcWorkV1 *work;
	uint64 work_generation;
	ClusterRecoveryCheckpointOwnerV1 owner;
	ClusterControlRootFileToken root;
	int namespace_fd;
	bool namespace_verified;
	char namespace_path[MAXPGPATH];
	struct stat namespace_identity;
} ClusterRecoveryCtrcIoScopeV1;

struct ClusterWalInputsV1 {
	uint64 system_identifier;
	uint8 storage_uuid[16];
	ClusterControlRootFileToken token;
	ClusterWalReadPinV1 *pin;
	ResourceOwner owner;
	pid_t pid;
	bool stale;
	bool suspended;
	WalInputsPurpose purpose;
	ClusterRecoveryTxIoScopeV1 tx;
	ClusterRecoveryCtrcIoScopeV1 ctrc;
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

/* A locator, not another owner or a grant. Keep the native incarnation outside
 * borrowed allocations so stale/forged views are rejected before dereference. */
static ClusterRecoveryTxRetentionV1 active_tx_retention;

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
inputs_scope_role(const ClusterWalInputsV1 *inputs)
{
	const ClusterColdTypedV1 *typed;
	uint64 generation, serial;
	if (inputs->purpose == WAL_INPUTS_CTRC_CENSUS) {
		ClusterRecoveryCheckpointOwnerV1 owner;

		/* WorkCurrent locates the original pointer before dereferencing it.
		 * Snapshot the publication version too: the allocator may reuse it. */
		if (MyBackendType != B_STARTUP || RecoveryInProgress()
			|| !ClusterColdRecoveryCtrcWorkCurrent(inputs->ctrc.start, inputs->ctrc.work)
			|| inputs->ctrc.work->generation != inputs->ctrc.work_generation
			|| !ClusterColdRecoveryTxCheckpointOwnerV1(inputs->ctrc.start, &owner)
			|| memcmp(&owner, &inputs->ctrc.owner, sizeof(owner)) != 0)
			return false;
		typed = ClusterColdRecoveryTxInputs(inputs->ctrc.start);
		return typed != NULL && typed == inputs->ctrc.inputs && typed->census_root_captured
			&& memcmp(&typed->census_root, &inputs->ctrc.root, sizeof(inputs->ctrc.root)) == 0;
	}
	if (inputs->purpose != WAL_INPUTS_TX_CLOSURE)
		return inputs_io_role(inputs->purpose == WAL_INPUTS_COLD_REPLAY);
	if (MyBackendType != B_STARTUP || RecoveryInProgress()
		|| !ClusterColdRecoveryTxSubjectCurrent(inputs->tx.start, inputs->tx.subject,
			&generation, &serial)
		|| generation != inputs->tx.generation || serial != inputs->tx.serial)
		return false;
	typed = ClusterColdRecoveryTxInputs(inputs->tx.start);
	return typed != NULL && typed == inputs->tx.inputs && typed->census_root_captured
		&& memcmp(&typed->census_root, &inputs->tx.root, sizeof(inputs->tx.root)) == 0
		&& memcmp(inputs->tx.subject, &inputs->tx.version, sizeof(inputs->tx.version)) == 0;
}

static bool
inputs_current(ClusterWalInputsV1 *inputs)
{
	return cluster_enabled && cluster_shared_config && inputs_owned(inputs)
		   && inputs_scope_role(inputs) && !inputs->stale && inputs->thread_count > 0
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
	if (out_end == NULL || source == NULL || !inputs_current(inputs) || (inputs->purpose != WAL_INPUTS_ONLINE)
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
	const uint32 closed_flags
		= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID
		  | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID | CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out == NULL || !inputs_current(inputs) || (inputs->purpose != WAL_INPUTS_ONLINE) || node < 0
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
	if (current == NULL || !cluster_wal_inputs_recovered_prefix_v1(inputs, &current->source, &end))
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

	if (!inputs_current(inputs) || (inputs->purpose != WAL_INPUTS_ONLINE) || predecessor == NULL || writer == NULL
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
	/* The original creation input was never an admitted writer and therefore
	 * has no prior-exit vote. Its actual INITIALIZED INSTALL publishes a
	 * separate, epoch-bound qualification; bind it to the immutable restart
	 * source and the complete selected inputs. CLOSED/lineage alone cannot
	 * establish this alternative, and CLEAN INSTALL never qualifies here. */
	if (lower == 1 && lineage == 2 && old->kind == CLUSTER_WAL_INPUT_CHECKPOINT
		&& old->checkpoint.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED
		&& old->checkpoint.checkpoint_source_kind == CLUSTER_CONTROL_ROOT_CHECKPOINT_NATIVE_V1) {
		ClusterWalSourceRef restart;
		uint64 epoch = cluster_epoch_get_current();

		if (cluster_wal_thread_initialized_writer_matches(writer, epoch)
			&& cluster_wal_thread_restart_v2_ref(&restart)
			&& memcmp(&restart, predecessor, sizeof(restart)) == 0)
			return inputs_current(inputs) && cluster_epoch_get_current() == epoch;
	}
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

static void
inputs_namespace_close(ClusterWalInputsV1 *inputs)
{
	int fd = inputs->ctrc.namespace_fd;

	inputs->ctrc.namespace_fd = -1;
	inputs->ctrc.namespace_verified = false;
	if (fd >= 0 && close(fd) != 0)
		elog(FATAL, "could not close CTRC input namespace owner");
}

/* Revoked Start/scope/proofs do not prevent exact native cleanup. Do not
 * drop the pin or borrowed proof owner before IR release is confirmed. */
static void
inputs_tx_serial_release(ClusterWalInputsV1 *inputs)
{
	if (!inputs->tx.ir.held && !inputs->tx.ir.release_uncertain)
		return;
	inputs->tx.ir_ready = false;
	inputs->tx.ir_revoked = true;
	if (cluster_recovery_serial_release(&inputs->tx.ir) != CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED)
		elog(FATAL, "could not release TX_CLOSURE recovery serial owner");
}

void
cluster_wal_inputs_release_v1(ClusterWalInputsV1 **inputs)
{
	if (inputs == NULL || *inputs == NULL)
		return;
	if (!inputs_owned(*inputs))
		elog(ERROR, "WAL input scope belongs to another resource owner");
	inputs_tx_serial_release(*inputs);
	inputs_namespace_close(*inputs);
	for (uint32 i = 0; i < CLUSTER_WAL_INPUTS_MAX; i++)
		cluster_wal_cut_release_v1(&(*inputs)->remote[i]);
	if ((*inputs)->pin != NULL
		&& cluster_wal_read_pin_release_v1(&(*inputs)->pin) != CLUSTER_WALR_RELEASE_CONFIRMED)
		elog(FATAL, "could not release WAL input retention owner");
	if (active_tx_retention.scope == *inputs)
		memset(&active_tx_retention, 0, sizeof(active_tx_retention));
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
	memcpy(&item->native_checkpoint, &native.checkPointCopy, sizeof(item->native_checkpoint));
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

/* A TX-closure scope starts from the original census ROOT, but the original
 * source may be archived into history by INSTALL before the scope is opened.
 * Re-select each original source from the post-INSTALL current/history cut by
 * its immutable claim and physical cut.  This accepts that one legal ROOT
 * transition, while refusing a missing, duplicated, or substituted source. */
static ClusterControlRootResult
inputs_tx_reselect_original(const ClusterWalInputsV1 *inputs,
							const ClusterColdTypedV1 *typed)
{
	if (inputs == NULL || typed == NULL || typed->participant_count == 0
		|| typed->participant_count > CLUSTER_COLD_MAX_PARTICIPANTS
		|| typed->replay_count > typed->participant_count)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;

	for (uint32 p = 0; p < typed->participant_count; p++) {
		const ClusterColdParticipantV1 *participant = &typed->participants[p];
		const ClusterWalSourceRef *source = &typed->sources[p];
		XLogRecPtr expected_redo = participant->native_redo;
		uint32 matches = 0;

		if (source->claim.identity.origin_thread_id != participant->thread_id
			|| source->claim.identity.origin_owner_incarnation != participant->owner_incarnation
			|| source->timeline != participant->timeline)
			return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		if (p >= typed->replay_count) {
			const ClusterColdXidProgressV1 *history = &typed->history_xid[p];

			if (!history->begun || history->failed
				|| history->source.thread_id != participant->thread_id
				|| history->source.owner_incarnation != participant->owner_incarnation
				|| history->source.timeline != participant->timeline)
				return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
			expected_redo = history->source.native_redo;
		}
		if (XLogRecPtrIsInvalid(participant->physical_lower)
			|| XLogRecPtrIsInvalid(participant->tail_end)
			|| XLogRecPtrIsInvalid(expected_redo)
			|| expected_redo < participant->physical_lower
			|| expected_redo > participant->tail_end)
		{
			return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		}

		for (uint32 i = 0; i < inputs->count; i++) {
			const ClusterWalInputV1 *item = &inputs->items[i];

			if (item->kind != CLUSTER_WAL_INPUT_CHECKPOINT
				|| !inputs_source_selected(source, &item->source)
				|| !cluster_control_root_identity_equal(&item->checkpoint.identity,
													&source->claim.identity)
				|| item->source.timeline != participant->timeline
				|| item->checkpoint.checkpoint_lower_lsn != participant->physical_lower
				|| item->checkpoint.validated_tail_lsn_exclusive != participant->tail_end
				|| item->native_redo != expected_redo)
				continue;
			if (++matches > 1)
				return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		}
		if (matches != 1)
		{
			return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		}
	}
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
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
	if (!inputs_current(inputs) || (inputs->purpose != WAL_INPUTS_ONLINE) || index >= inputs->count || CritSectionCount != 0
		|| ShutdownRequestPending || RecoveryInProgress())
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (cluster_semantic_normal_start_state() == CLUSTER_NORMAL_START_EXISTING_OTHER
		&& !cluster_semantic_activation_recovered_open_current())
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
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
	if (!inputs_current(inputs) || (inputs->purpose != WAL_INPUTS_ONLINE) || index >= inputs->count || CritSectionCount != 0
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
	ClusterWalCensusSourceVisitorV1 source_census;
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
	/* A cold reader cannot request a live writer's cut. Check the entire
	 * selected set first: no callback may observe a prefix while a later
	 * source is still OPEN. This preserves the original retained-reader
	 * WAIT polarity without borrowing its online sampling path. */
	if ((inputs->purpose != WAL_INPUTS_ONLINE)) {
		for (uint32 i = 0; i < work->count; i++) {
			const ClusterWalInputV1 *item = &inputs->items[i];
			if (item->kind == CLUSTER_WAL_INPUT_CHECKPOINT) {
				uint32 lifecycle = item->checkpoint.lifecycle;
				if (lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN)
					return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
				if (lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED
					&& lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
					&& lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE)
					return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
			} else if (item->kind != CLUSTER_WAL_INPUT_INITIALIZER_TERMINAL)
				return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
		}
	}
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
	request.purpose = RF_SIDE_PLAN_PI_CONTRIBUTIONS;
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
		if (work->source_census != NULL) {
			work->detail
				= work->source_census(&work->sources[i], &work->cuts[i], NULL, work->census_arg);
			if (work->detail != RF_PAGE_PROOF_DETAIL_OK)
				return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		}
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
		if (work->source_census != NULL) {
			work->detail = work->source_census(&work->sources[i], &work->cuts[i], &observed,
											   work->census_arg);
			if (work->detail != RF_PAGE_PROOF_DETAIL_OK)
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
						  ClusterWalCensusVisitorV1 census,
						  ClusterWalCensusSourceVisitorV1 source_census, bool cold,
						  void *census_arg, ClusterThreadRecoveryFabricPlanV1 **out_plan,
						  uint64 *out_record_count, RfPageProofDetailV1 *out_detail)
{
	WalContributionWork *work;
	ClusterControlRootResult result;
	if (out_plan == NULL || out_record_count == NULL || out_detail == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	*out_plan = NULL;
	*out_record_count = 0;
	*out_detail = RF_PAGE_PROOF_DETAIL_OK;
	if (!inputs_current(inputs) || (inputs->purpose != WAL_INPUTS_ONLINE) != cold || CritSectionCount != 0
		|| ShutdownRequestPending || (cold && (census == NULL || source_census == NULL)))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	work = palloc0(sizeof(*work));
	work->census = census;
	work->source_census = source_census;
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
	return inputs_scan_contributions(inputs, space_active, NULL, NULL, false, NULL, out_plan,
									 out_record_count, out_detail);
}

ClusterControlRootResult
cluster_wal_inputs_census_v1(ClusterWalInputsV1 *inputs, ClusterWalCensusVisitorV1 visitor,
							 void *arg, uint64 *out_record_count, RfPageProofDetailV1 *out_detail)
{
	ClusterThreadRecoveryFabricPlanV1 *unused = NULL;
	if (visitor == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	return inputs_scan_contributions(inputs, true, visitor, NULL, false, arg, &unused,
									 out_record_count, out_detail);
}

ClusterControlRootResult
cluster_wal_inputs_cold_census_v1(ClusterWalInputsV1 *inputs, ClusterWalCensusVisitorV1 visitor,
								  ClusterWalCensusSourceVisitorV1 source_visitor, void *arg,
								  uint64 *out_record_count, RfPageProofDetailV1 *out_detail)
{
	ClusterThreadRecoveryFabricPlanV1 *unused = NULL;
	return inputs_scan_contributions(inputs, true, visitor, source_visitor, true, arg, &unused,
									 out_record_count, out_detail);
}

static ClusterControlRootResult
inputs_begin(const uint8 storage_uuid[16], uint64 system_identifier, WalInputsPurpose purpose,
			 const struct ClusterRecoveryStartV1 *start,
			 const ClusterColdTopTxCensusEntryV1 *subject, const ClusterColdCtrcWorkV1 *ctrc_work,
			 ClusterWalInputsV1 **out)
{
	ClusterWalInputsV1 *inputs;
	WalInputsWork *work;
	ClusterControlRootResult result;
	ClusterWalPinResult pinned;
	const ClusterColdTypedV1 *typed = NULL;
	uint64 generation = 0, serial = 0;
	ClusterRecoveryCheckpointOwnerV1 ctrc_owner;

	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	*out = NULL;
	if (storage_uuid == NULL || system_identifier == 0 || !cluster_enabled || !cluster_shared_config
		|| !cluster_controlfile_shared_authority || CritSectionCount != 0
		|| CurrentResourceOwner == NULL || system_identifier != GetSystemIdentifier()
		|| (purpose != WAL_INPUTS_TX_CLOSURE && purpose != WAL_INPUTS_CTRC_CENSUS
			&& !inputs_io_role(purpose == WAL_INPUTS_COLD_REPLAY)))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (purpose == WAL_INPUTS_CTRC_CENSUS) {
		if (MyBackendType != B_STARTUP || RecoveryInProgress()
			|| !ClusterColdRecoveryCtrcWorkCurrent(start, ctrc_work)
			|| (typed = ClusterColdRecoveryTxInputs(start)) == NULL
			|| !typed->census_root_captured
			|| !ClusterColdRecoveryTxCheckpointOwnerV1(start, &ctrc_owner))
			return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	}
	if (purpose == WAL_INPUTS_TX_CLOSURE) {
		if (!ClusterColdRecoveryTxSubjectCurrent(start, subject, &generation, &serial)
			|| generation == 0 || serial == 0
			|| (typed = ClusterColdRecoveryTxInputs(start)) == NULL
			|| !typed->census_root_captured)
			return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	}
	if (cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	inputs = palloc0(sizeof(*inputs));
	inputs->ctrc.namespace_fd = -1;
	inputs->pid = getpid();
	inputs->owner = CurrentResourceOwner;
	inputs->purpose = purpose;
	if (purpose == WAL_INPUTS_CTRC_CENSUS) {
		inputs->ctrc.start = start;
		inputs->ctrc.inputs = typed;
		inputs->ctrc.work = ctrc_work;
		inputs->ctrc.work_generation = ctrc_work->generation;
		inputs->ctrc.owner = ctrc_owner;
		inputs->ctrc.root = typed->census_root;
		/* Bind the physical directory name/object before the original ROOT
		 * selection. Lazy opening must not adopt a later configuration or a
		 * replacement directory containing a copy of the same ROOT bytes.
		 * An unavailable namespace does not change existing WAL-only scope
		 * semantics; its separate physical accessor remains unavailable. */
		if (cluster_shared_data_dir == NULL
			|| strlcpy(inputs->ctrc.namespace_path, cluster_shared_data_dir,
				   sizeof(inputs->ctrc.namespace_path)) >= sizeof(inputs->ctrc.namespace_path)
			|| lstat(inputs->ctrc.namespace_path, &inputs->ctrc.namespace_identity) != 0)
			inputs->ctrc.namespace_path[0] = '\0';
	}
	if (purpose == WAL_INPUTS_TX_CLOSURE) {
		inputs->tx.start = start;
		inputs->tx.inputs = typed;
		inputs->tx.subject = subject;
		inputs->tx.generation = generation;
		inputs->tx.serial = serial;
		memcpy(&inputs->tx.version, subject, sizeof(*subject));
		inputs->tx.root = typed->census_root;
	}
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
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
			&& (purpose == WAL_INPUTS_TX_CLOSURE || purpose == WAL_INPUTS_CTRC_CENSUS))
			result = inputs_tx_reselect_original(inputs, typed);
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
	return inputs_begin(storage_uuid, system_identifier, WAL_INPUTS_ONLINE, NULL, NULL, NULL, out);
}

ClusterControlRootResult
cluster_wal_inputs_cold_begin_v1(const uint8 storage_uuid[16], uint64 system_identifier,
								 ClusterWalInputsV1 **out)
{
	return inputs_begin(storage_uuid, system_identifier, WAL_INPUTS_COLD_REPLAY, NULL, NULL, NULL, out);
}

ClusterControlRootResult
cluster_wal_inputs_begin_tx_closure_v1(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdTopTxCensusEntryV1 *subject, ClusterWalInputsV1 **out)
{
	const ClusterColdTypedV1 *typed;
	ClusterControlRootResult result;
	ClusterWalReadPinOwnerV1 owner;
	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	*out = NULL;
	typed = ClusterColdRecoveryTxInputs(start);
	if (typed == NULL || typed->participant_count == 0)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	result = inputs_begin(typed->sources[0].claim.identity.storage_uuid, typed->system_identifier,
		WAL_INPUTS_TX_CLOSURE, start, subject, NULL, out);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!cluster_wal_read_pin_owner_v1((*out)->pin, &owner)) {
		cluster_wal_inputs_release_v1(out);
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	}
	/* Native read-pin exclusivity makes this the original single live scope.
	 * Publishing this locator supplies no IR: ir_ready is still false. */
	memset(&active_tx_retention, 0, sizeof(active_tx_retention));
	active_tx_retention.scope = *out;
	active_tx_retention.serial = &(*out)->tx.ir;
	active_tx_retention.input_owner = owner;
	return result;
}

ClusterControlRootResult
cluster_wal_inputs_begin_ctrc_census_v1(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, ClusterWalInputsV1 **out)
{
	const ClusterColdTypedV1 *typed;

	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	*out = NULL;
	if (!ClusterColdRecoveryCtrcWorkCurrent(start, work)
		|| (typed = ClusterColdRecoveryTxInputs(start)) == NULL || typed->participant_count == 0)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	return inputs_begin(typed->sources[0].claim.identity.storage_uuid, typed->system_identifier,
		WAL_INPUTS_CTRC_CENSUS, start, NULL, work, out);
}

static bool
inputs_namespace_directory(const struct stat *st)
{
	return S_ISDIR(st->st_mode) && st->st_uid == geteuid()
		&& (st->st_mode & (S_IWGRP | S_IWOTH)) == 0;
}

/* Directory contents/timestamps belong to the complete namespace census.
 * This owner checks the selected name, object and access policy only. */
static bool
inputs_namespace_same(const struct stat *a, const struct stat *b)
{
	return inputs_namespace_directory(a) && inputs_namespace_directory(b)
		&& a->st_dev == b->st_dev && a->st_ino == b->st_ino
		&& a->st_mode == b->st_mode && a->st_uid == b->st_uid && a->st_gid == b->st_gid;
}

static bool
inputs_namespace_current(const ClusterWalInputsV1 *inputs)
{
	struct stat name, held;
	const ClusterRecoveryCtrcIoScopeV1 *scope = &inputs->ctrc;

	return scope->namespace_fd >= 0 && cluster_shared_data_dir != NULL
		&& strcmp(cluster_shared_data_dir, scope->namespace_path) == 0
		&& fstat(scope->namespace_fd, &held) == 0
		&& lstat(scope->namespace_path, &name) == 0
		&& inputs_namespace_same(&scope->namespace_identity, &held)
		&& inputs_namespace_same(&held, &name);
}

ClusterControlRootResult
cluster_wal_inputs_ctrc_namespace_v1(ClusterWalInputsV1 *inputs, int *root_fd)
{
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	ClusterRecoveryCtrcIoScopeV1 *scope;

	if (root_fd == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	*root_fd = -1;
	if (CritSectionCount != 0 || cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	if (!inputs_current(inputs) || inputs->purpose != WAL_INPUTS_CTRC_CENSUS)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	scope = &inputs->ctrc;
	if (scope->namespace_fd < 0) {
#if defined(O_NOFOLLOW) && defined(O_DIRECTORY) && defined(O_CLOEXEC)
		struct stat named, held;
		int fd;

		if (cluster_shared_data_dir == NULL || scope->namespace_path[0] != '/'
			|| strcmp(cluster_shared_data_dir, scope->namespace_path) != 0
			|| lstat(scope->namespace_path, &named) != 0
			|| !inputs_namespace_same(&scope->namespace_identity, &named)) {
			inputs->stale = true;
			return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		}
		fd = open(scope->namespace_path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
		if (fd < 0) {
			inputs->stale = true;
			return CLUSTER_CONTROL_ROOT_IO_ERROR;
		}
		/* Publish cleanup ownership before any backend call can ERROR. */
		scope->namespace_fd = fd;
		if (fstat(fd, &held) != 0 || !inputs_namespace_same(&named, &held)) {
			inputs->stale = true;
			inputs_namespace_close(inputs);
			return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		}
#else
		inputs->stale = true;
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
#endif
	}
	PG_TRY();
	{
		if (!inputs_namespace_current(inputs)) {
			inputs->stale = true;
			result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		} else if (!scope->namespace_verified) {
			result = cluster_wal_inputs_revalidate_v1(inputs);
			if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
				&& (!inputs_current(inputs) || !inputs_namespace_current(inputs))) {
				inputs->stale = true;
				result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
			}
			if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
				scope->namespace_verified = true;
		}
	}
	PG_CATCH();
	{
		inputs->stale = true;
		inputs_namespace_close(inputs);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		*root_fd = scope->namespace_fd;
	return result;
}

/* Reject aliasing before clearing a failed read's output. */
static bool
inputs_overlap(const void *a, Size a_size, const void *b, Size b_size)
{
	uintptr_t left = (uintptr_t)a, right = (uintptr_t)b;

	return a != NULL && b != NULL
		   && (left <= right ? right - left < a_size : left - right < b_size);
}

bool
cluster_wal_inputs_tx_abort_current_v1(ClusterWalInputsV1 *inputs,
	const ClusterRecoveryTTAbortV1 *record)
{
	ClusterRecoveryTTAbortDerivationV1 candidate;

	return inputs_current(inputs) && inputs->purpose == WAL_INPUTS_TX_CLOSURE
		&& cluster_recovery_tt_abort_valid_v1(record)
		&& ClusterColdRecoveryTxC0Candidate(inputs->tx.start, inputs->tx.subject, &candidate)
		&& memcmp(record, &candidate.record, sizeof(*record)) == 0;
}

/* Reconstruct only original request inputs, never a held guard. The old RR
 * token is retained verbatim; current COMPLETE/history comes solely from this
 * scope's actual full ROOT selection, original source cut and WALR owner. */
static bool
inputs_tx_subject_request(ClusterWalInputsV1 *inputs, ClusterRecoverySerialRequest *out,
	const ClusterWalInputV1 **out_selected)
{
	ClusterRecoverySerialRequest request;
	const ClusterColdTypedV1 *typed;
	const ClusterWalSourceRef *source = NULL;
	const ClusterWalInputV1 *selected = NULL;
	const uint32 required = CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID
		| CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID
		| CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID
		| CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_LAST_RECORD_VALID;

	if (!inputs_current(inputs) || inputs->purpose != WAL_INPUTS_TX_CLOSURE
		|| inputs->tx.ir_revoked
		|| !ClusterColdRecoveryTxSerialRequest(inputs->tx.start, inputs->tx.subject, &request)
		|| request.mode != CLUSTER_RECOVERY_SERIAL_TX_CLOSURE
		|| request.tx_closure_scope != NULL)
		return false;
	typed = inputs->tx.inputs;
	if (inputs_tx_reselect_original(inputs, typed) != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return false;
	for (uint32 p = 0; p < typed->replay_count; p++) {
		if (memcmp(&request.duty, &typed->sources[p].claim.identity, sizeof(request.duty)) != 0)
			continue;
		if (source != NULL)
			return false;
		source = &typed->sources[p];
	}
	if (source == NULL)
		return false;
	for (uint32 i = 0; i < inputs->count; i++) {
		const ClusterWalInputV1 *item = &inputs->items[i];
		if (item->kind != CLUSTER_WAL_INPUT_CHECKPOINT || !inputs_source_selected(source, &item->source))
			continue;
		if (selected != NULL)
			return false;
		selected = item;
	}
	if (selected == NULL
		|| memcmp(&selected->checkpoint.identity, &request.duty, sizeof(request.duty)) != 0
		|| selected->checkpoint.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE
		|| (selected->checkpoint.root_flags & required) != required
		|| selected->checkpoint.checkpoint_tli != source->timeline
		|| selected->checkpoint.tail_tli != source->timeline
		|| selected->checkpoint.recovered_tli != source->timeline
		|| selected->checkpoint.validated_tail_lsn_exclusive <= selected->checkpoint.checkpoint_lower_lsn
		|| selected->checkpoint.recovered_through_lsn_exclusive != selected->checkpoint.validated_tail_lsn_exclusive
		|| selected->checkpoint.recovered_last_record_lsn != selected->checkpoint.tail_last_record_lsn
		|| selected->checkpoint.recovered_last_record_crc32c != selected->checkpoint.tail_last_record_crc32c)
		return false;
	request.tx_closure_scope = inputs;
	*out = request;
	if (out_selected != NULL)
		*out_selected = selected;
	return true;
}

static bool
inputs_tx_serial_request(ClusterWalInputsV1 *inputs, ClusterRecoverySerialRequest *out)
{
	return inputs_tx_subject_request(inputs, out, NULL);
}

bool
cluster_wal_inputs_tx_serial_current_v1(ClusterWalInputsV1 *inputs,
	const ClusterRecoverySerialRequest *request)
{
	ClusterRecoverySerialRequest original;

	return request != NULL && inputs_tx_serial_request(inputs, &original)
		&& request->mode == original.mode && request->tx_closure_scope == inputs
		&& memcmp(&request->duty, &original.duty, sizeof(original.duty)) == 0
		&& memcmp(&request->expected_root_token, &original.expected_root_token, sizeof(original.expected_root_token)) == 0
		&& memcmp(&request->pending, &original.pending, sizeof(original.pending)) == 0
		&& request->formation == original.formation
		&& request->fence_need_set == original.fence_need_set
		&& request->fence_admission_set == original.fence_admission_set
		&& request->acquire_timeout_ms == original.acquire_timeout_ms
		&& request->release_timeout_ms == original.release_timeout_ms;
}

ClusterControlRootResult
cluster_wal_inputs_tx_serial_validate_v1(ClusterWalInputsV1 *inputs,
	const ClusterRecoverySerialRequest *request)
{
	ClusterControlRootResult result;

	if (!cluster_wal_inputs_tx_serial_current_v1(inputs, request))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	result = cluster_wal_inputs_revalidate_v1(inputs);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& !cluster_wal_inputs_tx_serial_current_v1(inputs, request))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	return result;
}

bool
cluster_wal_inputs_tx_abort_authorized_v1(ClusterWalInputsV1 *inputs,
	const ClusterRecoveryTTAbortV1 *record)
{
	if (!inputs_owned(inputs) || inputs->purpose != WAL_INPUTS_TX_CLOSURE
		|| !inputs->tx.ir_ready || inputs->tx.ir_revoked)
		return false;
	if (!cluster_wal_inputs_tx_abort_current_v1(inputs, record)
		|| cluster_recovery_serial_tx_revalidate(&inputs->tx.ir) != CLUSTER_RECOVERY_SERIAL_CURRENT) {
		inputs->tx.ir_revoked = true;
		inputs->tx.ir_ready = false;
		return false;
	}
	return true;
}

ClusterControlRootResult
cluster_wal_inputs_tx_abort_acquire_v1(ClusterWalInputsV1 *inputs,
	const ClusterRecoveryTTAbortV1 *record)
{
	ClusterRecoverySerialRequest request;
	ClusterRecoverySerialAcquireResult acquired;
	ClusterControlRootResult result;

	if (CritSectionCount != 0 || cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (!cluster_wal_inputs_tx_abort_current_v1(inputs, record)
		|| !inputs_tx_serial_request(inputs, &request))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (inputs->tx.ir.release_uncertain)
		return CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN;
	if (!inputs->tx.ir.held) {
		acquired = cluster_recovery_serial_acquire(&request, &inputs->tx.ir);
		if (acquired != CLUSTER_RECOVERY_SERIAL_GRANTED) {
			if (inputs->tx.ir.held || inputs->tx.ir.release_uncertain) {
				inputs->tx.ir_revoked = true;
				return CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN;
			}
			if (acquired == CLUSTER_RECOVERY_SERIAL_BUSY || acquired == CLUSTER_RECOVERY_SERIAL_RETRY)
				return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
			inputs->tx.ir_revoked = true;
			return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		}
	}
	/* A full ROOT compare after the actual grant must precede all target I/O.
	 * CF contention retains the same holder for a cancellable local wait. */
	result = cluster_wal_inputs_tx_serial_validate_v1(inputs, &request);
	if (result == CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE)
		return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| cluster_recovery_serial_tx_revalidate(&inputs->tx.ir) != CLUSTER_RECOVERY_SERIAL_CURRENT
		|| !cluster_wal_inputs_tx_abort_current_v1(inputs, record)) {
		inputs->tx.ir_revoked = true;
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	}
	inputs->tx.ir_ready = true;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static bool
inputs_tx_retention_locator(const ClusterRecoveryTxRetentionV1 *view)
{
	return view->scope != NULL && view->scope == active_tx_retention.scope
		&& view->serial == active_tx_retention.serial
		&& view->input_owner.pin == active_tx_retention.input_owner.pin
		&& view->input_owner.generation == active_tx_retention.input_owner.generation
		&& cluster_wal_read_pin_owner_current_v1(&view->input_owner);
}

bool
cluster_wal_inputs_tx_retention_current_v1(const ClusterRecoveryTxRetentionV1 *view)
{
	ClusterRecoveryTxRetentionV1 original;
	ClusterRecoveryTTAbortDerivationV1 candidate;
	ClusterWalInputsV1 *inputs;

	if (view == NULL)
		return false;
	original = *view;
	/* Do not dereference caller-supplied scope/serial pointers until both
	 * the original locator and native allocation incarnation have matched. */
	if (!inputs_tx_retention_locator(&original))
		return false;
	inputs = original.scope;
	if (!inputs_owned(inputs) || inputs->purpose != WAL_INPUTS_TX_CLOSURE
		|| inputs->pin != original.input_owner.pin || original.serial != &inputs->tx.ir
		|| !ClusterColdRecoveryTxC0Candidate(inputs->tx.start, inputs->tx.subject, &candidate)
		|| !cluster_wal_inputs_tx_abort_authorized_v1(inputs, &candidate.record))
		return false;
	return inputs_tx_retention_locator(&original);
}

ClusterRecoveryTxRetentionV1
cluster_wal_inputs_tx_retention_v1(ClusterWalInputsV1 *inputs,
	const ClusterRecoveryTTAbortV1 *record)
{
	ClusterRecoveryTxRetentionV1 view, empty = {0};
	ClusterRecoveryTTAbortV1 original;

	/* No caller output pointer can alias an opaque owner. Freeze the exact
	 * supplied candidate before callbacks; all later checks use that value. */
	if (inputs == NULL || inputs != active_tx_retention.scope || record == NULL)
		return empty;
	view = active_tx_retention;
	if (!inputs_tx_retention_locator(&view))
		return empty;
	original = *record;
	if (!cluster_wal_inputs_tx_abort_current_v1(inputs, &original)
		|| !cluster_wal_inputs_tx_abort_authorized_v1(inputs, &original)
		|| !cluster_wal_inputs_tx_retention_current_v1(&view)
		|| memcmp(record, &original, sizeof(original)) != 0)
		return empty;
	return view;
}

/* All outputs below are private stack values. Native incarnation and locator
 * checks precede any borrowed scope/IR access; the original request producer
 * alone chooses this subject from the full selected current/history union. */
static bool
inputs_tx_subject_authority(const ClusterRecoveryTxRetentionV1 *view,
	ClusterRecoverySubjectAuthorityV1 *out)
{
	ClusterRecoverySerialRequest request;
	const ClusterWalInputV1 *selected;

	if (!cluster_wal_inputs_tx_retention_current_v1(view)
		|| !inputs_tx_subject_request(view->scope, &request, &selected))
		return false;
	memset(out, 0, sizeof(*out));
	memcpy(&out->retention, view, sizeof(*view));
	memcpy(&out->root, &view->scope->token, sizeof(out->root));
	memcpy(&out->selected, selected, sizeof(*selected));
	memcpy(&out->replay_token, &request.expected_root_token, sizeof(out->replay_token));
	/* These are exact retained inputs, not a new claim or a current RR token.
	 * Do not replace either root identity with the installed writer's epoch. */
	return cluster_wal_inputs_tx_retention_current_v1(view);
}

ClusterRecoverySubjectAuthorityV1
cluster_wal_inputs_tx_subject_v1(const ClusterRecoveryTxRetentionV1 *view)
{
	ClusterRecoverySubjectAuthorityV1 authority, empty = {0};
	ClusterRecoveryTxRetentionV1 original;

	if (view == NULL)
		return empty;
	memcpy(&original, view, sizeof(original));
	if (!inputs_tx_subject_authority(&original, &authority)
		|| memcmp(view, &original, sizeof(original)) != 0)
		return empty;
	return authority;
}

bool
cluster_wal_inputs_tx_subject_current_v1(const ClusterRecoverySubjectAuthorityV1 *authority)
{
	ClusterRecoverySubjectAuthorityV1 original, expected;

	if (authority == NULL)
		return false;
	memcpy(&original, authority, sizeof(original));
	return inputs_tx_subject_authority(&original.retention, &expected)
		&& memcmp(&original, &expected, sizeof(original)) == 0
		&& memcmp(authority, &original, sizeof(original)) == 0;
}

bool
cluster_wal_inputs_tx_subject_covers_window_v1(
	const ClusterRecoverySubjectAuthorityV1 *authority, uint16 thread,
	XLogRecPtr lower, XLogRecPtr upper)
{
	ClusterRecoverySubjectAuthorityV1 original;

	if (authority == NULL || thread == 0 || lower == InvalidXLogRecPtr || upper <= lower)
		return false;
	memcpy(&original, authority, sizeof(original));
	return cluster_wal_inputs_tx_subject_current_v1(&original)
		&& thread == original.selected.source.claim.identity.origin_thread_id
		&& lower >= original.selected.checkpoint.checkpoint_lower_lsn
		&& upper <= original.selected.checkpoint.validated_tail_lsn_exclusive
		&& memcmp(authority, &original, sizeof(original)) == 0;
}

bool
cluster_wal_inputs_tx_abort_path_v1(ClusterWalInputsV1 *inputs,
	const ClusterRecoveryTTAbortV1 *record, char *out, size_t out_size)
{
	char path[MAXPGPATH];

	/* Do not clear output that aliases the retained subject or this scope. */
	if (out == NULL || out_size == 0
		|| inputs_overlap(out, out_size, record, sizeof(*record))
		|| inputs_overlap(out, out_size, inputs, sizeof(*inputs))
		|| (inputs_owned(inputs) && inputs->purpose == WAL_INPUTS_TX_CLOSURE
			&& (inputs_overlap(out, out_size, inputs->tx.subject, sizeof(*inputs->tx.subject))
				|| inputs_overlap(out, out_size, inputs->tx.inputs, sizeof(*inputs->tx.inputs)))))
		return false;
	out[0] = '\0';
	if (!cluster_wal_inputs_tx_abort_current_v1(inputs, record))
		return false;
	if (cluster_shared_fs_undo_path_resolve(record->exact.instance, record->exact.segment_id,
			path, sizeof(path)) != 0
		|| !cluster_wal_inputs_tx_abort_current_v1(inputs, record)
		|| strlen(path) >= out_size)
		return false;
	memcpy(out, path, strlen(path) + 1);
	return true;
}

/* CF is already held on the exact whole ROOT. This scratch selection owns
 * no WALR locks; retention belongs to the caller's original execution pin. */
static ClusterControlRootResult
inputs_cold_checkpoint_select(ClusterWalInputsV1 *scratch, WalInputsWork *work,
							 const ClusterControlRootSnapshot *expected)
{
	const ClusterWalHistoryRecord *selected = NULL;
	uint32 node = expected->identity.origin_node_id;
	ClusterControlRootResult result;

	if (!work->root.present[node])
		return CLUSTER_CONTROL_ROOT_ABSENT;
	result = cluster_wal_origin_inputs_read_locked(&work->root, node, &work->origin);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (work->origin.has_pending)
		return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
	for (uint32 i = 0; i <= work->origin.history.count; i++) {
		const ClusterWalHistoryRecord *record = i == work->origin.history.count
												   ? &work->origin.current
												   : &work->origin.history.records[i];
		if (!cluster_control_root_identity_equal(&record->snapshot.identity, &expected->identity))
			continue;
		if (selected != NULL || memcmp(&record->snapshot, expected, sizeof(*expected)) != 0)
			return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		selected = record;
	}
	if (selected == NULL)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	return inputs_checkpoint(scratch, work, selected, selected == &work->origin.current);
}

ClusterControlRootResult
cluster_wal_inputs_cold_checkpoint_v1(const ClusterControlRootSnapshot *expected,
									const ClusterControlRootFileToken *root_token,
									ClusterWalRecoveryCheckpointV1 *out)
{
	ClusterWalInputsV1 *scratch;
	WalInputsWork *work;
	ClusterControlRootResult result;

	if (inputs_overlap(expected, sizeof(*expected), out, sizeof(*out))
		|| inputs_overlap(root_token, sizeof(*root_token), out, sizeof(*out)))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (expected == NULL || root_token == NULL || out == NULL
		|| expected->identity.origin_node_id < 0
		|| expected->identity.origin_node_id >= CLUSTER_MAX_NODES
		|| expected->identity.origin_thread_id != expected->identity.origin_node_id + 1
		|| (expected->lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED
			&& expected->lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
			&& expected->lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (!cluster_enabled || !cluster_shared_config || !cluster_controlfile_shared_authority
		|| CritSectionCount != 0 || CurrentResourceOwner == NULL || !inputs_io_role(true)
		|| expected->identity.system_identifier != GetSystemIdentifier())
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	scratch = palloc0(sizeof(*scratch));
	scratch->system_identifier = expected->identity.system_identifier;
	memcpy(scratch->storage_uuid, expected->identity.storage_uuid, 16);
	scratch->token = *root_token;
	work = palloc0(sizeof(*work));
	PG_TRY();
	{
		ClusterWalThreadClaimV2 claim;

		result = inputs_root(scratch, work, false);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			result = inputs_cold_checkpoint_select(scratch, work, expected);
		inputs_unlock(work);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			result = cluster_wal_claim_v2_read(cluster_wal_threads_dir,
											   &scratch->items[0].source.claim, &claim);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			result = inputs_root(scratch, work, false);
		inputs_unlock(work);
	}
	PG_CATCH();
	{
		inputs_unlock(work);
		pfree(work);
		pfree(scratch);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		out->source = scratch->items[0].source;
		out->checkpoint_start = scratch->items[0].checkpoint_start;
		memcpy(&out->checkpoint, &scratch->items[0].native_checkpoint, sizeof(out->checkpoint));
	}
	pfree(work);
	pfree(scratch);
	return result;
}

ClusterControlRootResult
cluster_wal_inputs_cold_sync_v1(const ClusterControlRootSnapshot *expected,
								const ClusterControlRootFileToken *root_token,
								ClusterWalTailObservation *out)
{
	ClusterWalRecoveryCheckpointV1 selected, confirmed;
	ClusterWalTailObservation observed;
	ClusterControlRootResult result;

	if (inputs_overlap(expected, sizeof(*expected), out, sizeof(*out))
		|| inputs_overlap(root_token, sizeof(*root_token), out, sizeof(*out)))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (expected == NULL || root_token == NULL || out == NULL || !enableFsync)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	result = cluster_wal_inputs_cold_checkpoint_v1(expected, root_token, &selected);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	/* The checkpoint selector releases CF before returning. Keep all physical
	 * WAL/claim/namespace I/O under the caller's existing execution retention. */
	result
		= cluster_wal_retained_sync_v1(cluster_wal_threads_dir, &selected.source, wal_segment_size,
									   expected, selected.checkpoint_start, &observed);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	result = cluster_wal_inputs_cold_checkpoint_v1(expected, root_token, &confirmed);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (memcmp(&selected, &confirmed, sizeof(selected)) != 0)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	*out = observed;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
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
	if (!inputs_owned(inputs) || !inputs_scope_role(inputs) || inputs->stale)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	if (inputs->suspended)
		return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	if (!inputs_current(inputs))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	inputs_tx_serial_release(inputs);
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
	if (!inputs_owned(inputs) || !inputs_scope_role(inputs) || CritSectionCount != 0
		|| !cluster_enabled || !cluster_shared_config)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	/* Failure remains terminal even when the ROOT still matches. No WAL
	 * or plan consumer can use the scope after this call. */
	inputs->stale = true;
	if (cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	inputs_tx_serial_release(inputs);
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
	if (!inputs_owned(inputs) || !inputs_scope_role(inputs) || inputs->stale)
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
