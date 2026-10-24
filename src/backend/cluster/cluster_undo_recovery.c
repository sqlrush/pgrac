/*-------------------------------------------------------------------------
 * cluster_undo_recovery.c
 *    Canonical UNDO access within the original failed-origin recovery scope.
 *-------------------------------------------------------------------------
 * Author: SqlRush <sqlrush@gmail.com>
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER
#include "access/clog.h"
#include "access/xlog.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_mode.h"
#include "cluster/cluster_remote_xact.h"
#include "cluster/cluster_undo_recovery.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/cluster_wal_tail.h"
#include "cluster/storage/cluster_shared_fs.h"

static ClusterUndoRecoveryScopeV1 *active_scope;
/* Never reuse an original-round identity within this backend process. */
static uint64 undo_scope_serial;

/* A selected C0 grants a path to one target segment only. It never becomes
 * the ordinary origin scope used by native CLOG/MX or general UNDO redo. */
typedef struct RecoveryAbortScope {
	const RfSideOnlinePlanV1 *plan;
	uint32 index;
	const ClusterThreadRecoveryAuthorityV1 *authority[2];
	ClusterControlRootSnapshot root[2];
	ClusterControlRootReadToken token[2];
	ClusterWalSourceRef source[2];
	RfPageOnlineRecordIdentityV1 identity;
	ClusterRecoveryTTAbortV1 record;
	bool clog_active;
	bool projection_active;
} RecoveryAbortScope;

static const RecoveryAbortScope *active_abort;

static bool
undo_source_equal(const ClusterWalSourceRef *left, const ClusterWalSourceRef *right)
{
	return memcmp(&left->claim.identity, &right->claim.identity, sizeof(left->claim.identity)) == 0
		   && left->claim.database_incarnation == right->claim.database_incarnation
		   && left->claim.max_config_generation == right->claim.max_config_generation
		   && memcmp(left->claim.claim_sha256, right->claim.claim_sha256, 32) == 0
		   && left->timeline == right->timeline;
}

static bool
recovery_abort_current(const RecoveryAbortScope *scope)
{
	ClusterWalSourceRef bound[2];
	ClusterProtectedSetIdentityV1 storage;
	RfContributorStreamCutV1 cut = { 0 };

	if (!cluster_cold_replay_window_active_v1() || !cluster_peer_mode_enabled()
		|| !cluster_undo_gcs_coherence
		|| scope->identity.record.system_identifier != GetSystemIdentifier()
		|| !rf_side_online_plan_recovery_abort_sources_v1(scope->plan, scope->index, &bound[0],
														  &bound[1])
		|| !undo_source_equal(&bound[0], &scope->source[0])
		|| !undo_source_equal(&bound[1], &scope->source[1])
		|| !cluster_shared_fs_get_protected_set_identity(&storage)
		|| storage.backend_id != CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS
		|| memcmp(storage.storage_uuid, scope->identity.record.storage_uuid, 16) != 0)
		return false;
	for (int i = 0; i < 2; i++) {
		const ClusterThreadRecoveryAuthorityV1 *a = scope->authority[i];
		const ClusterControlRootSnapshot *root = &scope->root[i];
		ClusterWalSourceRef observed;
		XLogRecPtr redo;

		if (a == NULL || a->duty == NULL || a->root_snapshot == NULL || a->root_token == NULL
			|| a->serial_guard == NULL
			|| a->serial_guard->mode != CLUSTER_RECOVERY_SERIAL_COLD_FORMED
			|| memcmp(a->duty, &root->identity, sizeof(root->identity)) != 0
			|| memcmp(a->root_snapshot, root, sizeof(*root)) != 0
			|| memcmp(a->root_token, &scope->token[i], sizeof(scope->token[i])) != 0
			|| memcmp(&scope->source[i].claim.identity, &root->identity, sizeof(root->identity))
				   != 0
			|| root->checkpoint_tli != scope->source[i].timeline
			|| root->tail_tli != root->checkpoint_tli
			|| root->checkpoint_lower_lsn == InvalidXLogRecPtr
			|| root->validated_tail_lsn_exclusive <= root->checkpoint_lower_lsn
			|| cluster_thread_recovery_authority_revalidate_nowait_v1(a)
				   != CLUSTER_THREAD_AUTHORITY_OK
			|| cluster_control_root_recovery_source_v1(root, &scope->token[i], &observed, &redo)
				   != CLUSTER_CONTROL_ROOT_OK_PRIMARY
			|| !undo_source_equal(&observed, &scope->source[i]) || redo == InvalidXLogRecPtr
			|| redo > root->validated_tail_lsn_exclusive
			|| cluster_thread_recovery_authority_revalidate_nowait_v1(a)
				   != CLUSTER_THREAD_AUTHORITY_OK)
			return false;
		if (i == 0
			&& (scope->identity.record.read_rec_ptr < redo
				|| !rf_side_online_plan_replay_start_matches_v1(
					scope->plan, root->identity.origin_thread_id,
					root->identity.origin_owner_incarnation, observed.claim.database_incarnation,
					redo)))
			return false;
	}
	cut.failed_thread = scope->root[0].identity.origin_thread_id;
	cut.origin_owner_incarnation = scope->root[0].identity.origin_owner_incarnation;
	cut.timeline_id = scope->root[0].checkpoint_tli;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	cut.scan_begin_inclusive = scope->root[0].checkpoint_lower_lsn;
	cut.scan_end_exclusive = scope->root[0].validated_tail_lsn_exclusive;
	return scope->identity.record.read_rec_ptr >= cut.scan_begin_inclusive
		   && scope->identity.record.end_rec_ptr > scope->identity.record.read_rec_ptr
		   && scope->identity.record.end_rec_ptr <= cut.scan_end_exclusive
		   && rf_side_online_plan_recovery_source_matches_v1(
			   scope->plan, scope->identity.record.system_identifier,
			   scope->identity.record.storage_uuid, &cut);
}

static bool
recovery_abort_sync(const RecoveryAbortScope *scope, int i)
{
	const ClusterControlRootSnapshot *root = &scope->root[i];
	ClusterWalTailObservation observed;

	return cluster_control_root_recovery_sync_v1(root, &scope->token[i], &observed)
			   == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		   && observed.complete_end == root->validated_tail_lsn_exclusive
		   && observed.last_record_start == root->tail_last_record_lsn
		   && observed.last_record_crc == root->tail_last_record_crc32c
		   && observed.database_incarnation == scope->source[i].claim.database_incarnation
		   && recovery_abort_current(scope);
}

bool
cluster_undo_recovery_abort_replay_v1(const RfSideOnlinePlanV1 *plan, uint32 index,
									  const RfPageOnlineRecordIdentityV1 *identity,
									  const ClusterThreadRecoveryAuthorityV1 *writer,
									  const ClusterThreadRecoveryAuthorityV1 *target)
{
	RecoveryAbortScope scope = { 0 };
	RfSideOnlineOperationV1 operation;
	volatile ClusterUndoSmgrRecoveryStampV1 stamp = { 0 };
	volatile bool ok = false;

	if (active_scope != NULL || active_abort != NULL || identity == NULL
		|| !rf_side_online_plan_operation_v1(plan, index, &operation)
		|| operation.kind != RF_SIDE_ONLINE_OPERATION_RECOVERY_TT_ABORT || operation.history_only
		|| memcmp(&operation.identity, identity, sizeof(*identity)) != 0
		|| !cluster_recovery_tt_abort_valid_v1(&operation.recovery_abort)
		|| !rf_side_online_plan_recovery_abort_sources_v1(plan, index, &scope.source[0],
														  &scope.source[1]))
		return false;
	scope.plan = plan;
	scope.index = index;
	scope.authority[0] = writer;
	scope.authority[1] = target;
	scope.identity = *identity;
	scope.record = operation.recovery_abort;
	for (int i = 0; i < 2; i++) {
		const ClusterThreadRecoveryAuthorityV1 *a = scope.authority[i];
		const ClusterWalSourceRef *source = &scope.source[i];
		if (a == NULL || a->root_snapshot == NULL || a->root_token == NULL
			|| !cluster_wal_claim_v2_ref_valid(&source->claim)
			|| source->claim.identity.system_identifier != identity->record.system_identifier
			|| memcmp(source->claim.identity.storage_uuid, identity->record.storage_uuid, 16) != 0)
			return false;
		scope.root[i] = *a->root_snapshot;
		scope.token[i] = *a->root_token;
	}
	if (scope.source[0].claim.identity.origin_thread_id != scope.record.writer_thread
		|| scope.source[0].claim.identity.origin_owner_incarnation
			   != scope.record.writer_incarnation
		|| scope.source[1].claim.identity.origin_thread_id != scope.record.target_thread
		|| scope.source[1].claim.identity.origin_owner_incarnation
			   != scope.record.target_incarnation
		|| memcmp(scope.source[1].claim.claim_sha256, scope.record.target_claim_sha256, 32) != 0
		|| scope.source[0].claim.database_incarnation != scope.source[1].claim.database_incarnation
		|| memcmp(scope.source[0].claim.identity.authority_uuid,
				  scope.source[1].claim.identity.authority_uuid, 16)
			   != 0
		|| identity->record.origin_thread != scope.record.writer_thread
		|| identity->record.timeline_id != scope.source[0].timeline
		|| identity->record.rmid != RM_CLUSTER_UNDO_ID
		|| identity->record.info != XLOG_UNDO_RECOVERY_TT_ABORT || !recovery_abort_current(&scope))
		return false;

	cluster_undo_smgr_fd_cache_reset();
	active_abort = &scope;
	PG_TRY();
	{
		/* Existing C0 redo, not a new stamp insertion: retain both original
		 * owners and durable input before opening the exact canonical target. */
		if (recovery_abort_sync(&scope, 0) && recovery_abort_sync(&scope, 1)
			&& cluster_undo_smgr_recovery_abort_prepare_v1(&scope.record,
														   (ClusterUndoSmgrRecoveryStampV1 *)&stamp)
			&& recovery_abort_current(&scope)
			&& cluster_undo_smgr_recovery_abort_write_v1(
				(ClusterUndoSmgrRecoveryStampV1){ stamp.token })
			&& recovery_abort_current(&scope)) {
			/* The target slot and both inputs are durable before restoring
			 * this one target-native ABORT bit. No ordinary origin is admitted. */
			scope.clog_active = true;
			ok = cluster_clog_native_recovery_apply_v1(scope.record.target_thread - 1,
													   scope.record.exact.xid, 0, NULL,
													   TRANSACTION_STATUS_ABORTED)
				 && recovery_abort_current(&scope)
				 && cluster_clog_native_recovery_verify_v1(scope.record.target_thread - 1,
														   scope.record.exact.xid, 0, NULL,
														   TRANSACTION_STATUS_ABORTED)
				 && recovery_abort_current(&scope);
			scope.clog_active = false;
			if (ok) {
				ClusterRemoteXactMutationV2 mutation;
				SCN scn = InvalidScn;
				uint16 wrap = 0;
				bool wrap_valid = false;

				/* Derived target projection follows canonical and native
				 * durability. The WAL writer remains a separate identity. */
				scope.projection_active = true;
				mutation = cluster_remote_xact_store_terminal_v2(
					scope.record.target_thread - 1, scope.record.exact.xid, false, NULL,
					CLUSTER_REMOTE_XACT_ABORTED, InvalidScn, 0, false, 0);
				scope.projection_active = false;
				ok = (mutation == CLUSTER_REMOTE_XACT_MUTATION_STORED
					  || mutation == CLUSTER_REMOTE_XACT_MUTATION_UNCHANGED)
					 && recovery_abort_current(&scope);
				if (ok)
					ok = cluster_remote_commit_outcome_ex(scope.record.target_thread - 1,
														  scope.record.exact.xid, &scn, &wrap,
														  &wrap_valid)
							 == CLUSTER_REMOTE_XACT_ABORTED
						 && scn == InvalidScn && !wrap_valid && wrap == 0
						 && recovery_abort_current(&scope);
			}
		}
	}
	PG_FINALLY();
	{
		active_abort = NULL;
		if (stamp.token != 0
			&& !cluster_undo_smgr_recovery_abort_close_v1(
				(ClusterUndoSmgrRecoveryStampV1){ stamp.token }))
			ok = false;
		cluster_undo_smgr_fd_cache_reset();
	}
	PG_END_TRY();
	return ok;
}

static bool
undo_scope_current(const ClusterUndoRecoveryScopeV1 *scope)
{
	const ClusterThreadRecoveryAuthorityV1 *a;
	ClusterProtectedSetIdentityV1 storage;

	if (scope == NULL || (a = scope->authority) == NULL || a->duty == NULL
		|| a->root_snapshot == NULL || a->root_token == NULL || a->serial_guard == NULL
		|| (a->serial_guard->mode != CLUSTER_RECOVERY_SERIAL_ONLINE
			&& a->serial_guard->mode != CLUSTER_RECOVERY_SERIAL_COLD_FORMED)
		|| !cluster_peer_mode_enabled() || !cluster_undo_gcs_coherence
		|| scope->duty.system_identifier != GetSystemIdentifier()
		|| memcmp(&scope->duty, a->duty, sizeof(scope->duty)) != 0
		|| memcmp(&scope->root_token, a->root_token, sizeof(scope->root_token)) != 0
		|| a->root_snapshot->checkpoint_tli != scope->cut.timeline_id
		|| a->duty->origin_owner_incarnation != scope->cut.origin_owner_incarnation
		|| a->root_snapshot->tail_tli != scope->cut.timeline_id
		|| a->root_snapshot->checkpoint_lower_lsn != scope->cut.scan_begin_inclusive
		|| a->root_snapshot->validated_tail_lsn_exclusive != scope->cut.scan_end_exclusive
		|| cluster_thread_recovery_authority_revalidate_nowait_v1(a) != CLUSTER_THREAD_AUTHORITY_OK
		|| !cluster_shared_fs_get_protected_set_identity(&storage)
		|| storage.backend_id != CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS
		|| memcmp(storage.storage_uuid, scope->duty.storage_uuid, 16) != 0)
		return false;
	return rf_side_online_plan_source_matches_v1(scope->plan, scope->duty.system_identifier,
												 scope->duty.storage_uuid, &scope->cut);
}

bool
cluster_undo_recovery_scope_enter_v1(ClusterUndoRecoveryScopeV1 *scope,
									 const ClusterThreadRecoveryAuthorityV1 *authority,
									 const RfSideOnlinePlanV1 *plan)
{
	ClusterUndoRecoveryScopeV1 candidate = { 0 };

	if (undo_scope_serial == UINT64_MAX || active_scope != NULL || active_abort != NULL
		|| scope == NULL || authority == NULL || plan == NULL || authority->duty == NULL
		|| authority->root_token == NULL || authority->root_snapshot == NULL
		|| authority->duty->origin_thread_id == 0 || authority->duty->origin_thread_id > 128)
		return false;
	candidate.authority = authority;
	candidate.plan = plan;
	candidate.duty = *authority->duty;
	candidate.root_token = *authority->root_token;
	candidate.cut.failed_thread = authority->duty->origin_thread_id;
	candidate.cut.origin_owner_incarnation = authority->duty->origin_owner_incarnation;
	candidate.cut.timeline_id = authority->root_snapshot->checkpoint_tli;
	candidate.cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	candidate.cut.scan_begin_inclusive = authority->root_snapshot->checkpoint_lower_lsn;
	candidate.cut.scan_end_exclusive = authority->root_snapshot->validated_tail_lsn_exclusive;
	if (candidate.cut.timeline_id == 0 || candidate.cut.scan_begin_inclusive == InvalidXLogRecPtr
		|| candidate.cut.scan_end_exclusive <= candidate.cut.scan_begin_inclusive
		|| !undo_scope_current(&candidate))
		return false;
	cluster_undo_smgr_fd_cache_reset();
	*scope = candidate;
	active_scope = scope;
	undo_scope_serial++;
	return true;
}

void
cluster_undo_recovery_scope_leave_v1(ClusterUndoRecoveryScopeV1 *scope)
{
	if (scope != NULL && active_scope == scope) {
		active_scope = NULL;
		memset(scope, 0, sizeof(*scope));
		cluster_undo_smgr_fd_cache_reset();
	}
}

ClusterUndoPathIntent
cluster_undo_recovery_intent_for_owner(uint8 owner)
{
	/* Keep an expired or mismatched active request on the qualified path so
	 * its resolver refuses instead of silently materializing a local copy. */
	return active_scope != NULL || active_abort != NULL ? CLUSTER_UNDO_PATH_RECOVERY_SHARED
														: cluster_undo_intent_for_owner(owner);
}

bool
cluster_undo_recovery_origin_authorized_v1(int origin_node)
{
	return origin_node >= 0 && origin_node < 128 && active_scope != NULL
		   && active_scope->duty.origin_thread_id == origin_node + 1
		   && undo_scope_current(active_scope);
}

bool
cluster_undo_recovery_cold_origin_authorized_v1(int origin_node)
{
	return cluster_undo_recovery_origin_authorized_v1(origin_node)
		   && active_scope->authority->serial_guard->mode == CLUSTER_RECOVERY_SERIAL_COLD_FORMED
		   && rf_side_online_plan_recovery_source_matches_v1(
			   active_scope->plan, active_scope->duty.system_identifier,
			   active_scope->duty.storage_uuid, &active_scope->cut);
}

bool
cluster_undo_recovery_cold_source_matches_v1(const ClusterWalSourceRef *source,
											 const ClusterControlRootSnapshot *checkpoint,
											 XLogRecPtr native_redo)
{
	const ClusterUndoRecoveryScopeV1 *scope = active_scope;
	ClusterWalSourceRef observed;
	XLogRecPtr redo;

	if (source == NULL || checkpoint == NULL || native_redo == InvalidXLogRecPtr
		|| !cluster_undo_recovery_cold_origin_authorized_v1(source->claim.identity.origin_node_id)
		|| active_scope != scope
		|| memcmp(checkpoint, scope->authority->root_snapshot, sizeof(*checkpoint)) != 0
		|| cluster_control_root_recovery_source_v1(scope->authority->root_snapshot,
												   &scope->root_token, &observed, &redo)
			   != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| !undo_source_equal(&observed, source) || redo != native_redo)
		return false;
	/* The original source accessor performs I/O; do not accept authority or
	 * snapshot changes across it. No new scope or authority is manufactured. */
	return active_scope == scope
		   && cluster_undo_recovery_cold_origin_authorized_v1(source->claim.identity.origin_node_id)
		   && memcmp(checkpoint, scope->authority->root_snapshot, sizeof(*checkpoint)) == 0;
}

uint64
cluster_undo_recovery_cold_scope_id_v1(const ClusterWalSourceRef *source,
									   const ClusterControlRootSnapshot *checkpoint,
									   XLogRecPtr native_redo)
{
	const ClusterUndoRecoveryScopeV1 *scope = active_scope;
	uint64 serial = undo_scope_serial;
	if (serial == 0
		|| !cluster_undo_recovery_cold_source_matches_v1(source, checkpoint, native_redo)
		|| active_scope != scope || undo_scope_serial != serial)
		return 0;
	return serial;
}

typedef struct UndoRecoveryCheckpointVisit {
	const ClusterUndoRecoveryScopeV1 *scope;
	ClusterControlRootSnapshot root;
	ClusterControlRootReadToken token;
	ClusterWalRecoveryCheckpointV1 checkpoint;
	ClusterUndoRecoveryCheckpointVisitorV1 visitor;
	void *arg;
	uint64 serial;
	uint64 records;
	int origin;
} UndoRecoveryCheckpointVisit;

static bool
undo_checkpoint_same(const UndoRecoveryCheckpointVisit *work)
{
	return active_scope == work->scope && undo_scope_serial == work->serial
		   && work->scope->authority != NULL && work->scope->authority->root_snapshot != NULL
		   && memcmp(&work->root, work->scope->authority->root_snapshot, sizeof(work->root)) == 0
		   && memcmp(&work->token, &work->scope->root_token, sizeof(work->token)) == 0;
}

static bool
undo_checkpoint_current(const UndoRecoveryCheckpointVisit *work)
{
	return undo_checkpoint_same(work)
		   && cluster_undo_recovery_cold_origin_authorized_v1(work->origin)
		   && undo_checkpoint_same(work);
}

static bool
undo_checkpoint_record(XLogReaderState *reader, void *arg)
{
	UndoRecoveryCheckpointVisit *work = arg;

	/* No authority can be cached across leave/reenter, even at the same
	 * address. Full owner/source checks bracket the original complete visit. */
	if (reader == NULL || !undo_checkpoint_same(work) || work->records == UINT64_MAX
		|| !work->visitor(&work->checkpoint, &work->root, work->serial, reader, work->arg)
		|| !undo_checkpoint_same(work))
		return false;
	work->records++;
	return true;
}

bool
cluster_undo_recovery_cold_checkpoint_visit_v1(int origin_node,
											   ClusterUndoRecoveryCheckpointVisitorV1 visitor,
											   void *arg)
{
	UndoRecoveryCheckpointVisit work = { 0 };
	ClusterWalRecoveryCheckpointV1 after = { 0 };
	ClusterWalTailObservation observed = { 0 };
	const ClusterUndoRecoveryScopeV1 *scope = active_scope;
	uint64 serial = undo_scope_serial;

	if (visitor == NULL || serial == 0
		|| !cluster_undo_recovery_cold_origin_authorized_v1(origin_node) || active_scope != scope
		|| undo_scope_serial != serial)
		return false;
	work.scope = scope;
	work.root = *scope->authority->root_snapshot;
	work.token = scope->root_token;
	work.serial = serial;
	work.origin = origin_node;
	work.visitor = visitor;
	work.arg = arg;
	/* The caller already holds the original execution retention/IR owner.
	 * In particular, do not reacquire pass-1's census WAL read set here. */
	if (cluster_control_root_recovery_checkpoint_v1(&work.root, &work.token, &work.checkpoint)
			!= CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| !undo_checkpoint_current(&work)
		|| cluster_undo_recovery_cold_scope_id_v1(&work.checkpoint.source, &work.root,
												  work.checkpoint.checkpoint.redo)
			   != serial
		|| cluster_control_root_recovery_visit(&work.root, &work.token, undo_checkpoint_record,
											   &work, &observed)
			   != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| !undo_checkpoint_current(&work) || work.records == 0 || observed.records != work.records
		|| observed.complete_end != work.root.validated_tail_lsn_exclusive
		|| observed.last_record_start != work.root.tail_last_record_lsn
		|| observed.last_record_crc != work.root.tail_last_record_crc32c
		|| observed.database_incarnation != work.checkpoint.source.claim.database_incarnation
		|| cluster_control_root_recovery_checkpoint_v1(&work.root, &work.token, &after)
			   != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| memcmp(&after, &work.checkpoint, sizeof(after)) != 0
		|| cluster_undo_recovery_cold_scope_id_v1(&work.checkpoint.source, &work.root,
												  work.checkpoint.checkpoint.redo)
			   != serial
		|| !undo_checkpoint_current(&work))
		return false;
	/* Only stack-owned provisional work; original readers own their ERROR
	 * cleanup and the caller retains the original scope and every pin. */
	return true;
}

bool
cluster_undo_recovery_cold_terminal_matches_v1(const ClusterWalSourceRef *source,
											   const ClusterControlRootSnapshot *checkpoint,
											   XLogRecPtr native_redo, uint64 scope_serial,
											   const XLogReaderState *reader)
{
	const ClusterUndoRecoveryScopeV1 *scope = active_scope;

	if (reader == NULL || scope_serial == 0
		|| cluster_undo_recovery_cold_scope_id_v1(source, checkpoint, native_redo) != scope_serial
		|| active_scope != scope
		|| !rf_side_online_plan_replay_start_matches_v1(
			scope->plan, source->claim.identity.origin_thread_id,
			source->claim.identity.origin_owner_incarnation, source->claim.database_incarnation,
			native_redo)
		|| !rf_side_online_plan_terminal_matches_v1(scope->plan, source, reader))
		return false;
	return active_scope == scope && undo_scope_serial == scope_serial
		   && cluster_undo_recovery_cold_scope_id_v1(source, checkpoint, native_redo)
				  == scope_serial;
}

uint32
cluster_undo_recovery_cold_terminal_count_v1(const ClusterWalSourceRef *source,
											 const ClusterControlRootSnapshot *checkpoint,
											 XLogRecPtr native_redo, uint64 scope_serial)
{
	const ClusterUndoRecoveryScopeV1 *scope = active_scope;
	uint32 count;

	if (scope_serial == 0
		|| cluster_undo_recovery_cold_scope_id_v1(source, checkpoint, native_redo) != scope_serial
		|| active_scope != scope
		|| !rf_side_online_plan_replay_start_matches_v1(
			scope->plan, source->claim.identity.origin_thread_id,
			source->claim.identity.origin_owner_incarnation, source->claim.database_incarnation,
			native_redo))
		return UINT32_MAX;
	count = rf_side_online_plan_native_terminal_count_v1(scope->plan, source);
	if (active_scope != scope || undo_scope_serial != scope_serial
		|| cluster_undo_recovery_cold_scope_id_v1(source, checkpoint, native_redo) != scope_serial)
		return UINT32_MAX;
	return count;
}

bool
cluster_undo_recovery_abort_clog_authorized_v1(int origin_node, TransactionId xid)
{
	return origin_node >= 0 && origin_node < 128 && active_abort != NULL
		   && active_abort->clog_active && active_abort->record.target_thread == origin_node + 1
		   && active_abort->record.exact.xid == xid && recovery_abort_current(active_abort);
}

bool
cluster_undo_recovery_abort_projection_authorized_v1(int origin_node, TransactionId xid)
{
	return origin_node >= 0 && origin_node < 128 && active_abort != NULL
		   && active_abort->projection_active
		   && active_abort->record.target_thread == origin_node + 1
		   && active_abort->record.exact.xid == xid && recovery_abort_current(active_abort);
}

bool
cluster_undo_recovery_cold_source_sync_v1(int origin_node)
{
	ClusterUndoRecoveryScopeV1 *scope = active_scope;
	ClusterWalTailObservation observed;
	const ClusterControlRootSnapshot *root;

	if (!cluster_undo_recovery_cold_origin_authorized_v1(origin_node))
		return false;
	if (scope->wal_synced)
		return true;
	root = scope->authority->root_snapshot;
	/* No cache survives this scope. ROOT selection, claim/physical files and
	 * the complete tail are checked by the original owner outside CF. */
	if (cluster_control_root_recovery_sync_v1(root, scope->authority->root_token, &observed)
			!= CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| active_scope != scope || !cluster_undo_recovery_cold_origin_authorized_v1(origin_node)
		|| observed.complete_end != root->validated_tail_lsn_exclusive
		|| observed.last_record_start != root->tail_last_record_lsn
		|| observed.last_record_crc != root->tail_last_record_crc32c)
		return false;
	scope->wal_synced = true;
	return true;
}

bool
cluster_undo_recovery_cold_source_synced_v1(int origin_node)
{
	return cluster_undo_recovery_cold_origin_authorized_v1(origin_node) && active_scope->wal_synced;
}

int
cluster_undo_recovery_path_resolve_v1(uint8 owner, uint32 segment, char *path, size_t size)
{
	char resolved[MAXPGPATH];

	if (active_abort != NULL) {
		if (owner != active_abort->record.exact.instance
			|| segment != active_abort->record.exact.segment_id || path == NULL || size == 0
			|| !recovery_abort_current(active_abort)
			|| cluster_shared_fs_undo_path_resolve(owner, segment, resolved, sizeof(resolved)) != 0
			|| strlen(resolved) >= size)
			return -1;
		strlcpy(path, resolved, size);
		return 0;
	}
	if (active_scope == NULL || owner == 0 || owner != active_scope->duty.origin_thread_id
		|| segment == 0 || ((segment - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE) + 1 != owner
		|| path == NULL || size == 0 || !undo_scope_current(active_scope)
		|| cluster_shared_fs_undo_path_resolve(owner, segment, resolved, sizeof(resolved)) != 0
		|| strlen(resolved) >= size)
		return -1;
	strlcpy(path, resolved, size);
	return 0;
}

bool
cluster_undo_recovery_multixact_page_retired_v1(int origin_node, XLogRecPtr source_lsn,
												XLogRecPtr source_end_lsn, bool members,
												uint32 page)
{
	return cluster_undo_recovery_origin_authorized_v1(origin_node)
		   && rf_side_online_plan_multixact_page_retired_v1(
			   active_scope->plan, origin_node + 1, source_lsn, source_end_lsn, members, page);
}

#endif
