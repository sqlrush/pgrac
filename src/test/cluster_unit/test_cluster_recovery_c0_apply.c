/* Exact C0 replay uses the real recovery owner/path and codec. ROOT/IR,
 * immutable plan delivery and held-FD IO below are controlled boundaries. */
#include "postgres.h"
#include "access/xlog.h"
#include "access/clog.h"
#include "cluster/cluster_mode.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_remote_xact.h"
#include "cluster/cluster_undo_recovery.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/cluster_wal_inputs.h"
#include "cluster/cluster_wal_tail.h"
#include "cluster/storage/cluster_shared_fs.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
int cluster_node_id = 0;
bool cluster_enabled = true;
bool cluster_undo_gcs_coherence = true;
static ClusterConf conf;
ClusterConf *ClusterConfShmem = &conf;
static const RfSideOnlinePlanV1 *plan = (const RfSideOnlinePlanV1 *)(uintptr_t)1;
static ClusterWalSourceRef sources[2], live_sources[2];
static ClusterControlRootSnapshot roots[2];
static ClusterControlRootReadToken tokens[2];
static ClusterRecoverySerialGuard serials[2];
static ClusterThreadRecoveryAuthorityV1 authorities[2];
static ClusterThreadRecoveryAuthorityResultV1 current[2];
static RfSideOnlineOperationV1 operation;
static TTSlot disk_slot;
static uint64 system_id;
static uint8 storage_uuid[16];
static bool storage_ok, plan_ok, source_ok, cut_ok, redo_ok, replay_window;
static bool synced[2], fd_held, nested;
static unsigned sync_calls, prepares, writes, closes, path_calls;
static int fail_sync, fail_read_source, throw_stage, drift_stage, drift_kind;
static bool fail_prepare, fail_write, fail_close;
static bool fail_clog_apply, fail_clog_verify;
static unsigned clog_applies, clog_verifies;
static unsigned projection_stores, projection_reads;
static ClusterRemoteXactMutationV2 projection_result;
static int projection_read_fault;
static char order[64];
static unsigned order_count;
static char tx_inputs_storage;
static bool tx_mode, tx_current, tx_durable;
static unsigned tx_revalidates, tx_fail_revalidate;

bool
cluster_wal_inputs_tx_abort_current_v1(ClusterWalInputsV1 *inputs,
	const ClusterRecoveryTTAbortV1 *record)
{
	return tx_mode && tx_current && inputs == (ClusterWalInputsV1 *)&tx_inputs_storage
		&& memcmp(record, &operation.recovery_abort, sizeof(*record)) == 0;
}

ClusterControlRootResult
cluster_wal_inputs_revalidate_v1(ClusterWalInputsV1 *inputs)
{
	UT_ASSERT(tx_mode && inputs == (ClusterWalInputsV1 *)&tx_inputs_storage);
	if (++tx_revalidates == tx_fail_revalidate)
		tx_current = false;
	return tx_current ? CLUSTER_CONTROL_ROOT_OK_PRIMARY : CLUSTER_CONTROL_ROOT_STALE_TOKEN;
}

bool
cluster_undo_smgr_recovery_abort_durable_v1(ClusterUndoSmgrRecoveryStampV1 stamp,
	const ClusterRecoveryTTAbortV1 *record)
{
	return tx_mode && tx_durable && fd_held && stamp.token == 19
		&& memcmp(record, &operation.recovery_abort, sizeof(*record)) == 0;
}

static void
note(char value)
{
	UT_ASSERT(order_count + 1 < sizeof(order));
	if (order_count + 1 < sizeof(order)) {
		order[order_count++] = value;
		order[order_count] = '\0';
	}
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

static void
inject(int stage)
{
	if (throw_stage == stage)
		pg_re_throw();
	if (drift_stage != stage)
		return;
	if (tx_mode) {
		if (drift_kind == 0)
			tx_current = false;
		else
			tx_durable = false;
		return;
	}
	switch (drift_kind) {
	case 0:
		current[0] = CLUSTER_THREAD_AUTHORITY_PIN_STALE;
		break;
	case 1:
		current[1] = CLUSTER_THREAD_AUTHORITY_SERIAL_STALE;
		break;
	case 2:
		roots[0].tail_last_record_crc32c++;
		break;
	case 3:
		roots[1].validated_tail_lsn_exclusive++;
		break;
	case 4:
		tokens[1].file_txn_seq++;
		break;
	case 5:
		live_sources[1].claim.claim_sha256[31] ^= 1;
		break;
	case 6:
		plan_ok = false;
		break;
	case 7:
		storage_uuid[15] ^= 1;
		break;
	case 8:
		live_sources[0].claim.max_config_generation++;
		break;
	case 9:
		replay_window = false;
		break;
	default:
		abort();
	}
}

bool
cluster_cold_replay_window_active_v1(void)
{
	return replay_window;
}

uint64
GetSystemIdentifier(void)
{
	return system_id;
}

bool
cluster_shared_fs_get_protected_set_identity(ClusterProtectedSetIdentityV1 *out)
{
	memset(out, 0, sizeof(*out));
	out->backend_id = CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS;
	memcpy(out->storage_uuid, storage_uuid, 16);
	return storage_ok;
}

ClusterThreadRecoveryAuthorityResultV1
cluster_thread_recovery_authority_revalidate_nowait_v1(const ClusterThreadRecoveryAuthorityV1 *a)
{
	for (int i = 0; i < 2; i++)
		if (a == &authorities[i])
			return current[i];
	return CLUSTER_THREAD_AUTHORITY_INVALID;
}

bool
rf_side_online_plan_operation_v1(const RfSideOnlinePlanV1 *p, uint32 index,
								 RfSideOnlineOperationV1 *out)
{
	if (p != plan || index != 0 || !plan_ok)
		return false;
	*out = operation;
	return true;
}

bool
rf_side_online_plan_recovery_abort_sources_v1(const RfSideOnlinePlanV1 *p, uint32 index,
											  ClusterWalSourceRef *writer,
											  ClusterWalSourceRef *target)
{
	if (p != plan || index != 0 || !plan_ok || !source_ok)
		return false;
	*writer = sources[0];
	*target = sources[1];
	return true;
}

bool
rf_side_online_plan_source_matches_v1(const RfSideOnlinePlanV1 *p, uint64 sysid,
									  const uint8 uuid[16], const RfContributorStreamCutV1 *cut)
{
	return p == plan && cut_ok && sysid == sources[0].claim.identity.system_identifier
		   && memcmp(uuid, sources[0].claim.identity.storage_uuid, 16) == 0
		   && cut->failed_thread == 3 && cut->origin_owner_incarnation == 33
		   && cut->timeline_id == 7 && cut->flags == RF_CONTRIBUTOR_CUT_COMPLETE
		   && cut->scan_begin_inclusive == 100 && cut->scan_end_exclusive == 500;
}

bool
rf_side_online_plan_recovery_source_matches_v1(const RfSideOnlinePlanV1 *p, uint64 sysid,
											   const uint8 uuid[16],
											   const RfContributorStreamCutV1 *cut)
{
	return plan_ok && rf_side_online_plan_source_matches_v1(p, sysid, uuid, cut);
}

bool
rf_side_online_plan_replay_start_matches_v1(const RfSideOnlinePlanV1 *p, uint32 thread,
											uint64 incarnation, uint64 db, XLogRecPtr redo)
{
	return p == plan && redo_ok && thread == 3 && incarnation == 33 && db == 7 && redo == 100;
}

ClusterControlRootResult
cluster_control_root_recovery_source_v1(const ClusterControlRootSnapshot *root,
										const ClusterControlRootReadToken *token,
										ClusterWalSourceRef *out, XLogRecPtr *redo)
{
	for (int i = 0; i < 2; i++) {
		if (root->identity.origin_thread_id != roots[i].identity.origin_thread_id)
			continue;
		UT_ASSERT(memcmp(root, &roots[i], sizeof(*root)) == 0);
		UT_ASSERT(memcmp(token, &tokens[i], sizeof(*token)) == 0);
		*out = live_sources[i];
		*redo = 100;
		return fail_read_source == i ? CLUSTER_CONTROL_ROOT_IO_ERROR
									 : CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	}
	UT_ASSERT(false);
	return CLUSTER_CONTROL_ROOT_IO_ERROR;
}

ClusterControlRootResult
cluster_control_root_recovery_sync_v1(const ClusterControlRootSnapshot *root,
									  const ClusterControlRootReadToken *token,
									  ClusterWalTailObservation *out)
{
	int i = root->identity.origin_thread_id == 3 ? 0 : 1;
	UT_ASSERT(memcmp(root, &roots[i], sizeof(*root)) == 0);
	UT_ASSERT(memcmp(token, &tokens[i], sizeof(*token)) == 0);
	sync_calls++;
	note(i == 0 ? 'W' : 'T');
	memset(out, 0, sizeof(*out));
	out->complete_end = root->validated_tail_lsn_exclusive;
	out->last_record_start = root->tail_last_record_lsn;
	out->last_record_crc = root->tail_last_record_crc32c;
	out->database_incarnation = live_sources[i].claim.database_incarnation;
	synced[i] = fail_sync != i;
	inject(i + 1);
	if (fail_sync == i + 2)
		out->last_record_crc++;
	return fail_sync == i ? CLUSTER_CONTROL_ROOT_IO_ERROR : CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

void
cluster_undo_smgr_fd_cache_reset(void)
{}

int
cluster_shared_fs_undo_path_resolve(uint8 owner, uint32 segment, char *path, size_t size)
{
	path_calls++;
	return snprintf(path, size, "/shared/undo/%u/%u", owner, segment) >= (int)size ? -1 : 0;
}

bool
cluster_undo_smgr_recovery_abort_prepare_v1(const ClusterRecoveryTTAbortV1 *record,
											ClusterUndoSmgrRecoveryStampV1 *out)
{
	char path[MAXPGPATH];
	ClusterUndoRecoveryScopeV1 ordinary = { 0 };
	prepares++;
	note('P');
	UT_ASSERT(synced[0] && synced[1]);
	UT_ASSERT(memcmp(record, &operation.recovery_abort, sizeof(*record)) == 0);
	UT_ASSERT(!cluster_undo_recovery_origin_authorized_v1(1));
	UT_ASSERT(!cluster_undo_recovery_cold_origin_authorized_v1(1));
	UT_ASSERT(!cluster_undo_recovery_origin_authorized_v1(2));
	UT_ASSERT(!cluster_undo_recovery_abort_clog_authorized_v1(1, record->exact.xid));
	UT_ASSERT(!cluster_undo_recovery_abort_projection_authorized_v1(1, record->exact.xid));
	UT_ASSERT(!cluster_undo_recovery_scope_enter_v1(&ordinary, &authorities[1], plan));
	UT_ASSERT(cluster_undo_recovery_path_resolve_v1(2, 257, path, sizeof(path)) == 0);
	UT_ASSERT(strcmp(path, "/shared/undo/2/257") == 0);
	UT_ASSERT(cluster_undo_recovery_path_resolve_v1(2, 258, path, sizeof(path)) == -1);
	UT_ASSERT(cluster_undo_recovery_path_resolve_v1(3, 513, path, sizeof(path)) == -1);
	if (!nested) {
		nested = true;
		UT_ASSERT(!cluster_undo_recovery_abort_replay_v1(plan, 0, &operation.identity,
														 &authorities[0], &authorities[1]));
		nested = false;
	}
	out->token = fail_prepare ? 0 : 19;
	fd_held = !fail_prepare;
	inject(3);
	return !fail_prepare;
}

bool
cluster_undo_smgr_recovery_abort_write_v1(ClusterUndoSmgrRecoveryStampV1 stamp)
{
	UT_ASSERT(fd_held && stamp.token == 19);
	writes++;
	UT_ASSERT(
		!cluster_undo_recovery_abort_clog_authorized_v1(1, operation.recovery_abort.exact.xid));
	note('S');
	inject(4);
	if (fail_write)
		return false;
	disk_slot.status = TT_SLOT_ABORTED;
	return true;
}

static void
clog_exact_scope(int origin, TransactionId xid, int nsubxids, const TransactionId *children,
				 XidStatus status)
{
	UT_ASSERT_EQ(origin, 1);
	UT_ASSERT_EQ(xid, operation.recovery_abort.exact.xid);
	UT_ASSERT_EQ(nsubxids, 0);
	UT_ASSERT_NULL(children);
	UT_ASSERT_EQ(status, TRANSACTION_STATUS_ABORTED);
	UT_ASSERT(fd_held && synced[0] && synced[1]);
	UT_ASSERT_EQ(disk_slot.status, TT_SLOT_ABORTED);
	UT_ASSERT(!cluster_undo_recovery_abort_projection_authorized_v1(origin, xid));
	UT_ASSERT(cluster_undo_recovery_abort_clog_authorized_v1(origin, xid));
	UT_ASSERT(!cluster_undo_recovery_abort_clog_authorized_v1(origin, xid + 1));
	UT_ASSERT(!cluster_undo_recovery_abort_clog_authorized_v1(2, xid));
	UT_ASSERT(!cluster_undo_recovery_abort_clog_authorized_v1(-1, xid));
	UT_ASSERT(!cluster_undo_recovery_abort_clog_authorized_v1(128, xid));
	UT_ASSERT(!cluster_undo_recovery_origin_authorized_v1(origin));
	UT_ASSERT(!cluster_undo_recovery_cold_origin_authorized_v1(origin));
}

bool
cluster_clog_native_recovery_apply_v1(int origin, TransactionId xid, int nsubxids,
									  const TransactionId *children, XidStatus status)
{
	clog_applies++;
	note('A');
	clog_exact_scope(origin, xid, nsubxids, children, status);
	inject(5);
	return !fail_clog_apply;
}

bool
cluster_clog_native_recovery_verify_v1(int origin, TransactionId xid, int nsubxids,
									   const TransactionId *children, XidStatus status)
{
	clog_verifies++;
	note('V');
	clog_exact_scope(origin, xid, nsubxids, children, status);
	inject(6);
	return !fail_clog_verify;
}

ClusterRemoteXactMutationV2
cluster_remote_xact_store_terminal_v2(int origin, TransactionId xid, bool prepared,
									  const uint8 digest[28], ClusterRemoteXactOutcome outcome,
									  SCN scn, TimestampTz timestamp, bool wrap_valid, uint16 wrap)
{
	projection_stores++;
	note('M');
	UT_ASSERT_EQ(origin, 1); /* Target thread 2, not writer thread 3. */
	UT_ASSERT_EQ(xid, 0x400102);
	UT_ASSERT(!prepared && digest == NULL && outcome == CLUSTER_REMOTE_XACT_ABORTED);
	UT_ASSERT(scn == InvalidScn && timestamp == 0 && !wrap_valid && wrap == 0);
	UT_ASSERT(fd_held && synced[0] && synced[1] && disk_slot.status == TT_SLOT_ABORTED);
	UT_ASSERT(clog_applies == 1 && clog_verifies == 1);
	UT_ASSERT(!cluster_undo_recovery_abort_clog_authorized_v1(origin, xid));
	UT_ASSERT(!cluster_undo_recovery_origin_authorized_v1(origin));
	UT_ASSERT(!cluster_undo_recovery_cold_origin_authorized_v1(origin));
	UT_ASSERT(cluster_undo_recovery_abort_projection_authorized_v1(origin, xid));
	UT_ASSERT(!cluster_undo_recovery_abort_projection_authorized_v1(origin, xid + 1));
	UT_ASSERT(!cluster_undo_recovery_abort_projection_authorized_v1(2, xid));
	UT_ASSERT(!cluster_undo_recovery_abort_projection_authorized_v1(-1, xid));
	UT_ASSERT(!cluster_undo_recovery_abort_projection_authorized_v1(128, xid));
	inject(7);
	return projection_result;
}

ClusterRemoteXactOutcome
cluster_remote_commit_outcome_ex(int origin, TransactionId xid, SCN *scn, uint16 *wrap,
								 bool *wrap_valid)
{
	projection_reads++;
	note('Q');
	UT_ASSERT_EQ(origin, 1);
	UT_ASSERT_EQ(xid, 0x400102);
	UT_ASSERT_EQ(projection_stores, 1);
	/* The write capability ends before the independent read. */
	UT_ASSERT(!cluster_undo_recovery_abort_projection_authorized_v1(origin, xid));
	*scn = projection_read_fault == 1 ? 17 : InvalidScn;
	*wrap = projection_read_fault == 2 ? 9 : 0;
	*wrap_valid = projection_read_fault == 2;
	inject(8);
	return projection_read_fault == 3	? CLUSTER_REMOTE_XACT_COMMITTED
		   : projection_read_fault == 4 ? CLUSTER_REMOTE_XACT_INDOUBT
										: CLUSTER_REMOTE_XACT_ABORTED;
}

bool
cluster_undo_smgr_recovery_abort_close_v1(ClusterUndoSmgrRecoveryStampV1 stamp)
{
	UT_ASSERT(fd_held && stamp.token == 19);
	fd_held = false;
	closes++;
	note('C');
	return !fail_close;
}

static void
reset(void)
{
	memset(sources, 0, sizeof(sources));
	memset(roots, 0, sizeof(roots));
	memset(tokens, 0, sizeof(tokens));
	memset(serials, 0, sizeof(serials));
	memset(authorities, 0, sizeof(authorities));
	memset(&operation, 0, sizeof(operation));
	memset(order, 0, sizeof(order));
	order_count = 0;
	tx_mode = tx_current = tx_durable = false;
	tx_revalidates = tx_fail_revalidate = 0;
	conf.node_count = 4;
	cluster_enabled = cluster_undo_gcs_coherence = true;
	system_id = 123;
	memset(storage_uuid, 0x44, 16);
	storage_ok = plan_ok = source_ok = cut_ok = redo_ok = replay_window = true;
	synced[0] = synced[1] = fd_held = nested = false;
	sync_calls = prepares = writes = closes = path_calls = 0;
	fail_sync = fail_read_source = -1;
	throw_stage = drift_stage = drift_kind = 0;
	fail_prepare = fail_write = fail_close = false;
	fail_clog_apply = fail_clog_verify = false;
	clog_applies = clog_verifies = 0;
	projection_stores = projection_reads = 0;
	projection_result = CLUSTER_REMOTE_XACT_MUTATION_STORED;
	projection_read_fault = 0;
	for (int i = 0; i < 2; i++) {
		ClusterWalThreadClaimRefV2 *ref = &sources[i].claim;
		ref->identity.system_identifier = system_id;
		memcpy(ref->identity.storage_uuid, storage_uuid, 16);
		memset(ref->identity.authority_uuid, 0x55, 16);
		ref->identity.origin_thread_id = 3 - i;
		ref->identity.origin_node_id = 2 - i;
		ref->identity.origin_owner_incarnation = 33 - 11 * i;
		ref->identity.root_lineage_seq = 1;
		ref->identity.thread_claim_created_at = 42;
		ref->database_incarnation = 7;
		ref->max_config_generation = 9;
		memset(ref->claim_sha256, 0x20 + i, 32);
		sources[i].timeline = 7;
		roots[i].identity = ref->identity;
		roots[i].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
		roots[i].checkpoint_tli = roots[i].tail_tli = 7;
		roots[i].checkpoint_lower_lsn = 100;
		roots[i].validated_tail_lsn_exclusive = 500;
		roots[i].tail_last_record_lsn = 400;
		roots[i].tail_last_record_crc32c = 33;
		serials[i].held = true;
		serials[i].mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
		authorities[i].duty = &roots[i].identity;
		authorities[i].root_snapshot = &roots[i];
		authorities[i].root_token = &tokens[i];
		authorities[i].serial_guard = &serials[i];
		current[i] = CLUSTER_THREAD_AUTHORITY_OK;
	}
	memcpy(live_sources, sources, sizeof(sources));
	operation.kind = RF_SIDE_ONLINE_OPERATION_RECOVERY_TT_ABORT;
	operation.identity.record.system_identifier = system_id;
	memcpy(operation.identity.record.storage_uuid, storage_uuid, 16);
	operation.identity.record.origin_thread = 3;
	operation.identity.record.timeline_id = 7;
	operation.identity.record.read_rec_ptr = 200;
	operation.identity.record.end_rec_ptr = 250;
	operation.identity.record.record_crc = 39;
	operation.identity.record.rmid = RM_CLUSTER_UNDO_ID;
	operation.identity.record.info = XLOG_UNDO_RECOVERY_TT_ABORT;
	operation.recovery_abort.version = 1;
	operation.recovery_abort.length = 152;
	operation.recovery_abort.flags = CLUSTER_RECOVERY_TT_ABORT_TOP_KNOWN;
	operation.recovery_abort.target_thread = 2;
	operation.recovery_abort.writer_thread = 3;
	operation.recovery_abort.target_incarnation = 22;
	operation.recovery_abort.writer_incarnation = 33;
	operation.recovery_abort.recovery_serial = 19;
	operation.recovery_abort.target_epoch = 4;
	operation.recovery_abort.target_full_xid = operation.recovery_abort.top_full_xid
		= UINT64_C(0x700400102);
	memcpy(operation.recovery_abort.target_claim_sha256, sources[1].claim.claim_sha256, 32);
	operation.recovery_abort.exact.segment_id = 257;
	operation.recovery_abort.exact.instance = 2;
	operation.recovery_abort.exact.format_version = CLUSTER_UNDO_TT_ABORT_EXACT_VERSION;
	operation.recovery_abort.exact.slot_offset = 5;
	operation.recovery_abort.exact.wrap = 9;
	operation.recovery_abort.exact.xid = 0x400102;
	operation.recovery_abort.before.xid = 0x400102;
	operation.recovery_abort.before.wrap = 9;
	operation.recovery_abort.before.status = TT_SLOT_ACTIVE;
	operation.recovery_abort.before.flags = TT_FLAGS_RESERVED;
	disk_slot = operation.recovery_abort.before;
}

static bool
apply(void)
{
	return cluster_undo_recovery_abort_replay_v1(plan, 0, &operation.identity, &authorities[0],
												 &authorities[1]);
}

static void
no_path(void)
{
	char path[MAXPGPATH];
	UT_ASSERT(!fd_held);
	UT_ASSERT(
		!cluster_undo_recovery_abort_clog_authorized_v1(1, operation.recovery_abort.exact.xid));
	UT_ASSERT(!cluster_undo_recovery_abort_projection_authorized_v1(
		1, operation.recovery_abort.exact.xid));
	UT_ASSERT_EQ(cluster_undo_recovery_path_resolve_v1(2, 257, path, sizeof(path)), -1);
	UT_ASSERT_EQ(cluster_undo_recovery_intent_for_owner(2), CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL);
}

UT_TEST(exact_c0_replay_binds_two_owners_and_durable_order)
{
	reset();
	UT_ASSERT(cluster_recovery_tt_abort_valid_v1(&operation.recovery_abort));
	UT_ASSERT(apply());
	UT_ASSERT(strcmp(order, "WTPSAVMQC") == 0);
	UT_ASSERT_EQ(clog_applies, 1);
	UT_ASSERT_EQ(clog_verifies, 1);
	UT_ASSERT_EQ(projection_stores, 1);
	UT_ASSERT_EQ(projection_reads, 1);
	UT_ASSERT_EQ(disk_slot.status, TT_SLOT_ABORTED);
	UT_ASSERT_EQ(closes, 1);
	no_path();
}

UT_TEST(missing_authority_or_changed_claim_refuses_before_io)
{
	for (int fault = 0; fault < 24; fault++) {
		reset();
		switch (fault) {
		case 0:
			current[0] = CLUSTER_THREAD_AUTHORITY_PIN_STALE;
			break;
		case 1:
			current[1] = CLUSTER_THREAD_AUTHORITY_FENCE_STALE;
			break;
		case 2:
			authorities[0].duty = NULL;
			break;
		case 3:
			authorities[1].root_token = NULL;
			break;
		case 4:
			serials[0].mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
			break;
		case 5:
			serials[1].mode = CLUSTER_RECOVERY_SERIAL_INPUT_SEAL;
			break;
		case 6:
			live_sources[0].claim.claim_sha256[0] ^= 1;
			break;
		case 7:
			live_sources[1].claim.claim_sha256[31] ^= 1;
			break;
		case 8:
			live_sources[1].claim.database_incarnation++;
			break;
		case 9:
			live_sources[1].claim.max_config_generation++;
			break;
		case 10:
			live_sources[1].timeline++;
			break;
		case 11:
			roots[1].identity.origin_owner_incarnation++;
			break;
		case 12:
			operation.history_only = true;
			break;
		case 13:
			operation.kind = RF_SIDE_ONLINE_OPERATION_UNDO;
			break;
		case 14:
			source_ok = false;
			break;
		case 15:
			cut_ok = false;
			break;
		case 16:
			redo_ok = false;
			break;
		case 17:
			system_id++;
			break;
		case 18:
			storage_ok = false;
			break;
		case 19:
			operation.recovery_abort.target_claim_sha256[31] ^= 1;
			break;
		case 20:
			operation.recovery_abort.writer_incarnation++;
			break;
		case 21:
			fail_read_source = 1;
			break;
		case 22:
			operation.identity.record.end_rec_ptr = 501;
			break;
		case 23:
			operation.identity.record.read_rec_ptr = 99;
			break;
		}
		UT_ASSERT(!apply());
		UT_ASSERT_EQ(sync_calls + prepares + writes + closes, 0);
		no_path();
	}
}

UT_TEST(wrong_record_or_crossed_authorities_never_resolve_a_path)
{
	RfPageOnlineRecordIdentityV1 other;
	reset();
	other = operation.identity;
	other.record.record_crc++;
	UT_ASSERT(
		!cluster_undo_recovery_abort_replay_v1(plan, 0, &other, &authorities[0], &authorities[1]));
	UT_ASSERT(!cluster_undo_recovery_abort_replay_v1(plan, 1, &operation.identity, &authorities[0],
													 &authorities[1]));
	UT_ASSERT(!cluster_undo_recovery_abort_replay_v1(plan, 0, &operation.identity, &authorities[1],
													 &authorities[0]));
	UT_ASSERT(
		!cluster_undo_recovery_abort_replay_v1(plan, 0, NULL, &authorities[0], &authorities[1]));
	UT_ASSERT_EQ(sync_calls + path_calls, 0);
	no_path();
}

UT_TEST(sync_prepare_write_close_failure_never_reports_completion)
{
	for (int fault = 0; fault < 9; fault++) {
		reset();
		if (fault < 4)
			fail_sync = fault;
		if (fault == 4)
			fail_prepare = true;
		if (fault == 5)
			fail_write = true;
		if (fault == 6)
			fail_close = true;
		if (fault == 7)
			fail_clog_apply = true;
		if (fault == 8)
			fail_clog_verify = true;
		UT_ASSERT(!apply());
		UT_ASSERT_EQ(writes, fault >= 5 ? 1 : 0);
		UT_ASSERT_EQ(closes, fault >= 5 ? 1 : 0);
		UT_ASSERT_EQ(clog_applies, fault >= 6 ? 1 : 0);
		UT_ASSERT_EQ(clog_verifies, fault == 6 || fault == 8 ? 1 : 0);
		UT_ASSERT_EQ(projection_stores, fault == 6 ? 1 : 0);
		UT_ASSERT_EQ(projection_reads, fault == 6 ? 1 : 0);
		no_path();
	}
}

UT_TEST(authority_drift_at_each_io_boundary_is_not_success)
{
	for (int stage = 1; stage <= 8; stage++)
		for (int fault = 0; fault < 10; fault++) {
			reset();
			drift_stage = stage;
			drift_kind = fault;
			UT_ASSERT(!apply());
			UT_ASSERT_EQ(writes, stage >= 4 ? 1 : 0);
			UT_ASSERT_EQ(closes, stage >= 3 ? 1 : 0);
			no_path();
		}
}

UT_TEST(error_rethrows_and_removes_target_scope_and_held_descriptor)
{
	for (int stage = 1; stage <= 8; stage++) {
		volatile bool caught = false;
		reset();
		throw_stage = stage;
		PG_TRY();
		{
			(void)apply();
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(closes, stage >= 3 ? 1 : 0);
		no_path();
	}
}

UT_TEST(replay_outside_original_startup_window_cannot_mutate)
{
	reset();
	replay_window = false;
	UT_ASSERT(!apply());
	UT_ASSERT_EQ(sync_calls + prepares + writes + closes, 0);
	no_path();
}

UT_TEST(replay_does_not_reuse_prior_authority_or_sync_success)
{
	reset();
	UT_ASSERT(apply());
	current[1] = CLUSTER_THREAD_AUTHORITY_SERIAL_STALE;
	UT_ASSERT(!apply());
	UT_ASSERT_EQ(writes, 1);
	current[1] = CLUSTER_THREAD_AUTHORITY_OK;
	clog_applies = clog_verifies = projection_stores = projection_reads = 0;
	UT_ASSERT(apply());
	UT_ASSERT_EQ(writes, 2);
	UT_ASSERT_EQ(sync_calls, 4);
	no_path();
}

UT_TEST(projection_conflict_or_bad_readback_never_reports_completion)
{
	for (int fault = 0; fault < 7; fault++) {
		reset();
		if (fault == 0)
			projection_result = CLUSTER_REMOTE_XACT_MUTATION_CONFLICT;
		else if (fault == 1)
			projection_result = CLUSTER_REMOTE_XACT_MUTATION_INVALID;
		else if (fault == 2)
			projection_result = CLUSTER_REMOTE_XACT_MUTATION_UNCHANGED;
		else
			projection_read_fault = fault - 2;
		UT_ASSERT_EQ(apply(), fault == 2);
		UT_ASSERT_EQ(projection_stores, 1);
		UT_ASSERT_EQ(projection_reads, fault >= 2 ? 1 : 0);
		UT_ASSERT_EQ(closes, 1);
		no_path();
	}
}

static void
reset_tx(void)
{
	reset();
	replay_window = false;
	tx_mode = tx_current = tx_durable = true;
	fd_held = synced[0] = synced[1] = true;
	disk_slot.status = TT_SLOT_ABORTED;
}

static bool
project_tx(void)
{
	return cluster_undo_recovery_abort_project_tx_v1((ClusterWalInputsV1 *)&tx_inputs_storage,
		&operation.recovery_abort, (ClusterUndoSmgrRecoveryStampV1){19});
}

static void
no_tx_authority(void)
{
	/* Caller still owns the FD and original exclusion, even on refusal. */
	UT_ASSERT(fd_held);
	UT_ASSERT_EQ(prepares + writes + closes + sync_calls + path_calls, 0);
	fd_held = false;
	no_path();
}

UT_TEST(new_c0_restores_exact_foreign_native_then_projection_after_eor)
{
	reset_tx();
	UT_ASSERT(project_tx());
	UT_ASSERT(strcmp(order, "AVMQ") == 0);
	UT_ASSERT_EQ(tx_revalidates, 5);
	no_tx_authority();
}

UT_TEST(new_c0_requires_current_input_and_exact_durable_held_stamp)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		ClusterRecoveryTTAbortV1 record;
		ClusterWalInputsV1 *inputs;
		ClusterUndoSmgrRecoveryStampV1 stamp = {19};
		reset_tx();
		record = operation.recovery_abort;
		inputs = (ClusterWalInputsV1 *)&tx_inputs_storage;
		switch (fault) {
		case 0: tx_current = false; break;
		case 1: tx_durable = false; break;
		case 2: inputs = NULL; break;
		case 3: stamp.token++; break;
		case 4: record.writer_incarnation++; break;
		}
		UT_ASSERT(!cluster_undo_recovery_abort_project_tx_v1(inputs, &record, stamp));
		UT_ASSERT_EQ(clog_applies + projection_stores, 0);
		no_tx_authority();
	}
}

UT_TEST(new_c0_restores_own_origin_only_through_the_exact_held_scope)
{
	reset_tx();
	cluster_node_id = 1; /* Target thread 2 now belongs to this startup node. */
	UT_ASSERT(project_tx());
	UT_ASSERT(strcmp(order, "AVMQ") == 0);
	UT_ASSERT_EQ(tx_revalidates, 5);
	cluster_node_id = 0;
	no_tx_authority();
}

UT_TEST(new_c0_io_refusal_or_source_drift_never_reports_completion)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		reset_tx();
		switch (fault) {
		case 0: fail_clog_apply = true; break;
		case 1: fail_clog_verify = true; break;
		case 2: projection_result = CLUSTER_REMOTE_XACT_MUTATION_CONFLICT; break;
		case 3: projection_read_fault = 1; break;
		case 4: projection_read_fault = 2; break;
		case 5: projection_read_fault = 3; break;
		case 6: projection_read_fault = 4; break;
		case 7: tx_fail_revalidate = 1; break;
		case 8: tx_fail_revalidate = 3; break;
		case 9: tx_fail_revalidate = 5; break;
		}
		UT_ASSERT(!project_tx());
		no_tx_authority();
	}
	for (int stage = 5; stage <= 8; stage++)
		for (int kind = 0; kind < 2; kind++) {
			reset_tx();
			drift_stage = stage;
			drift_kind = kind;
			UT_ASSERT(!project_tx());
			no_tx_authority();
		}
}

UT_TEST(new_c0_error_revokes_capability_without_releasing_callers_fd)
{
	for (int stage = 5; stage <= 8; stage++) {
		volatile bool caught = false;
		reset_tx();
		throw_stage = stage;
		PG_TRY();
		{
			(void)project_tx();
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		no_tx_authority();
	}
}

int
main(void)
{
	UT_PLAN(14);
	UT_RUN(exact_c0_replay_binds_two_owners_and_durable_order);
	UT_RUN(missing_authority_or_changed_claim_refuses_before_io);
	UT_RUN(wrong_record_or_crossed_authorities_never_resolve_a_path);
	UT_RUN(sync_prepare_write_close_failure_never_reports_completion);
	UT_RUN(authority_drift_at_each_io_boundary_is_not_success);
	UT_RUN(error_rethrows_and_removes_target_scope_and_held_descriptor);
	UT_RUN(replay_outside_original_startup_window_cannot_mutate);
	UT_RUN(replay_does_not_reuse_prior_authority_or_sync_success);
	UT_RUN(projection_conflict_or_bad_readback_never_reports_completion);
	UT_RUN(new_c0_restores_exact_foreign_native_then_projection_after_eor);
	UT_RUN(new_c0_requires_current_input_and_exact_durable_held_stamp);
	UT_RUN(new_c0_restores_own_origin_only_through_the_exact_held_scope);
	UT_RUN(new_c0_io_refusal_or_source_drift_never_reports_completion);
	UT_RUN(new_c0_error_revokes_capability_without_releasing_callers_fd);
	UT_DONE();
	return ut_failed_count != 0;
}
