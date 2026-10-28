/*-------------------------------------------------------------------------
 * test_cluster_side_xact.c
 *    RF-SIDE immutable XACT decode tests.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/subtrans.h"
#include "storage/proc.h"
#include <unistd.h>
#include <sys/stat.h>

#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/multixact.h"
#include "access/rmgr.h"
#include "access/twophase.h"
#include "access/twophase_rmgr.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlogutils.h"
#include "catalog/storage_xlog.h"
#include "catalog/pg_control.h"
#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_multixact.h"
#include "cluster/cluster_recovery_undo_scan.h"
#include "cluster/cluster_side_xact.h"
#include "cluster/cluster_side_online_plan.h"
#include "cluster/cluster_side_online_owner.h"
#include "cluster/cluster_space_recovery.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/cluster_side_undo.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_undo_segment_init.h"
#include "cluster/cluster_tt_2pc.h"
#include "cluster/cluster_xid_stripe.h"
#include "cluster/cluster_xid_stripe_boot.h"
#include "cluster/cluster_xid_stripe_xlog.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "storage/standbydefs.h"
#include "storage/standby.h"
#include "pgstat.h"
#include "utils/inval.h"
#include "utils/syscache.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/* Track the real payload allocator without replacing the production owner,
 * parser or its nested preflight calls. Other targets keep the ordinary object. */
static bool payload_tracking;
static void *payload_live[8];
static Size payload_sizes[8], payload_bytes, payload_peak;
extern void *side_xact_fixture_malloc(size_t size);
extern void side_xact_fixture_free(void *ptr);

void *
side_xact_fixture_malloc(size_t size)
{
	void *ptr = malloc(size);

	if (payload_tracking && ptr != NULL) {
		unsigned i;

		for (i = 0; i < lengthof(payload_live) && payload_live[i] != NULL; i++)
			;
		UT_ASSERT(i < lengthof(payload_live));
		if (i < lengthof(payload_live)) {
			payload_live[i] = ptr;
			payload_sizes[i] = size;
			payload_bytes += size;
			payload_peak = Max(payload_peak, payload_bytes);
		}
	}
	return ptr;
}

void
side_xact_fixture_free(void *ptr)
{
	for (unsigned i = 0; i < lengthof(payload_live); i++)
		if (ptr != NULL && payload_live[i] == ptr) {
			payload_bytes -= payload_sizes[i];
			payload_live[i] = NULL;
			break;
		}
	free(ptr);
}

#include "test_cluster_undo_header_identity.inc"
#include "test_cluster_heap_small_scn.inc"

sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

#include <stdio.h>

typedef struct PrepareApplyCapture {
	uint64 system_identifier;
	int origin_slot;
	TransactionId mismatched_origin_xid;
	TTSlot slot;
	TwoPhaseRecoveryPendingResult pending_preflight;
	TwoPhaseRecoveryPendingResult pending_install;
	TwoPhaseRecoveryPendingResult pending_read;
	TwoPhaseRecoveryPendingResult pending_resolve;
	ClusterRemoteXactMutationV2 projection_store;
	ClusterRemoteXactOutcome terminal_outcome;
	bool projection_postread;
	bool resolved_is_commit;
	uint8 native_payload[512];
	uint32 native_payload_length;
	uint32 tt_reads;
	uint32 tt_stamps;
	uint32 pending_preflights;
	uint32 pending_installs;
	uint32 pending_reads;
	uint32 pending_resolves;
	uint32 projection_stores;
	uint32 terminal_projection_stores;
	uint32 projection_postreads;
	uint32 next_order;
	uint32 install_order;
	uint32 projection_order;
	uint32 terminal_projection_order;
	uint32 resolve_order;
} PrepareApplyCapture;

static PrepareApplyCapture prepare_apply;
static FILE *canonical_file;
static int compound_io_drift;

/* Real file identity/extent change at the proof or write boundary, not a
 * failed-return stub in the strict reader that is being tested. */
static void
compound_change_file(bool replace)
{
	if (replace) {
		PGAlignedBlock page;
		FILE *replacement = tmpfile();

		UT_ASSERT(replacement != NULL);
		UT_ASSERT_EQ(pread(fileno(canonical_file), page.data, BLCKSZ, 0), BLCKSZ);
		UT_ASSERT_EQ(ftruncate(fileno(replacement), UNDO_SEGMENT_SIZE_BYTES), 0);
		UT_ASSERT_EQ(pwrite(fileno(replacement), page.data, BLCKSZ, 0), BLCKSZ);
		UT_ASSERT_EQ(fsync(fileno(replacement)), 0);
		UT_ASSERT_EQ(fclose(canonical_file), 0);
		canonical_file = replacement;
	} else
		UT_ASSERT_EQ(ftruncate(fileno(canonical_file), UNDO_SEGMENT_SIZE_BYTES + BLCKSZ), 0);
}
static const ClusterRecoveryDutyKey canonical_duty = { .origin_thread_id = 3 };
static ClusterUndoRecoveryScopeV1 *canonical_scope;
static uint32 canonical_writes, canonical_syncs, canonical_writer_depth;
static SCN canonical_projection_scn;
static SCN control_observed_scn;
static uint32 control_observe_calls;
static bool control_scope_test, control_scope_denied, control_fresh = true;
static int control_scope_fault;
static bool control_observe_throw, control_expire_after_observe;
static bool cold_control_test;
static unsigned cold_control_drift;
static bool cold_join_test;
static unsigned cold_join_preflights, cold_join_applies;
static int cold_join_fault = -1;
int cluster_node_id = 0;
static TimestampTz canonical_projection_time;
static uint16 canonical_projection_wrap;
static TransactionId canonical_expected_xid = 818;
static uint16 canonical_expected_wrap = 8;
static bool canonical_check_block, canonical_fail_data_write, canonical_corrupt_data_write;
static bool canonical_file_io_error, canonical_materialize_failure;
static int record_target_drift;
static bool head_data_drift;
static bool record_sync_failure;
static int native_abort_sync_drift;
static int ctrc_io_fault;
static bool cold_mx_test, cold_mx_online, cold_mx_preflight_fail, cold_mx_throw;
static unsigned cold_mx_preflights, cold_mx_projections;
static bool cold_clog_test, cold_clog_denied, cold_clog_apply_fail, cold_clog_verify_fail;
static bool cold_clog_throw, cold_clog_stale_after_verify;
static bool cold_abort_test, cold_abort_projection_conflict, cold_abort_projection_bad_read;
static uint32 cold_clog_applies, cold_clog_verifies, cold_clog_verified_order;
static bool cold_cache_test, cold_cache_throw, cold_cache_expire;
static bool cold_standalone_cache_test;
static uint32 cold_cache_calls, cold_cache_order;
static bool cold_cache_init;
static bool cold_completion_test, cold_source_ready, cold_source_fail, cold_source_throw;
static bool cold_stats_throw, cold_stats_expire;
static unsigned cold_source_calls, cold_source_order, cold_stats_calls, cold_stats_gc;
static unsigned cold_stats_order;
static bool cold_source_barrier_test, cold_source_drift;
HotStandbyState standbyState = STANDBY_DISABLED;
static bool cold_locks_test, cold_locks_throw, cold_locks_expire;
static unsigned cold_locks_calls, cold_locks_order;
static int cold_expected_children;
static const TransactionId cold_child_xids[] = { 930, 1058, 1186 };

static bool cold_assignment_test, assignment_refuse, assignment_throw, assignment_expire;
static unsigned assignment_preflights, assignment_applies, assignment_verifies;

/* Native file IO is covered separately by test_cluster_subtrans_recovery. */
static bool
assignment_call(int origin, TransactionId top, int count, const TransactionId *children)
{
	UT_ASSERT(cold_assignment_test && canonical_scope != NULL && origin == 2);
	UT_ASSERT(top == 800 && count == 2 && children[0] == 801 && children[1] == 802);
	return !assignment_refuse;
}
bool
cluster_subtrans_native_recovery_preflight_v1(int origin, TransactionId top, int count,
											  const TransactionId *children)
{
	assignment_preflights++;
	return assignment_call(origin, top, count, children);
}
bool
cluster_subtrans_native_recovery_apply_v1(int origin, TransactionId top, int count,
										  const TransactionId *children)
{
	assignment_applies++;
	UT_ASSERT(cold_source_ready && cold_source_calls > 0);
	if (assignment_throw)
		pg_re_throw();
	return assignment_call(origin, top, count, children);
}
bool
cluster_subtrans_native_recovery_verify_v1(int origin, TransactionId top, int count,
										   const TransactionId *children)
{
	assignment_verifies++;
	UT_ASSERT(assignment_applies == assignment_verifies);
	if (assignment_expire)
		control_fresh = false;
	return assignment_call(origin, top, count, children);
}

/* Only the native recovery-lock API boundary is a fixture. The complete
 * immutable plan, exact TT consumer and terminal ordering remain real. */
void
StandbyReleaseLockTree(TransactionId xid, int count, TransactionId *children)
{
	UT_ASSERT(cold_locks_test && canonical_scope != NULL);
	UT_ASSERT(standbyState >= STANDBY_INITIALIZED && standbyState <= STANDBY_SNAPSHOT_READY);
	UT_ASSERT_EQ(xid, 802);
	UT_ASSERT_EQ(count, cold_expected_children);
	UT_ASSERT(count == 0
				  ? children == NULL
				  : children != NULL
						&& memcmp(children, cold_child_xids, count * sizeof(TransactionId)) == 0);
	UT_ASSERT_EQ(cold_clog_verifies, 1);
	UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
	cold_locks_calls++;
	cold_locks_order = ++prepare_apply.next_order;
	if (cold_locks_throw)
		pg_re_throw();
	if (cold_locks_expire)
		cold_clog_denied = true;
}

char *DatabasePath;

bool
cluster_undo_recovery_cold_source_sync_v1(int node)
{
	UT_ASSERT(canonical_scope != NULL && node == 2);
	/* The real helper caches only within the qualified scope, not by xid or
	 * a process-global successful sync from a previous replay attempt. */
	if (canonical_scope->wal_synced)
		return true;
	if (cold_completion_test || cold_source_barrier_test)
		UT_ASSERT_EQ(canonical_writes, 0);
	cold_source_calls++;
	cold_source_order = ++prepare_apply.next_order;
	if (cold_source_throw)
		pg_re_throw();
	cold_source_ready = !cold_source_fail;
	canonical_scope->wal_synced = cold_source_ready;
	return cold_source_ready;
}

bool
cluster_undo_recovery_cold_source_synced_v1(int node)
{
	return cold_source_ready && cluster_undo_recovery_cold_origin_authorized_v1(node);
}

/* Execute the original native transactional-drop loop; only its runtime
 * statistics table and GC boundary are fixtures, not a second redo loop. */
static bool
pgstat_drop_entry(PgStat_Kind kind, Oid dbid, Oid object)
{
	const PgStat_Kind kinds[]
		= { PGSTAT_KIND_RELATION, PGSTAT_KIND_FUNCTION, PGSTAT_KIND_RELATION };
	const Oid dbids[] = { 5, 5, InvalidOid };
	const Oid objects[] = { 16388, 16390, 1260 };
	UT_ASSERT(cold_completion_test && cold_source_ready && canonical_scope != NULL);
	UT_ASSERT_EQ(cold_clog_verifies, 1);
	UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
	UT_ASSERT(cold_stats_calls < lengthof(kinds));
	if (cold_stats_calls >= lengthof(kinds))
		return false;
	UT_ASSERT_EQ(kind, kinds[cold_stats_calls]);
	UT_ASSERT_EQ(dbid, dbids[cold_stats_calls]);
	UT_ASSERT_EQ(object, objects[cold_stats_calls]);
	cold_stats_calls++;
	cold_stats_order = ++prepare_apply.next_order;
	if (cold_stats_throw)
		pg_re_throw();
	if (cold_stats_expire)
		cold_clog_denied = true;
	return cold_stats_calls != 2;
}

static void
pgstat_request_entry_refs_gc(void)
{
	cold_stats_gc++;
}

#include "test_cluster_side_xact_stats.inc"

void
ProcessCommittedInvalidationMessages(const SharedInvalidationMessage *msgs, int nmsgs,
									 bool initfile, Oid dbid, Oid tsid)
{
	UT_ASSERT(cold_cache_test && canonical_scope != NULL);
	UT_ASSERT_EQ(cold_clog_verifies, cold_standalone_cache_test ? 0 : 1);
	UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
	UT_ASSERT_EQ(nmsgs, 2);
	UT_ASSERT_EQ(dbid, 5);
	UT_ASSERT_EQ(tsid, 1663);
	UT_ASSERT_EQ(initfile, cold_cache_init);
	UT_ASSERT_EQ(msgs[0].rc.id, SHAREDINVALRELCACHE_ID);
	UT_ASSERT_EQ(msgs[0].rc.dbId, 5);
	UT_ASSERT_EQ(msgs[0].rc.relId, 16388);
	UT_ASSERT_EQ(msgs[1].cat.id, SHAREDINVALCATALOG_ID);
	UT_ASSERT_EQ(msgs[1].cat.dbId, InvalidOid);
	UT_ASSERT_EQ(msgs[1].cat.catId, 1260);
	if (cold_source_barrier_test)
		UT_ASSERT(cold_source_ready && cold_source_calls == 1);
	cold_cache_calls++;
	cold_cache_order = ++prepare_apply.next_order;
	if (cold_cache_throw) {
		DatabasePath = malloc(8); /* Native init-file pre-invalidate ERROR. */
		pg_re_throw();
	}
	if (cold_cache_expire)
		cold_clog_denied = true;
}

static XLogRecPtr canonical_expected_block_lsn = 400;
ClusterUndoTargetPreflightV1 fixture_preflight_tt_target(const ClusterUndoDecoded *d);
ClusterUndoApplyResultV1 fixture_apply_tt(const ClusterUndoDecoded *d);
static bool canonical_authority_fresh(void *arg);

bool
cluster_undo_recovery_scope_enter_v1(ClusterUndoRecoveryScopeV1 *scope,
									 const ClusterThreadRecoveryAuthorityV1 *authority,
									 const RfSideOnlinePlanV1 *plan)
{
	UT_ASSERT((canonical_file != NULL || control_scope_test) && canonical_scope == NULL
			  && plan != NULL && authority != NULL);
	memset(scope, 0, sizeof(*scope));
	scope->authority = authority;
	scope->plan = plan;
	scope->duty = *authority->duty;
	cold_source_ready = false;
	if (control_scope_test) {
		if (control_scope_denied)
			return false;
		scope->authority = authority;
		scope->plan = plan;
		scope->duty = *authority->duty;
		scope->cut.failed_thread = 3;
		scope->cut.origin_owner_incarnation = 9;
		scope->cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		scope->cut.timeline_id = 7;
		scope->cut.scan_begin_inclusive = 100;
		scope->cut.scan_end_exclusive = 300;
		if (control_scope_fault == 7)
			scope->cut.scan_begin_inclusive++;
		if (control_scope_fault == 8)
			scope->cut.scan_end_exclusive--;
		if (control_scope_fault == 9)
			scope->cut.origin_owner_incarnation = 0;
		if (control_scope_fault == 10)
			scope->cut.origin_owner_incarnation++;
		if (control_scope_fault == 11)
			scope->cut.timeline_id++;
		if (control_scope_fault == 13)
			scope->cut.flags = 0;
		if (control_scope_fault == 14)
			scope->plan = NULL;
		if (control_scope_fault == 15)
			scope->duty.system_identifier++;
	}
	canonical_scope = scope;
	return true;
}

bool
cluster_undo_recovery_origin_authorized_v1(int node)
{
	return control_scope_test && !control_scope_denied && canonical_scope != NULL && node == 2
		   && canonical_scope->duty.origin_thread_id == 3;
}

bool
cluster_undo_recovery_cold_origin_authorized_v1(int node)
{
	return (cold_clog_test || cold_mx_test || cold_standalone_cache_test || cold_assignment_test
			|| cold_control_test)
		   && !cold_clog_denied && canonical_scope != NULL && node == 2;
}

/* The native adapter runs real SLRU in test_cluster_multixact_recovery. */
bool
cluster_multixact_native_recovery_preflight(int origin, const ClusterSideProjectionOperationV1 *op,
											const uint8 *payload, uint32 length, XLogRecPtr begin,
											XLogRecPtr end)
{
	UT_ASSERT(cold_mx_test && !cold_mx_online && canonical_scope != NULL);
	UT_ASSERT_EQ(origin, 2);
	UT_ASSERT_EQ(op->kind, CLUSTER_SIDE_PROJECTION_MULTIXACT);
	UT_ASSERT(payload == NULL && length == 0 && begin >= 100 && end == begin + 100);
	cold_mx_preflights++;
	if (cold_mx_throw)
		pg_re_throw();
	return !cold_mx_preflight_fail && op->page_number < 524288;
}

/* Exact native CLOG IO is covered by test_cluster_clog_recovery. This boundary
 * checks the original real planner/owner's call identity and publication order. */
static bool cold_clog_record_test, cold_clog_record_online;
static unsigned cold_clog_record_projections;

bool
cluster_clog_native_recovery_record_apply_v1(int origin, const ClusterSideProjectionOperationV1 *op)
{
	UT_ASSERT(cold_clog_record_test && !cold_clog_record_online && canonical_scope != NULL);
	UT_ASSERT_EQ(origin, 2);
	UT_ASSERT_EQ(op->kind, CLUSTER_SIDE_PROJECTION_CLOG);
	UT_ASSERT_EQ(op->page_number, 97);
	UT_ASSERT_EQ(cold_clog_record_projections, 0);
	if (cold_source_barrier_test)
		UT_ASSERT(cold_source_ready && cold_source_calls == 1);
	cold_clog_applies++;
	if (cold_clog_throw)
		siglongjmp(*PG_exception_stack, 1);
	return !cold_clog_apply_fail;
}
bool
cluster_clog_native_recovery_record_verify_v1(int origin,
											  const ClusterSideProjectionOperationV1 *op)
{
	UT_ASSERT_EQ(origin, 2);
	UT_ASSERT_EQ(op->page_number, 97);
	UT_ASSERT_EQ(cold_clog_applies, 1);
	UT_ASSERT_EQ(cold_clog_record_projections, 0);
	cold_clog_verifies++;
	if (cold_clog_stale_after_verify)
		cold_clog_denied = true;
	return !cold_clog_verify_fail;
}

bool
cluster_clog_native_recovery_apply_v1(int origin, TransactionId xid, int count,
									  const TransactionId *children, XidStatus status)
{
	UT_ASSERT(cold_clog_test && canonical_scope != NULL && canonical_syncs > 0);
	UT_ASSERT_EQ(origin, 2);
	UT_ASSERT_EQ(xid, 802);
	UT_ASSERT_EQ(count, cold_expected_children);
	UT_ASSERT(count == 0
				  ? children == NULL
				  : children != NULL
						&& memcmp(children, cold_child_xids, count * sizeof(TransactionId)) == 0);
	UT_ASSERT_EQ(status,
				 cold_abort_test ? TRANSACTION_STATUS_ABORTED : TRANSACTION_STATUS_COMMITTED);
	UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
	if (cold_source_barrier_test)
		UT_ASSERT(cold_source_ready && cold_source_calls == 1);
	cold_clog_applies++;
	if (cold_clog_throw)
		siglongjmp(*PG_exception_stack, 1);
	return !cold_clog_apply_fail;
}

bool
cluster_clog_native_recovery_verify_v1(int origin, TransactionId xid, int count,
									   const TransactionId *children, XidStatus status)
{
	UT_ASSERT_EQ(cold_clog_applies, 1);
	UT_ASSERT_EQ(origin, 2);
	UT_ASSERT_EQ(xid, 802);
	UT_ASSERT_EQ(count, cold_expected_children);
	UT_ASSERT(count == 0
				  ? children == NULL
				  : children != NULL
						&& memcmp(children, cold_child_xids, count * sizeof(TransactionId)) == 0);
	UT_ASSERT_EQ(status,
				 cold_abort_test ? TRANSACTION_STATUS_ABORTED : TRANSACTION_STATUS_COMMITTED);
	UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
	cold_clog_verifies++;
	cold_clog_verified_order = ++prepare_apply.next_order;
	if (cold_clog_stale_after_verify)
		cold_clog_denied = true;
	return !cold_clog_verify_fail;
}

void
cluster_undo_recovery_scope_leave_v1(ClusterUndoRecoveryScopeV1 *scope)
{
	if (canonical_scope == scope)
		canonical_scope = NULL;
}
void
cluster_remote_xact_online_writer_push(void)
{
	canonical_writer_depth++;
}
void
cluster_remote_xact_online_writer_pop(void)
{
	canonical_writer_depth--;
}

bool
cluster_undo_smgr_read_block(ClusterUndoPathIntent intent, uint32 segment, uint8 instance,
							 uint32 block, char *out)
{
	bool ok;

	UT_ASSERT(canonical_scope != NULL && intent == CLUSTER_UNDO_PATH_RECOVERY_SHARED);
	UT_ASSERT(segment == 513 && instance == 3 && (block == 0 || block == 9));
	ok = pread(fileno(canonical_file), out, BLCKSZ, (off_t)block * BLCKSZ) == BLCKSZ;
	if (ok && ctrc_io_fault == 2 && canonical_writes != 0)
		((UndoSegmentHeaderData *)out)->tt_slots[4].flags = TT_FLAGS_RESERVED;
	if (ok && block == 0 && ctrc_io_fault == 5 && canonical_writes != 0)
		((UndoSegmentHeaderData *)out)->tt_slots[4].xid++;
	return ok;
}

bool
cluster_undo_smgr_write_block(ClusterUndoPathIntent intent, uint32 segment, uint8 instance,
							  uint32 block, const char *data, bool sync)
{
	UT_ASSERT(canonical_scope != NULL && intent == CLUSTER_UNDO_PATH_RECOVERY_SHARED);
	UT_ASSERT(segment == 513 && instance == 3 && (block == 0 || block == 9) && sync);
	if (cold_source_barrier_test)
		UT_ASSERT(cold_source_ready && cold_source_calls == 1);
	canonical_writes++;
	if (ctrc_io_fault == 1)
		return false;
	if (block != 0 && canonical_fail_data_write)
		return false;
	if (block == 0 && canonical_check_block) {
		PGAlignedBlock physical;

		UT_ASSERT(canonical_syncs != 0);
		UT_ASSERT(pread(fileno(canonical_file), physical.data, BLCKSZ, 9 * BLCKSZ) == BLCKSZ);
		UT_ASSERT_EQ(((UndoBlockHeader *)physical.data)->block_lsn, canonical_expected_block_lsn);
		UT_ASSERT_EQ((uint8)physical.data[128], 0x31);
		UT_ASSERT_EQ((uint8)physical.data[sizeof(UndoBlockHeader)], 0x6b);
	}
	if (pwrite(fileno(canonical_file), data, BLCKSZ, (off_t)block * BLCKSZ) != BLCKSZ)
		return false;
	if (block != 0 && canonical_corrupt_data_write) {
		uint8 wrong = 0x99;
		UT_ASSERT(pwrite(fileno(canonical_file), &wrong, 1, (off_t)block * BLCKSZ + 128) == 1);
	}
	canonical_syncs++;
	if (ctrc_io_fault == 3)
		pg_re_throw();
	if (fsync(fileno(canonical_file)) != 0)
		return false;
	if (compound_io_drift != 0)
		compound_change_file(compound_io_drift == 2);
	return true;
}

bool
cluster_undo_smgr_fsync_segment_file(uint32 segment, uint8 instance)
{
	UT_ASSERT(canonical_scope != NULL && segment == 513 && instance == 3);
	canonical_syncs++;
	if (native_abort_sync_drift == 1)
		UT_ASSERT_EQ(ftruncate(fileno(canonical_file), UNDO_SEGMENT_SIZE_BYTES + BLCKSZ), 0);
	if (native_abort_sync_drift == 2) {
		PGAlignedBlock changed;

		UT_ASSERT(pread(fileno(canonical_file), changed.data, BLCKSZ, 0) == BLCKSZ);
		((UndoSegmentHeaderData *)changed.data)->wrap_count++;
		UT_ASSERT(pwrite(fileno(canonical_file), changed.data, BLCKSZ, 0) == BLCKSZ);
	}
	return !record_sync_failure && fsync(fileno(canonical_file)) == 0;
}

bool
cluster_undo_smgr_recovery_probe_v1(uint32 segment, uint8 instance,
									ClusterUndoSmgrRecoveryFileV1 *file, char block0[BLCKSZ])
{
	struct stat st;
	Size length;

	UT_ASSERT(canonical_scope != NULL && segment == 513 && instance == 3);
	if (canonical_file_io_error || fstat(fileno(canonical_file), &st) != 0)
		return false;
	memset(file, 0, sizeof(*file));
	file->exists = true;
	file->device = st.st_dev;
	file->inode = st.st_ino;
	file->size = st.st_size;
	length = Min(file->size, BLCKSZ);
	memset(block0, 0, BLCKSZ);
	return pread(fileno(canonical_file), block0, length, 0) == (ssize_t)length;
}

bool
cluster_undo_smgr_recovery_read_block_v1(uint32 segment, uint8 instance, uint32 block,
										 const ClusterUndoSmgrRecoveryFileV1 *file,
										 char out[BLCKSZ])
{
	uint64 offset = (uint64)block * BLCKSZ;
	Size length = file->size <= offset ? 0 : Min(file->size - offset, BLCKSZ);
	struct stat observed;

	UT_ASSERT(canonical_scope != NULL && segment == 513 && instance == 3);
	if (canonical_file_io_error)
		return false;
	if (block == 0 && record_target_drift != 0
		&& ((record_target_drift <= 2) || canonical_writes != 0)) {
		if ((record_target_drift & 1) != 0)
			UT_ASSERT_EQ(ftruncate(fileno(canonical_file), UNDO_SEGMENT_SIZE_BYTES + BLCKSZ), 0);
		else {
			PGAlignedBlock changed;
			UT_ASSERT(pread(fileno(canonical_file), changed.data, BLCKSZ, 0) == BLCKSZ);
			((UndoSegmentHeaderData *)changed.data)->wrap_count++;
			UT_ASSERT(pwrite(fileno(canonical_file), changed.data, BLCKSZ, 0) == BLCKSZ);
		}
	}
	if (fstat(fileno(canonical_file), &observed) != 0 || !file->exists
		|| observed.st_dev != file->device || observed.st_ino != file->inode
		|| observed.st_size != file->size)
		return false;
	memset(out, 0, BLCKSZ);
	if (pread(fileno(canonical_file), out, length, offset) != (ssize_t)length)
		return false;
	if (head_data_drift && block == 9 && canonical_writes != 0)
		out[BLCKSZ / 2] ^= 1;
	return true;
}

bool
cluster_undo_smgr_recovery_materialize_v1(uint32 segment, uint8 instance,
										  const ClusterUndoSmgrRecoveryFileV1 *expected,
										  const char base[BLCKSZ], const char final[BLCKSZ])
{
	ClusterUndoSmgrRecoveryFileV1 observed;
	PGAlignedBlock current;

	if (canonical_materialize_failure
		|| !cluster_undo_smgr_recovery_probe_v1(segment, instance, &observed, current.data))
		return false;
	UT_ASSERT(UndoSegmentHeader_identity_matches(final, segment, instance));
	if (observed.size != expected->size || observed.inode != expected->inode
		|| memcmp(current.data, base, BLCKSZ) != 0)
		return false;
	canonical_syncs++;
	return ftruncate(fileno(canonical_file), UNDO_SEGMENT_SIZE_BYTES) == 0
		   && fsync(fileno(canonical_file)) == 0;
}

bool
rf_side_online_projection_owner_init_v1(RfSideOnlineProjectionOwnerV1 *owner, uint32 epoch,
										bool retained)
{
	memset(owner, 0, sizeof(*owner));
	return epoch != 0 && retained;
}
bool
rf_side_online_projection_preflight_owned_v1(void *arg, const RfSideOnlineOperationV1 *op)
{
	UT_ASSERT((cold_clog_record_test || cold_mx_test) && arg != NULL && op != NULL);
	return cold_clog_record_test || cold_mx_test;
}
bool
rf_side_online_projection_apply_owned_v1(void *arg, const RfSideOnlineOperationV1 *op)
{
	if (cold_mx_test) {
		UT_ASSERT(arg != NULL && op != NULL);
		UT_ASSERT(cold_mx_online || cold_mx_preflights >= 2);
		if (cold_source_barrier_test)
			UT_ASSERT(cold_source_ready && cold_source_calls == 1);
		cold_mx_projections++;
		return true;
	}
	UT_ASSERT(cold_clog_record_test && arg != NULL && op != NULL);
	UT_ASSERT(cold_clog_record_online || (cold_clog_applies == 1 && cold_clog_verifies == 1));
	cold_clog_record_projections++;
	return cold_clog_record_test;
}
ClusterUndoTargetPreflightV1
fixture_preflight_tt_target(const ClusterUndoDecoded *d)
{
	UT_ASSERT(false);
	return CLUSTER_UNDO_TARGET_BLOCKED;
}
ClusterUndoApplyResultV1
fixture_apply_tt(const ClusterUndoDecoded *d)
{
	UT_ASSERT(false);
	return CLUSTER_UNDO_APPLY_BLOCKED;
}

static void
reset_prepare_apply(void)
{
	memset(&prepare_apply, 0, sizeof(prepare_apply));
	prepare_apply.system_identifier = UINT64_C(0x11223344);
	prepare_apply.origin_slot = 2;
	prepare_apply.slot.xid = 802;
	prepare_apply.slot.wrap = 7;
	prepare_apply.slot.status = TT_SLOT_ACTIVE;
	prepare_apply.slot.commit_scn = InvalidScn;
	prepare_apply.pending_preflight = TWOPHASE_RECOVERY_PENDING_OK;
	prepare_apply.pending_install = TWOPHASE_RECOVERY_PENDING_OK;
	prepare_apply.pending_read = TWOPHASE_RECOVERY_PENDING_OK;
	prepare_apply.pending_resolve = TWOPHASE_RECOVERY_PENDING_OK;
	prepare_apply.projection_store = CLUSTER_REMOTE_XACT_MUTATION_STORED;
	prepare_apply.terminal_outcome = CLUSTER_REMOTE_XACT_COMMITTED;
	prepare_apply.projection_postread = true;
}

uint64
GetSystemIdentifier(void)
{
	return prepare_apply.system_identifier;
}

int
cluster_xid_origin_slot(TransactionId xid pg_attribute_unused())
{
	if (TransactionIdIsValid(prepare_apply.mismatched_origin_xid)
		&& TransactionIdEquals(xid, prepare_apply.mismatched_origin_xid))
		return prepare_apply.origin_slot + 1;
	return prepare_apply.origin_slot;
}

bool
cluster_tt_slot_durable_read_exact_stable(uint32 segment_id pg_attribute_unused(),
										  uint16 slot_offset pg_attribute_unused(),
										  TransactionId xid pg_attribute_unused(),
										  uint16 expected_wrap pg_attribute_unused(),
										  TTSlot *slot_out)
{
	prepare_apply.tt_reads++;
	*slot_out = prepare_apply.slot;
	return true;
}

TwoPhaseRecoveryPendingResult
TwoPhaseRecoveryPendingPreflight(TransactionId xid pg_attribute_unused(),
								 Oid database pg_attribute_unused(),
								 Oid owner pg_attribute_unused(),
								 TimestampTz prepared_at pg_attribute_unused(),
								 const char *gid pg_attribute_unused(),
								 const void *content pg_attribute_unused(),
								 uint32 len pg_attribute_unused())
{
	prepare_apply.pending_preflights++;
	return prepare_apply.pending_preflight;
}

TwoPhaseRecoveryPendingResult
TwoPhaseRecoveryPendingInstall(TransactionId xid pg_attribute_unused(),
							   Oid database pg_attribute_unused(), Oid owner pg_attribute_unused(),
							   TimestampTz prepared_at pg_attribute_unused(),
							   const char *gid pg_attribute_unused(),
							   const void *content pg_attribute_unused(),
							   uint32 len pg_attribute_unused())
{
	prepare_apply.pending_installs++;
	prepare_apply.install_order = ++prepare_apply.next_order;
	return prepare_apply.pending_install;
}

TwoPhaseRecoveryPendingResult
TwoPhaseRecoveryPendingReadExact(TransactionId xid pg_attribute_unused(),
								 Oid database pg_attribute_unused(),
								 const char *gid pg_attribute_unused(), void **content_out,
								 uint32 *len_out)
{
	prepare_apply.pending_reads++;
	if (prepare_apply.pending_read != TWOPHASE_RECOVERY_PENDING_OK)
		return prepare_apply.pending_read;
	if (content_out == NULL || len_out == NULL || prepare_apply.native_payload_length == 0)
		return TWOPHASE_RECOVERY_PENDING_BLOCKED;
	*content_out = malloc(prepare_apply.native_payload_length);
	memcpy(*content_out, prepare_apply.native_payload, prepare_apply.native_payload_length);
	*len_out = prepare_apply.native_payload_length;
	return TWOPHASE_RECOVERY_PENDING_OK;
}

TwoPhaseRecoveryPendingResult
TwoPhaseRecoveryPendingResolveExact(TransactionId xid pg_attribute_unused(),
									Oid database pg_attribute_unused(),
									const char *gid pg_attribute_unused(), const void *content,
									uint32 len, bool isCommit)
{
	prepare_apply.pending_resolves++;
	prepare_apply.resolved_is_commit = isCommit;
	prepare_apply.resolve_order = ++prepare_apply.next_order;
	if (content == NULL || len != prepare_apply.native_payload_length
		|| memcmp(content, prepare_apply.native_payload, len) != 0)
		return TWOPHASE_RECOVERY_PENDING_CONFLICT;
	return prepare_apply.pending_resolve;
}

ClusterRemoteXactMutationV2
cluster_remote_xact_store_prepared_v2(
	int origin_node pg_attribute_unused(), TransactionId xid pg_attribute_unused(),
	const uint8 digest[CLUSTER_REMOTE_XACT_PREPARE_DIGEST_BYTES] pg_attribute_unused())
{
	prepare_apply.projection_stores++;
	prepare_apply.projection_order = ++prepare_apply.next_order;
	return prepare_apply.projection_store;
}

bool
cluster_remote_xact_pending_matches_v2(
	int origin_node pg_attribute_unused(), TransactionId xid pg_attribute_unused(),
	const uint8 digest[CLUSTER_REMOTE_XACT_PREPARE_DIGEST_BYTES] pg_attribute_unused())
{
	prepare_apply.projection_postreads++;
	return prepare_apply.projection_postread;
}

void
cluster_tt_durable_redo_stamp_slot(uint8 instance pg_attribute_unused(),
								   uint32 segment_id pg_attribute_unused(),
								   uint16 slot_offset pg_attribute_unused(),
								   uint16 wrap pg_attribute_unused(),
								   TransactionId xid pg_attribute_unused(),
								   SCN commit_scn pg_attribute_unused())
{}

void
cluster_tt_durable_redo_stamp_slot_exact(uint8 instance pg_attribute_unused(),
										 uint32 segment_id pg_attribute_unused(),
										 uint32 segment_generation pg_attribute_unused(),
										 uint16 slot_offset pg_attribute_unused(),
										 uint16 wrap pg_attribute_unused(),
										 TransactionId xid pg_attribute_unused(),
										 SCN commit_scn pg_attribute_unused())
{
	prepare_apply.tt_stamps++;
}

ClusterTTDurableResolve
cluster_tt_slot_durable_resolve_by_xid_origin(int origin_node pg_attribute_unused(),
											  TransactionId xid pg_attribute_unused(),
											  uint32 expected_wrap pg_attribute_unused(),
											  SCN *commit_scn, uint16 *out_seg, uint16 *out_slot,
											  uint16 *out_wrap)
{
	*commit_scn = UINT64_C(901);
	*out_seg = 9;
	*out_slot = 4;
	*out_wrap = 7;
	return CLUSTER_TT_DURABLE_RESOLVED_SCN;
}

void
cluster_scn_recovery_replay_observe(SCN scn)
{
	if (control_scope_test) {
		UT_ASSERT(canonical_scope != NULL && canonical_writer_depth == 1);
		if (control_observe_throw)
			pg_re_throw();
		control_observed_scn = Max(control_observed_scn, scn);
		control_observe_calls++;
		if (control_expire_after_observe)
			control_fresh = false;
		if (cold_control_drift == 1)
			standbyState = STANDBY_INITIALIZED;
		else if (cold_control_drift == 2)
			cluster_node_id = 2;
		else if (cold_control_drift == 3)
			((ClusterRecoverySerialGuard *)canonical_scope->authority->serial_guard)->mode
				= CLUSTER_RECOVERY_SERIAL_ONLINE;
	}
}

/* The actual native publisher/readback is tested against original shmem in
 * test_cluster_xid_stripe_boot; this boundary verifies the production owner. */
bool
cluster_xid_stripe_replay_join_v1(uint64 floor, uint64 epoch, int slot, bool apply)
{
	UT_ASSERT(cold_join_test && canonical_scope != NULL && canonical_writer_depth == 1);
	UT_ASSERT_EQ(floor, 4195104);
	UT_ASSERT_EQ(epoch, 1);
	UT_ASSERT_EQ(slot, 2);
	if (!apply) {
		cold_join_preflights++;
		return cold_join_fault != 0 && cold_join_fault != 2;
	}
	cold_join_applies++;
	if (cold_join_fault == 1)
		return false;
	if (cold_join_fault == 3)
		control_fresh = false;
	if (cold_join_fault == 4)
		cold_join_fault = 2; /* Native state lost before its postread. */
	if (cold_join_fault == 5)
		pg_re_throw();
	return true;
}

ClusterRemoteXactMutationV2
cluster_remote_xact_store_terminal_v2(
	int origin_node pg_attribute_unused(), TransactionId xid pg_attribute_unused(),
	bool require_prepared pg_attribute_unused(),
	const uint8
		expected_prepare_digest[CLUSTER_REMOTE_XACT_PREPARE_DIGEST_BYTES] pg_attribute_unused(),
	ClusterRemoteXactOutcome outcome, SCN commit_scn pg_attribute_unused(),
	TimestampTz commit_timestamp pg_attribute_unused(), bool wrap_valid pg_attribute_unused(),
	uint16 wrap pg_attribute_unused())
{
	prepare_apply.terminal_projection_stores++;
	if (canonical_file != NULL) {
		PGAlignedBlock disk;
		TTSlot *slot;

		UT_ASSERT(canonical_scope != NULL && canonical_syncs != 0);
		UT_ASSERT(pread(fileno(canonical_file), disk.data, BLCKSZ, 0) == BLCKSZ);
		slot = &((UndoSegmentHeaderData *)disk.data)->tt_slots[4];
		UT_ASSERT(slot->status == (cold_abort_test ? TT_SLOT_ABORTED : TT_SLOT_COMMITTED)
				  && slot->xid == canonical_expected_xid && slot->wrap == canonical_expected_wrap);
		canonical_projection_scn = commit_scn;
		canonical_projection_time = commit_timestamp;
		canonical_projection_wrap = wrap;
	}
	if (cold_abort_test) {
		UT_ASSERT_EQ(outcome, CLUSTER_REMOTE_XACT_ABORTED);
		UT_ASSERT(!require_prepared && commit_scn == InvalidScn && commit_timestamp == 0
				  && !wrap_valid && wrap == 0);
		UT_ASSERT_EQ(cold_clog_verifies, 1);
		if (cold_abort_projection_conflict)
			return CLUSTER_REMOTE_XACT_MUTATION_CONFLICT;
	}
	prepare_apply.terminal_outcome = outcome;
	prepare_apply.terminal_projection_order = ++prepare_apply.next_order;
	return CLUSTER_REMOTE_XACT_MUTATION_STORED;
}

ClusterRemoteXactOutcome
cluster_remote_commit_outcome_ex(int origin_node pg_attribute_unused(),
								 TransactionId xid pg_attribute_unused(), SCN *commit_scn,
								 uint16 *out_wrap, bool *out_wrap_valid)
{
	if (prepare_apply.terminal_outcome == CLUSTER_REMOTE_XACT_COMMITTED) {
		*commit_scn = UINT64_C(901);
		*out_wrap = 7;
		*out_wrap_valid = true;
		if (canonical_file != NULL) {
			*commit_scn = canonical_projection_scn;
			*out_wrap = canonical_projection_wrap;
		}
	} else {
		*commit_scn = InvalidScn;
		*out_wrap = 0;
		*out_wrap_valid = false;
	}
	return cold_abort_projection_bad_read ? CLUSTER_REMOTE_XACT_INDOUBT
										  : prepare_apply.terminal_outcome;
}

bool
cluster_remote_commit_timestamp(int origin_node pg_attribute_unused(),
								TransactionId xid pg_attribute_unused(),
								TimestampTz *commit_timestamp)
{
	if (prepare_apply.terminal_outcome != CLUSTER_REMOTE_XACT_COMMITTED)
		return false;
	*commit_timestamp = INT64_C(123456);
	if (canonical_file != NULL)
		*commit_timestamp = canonical_projection_time;
	return true;
}

typedef struct FakeXactRecord {
	XLogReaderState reader;
	uint8 data[BLCKSZ + 1024];
	union {
		DecodedXLogRecord decoded;
		char pad[sizeof(DecodedXLogRecord) + 2 * sizeof(DecodedBkpBlock)];
	} u;
} FakeXactRecord;

static XLogReaderState *
make_commit(FakeXactRecord *fake, TransactionId xid, SCN scn, TimestampTz timestamp,
			bool conflicting_tt)
{
	xl_xact_commit commit;
	xl_xact_xinfo xinfo;
	xl_xact_scn wal_scn;
	xl_xact_tt_commit tt;
	uint8 *cursor;

	memset(fake, 0, sizeof(*fake));
	memset(&commit, 0, sizeof(commit));
	memset(&xinfo, 0, sizeof(xinfo));
	memset(&wal_scn, 0, sizeof(wal_scn));
	memset(&tt, 0, sizeof(tt));
	commit.xact_time = timestamp;
	xinfo.xinfo = XACT_XINFO_HAS_SCN | XACT_XINFO_HAS_TT_COMMIT;
	wal_scn.scn = scn;
	tt.instance = 3;
	tt.segment_id = 513;
	tt.segment_generation = 11;
	tt.slot_offset = 4;
	tt.wrap = 7;
	tt.xid = conflicting_tt ? xid + 1 : xid;
	tt.format_version = CLUSTER_XACT_TT_COMMIT_VERSION;
	tt.commit_scn = scn;
	cursor = fake->data;
	memcpy(cursor, &commit, sizeof(commit));
	cursor += sizeof(commit);
	memcpy(cursor, &xinfo, sizeof(xinfo));
	cursor += sizeof(xinfo);
	memcpy(cursor, &wal_scn, sizeof(wal_scn));
	cursor += sizeof(wal_scn);
	memcpy(cursor, &tt, sizeof(tt));
	cursor += sizeof(tt);
	fake->u.decoded.header.xl_rmid = RM_XACT_ID;
	fake->u.decoded.header.xl_info = XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO;
	fake->u.decoded.header.xl_xid = xid;
	fake->u.decoded.main_data = (char *)fake->data;
	fake->u.decoded.main_data_len = (uint32)(cursor - fake->data);
	fake->reader.record = &fake->u.decoded;
	return &fake->reader;
}

static XLogReaderState *
make_prepare_with_head(FakeXactRecord *fake, TransactionId xid, bool with_tt, bool truncated_gid,
					   const UBA *head)
{
	typedef struct FakeTwoPhaseRecordOnDisk {
		uint32 len;
		TwoPhaseRmgrId rmid;
		uint16 info;
	} FakeTwoPhaseRecordOnDisk;
	xl_xact_prepare prepare;
	ClusterTT2PCBinding binding;
	FakeTwoPhaseRecordOnDisk disk_record;
	Size header_size = MAXALIGN(sizeof(prepare));
	Size payload_size;
	uint32 tt_size;
	char *cursor;

	memset(fake, 0, sizeof(*fake));
	memset(&prepare, 0, sizeof(prepare));
	memset(&binding, 0, sizeof(binding));
	memset(&disk_record, 0, sizeof(disk_record));
	prepare.magic = UINT32_C(0x57F94534);
	prepare.xid = xid;
	prepare.database = 16384;
	prepare.owner = 10;
	prepare.prepared_at = INT64_C(123456);
	prepare.gidlen = 4;
	cursor = (char *)fake->data + header_size;
	memcpy(cursor, "gid", 4);
	cursor += MAXALIGN(4);
	if (with_tt) {
		binding.undo_segment_id = 513;
		binding.slot_offset = 4;
		binding.wrap = 7;
		binding.cluster_epoch = 11;
		binding.xid = xid;
		tt_size = cluster_tt_2pc_record_size(CLUSTER_TT_2PC_VERSION, 1, 0);
		disk_record.len = tt_size;
		disk_record.rmid = TWOPHASE_RM_CLUSTER_TT_ID;
		memcpy(cursor, &disk_record, sizeof(disk_record));
		cursor += MAXALIGN(sizeof(disk_record));
		UT_ASSERT_EQ(cluster_tt_2pc_serialize(&binding, head, 1, NULL, 0, cursor, tt_size),
					 tt_size);
		cursor += MAXALIGN(tt_size);
	}
	memset(&disk_record, 0, sizeof(disk_record));
	disk_record.rmid = TWOPHASE_RM_END_ID;
	memcpy(cursor, &disk_record, sizeof(disk_record));
	cursor += MAXALIGN(sizeof(disk_record));
	payload_size = (Size)(cursor - (char *)fake->data);
	prepare.total_len = (uint32)payload_size + sizeof(uint32);
	memcpy(fake->data, &prepare, sizeof(prepare));
	fake->u.decoded.header.xl_rmid = RM_XACT_ID;
	fake->u.decoded.header.xl_info = XLOG_XACT_PREPARE;
	fake->u.decoded.main_data = (char *)fake->data;
	fake->u.decoded.main_data_len = truncated_gid ? (uint32)header_size : (uint32)payload_size;
	fake->reader.record = &fake->u.decoded;
	return &fake->reader;
}

static XLogReaderState *
make_prepare(FakeXactRecord *fake, TransactionId xid, bool with_tt, bool truncated_gid)
{
	return make_prepare_with_head(fake, xid, with_tt, truncated_gid, NULL);
}

static XLogReaderState *
make_commit_prepared(FakeXactRecord *fake, TransactionId xid, Oid database, SCN scn,
					 TimestampTz timestamp)
{
	xl_xact_commit commit;
	xl_xact_xinfo xinfo;
	xl_xact_dbinfo dbinfo;
	xl_xact_twophase twophase;
	xl_xact_scn wal_scn;
	char *cursor;

	memset(fake, 0, sizeof(*fake));
	memset(&commit, 0, sizeof(commit));
	memset(&xinfo, 0, sizeof(xinfo));
	memset(&dbinfo, 0, sizeof(dbinfo));
	memset(&twophase, 0, sizeof(twophase));
	memset(&wal_scn, 0, sizeof(wal_scn));
	commit.xact_time = timestamp;
	xinfo.xinfo
		= XACT_XINFO_HAS_DBINFO | XACT_XINFO_HAS_TWOPHASE | XACT_XINFO_HAS_GID | XACT_XINFO_HAS_SCN;
	dbinfo.dbId = database;
	twophase.xid = xid;
	wal_scn.scn = scn;
	cursor = (char *)fake->data;
	memcpy(cursor, &commit, sizeof(commit));
	cursor += sizeof(commit);
	memcpy(cursor, &xinfo, sizeof(xinfo));
	cursor += sizeof(xinfo);
	memcpy(cursor, &dbinfo, sizeof(dbinfo));
	cursor += sizeof(dbinfo);
	memcpy(cursor, &twophase, sizeof(twophase));
	cursor += sizeof(twophase);
	memcpy(cursor, "gid", 4);
	cursor += 4;
	memcpy(cursor, &wal_scn, sizeof(wal_scn));
	cursor += sizeof(wal_scn);
	fake->u.decoded.header.xl_rmid = RM_XACT_ID;
	fake->u.decoded.header.xl_info = XLOG_XACT_COMMIT_PREPARED | XLOG_XACT_HAS_INFO;
	fake->u.decoded.main_data = (char *)fake->data;
	fake->u.decoded.main_data_len = (uint32)(cursor - (char *)fake->data);
	fake->reader.record = &fake->u.decoded;
	return &fake->reader;
}

static XLogReaderState *
make_abort(FakeXactRecord *fake, TransactionId xid)
{
	xl_xact_abort abort = { .xact_time = 123456 };
	xl_xact_xinfo xinfo = { .xinfo = XACT_XINFO_HAS_DBINFO | XACT_XINFO_HAS_SCN };
	xl_xact_dbinfo dbinfo = { .dbId = 999 };
	xl_xact_scn scn = { .scn = 12345 };
	char *cursor;

	memset(fake, 0, sizeof(*fake));
	cursor = (char *)fake->data;
	memcpy(cursor, &abort, sizeof(abort));
	cursor += sizeof(abort);
	memcpy(cursor, &xinfo, sizeof(xinfo));
	cursor += sizeof(xinfo);
	memcpy(cursor, &dbinfo, sizeof(dbinfo));
	cursor += sizeof(dbinfo);
	memcpy(cursor, &scn, sizeof(scn));
	cursor += sizeof(scn);
	fake->u.decoded.header.xl_rmid = RM_XACT_ID;
	fake->u.decoded.header.xl_info = XLOG_XACT_ABORT | XLOG_XACT_HAS_INFO;
	fake->u.decoded.header.xl_xid = xid;
	fake->u.decoded.main_data = (char *)fake->data;
	fake->u.decoded.main_data_len = (uint32)(cursor - (char *)fake->data);
	fake->reader.record = &fake->u.decoded;
	return &fake->reader;
}

static XLogReaderState *
make_abort_prepared(FakeXactRecord *fake, TransactionId xid, Oid database, SCN scn)
{
	xl_xact_abort abort;
	xl_xact_xinfo xinfo;
	xl_xact_dbinfo dbinfo;
	xl_xact_twophase twophase;
	xl_xact_scn wal_scn;
	char *cursor;

	memset(fake, 0, sizeof(*fake));
	memset(&abort, 0, sizeof(abort));
	memset(&xinfo, 0, sizeof(xinfo));
	memset(&dbinfo, 0, sizeof(dbinfo));
	memset(&twophase, 0, sizeof(twophase));
	memset(&wal_scn, 0, sizeof(wal_scn));
	abort.xact_time = INT64_C(123456);
	xinfo.xinfo
		= XACT_XINFO_HAS_DBINFO | XACT_XINFO_HAS_TWOPHASE | XACT_XINFO_HAS_GID | XACT_XINFO_HAS_SCN;
	dbinfo.dbId = database;
	twophase.xid = xid;
	wal_scn.scn = scn;
	cursor = (char *)fake->data;
	memcpy(cursor, &abort, sizeof(abort));
	cursor += sizeof(abort);
	memcpy(cursor, &xinfo, sizeof(xinfo));
	cursor += sizeof(xinfo);
	memcpy(cursor, &dbinfo, sizeof(dbinfo));
	cursor += sizeof(dbinfo);
	memcpy(cursor, &twophase, sizeof(twophase));
	cursor += sizeof(twophase);
	memcpy(cursor, "gid", 4);
	cursor += 4;
	memcpy(cursor, &wal_scn, sizeof(wal_scn));
	cursor += sizeof(wal_scn);
	fake->u.decoded.header.xl_rmid = RM_XACT_ID;
	fake->u.decoded.header.xl_info = XLOG_XACT_ABORT_PREPARED | XLOG_XACT_HAS_INFO;
	fake->u.decoded.main_data = (char *)fake->data;
	fake->u.decoded.main_data_len = (uint32)(cursor - (char *)fake->data);
	fake->reader.record = &fake->u.decoded;
	return &fake->reader;
}

static RfPageOnlineRecordIdentityV1
make_identity(FakeXactRecord *fake, uint8 storage_uuid[16])
{
	RfPageOnlineRecordIdentityV1 identity;

	memset(&identity, 0, sizeof(identity));
	identity.record.system_identifier = UINT64_C(0x11223344);
	memcpy(identity.record.storage_uuid, storage_uuid, 16);
	identity.record.origin_thread = 3;
	identity.record.timeline_id = 7;
	identity.record.read_rec_ptr = UINT64_C(100);
	identity.record.end_rec_ptr = UINT64_C(200);
	identity.record.record_crc = UINT32_C(0xabc123);
	identity.record.rmid = fake->u.decoded.header.xl_rmid;
	identity.record.info = fake->u.decoded.header.xl_info;
	fake->reader.system_identifier = identity.record.system_identifier;
	fake->reader.ReadRecPtr = identity.record.read_rec_ptr;
	fake->reader.EndRecPtr = identity.record.end_rec_ptr;
	fake->u.decoded.lsn = identity.record.read_rec_ptr;
	fake->u.decoded.next_lsn = identity.record.end_rec_ptr;
	fake->u.decoded.header.xl_crc = identity.record.record_crc;
	return identity;
}

static void
set_identity_range(FakeXactRecord *fake, RfPageOnlineRecordIdentityV1 *identity, XLogRecPtr begin,
				   XLogRecPtr end)
{
	identity->record.read_rec_ptr = begin;
	identity->record.end_rec_ptr = end;
	fake->reader.ReadRecPtr = begin;
	fake->reader.EndRecPtr = end;
	fake->u.decoded.lsn = begin;
	fake->u.decoded.next_lsn = end;
}

static XLogReaderState *
make_undo_delta(FakeXactRecord *fake)
{
	xl_undo_block_write undo;
	uint32 body_len = UNDO_BLOCK_HDR_PREFIX_LEN + 24 + sizeof(UndoSlotDirEntry);

	memset(fake, 0, sizeof(*fake));
	memset(&undo, 0, sizeof(undo));
	undo.instance = 3;
	undo.segment_id = 513;
	undo.block_no = 9;
	undo.rec_off = sizeof(UndoBlockHeader);
	undo.rec_len = 24;
	undo.slot_off = BLCKSZ - sizeof(UndoSlotDirEntry);
	memcpy(fake->data, &undo, sizeof(undo));
	memset(fake->data + sizeof(undo), 0x6b, body_len);
	fake->u.decoded.header.xl_rmid = RM_CLUSTER_UNDO_ID;
	fake->u.decoded.header.xl_info = XLOG_UNDO_BLOCK_WRITE;
	fake->u.decoded.main_data = (char *)fake->data;
	fake->u.decoded.main_data_len = sizeof(undo) + body_len;
	fake->reader.record = &fake->u.decoded;
	return &fake->reader;
}

static XLogReaderState *
make_tt_commit_delta(FakeXactRecord *fake, TransactionId xid, SCN commit_scn)
{
	xl_undo_tt_slot_commit commit;

	memset(fake, 0, sizeof(*fake));
	memset(&commit, 0, sizeof(commit));
	commit.instance = 3;
	commit.segment_id = 513;
	commit.slot_offset = 4;
	commit.wrap = 7;
	commit.xid = xid;
	commit.commit_scn = commit_scn;
	memcpy(fake->data, &commit, sizeof(commit));
	fake->u.decoded.header.xl_rmid = RM_CLUSTER_UNDO_ID;
	fake->u.decoded.header.xl_info = XLOG_UNDO_TT_SLOT_COMMIT;
	fake->u.decoded.main_data = (char *)fake->data;
	fake->u.decoded.main_data_len = sizeof(commit);
	fake->reader.record = &fake->u.decoded;
	return &fake->reader;
}

static XLogReaderState *
make_tt_abort_delta(FakeXactRecord *fake, TransactionId xid)
{
	xl_undo_tt_slot_abort abort;

	memset(fake, 0, sizeof(*fake));
	memset(&abort, 0, sizeof(abort));
	abort.instance = 3;
	abort.segment_id = 513;
	abort.slot_offset = 4;
	abort.wrap = 7;
	abort.xid = xid;
	memcpy(fake->data, &abort, sizeof(abort));
	fake->u.decoded.header.xl_rmid = RM_CLUSTER_UNDO_ID;
	fake->u.decoded.header.xl_info = XLOG_UNDO_TT_SLOT_ABORT;
	fake->u.decoded.main_data = (char *)fake->data;
	fake->u.decoded.main_data_len = sizeof(abort);
	fake->reader.record = &fake->u.decoded;
	return &fake->reader;
}

static XLogReaderState *
make_tt_set_head_delta(FakeXactRecord *fake, TransactionId xid, const UBA *head)
{
	xl_undo_tt_slot_set_head set_head;

	memset(fake, 0, sizeof(*fake));
	memset(&set_head, 0, sizeof(set_head));
	set_head.instance = 3;
	set_head.segment_id = 513;
	set_head.slot_offset = 4;
	set_head.wrap = 7;
	set_head.xid = xid;
	set_head.first_undo_block = *head;
	memcpy(fake->data, &set_head, sizeof(set_head));
	fake->u.decoded.header.xl_rmid = RM_CLUSTER_UNDO_ID;
	fake->u.decoded.header.xl_info = XLOG_UNDO_TT_SLOT_SET_HEAD;
	fake->u.decoded.main_data = (char *)fake->data;
	fake->u.decoded.main_data_len = sizeof(set_head);
	fake->reader.record = &fake->u.decoded;
	return &fake->reader;
}

static RfDetachedRecordPlanV1
make_undo_record_plan(FakeXactRecord *fake)
{
	RfDetachedRecordPlanV1 record_plan;

	memset(&record_plan, 0, sizeof(record_plan));
	record_plan.source_record = &fake->reader;
	record_plan.route.rmid = RM_CLUSTER_UNDO_ID;
	record_plan.route.normalized_info = fake->u.decoded.header.xl_info & XLR_RMGR_INFO_MASK;
	record_plan.route.record_owner = RF_ROUTE_OWNER_SIDE_TYPED;
	record_plan.route.block_policy = RF_ROUTE_BLOCKS_FORBIDDEN;
	record_plan.route.codec_id = RF_ROUTE_CODEC_SIDE_CLUSTER_UNDO;
	record_plan.preflight_complete = true;
	return record_plan;
}

static RfDetachedRecordPlanV1
make_record_plan(FakeXactRecord *fake)
{
	RfDetachedRecordPlanV1 record_plan;

	memset(&record_plan, 0, sizeof(record_plan));
	record_plan.source_record = &fake->reader;
	record_plan.route.rmid = RM_XACT_ID;
	record_plan.route.normalized_info = fake->u.decoded.header.xl_info & XLOG_XACT_OPMASK;
	record_plan.route.legal_info_flags = XLOG_XACT_HAS_INFO;
	record_plan.route.record_owner = RF_ROUTE_OWNER_SIDE_TYPED;
	record_plan.route.block_policy = RF_ROUTE_BLOCKS_FORBIDDEN;
	record_plan.route.codec_id = RF_ROUTE_CODEC_SIDE_STANDARD;
	record_plan.preflight_complete = true;
	return record_plan;
}

static XLogReaderState *
make_projection_record(FakeXactRecord *fake, RmgrId rmid, uint8 info, const void *payload,
					   uint32 payload_length)
{
	memset(fake, 0, sizeof(*fake));
	UT_ASSERT(payload_length <= sizeof(fake->data));
	memcpy(fake->data, payload, payload_length);
	fake->u.decoded.header.xl_rmid = rmid;
	fake->u.decoded.header.xl_info = info;
	fake->u.decoded.main_data = (char *)fake->data;
	fake->u.decoded.main_data_len = payload_length;
	fake->reader.record = &fake->u.decoded;
	return &fake->reader;
}

static RfDetachedRecordPlanV1
make_projection_record_plan(FakeXactRecord *fake)
{
	RfDetachedRecordPlanV1 record_plan;

	memset(&record_plan, 0, sizeof(record_plan));
	record_plan.source_record = &fake->reader;
	record_plan.route.rmid = fake->u.decoded.header.xl_rmid;
	record_plan.route.normalized_info = fake->u.decoded.header.xl_info & ~XLR_INFO_MASK;
	record_plan.route.record_owner = RF_ROUTE_OWNER_SIDE_TYPED;
	record_plan.route.block_policy = RF_ROUTE_BLOCKS_FORBIDDEN;
	record_plan.route.codec_id = RF_ROUTE_CODEC_SIDE_STANDARD;
	record_plan.preflight_complete = true;
	return record_plan;
}

typedef struct ApplyCapture {
	uint32 count;
	uint32 undo_count;
	uint32 projection_count;
	uint32 begin_count;
	uint32 end_count;
	TransactionId xid;
	SCN scn;
	uint8 undo_first_byte;
	uint8 projection_first_byte;
	bool end_complete;
	uint16 projection_origin;
} ApplyCapture;

static bool
capture_begin(void *arg)
{
	ApplyCapture *capture = (ApplyCapture *)arg;

	capture->begin_count++;
	return true;
}

static void
capture_end(void *arg, bool complete)
{
	ApplyCapture *capture = (ApplyCapture *)arg;

	capture->end_count++;
	capture->end_complete = complete;
}

static bool
capture_apply(void *arg, const RfSideOnlineOperationV1 *operation)
{
	ApplyCapture *capture = (ApplyCapture *)arg;

	capture->count++;
	capture->xid = operation->xact.xid;
	capture->scn = operation->xact.terminal_scn;
	return true;
}

static bool
capture_apply_undo(void *arg, const RfSideOnlineOperationV1 *operation)
{
	ApplyCapture *capture = (ApplyCapture *)arg;

	capture->undo_count++;
	if (operation->owned_payload_length > 0)
		capture->undo_first_byte = operation->owned_payload[0];
	return operation->kind == RF_SIDE_ONLINE_OPERATION_UNDO;
}

static bool
capture_apply_projection(void *arg, const RfSideOnlineOperationV1 *operation)
{
	ApplyCapture *capture = (ApplyCapture *)arg;

	capture->projection_count++;
	capture->projection_origin = operation->identity.record.origin_thread;
	if (operation->owned_payload_length > 0)
		capture->projection_first_byte = operation->owned_payload[0];
	return operation->kind == RF_SIDE_ONLINE_OPERATION_PROJECTION;
}

static bool
accept_preflight(void *arg pg_attribute_unused(),
				 const RfSideOnlineOperationV1 *operation pg_attribute_unused())
{
	return true;
}

static bool
reject_undo_preflight(void *arg pg_attribute_unused(), const RfSideOnlineOperationV1 *operation)
{
	return operation->kind != RF_SIDE_ONLINE_OPERATION_UNDO;
}

UT_TEST(test_commit_decodes_to_immutable_truth_operation)
{
	FakeXactRecord fake;
	RfSideXactOperationV1 operation;

	UT_ASSERT(rf_side_xact_decode_v1(make_commit(&fake, 800, UINT64_C(901), INT64_C(123456), false),
									 UINT64_C(0x11223344), 3, &operation));
	UT_ASSERT_EQ(operation.kind, RF_SIDE_XACT_COMMIT);
	UT_ASSERT_EQ(operation.system_identifier, UINT64_C(0x11223344));
	UT_ASSERT_EQ(operation.origin_thread, 3);
	UT_ASSERT_EQ(operation.xid, 800);
	UT_ASSERT_EQ(operation.terminal_scn, UINT64_C(901));
	UT_ASSERT_EQ(operation.terminal_timestamp, INT64_C(123456));
	UT_ASSERT(operation.has_tt_delta);
	UT_ASSERT_EQ(operation.tt_delta.xid, 800);
	UT_ASSERT_EQ(operation.tt_delta.segment_generation, 11);
	UT_ASSERT_EQ(operation.tt_delta.commit_scn, UINT64_C(901));
	UT_ASSERT(rf_side_xact_structural_preflight_v1(&operation));
}

UT_TEST(test_commit_tt_conflict_is_blocked_before_apply)
{
	FakeXactRecord fake;
	RfSideXactOperationV1 operation;

	UT_ASSERT(!rf_side_xact_decode_v1(make_commit(&fake, 800, UINT64_C(901), INT64_C(123456), true),
									  UINT64_C(0x11223344), 3, &operation));
	UT_ASSERT_EQ(operation.kind, RF_SIDE_XACT_INVALID);
}

static bool
verify_test_commit_coverage(void *arg, const RfSideXactOperationV1 *operation)
{
	UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
	UT_ASSERT_EQ(operation->tt_delta.segment_id, 513);
	return *(bool *)arg;
}

UT_TEST(test_covered_commit_requires_owner_proof_before_projection_without_tt_rewind)
{
	FakeXactRecord fake;
	RfSideXactOperationV1 operation;
	bool covered;

	for (int variant = 0; variant < 3; variant++) {
		reset_prepare_apply();
		UT_ASSERT(rf_side_xact_decode_v1(make_commit(&fake, 802, 901, 123456, false),
										 UINT64_C(0x11223344), 3, &operation));
		covered = variant != 0;
		if (variant == 2)
			operation.system_identifier++;
		UT_ASSERT_EQ(
			rf_side_xact_apply_covered_commit_v1(&operation, &covered, verify_test_commit_coverage),
			variant == 1 ? RF_SIDE_XACT_APPLY_OK : RF_SIDE_XACT_APPLY_BLOCKED);
		UT_ASSERT_EQ(prepare_apply.tt_stamps, 0);
		UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, variant == 1 ? 1 : 0);
	}
}

UT_TEST(test_non_xact_and_missing_tt_are_blocked)
{
	FakeXactRecord fake;
	RfSideXactOperationV1 operation;
	XLogReaderState *record = make_commit(&fake, 800, UINT64_C(901), INT64_C(123456), false);

	fake.u.decoded.header.xl_rmid = RM_HEAP_ID;
	UT_ASSERT(!rf_side_xact_decode_v1(record, UINT64_C(0x11223344), 3, &operation));
	record = make_commit(&fake, 800, UINT64_C(901), INT64_C(123456), false);
	fake.u.decoded.main_data_len -= sizeof(xl_xact_tt_commit);
	UT_ASSERT(!rf_side_xact_decode_v1(record, UINT64_C(0x11223344), 3, &operation));
	UT_ASSERT(!rf_side_xact_structural_preflight_v1(NULL));
}

UT_TEST(test_prepare_requires_bounded_aligned_gid)
{
	FakeXactRecord fake;
	RfSideXactOperationV1 operation;

	UT_ASSERT(rf_side_xact_decode_v1(make_prepare(&fake, 801, true, false), UINT64_C(0x11223344), 3,
									 &operation));
	UT_ASSERT_EQ(operation.kind, RF_SIDE_XACT_PREPARE);
	UT_ASSERT_EQ(operation.xid, 801);
	UT_ASSERT_EQ(operation.database, 16384);
	UT_ASSERT_EQ(operation.prepared_owner, 10);
	UT_ASSERT_EQ(operation.prepared_at, INT64_C(123456));
	UT_ASSERT(strcmp(operation.prepare_gid, "gid") == 0);
	UT_ASSERT(operation.prepare_payload_length > 0);
	UT_ASSERT_EQ(operation.prepared_binding_count, 1);
	UT_ASSERT_EQ(operation.prepared_bindings[0].undo_segment_id, 513);
	UT_ASSERT_EQ(operation.prepared_bindings[0].slot_offset, 4);
	UT_ASSERT_EQ(operation.prepared_bindings[0].wrap, 7);
	UT_ASSERT_EQ(operation.prepared_bindings[0].xid, 801);
	UT_ASSERT(!rf_side_xact_decode_v1(make_prepare(&fake, 801, false, false), UINT64_C(0x11223344),
									  3, &operation));
	UT_ASSERT(!rf_side_xact_decode_v1(make_prepare(&fake, 801, true, true), UINT64_C(0x11223344), 3,
									  &operation));
	UT_ASSERT_EQ(operation.kind, RF_SIDE_XACT_INVALID);
}

UT_TEST(test_prepare_apply_installs_authoritative_pending_before_projection)
{
	FakeXactRecord fake;
	RfSideXactOperationV1 operation;
	XLogReaderState *record;

	reset_prepare_apply();
	record = make_prepare(&fake, 802, true, false);
	UT_ASSERT(rf_side_xact_decode_v1(record, UINT64_C(0x11223344), 3, &operation));
	UT_ASSERT_EQ(rf_side_xact_target_preflight_owned_v1(&operation, fake.data,
														operation.prepare_payload_length),
				 RF_SIDE_XACT_APPLY_OK);
	UT_ASSERT_EQ(prepare_apply.tt_reads, 1);
	UT_ASSERT_EQ(prepare_apply.pending_preflights, 1);
	UT_ASSERT_EQ(prepare_apply.pending_installs, 0);
	UT_ASSERT_EQ(prepare_apply.projection_stores, 0);

	UT_ASSERT_EQ(
		rf_side_xact_apply_owned_v1(&operation, fake.data, operation.prepare_payload_length),
		RF_SIDE_XACT_APPLY_OK);
	UT_ASSERT_EQ(prepare_apply.pending_installs, 1);
	UT_ASSERT_EQ(prepare_apply.projection_stores, 1);
	UT_ASSERT_EQ(prepare_apply.projection_postreads, 1);
	UT_ASSERT(prepare_apply.install_order < prepare_apply.projection_order);
}

UT_TEST(test_prepare_apply_blocks_underivable_or_wrong_system_before_pending)
{
	FakeXactRecord fake;
	RfSideXactOperationV1 operation;

	reset_prepare_apply();
	UT_ASSERT(rf_side_xact_decode_v1(make_prepare(&fake, 802, true, false), UINT64_C(0x11223344), 3,
									 &operation));
	prepare_apply.origin_slot = -1;
	UT_ASSERT_EQ(rf_side_xact_target_preflight_owned_v1(&operation, fake.data,
														operation.prepare_payload_length),
				 RF_SIDE_XACT_APPLY_BLOCKED);
	UT_ASSERT_EQ(prepare_apply.tt_reads, 0);
	UT_ASSERT_EQ(prepare_apply.pending_preflights, 0);

	prepare_apply.origin_slot = 2;
	prepare_apply.system_identifier++;
	UT_ASSERT_EQ(
		rf_side_xact_apply_owned_v1(&operation, fake.data, operation.prepare_payload_length),
		RF_SIDE_XACT_APPLY_BLOCKED);
	UT_ASSERT_EQ(prepare_apply.pending_installs, 0);
	UT_ASSERT_EQ(prepare_apply.projection_stores, 0);
}

UT_TEST(test_prepare_apply_blocks_wrong_origin_subxid_before_target_reads)
{
	FakeXactRecord fake;
	RfSideXactOperationV1 operation;
	ClusterTT2PCBinding *child;
	ClusterTT2PCSubLink *link;

	reset_prepare_apply();
	UT_ASSERT(rf_side_xact_decode_v1(make_prepare(&fake, 802, true, false), UINT64_C(0x11223344), 3,
									 &operation));
	child = &operation.prepared_bindings[1];
	memset(child, 0, sizeof(*child));
	child->undo_segment_id = 514;
	child->slot_offset = 5;
	child->wrap = 7;
	child->cluster_epoch = 11;
	child->xid = 803;
	operation.prepared_binding_count = 2;
	link = &operation.prepared_sublinks[0];
	memset(link, 0, sizeof(*link));
	link->child_key.origin_node_id = 2;
	link->child_key.undo_segment_id = 514;
	link->child_key.tt_slot_id = cluster_tt_slot_offset_to_id(5);
	link->child_key.cluster_epoch = 11;
	link->child_key.local_xid = 803;
	link->parent_key.origin_node_id = 2;
	link->parent_key.undo_segment_id = 513;
	link->parent_key.tt_slot_id = cluster_tt_slot_offset_to_id(4);
	link->parent_key.cluster_epoch = 11;
	link->parent_key.local_xid = 802;
	operation.prepared_sublink_count = 1;
	UT_ASSERT(rf_side_xact_structural_preflight_v1(&operation));
	prepare_apply.mismatched_origin_xid = 803;

	UT_ASSERT_EQ(rf_side_xact_target_preflight_owned_v1(&operation, fake.data,
														operation.prepare_payload_length),
				 RF_SIDE_XACT_APPLY_BLOCKED);
	UT_ASSERT_EQ(prepare_apply.tt_reads, 0);
	UT_ASSERT_EQ(prepare_apply.pending_preflights, 0);
	UT_ASSERT_EQ(prepare_apply.pending_installs, 0);
	UT_ASSERT_EQ(prepare_apply.projection_stores, 0);
}

UT_TEST(test_prepare_apply_never_projects_unverified_pending)
{
	FakeXactRecord fake;
	RfSideXactOperationV1 operation;

	reset_prepare_apply();
	UT_ASSERT(rf_side_xact_decode_v1(make_prepare(&fake, 802, true, false), UINT64_C(0x11223344), 3,
									 &operation));
	prepare_apply.pending_install = TWOPHASE_RECOVERY_PENDING_POST_READ_FAILED;
	UT_ASSERT_EQ(
		rf_side_xact_apply_owned_v1(&operation, fake.data, operation.prepare_payload_length),
		RF_SIDE_XACT_APPLY_POST_READ_FAILED);
	UT_ASSERT_EQ(prepare_apply.pending_installs, 1);
	UT_ASSERT_EQ(prepare_apply.projection_stores, 0);

	reset_prepare_apply();
	prepare_apply.projection_store = CLUSTER_REMOTE_XACT_MUTATION_CONFLICT;
	UT_ASSERT_EQ(
		rf_side_xact_apply_owned_v1(&operation, fake.data, operation.prepare_payload_length),
		RF_SIDE_XACT_APPLY_CONFLICT);
	UT_ASSERT_EQ(prepare_apply.pending_installs, 1);
	UT_ASSERT_EQ(prepare_apply.projection_stores, 1);
	UT_ASSERT_EQ(prepare_apply.projection_postreads, 0);
}

UT_TEST(test_commit_prepared_resolves_exact_native_owner_after_tt_truth)
{
	FakeXactRecord prepare_fake;
	FakeXactRecord terminal_fake;
	RfSideXactOperationV1 operation;
	XLogReaderState *prepare_record;
	UBA head = InvalidUba_init;

	reset_prepare_apply();
	head.raw[0] = UINT64_C(513) | (UINT64_C(9) << 32);
	head.raw[1] = UINT64_C(4);
	prepare_record = make_prepare_with_head(&prepare_fake, 802, true, false, &head);
	prepare_apply.native_payload_length = XLogRecGetDataLen(prepare_record);
	memcpy(prepare_apply.native_payload, XLogRecGetData(prepare_record),
		   prepare_apply.native_payload_length);
	prepare_apply.slot.status = TT_SLOT_COMMITTED;
	prepare_apply.slot.commit_scn = UINT64_C(901);
	prepare_apply.slot.first_undo_block = (UBA)InvalidUba_init;
	UT_ASSERT(rf_side_xact_decode_v1(
		make_commit_prepared(&terminal_fake, 802, 16384, UINT64_C(901), INT64_C(123456)),
		UINT64_C(0x11223344), 3, &operation));
	UT_ASSERT_EQ(operation.kind, RF_SIDE_XACT_COMMIT_PREPARED);
	UT_ASSERT_EQ(rf_side_xact_target_preflight_owned_v1(&operation, NULL, 0),
				 RF_SIDE_XACT_APPLY_OK);
	UT_ASSERT_EQ(rf_side_xact_apply_owned_v1(&operation, NULL, 0), RF_SIDE_XACT_APPLY_OK);
	UT_ASSERT(prepare_apply.pending_reads > 0);
	UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 1);
	UT_ASSERT_EQ(prepare_apply.pending_resolves, 1);
	UT_ASSERT(prepare_apply.terminal_projection_order < prepare_apply.resolve_order);
}

UT_TEST(test_abort_prepared_resolves_exact_native_owner_after_tt_truth)
{
	FakeXactRecord prepare_fake;
	FakeXactRecord terminal_fake;
	RfSideXactOperationV1 operation;
	XLogReaderState *prepare_record;

	reset_prepare_apply();
	prepare_record = make_prepare(&prepare_fake, 802, true, false);
	prepare_apply.native_payload_length = XLogRecGetDataLen(prepare_record);
	memcpy(prepare_apply.native_payload, XLogRecGetData(prepare_record),
		   prepare_apply.native_payload_length);
	prepare_apply.slot.status = TT_SLOT_ABORTED;
	prepare_apply.slot.commit_scn = InvalidScn;
	prepare_apply.slot.first_undo_block = (UBA)InvalidUba_init;
	UT_ASSERT(rf_side_xact_decode_v1(make_abort_prepared(&terminal_fake, 802, 16384, UINT64_C(902)),
									 UINT64_C(0x11223344), 3, &operation));
	UT_ASSERT_EQ(operation.kind, RF_SIDE_XACT_ABORT_PREPARED);
	UT_ASSERT_EQ(rf_side_xact_target_preflight_owned_v1(&operation, NULL, 0),
				 RF_SIDE_XACT_APPLY_OK);
	UT_ASSERT_EQ(rf_side_xact_apply_owned_v1(&operation, NULL, 0), RF_SIDE_XACT_APPLY_OK);
	UT_ASSERT(prepare_apply.pending_reads > 0);
	UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 1);
	UT_ASSERT_EQ(prepare_apply.terminal_outcome, CLUSTER_REMOTE_XACT_ABORTED);
	UT_ASSERT_EQ(prepare_apply.pending_resolves, 1);
	UT_ASSERT(!prepare_apply.resolved_is_commit);
	UT_ASSERT(prepare_apply.terminal_projection_order < prepare_apply.resolve_order);
}

UT_TEST(test_abort_prepared_nonempty_undo_retains_native_owner)
{
	FakeXactRecord prepare_fake;
	FakeXactRecord terminal_fake;
	RfSideXactOperationV1 operation;
	XLogReaderState *prepare_record;
	UBA head = InvalidUba_init;

	head.raw[0] = UINT64_C(513) | (UINT64_C(9) << 32);
	head.raw[1] = UINT64_C(4);
	reset_prepare_apply();
	prepare_record = make_prepare_with_head(&prepare_fake, 802, true, false, &head);
	prepare_apply.native_payload_length = XLogRecGetDataLen(prepare_record);
	memcpy(prepare_apply.native_payload, XLogRecGetData(prepare_record),
		   prepare_apply.native_payload_length);
	prepare_apply.slot.status = TT_SLOT_ABORTED;
	prepare_apply.slot.commit_scn = InvalidScn;
	prepare_apply.slot.first_undo_block = (UBA)InvalidUba_init;
	UT_ASSERT(rf_side_xact_decode_v1(make_abort_prepared(&terminal_fake, 802, 16384, UINT64_C(902)),
									 UINT64_C(0x11223344), 3, &operation));
	UT_ASSERT_EQ(rf_side_xact_target_preflight_owned_v1(&operation, NULL, 0),
				 RF_SIDE_XACT_APPLY_BLOCKED);
	UT_ASSERT_EQ(rf_side_xact_apply_owned_v1(&operation, NULL, 0), RF_SIDE_XACT_APPLY_BLOCKED);
	UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
	UT_ASSERT_EQ(prepare_apply.pending_resolves, 0);
}

static void
assert_contribution_owners(const RfSideOnlinePlanV1 *plan, uint32 index, uint32 owners,
						   uint32 space_count)
{
	RfSideContributionOwnersV1 result = { 0 };
	UT_ASSERT(rf_side_online_plan_contribution_owners_v1(plan, index, &result));
	UT_ASSERT_EQ(result.owners, owners);
	UT_ASSERT_EQ(result.space_locator_count, space_count);
}

UT_TEST(test_online_plan_owns_decoded_operation_not_raw_record)
{
	FakeXactRecord fake;
	RfDetachedRecordPlanV1 record_plan;
	RfPageOnlineRecordIdentityV1 identity;
	RfSideOnlinePlanRequestV1 request;
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut;
	RfSideOnlineOperationV1 operation;
	RfSideOnlineApplyOpsV1 apply_ops;
	ApplyCapture capture;
	uint8 storage_uuid[16];

	memset(storage_uuid, 0x42, sizeof(storage_uuid));
	(void)make_commit(&fake, 800, UINT64_C(901), INT64_C(123456), false);
	identity = make_identity(&fake, storage_uuid);
	record_plan = make_record_plan(&fake);
	memset(&cut, 0, sizeof(cut));
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 200;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	memset(&request, 0, sizeof(request));
	request.system_identifier = identity.record.system_identifier;
	memcpy(request.storage_uuid, storage_uuid, sizeof(storage_uuid));
	request.physical_cuts = &cut;
	request.participant_count = 1;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record_plan, &identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	memset(fake.data, 0xee, sizeof(fake.data));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 1);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
	UT_ASSERT_EQ(operation.xact.xid, 800);
	UT_ASSERT_EQ(operation.xact.terminal_scn, UINT64_C(901));
	assert_contribution_owners(plan, 0,
							   RF_SIDE_CONTRIBUTION_UNDO_HEADER | RF_SIDE_CONTRIBUTION_TERMINAL, 0);
	memset(&capture, 0, sizeof(capture));
	memset(&apply_ops, 0, sizeof(apply_ops));
	apply_ops.arg = &capture;
	apply_ops.begin_protected_set = capture_begin;
	apply_ops.end_protected_set = capture_end;
	apply_ops.preflight_xact = accept_preflight;
	apply_ops.apply_xact = capture_apply;
	UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &apply_ops), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(capture.count, 1);
	UT_ASSERT_EQ(capture.xid, 800);
	UT_ASSERT_EQ(capture.scn, UINT64_C(901));
	UT_ASSERT_EQ(capture.begin_count, 1);
	UT_ASSERT_EQ(capture.end_count, 1);
	UT_ASSERT(capture.end_complete);
	rf_side_online_plan_destroy_v1(&plan);
	UT_ASSERT(plan == NULL);
}

UT_TEST(test_online_plan_owns_typed_projection_records)
{
	FakeXactRecord clog_fake;
	FakeXactRecord multi_fake;
	FakeXactRecord commit_ts_fake;
	RfDetachedRecordPlanV1 clog_plan;
	RfDetachedRecordPlanV1 multi_plan;
	RfDetachedRecordPlanV1 commit_ts_plan;
	RfPageOnlineRecordIdentityV1 clog_identity;
	RfPageOnlineRecordIdentityV1 multi_identity;
	RfPageOnlineRecordIdentityV1 commit_ts_identity;
	RfSideOnlinePlanRequestV1 request;
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut;
	RfSideOnlineOperationV1 operation;
	RfSideOnlineApplyOpsV1 apply_ops;
	ApplyCapture capture;
	xl_multixact_create *create;
	xl_commit_ts_truncate commit_ts;
	uint8 create_payload[SizeOfMultiXactCreate + 2 * sizeof(MultiXactMember)];
	uint8 storage_uuid[16];
	int clog_page = 17;

	memset(storage_uuid, 0x45, sizeof(storage_uuid));
	(void)make_projection_record(&clog_fake, RM_CLOG_ID, CLOG_ZEROPAGE, &clog_page,
								 sizeof(clog_page));
	memset(create_payload, 0, sizeof(create_payload));
	create = (xl_multixact_create *)create_payload;
	create->mid = 33;
	create->moff = 71;
	create->nmembers = 2;
	create->members[0].xid = 800;
	create->members[0].status = MultiXactStatusForKeyShare;
	create->members[1].xid = 816;
	create->members[1].status = MultiXactStatusUpdate;
	(void)make_projection_record(&multi_fake, RM_MULTIXACT_ID, XLOG_MULTIXACT_CREATE_ID,
								 create_payload, sizeof(create_payload));
	memset(&commit_ts, 0, sizeof(commit_ts));
	commit_ts.pageno = 19;
	commit_ts.oldestXid = 800;
	(void)make_projection_record(&commit_ts_fake, RM_COMMIT_TS_ID, COMMIT_TS_TRUNCATE, &commit_ts,
								 SizeOfCommitTsTruncate);

	clog_identity = make_identity(&clog_fake, storage_uuid);
	multi_identity = make_identity(&multi_fake, storage_uuid);
	commit_ts_identity = make_identity(&commit_ts_fake, storage_uuid);
	set_identity_range(&multi_fake, &multi_identity, 200, 300);
	set_identity_range(&commit_ts_fake, &commit_ts_identity, 300, 400);
	clog_plan = make_projection_record_plan(&clog_fake);
	multi_plan = make_projection_record_plan(&multi_fake);
	commit_ts_plan = make_projection_record_plan(&commit_ts_fake);
	memset(&cut, 0, sizeof(cut));
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 400;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	memset(&request, 0, sizeof(request));
	request.system_identifier = clog_identity.record.system_identifier;
	memcpy(request.storage_uuid, storage_uuid, sizeof(storage_uuid));
	request.physical_cuts = &cut;
	request.participant_count = 1;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &clog_plan, &clog_identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &multi_plan, &multi_identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &commit_ts_plan, &commit_ts_identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	memset(create_payload, 0xee, sizeof(create_payload));
	memset(multi_fake.data, 0xee, sizeof(multi_fake.data));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 3);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
	UT_ASSERT_EQ(operation.kind, RF_SIDE_ONLINE_OPERATION_PROJECTION);
	UT_ASSERT_EQ(operation.projection.kind, CLUSTER_SIDE_PROJECTION_CLOG);
	UT_ASSERT_EQ(operation.projection.action, CLUSTER_SIDE_PROJECTION_ACTION_ZERO_PAGE);
	UT_ASSERT_EQ(operation.projection.page_number, 17);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
	UT_ASSERT_EQ(operation.projection.kind, CLUSTER_SIDE_PROJECTION_MULTIXACT);
	UT_ASSERT_EQ(operation.projection.action, CLUSTER_SIDE_PROJECTION_ACTION_CREATE);
	UT_ASSERT_EQ(operation.projection.multixact_id, 33);
	UT_ASSERT_EQ(operation.projection.member_offset, 71);
	UT_ASSERT_EQ(operation.projection.member_count, 2);
	UT_ASSERT_EQ(operation.owned_payload_length, 2 * sizeof(MultiXactMember));
	UT_ASSERT_EQ(((const MultiXactMember *)operation.owned_payload)[1].xid, 816);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 2, &operation));
	UT_ASSERT_EQ(operation.projection.kind, CLUSTER_SIDE_PROJECTION_COMMIT_TS);
	UT_ASSERT_EQ(operation.projection.action, CLUSTER_SIDE_PROJECTION_ACTION_TRUNCATE);
	UT_ASSERT_EQ(operation.projection.page_number, 19);
	UT_ASSERT_EQ(operation.projection.oldest_xid, 800);
	assert_contribution_owners(plan, 0, RF_SIDE_CONTRIBUTION_CLOG, 0);
	assert_contribution_owners(plan, 1, RF_SIDE_CONTRIBUTION_MULTIXACT, 0);
	assert_contribution_owners(plan, 2, RF_SIDE_CONTRIBUTION_COMMIT_TS, 0);

	memset(&capture, 0, sizeof(capture));
	memset(&apply_ops, 0, sizeof(apply_ops));
	apply_ops.arg = &capture;
	apply_ops.begin_protected_set = capture_begin;
	apply_ops.end_protected_set = capture_end;
	apply_ops.preflight_projection = accept_preflight;
	apply_ops.apply_projection = capture_apply_projection;
	UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &apply_ops), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(capture.projection_count, 3);
	UT_ASSERT_EQ(capture.projection_first_byte, 0x20);
	UT_ASSERT(capture.end_complete);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_online_plan_rejects_malformed_multixact_projection)
{
	FakeXactRecord fake;
	RfDetachedRecordPlanV1 record_plan;
	RfPageOnlineRecordIdentityV1 identity;
	RfSideOnlinePlanRequestV1 request;
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut;
	xl_multixact_create create;
	uint8 storage_uuid[16];

	memset(storage_uuid, 0x46, sizeof(storage_uuid));
	memset(&create, 0, sizeof(create));
	create.mid = 33;
	create.moff = 71;
	create.nmembers = 2;
	(void)make_projection_record(&fake, RM_MULTIXACT_ID, XLOG_MULTIXACT_CREATE_ID, &create,
								 SizeOfMultiXactCreate);
	identity = make_identity(&fake, storage_uuid);
	record_plan = make_projection_record_plan(&fake);
	memset(&cut, 0, sizeof(cut));
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 200;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	memset(&request, 0, sizeof(request));
	request.system_identifier = identity.record.system_identifier;
	memcpy(request.storage_uuid, storage_uuid, sizeof(storage_uuid));
	request.physical_cuts = &cut;
	request.participant_count = 1;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record_plan, &identity),
				 RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
	UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 0);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_online_plan_denies_incomplete_physical_cut)
{
	RfSideOnlinePlanRequestV1 request;
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut;
	uint8 storage_uuid[16];

	memset(storage_uuid, 0x41, sizeof(storage_uuid));
	memset(&cut, 0, sizeof(cut));
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 200;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	memset(&request, 0, sizeof(request));
	request.system_identifier = UINT64_C(0x11223344);
	memcpy(request.storage_uuid, storage_uuid, sizeof(storage_uuid));
	request.physical_cuts = &cut;
	request.participant_count = 1;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
	UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 0);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_online_plan_owns_undo_payload_not_raw_record)
{
	FakeXactRecord fake;
	RfDetachedRecordPlanV1 record_plan;
	RfPageOnlineRecordIdentityV1 identity;
	RfSideOnlinePlanRequestV1 request;
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut;
	RfSideOnlineOperationV1 operation;
	RfSideOnlineApplyOpsV1 apply_ops;
	ApplyCapture capture;
	uint8 storage_uuid[16];

	memset(storage_uuid, 0x44, sizeof(storage_uuid));
	(void)make_undo_delta(&fake);
	identity = make_identity(&fake, storage_uuid);
	record_plan = make_undo_record_plan(&fake);
	memset(&cut, 0, sizeof(cut));
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 200;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	memset(&request, 0, sizeof(request));
	request.system_identifier = identity.record.system_identifier;
	memcpy(request.storage_uuid, storage_uuid, sizeof(storage_uuid));
	request.physical_cuts = &cut;
	request.participant_count = 1;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record_plan, &identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	memset(fake.data, 0xee, sizeof(fake.data));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
	UT_ASSERT_EQ(operation.kind, RF_SIDE_ONLINE_OPERATION_UNDO);
	UT_ASSERT_EQ((int)operation.undo.kind, (int)CLUSTER_UNDO_KIND_BLOCK_WRITE);
	UT_ASSERT_EQ(operation.owned_payload_length,
				 UNDO_BLOCK_HDR_PREFIX_LEN + 24 + sizeof(UndoSlotDirEntry));
	UT_ASSERT_EQ(operation.owned_payload[0], 0x6b);
	assert_contribution_owners(plan, 0, RF_SIDE_CONTRIBUTION_UNDO_BLOCK, 0);
	memset(&capture, 0, sizeof(capture));
	memset(&apply_ops, 0, sizeof(apply_ops));
	apply_ops.arg = &capture;
	apply_ops.begin_protected_set = capture_begin;
	apply_ops.end_protected_set = capture_end;
	apply_ops.preflight_undo = accept_preflight;
	apply_ops.apply_xact = capture_apply;
	apply_ops.apply_undo = capture_apply_undo;
	UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &apply_ops), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(capture.undo_count, 1);
	UT_ASSERT_EQ(capture.undo_first_byte, 0x6b);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_online_plan_owns_prepare_state_not_raw_record)
{
	FakeXactRecord fake;
	RfDetachedRecordPlanV1 record_plan;
	RfPageOnlineRecordIdentityV1 identity;
	RfSideOnlinePlanRequestV1 request;
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut;
	RfSideOnlineOperationV1 operation;
	uint8 storage_uuid[16];
	uint32 magic;

	memset(storage_uuid, 0x46, sizeof(storage_uuid));
	(void)make_prepare(&fake, 801, true, false);
	identity = make_identity(&fake, storage_uuid);
	record_plan = make_record_plan(&fake);
	memset(&cut, 0, sizeof(cut));
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 200;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	memset(&request, 0, sizeof(request));
	request.system_identifier = identity.record.system_identifier;
	memcpy(request.storage_uuid, storage_uuid, sizeof(storage_uuid));
	request.physical_cuts = &cut;
	request.participant_count = 1;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record_plan, &identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	memset(fake.data, 0xee, sizeof(fake.data));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
	UT_ASSERT_EQ(operation.kind, RF_SIDE_ONLINE_OPERATION_XACT);
	UT_ASSERT_EQ(operation.xact.kind, RF_SIDE_XACT_PREPARE);
	UT_ASSERT_EQ(operation.xact.prepared_owner, 10);
	UT_ASSERT(strcmp(operation.xact.prepare_gid, "gid") == 0);
	UT_ASSERT_EQ(operation.owned_payload_length, operation.xact.prepare_payload_length);
	UT_ASSERT(operation.owned_payload != NULL);
	magic = 0;
	if (operation.owned_payload != NULL)
		memcpy(&magic, operation.owned_payload, sizeof(magic));
	UT_ASSERT_EQ(magic, UINT32_C(0x57F94534));
	assert_contribution_owners(plan, 0,
							   RF_SIDE_CONTRIBUTION_PREPARED | RF_SIDE_CONTRIBUTION_UNDO_HEADER
								   | RF_SIDE_CONTRIBUTION_TERMINAL,
							   0);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_online_plan_preflights_all_targets_before_first_mutation)
{
	FakeXactRecord xact_fake;
	FakeXactRecord undo_fake;
	RfDetachedRecordPlanV1 xact_plan;
	RfDetachedRecordPlanV1 undo_plan;
	RfPageOnlineRecordIdentityV1 xact_identity;
	RfPageOnlineRecordIdentityV1 undo_identity;
	RfSideOnlinePlanRequestV1 request;
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut;
	RfSideOnlineApplyOpsV1 apply_ops;
	ApplyCapture capture;
	uint8 storage_uuid[16];

	memset(storage_uuid, 0x45, sizeof(storage_uuid));
	(void)make_commit(&xact_fake, 800, UINT64_C(901), INT64_C(123456), false);
	xact_identity = make_identity(&xact_fake, storage_uuid);
	xact_identity.record.end_rec_ptr = 150;
	xact_fake.reader.EndRecPtr = 150;
	xact_fake.u.decoded.next_lsn = 150;
	xact_plan = make_record_plan(&xact_fake);
	(void)make_undo_delta(&undo_fake);
	undo_identity = make_identity(&undo_fake, storage_uuid);
	undo_identity.record.read_rec_ptr = 150;
	undo_fake.reader.ReadRecPtr = 150;
	undo_fake.u.decoded.lsn = 150;
	undo_plan = make_undo_record_plan(&undo_fake);
	memset(&cut, 0, sizeof(cut));
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 200;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	memset(&request, 0, sizeof(request));
	request.system_identifier = xact_identity.record.system_identifier;
	memcpy(request.storage_uuid, storage_uuid, sizeof(storage_uuid));
	request.physical_cuts = &cut;
	request.participant_count = 1;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &xact_plan, &xact_identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &undo_plan, &undo_identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	memset(&capture, 0, sizeof(capture));
	memset(&apply_ops, 0, sizeof(apply_ops));
	apply_ops.arg = &capture;
	apply_ops.begin_protected_set = capture_begin;
	apply_ops.end_protected_set = capture_end;
	apply_ops.preflight_xact = accept_preflight;
	apply_ops.preflight_undo = reject_undo_preflight;
	apply_ops.apply_xact = capture_apply;
	apply_ops.apply_undo = capture_apply_undo;
	UT_ASSERT_EQ(rf_side_online_plan_preflight_v1(plan, &apply_ops),
				 RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
	UT_ASSERT_EQ(capture.count, 0);
	UT_ASSERT_EQ(capture.undo_count, 0);
	UT_ASSERT_EQ(capture.begin_count, 1);
	UT_ASSERT_EQ(capture.end_count, 1);
	UT_ASSERT(!capture.end_complete);
	memset(&capture, 0, sizeof(capture));
	UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &apply_ops),
				 RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
	UT_ASSERT_EQ(capture.count, 0);
	UT_ASSERT_EQ(capture.undo_count, 0);
	UT_ASSERT_EQ(capture.begin_count, 1);
	UT_ASSERT_EQ(capture.end_count, 1);
	UT_ASSERT(!capture.end_complete);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_online_plan_denies_terminal_missing_required_tt_commit)
{
	FakeXactRecord prepare_fake;
	FakeXactRecord terminal_fake;
	RfDetachedRecordPlanV1 terminal_plan;
	RfPageOnlineRecordIdentityV1 terminal_identity;
	RfSideOnlinePlanRequestV1 request;
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut;
	RfSideOnlineApplyOpsV1 apply_ops;
	ApplyCapture capture;
	XLogReaderState *prepare_record;
	uint8 storage_uuid[16];

	reset_prepare_apply();
	prepare_record = make_prepare(&prepare_fake, 802, true, false);
	prepare_apply.native_payload_length = XLogRecGetDataLen(prepare_record);
	memcpy(prepare_apply.native_payload, XLogRecGetData(prepare_record),
		   prepare_apply.native_payload_length);
	(void)make_commit_prepared(&terminal_fake, 802, 16384, UINT64_C(901), INT64_C(123456));
	memset(storage_uuid, 0x42, sizeof(storage_uuid));
	terminal_identity = make_identity(&terminal_fake, storage_uuid);
	terminal_plan = make_record_plan(&terminal_fake);
	memset(&cut, 0, sizeof(cut));
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 200;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	memset(&request, 0, sizeof(request));
	request.system_identifier = terminal_identity.record.system_identifier;
	memcpy(request.storage_uuid, storage_uuid, sizeof(storage_uuid));
	request.physical_cuts = &cut;
	request.participant_count = 1;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &terminal_plan, &terminal_identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	memset(&capture, 0, sizeof(capture));
	memset(&apply_ops, 0, sizeof(apply_ops));
	apply_ops.arg = &capture;
	apply_ops.begin_protected_set = capture_begin;
	apply_ops.end_protected_set = capture_end;
	apply_ops.preflight_xact = accept_preflight;
	apply_ops.apply_xact = capture_apply;
	UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &apply_ops),
				 RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
	UT_ASSERT_EQ(capture.count, 0);
	UT_ASSERT_EQ(capture.begin_count, 1);
	UT_ASSERT_EQ(capture.end_count, 1);
	UT_ASSERT(!capture.end_complete);
	rf_side_online_plan_destroy_v1(&plan);
}

static void
check_preceding_tt_commit_dependency(bool historical)
{
	FakeXactRecord prepare_fake;
	FakeXactRecord undo_fake;
	FakeXactRecord terminal_fake;
	RfDetachedRecordPlanV1 undo_plan;
	RfDetachedRecordPlanV1 terminal_plan;
	RfPageOnlineRecordIdentityV1 undo_identity;
	RfPageOnlineRecordIdentityV1 terminal_identity;
	RfSideOnlinePlanRequestV1 request;
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut;
	RfSideOnlineApplyOpsV1 apply_ops;
	ApplyCapture capture;
	XLogReaderState *prepare_record;
	uint8 storage_uuid[16];
	XLogRecPtr redo = historical ? 200 : 100;

	reset_prepare_apply();
	prepare_record = make_prepare(&prepare_fake, 802, true, false);
	prepare_apply.native_payload_length = XLogRecGetDataLen(prepare_record);
	memcpy(prepare_apply.native_payload, XLogRecGetData(prepare_record),
		   prepare_apply.native_payload_length);
	(void)make_tt_commit_delta(&undo_fake, 802, UINT64_C(901));
	(void)make_commit_prepared(&terminal_fake, 802, 16384, UINT64_C(901), INT64_C(123456));
	memset(storage_uuid, 0x42, sizeof(storage_uuid));
	undo_identity = make_identity(&undo_fake, storage_uuid);
	terminal_identity = make_identity(&terminal_fake, storage_uuid);
	terminal_identity.record.read_rec_ptr = 200;
	terminal_identity.record.end_rec_ptr = 300;
	terminal_fake.reader.ReadRecPtr = 200;
	terminal_fake.reader.EndRecPtr = 300;
	terminal_fake.u.decoded.lsn = 200;
	terminal_fake.u.decoded.next_lsn = 300;
	undo_plan = make_undo_record_plan(&undo_fake);
	terminal_plan = make_record_plan(&terminal_fake);
	memset(&cut, 0, sizeof(cut));
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 300;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	memset(&request, 0, sizeof(request));
	request.system_identifier = undo_identity.record.system_identifier;
	memcpy(request.storage_uuid, storage_uuid, sizeof(storage_uuid));
	request.physical_cuts = &cut;
	request.participant_count = 1;
	request.redo_starts = &redo;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &undo_plan, &undo_identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &terminal_plan, &terminal_identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	memset(&capture, 0, sizeof(capture));
	memset(&apply_ops, 0, sizeof(apply_ops));
	apply_ops.arg = &capture;
	apply_ops.begin_protected_set = capture_begin;
	apply_ops.end_protected_set = capture_end;
	apply_ops.preflight_xact = accept_preflight;
	apply_ops.preflight_undo = accept_preflight;
	apply_ops.apply_xact = capture_apply;
	apply_ops.apply_undo = capture_apply_undo;
	UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &apply_ops), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(capture.count, 1);
	UT_ASSERT_EQ(capture.undo_count, historical ? 0 : 1);
	UT_ASSERT(capture.end_complete);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_online_plan_accepts_exact_preceding_tt_commit_dependency)
{
	check_preceding_tt_commit_dependency(false);
	check_preceding_tt_commit_dependency(true);
}

UT_TEST(test_online_plan_denies_abort_terminal_missing_tt_abort)
{
	FakeXactRecord prepare_fake;
	FakeXactRecord terminal_fake;
	RfDetachedRecordPlanV1 terminal_plan;
	RfPageOnlineRecordIdentityV1 terminal_identity;
	RfSideOnlinePlanRequestV1 request;
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut;
	RfSideOnlineApplyOpsV1 apply_ops;
	ApplyCapture capture;
	XLogReaderState *prepare_record;
	uint8 storage_uuid[16];

	reset_prepare_apply();
	prepare_record = make_prepare(&prepare_fake, 802, true, false);
	prepare_apply.native_payload_length = XLogRecGetDataLen(prepare_record);
	memcpy(prepare_apply.native_payload, XLogRecGetData(prepare_record),
		   prepare_apply.native_payload_length);
	(void)make_abort_prepared(&terminal_fake, 802, 16384, UINT64_C(902));
	memset(storage_uuid, 0x42, sizeof(storage_uuid));
	terminal_identity = make_identity(&terminal_fake, storage_uuid);
	terminal_plan = make_record_plan(&terminal_fake);
	memset(&cut, 0, sizeof(cut));
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 200;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	memset(&request, 0, sizeof(request));
	request.system_identifier = terminal_identity.record.system_identifier;
	memcpy(request.storage_uuid, storage_uuid, sizeof(storage_uuid));
	request.physical_cuts = &cut;
	request.participant_count = 1;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &terminal_plan, &terminal_identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	memset(&capture, 0, sizeof(capture));
	memset(&apply_ops, 0, sizeof(apply_ops));
	apply_ops.arg = &capture;
	apply_ops.begin_protected_set = capture_begin;
	apply_ops.end_protected_set = capture_end;
	apply_ops.preflight_xact = accept_preflight;
	apply_ops.apply_xact = capture_apply;
	UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &apply_ops),
				 RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
	UT_ASSERT_EQ(capture.count, 0);
	UT_ASSERT(!capture.end_complete);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_online_plan_accepts_exact_preceding_tt_abort_dependency)
{
	FakeXactRecord prepare_fake;
	FakeXactRecord abort_fake;
	FakeXactRecord terminal_fake;
	RfDetachedRecordPlanV1 abort_plan;
	RfDetachedRecordPlanV1 terminal_plan;
	RfPageOnlineRecordIdentityV1 abort_identity;
	RfPageOnlineRecordIdentityV1 terminal_identity;
	RfSideOnlinePlanRequestV1 request;
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut;
	RfSideOnlineApplyOpsV1 apply_ops;
	ApplyCapture capture;
	XLogReaderState *prepare_record;
	uint8 storage_uuid[16];

	reset_prepare_apply();
	prepare_record = make_prepare(&prepare_fake, 802, true, false);
	prepare_apply.native_payload_length = XLogRecGetDataLen(prepare_record);
	memcpy(prepare_apply.native_payload, XLogRecGetData(prepare_record),
		   prepare_apply.native_payload_length);
	prepare_apply.slot.first_undo_block = (UBA)InvalidUba_init;
	(void)make_tt_abort_delta(&abort_fake, 802);
	(void)make_abort_prepared(&terminal_fake, 802, 16384, UINT64_C(902));
	memset(storage_uuid, 0x42, sizeof(storage_uuid));
	abort_identity = make_identity(&abort_fake, storage_uuid);
	terminal_identity = make_identity(&terminal_fake, storage_uuid);
	set_identity_range(&terminal_fake, &terminal_identity, 200, 300);
	abort_plan = make_undo_record_plan(&abort_fake);
	terminal_plan = make_record_plan(&terminal_fake);
	memset(&cut, 0, sizeof(cut));
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 300;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	memset(&request, 0, sizeof(request));
	request.system_identifier = abort_identity.record.system_identifier;
	memcpy(request.storage_uuid, storage_uuid, sizeof(storage_uuid));
	request.physical_cuts = &cut;
	request.participant_count = 1;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &abort_plan, &abort_identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &terminal_plan, &terminal_identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	memset(&capture, 0, sizeof(capture));
	memset(&apply_ops, 0, sizeof(apply_ops));
	apply_ops.arg = &capture;
	apply_ops.begin_protected_set = capture_begin;
	apply_ops.end_protected_set = capture_end;
	apply_ops.preflight_xact = accept_preflight;
	apply_ops.preflight_undo = accept_preflight;
	apply_ops.apply_xact = capture_apply;
	apply_ops.apply_undo = capture_apply_undo;
	UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &apply_ops), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(capture.count, 1);
	UT_ASSERT_EQ(capture.undo_count, 1);
	UT_ASSERT(capture.end_complete);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_online_plan_denies_nonempty_abort_without_undo_completion)
{
	FakeXactRecord prepare_fake;
	FakeXactRecord abort_fake;
	FakeXactRecord head_fake;
	FakeXactRecord terminal_fake;
	RfDetachedRecordPlanV1 abort_plan;
	RfDetachedRecordPlanV1 head_plan;
	RfDetachedRecordPlanV1 terminal_plan;
	RfPageOnlineRecordIdentityV1 abort_identity;
	RfPageOnlineRecordIdentityV1 head_identity;
	RfPageOnlineRecordIdentityV1 terminal_identity;
	RfSideOnlinePlanRequestV1 request;
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut;
	RfSideOnlineApplyOpsV1 apply_ops;
	ApplyCapture capture;
	XLogReaderState *prepare_record;
	UBA head = InvalidUba_init;
	uint8 storage_uuid[16];

	head.raw[0] = UINT64_C(513) | (UINT64_C(9) << 32);
	head.raw[1] = UINT64_C(4);
	reset_prepare_apply();
	prepare_record = make_prepare_with_head(&prepare_fake, 802, true, false, &head);
	prepare_apply.native_payload_length = XLogRecGetDataLen(prepare_record);
	memcpy(prepare_apply.native_payload, XLogRecGetData(prepare_record),
		   prepare_apply.native_payload_length);
	prepare_apply.slot.first_undo_block = head;
	(void)make_tt_abort_delta(&abort_fake, 802);
	(void)make_tt_set_head_delta(&head_fake, 802, &head);
	(void)make_abort_prepared(&terminal_fake, 802, 16384, UINT64_C(902));
	memset(storage_uuid, 0x42, sizeof(storage_uuid));
	abort_identity = make_identity(&abort_fake, storage_uuid);
	head_identity = make_identity(&head_fake, storage_uuid);
	terminal_identity = make_identity(&terminal_fake, storage_uuid);
	set_identity_range(&head_fake, &head_identity, 200, 300);
	set_identity_range(&terminal_fake, &terminal_identity, 200, 300);
	abort_plan = make_undo_record_plan(&abort_fake);
	head_plan = make_undo_record_plan(&head_fake);
	terminal_plan = make_record_plan(&terminal_fake);
	memset(&cut, 0, sizeof(cut));
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 300;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	memset(&request, 0, sizeof(request));
	request.system_identifier = abort_identity.record.system_identifier;
	memcpy(request.storage_uuid, storage_uuid, sizeof(storage_uuid));
	request.physical_cuts = &cut;
	request.participant_count = 1;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &abort_plan, &abort_identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &terminal_plan, &terminal_identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	memset(&capture, 0, sizeof(capture));
	memset(&apply_ops, 0, sizeof(apply_ops));
	apply_ops.arg = &capture;
	apply_ops.begin_protected_set = capture_begin;
	apply_ops.end_protected_set = capture_end;
	apply_ops.preflight_xact = accept_preflight;
	apply_ops.preflight_undo = accept_preflight;
	apply_ops.apply_xact = capture_apply;
	apply_ops.apply_undo = capture_apply_undo;
	UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &apply_ops),
				 RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
	UT_ASSERT_EQ(capture.count, 0);
	UT_ASSERT_EQ(capture.undo_count, 0);
	rf_side_online_plan_destroy_v1(&plan);

	cut.scan_end_exclusive = 400;
	set_identity_range(&terminal_fake, &terminal_identity, 300, 400);
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &abort_plan, &abort_identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &head_plan, &head_identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &terminal_plan, &terminal_identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	memset(&capture, 0, sizeof(capture));
	memset(&apply_ops, 0, sizeof(apply_ops));
	apply_ops.arg = &capture;
	apply_ops.begin_protected_set = capture_begin;
	apply_ops.end_protected_set = capture_end;
	apply_ops.preflight_xact = accept_preflight;
	apply_ops.preflight_undo = accept_preflight;
	apply_ops.apply_xact = capture_apply;
	apply_ops.apply_undo = capture_apply_undo;
	UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &apply_ops),
				 RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
	UT_ASSERT_EQ(capture.count, 0);
	UT_ASSERT_EQ(capture.undo_count, 0);
	UT_ASSERT(!capture.end_complete);
	rf_side_online_plan_destroy_v1(&plan);
}

static bool
raise_owner_error(void *arg pg_attribute_unused(),
				  const RfSideOnlineOperationV1 *operation pg_attribute_unused())
{
	pg_re_throw();
}

UT_TEST(test_protected_set_released_on_preflight_or_apply_error)
{
	FakeXactRecord fake;
	RfDetachedRecordPlanV1 record_plan;
	RfPageOnlineRecordIdentityV1 identity;
	RfSideOnlinePlanRequestV1 request = { 0 };
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut = { 0 };
	RfSideOnlineApplyOpsV1 ops = { 0 };
	ApplyCapture *capture = calloc(1, sizeof(*capture));
	uint8 storage_uuid[16];

	memset(storage_uuid, 0x42, sizeof(storage_uuid));
	(void)make_commit(&fake, 800, UINT64_C(901), INT64_C(123456), false);
	identity = make_identity(&fake, storage_uuid);
	record_plan = make_record_plan(&fake);
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 200;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	request.system_identifier = identity.record.system_identifier;
	memcpy(request.storage_uuid, storage_uuid, sizeof(storage_uuid));
	request.physical_cuts = &cut;
	request.participant_count = 1;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record_plan, &identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	ops.arg = capture;
	ops.begin_protected_set = capture_begin;
	ops.end_protected_set = capture_end;
	ops.source_thread = 3;
	for (int cutpoint = 0; cutpoint < 3; cutpoint++) {
		volatile bool caught = false;

		memset(capture, 0, sizeof(*capture));
		ops.preflight_xact = cutpoint == 2 ? accept_preflight : raise_owner_error;
		ops.apply_xact = cutpoint == 2 ? raise_owner_error : capture_apply;
		PG_TRY();
		{
			if (cutpoint == 0)
				(void)rf_side_online_plan_preflight_v1(plan, &ops);
			else
				(void)rf_side_online_plan_apply_v1(plan, &ops);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(capture->begin_count, 1);
		UT_ASSERT_EQ(capture->end_count, 1);
		UT_ASSERT(!capture->end_complete);
		UT_ASSERT_EQ(capture->count, 0);
		UT_ASSERT(PG_exception_stack == NULL);
		/* A clean retry must acquire/release its own scope normally. */
		ops.preflight_xact = accept_preflight;
		ops.apply_xact = capture_apply;
		UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &ops), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(capture->begin_count, 2);
		UT_ASSERT_EQ(capture->end_count, 2);
		UT_ASSERT(capture->end_complete);
	}
	rf_side_online_plan_destroy_v1(&plan);
	free(capture);
}

static ClusterSpaceReservationChange
space_advance_fixture(void)
{
	ClusterSpaceReservationChange c = { 0 };

	c.action = CLUSTER_SPACE_RESERVATION_ADVANCE;
	c.before.identity.key.system_identifier = UINT64_C(0x11223344);
	c.before.identity.key.database_incarnation = 42;
	memset(c.before.identity.key.storage_uuid, 0x44, 16);
	c.before.identity.key.locator = (RelFileLocator){ DEFAULTTABLESPACE_OID, 5, 16384 };
	memset(c.before.identity.incarnation, 0x17, 16);
	c.before.identity.sequence = c.before.identity.operation = 1;
	c.before.identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	c.before.next_block = c.first_block = 3;
	c.granted = 4;
	c.result = c.before;
	c.result.next_block += c.granted;
	c.before_token = 100;
	c.result_token = 80;
	return c;
}

static RfSideOnlinePlanV1 *
space_online_plan_redo(uint64 end, const XLogRecPtr *redo)
{
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut = { 0 };
	RfSideOnlinePlanRequestV1 request = { 0 };

	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = end;
	request.system_identifier = UINT64_C(0x11223344);
	memset(request.storage_uuid, 0x44, 16);
	request.physical_cuts = &cut;
	request.participant_count = 1;
	request.redo_starts = redo;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	return plan;
}

static RfSideOnlinePlanV1 *
space_online_plan(uint64 end)
{
	return space_online_plan_redo(end, NULL);
}

static ClusterSpaceStructureChange
space_drop_fixture(uint32 relnumber)
{
	ClusterSpaceReservationChange advance = space_advance_fixture();
	ClusterSpaceStructureChange drop = { 0 };

	drop.identity.action = CLUSTER_SPACE_WAL_TOMBSTONE;
	drop.identity.nblocks = InvalidBlockNumber;
	drop.identity.expected = advance.result.identity;
	drop.identity.expected.key.locator.relNumber = relnumber;
	drop.identity.result = drop.identity.expected;
	drop.identity.result.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
	drop.identity.result.sequence++;
	drop.identity.result.operation++;
	drop.identity.before_token = 123;
	drop.identity.result_token = 70;
	drop.reservation.action = CLUSTER_SPACE_RESERVATION_TOMBSTONE;
	drop.reservation.before = advance.result;
	drop.reservation.before.identity = drop.identity.expected;
	drop.reservation.result = drop.reservation.before;
	drop.reservation.result.identity = drop.identity.result;
	drop.reservation.before_token = advance.result_token;
	drop.reservation.result_token = drop.identity.result_token;
	return drop;
}

static void
make_space_commit(FakeXactRecord *fake, const ClusterSpaceStructureChange *drops, uint32 count)
{
	uint32 prefix = MinSizeOfXactCommit + sizeof(xl_xact_xinfo);
	uint32 locator_bytes = sizeof(int) + count * sizeof(RelFileLocator);
	uint32 old_len;
	uint32 xinfo;
	int nrels = (int)count;

	make_commit(fake, 802, 901, 123456, false);
	fake->u.decoded.max_block_id = -1;
	old_len = fake->u.decoded.main_data_len;
	UT_ASSERT(old_len + locator_bytes + sizeof(count) + count * CLUSTER_SPACE_STRUCTURE_WAL_BYTES
			  <= sizeof(fake->data));
	memmove(fake->data + prefix + locator_bytes, fake->data + prefix, old_len - prefix);
	memcpy(&xinfo, fake->data + MinSizeOfXactCommit, sizeof(xinfo));
	xinfo |= XACT_XINFO_HAS_RELFILELOCATORS | XACT_XINFO_HAS_SPACE_DROP;
	memcpy(fake->data + MinSizeOfXactCommit, &xinfo, sizeof(xinfo));
	memcpy(fake->data + prefix, &nrels, sizeof(nrels));
	for (uint32 i = 0; i < count; i++)
		memcpy(fake->data + prefix + sizeof(nrels) + i * sizeof(RelFileLocator),
			   &drops[i].identity.result.key.locator, sizeof(RelFileLocator));
	fake->u.decoded.main_data_len += locator_bytes;
	memcpy(fake->data + fake->u.decoded.main_data_len, &count, sizeof(count));
	fake->u.decoded.main_data_len += sizeof(count);
	for (uint32 i = 0; i < count; i++) {
		UT_ASSERT(cluster_space_structure_wal_encode(&drops[i],
													 fake->data + fake->u.decoded.main_data_len,
													 CLUSTER_SPACE_STRUCTURE_WAL_BYTES));
		fake->u.decoded.main_data_len += CLUSTER_SPACE_STRUCTURE_WAL_BYTES;
	}
}

static ClusterSpaceStructureChange
space_truncate_fixture(const ClusterSpaceReservation *before, uint64 before_token,
					   uint64 identity_token, uint64 result_token, uint8 incarnation)
{
	ClusterSpaceStructureChange change = { 0 };

	change.identity.action = CLUSTER_SPACE_WAL_TRUNCATE;
	change.identity.nblocks = 2;
	change.identity.expected = before->identity;
	change.identity.result = before->identity;
	memset(change.identity.result.incarnation, incarnation, 16);
	change.identity.result.sequence++;
	change.identity.result.operation++;
	change.identity.before_token = identity_token;
	change.identity.result_token = result_token;
	change.reservation.action = CLUSTER_SPACE_RESERVATION_RESET;
	change.reservation.before = *before;
	change.reservation.result = *before;
	change.reservation.result.identity = change.identity.result;
	change.reservation.result.next_block = change.reservation.first_block = 2;
	change.reservation.before_token = before_token;
	change.reservation.result_token = result_token;
	return change;
}

static RfPageProofDetailV1
space_commit_feed(RfSideOnlinePlanV1 *plan, FakeXactRecord *fake, XLogRecPtr begin)
{
	uint8 uuid[16];
	RfPageOnlineRecordIdentityV1 identity;
	RfDetachedRecordPlanV1 record;

	memset(uuid, 0x44, sizeof(uuid));
	identity = make_identity(fake, uuid);
	set_identity_range(fake, &identity, begin, begin + 100);
	record = make_record_plan(fake);
	return rf_side_online_plan_feed_record_v1(plan, &record, &identity);
}

static RfPageProofDetailV1
space_online_feed(RfSideOnlinePlanV1 *plan, uint8 info, const void *wal, uint32 length,
				  XLogRecPtr begin, bool block_ref)
{
	FakeXactRecord fake;
	RfDetachedRecordPlanV1 record;
	RfPageOnlineRecordIdentityV1 identity;
	uint8 storage_uuid[16];

	memset(storage_uuid, 0x44, sizeof(storage_uuid));
	make_projection_record(&fake, RM_SMGR_ID, info | XLR_SPECIAL_REL_UPDATE, wal, length);
	fake.u.decoded.max_block_id = block_ref ? 0 : -1;
	identity = make_identity(&fake, storage_uuid);
	set_identity_range(&fake, &identity, begin, begin + 100);
	record = make_projection_record_plan(&fake);
	return rf_side_online_plan_feed_record_v1(plan, &record, &identity);
}

UT_TEST(test_space_commit_owned_multiple_targets_close_exact_source_chains)
{
	ClusterSpaceReservationChange advance = space_advance_fixture();
	ClusterSpaceStructureChange drops[2] = { space_drop_fixture(16384), space_drop_fixture(16385) };
	FakeXactRecord fake;
	RfSideOnlinePlanV1 *plan = space_online_plan(300);
	RfSideOnlineOperationV1 operation;
	uint8 wal[CLUSTER_SPACE_RESERVATION_WAL_BYTES];
	PGAlignedBlock pages[2];
	ClusterSpaceRecoveryImage out;
	uint32 order[2], count;
	RfPageProofDetailV1 detail;

	reset_prepare_apply();
	UT_ASSERT(cluster_space_reservation_wal_encode(&advance, wal, sizeof(wal)));
	UT_ASSERT_EQ(space_online_feed(plan, XLOG_SMGR_SPACE_RESERVATION, wal, sizeof(wal), 100, false),
				 RF_PAGE_PROOF_DETAIL_OK);
	make_space_commit(&fake, drops, 2);
	detail = space_commit_feed(plan, &fake, 200);
	UT_ASSERT_EQ(detail, RF_PAGE_PROOF_DETAIL_OK);
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		goto done;
	/* The native decoder buffer is reused immediately after feeding. */
	memset(fake.data, 0xee, sizeof(fake.data));
	UT_ASSERT(!rf_side_online_plan_bind_database_v1(plan, 43));
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 2);
	for (int i = 0; i < 2; i++) {
		ClusterSpaceReservation result;
		uint64 token;

		UT_ASSERT(cluster_space_identity_page_encode(&drops[i].identity.expected, 123,
													 pages[0].data, BLCKSZ));
		UT_ASSERT(cluster_space_reservation_page_encode(
			i == 0 ? &advance.before : &drops[i].reservation.before,
			i == 0 ? advance.before_token : drops[i].reservation.before_token, pages[1].data,
			BLCKSZ));
		detail = rf_side_online_plan_prepare_space_v1(plan, &drops[i].identity.result.key,
													  pages[0].data, pages[1].data, order, 2,
													  &count, &out);
		UT_ASSERT_EQ(detail, RF_PAGE_PROOF_DETAIL_OK);
		if (detail != RF_PAGE_PROOF_DETAIL_OK)
			goto done;
		UT_ASSERT_EQ(count, i == 0 ? 2 : 1);
		UT_ASSERT_EQ(order[count - 1], 1);
		UT_ASSERT_EQ(out.source_index[0], 1);
		UT_ASSERT_EQ(out.source_index[1], 1);
		UT_ASSERT(cluster_space_reservation_page_decode(out.pages[1].data, BLCKSZ, SPACE_FORKNUM, 1,
														&drops[i].identity.result.key, &result,
														&token));
		UT_ASSERT_EQ(result.identity.state, CLUSTER_SPACE_IDENTITY_TOMBSTONED);
		UT_ASSERT_EQ(result.next_block, 7);
		UT_ASSERT_EQ(token, 70);
	}
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
	/* A source plan cannot silently apply TT/projection and drop its structural effects. */
	UT_ASSERT_EQ(rf_side_xact_apply_owned_v1(&operation.xact, operation.owned_payload,
											 operation.owned_payload_length),
				 RF_SIDE_XACT_APPLY_BLOCKED);
	UT_ASSERT_EQ(rf_side_xact_apply_v1(&operation.xact), RF_SIDE_XACT_APPLY_BLOCKED);
	UT_ASSERT_EQ(prepare_apply.tt_stamps, 0);
	UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
done:
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_space_commit_rejects_unowned_duplicate_or_malformed_drop_inputs)
{
	for (int fault = 0; fault < 10; fault++) {
		ClusterSpaceStructureChange drops[2]
			= { space_drop_fixture(16384), space_drop_fixture(16385) };
		RfSideOnlinePlanV1 *plan = space_online_plan(200);
		FakeXactRecord fake;
		uint32 prefix = MinSizeOfXactCommit + sizeof(xl_xact_xinfo) + sizeof(int);

		if (fault == 1)
			drops[1] = drops[0];
		if (fault == 2) {
			ClusterSpaceStructureChange tmp = drops[0];

			drops[0] = drops[1];
			drops[1] = tmp;
		}
		if (fault == 6 || fault == 7 || fault == 8) {
			ClusterSpaceIdentityKey *key = &drops[1].identity.expected.key;

			if (fault == 6)
				key->system_identifier++;
			if (fault == 7)
				key->storage_uuid[0]++;
			if (fault == 8)
				key->database_incarnation++;
			drops[1].identity.result.key = *key;
			drops[1].reservation.before.identity.key = *key;
			drops[1].reservation.result.identity.key = *key;
		}
		make_space_commit(&fake, drops, 2);
		if (fault == 0) {
			RelFileLocator wrong = { DEFAULTTABLESPACE_OID, 5, 99999 };

			memcpy(fake.data + prefix, &wrong, sizeof(wrong));
		}
		if (fault == 3)
			fake.u.decoded.main_data_len--;
		if (fault == 4) {
			uint32 count = UINT32_MAX;

			memcpy(fake.data + fake.u.decoded.main_data_len - 2 * CLUSTER_SPACE_STRUCTURE_WAL_BYTES
					   - sizeof(count),
				   &count, sizeof(count));
		}
		if (fault == 5)
			fake.u.decoded.max_block_id = 0;
		if (fault == 9)
			fake.u.decoded.header.xl_info = XLOG_XACT_COMMIT_PREPARED | XLOG_XACT_HAS_INFO;
		if (fault == 8) {
			UT_ASSERT_EQ(space_commit_feed(plan, &fake, 100), RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT(!rf_side_online_plan_bind_database_v1(plan, 42));
			UT_ASSERT(!rf_side_online_plan_bind_database_v1(plan, 43));
			UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH);
		} else {
			UT_ASSERT(space_commit_feed(plan, &fake, 100) != RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 0);
		}
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_space_commit_retains_all_native_side_effects_and_refuses_tt_only_owner)
{
	ClusterSpaceStructureChange drop = space_drop_fixture(16384);
	RfSideOnlinePlanV1 *plan = space_online_plan(200);
	FakeXactRecord fake;
	RfSideXactOperationV1 operation;
	RfSideOnlineOperationV1 owned;
	xl_xact_parsed_commit parsed;
	uint8 saved[sizeof(fake.data)];
	TransactionId subxid = 818;
	uint32 prefix = MinSizeOfXactCommit + sizeof(xl_xact_xinfo);
	uint32 xinfo, added = sizeof(int) + sizeof(subxid), len;
	int count = 1;

	reset_prepare_apply();
	make_space_commit(&fake, &drop, 1);
	memmove(fake.data + prefix + added, fake.data + prefix, fake.u.decoded.main_data_len - prefix);
	memcpy(fake.data + prefix, &count, sizeof(count));
	memcpy(fake.data + prefix + sizeof(count), &subxid, sizeof(subxid));
	memcpy(&xinfo, fake.data + MinSizeOfXactCommit, sizeof(xinfo));
	xinfo |= XACT_XINFO_HAS_SUBXACTS;
	memcpy(fake.data + MinSizeOfXactCommit, &xinfo, sizeof(xinfo));
	fake.u.decoded.main_data_len += added;
	len = fake.u.decoded.main_data_len;
	memcpy(saved, fake.data, len);
	/* The strict decoder retains the whole carrier but cannot consume its
	 * effects. Cold recovery must own the complete child/structural payload,
	 * never reduce it to TT-only coverage or infer child-write authority. */
	UT_ASSERT(rf_side_xact_decode_v1(&fake.reader, UINT64_C(0x11223344), 3, &operation));
	UT_ASSERT_EQ(operation.completion_payload_length, len);
	UT_ASSERT(
		ParseCommitRecord(XLogRecGetInfo(&fake.reader), (xl_xact_commit *)saved, len, &parsed));
	UT_ASSERT_EQ(parsed.nsubxacts, 1);
	UT_ASSERT_EQ(parsed.nrels, 1);
	UT_ASSERT_EQ(parsed.nspace_drops, 1);
	UT_ASSERT_EQ(parsed.subxacts[0], subxid);
	UT_ASSERT_EQ(parsed.tt_commit.xid, 802);
	UT_ASSERT_EQ(parsed.scn, 901);
	UT_ASSERT_EQ(rf_side_xact_apply_owned_v1(&operation, saved, len), RF_SIDE_XACT_APPLY_BLOCKED);
	UT_ASSERT(rf_side_xact_decode_recovery_v1(&fake.reader, UINT64_C(0x11223344), 3, &operation));
	UT_ASSERT(operation.cold_completion_effects && !operation.pi_read_only);
	UT_ASSERT_EQ(rf_side_xact_apply_owned_v1(&operation, saved, len), RF_SIDE_XACT_APPLY_BLOCKED);
	UT_ASSERT_EQ(space_commit_feed(plan, &fake, 100), RF_PAGE_PROOF_DETAIL_OK);
	memset(fake.data, 0xee, len);
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 1);
	if (rf_side_online_plan_operation_v1(plan, 0, &owned)) {
		UT_ASSERT(owned.xact.cold_completion_effects);
		UT_ASSERT_EQ(owned.owned_payload_length, len);
		UT_ASSERT(memcmp(owned.owned_payload, saved, len) == 0);
	} else
		UT_ASSERT(false);
	UT_ASSERT_EQ(prepare_apply.tt_stamps, 0);
	UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_space_plan_owns_exact_chain_and_requires_observed_namespace)
{
	ClusterSpaceReservationChange c = space_advance_fixture();
	ClusterSpaceIdentityKey key = c.result.identity.key;
	RfSideOnlinePlanV1 *plan = space_online_plan(300);
	RfSideOnlineApplyOpsV1 ops = { 0 };
	ApplyCapture capture = { 0 };
	ClusterSpaceRecoveryImage out = { 0 };
	PGAlignedBlock pages[2] = { 0 };
	uint8 wal[CLUSTER_SPACE_RESERVATION_WAL_BYTES];
	uint32 order[2] = { 99, 99 }, count = 99;
	RfOpcodeRouteV1 route;
	ClusterSpaceReservation final;
	uint64 token;

	UT_ASSERT_EQ(
		rf_opcode_route_lookup_v1(RM_SMGR_ID, XLOG_SMGR_SPACE_RESERVATION, false, false, &route),
		RF_OPCODE_ROUTE_OK);
	UT_ASSERT_EQ(route.record_owner, RF_ROUTE_OWNER_SIDE_TYPED);

	UT_ASSERT(cluster_space_identity_page_encode(&c.before.identity, 123, pages[0].data, BLCKSZ));
	UT_ASSERT(
		cluster_space_reservation_page_encode(&c.before, c.before_token, pages[1].data, BLCKSZ));
	for (int i = 0; i < 2; i++) {
		UT_ASSERT(cluster_space_reservation_wal_encode(&c, wal, sizeof(wal)));
		UT_ASSERT_EQ(space_online_feed(plan, XLOG_SMGR_SPACE_RESERVATION, wal, sizeof(wal),
									   100 + i * 100, false),
					 RF_PAGE_PROOF_DETAIL_OK);
		c.before = c.result;
		c.before_token = c.result_token;
		c.first_block = c.before.next_block;
		c.result.next_block += c.granted;
		c.result_token -= 20;
	}
	memset(wal, 0xee, sizeof(wal));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH);
	UT_ASSERT(!rf_side_online_plan_bind_database_v1(plan, 0));
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT(!rf_side_online_plan_bind_database_v1(plan, 43));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 2);
	UT_ASSERT_EQ(rf_side_online_plan_prepare_space_v1(plan, &key, pages[0].data, pages[1].data,
													  order, 2, &count, &out),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(count, 2);
	UT_ASSERT_EQ(order[0], 0);
	UT_ASSERT_EQ(order[1], 1);
	UT_ASSERT_EQ(out.apply_mask, 2);
	UT_ASSERT(memcmp(out.pages[0].data, pages[0].data, BLCKSZ) == 0);
	UT_ASSERT(cluster_space_reservation_page_decode(out.pages[1].data, BLCKSZ, SPACE_FORKNUM, 1,
													&key, &final, &token));
	UT_ASSERT_EQ(final.next_block, 11);
	UT_ASSERT_EQ(token, 60);
	/* Collecting source bytes cannot supply the missing protected writer. */
	ops.arg = &capture;
	ops.begin_protected_set = capture_begin;
	ops.end_protected_set = capture_end;
	UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &ops), RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT);
	UT_ASSERT_EQ(capture.begin_count, 0);
	if (count == 2) {
		ClusterSpaceRecoveryImage saved = out;

		count = 99;
		UT_ASSERT_EQ(rf_side_online_plan_prepare_space_v1(plan, &key, pages[0].data, pages[1].data,
														  order, 1, &count, &out),
					 RF_PAGE_PROOF_DETAIL_CAPACITY);
		UT_ASSERT_EQ(count, 99);
		UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
		key.database_incarnation++;
		UT_ASSERT_EQ(rf_side_online_plan_prepare_space_v1(plan, &key, pages[0].data, pages[1].data,
														  order, 2, &count, &out),
					 RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH);
		UT_ASSERT_EQ(count, 99);
		UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	}
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_space_plan_structural_pair_and_standalone_tombstone)
{
	ClusterSpaceStructureChange change = { 0 };
	ClusterSpaceReservationChange advance = space_advance_fixture();
	RfSideOnlinePlanV1 *plan = space_online_plan(200);
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	PGAlignedBlock zero = { 0 };
	ClusterSpaceRecoveryImage out;
	uint32 order, count = 0;
	RfSideSpaceContributionV1 contribution;

	change.identity.action = CLUSTER_SPACE_WAL_CREATE;
	change.identity.result = advance.result.identity;
	change.identity.result_token = 50;
	change.identity.nblocks = InvalidBlockNumber;
	change.reservation.action = CLUSTER_SPACE_RESERVATION_INIT;
	change.reservation.result.identity = change.identity.result;
	change.reservation.result_token = 50;
	UT_ASSERT(cluster_space_structure_wal_encode(&change, wal, sizeof(wal)));
	UT_ASSERT_EQ(space_online_feed(plan, XLOG_SMGR_SPACE_IDENTITY, wal, sizeof(wal), 100, false),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(!rf_side_online_plan_bind_database_v1(plan, 43));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH);
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_prepare_space_v1(plan, &change.identity.result.key, zero.data,
													  zero.data, &order, 1, &count, &out),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(count, 1);
	UT_ASSERT_EQ(out.apply_mask, 3);
	UT_ASSERT_EQ(rf_side_online_plan_space_contribution_count_v1(plan, 0), 1);
	UT_ASSERT(rf_side_online_plan_space_contribution_v1(plan, 0, 0, &contribution));
	UT_ASSERT_EQ(contribution.page_mask, 3);
	UT_ASSERT_EQ(contribution.result_token[0], change.identity.result_token);
	UT_ASSERT_EQ(contribution.result_token[1], change.reservation.result_token);
	UT_ASSERT(!rf_side_online_plan_bind_database_v1(plan, 42));
	rf_side_online_plan_destroy_v1(&plan);
	change.identity.action = CLUSTER_SPACE_WAL_TOMBSTONE;
	change.identity.expected = change.identity.result;
	change.identity.before_token = change.identity.result_token;
	change.identity.result.sequence++;
	change.identity.result.operation++;
	change.identity.result.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
	change.identity.result_token++;
	change.reservation.action = CLUSTER_SPACE_RESERVATION_TOMBSTONE;
	change.reservation.before = change.reservation.result;
	change.reservation.before_token = change.identity.before_token;
	change.reservation.result.identity = change.identity.result;
	change.reservation.result_token = change.identity.result_token;
	UT_ASSERT(cluster_space_structure_wal_encode(&change, wal, sizeof(wal)));
	plan = space_online_plan(200);
	UT_ASSERT(space_online_feed(plan, XLOG_SMGR_SPACE_IDENTITY, wal, sizeof(wal), 100, false)
			  != RF_PAGE_PROOF_DETAIL_OK);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_space_seal_rejects_incomplete_source_chains_before_target_io)
{
	for (int fault = 0; fault < 4; fault++) {
		ClusterSpaceReservationChange c = space_advance_fixture();
		RfSideOnlinePlanV1 *plan = space_online_plan(300);
		uint8 wal[CLUSTER_SPACE_RESERVATION_WAL_BYTES];

		UT_ASSERT(cluster_space_reservation_wal_encode(&c, wal, sizeof(wal)));
		UT_ASSERT_EQ(
			space_online_feed(plan, XLOG_SMGR_SPACE_RESERVATION, wal, sizeof(wal), 100, false),
			RF_PAGE_PROOF_DETAIL_OK);
		c.before = c.result;
		c.before_token = c.result_token;
		c.result_token = 60;
		if (fault == 0)
			c.before_token = 900;
		if (fault == 1)
			c.before_token = 100;
		if (fault == 2)
			c.before.next_block++;
		if (fault == 3)
			c.before.identity.incarnation[0]++;
		c.result = c.before;
		c.first_block = c.before.next_block;
		c.result.next_block += c.granted;
		UT_ASSERT(cluster_space_reservation_wal_encode(&c, wal, sizeof(wal)));
		UT_ASSERT_EQ(
			space_online_feed(plan, XLOG_SMGR_SPACE_RESERVATION, wal, sizeof(wal), 200, false),
			RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_VERSION_MISMATCH);
		UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 0);
		UT_ASSERT_EQ(rf_side_online_plan_space_target_count_v1(plan), UINT32_MAX);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_space_seal_enumerates_every_unique_commit_and_smgr_target)
{
	ClusterSpaceReservationChange c = space_advance_fixture();
	ClusterSpaceStructureChange drops[2] = { space_drop_fixture(16384), space_drop_fixture(16385) };
	RfSideOnlinePlanV1 *plan = space_online_plan(400);
	uint8 wal[CLUSTER_SPACE_RESERVATION_WAL_BYTES];
	FakeXactRecord fake;
	ClusterSpaceIdentityKey key, saved;

	UT_ASSERT_EQ(rf_side_online_plan_space_target_count_v1(plan), UINT32_MAX);
	c.before.identity.key.locator.relNumber = c.result.identity.key.locator.relNumber = 16385;
	UT_ASSERT(cluster_space_reservation_wal_encode(&c, wal, sizeof(wal)));
	UT_ASSERT_EQ(space_online_feed(plan, XLOG_SMGR_SPACE_RESERVATION, wal, sizeof(wal), 100, false),
				 RF_PAGE_PROOF_DETAIL_OK);
	make_space_commit(&fake, drops, 2);
	UT_ASSERT_EQ(space_commit_feed(plan, &fake, 200), RF_PAGE_PROOF_DETAIL_OK);
	c.before.identity.key.locator.relNumber = c.result.identity.key.locator.relNumber = 16383;
	UT_ASSERT(cluster_space_reservation_wal_encode(&c, wal, sizeof(wal)));
	UT_ASSERT_EQ(space_online_feed(plan, XLOG_SMGR_SPACE_RESERVATION, wal, sizeof(wal), 300, false),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_space_target_count_v1(plan), 3);
	UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 3);
	for (uint32 i = 0; i < 3; i++) {
		memset(&key, 0, sizeof(key));
		UT_ASSERT(rf_side_online_plan_space_target_v1(plan, i, &key));
		UT_ASSERT_EQ(key.locator.relNumber, 16383 + i);
		UT_ASSERT_EQ(key.system_identifier, c.result.identity.key.system_identifier);
		UT_ASSERT_EQ(key.database_incarnation, 42);
		UT_ASSERT(memcmp(key.storage_uuid, c.result.identity.key.storage_uuid, 16) == 0);
	}
	saved = key;
	UT_ASSERT(!rf_side_online_plan_space_target_v1(plan, 3, &key));
	UT_ASSERT(memcmp(&key, &saved, sizeof(key)) == 0);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_space_plan_rejects_unbound_native_shapes)
{
	for (int variant = 0; variant < 5; variant++) {
		ClusterSpaceReservationChange c = space_advance_fixture();
		RfSideOnlinePlanV1 *plan = space_online_plan(200);
		uint8 wal[CLUSTER_SPACE_RESERVATION_WAL_BYTES];
		uint8 opcode = XLOG_SMGR_SPACE_RESERVATION;
		uint32 len = sizeof(wal);

		if (variant == 0)
			c.result.identity.key.system_identifier++;
		if (variant == 1)
			c.result.identity.key.storage_uuid[0]++;
		if (variant <= 1)
			c.before.identity.key = c.result.identity.key;
		UT_ASSERT(cluster_space_reservation_wal_encode(&c, wal, sizeof(wal)));
		if (variant == 2)
			len--;
		if (variant == 3)
			opcode = XLOG_SMGR_SPACE_IDENTITY;
		UT_ASSERT(space_online_feed(plan, opcode, wal, len, 100, variant == 4)
				  != RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 0);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

static void
undo_header_feed(RfSideOnlinePlanV1 *plan, FakeXactRecord *fake, XLogRecPtr begin)
{
	uint8 uuid[16];
	RfPageOnlineRecordIdentityV1 identity;
	RfDetachedRecordPlanV1 record;

	memset(uuid, 0x44, sizeof(uuid));
	identity = make_identity(fake, uuid);
	set_identity_range(fake, &identity, begin, begin + 100);
	record = fake->u.decoded.header.xl_rmid == RM_CLUSTER_UNDO_ID ? make_undo_record_plan(fake)
																  : make_record_plan(fake);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	memset(fake->data, 0xee, sizeof(fake->data));
}

static void
undo_header_bind(FakeXactRecord *fake, uint32 generation)
{
	xl_undo_tt_slot_bind bind = { 0 };

	bind.instance = 3;
	bind.segment_id = 513;
	bind.segment_generation = generation;
	bind.slot_offset = 4;
	bind.wrap = 7;
	bind.xid = 802;
	bind.format_version = CLUSTER_UNDO_TT_BIND_VERSION;
	make_projection_record(fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_BIND, &bind, sizeof(bind));
}

static void
undo_header_commit(FakeXactRecord *fake, uint32 generation, TransactionId xid)
{
	xl_xact_tt_commit delta;
	uint32 offset;

	make_commit(fake, xid, 999, 12345, false);
	offset = fake->u.decoded.main_data_len - sizeof(delta);
	memcpy(&delta, fake->data + offset, sizeof(delta));
	delta.segment_id = 513;
	delta.segment_generation = generation;
	memcpy(fake->data + offset, &delta, sizeof(delta));
}

static void
undo_header_abort(FakeXactRecord *fake, uint16 slot)
{
	xl_undo_tt_slot_abort_exact abort = { 0 };

	abort.instance = 3;
	abort.segment_id = 513;
	abort.segment_generation = 0;
	abort.slot_offset = slot;
	abort.wrap = 7;
	abort.xid = 802;
	abort.format_version = CLUSTER_UNDO_TT_ABORT_EXACT_VERSION;
	make_projection_record(fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_ABORT, &abort,
						   sizeof(abort));
}

/* The original strict decoder/planner retain each carrier and reject unknown
 * shapes. No target IO is used to infer a missing transaction identity. */
static void
side_abort_carrier_identity(bool history)
{
	for (int fault = 0; fault < 16; fault++) {
		RfSideOnlinePlanV1 *plan;
		RfSideOnlineOperationV1 operation;
		ClusterUndoDecoded target, unchanged;
		FakeXactRecord fake;
		XLogRecPtr begin = 100;
		uint32 abort_index = 0;
		unsigned records
			= 3 + (fault == 4 || fault == 5 || fault == 6 || fault == 7 || fault == 11);
		XLogRecPtr end;

		if (fault == 1)
			records--;
		end = 100 + records * 100;
		plan = space_online_plan_redo(end, history ? &end : NULL);
		undo_header_bind(&fake, fault == 10 ? 1 : 0);
		undo_header_feed(plan, &fake, begin);
		begin += 100;
		if (fault == 3) {
			make_abort(&fake, 802);
			abort_index = 1;
			undo_header_feed(plan, &fake, begin);
			begin += 100;
		}
		if (fault != 1) {
			if (fault == 2)
				make_tt_abort_delta(&fake, 802);
			else
				undo_header_abort(&fake, 4);
			undo_header_feed(plan, &fake, begin);
			begin += 100;
		}
		if (fault == 4 || fault == 5) {
			undo_header_abort(&fake, fault == 5 ? 5 : 4);
			undo_header_feed(plan, &fake, begin);
			begin += 100;
		}
		if (fault == 6) {
			undo_header_bind(&fake, 0);
			undo_header_feed(plan, &fake, begin);
			begin += 100;
		}
		if (fault == 7) {
			xl_undo_segment_recycle recycle = { 0 };
			recycle.instance = 3;
			recycle.segment_id = 513;
			recycle.expected_generation = 0;
			recycle.old_state = SEGMENT_COMMITTED;
			recycle.new_state = SEGMENT_RECYCLABLE;
			make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_RECYCLE, &recycle,
								   sizeof(recycle));
			undo_header_feed(plan, &fake, begin);
			begin += 100;
		}
		if (fault == 11) {
			undo_header_commit(&fake, 0, 802);
			undo_header_feed(plan, &fake, begin);
			begin += 100;
		}
		if (fault != 3) {
			make_abort(&fake, 802);
			abort_index = records - 1;
			undo_header_feed(plan, &fake, begin);
		}
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, abort_index, &operation));
		UT_ASSERT_EQ(operation.history_only, history);
		if (fault == 8)
			operation.identity.record.origin_thread++;
		if (fault == 9)
			operation.xact.terminal_scn++;
		if (fault == 12)
			operation.purpose = RF_SIDE_PLAN_PI_CONTRIBUTIONS;
		if (fault == 13)
			operation.history_only = !operation.history_only;
		if (fault == 14)
			operation.identity.participant_index++;
		if (fault == 15)
			operation.identity.record.read_rec_ptr++;
		memset(&unchanged, 0x93, sizeof(unchanged));
		target = unchanged;
		UT_ASSERT_EQ(rf_side_online_plan_abort_target_v1(plan, &operation, &target),
					 fault == 0 && !history);
		if (fault != 0 || history)
			UT_ASSERT_EQ(memcmp(&target, &unchanged, sizeof(target)), 0);
		target = unchanged;
		UT_ASSERT_EQ(rf_side_online_plan_abort_identity_v1(plan, &operation, &target), fault == 0);
		if (fault == 0) {
			UT_ASSERT(target.instance == 3 && target.segment_id == 513
					  && target.expected_generation == 0 && target.slot_offset == 4
					  && target.wrap == 7 && target.xid == 802
					  && target.format_version == CLUSTER_UNDO_TT_ABORT_EXACT_VERSION);
		} else
			UT_ASSERT_EQ(memcmp(&target, &unchanged, sizeof(target)), 0);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_abort_requires_exact_preceding_immutable_carrier)
{
	side_abort_carrier_identity(false);
}

UT_TEST(test_cold_history_abort_identity_does_not_authorize_replay)
{
	side_abort_carrier_identity(true);
}

UT_TEST(test_cold_owner_abort_requires_durable_tt_and_native_clog_before_projection)
{
	for (int fault = 0; fault < 12; fault++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(400);
		ClusterRecoverySerialGuard guard = { 0 };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 *owner = calloc(1, sizeof(*owner));
		RfSideOnlineOperationV1 operation;
		FakeXactRecord fake;
		PGAlignedBlock initial;
		volatile bool caught = false;
		RfPageProofDetailV1 result = RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;

		guard.mode = (fault == 7 || fault == 8) ? CLUSTER_RECOVERY_SERIAL_ONLINE
												: CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
		undo_header_bind(&fake, 0);
		undo_header_feed(plan, &fake, 100);
		undo_header_abort(&fake, 4);
		undo_header_feed(plan, &fake, 200);
		make_abort(&fake, 802);
		if (fault == 8) {
			xl_xact_xinfo xinfo;

			fake.u.decoded.max_block_id = -1;
			memcpy(&xinfo, fake.data + sizeof(xl_xact_abort), sizeof(xinfo));
			xinfo.xinfo |= XACT_XINFO_HAS_AE_LOCKS;
			memcpy(fake.data + sizeof(xl_xact_abort), &xinfo, sizeof(xinfo));
		}
		undo_header_feed(plan, &fake, 300);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 2, &operation));
		UT_ASSERT_EQ(rf_side_xact_target_preflight_owned_v1(&operation.xact, NULL, 0),
					 RF_SIDE_XACT_APPLY_BLOCKED);
		reset_prepare_apply();
		canonical_file = tmpfile();
		UT_ASSERT(canonical_file != NULL && owner != NULL);
		UT_ASSERT_EQ(ftruncate(fileno(canonical_file), UNDO_SEGMENT_SIZE_BYTES), 0);
		cluster_undo_segment_make_header_bytes(513, 3, initial.data);
		UT_ASSERT(pwrite(fileno(canonical_file), initial.data, BLCKSZ, 0) == BLCKSZ);
		UT_ASSERT_EQ(fsync(fileno(canonical_file)), 0);
		canonical_writes = canonical_syncs = 0;
		canonical_expected_xid = 802;
		canonical_expected_wrap = 7;
		cold_clog_test = cold_abort_test = true;
		cold_clog_denied = fault == 1;
		cold_clog_apply_fail = fault == 2;
		cold_clog_verify_fail = fault == 3;
		cold_clog_throw = fault == 4;
		cold_clog_stale_after_verify = fault == 5;
		canonical_file_io_error = fault == 6;
		cold_abort_projection_conflict = fault == 9;
		cold_abort_projection_bad_read = fault == 10;
		cold_clog_applies = cold_clog_verifies = cold_clog_verified_order = 0;
		UT_ASSERT(rf_side_online_production_owner_init_v1(owner, &authority,
														  canonical_authority_fresh, 19, true));
		UT_ASSERT(rf_side_online_production_bind_undo_v1(owner, &authority));
		if (fault == 11) {
			UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, owner), RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_EQ(canonical_writes, 1);
			cold_clog_applies = cold_clog_verifies = cold_clog_verified_order = 0;
			reset_prepare_apply();
		}
		PG_TRY();
		{
			result = rf_side_online_production_apply_v1(plan, owner);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 4);
		if (!caught)
			UT_ASSERT_EQ(result, fault == 0 || fault == 11 ? RF_PAGE_PROOF_DETAIL_OK
														   : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
		UT_ASSERT_EQ(cold_clog_applies, fault == 1 || (fault >= 6 && fault <= 8) ? 0 : 1);
		UT_ASSERT_EQ(cold_clog_verifies,
					 fault == 0 || fault == 3 || fault == 5 || fault >= 9 ? 1 : 0);
		UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, fault == 0 || fault >= 9 ? 1 : 0);
		if (fault == 0 || fault == 10 || fault == 11)
			UT_ASSERT(cold_clog_verified_order < prepare_apply.terminal_projection_order);
		if (fault == 1 || fault == 7 || fault == 8)
			UT_ASSERT_EQ(canonical_writes, 0);
		if (fault == 11)
			UT_ASSERT_EQ(canonical_writes, 1);
		UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0
				  && owner->undo_headers == NULL && !owner->protected_set_active);
		UT_ASSERT_EQ(owner->protected_set_complete, fault == 0 || fault == 11);
		free(owner);
		UT_ASSERT_EQ(fclose(canonical_file), 0);
		canonical_file = NULL;
		canonical_file_io_error = false;
		cold_clog_test = cold_abort_test = cold_clog_denied = cold_clog_apply_fail
			= cold_clog_verify_fail = false;
		cold_clog_throw = cold_clog_stale_after_verify = cold_abort_projection_conflict
			= cold_abort_projection_bad_read = false;
		canonical_expected_xid = 818;
		canonical_expected_wrap = 8;
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_online_undo_header_evolves_init_bind_and_folded_commit)
{
	RfSideOnlinePlanV1 *plan = space_online_plan(500);
	FakeXactRecord fake;
	RfSideUndoHeaderImageV1 image, repeated;
	PGAlignedBlock base;
	struct {
		xl_cluster_undo_segment_init record;
		uint8 page[BLCKSZ];
	} init = { 0 };

	init.record.instance = 3;
	init.record.segment_id = 513;
	cluster_undo_segment_make_header_bytes(513, 3, (char *)init.page);
	make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_INIT, &init, sizeof(init));
	undo_header_feed(plan, &fake, 100);
	undo_header_bind(&fake, 0);
	undo_header_feed(plan, &fake, 200);
	make_undo_delta(&fake);
	undo_header_feed(plan, &fake, 300);
	undo_header_commit(&fake, 0, 802);
	undo_header_feed(plan, &fake, 400);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	assert_contribution_owners(
		plan, 0, RF_SIDE_CONTRIBUTION_UNDO_HEADER | RF_SIDE_CONTRIBUTION_UNDO_BLOCK, 0);
	assert_contribution_owners(plan, 1, RF_SIDE_CONTRIBUTION_UNDO_HEADER, 0);
	assert_contribution_owners(plan, 2, RF_SIDE_CONTRIBUTION_UNDO_BLOCK, 0);
	assert_contribution_owners(plan, 3,
							   RF_SIDE_CONTRIBUTION_UNDO_HEADER | RF_SIDE_CONTRIBUTION_TERMINAL, 0);
	memset(&image, 0x7a, sizeof(image));
	UT_ASSERT_EQ(rf_side_online_plan_prepare_undo_header_v1(plan, 3, 513, NULL, &image),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(image.operation_count, 3);
	UT_ASSERT_EQ(image.source_index, 3);
	UT_ASSERT_EQ(((UndoSegmentHeaderData *)image.page.data)->tt_slots[4].status, TT_SLOT_COMMITTED);
	UT_ASSERT_EQ(((UndoSegmentHeaderData *)image.page.data)->tt_slots[4].commit_scn, 999);
	base = image.page;
	UT_ASSERT_EQ(rf_side_online_plan_prepare_undo_header_v1(plan, 3, 513, base.data, &repeated),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(memcmp(repeated.page.data, image.page.data, BLCKSZ) == 0);
	UT_ASSERT(memcmp(base.data, image.page.data, BLCKSZ) == 0);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_online_undo_header_private_base_and_late_failure)
{
	for (int fault = 0; fault < 3; fault++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(300);
		FakeXactRecord fake;
		PGAlignedBlock base, saved;
		RfSideUndoHeaderImageV1 image, unchanged;

		cluster_undo_segment_make_header_bytes(513, 3, base.data);
		saved = base;
		undo_header_bind(&fake, 0);
		undo_header_feed(plan, &fake, 100);
		undo_header_commit(&fake, fault == 1 ? 1 : 0, fault == 2 ? 803 : 802);
		undo_header_feed(plan, &fake, 200);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		memset(&unchanged, 0xa9, sizeof(unchanged));
		image = unchanged;
		UT_ASSERT_EQ(rf_side_online_plan_prepare_undo_header_v1(plan, 3, 513, base.data, &image),
					 fault == 0 ? RF_PAGE_PROOF_DETAIL_OK : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
		UT_ASSERT(memcmp(base.data, saved.data, BLCKSZ) == 0);
		if (fault != 0)
			UT_ASSERT(memcmp(&image, &unchanged, sizeof(image)) == 0);
		else {
			UT_ASSERT_EQ(image.operation_count, 2);
			UT_ASSERT_EQ(image.source_index, 1);
		}
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_lifecycle_header_requires_known_generation_and_complete_source_chain)
{
	for (int scenario = 0; scenario < 5; scenario++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(500);
		FakeXactRecord fake;
		PGAlignedBlock base;
		RfSideUndoHeaderImageV1 image, saved;
		struct {
			xl_undo_segment_reuse record;
			uint8 page[BLCKSZ];
		} reuse = { 0 };

		cluster_undo_segment_make_header_bytes(513, 3, base.data);
		reuse.record.instance = 3;
		reuse.record.segment_id = 513;
		reuse.record.new_generation = 1;
		cluster_undo_segment_make_header_bytes(513, 3, (char *)reuse.page);
		((UndoSegmentHeaderData *)reuse.page)->wrap_count = 1;
		undo_header_bind(&fake, 0);
		undo_header_feed(plan, &fake, 100);
		undo_header_commit(&fake, 0, scenario == 3 ? 803 : 802);
		undo_header_feed(plan, &fake, 200);
		make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_REUSE, &reuse,
							   sizeof(reuse));
		undo_header_feed(plan, &fake, 300);
		undo_header_bind(&fake, 1);
		undo_header_feed(plan, &fake, 400);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		if (scenario != 0) {
			memcpy(base.data, reuse.page, BLCKSZ);
			if (scenario == 1 || scenario == 3) {
				TTSlot *slot = &((UndoSegmentHeaderData *)base.data)->tt_slots[4];
				slot->xid = 802;
				slot->wrap = 7;
				slot->status = TT_SLOT_ACTIVE;
				slot->first_undo_block = (UBA)InvalidUba_init;
			}
			if (scenario == 2)
				((UndoSegmentHeaderData *)base.data)->wrap_count = 2;
		}
		memset(&saved, 0x8c, sizeof(saved));
		image = saved;
		UT_ASSERT_EQ(rf_side_online_plan_prepare_undo_header_v1(
						 plan, 3, 513, scenario == 4 ? NULL : base.data, &image),
					 scenario == 2 || scenario == 3 ? RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE
													: RF_PAGE_PROOF_DETAIL_OK);
		if (scenario == 2 || scenario == 3)
			UT_ASSERT(memcmp(&image, &saved, sizeof(image)) == 0);
		else {
			UT_ASSERT_EQ(((UndoSegmentHeaderData *)image.page.data)->wrap_count, 1);
			UT_ASSERT_EQ(((UndoSegmentHeaderData *)image.page.data)->tt_slots[4].status,
						 TT_SLOT_ACTIVE);
			UT_ASSERT_EQ(image.operation_count, 4);
		}
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_recycle_window_repeated_terminal_and_init_unknown_generation)
{
	for (int init = 0; init < 2; init++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(init ? 200 : 400);
		FakeXactRecord fake;
		PGAlignedBlock base;
		RfSideUndoHeaderImageV1 image, saved;

		cluster_undo_segment_make_header_bytes(513, 3, base.data);
		if (init) {
			struct {
				xl_cluster_undo_segment_init record;
				uint8 page[BLCKSZ];
			} record = { 0 };
			record.record.instance = 3;
			record.record.segment_id = 513;
			memcpy(record.page, base.data, BLCKSZ);
			make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_INIT, &record,
								   sizeof(record));
			undo_header_feed(plan, &fake, 100);
			((UndoSegmentHeaderData *)base.data)->wrap_count = 1;
		} else {
			xl_undo_segment_recycle record = { 0 };
			TTSlot *slot = &((UndoSegmentHeaderData *)base.data)->tt_slots[4];
			record.instance = 3;
			record.segment_id = 513;
			record.old_state = SEGMENT_COMMITTED;
			record.new_state = SEGMENT_RECYCLABLE;
			undo_header_bind(&fake, 0);
			undo_header_feed(plan, &fake, 100);
			undo_header_commit(&fake, 0, 802);
			undo_header_feed(plan, &fake, 200);
			make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_RECYCLE, &record,
								   sizeof(record));
			undo_header_feed(plan, &fake, 300);
			slot->xid = 802;
			slot->wrap = 7;
			slot->status = TT_SLOT_COMMITTED;
			slot->commit_scn = 999;
			slot->first_undo_block = (UBA)InvalidUba_init;
		}
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		memset(&saved, 0x1a, sizeof(saved));
		image = saved;
		UT_ASSERT_EQ(rf_side_online_plan_prepare_undo_header_v1(plan, 3, 513, base.data, &image),
					 init ? RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE : RF_PAGE_PROOF_DETAIL_OK);
		if (init)
			UT_ASSERT(memcmp(&image, &saved, sizeof(image)) == 0);
		else
			UT_ASSERT_EQ(((UndoSegmentHeaderData *)image.page.data)->segment_state,
						 SEGMENT_RECYCLABLE);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

static RfSideOnlinePlanV1 *
undo_repeated_slot_plan_sources(bool wrong_second_xid, bool with_peer)
{
	RfSideOnlinePlanV1 *plan = NULL;
	FakeXactRecord fake;
	xl_undo_tt_slot_bind bind;
	xl_xact_tt_commit delta;
	uint32 offset;

	if (with_peer) {
		RfContributorStreamCutV1 cuts[2] = { { 0 } };
		RfSideOnlinePlanRequestV1 request = { 0 };

		for (uint32 i = 0; i < 2; i++) {
			cuts[i].failed_thread = i + 3;
			cuts[i].timeline_id = 7;
			cuts[i].flags = RF_CONTRIBUTOR_CUT_COMPLETE;
			cuts[i].scan_begin_inclusive = 100;
			cuts[i].scan_end_exclusive = i == 0 ? 500 : 200;
		}
		request.system_identifier = UINT64_C(0x11223344);
		memset(request.storage_uuid, 0x44, 16);
		request.physical_cuts = cuts;
		request.participant_count = 2;
		UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	} else
		plan = space_online_plan(500);
	undo_header_bind(&fake, 0);
	undo_header_feed(plan, &fake, 100);
	undo_header_commit(&fake, 0, 802);
	undo_header_feed(plan, &fake, 200);
	undo_header_bind(&fake, 0);
	memcpy(&bind, fake.data, sizeof(bind));
	bind.wrap = 8;
	bind.xid = 818;
	memcpy(fake.data, &bind, sizeof(bind));
	undo_header_feed(plan, &fake, 300);
	undo_header_commit(&fake, 0, wrong_second_xid ? 834 : 818);
	offset = fake.u.decoded.main_data_len - sizeof(delta);
	memcpy(&delta, fake.data + offset, sizeof(delta));
	delta.wrap = 8;
	memcpy(fake.data + offset, &delta, sizeof(delta));
	undo_header_feed(plan, &fake, 400);
	if (with_peer) {
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 record;
		uint8 uuid[16];
		int page = 17;

		memset(uuid, 0x44, 16);
		make_projection_record(&fake, RM_MULTIXACT_ID, XLOG_MULTIXACT_ZERO_OFF_PAGE, &page,
							   sizeof(page));
		identity = make_identity(&fake, uuid);
		identity.participant_index = 1;
		identity.record.origin_thread = 4;
		record = make_projection_record_plan(&fake);
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
					 RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	}
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	return plan;
}

static RfSideOnlinePlanV1 *
undo_repeated_slot_plan(bool wrong_second_xid)
{
	return undo_repeated_slot_plan_sources(wrong_second_xid, false);
}

UT_TEST(test_undo_header_repeated_slot_exact_source_membership_without_init)
{
	RfSideOnlinePlanV1 *plan = undo_repeated_slot_plan(false);
	static const UBA invalid_head = InvalidUba_init;
	RfSideOnlineOperationV1 commit;

	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &commit));
	UT_ASSERT(rf_side_online_plan_contains_commit_v1(plan, &commit.xact));
	commit.xact.terminal_timestamp++;
	UT_ASSERT(!rf_side_online_plan_contains_commit_v1(plan, &commit.xact));

	for (int state = 0; state < 9; state++) {
		PGAlignedBlock base, saved, expected;
		RfSideUndoHeaderImageV1 out, unchanged;
		UndoSegmentHeaderData *header;
		TTSlot *slot;

		cluster_undo_segment_make_header_bytes(513, 3, base.data);
		header = (UndoSegmentHeaderData *)base.data;
		/* Untouched native metadata and slots must survive private replay. */
		header->last_used_at = 4321;
		header->tt_slots[5].xid = 777;
		slot = &header->tt_slots[4];
		if (state != 0) {
			slot->xid = state < 3 ? 802 : 818;
			slot->wrap = state < 3 ? 7 : 8;
			slot->status = state == 1 || state == 3 ? TT_SLOT_ACTIVE : TT_SLOT_COMMITTED;
			slot->commit_scn = slot->status == TT_SLOT_COMMITTED ? 999 : InvalidScn;
			slot->first_undo_block = invalid_head;
		}
		if (state == 5)
			slot->xid = 834;
		if (state == 6)
			slot->wrap = 9;
		if (state == 7)
			slot->commit_scn = 1000;
		if (state == 8)
			header->wrap_count = 1;
		saved = base;
		expected = base;
		slot = &((UndoSegmentHeaderData *)expected.data)->tt_slots[4];
		memset(slot, 0, sizeof(*slot));
		slot->xid = 818;
		slot->wrap = 8;
		slot->status = TT_SLOT_COMMITTED;
		slot->commit_scn = 999;
		slot->first_undo_block = invalid_head;
		if (state < 4)
			UT_ASSERT(cluster_undo_segment_header_seal(expected.data));
		memset(&unchanged, 0xc5, sizeof(unchanged));
		out = unchanged;
		UT_ASSERT_EQ(rf_side_online_plan_prepare_undo_header_v1(plan, 3, 513, base.data, &out),
					 state < 5 ? RF_PAGE_PROOF_DETAIL_OK : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
		UT_ASSERT(memcmp(base.data, saved.data, BLCKSZ) == 0);
		if (state < 5) {
			UT_ASSERT(memcmp(out.page.data, expected.data, BLCKSZ) == 0);
			UT_ASSERT_EQ(out.operation_count, 4);
			UT_ASSERT_EQ(out.source_index, state == 4 ? UINT32_MAX : 3);
		} else
			UT_ASSERT(memcmp(&out, &unchanged, sizeof(out)) == 0);
	}
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_undo_header_later_disk_slot_does_not_hide_conflicting_source)
{
	RfSideOnlinePlanV1 *plan = undo_repeated_slot_plan(true);
	PGAlignedBlock base;
	RfSideUndoHeaderImageV1 out, saved;
	TTSlot *slot;

	cluster_undo_segment_make_header_bytes(513, 3, base.data);
	slot = &((UndoSegmentHeaderData *)base.data)->tt_slots[4];
	slot->xid = 818;
	slot->wrap = 8;
	slot->status = TT_SLOT_COMMITTED;
	slot->commit_scn = 999;
	memset(&saved, 0x7d, sizeof(saved));
	out = saved;
	UT_ASSERT_EQ(rf_side_online_plan_prepare_undo_header_v1(plan, 3, 513, base.data, &out),
				 RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
	UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	rf_side_online_plan_destroy_v1(&plan);
}

static bool
canonical_authority_fresh(void *arg)
{
	return arg != NULL;
}

UT_TEST(test_cold_owner_multixact_preflights_whole_set_before_projection)
{
	for (int fault = 0; fault < 6; fault++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(fault == 5 ? 300 : 200);
		RfSideOnlineProductionOwnerV1 owner;
		ClusterRecoverySerialGuard guard = { 0 };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfPageProofDetailV1 result = RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
		volatile bool caught = false;
		guard.mode
			= fault == 4 ? CLUSTER_RECOVERY_SERIAL_ONLINE : CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
		for (int n = 0; n < (fault == 5 ? 2 : 1); n++) {
			FakeXactRecord fake;
			uint8 uuid[16];
			RfPageOnlineRecordIdentityV1 identity;
			RfDetachedRecordPlanV1 record;
			int page = n == 1 ? 524288 : 0;
			make_projection_record(&fake, RM_MULTIXACT_ID, XLOG_MULTIXACT_ZERO_OFF_PAGE, &page,
								   sizeof(page));
			memset(uuid, 0x44, sizeof(uuid));
			identity = make_identity(&fake, uuid);
			set_identity_range(&fake, &identity, 100 + n * 100, 200 + n * 100);
			record = make_projection_record_plan(&fake);
			UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
						 RF_PAGE_PROOF_DETAIL_OK);
		}
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		cold_mx_test = control_scope_test = true;
		cold_mx_online = fault == 4;
		control_scope_denied = false;
		control_scope_fault = 0;
		cold_clog_denied = fault == 1;
		cold_mx_preflight_fail = fault == 2;
		cold_mx_throw = fault == 3;
		cold_mx_preflights = cold_mx_projections = 0;
		cold_source_calls = 0;
		UT_ASSERT(rf_side_online_production_owner_init_v1(&owner, &authority,
														  canonical_authority_fresh, 19, true));
		UT_ASSERT(rf_side_online_production_bind_undo_v1(&owner, &authority));
		PG_TRY();
		{
			result = rf_side_online_production_apply_v1(plan, &owner);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 3);
		if (!caught)
			UT_ASSERT_EQ(result, fault == 0 || fault == 4 ? RF_PAGE_PROOF_DETAIL_OK
														  : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
		UT_ASSERT_EQ(cold_mx_projections, fault == 0 || fault == 4 ? 1 : 0);
		/* Rejected full-set preflight and ONLINE never reach the cold
		 * barrier, including a valid first record followed by an invalid one. */
		UT_ASSERT_EQ(cold_source_calls, fault == 0 ? 1 : 0);
		UT_ASSERT_EQ(owner.protected_set_complete, fault == 0 || fault == 4);
		UT_ASSERT(canonical_scope == NULL && !owner.protected_set_active);
		cold_mx_test = cold_mx_online = cold_mx_preflight_fail = cold_mx_throw = false;
		cold_clog_denied = control_scope_test = false;
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_owner_native_clog_lifecycle_precedes_derived_projection)
{
	for (int fault = 0; fault < 11; fault++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(fault == 10 ? 300 : 200);
		RfSideOnlineProductionOwnerV1 owner;
		ClusterRecoverySerialGuard guard = { 0 };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfPageProofDetailV1 result = RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
		volatile bool caught = false;
		guard.mode
			= fault == 7 ? CLUSTER_RECOVERY_SERIAL_ONLINE : CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
		for (int n = 0; n < (fault == 10 ? 2 : 1); n++) {
			FakeXactRecord fake;
			uint8 uuid[16];
			RfPageOnlineRecordIdentityV1 identity;
			RfDetachedRecordPlanV1 record;
			int page = fault == 8 || n == 1 ? 131072 : 97;
			xl_clog_truncate trunc = { 97, 97 * BLCKSZ * 4 + 1, 5 };
			if (fault == 9)
				trunc.pageno++;
			if (fault == 1 || fault == 9)
				make_projection_record(&fake, RM_CLOG_ID, CLOG_TRUNCATE, &trunc, sizeof(trunc));
			else
				make_projection_record(&fake, RM_CLOG_ID, CLOG_ZEROPAGE, &page, sizeof(page));
			memset(uuid, 0x44, sizeof(uuid));
			identity = make_identity(&fake, uuid);
			set_identity_range(&fake, &identity, 100 + n * 100, 200 + n * 100);
			record = make_projection_record_plan(&fake);
			UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
						 RF_PAGE_PROOF_DETAIL_OK);
		}
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		cold_clog_test = cold_clog_record_test = true;
		control_scope_test = true;
		control_scope_denied = false;
		control_scope_fault = 0;
		cold_clog_record_online = fault == 7;
		cold_clog_denied = fault == 2;
		cold_clog_apply_fail = fault == 3;
		cold_clog_verify_fail = fault == 4;
		cold_clog_throw = fault == 5;
		cold_clog_stale_after_verify = fault == 6;
		cold_clog_applies = cold_clog_verifies = cold_clog_record_projections = 0;
		UT_ASSERT(rf_side_online_production_owner_init_v1(&owner, &authority,
														  canonical_authority_fresh, 19, true));
		UT_ASSERT(rf_side_online_production_bind_undo_v1(&owner, &authority));
		PG_TRY();
		{
			result = rf_side_online_production_apply_v1(plan, &owner);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 5);
		if (!caught)
			UT_ASSERT_EQ(result, fault <= 1 || fault == 7 ? RF_PAGE_PROOF_DETAIL_OK
														  : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
		UT_ASSERT_EQ(cold_clog_applies, fault == 2 || fault >= 7 ? 0 : 1);
		UT_ASSERT_EQ(cold_clog_verifies, fault <= 1 || fault == 4 || fault == 6 ? 1 : 0);
		UT_ASSERT_EQ(cold_clog_record_projections, fault <= 1 || fault == 7 ? 1 : 0);
		UT_ASSERT_EQ(owner.protected_set_complete, fault <= 1 || fault == 7);
		UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0
				  && !owner.protected_set_active);
		cold_clog_record_test = cold_clog_record_online = cold_clog_test = cold_clog_denied = false;
		control_scope_test = false;
		cold_clog_apply_fail = cold_clog_verify_fail = cold_clog_throw
			= cold_clog_stale_after_verify = false;
		rf_side_online_plan_destroy_v1(&plan);
	}
}

static void
cold_cache_commit(FakeXactRecord *fake, bool initfile)
{
	xl_xact_dbinfo db = { .dbId = 5, .tsId = 1663 };
	SharedInvalidationMessage msgs[2] = { 0 };
	uint32 prefix = MinSizeOfXactCommit + sizeof(xl_xact_xinfo);
	uint32 added = sizeof(db) + sizeof(int) + sizeof(msgs);
	uint32 xinfo;
	int count = 2;

	undo_header_commit(fake, 0, 802);
	msgs[0].rc.id = SHAREDINVALRELCACHE_ID;
	msgs[0].rc.dbId = 5;
	msgs[0].rc.relId = 16388;
	msgs[1].cat.id = SHAREDINVALCATALOG_ID;
	msgs[1].cat.dbId = InvalidOid;
	msgs[1].cat.catId = 1260;
	memmove(fake->data + prefix + added, fake->data + prefix,
			fake->u.decoded.main_data_len - prefix);
	memcpy(fake->data + prefix, &db, sizeof(db));
	memcpy(fake->data + prefix + sizeof(db), &count, sizeof(count));
	memcpy(fake->data + prefix + sizeof(db) + sizeof(count), msgs, sizeof(msgs));
	memcpy(&xinfo, fake->data + MinSizeOfXactCommit, sizeof(xinfo));
	xinfo |= XACT_XINFO_HAS_DBINFO | XACT_XINFO_HAS_INVALS;
	if (initfile)
		xinfo |= XACT_COMPLETION_UPDATE_RELCACHE_FILE;
	memcpy(fake->data + MinSizeOfXactCommit, &xinfo, sizeof(xinfo));
	fake->u.decoded.main_data_len += added;
	fake->u.decoded.max_block_id = -1;
}

static void
cold_completion_commit(FakeXactRecord *fake, bool cache, bool stats, bool force)
{
	xl_xact_stats_item items[] = { { PGSTAT_KIND_RELATION, 5, 16388 },
								   { PGSTAT_KIND_FUNCTION, 5, 16390 },
								   { PGSTAT_KIND_RELATION, InvalidOid, 1260 } };
	uint32 offset = MinSizeOfXactCommit + sizeof(xl_xact_xinfo);
	uint32 xinfo;
	int count = lengthof(items);

	if (cache) {
		cold_cache_commit(fake, false);
		offset += sizeof(xl_xact_dbinfo);
	} else {
		undo_header_commit(fake, 0, 802);
		fake->u.decoded.max_block_id = -1;
	}
	memcpy(&xinfo, fake->data + MinSizeOfXactCommit, sizeof(xinfo));
	if (stats) {
		uint32 added = sizeof(count) + sizeof(items);
		memmove(fake->data + offset + added, fake->data + offset,
				fake->u.decoded.main_data_len - offset);
		memcpy(fake->data + offset, &count, sizeof(count));
		memcpy(fake->data + offset + sizeof(count), items, sizeof(items));
		fake->u.decoded.main_data_len += added;
		xinfo |= XACT_XINFO_HAS_DROPPED_STATS;
	}
	if (force)
		xinfo |= XACT_COMPLETION_FORCE_SYNC_COMMIT;
	memcpy(fake->data + MinSizeOfXactCommit, &xinfo, sizeof(xinfo));
}

static void
cold_completion_abort(FakeXactRecord *fake)
{
	xl_xact_stats_item items[] = { { PGSTAT_KIND_RELATION, 5, 16388 },
								   { PGSTAT_KIND_FUNCTION, 5, 16390 },
								   { PGSTAT_KIND_RELATION, InvalidOid, 1260 } };
	xl_xact_dbinfo db = { .dbId = 5, .tsId = 1663 };
	uint32 offset = MinSizeOfXactAbort + sizeof(xl_xact_xinfo) + sizeof(db);
	uint32 added = sizeof(int) + sizeof(items), xinfo;
	int count = lengthof(items);

	make_abort(fake, 802);
	memcpy(fake->data + MinSizeOfXactAbort + sizeof(xl_xact_xinfo), &db, sizeof(db));
	memmove(fake->data + offset + added, fake->data + offset,
			fake->u.decoded.main_data_len - offset);
	memcpy(fake->data + offset, &count, sizeof(count));
	memcpy(fake->data + offset + sizeof(count), items, sizeof(items));
	memcpy(&xinfo, fake->data + MinSizeOfXactAbort, sizeof(xinfo));
	xinfo |= XACT_XINFO_HAS_DROPPED_STATS;
	memcpy(fake->data + MinSizeOfXactAbort, &xinfo, sizeof(xinfo));
	fake->u.decoded.main_data_len += added;
	fake->u.decoded.max_block_id = -1;
}

static void
cold_abort_deletes(FakeXactRecord *fake, const RelFileLocator *locators, int count, bool stats)
{
	xl_xact_dbinfo db = { .dbId = 5, .tsId = DEFAULTTABLESPACE_OID };
	uint32 offset = MinSizeOfXactAbort + sizeof(xl_xact_xinfo) + sizeof(db);
	uint32 added = sizeof(count) + count * sizeof(*locators), xinfo;

	if (stats)
		cold_completion_abort(fake);
	else
		make_abort(fake, 802);
	memcpy(fake->data + MinSizeOfXactAbort + sizeof(xl_xact_xinfo), &db, sizeof(db));
	UT_ASSERT(fake->u.decoded.main_data_len + added <= sizeof(fake->data));
	memmove(fake->data + offset + added, fake->data + offset,
			fake->u.decoded.main_data_len - offset);
	memcpy(fake->data + offset, &count, sizeof(count));
	memcpy(fake->data + offset + sizeof(count), locators, count * sizeof(*locators));
	memcpy(&xinfo, fake->data + MinSizeOfXactAbort, sizeof(xinfo));
	xinfo |= XACT_XINFO_HAS_RELFILELOCATORS;
	memcpy(fake->data + MinSizeOfXactAbort, &xinfo, sizeof(xinfo));
	fake->u.decoded.main_data_len += added;
	fake->u.decoded.header.xl_info |= XLR_SPECIAL_REL_UPDATE;
	fake->u.decoded.max_block_id = -1;
}

static RfPageProofDetailV1
cold_abort_delete_feed(RfSideOnlinePlanV1 *plan, FakeXactRecord *fake, XLogRecPtr begin,
					   uint32 participant)
{
	uint8 uuid[16];
	RfPageOnlineRecordIdentityV1 identity;
	RfDetachedRecordPlanV1 record;

	memset(uuid, 0x44, sizeof(uuid));
	identity = make_identity(fake, uuid);
	identity.participant_index = participant;
	identity.record.origin_thread += participant;
	set_identity_range(fake, &identity, begin, begin + 100);
	record = fake->u.decoded.header.xl_rmid == RM_XACT_ID ? make_record_plan(fake)
														  : make_projection_record_plan(fake);
	return rf_side_online_plan_feed_record_v1(plan, &record, &identity);
}

static void
cold_abort_create_record(FakeXactRecord *fake, RelFileLocator locator, bool native,
						 TransactionId xid)
{
	ClusterSpaceReservationChange advance = space_advance_fixture();
	ClusterSpaceStructureChange create = { 0 };
	xl_smgr_create smgr = { locator, MAIN_FORKNUM };
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];

	if (native)
		make_projection_record(fake, RM_SMGR_ID, XLOG_SMGR_CREATE | XLR_SPECIAL_REL_UPDATE, &smgr,
							   sizeof(smgr));
	else {
		advance.before.identity.key.locator = locator;
		advance.before.next_block = 0;
		create.identity.action = CLUSTER_SPACE_WAL_CREATE;
		create.identity.nblocks = InvalidBlockNumber;
		create.identity.result = advance.before.identity;
		create.identity.result_token = advance.before_token;
		create.reservation.action = CLUSTER_SPACE_RESERVATION_INIT;
		create.reservation.result = advance.before;
		create.reservation.result_token = advance.before_token;
		UT_ASSERT(cluster_space_structure_wal_encode(&create, wal, sizeof(wal)));
		make_projection_record(fake, RM_SMGR_ID, XLOG_SMGR_SPACE_IDENTITY | XLR_SPECIAL_REL_UPDATE,
							   wal, sizeof(wal));
	}
	fake->u.decoded.header.xl_xid = xid;
	fake->u.decoded.max_block_id = -1;
}

static void
cold_lock_completion(FakeXactRecord *fake, bool abort, bool combined)
{
	uint32 offset = abort ? MinSizeOfXactAbort : MinSizeOfXactCommit;
	uint32 xinfo;

	if (abort) {
		if (combined)
			cold_completion_abort(fake);
		else
			make_abort(fake, 802);
	} else
		cold_completion_commit(fake, combined, combined, combined);
	memcpy(&xinfo, fake->data + offset, sizeof(xinfo));
	xinfo |= XACT_XINFO_HAS_AE_LOCKS;
	memcpy(fake->data + offset, &xinfo, sizeof(xinfo));
	fake->u.decoded.max_block_id = -1;
}

/* Preserve the native array order: DBINFO, locators, stats, messages,
 * SCN/TT and finally the typed structural effects. */
static void
cold_drop_commit(FakeXactRecord *fake, const ClusterSpaceStructureChange *drops, uint32 count,
				 int shape)
{
	uint32 prefix = MinSizeOfXactCommit + sizeof(xl_xact_xinfo), xinfo;
	uint32 added = sizeof(int) + count * sizeof(RelFileLocator);
	bool cache = shape == 1 || shape == 4;
	int nrels = count;

	cold_completion_commit(fake, cache, shape == 2 || shape == 4, shape == 3 || shape == 4);
	if (cache)
		prefix += sizeof(xl_xact_dbinfo);
	UT_ASSERT(fake->u.decoded.main_data_len + added + sizeof(count)
				  + count * CLUSTER_SPACE_STRUCTURE_WAL_BYTES
			  <= sizeof(fake->data));
	memmove(fake->data + prefix + added, fake->data + prefix,
			fake->u.decoded.main_data_len - prefix);
	memcpy(fake->data + prefix, &nrels, sizeof(nrels));
	for (uint32 i = 0; i < count; i++)
		memcpy(fake->data + prefix + sizeof(nrels) + i * sizeof(RelFileLocator),
			   &drops[i].identity.result.key.locator, sizeof(RelFileLocator));
	fake->u.decoded.main_data_len += added;
	memcpy(fake->data + fake->u.decoded.main_data_len, &count, sizeof(count));
	fake->u.decoded.main_data_len += sizeof(count);
	for (uint32 i = 0; i < count; i++) {
		UT_ASSERT(cluster_space_structure_wal_encode(&drops[i],
													 fake->data + fake->u.decoded.main_data_len,
													 CLUSTER_SPACE_STRUCTURE_WAL_BYTES));
		fake->u.decoded.main_data_len += CLUSTER_SPACE_STRUCTURE_WAL_BYTES;
	}
	memcpy(&xinfo, fake->data + MinSizeOfXactCommit, sizeof(xinfo));
	xinfo |= XACT_XINFO_HAS_RELFILELOCATORS | XACT_XINFO_HAS_SPACE_DROP;
	if (shape == 5)
		xinfo |= XACT_XINFO_HAS_AE_LOCKS;
	memcpy(fake->data + MinSizeOfXactCommit, &xinfo, sizeof(xinfo));
}

UT_TEST(test_cold_compound_drop_classifies_the_complete_native_record)
{
	ClusterSpaceStructureChange drops[2] = { space_drop_fixture(16384), space_drop_fixture(16385) };

	for (int shape = 0; shape < 6; shape++) {
		FakeXactRecord fake;
		RfSideXactOperationV1 op;
		uint8 unaligned[sizeof(fake.data) + 1];

		reset_prepare_apply();
		cold_drop_commit(&fake, drops, 2, shape);
		if (!rf_side_xact_decode_recovery_v1(&fake.reader, UINT64_C(0x11223344), 3, &op)
			|| !op.cold_completion_effects) {
			UT_ASSERT(false);
			continue;
		}
		UT_ASSERT(!op.pi_read_only && op.has_tt_delta);
		UT_ASSERT_EQ(op.relation_count, 2);
		UT_ASSERT_EQ(op.space_drop_count, 2);
		UT_ASSERT_EQ(op.invalidation_count, shape == 1 || shape == 4 ? 2 : 0);
		UT_ASSERT_EQ(op.completion_payload_length, fake.u.decoded.main_data_len);
		memcpy(unaligned + 1, fake.data, fake.u.decoded.main_data_len);
		UT_ASSERT(rf_side_xact_cold_completion_preflight_v1(&op, unaligned + 1,
															fake.u.decoded.main_data_len));
		UT_ASSERT(!rf_side_xact_cold_completion_preflight_v1(&op, unaligned + 1,
															 fake.u.decoded.main_data_len - 1));
		UT_ASSERT_EQ(rf_side_xact_apply_owned_v1(&op, fake.data, fake.u.decoded.main_data_len),
					 RF_SIDE_XACT_APPLY_BLOCKED);
		UT_ASSERT_EQ(prepare_apply.tt_stamps, 0);
		UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
	}
}

UT_TEST(test_cold_compound_drop_rejects_partial_or_unknown_effects)
{
	for (int fault = 0; fault < 13; fault++) {
		ClusterSpaceStructureChange drops[2]
			= { space_drop_fixture(16384), space_drop_fixture(16385) };
		FakeXactRecord fake;
		RfSideXactOperationV1 op, empty = { 0 };
		xl_xact_parsed_commit parsed;
		uint32 flags;

		reset_prepare_apply();
		if (fault == 0)
			drops[1] = drops[0];
		cold_drop_commit(&fake, drops, 2, 4);
		UT_ASSERT(ParseCommitRecord(XLogRecGetInfo(&fake.reader), (xl_xact_commit *)fake.data,
									fake.u.decoded.main_data_len, &parsed));
		memcpy(&flags, fake.data + MinSizeOfXactCommit, sizeof(flags));
		switch (fault) {
		case 1:
			parsed.xlocators[1].relNumber++;
			break;
		case 2:
			parsed.msgs[0].rc.dbId++;
			break;
		case 3:
			parsed.msgs[0].id = SHAREDINVALSMGR_ID;
			break;
		case 4:
			parsed.stats[0].kind = (PgStat_Kind)UINT32_MAX;
			break;
		case 5:
			fake.u.decoded.main_data_len--;
			break;
		case 6:
			fake.data[fake.u.decoded.main_data_len++] = 0;
			break;
		case 7:
			fake.u.decoded.max_block_id = 0;
			break;
		case 8:
			flags |= XACT_COMPLETION_APPLY_FEEDBACK;
			break;
		case 9:
			flags |= XACT_XINFO_HAS_ORIGIN;
			break;
		case 10:
			flags |= XACT_XINFO_HAS_SUBXACTS;
			break;
		case 11:
			flags |= XACT_XINFO_HAS_TWOPHASE;
			break;
		case 12: {
			uint32 count = UINT32_MAX;
			memcpy((char *)parsed.space_drops - sizeof(count), &count, sizeof(count));
			break;
		}
		}
		memcpy(fake.data + MinSizeOfXactCommit, &flags, sizeof(flags));
		memset(&op, 0xff, sizeof(op));
		UT_ASSERT(!rf_side_xact_decode_recovery_v1(&fake.reader, UINT64_C(0x11223344), 3, &op));
		UT_ASSERT(memcmp(&op, &empty, sizeof(op)) == 0);
	}
}

UT_TEST(test_cold_compound_drop_owns_all_effects_and_original_space_inputs)
{
	ClusterSpaceStructureChange drops[2] = { space_drop_fixture(16384), space_drop_fixture(16385) };
	FakeXactRecord fake;
	RfSideOnlinePlanV1 *plan = space_online_plan(200);
	RfSideOnlineOperationV1 op;
	uint8 saved[sizeof(fake.data)];
	uint32 len;

	reset_prepare_apply();
	cold_drop_commit(&fake, drops, 2, 4);
	len = fake.u.decoded.main_data_len;
	memcpy(saved, fake.data, len);
	if (space_commit_feed(plan, &fake, 100) != RF_PAGE_PROOF_DETAIL_OK) {
		UT_ASSERT(false);
		goto done;
	}
	memset(fake.data, 0xee, sizeof(fake.data));
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &op));
	UT_ASSERT_EQ(op.owned_payload_length, len);
	UT_ASSERT(memcmp(op.owned_payload, saved, len) == 0);
	assert_contribution_owners(
		plan, 0,
		RF_SIDE_CONTRIBUTION_SPACE | RF_SIDE_CONTRIBUTION_UNDO_HEADER
			| RF_SIDE_CONTRIBUTION_TERMINAL | RF_SIDE_CONTRIBUTION_NATIVE_STRUCTURE
			| RF_SIDE_CONTRIBUTION_INVALIDATION | RF_SIDE_CONTRIBUTION_COMPLETION,
		2);
	for (int i = 0; i < 2; i++) {
		PGAlignedBlock pages[2];
		ClusterSpaceRecoveryImage out;
		ClusterSpaceReservation result;
		RfSideSpaceContributionV1 contribution;
		uint64 token;
		uint32 order[1], count = 0;

		UT_ASSERT(rf_side_online_plan_space_contribution_v1(plan, 0, i, &contribution));
		UT_ASSERT_EQ(contribution.result.key.locator.relNumber, 16384 + i);
		UT_ASSERT(cluster_space_identity_page_encode(&drops[i].identity.expected, 123,
													 pages[0].data, BLCKSZ));
		UT_ASSERT(cluster_space_reservation_page_encode(&drops[i].reservation.before,
														drops[i].reservation.before_token,
														pages[1].data, BLCKSZ));
		UT_ASSERT_EQ(rf_side_online_plan_prepare_space_v1(plan, &drops[i].identity.result.key,
														  pages[0].data, pages[1].data, order, 1,
														  &count, &out),
					 RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(count, 1);
		UT_ASSERT(cluster_space_reservation_page_decode(out.pages[1].data, BLCKSZ, SPACE_FORKNUM, 1,
														&drops[i].identity.result.key, &result,
														&token));
		UT_ASSERT_EQ(result.identity.state, CLUSTER_SPACE_IDENTITY_TOMBSTONED);
		UT_ASSERT_EQ(token, 70);
	}
	/* A valid typed suffix cannot hide damage to another owner's native bytes. */
	{
		xl_xact_parsed_commit parsed;
		RfSideSpaceContributionV1 contribution;
		UT_ASSERT(ParseCommitRecord(op.identity.record.info, (xl_xact_commit *)op.owned_payload,
									op.owned_payload_length, &parsed));
		parsed.stats[0].objoid = InvalidOid;
		UT_ASSERT(!rf_side_online_plan_space_contribution_v1(plan, 0, 0, &contribution));
	}
done:
	rf_side_online_plan_destroy_v1(&plan);
}

static bool
drop_coverage_must_not_run(void *arg, const RfSideXactOperationV1 *operation pg_attribute_unused())
{
	(*(int *)arg)++;
	return false;
}

UT_TEST(test_cold_compound_drop_cannot_project_without_structural_consumer)
{
	ClusterSpaceStructureChange drop = space_drop_fixture(16384);
	ClusterUndoRecoveryScopeV1 scope = { 0 };
	ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
	ClusterThreadRecoveryAuthorityV1 authority
		= { .duty = &canonical_duty, .serial_guard = &guard };
	RfSideOnlineProductionOwnerV1 *owner;
	RfSideOnlinePlanV1 *plan;
	RfSideXactOperationV1 op;
	FakeXactRecord fake;
	PGAlignedBlock initial, disk;
	int coverage_calls = 0;

	reset_prepare_apply();
	cold_drop_commit(&fake, &drop, 1, 4);
	if (!rf_side_xact_decode_recovery_v1(&fake.reader, UINT64_C(0x11223344), 3, &op)
		|| !op.cold_completion_effects) {
		UT_ASSERT(false);
		return;
	}
	/* Even a current cold scope with a synced source is not the DROP consumer. */
	canonical_scope = &scope;
	cold_clog_test = cold_source_ready = true;
	UT_ASSERT_EQ(rf_side_xact_apply_cold_completion_v1(&op, fake.data, fake.u.decoded.main_data_len,
													   &coverage_calls, drop_coverage_must_not_run),
				 RF_SIDE_XACT_APPLY_BLOCKED);
	UT_ASSERT_EQ(coverage_calls, 0);
	UT_ASSERT_EQ(prepare_apply.tt_stamps, 0);
	UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
	canonical_scope = NULL;
	cold_clog_test = cold_source_ready = false;

	/* The original whole-set owner must reject before even installing TT. */
	plan = space_online_plan(300);
	undo_header_bind(&fake, 0);
	undo_header_feed(plan, &fake, 100);
	cold_drop_commit(&fake, &drop, 1, 4);
	UT_ASSERT_EQ(space_commit_feed(plan, &fake, 200), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	owner = calloc(1, sizeof(*owner));
	canonical_file = tmpfile();
	UT_ASSERT(owner != NULL && canonical_file != NULL);
	UT_ASSERT_EQ(ftruncate(fileno(canonical_file), UNDO_SEGMENT_SIZE_BYTES), 0);
	cluster_undo_segment_make_header_bytes(513, 3, initial.data);
	UT_ASSERT_EQ(pwrite(fileno(canonical_file), initial.data, BLCKSZ, 0), BLCKSZ);
	UT_ASSERT_EQ(fsync(fileno(canonical_file)), 0);
	canonical_writes = canonical_syncs = cold_clog_applies = 0;
	cold_clog_test = cold_source_ready = true;
	UT_ASSERT(rf_side_online_production_owner_init_v1(owner, &authority, canonical_authority_fresh,
													  19, true));
	UT_ASSERT(rf_side_online_production_bind_undo_v1(owner, &authority));
	UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, owner),
				 RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
	UT_ASSERT_EQ(canonical_writes, 0);
	UT_ASSERT_EQ(cold_clog_applies, 0);
	UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
	UT_ASSERT(!owner->protected_set_complete && !owner->protected_set_active);
	UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0);
	UT_ASSERT_EQ(pread(fileno(canonical_file), disk.data, BLCKSZ, 0), BLCKSZ);
	UT_ASSERT(memcmp(initial.data, disk.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(fclose(canonical_file), 0);
	canonical_file = NULL;
	cold_clog_test = cold_source_ready = false;
	free(owner);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_compound_space_observation_does_not_borrow_native_execution_state)
{
	ClusterSpaceStructureChange drop = space_drop_fixture(16384);
	RfSideOnlinePlanV1 *plan = space_online_plan(200);
	RfSideSpaceContributionV1 contribution;
	RfSideOnlineOperationV1 op;
	FakeXactRecord fake;

	reset_prepare_apply();
	cold_drop_commit(&fake, &drop, 1, 5);
	UT_ASSERT_EQ(space_commit_feed(plan, &fake, 100), RF_PAGE_PROOF_DETAIL_OK);
	/* The immutable source identity is already bound by the plan. Observation
	 * must work without borrowing this process's execution environment. */
	prepare_apply.system_identifier++;
	prepare_apply.origin_slot++;
	standbyState = (HotStandbyState)-1;
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_space_contribution_v1(plan, 0, 0, &contribution));
	if (!rf_side_online_plan_operation_v1(plan, 0, &op)) {
		UT_ASSERT(false);
		goto done;
	}
	UT_ASSERT(!rf_side_xact_cold_completion_preflight_v1(&op.xact, op.owned_payload,
														 op.owned_payload_length));
	reset_prepare_apply();
	UT_ASSERT(!rf_side_xact_cold_completion_preflight_v1(&op.xact, op.owned_payload,
														 op.owned_payload_length));
	standbyState = STANDBY_DISABLED;
	UT_ASSERT(rf_side_xact_cold_completion_preflight_v1(&op.xact, op.owned_payload,
														op.owned_payload_length));
	UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
done:
	reset_prepare_apply();
	standbyState = STANDBY_DISABLED;
	rf_side_online_plan_destroy_v1(&plan);
}

/* Insert the complete native terminal list; it records top membership, not
 * immediate SUBTRANS parents. Keep stats/SCN and every other section intact. */
static void
cold_terminal_children(FakeXactRecord *fake, bool abort)
{
	uint32 header = abort ? MinSizeOfXactAbort : MinSizeOfXactCommit;
	uint32 offset = header + sizeof(xl_xact_xinfo);
	uint32 xinfo;
	int count = lengthof(cold_child_xids);
	uint32 added = sizeof(count) + sizeof(cold_child_xids);

	memcpy(&xinfo, fake->data + header, sizeof(xinfo));
	if ((xinfo & XACT_XINFO_HAS_DBINFO) != 0)
		offset += sizeof(xl_xact_dbinfo);
	memmove(fake->data + offset + added, fake->data + offset,
			fake->u.decoded.main_data_len - offset);
	memcpy(fake->data + offset, &count, sizeof(count));
	memcpy(fake->data + offset + sizeof(count), cold_child_xids, sizeof(cold_child_xids));
	xinfo |= XACT_XINFO_HAS_SUBXACTS;
	memcpy(fake->data + header, &xinfo, sizeof(xinfo));
	fake->u.decoded.main_data_len += added;
}

static void
check_cold_terminal_children(bool abort)
{
	for (int fault = 0; fault < 12; fault++) {
		FakeXactRecord fake;
		RfSideXactOperationV1 operation, rejected;
		uint8 unaligned[1024];
		uint32 header = abort ? MinSizeOfXactAbort : MinSizeOfXactCommit;
		uint32 flags;
		uint32 offset = header + sizeof(xl_xact_xinfo);
		uint32 length;
		bool decoded;
		TransactionId bad = fault == 1	 ? FrozenTransactionId
							: fault == 2 ? 802
							: fault == 3 ? 674
							: fault == 4 ? 930
										 : 1200;

		if (abort)
			make_abort(&fake, 802);
		else
			cold_completion_commit(&fake, false, false, false);
		fake.u.decoded.max_block_id = -1;
		cold_terminal_children(&fake, abort);
		memcpy(&flags, fake.data + header, sizeof(flags));
		if ((flags & XACT_XINFO_HAS_DBINFO) != 0)
			offset += sizeof(xl_xact_dbinfo);
		length = fake.u.decoded.main_data_len;
		if (fault >= 1 && fault <= 4)
			memcpy(fake.data + offset + sizeof(int) + sizeof(TransactionId), &bad, sizeof(bad));
		else if (fault == 5) {
			int count = PG_INT32_MAX;
			memcpy(fake.data + offset, &count, sizeof(count));
		} else if (fault == 6)
			fake.u.decoded.main_data_len--;
		else if (fault == 7)
			fake.data[fake.u.decoded.main_data_len++] = 0;
		else if (fault == 8)
			fake.u.decoded.max_block_id = 0;
		else if (fault == 9) {
			memcpy(&flags, fake.data + header, sizeof(flags));
			flags |= XACT_XINFO_HAS_ORIGIN;
			memcpy(fake.data + header, &flags, sizeof(flags));
		} else if (fault == 10) {
			int count = 0;
			memcpy(fake.data + offset, &count, sizeof(count));
		} else if (fault == 11) {
			int count = -1;
			memcpy(fake.data + offset, &count, sizeof(count));
		}
		memmove(unaligned + 1, fake.data, fake.u.decoded.main_data_len);
		fake.u.decoded.main_data = (char *)unaligned + 1;
		reset_prepare_apply();
		UT_ASSERT(!rf_side_xact_decode_v1(&fake.reader, UINT64_C(0x11223344), 3, &rejected));
		UT_ASSERT(!rf_side_xact_inspect_pi_v1(&fake.reader, UINT64_C(0x11223344), 3, &rejected));
		decoded
			= rf_side_xact_decode_recovery_v1(&fake.reader, UINT64_C(0x11223344), 3, &operation);
		UT_ASSERT_EQ(decoded, fault == 0);
		if (fault == 0 && decoded) {
			UT_ASSERT(operation.cold_completion_effects && !operation.pi_read_only);
			UT_ASSERT_EQ(operation.completion_payload_length, length);
			UT_ASSERT(rf_side_xact_cold_completion_payload_v1(&operation, unaligned + 1, length));
			UT_ASSERT_EQ(rf_side_xact_apply_v1(&operation), RF_SIDE_XACT_APPLY_BLOCKED);
			UT_ASSERT(
				!rf_side_xact_cold_completion_payload_v1(&operation, unaligned + 1, length - 1));
		}
		UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
	}
}

UT_TEST(test_cold_abort_children_require_complete_supported_native_carrier)
{
	check_cold_terminal_children(true);
}

UT_TEST(test_cold_commit_children_require_complete_supported_native_carrier)
{
	check_cold_terminal_children(false);
}

UT_TEST(test_cold_lock_completion_retains_whole_native_record)
{
	for (int abort = 0; abort < 2; abort++)
		for (int combined = 0; combined < 2; combined++) {
			FakeXactRecord fake;
			RfSideXactOperationV1 op;
			uint8 unaligned[2048];

			reset_prepare_apply();
			cold_lock_completion(&fake, abort, combined);
			if (!rf_side_xact_decode_recovery_v1(&fake.reader, UINT64_C(0x11223344), 3, &op)
				|| !op.cold_completion_effects) {
				UT_ASSERT(false);
				continue;
			}
			UT_ASSERT(!op.pi_read_only && (op.xinfo & XACT_XINFO_HAS_AE_LOCKS) != 0);
			UT_ASSERT_EQ(op.completion_payload_length, fake.u.decoded.main_data_len);
			memcpy(unaligned + 1, fake.data, fake.u.decoded.main_data_len);
			UT_ASSERT(rf_side_xact_cold_completion_preflight_v1(&op, unaligned + 1,
																fake.u.decoded.main_data_len));
			UT_ASSERT(!rf_side_xact_cold_completion_preflight_v1(&op, unaligned + 1,
																 fake.u.decoded.main_data_len - 1));
			UT_ASSERT_EQ(rf_side_xact_apply_owned_v1(&op, fake.data, fake.u.decoded.main_data_len),
						 RF_SIDE_XACT_APPLY_BLOCKED);
			for (int bad = 0; bad < 3; bad++) {
				RfSideXactOperationV1 changed = op;
				if (bad == 0)
					changed.origin_thread++;
				else if (bad == 1)
					changed.xinfo &= ~XACT_XINFO_HAS_AE_LOCKS;
				else
					changed.pi_read_only = true;
				UT_ASSERT(!rf_side_xact_cold_completion_preflight_v1(&changed, unaligned + 1,
																	 fake.u.decoded.main_data_len));
			}
		}
}

UT_TEST(test_cold_lock_completion_uses_exact_tt_clog_and_native_owner)
{
	for (int variant = 0; variant < 4; variant++)
		for (int fault = 0; fault < 15; fault++) {
			RfSideOnlinePlanV1 *plan;
			ClusterRecoverySerialGuard guard = { 0 };
			ClusterThreadRecoveryAuthorityV1 authority
				= { .duty = &canonical_duty, .serial_guard = &guard };
			RfSideOnlineProductionOwnerV1 *owner;
			RfSideXactOperationV1 op;
			FakeXactRecord fake;
			PGAlignedBlock initial;
			volatile bool caught = false;
			RfPageProofDetailV1 result = RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
			bool abort = variant == 1 || variant == 2;
			bool children = variant >= 2;
			bool combined = fault == 4;
			bool success = fault <= 4 || fault == 11;

			cold_lock_completion(&fake, abort, combined);
			if (children)
				cold_terminal_children(&fake, abort);
			if (!rf_side_xact_decode_recovery_v1(&fake.reader, UINT64_C(0x11223344), 3, &op)
				|| !op.cold_completion_effects) {
				UT_ASSERT(false);
				continue;
			}
			plan = space_online_plan(abort ? 400 : 300);
			owner = calloc(1, sizeof(*owner));
			guard.mode = fault == 10 ? CLUSTER_RECOVERY_SERIAL_ONLINE
									 : CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
			undo_header_bind(&fake, 0);
			undo_header_feed(plan, &fake, 100);
			if (abort) {
				undo_header_abort(&fake, 4);
				undo_header_feed(plan, &fake, 200);
			}
			cold_lock_completion(&fake, abort, combined);
			if (children)
				cold_terminal_children(&fake, abort);
			undo_header_feed(plan, &fake, abort ? 300 : 200);
			UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
			if (fault == 14) {
				RfSideOnlineOperationV1 saved;
				uint32 flags;
				UT_ASSERT(rf_side_online_plan_operation_v1(plan, abort ? 2 : 1, &saved));
				memcpy(&flags,
					   saved.owned_payload + (abort ? MinSizeOfXactAbort : MinSizeOfXactCommit),
					   sizeof(flags));
				flags |= XACT_XINFO_HAS_GID;
				memcpy((uint8 *)saved.owned_payload
						   + (abort ? MinSizeOfXactAbort : MinSizeOfXactCommit),
					   &flags, sizeof(flags));
			}
			reset_prepare_apply();
			canonical_file = tmpfile();
			UT_ASSERT(canonical_file != NULL && owner != NULL);
			UT_ASSERT_EQ(ftruncate(fileno(canonical_file), UNDO_SEGMENT_SIZE_BYTES), 0);
			cluster_undo_segment_make_header_bytes(513, 3, initial.data);
			UT_ASSERT(pwrite(fileno(canonical_file), initial.data, BLCKSZ, 0) == BLCKSZ);
			UT_ASSERT_EQ(fsync(fileno(canonical_file)), 0);
			canonical_writes = canonical_syncs = 0;
			canonical_expected_xid = 802;
			canonical_expected_wrap = 7;
			cold_expected_children = children ? lengthof(cold_child_xids) : 0;
			cold_completion_test = cold_clog_test = cold_locks_test = true;
			cold_abort_test = abort;
			cold_cache_test = combined && !abort;
			cold_cache_init = false;
			cold_source_ready = false;
			cold_source_calls = cold_source_order = cold_stats_calls = cold_stats_order
				= cold_stats_gc = 0;
			cold_cache_calls = cold_cache_order = cold_clog_applies = cold_clog_verifies
				= cold_clog_verified_order = 0;
			cold_locks_calls = cold_locks_order = 0;
			cold_clog_apply_fail = fault == 5;
			cold_clog_verify_fail = fault == 6;
			cold_clog_stale_after_verify = fault == 7;
			cold_locks_throw = fault == 8;
			cold_locks_expire = fault == 9;
			standbyState = fault < 3	 ? (HotStandbyState)(STANDBY_INITIALIZED + fault)
						   : fault == 3	 ? STANDBY_DISABLED
						   : fault == 12 ? (HotStandbyState)77
						   : fault == 13 ? (HotStandbyState)-1
										 : STANDBY_INITIALIZED;
			UT_ASSERT_EQ(payload_bytes, 0);
			payload_peak = 0;
			payload_tracking = true;
			UT_ASSERT(rf_side_online_production_owner_init_v1(owner, &authority,
															  canonical_authority_fresh, 19, true));
			UT_ASSERT(rf_side_online_production_bind_undo_v1(owner, &authority));
			if (fault == 11) {
				UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, owner),
							 RF_PAGE_PROOF_DETAIL_OK);
				UT_ASSERT_EQ(cold_locks_calls, 1);
				reset_prepare_apply();
				canonical_writes = cold_clog_applies = cold_clog_verifies = cold_clog_verified_order
					= 0;
				cold_locks_calls = cold_locks_order = 0;
				cold_source_ready = false;
			}
			PG_TRY();
			{
				result = rf_side_online_production_apply_v1(plan, owner);
			}
			PG_CATCH();
			{
				caught = true;
			}
			PG_END_TRY();
			payload_tracking = false;
			UT_ASSERT_EQ(payload_bytes, 0);
			UT_ASSERT(payload_peak <= op.completion_payload_length);
			UT_ASSERT_EQ(caught, fault == 8);
			if (!caught)
				UT_ASSERT_EQ(result, success ? RF_PAGE_PROOF_DETAIL_OK
											 : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
			UT_ASSERT_EQ(cold_locks_calls,
						 (success && fault != 3) || fault == 8 || fault == 9 ? 1 : 0);
			UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, success ? 1 : 0);
			if (success && fault != 3) {
				UT_ASSERT(cold_clog_verified_order < cold_locks_order);
				UT_ASSERT(cold_locks_order < prepare_apply.terminal_projection_order);
				if (combined) {
					if (!abort)
						UT_ASSERT(cold_cache_order < cold_locks_order);
					UT_ASSERT(cold_locks_order < cold_stats_order);
				}
			}
			if (fault >= 10)
				UT_ASSERT_EQ(canonical_writes, 0);
			UT_ASSERT_EQ(owner->protected_set_complete, success);
			UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0
					  && owner->undo_headers == NULL && !owner->protected_set_active
					  && DatabasePath == NULL);
			free(owner);
			UT_ASSERT_EQ(fclose(canonical_file), 0);
			canonical_file = NULL;
			cold_completion_test = cold_source_ready = cold_cache_test = cold_clog_test
				= cold_abort_test = false;
			cold_clog_denied = cold_clog_apply_fail = cold_clog_verify_fail
				= cold_clog_stale_after_verify = false;
			cold_locks_test = cold_locks_throw = cold_locks_expire = false;
			standbyState = STANDBY_DISABLED;
			cold_expected_children = 0;
			canonical_expected_xid = 818;
			canonical_expected_wrap = 8;
			rf_side_online_plan_destroy_v1(&plan);
		}
}

UT_TEST(test_cold_native_stats_retain_completion_owner)
{
	for (int abort = 0; abort < 2; abort++) {
		FakeXactRecord fake;
		RfSideOnlinePlanV1 *plan = space_online_plan(abort ? 400 : 300);
		RfSideContributionOwnersV1 owners;

		undo_header_bind(&fake, 0);
		undo_header_feed(plan, &fake, 100);
		if (abort) {
			undo_header_abort(&fake, 4);
			undo_header_feed(plan, &fake, 200);
			cold_completion_abort(&fake);
		} else
			cold_completion_commit(&fake, false, true, false);
		undo_header_feed(plan, &fake, abort ? 300 : 200);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(rf_side_online_plan_contribution_owners_v1(plan, abort ? 2 : 1, &owners));
		UT_ASSERT((owners.owners & RF_SIDE_CONTRIBUTION_COMPLETION) != 0);
		UT_ASSERT((owners.owners & RF_SIDE_CONTRIBUTION_TERMINAL) != 0);
		UT_ASSERT((owners.owners & RF_SIDE_CONTRIBUTION_INVALIDATION) == 0);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_abort_delete_retains_complete_native_carrier)
{
	RelFileLocator locators[] = { { DEFAULTTABLESPACE_OID, 5, 16384 },
								  { DEFAULTTABLESPACE_OID, 5, 16384 },
								  { DEFAULTTABLESPACE_OID, 5, 16385 } };

	for (int stats = 0; stats < 2; stats++) {
		FakeXactRecord fake;
		RfSideXactOperationV1 operation, rejected;
		uint8 unaligned[1025];

		cold_abort_deletes(&fake, locators, lengthof(locators), stats);
		if (!rf_side_xact_decode_recovery_v1(&fake.reader, UINT64_C(0x11223344), 3, &operation)) {
			UT_ASSERT(false);
			continue;
		}
		UT_ASSERT(operation.kind == RF_SIDE_XACT_ABORT && operation.cold_completion_effects);
		UT_ASSERT_EQ(operation.relation_count, lengthof(locators));
		UT_ASSERT_EQ(operation.space_drop_count, 0);
		UT_ASSERT_EQ(operation.completion_payload_length, fake.u.decoded.main_data_len);
		UT_ASSERT(!rf_side_xact_decode_v1(&fake.reader, UINT64_C(0x11223344), 3, &rejected));
		UT_ASSERT(!rf_side_xact_inspect_pi_v1(&fake.reader, UINT64_C(0x11223344), 3, &rejected));
		memcpy(unaligned + 1, fake.data, fake.u.decoded.main_data_len);
		UT_ASSERT(rf_side_xact_cold_completion_payload_v1(&operation, unaligned + 1,
														  fake.u.decoded.main_data_len));
		operation.relation_count--;
		UT_ASSERT(!rf_side_xact_cold_completion_payload_v1(&operation, unaligned + 1,
														   fake.u.decoded.main_data_len));
	}
	for (int fault = 0; fault < 4; fault++) {
		FakeXactRecord fake;
		RfSideXactOperationV1 rejected, empty = { 0 };
		uint32 offset = MinSizeOfXactAbort + sizeof(xl_xact_xinfo) + sizeof(xl_xact_dbinfo);
		int count = fault == 0 ? -1 : INT_MAX;

		cold_abort_deletes(&fake, locators, lengthof(locators), false);
		if (fault < 2)
			memcpy(fake.data + offset, &count, sizeof(count));
		else if (fault == 2)
			fake.u.decoded.main_data_len--;
		else
			fake.data[fake.u.decoded.main_data_len++] = 0;
		memset(&rejected, 0xa5, sizeof(rejected));
		UT_ASSERT(
			!rf_side_xact_decode_recovery_v1(&fake.reader, UINT64_C(0x11223344), 3, &rejected));
		UT_ASSERT(memcmp(&rejected, &empty, sizeof(empty)) == 0);
	}
}

UT_TEST(test_cold_abort_delete_plan_binds_every_locator_and_owns_original_bytes)
{
	RelFileLocator locators[] = { { DEFAULTTABLESPACE_OID, 5, 16384 },
								  { DEFAULTTABLESPACE_OID, 5, 16384 },
								  { DEFAULTTABLESPACE_OID, 5, 16385 } };

	for (int shape = 0; shape < 3; shape++) {
		uint32 pairs = shape == 2 ? 2 : 1;
		RfSideOnlinePlanV1 *plan = space_online_plan((pairs * 2 + 2) * 100);
		RfSideOnlineOperationV1 operation;
		RfSideContributionOwnersV1 owners;
		FakeXactRecord fake;
		uint8 original[1024];
		uint32 length;

		for (uint32 i = 0; i < pairs * 2; i++) {
			cold_abort_create_record(&fake, locators[i < 2 ? 0 : 2], i % 2 == 0, 802);
			UT_ASSERT_EQ(cold_abort_delete_feed(plan, &fake, (i + 1) * 100, 0),
						 RF_PAGE_PROOF_DETAIL_OK);
		}
		cold_abort_deletes(&fake, locators, shape + 1, shape == 2);
		length = fake.u.decoded.main_data_len;
		memcpy(original, fake.data, length);
		UT_ASSERT_EQ(cold_abort_delete_feed(plan, &fake, (pairs * 2 + 1) * 100, 0),
					 RF_PAGE_PROOF_DETAIL_OK);
		memset(fake.data, 0xee, length);
		UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
		if (rf_side_online_plan_seal_v1(plan) != RF_PAGE_PROOF_DETAIL_OK) {
			UT_ASSERT(false);
			rf_side_online_plan_destroy_v1(&plan);
			continue;
		}
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, pairs * 2, &operation));
		UT_ASSERT_EQ(operation.xact.relation_count, shape + 1);
		UT_ASSERT_EQ(operation.owned_payload_length, length);
		UT_ASSERT(memcmp(operation.owned_payload, original, length) == 0);
		for (uint32 r = 0; r <= (uint32)shape; r++) {
			uint32 creation = UINT32_MAX;
			RfSideOnlineOperationV1 bound;
			UT_ASSERT(rf_side_online_plan_abort_creation_v1(plan, pairs * 2, r, &creation));
			UT_ASSERT_EQ(creation, r == 2 ? 3 : 1);
			UT_ASSERT(rf_side_online_plan_operation_v1(plan, creation, &bound));
			UT_ASSERT(RelFileLocatorEquals(bound.space_key.locator, locators[r]));
		}
		{
			uint32 creation = 0;
			UT_ASSERT(
				!rf_side_online_plan_abort_creation_v1(plan, pairs * 2, shape + 1, &creation));
			UT_ASSERT_EQ(creation, UINT32_MAX);
			creation = 0;
			UT_ASSERT(!rf_side_online_plan_abort_creation_v1(plan, 0, 0, &creation));
			UT_ASSERT_EQ(creation, UINT32_MAX);
			UT_ASSERT(!rf_side_online_plan_abort_creation_v1(plan, pairs * 2, 0, NULL));
		}
		UT_ASSERT(rf_side_online_plan_contribution_owners_v1(plan, pairs * 2, &owners));
		UT_ASSERT((owners.owners & RF_SIDE_CONTRIBUTION_NATIVE_STRUCTURE) != 0);
		UT_ASSERT((owners.owners & RF_SIDE_CONTRIBUTION_COMPLETION) != 0);
		UT_ASSERT((owners.owners & RF_SIDE_CONTRIBUTION_TERMINAL) != 0);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_abort_delete_plan_refuses_missing_conflicting_or_late_source_proof)
{
	/* N=native MAIN CREATE, T=typed CREATE, B=earlier terminal, A=native ABORT. */
	const char *events[] = { "TA", "NA", "NTA", "NTA", "NTA", "NAT", "NTBA", "NTNTA" };
	RelFileLocator locator = { DEFAULTTABLESPACE_OID, 5, 16384 };

	for (uint32 fault = 0; fault < lengthof(events); fault++) {
		uint32 count = strlen(events[fault]);
		RfContributorStreamCutV1 cuts[2] = { { .failed_thread = 3,
											   .timeline_id = 7,
											   .flags = RF_CONTRIBUTOR_CUT_COMPLETE,
											   .scan_begin_inclusive = 100,
											   .scan_end_exclusive = (count + 1) * 100 },
											 { .failed_thread = 4,
											   .timeline_id = 7,
											   .flags = RF_CONTRIBUTOR_CUT_COMPLETE,
											   .scan_begin_inclusive = 100,
											   .scan_end_exclusive = 200 } };
		RfSideOnlinePlanRequestV1 request = { .system_identifier = UINT64_C(0x11223344),
											  .physical_cuts = cuts,
											  .participant_count = fault == 4 ? 2 : 1 };
		RfSideOnlinePlanV1 *plan = NULL;
		RelFileLocator created = locator;
		bool fed = true;

		if (fault == 3)
			created.relNumber++;
		if (fault == 4)
			cuts[0].scan_end_exclusive -= 100;
		memset(request.storage_uuid, 0x44, 16);
		UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
		for (uint32 i = 0; i < count; i++) {
			FakeXactRecord fake;
			bool abort = events[fault][i] == 'A';
			uint32 participant = fault == 4 && abort ? 1 : 0;

			if (abort)
				cold_abort_deletes(&fake, &locator, 1, false);
			else if (events[fault][i] == 'B') {
				make_abort(&fake, 802);
				fake.u.decoded.max_block_id = -1;
			} else
				cold_abort_create_record(&fake, created, events[fault][i] == 'N',
										 fault == 2 ? 803 : 802);
			if (cold_abort_delete_feed(plan, &fake, participant ? 100 : (i + 1) * 100, participant)
				!= RF_PAGE_PROOF_DETAIL_OK) {
				UT_ASSERT(false);
				fed = false;
				break;
			}
		}
		if (fed) {
			UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
			UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
		}
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_abort_delete_input_cannot_enter_tt_clog_or_native_completion)
{
	RelFileLocator locator = { DEFAULTTABLESPACE_OID, 5, 16384 };
	FakeXactRecord fake;
	RfSideXactOperationV1 operation;
	ClusterUndoRecoveryScopeV1 scope = { 0 };
	int coverage_calls = 0;

	reset_prepare_apply();
	cold_abort_deletes(&fake, &locator, 1, true);
	if (!rf_side_xact_decode_recovery_v1(&fake.reader, UINT64_C(0x11223344), 3, &operation)) {
		UT_ASSERT(false);
		return;
	}
	canonical_scope = &scope;
	cold_clog_test = cold_source_ready = cold_abort_test = true;
	cold_clog_applies = cold_clog_verifies = cold_stats_calls = 0;
	UT_ASSERT(rf_side_xact_cold_completion_preflight_v1(&operation, fake.data,
														fake.u.decoded.main_data_len));
	UT_ASSERT(
		!rf_side_xact_cold_abort_clog_v1(&operation, fake.data, fake.u.decoded.main_data_len));
	UT_ASSERT_EQ(rf_side_xact_apply_cold_abort_completion_v1(
					 &operation, fake.data, fake.u.decoded.main_data_len, &coverage_calls,
					 drop_coverage_must_not_run),
				 RF_SIDE_XACT_APPLY_BLOCKED);
	UT_ASSERT_EQ(rf_side_xact_apply_owned_v1(&operation, fake.data, fake.u.decoded.main_data_len),
				 RF_SIDE_XACT_APPLY_BLOCKED);
	UT_ASSERT_EQ(coverage_calls, 0);
	UT_ASSERT_EQ(cold_clog_applies, 0);
	UT_ASSERT_EQ(cold_clog_verifies, 0);
	UT_ASSERT_EQ(cold_stats_calls, 0);
	UT_ASSERT_EQ(prepare_apply.tt_stamps, 0);
	UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
	canonical_scope = NULL;
	cold_clog_test = cold_source_ready = cold_abort_test = false;
}

UT_TEST(test_cold_abort_completion_keeps_owned_stats_and_refuses_generic_apply)
{
	FakeXactRecord fake;
	RfSideXactOperationV1 operation, rejected;
	uint8 unaligned[1024];

	cold_completion_abort(&fake);
	reset_prepare_apply();
	if (!rf_side_xact_decode_recovery_v1(&fake.reader, UINT64_C(0x11223344), 3, &operation)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT(operation.kind == RF_SIDE_XACT_ABORT && operation.cold_completion_effects
			  && !operation.pi_read_only && !operation.has_tt_delta);
	UT_ASSERT_EQ(operation.completion_payload_length, fake.u.decoded.main_data_len);
	UT_ASSERT(!rf_side_xact_decode_v1(&fake.reader, UINT64_C(0x11223344), 3, &rejected));
	UT_ASSERT(!rf_side_xact_inspect_pi_v1(&fake.reader, UINT64_C(0x11223344), 3, &rejected));
	UT_ASSERT(!rf_side_xact_preflight_covered_abort_v1(&operation));
	memcpy(unaligned + 1, fake.data, fake.u.decoded.main_data_len);
	UT_ASSERT(rf_side_xact_cold_completion_preflight_v1(&operation, unaligned + 1,
														fake.u.decoded.main_data_len));
	UT_ASSERT_EQ(rf_side_xact_apply_owned_v1(&operation, fake.data, fake.u.decoded.main_data_len),
				 RF_SIDE_XACT_APPLY_BLOCKED);
	UT_ASSERT_EQ(rf_side_xact_apply_cold_completion_v1(&operation, fake.data,
													   fake.u.decoded.main_data_len, NULL, NULL),
				 RF_SIDE_XACT_APPLY_BLOCKED);
	UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
}

UT_TEST(test_cold_abort_stats_rejects_malformed_or_unowned_native_effects)
{
	for (int fault = 0; fault < 13; fault++) {
		FakeXactRecord fake;
		RfSideXactOperationV1 operation, empty = { 0 };
		xl_xact_stats_item item;
		uint32 offset = MinSizeOfXactAbort + sizeof(xl_xact_xinfo) + sizeof(xl_xact_dbinfo);
		uint32 xinfo;
		int count;

		cold_completion_abort(&fake);
		memcpy(&item, fake.data + offset + sizeof(int), sizeof(item));
		if (fault == 0)
			item.kind = PGSTAT_KIND_INVALID;
		if (fault == 1)
			item.kind = PGSTAT_KIND_WAL;
		if (fault == 2)
			item.kind = PGSTAT_KIND_SUBSCRIPTION;
		if (fault == 3)
			item.objoid = InvalidOid;
		if (fault == 4)
			item.dboid = 6;
		if (fault == 5) {
			item.kind = PGSTAT_KIND_FUNCTION;
			item.dboid = InvalidOid;
		}
		memcpy(fake.data + offset + sizeof(int), &item, sizeof(item));
		if (fault == 6 || fault == 7) {
			count = fault == 6 ? -1 : INT_MAX;
			memcpy(fake.data + offset, &count, sizeof(count));
		}
		if (fault == 8)
			fake.u.decoded.main_data_len--;
		memcpy(&xinfo, fake.data + MinSizeOfXactAbort, sizeof(xinfo));
		if (fault == 9)
			xinfo |= XACT_XINFO_HAS_GID;
		if (fault == 10)
			xinfo |= XACT_COMPLETION_FORCE_SYNC_COMMIT;
		if (fault == 11)
			xinfo |= XACT_XINFO_HAS_INVALS;
		memcpy(fake.data + MinSizeOfXactAbort, &xinfo, sizeof(xinfo));
		if (fault == 12)
			fake.u.decoded.header.xl_info = XLOG_XACT_ABORT_PREPARED | XLOG_XACT_HAS_INFO;
		memset(&operation, 0xa5, sizeof(operation));
		UT_ASSERT(
			!rf_side_xact_decode_recovery_v1(&fake.reader, UINT64_C(0x11223344), 3, &operation));
		UT_ASSERT_EQ(memcmp(&operation, &empty, sizeof(empty)), 0);
	}
}

static void
side_abort_stats_identity(bool history)
{
	for (int fault = 0; fault < 10; fault++) {
		FakeXactRecord fake;
		RfSideXactOperationV1 decoded;
		RfSideOnlineOperationV1 operation;
		RfSideOnlinePlanV1 *plan;
		ClusterUndoDecoded target, untouched;
		uint8 copy[1024];
		XLogRecPtr end = fault == 1 ? 300 : 400;

		cold_completion_abort(&fake);
		if (!rf_side_xact_decode_recovery_v1(&fake.reader, UINT64_C(0x11223344), 3, &decoded)) {
			UT_ASSERT(false);
			return;
		}
		plan = space_online_plan_redo(end, history ? &end : NULL);
		undo_header_bind(&fake, 0);
		undo_header_feed(plan, &fake, 100);
		if (fault != 1) {
			undo_header_abort(&fake, fault == 2 ? 5 : 4);
			undo_header_feed(plan, &fake, 200);
		}
		cold_completion_abort(&fake);
		undo_header_feed(plan, &fake, fault == 1 ? 200 : 300);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, fault == 1 ? 1 : 2, &operation));
		UT_ASSERT_EQ(operation.history_only, history);
		if (fault == 3)
			operation.owned_payload_length--;
		if (fault == 4)
			operation.xact.terminal_scn++;
		if (fault == 5)
			operation.identity.participant_index++;
		if (fault == 6) {
			memcpy(copy, operation.owned_payload, operation.owned_payload_length);
			operation.owned_payload = copy;
		}
		if (fault == 7)
			operation.purpose = RF_SIDE_PLAN_PI_CONTRIBUTIONS;
		if (fault == 8) {
			uint32 offset
				= MinSizeOfXactAbort + sizeof(xl_xact_xinfo) + sizeof(xl_xact_dbinfo) + sizeof(int);
			xl_xact_stats_item item = { PGSTAT_KIND_WAL, 5, 16388 };
			memcpy((uint8 *)operation.owned_payload + offset, &item, sizeof(item));
		}
		if (fault == 9)
			operation.history_only = !operation.history_only;
		memset(&untouched, 0x93, sizeof(untouched));
		target = untouched;
		UT_ASSERT_EQ(rf_side_online_plan_abort_target_v1(plan, &operation, &target),
					 fault == 0 && !history);
		if (fault != 0 || history)
			UT_ASSERT_EQ(memcmp(&target, &untouched, sizeof(target)), 0);
		target = untouched;
		UT_ASSERT_EQ(rf_side_online_plan_abort_identity_v1(plan, &operation, &target), fault == 0);
		if (fault == 0)
			UT_ASSERT(target.instance == 3 && target.segment_id == 513 && target.slot_offset == 4
					  && target.expected_generation == 0 && target.wrap == 7 && target.xid == 802);
		else
			UT_ASSERT_EQ(memcmp(&target, &untouched, sizeof(target)), 0);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_abort_stats_keeps_exact_native_record_and_tt_carrier)
{
	side_abort_stats_identity(false);
}

UT_TEST(test_cold_history_abort_stats_identity_does_not_authorize_replay)
{
	side_abort_stats_identity(true);
}

UT_TEST(test_cold_abort_stats_source_tt_clog_and_native_owner_precede_terminal)
{
	for (int fault = 0; fault < 13; fault++) {
		RfSideOnlinePlanV1 *plan;
		ClusterRecoverySerialGuard guard = { 0 };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 *owner;
		RfSideOnlineOperationV1 operation;
		RfSideXactOperationV1 decoded;
		FakeXactRecord fake;
		PGAlignedBlock initial;
		volatile bool caught = false;
		RfPageProofDetailV1 result = RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
		bool success = fault == 0 || fault == 12;

		cold_completion_abort(&fake);
		if (!rf_side_xact_decode_recovery_v1(&fake.reader, UINT64_C(0x11223344), 3, &decoded)) {
			UT_ASSERT(false);
			return;
		}
		plan = space_online_plan(400);
		owner = calloc(1, sizeof(*owner));
		guard.mode
			= fault == 7 ? CLUSTER_RECOVERY_SERIAL_ONLINE : CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
		undo_header_bind(&fake, 0);
		undo_header_feed(plan, &fake, 100);
		undo_header_abort(&fake, 4);
		undo_header_feed(plan, &fake, 200);
		cold_completion_abort(&fake);
		undo_header_feed(plan, &fake, 300);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 2, &operation));
		if (fault == 8) {
			uint32 offset
				= MinSizeOfXactAbort + sizeof(xl_xact_xinfo) + sizeof(xl_xact_dbinfo) + sizeof(int);
			xl_xact_stats_item item = { PGSTAT_KIND_WAL, 5, 16388 };
			memcpy((uint8 *)operation.owned_payload + offset, &item, sizeof(item));
		}
		reset_prepare_apply();
		canonical_file = tmpfile();
		UT_ASSERT(canonical_file != NULL && owner != NULL);
		UT_ASSERT_EQ(ftruncate(fileno(canonical_file), UNDO_SEGMENT_SIZE_BYTES), 0);
		cluster_undo_segment_make_header_bytes(513, 3, initial.data);
		UT_ASSERT(pwrite(fileno(canonical_file), initial.data, BLCKSZ, 0) == BLCKSZ);
		UT_ASSERT_EQ(fsync(fileno(canonical_file)), 0);
		canonical_writes = canonical_syncs = 0;
		canonical_expected_xid = 802;
		canonical_expected_wrap = 7;
		cold_completion_test = cold_clog_test = cold_abort_test = true;
		cold_source_calls = cold_source_order = cold_stats_calls = cold_stats_order = cold_stats_gc
			= 0;
		cold_cache_calls = cold_clog_applies = cold_clog_verifies = cold_clog_verified_order = 0;
		cold_source_ready = false;
		cold_source_fail = fault == 1;
		cold_source_throw = fault == 2;
		cold_stats_throw = fault == 3;
		cold_stats_expire = fault == 4;
		cold_clog_apply_fail = fault == 5;
		cold_clog_verify_fail = fault == 6;
		cold_clog_stale_after_verify = fault == 9;
		canonical_file_io_error = fault == 10;
		cold_abort_projection_conflict = fault == 11;
		UT_ASSERT_EQ(payload_bytes, 0);
		payload_peak = 0;
		payload_tracking = true;
		UT_ASSERT(rf_side_online_production_owner_init_v1(owner, &authority,
														  canonical_authority_fresh, 19, true));
		UT_ASSERT(rf_side_online_production_bind_undo_v1(owner, &authority));
		if (fault == 12) {
			UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, owner), RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_EQ(canonical_writes, 1);
			reset_prepare_apply();
			canonical_writes = cold_source_calls = cold_source_order = cold_stats_calls
				= cold_stats_gc = 0;
			cold_clog_applies = cold_clog_verifies = cold_clog_verified_order = 0;
			cold_source_ready = false;
		}
		PG_TRY();
		{
			result = rf_side_online_production_apply_v1(plan, owner);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		payload_tracking = false;
		UT_ASSERT_EQ(payload_bytes, 0);
		UT_ASSERT(payload_peak <= operation.owned_payload_length);
		UT_ASSERT_EQ(caught, fault == 2 || fault == 3);
		if (!caught)
			UT_ASSERT_EQ(result,
						 success ? RF_PAGE_PROOF_DETAIL_OK : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
		UT_ASSERT_EQ(cold_source_calls, fault == 7 || fault == 8 || fault == 10 ? 0 : 1);
		UT_ASSERT_EQ(cold_stats_calls, success || fault == 4 || fault == 11 ? 3
									   : fault == 3							? 1
																			: 0);
		UT_ASSERT_EQ(cold_stats_gc, cold_stats_calls == 3 ? 1 : 0);
		UT_ASSERT_EQ(cold_cache_calls, 0);
		UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, success || fault == 11 ? 1 : 0);
		if (success) {
			UT_ASSERT(cold_source_order < cold_clog_verified_order);
			UT_ASSERT(cold_clog_verified_order < cold_stats_order);
			UT_ASSERT(cold_stats_order < prepare_apply.terminal_projection_order);
		}
		if (fault == 1 || fault == 2 || fault == 7 || fault == 8 || fault == 10 || fault == 12)
			UT_ASSERT_EQ(canonical_writes, 0);
		UT_ASSERT_EQ(owner->protected_set_complete, success);
		UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0
				  && owner->undo_headers == NULL && !owner->protected_set_active);
		free(owner);
		UT_ASSERT_EQ(fclose(canonical_file), 0);
		canonical_file = NULL;
		canonical_file_io_error = false;
		cold_completion_test = cold_source_ready = cold_source_fail = cold_source_throw = false;
		cold_stats_throw = cold_stats_expire = cold_abort_test = cold_clog_test = cold_clog_denied
			= false;
		cold_clog_apply_fail = cold_clog_verify_fail = cold_clog_stale_after_verify
			= cold_abort_projection_conflict = false;
		canonical_expected_xid = 818;
		canonical_expected_wrap = 8;
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_completion_keeps_stats_and_force_payload_owned)
{
	for (unsigned shape = 0; shape < 5; shape++) {
		FakeXactRecord fake;
		RfSideXactOperationV1 operation, strict;
		bool cache = shape == 1 || shape == 4;
		bool stats = shape != 2;
		cold_completion_commit(&fake, cache, stats, shape >= 2);
		reset_prepare_apply();
		UT_ASSERT(
			rf_side_xact_decode_recovery_v1(&fake.reader, UINT64_C(0x11223344), 3, &operation));
		UT_ASSERT(!rf_side_xact_decode_v1(&fake.reader, UINT64_C(0x11223344), 3, &strict));
		if (stats)
			UT_ASSERT(!rf_side_xact_inspect_pi_v1(&fake.reader, UINT64_C(0x11223344), 3, &strict));
		UT_ASSERT_EQ(operation.completion_payload_length, fake.u.decoded.main_data_len);
		UT_ASSERT(rf_side_xact_cold_completion_preflight_v1(&operation, fake.data,
															fake.u.decoded.main_data_len));
		UT_ASSERT_EQ(
			rf_side_xact_apply_owned_v1(&operation, fake.data, fake.u.decoded.main_data_len),
			RF_SIDE_XACT_APPLY_BLOCKED);
	}
}

UT_TEST(test_cold_stats_rejects_unknown_kinds_and_malformed_complete_batch)
{
	for (int fault = 0; fault < 9; fault++) {
		FakeXactRecord fake;
		RfSideXactOperationV1 operation, empty = { 0 };
		xl_xact_stats_item item;
		uint32 offset = MinSizeOfXactCommit + sizeof(xl_xact_xinfo) + sizeof(xl_xact_dbinfo);
		cold_completion_commit(&fake, true, true, true);
		memcpy(&item, fake.data + offset + sizeof(int), sizeof(item));
		if (fault == 0)
			item.kind = PGSTAT_KIND_INVALID;
		if (fault == 1)
			item.kind = PGSTAT_KIND_WAL;
		if (fault == 2)
			item.kind = PGSTAT_KIND_SUBSCRIPTION;
		if (fault == 3)
			item.objoid = InvalidOid;
		if (fault == 4)
			item.dboid = 6;
		if (fault == 5) {
			item.kind = PGSTAT_KIND_FUNCTION;
			item.dboid = InvalidOid;
		}
		memcpy(fake.data + offset + sizeof(int), &item, sizeof(item));
		if (fault == 6) {
			int count = -1;
			memcpy(fake.data + offset, &count, sizeof(count));
		}
		if (fault == 7)
			fake.u.decoded.main_data_len--;
		if (fault == 8) {
			uint32 xinfo;
			memcpy(&xinfo, fake.data + MinSizeOfXactCommit, sizeof(xinfo));
			xinfo |= XACT_COMPLETION_APPLY_FEEDBACK;
			memcpy(fake.data + MinSizeOfXactCommit, &xinfo, sizeof(xinfo));
		}
		memset(&operation, 0xa5, sizeof(operation));
		UT_ASSERT(
			!rf_side_xact_decode_recovery_v1(&fake.reader, UINT64_C(0x11223344), 3, &operation));
		UT_ASSERT_EQ(memcmp(&operation, &empty, sizeof(empty)), 0);
	}
}

/* Compare recovery and inspection storage for identical records. Inspection
 * never allocates a native replay copy; recovery must reserve the maximum
 * simultaneous copy in addition to the same owned records and page scratch. */
static RfSideOnlinePlanV1 *
cold_cache_budget_plan(Size budget, bool inspect, uint32 commits, RfPageProofDetailV1 *last_feed)
{
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut = { 0 };
	RfSideOnlinePlanRequestV1 request = { 0 };
	FakeXactRecord fake;

	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 100 * (commits + 2);
	request.system_identifier = UINT64_C(0x11223344);
	memset(request.storage_uuid, 0x44, 16);
	request.physical_cuts = &cut;
	request.participant_count = 1;
	request.memory_budget = budget;
	request.purpose = inspect ? RF_SIDE_PLAN_PI_CONTRIBUTIONS : RF_SIDE_PLAN_RECOVERY;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	undo_header_bind(&fake, 0);
	undo_header_feed(plan, &fake, 100);
	for (uint32 i = 0; i < commits; i++) {
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 record;
		uint8 uuid[16];

		cold_cache_commit(&fake, false);
		if (i == 1) {
			uint32 offset = MinSizeOfXactCommit + sizeof(xl_xact_xinfo) + sizeof(xl_xact_dbinfo);
			uint32 bytes = 2 * sizeof(SharedInvalidationMessage);
			uint32 tail = offset + sizeof(int) + bytes;
			int count = 4;

			memmove(fake.data + tail + bytes, fake.data + tail,
					fake.u.decoded.main_data_len - tail);
			memcpy(fake.data + tail, fake.data + offset + sizeof(int), bytes);
			memcpy(fake.data + offset, &count, sizeof(count));
			fake.u.decoded.main_data_len += bytes;
		}
		memset(uuid, 0x44, sizeof(uuid));
		identity = make_identity(&fake, uuid);
		set_identity_range(&fake, &identity, 200 + 100 * i, 300 + 100 * i);
		record = make_record_plan(&fake);
		*last_feed = rf_side_online_plan_feed_record_v1(plan, &record, &identity);
		if (*last_feed != RF_PAGE_PROOF_DETAIL_OK)
			break;
	}
	return plan;
}

UT_TEST(test_cold_cache_payload_copy_is_reserved_once_at_maximum)
{
	for (uint32 count = 1; count <= 3; count++) {
		RfPageProofDetailV1 detail;
		RfSideOnlinePlanV1 *inspect = cold_cache_budget_plan(0, true, count, &detail);
		RfSideOnlinePlanV1 *replay;
		uint32 maximum = 0;

		UT_ASSERT_EQ(detail, RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(inspect), RF_PAGE_PROOF_DETAIL_OK);
		replay = cold_cache_budget_plan(0, false, count, &detail);
		UT_ASSERT_EQ(detail, RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(replay), RF_PAGE_PROOF_DETAIL_OK);
		for (uint32 i = 1; i <= count; i++) {
			RfSideOnlineOperationV1 op;

			UT_ASSERT(rf_side_online_plan_operation_v1(replay, i, &op));
			maximum = Max(maximum, op.owned_payload_length);
		}
		UT_ASSERT_EQ(rf_side_online_plan_scratch_available_v1(inspect)
						 - rf_side_online_plan_scratch_available_v1(replay),
					 maximum);
		rf_side_online_plan_destroy_v1(&replay);
		rf_side_online_plan_destroy_v1(&inspect);
	}
}

UT_TEST(test_cold_cache_budget_refuses_before_header_preparation)
{
	RfPageProofDetailV1 detail;
	RfSideOnlinePlanV1 *inspect = cold_cache_budget_plan(0, true, 1, &detail);
	RfSideOnlineOperationV1 op;
	RfSideUndoHeaderImageV1 image, untouched;
	PGAlignedBlock base, saved;
	Size stored, copy, page_scratch = sizeof(image) + 4 * BLCKSZ;

	UT_ASSERT_EQ(detail, RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(inspect), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_operation_v1(inspect, 1, &op));
	stored = RF_SIDE_ONLINE_PLAN_MAX_BYTES - rf_side_online_plan_scratch_available_v1(inspect);
	copy = op.owned_payload_length;
	rf_side_online_plan_destroy_v1(&inspect);
	cluster_undo_segment_make_header_bytes(513, 3, base.data);
	saved = base;
	reset_prepare_apply();
	for (int exact = 0; exact <= 1; exact++) {
		RfSideOnlinePlanV1 *plan
			= cold_cache_budget_plan(stored + copy - 1 + exact, false, 1, &detail);

		UT_ASSERT_EQ(detail, exact ? RF_PAGE_PROOF_DETAIL_OK : RF_PAGE_PROOF_DETAIL_CAPACITY);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan) == RF_PAGE_PROOF_DETAIL_OK, exact);
		rf_side_online_plan_destroy_v1(&plan);
		plan = cold_cache_budget_plan(stored + copy + page_scratch - 1 + exact, false, 1, &detail);
		UT_ASSERT_EQ(detail, RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		memset(&image, 0xa5, sizeof(image));
		untouched = image;
		UT_ASSERT_EQ(rf_side_online_plan_prepare_undo_header_v1(plan, 3, 513, base.data, &image),
					 exact ? RF_PAGE_PROOF_DETAIL_OK : RF_PAGE_PROOF_DETAIL_CAPACITY);
		if (!exact)
			UT_ASSERT(memcmp(&image, &untouched, sizeof(image)) == 0);
		UT_ASSERT(memcmp(base.data, saved.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_cache_decode_keeps_unsupported_effects_and_bad_messages_refused)
{
	for (int fault = 0; fault < 12; fault++) {
		FakeXactRecord fake;
		RfSideXactOperationV1 operation, empty = { 0 };
		uint32 xinfo, count;
		uint32 count_offset = MinSizeOfXactCommit + sizeof(xl_xact_xinfo) + sizeof(xl_xact_dbinfo);
		SharedInvalidationMessage message;

		cold_cache_commit(&fake, false);
		if (fault < 6) {
			const uint32 flags[] = {
				UINT32_C(1) << 20,		 XACT_COMPLETION_APPLY_FEEDBACK, XACT_XINFO_HAS_GID,
				XACT_XINFO_HAS_SUBXACTS, XACT_XINFO_HAS_DROPPED_STATS,	 XACT_XINFO_HAS_ORIGIN
			};
			memcpy(&xinfo, fake.data + MinSizeOfXactCommit, sizeof(xinfo));
			xinfo |= flags[fault];
			memcpy(fake.data + MinSizeOfXactCommit, &xinfo, sizeof(xinfo));
		} else if (fault == 6)
			fake.u.decoded.main_data_len--;
		else if (fault == 7 || fault == 8) {
			count = fault == 7 ? UINT32_MAX : 10000;
			memcpy(fake.data + count_offset, &count, sizeof(count));
		} else if (fault == 9 || fault == 10) {
			memcpy(&message, fake.data + count_offset + sizeof(int), sizeof(message));
			if (fault == 9)
				message.id = SHAREDINVALRELMAP_ID;
			else
				message.rc.dbId = 6;
			memcpy(fake.data + count_offset + sizeof(int), &message, sizeof(message));
		} else
			fake.u.decoded.max_block_id = 0;
		memset(&operation, 0xa5, sizeof(operation));
		UT_ASSERT(
			!rf_side_xact_decode_recovery_v1(&fake.reader, UINT64_C(0x11223344), 3, &operation));
		UT_ASSERT(memcmp(&operation, &empty, sizeof(empty)) == 0);
	}
}

UT_TEST(test_cold_cache_owned_bytes_and_purpose_are_not_interchangeable)
{
	FakeXactRecord fake;
	RfSideXactOperationV1 operation, inspection, changed;
	uint8 unaligned[sizeof(fake.data) + 1];

	reset_prepare_apply();
	cold_cache_commit(&fake, true);
	UT_ASSERT(rf_side_xact_decode_recovery_v1(&fake.reader, UINT64_C(0x11223344), 3, &operation));
	UT_ASSERT(rf_side_xact_inspect_pi_v1(&fake.reader, UINT64_C(0x11223344), 3, &inspection));
	memcpy(unaligned + 1, fake.data, fake.u.decoded.main_data_len);
	UT_ASSERT(rf_side_xact_cold_completion_preflight_v1(&operation, unaligned + 1,
														fake.u.decoded.main_data_len));
	UT_ASSERT(!rf_side_xact_cold_completion_preflight_v1(&inspection, fake.data,
														 fake.u.decoded.main_data_len));
	UT_ASSERT(!rf_side_xact_structural_preflight_v1(&operation));
	UT_ASSERT(!rf_side_xact_inspection_preflight_v1(&operation));
	for (int fault = 0; fault < 6; fault++) {
		changed = operation;
		switch (fault) {
		case 0:
			changed.invalidation_count++;
			break;
		case 1:
			changed.database++;
			break;
		case 2:
			changed.terminal_scn++;
			break;
		case 3:
			changed.tt_delta.wrap++;
			break;
		case 4:
			changed.origin_thread++;
			break;
		case 5:
			changed.completion_payload_length--;
			break;
		}
		UT_ASSERT(!rf_side_xact_cold_completion_preflight_v1(&changed, fake.data,
															 fake.u.decoded.main_data_len));
	}
	UT_ASSERT_EQ(rf_side_xact_apply_cold_completion_v1(&operation, fake.data,
													   fake.u.decoded.main_data_len, NULL, NULL),
				 RF_SIDE_XACT_APPLY_BLOCKED);
	UT_ASSERT_EQ(cold_cache_calls, 0);
}

UT_TEST(test_cold_commit_keeps_native_cache_effects_before_terminal_projection)
{
	for (int fault = 0; fault < 8; fault++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(300);
		ClusterRecoverySerialGuard guard = { 0 };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 *owner = calloc(1, sizeof(*owner));
		RfSideOnlineOperationV1 operation;
		RfSideXactOperationV1 strict;
		FakeXactRecord fake;
		PGAlignedBlock initial;
		volatile bool caught = false;
		RfPageProofDetailV1 result = RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
		bool success = fault <= 1;

		guard.mode
			= fault == 2 ? CLUSTER_RECOVERY_SERIAL_ONLINE : CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
		undo_header_bind(&fake, 0);
		undo_header_feed(plan, &fake, 100);
		cold_cache_commit(&fake, fault == 1);
		UT_ASSERT(!rf_side_xact_decode_v1(&fake.reader, UINT64_C(123456), 3, &strict));
		undo_header_feed(plan, &fake, 200);
		if (rf_side_online_plan_seal_v1(plan) != RF_PAGE_PROOF_DETAIL_OK) {
			UT_ASSERT(false);
			rf_side_online_plan_destroy_v1(&plan);
			free(owner);
			return;
		}
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
		UT_ASSERT(operation.owned_payload != NULL && operation.owned_payload_length > 0);
		if (fault == 7) {
			/* Corrupt the owned copy after sealing: no partial set may apply. */
			uint32 bad = UINT32_MAX;
			memcpy((uint8 *)operation.owned_payload + MinSizeOfXactCommit, &bad, sizeof(bad));
		}
		reset_prepare_apply();
		canonical_file = tmpfile();
		UT_ASSERT(canonical_file != NULL && owner != NULL);
		UT_ASSERT_EQ(ftruncate(fileno(canonical_file), UNDO_SEGMENT_SIZE_BYTES), 0);
		cluster_undo_segment_make_header_bytes(513, 3, initial.data);
		UT_ASSERT(pwrite(fileno(canonical_file), initial.data, BLCKSZ, 0) == BLCKSZ);
		UT_ASSERT_EQ(fsync(fileno(canonical_file)), 0);
		canonical_writes = canonical_syncs = 0;
		canonical_expected_xid = 802;
		canonical_expected_wrap = 7;
		cold_cache_test = cold_clog_test = true;
		cold_cache_calls = cold_cache_order = 0;
		cold_cache_init = fault == 1;
		cold_cache_throw = fault == 4;
		cold_cache_expire = fault == 5;
		cold_clog_apply_fail = fault == 3;
		cold_clog_applies = cold_clog_verifies = cold_clog_verified_order = 0;
		DatabasePath = fault == 6 ? "existing-database" : NULL;
		UT_ASSERT_EQ(rf_side_xact_apply_owned_v1(&operation.xact, operation.owned_payload,
												 operation.owned_payload_length),
					 RF_SIDE_XACT_APPLY_BLOCKED);
		UT_ASSERT(rf_side_online_production_owner_init_v1(owner, &authority,
														  canonical_authority_fresh, 19, true));
		UT_ASSERT(rf_side_online_production_bind_undo_v1(owner, &authority));
		PG_TRY();
		{
			result = rf_side_online_production_apply_v1(plan, owner);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 4);
		if (fault == 4)
			UT_ASSERT(DatabasePath == NULL);
		if (!caught)
			UT_ASSERT_EQ(result,
						 success ? RF_PAGE_PROOF_DETAIL_OK : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
		UT_ASSERT_EQ(cold_cache_calls, success || fault == 4 || fault == 5 ? 1 : 0);
		UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, success ? 1 : 0);
		if (success) {
			UT_ASSERT(cold_clog_verified_order < cold_cache_order);
			UT_ASSERT(cold_cache_order < prepare_apply.terminal_projection_order);
		}
		if (fault == 2 || fault >= 6)
			UT_ASSERT_EQ(canonical_writes, 0);
		UT_ASSERT_EQ(owner->protected_set_complete, success);
		UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0
				  && owner->undo_headers == NULL && !owner->protected_set_active);
		free(owner);
		UT_ASSERT_EQ(fclose(canonical_file), 0);
		canonical_file = NULL;
		DatabasePath = NULL;
		cold_cache_test = cold_cache_throw = cold_cache_expire = false;
		cold_clog_test = cold_clog_denied = cold_clog_apply_fail = false;
		canonical_expected_xid = 818;
		canonical_expected_wrap = 8;
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_completion_syncs_source_and_executes_all_effects_before_terminal)
{
	for (int fault = 0; fault < 13; fault++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(300);
		ClusterRecoverySerialGuard guard = { 0 };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 *owner = calloc(1, sizeof(*owner));
		RfSideOnlineOperationV1 operation;
		FakeXactRecord fake;
		PGAlignedBlock initial;
		volatile bool caught = false;
		RfPageProofDetailV1 result = RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
		bool success = fault < 5;
		bool cache = fault == 1 || fault >= 4;
		bool stats = fault != 2;

		guard.mode
			= fault == 10 ? CLUSTER_RECOVERY_SERIAL_ONLINE : CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
		undo_header_bind(&fake, 0);
		undo_header_feed(plan, &fake, 100);
		cold_completion_commit(&fake, cache, stats, fault >= 2);
		undo_header_feed(plan, &fake, 200);
		if (rf_side_online_plan_seal_v1(plan) != RF_PAGE_PROOF_DETAIL_OK) {
			UT_ASSERT(false);
			rf_side_online_plan_destroy_v1(&plan);
			free(owner);
			return;
		}
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
		if (fault == 11) {
			xl_xact_stats_item bad = { PGSTAT_KIND_WAL, 5, 16388 };
			uint32 offset = MinSizeOfXactCommit + sizeof(xl_xact_xinfo) + sizeof(xl_xact_dbinfo)
							+ sizeof(int);
			memcpy((uint8 *)operation.owned_payload + offset, &bad, sizeof(bad));
		}
		reset_prepare_apply();
		canonical_file = tmpfile();
		UT_ASSERT(canonical_file != NULL && owner != NULL);
		UT_ASSERT_EQ(ftruncate(fileno(canonical_file), UNDO_SEGMENT_SIZE_BYTES), 0);
		cluster_undo_segment_make_header_bytes(513, 3, initial.data);
		UT_ASSERT(pwrite(fileno(canonical_file), initial.data, BLCKSZ, 0) == BLCKSZ);
		UT_ASSERT_EQ(fsync(fileno(canonical_file)), 0);
		canonical_writes = canonical_syncs = 0;
		canonical_expected_xid = 802;
		canonical_expected_wrap = 7;
		cold_completion_test = cold_clog_test = true;
		cold_cache_test = cache;
		cold_cache_init = false;
		cold_source_calls = cold_source_order = cold_stats_calls = cold_stats_order = cold_stats_gc
			= 0;
		cold_cache_calls = cold_cache_order = cold_clog_applies = cold_clog_verifies
			= cold_clog_verified_order = 0;
		cold_source_ready = false;
		cold_source_fail = fault == 5;
		cold_source_throw = fault == 6;
		cold_stats_throw = fault == 7;
		cold_stats_expire = fault == 8;
		cold_clog_apply_fail = fault == 9;
		cold_cache_throw = fault == 12;
		DatabasePath = NULL;
		UT_ASSERT(rf_side_online_production_owner_init_v1(owner, &authority,
														  canonical_authority_fresh, 19, true));
		UT_ASSERT(rf_side_online_production_bind_undo_v1(owner, &authority));
		PG_TRY();
		{
			result = rf_side_online_production_apply_v1(plan, owner);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 6 || fault == 7 || fault == 12);
		if (!caught)
			UT_ASSERT_EQ(result,
						 success ? RF_PAGE_PROOF_DETAIL_OK : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
		UT_ASSERT_EQ(cold_source_calls, fault == 10 || fault == 11 ? 0 : 1);
		UT_ASSERT_EQ(cold_stats_calls, (success && stats) || fault == 8 ? 3 : fault == 7 ? 1 : 0);
		UT_ASSERT_EQ(cold_stats_gc, cold_stats_calls == 3 ? 1 : 0);
		UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, success ? 1 : 0);
		if (success) {
			UT_ASSERT(cold_source_order < cold_clog_verified_order);
			if (cache)
				UT_ASSERT(cold_clog_verified_order < cold_cache_order);
			if (stats) {
				UT_ASSERT(cold_clog_verified_order < cold_stats_order);
				if (cache)
					UT_ASSERT(cold_cache_order < cold_stats_order);
				UT_ASSERT(cold_stats_order < prepare_apply.terminal_projection_order);
			}
		}
		if (fault == 5 || fault == 6 || fault == 10 || fault == 11)
			UT_ASSERT_EQ(canonical_writes, 0);
		UT_ASSERT_EQ(owner->protected_set_complete, success);
		UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0
				  && owner->undo_headers == NULL && !owner->protected_set_active);
		UT_ASSERT(DatabasePath == NULL);
		free(owner);
		UT_ASSERT_EQ(fclose(canonical_file), 0);
		canonical_file = NULL;
		cold_completion_test = cold_source_ready = cold_source_fail = cold_source_throw = false;
		cold_stats_throw = cold_stats_expire = cold_cache_test = cold_cache_throw = false;
		cold_cache_calls = 0;
		cold_clog_test = cold_clog_denied = cold_clog_apply_fail = false;
		canonical_expected_xid = 818;
		canonical_expected_wrap = 8;
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_owner_persists_native_clog_before_commit_projection)
{
	/* Same original source/TT consumer: no alternate decoder or fake commit. */
	for (int fault = 0; fault < 9; fault++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(300);
		ClusterRecoverySerialGuard guard = { 0 };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 *owner = calloc(1, sizeof(*owner));
		FakeXactRecord fake;
		PGAlignedBlock initial;
		volatile bool caught = false;
		RfPageProofDetailV1 result = RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;

		guard.mode
			= fault >= 7 ? CLUSTER_RECOVERY_SERIAL_ONLINE : CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
		undo_header_bind(&fake, 0);
		undo_header_feed(plan, &fake, 100);
		undo_header_commit(&fake, 0, 802);
		if (fault == 8) {
			xl_xact_xinfo xinfo;

			fake.u.decoded.max_block_id = -1;

			/* ONLINE cannot use the cold recovery-lock completion owner. */
			memcpy(&xinfo, fake.data + sizeof(xl_xact_commit), sizeof(xinfo));
			xinfo.xinfo |= XACT_XINFO_HAS_AE_LOCKS;
			memcpy(fake.data + sizeof(xl_xact_commit), &xinfo, sizeof(xinfo));
		}
		undo_header_feed(plan, &fake, 200);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		reset_prepare_apply();
		canonical_file = tmpfile();
		UT_ASSERT(canonical_file != NULL && owner != NULL);
		UT_ASSERT_EQ(ftruncate(fileno(canonical_file), UNDO_SEGMENT_SIZE_BYTES), 0);
		cluster_undo_segment_make_header_bytes(513, 3, initial.data);
		UT_ASSERT(pwrite(fileno(canonical_file), initial.data, BLCKSZ, 0) == BLCKSZ);
		UT_ASSERT_EQ(fsync(fileno(canonical_file)), 0);
		canonical_writes = canonical_syncs = 0;
		canonical_expected_xid = 802;
		canonical_expected_wrap = 7;
		cold_clog_test = true;
		cold_clog_denied = fault == 1;
		cold_clog_apply_fail = fault == 2;
		cold_clog_verify_fail = fault == 3;
		cold_clog_throw = fault == 4;
		cold_clog_stale_after_verify = fault == 5;
		canonical_file_io_error = fault == 6;
		cold_clog_applies = cold_clog_verifies = cold_clog_verified_order = 0;
		UT_ASSERT(rf_side_online_production_owner_init_v1(owner, &authority,
														  canonical_authority_fresh, 19, true));
		UT_ASSERT(rf_side_online_production_bind_undo_v1(owner, &authority));
		PG_TRY();
		{
			result = rf_side_online_production_apply_v1(plan, owner);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 4);
		if (!caught)
			UT_ASSERT_EQ(result, fault == 0 || fault == 7 ? RF_PAGE_PROOF_DETAIL_OK
														  : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
		UT_ASSERT_EQ(cold_clog_applies, fault == 1 || fault >= 6 ? 0 : 1);
		UT_ASSERT_EQ(cold_clog_verifies, fault == 0 || fault == 3 || fault == 5 ? 1 : 0);
		UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, fault == 0 || fault == 7 ? 1 : 0);
		if (fault == 0)
			UT_ASSERT(cold_clog_verified_order < prepare_apply.terminal_projection_order);
		if (fault == 1 || fault == 8)
			UT_ASSERT_EQ(canonical_writes, 0);
		UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0
				  && owner->undo_headers == NULL && !owner->protected_set_active);
		UT_ASSERT_EQ(owner->protected_set_complete, fault == 0 || fault == 7);
		free(owner);
		UT_ASSERT_EQ(fclose(canonical_file), 0);
		canonical_file = NULL;
		canonical_file_io_error = false;
		cold_clog_test = cold_clog_denied = cold_clog_apply_fail = cold_clog_verify_fail = false;
		cold_clog_throw = cold_clog_stale_after_verify = false;
		canonical_expected_xid = 818;
		canonical_expected_wrap = 8;
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_real_plan_owner_and_xact_install_final_header_before_older_commit_projection)
{
	for (int scenario = 0; scenario < 4; scenario++) {
		bool conflict = (scenario & 1) != 0;
		RfSideOnlinePlanV1 *plan = undo_repeated_slot_plan_sources(conflict, scenario >= 2);
		ClusterThreadRecoveryAuthorityV1 authority = { .duty = &canonical_duty };
		RfSideOnlineProductionOwnerV1 owner;
		PGAlignedBlock initial, after;

		reset_prepare_apply();
		canonical_file = tmpfile();
		UT_ASSERT(canonical_file != NULL);
		UT_ASSERT_EQ(ftruncate(fileno(canonical_file), UNDO_SEGMENT_SIZE_BYTES), 0);
		cluster_undo_segment_make_header_bytes(513, 3, initial.data);
		UT_ASSERT(pwrite(fileno(canonical_file), initial.data, BLCKSZ, 0) == BLCKSZ);
		UT_ASSERT_EQ(fsync(fileno(canonical_file)), 0);
		canonical_writes = canonical_syncs = 0;
		UT_ASSERT(rf_side_online_production_owner_init_v1(&owner, &authority,
														  canonical_authority_fresh, 19, true));
		UT_ASSERT(rf_side_online_production_bind_undo_v1(&owner, &authority));
		UT_ASSERT_EQ(rf_side_online_production_preflight_v1(plan, &owner),
					 conflict ? RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE : RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(canonical_writes, 0);
		UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
		UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner),
					 conflict ? RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE : RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(canonical_writes, conflict ? 0 : 1);
		UT_ASSERT_EQ(canonical_syncs, conflict ? 0 : 1);
		UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, conflict ? 0 : 2);
		UT_ASSERT_EQ(prepare_apply.tt_stamps, 0);
		UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 0) == BLCKSZ);
		if (conflict)
			UT_ASSERT(memcmp(after.data, initial.data, BLCKSZ) == 0);
		else {
			UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner), RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_EQ(canonical_writes, 1);
			UT_ASSERT_EQ(canonical_syncs, 2);
			UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 4);
		}
		UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0
				  && owner.undo_headers == NULL);
		UT_ASSERT_EQ(fclose(canonical_file), 0);
		canonical_file = NULL;
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_physical_undo_is_durable_before_canonical_tt_publication)
{
	for (int fault = 0; fault < 4; fault++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(fault == 1 ? 400 : 500);
		ClusterThreadRecoveryAuthorityV1 authority = { .duty = &canonical_duty };
		RfSideOnlineProductionOwnerV1 owner;
		FakeXactRecord fake;
		PGAlignedBlock initial, after;
		XLogRecPtr begin = 100;
		struct {
			xl_undo_block_write record;
			uint8 image[BLCKSZ];
		} fpi = { 0 };

		undo_header_bind(&fake, 0);
		undo_header_feed(plan, &fake, begin);
		begin += 100;
		if (fault != 1) {
			fpi.record.instance = 3;
			fpi.record.segment_id = 513;
			fpi.record.block_no = 9;
			fpi.record.has_fpi = 1;
			memset(fpi.image, 0x31, sizeof(fpi.image));
			make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE, &fpi,
								   sizeof(fpi));
			undo_header_feed(plan, &fake, begin);
			begin += 100;
		}
		make_undo_delta(&fake);
		undo_header_feed(plan, &fake, begin);
		begin += 100;
		undo_header_commit(&fake, 0, 802);
		undo_header_feed(plan, &fake, begin);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		reset_prepare_apply();
		canonical_file = tmpfile();
		UT_ASSERT(canonical_file != NULL);
		UT_ASSERT_EQ(ftruncate(fileno(canonical_file), UNDO_SEGMENT_SIZE_BYTES), 0);
		cluster_undo_segment_make_header_bytes(513, 3, initial.data);
		UT_ASSERT(pwrite(fileno(canonical_file), initial.data, BLCKSZ, 0) == BLCKSZ);
		UT_ASSERT_EQ(fsync(fileno(canonical_file)), 0);
		canonical_writes = canonical_syncs = 0;
		canonical_expected_xid = 802;
		canonical_expected_wrap = 7;
		canonical_check_block = true;
		canonical_fail_data_write = fault == 2;
		canonical_corrupt_data_write = fault == 3;
		UT_ASSERT(rf_side_online_production_owner_init_v1(&owner, &authority,
														  canonical_authority_fresh, 19, true));
		UT_ASSERT(rf_side_online_production_bind_undo_v1(&owner, &authority));
		UT_ASSERT_EQ(rf_side_online_production_preflight_v1(plan, &owner),
					 fault == 1 ? RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE : RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(canonical_writes, 0);
		UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner),
					 fault == 0 ? RF_PAGE_PROOF_DETAIL_OK : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
		UT_ASSERT_EQ(canonical_writes, fault == 0 ? 2 : fault >= 2 ? 1 : 0);
		UT_ASSERT_EQ(canonical_syncs, fault == 0 ? 2 : fault == 3 ? 1 : 0);
		UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, fault == 0 ? 1 : 0);
		UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 0) == BLCKSZ);
		if (fault != 0)
			UT_ASSERT(memcmp(after.data, initial.data, BLCKSZ) == 0);
		UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0
				  && owner.undo_headers == NULL);
		UT_ASSERT_EQ(fclose(canonical_file), 0);
		canonical_file = NULL;
		canonical_check_block = canonical_fail_data_write = canonical_corrupt_data_write = false;
		canonical_expected_xid = 818;
		canonical_expected_wrap = 8;
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_init_owner_repairs_short_segment_before_header_and_commit)
{
	for (int partial = 0; partial < 5; partial++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(500);
		ClusterThreadRecoveryAuthorityV1 authority = { .duty = &canonical_duty };
		RfSideOnlineProductionOwnerV1 owner;
		FakeXactRecord fake;
		PGAlignedBlock after;
		struct {
			xl_cluster_undo_segment_init record;
			uint8 image[BLCKSZ];
		} init = { 0 };
		struct {
			xl_undo_block_write record;
			uint8 image[BLCKSZ];
		} fpi = { 0 };

		init.record.instance = fpi.record.instance = 3;
		init.record.segment_id = fpi.record.segment_id = 513;
		cluster_undo_segment_make_header_bytes(513, 3, (char *)init.image);
		make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_INIT, &init,
							   sizeof(init));
		undo_header_feed(plan, &fake, 100);
		undo_header_bind(&fake, 0);
		undo_header_feed(plan, &fake, 200);
		fpi.record.block_no = 9;
		fpi.record.has_fpi = 1;
		memset(fpi.image, 0x31, BLCKSZ);
		make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE, &fpi, sizeof(fpi));
		undo_header_feed(plan, &fake, 300);
		undo_header_commit(&fake, 0, 802);
		undo_header_feed(plan, &fake, 400);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		reset_prepare_apply();
		canonical_file = tmpfile();
		UT_ASSERT(canonical_file != NULL);
		if (partial) {
			UT_ASSERT_EQ(
				ftruncate(fileno(canonical_file), partial == 1 ? 17 : UNDO_SEGMENT_SIZE_BYTES), 0);
			UT_ASSERT(pwrite(fileno(canonical_file), init.image, partial == 1 ? 17 : BLCKSZ, 0)
					  == (partial == 1 ? 17 : BLCKSZ));
		}
		canonical_writes = canonical_syncs = 0;
		canonical_file_io_error = partial == 3;
		canonical_materialize_failure = partial == 4;
		canonical_expected_xid = 802;
		canonical_expected_wrap = 7;
		UT_ASSERT(rf_side_online_production_owner_init_v1(&owner, &authority,
														  canonical_authority_fresh, 19, true));
		UT_ASSERT(rf_side_online_production_bind_undo_v1(&owner, &authority));
		UT_ASSERT_EQ(rf_side_online_production_preflight_v1(plan, &owner),
					 partial == 3 ? RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE : RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(canonical_writes, 0);
		UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner),
					 partial >= 3 ? RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE : RF_PAGE_PROOF_DETAIL_OK);
		if (partial >= 3) {
			UT_ASSERT_EQ(canonical_writes, 0);
			UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
			canonical_file_io_error = canonical_materialize_failure = false;
			UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner), RF_PAGE_PROOF_DETAIL_OK);
		}
		UT_ASSERT_EQ(canonical_writes, 2);
		UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 1);
		UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 9 * BLCKSZ) == BLCKSZ);
		UT_ASSERT_EQ(((UndoBlockHeader *)after.data)->block_lsn, 400);
		UT_ASSERT_EQ((uint8)after.data[128], 0x31);
		UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(canonical_writes, 2);
		UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 2);
		UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0
				  && owner.undo_headers == NULL);
		UT_ASSERT_EQ(fclose(canonical_file), 0);
		canonical_file = NULL;
		canonical_expected_xid = 818;
		canonical_expected_wrap = 8;
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_reuse_owner_covers_retired_commit_only_after_new_physical_and_tt)
{
	RfSideOnlinePlanV1 *plan = space_online_plan(800);
	ClusterThreadRecoveryAuthorityV1 authority = { .duty = &canonical_duty };
	RfSideOnlineProductionOwnerV1 owner;
	FakeXactRecord fake;
	PGAlignedBlock initial, after;
	xl_undo_tt_slot_bind bind;
	xl_xact_tt_commit commit;
	uint32 offset;
	struct {
		xl_undo_segment_reuse record;
		uint8 page[BLCKSZ];
	} reuse = { 0 };
	struct {
		xl_undo_block_write record;
		uint8 page[BLCKSZ];
	} fpi = { 0 };

	undo_header_bind(&fake, 0);
	undo_header_feed(plan, &fake, 100);
	undo_header_commit(&fake, 0, 802);
	undo_header_feed(plan, &fake, 200);
	reuse.record.instance = 3;
	reuse.record.segment_id = 513;
	reuse.record.new_generation = 1;
	cluster_undo_segment_make_header_bytes(513, 3, (char *)reuse.page);
	((UndoSegmentHeaderData *)reuse.page)->wrap_count = 1;
	make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_REUSE, &reuse,
						   sizeof(reuse));
	undo_header_feed(plan, &fake, 300);
	undo_header_bind(&fake, 1);
	memcpy(&bind, fake.data, sizeof(bind));
	bind.xid = 818;
	bind.wrap = 8;
	memcpy(fake.data, &bind, sizeof(bind));
	undo_header_feed(plan, &fake, 400);
	fpi.record.instance = 3;
	fpi.record.segment_id = 513;
	fpi.record.block_no = 9;
	fpi.record.has_fpi = 1;
	memset(fpi.page, 0x31, BLCKSZ);
	make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE, &fpi, sizeof(fpi));
	undo_header_feed(plan, &fake, 500);
	make_undo_delta(&fake);
	undo_header_feed(plan, &fake, 600);
	undo_header_commit(&fake, 1, 818);
	offset = fake.u.decoded.main_data_len - sizeof(commit);
	memcpy(&commit, fake.data + offset, sizeof(commit));
	commit.wrap = 8;
	memcpy(fake.data + offset, &commit, sizeof(commit));
	undo_header_feed(plan, &fake, 700);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	reset_prepare_apply();
	canonical_file = tmpfile();
	UT_ASSERT(canonical_file != NULL);
	UT_ASSERT_EQ(ftruncate(fileno(canonical_file), UNDO_SEGMENT_SIZE_BYTES), 0);
	cluster_undo_segment_make_header_bytes(513, 3, initial.data);
	UT_ASSERT(pwrite(fileno(canonical_file), initial.data, BLCKSZ, 0) == BLCKSZ);
	canonical_writes = canonical_syncs = 0;
	canonical_check_block = true;
	canonical_expected_block_lsn = 700;
	UT_ASSERT(rf_side_online_production_owner_init_v1(&owner, &authority, canonical_authority_fresh,
													  19, true));
	UT_ASSERT(rf_side_online_production_bind_undo_v1(&owner, &authority));
	UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(canonical_writes, 2);
	UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 2);
	UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 0) == BLCKSZ);
	UT_ASSERT_EQ(((UndoSegmentHeaderData *)after.data)->wrap_count, 1);
	UT_ASSERT_EQ(((UndoSegmentHeaderData *)after.data)->tt_slots[4].xid, 818);
	UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(canonical_writes, 2);
	UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 4);
	UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0 && owner.undo_headers == NULL);
	UT_ASSERT_EQ(fclose(canonical_file), 0);
	canonical_file = NULL;
	canonical_check_block = false;
	canonical_expected_block_lsn = 400;
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_physical_only_cut_and_reuse_anchor_boundary)
{
	for (int reuse = 0; reuse < 2; reuse++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(reuse ? 400 : 300);
		FakeXactRecord fake;
		RfSideUndoBlockImageV1 image, saved;
		struct {
			xl_undo_block_write record;
			uint8 image[BLCKSZ];
		} fpi = { 0 };
		XLogRecPtr begin = 200;

		fpi.record.instance = 3;
		fpi.record.segment_id = 513;
		fpi.record.block_no = 9;
		fpi.record.has_fpi = 1;
		memset(fpi.image, 0x31, BLCKSZ);
		make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE, &fpi, sizeof(fpi));
		undo_header_feed(plan, &fake, 100);
		if (reuse) {
			struct {
				xl_undo_segment_reuse record;
				uint8 image[BLCKSZ];
			} record = { 0 };

			record.record.instance = 3;
			record.record.segment_id = 513;
			record.record.new_generation = 1;
			cluster_undo_segment_make_header_bytes(513, 3, (char *)record.image);
			((UndoSegmentHeaderData *)record.image)->wrap_count = 1;
			make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_REUSE, &record,
								   sizeof(record));
			undo_header_feed(plan, &fake, begin);
			begin += 100;
		}
		make_undo_delta(&fake);
		undo_header_feed(plan, &fake, begin);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		memset(&saved, 0x91, sizeof(saved));
		image = saved;
		UT_ASSERT_EQ(rf_side_online_plan_prepare_undo_block_v1(plan, 3, 513, 9, &image),
					 reuse ? RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE : RF_PAGE_PROOF_DETAIL_OK);
		if (reuse)
			UT_ASSERT(memcmp(&image, &saved, sizeof(image)) == 0);
		else {
			RfSideOnlineProductionOwnerV1 owner;
			ClusterThreadRecoveryAuthorityV1 authority = { .duty = &canonical_duty };
			PGAlignedBlock initial, actual;

			canonical_file = tmpfile();
			UT_ASSERT(canonical_file != NULL);
			UT_ASSERT_EQ(ftruncate(fileno(canonical_file), UNDO_SEGMENT_SIZE_BYTES), 0);
			cluster_undo_segment_make_header_bytes(513, 3, initial.data);
			UT_ASSERT(pwrite(fileno(canonical_file), initial.data, BLCKSZ, 0) == BLCKSZ);
			canonical_writes = canonical_syncs = 0;
			UT_ASSERT(rf_side_online_production_owner_init_v1(&owner, &authority,
															  canonical_authority_fresh, 19, true));
			UT_ASSERT(rf_side_online_production_bind_undo_v1(&owner, &authority));
			UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner), RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_EQ(canonical_writes, 1);
			UT_ASSERT_EQ(canonical_syncs, 1);
			UT_ASSERT(pread(fileno(canonical_file), actual.data, BLCKSZ, 0) == BLCKSZ);
			UT_ASSERT(memcmp(actual.data, initial.data, BLCKSZ) == 0);
			UT_ASSERT(pread(fileno(canonical_file), actual.data, BLCKSZ, 9 * BLCKSZ) == BLCKSZ);
			UT_ASSERT(memcmp(actual.data, image.page.data, BLCKSZ) == 0);
			UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner), RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_EQ(canonical_writes, 1);
			UT_ASSERT_EQ(canonical_syncs, 2);
			UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0
					  && owner.undo_headers == NULL);
			UT_ASSERT_EQ(fclose(canonical_file), 0);
			canonical_file = NULL;
		}
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_folded_commit_segment_must_belong_to_source_origin)
{
	FakeXactRecord fake;
	xl_xact_tt_commit delta;
	RfSideXactOperationV1 operation;
	uint32 offset;

	make_commit(&fake, 802, 999, 12345, false);
	offset = fake.u.decoded.main_data_len - sizeof(delta);
	memcpy(&delta, fake.data + offset, sizeof(delta));
	delta.segment_id = 9;
	memcpy(fake.data + offset, &delta, sizeof(delta));
	UT_ASSERT(!rf_side_xact_decode_v1(&fake.reader, UINT64_C(0x11223344), 3, &operation));
}

UT_TEST(test_sealed_source_match_requires_observed_namespace_and_exact_cut)
{
	RfSideOnlinePlanV1 *p = space_online_plan(200);
	FakeXactRecord fake;
	RfContributorStreamCutV1 cut = { 0 };
	uint8 uuid[16];

	memset(uuid, 0x44, sizeof(uuid));
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 200;
	make_undo_delta(&fake);
	undo_header_feed(p, &fake, 100);
	UT_ASSERT(!rf_side_online_plan_source_matches_v1(p, UINT64_C(0x11223344), uuid, &cut));
	UT_ASSERT(rf_side_online_plan_bind_database_v1(p, 42));
	UT_ASSERT(!rf_side_online_plan_source_matches_v1(p, UINT64_C(0x11223344), uuid, &cut));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(p), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_source_matches_v1(p, UINT64_C(0x11223344), uuid, &cut));
	UT_ASSERT(rf_side_online_plan_recovery_source_matches_v1(p, UINT64_C(0x11223344), uuid, &cut));
	for (int fault = 0; fault < 6; fault++) {
		RfContributorStreamCutV1 bad = cut;
		if (fault == 0)
			bad.failed_thread++;
		if (fault == 1)
			bad.timeline_id++;
		if (fault == 2)
			bad.scan_begin_inclusive++;
		if (fault == 3)
			bad.scan_end_exclusive++;
		if (fault == 4)
			bad.flags = RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY;
		if (fault == 5)
			uuid[0] ^= 1;
		UT_ASSERT(!rf_side_online_plan_source_matches_v1(p, UINT64_C(0x11223344), uuid, &bad));
		UT_ASSERT(
			!rf_side_online_plan_recovery_source_matches_v1(p, UINT64_C(0x11223344), uuid, &bad));
	}
	rf_side_online_plan_destroy_v1(&p);
	p = space_online_plan(200);
	make_undo_delta(&fake);
	undo_header_feed(p, &fake, 100);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(p), RF_PAGE_PROOF_DETAIL_OK);
	memset(uuid, 0x44, sizeof(uuid));
	UT_ASSERT(!rf_side_online_plan_source_matches_v1(p, UINT64_C(0x11223344), uuid, &cut));
	rf_side_online_plan_destroy_v1(&p);
	{
		RfSideOnlinePlanRequestV1 request = { 0 };
		request.system_identifier = UINT64_C(0x11223344);
		memcpy(request.storage_uuid, uuid, 16);
		request.physical_cuts = &cut;
		request.participant_count = 1;
		request.purpose = RF_SIDE_PLAN_PI_CONTRIBUTIONS;
		UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &p), RF_PAGE_PROOF_DETAIL_OK);
		make_undo_delta(&fake);
		undo_header_feed(p, &fake, 100);
		UT_ASSERT(rf_side_online_plan_bind_database_v1(p, 42));
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(p), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(rf_side_online_plan_source_matches_v1(p, UINT64_C(0x11223344), uuid, &cut));
		UT_ASSERT(
			!rf_side_online_plan_recovery_source_matches_v1(p, UINT64_C(0x11223344), uuid, &cut));
		rf_side_online_plan_destroy_v1(&p);
	}
}

UT_TEST(test_multixact_retired_page_requires_exact_later_sealed_truncate)
{
	for (int scenario = 0; scenario < 4; scenario++) {
		bool earlier = scenario == 1;
		bool crossing = scenario == 2;
		FakeXactRecord create_fake, truncate_fake;
		RfDetachedRecordPlanV1 create_plan, truncate_plan;
		RfPageOnlineRecordIdentityV1 create_identity, truncate_identity;
		RfSideOnlinePlanRequestV1 request = { 0 };
		RfContributorStreamCutV1 cut = { 0 };
		RfSideOnlinePlanV1 *plan = NULL;
		PGAlignedBlock payload;
		xl_multixact_create *create = (xl_multixact_create *)payload.data;
		xl_multixact_truncate trunc = { 0 };
		uint8 uuid[16];
		XLogRecPtr begin = earlier ? 200 : 100;

		memset(uuid, 0x45, sizeof(uuid));
		memset(&payload, 0, sizeof(payload));
		create->mid = crossing ? (BLCKSZ / sizeof(MultiXactOffset)) * 32 - 1 : 17;
		create->moff = crossing ? (BLCKSZ / 20) * 4 * 32 - 1 : 71;
		create->nmembers = 2;
		create->members[0].xid = 800;
		create->members[0].status = MultiXactStatusForShare;
		create->members[1].xid = 816;
		create->members[1].status = MultiXactStatusForShare;
		make_projection_record(&create_fake, RM_MULTIXACT_ID, XLOG_MULTIXACT_CREATE_ID,
							   payload.data, SizeOfMultiXactCreate + 2 * sizeof(MultiXactMember));
		trunc.oldestMultiDB = 1;
		trunc.startTruncOff = 1;
		trunc.endTruncOff = (BLCKSZ / sizeof(MultiXactOffset)) * 32 + (scenario == 3 ? 0 : 1);
		trunc.endTruncMemb = (BLCKSZ / 20) * 4 * 32;
		make_projection_record(&truncate_fake, RM_MULTIXACT_ID, XLOG_MULTIXACT_TRUNCATE_ID, &trunc,
							   SizeOfMultiXactTruncate);
		create_identity = make_identity(&create_fake, uuid);
		truncate_identity = make_identity(&truncate_fake, uuid);
		set_identity_range(&create_fake, &create_identity, begin, begin + 100);
		set_identity_range(&truncate_fake, &truncate_identity, earlier ? 100 : 200,
						   earlier ? 200 : 300);
		create_plan = make_projection_record_plan(&create_fake);
		truncate_plan = make_projection_record_plan(&truncate_fake);
		cut.failed_thread = 3;
		cut.timeline_id = 7;
		cut.scan_begin_inclusive = 100;
		cut.scan_end_exclusive = 300;
		cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		request.system_identifier = create_identity.record.system_identifier;
		memcpy(request.storage_uuid, uuid, sizeof(uuid));
		request.physical_cuts = &cut;
		request.participant_count = 1;
		UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(
			rf_side_online_plan_feed_record_v1(plan, earlier ? &truncate_plan : &create_plan,
											   earlier ? &truncate_identity : &create_identity),
			RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(
			rf_side_online_plan_feed_record_v1(plan, earlier ? &create_plan : &truncate_plan,
											   earlier ? &create_identity : &truncate_identity),
			RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(
			!rf_side_online_plan_multixact_page_retired_v1(plan, 3, begin, begin + 100, false, 0));
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_multixact_page_retired_v1(plan, 3, begin, begin + 100,
																   false, crossing ? 31 : 0),
					 !earlier && scenario != 3);
		UT_ASSERT_EQ(rf_side_online_plan_multixact_page_retired_v1(plan, 3, begin, begin + 100,
																   true, crossing ? 31 : 0),
					 !earlier);
		UT_ASSERT(
			!rf_side_online_plan_multixact_page_retired_v1(plan, 3, begin, begin + 100, false, 32));
		UT_ASSERT(
			!rf_side_online_plan_multixact_page_retired_v1(plan, 3, begin, begin + 100, true, 32));
		UT_ASSERT(
			!rf_side_online_plan_multixact_page_retired_v1(plan, 2, begin, begin + 100, false, 0));
		UT_ASSERT(
			!rf_side_online_plan_multixact_page_retired_v1(plan, 3, begin, begin + 101, false, 0));
		UT_ASSERT(
			!rf_side_online_plan_multixact_page_retired_v1(plan, 3, begin, begin + 100, false, 1));
		UT_ASSERT(!rf_side_online_plan_multixact_page_retired_v1(plan, 3, begin, begin + 100, true,
																 UINT32_MAX));
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_multi_origin_side_selection_keeps_exact_source_and_original_plan)
{
	RfSideOnlinePlanRequestV1 request = { 0 };
	RfContributorStreamCutV1 cuts[3] = { { 0 } };
	RfSideOnlinePlanV1 *plan = NULL;
	RfSideOnlineApplyOpsV1 ops = { 0 };
	ApplyCapture capture = { 0 };
	FakeXactRecord fake;
	RfPageOnlineRecordIdentityV1 identity;
	RfDetachedRecordPlanV1 record_plan;
	int page = 17;

	request.system_identifier = UINT64_C(0x11223344);
	memset(request.storage_uuid, 0x45, 16);
	request.participant_count = 3;
	request.physical_cuts = cuts;
	for (uint32 i = 0; i < 3; i++) {
		cuts[i].failed_thread = i + 3;
		cuts[i].timeline_id = 7;
		cuts[i].flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		cuts[i].scan_begin_inclusive = 100;
		cuts[i].scan_end_exclusive = 200;
	}
	/* Third origin has a physically empty input, not a missing origin. */
	cuts[2].scan_end_exclusive = 100;
	cuts[2].flags |= RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	if (plan == NULL)
		return;
	for (uint32 i = 0; i < 2; i++) {
		make_projection_record(&fake, RM_MULTIXACT_ID, XLOG_MULTIXACT_ZERO_OFF_PAGE, &page,
							   sizeof(page));
		identity = make_identity(&fake, request.storage_uuid);
		identity.participant_index = i;
		identity.record.origin_thread = i + 3;
		record_plan = make_projection_record_plan(&fake);
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record_plan, &identity),
					 RF_PAGE_PROOF_DETAIL_OK);
	}
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_source_matches_v1(plan, request.system_identifier,
													request.storage_uuid, &cuts[0]));
	UT_ASSERT(rf_side_online_plan_source_matches_v1(plan, request.system_identifier,
													request.storage_uuid, &cuts[1]));
	UT_ASSERT_EQ(rf_side_online_plan_origin_operation_count_v1(plan, 0), 2);
	UT_ASSERT_EQ(rf_side_online_plan_origin_operation_count_v1(plan, 3), 1);
	UT_ASSERT_EQ(rf_side_online_plan_origin_operation_count_v1(plan, 4), 1);
	UT_ASSERT_EQ(rf_side_online_plan_origin_operation_count_v1(plan, 5), 0);
	UT_ASSERT_EQ(rf_side_online_plan_origin_operation_count_v1(plan, 6), UINT32_MAX);
	ops.arg = &capture;
	ops.begin_protected_set = capture_begin;
	ops.end_protected_set = capture_end;
	ops.preflight_projection = accept_preflight;
	ops.apply_projection = capture_apply_projection;
	for (uint16 origin = 3; origin <= 6; origin++) {
		memset(&capture, 0, sizeof(capture));
		ops.source_thread = origin;
		UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &ops),
					 origin == 6 ? RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT : RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(capture.projection_count, origin < 5 ? 1 : 0);
		UT_ASSERT_EQ(capture.projection_origin, origin < 5 ? origin : 0);
		UT_ASSERT_EQ(capture.begin_count, origin < 5 ? 1 : 0);
		UT_ASSERT_EQ(capture.end_count, capture.begin_count);
	}
	/* The selector leaves the complete immutable input available. */
	UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 2);
	UT_ASSERT(rf_side_online_plan_source_matches_v1(plan, request.system_identifier,
													request.storage_uuid, &cuts[0]));
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_space_contribution_census_includes_history_and_every_drop_page)
{
	ClusterSpaceReservationChange advance = space_advance_fixture();
	ClusterSpaceStructureChange drops[2] = { space_drop_fixture(16384), space_drop_fixture(16385) };
	XLogRecPtr redo = 300;
	RfSideOnlinePlanV1 *plan = space_online_plan_redo(300, &redo);
	RfSideSpaceContributionV1 contribution, saved;
	RfSideContributionOwnersV1 owners = { UINT32_MAX, UINT32_MAX }, saved_owners = owners;
	RfSideOnlineOperationV1 op;
	FakeXactRecord fake;
	uint8 wal[CLUSTER_SPACE_RESERVATION_WAL_BYTES];

	reset_prepare_apply();
	memset(&contribution, 0x5a, sizeof(contribution));
	saved = contribution;
	UT_ASSERT_EQ(rf_side_online_plan_space_contribution_count_v1(plan, 0), UINT32_MAX);
	UT_ASSERT(!rf_side_online_plan_space_contribution_v1(plan, 0, 0, &contribution));
	UT_ASSERT(!rf_side_online_plan_contribution_owners_v1(plan, 0, &owners));
	UT_ASSERT(memcmp(&owners, &saved_owners, sizeof(owners)) == 0);
	UT_ASSERT(memcmp(&contribution, &saved, sizeof(saved)) == 0);
	UT_ASSERT(cluster_space_reservation_wal_encode(&advance, wal, sizeof(wal)));
	UT_ASSERT_EQ(space_online_feed(plan, XLOG_SMGR_SPACE_RESERVATION, wal, sizeof(wal), 100, false),
				 RF_PAGE_PROOF_DETAIL_OK);
	make_space_commit(&fake, drops, 2);
	UT_ASSERT_EQ(space_commit_feed(plan, &fake, 200), RF_PAGE_PROOF_DETAIL_OK);
	memset(fake.data, 0xee, sizeof(fake.data));
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_space_target_count_v1(plan), 0);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &op));
	UT_ASSERT(op.history_only);
	UT_ASSERT_EQ(rf_side_online_plan_space_contribution_count_v1(plan, 0), 1);
	UT_ASSERT_EQ(rf_side_online_plan_space_contribution_count_v1(plan, 1), 2);
	assert_contribution_owners(plan, 0, RF_SIDE_CONTRIBUTION_SPACE, 1);
	assert_contribution_owners(plan, 1,
							   RF_SIDE_CONTRIBUTION_SPACE | RF_SIDE_CONTRIBUTION_UNDO_HEADER
								   | RF_SIDE_CONTRIBUTION_TERMINAL
								   | RF_SIDE_CONTRIBUTION_NATIVE_STRUCTURE,
							   2);
	if (rf_side_online_plan_space_contribution_v1(plan, 0, 0, &contribution)) {
		UT_ASSERT_EQ(contribution.page_mask, 2);
		UT_ASSERT_EQ(contribution.result_token[0], 0);
		UT_ASSERT_EQ(contribution.result_token[1], advance.result_token);
		UT_ASSERT_EQ(contribution.result.key.database_incarnation, 42);
		UT_ASSERT(RelFileLocatorEquals(contribution.result.key.locator,
									   advance.result.identity.key.locator));
	} else
		UT_ASSERT(false);
	for (uint32 i = 0; i < 2; i++) {
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &op));
		UT_ASSERT(op.history_only);
		if (rf_side_online_plan_space_contribution_v1(plan, 1, i, &contribution)) {
			UT_ASSERT_EQ(contribution.page_mask, 3);
			UT_ASSERT_EQ(contribution.result_token[0], drops[i].identity.result_token);
			UT_ASSERT_EQ(contribution.result_token[1], drops[i].reservation.result_token);
			UT_ASSERT_EQ(contribution.result.state, CLUSTER_SPACE_IDENTITY_TOMBSTONED);
			UT_ASSERT(RelFileLocatorEquals(contribution.result.key.locator,
										   drops[i].identity.result.key.locator));
		} else
			UT_ASSERT(false);
	}
	saved = contribution;
	UT_ASSERT(!rf_side_online_plan_space_contribution_v1(plan, 1, 2, &contribution));
	UT_ASSERT(!rf_side_online_plan_space_contribution_v1(plan, 2, 0, &contribution));
	UT_ASSERT_EQ(rf_side_online_plan_space_contribution_count_v1(plan, 2), UINT32_MAX);
	UT_ASSERT(!rf_side_online_plan_contribution_owners_v1(plan, 2, &owners));
	UT_ASSERT(!rf_side_online_plan_contribution_owners_v1(NULL, 0, &owners));
	UT_ASSERT(!rf_side_online_plan_contribution_owners_v1(plan, 0, NULL));
	UT_ASSERT(memcmp(&owners, &saved_owners, sizeof(owners)) == 0);
	UT_ASSERT(memcmp(&contribution, &saved, sizeof(saved)) == 0);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_retained_commit_drop_does_not_create_space_mutation_target)
{
	ClusterSpaceStructureChange drop = space_drop_fixture(16384);
	XLogRecPtr redo = 200;
	RfSideOnlinePlanV1 *plan = space_online_plan_redo(200, &redo);
	FakeXactRecord fake;
	RfSideOnlineOperationV1 op;
	RfSideOnlineApplyOpsV1 ops = { 0 };
	ApplyCapture capture = { 0 };
	reset_prepare_apply();
	make_space_commit(&fake, &drop, 1);
	UT_ASSERT_EQ(space_commit_feed(plan, &fake, 100), RF_PAGE_PROOF_DETAIL_OK);
	/* History still belongs to the exact database; skipping redo is not
	 * permission to accept an unrelated namespace. */
	UT_ASSERT(!rf_side_online_plan_bind_database_v1(plan, 43));
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_space_target_count_v1(plan), 0);
	UT_ASSERT_EQ(rf_side_online_plan_origin_operation_count_v1(plan, 3), 0);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &op));
	UT_ASSERT(op.history_only);
	UT_ASSERT_EQ(op.xact.space_drop_count, 1);
	ops.arg = &capture;
	ops.begin_protected_set = capture_begin;
	ops.end_protected_set = capture_end;
	UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &ops), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(capture.begin_count, 0);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_retained_side_history_is_validated_but_never_dispatched)
{
	for (unsigned mode = 0; mode < 4; mode++) {
		RfSideOnlinePlanRequestV1 request = { 0 };
		RfContributorStreamCutV1 cut = { 0 };
		RfSideOnlinePlanV1 *plan = NULL;
		RfSideOnlineOperationV1 op;
		RfSideOnlineApplyOpsV1 ops = { 0 };
		ApplyCapture capture = { 0 };
		FakeXactRecord fake;
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 record;
		XLogRecPtr redo = mode == 0 ? 100 : mode == 1 ? 200 : mode == 2 ? 300 : 150;
		request.system_identifier = UINT64_C(0x11223344);
		memset(request.storage_uuid, 0x42, 16);
		cut.failed_thread = 3;
		cut.timeline_id = 7;
		cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		cut.scan_begin_inclusive = 100;
		cut.scan_end_exclusive = 300;
		request.physical_cuts = &cut;
		request.participant_count = 1;
		request.redo_starts = &redo;
		UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
		redo = 0;
		for (unsigned i = 0; i < 2; i++) {
			(void)make_commit(&fake, 800 + i, 901 + i, 123456, false);
			identity = make_identity(&fake, request.storage_uuid);
			set_identity_range(&fake, &identity, 100 + i * 100, 200 + i * 100);
			record = make_record_plan(&fake);
			UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
						 mode == 3 && i == 0 ? RF_PAGE_PROOF_DETAIL_SOURCE_GAP
											 : RF_PAGE_PROOF_DETAIL_OK);
			if (mode == 3)
				break;
		}
		if (mode == 3) {
			UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
			rf_side_online_plan_destroy_v1(&plan);
			continue;
		}
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 2);
		UT_ASSERT_EQ(rf_side_online_plan_origin_operation_count_v1(plan, 0), 2 - mode);
		UT_ASSERT_EQ(rf_side_online_plan_origin_operation_count_v1(plan, 3), 2 - mode);
		for (unsigned i = 0; i < 2; i++) {
			UT_ASSERT(rf_side_online_plan_operation_v1(plan, i, &op));
			UT_ASSERT_EQ(op.history_only, i < mode);
		}
		ops.arg = &capture;
		ops.begin_protected_set = capture_begin;
		ops.end_protected_set = capture_end;
		if (mode < 2) {
			ops.preflight_xact = accept_preflight;
			ops.apply_xact = capture_apply;
		}
		UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &ops), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(capture.count, 2 - mode);
		UT_ASSERT_EQ(capture.begin_count, mode < 2 ? 1 : 0);
		UT_ASSERT_EQ(capture.end_count, capture.begin_count);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_side_plan_does_not_drop_unowned_page_components)
{
	const uint8 classes[]
		= { RF_PAGE_CLASS_ROUTED_HEADER, RF_PAGE_CLASS_ROUTED_SIDE, RF_PAGE_CLASS_ROUTED_SPACE };
	uint8 uuid[16];

	memset(uuid, 0x44, sizeof(uuid));
	for (unsigned i = 0; i < lengthof(classes); i++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(200);
		FakeXactRecord fake = { 0 };
		RfDetachedRecordPlanV1 record = { 0 };
		RfPageOnlineRecordIdentityV1 identity;

		fake.reader.record = &fake.u.decoded;
		fake.u.decoded.header.xl_rmid = RM_XLOG_ID;
		fake.u.decoded.header.xl_info = XLOG_FPI;
		fake.u.decoded.max_block_id = 0;
		fake.u.decoded.blocks[0].in_use = true;
		fake.u.decoded.blocks[0].forknum = i == 2 ? SPACE_FORKNUM : MAIN_FORKNUM;
		identity = make_identity(&fake, uuid);
		record.source_record = &fake.reader;
		record.route.record_owner = RF_ROUTE_OWNER_PAGE_CODEC;
		record.route.rmid = RM_XLOG_ID;
		record.route.codec_id = RF_ROUTE_CODEC_XLOG_FPI;
		record.component_count = 1;
		record.components[0].page_class = classes[i];
		record.components[0].owner = RF_DETACHED_COMPONENT_SIDE_TYPED;
		record.components[0].before_kind = record.components[0].result_kind = RF_PAGE_STATE_ROUTED;
		record.preflight_complete = true;
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
					 RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
		/* A refusal did not consume the source position or add any operation.
		 * The same cut can still contain a PAGE-owned ordinary component. */
		record.components[0].page_class
			= i == 1 ? RF_PAGE_CLASS_REBUILDABLE_FSM : RF_PAGE_CLASS_ORDINARY;
		record.components[0].owner
			= i == 1 ? RF_DETACHED_COMPONENT_REBUILDABLE : RF_DETACHED_COMPONENT_PAGE_CODEC;
		record.components[0].before_kind = record.components[0].result_kind
			= i == 1 ? RF_PAGE_STATE_REBUILDABLE : RF_PAGE_STATE_PRESENT;
		fake.u.decoded.blocks[0].forknum = i == 1 ? FSM_FORKNUM : MAIN_FORKNUM;
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
					 RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 0);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_retained_generations_refuse_ambiguous_thread_selectors)
{
	RfSideOnlinePlanRequestV1 request = { 0 };
	RfContributorStreamCutV1 cuts[2] = { { 0 } }, changed;
	XLogRecPtr redo[2] = { 200, 100 };
	RfSideOnlinePlanV1 *plan = NULL;
	RfSideOnlineApplyOpsV1 ops = { 0 };
	ApplyCapture capture = { 0 };
	FakeXactRecord fake;
	int page = 17;
	request.system_identifier = UINT64_C(0x11223344);
	memset(request.storage_uuid, 0x45, 16);
	request.participant_count = 2;
	request.physical_cuts = cuts;
	request.redo_starts = redo;
	for (uint32 i = 0; i < 2; i++) {
		cuts[i].failed_thread = 3;
		cuts[i].timeline_id = 7;
		cuts[i].flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		cuts[i].scan_begin_inclusive = 100;
		cuts[i].scan_end_exclusive = 200;
		cuts[i].origin_owner_incarnation = 41 + i;
	}
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	if (plan == NULL)
		return;
	for (uint32 i = 0; i < 2; i++) {
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 record;
		make_projection_record(&fake, RM_MULTIXACT_ID, XLOG_MULTIXACT_ZERO_OFF_PAGE, &page,
							   sizeof(page));
		identity = make_identity(&fake, request.storage_uuid);
		identity.participant_index = i;
		record = make_projection_record_plan(&fake);
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
					 RF_PAGE_PROOF_DETAIL_OK);
	}
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	for (uint32 i = 0; i < 2; i++)
		UT_ASSERT(rf_side_online_plan_source_matches_v1(plan, request.system_identifier,
														request.storage_uuid, &cuts[i]));
	changed = cuts[1];
	changed.origin_owner_incarnation = 0;
	UT_ASSERT(!rf_side_online_plan_source_matches_v1(plan, request.system_identifier,
													 request.storage_uuid, &changed));
	changed.origin_owner_incarnation = 43;
	UT_ASSERT(!rf_side_online_plan_source_matches_v1(plan, request.system_identifier,
													 request.storage_uuid, &changed));
	UT_ASSERT_EQ(rf_side_online_plan_origin_operation_count_v1(plan, 0), 1);
	UT_ASSERT_EQ(rf_side_online_plan_origin_operation_count_v1(plan, 3), UINT32_MAX);
	ops.arg = &capture;
	ops.begin_protected_set = capture_begin;
	ops.end_protected_set = capture_end;
	ops.preflight_projection = accept_preflight;
	ops.apply_projection = capture_apply_projection;
	ops.source_thread = 3;
	UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &ops), RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT);
	UT_ASSERT_EQ(capture.begin_count, 0);
	UT_ASSERT_EQ(capture.projection_count, 0);
	rf_side_online_plan_destroy_v1(&plan);
}

static uint32
native_control_payload(uint8 info, uint8 *payload)
{
	CheckPoint checkpoint = { 0 };
	xl_parameter_change parameters = { 0 };
	xl_restore_point restore = { 0 };
	xl_end_of_recovery end = { 0 };
	xl_overwrite_contrecord overwrite = { 0 };
	Oid oid = 5;
	XLogRecPtr backup = 50;
	checkpoint.ThisTimeLineID = checkpoint.PrevTimeLineID = 7;
	checkpoint.redo = 100;
	checkpoint.nextXid = FullTransactionIdFromU64(20);
	checkpoint.fullPageWrites = true;
	parameters.MaxConnections = parameters.max_locks_per_xact = 20;
	parameters.wal_level = WAL_LEVEL_REPLICA;
	end.ThisTimeLineID = end.PrevTimeLineID = 7;
	overwrite.overwritten_lsn = 50;
#define CONTROL_COPY(value_)                                                                       \
	do {                                                                                           \
		memcpy(payload, &(value_), sizeof(value_));                                                \
		return sizeof(value_);                                                                     \
	} while (0)
	switch (info) {
	case XLOG_CHECKPOINT_SHUTDOWN:
	case XLOG_CHECKPOINT_ONLINE:
		CONTROL_COPY(checkpoint);
	case XLOG_PARAMETER_CHANGE:
		CONTROL_COPY(parameters);
	case XLOG_FPW_CHANGE:
		payload[0] = 1;
		return sizeof(bool);
	case XLOG_NEXTOID:
		CONTROL_COPY(oid);
	case XLOG_BACKUP_END:
		CONTROL_COPY(backup);
	case XLOG_RESTORE_POINT:
		CONTROL_COPY(restore);
	case XLOG_END_OF_RECOVERY:
		CONTROL_COPY(end);
	case XLOG_OVERWRITE_CONTRECORD:
		CONTROL_COPY(overwrite);
	case XLOG_SWITCH:
		return 0;
	default:
		payload[0] = 0x52;
		return 1;
	}
#undef CONTROL_COPY
}

UT_TEST(test_native_control_is_owned_input_and_not_replay_permission)
{
	static const uint8 infos[] = { XLOG_CHECKPOINT_SHUTDOWN,
								   XLOG_CHECKPOINT_ONLINE,
								   XLOG_PARAMETER_CHANGE,
								   XLOG_FPW_CHANGE,
								   XLOG_NEXTOID,
								   XLOG_NOOP,
								   XLOG_SWITCH,
								   XLOG_BACKUP_END,
								   XLOG_RESTORE_POINT,
								   XLOG_END_OF_RECOVERY,
								   XLOG_OVERWRITE_CONTRECORD };
	for (unsigned i = 0; i < lengthof(infos); i++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(200);
		FakeXactRecord fake;
		uint8 payload[512] = { 0 }, uuid[16];
		uint32 length = native_control_payload(infos[i], payload);
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 record;
		RfSideOnlineOperationV1 operation;
		RfSideOnlineApplyOpsV1 ops = { 0 };
		ApplyCapture capture = { 0 };
		memset(uuid, 0x44, sizeof(uuid));
		make_projection_record(&fake, RM_XLOG_ID, infos[i], payload, length);
		identity = make_identity(&fake, uuid);
		fake.u.decoded.max_block_id = -1;
		record = make_projection_record_plan(&fake);
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
					 RF_PAGE_PROOF_DETAIL_OK);
		memset(fake.data, 0xa5, length);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 1);
		assert_contribution_owners(plan, 0, RF_SIDE_CONTRIBUTION_NATIVE_CONTROL, 0);
		if (rf_side_online_plan_operation_v1(plan, 0, &operation)) {
			UT_ASSERT_EQ(operation.kind, RF_SIDE_ONLINE_OPERATION_NATIVE_CONTROL);
			UT_ASSERT_EQ(operation.owned_payload_length, length);
			if (length > 0)
				UT_ASSERT_EQ(memcmp(operation.owned_payload, payload, length), 0);
			UT_ASSERT(!operation.history_only);
		} else
			UT_ASSERT(false);
		ops.arg = &capture;
		ops.begin_protected_set = capture_begin;
		ops.end_protected_set = capture_end;
		ops.preflight_projection = accept_preflight;
		ops.apply_projection = capture_apply_projection;
		UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &ops),
					 RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT);
		UT_ASSERT_EQ(capture.begin_count + capture.projection_count, 0);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

static bool
stripe_unexpected_space(void *arg, const RfSideSpaceContributionV1 *space)
{
	unsigned *calls = arg;
	(void)space;
	(*calls)++;
	return false;
}

UT_TEST(test_stripe_records_retain_exact_control_obligation_without_pcm_target)
{
	for (unsigned which = 0; which < 2; which++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(200);
		xl_cluster_xid_stripe_join join = { 0 };
		xl_cluster_xid_stripe_retire retire = { 9 };
		FakeXactRecord fake;
		uint8 uuid[16];
		const void *payload = which == 0 ? (const void *)&join : (const void *)&retire;
		uint32 length = which == 0 ? sizeof(join) : sizeof(retire);
		uint8 info = which == 0 ? XLOG_CLUSTER_XID_STRIPE_JOIN : XLOG_CLUSTER_XID_STRIPE_RETIRE;
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 record;
		RfSideOnlineOperationV1 operation;
		RfSideContributionOwnersV1 owners = { 0 };
		RfContributorStreamCutV1 cut = { 0 };
		RfSideOnlineApplyOpsV1 ops = { 0 };
		ApplyCapture capture = { 0 };
		unsigned space_calls = 0;

		join.activated_floor_full = 4195104;
		join.stride_mode_epoch = 7;
		join.slot = 2;
		memset(uuid, 0x44, sizeof(uuid));
		make_projection_record(&fake, RM_CLUSTER_XID_STRIPE_ID, info, payload, length);
		identity = make_identity(&fake, uuid);
		fake.u.decoded.max_block_id = -1;
		record = make_projection_record_plan(&fake);
		UT_ASSERT_EQ(
			rf_opcode_route_lookup_v1(RM_CLUSTER_XID_STRIPE_ID, info, false, true, &record.route),
			RF_OPCODE_ROUTE_OK);
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
					 RF_PAGE_PROOF_DETAIL_OK);
		cut.failed_thread = 3;
		cut.timeline_id = 7;
		cut.origin_owner_incarnation = 9;
		cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		cut.scan_begin_inclusive = 100;
		cut.scan_end_exclusive = 200;
		UT_ASSERT_EQ(rf_side_record_census_v1(&record, &identity, &cut, 42, stripe_unexpected_space,
											  &space_calls, &owners),
					 RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(owners.owners, RF_SIDE_CONTRIBUTION_NATIVE_CONTROL);
		UT_ASSERT_EQ(owners.space_locator_count, 0);
		UT_ASSERT_EQ(space_calls, 0);
		memset(fake.data, 0xa5, length);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 1);
		if (rf_side_online_plan_operation_v1(plan, 0, &operation)) {
			UT_ASSERT_EQ(operation.kind, RF_SIDE_ONLINE_OPERATION_NATIVE_CONTROL);
			UT_ASSERT_EQ(operation.identity.record.rmid, RM_CLUSTER_XID_STRIPE_ID);
			UT_ASSERT_EQ(operation.owned_payload_length, length);
			UT_ASSERT_EQ(memcmp(operation.owned_payload, payload, length), 0);
		} else
			UT_ASSERT(false);
		/* A retained control obligation is not authority to replay stripe
		 * state through a projection or TT callback. */
		ops.arg = &capture;
		ops.begin_protected_set = capture_begin;
		ops.end_protected_set = capture_end;
		ops.preflight_projection = accept_preflight;
		ops.apply_projection = capture_apply_projection;
		UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &ops),
					 RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT);
		UT_ASSERT_EQ(capture.begin_count + capture.projection_count, 0);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_stripe_malformed_or_wrong_source_cannot_seal)
{
	for (unsigned fault = 0; fault < 14; fault++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(200);
		xl_cluster_xid_stripe_join join = { 0 };
		xl_cluster_xid_stripe_retire retire = { 9 };
		uint8 payload[sizeof(join) + 1] = { 0 }, uuid[16];
		uint8 info = (fault >= 8 && fault <= 10) ? XLOG_CLUSTER_XID_STRIPE_RETIRE : XLOG_CLUSTER_XID_STRIPE_JOIN;
		uint32 length = (fault >= 8 && fault <= 10) ? sizeof(retire) : sizeof(join);
		FakeXactRecord fake;
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 record;

		join.activated_floor_full = fault == 2 ? 0 : 4195104;
		join.stride_mode_epoch = fault == 3 ? 0 : 7;
		join.slot = fault == 4 ? -1 : fault == 5 ? CLUSTER_XID_STRIDE : fault == 6 ? 3 : 2;
		retire.slot = fault == 8 ? -1 : fault == 9 ? CLUSTER_XID_STRIDE : 9;
		memcpy(payload, (fault >= 8 && fault <= 10) ? (const void *)&retire : (const void *)&join, length);
		if (fault == 0 || fault == 10)
			length--;
		if (fault == 1)
			length++;
		if (fault == 7)
			payload[offsetof(xl_cluster_xid_stripe_join, slot) + sizeof(int32)] = 1;
		memset(uuid, 0x44, sizeof(uuid));
		make_projection_record(&fake, RM_CLUSTER_XID_STRIPE_ID, info, payload, length);
		if (fault == 11) fake.u.decoded.header.xl_xid = 818;
		if (fault == 12) fake.u.decoded.toplevel_xid = 818;
		if (fault == 13) fake.u.decoded.record_origin = 1;
		identity = make_identity(&fake, uuid);
		fake.u.decoded.max_block_id = -1;
		record = make_projection_record_plan(&fake);
		UT_ASSERT_EQ(
			rf_opcode_route_lookup_v1(RM_CLUSTER_XID_STRIPE_ID, info, false, true, &record.route),
			RF_OPCODE_ROUTE_OK);
		UT_ASSERT_NE(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
					 RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 0);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_native_control_rejects_bad_shape_and_identity)
{
	for (unsigned fault = 0; fault < 9; fault++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(200);
		FakeXactRecord fake;
		uint8 payload[512] = { 0 }, uuid[16];
		uint8 info = fault < 5	 ? XLOG_CHECKPOINT_SHUTDOWN
					 : fault < 8 ? XLOG_PARAMETER_CHANGE
								 : 0xc0;
		uint32 length = native_control_payload(info, payload);
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 record;
		if (fault == 0)
			length--;
		if (fault == 1)
			payload[offsetof(CheckPoint, fullPageWrites)] = 2;
		if (fault == 2) {
			TimeLineID tli = 9;
			memcpy(payload + offsetof(CheckPoint, ThisTimeLineID), &tli, sizeof(tli));
		}
		if (fault == 3) {
			XLogRecPtr redo = 101;
			memcpy(payload + offsetof(CheckPoint, redo), &redo, sizeof(redo));
		}
		if (fault == 5)
			payload[offsetof(xl_parameter_change, track_commit_timestamp)] = 2;
		if (fault == 6)
			memset(payload + offsetof(xl_parameter_change, MaxConnections), 0, sizeof(int));
		if (fault == 7)
			memset(payload + offsetof(xl_parameter_change, wal_level), 0xff, sizeof(int));
		memset(uuid, 0x44, sizeof(uuid));
		make_projection_record(&fake, RM_XLOG_ID, info, payload, length);
		identity = make_identity(&fake, uuid);
		fake.u.decoded.max_block_id = fault == 4 ? 0 : -1;
		record = make_projection_record_plan(&fake);
		UT_ASSERT_NE(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
					 RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

static uint32
standby_payload(uint8 info, char *payload)
{
	if (info == XLOG_STANDBY_LOCK) {
		xl_standby_locks *locks = (xl_standby_locks *)payload;
		locks->nlocks = 1;
		locks->locks[0] = (xl_standby_lock){ 42, 0, 1259 };
		return offsetof(xl_standby_locks, locks) + sizeof(xl_standby_lock);
	}
	if (info == XLOG_RUNNING_XACTS) {
		xl_running_xacts *running = (xl_running_xacts *)payload;
		running->xcnt = running->subxcnt = 1;
		running->nextXid = 5;
		running->oldestRunningXid = MaxTransactionId;
		running->latestCompletedXid = 3;
		running->xids[0] = MaxTransactionId;
		running->xids[1] = 4;
		return offsetof(xl_running_xacts, xids) + 2 * sizeof(TransactionId);
	}
	{
		xl_invalidations *invals = (xl_invalidations *)payload;
		invals->dbId = 1;
		invals->tsId = 1663;
		invals->relcacheInitFileInval = true;
		invals->nmsgs = 1;
		invals->msgs[0].rc.id = SHAREDINVALRELCACHE_ID;
		invals->msgs[0].rc.dbId = 0;
		invals->msgs[0].rc.relId = 0;
		return MinSizeOfInvalidations + sizeof(SharedInvalidationMessage);
	}
}


static bool
control_authority_fresh(void *arg)
{
	return arg != NULL && control_fresh;
}

static RfSideOnlinePlanV1 *
control_online_plan(uint8 native_info, uint8 standby_info)
{
	RfSideOnlinePlanV1 *plan = space_online_plan(300);
	FakeXactRecord fake;
	uint8 uuid[16];

	memset(uuid, 0x44, sizeof(uuid));
	for (int i = 0; i < 2; i++) {
		PGAlignedBlock payload = { 0 };
		uint8 rmid = i == 0 ? RM_XLOG_ID : RM_STANDBY_ID;
		uint8 info = i == 0 ? native_info : standby_info;
		uint32 length = i == 0 ? native_control_payload(info, (uint8 *)payload.data)
							   : standby_payload(info, payload.data);
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 record;

		make_projection_record(&fake, rmid, info, payload.data, length);
		fake.u.decoded.header.xl_scn = 700 + i;
		fake.u.decoded.max_block_id = -1;
		identity = make_identity(&fake, uuid);
		set_identity_range(&fake, &identity, 100 + i * 100, 200 + i * 100);
		record = make_projection_record_plan(&fake);
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
					 RF_PAGE_PROOF_DETAIL_OK);
	}
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	return plan;
}

/* Both records retain native bytes; a bad second record must prevent the
 * first record's cache effects, rather than fail after partial dispatch. */
static RfSideOnlinePlanV1 *
cold_standalone_cache_budget_plan(int fault, bool inspect, Size budget, RfPageProofDetailV1 *detail)
{
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut = { .failed_thread = 3,
									 .timeline_id = 7,
									 .flags = RF_CONTRIBUTOR_CUT_COMPLETE,
									 .scan_begin_inclusive = 100,
									 .scan_end_exclusive = 300 };
	RfSideOnlinePlanRequestV1 request
		= { .system_identifier = UINT64_C(0x11223344),
			.physical_cuts = &cut,
			.participant_count = 1,
			.memory_budget = budget,
			.purpose = inspect ? RF_SIDE_PLAN_PI_CONTRIBUTIONS : RF_SIDE_PLAN_RECOVERY };
	uint8 uuid[16];

	memset(uuid, 0x44, sizeof(uuid));
	memcpy(request.storage_uuid, uuid, sizeof(uuid));
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	for (int i = 0; i < 2; i++) {
		PGAlignedBlock payload = { 0 };
		xl_invalidations *invals = (xl_invalidations *)payload.data;
		FakeXactRecord fake;
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 record;

		invals->dbId = 5;
		invals->tsId = 1663;
		invals->relcacheInitFileInval = true;
		invals->nmsgs = 2;
		invals->msgs[0].rc.id = SHAREDINVALRELCACHE_ID;
		invals->msgs[0].rc.dbId = 5;
		invals->msgs[0].rc.relId = 16388;
		invals->msgs[1].cat.id = SHAREDINVALCATALOG_ID;
		invals->msgs[1].cat.dbId = InvalidOid;
		invals->msgs[1].cat.catId = 1260;
		if (i == 1) {
			switch (fault) {
			case 1:
				invals->msgs[1].id = -127;
				break;
			case 2:
				invals->msgs[1].cat.catId = InvalidOid;
				break;
			case 3:
				invals->msgs[1].id = SysCacheSize;
				break;
			case 4:
				invals->msgs[0].rc.dbId = 6;
				break;
			case 5:
				invals->dbId = InvalidOid;
				break;
			case 6:
				invals->tsId = InvalidOid;
				break;
			case 7:
				invals->msgs[1].id = SHAREDINVALSMGR_ID;
				break;
			case 8:
				invals->msgs[1].id = SHAREDINVALRELMAP_ID;
				break;
			}
		}
		make_projection_record(&fake, RM_STANDBY_ID, XLOG_INVALIDATIONS, payload.data,
							   MinSizeOfInvalidations + 2 * sizeof(SharedInvalidationMessage));
		fake.u.decoded.header.xl_scn = 700 + i;
		fake.u.decoded.max_block_id = -1;
		identity = make_identity(&fake, uuid);
		set_identity_range(&fake, &identity, 100 + i * 100, 200 + i * 100);
		record = make_projection_record_plan(&fake);
		*detail = rf_side_online_plan_feed_record_v1(plan, &record, &identity);
		if (*detail != RF_PAGE_PROOF_DETAIL_OK)
			break;
	}
	if (*detail == RF_PAGE_PROOF_DETAIL_OK)
		*detail = rf_side_online_plan_seal_v1(plan);
	return plan;
}

static RfSideOnlinePlanV1 *
cold_standalone_cache_plan(int fault)
{
	RfPageProofDetailV1 detail;
	RfSideOnlinePlanV1 *plan = cold_standalone_cache_budget_plan(fault, false, 0, &detail);

	UT_ASSERT_EQ(detail, RF_PAGE_PROOF_DETAIL_OK);
	return plan;
}

static RfSideOnlinePlanV1 *
assignment_plan_options(int fault, bool inspect, Size budget, RfPageProofDetailV1 *detail)
{
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut = { .failed_thread = 3,
									 .timeline_id = 7,
									 .flags = RF_CONTRIBUTOR_CUT_COMPLETE,
									 .scan_begin_inclusive = 100,
									 .scan_end_exclusive = 200 };
	RfSideOnlinePlanRequestV1 request
		= { .system_identifier = UINT64_C(0x11223344),
			.physical_cuts = &cut,
			.participant_count = 1,
			.memory_budget = budget,
			.purpose = inspect ? RF_SIDE_PLAN_PI_CONTRIBUTIONS : RF_SIDE_PLAN_RECOVERY };
	PGAlignedBlock payload = { 0 };
	xl_xact_assignment *a = (xl_xact_assignment *)payload.data;
	uint32 length = MinSizeOfXactAssignment + 2 * sizeof(TransactionId);
	FakeXactRecord fake;
	RfPageOnlineRecordIdentityV1 identity;
	RfDetachedRecordPlanV1 record;
	uint8 uuid[16];

	memset(request.storage_uuid, 0x44, sizeof(request.storage_uuid));
	*detail = rf_side_online_plan_create_v1(&request, &plan);
	if (*detail != RF_PAGE_PROOF_DETAIL_OK)
		return plan;
	a->xtop = 800;
	a->nsubxacts = 2;
	a->xsub[0] = 801;
	a->xsub[1] = 802;
	switch (fault) {
	case 1:
		length--;
		break;
	case 2:
		length++;
		break;
	case 3:
		a->nsubxacts = -1;
		break;
	case 4:
		a->nsubxacts = INT_MAX;
		break;
	case 5:
		a->xtop = FrozenTransactionId;
		break;
	case 6:
		a->xsub[1] = a->xsub[0];
		break;
	case 7:
		a->xsub[1] = 799;
		break;
	case 8:
		a->xsub[1] = InvalidTransactionId;
		break;
	case 9:
		a->xsub[1] = a->xtop + UINT32_C(0x80000000);
		break;
	}
	make_projection_record(&fake, RM_XACT_ID, XLOG_XACT_ASSIGNMENT, payload.data, length);
	fake.u.decoded.max_block_id = -1;
	memset(uuid, 0x44, sizeof(uuid));
	identity = make_identity(&fake, uuid);
	set_identity_range(&fake, &identity, 100, 200);
	record = make_projection_record_plan(&fake);
	*detail = rf_side_online_plan_feed_record_v1(plan, &record, &identity);
	/* The owned payload must survive the source reader buffer. */
	memset(fake.data, 0xe1, sizeof(fake.data));
	if (*detail == RF_PAGE_PROOF_DETAIL_OK)
		*detail = rf_side_online_plan_seal_v1(plan);
	return plan;
}

static RfSideOnlinePlanV1 *
assignment_plan(int fault, RfPageProofDetailV1 *detail)
{
	return assignment_plan_options(fault, false, 0, detail);
}

UT_TEST(test_cold_assignment_shape_wrap_alignment_and_budget)
{
	PGAlignedBlock payload = { 0 };
	xl_xact_assignment *a = (xl_xact_assignment *)payload.data;
	uint8 unaligned[512];
	uint32 length;
	RfPageProofDetailV1 detail;
	RfSideOnlinePlanV1 *inspect = assignment_plan_options(0, true, 0, &detail);
	RfSideOnlinePlanV1 *replay;
	RfSideOnlineProductionOwnerV1 owner;
	RfSideOnlineOperationV1 op;
	Size stored, copy;

	UT_ASSERT_EQ(detail, RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_operation_v1(inspect, 0, &op));
	copy = op.owned_payload_length;
	stored = RF_SIDE_ONLINE_PLAN_MAX_BYTES - rf_side_online_plan_scratch_available_v1(inspect);
	UT_ASSERT(
		rf_side_online_production_owner_init_v1(&owner, &owner, control_authority_fresh, 19, true));
	UT_ASSERT_EQ(rf_side_online_production_apply_v1(inspect, &owner),
				 RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT);
	rf_side_online_plan_destroy_v1(&inspect);
	for (int exact = 0; exact < 2; exact++) {
		replay = assignment_plan_options(0, false, stored + copy - 1 + exact, &detail);
		UT_ASSERT_EQ(detail, exact ? RF_PAGE_PROOF_DETAIL_OK : RF_PAGE_PROOF_DETAIL_CAPACITY);
		rf_side_online_plan_destroy_v1(&replay);
	}
	a->xtop = UINT32_MAX - 1;
	a->nsubxacts = 3;
	a->xsub[0] = UINT32_MAX;
	a->xsub[1] = 3;
	a->xsub[2] = 4;
	length = MinSizeOfXactAssignment + 3 * sizeof(TransactionId);
	memcpy(unaligned + 1, payload.data, length);
	UT_ASSERT(rf_side_xact_assignment_preflight_v1(unaligned + 1, length));
	UT_ASSERT(!rf_side_xact_assignment_preflight_v1(NULL, length));
	a->xtop = 800;
	a->nsubxacts = PGPROC_MAX_CACHED_SUBXIDS;
	for (int i = 0; i < PGPROC_MAX_CACHED_SUBXIDS; i++)
		a->xsub[i] = 801 + i;
	length = MinSizeOfXactAssignment + a->nsubxacts * sizeof(TransactionId);
	UT_ASSERT(rf_side_xact_assignment_preflight_v1((uint8 *)payload.data, length));
	a->nsubxacts++;
	UT_ASSERT(!rf_side_xact_assignment_preflight_v1((uint8 *)payload.data,
													length + sizeof(TransactionId)));
	a->nsubxacts = 0;
	UT_ASSERT(
		!rf_side_xact_assignment_preflight_v1((uint8 *)payload.data, MinSizeOfXactAssignment));
}

UT_TEST(test_cold_assignment_retains_complete_bounded_original_payload)
{
	for (int fault = 0; fault < 10; fault++) {
		RfPageProofDetailV1 detail;
		RfSideOnlinePlanV1 *plan = assignment_plan(fault, &detail);
		UT_ASSERT_EQ(detail == RF_PAGE_PROOF_DETAIL_OK, fault == 0);
		if (detail == RF_PAGE_PROOF_DETAIL_OK) {
			RfSideOnlineOperationV1 op;
			xl_xact_assignment copy;
			UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &op));
			UT_ASSERT_EQ(op.kind, RF_SIDE_ONLINE_OPERATION_NATIVE_CONTROL);
			UT_ASSERT_EQ(op.owned_payload_length, MinSizeOfXactAssignment + 8);
			memcpy(&copy, op.owned_payload, MinSizeOfXactAssignment);
			UT_ASSERT_EQ(copy.xtop, 800);
			UT_ASSERT_EQ(copy.nsubxacts, 2);
		}
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_assignment_original_owner_gates_and_cleanup)
{
	for (int fault = 0; fault < 8; fault++) {
		RfPageProofDetailV1 detail;
		RfSideOnlinePlanV1 *plan = assignment_plan(0, &detail);
		ClusterRecoveryDutyKey duty = canonical_duty;
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority = { .duty = &duty, .serial_guard = &guard };
		static RfSideOnlineProductionOwnerV1 owner;
		volatile RfPageProofDetailV1 result = RF_PAGE_PROOF_DETAIL_INTERNAL;
		volatile bool caught = false;

		UT_ASSERT_EQ(detail, RF_PAGE_PROOF_DETAIL_OK);
		duty.system_identifier = UINT64_C(0x11223344);
		duty.origin_owner_incarnation = 9;
		memset(duty.storage_uuid, 0x44, 16);
		if (fault == 1)
			guard.mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
		control_scope_test = cold_assignment_test = cold_completion_test = true;
		cold_clog_denied = fault == 2;
		control_scope_fault = fault == 3 ? 7 : 0;
		assignment_refuse = fault == 4;
		cold_source_fail = fault == 5;
		assignment_throw = fault == 6;
		assignment_expire = fault == 7;
		assignment_preflights = assignment_applies = assignment_verifies = 0;
		cold_source_calls = control_observe_calls = canonical_writes = 0;
		cold_source_ready = false;
		UT_ASSERT(rf_side_online_production_owner_init_v1(&owner, &authority,
														  control_authority_fresh, 19, true));
		UT_ASSERT(rf_side_online_production_bind_undo_v1(&owner, &authority));
		PG_TRY();
		{
			result = rf_side_online_production_apply_v1(plan, &owner);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 6);
		UT_ASSERT_EQ(result == RF_PAGE_PROOF_DETAIL_OK, fault == 0);
		UT_ASSERT_EQ(owner.protected_set_complete, fault == 0);
		UT_ASSERT_EQ(assignment_applies, fault == 0 || fault >= 6 ? 1 : 0);
		UT_ASSERT_EQ(assignment_verifies, fault == 0 || fault == 7 ? 1 : 0);
		UT_ASSERT_EQ(control_observe_calls, fault == 0 ? 1 : 0);
		UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0);
		control_scope_test = cold_assignment_test = cold_completion_test = false;
		cold_clog_denied = cold_source_fail = assignment_refuse = false;
		assignment_throw = assignment_expire = false;
		control_scope_fault = 0;
		control_fresh = true;
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_standalone_cache_shape_and_unaligned_payload)
{
	for (int fault = 0; fault <= 14; fault++) {
		PGAlignedBlock payload = { 0 };
		xl_invalidations *invals = (xl_invalidations *)payload.data;
		uint8 unaligned[256];
		uint32 length = standby_payload(XLOG_INVALIDATIONS, payload.data);
		bool accepted = fault == 0 || fault >= 9;

		switch (fault) {
		case 1:
			length = MinSizeOfInvalidations - 1;
			break;
		case 2:
			length--;
			break;
		case 3:
			length++;
			break;
		case 4:
			invals->nmsgs = -1;
			break;
		case 5:
			invals->nmsgs = INT_MAX;
			break;
		case 6:
			payload.data[offsetof(xl_invalidations, relcacheInitFileInval)] = 2;
			break;
		case 7:
			invals->dbId = InvalidOid;
			break;
		case 8:
			invals->tsId = InvalidOid;
			break;
		case 9:
			invals->msgs[0].cc.id = 0;
			break;
		case 10:
			invals->msgs[0].cat.id = SHAREDINVALCATALOG_ID;
			invals->msgs[0].cat.catId = 1259;
			break;
		case 11:
			invals->msgs[0].sn.id = SHAREDINVALSNAPSHOT_ID;
			break;
		case 12:
			invals->relcacheInitFileInval = false;
			break;
		case 13:
			invals->nmsgs = 0;
			length = MinSizeOfInvalidations;
			break;
		case 14:
			invals->nmsgs = 0;
			length = MinSizeOfInvalidations;
			invals->relcacheInitFileInval = false;
			invals->dbId = invals->tsId = InvalidOid;
			break;
		}
		memcpy(unaligned + 1, payload.data, length);
		UT_ASSERT_EQ(rf_side_standby_invalidation_preflight_v1(unaligned + 1, length), accepted);
	}
	UT_ASSERT(!rf_side_standby_invalidation_preflight_v1(NULL, MinSizeOfInvalidations));
}

UT_TEST(test_cold_standalone_cache_reserves_one_copy_and_keeps_pi_nonexecuting)
{
	RfPageProofDetailV1 detail;
	RfSideOnlinePlanV1 *inspect = cold_standalone_cache_budget_plan(0, true, 0, &detail);
	RfSideOnlinePlanV1 *replay;
	RfSideOnlineOperationV1 op;
	RfSideOnlineProductionOwnerV1 owner;
	Size stored, copy;

	UT_ASSERT_EQ(detail, RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_operation_v1(inspect, 0, &op));
	copy = op.owned_payload_length;
	stored = RF_SIDE_ONLINE_PLAN_MAX_BYTES - rf_side_online_plan_scratch_available_v1(inspect);
	UT_ASSERT(
		rf_side_online_production_owner_init_v1(&owner, &owner, control_authority_fresh, 19, true));
	UT_ASSERT_EQ(rf_side_online_production_apply_v1(inspect, &owner),
				 RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT);
	replay = cold_standalone_cache_budget_plan(0, false, 0, &detail);
	UT_ASSERT_EQ(detail, RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_scratch_available_v1(inspect)
					 - rf_side_online_plan_scratch_available_v1(replay),
				 copy);
	rf_side_online_plan_destroy_v1(&replay);
	rf_side_online_plan_destroy_v1(&inspect);
	for (int exact = 0; exact < 2; exact++) {
		replay = cold_standalone_cache_budget_plan(0, false, stored + copy - 1 + exact, &detail);
		UT_ASSERT_EQ(detail, exact ? RF_PAGE_PROOF_DETAIL_OK : RF_PAGE_PROOF_DETAIL_CAPACITY);
		if (!exact)
			UT_ASSERT_EQ(rf_side_online_production_apply_v1(replay, &owner),
						 RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT);
		rf_side_online_plan_destroy_v1(&replay);
	}
}

UT_TEST(test_cold_standalone_cache_uses_original_owner_and_full_set_preflight)
{
	for (int fault = 0; fault <= 8; fault++) {
		RfSideOnlinePlanV1 *plan = cold_standalone_cache_plan(fault);
		ClusterRecoveryDutyKey duty = canonical_duty;
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority = { .duty = &duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		bool accepted = fault == 0;

		duty.system_identifier = UINT64_C(0x11223344);
		duty.origin_owner_incarnation = 9;
		memset(duty.storage_uuid, 0x44, 16);
		memset(&prepare_apply, 0, sizeof(prepare_apply));
		control_scope_test = cold_standalone_cache_test = cold_cache_test = cold_cache_init = true;
		cold_cache_calls = cold_clog_verifies = control_observe_calls = 0;
		canonical_writes = canonical_syncs = 0;
		UT_ASSERT(rf_side_online_production_owner_init_v1(&owner, &authority,
														  control_authority_fresh, 19, true));
		UT_ASSERT(rf_side_online_production_bind_undo_v1(&owner, &authority));
		UT_ASSERT_EQ(rf_side_online_production_preflight_v1(plan, &owner),
					 accepted ? RF_PAGE_PROOF_DETAIL_OK : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
		UT_ASSERT_EQ(cold_cache_calls + control_observe_calls, 0);
		UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner),
					 accepted ? RF_PAGE_PROOF_DETAIL_OK : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
		UT_ASSERT_EQ(cold_cache_calls, accepted ? 2 : 0);
		UT_ASSERT_EQ(control_observe_calls, accepted ? 2 : 0);
		UT_ASSERT_EQ(canonical_writes + canonical_syncs + prepare_apply.terminal_projection_stores,
					 0);
		UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0 && DatabasePath == NULL);
		if (accepted) {
			UT_ASSERT(owner.protected_set_complete);
			UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner), RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_EQ(cold_cache_calls, 4);
		}
		control_scope_test = cold_standalone_cache_test = cold_cache_test = cold_cache_init = false;
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_standalone_cache_keeps_source_and_mode_gates)
{
	for (int fault = 1; fault <= 17; fault++) {
		RfSideOnlinePlanV1 *plan = cold_standalone_cache_plan(0);
		ClusterRecoveryDutyKey duty = canonical_duty;
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority = { .duty = &duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;

		duty.system_identifier = UINT64_C(0x11223344);
		duty.origin_owner_incarnation = 9;
		memset(duty.storage_uuid, 0x44, 16);
		if (fault == 2)
			duty.origin_thread_id = 2;
		if (fault == 4)
			guard.mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
		if (fault == 5)
			duty.storage_uuid[0]++;
		if (fault == 6)
			guard.mode = 0;
		if (fault == 12)
			authority.serial_guard = NULL;
		control_scope_test = cold_standalone_cache_test = cold_cache_test = cold_cache_init = true;
		control_scope_denied = fault == 1;
		control_fresh = fault != 3;
		control_scope_fault = fault;
		cold_clog_denied = fault == 16;
		if (fault == 17)
			DatabasePath = (char *)"pre-existing path";
		cold_cache_calls = control_observe_calls = 0;
		UT_ASSERT(rf_side_online_production_owner_init_v1(&owner, &authority,
														  control_authority_fresh, 19, true));
		UT_ASSERT(rf_side_online_production_bind_undo_v1(&owner, &authority));
		UT_ASSERT(rf_side_online_production_apply_v1(plan, &owner) != RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(cold_cache_calls + control_observe_calls, 0);
		UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0);
		if (fault == 17)
			UT_ASSERT(strcmp(DatabasePath, "pre-existing path") == 0);
		DatabasePath = NULL;
		control_scope_test = cold_standalone_cache_test = cold_cache_test = cold_cache_init = false;
		control_scope_denied = cold_clog_denied = false;
		control_scope_fault = 0;
		control_fresh = true;
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_standalone_cache_error_and_expiry_leave_no_completion)
{
	for (int fault = 0; fault < 2; fault++) {
		RfSideOnlinePlanV1 *plan = cold_standalone_cache_plan(0);
		ClusterRecoveryDutyKey duty = canonical_duty;
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority = { .duty = &duty, .serial_guard = &guard };
		static RfSideOnlineProductionOwnerV1 owner;
		volatile bool caught = false;
		volatile RfPageProofDetailV1 result = RF_PAGE_PROOF_DETAIL_INTERNAL;

		duty.system_identifier = UINT64_C(0x11223344);
		duty.origin_owner_incarnation = 9;
		memset(duty.storage_uuid, 0x44, 16);
		memset(&prepare_apply, 0, sizeof(prepare_apply));
		control_scope_test = cold_standalone_cache_test = cold_cache_test = cold_cache_init = true;
		cold_cache_calls = cold_clog_verifies = control_observe_calls = 0;
		cold_cache_throw = fault == 0;
		cold_cache_expire = fault == 1;
		UT_ASSERT(rf_side_online_production_owner_init_v1(&owner, &authority,
														  control_authority_fresh, 19, true));
		UT_ASSERT(rf_side_online_production_bind_undo_v1(&owner, &authority));
		PG_TRY();
		{
			result = rf_side_online_production_apply_v1(plan, &owner);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 0);
		UT_ASSERT(result != RF_PAGE_PROOF_DETAIL_OK && !owner.protected_set_complete);
		UT_ASSERT_EQ(cold_cache_calls, 1);
		UT_ASSERT_EQ(control_observe_calls + prepare_apply.terminal_projection_stores, 0);
		UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0 && DatabasePath == NULL);
		cold_cache_throw = cold_cache_expire = cold_clog_denied = false;
		control_scope_test = cold_standalone_cache_test = cold_cache_test = cold_cache_init = false;
		cold_cache_calls = 0;
		rf_side_online_plan_destroy_v1(&plan);
	}
}


/* Mutations caught: treating every cold control as unsupported, dropping a
 * foreign invalidation with the controls, or accepting stale primary/source
 * context after SCN observation. Use the real immutable plan and owner. */
static void
cold_control_reset(void)
{
	control_scope_test = cold_control_test = true;
	control_scope_denied = cold_clog_denied = false;
	control_fresh = true;
	control_scope_fault = 0;
	control_observe_throw = control_expire_after_observe = false;
	cold_control_drift = 0;
	cluster_node_id = 0;
	standbyState = STANDBY_DISABLED;
	control_observed_scn = control_observe_calls = 0;
	canonical_writes = canonical_syncs = cold_cache_calls = cold_source_calls = 0;
	cold_clog_applies = cold_clog_verifies = cold_locks_calls = 0;
	memset(&prepare_apply, 0, sizeof(prepare_apply));
}

static void
cold_control_owner(RfSideOnlineProductionOwnerV1 *owner,
				   ClusterThreadRecoveryAuthorityV1 *authority, ClusterRecoveryDutyKey *duty)
{
	duty->system_identifier = UINT64_C(0x11223344);
	duty->origin_owner_incarnation = 9;
	memset(duty->storage_uuid, 0x44, 16);
	UT_ASSERT(rf_side_online_production_owner_init_v1(owner, authority, control_authority_fresh, 19,
													  true));
	UT_ASSERT(rf_side_online_production_bind_undo_v1(owner, authority));
}

static RfSideOnlinePlanV1 *
cold_join_plan(bool retire)
{
	RfSideOnlinePlanV1 *plan = space_online_plan(200);
	xl_cluster_xid_stripe_join join = { 0 };
	xl_cluster_xid_stripe_retire removal = { 2 };
	FakeXactRecord fake;
	uint8 uuid[16];
	RfPageOnlineRecordIdentityV1 identity;
	RfDetachedRecordPlanV1 record;

	join.activated_floor_full = 4195104;
	join.stride_mode_epoch = 1;
	join.slot = 2;
	memset(uuid, 0x44, sizeof(uuid));
	make_projection_record(&fake, RM_CLUSTER_XID_STRIPE_ID,
						   retire ? XLOG_CLUSTER_XID_STRIPE_RETIRE : XLOG_CLUSTER_XID_STRIPE_JOIN,
						   retire ? (const void *)&removal : (const void *)&join,
						   retire ? sizeof(removal) : sizeof(join));
	identity = make_identity(&fake, uuid);
	fake.u.decoded.max_block_id = -1;
	record = make_projection_record_plan(&fake);
	UT_ASSERT_EQ(rf_opcode_route_lookup_v1(RM_CLUSTER_XID_STRIPE_ID, identity.record.info,
										 false, true, &record.route), RF_OPCODE_ROUTE_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	memset(fake.data, 0xa5, sizeof(join)); /* Only the immutable capture may execute. */
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	return plan;
}

UT_TEST(test_cold_join_uses_captured_native_owner_without_full_source_completion)
{
	RfSideOnlinePlanV1 *plan = cold_join_plan(false);
	ClusterRecoveryDutyKey duty = canonical_duty;
	ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
	ClusterThreadRecoveryAuthorityV1 authority = { .duty = &duty, .serial_guard = &guard };
	RfSideOnlineProductionOwnerV1 owner;
	RfSideOnlineOperationV1 operation;

	cold_control_reset();
	cold_join_test = true;
	cold_join_fault = -1;
	cold_join_preflights = cold_join_applies = 0;
	cold_control_owner(&owner, &authority, &duty);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
	UT_ASSERT_EQ(rf_side_online_production_preflight_v1(plan, &owner), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(cold_join_applies + control_observe_calls, 0);
	for (unsigned repeat = 0; repeat < 2; repeat++) {
		UT_ASSERT_EQ(rf_side_online_production_apply_record_v1(plan, 0, &operation.identity, &owner),
					 RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(cold_join_applies, repeat + 1);
		UT_ASSERT_EQ(control_observe_calls, repeat + 1);
		UT_ASSERT(!owner.protected_set_complete && !owner.protected_set_active);
		UT_ASSERT(owner.protected_plan == NULL && canonical_scope == NULL
				  && canonical_writer_depth == 0);
	}
	UT_ASSERT(cold_join_preflights >= 5);
	UT_ASSERT_EQ(canonical_writes + canonical_syncs + cold_cache_calls + cold_clog_applies
				 + prepare_apply.terminal_projection_stores, 0);
	cold_join_test = false;
	cold_control_reset();
	control_scope_test = cold_control_test = false;
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_cold_join_keeps_original_authority_retire_and_failure_boundaries)
{
	for (int fault = 0; fault < 15; fault++) {
		RfSideOnlinePlanV1 *plan = cold_join_plan(fault == 14);
		ClusterRecoveryDutyKey duty = canonical_duty;
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority = { .duty = &duty, .serial_guard = &guard };
		static RfSideOnlineProductionOwnerV1 owner;
		RfSideOnlineOperationV1 operation;
		volatile bool caught = false;
		volatile RfPageProofDetailV1 result = RF_PAGE_PROOF_DETAIL_INTERNAL;

		cold_control_reset();
		cold_join_test = true;
		cold_join_fault = fault < 6 ? fault : -1;
		cold_join_preflights = cold_join_applies = 0;
		cold_control_owner(&owner, &authority, &duty);
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
		if (fault == 6) standbyState = STANDBY_INITIALIZED;
		if (fault == 7) guard.mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
		if (fault == 8) cold_clog_denied = true;
		if (fault == 9) control_scope_fault = 7;
		if (fault == 10) operation.identity.record.record_crc++;
		if (fault == 11) control_fresh = false;
		if (fault == 12) cold_control_drift = 1;
		if (fault == 13) cluster_node_id = 2;
		PG_TRY();
		{
			result = rf_side_online_production_apply_record_v1(plan, 0, &operation.identity, &owner);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 5);
		UT_ASSERT(result != RF_PAGE_PROOF_DETAIL_OK && !owner.protected_set_complete);
		UT_ASSERT_EQ(cold_join_applies, (fault == 1 || (fault >= 3 && fault <= 5) || fault == 12) ? 1 : 0);
		UT_ASSERT_EQ(control_observe_calls, fault == 12 ? 1 : 0);
		UT_ASSERT(owner.protected_plan == NULL && !owner.protected_set_active
				  && canonical_scope == NULL && canonical_writer_depth == 0);
		cold_join_test = false;
		cold_control_reset();
		control_scope_test = cold_control_test = false;
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_foreign_control_consumes_no_founder_or_transaction_effects)
{
	static const uint8 infos[] = { XLOG_CHECKPOINT_SHUTDOWN,
								   XLOG_CHECKPOINT_ONLINE,
								   XLOG_PARAMETER_CHANGE,
								   XLOG_FPW_CHANGE,
								   XLOG_NEXTOID,
								   XLOG_NOOP,
								   XLOG_SWITCH,
								   XLOG_BACKUP_END,
								   XLOG_RESTORE_POINT,
								   XLOG_END_OF_RECOVERY,
								   XLOG_OVERWRITE_CONTRECORD };
	for (unsigned i = 0; i < lengthof(infos); i++)
		for (unsigned standby = 0; standby < 2; standby++) {
			RfSideOnlinePlanV1 *plan = control_online_plan(
				infos[i], standby == 0 ? XLOG_STANDBY_LOCK : XLOG_RUNNING_XACTS);
			ClusterRecoveryDutyKey duty = canonical_duty;
			ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
			ClusterThreadRecoveryAuthorityV1 authority = { .duty = &duty, .serial_guard = &guard };
			RfSideOnlineProductionOwnerV1 owner;
			cold_control_reset();
			cold_control_owner(&owner, &authority, &duty);
			UT_ASSERT_EQ(rf_side_online_production_preflight_v1(plan, &owner),
						 RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_EQ(control_observe_calls, 0);
			UT_ASSERT_EQ(cold_source_calls, 0);
			UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner), RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_EQ(control_observe_calls, 2);
			UT_ASSERT_EQ(control_observed_scn, 701);
			UT_ASSERT(owner.protected_set_complete);
			/* The whole cold source barrier is durability, not replay of the
			 * foreign founder/checkpoint or any transaction effect. */
			UT_ASSERT_EQ(cold_source_calls, 1);
			UT_ASSERT_EQ(canonical_writes + canonical_syncs + cold_cache_calls + cold_clog_applies
							 + cold_clog_verifies + cold_locks_calls
							 + prepare_apply.terminal_projection_stores,
						 0);
			UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0
					  && owner.protected_plan == NULL && !owner.protected_set_active);
			rf_side_online_plan_destroy_v1(&plan);
		}
	cold_control_reset();
	control_scope_test = cold_control_test = false;
}

UT_TEST(test_cold_foreign_controls_keep_original_role_source_and_effect_gates)
{
	static const int scope_faults[] = { 7, 8, 9, 10, 11, 13, 14, 15 };
	for (unsigned fault = 0; fault < 26; fault++) {
		RfSideOnlinePlanV1 *plan = control_online_plan(
			XLOG_CHECKPOINT_ONLINE, fault == 25 ? XLOG_INVALIDATIONS : XLOG_RUNNING_XACTS);
		ClusterRecoveryDutyKey duty = canonical_duty;
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority = { .duty = &duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		cold_control_reset();
		cold_control_owner(&owner, &authority, &duty);
		switch (fault) {
		case 0:
			cluster_node_id = 2;
			break;
		case 1:
			cluster_node_id = -1;
			break;
		case 2:
			cluster_node_id = 128;
			break;
		case 3:
			standbyState = STANDBY_INITIALIZED;
			break;
		case 4:
			standbyState = STANDBY_SNAPSHOT_PENDING;
			break;
		case 5:
			standbyState = STANDBY_SNAPSHOT_READY;
			break;
		case 6:
			standbyState = (HotStandbyState)-1;
			break;
		case 7:
			standbyState = (HotStandbyState)(STANDBY_SNAPSHOT_READY + 1);
			break;
		case 8:
			cold_clog_denied = true;
			break;
		case 9:
			control_scope_denied = true;
			break;
		case 10:
			control_fresh = false;
			break;
		case 11:
			guard.mode = 0;
			break;
		case 12:
			authority.serial_guard = NULL;
			break;
		case 13:
			duty.origin_thread_id = 2;
			break;
		case 22:
			duty.storage_uuid[0]++;
			break;
		case 23:
			duty.system_identifier++;
			break;
		case 24:
			duty.origin_owner_incarnation++;
			break;
		case 25:
			/* A valid invalidation still needs its native consumer. An
			 * occupied DatabasePath must refuse, not silently no-op. */
			DatabasePath = malloc(1);
			UT_ASSERT(DatabasePath != NULL);
			DatabasePath[0] = '\0';
			break;
		default:
			if (fault >= 14 && fault < 22)
				control_scope_fault = scope_faults[fault - 14];
			break;
		}
		UT_ASSERT_NE(rf_side_online_production_preflight_v1(plan, &owner), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_NE(rf_side_online_production_apply_v1(plan, &owner), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(control_observe_calls + cold_cache_calls + canonical_writes, 0);
		UT_ASSERT(!owner.protected_set_complete && !owner.protected_set_active
				  && canonical_scope == NULL && canonical_writer_depth == 0);
		if (fault == 25) {
			UT_ASSERT(DatabasePath != NULL);
			free(DatabasePath);
			DatabasePath = NULL;
		}
		rf_side_online_plan_destroy_v1(&plan);
	}
	cold_control_reset();
	control_scope_test = cold_control_test = false;
}

UT_TEST(test_cold_control_rechecks_context_after_observation_and_unwinds_error)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		RfSideOnlinePlanV1 *plan = control_online_plan(XLOG_NEXTOID, XLOG_RUNNING_XACTS);
		ClusterRecoveryDutyKey duty = canonical_duty;
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority = { .duty = &duty, .serial_guard = &guard };
		static RfSideOnlineProductionOwnerV1 owner;
		volatile bool caught = false;
		volatile RfPageProofDetailV1 result = RF_PAGE_PROOF_DETAIL_INTERNAL;
		cold_control_reset();
		cold_control_owner(&owner, &authority, &duty);
		control_observe_throw = fault == 0;
		control_expire_after_observe = fault == 1;
		cold_control_drift = fault >= 2 ? fault - 1 : 0;
		PG_TRY();
		{
			result = rf_side_online_production_apply_v1(plan, &owner);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 0);
		UT_ASSERT_NE(result, RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(control_observe_calls, fault == 0 ? 0 : 1);
		UT_ASSERT(!owner.protected_set_complete && !owner.protected_set_active
				  && owner.protected_plan == NULL && canonical_scope == NULL
				  && canonical_writer_depth == 0);
		rf_side_online_plan_destroy_v1(&plan);
	}
	cold_control_reset();
	control_scope_test = cold_control_test = false;
}

UT_TEST(test_native_controls_use_original_online_owner_before_any_side_effect)
{
	static const uint8 infos[] = { XLOG_CHECKPOINT_SHUTDOWN,
								   XLOG_CHECKPOINT_ONLINE,
								   XLOG_PARAMETER_CHANGE,
								   XLOG_FPW_CHANGE,
								   XLOG_NEXTOID,
								   XLOG_NOOP,
								   XLOG_SWITCH,
								   XLOG_BACKUP_END,
								   XLOG_RESTORE_POINT,
								   XLOG_END_OF_RECOVERY,
								   XLOG_OVERWRITE_CONTRECORD };

	for (int fault = 0; fault < 16 + lengthof(infos); fault++) {
		RfSideOnlinePlanV1 *plan;
		ClusterRecoveryDutyKey duty = canonical_duty;
		ClusterRecoverySerialGuard guard = { 0 };
		ClusterThreadRecoveryAuthorityV1 authority = { .duty = &duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		bool accepted = fault == 0 || fault >= 16;

		plan = control_online_plan(fault >= 16 ? infos[fault - 16] : XLOG_CHECKPOINT_ONLINE,
								   fault == 4	 ? XLOG_INVALIDATIONS
								   : fault >= 16 ? XLOG_STANDBY_LOCK
												 : XLOG_RUNNING_XACTS);
		duty.system_identifier = UINT64_C(0x11223344);
		duty.origin_owner_incarnation = 9;
		memset(duty.storage_uuid, 0x44, 16);
		guard.mode
			= fault == 6 ? CLUSTER_RECOVERY_SERIAL_COLD_FORMED : CLUSTER_RECOVERY_SERIAL_ONLINE;
		if (fault == 12)
			authority.serial_guard = NULL;
		control_scope_test = true;
		control_scope_denied = fault == 1;
		control_scope_fault = fault;
		control_fresh = fault != 3;
		control_observed_scn = control_observe_calls = 0;
		canonical_writes = canonical_syncs = 0;
		if (fault == 2)
			duty.origin_thread_id = 2;
		if (fault == 5)
			duty.storage_uuid[0]++;
		UT_ASSERT(rf_side_online_production_owner_init_v1(&owner, &authority,
														  control_authority_fresh, 19, true));
		UT_ASSERT(rf_side_online_production_bind_undo_v1(&owner, &authority));
		UT_ASSERT_EQ(rf_side_online_production_preflight_v1(plan, &owner),
					 accepted	  ? RF_PAGE_PROOF_DETAIL_OK
					 : fault == 2 ? RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT
								  : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
		UT_ASSERT_EQ(control_observe_calls, 0);
		UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner),
					 accepted	  ? RF_PAGE_PROOF_DETAIL_OK
					 : fault == 2 ? RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT
								  : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
		UT_ASSERT_EQ(control_observe_calls, accepted ? 2 : 0);
		UT_ASSERT_EQ(control_observed_scn, accepted ? 701 : 0);
		UT_ASSERT_EQ(canonical_writes + canonical_syncs, 0);
		UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0);
		if (accepted) {
			UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner), RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_EQ(control_observed_scn, 701);
		}
		control_scope_test = control_scope_denied = false;
		control_scope_fault = 0;
		control_fresh = true;
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_native_control_error_or_expiry_releases_original_scope)
{
	RfSideOnlinePlanV1 *plan = control_online_plan(XLOG_NEXTOID, XLOG_RUNNING_XACTS);
	ClusterRecoveryDutyKey duty = canonical_duty;
	ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_ONLINE };
	ClusterThreadRecoveryAuthorityV1 authority = { .duty = &duty, .serial_guard = &guard };
	static RfSideOnlineProductionOwnerV1 owner;
	volatile bool caught = false;

	duty.system_identifier = UINT64_C(0x11223344);
	duty.origin_owner_incarnation = 9;
	memset(duty.storage_uuid, 0x44, 16);
	control_scope_test = true;
	control_observe_calls = control_observed_scn = 0;
	UT_ASSERT(rf_side_online_production_owner_init_v1(&owner, &authority, control_authority_fresh,
													  19, true));
	UT_ASSERT(rf_side_online_production_bind_undo_v1(&owner, &authority));
	control_observe_throw = true;
	PG_TRY();
	{
		(void)rf_side_online_production_apply_v1(plan, &owner);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught && canonical_scope == NULL && canonical_writer_depth == 0);
	UT_ASSERT(!owner.protected_set_active && owner.protected_plan == NULL);
	UT_ASSERT_EQ(control_observe_calls, 0);
	control_observe_throw = false;
	control_expire_after_observe = true;
	UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner),
				 RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
	UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0);
	UT_ASSERT_EQ(control_observe_calls, 1);
	UT_ASSERT_EQ(control_observed_scn, 700);
	UT_ASSERT(!owner.protected_set_complete);
	control_expire_after_observe = false;
	control_fresh = true;
	UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(control_observed_scn, 701);
	control_scope_test = false;
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_standby_retains_exact_payload_and_independent_control_obligation)
{
	const uint8 infos[] = { XLOG_STANDBY_LOCK, XLOG_RUNNING_XACTS, XLOG_INVALIDATIONS };
	for (unsigned i = 0; i < lengthof(infos); i++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(200);
		FakeXactRecord fake;
		PGAlignedBlock payload = { 0 };
		uint8 uuid[16];
		uint32 length = standby_payload(infos[i], payload.data);
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 record;
		RfSideOnlineOperationV1 operation;
		RfSideOnlineApplyOpsV1 ops = { 0 };
		ApplyCapture capture = { 0 };

		memset(uuid, 0x44, sizeof(uuid));
		make_projection_record(&fake, RM_STANDBY_ID, infos[i], payload.data, length);
		identity = make_identity(&fake, uuid);
		fake.u.decoded.max_block_id = -1;
		record = make_projection_record_plan(&fake);
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
					 RF_PAGE_PROOF_DETAIL_OK);
		memset(fake.data, 0xa5, length);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 1);
		assert_contribution_owners(plan, 0, RF_SIDE_CONTRIBUTION_NATIVE_CONTROL, 0);
		if (rf_side_online_plan_operation_v1(plan, 0, &operation)) {
			UT_ASSERT_EQ(operation.kind, RF_SIDE_ONLINE_OPERATION_NATIVE_CONTROL);
			UT_ASSERT_EQ(operation.identity.record.rmid, RM_STANDBY_ID);
			UT_ASSERT_EQ(operation.owned_payload_length, length);
			UT_ASSERT_EQ(memcmp(operation.owned_payload, payload.data, length), 0);
		} else
			UT_ASSERT(false);
		ops.arg = &capture;
		ops.begin_protected_set = capture_begin;
		ops.end_protected_set = capture_end;
		ops.preflight_projection = accept_preflight;
		ops.apply_projection = capture_apply_projection;
		UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &ops),
					 RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT);
		UT_ASSERT_EQ(capture.begin_count + capture.projection_count, 0);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_standby_empty_snapshot_and_initial_completed_xid_are_valid)
{
	RfSideOnlinePlanV1 *plan = space_online_plan(200);
	FakeXactRecord fake;
	xl_running_xacts running = { 0 };
	uint8 uuid[16];
	RfPageOnlineRecordIdentityV1 identity;
	RfDetachedRecordPlanV1 record;

	running.nextXid = running.oldestRunningXid = FirstNormalTransactionId;
	running.latestCompletedXid = FrozenTransactionId;
	memset(uuid, 0x44, sizeof(uuid));
	make_projection_record(&fake, RM_STANDBY_ID, XLOG_RUNNING_XACTS, &running,
						   offsetof(xl_running_xacts, xids));
	identity = make_identity(&fake, uuid);
	fake.u.decoded.max_block_id = -1;
	record = make_projection_record_plan(&fake);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_standby_invalid_counts_shapes_and_sources_never_seal)
{
	const uint8 infos[] = { XLOG_STANDBY_LOCK, XLOG_RUNNING_XACTS, XLOG_INVALIDATIONS };
	for (unsigned i = 0; i < lengthof(infos); i++)
		for (unsigned fault = 0; fault < 8; fault++) {
			RfSideOnlinePlanV1 *plan = space_online_plan(200);
			FakeXactRecord fake;
			PGAlignedBlock payload = { 0 };
			uint8 uuid[16];
			uint8 info = infos[i];
			uint32 length = standby_payload(info, payload.data);
			RfPageOnlineRecordIdentityV1 identity;
			RfDetachedRecordPlanV1 record;
			int count = fault == 2 ? -1 : INT_MAX;
			Size offset = i == 0   ? offsetof(xl_standby_locks, nlocks)
						  : i == 1 ? offsetof(xl_running_xacts, subxcnt)
								   : offsetof(xl_invalidations, nmsgs);

			if (fault == 0)
				length--;
			if (fault == 1)
				length++;
			if (fault == 2 || fault == 3)
				memcpy(payload.data + offset, &count, sizeof(count));
			if (fault == 4) {
				if (i == 0)
					((xl_standby_locks *)payload.data)->locks[0].xid = InvalidTransactionId;
				else
					payload.data[i == 1 ? offsetof(xl_running_xacts, subxid_overflow)
										: offsetof(xl_invalidations, relcacheInitFileInval)]
						= 2;
			}
			if (fault == 5)
				info |= XLR_SPECIAL_REL_UPDATE;
			if (fault == 7)
				info = 0xf0;
			memset(uuid, 0x44, sizeof(uuid));
			make_projection_record(&fake, RM_STANDBY_ID, info, payload.data, length);
			identity = make_identity(&fake, uuid);
			fake.u.decoded.max_block_id = fault == 6 ? 0 : -1;
			record = make_projection_record_plan(&fake);
			UT_ASSERT_NE(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
						 RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
			rf_side_online_plan_destroy_v1(&plan);
		}
}

static bool
census_space(void *arg, const RfSideSpaceContributionV1 *space)
{
	unsigned *calls = arg;
	(*calls)++;
	UT_ASSERT_EQ(space->page_mask, 2);
	UT_ASSERT_EQ(space->result.key.database_incarnation, 42);
	UT_ASSERT_EQ(space->result_token[1], 80);
	return true;
}

UT_TEST(test_stream_census_does_not_accumulate_a_million_side_operations)
{
	FakeXactRecord fake;
	RfPageOnlineRecordIdentityV1 identity;
	RfDetachedRecordPlanV1 record;
	RfContributorStreamCutV1 cut = { 0 };
	RfSideContributionOwnersV1 owners;
	uint8 uuid[16];
	unsigned calls = 0;
	memset(uuid, 0x44, sizeof(uuid));
	make_commit(&fake, 802, 999, 12345, false);
	identity = make_identity(&fake, uuid);
	record = make_record_plan(&fake);
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.origin_owner_incarnation = 9;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 100000100;
	for (uint32 i = 0; i < 1000000; i++) {
		set_identity_range(&fake, &identity, 100 + (uint64)i * 100, 200 + (uint64)i * 100);
		UT_ASSERT_EQ(
			rf_side_record_census_v1(&record, &identity, &cut, 42, census_space, &calls, &owners),
			RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(owners.owners,
					 RF_SIDE_CONTRIBUTION_UNDO_HEADER | RF_SIDE_CONTRIBUTION_TERMINAL);
		UT_ASSERT_EQ(owners.space_locator_count, 0);
	}
	UT_ASSERT_EQ(calls, 0);
	/* The census cannot make a malformed final record disappear. */
	fake.u.decoded.main_data_len--;
	UT_ASSERT_NE(
		rf_side_record_census_v1(&record, &identity, &cut, 42, census_space, &calls, &owners),
		RF_PAGE_PROOF_DETAIL_OK);
}

UT_TEST(test_stream_census_checks_native_and_space_payloads_without_replay)
{
	FakeXactRecord fake;
	RfPageOnlineRecordIdentityV1 identity;
	RfDetachedRecordPlanV1 record;
	RfContributorStreamCutV1 cut = { 0 };
	RfSideContributionOwnersV1 owners;
	ClusterSpaceReservationChange change = space_advance_fixture();
	PGAlignedBlock payload = { 0 };
	uint8 uuid[16];
	unsigned calls = 0;
	memset(uuid, 0x44, sizeof(uuid));
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.origin_owner_incarnation = 9;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 200;
	for (uint8 info = 0; info <= XLOG_INVALIDATIONS; info += 0x10) {
		uint32 length = standby_payload(info, payload.data);
		make_projection_record(&fake, RM_STANDBY_ID, info, payload.data, length);
		identity = make_identity(&fake, uuid);
		record = make_projection_record_plan(&fake);
		fake.u.decoded.max_block_id = -1;
		UT_ASSERT_EQ(
			rf_side_record_census_v1(&record, &identity, &cut, 42, census_space, &calls, &owners),
			RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(owners.owners, RF_SIDE_CONTRIBUTION_NATIVE_CONTROL);
		fake.u.decoded.main_data_len--;
		UT_ASSERT_NE(
			rf_side_record_census_v1(&record, &identity, &cut, 42, census_space, &calls, &owners),
			RF_PAGE_PROOF_DETAIL_OK);
	}
	UT_ASSERT(cluster_space_reservation_wal_encode(&change, payload.data,
												   CLUSTER_SPACE_RESERVATION_WAL_BYTES));
	make_projection_record(&fake, RM_SMGR_ID, XLOG_SMGR_SPACE_RESERVATION, payload.data,
						   CLUSTER_SPACE_RESERVATION_WAL_BYTES);
	identity = make_identity(&fake, uuid);
	record = make_projection_record_plan(&fake);
	fake.u.decoded.max_block_id = -1;
	UT_ASSERT_EQ(
		rf_side_record_census_v1(&record, &identity, &cut, 42, census_space, &calls, &owners),
		RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(calls, 1);
	UT_ASSERT_EQ(owners.space_locator_count, 1);
	UT_ASSERT_NE(
		rf_side_record_census_v1(&record, &identity, &cut, 43, census_space, &calls, &owners),
		RF_PAGE_PROOF_DETAIL_OK);
	cut.origin_owner_incarnation = 0;
	UT_ASSERT_NE(
		rf_side_record_census_v1(&record, &identity, &cut, 42, census_space, &calls, &owners),
		RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(calls, 1);
}

UT_TEST(test_space_retained_ancestry_covers_history_and_each_committed_drop)
{
	for (int history_only = 0; history_only < 2; history_only++) {
		ClusterSpaceReservationChange advance = space_advance_fixture();
		ClusterSpaceStructureChange drops[2]
			= { space_drop_fixture(16384), space_drop_fixture(16385) };
		XLogRecPtr redo = history_only ? 300 : 200;
		RfSideOnlinePlanV1 *plan = space_online_plan_redo(300, &redo);
		FakeXactRecord fake;
		uint8 wal[CLUSTER_SPACE_RESERVATION_WAL_BYTES];
		bool terminal = false;
		RfSideSpaceIncarnationEndV1 ended, untouched;

		memset(&untouched, 0xa5, sizeof(untouched));
		ended = untouched;

		UT_ASSERT(cluster_space_reservation_wal_encode(&advance, wal, sizeof(wal)));
		UT_ASSERT_EQ(
			space_online_feed(plan, XLOG_SMGR_SPACE_RESERVATION, wal, sizeof(wal), 100, false),
			RF_PAGE_PROOF_DETAIL_OK);
		make_space_commit(&fake, drops, 2);
		UT_ASSERT_EQ(space_commit_feed(plan, &fake, 200), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(!rf_side_online_plan_space_covers_v1(plan, &advance.result.identity.key, 1, 0, 1,
													   &terminal));
		UT_ASSERT(!rf_side_online_plan_space_incarnation_end_v1(
			plan, &advance.result.identity.key, advance.result.identity.incarnation, 1, &ended));
		UT_ASSERT_EQ(memcmp(&ended, &untouched, sizeof(ended)), 0);
		UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_space_target_count_v1(plan), history_only ? 0 : 2);
		for (int locator = 0; locator < 2; locator++) {
			memset(&ended, 0, sizeof(ended));
			UT_ASSERT(rf_side_online_plan_space_incarnation_end_v1(
				plan, &drops[locator].identity.expected.key,
				drops[locator].identity.expected.incarnation, 1, &ended));
			UT_ASSERT_EQ(ended.operation, 1);
			UT_ASSERT_EQ(ended.change.identity.action, CLUSTER_SPACE_WAL_TOMBSTONE);
			UT_ASSERT_EQ(ended.change.identity.expected.key.locator.relNumber, 16384 + locator);
			UT_ASSERT_EQ(ended.change.identity.result_token, 70);
			UT_ASSERT(ended.terminal);
		}
		ended = untouched;
		UT_ASSERT(!rf_side_online_plan_space_incarnation_end_v1(
			plan, &advance.result.identity.key, advance.result.identity.incarnation, 0,
			&ended)); /* ADVANCE is not SPACE0. */
		UT_ASSERT_EQ(memcmp(&ended, &untouched, sizeof(ended)), 0);
		UT_ASSERT(rf_side_online_plan_space_covers_v1(plan, &advance.result.identity.key, 1, 0, 1,
													  &terminal));
		UT_ASSERT(terminal);
		UT_ASSERT(rf_side_online_plan_space_covers_v1(plan, &advance.result.identity.key, 1, 0, 0,
													  &terminal));
		UT_ASSERT(!terminal);
		for (int block = 0; block < 2; block++) {
			UT_ASSERT(rf_side_online_plan_space_covers_v1(plan, &drops[1].identity.result.key,
														  block, 1, 1, &terminal));
			UT_ASSERT(terminal);
		}
		/* The other COMMIT locator has no contribution in operation zero. */
		UT_ASSERT(!rf_side_online_plan_space_covers_v1(plan, &drops[1].identity.result.key, 1, 0, 1,
													   &terminal));
		UT_ASSERT(!rf_side_online_plan_space_covers_v1(plan, &advance.result.identity.key, 0, 0, 1,
													   &terminal));
		UT_ASSERT(!rf_side_online_plan_space_covers_v1(plan, &advance.result.identity.key, 1, 1, 0,
													   &terminal));
		UT_ASSERT(!rf_side_online_plan_space_covers_v1(plan, &advance.result.identity.key, 2, 1, 1,
													   &terminal));
		UT_ASSERT(!rf_side_online_plan_space_covers_v1(plan, &advance.result.identity.key, 1, 0, 2,
													   &terminal));
		advance.result.identity.key.database_incarnation++;
		UT_ASSERT(!rf_side_online_plan_space_covers_v1(plan, &advance.result.identity.key, 1, 1, 1,
													   &terminal));
		UT_ASSERT(terminal); /* Every refusal preserves output. */
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_space_retained_ancestry_rejects_broken_history_before_live_suffix)
{
	for (int fault = 0; fault < 4; fault++) {
		ClusterSpaceReservationChange advance = space_advance_fixture();
		ClusterSpaceIdentityKey key = advance.result.identity.key;
		XLogRecPtr redo = 200;
		RfSideOnlinePlanV1 *plan = space_online_plan_redo(300, &redo);
		uint8 wal[CLUSTER_SPACE_RESERVATION_WAL_BYTES];
		bool terminal = true;
		RfSideSpaceTerminalV1 selected, untouched;
		memset(&untouched, 0xa5, sizeof(untouched));
		selected = untouched;
		UT_ASSERT(cluster_space_reservation_wal_encode(&advance, wal, sizeof(wal)));
		UT_ASSERT_EQ(
			space_online_feed(plan, XLOG_SMGR_SPACE_RESERVATION, wal, sizeof(wal), 100, false),
			RF_PAGE_PROOF_DETAIL_OK);
		advance.before = advance.result;
		advance.before_token = advance.result_token;
		if (fault == 0)
			advance.before_token++;
		if (fault == 1)
			advance.before.next_block--;
		if (fault == 2)
			advance.before.identity.incarnation[0]++;
		if (fault == 3) {
			/* Replay permits survivor gaps; PI ancestry must not infer them. */
			advance.before.next_block += 4;
			advance.before_token = 70;
		}
		advance.first_block = advance.before.next_block;
		advance.result = advance.before;
		advance.result.next_block += advance.granted;
		advance.result_token = 60;
		UT_ASSERT(cluster_space_reservation_wal_encode(&advance, wal, sizeof(wal)));
		UT_ASSERT_EQ(
			space_online_feed(plan, XLOG_SMGR_SPACE_RESERVATION, wal, sizeof(wal), 200, false),
			RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
		/* A valid standalone redo suffix must not bless its broken retained history. */
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(!rf_side_online_plan_space_covers_v1(plan, &key, 1, 0, 1, &terminal));
		UT_ASSERT(!rf_side_online_plan_space_covers_v1(plan, &key, 1, 1, 1, &terminal));
		UT_ASSERT(terminal);
		UT_ASSERT(!rf_side_online_plan_space_terminal_v1(plan, &key, 1, &selected));
		UT_ASSERT_EQ(memcmp(&selected, &untouched, sizeof(selected)), 0);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_space_retained_ancestry_orders_interleaved_writers_not_feed_or_token)
{
	RfContributorStreamCutV1 cuts[3] = { { 0 } };
	RfSideOnlinePlanRequestV1 request = { 0 };
	RfSideOnlinePlanV1 *plan = NULL;
	ClusterSpaceReservationChange chain[3];
	uint8 wal[CLUSTER_SPACE_RESERVATION_WAL_BYTES];
	const int feed_order[] = { 1, 2, 0 };
	bool terminal = false;
	RfSideSpaceTerminalV1 selected = { 0 };
	chain[0] = space_advance_fixture();
	for (int i = 0; i < 3; i++) {
		cuts[i].failed_thread = i + 1;
		cuts[i].origin_owner_incarnation = i + 11;
		cuts[i].timeline_id = 7;
		cuts[i].flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		cuts[i].scan_begin_inclusive = 100;
		cuts[i].scan_end_exclusive = 200;
		if (i > 0) {
			chain[i] = chain[i - 1];
			chain[i].before = chain[i - 1].result;
			chain[i].before_token = chain[i - 1].result_token;
			chain[i].first_block = chain[i].before.next_block;
			chain[i].result = chain[i].before;
			chain[i].result.next_block += chain[i].granted;
			chain[i].result_token -= 20;
		}
	}
	request.system_identifier = chain[0].before.identity.key.system_identifier;
	memcpy(request.storage_uuid, chain[0].before.identity.key.storage_uuid, 16);
	request.physical_cuts = cuts;
	request.participant_count = 3;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	for (int i = 0; i < 3; i++) {
		int origin = feed_order[i];
		FakeXactRecord fake;
		RfDetachedRecordPlanV1 record;
		RfPageOnlineRecordIdentityV1 identity;
		UT_ASSERT(cluster_space_reservation_wal_encode(&chain[origin], wal, sizeof(wal)));
		make_projection_record(&fake, RM_SMGR_ID,
							   XLOG_SMGR_SPACE_RESERVATION | XLR_SPECIAL_REL_UPDATE, wal,
							   sizeof(wal));
		fake.u.decoded.max_block_id = -1;
		identity = make_identity(&fake, request.storage_uuid);
		identity.participant_index = origin;
		identity.record.origin_thread = cuts[origin].failed_thread;
		set_identity_range(&fake, &identity, 100, 200);
		record = make_projection_record_plan(&fake);
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
					 RF_PAGE_PROOF_DETAIL_OK);
	}
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	/* Operation order is B,C,A; dependency order is A,B,C (tokens 80,60,40). */
	UT_ASSERT(
		rf_side_online_plan_space_terminal_v1(plan, &chain[0].before.identity.key, 1, &selected));
	UT_ASSERT_EQ(selected.operation, 1);
	UT_ASSERT_EQ(selected.contribution.result_token[1], 40);
	UT_ASSERT(
		!rf_side_online_plan_space_terminal_v1(plan, &chain[0].before.identity.key, 0, &selected));
	UT_ASSERT(rf_side_online_plan_space_covers_v1(plan, &chain[0].before.identity.key, 1, 2, 1,
												  &terminal));
	UT_ASSERT(terminal);
	UT_ASSERT(rf_side_online_plan_space_covers_v1(plan, &chain[0].before.identity.key, 1, 2, 0,
												  &terminal));
	UT_ASSERT(!terminal);
	UT_ASSERT(!rf_side_online_plan_space_covers_v1(plan, &chain[0].before.identity.key, 1, 1, 2,
												   &terminal));
	UT_ASSERT(!terminal);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_space_retained_ancestry_does_not_retire_prior_truncate_incarnation)
{
	ClusterSpaceReservationChange advance = space_advance_fixture();
	ClusterSpaceStructureChange truncate = { 0 };
	RfSideOnlinePlanV1 *plan = space_online_plan(300);
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	bool terminal = false;
	UT_ASSERT(
		cluster_space_reservation_wal_encode(&advance, wal, CLUSTER_SPACE_RESERVATION_WAL_BYTES));
	UT_ASSERT_EQ(space_online_feed(plan, XLOG_SMGR_SPACE_RESERVATION, wal,
								   CLUSTER_SPACE_RESERVATION_WAL_BYTES, 100, false),
				 RF_PAGE_PROOF_DETAIL_OK);
	truncate.identity.action = CLUSTER_SPACE_WAL_TRUNCATE;
	truncate.identity.nblocks = 2;
	truncate.identity.expected = advance.result.identity;
	truncate.identity.result = truncate.identity.expected;
	truncate.identity.result.incarnation[0]++;
	truncate.identity.result.sequence++;
	truncate.identity.result.operation++;
	truncate.identity.before_token = 123;
	truncate.identity.result_token = 60;
	truncate.reservation.action = CLUSTER_SPACE_RESERVATION_RESET;
	truncate.reservation.before = advance.result;
	truncate.reservation.result = truncate.reservation.before;
	truncate.reservation.result.identity = truncate.identity.result;
	truncate.reservation.result.next_block = 2;
	truncate.reservation.first_block = 2;
	truncate.reservation.before_token = advance.result_token;
	truncate.reservation.result_token = 60;
	UT_ASSERT(cluster_space_structure_wal_encode(&truncate, wal, sizeof(wal)));
	UT_ASSERT_EQ(space_online_feed(plan, XLOG_SMGR_SPACE_IDENTITY, wal, sizeof(wal), 200, false),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(!rf_side_online_plan_space_covers_v1(plan, &truncate.identity.result.key, 1, 0, 1,
												   &terminal));
	UT_ASSERT(!terminal);
	for (int block = 0; block < 2; block++) {
		UT_ASSERT(rf_side_online_plan_space_covers_v1(plan, &truncate.identity.result.key, block, 1,
													  1, &terminal));
		UT_ASSERT(terminal);
	}
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_space_retained_ancestry_create_and_independent_page_terminal)
{
	ClusterSpaceReservationChange advance = space_advance_fixture();
	ClusterSpaceStructureChange create = { 0 };
	XLogRecPtr redo = 300;
	RfSideOnlinePlanV1 *plan = space_online_plan_redo(300, &redo);
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	bool terminal = false;
	RfSideSpaceTerminalV1 selected = { 0 };
	create.identity.action = CLUSTER_SPACE_WAL_CREATE;
	create.identity.nblocks = InvalidBlockNumber;
	create.identity.result = advance.result.identity;
	create.identity.result_token = 50;
	create.reservation.action = CLUSTER_SPACE_RESERVATION_INIT;
	create.reservation.result.identity = create.identity.result;
	create.reservation.result_token = 50;
	UT_ASSERT(cluster_space_structure_wal_encode(&create, wal, sizeof(wal)));
	UT_ASSERT_EQ(space_online_feed(plan, XLOG_SMGR_SPACE_IDENTITY, wal, sizeof(wal), 100, false),
				 RF_PAGE_PROOF_DETAIL_OK);
	advance.before = create.reservation.result;
	advance.before_token = 50;
	advance.first_block = 0;
	advance.result = advance.before;
	advance.result.next_block = advance.granted;
	UT_ASSERT(
		cluster_space_reservation_wal_encode(&advance, wal, CLUSTER_SPACE_RESERVATION_WAL_BYTES));
	UT_ASSERT_EQ(space_online_feed(plan, XLOG_SMGR_SPACE_RESERVATION, wal,
								   CLUSTER_SPACE_RESERVATION_WAL_BYTES, 200, false),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(
		rf_side_online_plan_space_terminal_v1(plan, &create.identity.result.key, 0, &selected));
	UT_ASSERT_EQ(selected.operation, 0);
	UT_ASSERT_EQ(selected.contribution.result_token[0], 50);
	UT_ASSERT(
		rf_side_online_plan_space_terminal_v1(plan, &create.identity.result.key, 1, &selected));
	UT_ASSERT_EQ(selected.operation, 1);
	UT_ASSERT_EQ(selected.contribution.result_token[1], 80);
	UT_ASSERT(
		rf_side_online_plan_space_covers_v1(plan, &create.identity.result.key, 0, 0, 0, &terminal));
	UT_ASSERT(terminal); /* ADVANCE changes only block one. */
	UT_ASSERT(
		rf_side_online_plan_space_covers_v1(plan, &create.identity.result.key, 1, 0, 0, &terminal));
	UT_ASSERT(!terminal);
	UT_ASSERT(
		rf_side_online_plan_space_covers_v1(plan, &create.identity.result.key, 1, 0, 1, &terminal));
	UT_ASSERT(terminal);
	UT_ASSERT_EQ(rf_side_online_plan_space_target_count_v1(plan), 0);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_space_incarnation_end_requires_an_actual_later_space0_contribution)
{
	ClusterSpaceReservationChange advance = space_advance_fixture();
	ClusterSpaceStructureChange changes[2];
	RfContributorStreamCutV1 cuts[3] = { { 0 } };
	RfSideOnlinePlanRequestV1 request = { 0 };
	RfSideOnlinePlanV1 *plan = NULL;
	RfSideSpaceIncarnationEndV1 ended = { 0 }, untouched;
	const int feed_order[] = { 1, 2, 0 };
	XLogRecPtr redo[] = { 200, 200, 200 };
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];

	changes[0] = space_truncate_fixture(&advance.result, 80, 123, 60, 0x18);
	changes[1] = space_truncate_fixture(&changes[0].reservation.result, 60, 60, 40, 0x19);
	for (int i = 0; i < 3; i++) {
		cuts[i].failed_thread = i + 1;
		cuts[i].origin_owner_incarnation = i + 11;
		cuts[i].timeline_id = 7;
		cuts[i].flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		cuts[i].scan_begin_inclusive = 100;
		cuts[i].scan_end_exclusive = 200;
	}
	request.system_identifier = advance.before.identity.key.system_identifier;
	memcpy(request.storage_uuid, advance.before.identity.key.storage_uuid, 16);
	request.physical_cuts = cuts;
	request.participant_count = 3;
	request.redo_starts = redo;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	for (int i = 0; i < 3; i++) {
		int origin = feed_order[i];
		FakeXactRecord fake;
		RfDetachedRecordPlanV1 record;
		RfPageOnlineRecordIdentityV1 identity;
		uint32 length = origin == 0 ? CLUSTER_SPACE_RESERVATION_WAL_BYTES : sizeof(wal);
		uint8 opcode = origin == 0 ? XLOG_SMGR_SPACE_RESERVATION : XLOG_SMGR_SPACE_IDENTITY;
		if (origin == 0)
			UT_ASSERT(cluster_space_reservation_wal_encode(&advance, wal, length));
		else
			UT_ASSERT(cluster_space_structure_wal_encode(&changes[origin - 1], wal, length));
		make_projection_record(&fake, RM_SMGR_ID, opcode | XLR_SPECIAL_REL_UPDATE, wal, length);
		fake.u.decoded.max_block_id = -1;
		identity = make_identity(&fake, request.storage_uuid);
		identity.participant_index = origin;
		identity.record.origin_thread = cuts[origin].failed_thread;
		set_identity_range(&fake, &identity, 100, 200);
		record = make_projection_record_plan(&fake);
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
					 RF_PAGE_PROOF_DETAIL_OK);
	}
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_space_target_count_v1(plan), 0); /* All history. */
	/* Feed B,C,A, tokens 80,60,40: neither feed nor numeric order is ancestry. */
	UT_ASSERT(rf_side_online_plan_space_incarnation_end_v1(
		plan, &advance.before.identity.key, advance.before.identity.incarnation, 0, &ended));
	UT_ASSERT_EQ(ended.operation, 0);
	UT_ASSERT_EQ(ended.change.identity.action, CLUSTER_SPACE_WAL_TRUNCATE);
	UT_ASSERT_EQ(ended.change.identity.nblocks, 2);
	UT_ASSERT_EQ(ended.change.identity.expected.incarnation[0], 0x17);
	UT_ASSERT_EQ(ended.change.identity.result.incarnation[0], 0x18);
	UT_ASSERT(!ended.terminal);
	UT_ASSERT(rf_side_online_plan_space_incarnation_end_v1(
		plan, &advance.before.identity.key, advance.before.identity.incarnation, 1, &ended));
	UT_ASSERT_EQ(ended.operation, 0); /* Original end, not its later covering operation. */
	UT_ASSERT(ended.terminal);
	UT_ASSERT(rf_side_online_plan_space_incarnation_end_v1(
		plan, &advance.before.identity.key, changes[0].identity.result.incarnation, 1, &ended));
	UT_ASSERT_EQ(ended.operation, 1);
	UT_ASSERT_EQ(ended.change.identity.result_token, 40);
	memset(&untouched, 0xa5, sizeof(untouched));
	for (int fault = 0; fault < 9; fault++) {
		ClusterSpaceIdentityKey key = advance.before.identity.key;
		uint8 incarnation[16];
		uint32 completed = 1;
		memcpy(incarnation, changes[0].identity.result.incarnation, 16);
		if (fault == 0)
			completed = 0; /* Before this incarnation ended. */
		if (fault == 1)
			completed = 2; /* Reservation-only contribution. */
		if (fault == 2)
			completed = 3;
		if (fault == 3)
			key.database_incarnation++;
		if (fault == 4)
			key.locator.spcOid++;
		if (fault == 5)
			key.locator.dbOid++;
		if (fault == 6)
			key.system_identifier++;
		if (fault == 7)
			memset(incarnation, 0, 16);
		if (fault == 8)
			memcpy(incarnation, changes[1].identity.result.incarnation, 16);
		ended = untouched;
		UT_ASSERT(!rf_side_online_plan_space_incarnation_end_v1(plan, &key, incarnation, completed,
																&ended));
		UT_ASSERT_EQ(memcmp(&ended, &untouched, sizeof(ended)), 0);
	}
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_space_incarnation_end_rejects_old_uuid_reappearance_and_retained_gaps)
{
	for (int fault = 0; fault < 5; fault++) {
		ClusterSpaceReservationChange advance = space_advance_fixture();
		ClusterSpaceStructureChange changes[2];
		XLogRecPtr redo = 400;
		RfSideOnlinePlanV1 *plan = space_online_plan_redo(400, &redo);
		RfSideSpaceIncarnationEndV1 ended, untouched;
		uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
		changes[0] = space_truncate_fixture(&advance.result, 80, 123, 60, 0x18);
		changes[1] = space_truncate_fixture(&changes[0].reservation.result, 60, 60, 40,
											fault == 0 ? 0x17 : 0x19);
		if (fault == 1)
			changes[0].reservation.before_token++;
		if (fault == 2)
			changes[0].reservation.before.next_block++;
		if (fault == 3)
			changes[1].reservation.before_token++;
		if (fault == 4)
			changes[1].reservation.before.next_block++;
		UT_ASSERT(cluster_space_reservation_wal_encode(&advance, wal,
													   CLUSTER_SPACE_RESERVATION_WAL_BYTES));
		UT_ASSERT_EQ(space_online_feed(plan, XLOG_SMGR_SPACE_RESERVATION, wal,
									   CLUSTER_SPACE_RESERVATION_WAL_BYTES, 100, false),
					 RF_PAGE_PROOF_DETAIL_OK);
		for (int i = 0; i < 2; i++) {
			UT_ASSERT(cluster_space_structure_wal_encode(&changes[i], wal, sizeof(wal)));
			UT_ASSERT_EQ(space_online_feed(plan, XLOG_SMGR_SPACE_IDENTITY, wal, sizeof(wal),
										   200 + i * 100, false),
						 RF_PAGE_PROOF_DETAIL_OK);
		}
		UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		memset(&untouched, 0xa5, sizeof(untouched));
		for (int completed = 1; completed <= 2; completed++) {
			ended = untouched;
			UT_ASSERT(!rf_side_online_plan_space_incarnation_end_v1(
				plan, &advance.before.identity.key, advance.before.identity.incarnation, completed,
				&ended));
			UT_ASSERT_EQ(memcmp(&ended, &untouched, sizeof(ended)), 0);
		}
		rf_side_online_plan_destroy_v1(&plan);
	}
}

static void
make_ctrc_release(FakeXactRecord *fake, uint32 generation, uint8 status)
{
	xl_undo_tt_slot_ctrc_release_v1 record = { 0 };
	uint8 wal[CLUSTER_UNDO_TT_CTRC_RELEASE_BYTES];

	record.segment_id = 513;
	record.segment_generation = generation;
	record.xid = 802;
	record.cluster_epoch = 11;
	record.root_id = 12;
	record.root_generation = 13;
	record.formation_epoch = 14;
	record.admission_record_generation = 15;
	record.seal_generation = 16;
	record.touched_nodes_low = 4;
	record.slot_offset = 4;
	record.slot_wrap = 7;
	record.owner_instance = 3;
	record.terminal_status = status;
	record.format_version = CLUSTER_UNDO_TT_CTRC_RELEASE_VERSION;
	record.flags = CLUSTER_UNDO_TT_CTRC_RELEASE_ALL_TOUCHED_ACKED;
	memset(record.ack_set_digest, 0x5a, sizeof(record.ack_set_digest));
	UT_ASSERT(cluster_undo_tt_ctrc_release_encode(&record, wal));
	make_projection_record(fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_CTRC_RELEASE, wal,
						   sizeof(wal));
}

static void
ctrc_base(PGAlignedBlock *base, uint32 generation, uint8 status)
{
	UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)base->data;
	TTSlot *slot;

	cluster_undo_segment_make_header_bytes(513, 3, base->data);
	header->wrap_count = generation;
	slot = &header->tt_slots[4];
	slot->xid = 802;
	slot->wrap = 7;
	slot->status = status;
	slot->commit_scn = status == TT_SLOT_COMMITTED ? 901 : InvalidScn;
}

UT_TEST(test_initial_ctrc_release_keeps_exact_header_transition)
{
	/* Generation zero is the real INIT result; generation nine is the
	 * existing nonzero control. Neither can identify the slot by itself. */
	const uint32 generations[] = { 0, 9 };
	for (unsigned g = 0; g < lengthof(generations); g++)
		for (int fault = 0; fault < 12; fault++) {
			FakeXactRecord fake;
			ClusterUndoDecoded decoded;
			PGAlignedBlock base, saved, out, untouched, expected;
			UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)base.data;
			TTSlot *slot;
			ClusterUndoHeaderPrepareResultV1 result;
			uint8 status = fault == 1 || fault == 9 ? TT_SLOT_ABORTED : TT_SLOT_COMMITTED;

			make_ctrc_release(&fake, generations[g], status);
			UT_ASSERT(cluster_undo_decode(&fake.reader, &decoded));
			UT_ASSERT_EQ(decoded.expected_generation, generations[g]);
			ctrc_base(&base, generations[g], status);
			slot = &header->tt_slots[4];
			switch (fault) {
			case 2:
				slot->flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
				break;
			case 3:
				header->wrap_count++;
				slot->xid++;
				slot->status = TT_SLOT_ACTIVE;
				break;
			case 4:
				slot->wrap++;
				slot->status = TT_SLOT_ACTIVE;
				break;
			case 5:
				slot->xid++;
				break;
			case 6:
				slot->wrap--;
				break;
			case 7:
				slot->status = TT_SLOT_ACTIVE;
				break;
			case 8:
				slot->commit_scn = InvalidScn;
				break;
			case 9:
				slot->commit_scn = 901;
				break;
			case 10:
				slot->flags = 0x80;
				break;
			case 11:
				decoded.expected_generation = UINT32_MAX;
				break;
			}
			saved = base;
			memset(&untouched, 0xa5, sizeof(untouched));
			out = untouched;
			result = cluster_undo_prepare_header_v1(&decoded, NULL, 0, base.data, out.data);
			UT_ASSERT_EQ(result, fault <= 1	  ? CLUSTER_UNDO_HEADER_APPLY
								 : fault == 2 ? CLUSTER_UNDO_HEADER_ALREADY
								 : fault <= 4 ? CLUSTER_UNDO_HEADER_SKIP_STALE
											  : CLUSTER_UNDO_HEADER_BLOCKED);
			UT_ASSERT_EQ(memcmp(base.data, saved.data, BLCKSZ), 0);
			if (fault <= 1) {
				expected = base;
				((UndoSegmentHeaderData *)expected.data)->tt_slots[4].flags
					= TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
				UT_ASSERT_EQ(memcmp(out.data, expected.data, BLCKSZ), 0);
			} else
				UT_ASSERT_EQ(memcmp(out.data, untouched.data, BLCKSZ), 0);
		}
}

static bool
ctrc_authority_fresh(void *arg)
{
	return arg != NULL && !(ctrc_io_fault == 4 && canonical_writes != 0);
}

UT_TEST(test_cold_initial_ctrc_release_installs_exact_durable_header)
{
	for (int variant = 0; variant < 5; variant++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(200);
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		FakeXactRecord fake;
		PGAlignedBlock initial, expected, disk;
		uint8 status = variant == 1 ? TT_SLOT_ABORTED : TT_SLOT_COMMITTED;

		make_ctrc_release(&fake, 0, status);
		undo_header_feed(plan, &fake, 100);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		/* Retained WAL has been overwritten; all consumers must use the
		 * original plan's owned decoded fields and complete source identity. */
		ctrc_base(&initial, variant == 3 ? 1 : 0, status);
		if (variant == 2)
			((UndoSegmentHeaderData *)initial.data)->tt_slots[4].flags
				= TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
		if (variant == 4)
			((UndoSegmentHeaderData *)initial.data)->tt_slots[4].wrap++;
		expected = initial;
		if (variant <= 1)
			((UndoSegmentHeaderData *)expected.data)->tt_slots[4].flags
				= TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
		if (variant <= 1)
			UT_ASSERT(cluster_undo_segment_header_seal(expected.data));
		canonical_file = tmpfile();
		UT_ASSERT(canonical_file != NULL);
		UT_ASSERT_EQ(ftruncate(fileno(canonical_file), UNDO_SEGMENT_SIZE_BYTES), 0);
		UT_ASSERT_EQ(pwrite(fileno(canonical_file), initial.data, BLCKSZ, 0), BLCKSZ);
		UT_ASSERT_EQ(fsync(fileno(canonical_file)), 0);
		canonical_writes = canonical_syncs = 0;
		ctrc_io_fault = 0;
		reset_prepare_apply();
		UT_ASSERT(rf_side_online_production_owner_init_v1(&owner, &authority, ctrc_authority_fresh,
														  19, true));
		UT_ASSERT(rf_side_online_production_bind_undo_v1(&owner, &authority));
		for (int repeat = 0; repeat < 2; repeat++) {
			/* The native slot transition skips stale WAL without rewriting.
			 * That does not prove the canonical target's later membership. */
			UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner),
						 variant < 3 ? RF_PAGE_PROOF_DETAIL_OK
									 : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
			UT_ASSERT_EQ(owner.protected_set_complete, variant < 3);
			UT_ASSERT(!owner.protected_set_active);
			UT_ASSERT_EQ(canonical_writes, variant <= 1 ? 1 : 0);
			UT_ASSERT_EQ(canonical_syncs > 0, variant < 3);
			UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0);
			UT_ASSERT(owner.undo_headers == NULL && owner.protected_plan == NULL);
			UT_ASSERT_EQ(pread(fileno(canonical_file), disk.data, BLCKSZ, 0), BLCKSZ);
			UT_ASSERT_EQ(memcmp(disk.data, expected.data, BLCKSZ), 0);
			UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
		}
		UT_ASSERT_EQ(fclose(canonical_file), 0);
		canonical_file = NULL;
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_initial_ctrc_release_preserves_failed_io_and_scope_responsibility)
{
	for (int fault = 1; fault <= 7; fault++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(200);
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 *owner = calloc(1, sizeof(*owner));
		FakeXactRecord fake;
		PGAlignedBlock initial, expected, disk;
		volatile bool caught = false;
		volatile RfPageProofDetailV1 result = RF_PAGE_PROOF_DETAIL_OK;

		make_ctrc_release(&fake, 0, TT_SLOT_COMMITTED);
		undo_header_feed(plan, &fake, 100);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		ctrc_base(&initial, 0, TT_SLOT_COMMITTED);
		if (fault == 5)
			((UndoSegmentHeaderData *)initial.data)->tt_slots[4].xid++;
		expected = initial;
		if (fault != 5)
			((UndoSegmentHeaderData *)expected.data)->tt_slots[4].flags
				= TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
		if (fault != 5)
			UT_ASSERT(cluster_undo_segment_header_seal(expected.data));
		canonical_file = tmpfile();
		UT_ASSERT(canonical_file != NULL && owner != NULL);
		UT_ASSERT_EQ(ftruncate(fileno(canonical_file), UNDO_SEGMENT_SIZE_BYTES), 0);
		UT_ASSERT_EQ(pwrite(fileno(canonical_file), initial.data, BLCKSZ, 0), BLCKSZ);
		UT_ASSERT_EQ(fsync(fileno(canonical_file)), 0);
		canonical_writes = canonical_syncs = 0;
		ctrc_io_fault = fault <= 4 ? fault : 0;
		canonical_file_io_error = fault == 6;
		reset_prepare_apply();
		UT_ASSERT(rf_side_online_production_owner_init_v1(owner, &authority, ctrc_authority_fresh,
														  19, true));
		UT_ASSERT(rf_side_online_production_bind_undo_v1(owner, &authority));
		if (fault == 7)
			owner->authority_arg = NULL;
		PG_TRY();
		{
			result = rf_side_online_production_apply_v1(plan, owner);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 3);
		if (!caught)
			UT_ASSERT_NE(result, RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(!owner->protected_set_complete && !owner->protected_set_active);
		UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0
				  && owner->undo_headers == NULL && owner->protected_plan == NULL);
		UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
		UT_ASSERT_EQ(pread(fileno(canonical_file), disk.data, BLCKSZ, 0), BLCKSZ);
		UT_ASSERT_EQ(
			memcmp(disk.data, fault <= 1 || fault >= 5 ? initial.data : expected.data, BLCKSZ), 0);
		if (fault >= 5)
			UT_ASSERT_EQ(canonical_writes, 0);
		ctrc_io_fault = 0;
		canonical_file_io_error = false;
		owner->authority_arg = &authority;
		UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, owner),
					 fault == 5 ? RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE : RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(pread(fileno(canonical_file), disk.data, BLCKSZ, 0), BLCKSZ);
		UT_ASSERT_EQ(memcmp(disk.data, expected.data, BLCKSZ), 0);
		UT_ASSERT_EQ(fclose(canonical_file), 0);
		canonical_file = NULL;
		free(owner);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

static bool
source_barrier_authority_fresh(void *arg)
{
	return arg != NULL && !(cold_source_drift && cold_source_calls != 0);
}

UT_TEST(test_cold_whole_source_barrier_precedes_every_native_effect)
{
	/* Actual immutable plan and production owner. Source durability and
	 * native CLOG/MX/cache IO are fixtures; canonical TT uses a real file. */
	for (int family = 0; family < 4; family++)
		for (int fault = 0; fault < 4; fault++) {
			RfSideOnlinePlanV1 *plan;
			ClusterRecoveryDutyKey duty = canonical_duty;
			ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
			ClusterThreadRecoveryAuthorityV1 authority = { .duty = &duty, .serial_guard = &guard };
			RfSideOnlineProductionOwnerV1 *owner = calloc(1, sizeof(*owner));
			volatile RfPageProofDetailV1 detail = RF_PAGE_PROOF_DETAIL_INTERNAL;
			volatile bool caught = false;
			FakeXactRecord fake;

			reset_prepare_apply();
			cold_source_barrier_test = true;
			cold_source_calls = cold_source_order = 0;
			cold_source_ready = false;
			cold_source_fail = fault == 1;
			cold_source_throw = fault == 2;
			cold_source_drift = fault == 3;
			canonical_writes = canonical_syncs = 0;
			cold_mx_preflights = cold_mx_projections = 0;
			cold_clog_applies = cold_clog_verifies = cold_clog_record_projections = 0;
			cold_cache_calls = control_observe_calls = 0;
			cold_mx_test = family == 0;
			cold_clog_test = family == 1 || family == 2;
			cold_clog_record_test = family == 1;
			control_scope_test = family != 2;
			cold_standalone_cache_test = cold_cache_test = cold_cache_init = family == 3;
			control_scope_fault = 0;
			control_fresh = true;
			duty.system_identifier = UINT64_C(0x11223344);
			duty.origin_owner_incarnation = 9;
			memset(duty.storage_uuid, 0x44, 16);
			if (family == 3)
				plan = cold_standalone_cache_plan(0);
			else {
				plan = space_online_plan(family == 2 ? 300 : 200);
				if (family == 2) {
					PGAlignedBlock initial;

					undo_header_bind(&fake, 0);
					undo_header_feed(plan, &fake, 100);
					undo_header_commit(&fake, 0, 802);
					undo_header_feed(plan, &fake, 200);
					canonical_file = tmpfile();
					UT_ASSERT(canonical_file != NULL);
					UT_ASSERT_EQ(ftruncate(fileno(canonical_file), UNDO_SEGMENT_SIZE_BYTES), 0);
					cluster_undo_segment_make_header_bytes(513, 3, initial.data);
					UT_ASSERT_EQ(pwrite(fileno(canonical_file), initial.data, BLCKSZ, 0), BLCKSZ);
					UT_ASSERT_EQ(fsync(fileno(canonical_file)), 0);
					canonical_expected_xid = 802;
					canonical_expected_wrap = 7;
				} else {
					RfPageOnlineRecordIdentityV1 identity;
					RfDetachedRecordPlanV1 record;
					uint8 uuid[16];
					int page = family == 0 ? 0 : 97;

					make_projection_record(&fake, family == 0 ? RM_MULTIXACT_ID : RM_CLOG_ID,
										   family == 0 ? XLOG_MULTIXACT_ZERO_OFF_PAGE
													   : CLOG_ZEROPAGE,
										   &page, sizeof(page));
					memset(uuid, 0x44, sizeof(uuid));
					identity = make_identity(&fake, uuid);
					set_identity_range(&fake, &identity, 100, 200);
					record = make_projection_record_plan(&fake);
					UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
								 RF_PAGE_PROOF_DETAIL_OK);
				}
				UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
			}
			UT_ASSERT(rf_side_online_production_owner_init_v1(
				owner, &authority, source_barrier_authority_fresh, 19, true));
			UT_ASSERT(rf_side_online_production_bind_undo_v1(owner, &authority));
			/* A read-only target preflight cannot consume this write barrier. */
			UT_ASSERT_EQ(rf_side_online_production_preflight_v1(plan, owner),
						 RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_EQ(cold_source_calls, 0);
			PG_TRY();
			{
				detail = rf_side_online_production_apply_v1(plan, owner);
			}
			PG_CATCH();
			{
				caught = true;
			}
			PG_END_TRY();
			UT_ASSERT_EQ(caught, fault == 2);
			if (!caught)
				UT_ASSERT_EQ(detail, fault == 0 ? RF_PAGE_PROOF_DETAIL_OK
												: RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
			UT_ASSERT_EQ(cold_source_calls, 1);
			UT_ASSERT_EQ(owner->protected_set_complete, fault == 0);
			if (fault != 0) {
				UT_ASSERT_EQ(canonical_writes + canonical_syncs + cold_clog_applies
								 + cold_clog_verifies + cold_clog_record_projections
								 + cold_mx_projections + cold_cache_calls + control_observe_calls
								 + prepare_apply.terminal_projection_stores,
							 0);
			}
			UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0
					  && owner->undo_headers == NULL && !owner->protected_set_active);
			if (canonical_file != NULL) {
				UT_ASSERT_EQ(fclose(canonical_file), 0);
				canonical_file = NULL;
			}
			free(owner);
			rf_side_online_plan_destroy_v1(&plan);
			cold_source_barrier_test = cold_source_fail = cold_source_throw = cold_source_drift
				= false;
			cold_mx_test = cold_clog_test = cold_clog_record_test = control_scope_test = false;
			cold_standalone_cache_test = cold_cache_test = cold_cache_init = false;
			cold_source_ready = false;
			canonical_expected_xid = 818;
			canonical_expected_wrap = 8;
		}
}

/* Logical command invalidations are deliberately not the standby cache
 * record. Physical xact_redo leaves them to logical decoding; committed
 * invalidations are independently owned by the complete COMMIT record. */
static RfPageProofDetailV1
logical_invalidation_feed(RfSideOnlinePlanV1 *plan, int index, int fault)
{
	PGAlignedBlock payload = { 0 };
	xl_xact_invals *invals = (xl_xact_invals *)payload.data;
	uint32 length = MinSizeOfXactInvals + 2 * sizeof(SharedInvalidationMessage);
	FakeXactRecord fake;
	uint8 uuid[16];
	RfPageOnlineRecordIdentityV1 identity;
	RfDetachedRecordPlanV1 record;

	invals->nmsgs = 2;
	invals->msgs[0].rc.id = SHAREDINVALRELCACHE_ID;
	invals->msgs[0].rc.dbId = 5;
	invals->msgs[0].rc.relId = 1259;
	invals->msgs[1].cc.id = 0;
	invals->msgs[1].cc.dbId = 5;
	invals->msgs[1].cc.hashValue = 0x11223344;
	switch (fault) {
	case 1:
		length = MinSizeOfXactInvals - 1;
		break;
	case 2:
		length--;
		break;
	case 3:
		length++;
		break;
	case 4:
		invals->nmsgs = -1;
		break;
	case 5:
		invals->nmsgs = INT_MAX;
		break;
	case 10:
		invals->nmsgs = 0;
		length = MinSizeOfXactInvals;
		break;
	}
	make_projection_record(&fake, RM_XACT_ID, XLOG_XACT_INVALIDATIONS, payload.data, length);
	fake.u.decoded.max_block_id = -1;
	fake.u.decoded.header.xl_scn = 700 + index;
	fake.u.decoded.header.xl_xid = fault == 9 ? InvalidTransactionId : 800;
	if (fault == 6)
		fake.u.decoded.header.xl_info |= XLOG_XACT_HAS_INFO;
	if (fault == 7)
		fake.u.decoded.header.xl_info |= 1;
	if (fault == 8)
		fake.u.decoded.max_block_id = 0;
	memset(uuid, 0x44, sizeof(uuid));
	identity = make_identity(&fake, uuid);
	set_identity_range(&fake, &identity, 100 + index * 100, 200 + index * 100);
	record = make_projection_record_plan(&fake);
	return rf_side_online_plan_feed_record_v1(plan, &record, &identity);
}

static RfSideOnlinePlanV1 *
logical_invalidation_plan(void)
{
	RfSideOnlinePlanV1 *plan = space_online_plan(300);
	UT_ASSERT_EQ(logical_invalidation_feed(plan, 0, 0), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(logical_invalidation_feed(plan, 1, 0), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	return plan;
}

UT_TEST(test_logical_invalidation_capture_is_bounded_and_owned)
{
	for (int fault = 0; fault <= 10; fault++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(200);
		RfSideOnlineOperationV1 operation;
		RfPageProofDetailV1 result = logical_invalidation_feed(plan, 0, fault);
		UT_ASSERT_EQ(result == RF_PAGE_PROOF_DETAIL_OK, fault == 0 || fault >= 9);
		if (result == RF_PAGE_PROOF_DETAIL_OK) {
			xl_xact_invals *invals;
			UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
			UT_ASSERT_EQ(operation.kind, RF_SIDE_ONLINE_OPERATION_NATIVE_CONTROL);
			UT_ASSERT_EQ(operation.identity.record.rmid, RM_XACT_ID);
			UT_ASSERT_EQ(operation.identity.record.info, XLOG_XACT_INVALIDATIONS);
			UT_ASSERT_EQ(operation.control_scn, 700);
			invals = (xl_xact_invals *)operation.owned_payload;
			UT_ASSERT_EQ(invals->nmsgs, fault == 10 ? 0 : 2);
			UT_ASSERT_EQ(operation.owned_payload_length,
						 MinSizeOfXactInvals + invals->nmsgs * sizeof(SharedInvalidationMessage));
			if (invals->nmsgs != 0) {
				UT_ASSERT_EQ(invals->msgs[0].rc.relId, 1259);
				UT_ASSERT_EQ(invals->msgs[1].cc.hashValue, 0x11223344);
			}
		}
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_logical_invalidations_do_not_execute_cache_or_terminal)
{
	RfSideOnlinePlanV1 *plan = logical_invalidation_plan();
	ClusterRecoveryDutyKey duty = canonical_duty;
	ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
	ClusterThreadRecoveryAuthorityV1 authority = { .duty = &duty, .serial_guard = &guard };
	RfSideOnlineProductionOwnerV1 owner;
	RfSideOnlineOperationV1 operation;

	cold_control_reset();
	cold_control_owner(&owner, &authority, &duty);
	for (uint32 i = 0; i < 2; i++) {
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, i, &operation));
		cold_source_calls = 0;
		UT_ASSERT_EQ(
			rf_side_online_production_apply_record_v1(plan, i, &operation.identity, &owner),
			RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(control_observe_calls, i + 1);
		UT_ASSERT_EQ(control_observed_scn, 700 + i);
		UT_ASSERT_EQ(cold_source_calls, 1);
		UT_ASSERT_EQ(cold_cache_calls + canonical_writes + canonical_syncs + cold_clog_applies
						 + cold_clog_verifies + cold_locks_calls
						 + prepare_apply.terminal_projection_stores,
					 0);
		UT_ASSERT(!owner.protected_set_complete && !owner.protected_set_active);
		UT_ASSERT(owner.protected_plan == NULL && canonical_scope == NULL
				  && canonical_writer_depth == 0 && DatabasePath == NULL);
	}
	cold_control_reset();
	control_scope_test = cold_control_test = false;
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_cold_logical_invalidation_rejects_wrong_context_and_unwinds)
{
	for (int fault = 0; fault < 13; fault++) {
		RfSideOnlinePlanV1 *plan = logical_invalidation_plan();
		ClusterRecoveryDutyKey duty = canonical_duty;
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority = { .duty = &duty, .serial_guard = &guard };
		static RfSideOnlineProductionOwnerV1 owner;
		RfSideOnlineOperationV1 operation;
		volatile bool caught = false;
		volatile RfPageProofDetailV1 result = RF_PAGE_PROOF_DETAIL_INTERNAL;

		cold_control_reset();
		cold_control_owner(&owner, &authority, &duty);
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
		switch (fault) {
		case 0:
			guard.mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
			break;
		case 1:
			cluster_node_id = 2;
			break;
		case 2:
			standbyState = STANDBY_INITIALIZED;
			break;
		case 3:
			cold_clog_denied = true;
			break;
		case 4:
			control_scope_fault = 8;
			break; /* A later record crosses the cut. */
		case 5:
			control_fresh = false;
			break;
		case 6:
			operation.identity.record.record_crc++;
			break;
		case 7:
			control_observe_throw = true;
			break;
		case 8:
			control_expire_after_observe = true;
			break;
		case 9:
			cold_control_drift = 1;
			break;
		case 10:
			cold_control_drift = 2;
			break;
		case 11:
			cold_control_drift = 3;
			break;
		case 12:
			operation.identity.record.info = XLOG_XACT_ASSIGNMENT;
			break;
		}
		PG_TRY();
		{
			result
				= rf_side_online_production_apply_record_v1(plan, 0, &operation.identity, &owner);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_NE(result, RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(caught, fault == 7);
		UT_ASSERT_EQ(control_observe_calls, fault >= 8 && fault <= 11 ? 1 : 0);
		UT_ASSERT_EQ(cold_cache_calls + canonical_writes + cold_clog_applies + cold_locks_calls
						 + prepare_apply.terminal_projection_stores,
					 0);
		UT_ASSERT(!owner.protected_set_complete && !owner.protected_set_active
				  && owner.protected_plan == NULL && canonical_scope == NULL
				  && canonical_writer_depth == 0 && DatabasePath == NULL);
		rf_side_online_plan_destroy_v1(&plan);
	}
	cold_control_reset();
	control_scope_test = cold_control_test = false;
}

UT_TEST(test_cold_record_dispatch_does_not_execute_future_controls_or_close_source)
{
	RfSideOnlinePlanV1 *plan = cold_standalone_cache_plan(0);
	ClusterRecoveryDutyKey duty = canonical_duty;
	ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
	ClusterThreadRecoveryAuthorityV1 authority = { .duty = &duty, .serial_guard = &guard };
	RfSideOnlineProductionOwnerV1 owner;
	RfSideOnlineOperationV1 operation;

	cold_control_reset();
	cold_control_test = false;
	cold_standalone_cache_test = cold_cache_test = cold_cache_init = cold_source_barrier_test
		= true;
	cold_control_owner(&owner, &authority, &duty);
	for (uint32 i = 0; i < 2; i++) {
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, i, &operation));
		cold_source_calls = 0;
		UT_ASSERT_EQ(
			rf_side_online_production_apply_record_v1(plan, i, &operation.identity, &owner),
			RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(cold_cache_calls, i + 1);
		UT_ASSERT_EQ(control_observe_calls, i + 1);
		UT_ASSERT_EQ(control_observed_scn, 700 + i);
		UT_ASSERT_EQ(cold_source_calls, 1);
		UT_ASSERT(!owner.protected_set_complete && !owner.protected_set_active);
		UT_ASSERT(owner.protected_plan == NULL && owner.undo_headers == NULL);
		UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0 && DatabasePath == NULL);
	}
	UT_ASSERT_EQ(canonical_writes + canonical_syncs + prepare_apply.terminal_projection_stores, 0);
	/* The existing full-source entry keeps its original completion meaning. */
	cold_source_calls = 0;
	UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(cold_cache_calls, 4);
	UT_ASSERT(owner.protected_set_complete);
	control_scope_test = cold_standalone_cache_test = cold_cache_test = cold_cache_init = false;
	cold_source_barrier_test = false;
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_cold_record_dispatch_requires_exact_record_and_original_cold_scope)
{
	for (int fault = 0; fault < 18; fault++) {
		RfSideOnlinePlanV1 *plan = cold_standalone_cache_plan(0);
		ClusterRecoveryDutyKey duty = canonical_duty;
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority = { .duty = &duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		RfSideOnlineOperationV1 operation;
		uint32 index = 0;

		cold_control_reset();
		cold_control_test = false;
		cold_standalone_cache_test = cold_cache_test = cold_cache_init = true;
		cold_control_owner(&owner, &authority, &duty);
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
		switch (fault) {
		case 0:
			index = 1;
			break;
		case 1:
			index = UINT32_MAX;
			break;
		case 2:
			operation.identity.participant_index++;
			break;
		case 3:
			operation.identity.reserved_zero++;
			break;
		case 4:
			operation.identity.record.system_identifier++;
			break;
		case 5:
			operation.identity.record.storage_uuid[15]++;
			break;
		case 6:
			operation.identity.record.origin_thread++;
			break;
		case 7:
			operation.identity.record.timeline_id++;
			break;
		case 8:
			operation.identity.record.read_rec_ptr++;
			break;
		case 9:
			operation.identity.record.end_rec_ptr++;
			break;
		case 10:
			operation.identity.record.record_crc++;
			break;
		case 11:
			operation.identity.record.rmid++;
			break;
		case 12:
			operation.identity.record.info++;
			break;
		case 13:
			operation.identity.record.reserved_zero++;
			break;
		case 14:
			operation.identity.record.reserved_zero2++;
			break;
		case 15:
			guard.mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
			break;
		case 16:
			owner.undo_authority = NULL;
			break;
		case 17:
			duty.origin_thread_id = 2;
			break;
		}
		UT_ASSERT_NE(
			rf_side_online_production_apply_record_v1(plan, index, &operation.identity, &owner),
			RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(cold_source_calls + cold_cache_calls + control_observe_calls, 0);
		UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0);
		UT_ASSERT(!owner.protected_set_complete && !owner.protected_set_active);
		UT_ASSERT(owner.protected_plan == NULL);
		control_scope_test = cold_standalone_cache_test = cold_cache_test = cold_cache_init = false;
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_record_dispatch_keeps_whole_source_preflight_and_error_cleanup)
{
	for (int fault = 0; fault < 7; fault++) {
		RfSideOnlinePlanV1 *plan = cold_standalone_cache_plan(fault == 0 ? 1 : 0);
		ClusterRecoveryDutyKey duty = canonical_duty;
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority = { .duty = &duty, .serial_guard = &guard };
		static RfSideOnlineProductionOwnerV1 owner;
		RfSideOnlineOperationV1 operation;
		volatile bool caught = false;
		volatile RfPageProofDetailV1 detail = RF_PAGE_PROOF_DETAIL_INTERNAL;

		cold_control_reset();
		cold_control_test = false;
		cold_standalone_cache_test = cold_cache_test = cold_cache_init = cold_source_barrier_test
			= true;
		cold_control_owner(&owner, &authority, &duty);
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
		cold_source_fail = fault == 1;
		cold_source_throw = fault == 2;
		cold_cache_throw = fault == 3;
		cold_cache_expire = fault == 4;
		control_scope_denied = fault == 5;
		control_scope_fault = fault == 6 ? 8 : 0;
		PG_TRY();
		{
			detail
				= rf_side_online_production_apply_record_v1(plan, 0, &operation.identity, &owner);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_NE(detail, RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(caught, fault == 2 || fault == 3);
		UT_ASSERT_EQ(cold_cache_calls, fault == 3 || fault == 4 ? 1 : 0);
		UT_ASSERT_EQ(cold_source_calls, fault >= 1 && fault <= 4 ? 1 : 0);
		UT_ASSERT_EQ(control_observe_calls, 0);
		UT_ASSERT(!owner.protected_set_complete && !owner.protected_set_active);
		UT_ASSERT(owner.protected_plan == NULL && owner.undo_headers == NULL);
		UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0 && DatabasePath == NULL);
		cold_source_fail = cold_source_throw = cold_cache_throw = cold_cache_expire = false;
		cold_clog_denied = control_scope_denied = false;
		control_scope_fault = 0;
		cold_source_barrier_test = control_scope_test = cold_standalone_cache_test = cold_cache_test
			= cold_cache_init = false;
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_record_plan_selector_refuses_history_pi_and_nonlocal_effects)
{
	for (int variant = 0; variant < 4; variant++) {
		RfSideOnlinePlanV1 *plan;
		RfPageProofDetailV1 detail;
		RfSideOnlineOperationV1 operation;
		RfSideOnlineApplyOpsV1 ops = { 0 };
		ApplyCapture capture = { 0 };
		FakeXactRecord fake;
		XLogRecPtr redo = 200;

		if (variant == 0)
			plan = cold_standalone_cache_budget_plan(0, true, 0, &detail);
		else {
			plan = space_online_plan_redo(200, variant == 1 ? &redo : NULL);
			if (variant == 1) {
				uint8 uuid[16];
				RfPageOnlineRecordIdentityV1 identity;
				RfDetachedRecordPlanV1 record;

				make_projection_record(&fake, RM_XLOG_ID, XLOG_NOOP, NULL, 0);
				fake.u.decoded.max_block_id = -1;
				memset(uuid, 0x44, sizeof(uuid));
				identity = make_identity(&fake, uuid);
				set_identity_range(&fake, &identity, 100, 200);
				record = make_projection_record_plan(&fake);
				UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
							 RF_PAGE_PROOF_DETAIL_OK);
			} else if (variant == 2) {
				make_commit(&fake, 800, 901, 123456, false);
				UT_ASSERT_EQ(space_commit_feed(plan, &fake, 100), RF_PAGE_PROOF_DETAIL_OK);
			} else {
				make_ctrc_release(&fake, 0, TT_SLOT_COMMITTED);
				undo_header_feed(plan, &fake, 100);
			}
			UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		}
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
		ops.arg = &capture;
		ops.source_thread = 3;
		ops.begin_protected_set = capture_begin;
		ops.end_protected_set = capture_end;
		ops.preflight_control = ops.preflight_xact = ops.preflight_undo = accept_preflight;
		ops.apply_control = ops.apply_xact = capture_apply;
		ops.apply_undo = capture_apply_undo;
		UT_ASSERT_NE(rf_side_online_plan_apply_record_v1(plan, 0, &operation.identity, &ops),
					 RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(capture.begin_count + capture.count + capture.undo_count, 0);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_record_dispatch_executes_only_selected_native_projection)
{
	for (int family = 0; family < 2; family++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(300);
		ClusterRecoveryDutyKey duty = canonical_duty;
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority = { .duty = &duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		RfSideOnlineOperationV1 operation;
		for (int n = 0; n < 2; n++) {
			FakeXactRecord fake;
			uint8 uuid[16];
			RfPageOnlineRecordIdentityV1 identity;
			RfDetachedRecordPlanV1 record;
			int page = family == 0 ? 97 : 0;
			make_projection_record(&fake, family == 0 ? RM_CLOG_ID : RM_MULTIXACT_ID,
								   family == 0 ? CLOG_ZEROPAGE : XLOG_MULTIXACT_ZERO_OFF_PAGE,
								   &page, sizeof(page));
			memset(uuid, 0x44, sizeof(uuid));
			identity = make_identity(&fake, uuid);
			set_identity_range(&fake, &identity, 100 + n * 100, 200 + n * 100);
			record = make_projection_record_plan(&fake);
			UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
						 RF_PAGE_PROOF_DETAIL_OK);
		}
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		cold_control_reset();
		cold_control_test = false;
		cold_clog_test = cold_clog_record_test = family == 0;
		cold_mx_test = family == 1;
		cold_clog_applies = cold_clog_verifies = cold_clog_record_projections = 0;
		cold_mx_preflights = cold_mx_projections = 0;
		cold_control_owner(&owner, &authority, &duty);
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
		UT_ASSERT_EQ(
			rf_side_online_production_apply_record_v1(plan, 0, &operation.identity, &owner),
			RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(cold_clog_applies, family == 0 ? 1 : 0);
		UT_ASSERT_EQ(cold_clog_verifies, family == 0 ? 1 : 0);
		UT_ASSERT_EQ(cold_clog_record_projections + cold_mx_projections, 1);
		UT_ASSERT_EQ(cold_source_calls, 1);
		UT_ASSERT(!owner.protected_set_complete && !owner.protected_set_active);
		UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0);
		cold_clog_test = cold_clog_record_test = cold_mx_test = control_scope_test = false;
		rf_side_online_plan_destroy_v1(&plan);
	}
}

/* The selected-prefix preparation must not borrow a later terminal, REUSE
 * or FPI. These are the real immutable decoder and transition functions;
 * no IO/authority stub participates in producing these private images. */
UT_TEST(test_undo_record_prefix_stops_before_future_terminal_and_rebind)
{
	RfSideOnlinePlanV1 *plan = undo_repeated_slot_plan_sources(false, true);
	PGAlignedBlock base, saved;
	RfSideUndoHeaderImageV1 out = { 0 }, whole;
	RfSideOnlineOperationV1 operation;

	cluster_undo_segment_make_header_bytes(513, 3, base.data);
	saved = base;
	for (uint32 i = 0; i < 4; i++) {
		const TTSlot *slot;

		UT_ASSERT(rf_side_online_plan_operation_v1(plan, i, &operation));
		UT_ASSERT_EQ(rf_side_online_plan_prepare_undo_record_v1(plan, i, &operation.identity,
																base.data, &out),
					 RF_PAGE_PROOF_DETAIL_OK);
		slot = &((UndoSegmentHeaderData *)out.page.data)->tt_slots[4];
		UT_ASSERT_EQ(slot->xid, i < 2 ? 802 : 818);
		UT_ASSERT_EQ(slot->wrap, i < 2 ? 7 : 8);
		UT_ASSERT_EQ(slot->status, i % 2 ? TT_SLOT_COMMITTED : TT_SLOT_ACTIVE);
		UT_ASSERT_EQ(slot->commit_scn, i % 2 ? 999 : 0);
		UT_ASSERT_EQ(out.operation_count, i + 1);
		UT_ASSERT_EQ(out.source_index, i);
		UT_ASSERT(memcmp(base.data, saved.data, BLCKSZ) == 0);
	}
	UT_ASSERT_EQ(rf_side_online_plan_prepare_undo_header_v1(plan, 3, 513, base.data, &whole),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(memcmp(out.page.data, whole.page.data, BLCKSZ) == 0);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_undo_record_prefix_keeps_init_and_reuse_boundaries)
{
	RfSideOnlinePlanV1 *plan = space_online_plan(600);
	FakeXactRecord fake;
	RfSideOnlineOperationV1 operation;
	RfSideUndoHeaderImageV1 out = { 0 };
	struct {
		xl_cluster_undo_segment_init record;
		uint8 page[BLCKSZ];
	} init = { 0 };
	struct {
		xl_undo_segment_reuse record;
		uint8 page[BLCKSZ];
	} reuse = { 0 };

	init.record.instance = reuse.record.instance = 3;
	init.record.segment_id = reuse.record.segment_id = 513;
	reuse.record.new_generation = 1;
	cluster_undo_segment_make_header_bytes(513, 3, (char *)init.page);
	memcpy(reuse.page, init.page, BLCKSZ);
	((UndoSegmentHeaderData *)reuse.page)->wrap_count = 1;
	make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_INIT, &init, sizeof(init));
	undo_header_feed(plan, &fake, 100);
	undo_header_bind(&fake, 0);
	undo_header_feed(plan, &fake, 200);
	undo_header_commit(&fake, 0, 802);
	undo_header_feed(plan, &fake, 300);
	make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_REUSE, &reuse,
						   sizeof(reuse));
	undo_header_feed(plan, &fake, 400);
	undo_header_bind(&fake, 1);
	undo_header_feed(plan, &fake, 500);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	for (uint32 i = 0; i < 5; i++) {
		const UndoSegmentHeaderData *header;

		UT_ASSERT(rf_side_online_plan_operation_v1(plan, i, &operation));
		UT_ASSERT_EQ(
			rf_side_online_plan_prepare_undo_record_v1(plan, i, &operation.identity, NULL, &out),
			RF_PAGE_PROOF_DETAIL_OK);
		header = (const UndoSegmentHeaderData *)out.page.data;
		UT_ASSERT_EQ(header->wrap_count, i < 3 ? 0 : 1);
		UT_ASSERT_EQ(header->tt_slots[4].status, i == 2				? TT_SLOT_COMMITTED
												 : i == 1 || i == 4 ? TT_SLOT_ACTIVE
																	: 0);
		UT_ASSERT(out.has_full_image);
		UT_ASSERT_EQ(out.operation_count, i + 1);
		UT_ASSERT_EQ(out.source_index, i);
	}
	rf_side_online_plan_destroy_v1(&plan);
}

static void
record_prefix_fpi(RfSideOnlinePlanV1 *plan, XLogRecPtr begin, uint8 byte)
{
	FakeXactRecord fake;
	struct {
		xl_undo_block_write record;
		uint8 page[BLCKSZ];
	} fpi = { 0 };
	fpi.record.instance = 3;
	fpi.record.segment_id = 513;
	fpi.record.block_no = 9;
	fpi.record.has_fpi = 1;
	memset(fpi.page, byte, BLCKSZ);
	make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE, &fpi, sizeof(fpi));
	undo_header_feed(plan, &fake, begin);
}

UT_TEST(test_undo_record_prefix_never_installs_a_later_block_image)
{
	RfSideOnlinePlanV1 *plan = space_online_plan(400);
	RfSideOnlineOperationV1 operation;
	RfSideUndoBlockImageV1 out = { 0 }, whole;
	FakeXactRecord fake;
	PGAlignedBlock expected;

	record_prefix_fpi(plan, 100, 0x31);
	make_undo_delta(&fake);
	undo_header_feed(plan, &fake, 200);
	record_prefix_fpi(plan, 300, 0x73);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	for (uint32 i = 0; i < 3; i++) {
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, i, &operation));
		UT_ASSERT(cluster_undo_prepare_block_v1(
			&operation.undo, operation.owned_payload, operation.owned_payload_length,
			operation.identity.record.end_rec_ptr, i ? expected.data : NULL, expected.data));
		UT_ASSERT_EQ(
			rf_side_online_plan_prepare_undo_record_v1(plan, i, &operation.identity, NULL, &out),
			RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(memcmp(out.page.data, expected.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(((UndoBlockHeader *)out.page.data)->block_lsn, 200 + i * 100);
		UT_ASSERT_EQ(out.source_index, i);
		UT_ASSERT_EQ(out.operation_count, i + 1);
	}
	UT_ASSERT_EQ(rf_side_online_plan_prepare_undo_block_v1(plan, 3, 513, 9, &whole),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(memcmp(out.page.data, whole.page.data, BLCKSZ) == 0);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_undo_record_prefix_rejects_future_fpi_and_ahead_data)
{
	/* A later full image cannot repair the selected unanchored delta. */
	RfSideOnlinePlanV1 *plan = space_online_plan(300);
	RfSideOnlineOperationV1 operation;
	RfSideUndoHeaderImageV1 out, saved, final;
	FakeXactRecord fake;
	PGAlignedBlock base;

	make_undo_delta(&fake);
	undo_header_feed(plan, &fake, 100);
	record_prefix_fpi(plan, 200, 0x31);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
	memset(&saved, 0x92, sizeof(saved));
	out = saved;
	UT_ASSERT_NE(
		rf_side_online_plan_prepare_undo_record_v1(plan, 0, &operation.identity, NULL, &out),
		RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	rf_side_online_plan_destroy_v1(&plan);

	plan = undo_repeated_slot_plan_sources(false, false);
	cluster_undo_segment_make_header_bytes(513, 3, base.data);
	UT_ASSERT_EQ(rf_side_online_plan_prepare_undo_header_v1(plan, 3, 513, base.data, &final),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
	for (int drift = 0; drift < 2; drift++) {
		PGAlignedBlock observed = final.page;
		if (drift)
			((UndoSegmentHeaderData *)observed.data)->wrap_count++;
		out = saved;
		UT_ASSERT_NE(rf_side_online_plan_prepare_undo_record_v1(plan, 0, &operation.identity,
																observed.data, &out),
					 RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	}
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_undo_record_prefix_selector_requires_complete_identity_and_replay_role)
{
	for (unsigned fault = 0; fault < 17; fault++) {
		XLogRecPtr redo = 300;
		RfSideOnlinePlanV1 *plan = space_online_plan_redo(300, fault == 14 ? &redo : NULL);
		RfSideOnlineOperationV1 operation;
		RfSideUndoHeaderImageV1 out, saved;
		FakeXactRecord fake;
		PGAlignedBlock base;
		uint32 index = 0;

		undo_header_bind(&fake, 0);
		undo_header_feed(plan, &fake, 100);
		undo_header_commit(&fake, 0, 802);
		undo_header_feed(plan, &fake, 200);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
		if (fault == 0)
			operation.identity.record.system_identifier++;
		if (fault == 1)
			operation.identity.record.storage_uuid[15]++;
		if (fault == 2)
			operation.identity.record.origin_thread++;
		if (fault == 3)
			operation.identity.record.timeline_id++;
		if (fault == 4)
			operation.identity.record.read_rec_ptr++;
		if (fault == 5)
			operation.identity.record.end_rec_ptr++;
		if (fault == 6)
			operation.identity.record.record_crc++;
		if (fault == 7)
			operation.identity.record.rmid++;
		if (fault == 8)
			operation.identity.record.info++;
		if (fault == 9)
			operation.identity.participant_index++;
		if (fault == 10)
			operation.identity.reserved_zero = 1;
		if (fault == 11)
			index = UINT32_MAX;
		if (fault == 12)
			index = 2;
		cluster_undo_segment_make_header_bytes(513, 3, base.data);
		memset(&saved, 0xa6, sizeof(saved));
		out = saved;
		UT_ASSERT_NE(rf_side_online_plan_prepare_undo_record_v1(
						 fault == 15 ? NULL : plan, index, fault == 13 ? NULL : &operation.identity,
						 base.data, fault == 16 ? NULL : &out),
					 RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
		rf_side_online_plan_destroy_v1(&plan);
	}
	{
		RfPageProofDetailV1 detail;
		RfSideOnlinePlanV1 *pi = cold_cache_budget_plan(0, true, 1, &detail);
		RfSideOnlineOperationV1 operation;
		RfSideUndoHeaderImageV1 out = { 0 };
		PGAlignedBlock base;

		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(pi), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(rf_side_online_plan_operation_v1(pi, 0, &operation));
		cluster_undo_segment_make_header_bytes(513, 3, base.data);
		UT_ASSERT_NE(
			rf_side_online_plan_prepare_undo_record_v1(pi, 0, &operation.identity, base.data, &out),
			RF_PAGE_PROOF_DETAIL_OK);
		rf_side_online_plan_destroy_v1(&pi);
	}
}

UT_TEST(test_undo_record_prefix_is_not_complete_source_acceptance)
{
	RfSideOnlinePlanV1 *plan = undo_repeated_slot_plan_sources(true, false);
	RfSideOnlineOperationV1 operation;
	RfSideUndoHeaderImageV1 out, saved;
	PGAlignedBlock base;

	cluster_undo_segment_make_header_bytes(513, 3, base.data);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
	UT_ASSERT_EQ(
		rf_side_online_plan_prepare_undo_record_v1(plan, 0, &operation.identity, base.data, &out),
		RF_PAGE_PROOF_DETAIL_OK);
	memset(&saved, 0x32, sizeof(saved));
	out = saved;
	UT_ASSERT_NE(rf_side_online_plan_prepare_undo_header_v1(plan, 3, 513, base.data, &out),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 3, &operation));
	UT_ASSERT_NE(
		rf_side_online_plan_prepare_undo_record_v1(plan, 3, &operation.identity, base.data, &out),
		RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_undo_record_prefix_refuses_ambiguous_sources_and_other_effects)
{
	for (int variant = 0; variant < 2; variant++) {
		RfSideOnlinePlanRequestV1 request = { 0 };
		RfContributorStreamCutV1 cuts[2] = { { 0 } };
		RfSideOnlinePlanV1 *plan = NULL;
		RfSideOnlineOperationV1 operation;
		RfSideUndoHeaderImageV1 out, saved;
		PGAlignedBlock base;

		request.system_identifier = UINT64_C(0x11223344);
		memset(request.storage_uuid, 0x44, 16);
		request.participant_count = variant == 0 ? 2 : 1;
		request.physical_cuts = cuts;
		for (uint32 i = 0; i < request.participant_count; i++) {
			cuts[i].failed_thread = 3;
			cuts[i].timeline_id = 7;
			cuts[i].flags = RF_CONTRIBUTOR_CUT_COMPLETE;
			cuts[i].scan_begin_inclusive = 100;
			cuts[i].scan_end_exclusive = 200;
			cuts[i].origin_owner_incarnation = 41 + i;
		}
		UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
		for (uint32 i = 0; i < request.participant_count; i++) {
			FakeXactRecord fake;
			RfPageOnlineRecordIdentityV1 identity;
			RfDetachedRecordPlanV1 record;
			int page = 17;

			if (variant == 0)
				undo_header_bind(&fake, 0);
			else
				make_projection_record(&fake, RM_CLOG_ID, CLOG_ZEROPAGE, &page, sizeof(page));
			identity = make_identity(&fake, request.storage_uuid);
			identity.participant_index = i;
			set_identity_range(&fake, &identity, 100, 200);
			record
				= variant == 0 ? make_undo_record_plan(&fake) : make_projection_record_plan(&fake);
			UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
						 RF_PAGE_PROOF_DETAIL_OK);
		}
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		cluster_undo_segment_make_header_bytes(513, 3, base.data);
		memset(&saved, 0x18, sizeof(saved));
		for (uint32 i = 0; i < request.participant_count; i++) {
			out = saved;
			UT_ASSERT(rf_side_online_plan_operation_v1(plan, i, &operation));
			UT_ASSERT_NE(rf_side_online_plan_prepare_undo_record_v1(plan, i, &operation.identity,
																	base.data, &out),
						 RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
		}
		rf_side_online_plan_destroy_v1(&plan);
	}
}

static RfSideOnlinePlanV1 *
cold_undo_block_record_plan(bool conflict, bool multi)
{
	RfSideOnlinePlanV1 *plan = space_online_plan(600);
	FakeXactRecord fake;

	undo_header_bind(&fake, 0);
	undo_header_feed(plan, &fake, 100);
	if (multi) {
		struct {
			xl_undo_block_write_multi record;
			uint8 image[BLCKSZ];
		} fpi = { 0 };
		fpi.record.instance = 3;
		fpi.record.segment_id = 513;
		fpi.record.block_no = 9;
		fpi.record.has_fpi = 1;
		memset(fpi.image, 0x31, BLCKSZ);
		make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE_MULTI, &fpi,
							   sizeof(fpi));
		undo_header_feed(plan, &fake, 200);
	} else
		record_prefix_fpi(plan, 200, 0x31);
	make_undo_delta(&fake);
	undo_header_feed(plan, &fake, 300);
	record_prefix_fpi(plan, 400, 0x52);
	undo_header_commit(&fake, 0, conflict ? 818 : 802);
	undo_header_feed(plan, &fake, 500);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	return plan;
}

static void
cold_undo_block_record_open(PGAlignedBlock *header, RfSideOnlineProductionOwnerV1 *owner,
							ClusterThreadRecoveryAuthorityV1 *authority)
{
	UndoSegmentHeaderData *h = (UndoSegmentHeaderData *)header->data;

	reset_prepare_apply();
	canonical_file = tmpfile();
	UT_ASSERT(canonical_file != NULL);
	UT_ASSERT_EQ(ftruncate(fileno(canonical_file), UNDO_SEGMENT_SIZE_BYTES), 0);
	cluster_undo_segment_make_header_bytes(513, 3, header->data);
	h->tt_slots[4].xid = 802;
	h->tt_slots[4].wrap = 7;
	h->tt_slots[4].status = TT_SLOT_ACTIVE;
	h->tt_slots[4].first_undo_block = (UBA)InvalidUba_init;
	UT_ASSERT(pwrite(fileno(canonical_file), header->data, BLCKSZ, 0) == BLCKSZ);
	UT_ASSERT_EQ(fsync(fileno(canonical_file)), 0);
	canonical_writes = canonical_syncs = cold_source_calls = 0;
	cold_clog_applies = cold_clog_verifies = 0;
	cold_clog_test = cold_source_barrier_test = true;
	UT_ASSERT(rf_side_online_production_owner_init_v1(owner, authority, canonical_authority_fresh,
													  19, true));
	UT_ASSERT(rf_side_online_production_bind_undo_v1(owner, authority));
}

static void
cold_undo_block_record_close(RfSideOnlineProductionOwnerV1 *owner)
{
	UT_ASSERT(!owner->protected_set_complete && !owner->protected_set_active);
	UT_ASSERT(owner->undo_headers == NULL && owner->protected_plan == NULL);
	UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0);
	UT_ASSERT_EQ(cold_clog_applies + cold_clog_verifies + prepare_apply.terminal_projection_stores,
				 0);
	UT_ASSERT_EQ(fclose(canonical_file), 0);
	canonical_file = NULL;
	cold_clog_test = cold_source_barrier_test = false;
	cold_source_fail = cold_source_throw = false;
	canonical_file_io_error = canonical_fail_data_write = canonical_corrupt_data_write = false;
	record_target_drift = 0;
	head_data_drift = false;
	record_sync_failure = false;
	ctrc_io_fault = 0;
}

UT_TEST(test_cold_undo_block_record_installs_only_selected_prefix)
{
	for (int multi = 0; multi < 2; multi++) {
		RfSideOnlinePlanV1 *plan = cold_undo_block_record_plan(false, multi != 0);
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		PGAlignedBlock header, after;
		RfSideOnlineOperationV1 operation;

		cold_undo_block_record_open(&header, &owner, &authority);
		for (uint32 i = 1; i <= 3; i++) {
			canonical_writes = canonical_syncs = cold_source_calls = 0;
			UT_ASSERT(rf_side_online_plan_operation_v1(plan, i, &operation));
			UT_ASSERT_EQ(
				rf_side_online_production_apply_record_v1(plan, i, &operation.identity, &owner),
				RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 9 * BLCKSZ) == BLCKSZ);
			UT_ASSERT_EQ(((UndoBlockHeader *)after.data)->block_lsn, 200 + 100 * i);
			UT_ASSERT_EQ((uint8)after.data[128], i == 3 ? 0x52 : 0x31);
			UT_ASSERT_EQ((uint8)after.data[sizeof(UndoBlockHeader)], i == 1	  ? 0x31
																	 : i == 2 ? 0x6b
																			  : 0x52);
			UT_ASSERT_EQ(canonical_writes, 1);
			UT_ASSERT_EQ(canonical_syncs, 1);
			UT_ASSERT_EQ(cold_source_calls, 1);
			UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 0) == BLCKSZ);
			UT_ASSERT(memcmp(after.data, header.data, BLCKSZ) == 0);
			UT_ASSERT(!owner.protected_set_complete && !owner.protected_set_active);
			/* The same exact record is durable again without rewriting it. */
			canonical_writes = canonical_syncs = cold_source_calls = 0;
			UT_ASSERT_EQ(
				rf_side_online_production_apply_record_v1(plan, i, &operation.identity, &owner),
				RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_EQ(canonical_writes, 0);
			UT_ASSERT_EQ(canonical_syncs, 1);
		}
		cold_undo_block_record_close(&owner);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_undo_block_record_requires_prefix_header_and_complete_source)
{
	for (int fault = 0; fault < 10; fault++) {
		RfSideOnlinePlanV1 *plan = cold_undo_block_record_plan(fault == 8, false);
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		RfSideOnlineOperationV1 operation;
		PGAlignedBlock header, block = { 0 }, after;
		UndoSegmentHeaderData *h = (UndoSegmentHeaderData *)header.data;

		cold_undo_block_record_open(&header, &owner, &authority);
		if (fault == 0)
			memset(&h->tt_slots[4], 0, sizeof(TTSlot));
		if (fault == 1) {
			h->tt_slots[4].status = TT_SLOT_COMMITTED;
			h->tt_slots[4].commit_scn = 999;
			h->tt_slots[4].flags = TT_FLAGS_RESERVED;
		}
		if (fault == 2)
			h->wrap_count = 1;
		if (fault <= 2)
			UT_ASSERT(pwrite(fileno(canonical_file), header.data, BLCKSZ, 0) == BLCKSZ);
		if (fault == 3 || fault == 4) {
			((UndoBlockHeader *)block.data)->block_lsn = fault == 3 ? 301 : 300;
			UT_ASSERT(pwrite(fileno(canonical_file), block.data, BLCKSZ, 9 * BLCKSZ) == BLCKSZ);
		}
		if (fault == 5)
			UT_ASSERT_EQ(ftruncate(fileno(canonical_file), BLCKSZ), 0);
		canonical_file_io_error = fault == 6;
		cold_source_fail = fault == 7;
		if (fault == 9)
			guard.mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
		UT_ASSERT_NE(
			rf_side_online_production_apply_record_v1(plan, 1, &operation.identity, &owner),
			RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(canonical_writes, 0);
		UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 0) == BLCKSZ);
		UT_ASSERT(memcmp(after.data, header.data, BLCKSZ) == 0);
		cold_undo_block_record_close(&owner);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_undo_block_record_io_failure_never_closes_source)
{
	for (int fault = 0; fault < 4; fault++) {
		RfSideOnlinePlanV1 *plan = cold_undo_block_record_plan(false, false);
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		RfSideOnlineOperationV1 operation;
		PGAlignedBlock header, after;
		volatile bool caught = false;
		volatile RfPageProofDetailV1 detail = RF_PAGE_PROOF_DETAIL_INTERNAL;

		cold_undo_block_record_open(&header, &owner, &authority);
		canonical_fail_data_write = fault == 0;
		canonical_corrupt_data_write = fault == 1;
		ctrc_io_fault = fault == 2 ? 3 : 0;
		cold_source_throw = fault == 3;
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
		PG_TRY();
		{
			detail
				= rf_side_online_production_apply_record_v1(plan, 1, &operation.identity, &owner);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_NE(detail, RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(caught, fault >= 2);
		UT_ASSERT_EQ(canonical_writes, fault == 3 ? 0 : 1);
		UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 0) == BLCKSZ);
		UT_ASSERT(memcmp(after.data, header.data, BLCKSZ) == 0);
		cold_undo_block_record_close(&owner);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_undo_block_record_requires_dedicated_record_consumer)
{
	RfSideOnlinePlanV1 *plan = cold_undo_block_record_plan(false, false);
	RfSideOnlineOperationV1 operation;
	RfSideOnlineApplyOpsV1 ops = { 0 };
	ApplyCapture capture = { 0 };

	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
	ops.arg = &capture;
	ops.source_thread = 3;
	ops.begin_protected_set = capture_begin;
	ops.end_protected_set = capture_end;
	ops.preflight_undo = ops.preflight_xact = accept_preflight;
	ops.apply_xact = capture_apply;
	ops.apply_undo = capture_apply_undo;
	UT_ASSERT_NE(rf_side_online_plan_apply_record_v1(plan, 1, &operation.identity, &ops),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(capture.begin_count + capture.count + capture.undo_count, 0);
	rf_side_online_plan_destroy_v1(&plan);
}


UT_TEST(test_cold_undo_block_record_rechecks_file_and_header_around_write)
{
	for (int fault = 1; fault <= 4; fault++) {
		RfSideOnlinePlanV1 *plan = cold_undo_block_record_plan(false, false);
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		RfSideOnlineOperationV1 operation;
		PGAlignedBlock header;

		cold_undo_block_record_open(&header, &owner, &authority);
		record_target_drift = fault;
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
		UT_ASSERT_NE(
			rf_side_online_production_apply_record_v1(plan, 1, &operation.identity, &owner),
			RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(canonical_writes, fault <= 2 ? 0 : 1);
		cold_undo_block_record_close(&owner);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_undo_block_record_idempotent_bytes_still_require_sync)
{
	RfSideOnlinePlanV1 *plan = cold_undo_block_record_plan(false, false);
	ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
	ClusterThreadRecoveryAuthorityV1 authority
		= { .duty = &canonical_duty, .serial_guard = &guard };
	RfSideOnlineProductionOwnerV1 owner;
	RfSideOnlineOperationV1 operation;
	PGAlignedBlock header, before, after;

	cold_undo_block_record_open(&header, &owner, &authority);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
	UT_ASSERT_EQ(rf_side_online_production_apply_record_v1(plan, 1, &operation.identity, &owner),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(pread(fileno(canonical_file), before.data, BLCKSZ, 9 * BLCKSZ) == BLCKSZ);
	canonical_writes = canonical_syncs = cold_source_calls = 0;
	record_sync_failure = true;
	UT_ASSERT_NE(rf_side_online_production_apply_record_v1(plan, 1, &operation.identity, &owner),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(canonical_writes, 0);
	UT_ASSERT_EQ(canonical_syncs, 1);
	UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 9 * BLCKSZ) == BLCKSZ);
	UT_ASSERT(memcmp(before.data, after.data, BLCKSZ) == 0);
	cold_undo_block_record_close(&owner);
	rf_side_online_plan_destroy_v1(&plan);
}


static RfSideOnlinePlanV1 *
cold_undo_header_record_plan(bool reuse, bool conflict)
{
	RfSideOnlinePlanV1 *plan = space_online_plan(600);
	FakeXactRecord fake;
	struct {
		xl_cluster_undo_segment_init record;
		uint8 page[BLCKSZ];
	} init = { 0 };
	struct {
		xl_undo_segment_reuse record;
		uint8 page[BLCKSZ];
	} rebirth = { 0 };
	xl_undo_segment_recycle recycle = { .segment_id = 513,
										.instance = 3,
										.old_state = SEGMENT_COMMITTED,
										.new_state = SEGMENT_RECYCLABLE };

	init.record.instance = rebirth.record.instance = 3;
	init.record.segment_id = rebirth.record.segment_id = 513;
	rebirth.record.new_generation = 1;
	cluster_undo_segment_make_header_bytes(513, 3, (char *)init.page);
	memcpy(rebirth.page, init.page, BLCKSZ);
	((UndoSegmentHeaderData *)rebirth.page)->wrap_count = 1;
	if (reuse) {
		make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_RECYCLE, &recycle,
							   sizeof(recycle));
		undo_header_feed(plan, &fake, 100);
		make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_REUSE, &rebirth,
							   sizeof(rebirth));
	} else
		make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_INIT, &init,
							   sizeof(init));
	undo_header_feed(plan, &fake, reuse ? 200 : 100);
	undo_header_bind(&fake, reuse ? 1 : 0);
	undo_header_feed(plan, &fake, 300);
	record_prefix_fpi(plan, 400, 0x31);
	undo_header_commit(&fake, reuse ? 1 : 0, conflict ? 818 : 802);
	undo_header_feed(plan, &fake, 500);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	return plan;
}

UT_TEST(test_cold_header_record_keeps_prefix_and_does_not_install_future_data)
{
	for (int reuse = 0; reuse < 2; reuse++) {
		RfSideOnlinePlanV1 *plan = cold_undo_header_record_plan(reuse, false);
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		PGAlignedBlock header, before_data = { 0 }, after;
		RfSideOnlineOperationV1 operation;
		RfSideUndoHeaderImageV1 expected;
		uint32 n = reuse ? 3 : 2;

		cold_undo_block_record_open(&header, &owner, &authority);
		cluster_undo_segment_make_header_bytes(513, 3, header.data);
		if (reuse)
			((UndoSegmentHeaderData *)header.data)->segment_state = SEGMENT_COMMITTED;
		else
			memset(header.data, 0, BLCKSZ);
		UT_ASSERT(pwrite(fileno(canonical_file), header.data, BLCKSZ, 0) == BLCKSZ);
		before_data.data[128] = 0x77;
		UT_ASSERT(pwrite(fileno(canonical_file), before_data.data, BLCKSZ, 9 * BLCKSZ) == BLCKSZ);
		for (uint32 i = 0; i < n; i++) {
			UT_ASSERT(rf_side_online_plan_operation_v1(plan, i, &operation));
			UT_ASSERT_EQ(rf_side_online_plan_prepare_undo_record_v1(plan, i, &operation.identity,
																	header.data, &expected),
						 RF_PAGE_PROOF_DETAIL_OK);
			canonical_writes = canonical_syncs = cold_source_calls = 0;
			UT_ASSERT_EQ(
				rf_side_online_production_apply_record_v1(plan, i, &operation.identity, &owner),
				RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 0) == BLCKSZ);
			UT_ASSERT(memcmp(after.data, expected.page.data, BLCKSZ) == 0);
			UT_ASSERT_EQ(((UndoSegmentHeaderData *)after.data)->tt_slots[4].status,
						 i == n - 1 ? TT_SLOT_ACTIVE : 0);
			UT_ASSERT_EQ(canonical_writes, 1);
			UT_ASSERT_EQ(cold_source_calls, 1);
			header = after;
			UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 9 * BLCKSZ) == BLCKSZ);
			UT_ASSERT(memcmp(after.data, before_data.data, BLCKSZ) == 0);
			canonical_writes = canonical_syncs = cold_source_calls = 0;
			UT_ASSERT_EQ(
				rf_side_online_production_apply_record_v1(plan, i, &operation.identity, &owner),
				RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_EQ(canonical_writes, 0);
			UT_ASSERT(canonical_syncs > 0);
			UT_ASSERT(!owner.protected_set_complete && !owner.protected_set_active);
		}
		cold_undo_block_record_close(&owner);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_header_record_refuses_ahead_missing_base_and_full_source_conflict)
{
	for (int fault = 0; fault < 10; fault++) {
		RfSideOnlinePlanV1 *plan = cold_undo_header_record_plan(false, fault == 6);
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		PGAlignedBlock header;
		RfSideOnlineOperationV1 operation;
		UndoSegmentHeaderData *h = (UndoSegmentHeaderData *)header.data;
		uint32 selected = fault == 9 ? 0 : 1;

		cold_undo_block_record_open(&header, &owner, &authority);
		if (fault == 0) {
			h->tt_slots[4].status = TT_SLOT_COMMITTED;
			h->tt_slots[4].commit_scn = 999;
			h->tt_slots[4].flags = TT_FLAGS_RESERVED;
		}
		if (fault == 1)
			h->wrap_count = 1;
		if (fault <= 1)
			UT_ASSERT(pwrite(fileno(canonical_file), header.data, BLCKSZ, 0) == BLCKSZ);
		if (fault == 2 || fault == 3)
			UT_ASSERT_EQ(ftruncate(fileno(canonical_file), fault == 2 ? 0 : BLCKSZ), 0);
		canonical_file_io_error = fault == 4;
		cold_source_fail = fault == 5;
		if (fault == 7)
			guard.mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
		record_target_drift = fault == 8 ? 2 : 0;
		canonical_materialize_failure = fault == 9;
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, selected, &operation));
		UT_ASSERT_NE(
			rf_side_online_production_apply_record_v1(plan, selected, &operation.identity, &owner),
			RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(canonical_writes, 0);
		canonical_materialize_failure = false;
		cold_undo_block_record_close(&owner);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_header_record_io_failure_keeps_source_incomplete)
{
	for (int fault = 0; fault < 6; fault++) {
		RfSideOnlinePlanV1 *plan = cold_undo_header_record_plan(false, false);
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		PGAlignedBlock header;
		RfSideOnlineOperationV1 operation;
		volatile bool caught = false;
		volatile RfPageProofDetailV1 detail = RF_PAGE_PROOF_DETAIL_INTERNAL;

		cold_undo_block_record_open(&header, &owner, &authority);
		cluster_undo_segment_make_header_bytes(513, 3, header.data);
		UT_ASSERT(pwrite(fileno(canonical_file), header.data, BLCKSZ, 0) == BLCKSZ);
		ctrc_io_fault = fault == 1 ? 5 : fault < 3 ? fault + 1 : 0;
		cold_source_throw = fault == 3;
		record_target_drift = fault >= 4 ? fault - 1 : 0;
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
		PG_TRY();
		{
			detail
				= rf_side_online_production_apply_record_v1(plan, 1, &operation.identity, &owner);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_NE(detail, RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(caught, fault == 2 || fault == 3);
		UT_ASSERT_EQ(canonical_writes, fault == 3 ? 0 : 1);
		cold_undo_block_record_close(&owner);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_header_full_image_materializes_only_its_prefix)
{
	const off_t sizes[] = { 0, BLCKSZ / 2 };
	for (uint32 i = 0; i < lengthof(sizes); i++) {
		RfSideOnlinePlanV1 *plan = cold_undo_header_record_plan(false, false);
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		PGAlignedBlock header, after;
		RfSideOnlineOperationV1 operation;
		struct stat st;

		cold_undo_block_record_open(&header, &owner, &authority);
		memset(header.data, 0, BLCKSZ);
		UT_ASSERT(pwrite(fileno(canonical_file), header.data, BLCKSZ, 0) == BLCKSZ);
		UT_ASSERT_EQ(ftruncate(fileno(canonical_file), sizes[i]), 0);
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
		UT_ASSERT_EQ(
			rf_side_online_production_apply_record_v1(plan, 0, &operation.identity, &owner),
			RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(fstat(fileno(canonical_file), &st), 0);
		UT_ASSERT_EQ(st.st_size, UNDO_SEGMENT_SIZE_BYTES);
		cluster_undo_segment_make_header_bytes(513, 3, header.data);
		UT_ASSERT(cluster_undo_segment_header_seal(header.data));
		UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 0) == BLCKSZ);
		UT_ASSERT(memcmp(after.data, header.data, BLCKSZ) == 0);
		memset(header.data, 0, BLCKSZ);
		UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 9 * BLCKSZ) == BLCKSZ);
		UT_ASSERT(memcmp(after.data, header.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(canonical_writes, 1);
		cold_undo_block_record_close(&owner);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_header_identical_record_sync_failure_is_not_success)
{
	RfSideOnlinePlanV1 *plan = cold_undo_header_record_plan(false, false);
	ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
	ClusterThreadRecoveryAuthorityV1 authority
		= { .duty = &canonical_duty, .serial_guard = &guard };
	RfSideOnlineProductionOwnerV1 owner;
	PGAlignedBlock header, after;
	RfSideOnlineOperationV1 operation;

	cold_undo_block_record_open(&header, &owner, &authority);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
	record_sync_failure = true;
	UT_ASSERT_NE(rf_side_online_production_apply_record_v1(plan, 1, &operation.identity, &owner),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(canonical_writes, 0);
	UT_ASSERT_EQ(canonical_syncs, 1);
	UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 0) == BLCKSZ);
	UT_ASSERT(memcmp(after.data, header.data, BLCKSZ) == 0);
	cold_undo_block_record_close(&owner);
	rf_side_online_plan_destroy_v1(&plan);
}

static void
head_record_image(PGAlignedBlock *image, int bad)
{
	UndoBlockHeader block = { .magic = PGRAC_UNDO_BLOCK_MAGIC,
							  .block_version = UNDO_BLOCK_VERSION_1,
							  .slot_count = 1,
							  .block_lsn = 200 };
	UndoRecordHeader record = { .record_type = UNDO_RECORD_INSERT,
								.flags = UNDO_REC_FLAG_FIRST_IN_TX,
								.xid = 802,
								.origin_node_id = 2,
								.tt_slot_segment_id = 513,
								.tt_slot_id = 5,
								.tt_wrap_plus1 = 8,
								.write_scn = 10,
								.target_locator = { 1663, 5, 16385 },
								.target_fork = MAIN_FORKNUM,
								.target_block = 3,
								.target_offset = 1,
								.payload_length = sizeof(UndoInsertPayload) };
	UndoSlotDirEntry slot = { .record_offset = sizeof(block),
							  .record_length = sizeof(record) + sizeof(UndoInsertPayload),
							  .record_type = UNDO_RECORD_INSERT,
							  .flags = UNDO_REC_FLAG_FIRST_IN_TX };
	UndoInsertPayload payload = { 32, 0 };

	memset(image, 0, sizeof(*image));
	if (bad == 1)
		record.xid++;
	if (bad == 2)
		record.tt_wrap_plus1++;
	if (bad == 3)
		record.tt_slot_segment_id++;
	if (bad == 4)
		record.tt_slot_id++;
	if (bad == 5)
		record.origin_node_id--;
	if (bad == 6)
		block.magic = 0;
	if (bad == 7)
		slot.record_length = BLCKSZ;
	if (bad == 8)
		record.record_type = 255;
	block.free_offset = sizeof(block) + sizeof(record) + sizeof(payload);
	memcpy(image->data, &block, sizeof(block));
	memcpy(image->data + sizeof(block), &record, sizeof(record));
	memcpy(image->data + sizeof(block) + sizeof(record), &payload, sizeof(payload));
	memcpy(image->data + UNDO_SLOT_DIR_OFFSET(0), &slot, sizeof(slot));
}

static RfSideOnlinePlanV1 *
head_record_plan(const PGAlignedBlock *image, bool omit_prefix, uint16 row)
{
	RfSideOnlinePlanV1 *plan = space_online_plan(400);
	FakeXactRecord fake;
	struct {
		xl_undo_block_write record;
		char page[BLCKSZ];
	} fpi = { 0 };
	UBA head = { .raw = { UINT64_C(513) | (UINT64_C(9) << 32), 4 | ((uint64)row << 16) } };

	fpi.record.instance = 3;
	fpi.record.segment_id = 513;
	fpi.record.block_no = 9;
	fpi.record.has_fpi = 1;
	memcpy(fpi.page, image->data, BLCKSZ);
	if (!omit_prefix) {
		make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE, &fpi, sizeof(fpi));
		undo_header_feed(plan, &fake, 100);
	}
	make_tt_set_head_delta(&fake, 802, &head);
	undo_header_feed(plan, &fake, 200);
	/* This later FPI must never prove or overwrite the selected head DATA. */
	fpi.page[BLCKSZ / 2] = 0x73;
	make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE, &fpi, sizeof(fpi));
	undo_header_feed(plan, &fake, 300);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	return plan;
}

static void
head_record_open(PGAlignedBlock *header, const PGAlignedBlock *data,
				 RfSideOnlineProductionOwnerV1 *owner, ClusterThreadRecoveryAuthorityV1 *authority)
{
	cold_undo_block_record_open(header, owner, authority);
	((UndoSegmentHeaderData *)header->data)->tt_slots[4].status = TT_SLOT_ABORTED;
	UT_ASSERT(pwrite(fileno(canonical_file), header->data, BLCKSZ, 0) == BLCKSZ);
	UT_ASSERT(pwrite(fileno(canonical_file), data->data, BLCKSZ, 9 * BLCKSZ) == BLCKSZ);
}

UT_TEST(test_cold_head_record_requires_installed_exact_data_before_header)
{
	PGAlignedBlock data, header, after;
	ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
	ClusterThreadRecoveryAuthorityV1 authority
		= { .duty = &canonical_duty, .serial_guard = &guard };
	RfSideOnlineProductionOwnerV1 owner;
	RfSideOnlineOperationV1 operation;
	RfSideOnlinePlanV1 *plan;

	head_record_image(&data, 0);
	plan = head_record_plan(&data, false, 0);
	head_record_open(&header, &data, &owner, &authority);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
	for (int repeat = 0; repeat < 2; repeat++) {
		canonical_writes = canonical_syncs = cold_source_calls = 0;
		UT_ASSERT_EQ(
			rf_side_online_production_apply_record_v1(plan, 1, &operation.identity, &owner),
			RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(canonical_writes, repeat == 0 ? 1 : 0);
		UT_ASSERT(canonical_syncs >= 1);
		UT_ASSERT_EQ(cold_source_calls, 1);
		UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 0) == BLCKSZ);
		((UndoSegmentHeaderData *)header.data)->tt_slots[4].first_undo_block
			= operation.undo.first_undo_block;
		UT_ASSERT(cluster_undo_segment_header_seal(header.data));
		UT_ASSERT(memcmp(after.data, header.data, BLCKSZ) == 0);
		UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 9 * BLCKSZ) == BLCKSZ);
		UT_ASSERT(memcmp(after.data, data.data, BLCKSZ) == 0);
	}
	cold_undo_block_record_close(&owner);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_cold_head_record_refuses_wrong_record_or_uninstalled_data)
{
	for (int fault = 0; fault < 17; fault++) {
		PGAlignedBlock data, header, after;
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		RfSideOnlineOperationV1 operation;
		RfSideOnlinePlanV1 *plan;
		uint32 selected = fault == 9 ? 0 : 1;

		head_record_image(&data, fault <= 8 ? fault : 0);
		plan = head_record_plan(&data, fault == 9, fault == 0 ? 1 : 0);
		head_record_open(&header, &data, &owner, &authority);
		if (fault == 10 || fault == 11) {
			if (fault == 10)
				memset(data.data, 0, BLCKSZ);
			else {
				data.data[BLCKSZ / 2] = 0x73;
				((UndoBlockHeader *)data.data)->block_lsn = 400;
			}
			UT_ASSERT(pwrite(fileno(canonical_file), data.data, BLCKSZ, 9 * BLCKSZ) == BLCKSZ);
		}
		if (fault == 12)
			guard.mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
		if (fault == 13)
			((UndoSegmentHeaderData *)header.data)->tt_slots[4].status = TT_SLOT_ACTIVE;
		if (fault == 14)
			((UndoSegmentHeaderData *)header.data)->tt_slots[4].wrap++;
		if (fault == 13 || fault == 14)
			UT_ASSERT(pwrite(fileno(canonical_file), header.data, BLCKSZ, 0) == BLCKSZ);
		if (fault == 15)
			UT_ASSERT_EQ(ftruncate(fileno(canonical_file), 9 * BLCKSZ), 0);
		cold_source_fail = fault == 16;
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, selected, &operation));
		UT_ASSERT_NE(
			rf_side_online_production_apply_record_v1(plan, selected, &operation.identity, &owner),
			RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(canonical_writes, 0);
		UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 0) == BLCKSZ);
		UT_ASSERT(memcmp(after.data, header.data, BLCKSZ) == 0);
		cold_undo_block_record_close(&owner);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_head_record_io_and_postwrite_drift_never_complete)
{
	for (int fault = 0; fault < 7; fault++) {
		PGAlignedBlock data, header;
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		RfSideOnlineOperationV1 operation;
		RfSideOnlinePlanV1 *plan;
		volatile bool caught = false;
		volatile RfPageProofDetailV1 result = RF_PAGE_PROOF_DETAIL_INTERNAL;

		head_record_image(&data, 0);
		plan = head_record_plan(&data, false, 0);
		head_record_open(&header, &data, &owner, &authority);
		record_sync_failure = fault == 0;
		canonical_file_io_error = fault == 1;
		ctrc_io_fault = fault == 2 ? 1 : fault == 3 ? 5 : fault == 4 ? 3 : 0;
		cold_source_throw = fault == 5;
		head_data_drift = fault == 6;
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
		PG_TRY();
		{
			result
				= rf_side_online_production_apply_record_v1(plan, 1, &operation.identity, &owner);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_NE(result, RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(caught, fault == 4 || fault == 5);
		UT_ASSERT_EQ(canonical_writes, fault <= 1 || fault == 5 ? 0 : 1);
		cold_undo_block_record_close(&owner);
		rf_side_online_plan_destroy_v1(&plan);
	}
}


UT_TEST(test_cold_head_prefix_cannot_borrow_fpi_across_segment_rebirth)
{
	for (int reuse = 0; reuse < 2; reuse++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(500);
		FakeXactRecord fake;
		RfSideOnlineOperationV1 op;
		RfSideUndoBlockImageV1 out, sentinel;
		struct {
			xl_cluster_undo_segment_init record;
			char page[BLCKSZ];
		} init = { 0 };
		struct {
			xl_undo_segment_reuse record;
			char page[BLCKSZ];
		} rebirth = { 0 };
		UBA head = { .raw = { UINT64_C(513) | (UINT64_C(9) << 32), 4 } };

		record_prefix_fpi(plan, 100, 0x31);
		cluster_undo_segment_make_header_bytes(513, 3, init.page);
		init.record.instance = rebirth.record.instance = 3;
		init.record.segment_id = rebirth.record.segment_id = 513;
		memcpy(rebirth.page, init.page, BLCKSZ);
		rebirth.record.new_generation = 1;
		((UndoSegmentHeaderData *)rebirth.page)->wrap_count = 1;
		if (reuse)
			make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_REUSE, &rebirth,
								   sizeof(rebirth));
		else
			make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_INIT, &init,
								   sizeof(init));
		undo_header_feed(plan, &fake, 200);
		make_tt_set_head_delta(&fake, 802, &head);
		undo_header_feed(plan, &fake, 300);
		record_prefix_fpi(plan, 400, 0x73);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 2, &op));
		memset(&out, 0x5a, sizeof(out));
		sentinel = out;
		UT_ASSERT_NE(rf_side_online_plan_prepare_undo_head_block_v1(plan, 2, &op.identity, &out),
					 RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(memcmp(&out, &sentinel, sizeof(out)) == 0);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_head_prefix_requires_complete_selected_identity)
{
	PGAlignedBlock data;
	RfSideOnlinePlanV1 *plan;
	RfSideOnlineOperationV1 op;
	RfSideUndoBlockImageV1 out;

	head_record_image(&data, 0);
	plan = head_record_plan(&data, false, 0);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &op));
	UT_ASSERT_EQ(rf_side_online_plan_prepare_undo_head_block_v1(plan, 1, &op.identity, &out),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(memcmp(out.page.data, data.data, BLCKSZ) == 0);
	for (int fault = 0; fault < 5; fault++) {
		RfPageOnlineRecordIdentityV1 identity = op.identity;
		RfSideUndoBlockImageV1 sentinel;
		uint32 selected = fault == 4 ? 0 : 1;
		if (fault == 0)
			identity.participant_index++;
		if (fault == 1)
			identity.record.end_rec_ptr++;
		if (fault == 2)
			identity.record.origin_thread++;
		if (fault == 3)
			identity.reserved_zero = 1;
		memset(&out, 0x5a, sizeof(out));
		sentinel = out;
		UT_ASSERT_NE(
			rf_side_online_plan_prepare_undo_head_block_v1(plan, selected, &identity, &out),
			RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(memcmp(&out, &sentinel, sizeof(out)) == 0);
	}
	rf_side_online_plan_destroy_v1(&plan);
}


UT_TEST(test_cold_head_record_reserves_nested_data_reader_scratch)
{
	PGAlignedBlock data, header;
	ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
	ClusterThreadRecoveryAuthorityV1 authority
		= { .duty = &canonical_duty, .serial_guard = &guard };
	RfSideOnlineProductionOwnerV1 owner;
	RfSideOnlineOperationV1 operation;
	RfSideOnlinePlanV1 *plan;
	Size available;

	head_record_image(&data, 0);
	plan = head_record_plan(&data, false, 0);
	head_record_open(&header, &data, &owner, &authority);
	available = rf_side_online_plan_scratch_available_v1(plan);
	UT_ASSERT(available > 14 * BLCKSZ);
	owner.borrowed_scratch_bytes = available - 14 * BLCKSZ;
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
	UT_ASSERT_NE(rf_side_online_production_apply_record_v1(plan, 1, &operation.identity, &owner),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(canonical_writes, 0);
	cold_undo_block_record_close(&owner);
	rf_side_online_plan_destroy_v1(&plan);
}


static RfSideOnlinePlanV1 *
abort_record_plan(uint32 generation, bool legacy, bool conflict)
{
	RfSideOnlinePlanV1 *plan = space_online_plan(400);
	FakeXactRecord fake;
	xl_undo_tt_slot_abort_exact abort = { 0 };
	UBA head = { .raw = { UINT64_C(513) | (UINT64_C(9) << 32), 4 } };

	abort.instance = 3;
	abort.segment_id = 513;
	abort.segment_generation = generation;
	abort.slot_offset = 4;
	abort.xid = 802;
	abort.wrap = 7;
	abort.format_version = CLUSTER_UNDO_TT_ABORT_EXACT_VERSION;
	if (legacy)
		make_tt_abort_delta(&fake, 802);
	else
		make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_ABORT, &abort,
							   sizeof(abort));
	undo_header_feed(plan, &fake, 100);
	/* The selected terminal header must not execute a later DATA or head. */
	record_prefix_fpi(plan, 200, 0x31);
	make_tt_set_head_delta(&fake, conflict ? 818 : 802, &head);
	undo_header_feed(plan, &fake, 300);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	return plan;
}

UT_TEST(test_cold_exact_abort_record_installs_only_its_terminal_header)
{
	for (uint32 generation = 0; generation <= 9; generation += 9) {
		RfSideOnlinePlanV1 *plan = abort_record_plan(generation, false, false);
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		RfSideOnlineOperationV1 operation;
		PGAlignedBlock header, after, data = { 0 };
		UndoSegmentHeaderData *h = (UndoSegmentHeaderData *)header.data;

		cold_undo_block_record_open(&header, &owner, &authority);
		h->wrap_count = generation;
		UT_ASSERT(pwrite(fileno(canonical_file), header.data, BLCKSZ, 0) == BLCKSZ);
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
		for (int repeat = 0; repeat < 3; repeat++) {
			/* The original exact terminal transition rearms old release
			 * flags; this is not an idempotent no-work or GC proof. */
			if (repeat == 2) {
				h->tt_slots[4].flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
				UT_ASSERT(cluster_undo_segment_header_seal(header.data));
				UT_ASSERT(pwrite(fileno(canonical_file), header.data, BLCKSZ, 0) == BLCKSZ);
			}
			canonical_writes = canonical_syncs = cold_source_calls = 0;
			UT_ASSERT_EQ(
				rf_side_online_production_apply_record_v1(plan, 0, &operation.identity, &owner),
				RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_EQ(canonical_writes, repeat != 1);
			UT_ASSERT(canonical_syncs > 0 && cold_source_calls == 1);
			UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 0) == BLCKSZ);
			h->tt_slots[4].status = TT_SLOT_ABORTED;
			h->tt_slots[4].flags = TT_FLAGS_RESERVED;
			UT_ASSERT(cluster_undo_segment_header_seal(header.data));
			UT_ASSERT(memcmp(header.data, after.data, BLCKSZ) == 0);
			UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 9 * BLCKSZ) == BLCKSZ);
			UT_ASSERT(memcmp(data.data, after.data, BLCKSZ) == 0);
			UT_ASSERT_EQ(cold_clog_applies + cold_clog_verifies
							 + prepare_apply.terminal_projection_stores,
						 0);
		}
		cold_undo_block_record_close(&owner);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_exact_abort_record_keeps_complete_identity_and_source_refusals)
{
	for (int fault = 0; fault < 15; fault++) {
		RfSideOnlinePlanV1 *plan = abort_record_plan(0, fault == 0, fault == 10);
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		RfSideOnlineOperationV1 operation;
		PGAlignedBlock header;
		UndoSegmentHeaderData *h = (UndoSegmentHeaderData *)header.data;

		cold_undo_block_record_open(&header, &owner, &authority);
		if (fault == 1)
			h->wrap_count++;
		if (fault == 2)
			h->tt_slots[4].xid++;
		if (fault == 3)
			h->tt_slots[4].wrap++;
		if (fault == 4) {
			h->tt_slots[4].status = TT_SLOT_COMMITTED;
			h->tt_slots[4].commit_scn = 999;
		}
		if (fault == 5) {
			h->tt_slots[4].status = TT_SLOT_ABORTED;
			h->tt_slots[4].flags = ~TT_SLOT_FLAGS_KNOWN;
		}
		if (fault == 14) {
			h->tt_slots[4].status = TT_SLOT_ABORTED;
			h->tt_slots[4].first_undo_block
				= (UBA){ .raw = { UINT64_C(513) | (UINT64_C(9) << 32), 4 } };
		}
		UT_ASSERT(pwrite(fileno(canonical_file), header.data, BLCKSZ, 0) == BLCKSZ);
		if (fault == 6 || fault == 7)
			UT_ASSERT_EQ(ftruncate(fileno(canonical_file), fault == 6 ? 0 : BLCKSZ), 0);
		cold_source_fail = fault == 8;
		if (fault == 9)
			guard.mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
		if (fault == 11)
			record_target_drift = 2;
		canonical_file_io_error = fault == 12;
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
		if (fault == 13)
			operation.identity.record.record_crc++;
		UT_ASSERT_NE(
			rf_side_online_production_apply_record_v1(plan, 0, &operation.identity, &owner),
			RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(canonical_writes, 0);
		cold_undo_block_record_close(&owner);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_exact_abort_record_io_error_and_postwrite_drift_keep_incomplete)
{
	for (int fault = 0; fault < 6; fault++) {
		RfSideOnlinePlanV1 *plan = abort_record_plan(0, false, false);
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		RfSideOnlineOperationV1 operation;
		PGAlignedBlock header;
		volatile bool caught = false;
		volatile RfPageProofDetailV1 detail = RF_PAGE_PROOF_DETAIL_INTERNAL;

		cold_undo_block_record_open(&header, &owner, &authority);
		ctrc_io_fault = fault == 1 ? 5 : fault < 3 ? fault + 1 : 0;
		cold_source_throw = fault == 3;
		record_target_drift = fault >= 4 ? fault - 1 : 0;
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
		PG_TRY();
		{
			detail
				= rf_side_online_production_apply_record_v1(plan, 0, &operation.identity, &owner);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_NE(detail, RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(caught, fault == 2 || fault == 3);
		UT_ASSERT_EQ(canonical_writes, fault == 3 ? 0 : 1);
		cold_undo_block_record_close(&owner);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_exact_abort_record_idempotence_still_requires_sync)
{
	RfSideOnlinePlanV1 *plan = abort_record_plan(0, false, false);
	ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
	ClusterThreadRecoveryAuthorityV1 authority
		= { .duty = &canonical_duty, .serial_guard = &guard };
	RfSideOnlineProductionOwnerV1 owner;
	RfSideOnlineOperationV1 operation;
	PGAlignedBlock header;
	UndoSegmentHeaderData *h = (UndoSegmentHeaderData *)header.data;

	cold_undo_block_record_open(&header, &owner, &authority);
	h->tt_slots[4].status = TT_SLOT_ABORTED;
	h->tt_slots[4].flags = TT_FLAGS_RESERVED;
	UT_ASSERT(pwrite(fileno(canonical_file), header.data, BLCKSZ, 0) == BLCKSZ);
	record_sync_failure = true;
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
	UT_ASSERT_NE(rf_side_online_production_apply_record_v1(plan, 0, &operation.identity, &owner),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(canonical_syncs, 1);
	UT_ASSERT_EQ(canonical_writes, 0);
	cold_undo_block_record_close(&owner);
	rf_side_online_plan_destroy_v1(&plan);
}


static RfSideOnlinePlanV1 *
native_abort_record_plan_children(bool completion, bool missing, bool conflict, bool include_future,
								  bool children)
{
	RfSideOnlinePlanV1 *plan = space_online_plan(include_future ? 500 : 300);
	FakeXactRecord fake;
	xl_undo_tt_slot_bind later = { .segment_id = 513, .instance = 3,
		.slot_offset = 5, .xid = 818, .wrap = 8,
		.format_version = CLUSTER_UNDO_TT_BIND_VERSION };

	if (missing)
		undo_header_bind(&fake, 0);
	else
		undo_header_abort(&fake, 4);
	undo_header_feed(plan, &fake, 100);
	if (completion)
		cold_completion_abort(&fake);
	else
		make_abort(&fake, 802);
	if (children) {
		fake.u.decoded.max_block_id = -1;
		cold_terminal_children(&fake, true);
	}
	undo_header_feed(plan, &fake, 200);
	if (include_future) {
		record_prefix_fpi(plan, 300, 0x31);
		if (conflict)
			later.slot_offset = 4;
		make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_BIND,
						   &later, sizeof(later));
		undo_header_feed(plan, &fake, 400);
	}
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	return plan;
}

static RfSideOnlinePlanV1 *
native_abort_record_plan(bool completion, bool missing, bool conflict, bool include_future)
{
	return native_abort_record_plan_children(completion, missing, conflict, include_future, false);
}

static void
native_abort_record_open(PGAlignedBlock *header, RfSideOnlineProductionOwnerV1 *owner,
						 ClusterThreadRecoveryAuthorityV1 *authority, bool completion)
{
	cold_undo_block_record_open(header, owner, authority);
	((UndoSegmentHeaderData *)header->data)->tt_slots[4].status = TT_SLOT_ABORTED;
	UT_ASSERT(pwrite(fileno(canonical_file), header->data, BLCKSZ, 0) == BLCKSZ);
	canonical_expected_xid = 802;
	canonical_expected_wrap = 7;
	cold_abort_test = true;
	cold_completion_test = completion;
	cold_source_ready = false;
	cold_source_calls = cold_source_order = cold_stats_calls = cold_stats_order = cold_stats_gc = 0;
	cold_cache_calls = cold_clog_verified_order = 0;
}

static void
native_abort_record_close(RfSideOnlineProductionOwnerV1 *owner)
{
	cold_clog_applies = cold_clog_verifies = prepare_apply.terminal_projection_stores = 0;
	cold_completion_test = cold_abort_test = cold_clog_denied = cold_clog_apply_fail
		= cold_clog_verify_fail = cold_clog_throw = cold_clog_stale_after_verify = false;
	cold_stats_throw = cold_stats_expire = false;
	native_abort_sync_drift = 0;
	canonical_expected_xid = 818;
	canonical_expected_wrap = 8;
	owner->protected_set_complete = false;
	cold_undo_block_record_close(owner);
}

/* The native SPACE boundary is a fixture here. Its original batch, physical
 * qualification and I/O are exercised independently by SPACE owner tests. */
static const RfSideOnlinePlanV1 *abort_terminal_plan;
static int abort_terminal_fault;
static unsigned abort_terminal_entries, abort_terminal_deletes, abort_terminal_order;
static bool abort_terminal_consumed;

bool
cluster_space_recovery_cold_abort_finish_v1(const RfSideOnlinePlanV1 *plan, uint32 index,
											Size bytes, void *arg,
											ClusterSpaceColdAbortFinishV1 before_delete,
											ClusterSpaceColdAbortFinishV1 finish)
{
	UT_ASSERT(plan == abort_terminal_plan && index == 5 && canonical_scope != NULL);
	UT_ASSERT(before_delete != NULL && finish != NULL);
	UT_ASSERT(bytes >= 8 * BLCKSZ && bytes < rf_side_online_plan_scratch_available_v1(plan));
	UT_ASSERT_EQ(cold_stats_calls + prepare_apply.terminal_projection_stores, 0);
	abort_terminal_entries++;
	if (abort_terminal_consumed || abort_terminal_fault == 1)
		return false;
	UT_ASSERT_EQ(cold_clog_applies + cold_clog_verifies + cold_locks_calls, 0);
	if (!before_delete(arg, plan, index))
		return false;
	UT_ASSERT_EQ(cold_clog_verifies, 1);
	UT_ASSERT_EQ(cold_locks_calls, cold_locks_test ? 1 : 0);
	UT_ASSERT_EQ(cold_stats_calls + prepare_apply.terminal_projection_stores, 0);
	abort_terminal_deletes++;
	abort_terminal_order = ++prepare_apply.next_order;
	abort_terminal_consumed = true;
	if (abort_terminal_fault == 4)
		pg_re_throw();
	if (abort_terminal_fault == 5)
		cold_clog_denied = true;
	if (abort_terminal_fault == 3)
		return false;
	return finish(arg, plan, index) && abort_terminal_fault != 19;
}

static RfSideOnlinePlanV1 *
abort_terminal_build(bool combined)
{
	RfSideOnlinePlanV1 *plan = space_online_plan(900);
	RelFileLocator locators[] = { { DEFAULTTABLESPACE_OID, 5, 16384 },
								  { DEFAULTTABLESPACE_OID, 5, 16384 },
								  { DEFAULTTABLESPACE_OID, 5, 16385 } };
	FakeXactRecord fake;
	xl_undo_tt_slot_bind later = { .segment_id = 513,
								   .instance = 3,
								   .slot_offset = 5,
								   .xid = 818,
								   .wrap = 8,
								   .format_version = CLUSTER_UNDO_TT_BIND_VERSION };

	undo_header_abort(&fake, 4);
	undo_header_feed(plan, &fake, 100);
	for (uint32 i = 0; i < 4; i++) {
		cold_abort_create_record(&fake, locators[i < 2 ? 0 : 2], i % 2 == 0, 802);
		UT_ASSERT_EQ(cold_abort_delete_feed(plan, &fake, (i + 2) * 100, 0),
					 RF_PAGE_PROOF_DETAIL_OK);
	}
	cold_abort_deletes(&fake, locators, lengthof(locators), combined);
	if (combined) {
		uint32 xinfo;
		cold_terminal_children(&fake, true);
		memcpy(&xinfo, fake.data + MinSizeOfXactAbort, sizeof(xinfo));
		xinfo |= XACT_XINFO_HAS_AE_LOCKS;
		memcpy(fake.data + MinSizeOfXactAbort, &xinfo, sizeof(xinfo));
	}
	UT_ASSERT_EQ(cold_abort_delete_feed(plan, &fake, 600, 0), RF_PAGE_PROOF_DETAIL_OK);
	record_prefix_fpi(plan, 700, 0x31);
	make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_BIND, &later,
						   sizeof(later));
	undo_header_feed(plan, &fake, 800);
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	return plan;
}

static bool
abort_terminal_space_preflight(void *arg, const RfSideOnlineOperationV1 *op)
{
	UT_ASSERT(arg == abort_terminal_plan && canonical_scope != NULL);
	UT_ASSERT(op->kind == RF_SIDE_ONLINE_OPERATION_SPACE
			  || op->kind == RF_SIDE_ONLINE_OPERATION_NATIVE_STRUCTURE);
	return true; /* No physical permission: the finish boundary proves that. */
}

static bool
abort_terminal_space_apply(void *arg pg_attribute_unused(),
						   const RfSideOnlineOperationV1 *op pg_attribute_unused())
{
	UT_ASSERT(false); /* A selected ABORT must never replay earlier CREATEs. */
	return false;
}

static void
abort_terminal_setup(RfSideOnlineProductionOwnerV1 *owner, RfSideOnlinePlanV1 *plan, bool combined)
{
	abort_terminal_plan = plan;
	owner->preflight_space = abort_terminal_space_preflight;
	owner->apply_space = abort_terminal_space_apply;
	owner->space_arg = plan;
	abort_terminal_fault = 0;
	abort_terminal_entries = abort_terminal_deletes = abort_terminal_order = 0;
	abort_terminal_consumed = false;
	cold_expected_children = combined ? lengthof(cold_child_xids) : 0;
	cold_locks_test = combined;
	cold_locks_calls = cold_locks_order = 0;
	standbyState = combined ? STANDBY_INITIALIZED : STANDBY_DISABLED;
	payload_tracking = true;
	payload_bytes = payload_peak = 0;
}

static void
abort_terminal_cleanup(RfSideOnlineProductionOwnerV1 *owner, RfSideOnlinePlanV1 **plan)
{
	UT_ASSERT_EQ(payload_bytes, 0);
	UT_ASSERT(!owner->protected_set_complete && !owner->protected_set_active
			  && owner->undo_headers == NULL && owner->protected_plan == NULL);
	payload_tracking = false;
	cold_locks_test = cold_locks_throw = cold_locks_expire = false;
	standbyState = STANDBY_DISABLED;
	cold_expected_children = 0;
	native_abort_record_close(owner);
	rf_side_online_plan_destroy_v1(plan);
	abort_terminal_plan = NULL;
}

UT_TEST(test_cold_selected_abort_delete_preserves_native_order_and_exact_prefix)
{
	for (int combined = 0; combined < 2; combined++) {
		RfSideOnlinePlanV1 *plan = abort_terminal_build(combined);
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		RfSideOnlineOperationV1 operation;
		PGAlignedBlock header, after, zero = { 0 };

		native_abort_record_open(&header, &owner, &authority, true);
		abort_terminal_setup(&owner, plan, combined);
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 5, &operation));
		/* Earlier CREATEs require their ordered owner; full-source apply cannot
		 * substitute for this selected terminal consumer. */
		UT_ASSERT_NE(rf_side_online_production_apply_v1(plan, &owner), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(abort_terminal_entries + cold_clog_applies + cold_stats_calls
						 + prepare_apply.terminal_projection_stores + canonical_writes,
					 0);
		UT_ASSERT_EQ(
			rf_side_online_production_apply_record_v1(plan, 5, &operation.identity, &owner),
			RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(abort_terminal_entries, 1);
		UT_ASSERT_EQ(abort_terminal_deletes, 1);
		UT_ASSERT_EQ(canonical_writes, 0);
		UT_ASSERT_EQ(cold_clog_applies, 1);
		UT_ASSERT_EQ(cold_clog_verifies, 1);
		UT_ASSERT(cold_clog_verified_order < abort_terminal_order);
		if (combined) {
			UT_ASSERT(cold_clog_verified_order < cold_locks_order
					  && cold_locks_order < abort_terminal_order
					  && abort_terminal_order < cold_stats_order);
			UT_ASSERT_EQ(cold_stats_calls, 3);
		}
		UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 1);
		UT_ASSERT(abort_terminal_order < prepare_apply.terminal_projection_order);
		UT_ASSERT(payload_peak <= operation.owned_payload_length);
		UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 0) == BLCKSZ);
		UT_ASSERT(memcmp(header.data, after.data, BLCKSZ) == 0);
		UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 9 * BLCKSZ) == BLCKSZ);
		UT_ASSERT(memcmp(zero.data, after.data, BLCKSZ) == 0);
		/* A consumed SPACE proof cannot be replaced by already-terminal TT. */
		cold_stats_calls = prepare_apply.terminal_projection_stores = 0;
		UT_ASSERT_NE(
			rf_side_online_production_apply_record_v1(plan, 5, &operation.identity, &owner),
			RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(abort_terminal_deletes, 1);
		UT_ASSERT_EQ(cold_stats_calls + prepare_apply.terminal_projection_stores, 0);
		abort_terminal_cleanup(&owner, &plan);
	}
}

UT_TEST(test_cold_selected_abort_delete_failure_or_error_cannot_complete)
{
	for (int fault = 1; fault < 20; fault++) {
		RfSideOnlinePlanV1 *plan = abort_terminal_build(true);
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		RfSideOnlineOperationV1 operation;
		PGAlignedBlock header;
		volatile bool caught = false;
		volatile RfPageProofDetailV1 detail = RF_PAGE_PROOF_DETAIL_INTERNAL;

		native_abort_record_open(&header, &owner, &authority, true);
		abort_terminal_setup(&owner, plan, true);
		abort_terminal_fault = fault;
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 5, &operation));
		cold_clog_apply_fail = fault == 6;
		cold_clog_verify_fail = fault == 7;
		cold_clog_throw = fault == 8;
		cold_locks_throw = fault == 9;
		cold_locks_expire = fault == 10;
		cold_stats_throw = fault == 11;
		cold_stats_expire = fault == 12;
		if (fault == 13)
			((UndoSegmentHeaderData *)header.data)->tt_slots[4].status = TT_SLOT_ACTIVE;
		if (fault == 14)
			((UndoSegmentHeaderData *)header.data)->tt_slots[4].xid++;
		UT_ASSERT(pwrite(fileno(canonical_file), header.data, BLCKSZ, 0) == BLCKSZ);
		record_sync_failure = fault == 2 || fault == 15;
		cold_source_fail = fault == 16;
		cold_source_throw = fault == 17;
		if (fault == 18)
			owner.borrowed_scratch_bytes = rf_side_online_plan_scratch_available_v1(plan);
		PG_TRY();
		{
			detail
				= rf_side_online_production_apply_record_v1(plan, 5, &operation.identity, &owner);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_NE(detail, RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(caught, fault == 4 || fault == 8 || fault == 9 || fault == 11 || fault == 17);
		UT_ASSERT_EQ(canonical_writes, 0);
		UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, fault == 19 ? 1 : 0);
		if (fault == 1 || fault == 2 || (fault >= 6 && fault <= 10) || (fault >= 13 && fault <= 18))
			UT_ASSERT_EQ(abort_terminal_deletes + cold_stats_calls, 0);
		if (fault == 1 || (fault >= 13 && fault <= 18))
			UT_ASSERT_EQ(cold_clog_applies, 0);
		abort_terminal_cleanup(&owner, &plan);
	}
}

UT_TEST(test_cold_native_abort_record_requires_installed_tt_before_exact_effects)
{
	for (int completion = 0; completion < 2; completion++) {
		RfSideOnlinePlanV1 *plan
			= native_abort_record_plan(completion != 0, false, false, false);
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority = { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		PGAlignedBlock header, after, zero = { 0 };

		native_abort_record_open(&header, &owner, &authority, completion != 0);
		for (int repeat = 0; repeat < 2; repeat++) {
			reset_prepare_apply();
			canonical_writes = canonical_syncs = cold_source_calls = cold_stats_calls = 0;
			cold_clog_applies = cold_clog_verifies = 0;
			cold_source_ready = false;
			UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner),
						 RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_EQ(canonical_writes, 0);
			UT_ASSERT(canonical_syncs > 0 && cold_source_calls == 1);
			UT_ASSERT_EQ(cold_clog_applies, 1);
			UT_ASSERT_EQ(cold_clog_verifies, 1);
			UT_ASSERT_EQ(cold_stats_calls, completion ? 3 : 0);
			UT_ASSERT_EQ(cold_cache_calls, 0);
			UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 1);
			UT_ASSERT(cold_source_order < cold_clog_verified_order);
			UT_ASSERT(cold_clog_verified_order < prepare_apply.terminal_projection_order);
			if (completion)
				UT_ASSERT(cold_clog_verified_order < cold_stats_order
						  && cold_stats_order < prepare_apply.terminal_projection_order);
			UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 0) == BLCKSZ);
			UT_ASSERT(memcmp(header.data, after.data, BLCKSZ) == 0);
			UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 9 * BLCKSZ) == BLCKSZ);
			UT_ASSERT(memcmp(zero.data, after.data, BLCKSZ) == 0);
			UT_ASSERT(owner.protected_set_complete && !owner.protected_set_active
					  && owner.undo_headers == NULL && owner.protected_plan == NULL);
		}
		native_abort_record_close(&owner);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_native_abort_record_refuses_unproved_prefix_and_native_failures)
{
	static const int faults[] = {
		1, 2, 3, 4, 6, 7, 8, 9, 10, 11,
		12, 13, 14, 15, 16, 17, 18, 19, 22, 23
	};

	for (uint32 fault_index = 0; fault_index < lengthof(faults); fault_index++) {
		int fault = faults[fault_index];
		RfSideOnlinePlanV1 *plan
			= native_abort_record_plan(true, fault == 23, false, false);
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority = { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		RfSideOnlineOperationV1 operation;
		PGAlignedBlock header;
		UndoSegmentHeaderData *h = (UndoSegmentHeaderData *)header.data;
		volatile bool caught = false;
		volatile RfPageProofDetailV1 detail = RF_PAGE_PROOF_DETAIL_INTERNAL;

		native_abort_record_open(&header, &owner, &authority, true);
		if (fault == 0) h->tt_slots[4].status = TT_SLOT_ACTIVE;
		if (fault == 1) h->tt_slots[4].wrap++;
		if (fault == 2) h->tt_slots[4].xid++;
		if (fault == 3) h->wrap_count++;
		if (fault == 4) { h->tt_slots[4].status = TT_SLOT_COMMITTED; h->tt_slots[4].commit_scn = 999; }
		if (fault == 5) h->tt_slots[4].flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
		if (fault == 19) h->tt_slots[4].first_undo_block = (UBA){ .raw = { UINT64_C(513) | (UINT64_C(9) << 32), 4 } };
		UT_ASSERT(pwrite(fileno(canonical_file), header.data, BLCKSZ, 0) == BLCKSZ);
		if (fault == 6 || fault == 7) UT_ASSERT_EQ(ftruncate(fileno(canonical_file), fault == 6 ? BLCKSZ : 0), 0);
		if (fault == 8) guard.mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
		cold_source_fail = fault == 9;
		canonical_file_io_error = fault == 10;
		cold_clog_apply_fail = fault == 11;
		cold_clog_verify_fail = fault == 12;
		cold_clog_stale_after_verify = fault == 13;
		cold_clog_throw = fault == 14;
		cold_stats_throw = fault == 15;
		cold_stats_expire = fault == 16;
		cold_source_throw = fault == 17;
		record_target_drift = fault == 20 ? 1 : fault == 21 ? 2 : 0;
		record_sync_failure = fault == 22;
		if (fault == 18) {
			UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
			operation.identity.record.record_crc++;
		}
		PG_TRY();
		{
			if (fault == 18)
				detail = rf_side_online_production_apply_record_v1(
					plan, 1, &operation.identity, &owner);
			else
				detail = rf_side_online_production_apply_v1(plan, &owner);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_NE(detail, RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(caught, fault == 14 || fault == 15 || fault == 17);
		UT_ASSERT_EQ(canonical_writes, 0);
		UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
		UT_ASSERT(!owner.protected_set_complete && !owner.protected_set_active
				  && owner.undo_headers == NULL && owner.protected_plan == NULL);
		native_abort_record_close(&owner);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

static RfSideOnlinePlanV1 *
native_commit_record_plan(bool completion, bool conflict)
{
	RfSideOnlinePlanV1 *plan = space_online_plan(600);
	FakeXactRecord fake;
	xl_undo_tt_slot_bind bind;
	xl_xact_tt_commit delta;
	uint32 offset;

	undo_header_bind(&fake, 0);
	undo_header_feed(plan, &fake, 100);
	if (completion)
		cold_completion_commit(&fake, true, true, true);
	else
		undo_header_commit(&fake, 0, 802);
	undo_header_feed(plan, &fake, 200);
	/* Neither later DATA nor reuse/COMMIT of the same TT slot belongs to
	 * the selected record. Full-source preflight must still check them. */
	record_prefix_fpi(plan, 300, 0x31);
	undo_header_bind(&fake, 0);
	memcpy(&bind, fake.data, sizeof(bind));
	bind.xid = 818;
	bind.wrap = 8;
	memcpy(fake.data, &bind, sizeof(bind));
	undo_header_feed(plan, &fake, 400);
	undo_header_commit(&fake, 0, conflict ? 834 : 818);
	offset = fake.u.decoded.main_data_len - sizeof(delta);
	memcpy(&delta, fake.data + offset, sizeof(delta));
	delta.wrap = 8;
	memcpy(fake.data + offset, &delta, sizeof(delta));
	undo_header_feed(plan, &fake, 500);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	return plan;
}

static void
native_commit_record_open(PGAlignedBlock *header, RfSideOnlineProductionOwnerV1 *owner,
						  ClusterThreadRecoveryAuthorityV1 *authority, bool completion)
{
	cold_undo_block_record_open(header, owner, authority);
	canonical_expected_xid = 802;
	canonical_expected_wrap = 7;
	cold_completion_test = completion;
	cold_cache_test = completion;
	cold_source_ready = false;
	cold_source_calls = cold_source_order = cold_stats_calls = cold_stats_order = cold_stats_gc = 0;
	cold_cache_calls = cold_clog_verified_order = 0;
}

static void
native_commit_record_close(RfSideOnlineProductionOwnerV1 *owner)
{
	cold_cache_test = cold_cache_throw = cold_cache_expire = false;
	native_abort_record_close(owner);
}

UT_TEST(test_cold_native_commit_record_installs_only_selected_terminal)
{
	for (int completion = 0; completion < 2; completion++) {
		RfSideOnlinePlanV1 *plan = native_commit_record_plan(completion != 0, false);
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		RfSideOnlineOperationV1 operation;
		PGAlignedBlock header, after, zero = { 0 };

		native_commit_record_open(&header, &owner, &authority, completion != 0);
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
		for (int repeat = 0; repeat < 2; repeat++) {
			reset_prepare_apply();
			canonical_writes = canonical_syncs = cold_source_calls = cold_stats_calls = 0;
			cold_clog_applies = cold_clog_verifies = cold_cache_calls = 0;
			cold_source_ready = false;
			UT_ASSERT_EQ(
				rf_side_online_production_apply_record_v1(plan, 1, &operation.identity, &owner),
				RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_EQ(canonical_writes, repeat == 0 ? 1 : 0);
			UT_ASSERT(canonical_syncs > 0 && cold_source_calls == 1);
			UT_ASSERT_EQ(cold_clog_applies, 1);
			UT_ASSERT_EQ(cold_clog_verifies, 1);
			UT_ASSERT_EQ(cold_stats_calls, completion ? 3 : 0);
			UT_ASSERT_EQ(cold_cache_calls, completion ? 1 : 0);
			UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 1);
			UT_ASSERT(cold_source_order < cold_clog_verified_order
					  && cold_clog_verified_order < prepare_apply.terminal_projection_order);
			if (completion)
				UT_ASSERT(cold_clog_verified_order < cold_stats_order
						  && cold_stats_order < prepare_apply.terminal_projection_order);
			((UndoSegmentHeaderData *)header.data)->tt_slots[4].status = TT_SLOT_COMMITTED;
			((UndoSegmentHeaderData *)header.data)->tt_slots[4].commit_scn = 999;
		UT_ASSERT(cluster_undo_segment_header_seal(header.data));
			UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 0) == BLCKSZ);
			UT_ASSERT(memcmp(header.data, after.data, BLCKSZ) == 0);
			UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 9 * BLCKSZ) == BLCKSZ);
			UT_ASSERT(memcmp(zero.data, after.data, BLCKSZ) == 0);
			UT_ASSERT(!owner.protected_set_complete && !owner.protected_set_active
					  && owner.undo_headers == NULL && owner.protected_plan == NULL);
		}
		native_commit_record_close(&owner);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_native_commit_record_requires_prior_tt_steps)
{
	RfSideOnlinePlanV1 *plan = space_online_plan(400);
	ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
	ClusterThreadRecoveryAuthorityV1 authority
		= { .duty = &canonical_duty, .serial_guard = &guard };
	RfSideOnlineProductionOwnerV1 owner;
	RfSideOnlineOperationV1 operation;
	PGAlignedBlock header;
	FakeXactRecord fake;
	xl_undo_tt_slot_bind other;

	undo_header_bind(&fake, 0);
	undo_header_feed(plan, &fake, 100);
	undo_header_bind(&fake, 0);
	memcpy(&other, fake.data, sizeof(other));
	other.slot_offset = 5;
	other.xid = 818;
	memcpy(fake.data, &other, sizeof(other));
	undo_header_feed(plan, &fake, 200);
	undo_header_commit(&fake, 0, 802);
	undo_header_feed(plan, &fake, 300);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	native_commit_record_open(&header, &owner, &authority, false);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 2, &operation));
	UT_ASSERT_NE(rf_side_online_production_apply_record_v1(plan, 2, &operation.identity, &owner),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(canonical_writes + cold_clog_applies + prepare_apply.terminal_projection_stores,
				 0);
	/* The original ordered BIND, not COMMIT's source-prefix reconstruction,
	 * installs the other slot. COMMIT can then consume its exact predecessor. */
	cold_source_calls = 0;
	cold_source_ready = false;
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
	UT_ASSERT_EQ(rf_side_online_production_apply_record_v1(plan, 1, &operation.identity, &owner),
				 RF_PAGE_PROOF_DETAIL_OK);
	canonical_writes = cold_source_calls = 0;
	cold_source_ready = false;
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 2, &operation));
	UT_ASSERT_EQ(rf_side_online_production_apply_record_v1(plan, 2, &operation.identity, &owner),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(canonical_writes, 1);
	UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 1);
	native_commit_record_close(&owner);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_cold_native_commit_record_refuses_unproved_inputs_before_write)
{
	for (int fault = 0; fault < 16; fault++) {
		RfSideOnlinePlanV1 *plan = native_commit_record_plan(false, fault == 14);
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		RfSideOnlineOperationV1 operation;
		PGAlignedBlock header;
		UndoSegmentHeaderData *h = (UndoSegmentHeaderData *)header.data;

		native_commit_record_open(&header, &owner, &authority, false);
		if (fault == 0)
			memset(&h->tt_slots[4], 0, sizeof(TTSlot));
		if (fault == 1)
			h->tt_slots[4].status = TT_SLOT_ABORTED;
		if (fault == 2)
			h->tt_slots[4].xid++;
		if (fault == 3)
			h->tt_slots[4].wrap++;
		if (fault == 4)
			h->wrap_count++;
		if (fault == 5) {
			h->tt_slots[4].xid = 818;
			h->tt_slots[4].wrap = 8;
			h->tt_slots[4].status = TT_SLOT_COMMITTED;
			h->tt_slots[4].commit_scn = 999;
		}
		UT_ASSERT(pwrite(fileno(canonical_file), header.data, BLCKSZ, 0) == BLCKSZ);
		if (fault == 6 || fault == 7)
			UT_ASSERT_EQ(ftruncate(fileno(canonical_file), fault == 6 ? BLCKSZ : 0), 0);
		if (fault == 8)
			guard.mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
		if (fault == 9)
			operation.identity.record.record_crc++;
		cold_source_fail = fault == 10;
		canonical_file_io_error = fault == 11;
		record_target_drift = fault == 12 ? 1 : fault == 13 ? 2 : 0;
		cold_clog_denied = fault == 15;
		UT_ASSERT_NE(
			rf_side_online_production_apply_record_v1(plan, 1, &operation.identity, &owner),
			RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(
			canonical_writes + cold_clog_applies + prepare_apply.terminal_projection_stores, 0);
		native_commit_record_close(&owner);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_native_commit_record_keeps_ordered_undo_data)
{
	RfSideOnlinePlanV1 *plan = space_online_plan(500);
	ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
	ClusterThreadRecoveryAuthorityV1 authority
		= { .duty = &canonical_duty, .serial_guard = &guard };
	RfSideOnlineProductionOwnerV1 owner;
	RfSideOnlineOperationV1 operation;
	PGAlignedBlock header, before, after;
	FakeXactRecord fake;

	undo_header_bind(&fake, 0);
	undo_header_feed(plan, &fake, 100);
	record_prefix_fpi(plan, 200, 0x31);
	undo_header_commit(&fake, 0, 802);
	undo_header_feed(plan, &fake, 300);
	record_prefix_fpi(plan, 400, 0x52);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	native_commit_record_open(&header, &owner, &authority, false);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
	UT_ASSERT_EQ(rf_side_online_production_apply_record_v1(plan, 1, &operation.identity, &owner),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(pread(fileno(canonical_file), before.data, BLCKSZ, 9 * BLCKSZ) == BLCKSZ);
	UT_ASSERT_EQ(((UndoBlockHeader *)before.data)->block_lsn, 300);
	canonical_writes = cold_source_calls = 0;
	cold_source_ready = false;
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 2, &operation));
	UT_ASSERT_EQ(rf_side_online_production_apply_record_v1(plan, 2, &operation.identity, &owner),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(canonical_writes, 1);
	UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 1);
	UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 9 * BLCKSZ) == BLCKSZ);
	UT_ASSERT(memcmp(before.data, after.data, BLCKSZ) == 0);
	native_commit_record_close(&owner);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_cold_native_commit_record_failures_never_publish_completion)
{
	for (int fault = 0; fault < 13; fault++) {
		RfSideOnlinePlanV1 *plan = native_commit_record_plan(true, false);
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		RfSideOnlineOperationV1 operation;
		PGAlignedBlock header;
		volatile bool caught = false;
		volatile RfPageProofDetailV1 detail = RF_PAGE_PROOF_DETAIL_INTERNAL;

		native_commit_record_open(&header, &owner, &authority, true);
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
		cold_clog_apply_fail = fault == 0;
		cold_clog_verify_fail = fault == 1;
		cold_clog_stale_after_verify = fault == 2;
		cold_clog_throw = fault == 3;
		cold_stats_throw = fault == 4;
		cold_stats_expire = fault == 5;
		cold_source_throw = fault == 6;
		cold_cache_throw = fault == 7;
		cold_cache_expire = fault == 8;
		ctrc_io_fault = fault == 9 ? 1 : fault == 10 ? 3 : 0;
		record_target_drift = fault == 11 ? 3 : fault == 12 ? 4 : 0;
		PG_TRY();
		{
			detail
				= rf_side_online_production_apply_record_v1(plan, 1, &operation.identity, &owner);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_NE(detail, RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(caught, fault == 3 || fault == 4 || fault == 6 || fault == 7 || fault == 10);
		UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
		UT_ASSERT(!owner.protected_set_complete && !owner.protected_set_active
				  && owner.undo_headers == NULL && owner.protected_plan == NULL);
		native_commit_record_close(&owner);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_native_commit_record_idempotence_requires_sync)
{
	RfSideOnlinePlanV1 *plan = native_commit_record_plan(false, false);
	ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
	ClusterThreadRecoveryAuthorityV1 authority
		= { .duty = &canonical_duty, .serial_guard = &guard };
	RfSideOnlineProductionOwnerV1 owner;
	RfSideOnlineOperationV1 operation;
	PGAlignedBlock header;

	native_commit_record_open(&header, &owner, &authority, false);
	((UndoSegmentHeaderData *)header.data)->tt_slots[4].status = TT_SLOT_COMMITTED;
	((UndoSegmentHeaderData *)header.data)->tt_slots[4].commit_scn = 999;
	UT_ASSERT(pwrite(fileno(canonical_file), header.data, BLCKSZ, 0) == BLCKSZ);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
	record_sync_failure = true;
	UT_ASSERT_NE(rf_side_online_production_apply_record_v1(plan, 1, &operation.identity, &owner),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(canonical_writes + cold_clog_applies + prepare_apply.terminal_projection_stores,
				 0);
	native_commit_record_close(&owner);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_cold_selected_abort_completes_only_its_native_effects)
{
	for (int completion = 0; completion < 4; completion++) {
		for (int future = 0; future < 3; future++) {
			RfSideOnlinePlanV1 *plan = native_abort_record_plan_children(
				(completion & 1) != 0, false, future == 2, future != 0, completion >= 2);
			ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
			ClusterThreadRecoveryAuthorityV1 authority
				= { .duty = &canonical_duty, .serial_guard = &guard };
			RfSideOnlineProductionOwnerV1 owner;
			RfSideOnlineOperationV1 operation;
			PGAlignedBlock header, after, zero = { 0 };

			native_abort_record_open(&header, &owner, &authority, (completion & 1) != 0);
			cold_expected_children = completion >= 2 ? lengthof(cold_child_xids) : 0;
			UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
			for (int repeat = 0; repeat < 2; repeat++) {
				reset_prepare_apply();
				canonical_writes = canonical_syncs = cold_source_calls = cold_stats_calls = 0;
				cold_clog_applies = cold_clog_verifies = 0;
				cold_source_ready = false;
				UT_ASSERT_EQ(
					rf_side_online_production_apply_record_v1(plan, 1, &operation.identity, &owner),
					RF_PAGE_PROOF_DETAIL_OK);
				UT_ASSERT_EQ(canonical_writes, 0);
				UT_ASSERT(canonical_syncs > 0 && cold_source_calls == 1);
				UT_ASSERT_EQ(cold_clog_applies, 1);
				UT_ASSERT_EQ(cold_clog_verifies, 1);
				UT_ASSERT_EQ(cold_stats_calls, (completion & 1) ? 3 : 0);
				UT_ASSERT_EQ(cold_cache_calls, 0);
				UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 1);
				UT_ASSERT(cold_source_order < cold_clog_verified_order
						  && cold_clog_verified_order < prepare_apply.terminal_projection_order);
				if (completion & 1)
					UT_ASSERT(cold_clog_verified_order < cold_stats_order
							  && cold_stats_order < prepare_apply.terminal_projection_order);
				UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 0) == BLCKSZ);
				UT_ASSERT(memcmp(header.data, after.data, BLCKSZ) == 0);
				UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 9 * BLCKSZ) == BLCKSZ);
				UT_ASSERT(memcmp(zero.data, after.data, BLCKSZ) == 0);
				UT_ASSERT(!owner.protected_set_complete && !owner.protected_set_active
						  && owner.undo_headers == NULL && owner.protected_plan == NULL);
			}
			cold_expected_children = 0;
			native_abort_record_close(&owner);
			rf_side_online_plan_destroy_v1(&plan);
		}
	}
}

UT_TEST(test_cold_selected_abort_requires_ordered_tt_abort)
{
	RfSideOnlinePlanV1 *plan = native_abort_record_plan(false, false, false, true);
	ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
	ClusterThreadRecoveryAuthorityV1 authority
		= { .duty = &canonical_duty, .serial_guard = &guard };
	RfSideOnlineProductionOwnerV1 owner;
	RfSideOnlineOperationV1 operation;
	PGAlignedBlock header;

	native_abort_record_open(&header, &owner, &authority, false);
	((UndoSegmentHeaderData *)header.data)->tt_slots[4].status = TT_SLOT_ACTIVE;
	UT_ASSERT(pwrite(fileno(canonical_file), header.data, BLCKSZ, 0) == BLCKSZ);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
	UT_ASSERT_NE(rf_side_online_production_apply_record_v1(plan, 1, &operation.identity, &owner),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(canonical_writes + cold_clog_applies + prepare_apply.terminal_projection_stores,
				 0);
	/* Only the earlier exact TT_ABORT is allowed to change the canonical slot. */
	cold_source_calls = 0;
	cold_source_ready = false;
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
	UT_ASSERT_EQ(rf_side_online_production_apply_record_v1(plan, 0, &operation.identity, &owner),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(canonical_writes, 1);
	canonical_writes = cold_source_calls = 0;
	cold_source_ready = false;
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
	UT_ASSERT_EQ(rf_side_online_production_apply_record_v1(plan, 1, &operation.identity, &owner),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(canonical_writes, 0);
	UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 1);
	native_abort_record_close(&owner);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_cold_selected_abort_rejects_unproved_input_without_writes)
{
	for (int fault = 0; fault < 15; fault++) {
		RfSideOnlinePlanV1 *plan = native_abort_record_plan(true, fault == 0, false, true);
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		RfSideOnlineOperationV1 operation;
		PGAlignedBlock header;
		UndoSegmentHeaderData *h = (UndoSegmentHeaderData *)header.data;

		native_abort_record_open(&header, &owner, &authority, true);
		if (fault == 1)
			h->tt_slots[4].xid++;
		if (fault == 2)
			h->tt_slots[4].wrap++;
		if (fault == 3)
			h->wrap_count++;
		if (fault == 4) {
			h->tt_slots[4].status = TT_SLOT_COMMITTED;
			h->tt_slots[4].commit_scn = 999;
		}
		if (fault == 5)
			h->tt_slots[4].first_undo_block
				= (UBA){ .raw = { UINT64_C(513) | (UINT64_C(9) << 32), 4 } };
		if (fault == 6) {
			h->tt_slots[4].xid = 818;
			h->tt_slots[4].wrap = 8;
			h->tt_slots[4].status = TT_SLOT_ACTIVE;
		}
		UT_ASSERT(pwrite(fileno(canonical_file), header.data, BLCKSZ, 0) == BLCKSZ);
		if (fault == 7 || fault == 8)
			UT_ASSERT_EQ(ftruncate(fileno(canonical_file), fault == 7 ? BLCKSZ : 0), 0);
		if (fault == 9)
			guard.mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
		if (fault == 10)
			operation.identity.record.record_crc++;
		cold_source_fail = fault == 11;
		canonical_file_io_error = fault == 12;
		record_target_drift = fault == 13 ? 1 : fault == 14 ? 2 : 0;
		UT_ASSERT_NE(
			rf_side_online_production_apply_record_v1(plan, 1, &operation.identity, &owner),
			RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(
			canonical_writes + cold_clog_applies + prepare_apply.terminal_projection_stores, 0);
		native_abort_record_close(&owner);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_selected_abort_failure_and_error_keep_obligation_open)
{
	for (int fault = 0; fault < 10; fault++) {
		RfSideOnlinePlanV1 *plan = native_abort_record_plan(true, false, false, true);
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		RfSideOnlineOperationV1 operation;
		PGAlignedBlock header;
		volatile bool caught = false;
		volatile RfPageProofDetailV1 detail = RF_PAGE_PROOF_DETAIL_INTERNAL;

		native_abort_record_open(&header, &owner, &authority, true);
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
		cold_clog_apply_fail = fault == 0;
		cold_clog_verify_fail = fault == 1;
		cold_clog_stale_after_verify = fault == 2;
		cold_clog_throw = fault == 3;
		cold_stats_throw = fault == 4;
		cold_stats_expire = fault == 5;
		cold_source_throw = fault == 6;
		record_sync_failure = fault == 7;
		native_abort_sync_drift = fault == 8 ? 1 : fault == 9 ? 2 : 0;
		PG_TRY();
		{
			detail
				= rf_side_online_production_apply_record_v1(plan, 1, &operation.identity, &owner);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_NE(detail, RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(caught, fault == 3 || fault == 4 || fault == 6);
		UT_ASSERT_EQ(canonical_writes + prepare_apply.terminal_projection_stores, 0);
		if (fault >= 7)
			UT_ASSERT_EQ(cold_clog_applies, 0);
		UT_ASSERT(!owner.protected_set_complete && !owner.protected_set_active
				  && owner.undo_headers == NULL && owner.protected_plan == NULL);
		native_abort_record_close(&owner);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_abort_prefix_keeps_exact_identity_and_does_not_advance)
{
	for (int completion = 0; completion < 2; completion++) {
		RfSideOnlinePlanV1 *plan = native_abort_record_plan(completion != 0, false, true, true);
		RfSideOnlineOperationV1 operation;
		RfSideUndoHeaderImageV1 prefix, saved;
		PGAlignedBlock base;
		UndoSegmentHeaderData *h = (UndoSegmentHeaderData *)base.data;

		cluster_undo_segment_make_header_bytes(513, 3, base.data);
		h->tt_slots[4].xid = 802;
		h->tt_slots[4].wrap = 7;
		h->tt_slots[4].status = TT_SLOT_ABORTED;
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
		UT_ASSERT_EQ(rf_side_online_plan_prepare_undo_record_header_v1(plan, 1, &operation.identity,
																	   base.data, &prefix),
					 RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(memcmp(prefix.page.data, base.data, BLCKSZ) == 0);
		memset(&saved, 0x31, sizeof(saved));
		for (int fault = 0; fault < 6; fault++) {
			RfPageOnlineRecordIdentityV1 identity = operation.identity;

			if (fault == 0)
				identity.participant_index++;
			if (fault == 1)
				identity.reserved_zero++;
			if (fault == 2)
				identity.record.record_crc++;
			if (fault == 3)
				identity.record.origin_thread++;
			if (fault == 4)
				identity.record.read_rec_ptr++;
			if (fault == 5)
				identity.record.end_rec_ptr++;
			prefix = saved;
			UT_ASSERT_NE(rf_side_online_plan_prepare_undo_record_header_v1(plan, 1, &identity,
																		   base.data, &prefix),
						 RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT(memcmp(&prefix, &saved, sizeof(saved)) == 0);
		}
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_selected_abort_requires_indexed_xact_consumer)
{
	RfSideOnlinePlanV1 *plan = native_abort_record_plan(false, false, false, false);
	RfSideOnlineOperationV1 operation;
	RfSideOnlineApplyOpsV1 ops = { 0 };
	ApplyCapture capture = { 0 };

	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
	ops.arg = &capture;
	ops.source_thread = 3;
	ops.begin_protected_set = capture_begin;
	ops.end_protected_set = capture_end;
	ops.preflight_xact = ops.preflight_undo = accept_preflight;
	ops.apply_xact = capture_apply;
	ops.apply_undo = capture_apply_undo;
	UT_ASSERT_NE(rf_side_online_plan_apply_record_v1(plan, 1, &operation.identity, &ops),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(capture.begin_count + capture.count + capture.undo_count, 0);
	rf_side_online_plan_destroy_v1(&plan);
}

static RfSideOnlinePlanV1 *
cold_ctrc_record_plan(uint8 status, bool prior_commit, bool future)
{
	RfSideOnlinePlanV1 *plan = space_online_plan(future			? (prior_commit ? 500 : 400)
												 : prior_commit ? 300
																: 200);
	FakeXactRecord fake;
	xl_undo_tt_slot_bind bind = { .instance = 3,
								  .segment_id = 513,
								  .slot_offset = 4,
								  .xid = 818,
								  .wrap = 8,
								  .format_version = CLUSTER_UNDO_TT_BIND_VERSION };

	if (prior_commit) {
		undo_header_commit(&fake, 0, 802);
		undo_header_feed(plan, &fake, 100);
	}
	make_ctrc_release(&fake, 0, status);
	undo_header_feed(plan, &fake, prior_commit ? 200 : 100);
	if (future) {
		/* Recreate each carrier: feed poisons the caller's original bytes. */
		record_prefix_fpi(plan, prior_commit ? 300 : 200, 0x31);
		make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_BIND, &bind,
							   sizeof(bind));
		undo_header_feed(plan, &fake, prior_commit ? 400 : 300);
	}
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	return plan;
}

UT_TEST(test_cold_ctrc_record_changes_only_selected_release_bit)
{
	for (int variant = 0; variant < 4; variant++) {
		uint8 status = variant & 1 ? TT_SLOT_ABORTED : TT_SLOT_COMMITTED;
		RfSideOnlinePlanV1 *plan = cold_ctrc_record_plan(status, false, false);
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		RfSideOnlineOperationV1 operation;
		PGAlignedBlock header, expected, after;

		cold_undo_block_record_open(&header, &owner, &authority);
		ctrc_base(&header, 0, status);
		if (variant >= 2)
			((UndoSegmentHeaderData *)header.data)->tt_slots[4].flags
				= TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
		expected = header;
		((UndoSegmentHeaderData *)expected.data)->tt_slots[4].flags
			= TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
		UT_ASSERT(pwrite(fileno(canonical_file), header.data, BLCKSZ, 0) == BLCKSZ);
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
		for (int repeat = 0; repeat < 2; repeat++) {
			if (variant < 2)
			UT_ASSERT(cluster_undo_segment_header_seal(expected.data));
		canonical_writes = canonical_syncs = cold_source_calls = 0;
			cold_source_ready = false;
			UT_ASSERT_EQ(
				rf_side_online_production_apply_record_v1(plan, 0, &operation.identity, &owner),
				RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_EQ(canonical_writes, variant < 2 && repeat == 0 ? 1 : 0);
			UT_ASSERT(canonical_syncs > 0 && cold_source_calls == 1);
			UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 0) == BLCKSZ);
			UT_ASSERT(memcmp(expected.data, after.data, BLCKSZ) == 0);
			UT_ASSERT(!owner.protected_set_complete && !owner.protected_set_active);
		}
		cold_undo_block_record_close(&owner);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_ctrc_record_requires_prior_terminal_and_leaves_future_unapplied)
{
	RfSideOnlinePlanV1 *plan = cold_ctrc_record_plan(TT_SLOT_COMMITTED, true, true);
	ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
	ClusterThreadRecoveryAuthorityV1 authority
		= { .duty = &canonical_duty, .serial_guard = &guard };
	RfSideOnlineProductionOwnerV1 owner;
	RfSideOnlineOperationV1 operation;
	PGAlignedBlock header, expected, after, zero = { 0 };
	UndoSegmentHeaderData *h = (UndoSegmentHeaderData *)header.data;

	cold_undo_block_record_open(&header, &owner, &authority);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
	UT_ASSERT_NE(rf_side_online_production_apply_record_v1(plan, 1, &operation.identity, &owner),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(canonical_writes + canonical_syncs, 0);
	/* The original earlier terminal must be installed before release. */
	h->tt_slots[4].status = TT_SLOT_COMMITTED;
	h->tt_slots[4].commit_scn = 999;
	UT_ASSERT(pwrite(fileno(canonical_file), header.data, BLCKSZ, 0) == BLCKSZ);
	expected = header;
	((UndoSegmentHeaderData *)expected.data)->tt_slots[4].flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
	UT_ASSERT(cluster_undo_segment_header_seal(expected.data));
	cold_source_calls = 0;
	cold_source_ready = false;
	UT_ASSERT_EQ(rf_side_online_production_apply_record_v1(plan, 1, &operation.identity, &owner),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(canonical_writes, 1);
	UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 0) == BLCKSZ);
	UT_ASSERT(memcmp(expected.data, after.data, BLCKSZ) == 0);
	UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 9 * BLCKSZ) == BLCKSZ);
	UT_ASSERT(memcmp(zero.data, after.data, BLCKSZ) == 0);
	cold_undo_block_record_close(&owner);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_cold_ctrc_record_refuses_unproved_target_before_mutation)
{
	for (int fault = 0; fault < 14; fault++) {
		RfSideOnlinePlanV1 *plan = cold_ctrc_record_plan(TT_SLOT_COMMITTED, false, false);
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		RfSideOnlineOperationV1 operation;
		PGAlignedBlock header;
		UndoSegmentHeaderData *h = (UndoSegmentHeaderData *)header.data;
		uint32 selected = 0;

		if (fault == 13) {
			FakeXactRecord fake;
			xl_undo_tt_slot_bind bind = { .instance = 3,
										  .segment_id = 513,
										  .slot_offset = 5,
										  .xid = 818,
										  .wrap = 8,
										  .format_version = CLUSTER_UNDO_TT_BIND_VERSION };
			rf_side_online_plan_destroy_v1(&plan);
			plan = space_online_plan(300);
			make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_BIND, &bind,
								   sizeof(bind));
			undo_header_feed(plan, &fake, 100);
			make_ctrc_release(&fake, 0, TT_SLOT_COMMITTED);
			undo_header_feed(plan, &fake, 200);
			UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
			selected = 1; /* Release must not install another slot's earlier BIND. */
		}
		cold_undo_block_record_open(&header, &owner, &authority);
		ctrc_base(&header, 0, TT_SLOT_COMMITTED);
		if (fault == 0)
			h->wrap_count++;
		if (fault == 1)
			h->tt_slots[4].wrap++;
		if (fault == 2)
			h->tt_slots[4].xid++;
		if (fault == 3)
			h->tt_slots[4].status = TT_SLOT_ACTIVE;
		if (fault == 4)
			h->tt_slots[4].commit_scn = InvalidScn;
		if (fault == 5)
			h->tt_slots[4].flags = 0x80;
		UT_ASSERT(pwrite(fileno(canonical_file), header.data, BLCKSZ, 0) == BLCKSZ);
		if (fault == 6)
			UT_ASSERT_EQ(ftruncate(fileno(canonical_file), BLCKSZ), 0);
		if (fault == 7)
			guard.mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
		cold_source_fail = fault == 8;
		canonical_file_io_error = fault == 9;
		record_target_drift = fault == 10 ? 1 : fault == 11 ? 2 : 0;
		if (fault == 12) {
			Size available = rf_side_online_plan_scratch_available_v1(plan);
			UT_ASSERT(available > 13 * BLCKSZ);
			/* The cached target still fits; nested page scratch does not. */
			owner.borrowed_scratch_bytes = available - 13 * BLCKSZ;
		}
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, selected, &operation));
		UT_ASSERT_NE(
			rf_side_online_production_apply_record_v1(plan, selected, &operation.identity, &owner),
			RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(canonical_writes, 0);
		cold_undo_block_record_close(&owner);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_ctrc_record_failed_io_and_error_preserve_responsibility)
{
	for (int fault = 0; fault < 7; fault++) {
		RfSideOnlinePlanV1 *plan = cold_ctrc_record_plan(TT_SLOT_COMMITTED, false, false);
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		RfSideOnlineOperationV1 operation;
		PGAlignedBlock header;
		volatile bool caught = false;
		volatile RfPageProofDetailV1 detail = RF_PAGE_PROOF_DETAIL_INTERNAL;

		cold_undo_block_record_open(&header, &owner, &authority);
		ctrc_base(&header, 0, TT_SLOT_COMMITTED);
		if (fault == 6)
			((UndoSegmentHeaderData *)header.data)->tt_slots[4].flags
				= TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
		UT_ASSERT(pwrite(fileno(canonical_file), header.data, BLCKSZ, 0) == BLCKSZ);
		ctrc_io_fault = fault == 0 ? 1 : fault == 1 ? 5 : fault == 2 ? 3 : 0;
		cold_source_throw = fault == 3;
		record_target_drift = fault == 4 ? 3 : fault == 5 ? 4 : 0;
		record_sync_failure = fault == 6;
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
		PG_TRY();
		{
			detail
				= rf_side_online_production_apply_record_v1(plan, 0, &operation.identity, &owner);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_NE(detail, RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(caught, fault == 2 || fault == 3);
		UT_ASSERT_EQ(canonical_writes, fault == 3 || fault == 6 ? 0 : 1);
		cold_undo_block_record_close(&owner);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

/* Structural IO/proof is tested by test_cluster_space_storage. Here the
 * boundary records the real selected-record owner and XACT execution order. */
static bool compound_test;
static int compound_fault;
static unsigned compound_preflights, compound_applies, compound_order;
static RfPageOnlineRecordIdentityV1 compound_identity;
static uint8 compound_payload[4096];
static uint32 compound_length;

extern bool cluster_space_cold_drop_foreign_preflight_v1(XLogReaderState *record,
														 TransactionId xid);
extern bool cluster_space_cold_drop_foreign_v1(XLogReaderState *record, TransactionId xid);

static void
compound_record_check(XLogReaderState *record, TransactionId xid)
{
	UT_ASSERT(compound_test && canonical_scope != NULL && cold_source_ready);
	UT_ASSERT_EQ(xid, 802);
	UT_ASSERT_EQ(record->ReadRecPtr, compound_identity.record.read_rec_ptr);
	UT_ASSERT_EQ(record->EndRecPtr, compound_identity.record.end_rec_ptr);
	UT_ASSERT_EQ(record->cluster_expected_thread_id, compound_identity.record.origin_thread);
	UT_ASSERT_EQ(record->record->header.xl_crc, compound_identity.record.record_crc);
	UT_ASSERT_EQ(XLogRecGetInfo(record), compound_identity.record.info);
	UT_ASSERT_EQ(XLogRecGetRmid(record), RM_XACT_ID);
	UT_ASSERT_EQ(XLogRecGetXid(record), xid);
	UT_ASSERT(!XLogRecHasAnyBlockRefs(record));
	UT_ASSERT_EQ(XLogRecGetDataLen(record), compound_length);
	UT_ASSERT(memcmp(XLogRecGetData(record), compound_payload, compound_length) == 0);
	UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
}

bool
cluster_space_cold_drop_foreign_preflight_v1(XLogReaderState *record, TransactionId xid)
{
	compound_record_check(record, xid);
	UT_ASSERT_EQ(canonical_writes + cold_clog_applies + cold_cache_calls, 0);
	compound_preflights++;
	if (compound_fault == 1)
		pg_re_throw();
	if (compound_fault == 12)
		cold_clog_denied = true;
	if (compound_fault == 13 || compound_fault == 14)
		compound_change_file(compound_fault == 14);
	return compound_fault != 0;
}

bool
cluster_space_cold_drop_foreign_v1(XLogReaderState *record, TransactionId xid)
{
	xl_xact_parsed_commit parsed;

	compound_record_check(record, xid);
	UT_ASSERT_EQ(compound_preflights, 1);
	UT_ASSERT(canonical_syncs > 0 && cold_clog_verifies == 1);
	UT_ASSERT(ParseCommitRecord(XLogRecGetInfo(record), (xl_xact_commit *)XLogRecGetData(record),
								XLogRecGetDataLen(record), &parsed));
	UT_ASSERT_EQ(cold_cache_calls, parsed.nmsgs > 0 ? 1 : 0);
	UT_ASSERT_EQ(cold_locks_calls, (parsed.xinfo & XACT_XINFO_HAS_AE_LOCKS) != 0 ? 1 : 0);
	UT_ASSERT_EQ(cold_stats_calls, 0);
	compound_applies++;
	compound_order = ++prepare_apply.next_order;
	if (compound_fault == 3)
		pg_re_throw();
	if (compound_fault == 4)
		cold_clog_denied = true;
	return compound_fault != 2;
}

static RfSideOnlinePlanV1 *
compound_record_plan(int shape, bool children)
{
	RfSideOnlinePlanV1 *plan = space_online_plan(400);
	ClusterSpaceStructureChange drops[2] = { space_drop_fixture(16384), space_drop_fixture(16385) };
	FakeXactRecord fake;

	undo_header_bind(&fake, 0);
	undo_header_feed(plan, &fake, 100);
	cold_drop_commit(&fake, drops, 2, shape);
	if (children)
		cold_terminal_children(&fake, false);
	/* The actual native record carries this flag for structural changes. */
	fake.u.decoded.header.xl_info |= XLR_SPECIAL_REL_UPDATE;
	if (space_commit_feed(plan, &fake, 200) != RF_PAGE_PROOF_DETAIL_OK) {
		UT_ASSERT(false);
		rf_side_online_plan_destroy_v1(&plan);
		return NULL;
	}
	record_prefix_fpi(plan, 300, 0x31);
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	return plan;
}

static void
compound_capture(const RfSideOnlineOperationV1 *operation)
{
	compound_test = true;
	compound_preflights = compound_applies = compound_order = 0;
	compound_identity = operation->identity;
	compound_length = operation->owned_payload_length;
	cold_expected_children
		= (operation->xact.xinfo & XACT_XINFO_HAS_SUBXACTS) != 0 ? lengthof(cold_child_xids) : 0;
	UT_ASSERT(compound_length <= sizeof(compound_payload));
	memcpy(compound_payload, operation->owned_payload, compound_length);
	cold_locks_test = (operation->xact.xinfo & XACT_XINFO_HAS_AE_LOCKS) != 0;
	standbyState = cold_locks_test ? STANDBY_INITIALIZED : STANDBY_DISABLED;
	cold_locks_calls = cold_locks_order = 0;
	payload_tracking = true;
	payload_bytes = payload_peak = 0;
}

static void
compound_close(RfSideOnlineProductionOwnerV1 *owner)
{
	payload_tracking = false;
	UT_ASSERT_EQ(payload_bytes, 0);
	UT_ASSERT(payload_peak <= compound_length);
	UT_ASSERT(!owner->protected_set_complete && DatabasePath == NULL);
	compound_test = cold_locks_test = false;
	cold_expected_children = 0;
	compound_io_drift = 0;
	standbyState = STANDBY_DISABLED;
	native_commit_record_close(owner);
}

UT_TEST(test_cold_selected_compound_commit_preserves_all_effect_order)
{
	for (int shape = 0; shape < 12; shape++) {
		RfSideOnlinePlanV1 *plan = compound_record_plan(shape % 6, shape >= 6);
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		RfSideOnlineOperationV1 operation;
		PGAlignedBlock header, after, zero = { 0 };

		if (plan == NULL)
			continue;
		native_commit_record_open(&header, &owner, &authority, true);
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
		compound_capture(&operation);
		compound_fault = -1;
		UT_ASSERT_EQ(
			rf_side_online_production_apply_record_v1(plan, 1, &operation.identity, &owner),
			RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(compound_preflights, 1);
		UT_ASSERT_EQ(compound_applies, 1);
		UT_ASSERT_EQ(canonical_writes, 1);
		UT_ASSERT_EQ(cold_clog_verifies, 1);
		UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 1);
		UT_ASSERT(cold_clog_verified_order < compound_order);
		UT_ASSERT(compound_order < prepare_apply.terminal_projection_order);
		if (operation.xact.invalidation_count > 0)
			UT_ASSERT(cold_cache_order < compound_order);
		if ((operation.xact.xinfo & XACT_XINFO_HAS_DROPPED_STATS) != 0)
			UT_ASSERT(compound_order < cold_stats_order);
		if (cold_locks_test)
			UT_ASSERT(cold_locks_order < compound_order);
		((UndoSegmentHeaderData *)header.data)->tt_slots[4].status = TT_SLOT_COMMITTED;
		((UndoSegmentHeaderData *)header.data)->tt_slots[4].commit_scn = 999;
		UT_ASSERT(cluster_undo_segment_header_seal(header.data));
		UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 0) == BLCKSZ);
		UT_ASSERT(memcmp(header.data, after.data, BLCKSZ) == 0);
		UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 9 * BLCKSZ) == BLCKSZ);
		UT_ASSERT(memcmp(zero.data, after.data, BLCKSZ) == 0);
		compound_close(&owner);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_selected_compound_commit_refusals_never_publish_terminal)
{
	for (int scenario = 0; scenario < 26; scenario++) {
		int fault = scenario % 13;
		RfSideOnlinePlanV1 *plan = compound_record_plan(4, scenario >= 13);
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		RfSideOnlineOperationV1 operation;
		PGAlignedBlock header;
		volatile bool caught = false;
		volatile RfPageProofDetailV1 detail = RF_PAGE_PROOF_DETAIL_INTERNAL;

		if (plan == NULL)
			continue;
		native_commit_record_open(&header, &owner, &authority, true);
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
		compound_capture(&operation);
		compound_fault = fault;
		cold_cache_throw = fault == 5;
		cold_cache_expire = fault == 6;
		cold_clog_verify_fail = fault == 7;
		cold_source_fail = fault == 8;
		ctrc_io_fault = fault == 9 ? 1 : 0;
		if (fault == 10)
			operation.identity.record.record_crc++;
		if (fault == 11)
			guard.mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
		PG_TRY();
		{
			detail
				= rf_side_online_production_apply_record_v1(plan, 1, &operation.identity, &owner);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 1 || fault == 3 || fault == 5);
		UT_ASSERT_NE(detail, RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
		UT_ASSERT_EQ(cold_stats_calls, 0);
		if (fault <= 1 || (fault >= 8 && fault != 9))
			UT_ASSERT_EQ(canonical_writes + cold_clog_applies, 0);
		if (fault == 9)
			UT_ASSERT(canonical_writes == 1 && cold_clog_applies == 0);
		UT_ASSERT_EQ(compound_applies, fault >= 2 && fault <= 4 ? 1 : 0);
		compound_close(&owner);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_compound_preserves_strict_tt_file_binding_across_space_and_write)
{
	for (int fault = 0; fault < 4; fault++) {
		RfSideOnlinePlanV1 *plan = compound_record_plan(4, false);
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		RfSideOnlineProductionOwnerV1 owner;
		RfSideOnlineOperationV1 operation;
		PGAlignedBlock header;

		native_commit_record_open(&header, &owner, &authority, true);
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
		compound_capture(&operation);
		compound_fault = fault < 2 ? 13 + fault : -1;
		compound_io_drift = fault < 2 ? 0 : fault - 1;
		UT_ASSERT_NE(
			rf_side_online_production_apply_record_v1(plan, 1, &operation.identity, &owner),
			RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(compound_preflights, 1);
		UT_ASSERT_EQ(canonical_writes, fault < 2 ? 0 : 1);
		UT_ASSERT_EQ(cold_clog_applies + cold_cache_calls + compound_applies + cold_stats_calls
						 + prepare_apply.terminal_projection_stores,
					 0);
		compound_close(&owner);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_cold_compound_entry_refuses_unbound_record_before_effects)
{
	ClusterSpaceStructureChange drop = space_drop_fixture(16384);
	ClusterUndoRecoveryScopeV1 scope = { 0 };
	int covered = 0;

	reset_prepare_apply();
	canonical_scope = &scope;
	cold_clog_test = cold_source_ready = true;
	for (int fault = 0; fault < 14; fault++) {
		FakeXactRecord fake;
		RfSideXactOperationV1 operation;
		XLogReaderState *record = &fake.reader;

		cold_drop_commit(&fake, &drop, 1, 4);
		UT_ASSERT(rf_side_xact_decode_recovery_v1(record, UINT64_C(0x11223344), 3, &operation));
		record->ReadRecPtr = 200;
		record->EndRecPtr = 300;
		record->system_identifier = operation.system_identifier;
		record->cluster_expected_thread_id = 3;
		switch (fault) {
		case 0:
			record = NULL;
			break;
		case 1:
			record->record = NULL;
			break;
		case 2:
			fake.u.decoded.header.xl_xid++;
			break;
		case 3:
			record->system_identifier++;
			break;
		case 4:
			record->cluster_expected_thread_id++;
			break;
		case 5:
			record->ReadRecPtr = InvalidXLogRecPtr;
			break;
		case 6:
			record->EndRecPtr = record->ReadRecPtr;
			break;
		case 7:
			fake.u.decoded.header.xl_rmid = RM_STANDBY_ID;
			break;
		case 8:
			fake.u.decoded.header.xl_info |= XLR_CHECK_CONSISTENCY;
			break;
		case 9:
			fake.u.decoded.max_block_id = 0;
			break;
		case 10:
			fake.u.decoded.record_origin = 1;
			break;
		case 11:
			fake.u.decoded.toplevel_xid = 802;
			break;
		case 12:
			fake.u.decoded.has_page_version_edge = true;
			break;
		case 13:
			fake.u.decoded.main_data_len--;
			break;
		}
		UT_ASSERT_EQ(rf_side_xact_apply_cold_drop_completion_v1(&operation, record, &covered,
																drop_coverage_must_not_run),
					 RF_SIDE_XACT_APPLY_BLOCKED);
	}
	UT_ASSERT_EQ(covered, 0);
	UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
	canonical_scope = NULL;
	cold_clog_test = cold_source_ready = false;
}

UT_TEST(test_cold_compound_capture_rejects_original_origin_or_top_frame)
{
	for (int fault = 0; fault < 2; fault++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(200);
		ClusterSpaceStructureChange drop = space_drop_fixture(16384);
		FakeXactRecord fake;

		cold_drop_commit(&fake, &drop, 1, 4);
		if (fault == 0)
			fake.u.decoded.record_origin = 1;
		else
			fake.u.decoded.toplevel_xid = 802;
		UT_ASSERT_NE(space_commit_feed(plan, &fake, 100), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_NE(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

static void
cold_terminal_frame(FakeXactRecord *fake, bool abort, bool completion, int field)
{
	if (abort) {
		if (completion)
			cold_completion_abort(fake);
		else
			make_abort(fake, 802);
	} else if (completion)
		cold_completion_commit(fake, true, true, true);
	else
		make_commit(fake, 802, 12345, 123456, false);
	fake->u.decoded.max_block_id = -1;
	if (field == 0)
		fake->u.decoded.record_origin = 1;
	else if (field == 1)
		fake->u.decoded.toplevel_xid = 674;
	else if (field == 2)
		fake->u.decoded.has_page_version_edge = true;
}

static void
check_cold_terminal_frame(bool completion)
{
	const RfSideXactOperationV1 zero = { 0 };

	for (int abort = 0; abort < 2; abort++)
		for (int field = 0; field < 3; field++) {
			FakeXactRecord fake;
			RfSideXactOperationV1 operation;

			cold_terminal_frame(&fake, abort, completion, -1);
			UT_ASSERT(
				rf_side_xact_decode_recovery_v1(&fake.reader, UINT64_C(0x11223344), 3, &operation));
			UT_ASSERT_EQ(operation.cold_completion_effects, completion);
			cold_terminal_frame(&fake, abort, completion, field);
			memset(&operation, 0x5a, sizeof(operation));
			UT_ASSERT(!rf_side_xact_decode_recovery_v1(&fake.reader, UINT64_C(0x11223344), 3,
													   &operation));
			UT_ASSERT(memcmp(&operation, &zero, sizeof(operation)) == 0);
		}
}

UT_TEST(test_cold_strict_terminals_refuse_unconsumed_frame)
{
	check_cold_terminal_frame(false);
}

UT_TEST(test_cold_effectful_terminals_refuse_unconsumed_frame)
{
	check_cold_terminal_frame(true);
}

UT_TEST(test_cold_terminal_capture_keeps_unconsumed_frame_out_of_history_and_replay)
{
	for (int history = 0; history < 2; history++)
		for (int abort = 0; abort < 2; abort++)
			for (int completion = 0; completion < 2; completion++)
				for (int field = 0; field < 3; field++) {
					XLogRecPtr redo = history ? 200 : 100;
					RfSideOnlinePlanV1 *plan = space_online_plan_redo(200, &redo);
					FakeXactRecord fake;

					cold_terminal_frame(&fake, abort, completion, field);
					UT_ASSERT_NE(space_commit_feed(plan, &fake, 100), RF_PAGE_PROOF_DETAIL_OK);
					UT_ASSERT_NE(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
					rf_side_online_plan_destroy_v1(&plan);
				}
}

UT_TEST(test_cold_terminal_frame_refusal_preserves_pi_inspection)
{
	for (int field = 0; field < 3; field++) {
		FakeXactRecord fake;
		RfSideXactOperationV1 operation;

		cold_terminal_frame(&fake, false, false, field);
		UT_ASSERT(rf_side_xact_inspect_pi_v1(&fake.reader, UINT64_C(0x11223344), 3, &operation));
		UT_ASSERT(operation.pi_read_only && !operation.cold_completion_effects);
	}
}

/* Native reader/raw CRC is a leaf boundary in this planner fixture. */
static RfSideOnlinePlanV1 *
terminal_witness_plan_shape(ClusterWalSourceRef *source, Size budget, unsigned shape)
{
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut = { 0 };
	RfSideOnlinePlanRequestV1 request = { 0 };
	ClusterWalSourceRef sources[2];
	XLogRecPtr redo = 100;

	memset(source, 0, sizeof(*source));
	source->claim.identity.system_identifier = UINT64_C(0x11223344);
	memset(source->claim.identity.storage_uuid, 0x44, 16);
	memset(source->claim.identity.authority_uuid, 0x55, 16);
	source->claim.identity.origin_thread_id = 3;
	source->claim.identity.origin_node_id = 2;
	source->claim.identity.origin_owner_incarnation = 19;
	source->claim.identity.thread_claim_created_at = 1;
	source->claim.identity.root_lineage_seq = 1;
	source->claim.database_incarnation = 42;
	source->claim.max_config_generation = 4;
	memset(source->claim.claim_sha256, 0x66, 32);
	source->timeline = 7;
	cut.failed_thread = 3;
	cut.origin_owner_incarnation = 19;
	cut.timeline_id = 7;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 512;
	request.system_identifier = source->claim.identity.system_identifier;
	memcpy(request.storage_uuid, source->claim.identity.storage_uuid, 16);
	request.physical_cuts = &cut;
	request.participant_count = 1;
	request.redo_starts = &redo;
	request.memory_budget = budget;
	request.purpose = shape == 3 ? RF_SIDE_PLAN_PI_CONTRIBUTIONS : RF_SIDE_PLAN_RECOVERY;
	if (shape == 3)
		request.redo_starts = NULL;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	sources[0] = *source;
	sources[1] = *source;
	sources[1].claim.identity.origin_owner_incarnation++;
	sources[1].claim.identity.root_lineage_seq++;
	sources[1].claim.claim_sha256[31] ^= 1;
	if (shape == 2) {
		sources[1].claim.identity.origin_node_id = 1;
		sources[1].claim.identity.origin_thread_id = 2;
	}
	if (shape != 3)
		UT_ASSERT(rf_side_online_plan_bind_sources_v1(plan, sources, shape == 0 ? 1 : 2));
	return plan;
}

static RfSideOnlinePlanV1 *
terminal_witness_plan(ClusterWalSourceRef *source, Size budget)
{
	return terminal_witness_plan_shape(source, budget, 0);
}

UT_TEST(test_recovery_single_source_uses_complete_original_bound_origin)
{
	for (unsigned shape = 0; shape < 4; shape++) {
		ClusterWalSourceRef source, other;
		RfSideOnlinePlanV1 *plan = terminal_witness_plan_shape(&source, 0, shape);
		FakeXactRecord fake;
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 record;
		UT_ASSERT(!rf_side_online_plan_recovery_single_source_v1(plan, &source));
		cold_terminal_frame(&fake, false, false, -1);
		fake.reader.cluster_expected_thread_id = 3;
		fake.reader.seg.ws_tli = 7;
		identity = make_identity(&fake, source.claim.identity.storage_uuid);
		set_identity_range(&fake, &identity, 100, 512);
		record = make_record_plan(&fake);
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
					 RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_recovery_single_source_v1(plan, &source),
					 shape == 0 || shape == 2);
		other = source;
		other.claim.claim_sha256[31] ^= 1;
		UT_ASSERT(!rf_side_online_plan_recovery_single_source_v1(plan, &other));
		UT_ASSERT(!rf_side_online_plan_recovery_single_source_v1(plan, NULL));
		UT_ASSERT(!rf_side_online_plan_recovery_single_source_v1(NULL, &source));
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_native_terminal_witness_owns_exact_original_header_and_payload)
{
	for (unsigned aborting = 0; aborting < 2; aborting++) {
		ClusterWalSourceRef source;
		RfSideOnlinePlanV1 *plan = terminal_witness_plan(&source, 0);
		FakeXactRecord fake, original;
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 record;
		RfSideOnlineOperationV1 operation;

		if (aborting) {
			undo_header_abort(&fake, 4);
			undo_header_feed(plan, &fake, 100);
		}
		cold_terminal_frame(&fake, aborting, false, -1);
		fake.reader.cluster_expected_thread_id = 3;
		fake.reader.seg.ws_tli = 7;
		fake.u.decoded.header.xl_tot_len = SizeOfXLogRecord + 2 + fake.u.decoded.main_data_len;
		identity = make_identity(&fake, source.claim.identity.storage_uuid);
		set_identity_range(&fake, &identity, aborting ? 200 : 100, 512);
		record = make_record_plan(&fake);
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
					 RF_PAGE_PROOF_DETAIL_OK);
		original = fake;
		original.reader.record = &original.u.decoded;
		original.u.decoded.main_data = (char *)original.data;
		UT_ASSERT_EQ(rf_side_online_plan_native_terminal_count_v1(plan, &source), UINT32_MAX);
		UT_ASSERT(!rf_side_online_plan_terminal_matches_v1(plan, &source, &original.reader));
		/* Borrowed decoder bytes are reused before sealing. */
		memset(fake.data, 0xa5, sizeof(fake.data));
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(rf_side_online_plan_terminal_matches_v1(plan, &source, &original.reader));
		UT_ASSERT_EQ(rf_side_online_plan_native_terminal_count_v1(plan, &source), 1);
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, aborting ? 1 : 0, &operation));
		UT_ASSERT_EQ(operation.owned_payload_length, 0);
		UT_ASSERT(operation.owned_payload == NULL);
		for (unsigned fault = 0; fault < 18; fault++) {
			FakeXactRecord query = original;
			ClusterWalSourceRef other = source;
			query.reader.record = &query.u.decoded;
			query.u.decoded.main_data = (char *)query.data;
			switch (fault) {
			case 0:
				query.data[0] ^= 1;
				break; /* Timestamp, not merely decoded TT fields. */
			case 1:
				query.u.decoded.header.xl_prev ^= 1;
				break;
			case 2:
				query.u.decoded.header.xl_crc ^= 1;
				break;
			case 3:
				query.u.decoded.header.xl_xid++;
				break;
			case 4:
				query.reader.cluster_expected_thread_id++;
				break;
			case 5:
				query.reader.seg.ws_tli++;
				break;
			case 6:
				query.reader.ReadRecPtr++;
				break;
			case 7:
				query.reader.EndRecPtr++;
				break;
			case 8:
				query.u.decoded.main_data_len--;
				break;
			case 9:
				query.reader.cluster_record_crc_failed = true;
				break;
			case 10:
				other.claim.claim_sha256[31] ^= 1;
				break;
			case 11:
				other.claim.identity.origin_owner_incarnation++;
				break;
			case 12:
				query.u.decoded.record_origin = 1;
				break;
			case 13:
				query.u.decoded.toplevel_xid = 802;
				break;
			case 14:
				query.u.decoded.max_block_id = 0;
				break;
			case 15:
				query.u.decoded.has_page_version_edge = true;
				break;
			case 16:
				query.u.decoded.lsn++;
				break;
			case 17:
				query.u.decoded.next_lsn++;
				break;
			}
			UT_ASSERT(!rf_side_online_plan_terminal_matches_v1(plan, &other, &query.reader));
			UT_ASSERT_EQ(rf_side_online_plan_native_terminal_count_v1(plan, &other),
						 fault == 10 || fault == 11 ? UINT32_MAX : 1);
		}
		UT_ASSERT(rf_side_online_plan_terminal_matches_v1(plan, &source, &original.reader));
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_native_terminal_witness_uses_original_bound_plan_budget)
{
	Size used = 0;
	for (unsigned attempt = 0; attempt < 3; attempt++) {
		ClusterWalSourceRef source;
		RfSideOnlinePlanV1 *plan
			= terminal_witness_plan(&source, attempt == 0 ? 0 : used - (attempt == 1));
		FakeXactRecord fake;
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 record;
		RfPageProofDetailV1 detail;
		cold_terminal_frame(&fake, false, false, -1);
		identity = make_identity(&fake, source.claim.identity.storage_uuid);
		set_identity_range(&fake, &identity, 100, 512);
		record = make_record_plan(&fake);
		detail = rf_side_online_plan_feed_record_v1(plan, &record, &identity);
		UT_ASSERT_EQ(detail,
					 attempt == 1 ? RF_PAGE_PROOF_DETAIL_CAPACITY : RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan) == RF_PAGE_PROOF_DETAIL_OK, attempt != 1);
		if (attempt == 0)
			used = RF_SIDE_ONLINE_PLAN_MAX_BYTES - rf_side_online_plan_scratch_available_v1(plan);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_native_terminal_witness_rejects_duplicate_physical_raw_terminal)
{
	ClusterWalSourceRef source;
	RfSideOnlinePlanV1 *plan = terminal_witness_plan(&source, 0);
	FakeXactRecord fake;
	for (unsigned i = 0; i < 2; i++) {
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 record;
		cold_terminal_frame(&fake, false, false, -1);
		fake.reader.cluster_expected_thread_id = 3;
		fake.reader.seg.ws_tli = 7;
		identity = make_identity(&fake, source.claim.identity.storage_uuid);
		set_identity_range(&fake, &identity, i == 0 ? 100 : 300, i == 0 ? 300 : 512);
		record = make_record_plan(&fake);
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
					 RF_PAGE_PROOF_DETAIL_OK);
	}
	(void)rf_side_online_plan_seal_v1(plan);
	UT_ASSERT(!rf_side_online_plan_terminal_matches_v1(plan, &source, &fake.reader));
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_native_terminal_witness_rejects_unbound_plan)
{
	RfSideOnlinePlanV1 *plan = space_online_plan(200);
	FakeXactRecord fake;
	ClusterWalSourceRef source = { 0 };

	cold_terminal_frame(&fake, false, false, -1);
	UT_ASSERT_EQ(space_commit_feed(plan, &fake, 100), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(!rf_side_online_plan_terminal_matches_v1(plan, &source, &fake.reader));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(!rf_side_online_plan_terminal_matches_v1(plan, &source, &fake.reader));
	rf_side_online_plan_destroy_v1(&plan);
}
/* Keep the incoming carrier cases separate from the existing fixture family. */
#define terminal_witness_plan_shape adopted_relmap_terminal_witness_plan_shape
#define terminal_witness_plan adopted_relmap_terminal_witness_plan
#define test_native_terminal_witness_uses_original_bound_plan_budget adopted_relmap_test_native_terminal_witness_uses_original_bound_plan_budget
#define test_native_terminal_witness_rejects_duplicate_physical_raw_terminal adopted_relmap_test_native_terminal_witness_rejects_duplicate_physical_raw_terminal
#define test_native_terminal_witness_owns_exact_original_header_and_payload adopted_relmap_test_native_terminal_witness_owns_exact_original_header_and_payload
#define test_native_terminal_witness_rejects_unbound_plan adopted_relmap_test_native_terminal_witness_rejects_unbound_plan
#include "test_cluster_side_relmap_terminal.inc"
#undef test_native_terminal_witness_rejects_unbound_plan
#undef test_native_terminal_witness_owns_exact_original_header_and_payload
#undef test_native_terminal_witness_rejects_duplicate_physical_raw_terminal
#undef test_native_terminal_witness_uses_original_bound_plan_budget
#undef terminal_witness_plan
#undef terminal_witness_plan_shape

#include "test_cluster_side_clog_full.inc"
#define terminal_witness_plan_shape adopted_relmap_terminal_witness_plan_shape
#define terminal_witness_plan adopted_relmap_terminal_witness_plan
#include "test_cluster_side_clog_witness.inc"
#include "test_cluster_side_clog_applied.inc"
#undef terminal_witness_plan
#undef terminal_witness_plan_shape

int
main(void)
{
	if (getenv("PGRAC_PRE2_TEST_CLOG_APPLIED") != NULL) {
		UT_PLAN(4);
		UT_RUN(test_clog_applied_requires_original_cold_record_success);
		UT_RUN(test_clog_applied_failed_attempt_and_error_keep_no_reusable_success);
		UT_RUN(test_clog_applied_cannot_borrow_other_plan_or_whole_source_apply);
		UT_RUN(test_clog_applied_requires_exact_identity_and_final_owner_after_cleanup);
		UT_DONE();
		return ut_failed_count != 0;
	}

	if (getenv("PGRAC_PRE2_TEST_CLOG_WITNESS") != NULL) {
		UT_PLAN(4);
	UT_RUN(test_clog_witness_keeps_complete_native_bytes_after_reader_reuse);
	UT_RUN(test_clog_witness_refuses_changed_full_header_payload_and_source);
	UT_RUN(test_clog_witness_requires_original_physical_recovery_binding);
	UT_RUN(test_clog_witness_charges_original_budget_and_counts_complete_stream);
		UT_DONE();
		return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_CLOG_FULL_SIDE") != NULL) {
		UT_PLAN(3);
		UT_RUN(test_full_clog_plan_owns_all_original_birth_bytes);
		UT_RUN(test_full_clog_shape_refuses_incomplete_or_unconsumed_frame);
		UT_RUN(test_full_clog_cold_owner_keeps_native_postread_before_projection);
		UT_DONE();
		return ut_failed_count != 0;
	}
	UT_PLAN(206);
	UT_RUN(test_clog_witness_keeps_complete_native_bytes_after_reader_reuse);
	UT_RUN(test_clog_witness_refuses_changed_full_header_payload_and_source);
	UT_RUN(test_clog_witness_requires_original_physical_recovery_binding);
	UT_RUN(test_clog_witness_charges_original_budget_and_counts_complete_stream);

	UT_RUN(test_full_clog_plan_owns_all_original_birth_bytes);
	UT_RUN(test_full_clog_shape_refuses_incomplete_or_unconsumed_frame);
	UT_RUN(test_full_clog_cold_owner_keeps_native_postread_before_projection);
	UT_RUN(adopted_relmap_test_native_terminal_witness_uses_original_bound_plan_budget);
	UT_RUN(adopted_relmap_test_native_terminal_witness_rejects_duplicate_physical_raw_terminal);
	UT_RUN(adopted_relmap_test_native_terminal_witness_owns_exact_original_header_and_payload);
	UT_RUN(adopted_relmap_test_native_terminal_witness_rejects_unbound_plan);
	UT_RUN(test_recovery_single_source_uses_complete_original_bound_origin);
	UT_RUN(test_native_terminal_witness_uses_original_bound_plan_budget);
	UT_RUN(test_native_terminal_witness_rejects_duplicate_physical_raw_terminal);
	UT_RUN(test_native_terminal_witness_owns_exact_original_header_and_payload);
	UT_RUN(test_native_terminal_witness_rejects_unbound_plan);
	UT_RUN(test_cold_strict_terminals_refuse_unconsumed_frame);
	UT_RUN(test_cold_effectful_terminals_refuse_unconsumed_frame);
	UT_RUN(test_cold_terminal_capture_keeps_unconsumed_frame_out_of_history_and_replay);
	UT_RUN(test_cold_terminal_frame_refusal_preserves_pi_inspection);
	UT_RUN(test_cold_ctrc_record_changes_only_selected_release_bit);
	UT_RUN(test_cold_ctrc_record_requires_prior_terminal_and_leaves_future_unapplied);
	UT_RUN(test_cold_ctrc_record_refuses_unproved_target_before_mutation);
	UT_RUN(test_cold_ctrc_record_failed_io_and_error_preserve_responsibility);
	UT_RUN(test_cold_selected_abort_completes_only_its_native_effects);
	UT_RUN(test_cold_selected_abort_requires_ordered_tt_abort);
	UT_RUN(test_cold_selected_abort_rejects_unproved_input_without_writes);
	UT_RUN(test_cold_selected_abort_failure_and_error_keep_obligation_open);
	UT_RUN(test_cold_abort_prefix_keeps_exact_identity_and_does_not_advance);
	UT_RUN(test_cold_selected_abort_requires_indexed_xact_consumer);
	UT_RUN(test_cold_native_commit_record_installs_only_selected_terminal);
	UT_RUN(test_cold_native_commit_record_requires_prior_tt_steps);
	UT_RUN(test_cold_native_commit_record_refuses_unproved_inputs_before_write);
	UT_RUN(test_cold_native_commit_record_keeps_ordered_undo_data);
	UT_RUN(test_cold_native_commit_record_failures_never_publish_completion);
	UT_RUN(test_cold_native_commit_record_idempotence_requires_sync);
	UT_RUN(test_cold_selected_abort_delete_preserves_native_order_and_exact_prefix);
	UT_RUN(test_cold_selected_abort_delete_failure_or_error_cannot_complete);
	UT_RUN(test_cold_native_abort_record_requires_installed_tt_before_exact_effects);
	UT_RUN(test_cold_native_abort_record_refuses_unproved_prefix_and_native_failures);
	UT_RUN(test_cold_exact_abort_record_installs_only_its_terminal_header);
	UT_RUN(test_cold_exact_abort_record_keeps_complete_identity_and_source_refusals);
	UT_RUN(test_cold_exact_abort_record_io_error_and_postwrite_drift_keep_incomplete);
	UT_RUN(test_cold_exact_abort_record_idempotence_still_requires_sync);
	UT_RUN(test_cold_head_record_reserves_nested_data_reader_scratch);
	UT_RUN(test_cold_head_prefix_cannot_borrow_fpi_across_segment_rebirth);
	UT_RUN(test_cold_head_prefix_requires_complete_selected_identity);
	UT_RUN(test_cold_head_record_requires_installed_exact_data_before_header);
	UT_RUN(test_cold_head_record_refuses_wrong_record_or_uninstalled_data);
	UT_RUN(test_cold_head_record_io_and_postwrite_drift_never_complete);
	UT_RUN(test_cold_header_full_image_materializes_only_its_prefix);
	UT_RUN(test_cold_header_identical_record_sync_failure_is_not_success);
	UT_RUN(test_cold_header_record_keeps_prefix_and_does_not_install_future_data);
	UT_RUN(test_cold_header_record_refuses_ahead_missing_base_and_full_source_conflict);
	UT_RUN(test_cold_header_record_io_failure_keeps_source_incomplete);
	UT_RUN(test_cold_undo_block_record_rechecks_file_and_header_around_write);
	UT_RUN(test_cold_undo_block_record_idempotent_bytes_still_require_sync);
	UT_RUN(test_cold_undo_block_record_installs_only_selected_prefix);
	UT_RUN(test_cold_undo_block_record_requires_prefix_header_and_complete_source);
	UT_RUN(test_cold_undo_block_record_io_failure_never_closes_source);
	UT_RUN(test_cold_undo_block_record_requires_dedicated_record_consumer);
	UT_RUN(test_undo_record_prefix_stops_before_future_terminal_and_rebind);
	UT_RUN(test_undo_record_prefix_keeps_init_and_reuse_boundaries);
	UT_RUN(test_undo_record_prefix_never_installs_a_later_block_image);
	UT_RUN(test_undo_record_prefix_rejects_future_fpi_and_ahead_data);
	UT_RUN(test_undo_record_prefix_selector_requires_complete_identity_and_replay_role);
	UT_RUN(test_undo_record_prefix_is_not_complete_source_acceptance);
	UT_RUN(test_undo_record_prefix_refuses_ambiguous_sources_and_other_effects);
	UT_RUN(test_logical_invalidation_capture_is_bounded_and_owned);
	UT_RUN(test_cold_logical_invalidations_do_not_execute_cache_or_terminal);
	UT_RUN(test_cold_logical_invalidation_rejects_wrong_context_and_unwinds);
	UT_RUN(test_cold_record_dispatch_does_not_execute_future_controls_or_close_source);
	UT_RUN(test_cold_record_dispatch_requires_exact_record_and_original_cold_scope);
	UT_RUN(test_cold_record_dispatch_keeps_whole_source_preflight_and_error_cleanup);
	UT_RUN(test_record_plan_selector_refuses_history_pi_and_nonlocal_effects);
	UT_RUN(test_cold_record_dispatch_executes_only_selected_native_projection);
	UT_RUN(test_cold_compound_drop_classifies_the_complete_native_record);
	UT_RUN(test_cold_compound_drop_rejects_partial_or_unknown_effects);
	UT_RUN(test_cold_compound_drop_owns_all_effects_and_original_space_inputs);
	UT_RUN(test_cold_compound_drop_cannot_project_without_structural_consumer);
	UT_RUN(test_cold_selected_compound_commit_preserves_all_effect_order);
	UT_RUN(test_cold_selected_compound_commit_refusals_never_publish_terminal);
	UT_RUN(test_cold_compound_preserves_strict_tt_file_binding_across_space_and_write);
	UT_RUN(test_cold_compound_entry_refuses_unbound_record_before_effects);
	UT_RUN(test_cold_compound_capture_rejects_original_origin_or_top_frame);
	UT_RUN(test_compound_space_observation_does_not_borrow_native_execution_state);
	UT_RUN(test_initial_ctrc_release_keeps_exact_header_transition);
	UT_RUN(test_cold_initial_ctrc_release_installs_exact_durable_header);
	UT_RUN(test_cold_initial_ctrc_release_preserves_failed_io_and_scope_responsibility);
	UT_RUN(test_cold_abort_children_require_complete_supported_native_carrier);
	UT_RUN(test_cold_commit_children_require_complete_supported_native_carrier);
	UT_RUN(test_cold_lock_completion_retains_whole_native_record);
	UT_RUN(test_cold_lock_completion_uses_exact_tt_clog_and_native_owner);
	UT_RUN(test_cold_native_stats_retain_completion_owner);
	UT_RUN(test_cold_abort_delete_retains_complete_native_carrier);
	UT_RUN(test_cold_abort_delete_plan_binds_every_locator_and_owns_original_bytes);
	UT_RUN(test_cold_abort_delete_plan_refuses_missing_conflicting_or_late_source_proof);
	UT_RUN(test_cold_abort_delete_input_cannot_enter_tt_clog_or_native_completion);
	UT_RUN(test_cold_abort_completion_keeps_owned_stats_and_refuses_generic_apply);
	UT_RUN(test_cold_abort_stats_rejects_malformed_or_unowned_native_effects);
	UT_RUN(test_cold_abort_stats_keeps_exact_native_record_and_tt_carrier);
	UT_RUN(test_cold_history_abort_stats_identity_does_not_authorize_replay);
	UT_RUN(test_cold_abort_stats_source_tt_clog_and_native_owner_precede_terminal);
	UT_RUN(test_cold_completion_keeps_stats_and_force_payload_owned);
	UT_RUN(test_cold_stats_rejects_unknown_kinds_and_malformed_complete_batch);
	UT_RUN(test_cold_completion_syncs_source_and_executes_all_effects_before_terminal);
	UT_RUN(test_space_incarnation_end_requires_an_actual_later_space0_contribution);
	UT_RUN(test_space_incarnation_end_rejects_old_uuid_reappearance_and_retained_gaps);
	UT_RUN(test_space_retained_ancestry_create_and_independent_page_terminal);
	UT_RUN(test_space_retained_ancestry_covers_history_and_each_committed_drop);
	UT_RUN(test_space_retained_ancestry_rejects_broken_history_before_live_suffix);
	UT_RUN(test_space_retained_ancestry_orders_interleaved_writers_not_feed_or_token);
	UT_RUN(test_space_retained_ancestry_does_not_retire_prior_truncate_incarnation);
	UT_RUN(test_stream_census_does_not_accumulate_a_million_side_operations);
	UT_RUN(test_stream_census_checks_native_and_space_payloads_without_replay);
	UT_RUN(test_standby_retains_exact_payload_and_independent_control_obligation);
	UT_RUN(test_standby_empty_snapshot_and_initial_completed_xid_are_valid);
	UT_RUN(test_standby_invalid_counts_shapes_and_sources_never_seal);
	UT_RUN(test_space_contribution_census_includes_history_and_every_drop_page);
	UT_RUN(test_native_control_is_owned_input_and_not_replay_permission);
	UT_RUN(test_stripe_records_retain_exact_control_obligation_without_pcm_target);
	UT_RUN(test_stripe_malformed_or_wrong_source_cannot_seal);
	UT_RUN(test_native_controls_use_original_online_owner_before_any_side_effect);
	UT_RUN(test_cold_join_uses_captured_native_owner_without_full_source_completion);
	UT_RUN(test_cold_join_keeps_original_authority_retire_and_failure_boundaries);
	UT_RUN(test_cold_foreign_control_consumes_no_founder_or_transaction_effects);
	UT_RUN(test_cold_foreign_controls_keep_original_role_source_and_effect_gates);
	UT_RUN(test_cold_control_rechecks_context_after_observation_and_unwinds_error);
	UT_RUN(test_cold_assignment_shape_wrap_alignment_and_budget);
	UT_RUN(test_cold_assignment_retains_complete_bounded_original_payload);
	UT_RUN(test_cold_assignment_original_owner_gates_and_cleanup);
	UT_RUN(test_cold_standalone_cache_shape_and_unaligned_payload);
	UT_RUN(test_cold_standalone_cache_reserves_one_copy_and_keeps_pi_nonexecuting);
	UT_RUN(test_cold_standalone_cache_uses_original_owner_and_full_set_preflight);
	UT_RUN(test_cold_standalone_cache_keeps_source_and_mode_gates);
	UT_RUN(test_cold_standalone_cache_error_and_expiry_leave_no_completion);
	UT_RUN(test_native_control_error_or_expiry_releases_original_scope);
	UT_RUN(test_native_control_rejects_bad_shape_and_identity);
	UT_RUN(test_retained_generations_refuse_ambiguous_thread_selectors);
	UT_RUN(test_side_plan_does_not_drop_unowned_page_components);
	UT_RUN(test_retained_side_history_is_validated_but_never_dispatched);
	UT_RUN(test_retained_commit_drop_does_not_create_space_mutation_target);
	UT_RUN(test_multi_origin_side_selection_keeps_exact_source_and_original_plan);
	UT_RUN(test_reuse_owner_covers_retired_commit_only_after_new_physical_and_tt);
	UT_RUN(test_init_owner_repairs_short_segment_before_header_and_commit);
	UT_RUN(test_lifecycle_header_requires_known_generation_and_complete_source_chain);
	UT_RUN(test_recycle_window_repeated_terminal_and_init_unknown_generation);
	UT_RUN(test_physical_only_cut_and_reuse_anchor_boundary);
	UT_RUN(test_physical_undo_is_durable_before_canonical_tt_publication);
	UT_RUN(test_cold_owner_multixact_preflights_whole_set_before_projection);
	UT_RUN(test_cold_cache_payload_copy_is_reserved_once_at_maximum);
	UT_RUN(test_cold_cache_budget_refuses_before_header_preparation);
	UT_RUN(test_cold_cache_decode_keeps_unsupported_effects_and_bad_messages_refused);
	UT_RUN(test_cold_cache_owned_bytes_and_purpose_are_not_interchangeable);
	UT_RUN(test_cold_commit_keeps_native_cache_effects_before_terminal_projection);
	UT_RUN(test_cold_owner_native_clog_lifecycle_precedes_derived_projection);
	UT_RUN(test_cold_owner_persists_native_clog_before_commit_projection);
	UT_RUN(test_real_plan_owner_and_xact_install_final_header_before_older_commit_projection);
	UT_RUN(test_covered_commit_requires_owner_proof_before_projection_without_tt_rewind);
	UT_RUN(test_undo_header_repeated_slot_exact_source_membership_without_init);
	UT_RUN(test_undo_header_later_disk_slot_does_not_hide_conflicting_source);
	UT_RUN(test_sealed_source_match_requires_observed_namespace_and_exact_cut);
	UT_RUN(test_folded_commit_segment_must_belong_to_source_origin);
	UT_RUN(test_online_undo_header_evolves_init_bind_and_folded_commit);
	UT_RUN(test_online_undo_header_private_base_and_late_failure);
	UT_RUN(test_commit_decodes_to_immutable_truth_operation);
	UT_RUN(test_commit_tt_conflict_is_blocked_before_apply);
	UT_RUN(test_non_xact_and_missing_tt_are_blocked);
	UT_RUN(test_prepare_requires_bounded_aligned_gid);
	UT_RUN(test_prepare_apply_installs_authoritative_pending_before_projection);
	UT_RUN(test_prepare_apply_blocks_underivable_or_wrong_system_before_pending);
	UT_RUN(test_prepare_apply_blocks_wrong_origin_subxid_before_target_reads);
	UT_RUN(test_prepare_apply_never_projects_unverified_pending);
	UT_RUN(test_commit_prepared_resolves_exact_native_owner_after_tt_truth);
	UT_RUN(test_abort_prepared_resolves_exact_native_owner_after_tt_truth);
	UT_RUN(test_abort_prepared_nonempty_undo_retains_native_owner);
	UT_RUN(test_online_plan_owns_decoded_operation_not_raw_record);
	UT_RUN(test_online_plan_owns_typed_projection_records);
	UT_RUN(test_online_plan_rejects_malformed_multixact_projection);
	UT_RUN(test_online_plan_denies_incomplete_physical_cut);
	UT_RUN(test_online_plan_owns_undo_payload_not_raw_record);
	UT_RUN(test_online_plan_owns_prepare_state_not_raw_record);
	UT_RUN(test_online_plan_preflights_all_targets_before_first_mutation);
	UT_RUN(test_online_plan_denies_terminal_missing_required_tt_commit);
	UT_RUN(test_online_plan_accepts_exact_preceding_tt_commit_dependency);
	UT_RUN(test_online_plan_denies_abort_terminal_missing_tt_abort);
	UT_RUN(test_online_plan_accepts_exact_preceding_tt_abort_dependency);
	UT_RUN(test_online_plan_denies_nonempty_abort_without_undo_completion);
	UT_RUN(test_protected_set_released_on_preflight_or_apply_error);
	UT_RUN(test_space_plan_owns_exact_chain_and_requires_observed_namespace);
	UT_RUN(test_space_plan_rejects_unbound_native_shapes);
	UT_RUN(test_space_plan_structural_pair_and_standalone_tombstone);
	UT_RUN(test_space_seal_rejects_incomplete_source_chains_before_target_io);
	UT_RUN(test_space_seal_enumerates_every_unique_commit_and_smgr_target);
	UT_RUN(test_space_commit_owned_multiple_targets_close_exact_source_chains);
	UT_RUN(test_space_commit_rejects_unowned_duplicate_or_malformed_drop_inputs);
	UT_RUN(test_space_commit_retains_all_native_side_effects_and_refuses_tt_only_owner);
	UT_RUN(test_multixact_retired_page_requires_exact_later_sealed_truncate);
	UT_RUN(test_cold_abort_requires_exact_preceding_immutable_carrier);
	UT_RUN(test_cold_history_abort_identity_does_not_authorize_replay);
	UT_RUN(test_cold_owner_abort_requires_durable_tt_and_native_clog_before_projection);
	UT_RUN(test_cold_whole_source_barrier_precedes_every_native_effect);
	UT_RUN(test_clog_applied_requires_original_cold_record_success);
	UT_RUN(test_clog_applied_failed_attempt_and_error_keep_no_reusable_success);
	UT_RUN(test_clog_applied_cannot_borrow_other_plan_or_whole_source_apply);
	UT_RUN(test_clog_applied_requires_exact_identity_and_final_owner_after_cleanup);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
