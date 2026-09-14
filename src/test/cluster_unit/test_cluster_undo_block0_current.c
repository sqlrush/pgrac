/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_block0_current.c
 *	  Watched tests for the Candidate-2 cooperative block0 current guard.
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include "cluster/cluster_undo_segment.h"
#include "cluster/cluster_undo_segment_init.h"
#include "cluster/storage/cluster_undo_block0_current.h"

extern ClusterUndoBlock0Result cluster_undo_block0_current_pin_exclusive(
	ClusterUndoBlock0CurrentGuard *guard, const ClusterUndoBlock0ResolvedRoot *root,
	const ClusterUndoBlock0Generation *expected, ClusterUndoBlock0Pin *pin, char **page);
extern ClusterUndoBlock0CurrentStep cluster_undo_block0_current_acquire_begin_live_owner_source(
	const ClusterUndoBlock0LogicalKey *key, int timeout_ms,
	const ClusterSemanticAdmissionToken *admission, ClusterUndoBlock0CurrentGuard *guard,
	ClusterUndoBlock0Result *failure);
extern ClusterUndoBlock0Result
cluster_undo_block0_current_live_owner_ensure_resident(const ClusterUndoBlock0LogicalKey *key,
													   int timeout_ms);
extern ClusterUndoBlock0Result cluster_undo_block0_current_live_owner_reuse_exact(
	const ClusterUndoBlock0LogicalKey *key, const ClusterUndoBlock0Generation *expected,
	const char successor_page[BLCKSZ], int timeout_ms);
extern ClusterUndoBlock0RecycleResult cluster_undo_block0_current_live_owner_recycle_exact(
	const ClusterUndoBlock0LogicalKey *key, SCN horizon, uint64 expected_epoch, int timeout_ms);
extern ClusterUndoBlock0Result
cluster_undo_block0_current_prove_strict_empty_exclusive(ClusterUndoBlock0CurrentGuard *guard);

/*
 * Include the owner so this standalone fixture can inspect its private phase
 * and canonical key builder without widening the frozen public ABI.
 */
#include "../../backend/cluster/storage/cluster_undo_block0_current.c"

#undef printf
#undef fprintf
#undef snprintf
#undef vsnprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

static char admission_log[512];
static char wait_log[1024];
static unsigned int wait_log_count;
static bool fake_handoff_pending;

/* The real shared CTRC predicate is covered in test_cluster_ctrc_capacity;
 * this boundary controls whether its publication handoff has completed. */
bool
cluster_ctrc_reuse_handoff_complete(const UndoSegmentHeaderData *header, uint16 slot_offset)
{
	UT_ASSERT_NOT_NULL(header);
	UT_ASSERT_EQ(slot_offset, INVALID_TT_SLOT_OFFSET);
	return !fake_handoff_pending;
}
static unsigned int admission_log_count;
static unsigned int readiness_reads;

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	UT_ASSERT_EQ(elevel, LOG);
	return true;
}

int
errmsg_internal(const char *fmt, ...)
{
	va_list args;

	va_start(args, fmt);
	if (strstr(fmt, "block0 current wait:") != NULL) {
		vsnprintf(wait_log, sizeof(wait_log), fmt, args);
		wait_log_count++;
	} else {
		UT_ASSERT(strstr(fmt, "block0 current acquire admission refused:") != NULL);
		vsnprintf(admission_log, sizeof(admission_log), fmt, args);
		admission_log_count++;
	}
	va_end(args);
	return 0;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

bool cluster_enabled = true;
int cluster_node_id = 0;
static ClusterConf test_cluster_conf = { .node_count = 2 };
ClusterConf *ClusterConfShmem = &test_cluster_conf;
int cluster_ges_request_timeout_ms = 1000;
int cluster_ges_retransmit_max_attempts = 5;
struct PGPROC *MyProc = NULL;
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;
volatile sig_atomic_t InterruptPending = false;

int
cluster_conf_node_count(void)
{
	return test_cluster_conf.node_count;
}

static TimestampTz fake_now;
static uint64 fake_epoch;
static int32 fake_master;
static uint64 fake_routing_generation;
static ClusterGrdShardPhase fake_shard_phase;
static bool fake_lms_ready;
static ClusterLmonStatus fake_lmon_status;
static bool fake_quorum;
static bool fake_member;
static ClusterSemanticAdmissionResult fake_admission_result;
static ClusterSemanticAdmissionSide fake_modifier_side;
static bool fake_admission_recheck;
static bool fake_post_pin_recheck_throws;
static ClusterGrdEntryResult fake_reserve_result;
static ClusterGrdGrantAction fake_grant_action;
static ClusterGrdEntryResult fake_promote_result;
static GesReplyWaitPollResult fake_poll_result;
static GesReplyWaitVerdict fake_poll_verdict;
static bool fake_poll_throws;
static bool fake_reply_insert_ok;
static bool fake_outbound_ok;
static bool fake_abandon_raced_grant;
static ClusterGesTimeoutSrc fake_timeout_source;
static ClusterUndoBlock0Result fake_sample_result;
static ClusterUndoBlock0Generation fake_sample_generation;
static bool fake_sample_invalidates_authority;
static bool fake_sample_throws;
static ClusterUndoBlock0Result fake_copy_result;
static ClusterUndoBlock0Generation fake_copy_generation;
static bool fake_copy_invalidates_authority;
static bool fake_copy_throws;
static ClusterUndoBlock0Result fake_pin_result;
static bool fake_pin_invalidates_authority;
static bool fake_pin_throws;
static ClusterUndoBlock0Result fake_empty_result;
static bool fake_empty_invalidates_authority;
static char fake_pin_page[BLCKSZ];
static char fake_disk_page[BLCKSZ];
static bool fake_smgr_read_ok;
static ClusterUndoBlock0ResolvedRoot fake_resolved_root;
static int fake_root_resolve_success_limit;
static ClusterUndoBlock0Result fake_frame_result;
static ClusterUndoBlock0Result fake_provision_result;
static ClusterUndoBlock0Generation fake_provision_generation;
static bool fake_provision_creator;
static pg_on_exit_callback fake_exit_callback;
static bool fake_exit_lifo_ok;
static int smgr_exit_hook_ensure_calls;

typedef struct FakeBeforeExitEntry {
	pg_on_exit_callback function;
	Datum arg;
} FakeBeforeExitEntry;

static FakeBeforeExitEntry fake_before_exit_stack[8];
static int fake_before_exit_count;
static bool fake_smgr_exit_registered;

static int event_sequence;
static int reserve_event;
static int reserve_calls;
static int insert_event;
static int outbound_event;
static int reply_delete_calls;
static int reply_poll_calls;
static int reply_sleep_calls;
static ClusterUndoBlock0ReplyWaitStats reply_wait_stats;

void
cluster_undo_block0_reply_wait_metric_add(ClusterUndoBlock0ReplyWaitSite site,
										  ClusterUndoBlock0ReplyWaitMetric metric)
{
	UT_ASSERT((unsigned)site < CLUSTER_UNDO_BLOCK0_WAIT_SITE_COUNT);
	UT_ASSERT((unsigned)metric < CLUSTER_UNDO_BLOCK0_WAIT_METRIC_COUNT);
	if ((unsigned)site < CLUSTER_UNDO_BLOCK0_WAIT_SITE_COUNT
		&& (unsigned)metric < CLUSTER_UNDO_BLOCK0_WAIT_METRIC_COUNT)
		reply_wait_stats.count[site][metric]++;
}
static int fake_reply_wait_held_tranche;
static int reservation_cancel_calls;
static int waiter_cancel_calls;
static int local_release_calls;
static int mirror_release_calls;
static int generic_promote_calls;
static int remote_promote_calls;
static int cancel_wait_calls;
static int cleanup_release_calls;
static int semantic_enter_calls;
static int semantic_ordinary_recheck_calls;
static int semantic_census_recheck_calls;
static int semantic_leave_calls;
static int sample_calls;
static int copy_calls;
static int pin_calls;
static int unpin_calls;
static int empty_calls;
static int root_resolve_calls;
static int frame_reserve_calls;
static int frame_release_calls;
static int provision_calls;
static int provision_abort_calls;
static int flush_sync_calls;
static char last_flush_successor[BLCKSZ];
static XLogRecPtr last_flush_lsn;
static int reuse_wal_calls;
static int recycle_wal_calls;
static XLogRecPtr fake_reuse_lsn;
static XLogRecPtr fake_recycle_lsn;
static int semantic_enter_event;
static int current_exit_hook_event;
static int smgr_exit_hook_event;
static int first_root_resolve_event;
static int final_root_resolve_event;
static int sample_event;
static int frame_reserve_event;
static int provision_event;
static int provision_abort_event;
static int local_release_event;
static int mirror_release_event;
static int semantic_leave_event;
static int pin_error_local_cleanup_event;
static int unpin_event;
static int cleanup_release_event;
static ClusterSemanticAdmissionSide last_semantic_enter_side;
static int last_outbound_len;
static GesRequestPayload last_outbound;
static GesRequestPayload last_cleanup_release;
static GesReplyWaitKey last_insert_key;
static GesReplyWaitKey last_poll_key;
static GesReplyWaitKey last_sleep_key;
static long last_sleep_timeout_ms;
static uint32 last_sleep_wait_event;
static ClusterGrdHolderId last_cancel_holder;
static ClusterResId last_cancel_resid;
static uint64 last_cancel_wait_seq;
static ClusterUndoBlock0LogicalKey last_pin_logical;
static ClusterUndoBlock0ResolvedRoot last_pin_root;
static ClusterUndoBlock0Generation last_pin_expected;
static ClusterUndoBlock0Mode last_pin_mode;
static ClusterUndoBlock0AuthorityProof last_pin_proof;
static ClusterUndoBlock0Pin *last_pin_handle;
static ClusterUndoBlock0Pin *last_unpin_handle;
static ClusterUndoBlock0LogicalKey last_empty_logical;
static ClusterUndoBlock0AuthorityProof last_empty_proof;

void
ExceptionalCondition(const char *condition_name pg_attribute_unused(),
					 const char *file_name pg_attribute_unused(),
					 int line_number pg_attribute_unused())
{
	abort();
}

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

void
ProcessInterrupts(void)
{}

TimestampTz
GetCurrentTimestamp(void)
{
	return fake_now;
}

static void
fake_smgr_exit(int code pg_attribute_unused(), Datum arg pg_attribute_unused())
{}

void
before_shmem_exit(pg_on_exit_callback function, Datum arg)
{
	if (fake_before_exit_count >= lengthof(fake_before_exit_stack))
		abort();
	fake_before_exit_stack[fake_before_exit_count].function = function;
	fake_before_exit_stack[fake_before_exit_count].arg = arg;
	fake_before_exit_count++;
	if (function == current_backend_exit) {
		fake_exit_callback = function;
		current_exit_hook_event = ++event_sequence;
	} else if (function == fake_smgr_exit) {
		smgr_exit_hook_event = ++event_sequence;
	}
}

void
cancel_before_shmem_exit(pg_on_exit_callback function, Datum arg)
{
	int i;

	if (fake_before_exit_count > 0
		&& fake_before_exit_stack[fake_before_exit_count - 1].function == function
		&& fake_before_exit_stack[fake_before_exit_count - 1].arg == arg) {
		fake_before_exit_count--;
		return;
	}
	fake_exit_lifo_ok = false;
	for (i = fake_before_exit_count - 1; i >= 0; i--) {
		if (fake_before_exit_stack[i].function == function
			&& fake_before_exit_stack[i].arg == arg) {
			memmove(&fake_before_exit_stack[i], &fake_before_exit_stack[i + 1],
					(size_t)(fake_before_exit_count - i - 1) * sizeof(fake_before_exit_stack[0]));
			fake_before_exit_count--;
			return;
		}
	}
}

void
cluster_undo_smgr_ensure_exit_hook(void)
{
	smgr_exit_hook_ensure_calls++;
	if (!fake_smgr_exit_registered) {
		before_shmem_exit(fake_smgr_exit, (Datum)0);
		fake_smgr_exit_registered = true;
	}
}

bool
cluster_lms_is_ready(void)
{
	readiness_reads |= 1;
	return fake_lms_ready;
}

ClusterLmonStatus
cluster_lmon_status(void)
{
	readiness_reads |= 2;
	return fake_lmon_status;
}

bool
cluster_qvotec_in_quorum(void)
{
	readiness_reads |= 4;
	return fake_quorum;
}

bool
cluster_membership_is_member(int32 node_id)
{
	readiness_reads |= 8;
	return fake_member && node_id == cluster_node_id;
}

uint64
cluster_epoch_get_current(void)
{
	return fake_epoch;
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_enter(uint64 feature_bit, ClusterSemanticAdmissionSide side,
								  ClusterSemanticAdmissionToken *token)
{
	semantic_enter_calls++;
	semantic_enter_event = ++event_sequence;
	last_semantic_enter_side = side;
	memset(token, 0, sizeof(*token));
	if (fake_admission_result == CLUSTER_SEMANTIC_ADMISSION_OK) {
		token->feature_bit = feature_bit;
		token->formation_epoch = fake_epoch;
		token->side = side;
		token->entered = true;
	}
	return fake_admission_result;
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_modifier_enter(bool writable_admission,
										   ClusterSemanticAdmissionToken *token)
{
	if (!writable_admission)
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
	return cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
											 fake_modifier_side, token);
}

bool
cluster_semantic_activation_recheck(const ClusterSemanticAdmissionToken *token)
{
	semantic_ordinary_recheck_calls++;
	if (fake_post_pin_recheck_throws && pin_calls > 0) {
		UT_ASSERT_NOT_NULL(PG_exception_stack);
		siglongjmp(*PG_exception_stack, 1);
	}
	return fake_admission_recheck && token != NULL && token->entered
		   && token->formation_epoch == fake_epoch;
}

bool
cluster_semantic_activation_modifier_recheck(const ClusterSemanticAdmissionToken *token,
											 bool writable_admission)
{
	return writable_admission && cluster_semantic_activation_recheck(token);
}

bool
cluster_semantic_activation_recheck_r4_terminal_census(const ClusterSemanticAdmissionToken *token)
{
	semantic_census_recheck_calls++;
	return fake_admission_recheck && token != NULL && token->entered
		   && token->formation_epoch == fake_epoch;
}

void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	semantic_leave_calls++;
	semantic_leave_event = ++event_sequence;
	if (token != NULL)
		token->entered = false;
}

bool
cluster_semantic_activation_resolve_shared_undo_root_live_owner_source(
	const ClusterSemanticAdmissionToken *token, ClusterUndoPathIntent intent, uint32 owner_instance,
	uint32 segment_id, ClusterUndoBlock0ResolvedRoot *out)
{
	root_resolve_calls++;
	if (root_resolve_calls == 1)
		first_root_resolve_event = ++event_sequence;
	else
		final_root_resolve_event = ++event_sequence;
	if (fake_root_resolve_success_limit >= 0
		&& root_resolve_calls > fake_root_resolve_success_limit)
		return false;
	if (token == NULL || !token->entered || token->side != CLUSTER_SEMANTIC_SOURCE_SIDE
		|| intent != CLUSTER_UNDO_PATH_RUNTIME_SHARED
		|| owner_instance != (uint32)cluster_node_id + 1
		|| segment_id != (owner_instance - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1 || out == NULL)
		return false;
	*out = fake_resolved_root;
	return true;
}

bool
cluster_semantic_activation_resolve_shared_undo_root(const ClusterSemanticAdmissionToken *token,
													 ClusterUndoPathIntent intent,
													 uint32 owner_instance, uint32 segment_id,
													 ClusterUndoBlock0ResolvedRoot *out)
{
	root_resolve_calls++;
	if (root_resolve_calls == 1)
		first_root_resolve_event = ++event_sequence;
	else
		final_root_resolve_event = ++event_sequence;
	if (fake_root_resolve_success_limit >= 0
		&& root_resolve_calls > fake_root_resolve_success_limit)
		return false;
	if (token == NULL || !token->entered || token->side != CLUSTER_SEMANTIC_TARGET_SIDE
		|| intent != CLUSTER_UNDO_PATH_RUNTIME_SHARED
		|| owner_instance != (uint32)cluster_node_id + 1
		|| segment_id != (owner_instance - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1 || out == NULL)
		return false;
	*out = fake_resolved_root;
	return true;
}

ClusterUndoBlock0Result
cluster_undo_block0_logical_slot(const ClusterUndoBlock0LogicalKey *logical, uint32 *slot)
{
	uint32 first;

	if (logical == NULL || slot == NULL || logical->owner_instance == 0
		|| logical->owner_instance > UNDO_OWNER_INSTANCE_MAX)
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	first = ((uint32)logical->owner_instance - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	if (logical->segment_id < first
		|| logical->segment_id >= first + CLUSTER_UNDO_SEGS_PER_INSTANCE)
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	*slot = logical->segment_id - 1;
	return CLUSTER_UNDO_BLOCK0_OK;
}

bool
cluster_undo_block0_root_matches(const ClusterUndoBlock0ResolvedRoot *observed,
								 const ClusterUndoBlock0ResolvedRoot *expected)
{
	return observed != NULL && expected != NULL && observed->intent == expected->intent
		   && observed->root_id == expected->root_id
		   && observed->root_generation == expected->root_generation;
}

bool
cluster_undo_block0_generation_matches(const ClusterUndoBlock0Generation *observed,
									   const ClusterUndoBlock0Generation *expected)
{
	if (observed == NULL || expected == NULL)
		return false;
	return !expected->known || (observed->known && observed->value == expected->value);
}

int32
cluster_grd_lookup_master_gen(const ClusterResId *resid pg_attribute_unused(),
							  uint64 *out_routing_generation)
{
	*out_routing_generation = fake_routing_generation;
	return fake_master;
}

uint32
cluster_grd_shard_for_resource(const ClusterResId *resid pg_attribute_unused())
{
	return 17;
}

ClusterGrdShardPhase
cluster_grd_shard_phase(uint32 shard_id)
{
	UT_ASSERT_EQ(shard_id, 17);
	return fake_shard_phase;
}

uint64
cluster_ges_reply_wait_next_request_id(void)
{
	static uint64 next_request_id = 40;

	return ++next_request_id;
}

ClusterGrdEntryResult
cluster_grd_try_reserve(const ClusterResId *resid pg_attribute_unused(),
						const ClusterGrdHolderId *holder pg_attribute_unused(), int mode,
						int32 self_node_id, bool *fast_path_out, uint64 *gen_snapshot_out)
{
	reserve_calls++;
	reserve_event = ++event_sequence;
	UT_ASSERT(mode == ShareLock || mode == ExclusiveLock);
	UT_ASSERT_EQ(self_node_id, cluster_node_id);
	if (fast_path_out != NULL)
		*fast_path_out = fake_master == cluster_node_id;
	if (gen_snapshot_out != NULL)
		*gen_snapshot_out = 77;
	return fake_reserve_result;
}

GesReplyWaitEntry *
cluster_ges_reply_wait_insert(const GesReplyWaitKey *key,
							  TimestampTz deadline pg_attribute_unused())
{
	static GesReplyWaitEntry entry;

	insert_event = ++event_sequence;
	last_insert_key = *key;
	return fake_reply_insert_ok ? &entry : NULL;
}

void
cluster_ges_reply_wait_delete(const GesReplyWaitKey *key pg_attribute_unused())
{
	reply_delete_calls++;
}

GesReplyWaitPollResult
cluster_ges_reply_wait_poll_consume(const GesReplyWaitKey *key, GesReplyWaitVerdict *verdict_out)
{
	reply_poll_calls++;
	last_poll_key = *key;
	if (fake_poll_throws) {
		UT_ASSERT_NOT_NULL(PG_exception_stack);
		siglongjmp(*PG_exception_stack, 1);
	}
	if (fake_poll_result == GES_REPLY_WAIT_POLL_DELIVERED)
		*verdict_out = fake_poll_verdict;
	return fake_poll_result;
}

bool
cluster_ges_reply_wait_sleep_exact(const GesReplyWaitKey *key, long timeout_ms, uint32 wait_event)
{
	reply_sleep_calls++;
	last_sleep_key = *key;
	last_sleep_timeout_ms = timeout_ms;
	last_sleep_wait_event = wait_event;
	return true;
}

void
ForEachLWLockHeldByMe(void (*callback)(LWLock *, LWLockMode, void *), void *context)
{
	if (fake_reply_wait_held_tranche != 0) {
		LWLock held = { 0 };

		held.tranche = fake_reply_wait_held_tranche;
		callback(&held, LW_EXCLUSIVE, context);
	}
}

bool
cluster_ges_reply_wait_mark_abandoned(const GesReplyWaitKey *key pg_attribute_unused(),
									  TimestampTz tombstone_deadline pg_attribute_unused())
{
	return fake_abandon_raced_grant;
}

bool
cluster_grd_outbound_enqueue_backend_request(uint32 dest_node_id, const void *payload,
											 uint16 payload_len)
{
	outbound_event = ++event_sequence;
	UT_ASSERT_EQ(dest_node_id, fake_master);
	last_outbound_len = payload_len;
	memcpy(&last_outbound, payload, Min((Size)payload_len, sizeof(last_outbound)));
	return fake_outbound_ok;
}

void
cluster_ges_timeout_detail_reset(void)
{
	fake_timeout_source = CLUSTER_GES_TSRC_NONE;
}

void
cluster_ges_timeout_detail_set(ClusterGesTimeoutSrc src, int32 master_node pg_attribute_unused(),
							   long elapsed_ms pg_attribute_unused(),
							   int attempts pg_attribute_unused(),
							   int conflict_holders pg_attribute_unused(),
							   int timeout_ms pg_attribute_unused())
{
	fake_timeout_source = src;
}

void
cluster_grd_outbound_enqueue_cleanup_release(uint32 dest_node_id, const void *payload,
											 uint16 payload_len)
{
	cleanup_release_calls++;
	cleanup_release_event = ++event_sequence;
	UT_ASSERT_EQ(dest_node_id, fake_master);
	UT_ASSERT_EQ(payload_len, sizeof(GesRequestPayload));
	memcpy(&last_cleanup_release, payload, sizeof(last_cleanup_release));
}

ClusterGrdGrantAction
cluster_grd_entry_enqueue_or_grant(
	const ClusterResId *resid pg_attribute_unused(),
	const ClusterGrdHolderId *holder pg_attribute_unused(),
	int32 source_node_id pg_attribute_unused(), uint64 request_id pg_attribute_unused(),
	uint64 shard_master_generation pg_attribute_unused(),
	uint32 request_opcode pg_attribute_unused(), int lockmode,
	ClusterGrdConflictHolder *conflict_holders_out pg_attribute_unused(), int *n_conflict_out)
{
	UT_ASSERT(insert_event != 0);
	UT_ASSERT(lockmode == ShareLock || lockmode == ExclusiveLock);
	*n_conflict_out = 0;
	return fake_grant_action;
}

void
cluster_ges_send_bast_targeted(const ClusterResId *resid pg_attribute_unused(),
							   int requested_mode pg_attribute_unused(),
							   const ClusterGrdConflictHolder *holders pg_attribute_unused(),
							   int n_holders pg_attribute_unused())
{}

ClusterGrdEntryResult
cluster_grd_revalidate_and_promote(const ClusterResId *resid pg_attribute_unused(),
								   const ClusterGrdHolderId *holder pg_attribute_unused(),
								   int32 self_node_id pg_attribute_unused(), uint64 gen_snapshot)
{
	generic_promote_calls++;
	UT_ASSERT_EQ(gen_snapshot, 77);
	return fake_promote_result;
}

ClusterGrdEntryResult
cluster_grd_promote_remote_grant_exact(const ClusterResId *resid pg_attribute_unused(),
									   const ClusterGrdHolderId *holder pg_attribute_unused())
{
	remote_promote_calls++;
	return fake_promote_result;
}

ClusterGrdEntryResult
cluster_grd_cancel_reservation_by_id(const ClusterResId *resid pg_attribute_unused(),
									 const ClusterGrdHolderId *holder pg_attribute_unused())
{
	reservation_cancel_calls++;
	return CLUSTER_GRD_ENTRY_OK;
}

ClusterGrdEntryResult
cluster_grd_cancel_waiter_by_id_seq(const ClusterResId *resid, const ClusterGrdHolderId *holder,
									uint64 wait_seq)
{
	waiter_cancel_calls++;
	last_cancel_resid = *resid;
	last_cancel_holder = *holder;
	last_cancel_wait_seq = wait_seq;
	return CLUSTER_GRD_ENTRY_OK;
}

uint32
cluster_ges_release_and_drain_local(const ClusterResId *resid pg_attribute_unused(),
									const ClusterGrdHolderId *holder pg_attribute_unused())
{
	local_release_calls++;
	local_release_event = ++event_sequence;
	return GES_REJECT_REASON_NONE;
}

ClusterGrdEntryResult
cluster_grd_release_holder_by_id(const ClusterResId *resid pg_attribute_unused(),
								 const ClusterGrdHolderId *holder pg_attribute_unused())
{
	mirror_release_calls++;
	mirror_release_event = ++event_sequence;
	return CLUSTER_GRD_ENTRY_OK;
}

void
cluster_ges_send_cancel_wait(int32 master_node_id, const ClusterResId *resid,
							 const ClusterGrdHolderId *waiter, uint64 wait_seq, uint64 cancel_id,
							 uint8 kind)
{
	cancel_wait_calls++;
	UT_ASSERT_EQ(master_node_id, fake_master);
	UT_ASSERT_EQ(cancel_id, 0);
	UT_ASSERT_EQ(kind, GES_CANCEL_WAIT_KIND_REQUEST);
	last_cancel_resid = *resid;
	last_cancel_holder = *waiter;
	last_cancel_wait_seq = wait_seq;
}

ClusterUndoBlock0Result
cluster_undo_block0_sample_resident_generation(
	const ClusterUndoBlock0LogicalKey *logical pg_attribute_unused(),
	const ClusterUndoBlock0ResolvedRoot *expected_root pg_attribute_unused(),
	const ClusterUndoBlock0AuthorityProof *proof, ClusterUndoBlock0Generation *observed_generation)
{
	sample_calls++;
	sample_event = ++event_sequence;
	UT_ASSERT_EQ(proof->kind, CLUSTER_UNDO_BLOCK0_LIVE_OWNER);
	UT_ASSERT_EQ(proof->cluster_epoch, fake_epoch);
	if (fake_sample_throws) {
		UT_ASSERT_NOT_NULL(PG_exception_stack);
		siglongjmp(*PG_exception_stack, 1);
	}
	if (fake_sample_result == CLUSTER_UNDO_BLOCK0_OK)
		*observed_generation = fake_sample_generation;
	if (fake_sample_invalidates_authority)
		fake_admission_recheck = false;
	return fake_sample_result;
}

ClusterUndoBlock0Result
cluster_undo_block0_sample_resident_generation_conditional(
	const ClusterUndoBlock0LogicalKey *logical, const ClusterUndoBlock0ResolvedRoot *expected_root,
	const ClusterUndoBlock0AuthorityProof *proof, ClusterUndoBlock0Generation *observed_generation)
{
	return cluster_undo_block0_sample_resident_generation(logical, expected_root, proof,
														  observed_generation);
}

ClusterUndoBlock0Result
cluster_undo_block0_copy_resident(
	const ClusterUndoBlock0LogicalKey *logical pg_attribute_unused(),
	const ClusterUndoBlock0ResolvedRoot *expected_root pg_attribute_unused(),
	const ClusterUndoBlock0Generation *expected pg_attribute_unused(),
	const ClusterUndoBlock0AuthorityProof *proof, char private_page[BLCKSZ],
	ClusterUndoBlock0Generation *observed_generation)
{
	copy_calls++;
	UT_ASSERT_EQ(proof->kind, CLUSTER_UNDO_BLOCK0_LIVE_OWNER);
	if (fake_copy_throws) {
		UT_ASSERT_NOT_NULL(PG_exception_stack);
		siglongjmp(*PG_exception_stack, 1);
	}
	if (fake_copy_result == CLUSTER_UNDO_BLOCK0_OK) {
		memset(private_page, 0x5a, BLCKSZ);
		*observed_generation = fake_copy_generation;
	}
	if (fake_copy_invalidates_authority)
		fake_admission_recheck = false;
	return fake_copy_result;
}

ClusterUndoBlock0Result
cluster_undo_block0_pin(const ClusterUndoBlock0LogicalKey *logical,
						const ClusterUndoBlock0ResolvedRoot *expected_root,
						const ClusterUndoBlock0Generation *expected, ClusterUndoBlock0Mode mode,
						const ClusterUndoBlock0AuthorityProof *proof, ClusterUndoBlock0Pin *pin,
						char **page)
{
	pin_calls++;
	last_pin_handle = pin;
	last_pin_logical = *logical;
	last_pin_root = *expected_root;
	last_pin_expected = *expected;
	last_pin_mode = mode;
	last_pin_proof = *proof;
	if (fake_pin_throws) {
		/* The real resident primitive must unwind its local reservation first. */
		pin_error_local_cleanup_event = ++event_sequence;
		UT_ASSERT_NOT_NULL(PG_exception_stack);
		siglongjmp(*PG_exception_stack, 1);
	}
	if (fake_pin_result == CLUSTER_UNDO_BLOCK0_OK) {
		memset(pin, 0, sizeof(*pin));
		pin->slot = 7;
		pin->logical = *logical;
		pin->resolved_root = *expected_root;
		pin->observed_generation = *expected;
		pin->mode = mode;
		pin->proof = *proof;
		*page = fake_pin_page;
	}
	if (fake_pin_invalidates_authority)
		fake_admission_recheck = false;
	return fake_pin_result;
}

bool
cluster_undo_block0_generation_advance(const ClusterUndoBlock0Generation *current,
									   ClusterUndoBlock0Generation *next)
{
	if (current == NULL || next == NULL || !current->known || current->value == UINT32_MAX)
		return false;
	next->known = true;
	next->value = current->value + 1;
	return true;
}

bool
cluster_undo_smgr_read_block(ClusterUndoPathIntent intent pg_attribute_unused(),
							 uint32 segment_id pg_attribute_unused(),
							 uint8 owner_instance pg_attribute_unused(), BlockNumber block_no,
							 char *buf)
{
	UT_ASSERT_EQ(block_no, 0);
	if (!fake_smgr_read_ok)
		return false;
	memcpy(buf, fake_disk_page, BLCKSZ);
	return true;
}

void
cluster_undo_block0_flush_sync(ClusterUndoBlock0Pin *pin, const char *successor_page,
							   XLogRecPtr required_wal_lsn, bool fsync_parent)
{
	UT_ASSERT_NOT_NULL(pin);
	UT_ASSERT_NOT_NULL(successor_page);
	UT_ASSERT(!fsync_parent);
	flush_sync_calls++;
	last_flush_lsn = required_wal_lsn;
	memcpy(last_flush_successor, successor_page, BLCKSZ);
	memcpy(fake_pin_page, successor_page, BLCKSZ);
	memcpy(fake_disk_page, successor_page, BLCKSZ);
}

XLogRecPtr
cluster_undo_emit_segment_reuse(uint8 instance pg_attribute_unused(),
								uint32 segment_id pg_attribute_unused(),
								uint32 old_generation pg_attribute_unused(),
								uint32 new_generation pg_attribute_unused(),
								const char page_image[BLCKSZ] pg_attribute_unused())
{
	reuse_wal_calls++;
	return fake_reuse_lsn;
}

XLogRecPtr
cluster_undo_emit_segment_recycle(uint8 instance pg_attribute_unused(),
								  uint32 segment_id pg_attribute_unused(),
								  uint32 generation pg_attribute_unused(),
								  uint8 old_state pg_attribute_unused(),
								  uint8 new_state pg_attribute_unused())
{
	recycle_wal_calls++;
	return fake_recycle_lsn;
}

bool
cluster_undo_segment_recyclable_for_mode(const struct UndoSegmentHeaderData *header, SCN horizon,
										 bool peer_mode)
{
	int i;

	if (header == NULL || header->segment_state != SEGMENT_COMMITTED)
		return false;
	if (!peer_mode)
		return true;
	for (i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		const TTSlot *slot = &header->tt_slots[i];

		if (slot->status == TT_SLOT_COMMITTED) {
			if (!TransactionIdIsNormal(slot->xid) || slot->flags != TT_SLOT_FLAG_CTRC_RELEASE_PROVEN
				|| !SCN_VALID(slot->commit_scn) || !SCN_VALID(horizon)
				|| scn_time_cmp(slot->commit_scn, horizon) > 0)
				return false;
		} else if (slot->status == TT_SLOT_ABORTED) {
			if (!TransactionIdIsNormal(slot->xid) || slot->flags != TT_SLOT_FLAG_CTRC_RELEASE_PROVEN
				|| slot->commit_scn != InvalidScn)
				return false;
		} else if ((slot->status == TT_SLOT_UNUSED || slot->status == TT_SLOT_RECYCLABLE)
				   && slot->flags != TT_FLAGS_RESERVED)
			return false;
		else if (slot->status != TT_SLOT_UNUSED && slot->status != TT_SLOT_RECYCLABLE)
			return false;
	}
	return true;
}

int
scn_time_cmp(SCN a, SCN b)
{
	uint64 la = scn_local(a);
	uint64 lb = scn_local(b);

	return la < lb ? -1 : la > lb ? 1 : 0;
}

ClusterUndoBlock0Result
cluster_undo_block0_prove_strict_empty(const ClusterUndoBlock0LogicalKey *logical,
									   const ClusterUndoBlock0AuthorityProof *proof)
{
	empty_calls++;
	last_empty_logical = *logical;
	last_empty_proof = *proof;
	if (fake_empty_invalidates_authority)
		fake_admission_recheck = false;
	return fake_empty_result;
}

void
cluster_undo_block0_unpin(ClusterUndoBlock0Pin *pin)
{
	unpin_calls++;
	last_unpin_handle = pin;
	unpin_event = ++event_sequence;
	if (pin != NULL)
		pin->slot = -1;
}

ClusterUndoBlock0Result
cluster_undo_block0_frame_reserve_batch(uint32 count, ClusterUndoBlock0FrameToken *tokens)
{
	frame_reserve_calls++;
	frame_reserve_event = ++event_sequence;
	UT_ASSERT_EQ(count, 1);
	UT_ASSERT_NOT_NULL(tokens);
	tokens[0].frame_index = UINT32_MAX;
	tokens[0].owned = false;
	if (fake_frame_result == CLUSTER_UNDO_BLOCK0_OK) {
		tokens[0].frame_index = 3;
		tokens[0].owned = true;
	}
	return fake_frame_result;
}

void
cluster_undo_block0_frame_release(ClusterUndoBlock0FrameToken *token)
{
	frame_release_calls++;
	if (token != NULL) {
		token->frame_index = UINT32_MAX;
		token->owned = false;
	}
}

ClusterUndoBlock0Result
cluster_undo_block0_provision_begin(const ClusterUndoBlock0LogicalKey *logical,
									const ClusterUndoBlock0ResolvedRoot *target_root,
									const ClusterUndoBlock0AuthorityProof *proof,
									ClusterUndoBlock0FrameToken *token, ClusterUndoBlock0Pin *pin,
									char **unpublished_page, bool *creator)
{
	provision_calls++;
	provision_event = ++event_sequence;
	UT_ASSERT_NOT_NULL(logical);
	UT_ASSERT_NOT_NULL(target_root);
	UT_ASSERT_NOT_NULL(proof);
	UT_ASSERT_NOT_NULL(token);
	UT_ASSERT(token->owned);
	UT_ASSERT_EQ(proof->kind, CLUSTER_UNDO_BLOCK0_LIVE_OWNER);
	UT_ASSERT_EQ(proof->owner_instance, logical->owner_instance);
	if (fake_provision_result != CLUSTER_UNDO_BLOCK0_OK)
		return fake_provision_result;
	token->frame_index = UINT32_MAX;
	token->owned = false;
	memset(pin, 0, sizeof(*pin));
	pin->slot = 7;
	pin->logical = *logical;
	pin->resolved_root = *target_root;
	pin->observed_generation = fake_provision_creator ? (ClusterUndoBlock0Generation){ false, 0 }
													  : fake_provision_generation;
	pin->mode = CLUSTER_UNDO_BLOCK0_EXCLUSIVE;
	pin->proof = *proof;
	*unpublished_page = fake_pin_page;
	*creator = fake_provision_creator;
	return CLUSTER_UNDO_BLOCK0_OK;
}

void
cluster_undo_block0_provision_abort(ClusterUndoBlock0Pin *pin)
{
	provision_abort_calls++;
	provision_abort_event = ++event_sequence;
	if (pin != NULL)
		pin->slot = -1;
}

static ClusterUndoBlock0LogicalKey
test_key(uint8 owner_instance)
{
	ClusterUndoBlock0LogicalKey key;

	memset(&key, 0, sizeof(key));
	key.owner_instance = owner_instance;
	key.segment_id = ((uint32)owner_instance - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	return key;
}

static ClusterUndoBlock0ResolvedRoot
test_root(void)
{
	ClusterUndoBlock0ResolvedRoot root;

	memset(&root, 0, sizeof(root));
	root.intent = CLUSTER_UNDO_PATH_RUNTIME_SHARED;
	root.root_id = 91;
	root.root_generation = 7;
	return root;
}

static void
reset_fixture(void)
{
	cluster_enabled = true;
	cluster_node_id = 0;
	readiness_reads = admission_log_count = 0;
	wait_log_count = 0;
	wait_log[0] = '\0';
	admission_log[0] = '\0';
	fake_now = UINT64_C(1000000);
	fake_epoch = 9;
	fake_master = 2;
	fake_routing_generation = UINT64_C(0x0000000900000004);
	fake_shard_phase = GRD_SHARD_NORMAL;
	fake_lms_ready = true;
	fake_lmon_status = CLUSTER_LMON_READY;
	fake_quorum = true;
	fake_member = true;
	fake_admission_result = CLUSTER_SEMANTIC_ADMISSION_OK;
	fake_modifier_side = CLUSTER_SEMANTIC_SOURCE_SIDE;
	fake_admission_recheck = true;
	fake_post_pin_recheck_throws = false;
	fake_reserve_result = CLUSTER_GRD_ENTRY_OK;
	fake_grant_action = CLUSTER_GRD_ENQUEUED_WAITER;
	fake_promote_result = CLUSTER_GRD_ENTRY_OK;
	fake_poll_result = GES_REPLY_WAIT_POLL_PENDING;
	fake_poll_verdict = (GesReplyWaitVerdict){ GES_REPLY_OPCODE_GRANT, GES_REJECT_REASON_NONE };
	fake_poll_throws = false;
	fake_reply_insert_ok = true;
	fake_outbound_ok = true;
	fake_abandon_raced_grant = false;
	fake_timeout_source = CLUSTER_GES_TSRC_NONE;
	fake_sample_result = CLUSTER_UNDO_BLOCK0_OK;
	fake_sample_generation = (ClusterUndoBlock0Generation){ true, 0 };
	fake_sample_invalidates_authority = false;
	fake_sample_throws = false;
	fake_copy_result = CLUSTER_UNDO_BLOCK0_OK;
	fake_copy_generation = (ClusterUndoBlock0Generation){ true, 0 };
	fake_copy_invalidates_authority = false;
	fake_copy_throws = false;
	fake_pin_result = CLUSTER_UNDO_BLOCK0_OK;
	fake_pin_invalidates_authority = false;
	fake_pin_throws = false;
	fake_empty_result = CLUSTER_UNDO_BLOCK0_OK;
	fake_empty_invalidates_authority = false;
	fake_smgr_read_ok = true;
	fake_resolved_root = test_root();
	fake_root_resolve_success_limit = -1;
	fake_frame_result = CLUSTER_UNDO_BLOCK0_OK;
	fake_provision_result = CLUSTER_UNDO_BLOCK0_OK;
	fake_provision_generation = (ClusterUndoBlock0Generation){ true, 4 };
	fake_provision_creator = false;
	memset(fake_pin_page, 0x6b, sizeof(fake_pin_page));
	memset(fake_disk_page, 0x6b, sizeof(fake_disk_page));
	fake_exit_callback = current_exit_hook_registered ? current_backend_exit : NULL;
	fake_exit_lifo_ok = true;
	smgr_exit_hook_ensure_calls = 0;
	event_sequence = reserve_event = insert_event = outbound_event = 0;
	reserve_calls = 0;
	reply_delete_calls = reply_poll_calls = reply_sleep_calls = 0;
	memset(&reply_wait_stats, 0, sizeof(reply_wait_stats));
	fake_reply_wait_held_tranche = 0;
	reservation_cancel_calls = waiter_cancel_calls = local_release_calls = 0;
	mirror_release_calls = cancel_wait_calls = cleanup_release_calls = 0;
	generic_promote_calls = remote_promote_calls = 0;
	semantic_enter_calls = semantic_ordinary_recheck_calls = 0;
	semantic_census_recheck_calls = semantic_leave_calls = 0;
	sample_calls = copy_calls = 0;
	pin_calls = unpin_calls = empty_calls = 0;
	root_resolve_calls = frame_reserve_calls = frame_release_calls = 0;
	provision_calls = provision_abort_calls = 0;
	flush_sync_calls = 0;
	last_flush_lsn = InvalidXLogRecPtr;
	memset(last_flush_successor, 0, sizeof(last_flush_successor));
	reuse_wal_calls = recycle_wal_calls = 0;
	fake_reuse_lsn = UINT64_C(0x01000020);
	fake_recycle_lsn = UINT64_C(0x01000040);
	semantic_enter_event = current_exit_hook_event = smgr_exit_hook_event = 0;
	first_root_resolve_event = final_root_resolve_event = 0;
	sample_event = frame_reserve_event = provision_event = 0;
	provision_abort_event = local_release_event = mirror_release_event = 0;
	semantic_leave_event = 0;
	pin_error_local_cleanup_event = unpin_event = cleanup_release_event = 0;
	last_semantic_enter_side = CLUSTER_SEMANTIC_TARGET_SIDE;
	last_outbound_len = 0;
	memset(&last_outbound, 0, sizeof(last_outbound));
	memset(&last_cleanup_release, 0, sizeof(last_cleanup_release));
	memset(&last_insert_key, 0, sizeof(last_insert_key));
	memset(&last_poll_key, 0, sizeof(last_poll_key));
	memset(&last_sleep_key, 0, sizeof(last_sleep_key));
	last_sleep_timeout_ms = 0;
	last_sleep_wait_event = 0;
	memset(&last_pin_logical, 0, sizeof(last_pin_logical));
	memset(&last_pin_root, 0, sizeof(last_pin_root));
	memset(&last_pin_expected, 0, sizeof(last_pin_expected));
	memset(&last_pin_proof, 0, sizeof(last_pin_proof));
	memset(&last_empty_logical, 0, sizeof(last_empty_logical));
	memset(&last_empty_proof, 0, sizeof(last_empty_proof));
	last_pin_handle = NULL;
	last_unpin_handle = NULL;
}

static void
acquire_remote_held(ClusterUndoBlock0CurrentGuard *guard)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;

	memset(guard, 0, sizeof(*guard));
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_SCUR, 1000,
														   guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	fake_poll_result = GES_REPLY_WAIT_POLL_DELIVERED;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_poll(guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_HELD);
}

static void
acquire_remote_xcur_held(ClusterUndoBlock0CurrentGuard *guard)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;

	memset(guard, 0, sizeof(*guard));
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_XCUR, 1000,
														   guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	fake_poll_result = GES_REPLY_WAIT_POLL_DELIVERED;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_poll(guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_HELD);
}

UT_TEST(test_key_guard_and_phase_abi)
{
	ClusterUndoBlock0LogicalKey key = test_key(128);
	ClusterResId resid;

	reset_fixture();
	UT_ASSERT_EQ(sizeof(ClusterUndoBlock0CurrentGuard), 168);
	UT_ASSERT_EQ(sizeof(ClusterUndoBlock0CurrentGuardData), 168);
	UT_ASSERT_EQ(CLUSTER_UNDO_BLOCK0_CURRENT_UNUSED, 0);
	UT_ASSERT_EQ(CLUSTER_UNDO_BLOCK0_CURRENT_ACQUIRE_WAIT, 1);
	UT_ASSERT_EQ(CLUSTER_UNDO_BLOCK0_CURRENT_PHASE_HELD, 2);
	UT_ASSERT_EQ(CLUSTER_UNDO_BLOCK0_CURRENT_RELEASE_WAIT, 3);
	UT_ASSERT_EQ(CLUSTER_UNDO_BLOCK0_CURRENT_CLEANUP, 4);
	UT_ASSERT_EQ(CLUSTER_UNDO_BLOCK0_CURRENT_STARTUP_FENCED_XCUR, 5);
	UT_ASSERT(current_resid_build(&key, &resid));
	UT_ASSERT_EQ(resid.field1, key.segment_id);
	UT_ASSERT_EQ(resid.field2, 0);
	UT_ASSERT_EQ(resid.field3, 0);
	UT_ASSERT_EQ(resid.field4, key.owner_instance);
	UT_ASSERT_EQ(resid.type, 0xFB);
	UT_ASSERT_EQ(resid.lockmethodid, DEFAULT_LOCKMETHOD);
}

UT_TEST(test_wait_reply_uses_exact_acquire_and_release_keys)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;
	GesReplyWaitKey acquire_key;

	reset_fixture();
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_SCUR, 1000,
														   &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	acquire_key = last_insert_key;
	UT_ASSERT(cluster_undo_block0_current_wait_reply(&guard,
													 CLUSTER_UNDO_BLOCK0_WAIT_LIVE_OWNER_ACQUIRE));
	UT_ASSERT_EQ(reply_sleep_calls, 1);
	UT_ASSERT_EQ(memcmp(&last_sleep_key, &acquire_key, sizeof(acquire_key)), 0);
	UT_ASSERT_EQ(last_sleep_key.request_opcode, GES_REQ_OPCODE_REQUEST);
	UT_ASSERT_EQ(last_sleep_timeout_ms, 1);
	UT_ASSERT_EQ(last_sleep_wait_event, WAIT_EVENT_CLUSTER_GES_REPLY_WAIT);

	fake_poll_result = GES_REPLY_WAIT_POLL_DELIVERED;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_poll(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_HELD);
	UT_ASSERT_EQ(cluster_undo_block0_current_release_begin(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	UT_ASSERT(cluster_undo_block0_current_wait_reply(&guard,
													 CLUSTER_UNDO_BLOCK0_WAIT_LIVE_OWNER_ACQUIRE));
	UT_ASSERT_EQ(reply_sleep_calls, 2);
	UT_ASSERT_EQ(last_sleep_key.request_id, acquire_key.request_id);
	UT_ASSERT_EQ(last_sleep_key.source_node_id, acquire_key.source_node_id);
	UT_ASSERT_EQ(last_sleep_key.dest_node_id, acquire_key.dest_node_id);
	UT_ASSERT_EQ(last_sleep_key.cluster_epoch, acquire_key.cluster_epoch);
	UT_ASSERT_EQ(last_sleep_key.request_opcode, GES_REQ_OPCODE_RELEASE);

	UT_ASSERT_EQ(cluster_undo_block0_current_release_poll(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED);
	UT_ASSERT(!cluster_undo_block0_current_wait_reply(&guard,
													  CLUSTER_UNDO_BLOCK0_WAIT_LIVE_OWNER_ACQUIRE));
	UT_ASSERT_EQ(reply_sleep_calls, 2);
}

UT_TEST(test_wait_reply_never_rounds_up_remaining_deadline)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;
	TimestampTz deadline;

	reset_fixture();
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_SCUR, 1000,
														   &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	deadline = current_guard_data(&guard)->deadline;
	fake_now = deadline - 999;
	/* true here suppresses the caller's 1ms fallback; it is never a grant. */
	UT_ASSERT(cluster_undo_block0_current_wait_reply(&guard,
													 CLUSTER_UNDO_BLOCK0_WAIT_LIVE_OWNER_ACQUIRE));
	UT_ASSERT_EQ(reply_sleep_calls, 0);
	UT_ASSERT_EQ(current_guard_data(&guard)->deadline, deadline);
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_poll(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	fake_now = deadline;
	UT_ASSERT(cluster_undo_block0_current_wait_reply(&guard,
													 CLUSTER_UNDO_BLOCK0_WAIT_LIVE_OWNER_ACQUIRE));
	UT_ASSERT_EQ(reply_sleep_calls, 0);
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_poll(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_FAILED);
}

UT_TEST(test_wait_reply_wake_is_only_a_repoll_hint)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;
	TimestampTz deadline;

	reset_fixture();
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_SCUR, 1000,
														   &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	deadline = current_guard_data(&guard)->deadline;
	UT_ASSERT(cluster_undo_block0_current_wait_reply(&guard,
													 CLUSTER_UNDO_BLOCK0_WAIT_LIVE_OWNER_ACQUIRE));
	UT_ASSERT_EQ(reply_poll_calls, 0);
	UT_ASSERT_EQ(reply_delete_calls, 0);
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_poll(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	UT_ASSERT_EQ(current_guard_data(&guard)->deadline, deadline);
	UT_ASSERT_EQ(current_guard_data(&guard)->phase, CLUSTER_UNDO_BLOCK0_CURRENT_ACQUIRE_WAIT);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_reply_wait_counters_measure_real_repoll_and_violation)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;
	uint64 *counts = reply_wait_stats.count[CLUSTER_UNDO_BLOCK0_WAIT_LIVE_OWNER_ACQUIRE];

	reset_fixture();
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_SCUR, 1000,
														   &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	UT_ASSERT(cluster_undo_block0_current_wait_reply(&guard,
													 CLUSTER_UNDO_BLOCK0_WAIT_LIVE_OWNER_ACQUIRE));
	UT_ASSERT_EQ(counts[CLUSTER_UNDO_BLOCK0_WAIT_ELIGIBLE], UINT64_C(1));
	UT_ASSERT_EQ(counts[CLUSTER_UNDO_BLOCK0_WAIT_CV], UINT64_C(1));
	UT_ASSERT_EQ(counts[CLUSTER_UNDO_BLOCK0_WAIT_WAKE_REPOLL], UINT64_C(0));
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_poll(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	UT_ASSERT_EQ(counts[CLUSTER_UNDO_BLOCK0_WAIT_WAKE_REPOLL], UINT64_C(1));
	UT_ASSERT_EQ(counts[CLUSTER_UNDO_BLOCK0_WAIT_POLL_VIOLATION], UINT64_C(0));
	/* Mutation control: skip the required adapter between eligible polls. */
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_poll(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	UT_ASSERT_EQ(counts[CLUSTER_UNDO_BLOCK0_WAIT_POLL_VIOLATION], UINT64_C(1));
	fake_now = current_guard_data(&guard)->deadline - 999;
	UT_ASSERT(cluster_undo_block0_current_wait_reply(&guard,
													 CLUSTER_UNDO_BLOCK0_WAIT_LIVE_OWNER_ACQUIRE));
	UT_ASSERT_EQ(counts[CLUSTER_UNDO_BLOCK0_WAIT_CV], UINT64_C(1));
	UT_ASSERT_EQ(counts[CLUSTER_UNDO_BLOCK0_WAIT_BUDGET_REPOLL], UINT64_C(1));
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_reply_wait_site_and_lock_boundaries)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;
	int site;

	reset_fixture();
	UT_ASSERT_EQ(CLUSTER_UNDO_BLOCK0_WAIT_SITE_COUNT, 15);
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_SCUR, 1000,
														   &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	UT_ASSERT(!cluster_undo_block0_current_wait_reply(&guard, (ClusterUndoBlock0ReplyWaitSite)-1));
	UT_ASSERT(!cluster_undo_block0_current_wait_reply(&guard, CLUSTER_UNDO_BLOCK0_WAIT_SITE_COUNT));
	UT_ASSERT_EQ(reply_sleep_calls, 0);
	for (site = 0; site < CLUSTER_UNDO_BLOCK0_WAIT_SITE_COUNT; site++)
		UT_ASSERT(
			cluster_undo_block0_current_wait_reply(&guard, (ClusterUndoBlock0ReplyWaitSite)site));
	UT_ASSERT_EQ(reply_sleep_calls, 15);
#ifdef USE_ASSERT_CHECKING
	UT_ASSERT(current_reply_wait_locks_safe());
	fake_reply_wait_held_tranche = LWTRANCHE_CLUSTER_UNDO_BUF;
	UT_ASSERT(!current_reply_wait_locks_safe());
	fake_reply_wait_held_tranche = LWTRANCHE_CLUSTER_GES_REPLY_WAIT;
	UT_ASSERT(!current_reply_wait_locks_safe());
	fake_reply_wait_held_tranche = LWTRANCHE_CLUSTER_TT_SLOT;
	UT_ASSERT(!current_reply_wait_locks_safe());
	fake_reply_wait_held_tranche = 0;
#endif
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_startup_namespace_check_rejects_every_reserved_or_unfrozen_type)
{
	uint16 type;

	for (type = UINT8_C(0xF0); type <= UINT8_C(0xFA); type++)
		UT_ASSERT(!current_resid_namespace_valid((uint8)type));
	UT_ASSERT(current_resid_namespace_valid(UINT8_C(0xFB)));
	UT_ASSERT(!current_resid_namespace_valid(UINT8_C(0xFC)));
}

UT_TEST(test_live_owner_resident_preregisters_persistent_exit_hooks)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);

	reset_fixture();
	fake_master = cluster_node_id;
	fake_grant_action = CLUSTER_GRD_GRANT_NOW;
	fake_sample_result = CLUSTER_UNDO_BLOCK0_OK;
	fake_sample_generation = (ClusterUndoBlock0Generation){ true, 0 };
	UT_ASSERT_EQ(cluster_undo_block0_current_live_owner_ensure_resident(&key, 1000),
				 CLUSTER_UNDO_BLOCK0_OK);
	/* The outer live-owner cleanup and the nested generic guard each ensure
	 * the same idempotent persistent hook; only one callback is registered. */
	UT_ASSERT_EQ(smgr_exit_hook_ensure_calls, 2);
	UT_ASSERT(fake_exit_lifo_ok);
	UT_ASSERT_EQ(fake_before_exit_count, 2);
	UT_ASSERT_EQ(fake_before_exit_stack[0].function, current_backend_exit);
	UT_ASSERT_EQ(fake_before_exit_stack[1].function, fake_smgr_exit);
	UT_ASSERT(current_exit_hook_event < semantic_enter_event);
	UT_ASSERT(smgr_exit_hook_event < semantic_enter_event);
}

UT_TEST(test_batch_preflight_and_eight_defensive_ensures_register_once)
{
	int callbacks_before;
	int i;

	reset_fixture();
	callbacks_before = fake_before_exit_count;
	UT_ASSERT_EQ(callbacks_before, 2);
	for (i = 0; i < 9; i++)
		cluster_undo_block0_current_ensure_exit_hooks();
	UT_ASSERT_EQ(smgr_exit_hook_ensure_calls, 9);
	UT_ASSERT_EQ(fake_before_exit_count, callbacks_before);
	UT_ASSERT_EQ(fake_before_exit_stack[0].function, current_backend_exit);
	UT_ASSERT_EQ(fake_before_exit_stack[1].function, fake_smgr_exit);
	UT_ASSERT(fake_exit_lifo_ok);
}

UT_TEST(test_startup_fenced_xcur_begin_end_owns_exact_private_phase)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0CurrentGuard other = { 0 };
	ClusterUndoBlock0CurrentGuardData *data = current_guard_data(&guard);

	reset_fixture();
	UT_ASSERT(cluster_undo_block0_current_startup_fenced_begin(&guard));
	UT_ASSERT_EQ(data->phase, CLUSTER_UNDO_BLOCK0_CURRENT_STARTUP_FENCED_XCUR);
	UT_ASSERT(data->active_linked);
	UT_ASSERT_NOT_NULL(fake_exit_callback);
	UT_ASSERT(cluster_undo_block0_current_startup_fenced_owned());
	UT_ASSERT(!cluster_undo_block0_current_startup_fenced_begin(&guard));
	UT_ASSERT(!cluster_undo_block0_current_startup_fenced_begin(&other));
	UT_ASSERT(cluster_undo_block0_current_startup_fenced_end(&guard));
	UT_ASSERT_EQ(memcmp(&guard, &(ClusterUndoBlock0CurrentGuard){ 0 }, sizeof(guard)), 0);
	UT_ASSERT(!cluster_undo_block0_current_startup_fenced_owned());
	UT_ASSERT(!cluster_undo_block0_current_startup_fenced_end(&guard));
}

UT_TEST(test_begin_preregisters_persistent_hooks_before_exact_72_byte_remote_send)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;
	ClusterUndoBlock0CurrentGuardData *data = current_guard_data(&guard);

	reset_fixture();
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_SCUR, 1000,
														   &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	UT_ASSERT_EQ(smgr_exit_hook_ensure_calls, 1);
	UT_ASSERT(fake_exit_lifo_ok);
	UT_ASSERT(reserve_event < insert_event && insert_event < outbound_event);
	UT_ASSERT_EQ(last_outbound_len, 72);
	UT_ASSERT_EQ(last_outbound.opcode, GES_REQ_OPCODE_REQUEST);
	UT_ASSERT_EQ(last_outbound.lockmode, ShareLock);
	UT_ASSERT_EQ(last_outbound.wait_seq, 0);
	UT_ASSERT_EQ(last_insert_key.request_opcode, GES_REQ_OPCODE_REQUEST);
	UT_ASSERT_EQ(last_insert_key.request_id, data->holder.request_id);
	UT_ASSERT_EQ(data->phase, CLUSTER_UNDO_BLOCK0_CURRENT_ACQUIRE_WAIT);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_census_borrows_one_caller_token_without_ordinary_reentry_or_leave)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;
	ClusterSemanticAdmissionToken admission
		= { .feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
			.record_generation = 41,
			.formation_epoch = 9,
			.side = CLUSTER_SEMANTIC_TARGET_SIDE,
			.entered = true };

	reset_fixture();
	fake_admission_result = CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED;
	fake_master = cluster_node_id;
	fake_grant_action = CLUSTER_GRD_GRANT_NOW;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin_admitted(
					 &key, CLUSTER_UNDO_BLOCK0_SCUR, 1000, &admission, &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_HELD);
	UT_ASSERT_EQ(semantic_enter_calls, 0);
	UT_ASSERT_EQ(semantic_ordinary_recheck_calls, 0);
	UT_ASSERT(semantic_census_recheck_calls > 0);
	UT_ASSERT_EQ(semantic_leave_calls, 0);
	UT_ASSERT(admission.entered);
	UT_ASSERT_EQ(cluster_undo_block0_current_release_begin(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED);
	UT_ASSERT_EQ(semantic_leave_calls, 0);
	UT_ASSERT(admission.entered);
}

UT_TEST(test_ctrc_release_borrows_census_token_for_local_xcur_only)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;
	ClusterSemanticAdmissionToken admission
		= { .feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
			.record_generation = 41,
			.formation_epoch = 9,
			.side = CLUSTER_SEMANTIC_TARGET_SIDE,
			.entered = true };

	reset_fixture();
	fake_admission_result = CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED;
	fake_master = cluster_node_id;
	fake_grant_action = CLUSTER_GRD_GRANT_NOW;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin_ctrc_release(&key, 1000, &admission,
																		&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_HELD);
	UT_ASSERT_EQ(current_guard_data(&guard)->mode, ExclusiveLock);
	UT_ASSERT_EQ(semantic_enter_calls, 0);
	UT_ASSERT_EQ(semantic_ordinary_recheck_calls, 0);
	UT_ASSERT(semantic_census_recheck_calls > 0);
	UT_ASSERT_EQ(cluster_undo_block0_current_release_begin(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED);
	UT_ASSERT_EQ(semantic_leave_calls, 0);
	UT_ASSERT(admission.entered);
}

UT_TEST(test_live_owner_source_borrows_only_xcur_and_target_cannot_produce)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0LogicalKey foreign = test_key(2);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;
	int reserve_before;
	int insert_before;
	int outbound_before;
	ClusterSemanticAdmissionToken admission
		= { .feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
			.record_generation = 0,
			.formation_epoch = 9,
			.side = CLUSTER_SEMANTIC_SOURCE_SIDE,
			.entered = true };

	reset_fixture();
	fake_master = cluster_node_id;
	fake_grant_action = CLUSTER_GRD_GRANT_NOW;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin_live_owner_source(&key, 1000, &admission,
																			 &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_HELD);
	UT_ASSERT_EQ(current_guard_data(&guard)->mode, ExclusiveLock);
	UT_ASSERT_EQ(semantic_enter_calls, 0);
	UT_ASSERT(semantic_ordinary_recheck_calls > 0);
	UT_ASSERT_EQ(semantic_census_recheck_calls, 0);
	UT_ASSERT_EQ(cluster_undo_block0_current_release_begin(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED);
	UT_ASSERT_EQ(semantic_leave_calls, 0);
	UT_ASSERT(admission.entered);
	reserve_before = reserve_event;
	insert_before = insert_event;
	outbound_before = outbound_event;

	memset(&guard, 0, sizeof(guard));
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin_live_owner_source(
					 &foreign, 1000, &admission, &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_FAILED);
	UT_ASSERT_EQ(current_guard_data(&guard)->phase, CLUSTER_UNDO_BLOCK0_CURRENT_UNUSED);
	UT_ASSERT_EQ(reserve_event, reserve_before);
	UT_ASSERT_EQ(insert_event, insert_before);
	UT_ASSERT_EQ(outbound_event, outbound_before);
	UT_ASSERT_EQ(semantic_leave_calls, 0);

	memset(&guard, 0, sizeof(guard));
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin_admitted(
					 &key, CLUSTER_UNDO_BLOCK0_XCUR, 1000, &admission, &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_FAILED);
	UT_ASSERT_EQ(current_guard_data(&guard)->phase, CLUSTER_UNDO_BLOCK0_CURRENT_UNUSED);
	UT_ASSERT_EQ(reserve_event, reserve_before);
	UT_ASSERT_EQ(insert_event, insert_before);
	UT_ASSERT_EQ(outbound_event, outbound_before);
	UT_ASSERT_EQ(semantic_leave_calls, 0);

	memset(&guard, 0, sizeof(guard));
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin_live_owner_source(&key, 1000, &admission,
																			 &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_FAILED);
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin_admitted(
					 &key, CLUSTER_UNDO_BLOCK0_XCUR, 1000, &admission, &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_FAILED);
	UT_ASSERT_EQ(current_guard_data(&guard)->phase, CLUSTER_UNDO_BLOCK0_CURRENT_UNUSED);
}

UT_TEST(test_live_owner_target_borrows_exact_target_token_for_same_xcur)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;
	ClusterSemanticAdmissionToken admission
		= { .feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
			.record_generation = 12,
			.formation_epoch = 9,
			.side = CLUSTER_SEMANTIC_TARGET_SIDE,
			.entered = true };

	reset_fixture();
	fake_master = cluster_node_id;
	fake_grant_action = CLUSTER_GRD_GRANT_NOW;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin_live_owner_target(&key, 1000, &admission,
																			 &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_HELD);
	UT_ASSERT_EQ(current_guard_data(&guard)->mode, ExclusiveLock);
	UT_ASSERT_EQ(semantic_enter_calls, 0);
	UT_ASSERT(semantic_ordinary_recheck_calls > 0);
	UT_ASSERT_EQ(semantic_census_recheck_calls, 0);
	UT_ASSERT_EQ(cluster_undo_block0_current_prove_strict_empty_exclusive(&guard),
				 CLUSTER_UNDO_BLOCK0_OK);
	cluster_undo_block0_current_cancel(&guard);
	UT_ASSERT_EQ(semantic_leave_calls, 0);
	UT_ASSERT(admission.entered);
}

UT_TEST(test_live_owner_resident_re_admit_obeys_exact_resource_order)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);

	reset_fixture();
	fake_master = cluster_node_id;
	fake_grant_action = CLUSTER_GRD_GRANT_NOW;
	fake_sample_result = CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED;
	UT_ASSERT_EQ(cluster_undo_block0_current_live_owner_ensure_resident(&key, 1000),
				 CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(last_semantic_enter_side, CLUSTER_SEMANTIC_SOURCE_SIDE);
	UT_ASSERT_EQ(root_resolve_calls, 2);
	UT_ASSERT_EQ(frame_reserve_calls, 1);
	UT_ASSERT_EQ(provision_calls, 1);
	UT_ASSERT_EQ(provision_abort_calls, 0);
	UT_ASSERT_EQ(unpin_calls, 1);
	UT_ASSERT_EQ(frame_release_calls, 0);
	UT_ASSERT_EQ(local_release_calls, 1);
	UT_ASSERT_EQ(semantic_leave_calls, 1);
	UT_ASSERT(semantic_enter_event < first_root_resolve_event);
	UT_ASSERT(first_root_resolve_event < reserve_event);
	UT_ASSERT(reserve_event < sample_event);
	UT_ASSERT(sample_event < frame_reserve_event);
	UT_ASSERT(frame_reserve_event < provision_event);
	UT_ASSERT(provision_event < final_root_resolve_event);
	UT_ASSERT(final_root_resolve_event < unpin_event);
	UT_ASSERT(unpin_event < local_release_event);
	UT_ASSERT(local_release_event < semantic_leave_event);
}

UT_TEST(test_live_owner_resident_uses_current_target_modifier_admission)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);

	reset_fixture();
	fake_modifier_side = CLUSTER_SEMANTIC_TARGET_SIDE;
	fake_master = cluster_node_id;
	fake_grant_action = CLUSTER_GRD_GRANT_NOW;
	fake_sample_result = CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED;
	UT_ASSERT_EQ(cluster_undo_block0_current_live_owner_ensure_resident(&key, 1000),
				 CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(last_semantic_enter_side, CLUSTER_SEMANTIC_TARGET_SIDE);
	UT_ASSERT_EQ(root_resolve_calls, 2);
	UT_ASSERT_EQ(provision_calls, 1);
	UT_ASSERT_EQ(local_release_calls, 1);
	UT_ASSERT_EQ(semantic_leave_calls, 1);
}

UT_TEST(test_live_owner_resident_existing_exact_needs_no_new_frame)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);

	reset_fixture();
	fake_master = cluster_node_id;
	fake_grant_action = CLUSTER_GRD_GRANT_NOW;
	fake_sample_result = CLUSTER_UNDO_BLOCK0_OK;
	fake_sample_generation = (ClusterUndoBlock0Generation){ true, 0 };
	UT_ASSERT_EQ(cluster_undo_block0_current_live_owner_ensure_resident(&key, 1000),
				 CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(root_resolve_calls, 2);
	UT_ASSERT_EQ(sample_calls, 1);
	UT_ASSERT_EQ(frame_reserve_calls, 0);
	UT_ASSERT_EQ(provision_calls, 0);
	UT_ASSERT_EQ(local_release_calls, 1);
	UT_ASSERT_EQ(semantic_leave_calls, 1);
	UT_ASSERT(sample_event < final_root_resolve_event);
	UT_ASSERT(final_root_resolve_event < local_release_event);
	UT_ASSERT(local_release_event < semantic_leave_event);
}

UT_TEST(test_live_owner_publication_receipt_rechecks_exact_local_state)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0LiveOwnerPublication publication;

	reset_fixture();
	fake_modifier_side = CLUSTER_SEMANTIC_TARGET_SIDE;
	fake_master = cluster_node_id;
	fake_grant_action = CLUSTER_GRD_GRANT_NOW;
	fake_sample_result = CLUSTER_UNDO_BLOCK0_OK;
	fake_sample_generation = (ClusterUndoBlock0Generation){ true, 17 };
	memset(&publication, 0xa5, sizeof(publication));
	UT_ASSERT_EQ(
		cluster_undo_block0_current_live_owner_ensure_resident_exact(&key, 1000, &publication),
		CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT(cluster_undo_block0_current_live_owner_publication_recheck(&publication));
	UT_ASSERT_EQ(root_resolve_calls, 3);
	UT_ASSERT_EQ(sample_calls, 2);

	fake_sample_generation.value = 18;
	UT_ASSERT(!cluster_undo_block0_current_live_owner_publication_recheck(&publication));
	fake_sample_generation.value = 17;
	fake_admission_recheck = false;
	UT_ASSERT(!cluster_undo_block0_current_live_owner_publication_recheck(&publication));
}

UT_TEST(test_live_owner_resident_authority_failures_never_produce)
{
	ClusterUndoBlock0LogicalKey local = test_key(1);
	ClusterUndoBlock0LogicalKey foreign = test_key(2);

	reset_fixture();
	UT_ASSERT_EQ(cluster_undo_block0_current_live_owner_ensure_resident(&foreign, 1000),
				 CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT_EQ(semantic_enter_calls, 0);
	UT_ASSERT_EQ(root_resolve_calls, 0);
	UT_ASSERT_EQ(reserve_event, 0);

	reset_fixture();
	fake_root_resolve_success_limit = 0;
	UT_ASSERT_EQ(cluster_undo_block0_current_live_owner_ensure_resident(&local, 1000),
				 CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT_EQ(semantic_enter_calls, 1);
	UT_ASSERT_EQ(root_resolve_calls, 1);
	UT_ASSERT_EQ(reserve_event, 0);
	UT_ASSERT_EQ(frame_reserve_calls, 0);
	UT_ASSERT_EQ(semantic_leave_calls, 1);

	reset_fixture();
	fake_admission_result = CLUSTER_SEMANTIC_ADMISSION_CLOSED;
	UT_ASSERT_EQ(cluster_undo_block0_current_live_owner_ensure_resident(&local, 1000),
				 CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT_EQ(root_resolve_calls, 0);
	UT_ASSERT_EQ(reserve_event, 0);
	UT_ASSERT_EQ(semantic_leave_calls, 0);

	reset_fixture();
	fake_lms_ready = false;
	UT_ASSERT_EQ(cluster_undo_block0_current_live_owner_ensure_resident(&local, 1000),
				 CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT_EQ(root_resolve_calls, 1);
	UT_ASSERT_EQ(reserve_event, 0);
	UT_ASSERT_EQ(frame_reserve_calls, 0);
	UT_ASSERT_EQ(semantic_leave_calls, 1);
}

UT_TEST(test_live_owner_resident_final_pgrd_drift_unpins_before_xcur_and_source)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);

	reset_fixture();
	fake_master = cluster_node_id;
	fake_grant_action = CLUSTER_GRD_GRANT_NOW;
	fake_sample_result = CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED;
	fake_root_resolve_success_limit = 1;
	UT_ASSERT_EQ(cluster_undo_block0_current_live_owner_ensure_resident(&key, 1000),
				 CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT_EQ(provision_calls, 1);
	UT_ASSERT_EQ(unpin_calls, 1);
	UT_ASSERT_EQ(local_release_calls, 1);
	UT_ASSERT_EQ(semantic_leave_calls, 1);
	UT_ASSERT(unpin_event < local_release_event);
	UT_ASSERT(local_release_event < semantic_leave_event);
}

UT_TEST(test_live_owner_resident_absent_or_invalid_generation_fails_closed)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);

	reset_fixture();
	fake_master = cluster_node_id;
	fake_grant_action = CLUSTER_GRD_GRANT_NOW;
	fake_sample_result = CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED;
	fake_provision_creator = true;
	UT_ASSERT_EQ(cluster_undo_block0_current_live_owner_ensure_resident(&key, 1000),
				 CLUSTER_UNDO_BLOCK0_NOT_FOUND);
	UT_ASSERT_EQ(provision_abort_calls, 1);
	UT_ASSERT_EQ(unpin_calls, 0);
	UT_ASSERT(provision_event < provision_abort_event);
	UT_ASSERT(provision_abort_event < local_release_event);
	UT_ASSERT(local_release_event < semantic_leave_event);

	reset_fixture();
	fake_master = cluster_node_id;
	fake_grant_action = CLUSTER_GRD_GRANT_NOW;
	fake_sample_result = CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED;
	fake_provision_generation = (ClusterUndoBlock0Generation){ false, 0 };
	UT_ASSERT_EQ(cluster_undo_block0_current_live_owner_ensure_resident(&key, 1000),
				 CLUSTER_UNDO_BLOCK0_GENERATION_MISMATCH);
	UT_ASSERT_EQ(provision_calls, 1);
	UT_ASSERT_EQ(unpin_calls, 1);
	UT_ASSERT(unpin_event < local_release_event);
	UT_ASSERT(local_release_event < semantic_leave_event);
}

UT_TEST(test_pending_poll_is_pure_nonblocking_and_keeps_correlation)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;

	reset_fixture();
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_SCUR, 1000,
														   &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_poll(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	UT_ASSERT_EQ(reply_poll_calls, 1);
	UT_ASSERT_EQ(reply_delete_calls, 0);
	UT_ASSERT_EQ(failure, CLUSTER_UNDO_BLOCK0_NOT_FOUND);
	UT_ASSERT_EQ(last_poll_key.request_opcode, GES_REQ_OPCODE_REQUEST);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_reservation_capacity_waits_then_retries_exact_round)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;
	uint64 request_id;
	TimestampTz deadline;

	reset_fixture();
	fake_reserve_result = CLUSTER_GRD_ENTRY_FULL;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_SCUR, 1000,
														   &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	request_id = current_guard_data(&guard)->holder.request_id;
	deadline = current_guard_data(&guard)->deadline;
	UT_ASSERT_EQ(reserve_calls, 1);
	UT_ASSERT(!current_guard_data(&guard)->reservation_held);
	UT_ASSERT(!current_guard_data(&guard)->reply_installed);
	UT_ASSERT(!current_guard_data(&guard)->request_dispatched);
	UT_ASSERT_EQ(insert_event, 0);
	UT_ASSERT_EQ(outbound_event, 0);
	UT_ASSERT_EQ(failure, CLUSTER_UNDO_BLOCK0_NOT_FOUND);

	/* The original round waits until its bounded retry point. */
	fake_reserve_result = CLUSTER_GRD_ENTRY_OK;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_poll(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	UT_ASSERT_EQ(reserve_calls, 1);
	UT_ASSERT_EQ(reply_poll_calls, 0);
	fake_now = current_guard_data(&guard)->next_retry_at;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_poll(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	UT_ASSERT_EQ(reserve_calls, 2);
	UT_ASSERT(current_guard_data(&guard)->reservation_held);
	UT_ASSERT(current_guard_data(&guard)->reply_installed);
	UT_ASSERT(current_guard_data(&guard)->request_dispatched);
	UT_ASSERT_EQ(current_guard_data(&guard)->holder.request_id, request_id);
	UT_ASSERT_EQ(current_guard_data(&guard)->deadline, deadline);
	UT_ASSERT_EQ(reply_poll_calls, 0);
	UT_ASSERT(insert_event > reserve_event);
	UT_ASSERT(outbound_event > insert_event);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_delivered_grant_promotes_to_held)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };

	reset_fixture();
	acquire_remote_held(&guard);
	UT_ASSERT_EQ(current_guard_data(&guard)->phase, CLUSTER_UNDO_BLOCK0_CURRENT_PHASE_HELD);
	UT_ASSERT(!current_guard_data(&guard)->reply_installed);
	UT_ASSERT(!current_guard_data(&guard)->reservation_held);
	UT_ASSERT_EQ(remote_promote_calls, 1);
	UT_ASSERT_EQ(generic_promote_calls, 0);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_remote_grant_with_failed_promote_stages_reliable_release)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;

	reset_fixture();
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_SCUR, 1000,
														   &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	fake_promote_result = CLUSTER_GRD_ENTRY_NOT_FOUND;
	fake_poll_result = GES_REPLY_WAIT_POLL_DELIVERED;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_poll(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_FAILED);
	UT_ASSERT_EQ(failure, CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT_EQ(remote_promote_calls, 1);
	UT_ASSERT_EQ(generic_promote_calls, 0);
	UT_ASSERT_EQ(cleanup_release_calls, 1);
	UT_ASSERT_EQ(last_cleanup_release.opcode, GES_REQ_OPCODE_RELEASE);
	UT_ASSERT_EQ(cancel_wait_calls, 1);
	UT_ASSERT_EQ(current_guard_data(&guard)->phase, CLUSTER_UNDO_BLOCK0_CURRENT_CLEANUP);
}

UT_TEST(test_local_grant_with_failed_promote_drains_local_holder)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;

	reset_fixture();
	fake_master = cluster_node_id;
	fake_grant_action = CLUSTER_GRD_GRANT_NOW;
	fake_promote_result = CLUSTER_GRD_ENTRY_NOT_FOUND;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_XCUR, 1000,
														   &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_FAILED);
	UT_ASSERT_EQ(failure, CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT_EQ(generic_promote_calls, 1);
	UT_ASSERT_EQ(remote_promote_calls, 0);
	UT_ASSERT_EQ(local_release_calls, 1);
	UT_ASSERT_EQ(cleanup_release_calls, 0);
	UT_ASSERT_EQ(current_guard_data(&guard)->phase, CLUSTER_UNDO_BLOCK0_CURRENT_CLEANUP);
}

UT_TEST(test_cancel_tombstones_exact_pending_and_releases_raced_grant)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;

	reset_fixture();
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_SCUR, 1000,
														   &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	fake_abandon_raced_grant = true;
	cluster_undo_block0_current_cancel(&guard);
	UT_ASSERT_EQ(cancel_wait_calls, 1);
	UT_ASSERT_EQ(last_cancel_wait_seq, 0);
	UT_ASSERT_EQ(cleanup_release_calls, 1);
	UT_ASSERT_EQ(last_cleanup_release.opcode, GES_REQ_OPCODE_RELEASE);
	UT_ASSERT_EQ(reservation_cancel_calls, 1);
	UT_ASSERT_EQ(current_guard_data(&guard)->phase, CLUSTER_UNDO_BLOCK0_CURRENT_CLEANUP);
}

UT_TEST(test_send_failure_cleans_only_created_local_obligations)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;

	reset_fixture();
	fake_outbound_ok = false;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_SCUR, 1000,
														   &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_FAILED);
	UT_ASSERT_EQ(failure, CLUSTER_UNDO_BLOCK0_CAPACITY_UNAVAILABLE);
	UT_ASSERT_EQ(cancel_wait_calls, 0);
	UT_ASSERT_EQ(cleanup_release_calls, 0);
	UT_ASSERT_EQ(reservation_cancel_calls, 1);
	UT_ASSERT_EQ(reply_delete_calls, 1);
	UT_ASSERT_EQ(fake_timeout_source, CLUSTER_GES_TSRC_OUTBOUND_RING_FULL);
}

UT_TEST(test_reply_wait_capacity_records_exact_failure_domain)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;

	reset_fixture();
	fake_reply_insert_ok = false;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_SCUR, 1000,
														   &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_FAILED);
	UT_ASSERT_EQ(failure, CLUSTER_UNDO_BLOCK0_CAPACITY_UNAVAILABLE);
	UT_ASSERT_EQ(fake_timeout_source, CLUSTER_GES_TSRC_REPLY_WAIT_TABLE_FULL);
	UT_ASSERT_EQ(outbound_event, 0);
	UT_ASSERT_EQ(reservation_cancel_calls, 1);
}

UT_TEST(test_nonzero_unused_guard_is_rejected_before_any_mutation)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;

	reset_fixture();
	guard.opaque[167] = 1;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_SCUR, 1000,
														   &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_FAILED);
	UT_ASSERT_EQ(failure, CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH);
	UT_ASSERT_EQ(reserve_event, 0);
	UT_ASSERT_EQ(insert_event, 0);
	UT_ASSERT_EQ(outbound_event, 0);
}

UT_TEST(test_same_resid_nesting_refuses_second_guard_without_touching_first)
{
	ClusterUndoBlock0CurrentGuard first = { 0 };
	ClusterUndoBlock0CurrentGuard second = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;
	uint64 first_request_id;

	reset_fixture();
	acquire_remote_held(&first);
	first_request_id = current_guard_data(&first)->holder.request_id;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_SCUR, 1000,
														   &second, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_FAILED);
	UT_ASSERT_EQ(failure, CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT_EQ(memcmp(second.opaque, (const uint8[168]){ 0 }, sizeof(second.opaque)), 0);
	UT_ASSERT_EQ(current_guard_data(&first)->phase, CLUSTER_UNDO_BLOCK0_CURRENT_PHASE_HELD);
	UT_ASSERT_EQ(current_guard_data(&first)->holder.request_id, first_request_id);
	cluster_undo_block0_current_cancel(&first);
}

UT_TEST(test_preflight_failure_restores_reusable_zero_guard)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;

	reset_fixture();
	fake_lms_ready = false;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_SCUR, 1000,
														   &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_FAILED);
	UT_ASSERT_EQ(failure, CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT_EQ(memcmp(guard.opaque, (const uint8[168]){ 0 }, sizeof(guard.opaque)), 0);
	fake_lms_ready = true;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_SCUR, 1000,
														   &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_release_retains_mirror_until_exact_ack_is_consumed)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;

	reset_fixture();
	acquire_remote_held(&guard);
	fake_poll_result = GES_REPLY_WAIT_POLL_PENDING;
	UT_ASSERT_EQ(cluster_undo_block0_current_release_begin(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	UT_ASSERT_EQ(last_outbound.opcode, GES_REQ_OPCODE_RELEASE);
	UT_ASSERT_EQ(mirror_release_calls, 0);
	UT_ASSERT_EQ(cluster_undo_block0_current_release_poll(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	UT_ASSERT_EQ(mirror_release_calls, 0);
	fake_poll_result = GES_REPLY_WAIT_POLL_DELIVERED;
	UT_ASSERT_EQ(cluster_undo_block0_current_release_poll(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED);
	UT_ASSERT_EQ(last_poll_key.request_opcode, GES_REQ_OPCODE_RELEASE);
	UT_ASSERT_EQ(mirror_release_calls, 1);
}

UT_TEST(test_remote_held_cancel_stages_release_then_drops_exact_local_mirror)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };

	reset_fixture();
	acquire_remote_held(&guard);
	UT_ASSERT_EQ(mirror_release_calls, 0);
	cluster_undo_block0_current_cancel(&guard);
	UT_ASSERT_EQ(cleanup_release_calls, 1);
	UT_ASSERT_EQ(mirror_release_calls, 1);
	UT_ASSERT(cleanup_release_event > 0);
	UT_ASSERT(mirror_release_event > cleanup_release_event);
	UT_ASSERT_EQ(current_guard_data(&guard)->phase, CLUSTER_UNDO_BLOCK0_CURRENT_CLEANUP);
}

UT_TEST(test_release_reuses_exact_canonical_72_byte_ges_shape)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;
	ClusterUndoBlock0CurrentGuardData *data = current_guard_data(&guard);

	reset_fixture();
	acquire_remote_held(&guard);
	fake_poll_result = GES_REPLY_WAIT_POLL_PENDING;
	UT_ASSERT_EQ(cluster_undo_block0_current_release_begin(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	UT_ASSERT_EQ(last_outbound_len, sizeof(GesRequestPayload));
	UT_ASSERT_EQ(last_outbound.opcode, GES_REQ_OPCODE_RELEASE);
	UT_ASSERT_EQ(last_outbound.lockmode, NoLock);
	UT_ASSERT_EQ(last_outbound.waiter_xid, InvalidTransactionId);
	UT_ASSERT_EQ(last_outbound.wait_seq, 0);
	UT_ASSERT_EQ(last_outbound.shard_master_generation_lo,
				 (uint32)(data->routing_generation & UINT64_C(0xffffffff)));
	UT_ASSERT_EQ(last_outbound.shard_master_generation_hi,
				 (uint32)(data->routing_generation >> 32));
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_explicit_perpetual_timeout_survives_acquire_and_release)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0CurrentGuardData *data = current_guard_data(&guard);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;

	reset_fixture();
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_SCUR, -1,
														   &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	UT_ASSERT_EQ(data->timeout_ms, -1);
	UT_ASSERT_EQ(data->deadline, 0);
	fake_poll_result = GES_REPLY_WAIT_POLL_DELIVERED;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_poll(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_HELD);
	UT_ASSERT_EQ(cluster_undo_block0_current_release_begin(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	UT_ASSERT_EQ(data->timeout_ms, -1);
	UT_ASSERT_EQ(data->deadline, 0);
	cluster_undo_block0_current_cancel(&guard);
}

/* Drive the real owner clock/retransmit schedule, not a replacement FSM. */
UT_TEST(test_acquire_retries_do_not_preempt_live_caller_budget)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0CurrentGuardData *data = current_guard_data(&guard);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_OK;
	ClusterSemanticAdmissionToken admission
		= { .feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
			.record_generation = 12,
			.formation_epoch = 9,
			.side = CLUSTER_SEMANTIC_TARGET_SIDE,
			.entered = true };
	TimestampTz began;
	TimestampTz deadline;
	GesReplyWaitKey original_key;
	int i;

	reset_fixture();
	began = fake_now;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin_live_owner_target(
					 &key, 60000, &admission, &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	deadline = data->deadline;
	original_key = last_insert_key;
	for (i = 0; i < 6; i++) {
		fake_now = data->next_retry_at;
		UT_ASSERT_EQ(cluster_undo_block0_current_acquire_poll(&guard, &failure),
					 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	}
	UT_ASSERT_EQ(fake_now - began, INT64_C(4700000));
	UT_ASSERT_EQ(deadline - fake_now, INT64_C(55300000));
	UT_ASSERT_EQ(data->deadline, deadline);
	UT_ASSERT_EQ(memcmp(&last_poll_key, &original_key, sizeof(original_key)), 0);
	UT_ASSERT_EQ(cancel_wait_calls, 0);
	UT_ASSERT_EQ(reservation_cancel_calls, 0);
	UT_ASSERT_EQ(pin_calls, 0);
	fake_poll_result = GES_REPLY_WAIT_POLL_DELIVERED;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_poll(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_HELD);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_release_retries_keep_mirror_until_live_budget_ack)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0CurrentGuardData *data = current_guard_data(&guard);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_OK;
	TimestampTz deadline;
	int i;

	reset_fixture();
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_XCUR, 60000,
														   &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	fake_poll_result = GES_REPLY_WAIT_POLL_DELIVERED;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_poll(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_HELD);
	fake_poll_result = GES_REPLY_WAIT_POLL_PENDING;
	UT_ASSERT_EQ(cluster_undo_block0_current_release_begin(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	deadline = data->deadline;
	for (i = 0; i < 6; i++) {
		fake_now = data->next_retry_at;
		UT_ASSERT_EQ(cluster_undo_block0_current_release_poll(&guard, &failure),
					 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	}
	UT_ASSERT_EQ(deadline - fake_now, INT64_C(55300000));
	UT_ASSERT_EQ(data->deadline, deadline);
	UT_ASSERT_EQ(mirror_release_calls, 0);
	UT_ASSERT_EQ(cleanup_release_calls, 0);
	fake_poll_result = GES_REPLY_WAIT_POLL_DELIVERED;
	UT_ASSERT_EQ(cluster_undo_block0_current_release_poll(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED);
	UT_ASSERT_EQ(mirror_release_calls, 1);
}

UT_TEST(test_perpetual_acquire_retransmits_past_attempt_threshold)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0CurrentGuardData *data = current_guard_data(&guard);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;

	reset_fixture();
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_SCUR, -1,
														   &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	data->retry_attempt = (uint16)cluster_ges_retransmit_max_attempts;
	fake_now = data->next_retry_at;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_poll(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	UT_ASSERT_EQ(data->retry_attempt, (uint16)cluster_ges_retransmit_max_attempts + 1);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_wait_failures_preserve_exact_reason_and_cleanup)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);
	const GesReplyWaitPollResult polls[]
		= { GES_REPLY_WAIT_POLL_MISSING, GES_REPLY_WAIT_POLL_ABANDONED,
			GES_REPLY_WAIT_POLL_DELIVERED };
	const ClusterGesTimeoutSrc sources[]
		= { CLUSTER_GES_TSRC_BLOCK0_REPLY_MISSING, CLUSTER_GES_TSRC_BLOCK0_REPLY_ABANDONED,
			CLUSTER_GES_TSRC_MASTER_REJECT_TIMEOUT };
	const char *reasons[] = { "REPLY_MISSING", "REPLY_ABANDONED", "REMOTE_REJECT" };
	unsigned int i;

	for (i = 0; i < lengthof(polls); i++) {
		ClusterUndoBlock0CurrentGuard guard = { 0 };
		ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_OK;

		reset_fixture();
		UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_XCUR,
															   60000, &guard, &failure),
					 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
		fake_poll_result = polls[i];
		fake_poll_verdict
			= (GesReplyWaitVerdict){ GES_REPLY_OPCODE_REJECT, GES_REJECT_REASON_TIMEOUT };
		UT_ASSERT_EQ(cluster_undo_block0_current_acquire_poll(&guard, &failure),
					 CLUSTER_UNDO_BLOCK0_CURRENT_FAILED);
		UT_ASSERT_EQ(failure, CLUSTER_UNDO_BLOCK0_IO_ERROR);
		UT_ASSERT_EQ(fake_timeout_source, sources[i]);
		UT_ASSERT(strstr(wait_log, reasons[i]) != NULL);
		UT_ASSERT(strstr(wait_log, "phase=1") != NULL);
		UT_ASSERT_EQ(reservation_cancel_calls, 1);
		UT_ASSERT_EQ(pin_calls, 0);
		UT_ASSERT(!current_guard_data(&guard)->active_linked);
	}
}

UT_TEST(test_partial_guard_is_not_repaired_by_retry)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_OK;

	reset_fixture();
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_XCUR, 60000,
														   &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	current_guard_data(&guard)->reply_installed = false;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_poll(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_FAILED);
	UT_ASSERT_EQ(fake_timeout_source, CLUSTER_GES_TSRC_BLOCK0_GUARD_INCONSISTENT);
	UT_ASSERT(strstr(wait_log, "GUARD_FLAGS_INCONSISTENT") != NULL);
	UT_ASSERT_EQ(reservation_cancel_calls, 1);
	UT_ASSERT_EQ(pin_calls, 0);
}

UT_TEST(test_real_deadline_still_expires_after_threshold_and_preserves_cleanup)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);
	unsigned int leg;

	for (leg = 0; leg < 3; leg++) {
		ClusterUndoBlock0CurrentGuard guard = { 0 };
		ClusterUndoBlock0CurrentGuardData *data = current_guard_data(&guard);
		ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_OK;

		reset_fixture();
		if (leg == 0)
			fake_reserve_result = CLUSTER_GRD_ENTRY_FULL;
		UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_XCUR,
															   60000, &guard, &failure),
					 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
		if (leg == 2) {
			fake_poll_result = GES_REPLY_WAIT_POLL_DELIVERED;
			UT_ASSERT_EQ(cluster_undo_block0_current_acquire_poll(&guard, &failure),
						 CLUSTER_UNDO_BLOCK0_CURRENT_HELD);
			UT_ASSERT_EQ(cluster_undo_block0_current_release_begin(&guard, &failure),
						 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
			fake_poll_result = GES_REPLY_WAIT_POLL_PENDING;
		}
		data->retry_attempt = 6;
		fake_now = data->deadline;
		UT_ASSERT_EQ(leg == 2 ? cluster_undo_block0_current_release_poll(&guard, &failure)
							  : cluster_undo_block0_current_acquire_poll(&guard, &failure),
					 CLUSTER_UNDO_BLOCK0_CURRENT_FAILED);
		UT_ASSERT_EQ(failure, CLUSTER_UNDO_BLOCK0_IO_ERROR);
		UT_ASSERT_EQ(fake_timeout_source, CLUSTER_GES_TSRC_CV_WAIT_TIMEOUT);
		UT_ASSERT(strstr(wait_log, "elapsed_ms=60000 remaining_ms=0") != NULL);
		UT_ASSERT(strstr(wait_log, leg == 0	  ? "RESERVATION_DEADLINE_EXPIRED"
								   : leg == 1 ? "ACQUIRE_DEADLINE_EXPIRED"
											  : "RELEASE_DEADLINE_EXPIRED")
				  != NULL);
		UT_ASSERT(!data->active_linked);
		UT_ASSERT_EQ(mirror_release_calls, leg == 2 ? 1 : 0);
		UT_ASSERT_EQ(cleanup_release_calls, leg == 2 ? 1 : 0);
	}
}

UT_TEST(test_threshold_keeps_cancel_and_membership_failure)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);
	unsigned int cause;

	for (cause = 0; cause < 2; cause++) {
		ClusterUndoBlock0CurrentGuard guard = { 0 };
		ClusterUndoBlock0CurrentGuardData *data = current_guard_data(&guard);
		ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_OK;

		reset_fixture();
		UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_XCUR,
															   60000, &guard, &failure),
					 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
		data->retry_attempt = 5;
		fake_now = data->next_retry_at;
		UT_ASSERT_EQ(cluster_undo_block0_current_acquire_poll(&guard, &failure),
					 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
		fake_abandon_raced_grant = true;
		if (cause == 0)
			cluster_undo_block0_current_cancel(&guard);
		else {
			fake_member = false;
			UT_ASSERT_EQ(cluster_undo_block0_current_acquire_poll(&guard, &failure),
						 CLUSTER_UNDO_BLOCK0_CURRENT_FAILED);
			UT_ASSERT_EQ(failure, CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
		}
		UT_ASSERT_EQ(cancel_wait_calls, 1);
		UT_ASSERT_EQ(cleanup_release_calls, 1);
		UT_ASSERT_EQ(reservation_cancel_calls, 1);
		UT_ASSERT(!data->active_linked);
	}
}

UT_TEST(test_retry_threshold_logs_once_per_leg_and_attempts_saturate)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0CurrentGuardData *data = current_guard_data(&guard);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_OK;
	int i;

	reset_fixture();
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_XCUR, -1,
														   &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	data->retry_attempt = UINT16_MAX;
	for (i = 0; i < 2; i++) {
		fake_now = data->next_retry_at;
		UT_ASSERT_EQ(cluster_undo_block0_current_acquire_poll(&guard, &failure),
					 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
		UT_ASSERT_EQ(data->retry_attempt, UINT16_MAX);
		UT_ASSERT_EQ(data->next_retry_at - fake_now, INT64_C(1600000));
		UT_ASSERT_EQ(wait_log_count, 1);
	}
	UT_ASSERT(strstr(wait_log, "RETRANSMIT_THRESHOLD_WAIT") != NULL);
	UT_ASSERT_EQ(fake_timeout_source, CLUSTER_GES_TSRC_NONE);
	fake_poll_result = GES_REPLY_WAIT_POLL_DELIVERED;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_poll(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_HELD);
	fake_poll_result = GES_REPLY_WAIT_POLL_PENDING;
	UT_ASSERT_EQ(cluster_undo_block0_current_release_begin(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	data->retry_attempt = UINT16_MAX;
	fake_now = data->next_retry_at;
	UT_ASSERT_EQ(cluster_undo_block0_current_release_poll(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	UT_ASSERT_EQ(data->retry_attempt, UINT16_MAX);
	UT_ASSERT_EQ(wait_log_count, 2);
	UT_ASSERT_EQ(mirror_release_calls, 0);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_perpetual_release_retransmits_past_attempt_threshold)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0CurrentGuardData *data = current_guard_data(&guard);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;

	reset_fixture();
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_SCUR, -1,
														   &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	fake_poll_result = GES_REPLY_WAIT_POLL_DELIVERED;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_poll(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_HELD);
	fake_poll_result = GES_REPLY_WAIT_POLL_PENDING;
	UT_ASSERT_EQ(cluster_undo_block0_current_release_begin(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	data->retry_attempt = (uint16)cluster_ges_retransmit_max_attempts;
	fake_now = data->next_retry_at;
	UT_ASSERT_EQ(cluster_undo_block0_current_release_poll(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	UT_ASSERT_EQ(data->retry_attempt, (uint16)cluster_ges_retransmit_max_attempts + 1);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_generation_zero_is_valid_and_max_is_exhausted)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0ResolvedRoot root = test_root();
	ClusterUndoBlock0Generation observed = { false, 77 };
	ClusterUndoBlock0Generation expected = { true, 0 };
	char page[BLCKSZ];

	reset_fixture();
	acquire_remote_held(&guard);
	UT_ASSERT_EQ(cluster_undo_block0_current_sample_generation(&guard, &root, &observed),
				 CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT(observed.known);
	UT_ASSERT_EQ(observed.value, 0);
	UT_ASSERT_EQ(cluster_undo_block0_current_copy_resident(&guard, &root, &expected, page),
				 CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ((unsigned char)page[0], 0x5a);
	fake_sample_generation.value = UINT32_MAX;
	observed = (ClusterUndoBlock0Generation){ false, 99 };
	UT_ASSERT_EQ(cluster_undo_block0_current_sample_generation(&guard, &root, &observed),
				 CLUSTER_UNDO_BLOCK0_GENERATION_MISMATCH);
	UT_ASSERT(!observed.known);
	UT_ASSERT_EQ(observed.value, 99);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_xcur_guard_cannot_use_scur_sampling_or_copy_surface)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0ResolvedRoot root = test_root();
	ClusterUndoBlock0Generation observed = { false, 77 };
	ClusterUndoBlock0Generation expected = { true, 0 };
	char page[BLCKSZ];

	reset_fixture();
	acquire_remote_xcur_held(&guard);
	memset(page, 0xa5, sizeof(page));
	UT_ASSERT_EQ(cluster_undo_block0_current_sample_generation(&guard, &root, &observed),
				 CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT(!observed.known);
	UT_ASSERT_EQ(observed.value, 77);
	UT_ASSERT_EQ(sample_calls, 0);
	UT_ASSERT_EQ(cluster_undo_block0_current_copy_resident(&guard, &root, &expected, page),
				 CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT_EQ(copy_calls, 0);
	UT_ASSERT_EQ((unsigned char)page[0], 0xa5);
	UT_ASSERT_EQ((unsigned char)page[BLCKSZ - 1], 0xa5);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_xcur_guard_samples_generation_only_through_exclusive_surface)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0ResolvedRoot root = test_root();
	ClusterUndoBlock0Generation observed = { false, 77 };

	reset_fixture();
	acquire_remote_xcur_held(&guard);
	UT_ASSERT_EQ(cluster_undo_block0_current_sample_generation_exclusive(&guard, &root, &observed),
				 CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT(observed.known);
	UT_ASSERT_EQ(observed.value, 0);
	UT_ASSERT_EQ(sample_calls, 1);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_live_owner_xcur_proves_strict_empty_with_exact_current_authority)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;
	ClusterSemanticAdmissionToken admission
		= { .feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
			.formation_epoch = 9,
			.side = CLUSTER_SEMANTIC_SOURCE_SIDE,
			.entered = true };

	reset_fixture();
	fake_master = cluster_node_id;
	fake_grant_action = CLUSTER_GRD_GRANT_NOW;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin_live_owner_source(&key, 1000, &admission,
																			 &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_HELD);
	UT_ASSERT_EQ(cluster_undo_block0_current_prove_strict_empty_exclusive(&guard),
				 CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(empty_calls, 1);
	UT_ASSERT_EQ(last_empty_logical.segment_id, key.segment_id);
	UT_ASSERT_EQ(last_empty_logical.owner_instance, key.owner_instance);
	UT_ASSERT_EQ(last_empty_proof.kind, CLUSTER_UNDO_BLOCK0_LIVE_OWNER);
	UT_ASSERT_EQ(last_empty_proof.owner_instance, key.owner_instance);
	UT_ASSERT(last_empty_proof.cluster_epoch_present);
	UT_ASSERT_EQ(last_empty_proof.cluster_epoch, fake_epoch);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_strict_empty_proof_rejects_non_source_or_drifted_xcur)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };

	reset_fixture();
	acquire_remote_xcur_held(&guard);
	UT_ASSERT_EQ(cluster_undo_block0_current_prove_strict_empty_exclusive(&guard),
				 CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT_EQ(empty_calls, 0);
	cluster_undo_block0_current_cancel(&guard);

	reset_fixture();
	memset(&guard, 0, sizeof(guard));
	{
		ClusterUndoBlock0LogicalKey key = test_key(1);
		ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;
		ClusterSemanticAdmissionToken admission
			= { .feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
				.formation_epoch = 9,
				.side = CLUSTER_SEMANTIC_SOURCE_SIDE,
				.entered = true };

		fake_master = cluster_node_id;
		fake_grant_action = CLUSTER_GRD_GRANT_NOW;
		UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin_live_owner_source(
						 &key, 1000, &admission, &guard, &failure),
					 CLUSTER_UNDO_BLOCK0_CURRENT_HELD);
		fake_empty_invalidates_authority = true;
		UT_ASSERT_EQ(cluster_undo_block0_current_prove_strict_empty_exclusive(&guard),
					 CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
		UT_ASSERT_EQ(empty_calls, 1);
		cluster_undo_block0_current_cancel(&guard);
	}
}

UT_TEST(test_copy_generation_drift_does_not_publish_private_bytes)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0ResolvedRoot root = test_root();
	ClusterUndoBlock0Generation expected = { true, 0 };
	char page[BLCKSZ];

	reset_fixture();
	acquire_remote_held(&guard);
	fake_copy_generation = (ClusterUndoBlock0Generation){ true, 1 };
	memset(page, 0xa5, sizeof(page));
	UT_ASSERT_EQ(cluster_undo_block0_current_copy_resident(&guard, &root, &expected, page),
				 CLUSTER_UNDO_BLOCK0_GENERATION_MISMATCH);
	UT_ASSERT_EQ((unsigned char)page[0], 0xa5);
	UT_ASSERT_EQ((unsigned char)page[BLCKSZ - 1], 0xa5);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_sample_authority_drift_does_not_publish_generation)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0ResolvedRoot root = test_root();
	ClusterUndoBlock0Generation observed = { false, 77 };

	reset_fixture();
	acquire_remote_held(&guard);
	fake_sample_generation = (ClusterUndoBlock0Generation){ true, 9 };
	fake_sample_invalidates_authority = true;
	UT_ASSERT_EQ(cluster_undo_block0_current_sample_generation(&guard, &root, &observed),
				 CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT(!observed.known);
	UT_ASSERT_EQ(observed.value, 77);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_copy_authority_drift_does_not_publish_private_bytes)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0ResolvedRoot root = test_root();
	ClusterUndoBlock0Generation expected = { true, 0 };
	char page[BLCKSZ];

	reset_fixture();
	acquire_remote_held(&guard);
	fake_copy_invalidates_authority = true;
	memset(page, 0xa5, sizeof(page));
	UT_ASSERT_EQ(cluster_undo_block0_current_copy_resident(&guard, &root, &expected, page),
				 CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT_EQ((unsigned char)page[0], 0xa5);
	UT_ASSERT_EQ((unsigned char)page[BLCKSZ - 1], 0xa5);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_xcur_exclusive_pin_binds_guard_root_generation_and_live_proof)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0ResolvedRoot root = test_root();
	ClusterUndoBlock0Generation expected = { true, 0 };
	ClusterUndoBlock0Pin pin;
	char *page = NULL;

	reset_fixture();
	acquire_remote_xcur_held(&guard);
	memset(&pin, 0xa5, sizeof(pin));
	UT_ASSERT_EQ(cluster_undo_block0_current_pin_exclusive(&guard, &root, &expected, &pin, &page),
				 CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(pin_calls, 1);
	UT_ASSERT_EQ(last_pin_logical.segment_id, test_key(1).segment_id);
	UT_ASSERT_EQ(last_pin_logical.owner_instance, 1);
	UT_ASSERT_EQ(last_pin_root.root_id, root.root_id);
	UT_ASSERT_EQ(last_pin_root.root_generation, root.root_generation);
	UT_ASSERT(last_pin_expected.known);
	UT_ASSERT_EQ(last_pin_expected.value, 0);
	UT_ASSERT_EQ(last_pin_mode, CLUSTER_UNDO_BLOCK0_EXCLUSIVE);
	UT_ASSERT_EQ(last_pin_proof.kind, CLUSTER_UNDO_BLOCK0_LIVE_OWNER);
	UT_ASSERT_EQ(last_pin_proof.owner_instance, 1);
	UT_ASSERT(last_pin_proof.cluster_epoch_present);
	UT_ASSERT_EQ(last_pin_proof.cluster_epoch, fake_epoch);
	UT_ASSERT_EQ(pin.slot, 7);
	UT_ASSERT_EQ(page, fake_pin_page);
	UT_ASSERT_EQ(last_pin_handle, &pin);
	cluster_undo_block0_unpin(&pin);
	UT_ASSERT_EQ(last_unpin_handle, &pin);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_exclusive_pin_refuses_scur_and_nonheld_without_publishing_outputs)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0ResolvedRoot root = test_root();
	ClusterUndoBlock0Generation expected = { true, 0 };
	ClusterUndoBlock0Pin pin;
	ClusterUndoBlock0Pin sentinel;
	char *page = (char *)(uintptr_t)0x1234;

	reset_fixture();
	memset(&pin, 0xa5, sizeof(pin));
	sentinel = pin;
	UT_ASSERT_EQ(cluster_undo_block0_current_pin_exclusive(&guard, &root, &expected, &pin, &page),
				 CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT_EQ(memcmp(&pin, &sentinel, sizeof(pin)), 0);
	UT_ASSERT_EQ(page, (char *)(uintptr_t)0x1234);
	UT_ASSERT_EQ(pin_calls, 0);

	acquire_remote_held(&guard);
	UT_ASSERT_EQ(cluster_undo_block0_current_pin_exclusive(&guard, &root, &expected, &pin, &page),
				 CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT_EQ(memcmp(&pin, &sentinel, sizeof(pin)), 0);
	UT_ASSERT_EQ(page, (char *)(uintptr_t)0x1234);
	UT_ASSERT_EQ(pin_calls, 0);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_exclusive_pin_post_pin_drift_unpins_before_refusal_and_keeps_outputs_private)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0ResolvedRoot root = test_root();
	ClusterUndoBlock0Generation expected = { true, 0 };
	ClusterUndoBlock0Pin pin;
	ClusterUndoBlock0Pin sentinel;
	char *page = (char *)(uintptr_t)0x1234;

	reset_fixture();
	acquire_remote_xcur_held(&guard);
	fake_pin_invalidates_authority = true;
	memset(&pin, 0xa5, sizeof(pin));
	sentinel = pin;
	UT_ASSERT_EQ(cluster_undo_block0_current_pin_exclusive(&guard, &root, &expected, &pin, &page),
				 CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT_EQ(pin_calls, 1);
	UT_ASSERT_EQ(unpin_calls, 1);
	UT_ASSERT(unpin_event > 0);
	UT_ASSERT_EQ(memcmp(&pin, &sentinel, sizeof(pin)), 0);
	UT_ASSERT_EQ(page, (char *)(uintptr_t)0x1234);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_exclusive_pin_error_unwinds_local_before_staging_xcur_cleanup)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0ResolvedRoot root = test_root();
	ClusterUndoBlock0Generation expected = { true, 0 };
	ClusterUndoBlock0Pin pin;
	char *page = NULL;
	volatile bool caught = false;

	reset_fixture();
	acquire_remote_xcur_held(&guard);
	fake_pin_throws = true;
	PG_TRY();
	{
		(void)cluster_undo_block0_current_pin_exclusive(&guard, &root, &expected, &pin, &page);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT(pin_error_local_cleanup_event > 0);
	UT_ASSERT(cleanup_release_event > pin_error_local_cleanup_event);
	UT_ASSERT_EQ(cleanup_release_calls, 1);
	UT_ASSERT_EQ(semantic_leave_calls, 1);
	UT_ASSERT_EQ(current_guard_data(&guard)->phase, CLUSTER_UNDO_BLOCK0_CURRENT_CLEANUP);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_exclusive_pin_post_pin_error_unpins_before_staging_xcur_cleanup)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0ResolvedRoot root = test_root();
	ClusterUndoBlock0Generation expected = { true, 0 };
	ClusterUndoBlock0Pin pin;
	char *page = NULL;
	volatile bool caught = false;

	reset_fixture();
	acquire_remote_xcur_held(&guard);
	fake_post_pin_recheck_throws = true;
	PG_TRY();
	{
		(void)cluster_undo_block0_current_pin_exclusive(&guard, &root, &expected, &pin, &page);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(pin_calls, 1);
	UT_ASSERT_EQ(unpin_calls, 1);
	UT_ASSERT(unpin_event > 0);
	UT_ASSERT(cleanup_release_event > unpin_event);
	UT_ASSERT_EQ(cleanup_release_calls, 1);
	UT_ASSERT_EQ(semantic_leave_calls, 1);
	UT_ASSERT_EQ(current_guard_data(&guard)->phase, CLUSTER_UNDO_BLOCK0_CURRENT_CLEANUP);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_sample_error_stages_held_guard_cleanup_before_rethrow)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0ResolvedRoot root = test_root();
	ClusterUndoBlock0Generation observed = { false, 77 };
	volatile bool caught = false;

	reset_fixture();
	acquire_remote_held(&guard);
	fake_sample_throws = true;
	PG_TRY();
	{
		(void)cluster_undo_block0_current_sample_generation(&guard, &root, &observed);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(cleanup_release_calls, 1);
	UT_ASSERT_EQ(semantic_leave_calls, 1);
	UT_ASSERT_EQ(current_guard_data(&guard)->phase, CLUSTER_UNDO_BLOCK0_CURRENT_CLEANUP);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_copy_error_stages_held_guard_cleanup_before_rethrow)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0ResolvedRoot root = test_root();
	ClusterUndoBlock0Generation expected = { true, 0 };
	char page[BLCKSZ];
	volatile bool caught = false;

	reset_fixture();
	acquire_remote_held(&guard);
	fake_copy_throws = true;
	PG_TRY();
	{
		(void)cluster_undo_block0_current_copy_resident(&guard, &root, &expected, page);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(cleanup_release_calls, 1);
	UT_ASSERT_EQ(semantic_leave_calls, 1);
	UT_ASSERT_EQ(current_guard_data(&guard)->phase, CLUSTER_UNDO_BLOCK0_CURRENT_CLEANUP);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_acquire_poll_error_stages_pending_guard_cleanup_before_rethrow)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;
	volatile bool caught = false;

	reset_fixture();
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_SCUR, 1000,
														   &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	fake_poll_throws = true;
	PG_TRY();
	{
		(void)cluster_undo_block0_current_acquire_poll(&guard, &failure);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(cancel_wait_calls, 1);
	UT_ASSERT_EQ(cleanup_release_calls, 1);
	UT_ASSERT_EQ(semantic_leave_calls, 1);
	UT_ASSERT_EQ(current_guard_data(&guard)->phase, CLUSTER_UNDO_BLOCK0_CURRENT_CLEANUP);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_release_poll_error_stages_release_guard_cleanup_before_rethrow)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;
	volatile bool caught = false;

	reset_fixture();
	acquire_remote_held(&guard);
	fake_poll_result = GES_REPLY_WAIT_POLL_PENDING;
	UT_ASSERT_EQ(cluster_undo_block0_current_release_begin(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	fake_poll_throws = true;
	PG_TRY();
	{
		(void)cluster_undo_block0_current_release_poll(&guard, &failure);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(reply_delete_calls, 1);
	UT_ASSERT_EQ(cleanup_release_calls, 1);
	UT_ASSERT_EQ(semantic_leave_calls, 1);
	UT_ASSERT_EQ(current_guard_data(&guard)->phase, CLUSTER_UNDO_BLOCK0_CURRENT_CLEANUP);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_backend_exit_stages_exact_cleanup_without_poll_or_wait)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;

	reset_fixture();
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_SCUR, 1000,
														   &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	UT_ASSERT_NOT_NULL(fake_exit_callback);
	fake_exit_callback(0, (Datum)0);
	UT_ASSERT_EQ(cancel_wait_calls, 1);
	UT_ASSERT_EQ(last_cancel_wait_seq, 0);
	UT_ASSERT_EQ(cleanup_release_calls, 1);
	UT_ASSERT_EQ(reply_poll_calls, 0);
	UT_ASSERT_EQ(current_guard_data(&guard)->phase, CLUSTER_UNDO_BLOCK0_CURRENT_CLEANUP);
}

static void
init_recyclable_block0(char page[BLCKSZ], uint32 segment_id, uint8 owner_instance,
					   uint32 generation)
{
	UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)page;

	memset(page, 0, BLCKSZ);
	((PageHeader)page)->pd_flags = PD_UNDO_SEG_HEADER;
	((PageHeader)page)->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	header->segment_id = segment_id;
	header->segment_size_bytes = UNDO_SEGMENT_SIZE_BYTES;
	header->owner_instance = owner_instance;
	header->segment_state = SEGMENT_RECYCLABLE;
	header->tt_slots_count = TT_SLOTS_PER_SEGMENT;
	header->wrap_count = generation;
}

static void
init_fresh_successor_block0(char page[BLCKSZ], uint32 segment_id, uint8 owner_instance,
							uint32 generation)
{
	UndoSegmentHeaderData *header;

	cluster_undo_segment_make_header_bytes(segment_id, owner_instance, page);
	header = (UndoSegmentHeaderData *)page;
	header->wrap_count = generation;
}

UT_TEST(test_live_owner_reuse_flushes_one_exact_successor_to_disk_and_resident)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Generation expected = { true, 7 };
	char successor_page[BLCKSZ];

	reset_fixture();
	fake_modifier_side = CLUSTER_SEMANTIC_TARGET_SIDE;
	fake_master = cluster_node_id;
	fake_grant_action = CLUSTER_GRD_GRANT_NOW;
	fake_sample_generation = expected;
	init_recyclable_block0(fake_pin_page, key.segment_id, key.owner_instance, expected.value);
	memcpy(fake_disk_page, fake_pin_page, BLCKSZ);
	init_fresh_successor_block0(successor_page, key.segment_id, key.owner_instance,
								expected.value + 1);

	UT_ASSERT_EQ(
		cluster_undo_block0_current_live_owner_reuse_exact(&key, &expected, successor_page, 1000),
		CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(reuse_wal_calls, 1);
	UT_ASSERT_EQ(flush_sync_calls, 1);
	UT_ASSERT_EQ(last_flush_lsn, fake_reuse_lsn);
	UT_ASSERT_EQ(memcmp(last_flush_successor, successor_page, BLCKSZ), 0);
	UT_ASSERT_EQ(memcmp(fake_pin_page, successor_page, BLCKSZ), 0);
	UT_ASSERT_EQ(local_release_calls, 1);
	UT_ASSERT_EQ(semantic_leave_calls, 1);
}

UT_TEST(test_cold_live_owner_reuse_loads_existing_header_under_same_xcur)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Generation expected = { true, 7 };
	char successor_page[BLCKSZ];

	reset_fixture();
	fake_modifier_side = CLUSTER_SEMANTIC_TARGET_SIDE;
	fake_master = cluster_node_id;
	fake_grant_action = CLUSTER_GRD_GRANT_NOW;
	fake_sample_result = CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED;
	fake_provision_creator = false;
	fake_provision_generation = expected;
	init_recyclable_block0(fake_pin_page, key.segment_id, key.owner_instance, expected.value);
	memcpy(fake_disk_page, fake_pin_page, BLCKSZ);
	init_fresh_successor_block0(successor_page, key.segment_id, key.owner_instance,
								expected.value + 1);
	UT_ASSERT_EQ(
		cluster_undo_block0_current_live_owner_reuse_exact(&key, &expected, successor_page, 1000),
		CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(provision_calls, 1);
	UT_ASSERT_EQ(reuse_wal_calls, 1);
	UT_ASSERT_EQ(flush_sync_calls, 1);
	UT_ASSERT_EQ(local_release_calls, 1);
	UT_ASSERT_EQ(semantic_leave_calls, 1);
}

UT_TEST(test_cold_reuse_retains_absence_generation_io_and_authority_negatives)
{
	ClusterUndoBlock0Result failures[]
		= { CLUSTER_UNDO_BLOCK0_NOT_FOUND, CLUSTER_UNDO_BLOCK0_GENERATION_MISMATCH,
			CLUSTER_UNDO_BLOCK0_IO_ERROR, CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED };
	unsigned i;

	for (i = 0; i < lengthof(failures); i++) {
		ClusterUndoBlock0LogicalKey key = test_key(1);
		ClusterUndoBlock0Generation expected = { true, 7 };
		char successor_page[BLCKSZ];

		reset_fixture();
		fake_modifier_side = CLUSTER_SEMANTIC_TARGET_SIDE;
		fake_master = cluster_node_id;
		fake_grant_action = CLUSTER_GRD_GRANT_NOW;
		fake_sample_result = CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED;
		fake_provision_creator = i == 0;
		fake_provision_generation = (ClusterUndoBlock0Generation){ true, i == 1 ? 8 : 7 };
		if (i == 2)
			fake_provision_result = CLUSTER_UNDO_BLOCK0_IO_ERROR;
		if (i == 3)
			fake_sample_invalidates_authority = true;
		init_recyclable_block0(fake_pin_page, key.segment_id, key.owner_instance, 7);
		memcpy(fake_disk_page, fake_pin_page, BLCKSZ);
		init_fresh_successor_block0(successor_page, key.segment_id, key.owner_instance, 8);
		UT_ASSERT_EQ(cluster_undo_block0_current_live_owner_reuse_exact(&key, &expected,
																		successor_page, 1000),
					 failures[i]);
		UT_ASSERT_EQ(reuse_wal_calls, 0);
		UT_ASSERT_EQ(flush_sync_calls, 0);
		UT_ASSERT_EQ(local_release_calls, 1);
		UT_ASSERT_EQ(semantic_leave_calls, 1);
		if (i == 0)
			UT_ASSERT_EQ(provision_abort_calls, 1);
		if (i == 2)
			UT_ASSERT_EQ(frame_release_calls, 1);
	}
}

UT_TEST(test_ordinary_xcur_cold_sample_does_not_gain_live_owner_loading)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0ResolvedRoot root = test_root();
	ClusterUndoBlock0Generation observed = { false, 0 };
	ClusterUndoBlock0Result failure;

	reset_fixture();
	fake_master = cluster_node_id;
	fake_grant_action = CLUSTER_GRD_GRANT_NOW;
	fake_sample_result = CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_XCUR, 1000,
														   &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_HELD);
	UT_ASSERT_EQ(cluster_undo_block0_current_sample_generation_exclusive(&guard, &root, &observed),
				 CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED);
	UT_ASSERT_EQ(provision_calls, 0);
	UT_ASSERT_EQ(frame_reserve_calls, 0);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_live_owner_reuse_rejects_stale_disk_before_any_flush)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Generation expected = { true, 7 };
	UndoSegmentHeaderData *disk;
	char successor_page[BLCKSZ];

	reset_fixture();
	fake_modifier_side = CLUSTER_SEMANTIC_TARGET_SIDE;
	fake_master = cluster_node_id;
	fake_grant_action = CLUSTER_GRD_GRANT_NOW;
	fake_sample_generation = expected;
	init_recyclable_block0(fake_pin_page, key.segment_id, key.owner_instance, expected.value);
	memcpy(fake_disk_page, fake_pin_page, BLCKSZ);
	disk = (UndoSegmentHeaderData *)fake_disk_page;
	disk->wrap_count++;
	init_fresh_successor_block0(successor_page, key.segment_id, key.owner_instance,
								expected.value + 1);

	UT_ASSERT_EQ(
		cluster_undo_block0_current_live_owner_reuse_exact(&key, &expected, successor_page, 1000),
		CLUSTER_UNDO_BLOCK0_GENERATION_MISMATCH);
	UT_ASSERT_EQ(reuse_wal_calls, 0);
	UT_ASSERT_EQ(flush_sync_calls, 0);
	UT_ASSERT_EQ(local_release_calls, 1);
	UT_ASSERT_EQ(semantic_leave_calls, 1);
}

UT_TEST(test_live_owner_reuse_rejects_lifecycle_byte_drift)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Generation expected = { true, 7 };
	UndoSegmentHeaderData *resident;
	char successor_page[BLCKSZ];

	reset_fixture();
	fake_modifier_side = CLUSTER_SEMANTIC_TARGET_SIDE;
	fake_master = cluster_node_id;
	fake_grant_action = CLUSTER_GRD_GRANT_NOW;
	fake_sample_generation = expected;
	init_recyclable_block0(fake_pin_page, key.segment_id, key.owner_instance, expected.value);
	memcpy(fake_disk_page, fake_pin_page, BLCKSZ);
	resident = (UndoSegmentHeaderData *)fake_pin_page;
	resident->tail_block = 19;
	init_fresh_successor_block0(successor_page, key.segment_id, key.owner_instance,
								expected.value + 1);

	UT_ASSERT_EQ(
		cluster_undo_block0_current_live_owner_reuse_exact(&key, &expected, successor_page, 1000),
		CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH);
	UT_ASSERT_EQ(reuse_wal_calls, 0);
	UT_ASSERT_EQ(flush_sync_calls, 0);
}

UT_TEST(test_live_owner_reuse_rejects_nonfresh_successor_template_before_wal)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Generation expected = { true, 7 };
	char successor_page[BLCKSZ];
	int mutation;

	for (mutation = 0; mutation < 4; mutation++) {
		UndoSegmentHeaderData *successor;

		reset_fixture();
		fake_modifier_side = CLUSTER_SEMANTIC_TARGET_SIDE;
		fake_master = cluster_node_id;
		fake_grant_action = CLUSTER_GRD_GRANT_NOW;
		fake_sample_generation = expected;
		init_recyclable_block0(fake_pin_page, key.segment_id, key.owner_instance, expected.value);
		memcpy(fake_disk_page, fake_pin_page, BLCKSZ);
		init_fresh_successor_block0(successor_page, key.segment_id, key.owner_instance,
									expected.value + 1);
		successor = (UndoSegmentHeaderData *)successor_page;

		switch (mutation) {
		case 0:
			successor->free_block_bitmap[0] = UINT8_C(1);
			break;
		case 1:
			successor->segment_flags = UNDO_SEGMENT_FLAG_FULL;
			break;
		case 2:
			successor->tail_block = 1;
			break;
		default:
			successor->_reserved[0] = UINT8_C(1);
			break;
		}

		UT_ASSERT_EQ(cluster_undo_block0_current_live_owner_reuse_exact(&key, &expected,
																		successor_page, 1000),
					 CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH);
		UT_ASSERT_EQ(reuse_wal_calls, 0);
		UT_ASSERT_EQ(flush_sync_calls, 0);
	}
}

UT_TEST(test_live_owner_lifecycle_mutation_flushes_exact_same_generation_successor)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Generation expected = { true, 7 };
	PGAlignedBlock predecessor;
	PGAlignedBlock successor;
	UndoSegmentHeaderData *before = (UndoSegmentHeaderData *)predecessor.data;
	UndoSegmentHeaderData *after = (UndoSegmentHeaderData *)successor.data;

	reset_fixture();
	fake_modifier_side = CLUSTER_SEMANTIC_TARGET_SIDE;
	fake_master = cluster_node_id;
	fake_grant_action = CLUSTER_GRD_GRANT_NOW;
	fake_sample_generation = expected;
	init_recyclable_block0(predecessor.data, key.segment_id, key.owner_instance, expected.value);
	before->segment_state = SEGMENT_ACTIVE;
	memcpy(successor.data, predecessor.data, BLCKSZ);
	after->segment_flags |= UNDO_SEGMENT_FLAG_FULL;
	after->free_block_bitmap[1] |= UINT8_C(0x0f);
	memcpy(fake_disk_page, predecessor.data, BLCKSZ);
	memcpy(fake_pin_page, predecessor.data, BLCKSZ);

	UT_ASSERT_EQ(cluster_undo_block0_current_live_owner_mutate_exact(
					 &key, &expected, predecessor.data, successor.data, 1000),
				 CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(flush_sync_calls, 1);
	UT_ASSERT_EQ(last_flush_lsn, InvalidXLogRecPtr);
	UT_ASSERT_EQ(memcmp(last_flush_successor, successor.data, BLCKSZ), 0);
	UT_ASSERT_EQ(reuse_wal_calls, 0);
	UT_ASSERT_EQ(recycle_wal_calls, 0);
}

UT_TEST(test_live_owner_lifecycle_mutation_rejects_tt_or_reserved_changes)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Generation expected = { true, 7 };
	PGAlignedBlock predecessor;
	PGAlignedBlock successor;
	UndoSegmentHeaderData *before = (UndoSegmentHeaderData *)predecessor.data;
	UndoSegmentHeaderData *after = (UndoSegmentHeaderData *)successor.data;

	reset_fixture();
	init_recyclable_block0(predecessor.data, key.segment_id, key.owner_instance, expected.value);
	before->segment_state = SEGMENT_ACTIVE;
	memcpy(successor.data, predecessor.data, BLCKSZ);
	after->tt_slots[0].status = TT_SLOT_ACTIVE;
	after->tt_slots[0].xid = 700;

	UT_ASSERT_EQ(cluster_undo_block0_current_live_owner_mutate_exact(
					 &key, &expected, predecessor.data, successor.data, 1000),
				 CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH);
	UT_ASSERT_EQ(semantic_enter_calls, 0);
	UT_ASSERT_EQ(flush_sync_calls, 0);

	after->tt_slots[0] = before->tt_slots[0];
	after->_reserved[0] = 1;
	UT_ASSERT_EQ(cluster_undo_block0_current_live_owner_mutate_exact(
					 &key, &expected, predecessor.data, successor.data, 1000),
				 CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH);
	UT_ASSERT_EQ(semantic_enter_calls, 0);
	UT_ASSERT_EQ(flush_sync_calls, 0);
}

UT_TEST(test_record_drain_bound_is_owned_by_active_to_committed_only)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Generation expected = { true, 7 };
	PGAlignedBlock predecessor;
	PGAlignedBlock successor;
	UndoSegmentHeaderData *before = (UndoSegmentHeaderData *)predecessor.data;
	UndoSegmentHeaderData *after = (UndoSegmentHeaderData *)successor.data;
	unsigned variant;

	for (variant = 0; variant < 5; variant++) {
		reset_fixture();
		fake_modifier_side = CLUSTER_SEMANTIC_TARGET_SIDE;
		fake_master = cluster_node_id;
		fake_grant_action = CLUSTER_GRD_GRANT_NOW;
		fake_sample_generation = expected;
		init_recyclable_block0(predecessor.data, key.segment_id, key.owner_instance,
							   expected.value);
		before->segment_state = SEGMENT_ACTIVE;
		before->tail_block = 1;
		UndoSegmentHeader_set_record_seal_upper_scn(before, 100);
		if (variant == 4) {
			before->segment_state = SEGMENT_COMMITTED;
			before->commit_horizon_scn = 120;
		}
		memcpy(successor.data, predecessor.data, BLCKSZ);
		after->segment_state = SEGMENT_COMMITTED;
		after->commit_horizon_scn = 150;
		if (variant == 1)
			after->segment_state = SEGMENT_ACTIVE;
		if (variant == 2)
			after->commit_horizon_scn = 99;
		if (variant == 3)
			after->commit_horizon_scn = InvalidScn;
		memcpy(fake_disk_page, predecessor.data, BLCKSZ);
		memcpy(fake_pin_page, predecessor.data, BLCKSZ);
		if (variant == 0) {
			UT_ASSERT_EQ(cluster_undo_block0_current_live_owner_mutate_exact(
							 &key, &expected, predecessor.data, successor.data, 1000),
						 CLUSTER_UNDO_BLOCK0_OK);
			UT_ASSERT_EQ(flush_sync_calls, 1);
			UT_ASSERT_EQ(memcmp(last_flush_successor, successor.data, BLCKSZ), 0);
		} else {
			UT_ASSERT(cluster_undo_block0_current_live_owner_mutate_exact(
						  &key, &expected, predecessor.data, successor.data, 1000)
					  != CLUSTER_UNDO_BLOCK0_OK);
			UT_ASSERT_EQ(flush_sync_calls, 0);
		}
	}
}

UT_TEST(test_record_seal_requires_an_active_predecessor)
{
	const uint8 states[] = { SEGMENT_ALLOCATED, SEGMENT_COMMITTED, SEGMENT_RECYCLABLE };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Generation expected = { true, 7 };
	PGAlignedBlock predecessor;
	PGAlignedBlock successor;
	UndoSegmentHeaderData *before = (UndoSegmentHeaderData *)predecessor.data;
	UndoSegmentHeaderData *after = (UndoSegmentHeaderData *)successor.data;
	unsigned i;

	for (i = 0; i < lengthof(states); i++) {
		reset_fixture();
		fake_modifier_side = CLUSTER_SEMANTIC_TARGET_SIDE;
		fake_master = cluster_node_id;
		fake_grant_action = CLUSTER_GRD_GRANT_NOW;
		fake_sample_generation = expected;
		init_recyclable_block0(predecessor.data, key.segment_id, key.owner_instance,
							   expected.value);
		before->segment_state = states[i];
		memcpy(successor.data, predecessor.data, BLCKSZ);
		UndoSegmentHeader_set_record_seal_upper_scn(after, 100);
		memcpy(fake_disk_page, predecessor.data, BLCKSZ);
		memcpy(fake_pin_page, predecessor.data, BLCKSZ);
		UT_ASSERT_EQ(cluster_undo_block0_current_live_owner_mutate_exact(
						 &key, &expected, predecessor.data, successor.data, 1000),
					 CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH);
		UT_ASSERT_EQ(semantic_enter_calls, 0);
		UT_ASSERT_EQ(flush_sync_calls, 0);
		UT_ASSERT_EQ(memcmp(fake_disk_page, predecessor.data, BLCKSZ), 0);
	}
}

UT_TEST(test_live_owner_lifecycle_mutation_rebases_only_exact_current_tt_bytes)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Generation expected = { true, 7 };
	PGAlignedBlock predecessor;
	PGAlignedBlock successor;
	UndoSegmentHeaderData *before = (UndoSegmentHeaderData *)predecessor.data;
	UndoSegmentHeaderData *after = (UndoSegmentHeaderData *)successor.data;
	UndoSegmentHeaderData *current;

	reset_fixture();
	fake_modifier_side = CLUSTER_SEMANTIC_TARGET_SIDE;
	fake_master = cluster_node_id;
	fake_grant_action = CLUSTER_GRD_GRANT_NOW;
	fake_sample_generation = expected;
	init_recyclable_block0(predecessor.data, key.segment_id, key.owner_instance, expected.value);
	before->segment_state = SEGMENT_ACTIVE;
	memcpy(successor.data, predecessor.data, BLCKSZ);
	after->segment_flags |= UNDO_SEGMENT_FLAG_FULL;
	memcpy(fake_disk_page, predecessor.data, BLCKSZ);
	memcpy(fake_pin_page, predecessor.data, BLCKSZ);
	current = (UndoSegmentHeaderData *)fake_disk_page;
	current->tt_slots[7].status = TT_SLOT_ACTIVE;
	current->tt_slots[7].xid = 700;
	current->tt_slots[7].wrap = 3;
	memcpy(fake_pin_page, fake_disk_page, BLCKSZ);

	UT_ASSERT_EQ(cluster_undo_block0_current_live_owner_mutate_exact(
					 &key, &expected, predecessor.data, successor.data, 1000),
				 CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(flush_sync_calls, 1);
	current = (UndoSegmentHeaderData *)last_flush_successor;
	UT_ASSERT_EQ(current->tt_slots[7].status, TT_SLOT_ACTIVE);
	UT_ASSERT_EQ(current->tt_slots[7].xid, (TransactionId)700);
	UT_ASSERT(UndoSegmentFlags_is_full(current->segment_flags));
}

UT_TEST(test_live_owner_recycle_converges_exact_committed_disk_successor)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);
	UndoSegmentHeaderData *disk;
	UndoSegmentHeaderData *resident;

	reset_fixture();
	fake_modifier_side = CLUSTER_SEMANTIC_TARGET_SIDE;
	fake_master = cluster_node_id;
	fake_grant_action = CLUSTER_GRD_GRANT_NOW;
	fake_sample_generation = (ClusterUndoBlock0Generation){ true, 7 };
	init_recyclable_block0(fake_disk_page, key.segment_id, key.owner_instance, 7);
	disk = (UndoSegmentHeaderData *)fake_disk_page;
	disk->segment_state = SEGMENT_COMMITTED;
	memcpy(fake_pin_page, fake_disk_page, BLCKSZ);
	resident = (UndoSegmentHeaderData *)fake_pin_page;
	resident->segment_state = SEGMENT_ACTIVE;
	resident->tail_block = 1;
	disk->tail_block = 9;

	UT_ASSERT_EQ(
		cluster_undo_block0_current_live_owner_recycle_exact(&key, UINT64_C(100), fake_epoch, 1000),
		CLUSTER_UNDO_BLOCK0_RECYCLE_ADVANCED);
	UT_ASSERT_EQ(recycle_wal_calls, 1);
	UT_ASSERT_EQ(reuse_wal_calls, 0);
	UT_ASSERT_EQ(flush_sync_calls, 1);
	UT_ASSERT_EQ(last_flush_lsn, fake_recycle_lsn);
	UT_ASSERT_EQ(((UndoSegmentHeaderData *)last_flush_successor)->segment_state,
				 SEGMENT_RECYCLABLE);
	UT_ASSERT_EQ(memcmp(fake_pin_page, last_flush_successor, BLCKSZ), 0);
}

UT_TEST(test_live_owner_recycle_epoch_zero_requires_clean_four_node_formation)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);
	UndoSegmentHeaderData *disk;

	reset_fixture();
	test_cluster_conf.node_count = 4;
	fake_epoch = 0;
	fake_modifier_side = CLUSTER_SEMANTIC_TARGET_SIDE;
	fake_master = cluster_node_id;
	fake_grant_action = CLUSTER_GRD_GRANT_NOW;
	fake_sample_generation = (ClusterUndoBlock0Generation){ true, 7 };
	init_recyclable_block0(fake_disk_page, key.segment_id, key.owner_instance, 7);
	disk = (UndoSegmentHeaderData *)fake_disk_page;
	disk->segment_state = SEGMENT_COMMITTED;
	memcpy(fake_pin_page, fake_disk_page, BLCKSZ);

	UT_ASSERT_EQ(cluster_undo_block0_current_live_owner_recycle_exact(&key, UINT64_C(100), 0, 1000),
				 CLUSTER_UNDO_BLOCK0_RECYCLE_ADVANCED);

	reset_fixture();
	test_cluster_conf.node_count = 2;
	fake_epoch = 0;
	UT_ASSERT_EQ(cluster_undo_block0_current_live_owner_recycle_exact(&key, UINT64_C(100), 0, 1000),
				 CLUSTER_UNDO_BLOCK0_RECYCLE_FAILED);
	UT_ASSERT_EQ(semantic_enter_calls, 0);
}

UT_TEST(test_live_owner_recycle_rejects_tt_drift_before_wal)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);
	UndoSegmentHeaderData *disk;
	UndoSegmentHeaderData *resident;

	reset_fixture();
	fake_modifier_side = CLUSTER_SEMANTIC_TARGET_SIDE;
	fake_master = cluster_node_id;
	fake_grant_action = CLUSTER_GRD_GRANT_NOW;
	fake_sample_generation = (ClusterUndoBlock0Generation){ true, 7 };
	init_recyclable_block0(fake_disk_page, key.segment_id, key.owner_instance, 7);
	disk = (UndoSegmentHeaderData *)fake_disk_page;
	disk->segment_state = SEGMENT_COMMITTED;
	memcpy(fake_pin_page, fake_disk_page, BLCKSZ);
	resident = (UndoSegmentHeaderData *)fake_pin_page;
	resident->tt_slots[0].xid = 99;

	UT_ASSERT_EQ(
		cluster_undo_block0_current_live_owner_recycle_exact(&key, UINT64_C(100), fake_epoch, 1000),
		CLUSTER_UNDO_BLOCK0_RECYCLE_FAILED);
	UT_ASSERT_EQ(recycle_wal_calls, 0);
	UT_ASSERT_EQ(flush_sync_calls, 0);
}

UT_TEST(test_live_owner_recycle_requires_release_on_every_committed_slot)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);
	UndoSegmentHeaderData *disk;
	UndoSegmentHeaderData *resident;

	reset_fixture();
	fake_modifier_side = CLUSTER_SEMANTIC_TARGET_SIDE;
	fake_master = cluster_node_id;
	fake_grant_action = CLUSTER_GRD_GRANT_NOW;
	fake_sample_generation = (ClusterUndoBlock0Generation){ true, 7 };
	init_recyclable_block0(fake_disk_page, key.segment_id, key.owner_instance, 7);
	disk = (UndoSegmentHeaderData *)fake_disk_page;
	disk->segment_state = SEGMENT_COMMITTED;
	disk->tt_slots[0].status = TT_SLOT_COMMITTED;
	disk->tt_slots[0].xid = (TransactionId)700;
	disk->tt_slots[0].wrap = 3;
	disk->tt_slots[0].commit_scn = (SCN)100;
	memcpy(fake_pin_page, fake_disk_page, BLCKSZ);

	UT_ASSERT_EQ(
		cluster_undo_block0_current_live_owner_recycle_exact(&key, (SCN)100, fake_epoch, 1000),
		CLUSTER_UNDO_BLOCK0_RECYCLE_RETAINED);
	UT_ASSERT_EQ(recycle_wal_calls, 0);
	UT_ASSERT_EQ(flush_sync_calls, 0);

	/* Exact release bit plus the inclusive folded floor opens L11. */
	disk->tt_slots[0].flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
	resident = (UndoSegmentHeaderData *)fake_pin_page;
	resident->tt_slots[0].flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
	fake_handoff_pending = true;
	UT_ASSERT_EQ(
		cluster_undo_block0_current_live_owner_recycle_exact(&key, (SCN)100, fake_epoch, 1000),
		CLUSTER_UNDO_BLOCK0_RECYCLE_RETAINED);
	UT_ASSERT_EQ(recycle_wal_calls, 0);
	UT_ASSERT_EQ(flush_sync_calls, 0);
	fake_handoff_pending = false;
	UT_ASSERT_EQ(
		cluster_undo_block0_current_live_owner_recycle_exact(&key, (SCN)100, fake_epoch, 1000),
		CLUSTER_UNDO_BLOCK0_RECYCLE_ADVANCED);
	UT_ASSERT_EQ(recycle_wal_calls, 1);
	UT_ASSERT_EQ(flush_sync_calls, 1);
}

UT_TEST(test_terminal_reuse_chain_preserves_refs_and_horizon_before_generation_rebirth)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Generation expected = { true, 7 };
	PGAlignedBlock successor;
	UndoSegmentHeaderData *disk;

	reset_fixture();
	fake_modifier_side = CLUSTER_SEMANTIC_TARGET_SIDE;
	fake_master = cluster_node_id;
	fake_grant_action = CLUSTER_GRD_GRANT_NOW;
	fake_sample_generation = expected;
	init_recyclable_block0(fake_disk_page, key.segment_id, key.owner_instance, 7);
	disk = (UndoSegmentHeaderData *)fake_disk_page;
	disk->segment_state = SEGMENT_COMMITTED;
	disk->tt_slots[0].status = TT_SLOT_COMMITTED;
	disk->tt_slots[0].xid = (TransactionId)700;
	disk->tt_slots[0].wrap = 3;
	disk->tt_slots[0].commit_scn = (SCN)100;
	memcpy(fake_pin_page, fake_disk_page, BLCKSZ);

	/* Terminal alone cannot consume a live receipt/ref. */
	UT_ASSERT_EQ(
		cluster_undo_block0_current_live_owner_recycle_exact(&key, (SCN)100, fake_epoch, 1000),
		CLUSTER_UNDO_BLOCK0_RECYCLE_RETAINED);
	UT_ASSERT_EQ(flush_sync_calls, 0);
	disk->tt_slots[0].flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
	memcpy(fake_pin_page, fake_disk_page, BLCKSZ);
	/* A live reader's folded horizon remains a separate hard gate. */
	UT_ASSERT_EQ(
		cluster_undo_block0_current_live_owner_recycle_exact(&key, (SCN)99, fake_epoch, 1000),
		CLUSTER_UNDO_BLOCK0_RECYCLE_RETAINED);
	UT_ASSERT_EQ(flush_sync_calls, 0);
	UT_ASSERT_EQ(
		cluster_undo_block0_current_live_owner_recycle_exact(&key, (SCN)100, fake_epoch, 1000),
		CLUSTER_UNDO_BLOCK0_RECYCLE_ADVANCED);
	UT_ASSERT_EQ(disk->segment_state, SEGMENT_RECYCLABLE);
	UT_ASSERT_EQ(disk->wrap_count, 7);
	UT_ASSERT_EQ(recycle_wal_calls, 1);
	UT_ASSERT_EQ(memcmp(fake_pin_page, fake_disk_page, BLCKSZ), 0);

	init_fresh_successor_block0(successor.data, key.segment_id, key.owner_instance, 8);
	UT_ASSERT_EQ(
		cluster_undo_block0_current_live_owner_reuse_exact(&key, &expected, successor.data, 1000),
		CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(disk->segment_state, SEGMENT_ALLOCATED);
	UT_ASSERT_EQ(disk->wrap_count, 8);
	UT_ASSERT_EQ(reuse_wal_calls, 1);
	UT_ASSERT_EQ(flush_sync_calls, 2);
	UT_ASSERT_EQ(memcmp(fake_pin_page, fake_disk_page, BLCKSZ), 0);
	UT_ASSERT_EQ(memcmp(fake_pin_page, successor.data, BLCKSZ), 0);
	/* Replaying the old exact identity cannot produce another generation. */
	UT_ASSERT_NE(
		cluster_undo_block0_current_live_owner_reuse_exact(&key, &expected, successor.data, 1000),
		CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(reuse_wal_calls, 1);
	UT_ASSERT_EQ(flush_sync_calls, 2);
}

UT_TEST(test_live_owner_recycle_aborted_needs_release_not_scn_age)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);
	UndoSegmentHeaderData *disk;
	UndoSegmentHeaderData *resident;

	reset_fixture();
	fake_modifier_side = CLUSTER_SEMANTIC_TARGET_SIDE;
	fake_master = cluster_node_id;
	fake_grant_action = CLUSTER_GRD_GRANT_NOW;
	fake_sample_generation = (ClusterUndoBlock0Generation){ true, 7 };
	init_recyclable_block0(fake_disk_page, key.segment_id, key.owner_instance, 7);
	disk = (UndoSegmentHeaderData *)fake_disk_page;
	disk->segment_state = SEGMENT_COMMITTED;
	disk->tt_slots[0].status = TT_SLOT_ABORTED;
	disk->tt_slots[0].xid = (TransactionId)700;
	disk->tt_slots[0].wrap = 3;
	disk->tt_slots[0].commit_scn = InvalidScn;
	memcpy(fake_pin_page, fake_disk_page, BLCKSZ);

	UT_ASSERT_EQ(
		cluster_undo_block0_current_live_owner_recycle_exact(&key, (SCN)1, fake_epoch, 1000),
		CLUSTER_UNDO_BLOCK0_RECYCLE_RETAINED);
	disk->tt_slots[0].flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
	resident = (UndoSegmentHeaderData *)fake_pin_page;
	resident->tt_slots[0].flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
	UT_ASSERT_EQ(
		cluster_undo_block0_current_live_owner_recycle_exact(&key, (SCN)1, fake_epoch, 1000),
		CLUSTER_UNDO_BLOCK0_RECYCLE_ADVANCED);
	UT_ASSERT_EQ(recycle_wal_calls, 1);
}

UT_TEST(test_live_owner_recycle_rejects_invalid_horizon_before_authority_or_wal)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);
	UndoSegmentHeaderData *disk;

	reset_fixture();
	fake_modifier_side = CLUSTER_SEMANTIC_TARGET_SIDE;
	fake_master = cluster_node_id;
	fake_grant_action = CLUSTER_GRD_GRANT_NOW;
	fake_sample_generation = (ClusterUndoBlock0Generation){ true, 7 };
	init_recyclable_block0(fake_disk_page, key.segment_id, key.owner_instance, 7);
	disk = (UndoSegmentHeaderData *)fake_disk_page;
	disk->segment_state = SEGMENT_COMMITTED;
	memcpy(fake_pin_page, fake_disk_page, BLCKSZ);

	UT_ASSERT_EQ(
		cluster_undo_block0_current_live_owner_recycle_exact(&key, InvalidScn, fake_epoch, 1000),
		CLUSTER_UNDO_BLOCK0_RECYCLE_FAILED);
	UT_ASSERT_EQ(semantic_enter_calls, 0);
	UT_ASSERT_EQ(reserve_calls, 0);
	UT_ASSERT_EQ(recycle_wal_calls, 0);
	UT_ASSERT_EQ(flush_sync_calls, 0);
}

UT_TEST(test_live_owner_recycle_rejects_fold_epoch_drift_before_authority_or_wal)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);
	UndoSegmentHeaderData *disk;

	reset_fixture();
	fake_modifier_side = CLUSTER_SEMANTIC_TARGET_SIDE;
	fake_master = cluster_node_id;
	fake_grant_action = CLUSTER_GRD_GRANT_NOW;
	fake_sample_generation = (ClusterUndoBlock0Generation){ true, 7 };
	init_recyclable_block0(fake_disk_page, key.segment_id, key.owner_instance, 7);
	disk = (UndoSegmentHeaderData *)fake_disk_page;
	disk->segment_state = SEGMENT_COMMITTED;
	memcpy(fake_pin_page, fake_disk_page, BLCKSZ);

	UT_ASSERT_EQ(cluster_undo_block0_current_live_owner_recycle_exact(&key, UINT64_C(100),
																	  fake_epoch + 1, 1000),
				 CLUSTER_UNDO_BLOCK0_RECYCLE_FAILED);
	UT_ASSERT_EQ(semantic_enter_calls, 0);
	UT_ASSERT_EQ(reserve_calls, 0);
	UT_ASSERT_EQ(recycle_wal_calls, 0);
	UT_ASSERT_EQ(flush_sync_calls, 0);
}

UT_TEST(test_readiness_denial_names_first_false_gate_without_side_effects)
{
	const char *expected[]
		= { "gate=CLUSTER_DISABLED", "gate=NODE_INVALID",  "gate=LMS_NOT_READY",
			"gate=LMON_NOT_READY",	 "gate=QUORUM_DENIED", "gate=LOCAL_NOT_MEMBER" };
	const unsigned int reads[] = { 0, 0, 1, 3, 7, 15 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	unsigned int i;

	for (i = 0; i < lengthof(expected); i++) {
		ClusterUndoBlock0CurrentGuard guard = { 0 };
		ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_OK;

		reset_fixture();
		/* All later prerequisites also fail: prove original short-circuiting. */
		cluster_enabled = i != 0;
		cluster_node_id = i <= 1 ? -1 : 0;
		fake_lms_ready = i > 2;
		fake_lmon_status = i > 3 ? CLUSTER_LMON_READY : CLUSTER_LMON_NOT_STARTED;
		fake_quorum = i > 4;
		fake_member = false;
		UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_XCUR, 1000,
															   &guard, &failure),
					 CLUSTER_UNDO_BLOCK0_CURRENT_FAILED);
		UT_ASSERT_EQ(failure, CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
		UT_ASSERT_EQ(memcmp(guard.opaque, (const uint8[168]){ 0 }, 168), 0);
		UT_ASSERT_EQ(readiness_reads, reads[i]);
		UT_ASSERT_EQ(semantic_enter_calls, 0);
		UT_ASSERT_EQ(reserve_calls, 0);
		UT_ASSERT_EQ(insert_event, 0);
		UT_ASSERT_EQ(outbound_event, 0);
		UT_ASSERT(strstr(admission_log, "predicate=2 result=5") != NULL);
		UT_ASSERT(strstr(admission_log, expected[i]) != NULL);
		UT_ASSERT_EQ(admission_log_count, 1);
		UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_XCUR, 1000,
															   &guard, &failure),
					 CLUSTER_UNDO_BLOCK0_CURRENT_FAILED);
		UT_ASSERT_EQ(admission_log_count, 1);
	}
	reset_fixture();
}

int
main(void)
{
	UT_PLAN(85);
	UT_RUN(test_cold_live_owner_reuse_loads_existing_header_under_same_xcur);
	UT_RUN(test_cold_reuse_retains_absence_generation_io_and_authority_negatives);
	UT_RUN(test_ordinary_xcur_cold_sample_does_not_gain_live_owner_loading);
	UT_RUN(test_record_seal_requires_an_active_predecessor);
	UT_RUN(test_record_drain_bound_is_owned_by_active_to_committed_only);
	UT_RUN(test_wait_failures_preserve_exact_reason_and_cleanup);
	UT_RUN(test_partial_guard_is_not_repaired_by_retry);
	UT_RUN(test_real_deadline_still_expires_after_threshold_and_preserves_cleanup);
	UT_RUN(test_threshold_keeps_cancel_and_membership_failure);
	UT_RUN(test_retry_threshold_logs_once_per_leg_and_attempts_saturate);
	UT_RUN(test_acquire_retries_do_not_preempt_live_caller_budget);
	UT_RUN(test_release_retries_keep_mirror_until_live_budget_ack);
	UT_RUN(test_readiness_denial_names_first_false_gate_without_side_effects);
	UT_RUN(test_terminal_reuse_chain_preserves_refs_and_horizon_before_generation_rebirth);
	UT_RUN(test_key_guard_and_phase_abi);
	UT_RUN(test_wait_reply_uses_exact_acquire_and_release_keys);
	UT_RUN(test_wait_reply_never_rounds_up_remaining_deadline);
	UT_RUN(test_wait_reply_wake_is_only_a_repoll_hint);
	UT_RUN(test_reply_wait_site_and_lock_boundaries);
	UT_RUN(test_reply_wait_counters_measure_real_repoll_and_violation);
	UT_RUN(test_startup_namespace_check_rejects_every_reserved_or_unfrozen_type);
	UT_RUN(test_live_owner_resident_preregisters_persistent_exit_hooks);
	UT_RUN(test_batch_preflight_and_eight_defensive_ensures_register_once);
	UT_RUN(test_startup_fenced_xcur_begin_end_owns_exact_private_phase);
	UT_RUN(test_begin_preregisters_persistent_hooks_before_exact_72_byte_remote_send);
	UT_RUN(test_census_borrows_one_caller_token_without_ordinary_reentry_or_leave);
	UT_RUN(test_ctrc_release_borrows_census_token_for_local_xcur_only);
	UT_RUN(test_live_owner_source_borrows_only_xcur_and_target_cannot_produce);
	UT_RUN(test_live_owner_target_borrows_exact_target_token_for_same_xcur);
	UT_RUN(test_live_owner_resident_re_admit_obeys_exact_resource_order);
	UT_RUN(test_live_owner_resident_uses_current_target_modifier_admission);
	UT_RUN(test_live_owner_resident_existing_exact_needs_no_new_frame);
	UT_RUN(test_live_owner_publication_receipt_rechecks_exact_local_state);
	UT_RUN(test_live_owner_resident_authority_failures_never_produce);
	UT_RUN(test_live_owner_resident_final_pgrd_drift_unpins_before_xcur_and_source);
	UT_RUN(test_live_owner_resident_absent_or_invalid_generation_fails_closed);
	UT_RUN(test_pending_poll_is_pure_nonblocking_and_keeps_correlation);
	UT_RUN(test_reservation_capacity_waits_then_retries_exact_round);
	UT_RUN(test_delivered_grant_promotes_to_held);
	UT_RUN(test_remote_grant_with_failed_promote_stages_reliable_release);
	UT_RUN(test_local_grant_with_failed_promote_drains_local_holder);
	UT_RUN(test_cancel_tombstones_exact_pending_and_releases_raced_grant);
	UT_RUN(test_send_failure_cleans_only_created_local_obligations);
	UT_RUN(test_reply_wait_capacity_records_exact_failure_domain);
	UT_RUN(test_nonzero_unused_guard_is_rejected_before_any_mutation);
	UT_RUN(test_same_resid_nesting_refuses_second_guard_without_touching_first);
	UT_RUN(test_preflight_failure_restores_reusable_zero_guard);
	UT_RUN(test_release_retains_mirror_until_exact_ack_is_consumed);
	UT_RUN(test_remote_held_cancel_stages_release_then_drops_exact_local_mirror);
	UT_RUN(test_release_reuses_exact_canonical_72_byte_ges_shape);
	UT_RUN(test_explicit_perpetual_timeout_survives_acquire_and_release);
	UT_RUN(test_perpetual_acquire_retransmits_past_attempt_threshold);
	UT_RUN(test_perpetual_release_retransmits_past_attempt_threshold);
	UT_RUN(test_generation_zero_is_valid_and_max_is_exhausted);
	UT_RUN(test_xcur_guard_cannot_use_scur_sampling_or_copy_surface);
	UT_RUN(test_xcur_guard_samples_generation_only_through_exclusive_surface);
	UT_RUN(test_live_owner_xcur_proves_strict_empty_with_exact_current_authority);
	UT_RUN(test_strict_empty_proof_rejects_non_source_or_drifted_xcur);
	UT_RUN(test_sample_authority_drift_does_not_publish_generation);
	UT_RUN(test_copy_generation_drift_does_not_publish_private_bytes);
	UT_RUN(test_copy_authority_drift_does_not_publish_private_bytes);
	UT_RUN(test_xcur_exclusive_pin_binds_guard_root_generation_and_live_proof);
	UT_RUN(test_exclusive_pin_refuses_scur_and_nonheld_without_publishing_outputs);
	UT_RUN(test_exclusive_pin_post_pin_drift_unpins_before_refusal_and_keeps_outputs_private);
	UT_RUN(test_exclusive_pin_error_unwinds_local_before_staging_xcur_cleanup);
	UT_RUN(test_exclusive_pin_post_pin_error_unpins_before_staging_xcur_cleanup);
	UT_RUN(test_sample_error_stages_held_guard_cleanup_before_rethrow);
	UT_RUN(test_copy_error_stages_held_guard_cleanup_before_rethrow);
	UT_RUN(test_acquire_poll_error_stages_pending_guard_cleanup_before_rethrow);
	UT_RUN(test_release_poll_error_stages_release_guard_cleanup_before_rethrow);
	UT_RUN(test_backend_exit_stages_exact_cleanup_without_poll_or_wait);
	UT_RUN(test_live_owner_reuse_flushes_one_exact_successor_to_disk_and_resident);
	UT_RUN(test_live_owner_reuse_rejects_stale_disk_before_any_flush);
	UT_RUN(test_live_owner_reuse_rejects_lifecycle_byte_drift);
	UT_RUN(test_live_owner_reuse_rejects_nonfresh_successor_template_before_wal);
	UT_RUN(test_live_owner_lifecycle_mutation_flushes_exact_same_generation_successor);
	UT_RUN(test_live_owner_lifecycle_mutation_rejects_tt_or_reserved_changes);
	UT_RUN(test_live_owner_lifecycle_mutation_rebases_only_exact_current_tt_bytes);
	UT_RUN(test_live_owner_recycle_converges_exact_committed_disk_successor);
	UT_RUN(test_live_owner_recycle_epoch_zero_requires_clean_four_node_formation);
	UT_RUN(test_live_owner_recycle_rejects_tt_drift_before_wal);
	UT_RUN(test_live_owner_recycle_requires_release_on_every_committed_slot);
	UT_RUN(test_live_owner_recycle_aborted_needs_release_not_scn_age);
	UT_RUN(test_live_owner_recycle_rejects_invalid_horizon_before_authority_or_wal);
	UT_RUN(test_live_owner_recycle_rejects_fold_epoch_drift_before_authority_or_wal);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
