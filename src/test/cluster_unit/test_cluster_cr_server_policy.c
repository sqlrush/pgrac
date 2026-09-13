/*-------------------------------------------------------------------------
 *
 * test_cluster_cr_server_policy.c
 *	  Standalone unit tests for the spec-6.12b CR-server split policy
 *	  (cluster_cr_server_split_classify): FULL / PARTIAL / DENY over
 *	  write_scn-DESC chain-origin sequences, including the malformed-input
 *	  fail-closed and the empty-chain FULL.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cr_server_policy.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Links cluster_cr_server_policy.o
 *	  only; the policy is pure (no shmem / locks / elog).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/clog.h"
#include "access/transam.h"
#include "cluster/cluster_cr.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_undo_retention.h"
#include "cluster/cluster_xid_authority.h"
#include "cluster/cluster_xid_stripe.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/procarray.h"
#include "utils/elog.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;
volatile uint32 InterruptHoldoffCount = 0;

static LWLockPadded c0_lwlocks[64];
LWLockPadded *MainLWLockArray = c0_lwlocks;
static VariableCacheData c0_variable_cache;
VariableCache ShmemVariableCache = &c0_variable_cache;

/*
 * TT-P013-RULE25-B RED seam.  The implementation belongs in the pure
 * CR-server policy object after the RED is accepted; keeping the prototypes
 * test-local makes the current product fail at link time without changing a
 * product header.
 *
 * Only a positive origin-live proof may produce IN_PROGRESS.  An explicit
 * origin CLOG abort is positive ABORTED authority, while "not committed" by
 * itself remains UNKNOWN_FAIL_CLOSED.
 */
extern ClusterUndoVerdictKind
cluster_cr_server_resolved_scn_verdict(bool clog_did_commit, bool clog_did_abort,
									   bool xid_is_in_progress);
extern bool cluster_cr_server_live_binding_exact(bool xid_is_mine,
												 uint32 expected_segment_id,
												 uint32 expected_tt_slot_id,
												 uint16 matched_segment,
												 uint16 matched_slot,
												 bool xid_is_in_progress,
												 bool durable_binding_stable);

/*
 * TT-P013-RULE25-C0 test-local RED seams.  The first is the pure positive-
 * proof table; the second calls the real static own-xid resolver from a
 * function-sectioned USE_CLUSTER_UNIT product object.  Keeping both
 * declarations test-local makes old 34b fail at link without changing a
 * product header before the RED receipt is captured.
 */
extern ClusterUndoVerdictKind cluster_cr_server_c0_zero_match_verdict(
	bool authoritative, bool xid_is_mine, uint32 expected_segment_id,
	uint32 expected_tt_slot_id, bool no_raw_reuse_window, bool clog_is_committed,
	bool clog_is_aborted, bool clog_is_in_progress, bool xid_is_in_progress);
extern ClusterUndoVerdictKind cluster_cr_server_test_own_xid_verdict(
	TransactionId xid, uint32 expected_segment_id, uint32 expected_tt_slot_id,
	bool authoritative);
extern const char *cluster_cr_server_test_ordinary_reason(TransactionId xid, uint32 segment_hint,
														  ClusterUndoVerdictKind *kind);
extern ClusterUndoVerdictResult cluster_cr_server_test_own_xid_pair_verdict(
	TransactionId xid, uint32 expected_segment_id, uint32 expected_tt_slot_id,
	SCN proposed_scn);
extern const char *cluster_cr_server_test_own_xid_pair_reason(TransactionId xid,
															  uint32 expected_segment_id,
															  uint32 expected_tt_slot_id,
															  SCN proposed_scn,
															  ClusterUndoVerdictResult *out);
extern bool cluster_cr_server_test_pair_detail_admit(uint32 *emitted);
extern bool cluster_cr_server_freshref_c1b_pair_request_decode(
	const GcsBlockForwardPayload *fwd, int32 authenticated_source_node,
	int32 local_node, uint64 current_epoch, int max_backends,
	uint32 *segment_id, TransactionId *xid, uint32 *expected_tt_slot_id,
	SCN *proposed_scn);
extern ClusterUndoVerdictKind cluster_cr_server_freshref_c1b_pair_verdict(
	bool pair_request, bool xid_is_mine, uint32 expected_segment_id,
	uint32 expected_tt_slot_id, bool no_raw_reuse_window, XidStatus raw_status,
	ClusterTTDurableResolve resolve, uint16 matched_segment, uint16 matched_slot,
	SCN resolved_scn, SCN proposed_scn, bool retention_ok, SCN horizon_scn);

typedef enum C0TestEvent {
	C0_EV_SCAN = 1,
	C0_EV_NATIVE_LOCK,
	C0_EV_COVERED,
	C0_EV_DISABLED,
	C0_EV_IS_MINE,
	C0_EV_XACT_LOCK,
	C0_EV_CLOG,
	C0_EV_SLRU_LOCK,
	C0_EV_PROCARRAY,
	C0_EV_XACT_UNLOCK,
	C0_EV_NATIVE_UNLOCK,
	C0_EV_RELEASE_ALL,
	C0_EV_RETENTION,
	C0_EV_BOUND
} C0TestEvent;

static C0TestEvent c0_events[32];
static int c0_event_count;
static ClusterTTDurableResolve c0_resolve;
static SCN c0_resolved_scn;
static SCN c0_horizon_scn;
static uint16 c0_matched_segment;
static uint16 c0_matched_slot;
static bool c0_xid_is_mine;
static uint64 c0_covered_hw;
static bool c0_disabled;
static bool c0_retention_ok;
static bool c0_reader_pending;
static uint64 c0_ungated_recycles;
int cluster_node_id = 0;
bool cluster_undo_retention_horizon_enabled = true;
static bool c0_did_commit;
static bool c0_commit_after_procarray;
static bool c0_durable_drift_after_procarray;
static bool c0_did_abort;
static bool c0_procarray_live;
static bool c0_expect_procarray_under_native_lock;
static bool c0_accept_resolved_scn;
static XidStatus c0_raw_status;
static bool c0_throw_on_clog;
static bool c0_disable_before_native_recheck;
static int c0_native_lock_depth;
static int c0_xact_lock_depth;
static int c0_slru_lock_depth;
static int c0_release_all_calls;
static int c0_raw_clog_calls;
static int c0_procarray_calls;
static int c0_did_commit_calls;
static int c0_did_abort_calls;
static int c0_retention_calls;
static int c0_bound_calls;
static int c0_native_provable_calls;

static void
c0_note(C0TestEvent event)
{
	UT_ASSERT(c0_event_count < (int)lengthof(c0_events));
	if (c0_event_count < (int)lengthof(c0_events))
		c0_events[c0_event_count++] = event;
}

static int
c0_event_pos(C0TestEvent event)
{
	for (int i = 0; i < c0_event_count; i++) {
		if (c0_events[i] == event)
			return i;
	}
	return -1;
}

static int
c0_event_pos_after(C0TestEvent event, int after)
{
	for (int i = after + 1; i < c0_event_count; i++) {
		if (c0_events[i] == event)
			return i;
	}
	return -1;
}

static void
c0_reset(void)
{
	memset(c0_events, 0, sizeof(c0_events));
	c0_event_count = 0;
	c0_resolve = CLUSTER_TT_DURABLE_RECYCLED_ZERO_MATCH;
	c0_resolved_scn = InvalidScn;
	c0_horizon_scn = (SCN)100;
	c0_matched_segment = 0;
	c0_matched_slot = 0;
	c0_xid_is_mine = true;
	c0_covered_hw = UINT64_C(816); /* armed-drain witness, NOT an xid bound */
	c0_disabled = false;
	c0_retention_ok = false; /* exact RED: old zero-match abort cannot pass */
	c0_reader_pending = false;
	c0_ungated_recycles = 0;
	cluster_node_id = 0;
	cluster_undo_retention_horizon_enabled = true;
	c0_did_commit = false;
	c0_commit_after_procarray = false;
	c0_durable_drift_after_procarray = false;
	c0_did_abort = false;
	c0_procarray_live = false;
	c0_expect_procarray_under_native_lock = true;
	c0_accept_resolved_scn = false;
	c0_raw_status = TRANSACTION_STATUS_IN_PROGRESS;
	c0_throw_on_clog = false;
	c0_disable_before_native_recheck = false;
	c0_native_lock_depth = 0;
	c0_xact_lock_depth = 0;
	c0_slru_lock_depth = 0;
	c0_release_all_calls = 0;
	c0_raw_clog_calls = 0;
	c0_procarray_calls = 0;
	c0_did_commit_calls = 0;
	c0_did_abort_calls = 0;
	c0_retention_calls = 0;
	c0_bound_calls = 0;
	c0_native_provable_calls = 0;
	InterruptHoldoffCount = 0;
	memset(&c0_variable_cache, 0, sizeof(c0_variable_cache));
	c0_variable_cache.oldestClogXid = FirstNormalTransactionId;
}

ClusterTTDurableResolve
cluster_tt_slot_durable_resolve_by_xid(TransactionId xid pg_attribute_unused(),
									   uint32 expected_wrap pg_attribute_unused(), SCN *commit_scn,
									   uint16 *out_seg, uint16 *out_slot, uint16 *out_wrap)
{
	c0_note(C0_EV_SCAN);
	if (commit_scn != NULL)
		*commit_scn = c0_resolved_scn;
	if (out_seg != NULL)
		*out_seg = c0_matched_segment;
	if (out_slot != NULL)
		*out_slot = c0_matched_slot
					+ ((c0_durable_drift_after_procarray && c0_procarray_calls > 0) ? 1 : 0);
	if (out_wrap != NULL)
		*out_wrap = 0;
	return c0_resolve;
}

bool
TransactionIdDidCommit(TransactionId xid pg_attribute_unused())
{
	c0_did_commit_calls++;
	return c0_did_commit || (c0_commit_after_procarray && c0_procarray_calls > 0);
}

bool
TransactionIdDidAbort(TransactionId xid pg_attribute_unused())
{
	c0_did_abort_calls++;
	return c0_did_abort;
}

bool
TransactionIdIsInProgress(TransactionId xid pg_attribute_unused())
{
	UT_ASSERT_EQ(c0_native_lock_depth,
				 c0_expect_procarray_under_native_lock ? 1 : 0);
	UT_ASSERT_EQ(c0_xact_lock_depth, 0);
	c0_procarray_calls++;
	c0_note(C0_EV_PROCARRAY);
	return c0_procarray_live;
}

XidStatus
TransactionIdGetStatus(TransactionId xid pg_attribute_unused(), XLogRecPtr *lsn)
{
	c0_raw_clog_calls++;
	c0_note(C0_EV_CLOG);
	if (lsn != NULL)
		*lsn = InvalidXLogRecPtr;
	if (c0_throw_on_clog) {
		/* Model the SLRU ControlLock that the physical-read ERROR leaves held. */
		c0_note(C0_EV_SLRU_LOCK);
		c0_slru_lock_depth++;
		InterruptHoldoffCount++;
		/* ERROR resets the interrupt holdoff before longjmp (elog.c). */
		InterruptHoldoffCount = 0;
		UT_ASSERT_NOT_NULL(PG_exception_stack);
		if (PG_exception_stack != NULL)
			siglongjmp(*PG_exception_stack, 1);
		abort();
	}
	return c0_raw_status;
}

bool
TransactionIdPrecedes(TransactionId id1, TransactionId id2)
{
	UT_ASSERT_EQ(c0_xact_lock_depth, 1);
	return (int32)(id1 - id2) < 0;
}

bool
cluster_xid_is_mine(TransactionId xid pg_attribute_unused())
{
	c0_note(C0_EV_IS_MINE);
	return c0_xid_is_mine;
}

uint64
cluster_cr_native_prehistory_covered_hw(void)
{
	c0_note(C0_EV_COVERED);
	return c0_covered_hw;
}

bool
cluster_cr_native_prehistory_disabled(void)
{
	c0_note(C0_EV_DISABLED);
	return c0_disabled;
}

void
cluster_cr_native_prehistory_reader_lock(void)
{
	c0_note(C0_EV_NATIVE_LOCK);
	c0_native_lock_depth++;
	InterruptHoldoffCount++;
	if (c0_disable_before_native_recheck) {
		/* Model DISABLE winning after the unlocked prefilter but before SHARED. */
		c0_disabled = true;
		c0_covered_hw = 0;
	}
}

void
cluster_cr_native_prehistory_reader_unlock(void)
{
	c0_note(C0_EV_NATIVE_UNLOCK);
	UT_ASSERT(c0_native_lock_depth > 0);
	UT_ASSERT(InterruptHoldoffCount > 0);
	c0_native_lock_depth--;
	InterruptHoldoffCount--;
}

bool
cluster_cr_retention_proof_origin_legs(SCN *out_horizon)
{
	SCN current = c0_retention_ok ? c0_horizon_scn : InvalidScn;
	bool valid;

	c0_retention_calls++;
	c0_note(C0_EV_RETENTION);
	if (c0_reader_pending)
		current = cluster_undo_retention_sample_min(current, 4199120, InvalidScn);
	valid = cluster_node_id >= 0 && cluster_undo_retention_horizon_enabled
			&& c0_ungated_recycles == 0 && SCN_VALID(current);
	if (out_horizon != NULL)
		*out_horizon = valid ? current : InvalidScn;
	return valid;
}

uint64
cluster_tt_slot_retention_off_recycle_count(void)
{
	return c0_ungated_recycles;
}

SCN
cluster_tt_slot_max_recycle_horizon(void)
{
	c0_bound_calls++;
	c0_note(C0_EV_BOUND);
	return c0_retention_ok ? c0_horizon_scn : InvalidScn;
}

bool
cluster_cr_accept_resolved_scn(SCN scn pg_attribute_unused())
{
	return c0_accept_resolved_scn;
}

int
scn_time_cmp(SCN a, SCN b)
{
	return a < b ? -1 : a > b ? 1 : 0;
}

bool
cluster_xid_native_prehistory_provable_full(uint64 next_full_xid pg_attribute_unused(),
										uint64 covered_hw_full pg_attribute_unused(),
										TransactionId xid pg_attribute_unused())
{
	c0_native_provable_calls++;
	return false;
}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode pg_attribute_unused())
{
	UT_ASSERT(lock == XactTruncationLock);
	c0_note(C0_EV_XACT_LOCK);
	c0_xact_lock_depth++;
	InterruptHoldoffCount++;
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	UT_ASSERT(lock == XactTruncationLock);
	c0_note(C0_EV_XACT_UNLOCK);
	UT_ASSERT(c0_xact_lock_depth > 0);
	UT_ASSERT(InterruptHoldoffCount > 0);
	c0_xact_lock_depth--;
	InterruptHoldoffCount--;
}

void
LWLockReleaseAll(void)
{
	c0_release_all_calls++;
	c0_note(C0_EV_RELEASE_ALL);
	UT_ASSERT_EQ(c0_native_lock_depth, 1);
	UT_ASSERT_EQ(c0_xact_lock_depth, 1);
	UT_ASSERT_EQ(c0_slru_lock_depth, 1);
	/* ReleaseAll preserves its caller's interrupt holdoff level. */
	UT_ASSERT_EQ((int)InterruptHoldoffCount, 1);
	c0_slru_lock_depth = 0;
	c0_xact_lock_depth = 0;
	c0_native_lock_depth = 0;
}

void
pg_re_throw(void)
{
	UT_ASSERT_NOT_NULL(PG_exception_stack);
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

static char *
read_cr_server_source(void)
{
	const char *source_file = __FILE__;
	const char *source_suffix = "/src/test/cluster_unit/test_cluster_cr_server_policy.c";
	const char *suffix_at = strstr(source_file, source_suffix);
	char path[MAXPGPATH];
	FILE *file;
	long length;
	char *source;

	if (suffix_at != NULL)
		snprintf(path, sizeof(path), "%.*s/src/backend/cluster/cluster_cr_server.c",
				 (int)(suffix_at - source_file), source_file);
	else
		snprintf(path, sizeof(path), "../../../src/backend/cluster/cluster_cr_server.c");

	file = fopen(path, "rb");
	UT_ASSERT_NOT_NULL(file);
	if (file == NULL)
		return NULL;
	UT_ASSERT_EQ(fseek(file, 0, SEEK_END), 0);
	length = ftell(file);
	UT_ASSERT(length > 0);
	UT_ASSERT_EQ(fseek(file, 0, SEEK_SET), 0);
	source = malloc((size_t)length + 1);
	UT_ASSERT_NOT_NULL(source);
	if (source == NULL) {
		fclose(file);
		return NULL;
	}
	UT_ASSERT_EQ(fread(source, 1, (size_t)length, file), (size_t)length);
	source[length] = '\0';
	fclose(file);
	return source;
}

/* Assert hook stub so the cassert libpgport links standalone. */
void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

UT_TEST(test_split_empty_is_full_prefix_zero)
{
	int prefix = -1;

	UT_ASSERT_EQ((int)cluster_cr_server_split_classify(NULL, 0, 0, &prefix),
				 (int)CLUSTER_CR_SPLIT_FULL);
	UT_ASSERT_EQ(prefix, 0);
}

UT_TEST(test_split_all_self_is_full)
{
	int32 origins[3] = { 0, 0, 0 };
	int prefix = -1;

	UT_ASSERT_EQ((int)cluster_cr_server_split_classify(origins, 3, 0, &prefix),
				 (int)CLUSTER_CR_SPLIT_FULL);
	UT_ASSERT_EQ(prefix, 3);
}

UT_TEST(test_split_self_prefix_foreign_suffix_is_partial)
{
	int32 origins[4] = { 0, 0, 1, 1 };
	int prefix = -1;

	UT_ASSERT_EQ((int)cluster_cr_server_split_classify(origins, 4, 0, &prefix),
				 (int)CLUSTER_CR_SPLIT_PARTIAL);
	UT_ASSERT_EQ(prefix, 2);
}

UT_TEST(test_split_all_foreign_is_partial_prefix_zero)
{
	/* Serving nothing is still a legal one-hop reply: the shipped current
	 * copy lets the requester do the whole peel (equivalent to a plain
	 * read image + local construction). */
	int32 origins[2] = { 1, 1 };
	int prefix = -1;

	UT_ASSERT_EQ((int)cluster_cr_server_split_classify(origins, 2, 0, &prefix),
				 (int)CLUSTER_CR_SPLIT_PARTIAL);
	UT_ASSERT_EQ(prefix, 0);
}

UT_TEST(test_split_interleave_is_deny)
{
	int32 self_after_foreign[3] = { 0, 1, 0 };
	int32 foreign_then_self[2] = { 1, 0 };
	int prefix = 77;

	UT_ASSERT_EQ((int)cluster_cr_server_split_classify(self_after_foreign, 3, 0, &prefix),
				 (int)CLUSTER_CR_SPLIT_DENY);
	UT_ASSERT_EQ((int)cluster_cr_server_split_classify(foreign_then_self, 2, 0, NULL),
				 (int)CLUSTER_CR_SPLIT_DENY);
}

UT_TEST(test_split_third_party_suffix_stays_partial)
{
	/* A >=3-node foreign suffix mixing OTHER nodes is still a clean
	 * self-prefix cut here; the REQUESTER's continue-run hits its own
	 * class-(3) walk backstop for the third-party chains (53R9G). */
	int32 origins[3] = { 0, 1, 2 };
	int prefix = -1;

	UT_ASSERT_EQ((int)cluster_cr_server_split_classify(origins, 3, 0, &prefix),
				 (int)CLUSTER_CR_SPLIT_PARTIAL);
	UT_ASSERT_EQ(prefix, 1);
}

UT_TEST(test_split_malformed_is_deny)
{
	UT_ASSERT_EQ((int)cluster_cr_server_split_classify(NULL, 2, 0, NULL),
				 (int)CLUSTER_CR_SPLIT_DENY);
	UT_ASSERT_EQ((int)cluster_cr_server_split_classify(NULL, -1, 0, NULL),
				 (int)CLUSTER_CR_SPLIT_DENY);
}

/*
 * spec-7.1 D1 serve: the INVALID_SCN verdict decision.  An explicit CLOG abort
 * upgrades to a positive ABORTED (the aborted-unstamped delayed-cleanout
 * window); everything else -- committed-but-unstamped, in-flight, 2PC, crashed
 * -- must stay REFUSE (8.A: never fabricate a commit_scn / positive answer).
 */
UT_TEST(test_invalid_scn_aborted_is_positive)
{
	UT_ASSERT_EQ((int)cluster_cr_server_invalid_scn_verdict(true),
				 (int)CLUSTER_CR_INVALID_SCN_ABORTED);
}

UT_TEST(test_invalid_scn_not_aborted_refuses)
{
	/* The 8.A tooth: a NON-abort (committed-unstamped / in-flight / in-doubt)
	 * must NOT upgrade -- it stays fail-closed on the refuse leg. */
	UT_ASSERT_EQ((int)cluster_cr_server_invalid_scn_verdict(false),
				 (int)CLUSTER_CR_INVALID_SCN_REFUSE);
}

/*
 * TT-P013-RULE25-B: split the current LMS_OWN_XID_REFUSE_OTHER bucket at the
 * exact RESOLVED_SCN positive-proof branches.  The origin's own ProcArray is
 * live authority, CLOG is terminal authority, and an unproved non-commit
 * remains UNKNOWN.
 */
UT_TEST(test_resolved_scn_live_xid_is_in_progress_not_other)
{
	UT_ASSERT_EQ((int)cluster_cr_server_resolved_scn_verdict(false, false, true),
				 (int)CLUSTER_UNDO_VERDICT_IN_PROGRESS);
}

UT_TEST(test_resolved_scn_terminal_and_unknown_boundaries)
{
	UT_ASSERT_EQ((int)cluster_cr_server_resolved_scn_verdict(true, false, false),
				 (int)CLUSTER_UNDO_VERDICT_COMMITTED_EXACT);
	UT_ASSERT_EQ((int)cluster_cr_server_resolved_scn_verdict(true, true, true),
				 (int)CLUSTER_UNDO_VERDICT_COMMITTED_EXACT);
	UT_ASSERT_EQ((int)cluster_cr_server_resolved_scn_verdict(false, false, false),
				 (int)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
}

UT_TEST(test_resolved_scn_live_requires_every_exact_binding_gate)
{
	/* matched_slot is allocator-internal 0-based; wire slot id is 1-based. */
	UT_ASSERT(cluster_cr_server_live_binding_exact(true, 17, 4, 17, 3, true, true));
	UT_ASSERT(!cluster_cr_server_live_binding_exact(false, 17, 4, 17, 3, true, true));
	UT_ASSERT(!cluster_cr_server_live_binding_exact(true, 18, 4, 17, 3, true, true));
	UT_ASSERT(!cluster_cr_server_live_binding_exact(true, 17, 5, 17, 3, true, true));
	UT_ASSERT(!cluster_cr_server_live_binding_exact(true, 17, 0, 17, 3, true, true));
	UT_ASSERT(!cluster_cr_server_live_binding_exact(true, 17, 49, 17, 3, true, true));
	UT_ASSERT(!cluster_cr_server_live_binding_exact(true, 17, 4, 17, 3, false, true));
	UT_ASSERT(!cluster_cr_server_live_binding_exact(true, 17, 4, 17, 3, true, false));
}

/*
 * The durable RESOLVED_SCN stamp precedes the CLOG terminal record.  Once
 * the exact origin binding has an explicit abort, ABORTED must win before
 * the unproved UNKNOWN leg; crash-lost/in-doubt remains fail-closed.
 */
UT_TEST(test_resolved_scn_explicit_abort_after_stamp_is_positive)
{
	char *source = read_cr_server_source();
	const char *resolved
		= source != NULL ? strstr(source, "case CLUSTER_TT_DURABLE_RESOLVED_SCN:") : NULL;
	const char *resolved_end
		= resolved != NULL ? strstr(resolved, "if (cluster_cr_accept_resolved_scn(scn))") : NULL;
	const char *exact_gate
		= resolved != NULL ? strstr(resolved, "if (exact_binding)") : NULL;
	const char *did_abort
		= exact_gate != NULL ? strstr(exact_gate, "TransactionIdDidAbort(xid)") : NULL;
	const char *abort_verdict
		= did_abort != NULL ? strstr(did_abort, "CLUSTER_UNDO_VERDICT_ABORTED") : NULL;
	const char *unknown
		= abort_verdict != NULL
			  ? strstr(abort_verdict, "CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED")
			  : NULL;

	UT_ASSERT_EQ((int)cluster_cr_server_resolved_scn_verdict(false, true, false),
				 (int)CLUSTER_UNDO_VERDICT_ABORTED);
	UT_ASSERT_EQ((int)cluster_cr_server_resolved_scn_verdict(false, false, false),
				 (int)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);

	UT_ASSERT_NOT_NULL(resolved);
	UT_ASSERT_NOT_NULL(resolved_end);
	UT_ASSERT_NOT_NULL(exact_gate);
	UT_ASSERT_NOT_NULL(did_abort);
	UT_ASSERT_NOT_NULL(abort_verdict);
	UT_ASSERT_NOT_NULL(unknown);
	if (resolved_end != NULL) {
		UT_ASSERT(exact_gate != NULL && exact_gate < resolved_end);
		UT_ASSERT(did_abort != NULL && did_abort < resolved_end);
		UT_ASSERT(abort_verdict != NULL && abort_verdict < resolved_end);
		UT_ASSERT(unknown != NULL && unknown < resolved_end);
	}
	free(source);
}

/*
 * S8 happy-path regression: the origin may publish CLOG terminal state after
 * the first DidCommit sample but before the following ProcArray sample.  The
 * same verdict request must re-sample terminal authority and revalidate the
 * exact durable segment/slot/scn binding instead of observing neither LIVE
 * nor COMMITTED and returning a spurious UNKNOWN.
 */
UT_TEST(test_resolved_scn_commit_between_clog_and_procarray_rechecks_exact_binding)
{
	ClusterUndoVerdictKind kind;
	int first_scan;
	int second_scan;

	c0_reset();
	c0_resolve = CLUSTER_TT_DURABLE_RESOLVED_SCN;
	c0_resolved_scn = (SCN)10498;
	c0_matched_segment = 7;
	c0_matched_slot = 0;
	c0_commit_after_procarray = true;
	c0_expect_procarray_under_native_lock = false;
	c0_accept_resolved_scn = true;

	kind = cluster_cr_server_test_own_xid_verdict(4195138, 7, 1, true);
	UT_ASSERT_EQ((int)kind, (int)CLUSTER_UNDO_VERDICT_COMMITTED_EXACT);
	UT_ASSERT_EQ(c0_did_commit_calls, 2);
	UT_ASSERT_EQ(c0_did_abort_calls, 1);
	UT_ASSERT_EQ(c0_procarray_calls, 1);
	first_scan = c0_event_pos(C0_EV_SCAN);
	second_scan = c0_event_pos_after(C0_EV_SCAN, first_scan);
	UT_ASSERT(first_scan >= 0);
	UT_ASSERT(second_scan > first_scan);
	UT_ASSERT(c0_event_pos(C0_EV_PROCARRAY) < second_scan);

	/* A slot drift during the terminal re-sample invalidates the whole proof. */
	c0_reset();
	c0_resolve = CLUSTER_TT_DURABLE_RESOLVED_SCN;
	c0_resolved_scn = (SCN)10498;
	c0_matched_segment = 7;
	c0_matched_slot = 0;
	c0_commit_after_procarray = true;
	c0_durable_drift_after_procarray = true;
	c0_expect_procarray_under_native_lock = false;
	c0_accept_resolved_scn = true;
	kind = cluster_cr_server_test_own_xid_verdict(4195138, 7, 1, true);
	UT_ASSERT_EQ((int)kind, (int)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ(c0_did_commit_calls, 2);
}

/*
 * TT-P013-RULE25-C0 RED: pure truth table for the only positive zero-match
 * widening.  covered_hw is represented as a boolean armed-window input; it
 * must never be compared numerically with the cluster-era xid.
 */
UT_TEST(test_c0_zero_match_positive_proof_table)
{
	UT_ASSERT_EQ((int)cluster_cr_server_c0_zero_match_verdict(
					  true, true, 1, 1, true, false, true, false, false),
				 (int)CLUSTER_UNDO_VERDICT_ABORTED);
	UT_ASSERT_EQ((int)cluster_cr_server_c0_zero_match_verdict(
					  true, true, 1, 1, true, false, false, true, true),
				 (int)CLUSTER_UNDO_VERDICT_IN_PROGRESS);

	/* Commit-without-SCN, SUB_COMMITTED/unknown and contradictory samples. */
	UT_ASSERT_EQ((int)cluster_cr_server_c0_zero_match_verdict(
					  true, true, 1, 1, true, true, false, false, false),
				 (int)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ((int)cluster_cr_server_c0_zero_match_verdict(
					  true, true, 1, 1, true, false, false, false, false),
				 (int)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ((int)cluster_cr_server_c0_zero_match_verdict(
					  true, true, 1, 1, true, false, true, false, true),
				 (int)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);

	/* Every carrier/no-reuse gate is fail-closed. */
	UT_ASSERT_EQ((int)cluster_cr_server_c0_zero_match_verdict(
					  false, true, 1, 1, true, false, true, false, false),
				 (int)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ((int)cluster_cr_server_c0_zero_match_verdict(
					  true, false, 1, 1, true, false, true, false, false),
				 (int)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ((int)cluster_cr_server_c0_zero_match_verdict(
					  true, true, 0, 1, true, false, true, false, false),
				 (int)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ((int)cluster_cr_server_c0_zero_match_verdict(
					  true, true, UINT32_C(65536), 1, true, false, true, false, false),
				 (int)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ((int)cluster_cr_server_c0_zero_match_verdict(
					  true, true, 1, 0, true, false, true, false, false),
				 (int)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ((int)cluster_cr_server_c0_zero_match_verdict(
					  true, true, 1, 49, true, false, true, false, false),
				 (int)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ((int)cluster_cr_server_c0_zero_match_verdict(
					  true, true, 1, 1, false, false, true, false, false),
				 (int)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
}

/* S8-815PRE-FRESHREF-C1B-01 RED: a retained page SCN becomes exact only when
 * the pair discriminator, origin C1b, native no-reuse fence and either exact
 * live TT binding or horizon-covered zero-match all agree.  It never returns
 * COMMITTED_BOUND. */
UT_TEST(test_freshref_c1b_pair_exact_truth_table)
{
	const SCN proposed = (SCN) 10498;

	UT_ASSERT_EQ((int) cluster_cr_server_freshref_c1b_pair_verdict(
					  true, true, 7, 1, true, TRANSACTION_STATUS_COMMITTED,
					  CLUSTER_TT_DURABLE_RESOLVED_SCN, 7, 0, proposed, proposed,
					  false, InvalidScn),
				 (int) CLUSTER_UNDO_VERDICT_COMMITTED_EXACT);
	UT_ASSERT_EQ((int) cluster_cr_server_freshref_c1b_pair_verdict(
					  true, true, 7, 1, true, TRANSACTION_STATUS_COMMITTED,
					  CLUSTER_TT_DURABLE_RECYCLED_ZERO_MATCH, 0, 0, InvalidScn,
					  proposed, true, proposed),
				 (int) CLUSTER_UNDO_VERDICT_COMMITTED_EXACT);

	/* Pair identity, no-reuse and literal origin CLOG are all mandatory. */
	UT_ASSERT_EQ((int) cluster_cr_server_freshref_c1b_pair_verdict(
					  false, true, 7, 1, true, TRANSACTION_STATUS_COMMITTED,
					  CLUSTER_TT_DURABLE_RECYCLED_ZERO_MATCH, 0, 0, InvalidScn,
					  proposed, true, proposed),
				 (int) CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ((int) cluster_cr_server_freshref_c1b_pair_verdict(
					  true, true, 7, 1, false, TRANSACTION_STATUS_COMMITTED,
					  CLUSTER_TT_DURABLE_RECYCLED_ZERO_MATCH, 0, 0, InvalidScn,
					  proposed, true, proposed),
				 (int) CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ((int) cluster_cr_server_freshref_c1b_pair_verdict(
					  true, true, 7, 1, true, TRANSACTION_STATUS_ABORTED,
					  CLUSTER_TT_DURABLE_RECYCLED_ZERO_MATCH, 0, 0, InvalidScn,
					  proposed, true, proposed),
				 (int) CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);

	/* Live match requires exact segment, 1-based slot and SCN. */
	UT_ASSERT_EQ((int) cluster_cr_server_freshref_c1b_pair_verdict(
					  true, true, 7, 1, true, TRANSACTION_STATUS_COMMITTED,
					  CLUSTER_TT_DURABLE_RESOLVED_SCN, 8, 0, proposed, proposed,
					  false, InvalidScn),
				 (int) CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ((int) cluster_cr_server_freshref_c1b_pair_verdict(
					  true, true, 7, 1, true, TRANSACTION_STATUS_COMMITTED,
					  CLUSTER_TT_DURABLE_RESOLVED_SCN, 7, 1, proposed, proposed,
					  false, InvalidScn),
				 (int) CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ((int) cluster_cr_server_freshref_c1b_pair_verdict(
					  true, true, 7, 1, true, TRANSACTION_STATUS_COMMITTED,
					  CLUSTER_TT_DURABLE_RESOLVED_SCN, 7, 0, proposed + 1, proposed,
					  false, InvalidScn),
				 (int) CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);

	/* Recycled zero-match requires retention and proposed <= frozen horizon. */
	UT_ASSERT_EQ((int) cluster_cr_server_freshref_c1b_pair_verdict(
					  true, true, 7, 1, true, TRANSACTION_STATUS_COMMITTED,
					  CLUSTER_TT_DURABLE_RECYCLED_ZERO_MATCH, 0, 0, InvalidScn,
					  proposed, false, proposed),
				 (int) CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ((int) cluster_cr_server_freshref_c1b_pair_verdict(
					  true, true, 7, 1, true, TRANSACTION_STATUS_COMMITTED,
					  CLUSTER_TT_DURABLE_RECYCLED_ZERO_MATCH, 0, 0, InvalidScn,
					  proposed, true, proposed - 1),
				 (int) CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ((int) cluster_cr_server_freshref_c1b_pair_verdict(
					  true, true, 7, 1, true, TRANSACTION_STATUS_COMMITTED,
					  CLUSTER_TT_DURABLE_AMBIGUOUS_WRAP, 0, 0, InvalidScn,
					  proposed, true, proposed),
				 (int) CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
}

UT_TEST(test_freshref_c1b_pair_real_resolver_holds_no_reuse_through_c1b)
{
	ClusterUndoVerdictResult result;
	const SCN proposed = (SCN)10498;

	/* Recycled terminal: literal CLOG + retention horizon echo exact page SCN. */
	c0_reset();
	c0_retention_ok = true;
	c0_horizon_scn = proposed + 2;
	c0_raw_status = TRANSACTION_STATUS_COMMITTED;
	result = cluster_cr_server_test_own_xid_pair_verdict(4195136, 7, 1, proposed);
	UT_ASSERT_EQ((int)result.kind, (int)CLUSTER_UNDO_VERDICT_COMMITTED_EXACT);
	UT_ASSERT_EQ((uint64)result.commit_scn, (uint64)proposed);
	UT_ASSERT_EQ(c0_raw_clog_calls, 1);
	UT_ASSERT_EQ(c0_retention_calls, 0);
	UT_ASSERT_EQ(c0_bound_calls, 1);
	UT_ASSERT_EQ(c0_did_commit_calls, 0);
	UT_ASSERT_EQ(c0_native_lock_depth, 0);
	UT_ASSERT(c0_event_pos(C0_EV_NATIVE_LOCK) < c0_event_pos(C0_EV_CLOG));
	UT_ASSERT(c0_event_pos(C0_EV_CLOG) < c0_event_pos(C0_EV_BOUND));
	UT_ASSERT(c0_event_pos(C0_EV_BOUND) < c0_event_pos(C0_EV_NATIVE_UNLOCK));

	/* Still-bound terminal: exact segment/slot/scn plus the same raw CLOG fence. */
	c0_reset();
	c0_resolve = CLUSTER_TT_DURABLE_RESOLVED_SCN;
	c0_resolved_scn = proposed;
	c0_matched_segment = 7;
	c0_matched_slot = 0;
	c0_raw_status = TRANSACTION_STATUS_COMMITTED;
	result = cluster_cr_server_test_own_xid_pair_verdict(4195136, 7, 1, proposed);
	UT_ASSERT_EQ((int)result.kind, (int)CLUSTER_UNDO_VERDICT_COMMITTED_EXACT);
	UT_ASSERT_EQ((uint64)result.commit_scn, (uint64)proposed);
	UT_ASSERT_EQ(c0_retention_calls, 0);

	/* A clean four-node formation can have no native-era prehistory at all.
	 * For a derivable cluster-era xid, covered_hw is therefore legitimately
	 * zero and is not an admission predicate: stripe ownership plus the
	 * reader-fenced one-way reuse-disable state is the exact no-raw-reuse
	 * proof.  This is the r30 happy-path shape. */
	c0_reset();
	c0_covered_hw = 0;
	c0_retention_ok = true;
	c0_horizon_scn = proposed + 2;
	c0_raw_status = TRANSACTION_STATUS_COMMITTED;
	result = cluster_cr_server_test_own_xid_pair_verdict(
		4195136, 7, 1, proposed);
	UT_ASSERT_EQ((int)result.kind,
				 (int)CLUSTER_UNDO_VERDICT_COMMITTED_EXACT);
	UT_ASSERT_EQ((uint64)result.commit_scn, (uint64)proposed);
	UT_ASSERT_EQ(c0_raw_clog_calls, 1);
	UT_ASSERT_EQ(c0_retention_calls, 0);
	UT_ASSERT_EQ(c0_bound_calls, 1);
	UT_ASSERT(c0_event_pos(C0_EV_NATIVE_LOCK) < c0_event_pos(C0_EV_CLOG));
	UT_ASSERT(c0_event_pos(C0_EV_CLOG) < c0_event_pos(C0_EV_BOUND));
	UT_ASSERT(c0_event_pos(C0_EV_BOUND) < c0_event_pos(C0_EV_NATIVE_UNLOCK));

	/* A one-way reuse-disable race invalidates the whole pair before CLOG. */
	c0_reset();
	c0_retention_ok = true;
	c0_horizon_scn = proposed;
	c0_raw_status = TRANSACTION_STATUS_COMMITTED;
	c0_disable_before_native_recheck = true;
	result = cluster_cr_server_test_own_xid_pair_verdict(4195136, 7, 1, proposed);
	UT_ASSERT_EQ((int)result.kind,
				 (int)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ(c0_raw_clog_calls, 0);
	UT_ASSERT_EQ(c0_retention_calls, 0);
	UT_ASSERT_EQ(c0_native_lock_depth, 0);
}

UT_TEST(test_freshref_c1b_pair_request_canonical_decode)
{
	GcsBlockForwardPayload fwd;
	uint32 segment = 0;
	uint32 slot = 0;
	TransactionId xid = InvalidTransactionId;
	SCN proposed = InvalidScn;

	memset(&fwd, 0, sizeof(fwd));
	fwd.request_id = 77;
	fwd.epoch = 11;
	fwd.tag = GcsBlockUndoFreshRefC1bTagMake(7, (TransactionId)4195136, 1);
	fwd.original_requester_node = 1;
	fwd.requester_backend_id = 4;
	fwd.master_node = 1;
	fwd.transition_id = (uint8)PCM_TRANS_N_TO_S;
	GcsBlockForwardPayloadSetExpectedPiWatermarkScn(&fwd, (SCN)10498);
	GcsBlockForwardPayloadSetUndoFreshRefC1bPairRequest(&fwd);

	UT_ASSERT(cluster_cr_server_freshref_c1b_pair_request_decode(
		&fwd, 1, 0, 11, 8, &segment, &xid, &slot, &proposed));
	UT_ASSERT_EQ(segment, 7);
	UT_ASSERT_EQ(xid, (TransactionId)4195136);
	UT_ASSERT_EQ(slot, 1);
	UT_ASSERT_EQ((uint64)proposed, UINT64_C(10498));

	/* Decode is staging-only.  Exact epoch zero is syntactically canonical;
	 * the runtime requester/origin pair gate must additionally hold a current
	 * homogeneous R4 TARGET admission before it can send or consume it. */
	fwd.epoch = 0;
	UT_ASSERT(cluster_cr_server_freshref_c1b_pair_request_decode(
		&fwd, 1, 0, 0, 8, NULL, NULL, NULL, NULL));
	fwd.epoch = 11;

	/* Every transport/canonical identity axis independently fails closed. */
	fwd.epoch = 12;
	UT_ASSERT(!cluster_cr_server_freshref_c1b_pair_request_decode(
		&fwd, 1, 0, 11, 8, NULL, NULL, NULL, NULL));
	fwd.epoch = 11;
	UT_ASSERT(!cluster_cr_server_freshref_c1b_pair_request_decode(
		&fwd, 2, 0, 11, 8, NULL, NULL, NULL, NULL));
	fwd.master_node = 2;
	UT_ASSERT(!cluster_cr_server_freshref_c1b_pair_request_decode(
		&fwd, 1, 0, 11, 8, NULL, NULL, NULL, NULL));
	fwd.master_node = 1;
	fwd.requester_backend_id = 9;
	UT_ASSERT(!cluster_cr_server_freshref_c1b_pair_request_decode(
		&fwd, 1, 0, 11, 8, NULL, NULL, NULL, NULL));
	fwd.requester_backend_id = 4;
	fwd.reserved_0[0] = 1;
	UT_ASSERT(!cluster_cr_server_freshref_c1b_pair_request_decode(
		&fwd, 1, 0, 11, 8, NULL, NULL, NULL, NULL));
	fwd.reserved_0[0] = 0;
	GcsBlockForwardPayloadSetExpectedPiWatermarkScn(&fwd, InvalidScn);
	UT_ASSERT(!cluster_cr_server_freshref_c1b_pair_request_decode(
		&fwd, 1, 0, 11, 8, NULL, NULL, NULL, NULL));
	GcsBlockForwardPayloadSetExpectedPiWatermarkScn(&fwd, (SCN)10498);
	GcsBlockForwardPayloadSetUndoVerdictRequest(&fwd, true);
	UT_ASSERT(!cluster_cr_server_freshref_c1b_pair_request_decode(
		&fwd, 1, 0, 11, 8, NULL, NULL, NULL, NULL));
}

/* Observe the actual resolver, without promoting an old refusal to success. */
UT_TEST(test_freshref_pair_diagnostic_preserves_exact_and_alias_refusal)
{
	ClusterUndoVerdictResult result;
	const char *reason;

	c0_reset();
	c0_raw_status = TRANSACTION_STATUS_COMMITTED;
	c0_retention_ok = true;
	c0_horizon_scn = 10499;
	reason = cluster_cr_server_test_own_xid_pair_reason(4195136, 7, 1, 10498, &result);
	UT_ASSERT(strcmp(reason, "PROVEN") == 0);
	UT_ASSERT_EQ(result.kind, CLUSTER_UNDO_VERDICT_COMMITTED_EXACT);
	UT_ASSERT_EQ(result.commit_scn, 10498);
	UT_ASSERT_EQ(c0_raw_clog_calls, 1);
	UT_ASSERT_EQ(c0_bound_calls, 1);
	UT_ASSERT_EQ(c0_native_lock_depth, 0);

	c0_reset();
	c0_raw_status = TRANSACTION_STATUS_COMMITTED;
	c0_resolve = CLUSTER_TT_DURABLE_RESOLVED_SCN;
	c0_matched_segment = 8;
	c0_matched_slot = 0;
	c0_resolved_scn = 10498;
	reason = cluster_cr_server_test_own_xid_pair_reason(4195136, 7, 1, 10498, &result);
	UT_ASSERT(strcmp(reason, "TT_SEGMENT_MISMATCH") == 0);
	UT_ASSERT_EQ(result.kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ(result.commit_scn, InvalidScn);
	UT_ASSERT_EQ(c0_raw_clog_calls, 1);
	UT_ASSERT_EQ(c0_bound_calls, 0);
	UT_ASSERT_EQ(c0_native_lock_depth, 0);
}

UT_TEST(test_freshref_pair_diagnostic_negative_matrix)
{
	static const char *expected[] = { "BAD_INPUT",
									  "NATIVE_REUSE_GUARD_DISABLED",
									  "ORIGIN_NOT_OWNED",
									  "TT_SCAN_UNAVAILABLE",
									  "TT_AMBIGUOUS",
									  "TT_UNSTAMPED",
									  "CLOG_TRUNCATED",
									  "CLOG_NOT_COMMITTED",
									  "CLOG_NOT_COMMITTED",
									  "CLOG_NOT_COMMITTED",
									  "TT_SEGMENT_MISMATCH",
									  "TT_SLOT_MISMATCH",
									  "TT_SCN_MISMATCH",
									  "RETENTION_UNAVAILABLE",
									  "RETENTION_NOT_COVERED" };
	int i;

	for (i = 0; i < lengthof(expected); i++) {
		ClusterUndoVerdictResult result;
		uint32 segment = 7;
		const char *reason;
		int scans = 0;
		int event;

		c0_reset();
		c0_raw_status = TRANSACTION_STATUS_COMMITTED;
		c0_retention_ok = true;
		c0_horizon_scn = 10499;
		switch (i) {
		case 0:
			segment = 0;
			break;
		case 1:
			c0_disabled = true;
			break;
		case 2:
			c0_xid_is_mine = false;
			break;
		case 3:
			c0_resolve = CLUSTER_TT_DURABLE_SCAN_UNAVAILABLE;
			break;
		case 4:
			c0_resolve = CLUSTER_TT_DURABLE_AMBIGUOUS_WRAP;
			break;
		case 5:
			c0_resolve = CLUSTER_TT_DURABLE_XID_MATCH_INVALID_SCN;
			break;
		case 6:
			c0_variable_cache.oldestClogXid = 4195137;
			break;
		case 7:
			c0_raw_status = TRANSACTION_STATUS_IN_PROGRESS;
			break;
		case 8:
			c0_raw_status = TRANSACTION_STATUS_ABORTED;
			break;
		case 9:
			c0_raw_status = TRANSACTION_STATUS_SUB_COMMITTED;
			break;
		case 10:
		case 11:
		case 12:
			c0_resolve = CLUSTER_TT_DURABLE_RESOLVED_SCN;
			c0_matched_segment = i == 10 ? 8 : 7;
			c0_matched_slot = i == 11 ? 1 : 0;
			c0_resolved_scn = i == 12 ? 10499 : 10498;
			break;
		case 13:
			c0_horizon_scn = InvalidScn;
			break;
		case 14:
			c0_horizon_scn = 10497;
			break;
		}
		reason = cluster_cr_server_test_own_xid_pair_reason(4195136, segment, 1, 10498, &result);
		UT_ASSERT(strcmp(reason, expected[i]) == 0);
		UT_ASSERT_EQ(result.kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
		UT_ASSERT_EQ(result.commit_scn, InvalidScn);
		UT_ASSERT_EQ(c0_raw_clog_calls, i >= 7 ? 1 : 0);
		UT_ASSERT_EQ(c0_native_lock_depth, 0);
		UT_ASSERT_EQ(c0_xact_lock_depth, 0);
		for (event = 0; event < c0_event_count; event++)
			if (c0_events[event] == C0_EV_SCAN)
				scans++;
		UT_ASSERT_EQ(scans, i >= 3 ? 1 : 0);
	}
}

UT_TEST(test_freshref_pair_diagnostic_preserves_clog_exception_cleanup)
{
	volatile bool caught = false;

	c0_reset();
	c0_throw_on_clog = true;
	PG_TRY();
	{
		ClusterUndoVerdictResult result;

		(void)cluster_cr_server_test_own_xid_pair_reason(4195136, 7, 1, 10498, &result);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(c0_raw_clog_calls, 1);
	UT_ASSERT_EQ(c0_release_all_calls, 1);
	UT_ASSERT_EQ(c0_native_lock_depth, 0);
	UT_ASSERT_EQ(c0_xact_lock_depth, 0);
	UT_ASSERT_EQ(c0_slru_lock_depth, 0);
}

UT_TEST(test_freshref_pair_diagnostic_output_budget_is_finite)
{
	uint32 count = 0;
	int i;

	for (i = 0; i < 66; i++)
		UT_ASSERT_EQ(cluster_cr_server_test_pair_detail_admit(&count), i < 64);
	UT_ASSERT_EQ(count, 64);
	count = UINT32_MAX;
	UT_ASSERT(!cluster_cr_server_test_pair_detail_admit(&count));
	UT_ASSERT_EQ(count, UINT32_MAX);
}

/*
 * Execute the real own-xid resolver at the exact formal zero-match shape.
 * retention=false makes both positive rows RED on 34b: the old branch never
 * reached abort before retention and had no live result at all.
 */
UT_TEST(test_c0_real_zero_match_abort_live_and_self_disable)
{
	ClusterUndoVerdictKind kind;
	bool caught = false;

	c0_reset();
	c0_raw_status = TRANSACTION_STATUS_ABORTED;
	kind = cluster_cr_server_test_own_xid_verdict(4195136, 1, 1, true);
	UT_ASSERT_EQ((int)kind, (int)CLUSTER_UNDO_VERDICT_ABORTED);
	UT_ASSERT_EQ(c0_retention_calls, 0);
	UT_ASSERT_EQ(c0_raw_clog_calls, 1);
	UT_ASSERT_EQ(c0_procarray_calls, 0);
	UT_ASSERT_EQ(c0_native_provable_calls, 0);
	UT_ASSERT_EQ(c0_native_lock_depth, 0);
	UT_ASSERT_EQ(c0_xact_lock_depth, 0);

	c0_reset();
	c0_raw_status = TRANSACTION_STATUS_IN_PROGRESS;
	c0_procarray_live = true;
	kind = cluster_cr_server_test_own_xid_verdict(4195136, 1, 1, true);
	UT_ASSERT_EQ((int)kind, (int)CLUSTER_UNDO_VERDICT_IN_PROGRESS);
	UT_ASSERT_EQ(c0_retention_calls, 0);
	UT_ASSERT_EQ(c0_raw_clog_calls, 1);
	UT_ASSERT_EQ(c0_procarray_calls, 1);
	UT_ASSERT(c0_event_pos(C0_EV_SCAN) < c0_event_pos(C0_EV_NATIVE_LOCK));
	UT_ASSERT(c0_event_pos(C0_EV_NATIVE_LOCK)
			  < c0_event_pos_after(C0_EV_COVERED, c0_event_pos(C0_EV_NATIVE_LOCK)));
	UT_ASSERT(c0_event_pos(C0_EV_DISABLED) < c0_event_pos(C0_EV_CLOG));
	UT_ASSERT(c0_event_pos(C0_EV_CLOG) < c0_event_pos(C0_EV_XACT_UNLOCK));
	UT_ASSERT(c0_event_pos(C0_EV_XACT_UNLOCK) < c0_event_pos(C0_EV_PROCARRAY));
	UT_ASSERT(c0_event_pos(C0_EV_PROCARRAY) < c0_event_pos(C0_EV_NATIVE_UNLOCK));
	UT_ASSERT_EQ(c0_native_provable_calls, 0);

	/* One-way disable and an unset boot witness both suppress C0. */
	c0_reset();
	c0_disabled = true;
	c0_raw_status = TRANSACTION_STATUS_ABORTED;
	kind = cluster_cr_server_test_own_xid_verdict(4195136, 1, 1, true);
	UT_ASSERT_EQ((int)kind, (int)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ(c0_raw_clog_calls, 0);
	UT_ASSERT_EQ(c0_procarray_calls, 0);
	UT_ASSERT_EQ(c0_native_lock_depth, 0);

	/* DISABLE wins between the unlocked prefilter and the in-lock recheck. */
	c0_reset();
	c0_disable_before_native_recheck = true;
	c0_raw_status = TRANSACTION_STATUS_ABORTED;
	kind = cluster_cr_server_test_own_xid_verdict(4195136, 1, 1, true);
	UT_ASSERT_EQ((int)kind, (int)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ(c0_raw_clog_calls, 0);
	UT_ASSERT(c0_event_pos(C0_EV_NATIVE_LOCK) >= 0);
	UT_ASSERT(c0_event_pos(C0_EV_NATIVE_LOCK) < c0_event_pos(C0_EV_NATIVE_UNLOCK));
	UT_ASSERT_EQ(c0_native_lock_depth, 0);

	c0_reset();
	c0_covered_hw = 0;
	c0_raw_status = TRANSACTION_STATUS_ABORTED;
	kind = cluster_cr_server_test_own_xid_verdict(4195136, 1, 1, true);
	UT_ASSERT_EQ((int)kind, (int)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ(c0_raw_clog_calls, 0);

	/* Terminal-only slot0 callers never enter the live/abort widening. */
	c0_reset();
	c0_raw_status = TRANSACTION_STATUS_IN_PROGRESS;
	c0_procarray_live = true;
	kind = cluster_cr_server_test_own_xid_verdict(4195136, 0, 0, false);
	UT_ASSERT_EQ((int)kind, (int)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ(c0_raw_clog_calls, 0);
	UT_ASSERT_EQ(c0_native_lock_depth, 0);

	/*
	 * Once C0 sampled a literal byte, only raw COMMITTED may compose with
	 * the unchanged retention-bound path.  Every other non-positive sample
	 * is a hard refusal: never re-enter recursive DidAbort/unguarded CLOG.
	 */
	c0_reset();
	c0_retention_ok = true;
	c0_raw_status = TRANSACTION_STATUS_SUB_COMMITTED;
	c0_did_abort = true; /* old fallback would incorrectly promote this */
	kind = cluster_cr_server_test_own_xid_verdict(4195136, 1, 1, true);
	UT_ASSERT_EQ((int)kind, (int)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ(c0_retention_calls, 0);
	UT_ASSERT_EQ(c0_did_commit_calls, 0);
	UT_ASSERT_EQ(c0_did_abort_calls, 0);

	c0_reset();
	c0_retention_ok = true;
	c0_variable_cache.oldestClogXid = 4195137;
	c0_did_abort = true;
	kind = cluster_cr_server_test_own_xid_verdict(4195136, 1, 1, true);
	UT_ASSERT_EQ((int)kind, (int)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ(c0_raw_clog_calls, 0);
	UT_ASSERT_EQ(c0_retention_calls, 0);
	UT_ASSERT_EQ(c0_did_commit_calls, 0);
	UT_ASSERT_EQ(c0_did_abort_calls, 0);

	c0_reset();
	c0_retention_ok = true;
	c0_raw_status = TRANSACTION_STATUS_IN_PROGRESS;
	c0_procarray_live = false;
	c0_did_abort = true;
	kind = cluster_cr_server_test_own_xid_verdict(4195136, 1, 1, true);
	UT_ASSERT_EQ((int)kind, (int)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ(c0_procarray_calls, 1);
	UT_ASSERT_EQ(c0_retention_calls, 0);
	UT_ASSERT_EQ(c0_did_commit_calls, 0);
	UT_ASSERT_EQ(c0_did_abort_calls, 0);

	c0_reset();
	c0_retention_ok = true;
	c0_raw_status = TRANSACTION_STATUS_COMMITTED;
	c0_did_commit = true;
	kind = cluster_cr_server_test_own_xid_verdict(4195136, 1, 1, true);
	UT_ASSERT_EQ((int)kind, (int)CLUSTER_UNDO_VERDICT_COMMITTED_BOUND);
	UT_ASSERT_EQ(c0_retention_calls, 0);
	UT_ASSERT_EQ(c0_bound_calls, 1);
	UT_ASSERT_EQ(c0_did_commit_calls, 1);
	UT_ASSERT_EQ(c0_did_abort_calls, 0);

	/* ERROR during CLOG read releases SLRU, Xact and native before rethrow. */
	c0_reset();
	c0_throw_on_clog = true;
	PG_TRY(c0_test_outer);
	{
		(void)cluster_cr_server_test_own_xid_verdict(4195136, 1, 1, true);
	}
	PG_CATCH(c0_test_outer);
	{
		caught = true;
	}
	PG_END_TRY(c0_test_outer);
	UT_ASSERT(caught);
	UT_ASSERT_EQ(c0_release_all_calls, 1);
	UT_ASSERT_EQ(c0_native_lock_depth, 0);
	UT_ASSERT_EQ(c0_xact_lock_depth, 0);
	UT_ASSERT_EQ(c0_slru_lock_depth, 0);
	UT_ASSERT_EQ((int)InterruptHoldoffCount, 0);
	UT_ASSERT(c0_event_pos(C0_EV_SLRU_LOCK) < c0_event_pos(C0_EV_RELEASE_ALL));
}

UT_TEST(test_pending_reader_does_not_revoke_recycled_terminal_bound)
{
	ClusterUndoVerdictKind kind;
	SCN current = 1;

	c0_reset();
	c0_retention_ok = true;
	c0_reader_pending = true;
	c0_did_commit = true;
	/* Real A55 sampler preserves the no-new-recycle safety rule. */
	UT_ASSERT(!cluster_cr_retention_proof_origin_legs(&current));
	UT_ASSERT_EQ(current, InvalidScn);
	UT_ASSERT_EQ(cluster_tt_slot_max_recycle_horizon(), c0_horizon_scn);
	c0_event_count = 0;
	c0_retention_calls = 0;
	c0_bound_calls = 0;
	kind = cluster_cr_server_test_own_xid_verdict(4195120, 1, 0, false);
	UT_ASSERT_EQ((int)kind, (int)CLUSTER_UNDO_VERDICT_COMMITTED_BOUND);
	UT_ASSERT_EQ(c0_did_commit_calls, 1);
	UT_ASSERT_EQ(c0_retention_calls, 0);
	UT_ASSERT_EQ(c0_bound_calls, 1);
	UT_ASSERT(c0_event_pos(C0_EV_SCAN) < c0_event_pos(C0_EV_BOUND));
}

UT_TEST(test_pending_reader_does_not_revoke_wrap_suspect_bound)
{
	ClusterUndoVerdictKind kind;

	c0_reset();
	c0_retention_ok = true;
	c0_reader_pending = true;
	c0_did_commit = true;
	c0_resolve = CLUSTER_TT_DURABLE_RESOLVED_SCN;
	c0_resolved_scn = 90;
	c0_matched_segment = 1;
	kind = cluster_cr_server_test_own_xid_verdict(4195120, 1, 0, false);
	UT_ASSERT_EQ((int)kind, (int)CLUSTER_UNDO_VERDICT_COMMITTED_BOUND);
	UT_ASSERT_EQ(c0_retention_calls, 0);
	UT_ASSERT_EQ(c0_bound_calls, 1);
	UT_ASSERT(c0_event_pos(C0_EV_SCAN) < c0_event_pos(C0_EV_BOUND));
}

UT_TEST(test_pending_reader_preserves_exact_freshref_c1b_pair)
{
	ClusterUndoVerdictResult result;

	c0_reset();
	c0_retention_ok = true;
	c0_reader_pending = true;
	c0_raw_status = TRANSACTION_STATUS_COMMITTED;
	result = cluster_cr_server_test_own_xid_pair_verdict(4195120, 7, 1, 90);
	UT_ASSERT_EQ((int)result.kind, (int)CLUSTER_UNDO_VERDICT_COMMITTED_EXACT);
	UT_ASSERT_EQ(result.commit_scn, 90);
	UT_ASSERT_EQ(c0_native_lock_depth, 0);
	UT_ASSERT_EQ(c0_retention_calls, 0);
	UT_ASSERT_EQ(c0_bound_calls, 1);
	UT_ASSERT(c0_event_pos(C0_EV_SCAN) < c0_event_pos(C0_EV_BOUND));
	UT_ASSERT(c0_event_pos(C0_EV_CLOG) < c0_event_pos(C0_EV_BOUND));
	UT_ASSERT(c0_event_pos(C0_EV_BOUND) < c0_event_pos(C0_EV_NATIVE_UNLOCK));
}

UT_TEST(test_historical_bound_keeps_all_terminal_refusal_gates)
{
	/* The three real consumer branches share no success shortcut. */
	for (int branch = 0; branch < 3; branch++) {
		for (int bad = 0; bad < 7; bad++) {
			ClusterUndoVerdictKind kind;

			c0_reset();
			c0_retention_ok = true;
			c0_reader_pending = true;
			c0_did_commit = true;
			c0_raw_status = TRANSACTION_STATUS_COMMITTED;
			if (branch == 1) {
				c0_resolve = CLUSTER_TT_DURABLE_RESOLVED_SCN;
				c0_resolved_scn = 90;
				c0_matched_segment = 7;
			}
			switch (bad) {
			case 0:
				cluster_node_id = -1;
				break;
			case 1:
				cluster_undo_retention_horizon_enabled = false;
				break;
			case 2:
				c0_ungated_recycles = 1;
				break;
			case 3:
				c0_retention_ok = false;
				break;
			case 4:
				c0_resolve = CLUSTER_TT_DURABLE_SCAN_UNAVAILABLE;
				break;
			case 5:
				c0_resolve = CLUSTER_TT_DURABLE_AMBIGUOUS_WRAP;
				break;
			case 6:
				c0_did_commit = false;
				c0_raw_status = TRANSACTION_STATUS_IN_PROGRESS;
				break;
			}
			if (branch == 2) {
				ClusterUndoVerdictResult result
					= cluster_cr_server_test_own_xid_pair_verdict(4195120, 7, 1, 90);
				kind = result.kind;
				UT_ASSERT_EQ(result.commit_scn, InvalidScn);
			} else
				kind = cluster_cr_server_test_own_xid_verdict(4195120, 7, 0, false);
			UT_ASSERT_EQ((int)kind, (int)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
			UT_ASSERT_EQ(c0_native_lock_depth, 0);
		}
	}
	c0_reset();
}

/*
 * D11 R19: UNDO_MULTI_VERDICT remains park-only.  Freeze the defensive inline
 * entry separately: an explicit kind case must jump over both the serve call
 * and inline-serve accounting to the pre-set DENIED one-reply path.
 */
UT_TEST(test_undo_multi_verdict_inline_entry_is_denied_without_serve)
{
	char *source = read_cr_server_source();
	const char *function
		= source != NULL ? strstr(source, "\ncluster_gcs_block_forward_serve_inline(") : NULL;
	const char *function_end = function != NULL ? strstr(function, "\n}\n\n#endif") : NULL;
	const char *kind_switch = function != NULL ? strstr(function, "\tswitch (kind) {") : NULL;
	const char *multi_case
		= kind_switch != NULL
			  ? strstr(kind_switch, "case CLUSTER_LMS_SLOT_KIND_UNDO_MULTI_VERDICT:")
			  : NULL;
	const char *deny_jump
		= multi_case != NULL ? strstr(multi_case, "goto inline_deny_no_serve;") : NULL;
	const char *serve = kind_switch != NULL ? strstr(kind_switch, "cr_serve_slot(&slot);") : NULL;
	const char *inline_note
		= serve != NULL ? strstr(serve, "cluster_lms_obs_note_inline_serve(") : NULL;
	const char *deny_label
		= inline_note != NULL ? strstr(inline_note, "\ninline_deny_no_serve:") : NULL;
	const char *reply
		= deny_label != NULL ? strstr(deny_label, "cr_build_and_send_reply(&slot);") : NULL;
	const char *direct_note
		= reply != NULL ? strstr(reply, "cluster_lms_obs_note_direct_reply();") : NULL;

	UT_ASSERT_NOT_NULL(function);
	UT_ASSERT_NOT_NULL(function_end);
	UT_ASSERT_NOT_NULL(kind_switch);
	UT_ASSERT_NOT_NULL(multi_case);
	UT_ASSERT_NOT_NULL(deny_jump);
	UT_ASSERT_NOT_NULL(serve);
	UT_ASSERT_NOT_NULL(inline_note);
	UT_ASSERT_NOT_NULL(deny_label);
	UT_ASSERT_NOT_NULL(reply);
	UT_ASSERT_NOT_NULL(direct_note);
	if (function != NULL && function_end != NULL && kind_switch != NULL && multi_case != NULL
		&& deny_jump != NULL && serve != NULL && inline_note != NULL && deny_label != NULL
		&& reply != NULL && direct_note != NULL)
		UT_ASSERT(function < kind_switch && kind_switch < multi_case && multi_case < deny_jump
				  && deny_jump < serve && serve < inline_note && inline_note < deny_label
				  && deny_label < reply && reply < direct_note && direct_note < function_end);
	free(source);
}

/*
 * R4 CR-build owns the separate FORWARD96 -> queued worker-0 state machine.
 * A defensive call through the legacy inline entry must therefore take the
 * same pre-set DENIED path without reaching the generic CR constructor.
 */
UT_TEST(test_r4_cr_build_inline_entry_is_denied_without_serve)
{
	char *source = read_cr_server_source();
	const char *function
		= source != NULL ? strstr(source, "\ncluster_gcs_block_forward_serve_inline(") : NULL;
	const char *function_end = function != NULL ? strstr(function, "\n}\n\n#endif") : NULL;
	const char *kind_switch = function != NULL ? strstr(function, "\tswitch (kind) {") : NULL;
	const char *r4_case
		= kind_switch != NULL
			  ? strstr(kind_switch, "case CLUSTER_LMS_SLOT_KIND_R4_CR_BUILD:")
			  : NULL;
	const char *deny_jump
		= r4_case != NULL ? strstr(r4_case, "goto inline_deny_no_serve;") : NULL;
	const char *serve = kind_switch != NULL ? strstr(kind_switch, "cr_serve_slot(&slot);") : NULL;
	const char *deny_label
		= serve != NULL ? strstr(serve, "\ninline_deny_no_serve:") : NULL;
	const char *reply
		= deny_label != NULL ? strstr(deny_label, "cr_build_and_send_reply(&slot);") : NULL;

	UT_ASSERT_NOT_NULL(function);
	UT_ASSERT_NOT_NULL(function_end);
	UT_ASSERT_NOT_NULL(kind_switch);
	UT_ASSERT_NOT_NULL(r4_case);
	UT_ASSERT_NOT_NULL(deny_jump);
	UT_ASSERT_NOT_NULL(serve);
	UT_ASSERT_NOT_NULL(deny_label);
	UT_ASSERT_NOT_NULL(reply);
	if (function != NULL && function_end != NULL && kind_switch != NULL
		&& r4_case != NULL && deny_jump != NULL && serve != NULL
		&& deny_label != NULL && reply != NULL)
		UT_ASSERT(function < kind_switch && kind_switch < r4_case
				  && r4_case < deny_jump && deny_jump < serve
				  && serve < deny_label && deny_label < reply
				  && reply < function_end);
	free(source);
}

/*
 * S8-815PRE-FRESHREF-C1B-01: a status-22 DATA->canonical-TT continuation
 * shares LMS worker 0 with the legacy complete-TT verdict slots.  Serving all
 * four legacy slots in one drain turn can keep the DATA socket undispatched
 * for longer than the requester's unchanged absolute deadline.  Freeze the
 * bounded scheduling contract: progress status-22 first, do no legacy scan
 * while one remains active, and otherwise serve at most one legacy slot from
 * a round-robin cursor before returning to the event loop.
 */
UT_TEST(test_lms_exact_status22_preempts_legacy_tt_scan_convoy)
{
	char *source = read_cr_server_source();
	const char *function
		= source != NULL ? strstr(source, "\ncluster_lms_cr_drain(") : NULL;
	const char *function_end
		= function != NULL
			  ? strstr(function, "\n}\n\n/*\n * cluster_lms_cr_ship_ready")
			  : NULL;
	const char *exact_drain
		= function != NULL
			  ? strstr(function, "cluster_gcs_block_r4_tx_resolve_drain();")
			  : NULL;
	const char *exact_active
		= exact_drain != NULL
			  ? strstr(exact_drain, "cluster_gcs_block_r4_tx_resolve_active()")
			  : NULL;
	const char *cursor
		= exact_active != NULL
			  ? strstr(exact_active, "cluster_lms_cr_legacy_drain_cursor")
			  : NULL;
	const char *single_serve
		= cursor != NULL ? strstr(cursor, "cr_serve_slot(slot);") : NULL;
	const char *advance
		= single_serve != NULL
			  ? strstr(single_serve, "cluster_lms_cr_legacy_drain_cursor =")
			  : NULL;
	const char *bounded_break
		= advance != NULL ? strstr(advance, "break;") : NULL;

	UT_ASSERT_NOT_NULL(function);
	UT_ASSERT_NOT_NULL(function_end);
	UT_ASSERT_NOT_NULL(exact_drain);
	UT_ASSERT_NOT_NULL(exact_active);
	UT_ASSERT_NOT_NULL(cursor);
	UT_ASSERT_NOT_NULL(single_serve);
	UT_ASSERT_NOT_NULL(advance);
	UT_ASSERT_NOT_NULL(bounded_break);
	if (function != NULL && function_end != NULL && exact_drain != NULL
		&& exact_active != NULL && cursor != NULL && single_serve != NULL
		&& advance != NULL && bounded_break != NULL)
		UT_ASSERT(function < exact_drain && exact_drain < exact_active
				  && exact_active < cursor && cursor < single_serve
				  && single_serve < advance && advance < bounded_break
				  && bounded_break < function_end);
	free(source);
}

UT_TEST(test_ordinary_diagnostic_preserves_wrong_carrier_exact_commit)
{
	ClusterUndoVerdictKind kind;
	const char *reason;

	c0_reset();
	c0_resolve = CLUSTER_TT_DURABLE_RESOLVED_SCN;
	c0_resolved_scn = 14780681;
	c0_matched_segment = 33;
	c0_matched_slot = 27;
	c0_did_commit = true;
	c0_accept_resolved_scn = true;
	reason = cluster_cr_server_test_ordinary_reason(4269296, 769, &kind);
	UT_ASSERT_STR_EQ(reason, "COMMITTED_EXACT");
	UT_ASSERT_EQ(kind, CLUSTER_UNDO_VERDICT_COMMITTED_EXACT);
	UT_ASSERT_EQ(c0_did_commit_calls, 1);
	UT_ASSERT_EQ(c0_did_abort_calls, 0);
	UT_ASSERT_EQ(c0_bound_calls, 0);
	UT_ASSERT_EQ(c0_raw_clog_calls, 0);
}

UT_TEST(test_ordinary_diagnostic_keeps_refusals_and_no_extra_reads)
{
	const ClusterTTDurableResolve cases[]
		= { CLUSTER_TT_DURABLE_SCAN_UNAVAILABLE, CLUSTER_TT_DURABLE_AMBIGUOUS_WRAP,
			CLUSTER_TT_DURABLE_XID_MATCH_INVALID_SCN, CLUSTER_TT_DURABLE_RESOLVED_SCN };
	const char *reasons[]
		= { "TT_SCAN_UNAVAILABLE", "TT_AMBIGUOUS", "TT_UNSTAMPED", "CLOG_NOT_TERMINAL" };

	for (int i = 0; i < lengthof(cases); i++) {
		ClusterUndoVerdictKind kind;

		c0_reset();
		c0_resolve = cases[i];
		UT_ASSERT_STR_EQ(cluster_cr_server_test_ordinary_reason(4269296, 769, &kind), reasons[i]);
		UT_ASSERT_EQ(kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
		UT_ASSERT_EQ(c0_raw_clog_calls, 0);
		UT_ASSERT_EQ(c0_procarray_calls, 0);
		UT_ASSERT_EQ(c0_bound_calls, 0);
		UT_ASSERT_EQ(c0_did_commit_calls, i == 3 ? 1 : 0);
		UT_ASSERT_EQ(c0_did_abort_calls, i == 2 ? 1 : 0);
	}
}

UT_TEST(test_ordinary_diagnostic_keeps_recycled_bound_and_refusal)
{
	ClusterUndoVerdictKind kind;

	c0_reset();
	c0_retention_ok = true;
	c0_did_commit = true;
	UT_ASSERT_STR_EQ(cluster_cr_server_test_ordinary_reason(4269296, 769, &kind),
					 "COMMITTED_BOUND");
	UT_ASSERT_EQ(kind, CLUSTER_UNDO_VERDICT_COMMITTED_BOUND);
	UT_ASSERT_EQ(c0_bound_calls, 1);
	UT_ASSERT_EQ(c0_did_commit_calls, 1);
	c0_reset();
	cluster_undo_retention_horizon_enabled = false;
	UT_ASSERT_STR_EQ(cluster_cr_server_test_ordinary_reason(4269296, 769, &kind),
					 "RETENTION_UNAVAILABLE");
	UT_ASSERT_EQ(kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ(c0_did_commit_calls, 0);
	c0_reset();
	c0_retention_ok = true;
	UT_ASSERT_STR_EQ(cluster_cr_server_test_ordinary_reason(4269296, 769, &kind),
					 "ZERO_MATCH_CLOG_UNPROVEN");
	UT_ASSERT_EQ(kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ(c0_bound_calls, 1);
	UT_ASSERT_EQ(c0_did_commit_calls, 1);
	UT_ASSERT_EQ(c0_did_abort_calls, 1);
}

int
main(void)
{
	UT_PLAN(33);
	UT_RUN(test_split_empty_is_full_prefix_zero);
	UT_RUN(test_split_all_self_is_full);
	UT_RUN(test_split_self_prefix_foreign_suffix_is_partial);
	UT_RUN(test_split_all_foreign_is_partial_prefix_zero);
	UT_RUN(test_split_interleave_is_deny);
	UT_RUN(test_split_third_party_suffix_stays_partial);
	UT_RUN(test_split_malformed_is_deny);
	UT_RUN(test_invalid_scn_aborted_is_positive);
	UT_RUN(test_invalid_scn_not_aborted_refuses);
	UT_RUN(test_resolved_scn_live_xid_is_in_progress_not_other);
	UT_RUN(test_resolved_scn_terminal_and_unknown_boundaries);
	UT_RUN(test_resolved_scn_live_requires_every_exact_binding_gate);
	UT_RUN(test_resolved_scn_explicit_abort_after_stamp_is_positive);
	UT_RUN(test_resolved_scn_commit_between_clog_and_procarray_rechecks_exact_binding);
	UT_RUN(test_c0_zero_match_positive_proof_table);
	UT_RUN(test_freshref_c1b_pair_exact_truth_table);
	UT_RUN(test_freshref_c1b_pair_real_resolver_holds_no_reuse_through_c1b);
	UT_RUN(test_freshref_c1b_pair_request_canonical_decode);
	UT_RUN(test_freshref_pair_diagnostic_preserves_exact_and_alias_refusal);
	UT_RUN(test_freshref_pair_diagnostic_negative_matrix);
	UT_RUN(test_freshref_pair_diagnostic_preserves_clog_exception_cleanup);
	UT_RUN(test_freshref_pair_diagnostic_output_budget_is_finite);
	UT_RUN(test_c0_real_zero_match_abort_live_and_self_disable);
	UT_RUN(test_pending_reader_does_not_revoke_recycled_terminal_bound);
	UT_RUN(test_pending_reader_does_not_revoke_wrap_suspect_bound);
	UT_RUN(test_pending_reader_preserves_exact_freshref_c1b_pair);
	UT_RUN(test_historical_bound_keeps_all_terminal_refusal_gates);
	UT_RUN(test_undo_multi_verdict_inline_entry_is_denied_without_serve);
	UT_RUN(test_r4_cr_build_inline_entry_is_denied_without_serve);
	UT_RUN(test_lms_exact_status22_preempts_legacy_tt_scan_convoy);
	UT_RUN(test_ordinary_diagnostic_preserves_wrong_carrier_exact_commit);
	UT_RUN(test_ordinary_diagnostic_keeps_refusals_and_no_extra_reads);
	UT_RUN(test_ordinary_diagnostic_keeps_recycled_bound_and_refusal);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
