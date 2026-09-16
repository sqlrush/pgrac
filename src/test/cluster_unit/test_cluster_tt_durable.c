/*-------------------------------------------------------------------------
 *
 * test_cluster_tt_durable.c
 *	  cluster_unit tests for spec-3.11 durable TT slot (D2).
 *
 *	  Covers the I/O-free decision logic + the lookup / by-xid scan logic with
 *	  a mocked cluster_undo_smgr (canned slot / block).  The real file-I/O
 *	  behavior (write -> read -> redo -> restart survival) is end-to-end in
 *	  cluster_tap t/219 (spec-3.11 §4; same pure-unit + e2e-IO split as
 *	  spec-3.9 / spec-3.10 CR -- undo segment I/O needs a real $PGDATA).
 *
 *	  U1  byte-layout (sizeof TTSlot==32, xl_undo_tt_slot_commit==24)
 *	  U2  cluster_tt_durable_redo_decide -- APPLY (newer / same-wrap reuse /
 *	      unused-slot first write) / SKIP (stale) / BADSTATUS (§2.3 last-writer)
 *	  U3  cluster_tt_durable_slot_match -- exact / wrong-xid / UNUSED /
 *	      invalid-scn (xid-match; recycle stamps a new owner xid)
 *	  U4  cluster_tt_slot_durable_lookup -- match / wrong-xid / unused /
 *	      read-fail; stable CLOG-window lookup success / torn-read fail-closed /
 *	      uncommitted fail-closed (mocked smgr)
 *	  U5  cluster_tt_slot_durable_lookup_by_xid -- 0 / 1 / >1 matches (mocked
 *	      block); ambiguity fail-closed (规则 8.A)
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.11-durable-tt-slot.md (§4.1)
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_tt_durable.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "access/transam.h"
#include "access/xlog.h"
#include "storage/bufpage.h"
#include "utils/timestamp.h"

#include "cluster/cluster_scn.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_ges.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_undo_cleaner.h" /* scan-pass stats (spec-3.13 D2-B) */

/* spec-3.13 D2-B stub: scan pass compares commit_scn vs horizon. */
int
scn_time_cmp(SCN a, SCN b)
{
	uint64 la = a & ((((uint64)1) << 56) - 1);
	uint64 lb = b & ((((uint64)1) << 56) - 1);

	return (la < lb) ? -1 : (la > lb) ? 1 : 0;
}
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/storage/cluster_undo_alloc.h" /* spec-3.22: file_exists prototype */
#include "cluster/storage/cluster_undo_block0_current.h"
#include "cluster/storage/cluster_undo_xlog.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

static void (*g_epoch_hook)(void) = NULL;

uint64
GetSystemIdentifier(void)
{
	return UINT64_C(0x1122334455667788);
}

uint64
cluster_epoch_get_current(void)
{
	if (g_epoch_hook != NULL)
		g_epoch_hook();
	return 17;
}

uint64
cluster_qvotec_get_self_incarnation(void)
{
	return 23;
}

static ClusterGesTimeoutDetail fake_ges_timeout_detail = {
	.source = CLUSTER_GES_TSRC_NONE,
	.master_node = -1,
	.conflict_holders = -1,
};

const ClusterGesTimeoutDetail *
cluster_ges_timeout_detail_get(void)
{
	return &fake_ges_timeout_detail;
}

const char *
cluster_ges_timeout_src_text(ClusterGesTimeoutSrc src pg_attribute_unused())
{
	return "unit-test";
}

bool
cluster_semantic_activation_modifier_recheck(
	const ClusterSemanticAdmissionToken *token, bool writable_admission)
{
	return token != NULL && token->entered && writable_admission;
}

/*
 * R4 D5 expected seam.  Keep the declaration test-local for the immutable
 * RED: production does not acquire this API until the behavior below is
 * frozen.
 */
extern bool cluster_tt_slot_durable_read_exact_stable(uint32 segment_id, uint16 slot_offset,
											   TransactionId xid, uint16 expected_wrap,
											   TTSlot *slot_out);
extern XLogRecPtr cluster_tt_slot_durable_publish_active(
	const ClusterTTSlotCurrentOwner *expected_owner,
	const ClusterSemanticAdmissionToken *admission,
	uint32 *segment_generation_out, TTSlot *successor_out);


/* ============================================================
 *	ereport / Assert stubs (cluster_tt_durable.c ereports on I/O fail)
 * ============================================================ */
static int last_ereport_errcode = 0;

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	return elevel >= ERROR;
}
bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}
void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}
int
errcode(int sqlerrcode)
{
	last_ereport_errcode = sqlerrcode;
	return 0;
}
int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
int
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}


/* ============================================================
 *	Mocked cluster_undo_smgr + emit + cluster_node_id
 * ============================================================ */
int cluster_node_id = 0;
int cluster_ges_request_timeout_ms = 1000;
volatile sig_atomic_t InterruptPending = false;
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;
static TimestampTz g_mock_now = 0;

TimestampTz
GetCurrentTimestamp(void)
{
	g_mock_now += INT64_C(1000);
	return g_mock_now;
}

bool
cluster_ctrc_shmem_ready(void)
{
	return true;
}

static ClusterUndoBlock0ResolvedRoot g_current_root;
static uint32 g_current_segment = 1;
static void (*g_before_current_acquire_hook)(void) = NULL;
static void (*g_after_bind_emit_hook)(void) = NULL;
static ClusterUndoBlock0Generation g_current_generation;
static char g_current_resident[BLCKSZ];
static bool g_current_root_ok = true;
static bool g_current_root_drift = false;
static bool g_current_acquire_ok = true;
static bool g_current_sample_ok = true;
static bool g_current_pin_ok = true;
static bool g_current_recheck_ok = true;
static ClusterUndoBlock0Result g_current_empty_result = CLUSTER_UNDO_BLOCK0_OK;
static bool g_current_release_ok = true;
static bool g_current_active = false;
static int g_current_root_calls = 0;
static int g_current_acquire_calls = 0;
static int g_current_sample_calls = 0;
static int g_current_pin_calls = 0;
static int g_current_recheck_calls = 0;
static int g_current_empty_calls = 0;
static int g_current_unpin_calls = 0;
static int g_current_release_calls = 0;
static int g_current_cancel_calls = 0;
static int g_current_target_root_calls = 0;
static int g_current_target_acquire_calls = 0;
static bool g_process_interrupt_error = false;

void ProcessInterrupts(void);

void
ProcessInterrupts(void)
{
	if (g_process_interrupt_error)
		ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("injected caller cancel")));
}

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

bool
cluster_semantic_activation_resolve_shared_undo_root_live_owner_source(
	const ClusterSemanticAdmissionToken *token, ClusterUndoPathIntent intent,
	uint32 owner_instance, uint32 segment_id, ClusterUndoBlock0ResolvedRoot *out)
{
	g_current_root_calls++;
	if (!g_current_root_ok || token == NULL || !token->entered
		|| token->side != CLUSTER_SEMANTIC_SOURCE_SIDE || intent != CLUSTER_UNDO_PATH_RUNTIME_SHARED
		|| owner_instance != (uint32)cluster_node_id + 1 || segment_id != g_current_segment
		|| out == NULL)
		return false;
	*out = g_current_root;
	if (g_current_root_drift && g_current_root_calls > 1)
		out->root_generation++;
	return true;
}

bool
cluster_semantic_activation_resolve_shared_undo_root(
	const ClusterSemanticAdmissionToken *token, ClusterUndoPathIntent intent,
	uint32 owner_instance, uint32 segment_id, ClusterUndoBlock0ResolvedRoot *out)
{
	g_current_root_calls++;
	g_current_target_root_calls++;
	if (!g_current_root_ok || token == NULL || !token->entered
		|| token->side != CLUSTER_SEMANTIC_TARGET_SIDE || intent != CLUSTER_UNDO_PATH_RUNTIME_SHARED
		|| owner_instance != (uint32)cluster_node_id + 1 || segment_id != g_current_segment
		|| out == NULL)
		return false;
	*out = g_current_root;
	if (g_current_root_drift && g_current_target_root_calls > 1)
		out->root_generation++;
	return true;
}

bool
cluster_undo_block0_root_matches(const ClusterUndoBlock0ResolvedRoot *observed,
								 const ClusterUndoBlock0ResolvedRoot *expected)
{
	return observed != NULL && expected != NULL
		&& observed->intent == expected->intent
		&& observed->root_id == expected->root_id
		&& observed->root_generation == expected->root_generation;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_acquire_begin_live_owner_source(
	const ClusterUndoBlock0LogicalKey *key, int timeout_ms pg_attribute_unused(),
	const ClusterSemanticAdmissionToken *admission,
	ClusterUndoBlock0CurrentGuard *guard,
	ClusterUndoBlock0Result *failure)
{
	g_current_acquire_calls++;
	if (g_before_current_acquire_hook != NULL)
		g_before_current_acquire_hook();
	if (!g_current_acquire_ok || key == NULL || key->owner_instance != 1
		|| key->segment_id != g_current_segment || admission == NULL || !admission->entered
		|| admission->side != CLUSTER_SEMANTIC_SOURCE_SIDE || guard == NULL
		|| guard->opaque[0] != 0) {
		if (failure != NULL)
			*failure = guard != NULL && guard->opaque[0] != 0
				? CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH
				: CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
		return CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
	}
	guard->opaque[0] = 1;
	g_current_active = true;
	return CLUSTER_UNDO_BLOCK0_CURRENT_HELD;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_acquire_begin_live_owner_target(
	const ClusterUndoBlock0LogicalKey *key, int timeout_ms pg_attribute_unused(),
	const ClusterSemanticAdmissionToken *admission,
	ClusterUndoBlock0CurrentGuard *guard,
	ClusterUndoBlock0Result *failure)
{
	g_current_acquire_calls++;
	g_current_target_acquire_calls++;
	if (g_before_current_acquire_hook != NULL)
		g_before_current_acquire_hook();
	if (!g_current_acquire_ok || key == NULL || key->owner_instance != 1
		|| key->segment_id != g_current_segment || admission == NULL || !admission->entered
		|| admission->side != CLUSTER_SEMANTIC_TARGET_SIDE || guard == NULL
		|| guard->opaque[0] != 0) {
		if (failure != NULL)
			*failure = guard != NULL && guard->opaque[0] != 0
				? CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH
				: CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
		return CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
	}
	guard->opaque[0] = 1;
	g_current_active = true;
	return CLUSTER_UNDO_BLOCK0_CURRENT_HELD;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_acquire_poll(
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
	ClusterUndoBlock0Result *failure pg_attribute_unused())
{
	return g_current_active ? CLUSTER_UNDO_BLOCK0_CURRENT_HELD
						: CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
}

bool
cluster_undo_block0_current_wait_reply(ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
									   ClusterUndoBlock0ReplyWaitSite site pg_attribute_unused())
{
	/* These current-guard fixtures complete synchronously.  A new pending
	 * path must add an explicit fixture, not receive a fabricated wake. */
	abort();
}

ClusterUndoBlock0Result
cluster_undo_block0_current_sample_generation_exclusive(
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
	const ClusterUndoBlock0ResolvedRoot *root,
	ClusterUndoBlock0Generation *observed)
{
	g_current_sample_calls++;
	if (!g_current_active || !g_current_sample_ok
		|| !cluster_undo_block0_root_matches(root, &g_current_root))
		return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	*observed = g_current_generation;
	return CLUSTER_UNDO_BLOCK0_OK;
}

ClusterUndoBlock0Result
cluster_undo_block0_current_pin_exclusive(
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
	const ClusterUndoBlock0ResolvedRoot *root,
	const ClusterUndoBlock0Generation *expected, ClusterUndoBlock0Pin *pin,
	char **page)
{
	g_current_pin_calls++;
	if (!g_current_active || !g_current_pin_ok
		|| !cluster_undo_block0_root_matches(root, &g_current_root)
		|| expected == NULL || !expected->known
		|| expected->value != g_current_generation.value)
		return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	memset(pin, 0, sizeof(*pin));
	pin->slot = 7;
	pin->mode = CLUSTER_UNDO_BLOCK0_EXCLUSIVE;
	pin->observed_generation = *expected;
	*page = g_current_resident;
	return CLUSTER_UNDO_BLOCK0_OK;
}

ClusterUndoBlock0Result
cluster_undo_block0_current_recheck_exclusive(
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused())
{
	g_current_recheck_calls++;
	return g_current_active && g_current_recheck_ok
		? CLUSTER_UNDO_BLOCK0_OK
		: CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
}

ClusterUndoBlock0Result
cluster_undo_block0_current_prove_strict_empty_exclusive(
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused())
{
	g_current_empty_calls++;
	if (!g_current_active)
		return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	return g_current_empty_result;
}

void
cluster_undo_block0_unpin(ClusterUndoBlock0Pin *pin)
{
	if (pin != NULL && pin->slot >= 0) {
		g_current_unpin_calls++;
		pin->slot = -1;
	}
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_release_begin(
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
	ClusterUndoBlock0Result *failure pg_attribute_unused())
{
	g_current_release_calls++;
	if (!g_current_release_ok)
		return CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
	g_current_active = false;
	return CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_release_poll(
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
	ClusterUndoBlock0Result *failure pg_attribute_unused())
{
	return CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED;
}

void
cluster_undo_block0_current_cancel(ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused())
{
	g_current_cancel_calls++;
	g_current_active = false;
}

static TTSlot g_canned_slot;	  /* returned by read_header_bytes */
static bool g_read_hdr_ok = true; /* read_header_bytes success flag */
static TTSlot g_canned_slot_second;
static bool g_read_hdr_second_enabled = false;
static int g_read_hdr_calls = 0;
static int g_read_hdr_fail_on_call = 0;

static char g_canned_block[BLCKSZ];		  /* returned by read_block for segment 1 */
static uint32 g_canned_block_segment = 1; /* which segment_id the canned block answers */
static bool g_read_block_ok = true;
static int g_read_block_calls = 0;
static ClusterTTSlotCurrentOwner g_allocator_owner;
static bool g_allocator_owner_ok = true;
static int g_allocator_owner_calls = 0;
static uint32 g_allocator_current_segment = 1;
static bool g_allocator_rollover_after_first_owner = false;
static int g_bind_emit_calls = 0;
static xl_undo_tt_slot_bind g_last_bind;
static int g_abort_exact_emit_calls = 0;
static XLogRecPtr g_abort_exact_lsn = (XLogRecPtr)0xfedcba;
static XLogRecPtr g_flushed_lsn = InvalidXLogRecPtr;
static bool g_abort_flush_seen = false;
static bool g_require_abort_flush_before_write = false;
static int g_ctrc_event_sequence = 0;
static int g_ctrc_reserve_calls = 0;
static int g_ctrc_reserve_order = 0;
static int g_ctrc_bind_order = 0;
static int g_ctrc_write_order = 0;
static int g_ctrc_open_calls = 0;
static int g_ctrc_open_order = 0;
static ClusterCtrcOriginReserveResult g_ctrc_reserve_result
	= CLUSTER_CTRC_ORIGIN_RESERVED_PENDING;
static int g_ctrc_reserve_retry_countdown = 0;
static int g_ctrc_release_overlap_pending_countdown = 0;
static int g_ctrc_release_overlap_pending_polls = 0;
static void (*g_ctrc_overlap_hook)(void) = NULL;
static int g_undo_cleaner_wakeup_calls = 0;
static bool g_ctrc_open_ok = true;
static uint32 g_ctrc_open_grant = 7;
static int g_ctrc_cancel_pre_bind_calls = 0;
static int g_ctrc_block_post_bind_calls = 0;
static bool g_bind_emit_error = false;
static XLogRecPtr g_bind_emit_lsn = (XLogRecPtr)0xabcdef;

ClusterCtrcOriginReserveResult
cluster_ctrc_origin_reserve_active(const ClusterCtrcTxnKeyV1 *key,
								   ClusterCtrcOriginReservation *reservation)
{
	g_ctrc_reserve_calls++;
	g_ctrc_reserve_order = ++g_ctrc_event_sequence;
	if (key == NULL || reservation == NULL)
		return CLUSTER_CTRC_ORIGIN_RESERVE_REFUSED;
	memset(reservation, 0, sizeof(*reservation));
	if (g_ctrc_reserve_retry_countdown > 0)
	{
		g_ctrc_reserve_retry_countdown--;
		return CLUSTER_CTRC_ORIGIN_RESERVE_RETRY_RELEASED;
	}
	reservation->key = *key;
	reservation->origin_index = 7;
	reservation->reservation_generation = 41;
	reservation->kind = g_ctrc_reserve_result
		== CLUSTER_CTRC_ORIGIN_RESERVED_PENDING
		? CTRC_ORIGIN_RESERVATION_PENDING_OWNED
		: CTRC_ORIGIN_RESERVATION_EXISTING_OPEN;
	reservation->valid = true;
	return g_ctrc_reserve_result;
}

bool
cluster_ctrc_origin_release_overlap_pending(
	const ClusterCtrcTxnKeyV1 *key)
{
	g_ctrc_release_overlap_pending_polls++;
	if (g_ctrc_overlap_hook != NULL)
		g_ctrc_overlap_hook();
	if (key == NULL)
		return false;
	if (g_ctrc_release_overlap_pending_countdown > 0)
	{
		g_ctrc_release_overlap_pending_countdown--;
		return true;
	}
	return false;
}

void
cluster_undo_cleaner_wakeup(void)
{
	g_undo_cleaner_wakeup_calls++;
}

bool
cluster_ctrc_origin_open_reserved(
	const ClusterCtrcOriginReservation *reservation,
	uint32 *grant_generation)
{
	g_ctrc_open_calls++;
	g_ctrc_open_order = ++g_ctrc_event_sequence;
	if (!g_ctrc_open_ok || reservation == NULL || grant_generation == NULL)
		return false;
	*grant_generation = g_ctrc_open_grant;
	return true;
}

bool
cluster_ctrc_origin_cancel_pre_bind(
	const ClusterCtrcOriginReservation *reservation)
{
	g_ctrc_cancel_pre_bind_calls++;
	return reservation != NULL
		&& reservation->kind == CTRC_ORIGIN_RESERVATION_PENDING_OWNED;
}

bool
cluster_ctrc_origin_block_post_bind(
	const ClusterCtrcOriginReservation *reservation)
{
	g_ctrc_block_post_bind_calls++;
	return reservation != NULL
		&& reservation->kind == CTRC_ORIGIN_RESERVATION_PENDING_OWNED;
}

#ifndef TT_DURABLE_REAL_ALLOCATOR_FIXTURE
bool
cluster_tt_slot_current_owner_by_xid(int node_id, TransactionId xid,
									 ClusterTTSlotCurrentOwner *out)
{
	g_allocator_owner_calls++;
	if (g_allocator_rollover_after_first_owner
		&& g_allocator_owner_calls > 1) {
		if (out != NULL)
			memset(out, 0, sizeof(*out));
		return false;
	}
	if (!g_allocator_owner_ok || node_id != cluster_node_id || out == NULL
		|| xid != g_allocator_owner.xid) {
		if (out != NULL)
			memset(out, 0, sizeof(*out));
		return false;
	}
	*out = g_allocator_owner;
	return true;
}

uint32
cluster_tt_slot_current_segment(int node_id)
{
	if (node_id != cluster_node_id)
		return 0;
	if (g_allocator_rollover_after_first_owner
		&& g_allocator_owner_calls >= 1)
		return 2;
	return g_allocator_current_segment;
}
#endif

XLogRecPtr
cluster_undo_emit_tt_slot_bind(uint8 instance, uint32 segment_id,
								uint32 segment_generation, uint16 slot_offset,
								uint16 wrap, TransactionId xid)
{
	g_bind_emit_calls++;
	g_ctrc_bind_order = ++g_ctrc_event_sequence;
	if (g_bind_emit_error)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("injected pre-return BIND failure")));
	memset(&g_last_bind, 0, sizeof(g_last_bind));
	g_last_bind.instance = instance;
	g_last_bind.segment_id = segment_id;
	g_last_bind.segment_generation = segment_generation;
	g_last_bind.slot_offset = slot_offset;
	g_last_bind.wrap = wrap;
	g_last_bind.xid = xid;
	g_last_bind.format_version = CLUSTER_UNDO_TT_BIND_VERSION;
	if (g_after_bind_emit_hook != NULL)
		g_after_bind_emit_hook();
	return g_bind_emit_lsn;
}

/* spec-3.15 D5 stub: 0x31 emit (WAL machinery not linked in unit). */
XLogRecPtr
cluster_undo_emit_tt_slot_abort(uint8 instance pg_attribute_unused(),
								uint32 segment_id pg_attribute_unused(),
								uint16 slot_offset pg_attribute_unused(),
								uint16 wrap pg_attribute_unused(),
								TransactionId xid pg_attribute_unused())
{
	return InvalidXLogRecPtr;
}

XLogRecPtr
cluster_undo_emit_tt_slot_abort_exact(uint8 instance pg_attribute_unused(),
									 uint32 segment_id pg_attribute_unused(),
									 uint32 segment_generation pg_attribute_unused(),
									 uint16 slot_offset pg_attribute_unused(),
									 uint16 wrap pg_attribute_unused(),
									 TransactionId xid pg_attribute_unused())
{
	g_abort_exact_emit_calls++;
	return g_abort_exact_lsn;
}

void
XLogFlush(XLogRecPtr record)
{
	g_flushed_lsn = record;
	g_abort_flush_seen = true;
}

/* spec-4.8 D7-A stub: 0x90 set-head emit (WAL machinery not linked in unit). */
XLogRecPtr
cluster_undo_emit_tt_slot_set_head(uint8 instance pg_attribute_unused(),
								   uint32 segment_id pg_attribute_unused(),
								   uint16 slot_offset pg_attribute_unused(),
								   uint16 wrap pg_attribute_unused(),
								   TransactionId xid pg_attribute_unused(),
								   UBA first_undo_block pg_attribute_unused())
{
	return InvalidXLogRecPtr;
}

XLogRecPtr
cluster_undo_emit_tt_slot_commit(uint8 instance pg_attribute_unused(),
								 uint32 segment_id pg_attribute_unused(),
								 uint16 slot_offset pg_attribute_unused(),
								 uint16 wrap pg_attribute_unused(),
								 TransactionId xid pg_attribute_unused(),
								 SCN commit_scn pg_attribute_unused())
{
	return InvalidXLogRecPtr;
}

/*
 * Observability hooks (cluster_tt_durable_stat.c) stubbed as no-ops: the pure
 * logic under test bumps counters / brackets wait events through these, but the
 * shmem region + wait-event backend symbols are not linked into cluster_unit.
 */
void
cluster_tt_durable_count_commit(void)
{}
void
cluster_tt_durable_count_lookup(bool hit pg_attribute_unused())
{}
void
cluster_tt_durable_count_by_xid_scan(void)
{}
void
cluster_tt_durable_count_redo_apply(void)
{}
void
cluster_tt_durable_io_wait_start(void)
{}
void
cluster_tt_durable_io_wait_end(void)
{}
void
cluster_vis_bump_recovery_undo_redo_applies(void)
{}
void
cluster_vis_bump_recovery_undo_redo_skips(void)
{}

/* spec-3.13 D6 stubs: scan-pass wait wrappers (no pgstat in unit). */
void
cluster_undo_cleaner_scan_wait_start(void)
{}
void
cluster_undo_cleaner_scan_wait_end(void)
{}

bool
cluster_undo_smgr_read_header_bytes(ClusterUndoPathIntent intent pg_attribute_unused(),
									uint32 segment_id pg_attribute_unused(),
									uint8 owner_instance pg_attribute_unused(), uint32 offset,
									char *buf, uint32 len)
{
	g_read_hdr_calls++;
	if (!g_read_hdr_ok || (g_read_hdr_fail_on_call > 0
						  && g_read_hdr_calls == g_read_hdr_fail_on_call)
		|| buf == NULL || len != sizeof(TTSlot))
		return false;
	(void)offset;
	memcpy(buf,
		   (g_read_hdr_second_enabled && g_read_hdr_calls == 2) ? &g_canned_slot_second
																: &g_canned_slot,
		   sizeof(TTSlot));
	return true;
}

static int g_write_hdr_calls = 0;  /* spec-3.13 D2-B scan-only invariant probe */
static TTSlot g_last_written_slot; /* spec-3.15: capture write payload */
static bool g_write_hdr_ok = true;
static int g_fsync_segment_calls = 0;

bool
cluster_undo_smgr_write_header_bytes(ClusterUndoPathIntent intent pg_attribute_unused(),
									 uint32 segment_id pg_attribute_unused(),
									 uint8 owner_instance pg_attribute_unused(),
									 uint32 offset pg_attribute_unused(), const char *buf,
									 uint32 len)
{
	g_write_hdr_calls++;
	g_ctrc_write_order = ++g_ctrc_event_sequence;
	if (g_require_abort_flush_before_write && !g_abort_flush_seen)
		return false;
	if (buf != NULL && len == sizeof(TTSlot))
		memcpy(&g_last_written_slot, buf, sizeof(TTSlot));
	return g_write_hdr_ok && buf != NULL && len == sizeof(TTSlot);
}

bool
cluster_undo_smgr_fsync_segment_file(uint32 segment_id pg_attribute_unused(),
								 uint8 owner_instance pg_attribute_unused())
{
	g_fsync_segment_calls++;
	return true;
}

static uint32 g_read_block_absent_once_segment;

bool
cluster_undo_smgr_read_block(ClusterUndoPathIntent intent pg_attribute_unused(), uint32 segment_id,
							 uint8 owner_instance pg_attribute_unused(), uint32 block_no, char *buf)
{
	g_read_block_calls++;
	if (segment_id == g_read_block_absent_once_segment) {
		g_read_block_absent_once_segment = 0;
		return false;
	}
	if (!g_read_block_ok || buf == NULL || block_no != 0
		|| segment_id != g_canned_block_segment)
		return false; /* other segments "don't exist" -> by-xid skips them */
	memcpy(buf, g_canned_block, BLCKSZ);
	return true;
}

/*
 * spec-3.22: a segment that file_exists() reports present but read_block() fails
 * = an unreadable existing segment (I/O error) -> the by-xid scan is incomplete
 * -> SCAN_UNAVAILABLE (never a 0-match).  Default 0 = "no existing segment is
 * unreadable", so every read_block miss is a genuinely-absent segment (sound
 * skip) and the scan stays complete -- matching the legacy by-xid behavior.
 */
static uint32 g_unreadable_existing_segment = 0;

bool
cluster_undo_segment_file_exists(uint8 owner_instance pg_attribute_unused(), uint32 segment_id)
{
	return segment_id == g_unreadable_existing_segment;
}


/* helper: seed g_canned_block's TT slot i with (status, xid, commit_scn). */
static void
seed_block_slot(int i, uint8 status, TransactionId xid, SCN commit_scn)
{
	UndoSegmentHeaderData *hdr = (UndoSegmentHeaderData *)g_canned_block;

	hdr->tt_slots[i].status = status;
	hdr->tt_slots[i].xid = xid;
	hdr->tt_slots[i].commit_scn = commit_scn;
}

static bool g_commit_check_result = true;
static int g_commit_check_calls = 0;
static int g_commit_check_read_calls_seen = 0;
static TransactionId g_commit_check_xid = InvalidTransactionId;

static bool
mock_xid_committed(TransactionId xid)
{
	g_commit_check_calls++;
	g_commit_check_xid = xid;
	g_commit_check_read_calls_seen = g_read_hdr_calls;
	return g_commit_check_result;
}

static void
reset_header_read_mock(void)
{
	g_read_hdr_ok = true;
	g_read_hdr_second_enabled = false;
	g_read_hdr_calls = 0;
	g_read_hdr_fail_on_call = 0;
	memset(&g_canned_slot, 0, sizeof(g_canned_slot));
	memset(&g_canned_slot_second, 0, sizeof(g_canned_slot_second));
	g_commit_check_result = true;
	g_commit_check_calls = 0;
	g_commit_check_read_calls_seen = 0;
	g_commit_check_xid = InvalidTransactionId;
}

static void
reset_current_write_mock(void)
{
	reset_header_read_mock();
	g_epoch_hook = NULL;
	g_before_current_acquire_hook = NULL;
	g_after_bind_emit_hook = NULL;
	g_ctrc_overlap_hook = NULL;
	g_process_interrupt_error = false;
	InterruptPending = false;
	g_current_segment = 1;
	g_canned_block_segment = 1;
	g_write_hdr_ok = true;
	g_write_hdr_calls = 0;
	memset(&g_last_written_slot, 0, sizeof(g_last_written_slot));
	g_current_root = (ClusterUndoBlock0ResolvedRoot){
		.intent = CLUSTER_UNDO_PATH_RUNTIME_SHARED,
		.root_id = 71,
		.root_generation = 9,
	};
	g_current_generation = (ClusterUndoBlock0Generation){true, 4};
	memset(g_current_resident, 0, sizeof(g_current_resident));
	memset(g_canned_block, 0, sizeof(g_canned_block));
	g_read_block_ok = true;
	g_read_block_calls = 0;
	g_allocator_owner = (ClusterTTSlotCurrentOwner){
		.segment_id = 1,
		.xid = 100,
		.slot_offset = 7,
		.wrap = 0,
		.status = CTS_ACTIVE,
	};
	g_allocator_owner_ok = true;
	g_allocator_owner_calls = 0;
	g_allocator_current_segment = 1;
	g_allocator_rollover_after_first_owner = false;
	g_bind_emit_calls = 0;
	memset(&g_last_bind, 0, sizeof(g_last_bind));
	g_abort_exact_emit_calls = 0;
	g_flushed_lsn = InvalidXLogRecPtr;
	g_abort_flush_seen = false;
	g_require_abort_flush_before_write = false;
	g_ctrc_event_sequence = 0;
	g_ctrc_reserve_calls = 0;
	g_ctrc_reserve_order = 0;
	g_ctrc_bind_order = 0;
	g_ctrc_write_order = 0;
	g_ctrc_open_calls = 0;
	g_ctrc_open_order = 0;
	g_ctrc_reserve_result = CLUSTER_CTRC_ORIGIN_RESERVED_PENDING;
	g_ctrc_reserve_retry_countdown = 0;
	g_ctrc_release_overlap_pending_countdown = 0;
	g_ctrc_release_overlap_pending_polls = 0;
	g_undo_cleaner_wakeup_calls = 0;
	g_ctrc_open_ok = true;
	g_ctrc_open_grant = 7;
	g_ctrc_cancel_pre_bind_calls = 0;
	g_ctrc_block_post_bind_calls = 0;
	g_bind_emit_error = false;
	g_bind_emit_lsn = (XLogRecPtr)0xabcdef;
	g_current_root_ok = true;
	g_current_root_drift = false;
	g_current_acquire_ok = true;
	g_current_sample_ok = true;
	g_current_pin_ok = true;
	g_current_recheck_ok = true;
	g_current_empty_result = CLUSTER_UNDO_BLOCK0_OK;
	g_current_release_ok = true;
	g_current_active = false;
	g_current_root_calls = 0;
	g_current_acquire_calls = 0;
	g_current_sample_calls = 0;
	g_current_pin_calls = 0;
	g_current_recheck_calls = 0;
	g_current_empty_calls = 0;
	g_current_unpin_calls = 0;
	g_current_release_calls = 0;
	g_current_cancel_calls = 0;
	g_current_target_root_calls = 0;
	g_current_target_acquire_calls = 0;
	last_ereport_errcode = 0;
}

static ClusterSemanticAdmissionToken
source_modifier_token(void)
{
	ClusterSemanticAdmissionToken token;

	memset(&token, 0, sizeof(token));
	token.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	token.record_generation = 19;
	token.formation_epoch = 17;
	token.side = CLUSTER_SEMANTIC_SOURCE_SIDE;
	token.entered = true;
	return token;
}

static ClusterSemanticAdmissionToken
target_modifier_token(void)
{
	ClusterSemanticAdmissionToken token = source_modifier_token();

	token.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	return token;
}

static void
seed_current_exact_active(uint16 slot_offset, TransactionId xid, uint16 wrap)
{
	UndoSegmentHeaderData *disk = (UndoSegmentHeaderData *)g_canned_block;
	UndoSegmentHeaderData *resident = (UndoSegmentHeaderData *)g_current_resident;

	disk->segment_id = 1;
	disk->owner_instance = 1;
	disk->tt_slots_count = TT_SLOTS_PER_SEGMENT;
	disk->wrap_count = g_current_generation.value;
	disk->tt_slots[slot_offset].xid = xid;
	disk->tt_slots[slot_offset].wrap = wrap;
	disk->tt_slots[slot_offset].status = TT_SLOT_ACTIVE;
	disk->tt_slots[slot_offset].flags = TT_FLAGS_RESERVED;
	disk->tt_slots[slot_offset].commit_scn = InvalidScn;
	memcpy(resident, disk, BLCKSZ);
	g_allocator_owner.segment_id = 1;
	g_allocator_owner.xid = xid;
	g_allocator_owner.slot_offset = slot_offset;
	g_allocator_owner.wrap = wrap;
	g_allocator_owner.status = CTS_ACTIVE;
	g_allocator_owner.commit_scn = InvalidScn;
}


/* ============================================================
 *	U1: byte layout
 * ============================================================ */
UT_TEST(test_layout_sizes)
{
	UT_ASSERT_EQ((int)sizeof(TTSlot), 32);
	UT_ASSERT_EQ((int)sizeof(xl_undo_tt_slot_commit), 24);
}


/* ============================================================
 *	U2: redo wrap-comparison table (§2.3)
 * ============================================================ */
UT_TEST(test_redo_decide_newer_wrap_applies)
{
	/* rec.wrap > slot.wrap -> APPLY (recycle-then-commit normal path). */
	UT_ASSERT_EQ((int)cluster_tt_durable_redo_decide(TT_SLOT_COMMITTED, 100, 5, 200, 6),
				 (int)CLUSTER_TT_REDO_APPLY);
}
UT_TEST(test_redo_decide_same_wrap_same_xid_idempotent)
{
	UT_ASSERT_EQ((int)cluster_tt_durable_redo_decide(TT_SLOT_COMMITTED, 100, 5, 100, 5),
				 (int)CLUSTER_TT_REDO_APPLY);
}
UT_TEST(test_redo_decide_same_wrap_diff_xid_applies)
{
	/* spec-3.11 §2.3 (P0 fix): FREE-path slot reuse keeps wrap unchanged while
	 * the xid differs; BIND is not WAL'd so the on-disk slot lags.  "same wrap,
	 * different xid" during redo is normal reuse -> APPLY (last-writer-wins),
	 * NOT corruption (规则 8.A: crash recovery of a reused slot must not PANIC). */
	UT_ASSERT_EQ((int)cluster_tt_durable_redo_decide(TT_SLOT_COMMITTED, 100, 5, 999, 5),
				 (int)CLUSTER_TT_REDO_APPLY);
}
UT_TEST(test_redo_decide_unused_slot_applies)
{
	/* zero-init (UNUSED, xid 0, wrap 0) slot + first commit record (wrap 0) ->
	 * APPLY (the crash-recovery PANIC this fix repaired: t/219 L2/L7). */
	UT_ASSERT_EQ((int)cluster_tt_durable_redo_decide(TT_SLOT_UNUSED, 0, 0, 768, 0),
				 (int)CLUSTER_TT_REDO_APPLY);
}
UT_TEST(test_redo_decide_older_wrap_skips)
{
	UT_ASSERT_EQ((int)cluster_tt_durable_redo_decide(TT_SLOT_COMMITTED, 100, 6, 100, 5),
				 (int)CLUSTER_TT_REDO_SKIP);
}
UT_TEST(test_redo_decide_bad_status)
{
	/* status > TT_SLOT_RECYCLABLE (4) -> BADSTATUS even if wrap would apply. */
	UT_ASSERT_EQ((int)cluster_tt_durable_redo_decide(99, 100, 5, 200, 6),
				 (int)CLUSTER_TT_REDO_BADSTATUS);
}


/* ============================================================
 *	U3: slot_match predicate (C5)
 * ============================================================ */
UT_TEST(test_slot_match_exact)
{
	UT_ASSERT_EQ((int)cluster_tt_durable_slot_match(TT_SLOT_COMMITTED, 100, 5, scn_encode(1, 42),
													100, CLUSTER_TT_WRAP_ANY),
				 1);
}
UT_TEST(test_slot_match_wrong_xid)
{
	/* recycle stamps a new owner xid -> xid mismatch is the recycle detector. */
	UT_ASSERT_EQ((int)cluster_tt_durable_slot_match(TT_SLOT_COMMITTED, 999, 5, scn_encode(1, 42),
													100, CLUSTER_TT_WRAP_ANY),
				 0);
}
UT_TEST(test_slot_match_unused)
{
	UT_ASSERT_EQ((int)cluster_tt_durable_slot_match(TT_SLOT_UNUSED, 100, 5, scn_encode(1, 42), 100,
													CLUSTER_TT_WRAP_ANY),
				 0);
}
UT_TEST(test_slot_match_invalid_scn)
{
	UT_ASSERT_EQ((int)cluster_tt_durable_slot_match(TT_SLOT_COMMITTED, 100, 5, InvalidScn, 100,
													CLUSTER_TT_WRAP_ANY),
				 0);

	/* spec-4.5a G4 (F3, L9b): wrap-qualified matching.  A known expected
	 * wrap MATCHES its own generation and EXCLUDES a recycled slot whose
	 * 32-bit xid wrapped to the same value (same xid, different wrap) --
	 * the case xid-only matching could not detect. */
	UT_ASSERT_EQ(
		(int)cluster_tt_durable_slot_match(TT_SLOT_COMMITTED, 100, 5, scn_encode(1, 42), 100, 5),
		1);
	UT_ASSERT_EQ(
		(int)cluster_tt_durable_slot_match(TT_SLOT_COMMITTED, 100, 6, scn_encode(1, 42), 100, 5),
		0);
}


/* ============================================================
 *	U4: cluster_tt_slot_durable_lookup (mocked smgr)
 * ============================================================ */
UT_TEST(test_lookup_match)
{
	SCN got = InvalidScn;

	g_read_hdr_ok = true;
	memset(&g_canned_slot, 0, sizeof(g_canned_slot));
	g_canned_slot.status = TT_SLOT_COMMITTED;
	g_canned_slot.xid = 100;
	g_canned_slot.wrap = 5;
	g_canned_slot.commit_scn = scn_encode(1, 42);

	UT_ASSERT_EQ((int)cluster_tt_slot_durable_lookup(1, 0, 100, CLUSTER_TT_WRAP_ANY, &got), 1);
	UT_ASSERT_EQ((int)(scn_local(got)), 42);
}
UT_TEST(test_lookup_wrong_xid_miss)
{
	SCN got = InvalidScn;

	g_read_hdr_ok = true;
	memset(&g_canned_slot, 0, sizeof(g_canned_slot));
	g_canned_slot.status = TT_SLOT_COMMITTED;
	g_canned_slot.xid = 999; /* slot recycled to a new owner xid */
	g_canned_slot.commit_scn = scn_encode(1, 42);

	UT_ASSERT_EQ((int)cluster_tt_slot_durable_lookup(1, 0, 100, CLUSTER_TT_WRAP_ANY, &got), 0);
}
UT_TEST(test_lookup_unused_miss)
{
	SCN got = InvalidScn;

	g_read_hdr_ok = true;
	memset(&g_canned_slot, 0, sizeof(g_canned_slot)); /* status=UNUSED(0) */

	UT_ASSERT_EQ((int)cluster_tt_slot_durable_lookup(1, 0, 100, CLUSTER_TT_WRAP_ANY, &got), 0);
}
UT_TEST(test_lookup_read_fail_miss)
{
	SCN got = InvalidScn;

	g_read_hdr_ok = false; /* segment absent / I/O error */
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_lookup(1, 0, 100, CLUSTER_TT_WRAP_ANY, &got), 0);
}
UT_TEST(test_lookup_committed_stable_success)
{
	SCN got = InvalidScn;

	reset_header_read_mock();
	g_canned_slot.status = TT_SLOT_COMMITTED;
	g_canned_slot.xid = 100;
	g_canned_slot.wrap = 5;
	g_canned_slot.commit_scn = scn_encode(1, 42);

	UT_ASSERT_EQ((int)cluster_tt_slot_durable_lookup_committed_stable(
					 1, 0, 100, CLUSTER_TT_WRAP_ANY, mock_xid_committed, &got),
				 1);
	UT_ASSERT_EQ((int)(scn_local(got)), 42);
	UT_ASSERT_EQ(g_read_hdr_calls, 2);
	UT_ASSERT_EQ(g_commit_check_calls, 1);
	UT_ASSERT_EQ((int)g_commit_check_xid, 100);
	UT_ASSERT_EQ(g_commit_check_read_calls_seen, 1);
}
UT_TEST(test_lookup_committed_stable_torn_read_failclosed)
{
	SCN got = InvalidScn;

	reset_header_read_mock();
	g_canned_slot.status = TT_SLOT_COMMITTED;
	g_canned_slot.xid = 100;
	g_canned_slot.wrap = 5;
	g_canned_slot.commit_scn = scn_encode(1, 42);
	g_canned_slot_second = g_canned_slot;
	g_canned_slot_second.commit_scn = scn_encode(1, 17);
	g_read_hdr_second_enabled = true;

	UT_ASSERT_EQ((int)cluster_tt_slot_durable_lookup_committed_stable(
					 1, 0, 100, CLUSTER_TT_WRAP_ANY, mock_xid_committed, &got),
				 0);
	UT_ASSERT_EQ(g_read_hdr_calls, 2);
	UT_ASSERT_EQ(g_commit_check_calls, 1);
}
UT_TEST(test_lookup_committed_stable_uncommitted_failclosed)
{
	SCN got = InvalidScn;

	reset_header_read_mock();
	g_canned_slot.status = TT_SLOT_COMMITTED;
	g_canned_slot.xid = 100;
	g_canned_slot.wrap = 5;
	g_canned_slot.commit_scn = scn_encode(1, 42);
	g_commit_check_result = false;

	UT_ASSERT_EQ((int)cluster_tt_slot_durable_lookup_committed_stable(
					 1, 0, 100, CLUSTER_TT_WRAP_ANY, mock_xid_committed, &got),
				 0);
	UT_ASSERT_EQ(g_read_hdr_calls, 1);
	UT_ASSERT_EQ(g_commit_check_calls, 1);
}

/* ============================================================
 *	R4 D5: exact durable TT identity snapshot (mocked smgr)
 * ============================================================ */
UT_TEST(test_read_exact_stable_accepts_known_status_and_preserves_32_bytes)
{
	static const uint8 known_statuses[] = {TT_SLOT_UNUSED, TT_SLOT_ACTIVE, TT_SLOT_COMMITTED,
											 TT_SLOT_ABORTED, TT_SLOT_RECYCLABLE};
	TTSlot got;
	int i;

	for (i = 0; i < lengthof(known_statuses); i++) {
		reset_header_read_mock();
		memset(&g_canned_slot, 0x5a, sizeof(g_canned_slot));
		g_canned_slot.status = known_statuses[i];
		g_canned_slot.xid = 100;
		g_canned_slot.wrap = 5;
		memset(&got, 0xa5, sizeof(got));

		UT_ASSERT_EQ((int)cluster_tt_slot_durable_read_exact_stable(1, 0, 100, 5, &got), 1);
		UT_ASSERT_EQ(memcmp(&got, &g_canned_slot, sizeof(got)), 0);
		UT_ASSERT_EQ(g_read_hdr_calls, 2);
	}
}
UT_TEST(test_read_exact_stable_rejects_wrong_xid)
{
	TTSlot got;
	TTSlot zero = {0};

	reset_header_read_mock();
	g_canned_slot.status = TT_SLOT_COMMITTED;
	g_canned_slot.xid = 101;
	g_canned_slot.wrap = 5;
	memset(&got, 0xa5, sizeof(got));

	UT_ASSERT_EQ((int)cluster_tt_slot_durable_read_exact_stable(1, 0, 100, 5, &got), 0);
	UT_ASSERT_EQ(memcmp(&got, &zero, sizeof(got)), 0);
}
UT_TEST(test_read_exact_stable_rejects_wrong_wrap)
{
	TTSlot got;
	TTSlot zero = {0};

	reset_header_read_mock();
	g_canned_slot.status = TT_SLOT_COMMITTED;
	g_canned_slot.xid = 100;
	g_canned_slot.wrap = 6;
	memset(&got, 0xa5, sizeof(got));

	UT_ASSERT_EQ((int)cluster_tt_slot_durable_read_exact_stable(1, 0, 100, 5, &got), 0);
	UT_ASSERT_EQ(memcmp(&got, &zero, sizeof(got)), 0);
}
UT_TEST(test_read_exact_stable_rejects_unknown_status)
{
	TTSlot got;
	TTSlot zero = {0};

	reset_header_read_mock();
	g_canned_slot.status = 99;
	g_canned_slot.xid = 100;
	g_canned_slot.wrap = 5;
	memset(&got, 0xa5, sizeof(got));

	UT_ASSERT_EQ((int)cluster_tt_slot_durable_read_exact_stable(1, 0, 100, 5, &got), 0);
	UT_ASSERT_EQ(memcmp(&got, &zero, sizeof(got)), 0);
}
UT_TEST(test_read_exact_stable_rejects_torn_slot)
{
	TTSlot got;
	TTSlot zero = {0};

	reset_header_read_mock();
	g_canned_slot.status = TT_SLOT_ABORTED;
	g_canned_slot.xid = 100;
	g_canned_slot.wrap = 5;
	g_canned_slot_second = g_canned_slot;
	g_canned_slot_second.wrap = 6;
	g_read_hdr_second_enabled = true;
	memset(&got, 0xa5, sizeof(got));

	UT_ASSERT_EQ((int)cluster_tt_slot_durable_read_exact_stable(1, 0, 100, 5, &got), 0);
	UT_ASSERT_EQ(memcmp(&got, &zero, sizeof(got)), 0);
	UT_ASSERT_EQ(g_read_hdr_calls, 2);
}
UT_TEST(test_read_exact_stable_rejects_either_io_failure)
{
	TTSlot got;
	TTSlot zero = {0};

	reset_header_read_mock();
	g_canned_slot.status = TT_SLOT_ABORTED;
	g_canned_slot.xid = 100;
	g_canned_slot.wrap = 5;
	g_read_hdr_ok = false;
	memset(&got, 0xa5, sizeof(got));
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_read_exact_stable(1, 0, 100, 5, &got), 0);
	UT_ASSERT_EQ(memcmp(&got, &zero, sizeof(got)), 0);
	UT_ASSERT_EQ(g_read_hdr_calls, 1);

	reset_header_read_mock();
	g_canned_slot.status = TT_SLOT_ABORTED;
	g_canned_slot.xid = 100;
	g_canned_slot.wrap = 5;
	g_read_hdr_fail_on_call = 2;
	memset(&got, 0xa5, sizeof(got));
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_read_exact_stable(1, 0, 100, 5, &got), 0);
	UT_ASSERT_EQ(memcmp(&got, &zero, sizeof(got)), 0);
	UT_ASSERT_EQ(g_read_hdr_calls, 2);
}

UT_TEST(test_active_bind_predecessor_table_is_exact)
{
	TTSlot slot;

	memset(&slot, 0, sizeof(slot));
	UT_ASSERT_EQ(cluster_tt_active_transition_decide(
		&slot, 7, 7, 100, 5, true), CLUSTER_TT_ACTIVE_APPLY);
	UT_ASSERT_EQ(cluster_tt_active_transition_decide(
		&slot, 7, 7, 100, 5, false), CLUSTER_TT_ACTIVE_CONFLICT);

	slot.xid = 100;
	slot.wrap = 5;
	slot.status = TT_SLOT_ACTIVE;
	UT_ASSERT_EQ(cluster_tt_active_transition_decide(
		&slot, 7, 7, 100, 5, true), CLUSTER_TT_ACTIVE_IDEMPOTENT);

	slot.status = TT_SLOT_COMMITTED;
	slot.commit_scn = scn_encode(1, 42);
	UT_ASSERT_EQ(cluster_tt_active_transition_decide(
		&slot, 7, 7, 200, 6, true), CLUSTER_TT_ACTIVE_APPLY);
	slot.flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
	UT_ASSERT_EQ(cluster_tt_active_transition_decide(
		&slot, 7, 7, 200, 6, true), CLUSTER_TT_ACTIVE_APPLY);
	slot.flags = UINT8_C(0x80);
	UT_ASSERT_EQ(cluster_tt_active_transition_decide(
		&slot, 7, 7, 200, 6, true), CLUSTER_TT_ACTIVE_CORRUPT);
	slot.flags = TT_FLAGS_RESERVED;
	UT_ASSERT_EQ(cluster_tt_active_transition_decide(
		&slot, 7, 7, 200, 5, true), CLUSTER_TT_ACTIVE_CONFLICT);

	slot.status = TT_SLOT_ACTIVE;
	slot.xid = 200;
	slot.wrap = 6;
	slot.commit_scn = InvalidScn;
	UT_ASSERT_EQ(cluster_tt_active_transition_decide(
		&slot, 7, 7, 100, 5, true), CLUSTER_TT_ACTIVE_STALE);
	UT_ASSERT_EQ(cluster_tt_active_transition_decide(
		&slot, 7, 7, 300, 6, true), CLUSTER_TT_ACTIVE_CONFLICT);

	UT_ASSERT_EQ(cluster_tt_active_transition_decide(
		&slot, 8, 7, 200, 6, true), CLUSTER_TT_ACTIVE_STALE);
	UT_ASSERT_EQ(cluster_tt_active_transition_decide(
		&slot, 6, 7, 200, 6, true), CLUSTER_TT_ACTIVE_CORRUPT);

	slot.status = TT_SLOT_INVALID;
	UT_ASSERT_EQ(cluster_tt_active_transition_decide(
		&slot, 7, 7, 200, 6, true), CLUSTER_TT_ACTIVE_CORRUPT);
}

UT_TEST(test_terminal_transition_requires_same_exact_active_entity)
{
	TTSlot slot;
	SCN commit_scn = scn_encode(1, 42);

	memset(&slot, 0, sizeof(slot));
	slot.xid = 100;
	slot.wrap = 5;
	slot.status = TT_SLOT_ACTIVE;
	UT_ASSERT_EQ(cluster_tt_terminal_transition_decide(
		&slot, 7, 7, 100, 5, TT_SLOT_COMMITTED, commit_scn),
		CLUSTER_TT_TERMINAL_APPLY);
	UT_ASSERT_EQ(cluster_tt_terminal_transition_decide(
		&slot, 7, 7, 100, 5, TT_SLOT_ABORTED, InvalidScn),
		CLUSTER_TT_TERMINAL_APPLY);

	slot.status = TT_SLOT_COMMITTED;
	slot.commit_scn = commit_scn;
	UT_ASSERT_EQ(cluster_tt_terminal_transition_decide(
		&slot, 7, 7, 100, 5, TT_SLOT_COMMITTED, commit_scn),
		CLUSTER_TT_TERMINAL_IDEMPOTENT);
	slot.flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
	UT_ASSERT_EQ(cluster_tt_terminal_transition_decide(
		&slot, 7, 7, 100, 5, TT_SLOT_COMMITTED, commit_scn),
		CLUSTER_TT_TERMINAL_APPLY);
	slot.flags = UINT8_C(0x80);
	UT_ASSERT_EQ(cluster_tt_terminal_transition_decide(
		&slot, 7, 7, 100, 5, TT_SLOT_COMMITTED, commit_scn),
		CLUSTER_TT_TERMINAL_CORRUPT);
	slot.flags = TT_FLAGS_RESERVED;
	UT_ASSERT_EQ(cluster_tt_terminal_transition_decide(
		&slot, 7, 7, 100, 5, TT_SLOT_ABORTED, InvalidScn),
		CLUSTER_TT_TERMINAL_CONFLICT);

	slot.xid = 101;
	UT_ASSERT_EQ(cluster_tt_terminal_transition_decide(
		&slot, 7, 7, 100, 5, TT_SLOT_COMMITTED, commit_scn),
		CLUSTER_TT_TERMINAL_CONFLICT);
	slot.xid = 100;
	slot.wrap = 6;
	UT_ASSERT_EQ(cluster_tt_terminal_transition_decide(
		&slot, 7, 7, 100, 5, TT_SLOT_COMMITTED, commit_scn),
		CLUSTER_TT_TERMINAL_STALE);
	UT_ASSERT_EQ(cluster_tt_terminal_transition_decide(
		&slot, 8, 7, 100, 6, TT_SLOT_COMMITTED, commit_scn),
		CLUSTER_TT_TERMINAL_STALE);
	UT_ASSERT_EQ(cluster_tt_terminal_transition_decide(
		&slot, 6, 7, 100, 6, TT_SLOT_COMMITTED, commit_scn),
		CLUSTER_TT_TERMINAL_CORRUPT);

	memset(&slot, 0, sizeof(slot));
	UT_ASSERT_EQ(cluster_tt_terminal_transition_decide(
		&slot, 7, 7, 100, 5, TT_SLOT_ABORTED, InvalidScn),
		CLUSTER_TT_TERMINAL_CONFLICT);
	slot.status = TT_SLOT_ACTIVE;
	slot.xid = 100;
	slot.wrap = 5;
	slot.flags = 1;
	UT_ASSERT_EQ(cluster_tt_terminal_transition_decide(
		&slot, 7, 7, 100, 5, TT_SLOT_ABORTED, InvalidScn),
		CLUSTER_TT_TERMINAL_CORRUPT);
}

UT_TEST(test_active_publish_wal_precedes_identical_disk_and_resident_successor)
{
	ClusterSemanticAdmissionToken admission = target_modifier_token();
	UndoSegmentHeaderData *disk = (UndoSegmentHeaderData *)g_canned_block;
	UndoSegmentHeaderData *resident = (UndoSegmentHeaderData *)g_current_resident;
	TTSlot successor;
	uint32 generation = UINT32_MAX;
	XLogRecPtr lsn;

	reset_current_write_mock();
	disk->segment_id = 1;
	disk->owner_instance = 1;
	disk->tt_slots_count = TT_SLOTS_PER_SEGMENT;
	disk->wrap_count = 4;
	memcpy(resident, disk, BLCKSZ);

	lsn = cluster_tt_slot_durable_publish_active(
		&g_allocator_owner, &admission, &generation, &successor);

	UT_ASSERT_EQ(lsn, (XLogRecPtr)0xabcdef);
	UT_ASSERT_EQ(generation, 4);
	UT_ASSERT_EQ(g_bind_emit_calls, 1);
	UT_ASSERT_EQ(g_last_bind.segment_id, 1);
	UT_ASSERT_EQ(g_last_bind.segment_generation, 4);
	UT_ASSERT_EQ(g_last_bind.slot_offset, 7);
	UT_ASSERT_EQ(g_last_bind.wrap, 0);
	UT_ASSERT_EQ(g_last_bind.xid, 100);
	UT_ASSERT_EQ(g_write_hdr_calls, 1);
	UT_ASSERT_EQ(successor.status, TT_SLOT_ACTIVE);
	UT_ASSERT_EQ(successor.xid, 100);
	UT_ASSERT_EQ(successor.wrap, 0);
	UT_ASSERT_EQ(successor.commit_scn, InvalidScn);
	UT_ASSERT(UBA_is_invalid(successor.first_undo_block));
	UT_ASSERT_EQ(memcmp(&successor, &g_last_written_slot, sizeof(successor)), 0);
	UT_ASSERT_EQ(memcmp(&successor, &resident->tt_slots[7], sizeof(successor)), 0);
	UT_ASSERT_EQ(g_allocator_owner_calls, 3);
	UT_ASSERT_EQ(g_current_recheck_calls, 1);
	UT_ASSERT_EQ(g_ctrc_reserve_calls, 1);
	UT_ASSERT_EQ(g_ctrc_open_calls, 1);
	UT_ASSERT(g_ctrc_reserve_order < g_ctrc_bind_order);
	UT_ASSERT(g_ctrc_bind_order < g_ctrc_write_order);
	UT_ASSERT(g_ctrc_write_order < g_ctrc_open_order);
}

UT_TEST(test_active_publish_waits_for_released_origin_notification_before_bind)
{
	ClusterSemanticAdmissionToken admission = target_modifier_token();
	UndoSegmentHeaderData *disk = (UndoSegmentHeaderData *)g_canned_block;
	UndoSegmentHeaderData *resident = (UndoSegmentHeaderData *)g_current_resident;
	TTSlot successor;
	uint32 generation = UINT32_MAX;
	XLogRecPtr lsn;

	reset_current_write_mock();
	disk->segment_id = 1;
	disk->owner_instance = 1;
	disk->tt_slots_count = TT_SLOTS_PER_SEGMENT;
	disk->wrap_count = 4;
	memcpy(resident, disk, BLCKSZ);
	g_ctrc_reserve_retry_countdown = 1;
	g_ctrc_release_overlap_pending_countdown = 5;

	lsn = cluster_tt_slot_durable_publish_active(
		&g_allocator_owner, &admission, &generation, &successor);

	UT_ASSERT_EQ(lsn, (XLogRecPtr)0xabcdef);
	UT_ASSERT_EQ(g_ctrc_reserve_calls, 2);
	UT_ASSERT_EQ(g_current_acquire_calls, 2);
	UT_ASSERT_EQ(g_current_sample_calls, 2);
	UT_ASSERT_EQ(g_current_pin_calls, 2);
	UT_ASSERT_EQ(g_current_unpin_calls, 2);
	UT_ASSERT_EQ(g_current_cancel_calls, 2);
	UT_ASSERT_EQ(g_ctrc_release_overlap_pending_polls, 6);
	UT_ASSERT_EQ(g_undo_cleaner_wakeup_calls, 1);
	UT_ASSERT_EQ(g_bind_emit_calls, 1);
	UT_ASSERT_EQ(g_ctrc_open_calls, 1);
	UT_ASSERT(g_ctrc_reserve_order < g_ctrc_bind_order);
}

UT_TEST(test_active_publish_final_xcur_drift_stays_unpublished)
{
	ClusterSemanticAdmissionToken admission = target_modifier_token();
	UndoSegmentHeaderData *disk = (UndoSegmentHeaderData *)g_canned_block;
	UndoSegmentHeaderData *resident = (UndoSegmentHeaderData *)g_current_resident;
	TTSlot successor;
	uint32 generation = UINT32_MAX;
	volatile bool caught = false;

	reset_current_write_mock();
	disk->segment_id = 1;
	disk->owner_instance = 1;
	disk->tt_slots_count = TT_SLOTS_PER_SEGMENT;
	disk->wrap_count = 4;
	memcpy(resident, disk, BLCKSZ);
	g_current_recheck_ok = false;
	memset(&successor, 0xa5, sizeof(successor));

	PG_TRY();
	{
		(void)cluster_tt_slot_durable_publish_active(
			&g_allocator_owner, &admission, &generation, &successor);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();

	UT_ASSERT(caught);
	UT_ASSERT_EQ(g_bind_emit_calls, 1);
	UT_ASSERT_EQ(g_write_hdr_calls, 1);
	UT_ASSERT_EQ(g_current_recheck_calls, 1);
	UT_ASSERT_EQ(generation, UINT32_MAX);
	UT_ASSERT_EQ(g_ctrc_cancel_pre_bind_calls, 0);
	UT_ASSERT_EQ(g_ctrc_block_post_bind_calls, 1);
}

UT_TEST(test_active_publish_prebind_failures_cancel_only_owned_pending_reservation)
{
	ClusterSemanticAdmissionToken admission = target_modifier_token();
	UndoSegmentHeaderData *disk = (UndoSegmentHeaderData *)g_canned_block;
	UndoSegmentHeaderData *resident = (UndoSegmentHeaderData *)g_current_resident;
	TTSlot successor;
	uint32 generation = UINT32_MAX;
	volatile bool caught = false;

	reset_current_write_mock();
	disk->segment_id = 1;
	disk->owner_instance = 1;
	disk->tt_slots_count = TT_SLOTS_PER_SEGMENT;
	disk->wrap_count = 4;
	disk->tt_slots[7].status = TT_SLOT_ACTIVE;
	disk->tt_slots[7].xid = 999;
	disk->tt_slots[7].wrap = 0;
	memcpy(resident, disk, BLCKSZ);
	PG_TRY();
	{
		(void)cluster_tt_slot_durable_publish_active(
			&g_allocator_owner, &admission, &generation, &successor);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(g_bind_emit_calls, 0);
	UT_ASSERT_EQ(g_ctrc_cancel_pre_bind_calls, 1);
	UT_ASSERT_EQ(g_ctrc_block_post_bind_calls, 0);

	reset_current_write_mock();
	disk->segment_id = 1;
	disk->owner_instance = 1;
	disk->tt_slots_count = TT_SLOTS_PER_SEGMENT;
	disk->wrap_count = 4;
	memcpy(resident, disk, BLCKSZ);
	g_bind_emit_error = true;
	caught = false;
	PG_TRY();
	{
		(void)cluster_tt_slot_durable_publish_active(
			&g_allocator_owner, &admission, &generation, &successor);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(g_bind_emit_calls, 1);
	UT_ASSERT_EQ(g_ctrc_cancel_pre_bind_calls, 1);
	UT_ASSERT_EQ(g_ctrc_block_post_bind_calls, 0);

	reset_current_write_mock();
	disk->segment_id = 1;
	disk->owner_instance = 1;
	disk->tt_slots_count = TT_SLOTS_PER_SEGMENT;
	disk->wrap_count = 4;
	memcpy(resident, disk, BLCKSZ);
	g_bind_emit_lsn = InvalidXLogRecPtr;
	caught = false;
	PG_TRY();
	{
		(void)cluster_tt_slot_durable_publish_active(
			&g_allocator_owner, &admission, &generation, &successor);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(g_ctrc_cancel_pre_bind_calls, 1);
	UT_ASSERT_EQ(g_ctrc_block_post_bind_calls, 0);
}

UT_TEST(test_active_publish_postbind_failures_block_owned_pending_reservation)
{
	ClusterSemanticAdmissionToken admission = target_modifier_token();
	UndoSegmentHeaderData *disk = (UndoSegmentHeaderData *)g_canned_block;
	UndoSegmentHeaderData *resident = (UndoSegmentHeaderData *)g_current_resident;
	TTSlot successor;
	uint32 generation = UINT32_MAX;
	volatile bool caught = false;

	reset_current_write_mock();
	disk->segment_id = 1;
	disk->owner_instance = 1;
	disk->tt_slots_count = TT_SLOTS_PER_SEGMENT;
	disk->wrap_count = 4;
	memcpy(resident, disk, BLCKSZ);
	g_write_hdr_ok = false;
	PG_TRY();
	{
		(void)cluster_tt_slot_durable_publish_active(
			&g_allocator_owner, &admission, &generation, &successor);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(g_ctrc_cancel_pre_bind_calls, 0);
	UT_ASSERT_EQ(g_ctrc_block_post_bind_calls, 1);

	reset_current_write_mock();
	disk->segment_id = 1;
	disk->owner_instance = 1;
	disk->tt_slots_count = TT_SLOTS_PER_SEGMENT;
	disk->wrap_count = 4;
	memcpy(resident, disk, BLCKSZ);
	g_ctrc_open_ok = false;
	caught = false;
	PG_TRY();
	{
		(void)cluster_tt_slot_durable_publish_active(
			&g_allocator_owner, &admission, &generation, &successor);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(g_ctrc_cancel_pre_bind_calls, 0);
	UT_ASSERT_EQ(g_ctrc_block_post_bind_calls, 1);
}

UT_TEST(test_active_publish_existing_open_error_never_cancels_or_blocks_entry)
{
	ClusterSemanticAdmissionToken admission = target_modifier_token();
	UndoSegmentHeaderData *disk = (UndoSegmentHeaderData *)g_canned_block;
	UndoSegmentHeaderData *resident = (UndoSegmentHeaderData *)g_current_resident;
	TTSlot successor;
	uint32 generation = UINT32_MAX;
	volatile bool caught = false;

	reset_current_write_mock();
	disk->segment_id = 1;
	disk->owner_instance = 1;
	disk->tt_slots_count = TT_SLOTS_PER_SEGMENT;
	disk->wrap_count = 4;
	memcpy(resident, disk, BLCKSZ);
	g_ctrc_reserve_result = CLUSTER_CTRC_ORIGIN_RESERVED_EXISTING_OPEN;
	g_write_hdr_ok = false;
	PG_TRY();
	{
		(void)cluster_tt_slot_durable_publish_active(
			&g_allocator_owner, &admission, &generation, &successor);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(g_ctrc_cancel_pre_bind_calls, 0);
	UT_ASSERT_EQ(g_ctrc_block_post_bind_calls, 0);
}

UT_TEST(test_active_publish_requires_exact_root_and_resident_disk_generation)
{
	ClusterSemanticAdmissionToken admission = target_modifier_token();
	UndoSegmentHeaderData *disk = (UndoSegmentHeaderData *)g_canned_block;
	UndoSegmentHeaderData *resident = (UndoSegmentHeaderData *)g_current_resident;
	TTSlot successor;
	uint32 generation = UINT32_MAX;
	volatile bool caught = false;

	reset_current_write_mock();
	disk->segment_id = 1;
	disk->owner_instance = 1;
	disk->tt_slots_count = TT_SLOTS_PER_SEGMENT;
	disk->wrap_count = 4;
	memcpy(resident, disk, BLCKSZ);
	resident->wrap_count = 5;
	memset(&successor, 0xa5, sizeof(successor));

	PG_TRY();
	{
		(void)cluster_tt_slot_durable_publish_active(
			&g_allocator_owner, &admission, &generation, &successor);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();

	UT_ASSERT(caught);
	UT_ASSERT_EQ(g_bind_emit_calls, 0);
	UT_ASSERT_EQ(g_write_hdr_calls, 0);
	UT_ASSERT_EQ(generation, UINT32_MAX);
}

UT_TEST(test_active_publish_rejects_allocator_identity_drift_before_wal)
{
	ClusterSemanticAdmissionToken admission = target_modifier_token();
	UndoSegmentHeaderData *disk = (UndoSegmentHeaderData *)g_canned_block;
	UndoSegmentHeaderData *resident = (UndoSegmentHeaderData *)g_current_resident;
	TTSlot successor;
	uint32 generation = UINT32_MAX;
	volatile bool caught = false;

	reset_current_write_mock();
	disk->segment_id = 1;
	disk->owner_instance = 1;
	disk->tt_slots_count = TT_SLOTS_PER_SEGMENT;
	disk->wrap_count = 4;
	memcpy(resident, disk, BLCKSZ);
	g_allocator_owner_ok = false;

	PG_TRY();
	{
		(void)cluster_tt_slot_durable_publish_active(
			&g_allocator_owner, &admission, &generation, &successor);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();

	UT_ASSERT(caught);
	UT_ASSERT_EQ(g_bind_emit_calls, 0);
	UT_ASSERT_EQ(g_write_hdr_calls, 0);
}

UT_TEST(test_active_publish_returns_unpublished_retry_when_allocator_already_rolled)
{
	ClusterSemanticAdmissionToken admission = target_modifier_token();
	TTSlot successor;
	TTSlot zero_slot;
	uint32 generation = UINT32_MAX;
	volatile XLogRecPtr lsn = InvalidXLogRecPtr;
	volatile bool caught = false;

	reset_current_write_mock();
	g_allocator_owner_ok = false;
	g_allocator_current_segment = 2;
	memset(&successor, 0xa5, sizeof(successor));
	memset(&zero_slot, 0, sizeof(zero_slot));
	PG_TRY();
	{
		lsn = cluster_tt_slot_durable_publish_active(&g_allocator_owner, &admission, &generation,
													 &successor);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();

	UT_ASSERT(!caught);
	UT_ASSERT(XLogRecPtrIsInvalid(lsn));
	UT_ASSERT_EQ(generation, UINT32_MAX);
	UT_ASSERT_EQ(memcmp(&successor, &zero_slot, sizeof(successor)), 0);
	UT_ASSERT_EQ(g_bind_emit_calls, 0);
	UT_ASSERT_EQ(g_write_hdr_calls, 0);
	UT_ASSERT_EQ(g_ctrc_reserve_calls, 0);
	UT_ASSERT_EQ(g_current_acquire_calls, 0);
}

static void
roll_mock_before_current_result(void)
{
	g_before_current_acquire_hook = NULL;
	g_allocator_current_segment = 2;
	g_allocator_owner_ok = false;
}

UT_TEST(test_active_publish_rollover_with_old_root_or_current_unavailable_is_unpublished)
{
	volatile unsigned scenario;

	for (scenario = 0; scenario < 2; scenario++) {
		ClusterSemanticAdmissionToken admission = target_modifier_token();
		TTSlot successor;
		uint32 generation = UINT32_MAX;
		volatile XLogRecPtr lsn = InvalidXLogRecPtr;
		volatile bool caught = false;

		reset_current_write_mock();
		g_current_root_ok = scenario != 0;
		g_current_acquire_ok = scenario != 1;
		g_before_current_acquire_hook = roll_mock_before_current_result;
		PG_TRY();
		{
			lsn = cluster_tt_slot_durable_publish_active(&g_allocator_owner, &admission,
														 &generation, &successor);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(!caught);
		UT_ASSERT(XLogRecPtrIsInvalid(lsn));
		UT_ASSERT_EQ(generation, UINT32_MAX);
		UT_ASSERT_EQ(g_bind_emit_calls, 0);
		UT_ASSERT_EQ(g_write_hdr_calls, 0);
		UT_ASSERT_EQ(g_ctrc_reserve_calls, 0);
		UT_ASSERT(!g_current_active);
		UT_ASSERT_EQ(g_current_cancel_calls, scenario == 0 ? 1 : 0);
	}
}

UT_TEST(test_active_publish_retries_rollover_before_xcur_allocator_linearization)
{
	ClusterSemanticAdmissionToken admission = target_modifier_token();
	UndoSegmentHeaderData *disk = (UndoSegmentHeaderData *)g_canned_block;
	UndoSegmentHeaderData *resident = (UndoSegmentHeaderData *)g_current_resident;
	TTSlot successor;
	uint32 generation = UINT32_MAX;
	XLogRecPtr lsn = InvalidXLogRecPtr;
	volatile bool caught = false;

	reset_current_write_mock();
	disk->segment_id = 1;
	disk->owner_instance = 1;
	disk->tt_slots_count = TT_SLOTS_PER_SEGMENT;
	disk->wrap_count = 4;
	memcpy(resident, disk, BLCKSZ);
	g_allocator_rollover_after_first_owner = true;

	PG_TRY();
	{
		lsn = cluster_tt_slot_durable_publish_active(
			&g_allocator_owner, &admission, &generation, &successor);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();

	UT_ASSERT(!caught);
	UT_ASSERT(XLogRecPtrIsInvalid(lsn));
	UT_ASSERT_EQ(generation, UINT32_MAX);
	UT_ASSERT_EQ(g_bind_emit_calls, 0);
	UT_ASSERT_EQ(g_write_hdr_calls, 0);
	UT_ASSERT_EQ(g_ctrc_reserve_calls, 0);
	UT_ASSERT_EQ(g_current_acquire_calls, 1);
	UT_ASSERT_EQ(g_current_cancel_calls, 1);
	UT_ASSERT_EQ(g_current_pin_calls, 0);
	UT_ASSERT_EQ(g_current_unpin_calls, 0);
	UT_ASSERT_EQ(successor.status, 0);
	UT_ASSERT_EQ(successor.xid, InvalidTransactionId);
	UT_ASSERT_EQ(successor.wrap, 0);
	UT_ASSERT_EQ(g_allocator_owner_calls, 1);
}

UT_TEST(test_active_publish_rejects_same_wrap_different_xid_before_wal)
{
	ClusterSemanticAdmissionToken admission = target_modifier_token();
	UndoSegmentHeaderData *disk = (UndoSegmentHeaderData *)g_canned_block;
	UndoSegmentHeaderData *resident = (UndoSegmentHeaderData *)g_current_resident;
	TTSlot successor;
	uint32 generation = UINT32_MAX;
	volatile bool caught = false;

	reset_current_write_mock();
	disk->segment_id = 1;
	disk->owner_instance = 1;
	disk->tt_slots_count = TT_SLOTS_PER_SEGMENT;
	disk->wrap_count = 4;
	disk->tt_slots[7].status = TT_SLOT_ACTIVE;
	disk->tt_slots[7].xid = 999;
	disk->tt_slots[7].wrap = 0;
	memcpy(resident, disk, BLCKSZ);

	PG_TRY();
	{
		(void)cluster_tt_slot_durable_publish_active(
			&g_allocator_owner, &admission, &generation, &successor);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();

	UT_ASSERT(caught);
	UT_ASSERT_EQ(g_bind_emit_calls, 0);
	UT_ASSERT_EQ(g_write_hdr_calls, 0);
}

/* ============================================================
 *	RF-ROOT P4: normal precommit 32-byte durable/resident successor
 * ============================================================ */
UT_TEST(test_precommit_writeonly_publishes_identical_durable_and_resident_successor)
{
	ClusterSemanticAdmissionToken admission = source_modifier_token();
	UndoSegmentHeaderData *resident = (UndoSegmentHeaderData *)g_current_resident;
	TTSlot successor;
	uint8 owner;

	reset_current_write_mock();
	seed_current_exact_active(7, 100, 5);
	memset(&successor, 0xa5, sizeof(successor));

	owner = cluster_tt_slot_durable_commit_writeonly(
		1, 4, 7, 100, 5, scn_encode(1, 42), &admission, &successor);

	UT_ASSERT_EQ(owner, 1);
	UT_ASSERT(admission.entered);
	UT_ASSERT_EQ(g_write_hdr_calls, 1);
	UT_ASSERT_EQ(memcmp(&successor, &g_last_written_slot, sizeof(successor)), 0);
	UT_ASSERT_EQ(memcmp(&resident->tt_slots[7], &successor, sizeof(successor)), 0);
	UT_ASSERT(cluster_tt_durable_slot_match(
		successor.status, successor.xid, successor.wrap, successor.commit_scn,
		100, 5));
	UT_ASSERT_EQ(g_current_root_calls, 3);
	UT_ASSERT_EQ(g_current_acquire_calls, 1);
	UT_ASSERT_EQ(g_current_sample_calls, 1);
	UT_ASSERT_EQ(g_current_pin_calls, 1);
	UT_ASSERT_EQ(g_current_unpin_calls, 1);
	UT_ASSERT_EQ(g_current_release_calls, 0);
	UT_ASSERT_EQ(g_current_cancel_calls, 1);
}

UT_TEST(test_precommit_writeonly_target_open_reuses_same_block0_authority)
{
	ClusterSemanticAdmissionToken admission = target_modifier_token();
	UndoSegmentHeaderData *resident = (UndoSegmentHeaderData *)g_current_resident;
	TTSlot successor;
	uint8 owner = 0;
	volatile bool caught = false;

	reset_current_write_mock();
	seed_current_exact_active(7, 100, 5);
	memset(&successor, 0xa5, sizeof(successor));

	PG_TRY();
	{
		owner = cluster_tt_slot_durable_commit_writeonly(
			1, 4, 7, 100, 5, scn_encode(1, 42), &admission, &successor);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();

	UT_ASSERT(!caught);
	UT_ASSERT_EQ(owner, 1);
	UT_ASSERT(admission.entered);
	UT_ASSERT_EQ(g_current_target_root_calls, 3);
	UT_ASSERT_EQ(g_current_target_acquire_calls, 1);
	UT_ASSERT_EQ(g_write_hdr_calls, 1);
	UT_ASSERT_EQ(memcmp(&successor, &g_last_written_slot, sizeof(successor)), 0);
	UT_ASSERT_EQ(memcmp(&resident->tt_slots[7], &successor, sizeof(successor)), 0);
	UT_ASSERT_EQ(g_current_cancel_calls, 1);
}

UT_TEST(test_precommit_writeonly_accepts_exact_active_on_rolled_away_segment)
{
	ClusterSemanticAdmissionToken admission = target_modifier_token();
	UndoSegmentHeaderData *resident = (UndoSegmentHeaderData *)g_current_resident;
	TTSlot successor;
	uint8 owner = 0;
	volatile bool caught = false;

	reset_current_write_mock();
	seed_current_exact_active(7, 100, 5);
	g_allocator_current_segment = 2;
	g_allocator_owner_ok = false;
	memset(&successor, 0xa5, sizeof(successor));

	PG_TRY();
	{
		owner = cluster_tt_slot_durable_commit_writeonly(
			1, 4, 7, 100, 5, scn_encode(1, 42), &admission, &successor);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();

	UT_ASSERT(!caught);
	UT_ASSERT_EQ(owner, 1);
	UT_ASSERT_EQ(g_allocator_owner_calls, 0);
	UT_ASSERT_EQ(g_write_hdr_calls, 1);
	UT_ASSERT_EQ(successor.status, TT_SLOT_COMMITTED);
	UT_ASSERT_EQ(successor.xid, 100);
	UT_ASSERT_EQ(successor.wrap, 5);
	UT_ASSERT_EQ(memcmp(&resident->tt_slots[7], &successor,
			sizeof(successor)), 0);
}

UT_TEST(test_precommit_writeonly_root_drift_never_writes_or_publishes)
{
	ClusterSemanticAdmissionToken admission = source_modifier_token();
	UndoSegmentHeaderData *resident = (UndoSegmentHeaderData *)g_current_resident;
	TTSlot before;
	TTSlot successor;
	TTSlot successor_before;
	volatile bool caught = false;

	reset_current_write_mock();
	seed_current_exact_active(7, 100, 5);
	before = resident->tt_slots[7];
	memset(&successor, 0xa5, sizeof(successor));
	successor_before = successor;
	g_current_root_drift = true;

	PG_TRY();
	{
		(void)cluster_tt_slot_durable_commit_writeonly(
			1, 4, 7, 100, 5, scn_encode(1, 42), &admission, &successor);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();

	UT_ASSERT(caught);
	UT_ASSERT(admission.entered);
	UT_ASSERT_EQ(g_write_hdr_calls, 0);
	UT_ASSERT_EQ(memcmp(&resident->tt_slots[7], &before, sizeof(before)), 0);
	UT_ASSERT_EQ(memcmp(&successor, &successor_before, sizeof(successor)), 0);
	UT_ASSERT_EQ(g_current_unpin_calls, 1);
	UT_ASSERT_EQ(g_current_release_calls, 0);
	UT_ASSERT_EQ(g_current_cancel_calls, 1);
	g_write_hdr_ok = true;
}

UT_TEST(test_precommit_writeonly_missing_current_authority_never_writes_or_publishes)
{
	ClusterSemanticAdmissionToken admission = source_modifier_token();
	UndoSegmentHeaderData *resident = (UndoSegmentHeaderData *)g_current_resident;
	TTSlot before;
	TTSlot successor;
	TTSlot successor_before;
	volatile bool caught = false;

	reset_current_write_mock();
	seed_current_exact_active(7, 100, 5);
	before = resident->tt_slots[7];
	memset(&successor, 0xa5, sizeof(successor));
	successor_before = successor;
	g_current_acquire_ok = false;

	PG_TRY();
	{
		(void)cluster_tt_slot_durable_commit_writeonly(
			1, 4, 7, 100, 5, scn_encode(1, 42), &admission, &successor);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();

	UT_ASSERT(caught);
	UT_ASSERT(admission.entered);
	UT_ASSERT_EQ(g_write_hdr_calls, 0);
	UT_ASSERT_EQ(memcmp(&resident->tt_slots[7], &before, sizeof(before)), 0);
	UT_ASSERT_EQ(memcmp(&successor, &successor_before, sizeof(successor)), 0);
	UT_ASSERT_EQ(g_current_unpin_calls, 0);
	UT_ASSERT_EQ(g_current_release_calls, 0);
	UT_ASSERT_EQ(g_current_cancel_calls, 0);
}

UT_TEST(test_precommit_writeonly_durable_failure_never_publishes_resident)
{
	ClusterSemanticAdmissionToken admission = source_modifier_token();
	UndoSegmentHeaderData *resident = (UndoSegmentHeaderData *)g_current_resident;
	TTSlot before;
	TTSlot successor;
	TTSlot successor_before;
	volatile bool caught = false;

	reset_current_write_mock();
	seed_current_exact_active(7, 100, 5);
	before = resident->tt_slots[7];
	memset(&successor, 0xa5, sizeof(successor));
	successor_before = successor;
	g_write_hdr_ok = false;

	PG_TRY();
	{
		(void)cluster_tt_slot_durable_commit_writeonly(
			1, 4, 7, 100, 5, scn_encode(1, 42), &admission, &successor);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();

	UT_ASSERT(caught);
	UT_ASSERT(admission.entered);
	UT_ASSERT_EQ(g_write_hdr_calls, 1);
	UT_ASSERT_EQ(memcmp(&resident->tt_slots[7], &before, sizeof(before)), 0);
	UT_ASSERT_EQ(memcmp(&successor, &successor_before, sizeof(successor)), 0);
	UT_ASSERT_EQ(g_current_unpin_calls, 1);
	UT_ASSERT_EQ(g_current_release_calls, 0);
	UT_ASSERT_EQ(g_current_cancel_calls, 1);
	g_write_hdr_ok = true;
}

UT_TEST(test_precommit_writeonly_postwrite_release_failure_is_nothrow_cleanup)
{
	ClusterSemanticAdmissionToken admission = source_modifier_token();
	UndoSegmentHeaderData *resident = (UndoSegmentHeaderData *)g_current_resident;
	TTSlot successor;
	uint8 owner = 0;
	volatile bool caught = false;

	reset_current_write_mock();
	seed_current_exact_active(7, 100, 5);
	memset(&successor, 0xa5, sizeof(successor));
	g_current_release_ok = false;

	PG_TRY();
	{
		owner = cluster_tt_slot_durable_commit_writeonly(
			1, 4, 7, 100, 5, scn_encode(1, 42), &admission, &successor);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();

	UT_ASSERT(!caught);
	UT_ASSERT_EQ(owner, 1);
	UT_ASSERT(admission.entered);
	UT_ASSERT_EQ(g_write_hdr_calls, 1);
	UT_ASSERT_EQ(memcmp(&successor, &g_last_written_slot, sizeof(successor)), 0);
	UT_ASSERT_EQ(memcmp(&resident->tt_slots[7], &successor, sizeof(successor)), 0);
	UT_ASSERT_EQ(g_current_unpin_calls, 1);
	UT_ASSERT_EQ(g_current_release_calls, 0);
	UT_ASSERT_EQ(g_current_cancel_calls, 1);
}

UT_TEST(test_ordinary_abort_flushes_exact_carrier_before_terminal_write)
{
	ClusterSemanticAdmissionToken admission = target_modifier_token();
	UndoSegmentHeaderData *resident = (UndoSegmentHeaderData *)g_current_resident;
	TTSlot successor;
	XLogRecPtr lsn;

	reset_current_write_mock();
	seed_current_exact_active(7, 100, 5);
	g_require_abort_flush_before_write = true;
	memset(&successor, 0xa5, sizeof(successor));

	lsn = cluster_tt_slot_durable_abort_exact(
		1, 4, 7, 100, 5, &admission, &successor);

	UT_ASSERT_EQ(lsn, g_abort_exact_lsn);
	UT_ASSERT_EQ(g_abort_exact_emit_calls, 1);
	UT_ASSERT_EQ(g_flushed_lsn, g_abort_exact_lsn);
	UT_ASSERT(g_abort_flush_seen);
	UT_ASSERT_EQ(g_write_hdr_calls, 1);
	UT_ASSERT_EQ(successor.status, TT_SLOT_ABORTED);
	UT_ASSERT_EQ(successor.xid, 100);
	UT_ASSERT_EQ(successor.wrap, 5);
	UT_ASSERT(!SCN_VALID(successor.commit_scn));
	UT_ASSERT_EQ(memcmp(&resident->tt_slots[7], &successor,
			sizeof(successor)), 0);
	UT_ASSERT_EQ(g_current_acquire_calls, 2);
	UT_ASSERT_EQ(g_current_cancel_calls, 2);
}

UT_TEST(test_precommit_writeonly_missing_canonical_active_refuses_without_write)
{
	ClusterSemanticAdmissionToken admission = source_modifier_token();
	TTSlot resident_before;
	TTSlot successor;
	uint8 owner = 0;
	volatile bool caught = false;

	reset_current_write_mock();
	memset(&g_canned_slot, 0, sizeof(g_canned_slot));
	g_canned_slot.status = TT_SLOT_ACTIVE;
	g_canned_slot.xid = 100;
	g_canned_slot.wrap = 5;
	memset(g_current_resident, 0x6d, sizeof(g_current_resident));
	memcpy(&resident_before,
		   &((UndoSegmentHeaderData *)g_current_resident)->tt_slots[7],
		   sizeof(resident_before));
	memset(&successor, 0xa5, sizeof(successor));
	g_allocator_owner.wrap = 5;
	g_current_root_ok = false;
	g_current_empty_result = CLUSTER_UNDO_BLOCK0_OK;

	PG_TRY();
	{
		owner = cluster_tt_slot_durable_commit_writeonly(
			1, 4, 7, 100, 5, scn_encode(1, 42), &admission, &successor);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();

	UT_ASSERT(caught);
	UT_ASSERT_EQ(owner, 0);
	UT_ASSERT(admission.entered);
	UT_ASSERT_EQ(g_current_root_calls, 1);
	UT_ASSERT_EQ(g_current_acquire_calls, 1);
	UT_ASSERT_EQ(g_current_empty_calls, 0);
	UT_ASSERT_EQ(g_current_sample_calls, 0);
	UT_ASSERT_EQ(g_current_pin_calls, 0);
	UT_ASSERT_EQ(g_write_hdr_calls, 0);
	UT_ASSERT_EQ(memcmp(
		&((UndoSegmentHeaderData *)g_current_resident)->tt_slots[7],
		&resident_before, sizeof(resident_before)), 0);
	UT_ASSERT_EQ(g_current_cancel_calls, 1);
}

UT_TEST(test_precommit_writeonly_missing_pgrd_nonempty_refuses_without_write)
{
	ClusterSemanticAdmissionToken admission = source_modifier_token();
	TTSlot successor;
	TTSlot successor_before;
	volatile bool caught = false;

	reset_current_write_mock();
	memset(&successor, 0xa5, sizeof(successor));
	successor_before = successor;
	g_allocator_owner.wrap = 5;
	g_current_root_ok = false;
	g_current_empty_result = CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;

	PG_TRY();
	{
		(void)cluster_tt_slot_durable_commit_writeonly(
			1, 4, 7, 100, 5, scn_encode(1, 42), &admission, &successor);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();

	UT_ASSERT(caught);
	UT_ASSERT(admission.entered);
	UT_ASSERT_EQ(g_current_root_calls, 1);
	UT_ASSERT_EQ(g_current_acquire_calls, 1);
	UT_ASSERT_EQ(g_current_empty_calls, 0);
	UT_ASSERT_EQ(g_write_hdr_calls, 0);
	UT_ASSERT_EQ(memcmp(&successor, &successor_before, sizeof(successor)), 0);
	UT_ASSERT_EQ(g_current_cancel_calls, 1);
}


/* ============================================================
 *	U5: cluster_tt_slot_durable_lookup_by_xid (mocked block)
 * ============================================================ */
UT_TEST(test_by_xid_zero_match_miss)
{
	SCN got = InvalidScn;

	cluster_node_id = 0;
	memset(g_canned_block, 0, sizeof(g_canned_block)); /* all UNUSED */
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_lookup_by_xid(12345, &got), 0);
}
UT_TEST(test_by_xid_one_match)
{
	SCN got = InvalidScn;

	cluster_node_id = 0;
	memset(g_canned_block, 0, sizeof(g_canned_block));
	seed_block_slot(3, TT_SLOT_COMMITTED, 12345, scn_encode(1, 77));
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_lookup_by_xid(12345, &got), 1);
	UT_ASSERT_EQ((int)(scn_local(got)), 77);
}
UT_TEST(test_by_xid_two_match_ambiguous_failclosed)
{
	SCN got = InvalidScn;

	cluster_node_id = 0;
	memset(g_canned_block, 0, sizeof(g_canned_block));
	seed_block_slot(3, TT_SLOT_COMMITTED, 12345, scn_encode(1, 77));
	seed_block_slot(9, TT_SLOT_COMMITTED, 12345, scn_encode(1, 88)); /* duplicate xid */
	/* 规则 8.A: >1 match -> fail-closed (never first-match). */
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_lookup_by_xid(12345, &got), 0);
}

/* ============================================================
 *	U7: spec-3.22 durable by-xid RESOLVE enum + pure classifier.
 *
 *	cluster_tt_durable_classify maps the scan tallies to the resolve
 *	verdict; the §2.4 fix is that a slot OWNED BY xid with an unstamped
 *	(invalid) commit_scn is a 1-match (XID_MATCH_INVALID_SCN, retained,
 *	NOT below horizon) -- never conflated with a 0-match (recycled,
 *	provably below horizon).  scan_complete gates the 0/1 tally so an
 *	unreadable existing segment is SCAN_UNAVAILABLE, never a 0-match.
 * ============================================================ */

/* --- pure classifier truth table (no I/O) --- */
UT_TEST(test_classify_zero_match_complete_is_recycled)
{
	UT_ASSERT_EQ((int)cluster_tt_durable_classify(0, false, true),
				 (int)CLUSTER_TT_DURABLE_RECYCLED_ZERO_MATCH);
}
UT_TEST(test_classify_one_valid_complete_is_resolved)
{
	UT_ASSERT_EQ((int)cluster_tt_durable_classify(1, true, true),
				 (int)CLUSTER_TT_DURABLE_RESOLVED_SCN);
}
UT_TEST(test_classify_one_invalid_complete_is_invalid_scn)
{
	/* §2.4: owned-by-xid but commit_scn unstamped -> retained, NOT recycled. */
	UT_ASSERT_EQ((int)cluster_tt_durable_classify(1, false, true),
				 (int)CLUSTER_TT_DURABLE_XID_MATCH_INVALID_SCN);
}
UT_TEST(test_classify_two_match_is_ambiguous_regardless)
{
	UT_ASSERT_EQ((int)cluster_tt_durable_classify(2, true, true),
				 (int)CLUSTER_TT_DURABLE_AMBIGUOUS_WRAP);
	/* >1 is definitive ambiguity -- wins even over an incomplete scan. */
	UT_ASSERT_EQ((int)cluster_tt_durable_classify(2, false, false),
				 (int)CLUSTER_TT_DURABLE_AMBIGUOUS_WRAP);
}
UT_TEST(test_classify_incomplete_scan_is_unavailable)
{
	/* incomplete scan beats a 0- or 1-tally: cannot prove recycled / resolved. */
	UT_ASSERT_EQ((int)cluster_tt_durable_classify(0, false, false),
				 (int)CLUSTER_TT_DURABLE_SCAN_UNAVAILABLE);
	UT_ASSERT_EQ((int)cluster_tt_durable_classify(1, true, false),
				 (int)CLUSTER_TT_DURABLE_SCAN_UNAVAILABLE);
}

/* --- resolve-by-xid scan (mocked smgr) --- */
UT_TEST(test_resolve_zero_match_recycled)
{
	SCN got = scn_encode(1, 9);

	cluster_node_id = 0;
	g_unreadable_existing_segment = 0;
	memset(g_canned_block, 0, sizeof(g_canned_block));
	/* slot recycled to a DIFFERENT owner xid -> 0 matches for the target. */
	seed_block_slot(5, TT_SLOT_COMMITTED, 999, scn_encode(1, 50));
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_resolve_by_xid(12345, CLUSTER_TT_WRAP_ANY, &got, NULL,
															 NULL, NULL),
				 (int)CLUSTER_TT_DURABLE_RECYCLED_ZERO_MATCH);
	UT_ASSERT_EQ((int)SCN_VALID(got), 0); /* commit_scn cleared on non-RESOLVED */
}
UT_TEST(test_resolve_one_valid_resolved)
{
	SCN got = InvalidScn;

	cluster_node_id = 0;
	g_unreadable_existing_segment = 0;
	memset(g_canned_block, 0, sizeof(g_canned_block));
	seed_block_slot(3, TT_SLOT_COMMITTED, 12345, scn_encode(1, 77));
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_resolve_by_xid(12345, CLUSTER_TT_WRAP_ANY, &got, NULL,
															 NULL, NULL),
				 (int)CLUSTER_TT_DURABLE_RESOLVED_SCN);
	UT_ASSERT_EQ((int)(scn_local(got)), 77);
}
/* spec-5.55 D1/U1: RESOLVED_SCN also reports the matched durable slot identity
 * (out_seg/out_slot/out_wrap) for the shared resolver cache position hint; NULL
 * out params (legacy callers) resolve unchanged. */
UT_TEST(test_resolve_reports_matched_identity)
{
	SCN got = InvalidScn;
	uint16 seg = 0xffff;
	uint16 slot = 0xffff;
	uint16 wrap = 0xffff;

	cluster_node_id = 0;
	g_unreadable_existing_segment = 0;
	memset(g_canned_block, 0, sizeof(g_canned_block));
	seed_block_slot(5, TT_SLOT_COMMITTED, 12345, scn_encode(1, 77));
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_resolve_by_xid(12345, CLUSTER_TT_WRAP_ANY, &got, &seg,
															 &slot, &wrap),
				 (int)CLUSTER_TT_DURABLE_RESOLVED_SCN);
	UT_ASSERT_EQ((int)seg, 1);	/* g_canned_block answers for segment_id 1 */
	UT_ASSERT_EQ((int)slot, 5); /* the seeded slot offset */
	UT_ASSERT_EQ((int)wrap, 0); /* seeded wrap default */

	/* a 0-match (RECYCLED) leaves the out params untouched (only set on RESOLVED). */
	seg = slot = wrap = 0xffff;
	memset(g_canned_block, 0, sizeof(g_canned_block));
	seed_block_slot(5, TT_SLOT_COMMITTED, 999, scn_encode(1, 50)); /* different xid */
	got = InvalidScn;
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_resolve_by_xid(12345, CLUSTER_TT_WRAP_ANY, &got, &seg,
															 &slot, &wrap),
				 (int)CLUSTER_TT_DURABLE_RECYCLED_ZERO_MATCH);
	UT_ASSERT_EQ((int)seg, 0xffff); /* untouched on non-RESOLVED */
}
UT_TEST(test_resolve_xid_match_invalid_scn_not_recycled)
{
	SCN got = scn_encode(1, 9);

	/* THE conflation fix: COMMITTED slot owned by xid, commit_scn unstamped.
	 * Legacy lookup_by_xid counted this as 0 (SCN_VALID gate) -> looked
	 * recycled.  The resolve enum must report XID_MATCH_INVALID_SCN. */
	cluster_node_id = 0;
	g_unreadable_existing_segment = 0;
	memset(g_canned_block, 0, sizeof(g_canned_block));
	seed_block_slot(7, TT_SLOT_COMMITTED, 12345, InvalidScn);
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_resolve_by_xid(12345, CLUSTER_TT_WRAP_ANY, &got, NULL,
															 NULL, NULL),
				 (int)CLUSTER_TT_DURABLE_XID_MATCH_INVALID_SCN);
	UT_ASSERT_EQ((int)SCN_VALID(got), 0);
}
UT_TEST(test_resolve_two_match_ambiguous)
{
	SCN got = InvalidScn;

	cluster_node_id = 0;
	g_unreadable_existing_segment = 0;
	memset(g_canned_block, 0, sizeof(g_canned_block));
	seed_block_slot(3, TT_SLOT_COMMITTED, 12345, scn_encode(1, 77));
	seed_block_slot(9, TT_SLOT_COMMITTED, 12345, scn_encode(1, 88));
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_resolve_by_xid(12345, CLUSTER_TT_WRAP_ANY, &got, NULL,
															 NULL, NULL),
				 (int)CLUSTER_TT_DURABLE_AMBIGUOUS_WRAP);
}
UT_TEST(test_resolve_node_degraded_unavailable)
{
	SCN got = InvalidScn;

	/* single-node degraded: NO durable scan possible -> SCAN_UNAVAILABLE,
	 * never a 0-match (which would false-prove "recycled below horizon"). */
	cluster_node_id = -1;
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_resolve_by_xid(12345, CLUSTER_TT_WRAP_ANY, &got, NULL,
															 NULL, NULL),
				 (int)CLUSTER_TT_DURABLE_SCAN_UNAVAILABLE);
	cluster_node_id = 0; /* restore for later tests */
}
UT_TEST(test_resolve_unreadable_existing_segment_unavailable)
{
	SCN got = InvalidScn;

	/* an existing segment that fails to read -> incomplete scan -> UNAVAILABLE,
	 * never a 0-match.  Segment 2 is not the canned block (read_block fails) but
	 * file_exists() reports it present (I/O error on a real segment). */
	cluster_node_id = 0;
	memset(g_canned_block, 0, sizeof(g_canned_block));
	g_unreadable_existing_segment = 2;
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_resolve_by_xid(12345, CLUSTER_TT_WRAP_ANY, &got, NULL,
															 NULL, NULL),
				 (int)CLUSTER_TT_DURABLE_SCAN_UNAVAILABLE);
	g_unreadable_existing_segment = 0; /* restore */
}
UT_TEST(test_resolve_lookup_wrapper_maps_resolved_only)
{
	/* the thin wrapper (xmin-side caller) is true IFF RESOLVED_SCN. */
	SCN got = InvalidScn;

	cluster_node_id = 0;
	g_unreadable_existing_segment = 0;
	memset(g_canned_block, 0, sizeof(g_canned_block));
	seed_block_slot(3, TT_SLOT_COMMITTED, 12345, scn_encode(1, 77));
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_lookup_by_xid(12345, &got), 1);
	UT_ASSERT_EQ((int)(scn_local(got)), 77);

	/* unstamped owned-by-xid -> wrapper false (was already false legacy-side). */
	memset(g_canned_block, 0, sizeof(g_canned_block));
	seed_block_slot(3, TT_SLOT_COMMITTED, 12345, InvalidScn);
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_lookup_by_xid(12345, &got), 0);
}

UT_TEST(test_locate_any_state_reports_active_exact_identity)
{
	uint16 seg = 0xffff;
	uint16 slot = 0xffff;
	uint16 wrap = 0xffff;
	uint8 status = TT_SLOT_INVALID;
	UndoSegmentHeaderData *header = (UndoSegmentHeaderData *) g_canned_block;

	cluster_node_id = 0;
	g_unreadable_existing_segment = 0;
	memset(g_canned_block, 0, sizeof(g_canned_block));
	seed_block_slot(6, TT_SLOT_ACTIVE, 12345, InvalidScn);
	header->tt_slots[6].wrap = 9;
	UT_ASSERT_EQ((int) cluster_tt_slot_durable_locate_any_by_xid_origin(
		0, 12345, &seg, &slot, &wrap, &status),
		(int) CLUSTER_TT_DURABLE_LOCATE_FOUND);
	UT_ASSERT_EQ(seg, 1);
	UT_ASSERT_EQ(slot, 6);
	UT_ASSERT_EQ(wrap, 9);
	UT_ASSERT_EQ(status, TT_SLOT_ACTIVE);
}

UT_TEST(test_locate_any_state_distinguishes_missing_and_ambiguous)
{
	uint16 seg = 0xffff;

	cluster_node_id = 0;
	g_unreadable_existing_segment = 0;
	memset(g_canned_block, 0, sizeof(g_canned_block));
	UT_ASSERT_EQ((int) cluster_tt_slot_durable_locate_any_by_xid_origin(
		0, 12345, &seg, NULL, NULL, NULL),
		(int) CLUSTER_TT_DURABLE_LOCATE_MISSING);
	UT_ASSERT_EQ(seg, 0xffff);
	seed_block_slot(3, TT_SLOT_ACTIVE, 12345, InvalidScn);
	seed_block_slot(9, TT_SLOT_ABORTED, 12345, InvalidScn);
	UT_ASSERT_EQ((int) cluster_tt_slot_durable_locate_any_by_xid_origin(
		0, 12345, NULL, NULL, NULL, NULL),
		(int) CLUSTER_TT_DURABLE_LOCATE_AMBIGUOUS);
}

UT_TEST(test_locate_any_state_incomplete_scan_fails_closed)
{
	memset(g_canned_block, 0, sizeof(g_canned_block));
	g_unreadable_existing_segment = 2;
	UT_ASSERT_EQ((int) cluster_tt_slot_durable_locate_any_by_xid_origin(
		0, 12345, NULL, NULL, NULL, NULL),
		(int) CLUSTER_TT_DURABLE_LOCATE_SCAN_UNAVAILABLE);
	g_unreadable_existing_segment = 0;
}

/* The initial open can precede a complete first publication while the later
 * existence probe follows it. The scan must read that image, not skip it or
 * call it unreadable based only on the earlier open. */
UT_TEST(test_resolve_resamples_a_publication_crossing)
{
	SCN got = InvalidScn;
	uint16 seg = 0;
	uint16 slot = 0;

	cluster_node_id = 0;
	g_read_block_ok = true;
	g_canned_block_segment = 1;
	g_unreadable_existing_segment = 1;
	g_read_block_absent_once_segment = 1;
	memset(g_canned_block, 0, sizeof(g_canned_block));
	seed_block_slot(3, TT_SLOT_COMMITTED, 12345, scn_encode(1, 77));
	UT_ASSERT_EQ(cluster_tt_slot_durable_resolve_by_xid_origin(0, 12345, CLUSTER_TT_WRAP_ANY, &got,
															   &seg, &slot, NULL),
				 CLUSTER_TT_DURABLE_RESOLVED_SCN);
	UT_ASSERT_EQ(got, scn_encode(1, 77));
	UT_ASSERT_EQ(seg, 1);
	UT_ASSERT_EQ(slot, 3);
	g_unreadable_existing_segment = 0;
}

UT_TEST(test_locate_resamples_a_publication_crossing)
{
	const uint8 states[] = { TT_SLOT_ACTIVE, TT_SLOT_ABORTED };
	size_t i;

	for (i = 0; i < lengthof(states); i++) {
		uint16 seg = 0;
		uint16 slot = 0;
		uint8 status = TT_SLOT_INVALID;

		cluster_node_id = 0;
		g_read_block_ok = true;
		g_canned_block_segment = 1;
		g_unreadable_existing_segment = 1;
		g_read_block_absent_once_segment = 1;
		memset(g_canned_block, 0, sizeof(g_canned_block));
		seed_block_slot(6, states[i], 12345, InvalidScn);
		UT_ASSERT_EQ(
			cluster_tt_slot_durable_locate_any_by_xid_origin(0, 12345, &seg, &slot, NULL, &status),
			CLUSTER_TT_DURABLE_LOCATE_FOUND);
		UT_ASSERT_EQ(seg, 1);
		UT_ASSERT_EQ(slot, 6);
		UT_ASSERT_EQ(status, states[i]);
		g_unreadable_existing_segment = 0;
	}
}


/* ============================================================
 *	U6: cluster_undo_segment_tt_header_scan_pass (spec-3.13 D2-B,
 *	scan-only) — classification + zero-write invariant.
 * ============================================================ */

UT_TEST(test_scan_pass_classifies_inventory)
{
	ClusterUndoCleanerPassStats stats = { 0 };
	bool ok;

	memset(g_canned_block, 0, sizeof(g_canned_block));
	seed_block_slot(0, TT_SLOT_COMMITTED, (TransactionId)100, (SCN)5);	/* below */
	seed_block_slot(1, TT_SLOT_COMMITTED, (TransactionId)101, (SCN)20); /* == horizon: retained */
	seed_block_slot(2, TT_SLOT_COMMITTED, (TransactionId)102, (SCN)0);	/* unresolved (8.A) */
	seed_block_slot(3, TT_SLOT_ACTIVE, (TransactionId)103, (SCN)0);		/* stale residue */
	seed_block_slot(4, TT_SLOT_ABORTED, (TransactionId)104, (SCN)0);	/* no inventory impact */

	ok = cluster_undo_segment_tt_header_scan_pass(1, 1, (SCN)20, &stats);
	UT_ASSERT_EQ((int)ok, 1);
	UT_ASSERT_EQ((int)stats.header_tt_slots_below_horizon, 1);
	UT_ASSERT_EQ((int)stats.header_unresolved_committed, 1);
	UT_ASSERT_EQ((int)stats.stale_active_skipped, 1);
	UT_ASSERT_EQ((int)stats.segments_scanned, 1);
}

UT_TEST(test_scan_pass_writes_nothing)
{
	/* v0.3 (3) scan-only invariant: the pass must never call the smgr write
	 * surface, and the canned block bytes must be bit-identical after. */
	ClusterUndoCleanerPassStats stats = { 0 };
	char before[BLCKSZ];

	memset(g_canned_block, 0, sizeof(g_canned_block));
	seed_block_slot(0, TT_SLOT_COMMITTED, (TransactionId)100, (SCN)5);
	memcpy(before, g_canned_block, BLCKSZ);
	g_write_hdr_calls = 0;

	(void)cluster_undo_segment_tt_header_scan_pass(1, 1, (SCN)20, &stats);

	UT_ASSERT_EQ(g_write_hdr_calls, 0);
	UT_ASSERT_EQ(memcmp(before, g_canned_block, BLCKSZ), 0);
}

UT_TEST(test_scan_pass_read_fail_returns_false)
{
	ClusterUndoCleanerPassStats stats = { 0 };
	bool ok;

	/* segment 7 has no canned block -> read_block returns false. */
	ok = cluster_undo_segment_tt_header_scan_pass(7, 1, (SCN)20, &stats);
	UT_ASSERT_EQ((int)ok, 0);
	UT_ASSERT_EQ((int)stats.segments_scanned, 0);
}


/* ============================================================
 *	spec-3.15 D5 — durable_abort stamps ABORTED preserving identity
 * ============================================================ */

UT_TEST(test_durable_abort_preserves_identity)
{
	/* V-2: identity (xid/wrap) must survive so by-exact-key lookups
	 * resolve ABORTED instead of missing into 53R97. */
	g_read_hdr_ok = true;
	memset(&g_canned_slot, 0, sizeof(g_canned_slot));
	g_canned_slot.status = TT_SLOT_ACTIVE;
	g_canned_slot.xid = (TransactionId)777;
	g_canned_slot.wrap = 4;
	g_canned_slot.commit_scn = (SCN)123; /* stale garbage to be cleared */

	g_write_hdr_calls = 0;
	cluster_tt_slot_durable_abort(1, 7, (TransactionId)777, 4);

	UT_ASSERT_EQ(g_write_hdr_calls, 1);
	UT_ASSERT_EQ((int)g_last_written_slot.status, (int)TT_SLOT_ABORTED);
	UT_ASSERT_EQ((int)g_last_written_slot.xid, 777);
	UT_ASSERT_EQ((int)g_last_written_slot.wrap, 4);
	UT_ASSERT_EQ((int)SCN_VALID(g_last_written_slot.commit_scn), 0);
}

UT_TEST(test_typed_redo_abort_is_durable_and_idempotent)
{
	g_read_hdr_ok = true;
	g_read_hdr_calls = 0;
	memset(&g_canned_slot, 0, sizeof(g_canned_slot));
	g_canned_slot.status = TT_SLOT_ACTIVE;
	g_canned_slot.xid = 778;
	g_canned_slot.wrap = 5;
	g_write_hdr_calls = 0;
	g_fsync_segment_calls = 0;

	cluster_tt_durable_redo_abort_slot(1, 1, 7, 5, 778);
	UT_ASSERT_EQ(g_write_hdr_calls, 1);
	UT_ASSERT_EQ(g_fsync_segment_calls, 1);
	UT_ASSERT_EQ((int) g_last_written_slot.status, (int) TT_SLOT_ABORTED);
	UT_ASSERT_EQ(g_last_written_slot.xid, 778);
	UT_ASSERT_EQ(g_last_written_slot.wrap, 5);
	UT_ASSERT(UBA_is_invalid(g_last_written_slot.first_undo_block));
}

UT_TEST(test_typed_redo_set_head_requires_aborted_identity)
{
	UBA head = InvalidUba_init;

	head.raw[0] = UINT64_C(1) | (UINT64_C(9) << 32);
	head.raw[1] = UINT64_C(7);
	g_read_hdr_ok = true;
	g_read_hdr_calls = 0;
	memset(&g_canned_slot, 0, sizeof(g_canned_slot));
	g_canned_slot.status = TT_SLOT_ABORTED;
	g_canned_slot.xid = 778;
	g_canned_slot.wrap = 5;
	g_write_hdr_calls = 0;
	g_fsync_segment_calls = 0;
	cluster_tt_durable_redo_set_head_slot(1, 1, 7, 5, 778, head);
	UT_ASSERT_EQ(g_write_hdr_calls, 1);
	UT_ASSERT_EQ(g_fsync_segment_calls, 1);
	UT_ASSERT_EQ(memcmp(&g_last_written_slot.first_undo_block, &head,
		sizeof(head)), 0);

	g_canned_slot.status = TT_SLOT_COMMITTED;
	g_write_hdr_calls = 0;
	g_fsync_segment_calls = 0;
	cluster_tt_durable_redo_set_head_slot(1, 1, 7, 5, 778, head);
	UT_ASSERT_EQ(g_write_hdr_calls, 0);
	UT_ASSERT_EQ(g_fsync_segment_calls, 0);
}


/* ============================================================
 *	spec-3.16 D2 — 0x30/0x60 redo decide table idempotent + shared
 *
 *	cluster_tt_durable_redo_decide is the SINGLE last-writer-wins table
 *	for BOTH XLOG_UNDO_TT_SLOT_COMMIT (0x30) and XLOG_UNDO_TT_SLOT_ABORT
 *	(0x60); the handlers differ only in the status they stamp on APPLY
 *	(COMMITTED vs ABORTED).  These tests lock that the table is a pure
 *	function (replay-idempotent) and that the abort path reaches the
 *	same decisions (anti-divergence, L216).
 * ============================================================ */

UT_TEST(test_redo_decide_idempotent_replay)
{
	/* Pure function: same inputs -> same decision, any number of replays
	 * (the byte-level idempotency of the on-disk slot is the e2e job of
	 * t/225 L1/L2; here we lock the decision determinism). */
	int i;

	for (i = 0; i < 5; i++) {
		UT_ASSERT_EQ((int)cluster_tt_durable_redo_decide(TT_SLOT_COMMITTED, 100, 5, 100, 5),
					 (int)CLUSTER_TT_REDO_APPLY); /* same owner: idempotent re-apply */
		UT_ASSERT_EQ((int)cluster_tt_durable_redo_decide(TT_SLOT_COMMITTED, 100, 6, 100, 5),
					 (int)CLUSTER_TT_REDO_SKIP); /* disk newer wrap: stale record */
		UT_ASSERT_EQ((int)cluster_tt_durable_redo_decide(TT_SLOT_ACTIVE, 100, 5, 200, 6),
					 (int)CLUSTER_TT_REDO_APPLY); /* recycle-then-write */
	}
}

UT_TEST(test_redo_decide_abort_shares_commit_table)
{
	/* The 0x60 abort redo handler feeds the SAME decide table as 0x30.
	 * Enumerate the abort-replay scenarios and confirm they reach the
	 * same verdicts -- a future change that forks abort decisioning must
	 * break this test. */

	/* abort record replayed onto its own already-ABORTED slot (crash
	 * after the abort write): same wrap/xid -> idempotent APPLY. */
	UT_ASSERT_EQ((int)cluster_tt_durable_redo_decide(TT_SLOT_ABORTED, 777, 4, 777, 4),
				 (int)CLUSTER_TT_REDO_APPLY);
	/* abort record onto a slot a newer owner already took (higher wrap)
	 * -> SKIP (do not clobber the newer transaction). */
	UT_ASSERT_EQ((int)cluster_tt_durable_redo_decide(TT_SLOT_ACTIVE, 800, 5, 777, 4),
				 (int)CLUSTER_TT_REDO_SKIP);
	/* abort record onto the still-ACTIVE slot it owns -> APPLY. */
	UT_ASSERT_EQ((int)cluster_tt_durable_redo_decide(TT_SLOT_ACTIVE, 777, 4, 777, 4),
				 (int)CLUSTER_TT_REDO_APPLY);
	/* invalid disk status -> corruption regardless of commit/abort. */
	UT_ASSERT_EQ((int)cluster_tt_durable_redo_decide(99, 777, 4, 777, 4),
				 (int)CLUSTER_TT_REDO_BADSTATUS);
}


/* ============================================================
 *	spec-4.8 D1: crash-left ACTIVE liveness classifier truth table
 * ============================================================ */
UT_TEST(test_recovery_liveness_indeterminable_is_ambiguous)
{
	/* !determinable -> AMBIGUOUS (fail-closed -> caller aborts the slot). */
	UT_ASSERT_EQ((int)cluster_tt_recovery_classify_liveness(false, false, false),
				 (int)CLUSTER_TT_RECOVERY_AMBIGUOUS);
	UT_ASSERT_EQ((int)cluster_tt_recovery_classify_liveness(false, true, true),
				 (int)CLUSTER_TT_RECOVERY_AMBIGUOUS);
}
UT_TEST(test_recovery_liveness_committed_is_live)
{
	/* did_commit -> LIVE (never abort a committed xact, even if slot ACTIVE). */
	UT_ASSERT_EQ((int)cluster_tt_recovery_classify_liveness(true, true, false),
				 (int)CLUSTER_TT_RECOVERY_LIVE);
}
UT_TEST(test_recovery_liveness_committed_precedence_over_inprogress)
{
	/* did_commit wins over is_in_progress (fail-safe precedence). */
	UT_ASSERT_EQ((int)cluster_tt_recovery_classify_liveness(true, true, true),
				 (int)CLUSTER_TT_RECOVERY_LIVE);
}
UT_TEST(test_recovery_liveness_inprogress_is_live)
{
	/* !did_commit && is_in_progress -> LIVE (resurrected prepared 2PC). */
	UT_ASSERT_EQ((int)cluster_tt_recovery_classify_liveness(true, false, true),
				 (int)CLUSTER_TT_RECOVERY_LIVE);
}
UT_TEST(test_recovery_liveness_neither_is_dead)
{
	/* determinable, !committed, !in_progress -> DEAD (crash-left -> ABORTED). */
	UT_ASSERT_EQ((int)cluster_tt_recovery_classify_liveness(true, false, false),
				 (int)CLUSTER_TT_RECOVERY_DEAD);
}
UT_TEST(test_recovery_liveness_dead_and_ambiguous_both_abort)
{
	/* The resolve loop aborts both DEAD and AMBIGUOUS; LIVE alone is kept. */
	UT_ASSERT(cluster_tt_recovery_classify_liveness(true, false, false)
			  != CLUSTER_TT_RECOVERY_LIVE);
	UT_ASSERT(cluster_tt_recovery_classify_liveness(false, false, false)
			  != CLUSTER_TT_RECOVERY_LIVE);
}

/* ============================================================
 *	spec-4.8 D2: cross-node TT authority recovered_through LSN gate
 * ============================================================ */
UT_TEST(test_remote_authority_anchor_zero_skips_gate)
{
	/* anchor_lsn 0 (unwritten page) -> is_materialized-only (pre-D2). */
	UT_ASSERT(cluster_tt_recovery_remote_authority_covers(0, 0));
	UT_ASSERT(cluster_tt_recovery_remote_authority_covers(100, 0));
}
UT_TEST(test_remote_authority_recovered_covers_anchor)
{
	/* recovered_through >= anchor -> trust the durable outcome. */
	UT_ASSERT(cluster_tt_recovery_remote_authority_covers(200, 100));
}
UT_TEST(test_remote_authority_recovered_equal_anchor)
{
	/* boundary: recovered_through == anchor -> covered (>=). */
	UT_ASSERT(cluster_tt_recovery_remote_authority_covers(100, 100));
}
UT_TEST(test_remote_authority_under_recovered_failclosed)
{
	/* recovered_through < anchor -> NOT covered -> caller fail-closes. */
	UT_ASSERT(!cluster_tt_recovery_remote_authority_covers(99, 100));
	UT_ASSERT(!cluster_tt_recovery_remote_authority_covers(0, 1));
}

/* ============================================================
 *	spec-4.8 D3 (task#90): WRAP_ANY by-xid wrap-suspect gate
 * ============================================================ */
UT_TEST(test_wrap_suspect_wrap_checked_never_suspect)
{
	/* expected_wrap != WRAP_ANY (already disambiguated) -> never suspect. */
	UT_ASSERT(!cluster_tt_recovery_wrap_suspect(5, scn_encode(1, 10), scn_encode(1, 20), false));
}
UT_TEST(test_wrap_suspect_retention_reliable_never_suspect)
{
	/* retention reliable: below-horizon 1-match is a legit recycle-lag commit
	 * (a wrapped collision's slot would already be recycled) -> not suspect. */
	UT_ASSERT(!cluster_tt_recovery_wrap_suspect(CLUSTER_TT_WRAP_ANY, scn_encode(1, 10),
												scn_encode(1, 20), true));
}
UT_TEST(test_wrap_suspect_below_horizon_unreliable_is_suspect)
{
	/* WRAP_ANY + unreliable retention + below horizon -> wrap-suspect. */
	UT_ASSERT(cluster_tt_recovery_wrap_suspect(CLUSTER_TT_WRAP_ANY, scn_encode(1, 10),
											   scn_encode(1, 20), false));
}
UT_TEST(test_wrap_suspect_at_or_above_horizon_not_suspect)
{
	/* WRAP_ANY + unreliable + match at/above horizon -> not below -> trusted. */
	UT_ASSERT(!cluster_tt_recovery_wrap_suspect(CLUSTER_TT_WRAP_ANY, scn_encode(1, 20),
												scn_encode(1, 20), false));
	UT_ASSERT(!cluster_tt_recovery_wrap_suspect(CLUSTER_TT_WRAP_ANY, scn_encode(1, 30),
												scn_encode(1, 20), false));
}
UT_TEST(test_wrap_suspect_unjudgeable_unreliable_failclosed)
{
	/* WRAP_ANY + unreliable + invalid horizon/scn -> cannot judge -> suspect
	 * (fail-closed). */
	UT_ASSERT(cluster_tt_recovery_wrap_suspect(CLUSTER_TT_WRAP_ANY, scn_encode(1, 10), InvalidScn,
											   false));
	UT_ASSERT(cluster_tt_recovery_wrap_suspect(CLUSTER_TT_WRAP_ANY, InvalidScn, scn_encode(1, 20),
											   false));
}

/* ============================================================
 *	spec-4.8 D7 (mini-plan v2): index-aware physical rollback safety matrix
 * ============================================================ */
UT_TEST(test_revert_non_delete_failclosed)
{
	/* INSERT/UPDATE (not a DELETE record) -> fail-closed (index-unsafe),
	 * regardless of the other inputs. */
	UT_ASSERT_EQ((int)cluster_tt_recovery_classify_revert(false, true, true, false),
				 (int)CLUSTER_TT_REVERT_FAILCLOSED);
	UT_ASSERT_EQ((int)cluster_tt_recovery_classify_revert(false, false, false, true),
				 (int)CLUSTER_TT_REVERT_FAILCLOSED);
}
UT_TEST(test_revert_delete_not_aborted_failclosed)
{
	/* DELETE record but the deleter is not aborted -> never revert. */
	UT_ASSERT_EQ((int)cluster_tt_recovery_classify_revert(true, false, true, false),
				 (int)CLUSTER_TT_REVERT_FAILCLOSED);
}
UT_TEST(test_revert_delete_already_clear_skip_done)
{
	/* DELETE + aborted + xmax already clear -> idempotent SKIP (done). */
	UT_ASSERT_EQ((int)cluster_tt_recovery_classify_revert(true, true, false, true),
				 (int)CLUSTER_TT_REVERT_SKIP_DONE);
	/* SKIP_DONE wins even if identity would otherwise match. */
	UT_ASSERT_EQ((int)cluster_tt_recovery_classify_revert(true, true, true, true),
				 (int)CLUSTER_TT_REVERT_SKIP_DONE);
}
UT_TEST(test_revert_delete_identity_match_apply)
{
	/* DELETE + aborted + not-clear + tuple still carries this deleter's xmax
	 * -> APPLY (clear xmax). */
	UT_ASSERT_EQ((int)cluster_tt_recovery_classify_revert(true, true, true, false),
				 (int)CLUSTER_TT_REVERT_APPLY);
}
UT_TEST(test_revert_delete_identity_mismatch_failclosed)
{
	/* DELETE + aborted + not-clear but the tuple no longer carries this
	 * deleter's xmax (slot/tuple reused) -> fail-closed (identity gate). */
	UT_ASSERT_EQ((int)cluster_tt_recovery_classify_revert(true, true, false, false),
				 (int)CLUSTER_TT_REVERT_FAILCLOSED);
}


int
main(int argc, char **argv)
{
	UT_PLAN(97);

	UT_RUN(test_layout_sizes);

	UT_RUN(test_redo_decide_newer_wrap_applies);
	UT_RUN(test_redo_decide_same_wrap_same_xid_idempotent);
	UT_RUN(test_redo_decide_same_wrap_diff_xid_applies);
	UT_RUN(test_redo_decide_unused_slot_applies);
	UT_RUN(test_redo_decide_older_wrap_skips);
	UT_RUN(test_redo_decide_bad_status);

	UT_RUN(test_slot_match_exact);
	UT_RUN(test_slot_match_wrong_xid);
	UT_RUN(test_slot_match_unused);
	UT_RUN(test_slot_match_invalid_scn);

	UT_RUN(test_lookup_match);
	UT_RUN(test_lookup_wrong_xid_miss);
	UT_RUN(test_lookup_unused_miss);
	UT_RUN(test_lookup_read_fail_miss);
	UT_RUN(test_lookup_committed_stable_success);
	UT_RUN(test_lookup_committed_stable_torn_read_failclosed);
	UT_RUN(test_lookup_committed_stable_uncommitted_failclosed);
	UT_RUN(test_read_exact_stable_accepts_known_status_and_preserves_32_bytes);
	UT_RUN(test_read_exact_stable_rejects_wrong_xid);
	UT_RUN(test_read_exact_stable_rejects_wrong_wrap);
	UT_RUN(test_read_exact_stable_rejects_unknown_status);
	UT_RUN(test_read_exact_stable_rejects_torn_slot);
	UT_RUN(test_read_exact_stable_rejects_either_io_failure);
	UT_RUN(test_active_bind_predecessor_table_is_exact);
	UT_RUN(test_terminal_transition_requires_same_exact_active_entity);
	UT_RUN(test_active_publish_wal_precedes_identical_disk_and_resident_successor);
	UT_RUN(test_active_publish_waits_for_released_origin_notification_before_bind);
	UT_RUN(test_active_publish_final_xcur_drift_stays_unpublished);
	UT_RUN(test_active_publish_prebind_failures_cancel_only_owned_pending_reservation);
	UT_RUN(test_active_publish_postbind_failures_block_owned_pending_reservation);
	UT_RUN(test_active_publish_existing_open_error_never_cancels_or_blocks_entry);
	UT_RUN(test_active_publish_requires_exact_root_and_resident_disk_generation);
	UT_RUN(test_active_publish_rejects_allocator_identity_drift_before_wal);
	UT_RUN(test_active_publish_returns_unpublished_retry_when_allocator_already_rolled);
	UT_RUN(test_active_publish_rollover_with_old_root_or_current_unavailable_is_unpublished);
	UT_RUN(test_active_publish_retries_rollover_before_xcur_allocator_linearization);
	UT_RUN(test_active_publish_rejects_same_wrap_different_xid_before_wal);
	UT_RUN(test_precommit_writeonly_publishes_identical_durable_and_resident_successor);
	UT_RUN(test_precommit_writeonly_target_open_reuses_same_block0_authority);
	UT_RUN(test_precommit_writeonly_accepts_exact_active_on_rolled_away_segment);
	UT_RUN(test_precommit_writeonly_root_drift_never_writes_or_publishes);
	UT_RUN(test_precommit_writeonly_missing_current_authority_never_writes_or_publishes);
	UT_RUN(test_precommit_writeonly_durable_failure_never_publishes_resident);
	UT_RUN(test_precommit_writeonly_postwrite_release_failure_is_nothrow_cleanup);
	UT_RUN(test_ordinary_abort_flushes_exact_carrier_before_terminal_write);
	UT_RUN(test_precommit_writeonly_missing_canonical_active_refuses_without_write);
	UT_RUN(test_precommit_writeonly_missing_pgrd_nonempty_refuses_without_write);

	UT_RUN(test_by_xid_zero_match_miss);
	UT_RUN(test_by_xid_one_match);
	UT_RUN(test_by_xid_two_match_ambiguous_failclosed);

	UT_RUN(test_classify_zero_match_complete_is_recycled);
	UT_RUN(test_classify_one_valid_complete_is_resolved);
	UT_RUN(test_classify_one_invalid_complete_is_invalid_scn);
	UT_RUN(test_classify_two_match_is_ambiguous_regardless);
	UT_RUN(test_classify_incomplete_scan_is_unavailable);
	UT_RUN(test_resolve_zero_match_recycled);
	UT_RUN(test_resolve_one_valid_resolved);
	UT_RUN(test_resolve_reports_matched_identity);
	UT_RUN(test_resolve_xid_match_invalid_scn_not_recycled);
	UT_RUN(test_resolve_two_match_ambiguous);
	UT_RUN(test_resolve_node_degraded_unavailable);
	UT_RUN(test_resolve_unreadable_existing_segment_unavailable);
	UT_RUN(test_resolve_lookup_wrapper_maps_resolved_only);
	UT_RUN(test_locate_any_state_reports_active_exact_identity);
	UT_RUN(test_locate_any_state_distinguishes_missing_and_ambiguous);
	UT_RUN(test_locate_any_state_incomplete_scan_fails_closed);
	UT_RUN(test_resolve_resamples_a_publication_crossing);
	UT_RUN(test_locate_resamples_a_publication_crossing);

	UT_RUN(test_scan_pass_classifies_inventory);
	UT_RUN(test_scan_pass_writes_nothing);
	UT_RUN(test_scan_pass_read_fail_returns_false);

	UT_RUN(test_durable_abort_preserves_identity);
	UT_RUN(test_typed_redo_abort_is_durable_and_idempotent);
	UT_RUN(test_typed_redo_set_head_requires_aborted_identity);

	UT_RUN(test_redo_decide_idempotent_replay);
	UT_RUN(test_redo_decide_abort_shares_commit_table);

	UT_RUN(test_recovery_liveness_indeterminable_is_ambiguous);
	UT_RUN(test_recovery_liveness_committed_is_live);
	UT_RUN(test_recovery_liveness_committed_precedence_over_inprogress);
	UT_RUN(test_recovery_liveness_inprogress_is_live);
	UT_RUN(test_recovery_liveness_neither_is_dead);
	UT_RUN(test_recovery_liveness_dead_and_ambiguous_both_abort);

	UT_RUN(test_remote_authority_anchor_zero_skips_gate);
	UT_RUN(test_remote_authority_recovered_covers_anchor);
	UT_RUN(test_remote_authority_recovered_equal_anchor);
	UT_RUN(test_remote_authority_under_recovered_failclosed);

	UT_RUN(test_wrap_suspect_wrap_checked_never_suspect);
	UT_RUN(test_wrap_suspect_retention_reliable_never_suspect);
	UT_RUN(test_wrap_suspect_below_horizon_unreliable_is_suspect);
	UT_RUN(test_wrap_suspect_at_or_above_horizon_not_suspect);
	UT_RUN(test_wrap_suspect_unjudgeable_unreliable_failclosed);

	UT_RUN(test_revert_non_delete_failclosed);
	UT_RUN(test_revert_delete_not_aborted_failclosed);
	UT_RUN(test_revert_delete_already_clear_skip_done);
	UT_RUN(test_revert_delete_identity_match_apply);
	UT_RUN(test_revert_delete_identity_mismatch_failclosed);

	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
