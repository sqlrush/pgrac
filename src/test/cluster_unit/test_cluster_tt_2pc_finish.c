/*-------------------------------------------------------------------------
 *
 * test_cluster_tt_2pc_finish.c
 *	  cluster_unit tests for the staged two-phase TT finish in
 *	  cluster_tt_2pc.c, linked as the product object.
 *
 *	      F1  normal finish: emit, apply, publish, release once
 *	      F2  abort after the stage releases stage and admission once
 *	      F3  abort after the record releases without publishing
 *	      F4  a failed prefinish releases what it staged; abort is a no-op
 *	      F5  stamps that would precede their flushed record PANIC
 *	      F6  the abort callback is registered once per backend
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_tt_2pc_finish.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/twophase_rmgr.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_itl_touch.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_subtrans.h"
#include "cluster/cluster_tt_2pc.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_tt_local.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_tt_status_hint.h"
#include "cluster/cluster_undo_record_api.h"

#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/* ============================================================
 *	elog stubs: ERROR and above unwind to the caller's PG_TRY
 * ============================================================ */
static int last_elevel = 0;

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	if (elevel >= ERROR)
		last_elevel = elevel;
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
errcode(int sqlerrcode pg_attribute_unused())
{
	return 0;
}
int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}
void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	fprintf(stderr, "Assert(%s) failed at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

void *
palloc(Size size)
{
	return calloc(1, size);
}
void
pfree(void *pointer)
{
	free(pointer);
}

/* ============================================================
 *	Seams of the staged finish
 * ============================================================ */
bool cluster_enabled = true;
int cluster_node_id = 0;

static XactCallback registered_callback = NULL;
static int registrations = 0;
static int stages, emits, applies, releases, enters, leaves, marks, installs, hints;
static int fail_stage_on = 0; /* 1-based binding index whose stage throws */
static XLogRecPtr flush_ptr = 0;

void
RegisterXactCallback(XactCallback callback, void *arg pg_attribute_unused())
{
	registered_callback = callback;
	registrations++;
}

XLogRecPtr
GetFlushRecPtr(TimeLineID *insertTLI pg_attribute_unused())
{
	return flush_ptr;
}

ClusterJoinGateVerdict
cluster_reconfig_self_join_gate_verdict(void)
{
	return CLUSTER_JOIN_GATE_ALLOW;
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_modifier_enter(bool writable_admission pg_attribute_unused(),
										   ClusterSemanticAdmissionToken *token)
{
	enters++;
	token->entered = true;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

bool
cluster_semantic_activation_modifier_recheck(const ClusterSemanticAdmissionToken *token,
											 bool writable_admission pg_attribute_unused())
{
	return token->entered;
}

void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	if (token->entered)
		leaves++;
	token->entered = false;
}

void
cluster_tt_slot_durable_prepared_stage(uint32 segment_id, uint16 slot_offset, TransactionId xid,
									   uint16 wrap, bool commit, SCN commit_scn,
									   UBA head pg_attribute_unused(),
									   ClusterTTPreparedStage *stage)
{
	memset(stage, 0, sizeof(*stage));
	stage->fd = -1;
	if (++stages == fail_stage_on)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("slot no longer names binding")));
	stage->segment_id = segment_id;
	stage->slot_offset = slot_offset;
	stage->fd = 77;
	stage->successor.xid = xid;
	stage->successor.wrap = wrap;
	stage->successor.status = commit ? TT_SLOT_COMMITTED : TT_SLOT_ABORTED;
	stage->successor.commit_scn = commit_scn;
}

void
cluster_tt_slot_durable_prepared_emit(const ClusterTTPreparedStage *stage)
{
	UT_ASSERT_EQ(stage->fd, 77);
	emits++;
}

void
cluster_tt_slot_durable_prepared_apply(ClusterTTPreparedStage *stage)
{
	UT_ASSERT_EQ(stage->fd, 77);
	applies++;
}

void
cluster_tt_slot_durable_prepared_release(ClusterTTPreparedStage *stage)
{
	releases++;
	stage->fd = -1;
}

void
cluster_tt_slot_mark_committed(uint32 segment_id pg_attribute_unused(),
							   uint16 slot_offset pg_attribute_unused(),
							   TransactionId xid pg_attribute_unused(),
							   SCN commit_scn pg_attribute_unused())
{
	marks++;
}

void
cluster_tt_slot_mark_aborted(uint32 segment_id pg_attribute_unused(),
							 uint16 slot_offset pg_attribute_unused(),
							 TransactionId xid pg_attribute_unused())
{
	marks++;
}

ClusterSemanticAdmissionResult
cluster_tt_status_source_dispatch(ClusterTTStatusSourceOp op pg_attribute_unused(),
								  const ClusterTTStatusSourceRequest *request pg_attribute_unused(),
								  ClusterTTStatusSourceResult *result pg_attribute_unused())
{
	installs++;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

ClusterSemanticAdmissionResult
cluster_tt_status_hint_source_dispatch(ClusterTTStatusHintSourceOp op pg_attribute_unused(),
									   const ClusterTTStatusHintSourceRequest *request
										   pg_attribute_unused())
{
	hints++;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

/* ============================================================
 *	Unused by the finish path (PREPARE, recovery, standby)
 * ============================================================ */
void
cluster_itl_touch_reset_at_end_xact(void)
{}
uint32
cluster_subtrans_export_links(struct ClusterTT2PCSubLink *dst pg_attribute_unused(),
							  uint32 max pg_attribute_unused())
{
	return 0;
}
void
cluster_subtrans_reset_local_links(void)
{}
uint16
cluster_tt_local_export_bindings(struct ClusterTT2PCBinding *dst pg_attribute_unused(),
								 uint16 max pg_attribute_unused())
{
	return 0;
}
void
cluster_tt_local_reset_binding(void)
{}
bool
cluster_tt_slot_protect(uint32 segment_id pg_attribute_unused(),
						uint16 slot_offset pg_attribute_unused(), uint16 wrap pg_attribute_unused(),
						TransactionId xid pg_attribute_unused())
{
	return true;
}
uint32
cluster_tt_slot_unprotect_xid(TransactionId xid pg_attribute_unused())
{
	return 0;
}
UBA
cluster_undo_local_head_get(uint16 tt_slot_segment_id pg_attribute_unused(),
							uint16 tt_slot_offset pg_attribute_unused())
{
	UBA invalid = InvalidUba_init;

	return invalid;
}
void
cluster_undo_record_xact_reset(void)
{}
void
RegisterTwoPhaseRecord(TwoPhaseRmgrId rmid pg_attribute_unused(), uint16 info pg_attribute_unused(),
					   const void *data pg_attribute_unused(), uint32 len pg_attribute_unused())
{}
void
cluster_vis_bump_recovery_2pc_standby_rebuilds(void)
{}
void
cluster_vis_bump_recovery_overlay_rebuild_count(void)
{}
void
cluster_vis_bump_twopc_postprepare_transfers(void)
{}
void
cluster_vis_bump_twopc_prefinish_commits(void)
{}
void
cluster_vis_bump_twopc_prefinish_aborts(void)
{}
void
cluster_vis_bump_twopc_prepare_records(void)
{}
void
cluster_vis_bump_twopc_recover_rebinds(void)
{}

/* ============================================================
 *	Fixture
 * ============================================================ */
#define XID 1001
#define RECORD_END ((XLogRecPtr)0x5000)

static char record[256];
static uint32 record_len;

static void
reset(void)
{
	stages = emits = applies = releases = enters = leaves = marks = installs = hints = 0;
	fail_stage_on = 0;
	flush_ptr = RECORD_END;
	last_elevel = 0;
}

/* A prepared transaction with two bindings in this node's segments. */
static void
make_record(void)
{
	ClusterTT2PCBinding b[2];

	memset(b, 0, sizeof(b));
	for (int i = 0; i < 2; i++) {
		b[i].undo_segment_id = 1 + i;
		b[i].slot_offset = 5 + i;
		b[i].wrap = 2;
		b[i].cluster_epoch = 9;
		b[i].xid = XID;
	}
	record_len = cluster_tt_2pc_serialize(b, NULL, 2, NULL, 0, record, sizeof(record));
	if (record_len == 0)
		abort();
}

/* Run fn() under a PG_TRY; report whether it raised. */
static bool
raises(void (*fn)(void))
{
	sigjmp_buf local;
	sigjmp_buf *save = PG_exception_stack;
	volatile bool raised = false;

	if (sigsetjmp(local, 0) == 0) {
		PG_exception_stack = &local;
		fn();
	} else
		raised = true;
	PG_exception_stack = save;
	return raised;
}

static void
prefinish_commit(void)
{
	cluster_tt_twophase_prefinish(XID, (SCN)42, true, record, record_len);
}

static void
apply_staged(void)
{
	cluster_tt_twophase_apply_staged(XID, RECORD_END);
}

/* The transaction ends abnormally: deliver the event to the registered
 * callback (none registered = the stage is never released). */
static void
xact_event(XactEvent event)
{
	UT_ASSERT(registered_callback != NULL);
	if (registered_callback != NULL)
		registered_callback(event, NULL);
}

static void
abort_event(void)
{
	xact_event(XACT_EVENT_ABORT);
}

/* ============================================================
 *	Tests
 * ============================================================ */
UT_TEST(test_f1_normal_finish_publishes_and_releases_once)
{
	reset();
	UT_ASSERT(!raises(prefinish_commit));
	UT_ASSERT_EQ(stages, 2);
	UT_ASSERT_EQ(enters, 1);
	UT_ASSERT_EQ(emits + applies + marks + installs + hints + releases + leaves, 0);
	cluster_tt_twophase_emit_staged(XID);
	UT_ASSERT_EQ(emits, 2);
	UT_ASSERT(!raises(apply_staged));
	UT_ASSERT_EQ(applies, 2);
	UT_ASSERT_EQ(marks + installs + hints, 0);
	cluster_tt_twophase_postfinish(XID);
	UT_ASSERT_EQ(marks, 2);
	UT_ASSERT_EQ(installs, 2);
	UT_ASSERT_EQ(hints, 2);
	UT_ASSERT_EQ(releases, 2);
	UT_ASSERT_EQ(leaves, 1);

	/* The transaction's end does not release twice. */
	abort_event();
	UT_ASSERT_EQ(releases, 2);
	UT_ASSERT_EQ(leaves, 1);
}

UT_TEST(test_f2_abort_after_stage_releases_stage_and_admission)
{
	reset();
	UT_ASSERT(!raises(prefinish_commit));
	abort_event();
	UT_ASSERT_EQ(releases, 2);
	UT_ASSERT_EQ(leaves, 1);
	UT_ASSERT_EQ(emits + applies + marks + installs + hints, 0);
	abort_event();
	UT_ASSERT_EQ(releases, 2);
	UT_ASSERT_EQ(leaves, 1);

	/* The parallel-worker abort releases as well. */
	reset();
	UT_ASSERT(!raises(prefinish_commit));
	xact_event(XACT_EVENT_PARALLEL_ABORT);
	UT_ASSERT_EQ(releases, 2);
	UT_ASSERT_EQ(leaves, 1);

	/* Commit-side events leave a live stage alone. */
	reset();
	UT_ASSERT(!raises(prefinish_commit));
	xact_event(XACT_EVENT_PRE_COMMIT);
	UT_ASSERT_EQ(releases + leaves, 0);
	abort_event();

	/* The next finish in this backend is not refused as already staged. */
	reset();
	UT_ASSERT(!raises(prefinish_commit));
	UT_ASSERT_EQ(stages, 2);
	abort_event();
}

UT_TEST(test_f3_abort_after_record_releases_without_publishing)
{
	reset();
	UT_ASSERT(!raises(prefinish_commit));
	cluster_tt_twophase_emit_staged(XID);
	UT_ASSERT(!raises(apply_staged));
	abort_event();
	UT_ASSERT_EQ(applies, 2);
	UT_ASSERT_EQ(marks + installs + hints, 0);
	UT_ASSERT_EQ(releases, 2);
	UT_ASSERT_EQ(leaves, 1);
}

UT_TEST(test_f4_failed_prefinish_releases_what_it_staged)
{
	reset();
	fail_stage_on = 2;
	UT_ASSERT(raises(prefinish_commit));
	UT_ASSERT_EQ(last_elevel, ERROR);
	UT_ASSERT_EQ(releases, 2);
	UT_ASSERT_EQ(leaves, 1);
	abort_event();
	UT_ASSERT_EQ(releases, 2);
	UT_ASSERT_EQ(leaves, 1);
	cluster_tt_twophase_emit_staged(XID);
	UT_ASSERT_EQ(emits, 0);
}

UT_TEST(test_f5_stamps_before_their_record_panic)
{
	reset();
	UT_ASSERT(!raises(prefinish_commit));
	cluster_tt_twophase_emit_staged(XID);
	flush_ptr = RECORD_END - 1;
	UT_ASSERT(raises(apply_staged));
	UT_ASSERT_EQ(last_elevel, PANIC);
	UT_ASSERT_EQ(applies, 0);
	abort_event();
	UT_ASSERT_EQ(leaves, 1);
}

UT_TEST(test_f6_abort_callback_registered_once)
{
	UT_ASSERT_EQ(registrations, 1);
}

int
main(void)
{
	make_record();
	UT_PLAN(6);
	UT_RUN(test_f1_normal_finish_publishes_and_releases_once);
	UT_RUN(test_f2_abort_after_stage_releases_stage_and_admission);
	UT_RUN(test_f3_abort_after_record_releases_without_publishing);
	UT_RUN(test_f4_failed_prefinish_releases_what_it_staged);
	UT_RUN(test_f5_stamps_before_their_record_panic);
	UT_RUN(test_f6_abort_callback_registered_once);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
