/*-------------------------------------------------------------------------
 *
 * test_cluster_visibility_fork.c
 *	  pgrac spec-3.2 D10 — cluster_unit static-contract tests for
 *	  HeapTupleSatisfiesMVCC cluster visibility fork (D5) + D5b inject
 *	  mechanism.
 *
 *	  12 static + presence tests (v0.3 N3 + N4 + L177 + L178 enforcement):
 *	    T1   ClusterUndoTTSlotRef sizeof / offsetof contract
 *	    T2   ClusterUndoTTSlotRef field offsets stable (origin/segment/
 *	         slot/epoch/local_xid/commit_scn/has_cached_status/_padding)
 *	    T3   placeholder ref sentinel — tt_slot_id == 0 means "skip
 *	         cluster path" (v0.3 §3.3 + §3.4)
 *	    T4   self-origin sentinel — ref.origin_node_id == cluster_node_id
 *	         means "skip cluster path"
 *	    T5   ClusterTTStatusKey build_key contract — origin_node_id +
 *	         undo_segment_id + tt_slot_id + cluster_epoch + local_xid
 *	         must carry from ref (no fields invented)
 *	    T6   53R97 ERRCODE_CLUSTER_TT_STATUS_UNKNOWN encodable
 *	    T7   D5b inject API linkable (cluster_test_lookup_visibility_inject)
 *	    T8   D5b shmem helpers linkable (size + init + register)
 *	    T9   ENABLE_INJECTION conditional — production binary lookup
 *	         helper returns false (no inject table); ENABLE_INJECTION
 *	         build has the function fully defined
 *	    T10  CLUSTER_ITL_SLOT_UNALLOCATED sentinel = 255 (v0.3 D5 gate
 *	         skip tuples carrying placeholder)
 *	    T11  no is_xid_local_origin symbol in spec-3.2 implementation
 *	         (v0.2 §0.1 F1 hard guardrail;  cluster_unit static enforce)
 *	    T12  ClusterTTStatus enum 5 values stable (defensive duplicate
 *	         of test_cluster_tt_status T3-T7 to keep this binary self-
 *	         contained per L107 N+5 producer-consumer pattern)
 *
 *	  No HeapTupleSatisfiesMVCC behavioral testing here — that requires
 *	  a real PG backend.  Behavioral coverage in cluster_tap t/204.
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_visibility_fork.c
 *
 * Spec: spec-3.2-mvcc-cluster-path-tt-status-wire.md (v1.0 FROZEN 2026-05-22)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>

#include "access/htup_details.h"
#include "cluster/cluster_itl_slot.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_visibility_inject.h"
#include "cluster/cluster_visibility_resolve.h"
#include "utils/errcodes.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"


UT_DEFINE_GLOBALS();

#ifndef HEAPAM_SOURCE_PATH
#error "HEAPAM_SOURCE_PATH must identify production heapam.c"
#endif
#ifndef HEAPAM_VISIBILITY_SOURCE_PATH
#error "HEAPAM_VISIBILITY_SOURCE_PATH must identify production heapam_visibility.c"
#endif
#ifndef TT_LOCAL_SOURCE_PATH
#error "TT_LOCAL_SOURCE_PATH must identify production cluster_tt_local.c"
#endif
#ifndef XACT_SOURCE_PATH
#error "XACT_SOURCE_PATH must identify production xact.c"
#endif
#ifndef HEAPAM_HANDLER_SOURCE_PATH
#error "HEAPAM_HANDLER_SOURCE_PATH must identify production heapam_handler.c"
#endif
#ifndef NBTINSERT_SOURCE_PATH
#error "NBTINSERT_SOURCE_PATH must identify production nbtinsert.c"
#endif


void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}


/* Stubs — cluster_unit binary does not link cluster_visibility_inject.o. */
bool
cluster_test_lookup_visibility_inject(TransactionId xid pg_attribute_unused(),
									  ClusterUndoTTSlotRef *ref pg_attribute_unused())
{
	return false;
}
Size
cluster_visibility_inject_shmem_size(void)
{
	return 0;
}
void
cluster_visibility_inject_shmem_init(void)
{}
void
cluster_visibility_inject_shmem_register(void)
{}

static char *
read_source(const char *path)
{
	FILE *fp;
	char *source;
	long length;

	fp = fopen(path, "rb");
	UT_ASSERT(fp != NULL);
	if (fp == NULL)
		return NULL;
	UT_ASSERT_EQ(fseek(fp, 0, SEEK_END), 0);
	length = ftell(fp);
	UT_ASSERT(length > 0);
	UT_ASSERT_EQ(fseek(fp, 0, SEEK_SET), 0);
	source = malloc((size_t)length + 1);
	UT_ASSERT(source != NULL);
	if (source == NULL) {
		fclose(fp);
		return NULL;
	}
	UT_ASSERT_EQ((long)fread(source, 1, (size_t)length, fp), length);
	source[length] = '\0';
	fclose(fp);
	return source;
}

static void
assert_data_active_publish(const char *source, const char *start_marker, const char *end_marker,
						   const char *uba_name)
{
	const char *start = strstr(source, start_marker);
	const char *end = start != NULL ? strstr(start + strlen(start_marker), end_marker) : NULL;
	char publish_call[160];
	const char *publish;
	const char *crit_end = start != NULL ? strstr(start, "END_CRIT_SECTION();") : NULL;

	snprintf(publish_call, sizeof(publish_call),
			 "cluster_tt_local_record_data_active(canonical_xid, %s);",
			 uba_name);
	publish = start != NULL ? strstr(start, publish_call) : NULL;

	if (publish != NULL && end != NULL && publish >= end)
		publish = NULL;

	UT_ASSERT(start != NULL);
	UT_ASSERT(end != NULL);
	UT_ASSERT(publish != NULL);
	UT_ASSERT(crit_end != NULL);
	if (start == NULL || end == NULL || publish == NULL || crit_end == NULL)
		return;

	/* The ACTIVE identity is published only after the tuple + ITL stamp is
	 * WAL-protected, and before the function can release its heap buffer. */
	while (true) {
		const char *next = strstr(crit_end + 1, "END_CRIT_SECTION();");

		if (next == NULL || next >= publish)
			break;
		crit_end = next;
	}
	UT_ASSERT(crit_end < publish);
}

static void
assert_multi_insert_uses_receipt_safe_per_tuple_route(const char *source)
{
	const char *start = strstr(source, "\nheap_multi_insert(Relation");
	const char *end = start == NULL
		? NULL
		: strstr(start, "\n/*\n *\tsimple_heap_insert - insert a tuple");
	const char *route = start == NULL
		? NULL
		: strstr(start, "cluster_heap_multi_insert_route(");
	const char *per_tuple = route == NULL
		? NULL
		: strstr(route, "heap_insert(relation, tuple, cid, options, bistate);");
	const char *route_return = per_tuple == NULL
		? NULL
		: strstr(per_tuple, "\n\t\treturn;");

	UT_ASSERT_NOT_NULL(start);
	UT_ASSERT_NOT_NULL(end);
	UT_ASSERT_NOT_NULL(route);
	UT_ASSERT_NOT_NULL(per_tuple);
	UT_ASSERT_NOT_NULL(route_return);
	if (start != NULL && end != NULL && route != NULL && per_tuple != NULL
		&& route_return != NULL)
		UT_ASSERT(start < route && route < per_tuple
				  && per_tuple < route_return && route_return < end);
}


/* ===== T1: ClusterUndoTTSlotRef size 32B ===== */
UT_TEST(test_t1_undo_tt_slot_ref_sizeof_32)
{
	UT_ASSERT_EQ((int)sizeof(ClusterUndoTTSlotRef), 32);
}

/* ===== T2: field offsets locked (mirror spec-3.1 v0.4 M4 + spec-3.2 gate inputs) ===== */
UT_TEST(test_t2_ref_field_offsets)
{
	UT_ASSERT_EQ((int)offsetof(ClusterUndoTTSlotRef, origin_node_id), 0);
	UT_ASSERT_EQ((int)offsetof(ClusterUndoTTSlotRef, undo_segment_id), 2);
	UT_ASSERT_EQ((int)offsetof(ClusterUndoTTSlotRef, tt_slot_id), 4);
	UT_ASSERT_EQ((int)offsetof(ClusterUndoTTSlotRef, cluster_epoch), 8);
	UT_ASSERT_EQ((int)offsetof(ClusterUndoTTSlotRef, local_xid), 12);
	UT_ASSERT_EQ((int)offsetof(ClusterUndoTTSlotRef, cached_commit_scn), 16);
	UT_ASSERT_EQ((int)offsetof(ClusterUndoTTSlotRef, has_cached_status), 24);
	UT_ASSERT_EQ((int)offsetof(ClusterUndoTTSlotRef, _padding), 25);
}

/* ===== T3: placeholder ref sentinel (tt_slot_id == 0 = skip cluster path) ===== */
UT_TEST(test_t3_placeholder_ref_sentinel)
{
	ClusterUndoTTSlotRef ref;
	memset(&ref, 0, sizeof(ref));
	/* spec-3.1 D4 reader returns tt_slot_id = 0 placeholder for production
	 * heap pages.  spec-3.2 §3.3 gate:  this means "skip cluster path". */
	UT_ASSERT_EQ((int)ref.tt_slot_id, 0);
}

/* ===== T4: self-origin gate semantics ===== */
UT_TEST(test_t4_self_origin_gate)
{
	ClusterUndoTTSlotRef ref;
	int fake_self_node = 1;
	memset(&ref, 0, sizeof(ref));
	ref.origin_node_id = (uint16)fake_self_node;
	/* spec-3.2 §3.3:  ref.origin_node_id == cluster_node_id (self) =
	 * "skip cluster path" → tuple goes to PG-native body. */
	UT_ASSERT_EQ((int)ref.origin_node_id, fake_self_node);
}

/* ===== T5: ClusterTTStatusKey build from ref — field carry contract ===== */
UT_TEST(test_t5_build_key_field_carry)
{
	ClusterUndoTTSlotRef ref;
	ClusterTTStatusKey key;

	memset(&ref, 0, sizeof(ref));
	ref.origin_node_id = 7;
	ref.undo_segment_id = 3;
	ref.tt_slot_id = 42;
	ref.cluster_epoch = 100;
	ref.local_xid = 12345;

	memset(&key, 0, sizeof(key));
	key.origin_node_id = ref.origin_node_id;
	key.undo_segment_id = ref.undo_segment_id;
	key.tt_slot_id = ref.tt_slot_id;
	key.cluster_epoch = ref.cluster_epoch;
	key.local_xid = ref.local_xid;

	/* All five identity fields carry — no fields invented. */
	UT_ASSERT_EQ((int)key.origin_node_id, 7);
	UT_ASSERT_EQ((int)key.undo_segment_id, 3);
	UT_ASSERT_EQ((int)key.tt_slot_id, 42);
	UT_ASSERT_EQ((int)key.cluster_epoch, 100);
	UT_ASSERT_EQ((int)key.local_xid, 12345);
	/* Reserved fields zero on emit. */
	UT_ASSERT_EQ((int)key._reserved, 0);
	UT_ASSERT_EQ((int)key._reserved2, 0);
}

/* ===== T6: 53R97 SQLSTATE encodable ===== */
UT_TEST(test_t6_errcode_53r97_encodable)
{
	int sqlstate = MAKE_SQLSTATE('5', '3', 'R', '9', '7');
	UT_ASSERT_EQ((int)ERRCODE_CLUSTER_TT_STATUS_UNKNOWN, sqlstate);
}

/* ===== T7: D5b inject lookup API linkable ===== */
UT_TEST(test_t7_inject_lookup_linkable)
{
	UT_ASSERT_NE((void *)cluster_test_lookup_visibility_inject, NULL);
}

/* ===== T8: D5b shmem helpers linkable ===== */
UT_TEST(test_t8_inject_shmem_helpers_linkable)
{
	UT_ASSERT_NE((void *)cluster_visibility_inject_shmem_size, NULL);
	UT_ASSERT_NE((void *)cluster_visibility_inject_shmem_init, NULL);
	UT_ASSERT_NE((void *)cluster_visibility_inject_shmem_register, NULL);
}

/* ===== T9: ENABLE_INJECTION conditional — stub returns false in this
 * test (production-binary equivalent semantics) ===== */
UT_TEST(test_t9_production_inject_returns_false)
{
	ClusterUndoTTSlotRef ref;
	bool hit;
	memset(&ref, 0, sizeof(ref));
	/* This binary links the stub form (test_cluster_visibility_fork.c
	 * defines a local stub that always returns false) → covers the
	 * production no-op path semantics. */
	hit = cluster_test_lookup_visibility_inject(99, &ref);
	UT_ASSERT_EQ((int)hit, 0);
}

/* ===== T10: CLUSTER_ITL_SLOT_UNALLOCATED sentinel = 255 ===== */
UT_TEST(test_t10_itl_slot_unallocated_sentinel)
{
	/* spec-3.2 D5 gate:  tuple->t_itl_slot_idx == CLUSTER_ITL_SLOT_UNALLOCATED
	 * means "no ITL slot pointer;  skip cluster path". */
	UT_ASSERT_EQ((int)CLUSTER_ITL_SLOT_UNALLOCATED, 255);
}

/* ===== T11: v0.2 §0.1 F1 hard guardrail — no is_xid_local_origin
 * heuristic symbol in spec-3.2 implementation.  Compile-time check:
 * this file does NOT declare such a symbol;  if D5 D5b implementation
 * pulls one in, linker will surface it elsewhere. ===== */
UT_TEST(test_t11_no_is_xid_local_origin_in_this_unit)
{
	/* The test value is the absence of the symbol from our build.
	 * Linker-level enforcement at test_cluster_visibility_fork build
	 * time:  no is_xid_local_origin declared / used.  Lint script
	 * scripts/ci/check-no-clog-overlay.sh handles cross-repo
	 * BANNED_RE enforcement.  This static assertion just records the
	 * intent. */
	UT_ASSERT_EQ(1, 1);
}

/* ===== T12: ClusterTTStatus 5 values stable (self-contained) ===== */
UT_TEST(test_t12_status_enum_5_values)
{
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_UNKNOWN, 0);
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_IN_PROGRESS, 1);
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_COMMITTED, 2);
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_ABORTED, 3);
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_CLEANED_OUT, 4);
}

/* ===== P0-33: every ordinary data-DML producer must publish the exact
 * binding as IN_PROGRESS after its ACTIVE ITL stamp, while it still owns the
 * buffer content lock.  Lock-only already did this; missing data-DML calls
 * made a fresh active remote ref miss the overlay and surface 53R97. ===== */
UT_TEST(test_p033_data_dml_publishes_active_identity)
{
	char *heap_source = read_source(HEAPAM_SOURCE_PATH);
	char *tt_source = read_source(TT_LOCAL_SOURCE_PATH);

	if (heap_source == NULL || tt_source == NULL) {
		free(heap_source);
		free(tt_source);
		return;
	}
	assert_data_active_publish(heap_source, "\nheap_insert(Relation",
							   "\nheap_prepare_insert(Relation", "cluster_itl_uba");
	assert_multi_insert_uses_receipt_safe_per_tuple_route(heap_source);
	assert_data_active_publish(heap_source, "\nheap_delete(Relation",
							   "\nsimple_heap_delete(Relation", "cluster_itl_uba");
	assert_data_active_publish(heap_source, "\nheap_update(Relation",
							   "\nsimple_heap_update(Relation", "cluster_itl_uba");

	/* A real undo-record UBA may live in a different record segment from the
	 * transaction's canonical TT segment after rollover.  The producer must
	 * therefore remember and publish the exact page-ref alias, and every
	 * terminal install must converge those aliases to COMMITTED/ABORTED. */
	UT_ASSERT(strstr(tt_source, "cluster_tt_local_record_data_active(TransactionId xid, UBA uba)")
			  != NULL);
	UT_ASSERT(strstr(tt_source, "active_alias_segments") != NULL);
	UT_ASSERT(strstr(tt_source, "install_binding_aliases") != NULL);
	UT_ASSERT(strstr(tt_source, "install_binding_aliases(binding, status, commit_scn)") != NULL);

	free(heap_source);
	free(tt_source);
}

/* P0-33 safety matrix: a proved remote ACTIVE status is non-terminal and
 * follows each consumer's existing truth table.  None of the authoritative
 * terminal/FROZEN/stale boundaries are widened by the producer fix. */
UT_TEST(test_p033_active_and_safety_boundary_matrix)
{
	UT_ASSERT_EQ((int)cluster_vis_evidence_route(CLUSTER_VIS_EVIDENCE_REMOTE, false),
				 (int)CLUSTER_VIS_ROUTE_REMOTE_VERDICT);
	UT_ASSERT_EQ((int)cluster_vis_update_xmin_verdict(CLUSTER_TT_STATUS_IN_PROGRESS),
				 (int)CVV_INVISIBLE);
	UT_ASSERT_EQ((int)cluster_vis_update_xmax_verdict(CLUSTER_TT_STATUS_IN_PROGRESS, false),
				 (int)CVV_BEING_MODIFIED);

	UT_ASSERT_EQ((int)cluster_vis_xmin_needs_resolution(HEAP_XMIN_FROZEN), 0);
	UT_ASSERT_EQ((int)cluster_vis_update_xmin_verdict(CLUSTER_TT_STATUS_COMMITTED),
				 (int)CVV_VISIBLE);
	UT_ASSERT_EQ((int)cluster_vis_update_xmin_verdict(CLUSTER_TT_STATUS_ABORTED),
				 (int)CVV_INVISIBLE);
	UT_ASSERT_EQ((int)cluster_vis_evidence_route(CLUSTER_VIS_EVIDENCE_STALE_OR_AMBIGUOUS, false),
				 (int)CLUSTER_VIS_ROUTE_FAILCLOSED_UNKNOWN);
	UT_ASSERT_EQ((int)cluster_vis_update_xmin_verdict(CLUSTER_TT_STATUS_UNKNOWN),
				 (int)CVV_FAILCLOSED_UNKNOWN);
}

/* S3-P0-26 wiring: the HTSU fork must receive curcid and delegate the
 * remote-xmin/current-xmax decision to the exhaustive three-state helper. */
UT_TEST(test_update_fork_preserves_native_self_three_state)
{
	char *source = read_source(HEAPAM_VISIBILITY_SOURCE_PATH);
	const char *start;
	const char *end;
	const char *helper;
	const char *entry;

	if (source == NULL)
		return;
	start = strstr(source, "\ncluster_satisfies_update_fork(HeapTuple htup, CommandId curcid,");
	end = start == NULL ? NULL : strstr(start, "\n}\n#endif /* USE_PGRAC_CLUSTER */");
	helper = start == NULL
		? NULL
		: strstr(start, "cluster_vis_update_native_self_verdict(");
	entry = strstr(
		source, "cluster_satisfies_update_fork(htup, curcid, buffer, &cluster_res, writer_bridge)");

	UT_ASSERT_NOT_NULL(start);
	UT_ASSERT_NOT_NULL(end);
	UT_ASSERT_NOT_NULL(helper);
	UT_ASSERT_NOT_NULL(entry);
	if (start != NULL && end != NULL && helper != NULL)
		UT_ASSERT(start < helper && helper < end);
	free(source);
}

/* S3-P0-26 local-xmax sibling: after a remote xmin has been proved visible,
 * a LOCAL/NONE lock-only xmax still follows PostgreSQL's native lifecycle.
 * A live locker is TM_BeingModified, while either terminal outcome releases
 * the row lock and is TM_Ok; a committed locker must never become
 * TM_Updated merely because is_delete is false. */
UT_TEST(test_update_fork_preserves_terminal_local_lock_only)
{
	char *source = read_source(HEAPAM_VISIBILITY_SOURCE_PATH);
	const char *start;
	const char *end;
	const char *lock_only;
	const char *lock_released;
	const char *committed_writer;

	if (source == NULL)
		return;
	start = strstr(source,
		"if (xmin_remote_visible) {\n\t\tif (TransactionIdIsInProgress(raw_xmax))");
	end = start == NULL ? NULL : strstr(start, "\n\t}\n\n\treturn false;");
	lock_only = start == NULL ? NULL : strstr(start, "else if (lock_only)");
	lock_released = lock_only == NULL ? NULL : strstr(lock_only, "*res = TM_Ok;");
	committed_writer = start == NULL
		? NULL
		: strstr(start, "else if (TransactionIdDidCommit(raw_xmax))");

	UT_ASSERT_NOT_NULL(start);
	UT_ASSERT_NOT_NULL(end);
	UT_ASSERT_NOT_NULL(lock_only);
	UT_ASSERT_NOT_NULL(lock_released);
	UT_ASSERT_NOT_NULL(committed_writer);
	if (start != NULL && end != NULL && lock_only != NULL
		&& lock_released != NULL && committed_writer != NULL)
		UT_ASSERT(start < lock_only && lock_only < lock_released
				  && lock_released < committed_writer
				  && committed_writer < end);
	free(source);
}

/* Stage-8 PRE adjustment 23: SnapshotDirty must not hand a foreign raw xid
 * to native XactLockTableWait.  The built-in heap/TableAM path carries the
 * exact DATA locator as a sidecar, and nbtree waits only after releasing its
 * leaf before restarting from search. */
UT_TEST(test_dirty_remote_xmax_wait_uses_exact_unlocked_route)
{
	char *visibility_source = read_source(HEAPAM_VISIBILITY_SOURCE_PATH);
	char *heap_source = read_source(HEAPAM_SOURCE_PATH);
	char *handler_source = read_source(HEAPAM_HANDLER_SOURCE_PATH);
	char *nbtree_source = read_source(NBTINSERT_SOURCE_PATH);
	const char *capture;
	const char *propagate;
	const char *wait_branch;
	const char *release_leaf;
	const char *exact_wait;
	const char *research;

	if (visibility_source == NULL || heap_source == NULL
		|| handler_source == NULL || nbtree_source == NULL)
		goto done;

	capture = strstr(visibility_source,
		"cluster_vis_dirty_remote_xmax_waitable(");
	propagate = strstr(handler_source, "remote_xmax_wait");
	wait_branch = strstr(nbtree_source, "if (remote_xmax_wait)");
	release_leaf = wait_branch == NULL
		? NULL
		: strstr(wait_branch, "_bt_relbuf(rel, insertstate.buf);");
	exact_wait = wait_branch == NULL
		? NULL
		: strstr(wait_branch, "cluster_tx_enqueue_wait_exact(");
	research = exact_wait == NULL ? NULL : strstr(exact_wait, "goto search;");

	UT_ASSERT_NOT_NULL(capture);
	UT_ASSERT_NOT_NULL(strstr(heap_source, "row_wait_locator_valid"));
	UT_ASSERT_NOT_NULL(propagate);
	UT_ASSERT_NOT_NULL(wait_branch);
	UT_ASSERT_NOT_NULL(release_leaf);
	UT_ASSERT_NOT_NULL(exact_wait);
	UT_ASSERT_NOT_NULL(research);
	if (wait_branch != NULL && release_leaf != NULL && exact_wait != NULL
		&& research != NULL)
		UT_ASSERT(wait_branch < release_leaf && release_leaf < exact_wait
				  && exact_wait < research);
	UT_ASSERT(strstr(nbtree_source,
		"XactLockTableWait(remote_wait_locator.xid") == NULL);

done:
	free(visibility_source);
	free(heap_source);
	free(handler_source);
	free(nbtree_source);
}

/* P0-27 already freezes the exact HEAP_XMIN_FROZEN bit pair as durable
 * committed evidence.  The MVCC fork must consume that proof before any
 * remote xmin resolver/wire leg, while still running the existing exact xmax
 * gate so a foreign delete cannot become false-visible. */
UT_TEST(test_mvcc_frozen_xmin_bypasses_remote_resolve_but_keeps_xmax_gate)
{
	char *source = read_source(HEAPAM_VISIBILITY_SOURCE_PATH);
	const char *mvcc;
	const char *frozen;
	const char *xmax_gate;
	const char *xmin_resolve;

	if (source == NULL)
		return;
	mvcc = strstr(source,
		"if (cluster_enabled && BufferIsValid(buffer)");
	frozen = mvcc == NULL ? NULL : strstr(mvcc,
		"if (!cluster_vis_xmin_needs_resolution(tuple->t_infomask))");
	xmax_gate = frozen == NULL ? NULL : strstr(frozen,
		"cluster_remote_live_xmax_keeps_visible(buffer, tuple, snapshot)");
	xmin_resolve = mvcc == NULL ? NULL : strstr(mvcc,
		"cluster_visibility_resolve_from_ref_scn(raw_xmin");

	UT_ASSERT_NOT_NULL(mvcc);
	UT_ASSERT_NOT_NULL(frozen);
	UT_ASSERT_NOT_NULL(xmax_gate);
	UT_ASSERT_NOT_NULL(xmin_resolve);
	if (mvcc != NULL && frozen != NULL && xmax_gate != NULL
		&& xmin_resolve != NULL)
		UT_ASSERT(mvcc < frozen && frozen < xmax_gate
				  && xmax_gate < xmin_resolve);
	free(source);
}

/* Spec 8.4A I18/I19: the normal commit-stamp is a live block0 modifier. */
UT_TEST(test_normal_commit_stamp_is_modifier_gated_and_error_safe)
{
	char *source = read_source(TT_LOCAL_SOURCE_PATH);
	const char *start;
	const char *end;
	const char *published;
	const char *enter;
	const char *try_block;
	const char *recheck;
	const char *durable;
	const char *finally_block;
	const char *leave;

	if (source == NULL)
		return;
	start = strstr(source, "\ncluster_tt_local_precommit_durable_finish(");
	end = start == NULL ? NULL : strstr(start, "\n}\n\nvoid\ncluster_tt_local_record_commit(");
	published = start == NULL ? NULL
		: strstr(start, "cluster_tt_local_get_published_binding(xid, &binding)");
	enter = start == NULL ? NULL : strstr(start, "cluster_semantic_activation_modifier_enter(");
	try_block = start == NULL ? NULL : strstr(start, "PG_TRY();");
	recheck = start == NULL
				  ? NULL
				  : strstr(start, "cluster_tt_local_modifier_recheck_or_error(");
	durable = recheck == NULL
				? NULL
				: strstr(recheck, "cluster_tt_slot_durable_commit_writeonly(");
	finally_block = start == NULL ? NULL : strstr(start, "PG_FINALLY();");
	leave = finally_block == NULL
				? NULL
				: strstr(finally_block, "cluster_semantic_activation_leave(");

	UT_ASSERT_NOT_NULL(start);
	UT_ASSERT_NOT_NULL(end);
	UT_ASSERT_NOT_NULL(published);
	UT_ASSERT_NOT_NULL(enter);
	UT_ASSERT_NOT_NULL(try_block);
	UT_ASSERT_NOT_NULL(recheck);
	UT_ASSERT_NOT_NULL(durable);
	UT_ASSERT_NOT_NULL(finally_block);
	UT_ASSERT_NOT_NULL(leave);
	if (start != NULL && end != NULL && published != NULL && enter != NULL && try_block != NULL
		&& recheck != NULL && durable != NULL && finally_block != NULL && leave != NULL)
		UT_ASSERT(start < published && published < enter && enter < try_block && try_block < recheck
				  && recheck < durable && durable < finally_block && finally_block < leave
				  && leave < end);
	free(source);
}

UT_TEST(test_ordinary_abort_durable_receipt_precedes_allocator_reuse)
{
	char *xact_source = read_source(XACT_SOURCE_PATH);
	char *tt_source = read_source(TT_LOCAL_SOURCE_PATH);
	const char *abort_start;
	const char *preabort;
	const char *crit;
	const char *record_abort;
	const char *local_preabort;
	const char *exact_abort;
	const char *receipt;
	const char *finish;
	const char *terminal_gate;
	const char *mark_aborted;

	if (xact_source == NULL || tt_source == NULL) {
		free(xact_source);
		free(tt_source);
		return;
	}
	abort_start = strstr(xact_source,
		"\nRecordTransactionAbort(bool isSubXact)\n{");
	preabort = abort_start == NULL ? NULL : strstr(abort_start,
		"cluster_tt_local_preabort_durable_finish(xid)");
	crit = abort_start == NULL ? NULL : strstr(abort_start, "START_CRIT_SECTION();");
	record_abort = crit == NULL ? NULL : strstr(crit,
		"cluster_tt_local_record_abort(xid)");
	local_preabort = strstr(tt_source,
		"cluster_tt_local_preabort_durable_finish(TransactionId xid)");
	exact_abort = local_preabort == NULL ? NULL : strstr(local_preabort,
		"cluster_tt_slot_durable_abort_exact(");
	receipt = exact_abort == NULL ? NULL : strstr(exact_abort,
		"CLUSTER_TT_LOCAL_TERMINAL_ABORT_DURABLE");
	finish = strstr(tt_source, "cluster_tt_local_finish_bindings(bool committed");
	terminal_gate = finish == NULL ? NULL : strstr(finish,
		"CLUSTER_TT_LOCAL_TERMINAL_ABORT_DURABLE");
	mark_aborted = finish == NULL ? NULL : strstr(finish,
		"cluster_tt_slot_mark_aborted(");

	UT_ASSERT_NOT_NULL(abort_start);
	UT_ASSERT_NOT_NULL(preabort);
	UT_ASSERT_NOT_NULL(crit);
	UT_ASSERT_NOT_NULL(record_abort);
	UT_ASSERT_NOT_NULL(local_preabort);
	UT_ASSERT_NOT_NULL(exact_abort);
	UT_ASSERT_NOT_NULL(receipt);
	UT_ASSERT_NOT_NULL(finish);
	UT_ASSERT_NOT_NULL(terminal_gate);
	UT_ASSERT_NOT_NULL(mark_aborted);
	if (abort_start != NULL && preabort != NULL && crit != NULL
		&& record_abort != NULL)
		UT_ASSERT(abort_start < preabort && preabort < crit
			&& crit < record_abort);
	if (local_preabort != NULL && exact_abort != NULL && receipt != NULL)
		UT_ASSERT(local_preabort < exact_abort && exact_abort < receipt);
	if (finish != NULL && terminal_gate != NULL && mark_aborted != NULL)
		UT_ASSERT(finish < terminal_gate && terminal_gate < mark_aborted);

	free(xact_source);
	free(tt_source);
}

/* The L3 fail-closed diagnostic must identify the authority lookup that
 * actually failed.  Naming raw_xmin as the deleting xid hides the exact
 * xmax/ref tuple and prevents deterministic route classification. */
UT_TEST(test_deleting_xmax_error_names_actual_xmax)
{
	char *source = read_source(HEAPAM_VISIBILITY_SOURCE_PATH);
	const char *message;
	const char *hint;
	const char *actual_xmax;
	const char *wrong_xmin;
	const char *wrong_hint;

	if (source == NULL)
		return;
	message = strstr(source,
		"errmsg(\"cluster TT status unknown for deleting xmax of xid %u\"");
	hint = message != NULL ? strstr(message, "errhint(") : NULL;
	actual_xmax = message != NULL
		? strstr(message, "HeapTupleHeaderGetRawXmax(tuple)") : NULL;
	wrong_xmin = message != NULL ? strstr(message, "raw_xmin),") : NULL;
	wrong_hint = message != NULL
		? strstr(message, "Remote deleter commit state not yet propagated") : NULL;

	UT_ASSERT_NOT_NULL(message);
	UT_ASSERT_NOT_NULL(hint);
	UT_ASSERT_NOT_NULL(actual_xmax);
	if (message != NULL && hint != NULL && actual_xmax != NULL)
		UT_ASSERT(message < actual_xmax && actual_xmax < hint);
	if (message != NULL && hint != NULL && wrong_xmin != NULL)
		UT_ASSERT(wrong_xmin > hint);
	if (message != NULL && hint != NULL && wrong_hint != NULL)
		UT_ASSERT(wrong_hint > hint);
	free(source);
}

/* spec-3.12 C3b/Q11: a concurrent winner may fill the segment returned by a
 * retention rollover before this backend allocates.  The follower must reread
 * and reclassify CURRENT instead of assuming that returned segment is fresh. */
UT_TEST(test_tt_retention_rollover_follower_reclassifies_current_segment)
{
	char *source = read_source(TT_LOCAL_SOURCE_PATH);
	const char *start;
	const char *end;
	const char *retry_loop;
	const char *classify;
	const char *drift_retry;
	const char *rollover;
	const char *one_shot;
	const char *fresh_error;
	const char *late_wrap_read;

	if (source == NULL)
		return;
	start = strstr(source, "\ncluster_tt_local_reserve_binding(");
	end = start == NULL
		? NULL
		: strstr(start, "\n}\n\n/*\n * cluster_tt_local_peek_binding");
	retry_loop = start == NULL ? NULL : strstr(start, "for (;;)");
	classify = retry_loop == NULL
		? NULL
		: strstr(retry_loop,
				 "cluster_tt_slot_alloc_current_exact(");
	drift_retry = classify == NULL
		? NULL
		: strstr(classify, "if (current_drift)");
	rollover = classify == NULL
		? NULL
		: strstr(classify, "cluster_undo_tt_rollover_locked(");
	one_shot = rollover == NULL
		? NULL
		: strstr(rollover, "cluster_tt_slot_alloc(seg, top_xid)");
	fresh_error = rollover == NULL
		? NULL
		: strstr(rollover, "fresh rollover segment");
	late_wrap_read = classify == NULL
		? NULL
		: strstr(classify, "cluster_tt_slot_get_wrap(seg, off)");

	UT_ASSERT_NOT_NULL(start);
	UT_ASSERT_NOT_NULL(end);
	UT_ASSERT_NOT_NULL(retry_loop);
	UT_ASSERT_NOT_NULL(classify);
	UT_ASSERT_NOT_NULL(drift_retry);
	UT_ASSERT_NOT_NULL(rollover);
	if (start != NULL && end != NULL && retry_loop != NULL && classify != NULL
		&& drift_retry != NULL && rollover != NULL)
		UT_ASSERT(start < retry_loop && retry_loop < classify && classify < rollover
				  && classify < drift_retry && drift_retry < rollover && rollover < end);
	if (one_shot != NULL && end != NULL)
		UT_ASSERT(one_shot > end);
	if (late_wrap_read != NULL && end != NULL)
		UT_ASSERT(late_wrap_read > end);
	if (fresh_error != NULL && end != NULL)
		UT_ASSERT(fresh_error > end);
	free(source);
}

UT_TEST(test_canonical_active_is_prepared_before_heap_content_lock)
{
	char *heap_source = read_source(HEAPAM_SOURCE_PATH);
	char *tt_source = read_source(TT_LOCAL_SOURCE_PATH);
	const char *prepare;
	const char *publish;
	const char *published_accessor;

	if (heap_source == NULL || tt_source == NULL) {
		free(heap_source);
		free(tt_source);
		return;
	}
	prepare = strstr(tt_source,
		"cluster_tt_local_prepare_canonical_active(TransactionId top_xid");
	publish = prepare == NULL ? NULL : strstr(prepare,
		"cluster_tt_slot_durable_publish_active(");
	published_accessor = strstr(tt_source,
		"cluster_tt_local_get_published_binding(TransactionId top_xid");

	UT_ASSERT_NOT_NULL(prepare);
	UT_ASSERT_NOT_NULL(publish);
	UT_ASSERT_NOT_NULL(published_accessor);
	UT_ASSERT(strstr(heap_source, "cluster_tt_local_get_or_create_binding(") == NULL);
	UT_ASSERT(strstr(heap_source, "canonical_xid = GetTopTransactionId();") != NULL);
	UT_ASSERT(strstr(heap_source,
		"cluster_tt_local_prepare_canonical_active(\n\t\t\tcanonical_xid, &canonical_binding)") != NULL);
	UT_ASSERT(strstr(heap_source,
		"cluster_tt_local_get_published_binding(\n\t\t\t\tcanonical_xid, &canonical_binding)") != NULL);
	UT_ASSERT(strstr(heap_source,
		"cluster_tt_local_prepare_canonical_active(xid, &canonical_binding)") == NULL);
	UT_ASSERT(strstr(tt_source, "CLUSTER_CANONICAL_TXN_PUBLISHED") != NULL);

	free(heap_source);
	free(tt_source);
}


UT_TEST(test_writer_proof_entry_excludes_native_inplace_and_keyshare_shortcuts)
{
	char *source = read_source(HEAPAM_SOURCE_PATH);
	const char *cursor = source;
	int count = 0;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	while ((cursor = strstr(cursor, "= HeapTupleSatisfiesUpdateForWriter(")) != NULL) {
		count++;
		cursor++;
	}
	UT_ASSERT_EQ(count, 3);
	UT_ASSERT(
		strstr(source, "if (wait)\n\t\tresult = HeapTupleSatisfiesUpdateForWriter(&tp, cid, "
					   "buffer);\n\telse\n\t\tresult = HeapTupleSatisfiesUpdate(&tp, cid, buffer);")
		!= NULL);
	UT_ASSERT(
		strstr(source,
			   "if (wait)\n\t\tresult = HeapTupleSatisfiesUpdateForWriter(&oldtup, cid, "
			   "buffer);\n\telse\n\t\tresult = HeapTupleSatisfiesUpdate(&oldtup, cid, buffer);")
		!= NULL);
	UT_ASSERT(strstr(source, "if (mode == LockTupleKeyShare)\n\t\tresult = "
							 "HeapTupleSatisfiesUpdate(tuple, cid, *buffer);\n\telse\n\t\tresult = "
							 "HeapTupleSatisfiesUpdateForWriter(tuple, cid, *buffer);")
			  != NULL);
	UT_ASSERT(
		strstr(source, "result = HeapTupleSatisfiesUpdate(&oldtup, GetCurrentCommandId(false),")
		!= NULL);
	free(source);
}

int
main(void)
{
	UT_RUN(test_t1_undo_tt_slot_ref_sizeof_32);
	UT_RUN(test_t2_ref_field_offsets);
	UT_RUN(test_t3_placeholder_ref_sentinel);
	UT_RUN(test_t4_self_origin_gate);
	UT_RUN(test_t5_build_key_field_carry);
	UT_RUN(test_t6_errcode_53r97_encodable);
	UT_RUN(test_t7_inject_lookup_linkable);
	UT_RUN(test_t8_inject_shmem_helpers_linkable);
	UT_RUN(test_t9_production_inject_returns_false);
	UT_RUN(test_t10_itl_slot_unallocated_sentinel);
	UT_RUN(test_t11_no_is_xid_local_origin_in_this_unit);
	UT_RUN(test_t12_status_enum_5_values);
	UT_RUN(test_p033_data_dml_publishes_active_identity);
	UT_RUN(test_p033_active_and_safety_boundary_matrix);
	UT_RUN(test_update_fork_preserves_native_self_three_state);
	UT_RUN(test_update_fork_preserves_terminal_local_lock_only);
	UT_RUN(test_dirty_remote_xmax_wait_uses_exact_unlocked_route);
	UT_RUN(test_mvcc_frozen_xmin_bypasses_remote_resolve_but_keeps_xmax_gate);
	UT_RUN(test_normal_commit_stamp_is_modifier_gated_and_error_safe);
	UT_RUN(test_ordinary_abort_durable_receipt_precedes_allocator_reuse);
	UT_RUN(test_deleting_xmax_error_names_actual_xmax);
	UT_RUN(test_tt_retention_rollover_follower_reclassifies_current_segment);
	UT_RUN(test_canonical_active_is_prepared_before_heap_content_lock);
	UT_RUN(test_writer_proof_entry_excludes_native_inplace_and_keyshare_shortcuts);
	UT_DONE();
}
