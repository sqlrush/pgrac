/* Native unified boundary; external undo reservation/storage and receipt tables
 * are controlled. No full recursive/SQL/WAL durability claim. */
#define HISTORY_TWO_HOP_FIXTURE
#include "test_cluster_mx_history_writer_publication.c"
#undef main
#undef HISTORY_TWO_HOP_FIXTURE
#include "cluster/cluster_undo_record_api.h"
#include "access/xlog.h"
#include "../../common/sha2_int.h"
int wal_level = WAL_LEVEL_REPLICA;
static unsigned undo_applies, undo_consumes, undo_preflights, sealed_at_apply;
static bool undo_retry, preflight_retry, handle_drift;
static FormData_pg_class boundary_class;
static UBA reserved_uba(void) { return uba_encode(257, 12, 3, 0); }
bool cluster_ctrc_sha256_exact(const void *bytes, Size size, uint8 digest[32])
{
	pg_sha256_ctx ctx;
	pg_sha256_init(&ctx); pg_sha256_update(&ctx, bytes, size); pg_sha256_final(&ctx, digest);
	return true;
}
bool cluster_undo_record_prepared_recheck(const ClusterUndoRecordPrepareReceipt *r, uint16 n)
{ return n == sizeof(UndoItlPayload) && r->payload_capacity == n; }
bool cluster_undo_record_prepared_uba_exact(const ClusterUndoRecordPrepareReceipt *r, uint16 n, UBA *u)
{ if (!cluster_undo_record_prepared_recheck(r, n)) return false; *u = reserved_uba(); return true; }
bool cluster_undo_record_history_matches(const ClusterUndoRecordPrepareReceipt *r, uint8 i,
	const UndoItlHistoryEntry *e)
{ return i < 2 && (r->itl_history_mask & (1u << i)) && !memcmp(e, &r->itl_history[i], sizeof(*e)); }
bool cluster_undo_record_ctrc_pending_matches(const ClusterUndoRecordPrepareReceipt *r, uint8 i,
	const ClusterCtrcTargetV1 *t)
{ return i < 2 && !memcmp(t, &r->ctrc_pending_targets[i], sizeof(*t)); }
bool cluster_undo_record_ctrc_required_prepared(const ClusterUndoRecordPrepareReceipt *r, uint8 mask)
{ return r->ctrc_pending_mask == mask && r->ctrc_prepared_mask == mask; }
ClusterUndoRecordConsumePreflightResult
cluster_undo_record_consume_preflight(ClusterUndoRecordPrepareReceipt *r, uint16 n)
{
	(void)r; (void)n; undo_preflights++;
	return preflight_retry ? CLUSTER_UNDO_RECORD_CONSUME_PREFLIGHT_RETRY_REQUIRED
		: CLUSTER_UNDO_RECORD_CONSUME_PREFLIGHT_READY;
}
ClusterCtrcApplyResult cluster_undo_record_ctrc_apply_prepared(ClusterUndoRecordPrepareReceipt *r,
	uint8 i, const ClusterCtrcTargetV1 *t, ClusterCtrcApplyToken *token)
{
	UT_ASSERT(content_locked && exclusive); UT_ASSERT_EQ(t->kind, CTRC_TARGET_EXACT_ITL_SLOT);
	UT_ASSERT_EQ(i, 1); UT_ASSERT_EQ(r->ctrc_handles[i].receipt_index, 401);
	UT_ASSERT_EQ(r->ctrc_handles[i].key.xid, 129);
	undo_applies++; sealed_at_apply = mx_history_boundary_transition_state();
	memset(token, 0, sizeof(*token));
	if (undo_retry) return CLUSTER_CTRC_APPLY_RETRY_REQUIRED;
	r->ctrc_applied_mask |= 1u << i; token->valid = true;
	if (handle_drift) r->ctrc_handles[i].receipt_index++;
	return CLUSTER_CTRC_APPLY_APPLIED;
}
bool cluster_undo_record_release_zero_effect_preflight(ClusterUndoRecordPrepareReceipt *r)
{ return r->ctrc_applied_mask == 0; }
ClusterUndoRecordConsumeResult cluster_undo_record_consume_prepared(ClusterUndoRecordPrepareReceipt *r,
	const ClusterUndoRecordTarget *target, const void *payload, uint16 n, UBA *out)
{
	(void)payload; UT_ASSERT_EQ(n, sizeof(UndoItlPayload)); UT_ASSERT_EQ(target->blockno, 3);
	UT_ASSERT_EQ(r->ctrc_applied_mask, 2); undo_consumes++; *out = reserved_uba();
	memset(r, 0, sizeof(*r)); /* Original receipt consumption clears its handle. */
	return CLUSTER_UNDO_RECORD_CONSUME_APPLIED;
}
static Snapshot boundary_setup(MxHeapReadout *out, ClusterHeapSuccessorProof *next)
{
	Snapshot s = publication_setup(out);
	ClusterCtrcReceiptHandle undo = { .valid = true, .receipt_index = 401,
		.participant_index = 17, .journal_slot_generation = 19,
		.original_full_xid = FullTransactionIdFromEpochAndXid(1, 129) };
	ClusterCtrcParticipantIdentity unused_participant;
	publication_identity(129, 66, &undo.key, &unused_participant);
	memset(&boundary_class, 0, sizeof(boundary_class));
	boundary_class.relpersistence = RELPERSISTENCE_PERMANENT; relation.rd_rel = &boundary_class;
	undo_applies = undo_consumes = undo_preflights = sealed_at_apply = 0;
	undo_retry = preflight_retry = handle_drift = false;
	UT_ASSERT(mx_history_boundary_prepare(&relation, 1, &tuple, 1, &undo));
	UT_ASSERT(mx_history_recursive_next(&tuple, next));
	return s;
}
static void boundary_write(void)
{
	mx_history_boundary_write(&tuple);
	cluster_itl_stamp_lock_active_with_history(1, 7, 129, 91, reserved_uba());
	PageSetLSN(BufferBlocks, 129);
}
/* A missing/misordered producer or MX-handle substitution must fail this case. */
UT_TEST(native_boundary_seals_independent_undo_before_first_APPLY)
{
	MxHeapReadout out; ClusterHeapSuccessorProof next; bool planned;
	Snapshot s = boundary_setup(&out, &next); HeapTupleData candidate = hot_candidate();
	UT_ASSERT_EQ(mx_history_boundary_apply(&relation, 1, &tuple, &next, &planned), 0);
	UT_ASSERT(planned); UT_ASSERT_EQ(sealed_at_apply, 1);
	UT_ASSERT_EQ(undo_preflights, 1); UT_ASSERT_EQ(undo_applies, 1); UT_ASSERT_EQ(undo_consumes, 1);
	UT_ASSERT_EQ(publication_applies, 2); boundary_write();
	UT_ASSERT(mx_history_boundary_complete(1, true, 129));
	UT_ASSERT_EQ(writer_match(&candidate, &next, true), 1);
	UT_ASSERT_EQ(writer_match(&candidate, &next, true), 0);
	UT_ASSERT_EQ(publication_cancels, 0); writer_clean(s, true);
}
UT_TEST(preflight_retry_has_no_transition_or_APPLY)
{
	MxHeapReadout out; ClusterHeapSuccessorProof next; bool planned;
	Snapshot s = boundary_setup(&out, &next); preflight_retry = true;
	UT_ASSERT_EQ(mx_history_boundary_apply(&relation, 1, &tuple, &next, &planned), 1);
	UT_ASSERT(!planned); UT_ASSERT_EQ(mx_history_boundary_transition_state(), 0);
	UT_ASSERT_EQ(undo_applies, 0); UT_ASSERT_EQ(publication_applies, 0);
	mx_publication_cancel(&(MxPublicationReadout){0}); writer_clean(s, true);
}
UT_TEST(zero_effect_APPLY_retry_drops_only_private_witness)
{
	MxHeapReadout out; ClusterHeapSuccessorProof next; bool planned;
	Snapshot s = boundary_setup(&out, &next); HeapTupleData candidate = hot_candidate();
	undo_retry = true;
	UT_ASSERT_EQ(mx_history_boundary_apply(&relation, 1, &tuple, &next, &planned), 1);
	UT_ASSERT(!planned); UT_ASSERT_EQ(sealed_at_apply, 1);
	UT_ASSERT_EQ(mx_history_boundary_transition_state(), 0);
	UT_ASSERT_EQ(undo_consumes, 0); UT_ASSERT_EQ(publication_applies, 0);
	/* The controlled reservation remains requalifiable in this same scope. */
	undo_retry = false;
	UT_ASSERT_EQ(mx_history_boundary_apply(&relation, 1, &tuple, &next, &planned), 0);
	UT_ASSERT(planned); boundary_write(); UT_ASSERT(mx_history_boundary_complete(1, true, 129));
	UT_ASSERT_EQ(writer_match(&candidate, &next, true), 1);
	writer_clean(s, true);
}
UT_TEST(partial_APPLY_and_exact_handle_drift_cannot_complete)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		MxHeapReadout out; ClusterHeapSuccessorProof next; bool planned;
		Snapshot s = boundary_setup(&out, &next); HeapTupleData candidate = hot_candidate();
		if (fault == 0) publication_retry_at = 1;
		else handle_drift = true;
		UT_ASSERT_EQ(mx_history_boundary_apply(&relation, 1, &tuple, &next, &planned), fault == 0 ? 2 : 0);
		UT_ASSERT(planned); UT_ASSERT_EQ(sealed_at_apply, 1); UT_ASSERT_EQ(undo_applies, 1);
		UT_ASSERT_EQ(undo_consumes, fault == 0 ? 0 : 1);
		boundary_write(); UT_ASSERT(!mx_history_boundary_complete(1, fault != 0, 129));
		UT_ASSERT_EQ(writer_match(&candidate, &next, true), 0);
		UT_ASSERT_EQ(publication_cancels, 0); writer_clean(s, true);
	}
}
UT_TEST(untyped_and_cross_page_edges_keep_original_boundary)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		MxHeapReadout out; ClusterHeapSuccessorProof next; bool planned;
		Snapshot s = boundary_setup(&out, &next);
		if (fault == 0) next.writer_valid = false;
		if (fault == 1) next.history_valid = true;
		if (fault == 2) ItemPointerSet(&next.tid, 4, 2);
		UT_ASSERT_EQ(mx_history_boundary_apply(&relation, 1, &tuple, &next, &planned), 0);
		UT_ASSERT(!planned); UT_ASSERT_EQ(sealed_at_apply, 0);
		UT_ASSERT_EQ(mx_history_boundary_transition_state(), 0); UT_ASSERT_EQ(undo_consumes, 1);
		writer_clean(s, true);
	}
}
int main(void)
{
	history_fixture_init(); UT_PLAN(5);
	UT_RUN(native_boundary_seals_independent_undo_before_first_APPLY);
	UT_RUN(preflight_retry_has_no_transition_or_APPLY);
	UT_RUN(zero_effect_APPLY_retry_drops_only_private_witness);
	UT_RUN(partial_APPLY_and_exact_handle_drift_cannot_complete);
	UT_RUN(untyped_and_cross_page_edges_keep_original_boundary);
	free(procArray); UT_DONE(); return ut_failed_count ? 1 : 0;
}
