/* Actual native recursive heap/receipt/ITL/WAL registration bodies. Undo
 * storage, grant tables and XLogInsert return are controlled external leaves;
 * this is not SQL, durable WAL or native transport evidence. */
#define HISTORY_NATIVE_APPLY_FIXTURE
#include "test_cluster_mx_history_native_recursive.c"
#undef main
#undef HISTORY_NATIVE_APPLY_FIXTURE
#include "access/heapam_xlog.h"
#include "../../common/sha2_int.h"

typedef struct NativeUndoWrite {
	UBA uba;
	ClusterUndoRecordTarget target;
	UndoItlPayload payload;
	UndoItlHistoryEntry history;
	ClusterCtrcReceiptHandle handle;
} NativeUndoWrite;
static NativeUndoWrite native_writes[2];
static unsigned apply_plans, apply_prepares, apply_undos, apply_consumes;
static unsigned apply_dirty, apply_wal, apply_touches, apply_notes, apply_cancels;
static unsigned apply_wal_parts, apply_fault, apply_zero_retry;
static xl_heap_lock_updated apply_wal_lock;
static xl_heap_itl_delta_block apply_wal_header;
static xl_heap_itl_delta_v3 apply_wal_delta;
static SMgrRelationData apply_smgr;

static UBA apply_reserved(void) { return uba_encode(257, 12 + native_reserves, 3, 0); }
SCN cluster_scn_advance(void) { UT_ASSERT(content_locked && exclusive); return 91 + ++apply_plans; }
bool cluster_ctrc_sha256_exact(const void *bytes, Size size, uint8 digest[32])
{
	pg_sha256_ctx ctx;
	pg_sha256_init(&ctx); pg_sha256_update(&ctx, bytes, size); pg_sha256_final(&ctx, digest);
	return true;
}
ClusterUndoRecordPrepareResult
cluster_undo_record_prepare(uint8 type, uint16 capacity, uint16 segment, uint16 slot,
	UBA previous, uint64 deadline, ClusterUndoRecordPrepareReceipt *r)
{
	UT_ASSERT(!content_locked && type == UNDO_RECORD_ITL);
	UT_ASSERT_EQ(capacity, sizeof(UndoItlPayload)); UT_ASSERT_EQ(segment, 257);
	UT_ASSERT_EQ(slot, 3); UT_ASSERT_EQ(deadline, 5000000); UT_ASSERT(UBA_is_invalid(previous));
	memset(r, 0, sizeof(*r)); r->record_type = type; r->payload_capacity = capacity;
	r->tt_slot_segment_id = segment; r->tt_slot_offset = slot; native_reserves++;
	return CLUSTER_UNDO_RECORD_PREPARE_READY;
}
bool cluster_undo_record_prepared_recheck(const ClusterUndoRecordPrepareReceipt *r, uint16 n)
{
	UT_ASSERT(content_locked && exclusive); native_prepared_checks++;
	return r->record_type == UNDO_RECORD_ITL && n == sizeof(UndoItlPayload)
		&& r->payload_capacity == n && r->tt_slot_segment_id == 257 && r->tt_slot_offset == 3;
}
bool cluster_undo_record_prepared_uba_exact(const ClusterUndoRecordPrepareReceipt *r, uint16 n, UBA *u)
{ if (!cluster_undo_record_prepared_recheck(r, n)) return false; *u = apply_reserved(); return true; }
bool cluster_undo_record_stage_history(ClusterUndoRecordPrepareReceipt *r, uint8 i,
	const UndoItlHistoryEntry *e)
{
	UT_ASSERT_EQ(i, 0); UT_ASSERT_EQ(e->slot_index, 7);
	r->itl_history[i] = *e; r->itl_history_mask |= 1u << i; return true;
}
bool cluster_undo_record_history_matches(const ClusterUndoRecordPrepareReceipt *r, uint8 i,
	const UndoItlHistoryEntry *e)
{ return i == 0 && (r->itl_history_mask & 1) && !memcmp(&r->itl_history[i], e, sizeof(*e)); }
bool cluster_undo_record_ctrc_stage_pending(ClusterUndoRecordPrepareReceipt *r, uint8 i,
	const ClusterCtrcTargetV1 *t)
{
	UT_ASSERT(content_locked && exclusive && i == 0 && r->ctrc_applied_mask == 0);
	UT_ASSERT_EQ(t->kind, CTRC_TARGET_PAGE_PENDING_ITL_SLOT);
	r->ctrc_pending_targets[i] = *t; r->ctrc_pending_mask |= 1u << i; return true;
}
bool cluster_undo_record_ctrc_prepare_pending(ClusterUndoRecordPrepareReceipt *r, uint8 i)
{
	ClusterCtrcParticipantIdentity participant;
	ClusterCtrcReceiptHandle *h = &r->ctrc_handles[i];
	UT_ASSERT(!content_locked && i == 0 && r->ctrc_pending_mask == 1 && r->ctrc_applied_mask == 0);
	memset(h, 0, sizeof(*h)); publication_identity(129, 66, &h->key, &participant);
	h->valid = true; h->receipt_index = 400 + ++apply_prepares;
	h->participant_index = 17; h->journal_slot_generation = 19;
	h->original_full_xid = FullTransactionIdFromEpochAndXid(1, 129);
	r->ctrc_prepared_mask = 1; return true;
}
bool cluster_undo_record_ctrc_pending_matches(const ClusterUndoRecordPrepareReceipt *r, uint8 i,
	const ClusterCtrcTargetV1 *t)
{ return i == 0 && r->ctrc_pending_mask == 1 && !memcmp(&r->ctrc_pending_targets[i], t, sizeof(*t)); }
bool cluster_undo_record_ctrc_pending_recheck(const ClusterUndoRecordPrepareReceipt *r, uint8 i,
	const ClusterCtrcTargetV1 *t) { return cluster_undo_record_ctrc_pending_matches(r, i, t); }
bool cluster_undo_record_ctrc_required_prepared(const ClusterUndoRecordPrepareReceipt *r, uint8 mask)
{ return mask == 1 && r->ctrc_pending_mask == mask && r->ctrc_prepared_mask == mask; }
bool cluster_itl_touch_lookup_reusable_ctrc(const ClusterItlTouchHandle *t, Buffer b,
	TransactionId xid, ClusterCtrcReceiptHandle *h)
{ UT_ASSERT(content_locked && b == 1 && xid == 129 && t->slot_idx == 7); return false; }
bool cluster_ctrc_receipt_itl_reuse_candidate_shared(const ClusterCtrcReceiptHandle *h,
	const ClusterCtrcTargetV1 *t, const ClusterCtrcItlTargetIdentity *i, const uint8 *sha)
{ recursive_unexpected("unexpected positive reused ITL receipt"); }
ClusterUndoRecordPrepareResult
cluster_undo_record_requalify_for_retry(ClusterUndoRecordPrepareReceipt *r, uint16 n, bool changed)
{
	UT_ASSERT(!content_locked && r->ctrc_applied_mask == 0 && !changed);
	UT_ASSERT_EQ(n, sizeof(UndoItlPayload)); native_requalifications++;
	return CLUSTER_UNDO_RECORD_PREPARE_READY;
}
ClusterUndoRecordConsumePreflightResult
cluster_undo_record_consume_preflight(ClusterUndoRecordPrepareReceipt *r, uint16 n)
{ UT_ASSERT(content_locked && r->ctrc_applied_mask == 0 && n == sizeof(UndoItlPayload)); return CLUSTER_UNDO_RECORD_CONSUME_PREFLIGHT_READY; }
ClusterCtrcApplyResult cluster_undo_record_ctrc_apply_prepared(ClusterUndoRecordPrepareReceipt *r,
	uint8 i, const ClusterCtrcTargetV1 *t, ClusterCtrcApplyToken *token)
{
	UT_ASSERT(content_locked && exclusive && i == 0 && r->ctrc_applied_mask == 0);
	UT_ASSERT_EQ(t->kind, CTRC_TARGET_EXACT_ITL_SLOT); UT_ASSERT_EQ(t->itl_slot_index, 7);
	UT_ASSERT_EQ(t->itl_xid, 129); UT_ASSERT_EQ(r->ctrc_handles[0].key.xid, 129);
	apply_undos++; memset(token, 0, sizeof(*token));
	if (apply_zero_retry && apply_undos == 1) return CLUSTER_CTRC_APPLY_RETRY_REQUIRED;
	r->ctrc_applied_mask = 1; token->valid = true; return CLUSTER_CTRC_APPLY_APPLIED;
}
bool cluster_undo_record_release_zero_effect_preflight(ClusterUndoRecordPrepareReceipt *r)
{ return r->ctrc_applied_mask == 0; }
void cluster_undo_record_cancel_prepared(ClusterUndoRecordPrepareReceipt *r)
{ UT_ASSERT(!content_locked && r->ctrc_applied_mask == 0); apply_cancels++; memset(r, 0, sizeof(*r)); }
ClusterUndoRecordConsumeResult cluster_undo_record_consume_prepared(ClusterUndoRecordPrepareReceipt *r,
	const ClusterUndoRecordTarget *target, const void *payload, uint16 n, UBA *out)
{
	NativeUndoWrite *w;
	UT_ASSERT(content_locked && r->ctrc_applied_mask == 1 && apply_consumes < 2);
	UT_ASSERT_EQ(n, sizeof(UndoItlPayload)); UT_ASSERT_EQ(target->blockno, 3);
	w = &native_writes[apply_consumes++]; w->uba = *out = apply_reserved();
	w->target = *target; memcpy(&w->payload, payload, n); w->history = r->itl_history[0];
	w->handle = r->ctrc_handles[0]; memset(r, 0, sizeof(*r));
	return CLUSTER_UNDO_RECORD_CONSUME_APPLIED;
}
void cluster_ctrc_note_publication_after_apply(const ClusterCtrcReceiptHandle *h, bool published)
{ UT_ASSERT(h->valid && published && !CritSectionCount); apply_notes++; }
void MarkBufferDirty(Buffer b)
{ UT_ASSERT_EQ(b, 1); UT_ASSERT(content_locked && exclusive && CritSectionCount > 0); apply_dirty++; }
void XLogBeginInsert(void)
{ UT_ASSERT(CritSectionCount > 0 && apply_consumes == apply_wal + 1); apply_wal_parts = 0; }
void XLogRegisterBuffer(uint8 id, Buffer b, uint8 flags)
{ UT_ASSERT(id == 0 && b == 1 && flags == REGBUF_STANDARD); }
void XLogRegisterData(char *data, uint32 n)
{
	if (apply_wal_parts == 0) { UT_ASSERT_EQ(n, SizeOfHeapLockUpdated); memcpy(&apply_wal_lock, data, n); }
	else if (apply_wal_parts == 1) { UT_ASSERT_EQ(n, offsetof(xl_heap_itl_delta_block, deltas)); memcpy(&apply_wal_header, data, n); }
	else { UT_ASSERT_EQ(apply_wal_parts, 2); UT_ASSERT_EQ(n, sizeof(apply_wal_delta)); memcpy(&apply_wal_delta, data, n); }
	apply_wal_parts++;
}
XLogRecPtr XLogInsert(RmgrId rm, uint8 info)
{
	NativeUndoWrite *w = &native_writes[apply_wal];
	const ClusterItlSlotData *slot = &ClusterPageGetItlSlots(BufferBlocks)[7];
	UT_ASSERT(CritSectionCount > 0 && apply_wal < 2 && apply_wal_parts == 3);
	UT_ASSERT_EQ(rm, RM_HEAP2_ID); UT_ASSERT_EQ(info, XLOG_HEAP2_LOCK_UPDATED);
	UT_ASSERT_EQ(apply_wal_lock.offnum, w->target.offnum);
	UT_ASSERT_EQ(apply_wal_lock.xmax, HeapTupleHeaderGetRawXmax(two_tuple(w->target.offnum).t_data));
	UT_ASSERT(apply_wal_lock.flags & XLH_LOCK_UPDATED_ITL_DELTA);
	UT_ASSERT_EQ(apply_wal_header.format_version, CLUSTER_ITL_DELTA_FORMAT_V4);
	UT_ASSERT_EQ(apply_wal_header.ndeltas, 1); UT_ASSERT_EQ(apply_wal_delta.xid, 129);
	UT_ASSERT_EQ(apply_wal_delta.slot_idx, 7); UT_ASSERT_EQ(apply_wal_delta.write_scn, w->history.after_write_scn);
	UT_ASSERT(cluster_undo_record_uba_equal(apply_wal_delta.undo_segment_head, w->uba));
	UT_ASSERT_EQ(slot->xid, 129); UT_ASSERT_EQ(slot->flags, ITL_FLAG_LOCK_ONLY_ACTIVE);
	UT_ASSERT_EQ(slot->write_scn, apply_wal_delta.write_scn);
	UT_ASSERT(cluster_undo_record_uba_equal(slot->undo_segment_head, w->uba));
	UT_ASSERT_EQ(w->payload.itl_slot_idx, 7);
	UT_ASSERT_EQ(w->payload.new_flags, ITL_FLAG_LOCK_ONLY_ACTIVE);
	return 129 + ++apply_wal;
}
void cluster_tt_local_record_active(TransactionId xid)
{ UT_ASSERT_EQ(xid, 129); UT_ASSERT_EQ(CritSectionCount, 0); UT_ASSERT_EQ(apply_wal, apply_consumes); }
void cluster_itl_touch_register_exact_ctrc(const ClusterItlTouchHandle *t, Buffer b,
	TransactionId xid, const ClusterCtrcReceiptHandle *h, const ClusterSpaceIdentity *identity)
{
	NativeUndoWrite *w = &native_writes[apply_touches++];
	UT_ASSERT(content_locked && exclusive && b == 1 && xid == 129 && identity == NULL);
	UT_ASSERT(t->slot_idx == 7 && t->flags == CLUSTER_ITL_TOUCH_FLAG_NEEDS_WAL);
	UT_ASSERT(!memcmp(h, &w->handle, sizeof(*h)));
	UT_ASSERT_EQ(PageGetLSN(BufferBlocks), 129 + apply_wal);
	if (apply_fault == 1) BufferBlocks[((PageHeader)BufferBlocks)->pd_lower] ^= 1;
	if (apply_fault == 2) writer_grant_ok = false;
}

static Snapshot apply_setup(ClusterHeapSuccessorProof *next)
{
	Snapshot s = two_setup();
	MxHeapReadout out;
	HeapTupleHeader third = two_tuple(3).t_data;
	third->t_infomask = HEAP_XMAX_INVALID; /* Actual end-of-chain tuple, before capture. */
	memset(&native_class, 0, sizeof(native_class)); native_class.relpersistence = RELPERSISTENCE_PERMANENT;
	memset(&apply_smgr, 0, sizeof(apply_smgr)); apply_smgr.smgr_rlocator.locator = relation.rd_locator;
	relation.rd_rel = &native_class; relation.rd_smgr = &apply_smgr; native_snapshot = s;
	native_reads = native_releases = native_visibility = native_reserves = native_prepared_checks = native_pins = 0;
	native_retry_limit = native_requalifications = native_fault = 0;
	apply_plans = apply_prepares = apply_undos = apply_consumes = apply_dirty = apply_wal = 0;
	apply_touches = apply_notes = apply_cancels = apply_fault = apply_zero_retry = 0;
	publication_A_grant_revoked = false; memset(native_writes, 0, sizeof(native_writes));
	UT_ASSERT_EQ(writer_scoped_call(&out), 2); UT_ASSERT(two_publish(&out, &tuple, 0, next));
	bridge_lock_override = native_lock; LockBuffer(1, BUFFER_LOCK_UNLOCK); return s;
}
UT_TEST(native_recursion_applies_two_versions_and_registers_exact_WAL)
{
	ClusterHeapSuccessorProof next;
	Snapshot s = apply_setup(&next);
	int result = native_call(&next);
	printf("# native APPLY result=%d error=%d undo=%u consumes=%u WAL=%u touches=%u notes=%u requests=%u\n",
		result, error_code, apply_undos, apply_consumes, apply_wal, apply_touches, apply_notes, finish_requests);
	UT_ASSERT_EQ(result, TM_Ok); UT_ASSERT_EQ(error_code, 0);
	UT_ASSERT_EQ(apply_undos, 2); UT_ASSERT_EQ(apply_consumes, 2); UT_ASSERT_EQ(apply_wal, 2);
	UT_ASSERT_EQ(apply_touches, 2); UT_ASSERT_EQ(apply_notes, 2); UT_ASSERT_EQ(apply_dirty, 2);
	UT_ASSERT_EQ(native_reserves, 2); UT_ASSERT_EQ(native_pins, 0);
	UT_ASSERT_EQ(finish_requests, 13); UT_ASSERT_EQ(finish_reads, 13);
	for (unsigned i = 0; i < 6; i++) UT_ASSERT_EQ(two_records[i].reads, 2);
	UT_ASSERT_EQ(two_records[6].reads, 1);
	UT_ASSERT_EQ(native_writes[0].target.offnum, 2); UT_ASSERT_EQ(native_writes[1].target.offnum, 3);
	UT_ASSERT_EQ(native_writes[0].payload.lock_xid, 145); UT_ASSERT_EQ(native_writes[1].payload.lock_xid, 145);
	UT_ASSERT(cluster_undo_record_uba_equal(native_writes[1].history.prior.undo_segment_head, native_writes[0].uba));
	UT_ASSERT_EQ(PageGetLSN(BufferBlocks), 131); UT_ASSERT_EQ(publication_applies, 4);
	UT_ASSERT_EQ(apply_cancels, 0); UT_ASSERT_EQ(publication_cancels, 0); native_clean(s);
}
UT_TEST(native_zero_APPLY_retry_requalifies_without_replaying_effects)
{
	ClusterHeapSuccessorProof next;
	Snapshot s = apply_setup(&next); apply_zero_retry = 1;
	UT_ASSERT_EQ(native_call(&next), TM_Ok); UT_ASSERT_EQ(error_code, 0);
	UT_ASSERT_EQ(apply_undos, 3); UT_ASSERT_EQ(apply_consumes, 2); UT_ASSERT_EQ(apply_wal, 2);
	UT_ASSERT_EQ(native_requalifications, 1); UT_ASSERT_EQ(finish_requests, 20);
	UT_ASSERT_EQ(publication_applies, 4); UT_ASSERT_EQ(publication_cancels, 2);
	UT_ASSERT_EQ(apply_cancels, 0); native_clean(s);
}
UT_TEST(native_partial_APPLY_cannot_cancel_retry_or_publish)
{
	ClusterHeapSuccessorProof next;
	Snapshot s = apply_setup(&next); publication_retry_at = 3;
	UT_ASSERT_EQ(native_call(&next), -1); UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
	UT_ASSERT_EQ(apply_undos, 1); UT_ASSERT_EQ(apply_consumes, 0); UT_ASSERT_EQ(apply_wal, 0);
	UT_ASSERT_EQ(apply_dirty, 0); UT_ASSERT_EQ(apply_touches, 0);
	UT_ASSERT_EQ(native_requalifications, 0); UT_ASSERT_EQ(publication_cancels, 0);
	UT_ASSERT_EQ(apply_cancels, 0); UT_ASSERT_EQ(finish_requests, 13); native_clean(s);
}
UT_TEST(native_written_delta_or_B_grant_drift_stops_before_next_effect)
{
	for (unsigned fault = 1; fault <= 2; fault++) {
		ClusterHeapSuccessorProof next;
		Snapshot s = apply_setup(&next); apply_fault = fault;
		UT_ASSERT_EQ(native_call(&next), -1); UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
		UT_ASSERT_EQ(apply_consumes, 1); UT_ASSERT_EQ(apply_wal, 1); UT_ASSERT_EQ(apply_touches, 1);
		UT_ASSERT_EQ(native_reserves, 1); UT_ASSERT_EQ(native_requalifications, 0);
		UT_ASSERT_EQ(apply_cancels, 0); UT_ASSERT_EQ(publication_cancels, 0);
		UT_ASSERT_EQ(finish_requests, 13); native_clean(s);
	}
}
int main(void)
{
	setbuf(stdout, NULL); history_fixture_init(); UT_PLAN(4);
	UT_RUN(native_recursion_applies_two_versions_and_registers_exact_WAL);
	UT_RUN(native_zero_APPLY_retry_requalifies_without_replaying_effects);
	UT_RUN(native_partial_APPLY_cannot_cancel_retry_or_publish);
	UT_RUN(native_written_delta_or_B_grant_drift_stops_before_next_effect);
	free(procArray); UT_DONE(); return ut_failed_count ? 1 : 0;
}
