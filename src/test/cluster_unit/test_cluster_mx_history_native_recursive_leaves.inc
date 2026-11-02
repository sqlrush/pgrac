/* Typed link traps for native branches outside this directed fixture.
 * Any unexpected legacy authority, VM/census/RF or mutation leaf aborts. */
static unsigned native_reads, native_releases, native_visibility, native_reserves, native_prepared_checks, native_fault, native_pins;
static Snapshot native_snapshot;
static unsigned native_retry_limit, native_requalifications;
BackendType MyBackendType = B_BACKEND;
int wal_level = WAL_LEVEL_REPLICA;
static void recursive_unexpected(const char *name) pg_attribute_noreturn();
static void recursive_unexpected(const char *name)
{ fprintf(stderr, "unexpected native leaf: %s\n", name); abort(); }
#ifndef HISTORY_NATIVE_APPLY_FIXTURE
SCN
cluster_scn_advance(void)
{ recursive_unexpected("cluster_scn_advance"); }
#endif
bool
TransactionIdDidCommit(TransactionId p0)
{ recursive_unexpected("TransactionIdDidCommit"); }
bool
TransactionIdDidAbort(TransactionId p0)
{ recursive_unexpected("TransactionIdDidAbort"); }
bool
TransactionIdFollowsOrEquals(TransactionId p0, TransactionId p1)
{ recursive_unexpected("TransactionIdFollowsOrEquals"); }
Buffer
ReadBuffer(Relation p0, BlockNumber p1)
{ UT_ASSERT(p0 == &relation && p1 == 3 && !content_locked); native_reads++; native_pins++; return 1; }
void
ReleaseBuffer(Buffer p0)
{ UT_ASSERT_EQ(p0, 1); UT_ASSERT_EQ(native_pins, 1); native_pins--; native_releases++; }
void
UnlockReleaseBuffer(Buffer p0)
{ UT_ASSERT(content_locked); LockBuffer(p0, BUFFER_LOCK_UNLOCK); ReleaseBuffer(p0); }
#ifndef HISTORY_NATIVE_APPLY_FIXTURE
void
MarkBufferDirty(Buffer p0)
{ recursive_unexpected("MarkBufferDirty"); }
#endif
void
BufferGetTag(Buffer p0, RelFileLocator * p1, ForkNumber * p2, BlockNumber * p3)
{ recursive_unexpected("BufferGetTag"); }
void
MarkBufferDirtyHint(Buffer p0, bool p1)
{ recursive_unexpected("MarkBufferDirtyHint"); }
bool
ClusterLockBufferExclusiveAuxiliaryAliasAware(Buffer * p0, Buffer * p1, const ClusterBufferBarrierSiteId * p2, struct ResourceXAuxiliaryAcquireContext * p3)
{ recursive_unexpected("ClusterLockBufferExclusiveAuxiliaryAliasAware"); }
bool
ClusterLockBufferShareBarrierAware(Buffer p0)
{ recursive_unexpected("ClusterLockBufferShareBarrierAware"); }
void
TestForOldSnapshot_impl(Snapshot p0, Relation p1)
{ recursive_unexpected("TestForOldSnapshot_impl"); }
SMgrRelation
smgropen(RelFileLocator p0, BackendId p1)
{ recursive_unexpected("smgropen"); }
void
smgrsetowner(SMgrRelation * p0, SMgrRelation p1)
{ recursive_unexpected("smgrsetowner"); }
ResourceXWriterPath
cluster_resource_x_writer_path_snapshot(uint64 * p0)
{ recursive_unexpected("cluster_resource_x_writer_path_snapshot"); }
ClusterNormalStartState
cluster_semantic_normal_start_state(void)
{ recursive_unexpected("cluster_semantic_normal_start_state"); }
ClusterSemanticAdmissionResult
cluster_semantic_activation_enter_r4_terminal_census(ClusterSemanticAdmissionToken * p0)
{ recursive_unexpected("cluster_semantic_activation_enter_r4_terminal_census"); }
bool
cluster_semantic_activation_recovered_open_current(void)
{ recursive_unexpected("cluster_semantic_activation_recovered_open_current"); }
void
cluster_vis_evidence_note(ClusterVisEvidenceMetric p0)
{ recursive_unexpected("cluster_vis_evidence_note"); }
#ifndef HISTORY_NATIVE_APPLY_FIXTURE
void
cluster_tt_local_record_active(TransactionId p0)
{ recursive_unexpected("cluster_tt_local_record_active"); }
#endif
void
cluster_runtime_visibility_ensure_exit_hooks(void)
{ recursive_unexpected("cluster_runtime_visibility_ensure_exit_hooks"); }
ClusterTxOutcome
cluster_runtime_visibility_resolve_exact_origin(const ClusterTxLocator * p0, ClusterTxResolveMode p1, uint64 p2, ClusterTxResolution * p3, ClusterTxResolveReason * p4)
{ recursive_unexpected("cluster_runtime_visibility_resolve_exact_origin"); }
ClusterTxOutcome
cluster_runtime_visibility_resolve_exact_origin_admitted(const ClusterTxLocator * p0, ClusterTxResolveMode p1, const ClusterSemanticAdmissionToken * p2, ClusterTxResolution * p3, ClusterTxResolveReason * p4)
{ recursive_unexpected("cluster_runtime_visibility_resolve_exact_origin_admitted"); }
ClusterTxOutcome
cluster_runtime_visibility_resolve_terminal_census_retained_exact(const ClusterTxLocator * p0, SCN p1, const ClusterSemanticAdmissionToken * p2, ClusterTxResolution * p3, ClusterTxResolveReason * p4)
{ recursive_unexpected("cluster_runtime_visibility_resolve_terminal_census_retained_exact"); }
bool
LWLockHeldByMe(LWLock * p0)
{ recursive_unexpected("LWLockHeldByMe"); }
MultiXactId
MultiXactIdCreate(TransactionId p0, MultiXactStatus p1, TransactionId p2, MultiXactStatus p3)
{ recursive_unexpected("MultiXactIdCreate"); }
MultiXactId
MultiXactIdExpand(MultiXactId p0, TransactionId p1, MultiXactStatus p2)
{ recursive_unexpected("MultiXactIdExpand"); }
bool
MultiXactIdIsRunning(MultiXactId p0, bool p1)
{ recursive_unexpected("MultiXactIdIsRunning"); }
#ifndef HISTORY_NATIVE_APPLY_FIXTURE
bool
cluster_ctrc_sha256_exact(const void * p0, Size p1, uint8 * p2)
{ recursive_unexpected("cluster_ctrc_sha256_exact"); }
#endif
#ifndef HISTORY_NATIVE_APPLY_FIXTURE
void
cluster_ctrc_note_publication_after_apply(const ClusterCtrcReceiptHandle * p0, bool p1)
{ recursive_unexpected("cluster_ctrc_note_publication_after_apply"); }
#endif
#ifndef HISTORY_NATIVE_APPLY_FIXTURE
bool
cluster_ctrc_receipt_itl_reuse_candidate_shared(const ClusterCtrcReceiptHandle * p0, const ClusterCtrcTargetV1 * p1, const ClusterCtrcItlTargetIdentity * p2, const uint8 * p3)
{ recursive_unexpected("cluster_ctrc_receipt_itl_reuse_candidate_shared"); }
#endif
const char *
cluster_undo_record_receipt_last_reason(void)
{ recursive_unexpected("cluster_undo_record_receipt_last_reason"); }
#ifndef HISTORY_NATIVE_APPLY_FIXTURE
ClusterUndoRecordPrepareResult
cluster_undo_record_requalify_for_retry(ClusterUndoRecordPrepareReceipt * p0, uint16 p1, bool p2)
{ UT_ASSERT(!content_locked && p0->ctrc_applied_mask == 0 && native_retry_limit != 0);
  native_requalifications++;
  if (native_fault == 9 || native_fault == 10) {
    mx_history_native_attempt_change(native_fault == 10); native_fault = 0;
  }
  return CLUSTER_UNDO_RECORD_PREPARE_READY; }
#endif
uint64
cluster_undo_record_prepare_deadline_us(void)
{ UT_ASSERT(!content_locked); return 5000000; }
#ifndef HISTORY_NATIVE_APPLY_FIXTURE
ClusterUndoRecordPrepareResult
cluster_undo_record_prepare(uint8 p0, uint16 p1, uint16 p2, uint16 p3, UBA p4, uint64 p5, ClusterUndoRecordPrepareReceipt * p6)
{ UT_ASSERT(!content_locked); UT_ASSERT_EQ(p0, UNDO_RECORD_ITL); UT_ASSERT_EQ(p2, 257); UT_ASSERT_EQ(p3, 3); UT_ASSERT_EQ(p5, 5000000); memset(p6, 0, sizeof(*p6)); p6->record_type = p0; p6->payload_capacity = p1; p6->tt_slot_segment_id = p2; p6->tt_slot_offset = p3; native_reserves++; if (native_fault == 1) two_tuple(2).t_data->t_infomask2++; if (native_fault == 2) pcm_source.generation++; if (native_fault == 3) CurrentResourceOwner = (ResourceOwner)2; if (native_fault == 4) writer_grant_ok = false;
	if (native_fault == 5) BufferBlocks[((PageHeader)BufferBlocks)->pd_lower] ^= 1;
	if (native_fault == 6) native_snapshot->read_epoch++;
	if (native_fault == 7) pg_atomic_write_u32(&history_gate.transition_closed, 1);
	if (native_fault == 8) publication_A_grant_revoked = true; return CLUSTER_UNDO_RECORD_PREPARE_READY; }
#endif
#ifndef HISTORY_NATIVE_APPLY_FIXTURE
bool
cluster_undo_record_prepared_recheck(const ClusterUndoRecordPrepareReceipt * p0, uint16 p1)
{ UT_ASSERT(content_locked && exclusive); native_prepared_checks++; if (native_prepared_checks < native_retry_limit) return false; ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("directed cancellation at prepared storage recheck"))); return false; }
#endif
#ifndef HISTORY_NATIVE_APPLY_FIXTURE
bool
cluster_undo_record_prepared_uba_exact(const ClusterUndoRecordPrepareReceipt * p0, uint16 p1, UBA * p2)
{ recursive_unexpected("cluster_undo_record_prepared_uba_exact"); }
#endif
#ifndef HISTORY_NATIVE_APPLY_FIXTURE
bool
cluster_undo_record_stage_history(ClusterUndoRecordPrepareReceipt * p0, uint8 p1, const UndoItlHistoryEntry * p2)
{ recursive_unexpected("cluster_undo_record_stage_history"); }
#endif
#ifndef HISTORY_NATIVE_APPLY_FIXTURE
bool
cluster_undo_record_history_matches(const ClusterUndoRecordPrepareReceipt * p0, uint8 p1, const UndoItlHistoryEntry * p2)
{ recursive_unexpected("cluster_undo_record_history_matches"); }
#endif
#ifndef HISTORY_NATIVE_APPLY_FIXTURE
bool
cluster_undo_record_ctrc_stage_pending(ClusterUndoRecordPrepareReceipt * p0, uint8 p1, const ClusterCtrcTargetV1 * p2)
{ recursive_unexpected("cluster_undo_record_ctrc_stage_pending"); }
#endif
#ifndef HISTORY_NATIVE_APPLY_FIXTURE
bool
cluster_undo_record_ctrc_prepare_pending(ClusterUndoRecordPrepareReceipt * p0, uint8 p1)
{ recursive_unexpected("cluster_undo_record_ctrc_prepare_pending"); }
#endif
bool
cluster_undo_record_ctrc_stage_reuse(ClusterUndoRecordPrepareReceipt * p0, uint8 p1, const ClusterCtrcReceiptHandle * p2)
{ recursive_unexpected("cluster_undo_record_ctrc_stage_reuse"); }
#ifndef HISTORY_NATIVE_APPLY_FIXTURE
bool
cluster_undo_record_ctrc_pending_matches(const ClusterUndoRecordPrepareReceipt * p0, uint8 p1, const ClusterCtrcTargetV1 * p2)
{ recursive_unexpected("cluster_undo_record_ctrc_pending_matches"); }
#endif
#ifndef HISTORY_NATIVE_APPLY_FIXTURE
bool
cluster_undo_record_ctrc_pending_recheck(const ClusterUndoRecordPrepareReceipt * p0, uint8 p1, const ClusterCtrcTargetV1 * p2)
{ recursive_unexpected("cluster_undo_record_ctrc_pending_recheck"); }
#endif
#ifndef HISTORY_NATIVE_APPLY_FIXTURE
bool
cluster_undo_record_ctrc_required_prepared(const ClusterUndoRecordPrepareReceipt * p0, uint8 p1)
{ recursive_unexpected("cluster_undo_record_ctrc_required_prepared"); }
#endif
#ifndef HISTORY_NATIVE_APPLY_FIXTURE
ClusterCtrcApplyResult
cluster_undo_record_ctrc_apply_prepared(ClusterUndoRecordPrepareReceipt * p0, uint8 p1, const ClusterCtrcTargetV1 * p2, ClusterCtrcApplyToken * p3)
{ recursive_unexpected("cluster_undo_record_ctrc_apply_prepared"); }
#endif
#ifndef HISTORY_NATIVE_APPLY_FIXTURE
void
cluster_undo_record_cancel_prepared(ClusterUndoRecordPrepareReceipt * p0)
{ recursive_unexpected("cluster_undo_record_cancel_prepared"); }
#endif
#ifndef HISTORY_NATIVE_APPLY_FIXTURE
bool
cluster_undo_record_release_zero_effect_preflight(ClusterUndoRecordPrepareReceipt * p0)
{ recursive_unexpected("cluster_undo_record_release_zero_effect_preflight"); }
#endif
#ifndef HISTORY_NATIVE_APPLY_FIXTURE
ClusterUndoRecordConsumePreflightResult
cluster_undo_record_consume_preflight(ClusterUndoRecordPrepareReceipt * p0, uint16 p1)
{ recursive_unexpected("cluster_undo_record_consume_preflight"); }
#endif
#ifndef HISTORY_NATIVE_APPLY_FIXTURE
ClusterUndoRecordConsumeResult
cluster_undo_record_consume_prepared(ClusterUndoRecordPrepareReceipt * p0, const ClusterUndoRecordTarget * p1, const void * p2, uint16 p3, UBA * p4)
{ recursive_unexpected("cluster_undo_record_consume_prepared"); }
#endif
ClusterBufmgrItlRecycleGuardResult
cluster_bufmgr_itl_recycle_guard_arm(Buffer p0, const ClusterPcmOwnSnapshot * p1)
{ recursive_unexpected("cluster_bufmgr_itl_recycle_guard_arm"); }
void
cluster_bufmgr_itl_recycle_guard_unlock(Buffer p0)
{ recursive_unexpected("cluster_bufmgr_itl_recycle_guard_unlock"); }
bool
cluster_bufmgr_itl_recycle_guard_relock(Buffer p0)
{ recursive_unexpected("cluster_bufmgr_itl_recycle_guard_relock"); }
void
cluster_bufmgr_itl_recycle_guard_cancel(Buffer p0)
{ recursive_unexpected("cluster_bufmgr_itl_recycle_guard_cancel"); }
bool
HeapTupleSatisfiesVisibility(HeapTuple p0, Snapshot p1, Buffer p2)
{ UT_ASSERT(p1 == SnapshotAny && p2 == 1); UT_ASSERT(content_locked && !exclusive); native_visibility++; return true; }
HTSV_Result
HeapTupleSatisfiesVacuum(HeapTuple p0, TransactionId p1, Buffer p2)
{ recursive_unexpected("HeapTupleSatisfiesVacuum"); }
bool
HeapTupleHeaderIsOnlyLocked(HeapTupleHeader p0)
{ recursive_unexpected("HeapTupleHeaderIsOnlyLocked"); }
TransactionId
SubTransGetTopmostTransaction(TransactionId p0)
{ recursive_unexpected("SubTransGetTopmostTransaction"); }
bool
visibilitymap_clear(Relation p0, BlockNumber p1, Buffer p2, uint8 p3)
{ recursive_unexpected("visibilitymap_clear"); }
bool
visibilitymap_clear_locked(Relation p0, BlockNumber p1, Buffer p2, uint8 p3)
{ recursive_unexpected("visibilitymap_clear_locked"); }
void
visibilitymap_pin(Relation p0, BlockNumber p1, Buffer * p2)
{ recursive_unexpected("visibilitymap_pin"); }
bool
visibilitymap_pin_ok(BlockNumber p0, Buffer p1)
{ recursive_unexpected("visibilitymap_pin_ok"); }
uint8
visibilitymap_get_status(Relation p0, BlockNumber p1, Buffer * p2)
{ recursive_unexpected("visibilitymap_get_status"); }
#ifndef HISTORY_NATIVE_APPLY_FIXTURE
void
XLogBeginInsert(void)
{ recursive_unexpected("XLogBeginInsert"); }
#endif
#ifndef HISTORY_NATIVE_APPLY_FIXTURE
XLogRecPtr
XLogInsert(RmgrId p0, uint8 p1)
{ recursive_unexpected("XLogInsert"); }
#endif
#ifndef HISTORY_NATIVE_APPLY_FIXTURE
void
XLogRegisterData(char * p0, uint32 p1)
{ recursive_unexpected("XLogRegisterData"); }
#endif
#ifndef HISTORY_NATIVE_APPLY_FIXTURE
void
XLogRegisterBuffer(uint8 p0, Buffer p1, uint8 p2)
{ recursive_unexpected("XLogRegisterBuffer"); }
#endif
void
XactLockTableWait(TransactionId p0, Relation p1, ItemPointer p2, XLTW_Oper p3)
{ recursive_unexpected("XactLockTableWait"); }
void
PredicateLockTID(Relation p0, ItemPointer p1, Snapshot p2, TransactionId p3)
{ UT_ASSERT(p0 == &relation && p2 == SnapshotAny); }
bool
CheckForSerializableConflictOutNeeded(Relation p0, Snapshot p1)
{ UT_ASSERT(p0 == &relation && p1 == SnapshotAny); return false; }
void
CheckForSerializableConflictOut(Relation p0, TransactionId p1, Snapshot p2)
{ recursive_unexpected("CheckForSerializableConflictOut"); }
bool
TransactionIdIsInProgress(TransactionId p0)
{ recursive_unexpected("TransactionIdIsInProgress"); }
void
cluster_catalog_stats_vis_unknown_inc(void)
{ recursive_unexpected("cluster_catalog_stats_vis_unknown_inc"); }
ClusterTxwResult
cluster_tx_enqueue_wait_exact(const ClusterTxLocator * p0, int p1, ClusterTxResolveReason * p2)
{ recursive_unexpected("cluster_tx_enqueue_wait_exact"); }
bool
HeapTupleSatisfiesMVCCScratch(HeapTuple p0, Snapshot p1, const ClusterR4HotScratchTestContext * p2)
{ recursive_unexpected("HeapTupleSatisfiesMVCCScratch"); }
bool
cluster_heap_tuple_satisfies_visibility_waitable(HeapTuple p0, Snapshot p1, Buffer p2, bool * p3, ClusterTxLocator * p4)
{ recursive_unexpected("cluster_heap_tuple_satisfies_visibility_waitable"); }
ClusterCrBuildResult
cluster_gcs_block_cr_fetch_projection(BufferTag p0, SCN p1, char * p2, ClusterR4ProjectionScratch * p3, ClusterCrBuildReason * p4)
{ recursive_unexpected("cluster_gcs_block_cr_fetch_projection"); }
bool
cluster_bufmgr_block_write_permitted(Buffer p0)
{ return p0 == 1 && content_locked && exclusive && pcm_source.pcm_state == PCM_STATE_X && pcm_source.flags == 0; }
#ifndef HISTORY_NATIVE_APPLY_FIXTURE
void
cluster_itl_touch_register_exact_ctrc(const ClusterItlTouchHandle * p0, Buffer p1, TransactionId p2, const ClusterCtrcReceiptHandle * p3, const ClusterSpaceIdentity * p4)
{ recursive_unexpected("cluster_itl_touch_register_exact_ctrc"); }
#endif
#ifndef HISTORY_NATIVE_APPLY_FIXTURE
bool
cluster_itl_touch_lookup_reusable_ctrc(const ClusterItlTouchHandle * p0, Buffer p1, TransactionId p2, ClusterCtrcReceiptHandle * p3)
{ recursive_unexpected("cluster_itl_touch_lookup_reusable_ctrc"); }
#endif
bool
rf_page_producer_stamp_v1(RfPageProducerBatchV1 * p0)
{ recursive_unexpected("rf_page_producer_stamp_v1"); }
bool
rf_page_producer_register_wal_v1(const RfPageProducerBatchV1 * p0)
{ recursive_unexpected("rf_page_producer_register_wal_v1"); }
bool
cluster_space_relation_get_identity(Relation p0, ClusterSpaceIdentity * p1)
{ recursive_unexpected("cluster_space_relation_get_identity"); }
ClusterSpaceHintResult
cluster_space_hint_begin(Buffer p0, RfPageProducerBatchV1 * p1)
{ recursive_unexpected("cluster_space_hint_begin"); }
void
cluster_space_hint_finish(Buffer p0, bool p1, const RfPageProducerBatchV1 * p2)
{ recursive_unexpected("cluster_space_hint_finish"); }
bool
cluster_space_prepare_buffer_versions(const ClusterSpaceIdentity * p0, const Buffer * p1, const uint8 * p2, uint8 p3, RfPageProducerBatchV1 * p4)
{ recursive_unexpected("cluster_space_prepare_buffer_versions"); }
int
cluster_smgr_which_for(RelFileLocator p0, BackendId p1)
{ recursive_unexpected("cluster_smgr_which_for"); }
bool cluster_merged_any_remote_materialized(void) { return false; }
bool cluster_merged_instance_is_materialized(int origin_node) { UT_ASSERT(origin_node >= 0 && origin_node < 4); return false; }
void cluster_r4_observe(ClusterR4Event event, ClusterTxResolveReason reason, uint32 elapsed_ms) { recursive_unexpected(__func__); }
