/* Original xact.c private state and real backend collector. Transaction phase
 * scheduling is a host boundary: these are point observations, not execution
 * of transaction effects or a native producer-closure proof.
 * Author: SqlRush <sqlrush@gmail.com> */
static void
xact_return_reset(void)
{
 memset(&TopTransactionStateData, 0, sizeof(TopTransactionStateData));
 CurrentTransactionState = &TopTransactionStateData;
}

UT_TEST(backend_return_retains_each_original_transaction_phase)
{
 const TransState phases[] = {TRANS_START, TRANS_INPROGRESS, TRANS_COMMIT,
  TRANS_ABORT, TRANS_PREPARE};
 for (unsigned i = 0; i < lengthof(phases); ++i) {
  ClusterServiceBackendCollection out; ClusterSemanticAdmissionObservationResult result;
  bool unchanged;
  return_context_begin(); xact_return_reset(); TopTransactionStateData.state = phases[i];
  UT_ASSERT_EQ(IsTransactionState(), phases[i] == TRANS_INPROGRESS);
  result = backend_return_read(&out);
  unchanged = TopTransactionStateData.state == phases[i];
  xact_return_reset();
  UT_ASSERT_EQ(result, CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
  UT_ASSERT(unchanged && out.snapshots.known && out.admission.known);
  UT_ASSERT_EQ(out.snapshots.references, 0);
  UT_ASSERT_STR_EQ(out.reason, "BACKEND_COLLECTION_TRANSACTION_PENDING");
 }
}

UT_TEST(backend_return_retains_each_original_transaction_block_phase)
{
 for (TBlockState phase = TBLOCK_STARTED; phase <= TBLOCK_SUBABORT_RESTART; ++phase) {
  ClusterServiceBackendCollection out; ClusterSemanticAdmissionObservationResult result;
  bool unchanged;
  return_context_begin(); xact_return_reset(); TopTransactionStateData.blockState = phase;
  UT_ASSERT(!IsTransactionState());
  result = backend_return_read(&out);
  unchanged = TopTransactionStateData.blockState == phase;
  xact_return_reset();
  UT_ASSERT_EQ(result, CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
  UT_ASSERT(unchanged && out.snapshots.known && out.admission.known);
  UT_ASSERT_STR_EQ(out.reason, "BACKEND_COLLECTION_TRANSACTION_PENDING");
 }
}

UT_TEST(backend_return_retains_original_subtransaction_state)
{
 ClusterServiceBackendCollection out; TransactionStateData child = {0};
 ClusterSemanticAdmissionObservationResult result;
 bool unchanged;
 return_context_begin(); xact_return_reset();
 child.parent = &TopTransactionStateData; CurrentTransactionState = &child;
 UT_ASSERT(!IsTransactionState() && !IsTransactionOrTransactionBlock());
 result = backend_return_read(&out);
 unchanged = CurrentTransactionState == &child && child.parent == &TopTransactionStateData;
 xact_return_reset();
 UT_ASSERT_EQ(result, CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
 UT_ASSERT(unchanged && out.snapshots.known && out.admission.known);
 UT_ASSERT_STR_EQ(out.reason, "BACKEND_COLLECTION_TRANSACTION_PENDING");
}

static TransState xact_return_new_phase;
static unsigned xact_return_barriers;
static void
xact_return_change(void)
{
 if (++xact_return_barriers != 2) return;
 snapshot_return_schedule = NULL;
 TopTransactionStateData.state = xact_return_new_phase;
}
UT_TEST(backend_return_discards_transaction_change_during_shared_cut)
{
 for (unsigned returning = 0; returning < 2; ++returning) {
  ClusterServiceBackendCollection out; ClusterSemanticAdmissionObservationResult result;
  bool unchanged;
  return_context_begin(); xact_return_reset(); xact_return_barriers = 0;
  TopTransactionStateData.state = returning ? TRANS_COMMIT : TRANS_DEFAULT;
  xact_return_new_phase = returning ? TRANS_DEFAULT : TRANS_START;
  snapshot_return_schedule = xact_return_change;
  result = backend_return_read(&out); snapshot_return_schedule = NULL;
  unchanged = TopTransactionStateData.state == xact_return_new_phase;
  xact_return_reset();
  UT_ASSERT_EQ(result, CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
  UT_ASSERT(unchanged && xact_return_barriers == 2);
  backend_return_empty(&out);
  UT_ASSERT_STR_EQ(out.reason, "BACKEND_COLLECTION_POINT_CHANGED_OR_UNKNOWN");
 }
}

UT_TEST(backend_return_transaction_observation_preserves_resources_and_empty_pending)
{
 ClusterServiceBackendCollection out; Snapshot held;
 ClusterSemanticAdmissionObservationResult result;
 bool unchanged;
 return_context_begin(); xact_return_reset();
 TopTransactionStateData.state = TRANS_ABORT; return_context_hold(&held);
 ResourceOwnerEnlargeBufferIOs(CurrentResourceOwner);
 ResourceOwnerRememberBufferIO(CurrentResourceOwner, 1);
 result = backend_return_read(&out);
 unchanged = TopTransactionStateData.state == TRANS_ABORT
  && CurrentResourceOwner->bufferioarr.nitems == 1;
 ResourceOwnerForgetBufferIO(CurrentResourceOwner, 1);
 return_context_release(held); xact_return_reset();
 UT_ASSERT_EQ(result, CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
 UT_ASSERT(unchanged && out.snapshots.known && out.admission.known);
 UT_ASSERT(out.buffer_pins_pending && out.buffer_io_pending);
 UT_ASSERT_EQ(out.snapshots.references, CLUSTER_SERVICE_SNAPSHOT_REGISTERED);
 UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
 UT_ASSERT_STR_EQ(out.reason, "BACKEND_COLLECTION_PRODUCER_CLOSURE_UNPROVED");
}
