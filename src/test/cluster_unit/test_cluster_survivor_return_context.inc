/* Original snapmgr and native PinBuffer/UnpinBuffer retain their resources.
 * ResourceOwner identities and return scheduling are explicit host boundaries;
 * these negative cuts do not claim a native ResourceOwner lifecycle or closure.
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned return_context_fault, return_context_barriers;
static ResourceOwner return_context_original;
static void
return_context_change(void)
{
 if (++return_context_barriers != 2) return;
 snapshot_return_schedule = NULL;
 if (return_context_fault == 0) CurrentResourceOwner = (ResourceOwner)2;
 else if (return_context_fault == 1) CurrentResourceOwner = NULL;
 else proc_exit_inprogress = true;
}
static void
return_context_final_change(void)
{
 survivor_poll_barrier_hook = NULL;
 if (return_context_fault == 0) CurrentResourceOwner = (ResourceOwner)2;
 else if (return_context_fault == 1) CurrentResourceOwner = NULL;
 else proc_exit_inprogress = true;
}
static void
return_context_begin(void)
{
 proc_exit_inprogress = false; CurrentResourceOwner = (ResourceOwner)1;
 pin_return_begin(); return_context_original = CurrentResourceOwner;
 return_context_barriers = 0;
}
static void
return_context_hold(Snapshot *snapshot)
{
 *snapshot = registered(80);
 UT_ASSERT(PinBuffer(GetBufferDescriptor(0), NULL));
 UT_ASSERT_EQ(remembered, 1); UT_ASSERT_EQ(pin_remembered, 1);
}
static void
return_context_release(Snapshot snapshot)
{
 /* Observation must not retire or transfer the actual held resources. */
 UT_ASSERT_EQ(remembered, 1); UT_ASSERT_EQ(pin_remembered, 1);
 UT_ASSERT_EQ(snapshot->regd_count, 1);
 CurrentResourceOwner = return_context_original; proc_exit_inprogress = false;
 UnpinBuffer(GetBufferDescriptor(0)); UnregisterSnapshot(snapshot);
 UT_ASSERT_EQ(remembered, 0); UT_ASSERT_EQ(pin_remembered, 0);
}
UT_TEST(backend_return_rejects_resource_owner_change_without_retiring_native_resources)
{
 for (unsigned fault = 0; fault < 2; ++fault) {
  ClusterServiceBackendCollection out; Snapshot held;
  ClusterSemanticAdmissionObservationResult result;
  return_context_begin(); return_context_hold(&held);
  return_context_fault = fault; snapshot_return_schedule = return_context_change;
  result = backend_return_read(&out); snapshot_return_schedule = NULL;
  return_context_release(held);
  UT_ASSERT_EQ(result, CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED);
  UT_ASSERT_EQ(return_context_barriers, 2); backend_return_empty(&out);
  UT_ASSERT(!out.buffer_pins_pending);
 }
}
UT_TEST(backend_return_rejects_exit_during_original_shared_cut_without_cleanup)
{
 ClusterServiceBackendCollection out; Snapshot held;
 ClusterSemanticAdmissionObservationResult result;
 return_context_begin(); return_context_hold(&held);
 return_context_fault = 2; snapshot_return_schedule = return_context_change;
 result = backend_return_read(&out); snapshot_return_schedule = NULL;
 return_context_release(held);
 UT_ASSERT_EQ(result, CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED);
 UT_ASSERT_EQ(return_context_barriers, 2); backend_return_empty(&out);
}
UT_TEST(backend_return_refuses_already_exiting_before_observing_original_owners)
{
 ClusterServiceBackendCollection out; Snapshot held;
 ClusterSemanticAdmissionObservationResult result;
 return_context_begin(); return_context_hold(&held);
 proc_exit_inprogress = true; snapshot_return_schedule = return_context_change;
 result = backend_return_read(&out); snapshot_return_schedule = NULL;
 return_context_release(held);
 UT_ASSERT_EQ(result, CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED);
 UT_ASSERT_EQ(return_context_barriers, 0); backend_return_empty(&out);
}
UT_TEST(backend_return_keeps_stable_null_and_nonnull_owner_points_pending)
{
 for (unsigned owner = 0; owner < 2; ++owner) {
  ClusterServiceBackendCollection out;
  return_context_begin(); CurrentResourceOwner = owner ? (ResourceOwner)2 : NULL;
  UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
  UT_ASSERT(out.snapshots.known && out.admission.known);
  UT_ASSERT(!out.buffer_pins_pending);
  UT_ASSERT_EQ(strcmp(out.reason, "BACKEND_COLLECTION_PRODUCER_CLOSURE_UNPROVED"), 0);
  CurrentResourceOwner = return_context_original;
 }
}
UT_TEST(survivor_backend_rejects_resource_owner_change_at_final_return)
{
 ClusterSurvivorProducerScope scope = {7,7,19};
 for (unsigned fault = 0; fault < 3; ++fault) {
  ClusterSurvivorProducerObservation out;
  ClusterSemanticAdmissionObservationResult result;
  return_context_begin();
  if (fault == 2) CurrentResourceOwner = NULL;
  return_context_fault = fault == 1 ? 1 : 0;
  survivor_poll_barrier_hook = return_context_final_change;
  result = cluster_semantic_survivor_producer_poll(CLUSTER_SURVIVOR_PRODUCER_BACKEND, scope, &out);
  CurrentResourceOwner = return_context_original; survivor_poll_barrier_hook = NULL;
  UT_ASSERT_EQ(result, CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED);
  survivor_poll_unknown(&out);
 }
}
UT_TEST(survivor_backend_rejects_exit_after_leaf_sample_without_retiring_resources)
{
 ClusterSurvivorProducerScope scope = {7,7,19}; ClusterSurvivorProducerObservation out;
 ClusterSemanticAdmissionObservationResult result; Snapshot held;
 return_context_begin(); return_context_hold(&held); return_context_fault = 2;
 survivor_poll_barrier_hook = return_context_final_change;
 result = cluster_semantic_survivor_producer_poll(CLUSTER_SURVIVOR_PRODUCER_BACKEND, scope, &out);
 survivor_poll_barrier_hook = NULL; return_context_release(held);
 UT_ASSERT_EQ(result, CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED);
 survivor_poll_unknown(&out);
}
UT_TEST(survivor_backend_refuses_already_exiting_before_leaf_observation)
{
 ClusterSurvivorProducerScope scope = {7,7,19}; ClusterSurvivorProducerObservation out;
 ClusterSemanticAdmissionObservationResult result;
 return_context_begin(); proc_exit_inprogress = true;
 snapshot_return_schedule = return_context_change;
 result = cluster_semantic_survivor_producer_poll(CLUSTER_SURVIVOR_PRODUCER_BACKEND, scope, &out);
 snapshot_return_schedule = NULL; proc_exit_inprogress = false;
 CurrentResourceOwner = return_context_original;
 UT_ASSERT_EQ(result, CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED);
 UT_ASSERT_EQ(return_context_barriers, 0); survivor_poll_unknown(&out);
}
UT_TEST(survivor_backend_stable_original_owner_keeps_pin_and_snapshot_debt)
{
 ClusterSurvivorProducerScope scope = {7,7,19}; ClusterSurvivorProducerObservation out;
 Snapshot held;
 return_context_begin(); return_context_hold(&held);
 UT_ASSERT_EQ(cluster_semantic_survivor_producer_poll(CLUSTER_SURVIVOR_PRODUCER_BACKEND, scope, &out),
  CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
 UT_ASSERT_EQ(CurrentResourceOwner, return_context_original);
 UT_ASSERT(out.sample.backend.snapshots.known && out.sample.backend.admission.known);
 UT_ASSERT(out.sample.backend.buffer_pins_pending);
 UT_ASSERT_EQ(out.sample.backend.snapshots.references, CLUSTER_SERVICE_SNAPSHOT_REGISTERED);
 return_context_release(held);
}
