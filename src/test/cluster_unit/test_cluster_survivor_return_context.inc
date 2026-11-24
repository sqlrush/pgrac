/* Original snapmgr and native PinBuffer/UnpinBuffer retain their resources.
 * Return scheduling is a host boundary. Original ResourceOwner objects and
 * BufferIO bookkeeping execute below; neither is producer closure.
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned return_context_fault, return_context_barriers;
static ResourceOwner return_context_original;
static void
return_context_change(void)
{
 if (++return_context_barriers != 2) return;
 snapshot_return_schedule = NULL;
 if (return_context_fault == 0) CurrentResourceOwner = backend_resource_two;
 else if (return_context_fault == 1) CurrentResourceOwner = NULL;
 else proc_exit_inprogress = true;
}
static void
return_context_final_change(void)
{
 survivor_poll_barrier_hook = NULL;
 if (return_context_fault == 0) CurrentResourceOwner = backend_resource_two;
 else if (return_context_fault == 1) CurrentResourceOwner = NULL;
 else proc_exit_inprogress = true;
}
static void
return_context_begin(void)
{
 proc_exit_inprogress = false; CurrentResourceOwner = backend_resource_one;
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
  return_context_begin(); CurrentResourceOwner = owner ? backend_resource_two : NULL;
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

UT_TEST(backend_bufferio_observation_follows_original_remember_and_forget)
{
 ClusterServiceBackendCollection out;
 return_context_begin();
 ResourceOwnerEnlargeBufferIOs(CurrentResourceOwner);
 ResourceOwnerRememberBufferIO(CurrentResourceOwner, 1);
 ResourceOwnerEnlargeBufferIOs(CurrentResourceOwner);
 ResourceOwnerRememberBufferIO(CurrentResourceOwner, 2);
 UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
 UT_ASSERT(!out.buffer_pins_pending && out.snapshots.references == 0);
 UT_ASSERT(out.buffer_io_pending);
 UT_ASSERT_STR_EQ(out.reason, "BACKEND_COLLECTION_BUFFER_IO_PENDING");
 UT_ASSERT_EQ(CurrentResourceOwner->bufferioarr.nitems, 2);
 ResourceOwnerForgetBufferIO(CurrentResourceOwner, 1);
 UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
 UT_ASSERT_STR_EQ(out.reason, "BACKEND_COLLECTION_BUFFER_IO_PENDING");
 UT_ASSERT_EQ(CurrentResourceOwner->bufferioarr.nitems, 1);
 ResourceOwnerForgetBufferIO(CurrentResourceOwner, 2);
 UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
 UT_ASSERT_STR_EQ(out.reason, "BACKEND_COLLECTION_PRODUCER_CLOSURE_UNPROVED");
 UT_ASSERT_EQ(CurrentResourceOwner->bufferioarr.nitems, 0);
 UT_ASSERT(!out.buffer_io_pending);
}

UT_TEST(backend_bufferio_current_owner_does_not_claim_parent_or_null_closure)
{
 ClusterServiceBackendCollection out; ResourceOwner parent, child;
 return_context_begin(); parent = CurrentResourceOwner;
 ResourceOwnerEnlargeBufferIOs(parent); ResourceOwnerRememberBufferIO(parent, 1);
 child = ResourceOwnerCreate(parent, "bufferio-child"); CurrentResourceOwner = child;
 UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
 UT_ASSERT(!out.buffer_io_pending);
 UT_ASSERT_STR_EQ(out.reason, "BACKEND_COLLECTION_PRODUCER_CLOSURE_UNPROVED");
 UT_ASSERT_EQ(parent->bufferioarr.nitems, 1);
 CurrentResourceOwner = NULL;
 UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
 UT_ASSERT(out.buffer_io_pending); /* Unknown owner, never a proved IO count. */
 UT_ASSERT_STR_EQ(out.reason, "BACKEND_COLLECTION_PRODUCER_CLOSURE_UNPROVED");
 UT_ASSERT_EQ(parent->bufferioarr.nitems, 1);
 CurrentResourceOwner = parent; ResourceOwnerDelete(child); ResourceOwnerForgetBufferIO(parent, 1);
}

static unsigned bufferio_return_barriers;
static bool bufferio_return_arrives;
static void
bufferio_return_change(void)
{
 if (++bufferio_return_barriers != 2) return;
 snapshot_return_schedule = NULL;
 if (bufferio_return_arrives) ResourceOwnerRememberBufferIO(CurrentResourceOwner, 1);
 else ResourceOwnerForgetBufferIO(CurrentResourceOwner, 1);
}
UT_TEST(backend_bufferio_change_during_shared_cut_discards_the_point)
{
 ClusterServiceBackendCollection out;
 for (unsigned mode = 0; mode < 2; ++mode) {
  return_context_begin(); ResourceOwnerEnlargeBufferIOs(CurrentResourceOwner);
  bufferio_return_arrives = mode == 0; bufferio_return_barriers = 0;
  if (!bufferio_return_arrives) ResourceOwnerRememberBufferIO(CurrentResourceOwner, 1);
  snapshot_return_schedule = bufferio_return_change;
  UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
  snapshot_return_schedule = NULL;
  UT_ASSERT_EQ(bufferio_return_barriers, 2);
  UT_ASSERT(!out.snapshots.known && !out.admission.known);
  UT_ASSERT(!out.buffer_io_pending);
  UT_ASSERT_STR_EQ(out.reason, "BACKEND_COLLECTION_POINT_CHANGED_OR_UNKNOWN");
  UT_ASSERT_EQ(CurrentResourceOwner->bufferioarr.nitems, bufferio_return_arrives ? 1 : 0);
  if (bufferio_return_arrives) ResourceOwnerForgetBufferIO(CurrentResourceOwner, 1);
 }
}

UT_TEST(survivor_backend_final_refusal_retains_original_bufferio)
{
 ClusterSurvivorProducerObservation out; ClusterSurvivorProducerScope scope = {7,7,19};
 ClusterSemanticAdmissionObservationResult result;
 for (return_context_fault = 0; return_context_fault < 3; ++return_context_fault) {
  return_context_begin(); ResourceOwnerEnlargeBufferIOs(CurrentResourceOwner);
  ResourceOwnerRememberBufferIO(CurrentResourceOwner, 1);
  survivor_poll_barrier_hook = return_context_final_change;
  result = cluster_semantic_survivor_producer_poll(CLUSTER_SURVIVOR_PRODUCER_BACKEND, scope, &out);
  survivor_poll_barrier_hook = NULL; proc_exit_inprogress = false;
  CurrentResourceOwner = return_context_original;
  UT_ASSERT_EQ(result, CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED);
  survivor_poll_unknown(&out);
  UT_ASSERT_EQ(CurrentResourceOwner->bufferioarr.nitems, 1);
  ResourceOwnerForgetBufferIO(CurrentResourceOwner, 1);
 }
}

static ResourceOwner bufferio_deleted_owner;
static void
bufferio_delete_at_first_snapshot_cut(void)
{
 snapshot_return_schedule = NULL;
 CurrentResourceOwner = backend_resource_one;
 ResourceOwnerDelete(bufferio_deleted_owner);
}
UT_TEST(backend_bufferio_never_dereferences_a_retired_entry_owner)
{
 ClusterServiceBackendCollection out;
 return_context_begin();
 bufferio_deleted_owner = ResourceOwnerCreate(NULL, "retired-before-first-IO-observation");
 CurrentResourceOwner = bufferio_deleted_owner;
 snapshot_return_schedule = bufferio_delete_at_first_snapshot_cut;
 UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED);
 snapshot_return_schedule = NULL;
 UT_ASSERT_EQ(CurrentResourceOwner, backend_resource_one);
 UT_ASSERT(!out.snapshots.known && !out.admission.known && !out.buffer_io_pending);
}
