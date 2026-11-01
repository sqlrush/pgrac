/* This is a real native private/shared pin lifetime, not a GCS grant.
 * Empty token/snapshot domains cannot hide the original pin owner.
 * Author: SqlRush <sqlrush@gmail.com> */
UT_TEST(backend_collection_retains_original_buffer_pin_until_native_unpin)
{
	ClusterServiceBackendCollection out;
	BufferDesc *buf;
	pin_return_begin(); buf = GetBufferDescriptor(0);
	UT_ASSERT(PinBuffer(buf, NULL));
	UT_ASSERT_EQ(pin_remembered, 1);
	UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&buf->state)), 1);
	UT_ASSERT(cluster_buffer_backend_has_pins());
	UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
	UT_ASSERT_EQ(strcmp(out.reason, "BACKEND_COLLECTION_BUFFER_PINS_PENDING"), 0);
	UT_ASSERT(out.buffer_pins_pending);
	UT_ASSERT_EQ(pin_remembered, 1);
	UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&buf->state)), 1);
	UnpinBuffer(buf);
	UT_ASSERT_EQ(pin_remembered, 0); UT_ASSERT(!cluster_buffer_backend_has_pins());
	UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
	UT_ASSERT_EQ(strcmp(out.reason, "BACKEND_COLLECTION_PRODUCER_CLOSURE_UNPROVED"), 0);
	UT_ASSERT(!out.buffer_pins_pending);
}

UT_TEST(backend_collection_keeps_nested_native_pin_until_last_return)
{
	ClusterServiceBackendCollection out;
	BufferDesc *buf;
	pin_return_begin(); buf = GetBufferDescriptor(1);
	UT_ASSERT(PinBuffer(buf, NULL)); UT_ASSERT(PinBuffer(buf, NULL));
	UT_ASSERT_EQ(pin_remembered, 2);
	UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&buf->state)), 1);
	for (unsigned step = 0; step < 2; ++step) {
		UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
		UT_ASSERT(out.buffer_pins_pending);
		UT_ASSERT_EQ(pin_remembered, 2-step);
		UnpinBuffer(buf);
	}
	UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
	UT_ASSERT(!out.buffer_pins_pending); UT_ASSERT_EQ(pin_remembered, 0);
}
UT_TEST(backend_collection_retains_original_overflow_pins_after_hot_array_drains)
{
	ClusterServiceBackendCollection out;
	pin_return_begin();
	for (unsigned i = 0; i < 17; ++i) UT_ASSERT(PinBuffer(GetBufferDescriptor(i), NULL));
	UT_ASSERT_EQ(PrivateRefCountOverflowed, 9); UT_ASSERT_EQ(pin_remembered, 17);
	for (unsigned i = 0; i < lengthof(PrivateRefCountArray); ++i) {
		Buffer b = PrivateRefCountArray[i].buffer;
		if (BufferIsValid(b)) UnpinBuffer(GetBufferDescriptor(b-1));
	}
	UT_ASSERT_EQ(PrivateRefCountOverflowed, 9); UT_ASSERT_EQ(pin_remembered, 9);
	for (unsigned i = 0; i < lengthof(PrivateRefCountArray); ++i)
		UT_ASSERT_EQ(PrivateRefCountArray[i].refcount, 0);
	UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
	UT_ASSERT(out.buffer_pins_pending); UT_ASSERT_EQ(pin_remembered, 9);
	for (unsigned i = 0; i < 17; ++i)
		if (GetPrivateRefCountEntry(i+1, false) != NULL) UnpinBuffer(GetBufferDescriptor(i));
	UT_ASSERT_EQ(PrivateRefCountOverflowed, 0); UT_ASSERT_EQ(pin_remembered, 0);
	UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
	UT_ASSERT(!out.buffer_pins_pending);
}
static bool pin_schedule_release;
static unsigned pin_schedule_count;
static void
pin_change_at_original_shared_cut(void)
{
	if (++pin_schedule_count != 2) return;
	snapshot_return_schedule = NULL;
	if (pin_schedule_release) UnpinBuffer(GetBufferDescriptor(0));
	else UT_ASSERT(PinBuffer(GetBufferDescriptor(0), NULL));
}
UT_TEST(backend_collection_discards_pin_arrival_and_return_during_shared_observation)
{
	ClusterServiceBackendCollection out;
	for (unsigned release = 0; release < 2; ++release) {
		pin_return_begin(); pin_schedule_release = release != 0; pin_schedule_count = 0;
		if (release) UT_ASSERT(PinBuffer(GetBufferDescriptor(0), NULL));
		snapshot_return_schedule = pin_change_at_original_shared_cut;
		UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
		snapshot_return_schedule = NULL; UT_ASSERT_EQ(pin_schedule_count, 2);
		UT_ASSERT_EQ(strcmp(out.reason, "BACKEND_COLLECTION_POINT_CHANGED_OR_UNKNOWN"), 0);
		backend_return_empty(&out); UT_ASSERT(!out.buffer_pins_pending);
		UT_ASSERT_EQ(pin_remembered, release ? 0 : 1);
		if (!release) UnpinBuffer(GetBufferDescriptor(0));
	}
}
UT_TEST(survivor_backend_poll_preserves_native_buffer_owner)
{
	ClusterSurvivorProducerObservation out;
	ClusterSurvivorProducerScope scope;
	pin_return_begin(); UT_ASSERT(PinBuffer(GetBufferDescriptor(0), NULL));
	scope.native_epoch = snapshot_return_epoch; scope.gate_epoch = 7; scope.record_generation = 19;
	UT_ASSERT_EQ(cluster_semantic_survivor_producer_poll(CLUSTER_SURVIVOR_PRODUCER_BACKEND, scope, &out),
		CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
	UT_ASSERT_EQ(out.owner, CLUSTER_SURVIVOR_PRODUCER_BACKEND);
	UT_ASSERT(out.sample.backend.buffer_pins_pending);
	UT_ASSERT_EQ(strcmp(out.reason, "BACKEND_COLLECTION_BUFFER_PINS_PENDING"), 0);
	UT_ASSERT_EQ(pin_remembered, 1);
	UnpinBuffer(GetBufferDescriptor(0));
	UT_ASSERT_EQ(cluster_semantic_survivor_producer_poll(CLUSTER_SURVIVOR_PRODUCER_BACKEND, scope, &out),
		CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
	UT_ASSERT(!out.sample.backend.buffer_pins_pending); UT_ASSERT_EQ(pin_remembered, 0);
}
UT_TEST(backend_collection_preserves_snapshot_and_pin_responsibilities_together)
{
	ClusterServiceBackendCollection out;
	Snapshot held;
	pin_return_begin(); held = registered(100); UT_ASSERT(PinBuffer(GetBufferDescriptor(0), NULL));
	UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
	UT_ASSERT(out.buffer_pins_pending);
	UT_ASSERT_EQ(out.snapshots.references, CLUSTER_SERVICE_SNAPSHOT_REGISTERED);
	UT_ASSERT_EQ(strcmp(out.reason, "SNAPSHOT_ORIGINAL_REFERENCES_PENDING"), 0);
	UnregisterSnapshot(held);
	UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
	UT_ASSERT(out.buffer_pins_pending); UT_ASSERT_EQ(out.snapshots.references, 0);
	UnpinBuffer(GetBufferDescriptor(0)); UT_ASSERT_EQ(pin_remembered, 0);
}
UT_TEST(backend_collection_never_converts_unknown_local_pin_array_to_clear)
{
	ClusterServiceBackendCollection out;
	pin_return_begin(); NLocBuffer = lengthof(pin_local_refs);
	/* Local-buffer memory is explicit host input, not a native PinLocalBuffer proof. */
	LocalRefCount = NULL;
	UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
	UT_ASSERT(out.buffer_pins_pending);
	LocalRefCount = pin_local_refs; pin_local_refs[1] = 1;
	UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
	UT_ASSERT(out.buffer_pins_pending); UT_ASSERT_EQ(pin_local_refs[1], 1);
	pin_local_refs[1] = 0;
	UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
	UT_ASSERT(!out.buffer_pins_pending); NLocBuffer = 0;
}
UT_TEST(backend_collection_rejects_locked_or_changed_owner_without_returning_pin_sample)
{
	ClusterServiceBackendCollection out;
	pin_return_begin(); UT_ASSERT(PinBuffer(GetBufferDescriptor(0), NULL));
	for (unsigned fault = 0; fault < 3; ++fault) {
		memset(&out, 0xa5, sizeof(out));
		if (fault == 0) CritSectionCount = 1;
		if (fault == 1) backend_return_lock_held = true;
		if (fault == 2) MyBackendId++;
		UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED);
		UT_ASSERT(!out.snapshots.known && !out.admission.known && !out.buffer_pins_pending);
		UT_ASSERT_EQ(pin_remembered, 1);
		if (fault == 2) MyBackendId--;
		CritSectionCount = 0; backend_return_lock_held = false;
	}
	UnpinBuffer(GetBufferDescriptor(0)); UT_ASSERT_EQ(pin_remembered, 0);
}
