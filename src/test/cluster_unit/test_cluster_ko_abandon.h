/* Author: SqlRush <sqlrush@gmail.com> */
/* Original work fixtures; no physical-success or replacement-owner stub. */
/* The identity boundary agrees with this fixture's admitted local boot. */
uint64
cluster_qvotec_get_self_incarnation(void)
{
	return cluster_node_id >= 0 && cluster_node_id < CLUSTER_KO_SHARED_NODE_LIMIT
			   ? formation.membership.last_admitted_incarnation[cluster_node_id]
			   : 0;
}

extern void *cluster_ko_shared_drop_work_abandon_v2(ClusterKoDropWorkV2 *work, Size bytes);
extern bool cluster_ko_shared_drop_work_abandon_next_v2(uint32 *cursor, Size bytes,
														ClusterKoDropWorkV2 **out);

static void
abandon_fixture_process_exit(uint32 slot, const ClusterKoSharedContext *expected)
{
	exit_callback(0, (Datum)0);
	UT_ASSERT_EQ(completion_allocations, 0);
	UT_ASSERT(memcmp(expected, &storage.contexts[slot], sizeof(*expected)) == 0);
}

UT_TEST(test_drop_abandon_preserves_debt_and_permanently_refuses_execution)
{
	ClusterPageWalBindingV1 terminal;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	uint32 slot = prepare_promoted_drop(&terminal, wal), cursor = slot;
	ClusterKoDropWorkV2 *work = NULL;
	ClusterKoSharedContext before;
	uint64 *state, epoch = current_epoch;
	UT_ASSERT(cluster_ko_shared_drop_work_begin_v2(&cursor, 16, &work));
	state = cluster_ko_shared_drop_work_state_v2(work, 16);
	state[0] = 810;
	before = storage.contexts[slot];
	current_epoch++;
	UT_ASSERT(cluster_ko_shared_drop_work_state_v2(work, 16) == NULL);
	UT_ASSERT(cluster_ko_shared_drop_work_abandon_v2(work, 16) == state);
	UT_ASSERT_EQ(state[0], 810);
	state[0] = 0; /* Storage closes the original fd, without claiming completion. */
	current_epoch = epoch;
	UT_ASSERT(!cluster_ko_shared_drop_work_revalidate_v2(work));
	UT_ASSERT(!cluster_ko_shared_drop_work_read_v2(work, &terminal, wal, sizeof(wal)));
	UT_ASSERT(!cluster_ko_shared_drop_work_finish_v2(&work));
	UT_ASSERT(cluster_ko_shared_drop_work_state_v2(work, 16) == NULL);
	UT_ASSERT(cluster_ko_shared_drop_work_abandon_v2(work, 16) == state);
	UT_ASSERT_EQ(state[0], 0);
	UT_ASSERT(memcmp(&before, &storage.contexts[slot], sizeof(before)) == 0);
	UT_ASSERT(storage.contexts[slot].structure_drop_pending);
	abandon_fixture_process_exit(slot, &before);
}

UT_TEST(test_drop_abandon_authenticates_original_pointer_actor_and_size)
{
	ClusterPageWalBindingV1 terminal;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	uint32 slot = prepare_promoted_drop(&terminal, wal), cursor = slot;
	ClusterKoDropWorkV2 *work = NULL;
	ClusterKoSharedContext before;
	ResourceOwner owner = CurrentResourceOwner;
	int pid = MyProcPid;
	UT_ASSERT(cluster_ko_shared_drop_work_begin_v2(&cursor, 16, &work));
	before = storage.contexts[slot];
	UT_ASSERT(cluster_ko_shared_drop_work_abandon_v2((ClusterKoDropWorkV2 *)1, 16) == NULL);
	UT_ASSERT(cluster_ko_shared_drop_work_abandon_v2(NULL, 16) == NULL);
	UT_ASSERT(cluster_ko_shared_drop_work_abandon_v2(work, 8) == NULL);
	for (unsigned wrong = 0; wrong < 4; wrong++) {
		CurrentResourceOwner = wrong == 0 ? (ResourceOwner)2 : owner;
		MyProcPid = wrong == 1 ? pid + 1 : pid;
		MyBackendType = wrong == 2 ? B_BG_WRITER : B_CHECKPOINTER;
		CritSectionCount = wrong == 3 ? 1 : 0;
		UT_ASSERT(cluster_ko_shared_drop_work_abandon_v2(work, 16) == NULL);
	}
	CurrentResourceOwner = owner;
	MyProcPid = pid;
	MyBackendType = B_CHECKPOINTER;
	CritSectionCount = 0;
	UT_ASSERT(cluster_ko_shared_drop_work_revalidate_v2(work));
	UT_ASSERT(memcmp(&before, &storage.contexts[slot], sizeof(before)) == 0);
	/* Storage may explicitly abandon an irreparable namespace with a live cut. */
	UT_ASSERT(cluster_ko_shared_drop_work_abandon_v2(work, 16) != NULL);
	UT_ASSERT(!cluster_ko_shared_drop_work_revalidate_v2(work));
	abandon_fixture_process_exit(slot, &before);
}

UT_TEST(test_drop_abandon_scan_keeps_transient_wait_retryable)
{
	ClusterPageWalBindingV1 terminal;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	uint32 slot = prepare_promoted_drop(&terminal, wal), cursor = slot;
	ClusterKoDropWorkV2 *work = NULL, *selected = NULL;
	ClusterKoSharedContext before;
	UT_ASSERT(cluster_ko_shared_drop_work_begin_v2(&cursor, 16, &work));
	before = storage.contexts[slot];
	cursor = slot;
	UT_ASSERT(!cluster_ko_shared_drop_work_abandon_next_v2(&cursor, 16, &selected));
	UT_ASSERT_EQ(cursor, slot);
	UT_ASSERT(selected == NULL);
	capture_ok = false;
	UT_ASSERT(!cluster_ko_shared_drop_work_revalidate_v2(work));
	UT_ASSERT(!cluster_ko_shared_drop_work_abandon_next_v2(&cursor, 16, &selected));
	capture_ok = true;
	UT_ASSERT(cluster_ko_shared_drop_work_revalidate_v2(work));
	current_epoch++;
	UT_ASSERT(cluster_ko_shared_drop_work_abandon_next_v2(&cursor, 16, &selected));
	UT_ASSERT(selected == work);
	UT_ASSERT_EQ(cursor, slot + 1);
	UT_ASSERT(cluster_ko_shared_drop_work_abandon_v2(selected, 16) != NULL);
	UT_ASSERT(memcmp(&before, &storage.contexts[slot], sizeof(before)) == 0);
	abandon_fixture_process_exit(slot, &before);
}

UT_TEST(test_drop_abandon_scan_requires_original_executor_of_replaced_context)
{
	ClusterPageWalBindingV1 terminal;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	uint32 slot = prepare_promoted_drop(&terminal, wal), cursor = slot;
	ClusterKoDropWorkV2 *work = NULL, *selected = NULL;
	ClusterKoSharedContext before;
	int pid = MyProcPid;
	UT_ASSERT(cluster_ko_shared_drop_work_begin_v2(&cursor, 16, &work));
	storage.contexts[slot].serial++;
	before = storage.contexts[slot];
	cursor = slot;
	MyProcPid++;
	UT_ASSERT(!cluster_ko_shared_drop_work_abandon_next_v2(&cursor, 16, &selected));
	MyProcPid = pid;
	UT_ASSERT(!cluster_ko_shared_drop_work_abandon_next_v2(&cursor, 8, &selected));
	UT_ASSERT(cluster_ko_shared_drop_work_abandon_next_v2(&cursor, 16, &selected));
	UT_ASSERT(selected == work);
	UT_ASSERT(memcmp(&before, &storage.contexts[slot], sizeof(before)) == 0);
	abandon_fixture_process_exit(slot, &before);
}
