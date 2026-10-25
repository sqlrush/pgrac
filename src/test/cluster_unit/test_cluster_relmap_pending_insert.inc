/* PGRAC: original dormant insert/flush/stage algorithm, controlled native WAL
 * and pair-storage leaves. No native durability/transaction qualification.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
static unsigned relmap_pending_step, relmap_pending_registers;
static unsigned relmap_pending_begins, relmap_pending_inserts, relmap_pending_flushes;
static unsigned relmap_pending_fault;
static bool relmap_pending_shared_lock;
static uint64 relmap_pending_current;
static XLogRecPtr relmap_pending_actual_end;
static uint8 relmap_pending_payload[MinSizeOfRelmapUpdate + sizeof(RelMapFile)];

static bool
relmap_pending_lock_mode(LWLock *lock, LWLockMode mode)
{
	if (lock == RelationMappingLock)
		return relmap_local_lock && !relmap_pending_shared_lock && mode == LW_EXCLUSIVE;
	return LWLockHeldByMeInMode(lock, mode);
}

static FullTransactionId
relmap_pending_current_xid(void)
{
	if (relmap_retention_held || CritSectionCount != 0)
		abort();
	return FullTransactionIdFromU64(relmap_pending_current);
}

static void
relmap_pending_native_context(void)
{
	if (relmap_retention_held || !relmap_local_lock || relmap_pending_shared_lock
		|| CritSectionCount != 1 || relmap_runtime_stage_calls != 0)
		abort();
}

static void
relmap_pending_begin(void)
{
	relmap_pending_native_context();
	if (relmap_pending_step != 0)
		abort();
	relmap_pending_begins++;
	relmap_pending_step = 1;
}

static void
relmap_pending_register(char *data, uint32 bytes)
{
	size_t offset = relmap_pending_registers == 0 ? 0 : MinSizeOfRelmapUpdate;
	size_t expected = relmap_pending_registers == 0 ? MinSizeOfRelmapUpdate : sizeof(RelMapFile);

	relmap_pending_native_context();
	if (relmap_pending_registers >= 2 || relmap_pending_step != 1 + relmap_pending_registers
		|| bytes != expected)
		abort();
	memcpy(relmap_pending_payload + offset, data, bytes);
	relmap_pending_registers++;
	relmap_pending_step++;
}

static XLogRecPtr
relmap_pending_insert(RmgrId rmid, uint8 info)
{
	relmap_pending_native_context();
	if (relmap_pending_step != 3 || rmid != RM_RELMAP_ID || info != XLOG_RELMAP_UPDATE)
		abort();
	relmap_pending_step++;
	relmap_pending_inserts++;
	if (relmap_pending_fault == 1) epoch++;
	if (relmap_pending_fault == 3) return InvalidXLogRecPtr;
	if (relmap_pending_fault == 4) pg_re_throw();
	return relmap_pending_actual_end;
}

static void
relmap_pending_flush(XLogRecPtr end)
{
	relmap_pending_native_context();
	if (relmap_pending_step != 4 || end != relmap_pending_actual_end)
		abort();
	relmap_pending_step++;
	relmap_pending_flushes++;
	if (relmap_pending_fault == 2) epoch++;
	if (relmap_pending_fault == 5) pg_re_throw();
}

static void
relmap_pending_fixture(ClusterRelmapOwnerSampleV2 *sample,
	ClusterRelmapV2Namespace *identity, RelMapFile *map, unsigned origin, bool shared)
{
	relmap_runtime_prepare_stage(sample, identity, origin, shared);
	relmap_local_lock = relmap_stage_lock_window = true;
	relmap_pending_shared_lock = false;
	relmap_pending_tablespace = DEFAULTTABLESPACE_OID;
	relmap_pending_current = relmap_top_full;
	relmap_pending_actual_end = UINT64CONST(0x123450);
	relmap_pending_step = relmap_pending_registers = relmap_pending_begins = 0;
	relmap_pending_inserts = relmap_pending_flushes = relmap_pending_fault = 0;
	memset(relmap_pending_payload, 0, sizeof(relmap_pending_payload));
	memset(map, 0, sizeof(*map));
	map->magic = RELMAPPER_FILEMAGIC;
	map->num_mappings = 1;
	map->mappings[0].mapoid = 1259;
	map->mappings[0].mapfilenumber = 9000;
	relmap_wal_map_crc(map);
}

UT_TEST(test_relmap_pending_insert_uses_actual_record_end_and_native_payload)
{
	for (unsigned origin = 0; origin < CLUSTER_MAX_NODES; origin += CLUSTER_MAX_NODES - 1)
		for (unsigned shared = 0; shared < 2; shared++) {
			ClusterRelmapOwnerSampleV2 sample, before;
			ClusterRelmapV2Namespace identity;
			RelMapFile map;
			xl_relmap_update update;
			ClusterRelmapRetentionPin pin;

			relmap_pending_fixture(&sample, &identity, &map, origin, shared);
			before = sample;
			UT_ASSERT_EQ(cluster_relmap_retention_runtime_insert_pending_v2(&sample, &identity, 8, &map),
				relmap_pending_actual_end);
			memcpy(&update, relmap_pending_payload, MinSizeOfRelmapUpdate);
			UT_ASSERT_EQ(update.dbid, shared ? InvalidOid : MyDatabaseId);
			UT_ASSERT_EQ(update.tsid, shared ? GLOBALTABLESPACE_OID : DEFAULTTABLESPACE_OID);
			UT_ASSERT_EQ(update.nbytes, sizeof(map));
			UT_ASSERT_EQ(memcmp(relmap_pending_payload + MinSizeOfRelmapUpdate, &map, sizeof(map)), 0);
			UT_ASSERT_EQ(relmap_pending_begins, 1);
			UT_ASSERT_EQ(relmap_pending_inserts, 1);
			UT_ASSERT_EQ(relmap_pending_flushes, 1);
			UT_ASSERT_EQ(relmap_pending_step, 5);
			UT_ASSERT_EQ(relmap_runtime_stage_calls, 1);
			UT_ASSERT_EQ(relmap_runtime_stage_writes, 2);
			UT_ASSERT_EQ(relmap_runtime_stage_owner.wal_end, relmap_pending_actual_end);
			UT_ASSERT_EQ(memcmp(&relmap_runtime_stage_map, &map, sizeof(map)), 0);
			UT_ASSERT_EQ(memcmp(&sample, &before, sizeof(sample)), 0);
			pin = relmap_runtime_shmem->directories[origin].pins[0];
			UT_ASSERT(pin.occupied && pin.may_be_staged && pin.wal_end == relmap_pending_actual_end);
			UT_ASSERT_EQ(relmap_assign_calls, 1);
			UT_ASSERT_EQ(relmap_binding_reads, 1);
			UT_ASSERT_EQ(CritSectionCount, 0);
			UT_ASSERT(!relmap_retention_held && relmap_local_lock);
		}
}

UT_TEST(test_relmap_pending_insert_refuses_before_wal_without_original_preparation)
{
	for (unsigned fault = 0; fault < 36; fault++) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapV2Namespace identity;
		ClusterRelmapRetentionDirectory before;
		ClusterRelmapRetentionDirectory *directory;
		RelMapFile map;
		uint64 generation = 8;
		const RelMapFile *input = &map;
		const ClusterRelmapOwnerSampleV2 *owner = &sample;
		const ClusterRelmapV2Namespace *space = &identity;

		relmap_pending_fixture(&sample, &identity, &map, 0, false);
		directory = &relmap_runtime_shmem->directories[0];
		if (fault == 0) relmap_runtime_shmem->directories[0].state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
		if (fault == 1) relmap_runtime_shmem->directories[0].state = CLUSTER_RELMAP_RETENTION_BUILDING;
		if (fault == 2) relmap_runtime_shmem->directories[0].pins[0].occupied = false;
		if (fault == 3) relmap_runtime_shmem->directories[0].pins[0].key.generation++;
		if (fault == 4) relmap_runtime_shmem->directories[0].pins[0].may_be_staged = true;
		if (fault == 5) relmap_runtime_shmem->directories[0].pins[0].wal_end = 456;
		if (fault == 6) relmap_local_lock = false;
		if (fault == 7) relmap_pending_shared_lock = true;
		if (fault == 8) relmap_xidgen_held = true;
		if (fault == 9) relmap_pending_current = 0;
		if (fault == 10) relmap_pending_current += UINT64CONST(0x100000000);
		if (fault == 11) map.crc++;
		if (fault == 12) { map.magic++; relmap_wal_map_crc(&map); }
		if (fault == 13) generation = 0;
		if (fault == 14) generation = 1;
		if (fault == 15) identity.dbid++;
		if (fault == 16) identity.system_identifier++;
		if (fault == 17) identity.database_incarnation++;
		if (fault == 18) identity.shared = true;
		if (fault == 19) relmap_pending_tablespace = InvalidOid;
		if (fault == 20) relmap_pending_tablespace = GLOBALTABLESPACE_OID;
		if (fault == 21) owner = NULL;
		if (fault == 22) space = NULL;
		if (fault == 23) input = NULL;
		if (fault == 24) sample.producer_pid++;
		if (fault == 25) sample.owner.top_full_xid += UINT64CONST(0x100000000);
		if (fault == 26) relmap_runtime_shmem->directories[0].native_floor = relmap_top_full + 1;
		if (fault == 27) relmap_runtime_shmem->directories[0].last_issued = relmap_top_full - 1;
		if (fault == 28) relmap_runtime_shmem->directories[0].origin++;
		if (fault == 29) relmap_runtime_shmem->directories[0].system_identifier++;
		if (fault == 30) relmap_runtime_shmem = NULL;
		if (fault == 31) relmap_retention_held = true;
		if (fault == 32) CritSectionCount = 1;
		if (fault == 33) ShutdownRequestPending = true;
		if (fault == 34) relmap_global_x = false;
		if (fault == 35) relmap_in_transaction = false;
		before = *directory;
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_insert_pending_v2(owner, space, generation, input),
			InvalidXLogRecPtr);
		UT_ASSERT_EQ(memcmp(&before, directory, sizeof(before)), 0);
		UT_ASSERT_EQ(relmap_pending_begins + relmap_pending_inserts + relmap_pending_flushes, 0);
		UT_ASSERT_EQ(relmap_runtime_stage_calls, 0);
		UT_ASSERT_EQ(CritSectionCount, fault == 32 ? 1 : 0);
	}
}

UT_TEST(test_relmap_pending_insert_late_refusal_retains_original_obligations)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapV2Namespace identity;
		RelMapFile map;
		ClusterRelmapRetentionPin before, after;

		relmap_pending_fixture(&sample, &identity, &map, 0, true);
		before = relmap_runtime_shmem->directories[0].pins[0];
		if (fault < 3) relmap_pending_fault = fault + 1;
		else relmap_runtime_stage_mode = fault - 2;
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_insert_pending_v2(&sample, &identity, 8, &map),
			InvalidXLogRecPtr);
		after = relmap_runtime_shmem->directories[0].pins[0];
		UT_ASSERT(after.occupied);
		UT_ASSERT_EQ(memcmp(&before.key, &after.key, sizeof(before.key)), 0);
		UT_ASSERT_EQ(after.has_tt_binding, before.has_tt_binding);
		UT_ASSERT_EQ(relmap_pending_inserts, 1);
		UT_ASSERT_EQ(relmap_pending_flushes, fault == 2 ? 0 : 1);
		UT_ASSERT_EQ(relmap_runtime_stage_calls, fault < 3 ? 0 : 1);
		UT_ASSERT_EQ(after.may_be_staged, fault >= 4);
		UT_ASSERT_EQ(after.wal_end, fault >= 4 ? relmap_pending_actual_end : 0);
		UT_ASSERT_EQ(CritSectionCount, 0);
	}
	{
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapV2Namespace identity;
		RelMapFile map;
		ClusterRelmapRetentionDirectory before;

		relmap_pending_fixture(&sample, &identity, &map, 0, true);
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_insert_pending_v2(&sample, &identity, 8, &map),
			relmap_pending_actual_end);
		before = relmap_runtime_shmem->directories[0];
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_insert_pending_v2(&sample, &identity, 8, &map),
			InvalidXLogRecPtr);
		UT_ASSERT_EQ(memcmp(&before, &relmap_runtime_shmem->directories[0], sizeof(before)), 0);
		UT_ASSERT_EQ(relmap_pending_inserts, 1);
	}
}

UT_TEST(test_relmap_pending_insert_critical_error_propagates_without_local_release)
{
	for (unsigned fault = 4; fault <= 5; fault++) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapV2Namespace identity;
		ClusterRelmapRetentionDirectory before;
		RelMapFile map;
		volatile bool caught = false;

		relmap_pending_fixture(&sample, &identity, &map, 0, true);
		before = relmap_runtime_shmem->directories[0];
		relmap_pending_fault = fault;
		PG_TRY();
		{
			(void) cluster_relmap_retention_runtime_insert_pending_v2(&sample, &identity, 8, &map);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(CritSectionCount, 1);
		UT_ASSERT_EQ(memcmp(&before, &relmap_runtime_shmem->directories[0], sizeof(before)), 0);
		UT_ASSERT_EQ(relmap_runtime_stage_calls, 0);
		UT_ASSERT_EQ(relmap_pending_inserts, 1);
		UT_ASSERT(relmap_local_lock && !relmap_retention_held);
		/* Controlled leaf longjmp only; a real native critical ERROR is fail-stop.
		 * This fixture's caller simulates abort teardown, not producer cleanup. */
		CritSectionCount = 0;
	}
}
