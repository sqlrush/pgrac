/* Actual held-fd ROOT/pairs, controlled native execution owner and arbiter.
 * A new physical observation does not qualify any origin's native lifecycle.
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned census_owned_reads, census_owned_late_fault;

static bool
census_owned_ready(CensusFixture *f, Oid dbid, uint8 before[32])
{
	census_cold_reset();
	if (!census_batch_begin(f, 2, 2, false))
		return false;
	if (before != NULL)
		UT_ASSERT(cluster_recovery_root_set_digest(f->roots, before));
	census_batch_consuming = true;
	census_native_after_arbitration = census_cold_clear_pair_bytes;
	UT_ASSERT_EQ(cluster_relmap_retention_census_recovery_arbitrate_cold(f->census, dbid),
				 CLUSTER_RELMAP_V2_CLEARED_POSTREAD);
	return true;
}

static void
census_owned_count_read(void *buffer, size_t length, off_t offset)
{
	(void)buffer;
	(void)length;
	(void)offset;
	census_owned_reads++;
}

static void
census_owned_late_change(void *buffer, size_t length, off_t offset)
{
	CensusFixture *f = census_native_fixture;
	census_owned_count_read(buffer, length, offset);
	namespace_read_transform = NULL;
	if (census_owned_late_fault == 1)
		f->origin[1].pins[0].wal_end++;
	if (census_owned_late_fault == 2)
		census_cold_selected.scope_serial++;
	if (census_owned_late_fault == 3)
		census_cold_selected.checkpoint.root_publish_seq++;
	if (census_owned_late_fault == 4)
		census_merger_held = false;
	if (census_owned_late_fault == 5)
		f->origin[1].last_issued++;
	if (census_owned_late_fault == 6)
		census_cold_fault = 8; /* Post-I/O original owner check ERRORs. */
	if (census_owned_late_fault == 7)
		siglongjmp(*PG_exception_stack, 1);
}

UT_TEST(test_census_owned_reobserve_returns_only_new_complete_root_once)
{
	for (unsigned shared = 0; shared < 2; shared++) {
		CensusFixture f;
		ClusterRecoveryRootSet *fresh = NULL, *again = (void *)1;
		ClusterRelmapRetentionDirectory saved[2];
		uint8 before[32], after[32], actual[CLUSTER_RELMAP_V2_FILE_SIZE];
		uint8 expected[CLUSTER_RELMAP_V2_FILE_SIZE];
		Oid dbid = shared ? InvalidOid : 5;
		const char *path
			= shared ? "global/pgrac_relmap_authority.bak" : "base/5/pgrac_relmap_authority.bak";
		unsigned found = 0;
		if (census_owned_ready(&f, dbid, before)) {
			memcpy(saved, f.origin, sizeof(saved));
			census_owned_reads = 0;
			namespace_read_transform = census_owned_count_read;
			UT_ASSERT(
				cluster_relmap_retention_census_reobserve_cold(f.census, 1024 * 1024, &fresh));
			namespace_read_transform = NULL;
			UT_ASSERT(fresh != NULL && fresh != f.roots && census_owned_reads >= 8);
			if (fresh != NULL) {
				UT_ASSERT(cluster_recovery_root_set_digest(fresh, after));
				UT_ASSERT(memcmp(before, after, 32) != 0);
				census_wire(expected, dbid, 0, false, false);
				for (uint32 i = 0; i < cluster_recovery_root_set_count(fresh); i++) {
					const ClusterRecoveryRootEntry *entry
						= cluster_recovery_root_set_entry(fresh, i);
					if (entry != NULL && strcmp(entry->file.path, path) == 0) {
						UT_ASSERT_EQ(
							cluster_recovery_root_set_read(fresh, i, 0, actual, sizeof(actual)),
							CLUSTER_RECOVERY_ROOTS_BYTES);
						UT_ASSERT_EQ(memcmp(actual, expected, sizeof(actual)), 0);
						found++;
					}
				}
				UT_ASSERT_EQ(found, 1);
			}
			UT_ASSERT_EQ(memcmp(saved, f.origin, sizeof(saved)), 0);
			UT_ASSERT(
				!cluster_relmap_retention_census_reobserve_cold(f.census, 1024 * 1024, &again));
			UT_ASSERT(again == NULL);
			UT_ASSERT(census_native_scope_current && census_cold_selected.scope_serial == 73);
			census_refused(&f);
		}
		cluster_recovery_root_set_destroy(fresh);
		census_cold_consume_cleanup(&f);
	}
}

UT_TEST(test_census_owned_reobserve_requires_original_successful_execution_consumer)
{
	for (unsigned mode = 0; mode < 5; mode++) {
		CensusFixture f;
		ClusterRecoveryRootSet *fresh = (void *)1;
		volatile bool threw = false;
		census_cold_reset();
		if (census_native_begin(&f, 5, 2, mode != 0)) {
			if (mode >= 2) {
				census_arbitration_mode = mode == 4 ? 0 : mode - 1;
				census_batch_consuming = mode != 4;
				PG_TRY();
				{
					if (mode == 4)
						UT_ASSERT_EQ(cluster_relmap_retention_census_recovery_arbitrate(
										 f.census, (ClusterWalInputsV1 *)&census_inputs_cookie, 5),
									 CLUSTER_RELMAP_V2_CLEARED_POSTREAD);
					else
						UT_ASSERT_EQ(
							cluster_relmap_retention_census_recovery_arbitrate_cold(f.census, 5),
							CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
				}
				PG_CATCH();
				{
					threw = true;
				}
				PG_END_TRY();
			}
			UT_ASSERT_EQ(threw, mode == 3);
			UT_ASSERT(
				!cluster_relmap_retention_census_reobserve_cold(f.census, 1024 * 1024, &fresh));
			UT_ASSERT(fresh == NULL);
		}
		census_cold_consume_cleanup(&f);
	}
}

UT_TEST(test_census_owned_reobserve_missing_initial_ownership_preserves_follow_on)
{
	for (unsigned gate = 0; gate < 3; gate++) {
		CensusFixture f;
		ClusterRecoveryRootSet *fresh = NULL;
		unsigned checks;
		if (census_owned_ready(&f, 5, NULL)) {
			cluster_shared_catalog = gate != 0;
			census_merger_held = gate != 1;
			census_namespace_x = gate != 2;
			checks = census_cold_checks;
			census_owned_reads = 0;
			namespace_read_transform = census_owned_count_read;
			UT_ASSERT(
				!cluster_relmap_retention_census_reobserve_cold(f.census, 1024 * 1024, &fresh));
			UT_ASSERT(fresh == NULL && census_owned_reads == 0 && census_cold_checks == checks);
			cluster_shared_catalog = census_merger_held = census_namespace_x = true;
			UT_ASSERT(
				cluster_relmap_retention_census_reobserve_cold(f.census, 1024 * 1024, &fresh));
			namespace_read_transform = NULL;
			UT_ASSERT(census_owned_reads >= 8);
		}
		cluster_recovery_root_set_destroy(fresh);
		census_cold_consume_cleanup(&f);
	}
}

UT_TEST(test_census_owned_reobserve_refuses_changed_original_source_metadata_and_all_pins)
{
	for (unsigned fault = 0; fault < 13; fault++) {
		CensusFixture f;
		ClusterRecoveryRootSet *fresh = NULL, *again = NULL;
		ClusterRelmapRetentionDirectory expected[2];
		ClusterRelmapRecoverySourceObservationV1 source;
		if (census_owned_ready(&f, 5, NULL)) {
			source = census_cold_selected;
			if (fault == 1)
				census_cold_selected.source.claim.identity.system_identifier++;
			if (fault == 2)
				census_cold_selected.source.claim.identity.storage_uuid[0] ^= 1;
			if (fault == 3)
				census_cold_selected.source.claim.identity.origin_node_id++;
			if (fault == 4)
				census_cold_selected.checkpoint.root_publish_seq++;
			if (fault == 5)
				census_cold_selected.native_redo++;
			if (fault == 6)
				census_cold_selected.scope_serial++;
			if (fault == 7)
				census_native_scope_current = false;
			if (fault == 8)
				f.origin[1].generation++;
			if (fault == 9)
				f.origin[0].native_floor++;
			if (fault == 10)
				f.origin[0].pins[0].wal_end++;
			if (fault == 11)
				f.origin[1].pins[0].wal_end++;
			if (fault == 12)
				f.origin[1].pins[CLUSTER_RELMAP_RETENTION_CAPACITY - 1].wal_end++;
			memcpy(expected, f.origin, sizeof(expected));
			UT_ASSERT_EQ(
				cluster_relmap_retention_census_reobserve_cold(f.census, 1024 * 1024, &fresh),
				fault == 0);
			UT_ASSERT_EQ(fresh != NULL, fault == 0);
			UT_ASSERT_EQ(memcmp(expected, f.origin, sizeof(expected)), 0);
			census_cold_selected = source;
			census_native_scope_current = true;
			UT_ASSERT(
				!cluster_relmap_retention_census_reobserve_cold(f.census, 1024 * 1024, &again));
			UT_ASSERT(again == NULL);
		}
		cluster_recovery_root_set_destroy(fresh);
		census_cold_consume_cleanup(&f);
	}
}

UT_TEST(test_census_owned_reobserve_rechecks_after_io_and_propagates_late_error)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		CensusFixture f;
		ClusterRecoveryRootSet *volatile fresh = NULL;
		ClusterRelmapRetentionDirectory saved[2];
		volatile bool threw = false;
		if (census_owned_ready(&f, 5, NULL)) {
			memcpy(saved, f.origin, sizeof(saved));
			census_owned_reads = 0;
			census_owned_late_fault = fault;
			namespace_read_transform = census_owned_late_change;
			PG_TRY();
			{
				UT_ASSERT_EQ(cluster_relmap_retention_census_reobserve_cold(
								 f.census, 1024 * 1024, (ClusterRecoveryRootSet **)&fresh),
							 fault == 0);
			}
			PG_CATCH();
			{
				threw = true;
			}
			PG_END_TRY();
			UT_ASSERT_EQ(threw, fault >= 6);
			UT_ASSERT(census_owned_reads > 0);
			UT_ASSERT_EQ(fresh != NULL, fault == 0);
			if (fault == 1)
				saved[1].pins[0].wal_end++;
			census_batch_pins_same(&f, saved);
			UT_ASSERT(fcntl(f.fd, F_GETFD) >= 0);
		}
		cluster_recovery_root_set_destroy((ClusterRecoveryRootSet *)fresh);
		census_cold_consume_cleanup(&f);
	}
}

UT_TEST(test_census_owned_reobserve_root_refusal_and_early_error_keep_protection)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		CensusFixture f;
		ClusterRecoveryRootSet *fresh = NULL, *again = NULL;
		ClusterRelmapRetentionDirectory saved[2];
		volatile bool threw = false;
		int saved_fd = -1;
		if (census_owned_ready(&f, 5, NULL)) {
			memcpy(saved, f.origin, sizeof(saved));
			if (fault == 1)
				inject_pread_fault = 2;
			if (fault == 2)
				UT_ASSERT_EQ(unlinkat(f.fd, "base/5/pgrac_relmap_authority.bak", 0), 0);
			if (fault == 3) {
				saved_fd = dup(f.fd);
				UT_ASSERT(saved_fd >= 0);
				UT_ASSERT_EQ(close(f.fd), 0);
			}
			if (fault == 4)
				census_cold_fault = 8;
			PG_TRY();
			{
				UT_ASSERT(!cluster_relmap_retention_census_reobserve_cold(
					f.census, fault == 0 ? 1 : 1024 * 1024, &fresh));
			}
			PG_CATCH();
			{
				threw = true;
			}
			PG_END_TRY();
			UT_ASSERT_EQ(threw, fault == 4);
			UT_ASSERT(fresh == NULL);
			inject_pread_fault = 0;
			census_cold_fault = 0;
			if (saved_fd >= 0) {
				UT_ASSERT_EQ(dup2(saved_fd, f.fd), f.fd);
				close(saved_fd);
			}
			census_batch_pins_same(&f, saved);
			UT_ASSERT(
				!cluster_relmap_retention_census_reobserve_cold(f.census, 1024 * 1024, &again));
			UT_ASSERT(again == NULL);
		}
		cluster_recovery_root_set_destroy(fresh);
		census_cold_consume_cleanup(&f);
	}
}

UT_TEST(test_census_owned_reobserve_known_aliases_preserve_inputs_and_follow_on)
{
	CensusFixture f;
	ClusterRecoveryRootSet *fresh = NULL;
	ClusterRelmapRetentionDirectory saved[2];
	void *aliases[5];
	if (census_owned_ready(&f, 5, NULL)) {
		memcpy(saved, f.origin, sizeof(saved));
		aliases[0] = f.census;
		aliases[1] = (char *)f.census + sizeof(void *);
		aliases[2] = f.roots;
		aliases[3] = &f.origin[0];
		aliases[4] = &f.origin[1].pins[5];
		for (unsigned i = 0; i < lengthof(aliases); i++) {
			UT_ASSERT(!cluster_relmap_retention_census_reobserve_cold(
				f.census, 1024 * 1024, (ClusterRecoveryRootSet **)aliases[i]));
			UT_ASSERT_EQ(memcmp(saved, f.origin, sizeof(saved)), 0);
		}
		UT_ASSERT(cluster_relmap_retention_census_reobserve_cold(f.census, 1024 * 1024, &fresh));
	}
	cluster_recovery_root_set_destroy(fresh);
	census_cold_consume_cleanup(&f);
}
