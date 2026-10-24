/* Actual physical census/ROOT/pairs with controlled original native arbiter
 * and scope leaves. No live execution or directory completion qualification.
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned census_cold_post_fault;

static void
census_cold_post_native(void)
{
	CensusFixture *f = census_native_fixture;
	if (census_cold_post_fault == 1)
		census_native_mutated_pin
			= (ClusterRelmapRetentionPin *)census_pin(&f->origin[1], InvalidOid);
	if (census_cold_post_fault == 2)
		census_native_mutated_pin = &f->origin[1].pins[CLUSTER_RELMAP_RETENTION_CAPACITY - 1];
	if (census_native_mutated_pin != NULL)
		census_native_mutated_pin->wal_end++;
	if (census_cold_post_fault == 3)
		census_cold_selected.checkpoint.root_publish_seq++;
	if (census_cold_post_fault == 4)
		census_cold_selected.scope_serial++;
	if (census_cold_post_fault == 5)
		census_cold_fault = 8; /* The post-native original source check ERRORs. */
}

static void
census_cold_consume_cleanup(CensusFixture *f)
{
	census_native_after_arbitration = NULL;
	census_cold_cleanup(f);
}

static void
census_cold_changed_pin_expected(CensusFixture *f, ClusterRelmapRetentionDirectory saved[2])
{
	for (unsigned o = 0; o < 2; o++)
		for (unsigned i = 0; i < CLUSTER_RELMAP_RETENTION_CAPACITY; i++)
			if (census_native_mutated_pin == &f->origin[o].pins[i])
				saved[o].pins[i].wal_end++;
}

UT_TEST(test_census_cold_consumer_invalidates_before_original_native_attempt)
{
	for (unsigned shared = 0; shared < 2; shared++)
		for (unsigned cut = 0; cut < 3; cut++) {
			CensusFixture f;
			ClusterRelmapRetentionDirectory saved[2];
			Oid dbid = shared ? InvalidOid : 5;
			census_cold_reset();
			if (census_native_begin(&f, dbid, cut, true)) {
				memcpy(saved, f.origin, sizeof(saved));
				/* Any attempt to use the old pass-1 read scope ERRORs. The
				 * native arbiter verifies physical ROOT/directories revoked
				 * before entry, while original native authority remains. */
				census_native_scope_error = census_batch_consuming = true;
				UT_ASSERT_EQ(
					cluster_relmap_retention_census_recovery_arbitrate_cold(f.census, dbid),
					CLUSTER_RELMAP_V2_CLEARED_POSTREAD);
				UT_ASSERT_EQ(census_native_identity.dbid, dbid);
				UT_ASSERT_EQ(census_native_pending.pending_committed_generation, 1);
				UT_ASSERT_EQ(census_native_pending.committed_generation, cut == 1 ? 2 : 1);
				UT_ASSERT(census_arbitrations == 1 && census_native_prepares == 1
						  && census_native_releases == 1 && !census_native_handles[0].live);
				UT_ASSERT(census_native_scope_current && census_cold_selected.scope_serial == 73);
				census_batch_pins_same(&f, saved);
				census_refused(&f);
			}
			census_cold_consume_cleanup(&f);
		}
}

UT_TEST(test_census_cold_consumer_missing_initial_ownership_keeps_scan_unconsumed)
{
	CensusFixture f;
	census_cold_reset();
	UT_ASSERT_EQ(cluster_relmap_retention_census_recovery_arbitrate_cold(NULL, 5),
				 CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
	if (census_native_begin(&f, 5, 2, false)) {
		UT_ASSERT_EQ(cluster_relmap_retention_census_recovery_arbitrate_cold(f.census, 5),
					 CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
		UT_ASSERT_EQ(census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
		for (unsigned gate = 0; gate < 3; gate++) {
			uint8 digest[32];
			cluster_shared_catalog = gate != 0;
			census_merger_held = gate != 1;
			census_namespace_x = gate != 2;
			UT_ASSERT_EQ(cluster_relmap_retention_census_recovery_arbitrate_cold(f.census, 5),
						 CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
			UT_ASSERT(cluster_recovery_root_set_digest(f.roots, digest));
			UT_ASSERT_EQ(f.origin[0].state, CLUSTER_RELMAP_RETENTION_BUILDING);
			UT_ASSERT_EQ(census_native_prepares + census_arbitrations, 0);
		}
		cluster_shared_catalog = census_merger_held = census_namespace_x = true;
		census_batch_consuming = true;
		UT_ASSERT_EQ(cluster_relmap_retention_census_recovery_arbitrate_cold(f.census, 5),
					 CLUSTER_RELMAP_V2_CLEARED_POSTREAD);
	}
	census_cold_consume_cleanup(&f);
}

UT_TEST(test_census_cold_consumer_pre_native_refusal_error_never_attempts_target)
{
	const unsigned native_faults[] = { 1, 2, 0, 0, 0 };
	const unsigned cold_faults[] = { 0, 0, 1, 2, 5 };
	for (unsigned i = 0; i < lengthof(native_faults); i++) {
		CensusFixture f;
		ClusterRelmapRetentionDirectory saved[2];
		volatile bool threw = false;
		census_cold_reset();
		if (census_batch_begin(&f, 2, 2, false)) {
			memcpy(saved, f.origin, sizeof(saved));
			census_batch_consuming = true;
			census_native_fault = native_faults[i];
			census_cold_fault = cold_faults[i];
			census_cold_getter_fault_after = i >= 3 ? 3 : 1;
			PG_TRY();
			{
				UT_ASSERT_EQ(cluster_relmap_retention_census_recovery_arbitrate_cold(f.census, 5),
							 CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
			}
			PG_CATCH();
			{
				threw = true;
			}
			PG_END_TRY();
			UT_ASSERT_EQ(threw, i == 1 || i == 3);
			UT_ASSERT_EQ(census_native_prepares, 1);
			UT_ASSERT_EQ(census_arbitrations, 0);
			UT_ASSERT_EQ(census_native_releases, i < 2 ? 0 : 1);
			UT_ASSERT(!census_native_handles[0].live);
			census_batch_pins_same(&f, saved);
			census_refused(&f);
		}
		census_cold_consume_cleanup(&f);
	}
}

UT_TEST(test_census_cold_consumer_native_unknown_error_preserve_original_debt)
{
	for (unsigned mode = 1; mode <= 2; mode++) {
		CensusFixture f;
		ClusterRelmapRetentionDirectory saved[2];
		volatile bool threw = false;
		census_cold_reset();
		if (census_batch_begin(&f, 2, 2, false)) {
			memcpy(saved, f.origin, sizeof(saved));
			census_batch_consuming = true;
			census_arbitration_mode = mode;
			PG_TRY();
			{
				UT_ASSERT_EQ(cluster_relmap_retention_census_recovery_arbitrate_cold(f.census, 5),
							 CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
			}
			PG_CATCH();
			{
				threw = true;
			}
			PG_END_TRY();
			UT_ASSERT_EQ(threw, mode == 2);
			UT_ASSERT(census_arbitrations == 1 && census_native_releases == 1);
			UT_ASSERT(census_native_scope_current && !census_native_handles[0].live);
			census_batch_pins_same(&f, saved);
			census_refused(&f);
		}
		census_cold_consume_cleanup(&f);
	}
}

UT_TEST(test_census_cold_consumer_post_native_source_metadata_pins_and_ownership_drift)
{
	for (unsigned fault = 1; fault <= 11; fault++) {
		CensusFixture f;
		ClusterRelmapRetentionDirectory saved[2];
		census_cold_reset();
		if (census_batch_begin(&f, 2, 2, false)) {
			memcpy(saved, f.origin, sizeof(saved));
			census_batch_consuming = true;
			if (fault <= 7)
				census_arbitration_drift = fault;
			else {
				census_cold_post_fault = fault - 7;
				census_native_after_arbitration = census_cold_post_native;
			}
			UT_ASSERT_EQ(cluster_relmap_retention_census_recovery_arbitrate_cold(f.census, 5),
						 CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
			UT_ASSERT(census_arbitrations == 1 && census_native_releases == 1);
			census_cold_changed_pin_expected(&f, saved);
			census_batch_pins_same(&f, saved);
			census_refused(&f);
		}
		census_cold_consume_cleanup(&f);
	}
}

UT_TEST(test_census_cold_consumer_post_native_error_cannot_restore_observations)
{
	CensusFixture f;
	ClusterRelmapRetentionDirectory saved[2];
	volatile bool threw = false;
	census_cold_reset();
	if (census_batch_begin(&f, 2, 2, false)) {
		memcpy(saved, f.origin, sizeof(saved));
		census_batch_consuming = true;
		census_cold_post_fault = 5;
		census_native_after_arbitration = census_cold_post_native;
		PG_TRY();
		{
			(void)cluster_relmap_retention_census_recovery_arbitrate_cold(f.census, 5);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		UT_ASSERT(threw && census_arbitrations == 1 && census_native_releases == 1);
		UT_ASSERT(census_native_scope_current && !census_native_handles[0].live);
		census_batch_pins_same(&f, saved);
		census_refused(&f);
	}
	census_cold_consume_cleanup(&f);
}

UT_TEST(test_census_cold_consumer_cannot_reuse_old_observations_or_infer_empty_completion)
{
	for (unsigned clean = 0; clean < 2; clean++) {
		CensusFixture f;
		ClusterRelmapRetentionDirectory saved[2];
		census_cold_reset();
		if (census_native_begin(&f, 5, 2, true)) {
			memcpy(saved, f.origin, sizeof(saved));
			census_batch_consuming = true;
			UT_ASSERT_EQ(
				cluster_relmap_retention_census_recovery_arbitrate_cold(f.census,
																		clean ? InvalidOid : 5),
				clean ? CLUSTER_RELMAP_V2_RETAINED_UNKNOWN : CLUSTER_RELMAP_V2_CLEARED_POSTREAD);
			UT_ASSERT_EQ(cluster_relmap_retention_census_recovery_arbitrate_cold(f.census, 5),
						 CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
			UT_ASSERT_EQ(census_native_prepares, clean ? 0 : 1);
			UT_ASSERT_EQ(census_arbitrations, clean ? 0 : 1);
			census_batch_pins_same(&f, saved);
			census_refused(&f);
		}
		census_cold_consume_cleanup(&f);
	}
}

/* Controlled native leaf changes real pair bytes. This is not native
 * durability/terminal qualification; the physical reader itself is real. */
static void
census_cold_clear_pair_bytes(void)
{
	census_pair(census_native_fixture->fd, census_native_identity.dbid, 0, false, false, false);
}

UT_TEST(test_census_cold_consumer_then_fresh_original_root_keeps_all_debt)
{
	for (unsigned shared = 0; shared < 2; shared++) {
		CensusFixture f;
		ClusterRecoveryRootSet *fresh = NULL;
		ClusterRelmapRetentionDirectory saved[2];
		Oid dbid = shared ? InvalidOid : 5;
		uint8 before[32], after[32];
		size_t bytes[2] = { 0 };
		census_cold_reset();
		if (census_native_begin(&f, dbid, 2, true)) {
			memcpy(saved, f.origin, sizeof(saved));
			UT_ASSERT(cluster_recovery_root_set_digest(f.roots, before));
			census_batch_consuming = true;
			census_native_after_arbitration = census_cold_clear_pair_bytes;
			UT_ASSERT_EQ(cluster_relmap_retention_census_recovery_arbitrate_cold(f.census, dbid),
						 CLUSTER_RELMAP_V2_CLEARED_POSTREAD);
			census_refused(&f);
			UT_ASSERT_EQ(cluster_recovery_root_set_reobserve_begin(f.roots, 1024 * 1024, &fresh),
						 CLUSTER_RECOVERY_ROOTS_MORE);
			if (fresh != NULL) {
				unsigned copies = 0;
				uint8 expected[CLUSTER_RELMAP_V2_FILE_SIZE], actual[CLUSTER_RELMAP_V2_FILE_SIZE];
				const char *primary
					= shared ? "global/pgrac_relmap_authority" : "base/5/pgrac_relmap_authority";
				UT_ASSERT_EQ(finish_roots(fresh, bytes), CLUSTER_RECOVERY_ROOTS_OBSERVED);
				UT_ASSERT(bytes[0] == bytes[1] && bytes[0] >= 4 * sizeof(expected));
				UT_ASSERT(cluster_recovery_root_set_digest(fresh, after));
				UT_ASSERT(memcmp(before, after, 32) != 0);
				census_wire(expected, dbid, 0, false, false);
				for (uint32 i = 0; i < cluster_recovery_root_set_count(fresh); i++) {
					const ClusterRecoveryRootEntry *entry
						= cluster_recovery_root_set_entry(fresh, i);
					if (entry != NULL
						&& (strcmp(entry->file.path, primary) == 0
							|| (strncmp(entry->file.path, primary, strlen(primary)) == 0
								&& strcmp(entry->file.path + strlen(primary), ".bak") == 0))) {
						UT_ASSERT_EQ(
							cluster_recovery_root_set_read(fresh, i, 0, actual, sizeof(actual)),
							CLUSTER_RECOVERY_ROOTS_BYTES);
						UT_ASSERT_EQ(memcmp(expected, actual, sizeof(expected)), 0);
						copies++;
					}
				}
				UT_ASSERT_EQ(copies, 2);
			}
			census_batch_pins_same(&f, saved);
			census_refused(&f);
			UT_ASSERT(census_native_scope_current && census_cold_selected.scope_serial == 73);
		}
		cluster_recovery_root_set_destroy(fresh);
		census_cold_consume_cleanup(&f);
	}
}
