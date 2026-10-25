/* Actual finished ROOT/pairs/census; original checkpoint/native leaves remain
 * controlled. Copied committed images grant no completion or release.
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned census_committed_reads, census_committed_fault, census_committed_at;

static bool
census_committed_drain(ClusterRelmapRetentionCensus *census)
{
	ClusterRelmapCensusResult result = CLUSTER_RELMAP_CENSUS_MORE;
	for (unsigned i = 0; i < 100000 && result == CLUSTER_RELMAP_CENSUS_MORE; i++)
		result = cluster_relmap_retention_census_next(census);
	return result == CLUSTER_RELMAP_CENSUS_DRAINED;
}

static bool
census_committed_zero(const ClusterRelmapCensusCommitted *out)
{
	ClusterRelmapCensusCommitted zero = { 0 };
	return memcmp(out, &zero, sizeof(zero)) == 0;
}

static void
census_committed_read_fault(void *buffer, size_t length, off_t offset)
{
	(void)offset;
	if (++census_committed_reads != census_committed_at)
		return;
	namespace_read_transform = NULL;
	if (census_committed_fault == 1)
		census_cold_selected.scope_serial++;
	if (census_committed_fault == 2)
		census_cold_selected.checkpoint.root_publish_seq++;
	if (census_committed_fault == 3)
		census_cold_selected.native_redo++;
	if (census_committed_fault == 4)
		census_namespace_x = false;
	if (census_committed_fault == 5)
		census_initial_directory->pins[CLUSTER_RELMAP_RETENTION_CAPACITY - 1].wal_end++;
	if (census_committed_fault == 6)
		census_initial_directory->last_issued++;
	if (census_committed_fault == 7)
		census_cold_fault = 8;
	if (census_committed_fault == 8)
		siglongjmp(*PG_exception_stack, 1);
	if (census_committed_fault == 9) {
		uint8 bytes[CLUSTER_RELMAP_V2_FILE_SIZE];
		UT_ASSERT_EQ(length, sizeof(bytes));
		census_wire(bytes, 5, 0, false, true);
		memcpy(buffer, bytes, sizeof(bytes)); /* Valid CRC, wrong full hash. */
	}
}

UT_TEST(test_cold_committed_reads_clean_and_post_clear_exact_namespaces)
{
	for (unsigned pending = 0; pending < 2; pending++) {
		CensusFixture f;
		ClusterRelmapRetentionCensus *census = NULL;
		ClusterRelmapRetentionDirectory saved[2];
		census_execute_setup(&f, pending);
		memcpy(saved, f.origin, sizeof(saved));
		UT_ASSERT(census_execute_run(&census));
		for (unsigned ns = 0; ns < 3; ns++) {
			ClusterRelmapCensusCommitted out;
			Oid dbid = ns == 1 ? 5 : InvalidOid;
			unsigned native = census_native_prepares + census_arbitrations;
			memset(&out, 0xa5, sizeof(out));
			census_committed_reads = census_committed_at = 0;
			namespace_read_transform = census_committed_read_fault;
			UT_ASSERT(cluster_relmap_retention_census_committed_cold(census, dbid, &out));
			namespace_read_transform = NULL;
			UT_ASSERT(out.present && out.identity.system_identifier == 817
					  && out.identity.database_incarnation == 91 && out.identity.dbid == dbid
					  && out.identity.shared == (dbid == InvalidOid));
			UT_ASSERT_EQ(out.generation, 1);
			UT_ASSERT(out.map.magic == RELMAPPER_FILEMAGIC && out.map.num_mappings == 1);
			UT_ASSERT_EQ(out.map.mappings[0].mapfilenumber, 40000);
			UT_ASSERT(census_committed_reads > 2); /* Pair plus whole ROOT bytes. */
			UT_ASSERT_EQ(census_native_prepares + census_arbitrations, native);
			UT_ASSERT_EQ(cluster_relmap_retention_census_next(census),
						 CLUSTER_RELMAP_CENSUS_DRAINED);
		}
		census_execute_done(&f, census, saved);
	}
}

UT_TEST(test_cold_committed_initial_gates_preserve_unfinished_and_original_custody)
{
	ClusterRelmapCensusCommitted out;
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!cluster_relmap_retention_census_committed_cold(NULL, 5, &out));
	UT_ASSERT(census_committed_zero(&out));
	for (unsigned fault = 0; fault < 8; fault++) {
		CensusFixture f;
		ClusterRelmapRetentionCensus *census = NULL;
		ClusterRelmapRetentionDirectory saved[2];
		census_execute_setup(&f, false);
		memcpy(saved, f.origin, sizeof(saved));
		if (fault == 0)
			UT_ASSERT(cluster_relmap_retention_census_open_cold(3, 1024 * 1024, CENSUS_BUDGET,
																1024 * 1024, &census));
		else
			UT_ASSERT(census_execute_run(&census));
		if (fault == 1)
			MyBackendType = B_BG_WORKER;
		if (fault == 2)
			census_execute_recovery = false;
		if (fault == 3)
			CritSectionCount = 1;
		if (fault == 4)
			cluster_shared_catalog = false;
		if (fault == 5)
			census_merger_held = false;
		if (fault == 6)
			census_namespace_x = false;
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT(
			!cluster_relmap_retention_census_committed_cold(census, 5, fault == 7 ? NULL : &out));
		if (fault != 7)
			UT_ASSERT(census_committed_zero(&out));
		MyBackendType = B_STARTUP;
		census_execute_recovery = cluster_shared_catalog = census_merger_held = census_namespace_x
			= true;
		CritSectionCount = 0;
		UT_ASSERT(census_committed_drain(census));
		UT_ASSERT(cluster_relmap_retention_census_committed_cold(census, 5, &out));
		census_execute_done(&f, census, saved);
	}
}

UT_TEST(test_cold_committed_pending_and_missing_namespace_never_mean_empty)
{
	for (unsigned pending = 0; pending < 2; pending++) {
		CensusFixture f;
		ClusterRelmapRetentionCensus *census = NULL;
		ClusterRelmapCensusCommitted out;
		ClusterRelmapRetentionDirectory saved[2];
		census_execute_setup(&f, pending);
		memcpy(saved, f.origin, sizeof(saved));
		UT_ASSERT(cluster_relmap_retention_census_open_cold(3, 1024 * 1024, CENSUS_BUDGET,
															1024 * 1024, &census));
		UT_ASSERT(census_committed_drain(census));
		memset(&out, 0xa5, sizeof(out));
		/* Even a clean selected global cannot hide another namespace pending. */
		UT_ASSERT(!cluster_relmap_retention_census_committed_cold(
			census, pending ? InvalidOid : 70001, &out));
		UT_ASSERT(census_committed_zero(&out));
		UT_ASSERT_EQ(cluster_relmap_retention_census_next(census),
					 CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
		UT_ASSERT_EQ(census_native_prepares + census_arbitrations, 0);
		census_execute_done(&f, census, saved);
	}
}

UT_TEST(test_cold_committed_whole_root_and_pair_changes_are_sticky)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		CensusFixture f;
		ClusterRelmapRetentionCensus *census = NULL;
		ClusterRelmapCensusCommitted out;
		ClusterRelmapRetentionDirectory saved[2];
		census_execute_setup(&f, false);
		memcpy(saved, f.origin, sizeof(saved));
		UT_ASSERT(census_execute_run(&census));
		if (fault == 0)
			UT_ASSERT_EQ(unlinkat(f.fd, "base/5/pgrac_relmap_authority.bak", 0), 0);
		if (fault == 1)
			root_file(f.fd, "unrelated", "warm", 4);
		if (fault == 2) {
			UT_ASSERT_EQ(mkdirat(f.fd, "base/6", 0700), 0);
			census_pair(f.fd, 6, 3, false, false, false);
		}
		if (fault == 3)
			census_pair(f.fd, 5, 3, false, false, true);
		if (fault == 4) {
			UT_ASSERT_EQ(renameat(f.fd, "base/5/pgrac_relmap_authority", f.fd, "held-old"), 0);
			census_pair(f.fd, 5, 3, false, false, false);
		}
		if (fault == 5) {
			census_committed_reads = 0;
			census_committed_at = 1;
			census_committed_fault = 9;
			namespace_read_transform = census_committed_read_fault;
		}
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT(!cluster_relmap_retention_census_committed_cold(census, 5, &out));
		namespace_read_transform = NULL;
		UT_ASSERT(census_committed_zero(&out));
		UT_ASSERT(!cluster_relmap_retention_census_committed_cold(census, InvalidOid, &out));
		UT_ASSERT(census_committed_zero(&out));
		census_execute_done(&f, census, saved);
	}
}

UT_TEST(test_cold_committed_io_rechecks_source_context_and_all_ancestor_pins)
{
	for (unsigned pending = 0; pending < 2; pending++)
		for (unsigned at = 1; at <= 3; at += 2)
			for (unsigned fault = 1; fault <= 8; fault++) {
				CensusFixture f;
				ClusterRelmapRetentionCensus *census = NULL;
				ClusterRelmapCensusCommitted out;
				ClusterRelmapRetentionDirectory saved[2];
				volatile bool accepted = false, threw = false;
				census_execute_setup(&f, pending);
				memcpy(saved, f.origin, sizeof(saved));
				UT_ASSERT(census_execute_run(&census));
				census_committed_reads = 0;
				census_committed_at = at;
				census_committed_fault = fault;
				namespace_read_transform = census_committed_read_fault;
				memset(&out, 0xa5, sizeof(out));
				PG_TRY();
				{
					accepted = cluster_relmap_retention_census_committed_cold(census, 5, &out);
				}
				PG_CATCH();
				{
					threw = true;
				}
				PG_END_TRY();
				namespace_read_transform = NULL;
				UT_ASSERT(!accepted && census_committed_zero(&out));
				UT_ASSERT_EQ(threw, fault >= 7);
				UT_ASSERT_EQ(cluster_relmap_retention_census_next(census),
							 CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
				census_execute_done(&f, census, saved);
			}
}

UT_TEST(test_cold_committed_output_aliases_preserve_current_and_owned_parent_storage)
{
	for (unsigned pending = 0; pending < 2; pending++) {
		CensusFixture f;
		ClusterRelmapRetentionCensus *census = NULL;
		ClusterRelmapCensusCommitted before, out;
		ClusterRelmapRetentionDirectory saved[2];
		void *aliases[3];
		census_execute_setup(&f, pending);
		memcpy(saved, f.origin, sizeof(saved));
		UT_ASSERT(census_execute_run(&census));
		aliases[0] = census;
		aliases[1] = census_initial_directory;
		aliases[2] = &census_initial_directory->pins[2];
		for (unsigned i = 0; i < lengthof(aliases); i++) {
			memcpy(&before, aliases[i], sizeof(before));
			UT_ASSERT(!cluster_relmap_retention_census_committed_cold(census, 5, aliases[i]));
			UT_ASSERT_EQ(memcmp(&before, aliases[i], sizeof(before)), 0);
		}
		UT_ASSERT(cluster_relmap_retention_census_committed_cold(census, 5, &out));
		census_execute_done(&f, census, saved);
	}
}
