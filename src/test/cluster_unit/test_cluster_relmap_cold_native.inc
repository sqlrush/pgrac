/* Cold executor with actual WAL/custody, arbiter and dual-copy storage.
 * Original cold owner/native CLOG I/O and invalidation transport are controlled
 * leaves, not native/all-origin qualification. Author: SqlRush <sqlrush@gmail.com> */
static unsigned cold_native_syncs, cold_native_invals, cold_native_acks;
static unsigned cold_native_post_fault;
static char *cold_native_saved_dir;
ClusterConf *ClusterConfShmem;
static ClusterConf cold_native_conf = { .node_count = 2 };
static const char *const cold_native_paths[]
	= { "global/pgrac_relmap_authority", "global/pgrac_relmap_authority.bak",
		"base/42/pgrac_relmap_authority", "base/42/pgrac_relmap_authority.bak" };

static void
cold_native_sync(void)
{
	cold_native_syncs++;
	if (cold_native_syncs == 4 && cold_native_post_fault == 1)
		custody_scope++;
	if (cold_native_syncs == 4 && cold_native_post_fault == 2)
		custody_scope_error_after = custody_scope_reads + 1;
}

/* The actual arbiter must broadcast only after both new images are durable
 * and its original native/source handle has been rechecked. */
void
SendSharedInvalidMessages(const SharedInvalidationMessage *messages, int count)
{
	ClusterRelmapV2View view;
	UT_ASSERT_EQ(count, 1);
	UT_ASSERT_EQ(messages[0].rm.id, SHAREDINVALRELMAP_ID);
	UT_ASSERT_EQ(messages[0].rm.dbId, custody_image.identity.dbid);
	UT_ASSERT_EQ(cold_native_syncs, 4);
	UT_ASSERT(prefix_base_reads >= 3 && custody_scope == 7);
	UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&custody_image.identity, &view),
				 CLUSTER_RELMAP_V2_PAIR_VALID);
	UT_ASSERT(view.pending_complete && !view.has_pending);
	UT_ASSERT_EQ(view.committed_generation, 8);
	UT_ASSERT_EQ(memcmp(&view.committed, &custody_image.pending, sizeof(RelMapFile)), 0);
	cold_native_invals++;
}

ClusterSinvalAckResult
cluster_sinval_enqueue_and_wait_ack(const SharedInvalidationMessage *messages, int count)
{
	UT_ASSERT_EQ(count, 1);
	UT_ASSERT_EQ(messages[0].rm.dbId, custody_image.identity.dbid);
	UT_ASSERT_EQ(cold_native_invals, 1);
	cold_native_acks++;
	return CLUSTER_SINVAL_ACK_DONE;
}

static void
cold_native_setup(bool shared, bool aborted, bool clean)
{
	ClusterRelmapV2Image image;
	int fd;
	execution_pending_setup(shared, aborted, 0);
	execution_root_fault = execution_root_opens = 0;
	execution_root_fd = -1;
	relmap_root_merger = relmap_root_namespace_x = cluster_shared_catalog = true;
	observe_fault = observe_reads = 0;
	observe_inode = 0;
	cold_native_saved_dir = cluster_shared_data_dir;
	cluster_shared_data_dir = test_root;
	ClusterConfShmem = &cold_native_conf;
	cold_native_syncs = cold_native_invals = cold_native_acks = cold_native_post_fault = 0;
	test_history_sync_count = test_history_fail_sync = 0;
	fd = open(test_root, O_RDONLY | O_DIRECTORY);
	UT_ASSERT(fd >= 0);
	UT_ASSERT(mkdirat(fd, "base", 0700) == 0 || errno == EEXIST);
	UT_ASSERT_EQ(mkdirat(fd, "base/42", 0700), 0);
	for (unsigned i = 0; i < lengthof(cold_native_paths); i++) {
		image = custody_image;
		image.identity.shared = i < 2;
		image.identity.dbid = i < 2 ? InvalidOid : 42;
		if (clean || image.identity.shared != shared) {
			image.pending_generation = 0;
			memset(&image.pending_owner, 0, sizeof(image.pending_owner));
			memset(&image.pending, 0, sizeof(image.pending));
		}
		relmap_census_file(fd, cold_native_paths[i], &image);
	}
	UT_ASSERT_EQ(close(fd), 0);
	test_startup_sync_hook = cold_native_sync;
}

static void
cold_native_done(ClusterRelmapRetentionCensus *census)
{
	int fd;
	test_startup_sync_hook = NULL;
	test_history_fail_sync = test_history_sync_count = 0;
	cluster_relmap_retention_census_destroy(census);
	if (execution_root_fd >= 0)
		UT_ASSERT(fcntl(execution_root_fd, F_GETFD) < 0 && errno == EBADF);
	UT_ASSERT(!inputs_pin_held && inputs_pin_refuse && inputs_pin_current);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	UT_ASSERT_EQ(inputs_peer_begins + inputs_native_begin_calls + inputs_native_flush_calls, 0);
	fd = open(test_root, O_RDONLY | O_DIRECTORY);
	UT_ASSERT(fd >= 0);
	for (unsigned i = 0; i < lengthof(cold_native_paths); i++)
		UT_ASSERT_EQ(unlinkat(fd, cold_native_paths[i], 0), 0);
	UT_ASSERT_EQ(unlinkat(fd, "base/42", AT_REMOVEDIR), 0);
	UT_ASSERT_EQ(close(fd), 0);
	cluster_shared_data_dir = cold_native_saved_dir;
	ClusterConfShmem = NULL;
	execution_root_done();
}

static bool
cold_native_execute(ClusterRelmapRetentionCensus **out)
{
	return cluster_relmap_retention_census_execute_cold(
		0, 4 * 1024 * 1024, 512 * 1024, 1024 * 1024, out);
}

UT_TEST(test_cold_native_commit_abort_actual_pair_and_fresh_committed)
{
	for (unsigned sample = 0; sample < 8; sample++) {
		bool shared = (sample & 1) != 0, aborted = (sample & 2) != 0;
		ClusterRelmapRetentionCensus *census = NULL;
		ClusterRelmapCensusCommitted out;
		ClusterRelmapV2View view;
		custody_era_offset = sample & 4 ? UINT64CONST(1) << 32 : 0;
		cold_native_setup(shared, aborted, false);
		UT_ASSERT(cold_native_execute(&census));
		UT_ASSERT(census != NULL && fcntl(execution_root_fd, F_GETFD) >= 0);
		UT_ASSERT_EQ(execution_root_opens, 1);
		UT_ASSERT_EQ(cold_native_syncs, 4);
		UT_ASSERT_EQ(cold_native_invals, !aborted);
		UT_ASSERT_EQ(cold_native_acks, !aborted && cluster_conf_has_peers());
		UT_ASSERT(prefix_base_reads >= 3 && execution_visits >= 3 && custody_scope == 7);
		UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&custody_image.identity, &view),
					 CLUSTER_RELMAP_V2_PAIR_VALID);
		UT_ASSERT(view.pending_complete && !view.has_pending);
		UT_ASSERT(cluster_relmap_retention_census_committed_cold(
			census, custody_image.identity.dbid, &out));
		UT_ASSERT(out.present && out.generation == (aborted ? 7 : 8));
		UT_ASSERT_EQ(memcmp(&out.identity, &custody_image.identity, sizeof(out.identity)), 0);
		UT_ASSERT_EQ(memcmp(&out.map, aborted ? &custody_image.committed : &custody_image.pending,
						   sizeof(out.map)), 0);
		UT_ASSERT_EQ(cluster_relmap_retention_census_next(census), CLUSTER_RELMAP_CENSUS_DRAINED);
		cold_native_done(census);
		if (ut_current_failed)
			return;
	}
}

UT_TEST(test_cold_native_clean_does_not_read_clog_or_write_pairs)
{
	ClusterRelmapRetentionCensus *census = NULL;
	ClusterRelmapCensusCommitted out;
	cold_native_setup(false, false, true);
	UT_ASSERT(cold_native_execute(&census));
	UT_ASSERT(cluster_relmap_retention_census_committed_cold(census, 42, &out));
	UT_ASSERT(out.present && out.generation == 7);
	UT_ASSERT_EQ(memcmp(&out.map, &custody_image.committed, sizeof(out.map)), 0);
	UT_ASSERT_EQ(prefix_base_reads + cold_native_syncs + cold_native_invals + cold_native_acks, 0);
	UT_ASSERT_EQ(custody_scope, 7);
	cold_native_done(census);
}

UT_TEST(test_cold_native_unknown_and_native_error_keep_pending)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterRelmapRetentionCensus *census = NULL;
		ClusterRelmapV2View view;
		volatile bool accepted = false, threw = false;
		cold_native_setup(false, false, false);
		prefix_base_fault = fault == 0 ? 2 : fault == 1 ? 5 : 8;
		test_capture_error_level = true;
		PG_TRY();
		{
			accepted = cold_native_execute(&census);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		test_capture_error_level = false;
		UT_ASSERT(!accepted && census == NULL);
		UT_ASSERT_EQ(threw, fault == 2);
		UT_ASSERT_EQ(prefix_base_reads, 1);
		UT_ASSERT_EQ(cold_native_syncs + cold_native_invals + cold_native_acks, 0);
		UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&custody_image.identity, &view),
					 CLUSTER_RELMAP_V2_PAIR_VALID);
		UT_ASSERT(view.pending_complete && view.has_pending && custody_scope == 7);
		cold_native_done(census);
	}
}

UT_TEST(test_cold_native_each_sync_cut_returns_no_observation_or_retry)
{
	for (unsigned aborted = 0; aborted < 2; aborted++)
		for (unsigned cut = 1; cut <= 4; cut++) {
			ClusterRelmapRetentionCensus *census = NULL;
			ClusterRelmapV2View view;
			cold_native_setup(false, aborted, false);
			test_history_fail_sync = cut;
			UT_ASSERT(!cold_native_execute(&census));
			UT_ASSERT(census == NULL && custody_scope == 7);
			UT_ASSERT_EQ(cold_native_syncs, cut);
			UT_ASSERT_EQ(cold_native_invals + cold_native_acks, 0);
			UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&custody_image.identity, &view),
						 CLUSTER_RELMAP_V2_PAIR_VALID);
			UT_ASSERT_EQ(view.has_pending, cut < 4);
			UT_ASSERT_EQ(view.committed_generation, !aborted && cut >= 2 ? 8 : 7);
			cold_native_done(census);
			if (ut_current_failed)
				return;
		}
}

UT_TEST(test_cold_native_postwrite_scope_loss_or_error_keeps_original_debt)
{
	for (unsigned fault = 1; fault <= 2; fault++) {
		ClusterRelmapRetentionCensus *census = NULL;
		ClusterRelmapV2View view;
		volatile bool accepted = false, threw = false;
		cold_native_setup(false, false, false);
		cold_native_post_fault = fault;
		test_capture_error_level = true;
		PG_TRY();
		{
			accepted = cold_native_execute(&census);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		test_capture_error_level = false;
		UT_ASSERT(!accepted && census == NULL);
		UT_ASSERT_EQ(threw, fault == 2);
		UT_ASSERT_EQ(cold_native_syncs, 4);
		UT_ASSERT_EQ(custody_scope, fault == 1 ? 8 : 7);
		UT_ASSERT_EQ(cold_native_invals + cold_native_acks, 0);
		UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&custody_image.identity, &view),
					 CLUSTER_RELMAP_V2_PAIR_VALID);
		UT_ASSERT(!view.has_pending && view.committed_generation == 8);
		cold_native_done(census);
	}
}
