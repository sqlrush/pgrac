/* PGRAC: original owner clean conversion through real dual-copy I/O.
 * Uses the transaction/native writer/serialized lock providers of the runtime
 * storage fixture, without attaching its runtime or publishing COMPLETE.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md, D3.
 */
static void
owner_conversion_read_v1(int copy, unsigned char *bytes)
{
	char path[MAXPGPATH];
	struct stat st;
	int fd;

	snprintf(path, sizeof(path), "%s/%s", directory, names[copy]);
	fd = open(path, O_RDONLY);
	if (fd < 0 || fstat(fd, &st) != 0 || st.st_size != CLUSTER_RELMAP_AUTHORITY_FILE_SIZE
		|| read(fd, bytes, CLUSTER_RELMAP_AUTHORITY_FILE_SIZE) != CLUSTER_RELMAP_AUTHORITY_FILE_SIZE
		|| close(fd) != 0)
		abort();
}

static void
owner_conversion_setup(ClusterRelmapOwnerSampleV2 *sample,
	ClusterRelmapV2Namespace *ns, bool shared)
{
	MyDatabaseId = 42;
	*ns = identity;
	ns->shared = shared;
	ns->dbid = shared ? InvalidOid : MyDatabaseId;
	setup_v1(ns);
	/* A clean format migration neither needs nor publishes retention COMPLETE.
	 * The numeric bounds remain fixture data, not qualified producer inputs. */
	memset(&writer_retention, 0, sizeof(writer_retention));
	UT_ASSERT_EQ(cluster_relmap_retention_build_start(&writer_retention,
		ns->system_identifier, ns->database_incarnation, owner.node, 1,
		owner.top_full_xid - 16, owner.top_full_xid + 16), CLUSTER_RELMAP_RETENTION_OK);
	cluster_node_id = owner.node;
	MyBackendType = B_BACKEND;
	MyProcPid = 79;
	CritSectionCount = 0;
	ShutdownRequestPending = false;
	runtime_storage_full_xid = owner.top_full_xid;
	runtime_storage_in_progress = true;
	runtime_storage_assignments = 0;
	memset(&runtime_storage_writer, 0, sizeof(runtime_storage_writer));
	runtime_storage_writer.ref.timeline = 1;
	runtime_storage_writer.epoch = owner.membership_epoch;
	runtime_storage_writer.ref.claim.identity.system_identifier = ns->system_identifier;
	runtime_storage_writer.ref.claim.database_incarnation = ns->database_incarnation;
	runtime_storage_writer.ref.claim.identity.origin_node_id = owner.node;
	runtime_storage_writer.ref.claim.identity.origin_thread_id = owner.node + 1;
	runtime_storage_writer.ref.claim.identity.origin_owner_incarnation = owner.admitted_incarnation;
	UT_ASSERT(cluster_relmap_owner_capture_v2(sample));
	UT_ASSERT_EQ(runtime_storage_assignments, 1);
}

UT_TEST(test_owner_conversion_real_pair_preserves_building_directory)
{
	for (int shared = 0; shared < 2; shared++) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapV2Namespace ns;
		ClusterRelmapRetentionDirectory before;
		owner_conversion_setup(&sample, &ns, shared);
		before = writer_retention;
		UT_ASSERT(cluster_relmap_owner_convert_clean_v2(&sample, shared));
		UT_ASSERT_EQ(renames, 2);
		UT_ASSERT_EQ(file_syncs, 2);
		UT_ASSERT_EQ(dir_syncs, 2);
		UT_ASSERT(conversion_pair_clean(&ns));
		UT_ASSERT_EQ(runtime_storage_assignments, 1);
		UT_ASSERT_EQ(writer_retention.state, CLUSTER_RELMAP_RETENTION_BUILDING);
		UT_ASSERT_EQ(memcmp(&writer_retention, &before, sizeof(before)), 0);
	}
}

UT_TEST(test_owner_conversion_real_pending_refusal_preserves_both_copies)
{
	for (int shared = 0; shared < 2; shared++) {
		for (int which = 0; which < 3; which++) {
			ClusterRelmapOwnerSampleV2 sample;
			ClusterRelmapV2Namespace ns;
			ClusterRelmapRetentionDirectory before;
			ClusterRelmapAuthorityHeader hdr;
			unsigned char bytes[CLUSTER_RELMAP_AUTHORITY_FILE_SIZE];
			unsigned char originals[2][CLUSTER_RELMAP_AUTHORITY_FILE_SIZE];
			unsigned char after[CLUSTER_RELMAP_AUTHORITY_FILE_SIZE];
			owner_conversion_setup(&sample, &ns, shared);
			before = writer_retention;
			v1_image(&ns, bytes);
			memcpy(&hdr, bytes, sizeof(hdr));
			hdr.pending_generation = 8;
			hdr.owner_node = owner.node;
			hdr.owner_xid = (uint32) owner.top_full_xid;
			v1_header_crc(bytes, &hdr);
			write_copy(which == 1 ? 1 : 0, bytes, sizeof(bytes));
			if (which == 2) write_copy(1, bytes, sizeof(bytes));
			owner_conversion_read_v1(0, originals[0]);
			owner_conversion_read_v1(1, originals[1]);
			UT_ASSERT(!cluster_relmap_owner_convert_clean_v2(&sample, shared));
			UT_ASSERT_EQ(writes, 0);
			UT_ASSERT_EQ(renames, 0);
			for (int i = 0; i < 2; i++) {
				owner_conversion_read_v1(i, after);
				UT_ASSERT_EQ(memcmp(after, originals[i], sizeof(after)), 0);
			}
			UT_ASSERT_EQ(memcmp(&writer_retention, &before, sizeof(before)), 0);
		}
	}
}

UT_TEST(test_owner_conversion_real_mixed_cut_requires_distinct_retry)
{
	for (int shared = 0; shared < 2; shared++) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapV2Namespace ns;
		ClusterRelmapV2Image converted;
		ClusterRelmapRetentionDirectory before;
		unsigned char bytes[2176], v1[CLUSTER_RELMAP_AUTHORITY_FILE_SIZE];
		owner_conversion_setup(&sample, &ns, shared);
		before = writer_retention;
		fail_rename = 2;
		UT_ASSERT(!cluster_relmap_owner_convert_clean_v2(&sample, shared));
		owner_conversion_read_v1(0, v1);
		UT_ASSERT_EQ(cluster_relmap_authority_classify((char *) v1, sizeof(v1)),
			CLUSTER_RELMAP_AUTHORITY_VALID);
		read_copy(1, bytes);
		UT_ASSERT_EQ(cluster_relmap_v2_decode(bytes, sizeof(bytes), &ns, &converted),
			CLUSTER_RELMAP_V2_VALID);
		UT_ASSERT_EQ(converted.pending_generation, 0);
		reset_io();
		relmap_x_held = true;
		UT_ASSERT(cluster_relmap_owner_convert_clean_v2(&sample, shared));
		UT_ASSERT_EQ(renames, 2);
		UT_ASSERT_EQ(file_syncs, 2);
		UT_ASSERT_EQ(dir_syncs, 2);
		UT_ASSERT(conversion_pair_clean(&ns));
		UT_ASSERT_EQ(runtime_storage_assignments, 1);
		UT_ASSERT_EQ(memcmp(&writer_retention, &before, sizeof(before)), 0);
	}
}

static void
owner_conversion_drift_after_sync(bool is_directory)
{
	if (is_directory && dir_syncs == 2)
		runtime_storage_writer.epoch++;
}

UT_TEST(test_owner_conversion_real_final_owner_drift_is_not_rollback)
{
	for (int shared = 0; shared < 2; shared++) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapV2Namespace ns;
		ClusterRelmapRetentionDirectory before;
		owner_conversion_setup(&sample, &ns, shared);
		before = writer_retention;
		post_sync_hook = owner_conversion_drift_after_sync;
		UT_ASSERT(!cluster_relmap_owner_convert_clean_v2(&sample, shared));
		UT_ASSERT_EQ(renames, 2);
		UT_ASSERT(conversion_pair_clean(&ns));
		UT_ASSERT_EQ(memcmp(&writer_retention, &before, sizeof(before)), 0);
		reset_io();
		relmap_x_held = true;
		/* Same original FullXid, explicitly refreshed native writer sample.
		 * Already-v2 byte equality cannot substitute for the new sync receipt. */
		UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
		UT_ASSERT(cluster_relmap_owner_convert_clean_v2(&sample, shared));
		UT_ASSERT_EQ(file_syncs, 2);
		UT_ASSERT_EQ(dir_syncs, 2);
		UT_ASSERT(conversion_pair_clean(&ns));
		UT_ASSERT_EQ(memcmp(&writer_retention, &before, sizeof(before)), 0);
	}
}
