/* File-backed output and typed DATA/physical-PI boundaries.
 * The two fixture namespaces are separate executables: neither substitutes
 * a transport callback for a DATA receipt or a real PCM retirement owner.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifdef K_P130_H13_DATA
#include "test_cluster_k_p130_h13_data.inc"

static void
h13_data(int fault)
{
	ClusterPageDataReceiptV1 *data = wb_setup();
	ClusterPiWritebackJobV1 *job = NULL;
	ClusterPiPhysicalAckV1 *ack = NULL;
	ClusterPiWritebackMessageV2 request, reply;
	ClusterPcmPiStorageCutV1 proven;
	PGAlignedBlock before;
	int32 node = -1;
	bool decoded, imported;

	UT_ASSERT(data != NULL);
	UT_ASSERT_EQ(syncs, 1);
	UT_ASSERT_EQ(reads, 2);
	UT_ASSERT(cluster_page_data_pi_storage_proof_v1(data, wb_page_plan, wb_sources, 3, &proven));
	UT_ASSERT_EQ(memcmp(&proven, &wb_storage_cut, sizeof(proven)), 0);
	wb_local_caps = wb_peer_caps = PGRAC_IC_HELLO_CAP_PI_STRUCTURAL_V2;
	UT_ASSERT_EQ(cluster_pi_writeback_begin_v1((const ClusterPageDataReceiptV1 **)&data, 1,
		&wb_sources[1], &job), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(job != NULL);
	UT_ASSERT(!cluster_page_data_pi_ack_import_v1(job, 0, data, &ack));
	UT_ASSERT(ack == NULL);
	wb_select(0, B_LMON);
	cluster_pi_writeback_lmon_tick_v1();
	UT_ASSERT_EQ(wb_destination, 1);
	decoded = cluster_pi_writeback_decode_v2(wb_wire, wb_length, &request);
	UT_ASSERT(decoded);
	if (!decoded) goto done;
	UT_ASSERT_EQ(request.count, 1);
	UT_ASSERT_EQ(request.facts[0].kind, CLUSTER_PI_WRITEBACK_DATA_V2);
	physical_pi_from_source(&wb_sources[1], fault == 2 ? 81 : 19);
	memcpy(before.data, pages[1].data, BLCKSZ);
	if (fault == 1) pg_atomic_fetch_add_u32(&descriptors[1].bufferdesc.state, 1);
	wb_deliver(0, 1, wb_wire, wb_length);
	UT_ASSERT_EQ(pi_discards, 0); /* Notify ingress has no physical proof. */
	wb_select(1, B_BG_WRITER);
	UT_ASSERT(cluster_pi_writeback_bgwriter_tick_v1());
	UT_ASSERT_EQ(pi_discards, fault ? 0 : 1);
	UT_ASSERT_EQ(wb_plan_builds, 1);
	UT_ASSERT_EQ(wb_input_releases, 1);
	UT_ASSERT(!wb_inputs_pinned[1]);
	if (fault) UT_ASSERT_EQ(memcmp(before.data, pages[1].data, BLCKSZ), 0);
	wb_select(1, B_LMON);
	cluster_pi_writeback_lmon_tick_v1();
	UT_ASSERT_EQ(wb_destination, 0);
	decoded = cluster_pi_writeback_decode_v2(wb_wire, wb_length, &reply);
	UT_ASSERT(decoded);
	if (!decoded) goto done;
	UT_ASSERT(cluster_pi_writeback_ack_matches_v2(&request, &reply));
	UT_ASSERT_EQ(reply.count, fault ? 0 : 1);
	wb_deliver(1, 0, wb_wire, wb_length);
	wb_select(0, B_CHECKPOINTER);
	UT_ASSERT_EQ(cluster_pi_writeback_poll_v1(job), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	imported = cluster_page_data_pi_ack_import_v1(job, 0, data, &ack);
	UT_ASSERT_EQ(imported, fault == 0);
	if (imported) {
		UT_ASSERT(cluster_page_data_pi_ack_read_v1(ack, data, &node));
		UT_ASSERT_EQ(node, 1);
	} else UT_ASSERT(ack == NULL);
	/* The original physical owner calls this fixture's logical-PCM leaf.
	 * Count that call without claiming actual PCM debt was retired. */
	UT_ASSERT_EQ(logical_pi_retire_calls, fault ? 0 : 1);
	UT_ASSERT_EQ(wb_master_completions, 0);
	printf("# H13 DATA fault=%d sync=%u reads=%u discard=%u reply=%u ack=%d node=%d plans=%u released=%u logical_calls=%u master=%u\n",
		fault, syncs, reads, pi_discards, reply.count, imported, node,
		wb_plan_builds, wb_input_releases, logical_pi_retire_calls, wb_master_completions);
done:
	wb_select(0, B_CHECKPOINTER);
	if (fault == 1) pg_atomic_fetch_sub_u32(&descriptors[1].bufferdesc.state, 1);
	cluster_page_data_pi_ack_free_v1(&ack);
	cluster_pi_writeback_release_v1(&job);
	UT_ASSERT(job == NULL && ack == NULL);
	cluster_page_data_receipt_free_v1(&data);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	UT_ASSERT(!wb_inputs_pinned[0] && !wb_inputs_pinned[1] && !wb_inputs_pinned[2]);
	clean();
}
UT_TEST(h13_durable_DATA_reaches_original_physical_PI_owner) { h13_data(0); }
UT_TEST(h13_durable_DATA_cannot_discard_pinned_PI) { h13_data(1); }
UT_TEST(h13_durable_DATA_cannot_discard_uncovered_PI) { h13_data(2); }
UT_TEST(h13_sync_with_changed_storage_cut_cannot_publish_receipt)
{
	ClusterWalSourceRef sources[3];
	ClusterPcmPiStorageCutV1 cut;
	ClusterPageDataReceiptV1 *data = NULL;
	RfPageOnlinePlanV1 *plan = prepare_storage_observation(sources, &cut);
	storage_cut_changed = true;
	UT_ASSERT(!cluster_bufmgr_observe_pi_storage_v1(&target, &cut, plan, sources, 3, &data));
	UT_ASSERT(data == NULL);
	UT_ASSERT_EQ(syncs, 1);
	UT_ASSERT_EQ(writes + wal_flushes + pi_discards + logical_pi_retire_calls, 0);
	UT_ASSERT(!directory_io_entered);
	UT_ASSERT_EQ(directory_io_begins, directory_io_ends);
	printf("# H13 stale-cut sync=%u reads=%u receipt=%d discard=%u logical_calls=%u directory=%u/%u\n",
		syncs, reads, data != NULL, pi_discards, logical_pi_retire_calls,
		directory_io_begins, directory_io_ends);
	cluster_page_data_receipt_free_v1(&data);
	rf_page_online_plan_destroy_v1(&plan);
	clean();
}
#else
#include "test_cluster_k_p130_h13_native.inc"

static void
h13_native(PcmState target, bool sync_file)
{
	HolderFixture f;
	ClusterPageWalBindingV1 wal;
	ClusterPcmLocalPiSnapshotV1 before = {0}, after = {0};
	ClusterPcmLocalPiFloorV1 floor = {0};
	WRITEBACK_START(&f, target);
	wal = writeback_install_wal();
	writeback_dirty();
	HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(holder_writes, 1);
	UT_ASSERT_EQ(holder_write_notes, 1);
	UT_ASSERT_EQ(holder_output_wakes, 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.write_io_generation), 2);
	UT_ASSERT_EQ(disk_reads, 0);
	UT_ASSERT((pg_atomic_read_u32(&request_buf.state) & (BM_IO_IN_PROGRESS | BM_DIRTY)) == 0);
	UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(request_buf.tag, &before));
	UT_ASSERT(cluster_page_wal_same_mutation_v1(&before.last, &wal));
	UT_ASSERT(cluster_pcm_local_pi_floor_v1(&wal.source, &floor));
	UT_ASSERT_EQ(floor.bounded, 1);
	UT_ASSERT_EQ(floor.floor, wal.record_start);
	HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED);
	for (int half = 0; half < 2; half++) holder_send_control(&f, half, true);
	if (sync_file) UT_ASSERT_EQ(fsync(fileno(disk_file)), 0);
	UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(request_buf.tag, &after));
	UT_ASSERT_EQ(memcmp(&before, &after, sizeof(before)), 0);
	UT_ASSERT(!cluster_pcm_local_pi_retire_v1(&after, NULL, NULL, NULL, 0, NULL));
	UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(request_buf.tag, &after));
	UT_ASSERT_EQ(memcmp(&before, &after, sizeof(before)), 0);
	UT_ASSERT(cluster_pcm_local_pi_floor_v1(&wal.source, &floor));
	UT_ASSERT_EQ(floor.bounded, 2);
	UT_ASSERT_EQ(floor.floor, wal.record_start);
	UT_ASSERT_EQ(f.master.state->pi_carrier.pi_refs, 1);
	UT_ASSERT(!local_pi_covered);
	writeback_file_matches();
	writeback_idle();
	printf("# H13 native target=%d host_sync=%d writes=%u IOgen=%llu wakes=%u reads=%u lower=%llu floor=%llu PI=%u receipt=0\n",
		target, sync_file, holder_writes, (unsigned long long)pg_atomic_read_u64(&request_own.write_io_generation),
		holder_output_wakes, disk_reads, (unsigned long long)floor.bounded,
		(unsigned long long)floor.floor, f.master.state->pi_carrier.pi_refs);
}
UT_TEST(h13_write_only_S_retains_original_PI_and_WAL) { h13_native(PCM_STATE_S, false); }
UT_TEST(h13_write_only_X_retains_original_PI_and_WAL) { h13_native(PCM_STATE_X, false); }
UT_TEST(h13_host_sync_S_without_typed_receipt_retains_PI) { h13_native(PCM_STATE_S, true); }
UT_TEST(h13_host_sync_X_without_typed_receipt_retains_PI) { h13_native(PCM_STATE_X, true); }
#endif

#if defined(K_P130_EXPECT_ASSERT_OFF) && defined(USE_ASSERT_CHECKING)
#error "H13 assertion-OFF requires an assertion-disabled configuration"
#endif
int main(void)
{
#ifdef USE_ASSERT_CHECKING
	printf("# H13 USE_ASSERT_CHECKING=1\n");
#else
	printf("# H13 USE_ASSERT_CHECKING=0\n");
#endif
	UT_PLAN(4);
#ifdef K_P130_H13_DATA
	UT_RUN(h13_durable_DATA_reaches_original_physical_PI_owner);
	UT_RUN(h13_durable_DATA_cannot_discard_pinned_PI);
	UT_RUN(h13_durable_DATA_cannot_discard_uncovered_PI);
	UT_RUN(h13_sync_with_changed_storage_cut_cannot_publish_receipt);
#else
	UT_RUN(h13_write_only_S_retains_original_PI_and_WAL);
	UT_RUN(h13_write_only_X_retains_original_PI_and_WAL);
	UT_RUN(h13_host_sync_S_without_typed_receipt_retains_PI);
	UT_RUN(h13_host_sync_X_without_typed_receipt_retains_PI);
#endif
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
