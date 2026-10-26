/*-------------------------------------------------------------------------
 * test_cluster_recovery_relmap_fold.inc
 *     Complete original pending native carrier tests.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_relmap_fold.inc
 *
 * NOTES
 *     Native WAL framing and original retained readers execute. Raw carriers
 *     and DRAINED do not grant era, lifetime or terminal qualification.
 *-------------------------------------------------------------------------
 */
/* Actual native framing emitted by xloginsert for a child-xid carrier. */
static WalTestRecord
relmap_fold_write_child_carrier(const ClusterWalSourceRef *source, WalTestRecord next)
{
	uint8 bytes[256] = { 0 };
	char path[MAXPGPATH];
	xl_smgr_create payload = { .rlocator = { 1663, 5, 16385 }, .forkNum = MAIN_FORKNUM };
	TransactionId top = 4195136;
	XLogRecord header = { 0 };
	size_t at = SizeOfXLogRecord;
	int fd;

	bytes[at++] = XLR_BLOCK_ID_TOPLEVEL_XID;
	memcpy(bytes + at, &top, sizeof(top));
	at += sizeof(top);
	bytes[at++] = XLR_BLOCK_ID_DATA_SHORT;
	bytes[at++] = sizeof(payload);
	memcpy(bytes + at, &payload, sizeof(payload));
	at += sizeof(payload);
	header.xl_tot_len = at;
	header.xl_prev = next.record_start;
	header.xl_rmid = RM_SMGR_ID;
	header.xl_info = XLOG_SMGR_CREATE;
	header.xl_xid = 4195137;
	header.xl_scn = 5906;
	INIT_CRC32C(header.xl_crc);
	COMP_CRC32C(header.xl_crc, bytes + SizeOfXLogRecord, at - SizeOfXLogRecord);
	COMP_CRC32C(header.xl_crc, &header, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(header.xl_crc);
	memcpy(bytes, &header, SizeOfXLogRecord);
	UT_ASSERT(next.exclusive_end % XLOG_BLCKSZ + at < XLOG_BLCKSZ);
	v2_checkpoint_wal_path(&source->claim.identity, next.exclusive_end, source->timeline, path);
	fd = open(path, O_WRONLY);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pwrite(fd, bytes, at, next.exclusive_end % wal_segment_size), at);
	UT_ASSERT_EQ(close(fd), 0);
	return (WalTestRecord){ 1, MAXALIGN(next.exclusive_end + at), next.exclusive_end,
							header.xl_crc };
}

static WalTestRecord
inventory_write_relmap_fold_sequence(const ClusterWalSourceRef *source, WalTestRecord next,
									 unsigned node)
{
	char bytes[TEST_PREPARE_MAX];
	TransactionId children[300];
	TransactionId top = 4196000;
	int count = lengthof(children), page = 4195136 / CLUSTER_NATIVE_CLOG_PER_PAGE;
	xl_clog_truncate truncate = { 33, 33 * CLUSTER_NATIVE_CLOG_PER_PAGE, 5 };
	TimestampTz time = 8765;
	uint32 xinfo = XACT_XINFO_HAS_SUBXACTS;
	uint32 length;

	test_relmap_payload(bytes, node == 0, 2);
	next = inventory_write_record(source, next.exclusive_end, next.record_start, RM_RELMAP_ID,
								  XLOG_RELMAP_UPDATE, bytes, TEST_RELMAP_LENGTH);
	for (unsigned i = 0; i < lengthof(children); ++i)
		children[i] = 4197000 + i;
	children[0] = children[299] = 4195136;
	memcpy(bytes, &top, sizeof(top));
	memcpy(bytes + sizeof(top), &count, sizeof(count));
	memcpy(bytes + MinSizeOfXactAssignment, children, sizeof(children));
	next = inventory_write_record(source, next.exclusive_end, next.record_start, RM_XACT_ID,
								  XLOG_XACT_ASSIGNMENT, bytes,
								  MinSizeOfXactAssignment + sizeof(children));
	count = 1;
	memcpy(bytes, &time, sizeof(time));
	memcpy(bytes + sizeof(time), &xinfo, sizeof(xinfo));
	memcpy(bytes + sizeof(time) + sizeof(xinfo), &count, sizeof(count));
	memcpy(bytes + sizeof(time) + sizeof(xinfo) + sizeof(count), children, sizeof(*children));
	next = inventory_write_record(source, next.exclusive_end, next.record_start, RM_XACT_ID,
								  XLOG_XACT_ABORT | XLOG_XACT_HAS_INFO, bytes,
								  sizeof(time) + sizeof(xinfo) + sizeof(count) + sizeof(*children));
	length = test_prepare_payload(bytes, false, 13);
	next = inventory_write_record(source, next.exclusive_end, next.record_start, RM_XACT_ID,
								  XLOG_XACT_PREPARE, bytes, length);
	next = inventory_write_record(source, next.exclusive_end, next.record_start, RM_CLOG_ID,
								  CLOG_ZEROPAGE, &page, sizeof(page));
	page++;
	next = relmap_fold_write_child_carrier(source, next);
	next = inventory_write_record(source, next.exclusive_end, next.record_start, RM_CLOG_ID,
								  CLOG_ZEROPAGE, &page, sizeof(page));
	return inventory_write_record(source, next.exclusive_end, next.record_start, RM_CLOG_ID,
								  CLOG_TRUNCATE, &truncate, sizeof(truncate));
}

static ClusterRelmapV2Image
relmap_fold_pending(ClusterRecoveryNativeInventory *inventory, unsigned node)
{
	ClusterRelmapV2Image pending = { 0 };
	char bytes[TEST_RELMAP_LENGTH];
	const ClusterRecoveryNativeRecord *map = NULL;

	for (NativeInventoryRecord *r = inventory->head; r != NULL; r = r->next) {
		ClusterRecoveryNativeEffect primary;
		UT_ASSERT_EQ(cluster_recovery_native_scan_primary(r->scan, &primary),
					 CLUSTER_RECOVERY_NATIVE_EFFECT);
		if (primary.kind == CLUSTER_RECOVERY_NATIVE_RELMAP_UPDATE
			&& primary.record->source.claim.identity.origin_node_id == node) {
			map = primary.record;
			break;
		}
	}
	UT_ASSERT(map != NULL);
	if (map == NULL)
		return pending;
	pending.identity
		= (ClusterRelmapV2Namespace){ TEST_SYSID, map->source.claim.database_incarnation,
									  node ? 5 : InvalidOid, node == 0 };
	pending.committed_generation = 7;
	pending.pending_generation = 8;
	pending.pending_owner
		= (ClusterRelmapV2Owner){ node,
								  (UINT64_C(3) << 32) | 4195136,
								  9,
								  map->source.claim.identity.origin_owner_incarnation,
								  map->end_lsn,
								  17 };
	test_relmap_payload(bytes, node == 0, 2);
	memcpy(&pending.pending, bytes + MinSizeOfRelmapUpdate, sizeof(RelMapFile));
	pending.committed = pending.pending;
	return pending;
}

static ClusterRecoveryRelmapFoldResult
relmap_fold_drain(ClusterRecoveryNativeInventory *inventory, const ClusterRelmapV2Image *pending,
				  ClusterRecoveryRelmapFoldSummary *summary, uint32 *flags)
{
	ClusterRecoveryRelmapFoldEntry entry, zero = { 0 };
	ClusterRecoveryRelmapFoldSummary empty = { 0 };
	ClusterRecoveryNativeInventoryDiag diag = { 0 };
	uint64 examined = inventory->diag.relmap_examined;

	*flags = 0;
	for (unsigned step = 0; step < 10000; ++step) {
		ClusterRecoveryRelmapFoldResult result = cluster_recovery_native_inventory_relmap_step(
			inventory, pending, &entry, summary, &diag);
		UT_ASSERT(diag.relmap_examined - examined <= 128);
		examined = diag.relmap_examined;
		if (result == CLUSTER_RECOVERY_RELMAP_FOLD_ENTRY) {
			UT_ASSERT(entry.record != NULL && entry.flags != 0);
			UT_ASSERT(entry.source_index < inventory->source_count);
			if (entry.record != NULL) {
				UT_ASSERT_EQ(entry.record->source.claim.identity.origin_node_id,
							 pending->pending_owner.node);
				UT_ASSERT_EQ(entry.record->source.claim.identity.origin_thread_id,
							 pending->pending_owner.node + 1);
				UT_ASSERT(entry.record_ordinal != 0);
			}
			*flags |= entry.flags;
		} else {
			UT_ASSERT_EQ(memcmp(&entry, &zero, sizeof(entry)), 0);
			if (result != CLUSTER_RECOVERY_RELMAP_FOLD_MORE)
				return result;
		}
		UT_ASSERT_EQ(memcmp(summary, &empty, sizeof(*summary)), 0);
	}
	return CLUSTER_RECOVERY_RELMAP_FOLD_INVALID;
}

/* Dropping full inactive map bytes, using another origin, or consuming the
 * ordinary native/terminal cursor would each break this real-file test. */
UT_TEST(test_relmap_fold_matches_full_native_images_and_preserves_cursors)
{
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryRelmapFoldSummary summary;
	ClusterRecoveryNativeEffect effect;
	ClusterRecoveryTerminalEntry terminal;
	uint32 flags;

	inventory_extra = 14;
	inputs = inventory_fixture(false, false);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	for (unsigned node = 0; node <= 127; node += 127) {
		ClusterRelmapV2Image pending = relmap_fold_pending(inventory, node), changed;
		UT_ASSERT_EQ(relmap_fold_drain(inventory, &pending, &summary, &flags),
					 CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
		UT_ASSERT_EQ(summary.sources, 2);
		UT_ASSERT_EQ(summary.origin_sources, 1);
		UT_ASSERT_EQ(summary.wal_matches, 1);
		UT_ASSERT_EQ(summary.wal_aliases, 0);
		UT_ASSERT_EQ(summary.top_commits, 1);
		UT_ASSERT_EQ(summary.top_aborts | summary.child_records | summary.clog_truncates, 0);
		UT_ASSERT(flags & CLUSTER_RECOVERY_RELMAP_FOLD_WAL_MATCH);
		for (unsigned field = 0; field < 5; ++field) {
			changed = pending;
			if (field == 0) {
				changed.pending.mappings[3].mapfilenumber++;
				INIT_CRC32C(changed.pending.crc);
				COMP_CRC32C(changed.pending.crc, &changed.pending, offsetof(RelMapFile, crc));
				FIN_CRC32C(changed.pending.crc);
			} else if (field == 1)
				changed.pending_owner.wal_end++;
			else if (field == 2)
				changed.pending_owner.admitted_incarnation++;
			else if (field == 3)
				changed.pending_owner.top_full_xid += UINT64_C(1) << 32;
			else {
				changed.pending_owner.membership_epoch++;
				changed.pending_owner.boot_incarnation++;
			}
			UT_ASSERT_EQ(relmap_fold_drain(inventory, &changed, &summary, &flags),
						 CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
			/* Native WAL does not encode FullXid era/epoch/boot. Its byte
			 * match must never be promoted to those missing qualifications. */
			UT_ASSERT_EQ(summary.wal_matches, field >= 3 ? 1 : 0);
			UT_ASSERT_EQ(summary.wal_aliases, field >= 3 ? 0 : 1);
		}
	}
	UT_ASSERT_EQ(cluster_recovery_native_inventory_next(inventory, &effect),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT);
	UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_TERMINAL);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_terminal_step(inventory, &terminal, &diag),
				 CLUSTER_RECOVERY_TERMINAL_ENTRY);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	cluster_recovery_native_inventory_destroy(inventory);
	cold_census_done(&inputs);
	inventory_extra = 0;
}

UT_TEST(test_relmap_fold_keeps_terminals_children_and_lifecycle_obligations)
{
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryRelmapFoldSummary summary;
	uint32 flags;

	inventory_extra = 18;
	inputs = inventory_fixture(false, false);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	for (unsigned node = 0; node <= 127; node += 127) {
		ClusterRelmapV2Image pending = relmap_fold_pending(inventory, node);
		UT_ASSERT_EQ(relmap_fold_drain(inventory, &pending, &summary, &flags),
					 CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
		UT_ASSERT_EQ(summary.top_commits, 1);
		UT_ASSERT_EQ(summary.top_aborts, 1);
		UT_ASSERT_EQ(summary.assignments, 1);
		UT_ASSERT_EQ(summary.prepares, 1);
		UT_ASSERT_EQ(summary.record_top_xid_records, 1);
		UT_ASSERT(flags & CLUSTER_RECOVERY_RELMAP_FOLD_RECORD_TOP_XID);
		UT_ASSERT_EQ(summary.child_records, 2);
		UT_ASSERT_EQ(summary.child_references, 3);
		UT_ASSERT_EQ(summary.clog_zeroes, 1);
		UT_ASSERT_EQ(summary.clog_truncates, 1);
		UT_ASSERT_EQ(summary.wal_matches, 1);
		UT_ASSERT_EQ(
			flags
				& (CLUSTER_RECOVERY_RELMAP_FOLD_COMMIT | CLUSTER_RECOVERY_RELMAP_FOLD_ABORT
				   | CLUSTER_RECOVERY_RELMAP_FOLD_ASSIGNMENT | CLUSTER_RECOVERY_RELMAP_FOLD_PREPARE
				   | CLUSTER_RECOVERY_RELMAP_FOLD_CLOG_ZERO
				   | CLUSTER_RECOVERY_RELMAP_FOLD_CLOG_TRUNCATE),
			CLUSTER_RECOVERY_RELMAP_FOLD_COMMIT | CLUSTER_RECOVERY_RELMAP_FOLD_ABORT
				| CLUSTER_RECOVERY_RELMAP_FOLD_ASSIGNMENT | CLUSTER_RECOVERY_RELMAP_FOLD_PREPARE
				| CLUSTER_RECOVERY_RELMAP_FOLD_CLOG_ZERO
				| CLUSTER_RECOVERY_RELMAP_FOLD_CLOG_TRUNCATE);
	}
	cluster_recovery_native_inventory_destroy(inventory);
	cold_census_done(&inputs);
	inventory_extra = 0;
}

UT_TEST(test_relmap_fold_never_discards_historical_raw_aliases)
{
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryRelmapFoldSummary summary;
	ClusterRelmapV2Image pending;
	ClusterRecoveryNativeEffect primary;
	NativeInventoryRecord *copy;
	DecodedXLogRecord decoded = { 0 };
	XLogReaderState reader = { 0 };
	ClusterWalSourceRef historical;
	RfContributorStreamCutV1 cut;
	uint32 flags;

	inventory_extra = 14;
	inputs = inventory_fixture(false, false);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	pending = relmap_fold_pending(inventory, 0);
	UT_ASSERT_EQ(cluster_recovery_native_scan_primary(inventory->head->next->scan, &primary),
				 CLUSTER_RECOVERY_NATIVE_EFFECT);
	UT_ASSERT_EQ(primary.kind, CLUSTER_RECOVERY_NATIVE_RELMAP_UPDATE);
	/* Structural generation-collision fixture, not an actual new retained
	 * physical generation or qualified history/lifetime proof. */
	historical = primary.record->source;
	historical.claim.identity.origin_owner_incarnation--;
	historical.claim.identity.root_lineage_seq++;
	cut = primary.record->cut;
	cut.origin_owner_incarnation = historical.claim.identity.origin_owner_incarnation;
	decoded.header = primary.record->header;
	decoded.lsn = primary.record->read_lsn;
	decoded.next_lsn = primary.record->end_lsn;
	decoded.main_data = (char *)primary.record->payload;
	decoded.main_data_len = primary.record->payload_length;
	decoded.max_block_id = -1;
	reader.record = &decoded;
	reader.ReadRecPtr = decoded.lsn;
	reader.EndRecPtr = decoded.next_lsn;
	reader.system_identifier = TEST_SYSID;
	copy = calloc(1, sizeof(*copy));
	UT_ASSERT(copy != NULL);
	UT_ASSERT_EQ(cluster_recovery_native_scan_begin(&reader, &historical, &cut, 65536, &copy->scan),
				 CLUSTER_RECOVERY_NATIVE_MORE);
	/* The original source array has no spare slot: substitute the other
	 * origin's full descriptor, retaining its original ordinal position. */
	for (NativeInventoryRecord *r = inventory->head; r != NULL && r->next != NULL;) {
		if (r->next->source_index == 1) {
			NativeInventoryRecord *old = r->next;
			r->next = old->next;
			cluster_recovery_native_scan_destroy(old->scan);
			free(old);
		} else
			r = r->next;
	}
	inventory->tail = inventory->head;
	while (inventory->tail->next != NULL)
		inventory->tail = inventory->tail->next;
	inventory->sources[1].source = historical;
	inventory->sources[1].cut = cut;
	copy->source_index = 1;
	copy->ordinal = 2;
	inventory->tail->next = copy;
	inventory->tail = copy;
	UT_ASSERT_EQ(relmap_fold_drain(inventory, &pending, &summary, &flags),
				 CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	UT_ASSERT_EQ(summary.sources, 2);
	UT_ASSERT_EQ(summary.origin_sources, 2);
	UT_ASSERT_EQ(summary.wal_matches, 1);
	UT_ASSERT_EQ(summary.wal_aliases, 1);
	UT_ASSERT(flags & CLUSTER_RECOVERY_RELMAP_FOLD_WAL_ALIAS);
	cluster_recovery_native_inventory_destroy(inventory);
	cold_census_done(&inputs);
	inventory_extra = 0;
}

UT_TEST(test_relmap_fold_is_bounded_and_pins_the_complete_query)
{
	for (unsigned fault = 0; fault < 4; ++fault) {
		ClusterWalInputsV1 *inputs;
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryRelmapFoldEntry entry, zero = { 0 };
		ClusterRecoveryRelmapFoldSummary summary, empty = { 0 };
		ClusterRecoveryRelmapFoldResult result;
		ClusterRelmapV2Image pending;
		uint64 examined = 0;
		unsigned calls = 0, more = 0;
		inventory_extra = 18;
		inputs = inventory_fixture(false, false);
		UT_ASSERT_EQ(
			cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
			CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
		pending = relmap_fold_pending(inventory, 0);
		do {
			result = cluster_recovery_native_inventory_relmap_step(inventory, &pending, &entry,
																   &summary, &diag);
			UT_ASSERT(diag.relmap_examined - examined <= 128);
			examined = diag.relmap_examined;
			calls++;
			if (result == CLUSTER_RECOVERY_RELMAP_FOLD_MORE) {
				more++;
				UT_ASSERT_EQ(memcmp(&entry, &zero, sizeof(entry)), 0);
				UT_ASSERT_EQ(memcmp(&summary, &empty, sizeof(summary)), 0);
				if (fault == 1)
					pending.pending_owner.boot_incarnation++;
				if (fault == 2) {
					pending.committed_generation++;
					pending.pending_generation++;
				}
				if (fault == 3)
					pending.pending_owner.top_full_xid += UINT64_C(1) << 32;
			}
		} while (result <= CLUSTER_RECOVERY_RELMAP_FOLD_ENTRY && calls < 100);
		if (fault) {
			UT_ASSERT_EQ(result, CLUSTER_RECOVERY_RELMAP_FOLD_INVALID);
			UT_ASSERT_EQ(memcmp(&entry, &zero, sizeof(entry)), 0);
			UT_ASSERT_EQ(memcmp(&summary, &empty, sizeof(summary)), 0);
			UT_ASSERT(!cluster_recovery_native_inventory_digest(inventory, (uint8[32]){ 0 }));
		} else {
			UT_ASSERT_EQ(result, CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
			UT_ASSERT(more >= 2 && calls < 100 && examined >= 300);
			UT_ASSERT_EQ(summary.child_references, 3);
		}
		cluster_recovery_native_inventory_destroy(inventory);
		cold_census_done(&inputs);
	}
	inventory_extra = 0;
}

UT_TEST(test_relmap_fold_refusal_and_final_drift_revoke_dependents)
{
	for (unsigned fault = 0; fault < 3; ++fault) {
		ClusterWalInputsV1 *inputs;
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryRelmapFoldEntry entry, zero = { 0 };
		ClusterRecoveryRelmapFoldSummary summary, empty = { 0 };
		ClusterRecoveryReferenceGraph *graph = wal_root_empty_graph(1024 * 1024);
		ClusterRelmapV2Image pending;
		uint32 flags;
		inventory_extra = 14;
		inputs = inventory_fixture(false, false);
		UT_ASSERT_EQ(
			cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
			CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
		pending = relmap_fold_pending(inventory, 0);
		UT_ASSERT_EQ(wal_root_seed(inventory, graph, &diag), CLUSTER_RECOVERY_GRAPH_DRAINED);
		UT_ASSERT_EQ(relmap_fold_drain(inventory, &pending, &summary, &flags),
					 CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
		if (fault == 0) {
			inventory_relmap_change_family = false;
			inventory_relmap_drift();
			UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag),
						 CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED);
		} else if (fault == 1)
			pending.pending.magic++;
		else
			inventory->diag.relmap_examined = UINT64_MAX;
		UT_ASSERT(cluster_recovery_native_inventory_relmap_step(inventory, &pending, &entry,
																&summary, &diag)
				  >= CLUSTER_RECOVERY_RELMAP_FOLD_INVALID);
		UT_ASSERT_EQ(memcmp(&entry, &zero, sizeof(entry)), 0);
		UT_ASSERT_EQ(memcmp(&summary, &empty, sizeof(summary)), 0);
		UT_ASSERT(!cluster_recovery_native_inventory_digest(inventory, (uint8[32]){ 0 }));
		UT_ASSERT(!cluster_recovery_reference_graph_accepts_roots(graph));
		cluster_recovery_native_inventory_destroy(inventory);
		cluster_recovery_reference_graph_destroy(graph);
		cold_census_done(&inputs);
	}
	inventory_extra = 0;
}

UT_TEST(test_relmap_fold_counts_empty_origins_and_refuses_invalid_queries)
{
	for (unsigned fault = 0; fault < 5; ++fault) {
		ClusterWalInputsV1 *inputs;
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryRelmapFoldSummary summary;
		ClusterRecoveryRelmapFoldEntry entry;
		ClusterRelmapV2Image pending = { 0 };
		char bytes[TEST_RELMAP_LENGTH];
		uint32 flags;
		int saved_node = cluster_node_id;
		uint64 saved_incarnation = test_self_incarnation;
		uint32 selected;
		unsigned empty_source = 0;
		inventory_extra = 0;
		if (fault == 0) {
			ClusterRecoverySerialGuard serial;
			ClusterThreadRecLaunchEligibility eligibility;
			ControlRootImage root;
			ControlFileData control;
			ClusterControlRootFileToken token;
			(void)pending_worker_fixture(&serial, &eligibility);
			test_worker_pin_held = false;
			test_history_sync_count = 0;
			test_history_fail_sync = UINT_MAX;
			UT_ASSERT_EQ(thread_recovery_worker_run(&eligibility), CLUSTER_THREADREC_DONE);
			test_history_fail_sync = 0;
			test_actual_cf = test_cf_mode = ShareLock;
			UT_ASSERT_EQ(cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID, &root,
																	 &control, &token),
						 0);
			memset(&root.startup[3], 0, sizeof(root.startup[3]));
			UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0);
			v2_write_roots(root.bytes);
			test_actual_cf = test_cf_mode = NoLock;
			test_inputs_startup_cf = true;
			UT_ASSERT_EQ(inputs_begin_for_role(true, &inputs), 0);
		} else
			inputs = inventory_fixture(false, false);
		selected = cluster_wal_inputs_count_v1(inputs);
		UT_ASSERT_EQ(
			cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
			CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
		for (uint32 i = 0; i < selected; ++i)
			if (inventory->sources[i].cut.flags & RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY)
				empty_source = i;
		pending.identity = (ClusterRelmapV2Namespace){
			TEST_SYSID, inventory->sources[0].source.claim.database_incarnation, InvalidOid, true
		};
		pending.committed_generation = 7;
		pending.pending_generation = 8;
		pending.pending_owner = (ClusterRelmapV2Owner){
			inventory->sources[empty_source].source.claim.identity.origin_node_id,
			(UINT64_C(3) << 32) | 4195136,
			9,
			inventory->sources[empty_source].source.claim.identity.origin_owner_incarnation,
			123456,
			17
		};
		test_relmap_payload(bytes, true, 2);
		memcpy(&pending.pending, bytes + MinSizeOfRelmapUpdate, sizeof(RelMapFile));
		pending.committed = pending.pending;
		if (fault == 0) {
			UT_ASSERT_EQ(relmap_fold_drain(inventory, &pending, &summary, &flags),
						 CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
			UT_ASSERT_EQ(summary.sources, selected);
			UT_ASSERT(summary.origin_sources >= 1);
			UT_ASSERT(inventory->sources[empty_source].cut.flags
					  & RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY);
			UT_ASSERT_EQ(summary.wal_matches | summary.top_commits | summary.clog_truncates, 0);
			UT_ASSERT_EQ(flags, 0);
		} else {
			if (fault == 1)
				pending.identity.database_incarnation++;
			if (fault == 2)
				pending.pending_owner.node = 42;
			if (fault == 3)
				pending.pending_generation = 0;
			UT_ASSERT_EQ(cluster_recovery_native_inventory_relmap_step(
							 inventory, fault == 4 ? NULL : &pending, &entry, &summary, &diag),
						 CLUSTER_RECOVERY_RELMAP_FOLD_INVALID);
			UT_ASSERT(!cluster_recovery_native_inventory_digest(inventory, (uint8[32]){ 0 }));
		}
		cluster_recovery_native_inventory_destroy(inventory);
		cold_census_done(&inputs);
		test_reserve_mode = false;
		cluster_node_id = saved_node;
		test_self_incarnation = saved_incarnation;
	}
}
