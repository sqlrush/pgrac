/* Actual cold-reader transcripts, including non-native records and targets.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */

UT_TEST(test_native_inventory_detects_non_native_rewrite)
{
	for (inventory_extra = 1; inventory_extra <= 2; inventory_extra++) {
		ClusterWalInputsV1 *inputs = inventory_fixture(false, false);
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;

		inventory_fault = inventory_extra + 5;
		inventory_between_passes = inventory_drift;
		UT_ASSERT_EQ(
			cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
			CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED);
		UT_ASSERT_EQ(inventory_census_calls, 2);
		UT_ASSERT(inventory == NULL);
		cluster_recovery_native_inventory_destroy(inventory);
		inventory_between_passes = NULL;
		cold_census_done(&inputs);
	}
	inventory_extra = 0;
}

/* A separate held physical root: file observation is not authority. Both
 * original input passes still read the actual ROOT/claim/WAL fixtures. */
static ClusterRecoveryRootSet *
inventory_roots(ClusterWalInputsV1 *inputs, char path[MAXPGPATH], int *fd, bool mismatch)
{
	const ClusterWalSourceRef *source = &cluster_wal_inputs_at_v1(inputs, 0)->source;
	ClusterRecoveryRootSet *roots = NULL;
	int file;

	strlcpy(path, "/tmp/pgrac-native-roots-XXXXXX", MAXPGPATH);
	UT_ASSERT(mkdtemp(path) != NULL);
	*fd = open(path, O_RDONLY | O_DIRECTORY);
	UT_ASSERT(*fd >= 0);
	UT_ASSERT_EQ(mkdirat(*fd, "base", 0700), 0);
	UT_ASSERT_EQ(mkdirat(*fd, "base/5", 0700), 0);
	file = openat(*fd, "base/5/99999", O_CREAT | O_WRONLY, 0600);
	UT_ASSERT(file >= 0);
	UT_ASSERT_EQ(write(file, "unreferenced", 12), 12);
	UT_ASSERT_EQ(close(file), 0);
	UT_ASSERT_EQ(cluster_recovery_root_set_begin(*fd, source->claim.identity.system_identifier,
												 source->claim.database_incarnation + mismatch,
												 source->claim.identity.storage_uuid, 1024 * 1024,
												 &roots),
				 CLUSTER_RECOVERY_ROOTS_MORE);
	return roots;
}

static void
inventory_roots_done(ClusterRecoveryRootSet *roots, int fd, const char *path)
{
	cluster_recovery_root_set_destroy(roots);
	UT_ASSERT_EQ(unlinkat(fd, "base/5/99999", 0), 0);
	UT_ASSERT_EQ(unlinkat(fd, "base/5", AT_REMOVEDIR), 0);
	UT_ASSERT_EQ(unlinkat(fd, "base", AT_REMOVEDIR), 0);
	UT_ASSERT_EQ(close(fd), 0);
	UT_ASSERT_EQ(rmdir(path), 0);
}

UT_TEST(test_native_inventory_collects_real_fsm_and_all_physical_roots)
{
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryRootSet *roots;
	ClusterRecoveryRootsEvent event;
	ClusterRecoveryRootsResult result = CLUSTER_RECOVERY_ROOTS_MORE;
	unsigned fsm = 0, create = 0, untouched = 0;
	uint8 digest[32];
	char path[MAXPGPATH];
	int fd;

	inventory_extra = 2;
	inputs = inventory_fixture(false, false);
	roots = inventory_roots(inputs, path, &fd, false);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_collect_roots(inputs, roots, 1024 * 1024,
																 &inventory, &diag),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	UT_ASSERT_EQ(inventory_census_calls, 2);
	UT_ASSERT_EQ(diag.records, 8);
	UT_ASSERT_EQ(diag.native_records, 4);
	UT_ASSERT_EQ(diag.other_records, 4);
	UT_ASSERT_EQ(diag.rebuildable_targets, 2);
	UT_ASSERT_EQ(diag.unowned_target_records, 2); /* checkpoint effects still unowned */
	if (inventory != NULL) {
		for (unsigned i = 0; i < 10000 && result < CLUSTER_RECOVERY_ROOTS_OBSERVED; i++)
			result = cluster_recovery_root_set_step(roots, &event);
		UT_ASSERT_EQ(result, CLUSTER_RECOVERY_ROOTS_OBSERVED);
		UT_ASSERT(cluster_recovery_root_set_digest(roots, digest));
		for (uint32 i = 0; i < cluster_recovery_root_set_count(roots); i++) {
			const ClusterRecoveryRootEntry *entry = cluster_recovery_root_set_entry(roots, i);
			if (entry->file.kind != CLUSTER_RECOVERY_FILE_RELATION)
				continue;
			if (entry->file.locator.relNumber == 16386) {
				fsm++;
				UT_ASSERT_EQ(entry->file.forknum, FSM_FORKNUM);
				UT_ASSERT_EQ(entry->required_blocks, 32);
				UT_ASSERT_EQ(entry->origins, CLUSTER_RECOVERY_ROOT_FROM_WAL);
			} else if (entry->file.locator.relNumber == 16385) {
				create++;
				UT_ASSERT_EQ(entry->required_blocks, 0);
				UT_ASSERT_EQ(entry->origins, CLUSTER_RECOVERY_ROOT_FROM_WAL);
			} else if (entry->file.locator.relNumber == 99999) {
				untouched++;
				UT_ASSERT_EQ(entry->origins, CLUSTER_RECOVERY_ROOT_PHYSICAL);
			}
		}
		UT_ASSERT_EQ(fsm, 1);
		UT_ASSERT_EQ(create, 1);
		UT_ASSERT_EQ(untouched, 1);
	}
	inventory_roots_done(roots, fd, path);
	cluster_recovery_native_inventory_destroy(inventory);
	cold_census_done(&inputs);
	inventory_extra = 0;
}

static void
inventory_undo_drift(void)
{
	xl_undo_tt_slot_bind binding = { 0 };
	binding.segment_id = 2;
	binding.instance = 1;
	binding.xid = 4195136;
	binding.wrap = 1;
	binding.format_version = CLUSTER_UNDO_TT_BIND_VERSION;
	(void)inventory_write_record(&inventory_fault_source, inventory_extra_start,
								 inventory_extra_previous, RM_CLUSTER_UNDO_ID,
								 XLOG_UNDO_TT_SLOT_BIND, &binding, sizeof(binding));
}

UT_TEST(test_native_inventory_collects_undo_targets_from_real_retained_wal)
{
	for (unsigned variant = 0; variant < 2; variant++) {
		ClusterWalInputsV1 *inputs;
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryRootSet *roots;
		ClusterRecoveryRootsEvent event;
		ClusterRecoveryRootsResult result = CLUSTER_RECOVERY_ROOTS_MORE;
		uint8 digest[32];
		char path[MAXPGPATH];
		int fd;
		unsigned undo = 0;

		inventory_extra = 3;
		inputs = inventory_fixture(false, false);
		roots = inventory_roots(inputs, path, &fd, false);
		if (variant)
			inventory_between_passes = inventory_undo_drift;
		UT_ASSERT_EQ(cluster_recovery_native_inventory_collect_roots(inputs, roots, 1024 * 1024,
																	 &inventory, &diag),
					 variant ? CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED
							 : CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
		UT_ASSERT_EQ(inventory_census_calls, 2);
		if (variant) {
			UT_ASSERT(inventory == NULL);
			UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
			UT_ASSERT(cluster_recovery_root_set_step(roots, &event)
					  > CLUSTER_RECOVERY_ROOTS_OBSERVED);
		} else {
			UT_ASSERT_EQ(diag.undo_targets, 2);
			UT_ASSERT_EQ(diag.unowned_target_records, 4);
			for (unsigned i = 0; i < 10000 && result < CLUSTER_RECOVERY_ROOTS_OBSERVED; i++)
				result = cluster_recovery_root_set_step(roots, &event);
			UT_ASSERT_EQ(result, CLUSTER_RECOVERY_ROOTS_OBSERVED);
			for (uint32 i = 0; i < cluster_recovery_root_set_count(roots); i++) {
				const ClusterRecoveryRootEntry *entry = cluster_recovery_root_set_entry(roots, i);
				if (entry->file.kind != CLUSTER_RECOVERY_FILE_UNDO_SEGMENT)
					continue;
				undo++;
				UT_ASSERT(entry->file.undo_segment == 1 || entry->file.undo_segment == 32513);
				UT_ASSERT_EQ(entry->origins, CLUSTER_RECOVERY_ROOT_FROM_WAL);
				UT_ASSERT_EQ(entry->required_blocks, 1);
			}
			UT_ASSERT_EQ(undo, 2);
		}
		inventory_between_passes = NULL;
		inventory_roots_done(roots, fd, path);
		cluster_recovery_native_inventory_destroy(inventory);
		cold_census_done(&inputs);
	}
	inventory_extra = 0;
}

UT_TEST(test_native_inventory_poison_targets_on_error_and_drift)
{
	for (unsigned mode = 0; mode < 8; mode++) {
		ClusterWalInputsV1 *inputs;
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryRootSet *roots;
		ClusterRecoveryRootsEvent event;
		char path[MAXPGPATH];
		uint8 digest[32];
		int fd;
		bool threw = false;

		inventory_extra = 2;
		inputs = inventory_fixture(mode == 5, false);
		roots = inventory_roots(inputs, path, &fd, mode == 1);
		if (mode == 6)
			(void)cluster_recovery_root_set_step(roots, &event);
		inventory_fault = mode == 2 ? 4 : 7;
		if (mode == 0 || mode == 2)
			inventory_between_passes = inventory_drift;
		PG_TRY();
		{
			UT_ASSERT_NE(cluster_recovery_native_inventory_collect_roots(
							 mode == 3 ? NULL : inputs, roots, mode == 4 ? 1 : 1024 * 1024,
							 &inventory, mode == 7 ? NULL : &diag),
						 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(threw, mode == 2);
		UT_ASSERT(inventory == NULL);
		UT_ASSERT(cluster_recovery_root_set_step(roots, &event) > CLUSTER_RECOVERY_ROOTS_OBSERVED);
		UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
		inventory_between_passes = NULL;
		inventory_roots_done(roots, fd, path);
		cluster_recovery_native_inventory_destroy(inventory);
		cold_census_done(&inputs);
	}
	inventory_extra = 0;
}

static bool
transcript_hash(XLogReaderState *reader, uint8 out[32])
{
	pg_cryptohash_ctx *hash = pg_cryptohash_create(PG_SHA256);
	bool result = hash != NULL && pg_cryptohash_init(hash) == 0
				  && cluster_recovery_record_hash_update(hash, reader)
				  && pg_cryptohash_final(hash, out, 32) == 0;
	pg_cryptohash_free(hash);
	return result;
}

UT_TEST(test_record_transcript_full_fields_and_storage_independence)
{
	DecodedXLogRecord *record = calloc(1, sizeof(*record) + sizeof(DecodedBkpBlock));
	XLogReaderState reader = { .record = record };
	char main_data[6] = "main", data[6] = "data", image[6] = "page";
	uint8 baseline[32] = { 0 }, changed[32] = { 0 };
	DecodedBkpBlock *block = &record->blocks[0];
	RfPageVersionEdgeEntryV1 *edge = &record->page_version_edge.entries[0];

	record->max_block_id = 0;
	record->main_data = main_data;
	record->main_data_len = 5;
	record->has_page_version_edge = true;
	record->page_version_edge.entry_count = 1;
	block->in_use = block->has_image = block->has_data = true;
	block->bkp_image = image;
	block->bimg_len = 5;
	block->data = data;
	block->data_len = 5;
	UT_ASSERT(transcript_hash(&reader, baseline));
	/* Deliberately keep header CRC unchanged while mutating each full byte
	 * carrier: the hash is not just CRC or a pointer identity comparison. */
#define TRANSCRIPT_CHANGE(field)                                                                   \
	do {                                                                                           \
		(field)++;                                                                                 \
		UT_ASSERT(transcript_hash(&reader, changed));                                              \
		UT_ASSERT(memcmp(baseline, changed, 32) != 0);                                             \
		(field)--;                                                                                 \
	} while (0)
	TRANSCRIPT_CHANGE(record->lsn);
	TRANSCRIPT_CHANGE(record->next_lsn);
	TRANSCRIPT_CHANGE(record->header.xl_tot_len);
	TRANSCRIPT_CHANGE(record->header.xl_xid);
	TRANSCRIPT_CHANGE(record->header.xl_prev);
	TRANSCRIPT_CHANGE(record->header.xl_scn);
	TRANSCRIPT_CHANGE(record->header.xl_info);
	TRANSCRIPT_CHANGE(record->header.xl_rmid);
	TRANSCRIPT_CHANGE(record->header.xl_crc);
	TRANSCRIPT_CHANGE(record->record_origin);
	TRANSCRIPT_CHANGE(record->toplevel_xid);
	TRANSCRIPT_CHANGE(main_data[3]);
	TRANSCRIPT_CHANGE(record->main_data_len);
	TRANSCRIPT_CHANGE(block->rlocator.spcOid);
	TRANSCRIPT_CHANGE(block->rlocator.dbOid);
	TRANSCRIPT_CHANGE(block->rlocator.relNumber);
	TRANSCRIPT_CHANGE(block->forknum);
	TRANSCRIPT_CHANGE(block->blkno);
	TRANSCRIPT_CHANGE(block->flags);
	TRANSCRIPT_CHANGE(block->component_ordinal);
	TRANSCRIPT_CHANGE(block->hole_offset);
	TRANSCRIPT_CHANGE(block->hole_length);
	TRANSCRIPT_CHANGE(block->bimg_info);
	TRANSCRIPT_CHANGE(image[3]);
	TRANSCRIPT_CHANGE(block->bimg_len);
	TRANSCRIPT_CHANGE(data[3]);
	TRANSCRIPT_CHANGE(block->data_len);
	TRANSCRIPT_CHANGE(record->page_version_edge.result_token);
	TRANSCRIPT_CHANGE(edge->block_id);
	TRANSCRIPT_CHANGE(edge->page_class);
	TRANSCRIPT_CHANGE(edge->before_kind);
	TRANSCRIPT_CHANGE(edge->result_kind);
	TRANSCRIPT_CHANGE(edge->edge_flags);
	TRANSCRIPT_CHANGE(edge->component_ordinal);
	TRANSCRIPT_CHANGE(edge->before.segment_incarnation[15]);
	TRANSCRIPT_CHANGE(edge->before.mutation_token);
	TRANSCRIPT_CHANGE(edge->result_incarnation[15]);
#undef TRANSCRIPT_CHANGE
	block->apply_image = true;
	UT_ASSERT(transcript_hash(&reader, changed));
	UT_ASSERT(memcmp(baseline, changed, 32) != 0);
	block->apply_image = false;
	block->has_image = false;
	UT_ASSERT(transcript_hash(&reader, changed));
	UT_ASSERT(memcmp(baseline, changed, 32) != 0);
	block->has_image = true;
	block->has_data = false;
	UT_ASSERT(transcript_hash(&reader, changed));
	UT_ASSERT(memcmp(baseline, changed, 32) != 0);
	block->has_data = true;
	record->has_page_version_edge = false;
	UT_ASSERT(transcript_hash(&reader, changed));
	UT_ASSERT(memcmp(baseline, changed, 32) != 0);
	record->has_page_version_edge = true;
	/* Decoder allocation and prefetch metadata must not create false drift. */
	record->size++;
	record->oversized = true;
	record->next = record;
	block->prefetch_buffer++;
	block->data_bufsz++;
	record->main_data = strdup(main_data);
	UT_ASSERT(transcript_hash(&reader, changed));
	UT_ASSERT_EQ(memcmp(baseline, changed, 32), 0);
	free(record->main_data);
	free(record);
}

UT_TEST(test_record_transcript_rejects_unsafe_bounds)
{
	DecodedXLogRecord record = { .max_block_id = -1 };
	XLogReaderState reader = { .record = &record };
	static const uint8 empty_digest[32]
		= { 0x98, 0x5f, 0x43, 0x49, 0x0d, 0x82, 0xbc, 0x49, 0x43, 0x9d, 0xf6,
			0x70, 0xb0, 0x12, 0x12, 0x4b, 0x71, 0x2d, 0xa9, 0xa8, 0xfb, 0xef,
			0x9f, 0xaa, 0x62, 0x6e, 0x04, 0xa1, 0x3d, 0x04, 0x8f, 0xee };
	uint8 digest[32];

	UT_ASSERT(transcript_hash(&reader, digest));
	/* SHA-256(domain including NUL, followed by fourteen zero LE64s). */
	UT_ASSERT_EQ(memcmp(digest, empty_digest, 32), 0);
	record.max_block_id = XLR_MAX_BLOCK_ID + 1;
	UT_ASSERT(!transcript_hash(&reader, digest));
	record.max_block_id = -2;
	UT_ASSERT(!transcript_hash(&reader, digest));
	record.max_block_id = -1;
	record.main_data_len = 1;
	UT_ASSERT(!transcript_hash(&reader, digest));
	record.main_data_len = 0;
	record.has_page_version_edge = true;
	record.page_version_edge.entry_count = XLR_PAGE_VERSION_EDGE_MAX_ENTRIES + 1;
	UT_ASSERT(!transcript_hash(&reader, digest));
	reader.record = NULL;
	UT_ASSERT(!transcript_hash(&reader, digest));
	UT_ASSERT(!transcript_hash(NULL, digest));
}

UT_TEST(test_final_input_recheck_preserves_effect_cursor_and_all_sources)
{
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryNativeEffect effect;
	unsigned effects = 0;

	inventory_extra = 2;
	inputs = inventory_fixture(false, false);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_next(inventory, &effect),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	UT_ASSERT_EQ(inventory_census_calls, 3);
	while (cluster_recovery_native_inventory_next(inventory, &effect)
		   == CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT)
		effects++;
	UT_ASSERT_EQ(effects, 7); /* not the already-consumed terminal again */
	UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	UT_ASSERT_EQ(inventory_census_calls, 4);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_next(inventory, &effect),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_END);
	cluster_recovery_native_inventory_destroy(inventory);
	cold_census_done(&inputs);
	inventory_extra = 0;
}

UT_TEST(test_final_input_recheck_is_sticky_on_bytes_scope_io_and_error)
{
	for (unsigned mode = 0; mode < 6; mode++) {
		ClusterWalInputsV1 *inputs;
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryNativeEffect effect;
		bool threw = false;

		inventory_extra = 1;
		inputs = inventory_fixture(false, false);
		UT_ASSERT_EQ(
			cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
			CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
		inventory_fault = mode == 0 ? 0 : mode == 1 ? 6 : mode == 2 ? 1 : mode == 3 ? 3 : 4;
		if (mode < 4)
			inventory_drift();
		else if (mode == 4) {
			inventory_census_calls = 0;
			inventory_between_passes = inventory_drift;
		}
		PG_TRY();
		{
			UT_ASSERT_NE(cluster_recovery_native_inventory_recheck(mode == 5 ? NULL : inputs,
																   inventory, &diag),
						 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(threw, mode == 4);
		inventory_between_passes = NULL;
		inputs_pin_current = true;
		UT_ASSERT_NE(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag),
					 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
		memset(&effect, 0xa5, sizeof(effect));
		UT_ASSERT_EQ(cluster_recovery_native_inventory_next(inventory, &effect),
					 CLUSTER_RECOVERY_NATIVE_INVENTORY_INVALID);
		UT_ASSERT(effect.record == NULL && effect.bytes == NULL);
		cluster_recovery_native_inventory_destroy(inventory);
		cold_census_done(&inputs);
	}
	inventory_extra = 0;
}


UT_TEST(test_cold_inventory_owns_undo_records_and_rechecks_after_export)
{
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryNativeEffect effect;
	ClusterRecoveryNativeInventoryResult result;
	unsigned undo_count = 0;
	inventory_extra = 3;
	inputs = inventory_fixture(false, false);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	UT_ASSERT_EQ(diag.records, 8);
	UT_ASSERT_EQ(diag.native_records, 4);
	UT_ASSERT_EQ(diag.undo_records, 2);
	UT_ASSERT_EQ(diag.other_records, 2); /* real checkpoint records still need owner */
	if (inventory != NULL) {
		while ((result = cluster_recovery_native_inventory_next(inventory, &effect))
			   == CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT) {
			if (effect.kind == CLUSTER_RECOVERY_NATIVE_UNDO) {
				ClusterUndoDecoded undo;
				xl_undo_tt_slot_bind binding;
				UT_ASSERT_EQ(effect.length, sizeof(undo));
				memcpy(&undo, effect.bytes, sizeof(undo));
				memcpy(&binding, effect.record->payload, sizeof(binding));
				UT_ASSERT_EQ(undo.kind, CLUSTER_UNDO_KIND_TT_BIND);
				UT_ASSERT_EQ(undo.expected_generation, 0);
				UT_ASSERT_EQ(undo.instance, effect.record->source.claim.identity.origin_thread_id);
				UT_ASSERT_EQ(undo.segment_id, binding.segment_id);
				UT_ASSERT_EQ(undo.xid, binding.xid);
				UT_ASSERT(effect.record->cut.scan_begin_inclusive <= effect.record->read_lsn);
				UT_ASSERT(effect.record->cut.scan_end_exclusive >= effect.record->end_lsn);
				undo_count++;
			}
		}
		UT_ASSERT_EQ(result, CLUSTER_RECOVERY_NATIVE_INVENTORY_END);
		UT_ASSERT_EQ(undo_count, 2);
		UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag),
					 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
		UT_ASSERT_EQ(cluster_recovery_native_inventory_next(inventory, &effect),
					 CLUSTER_RECOVERY_NATIVE_INVENTORY_END);
		inventory_undo_drift();
		UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag),
					 CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED);
		UT_ASSERT_EQ(cluster_recovery_native_inventory_next(inventory, &effect),
					 CLUSTER_RECOVERY_NATIVE_INVENTORY_INVALID);
		UT_ASSERT(effect.record == NULL && effect.bytes == NULL);
	}
	cluster_recovery_native_inventory_destroy(inventory);
	cold_census_done(&inputs);
	inventory_extra = 0;
}

UT_TEST(test_cold_inventory_does_not_hide_invalid_undo_in_other_records)
{
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	inventory_extra = 4;
	inputs = inventory_fixture(false, false);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_UNSUPPORTED);
	UT_ASSERT(inventory == NULL);
	cluster_recovery_native_inventory_destroy(inventory);
	cold_census_done(&inputs);
	inventory_extra = 0;
}
