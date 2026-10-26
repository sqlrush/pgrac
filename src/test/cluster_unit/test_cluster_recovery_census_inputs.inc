/* One original cold scope owns the existing observation consumers.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */

/* Two required CLOG pages add 65536 one-slot observation steps; the optional
 * three-family fixture adds 35635 more. This bounds fixture calls, not time. */
static unsigned owned_target_mode;
static unsigned owned_clog_fault;
static bool owned_native_slru;
static bool owned_terminals;
static bool owned_page_roots_fixture;
static void owned_slru_files(int fd, bool create);

/* Both retained terminal records name xid4195136, CLOG page128/segment4.
 * Supply real files for the physical census, not a missing-target bypass. */
static void
owned_clog_files(int fd, bool create)
{
	char path[MAXPGPATH];
	PGAlignedBlock page = { 0 };
	for (unsigned i = 0; i < 2; ++i) {
		unsigned node = i ? 127 : 0;
		snprintf(path, sizeof(path), "native_side/origin_%u/pg_xact", node);
		if (create)
			UT_ASSERT_EQ(mkdirat(fd, path, 0700), 0);
		snprintf(path, sizeof(path), "native_side/origin_%u/pg_xact/0004", node);
		if (create) {
			int file = openat(fd, path, O_CREAT | O_WRONLY, 0600);
			UT_ASSERT(file >= 0);
			UT_ASSERT_EQ(write(file, page.data, BLCKSZ), BLCKSZ);
			UT_ASSERT_EQ(close(file), 0);
		} else {
			UT_ASSERT_EQ(unlinkat(fd, path, 0), 0);
			snprintf(path, sizeof(path), "native_side/origin_%u/pg_xact", node);
			UT_ASSERT_EQ(unlinkat(fd, path, AT_REMOVEDIR), 0);
		}
	}
}

static void
owned_census_relation(int fd, const ClusterWalSourceRef *source, unsigned rel, bool empty)
{
	ClusterSpaceReservation reservation = { 0 };
	PGAlignedBlock pages[2], data = { 0 };
	char path[80];
	int file;

	reservation.identity.key.system_identifier = source->claim.identity.system_identifier;
	reservation.identity.key.database_incarnation = source->claim.database_incarnation;
	memcpy(reservation.identity.key.storage_uuid, source->claim.identity.storage_uuid, 16);
	reservation.identity.key.locator = (RelFileLocator){ 1663, 5, rel };
	reservation.identity.incarnation[0] = 3;
	reservation.identity.sequence = 2;
	reservation.identity.operation = 9;
	reservation.identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	reservation.next_block = empty ? 0 : 1;
	UT_ASSERT(cluster_space_identity_page_encode(&reservation.identity, 7, pages[0].data, BLCKSZ));
	UT_ASSERT(cluster_space_reservation_page_encode(&reservation, 11, pages[1].data, BLCKSZ));
	snprintf(path, sizeof(path), "base/5/%u_space", rel);
	file = openat(fd, path, O_CREAT | O_TRUNC | O_WRONLY, 0600);
	UT_ASSERT(file >= 0);
	UT_ASSERT_EQ(write(file, pages, sizeof(pages)), sizeof(pages));
	UT_ASSERT_EQ(close(file), 0);
	snprintf(path, sizeof(path), "base/5/%u", rel);
	file = openat(fd, path, O_CREAT | O_TRUNC | O_WRONLY, 0600);
	UT_ASSERT(file >= 0);
	data.data[BLCKSZ - 1] = 73; /* raw non-heap remains an obligation */
	if (!empty)
		UT_ASSERT_EQ(write(file, &data, BLCKSZ), BLCKSZ);
	UT_ASSERT_EQ(close(file), 0);
}

static ClusterRecoveryCensus *
owned_census_fixture(ClusterWalInputsV1 **inputs, int *fd, char path[MAXPGPATH], bool open_source)
{
	ClusterRecoveryRootSet *roots;
	ClusterRecoveryCensus *census = NULL;
	int file;
	inventory_extra = owned_page_roots_fixture ? 6 : owned_terminals ? 16 : 9;
	inventory_binding_case = 0;
	*inputs = inventory_fixture(false, open_source);
	roots = inventory_roots(*inputs, path, fd, false);
	cluster_recovery_root_set_destroy(roots);
	owned_census_relation(*fd, &cluster_wal_inputs_at_v1(*inputs, 0)->source, 16385, false);
	owned_census_relation(*fd, &cluster_wal_inputs_at_v1(*inputs, 0)->source, 99999, true);
	UT_ASSERT_EQ(mkdirat(*fd, "pg_undo", 0700), 0);
	wal_root_segment(*fd, 0);
	wal_root_segment(*fd, 127);
	inventory_mx_files(*fd, true, true);
	owned_clog_files(*fd, true);
	if (owned_clog_fault != 0) {
		const char *path = "native_side/origin_0/pg_xact/0004";
		if (owned_clog_fault == 1)
			UT_ASSERT_EQ(unlinkat(*fd, path, 0), 0);
		else {
			int file = openat(*fd, path, O_WRONLY);
			UT_ASSERT(file >= 0);
			UT_ASSERT_EQ(ftruncate(file, owned_clog_fault == 2 ? 0 : BLCKSZ - 1), 0);
			UT_ASSERT_EQ(close(file), 0);
		}
	}
	file = openat(*fd, "unowned", O_CREAT | O_WRONLY, 0600);
	UT_ASSERT(file >= 0);
	UT_ASSERT_EQ(write(file, "unexplained", 11), 11);
	UT_ASSERT_EQ(close(file), 0);
	if (owned_target_mode != 0) {
		UndoRecordHeader record;
		off_t offset = BLCKSZ + sizeof(UndoBlockHeader);
		file = openat(*fd, "pg_undo/instance_0/seg_1.dat", O_RDWR);
		UT_ASSERT(file >= 0);
		UT_ASSERT_EQ(pread(file, &record, sizeof(record), offset), sizeof(record));
		if (owned_target_mode == 1)
			record.target_locator.relNumber = 20000;
		if (owned_target_mode == 2)
			record.target_locator.relNumber = 99999;
		if (owned_target_mode == 3)
			record.target_block = InvalidBlockNumber - 1;
		if (owned_target_mode == 4)
			UT_ASSERT_EQ(unlinkat(*fd, "base/5/16385_space", 0), 0);
		if (owned_target_mode == 5)
			record.target_fork = INIT_FORKNUM;
		UT_ASSERT_EQ(pwrite(file, &record, sizeof(record), offset), sizeof(record));
		UT_ASSERT_EQ(close(file), 0);
	}
	if (owned_native_slru)
		owned_slru_files(*fd, true);
	UT_ASSERT_EQ(cluster_recovery_census_begin(*inputs, *fd, 8 * 1024 * 1024, &census),
				 CLUSTER_RECOVERY_CENSUS_MORE);
	return census;
}

static void
owned_census_done(ClusterRecoveryCensus *census, ClusterWalInputsV1 **inputs, int fd,
				  const char *path)
{
	cluster_recovery_census_destroy(census);
	owned_clog_files(fd, false);
	inventory_mx_files(fd, false, false);
	UT_ASSERT_EQ(unlinkat(fd, "pg_undo/instance_0/seg_1.dat", 0), 0);
	UT_ASSERT_EQ(unlinkat(fd, "pg_undo/instance_127/seg_32513.dat", 0), 0);
	UT_ASSERT_EQ(unlinkat(fd, "pg_undo/instance_0", AT_REMOVEDIR), 0);
	UT_ASSERT_EQ(unlinkat(fd, "pg_undo/instance_127", AT_REMOVEDIR), 0);
	UT_ASSERT_EQ(unlinkat(fd, "pg_undo", AT_REMOVEDIR), 0);
	UT_ASSERT_EQ(unlinkat(fd, "base/5/16385", 0), 0);
	UT_ASSERT_EQ(unlinkat(fd, "base/5/16385_space", 0), 0);
	UT_ASSERT_EQ(unlinkat(fd, "base/5/99999_space", 0), 0);
	UT_ASSERT_EQ(unlinkat(fd, "unowned", 0), 0);
	inventory_roots_done(NULL, fd, path);
	inputs_pin_current = true;
	inventory_between_passes = NULL;
	cold_census_done(inputs);
	inventory_extra = 0;
}

static ClusterRecoveryCensusResult
owned_census_until(ClusterRecoveryCensus *census, ClusterRecoveryCensusPhase phase)
{
	ClusterRecoveryCensusDiag diag = { 0 };
	ClusterRecoveryCensusEntry entry;
	for (unsigned steps = 0; steps < 200000; steps++) {
		ClusterRecoveryCensusResult result;
		UT_ASSERT(cluster_recovery_census_diag(census, &diag));
		if (diag.phase == phase && phase != 0)
			return CLUSTER_RECOVERY_CENSUS_MORE;
		result = cluster_recovery_census_next(census, &entry);
		if (result >= CLUSTER_RECOVERY_CENSUS_DRAINED) {
			if (result == CLUSTER_RECOVERY_CENSUS_DRAINED && phase == CLUSTER_RECOVERY_CENSUS_DONE)
				return CLUSTER_RECOVERY_CENSUS_MORE;
			return result;
		}
	}
	UT_ASSERT(false);
	return CLUSTER_RECOVERY_CENSUS_INVALID;
}

static void
owned_census_sticky(ClusterRecoveryCensus *census, ClusterRecoveryCensusResult result)
{
	ClusterRecoveryCensusEntry entry, zero = { 0 };
	memset(&entry, 0x77, sizeof(entry));
	UT_ASSERT_EQ(cluster_recovery_census_next(census, &entry), result);
	UT_ASSERT_EQ(memcmp(&entry, &zero, sizeof(entry)), 0);
}

UT_TEST(test_owned_census_missing_or_short_terminal_clog_refuses)
{
	for (unsigned fault = 1; fault <= 3; ++fault) {
		ClusterWalInputsV1 *inputs;
		char path[MAXPGPATH];
		int fd;
		ClusterRecoveryCensus *census;
		owned_clog_fault = fault;
		census = owned_census_fixture(&inputs, &fd, path, false);
		owned_clog_fault = 0;
		UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_REFUSED);
		owned_census_sticky(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
		if (fault == 1) {
			/* Restore an empty path only for fixture teardown, after refusal. */
			int file = openat(fd, "native_side/origin_0/pg_xact/0004", O_CREAT | O_WRONLY, 0600);
			UT_ASSERT(file >= 0);
			UT_ASSERT_EQ(close(file), 0);
			owned_census_sticky(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
		}
		owned_census_done(census, &inputs, fd, path);
	}
}

UT_TEST(test_owned_census_real_inputs_all_observation_families)
{
	ClusterWalInputsV1 *inputs;
	char path[MAXPGPATH];
	int fd;
	ClusterRecoveryCensus *census = owned_census_fixture(&inputs, &fd, path, false);
	ClusterRecoveryCensusEntry entry;
	ClusterRecoveryCensusDiag diag = { 0 };
	ClusterRecoveryCensusResult result = CLUSTER_RECOVERY_CENSUS_MORE;
	unsigned unknown = 0, raw_pages = 0, mismatches = 0, kinds[13] = { 0 };
	for (unsigned steps = 0; steps < 200000; steps++) {
		result = cluster_recovery_census_next(census, &entry);
		if (result >= CLUSTER_RECOVERY_CENSUS_DRAINED)
			break;
		if (result != CLUSTER_RECOVERY_CENSUS_ENTRY)
			continue;
		UT_ASSERT(entry.kind >= 1 && entry.kind <= CLUSTER_RECOVERY_CENSUS_TT_TERMINAL);
		kinds[entry.kind]++;
		if (entry.kind == CLUSTER_RECOVERY_CENSUS_RELATION) {
			const ClusterRecoveryRelationCensusEntry *r = &entry.value.relation;
			unknown += r->kind == CLUSTER_RECOVERY_RELATIONS_ROOT
					   && strcmp(r->root->file.path, "unowned") == 0;
			raw_pages += r->kind == CLUSTER_RECOVERY_RELATIONS_PAGE && !r->heap;
		}
		if (entry.kind == CLUSTER_RECOVERY_CENSUS_TX_TARGET) {
			const ClusterRecoveryCensusTarget *target = &entry.value.target;
			UT_ASSERT(target->transaction.record != NULL);
			UT_ASSERT(target->root != NULL && target->space != NULL);
			UT_ASSERT(target->target_physical && target->target_extent_present
					  && target->space_physical);
			UT_ASSERT_EQ(target->key.locator.relNumber, 16385);
			UT_ASSERT_EQ(target->required_blocks, 1);
			UT_ASSERT_EQ(target->transaction.transaction.xid, 4195136);
			UT_ASSERT_EQ(target->transaction.transaction.slot_wrap, 1);
		}
		if (entry.kind == CLUSTER_RECOVERY_CENSUS_MX_MATCH)
			mismatches += !entry.value.compare.equal;
		if (entry.kind == CLUSTER_RECOVERY_CENSUS_XACT_MATCH) {
			const ClusterRecoveryXactJoinEntry *x = &entry.value.xact_compare;
			UT_ASSERT(x->record != NULL && x->is_top && !x->is_parent);
			UT_ASSERT_EQ(x->xid, 4195136);
			UT_ASSERT_EQ(x->expected_value, TRANSACTION_STATUS_COMMITTED);
			UT_ASSERT_EQ(x->observed_value, TRANSACTION_STATUS_IN_PROGRESS);
			UT_ASSERT(!x->equal);
		}
	}
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_CENSUS_DRAINED);
	UT_ASSERT_EQ(kinds[CLUSTER_RECOVERY_CENSUS_TT_SLOT], 96);
	UT_ASSERT_EQ(kinds[CLUSTER_RECOVERY_CENSUS_TX_RECORD], 2);
	UT_ASSERT_EQ(kinds[CLUSTER_RECOVERY_CENSUS_TX_TARGET], 2);
	UT_ASSERT(kinds[CLUSTER_RECOVERY_CENSUS_MX_SLOT] > 4096);
	UT_ASSERT_EQ(kinds[CLUSTER_RECOVERY_CENSUS_MX_MATCH], 6);
	UT_ASSERT_EQ(kinds[CLUSTER_RECOVERY_CENSUS_XACT_MATCH], 2);
	UT_ASSERT(kinds[CLUSTER_RECOVERY_CENSUS_NATIVE_EFFECT] > 10);
	UT_ASSERT_EQ(unknown, 1);
	UT_ASSERT(raw_pages > 0);
	UT_ASSERT_EQ(mismatches, 2);
	UT_ASSERT(cluster_recovery_census_diag(census, &diag));
	UT_ASSERT_EQ(diag.phase, CLUSTER_RECOVERY_CENSUS_DONE);
	UT_ASSERT_EQ(diag.tx_targets, 2);
	UT_ASSERT_EQ(diag.tx_target_missing | diag.tx_target_short | diag.tx_space_missing, 0);
	UT_ASSERT_EQ(diag.inventory.sources, 2);
	UT_ASSERT_EQ(diag.inventory.tt_roots_added, 2);
	UT_ASSERT_EQ(diag.inventory.mx_physical_mismatches, 2);
	UT_ASSERT_EQ(diag.xact_comparisons, 2);
	UT_ASSERT_EQ(diag.inventory.xact_clog_compared, 2);
	UT_ASSERT_EQ(diag.inventory.xact_parents_compared, 0);
	UT_ASSERT_EQ(diag.inventory.xact_physical_mismatches, 2);
	UT_ASSERT_EQ(inventory_census_calls, 3); /* two initial passes, original final reread */
	owned_census_sticky(census, CLUSTER_RECOVERY_CENSUS_DRAINED);
	owned_census_done(census, &inputs, fd, path);
}

UT_TEST(test_owned_census_final_rereads_refuse_changes)
{
	for (unsigned mode = 0; mode < 4; mode++) {
		ClusterWalInputsV1 *inputs;
		char path[MAXPGPATH];
		int fd, file;
		ClusterRecoveryCensus *census = owned_census_fixture(&inputs, &fd, path, false);
		UT_ASSERT_EQ(owned_census_until(census, CLUSTER_RECOVERY_CENSUS_FINAL_TT),
					 CLUSTER_RECOVERY_CENSUS_MORE);
		if (mode == 0) {
			uint32 xid = 12345;
			file = openat(fd, "pg_undo/instance_0/seg_1.dat", O_WRONLY);
			UT_ASSERT(file >= 0);
			UT_ASSERT_EQ(pwrite(file, &xid, sizeof(xid),
								offsetof(UndoSegmentHeaderData, tt_slots) + offsetof(TTSlot, xid)),
						 sizeof(xid));
			UT_ASSERT_EQ(close(file), 0);
		} else if (mode == 1 || mode == 3) {
			file = openat(fd, mode == 1 ? "base/5/99999" : "late", O_CREAT | O_WRONLY, 0600);
			UT_ASSERT(file >= 0);
			UT_ASSERT_EQ(write(file, "changed", 7), 7);
			UT_ASSERT_EQ(close(file), 0);
		} else {
			inventory_fault = 0;
			inventory_drift();
		}
		UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_REFUSED);
		owned_census_sticky(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
		if (mode == 3)
			UT_ASSERT_EQ(unlinkat(fd, "late", 0), 0);
		owned_census_done(census, &inputs, fd, path);
	}
}

UT_TEST(test_owned_census_scope_loss_and_drained_invalidation)
{
	for (unsigned mode = 0; mode < 2; mode++) {
		ClusterWalInputsV1 *inputs;
		char path[MAXPGPATH];
		int fd;
		ClusterRecoveryCensus *census = owned_census_fixture(&inputs, &fd, path, false);
		if (mode == 0) {
			UT_ASSERT_EQ(owned_census_until(census, CLUSTER_RECOVERY_CENSUS_TT),
						 CLUSTER_RECOVERY_CENSUS_MORE);
			inputs_pin_current = false;
		} else {
			UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
			v2_checkpoint_root_race();
		}
		owned_census_sticky(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
		inputs_pin_current = true;
		owned_census_sticky(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
		owned_census_done(census, &inputs, fd, path);
	}
}

UT_TEST(test_owned_census_missing_and_short_files_refuse)
{
	for (unsigned mode = 0; mode < 4; mode++) {
		ClusterWalInputsV1 *inputs;
		char path[MAXPGPATH];
		int fd, file;
		ClusterRecoveryCensus *census = owned_census_fixture(&inputs, &fd, path, false);
		const char *name = (mode == 0 || mode == 3) ? "base/5/16385_space"
						   : mode == 1 ? "pg_undo/instance_0/seg_1.dat"
									   : "native_side/origin_127/pg_multixact/members/0000";
		/* Namespace was captured, bytes are not yet observed. */
		if (mode == 3)
			UT_ASSERT_EQ(unlinkat(fd, name, 0), 0);
		else {
			file = openat(fd, name, O_WRONLY);
			UT_ASSERT(file >= 0);
			UT_ASSERT_EQ(ftruncate(file, 3), 0);
			UT_ASSERT_EQ(close(file), 0);
		}
		UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_REFUSED);
		owned_census_sticky(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
		if (mode == 3) {
			/* Restore only after the sticky refusal, for normal fixture cleanup. */
			owned_census_relation(fd, &cluster_wal_inputs_at_v1(inputs, 0)->source, 16385, false);
			owned_census_sticky(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
		}
		owned_census_done(census, &inputs, fd, path);
	}
}

UT_TEST(test_owned_census_error_releases_children)
{
	for (unsigned final = 0; final < 2; final++) {
		ClusterWalInputsV1 *inputs;
		char path[MAXPGPATH];
		int fd;
		ClusterRecoveryCensus *census = owned_census_fixture(&inputs, &fd, path, false);
		ClusterRecoveryCensusEntry entry, zero = { 0 };
		bool threw = false;
		if (final) {
			UT_ASSERT_EQ(owned_census_until(census, CLUSTER_RECOVERY_CENSUS_FINAL_WAL),
						 CLUSTER_RECOVERY_CENSUS_MORE);
			/* Reset only the test call counter: inject after the real final
			 * census has read the complete original WAL again. */
			inventory_census_calls = 0;
		}
		inventory_fault = 4;
		inventory_between_passes = inventory_drift;
		memset(&entry, 0x77, sizeof(entry));
		PG_TRY();
		{
			(void)cluster_recovery_census_next(census, &entry);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		UT_ASSERT(threw);
		UT_ASSERT_EQ(memcmp(&entry, &zero, sizeof(entry)), 0);
		{
			ClusterRecoveryCensusManifest manifest, empty = { 0 };
			memset(&manifest, 0x77, sizeof(manifest));
			UT_ASSERT(!cluster_recovery_census_manifest(census, &manifest));
			UT_ASSERT_EQ(memcmp(&manifest, &empty, sizeof(empty)), 0);
		}
		owned_census_sticky(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
		owned_census_done(census, &inputs, fd, path);
	}
}

UT_TEST(test_owned_census_open_input_and_budget_refuse)
{
	ClusterWalInputsV1 *inputs;
	char path[MAXPGPATH];
	int fd;
	ClusterRecoveryCensus *census = owned_census_fixture(&inputs, &fd, path, true);
	ClusterRecoveryCensus *small = (void *)1;
	UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_REFUSED);
	UT_ASSERT_EQ(cluster_recovery_census_begin(inputs, fd, 1, &small),
				 CLUSTER_RECOVERY_CENSUS_CAPACITY);
	UT_ASSERT(small == NULL);
	UT_ASSERT_EQ(cluster_recovery_census_begin(inputs, fd, 8 * 1024 * 1024, NULL),
				 CLUSTER_RECOVERY_CENSUS_INVALID);
	owned_census_done(census, &inputs, fd, path);
}

UT_TEST(test_owned_census_destroy_each_phase)
{
	for (unsigned phase = CLUSTER_RECOVERY_CENSUS_WAL; phase <= CLUSTER_RECOVERY_CENSUS_DONE;
		 phase++) {
		ClusterWalInputsV1 *inputs;
		char path[MAXPGPATH];
		int fd;
		ClusterRecoveryCensus *census = owned_census_fixture(&inputs, &fd, path, false);
		UT_ASSERT_EQ(owned_census_until(census, phase), CLUSTER_RECOVERY_CENSUS_MORE);
		owned_census_done(census, &inputs, fd, path);
	}
}

UT_TEST(test_owned_census_target_obligations_keep_absence_extent_and_unsupported_forks)
{
	for (unsigned mode = 1; mode <= 5; mode++) {
		ClusterWalInputsV1 *inputs;
		char path[MAXPGPATH];
		int fd;
		ClusterRecoveryCensus *census;
		ClusterRecoveryCensusEntry entry;
		ClusterRecoveryCensusDiag diag = { 0 };
		unsigned count = 0, seen = 0;
		owned_target_mode = mode;
		census = owned_census_fixture(&inputs, &fd, path, false);
		owned_target_mode = 0;
		if (mode == 5) {
			UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_REFUSED);
			owned_census_sticky(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
		} else {
			UT_ASSERT_EQ(owned_census_until(census, CLUSTER_RECOVERY_CENSUS_TX_TARGETS), 0);
			for (unsigned step = 0; step < 8; step++) {
				ClusterRecoveryCensusResult result = cluster_recovery_census_next(census, &entry);
				UT_ASSERT(result <= CLUSTER_RECOVERY_CENSUS_ENTRY);
				if (result > CLUSTER_RECOVERY_CENSUS_ENTRY)
					break;
				if (result == CLUSTER_RECOVERY_CENSUS_ENTRY) {
					const ClusterRecoveryCensusTarget *target = &entry.value.target;
					UT_ASSERT_EQ(entry.kind, CLUSTER_RECOVERY_CENSUS_TX_TARGET);
					count++;
					if (target->transaction.transaction.origin_node_id == 0) {
						seen++;
						UT_ASSERT_EQ(target->target_physical, mode != 1);
						UT_ASSERT_EQ(target->target_extent_present, mode == 4);
						UT_ASSERT_EQ(target->space_physical, mode == 2 || mode == 3);
						if (mode == 1) {
							UT_ASSERT(target->root == NULL && target->space == NULL);
							UT_ASSERT_EQ(target->root_index, UINT32_MAX);
							UT_ASSERT_EQ(target->space_index, UINT32_MAX);
						}
						if (mode == 3)
							UT_ASSERT_EQ(target->required_blocks, (uint64)InvalidBlockNumber);
					}
				}
				UT_ASSERT(cluster_recovery_census_diag(census, &diag));
				if (diag.phase == CLUSTER_RECOVERY_CENSUS_RELATIONS)
					break;
			}
			UT_ASSERT_EQ(count, 2);
			UT_ASSERT_EQ(seen, 1);
			UT_ASSERT_EQ(diag.phase, CLUSTER_RECOVERY_CENSUS_RELATIONS);
			UT_ASSERT_EQ(diag.tx_targets, 2);
			UT_ASSERT_EQ(diag.tx_target_missing, mode == 1);
			UT_ASSERT_EQ(diag.tx_target_short, mode == 2 || mode == 3);
			UT_ASSERT_EQ(diag.tx_space_missing, mode == 4 ? 2 : mode == 1 ? 1 : 0);
		}
		if (mode == 4)
			owned_census_relation(fd, &cluster_wal_inputs_at_v1(inputs, 0)->source, 16385, false);
		owned_census_done(census, &inputs, fd, path);
	}
}

UT_TEST(test_owned_census_target_join_and_final_observation_revoke_changed_inputs)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		ClusterWalInputsV1 *inputs;
		char path[MAXPGPATH];
		int fd;
		ClusterRecoveryCensus *census = owned_census_fixture(&inputs, &fd, path, false);
		ClusterRecoveryCensusEntry entry;
		UT_ASSERT_EQ(owned_census_until(census, CLUSTER_RECOVERY_CENSUS_TX_TARGETS), 0);
		UT_ASSERT_EQ(cluster_recovery_census_next(census, &entry), CLUSTER_RECOVERY_CENSUS_ENTRY);
		UT_ASSERT_EQ(entry.kind, CLUSTER_RECOVERY_CENSUS_TX_TARGET);
		if (mode == 0)
			cluster_recovery_reference_graph_invalidate(census->graph);
		if (mode == 1)
			cluster_recovery_root_set_invalidate(census->roots);
		if (mode == 2) {
			uint32 block = 10;
			int file = openat(fd, "pg_undo/instance_0/seg_1.dat", O_WRONLY);
			UT_ASSERT(file >= 0);
			UT_ASSERT_EQ(
				pwrite(file, &block, sizeof(block),
					   BLCKSZ + sizeof(UndoBlockHeader) + offsetof(UndoRecordHeader, target_block)),
				sizeof(block));
			UT_ASSERT_EQ(close(file), 0);
		}
		UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_REFUSED);
		owned_census_sticky(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
		owned_census_done(census, &inputs, fd, path);
	}
}

static const char *owned_slru_dirs[]
	= { "pg_xact", "native_side/origin_0/pg_subtrans", "native_side/origin_127/pg_commit_ts" };

static void
owned_slru_files(int fd, bool create)
{
	PGAlignedBlock page;
	char path[MAXPGPATH];
	memset(&page, 0xb1, sizeof(page));
	for (unsigned i = 0; i < lengthof(owned_slru_dirs); ++i) {
		snprintf(path, sizeof(path), "%s/0000", owned_slru_dirs[i]);
		if (create) {
			int file;
			UT_ASSERT_EQ(mkdirat(fd, owned_slru_dirs[i], 0700), 0);
			file = openat(fd, path, O_CREAT | O_WRONLY, 0600);
			UT_ASSERT(file >= 0);
			UT_ASSERT_EQ(write(file, page.data, BLCKSZ), BLCKSZ);
			UT_ASSERT_EQ(close(file), 0);
		} else {
			UT_ASSERT_EQ(unlinkat(fd, path, 0), 0);
			UT_ASSERT_EQ(unlinkat(fd, owned_slru_dirs[i], AT_REMOVEDIR), 0);
		}
	}
}

UT_TEST(test_owned_census_enumerates_raw_native_slru_slots)
{
	ClusterWalInputsV1 *inputs;
	char path[MAXPGPATH];
	int fd;
	ClusterRecoveryCensus *census;
	ClusterRecoveryCensusEntry entry;
	ClusterRecoveryCensusDiag diag;
	ClusterRecoveryCensusResult result = CLUSTER_RECOVERY_CENSUS_MORE;
	unsigned clogs = 0, origin_clogs = 0, parents = 0, times = 0, files = 0, padding = 0;
	owned_native_slru = true;
	census = owned_census_fixture(&inputs, &fd, path, false);
	owned_native_slru = false;
	for (unsigned i = 0; i < 200000; ++i) {
		const ClusterRecoverySlruEntry *s;
		result = cluster_recovery_census_next(census, &entry);
		if (result >= CLUSTER_RECOVERY_CENSUS_DRAINED)
			break;
		if (result != CLUSTER_RECOVERY_CENSUS_ENTRY
			|| entry.kind != CLUSTER_RECOVERY_CENSUS_SLRU_SLOT)
			continue;
		s = &entry.value.slru;
		if (s->kind == CLUSTER_RECOVERY_SLRU_FILE)
			files++;
		else if (s->kind == CLUSTER_RECOVERY_SLRU_PADDING)
			padding++;
		else if (s->kind == CLUSTER_RECOVERY_SLRU_CLOG) {
			if (s->origin_node == UINT32_MAX) {
				clogs++;
				UT_ASSERT_EQ(s->raw_status, (0xb1 >> (2 * (s->slot % 4))) & 3);
			} else {
				origin_clogs++;
				UT_ASSERT(s->origin_node == 0 || s->origin_node == 127);
				UT_ASSERT_EQ(s->raw_status, 0);
			}
		} else if (s->kind == CLUSTER_RECOVERY_SLRU_SUBTRANS) {
			parents++;
			UT_ASSERT_EQ(s->origin_node, 0);
			UT_ASSERT_EQ(s->raw_parent, UINT32_C(0xb1b1b1b1));
		} else {
			times++;
			UT_ASSERT_EQ(s->kind, CLUSTER_RECOVERY_SLRU_COMMIT_TS);
			UT_ASSERT_EQ(s->origin_node, 127);
			UT_ASSERT_EQ(s->raw_replication_origin, 0xb1b1);
		}
	}
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_CENSUS_DRAINED);
	UT_ASSERT_EQ(clogs, BLCKSZ * 4);
	UT_ASSERT_EQ(origin_clogs, 2 * BLCKSZ * 4);
	UT_ASSERT_EQ(parents, BLCKSZ / 4);
	UT_ASSERT_EQ(times, BLCKSZ / 10);
	UT_ASSERT_EQ(files, 5);
	UT_ASSERT_EQ(padding, 1);
	UT_ASSERT(cluster_recovery_census_diag(census, &diag));
	UT_ASSERT_EQ(diag.slru_entries, clogs + origin_clogs + parents + times + files + padding);
	UT_ASSERT_EQ(inventory_census_calls, 3);
	owned_census_sticky(census, CLUSTER_RECOVERY_CENSUS_DRAINED);
	owned_slru_files(fd, false);
	owned_census_done(census, &inputs, fd, path);
}

UT_TEST(test_owned_census_native_slru_drift_and_scope_loss_revoke_outputs)
{
	for (unsigned variant = 0; variant < 4; ++variant) {
		ClusterWalInputsV1 *inputs;
		char path[MAXPGPATH];
		int fd, file;
		ClusterRecoveryCensus *census;
		owned_native_slru = variant != 3;
		census = owned_census_fixture(&inputs, &fd, path, false);
		owned_native_slru = false;
		if (variant == 3) {
			/* The original ROOT begin already captured directory identity. */
			owned_slru_files(fd, true);
			UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_REFUSED);
			owned_census_sticky(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
			owned_slru_files(fd, false);
			owned_census_done(census, &inputs, fd, path);
			continue;
		}
		UT_ASSERT_EQ(owned_census_until(census, variant == 0 ? CLUSTER_RECOVERY_CENSUS_SLRU_PHYSICAL
															 : CLUSTER_RECOVERY_CENSUS_FINAL_TT),
					 CLUSTER_RECOVERY_CENSUS_MORE);
		if (variant == 2)
			inputs_pin_current = false;
		else {
			file = openat(fd, "pg_xact/0000", O_WRONLY);
			UT_ASSERT(file >= 0);
			if (variant == 0)
				UT_ASSERT_EQ(ftruncate(file, 1), 0);
			else
				UT_ASSERT_EQ(pwrite(file, "changed", 7, 101), 7);
			UT_ASSERT_EQ(close(file), 0);
		}
		UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_REFUSED);
		owned_census_sticky(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
		owned_slru_files(fd, false);
		owned_census_done(census, &inputs, fd, path);
	}
}


/* Original cold PAGE inventory: parser/ROOT/WAL/physical files are real.
 * Refuse-only mode is a test-first compatibility baseline, never production. */
#ifdef CTRC_PAGES_REFUSE_ONLY
const ClusterRecoveryCensusPagesV1 *
cluster_recovery_census_pages_v1(ClusterRecoveryCensus *census)
{
	(void)census;
	return NULL;
}
#endif

static void owned_page_root_files(int root);

UT_TEST(test_owned_page_inventory_retains_every_space_and_raw_main_page)
{
	static const uint8 main_digest[32] = {
		0x31,0x44,0x6d,0xdf,0x76,0xf6,0x02,0xf7,0x8a,0xb1,0xa3,0x70,0xe6,0x67,0xd9,0xe0,
		0xfc,0xd4,0x18,0x07,0x0d,0x97,0xe1,0x21,0xd3,0xec,0x2e,0xd7,0x5d,0x07,0x54,0xbd
	};
	ClusterWalInputsV1 *inputs;
	char path[MAXPGPATH];
	int fd;
	unsigned main_pages = 0, space_pages = 0, empty_fork_pages = 0;
	ClusterRecoveryCensus *census = owned_census_fixture(&inputs, &fd, path, false);
	const ClusterRecoveryCensusPagesV1 *pages;

	UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
	pages = cluster_recovery_census_pages_v1(census);
	UT_ASSERT(pages != NULL);
	if (pages == NULL) {
		owned_census_done(census, &inputs, fd, path);
		return;
	}
	UT_ASSERT_EQ(pages->count, 5);
	for (uint32 i = 0; i < pages->count; i++) {
		const ClusterRecoveryCensusPageV1 *page = &pages->entries[i];
		const ClusterRecoveryRootEntry *root = cluster_recovery_root_set_entry(census->roots,
			page->root_index);
		UT_ASSERT(root != NULL && root->file.kind == CLUSTER_RECOVERY_FILE_RELATION);
		UT_ASSERT(RelFileLocatorEquals(root->file.locator, page->reservation.identity.key.locator));
		UT_ASSERT_EQ(root->file.forknum, page->forknum);
		UT_ASSERT_EQ(page->identity_token, 7); UT_ASSERT_EQ(page->reservation_token, 11);
		UT_ASSERT_EQ(page->reservation.identity.sequence, 2);
		UT_ASSERT_EQ(page->reservation.identity.operation, 9);
		UT_ASSERT_EQ(page->reservation.identity.state, CLUSTER_SPACE_IDENTITY_LIVE);
		UT_ASSERT_EQ(page->reservation.identity.incarnation[0], 3);
		UT_ASSERT_EQ(page->reservation.identity.key.locator.spcOid, 1663);
		UT_ASSERT_EQ(page->reservation.identity.key.locator.dbOid, 5);
		UT_ASSERT(!page->heap);
		if (page->forknum == MAIN_FORKNUM) {
			main_pages++;
			UT_ASSERT_EQ(page->reservation.identity.key.locator.relNumber, 16385);
			UT_ASSERT_EQ(page->blockno, 0);
			UT_ASSERT(memcmp(page->page_sha256, main_digest, 32) == 0);
		} else {
			UT_ASSERT_EQ(page->forknum, SPACE_FORKNUM);
			UT_ASSERT(page->blockno < 2); space_pages++;
		}
		empty_fork_pages += page->reservation.identity.key.locator.relNumber == 99999
			&& page->forknum == MAIN_FORKNUM;
		if (i != 0) {
			const ClusterRecoveryCensusPageV1 *before = &pages->entries[i - 1];
			UT_ASSERT(before->root_index < page->root_index
				|| (before->root_index == page->root_index && before->blockno < page->blockno));
		}
	}
	UT_ASSERT_EQ(main_pages, 1); UT_ASSERT_EQ(space_pages, 4);
	UT_ASSERT_EQ(empty_fork_pages, 0);
	UT_ASSERT(cluster_recovery_census_pages_v1(census) == pages);
	owned_census_done(census, &inputs, fd, path);
}

UT_TEST(test_owned_page_inventory_keeps_original_heap_layout_and_space_identity)
{
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryCensus *census;
	const ClusterRecoveryCensusPagesV1 *pages;
	char path[MAXPGPATH];
	int fd;
	unsigned heap = 0;

	owned_page_roots_fixture = true;
	census = owned_census_fixture(&inputs, &fd, path, false);
	owned_page_roots_fixture = false;
	owned_page_root_files(fd);
	UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
	pages = cluster_recovery_census_pages_v1(census);
	UT_ASSERT(pages != NULL);
	if (pages == NULL) {
		owned_census_done(census, &inputs, fd, path);
		return;
	}
	UT_ASSERT_EQ(pages->count, 5);
	for (uint32 i = 0; i < pages->count; i++) {
		if (!pages->entries[i].heap) continue;
		heap++;
		UT_ASSERT_EQ(pages->entries[i].forknum, MAIN_FORKNUM);
		UT_ASSERT_EQ(pages->entries[i].blockno, 0);
		UT_ASSERT_EQ(pages->entries[i].reservation.identity.key.locator.relNumber, 16385);
	}
	UT_ASSERT_EQ(heap, 1);
	owned_census_done(census, &inputs, fd, path);
}

UT_TEST(test_owned_page_inventory_never_exports_or_advances_an_observation_prefix)
{
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryCensusDiag a, b;
	char path[MAXPGPATH];
	int fd;
	ClusterRecoveryCensus *census = owned_census_fixture(&inputs, &fd, path, false);

	UT_ASSERT(cluster_recovery_census_pages_v1(NULL) == NULL);
	UT_ASSERT_EQ(owned_census_until(census, CLUSTER_RECOVERY_CENSUS_RELATIONS), 0);
	UT_ASSERT(cluster_recovery_census_diag(census, &a));
	UT_ASSERT(cluster_recovery_census_pages_v1(census) == NULL);
	UT_ASSERT(cluster_recovery_census_diag(census, &b));
	UT_ASSERT(memcmp(&a, &b, sizeof(a)) == 0);
	UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
	UT_ASSERT(cluster_recovery_census_pages_v1(census) != NULL);
	owned_census_done(census, &inputs, fd, path);
}

UT_TEST(test_owned_page_inventory_mutation_revokes_the_original_complete_view)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		ClusterWalInputsV1 *inputs;
		char path[MAXPGPATH];
		int fd;
		ClusterRecoveryCensus *census = owned_census_fixture(&inputs, &fd, path, false);
		ClusterRecoveryCensusPagesV1 *pages;
		ClusterRecoveryCensusManifest manifest;
		UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
		pages = (ClusterRecoveryCensusPagesV1 *)cluster_recovery_census_pages_v1(census);
		UT_ASSERT(pages != NULL && pages->count == 5);
		if (pages == NULL || pages->count != 5) {
			owned_census_done(census, &inputs, fd, path);
			continue;
		}
		if (fault == 0) pages->count--;
		if (fault == 1) pages->entries[4].root_index++;
		if (fault == 2) pages->entries[4].reservation.identity.sequence++;
		if (fault == 3) pages->entries[4].page_sha256[31]++;
		if (fault == 4) pages->entries[4].heap = !pages->entries[4].heap;
		if (fault == 5) pages->entries[4].blockno++;
		UT_ASSERT(cluster_recovery_census_pages_v1(census) == NULL);
		UT_ASSERT(!cluster_recovery_census_manifest(census, &manifest));
		owned_census_sticky(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
		owned_census_done(census, &inputs, fd, path);
	}
}

UT_TEST(test_owned_page_inventory_scope_loss_is_sticky_without_rebasing_inputs)
{
	ClusterWalInputsV1 *inputs;
	char path[MAXPGPATH];
	int fd;
	ClusterRecoveryCensus *census = owned_census_fixture(&inputs, &fd, path, false);
	UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
	UT_ASSERT(cluster_recovery_census_pages_v1(census) != NULL);
	inputs_pin_current = false;
	UT_ASSERT(cluster_recovery_census_pages_v1(census) == NULL);
	inputs_pin_current = true;
	UT_ASSERT(cluster_recovery_census_pages_v1(census) == NULL);
	owned_census_sticky(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
	owned_census_done(census, &inputs, fd, path);
}

UT_TEST(test_owned_page_inventory_capacity_never_silently_omits_physical_pages)
{
	ClusterWalInputsV1 *inputs;
	char path[MAXPGPATH];
	int fd, file;
	ClusterSpaceReservation reservation;
	ClusterSpaceIdentityKey key = { 0 };
	const ClusterWalSourceRef *source;
	uint64 token;
	PGAlignedBlock page = { 0 };
	ClusterRecoveryCensus *census = owned_census_fixture(&inputs, &fd, path, false);
	cluster_recovery_census_destroy(census); census = NULL;
	source = &cluster_wal_inputs_at_v1(inputs, 0)->source;
	key.system_identifier = source->claim.identity.system_identifier;
	key.database_incarnation = source->claim.database_incarnation;
	memcpy(key.storage_uuid, source->claim.identity.storage_uuid, 16);
	key.locator = (RelFileLocator){ 1663, 5, 16385 };
	file = openat(fd, "base/5/16385_space", O_RDWR);
	UT_ASSERT(file >= 0);
	UT_ASSERT_EQ(pread(file, page.data, BLCKSZ, BLCKSZ), BLCKSZ);
	UT_ASSERT(cluster_space_reservation_page_decode(page.data, BLCKSZ, SPACE_FORKNUM, 1,
		&key, &reservation, &token));
	reservation.next_block = 4096;
	UT_ASSERT(cluster_space_reservation_page_encode(&reservation, 11, page.data, BLCKSZ));
	UT_ASSERT_EQ(pwrite(file, page.data, BLCKSZ, BLCKSZ), BLCKSZ);
	UT_ASSERT_EQ(close(file), 0);
	memset(page.data, 0, BLCKSZ); page.data[BLCKSZ - 1] = 73;
	file = openat(fd, "base/5/16385", O_WRONLY | O_TRUNC);
	UT_ASSERT(file >= 0);
	for (unsigned i = 0; i < 4096; i++) UT_ASSERT_EQ(write(file, page.data, BLCKSZ), BLCKSZ);
	UT_ASSERT_EQ(close(file), 0);
	UT_ASSERT_EQ(cluster_recovery_census_begin(inputs, fd, 2 * 1024 * 1024, &census), 0);
	UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_CAPACITY);
	UT_ASSERT(cluster_recovery_census_pages_v1(census) == NULL);
	owned_census_sticky(census, CLUSTER_RECOVERY_CENSUS_CAPACITY);
	owned_census_done(census, &inputs, fd, path);
}
