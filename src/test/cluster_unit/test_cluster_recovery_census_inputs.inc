/* One original cold scope owns the existing observation consumers.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */

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
	inventory_extra = 9;
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
	file = openat(*fd, "unowned", O_CREAT | O_WRONLY, 0600);
	UT_ASSERT(file >= 0);
	UT_ASSERT_EQ(write(file, "unexplained", 11), 11);
	UT_ASSERT_EQ(close(file), 0);
	UT_ASSERT_EQ(cluster_recovery_census_begin(*inputs, *fd, 8 * 1024 * 1024, &census),
				 CLUSTER_RECOVERY_CENSUS_MORE);
	return census;
}

static void
owned_census_done(ClusterRecoveryCensus *census, ClusterWalInputsV1 **inputs, int fd,
				  const char *path)
{
	cluster_recovery_census_destroy(census);
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
	ClusterRecoveryCensusDiag diag;
	ClusterRecoveryCensusEntry entry;
	for (unsigned steps = 0; steps < 100000; steps++) {
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

UT_TEST(test_owned_census_real_inputs_all_observation_families)
{
	ClusterWalInputsV1 *inputs;
	char path[MAXPGPATH];
	int fd;
	ClusterRecoveryCensus *census = owned_census_fixture(&inputs, &fd, path, false);
	ClusterRecoveryCensusEntry entry;
	ClusterRecoveryCensusDiag diag;
	ClusterRecoveryCensusResult result = CLUSTER_RECOVERY_CENSUS_MORE;
	unsigned unknown = 0, raw_pages = 0, mismatches = 0, kinds[7] = { 0 };
	for (unsigned steps = 0; steps < 100000; steps++) {
		result = cluster_recovery_census_next(census, &entry);
		if (result >= CLUSTER_RECOVERY_CENSUS_DRAINED)
			break;
		if (result != CLUSTER_RECOVERY_CENSUS_ENTRY)
			continue;
		UT_ASSERT(entry.kind >= 1 && entry.kind <= 6);
		kinds[entry.kind]++;
		if (entry.kind == CLUSTER_RECOVERY_CENSUS_RELATION) {
			const ClusterRecoveryRelationCensusEntry *r = &entry.value.relation;
			unknown += r->kind == CLUSTER_RECOVERY_RELATIONS_ROOT
					   && strcmp(r->root->file.path, "unowned") == 0;
			raw_pages += r->kind == CLUSTER_RECOVERY_RELATIONS_PAGE && !r->heap;
		}
		if (entry.kind == CLUSTER_RECOVERY_CENSUS_MX_MATCH)
			mismatches += !entry.value.compare.equal;
	}
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_CENSUS_DRAINED);
	UT_ASSERT_EQ(kinds[CLUSTER_RECOVERY_CENSUS_TT_SLOT], 96);
	UT_ASSERT_EQ(kinds[CLUSTER_RECOVERY_CENSUS_TX_RECORD], 2);
	UT_ASSERT(kinds[CLUSTER_RECOVERY_CENSUS_MX_SLOT] > 4096);
	UT_ASSERT_EQ(kinds[CLUSTER_RECOVERY_CENSUS_MX_MATCH], 6);
	UT_ASSERT(kinds[CLUSTER_RECOVERY_CENSUS_NATIVE_EFFECT] > 10);
	UT_ASSERT_EQ(unknown, 1);
	UT_ASSERT(raw_pages > 0);
	UT_ASSERT_EQ(mismatches, 2);
	UT_ASSERT(cluster_recovery_census_diag(census, &diag));
	UT_ASSERT_EQ(diag.phase, CLUSTER_RECOVERY_CENSUS_DONE);
	UT_ASSERT_EQ(diag.inventory.sources, 2);
	UT_ASSERT_EQ(diag.inventory.tt_roots_added, 2);
	UT_ASSERT_EQ(diag.inventory.mx_physical_mismatches, 2);
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
