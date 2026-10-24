/* Actual native SMGR reader and SPACE codecs; real files, no release proof. */
static ClusterSpaceIdentity input_identity;
static RfPageVersionV1 input_version;

static void
input_space_write(BlockNumber block, const PGAlignedBlock *page)
{
	PGAlignedBlock copy = *page;
	((PageHeader)copy.data)->pd_checksum = 0;
	if (checksums)
		((PageHeader)copy.data)->pd_checksum = pg_checksum_page(copy.data, block);
	UT_ASSERT_EQ(pwrite(input_space_fd, copy.data, BLCKSZ, (off_t)block * BLCKSZ), BLCKSZ);
}

static void
reset_input(bool enable_checksums)
{
	ClusterSpaceReservation reservation;
	PGAlignedBlock page;
	reset_install(enable_checksums);
	memset(&input_typed, 0, sizeof(input_typed));
	memset(&input_work_storage, 0, sizeof(input_work_storage));
	memset(&input_owner, 0, sizeof(input_owner));
	input_current = true;
	input_reads = input_throw_at = input_drift_at = input_drift_kind = 0;
	input_missing_space = false;
	input_space_blocks = 2;
	input_work->generation = 1;
	input_work->counts.subject_count = 1;
	input_typed.ctrc_work = input_work;
	input_typed.ctrc_work_generation = 1;
	input_typed.participant_count = input_typed.replay_count = 1;
	input_typed.sources[0].claim = owner.target;
	input_owner.writer = installed_writer;
	memset(input_owner.operation_uuid, 8, 16);
	input_owner.install_generation = 9;
	input_owner.start_generation = 10;
	input_owner.start_serial = 30;
	input_owner.owner_boot = 20;
	input_owner.owner_pid = MyProcPid;
	input_identity = frame.identity;
	input_version = frame.version.before;
	reservation.identity = input_identity;
	reservation.next_block = 8;
	strlcpy(input_space_path, "/tmp/pgrac-ctrc-space-XXXXXX", sizeof(input_space_path));
	input_space_fd = mkstemp(input_space_path);
	UT_ASSERT(input_space_fd >= 0);
	UT_ASSERT_EQ(ftruncate(input_space_fd, (off_t)2 * BLCKSZ), 0);
	UT_ASSERT(cluster_space_identity_page_encode(&input_identity, 55, page.data, BLCKSZ));
	input_space_write(0, &page);
	UT_ASSERT(cluster_space_reservation_page_encode(&reservation, 56, page.data, BLCKSZ));
	input_space_write(1, &page);
	UT_ASSERT_EQ(fsync(input_space_fd), 0);
}

static bool
read_input(PGAlignedBlock *out)
{
	bool result;
	reading_input = true;
	result = rf_page_recovery_ctrc_read_smgr_v1(input_start, input_work,
		(RfPageAuthorityGuardV1 *)&fpi_guard_object, &fpi_serial,
		(ClusterWalRetentionPin *)&fpi_pin_object, &input_identity, 7, &input_version, out);
	reading_input = false;
	return result;
}

static void
input_refused_unchanged(void)
{
	PGAlignedBlock out, saved;
	memset(&out, 0xa5, sizeof(out));
	saved = out;
	UT_ASSERT(!read_input(&out));
	UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	UT_ASSERT_EQ(writes, 0); UT_ASSERT_EQ(syncs, 0);
	UT_ASSERT(!inserted.target_synced);
	UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_INSERTED);
	UT_ASSERT(!region.thread_replay[2].stamp.failed);
}

UT_TEST(ctrc_input_reads_exact_native_page_without_mutation)
{
	for (unsigned checksum = 0; checksum < 2; checksum++) {
		PGAlignedBlock out, disk;
		reset_input(checksum != 0);
		UT_ASSERT(read_input(&out));
		UT_ASSERT_EQ(pread(install_fd, disk.data, BLCKSZ, (off_t)7 * BLCKSZ), BLCKSZ);
		UT_ASSERT(memcmp(out.data, disk.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(input_reads, 5); UT_ASSERT_EQ(opens, 1);
		UT_ASSERT_EQ(writes, 0); UT_ASSERT_EQ(syncs, 0);
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT(!inserted.target_synced);
		assert_install_pending(false);
	}
}

UT_TEST(ctrc_input_refuses_wrong_original_scope_before_io)
{
	for (unsigned fault = 0; fault < 19; fault++) {
		reset_input(false);
		switch (fault) {
		case 0: input_current = false; break;
		case 1: input_work->generation = 0; break;
		case 2: input_owner.owner_pid++; break;
		case 3: input_typed.replay_count = 0; break;
		case 4: input_typed.replay_count = input_typed.participant_count + 1; break;
		case 5: input_typed.participant_count = CLUSTER_COLD_MAX_PARTICIPANTS + 1; break;
		case 6: fpi_serial.held = false; break;
		case 7: fpi_serial.release_uncertain = true; break;
		case 8: fpi_serial.mode = CLUSTER_RECOVERY_SERIAL_INPUT_SEAL; break;
		case 9: fpi_protection_current = false; break;
		case 10: MyBackendType = B_BACKEND; break;
		case 11: ShutdownRequestPending = true; break;
		case 12: recovering = true; break;
		case 13: cluster_shared_config = false; break;
		case 14: IsUnderPostmaster = false; break;
		case 15: input_identity.key.database_incarnation++; break;
		case 16: input_identity.key.storage_uuid[15]++; break;
		case 17: input_version.segment_incarnation[15]++; break;
		case 18: input_identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED; break;
		}
		input_refused_unchanged();
		UT_ASSERT_EQ(input_reads, 0); UT_ASSERT_EQ(opens, 0);
	}
}

UT_TEST(ctrc_input_refuses_wrong_space_or_page)
{
	for (unsigned fault = 0; fault < 11; fault++) {
		PGAlignedBlock page;
		ClusterSpaceIdentity other;
		ClusterSpaceReservation reservation;
		reset_input(true);
		other = input_identity;
		other.sequence++;
		reservation.identity = input_identity;
		reservation.next_block = 8;
		switch (fault) {
		case 0: input_missing_space = true; break;
		case 1: input_space_blocks = 1; break;
		case 2: input_space_blocks = 3; break;
		case 3: missing_target = true; break;
		case 4:
			UT_ASSERT(cluster_space_identity_page_encode(&other, 55, page.data, BLCKSZ));
			input_space_write(0, &page); break;
		case 5: reservation.identity = other;
			UT_ASSERT(cluster_space_reservation_page_encode(&reservation, 56, page.data, BLCKSZ));
			input_space_write(1, &page); break;
		case 6: reservation.next_block = 7;
			UT_ASSERT(cluster_space_reservation_page_encode(&reservation, 56, page.data, BLCKSZ));
			input_space_write(1, &page); break;
		case 7: page = predecessor; ((PageHeader)page.data)->pd_block_scn++;
			install_file_write(&page); break;
		case 8: memset(&page, 0, sizeof(page)); install_file_write(&page); break;
		case 9:
			UT_ASSERT_EQ(pread(install_fd, page.data, BLCKSZ, (off_t)7 * BLCKSZ), BLCKSZ);
			page.data[SizeOfPageHeaderData] ^= 1;
			UT_ASSERT_EQ(pwrite(install_fd, page.data, BLCKSZ, (off_t)7 * BLCKSZ), BLCKSZ);
			break;
		case 10:
			UT_ASSERT_EQ(pread(input_space_fd, page.data, BLCKSZ, 0), BLCKSZ);
			((PageHeader)page.data)->pd_checksum ^= 1;
			UT_ASSERT_EQ(pwrite(input_space_fd, page.data, BLCKSZ, 0), BLCKSZ);
			break;
		}
		input_refused_unchanged();
	}
}

UT_TEST(ctrc_input_rechecks_scope_at_every_read)
{
	for (unsigned at = 1; at <= 5; at++) {
		for (unsigned kind = 0; kind < 9; kind++) {
			reset_input(true);
			input_drift_at = at;
			input_drift_kind = kind;
			input_refused_unchanged();
			UT_ASSERT_EQ(input_reads, at);
		}
	}
}

UT_TEST(ctrc_input_rechecks_namespace_bytes_after_main)
{
	for (unsigned at = 4; at <= 5; at++) {
		reset_input(false);
		input_drift_at = at;
		input_drift_kind = 9;
		input_refused_unchanged();
		UT_ASSERT_EQ(input_reads, at);
	}
}

UT_TEST(ctrc_input_errors_preserve_output_and_debt)
{
	for (unsigned at = 1; at <= 5; at++) {
		volatile bool caught = false;
		PGAlignedBlock out, saved;
		reset_input(true);
		memset(&out, 0xa5, sizeof(out)); saved = out;
		input_throw_at = at;
		PG_TRY();
		{
			(void)read_input(&out);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		reading_input = false;
		UT_ASSERT(caught && raised_level == ERROR);
		UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
		UT_ASSERT_EQ(writes, 0); UT_ASSERT_EQ(syncs, 0);
		UT_ASSERT(!inserted.target_synced);
		assert_install_pending(false);
	}
}

UT_TEST(ctrc_input_refuses_known_output_aliases)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		PGAlignedBlock *out;
		reset_input(false);
		out = fault == 0 ? (PGAlignedBlock *)&input_identity
			: fault == 1 ? (PGAlignedBlock *)&input_version
			: fault == 2 ? (PGAlignedBlock *)&fpi_serial
			: fault == 3 ? (PGAlignedBlock *)input_work
			: (PGAlignedBlock *)input_work->entries;
		UT_ASSERT(!read_input(out));
		UT_ASSERT_EQ(input_reads, 0); UT_ASSERT_EQ(opens, 0);
		UT_ASSERT_EQ(writes, 0); UT_ASSERT_EQ(syncs, 0);
	}
}

UT_TEST(ctrc_input_accepts_only_original_replay_serial)
{
	PGAlignedBlock out;
	reset_input(false);
	input_typed.participant_count = 2;
	input_typed.sources[1] = input_typed.sources[0];
	input_typed.sources[0].claim.identity.root_lineage_seq++;
	input_refused_unchanged();
	UT_ASSERT_EQ(opens, 0);
	input_typed.replay_count = 2;
	UT_ASSERT(read_input(&out));
	assert_install_pending(false);
}

UT_TEST(ctrc_input_refuses_absent_or_foreign_handles)
{
	for (unsigned fault = 0; fault < 11; fault++) {
		PGAlignedBlock out, saved;
		reset_input(false);
		memset(&out, 0xa5, sizeof(out)); saved = out;
		reading_input = true;
		UT_ASSERT(!rf_page_recovery_ctrc_read_smgr_v1(
			fault == 0 ? NULL : fault == 1 ? (struct ClusterRecoveryStartV1 *)&out : input_start,
			fault == 2 ? NULL : fault == 3 ? (ClusterColdCtrcWorkV1 *)&out : input_work,
			fault == 4 ? NULL : (RfPageAuthorityGuardV1 *)&fpi_guard_object,
			fault == 5 ? NULL : &fpi_serial,
			fault == 6 ? NULL : (ClusterWalRetentionPin *)&fpi_pin_object,
			fault == 7 ? NULL : &input_identity,
			fault == 8 ? InvalidBlockNumber : 7,
			fault == 9 ? NULL : &input_version, fault == 10 ? NULL : &out));
		reading_input = false;
		UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
		UT_ASSERT_EQ(input_reads, 0); UT_ASSERT_EQ(opens, 0);
	}
}
