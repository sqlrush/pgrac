/* Complete supplied native input, original protected reader and Generic frame. */
static ClusterColdTtSlotInputV1 page_fpi_scratch[CLUSTER_ITL_INITRANS_DEFAULT + 1];

static void
page_fpi_add_unrelated_subject(void)
{
	Size n = input_work->counts.subject_count;

	input_work->entries[n] = input_work->entries[0];
	input_work->entries[n].key.xid = input_work->entries[n].slot.xid = 8000;
	input_work->entries[n].slot.status = TT_SLOT_ACTIVE;
	input_work->entries[n].slot.commit_scn = InvalidScn;
	input_work->counts.subject_count++;
	input_work->counts.unresolved_count++;
}

static bool
read_page_fpi(ClusterColdTtSlotInputV1 *scratch, Size capacity,
	RfPageRecoveryCtrcItlFpiV1 *out)
{
	bool result;

	reading_input = preparing_input_frame = true;
	result = rf_page_recovery_ctrc_itl_page_fpi_smgr_v1(input_start, input_work,
		(RfPageAuthorityGuardV1 *)&fpi_guard_object, &fpi_serial,
		(ClusterWalRetentionPin *)&fpi_pin_object, &input_identity, 7,
		&input_version, scratch, capacity, out);
	reading_input = preparing_input_frame = false;
	return result;
}

static void
page_fpi_refused(void)
{
	RfPageRecoveryCtrcItlFpiV1 out, saved;

	memset(&out, 0xa5, sizeof(out));
	saved = out;
	UT_ASSERT(!read_page_fpi(page_fpi_scratch, lengthof(page_fpi_scratch), &out));
	UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	itl_input_debt_unchanged();
}

UT_TEST(ctrc_page_fpi_covers_all_native_itls_without_selections)
{
	for (unsigned checksum = 0; checksum < 2; checksum++)
		for (unsigned count = 1; count <= CLUSTER_ITL_INITRANS_DEFAULT; count++) {
			RfPageRecoveryCtrcItlFpiV1 out;
			PGAlignedBlock before, after, saved_work;
			ClusterItlSlotData *slots;
			HeapTupleHeader tuple;
			uint64 token;

			reset_itl_input(checksum != 0, count);
			input_token_fault = 0;
			page_fpi_add_unrelated_subject();
			for (unsigned i = 0; i < (count + 1) / 2; i++) {
				ClusterColdTtSlotInputV1 tmp = input_work->entries[i];
				input_work->entries[i] = input_work->entries[count - i];
				input_work->entries[count - i] = tmp;
			}
			memset(input_itls, 0xff, sizeof(input_itls));
			saved_work = input_work_storage;
			UT_ASSERT_EQ(pread(install_fd, before.data, BLCKSZ, (off_t)7 * BLCKSZ), BLCKSZ);
			token = next_token;
			UT_ASSERT(read_page_fpi(page_fpi_scratch, lengthof(page_fpi_scratch), &out));
			UT_ASSERT(out.rewritten);
			UT_ASSERT_EQ(input_reads, 5); UT_ASSERT_EQ(opens, 1);
			UT_ASSERT_EQ(next_token, token + 1);
			UT_ASSERT_EQ(out.frame.owner, GENERIC_XLOG_CTRC_ITL);
			UT_ASSERT_EQ(out.frame.fork_number, MAIN_FORKNUM);
			UT_ASSERT_EQ(out.frame.block_number, 7);
			UT_ASSERT_EQ(out.frame.result_token, next_token);
			UT_ASSERT(memcmp(&out.frame.version.before, &input_version, sizeof(input_version)) == 0);
			slots = ClusterPageGetItlSlots((Page)out.frame.image.data);
			for (unsigned i = 0; i < count; i++) {
				uint16 flags = i % 3 == 1
					? (i % 2 == 0 ? ITL_FLAG_LOCK_ONLY_ABORTED : ITL_FLAG_ABORTED)
					: (i % 2 == 0 ? ITL_FLAG_LOCK_ONLY_COMMITTED : ITL_FLAG_COMMITTED);
				UT_ASSERT_EQ(slots[i].flags, flags);
				UT_ASSERT_EQ(slots[i].commit_scn, i % 3 == 1 ? InvalidScn : 900 + i);
				UT_ASSERT_EQ(slots[i].xid, 7001 + i); UT_ASSERT_EQ(slots[i].wrap, 3 + i);
			}
			tuple = (HeapTupleHeader)PageGetItem((Page)out.frame.image.data,
				PageGetItemId((Page)out.frame.image.data, FirstOffsetNumber));
			UT_ASSERT((tuple->t_infomask & HEAP_XMAX_INVALID) != 0);
			for (Size i = ((PageHeader)out.frame.image.data)->pd_lower;
				i < ((PageHeader)out.frame.image.data)->pd_upper; i++)
				UT_ASSERT_EQ(out.frame.image.data[i], 0);
			UT_ASSERT(memcmp(page_fpi_scratch, input_work->entries,
				input_work->counts.subject_count * sizeof(*page_fpi_scratch)) == 0);
			UT_ASSERT(memcmp(&saved_work, &input_work_storage, sizeof(saved_work)) == 0);
			UT_ASSERT_EQ(pread(install_fd, after.data, BLCKSZ, (off_t)7 * BLCKSZ), BLCKSZ);
			UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
			itl_input_debt_unchanged();
		}
}

UT_TEST(ctrc_page_fpi_empty_and_terminal_results_do_not_mint_tokens)
{
	for (unsigned count = 0; count <= 3; count += 3) {
		RfPageRecoveryCtrcItlFpiV1 out, empty = {0};
		RfPageRecoveryCtrcItlImageV1 clean;
		uint64 token;

		reset_itl_input(true, count);
		input_token_fault = 0;
		if (count != 0) {
			UT_ASSERT(read_itl_input(count, &clean));
			install_file_write(&clean.image);
		}
		input_reads = opens = 0;
		token = next_token;
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT(read_page_fpi(count != 0 ? page_fpi_scratch : NULL, count, &out));
		UT_ASSERT(memcmp(&out, &empty, sizeof(out)) == 0);
		UT_ASSERT_EQ(input_reads, 5); UT_ASSERT_EQ(next_token, token);
		itl_input_debt_unchanged();
	}
}

UT_TEST(ctrc_page_fpi_missing_ambiguous_or_unqualified_late_input_refuses)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		PGAlignedBlock page;
		ClusterItlSlotData *slots;

		reset_itl_input(true, 3);
		input_token_fault = 0;
		UT_ASSERT_EQ(pread(install_fd, page.data, BLCKSZ, (off_t)7 * BLCKSZ), BLCKSZ);
		slots = ClusterPageGetItlSlots((Page)page.data);
		switch (fault) {
		case 0: input_work->counts.subject_count = 2; break;
		case 1:
			input_work->entries[3] = input_work->entries[2];
			input_work->entries[3].key.segment_generation++;
			input_work->counts.subject_count = 4;
			break;
		case 2: input_work->entries[2].slot.status = TT_SLOT_ACTIVE; break;
		case 3: input_work->entries[2].slot.commit_scn = InvalidScn; break;
		case 4: slots[2].flags = 0x80; install_file_write(&page); break;
		case 5: slots[2].flags = ITL_FLAG_FREE; install_file_write(&page); break;
		case 6: ItemIdSetNormal(PageGetItemId((Page)page.data, FirstOffsetNumber),
			((PageHeader)page.data)->pd_upper, SizeofHeapTupleHeader - 1);
			install_file_write(&page); break;
		}
		page_fpi_refused();
	}
}

UT_TEST(ctrc_page_fpi_rechecks_all_subject_bytes_after_each_read)
{
	for (unsigned at = 1; at <= 5; at++)
		for (unsigned kind = 0; kind <= 15; kind++) {
			if (kind == 12 || kind == 14) /* No selected-prefix input. */
				continue;
			reset_itl_input(true, CLUSTER_ITL_INITRANS_DEFAULT);
			page_fpi_add_unrelated_subject();
			input_token_fault = 0;
			input_drift_at = at; input_drift_kind = kind;
			page_fpi_refused();
		}
}

UT_TEST(ctrc_page_fpi_rechecks_full_input_after_token_preparation)
{
	for (unsigned fault = 1; fault <= 17; fault++) {
		RfPageRecoveryCtrcItlFpiV1 out;
		reset_itl_input(false, CLUSTER_ITL_INITRANS_DEFAULT);
		page_fpi_add_unrelated_subject();
		input_token_fault = fault;
		if (fault == 8 || fault == 14) {
			UT_ASSERT(read_page_fpi(page_fpi_scratch, lengthof(page_fpi_scratch), &out));
			UT_ASSERT(out.rewritten);
		} else if (fault != 12 && fault != 13) {
			page_fpi_refused();
		}
	}
	input_token_fault = 0;
}

UT_TEST(ctrc_page_fpi_rejects_capacity_and_overlap_before_io)
{
	for (unsigned fault = 0; fault < 12; fault++) {
		RfPageRecoveryCtrcItlFpiV1 out, saved, *output = &out;
		ClusterColdTtSlotInputV1 *scratch = page_fpi_scratch;
		Size capacity = lengthof(page_fpi_scratch);
		PGAlignedBlock saved_work;

		reset_itl_input(false, 2);
		input_token_fault = 0;
		memset(&out, 0xa5, sizeof(out)); saved = out;
		switch (fault) {
		case 0: scratch = NULL; break;
		case 1: capacity = 1; break;
		case 2: capacity = (Size)-1; break;
		case 3: scratch = input_work->entries; break;
		case 4: scratch = (void *)&input_identity; break;
		case 5: scratch = (void *)((char *)output + sizeof(*output) - 1); break;
		case 6: output = (void *)page_fpi_scratch; break;
		case 7: output = (void *)input_work; break;
		case 8: output = (void *)&input_typed; break;
		case 9: scratch = (void *)&fpi_serial; break;
		case 10: scratch = (void *)&input_version; break;
		case 11: input_work->counts.subject_count = UINT32_MAX; break;
		}
		saved_work = input_work_storage;
		UT_ASSERT(!read_page_fpi(scratch, capacity, output));
		UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
		UT_ASSERT(memcmp(&saved_work, &input_work_storage, sizeof(saved_work)) == 0);
		UT_ASSERT_EQ(input_reads, 0); UT_ASSERT_EQ(opens, 0);
		itl_input_debt_unchanged();
	}
}

UT_TEST(ctrc_page_fpi_keeps_original_scope_guards)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		reset_itl_input(false, 2);
		input_token_fault = 0;
		switch (fault) {
		case 0: MyBackendType = B_BACKEND; break;
		case 1: input_current = false; break;
		case 2: input_work->generation = 0; break;
		case 3: input_owner.install_generation = 0; break;
		case 4: fpi_protection_current = false; break;
		case 5: fpi_serial.mode = 0; break;
		case 6: fpi_serial.release_uncertain = true; break;
		case 7: input_typed.replay_count = 0; break;
		}
		page_fpi_refused();
		UT_ASSERT_EQ(input_reads, 0);
	}
}

UT_TEST(ctrc_page_fpi_errors_preserve_output_and_debt)
{
	for (unsigned at = 1; at <= 7; at++) {
		volatile bool caught = false;
		RfPageRecoveryCtrcItlFpiV1 out, saved;
		reset_itl_input(true, 2);
		input_throw_at = at <= 5 ? at : 0;
		input_token_fault = at == 6 ? 12 : at == 7 ? 13 : 0;
		memset(&out, 0xa5, sizeof(out)); saved = out;
		PG_TRY();
		{
			(void)read_page_fpi(page_fpi_scratch, lengthof(page_fpi_scratch), &out);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		reading_input = preparing_input_frame = false;
		UT_ASSERT(caught && raised_level == ERROR);
		UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
		itl_input_debt_unchanged();
	}
	input_token_fault = 0;
}
