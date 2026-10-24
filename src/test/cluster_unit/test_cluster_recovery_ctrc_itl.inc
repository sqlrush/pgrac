/* Native reader and actual atomic projector, with real-file storage input. */
#include "cluster/cluster_itl.h"
#include "cluster/cluster_uba.h"
#include "cluster/storage/cluster_undo_alloc.h"

static void
reset_itl_input(bool checksum, unsigned count)
{
	PGAlignedBlock page;
	PageHeader header = (PageHeader)page.data;
	ClusterItlSlotData *slots;
	HeapTupleHeader tuple;

	reset_input(checksum);
	memset(&page, 0, sizeof(page));
	memset(input_itls, 0, sizeof(input_itls));
	header->pd_flags = PD_HAS_ITL;
	header->pd_special = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
	header->pd_lower = SizeOfPageHeaderData + sizeof(ItemIdData);
	header->pd_upper = header->pd_special - MAXALIGN(SizeofHeapTupleHeader);
	header->pd_block_scn = input_version.mutation_token;
	PageSetPageSizeAndVersion((Page)page.data, BLCKSZ, PG_PAGE_LAYOUT_VERSION);
	ItemIdSetNormal(PageGetItemId((Page)page.data, FirstOffsetNumber),
		header->pd_upper, SizeofHeapTupleHeader);
	tuple = (HeapTupleHeader)(page.data + header->pd_upper);
	tuple->t_hoff = SizeofHeapTupleHeader;
	tuple->t_infomask = HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK;
	HeapTupleHeaderSetXmin(tuple, 800);
	HeapTupleHeaderSetXmax(tuple, 7001);
	slots = ClusterPageGetItlSlots((Page)page.data);
	input_work->counts.subject_count = count;
	input_work->counts.unresolved_count = count;
	for (unsigned i = 0; i < count; i++) {
		ClusterColdTtSlotInputV1 *subject = &input_work->entries[i];
		subject->key.owner_instance = 2;
		subject->key.segment_id = CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
		subject->key.segment_generation = i;
		subject->key.slot_offset = 47 - i;
		subject->key.xid = 7001 + i;
		subject->key.slot_wrap = 9 + i;
		subject->slot.xid = subject->key.xid;
		subject->slot.wrap = subject->key.slot_wrap;
		subject->slot.status = i % 3 == 1 ? TT_SLOT_ABORTED : TT_SLOT_COMMITTED;
		subject->slot.commit_scn = i % 3 == 1 ? InvalidScn : 900 + i;
		subject->slot.first_undo_block = uba_encode(subject->key.segment_id, 40 + i, 47 - i, 100);
		input_itls[i].subject_index = i;
		input_itls[i].itl_index = i;
		input_itls[i].expected_itl_wrap = 3 + i;
		input_itls[i].itl_class = i % 2 == 0 ? 2 : 1;
		slots[i].xid = subject->key.xid;
		slots[i].wrap = 3 + i;
		slots[i].flags = i % 2 == 0 ? ITL_FLAG_LOCK_ONLY_ACTIVE : ITL_FLAG_ACTIVE;
		slots[i].undo_segment_head = uba_encode(subject->key.segment_id, 12 + i, 47 - i, 132);
	}
	install_file_write(&page);
}

static bool
read_itl_input(Size count, RfPageRecoveryCtrcItlImageV1 *out)
{
	bool result;
	reading_input = true;
	result = rf_page_recovery_ctrc_itl_image_smgr_v1(input_start, input_work,
		(RfPageAuthorityGuardV1 *)&fpi_guard_object, &fpi_serial,
		(ClusterWalRetentionPin *)&fpi_pin_object, &input_identity, 7, &input_version,
		input_itls, count, out);
	reading_input = false;
	return result;
}

static void
itl_input_debt_unchanged(void)
{
	UT_ASSERT_EQ(writes, 0); UT_ASSERT_EQ(syncs, 0);
	UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
	UT_ASSERT(!inserted.target_synced);
	assert_install_pending(false);
}

UT_TEST(ctrc_itl_input_projects_only_original_selected_entries)
{
	for (unsigned checksum = 0; checksum < 2; checksum++)
		for (unsigned count = 1; count <= CLUSTER_ITL_INITRANS_DEFAULT; count++) {
			RfPageRecoveryCtrcItlImageV1 out;
			PGAlignedBlock before, after, saved_work;
			ClusterItlSlotData *slots;
			HeapTupleHeader tuple;
			reset_itl_input(checksum != 0, count);
			if (count > 1) {
				ClusterColdTtSlotInputV1 first = input_work->entries[0];
				input_work->entries[0] = input_work->entries[count - 1];
				input_work->entries[count - 1] = first;
				input_itls[0].subject_index = count - 1;
				input_itls[count - 1].subject_index = 0;
			}
			saved_work = input_work_storage;
			UT_ASSERT_EQ(pread(install_fd, before.data, BLCKSZ, (off_t)7 * BLCKSZ), BLCKSZ);
			UT_ASSERT(read_itl_input(count, &out));
			UT_ASSERT(out.rewritten);
			UT_ASSERT_EQ(input_reads, 5); UT_ASSERT_EQ(opens, 1);
			slots = ClusterPageGetItlSlots((Page)out.image.data);
			for (unsigned i = 0; i < count; i++) {
				uint16 flags = i % 3 == 1
					? (i % 2 == 0 ? ITL_FLAG_LOCK_ONLY_ABORTED : ITL_FLAG_ABORTED)
					: (i % 2 == 0 ? ITL_FLAG_LOCK_ONLY_COMMITTED : ITL_FLAG_COMMITTED);
				UT_ASSERT_EQ(slots[i].flags, flags);
				UT_ASSERT_EQ(slots[i].commit_scn, i % 3 == 1 ? InvalidScn : 900 + i);
				UT_ASSERT_EQ(slots[i].xid, 7001 + i); UT_ASSERT_EQ(slots[i].wrap, 3 + i);
			}
			UT_ASSERT(memcmp(out.image.data, before.data, SizeOfPageHeaderData) == 0);
			UT_ASSERT(memcmp(&slots[count], &ClusterPageGetItlSlots((Page)before.data)[count],
				(CLUSTER_ITL_INITRANS_DEFAULT - count) * sizeof(*slots)) == 0);
			tuple = (HeapTupleHeader)PageGetItem((Page)out.image.data,
				PageGetItemId((Page)out.image.data, FirstOffsetNumber));
			UT_ASSERT((tuple->t_infomask & HEAP_XMAX_INVALID) != 0);
			UT_ASSERT(memcmp(&saved_work, &input_work_storage, sizeof(saved_work)) == 0);
			UT_ASSERT_EQ(pread(install_fd, after.data, BLCKSZ, (off_t)7 * BLCKSZ), BLCKSZ);
			UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
			itl_input_debt_unchanged();
		}
}

UT_TEST(ctrc_itl_input_reports_idempotence_without_writing_storage)
{
	RfPageRecoveryCtrcItlImageV1 first, second;
	PGAlignedBlock before;
	reset_itl_input(true, 3);
	UT_ASSERT(read_itl_input(3, &first));
	install_file_write(&first.image);
	UT_ASSERT_EQ(pread(install_fd, before.data, BLCKSZ, (off_t)7 * BLCKSZ), BLCKSZ);
	UT_ASSERT(read_itl_input(3, &second));
	UT_ASSERT(!second.rewritten);
	UT_ASSERT(memcmp(&second.image, &before, sizeof(before)) == 0);
	itl_input_debt_unchanged();
}

UT_TEST(ctrc_itl_input_late_refusal_preserves_complete_output)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		RfPageRecoveryCtrcItlImageV1 out, saved;
		reset_itl_input(true, 3);
		switch (fault) {
		case 0: input_work->entries[2].slot.status = TT_SLOT_ACTIVE; break;
		case 1: input_work->entries[2].slot.commit_scn = InvalidScn; break;
		case 2: input_itls[2].expected_itl_wrap++; break;
		case 3: input_work->entries[2].key.xid++; break;
		case 4: input_work->entries[2].key.slot_offset--; break;
		case 5: input_work->entries[2].slot.flags = 0x80; break;
		}
		memset(&out, 0xa5, sizeof(out)); saved = out;
		UT_ASSERT(!read_itl_input(3, &out));
		UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
		itl_input_debt_unchanged();
	}
}

UT_TEST(ctrc_itl_input_refuses_bounds_duplicates_and_aliases_before_io)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		RfPageRecoveryCtrcItlImageV1 out, saved;
		Size count = 2;
		reset_itl_input(false, 2);
		switch (fault) {
		case 0: count = 0; break;
		case 1: count = CLUSTER_ITL_INITRANS_DEFAULT + 1; break;
		case 2: input_itls[1].subject_index = UINT32_MAX; break;
		case 3: input_itls[1].itl_index = CLUSTER_ITL_INITRANS_DEFAULT; break;
		case 4: input_itls[1].itl_index = 0; break;
		case 5: input_itls[1].itl_class = 0; break;
		case 6: input_itls[1].subject_index = input_work->counts.subject_count; break;
		case 7: count = (Size)-1; break;
		}
		memset(&out, 0xa5, sizeof(out)); saved = out;
		UT_ASSERT(!read_itl_input(count, &out));
		UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
		UT_ASSERT_EQ(input_reads, 0);
		itl_input_debt_unchanged();
	}
	for (unsigned fault = 0; fault < 3; fault++) {
		PGAlignedBlock saved;
		RfPageRecoveryCtrcItlImageV1 *out;
		reset_itl_input(false, 2);
		saved = input_work_storage;
		out = fault == 0 ? (RfPageRecoveryCtrcItlImageV1 *)input_work
			: fault == 1 ? (RfPageRecoveryCtrcItlImageV1 *)input_work->entries
			: (RfPageRecoveryCtrcItlImageV1 *)input_itls;
		UT_ASSERT(!read_itl_input(2, out));
		UT_ASSERT(memcmp(&saved, &input_work_storage, sizeof(saved)) == 0);
		UT_ASSERT_EQ(input_itls[0].subject_index, 0);
		UT_ASSERT_EQ(input_itls[1].subject_index, 1);
		UT_ASSERT_EQ(input_reads, 0);
		itl_input_debt_unchanged();
	}
}

UT_TEST(ctrc_itl_input_rechecks_original_scope_and_selected_bytes)
{
	for (unsigned at = 1; at <= 5; at++)
		for (unsigned kind = 0; kind <= 14; kind++) {
			RfPageRecoveryCtrcItlImageV1 out, saved;
			bool accepted;
			reset_itl_input(true, 2);
			input_drift_at = at; input_drift_kind = kind;
			memset(&out, 0xa5, sizeof(out)); saved = out;
			accepted = read_itl_input(2, &out);
			if (accepted)
				printf("# unexpected acceptance: read=%u drift=%u\n", at, kind);
			UT_ASSERT(!accepted);
			UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
			itl_input_debt_unchanged();
		}
}

UT_TEST(ctrc_itl_input_refuses_wrong_actor_before_borrowing_work)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		RfPageRecoveryCtrcItlImageV1 out, saved;
		reset_itl_input(false, 2);
		switch (fault) {
		case 0: cluster_enabled = false; break;
		case 1: cluster_shared_config = false; break;
		case 2: IsUnderPostmaster = false; break;
		case 3: MyBackendType = B_BACKEND; break;
		case 4: MyProcPid = 0; break;
		case 5: CritSectionCount = 1; break;
		case 6: ShutdownRequestPending = true; break;
		case 7: recovering = true; break;
		}
		input_current_checks = 0;
		memset(&out, 0xa5, sizeof(out)); saved = out;
		UT_ASSERT(!read_itl_input(2, &out));
		UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
		UT_ASSERT_EQ(input_current_checks, 0);
		UT_ASSERT_EQ(input_reads, 0); UT_ASSERT_EQ(opens, 0);
		CritSectionCount = 0;
		itl_input_debt_unchanged();
	}
}

UT_TEST(ctrc_itl_input_errors_preserve_output_and_owners)
{
	for (unsigned at = 1; at <= 5; at++) {
		volatile bool caught = false;
		RfPageRecoveryCtrcItlImageV1 out, saved;
		reset_itl_input(true, 2);
		input_throw_at = at;
		memset(&out, 0xa5, sizeof(out)); saved = out;
		PG_TRY();
		{
			(void)read_itl_input(2, &out);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		reading_input = false;
		UT_ASSERT(caught && raised_level == ERROR);
		UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
		itl_input_debt_unchanged();
	}
}
