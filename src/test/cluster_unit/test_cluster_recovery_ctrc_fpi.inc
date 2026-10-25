/* Native protected input, atomic ITL projection and original Generic frame.
 * No WAL insertion, installation, debt clearing or complete census proof. */
#include "cluster/cluster_terminal_ref_census.h"

SCN cluster_scn_advance(void);
SCN
cluster_scn_advance(void)
{
	SCN token = recovery_fpi_insert_scn_advance();

	if (!preparing_input_frame)
		return token;
	switch (input_token_fault) {
	case 1: input_current = false; break;
	case 2: input_work->generation++; break;
	case 3: input_owner.install_generation++; break;
	case 4: fpi_protection_current = false; break;
	case 5: fpi_serial.duty.root_lineage_seq++; break;
	case 6: input_work->counts.unresolved_count++; break;
	case 7: input_work->entries[0].slot.commit_scn++; break;
	case 8: input_itls[0].expected_itl_wrap++; break;
	case 9: input_typed.sources[0].claim.identity.root_lineage_seq++; break;
	case 10: input_identity.incarnation[15]++; break;
	case 11: input_version.mutation_token++; break;
	case 12: return InvalidScn;
	case 13: ereport(ERROR, (errmsg("test frame token error"))); break;
	case 14:
		for (Size i = offsetof(ClusterSpaceIdentityKey, locator) + sizeof(RelFileLocator);
			i < offsetof(ClusterSpaceIdentity, incarnation); i++)
			((unsigned char *)&input_identity)[i] ^= 0xff;
		for (Size i = offsetof(ClusterSpaceIdentity, state) + sizeof(input_identity.state);
			i < sizeof(input_identity); i++)
			((unsigned char *)&input_identity)[i] ^= 0xff;
		break;
	case 15: input_work->entries[input_work->counts.subject_count - 1].slot.commit_scn++; break;
	case 16: input_work->plan = (const ClusterColdPlanV1 *)&input_start_object; break;
	case 17: input_work->top_tx_census = (const struct ClusterColdTopTxCensusV1 *)&input_start_object; break;
	}
	return token;
}

static bool
read_itl_fpi(Size count, RfPageRecoveryCtrcItlFpiV1 *out)
{
	bool result;
	reading_input = preparing_input_frame = true;
	result = rf_page_recovery_ctrc_itl_fpi_smgr_v1(input_start, input_work,
		(RfPageAuthorityGuardV1 *)&fpi_guard_object, &fpi_serial,
		(ClusterWalRetentionPin *)&fpi_pin_object, &input_identity, 7, &input_version,
		input_itls, count, out);
	reading_input = preparing_input_frame = false;
	return result;
}

UT_TEST(ctrc_itl_fpi_prepares_exact_canonical_frame_from_original_input)
{
	for (unsigned checksum = 0; checksum < 2; checksum++)
		for (unsigned count = 1; count <= CLUSTER_ITL_INITRANS_DEFAULT; count++) {
			RfPageRecoveryCtrcItlFpiV1 out;
			PGAlignedBlock before, expected, stored, saved_work;
			ClusterCtrcRecoveryItlPlanV1 plans[CLUSTER_ITL_INITRANS_DEFAULT];
			PageHeader header;
			uint8 identity[CLUSTER_SPACE_IDENTITY_BYTES], actual[CLUSTER_SPACE_IDENTITY_BYTES];
			uint64 old_token;
			reset_itl_input(checksum != 0, count);
			input_token_fault = 0;
			UT_ASSERT_EQ(pread(install_fd, before.data, BLCKSZ, (off_t)7 * BLCKSZ), BLCKSZ);
			for (unsigned i = 0; i < count; i++) {
				plans[i].subject = &input_work->entries[input_itls[i].subject_index];
				plans[i].itl_index = input_itls[i].itl_index;
				plans[i].expected_itl_wrap = input_itls[i].expected_itl_wrap;
				plans[i].itl_class = input_itls[i].itl_class;
			}
			UT_ASSERT_EQ(cluster_ctrc_recovery_itl_batch_image_v1(plans, count,
				before.data, expected.data), CLUSTER_CTRC_ITL_CLEANOUT_REWRITTEN);
			saved_work = input_work_storage;
			old_token = next_token;
			memset(&out, 0xa5, sizeof(out));
			UT_ASSERT(read_itl_fpi(count, &out));
			UT_ASSERT(out.rewritten);
			UT_ASSERT_EQ(input_reads, 5); UT_ASSERT_EQ(opens, 1);
			UT_ASSERT_EQ(next_token, old_token + 1);
			UT_ASSERT_EQ(out.frame.owner, GENERIC_XLOG_CTRC_ITL);
			UT_ASSERT(cluster_space_identity_encode(&input_identity, identity, sizeof(identity)));
			UT_ASSERT(cluster_space_identity_encode(&out.frame.identity, actual, sizeof(actual)));
			UT_ASSERT(memcmp(identity, actual, sizeof(identity)) == 0);
			UT_ASSERT_EQ(out.frame.fork_number, MAIN_FORKNUM);
			UT_ASSERT_EQ(out.frame.block_number, 7);
			UT_ASSERT_EQ(out.frame.result_token, next_token);
			UT_ASSERT(memcmp(&out.frame.version.before, &input_version, sizeof(input_version)) == 0);
			header = (PageHeader)expected.data;
			memset(expected.data + header->pd_lower, 0, header->pd_upper - header->pd_lower);
			header->pd_block_scn = out.frame.result_token;
			UT_ASSERT(memcmp(expected.data, out.frame.image.data, BLCKSZ) == 0);
			UT_ASSERT_EQ(pread(install_fd, stored.data, BLCKSZ, (off_t)7 * BLCKSZ), BLCKSZ);
			UT_ASSERT(memcmp(before.data, stored.data, BLCKSZ) == 0);
			UT_ASSERT(memcmp(&saved_work, &input_work_storage, sizeof(saved_work)) == 0);
			itl_input_debt_unchanged();
		}
}

UT_TEST(ctrc_itl_fpi_unchanged_does_not_mint_a_version)
{
	RfPageRecoveryCtrcItlImageV1 page;
	RfPageRecoveryCtrcItlFpiV1 out, empty = {0};
	uint64 old_token;
	reset_itl_input(true, 3);
	input_token_fault = 0;
	UT_ASSERT(read_itl_input(3, &page));
	install_file_write(&page.image);
	input_reads = opens = 0;
	old_token = next_token;
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(read_itl_fpi(3, &out));
	UT_ASSERT(!out.rewritten);
	UT_ASSERT(memcmp(&out, &empty, sizeof(out)) == 0);
	UT_ASSERT_EQ(input_reads, 5); UT_ASSERT_EQ(opens, 1);
	UT_ASSERT_EQ(next_token, old_token);
	itl_input_debt_unchanged();
}

UT_TEST(ctrc_itl_fpi_ignores_native_space_padding)
{
	RfPageRecoveryCtrcItlFpiV1 out;
	uint8 before[CLUSTER_SPACE_IDENTITY_BYTES], after[CLUSTER_SPACE_IDENTITY_BYTES];
	bool accepted;
	reset_itl_input(false, 2);
	UT_ASSERT(cluster_space_identity_encode(&input_identity, before, sizeof(before)));
	input_token_fault = 14;
	accepted = read_itl_fpi(2, &out);
	input_token_fault = 0;
	UT_ASSERT(accepted);
	if (!accepted)
		return;
	UT_ASSERT(out.rewritten);
	UT_ASSERT(cluster_space_identity_encode(&input_identity, after, sizeof(after)));
	UT_ASSERT(memcmp(before, after, sizeof(before)) == 0);
	UT_ASSERT(cluster_space_identity_encode(&out.frame.identity, after, sizeof(after)));
	UT_ASSERT(memcmp(before, after, sizeof(before)) == 0);
	itl_input_debt_unchanged();
}

UT_TEST(ctrc_itl_fpi_refusals_keep_output_and_original_debt)
{
	for (unsigned at = 1; at <= 5; at++)
		for (unsigned kind = 0; kind <= 14; kind++) {
			RfPageRecoveryCtrcItlFpiV1 out, saved;
			uint64 old_token;
			reset_itl_input(true, 2);
			input_token_fault = 0;
			input_drift_at = at; input_drift_kind = kind;
			old_token = next_token;
			memset(&out, 0xa5, sizeof(out)); saved = out;
			UT_ASSERT(!read_itl_fpi(2, &out));
			UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
			UT_ASSERT_EQ(next_token, old_token);
			itl_input_debt_unchanged();
		}
}

UT_TEST(ctrc_itl_fpi_rechecks_original_inputs_after_token_preparation)
{
	for (unsigned fault = 1; fault <= 11; fault++) {
		RfPageRecoveryCtrcItlFpiV1 out, saved;
		reset_itl_input(false, 2);
		input_token_fault = fault;
		memset(&out, 0xa5, sizeof(out)); saved = out;
		UT_ASSERT(!read_itl_fpi(2, &out));
		UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
		UT_ASSERT_EQ(input_reads, 5);
		itl_input_debt_unchanged();
	}
	input_token_fault = 0;
}

UT_TEST(ctrc_itl_fpi_refuses_output_aliases_and_bounds_before_io)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		RfPageRecoveryCtrcItlFpiV1 out, saved, *output = &out;
		PGAlignedBlock saved_work;
		Size count = 2;
		reset_itl_input(false, 2);
		input_token_fault = 0;
		switch (fault) {
		case 0: count = 0; break;
		case 1: count = CLUSTER_ITL_INITRANS_DEFAULT + 1; break;
		case 2: input_itls[1].itl_index = input_itls[0].itl_index; break;
		case 3: input_itls[1].subject_index = UINT32_MAX; break;
		case 4: output = (RfPageRecoveryCtrcItlFpiV1 *)input_work; break;
		case 5: output = (RfPageRecoveryCtrcItlFpiV1 *)input_itls; break;
		case 6: output = (RfPageRecoveryCtrcItlFpiV1 *)&input_identity; break;
		case 7: MyBackendType = B_BACKEND; break;
		}
		saved_work = input_work_storage;
		memset(&out, 0xa5, sizeof(out)); saved = out;
		UT_ASSERT(!read_itl_fpi(count, output));
		UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
		UT_ASSERT(memcmp(&saved_work, &input_work_storage, sizeof(saved_work)) == 0);
		UT_ASSERT_EQ(input_reads, 0); UT_ASSERT_EQ(opens, 0);
		itl_input_debt_unchanged();
	}
}

UT_TEST(ctrc_itl_fpi_errors_keep_output_and_original_debt)
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
			(void)read_itl_fpi(2, &out);
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
