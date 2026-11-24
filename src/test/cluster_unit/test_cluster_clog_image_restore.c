/* Native full-image reconstruction; no replay custody or I/O is simulated.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "access/clog.h"
#include "access/clog_wal.h"
#ifdef CLOG_RESTORE_TEST_SOURCE
#include CLOG_RESTORE_TEST_SOURCE
#else
#include "../../common/cluster_clog_wal.c"
#endif
#ifdef CLOG_RESTORE_REFUSING_SCAFFOLD
#include CLOG_RESTORE_REFUSING_SCAFFOLD
#endif
#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

static ClusterClogFileBindingV2 binding;
static ClusterClogPageIdentityV2 image_page, current_page, restored;
static uint8 record[8384], current[8256], output[8256], saved[8256];
static uint8 image_payload[8192], current_payload[8192], restored_payload[8192];

void
ExceptionalCondition(const char *condition, const char *source, int line)
{
	fprintf(stderr, "%s at %s:%d\n", condition, source, line);
	abort();
}

static void
seal(void)
{
	if (!cluster_recovery_file_identity_encode_v2(&binding.identity, record, 128) ||
		!cluster_clog_file_page_encode_v2(&binding, &image_page, image_payload, 8192,
										 record + 128, 8256) ||
		!cluster_clog_file_page_encode_v2(&binding, &current_page, current_payload, 8192,
										 current, 8256))
		abort();
}

static void
reset(uint64 logical_page)
{
	memset(&binding, 0, sizeof(binding));
	binding.identity.kind = CLUSTER_RECOVERY_FILE_CLOG_V2;
	binding.identity.format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
	binding.identity.format_version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	binding.identity.system_identifier = 123;
	binding.identity.database_incarnation = 456;
	binding.identity.storage_uuid[0] = 1;
	binding.identity.redo_branch_uuid[0] = 2;
	binding.identity.redo_branch_generation = 2;
	binding.identity.redo_timeline = 1;
	binding.identity.creation_generation = 3;
	binding.identity.registration_generation = 4;
	binding.identity.origin = 2;
	binding.identity.object_id = binding.segment = logical_page / 32;
	memset(&image_page, 0, sizeof(image_page));
	image_page.identity.logical_page = logical_page;
	image_page.identity.page_generation = 9;
	image_page.identity.birth_observed = true;
	image_page.identity.birth_full_xid = FullTransactionIdFromU64(
		(UINT64CONST(7) << 32) + logical_page * 32768 + (logical_page == 0 ? 3 : 0));
	image_page.identity.page_lsn = 80;
	image_page.image_lsn = 100;
	current_page = image_page;
	memset(image_payload, 0, sizeof(image_payload));
	memset(current_payload, 0, sizeof(current_payload));
	memset(output, 0xa5, sizeof(output));
	memcpy(saved, output, sizeof(output));
	seal();
}

static bool
restore(bool have_current, XLogRecPtr end)
{
	return cluster_clog_wal_page_restore_v2(&binding, record, sizeof(record), end,
										  have_current ? current : NULL,
										  have_current ? sizeof(current) : 0,
										  output, sizeof(output));
}

static void
decode(void)
{
	UT_ASSERT(cluster_clog_file_page_decode_v2(&binding, image_page.identity.logical_page,
		output, sizeof(output), &restored, restored_payload, sizeof(restored_payload)));
}

static void
unchanged(void)
{
	UT_ASSERT(memcmp(output, saved, sizeof(output)) == 0);
}

UT_TEST(selected_image_uses_actual_end_and_distinct_source_watermark)
{
	reset(64);
	memset(image_payload, 0x55, sizeof(image_payload));
	seal();
	UT_ASSERT(restore(false, 200));
	decode();
	UT_ASSERT_EQ(restored.image_lsn, 200);
	UT_ASSERT_EQ(restored.identity.page_lsn, 80);
	UT_ASSERT_EQ(restored.identity.logical_page, 64);
	UT_ASSERT_EQ(restored.identity.page_generation, 9);
	UT_ASSERT(FullTransactionIdEquals(restored.identity.birth_full_xid,
		image_page.identity.birth_full_xid));
	UT_ASSERT(memcmp(restored_payload, image_payload, 8192) == 0);
}

UT_TEST(late_image_keeps_prior_terminal_and_subcommitted_effects)
{
	/* Raw xid positions span byte, group-LSN, page and end-of-page edges. */
	const unsigned positions[] = {0, 1, 3, 4, 31, 32, 4095, 16384, 32767};
	reset(64);
	for (unsigned i = 0; i < lengthof(positions); i++)
	{
		unsigned xid = positions[i];
		unsigned status = i % 3 + 1;
		current_payload[xid / 4] |= status << ((xid % 4) * 2);
	}
	/* The image has only SUB_COMMITTED where replay has COMMITTED. */
	image_payload[0] = TRANSACTION_STATUS_SUB_COMMITTED;
	current_page.identity.page_lsn = 150;
	seal();
	UT_ASSERT(restore(true, 200));
	decode();
	UT_ASSERT(memcmp(restored_payload, current_payload, 8192) == 0);
	UT_ASSERT_EQ(restored.identity.page_lsn, 150);
	UT_ASSERT_EQ(restored.image_lsn, 200);
}

UT_TEST(all_canonical_status_pairs_preserve_outcomes_or_refuse_conflict)
{
	/* Expected semantic join, indexed by original CLOG status constants.
	 * -1 denotes two different terminal outcomes, never a guessed winner. */
	static const int joined[4][4] = {
		{0, 1, 2, 3}, {1, 1, -1, 1},
		{2, -1, 2, 2}, {3, 1, 2, 3}
	};
	for (unsigned a = 0; a < 4; a++)
		for (unsigned b = 0; b < 4; b++)
			for (unsigned shift = 0; shift < 8; shift += 2)
			{
				bool ok;
				reset(64);
				image_payload[8191] = a << shift;
				current_payload[8191] = b << shift;
				seal();
				ok = restore(true, 200);
				UT_ASSERT_EQ(ok, joined[a][b] >= 0);
				if (ok)
				{
					decode();
					UT_ASSERT_EQ(restored_payload[8191], joined[a][b] << shift);
				}
				else
					unchanged();
			}
}

UT_TEST(last_entry_conflict_publishes_no_partial_prefix)
{
	reset(64);
	memset(image_payload, 0x55, sizeof(image_payload));
	current_payload[8191] = TRANSACTION_STATUS_ABORTED << 6;
	seal();
	UT_ASSERT(!restore(true, 200));
	unchanged();
}

UT_TEST(repeated_and_out_of_order_images_keep_full_same_birth_state)
{
	reset(64);
	image_payload[1] = TRANSACTION_STATUS_COMMITTED;
	current_payload[2] = TRANSACTION_STATUS_ABORTED;
	current_page.identity.page_lsn = 300;
	current_page.image_lsn = 400;
	seal();
	UT_ASSERT(restore(true, 200));
	decode();
	UT_ASSERT_EQ(restored.image_lsn, 400);
	UT_ASSERT_EQ(restored.identity.page_lsn, 300);
	UT_ASSERT_EQ(restored_payload[1], TRANSACTION_STATUS_COMMITTED);
	UT_ASSERT_EQ(restored_payload[2], TRANSACTION_STATUS_ABORTED);
	memcpy(current, output, sizeof(current));
	memcpy(saved, output, sizeof(saved));
	UT_ASSERT(restore(true, 200));
	UT_ASSERT(memcmp(saved, output, sizeof(output)) == 0);
}

UT_TEST(current_frame_requires_the_exact_birth_not_only_raw_page)
{
	for (unsigned which = 0; which < 3; which++)
	{
		reset(64);
		if (which == 0)
			current_page.identity.page_generation++;
		else if (which == 1)
			current_page.identity.birth_full_xid.value += UINT64CONST(1) << 32;
		else
		{
			current_page.identity.logical_page++;
			current_page.identity.birth_full_xid.value += 32768;
		}
		seal();
		UT_ASSERT(!restore(true, 200));
		unchanged();
	}
}

UT_TEST(expected_registration_is_not_reconstructed_from_wal)
{
	for (unsigned which = 0; which < 10; which++)
	{
		reset(64);
		switch (which)
		{
			case 0: binding.identity.system_identifier++; break;
			case 1: binding.identity.database_incarnation++; break;
			case 2: binding.identity.storage_uuid[0]++; break;
			case 3: binding.identity.redo_branch_uuid[0]++; break;
			case 4: binding.identity.redo_branch_generation++; break;
			case 5: binding.identity.redo_timeline++; break;
			case 6: binding.identity.origin++; break;
			case 7: binding.identity.creation_generation++; break;
			case 8: binding.identity.registration_generation++; break;
			case 9: binding.segment++; break;
		}
		UT_ASSERT(!restore(true, 200));
		unchanged();
	}
}

UT_TEST(malformed_record_and_current_frame_refuse_before_publication)
{
	const unsigned offsets[] = {0, 40, 127, 128, 144, 176, 192, 8383};
	for (unsigned i = 0; i < lengthof(offsets); i++)
	{
		reset(64);
		record[offsets[i]] ^= 1;
		UT_ASSERT(!restore(true, 200));
		unchanged();
	}
	for (unsigned i = 0; i < 3; i++)
	{
		reset(64);
		current[i == 0 ? 0 : i == 1 ? 48 : 8255] ^= 1;
		UT_ASSERT(!restore(true, 200));
		unchanged();
	}
}

UT_TEST(actual_record_end_must_follow_captured_source_and_prior_image)
{
	const XLogRecPtr ends[] = {0, 79, 80, 99, 100, UINT64_MAX};
	for (unsigned i = 0; i < lengthof(ends); i++)
	{
		reset(64);
		UT_ASSERT(!restore(true, ends[i]));
		unchanged();
	}
	reset(64);
	image_page.identity.page_lsn = 201;
	seal();
	UT_ASSERT(!restore(true, 200));
	unchanged();
}

UT_TEST(exact_shapes_and_nonoverlapping_output_are_required)
{
	uint8 saved_record[8384], saved_current[8256];
	ClusterClogFileBindingV2 saved_binding;
	reset(64);
	memcpy(saved_record, record, sizeof(record));
	memcpy(saved_current, current, sizeof(current));
	saved_binding = binding;
	UT_ASSERT(!cluster_clog_wal_page_restore_v2(NULL, record, 8384, 200,
		current, 8256, output, 8256));
	UT_ASSERT(!cluster_clog_wal_page_restore_v2(&binding, NULL, 8384, 200,
		current, 8256, output, 8256));
	UT_ASSERT(!cluster_clog_wal_page_restore_v2(&binding, record, 8383, 200,
		current, 8256, output, 8256));
	UT_ASSERT(!cluster_clog_wal_page_restore_v2(&binding, record, 8384, 200,
		current, 8255, output, 8256));
	UT_ASSERT(!cluster_clog_wal_page_restore_v2(&binding, record, 8384, 200,
		NULL, 8256, output, 8256));
	UT_ASSERT(!cluster_clog_wal_page_restore_v2(&binding, record, 8384, 200,
		current, 0, output, 8256));
	UT_ASSERT(!cluster_clog_wal_page_restore_v2(&binding, record, 8384, 200,
		current, 8256, NULL, 8256));
	UT_ASSERT(!cluster_clog_wal_page_restore_v2(&binding, record, 8384, 200,
		current, 8256, output, 8255));
	UT_ASSERT(!cluster_clog_wal_page_restore_v2(&binding, record, 8384, 200,
		current, 8256, current, 8256));
	UT_ASSERT(!cluster_clog_wal_page_restore_v2(&binding, record, 8384, 200,
		current, 8256, record + 128, 8256));
	UT_ASSERT(!cluster_clog_wal_page_restore_v2(&binding, record, 8384, 200,
		current, 8256, &binding, 8256));
	UT_ASSERT(memcmp(&binding, &saved_binding, sizeof(binding)) == 0);
	UT_ASSERT(memcmp(current, saved_current, sizeof(current)) == 0);
	UT_ASSERT(memcmp(record, saved_record, sizeof(record)) == 0);
	unchanged();
}

UT_TEST(first_last_pages_and_raw_wrap_keep_independent_full_birth)
{
	const uint64 pages[] = {0, 1, 31, 32, 131071};
	for (unsigned i = 0; i < lengthof(pages); i++)
	{
		reset(pages[i]);
		image_payload[0] = TRANSACTION_STATUS_COMMITTED;
		current_payload[8191] = TRANSACTION_STATUS_ABORTED;
		seal();
		UT_ASSERT(restore(true, 200));
		decode();
		UT_ASSERT_EQ(restored.identity.logical_page, pages[i]);
		UT_ASSERT(FullTransactionIdEquals(restored.identity.birth_full_xid,
			image_page.identity.birth_full_xid));
		UT_ASSERT_EQ(restored_payload[0], TRANSACTION_STATUS_COMMITTED);
		UT_ASSERT_EQ(restored_payload[8191], TRANSACTION_STATUS_ABORTED);
	}
}

int
main(void)
{
	UT_PLAN(11);
	UT_RUN(selected_image_uses_actual_end_and_distinct_source_watermark);
	UT_RUN(late_image_keeps_prior_terminal_and_subcommitted_effects);
	UT_RUN(all_canonical_status_pairs_preserve_outcomes_or_refuse_conflict);
	UT_RUN(last_entry_conflict_publishes_no_partial_prefix);
	UT_RUN(repeated_and_out_of_order_images_keep_full_same_birth_state);
	UT_RUN(current_frame_requires_the_exact_birth_not_only_raw_page);
	UT_RUN(expected_registration_is_not_reconstructed_from_wal);
	UT_RUN(malformed_record_and_current_frame_refuse_before_publication);
	UT_RUN(actual_record_end_must_follow_captured_source_and_prior_image);
	UT_RUN(exact_shapes_and_nonoverlapping_output_are_required);
	UT_RUN(first_last_pages_and_raw_wrap_keep_independent_full_birth);
	UT_DONE();
	return ut_failed_count != 0;
}
