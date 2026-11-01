/* Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include <fcntl.h>
#include <unistd.h>
#include "access/clog_file.h"
#include "cluster/cluster_recovery_format.h"
#include "cluster/cluster_undo_segment.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s at %s:%d\n", condition, file, line);
	abort();
}

static ClusterClogFileBindingV2 binding;
static ClusterClogPageIdentityV2 page;
static ClusterRecoveryFileIdentityV2 tt_identity;
static uint8 header[8192], frame[8256], payload[8192], output[8192];
static PGAlignedBlock tt;

static void
reset(void)
{
	memset(&binding, 0, sizeof(binding));
	binding.identity.kind = CLUSTER_RECOVERY_FILE_CLOG_V2;
	binding.identity.format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
	binding.identity.format_version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	binding.identity.system_identifier = 123;
	binding.identity.database_incarnation = 456;
	binding.identity.redo_branch_generation = 2;
	binding.identity.redo_timeline = 1;
	binding.identity.creation_generation = 3;
	binding.identity.registration_generation = 4;
	binding.identity.object_id = 2;
	for (int i = 0; i < 16; i++) {
		binding.identity.storage_uuid[i] = (uint8)(i + 1);
		binding.identity.redo_branch_uuid[i] = (uint8)(i + 17);
	}
	tt_identity = binding.identity;
	tt_identity.kind = CLUSTER_RECOVERY_FILE_UNDO_V2;
	tt_identity.object_id = 1;
	binding.segment = 2;
	memset(&page, 0, sizeof(page));
	page.identity.logical_page = 64;
	page.identity.page_generation = 9;
	page.identity.birth_observed = true;
	page.identity.birth_full_xid = FullTransactionIdFromU64((UINT64CONST(7) << 32) | UINT64CONST(2097155));
	page.identity.page_lsn = 80;
	page.image_lsn = 100;
	memset(payload, 0x39, sizeof(payload));
	memset(header, 0x55, sizeof(header));
	memset(frame, 0x55, sizeof(frame));
	memset(output, 0x77, sizeof(output));
}

#ifdef USE_PGRAC_CLUSTER
static void
fresh_tt(void)
{
	UndoSegmentHeaderDataV2 *h;

	memset(&tt, 0, sizeof(tt));
	h = (UndoSegmentHeaderDataV2 *)tt.data;
	h->pd_flags = PD_UNDO_SEG_HEADER;
	h->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	h->segment_id = 1;
	h->segment_size_bytes = UNDO_SEGMENT_SIZE_BYTES;
	h->owner_instance = 1;
	h->tt_slots_count = 48;
	h->tt_layout_version = 2;
	h->tt_slot_bytes = 40;
}
#endif

UT_TEST(offsets_are_frame_based_and_bounded)
{
	uint64 offset = 999;
	UT_ASSERT(cluster_clog_file_offset_v2(0, &offset));
	UT_ASSERT_EQ(offset, 8192);
	UT_ASSERT(cluster_clog_file_offset_v2(1, &offset));
	UT_ASSERT_EQ(offset, 16448);
	UT_ASSERT(cluster_clog_file_offset_v2(31, &offset));
	UT_ASSERT_EQ(offset, 264128);
	UT_ASSERT(!cluster_clog_file_offset_v2(32, &offset));
	UT_ASSERT_EQ(offset, 264128);
	UT_ASSERT(!cluster_clog_file_offset_v2(UINT32_MAX, &offset));
	UT_ASSERT(!cluster_clog_file_offset_v2(0, NULL));
}

UT_TEST(header_roundtrip_exact_binding)
{
	ClusterClogFileBindingV2 other;
	reset();
	UT_ASSERT(cluster_clog_file_header_encode_v2(&binding, header, sizeof(header)));
	UT_ASSERT(cluster_clog_file_header_matches_v2(header, sizeof(header), &binding));
	other = binding;
	other.identity.registration_generation++;
	UT_ASSERT(!cluster_clog_file_header_matches_v2(header, sizeof(header), &other));
	other = binding;
	other.segment++;
	UT_ASSERT(!cluster_clog_file_header_matches_v2(header, sizeof(header), &other));
}

UT_TEST(header_old_raw_short_unknown_corrupt_refused)
{
	reset();
	memset(header, 0, sizeof(header));
	UT_ASSERT(!cluster_clog_file_header_matches_v2(header, sizeof(header), &binding));
	UT_ASSERT(cluster_clog_file_header_encode_v2(&binding, header, sizeof(header)));
	UT_ASSERT(!cluster_clog_file_header_matches_v2(header, 8191, &binding));
	UT_ASSERT(!cluster_clog_file_header_matches_v2(header, 8193, &binding));
	for (int i = 0; i < 8192; i++) {
		header[i] ^= 1;
		UT_ASSERT(!cluster_clog_file_header_matches_v2(header, sizeof(header), &binding));
		header[i] ^= 1;
	}
}

UT_TEST(frame_roundtrip_preserves_full_page_and_identity)
{
	ClusterClogPageIdentityV2 decoded = {0};
	reset();
	UT_ASSERT(cluster_clog_file_page_encode_v2(&binding, &page, payload, 8192, frame, 8256));
	UT_ASSERT(cluster_clog_file_page_decode_v2(&binding, 64, frame, 8256, &decoded, output, 8192));
	UT_ASSERT_EQ(decoded.identity.logical_page, 64);
	UT_ASSERT_EQ(decoded.identity.page_generation, 9);
	UT_ASSERT_EQ(U64FromFullTransactionId(decoded.identity.birth_full_xid), UINT64CONST(30066868227));
	UT_ASSERT_EQ(decoded.identity.page_lsn, 80);
	UT_ASSERT_EQ(decoded.image_lsn, 100);
	UT_ASSERT(memcmp(output, payload, 8192) == 0);
}

UT_TEST(frame_corruption_short_and_cross_file_keep_outputs)
{
	ClusterClogFileBindingV2 other;
	ClusterClogPageIdentityV2 decoded, saved;
	reset();
	memset(&decoded, 0x67, sizeof(decoded));
	saved = decoded;
	UT_ASSERT(cluster_clog_file_page_encode_v2(&binding, &page, payload, 8192, frame, 8256));
	other = binding;
	other.identity.system_identifier++;
	UT_ASSERT(!cluster_clog_file_page_decode_v2(&other, 64, frame, 8256, &decoded, output, 8192));
	UT_ASSERT(!cluster_clog_file_page_decode_v2(&binding, 65, frame, 8256, &decoded, output, 8192));
	UT_ASSERT(!cluster_clog_file_page_decode_v2(&binding, 64, frame, 8255, &decoded, output, 8192));
	UT_ASSERT(!cluster_clog_file_page_decode_v2(&binding, 64, frame, 8256, &decoded, output, 8191));
	for (int i = 0; i < 8256; i++) {
		frame[i] ^= 1;
		UT_ASSERT(!cluster_clog_file_page_decode_v2(&binding, 64, frame, 8256, &decoded, output, 8192));
		frame[i] ^= 1;
	}
	UT_ASSERT(memcmp(&decoded, &saved, sizeof(decoded)) == 0);
	for (int i = 0; i < 8192; i++)
		UT_ASSERT_EQ(output[i], 0x77);
}

UT_TEST(invalid_identity_and_birth_refuse_before_writing)
{
	uint8 saved[8256];
	reset();
	memcpy(saved, frame, sizeof(saved));
	page.identity.birth_full_xid = FullTransactionIdFromU64((UINT64CONST(8) << 32) | 100);
	UT_ASSERT(!cluster_clog_file_page_encode_v2(&binding, &page, payload, 8192, frame, 8256));
	UT_ASSERT(memcmp(saved, frame, sizeof(saved)) == 0);
	reset();
	page.identity.page_generation = 0;
	UT_ASSERT(!cluster_clog_file_page_encode_v2(&binding, &page, payload, 8192, frame, 8256));
	page.identity.page_generation = UINT64_MAX;
	UT_ASSERT(!cluster_clog_file_page_encode_v2(&binding, &page, payload, 8192, frame, 8256));
	reset();
	memset(&binding.identity, 0, sizeof(binding.identity));
	UT_ASSERT(!cluster_clog_file_header_encode_v2(&binding, header, 8192));
	UT_ASSERT(!cluster_clog_file_page_encode_v2(&binding, &page, payload, 8192, frame, 8256));
}

UT_TEST(actual_file_roundtrip_uses_framed_offset)
{
	char name[] = "/tmp/pgrac-clog-frame-XXXXXX";
	uint64 offset = 0;
	int fd;
	ssize_t wrote_header, wrote_frame, got;
	ClusterClogPageIdentityV2 decoded = {0};

	reset();
	UT_ASSERT(cluster_clog_file_header_encode_v2(&binding, header, 8192));
	UT_ASSERT(cluster_clog_file_page_encode_v2(&binding, &page, payload, 8192, frame, 8256));
	UT_ASSERT(cluster_clog_file_offset_v2(0, &offset));
	fd = mkstemp(name);
	UT_ASSERT(fd >= 0);
	unlink(name);
	wrote_header = pwrite(fd, header, 8192, 0);
	wrote_frame = pwrite(fd, frame, 8256, (off_t)offset);
	got = pread(fd, frame, 8256, 8192);
	close(fd);
	UT_ASSERT_EQ(wrote_header, 8192);
	UT_ASSERT_EQ(wrote_frame, 8256);
	UT_ASSERT_EQ(got, 8256);
	UT_ASSERT(cluster_clog_file_page_decode_v2(&binding, 64, frame, 8256, &decoded, output, 8192));
	UT_ASSERT(memcmp(payload, output, 8192) == 0);
}

#ifdef USE_PGRAC_CLUSTER
UT_TEST(tt40_reference_only_changes_reserved_span)
{
	PGAlignedBlock before;
	reset();
	fresh_tt();
	memcpy(&before, &tt, sizeof(tt));
	UT_ASSERT(cluster_tt_file_reference_prepare_v2(tt.data, 8192, &tt_identity));
	UT_ASSERT(memcmp(tt.data, before.data, 3072) == 0);
	UT_ASSERT(memcmp(tt.data + 3200, before.data + 3200, 4992) == 0);
	UT_ASSERT(memcmp(tt.data + 3072, "RFI2", 4) == 0);
	UT_ASSERT(cluster_tt_file_reference_matches_v2(tt.data, 8192, &tt_identity));
	UT_ASSERT(cluster_tt_file_reference_prepare_v2(tt.data, 8192, &tt_identity));
}

UT_TEST(tt32_conflicts_short_and_nonzero_alignment_refused)
{
	PGAlignedBlock before;
	reset();
	fresh_tt();
	UT_ASSERT(cluster_tt_file_reference_prepare_v2(tt.data, 8192, &tt_identity));
	memcpy(&before, &tt, sizeof(tt));
	tt_identity.system_identifier++;
	UT_ASSERT(!cluster_tt_file_reference_prepare_v2(tt.data, 8192, &tt_identity));
	UT_ASSERT(memcmp(&before, &tt, sizeof(tt)) == 0);
	UT_ASSERT(!cluster_tt_file_reference_matches_v2(tt.data, 8191, &tt_identity));
	fresh_tt();
	tt.data[57] = 0;
	UT_ASSERT(!cluster_tt_file_reference_prepare_v2(tt.data, 8192, &tt_identity));
	fresh_tt();
	tt.data[3068] = 1;
	UT_ASSERT(!cluster_tt_file_reference_prepare_v2(tt.data, 8192, &tt_identity));
}
#endif

UT_TEST(first_last_page_and_epoch_zero_are_exact)
{
	ClusterClogPageIdentityV2 decoded = {0};
	reset();
	binding.segment = 0;
	page.identity.logical_page = 0;
	binding.identity.object_id = 0;
	page.identity.birth_full_xid = FullTransactionIdFromU64(3);
	page.identity.page_lsn = 0;
	page.image_lsn = 0;
	UT_ASSERT(cluster_clog_file_page_encode_v2(&binding, &page, payload, 8192, frame, 8256));
	UT_ASSERT(cluster_clog_file_page_decode_v2(&binding, 0, frame, 8256, &decoded, output, 8192));
	UT_ASSERT_EQ(U64FromFullTransactionId(decoded.identity.birth_full_xid), 3);
	binding.segment = 4095;
	page.identity.logical_page = 131071;
	binding.identity.object_id = 4095;
	page.identity.birth_full_xid = FullTransactionIdFromU64((UINT64CONST(7) << 32) | UINT64CONST(4294934531));
	UT_ASSERT(cluster_clog_file_page_encode_v2(&binding, &page, payload, 8192, frame, 8256));
	UT_ASSERT(cluster_clog_file_page_decode_v2(&binding, 131071, frame, 8256, &decoded, output, 8192));
	binding.segment = 4096;
	UT_ASSERT(!cluster_clog_file_header_encode_v2(&binding, header, 8192));
	UT_ASSERT(!cluster_clog_file_page_encode_v2(&binding, &page, payload, 8192, frame, 8256));
}

UT_TEST(failure_and_overlapping_outputs_do_not_publish)
{
	ClusterClogPageIdentityV2 decoded = {0};
	reset();
	UT_ASSERT(!cluster_clog_file_header_encode_v2(NULL, header, 8192));
	UT_ASSERT(!cluster_clog_file_header_encode_v2(&binding, header, 8191));
	UT_ASSERT(!cluster_clog_file_page_encode_v2(&binding, &page, payload, 8191, frame, 8256));
	UT_ASSERT(!cluster_clog_file_page_encode_v2(&binding, &page, payload, 8192, frame, 8255));
	for (int i = 0; i < 8192; i++)
		UT_ASSERT_EQ(header[i], 0x55);
	for (int i = 0; i < 8256; i++)
		UT_ASSERT_EQ(frame[i], 0x55);
	UT_ASSERT(cluster_clog_file_page_encode_v2(&binding, &page, payload, 8192, frame, 8256));
	UT_ASSERT(!cluster_clog_file_page_decode_v2(&binding, 64, frame, 8256,
		(ClusterClogPageIdentityV2 *)output, output, 8192));
	UT_ASSERT(!cluster_clog_file_page_decode_v2(&binding, 64, NULL, 8256, &decoded, output, 8192));
	UT_ASSERT(!cluster_clog_file_page_decode_v2(&binding, 64, frame, 8256, NULL, output, 8192));
	for (int i = 0; i < 8192; i++)
		UT_ASSERT_EQ(output[i], 0x77);
}

UT_TEST(common_file_identity_fields_are_explicit_and_exact)
{
	ClusterClogFileBindingV2 other;
	ClusterRecoveryFileIdentityV2 *id;
	uint8 saved[8192];
	reset();
	UT_ASSERT(cluster_clog_file_header_encode_v2(&binding, header, sizeof(header)));
	memcpy(saved, header, sizeof(saved));
	for (int field = 0; field < 16; field++) {
		other = binding;
		id = &other.identity;
		switch (field) {
			case 0: id->kind = CLUSTER_RECOVERY_FILE_UNDO_V2; break;
			case 1: id->format_id++; break;
			case 2: id->format_version++; break;
			case 3: id->system_identifier++; break;
			case 4: id->database_incarnation++; break;
			case 5: id->storage_uuid[15]++; break;
			case 6: id->redo_branch_uuid[15]++; break;
			case 7: id->redo_branch_generation++; break;
			case 8: id->redo_timeline++; break;
			case 9: id->database_oid++; break;
			case 10: id->tablespace_oid++; break;
			case 11: id->origin++; break;
			case 12: id->object_id += UINT64CONST(1) << 32; break;
			case 13: id->creation_generation++; break;
			case 14: id->registration_generation++; break;
			case 15: other.segment++; break;
		}
		UT_ASSERT(!cluster_clog_file_header_matches_v2(header, sizeof(header), &other));
	}
	other = binding;
	other.identity.format_id = 0;
	UT_ASSERT(!cluster_clog_file_header_encode_v2(&other, header, sizeof(header)));
	UT_ASSERT(memcmp(header, saved, sizeof(saved)) == 0);
	other = binding;
	memset(other.identity.redo_branch_uuid, 0, 16);
	UT_ASSERT(!cluster_clog_file_header_encode_v2(&other, header, sizeof(header)));
	UT_ASSERT(memcmp(header, saved, sizeof(saved)) == 0);
	other = binding;
	other.identity.origin = 128;
	UT_ASSERT(!cluster_clog_file_header_encode_v2(&other, header, sizeof(header)));
	UT_ASSERT(memcmp(header, saved, sizeof(saved)) == 0);
}

UT_TEST(common_page_birth_requires_actual_observation)
{
	ClusterClogPageIdentityV2 decoded = {0};
	uint8 saved[8256];
	reset();
	memcpy(saved, frame, sizeof(saved));
	page.identity.birth_observed = false;
	UT_ASSERT(!cluster_clog_file_page_encode_v2(&binding, &page, payload, 8192, frame, 8256));
	UT_ASSERT(memcmp(frame, saved, sizeof(saved)) == 0);
	page.identity.birth_observed = true;
	UT_ASSERT(cluster_clog_file_page_encode_v2(&binding, &page, payload, 8192, frame, 8256));
	UT_ASSERT(cluster_clog_file_page_decode_v2(&binding, 64, frame, 8256, &decoded, output, 8192));
	UT_ASSERT(decoded.identity.birth_observed);
	UT_ASSERT_EQ(decoded.identity.metadata_crc,
		((uint32)frame[48] << 24) | ((uint32)frame[49] << 16) | ((uint32)frame[50] << 8) | frame[51]);
	UT_ASSERT_EQ(decoded.identity.payload_crc,
		((uint32)frame[52] << 24) | ((uint32)frame[53] << 16) | ((uint32)frame[54] << 8) | frame[55]);
}

#ifdef USE_PGRAC_CLUSTER
UT_TEST(tt_full_framing_rejects_invalid_identity_and_preserves_slots)
{
	PGAlignedBlock before;
	UndoSegmentHeaderDataV2 *h;
	reset();
	fresh_tt();
	h = (UndoSegmentHeaderDataV2 *)tt.data;
	h->tt_slots[47].original_full_xid = UINT64CONST(30066868227);
	memcpy(&before, &tt, sizeof(tt));
	UT_ASSERT(cluster_tt_file_reference_prepare_v2(tt.data, 8192, &tt_identity));
	UT_ASSERT(memcmp(tt.data, before.data, 3072) == 0);
	for (int i = 0; i < 128; i++) {
		tt.data[3072 + i] ^= 1;
		UT_ASSERT(!cluster_tt_file_reference_matches_v2(tt.data, 8192, &tt_identity));
		tt.data[3072 + i] ^= 1;
	}
	h->segment_id = 257;
	UT_ASSERT(!cluster_tt_file_reference_matches_v2(tt.data, 8192, &tt_identity));
	fresh_tt();
	UT_ASSERT(!cluster_tt_file_reference_prepare_v2(tt.data, 8192, &binding.identity));
	tt_identity.origin = 1;
	UT_ASSERT(!cluster_tt_file_reference_prepare_v2(tt.data, 8192, &tt_identity));
	h->segment_id = 1;
	h->tt_active_count = 49;
	UT_ASSERT(!cluster_tt_file_reference_matches_v2(tt.data, 8192, &tt_identity));
	h->tt_active_count = 0;
	tt.data[3200] = 1;
	UT_ASSERT(!cluster_tt_file_reference_matches_v2(tt.data, 8192, &tt_identity));
}
#else
UT_TEST(disabled_build_refuses_cluster_tt_pages)
{
	reset();
	memset(&tt, 0, sizeof(tt));
	UT_ASSERT(!cluster_tt_file_reference_prepare_v2(tt.data, 8192, &tt_identity));
	UT_ASSERT(!cluster_tt_file_reference_matches_v2(tt.data, 8192, &tt_identity));
}
#endif

UT_TEST(clog_consumers_require_assigned_format_before_output)
{
	uint8 saved_header[8192], saved_frame[8256];
	ClusterClogPageIdentityV2 decoded, saved_decoded;
	reset();
	UT_ASSERT(cluster_clog_file_header_encode_v2(&binding, header, sizeof(header)));
	UT_ASSERT(cluster_clog_file_page_encode_v2(&binding, &page, payload, 8192, frame, 8256));
	memcpy(saved_header, header, sizeof(header));
	memcpy(saved_frame, frame, sizeof(frame));
	memset(&decoded, 0x67, sizeof(decoded));
	saved_decoded = decoded;
	for (int variant = 0; variant < 4; variant++) {
		binding.identity.format_id = variant < 2 ?
			(variant == 0 ? 700 : CLUSTER_RECOVERY_FORMAT_ID_V2 + 1) :
			CLUSTER_RECOVERY_FORMAT_ID_V2;
		binding.identity.format_version = variant < 2 ? 2 : (variant == 2 ? 1 : 3);
		UT_ASSERT(!cluster_clog_file_header_encode_v2(&binding, header, sizeof(header)));
		UT_ASSERT(!cluster_clog_file_page_encode_v2(&binding, &page, payload, 8192, frame, 8256));
		UT_ASSERT(!cluster_clog_file_header_matches_v2(saved_header, sizeof(saved_header), &binding));
		UT_ASSERT(!cluster_clog_file_page_decode_v2(&binding, 64, saved_frame, 8256, &decoded, output, 8192));
		UT_ASSERT(memcmp(header, saved_header, sizeof(header)) == 0);
		UT_ASSERT(memcmp(frame, saved_frame, sizeof(frame)) == 0);
		UT_ASSERT(memcmp(&decoded, &saved_decoded, sizeof(decoded)) == 0);
		for (int i = 0; i < 8192; i++)
			UT_ASSERT_EQ(output[i], 0x77);
	}
}

UT_TEST(common_identity_bytes_do_not_replace_family_guards)
{
	uint8 reference[128], saved_header[8192];
	ClusterRecoveryFileIdentityV2 decoded;
	reset();
	UT_ASSERT(cluster_recovery_file_identity_encode_v2(&binding.identity, reference, sizeof(reference)));
	UT_ASSERT(cluster_clog_file_header_encode_v2(&binding, header, sizeof(header)));
	UT_ASSERT(memcmp(header + 32, reference, sizeof(reference)) == 0);
	UT_ASSERT(cluster_recovery_file_identity_decode_v2(header + 32, sizeof(reference), &decoded));
	UT_ASSERT(cluster_recovery_file_identity_matches_v2(reference, sizeof(reference), &decoded));
	memcpy(saved_header, header, sizeof(header));
	for (int kind = CLUSTER_RECOVERY_FILE_UNDO_V2; kind <= CLUSTER_RECOVERY_FILE_DEBT_V2; kind++) {
		binding.identity.kind = (ClusterRecoveryFileKindV2)kind;
		UT_ASSERT(cluster_recovery_file_identity_encode_v2(&binding.identity, reference, sizeof(reference)));
		UT_ASSERT(!cluster_clog_file_header_encode_v2(&binding, header, sizeof(header)));
		UT_ASSERT(!cluster_clog_file_page_encode_v2(&binding, &page, payload, 8192, frame, 8256));
		UT_ASSERT(memcmp(header, saved_header, sizeof(header)) == 0);
	}
#ifdef USE_PGRAC_CLUSTER
	fresh_tt();
	UT_ASSERT(cluster_recovery_file_identity_encode_v2(&tt_identity, reference, sizeof(reference)));
	UT_ASSERT(cluster_tt_file_reference_prepare_v2(tt.data, 8192, &tt_identity));
	UT_ASSERT(memcmp(tt.data + 3072, reference, sizeof(reference)) == 0);
	UT_ASSERT(cluster_recovery_file_identity_matches_v2((uint8 *)tt.data + 3072, 128, &tt_identity));
	/* A common-valid identity is still refused by the original TT owner guard. */
	tt_identity.object_id++;
	UT_ASSERT(cluster_recovery_file_identity_encode_v2(&tt_identity, reference, sizeof(reference)));
	UT_ASSERT(!cluster_tt_file_reference_prepare_v2(tt.data, 8192, &tt_identity));
#endif
}

#ifdef USE_PGRAC_CLUSTER
UT_TEST(tt_consumers_require_assigned_format_before_output)
{
	PGAlignedBlock before;
	reset();
	for (int variant = 0; variant < 4; variant++) {
		fresh_tt();
		memcpy(&before, &tt, sizeof(tt));
		tt_identity.format_id = variant < 2 ?
			(variant == 0 ? 700 : CLUSTER_RECOVERY_FORMAT_ID_V2 + 1) :
			CLUSTER_RECOVERY_FORMAT_ID_V2;
		tt_identity.format_version = variant < 2 ? 2 : (variant == 2 ? 1 : 3);
		UT_ASSERT(!cluster_tt_file_reference_prepare_v2(tt.data, 8192, &tt_identity));
		UT_ASSERT(!cluster_tt_file_reference_matches_v2(tt.data, 8192, &tt_identity));
		UT_ASSERT(memcmp(&tt, &before, sizeof(tt)) == 0);
	}
}
#endif

int
main(void)
{
#ifdef USE_PGRAC_CLUSTER
	UT_PLAN(17);
#else
	UT_PLAN(14);
#endif
	UT_RUN(offsets_are_frame_based_and_bounded);
	UT_RUN(header_roundtrip_exact_binding);
	UT_RUN(header_old_raw_short_unknown_corrupt_refused);
	UT_RUN(frame_roundtrip_preserves_full_page_and_identity);
	UT_RUN(frame_corruption_short_and_cross_file_keep_outputs);
	UT_RUN(invalid_identity_and_birth_refuse_before_writing);
	UT_RUN(actual_file_roundtrip_uses_framed_offset);
#ifdef USE_PGRAC_CLUSTER
	UT_RUN(tt40_reference_only_changes_reserved_span);
	UT_RUN(tt32_conflicts_short_and_nonzero_alignment_refused);
#endif
	UT_RUN(first_last_page_and_epoch_zero_are_exact);
	UT_RUN(failure_and_overlapping_outputs_do_not_publish);
	UT_RUN(common_file_identity_fields_are_explicit_and_exact);
	UT_RUN(common_page_birth_requires_actual_observation);
#ifdef USE_PGRAC_CLUSTER
	UT_RUN(tt_full_framing_rejects_invalid_identity_and_preserves_slots);
#else
	UT_RUN(disabled_build_refuses_cluster_tt_pages);
#endif
	UT_RUN(clog_consumers_require_assigned_format_before_output);
	UT_RUN(common_identity_bytes_do_not_replace_family_guards);
#ifdef USE_PGRAC_CLUSTER
	UT_RUN(tt_consumers_require_assigned_format_before_output);
#endif
	UT_DONE();
	return ut_failed_count != 0;
}
