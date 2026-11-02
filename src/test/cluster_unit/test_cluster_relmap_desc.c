/*-------------------------------------------------------------------------
 * test_cluster_relmap_desc.c
 *    Strict managed WAL diagnostics without file or replay authority.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_desc.c
 * NOTES
 *    Spec: 2026-11-01-pre2-e1-format-closure-v1.2-path-c.md
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"
#include "access/rmgr.h"
#include "common/cluster_relmap_file.h"
#include "utils/relmapper.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static unsigned char bytes[CLUSTER_RELMAP_FILE_SIZE_V2];
static DecodedXLogRecord decoded;
static XLogReaderState reader;
static char desc_text[1024];
static StringInfoData description;

static void
prepare(bool pending)
{
	ClusterRecoveryFileIdentityV2 id = {0};
	ClusterRelmapV2Image image = {0};
	id.kind = CLUSTER_RECOVERY_FILE_RELMAP_V2;
	id.format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
	id.format_version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	id.system_identifier = 91; id.database_incarnation = 27;
	id.storage_uuid[0] = 1; id.redo_branch_uuid[0] = 2;
	id.redo_branch_generation = 3; id.redo_timeline = 4;
	id.tablespace_oid = 1664; id.creation_generation = 103; id.registration_generation = 19;
	image.identity = (ClusterRelmapV2Namespace) {91, 27, InvalidOid, true};
	image.committed_generation = 7;
	image.committed.magic = RELMAPPER_FILEMAGIC;
	image.committed.num_mappings = 1;
	image.committed.mappings[0] = (RelMapping) {1259, 9001};
	INIT_CRC32C(image.committed.crc);
	COMP_CRC32C(image.committed.crc, &image.committed, offsetof(RelMapFile, crc));
	FIN_CRC32C(image.committed.crc);
	if (pending) {
		image.pending_generation = 8;
		image.pending_owner = (ClusterRelmapV2Owner) {3, UINT64CONST(0x200000123), 0, 101, 909, 202};
		image.pending = image.committed;
	}
	UT_ASSERT(cluster_relmap_file_encode_v2(&id, &image, bytes, sizeof(bytes)));
	memset(&decoded, 0, sizeof(decoded)); memset(&reader, 0, sizeof(reader));
	decoded.header.xl_rmid = RM_RELMAP_ID;
	decoded.header.xl_info = CLUSTER_RECOVERY_RELMAP_UPDATE_WAL_V2;
	decoded.main_data = (char *)bytes; decoded.main_data_len = sizeof(bytes);
	decoded.max_block_id = -1; reader.record = &decoded;
	memset(desc_text, 0, sizeof(desc_text)); description = (StringInfoData) {desc_text, 0, sizeof(desc_text), 0};
}

static void
prepare_wal_intent(void)
{
	ClusterRecoveryFileIdentityV2 identity;
	ClusterRelmapV2Image image;
	prepare(true);
	UT_ASSERT(cluster_relmap_file_observe_v2(bytes, sizeof(bytes), &identity, &image));
	image.pending_owner.wal_end = 0;
	UT_ASSERT(cluster_relmap_wal_encode_intent_v2(&identity, &image, bytes, sizeof(bytes)));
}

UT_TEST(managed_intent_descriptor_reports_complete_identity)
{
	prepare_wal_intent(); relmap_desc(&description, &reader);
	UT_ASSERT(strcmp(desc_text, "WAL_INTENT database 0 tablespace 1664 origin 0 creation 103 registration 19 committed 7 pending 8") == 0);
	UT_ASSERT(strcmp(relmap_identify(0x10 | XLR_INFO_MASK), "UPDATE_V2") == 0);
}

UT_TEST(legacy_update_descriptor_and_identity_remain_unchanged)
{
	char legacy_bytes[MinSizeOfRelmapUpdate + sizeof(RelMapFile)] = {0};
	xl_relmap_update legacy = {0};
	prepare(false); legacy.dbid = 42; legacy.tsid = 1663; legacy.nbytes = 524;
	decoded.header.xl_info = XLOG_RELMAP_UPDATE;
	memcpy(legacy_bytes, &legacy, MinSizeOfRelmapUpdate);
	decoded.main_data = legacy_bytes; decoded.main_data_len = sizeof(legacy_bytes);
	relmap_desc(&description, &reader);
	UT_ASSERT(strcmp(desc_text, "database 42 tablespace 1663 size 524") == 0);
	UT_ASSERT(strcmp(relmap_identify(0), "UPDATE") == 0);
}

UT_TEST(managed_clean_file_is_not_a_wal_intent)
{
	prepare(false); relmap_desc(&description, &reader);
	UT_ASSERT(relmap_identify(0x10) != NULL && strcmp(relmap_identify(0x10), "UPDATE_V2") == 0);
	UT_ASSERT(relmap_identify(0x10 | XLR_INFO_MASK) != NULL
		&& strcmp(relmap_identify(0x10 | XLR_INFO_MASK), "UPDATE_V2") == 0);
	UT_ASSERT(strcmp(desc_text, "invalid managed relmap record") == 0);
}

UT_TEST(managed_pending_file_is_not_a_wal_intent)
{
	prepare(true); relmap_desc(&description, &reader);
	UT_ASSERT(strcmp(desc_text, "invalid managed relmap record") == 0);
}

UT_TEST(managed_short_long_or_null_payload_is_invalid)
{
	const uint32 lengths[] = {0, 128, 2176, 2303, 2305, UINT32_MAX};
	for (unsigned i = 0; i < lengthof(lengths); ++i) {
		prepare_wal_intent(); decoded.main_data_len = lengths[i]; relmap_desc(&description, &reader);
		UT_ASSERT(strcmp(desc_text, "invalid managed relmap record") == 0);
	}
	prepare_wal_intent(); decoded.main_data = NULL; relmap_desc(&description, &reader);
	UT_ASSERT(strcmp(desc_text, "invalid managed relmap record") == 0);
}

UT_TEST(managed_header_and_image_damage_never_print_fields)
{
	const unsigned offsets[] = {0, 12, 96, 104, 116, 120, 124, 200, 2303};
	for (unsigned i = 0; i < lengthof(offsets); ++i) {
		prepare_wal_intent(); bytes[offsets[i]] ^= 1; relmap_desc(&description, &reader);
		UT_ASSERT(strcmp(desc_text, "invalid managed relmap record") == 0);
	}
}

UT_TEST(managed_block_or_page_edge_records_are_invalid)
{
	prepare_wal_intent(); decoded.max_block_id = 0; relmap_desc(&description, &reader);
	UT_ASSERT(strcmp(desc_text, "invalid managed relmap record") == 0);
	prepare_wal_intent(); decoded.has_page_version_edge = true; relmap_desc(&description, &reader);
	UT_ASSERT(strcmp(desc_text, "invalid managed relmap record") == 0);
}

UT_TEST(unknown_opcode_stays_unknown_without_partial_description)
{
	prepare_wal_intent(); decoded.header.xl_info = 0x30; relmap_desc(&description, &reader);
	UT_ASSERT_EQ(description.len, 0); UT_ASSERT(relmap_identify(0x30) == NULL);
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(legacy_update_descriptor_and_identity_remain_unchanged);
	UT_RUN(managed_intent_descriptor_reports_complete_identity);
	UT_RUN(managed_clean_file_is_not_a_wal_intent);
	UT_RUN(managed_pending_file_is_not_a_wal_intent);
	UT_RUN(managed_short_long_or_null_payload_is_invalid);
	UT_RUN(managed_header_and_image_damage_never_print_fields);
	UT_RUN(managed_block_or_page_edge_records_are_invalid);
	UT_RUN(unknown_opcode_stays_unknown_without_partial_description);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
