/*-------------------------------------------------------------------------
 * test_cluster_relmap_wal_desc.c
 *    Real relmap WAL descriptions and malformed-record boundaries.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_wal_desc.c
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"
#include "common/cluster_relmap_create.h"
#include "utils/relmapper.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static XLogReaderState reader;
static DecodedXLogRecord decoded;
static StringInfoData description;
static unsigned char bytes[CLUSTER_RELMAP_CREATE_WAL_SIZE_V1 + 1];
static ClusterRecoveryFileIdentityV2 identity;
static RelMapFile map;

static void
prepare(uint8 info, char *data, uint32 length)
{
	memset(&reader, 0, sizeof(reader));
	memset(&decoded, 0, sizeof(decoded));
	decoded.header.xl_info = info;
	decoded.main_data = data;
	decoded.main_data_len = length;
	decoded.max_block_id = -1;
	reader.record = &decoded;
	resetStringInfo(&description);
}

static void
creation(void)
{
	uint8 batch[16] = {0x17, 0x2a, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x99};

	memset(&identity, 0, sizeof(identity));
	identity.kind = CLUSTER_RECOVERY_FILE_RELMAP_V2;
	identity.format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
	identity.format_version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	identity.system_identifier = 91;
	identity.database_incarnation = 27;
	identity.storage_uuid[0] = 1;
	identity.redo_branch_uuid[0] = 2;
	identity.redo_branch_generation = 3;
	identity.redo_timeline = 4;
	identity.database_oid = 2000;
	identity.tablespace_oid = 1663;
	identity.origin = 2;
	identity.creation_generation = 103;
	identity.registration_generation = 211;
	memset(&map, 0, sizeof(map));
	map.magic = RELMAPPER_FILEMAGIC;
	map.num_mappings = 1;
	map.mappings[0] = (RelMapping){1259, 9001};
	INIT_CRC32C(map.crc);
	COMP_CRC32C(map.crc, &map, offsetof(RelMapFile, crc));
	FIN_CRC32C(map.crc);
	UT_ASSERT(cluster_relmap_create_encode_v1(&identity, batch, &map, bytes,
		CLUSTER_RELMAP_CREATE_WAL_SIZE_V1));
	prepare(XLOG_RELMAP_CREATE_V1, (char *)bytes, CLUSTER_RELMAP_CREATE_WAL_SIZE_V1);
}

static void
put_crc(unsigned char *out, const unsigned char *data, Size length)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, data, length);
	FIN_CRC32C(crc);
	for (unsigned i = 0; i < 4; ++i)
		out[i] = (uint8)(crc >> (24 - i * 8));
}

UT_TEST(identify_assigned_opcodes_with_generic_flags)
{
	UT_ASSERT_STR_EQ(relmap_identify(0), "UPDATE");
	UT_ASSERT_NOT_NULL(relmap_identify(0x20));
	UT_ASSERT_STR_EQ(relmap_identify(0x20 | XLR_INFO_MASK), "CREATE_V1");
	UT_ASSERT_NOT_NULL(relmap_identify(0x10));
	UT_ASSERT_STR_EQ(relmap_identify(0x10 | XLR_INFO_MASK), "UPDATE_V2");
}

UT_TEST(creation_description_preserves_target_batch_and_generation)
{
	creation();
	/* MAIN bytes need not have native struct alignment. */
	memmove(bytes + 1, bytes, CLUSTER_RELMAP_CREATE_WAL_SIZE_V1);
	decoded.main_data = (char *)bytes + 1;
	decoded.header.xl_info |= XLR_INFO_MASK;
	relmap_desc(&description, &reader);
	UT_ASSERT_STR_EQ(description.data, "database 2000 tablespace 1663 origin 2 creation 103 registration 211 batch 172a0000000000000000000000000099 generation 1 mappings 1");
}

UT_TEST(creation_description_checks_every_original_byte)
{
	creation();
	for (Size i = 0; i < CLUSTER_RELMAP_CREATE_WAL_SIZE_V1; ++i)
	{
		bytes[i] ^= 1;
		resetStringInfo(&description);
		relmap_desc(&description, &reader);
		UT_ASSERT_STR_EQ(description.data, "invalid creation record");
		bytes[i] ^= 1;
	}
}

UT_TEST(creation_description_refuses_truncation_null_and_page_envelopes)
{
	creation();
	for (uint32 i = 0; i <= CLUSTER_RELMAP_CREATE_WAL_SIZE_V1 + 1; ++i)
	{
		if (i == CLUSTER_RELMAP_CREATE_WAL_SIZE_V1)
			continue;
		decoded.main_data_len = i;
		resetStringInfo(&description);
		relmap_desc(&description, &reader);
		UT_ASSERT_STR_EQ(description.data, "invalid creation record");
	}
	decoded.main_data_len = CLUSTER_RELMAP_CREATE_WAL_SIZE_V1;
	decoded.main_data = NULL;
	resetStringInfo(&description);
	relmap_desc(&description, &reader);
	UT_ASSERT_STR_EQ(description.data, "invalid creation record");
	decoded.main_data = (char *)bytes;
	decoded.max_block_id = 0;
	resetStringInfo(&description);
	relmap_desc(&description, &reader);
	UT_ASSERT_STR_EQ(description.data, "invalid creation record");
	decoded.max_block_id = -1;
	decoded.has_page_version_edge = true;
	resetStringInfo(&description);
	relmap_desc(&description, &reader);
	UT_ASSERT_STR_EQ(description.data, "invalid creation record");
}

UT_TEST(creation_description_refuses_rechecksummed_wrong_state_or_birth)
{
	ClusterRelmapV2Image image = {0};

	for (unsigned wrong = 0; wrong < 4; ++wrong)
	{
		creation();
		if (wrong == 0)
			bytes[11] = 1;
		else if (wrong == 1)
			memset(bytes + 24, 0, 16);
		else if (wrong == 2)
			bytes[40] = 1;
		else
		{
			image.identity = (ClusterRelmapV2Namespace){91, 27, 2000, false};
			image.committed_generation = 2;
			image.committed = map;
			UT_ASSERT(cluster_relmap_file_encode_v2(&identity, &image, bytes + 64,
				CLUSTER_RELMAP_FILE_SIZE_V2));
			put_crc(bytes + 56, bytes + 64, CLUSTER_RELMAP_FILE_SIZE_V2);
		}
		put_crc(bytes + 60, bytes, 60);
		relmap_desc(&description, &reader);
		UT_ASSERT_STR_EQ(description.data, "invalid creation record");
	}
}

UT_TEST(legacy_update_retains_original_output_with_unaligned_main)
{
	xl_relmap_update update = {0};

	update.dbid = 2000;
	update.tsid = 1663;
	update.nbytes = sizeof(RelMapFile);
	memcpy(bytes + 1, &update, MinSizeOfRelmapUpdate);
	prepare(XLOG_RELMAP_UPDATE | XLR_INFO_MASK, (char *)bytes + 1,
		MinSizeOfRelmapUpdate + sizeof(RelMapFile));
	relmap_desc(&description, &reader);
	UT_ASSERT_STR_EQ(description.data, "database 2000 tablespace 1663 size 524");
}

UT_TEST(legacy_update_refuses_short_headers_and_inconsistent_payload_lengths)
{
	xl_relmap_update update = {0};

	update.nbytes = sizeof(RelMapFile);
	memcpy(bytes, &update, MinSizeOfRelmapUpdate);
	for (uint32 i = 0; i < MinSizeOfRelmapUpdate; ++i)
	{
		prepare(XLOG_RELMAP_UPDATE, (char *)bytes, i);
		relmap_desc(&description, &reader);
		UT_ASSERT_STR_EQ(description.data, "invalid update record");
	}
	for (unsigned wrong = 0; wrong < 4; ++wrong)
	{
		update.nbytes = wrong == 0 ? -1 : sizeof(RelMapFile);
		memcpy(bytes, &update, MinSizeOfRelmapUpdate);
		prepare(XLOG_RELMAP_UPDATE, wrong == 1 ? NULL : (char *)bytes,
			MinSizeOfRelmapUpdate + sizeof(RelMapFile) + (wrong == 2 ? 1 : 0));
		if (wrong == 3)
			--decoded.main_data_len;
		relmap_desc(&description, &reader);
		UT_ASSERT_STR_EQ(description.data, "invalid update record");
	}
}

UT_TEST(managed_update_preserves_original_strict_intent_decoder)
{
	creation();
	decoded.header.xl_info = 0x10;
	relmap_desc(&description, &reader);
	UT_ASSERT_STR_EQ(description.data, "invalid managed relmap record");
	prepare(0x10 | XLR_INFO_MASK, NULL, 0);
	relmap_desc(&description, &reader);
	UT_ASSERT_STR_EQ(description.data, "invalid managed relmap record");
}

UT_TEST(unknown_opcodes_keep_original_no_description_behavior)
{
	prepare(0x30, (char *)bytes, sizeof(bytes));
	relmap_desc(&description, &reader);
	UT_ASSERT(description.len == 0 && relmap_identify(0x30) == NULL);
}

int
main(int argc, char **argv)
{
	bool only_new = argc == 2 && strcmp(argv[1], "--creation") == 0;

	initStringInfo(&description);
	UT_PLAN(only_new ? 5 : 9);
	UT_RUN(identify_assigned_opcodes_with_generic_flags);
	UT_RUN(creation_description_preserves_target_batch_and_generation);
	UT_RUN(creation_description_checks_every_original_byte);
	UT_RUN(creation_description_refuses_truncation_null_and_page_envelopes);
	UT_RUN(creation_description_refuses_rechecksummed_wrong_state_or_birth);
	if (!only_new)
	{
		UT_RUN(legacy_update_retains_original_output_with_unaligned_main);
		UT_RUN(legacy_update_refuses_short_headers_and_inconsistent_payload_lengths);
		UT_RUN(managed_update_preserves_original_strict_intent_decoder);
		UT_RUN(unknown_opcodes_keep_original_no_description_behavior);
	}
	UT_DONE();
	free(description.data);
	return ut_failed_count == 0 ? 0 : 1;
}
