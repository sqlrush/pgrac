/*-------------------------------------------------------------------------
 *
 * test_pgrac_fenced_config.c
 *	  RF-ROOT P4 strict root-config grammar tests.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pgrac_fenced_config.h"

#include <sys/stat.h>

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

static const char valid_config[] = "format_version=1\n"
								   "mapping_generation=7\n"
								   "system_identifier=81985529216486895\n"
								   "storage_backend_id=3\n"
								   "storage_uuid=00112233445566778899aabbccddeeff\n"
								   "allowed_db_uid=501\n"
								   "allowed_db_gid=20\n"
								   "provider_id=0\n"
								   "provider_abi=1\n"
								   "node.0.target_uuid=ffeeddccbbaa99887766554433221100\n"
								   "node.0.adapter_data=\n";

UT_TEST(test_config_accepts_exact_canonical_provider_zero)
{
	PgracFencedConfigV1 config;

	UT_ASSERT_EQ(pgrac_fenced_config_parse_v1((const uint8 *)valid_config, sizeof(valid_config) - 1,
											  &config),
				 PGRAC_FENCED_CONFIG_OK);
	UT_ASSERT_EQ(config.format_version, 1);
	UT_ASSERT_EQ(config.mapping_generation, 7);
	UT_ASSERT_EQ(config.system_identifier, UINT64_C(81985529216486895));
	UT_ASSERT_EQ(config.storage_backend_id, 3);
	UT_ASSERT_EQ(config.allowed_db_uid, 501);
	UT_ASSERT_EQ(config.allowed_db_gid, 20);
	UT_ASSERT_EQ(config.provider_id, 0);
	UT_ASSERT_EQ(config.provider_abi, 1);
	UT_ASSERT_EQ(config.node_count, 1);
	UT_ASSERT(config.nodes[0].present);
	UT_ASSERT_EQ(config.nodes[0].adapter_data_len, 0);
}

UT_TEST(test_config_rejects_noncanonical_numeric_and_hex)
{
	PgracFencedConfigV1 config;
	char changed[sizeof(valid_config) + 1];
	char *at;

	memcpy(changed, valid_config, sizeof(valid_config));
	at = strstr(changed, "mapping_generation=7");
	UT_ASSERT_NOT_NULL(at);
	if (at == NULL)
		return;
	memmove(at + strlen("mapping_generation=") + 2, at + strlen("mapping_generation=") + 1,
			sizeof(valid_config) - (at - changed + strlen("mapping_generation=") + 1));
	at[strlen("mapping_generation=")] = '0';
	UT_ASSERT_NE(
		pgrac_fenced_config_parse_v1((const uint8 *)changed, sizeof(valid_config), &config),
		PGRAC_FENCED_CONFIG_OK);

	memcpy(changed, valid_config, sizeof(valid_config));
	at = strstr(changed, "storage_uuid=");
	UT_ASSERT_NOT_NULL(at);
	if (at == NULL)
		return;
	at[strlen("storage_uuid=") + 20] = 'A';
	UT_ASSERT_NE(pgrac_fenced_config_parse_v1((const uint8 *)changed, sizeof(changed) - 1, &config),
				 PGRAC_FENCED_CONFIG_OK);
}

UT_TEST(test_config_rejects_reorder_unknown_blank_and_missing_lf)
{
	PgracFencedConfigV1 config;
	static const char reordered[] = "mapping_generation=7\nformat_version=1\n";
	static const char unknown[] = "format_version=1\nunknown=1\n";
	static const char blank[] = "format_version=1\n\n";

	UT_ASSERT_NE(
		pgrac_fenced_config_parse_v1((const uint8 *)reordered, sizeof(reordered) - 1, &config),
		PGRAC_FENCED_CONFIG_OK);
	UT_ASSERT_NE(pgrac_fenced_config_parse_v1((const uint8 *)unknown, sizeof(unknown) - 1, &config),
				 PGRAC_FENCED_CONFIG_OK);
	UT_ASSERT_NE(pgrac_fenced_config_parse_v1((const uint8 *)blank, sizeof(blank) - 1, &config),
				 PGRAC_FENCED_CONFIG_OK);
	UT_ASSERT_NE(pgrac_fenced_config_parse_v1((const uint8 *)valid_config, sizeof(valid_config) - 2,
											  &config),
				 PGRAC_FENCED_CONFIG_OK);
}

UT_TEST(test_config_rejects_mismatched_node_and_oversize)
{
	PgracFencedConfigV1 config;
	char changed[sizeof(valid_config)];
	char *at;
	uint8 oversized[PGRAC_FENCED_CONFIG_MAX_BYTES + 1];

	memcpy(changed, valid_config, sizeof(valid_config));
	at = strstr(changed, "node.0.target_uuid");
	UT_ASSERT_NOT_NULL(at);
	if (at == NULL)
		return;
	at[5] = '1';
	UT_ASSERT_NE(pgrac_fenced_config_parse_v1((const uint8 *)changed, sizeof(changed) - 1, &config),
				 PGRAC_FENCED_CONFIG_OK);

	memset(oversized, 'x', sizeof(oversized));
	UT_ASSERT_EQ(pgrac_fenced_config_parse_v1(oversized, sizeof(oversized), &config),
				 PGRAC_FENCED_CONFIG_TOO_LARGE);
}

UT_TEST(test_config_rejects_duplicate_target_uuid)
{
	PgracFencedConfigV1 config;
	static const char duplicate_target[] = "format_version=1\n"
										   "mapping_generation=7\n"
										   "system_identifier=81985529216486895\n"
										   "storage_backend_id=3\n"
										   "storage_uuid=00112233445566778899aabbccddeeff\n"
										   "allowed_db_uid=501\n"
										   "allowed_db_gid=20\n"
										   "provider_id=0\n"
										   "provider_abi=1\n"
										   "node.0.target_uuid=ffeeddccbbaa99887766554433221100\n"
										   "node.0.adapter_data=\n"
										   "node.1.target_uuid=ffeeddccbbaa99887766554433221100\n"
										   "node.1.adapter_data=\n";

	UT_ASSERT_NE(pgrac_fenced_config_parse_v1((const uint8 *)duplicate_target,
											  sizeof(duplicate_target) - 1, &config),
				 PGRAC_FENCED_CONFIG_OK);
}

UT_TEST(test_config_file_metadata_is_exact_root_regular_0600)
{
	struct stat st;

	memset(&st, 0, sizeof(st));
	st.st_mode = S_IFREG | 0600;
	st.st_uid = 0;
	st.st_gid = 0;
	st.st_size = sizeof(valid_config) - 1;
	UT_ASSERT(pgrac_fenced_config_stat_secure(&st));

	st.st_mode = S_IFREG | 0640;
	UT_ASSERT(!pgrac_fenced_config_stat_secure(&st));
	st.st_mode = S_IFREG | 0600;
	st.st_uid = 1;
	UT_ASSERT(!pgrac_fenced_config_stat_secure(&st));
	st.st_uid = 0;
	st.st_gid = 1;
	UT_ASSERT(!pgrac_fenced_config_stat_secure(&st));
	st.st_gid = 0;
	st.st_size = PGRAC_FENCED_CONFIG_MAX_BYTES + 1;
	UT_ASSERT(!pgrac_fenced_config_stat_secure(&st));
}

UT_TEST(test_mapping_reload_generation_rules)
{
	PgracFencedConfigV1 current;
	PgracFencedConfigV1 candidate;
	uint8 current_digest[PGRAC_FENCED_CONFIG_DIGEST_BYTES];
	uint8 candidate_digest[PGRAC_FENCED_CONFIG_DIGEST_BYTES];
	char changed[sizeof(valid_config)];
	char *at;

	UT_ASSERT_EQ(pgrac_fenced_config_parse_v1((const uint8 *)valid_config, sizeof(valid_config) - 1,
											  &current),
				 PGRAC_FENCED_CONFIG_OK);
	UT_ASSERT(pgrac_fenced_config_digest_v1((const uint8 *)valid_config, sizeof(valid_config) - 1,
											current_digest));
	UT_ASSERT_EQ(
		pgrac_fenced_config_reload_decide_v1(&current, current_digest, &current, current_digest),
		PGRAC_FENCED_CONFIG_RELOAD_UNCHANGED);

	memcpy(changed, valid_config, sizeof(changed));
	at = strstr(changed, "node.0.target_uuid=");
	UT_ASSERT_NOT_NULL(at);
	if (at == NULL)
		return;
	at[strlen("node.0.target_uuid=")] = 'e';
	UT_ASSERT_EQ(
		pgrac_fenced_config_parse_v1((const uint8 *)changed, sizeof(changed) - 1, &candidate),
		PGRAC_FENCED_CONFIG_OK);
	UT_ASSERT(pgrac_fenced_config_digest_v1((const uint8 *)changed, sizeof(changed) - 1,
											candidate_digest));
	UT_ASSERT_EQ(pgrac_fenced_config_reload_decide_v1(&current, current_digest, &candidate,
													  candidate_digest),
				 PGRAC_FENCED_CONFIG_RELOAD_REJECT_SAME_GENERATION_CHANGE);

	memcpy(changed, valid_config, sizeof(changed));
	at = strstr(changed, "mapping_generation=7");
	UT_ASSERT_NOT_NULL(at);
	if (at == NULL)
		return;
	at[strlen("mapping_generation=")] = '8';
	UT_ASSERT_EQ(
		pgrac_fenced_config_parse_v1((const uint8 *)changed, sizeof(changed) - 1, &candidate),
		PGRAC_FENCED_CONFIG_OK);
	UT_ASSERT(pgrac_fenced_config_digest_v1((const uint8 *)changed, sizeof(changed) - 1,
											candidate_digest));
	UT_ASSERT_EQ(pgrac_fenced_config_reload_decide_v1(&current, current_digest, &candidate,
													  candidate_digest),
				 PGRAC_FENCED_CONFIG_RELOAD_ADVANCE);

	at[strlen("mapping_generation=")] = '6';
	UT_ASSERT_EQ(
		pgrac_fenced_config_parse_v1((const uint8 *)changed, sizeof(changed) - 1, &candidate),
		PGRAC_FENCED_CONFIG_OK);
	UT_ASSERT(pgrac_fenced_config_digest_v1((const uint8 *)changed, sizeof(changed) - 1,
											candidate_digest));
	UT_ASSERT_EQ(pgrac_fenced_config_reload_decide_v1(&current, current_digest, &candidate,
													  candidate_digest),
				 PGRAC_FENCED_CONFIG_RELOAD_REJECT_REGRESSION);
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_config_accepts_exact_canonical_provider_zero);
	UT_RUN(test_config_rejects_noncanonical_numeric_and_hex);
	UT_RUN(test_config_rejects_reorder_unknown_blank_and_missing_lf);
	UT_RUN(test_config_rejects_mismatched_node_and_oversize);
	UT_RUN(test_config_rejects_duplicate_target_uuid);
	UT_RUN(test_config_file_metadata_is_exact_root_regular_0600);
	UT_RUN(test_mapping_reload_generation_rules);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
