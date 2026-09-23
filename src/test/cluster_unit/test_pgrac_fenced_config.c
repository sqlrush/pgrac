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

#include "data/pgrac_fence_map_v2_fixture.h"

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

static bool
replace_config_field(const char *source, const char *needle, const char *replacement, char *out,
					 size_t capacity)
{
	const char *at = strstr(source, needle);
	int written;

	if (at == NULL)
		return false;
	written = snprintf(out, capacity, "%.*s%s%s", (int)(at - source), source, replacement,
					   at + strlen(needle));
	return written > 0 && (size_t)written < capacity;
}

UT_TEST(test_config_v2_requires_verified_current_map)
{
	PgracFencedConfigV1 config;
	PgracFencedConfigResult result;
	uint8 digest[32];

	result = pgrac_fenced_config_parse((const uint8 *)fenced_config_v2,
									   sizeof(fenced_config_v2) - 1, &config);
#ifdef USE_OPENSSL
	UT_ASSERT_EQ(result, PGRAC_FENCED_CONFIG_OK);
	if (result != PGRAC_FENCED_CONFIG_OK)
		return;
	UT_ASSERT_EQ(config.format_version, 2);
	UT_ASSERT_EQ(config.provider_id, 257);
	UT_ASSERT_EQ(config.node_count, 1);
	UT_ASSERT(config.nodes[2].present);
	UT_ASSERT(pgrac_fenced_config_protected_set_digest(&config, 2, digest));
	UT_ASSERT(memcmp(digest, config.nodes[2].protected_set_digest, 32) == 0);
	/* Re-reading must not trust only the cached digest after signature corruption. */
	config.nodes[2].adapter_data[973] ^= 1;
	UT_ASSERT(!pgrac_fenced_config_protected_set_digest(&config, 2, digest));
#else
	UT_ASSERT_NE(result, PGRAC_FENCED_CONFIG_OK);
	UT_ASSERT(!pgrac_fenced_config_protected_set_digest(&config, 2, digest));
#endif
}

UT_TEST(test_config_v2_rejects_rebinding_and_legacy_provider)
{
	PgracFencedConfigV1 config;
	char changed[sizeof(fenced_config_v2) + 16];
	static const char *needles[] = { "mapping_generation=7",
									 "system_identifier=123456789",
									 "storage_backend_id=3",
									 "storage_uuid=02",
									 "provider_id=257",
									 "node.2.target_uuid=05",
									 "node.2.protected_set_digest=84",
									 "map_public_key=c0" };
	static const char *replacements[] = { "mapping_generation=8",
										  "system_identifier=123456790",
										  "storage_backend_id=2",
										  "storage_uuid=03",
										  "provider_id=256",
										  "node.2.target_uuid=06",
										  "node.2.protected_set_digest=85",
										  "map_public_key=c1" };
	unsigned int i;

	for (i = 0; i < lengthof(needles); i++) {
		UT_ASSERT(replace_config_field(fenced_config_v2, needles[i], replacements[i], changed,
									   sizeof(changed)));
		UT_ASSERT_NE(pgrac_fenced_config_parse((const uint8 *)changed, strlen(changed), &config),
					 PGRAC_FENCED_CONFIG_OK);
	}
	UT_ASSERT(replace_config_field(valid_config, "provider_id=0", "provider_id=257", changed,
								   sizeof(changed)));
	UT_ASSERT_NE(pgrac_fenced_config_parse_v1((const uint8 *)changed, strlen(changed), &config),
				 PGRAC_FENCED_CONFIG_OK);
	UT_ASSERT_NE(pgrac_fenced_config_parse((const uint8 *)changed, strlen(changed), &config),
				 PGRAC_FENCED_CONFIG_OK);
}

UT_TEST(test_profile_digest_retains_v1_and_rejects_unknown_node)
{
	PgracFencedConfigV1 config;
	uint8 digest[32];
	static const uint8 zero[32] = { 0 };

	UT_ASSERT_EQ(
		pgrac_fenced_config_parse((const uint8 *)valid_config, sizeof(valid_config) - 1, &config),
		PGRAC_FENCED_CONFIG_OK);
	UT_ASSERT(pgrac_fenced_config_protected_set_digest(&config, 0, digest));
	UT_ASSERT(memcmp(digest, zero, sizeof(digest)) != 0);
	UT_ASSERT(!pgrac_fenced_config_protected_set_digest(&config, -1, digest));
	UT_ASSERT(memcmp(digest, zero, sizeof(digest)) == 0);
	UT_ASSERT(!pgrac_fenced_config_protected_set_digest(&config, 1, digest));
	UT_ASSERT(!pgrac_fenced_config_protected_set_digest(&config, 128, digest));
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(test_config_accepts_exact_canonical_provider_zero);
	UT_RUN(test_config_rejects_noncanonical_numeric_and_hex);
	UT_RUN(test_config_rejects_reorder_unknown_blank_and_missing_lf);
	UT_RUN(test_config_rejects_mismatched_node_and_oversize);
	UT_RUN(test_config_rejects_duplicate_target_uuid);
	UT_RUN(test_config_file_metadata_is_exact_root_regular_0600);
	UT_RUN(test_mapping_reload_generation_rules);
	UT_RUN(test_config_v2_requires_verified_current_map);
	UT_RUN(test_config_v2_rejects_rebinding_and_legacy_provider);
	UT_RUN(test_profile_digest_retains_v1_and_rejects_unknown_node);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
