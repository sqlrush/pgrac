/*-------------------------------------------------------------------------
 *
 * test_pgrac_fenced_config.c
 *	  RF-ROOT P4 strict root-config grammar tests.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pgrac_fenced_config.h"
#include "pgrac_fenced_cib.h"
#include "common/pgrac_fence_map.h"
#ifdef USE_OPENSSL
#include <openssl/evp.h>
#endif

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

static const char native_fields[]
	= "pacemaker_resource=fence\n"
	  "pacemaker_configuration_digest="
	  "1111111111111111111111111111111111111111111111111111111111111111\n"
	  "target_inventory_digest=2222222222222222222222222222222222222222222222222222222222222222\n"
	  "target_drain_public_key=c050c5637a44fa8629fff3cccce2300cb362a63d99d95fc54145266f4332445a\n"
	  "target_bundle_directory=/usr/lib/pgrac/target\n"
	  "target_client_config=/etc/pgrac/target-client.json\n";

static void
native_fixture(char *out, size_t capacity)
{
	char intermediate[8192], replacement[2048];
	snprintf(replacement, sizeof(replacement), "%snode.2.target_uuid=", native_fields);
	UT_ASSERT(replace_config_field(fenced_config_v2, "node.2.target_uuid=", replacement,
								   intermediate, sizeof(intermediate)));
	UT_ASSERT(replace_config_field(intermediate, "node.2.protected_set_digest=",
								   "node.2.pacemaker_name=node2\nnode.2.protected_set_digest=", out,
								   capacity));
}

UT_TEST(test_native_config_binds_verified_map_to_independent_policy)
{
	char raw[8192];
	PgracFencedConfigV1 config;
	PgracFencedCibPolicy policy;
	PgracFencedCibNode nodes[2];
	PgracFencedConfigResult result;
	native_fixture(raw, sizeof(raw));
	result = pgrac_fenced_config_parse((const uint8 *)raw, strlen(raw), &config);
#ifdef USE_OPENSSL
	UT_ASSERT_EQ(result, PGRAC_FENCED_CONFIG_OK);
	if (result != PGRAC_FENCED_CONFIG_OK)
		return;
	UT_ASSERT(config.native.present);
	UT_ASSERT(strcmp(config.native.resource, "fence") == 0);
	UT_ASSERT_EQ(config.native.cib_digest[0], 0x11);
	UT_ASSERT_EQ(config.native.inventory_digest[31], 0x22);
	UT_ASSERT_EQ(config.native.drain_public_key[0], 0xc0);
	UT_ASSERT(strcmp(config.native.bundle_directory, "/usr/lib/pgrac/target") == 0);
	UT_ASSERT(strcmp(config.native.client_config, "/etc/pgrac/target-client.json") == 0);
	UT_ASSERT(pgrac_fenced_config_cib_policy(&config, &policy, nodes, lengthof(nodes)));
	UT_ASSERT_EQ(policy.node_count, 1);
	UT_ASSERT(policy.nodes == nodes);
	UT_ASSERT(strcmp(policy.resource, "fence") == 0);
	UT_ASSERT_EQ(policy.configuration_digest[31], 0x11);
	UT_ASSERT(strcmp(nodes[0].name, "node2") == 0);
	UT_ASSERT_EQ(nodes[0].guest_uuid[0], 5);
	UT_ASSERT_EQ(nodes[0].guest_uuid[15], 5);
	config.nodes[2].adapter_data[973] ^= 1;
	UT_ASSERT(!pgrac_fenced_config_cib_policy(&config, &policy, nodes, lengthof(nodes)));
#else
	UT_ASSERT_NE(result, PGRAC_FENCED_CONFIG_OK);
	UT_ASSERT(!pgrac_fenced_config_cib_policy(&config, &policy, nodes, lengthof(nodes)));
#endif
}

UT_TEST(test_native_config_is_not_optional_partial_or_unsafe)
{
	const char *changes[][2] = {
		{ "pacemaker_resource=fence", "pacemaker_resource=-fence" },
		{ "pacemaker_resource=fence", "pacemaker_resource=fence other" },
		{ "pacemaker_configuration_digest="
		  "1111111111111111111111111111111111111111111111111111111111111111",
		  "pacemaker_configuration_digest="
		  "0000000000000000000000000000000000000000000000000000000000000000" },
		{ "target_inventory_digest="
		  "2222222222222222222222222222222222222222222222222222222222222222",
		  "target_inventory_digest="
		  "0000000000000000000000000000000000000000000000000000000000000000" },
		{ "target_drain_public_key="
		  "c050c5637a44fa8629fff3cccce2300cb362a63d99d95fc54145266f4332445a",
		  "target_drain_public_key="
		  "0100000000000000000000000000000000000000000000000000000000000000" },
		{ "/usr/lib/pgrac/target", "/usr/lib/../target" },
		{ "/usr/lib/pgrac/target", "/usr//lib/target" },
		{ "/usr/lib/pgrac/target", "relative/path" },
		{ "/usr/lib/pgrac/target", "/usr/lib/pgrac/target/" },
		{ "/etc/pgrac/target-client.json", "/etc/./target-client.json" },
		{ "target_client_config=/etc/pgrac/target-client.json\n", "" },
		{ "target_inventory_digest=", "unknown=2222\ntarget_inventory_digest=" },
		{ "node.2.pacemaker_name=node2\n", "" },
		{ "node.2.pacemaker_name=node2", "node.3.pacemaker_name=node2" },
		{ "node.2.pacemaker_name=node2", "node.2.pacemaker_name=node2;other" },
	};
	char raw[8192], changed[8192];
	PgracFencedConfigV1 config, zero;
	native_fixture(raw, sizeof(raw));
	memset(&zero, 0, sizeof(zero));
	for (unsigned n = 0; n < lengthof(changes); n++) {
		UT_ASSERT(
			replace_config_field(raw, changes[n][0], changes[n][1], changed, sizeof(changed)));
		memset(&config, 0x7f, sizeof(config));
		UT_ASSERT_NE(pgrac_fenced_config_parse((const uint8 *)changed, strlen(changed), &config),
					 PGRAC_FENCED_CONFIG_OK);
		UT_ASSERT(memcmp(&config, &zero, sizeof(config)) == 0);
	}
}

UT_TEST(test_native_policy_cannot_be_derived_from_legacy_or_insufficient_storage)
{
	PgracFencedConfigV1 config;
	PgracFencedCibPolicy policy, zero = { 0 };
	PgracFencedCibNode nodes[1];
	char raw[8192];
	UT_ASSERT_EQ(
		pgrac_fenced_config_parse((const uint8 *)valid_config, sizeof(valid_config) - 1, &config),
		PGRAC_FENCED_CONFIG_OK);
	memset(&policy, 0x7f, sizeof(policy));
	UT_ASSERT(!pgrac_fenced_config_cib_policy(&config, &policy, nodes, 1));
	UT_ASSERT(memcmp(&policy, &zero, sizeof(policy)) == 0);
	native_fixture(raw, sizeof(raw));
#ifdef USE_OPENSSL
	UT_ASSERT_EQ(pgrac_fenced_config_parse((const uint8 *)raw, strlen(raw), &config),
				 PGRAC_FENCED_CONFIG_OK);
	UT_ASSERT(!pgrac_fenced_config_cib_policy(&config, &policy, nodes, 0));
	UT_ASSERT(memcmp(&policy, &zero, sizeof(policy)) == 0);
#endif
}

UT_TEST(test_native_policy_change_requires_config_generation)
{
#ifdef USE_OPENSSL
	PgracFencedConfigV1 first, next;
	char raw[8192], changed[8192];
	uint8 first_digest[32], next_digest[32];
	const char *changes[][2] = {
		{ "pacemaker_configuration_digest=11", "pacemaker_configuration_digest=12" },
		{ "/usr/lib/pgrac/target", "/usr/lib/pgrac/target-next" },
		{ "node2", "renamed-node2" },
	};
	native_fixture(raw, sizeof(raw));
	UT_ASSERT_EQ(pgrac_fenced_config_parse((const uint8 *)raw, strlen(raw), &first),
				 PGRAC_FENCED_CONFIG_OK);
	UT_ASSERT(pgrac_fenced_config_digest_v1((const uint8 *)raw, strlen(raw), first_digest));
	for (unsigned n = 0; n < lengthof(changes); n++) {
		UT_ASSERT(
			replace_config_field(raw, changes[n][0], changes[n][1], changed, sizeof(changed)));
		UT_ASSERT_EQ(pgrac_fenced_config_parse((const uint8 *)changed, strlen(changed), &next),
					 PGRAC_FENCED_CONFIG_OK);
		UT_ASSERT(
			pgrac_fenced_config_digest_v1((const uint8 *)changed, strlen(changed), next_digest));
		UT_ASSERT_EQ(pgrac_fenced_config_reload_decide_v1(&first, first_digest, &next, next_digest),
					 PGRAC_FENCED_CONFIG_RELOAD_REJECT_SAME_GENERATION_CHANGE);
	}
#endif
}

#ifdef USE_OPENSSL
static void
test_hex(const uint8 *bytes, size_t size, char *out)
{
	static const char hex[] = "0123456789abcdef";
	for (size_t n = 0; n < size; n++) {
		out[n * 2] = hex[bytes[n] >> 4];
		out[n * 2 + 1] = hex[bytes[n] & 15];
	}
	out[size * 2] = '\0';
}

/* Real signed fixture producer: this tests config name uniqueness, not the
 * common wire codec. Both nodes have independently valid crypto and bindings.
 * The synthetic seed is used only by this standalone test executable.
 */
static void
append_signed_node(char *raw, size_t capacity, unsigned node, EVP_PKEY *key,
				   const PgracFencedNodeConfigV1 *source)
{
	uint8 bytes[4096], digest[32];
	char encoded[8193], digest_hex[65], guest_hex[33];
	PgracProtectedSetDecodedV2 decoded;
	EVP_MD_CTX *signer = EVP_MD_CTX_new();
	size_t payload, signature = 64, used = strlen(raw);
	int written;
	if (!signer
		|| !pgrac_protected_set_v2_decode(source->adapter_data + 32, source->adapter_data_len - 96,
										  &decoded))
		abort();
	memset(decoded.set.guest_uuid, node + 3, 16);
	memcpy(bytes, source->adapter_data, 32);
	bytes[12] = node;
	if (!pgrac_protected_set_v2_encode(&decoded.set, bytes + 32, sizeof(bytes) - 96, &payload)
		|| !pgrac_external_fence_protected_set_digest_v2(&decoded.set, digest))
		abort();
	for (unsigned n = 0; n < 4; n++)
		bytes[24 + n] = payload >> (n * 8);
	if (EVP_DigestSignInit(signer, NULL, NULL, NULL, key) != 1
		|| EVP_DigestSign(signer, bytes + 32 + payload, &signature, bytes, 32 + payload) != 1
		|| signature != 64)
		abort();
	test_hex(bytes, 32 + payload + signature, encoded);
	test_hex(digest, 32, digest_hex);
	test_hex(decoded.set.guest_uuid, 16, guest_hex);
	written = snprintf(raw + used, capacity - used,
					   "node.%u.target_uuid=%s\nnode.%u.pacemaker_name=node%u\n"
					   "node.%u.protected_set_digest=%s\nnode.%u.adapter_data=%s\n",
					   node, guest_hex, node, node, node, digest_hex, node, encoded);
	if (written < 0 || (size_t)written >= capacity - used)
		abort();
	EVP_MD_CTX_free(signer);
}

static void
two_node_fixture(char *raw, size_t capacity)
{
	const uint8 synthetic_seed[32] = { 0x7a };
	EVP_PKEY *key = EVP_PKEY_new_raw_private_key(EVP_PKEY_ED25519, NULL, synthetic_seed, 32);
	uint8 public_key[32];
	size_t length = sizeof(public_key);
	char public_hex[65], temporary[8192];
	PgracFencedConfigV1 source;
	char *at;
	if (!key || EVP_PKEY_get_raw_public_key(key, public_key, &length) != 1 || length != 32
		|| pgrac_fenced_config_parse((const uint8 *)fenced_config_v2, sizeof(fenced_config_v2) - 1,
									 &source)
			   != PGRAC_FENCED_CONFIG_OK)
		abort();
	native_fixture(temporary, sizeof(temporary));
	at = strstr(temporary, "map_public_key=");
	if (at == NULL)
		abort();
	test_hex(public_key, 32, public_hex);
	memcpy(at + strlen("map_public_key="), public_hex, 64);
	at = strstr(temporary, "node.2.target_uuid=");
	if (at == NULL || (size_t)(at - temporary) >= capacity)
		abort();
	*at = '\0';
	strcpy(raw, temporary);
	append_signed_node(raw, capacity, 2, key, &source.nodes[2]);
	append_signed_node(raw, capacity, 3, key, &source.nodes[2]);
	EVP_PKEY_free(key);
}
#endif

UT_TEST(test_native_config_unique_names_across_valid_signed_nodes)
{
#ifdef USE_OPENSSL
	char raw[16384], changed[16384];
	PgracFencedConfigV1 config;
	PgracFencedCibPolicy policy, zero = { 0 };
	PgracFencedCibNode nodes[2];
	two_node_fixture(raw, sizeof(raw));
	UT_ASSERT_EQ(pgrac_fenced_config_parse((const uint8 *)raw, strlen(raw), &config),
				 PGRAC_FENCED_CONFIG_OK);
	UT_ASSERT(pgrac_fenced_config_cib_policy(&config, &policy, nodes, 2));
	UT_ASSERT_EQ(policy.node_count, 2);
	UT_ASSERT_EQ(nodes[1].guest_uuid[15], 6);
	UT_ASSERT(strcmp(nodes[1].name, "node3") == 0);
	strcpy(config.native.node_names[3], "node2");
	UT_ASSERT(!pgrac_fenced_config_cib_policy(&config, &policy, nodes, 2));
	UT_ASSERT(memcmp(&policy, &zero, sizeof(zero)) == 0);
	UT_ASSERT(replace_config_field(raw, "node.3.pacemaker_name=node3",
								   "node.3.pacemaker_name=node2", changed, sizeof(changed)));
	UT_ASSERT_NE(pgrac_fenced_config_parse((const uint8 *)changed, strlen(changed), &config),
				 PGRAC_FENCED_CONFIG_OK);
#endif
}

int
main(void)
{
	UT_PLAN(15);
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
	UT_RUN(test_native_config_binds_verified_map_to_independent_policy);
	UT_RUN(test_native_config_is_not_optional_partial_or_unsafe);
	UT_RUN(test_native_policy_cannot_be_derived_from_legacy_or_insufficient_storage);
	UT_RUN(test_native_policy_change_requires_config_generation);
	UT_RUN(test_native_config_unique_names_across_valid_signed_nodes);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
