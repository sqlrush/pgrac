/*-------------------------------------------------------------------------
 *
 * test_pgrac_fenced_ipmi.c
 *    Fixed IPMI adapter byte parser tests.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "pgrac_fenced_ipmi.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

#define VALID_PATH "/usr/libexec/pgrac/ipmitool"

static size_t
make_adapter(uint8 *bytes, uint16 address_family, const char *path)
{
	size_t path_len = strlen(path);
	size_t total_len = PGRAC_FENCED_IPMI_ADAPTER_FIXED_BYTES + path_len;

	memset(bytes, 0, PGRAC_FENCED_IPMI_ADAPTER_MAX_BYTES);
	memcpy(bytes, PGRAC_FENCED_IPMI_ADAPTER_MAGIC, 4);
	bytes[4] = PGRAC_FENCED_IPMI_ADAPTER_VERSION;
	bytes[6] = (uint8)total_len;
	bytes[7] = (uint8)(total_len >> 8);
	bytes[8] = (uint8)address_family;
	bytes[10] = UINT8_C(0x6f);
	bytes[11] = UINT8_C(0x02);
	if (address_family == 4) {
		bytes[12] = 192;
		bytes[13] = 0;
		bytes[14] = 2;
		bytes[15] = 10;
	} else {
		bytes[12] = 0x20;
		bytes[13] = 0x01;
		bytes[27] = 0x01;
	}
	bytes[28] = 17;
	bytes[30] = (uint8)path_len;
	bytes[31] = (uint8)(path_len >> 8);
	memset(bytes + 32, 0x5a, 32);
	memcpy(bytes + PGRAC_FENCED_IPMI_ADAPTER_FIXED_BYTES, path, path_len);
	return total_len;
}

UT_TEST(test_adapter_accepts_exact_ipv4_and_ipv6_layouts)
{
	PgracFencedIpmiAdapterV1 adapter;
	uint8 bytes[PGRAC_FENCED_IPMI_ADAPTER_MAX_BYTES];
	size_t len;

	len = make_adapter(bytes, 4, VALID_PATH);
	UT_ASSERT(pgrac_fenced_ipmi_adapter_parse(bytes, len, &adapter));
	UT_ASSERT_EQ(adapter.address_family, 4);
	UT_ASSERT_EQ(adapter.port, 623);
	UT_ASSERT_EQ(adapter.cipher_suite, 17);
	UT_ASSERT_EQ(adapter.executable_path_len, strlen(VALID_PATH));
	UT_ASSERT_STR_EQ(adapter.executable_path, VALID_PATH);
	UT_ASSERT(memcmp(adapter.executable_sha256, bytes + 32, 32) == 0);
	UT_ASSERT(memcmp(adapter.address, bytes + 12, 16) == 0);

	len = make_adapter(bytes, 6, VALID_PATH);
	UT_ASSERT(pgrac_fenced_ipmi_adapter_parse(bytes, len, &adapter));
	UT_ASSERT_EQ(adapter.address_family, 6);
	UT_ASSERT(memcmp(adapter.address, bytes + 12, 16) == 0);
}

UT_TEST(test_adapter_rejects_header_and_length_errors)
{
	PgracFencedIpmiAdapterV1 adapter;
	uint8 bytes[PGRAC_FENCED_IPMI_ADAPTER_MAX_BYTES];
	size_t len = make_adapter(bytes, 4, VALID_PATH);

	bytes[0] = 'X';
	UT_ASSERT(!pgrac_fenced_ipmi_adapter_parse(bytes, len, &adapter));
	make_adapter(bytes, 4, VALID_PATH);
	bytes[4] = 2;
	UT_ASSERT(!pgrac_fenced_ipmi_adapter_parse(bytes, len, &adapter));
	make_adapter(bytes, 4, VALID_PATH);
	bytes[5] = 1;
	UT_ASSERT(!pgrac_fenced_ipmi_adapter_parse(bytes, len, &adapter));
	make_adapter(bytes, 4, VALID_PATH);
	bytes[6]--;
	UT_ASSERT(!pgrac_fenced_ipmi_adapter_parse(bytes, len, &adapter));
	make_adapter(bytes, 4, VALID_PATH);
	bytes[30]--;
	UT_ASSERT(!pgrac_fenced_ipmi_adapter_parse(bytes, len, &adapter));
	UT_ASSERT(!pgrac_fenced_ipmi_adapter_parse(bytes, 63, &adapter));
	UT_ASSERT(
		!pgrac_fenced_ipmi_adapter_parse(bytes, PGRAC_FENCED_IPMI_ADAPTER_MAX_BYTES + 1, &adapter));
	UT_ASSERT(!pgrac_fenced_ipmi_adapter_parse(NULL, len, &adapter));
	UT_ASSERT(!pgrac_fenced_ipmi_adapter_parse(bytes, len, NULL));
}

UT_TEST(test_adapter_rejects_address_and_option_errors)
{
	PgracFencedIpmiAdapterV1 adapter;
	uint8 bytes[PGRAC_FENCED_IPMI_ADAPTER_MAX_BYTES];
	size_t len = make_adapter(bytes, 4, VALID_PATH);

	bytes[8] = 5;
	UT_ASSERT(!pgrac_fenced_ipmi_adapter_parse(bytes, len, &adapter));
	make_adapter(bytes, 4, VALID_PATH);
	bytes[9] = 1;
	UT_ASSERT(!pgrac_fenced_ipmi_adapter_parse(bytes, len, &adapter));
	make_adapter(bytes, 4, VALID_PATH);
	bytes[10] = 0;
	bytes[11] = 0;
	UT_ASSERT(!pgrac_fenced_ipmi_adapter_parse(bytes, len, &adapter));
	make_adapter(bytes, 4, VALID_PATH);
	bytes[16] = 1;
	UT_ASSERT(!pgrac_fenced_ipmi_adapter_parse(bytes, len, &adapter));
	make_adapter(bytes, 4, VALID_PATH);
	bytes[28] = 0;
	UT_ASSERT(!pgrac_fenced_ipmi_adapter_parse(bytes, len, &adapter));
	make_adapter(bytes, 4, VALID_PATH);
	bytes[29] = 1;
	UT_ASSERT(!pgrac_fenced_ipmi_adapter_parse(bytes, len, &adapter));
}

static void
assert_bad_path(const char *path)
{
	PgracFencedIpmiAdapterV1 adapter;
	uint8 bytes[PGRAC_FENCED_IPMI_ADAPTER_MAX_BYTES];
	size_t len = make_adapter(bytes, 4, path);

	UT_ASSERT(!pgrac_fenced_ipmi_adapter_parse(bytes, len, &adapter));
}

UT_TEST(test_adapter_rejects_noncanonical_paths)
{
	PgracFencedIpmiAdapterV1 adapter;
	uint8 bytes[PGRAC_FENCED_IPMI_ADAPTER_MAX_BYTES];
	size_t len;

	assert_bad_path("usr/bin/ipmitool");
	assert_bad_path("/");
	assert_bad_path("/usr//bin/ipmitool");
	assert_bad_path("/usr/./bin/ipmitool");
	assert_bad_path("/usr/../bin/ipmitool");
	assert_bad_path("/usr/bin/ipmitool/");
	len = make_adapter(bytes, 4, VALID_PATH);
	bytes[PGRAC_FENCED_IPMI_ADAPTER_FIXED_BYTES + 4] = 0;
	UT_ASSERT(!pgrac_fenced_ipmi_adapter_parse(bytes, len, &adapter));
}

UT_TEST(test_adapter_accepts_maximum_canonical_path)
{
	PgracFencedIpmiAdapterV1 adapter;
	uint8 bytes[PGRAC_FENCED_IPMI_ADAPTER_MAX_BYTES];
	char path[PGRAC_FENCED_IPMI_ADAPTER_PATH_MAX + 1];
	size_t len;

	path[0] = '/';
	memset(path + 1, 'a', PGRAC_FENCED_IPMI_ADAPTER_PATH_MAX - 1);
	path[PGRAC_FENCED_IPMI_ADAPTER_PATH_MAX] = '\0';
	len = make_adapter(bytes, 6, path);
	UT_ASSERT(pgrac_fenced_ipmi_adapter_parse(bytes, len, &adapter));
	UT_ASSERT_EQ(adapter.executable_path_len, PGRAC_FENCED_IPMI_ADAPTER_PATH_MAX);
	UT_ASSERT_STR_EQ(adapter.executable_path, path);
}

UT_TEST(test_executable_and_credential_stat_rules_are_exact)
{
	struct stat st;

	memset(&st, 0, sizeof(st));
	st.st_mode = S_IFREG | 0755;
	st.st_uid = 0;
	st.st_gid = 80;
	UT_ASSERT(pgrac_fenced_ipmi_executable_stat_secure(&st));
	st.st_mode = S_IFREG | 0775;
	UT_ASSERT(!pgrac_fenced_ipmi_executable_stat_secure(&st));
	st.st_mode = S_IFREG | 0755;
	st.st_uid = 1;
	UT_ASSERT(!pgrac_fenced_ipmi_executable_stat_secure(&st));
	st.st_uid = 0;
	st.st_mode = S_IFDIR | 0755;
	UT_ASSERT(!pgrac_fenced_ipmi_executable_stat_secure(&st));

	st.st_mode = S_IFREG | 0600;
	st.st_uid = 0;
	st.st_gid = 0;
	UT_ASSERT(pgrac_fenced_ipmi_credential_stat_secure(&st));
	st.st_mode = S_IFREG | 0400;
	UT_ASSERT(!pgrac_fenced_ipmi_credential_stat_secure(&st));
	st.st_mode = S_IFREG | 0600;
	st.st_gid = 1;
	UT_ASSERT(!pgrac_fenced_ipmi_credential_stat_secure(&st));
}

UT_TEST(test_credential_grammars_are_exact)
{
	char username[PGRAC_FENCED_IPMI_USERNAME_MAX + 1];
	static const uint8 user_ok[] = "admin-1\n";
	static const uint8 password_ok[] = "s ecret!\n";

	UT_ASSERT(pgrac_fenced_ipmi_username_parse(user_ok, sizeof(user_ok) - 1, username));
	UT_ASSERT_STR_EQ(username, "admin-1");
	UT_ASSERT(!pgrac_fenced_ipmi_username_parse((const uint8 *)" admin\n", 7, username));
	UT_ASSERT(!pgrac_fenced_ipmi_username_parse((const uint8 *)"admin \n", 7, username));
	UT_ASSERT(!pgrac_fenced_ipmi_username_parse((const uint8 *)"admin\r\n", 7, username));
	UT_ASSERT(!pgrac_fenced_ipmi_username_parse((const uint8 *)"admin\nroot\n", 11, username));
	UT_ASSERT(!pgrac_fenced_ipmi_username_parse((const uint8 *)"admin", 5, username));
	UT_ASSERT(pgrac_fenced_ipmi_password_validate(password_ok, sizeof(password_ok) - 1));
	UT_ASSERT(!pgrac_fenced_ipmi_password_validate((const uint8 *)"secret", 6));
	UT_ASSERT(!pgrac_fenced_ipmi_password_validate((const uint8 *)"secret\r\n", 8));
	UT_ASSERT(!pgrac_fenced_ipmi_password_validate((const uint8 *)"secret\nmore\n", 12));
}

UT_TEST(test_credential_paths_use_raw_uuid_lowercase_hex)
{
	uint8 uuid[16];
	char user_path[PGRAC_FENCED_IPMI_CREDENTIAL_PATH_MAX];
	char password_path[PGRAC_FENCED_IPMI_CREDENTIAL_PATH_MAX];
	size_t i;

	for (i = 0; i < sizeof(uuid); i++)
		uuid[i] = (uint8)i;
	UT_ASSERT(pgrac_fenced_ipmi_credential_paths(uuid, user_path, sizeof(user_path), password_path,
												 sizeof(password_path)));
	UT_ASSERT_STR_EQ(user_path,
					 "/etc/pgrac/credentials/ipmi-000102030405060708090a0b0c0d0e0f.user");
	UT_ASSERT_STR_EQ(password_path,
					 "/etc/pgrac/credentials/ipmi-000102030405060708090a0b0c0d0e0f.password");
	memset(uuid, 0, sizeof(uuid));
	UT_ASSERT(!pgrac_fenced_ipmi_credential_paths(uuid, user_path, sizeof(user_path), password_path,
												  sizeof(password_path)));
}

UT_TEST(test_executable_open_requires_fresh_matching_hash)
{
	PgracFencedIpmiAdapterV1 adapter;
	int source_fd;
	int validated_fd;

	memset(&adapter, 0, sizeof(adapter));
	strcpy(adapter.executable_path, "/usr/bin/true");
	adapter.executable_path_len = strlen(adapter.executable_path);
	source_fd = open(adapter.executable_path, O_RDONLY);
	UT_ASSERT(source_fd >= 0);
	if (source_fd < 0)
		return;
	UT_ASSERT(pgrac_fenced_ipmi_sha256_fd(source_fd, adapter.executable_sha256));
	(void)close(source_fd);
	validated_fd = pgrac_fenced_ipmi_executable_open(&adapter);
	UT_ASSERT(validated_fd >= 0);
	if (validated_fd >= 0)
		(void)close(validated_fd);
	adapter.executable_sha256[0] ^= 1;
	UT_ASSERT_EQ(pgrac_fenced_ipmi_executable_open(&adapter), -1);
}

UT_TEST(test_executable_open_rejects_final_symlink)
{
	PgracFencedIpmiAdapterV1 adapter;
	char directory[] = "/tmp/pgrac-ipmi-symlink.XXXXXX";
	char path[PGRAC_FENCED_IPMI_ADAPTER_PATH_MAX + 1];
	int source_fd;

	UT_ASSERT_NOT_NULL(mkdtemp(directory));
	UT_ASSERT(snprintf(path, sizeof(path), "%s/tool", directory) > 0);
	UT_ASSERT_EQ(symlink("/usr/bin/true", path), 0);
	memset(&adapter, 0, sizeof(adapter));
	strcpy(adapter.executable_path, path);
	adapter.executable_path_len = strlen(path);
	source_fd = open("/usr/bin/true", O_RDONLY);
	UT_ASSERT(source_fd >= 0);
	if (source_fd >= 0) {
		UT_ASSERT(pgrac_fenced_ipmi_sha256_fd(source_fd, adapter.executable_sha256));
		(void)close(source_fd);
	}
	UT_ASSERT_EQ(pgrac_fenced_ipmi_executable_open(&adapter), -1);
	UT_ASSERT_EQ(unlink(path), 0);
	UT_ASSERT_EQ(rmdir(directory), 0);
}

UT_TEST(test_credentials_loader_fails_closed_when_files_are_absent)
{
	PgracFencedIpmiCredentialsV1 credentials;
	uint8 uuid[16];

	memset(uuid, 0x9d, sizeof(uuid));
	memset(&credentials, 0x7f, sizeof(credentials));
	UT_ASSERT(!pgrac_fenced_ipmi_credentials_load(uuid, &credentials));
	UT_ASSERT_EQ(credentials.username[0], '\0');
	UT_ASSERT_EQ(credentials.user_path[0], '\0');
	UT_ASSERT_EQ(credentials.password_path[0], '\0');
}

UT_TEST(test_invocation_uses_only_fixed_numeric_argv)
{
	PgracFencedIpmiAdapterV1 adapter;
	PgracFencedIpmiInvocationV1 invocation;
	uint8 bytes[PGRAC_FENCED_IPMI_ADAPTER_MAX_BYTES];
	size_t len;

	len = make_adapter(bytes, 4, VALID_PATH);
	UT_ASSERT(pgrac_fenced_ipmi_adapter_parse(bytes, len, &adapter));
	UT_ASSERT(pgrac_fenced_ipmi_invocation_build(
		&adapter, "admin", "/etc/pgrac/credentials/ipmi-00112233445566778899aabbccddeeff.password",
		PGRAC_FENCED_IPMI_COMMAND_GUID, &invocation));
	UT_ASSERT_EQ(invocation.argc, 16);
	UT_ASSERT_STR_EQ(invocation.argv[0], "ipmitool");
	UT_ASSERT_STR_EQ(invocation.argv[1], "-I");
	UT_ASSERT_STR_EQ(invocation.argv[2], "lanplus");
	UT_ASSERT_STR_EQ(invocation.argv[3], "-H");
	UT_ASSERT_STR_EQ(invocation.argv[4], "192.0.2.10");
	UT_ASSERT_STR_EQ(invocation.argv[5], "-p");
	UT_ASSERT_STR_EQ(invocation.argv[6], "623");
	UT_ASSERT_STR_EQ(invocation.argv[7], "-C");
	UT_ASSERT_STR_EQ(invocation.argv[8], "17");
	UT_ASSERT_STR_EQ(invocation.argv[9], "-U");
	UT_ASSERT_STR_EQ(invocation.argv[10], "admin");
	UT_ASSERT_STR_EQ(invocation.argv[11], "-f");
	UT_ASSERT_STR_EQ(invocation.argv[12],
					 "/etc/pgrac/credentials/ipmi-00112233445566778899aabbccddeeff.password");
	UT_ASSERT_STR_EQ(invocation.argv[13], "raw");
	UT_ASSERT_STR_EQ(invocation.argv[14], "0x06");
	UT_ASSERT_STR_EQ(invocation.argv[15], "0x37");
	UT_ASSERT_NULL(invocation.argv[16]);

	UT_ASSERT(pgrac_fenced_ipmi_invocation_build(
		&adapter, "admin", "/etc/pgrac/credentials/ipmi-00112233445566778899aabbccddeeff.password",
		PGRAC_FENCED_IPMI_COMMAND_OFF, &invocation));
	UT_ASSERT_STR_EQ(invocation.argv[13], "chassis");
	UT_ASSERT_STR_EQ(invocation.argv[14], "power");
	UT_ASSERT_STR_EQ(invocation.argv[15], "off");
	UT_ASSERT(pgrac_fenced_ipmi_invocation_build(
		&adapter, "admin", "/etc/pgrac/credentials/ipmi-00112233445566778899aabbccddeeff.password",
		PGRAC_FENCED_IPMI_COMMAND_STATUS, &invocation));
	UT_ASSERT_STR_EQ(invocation.argv[15], "status");
	UT_ASSERT(pgrac_fenced_ipmi_invocation_build(
		&adapter, "admin", "/etc/pgrac/credentials/ipmi-00112233445566778899aabbccddeeff.password",
		PGRAC_FENCED_IPMI_COMMAND_ON, &invocation));
	UT_ASSERT_STR_EQ(invocation.argv[15], "on");

	len = make_adapter(bytes, 6, VALID_PATH);
	UT_ASSERT(pgrac_fenced_ipmi_adapter_parse(bytes, len, &adapter));
	UT_ASSERT(pgrac_fenced_ipmi_invocation_build(
		&adapter, "admin", "/etc/pgrac/credentials/ipmi-00112233445566778899aabbccddeeff.password",
		PGRAC_FENCED_IPMI_COMMAND_STATUS, &invocation));
	UT_ASSERT_STR_EQ(invocation.argv[4], "2001::1");
}

UT_TEST(test_guid_result_parser_is_byte_exact)
{
	static const uint8 valid[] = " 00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f\n";
	uint8 output[sizeof(valid)];
	uint8 uuid[16];
	size_t i;

	UT_ASSERT_EQ(sizeof(valid) - 1, 49);
	UT_ASSERT(pgrac_fenced_ipmi_guid_result_parse(0, valid, sizeof(valid) - 1, NULL, 0, uuid));
	for (i = 0; i < sizeof(uuid); i++)
		UT_ASSERT_EQ(uuid[i], i);
	memcpy(output, valid, sizeof(valid));
	output[31] = 'A';
	UT_ASSERT(!pgrac_fenced_ipmi_guid_result_parse(0, output, sizeof(valid) - 1, NULL, 0, uuid));
	UT_ASSERT(!pgrac_fenced_ipmi_guid_result_parse(1, valid, sizeof(valid) - 1, NULL, 0, uuid));
	UT_ASSERT(!pgrac_fenced_ipmi_guid_result_parse(0, valid, sizeof(valid) - 2, NULL, 0, uuid));
	UT_ASSERT(!pgrac_fenced_ipmi_guid_result_parse(0, valid, sizeof(valid) - 1, (const uint8 *)"x",
												   1, uuid));
}

UT_TEST(test_power_and_action_results_are_byte_exact)
{
	static const uint8 on[] = "Chassis Power is on\n";
	static const uint8 off[] = "Chassis Power is off\n";
	uint8 action_output[PGRAC_FENCED_IPMI_ACTION_OUTPUT_MAX + 1];
	PgracFencedTargetState state = PGRAC_FENCED_TARGET_UNKNOWN;

	memset(action_output, 'x', sizeof(action_output));
	UT_ASSERT(pgrac_fenced_ipmi_power_result_parse(0, on, sizeof(on) - 1, NULL, 0, &state));
	UT_ASSERT_EQ(state, PGRAC_FENCED_TARGET_ON);
	UT_ASSERT(pgrac_fenced_ipmi_power_result_parse(0, off, sizeof(off) - 1, NULL, 0, &state));
	UT_ASSERT_EQ(state, PGRAC_FENCED_TARGET_OFF);
	UT_ASSERT(!pgrac_fenced_ipmi_power_result_parse(0, on, sizeof(on), NULL, 0, &state));
	UT_ASSERT(!pgrac_fenced_ipmi_power_result_parse(0, (const uint8 *)"Chassis Power is unknown\n",
													25, NULL, 0, &state));
	UT_ASSERT(!pgrac_fenced_ipmi_power_result_parse(0, off, sizeof(off) - 1,
													(const uint8 *)"warning\n", 8, &state));
	UT_ASSERT(pgrac_fenced_ipmi_action_result_validate(
		0, action_output, PGRAC_FENCED_IPMI_ACTION_OUTPUT_MAX, NULL, 0));
	UT_ASSERT(!pgrac_fenced_ipmi_action_result_validate(
		0, action_output, PGRAC_FENCED_IPMI_ACTION_OUTPUT_MAX + 1, NULL, 0));
	UT_ASSERT(!pgrac_fenced_ipmi_action_result_validate(1, NULL, 0, NULL, 0));
	UT_ASSERT(!pgrac_fenced_ipmi_action_result_validate(0, NULL, 0, (const uint8 *)"warning\n", 8));
}

UT_TEST(test_uncertified_readback_never_claims_io_drained)
{
	PgracFencedReadbackV1 readback;
	uint8 uuid[16];

	memset(uuid, 0x42, sizeof(uuid));
	UT_ASSERT(pgrac_fenced_ipmi_uncertified_readback(uuid, PGRAC_FENCED_TARGET_OFF, &readback));
	UT_ASSERT_EQ(readback.state, PGRAC_FENCED_TARGET_OFF);
	UT_ASSERT_EQ(readback.io_drain_state, PGRAC_FENCED_IO_DRAIN_UNKNOWN);
	UT_ASSERT(memcmp(readback.observed_target_uuid, uuid, sizeof(uuid)) == 0);
	UT_ASSERT(pgrac_fenced_ipmi_uncertified_readback(uuid, PGRAC_FENCED_TARGET_ON, &readback));
	UT_ASSERT_EQ(readback.state, PGRAC_FENCED_TARGET_ON);
	UT_ASSERT_EQ(readback.io_drain_state, PGRAC_FENCED_IO_DRAIN_UNKNOWN);
	UT_ASSERT(
		!pgrac_fenced_ipmi_uncertified_readback(uuid, PGRAC_FENCED_TARGET_UNKNOWN, &readback));
}

int
main(void)
{
	UT_PLAN(15);
	UT_RUN(test_adapter_accepts_exact_ipv4_and_ipv6_layouts);
	UT_RUN(test_adapter_rejects_header_and_length_errors);
	UT_RUN(test_adapter_rejects_address_and_option_errors);
	UT_RUN(test_adapter_rejects_noncanonical_paths);
	UT_RUN(test_adapter_accepts_maximum_canonical_path);
	UT_RUN(test_executable_and_credential_stat_rules_are_exact);
	UT_RUN(test_credential_grammars_are_exact);
	UT_RUN(test_credential_paths_use_raw_uuid_lowercase_hex);
	UT_RUN(test_executable_open_requires_fresh_matching_hash);
	UT_RUN(test_executable_open_rejects_final_symlink);
	UT_RUN(test_credentials_loader_fails_closed_when_files_are_absent);
	UT_RUN(test_invocation_uses_only_fixed_numeric_argv);
	UT_RUN(test_guid_result_parser_is_byte_exact);
	UT_RUN(test_power_and_action_results_are_byte_exact);
	UT_RUN(test_uncertified_readback_never_claims_io_drained);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
