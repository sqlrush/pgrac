/*-------------------------------------------------------------------------
 * PGRAC: production local bootstrap binding codec and filesystem tests.
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/pgrac_control_binding.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

/* Hand-laid-out bytes. CRC independently calculated with reflected 0x82f63b78. */
static const uint8 golden[256] = {
	0x50, 0x47, 0x43, 0x42, 0x02, 0x00, 0x00, 0x01, 0x04, 0x03, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00,
	0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
	0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22,
	0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x28, 0x27, 0x26, 0x25, 0x24, 0x23, 0x22, 0x21,
	0x7f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66,
	0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66,
	0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77,
	0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77,
	0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x38, 0x37, 0x36, 0x35, 0x34, 0x33, 0x32, 0x31,
	0x48, 0x47, 0x46, 0x45, 0x44, 0x43, 0x42, 0x41, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0f, 0xb9, 0x21, 0x1e, 0x00, 0x00, 0x00, 0x00,
};

static bool
all_zero(const void *ptr, size_t length)
{
	const uint8 *bytes = ptr;

	for (size_t i = 0; i < length; i++)
		if (bytes[i] != 0)
			return false;
	return true;
}

static void
fixture(PgracControlBinding *binding)
{
	memset(binding, 0, sizeof(*binding));
	binding->system_identifier = UINT64CONST(0x0102030405060708);
	memset(binding->storage_uuid, 0x11, 16);
	memset(binding->authority_uuid, 0x22, 16);
	binding->database_incarnation = UINT64CONST(0x2122232425262728);
	binding->node_id = 127;
	memset(binding->migration_round_sha256, 0x66, 32);
	memset(binding->source_wal_state_sha256, 0x77, 32);
	binding->migration_prepare_generation = UINT64CONST(0x3132333435363738);
	binding->migration_transition_epoch = UINT64CONST(0x4142434445464748);
}

static void
repair_crc(uint8 bytes[256])
{
	uint32 crc = UINT32_C(0xffffffff);

	for (unsigned int i = 0; i < 248; i++) {
		crc ^= bytes[i];
		for (unsigned int j = 0; j < 8; j++)
			crc = (crc >> 1) ^ ((crc & 1) ? UINT32_C(0x82f63b78) : 0);
	}
	crc ^= UINT32_C(0xffffffff);
	for (unsigned int i = 0; i < 4; i++)
		bytes[248 + i] = (uint8)(crc >> (8 * i));
}

static bool
decode_refuses(const uint8 *bytes, size_t length)
{
	struct {
		PgracControlBinding binding;
		uint64 sentinel;
	} result;

	memset(&result, 0xa5, sizeof(result));
	result.sentinel = UINT64CONST(0x1122334455667788);
	return !pgrac_control_binding_decode(bytes, length, &result.binding)
		   && all_zero(&result.binding, sizeof(result.binding))
		   && result.sentinel == UINT64CONST(0x1122334455667788);
}

UT_TEST(test_golden_round_trip_and_node_endpoints)
{
	PgracControlBinding expected;
	PgracControlBinding decoded;
	uint8 bytes[256];
	uint8 crc_check[256];

	fixture(&expected);
	memcpy(crc_check, golden, sizeof(crc_check));
	repair_crc(crc_check);
	UT_ASSERT(memcmp(crc_check, golden, sizeof(golden)) == 0);
	UT_ASSERT(pgrac_control_binding_encode(&expected, bytes, sizeof(bytes)));
	UT_ASSERT(memcmp(bytes, golden, sizeof(golden)) == 0);
	UT_ASSERT(pgrac_control_binding_decode(golden, sizeof(golden), &decoded));
	UT_ASSERT(memcmp(&decoded, &expected, sizeof(expected)) == 0);
	expected.node_id = 0;
	UT_ASSERT(pgrac_control_binding_encode(&expected, bytes, sizeof(bytes)));
	UT_ASSERT(bytes[64] == 0 && bytes[65] == 0 && bytes[66] == 0 && bytes[67] == 0);
	UT_ASSERT(pgrac_control_binding_decode(bytes, sizeof(bytes), &decoded));
	UT_ASSERT(memcmp(&decoded, &expected, sizeof(expected)) == 0);
}

static void
creation_literal(uint8 bytes[256])
{
	memcpy(bytes, golden, 256);
	bytes[4] = 3;
	bytes[12] = 1;
	memset(bytes + 216, 0, 16);
	bytes[216] = 1;
	repair_crc(bytes);
}

UT_TEST(test_creation_literal_is_distinct_from_open_lineage)
{
	PgracControlBinding expected, decoded;
	uint8 literal[256], encoded[256];

	fixture(&expected);
	expected.lineage_kind = PGRAC_CONTROL_LINEAGE_CREATION_V1;
	expected.migration_prepare_generation = 1;
	expected.migration_transition_epoch = 0;
	creation_literal(literal);
	UT_ASSERT(pgrac_control_binding_decode(literal, sizeof(literal), &decoded));
	UT_ASSERT(memcmp(&expected, &decoded, sizeof(expected)) == 0);
	UT_ASSERT(pgrac_control_binding_encode(&expected, encoded, sizeof(encoded)));
	UT_ASSERT(memcmp(literal, encoded, sizeof(literal)) == 0);
}

UT_TEST(test_creation_rejects_version_tag_and_origin_corruption)
{
	static const struct { size_t offset; uint8 value; } invalid[] = {
		{4, 2}, {4, 4}, {12, 0}, {12, 2}, {13, 1},
		{216, 0}, {216, 2}, {217, 1}, {224, 1}, {231, 1},
		{72, 1}, {151, 1}, {232, 1}, {252, 1}
	};
	uint8 bytes[256];

	for (size_t i = 0; i < lengthof(invalid); i++) {
		creation_literal(bytes);
		bytes[invalid[i].offset] = invalid[i].value;
		repair_crc(bytes);
		UT_ASSERT(decode_refuses(bytes, sizeof(bytes)));
	}
	for (size_t i = 0; i < 256; i++) {
		creation_literal(bytes);
		bytes[i] ^= 1;
		UT_ASSERT(decode_refuses(bytes, sizeof(bytes)));
	}
	for (size_t i = 0; i < 2; i++) {
		creation_literal(bytes);
		memset(bytes + 152 + 32 * i, 0, 32);
		repair_crc(bytes);
		UT_ASSERT(decode_refuses(bytes, sizeof(bytes)));
	}
}

UT_TEST(test_creation_encoder_refuses_fabricated_epoch_or_generation)
{
	PgracControlBinding binding;
	uint8 bytes[256];

	for (unsigned i = 0; i < 5; i++) {
		fixture(&binding);
		binding.lineage_kind = PGRAC_CONTROL_LINEAGE_CREATION_V1;
		binding.migration_prepare_generation = 1;
		binding.migration_transition_epoch = 0;
		if (i == 0) binding.migration_prepare_generation = 0;
		if (i == 1) binding.migration_prepare_generation = 2;
		if (i == 2) binding.migration_transition_epoch = 1;
		if (i == 3) binding.lineage_kind = 2;
		if (i == 4) binding.lineage_kind = PGRAC_CONTROL_LINEAGE_MIGRATION_V1;
		memset(bytes, 0xa5, sizeof(bytes));
		UT_ASSERT(!pgrac_control_binding_encode(&binding, bytes, sizeof(bytes)));
		UT_ASSERT(all_zero(bytes, sizeof(bytes)));
	}
}

UT_TEST(test_exact_sizes_nulls_and_bounded_output)
{
	PgracControlBinding binding;
	uint8 bytes[257];

	fixture(&binding);
	UT_ASSERT(decode_refuses(NULL, 256));
	UT_ASSERT(decode_refuses(golden, 0));
	UT_ASSERT(decode_refuses(golden, 255));
	UT_ASSERT(decode_refuses(golden, 257));
	UT_ASSERT(!pgrac_control_binding_decode(golden, 256, NULL));
	UT_ASSERT(!pgrac_control_binding_encode(&binding, NULL, 256));
	memset(bytes, 0xa5, sizeof(bytes));
	UT_ASSERT(!pgrac_control_binding_encode(NULL, bytes, 256));
	UT_ASSERT(all_zero(bytes, 256) && bytes[256] == 0xa5);
	memset(bytes, 0xa5, sizeof(bytes));
	UT_ASSERT(!pgrac_control_binding_encode(&binding, bytes, 255));
	UT_ASSERT(all_zero(bytes, 255) && bytes[255] == 0xa5 && bytes[256] == 0xa5);
	memset(bytes, 0xa5, sizeof(bytes));
	UT_ASSERT(!pgrac_control_binding_encode(&binding, bytes, 257));
	UT_ASSERT(all_zero(bytes, 256) && bytes[256] == 0xa5);
}

UT_TEST(test_fresh_binding_rejects_cold_import)
{
	uint8 bytes[256];
	PgracControlBinding decoded;

	memcpy(bytes, golden, sizeof(bytes));
	bytes[4] = 2;
	memset(bytes + 72, 0, 80);
	repair_crc(bytes);
	UT_ASSERT(pgrac_control_binding_decode(bytes, sizeof(bytes), &decoded));
	/* A CRC-valid old import must not acquire new-database authority. */
	bytes[4] = 1;
	memset(bytes + 72, 0x33, 16);
	memset(bytes + 88, 0x44, 32);
	memset(bytes + 120, 0x55, 32);
	repair_crc(bytes);
	UT_ASSERT(decode_refuses(bytes, sizeof(bytes)));
}

UT_TEST(test_every_byte_is_checked)
{
	uint8 bytes[256];

	for (size_t i = 0; i < sizeof(bytes); i++) {
		memcpy(bytes, golden, sizeof(bytes));
		bytes[i] ^= 1;
		UT_ASSERT(decode_refuses(bytes, sizeof(bytes)));
	}
}

UT_TEST(test_crc_valid_invalid_fields)
{
	static const struct {
		size_t offset;
		size_t length;
	} required[] = { { 16, 8 },	  { 24, 16 },  { 40, 16 }, { 56, 8 },
					 { 152, 32 }, { 184, 32 }, { 216, 8 }, { 224, 8 } };
	static const size_t header[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };
	static const struct {
		size_t offset;
		size_t length;
	} reserved[] = { { 12, 4 }, { 68, 4 }, { 72, 80 }, { 232, 16 }, { 252, 4 } };
	uint8 bytes[256];

	for (size_t i = 0; i < lengthof(required); i++) {
		memcpy(bytes, golden, sizeof(bytes));
		memset(bytes + required[i].offset, 0, required[i].length);
		repair_crc(bytes);
		UT_ASSERT(decode_refuses(bytes, sizeof(bytes)));
	}
	for (size_t i = 0; i < lengthof(header); i++) {
		memcpy(bytes, golden, sizeof(bytes));
		bytes[header[i]] ^= 1;
		repair_crc(bytes);
		UT_ASSERT(decode_refuses(bytes, sizeof(bytes)));
	}
	for (size_t i = 0; i < lengthof(reserved); i++)
		for (size_t j = 0; j < reserved[i].length; j++) {
			memcpy(bytes, golden, sizeof(bytes));
			bytes[reserved[i].offset + j] = 1;
			repair_crc(bytes);
			UT_ASSERT(decode_refuses(bytes, sizeof(bytes)));
		}
	memcpy(bytes, golden, sizeof(bytes));
	bytes[64] = 128;
	repair_crc(bytes);
	UT_ASSERT(decode_refuses(bytes, sizeof(bytes)));
	bytes[64] = 0;
	bytes[67] = 1;
	repair_crc(bytes);
	UT_ASSERT(decode_refuses(bytes, sizeof(bytes)));
}

UT_TEST(test_encoder_refuses_incomplete_identity)
{
#define FIELD(name) { offsetof(PgracControlBinding, name), sizeof(binding.name) }
	PgracControlBinding binding;
	static const struct {
		size_t offset;
		size_t length;
	} required[] = { FIELD(system_identifier),
					 FIELD(storage_uuid),
					 FIELD(authority_uuid),
					 FIELD(database_incarnation),
					 FIELD(migration_round_sha256),
					 FIELD(source_wal_state_sha256),
					 FIELD(migration_prepare_generation),
					 FIELD(migration_transition_epoch) };
	uint8 bytes[256];

	for (size_t i = 0; i < lengthof(required) + 2; i++) {
		fixture(&binding);
		if (i < lengthof(required))
			memset((uint8 *)&binding + required[i].offset, 0, required[i].length);
		else if (i == lengthof(required))
			binding.node_id = 128;
		else
			binding.lineage_kind = 1;
		memset(bytes, 0xa5, sizeof(bytes));
		UT_ASSERT(!pgrac_control_binding_encode(&binding, bytes, sizeof(bytes)));
		UT_ASSERT(all_zero(bytes, sizeof(bytes)));
	}
#undef FIELD
}

UT_TEST(test_overlapping_output_is_refused)
{
	union {
		PgracControlBinding binding;
		uint8 bytes[512];
	} shared;

	fixture(&shared.binding);
	UT_ASSERT(!pgrac_control_binding_encode(&shared.binding, shared.bytes, 256));
	UT_ASSERT(all_zero(shared.bytes, 256));
	fixture(&shared.binding);
	UT_ASSERT(!pgrac_control_binding_encode(&shared.binding, shared.bytes + 8, 256));
	UT_ASSERT(all_zero(shared.bytes + 8, 256));
	memcpy(shared.bytes, golden, sizeof(golden));
	UT_ASSERT(!pgrac_control_binding_decode(shared.bytes, 256, &shared.binding));
	UT_ASSERT(all_zero(&shared.binding, sizeof(shared.binding)));
}

/* All writes below are fixture creation in a new private temporary directory. */
static char pgdata[MAXPGPATH];
static char global_path[MAXPGPATH];
static char binding_path[MAXPGPATH];

static void
fixture_write(const void *bytes, size_t length)
{
	int fd = open(binding_path, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY, 0600);
	size_t written = 0;

	if (fd < 0) {
		perror("fixture open");
		exit(2);
	}
	while (written < length) {
		ssize_t n = write(fd, (const char *)bytes + written, length - written);

		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0) {
			perror("fixture write");
			exit(2);
		}
		written += n;
	}
	if (close(fd) != 0) {
		perror("fixture close");
		exit(2);
	}
}

static bool
read_refuses(const char *path, PgracControlBindingResult want)
{
	PgracControlBinding result;

	memset(&result, 0xa5, sizeof(result));
	return pgrac_control_binding_read(path, &result) == want && all_zero(&result, sizeof(result));
}

UT_TEST(test_read_missing_and_valid_exact_file)
{
	PgracControlBinding expected;
	PgracControlBinding actual;

	UT_ASSERT(read_refuses(pgdata, PGRAC_CONTROL_BINDING_MISSING));
	fixture_write(golden, sizeof(golden));
	fixture(&expected);
	UT_ASSERT(pgrac_control_binding_read(pgdata, &actual) == PGRAC_CONTROL_BINDING_OK);
	UT_ASSERT(memcmp(&actual, &expected, sizeof(expected)) == 0);
	UT_ASSERT(pgrac_control_binding_read(pgdata, NULL) == PGRAC_CONTROL_BINDING_INVALID);
	UT_ASSERT(unlink(binding_path) == 0);
}

UT_TEST(test_read_short_extra_corrupt_and_no_backup_fallback)
{
	uint8 bytes[257];
	char backup[MAXPGPATH];

	memcpy(bytes, golden, 256);
	bytes[256] = 0;
	for (size_t size = 255; size <= 257; size += 2) {
		fixture_write(bytes, size);
		UT_ASSERT(read_refuses(pgdata, PGRAC_CONTROL_BINDING_INVALID));
	}
	fixture_write(golden, sizeof(golden));
	snprintf(backup, sizeof(backup), "%s.bak", binding_path);
	UT_ASSERT(rename(binding_path, backup) == 0);
	UT_ASSERT(read_refuses(pgdata, PGRAC_CONTROL_BINDING_MISSING));
	bytes[20] ^= 1;
	fixture_write(bytes, 256);
	UT_ASSERT(read_refuses(pgdata, PGRAC_CONTROL_BINDING_INVALID));
	UT_ASSERT(unlink(binding_path) == 0);
	UT_ASSERT(unlink(backup) == 0);
}

UT_TEST(test_read_refuses_symlink_at_each_open_boundary)
{
	char saved[MAXPGPATH];
	char alias[MAXPGPATH];

	snprintf(saved, sizeof(saved), "%s/real_binding", global_path);
	fixture_write(golden, sizeof(golden));
	UT_ASSERT(rename(binding_path, saved) == 0);
	UT_ASSERT(symlink(saved, binding_path) == 0);
	UT_ASSERT(read_refuses(pgdata, PGRAC_CONTROL_BINDING_UNSAFE));
	UT_ASSERT(unlink(binding_path) == 0);
	UT_ASSERT(rename(saved, binding_path) == 0);

	snprintf(saved, sizeof(saved), "%s/real_global", pgdata);
	UT_ASSERT(rename(global_path, saved) == 0);
	UT_ASSERT(symlink(saved, global_path) == 0);
	UT_ASSERT(read_refuses(pgdata, PGRAC_CONTROL_BINDING_UNSAFE));
	UT_ASSERT(unlink(global_path) == 0);
	UT_ASSERT(rename(saved, global_path) == 0);

	snprintf(alias, sizeof(alias), "%s-link", pgdata);
	UT_ASSERT(symlink(pgdata, alias) == 0);
	UT_ASSERT(read_refuses(alias, PGRAC_CONTROL_BINDING_UNSAFE));
	UT_ASSERT(unlink(alias) == 0);
	UT_ASSERT(unlink(binding_path) == 0);
}

UT_TEST(test_read_permissions_and_nonregular_never_block)
{
	PgracControlBinding result;

	fixture_write(golden, sizeof(golden));
	UT_ASSERT(chmod(binding_path, 0660) == 0);
	UT_ASSERT(read_refuses(pgdata, PGRAC_CONTROL_BINDING_UNSAFE));
	UT_ASSERT(chmod(binding_path, 0600) == 0);
	UT_ASSERT(chmod(global_path, 0770) == 0);
	UT_ASSERT(read_refuses(pgdata, PGRAC_CONTROL_BINDING_UNSAFE));
	UT_ASSERT(chmod(global_path, 0700) == 0);
	UT_ASSERT(chmod(pgdata, 0770) == 0);
	UT_ASSERT(read_refuses(pgdata, PGRAC_CONTROL_BINDING_UNSAFE));
	UT_ASSERT(chmod(pgdata, 0700) == 0);
	/* Restrict writes, not harmless group read permission. */
	UT_ASSERT(chmod(global_path, 0750) == 0 && chmod(binding_path, 0640) == 0);
	UT_ASSERT(pgrac_control_binding_read(pgdata, &result) == PGRAC_CONTROL_BINDING_OK);
	UT_ASSERT(chmod(global_path, 0700) == 0);
	UT_ASSERT(unlink(binding_path) == 0);
	UT_ASSERT(mkdir(binding_path, 0700) == 0);
	UT_ASSERT(read_refuses(pgdata, PGRAC_CONTROL_BINDING_UNSAFE));
	UT_ASSERT(rmdir(binding_path) == 0);
	UT_ASSERT(mkfifo(binding_path, 0600) == 0);
	alarm(3);
	UT_ASSERT(read_refuses(pgdata, PGRAC_CONTROL_BINDING_UNSAFE));
	alarm(0);
	UT_ASSERT(unlink(binding_path) == 0);
}

UT_TEST(test_read_rejects_noncanonical_pgdata_and_clears_output)
{
	char path[MAXPGPATH];

	fixture_write(golden, sizeof(golden));
	UT_ASSERT(read_refuses(NULL, PGRAC_CONTROL_BINDING_INVALID));
	UT_ASSERT(read_refuses("", PGRAC_CONTROL_BINDING_INVALID));
	UT_ASSERT(read_refuses(".", PGRAC_CONTROL_BINDING_INVALID));
	UT_ASSERT(read_refuses("relative", PGRAC_CONTROL_BINDING_INVALID));
	snprintf(path, sizeof(path), "%s/", pgdata);
	UT_ASSERT(read_refuses(path, PGRAC_CONTROL_BINDING_INVALID));
	snprintf(path, sizeof(path), "%s/.", pgdata);
	UT_ASSERT(read_refuses(path, PGRAC_CONTROL_BINDING_INVALID));
	snprintf(path, sizeof(path), "/tmp/../%s", pgdata + 1);
	UT_ASSERT(read_refuses(path, PGRAC_CONTROL_BINDING_INVALID));
	snprintf(path, sizeof(path), "/%s", pgdata);
	UT_ASSERT(read_refuses(path, PGRAC_CONTROL_BINDING_INVALID));
	memset(path, 'x', sizeof(path));
	path[0] = '/';
	UT_ASSERT(read_refuses(path, PGRAC_CONTROL_BINDING_INVALID));
	UT_ASSERT(unlink(binding_path) == 0);
}

int
main(void)
{
	strlcpy(pgdata, "/tmp/pgrac-control-binding-XXXXXX", sizeof(pgdata));
	if (mkdtemp(pgdata) == NULL) {
		perror("mkdtemp");
		return 2;
	}
	snprintf(global_path, sizeof(global_path), "%s/global", pgdata);
	snprintf(binding_path, sizeof(binding_path), "%s/%s", global_path, PGRAC_CONTROL_BINDING_NAME);
	if (mkdir(global_path, 0700) != 0) {
		perror("mkdir global");
		return 2;
	}
	UT_PLAN(15);
	UT_RUN(test_fresh_binding_rejects_cold_import);
	UT_RUN(test_golden_round_trip_and_node_endpoints);
	UT_RUN(test_creation_literal_is_distinct_from_open_lineage);
	UT_RUN(test_creation_rejects_version_tag_and_origin_corruption);
	UT_RUN(test_creation_encoder_refuses_fabricated_epoch_or_generation);
	UT_RUN(test_exact_sizes_nulls_and_bounded_output);
	UT_RUN(test_every_byte_is_checked);
	UT_RUN(test_crc_valid_invalid_fields);
	UT_RUN(test_encoder_refuses_incomplete_identity);
	UT_RUN(test_overlapping_output_is_refused);
	UT_RUN(test_read_missing_and_valid_exact_file);
	UT_RUN(test_read_short_extra_corrupt_and_no_backup_fallback);
	UT_RUN(test_read_refuses_symlink_at_each_open_boundary);
	UT_RUN(test_read_permissions_and_nonregular_never_block);
	UT_RUN(test_read_rejects_noncanonical_pgdata_and_clears_output);
	if (rmdir(global_path) != 0 || rmdir(pgdata) != 0) {
		perror("fixture cleanup");
		return 2;
	}
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
