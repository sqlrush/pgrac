/*-------------------------------------------------------------------------
 * PGRAC: real shared configuration representation and file-selection tests.
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_shared_config.h"
#include "common/cryptohash.h"

#undef printf
#undef snprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static bool held = true;
static LOCKMODE held_mode = ShareLock;
static unsigned allocations;

bool
cluster_cf_held_is_clusterwide(LOCKMODE mode)
{
	return held && mode == held_mode;
}
void *
palloc(Size size)
{
	void *ptr = malloc(size);
	if (!ptr)
		abort();
	++allocations;
	return ptr;
}
void
pfree(void *ptr)
{
	if (!ptr || !allocations)
		abort();
	--allocations;
	free(ptr);
}
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# Assert %s %s:%d\n", condition, file, line);
	abort();
}

static const char header[] = "@authority_uuid=22222222222222222222222222222222\n"
							 "@configured_0=0000000000000001\n"
							 "@configured_1=8000000000000000\n"
							 "@database_incarnation=41\n"
							 "@format=1\n"
							 "@generation=47\n"
							 "@storage_uuid=11111111111111111111111111111111\n"
							 "@system_identifier=123456789\n";
static const char body[] = "common.cluster.enabled='on'\n"
						   "common.test.label='a''b\\中文'\n"
						   "node000.port='5432'\n"
						   "node127.port='5433'\n";

static void
digest(const char *bytes, size_t len, uint8 hash[32])
{
	pg_cryptohash_ctx *ctx = pg_cryptohash_create(PG_SHA256);
	if (!ctx || pg_cryptohash_init(ctx) < 0
		|| pg_cryptohash_update(ctx, (const uint8 *)bytes, len) < 0
		|| pg_cryptohash_final(ctx, hash, 32) < 0)
		abort();
	pg_cryptohash_free(ctx);
}
static void
fixture(char *bytes, size_t cap, const char *suffix, ClusterSharedConfigRef *ref)
{
	memset(ref, 0, sizeof(*ref));
	ref->identity.system_identifier = 123456789;
	ref->identity.database_incarnation = 41;
	ref->identity.generation = 47;
	memset(ref->identity.storage_uuid, 0x11, 16);
	memset(ref->identity.authority_uuid, 0x22, 16);
	ref->identity.configured[0] = 1;
	ref->identity.configured[1] = UINT64_C(1) << 63;
	if ((size_t)snprintf(bytes, cap, "%s%s", header, suffix) >= cap)
		abort();
	digest(bytes, strlen(bytes), ref->sha256);
}

UT_TEST(test_independent_canonical_input)
{
	char bytes[1024];
	ClusterSharedConfigRef ref;
	uint32 count = 99;
	fixture(bytes, sizeof(bytes), body, &ref);
	UT_ASSERT_EQ(cluster_shared_config_validate(bytes, strlen(bytes), &ref, &count), 0);
	UT_ASSERT_EQ(count, 4);
}

UT_TEST(test_encoder_matches_independent_bytes)
{
	char expected[1024], bytes[1024];
	size_t len = 123;
	uint8 hash[32];
	ClusterSharedConfigRef ref;
	ClusterSharedConfigEntry entries[] = { { -1, "cluster.enabled", "on" },
										   { -1, "test.label", "a'b\\中文" },
										   { 0, "port", "5432" },
										   { 127, "port", "5433" } };
	fixture(expected, sizeof(expected), body, &ref);
	UT_ASSERT_EQ(
		cluster_shared_config_encode(&ref.identity, entries, 4, bytes, sizeof(bytes), &len, hash),
		0);
	UT_ASSERT_EQ(len, strlen(expected));
	UT_ASSERT_EQ(memcmp(bytes, expected, strlen(expected) + 1), 0);
	UT_ASSERT_EQ(memcmp(hash, ref.sha256, 32), 0);
}

UT_TEST(test_exact_scope_lookup)
{
	char bytes[1024], value[128];
	ClusterSharedConfigRef ref;
	fixture(bytes, sizeof(bytes), body, &ref);
	UT_ASSERT_EQ(cluster_shared_config_lookup(bytes, strlen(bytes), &ref, -1, "test.label", value,
											  sizeof(value)),
				 0);
	UT_ASSERT_EQ(strcmp(value, "a'b\\中文"), 0);
	UT_ASSERT_EQ(
		cluster_shared_config_lookup(bytes, strlen(bytes), &ref, 127, "port", value, sizeof(value)),
		0);
	UT_ASSERT_EQ(strcmp(value, "5433"), 0);
	UT_ASSERT_EQ(cluster_shared_config_lookup(bytes, strlen(bytes), &ref, 0, "cluster.enabled",
											  value, sizeof(value)),
				 CLUSTER_CONTROL_ROOT_ABSENT);
	UT_ASSERT_EQ(value[0], 0);
	UT_ASSERT(cluster_shared_config_lookup(bytes, strlen(bytes), &ref, -1, "test.label", value, 2)
			  != 0);
	UT_ASSERT_EQ(value[0], 0);
}

UT_TEST(test_every_identity_field_and_hash)
{
	char bytes[1024];
	ClusterSharedConfigRef ref, bad;
	uint32 count;
	fixture(bytes, sizeof(bytes), body, &ref);
	for (size_t i = 0; i < sizeof(ref); ++i) {
		bad = ref;
		((uint8 *)&bad)[i] ^= 1;
		count = 99;
		UT_ASSERT(cluster_shared_config_validate(bytes, strlen(bytes), &bad, &count) != 0);
		UT_ASSERT_EQ(count, 0);
	}
}

UT_TEST(test_ambiguous_or_unsafe_text)
{
	static const char *invalid[] = { "\n",
									 "# comment\n",
									 "include='other'\n",
									 "common.a='x'",
									 "common.b='x'\ncommon.a='y'\n",
									 "common.a='x'\ncommon.a='y'\n",
									 "common.A='x'\n",
									 "common.a..b='x'\n",
									 "common.1a='x'\n",
									 "common.a='x' \n",
									 "common.a= 'x'\n",
									 "common.a=x\n",
									 "common.a='a'b'\n",
									 "common.a='a\\'b'\n",
									 "common.a='\t'\n",
									 "common.a='\177'\n",
									 "common.a='\300\257'\n",
									 "common.a='\xed\xa0\x80'\n",
									 "common.a='\xf4\x90\x80\x80'\n",
									 "common.a='x'\r\n",
									 "node001.port='5432'\n",
									 "node128.port='5432'\n",
									 "node00.port='5432'\n",
									 "node0000.port='5432'\n",
									 "node-01.port='5432'\n",
									 "@extra=1\n" };
	char bytes[2048];
	ClusterSharedConfigRef ref;
	uint32 count;
	for (size_t i = 0; i < lengthof(invalid); ++i) {
		fixture(bytes, sizeof(bytes), invalid[i], &ref);
		count = 99;
		UT_ASSERT(cluster_shared_config_validate(bytes, strlen(bytes), &ref, &count) != 0);
		UT_ASSERT_EQ(count, 0);
	}
	fixture(bytes, sizeof(bytes), body, &ref);
	bytes[strlen(header) + 5] = '\0';
	digest(bytes, strlen(header) + strlen(body), ref.sha256);
	UT_ASSERT(cluster_shared_config_validate(bytes, strlen(header) + strlen(body), &ref, &count)
			  != 0);
}

UT_TEST(test_encoder_refusal_and_boundaries)
{
	char bytes[1024], value[CLUSTER_SHARED_CONFIG_MAX_VALUE + 2];
	ClusterSharedConfigRef ref;
	ClusterSharedConfigEntry entries[] = { { -1, "a", "x" }, { -1, "a", "y" } };
	size_t len;
	uint8 hash[32], zero[32] = { 0 };
	fixture(bytes, sizeof(bytes), body, &ref);
	memset(value, 'x', sizeof(value));
	value[sizeof(value) - 1] = 0;
	for (int n = 0; n < 4; ++n) {
		entries[0].node_id = n == 1 ? 128 : -1;
		entries[0].name = n == 2 ? "A" : "a";
		entries[0].value = n == 3 ? value : "x";
		memset(bytes, 0xa5, sizeof(bytes));
		len = 99;
		memset(hash, 0xa5, 32);
		UT_ASSERT(cluster_shared_config_encode(&ref.identity, entries, n == 0 ? 2 : 1, bytes,
											   sizeof(bytes), &len, hash)
				  != 0);
		UT_ASSERT_EQ(len, 0);
		UT_ASSERT_EQ(bytes[0], 0);
		UT_ASSERT_EQ(memcmp(hash, zero, 32), 0);
	}
	UT_ASSERT(cluster_shared_config_encode(&ref.identity, NULL, 0, bytes, 1, &len, hash) != 0);
	UT_ASSERT_EQ(len, 0);
	UT_ASSERT_EQ(bytes[0], 0);
}

UT_TEST(test_empty_representation_is_not_profile_admission)
{
	char expected[1024], bytes[1024];
	ClusterSharedConfigRef ref;
	size_t len;
	uint8 hash[32];
	uint32 count = 99;
	fixture(expected, sizeof(expected), "", &ref);
	UT_ASSERT_EQ(
		cluster_shared_config_encode(&ref.identity, NULL, 0, bytes, sizeof(bytes), &len, hash), 0);
	UT_ASSERT_EQ(len, strlen(header));
	UT_ASSERT_EQ(cluster_shared_config_validate(expected, strlen(expected), &ref, &count), 0);
	UT_ASSERT_EQ(count, 0);
}

UT_TEST(test_exact_limits_and_literal_boundaries)
{
	char expected[1024], name[CLUSTER_SHARED_CONFIG_MAX_NAME + 2];
	char *bytes = malloc(CLUSTER_SHARED_CONFIG_MAX_BYTES + 1);
	char *value = malloc(CLUSTER_SHARED_CONFIG_MAX_VALUE + 2);
	char *decoded = malloc(CLUSTER_SHARED_CONFIG_MAX_VALUE + 2);
	ClusterSharedConfigRef ref;
	ClusterSharedConfigEntry entry = { 127, name, value };
	size_t len;
	uint32 count;
	if (!bytes || !value || !decoded)
		abort();
	fixture(expected, sizeof(expected), body, &ref);
	memset(name, 'a', sizeof(name));
	name[CLUSTER_SHARED_CONFIG_MAX_NAME] = 0;
	memset(value, '\'', CLUSTER_SHARED_CONFIG_MAX_VALUE);
	value[CLUSTER_SHARED_CONFIG_MAX_VALUE] = 0;
	UT_ASSERT_EQ(cluster_shared_config_encode(&ref.identity, &entry, 1, bytes,
											  CLUSTER_SHARED_CONFIG_MAX_BYTES + 1, &len,
											  ref.sha256),
				 0);
	UT_ASSERT_EQ(cluster_shared_config_validate(bytes, len, &ref, &count), 0);
	UT_ASSERT_EQ(count, 1);
	UT_ASSERT_EQ(cluster_shared_config_lookup(bytes, len, &ref, 127, name, decoded,
											  CLUSTER_SHARED_CONFIG_MAX_VALUE + 1),
				 0);
	UT_ASSERT_EQ(memcmp(value, decoded, CLUSTER_SHARED_CONFIG_MAX_VALUE + 1), 0);
	name[CLUSTER_SHARED_CONFIG_MAX_NAME] = 'a';
	name[CLUSTER_SHARED_CONFIG_MAX_NAME + 1] = 0;
	UT_ASSERT(cluster_shared_config_encode(&ref.identity, &entry, 1, bytes,
										   CLUSTER_SHARED_CONFIG_MAX_BYTES + 1, &len, ref.sha256)
			  != 0);
	entry.name = "a";
	entry.node_id = -1;
	value[0] = 0;
	UT_ASSERT_EQ(cluster_shared_config_encode(&ref.identity, &entry, 1, bytes,
											  CLUSTER_SHARED_CONFIG_MAX_BYTES + 1, &len,
											  ref.sha256),
				 0);
	UT_ASSERT_EQ(cluster_shared_config_lookup(bytes, len, &ref, -1, "a", decoded, 1), 0);
	UT_ASSERT_EQ(decoded[0], 0);
	free(bytes);
	free(value);
	free(decoded);
}

UT_TEST(test_entry_and_object_bounds)
{
	char expected[1024];
	char *bytes = malloc(CLUSTER_SHARED_CONFIG_MAX_BYTES + 2);
	ClusterSharedConfigEntry *entries = calloc(CLUSTER_SHARED_CONFIG_MAX_ENTRIES, sizeof(*entries));
	char(*names)[16] = calloc(CLUSTER_SHARED_CONFIG_MAX_ENTRIES, sizeof(*names));
	ClusterSharedConfigRef ref;
	size_t len;
	uint32 count = 99;
	if (!bytes || !entries || !names)
		abort();
	fixture(expected, sizeof(expected), body, &ref);
	for (size_t i = 0; i < CLUSTER_SHARED_CONFIG_MAX_ENTRIES; ++i) {
		snprintf(names[i], 16, "a%04zu", i);
		entries[i].node_id = -1;
		entries[i].name = names[i];
		entries[i].value = "x";
	}
	UT_ASSERT_EQ(
		cluster_shared_config_encode(&ref.identity, entries, CLUSTER_SHARED_CONFIG_MAX_ENTRIES,
									 bytes, CLUSTER_SHARED_CONFIG_MAX_BYTES + 1, &len, ref.sha256),
		0);
	UT_ASSERT_EQ(cluster_shared_config_validate(bytes, len, &ref, &count), 0);
	UT_ASSERT_EQ(count, CLUSTER_SHARED_CONFIG_MAX_ENTRIES);
	memcpy(bytes + len, "common.a8192='x'\n", sizeof("common.a8192='x'\n"));
	len += sizeof("common.a8192='x'\n") - 1;
	digest(bytes, len, ref.sha256);
	UT_ASSERT(cluster_shared_config_validate(bytes, len, &ref, &count) != 0);
	UT_ASSERT_EQ(count, 0);
	UT_ASSERT(
		cluster_shared_config_validate(bytes, CLUSTER_SHARED_CONFIG_MAX_BYTES + 1, &ref, &count)
		!= 0);
	UT_ASSERT_EQ(count, 0);
	free(bytes);
	free(entries);
	free(names);
}

static char root[MAXPGPATH], object[MAXPGPATH];
static char file_bytes[1024];
static ClusterSharedConfigRef file_ref;

static void
write_file(const char *path, const char *bytes, size_t len)
{
	int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (fd < 0 || write(fd, bytes, len) != (ssize_t)len || close(fd) != 0)
		abort();
}
static void
setup_file(void)
{
	char tmp[] = "/tmp/pgrac-shared-config-XXXXXX";
	char path[MAXPGPATH], hex[65];
	if (!mkdtemp(tmp))
		abort();
	strlcpy(root, tmp, sizeof(root));
	snprintf(path, sizeof(path), "%s/global", root);
	if (mkdir(path, 0700))
		abort();
	snprintf(path, sizeof(path), "%s/global/config_images", root);
	if (mkdir(path, 0700))
		abort();
	fixture(file_bytes, sizeof(file_bytes), body, &file_ref);
	for (int i = 0; i < 32; ++i)
		snprintf(hex + 2 * i, 3, "%02x", file_ref.sha256[i]);
	snprintf(object, sizeof(object), "%s/global/config_images/47-%s.conf", root, hex);
	write_file(object, file_bytes, strlen(file_bytes));
	held = true;
	held_mode = ShareLock;
}
static void
cleanup_file(void)
{
	char path[MAXPGPATH];
	(void)unlink(object);
	snprintf(path, sizeof(path), "%s/global/config_images", root);
	(void)rmdir(path);
	snprintf(path, sizeof(path), "%s/global", root);
	(void)rmdir(path);
	(void)rmdir(root);
}

UT_TEST(test_exact_file_and_lifetime)
{
	ClusterSharedConfigImage out;
	setup_file();
	UT_ASSERT_EQ(cluster_shared_config_read_locked(root, &file_ref, &out), 0);
	UT_ASSERT_EQ(out.len, strlen(file_bytes));
	if (out.bytes)
		UT_ASSERT_EQ(memcmp(out.bytes, file_bytes, out.len + 1), 0);
	else
		UT_ASSERT(false);
	cluster_shared_config_free(&out);
	UT_ASSERT_EQ(out.bytes, NULL);
	UT_ASSERT_EQ(out.len, 0);
	UT_ASSERT_EQ(allocations, 0);
	held_mode = ExclusiveLock;
	UT_ASSERT_EQ(cluster_shared_config_read_locked(root, &file_ref, &out), 0);
	cluster_shared_config_free(&out);
	cleanup_file();
}

UT_TEST(test_file_refusals_without_fallback)
{
	ClusterSharedConfigImage out;
	char path[MAXPGPATH];
	for (int n = 0; n < 8; ++n) {
		setup_file();
		if (n == 0)
			held = false;
		if (n == 1)
			(void)unlink(object);
		if (n == 2)
			write_file(object, "broken", 6);
		if (n == 3)
			write_file(object, file_bytes, strlen(file_bytes) - 1);
		if (n == 4)
			write_file(object, file_bytes, strlen(file_bytes) + 1);
		if (n == 5)
			(void)chmod(object, 0666);
		if (n == 6) {
			(void)unlink(object);
			if (symlink("/dev/null", object))
				abort();
		}
		if (n == 7) {
			(void)unlink(object);
			if (mkfifo(object, 0600))
				abort();
		}
		/* A correct backup never repairs a missing or invalid selected object. */
		snprintf(path, sizeof(path), "%s.bak", object);
		write_file(path, file_bytes, strlen(file_bytes));
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT(cluster_shared_config_read_locked(root, &file_ref, &out) != 0);
		UT_ASSERT_EQ(out.bytes, NULL);
		UT_ASSERT_EQ(out.len, 0);
		UT_ASSERT_EQ(allocations, 0);
		(void)unlink(path);
		cleanup_file();
	}
}

UT_TEST(test_invalid_arguments)
{
	char bytes[1024], value[128];
	ClusterSharedConfigRef ref;
	uint32 count = 99;
	ClusterSharedConfigImage out;
	fixture(bytes, sizeof(bytes), body, &ref);
	UT_ASSERT(cluster_shared_config_validate(NULL, 0, &ref, &count) != 0);
	UT_ASSERT_EQ(count, 0);
	UT_ASSERT(cluster_shared_config_validate(bytes, strlen(bytes), NULL, &count) != 0);
	UT_ASSERT(cluster_shared_config_validate(bytes, strlen(bytes), &ref, NULL) != 0);
	UT_ASSERT(
		cluster_shared_config_lookup(bytes, strlen(bytes), &ref, -2, "a", value, sizeof(value))
		!= 0);
	UT_ASSERT(cluster_shared_config_read_locked("relative", &ref, &out) != 0);
	UT_ASSERT_EQ(out.bytes, NULL);
	UT_ASSERT_EQ(out.len, 0);
	cluster_shared_config_free(NULL);
}

UT_TEST(test_directory_guards)
{
	ClusterSharedConfigImage out;
	char path[MAXPGPATH], moved[MAXPGPATH];
	for (int n = 0; n < 3; ++n) {
		setup_file();
		snprintf(path, sizeof(path), "%s%s", root,
				 n == 0	  ? ""
				 : n == 1 ? "/global"
						  : "/global/config_images");
		UT_ASSERT_EQ(chmod(path, 0777), 0);
		UT_ASSERT(cluster_shared_config_read_locked(root, &file_ref, &out) != 0);
		UT_ASSERT_EQ(out.bytes, NULL);
		UT_ASSERT_EQ(allocations, 0);
		UT_ASSERT_EQ(chmod(path, 0700), 0);
		snprintf(moved, sizeof(moved), "%s-moved", path);
		UT_ASSERT_EQ(rename(path, moved), 0);
		UT_ASSERT_EQ(symlink(moved, path), 0);
		UT_ASSERT(cluster_shared_config_read_locked(root, &file_ref, &out) != 0);
		UT_ASSERT_EQ(out.bytes, NULL);
		UT_ASSERT_EQ(allocations, 0);
		UT_ASSERT_EQ(unlink(path), 0);
		UT_ASSERT_EQ(rename(moved, path), 0);
		cleanup_file();
	}
}

int
main(void)
{
	UT_PLAN(13);
	UT_RUN(test_independent_canonical_input);
	UT_RUN(test_encoder_matches_independent_bytes);
	UT_RUN(test_exact_scope_lookup);
	UT_RUN(test_every_identity_field_and_hash);
	UT_RUN(test_ambiguous_or_unsafe_text);
	UT_RUN(test_encoder_refusal_and_boundaries);
	UT_RUN(test_empty_representation_is_not_profile_admission);
	UT_RUN(test_exact_limits_and_literal_boundaries);
	UT_RUN(test_entry_and_object_bounds);
	UT_RUN(test_exact_file_and_lifetime);
	UT_RUN(test_file_refusals_without_fallback);
	UT_RUN(test_invalid_arguments);
	UT_RUN(test_directory_guards);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
