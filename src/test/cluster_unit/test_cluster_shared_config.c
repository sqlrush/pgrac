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
#include "storage/fd.h"

#undef printf
#undef snprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static bool held = true;
static LOCKMODE held_mode = ShareLock;
static unsigned allocations;
bool enableFsync = true;
static unsigned sync_calls, fail_sync;

int
pg_fsync(int fd)
{
	if (++sync_calls == fail_sync) {
		errno = EIO;
		return -1;
	}
	return fsync(fd);
}

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

static uint8 operation[16] = { 0x55, 0x12, 0x34 };
static char staged_file[MAXPGPATH];

static void
stage_paths(const ClusterSharedConfigStage *stage, char *final, char *temp)
{
	char hex[65];
	for (int i = 0; i < 32; ++i)
		snprintf(hex + 2 * i, 3, "%02x", stage->ref.sha256[i]);
	snprintf(final, MAXPGPATH, "%s/global/config_images/" UINT64_FORMAT "-%s.conf", root,
			 stage->ref.identity.generation, hex);
	for (int i = 0; i < 16; ++i)
		snprintf(hex + 2 * i, 3, "%02x", stage->operation_uuid[i]);
	snprintf(temp, MAXPGPATH, "%s/global/config_images/.staging/%s.tmp", root, hex);
}
static void
setup_stage(void)
{
	char path[MAXPGPATH], hex[33];
	setup_file();
	snprintf(path, sizeof(path), "%s/global/config_images/.staging", root);
	if (mkdir(path, 0700))
		abort();
	for (int i = 0; i < 16; ++i)
		snprintf(hex + 2 * i, 3, "%02x", operation[i]);
	snprintf(staged_file, sizeof(staged_file), "%s/%s.tmp", path, hex);
	sync_calls = fail_sync = 0;
	enableFsync = true;
}
static void
cleanup_stage(void)
{
	char path[MAXPGPATH];
	(void)unlink(staged_file);
	snprintf(path, sizeof(path), "%s/global/config_images/.staging", root);
	(void)rmdir(path);
	cleanup_file();
	UT_ASSERT_EQ(allocations, 0);
}
static bool
prepare_stage(ClusterSharedConfigStage *stage)
{
	ClusterControlRootResult result = cluster_shared_config_prepare(
		root, file_bytes, strlen(file_bytes), &file_ref, operation, stage);
	UT_ASSERT_EQ(result, 0);
	return result == 0;
}

UT_TEST(test_prepare_install_and_cancel_preserves_formal)
{
	ClusterSharedConfigStage stage;
	ClusterSharedConfigImage image;
	struct stat st;
	setup_stage();
	(void)unlink(object);
	if (!prepare_stage(&stage)) {
		cleanup_stage();
		return;
	}
	UT_ASSERT_EQ(stage.owner_pid, getpid());
	UT_ASSERT_EQ(stage.bytes, strlen(file_bytes));
	UT_ASSERT_EQ(stat(staged_file, &st), 0);
	UT_ASSERT_EQ(st.st_size, stage.bytes);
	UT_ASSERT(access(object, F_OK) != 0);
	UT_ASSERT_EQ(cluster_shared_config_install(root, &stage),
				 CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	held_mode = ExclusiveLock;
	UT_ASSERT_EQ(cluster_shared_config_install(root, &stage), 0);
	UT_ASSERT(access(staged_file, F_OK) != 0);
	UT_ASSERT_EQ(cluster_shared_config_read_locked(root, &file_ref, &image), 0);
	cluster_shared_config_free(&image);
	UT_ASSERT_EQ(cluster_shared_config_discard(root, &stage), 0);
	UT_ASSERT_EQ(stage.state, 0);
	UT_ASSERT_EQ(access(object, F_OK), 0);
	UT_ASSERT(cluster_shared_config_install(root, &stage) != 0);
	cleanup_stage();
}

UT_TEST(test_no_clobber_and_identical_retry)
{
	ClusterSharedConfigStage stage;
	struct stat before, after;
	for (int n = 0; n < 2; ++n) {
		setup_stage();
		if (n == 1)
			write_file(object, "foreign", 7);
		UT_ASSERT_EQ(stat(object, &before), 0);
		if (!prepare_stage(&stage)) {
			cleanup_stage();
			continue;
		}
		held_mode = ExclusiveLock;
		if (n == 0)
			UT_ASSERT_EQ(cluster_shared_config_install(root, &stage), 0);
		else
			UT_ASSERT(cluster_shared_config_install(root, &stage) != 0);
		UT_ASSERT_EQ(stat(object, &after), 0);
		UT_ASSERT_EQ(before.st_ino, after.st_ino);
		UT_ASSERT_EQ(before.st_size, after.st_size);
		UT_ASSERT_EQ(cluster_shared_config_discard(root, &stage), 0);
		cleanup_stage();
	}
}

UT_TEST(test_same_generation_different_hash_objects)
{
	ClusterSharedConfigStage first, second;
	ClusterSharedConfigRef other;
	ClusterSharedConfigImage image;
	uint8 uuid[16] = { 0x56 };
	char bytes[1024], final[MAXPGPATH], temp[MAXPGPATH];
	setup_stage();
	if (!prepare_stage(&first)) {
		cleanup_stage();
		return;
	}
	fixture(bytes, sizeof(bytes), "common.cluster.enabled='off'\n", &other);
	UT_ASSERT_EQ(cluster_shared_config_prepare(root, bytes, strlen(bytes), &other, uuid, &second),
				 0);
	if (!second.state) {
		cluster_shared_config_discard(root, &first);
		cleanup_stage();
		return;
	}
	held_mode = ExclusiveLock;
	UT_ASSERT_EQ(cluster_shared_config_install(root, &first), 0);
	UT_ASSERT_EQ(cluster_shared_config_install(root, &second), 0);
	UT_ASSERT(memcmp(first.ref.sha256, second.ref.sha256, 32) != 0);
	UT_ASSERT_EQ(cluster_shared_config_read_locked(root, &other, &image), 0);
	cluster_shared_config_free(&image);
	stage_paths(&second, final, temp);
	UT_ASSERT_EQ(cluster_shared_config_discard(root, &first), 0);
	UT_ASSERT_EQ(cluster_shared_config_discard(root, &second), 0);
	(void)unlink(final);
	(void)unlink(temp);
	cleanup_stage();
}

UT_TEST(test_owner_and_file_identity_cannot_be_substituted)
{
	ClusterSharedConfigStage stage, bad;
	char saved[MAXPGPATH];
	setup_stage();
	if (!prepare_stage(&stage)) {
		cleanup_stage();
		return;
	}
	held_mode = ExclusiveLock;
	for (int n = 0; n < 7; ++n) {
		bad = stage;
		if (n == 0)
			++bad.owner_pid;
		if (n == 1)
			++bad.file_ino;
		if (n == 2)
			++bad.object_dir_ino;
		if (n == 3)
			++bad.staging_dir_ino;
		if (n == 4)
			++bad.ref.identity.system_identifier;
		if (n == 5) {
			file_bytes[0] ^= 1;
			write_file(staged_file, file_bytes, strlen(file_bytes));
		}
		if (n == 6)
			UT_ASSERT_EQ(chmod(staged_file, 0666), 0);
		UT_ASSERT(cluster_shared_config_install(root, &bad) != 0);
		if (n == 5) {
			file_bytes[0] ^= 1;
			write_file(staged_file, file_bytes, strlen(file_bytes));
		}
		if (n == 6)
			UT_ASSERT_EQ(chmod(staged_file, 0600), 0);
	}
	snprintf(saved, sizeof(saved), "%s.saved", staged_file);
	UT_ASSERT_EQ(rename(staged_file, saved), 0);
	write_file(staged_file, file_bytes, strlen(file_bytes));
	UT_ASSERT(cluster_shared_config_install(root, &stage) != 0);
	UT_ASSERT(cluster_shared_config_discard(root, &stage) != 0);
	UT_ASSERT_EQ(access(staged_file, F_OK), 0);
	(void)unlink(staged_file);
	UT_ASSERT_EQ(rename(saved, staged_file), 0);
	UT_ASSERT_EQ(cluster_shared_config_discard(root, &stage), 0);
	cleanup_stage();
}

UT_TEST(test_prepare_failure_and_uuid_collision)
{
	ClusterSharedConfigStage stage, second;
	for (unsigned n = 1; n <= 2; ++n) {
		setup_stage();
		fail_sync = n;
		memset(&stage, 0xa5, sizeof(stage));
		UT_ASSERT(cluster_shared_config_prepare(root, file_bytes, strlen(file_bytes), &file_ref,
												operation, &stage)
				  != 0);
		UT_ASSERT_EQ(stage.state, 0);
		UT_ASSERT_EQ(stage.owner_pid, 0);
		UT_ASSERT(access(staged_file, F_OK) != 0);
		cleanup_stage();
	}
	setup_stage();
	if (!prepare_stage(&stage)) {
		cleanup_stage();
		return;
	}
	UT_ASSERT(cluster_shared_config_prepare(root, file_bytes, strlen(file_bytes), &file_ref,
											operation, &second)
			  != 0);
	UT_ASSERT_EQ(second.state, 0);
	UT_ASSERT_EQ(access(staged_file, F_OK), 0);
	UT_ASSERT_EQ(cluster_shared_config_discard(root, &stage), 0);
	cleanup_stage();
}

UT_TEST(test_install_sync_retry_after_unlink)
{
	ClusterSharedConfigStage stage;
	setup_stage();
	(void)unlink(object);
	if (!prepare_stage(&stage)) {
		cleanup_stage();
		return;
	}
	held_mode = ExclusiveLock;
	sync_calls = 0;
	fail_sync = 2;
	UT_ASSERT_EQ(cluster_shared_config_install(root, &stage), CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT(access(staged_file, F_OK) != 0);
	UT_ASSERT_EQ(access(object, F_OK), 0);
	fail_sync = 0;
	UT_ASSERT_EQ(cluster_shared_config_install(root, &stage), 0);
	UT_ASSERT_EQ(cluster_shared_config_discard(root, &stage), 0);
	cleanup_stage();
}

UT_TEST(test_sync_failed_cancel_never_resurrects)
{
	ClusterSharedConfigStage stage;
	for (int n = 0; n < 2; ++n) {
		setup_stage();
		if (!prepare_stage(&stage)) {
			cleanup_stage();
			continue;
		}
		held_mode = ExclusiveLock;
		if (n == 1)
			UT_ASSERT_EQ(cluster_shared_config_install(root, &stage), 0);
		sync_calls = 0;
		fail_sync = 1;
		UT_ASSERT_EQ(cluster_shared_config_discard(root, &stage), CLUSTER_CONTROL_ROOT_IO_ERROR);
		UT_ASSERT(access(staged_file, F_OK) != 0);
		UT_ASSERT(cluster_shared_config_install(root, &stage) != 0);
		fail_sync = 0;
		UT_ASSERT_EQ(cluster_shared_config_discard(root, &stage), 0);
		UT_ASSERT_EQ(access(object, F_OK), 0);
		cleanup_stage();
	}
}

UT_TEST(test_publication_preconditions)
{
	ClusterSharedConfigStage stage;
	uint8 zero[16] = { 0 };
	setup_stage();
	enableFsync = false;
	UT_ASSERT(cluster_shared_config_prepare(root, file_bytes, strlen(file_bytes), &file_ref,
											operation, &stage)
			  != 0);
	UT_ASSERT_EQ(stage.state, 0);
	enableFsync = true;
	UT_ASSERT(
		cluster_shared_config_prepare(root, file_bytes, strlen(file_bytes), &file_ref, zero, &stage)
		!= 0);
	UT_ASSERT_EQ(stage.state, 0);
	file_bytes[0] ^= 1;
	UT_ASSERT(cluster_shared_config_prepare(root, file_bytes, strlen(file_bytes), &file_ref,
											operation, &stage)
			  != 0);
	UT_ASSERT_EQ(stage.state, 0);
	UT_ASSERT(access(staged_file, F_OK) != 0);
	cleanup_stage();
}

static unsigned visit_calls, refuse_visit;
static ClusterControlRootResult
check_visit(const ClusterSharedConfigEntry *entry, void *arg)
{
	const ClusterSharedConfigEntry *expected = arg;
	unsigned n = visit_calls++;
	if (entry->node_id != expected[n].node_id || strcmp(entry->name, expected[n].name)
		|| strcmp(entry->value, expected[n].value))
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	return visit_calls == refuse_visit ? CLUSTER_CONTROL_ROOT_CAS_CONFLICT
									   : CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

UT_TEST(test_visitor_exact_literals_and_refusal)
{
	char bytes[1024];
	ClusterSharedConfigRef ref;
	ClusterSharedConfigEntry expected[] = { { -1, "cluster.enabled", "on" },
											{ -1, "test.label", "a'b\\中文" },
											{ 0, "port", "5432" },
											{ 127, "port", "5433" } };
	fixture(bytes, sizeof(bytes), body, &ref);
	visit_calls = refuse_visit = 0;
	UT_ASSERT_EQ(cluster_shared_config_visit(bytes, strlen(bytes), &ref, check_visit, expected), 0);
	UT_ASSERT_EQ(visit_calls, 4);
	visit_calls = 0;
	refuse_visit = 2;
	UT_ASSERT_EQ(cluster_shared_config_visit(bytes, strlen(bytes), &ref, check_visit, expected),
				 CLUSTER_CONTROL_ROOT_CAS_CONFLICT);
	UT_ASSERT_EQ(visit_calls, 2);
}

UT_TEST(test_visitor_validates_whole_object_first)
{
	char bytes[1024];
	ClusterSharedConfigRef ref;
	fixture(bytes, sizeof(bytes), body, &ref);
	visit_calls = refuse_visit = 0;
	bytes[strlen(bytes) - 1] = 'X';
	UT_ASSERT(cluster_shared_config_visit(bytes, strlen(bytes), &ref, check_visit, NULL) != 0);
	UT_ASSERT_EQ(visit_calls, 0);
	digest(bytes, strlen(bytes), ref.sha256);
	UT_ASSERT(cluster_shared_config_visit(bytes, strlen(bytes), &ref, check_visit, NULL) != 0);
	UT_ASSERT_EQ(visit_calls, 0);
	UT_ASSERT(cluster_shared_config_visit(bytes, strlen(bytes), &ref, NULL, NULL) != 0);
}

UT_TEST(test_amend_preserves_other_scopes_and_exact_literals)
{
	static const ClusterSharedConfigEntry changes[] = { { -1, "test.label", "new'\\中文" },
														{ 127, "port", "6433" },
														{ 0, "log_filename", "node%p.log" },
														{ -1, "aaa", "before" },
														{ 127, "zzz", "after" },
														{ -1, "test.label", NULL },
														{ 0, "port", NULL } };
	static const char *const expected[]
		= { "common.cluster.enabled='on'\ncommon.test.label='new''\\中文'\nnode000.port='5432'"
			"\nnode127.port='5433'\n",
			"common.cluster.enabled='on'\ncommon.test.label='a''b\\中文'\nnode000.port='5432'"
			"\nnode127.port='6433'\n",
			"common.cluster.enabled='on'\ncommon.test.label='a''b\\中文'\nnode000.log_filename='"
			"node%p.log'\nnode000.port='5432'\nnode127.port='5433'\n",
			"common.aaa='before'\ncommon.cluster.enabled='on'\ncommon.test.label='a''b\\中文'"
			"\nnode000.port='5432'\nnode127.port='5433'\n",
			"common.cluster.enabled='on'\ncommon.test.label='a''b\\中文'\nnode000.port='5432'"
			"\nnode127.port='5433'\nnode127.zzz='after'\n",
			"common.cluster.enabled='on'\nnode000.port='5432'\nnode127.port='5433'\n",
			"common.cluster.enabled='on'\ncommon.test.label='a''b\\中文'\nnode127.port='5433'\n" };
	char bytes[1024], before[1024];
	ClusterSharedConfigRef ref, original, next;
	fixture(bytes, sizeof(bytes), body, &ref);
	memcpy(before, bytes, strlen(bytes) + 1);
	original = ref;
	for (size_t i = 0; i < lengthof(changes); i++) {
		ClusterSharedConfigImage out;
		ClusterControlRootResult result;
		bool changed = false;
		uint32 count = 0;
		result = cluster_shared_config_amend(bytes, strlen(bytes), &ref, &changes[i], &out, &next,
											 &changed);
		UT_ASSERT_EQ(result, 0);
		if (result != 0)
			continue;
		UT_ASSERT(changed);
		UT_ASSERT_EQ(next.identity.generation, 48);
		UT_ASSERT_EQ(cluster_shared_config_validate(out.bytes, out.len, &next, &count), 0);
		UT_ASSERT_EQ(strcmp(strstr(out.bytes, "common."), expected[i]), 0);
		next.identity.generation--;
		UT_ASSERT_EQ(memcmp(&next.identity, &original.identity, sizeof(next.identity)), 0);
		UT_ASSERT_EQ(memcmp(&ref, &original, sizeof(ref)), 0);
		UT_ASSERT_EQ(strcmp(bytes, before), 0);
		cluster_shared_config_free(&out);
		UT_ASSERT_EQ(allocations, 0);
	}
}

UT_TEST(test_amend_noop_and_sequence_exhaustion)
{
	char bytes[1024];
	ClusterSharedConfigRef ref, next;
	ClusterSharedConfigImage out;
	ClusterSharedConfigEntry change = { -1, "test.label", "a'b\\中文" };
	fixture(bytes, sizeof(bytes), body, &ref);
	for (int i = 0; i < 2; i++) {
		bool changed = true;
		ClusterControlRootResult result;
		if (i == 1) {
			change.name = "missing";
			change.value = NULL;
		}
		result = cluster_shared_config_amend(bytes, strlen(bytes), &ref, &change, &out, &next,
											 &changed);
		UT_ASSERT_EQ(result, 0);
		if (result != 0)
			continue;
		UT_ASSERT(!changed);
		UT_ASSERT_EQ(memcmp(&next, &ref, sizeof(ref)), 0);
		UT_ASSERT_EQ(strcmp(out.bytes, bytes), 0);
		cluster_shared_config_free(&out);
	}
	ref.identity.generation = UINT64_MAX;
	UT_ASSERT_EQ(cluster_shared_config_encode(&ref.identity, NULL, 0, bytes, sizeof(bytes),
											  &out.len, ref.sha256),
				 0);
	{
		bool changed = true;
		change.name = "new";
		change.value = "on";
		UT_ASSERT_EQ(
			cluster_shared_config_amend(bytes, strlen(bytes), &ref, &change, &out, &next, &changed),
			CLUSTER_CONTROL_ROOT_SEQUENCE_EXHAUSTED);
		UT_ASSERT_EQ(out.bytes, NULL);
		UT_ASSERT_EQ(out.len, 0);
		UT_ASSERT_EQ(next.identity.generation, 0);
		UT_ASSERT(!changed);
	}
	UT_ASSERT_EQ(allocations, 0);
}

UT_TEST(test_amend_refuses_invalid_input_before_output)
{
	for (int i = 0; i < 7; i++) {
		char bytes[1024];
		ClusterSharedConfigRef ref, next;
		ClusterSharedConfigImage out;
		ClusterSharedConfigEntry change = { -1, "test.label", "next" };
		bool changed = true;
		fixture(bytes, sizeof(bytes), body, &ref);
		if (i == 0)
			ref.sha256[0] ^= 1;
		if (i == 1) {
			bytes[strlen(bytes) - 1] = 'X';
			digest(bytes, strlen(bytes), ref.sha256);
		}
		if (i == 2)
			change.node_id = 126;
		if (i == 3)
			change.name = "other\nsetting";
		if (i == 4)
			change.value = "value\nnew";
		if (i == 5)
			change.name = NULL;
		memset(&out, 0xa5, sizeof(out));
		memset(&next, 0xa5, sizeof(next));
		UT_ASSERT(cluster_shared_config_amend(bytes, strlen(bytes), i == 6 ? NULL : &ref, &change,
											  &out, &next, &changed)
				  != 0);
		UT_ASSERT_EQ(out.bytes, NULL);
		UT_ASSERT_EQ(out.len, 0);
		UT_ASSERT_EQ(next.identity.generation, 0);
		UT_ASSERT(!changed);
		UT_ASSERT_EQ(allocations, 0);
	}
}

UT_TEST(test_amend_preserves_entry_and_byte_limits)
{
	char *bytes = malloc(CLUSTER_SHARED_CONFIG_MAX_BYTES + 1);
	char *value = malloc(CLUSTER_SHARED_CONFIG_MAX_VALUE + 1);
	char(*names)[16] = malloc(CLUSTER_SHARED_CONFIG_MAX_ENTRIES * 16);
	ClusterSharedConfigEntry *entries
		= malloc(CLUSTER_SHARED_CONFIG_MAX_ENTRIES * sizeof(*entries));
	ClusterSharedConfigRef ref, next;
	ClusterSharedConfigImage out;
	ClusterSharedConfigEntry change = { -1, "zzz", "x" };
	char small[1024];
	size_t len;
	bool changed;
	if (!bytes || !value || !names || !entries)
		abort();
	fixture(small, sizeof(small), body, &ref);
	memset(value, 'v', CLUSTER_SHARED_CONFIG_MAX_VALUE);
	value[CLUSTER_SHARED_CONFIG_MAX_VALUE] = '\0';
	for (int n = 0; n < CLUSTER_SHARED_CONFIG_MAX_ENTRIES; n++) {
		snprintf(names[n], 16, "a%04d", n);
		entries[n] = (ClusterSharedConfigEntry){ -1, names[n], "x" };
	}
	UT_ASSERT_EQ(
		cluster_shared_config_encode(&ref.identity, entries, CLUSTER_SHARED_CONFIG_MAX_ENTRIES,
									 bytes, CLUSTER_SHARED_CONFIG_MAX_BYTES + 1, &len, ref.sha256),
		0);
	UT_ASSERT_EQ(cluster_shared_config_amend(bytes, len, &ref, &change, &out, &next, &changed),
				 CLUSTER_CONTROL_ROOT_BAD_SIZE);
	UT_ASSERT_EQ(out.bytes, NULL);
	UT_ASSERT(!changed);
	change.name = names[0];
	change.value = "replacement";
	UT_ASSERT_EQ(cluster_shared_config_amend(bytes, len, &ref, &change, &out, &next, &changed), 0);
	cluster_shared_config_free(&out);
	for (int n = 0; n < 127; n++)
		entries[n].value = value;
	UT_ASSERT_EQ(cluster_shared_config_encode(&ref.identity, entries, 127, bytes,
											  CLUSTER_SHARED_CONFIG_MAX_BYTES + 1, &len,
											  ref.sha256),
				 0);
	change.name = "zzz";
	change.value = value;
	UT_ASSERT_EQ(cluster_shared_config_amend(bytes, len, &ref, &change, &out, &next, &changed),
				 CLUSTER_CONTROL_ROOT_BAD_SIZE);
	UT_ASSERT_EQ(out.bytes, NULL);
	UT_ASSERT(!changed);
	UT_ASSERT_EQ(allocations, 0);
	free(entries);
	free(names);
	free(value);
	free(bytes);
}

int
main(void)
{
	UT_PLAN(27);
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
	UT_RUN(test_prepare_install_and_cancel_preserves_formal);
	UT_RUN(test_no_clobber_and_identical_retry);
	UT_RUN(test_same_generation_different_hash_objects);
	UT_RUN(test_owner_and_file_identity_cannot_be_substituted);
	UT_RUN(test_prepare_failure_and_uuid_collision);
	UT_RUN(test_install_sync_retry_after_unlink);
	UT_RUN(test_sync_failed_cancel_never_resurrects);
	UT_RUN(test_publication_preconditions);
	UT_RUN(test_visitor_exact_literals_and_refusal);
	UT_RUN(test_visitor_validates_whole_object_first);
	UT_RUN(test_amend_preserves_other_scopes_and_exact_literals);
	UT_RUN(test_amend_noop_and_sequence_exhaustion);
	UT_RUN(test_amend_refuses_invalid_input_before_output);
	UT_RUN(test_amend_preserves_entry_and_byte_limits);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
