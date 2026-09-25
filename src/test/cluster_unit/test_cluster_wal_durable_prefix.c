/*-------------------------------------------------------------------------
 * PGRAC: production durable-prefix codec and exact filesystem reader tests.
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cluster/cluster_wal_durable_prefix.h"

#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition pg_attribute_unused(),
					 const char *file pg_attribute_unused(), int line pg_attribute_unused())
{
	abort();
}

static ClusterWalDurablePrefixRef
reference(void)
{
	ClusterWalDurablePrefixRef ref;
	memset(&ref, 0, sizeof(ref));
	ref.claim.identity.system_identifier = UINT64CONST(0x1122334455667788);
	memset(ref.claim.identity.storage_uuid, 0x12, 16);
	memset(ref.claim.identity.authority_uuid, 0x34, 16);
	ref.claim.identity.origin_node_id = 0;
	ref.claim.identity.origin_thread_id = 1;
	ref.claim.identity.origin_owner_incarnation = 17;
	ref.claim.identity.root_lineage_seq = 29;
	ref.claim.identity.thread_claim_created_at = 31;
	ref.claim.database_incarnation = 37;
	ref.claim.max_config_generation = 41;
	memset(ref.claim.claim_sha256, 0x56, 32);
	ref.timeline = 3;
	return ref;
}

static void
put(uint8 *bytes, int offset, uint64 value, int width)
{
	for (int i = 0; i < width; ++i)
		bytes[offset + i] = value >> (8 * i);
}

static void
fix_crc(uint8 *bytes)
{
	pg_crc32c crc;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, 252);
	FIN_CRC32C(crc);
	put(bytes, 252, crc, 4);
}

/* Literal layout independent of the encoder under test. */
static void
literal(uint8 *bytes)
{
	memset(bytes, 0, 256);
	memcpy(bytes, "PGWP", 4);
	put(bytes, 4, 1, 2);
	put(bytes, 6, 256, 2);
	put(bytes, 8, UINT64CONST(0x1122334455667788), 8);
	put(bytes, 16, 37, 8);
	memset(bytes + 24, 0x12, 16);
	memset(bytes + 40, 0x34, 16);
	put(bytes, 60, 1, 4);
	put(bytes, 64, 17, 8);
	put(bytes, 72, 3, 4);
	memset(bytes + 80, 0x56, 32);
	put(bytes, 112, 7, 8);
	put(bytes, 120, 0x1000040, 8);
	put(bytes, 128, 0x1000028, 8);
	put(bytes, 136, 0x87654321, 4);
	fix_crc(bytes);
}

static bool
zero(const void *ptr, size_t n)
{
	const uint8 *bytes = ptr;
	for (size_t i = 0; i < n; ++i)
		if (bytes[i])
			return false;
	return true;
}

static void
reject(uint8 *bytes, size_t len, const ClusterWalDurablePrefixRef *ref,
	   ClusterControlRootResult result)
{
	ClusterWalDurablePrefix out;
	memset(&out, 0xA5, sizeof(out));
	UT_ASSERT_EQ(cluster_wal_durable_prefix_decode(bytes, len, ref, &out), result);
	UT_ASSERT(zero(&out, sizeof(out)));
}

UT_TEST(test_literal_and_roundtrip)
{
	ClusterWalDurablePrefixRef ref = reference();
	ClusterWalDurablePrefix out;
	ClusterWalDurablePrefix expected = { 7, 0x1000040, 0x1000028, 0x87654321 };
	uint8 bytes[256], encoded[256];
	literal(bytes);
	UT_ASSERT_EQ(cluster_wal_durable_prefix_decode(bytes, sizeof(bytes), &ref, &out), 0);
	UT_ASSERT_EQ(out.sequence, expected.sequence);
	UT_ASSERT_EQ(out.exclusive_end, expected.exclusive_end);
	UT_ASSERT_EQ(out.record_start, expected.record_start);
	UT_ASSERT_EQ(out.record_crc, expected.record_crc);
	UT_ASSERT_EQ(cluster_wal_durable_prefix_encode(&ref, &expected, encoded), 0);
	UT_ASSERT_EQ(memcmp(bytes, encoded, sizeof(bytes)), 0);
	{
		uint8 unaligned[257];
		memcpy(unaligned + 1, bytes, sizeof(bytes));
		UT_ASSERT_EQ(cluster_wal_durable_prefix_decode(unaligned + 1, sizeof(bytes), &ref, &out),
					 0);
		UT_ASSERT_EQ(out.record_start, expected.record_start);
	}
}

UT_TEST(test_empty_and_last_node)
{
	ClusterWalDurablePrefixRef ref = reference();
	ClusterWalDurablePrefix empty = { 1, 0, 0, 0 }, out;
	uint8 bytes[256];
	ref.claim.identity.origin_node_id = 127;
	ref.claim.identity.origin_thread_id = 128;
	UT_ASSERT_EQ(cluster_wal_durable_prefix_encode(&ref, &empty, bytes), 0);
	UT_ASSERT_EQ(bytes[56], 127);
	UT_ASSERT_EQ(bytes[60], 128);
	UT_ASSERT_EQ(cluster_wal_durable_prefix_decode(bytes, sizeof(bytes), &ref, &out), 0);
	UT_ASSERT_EQ(out.sequence, 1);
	UT_ASSERT_EQ(out.exclusive_end, 0);
	UT_ASSERT_EQ(out.record_start, 0);
	UT_ASSERT_EQ(out.record_crc, 0);
}

UT_TEST(test_header_and_reserved)
{
	ClusterWalDurablePrefixRef ref = reference();
	uint8 bytes[256];
	const int offsets[] = { 0, 4, 6 };
	const ClusterControlRootResult results[]
		= { CLUSTER_CONTROL_ROOT_BAD_MAGIC, CLUSTER_CONTROL_ROOT_BAD_VERSION,
			CLUSTER_CONTROL_ROOT_BAD_SIZE };
	for (int i = 0; i < 3; ++i) {
		literal(bytes);
		bytes[offsets[i]] ^= 1;
		fix_crc(bytes);
		reject(bytes, sizeof(bytes), &ref, results[i]);
	}
	for (int i = 76; i < 252; ++i) {
		if (i >= 80 && i < 140)
			continue;
		literal(bytes);
		bytes[i] = 1;
		fix_crc(bytes);
		reject(bytes, sizeof(bytes), &ref, CLUSTER_CONTROL_ROOT_BAD_RESERVED);
	}
}

UT_TEST(test_every_crc_byte)
{
	ClusterWalDurablePrefixRef ref = reference();
	uint8 bytes[256];
	for (int i = 0; i < 256; ++i) {
		literal(bytes);
		bytes[i] ^= 1;
		reject(bytes, sizeof(bytes), &ref, CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC);
	}
}

UT_TEST(test_exact_identity)
{
	ClusterWalDurablePrefixRef ref = reference();
	uint8 bytes[256];
	const int fields[] = { 8, 16, 24, 39, 40, 55, 56, 60, 64, 72, 80, 111 };
	for (size_t i = 0; i < lengthof(fields); ++i) {
		literal(bytes);
		bytes[fields[i]] ^= 1;
		fix_crc(bytes);
		reject(bytes, sizeof(bytes), &ref, CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
	}
}

UT_TEST(test_invalid_reference)
{
	ClusterWalDurablePrefixRef ref, good = reference();
	ClusterWalDurablePrefix prefix = { 1, 0, 0, 0 };
	uint8 bytes[256], encoded[256];
	literal(bytes);
#define BAD_REF(expr)                                                                              \
	do {                                                                                           \
		ref = good;                                                                                \
		expr;                                                                                      \
		reject(bytes, sizeof(bytes), &ref, CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);                 \
		memset(encoded, 0xA5, sizeof(encoded));                                                    \
		UT_ASSERT_EQ(cluster_wal_durable_prefix_encode(&ref, &prefix, encoded),                    \
					 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);                                       \
		UT_ASSERT(zero(encoded, sizeof(encoded)));                                                 \
	} while (0)
	BAD_REF(ref.timeline = 0);
	BAD_REF(ref.claim.identity.system_identifier = 0);
	BAD_REF(ref.claim.database_incarnation = 0);
	BAD_REF(ref.claim.max_config_generation = 0);
	BAD_REF(ref.claim.identity.origin_thread_id = 129);
	BAD_REF(ref.claim.identity.origin_node_id = -1);
	BAD_REF(ref.claim.identity.origin_owner_incarnation = 0);
	BAD_REF(ref.claim.identity.root_lineage_seq = 0);
	BAD_REF(ref.claim.identity.thread_claim_created_at = 0);
	BAD_REF(ref.claim.identity.reserved42 = 1);
	BAD_REF(ref.claim.identity.reserved60 = 1);
	BAD_REF(memset(ref.claim.identity.storage_uuid, 0, 16));
	BAD_REF(memset(ref.claim.identity.authority_uuid, 0, 16));
	BAD_REF(memset(ref.claim.claim_sha256, 0, 32));
#undef BAD_REF
}

UT_TEST(test_prefix_ranges)
{
	ClusterWalDurablePrefixRef ref = reference();
	ClusterWalDurablePrefix cases[]
		= { { 0, 0, 0, 0 },	 { 2, 0, 0, 0 },   { 1, 0, 1, 0 },	{ 1, 0, 0, 1 },
			{ 1, 10, 0, 1 }, { 1, 10, 10, 1 }, { 1, 10, 11, 1 } };
	uint8 bytes[256];
	for (size_t i = 0; i < lengthof(cases); ++i) {
		UT_ASSERT_EQ(cluster_wal_durable_prefix_encode(&ref, &cases[i], bytes),
					 CLUSTER_CONTROL_ROOT_RANGE_INVALID);
		UT_ASSERT(zero(bytes, sizeof(bytes)));
		literal(bytes);
		put(bytes, 112, cases[i].sequence, 8);
		put(bytes, 120, cases[i].exclusive_end, 8);
		put(bytes, 128, cases[i].record_start, 8);
		put(bytes, 136, cases[i].record_crc, 4);
		fix_crc(bytes);
		reject(bytes, sizeof(bytes), &ref, CLUSTER_CONTROL_ROOT_RANGE_INVALID);
	}
}

UT_TEST(test_zero_record_crc_valid)
{
	ClusterWalDurablePrefixRef ref = reference();
	ClusterWalDurablePrefix prefix = { UINT64_MAX, UINT64_MAX, UINT64_MAX - 1, 0 }, out;
	uint8 bytes[256];
	UT_ASSERT_EQ(cluster_wal_durable_prefix_encode(&ref, &prefix, bytes), 0);
	UT_ASSERT_EQ(cluster_wal_durable_prefix_decode(bytes, sizeof(bytes), &ref, &out), 0);
	UT_ASSERT_EQ(out.sequence, UINT64_MAX);
	UT_ASSERT_EQ(out.exclusive_end, UINT64_MAX);
	UT_ASSERT_EQ(out.record_crc, 0);
}

UT_TEST(test_arguments_and_size)
{
	ClusterWalDurablePrefixRef ref = reference();
	ClusterWalDurablePrefix prefix = { 1, 0, 0, 0 };
	uint8 bytes[257];
	literal(bytes);
	reject(NULL, 256, &ref, CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	reject(bytes, 256, NULL, CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	for (size_t i = 0; i < 256; ++i)
		reject(bytes, i, &ref, CLUSTER_CONTROL_ROOT_BAD_SIZE);
	reject(bytes, 257, &ref, CLUSTER_CONTROL_ROOT_BAD_SIZE);
	UT_ASSERT_EQ(cluster_wal_durable_prefix_encode(NULL, &prefix, bytes),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(zero(bytes, 256));
	UT_ASSERT_EQ(cluster_wal_durable_prefix_encode(&ref, NULL, bytes),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_durable_prefix_encode(&ref, &prefix, NULL),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_durable_prefix_decode(bytes, 256, &ref, NULL),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
}

UT_TEST(test_successor)
{
	ClusterWalDurablePrefixRef ref = reference();
	ClusterWalDurablePrefix old = { 1, 0, 0, 0 }, next = { 2, 200, 100, 42 };
	UT_ASSERT_EQ(cluster_wal_durable_prefix_successor(&ref, &old, &old), 0);
	UT_ASSERT_EQ(cluster_wal_durable_prefix_successor(&ref, &old, &next), 0);
	old = next;
	next.sequence++;
	next.record_start = 200;
	next.exclusive_end = 300;
	UT_ASSERT_EQ(cluster_wal_durable_prefix_successor(&ref, &old, &next), 0);
	UT_ASSERT_EQ(cluster_wal_durable_prefix_successor(&ref, &next, &next), 0);
	next.record_start = 201;
	UT_ASSERT_EQ(cluster_wal_durable_prefix_successor(&ref, &old, &next), 0);
}

UT_TEST(test_successor_refusals)
{
	ClusterWalDurablePrefixRef ref = reference();
	ClusterWalDurablePrefix old = { 7, 200, 100, 42 }, next = old;
	next.record_crc++;
	UT_ASSERT_EQ(cluster_wal_durable_prefix_successor(&ref, &old, &next),
				 CLUSTER_CONTROL_ROOT_COPY_DIVERGENT);
	next = old;
	next.sequence++;
	UT_ASSERT_EQ(cluster_wal_durable_prefix_successor(&ref, &old, &next),
				 CLUSTER_CONTROL_ROOT_RANGE_INVALID);
	next.record_start = 199;
	next.exclusive_end = 300;
	UT_ASSERT_EQ(cluster_wal_durable_prefix_successor(&ref, &old, &next),
				 CLUSTER_CONTROL_ROOT_RANGE_INVALID);
	next.record_start = 200;
	next.sequence += 1;
	UT_ASSERT_EQ(cluster_wal_durable_prefix_successor(&ref, &old, &next),
				 CLUSTER_CONTROL_ROOT_RANGE_INVALID);
	next.sequence = 6;
	UT_ASSERT_EQ(cluster_wal_durable_prefix_successor(&ref, &old, &next),
				 CLUSTER_CONTROL_ROOT_RANGE_INVALID);
	old.sequence = UINT64_MAX;
	next.sequence = 1;
	UT_ASSERT_EQ(cluster_wal_durable_prefix_successor(&ref, &old, &next),
				 CLUSTER_CONTROL_ROOT_SEQUENCE_EXHAUSTED);
	UT_ASSERT_EQ(cluster_wal_durable_prefix_successor(&ref, &old, &old), 0);
	UT_ASSERT_EQ(cluster_wal_durable_prefix_successor(NULL, &old, &old),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_durable_prefix_successor(&ref, NULL, &old),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_durable_prefix_successor(&ref, &old, NULL),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
}

static void
write_file(const char *path, const uint8 *bytes, size_t len)
{
	int fd = open(path, O_CREAT | O_WRONLY | O_EXCL, 0600);
	UT_ASSERT(fd >= 0);
	if (fd < 0)
		return;
	UT_ASSERT_EQ(write(fd, bytes, len), len);
	UT_ASSERT_EQ(close(fd), 0);
}

UT_TEST(test_filesystem)
{
	char root[] = "/tmp/pgrac-wp-XXXXXX", thread[256], generation[256], dir[256], file[256],
		 bak[256];
	ClusterWalDurablePrefixRef ref = reference();
	ClusterWalDurablePrefix out;
	uint8 bytes[257];
	UT_ASSERT(mkdtemp(root) != NULL);
	snprintf(thread, sizeof(thread), "%s/thread_1", root);
	snprintf(generation, sizeof(generation), "%s/thread_1/generation_17", root);
	snprintf(dir, sizeof(dir), "%s/thread_1/generation_17/durable_prefix", root);
	snprintf(file, sizeof(file), "%s/thread_1/generation_17/durable_prefix/current", root);
	snprintf(bak, sizeof(bak), "%s/thread_1/generation_17/durable_prefix/current.bak", root);
	UT_ASSERT_EQ(mkdir(thread, 0700), 0);
	UT_ASSERT_EQ(mkdir(generation, 0700), 0);
	UT_ASSERT_EQ(mkdir(dir, 0700), 0);
#define READ_EXPECT(result)                                                                        \
	do {                                                                                           \
		memset(&out, 0xA5, sizeof(out));                                                           \
		UT_ASSERT_EQ(cluster_wal_durable_prefix_read(root, &ref, &out), result);                   \
		if (result != 0)                                                                           \
			UT_ASSERT(zero(&out, sizeof(out)));                                                    \
	} while (0)
	READ_EXPECT(CLUSTER_CONTROL_ROOT_ABSENT);
	literal(bytes);
	write_file(bak, bytes, 256);
	READ_EXPECT(CLUSTER_CONTROL_ROOT_ABSENT);
	write_file(file, bytes, 256);
	READ_EXPECT(CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(out.sequence, 7);
	UT_ASSERT_EQ(out.exclusive_end, 0x1000040);
	ref.claim.identity.origin_owner_incarnation++;
	READ_EXPECT(CLUSTER_CONTROL_ROOT_ABSENT);
	ref.claim.identity.origin_owner_incarnation--;
	ref.timeline++;
	READ_EXPECT(CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
	ref.timeline--;
	UT_ASSERT_EQ(chmod(file, 0660), 0);
	READ_EXPECT(CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT_EQ(chmod(file, 0600), 0);
	UT_ASSERT_EQ(chmod(dir, 0770), 0);
	READ_EXPECT(CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT_EQ(chmod(dir, 0700), 0);
	UT_ASSERT_EQ(chmod(root, 0770), 0);
	READ_EXPECT(CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT_EQ(chmod(root, 0700), 0);
	UT_ASSERT_EQ(unlink(file), 0);
	UT_ASSERT_EQ(link(bak, file), 0);
	READ_EXPECT(CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT_EQ(unlink(file), 0);
	write_file(file, bytes, 255);
	READ_EXPECT(CLUSTER_CONTROL_ROOT_BAD_SIZE);
	UT_ASSERT_EQ(unlink(file), 0);
	bytes[256] = 0;
	write_file(file, bytes, 257);
	READ_EXPECT(CLUSTER_CONTROL_ROOT_BAD_SIZE);
	UT_ASSERT_EQ(unlink(file), 0);
	bytes[24] ^= 1;
	write_file(file, bytes, 256);
	READ_EXPECT(CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC);
	UT_ASSERT_EQ(unlink(file), 0);
	UT_ASSERT_EQ(symlink("current.bak", file), 0);
	READ_EXPECT(CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT_EQ(unlink(file), 0);
	UT_ASSERT_EQ(mkfifo(file, 0600), 0);
	READ_EXPECT(CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT_EQ(unlink(file), 0);
	UT_ASSERT_EQ(mkdir(file, 0700), 0);
	READ_EXPECT(CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT_EQ(rmdir(file), 0);
	UT_ASSERT_EQ(unlink(bak), 0);
	UT_ASSERT_EQ(rmdir(dir), 0);
	UT_ASSERT_EQ(symlink(".", dir), 0);
	READ_EXPECT(CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT_EQ(unlink(dir), 0);
	UT_ASSERT_EQ(rmdir(generation), 0);
	UT_ASSERT_EQ(rmdir(thread), 0);
	UT_ASSERT_EQ(rmdir(root), 0);
#undef READ_EXPECT
	UT_ASSERT_EQ(cluster_wal_durable_prefix_read(NULL, &ref, &out),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_durable_prefix_read("", &ref, &out),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_durable_prefix_read(root, NULL, &out),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_durable_prefix_read(root, &ref, NULL),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
}

int
main(void)
{
	UT_PLAN(12);
	UT_RUN(test_literal_and_roundtrip);
	UT_RUN(test_empty_and_last_node);
	UT_RUN(test_header_and_reserved);
	UT_RUN(test_every_crc_byte);
	UT_RUN(test_exact_identity);
	UT_RUN(test_invalid_reference);
	UT_RUN(test_prefix_ranges);
	UT_RUN(test_zero_record_crc_valid);
	UT_RUN(test_arguments_and_size);
	UT_RUN(test_successor);
	UT_RUN(test_successor_refusals);
	UT_RUN(test_filesystem);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
