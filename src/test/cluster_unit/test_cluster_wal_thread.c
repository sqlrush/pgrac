/*-------------------------------------------------------------------------
 *
 * test_cluster_wal_thread.c
 *	  pgrac spec-4.1 D8 — cluster_unit tests for the per-thread WAL
 *	  routing pure helpers (cluster_wal_thread.h, header-only inline).
 *
 *	  12 tests covering:
 *	    T1   identity: enabled=false -> LEGACY for any node_id
 *	    T2   identity: enabled + node_id=-1 -> LEGACY (unset stays legacy)
 *	    T3   identity: node 0 -> thread 1; node 127 -> thread 128 (=MAX)
 *	    T4   dir_name endpoints: "thread_1" / "thread_128"; never "thread_0"
 *	    T5   validator: cluster_flags != 0 rejected for any thread_id
 *	    T6   validator: LEGACY accepted under any expected value
 *	    T7   validator: real id accepted under expected=INVALID (any) and
 *	         under expected=same; rejected under expected=other (RL1 matrix)
 *	    T8   validator: out-of-range rejected (MAX+1, MAX_REAL, 0xFFFF)
 *	    T9   claim fill/validate round-trip (all identity fields + crc)
 *	    T10  claim corruption rejected: crc flip / magic / version
 *	    T11  claim identity mismatch rejected: thread_id / node_id
 *	    T12  Stage 1 strict wrapper semantics unchanged (0/0 only)
 *
 *	  Linkage mirrors test_cluster_undo_record: header-only inclusion +
 *	  libpgcommon/libpgport for pg_crc32c -- no module .o, no stubs.
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-4.1-per-thread-wal-routing.md (FROZEN v1.0)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cluster/cluster_wal_claim.h"
#include "cluster/cluster_wal_thread.h"
#include "common/cryptohash.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();


/*
 * Assert backstop: cassert builds pull libpgport_srv objects (snprintf.c
 * via the header's dir-name builder) that reference ExceptionalCondition.
 */
void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}


/* ---- T1: identity is LEGACY whenever the cluster is disabled ---- */
UT_TEST(test_identity_disabled_is_legacy)
{
	UT_ASSERT_EQ((int)cluster_wal_thread_id_for(false, -1), (int)XLP_THREAD_ID_LEGACY);
	UT_ASSERT_EQ((int)cluster_wal_thread_id_for(false, 0), (int)XLP_THREAD_ID_LEGACY);
	UT_ASSERT_EQ((int)cluster_wal_thread_id_for(false, 127), (int)XLP_THREAD_ID_LEGACY);
}

/* ---- T2: enabled but node_id unset stays LEGACY ---- */
UT_TEST(test_identity_unset_node_is_legacy)
{
	UT_ASSERT_EQ((int)cluster_wal_thread_id_for(true, -1), (int)XLP_THREAD_ID_LEGACY);
}

/* ---- T3: node 0 -> thread 1; node 127 -> thread 128 == MAX ---- */
UT_TEST(test_identity_real_mapping)
{
	UT_ASSERT_EQ((int)cluster_wal_thread_id_for(true, 0), (int)XLP_THREAD_ID_FIRST_REAL);
	UT_ASSERT_EQ((int)cluster_wal_thread_id_for(true, 127), (int)CLUSTER_WAL_THREAD_MAX);
	UT_ASSERT_EQ((int)cluster_wal_thread_id_for(true, 5), 6);
}

/* ---- T4: directory-name endpoints; the sentinel is not a directory ---- */
UT_TEST(test_dir_name_endpoints)
{
	char buf[64];

	cluster_wal_thread_dir_name(XLP_THREAD_ID_FIRST_REAL, buf, sizeof(buf));
	UT_ASSERT_EQ(strcmp(buf, "thread_1"), 0);

	cluster_wal_thread_dir_name(CLUSTER_WAL_THREAD_MAX, buf, sizeof(buf));
	UT_ASSERT_EQ(strcmp(buf, "thread_128"), 0);

	/* No real id maps to "thread_0" (namespace matrix, spec-4.1 §2.1). */
	cluster_wal_thread_dir_name(cluster_wal_thread_id_for(true, 0), buf, sizeof(buf));
	UT_ASSERT_EQ(strcmp(buf, "thread_0") != 0, 1);
}

/* ---- T5: non-zero cluster_flags is rejected for any thread_id ---- */
UT_TEST(test_validator_flags_rejected)
{
	UT_ASSERT_EQ(cluster_xlog_validate_page_header(XLP_THREAD_ID_LEGACY, 1, XLP_THREAD_ID_INVALID),
				 false);
	UT_ASSERT_EQ(cluster_xlog_validate_page_header(2, 0x8000, XLP_THREAD_ID_INVALID), false);
	UT_ASSERT_EQ(cluster_xlog_validate_page_header(2, 1, 2), false);
}

/* ---- T6: LEGACY accepted under any expected value ---- */
UT_TEST(test_validator_legacy_always_ok)
{
	UT_ASSERT_EQ(cluster_xlog_validate_page_header(XLP_THREAD_ID_LEGACY, XLP_CLUSTER_FLAGS_RESERVED,
												   XLP_THREAD_ID_INVALID),
				 true);
	/* mixed segments: own-stream strict recovery still accepts legacy pages */
	UT_ASSERT_EQ(
		cluster_xlog_validate_page_header(XLP_THREAD_ID_LEGACY, XLP_CLUSTER_FLAGS_RESERVED, 7),
		true);
}

/* ---- T7: RL1 matrix -- any-valid vs own-stream strict ---- */
UT_TEST(test_validator_rl1_matrix)
{
	/* tools / standby / cross-thread diagnostics: expected = INVALID = any */
	UT_ASSERT_EQ(
		cluster_xlog_validate_page_header(1, XLP_CLUSTER_FLAGS_RESERVED, XLP_THREAD_ID_INVALID),
		true);
	UT_ASSERT_EQ(cluster_xlog_validate_page_header(
					 CLUSTER_WAL_THREAD_MAX, XLP_CLUSTER_FLAGS_RESERVED, XLP_THREAD_ID_INVALID),
				 true);
	/* own-stream strict: same id passes, different id is rejected */
	UT_ASSERT_EQ(cluster_xlog_validate_page_header(7, XLP_CLUSTER_FLAGS_RESERVED, 7), true);
	UT_ASSERT_EQ(cluster_xlog_validate_page_header(7, XLP_CLUSTER_FLAGS_RESERVED, 8), false);
}

/* ---- T8: out-of-range real ids rejected ---- */
UT_TEST(test_validator_range_rejected)
{
	UT_ASSERT_EQ(cluster_xlog_validate_page_header(
					 CLUSTER_WAL_THREAD_MAX + 1, XLP_CLUSTER_FLAGS_RESERVED, XLP_THREAD_ID_INVALID),
				 false);
	UT_ASSERT_EQ(cluster_xlog_validate_page_header(
					 XLP_THREAD_ID_MAX_REAL, XLP_CLUSTER_FLAGS_RESERVED, XLP_THREAD_ID_INVALID),
				 false);
	UT_ASSERT_EQ(cluster_xlog_validate_page_header(
					 XLP_THREAD_ID_INVALID, XLP_CLUSTER_FLAGS_RESERVED, XLP_THREAD_ID_INVALID),
				 false);
}

/* ---- T9: claim fill/validate round-trip ---- */
UT_TEST(test_claim_roundtrip)
{
	ClusterWalThreadClaim claim;
	const char *reason = (const char *)0x1;

	UT_ASSERT_EQ((int)sizeof(ClusterWalThreadClaim), 40);

	cluster_wal_thread_claim_fill(&claim, 3, 2, 1234567890LL);
	UT_ASSERT_EQ(claim.magic == CLUSTER_WAL_THREAD_CLAIM_MAGIC, true);
	UT_ASSERT_EQ((int)claim.version, (int)CLUSTER_WAL_THREAD_CLAIM_VERSION);
	UT_ASSERT_EQ(cluster_wal_thread_claim_validate(&claim, 3, 2, &reason), true);
	UT_ASSERT_EQ(reason == NULL, true);
}

/* ---- T10: corruption rejected (crc / magic / version) ---- */
UT_TEST(test_claim_corruption_rejected)
{
	ClusterWalThreadClaim claim;
	const char *reason = NULL;

	cluster_wal_thread_claim_fill(&claim, 3, 2, 42);
	claim.created_at ^= 1; /* body flip without recomputing crc */
	UT_ASSERT_EQ(cluster_wal_thread_claim_validate(&claim, 3, 2, &reason), false);
	UT_ASSERT_EQ(strcmp(reason, "bad crc"), 0);

	cluster_wal_thread_claim_fill(&claim, 3, 2, 42);
	claim.magic = 0xDEADBEEF;
	UT_ASSERT_EQ(cluster_wal_thread_claim_validate(&claim, 3, 2, &reason), false);
	UT_ASSERT_EQ(strcmp(reason, "bad magic"), 0);

	cluster_wal_thread_claim_fill(&claim, 3, 2, 42);
	claim.version = 99;
	UT_ASSERT_EQ(cluster_wal_thread_claim_validate(&claim, 3, 2, &reason), false);
	UT_ASSERT_EQ(strcmp(reason, "bad version"), 0);
}

/* ---- T11: identity mismatch rejected (foreign claim) ---- */
UT_TEST(test_claim_identity_mismatch_rejected)
{
	ClusterWalThreadClaim claim;
	const char *reason = NULL;

	cluster_wal_thread_claim_fill(&claim, 3, 2, 42);
	UT_ASSERT_EQ(cluster_wal_thread_claim_validate(&claim, 4, 2, &reason), false);
	UT_ASSERT_EQ(strcmp(reason, "thread_id mismatch"), 0);

	cluster_wal_thread_claim_fill(&claim, 3, 2, 42);
	UT_ASSERT_EQ(cluster_wal_thread_claim_validate(&claim, 3, 9, &reason), false);
	UT_ASSERT_EQ(strcmp(reason, "node_id mismatch"), 0);
}

/* ---- T12: Stage 1 strict wrapper semantics unchanged ---- */
UT_TEST(test_stage1_wrapper_unchanged)
{
	UT_ASSERT_EQ(cluster_xlog_validate_page_header_stage1_invariant(0, 0), true);
	UT_ASSERT_EQ(cluster_xlog_validate_page_header_stage1_invariant(1, 0), false);
	UT_ASSERT_EQ(cluster_xlog_validate_page_header_stage1_invariant(0, 1), false);
	UT_ASSERT_EQ(cluster_xlog_validate_page_header_thread_id(0), true);
	UT_ASSERT_EQ(cluster_xlog_validate_page_header_thread_id(3), false);
}


/* PGRAC: independent byte fixtures, not encoder/decoder self-confirmation.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static void
claim_put(uint8 *bytes, size_t off, uint64 value, size_t width)
{
	for (size_t i = 0; i < width; ++i)
		bytes[off + i] = value >> (8 * i);
}

static uint32
claim_crc(uint8 bytes[112])
{
	pg_crc32c crc;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, 104);
	FIN_CRC32C(crc);
	claim_put(bytes, 104, crc, 4);
	return crc;
}

static void
claim_hash(const uint8 bytes[112], uint8 hash[32])
{
	pg_cryptohash_ctx *ctx = pg_cryptohash_create(PG_SHA256);
	UT_ASSERT_NOT_NULL(ctx);
	if (ctx == NULL)
		abort();
	UT_ASSERT_EQ(pg_cryptohash_init(ctx), 0);
	UT_ASSERT_EQ(pg_cryptohash_update(ctx, bytes, 112), 0);
	UT_ASSERT_EQ(pg_cryptohash_final(ctx, hash, 32), 0);
	pg_cryptohash_free(ctx);
}

static void
claim_fixture(uint8 bytes[112], ClusterWalThreadClaimV2 *claim, ClusterWalThreadClaimRefV2 *ref,
			  int node)
{
	memset(bytes, 0, 112);
	memset(claim, 0, sizeof(*claim));
	memset(ref, 0, sizeof(*ref));
	claim->identity.system_identifier = UINT64_C(0x1234567812345678);
	memset(claim->identity.storage_uuid, 0x11, 16);
	memset(claim->identity.authority_uuid, 0x22, 16);
	claim->identity.origin_thread_id = node + 1;
	claim->identity.origin_node_id = node;
	claim->identity.origin_owner_incarnation = 99;
	claim->identity.root_lineage_seq = 11;
	claim->identity.thread_claim_created_at = 12345;
	claim->database_incarnation = 41;
	claim->config_generation = 47;
	claim->claim_generation = 5;
	claim_put(bytes, 0, UINT32_C(0x50475443), 4);
	claim_put(bytes, 4, 2, 2);
	claim_put(bytes, 6, node + 1, 2);
	claim_put(bytes, 8, node, 4);
	claim_put(bytes, 16, claim->identity.system_identifier, 8);
	claim_put(bytes, 24, 41, 8);
	claim_put(bytes, 32, 99, 8);
	memset(bytes + 40, 0x11, 16);
	memset(bytes + 56, 0x22, 16);
	claim_put(bytes, 72, 11, 8);
	claim_put(bytes, 80, 12345, 8);
	claim_put(bytes, 88, 47, 8);
	claim_put(bytes, 96, 5, 8);
	claim->identity.thread_claim_crc32c = claim_crc(bytes);
	ref->identity = claim->identity;
	ref->database_incarnation = 41;
	ref->max_config_generation = 49; /* Immutable claim predates current config. */
	claim_hash(bytes, ref->claim_sha256);
}

static void
claim_decode_refuses(uint8 bytes[112], size_t len, ClusterWalThreadClaimRefV2 *ref,
					 ClusterControlRootResult expected)
{
	ClusterWalThreadClaimV2 out, zero = { 0 };
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT_EQ(cluster_wal_claim_v2_decode(bytes, len, ref, &out), expected);
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
}

UT_TEST(test_v2_independent_codec)
{
	for (int node = 0; node <= 127; node += 127) {
		uint8 bytes[112], actual[112];
		ClusterWalThreadClaimV2 claim, out;
		ClusterWalThreadClaimRefV2 ref;
		claim_fixture(bytes, &claim, &ref, node);
		UT_ASSERT_EQ(cluster_wal_claim_v2_encode(&claim, actual), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(memcmp(bytes, actual, 112), 0);
		claim.identity.thread_claim_crc32c = 0; /* Construction without a stored CRC. */
		UT_ASSERT_EQ(cluster_wal_claim_v2_encode(&claim, actual), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(memcmp(bytes, actual, 112), 0);
		UT_ASSERT_EQ(cluster_wal_claim_v2_decode(bytes, 112, &ref, &out),
					 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		claim.identity.thread_claim_crc32c = ref.identity.thread_claim_crc32c;
		UT_ASSERT_EQ(memcmp(&claim, &out, sizeof(claim)), 0);
	}
}

UT_TEST(test_v2_corruption_reserved_size)
{
	uint8 bytes[113];
	ClusterWalThreadClaimV2 claim;
	ClusterWalThreadClaimRefV2 ref;
	claim_fixture(bytes, &claim, &ref, 0);
	claim_decode_refuses(bytes, 40, &ref, CLUSTER_CONTROL_ROOT_BAD_SIZE);
	claim_decode_refuses(bytes, 111, &ref, CLUSTER_CONTROL_ROOT_BAD_SIZE);
	claim_decode_refuses(bytes, 113, &ref, CLUSTER_CONTROL_ROOT_BAD_SIZE);
	bytes[32] ^= 1;
	claim_decode_refuses(bytes, 112, &ref, CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC);
	for (int i = 0; i < 4; ++i) {
		claim_fixture(bytes, &claim, &ref, 0);
		bytes[i == 0 ? 0 : i == 1 ? 4 : i == 2 ? 12 : 108] ^= 1;
		claim_crc(bytes);
		claim_decode_refuses(bytes, 112, &ref,
							 i == 0	  ? CLUSTER_CONTROL_ROOT_BAD_MAGIC
							 : i == 1 ? CLUSTER_CONTROL_ROOT_BAD_VERSION
									  : CLUSTER_CONTROL_ROOT_BAD_RESERVED);
	}
	claim_fixture(bytes, &claim, &ref, 0);
	ref.claim_sha256[0] ^= 1;
	claim_decode_refuses(bytes, 112, &ref, CLUSTER_CONTROL_ROOT_HASH_MISMATCH);
}

UT_TEST(test_v2_exact_root_identity)
{
	const size_t offsets[] = { 6, 16, 24, 32, 40, 56, 72, 80 };
	for (size_t i = 0; i < lengthof(offsets); ++i) {
		uint8 bytes[112];
		ClusterWalThreadClaimV2 claim;
		ClusterWalThreadClaimRefV2 ref;
		claim_fixture(bytes, &claim, &ref, 0);
		++bytes[offsets[i]];
		if (offsets[i] == 6)
			claim_put(bytes, 8, 1, 4); /* Valid but foreign node/thread pair. */
		ref.identity.thread_claim_crc32c = claim_crc(bytes);
		claim_hash(bytes, ref.claim_sha256);
		claim_decode_refuses(bytes, 112, &ref, CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
	}
	{
		uint8 bytes[112];
		ClusterWalThreadClaimV2 claim;
		ClusterWalThreadClaimRefV2 ref;
		claim_fixture(bytes, &claim, &ref, 0);
		++ref.identity.thread_claim_crc32c;
		claim_decode_refuses(bytes, 112, &ref, CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
	}
}

UT_TEST(test_v2_generation_ranges)
{
	uint8 bytes[112];
	ClusterWalThreadClaimV2 claim;
	ClusterWalThreadClaimRefV2 ref;
	const size_t fields[] = { 6, 16, 24, 32, 40, 56, 72, 80, 88, 96 };
	const size_t widths[] = { 2, 8, 8, 8, 16, 16, 8, 8, 8, 8 };
	for (size_t i = 0; i < lengthof(fields); ++i) {
		claim_fixture(bytes, &claim, &ref, 0);
		memset(bytes + fields[i], 0, widths[i]);
		ref.identity.thread_claim_crc32c = claim_crc(bytes);
		claim_hash(bytes, ref.claim_sha256);
		claim_decode_refuses(bytes, 112, &ref,
							 fields[i] == 24 || fields[i] >= 88
								 ? CLUSTER_CONTROL_ROOT_RANGE_INVALID
								 : CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
	}
	claim_fixture(bytes, &claim, &ref, 0);
	claim_put(bytes, 88, 50, 8);
	ref.identity.thread_claim_crc32c = claim_crc(bytes);
	claim_hash(bytes, ref.claim_sha256);
	claim_decode_refuses(bytes, 112, &ref, CLUSTER_CONTROL_ROOT_RANGE_INVALID);
	claim_fixture(bytes, &claim, &ref, 0);
	claim_put(bytes, 80, UINT64_MAX, 8);
	ref.identity.thread_claim_crc32c = claim_crc(bytes);
	claim_decode_refuses(bytes, 112, &ref, CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
}

UT_TEST(test_v2_encode_refusal_clears_bytes)
{
	for (int i = 0; i < 7; ++i) {
		uint8 bytes[112], actual[112], zero[112] = { 0 };
		ClusterWalThreadClaimV2 claim;
		ClusterWalThreadClaimRefV2 ref;
		claim_fixture(bytes, &claim, &ref, 0);
		switch (i) {
		case 0:
			claim.identity.reserved42 = 1;
			break;
		case 1:
			claim.identity.reserved60 = 1;
			break;
		case 2:
			claim.identity.origin_node_id = 127;
			break;
		case 3:
			claim.config_generation = 0;
			break;
		case 4:
			claim.claim_generation = 0;
			break;
		case 5:
			++claim.identity.thread_claim_crc32c;
			break;
		case 6:
			claim.identity.origin_thread_id = 129;
			break;
		}
		memset(actual, 0xa5, sizeof(actual));
		UT_ASSERT_NE(cluster_wal_claim_v2_encode(&claim, actual), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(memcmp(actual, zero, 112), 0);
	}
}

/* All filesystem changes below are confined to a new mkdtemp-owned fixture. */
UT_TEST(test_v2_fixed_generation_file)
{
	char root[] = "/tmp/pgrac-wal-claim-XXXXXX";
	char thread[MAXPGPATH], gen[MAXPGPATH], file[MAXPGPATH], alt[MAXPGPATH], alias[MAXPGPATH];
	uint8 bytes[113];
	ClusterWalThreadClaimV2 claim, out, zero = { 0 };
	ClusterWalThreadClaimRefV2 ref;
	int fd;

	claim_fixture(bytes, &claim, &ref, 0);
	UT_ASSERT_NOT_NULL(mkdtemp(root));
	snprintf(thread, sizeof(thread), "%s/thread_1", root);
	snprintf(gen, sizeof(gen), "%s/generation_99", thread);
	snprintf(file, sizeof(file), "%s/pgrac_thread.claim", gen);
	snprintf(alt, sizeof(alt), "%s/real.claim", gen);
	UT_ASSERT_EQ(mkdir(thread, 0700), 0);
	UT_ASSERT_EQ(mkdir(gen, 0700), 0);
	UT_ASSERT_EQ(cluster_wal_claim_v2_read(root, &ref, &out), CLUSTER_CONTROL_ROOT_ABSENT);
	fd = open(file, O_WRONLY | O_CREAT | O_EXCL, 0600);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(write(fd, bytes, 112), 112);
	UT_ASSERT_EQ(close(fd), 0);
	UT_ASSERT_EQ(cluster_wal_claim_v2_read(root, &ref, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(memcmp(&claim, &out, sizeof(claim)), 0);
	for (int i = 0; i < 4; ++i) {
		const char *path = i == 0 ? file : i == 1 ? gen : i == 2 ? thread : root;
		UT_ASSERT_EQ(chmod(path, 0770), 0);
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT_EQ(cluster_wal_claim_v2_read(root, &ref, &out), CLUSTER_CONTROL_ROOT_IO_ERROR);
		UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
		UT_ASSERT_EQ(chmod(path, i == 0 ? 0600 : 0700), 0);
	}
	fd = open(file, O_WRONLY | O_APPEND);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(write(fd, bytes, 1), 1);
	UT_ASSERT_EQ(close(fd), 0);
	UT_ASSERT_EQ(cluster_wal_claim_v2_read(root, &ref, &out), CLUSTER_CONTROL_ROOT_BAD_SIZE);
	UT_ASSERT_EQ(truncate(file, 40), 0);
	UT_ASSERT_EQ(cluster_wal_claim_v2_read(root, &ref, &out), CLUSTER_CONTROL_ROOT_BAD_SIZE);
	UT_ASSERT_EQ(rename(file, alt), 0);
	UT_ASSERT_EQ(symlink("real.claim", file), 0);
	UT_ASSERT_EQ(cluster_wal_claim_v2_read(root, &ref, &out), CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT_EQ(unlink(file), 0);
	UT_ASSERT_EQ(mkfifo(file, 0600), 0);
	UT_ASSERT_EQ(cluster_wal_claim_v2_read(root, &ref, &out), CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT_EQ(unlink(file), 0);
	UT_ASSERT_EQ(mkdir(file, 0700), 0);
	UT_ASSERT_EQ(cluster_wal_claim_v2_read(root, &ref, &out), CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT_EQ(rmdir(file), 0);
	UT_ASSERT_EQ(unlink(alt), 0);
	UT_ASSERT_EQ(rmdir(gen), 0);
	UT_ASSERT_EQ(symlink(".", gen), 0);
	UT_ASSERT_EQ(cluster_wal_claim_v2_read(root, &ref, &out), CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT_EQ(unlink(gen), 0);
	UT_ASSERT_EQ(rmdir(thread), 0);
	UT_ASSERT_EQ(symlink(".", thread), 0);
	UT_ASSERT_EQ(cluster_wal_claim_v2_read(root, &ref, &out), CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT_EQ(unlink(thread), 0);
	snprintf(alias, sizeof(alias), "%s/root-alias", root);
	UT_ASSERT_EQ(symlink(root, alias), 0);
	UT_ASSERT_EQ(cluster_wal_claim_v2_read(alias, &ref, &out), CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT_EQ(unlink(alias), 0);
	UT_ASSERT_EQ(rmdir(root), 0);
}

UT_TEST(test_v2_invalid_arguments_clear_outputs)
{
	uint8 bytes[112], zero_bytes[112] = { 0 };
	ClusterWalThreadClaimV2 claim, out, zero = { 0 };
	ClusterWalThreadClaimRefV2 ref;

	claim_fixture(bytes, &claim, &ref, 0);
	UT_ASSERT_EQ(cluster_wal_claim_v2_encode(&claim, NULL), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_claim_v2_encode(NULL, bytes), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(memcmp(bytes, zero_bytes, sizeof(bytes)), 0);
	claim_fixture(bytes, &claim, &ref, 0);
	UT_ASSERT_EQ(cluster_wal_claim_v2_decode(bytes, 112, &ref, NULL),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT_EQ(cluster_wal_claim_v2_decode(NULL, 112, &ref, &out),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	ref.identity.reserved60 = 1;
	claim_decode_refuses(bytes, 112, &ref, CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT_EQ(cluster_wal_claim_v2_read(NULL, &ref, &out),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
}

int
main(int argc, char **argv)
{
	UT_PLAN(19);

	UT_RUN(test_identity_disabled_is_legacy);
	UT_RUN(test_identity_unset_node_is_legacy);
	UT_RUN(test_identity_real_mapping);
	UT_RUN(test_dir_name_endpoints);
	UT_RUN(test_validator_flags_rejected);
	UT_RUN(test_validator_legacy_always_ok);
	UT_RUN(test_validator_rl1_matrix);
	UT_RUN(test_validator_range_rejected);
	UT_RUN(test_claim_roundtrip);
	UT_RUN(test_claim_corruption_rejected);
	UT_RUN(test_claim_identity_mismatch_rejected);
	UT_RUN(test_stage1_wrapper_unchanged);
	UT_RUN(test_v2_independent_codec);
	UT_RUN(test_v2_corruption_reserved_size);
	UT_RUN(test_v2_exact_root_identity);
	UT_RUN(test_v2_generation_ranges);
	UT_RUN(test_v2_encode_refusal_clears_bytes);
	UT_RUN(test_v2_fixed_generation_file);
	UT_RUN(test_v2_invalid_arguments_clear_outputs);

	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
