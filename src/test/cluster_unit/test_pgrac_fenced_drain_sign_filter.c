/*-------------------------------------------------------------------------
 * test_pgrac_fenced_drain_sign_filter.c
 *    Actual crypto and output-boundary tests; fixture facts are not fencing.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 * IDENTIFICATION
 *    src/test/cluster_unit/test_pgrac_fenced_drain_sign_filter.c
 * NOTES
 *    PGRAC-original. No deployed key is used or persisted by these tests.
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"
#include "pgrac_fenced_drain.h"
#include "pgrac_fenced_drain_sign_filter.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#ifdef USE_OPENSSL
#include <openssl/evp.h>
#endif

#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

static void
number(uint8 *at, uint64 value, size_t bytes)
{
	for (size_t n = 0; n < bytes; ++n)
		at[n] = (uint8)(value >> (n * 8));
}

static size_t
fixture(uint8 *input, uint32 count, uint8 key[32], char hex[65],
		PgracFencedDrainIdentityV1 *identity)
{
	uint8 *frame = input + 32;
	size_t body = 176 + count * 8;
	static const char alphabet[] = "0123456789abcdef";

	memset(input, 0, 32 + body);
	memset(input, 0x42, 32); /* Explicit test-only seed, never a deployed key. */
	memset(key, 0x43, 32);
#ifdef USE_OPENSSL
	{
		EVP_PKEY *private_key = EVP_PKEY_new_raw_private_key(EVP_PKEY_ED25519, NULL, input, 32);
		size_t size = 32;
		UT_ASSERT(private_key != NULL);
		UT_ASSERT_EQ(EVP_PKEY_get_raw_public_key(private_key, key, &size), 1);
		UT_ASSERT_EQ(size, 32);
		EVP_PKEY_free(private_key);
	}
#endif
	for (size_t n = 0; n < 32; ++n) {
		hex[n * 2] = alphabet[key[n] >> 4];
		hex[n * 2 + 1] = alphabet[key[n] & 15];
	}
	hex[64] = '\0';
	memset(identity, 0, sizeof(*identity));
	memset(identity->operation_id, 1, 16);
	identity->attempt = 17;
	memset(identity->daemon_boot_id, 2, 16);
	memset(identity->target_boot_id, 3, 16);
	memset(identity->challenge, 4, 16);
	memset(identity->guest_uuid, 5, 16);
	identity->mapping_generation = 7;
	memset(identity->protected_set_digest, 6, 32);
	identity->route_count = count;
	memcpy(frame, "PGRDRN01", 8);
	number(frame + 8, 1, 2);
	number(frame + 10, 176, 2);
	number(frame + 12, body + 64, 4);
	memcpy(frame + 16, identity->operation_id, 16);
	number(frame + 32, identity->attempt, 8);
	memcpy(frame + 40, identity->daemon_boot_id, 16);
	memcpy(frame + 56, identity->target_boot_id, 16);
	memcpy(frame + 72, identity->challenge, 16);
	memcpy(frame + 88, identity->guest_uuid, 16);
	number(frame + 104, identity->mapping_generation, 8);
	memcpy(frame + 112, identity->protected_set_digest, 32);
	number(frame + 144, count, 4);
	number(frame + 148, PGRAC_FENCED_TARGET_OFF, 4);
	number(frame + 152, 3, 4);
	number(frame + 156, count, 4);
	for (uint32 n = 0; n < count; ++n) {
		number(frame + 176 + n * 8, n, 4);
		number(frame + 180 + n * 8, 31, 4);
	}
	return 32 + body;
}

static size_t
run(const uint8 *input, size_t length, char *key, int argc, int expected_rc, uint8 *output)
{
	FILE *in = tmpfile(), *out = tmpfile();
	char *argv[] = { "sign-filter", key, NULL };
	size_t size;
	UT_ASSERT(in != NULL && out != NULL);
	if (in == NULL || out == NULL)
		abort();
	UT_ASSERT_EQ(fwrite(input, 1, length, in), length);
	rewind(in);
	UT_ASSERT_EQ(pgrac_fenced_drain_sign_filter(argc, argv, in, out), expected_rc);
	rewind(out);
	size = fread(output, 1, PGRAC_DRAIN_FRAME_MAX_BYTES + 1, out);
	if (expected_rc != 0)
		UT_ASSERT_EQ(size, 0);
	fclose(in);
	fclose(out);
	return size;
}

UT_TEST(test_real_signature_and_maximum_routes)
{
	uint8 input[32 + PGRAC_DRAIN_FRAME_MAX_BYTES + 1], key[32],
		out[PGRAC_DRAIN_FRAME_MAX_BYTES + 1];
	char hex[65];
	PgracFencedDrainIdentityV1 identity;
	for (int which = 0; which < 2; ++which) {
		size_t length = fixture(input, which ? 128 : 4, key, hex, &identity);
#ifdef USE_OPENSSL
		PgracFencedReadbackV1 readback;
		size_t output = run(input, length, hex, 2, 0, out);
		UT_ASSERT_EQ(output, length - 32 + 64);
		UT_ASSERT(memcmp(out, input + 32, length - 32) == 0);
		UT_ASSERT_EQ(pgrac_fenced_drain_verify_frame(&identity, key, out, output, &readback),
					 PGRAC_DRAIN_PROVEN);
#else
		(void)run(input, length, hex, 2, 77, out);
#endif
	}
}

UT_TEST(test_every_truncation_and_extra_tail_emits_nothing)
{
	uint8 input[32 + PGRAC_DRAIN_FRAME_MAX_BYTES + 1], key[32],
		out[PGRAC_DRAIN_FRAME_MAX_BYTES + 1];
	char hex[65];
	PgracFencedDrainIdentityV1 identity;
	size_t length = fixture(input, 4, key, hex, &identity);
	for (size_t n = 0; n < length; ++n)
		(void)run(input, n, hex, 2, 77, out);
	input[length] = 0;
	(void)run(input, length + 1, hex, 2, 77, out);
	memset(input + length, 0, sizeof(input) - length);
	(void)run(input, sizeof(input), hex, 2, 77, out);
}

UT_TEST(test_incomplete_or_on_facts_cannot_be_signed)
{
	uint8 input[32 + PGRAC_DRAIN_FRAME_MAX_BYTES + 1], key[32],
		out[PGRAC_DRAIN_FRAME_MAX_BYTES + 1];
	char hex[65];
	PgracFencedDrainIdentityV1 identity;
	const size_t offsets[] = { 148, 152, 180, 188, 196, 204 };
	size_t length;
	for (size_t n = 0; n < lengthof(offsets); ++n) {
		length = fixture(input, 4, key, hex, &identity);
		input[32 + offsets[n]] = 0;
		(void)run(input, length, hex, 2, 77, out);
	}
	length = fixture(input, 4, key, hex, &identity);
	number(input + 32 + 148, PGRAC_FENCED_TARGET_ON, 4);
	(void)run(input, length, hex, 2, 77, out);
}

UT_TEST(test_malformed_identity_header_and_duplicate_route_emit_nothing)
{
	uint8 input[32 + PGRAC_DRAIN_FRAME_MAX_BYTES + 1], key[32],
		out[PGRAC_DRAIN_FRAME_MAX_BYTES + 1];
	char hex[65];
	PgracFencedDrainIdentityV1 identity;
	const size_t offsets[] = { 0, 8, 10, 12, 144, 156, 160, 184 };
	size_t length;
	for (size_t n = 0; n < lengthof(offsets); ++n) {
		length = fixture(input, 4, key, hex, &identity);
		input[32 + offsets[n]] ^= 1;
		(void)run(input, length, hex, 2, 77, out);
	}
	length = fixture(input, 4, key, hex, &identity);
	memset(input + 32 + 72, 0, 16);
	(void)run(input, length, hex, 2, 77, out);
}

UT_TEST(test_independently_pinned_key_must_match_the_seed)
{
	uint8 input[32 + PGRAC_DRAIN_FRAME_MAX_BYTES + 1], key[32],
		out[PGRAC_DRAIN_FRAME_MAX_BYTES + 1];
	char hex[65];
	PgracFencedDrainIdentityV1 identity;
	size_t length = fixture(input, 4, key, hex, &identity);
	input[0] ^= 1;
	(void)run(input, length, hex, 2, 77, out);
	memset(hex, '0', 64);
	(void)run(input, length, hex, 2, 77, out);
	hex[1] = '1'; /* Identity small-order public key. */
	(void)run(input, length, hex, 2, 77, out);
}

UT_TEST(test_bad_invocations_are_refused_before_output)
{
	uint8 input[32 + PGRAC_DRAIN_FRAME_MAX_BYTES + 1], key[32],
		out[PGRAC_DRAIN_FRAME_MAX_BYTES + 1];
	char hex[65];
	PgracFencedDrainIdentityV1 identity;
	size_t length = fixture(input, 4, key, hex, &identity);
	(void)run(input, length, hex, 1, 2, out);
	(void)run(input, length, NULL, 2, 2, out);
	hex[0] = 'g';
	(void)run(input, length, hex, 2, 2, out);
	UT_ASSERT_EQ(pgrac_fenced_drain_sign_filter(2, NULL, NULL, NULL), 2);
}

UT_TEST(test_stream_errors_never_report_success)
{
	uint8 input[32 + PGRAC_DRAIN_FRAME_MAX_BYTES + 1], key[32];
	char hex[65];
	char *argv[] = { "sign-filter", hex, NULL };
	PgracFencedDrainIdentityV1 identity;
	size_t length = fixture(input, 4, key, hex, &identity);
	FILE *in = tmpfile(), *out = fopen("/dev/null", "r"), *bad_input = fopen("/dev/null", "w");
	UT_ASSERT(in != NULL && out != NULL && bad_input != NULL);
	if (in == NULL || out == NULL || bad_input == NULL)
		abort();
	UT_ASSERT_EQ(fwrite(input, 1, length, in), length);
	rewind(in);
	UT_ASSERT_EQ(pgrac_fenced_drain_sign_filter(2, argv, in, out), 77);
	UT_ASSERT_EQ(pgrac_fenced_drain_sign_filter(2, argv, bad_input, in), 77);
	fclose(in);
	fclose(out);
	fclose(bad_input);
}

static void
run_descriptor(const uint8 *input, size_t length, char *hex, int fd, int expected)
{
	FILE *in = tmpfile(), *out = tmpfile();
	char descriptor[32];
	char *argv[] = { "sign-filter", hex, descriptor, NULL };
	uint8 result[PGRAC_DRAIN_FRAME_MAX_BYTES + 1] = { 0 };
	size_t size;
	UT_ASSERT(in != NULL && out != NULL);
	if (in == NULL || out == NULL)
		abort();
	snprintf(descriptor, sizeof(descriptor), "%d", fd);
	UT_ASSERT_EQ(fwrite(input + 32, 1, length - 32, in), length - 32);
	rewind(in);
	UT_ASSERT_EQ(pgrac_fenced_drain_sign_filter(3, argv, in, out), expected);
	rewind(out);
	size = fread(result, 1, sizeof(result), out);
	if (expected == 0) {
		UT_ASSERT_EQ(size, length - 32 + 64);
		UT_ASSERT(memcmp(result, input + 32, length - 32) == 0);
	} else
		UT_ASSERT_EQ(size, 0);
	fclose(in);
	fclose(out);
}

UT_TEST(test_owned_seed_descriptor_preserves_offset_and_checks_contents)
{
	uint8 input[32 + PGRAC_DRAIN_FRAME_MAX_BYTES + 1], key[32];
	char hex[65], path[] = "/tmp/pgrac-drain-key-XXXXXX";
	PgracFencedDrainIdentityV1 identity;
	size_t length = fixture(input, 4, key, hex, &identity);
	int fd = mkstemp(path);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(fchmod(fd, 0600), 0);
	UT_ASSERT_EQ(write(fd, input, 32), 32);
	UT_ASSERT_EQ(lseek(fd, 7, SEEK_SET), 7);
#ifdef USE_OPENSSL
	run_descriptor(input, length, hex, fd, 0);
#else
	run_descriptor(input, length, hex, fd, 77);
#endif
	UT_ASSERT_EQ(lseek(fd, 0, SEEK_CUR), 7);
	UT_ASSERT_EQ(pwrite(fd, "x", 1, 0), 1);
	run_descriptor(input, length, hex, fd, 77);
	close(fd);
	unlink(path);
}

UT_TEST(test_unprotected_or_nonregular_seed_descriptor_never_signs)
{
	uint8 input[32 + PGRAC_DRAIN_FRAME_MAX_BYTES + 1], key[32];
	char hex[65], path[] = "/tmp/pgrac-drain-key-XXXXXX";
	PgracFencedDrainIdentityV1 identity;
	size_t length = fixture(input, 4, key, hex, &identity);
	int fd = mkstemp(path), pipefd[2];
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(write(fd, input, 32), 32);
	UT_ASSERT_EQ(fchmod(fd, 0640), 0);
	run_descriptor(input, length, hex, fd, 77);
	UT_ASSERT_EQ(fchmod(fd, 0600), 0);
	UT_ASSERT_EQ(ftruncate(fd, 31), 0);
	run_descriptor(input, length, hex, fd, 77);
	UT_ASSERT_EQ(ftruncate(fd, 33), 0);
	run_descriptor(input, length, hex, fd, 77);
	UT_ASSERT_EQ(ftruncate(fd, 32), 0);
	UT_ASSERT_EQ(unlink(path), 0); /* No remaining protected pathname. */
	run_descriptor(input, length, hex, fd, 77);
	close(fd);
	UT_ASSERT_EQ(pipe(pipefd), 0);
	/* No bytes and writer stays open: refusal must not read a blocking pipe. */
	run_descriptor(input, length, hex, pipefd[0], 77);
	close(pipefd[0]);
	close(pipefd[1]);
}

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(test_real_signature_and_maximum_routes);
	UT_RUN(test_every_truncation_and_extra_tail_emits_nothing);
	UT_RUN(test_incomplete_or_on_facts_cannot_be_signed);
	UT_RUN(test_malformed_identity_header_and_duplicate_route_emit_nothing);
	UT_RUN(test_independently_pinned_key_must_match_the_seed);
	UT_RUN(test_bad_invocations_are_refused_before_output);
	UT_RUN(test_stream_errors_never_report_success);
	UT_RUN(test_owned_seed_descriptor_preserves_offset_and_checks_contents);
	UT_RUN(test_unprotected_or_nonregular_seed_descriptor_never_signs);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
