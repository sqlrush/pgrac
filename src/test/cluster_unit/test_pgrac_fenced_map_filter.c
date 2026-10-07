/*-------------------------------------------------------------------------
 * test_pgrac_fenced_map_filter.c
 *    Exercise the real signed-map filter with independent golden bytes.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_pgrac_fenced_map_filter.c
 *
 * NOTES
 *    PGRAC-original tests; fixture keys are public and not deployment keys.
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <unistd.h>

#include "common/pgrac_fence_map.h"
#include "data/pgrac_fence_map_v2_fixture.h"
#include "pgrac_fenced_map_filter.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static char *arguments[] = { "pgrac-fenced-map-verify",
							 "123456789",
							 "2",
							 "7",
							 "84dc62a01725f0361b3fa2de5615632a8fb9a95d07e564fbb3a661d51fd06828",
							 "c050c5637a44fa8629fff3cccce2300cb362a63d99d95fc54145266f4332445a" };
static uint8 fixture[PGRAC_FENCE_MAP_V2_MAX_BYTES + 1];
static size_t fixture_length;

static void
load_fixture(void)
{
	const char *hex = strstr(fenced_config_v2, "node.2.adapter_data=");

	hex += strlen("node.2.adapter_data=");
	fixture_length = 0;
	while (*hex != '\n') {
		unsigned value;

		if (sscanf(hex, "%2x", &value) != 1)
			abort();
		fixture[fixture_length++] = (uint8)value;
		hex += 2;
	}
}

static int
filter(int argc, char *const *argv, const uint8 *bytes, size_t length, uint8 *result,
	   size_t *result_length)
{
	FILE *input = tmpfile();
	FILE *output = tmpfile();
	int rc;

	if (input == NULL || output == NULL || fwrite(bytes, 1, length, input) != length)
		abort();
	rewind(input);
	rc = pgrac_fenced_map_filter(argc, argv, input, output);
	rewind(output);
	*result_length = fread(result, 1, PGRAC_FENCE_MAP_V2_MAX_BYTES + 1, output);
	fclose(input);
	fclose(output);
	return rc;
}

UT_TEST(test_exact_golden_payload_only_after_verification)
{
	uint8 result[PGRAC_FENCE_MAP_V2_MAX_BYTES + 1];
	size_t length;
	int rc = filter(6, arguments, fixture, fixture_length, result, &length);

#ifdef USE_OPENSSL
	UT_ASSERT_EQ(rc, 0);
	/* Independent fixture carries a canonical payload of exactly 878 bytes. */
	UT_ASSERT_EQ(length, 878);
	UT_ASSERT(memcmp(result, fixture + 32, 878) == 0);
#else
	UT_ASSERT_EQ(rc, 77);
	UT_ASSERT_EQ(length, 0);
#endif
}

UT_TEST(test_argument_shape_decimal_overflow_and_noncanonical_hex)
{
	static const struct {
		int index;
		const char *value;
	} bad[] = { { 1, "" },
				{ 1, "0" },
				{ 1, "-1" },
				{ 1, "+1" },
				{ 1, " 123456789" },
				{ 1, "18446744073709551616" },
				{ 1, "123456789x" },
				{ 2, "128" },
				{ 2, "-1" },
				{ 3, "0" },
				{ 3, "18446744073709551616" },
				{ 4, "" },
				{ 4, "aa" },
				{ 4, "84DC62a01725f0361b3fa2de5615632a8fb9a95d07e564fbb3a661d51fd06828" },
				{ 5, "z050c5637a44fa8629fff3cccce2300cb362a63d99d95fc54145266f4332445a" } };
	uint8 result[PGRAC_FENCE_MAP_V2_MAX_BYTES + 1];
	size_t length;
	char *args[6];

	for (size_t i = 0; i < lengthof(bad); ++i) {
		memcpy(args, arguments, sizeof(args));
		args[bad[i].index] = (char *)bad[i].value;
		UT_ASSERT_EQ(filter(6, args, fixture, fixture_length, result, &length), 2);
		UT_ASSERT_EQ(length, 0);
	}
	UT_ASSERT_EQ(filter(5, arguments, fixture, fixture_length, result, &length), 2);
	UT_ASSERT_EQ(length, 0);
	UT_ASSERT_EQ(filter(7, arguments, fixture, fixture_length, result, &length), 2);
	UT_ASSERT_EQ(length, 0);
}

UT_TEST(test_independent_expectation_mismatch_never_outputs_payload)
{
	static const char *changed[]
		= { "unused",
			"123456788",
			"1",
			"8",
			"94dc62a01725f0361b3fa2de5615632a8fb9a95d07e564fbb3a661d51fd06828",
			"d050c5637a44fa8629fff3cccce2300cb362a63d99d95fc54145266f4332445a" };
	uint8 result[PGRAC_FENCE_MAP_V2_MAX_BYTES + 1];
	size_t length;
	char *args[6];

	for (size_t i = 1; i < lengthof(changed); ++i) {
		memcpy(args, arguments, sizeof(args));
		args[i] = (char *)changed[i];
		UT_ASSERT_EQ(filter(6, args, fixture, fixture_length, result, &length), 77);
		UT_ASSERT_EQ(length, 0);
	}
}

UT_TEST(test_corrupted_byte_or_truncation_never_outputs_partial_configuration)
{
	uint8 result[PGRAC_FENCE_MAP_V2_MAX_BYTES + 1];
	size_t length;

	for (size_t i = 0; i < fixture_length; ++i) {
		fixture[i] ^= 1;
		UT_ASSERT_EQ(filter(6, arguments, fixture, fixture_length, result, &length), 77);
		UT_ASSERT_EQ(length, 0);
		fixture[i] ^= 1;
		UT_ASSERT_EQ(filter(6, arguments, fixture, i, result, &length), 77);
		UT_ASSERT_EQ(length, 0);
	}
}

UT_TEST(test_trailing_bytes_and_oversized_input_refuse)
{
	uint8 result[PGRAC_FENCE_MAP_V2_MAX_BYTES + 1];
	size_t length;

	UT_ASSERT_EQ(filter(6, arguments, fixture, fixture_length + 1, result, &length), 77);
	UT_ASSERT_EQ(length, 0);
	UT_ASSERT_EQ(filter(6, arguments, fixture, sizeof(fixture), result, &length), 77);
	UT_ASSERT_EQ(length, 0);
}

UT_TEST(test_weak_or_zero_public_key_is_unverified)
{
	uint8 result[PGRAC_FENCE_MAP_V2_MAX_BYTES + 1];
	size_t length;
	char *args[6];

	memcpy(args, arguments, sizeof(args));
	args[5] = "0100000000000000000000000000000000000000000000000000000000000000";
	UT_ASSERT_EQ(filter(6, args, fixture, fixture_length, result, &length), 77);
	UT_ASSERT_EQ(length, 0);
	args[5] = "0000000000000000000000000000000000000000000000000000000000000000";
	UT_ASSERT_EQ(filter(6, args, fixture, fixture_length, result, &length), 77);
	UT_ASSERT_EQ(length, 0);
}

UT_TEST(test_output_io_error_is_not_success)
{
	FILE *input = tmpfile();
	FILE *output = tmpfile();

	UT_ASSERT(input != NULL && output != NULL);
	if (input == NULL || output == NULL)
		abort();
	UT_ASSERT_EQ(fwrite(fixture, 1, fixture_length, input), fixture_length);
	rewind(input);
	UT_ASSERT_EQ(close(fileno(output)), 0);
	UT_ASSERT_EQ(pgrac_fenced_map_filter(6, arguments, input, output), 77);
	fclose(input);
	fclose(output);
}

UT_TEST(test_input_io_error_is_not_empty_valid_map)
{
	FILE *input = tmpfile();
	FILE *output = tmpfile();

	UT_ASSERT(input != NULL && output != NULL);
	if (input == NULL || output == NULL)
		abort();
	UT_ASSERT_EQ(close(fileno(input)), 0);
	UT_ASSERT_EQ(pgrac_fenced_map_filter(6, arguments, input, output), 77);
	UT_ASSERT_EQ(ftell(output), 0);
	fclose(input);
	fclose(output);
}

int
main(void)
{
	load_fixture();
	UT_PLAN(8);
	UT_RUN(test_exact_golden_payload_only_after_verification);
	UT_RUN(test_argument_shape_decimal_overflow_and_noncanonical_hex);
	UT_RUN(test_independent_expectation_mismatch_never_outputs_payload);
	UT_RUN(test_corrupted_byte_or_truncation_never_outputs_partial_configuration);
	UT_RUN(test_trailing_bytes_and_oversized_input_refuse);
	UT_RUN(test_weak_or_zero_public_key_is_unverified);
	UT_RUN(test_output_io_error_is_not_success);
	UT_RUN(test_input_io_error_is_not_empty_valid_map);
	UT_DONE();
	return ut_failed_count != 0;
}
