/*-------------------------------------------------------------------------
 *
 * test_pgrac_fenced_drain.c
 *    Real consumer tests for complete, current target drain evidence.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_pgrac_fenced_drain.c
 *
 * NOTES
 *    PGRAC-original unit; fixture facts are not a live storage certificate.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "pgrac_fenced_drain.h"

#ifdef USE_OPENSSL
#include <openssl/evp.h>
#endif

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

static void
fixture(PgracFencedDrainIdentityV1 *expected, PgracFencedDrainEvidenceV1 *observed,
		PgracFencedDrainRouteV1 routes[4])
{
	unsigned int i;

	memset(expected, 0, sizeof(*expected));
	memset(observed, 0, sizeof(*observed));
	memset(expected->operation_id, 1, 16);
	expected->attempt = 17;
	memset(expected->daemon_boot_id, 2, 16);
	memset(expected->target_boot_id, 3, 16);
	memset(expected->challenge, 4, 16);
	memset(expected->guest_uuid, 5, 16);
	expected->mapping_generation = 7;
	memset(expected->protected_set_digest, 6, 32);
	expected->route_count = 4;
	observed->identity = *expected;
	observed->target_state = PGRAC_FENCED_TARGET_OFF;
	observed->completed = PGRAC_DRAIN_GLOBAL_COMPLETE;
	observed->route_count = 4;
	observed->routes = routes;
	for (i = 0; i < 4; i++) {
		routes[i].ordinal = i;
		routes[i].completed = PGRAC_DRAIN_ROUTE_COMPLETE;
	}
}

static void
expect_failure(const PgracFencedDrainIdentityV1 *expected,
			   const PgracFencedDrainEvidenceV1 *observed, PgracFencedDrainResult result)
{
	PgracFencedReadbackV1 out;
	const PgracFencedReadbackV1 zero = { 0 };

	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT_EQ(pgrac_fenced_drain_verify(expected, observed, &out), result);
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
}

UT_TEST(test_complete_exact_set_and_reordered_evidence)
{
	PgracFencedDrainIdentityV1 expected;
	PgracFencedDrainEvidenceV1 observed;
	PgracFencedDrainRouteV1 routes[4];
	PgracFencedReadbackV1 out;

	fixture(&expected, &observed, routes);
	UT_ASSERT_EQ(pgrac_fenced_drain_verify(&expected, &observed, &out), PGRAC_DRAIN_PROVEN);
	UT_ASSERT_EQ(out.state, PGRAC_FENCED_TARGET_OFF);
	UT_ASSERT_EQ(out.io_drain_state, PGRAC_FENCED_IO_DRAIN_DRAINED);
	UT_ASSERT(memcmp(out.observed_target_uuid, expected.guest_uuid, 16) == 0);
	routes[0].ordinal = 3;
	/* observed.routes aliases routes; drain_verify reads every ordinal. */
	// cppcheck-suppress unreadVariable
	routes[3].ordinal = 0;
	UT_ASSERT_EQ(pgrac_fenced_drain_verify(&expected, &observed, &out), PGRAC_DRAIN_PROVEN);
}

UT_TEST(test_every_operation_identity_is_bound)
{
	PgracFencedDrainIdentityV1 expected;
	PgracFencedDrainEvidenceV1 observed;
	PgracFencedDrainRouteV1 routes[4];
	unsigned int i;

	for (i = 0; i < 9; i++) {
		fixture(&expected, &observed, routes);
		switch (i) {
		case 0:
			observed.identity.operation_id[0]++;
			break;
		case 1:
			observed.identity.attempt++;
			break;
		case 2:
			observed.identity.daemon_boot_id[0]++;
			break;
		case 3:
			observed.identity.target_boot_id[0]++;
			break;
		case 4:
			observed.identity.challenge[0]++;
			break;
		case 5:
			observed.identity.guest_uuid[0]++;
			break;
		case 6:
			observed.identity.mapping_generation++;
			break;
		case 7:
			observed.identity.protected_set_digest[0]++;
			break;
		case 8:
			observed.identity.route_count++;
			break;
		}
		expect_failure(&expected, &observed, PGRAC_DRAIN_IDENTITY_MISMATCH);
	}
}

UT_TEST(test_empty_duplicate_missing_extra_and_unmapped_routes)
{
	PgracFencedDrainIdentityV1 expected;
	PgracFencedDrainEvidenceV1 observed;
	PgracFencedDrainRouteV1 routes[4];

	fixture(&expected, &observed, routes);
	observed.route_count = 0;
	expect_failure(&expected, &observed, PGRAC_DRAIN_INCOMPLETE);
	observed.route_count = 3;
	expect_failure(&expected, &observed, PGRAC_DRAIN_INCOMPLETE);
	observed.route_count = 5; /* Reject length before dereferencing a fifth entry. */
	expect_failure(&expected, &observed, PGRAC_DRAIN_MALFORMED);
	observed.route_count = 4;
	routes[3].ordinal = 2;
	expect_failure(&expected, &observed, PGRAC_DRAIN_MALFORMED);
	/* The duplicate and unmapped ordinals reach expect_failure via observed.routes. */
	// cppcheck-suppress [redundantAssignment,unreadVariable]
	routes[3].ordinal = 4;
	expect_failure(&expected, &observed, PGRAC_DRAIN_MALFORMED);
	observed.routes = NULL;
	expect_failure(&expected, &observed, PGRAC_DRAIN_MALFORMED);
}

UT_TEST(test_off_and_empty_sessions_do_not_substitute_for_drain)
{
	PgracFencedDrainIdentityV1 expected;
	PgracFencedDrainEvidenceV1 observed;
	PgracFencedDrainRouteV1 routes[4];
	unsigned int i;

	for (i = 0; i < 5; i++) {
		fixture(&expected, &observed, routes);
		routes[2].completed &= ~(UINT32_C(1) << i);
		expect_failure(&expected, &observed, PGRAC_DRAIN_INCOMPLETE);
	}
	fixture(&expected, &observed, routes);
	/* expect_failure passes observed.routes to the real completion verifier. */
	routes[2].completed
		// cppcheck-suppress unreadVariable
		= PGRAC_DRAIN_DENY_DURABLE | PGRAC_DRAIN_ADMISSION_DISABLED | PGRAC_DRAIN_SESSION_STOPPED;
	expect_failure(&expected, &observed, PGRAC_DRAIN_INCOMPLETE);
	observed.completed &= ~PGRAC_DRAIN_INVENTORY_COMPLETE;
	expect_failure(&expected, &observed, PGRAC_DRAIN_INCOMPLETE);
}

UT_TEST(test_target_state_inventory_and_on_exclusion)
{
	PgracFencedDrainIdentityV1 expected;
	PgracFencedDrainEvidenceV1 observed;
	PgracFencedDrainRouteV1 routes[4];
	uint32 state;

	for (state = PGRAC_FENCED_TARGET_ON; state <= PGRAC_FENCED_TARGET_UNKNOWN; state++) {
		fixture(&expected, &observed, routes);
		observed.target_state = state;
		expect_failure(&expected, &observed, PGRAC_DRAIN_TARGET_NOT_OFF);
	}
	fixture(&expected, &observed, routes);
	observed.completed = PGRAC_DRAIN_INVENTORY_COMPLETE;
	expect_failure(&expected, &observed, PGRAC_DRAIN_INCOMPLETE);
	observed.completed = PGRAC_DRAIN_UNSOLICITED_ON_BLOCKED;
	expect_failure(&expected, &observed, PGRAC_DRAIN_INCOMPLETE);
	observed.completed = PGRAC_DRAIN_GLOBAL_COMPLETE | 4;
	expect_failure(&expected, &observed, PGRAC_DRAIN_MALFORMED);
	observed.completed = PGRAC_DRAIN_GLOBAL_COMPLETE;
	/* The real drain verifier consumes this invalid route through observed.routes. */
	// cppcheck-suppress unreadVariable
	routes[1].completed |= 32;
	expect_failure(&expected, &observed, PGRAC_DRAIN_MALFORMED);
}

UT_TEST(test_unproved_expectation_cannot_be_echoed_as_success)
{
	PgracFencedDrainIdentityV1 expected;
	PgracFencedDrainEvidenceV1 observed;
	PgracFencedDrainRouteV1 routes[4];

	fixture(&expected, &observed, routes);
	memset(expected.challenge, 0, 16);
	observed.identity = expected;
	expect_failure(&expected, &observed, PGRAC_DRAIN_BAD_ARGUMENT);
	fixture(&expected, &observed, routes);
	expected.route_count = PGRAC_PROTECTED_SET_V2_MAX_ROUTES + 1;
	observed.identity = expected;
	expect_failure(&expected, &observed, PGRAC_DRAIN_BAD_ARGUMENT);
	expect_failure(NULL, &observed, PGRAC_DRAIN_BAD_ARGUMENT);
	fixture(&expected, &observed, routes);
	expect_failure(&expected, NULL, PGRAC_DRAIN_BAD_ARGUMENT);
	UT_ASSERT_EQ(pgrac_fenced_drain_verify(&expected, &observed, NULL), PGRAC_DRAIN_BAD_ARGUMENT);
}

static void
frame_number(uint8 *at, uint64 value, size_t length)
{
	for (size_t i = 0; i < length; ++i)
		at[i] = (uint8)(value >> (i * 8));
}

/* Independent test encoder; no production writer or serialization round trip. */
static size_t
make_frame(uint8 frame[PGRAC_DRAIN_FRAME_MAX_BYTES], const PgracFencedDrainIdentityV1 *identity)
{
	size_t length = 176 + identity->route_count * 8 + 64;

	memset(frame, 0, PGRAC_DRAIN_FRAME_MAX_BYTES);
	memcpy(frame, "PGRDRN01", 8);
	frame_number(frame + 8, 1, 2);
	frame_number(frame + 10, 176, 2);
	frame_number(frame + 12, length, 4);
	memcpy(frame + 16, identity->operation_id, 16);
	frame_number(frame + 32, identity->attempt, 8);
	memcpy(frame + 40, identity->daemon_boot_id, 16);
	memcpy(frame + 56, identity->target_boot_id, 16);
	memcpy(frame + 72, identity->challenge, 16);
	memcpy(frame + 88, identity->guest_uuid, 16);
	frame_number(frame + 104, identity->mapping_generation, 8);
	memcpy(frame + 112, identity->protected_set_digest, 32);
	frame_number(frame + 144, identity->route_count, 4);
	frame_number(frame + 148, 1, 4);
	frame_number(frame + 152, 3, 4);
	frame_number(frame + 156, identity->route_count, 4);
	for (uint32 i = 0; i < identity->route_count; ++i) {
		frame_number(frame + 176 + i * 8, i, 4);
		frame_number(frame + 180 + i * 8, 31, 4);
	}
	return length;
}

static bool
sign_frame(uint8 *frame, size_t length, uint8 key[32], uint8 seed_byte)
{
#ifdef USE_OPENSSL
	uint8 seed[32];
	size_t public_length = 32, signature_length = 64;
	EVP_PKEY *private_key;
	EVP_MD_CTX *context = EVP_MD_CTX_new();
	bool result;

	/* Explicit non-secret fixture seed, never used as a deployed credential. */
	memset(seed, seed_byte, sizeof(seed));
	private_key = EVP_PKEY_new_raw_private_key(EVP_PKEY_ED25519, NULL, seed, sizeof(seed));
	result = private_key != NULL && context != NULL
			 && EVP_PKEY_get_raw_public_key(private_key, key, &public_length) == 1
			 && EVP_DigestSignInit(context, NULL, NULL, NULL, private_key) == 1
			 && EVP_DigestSign(context, frame + length - 64, &signature_length, frame, length - 64)
					== 1
			 && signature_length == 64;
	EVP_MD_CTX_free(context);
	EVP_PKEY_free(private_key);
	UT_ASSERT(result);
	return result;
#else
	(void)frame;
	(void)length;
	memset(key, seed_byte, 32);
	return true;
#endif
}

static PgracFencedDrainResult
frame_refused(const PgracFencedDrainIdentityV1 *identity, const uint8 key[32], const uint8 *frame,
			  size_t length)
{
	PgracFencedReadbackV1 out, zero = { 0 };
	PgracFencedDrainResult result;

	memset(&out, 0xa5, sizeof(out));
	result = pgrac_fenced_drain_verify_frame(identity, key, frame, length, &out);
	UT_ASSERT_NE(result, PGRAC_DRAIN_PROVEN);
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	return result;
}

UT_TEST(test_signed_frame_exact_and_maximum_set)
{
	PgracFencedDrainIdentityV1 identity;
	PgracFencedDrainEvidenceV1 evidence;
	PgracFencedDrainRouteV1 routes[4];
	PgracFencedReadbackV1 out;
	uint8 frame[PGRAC_DRAIN_FRAME_MAX_BYTES], key[32];

	fixture(&identity, &evidence, routes);
	for (int maximum = 0; maximum < 2; ++maximum) {
		size_t length;
		if (maximum)
			identity.route_count = PGRAC_PROTECTED_SET_V2_MAX_ROUTES;
		length = make_frame(frame, &identity);
		if (!sign_frame(frame, length, key, 0x43))
			return;
#ifdef USE_OPENSSL
		UT_ASSERT_EQ(pgrac_fenced_drain_verify_frame(&identity, key, frame, length, &out),
					 PGRAC_DRAIN_PROVEN);
		UT_ASSERT_EQ(out.state, PGRAC_FENCED_TARGET_OFF);
		UT_ASSERT_EQ(out.io_drain_state, PGRAC_FENCED_IO_DRAIN_DRAINED);
		UT_ASSERT(memcmp(out.observed_target_uuid, identity.guest_uuid, 16) == 0);
#else
		(void)out;
		UT_ASSERT_EQ(frame_refused(&identity, key, frame, length), PGRAC_DRAIN_UNSUPPORTED);
#endif
	}
}

UT_TEST(test_signed_frame_tampering_truncation_tail_and_wrong_signer)
{
	PgracFencedDrainIdentityV1 identity;
	PgracFencedDrainEvidenceV1 evidence;
	PgracFencedDrainRouteV1 routes[4];
	uint8 frame[PGRAC_DRAIN_FRAME_MAX_BYTES], key[32], other[32];
	size_t length;

	fixture(&identity, &evidence, routes);
	length = make_frame(frame, &identity);
	if (!sign_frame(frame, length, key, 0x43))
		return;
	for (size_t i = 0; i < length; ++i) {
		frame[i] ^= 1;
		frame_refused(&identity, key, frame, length);
		frame[i] ^= 1;
		frame_refused(&identity, key, frame, i);
	}
	frame_refused(&identity, key, frame, length + 1);
	if (!sign_frame(frame, length, other, 0x44))
		return;
#ifdef USE_OPENSSL
	UT_ASSERT_EQ(frame_refused(&identity, key, frame, length), PGRAC_DRAIN_BAD_SIGNATURE);
#else
	frame_refused(&identity, key, frame, length);
#endif
}

UT_TEST(test_valid_signature_does_not_prove_identity_or_completion)
{
	const unsigned int offsets[]
		= { 16, 32, 40, 56, 72, 88, 104, 112, 144, 148, 152, 176, 180, 188, 196, 204 };
	PgracFencedDrainIdentityV1 identity;
	PgracFencedDrainEvidenceV1 evidence;
	PgracFencedDrainRouteV1 routes[4];
	uint8 frame[PGRAC_DRAIN_FRAME_MAX_BYTES], key[32];

	fixture(&identity, &evidence, routes);
	for (size_t i = 0; i < lengthof(offsets); ++i) {
		size_t length = make_frame(frame, &identity);
		frame[offsets[i]] ^= 1;
		if (!sign_frame(frame, length, key, 0x43))
			return;
		frame_refused(&identity, key, frame, length);
	}
}

UT_TEST(test_signed_malformed_header_and_arguments_never_make_proof)
{
	const unsigned int offsets[]
		= { 0,	 8,	  9,   10,	11,	 12,  15,  156, 159, 160, 161, 162, 163,
			164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175 };
	PgracFencedDrainIdentityV1 identity;
	PgracFencedDrainEvidenceV1 evidence;
	PgracFencedDrainRouteV1 routes[4];
	uint8 frame[PGRAC_DRAIN_FRAME_MAX_BYTES], key[32];
	size_t length;

	fixture(&identity, &evidence, routes);
	for (size_t i = 0; i < lengthof(offsets); ++i) {
		length = make_frame(frame, &identity);
		frame[offsets[i]] ^= 1;
		if (!sign_frame(frame, length, key, 0x43))
			return;
		frame_refused(&identity, key, frame, length);
	}
	length = make_frame(frame, &identity);
	if (!sign_frame(frame, length, key, 0x43))
		return;
	frame_refused(NULL, key, frame, length);
	frame_refused(&identity, NULL, frame, length);
	frame_refused(&identity, key, NULL, length);
	frame_refused(&identity, key, frame, SIZE_MAX);
	memset(identity.challenge, 0, 16);
	length = make_frame(frame, &identity);
	if (!sign_frame(frame, length, key, 0x43))
		return;
	frame_refused(&identity, key, frame, length);
}

UT_TEST(test_identity_public_key_cannot_forge_a_drain_reply)
{
	PgracFencedDrainIdentityV1 identity;
	PgracFencedDrainEvidenceV1 evidence;
	PgracFencedDrainRouteV1 routes[4];
	uint8 frame[PGRAC_DRAIN_FRAME_MAX_BYTES], key[32] = { 1 };
	size_t length;

	fixture(&identity, &evidence, routes);
	length = make_frame(frame, &identity);
	/* No signer/private key: A = identity, R = identity, S = 0. */
	frame[length - 64] = 1;
	frame_refused(&identity, key, frame, length);
}

int
main(void)
{
	UT_PLAN(11);
	UT_RUN(test_complete_exact_set_and_reordered_evidence);
	UT_RUN(test_every_operation_identity_is_bound);
	UT_RUN(test_empty_duplicate_missing_extra_and_unmapped_routes);
	UT_RUN(test_off_and_empty_sessions_do_not_substitute_for_drain);
	UT_RUN(test_target_state_inventory_and_on_exclusion);
	UT_RUN(test_unproved_expectation_cannot_be_echoed_as_success);
	UT_RUN(test_signed_frame_exact_and_maximum_set);
	UT_RUN(test_signed_frame_tampering_truncation_tail_and_wrong_signer);
	UT_RUN(test_valid_signature_does_not_prove_identity_or_completion);
	UT_RUN(test_signed_malformed_header_and_arguments_never_make_proof);
	UT_RUN(test_identity_public_key_cannot_forge_a_drain_reply);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
