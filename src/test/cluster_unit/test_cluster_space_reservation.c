/*-------------------------------------------------------------------------
 * test_cluster_space_reservation.c
 *    Canonical sequential reservation bytes and exact transitions.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"
#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_space_reservation.h"
#include "port/pg_crc32c.h"
#include "storage/bufpage.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static ClusterSpaceReservation
state(void)
{
	ClusterSpaceReservation s = {0};

	s.identity.key.system_identifier = 7;
	s.identity.key.database_incarnation = 9;
	memset(s.identity.key.storage_uuid, 0xa7, 16);
	s.identity.key.locator = (RelFileLocator){DEFAULTTABLESPACE_OID, 5, 16384};
	memset(s.identity.incarnation, 0x31, 16);
	s.identity.sequence = 1;
	s.identity.operation = 123;
	s.identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	return s;
}

static ClusterSpaceReservationChange
advance(void)
{
	ClusterSpaceReservationChange c = {0};

	c.action = CLUSTER_SPACE_RESERVATION_ADVANCE;
	c.before = state();
	c.before.next_block = 11;
	c.result = c.before;
	c.result.next_block = 18;
	c.first_block = 11;
	c.granted = 7;
	c.before_token = 201;
	c.result_token = 101; /* Opaque: smaller is not stale. */
	return c;
}

static void
repair_crc(uint8 *bytes)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, 156);
	FIN_CRC32C(crc);
	for (int i = 0; i < 4; i++)
		bytes[156 + i] = (uint8)(crc >> (8 * i));
}

UT_TEST(test_literal_unaligned_payload_and_wal)
{
	ClusterSpaceReservation s = state(), out = {0};
	ClusterSpaceReservationChange c = advance(), decoded = {0};
	uint8 bytes[CLUSTER_SPACE_RESERVATION_BYTES + 1] = {0};
	uint8 wal[CLUSTER_SPACE_RESERVATION_WAL_BYTES + 1] = {0};
	uint8 *p = bytes + 1;
	const uint8 prefix[] = {'P', 'S', 'R', '1', 1, 0, 160, 0, 2, 0, 0, 0};

	s.next_block = 0x12345678;
	UT_ASSERT(cluster_space_reservation_encode(&s, p, sizeof(bytes) - 1));
	UT_ASSERT(memcmp(p, prefix, sizeof(prefix)) == 0);
	UT_ASSERT_EQ(p[16], 0x78);
	UT_ASSERT_EQ(p[19], 0x12);
	UT_ASSERT(memcmp(p + 24, "PSI1", 4) == 0);
	UT_ASSERT(cluster_space_reservation_decode(p, sizeof(bytes) - 1, &s.identity.key, &out));
	UT_ASSERT_EQ(out.next_block, s.next_block);
	UT_ASSERT(memcmp(out.identity.incarnation, s.identity.incarnation, 16) == 0);
	UT_ASSERT(cluster_space_reservation_wal_encode(&c, wal + 1, sizeof(wal) - 1));
	UT_ASSERT(memcmp(wal + 1, "PSV1\1\0\160\1", 8) == 0); /* 368 LE */
	UT_ASSERT(cluster_space_reservation_wal_decode(wal + 1, sizeof(wal) - 1, &decoded));
	UT_ASSERT_EQ(decoded.before_token, 201);
	UT_ASSERT_EQ(decoded.result_token, 101);
	UT_ASSERT_EQ(decoded.first_block, 11);
	UT_ASSERT_EQ(decoded.granted, 7);
	UT_ASSERT_EQ(decoded.result.next_block, 18);
}

UT_TEST(test_crc_reserved_namespace_and_refusal_atomicity)
{
	ClusterSpaceReservation s = state(), out, sentinel;
	uint8 valid[CLUSTER_SPACE_RESERVATION_BYTES] = {0}, broken[CLUSTER_SPACE_RESERVATION_BYTES];
	ClusterSpaceIdentityKey wrong;
	const int offsets[] = {8, 12, 20, 152};

	memset(&sentinel, 0xa5, sizeof(sentinel));
	UT_ASSERT(cluster_space_reservation_encode(&s, valid, sizeof(valid)));
	for (size_t i = 0; i < sizeof(valid); i++) {
		memcpy(broken, valid, sizeof(valid));
		broken[i] ^= 1;
		out = sentinel;
		UT_ASSERT(!cluster_space_reservation_decode(broken, sizeof(broken), &s.identity.key, &out));
		UT_ASSERT(memcmp(&out, &sentinel, sizeof(out)) == 0);
	}
	for (size_t i = 0; i < lengthof(offsets); i++) {
		memcpy(broken, valid, sizeof(valid));
		broken[offsets[i]] ^= 1;
		repair_crc(broken);
		out = sentinel;
		UT_ASSERT(!cluster_space_reservation_decode(broken, sizeof(broken), &s.identity.key, &out));
		UT_ASSERT(memcmp(&out, &sentinel, sizeof(out)) == 0);
	}
	wrong = s.identity.key;
	wrong.database_incarnation++;
	out = sentinel;
	UT_ASSERT(!cluster_space_reservation_decode(valid, sizeof(valid), &wrong, &out));
	UT_ASSERT(memcmp(&out, &sentinel, sizeof(out)) == 0);
	memcpy(broken, valid, sizeof(valid));
	s.identity.state = 0;
	UT_ASSERT(!cluster_space_reservation_encode(&s, broken, sizeof(broken)));
	UT_ASSERT(memcmp(broken, valid, sizeof(valid)) == 0);
}

UT_TEST(test_exact_page_class_and_header)
{
	ClusterSpaceReservation s = state(), out = {0};
	PGAlignedBlock page = {0}, bad;
	uint64 token = 99;

	UT_ASSERT(cluster_space_reservation_page_encode(&s, 201, page.data, BLCKSZ));
	UT_ASSERT_EQ(((PageHeader)page.data)->pd_lower, 192);
	UT_ASSERT(cluster_space_reservation_page_valid(page.data, BLCKSZ));
	UT_ASSERT(!cluster_space_identity_page_valid(page.data, BLCKSZ));
	UT_ASSERT(cluster_space_reservation_page_decode(page.data, BLCKSZ, SPACE_FORKNUM, 1,
		&s.identity.key, &out, &token));
	UT_ASSERT_EQ(token, 201);
	UT_ASSERT(!cluster_space_reservation_page_decode(page.data, BLCKSZ, SPACE_FORKNUM, 0,
		&s.identity.key, &out, &token));
	UT_ASSERT(!cluster_space_reservation_page_decode(page.data, BLCKSZ, MAIN_FORKNUM, 1,
		&s.identity.key, &out, &token));
	bad = page;
	bad.data[BLCKSZ - 1] = 1;
	UT_ASSERT(!cluster_space_reservation_page_valid(bad.data, BLCKSZ));
	bad = page;
	((PageHeader)bad.data)->pd_block_scn = 0;
	UT_ASSERT(!cluster_space_reservation_page_valid(bad.data, BLCKSZ));
	UT_ASSERT(cluster_space_identity_page_encode(&s.identity, 201, bad.data, BLCKSZ));
	UT_ASSERT(!cluster_space_reservation_page_valid(bad.data, BLCKSZ));
}

UT_TEST(test_init_advance_and_exact_idempotence)
{
	ClusterSpaceReservationChange c = {0};
	ClusterSpaceReservation out = {0};
	PGAlignedBlock page, saved;
	uint64 token = 0;

	c.action = CLUSTER_SPACE_RESERVATION_INIT;
	c.result = state();
	c.result_token = 17;
	memset(page.data, 0, BLCKSZ);
	UT_ASSERT_EQ(cluster_space_reservation_apply(&c, &c.result.identity.key, page.data, BLCKSZ),
		CLUSTER_SPACE_IDENTITY_APPLY);
	saved = page;
	UT_ASSERT_EQ(cluster_space_reservation_apply(&c, &c.result.identity.key, page.data, BLCKSZ),
		CLUSTER_SPACE_IDENTITY_ALREADY);
	UT_ASSERT(memcmp(page.data, saved.data, BLCKSZ) == 0);
	c = advance();
	UT_ASSERT(cluster_space_reservation_page_encode(&c.before, c.before_token, page.data, BLCKSZ));
	UT_ASSERT_EQ(cluster_space_reservation_apply(&c, &c.result.identity.key, page.data, BLCKSZ),
		CLUSTER_SPACE_IDENTITY_APPLY);
	UT_ASSERT(cluster_space_reservation_page_decode(page.data, BLCKSZ, SPACE_FORKNUM, 1,
		&c.result.identity.key, &out, &token));
	UT_ASSERT_EQ(token, c.result_token);
	UT_ASSERT_EQ(out.next_block, 18);
	saved = page;
	UT_ASSERT_EQ(cluster_space_reservation_apply(&c, &c.result.identity.key, page.data, BLCKSZ),
		CLUSTER_SPACE_IDENTITY_ALREADY);
	UT_ASSERT(memcmp(page.data, saved.data, BLCKSZ) == 0);
}

UT_TEST(test_same_token_wrong_bytes_and_numeric_larger_are_not_proof)
{
	ClusterSpaceReservationChange c = advance();
	ClusterSpaceReservation bad;
	PGAlignedBlock page = {0}, saved;

	for (int variant = 0; variant < 4; variant++) {
		uint64 token = c.result_token;

		bad = c.result;
		if (variant == 0) bad.next_block++;
		if (variant == 1) bad.identity.incarnation[0]++;
		if (variant == 2) token = 9000;
		if (variant == 3) { bad = c.before; bad.next_block--; token = c.before_token; }
		UT_ASSERT(cluster_space_reservation_page_encode(&bad, token, page.data, BLCKSZ));
		saved = page;
		UT_ASSERT_EQ(cluster_space_reservation_apply(&c, &c.result.identity.key, page.data, BLCKSZ),
			CLUSTER_SPACE_IDENTITY_MISMATCH);
		UT_ASSERT(memcmp(page.data, saved.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_exhaustion_overflow_and_structural_identity)
{
	ClusterSpaceReservationChange c = advance(), decoded = {0};
	uint8 wal[CLUSTER_SPACE_RESERVATION_WAL_BYTES] = {0};
	PGAlignedBlock page = {0};

	c.before.next_block = c.first_block = MaxBlockNumber;
	c.granted = 1;
	c.result.next_block = InvalidBlockNumber;
	UT_ASSERT(cluster_space_reservation_wal_encode(&c, wal, sizeof(wal)));
	UT_ASSERT(cluster_space_reservation_wal_decode(wal, sizeof(wal), &decoded));
	c.granted = 2;
	c.result.next_block = 0;
	UT_ASSERT(!cluster_space_reservation_wal_encode(&c, wal, sizeof(wal)));
	c = advance();
	c.action = CLUSTER_SPACE_RESERVATION_RESET;
	c.first_block = c.result.next_block = 3;
	c.granted = 0;
	c.result.identity.sequence++;
	c.result.identity.operation++;
	memset(c.result.identity.incarnation, 0x42, 16);
	UT_ASSERT(cluster_space_reservation_page_encode(&c.before, c.before_token, page.data, BLCKSZ));
	UT_ASSERT_EQ(cluster_space_reservation_apply(&c, &c.result.identity.key, page.data, BLCKSZ),
		CLUSTER_SPACE_IDENTITY_APPLY);
	memcpy(c.result.identity.incarnation, c.before.identity.incarnation, 16);
	UT_ASSERT(!cluster_space_reservation_wal_encode(&c, wal, sizeof(wal)));
	c = advance();
	c.action = CLUSTER_SPACE_RESERVATION_TOMBSTONE;
	c.first_block = c.granted = 0;
	c.result.next_block = c.before.next_block;
	c.result.identity.sequence++;
	c.result.identity.operation++;
	c.result.identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
	UT_ASSERT(cluster_space_reservation_page_encode(&c.before, c.before_token, page.data, BLCKSZ));
	UT_ASSERT_EQ(cluster_space_reservation_apply(&c, &c.result.identity.key, page.data, BLCKSZ),
		CLUSTER_SPACE_IDENTITY_APPLY);
	c = advance();
	c.before.identity.state = c.result.identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
	UT_ASSERT(!cluster_space_reservation_wal_encode(&c, wal, sizeof(wal)));
}

UT_TEST(test_wal_and_page_refusals_preserve_outputs)
{
	ClusterSpaceReservationChange c = advance(), out, sentinel;
	ClusterSpaceReservation decoded, saved;
	uint8 wal[CLUSTER_SPACE_RESERVATION_WAL_BYTES] = {0};
	uint8 broken[CLUSTER_SPACE_RESERVATION_WAL_BYTES];
	const unsigned offsets[] = {0, 4, 6, 8, 12, 16, 20, 40, 48, 207, 208, 367};
	PGAlignedBlock page = {0};
	uint64 token = 99;

	memset(&sentinel, 0xa5, sizeof(sentinel));
	memset(&saved, 0xa5, sizeof(saved));
	UT_ASSERT(cluster_space_reservation_wal_encode(&c, wal, sizeof(wal)));
	for (size_t i = 0; i < lengthof(offsets); i++) {
		memcpy(broken, wal, sizeof(wal));
		broken[offsets[i]] ^= 0x80;
		out = sentinel;
		UT_ASSERT(!cluster_space_reservation_wal_decode(broken, sizeof(broken), &out));
		UT_ASSERT(memcmp(&out, &sentinel, sizeof(out)) == 0);
	}
	UT_ASSERT(cluster_space_reservation_page_encode(&c.before, 201, page.data, BLCKSZ));
	page.data[BLCKSZ - 1] = 1;
	decoded = saved;
	UT_ASSERT(!cluster_space_reservation_page_decode(page.data, BLCKSZ, SPACE_FORKNUM, 1,
		&c.before.identity.key, &decoded, &token));
	UT_ASSERT(memcmp(&decoded, &saved, sizeof(decoded)) == 0);
	UT_ASSERT_EQ(token, 99);
	memset(&c, 0, sizeof(c));
	c.action = CLUSTER_SPACE_RESERVATION_INIT;
	c.result = state();
	c.result_token = 17;
	UT_ASSERT(cluster_space_reservation_wal_encode(&c, wal, sizeof(wal)));
	wal[48] = 1;
	out = sentinel;
	UT_ASSERT(!cluster_space_reservation_wal_decode(wal, sizeof(wal), &out));
	UT_ASSERT(memcmp(&out, &sentinel, sizeof(out)) == 0);
}

static ClusterSpaceStructureChange
structure(bool create)
{
	ClusterSpaceStructureChange c = {0};

	c.identity.action = create ? CLUSTER_SPACE_WAL_CREATE : CLUSTER_SPACE_WAL_TRUNCATE;
	c.identity.nblocks = create ? InvalidBlockNumber : 4;
	c.identity.result = state().identity;
	c.identity.result_token = 211;
	c.reservation.action = create ? CLUSTER_SPACE_RESERVATION_INIT : CLUSTER_SPACE_RESERVATION_RESET;
	c.reservation.result_token = 211;
	if (!create) {
		c.identity.expected = c.identity.result;
		c.identity.before_token = 17;
		c.identity.result.sequence++;
		c.identity.result.operation++;
		c.identity.result.incarnation[0]++;
		c.reservation.before.identity = c.identity.expected;
		c.reservation.before.next_block = 10;
		c.reservation.before_token = 99;
		c.reservation.first_block = c.reservation.result.next_block = 4;
	}
	c.reservation.result.identity = c.identity.result;
	return c;
}

UT_TEST(test_structural_record_binds_both_identities_and_actions)
{
	ClusterSpaceStructureChange c = structure(true), out = {0};
	uint8 bytes[CLUSTER_SPACE_STRUCTURE_WAL_BYTES] = {0};
	uint8 saved[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];

	UT_ASSERT(cluster_space_structure_wal_encode(&c, bytes, sizeof(bytes)));
	UT_ASSERT(cluster_space_structure_wal_decode(bytes, sizeof(bytes), &out));
	UT_ASSERT_EQ(out.reservation.result_token, 211);
	UT_ASSERT(!cluster_space_structure_wal_decode(bytes, CLUSTER_SPACE_WAL_BYTES, &out));
	c = structure(false);
	UT_ASSERT(cluster_space_structure_wal_encode(&c, bytes, sizeof(bytes)));
	UT_ASSERT(cluster_space_structure_wal_decode(bytes, sizeof(bytes), &out));
	memcpy(saved, bytes, sizeof(saved));
	c.reservation.result_token++;
	UT_ASSERT(!cluster_space_structure_wal_encode(&c, bytes, sizeof(bytes)));
	UT_ASSERT(memcmp(bytes, saved, sizeof(saved)) == 0);
	c = structure(false);
	c.reservation.result.identity.incarnation[1]++;
	/* Both subrecords are independently valid; their combined identity is not. */
	UT_ASSERT(cluster_space_reservation_wal_encode(&c.reservation, bytes + CLUSTER_SPACE_WAL_BYTES,
		CLUSTER_SPACE_RESERVATION_WAL_BYTES));
	memset(&out, 0xa5, sizeof(out));
	c = out;
	UT_ASSERT(!cluster_space_structure_wal_decode(bytes, sizeof(bytes), &out));
	UT_ASSERT(memcmp(&c, &out, sizeof(out)) == 0);
	c = structure(false);
	c.identity.nblocks++;
	UT_ASSERT(!cluster_space_structure_wal_encode(&c, bytes, sizeof(bytes)));
}

UT_TEST(test_structural_preflight_never_partially_changes_outputs)
{
	ClusterSpaceStructureChange c = structure(false);
	PGAlignedBlock pages[2] = {{0}}, saved[2];
	uint8 mask = 55;

	UT_ASSERT(cluster_space_identity_page_encode(&c.identity.expected, c.identity.before_token,
		pages[0].data, BLCKSZ));
	UT_ASSERT(cluster_space_reservation_page_encode(&c.reservation.before, 123, pages[1].data, BLCKSZ));
	memcpy(saved, pages, sizeof(pages));
	UT_ASSERT_EQ(cluster_space_structure_apply(&c, &c.identity.result.key, pages[0].data,
		pages[1].data, BLCKSZ, &mask), CLUSTER_SPACE_IDENTITY_MISMATCH);
	UT_ASSERT(memcmp(saved, pages, sizeof(pages)) == 0);
	UT_ASSERT_EQ(mask, 55);
	UT_ASSERT_EQ(cluster_space_structure_apply(&c, &c.identity.result.key, pages[0].data,
		pages[0].data, BLCKSZ, &mask), CLUSTER_SPACE_IDENTITY_INVALID);
	UT_ASSERT(memcmp(saved, pages, sizeof(pages)) == 0);
}

UT_TEST(test_structural_partial_restart_and_init)
{
	ClusterSpaceStructureChange c = structure(true);
	PGAlignedBlock pages[2] = {{0}};
	uint8 mask = 0;

	UT_ASSERT_EQ(cluster_space_structure_apply(&c, &c.identity.result.key, pages[0].data,
		pages[1].data, BLCKSZ, &mask), CLUSTER_SPACE_IDENTITY_APPLY);
	UT_ASSERT_EQ(mask, 3);
	UT_ASSERT_EQ(cluster_space_structure_apply(&c, &c.identity.result.key, pages[0].data,
		pages[1].data, BLCKSZ, &mask), CLUSTER_SPACE_IDENTITY_ALREADY);
	UT_ASSERT_EQ(mask, 0);
	c = structure(false);
	for (unsigned installed = 0; installed < 4; installed++) {
		UT_ASSERT(cluster_space_identity_page_encode(
			installed & 1 ? &c.identity.result : &c.identity.expected,
			installed & 1 ? c.identity.result_token : c.identity.before_token, pages[0].data, BLCKSZ));
		UT_ASSERT(cluster_space_reservation_page_encode(
			installed & 2 ? &c.reservation.result : &c.reservation.before,
			installed & 2 ? c.reservation.result_token : c.reservation.before_token, pages[1].data, BLCKSZ));
		UT_ASSERT_EQ(cluster_space_structure_apply(&c, &c.identity.result.key, pages[0].data,
			pages[1].data, BLCKSZ, &mask),
			installed == 3 ? CLUSTER_SPACE_IDENTITY_ALREADY : CLUSTER_SPACE_IDENTITY_APPLY);
		UT_ASSERT_EQ(mask, 3 ^ installed);
	}
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(test_literal_unaligned_payload_and_wal);
	UT_RUN(test_crc_reserved_namespace_and_refusal_atomicity);
	UT_RUN(test_exact_page_class_and_header);
	UT_RUN(test_init_advance_and_exact_idempotence);
	UT_RUN(test_same_token_wrong_bytes_and_numeric_larger_are_not_proof);
	UT_RUN(test_exhaustion_overflow_and_structural_identity);
	UT_RUN(test_wal_and_page_refusals_preserve_outputs);
	UT_RUN(test_structural_record_binds_both_identities_and_actions);
	UT_RUN(test_structural_preflight_never_partially_changes_outputs);
	UT_RUN(test_structural_partial_restart_and_init);
	UT_DONE();
	return ut_failed_count != 0;
}
