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

typedef struct RecoveryFixture {
	uint8 wal[5][CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	ClusterSpaceRecoveryInput input[5];
	ClusterSpaceReservationChange changes[5];
	ClusterSpaceWalChange identities[5];
	PGAlignedBlock pages[6][2];
} RecoveryFixture;

static void
recovery_fixture(RecoveryFixture *f)
{
	const uint64 tokens[] = {901, 80, 1200, 55, 12};
	uint64 identity_token = 0;
	const uint32 permutation[] = {2, 4, 1, 3, 0};

	memset(f, 0, sizeof(*f));
	for (uint32 i = 0; i < 5; i++) {
		ClusterSpaceReservationChange *c = &f->changes[i];
		ClusterSpaceWalChange *id = &f->identities[i];
		size_t len;

		c->result_token = tokens[i];
		if (i == 0) {
			c->action = CLUSTER_SPACE_RESERVATION_INIT;
			c->result = state();
			id->action = CLUSTER_SPACE_WAL_CREATE;
		} else {
			c->before = f->changes[i - 1].result;
			c->before_token = tokens[i - 1];
			c->result = c->before;
			if (i == 1 || i == 3) {
				c->action = CLUSTER_SPACE_RESERVATION_ADVANCE;
				c->first_block = c->before.next_block;
				c->granted = 9;
				c->result.next_block += 9;
			} else {
				c->result.identity.sequence++;
				c->result.identity.operation++;
				if (i == 2) {
					c->action = CLUSTER_SPACE_RESERVATION_RESET;
					c->result.identity.incarnation[15]++;
					c->first_block = c->result.next_block = 3;
					id->action = CLUSTER_SPACE_WAL_TRUNCATE;
				} else {
					c->action = CLUSTER_SPACE_RESERVATION_TOMBSTONE;
					c->result.identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
					id->action = CLUSTER_SPACE_WAL_TOMBSTONE;
				}
			}
		}
		f->pages[i + 1][0] = f->pages[i][0];
		if (id->action != 0) {
			ClusterSpaceStructureChange both;

			id->expected = c->before.identity;
			id->result = c->result.identity;
			id->before_token = identity_token;
			id->result_token = c->result_token;
			id->nblocks = i == 2 ? c->result.next_block : InvalidBlockNumber;
			identity_token = c->result_token;
			both.identity = *id;
			both.reservation = *c;
			len = CLUSTER_SPACE_STRUCTURE_WAL_BYTES;
			if (!cluster_space_structure_wal_encode(&both, f->wal[i], len)
				|| !cluster_space_identity_page_encode(&id->result, identity_token,
					f->pages[i + 1][0].data, BLCKSZ))
				abort();
		} else {
			len = CLUSTER_SPACE_RESERVATION_WAL_BYTES;
			if (!cluster_space_reservation_wal_encode(c, f->wal[i], len))
				abort();
		}
		if (!cluster_space_reservation_page_encode(&c->result, c->result_token,
			f->pages[i + 1][1].data, BLCKSZ))
			abort();
		for (uint32 j = 0; j < 5; j++)
			if (permutation[j] == i)
				f->input[j] = (ClusterSpaceRecoveryInput){f->wal[i], len};
	}
}

UT_TEST(test_recovery_closed_chain_and_independent_partial_pages)
{
	RecoveryFixture f;
	const uint32 expected_order[] = {4, 2, 0, 3, 1};
	ClusterSpaceRecoveryImage out = {0};
	uint32 order[5] = {0};

	recovery_fixture(&f);
	for (int id = 0; id < 6; id++)
		for (int reservation = 0; reservation < 6; reservation++) {
			UT_ASSERT(cluster_space_recovery_prepare(f.input, 5,
				&f.changes[0].result.identity.key, f.pages[id][0].data,
				f.pages[reservation][1].data, order, &out));
			UT_ASSERT(memcmp(order, expected_order, sizeof(order)) == 0);
			UT_ASSERT(memcmp(out.pages, f.pages[5], sizeof(out.pages)) == 0);
			UT_ASSERT_EQ(out.apply_mask, (id == 5 ? 0 : 1) | (reservation == 5 ? 0 : 2));
			UT_ASSERT_EQ(out.source_index[0], 1);
			UT_ASSERT_EQ(out.source_index[1], 1);
		}
}

UT_TEST(test_recovery_missing_duplicate_fork_and_cycle_refuse)
{
	RecoveryFixture f;
	ClusterSpaceRecoveryImage out, saved;
	uint32 order[5], saved_order[5];
	ClusterSpaceReservationChange extra;
	uint8 wal[CLUSTER_SPACE_RESERVATION_WAL_BYTES];

	memset(&saved, 0xa5, sizeof(saved));
	memset(saved_order, 0x5a, sizeof(saved_order));
	for (int variant = 0; variant < 5; variant++) {
		uint32 count = 5;

		recovery_fixture(&f);
		if (variant == 0) { f.input[0] = f.input[4]; count = 4; } /* missing RESET */
		if (variant == 1) f.input[0] = f.input[1]; /* duplicate */
		if (variant >= 2) {
			extra = f.changes[1];
			if (variant == 2) extra.result_token = 999; /* same before: fork */
			if (variant == 3) { extra.before_token = 12; extra.result_token = 901; } /* cycle */
			if (variant == 4) extra.result_token = 1200; /* same result: merge */
			UT_ASSERT(cluster_space_reservation_wal_encode(&extra, wal, sizeof(wal)));
			f.input[4] = (ClusterSpaceRecoveryInput){wal, sizeof(wal)};
		}
		out = saved;
		memcpy(order, saved_order, sizeof(order));
		UT_ASSERT(!cluster_space_recovery_prepare(f.input, count,
			&f.changes[0].result.identity.key, f.pages[1][0].data,
			f.pages[1][1].data, order, &out));
		UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
		UT_ASSERT(memcmp(order, saved_order, sizeof(order)) == 0);
	}
}

UT_TEST(test_recovery_unknown_target_and_namespace_preserve_outputs)
{
	RecoveryFixture f;
	ClusterSpaceRecoveryImage out, saved;
	uint32 order[5], saved_order[5];

	memset(&saved, 0xc7, sizeof(saved));
	memset(saved_order, 0xa1, sizeof(saved_order));
	for (int variant = 0; variant < 6; variant++) {
		ClusterSpaceIdentityKey key;
		PGAlignedBlock target[2];
		ClusterSpaceReservation wrong;

		recovery_fixture(&f);
		key = f.changes[0].result.identity.key;
		memcpy(target, f.pages[3], sizeof(target));
		wrong = f.changes[2].result;
		if (variant == 0) key.database_incarnation++;
		if (variant == 1) ((PageHeader)target[1].data)->pd_block_scn = UINT64_MAX;
		if (variant == 2) {
			wrong.next_block++;
			UT_ASSERT(cluster_space_reservation_page_encode(&wrong, 1200, target[1].data, BLCKSZ));
		}
		if (variant == 3) {
			wrong.identity.operation++;
			UT_ASSERT(cluster_space_identity_page_encode(&wrong.identity, 1200, target[0].data, BLCKSZ));
		}
		if (variant == 4) target[1].data[BLCKSZ - 1] = 1;
		if (variant == 5) f.wal[1][8] = CLUSTER_SPACE_RESERVATION_INIT;
		out = saved;
		memcpy(order, saved_order, sizeof(order));
		UT_ASSERT(!cluster_space_recovery_prepare(f.input, 5, &key,
			target[0].data, target[1].data, order, &out));
		UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
		UT_ASSERT(memcmp(order, saved_order, sizeof(order)) == 0);
	}
}

UT_TEST(test_recovery_advance_suffix_preserves_unchanged_identity)
{
	RecoveryFixture f;
	ClusterSpaceRecoveryImage out;
	uint32 order[1];
	PGAlignedBlock identity;

	recovery_fixture(&f);
	identity = f.pages[1][0];
	((PageHeader)identity.data)->pd_block_scn = 8001;
	PageXLogRecPtrSet(((PageHeader)identity.data)->pd_lsn, UINT64_C(0x9000));
	for (int target = 1; target <= 2; target++) {
		UT_ASSERT(cluster_space_recovery_prepare(&f.input[2], 1,
			&f.changes[0].result.identity.key, identity.data,
			f.pages[target][1].data, order, &out));
		UT_ASSERT(memcmp(out.pages[0].data, identity.data, BLCKSZ) == 0);
		UT_ASSERT(memcmp(out.pages[1].data, f.pages[2][1].data, BLCKSZ) == 0);
		UT_ASSERT_EQ(out.source_index[0], UINT32_MAX);
		UT_ASSERT_EQ(out.source_index[1], 0);
		UT_ASSERT_EQ(out.apply_mask, target == 2 ? 0 : 2);
	}
	UT_ASSERT(!cluster_space_recovery_prepare(&f.input[2], 1,
		&f.changes[0].result.identity.key, f.pages[3][0].data,
		f.pages[1][1].data, order, &out));
}

UT_TEST(test_recovery_requires_both_typed_before_chains)
{
	RecoveryFixture f;
	ClusterSpaceRecoveryImage out;
	ClusterSpaceStructureChange wrong;
	uint32 order[5];

	recovery_fixture(&f);
	wrong.identity = f.identities[2];
	wrong.reservation = f.changes[2];
	wrong.identity.before_token = 777;
	UT_ASSERT(cluster_space_structure_wal_encode(&wrong, f.wal[2], sizeof(f.wal[2])));
	UT_ASSERT(!cluster_space_recovery_prepare(f.input, 5, &wrong.identity.result.key,
		f.pages[1][0].data, f.pages[1][1].data, order, &out));
	recovery_fixture(&f);
	wrong.identity = f.identities[2];
	wrong.reservation = f.changes[2];
	wrong.reservation.before.next_block++;
	UT_ASSERT(cluster_space_structure_wal_encode(&wrong, f.wal[2], sizeof(f.wal[2])));
	UT_ASSERT(!cluster_space_recovery_prepare(f.input, 5, &wrong.identity.result.key,
		f.pages[1][0].data, f.pages[1][1].data, order, &out));
}

UT_TEST(test_source_order_never_needs_or_certifies_target_pages)
{
	RecoveryFixture f;
	uint32 order[5] = { 99, 99, 99, 99, 99 };
	const uint32 expected_order[5] = { 4, 2, 0, 3, 1 };

	recovery_fixture(&f);
	UT_ASSERT(cluster_space_recovery_order(f.input, 5, &f.changes[0].result.identity.key, order));
	UT_ASSERT(memcmp(order, expected_order, sizeof(order)) == 0);
	/* An ADVANCE-only cut never supplies an identity-page token. */
	order[0] = 99;
	UT_ASSERT(
		cluster_space_recovery_order(&f.input[2], 1, &f.changes[0].result.identity.key, order));
	UT_ASSERT_EQ(order[0], 0);
}

UT_TEST(test_source_order_refuses_missing_or_inconsistent_chain_atomically)
{
	RecoveryFixture f;
	uint32 order[5] = { 99, 99, 99, 99, 99 }, saved[5];
	ClusterSpaceStructureChange wrong;
	ClusterSpaceRecoveryInput missing[4];

	recovery_fixture(&f);
	memcpy(saved, order, sizeof(order));
	missing[0] = f.input[0];
	missing[1] = f.input[1];
	missing[2] = f.input[3];
	missing[3] = f.input[4];
	UT_ASSERT(!cluster_space_recovery_order(missing, 4, &f.changes[0].result.identity.key, order));
	UT_ASSERT(memcmp(order, saved, sizeof(order)) == 0);
	wrong.identity = f.identities[2];
	wrong.reservation = f.changes[2];
	wrong.identity.before_token = 777;
	UT_ASSERT(cluster_space_structure_wal_encode(&wrong, f.wal[2], sizeof(f.wal[2])));
	UT_ASSERT(!cluster_space_recovery_order(f.input, 5, &f.changes[0].result.identity.key, order));
	UT_ASSERT(memcmp(order, saved, sizeof(order)) == 0);
}

UT_TEST(test_recovery_interleaved_advances_and_later_target)
{
	ClusterSpaceReservationChange changes[2] = { advance(), advance() };
	uint8 wal[2][CLUSTER_SPACE_RESERVATION_WAL_BYTES];
	ClusterSpaceRecoveryInput input[2];
	ClusterSpaceRecoveryImage out;
	PGAlignedBlock id, target;
	uint32 order[2] = { 77, 77 };

	/* Source A 11->18, survivor 18->25, source C 25->32. Arrival is
	 * reversed and opaque tokens decrease. The survivor WAL is not input. */
	changes[1].before.next_block = changes[1].first_block = 25;
	changes[1].result.next_block = 32;
	changes[1].before_token = 7;
	changes[1].result_token = 3;
	for (int i = 0; i < 2; i++) {
		UT_ASSERT(cluster_space_reservation_wal_encode(&changes[i], wal[i], sizeof(wal[i])));
		input[1 - i] = (ClusterSpaceRecoveryInput){ wal[i], sizeof(wal[i]) };
	}
	UT_ASSERT(cluster_space_recovery_order(input, 2, &changes[0].before.identity.key, order));
	UT_ASSERT_EQ(order[0], 1);
	UT_ASSERT_EQ(order[1], 0);
	UT_ASSERT(
		cluster_space_identity_page_encode(&changes[0].before.identity, 9001, id.data, BLCKSZ));
	for (int variant = 0; variant < 3; variant++) {
		ClusterSpaceReservation at = variant == 0 ? changes[1].before : changes[1].result;
		uint64 token = variant == 0 ? 7 : 3;
		if (variant == 2) {
			at.next_block = 39;
			token = 1;
		}
		UT_ASSERT(cluster_space_reservation_page_encode(&at, token, target.data, BLCKSZ));
		memset(&out, 0, sizeof(out));
		UT_ASSERT(cluster_space_recovery_prepare(input, 2, &at.identity.key, id.data, target.data,
												 order, &out));
		UT_ASSERT_EQ(out.covered_by_successor_mask, 2);
		UT_ASSERT_EQ(out.source_index[0], UINT32_MAX);
		UT_ASSERT_EQ(out.source_index[1], variant == 2 ? UINT32_MAX : 0);
		UT_ASSERT_EQ(out.apply_mask, variant == 0 ? 2 : 0);
		UT_ASSERT(memcmp(out.pages[0].data, id.data, BLCKSZ) == 0);
		if (variant != 0)
			UT_ASSERT(memcmp(out.pages[1].data, target.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_recovery_successor_does_not_cover_gaps_ahead_or_conflicts)
{
	for (int variant = 0; variant < 7; variant++) {
		ClusterSpaceReservationChange changes[2] = { advance(), advance() };
		ClusterSpaceReservation at;
		uint8 wal[2][CLUSTER_SPACE_RESERVATION_WAL_BYTES];
		ClusterSpaceRecoveryInput input[2];
		ClusterSpaceRecoveryImage out, saved;
		PGAlignedBlock id, target;
		uint32 order[2] = { 77, 77 };
		uint64 token = 1;
		changes[1].before.next_block = changes[1].first_block = 25;
		changes[1].result.next_block = 32;
		changes[1].before_token = 7;
		changes[1].result_token = 3;
		at = changes[0].before;
		at.next_block = 39;
		if (variant == 0) {
			at = changes[0].before;
			token = changes[0].before_token;
		}
		if (variant == 1) {
			at.next_block = 32;
			token = 2;
		} /* equal HWM, wrong version */
		if (variant == 2)
			token = changes[0].result_token; /* known token, false bytes */
		if (variant == 3) {
			changes[1].before.next_block = changes[1].first_block = 17;
			changes[1].result.next_block = 24; /* overlapping allocations */
		}
		if (variant == 4)
			changes[1].before_token = changes[0].result_token; /* false link */
		if (variant == 5)
			changes[1].result_token = changes[0].result_token; /* merge */
		if (variant == 6) {
			changes[1].before.identity.incarnation[0]++;
			changes[1].result.identity = changes[1].before.identity;
		}
		for (int i = 0; i < 2; i++) {
			UT_ASSERT(cluster_space_reservation_wal_encode(&changes[i], wal[i], sizeof(wal[i])));
			input[i] = (ClusterSpaceRecoveryInput){ wal[i], sizeof(wal[i]) };
		}
		UT_ASSERT(cluster_space_identity_page_encode(&at.identity, 9001, id.data, BLCKSZ));
		UT_ASSERT(cluster_space_reservation_page_encode(&at, token, target.data, BLCKSZ));
		memset(&saved, 0xa5, sizeof(saved));
		out = saved;
		UT_ASSERT(!cluster_space_recovery_prepare(input, 2, &at.identity.key, id.data, target.data,
												  order, &out));
		UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
		UT_ASSERT_EQ(order[0], 77);
		UT_ASSERT_EQ(order[1], 77);
	}
}

UT_TEST(test_recovery_through_never_installs_a_later_structure_or_rewinds_a_component)
{
	static const struct {
		uint32 through, id, reservation, want_id, want_reservation;
		uint8 apply, covered;
		uint32 id_source, reservation_source;
	} cases[]
		= { { 0, 0, 0, 1, 1, 3, 0, 4, 4 },			{ 1, 1, 1, 1, 2, 2, 0, 4, 2 },
			{ 2, 2, 2, 3, 3, 3, 0, 0, 0 },			{ 3, 3, 3, 3, 4, 2, 0, 0, 3 },
			{ 4, 4, 4, 5, 5, 3, 0, 1, 1 },			{ 0, 3, 4, 3, 4, 0, 3, UINT32_MAX, UINT32_MAX },
			{ 2, 5, 2, 5, 3, 2, 1, UINT32_MAX, 0 }, { 2, 1, 5, 3, 5, 1, 2, 0, UINT32_MAX },
			{ 2, 3, 4, 3, 4, 0, 2, 0, UINT32_MAX }, { 4, 5, 5, 5, 5, 0, 0, 1, 1 } };
	const uint32 expected_order[] = { 4, 2, 0, 3, 1 };
	RecoveryFixture f;
	ClusterSpaceRecoveryImage out;
	uint32 order[5];

	recovery_fixture(&f);
	for (uint32 i = 0; i < lengthof(cases); i++) {
		UT_ASSERT(cluster_space_recovery_prepare_through(
			f.input, 5, cases[i].through, &f.changes[0].result.identity.key,
			f.pages[cases[i].id][0].data, f.pages[cases[i].reservation][1].data, order, &out));
		UT_ASSERT(memcmp(order, expected_order, sizeof(order)) == 0);
		UT_ASSERT(memcmp(out.pages[0].data, f.pages[cases[i].want_id][0].data, BLCKSZ) == 0);
		UT_ASSERT(memcmp(out.pages[1].data, f.pages[cases[i].want_reservation][1].data, BLCKSZ)
				  == 0);
		UT_ASSERT_EQ(out.apply_mask, cases[i].apply);
		UT_ASSERT_EQ(out.covered_by_successor_mask, cases[i].covered);
		UT_ASSERT_EQ(out.source_index[0], cases[i].id_source);
		UT_ASSERT_EQ(out.source_index[1], cases[i].reservation_source);
	}
}

UT_TEST(test_recovery_through_still_checks_the_complete_future_chain)
{
	RecoveryFixture f;
	ClusterSpaceRecoveryImage out, saved;
	uint32 order[5], saved_order[5];

	memset(&saved, 0xa5, sizeof(saved));
	memset(saved_order, 0x5a, sizeof(saved_order));
	for (int variant = 0; variant < 5; variant++) {
		uint32 through = 0;
		recovery_fixture(&f);
		if (variant == 0)
			through = 5;
		if (variant == 1)
			through = UINT32_MAX;
		if (variant == 2)
			f.wal[4][12] ^= 1;
		if (variant == 3)
			f.input[1] = f.input[3];
		if (variant == 4)
			((PageHeader)f.pages[4][1].data)->pd_block_scn = 777;
		out = saved;
		memcpy(order, saved_order, sizeof(order));
		UT_ASSERT(!cluster_space_recovery_prepare_through(
			f.input, 5, through, &f.changes[0].result.identity.key, f.pages[1][0].data,
			f.pages[4][1].data, order, &out));
		UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
		UT_ASSERT(memcmp(order, saved_order, sizeof(order)) == 0);
	}
}

UT_TEST(test_recovery_through_advances_preserves_survivor_bytes_and_gap_proof)
{
	static const struct {
		BlockNumber second_start, target_hwm;
		uint64 target_token;
		bool ok;
		uint8 apply, covered;
		uint32 source;
	} cases[] = { { 18, 11, 201, true, 2, 0, 1 },		 { 18, 18, 101, true, 0, 0, 1 },
				  { 18, 25, 3, true, 0, 2, UINT32_MAX }, { 25, 25, 7, true, 0, 2, UINT32_MAX },
				  { 25, 39, 1, true, 0, 2, UINT32_MAX }, { 25, 11, 201, false, 0, 0, 0 },
				  { 18, 25, 2, false, 0, 0, 0 } };

	for (uint32 i = 0; i < lengthof(cases); i++) {
		ClusterSpaceReservationChange changes[2] = { advance(), advance() };
		uint8 wal[2][CLUSTER_SPACE_RESERVATION_WAL_BYTES];
		ClusterSpaceRecoveryInput input[2];
		ClusterSpaceRecoveryImage out, saved;
		ClusterSpaceReservation target = changes[0].before;
		PGAlignedBlock pages[2], first_result;
		uint32 order[2] = { 77, 77 };

		changes[1].before.next_block = changes[1].first_block = cases[i].second_start;
		changes[1].result.next_block = cases[i].second_start + 7;
		changes[1].before_token = cases[i].second_start == 18 ? 101 : 7;
		changes[1].result_token = 3;
		for (int j = 0; j < 2; j++) {
			UT_ASSERT(cluster_space_reservation_wal_encode(&changes[j], wal[j], sizeof(wal[j])));
			input[1 - j] = (ClusterSpaceRecoveryInput){ wal[j], sizeof(wal[j]) };
		}
		target.next_block = cases[i].target_hwm;
		UT_ASSERT(
			cluster_space_identity_page_encode(&target.identity, 9001, pages[0].data, BLCKSZ));
		UT_ASSERT(cluster_space_reservation_page_encode(&target, cases[i].target_token,
														pages[1].data, BLCKSZ));
		UT_ASSERT(cluster_space_reservation_page_encode(&changes[0].result, 101, first_result.data,
														BLCKSZ));
		PageXLogRecPtrSet(((PageHeader)pages[0].data)->pd_lsn, UINT64_C(0x9900));
		PageXLogRecPtrSet(((PageHeader)pages[1].data)->pd_lsn, UINT64_C(0x8800));
		memset(&saved, 0xa5, sizeof(saved));
		out = saved;
		UT_ASSERT_EQ(cluster_space_recovery_prepare_through(input, 2, 0, &target.identity.key,
															pages[0].data, pages[1].data, order,
															&out),
					 cases[i].ok);
		if (!cases[i].ok) {
			UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
			UT_ASSERT_EQ(order[0], 77);
			UT_ASSERT_EQ(order[1], 77);
			continue;
		}
		UT_ASSERT_EQ(order[0], 1);
		UT_ASSERT_EQ(order[1], 0);
		UT_ASSERT_EQ(out.source_index[0], UINT32_MAX);
		UT_ASSERT_EQ(out.source_index[1], cases[i].source);
		UT_ASSERT_EQ(out.apply_mask, cases[i].apply);
		UT_ASSERT_EQ(out.covered_by_successor_mask, cases[i].covered);
		UT_ASSERT(memcmp(out.pages[0].data, pages[0].data, BLCKSZ) == 0);
		UT_ASSERT(
			memcmp(out.pages[1].data, cases[i].apply ? first_result.data : pages[1].data, BLCKSZ)
			== 0);
	}
}

UT_TEST(test_recovery_through_before_first_structure_does_not_borrow_its_identity_source)
{
	RecoveryFixture f;
	ClusterSpaceRecoveryImage out;
	uint32 order[4];
	const uint32 expected_order[] = { 2, 0, 3, 1 };

	recovery_fixture(&f);
	/* The first four inputs omit CREATE. ADVANCE precedes the first
	 * structural operation; the identity page still belongs to CREATE. */
	UT_ASSERT(cluster_space_recovery_prepare_through(
		f.input, 4, 0, &f.changes[0].result.identity.key, f.pages[1][0].data, f.pages[1][1].data,
		order, &out));
	UT_ASSERT(memcmp(order, expected_order, sizeof(order)) == 0);
	UT_ASSERT_EQ(out.source_index[0], UINT32_MAX);
	UT_ASSERT_EQ(out.source_index[1], 2);
	UT_ASSERT_EQ(out.apply_mask, 2);
	UT_ASSERT_EQ(out.covered_by_successor_mask, 0);
	UT_ASSERT(memcmp(out.pages, f.pages[2], sizeof(out.pages)) == 0);
}

int
main(void)
{
	UT_PLAN(23);
	UT_RUN(test_recovery_through_advances_preserves_survivor_bytes_and_gap_proof);
	UT_RUN(test_recovery_through_before_first_structure_does_not_borrow_its_identity_source);
	UT_RUN(test_recovery_through_never_installs_a_later_structure_or_rewinds_a_component);
	UT_RUN(test_recovery_through_still_checks_the_complete_future_chain);
	UT_RUN(test_recovery_interleaved_advances_and_later_target);
	UT_RUN(test_recovery_successor_does_not_cover_gaps_ahead_or_conflicts);
	UT_RUN(test_source_order_never_needs_or_certifies_target_pages);
	UT_RUN(test_source_order_refuses_missing_or_inconsistent_chain_atomically);
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
	UT_RUN(test_recovery_closed_chain_and_independent_partial_pages);
	UT_RUN(test_recovery_missing_duplicate_fork_and_cycle_refuse);
	UT_RUN(test_recovery_unknown_target_and_namespace_preserve_outputs);
	UT_RUN(test_recovery_advance_suffix_preserves_unchanged_identity);
	UT_RUN(test_recovery_requires_both_typed_before_chains);
	UT_DONE();
	return ut_failed_count != 0;
}
