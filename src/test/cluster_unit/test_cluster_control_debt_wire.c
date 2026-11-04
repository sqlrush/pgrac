/*-------------------------------------------------------------------------
 * test_cluster_control_debt_wire.c
 *     Complete debt vectors and original registry preservation.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#define TEST_CONTROL_REGISTRY 1
int debt_original_registry_main(void);
#define main debt_original_registry_main
#include "test_cluster_recovery_registry.c"
#undef main
#include "common/cluster_recovery_reservations.h"
#include "../../backend/cluster/cluster_control_debt_v2.inc"

#define DEBT_TEST_BUDGET (8 * 1024 * 1024)
static uint8 *debt_original;
static Size debt_original_bytes;
static uint8 debt_original_ref[128];
static ControlDebtWireEntryV1 debt_entries[130];
static ControlDebtWireBatchV1 debt_batches[2];
static ClusterRecoveryRegistryV3 debt_registry;
static ClusterRecoveryFileReservationV1 debt_reserved[2];

static void
debt_fixture(uint64 count)
{
	fixture();
	memset(&debt_registry, 0, sizeof(debt_registry));
	debt_registry.available = reg;
	debt_registry.catalog_version = CATALOG_VERSION_NO;
	debt_registry.reservation_count = 2;
	debt_registry.reservations = debt_reserved;
	for (unsigned i = 0; i < 2; i++) {
		memset(&debt_reserved[i], 0, sizeof(debt_reserved[i]));
		debt_reserved[i].expected = files[0].identity;
		debt_reserved[i].expected.kind = CLUSTER_RECOVERY_FILE_CLOG_V2;
		debt_reserved[i].expected.object_id = 7 + i;
		memset(debt_reserved[i].batch_uuid, 0x71, 16);
		debt_reserved[i].creation_owner = sources[0].source.claim.identity;
		debt_reserved[i].owner_generation = 1;
		debt_reserved[i].state = CLUSTER_RECOVERY_FILE_RESERVED_V1;
	}
	if (!cluster_recovery_registry_size_v3(&debt_registry, DEBT_TEST_BUDGET,
		&debt_original_bytes)) abort();
	debt_original = malloc(debt_original_bytes);
	if (!debt_original || !cluster_recovery_registry_encode_v3(&debt_registry,
		debt_original, debt_original_bytes, debt_original_ref)) abort();
	memset(debt_entries, 0, sizeof(debt_entries));
	memset(debt_batches, 0, sizeof(debt_batches));
	for (uint64 i = 0; i < count; i++) {
		ControlDebtWireEntryV1 *e = &debt_entries[i];
		e->entry.ordinal = i;
		e->entry.file = files[0].identity;
		e->entry.source = sources[0].source;
		e->entry.top_full_xid = FullTransactionIdFromU64(UINT64_C(0x200000003) + i);
		e->entry.object_generation = 100 + i;
		e->entry.page_count = 1;
		e->entry.retain_start = 80;
		e->entry.retain_end = 180;
		e->entry.owner = CLUSTER_RECOVERY_DEBT_OWNER_RELMAP_V2;
		e->entry.owner_generation = 1;
		e->pin.key.identity.system_identifier = 11;
		e->pin.key.identity.database_incarnation = 12;
		e->pin.key.identity.shared = true;
		e->pin.key.generation = 100 + i;
		e->pin.key.top_full_xid = U64FromFullTransactionId(e->entry.top_full_xid);
		e->pin.key.membership_epoch = 1;
		e->pin.key.admitted_incarnation = 40;
		e->pin.key.boot_incarnation = 40;
		e->pin.wal_end = 140;
		e->pin.occupied = true;
		e->pin.may_be_staged = true;
	}
	debt_batches[0].episode = 7;
	debt_batches[0].prepare = 16;
	debt_batches[0].basis = debt_original;
	debt_batches[0].basis_bytes = debt_original_bytes;
	memcpy(debt_batches[0].basis_ref, debt_original_ref, 128);
	debt_batches[0].entries = debt_entries;
	debt_batches[0].entry_count = count;
}
static void debt_fixture_end(void) { free(debt_original); debt_original = NULL; }
static bool
debt_encode(ControlDebtWireImageV1 **out, uint64 batches)
{
	return control_debt_wire_encode_v1(debt_original, debt_original_bytes,
		debt_original_ref, 16, 17, debt_batches, batches, DEBT_TEST_BUDGET, out);
}
static bool
debt_validate(ControlDebtWireImageV1 *w)
{
	return control_debt_wire_validate_v1(w->registry, w->registry_bytes, w->reference,
		w->pages, w->page_bytes, debt_batches, w->batch_count, DEBT_TEST_BUDGET);
}
UT_TEST(debt_wire_pages_are_not_a_batch_cap)
{
	static const uint64 counts[] = {1, 64, 65, 128, 129, 130};
	for (unsigned i = 0; i < lengthof(counts); i++) {
		ControlDebtWireImageV1 *w = NULL;
		debt_fixture(counts[i]);
		UT_ASSERT(debt_encode(&w, 1));
		if (w) {
			UT_ASSERT(debt_validate(w));
			UT_ASSERT_EQ(w->page_bytes, ((counts[i] + 63) / 64) * 33024);
			UT_ASSERT(memcmp(w->registry + 256, debt_original, debt_original_bytes) == 0);
			UT_ASSERT_EQ(debt_get(w->registry, 120, 8), 1);
			UT_ASSERT_EQ(debt_get(w->reference, 56, 8), counts[i]);
		}
		control_debt_wire_destroy_v1(&w);
		debt_fixture_end();
	}
}
UT_TEST(debt_wire_complete_multiple_batches_and_full_tt)
{
	ControlDebtWireImageV1 *w = NULL;
	debt_fixture(130);
	debt_batches[1] = debt_batches[0];
	debt_batches[0].entry_count = 65;
	debt_batches[1].episode++;
	debt_batches[1].entries += 65;
	debt_batches[1].entry_count = 65;
	for (unsigned i = 65; i < 130; i++) debt_entries[i].entry.ordinal -= 65;
	debt_entries[0].pin.has_tt_binding = true;
	debt_entries[0].pin.binding.segment_id = 4;
	debt_entries[0].pin.binding.segment_generation = 9;
	debt_entries[0].pin.binding.xid = 3;
	debt_entries[0].pin.binding.slot_offset = 2;
	debt_entries[0].pin.binding.slot_wrap = 1;
	debt_entries[0].pin.binding.origin_instance = 1;
	debt_entries[0].pin.binding.publish_state = CLUSTER_CANONICAL_TXN_PUBLISHED;
	debt_entries[0].pin.binding.active_lsn = 130;
	UT_ASSERT(debt_encode(&w, 2));
	if (w) {
		UT_ASSERT(debt_validate(w));
		UT_ASSERT_EQ(w->page_bytes, 4 * 33024);
		UT_ASSERT_EQ(debt_get(w->pages + 256 + 352, 88, 4), 4);
		UT_ASSERT_EQ(debt_get(w->pages + 256 + 352, 112, 8), 130);
	}
	control_debt_wire_destroy_v1(&w);
	debt_fixture_end();
}
UT_TEST(debt_wire_whole_budget_overflow_and_aliases)
{
	ControlDebtWireImageV1 *w = NULL;
	debt_fixture(65);
	UT_ASSERT(!control_debt_wire_encode_v1(debt_original, debt_original_bytes,
		debt_original_ref, 16, 17, debt_batches, 1, 65536, &w));
	UT_ASSERT(w == NULL);
	UT_ASSERT(!control_debt_wire_encode_v1(debt_original, debt_original_bytes,
		debt_original_ref, 16, 17, debt_batches, UINT64_MAX, DEBT_TEST_BUDGET, &w));
	UT_ASSERT(!control_debt_wire_encode_v1(debt_original, debt_original_bytes,
		debt_original_ref, 16, 17, debt_batches, 1, DEBT_TEST_BUDGET,
		(ControlDebtWireImageV1 **)debt_original));
	debt_batches[0].entry_count = UINT64_MAX;
	UT_ASSERT(!debt_encode(&w, 1));
	UT_ASSERT(w == NULL);
	debt_fixture_end();
}
UT_TEST(debt_wire_refuses_incomplete_basis_duplicate_key_and_unknown_tt)
{
	ControlDebtWireImageV1 *w = NULL;
	debt_fixture(2);
	debt_batches[0].basis_bytes--;
	UT_ASSERT(!debt_encode(&w, 1));
	debt_batches[0].basis_bytes++;
	debt_entries[1].pin.key = debt_entries[0].pin.key;
	debt_entries[1].entry.object_generation = debt_entries[0].entry.object_generation;
	debt_entries[1].entry.top_full_xid = debt_entries[0].entry.top_full_xid;
	UT_ASSERT(!debt_encode(&w, 1));
	debt_entries[1].pin.key.generation++;
	debt_entries[1].entry.object_generation++;
	debt_entries[0].pin.key.origin = 1; /* Complete source is still origin zero. */
	UT_ASSERT(!debt_encode(&w, 1));
	debt_entries[0].pin.key.origin = 0;
	debt_entries[0].pin.binding.active_lsn = 1;
	UT_ASSERT(!debt_encode(&w, 1));
	UT_ASSERT(w == NULL);
	debt_fixture_end();
}
UT_TEST(debt_wire_damage_prefix_and_source_drift_refuse_whole_batch)
{
	ControlDebtWireImageV1 *w = NULL;
	debt_fixture(65);
	UT_ASSERT(debt_encode(&w, 1));
	if (w) {
		UT_ASSERT(!control_debt_wire_validate_v1(w->registry, w->registry_bytes,
			w->reference, w->pages, 33024, debt_batches, 1, DEBT_TEST_BUDGET));
		for (Size at = 0; at < w->page_bytes; at += 997) {
			w->pages[at] ^= 1;
			UT_ASSERT(!debt_validate(w));
			w->pages[at] ^= 1;
		}
		debt_batches[0].basis_ref[72] ^= 1;
		UT_ASSERT(!debt_validate(w));
		debt_batches[0].basis_ref[72] ^= 1;
		UT_ASSERT(debt_validate(w));
	}
	control_debt_wire_destroy_v1(&w);
	debt_fixture_end();
}
/* Independently reseal all enclosing checksums after semantic tampering.
 * Acceptance must depend on original contracts, not merely on a bad digest. */
static void
debt_test_reseal(ControlDebtWireImageV1 *w)
{
	Size ba = debt_get(w->registry, 112, 8), ra = debt_get(w->registry, 136, 8);
	for (uint64 i = 0; i < w->batch_count; i++) {
		uint8 *b = w->registry + ba + i * 384;
		uint64 first = debt_get(b, 72, 8), np = debt_get(b, 64, 8);
		pg_sha256_ctx ctx;
		uint8 hash[32];
		pg_sha256_init(&ctx);
		for (uint64 j = 0; j < np; j++) {
			uint8 *q = w->pages + (first + j) * 33024;
			Size used = debt_get(q, 72, 4);
			if (used > 64) abort();
			for (Size k = 0; k < used; k++)
				debt_put(q + 256 + k * 512, 508, debt_crc(q + 256 + k * 512, 508), 4);
			pg_sha256_update(&ctx, q + 256, used * 512);
		}
		pg_sha256_final(&ctx, hash);
		memcpy(b + 88, hash, 32);
		for (uint64 j = 0; j < np; j++) {
			uint8 *q = w->pages + (first + j) * 33024;
			uint8 *r = w->registry + ra + (first + j) * 128;
			memcpy(q + 96, hash, 32);
			debt_put(q, 160, debt_crc(q + 256, 32768), 4);
			debt_put(q, 164, debt_header_crc(q, 256, 164), 4);
			debt_sha(q, 33024, r + 64);
			debt_put(r, 124, debt_crc(r, 124), 4);
		}
		debt_sha(w->registry + ra + first * 128, np * 128, b + 120);
		debt_put(b, 380, debt_crc(b, 380), 4);
	}
	debt_sha(w->registry + 256, debt_get(w->registry, 104, 8), w->registry + 168);
	debt_sha(w->registry + ba, w->registry_bytes - ba, w->registry + 200);
	debt_put(w->registry, 232, debt_crc(w->registry + 256, w->registry_bytes - 256), 4);
	debt_put(w->registry, 236, debt_header_crc(w->registry, 256, 236), 4);
	debt_root_reference(w->registry, w->registry_bytes, w->reference);
}
UT_TEST(debt_wire_resigned_semantic_damage_still_refuses)
{
	static const Size offsets[] = {0, 136 + 80, 288, 328, 332, 344, 348, 350,
		352 + 20, 352 + 21, 352 + 24, 352 + 40,
		352 + 80, 352 + 88, 352 + 106, 480};
	ControlDebtWireImageV1 *w = NULL;
	uint8 saved[512];
	debt_fixture(65);
	UT_ASSERT(debt_encode(&w, 1));
	if (w) {
		memcpy(saved, w->pages + 256, 512);
		for (unsigned i = 0; i < lengthof(offsets); i++) {
			w->pages[256 + offsets[i]] ^= 0x40;
			debt_test_reseal(w);
			UT_ASSERT(!debt_validate(w));
			memcpy(w->pages + 256, saved, 512);
			debt_test_reseal(w);
			UT_ASSERT(debt_validate(w));
		}
	}
	control_debt_wire_destroy_v1(&w); debt_fixture_end();
}
UT_TEST(debt_wire_resealed_state_zero_padding_and_partition_refuse)
{
	ControlDebtWireImageV1 *w = NULL;
	debt_fixture(65);
	UT_ASSERT(debt_encode(&w, 1));
	if (w) {
		Size ba = debt_get(w->registry, 112, 8), ra = debt_get(w->registry, 136, 8);
		uint8 *b = w->registry + ba;
		for (unsigned state = 2; state <= 3; state++) {
			debt_put(b, 12, state, 4); debt_put(b, 40, 16, 8);
			debt_put(b, 48, state == 3 ? 16 : 0, 8);
			debt_test_reseal(w); UT_ASSERT(!debt_validate(w));
		}
		debt_put(b, 12, 1, 4); debt_put(b, 40, 0, 8); debt_put(b, 48, 0, 8);
		w->pages[33024 + 256 + 512] = 1; /* Unused final-page slot. */
		debt_test_reseal(w); UT_ASSERT(!debt_validate(w));
		w->pages[33024 + 256 + 512] = 0;
		w->registry[ra + 128 + 55] ^= 1; /* Gap/overlap in second ref. */
		debt_test_reseal(w); UT_ASSERT(!debt_validate(w));
		w->registry[ra + 128 + 55] ^= 1;
		w->registry[256 + 80] ^= 1; /* Original RSV reserved bytes. */
		debt_test_reseal(w); UT_ASSERT(!debt_validate(w));
		w->registry[256 + 80] ^= 1;
		debt_test_reseal(w); UT_ASSERT(debt_validate(w));
	}
	control_debt_wire_destroy_v1(&w); debt_fixture_end();
}
UT_TEST(debt_wire_load_needs_no_recreated_caller_entry_vector)
{
	ControlDebtWireImageV1 *w = NULL;
	debt_fixture(65);
	UT_ASSERT(debt_encode(&w, 1));
	if (w) {
		debt_batches[0].entries = NULL;
		UT_ASSERT(debt_validate(w));
		debt_batches[0].episode++;
		UT_ASSERT(!debt_validate(w));
		debt_batches[0].episode--;
		debt_batches[0].prepare++;
		UT_ASSERT(!debt_validate(w));
	}
	control_debt_wire_destroy_v1(&w); debt_fixture_end();
}
UT_TEST(debt_wire_duplicate_batches_and_empty_unknown_refuse)
{
	ControlDebtWireImageV1 *w = NULL;
	debt_fixture(2);
	debt_batches[1] = debt_batches[0];
	debt_batches[0].entry_count = 1;
	debt_batches[1].entries++;
	debt_batches[1].entry_count = 1;
	debt_entries[1].entry.ordinal = 0;
	UT_ASSERT(!debt_encode(&w, 2));
	UT_ASSERT(!debt_encode(&w, 0));
	debt_batches[0].entry_count = 0;
	UT_ASSERT(!debt_encode(&w, 1));
	UT_ASSERT(w == NULL);
	debt_fixture_end();
}
UT_TEST(debt_wire_preserves_original_zero_membership_epoch)
{
	ControlDebtWireImageV1 *w = NULL;
	debt_fixture(1);
	debt_entries[0].pin.key.membership_epoch = 0;
	UT_ASSERT(debt_encode(&w, 1));
	if (w) {
		UT_ASSERT_EQ(debt_get(w->pages + 256 + 352, 48, 8), 0);
		UT_ASSERT(debt_validate(w));
	}
	control_debt_wire_destroy_v1(&w); debt_fixture_end();
}
#ifndef DEBT_WIRE_TEST_MAIN
#define DEBT_WIRE_TEST_MAIN main
#endif
int DEBT_WIRE_TEST_MAIN(void)
{
	UT_PLAN(10);
	UT_RUN(debt_wire_pages_are_not_a_batch_cap);
	UT_RUN(debt_wire_complete_multiple_batches_and_full_tt);
	UT_RUN(debt_wire_whole_budget_overflow_and_aliases);
	UT_RUN(debt_wire_refuses_incomplete_basis_duplicate_key_and_unknown_tt);
	UT_RUN(debt_wire_damage_prefix_and_source_drift_refuse_whole_batch);
	UT_RUN(debt_wire_resigned_semantic_damage_still_refuses);
	UT_RUN(debt_wire_resealed_state_zero_padding_and_partition_refuse);
	UT_RUN(debt_wire_load_needs_no_recreated_caller_entry_vector);
	UT_RUN(debt_wire_duplicate_batches_and_empty_unknown_refuse);
	UT_RUN(debt_wire_preserves_original_zero_membership_epoch);
	UT_DONE();
	return ut_failed_count != 0;
}
