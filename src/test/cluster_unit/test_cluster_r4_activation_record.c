/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_activation_record.c
 *	  Exact PGSA512 codec and strict-majority policy tests for R4 D13.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_semantic_activation.h"
#include "port/pg_crc32c.h"
#include "storage/shmem.h"

#include "cluster_r4_activation_test_stubs.h"
#include "cluster_unit_no_normal_stop.h"

int cluster_node_id = 0;
bool cluster_enabled = true;

bool
RecoveryInProgress(void)
{
	return true;
}

/* Link-only dependencies of unrelated bootstrap/migration entry points.
 * This record/codec fixture must fail if one is reached, never synthesize
 * a prepared root or a quorum-selected activation record. */
ClusterControlRootResult
cluster_control_root_activate_prepared(
	const ClusterControlRootFileToken *expected_token pg_attribute_unused(),
	const uint8 expected_round_sha256[32] pg_attribute_unused(),
	const ClusterControlRootMigrationRoundV1 *round pg_attribute_unused(),
	ClusterControlRootFileToken *out_token pg_attribute_unused())
{
	abort();
}

ClusterControlRootResult
cluster_control_root_build_migration_image(
	const ClusterControlRootMigrationRoundV1 *round pg_attribute_unused(),
	ClusterControlRootMigrationImage *out pg_attribute_unused())
{
	abort();
}

ClusterControlRootResult
cluster_control_root_create_prepared(
	const ClusterControlRootMigrationImage *image pg_attribute_unused(),
	const ClusterControlRootMigrationRoundV1 *round pg_attribute_unused(),
	ClusterControlRootFileToken *out_token pg_attribute_unused())
{
	abort();
}

bool
cluster_control_root_round_sha256(const ClusterControlRootMigrationRoundV1 *round
									  pg_attribute_unused(),
								  uint8 out_sha[PG_SHA256_DIGEST_LENGTH] pg_attribute_unused())
{
	abort();
}

ClusterSemanticActivationResult
cluster_qvotec_bootstrap_read_semantic_activation(
	uint8 selected[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES] pg_attribute_unused(),
	bool *implicit_open pg_attribute_unused())
{
	abort();
}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

/* The SQL entry pgrac_r4_bit22_cutover_begin (contract step ④e) references
 * superuser(); this binary does not link the backend superuser machinery. */
bool
superuser(void)
{
	return true;
}

/* The member-side bit22 cutover path (RF-ROOT P9 verification,
 * B′) binds the ACTIVE root via
 * cluster_control_root_bootstrap_validate_active_round_fields; this binary
 * does not link cluster_control_root.o.  GREEN stub — the RED identity
 * refusal path is covered in the control-root suites. */
ClusterControlRootResult
cluster_control_root_bootstrap_validate_active_round_fields(
	uint64 transition_epoch pg_attribute_unused(), uint64 prepare_generation pg_attribute_unused(),
	uint64 source_feature_bitmap pg_attribute_unused(),
	uint64 target_feature_bitmap pg_attribute_unused())
{
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

/* implementation (contract §C): cluster_semantic_activation.c now consults the
 * runtime census at the latch apply; this binary does not link
 * cluster_wal_state.o.  GREEN stub — the RED refusal path is covered in
 * test_cluster_r4_activation_fsm test_130. */
bool
cluster_wal_state_correctness_census_ok(void)
{
	return true;
}

bool
cluster_ctrc_shmem_ready(void)
{
	return true;
}

uint32
cluster_grd_recovery_state_value(void)
{
	return 0;
}

bool
cluster_reconfig_snapshot_initial_clean_formation(ClusterInitialCleanFormationSnapshot *out)
{
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	return false;
}

bool
cluster_reconfig_get_observed_slot(int32 node_id pg_attribute_unused(), uint64 *incarnation,
								   uint64 *generation)
{
	if (incarnation != NULL)
		*incarnation = 0;
	if (generation != NULL)
		*generation = 0;
	return false;
}

uint64
cluster_reconfig_get_observed_epoch(int32 node_id pg_attribute_unused())
{
	return 0;
}

const struct ClusterSemanticActivationCallbackBundle *
cluster_pcm_x_resource_x_activation_callbacks(void)
{
	return NULL;
}

bool
cluster_pcm_lock_resource_x_gate_snapshot(ResourceXGateSnapshot *snapshot_out)
{
	if (snapshot_out != NULL)
		memset(snapshot_out, 0, sizeof(*snapshot_out));
	return false;
}

bool
cluster_pcm_lock_resource_x_cutover_gate_snapshot_exact(ResourceXGateSnapshot *snapshot_out)
{
	if (snapshot_out != NULL)
		memset(snapshot_out, 0, sizeof(*snapshot_out));
	return false;
}

bool
cluster_pcm_lock_resource_x_cutover_current_proof_digest_exact(bool thawed pg_attribute_unused(),
															   ResourceXReconfigToken *token_out,
															   uint64 *digest_out)
{
	if (token_out != NULL)
		memset(token_out, 0, sizeof(*token_out));
	if (digest_out != NULL)
		*digest_out = 0;
	return false;
}

void *
palloc(Size size)
{
	return malloc(size);
}

void
pfree(void *pointer)
{
	free(pointer);
}

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size pg_attribute_unused(),
				bool *foundPtr pg_attribute_unused())
{
	return NULL;
}

/* Exercise the real product-local policy helpers without exporting a test API. */
#include "../../backend/cluster/cluster_semantic_activation.c"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

bool
cluster_replacement_phase3_handoff_poll_local(
	ClusterReplacementPhase3HandoffItem *item pg_attribute_unused())
{
	return false;
}

void
ExceptionalCondition(const char *condition_name pg_attribute_unused(),
					 const char *file_name pg_attribute_unused(),
					 int line_number pg_attribute_unused())
{
	abort();
}

static ClusterSemanticActivationRecord
valid_record(ClusterSemanticActivationPhase phase, uint64 generation)
{
	ClusterSemanticActivationRecord record;

	memset(&record, 0, sizeof(record));
	record.source_feature_bitmap = UINT64_C(0x11);
	record.target_feature_bitmap = UINT64_C(0x22);
	record.transition_epoch = UINT64_C(0x33445566778899aa);
	record.record_generation = generation;
	record.admitted_members_lo = UINT64_C(0x0123456789abcdef);
	record.admitted_members_hi = UINT64_C(0xfedcba9876543210);
	record.capability_sample_digest = UINT64_C(0x8877665544332211);
	record.rollback_feature_bitmap
		= phase == CLUSTER_SEMANTIC_PHASE_ROLLBACK_COMPLETE ? UINT64_C(0x20) : 0;
	record.coordinator_incarnation = UINT64_C(0x1020304050607080);
	record.coordinator_node = 37;
	record.phase = phase;
	return record;
}

static uint16
read_u16_le(const uint8 *p)
{
	return (uint16)p[0] | ((uint16)p[1] << 8);
}

static uint32
read_u32_le(const uint8 *p)
{
	return (uint32)p[0] | ((uint32)p[1] << 8) | ((uint32)p[2] << 16) | ((uint32)p[3] << 24);
}

static uint64
read_u64_le(const uint8 *p)
{
	uint64 value = 0;
	int i;

	for (i = 7; i >= 0; i--)
		value = (value << 8) | p[i];
	return value;
}

static void
write_u32_le(uint8 *p, uint32 value)
{
	p[0] = (uint8)value;
	p[1] = (uint8)(value >> 8);
	p[2] = (uint8)(value >> 16);
	p[3] = (uint8)(value >> 24);
}

static void
refresh_crc(uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES])
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, CLUSTER_SEMANTIC_RECORD_CRC_OFFSET);
	FIN_CRC32C(crc);
	write_u32_le(bytes + CLUSTER_SEMANTIC_RECORD_CRC_OFFSET, (uint32)crc);
}

static bool
encode(ClusterSemanticActivationRecord record,
	   uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES])
{
	memset(bytes, 0xa5, CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES);
	return cluster_semantic_activation_record_encode(&record, bytes);
}

static bool
decode(const uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES],
	   ClusterSemanticActivationRecord *record, ClusterSemanticActivationRefusal *refusal)
{
	memset(record, 0, sizeof(*record));
	memset(refusal, 0, sizeof(*refusal));
	return cluster_semantic_activation_record_decode(bytes, record, refusal);
}

static ClusterSemanticRecordSample
sample_from(const uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES], bool readable)
{
	ClusterSemanticRecordSample sample;

	memset(&sample, 0, sizeof(sample));
	sample.readable = readable;
	if (bytes != NULL)
		memcpy(sample.bytes, bytes, sizeof(sample.bytes));
	return sample;
}

static void
assert_roundtrip(ClusterSemanticActivationPhase phase)
{
	ClusterSemanticActivationRecord input = valid_record(phase, 9);
	ClusterSemanticActivationRecord output;
	ClusterSemanticActivationRefusal refusal;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(input, bytes));
	UT_ASSERT(decode(bytes, &output, &refusal));
	UT_ASSERT_EQ(memcmp(&input, &output, sizeof(input)), 0);
}

UT_TEST(test_01_record_constants)
{
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES, 512);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_RECORD_MAGIC, UINT32_C(0x50475341));
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_RECORD_VERSION, 1);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_RECORD_HEADER_LEN, 104);
}

UT_TEST(test_02_phase_numeric_values)
{
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_PHASE_NONE, 0);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_PHASE_PREPARE, 1);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_PHASE_COMMIT, 2);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_PHASE_OPEN, 3);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_PHASE_ROLLBACK_COMPLETE, 4);
}

UT_TEST(test_03_encode_rejects_null_record)
{
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(!cluster_semantic_activation_record_encode(NULL, bytes));
}

UT_TEST(test_04_encode_rejects_null_bytes)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);

	UT_ASSERT(!cluster_semantic_activation_record_encode(&record, NULL));
}

UT_TEST(test_05_decode_rejects_null_bytes)
{
	ClusterSemanticActivationRecord record;
	ClusterSemanticActivationRefusal refusal;

	UT_ASSERT(!cluster_semantic_activation_record_decode(NULL, &record, &refusal));
}

UT_TEST(test_06_decode_rejects_null_record)
{
	ClusterSemanticActivationRefusal refusal;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES] = { 0 };

	UT_ASSERT(!cluster_semantic_activation_record_decode(bytes, NULL, &refusal));
}

UT_TEST(test_07_roundtrip_prepare)
{
	assert_roundtrip(CLUSTER_SEMANTIC_PHASE_PREPARE);
}

UT_TEST(test_08_roundtrip_commit)
{
	assert_roundtrip(CLUSTER_SEMANTIC_PHASE_COMMIT);
}

UT_TEST(test_09_roundtrip_open)
{
	assert_roundtrip(CLUSTER_SEMANTIC_PHASE_OPEN);
}

UT_TEST(test_10_roundtrip_rollback_complete)
{
	assert_roundtrip(CLUSTER_SEMANTIC_PHASE_ROLLBACK_COMPLETE);
}

UT_TEST(test_11_generation_one_is_valid)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u64_le(bytes + 8), 1);
}

UT_TEST(test_12_generation_uint64_max_is_preserved)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, UINT64_MAX);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u64_le(bytes + 8), UINT64_MAX);
}

UT_TEST(test_13_epoch_zero_is_valid)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_PREPARE, 2);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	record.transition_epoch = 0;
	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u64_le(bytes + 40), 0);
}

UT_TEST(test_14_all_member_bits_are_preserved)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_COMMIT, 2);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	record.admitted_members_lo = UINT64_MAX;
	record.admitted_members_hi = UINT64_MAX;
	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u64_le(bytes + 64), UINT64_MAX);
	UT_ASSERT_EQ(read_u64_le(bytes + 72), UINT64_MAX);
}

UT_TEST(test_15_rollback_bitmap_is_preserved_only_for_terminal_phase)
{
	ClusterSemanticActivationRecord record
		= valid_record(CLUSTER_SEMANTIC_PHASE_ROLLBACK_COMPLETE, 4);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u64_le(bytes + 88), UINT64_C(0x20));
}

UT_TEST(test_16_magic_is_little_endian_at_zero)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u32_le(bytes), UINT32_C(0x50475341));
}

UT_TEST(test_17_version_is_little_endian_at_four)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u16_le(bytes + 4), 1);
}

UT_TEST(test_18_header_len_is_little_endian_at_six)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u16_le(bytes + 6), 104);
}

UT_TEST(test_19_generation_is_at_eight)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 77);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u64_le(bytes + 8), 77);
}

UT_TEST(test_20_phase_is_at_sixteen)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_COMMIT, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(bytes[16], 2);
}

UT_TEST(test_21_phase_reserved_bytes_are_zero)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_COMMIT, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	int i;

	UT_ASSERT(encode(record, bytes));
	for (i = 17; i < 24; i++)
		UT_ASSERT_EQ(bytes[i], 0);
}

UT_TEST(test_22_source_bitmap_is_at_twenty_four)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u64_le(bytes + 24), UINT64_C(0x11));
}

UT_TEST(test_23_target_bitmap_is_at_thirty_two)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u64_le(bytes + 32), UINT64_C(0x22));
}

UT_TEST(test_24_epoch_is_at_forty)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u64_le(bytes + 40), UINT64_C(0x33445566778899aa));
}

UT_TEST(test_25_coordinator_node_is_at_forty_eight)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u32_le(bytes + 48), 37);
}

UT_TEST(test_26_coordinator_reserved_word_is_zero)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u32_le(bytes + 52), 0);
}

UT_TEST(test_27_coordinator_incarnation_is_at_fifty_six)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u64_le(bytes + 56), UINT64_C(0x1020304050607080));
}

UT_TEST(test_28_member_words_are_at_sixty_four_and_seventy_two)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u64_le(bytes + 64), UINT64_C(0x0123456789abcdef));
	UT_ASSERT_EQ(read_u64_le(bytes + 72), UINT64_C(0xfedcba9876543210));
}

UT_TEST(test_29_digest_and_rollback_are_at_eighty_and_eighty_eight)
{
	ClusterSemanticActivationRecord record
		= valid_record(CLUSTER_SEMANTIC_PHASE_ROLLBACK_COMPLETE, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u64_le(bytes + 80), UINT64_C(0x8877665544332211));
	UT_ASSERT_EQ(read_u64_le(bytes + 88), UINT64_C(0x20));
}

UT_TEST(test_30_crc_covers_exactly_bytes_zero_through_ninety_five)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint32 stored;
	pg_crc32c expected;

	UT_ASSERT(encode(record, bytes));
	INIT_CRC32C(expected);
	COMP_CRC32C(expected, bytes, 96);
	FIN_CRC32C(expected);
	stored = read_u32_le(bytes + 96);
	UT_ASSERT_EQ(stored, (uint32)expected);
}

UT_TEST(test_31_bytes_one_hundred_through_end_are_zero)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	int i;

	UT_ASSERT(encode(record, bytes));
	for (i = 100; i < CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES; i++)
		UT_ASSERT_EQ(bytes[i], 0);
}

UT_TEST(test_32_encode_rejects_generation_zero)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 0);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(!encode(record, bytes));
}

UT_TEST(test_33_encode_rejects_phase_none)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_NONE, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(!encode(record, bytes));
}

UT_TEST(test_34_encode_rejects_rollback_bitmap_before_terminal_phase)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	record.rollback_feature_bitmap = 1;
	UT_ASSERT(!encode(record, bytes));
}

UT_TEST(test_35_decode_rejects_magic_mutation)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	ClusterSemanticActivationRecord output;
	ClusterSemanticActivationRefusal refusal;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	bytes[0] ^= 1;
	refresh_crc(bytes);
	UT_ASSERT(!decode(bytes, &output, &refusal));
}

UT_TEST(test_36_decode_rejects_version_mutation)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	ClusterSemanticActivationRecord output;
	ClusterSemanticActivationRefusal refusal;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	bytes[4] = 2;
	refresh_crc(bytes);
	UT_ASSERT(!decode(bytes, &output, &refusal));
}

UT_TEST(test_37_decode_rejects_header_len_mutation)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	ClusterSemanticActivationRecord output;
	ClusterSemanticActivationRefusal refusal;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	bytes[6] = 105;
	refresh_crc(bytes);
	UT_ASSERT(!decode(bytes, &output, &refusal));
}

UT_TEST(test_38_decode_rejects_generation_zero)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	ClusterSemanticActivationRecord output;
	ClusterSemanticActivationRefusal refusal;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	memset(bytes + 8, 0, 8);
	refresh_crc(bytes);
	UT_ASSERT(!decode(bytes, &output, &refusal));
}

UT_TEST(test_39_decode_rejects_unknown_phase)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	ClusterSemanticActivationRecord output;
	ClusterSemanticActivationRefusal refusal;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	bytes[16] = 5;
	refresh_crc(bytes);
	UT_ASSERT(!decode(bytes, &output, &refusal));
}

UT_TEST(test_40_decode_rejects_phase_reserved_mutation)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	ClusterSemanticActivationRecord output;
	ClusterSemanticActivationRefusal refusal;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	bytes[19] = 1;
	refresh_crc(bytes);
	UT_ASSERT(!decode(bytes, &output, &refusal));
}

UT_TEST(test_41_decode_rejects_word_reserved_mutation)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	ClusterSemanticActivationRecord output;
	ClusterSemanticActivationRefusal refusal;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	bytes[52] = 1;
	refresh_crc(bytes);
	UT_ASSERT(!decode(bytes, &output, &refusal));
}

UT_TEST(test_42_decode_rejects_crc_and_tail_mutations)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	ClusterSemanticActivationRecord output;
	ClusterSemanticActivationRefusal refusal;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	bytes[96] ^= 1;
	UT_ASSERT(!decode(bytes, &output, &refusal));
	UT_ASSERT(encode(record, bytes));
	bytes[511] = 1;
	UT_ASSERT(!decode(bytes, &output, &refusal));
}

UT_TEST(test_43_two_of_three_identical_records_select)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 7);
	ClusterSemanticRecordSample samples[3];
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 selected[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	bool implicit_open = true;

	UT_ASSERT(encode(record, bytes));
	samples[0] = sample_from(bytes, true);
	samples[1] = sample_from(bytes, true);
	samples[2] = sample_from(NULL, false);
	UT_ASSERT_EQ(semantic_activation_select_majority(samples, 3, selected, &implicit_open),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT(!implicit_open);
	UT_ASSERT_EQ(memcmp(selected, bytes, sizeof(selected)), 0);
}

UT_TEST(test_44_three_of_five_identical_records_select)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_COMMIT, 8);
	ClusterSemanticRecordSample samples[5];
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 selected[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	bool implicit_open = true;
	int i;

	UT_ASSERT(encode(record, bytes));
	for (i = 0; i < 3; i++)
		samples[i] = sample_from(bytes, true);
	for (; i < 5; i++)
		samples[i] = sample_from(NULL, false);
	UT_ASSERT_EQ(semantic_activation_select_majority(samples, 5, selected, &implicit_open),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT(!implicit_open);
}

UT_TEST(test_45_minority_higher_generation_does_not_win)
{
	ClusterSemanticActivationRecord old = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 7);
	ClusterSemanticActivationRecord newer = valid_record(CLUSTER_SEMANTIC_PHASE_PREPARE, 8);
	ClusterSemanticRecordSample samples[3];
	uint8 old_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 new_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 selected[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	bool implicit_open = true;

	UT_ASSERT(encode(old, old_bytes));
	UT_ASSERT(encode(newer, new_bytes));
	samples[0] = sample_from(old_bytes, true);
	samples[1] = sample_from(old_bytes, true);
	samples[2] = sample_from(new_bytes, true);
	UT_ASSERT_EQ(semantic_activation_select_majority(samples, 3, selected, &implicit_open),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT_EQ(memcmp(selected, old_bytes, sizeof(selected)), 0);
}

UT_TEST(test_46_readable_all_zero_majority_is_implicit_open_zero)
{
	ClusterSemanticRecordSample samples[3];
	uint8 selected[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	bool implicit_open = false;

	samples[0] = sample_from(NULL, true);
	samples[1] = sample_from(NULL, true);
	samples[2] = sample_from(NULL, false);
	UT_ASSERT_EQ(semantic_activation_select_majority(samples, 3, selected, &implicit_open),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT(implicit_open);
	UT_ASSERT_EQ(read_u64_le(selected + 8), 0);
}

UT_TEST(test_47_less_than_readable_majority_holds)
{
	ClusterSemanticRecordSample samples[3];
	uint8 selected[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	bool implicit_open = false;

	samples[0] = sample_from(NULL, true);
	samples[1] = sample_from(NULL, false);
	samples[2] = sample_from(NULL, false);
	UT_ASSERT_EQ(semantic_activation_select_majority(samples, 3, selected, &implicit_open),
				 CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD);
}

UT_TEST(test_48_equal_generation_conflict_without_majority_is_not_legacy)
{
	ClusterSemanticActivationRecord a = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 7);
	ClusterSemanticActivationRecord b = a;
	ClusterSemanticRecordSample samples[4];
	uint8 a_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 b_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 selected[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	bool implicit_open = true;

	b.target_feature_bitmap++;
	UT_ASSERT(encode(a, a_bytes));
	UT_ASSERT(encode(b, b_bytes));
	samples[0] = sample_from(a_bytes, true);
	samples[1] = sample_from(a_bytes, true);
	samples[2] = sample_from(b_bytes, true);
	samples[3] = sample_from(b_bytes, true);
	UT_ASSERT_EQ(semantic_activation_select_majority(samples, 4, selected, &implicit_open),
				 CLUSTER_SEMANTIC_ACTIVATION_RECORD_CONFLICT);
	UT_ASSERT(!implicit_open);
}

UT_TEST(test_49_record_cas_mailbox_exact_sequence_lifecycle)
{
	ClusterSemanticActivationShmem shmem;
	ClusterSemanticActivationUtilityMailboxShmem utility;
	ClusterSemanticActivationCasRequest request;
	ClusterSemanticActivationRecord desired_record
		= valid_record(CLUSTER_SEMANTIC_PHASE_PREPARE, 8);
	ClusterSemanticActivationResult result = CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
	uint8 desired[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint64 seq = UINT64_MAX;

	desired_record.transition_epoch = 0;
	desired_record.coordinator_node = (uint32)cluster_node_id;
	desired_record.coordinator_incarnation = 1;
	desired_record.source_feature_bitmap = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	desired_record.target_feature_bitmap = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
										   | CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1;
	memset(&shmem, 0, sizeof(shmem));
	pg_atomic_init_u64(&shmem.record_cas_request_seq, 0);
	pg_atomic_init_u64(&shmem.record_cas_completion_seq, 0);
	pg_atomic_init_u32(&shmem.record_cas_result, CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
	pg_atomic_init_u32(&shmem.record_cas_request_kind, CLUSTER_SEMANTIC_AUTHORITY_REQUEST_NONE);
	pg_atomic_init_u64(&shmem.admission_seq, 0);
	pg_atomic_init_u64(&shmem.active_bits, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	pg_atomic_init_u64(&shmem.record_generation, 7);
	pg_atomic_init_u64(&shmem.formation_epoch, 0);
	pg_atomic_init_u32(&shmem.transition_closed, 1);
	memset(&utility, 0, sizeof(utility));
	pg_atomic_init_u64(&utility.utility_request_seq, 1);
	pg_atomic_init_u64(&utility.utility_completion_seq, 0);
	pg_atomic_init_u32(&utility.utility_mailbox_state, SEMANTIC_ACTIVATION_UTILITY_MAILBOX_PENDING);
	utility.utility_action = CLUSTER_SEMANTIC_ENABLE_ALL;
	utility.utility_source_feature_bitmap = desired_record.source_feature_bitmap;
	utility.utility_target_feature_bitmap = desired_record.target_feature_bitmap;
	utility.utility_rollback_feature_bitmap = desired_record.rollback_feature_bitmap;
	utility.utility_expected_record_generation = 7;
	SemanticActivationUtilityMailbox = &utility;
	cluster_r4_activation_test_formation_valid = true;
	SemanticActivationShmem = NULL;
	UT_ASSERT(encode(desired_record, desired));
	UT_ASSERT(!semantic_activation_record_cas_mailbox_submit(
		7, desired_record.source_feature_bitmap, desired, &seq));

	SemanticActivationShmem = &shmem;
	UT_ASSERT(!semantic_activation_record_cas_mailbox_submit(
		7, desired_record.source_feature_bitmap, NULL, &seq));
	UT_ASSERT(!semantic_activation_record_cas_mailbox_submit(
		7, desired_record.source_feature_bitmap, desired, NULL));
	UT_ASSERT(semantic_activation_record_cas_mailbox_submit(7, desired_record.source_feature_bitmap,
															desired, &seq));
	UT_ASSERT_EQ(seq, 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&shmem.record_cas_request_seq), 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&shmem.record_cas_completion_seq), 0);
	UT_ASSERT(!semantic_activation_record_cas_mailbox_submit(8, UINT64_C(0x22), desired, &seq));

	memset(&request, 0, sizeof(request));
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_record_cas(&request));
	UT_ASSERT_EQ(request.request_seq, 1);
	UT_ASSERT_EQ(request.expected_generation, 7);
	UT_ASSERT_EQ(request.expected_source_feature_bitmap, desired_record.source_feature_bitmap);
	UT_ASSERT_EQ(memcmp(request.desired_bytes, desired, sizeof(desired)), 0);
	UT_ASSERT(
		!cluster_semantic_activation_qvotec_complete_record_cas(0, CLUSTER_SEMANTIC_ACTIVATION_OK));
	UT_ASSERT(
		!cluster_semantic_activation_qvotec_complete_record_cas(2, CLUSTER_SEMANTIC_ACTIVATION_OK));
	UT_ASSERT_EQ(pg_atomic_read_u64(&shmem.record_cas_completion_seq), 0);
	UT_ASSERT(cluster_semantic_activation_qvotec_complete_record_cas(
		1, CLUSTER_SEMANTIC_ACTIVATION_RECORD_CONFLICT));
	UT_ASSERT(
		!cluster_semantic_activation_qvotec_complete_record_cas(1, CLUSTER_SEMANTIC_ACTIVATION_OK));
	UT_ASSERT(semantic_activation_record_cas_mailbox_poll_completion(1, &result));
	UT_ASSERT_EQ(result, CLUSTER_SEMANTIC_ACTIVATION_RECORD_CONFLICT);
	UT_ASSERT(!semantic_activation_record_cas_mailbox_poll_completion(2, &result));

	UT_ASSERT(semantic_activation_record_cas_mailbox_submit(7, desired_record.source_feature_bitmap,
															desired, &seq));
	UT_ASSERT_EQ(seq, 2);
	pg_atomic_write_u64(&shmem.record_cas_request_seq, UINT64_MAX);
	pg_atomic_write_u64(&shmem.record_cas_completion_seq, UINT64_MAX);
	UT_ASSERT(!semantic_activation_record_cas_mailbox_submit(
		7, desired_record.source_feature_bitmap, desired, &seq));
	UT_ASSERT_EQ(pg_atomic_read_u64(&shmem.record_cas_request_seq), UINT64_MAX);
	UT_ASSERT_EQ(pg_atomic_read_u64(&shmem.record_cas_completion_seq), UINT64_MAX);
	SemanticActivationShmem = NULL;
	SemanticActivationUtilityMailbox = NULL;
	cluster_r4_activation_test_formation_valid = false;
}

UT_TEST(test_50_pgrd_uses_distinct_kind_in_existing_512_byte_mailbox)
{
	ClusterSemanticActivationShmem shmem;
	ClusterSemanticActivationUtilityMailboxShmem utility;
	ClusterSemanticFormationBinding formation = {
		.utility_request_seq = 1,
		.formation_epoch = 0,
		.coordinator_incarnation = 1,
		.expected_record_generation = 0,
	};
	ClusterUndoRootDescriptorRequest request;
	ClusterSemanticActivationCasRequest record_request;
	ClusterSemanticActivationResult result = CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
	uint8 desired[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint64 seq = 0;
	int i;

	memset(&shmem, 0, sizeof(shmem));
	pg_atomic_init_u64(&shmem.record_cas_request_seq, 0);
	pg_atomic_init_u64(&shmem.record_cas_completion_seq, 0);
	pg_atomic_init_u32(&shmem.record_cas_result, CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
	pg_atomic_init_u32(&shmem.record_cas_request_kind, CLUSTER_SEMANTIC_AUTHORITY_REQUEST_NONE);
	memset(&utility, 0, sizeof(utility));
	pg_atomic_init_u64(&utility.utility_request_seq, 1);
	pg_atomic_init_u64(&utility.utility_completion_seq, 0);
	pg_atomic_init_u32(&utility.utility_mailbox_state, SEMANTIC_ACTIVATION_UTILITY_MAILBOX_PENDING);
	utility.utility_action = CLUSTER_SEMANTIC_ENABLE_ALL;
	utility.utility_target_feature_bitmap = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	for (i = 0; i < (int)sizeof(desired); i++)
		desired[i] = (uint8)(i ^ 0x5c);
	SemanticActivationShmem = &shmem;
	SemanticActivationUtilityMailbox = &utility;
	cluster_r4_activation_test_formation_valid = true;

	UT_ASSERT(cluster_semantic_activation_undo_root_descriptor_mailbox_submit(
		&formation, UINT64_C(0x0123456789abcdef), desired, &seq));
	UT_ASSERT_EQ(seq, 1);
	UT_ASSERT(!cluster_semantic_activation_qvotec_poll_record_cas(&record_request));
	memset(&request, 0, sizeof(request));
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_undo_root_descriptor(&request));
	UT_ASSERT_EQ(request.request_seq, 1);
	UT_ASSERT_EQ(request.system_identifier, UINT64_C(0x0123456789abcdef));
	UT_ASSERT_EQ(request.formation.utility_request_seq, UINT64_C(1));
	UT_ASSERT_EQ(request.formation.formation_epoch, UINT64_C(0));
	UT_ASSERT_EQ(request.formation.coordinator_incarnation, UINT64_C(1));
	UT_ASSERT_EQ(request.formation.expected_record_generation, UINT64_C(0));
	UT_ASSERT_EQ(memcmp(request.desired_bytes, desired, sizeof(desired)), 0);
	UT_ASSERT(cluster_semantic_activation_qvotec_complete_undo_root_descriptor(
		request.request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK));
	UT_ASSERT(
		cluster_semantic_activation_undo_root_descriptor_mailbox_poll_completion(seq, &result));
	UT_ASSERT_EQ(result, CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT_EQ(offsetof(ClusterSemanticActivationShmem, record_cas_request_kind), 20);
	UT_ASSERT_EQ(offsetof(ClusterSemanticActivationShmem, admission_seq), 552);
	UT_ASSERT_EQ(sizeof(ClusterSemanticActivationShmem), 1104);
	SemanticActivationShmem = NULL;
	SemanticActivationUtilityMailbox = NULL;
	cluster_r4_activation_test_formation_valid = false;
}

UT_TEST(test_51_ack_sample_request_has_exact_wire_bytes)
{
	ClusterSemanticActivationAckWireV1 message;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];

	memset(&message, 0, sizeof(message));
	message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	message.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	message.coordinator_node = 1;
	message.member_node = 3;
	message.transition_epoch = UINT64_C(0x0102030405060708);
	message.record_generation = UINT64_C(0x1112131415161718);
	message.round_nonce = UINT64_C(0x2122232425262728);
	message.source_feature_bitmap = UINT64_C(0x3132333435363738);
	message.target_feature_bitmap = UINT64_C(0x4142434445464748);
	message.rollback_feature_bitmap = UINT64_C(0x5152535455565758);
	message.admitted_members_lo = UINT64_C(0x000000000000000f);
	message.admitted_members_hi = UINT64_C(0);
	memset(bytes, 0xa5, sizeof(bytes));

	UT_ASSERT_EQ(PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1, 65);
	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_ACK_V1, UINT32_C(0x00008000));
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS, UINT32_C(0x0071B000));
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES, 120);
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(&message, bytes));
	UT_ASSERT_EQ(bytes[0], 0x41);
	UT_ASSERT_EQ(bytes[1], 0x43);
	UT_ASSERT_EQ(bytes[2], 0x4b);
	UT_ASSERT_EQ(bytes[3], 0x31);
	UT_ASSERT_EQ(read_u16_le(bytes + 4), UINT16_C(1));
	UT_ASSERT_EQ(bytes[6], 1);
	UT_ASSERT_EQ(bytes[7], 1);
	UT_ASSERT_EQ(read_u32_le(bytes + 8), UINT32_C(0));
	UT_ASSERT_EQ(read_u32_le(bytes + 12), UINT32_C(0));
	UT_ASSERT_EQ(read_u32_le(bytes + 16), UINT32_C(1));
	UT_ASSERT_EQ(read_u32_le(bytes + 20), UINT32_C(3));
	UT_ASSERT_EQ(read_u64_le(bytes + 24), UINT64_C(0x0102030405060708));
	UT_ASSERT_EQ(read_u64_le(bytes + 32), UINT64_C(0x1112131415161718));
	UT_ASSERT_EQ(read_u64_le(bytes + 40), UINT64_C(0x2122232425262728));
	UT_ASSERT_EQ(read_u64_le(bytes + 48), UINT64_C(0x3132333435363738));
	UT_ASSERT_EQ(read_u64_le(bytes + 56), UINT64_C(0x4142434445464748));
	UT_ASSERT_EQ(read_u64_le(bytes + 64), UINT64_C(0x5152535455565758));
	UT_ASSERT_EQ(read_u64_le(bytes + 72), UINT64_C(0x000000000000000f));
	UT_ASSERT_EQ(read_u64_le(bytes + 80), UINT64_C(0));
	UT_ASSERT_EQ(read_u64_le(bytes + 88), UINT64_C(0));
	UT_ASSERT_EQ(read_u64_le(bytes + 96), UINT64_C(0));
	UT_ASSERT_EQ(read_u64_le(bytes + 104), UINT64_C(0));
	UT_ASSERT_EQ(read_u32_le(bytes + 112), UINT32_C(0));
	UT_ASSERT_EQ(read_u32_le(bytes + 116), UINT32_C(0));
}

UT_TEST(test_52_ack_wire_decodes_hand_written_little_endian_bytes)
{
	static const uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES]
		= { 0x41, 0x43, 0x4b, 0x31, 0x01, 0x00, 0x02, 0x04, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x08, 0x07, 0x06, 0x05,
			0x04, 0x03, 0x02, 0x01, 0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11, 0x28, 0x27,
			0x26, 0x25, 0x24, 0x23, 0x22, 0x21, 0x38, 0x37, 0x36, 0x35, 0x34, 0x33, 0x32, 0x31,
			0x48, 0x47, 0x46, 0x45, 0x44, 0x43, 0x42, 0x41, 0x58, 0x57, 0x56, 0x55, 0x54, 0x53,
			0x52, 0x51, 0x0f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x68, 0x67, 0x66, 0x65,
			0x64, 0x63, 0x62, 0x61, 0x78, 0x77, 0x76, 0x75, 0x74, 0x73, 0x72, 0x71, 0x88, 0x87,
			0x86, 0x85, 0x84, 0x83, 0x82, 0x81, 0x98, 0x97, 0x96, 0x95, 0x94, 0x93, 0x92, 0x91,
			0x00, 0xb0, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00 };
	ClusterSemanticActivationAckWireV1 message;

	memset(&message, 0xa5, sizeof(message));
	UT_ASSERT(cluster_semantic_activation_ack_wire_decode(bytes, &message));
	UT_ASSERT_EQ(message.kind, CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK);
	UT_ASSERT_EQ(message.stage, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED);
	UT_ASSERT_EQ(message.result, CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK);
	UT_ASSERT_EQ(message.reason, UINT32_C(0));
	UT_ASSERT_EQ(message.coordinator_node, UINT32_C(1));
	UT_ASSERT_EQ(message.member_node, UINT32_C(3));
	UT_ASSERT_EQ(message.transition_epoch, UINT64_C(0x0102030405060708));
	UT_ASSERT_EQ(message.record_generation, UINT64_C(0x1112131415161718));
	UT_ASSERT_EQ(message.round_nonce, UINT64_C(0x2122232425262728));
	UT_ASSERT_EQ(message.source_feature_bitmap, UINT64_C(0x3132333435363738));
	UT_ASSERT_EQ(message.target_feature_bitmap, UINT64_C(0x4142434445464748));
	UT_ASSERT_EQ(message.rollback_feature_bitmap, UINT64_C(0x5152535455565758));
	UT_ASSERT_EQ(message.admitted_members_lo, UINT64_C(0x000000000000000f));
	UT_ASSERT_EQ(message.admitted_members_hi, UINT64_C(0x6162636465666768));
	UT_ASSERT_EQ(message.capability_sample_digest, UINT64_C(0x7172737475767778));
	UT_ASSERT_EQ(message.boot_id, UINT64_C(0x8182838485868788));
	UT_ASSERT_EQ(message.admitted_incarnation, UINT64_C(0x9192939495969798));
	UT_ASSERT_EQ(message.capability_word, UINT32_C(0x0030B000));
}

UT_TEST(test_53_ack_wire_rejects_nonzero_reserved_without_output_mutation)
{
	ClusterSemanticActivationAckWireV1 input;
	ClusterSemanticActivationAckWireV1 output;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];
	const uint8 *output_bytes = (const uint8 *)&output;
	int i;

	memset(&input, 0, sizeof(input));
	input.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	input.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	input.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	input.coordinator_node = 1;
	input.member_node = 3;
	input.transition_epoch = 1;
	input.record_generation = 1;
	input.round_nonce = 1;
	input.admitted_members_lo = UINT64_C(0x0f);
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(&input, bytes));
	bytes[116] = 1;
	memset(&output, 0xa5, sizeof(output));

	UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(bytes, &output));
	for (i = 0; i < (int)sizeof(output); i++)
		UT_ASSERT_EQ(output_bytes[i], 0xa5);
}

UT_TEST(test_54_ack_wire_rejects_wrong_magic_and_version)
{
	ClusterSemanticActivationAckWireV1 input;
	ClusterSemanticActivationAckWireV1 output;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];

	memset(&input, 0, sizeof(input));
	input.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	input.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	input.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	input.coordinator_node = 1;
	input.member_node = 3;
	input.transition_epoch = 1;
	input.record_generation = 1;
	input.round_nonce = 1;
	input.admitted_members_lo = UINT64_C(0x0f);
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(&input, bytes));

	bytes[0] ^= 0x01;
	memset(&output, 0xa5, sizeof(output));
	UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(bytes, &output));
	bytes[0] ^= 0x01;
	bytes[4] = 2;
	memset(&output, 0xa5, sizeof(output));
	UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(bytes, &output));
}

UT_TEST(test_55_ack_wire_rejects_unknown_kind_stage_and_result)
{
	ClusterSemanticActivationAckWireV1 input;
	ClusterSemanticActivationAckWireV1 output;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];

	memset(&input, 0, sizeof(input));
	input.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	input.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	input.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	input.coordinator_node = 1;
	input.member_node = 3;
	input.transition_epoch = 1;
	input.record_generation = 1;
	input.round_nonce = 1;
	input.admitted_members_lo = UINT64_C(0x0f);
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(&input, bytes));

	bytes[6] = 0;
	UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(bytes, &output));
	bytes[6] = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	bytes[7] = 6;
	UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(bytes, &output));
	bytes[7] = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	bytes[8] = 3;
	UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(bytes, &output));
}

UT_TEST(test_56_ack_wire_rejects_invalid_request_shape)
{
	ClusterSemanticActivationAckWireV1 input;
	ClusterSemanticActivationAckWireV1 output;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];

	memset(&input, 0, sizeof(input));
	input.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	input.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	input.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	input.coordinator_node = 1;
	input.member_node = 3;
	input.transition_epoch = 1;
	input.record_generation = 1;
	input.round_nonce = 1;
	input.admitted_members_lo = UINT64_C(0x0f);
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(&input, bytes));

	bytes[8] = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK;
	UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(bytes, &output));
	bytes[8] = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	bytes[12] = 1;
	UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(bytes, &output));
	bytes[12] = 0;
	bytes[96] = 1;
	UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(bytes, &output));
	bytes[96] = 0;
	bytes[104] = 1;
	UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(bytes, &output));
	bytes[104] = 0;
	bytes[112] = 1;
	UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(bytes, &output));
	bytes[112] = 0;
	bytes[88] = 1;
	UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(bytes, &output));
	bytes[88] = 0;
	bytes[7] = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER;
	UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(bytes, &output));
	bytes[7] = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	memset(bytes + 32, 0, 8);
	UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(bytes, &output));
	bytes[32] = 1;
	memset(bytes + 40, 0, 8);
	UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(bytes, &output));
}

UT_TEST(test_57_ack_wire_rejects_invalid_positive_ack_shape)
{
	ClusterSemanticActivationAckWireV1 input;
	ClusterSemanticActivationAckWireV1 output;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];

	memset(&input, 0, sizeof(input));
	input.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK;
	input.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED;
	input.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK;
	input.coordinator_node = 1;
	input.member_node = 3;
	input.transition_epoch = 1;
	input.record_generation = 1;
	input.round_nonce = 1;
	input.admitted_members_lo = UINT64_C(0x0f);
	input.capability_sample_digest = 1;
	input.boot_id = 1;
	input.admitted_incarnation = 1;
	input.capability_word = CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(&input, bytes));

	bytes[8] = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(bytes, &output));
	bytes[8] = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK;
	bytes[12] = 1;
	UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(bytes, &output));
	bytes[12] = 0;
	memset(bytes + 96, 0, 8);
	UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(bytes, &output));
	bytes[96] = 1;
	memset(bytes + 104, 0, 8);
	UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(bytes, &output));
	bytes[104] = 1;
	memset(bytes + 112, 0, 4);
	UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(bytes, &output));
}

UT_TEST(test_58_ack_wire_rejects_invalid_refused_ack_shape)
{
	ClusterSemanticActivationAckWireV1 input;
	ClusterSemanticActivationAckWireV1 output;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];

	memset(&input, 0, sizeof(input));
	input.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK;
	input.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	input.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REFUSED;
	input.reason = CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
	input.coordinator_node = 1;
	input.member_node = 3;
	input.transition_epoch = 1;
	input.record_generation = 1;
	input.round_nonce = 1;
	input.admitted_members_lo = UINT64_C(0x0f);
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(&input, bytes));
	UT_ASSERT(cluster_semantic_activation_ack_wire_decode(bytes, &output));

	write_u32_le(bytes + 12, CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(bytes, &output));
	write_u32_le(bytes + 12, CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE + 1);
	UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(bytes, &output));
	write_u32_le(bytes + 12, CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
	bytes[96] = 1;
	UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(bytes, &output));
	bytes[96] = 0;
	bytes[104] = 1;
	UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(bytes, &output));
	bytes[104] = 0;
	bytes[112] = 1;
	UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(bytes, &output));
}

UT_TEST(test_59_ack_wire_rejects_out_of_range_or_unadmitted_nodes)
{
	ClusterSemanticActivationAckWireV1 input;
	ClusterSemanticActivationAckWireV1 output;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];

	memset(&input, 0, sizeof(input));
	input.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	input.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	input.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	input.coordinator_node = 1;
	input.member_node = 3;
	input.transition_epoch = 1;
	input.record_generation = 1;
	input.round_nonce = 1;
	input.admitted_members_lo = UINT64_C(0x0f);
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(&input, bytes));

	write_u32_le(bytes + 16, CLUSTER_MAX_NODES);
	UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(bytes, &output));
	write_u32_le(bytes + 16, 1);
	write_u32_le(bytes + 20, CLUSTER_MAX_NODES);
	UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(bytes, &output));
	write_u32_le(bytes + 20, 4);
	UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(bytes, &output));
	write_u32_le(bytes + 20, 3);
	write_u32_le(bytes + 16, 4);
	UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(bytes, &output));
}

UT_TEST(test_60_ack_wire_applies_digest_rule_to_ack_round_identity)
{
	ClusterSemanticActivationAckWireV1 input;
	ClusterSemanticActivationAckWireV1 output;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];

	memset(&input, 0, sizeof(input));
	input.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK;
	input.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	input.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK;
	input.coordinator_node = 1;
	input.member_node = 3;
	input.transition_epoch = 1;
	input.record_generation = 1;
	input.round_nonce = 1;
	input.admitted_members_lo = UINT64_C(0x0f);
	input.boot_id = 1;
	input.admitted_incarnation = 1;
	input.capability_word = CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(&input, bytes));
	UT_ASSERT(cluster_semantic_activation_ack_wire_decode(bytes, &output));

	bytes[88] = 1;
	UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(bytes, &output));
	bytes[88] = 0;
	bytes[7] = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED;
	UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(bytes, &output));
}

UT_TEST(test_61_ack_wire_encoder_rejects_invalid_host_shape)
{
	ClusterSemanticActivationAckWireV1 message;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];
	int i;

	memset(&message, 0, sizeof(message));
	message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	message.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	message.coordinator_node = 1;
	message.member_node = 3;
	message.transition_epoch = 1;
	message.record_generation = 1;
	message.round_nonce = 1;
	message.admitted_members_lo = UINT64_C(0x0f);

	message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_INVALID;
	memset(bytes, 0xa5, sizeof(bytes));
	UT_ASSERT(!cluster_semantic_activation_ack_wire_encode(&message, bytes));
	for (i = 0; i < (int)sizeof(bytes); i++)
		UT_ASSERT_EQ(bytes[i], 0xa5);
	message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	message.record_generation = 0;
	UT_ASSERT(!cluster_semantic_activation_ack_wire_encode(&message, bytes));
	message.record_generation = 1;
	message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK;
	UT_ASSERT(!cluster_semantic_activation_ack_wire_encode(&message, bytes));

	message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK;
	message.admitted_incarnation = 1;
	message.capability_word = CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	UT_ASSERT(!cluster_semantic_activation_ack_wire_encode(&message, bytes));
	message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REFUSED;
	message.admitted_incarnation = 0;
	message.capability_word = 0;
	UT_ASSERT(!cluster_semantic_activation_ack_wire_encode(&message, bytes));

	message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	message.member_node = 4;
	UT_ASSERT(!cluster_semantic_activation_ack_wire_encode(&message, bytes));
}

UT_TEST(test_62_ack_tuple_and_table_have_exact_frozen_layout)
{
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ACTIVATION_ACK_TUPLE_BYTES, 64);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ACTIVATION_ACK_TABLE_BYTES, 16496);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID, UINT32_C(1));
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE, UINT32_C(2));
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_OPEN_PROOF, UINT32_C(4));

	UT_ASSERT_EQ(sizeof(SemanticActivationAckTuple), 64);
	UT_ASSERT_EQ(offsetof(SemanticActivationAckTuple, node_id), 0);
	UT_ASSERT_EQ(offsetof(SemanticActivationAckTuple, boot_id), 8);
	UT_ASSERT_EQ(offsetof(SemanticActivationAckTuple, admitted_incarnation), 16);
	UT_ASSERT_EQ(offsetof(SemanticActivationAckTuple, control_connection_generation), 24);
	UT_ASSERT_EQ(offsetof(SemanticActivationAckTuple, capability_word), 32);
	UT_ASSERT_EQ(offsetof(SemanticActivationAckTuple, capability_generation), 40);
	UT_ASSERT_EQ(offsetof(SemanticActivationAckTuple, transition_epoch), 48);
	UT_ASSERT_EQ(offsetof(SemanticActivationAckTuple, record_generation), 56);

	UT_ASSERT_EQ(sizeof(ClusterSemanticActivationAckTableV1), 16496);
	UT_ASSERT_EQ(offsetof(ClusterSemanticActivationAckTableV1, publication_seq), 0);
	UT_ASSERT_EQ(offsetof(ClusterSemanticActivationAckTableV1, stage), 8);
	UT_ASSERT_EQ(offsetof(ClusterSemanticActivationAckTableV1, flags), 12);
	UT_ASSERT_EQ(offsetof(ClusterSemanticActivationAckTableV1, coordinator_node), 16);
	UT_ASSERT_EQ(offsetof(ClusterSemanticActivationAckTableV1, reserved), 20);
	UT_ASSERT_EQ(offsetof(ClusterSemanticActivationAckTableV1, round_nonce), 24);
	UT_ASSERT_EQ(offsetof(ClusterSemanticActivationAckTableV1, expected_members_lo), 32);
	UT_ASSERT_EQ(offsetof(ClusterSemanticActivationAckTableV1, expected_members_hi), 40);
	UT_ASSERT_EQ(offsetof(ClusterSemanticActivationAckTableV1, observed_members_lo), 48);
	UT_ASSERT_EQ(offsetof(ClusterSemanticActivationAckTableV1, observed_members_hi), 56);
	UT_ASSERT_EQ(offsetof(ClusterSemanticActivationAckTableV1, transition_epoch), 64);
	UT_ASSERT_EQ(offsetof(ClusterSemanticActivationAckTableV1, record_generation), 72);
	UT_ASSERT_EQ(offsetof(ClusterSemanticActivationAckTableV1, source_feature_bitmap), 80);
	UT_ASSERT_EQ(offsetof(ClusterSemanticActivationAckTableV1, target_feature_bitmap), 88);
	UT_ASSERT_EQ(offsetof(ClusterSemanticActivationAckTableV1, rollback_feature_bitmap), 96);
	UT_ASSERT_EQ(offsetof(ClusterSemanticActivationAckTableV1, capability_sample_digest), 104);
	UT_ASSERT_EQ(offsetof(ClusterSemanticActivationAckTableV1, expected), 112);
	UT_ASSERT_EQ(offsetof(ClusterSemanticActivationAckTableV1, observed), 8304);
}

UT_TEST(test_63_ack_tuple_encoder_is_exact_and_clears_reserved_gaps)
{
	SemanticActivationAckTuple tuple;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_ACK_TUPLE_BYTES];

	memset(&tuple, 0, sizeof(tuple));
	tuple.node_id = 3;
	tuple.boot_id = UINT64_C(0x0102030405060708);
	tuple.admitted_incarnation = UINT64_C(0x0102030405060708);
	tuple.control_connection_generation = UINT64_C(0x1112131415161718);
	tuple.capability_word = UINT32_C(0x0030B000);
	tuple.capability_generation = UINT64_C(0x2122232425262728);
	tuple.transition_epoch = UINT64_C(0x3132333435363738);
	tuple.record_generation = UINT64_C(0x4142434445464748);
	memset(bytes, 0xa5, sizeof(bytes));

	UT_ASSERT(semantic_activation_ack_tuple_encode(&tuple, bytes));
	UT_ASSERT_EQ(read_u32_le(bytes), UINT32_C(3));
	UT_ASSERT_EQ(read_u32_le(bytes + 4), UINT32_C(0));
	UT_ASSERT_EQ(read_u64_le(bytes + 8), UINT64_C(0x0102030405060708));
	UT_ASSERT_EQ(read_u64_le(bytes + 16), UINT64_C(0x0102030405060708));
	UT_ASSERT_EQ(read_u64_le(bytes + 24), UINT64_C(0x1112131415161718));
	UT_ASSERT_EQ(read_u32_le(bytes + 32), UINT32_C(0x0030B000));
	UT_ASSERT_EQ(read_u32_le(bytes + 36), UINT32_C(0));
	UT_ASSERT_EQ(read_u64_le(bytes + 40), UINT64_C(0x2122232425262728));
	UT_ASSERT_EQ(read_u64_le(bytes + 48), UINT64_C(0x3132333435363738));
	UT_ASSERT_EQ(read_u64_le(bytes + 56), UINT64_C(0x4142434445464748));
}

UT_TEST(test_64_shmem_size_includes_exact_ack_table)
{
	Size expected = MAXALIGN(sizeof(ClusterSemanticActivationShmem))
					+ MAXALIGN(sizeof(ClusterSemanticActivationUtilityMailboxShmem))
					+ MAXALIGN(CLUSTER_SEMANTIC_ACTIVATION_ACK_TABLE_BYTES)
					+ MAXALIGN(sizeof(ClusterSemanticActivationPgrdSnapshotShmem))
					/* implementation/implementation: the bit22 cutover latch (contract §B). */
					+ MAXALIGN(sizeof(ClusterR4Bit22CutoverLatchShmem))
					/* contract: the cutover round seam (step ②). */
					+ MAXALIGN(sizeof(ClusterR4Bit22CutoverSeamShmem))
					/* contract: online first-open via the source-close
					 * BARRIER. */
					+ MAXALIGN(sizeof(ClusterR4Bit22SourceCloseShmem))
					+ MAXALIGN(sizeof(pg_atomic_uint32)) /* local clean-start observation */
					+ MAXALIGN(sizeof(ClusterNormalStartCompletion));

	UT_ASSERT_EQ(cluster_semantic_activation_shmem_size(), expected);
}

UT_TEST(test_65_ack_table_snapshot_accepts_even_and_rejects_odd)
{
	ClusterSemanticActivationAckTableV1 table;
	ClusterSemanticActivationAckTableV1 snapshot;

	memset(&table, 0, sizeof(table));
	pg_atomic_init_u64(&table.publication_seq, 2);
	table.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER;
	table.flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID;
	table.coordinator_node = 1;
	table.round_nonce = 7;
	table.expected_members_lo = UINT64_C(0x0f);
	table.record_generation = 9;
	table.expected[3].node_id = 3;
	table.expected[3].boot_id = 11;
	SemanticActivationAckTable = &table;
	memset(&snapshot, 0xa5, sizeof(snapshot));

	UT_ASSERT(semantic_activation_ack_table_snapshot(&snapshot));
	UT_ASSERT_EQ(pg_atomic_read_u64(&snapshot.publication_seq), 2);
	UT_ASSERT_EQ(snapshot.stage, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
	UT_ASSERT_EQ(snapshot.flags, CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID);
	UT_ASSERT_EQ(snapshot.coordinator_node, 1);
	UT_ASSERT_EQ(snapshot.round_nonce, 7);
	UT_ASSERT_EQ(snapshot.expected_members_lo, UINT64_C(0x0f));
	UT_ASSERT_EQ(snapshot.record_generation, 9);
	UT_ASSERT_EQ(snapshot.expected[3].node_id, 3);
	UT_ASSERT_EQ(snapshot.expected[3].boot_id, 11);

	pg_atomic_write_u64(&table.publication_seq, 3);
	memset(&snapshot, 0xa5, sizeof(snapshot));
	UT_ASSERT(!semantic_activation_ack_table_snapshot(&snapshot));
	UT_ASSERT_EQ(snapshot.stage, UINT32_C(0xa5a5a5a5));
	SemanticActivationAckTable = NULL;
}

UT_TEST(test_66_ack_table_publish_advances_even_and_refuses_bad_sequence)
{
	ClusterSemanticActivationAckTableV1 table;
	ClusterSemanticActivationAckTableV1 image;

	memset(&table, 0, sizeof(table));
	memset(&image, 0, sizeof(image));
	pg_atomic_init_u64(&table.publication_seq, 4);
	pg_atomic_init_u64(&image.publication_seq, 100);
	image.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER;
	image.flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID;
	image.coordinator_node = 1;
	image.round_nonce = 7;
	image.expected_members_lo = UINT64_C(0x0f);
	image.transition_epoch = 8;
	image.record_generation = 9;
	image.expected[3].node_id = 3;
	image.expected[3].boot_id = 11;
	SemanticActivationAckTable = &table;

	UT_ASSERT(semantic_activation_ack_table_publish(&image));
	UT_ASSERT_EQ(pg_atomic_read_u64(&table.publication_seq), 6);
	UT_ASSERT_EQ(table.stage, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
	UT_ASSERT_EQ(table.flags, CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID);
	UT_ASSERT_EQ(table.coordinator_node, 1);
	UT_ASSERT_EQ(table.round_nonce, 7);
	UT_ASSERT_EQ(table.expected_members_lo, UINT64_C(0x0f));
	UT_ASSERT_EQ(table.transition_epoch, 8);
	UT_ASSERT_EQ(table.record_generation, 9);
	UT_ASSERT_EQ(table.expected[3].node_id, 3);
	UT_ASSERT_EQ(table.expected[3].boot_id, 11);

	pg_atomic_write_u64(&table.publication_seq, 7);
	image.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED;
	UT_ASSERT(!semantic_activation_ack_table_publish(&image));
	UT_ASSERT_EQ(pg_atomic_read_u64(&table.publication_seq), 7);
	UT_ASSERT_EQ(table.stage, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
	pg_atomic_write_u64(&table.publication_seq, UINT64_MAX - 1);
	UT_ASSERT(!semantic_activation_ack_table_publish(&image));
	UT_ASSERT_EQ(pg_atomic_read_u64(&table.publication_seq), UINT64_MAX - 1);
	UT_ASSERT_EQ(table.stage, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
	SemanticActivationAckTable = NULL;
}

UT_TEST(test_67_ack_ingress_handoff_has_exact_frozen_layout)
{
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ACTIVATION_ACK_INGRESS_CAPACITY, 256);
	UT_ASSERT_EQ(sizeof(ClusterSemanticActivationAckWireV1), 120);
	UT_ASSERT_EQ(sizeof(SemanticActivationAckIngressItem), 136);
	UT_ASSERT_EQ(offsetof(SemanticActivationAckIngressItem, message), 0);
	UT_ASSERT_EQ(offsetof(SemanticActivationAckIngressItem, authenticated_source_node_id), 120);
	UT_ASSERT_EQ(offsetof(SemanticActivationAckIngressItem, local_receiver_node_id), 124);
	UT_ASSERT_EQ(offsetof(SemanticActivationAckIngressItem, sampled_capability_word), 128);
	UT_ASSERT_EQ(offsetof(SemanticActivationAckIngressItem, sampled_capability_generation), 132);
	UT_ASSERT_EQ(sizeof(SemanticActivationAckIngress), 34832);
	UT_ASSERT_EQ(offsetof(SemanticActivationAckIngress, producer_seq), 0);
	UT_ASSERT_EQ(offsetof(SemanticActivationAckIngress, consumer_seq), 8);
	UT_ASSERT_EQ(offsetof(SemanticActivationAckIngress, items), 16);
}

UT_TEST(test_68_ack_ingress_handoff_is_bounded_and_fail_closed)
{
	SemanticActivationAckIngress ingress;
	SemanticActivationAckIngressItem item;
	SemanticActivationAckIngressItem out;
	uint32 i;

	memset(&item, 0, sizeof(item));
	item.message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK;
	item.message.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	item.authenticated_source_node_id = 3;
	item.local_receiver_node_id = 1;
	item.sampled_capability_word = CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	item.sampled_capability_generation = 7;
	semantic_activation_ack_ingress_init(&ingress);
	UT_ASSERT_EQ(semantic_activation_ack_ingress_pending(&ingress), 0);
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!semantic_activation_ack_ingress_poll(&ingress, &out));
	UT_ASSERT_EQ((uint32)out.authenticated_source_node_id, UINT32_C(0xa5a5a5a5));

	for (i = 0; i < CLUSTER_SEMANTIC_ACTIVATION_ACK_INGRESS_CAPACITY; i++) {
		item.message.member_node = i % CLUSTER_MAX_NODES;
		UT_ASSERT(semantic_activation_ack_ingress_push(&ingress, &item));
	}
	UT_ASSERT_EQ(semantic_activation_ack_ingress_pending(&ingress), 256);
	UT_ASSERT(!semantic_activation_ack_ingress_push(&ingress, &item));
	UT_ASSERT_EQ(ingress.producer_seq, UINT64_C(256));
	UT_ASSERT(semantic_activation_ack_ingress_poll(&ingress, &out));
	UT_ASSERT_EQ(out.message.kind, CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK);
	UT_ASSERT_EQ(out.authenticated_source_node_id, 3);
	UT_ASSERT_EQ(out.local_receiver_node_id, 1);
	UT_ASSERT_EQ(out.sampled_capability_word, CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS);
	UT_ASSERT_EQ(out.sampled_capability_generation, 7);
	UT_ASSERT_EQ(semantic_activation_ack_ingress_pending(&ingress), 255);

	ingress.producer_seq = CLUSTER_SEMANTIC_ACTIVATION_ACK_INGRESS_CAPACITY + 2;
	ingress.consumer_seq = 0;
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT_EQ(semantic_activation_ack_ingress_pending(&ingress), 0);
	UT_ASSERT(!semantic_activation_ack_ingress_push(&ingress, &item));
	UT_ASSERT(!semantic_activation_ack_ingress_poll(&ingress, &out));
	UT_ASSERT_EQ((uint32)out.authenticated_source_node_id, UINT32_C(0xa5a5a5a5));
}

static ClusterSemanticActivationAckWireV1
valid_positive_ack(void)
{
	ClusterSemanticActivationAckWireV1 message;

	memset(&message, 0, sizeof(message));
	message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK;
	message.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK;
	message.coordinator_node = 0;
	message.member_node = 3;
	message.transition_epoch = 9;
	message.record_generation = 10;
	message.round_nonce = 11;
	message.source_feature_bitmap = 0;
	message.target_feature_bitmap = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	message.admitted_members_lo = UINT64_C(0x0b);
	message.boot_id = 12;
	message.admitted_incarnation = 12;
	message.capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS | PGRAC_IC_HELLO_CAP_GCS_DONE_V1;
	return message;
}

static ClusterICEnvelope
valid_ack_envelope(uint32 source_node, uint32 dest_node)
{
	ClusterICEnvelope env;

	memset(&env, 0, sizeof(env));
	env.msg_type = PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1;
	env.source_node_id = source_node;
	env.dest_node_id = dest_node;
	env.epoch = 9;
	env.payload_length = CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES;
	return env;
}

UT_TEST(test_69_ack_ingress_stamps_coherent_remote_sample)
{
	SemanticActivationAckIngress ingress;
	SemanticActivationAckIngressItem out;
	ClusterSemanticActivationAckWireV1 message = valid_positive_ack();
	ClusterICEnvelope env = valid_ack_envelope(3, 1);
	uint8 payload[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];

	cluster_r4_activation_test_capability_word_sample_ok = true;
	cluster_r4_activation_test_capability_word = message.capability_word;
	cluster_r4_activation_test_capability_generation = 7;
	semantic_activation_ack_ingress_init(&ingress);
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(&message, payload));
	UT_ASSERT_EQ(
		semantic_activation_ack_ingress_receive(&ingress, &env, payload, sizeof(payload), 1, 9),
		SEMANTIC_ACTIVATION_ACK_INGRESS_ENQUEUED);
	UT_ASSERT(semantic_activation_ack_ingress_poll(&ingress, &out));
	UT_ASSERT_EQ(memcmp(&out.message, &message, sizeof(message)), 0);
	UT_ASSERT_EQ(out.authenticated_source_node_id, 3);
	UT_ASSERT_EQ(out.local_receiver_node_id, 1);
	UT_ASSERT_EQ(out.sampled_capability_word, message.capability_word);
	UT_ASSERT_EQ(out.sampled_capability_generation, 7);
}

UT_TEST(test_70_ack_ingress_rejects_role_epoch_and_sample_drift)
{
	SemanticActivationAckIngress ingress;
	ClusterSemanticActivationAckWireV1 message = valid_positive_ack();
	ClusterICEnvelope env = valid_ack_envelope(3, 1);
	uint8 payload[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];

	cluster_r4_activation_test_capability_word_sample_ok = true;
	cluster_r4_activation_test_capability_word = message.capability_word;
	cluster_r4_activation_test_capability_generation = 7;
	semantic_activation_ack_ingress_init(&ingress);
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(&message, payload));

	UT_ASSERT_EQ(
		semantic_activation_ack_ingress_receive(&ingress, &env, payload, sizeof(payload) - 1, 1, 9),
		SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED);
	env.dest_node_id = 2;
	UT_ASSERT_EQ(
		semantic_activation_ack_ingress_receive(&ingress, &env, payload, sizeof(payload), 1, 9),
		SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED);
	env = valid_ack_envelope(3, 1);
	env.epoch = 8;
	UT_ASSERT_EQ(
		semantic_activation_ack_ingress_receive(&ingress, &env, payload, sizeof(payload), 1, 9),
		SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED);
	env = valid_ack_envelope(4, 1);
	UT_ASSERT_EQ(
		semantic_activation_ack_ingress_receive(&ingress, &env, payload, sizeof(payload), 1, 9),
		SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED);
	env = valid_ack_envelope(3, 1);
	cluster_r4_activation_test_capability_word_sample_ok = false;
	UT_ASSERT_EQ(
		semantic_activation_ack_ingress_receive(&ingress, &env, payload, sizeof(payload), 1, 9),
		SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED);
	cluster_r4_activation_test_capability_word_sample_ok = true;
	cluster_r4_activation_test_capability_generation = 0;
	UT_ASSERT_EQ(
		semantic_activation_ack_ingress_receive(&ingress, &env, payload, sizeof(payload), 1, 9),
		SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED);
	cluster_r4_activation_test_capability_generation = 7;
	cluster_r4_activation_test_capability_word ^= PGRAC_IC_HELLO_CAP_GCS_DONE_V1;
	UT_ASSERT_EQ(
		semantic_activation_ack_ingress_receive(&ingress, &env, payload, sizeof(payload), 1, 9),
		SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED);
	cluster_r4_activation_test_capability_word = message.capability_word;
	message.admitted_incarnation++;
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(&message, payload));
	UT_ASSERT_EQ(
		semantic_activation_ack_ingress_receive(&ingress, &env, payload, sizeof(payload), 1, 9),
		SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED);
	UT_ASSERT_EQ(semantic_activation_ack_ingress_pending(&ingress), 0);
}

UT_TEST(test_71_ack_ingress_accepts_request_and_refusal_roles)
{
	SemanticActivationAckIngress ingress;
	ClusterSemanticActivationAckWireV1 message;
	ClusterICEnvelope env;
	uint8 payload[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];

	cluster_r4_activation_test_capability_word_sample_ok = true;
	cluster_r4_activation_test_capability_word = CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	cluster_r4_activation_test_capability_generation = 7;
	semantic_activation_ack_ingress_init(&ingress);
	memset(&message, 0, sizeof(message));
	message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	message.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	message.coordinator_node = 3;
	message.member_node = 1;
	message.transition_epoch = 9;
	message.record_generation = 10;
	message.round_nonce = 11;
	message.source_feature_bitmap = 0;
	message.target_feature_bitmap = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	message.admitted_members_lo = UINT64_C(0x0a);
	env = valid_ack_envelope(3, 1);
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(&message, payload));
	UT_ASSERT_EQ(
		semantic_activation_ack_ingress_receive(&ingress, &env, payload, sizeof(payload), 1, 9),
		SEMANTIC_ACTIVATION_ACK_INGRESS_ENQUEUED);

	message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK;
	message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REFUSED;
	message.reason = CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
	message.coordinator_node = 1;
	message.member_node = 3;
	env = valid_ack_envelope(3, 1);
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(&message, payload));
	UT_ASSERT_EQ(
		semantic_activation_ack_ingress_receive(&ingress, &env, payload, sizeof(payload), 1, 9),
		SEMANTIC_ACTIVATION_ACK_INGRESS_ENQUEUED);
	UT_ASSERT_EQ(semantic_activation_ack_ingress_pending(&ingress), 2);
}

UT_TEST(test_72_ack_handler_only_enqueues_and_counts_typed_drops)
{
	ClusterSemanticActivationAckWireV1 message = valid_positive_ack();
	ClusterICEnvelope env = valid_ack_envelope(3, 1);
	uint8 payload[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];

	message.transition_epoch = 0;
	env.epoch = 0;
	cluster_node_id = 1;
	cluster_r4_activation_test_capability_word_sample_ok = true;
	cluster_r4_activation_test_capability_word = message.capability_word;
	cluster_r4_activation_test_capability_generation = 7;
	semantic_activation_ack_ingress_init(&semantic_activation_ack_local_ingress);
	memset(semantic_activation_ack_ingress_result_count, 0,
		   sizeof(semantic_activation_ack_ingress_result_count));
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(&message, payload));

	cluster_semantic_activation_ack_handler(&env, payload);
	UT_ASSERT_EQ(semantic_activation_ack_ingress_pending(&semantic_activation_ack_local_ingress),
				 1);
	UT_ASSERT_EQ(
		semantic_activation_ack_ingress_result_count[SEMANTIC_ACTIVATION_ACK_INGRESS_ENQUEUED],
		UINT64_C(1));
	UT_ASSERT_EQ(
		semantic_activation_ack_ingress_result_count[SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED],
		UINT64_C(0));
	payload[119] = 1;
	cluster_semantic_activation_ack_handler(&env, payload);
	UT_ASSERT_EQ(semantic_activation_ack_ingress_pending(&semantic_activation_ack_local_ingress),
				 1);
	UT_ASSERT_EQ(
		semantic_activation_ack_ingress_result_count[SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED],
		UINT64_C(1));
	payload[119] = 0;
	semantic_activation_ack_local_ingress.consumer_seq = 0;
	semantic_activation_ack_local_ingress.producer_seq
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_INGRESS_CAPACITY;
	cluster_semantic_activation_ack_handler(&env, payload);
	UT_ASSERT_EQ(semantic_activation_ack_ingress_result_count[SEMANTIC_ACTIVATION_ACK_INGRESS_FULL],
				 UINT64_C(1));
}

static SemanticActivationAckIngressItem
valid_positive_ack_item(void)
{
	SemanticActivationAckIngressItem item;

	memset(&item, 0, sizeof(item));
	item.message = valid_positive_ack();
	item.authenticated_source_node_id = 3;
	item.local_receiver_node_id = 0;
	item.sampled_capability_word = item.message.capability_word;
	item.sampled_capability_generation = 7;
	return item;
}

static void
assert_remote_ack_tuple_rejected(const SemanticActivationAckIngressItem *item,
								 uint64 current_members_lo, uint64 current_members_hi,
								 uint64 current_epoch, int32 current_coordinator)
{
	SemanticActivationAckTuple output;
	SemanticActivationAckTuple before;

	memset(&output, 0xa5, sizeof(output));
	before = output;
	UT_ASSERT(!semantic_activation_ack_remote_tuple(item, current_members_lo, current_members_hi,
													current_epoch, current_coordinator, &output));
	UT_ASSERT_EQ(memcmp(&output, &before, sizeof(output)), 0);
}

UT_TEST(test_73_lmon_remote_ack_builds_exact_current_tuple)
{
	SemanticActivationAckIngressItem item = valid_positive_ack_item();
	SemanticActivationAckTuple output;

	cluster_r4_activation_test_membership_node = 3;
	cluster_r4_activation_test_membership_floor = 12;
	cluster_r4_activation_test_membership_state = CLUSTER_MEMBER_MEMBER;
	cluster_r4_activation_test_capability_generation_matches = true;
	cluster_r4_activation_test_capability_peer = 3;
	cluster_r4_activation_test_capability_required = CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	cluster_r4_activation_test_capability_expected_generation = 7;
	memset(&output, 0xa5, sizeof(output));

	UT_ASSERT(semantic_activation_ack_remote_tuple(&item, UINT64_C(0x0b), 0, 9, 0, &output));
	UT_ASSERT_EQ(output.node_id, 3);
	UT_ASSERT_EQ(output.boot_id, UINT64_C(12));
	UT_ASSERT_EQ(output.admitted_incarnation, UINT64_C(12));
	UT_ASSERT_EQ(output.control_connection_generation, UINT64_C(7));
	UT_ASSERT_EQ(output.capability_word, item.message.capability_word);
	UT_ASSERT_EQ(output.capability_generation, UINT64_C(7));
	UT_ASSERT_EQ(output.transition_epoch, UINT64_C(9));
	UT_ASSERT_EQ(output.record_generation, UINT64_C(10));
}

UT_TEST(test_74_lmon_remote_ack_revalidates_every_authority_input)
{
	SemanticActivationAckIngressItem item = valid_positive_ack_item();

	cluster_r4_activation_test_membership_node = 3;
	cluster_r4_activation_test_membership_floor = 12;
	cluster_r4_activation_test_membership_state = CLUSTER_MEMBER_MEMBER;
	cluster_r4_activation_test_capability_generation_matches = true;
	cluster_r4_activation_test_capability_peer = 3;
	cluster_r4_activation_test_capability_required = CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	cluster_r4_activation_test_capability_expected_generation = 7;

	assert_remote_ack_tuple_rejected(NULL, UINT64_C(0x0b), 0, 9, 0);
	assert_remote_ack_tuple_rejected(&item, UINT64_C(0x0b), 0, 9, -1);

	item.message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	assert_remote_ack_tuple_rejected(&item, UINT64_C(0x0b), 0, 9, 0);
	item = valid_positive_ack_item();
	item.message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REFUSED;
	assert_remote_ack_tuple_rejected(&item, UINT64_C(0x0b), 0, 9, 0);
	item = valid_positive_ack_item();
	item.message.member_node = 4;
	assert_remote_ack_tuple_rejected(&item, UINT64_C(0x0b), 0, 9, 0);
	item = valid_positive_ack_item();
	item.message.coordinator_node = 1;
	assert_remote_ack_tuple_rejected(&item, UINT64_C(0x0b), 0, 9, 0);
	item = valid_positive_ack_item();
	item.local_receiver_node_id = 2;
	assert_remote_ack_tuple_rejected(&item, UINT64_C(0x0b), 0, 9, 0);
	item = valid_positive_ack_item();
	assert_remote_ack_tuple_rejected(&item, UINT64_C(0x07), 0, 9, 0);
	assert_remote_ack_tuple_rejected(&item, UINT64_C(0x0b), 1, 9, 0);
	assert_remote_ack_tuple_rejected(&item, UINT64_C(0x0b), 0, 8, 0);

	cluster_r4_activation_test_membership_state = CLUSTER_MEMBER_JOINING;
	assert_remote_ack_tuple_rejected(&item, UINT64_C(0x0b), 0, 9, 0);
	cluster_r4_activation_test_membership_state = CLUSTER_MEMBER_MEMBER;
	cluster_r4_activation_test_membership_floor = 13;
	assert_remote_ack_tuple_rejected(&item, UINT64_C(0x0b), 0, 9, 0);
	cluster_r4_activation_test_membership_floor = 12;

	item.sampled_capability_word ^= PGRAC_IC_HELLO_CAP_GCS_DONE_V1;
	assert_remote_ack_tuple_rejected(&item, UINT64_C(0x0b), 0, 9, 0);
	item = valid_positive_ack_item();
	item.sampled_capability_generation = 0;
	assert_remote_ack_tuple_rejected(&item, UINT64_C(0x0b), 0, 9, 0);
	item = valid_positive_ack_item();
	cluster_r4_activation_test_capability_generation_matches = false;
	assert_remote_ack_tuple_rejected(&item, UINT64_C(0x0b), 0, 9, 0);
}

UT_TEST(test_75_lmon_self_ack_builds_exact_local_tuple)
{
	SemanticActivationAckTuple output;
	uint32 local_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS | PGRAC_IC_HELLO_CAP_GCS_DONE_V1;

	cluster_r4_activation_test_self_incarnation = 11;
	cluster_r4_activation_test_membership_node = 0;
	cluster_r4_activation_test_membership_floor = 11;
	cluster_r4_activation_test_membership_state = CLUSTER_MEMBER_MEMBER;
	memset(&output, 0xa5, sizeof(output));

	UT_ASSERT(semantic_activation_ack_self_tuple(0, local_capability_word, 9, 10, &output));
	UT_ASSERT_EQ(output.node_id, 0);
	UT_ASSERT_EQ(output.boot_id, UINT64_C(11));
	UT_ASSERT_EQ(output.admitted_incarnation, UINT64_C(11));
	UT_ASSERT_EQ(output.control_connection_generation, UINT64_C(11));
	UT_ASSERT_EQ(output.capability_word, local_capability_word);
	UT_ASSERT_EQ(output.capability_generation, UINT64_C(11));
	UT_ASSERT_EQ(output.transition_epoch, UINT64_C(9));
	UT_ASSERT_EQ(output.record_generation, UINT64_C(10));
}

UT_TEST(test_76_lmon_self_ack_is_fail_closed_on_local_drift)
{
	SemanticActivationAckTuple output;
	SemanticActivationAckTuple before;
	uint32 local_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS | PGRAC_IC_HELLO_CAP_GCS_DONE_V1;

	cluster_r4_activation_test_self_incarnation = 11;
	cluster_r4_activation_test_membership_node = 0;
	cluster_r4_activation_test_membership_floor = 11;
	cluster_r4_activation_test_membership_state = CLUSTER_MEMBER_MEMBER;
	memset(&output, 0xa5, sizeof(output));
	before = output;

	UT_ASSERT(!semantic_activation_ack_self_tuple(-1, local_capability_word, 9, 10, &output));
	UT_ASSERT_EQ(memcmp(&output, &before, sizeof(output)), 0);
	cluster_r4_activation_test_self_incarnation = 0;
	UT_ASSERT(!semantic_activation_ack_self_tuple(0, local_capability_word, 9, 10, &output));
	UT_ASSERT_EQ(memcmp(&output, &before, sizeof(output)), 0);
	cluster_r4_activation_test_self_incarnation = 11;
	cluster_r4_activation_test_membership_state = CLUSTER_MEMBER_JOINING;
	UT_ASSERT(!semantic_activation_ack_self_tuple(0, local_capability_word, 9, 10, &output));
	UT_ASSERT_EQ(memcmp(&output, &before, sizeof(output)), 0);
	cluster_r4_activation_test_membership_state = CLUSTER_MEMBER_MEMBER;
	cluster_r4_activation_test_membership_floor = 12;
	UT_ASSERT(!semantic_activation_ack_self_tuple(0, local_capability_word, 9, 10, &output));
	UT_ASSERT_EQ(memcmp(&output, &before, sizeof(output)), 0);
	cluster_r4_activation_test_membership_floor = 11;
	UT_ASSERT(!semantic_activation_ack_self_tuple(
		0, local_capability_word & ~PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_ACK_V1, 9, 10, &output));
	UT_ASSERT_EQ(memcmp(&output, &before, sizeof(output)), 0);
	UT_ASSERT(!semantic_activation_ack_self_tuple(0, local_capability_word, 9, 0, &output));
	UT_ASSERT_EQ(memcmp(&output, &before, sizeof(output)), 0);
}

static void assert_current_ack_authority_rejected(int32 local_node_id);

UT_TEST(test_77_lmon_current_ack_authority_uses_exact_member_snapshot)
{
	uint64 members_lo;
	uint64 members_hi;
	uint64 formation_epoch;
	int32 coordinator;

	cluster_r4_activation_test_in_quorum = true;
	cluster_r4_activation_test_admitted_snapshot_valid = true;
	cluster_r4_activation_test_admitted_members_lo = (UINT64_C(1) << 3) | (UINT64_C(1) << 7);
	cluster_r4_activation_test_admitted_members_hi = 0;
	cluster_r4_activation_test_admitted_epoch = 9;
	cluster_r4_activation_test_current_epoch = 9;
	cluster_r4_activation_test_membership_node = 7;
	cluster_r4_activation_test_membership_state = CLUSTER_MEMBER_MEMBER;

	UT_ASSERT(semantic_activation_ack_current_authority(7, &members_lo, &members_hi,
														&formation_epoch, &coordinator));
	UT_ASSERT_EQ(members_lo, (UINT64_C(1) << 3) | (UINT64_C(1) << 7));
	UT_ASSERT_EQ(members_hi, 0);
	UT_ASSERT_EQ(formation_epoch, UINT64_C(9));
	UT_ASSERT_EQ(coordinator, 3);

	cluster_r4_activation_test_admitted_members_lo = 0;
	cluster_r4_activation_test_admitted_members_hi = UINT64_C(1) << 1;
	cluster_r4_activation_test_membership_node = 65;
	assert_current_ack_authority_rejected(65);
}

static void
assert_current_ack_authority_rejected(int32 local_node_id)
{
	uint64 members_lo = UINT64_MAX;
	uint64 members_hi = UINT64_MAX;
	uint64 formation_epoch = UINT64_MAX;
	int32 coordinator = 127;

	UT_ASSERT(!semantic_activation_ack_current_authority(local_node_id, &members_lo, &members_hi,
														 &formation_epoch, &coordinator));
	UT_ASSERT_EQ(members_lo, 0);
	UT_ASSERT_EQ(members_hi, 0);
	UT_ASSERT_EQ(formation_epoch, 0);
	UT_ASSERT_EQ(coordinator, -1);
}

UT_TEST(test_78_lmon_current_ack_authority_fails_closed_on_drift)
{
	cluster_r4_activation_test_in_quorum = false;
	cluster_r4_activation_test_admitted_snapshot_valid = true;
	cluster_r4_activation_test_admitted_members_lo = UINT64_C(0x88);
	cluster_r4_activation_test_admitted_members_hi = 0;
	cluster_r4_activation_test_admitted_epoch = 9;
	cluster_r4_activation_test_current_epoch = 9;
	cluster_r4_activation_test_membership_node = 7;
	cluster_r4_activation_test_membership_state = CLUSTER_MEMBER_MEMBER;
	assert_current_ack_authority_rejected(7);

	cluster_r4_activation_test_in_quorum = true;
	cluster_r4_activation_test_admitted_snapshot_valid = false;
	assert_current_ack_authority_rejected(7);
	cluster_r4_activation_test_admitted_snapshot_valid = true;
	cluster_r4_activation_test_admitted_members_lo = 0;
	assert_current_ack_authority_rejected(7);
	cluster_r4_activation_test_admitted_members_lo = UINT64_C(0x88);
	cluster_r4_activation_test_admitted_epoch = 8;
	assert_current_ack_authority_rejected(7);
	cluster_r4_activation_test_admitted_epoch = 9;
	cluster_r4_activation_test_admitted_members_lo = UINT64_C(0x08);
	assert_current_ack_authority_rejected(7);
	cluster_r4_activation_test_admitted_members_lo = UINT64_C(0x88);
	cluster_r4_activation_test_membership_state = CLUSTER_MEMBER_JOINING;
	assert_current_ack_authority_rejected(7);
	assert_current_ack_authority_rejected(-1);
}

static SemanticActivationAckIngressItem
valid_current_sample_item(void)
{
	SemanticActivationAckIngressItem item = valid_positive_ack_item();

	item.message.admitted_members_lo = UINT64_C(0x09);
	item.local_receiver_node_id = 0;
	return item;
}

static SemanticActivationAckTuple
valid_current_self_tuple(void)
{
	return (SemanticActivationAckTuple){
		.node_id = 0,
		.boot_id = 11,
		.admitted_incarnation = 11,
		.control_connection_generation = 11,
		.capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS | PGRAC_IC_HELLO_CAP_GCS_DONE_V1,
		.capability_generation = 11,
		.transition_epoch = 9,
		.record_generation = 10,
	};
}

static void
init_current_sample_table(ClusterSemanticActivationAckTableV1 *table)
{
	memset(table, 0, sizeof(*table));
	pg_atomic_init_u64(&table->publication_seq, 0);
	table->stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	table->coordinator_node = 0;
	table->round_nonce = 11;
	table->expected_members_lo = UINT64_C(0x09);
	table->transition_epoch = 9;
	table->record_generation = 10;
	table->source_feature_bitmap = 0;
	table->target_feature_bitmap = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	table->observed_members_lo = UINT64_C(0x01);
	table->observed[0] = valid_current_self_tuple();
}

static void
set_current_remote_ack_dependencies(void)
{
	cluster_r4_activation_test_membership_node = 3;
	cluster_r4_activation_test_membership_floor = 12;
	cluster_r4_activation_test_membership_state = CLUSTER_MEMBER_MEMBER;
	cluster_r4_activation_test_capability_generation_matches = true;
	cluster_r4_activation_test_capability_peer = 3;
	cluster_r4_activation_test_capability_required = CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	cluster_r4_activation_test_capability_expected_generation = 7;
}

UT_TEST(test_79_lmon_sample_ack_promotes_complete_observed_image)
{
	ClusterSemanticActivationAckTableV1 table;
	SemanticActivationAckIngressItem item = valid_current_sample_item();

	init_current_sample_table(&table);
	set_current_remote_ack_dependencies();
	SemanticActivationAckTable = &table;
	UT_ASSERT_EQ(semantic_activation_ack_lmon_apply_item(&item, UINT64_C(0x09), 0, 9, 0),
				 SEMANTIC_ACTIVATION_ACK_CONSUME_APPLIED);
	UT_ASSERT_EQ(pg_atomic_read_u64(&table.publication_seq), 2);
	UT_ASSERT_EQ(table.flags, CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
								  | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE);
	UT_ASSERT_EQ(table.expected_members_lo, UINT64_C(0x09));
	UT_ASSERT_EQ(table.observed_members_lo, UINT64_C(0x09));
	UT_ASSERT(semantic_activation_full_ack_table_matches(
		table.observed, table.observed_members_lo, table.observed_members_hi, table.expected,
		table.expected_members_lo, table.expected_members_hi));
	UT_ASSERT_EQ(table.expected[3].node_id, 3);
	UT_ASSERT_EQ(table.expected[3].boot_id, UINT64_C(12));
	UT_ASSERT_EQ(table.expected[3].capability_generation, UINT64_C(7));
	SemanticActivationAckTable = NULL;
}

UT_TEST(test_80_lmon_ack_stale_drop_and_current_drift_invalidation)
{
	ClusterSemanticActivationAckTableV1 table;
	ClusterSemanticActivationAckTableV1 before;
	SemanticActivationAckIngressItem item = valid_current_sample_item();

	init_current_sample_table(&table);
	set_current_remote_ack_dependencies();
	SemanticActivationAckTable = &table;
	item.message.round_nonce++;
	before = table;
	UT_ASSERT_EQ(semantic_activation_ack_lmon_apply_item(&item, UINT64_C(0x09), 0, 9, 0),
				 SEMANTIC_ACTIVATION_ACK_CONSUME_STALE);
	UT_ASSERT_EQ(memcmp(&table, &before, sizeof(table)), 0);

	item = valid_current_sample_item();
	UT_ASSERT_EQ(semantic_activation_ack_lmon_apply_item(&item, UINT64_C(0x19), 0, 9, 0),
				 SEMANTIC_ACTIVATION_ACK_CONSUME_INVALIDATED);
	UT_ASSERT_EQ(pg_atomic_read_u64(&table.publication_seq), 2);
	UT_ASSERT_EQ(table.flags, 0);
	UT_ASSERT_EQ(table.expected_members_lo, 0);
	UT_ASSERT_EQ(table.expected_members_hi, 0);
	UT_ASSERT_EQ(table.observed_members_lo, 0);
	UT_ASSERT_EQ(table.observed_members_hi, 0);
	UT_ASSERT_EQ(memcmp(table.expected, (SemanticActivationAckTuple[CLUSTER_MAX_NODES]){ { 0 } },
						sizeof(table.expected)),
				 0);
	UT_ASSERT_EQ(memcmp(table.observed, (SemanticActivationAckTuple[CLUSTER_MAX_NODES]){ { 0 } },
						sizeof(table.observed)),
				 0);

	init_current_sample_table(&table);
	item = valid_current_sample_item();
	item.message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REFUSED;
	item.message.reason = CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
	item.message.boot_id = 0;
	item.message.admitted_incarnation = 0;
	item.message.capability_word = 0;
	UT_ASSERT_EQ(semantic_activation_ack_lmon_apply_item(&item, UINT64_C(0x09), 0, 9, 0),
				 SEMANTIC_ACTIVATION_ACK_CONSUME_INVALIDATED);
	UT_ASSERT_EQ(table.flags, 0);
	UT_ASSERT_EQ(table.expected_members_lo, 0);
	UT_ASSERT_EQ(table.observed_members_lo, 0);
	SemanticActivationAckTable = NULL;
}

UT_TEST(test_81_lmon_ack_duplicate_is_idempotent_but_conflict_invalidates)
{
	ClusterSemanticActivationAckTableV1 table;
	SemanticActivationAckIngressItem item = valid_current_sample_item();
	uint64 publication_seq;

	init_current_sample_table(&table);
	set_current_remote_ack_dependencies();
	SemanticActivationAckTable = &table;
	UT_ASSERT_EQ(semantic_activation_ack_lmon_apply_item(&item, UINT64_C(0x09), 0, 9, 0),
				 SEMANTIC_ACTIVATION_ACK_CONSUME_APPLIED);
	publication_seq = pg_atomic_read_u64(&table.publication_seq);
	UT_ASSERT_EQ(semantic_activation_ack_lmon_apply_item(&item, UINT64_C(0x09), 0, 9, 0),
				 SEMANTIC_ACTIVATION_ACK_CONSUME_DUPLICATE);
	UT_ASSERT_EQ(pg_atomic_read_u64(&table.publication_seq), publication_seq);

	item.message.boot_id = 13;
	item.message.admitted_incarnation = 13;
	cluster_r4_activation_test_membership_floor = 13;
	UT_ASSERT_EQ(semantic_activation_ack_lmon_apply_item(&item, UINT64_C(0x09), 0, 9, 0),
				 SEMANTIC_ACTIVATION_ACK_CONSUME_INVALIDATED);
	UT_ASSERT_EQ(table.flags, 0);
	UT_ASSERT_EQ(table.expected_members_lo, 0);
	UT_ASSERT_EQ(table.observed_members_lo, 0);
	SemanticActivationAckTable = NULL;
}

static void
set_current_ack_authority_dependencies(void)
{
	cluster_r4_activation_test_in_quorum = true;
	cluster_r4_activation_test_admitted_snapshot_valid = true;
	cluster_r4_activation_test_admitted_members_lo = UINT64_C(0x09);
	cluster_r4_activation_test_admitted_members_hi = 0;
	cluster_r4_activation_test_admitted_epoch = 9;
	cluster_r4_activation_test_current_epoch = 9;
}

UT_TEST(test_82_semantic_lmon_tick_drains_ack_ingress_before_other_work)
{
	ClusterSemanticActivationAckTableV1 table;
	SemanticActivationAckIngressItem item = valid_current_sample_item();

	init_current_sample_table(&table);
	set_current_remote_ack_dependencies();
	set_current_ack_authority_dependencies();
	cluster_node_id = 0;
	SemanticActivationShmem = NULL;
	SemanticActivationAckTable = &table;
	semantic_activation_ack_ingress_init(&semantic_activation_ack_local_ingress);
	UT_ASSERT(semantic_activation_ack_ingress_push(&semantic_activation_ack_local_ingress, &item));

	cluster_semantic_activation_lmon_tick();

	UT_ASSERT_EQ(semantic_activation_ack_ingress_pending(&semantic_activation_ack_local_ingress),
				 0);
	UT_ASSERT_EQ(table.flags, CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
								  | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE);
	UT_ASSERT_EQ(table.observed_members_lo, UINT64_C(0x09));
	SemanticActivationAckTable = NULL;
}

UT_TEST(test_83_semantic_lmon_tick_invalidates_active_ack_on_authority_loss)
{
	ClusterSemanticActivationAckTableV1 table;

	init_current_sample_table(&table);
	set_current_ack_authority_dependencies();
	cluster_r4_activation_test_in_quorum = false;
	cluster_node_id = 0;
	SemanticActivationShmem = NULL;
	SemanticActivationAckTable = &table;
	semantic_activation_ack_ingress_init(&semantic_activation_ack_local_ingress);

	cluster_semantic_activation_lmon_tick();

	UT_ASSERT_EQ(pg_atomic_read_u64(&table.publication_seq), 2);
	UT_ASSERT_EQ(table.flags, 0);
	UT_ASSERT_EQ(table.expected_members_lo, 0);
	UT_ASSERT_EQ(table.observed_members_lo, 0);
	cluster_r4_activation_test_in_quorum = true;
	SemanticActivationAckTable = NULL;
}

UT_TEST(test_84_semantic_lmon_idle_ack_path_takes_no_authority_snapshot)
{
	ClusterSemanticActivationAckTableV1 table;

	memset(&table, 0, sizeof(table));
	pg_atomic_init_u64(&table.publication_seq, 0);
	cluster_node_id = 0;
	SemanticActivationShmem = NULL;
	SemanticActivationAckTable = &table;
	semantic_activation_ack_ingress_init(&semantic_activation_ack_local_ingress);
	cluster_r4_activation_test_admitted_snapshot_calls = 0;

	cluster_semantic_activation_lmon_tick();

	UT_ASSERT_EQ(cluster_r4_activation_test_admitted_snapshot_calls, 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&table.publication_seq), 0);
	SemanticActivationAckTable = NULL;
}

static void
build_complete_current_sample_table(ClusterSemanticActivationAckTableV1 *table,
									SemanticActivationAckIngressItem *item)
{
	init_current_sample_table(table);
	*item = valid_current_sample_item();
	set_current_remote_ack_dependencies();
	cluster_r4_activation_test_membership_node2 = 0;
	cluster_r4_activation_test_membership_floor2 = 11;
	cluster_r4_activation_test_membership_state2 = CLUSTER_MEMBER_MEMBER;
	cluster_r4_activation_test_self_incarnation = 11;
	cluster_r4_activation_test_capability_word_sample_ok = true;
	cluster_r4_activation_test_capability_word = item->message.capability_word;
	cluster_r4_activation_test_capability_generation = 7;
	SemanticActivationAckTable = table;
	UT_ASSERT_EQ(semantic_activation_ack_lmon_apply_item(item, UINT64_C(0x09), 0, 9, 0),
				 SEMANTIC_ACTIVATION_ACK_CONSUME_APPLIED);
	SemanticActivationAckTable = NULL;
}

UT_TEST(test_85_complete_ack_image_revalidates_every_current_tuple)
{
	ClusterSemanticActivationAckTableV1 table;
	SemanticActivationAckIngressItem item;
	uint32 local_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS | PGRAC_IC_HELLO_CAP_GCS_DONE_V1;

	build_complete_current_sample_table(&table, &item);
	UT_ASSERT(semantic_activation_ack_complete_image_current(&table, UINT64_C(0x09), 0, 9, 0, 0,
															 local_capability_word));
}

UT_TEST(test_86_complete_ack_image_rejects_self_remote_and_row_drift)
{
	ClusterSemanticActivationAckTableV1 table;
	SemanticActivationAckIngressItem item;
	uint32 local_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS | PGRAC_IC_HELLO_CAP_GCS_DONE_V1;

	build_complete_current_sample_table(&table, &item);
	UT_ASSERT(!semantic_activation_ack_complete_image_current(&table, UINT64_C(0x19), 0, 9, 0, 0,
															  local_capability_word));
	UT_ASSERT(!semantic_activation_ack_complete_image_current(&table, UINT64_C(0x09), 0, 9, 1, 0,
															  local_capability_word));

	cluster_r4_activation_test_self_incarnation = 12;
	UT_ASSERT(!semantic_activation_ack_complete_image_current(&table, UINT64_C(0x09), 0, 9, 0, 0,
															  local_capability_word));
	cluster_r4_activation_test_self_incarnation = 11;
	cluster_r4_activation_test_membership_floor = 13;
	UT_ASSERT(!semantic_activation_ack_complete_image_current(&table, UINT64_C(0x09), 0, 9, 0, 0,
															  local_capability_word));
	cluster_r4_activation_test_membership_floor = 12;
	cluster_r4_activation_test_capability_generation = 8;
	UT_ASSERT(!semantic_activation_ack_complete_image_current(&table, UINT64_C(0x09), 0, 9, 0, 0,
															  local_capability_word));
	cluster_r4_activation_test_capability_generation = 7;
	table.observed[3].capability_word ^= PGRAC_IC_HELLO_CAP_GCS_DONE_V1;
	UT_ASSERT(!semantic_activation_ack_complete_image_current(&table, UINT64_C(0x09), 0, 9, 0, 0,
															  local_capability_word));
}

static SemanticActivationAckIngressItem
valid_current_sample_request_item(void)
{
	SemanticActivationAckIngressItem item;

	memset(&item, 0, sizeof(item));
	item.message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	item.message.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	item.message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	item.message.coordinator_node = 0;
	item.message.member_node = 3;
	item.message.transition_epoch = 9;
	item.message.record_generation = 10;
	item.message.round_nonce = 11;
	item.message.source_feature_bitmap = 0;
	item.message.target_feature_bitmap = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	item.message.admitted_members_lo = UINT64_C(0x09);
	item.authenticated_source_node_id = 0;
	item.local_receiver_node_id = 3;
	item.sampled_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS | PGRAC_IC_HELLO_CAP_GCS_DONE_V1;
	item.sampled_capability_generation = 7;
	return item;
}

static void
set_current_sample_request_dependencies(void)
{
	cluster_r4_activation_test_self_incarnation = 12;
	cluster_r4_activation_test_membership_node = 3;
	cluster_r4_activation_test_membership_floor = 12;
	cluster_r4_activation_test_membership_state = CLUSTER_MEMBER_MEMBER;
	cluster_r4_activation_test_membership_node2 = 0;
	cluster_r4_activation_test_membership_floor2 = 13;
	cluster_r4_activation_test_membership_state2 = CLUSTER_MEMBER_MEMBER;
	cluster_r4_activation_test_capability_generation_matches = true;
	cluster_r4_activation_test_capability_peer = 0;
	cluster_r4_activation_test_capability_required = CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	cluster_r4_activation_test_capability_expected_generation = 7;
}

UT_TEST(test_87_authorized_request_installs_self_and_duplicate_is_inert)
{
	ClusterSemanticActivationAckTableV1 table;
	ClusterSemanticActivationAckTableV1 before;
	SemanticActivationAckIngressItem item = valid_current_sample_request_item();
	SemanticActivationAckTuple expected_observed[CLUSTER_MAX_NODES] = { { 0 } };
	uint32 local_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS | PGRAC_IC_HELLO_CAP_GCS_DONE_V1;

	memset(&table, 0, sizeof(table));
	pg_atomic_init_u64(&table.publication_seq, 0);
	set_current_sample_request_dependencies();
	SemanticActivationAckTable = &table;

	UT_ASSERT_EQ(semantic_activation_ack_lmon_install_authorized_request(
					 &item, UINT64_C(0x09), 0, 9, 0, local_capability_word),
				 SEMANTIC_ACTIVATION_ACK_CONSUME_APPLIED);
	UT_ASSERT_EQ(pg_atomic_read_u64(&table.publication_seq), 2);
	UT_ASSERT_EQ(table.stage, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE);
	UT_ASSERT_EQ(table.flags, 0);
	UT_ASSERT_EQ(table.coordinator_node, 0);
	UT_ASSERT_EQ(table.reserved, 0);
	UT_ASSERT_EQ(table.round_nonce, UINT64_C(11));
	UT_ASSERT_EQ(table.expected_members_lo, UINT64_C(0x09));
	UT_ASSERT_EQ(table.expected_members_hi, 0);
	UT_ASSERT_EQ(table.observed_members_lo, UINT64_C(0x08));
	UT_ASSERT_EQ(table.observed_members_hi, 0);
	UT_ASSERT_EQ(table.transition_epoch, UINT64_C(9));
	UT_ASSERT_EQ(table.record_generation, UINT64_C(10));
	UT_ASSERT_EQ(table.source_feature_bitmap, UINT64_C(0));
	UT_ASSERT_EQ(table.target_feature_bitmap, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(table.rollback_feature_bitmap, 0);
	UT_ASSERT_EQ(table.capability_sample_digest, 0);
	UT_ASSERT_EQ(memcmp(table.expected, (SemanticActivationAckTuple[CLUSTER_MAX_NODES]){ { 0 } },
						sizeof(table.expected)),
				 0);
	expected_observed[3] = (SemanticActivationAckTuple){
		.node_id = 3,
		.boot_id = 12,
		.admitted_incarnation = 12,
		.control_connection_generation = 12,
		.capability_word = local_capability_word,
		.capability_generation = 12,
		.transition_epoch = 9,
		.record_generation = 10,
	};
	UT_ASSERT_EQ(memcmp(table.observed, expected_observed, sizeof(table.observed)), 0);

	before = table;
	UT_ASSERT_EQ(semantic_activation_ack_lmon_install_authorized_request(
					 &item, UINT64_C(0x09), 0, 9, 0, local_capability_word),
				 SEMANTIC_ACTIVATION_ACK_CONSUME_DUPLICATE);
	UT_ASSERT_EQ(memcmp(&table, &before, sizeof(table)), 0);
	SemanticActivationAckTable = NULL;
}

static void
assert_authorized_request_rejected_without_mutation(const SemanticActivationAckIngressItem *item,
													ClusterSemanticActivationAckTableV1 *table)
{
	ClusterSemanticActivationAckTableV1 before = *table;
	uint32 local_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS | PGRAC_IC_HELLO_CAP_GCS_DONE_V1;

	UT_ASSERT_EQ(semantic_activation_ack_lmon_install_authorized_request(item, UINT64_C(0x09), 0, 9,
																		 0, local_capability_word),
				 SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED);
	UT_ASSERT_EQ(memcmp(table, &before, sizeof(*table)), 0);
}

UT_TEST(test_88_authorized_request_drift_never_mutates_table)
{
	ClusterSemanticActivationAckTableV1 table;
	SemanticActivationAckIngressItem item;

	memset(&table, 0, sizeof(table));
	pg_atomic_init_u64(&table.publication_seq, 4);
	set_current_sample_request_dependencies();
	SemanticActivationAckTable = &table;

	item = valid_current_sample_request_item();
	item.authenticated_source_node_id = 1;
	assert_authorized_request_rejected_without_mutation(&item, &table);
	item = valid_current_sample_request_item();
	item.message.coordinator_node = 1;
	assert_authorized_request_rejected_without_mutation(&item, &table);
	item = valid_current_sample_request_item();
	item.local_receiver_node_id = 2;
	assert_authorized_request_rejected_without_mutation(&item, &table);
	item = valid_current_sample_request_item();
	item.message.member_node = 2;
	assert_authorized_request_rejected_without_mutation(&item, &table);
	item = valid_current_sample_request_item();
	item.message.admitted_members_lo = UINT64_C(0x19);
	assert_authorized_request_rejected_without_mutation(&item, &table);
	item = valid_current_sample_request_item();
	item.message.transition_epoch = 8;
	assert_authorized_request_rejected_without_mutation(&item, &table);
	item = valid_current_sample_request_item();
	item.sampled_capability_generation = 8;
	assert_authorized_request_rejected_without_mutation(&item, &table);
	item = valid_current_sample_request_item();
	cluster_r4_activation_test_membership_state2 = CLUSTER_MEMBER_JOINING;
	assert_authorized_request_rejected_without_mutation(&item, &table);
	cluster_r4_activation_test_membership_state2 = CLUSTER_MEMBER_MEMBER;
	item = valid_current_sample_request_item();
	cluster_r4_activation_test_membership_floor = 13;
	assert_authorized_request_rejected_without_mutation(&item, &table);
	SemanticActivationAckTable = NULL;
}

static SemanticActivationAdmissionSnapshot
valid_source_open_snapshot(void)
{
	return (SemanticActivationAdmissionSnapshot){
		.seq = 6,
		.active_bits = 0,
		.record_generation = 9,
		.formation_epoch = 9,
		.transition_closed = false,
	};
}

static SemanticActivationAckIngressItem
valid_current_durable_sample_request_item(void)
{
	SemanticActivationAckIngressItem item = valid_current_sample_request_item();

	item.message.source_feature_bitmap = 0;
	item.message.target_feature_bitmap = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	return item;
}

UT_TEST(test_89_current_sample_request_accepts_exact_durable_source_open)
{
	ClusterSemanticActivationAckTableV1 table;
	SemanticActivationAckIngressItem item = valid_current_durable_sample_request_item();
	SemanticActivationAdmissionSnapshot snapshot = valid_source_open_snapshot();
	uint32 local_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS | PGRAC_IC_HELLO_CAP_GCS_DONE_V1;

	memset(&table, 0, sizeof(table));
	pg_atomic_init_u64(&table.publication_seq, 0);
	set_current_sample_request_dependencies();
	SemanticActivationAckTable = &table;

	UT_ASSERT_EQ(semantic_activation_ack_lmon_accept_current_sample_request(
					 &item, &snapshot, UINT64_C(0x09), 0, 9, 0, local_capability_word),
				 SEMANTIC_ACTIVATION_ACK_CONSUME_APPLIED);
	UT_ASSERT_EQ(pg_atomic_read_u64(&table.publication_seq), 2);
	UT_ASSERT_EQ(table.record_generation, UINT64_C(10));
	UT_ASSERT_EQ(table.observed_members_lo, UINT64_C(0x08));
	UT_ASSERT_EQ(table.observed[3].boot_id, UINT64_C(12));
	SemanticActivationAckTable = NULL;
}

static void
assert_current_sample_request_not_authorized(const SemanticActivationAckIngressItem *item,
											 const SemanticActivationAdmissionSnapshot *snapshot,
											 ClusterSemanticActivationAckTableV1 *table)
{
	ClusterSemanticActivationAckTableV1 before = *table;
	uint32 local_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS | PGRAC_IC_HELLO_CAP_GCS_DONE_V1;

	UT_ASSERT_EQ(semantic_activation_ack_lmon_accept_current_sample_request(
					 item, snapshot, UINT64_C(0x09), 0, 9, 0, local_capability_word),
				 SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED);
	UT_ASSERT_EQ(memcmp(table, &before, sizeof(*table)), 0);
}

UT_TEST(test_90_current_sample_request_rejects_durable_stage_drift)
{
	ClusterSemanticActivationAckTableV1 table;
	SemanticActivationAckIngressItem item;
	SemanticActivationAdmissionSnapshot snapshot;

	memset(&table, 0, sizeof(table));
	pg_atomic_init_u64(&table.publication_seq, 4);
	set_current_sample_request_dependencies();
	SemanticActivationAckTable = &table;

	item = valid_current_durable_sample_request_item();
	snapshot = valid_source_open_snapshot();
	snapshot.seq = 7;
	assert_current_sample_request_not_authorized(&item, &snapshot, &table);
	snapshot = valid_source_open_snapshot();
	snapshot.transition_closed = true;
	assert_current_sample_request_not_authorized(&item, &snapshot, &table);
	snapshot = valid_source_open_snapshot();
	snapshot.formation_epoch = 8;
	assert_current_sample_request_not_authorized(&item, &snapshot, &table);
	snapshot = valid_source_open_snapshot();
	snapshot.record_generation = 8;
	assert_current_sample_request_not_authorized(&item, &snapshot, &table);
	snapshot = valid_source_open_snapshot();
	snapshot.active_bits = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	item.message.source_feature_bitmap = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	assert_current_sample_request_not_authorized(&item, &snapshot, &table);
	item = valid_current_durable_sample_request_item();
	snapshot = valid_source_open_snapshot();
	item.message.target_feature_bitmap = 0;
	assert_current_sample_request_not_authorized(&item, &snapshot, &table);
	item = valid_current_durable_sample_request_item();
	item.message.rollback_feature_bitmap = 1;
	assert_current_sample_request_not_authorized(&item, &snapshot, &table);
	SemanticActivationAckTable = NULL;
}

static SemanticActivationAckPendingSend
valid_pending_send(uint64 pending_members_lo, uint64 pending_members_hi)
{
	SemanticActivationAckIngressItem item = valid_current_durable_sample_request_item();
	SemanticActivationAckPendingSend pending;

	memset(&pending, 0, sizeof(pending));
	pending.message = item.message;
	pending.pending_members_lo = pending_members_lo;
	pending.pending_members_hi = pending_members_hi;
	return pending;
}

UT_TEST(test_91_pending_send_clears_done_and_admitted_would_block)
{
	SemanticActivationAckPendingSend pending = valid_pending_send(UINT64_C(0x0c), 0);

	UT_ASSERT_EQ(
		semantic_activation_ack_pending_send_note_result(&pending, 2, CLUSTER_IC_SEND_DONE),
		SEMANTIC_ACTIVATION_ACK_SEND_ADMITTED);
	UT_ASSERT_EQ(pending.pending_members_lo, UINT64_C(0x08));
	UT_ASSERT_EQ(pending.pending_members_hi, 0);
	UT_ASSERT(!pending.invalidated);

	UT_ASSERT_EQ(
		semantic_activation_ack_pending_send_note_result(&pending, 3, CLUSTER_IC_SEND_WOULD_BLOCK),
		SEMANTIC_ACTIVATION_ACK_SEND_ADMITTED);
	UT_ASSERT_EQ(pending.pending_members_lo, 0);
	UT_ASSERT_EQ(pending.pending_members_hi, 0);
	UT_ASSERT(!pending.invalidated);
}

UT_TEST(test_92_pending_send_retains_refusal_and_aborts_hard_error)
{
	SemanticActivationAckPendingSend pending = valid_pending_send(UINT64_C(0x04), UINT64_C(0x40));
	SemanticActivationAckPendingSend before = pending;

	UT_ASSERT_EQ(
		semantic_activation_ack_pending_send_note_result(&pending, 2, CLUSTER_IC_SEND_NOT_ADMITTED),
		SEMANTIC_ACTIVATION_ACK_SEND_RETAINED);
	UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);

	UT_ASSERT_EQ(
		semantic_activation_ack_pending_send_note_result(&pending, 70, CLUSTER_IC_SEND_HARD_ERROR),
		SEMANTIC_ACTIVATION_ACK_SEND_INVALIDATED);
	UT_ASSERT_EQ(pending.pending_members_lo, 0);
	UT_ASSERT_EQ(pending.pending_members_hi, 0);
	UT_ASSERT(pending.invalidated);

	before = pending;
	UT_ASSERT_EQ(
		semantic_activation_ack_pending_send_note_result(&pending, 2, CLUSTER_IC_SEND_DONE),
		SEMANTIC_ACTIVATION_ACK_SEND_REJECTED);
	UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
}

UT_TEST(test_93_four_node_sample_request_installs_self_and_sends_positive_ack)
{
	ClusterSemanticActivationShmem shmem;
	ClusterSemanticActivationAckTableV1 table;
	SemanticActivationAckIngressItem item = valid_current_durable_sample_request_item();
	ClusterSemanticActivationAckWireV1 expected;
	ClusterSemanticActivationAckWireV1 sent;
	uint32 local_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS | PGRAC_IC_HELLO_CAP_GCS_DONE_V1;
	int node;

	memset(&shmem, 0, sizeof(shmem));
	pg_atomic_init_u64(&shmem.admission_seq, 6);
	pg_atomic_init_u64(&shmem.active_bits, 0);
	pg_atomic_init_u64(&shmem.record_generation, 9);
	pg_atomic_init_u64(&shmem.formation_epoch, 9);
	pg_atomic_init_u32(&shmem.transition_closed, 0);
	memset(&table, 0, sizeof(table));
	pg_atomic_init_u64(&table.publication_seq, 0);
	item.message.admitted_members_lo = UINT64_C(0x0f);

	set_current_sample_request_dependencies();
	cluster_r4_activation_test_admitted_snapshot_valid = true;
	cluster_r4_activation_test_admitted_members_lo = UINT64_C(0x0f);
	cluster_r4_activation_test_admitted_members_hi = 0;
	cluster_r4_activation_test_admitted_epoch = 9;
	cluster_r4_activation_test_current_epoch = 9;
	cluster_r4_activation_test_capability_word_sample_ok = true;
	cluster_r4_activation_test_capability_word = local_capability_word;
	cluster_r4_activation_test_capability_generation = 7;
	cluster_r4_activation_test_local_capability_word = local_capability_word;
	memset(cluster_r4_activation_test_capability_sample_calls, 0,
		   sizeof(cluster_r4_activation_test_capability_sample_calls));
	memset(cluster_r4_activation_test_send_results, 0,
		   sizeof(cluster_r4_activation_test_send_results));
	memset(cluster_r4_activation_test_send_calls, 0, sizeof(cluster_r4_activation_test_send_calls));
	memset(cluster_r4_activation_test_send_payloads, 0,
		   sizeof(cluster_r4_activation_test_send_payloads));
	memset(cluster_r4_activation_test_send_payload_lengths, 0,
		   sizeof(cluster_r4_activation_test_send_payload_lengths));
	memset(cluster_r4_activation_test_send_msg_types, 0,
		   sizeof(cluster_r4_activation_test_send_msg_types));
	memset(cluster_r4_activation_test_close_calls, 0,
		   sizeof(cluster_r4_activation_test_close_calls));
	memset(&semantic_activation_ack_local_pending_send, 0,
		   sizeof(semantic_activation_ack_local_pending_send));
	cluster_node_id = 3;
	SemanticActivationShmem = &shmem;
	SemanticActivationAckTable = &table;
	semantic_activation_ack_ingress_init(&semantic_activation_ack_local_ingress);
	UT_ASSERT(semantic_activation_ack_ingress_push(&semantic_activation_ack_local_ingress, &item));

	cluster_semantic_activation_lmon_tick();

	UT_ASSERT_EQ(semantic_activation_ack_ingress_pending(&semantic_activation_ack_local_ingress),
				 0);
	UT_ASSERT_EQ(table.expected_members_lo, UINT64_C(0x0f));
	UT_ASSERT_EQ(table.observed_members_lo, UINT64_C(0x08));
	UT_ASSERT_EQ(table.observed[3].boot_id, UINT64_C(12));
	UT_ASSERT_EQ(table.observed[3].capability_word, local_capability_word);
	expected = item.message;
	expected.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK;
	expected.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK;
	expected.member_node = 3;
	expected.boot_id = 12;
	expected.admitted_incarnation = 12;
	expected.capability_word = local_capability_word;
	for (node = 0; node < 3; node++) {
		UT_ASSERT_EQ(cluster_r4_activation_test_capability_sample_calls[node], 1);
		UT_ASSERT_EQ(cluster_r4_activation_test_send_calls[node], 1);
		UT_ASSERT_EQ(cluster_r4_activation_test_send_msg_types[node],
					 PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1);
		UT_ASSERT_EQ(cluster_r4_activation_test_send_payload_lengths[node],
					 CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES);
		UT_ASSERT(cluster_semantic_activation_ack_wire_decode(
			cluster_r4_activation_test_send_payloads[node], &sent));
		UT_ASSERT_EQ(memcmp(&sent, &expected, sizeof(sent)), 0);
		UT_ASSERT_EQ(cluster_r4_activation_test_close_calls[node], 0);
	}
	UT_ASSERT_EQ(cluster_r4_activation_test_capability_sample_calls[3], 0);
	UT_ASSERT_EQ(cluster_r4_activation_test_send_calls[3], 0);
	UT_ASSERT_EQ(semantic_activation_ack_local_pending_send.pending_members_lo, 0);
	UT_ASSERT_EQ(semantic_activation_ack_local_pending_send.pending_members_hi, 0);
	UT_ASSERT(!semantic_activation_ack_local_pending_send.invalidated);
	SemanticActivationAckTable = NULL;
	SemanticActivationShmem = NULL;
}

static void
init_four_member_digest_table(ClusterSemanticActivationAckTableV1 *table, bool reverse_fill)
{
	int index;

	memset(table, 0, sizeof(*table));
	pg_atomic_init_u64(&table->publication_seq, 0);
	table->stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	table->flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
	table->coordinator_node = 0;
	table->round_nonce = UINT64_C(0x55);
	table->expected_members_lo = UINT64_C(0x0f);
	table->observed_members_lo = UINT64_C(0x0f);
	table->transition_epoch = UINT64_C(0x0102030405060708);
	table->record_generation = UINT64_C(0x1112131415161718);
	table->source_feature_bitmap = UINT64_C(0);
	table->target_feature_bitmap = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	for (index = 0; index < 4; index++) {
		SemanticActivationAckTuple tuple;
		int node = reverse_fill ? 3 - index : index;

		memset(&tuple, 0, sizeof(tuple));
		tuple.node_id = (uint32)node;
		tuple.boot_id = UINT64_C(0x100) + (uint64)node;
		tuple.admitted_incarnation = tuple.boot_id;
		tuple.control_connection_generation = UINT64_C(0x200) + (uint64)node;
		tuple.capability_word = CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS | (uint32)node;
		tuple.capability_generation = UINT64_C(0x300) + (uint64)node;
		tuple.transition_epoch = table->transition_epoch;
		tuple.record_generation = table->record_generation;
		table->expected[node] = tuple;
		table->observed[node] = tuple;
	}
}

UT_TEST(test_94_four_member_sample_digest_is_canonical)
{
	ClusterSemanticActivationAckTableV1 forward;
	ClusterSemanticActivationAckTableV1 reverse;
	uint64 forward_digest = 0;
	uint64 reverse_digest = 0;
	uint64 changed_digest = 0;

	init_four_member_digest_table(&forward, false);
	init_four_member_digest_table(&reverse, true);
	UT_ASSERT(semantic_activation_ack_sample_digest(&forward, &forward_digest));
	UT_ASSERT_EQ(forward_digest, UINT64_C(0x00db946c7c55c2f1));
	UT_ASSERT(semantic_activation_ack_sample_digest(&reverse, &reverse_digest));
	UT_ASSERT_EQ(reverse_digest, forward_digest);

	reverse.expected[2].capability_generation ^= UINT64_C(1);
	reverse.observed[2] = reverse.expected[2];
	UT_ASSERT(semantic_activation_ack_sample_digest(&reverse, &changed_digest));
	UT_ASSERT(changed_digest != forward_digest);

	forward.flags &= ~CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
	forward_digest = UINT64_MAX;
	UT_ASSERT(!semantic_activation_ack_sample_digest(&forward, &forward_digest));
	UT_ASSERT_EQ(forward_digest, UINT64_C(0));
}

UT_TEST(test_95_four_node_barrier_request_retains_expected_without_ack)
{
	ClusterSemanticActivationShmem shmem;
	ClusterSemanticActivationAckTableV1 table;
	SemanticActivationAckTuple expected[CLUSTER_MAX_NODES];
	SemanticActivationAckIngressItem item;
	uint64 digest = 0;
	uint32 local_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS | PGRAC_IC_HELLO_CAP_GCS_DONE_V1;
	int node;

	init_four_member_digest_table(&table, false);
	set_current_sample_request_dependencies();
	cluster_r4_activation_test_capability_word_sample_ok = true;
	cluster_r4_activation_test_capability_word = local_capability_word;
	cluster_r4_activation_test_capability_generation = 7;
	cluster_r4_activation_test_local_capability_word = local_capability_word;
	for (node = 0; node < 4; node++) {
		SemanticActivationAckTuple tuple;
		uint64 admitted_incarnation;

		if (node == 3) {
			UT_ASSERT(semantic_activation_ack_self_tuple(
				3, local_capability_word, table.transition_epoch, table.record_generation, &tuple));
		} else {
			admitted_incarnation = cluster_membership_get_last_admitted_incarnation(node);
			tuple = (SemanticActivationAckTuple){
				.node_id = (uint32)node,
				.boot_id = admitted_incarnation,
				.admitted_incarnation = admitted_incarnation,
				.control_connection_generation = 7,
				.capability_word = local_capability_word,
				.capability_generation = 7,
				.transition_epoch = table.transition_epoch,
				.record_generation = table.record_generation,
			};
		}
		table.expected[node] = tuple;
		table.observed[node] = tuple;
	}
	UT_ASSERT(semantic_activation_ack_sample_digest(&table, &digest));
	memcpy(expected, table.expected, sizeof(expected));
	memset(&item, 0, sizeof(item));
	item.message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	item.message.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER;
	item.message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	item.message.coordinator_node = 0;
	item.message.member_node = 3;
	item.message.transition_epoch = table.transition_epoch;
	item.message.record_generation = table.record_generation;
	item.message.round_nonce = table.round_nonce;
	item.message.source_feature_bitmap = table.source_feature_bitmap;
	item.message.target_feature_bitmap = table.target_feature_bitmap;
	item.message.rollback_feature_bitmap = table.rollback_feature_bitmap;
	item.message.admitted_members_lo = table.expected_members_lo;
	item.message.admitted_members_hi = table.expected_members_hi;
	item.message.capability_sample_digest = digest;
	item.authenticated_source_node_id = 0;
	item.local_receiver_node_id = 3;
	item.sampled_capability_word = CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	item.sampled_capability_generation = 7;

	memset(&shmem, 0, sizeof(shmem));
	pg_atomic_init_u64(&shmem.admission_seq, 6);
	pg_atomic_init_u64(&shmem.active_bits, 0);
	pg_atomic_init_u64(&shmem.record_generation, table.record_generation - 1);
	pg_atomic_init_u64(&shmem.formation_epoch, table.transition_epoch);
	pg_atomic_init_u32(&shmem.transition_closed, 0);
	cluster_r4_activation_test_admitted_snapshot_valid = true;
	cluster_r4_activation_test_admitted_members_lo = UINT64_C(0x0f);
	cluster_r4_activation_test_admitted_members_hi = 0;
	cluster_r4_activation_test_admitted_epoch = table.transition_epoch;
	cluster_r4_activation_test_current_epoch = table.transition_epoch;
	memset(cluster_r4_activation_test_send_calls, 0, sizeof(cluster_r4_activation_test_send_calls));
	memset(&semantic_activation_ack_local_pending_send, 0,
		   sizeof(semantic_activation_ack_local_pending_send));
	cluster_node_id = 3;
	SemanticActivationShmem = &shmem;
	SemanticActivationAckTable = &table;
	semantic_activation_ack_ingress_init(&semantic_activation_ack_local_ingress);
	UT_ASSERT(semantic_activation_ack_ingress_push(&semantic_activation_ack_local_ingress, &item));

	cluster_semantic_activation_lmon_tick();

	UT_ASSERT_EQ(semantic_activation_ack_ingress_pending(&semantic_activation_ack_local_ingress),
				 0);
	UT_ASSERT_EQ(table.stage, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
	UT_ASSERT_EQ(table.flags, CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID);
	UT_ASSERT_EQ(table.capability_sample_digest, digest);
	UT_ASSERT_EQ(table.observed_members_lo, UINT64_C(0));
	UT_ASSERT_EQ(table.observed_members_hi, UINT64_C(0));
	UT_ASSERT_EQ(memcmp(table.expected, expected, sizeof(expected)), 0);
	UT_ASSERT_EQ(memcmp(table.observed, (SemanticActivationAckTuple[CLUSTER_MAX_NODES]){ { 0 } },
						sizeof(table.observed)),
				 0);
	for (node = 0; node < 4; node++)
		UT_ASSERT_EQ(cluster_r4_activation_test_send_calls[node], 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&shmem.record_generation), table.record_generation - 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&shmem.transition_closed), 0);
	SemanticActivationAckTable = NULL;
	SemanticActivationShmem = NULL;
}

typedef struct StageAheadPreparedFixture {
	ClusterSemanticActivationShmem shmem;
	ClusterSemanticActivationAckTableV1 table;
	SemanticActivationAckIngressItem early;
	SemanticActivationAckIngressItem request;
	uint32 required_caps;
} StageAheadPreparedFixture;

static void
stage_ahead_prepared_fixture_init(StageAheadPreparedFixture *fixture)
{
	ClusterSemanticActivationShmem *shmem = &fixture->shmem;
	ClusterSemanticActivationAckTableV1 *table = &fixture->table;
	SemanticActivationAckIngressItem *early = &fixture->early;
	SemanticActivationAckIngressItem *request = &fixture->request;
	uint32 required_caps = CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS
						   | PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1
						   | PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1;
	int node;

	memset(fixture, 0, sizeof(*fixture));
	fixture->required_caps = required_caps;
	pg_atomic_init_u64(&shmem->admission_seq, 6);
	pg_atomic_init_u64(&shmem->active_bits, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	pg_atomic_init_u64(&shmem->record_generation, 10);
	pg_atomic_init_u64(&shmem->formation_epoch, 9);
	pg_atomic_init_u32(&shmem->transition_closed, 1);

	pg_atomic_init_u64(&table->publication_seq, 0);
	table->stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER;
	table->flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
	table->coordinator_node = 0;
	table->round_nonce = UINT64_C(0x55);
	table->expected_members_lo = UINT64_C(0x0f);
	table->observed_members_lo = UINT64_C(0x0f);
	table->transition_epoch = 9;
	table->record_generation = 10;
	table->source_feature_bitmap = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	table->target_feature_bitmap = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
								   | CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1;
	table->capability_sample_digest = UINT64_C(0x778899);
	for (node = 0; node < 4; node++) {
		SemanticActivationAckTuple tuple;

		memset(&tuple, 0, sizeof(tuple));
		tuple.node_id = (uint32)node;
		tuple.boot_id = 1;
		tuple.admitted_incarnation = 1;
		tuple.control_connection_generation = node == 2 ? 1 : 7;
		tuple.capability_word = required_caps;
		tuple.capability_generation = node == 2 ? 1 : 7;
		tuple.transition_epoch = 9;
		tuple.record_generation = 10;
		table->expected[node] = tuple;
		table->observed[node] = tuple;
	}

	early->message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK;
	early->message.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED;
	early->message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK;
	early->message.reason = CLUSTER_SEMANTIC_ACTIVATION_OK;
	early->message.coordinator_node = 0;
	early->message.member_node = 0;
	early->message.transition_epoch = 9;
	early->message.record_generation = 10;
	early->message.round_nonce = table->round_nonce;
	early->message.source_feature_bitmap = table->source_feature_bitmap;
	early->message.target_feature_bitmap = table->target_feature_bitmap;
	early->message.admitted_members_lo = table->expected_members_lo;
	early->message.capability_sample_digest = table->capability_sample_digest;
	early->message.boot_id = 1;
	early->message.admitted_incarnation = 1;
	early->message.capability_word = required_caps;
	early->authenticated_source_node_id = 0;
	early->local_receiver_node_id = 2;
	early->sampled_capability_word = required_caps;
	early->sampled_capability_generation = 7;

	*request = *early;
	request->message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	request->message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	request->message.member_node = 2;
	request->message.boot_id = 0;
	request->message.admitted_incarnation = 0;
	request->message.capability_word = 0;

	cluster_node_id = 2;
	cluster_r4_activation_test_self_incarnation = 1;
	cluster_r4_activation_test_current_epoch = 9;
	cluster_r4_activation_test_in_quorum = true;
	cluster_r4_activation_test_admitted_snapshot_valid = true;
	cluster_r4_activation_test_admitted_members_lo = UINT64_C(0x0f);
	cluster_r4_activation_test_admitted_members_hi = 0;
	cluster_r4_activation_test_admitted_epoch = 9;
	cluster_r4_activation_test_membership_node = -1;
	cluster_r4_activation_test_membership_node2 = -1;
	cluster_r4_activation_test_capability_word_sample_ok = true;
	cluster_r4_activation_test_capability_word = required_caps;
	cluster_r4_activation_test_capability_generation = 7;
	cluster_r4_activation_test_local_capability_word = required_caps;
	cluster_r4_activation_test_capability_generation_matches = true;
	cluster_r4_activation_test_capability_peer = 0;
	cluster_r4_activation_test_capability_peer_mask = UINT64_C(0x0b);
	cluster_r4_activation_test_capability_required = required_caps;
	cluster_r4_activation_test_capability_expected_generation = 7;
	memset(&semantic_activation_ack_local_pending_send, 0,
		   sizeof(semantic_activation_ack_local_pending_send));
	memset(&semantic_activation_ack_local_request_origin, 0,
		   sizeof(semantic_activation_ack_local_request_origin));
	memset(&semantic_activation_ack_local_stage_ahead, 0,
		   sizeof(semantic_activation_ack_local_stage_ahead));
	memset(&semantic_activation_ack_local_request_ahead, 0,
		   sizeof(semantic_activation_ack_local_request_ahead));
	semantic_activation_ack_ingress_init(&semantic_activation_ack_local_ingress);
	SemanticActivationShmem = shmem;
	SemanticActivationAckTable = table;
}

static void
stage_ahead_prepared_fixture_cleanup(void)
{
	SemanticActivationAckTable = NULL;
	SemanticActivationShmem = NULL;
	cluster_r4_activation_test_capability_peer_mask = 0;
	memset(&semantic_activation_ack_local_stage_ahead, 0,
		   sizeof(semantic_activation_ack_local_stage_ahead));
}

UT_TEST(test_96_stage_ahead_ack_is_applied_once_after_matching_request)
{
	StageAheadPreparedFixture fixture;

	stage_ahead_prepared_fixture_init(&fixture);

	UT_ASSERT(semantic_activation_ack_ingress_push(&semantic_activation_ack_local_ingress,
												   &fixture.early));
	UT_ASSERT(semantic_activation_ack_ingress_push(&semantic_activation_ack_local_ingress,
												   &fixture.early));
	semantic_activation_ack_lmon_drain();
	UT_ASSERT_EQ(fixture.table.stage, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
	UT_ASSERT_EQ(fixture.table.observed_members_lo, UINT64_C(0x0f));

	UT_ASSERT(semantic_activation_ack_ingress_push(&semantic_activation_ack_local_ingress,
												   &fixture.request));
	semantic_activation_ack_lmon_drain();
	UT_ASSERT_EQ(fixture.table.stage, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED);
	UT_ASSERT_EQ(fixture.table.observed_members_lo, UINT64_C(0x01));
	UT_ASSERT_EQ(memcmp(&fixture.table.observed[0], &fixture.table.expected[0],
						sizeof(fixture.table.observed[0])),
				 0);

	stage_ahead_prepared_fixture_cleanup();
}

UT_TEST(test_97_stage_ahead_identity_conflict_invalidates_round)
{
	StageAheadPreparedFixture fixture;
	SemanticActivationAckIngressItem conflict;

	stage_ahead_prepared_fixture_init(&fixture);
	conflict = fixture.early;
	conflict.message.round_nonce++;
	UT_ASSERT(semantic_activation_ack_ingress_push(&semantic_activation_ack_local_ingress,
												   &fixture.early));
	UT_ASSERT(
		semantic_activation_ack_ingress_push(&semantic_activation_ack_local_ingress, &conflict));
	semantic_activation_ack_lmon_drain();
	UT_ASSERT_EQ(fixture.table.flags, UINT32_C(0));
	UT_ASSERT_EQ(fixture.table.expected_members_lo, UINT64_C(0));

	UT_ASSERT(semantic_activation_ack_ingress_push(&semantic_activation_ack_local_ingress,
												   &fixture.request));
	semantic_activation_ack_lmon_drain();
	UT_ASSERT_EQ(fixture.table.expected_members_lo, UINT64_C(0));

	stage_ahead_prepared_fixture_cleanup();
}

UT_TEST(test_98_stage_ahead_refusal_invalidates_round)
{
	StageAheadPreparedFixture fixture;
	SemanticActivationAckIngressItem refusal;

	stage_ahead_prepared_fixture_init(&fixture);
	refusal = fixture.early;
	refusal.authenticated_source_node_id = 1;
	refusal.local_receiver_node_id = 0;
	refusal.message.member_node = 1;
	refusal.message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REFUSED;
	refusal.message.reason = CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
	refusal.message.boot_id = 0;
	refusal.message.admitted_incarnation = 0;
	refusal.message.capability_word = 0;
	UT_ASSERT(semantic_activation_ack_ingress_push(&semantic_activation_ack_local_ingress,
												   &fixture.early));
	UT_ASSERT(
		semantic_activation_ack_ingress_push(&semantic_activation_ack_local_ingress, &refusal));
	semantic_activation_ack_lmon_drain();
	UT_ASSERT_EQ(fixture.table.flags, UINT32_C(0));
	UT_ASSERT_EQ(fixture.table.expected_members_lo, UINT64_C(0));

	UT_ASSERT(semantic_activation_ack_ingress_push(&semantic_activation_ack_local_ingress,
												   &fixture.request));
	semantic_activation_ack_lmon_drain();
	UT_ASSERT_EQ(fixture.table.expected_members_lo, UINT64_C(0));

	stage_ahead_prepared_fixture_cleanup();
}

typedef struct StageAheadBarrierRequestFixture {
	ClusterSemanticActivationShmem shmem;
	ClusterSemanticActivationAckTableV1 table;
	SemanticActivationAckIngressItem request;
	SemanticActivationAckIngressItem final_sample_ack;
	uint64 digest;
	uint32 required_caps;
} StageAheadBarrierRequestFixture;

static void
stage_ahead_barrier_request_fixture_init(StageAheadBarrierRequestFixture *fixture)
{
	ClusterSemanticActivationAckTableV1 *table = &fixture->table;
	SemanticActivationAckIngressItem *request = &fixture->request;
	SemanticActivationAckIngressItem *final_ack = &fixture->final_sample_ack;
	uint32 required_caps = CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS
						   | PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1
						   | PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1;
	int node;

	memset(fixture, 0, sizeof(*fixture));
	fixture->required_caps = required_caps;
	pg_atomic_init_u64(&table->publication_seq, 0);
	table->stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	table->flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
	table->coordinator_node = 0;
	table->round_nonce = UINT64_C(0x6655);
	table->expected_members_lo = UINT64_C(0x0f);
	table->observed_members_lo = UINT64_C(0x0f);
	table->transition_epoch = UINT64_C(9);
	table->record_generation = UINT64_C(10);
	table->source_feature_bitmap = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	table->target_feature_bitmap = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
								   | CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1;
	for (node = 0; node < 4; node++) {
		SemanticActivationAckTuple tuple;

		memset(&tuple, 0, sizeof(tuple));
		tuple.node_id = (uint32)node;
		tuple.boot_id = 1;
		tuple.admitted_incarnation = 1;
		tuple.control_connection_generation = node == 2 ? 1 : 7;
		tuple.capability_word = required_caps;
		tuple.capability_generation = node == 2 ? 1 : 7;
		tuple.transition_epoch = table->transition_epoch;
		tuple.record_generation = table->record_generation;
		table->expected[node] = tuple;
		table->observed[node] = tuple;
	}
	UT_ASSERT(semantic_activation_ack_sample_digest(table, &fixture->digest));

	/* Recreate the exact receiver state from the four-node failure: the
	 * current SAMPLE round lacks only node 3 and therefore has no frozen
	 * expected image or digest yet. */
	table->flags = 0;
	table->observed_members_lo = UINT64_C(0x07);
	table->capability_sample_digest = 0;
	memset(table->expected, 0, sizeof(table->expected));
	memset(&table->observed[3], 0, sizeof(table->observed[3]));

	request->message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	request->message.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER;
	request->message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	request->message.coordinator_node = 0;
	request->message.member_node = 2;
	request->message.transition_epoch = table->transition_epoch;
	request->message.record_generation = table->record_generation;
	request->message.round_nonce = table->round_nonce;
	request->message.source_feature_bitmap = table->source_feature_bitmap;
	request->message.target_feature_bitmap = table->target_feature_bitmap;
	request->message.rollback_feature_bitmap = table->rollback_feature_bitmap;
	request->message.admitted_members_lo = table->expected_members_lo;
	request->message.admitted_members_hi = table->expected_members_hi;
	request->message.capability_sample_digest = fixture->digest;
	request->authenticated_source_node_id = 0;
	request->local_receiver_node_id = 2;
	request->sampled_capability_word = required_caps;
	request->sampled_capability_generation = 7;

	final_ack->message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK;
	final_ack->message.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	final_ack->message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK;
	final_ack->message.reason = CLUSTER_SEMANTIC_ACTIVATION_OK;
	final_ack->message.coordinator_node = 0;
	final_ack->message.member_node = 3;
	final_ack->message.transition_epoch = table->transition_epoch;
	final_ack->message.record_generation = table->record_generation;
	final_ack->message.round_nonce = table->round_nonce;
	final_ack->message.source_feature_bitmap = table->source_feature_bitmap;
	final_ack->message.target_feature_bitmap = table->target_feature_bitmap;
	final_ack->message.rollback_feature_bitmap = table->rollback_feature_bitmap;
	final_ack->message.admitted_members_lo = table->expected_members_lo;
	final_ack->message.admitted_members_hi = table->expected_members_hi;
	final_ack->message.boot_id = 1;
	final_ack->message.admitted_incarnation = 1;
	final_ack->message.capability_word = required_caps;
	final_ack->authenticated_source_node_id = 3;
	final_ack->local_receiver_node_id = 2;
	final_ack->sampled_capability_word = required_caps;
	final_ack->sampled_capability_generation = 7;

	pg_atomic_init_u64(&fixture->shmem.admission_seq, 6);
	pg_atomic_init_u64(&fixture->shmem.active_bits, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	pg_atomic_init_u64(&fixture->shmem.record_generation, 9);
	pg_atomic_init_u64(&fixture->shmem.formation_epoch, table->transition_epoch);
	pg_atomic_init_u32(&fixture->shmem.transition_closed, 0);

	cluster_node_id = 2;
	cluster_r4_activation_test_self_incarnation = 1;
	cluster_r4_activation_test_current_epoch = table->transition_epoch;
	cluster_r4_activation_test_in_quorum = true;
	cluster_r4_activation_test_admitted_snapshot_valid = true;
	cluster_r4_activation_test_admitted_members_lo = UINT64_C(0x0f);
	cluster_r4_activation_test_admitted_members_hi = 0;
	cluster_r4_activation_test_admitted_epoch = table->transition_epoch;
	cluster_r4_activation_test_membership_node = -1;
	cluster_r4_activation_test_membership_node2 = -1;
	cluster_r4_activation_test_capability_generation_matches = true;
	cluster_r4_activation_test_capability_peer = -1;
	cluster_r4_activation_test_capability_peer_mask = UINT64_C(0x0b);
	cluster_r4_activation_test_capability_required = required_caps;
	cluster_r4_activation_test_capability_expected_generation = 7;
	cluster_r4_activation_test_capability_word_sample_ok = true;
	cluster_r4_activation_test_capability_word = required_caps;
	cluster_r4_activation_test_capability_generation = 7;
	cluster_r4_activation_test_local_capability_word = required_caps;
	memset(cluster_r4_activation_test_send_calls, 0, sizeof(cluster_r4_activation_test_send_calls));
	memset(&semantic_activation_ack_local_pending_send, 0,
		   sizeof(semantic_activation_ack_local_pending_send));
	memset(&semantic_activation_ack_local_request_origin, 0,
		   sizeof(semantic_activation_ack_local_request_origin));
	memset(&semantic_activation_ack_local_stage_ahead, 0,
		   sizeof(semantic_activation_ack_local_stage_ahead));
	memset(&semantic_activation_ack_local_request_ahead, 0,
		   sizeof(semantic_activation_ack_local_request_ahead));
	semantic_activation_ack_ingress_init(&semantic_activation_ack_local_ingress);
	SemanticActivationShmem = &fixture->shmem;
	SemanticActivationAckTable = table;
}

static void
stage_ahead_barrier_request_fixture_cleanup(void)
{
	SemanticActivationAckTable = NULL;
	SemanticActivationShmem = NULL;
	cluster_r4_activation_test_capability_peer_mask = 0;
	cluster_r4_activation_test_capability_generation = 7;
	cluster_r4_activation_test_capability_expected_generation = 7;
	memset(&semantic_activation_ack_local_request_ahead, 0,
		   sizeof(semantic_activation_ack_local_request_ahead));
}

UT_TEST(test_99_stage_ahead_barrier_request_waits_without_mutation_then_applies)
{
	StageAheadBarrierRequestFixture fixture;
	ClusterSemanticActivationAckTableV1 before;
	int node;

	stage_ahead_barrier_request_fixture_init(&fixture);
	before = fixture.table;
	UT_ASSERT(semantic_activation_ack_ingress_push(&semantic_activation_ack_local_ingress,
												   &fixture.request));
	semantic_activation_ack_lmon_drain();
	UT_ASSERT_EQ(memcmp(&fixture.table, &before, sizeof(before)), 0);
	for (node = 0; node < 4; node++)
		UT_ASSERT_EQ(cluster_r4_activation_test_send_calls[node], 0);

	UT_ASSERT(semantic_activation_ack_ingress_push(&semantic_activation_ack_local_ingress,
												   &fixture.final_sample_ack));
	semantic_activation_ack_lmon_drain();
	UT_ASSERT_EQ(fixture.table.stage, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
	UT_ASSERT_EQ(fixture.table.flags, CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID);
	UT_ASSERT_EQ(fixture.table.capability_sample_digest, fixture.digest);
	UT_ASSERT_EQ(fixture.table.observed_members_lo, UINT64_C(0));
	UT_ASSERT_EQ(fixture.table.observed_members_hi, UINT64_C(0));
	for (node = 0; node < 4; node++)
		UT_ASSERT_EQ(cluster_r4_activation_test_send_calls[node], 0);

	stage_ahead_barrier_request_fixture_cleanup();
}

UT_TEST(test_100_stage_ahead_barrier_request_duplicate_is_consumed_once)
{
	StageAheadBarrierRequestFixture fixture;

	stage_ahead_barrier_request_fixture_init(&fixture);
	UT_ASSERT(semantic_activation_ack_ingress_push(&semantic_activation_ack_local_ingress,
												   &fixture.request));
	UT_ASSERT(semantic_activation_ack_ingress_push(&semantic_activation_ack_local_ingress,
												   &fixture.request));
	UT_ASSERT(semantic_activation_ack_ingress_push(&semantic_activation_ack_local_ingress,
												   &fixture.final_sample_ack));
	semantic_activation_ack_lmon_drain();
	UT_ASSERT_EQ(fixture.table.stage, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
	UT_ASSERT_EQ(fixture.table.flags, CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID);
	UT_ASSERT_EQ(fixture.table.capability_sample_digest, fixture.digest);
	UT_ASSERT_EQ(fixture.table.observed_members_lo, UINT64_C(0));

	stage_ahead_barrier_request_fixture_cleanup();
}

UT_TEST(test_101_stage_ahead_barrier_request_connection_conflict_fails_closed)
{
	StageAheadBarrierRequestFixture fixture;
	SemanticActivationAckIngressItem conflict;

	stage_ahead_barrier_request_fixture_init(&fixture);
	UT_ASSERT(semantic_activation_ack_ingress_push(&semantic_activation_ack_local_ingress,
												   &fixture.request));
	semantic_activation_ack_lmon_drain();

	conflict = fixture.request;
	conflict.sampled_capability_generation = 8;
	cluster_r4_activation_test_capability_generation = 8;
	cluster_r4_activation_test_capability_expected_generation = 8;
	UT_ASSERT(
		semantic_activation_ack_ingress_push(&semantic_activation_ack_local_ingress, &conflict));
	semantic_activation_ack_lmon_drain();
	UT_ASSERT_EQ(fixture.table.flags, UINT32_C(0));
	UT_ASSERT_EQ(fixture.table.expected_members_lo, UINT64_C(0));
	UT_ASSERT_EQ(fixture.table.observed_members_lo, UINT64_C(0));

	stage_ahead_barrier_request_fixture_cleanup();
}

int
main(void)
{
	UT_PLAN(101);
	UT_RUN(test_01_record_constants);
	UT_RUN(test_02_phase_numeric_values);
	UT_RUN(test_03_encode_rejects_null_record);
	UT_RUN(test_04_encode_rejects_null_bytes);
	UT_RUN(test_05_decode_rejects_null_bytes);
	UT_RUN(test_06_decode_rejects_null_record);
	UT_RUN(test_07_roundtrip_prepare);
	UT_RUN(test_08_roundtrip_commit);
	UT_RUN(test_09_roundtrip_open);
	UT_RUN(test_10_roundtrip_rollback_complete);
	UT_RUN(test_11_generation_one_is_valid);
	UT_RUN(test_12_generation_uint64_max_is_preserved);
	UT_RUN(test_13_epoch_zero_is_valid);
	UT_RUN(test_14_all_member_bits_are_preserved);
	UT_RUN(test_15_rollback_bitmap_is_preserved_only_for_terminal_phase);
	UT_RUN(test_16_magic_is_little_endian_at_zero);
	UT_RUN(test_17_version_is_little_endian_at_four);
	UT_RUN(test_18_header_len_is_little_endian_at_six);
	UT_RUN(test_19_generation_is_at_eight);
	UT_RUN(test_20_phase_is_at_sixteen);
	UT_RUN(test_21_phase_reserved_bytes_are_zero);
	UT_RUN(test_22_source_bitmap_is_at_twenty_four);
	UT_RUN(test_23_target_bitmap_is_at_thirty_two);
	UT_RUN(test_24_epoch_is_at_forty);
	UT_RUN(test_25_coordinator_node_is_at_forty_eight);
	UT_RUN(test_26_coordinator_reserved_word_is_zero);
	UT_RUN(test_27_coordinator_incarnation_is_at_fifty_six);
	UT_RUN(test_28_member_words_are_at_sixty_four_and_seventy_two);
	UT_RUN(test_29_digest_and_rollback_are_at_eighty_and_eighty_eight);
	UT_RUN(test_30_crc_covers_exactly_bytes_zero_through_ninety_five);
	UT_RUN(test_31_bytes_one_hundred_through_end_are_zero);
	UT_RUN(test_32_encode_rejects_generation_zero);
	UT_RUN(test_33_encode_rejects_phase_none);
	UT_RUN(test_34_encode_rejects_rollback_bitmap_before_terminal_phase);
	UT_RUN(test_35_decode_rejects_magic_mutation);
	UT_RUN(test_36_decode_rejects_version_mutation);
	UT_RUN(test_37_decode_rejects_header_len_mutation);
	UT_RUN(test_38_decode_rejects_generation_zero);
	UT_RUN(test_39_decode_rejects_unknown_phase);
	UT_RUN(test_40_decode_rejects_phase_reserved_mutation);
	UT_RUN(test_41_decode_rejects_word_reserved_mutation);
	UT_RUN(test_42_decode_rejects_crc_and_tail_mutations);
	UT_RUN(test_43_two_of_three_identical_records_select);
	UT_RUN(test_44_three_of_five_identical_records_select);
	UT_RUN(test_45_minority_higher_generation_does_not_win);
	UT_RUN(test_46_readable_all_zero_majority_is_implicit_open_zero);
	UT_RUN(test_47_less_than_readable_majority_holds);
	UT_RUN(test_48_equal_generation_conflict_without_majority_is_not_legacy);
	UT_RUN(test_49_record_cas_mailbox_exact_sequence_lifecycle);
	UT_RUN(test_50_pgrd_uses_distinct_kind_in_existing_512_byte_mailbox);
	UT_RUN(test_51_ack_sample_request_has_exact_wire_bytes);
	UT_RUN(test_52_ack_wire_decodes_hand_written_little_endian_bytes);
	UT_RUN(test_53_ack_wire_rejects_nonzero_reserved_without_output_mutation);
	UT_RUN(test_54_ack_wire_rejects_wrong_magic_and_version);
	UT_RUN(test_55_ack_wire_rejects_unknown_kind_stage_and_result);
	UT_RUN(test_56_ack_wire_rejects_invalid_request_shape);
	UT_RUN(test_57_ack_wire_rejects_invalid_positive_ack_shape);
	UT_RUN(test_58_ack_wire_rejects_invalid_refused_ack_shape);
	UT_RUN(test_59_ack_wire_rejects_out_of_range_or_unadmitted_nodes);
	UT_RUN(test_60_ack_wire_applies_digest_rule_to_ack_round_identity);
	UT_RUN(test_61_ack_wire_encoder_rejects_invalid_host_shape);
	UT_RUN(test_62_ack_tuple_and_table_have_exact_frozen_layout);
	UT_RUN(test_63_ack_tuple_encoder_is_exact_and_clears_reserved_gaps);
	UT_RUN(test_64_shmem_size_includes_exact_ack_table);
	UT_RUN(test_65_ack_table_snapshot_accepts_even_and_rejects_odd);
	UT_RUN(test_66_ack_table_publish_advances_even_and_refuses_bad_sequence);
	UT_RUN(test_67_ack_ingress_handoff_has_exact_frozen_layout);
	UT_RUN(test_68_ack_ingress_handoff_is_bounded_and_fail_closed);
	UT_RUN(test_69_ack_ingress_stamps_coherent_remote_sample);
	UT_RUN(test_70_ack_ingress_rejects_role_epoch_and_sample_drift);
	UT_RUN(test_71_ack_ingress_accepts_request_and_refusal_roles);
	UT_RUN(test_72_ack_handler_only_enqueues_and_counts_typed_drops);
	UT_RUN(test_73_lmon_remote_ack_builds_exact_current_tuple);
	UT_RUN(test_74_lmon_remote_ack_revalidates_every_authority_input);
	UT_RUN(test_75_lmon_self_ack_builds_exact_local_tuple);
	UT_RUN(test_76_lmon_self_ack_is_fail_closed_on_local_drift);
	UT_RUN(test_77_lmon_current_ack_authority_uses_exact_member_snapshot);
	UT_RUN(test_78_lmon_current_ack_authority_fails_closed_on_drift);
	UT_RUN(test_79_lmon_sample_ack_promotes_complete_observed_image);
	UT_RUN(test_80_lmon_ack_stale_drop_and_current_drift_invalidation);
	UT_RUN(test_81_lmon_ack_duplicate_is_idempotent_but_conflict_invalidates);
	UT_RUN(test_82_semantic_lmon_tick_drains_ack_ingress_before_other_work);
	UT_RUN(test_83_semantic_lmon_tick_invalidates_active_ack_on_authority_loss);
	UT_RUN(test_84_semantic_lmon_idle_ack_path_takes_no_authority_snapshot);
	UT_RUN(test_85_complete_ack_image_revalidates_every_current_tuple);
	UT_RUN(test_86_complete_ack_image_rejects_self_remote_and_row_drift);
	UT_RUN(test_87_authorized_request_installs_self_and_duplicate_is_inert);
	UT_RUN(test_88_authorized_request_drift_never_mutates_table);
	UT_RUN(test_89_current_sample_request_accepts_exact_durable_source_open);
	UT_RUN(test_90_current_sample_request_rejects_durable_stage_drift);
	UT_RUN(test_91_pending_send_clears_done_and_admitted_would_block);
	UT_RUN(test_92_pending_send_retains_refusal_and_aborts_hard_error);
	UT_RUN(test_93_four_node_sample_request_installs_self_and_sends_positive_ack);
	UT_RUN(test_94_four_member_sample_digest_is_canonical);
	UT_RUN(test_95_four_node_barrier_request_retains_expected_without_ack);
	UT_RUN(test_96_stage_ahead_ack_is_applied_once_after_matching_request);
	UT_RUN(test_97_stage_ahead_identity_conflict_invalidates_round);
	UT_RUN(test_98_stage_ahead_refusal_invalidates_round);
	UT_RUN(test_99_stage_ahead_barrier_request_waits_without_mutation_then_applies);
	UT_RUN(test_100_stage_ahead_barrier_request_duplicate_is_consumed_once);
	UT_RUN(test_101_stage_ahead_barrier_request_connection_conflict_fails_closed);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
