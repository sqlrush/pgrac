/*-------------------------------------------------------------------------
 * test_cluster_relmap_cold_create.c
 *    Canonical creation input through the real cold decoder and registry.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_cold_create.c
 * NOTES
 *    Spec: 2026-11-01-pre2-e1-format-closure-v1.2-path-c.md
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1
#include "postgres.h"
#include "cluster/cluster_cold_relmap.h"
#include "utils/relmapper.h"

int cold_effects_fixture_main(void);
#define main cold_effects_fixture_main
#include "test_cluster_cold_recovery_effects.c"
#undef main
#include "common/cluster_relmap_create.h"

static ClusterRelmapCreateWalV1 creation;

static void
creation_record(EffectRecord *r)
{
	RelMapFile *map;
	ClusterRecoveryFileIdentityV2 *id;

	record_init(r, RM_RELMAP_ID, XLOG_RELMAP_CREATE_V1);
	memset(&creation, 0, sizeof(creation));
	id = &creation.identity;
	id->kind = CLUSTER_RECOVERY_FILE_RELMAP_V2;
	id->format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
	id->format_version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	id->system_identifier = 99;
	id->database_incarnation = 77;
	memcpy(id->storage_uuid, uuid, 16);
	id->redo_branch_uuid[0] = 9;
	id->redo_branch_generation = 4;
	id->redo_timeline = 3;
	id->database_oid = 2000;
	id->tablespace_oid = DEFAULTTABLESPACE_OID;
	/* A target file's origin is not the WAL writer's thread. */
	id->origin = 2;
	id->creation_generation = 31;
	id->registration_generation = 41;
	creation.batch_uuid[0] = 7;
	creation.batch_uuid[15] = 19;
	map = &creation.committed;
	map->magic = RELMAPPER_FILEMAGIC;
	map->num_mappings = 2;
	map->mappings[0] = (RelMapping){1259, 9001};
	map->mappings[1] = (RelMapping){1249, 9002};
	map->mappings[MAX_MAPPINGS-1] = (RelMapping){999, 777};
	INIT_CRC32C(map->crc);
	COMP_CRC32C(map->crc, map, offsetof(RelMapFile, crc));
	FIN_CRC32C(map->crc);
	UT_ASSERT(cluster_relmap_create_encode_v1(id, creation.batch_uuid, map,
		r->data.bytes, CLUSTER_RELMAP_CREATE_WAL_SIZE_V1));
	r->decoded.main_data_len = CLUSTER_RELMAP_CREATE_WAL_SIZE_V1;
	r->reader.EndRecPtr = 0x2000;
}

UT_TEST(canonical_creation_keeps_complete_by_value_carrier)
{
	EffectRecord r;
	ClusterColdDecodedV1 out = {0};
	creation_record(&r);
	/* Exercise byte alignment, not a native-struct cast. */
	memmove(r.data.bytes+1, r.data.bytes, CLUSTER_RELMAP_CREATE_WAL_SIZE_V1);
	r.decoded.main_data = r.data.bytes+1;
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.route_owner, RF_ROUTE_OWNER_SIDE_TYPED);
	UT_ASSERT(out.record.relmap_create == &out.relmap_create);
	UT_ASSERT_NULL(out.record.relmap_image);
	UT_ASSERT_EQ(out.record.relmap_dbid, 2000);
	UT_ASSERT_EQ(out.record.relmap_tsid, DEFAULTTABLESPACE_OID);
	UT_ASSERT_EQ(memcmp(&out.relmap_create.identity, &creation.identity, sizeof(creation.identity)), 0);
	UT_ASSERT_EQ(memcmp(out.relmap_create.batch_uuid, creation.batch_uuid, 16), 0);
	UT_ASSERT_EQ(memcmp(&out.relmap_create.committed, &creation.committed, sizeof(RelMapFile)), 0);
	memset(r.data.bytes, 0xaa, sizeof(r.data.bytes));
	UT_ASSERT_EQ(out.relmap_create.committed.mappings[MAX_MAPPINGS-1].mapfilenumber, 777);
	cluster_cold_decoded_release_v1(&out);
}

UT_TEST(creation_has_no_native_noop_page_or_pending_execution_route)
{
	for (unsigned foreign = 0; foreign < 2; ++foreign) {
		EffectRecord r;
		ClusterColdDecodedV1 out = {0};
		ClusterColdStepV1 step = {0};
		creation_record(&r);
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, foreign, &out), CLUSTER_COLD_OK);
		UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_STRUCTURAL
			| (foreign ? CLUSTER_COLD_RECORD_SIDE_UNOWNED : 0));
		UT_ASSERT_EQ(out.effects.required, CLUSTER_COLD_EFFECT_RECORD);
		UT_ASSERT_EQ(out.effects.refused, CLUSTER_COLD_EFFECT_RECORD);
		UT_ASSERT_EQ(out.effects.native_apply | out.effects.page_apply | out.effects.space_apply
			| out.effects.no_effect | out.effects.participant_apply, 0);
		UT_ASSERT(!out.xid_input.complete);
		UT_ASSERT(!cluster_cold_replay_effects_v1(&out, NULL, !foreign));
		step.step_kind = CLUSTER_COLD_STEP_RELMAP;
		step.read_rec_ptr = out.record.read_rec_ptr;
		step.end_rec_ptr = out.record.end_rec_ptr;
		step.record_crc = out.record.record_crc;
		step.info = out.record.info;
		step.rmid = out.record.rmid;
		UT_ASSERT(!cluster_cold_replay_effects_v1(&out, &step, !foreign));
		cluster_cold_decoded_release_v1(&out);
	}
}

UT_TEST(original_plan_refuses_undeclared_creation_after_native_redo)
{
	EffectRecord r;
	ClusterColdDecodedV1 out = {0};
	ClusterColdPlanV1 *plan = NULL;
	ClusterColdParticipantV1 cut = {0};
	creation_record(&r);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, true, &out), CLUSTER_COLD_OK);
	cut.thread_id = 1;
	cut.timeline = 1;
	cut.owner_incarnation = 19;
	cut.physical_lower = cut.native_redo = r.reader.ReadRecPtr;
	cut.tail_end = r.reader.EndRecPtr;
	UT_ASSERT_EQ(cluster_cold_plan_create_v1(&cut, 1, 1024*1024, &plan), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_feed_v1(plan, 0, &out.record), CLUSTER_COLD_STRUCTURAL_UNSUPPORTED);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 0);
	cluster_cold_plan_destroy_v1(&plan);
	cluster_cold_decoded_release_v1(&out);
}

UT_TEST(creation_rejects_every_damaged_byte_and_nonexact_extent)
{
	EffectRecord r;
	ClusterColdDecodedV1 out = {0};
	creation_record(&r);
	for (Size i = 0; i < CLUSTER_RELMAP_CREATE_WAL_SIZE_V1; ++i) {
		r.data.bytes[i] ^= 1;
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out), CLUSTER_COLD_COMPONENT_INVALID);
		UT_ASSERT_NULL(out.record.relmap_create);
		UT_ASSERT_EQ(out.effects.participant_apply | out.effects.native_apply, 0);
		r.data.bytes[i] ^= 1;
	}
	for (uint32 len = 0; len <= CLUSTER_RELMAP_CREATE_WAL_SIZE_V1+1; ++len) {
		if (len == CLUSTER_RELMAP_CREATE_WAL_SIZE_V1) continue;
		r.decoded.main_data_len = len;
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out), CLUSTER_COLD_COMPONENT_INVALID);
		UT_ASSERT_NULL(out.record.relmap_create);
	}
	cluster_cold_decoded_release_v1(&out);
}

UT_TEST(creation_rejects_wrong_source_and_incomplete_envelopes)
{
	for (unsigned fault = 0; fault < 11; ++fault) {
		EffectRecord r;
		ClusterColdDecodedV1 out = {0};
		uint8 wrong_uuid[16] = {2};
		DecodedXLogRecord *with_block = NULL;
		creation_record(&r);
		if (fault == 2) r.decoded.main_data = NULL;
		if (fault == 3) r.decoded.has_page_version_edge = true;
		if (fault == 4) r.decoded.record_origin = 1;
		if (fault == 5) r.decoded.header.xl_info |= XLR_INFO_MASK;
		if (fault == 6) r.reader.ReadRecPtr = InvalidXLogRecPtr;
		if (fault == 7) r.reader.EndRecPtr = r.reader.ReadRecPtr;
		if (fault == 8) r.reader.EndRecPtr = r.reader.ReadRecPtr-1;
		if (fault == 9) r.decoded.toplevel_xid = 101;
		if (fault == 10) {
			with_block = calloc(1, sizeof(*with_block) + sizeof(DecodedBkpBlock));
			UT_ASSERT_NOT_NULL(with_block);
			if (with_block == NULL) return;
			*with_block = r.decoded;
			with_block->max_block_id = 0;
			with_block->blocks[0].in_use = true;
			r.reader.record = with_block;
		}
		UT_ASSERT(cluster_cold_recovery_decode_v1(&r.reader, fault == 0 ? 100 : 99,
			fault == 1 ? wrong_uuid : uuid, true, false, &out) != CLUSTER_COLD_OK);
		UT_ASSERT_NULL(out.record.relmap_create);
		UT_ASSERT_EQ(out.effects.native_apply | out.effects.no_effect | out.effects.participant_apply, 0);
		cluster_cold_decoded_release_v1(&out);
		free(with_block);
	}
}

UT_TEST(reusing_decode_cannot_reuse_a_prior_creation_carrier)
{
	EffectRecord r;
	ClusterColdDecodedV1 out = {0};
	creation_record(&r);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out), CLUSTER_COLD_OK);
	UT_ASSERT_NOT_NULL(out.record.relmap_create);
	r.data.bytes[0] ^= 1;
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out), CLUSTER_COLD_COMPONENT_INVALID);
	UT_ASSERT_NULL(out.record.relmap_create);
	creation_record(&r);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out), CLUSTER_COLD_OK);
	r.decoded.header.xl_rmid = RM_RELMAP_ID;
	r.decoded.header.xl_info = XLOG_RELMAP_UPDATE;
	r.decoded.main_data_len = MinSizeOfRelmapUpdate + sizeof(RelMapFile);
	{
		xl_relmap_update update = {InvalidOid, GLOBALTABLESPACE_OID, sizeof(RelMapFile)};
		memcpy(r.data.bytes, &update, MinSizeOfRelmapUpdate);
		memcpy(r.data.bytes + MinSizeOfRelmapUpdate, &creation.committed, sizeof(RelMapFile));
	}
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r.reader, 99, uuid, true, false, &out), CLUSTER_COLD_OK);
	UT_ASSERT_NULL(out.record.relmap_create);
	UT_ASSERT_NOT_NULL(out.record.relmap_image);
	UT_ASSERT_EQ(out.effects.participant_apply, CLUSTER_COLD_EFFECT_RECORD);
	cluster_cold_decoded_release_v1(&out);
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(canonical_creation_keeps_complete_by_value_carrier);
	UT_RUN(creation_has_no_native_noop_page_or_pending_execution_route);
	UT_RUN(original_plan_refuses_undeclared_creation_after_native_redo);
	UT_RUN(creation_rejects_every_damaged_byte_and_nonexact_extent);
	UT_RUN(creation_rejects_wrong_source_and_incomplete_envelopes);
	UT_RUN(reusing_decode_cannot_reuse_a_prior_creation_carrier);
	UT_DONE();
	return ut_failed_count != 0;
}
