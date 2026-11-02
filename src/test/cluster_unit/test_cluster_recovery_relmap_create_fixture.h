/*-------------------------------------------------------------------------
 * test_cluster_recovery_relmap_create_fixture.h
 *    Original RMC1 bytes for native observations and ROOT candidates.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_recovery_relmap_create_fixture.h
 * NOTES
 *    Spec: 2026-11-01-pre2-e1-format-closure-v1.2-path-c.md
 *-------------------------------------------------------------------------
 */
#ifndef TEST_RECOVERY_RELMAP_CREATE_FIXTURE_H
#define TEST_RECOVERY_RELMAP_CREATE_FIXTURE_H
#include "common/cluster_relmap_create.h"

static void
test_relmap_creation_bytes(char *bytes, const ClusterWalSourceRef *origin, int count)
{
	ClusterRecoveryFileIdentityV2 id = {0};
	RelMapFile map = {0};
	uint8 batch[16] = {7};
	id.kind = CLUSTER_RECOVERY_FILE_RELMAP_V2;
	id.format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
	id.format_version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	id.system_identifier = origin->claim.identity.system_identifier;
	id.database_incarnation = origin->claim.database_incarnation;
	memcpy(id.storage_uuid, origin->claim.identity.storage_uuid, 16);
	id.redo_branch_uuid[0] = 9;
	id.redo_branch_generation = 23;
	id.redo_timeline = origin->timeline;
	id.database_oid = 5;
	id.tablespace_oid = DEFAULTTABLESPACE_OID;
	id.origin = origin->claim.identity.origin_node_id;
	id.creation_generation = 101;
	id.registration_generation = 211;
	map.magic = RELMAPPER_FILEMAGIC;
	map.num_mappings = count;
	for (int i = 0; i < count; ++i)
		map.mappings[i] = (RelMapping){1247 + i, 16385 + i};
	INIT_CRC32C(map.crc);
	COMP_CRC32C(map.crc, &map, offsetof(RelMapFile, crc));
	FIN_CRC32C(map.crc);
	UT_ASSERT(cluster_relmap_create_encode_v1(&id, batch, &map, bytes,
		CLUSTER_RELMAP_CREATE_WAL_SIZE_V1));
}
#endif
