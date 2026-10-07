/*-------------------------------------------------------------------------
 *
 * cluster_relmap_init.c
 *    Validate an original initdb map and construct a committed authority.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/common/cluster_relmap_init.c
 *
 * NOTES
 *    Pure frontend/backend codec. The original creator owns all persistence.
 *-------------------------------------------------------------------------
 */
#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "catalog/pg_attribute_d.h"
#include "catalog/pg_auth_members_d.h"
#include "catalog/pg_authid_d.h"
#include "catalog/pg_class_d.h"
#include "catalog/pg_database_d.h"
#include "catalog/pg_db_role_setting_d.h"
#include "catalog/pg_parameter_acl_d.h"
#include "catalog/pg_proc_d.h"
#include "catalog/pg_replication_origin_d.h"
#include "catalog/pg_shdepend_d.h"
#include "catalog/pg_shdescription_d.h"
#include "catalog/pg_shseclabel_d.h"
#include "catalog/pg_subscription_d.h"
#include "catalog/pg_tablespace_d.h"
#include "catalog/pg_type_d.h"
#include "cluster/cluster_relmap_authority.h"
#include "common/relmap.h"

/*
 * Fresh bootstrap mapped catalogs, including their indexes and TOAST files.
 * Shared entries are the fixed set recognized by IsSharedRelation(). Local
 * entries belong to the four bootstrap catalogs. This is an initdb contract,
 * not a general validator for subsequent mapped-catalog rewrites. Real initdb
 * inputs are tested against this inventory to detect catalog changes.
 */
static const Oid shared_mapped_catalogs[] = { AuthIdRelationId,
											  AuthMemRelationId,
											  DatabaseRelationId,
											  DbRoleSettingRelationId,
											  ParameterAclRelationId,
											  ReplicationOriginRelationId,
											  SharedDependRelationId,
											  SharedDescriptionRelationId,
											  SharedSecLabelRelationId,
											  SubscriptionRelationId,
											  TableSpaceRelationId,
											  AuthIdOidIndexId,
											  AuthIdRolnameIndexId,
											  AuthMemMemRoleIndexId,
											  AuthMemRoleMemIndexId,
											  AuthMemOidIndexId,
											  AuthMemGrantorIndexId,
											  DatabaseNameIndexId,
											  DatabaseOidIndexId,
											  DbRoleSettingDatidRolidIndexId,
											  ParameterAclOidIndexId,
											  ParameterAclParnameIndexId,
											  ReplicationOriginIdentIndex,
											  ReplicationOriginNameIndex,
											  SharedDependDependerIndexId,
											  SharedDependReferenceIndexId,
											  SharedDescriptionObjIndexId,
											  SharedSecLabelObjectIndexId,
											  SubscriptionNameIndexId,
											  SubscriptionObjectIndexId,
											  TablespaceNameIndexId,
											  TablespaceOidIndexId,
											  PgAuthidToastTable,
											  PgAuthidToastIndex,
											  PgDatabaseToastTable,
											  PgDatabaseToastIndex,
											  PgDbRoleSettingToastTable,
											  PgDbRoleSettingToastIndex,
											  PgParameterAclToastTable,
											  PgParameterAclToastIndex,
											  PgReplicationOriginToastTable,
											  PgReplicationOriginToastIndex,
											  PgShdescriptionToastTable,
											  PgShdescriptionToastIndex,
											  PgShseclabelToastTable,
											  PgShseclabelToastIndex,
											  PgSubscriptionToastTable,
											  PgSubscriptionToastIndex,
											  PgTablespaceToastTable,
											  PgTablespaceToastIndex };
static const Oid local_mapped_catalogs[]
	= { RelationRelationId, AttributeRelationId, ProcedureRelationId, TypeRelationId,
		/* DECLARE_TOAST in pg_proc.h and pg_type.h has no symbolic OID names. */
		2836, 2837, 4171, 4172, ProcedureOidIndexId, ProcedureNameArgsNspIndexId, TypeOidIndexId,
		TypeNameNspIndexId, AttributeRelidNameIndexId, AttributeRelidNumIndexId, ClassOidIndexId,
		ClassNameNspIndexId, ClassTblspcRelfilenodeIndexId };

static bool
contains_oid(const Oid *set, size_t count, Oid oid)
{
	for (size_t i = 0; i < count; ++i)
		if (set[i] == oid)
			return true;
	return false;
}

static ClusterRelmapInitResult
validate_native_map(const RelMapFile *map, bool shared)
{
	pg_crc32c crc;
	size_t expected_count
		= shared ? lengthof(shared_mapped_catalogs) : lengthof(local_mapped_catalogs);

	if (map->magic != RELMAPPER_FILEMAGIC)
		return CLUSTER_RELMAP_INIT_INVALID_MAGIC;
	if (map->num_mappings <= 0 || map->num_mappings > MAX_MAPPINGS)
		return CLUSTER_RELMAP_INIT_INVALID_COUNT;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, map, offsetof(RelMapFile, crc));
	FIN_CRC32C(crc);
	if (!EQ_CRC32C(crc, map->crc))
		return CLUSTER_RELMAP_INIT_INVALID_CRC;

	/* Duplicates must be rejected even if one also changes a file number. */
	for (int i = 0; i < map->num_mappings; ++i) {
		const RelMapping *entry = &map->mappings[i];
		if (entry->mapoid == InvalidOid || entry->mapfilenumber == InvalidRelFileNumber)
			return CLUSTER_RELMAP_INIT_INVALID_ENTRY;
		for (int j = 0; j < i; ++j)
			if (entry->mapoid == map->mappings[j].mapoid
				|| entry->mapfilenumber == map->mappings[j].mapfilenumber)
				return CLUSTER_RELMAP_INIT_DUPLICATE;
	}
	for (int i = 0; i < map->num_mappings; ++i) {
		const RelMapping *entry = &map->mappings[i];
		bool is_shared
			= contains_oid(shared_mapped_catalogs, lengthof(shared_mapped_catalogs), entry->mapoid);
		bool is_local
			= contains_oid(local_mapped_catalogs, lengthof(local_mapped_catalogs), entry->mapoid);

		if (!is_shared && !is_local)
			return CLUSTER_RELMAP_INIT_INVALID_ENTRY;
		if (is_shared != shared)
			return CLUSTER_RELMAP_INIT_NAMESPACE_MISMATCH;
		if (entry->mapfilenumber != entry->mapoid)
			return CLUSTER_RELMAP_INIT_INVALID_ENTRY;
	}
	/* Unique members with the complete cardinality prove the entire set. */
	if ((size_t)map->num_mappings != expected_count)
		return CLUSTER_RELMAP_INIT_INVALID_COUNT;
	return CLUSTER_RELMAP_INIT_OK;
}

ClusterRelmapInitResult
cluster_relmap_authority_init_image(bool shared_map, Oid dbid, const void *native_map,
									size_t native_len, void *output, size_t output_len)
{
	RelMapFile map;
	ClusterRelmapAuthorityHeader header;
	char image[CLUSTER_RELMAP_AUTHORITY_FILE_SIZE];
	ClusterRelmapInitResult result;
	const unsigned char *destination = output;

	if (native_map == NULL || output == NULL)
		return CLUSTER_RELMAP_INIT_INVALID_ARGUMENT;
	if (native_len != sizeof(map) || output_len != sizeof(image))
		return CLUSTER_RELMAP_INIT_INVALID_LENGTH;
	if (shared_map != (dbid == InvalidOid))
		return CLUSTER_RELMAP_INIT_NAMESPACE_MISMATCH;
	memcpy(&map, native_map, sizeof(map));
	result = validate_native_map(&map, shared_map);
	if (result != CLUSTER_RELMAP_INIT_OK)
		return result;
	for (size_t i = 0; i < output_len; ++i)
		if (destination[i] != 0)
			return CLUSTER_RELMAP_INIT_OUTPUT_NOT_EMPTY;

	/* Same initial bytes as write_pending(1, empty owner) followed by publish. */
	memset(&header, 0, sizeof(header));
	header.magic = CLUSTER_RELMAP_AUTHORITY_MAGIC;
	header.version = CLUSTER_RELMAP_AUTHORITY_VERSION;
	header.committed_generation = 1;
	header.dbid = dbid;
	header.shared_map = shared_map ? 1 : 0;
	header.image_size = sizeof(map);
	INIT_CRC32C(header.crc);
	COMP_CRC32C(header.crc, &header, offsetof(ClusterRelmapAuthorityHeader, crc));
	FIN_CRC32C(header.crc);
	memset(image, 0, sizeof(image));
	memcpy(image, &header, sizeof(header));
	memcpy(image + CLUSTER_RELMAP_COMMITTED_OFFSET, &map, sizeof(map));
	memcpy(image + CLUSTER_RELMAP_PENDING_OFFSET, &map, sizeof(map));
	memcpy(output, image, sizeof(image));
	return CLUSTER_RELMAP_INIT_OK;
}
