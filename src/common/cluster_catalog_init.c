/* Original catalog/allocator images; no file or runtime services.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "access/transam.h"
#include "catalog/catversion.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_catalog_init.h"
#include "cluster/cluster_catalog_migrate.h"
#include "cluster/cluster_oid_lease.h"
#include "cluster/cluster_xid_authority.h"

static bool
initial_overlap(const void *a, size_t alen, const void *b, size_t blen)
{
	uintptr_t ap = (uintptr_t)a;
	uintptr_t bp = (uintptr_t)b;

	if (a == NULL || b == NULL || alen == 0 || blen == 0)
		return false;
	return ap <= bp ? bp - ap < alen : ap - bp < blen;
}

static bool
initial_inputs(const ClusterCatalogInitialInput *input, ControlFileData *cf)
{
	static const uint8 zero_uuid[16] = { 0 };
	uint64 hw;
	size_t clog_length;
	pg_crc32c crc;

	if (input == NULL || input->native_control == NULL || input->native_clog == NULL
		|| input->native_control_length != sizeof(*cf) || input->identity.system_identifier == 0
		|| input->identity.database_incarnation == 0
		|| memcmp(input->identity.authority_uuid, zero_uuid, sizeof(zero_uuid)) == 0
		|| memcmp(input->identity.storage_uuid, zero_uuid, sizeof(zero_uuid)) == 0)
		return false;
	memcpy(cf, input->native_control, sizeof(*cf));
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, cf, offsetof(ControlFileData, crc));
	FIN_CRC32C(crc);
	if (!EQ_CRC32C(crc, cf->crc) || cf->system_identifier != input->identity.system_identifier
		|| cf->pg_control_version != PG_CONTROL_VERSION
		|| cf->catalog_version_no != CATALOG_VERSION_NO || cf->blcksz != BLCKSZ
		|| cf->state != DB_SHUTDOWNED || cf->checkPoint == InvalidXLogRecPtr
		|| cf->checkPointCopy.redo != cf->checkPoint || cf->checkPointCopy.ThisTimeLineID != 1
		|| cf->checkPointCopy.PrevTimeLineID != 1 || cf->minRecoveryPoint != 0
		|| cf->minRecoveryPointTLI != 0 || cf->backupStartPoint != 0 || cf->backupEndPoint != 0
		|| cf->backupEndRequired || cf->max_prepared_xacts != 0 || cf->track_commit_timestamp
		|| cf->checkPointCopy.nextOid < FirstGenbkiObjectId || cf->checkPointCopy.nextMulti != 1
		|| cf->checkPointCopy.nextMultiOffset != 0 || cf->checkPointCopy.oldestMulti != 1)
		return false;
	hw = U64FromFullTransactionId(cf->checkPointCopy.nextXid);
	if (hw < FirstNormalTransactionId || hw > CLUSTER_XID_PREHISTORY_MAX_XID
		|| cf->checkPointCopy.oldestXid < FirstNormalTransactionId
		|| cf->checkPointCopy.oldestXid > hw)
		return false;
	clog_length = ((hw + BLCKSZ * 4 - 1) / (BLCKSZ * 4)) * BLCKSZ;
	if (input->native_clog_length != clog_length)
		return false;
	for (uint64 xid = FirstNormalTransactionId; xid < hw; ++xid)
		if (((input->native_clog[xid / 4] >> (2 * (xid % 4))) & 3)
			== CLUSTER_NATIVE_CLOG_SUB_COMMITTED)
			return false;
	return true;
}

size_t
cluster_catalog_initial_image_size(ClusterCatalogInitialKind kind,
								   const ClusterCatalogInitialInput *input)
{
	ControlFileData cf;

	if (!initial_inputs(input, &cf))
		return 0;
	switch (kind) {
	case CLUSTER_CATALOG_INITIAL_OID:
		return sizeof(ClusterOidAuthorityHeader);
	case CLUSTER_CATALOG_INITIAL_MARKER:
		return sizeof(ClusterCatalogAuthorityMarker);
	case CLUSTER_CATALOG_INITIAL_XID:
		return sizeof(ClusterXidAuthorityHeader);
	case CLUSTER_CATALOG_INITIAL_PREHISTORY:
		return sizeof(ClusterXidPrehistoryHeader) + input->native_clog_length;
	}
	return 0;
}

bool
cluster_catalog_initial_image(ClusterCatalogInitialKind kind,
							  const ClusterCatalogInitialInput *input, void *output,
							  size_t output_length)
{
	ControlFileData cf;
	size_t required = cluster_catalog_initial_image_size(kind, input);
	const uint8 *destination = output;

	if (required == 0 || output == NULL || output_length != required
		|| initial_overlap(output, output_length, input, sizeof(*input))
		|| initial_overlap(output, output_length, input->native_control,
						   input->native_control_length)
		|| initial_overlap(output, output_length, input->native_clog, input->native_clog_length))
		return false;
	for (size_t i = 0; i < output_length; ++i)
		if (destination[i] != 0)
			return false;
	memcpy(&cf, input->native_control, sizeof(cf));
	switch (kind) {
	case CLUSTER_CATALOG_INITIAL_OID: {
		ClusterOidAuthorityHeader h;
		memset(&h, 0, sizeof(h));
		h.magic = CLUSTER_OID_AUTHORITY_MAGIC;
		h.version = CLUSTER_OID_AUTHORITY_VERSION;
		h.next_oid = Max(cf.checkPointCopy.nextOid, (Oid)FirstNormalObjectId);
		INIT_CRC32C(h.crc);
		COMP_CRC32C(h.crc, &h, offsetof(ClusterOidAuthorityHeader, crc));
		FIN_CRC32C(h.crc);
		memcpy(output, &h, sizeof(h));
		return true;
	}
	case CLUSTER_CATALOG_INITIAL_MARKER: {
		ClusterCatalogAuthorityMarker h;
		memset(&h, 0, sizeof(h));
		h.magic = CLUSTER_CATALOG_AUTHORITY_MAGIC;
		h.version = CLUSTER_CATALOG_AUTHORITY_VERSION;
		h.system_identifier = cf.system_identifier;
		h.catalog_version_no = cf.catalog_version_no;
		INIT_CRC32C(h.crc);
		COMP_CRC32C(h.crc, &h, offsetof(ClusterCatalogAuthorityMarker, crc));
		FIN_CRC32C(h.crc);
		memcpy(output, &h, sizeof(h));
		return true;
	}
	case CLUSTER_CATALOG_INITIAL_XID: {
		ClusterXidAuthorityHeader h;
		memset(&h, 0, sizeof(h));
		h.magic = CLUSTER_XID_AUTHORITY_MAGIC;
		h.version = CLUSTER_XID_AUTHORITY_VERSION;
		h.flags = CLUSTER_XID_AUTHORITY_FLAG_SEALED | CLUSTER_XID_AUTHORITY_FLAG_CLUSTER_ERA;
		h.native_hw_full = U64FromFullTransactionId(cf.checkPointCopy.nextXid);
		h.next_multi = cf.checkPointCopy.nextMulti;
		INIT_CRC32C(h.crc);
		COMP_CRC32C(h.crc, &h, offsetof(ClusterXidAuthorityHeader, crc));
		FIN_CRC32C(h.crc);
		memcpy(output, &h, sizeof(h));
		return true;
	}
	case CLUSTER_CATALOG_INITIAL_PREHISTORY: {
		ClusterXidPrehistoryHeader h;
		memset(&h, 0, sizeof(h));
		h.magic = CLUSTER_XID_PREHISTORY_MAGIC;
		h.version = CLUSTER_XID_PREHISTORY_VERSION;
		h.native_hw_full = U64FromFullTransactionId(cf.checkPointCopy.nextXid);
		h.payload_len = input->native_clog_length;
		INIT_CRC32C(h.crc);
		COMP_CRC32C(h.crc, &h, offsetof(ClusterXidPrehistoryHeader, crc));
		COMP_CRC32C(h.crc, input->native_clog, input->native_clog_length);
		FIN_CRC32C(h.crc);
		memcpy(output, &h, sizeof(h));
		memcpy((uint8 *)output + sizeof(h), input->native_clog, input->native_clog_length);
		return true;
	}
	}
	return false;
}
