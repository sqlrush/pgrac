/* Read-only original catalog and current allocator validation.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "access/transam.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_catalog_migrate.h"
#include "cluster/cluster_catalog_startup.h"
#include "cluster/cluster_oid_lease.h"
#include "cluster/cluster_xid_authority.h"

static bool
image_present(const ClusterCatalogInputImage *image, size_t length)
{
	return length != 0 && image->bytes != NULL && image->length == length;
}

static bool
xid_image_matches(const ClusterCatalogInputImage *image, const ClusterXidAuthorityHeader *original)
{
	ClusterXidAuthorityHeader actual;
	ClusterXidAuthorityHeader expected;
	uint32 required = CLUSTER_XID_AUTHORITY_FLAG_SEALED | CLUSTER_XID_AUTHORITY_FLAG_CLUSTER_ERA;
	uint32 allowed = required | CLUSTER_XID_AUTHORITY_FLAG_NATIVE_RAW_REUSED
					 | CLUSTER_XID_AUTHORITY_FLAG_EPOCH_GATE_ADMITTED;

	if (!image_present(image, sizeof(actual)))
		return false;
	memcpy(&actual, image->bytes, sizeof(actual));
	if ((actual.flags & required) != required || (actual.flags & ~allowed) != 0
		|| ((actual.flags & CLUSTER_XID_AUTHORITY_FLAG_EPOCH_GATE_ADMITTED)
			&& !(actual.flags & CLUSTER_XID_AUTHORITY_FLAG_NATIVE_RAW_REUSED)))
		return false;
	memcpy(&expected, original, sizeof(expected));
	expected.flags = actual.flags;
	if (actual.flags & CLUSTER_XID_AUTHORITY_FLAG_NATIVE_RAW_REUSED)
		expected.magic = CLUSTER_XID_AUTHORITY_MAGIC_RAW_REUSED;
	INIT_CRC32C(expected.crc);
	COMP_CRC32C(expected.crc, &expected, offsetof(ClusterXidAuthorityHeader, crc));
	FIN_CRC32C(expected.crc);
	return memcmp(image->bytes, &expected, sizeof(expected)) == 0;
}

bool
cluster_catalog_startup_validate(const ClusterCatalogStartupInput *input)
{
	ClusterCatalogManifestInitial manifest;
	ClusterCatalogAuthorityMarker marker;
	ClusterOidAuthorityHeader oid;
	ClusterOidAuthorityHeader expected_oid;
	ClusterXidAuthorityHeader xid;
	ClusterXidPrehistoryHeader prehistory;
	size_t prehistory_length;

	if (input == NULL
		|| !cluster_catalog_manifest_decode_initial(
			input->manifest.bytes, input->manifest.length, &input->original.identity,
			input->catalog_generation, input->catalog_sha256, &manifest))
		return false;
	prehistory_length
		= cluster_catalog_initial_image_size(CLUSTER_CATALOG_INITIAL_PREHISTORY, &input->original);
	if (!image_present(&input->prehistory, prehistory_length)
		|| !image_present(&input->marker, sizeof(marker))
		|| !image_present(&input->oid, sizeof(oid)))
		return false;
	memset(&marker, 0, sizeof(marker));
	memset(&expected_oid, 0, sizeof(expected_oid));
	memset(&xid, 0, sizeof(xid));
	if (!cluster_catalog_initial_image(CLUSTER_CATALOG_INITIAL_MARKER, &input->original, &marker,
									   sizeof(marker))
		|| !cluster_catalog_initial_image(CLUSTER_CATALOG_INITIAL_OID, &input->original,
										  &expected_oid, sizeof(expected_oid))
		|| !cluster_catalog_initial_image(CLUSTER_CATALOG_INITIAL_XID, &input->original, &xid,
										  sizeof(xid))
		|| memcmp(input->marker.bytes, &marker, sizeof(marker)) != 0)
		return false;

	/* Issued OIDs may advance or wrap. Never restore the original high-water. */
	memcpy(&oid, input->oid.bytes, sizeof(oid));
	if (oid.next_oid != InvalidOid && oid.next_oid < FirstNormalObjectId)
		return false;
	expected_oid.next_oid = oid.next_oid;
	INIT_CRC32C(expected_oid.crc);
	COMP_CRC32C(expected_oid.crc, &expected_oid, offsetof(ClusterOidAuthorityHeader, crc));
	FIN_CRC32C(expected_oid.crc);
	if (memcmp(input->oid.bytes, &expected_oid, sizeof(expected_oid)) != 0
		|| !xid_image_matches(&input->xid, &xid) || !xid_image_matches(&input->xid_backup, &xid)
		|| memcmp(input->xid.bytes, input->xid_backup.bytes, sizeof(xid)) != 0)
		return false;

	memset(&prehistory, 0, sizeof(prehistory));
	prehistory.magic = CLUSTER_XID_PREHISTORY_MAGIC;
	prehistory.version = CLUSTER_XID_PREHISTORY_VERSION;
	prehistory.native_hw_full = xid.native_hw_full;
	prehistory.payload_len = input->original.native_clog_length;
	INIT_CRC32C(prehistory.crc);
	COMP_CRC32C(prehistory.crc, &prehistory, offsetof(ClusterXidPrehistoryHeader, crc));
	COMP_CRC32C(prehistory.crc, input->original.native_clog, input->original.native_clog_length);
	FIN_CRC32C(prehistory.crc);
	return memcmp(input->prehistory.bytes, &prehistory, sizeof(prehistory)) == 0
		   && memcmp((const uint8 *)input->prehistory.bytes + sizeof(prehistory),
					 input->original.native_clog, input->original.native_clog_length)
				  == 0;
}
