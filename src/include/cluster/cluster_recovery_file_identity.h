/* PGRAC: logical recovery file identities, independent of the disk codec. */
#ifndef CLUSTER_RECOVERY_FILE_IDENTITY_H
#define CLUSTER_RECOVERY_FILE_IDENTITY_H

#include "c.h"
#include "access/transam.h"
#include "access/xlogdefs.h"

#define CLUSTER_RECOVERY_V2_UUID_BYTES 16
#define CLUSTER_RECOVERY_V2_DIGEST_BYTES 32

/* Logical tags only; these values do not allocate persistent format IDs. */
typedef enum ClusterRecoveryFileKindV2
{
	CLUSTER_RECOVERY_FILE_UNKNOWN_V2 = 0,
	CLUSTER_RECOVERY_FILE_CLOG_V2,
	CLUSTER_RECOVERY_FILE_UNDO_V2,
	CLUSTER_RECOVERY_FILE_RELMAP_V2,
	CLUSTER_RECOVERY_FILE_DEBT_V2
} ClusterRecoveryFileKindV2;

/*
 * A by-value identity, not ownership or permission.  Copying it does not bind a
 * file to the control authority.  Zero format fields mean unassigned.  Origin
 * zero is valid.  Inodes, paths and current transaction counters are not file
 * creation identities.  Disk codecs must encode fields explicitly, never copy
 * this structure (including its padding) to disk or WAL.
 */
typedef struct ClusterRecoveryFileIdentityV2
{
	ClusterRecoveryFileKindV2 kind;
	uint32		format_id;
	uint32		format_version;
	uint64		system_identifier;
	uint64		database_incarnation;
	uint8		storage_uuid[CLUSTER_RECOVERY_V2_UUID_BYTES];
	uint8		redo_branch_uuid[CLUSTER_RECOVERY_V2_UUID_BYTES];
	uint64		redo_branch_generation;
	TimeLineID	redo_timeline;
	Oid			database_oid;
	Oid			tablespace_oid;
	uint16		origin;
	uint64		object_id;
	uint64		creation_generation;
	uint64		registration_generation;
} ClusterRecoveryFileIdentityV2;

/* Birth is an actual producer observation, not inferred from an XID counter. */
typedef struct ClusterRecoveryPageIdentityV2
{
	uint64		logical_page;
	uint64		page_generation;
	bool		birth_observed;
	FullTransactionId birth_full_xid;
	XLogRecPtr	page_lsn;
	uint32		metadata_crc;
	uint32		payload_crc;
} ClusterRecoveryPageIdentityV2;

/* A file identity owns no allocation, FD or pin; reset forgets only this value. */
static inline void
cluster_recovery_file_identity_reset_v2(ClusterRecoveryFileIdentityV2 *identity)
{
	if (identity != NULL)
		memset(identity, 0, sizeof(*identity));
}

#endif							/* CLUSTER_RECOVERY_FILE_IDENTITY_H */
