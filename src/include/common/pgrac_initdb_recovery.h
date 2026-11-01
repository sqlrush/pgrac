/* Original creator's same-build pipe, never a persisted identity authority. */
#ifndef PGRAC_INITDB_RECOVERY_H
#define PGRAC_INITDB_RECOVERY_H
#include "common/pgrac_initdb_wal.h"
#include "cluster/cluster_recovery_file_identity.h"

#define PGRAC_INITDB_RECOVERY_ENV "PGRAC_INITDB_RECOVERY_FD"
#define PGRAC_INITDB_RECOVERY_MAGIC UINT32_C(0x50474952)

typedef struct PgracInitdbRecoveryContext {
	uint32 magic;
	uint32 version;
	uint64 creator_pid;
	PgracInitdbConfigContext config;
	ClusterRecoveryFileIdentityV2 first;
	uint64 data_device, data_inode, shared_device, shared_inode;
} PgracInitdbRecoveryContext;

/* Original creator allocates first + next three creation generations before
 * shared file creation. first is global relmap; local object IDs are DB OIDs.
 * No identity is recovered from an existing target. Output remains unchanged
 * on refusal. Directory/config custody and ROOT publication remain caller's. */
extern bool pgrac_initdb_recovery_prepare_v2(const ClusterRecoveryFileIdentityV2 *first,
											 const PgracInitdbConfigContext *config, int data_fd,
											 int shared_fd, PgracInitdbRecoveryContext *out);
extern bool pgrac_initdb_recovery_relmaps_v2(const PgracInitdbRecoveryContext *,
											 ClusterRecoveryFileIdentityV2 out[4]);
/* Closed-writer pipe owns only its local request FD; no serving/format grant.
 * The original fork/exec path must expose this descriptor only to its child. */
extern bool pgrac_initdb_recovery_pipe_v2(const PgracInitdbRecoveryContext *, int *out);
/* Consumes/closes a valid request pipe, including refusal. Borrowed original
 * data/shared/config FDs remain caller-owned. Native initdb must additionally
 * verify its final actual control/timeline and complete config before creation. */
extern bool pgrac_initdb_recovery_read_v2(int pipe_fd, const PgracInitdbConfigContext *config,
										  uint64 system_identifier, uint64 database_incarnation,
										  const uint8 storage_uuid[16], int data_fd, int shared_fd,
										  PgracInitdbRecoveryContext *out);
#endif
