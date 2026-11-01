/* Original creator's same-build pipe, never a persisted identity authority. */
#ifndef PGRAC_INITDB_RECOVERY_H
#define PGRAC_INITDB_RECOVERY_H
#include "common/pgrac_initdb_wal.h"
#include "cluster/cluster_recovery_file_identity.h"
#include "cluster/cluster_recovery_checkpoint_coverage.h"

#define PGRAC_INITDB_RECOVERY_ENV "PGRAC_INITDB_RECOVERY_FD"
#define PGRAC_INITDB_RECOVERY_MAGIC UINT32_C(0x50474952)
#define PGRAC_INITDB_BOOT_FILES_V2 2

typedef struct PgracInitdbRecoveryContext {
	uint32 magic;
	uint32 version;
	uint64 creator_pid;
	PgracInitdbConfigContext config;
	ClusterRecoveryFileIdentityV2 first;
	uint64 data_device, data_inode, shared_device, shared_inode;
	/* Complete original initial set: CLOG segment 0, then local seed UNDO 0.
	 * Zero count is the older explicit relmap-only request, never full BOOT. */
	uint64 boot_file_count;
	ClusterRecoveryFileIdentityV2 boot_files[PGRAC_INITDB_BOOT_FILES_V2];
	/* Explicit complete creation cohort; zero preserves the older isolated
	 * relmap request. Only this selected origin's CLOG0 is a BOOT file; the
	 * local undo seed is unpublished material, never a table/registry entry. */
	uint64 cohort[2];
	uint64 cohort_origin;
} PgracInitdbRecoveryContext;

extern bool pgrac_initdb_recovery_prepare_cohort_v2(
	const ClusterRecoveryFileIdentityV2 *first_relmap, const ClusterRecoveryFileIdentityV2 *clog,
	const uint64 cohort[2], uint16 origin, const PgracInitdbConfigContext *, int data_fd,
	int shared_fd, PgracInitdbRecoveryContext *out);
extern bool pgrac_initdb_recovery_prepare_files_v2(
	const ClusterRecoveryFileIdentityV2 *first,
	const ClusterRecoveryFileIdentityV2 boot[PGRAC_INITDB_BOOT_FILES_V2],
	const PgracInitdbConfigContext *config, int data_fd, int shared_fd,
	PgracInitdbRecoveryContext *out);
extern bool
pgrac_initdb_recovery_boot_v2(const PgracInitdbRecoveryContext *,
							  ClusterRecoveryFileIdentityV2 out[PGRAC_INITDB_BOOT_FILES_V2]);

/* Original creator allocates first + next three creation generations before
 * shared file creation. first is global relmap; local object IDs are DB OIDs.
 * No identity is recovered from an existing target. Output remains unchanged
 * on refusal. Directory/config custody and ROOT publication remain caller's. */
extern bool pgrac_initdb_recovery_prepare_v2(const ClusterRecoveryFileIdentityV2 *first,
											 const PgracInitdbConfigContext *config, int data_fd,
											 int shared_fd, PgracInitdbRecoveryContext *out);
extern bool pgrac_initdb_recovery_relmaps_v2(const PgracInitdbRecoveryContext *,
											 ClusterRecoveryFileIdentityV2 out[4]);
extern bool pgrac_initdb_recovery_relay_v2(const PgracInitdbRecoveryContext *, int *out);
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
/* Original creator only, after its native child has completed. Sync and read
 * all four original pairs against preallocated identities; no file supplies an
 * identity. Eight observations have no source vector or coverage authority.
 * Caller retains original directory/config custody and rechecks it at publish.
 * Refusal leaves output and borrowed descriptors unchanged. */
extern bool pgrac_initdb_recovery_relmap_observe_v2(const PgracInitdbRecoveryContext *,
													int data_fd, int shared_fd,
													ClusterRecoveryCheckpointFileV2 out[8]);
/* Read-only complete comparison with the captured eight observations; no sync,
 * repair, rebasing or restoration of a failed original creation attempt. */
extern bool
pgrac_initdb_recovery_relmap_current_v2(const PgracInitdbRecoveryContext *, int data_fd,
										int shared_fd,
										const ClusterRecoveryCheckpointFileV2 captured[8]);

#endif
