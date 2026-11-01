/* Original creation only; no normal startup or reader publication authority.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_INITDB_ORIGIN_PRIVATE_H
#define CLUSTER_INITDB_ORIGIN_PRIVATE_H

#include <sys/stat.h>
#include "cluster/cluster_shared_config.h"
#include "cluster_control_root_private.h"
#include "common/pgrac_initdb_recovery.h"

/* Caller owns the new cohort, has reaped all native initializers, synced
 * their files and holds both freshly created directories. Recheck their
 * named identity and the original control before any later ROOT publication.
 * A partial failure leaves unselected objects; it never adopts/replaces one.
 * On refusal a distinct out is zero. These bytes confer no writer/OPEN grant. */
extern bool cluster_initdb_origin_create(const ClusterSharedConfigRef *config, uint32 node,
										 int64 created_at, int wal_fd, int anchor_fd,
										 const ControlFileData *control,
										 ClusterWalHistoryRecord *out);

/* Bounded original-object I/O, not creation ownership or selection proof. */
extern bool cluster_initdb_object_write_new(int directory, const char *name, const uint8 *bytes,
											Size length);

/* Original owner retains this volatile file identity for a final, read-only
 * check of its own published objects. It is never admission or adoption. */
extern bool cluster_initdb_object_write_observed(int directory, const char *name,
												 const uint8 *bytes, Size length, struct stat *out);
extern bool cluster_initdb_object_recheck(int directory, const char *name, const uint8 *bytes,
										  Size length, const struct stat *expected);

/* Original new-cohort owner only, before launching the founder. Reserve all
 * four births from its exact config/first redo branch, without reading any
 * existing authority file. No ROOT selection or format activation. */
extern bool cluster_initdb_cohort_prepare_v2(const ClusterSharedConfigRef *,
											 const PgracInitdbConfigContext *,
											 const uint8 redo_branch_uuid[16], uint16 origin,
											 int data_fd, int shared_fd,
											 PgracInitdbRecoveryContext *out);
extern bool cluster_initdb_cohort_relmap_prepare_v2(const ClusterSharedConfigRef *,
	const PgracInitdbConfigContext *, const uint8 redo_branch_uuid[16],
	int data_fd, int shared_fd, PgracInitdbRecoveryContext *out);

#endif
