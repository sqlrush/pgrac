/*-------------------------------------------------------------------------
 * PGRAC: provisional early-control composition, never startup admission.
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CONTROL_BOOTSTRAP_PRIVATE_H
#define CLUSTER_CONTROL_BOOTSTRAP_PRIVATE_H

#include "catalog/pg_control.h"
#include "cluster/cluster_shared_config.h"
#include "cluster/cluster_wal_durable_prefix.h"
#include "common/pgrac_control_binding.h"

typedef struct ClusterControlBootstrapBytes {
	const uint8 *data;
	size_t len;
} ClusterControlBootstrapBytes;

typedef struct ClusterControlBootstrapInput {
	uint32 node_id;
	ClusterControlBootstrapBytes binding;
	ClusterControlBootstrapBytes root_before;
	ClusterControlBootstrapBytes root_after;
	ClusterControlBootstrapBytes common;
	ClusterControlBootstrapBytes config;
	ClusterControlBootstrapBytes claim;
	ClusterControlBootstrapBytes anchor;
} ClusterControlBootstrapInput;

/*
 * A memory-only observation, NOT a CF token, migration/qualification proof or
 * permission to serve. The caller still owns all supplied bytes. No GUCs are
 * applied here. Physical qualification, full startup/recovery/config gates and
 * final real CF-bound revalidation remain mandatory before shared mutation.
 */
typedef struct ClusterControlBootstrapSnapshot {
	PgracControlBinding binding;
	ClusterControlRootIdentity thread;
	ClusterWalDurablePrefixRef wal;
	ClusterSharedConfigRef config;
	ControlFileData control;
	uint8 root_sha256[32];
	uint64 root_sequence;
	uint32 database_state;
	uint32 activation_state;
} ClusterControlBootstrapSnapshot;

/*
 * Compose only the root-selected objects, using both captured root versions.
 * All inputs must remain immutable during this call. No input may overlap out.
 * Every refusal clears out. Uses the caller's memory/hash resource owner, but
 * performs no I/O, native assignment, locking, retry or publication.
 */
extern ClusterControlRootResult
cluster_control_bootstrap_decode(const ClusterControlBootstrapInput *input,
								 ClusterControlBootstrapSnapshot *out);

struct ControlRootImage;
/* Exact on-disk version dispatch, not fallback or conversion. V1 is not a
 * shared-config bootstrap input; every v2/v3 validation remains mandatory. */
extern ClusterControlRootResult
cluster_control_bootstrap_root_decode(const uint8 *bytes, size_t len, const uint8 storage_uuid[16],
									  uint64 system_identifier, struct ControlRootImage *out);
/* Allocation-free binding check shared by the byte composer and collector. */
extern ClusterControlRootResult
cluster_control_bootstrap_root_bound(const PgracControlBinding *binding,
									 const struct ControlRootImage *root);

typedef struct ClusterControlBootstrapObservation {
	ClusterControlBootstrapSnapshot snapshot;
	ClusterControlRecoveryCapacity required;
	char *config_bytes;
	size_t config_len;
} ClusterControlBootstrapObservation;

/*
 * One read-only provisional observation, never a lock/admission token. Uses
 * independently configured absolute paths and node, and the retained binding.
 * No .bak/projection fallback, retry, mutation or GUC application. Every present
 * current and retained writer's claim/anchor contributes recovery capacities;
 * selected pending DURABLE writers contribute their own claim/anchor, while
 * INITIALIZING consumes the exact actual WAL/PGWP and native parameter records,
 * not an unselected anchor or guessed empty stream. This is sizing, not replay.
 * RESERVED permits no WAL mutation and contributes no new capacity minimum.
 * no lifecycle or serving bit bypasses input checks. These are root-selected
 * minima, not proof of the full recovery-obligation/WAL input union. All raw FDs
 * close before composition uses the caller's memory/hash resource owner.
 * Successful config_bytes is palloc-owned by the caller; free it before reusing
 * out. Every refusal clears out. Paths and out must not overlap. Earlier path
 * ancestors are trusted deployment paths; final roots and descendants are
 * no-follow, owned and not writable by group/other. Requires later real CF/root
 * revalidation, physical qualification and complete startup/admission gates.
 */
extern ClusterControlRootResult
cluster_control_bootstrap_read(const char *pgdata, const char *shared_root, const char *wal_root,
							   uint32 node_id, ClusterControlBootstrapObservation *out);

/* Read-only route/input check. pg_wal may be a symlink, but must resolve to the
 * exact owned no-follow generation selected by ref. Does not certify WAL bytes
 * or grant writer admission, and never creates a missing claim or prefix. */
extern ClusterControlRootResult
cluster_control_bootstrap_wal_route(const char *pgdata, const char *wal_root,
									const ClusterWalDurablePrefixRef *ref);

/* Process-local preparation and native control/geometry initialization only,
 * before shared memory or WAL startup. Not admission, physical qualification
 * or final CF validation. Reset has LocalProcessControlFile's native semantics.
 * Assignment is not reversible: every refusal is FATAL. Independent paths and
 * output must not overlap. */
typedef struct ClusterControlBootstrapPrepared {
	ClusterControlBootstrapSnapshot snapshot;
	ClusterControlRecoveryCapacity required;
	ClusterSharedConfigApplied applied;
} ClusterControlBootstrapPrepared;
extern void cluster_control_bootstrap_prepare(const char *pgdata, const char *shared_root,
											  const char *wal_root, const char *undo_root,
											  uint32 node_id, bool reset,
											  ClusterControlBootstrapPrepared *out);

/* Postmaster's actual WAL-thread initialization must reobserve the successful
 * early preparation before putting this read-only reference in shared memory.
 * FATAL when unprepared, changed, misrouted or called from a child. */
extern void cluster_control_bootstrap_wal_recheck(const char *pgdata,
												  ClusterWalDurablePrefixRef *out);

#endif /* CLUSTER_CONTROL_BOOTSTRAP_PRIVATE_H */
