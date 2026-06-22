/*-------------------------------------------------------------------------
 *
 * cluster_cf_authority.h
 *	  Shared pg_control single-authority file: path resolution, buffer
 *	  classification, read-source decision, and atomic (durable_rename)
 *	  authority read/write.
 *
 *	  In a pgrac cluster the control file is migrated from a per-node
 *	  $PGDATA/global/pg_control into one shared authority living under
 *	  cluster.shared_data_dir; every node's local path becomes a symlink
 *	  to it (see Da2).  This module owns that shared authority file: it
 *	  reads it (primary, then a strictly-validated .bak fallback) and
 *	  writes it atomically so a reader never observes a torn image.
 *
 *	  The correctness-critical part -- deciding when a read must
 *	  fail-closed rather than trust a stale/torn/foreign image -- is
 *	  factored into two pure functions (cluster_cf_classify_buffer and
 *	  cluster_cf_decide_source) so it can be unit-tested in isolation,
 *	  independently of the durable_rename I/O.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_cf_authority.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.6-cf-enqueue-shared-controlfile-authority.md (Da1)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CF_AUTHORITY_H
#define CLUSTER_CF_AUTHORITY_H

#include "catalog/pg_control.h"

/*
 * Relative paths of the shared authority and its corruption-recovery
 * backup, joined under cluster.shared_data_dir.  The primary keeps the
 * stock "global/pg_control" relative name so the per-node symlink target
 * is a fixed, well-known path that backend ReadControlFile and frontend
 * pg_controldata open transparently (spec §3.9 T1).
 */
#define CLUSTER_CF_REL_PATH		"global/pg_control"
#define CLUSTER_CF_BAK_REL_PATH "global/pg_control.bak"
#define CLUSTER_CF_TMP_REL_PATH "global/pg_control.tmp"
#define CLUSTER_CF_BAK_TMP_REL_PATH "global/pg_control.bak.tmp"

/*
 * ClusterCfValidity -- pure classification of one raw control-file image.
 *
 *	VALID			size, CRC and byte order all check out (and the
 *					system_identifier matches the expected one when an
 *					expected value was supplied).
 *	INVALID_SHORT	fewer than sizeof(ControlFileData) bytes available.
 *	INVALID_CRC		stored CRC does not match recomputed CRC (torn or
 *					corrupt image).
 *	INVALID_BYTE_ORDER	pg_control_version indicates a foreign byte order.
 *	INVALID_IDENTITY	CRC is fine but system_identifier differs from the
 *					expected one (symlink points at a foreign cluster).
 */
typedef enum ClusterCfValidity
{
	CLUSTER_CF_VALID = 0,
	CLUSTER_CF_INVALID_SHORT,
	CLUSTER_CF_INVALID_CRC,
	CLUSTER_CF_INVALID_BYTE_ORDER,
	CLUSTER_CF_INVALID_IDENTITY
} ClusterCfValidity;

/*
 * ClusterCfReadSource -- pure decision of which on-disk image to trust.
 *
 *	PRIMARY			the primary pg_control image is valid; use it.
 *	BAK				the primary is unusable but the .bak image is valid
 *					AND passed the strict (non-CRC-only) acceptance check;
 *					use it (a corruption-recovery path, not the hot path).
 *	FAILCLOSED		neither image can be trusted -> caller must raise a
 *					FATAL/ERROR rather than proceed (spec §3.1 A3).
 */
typedef enum ClusterCfReadSource
{
	CLUSTER_CF_SOURCE_PRIMARY = 0,
	CLUSTER_CF_SOURCE_BAK,
	CLUSTER_CF_SOURCE_FAILCLOSED
} ClusterCfReadSource;

/*
 * Path accessors.  Each returns a pointer to a per-function static buffer
 * rebuilt from cluster_shared_data_dir on every call; the caller must copy
 * the string if it needs to outlive the next call to the same accessor.
 * Returns NULL when cluster_shared_data_dir is unset.
 */
extern const char *cluster_cf_shared_path(void);
extern const char *cluster_cf_bak_path(void);

/*
 * Pure helpers (no I/O, no ereport) -- unit-tested in isolation.
 *
 * cluster_cf_classify_buffer: classify a raw image of `len` bytes.  When
 * `expected_sysid` is non-zero the system_identifier is cross-checked; pass
 * 0 to skip the identity check (e.g. the bootstrap early read before the
 * expected identity is known).
 *
 * cluster_cf_decide_source: given the primary and .bak classifications plus
 * whether the .bak passed the strict acceptance check, decide which source
 * to trust.  A .bak that is merely CRC-valid but failed the strict check is
 * never used (it could be CRC-correct yet stale/unreplayable, spec §3.9 T3).
 */
extern ClusterCfValidity cluster_cf_classify_buffer(const char *buf, size_t len,
													uint64 expected_sysid);
extern ClusterCfReadSource cluster_cf_decide_source(ClusterCfValidity primary,
													ClusterCfValidity bak,
													bool bak_strict_ok);

/*
 * cluster_cf_bak_strict_ok: strict (non-CRC-only) acceptance of a .bak image
 * before it is trusted as a corruption-recovery fallback (spec §3.9 T3).
 * Pure: true iff `bak` is non-NULL, its system_identifier matches
 * `expected_sysid` when that is non-zero, and `checkpoint_recoverable` is
 * true.  Recoverability is computed by the (impure) probe below and passed
 * in, keeping this decision unit-testable in isolation.
 */
extern bool cluster_cf_bak_strict_ok(const ControlFileData *bak,
									 uint64 expected_sysid,
									 bool checkpoint_recoverable);

/*
 * cluster_cf_bak_checkpoint_recoverable: does the WAL needed to replay from
 * the .bak's checkpoint still exist on this node?  Stats the segment holding
 * the redo start under pg_wal and returns false (fail-closed) when it is
 * absent/unreachable, so a stale-but-CRC-valid .bak is never silently used.
 * Defined in the recovery/storage layer; unit tests of the authority module
 * stub it to drive the acceptance decision.
 */
extern bool cluster_cf_bak_checkpoint_recoverable(const ControlFileData *bak);

/*
 * Read the shared authority into *out.  Tries the primary first (CRC), then
 * the strictly-validated .bak.  Returns true and fills *out when a
 * trustworthy image was found; returns false (and leaves *out untouched)
 * when the read must fail-closed -- the caller raises FATAL/ERROR.  Does not
 * itself ereport, so it is safe on the bootstrap early-read path.
 */
extern bool cluster_cf_authority_read(ControlFileData *out);

/*
 * Atomically write *cf to the shared authority: copy the current primary to
 * .bak, write the new image to pg_control.tmp, fsync it, durable_rename it
 * over the primary and fsync the directory.  The caller must hold CF X and
 * is responsible for the lock; on I/O failure this PANICs (mirroring the
 * stock update_controlfile contract).
 */
extern void cluster_cf_authority_write(const ControlFileData *cf);

#endif							/* CLUSTER_CF_AUTHORITY_H */
