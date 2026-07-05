/*-------------------------------------------------------------------------
 *
 * cluster_recovery_anchor.h
 *	  Per-node durable recovery anchor under the shared pg_control
 *	  authority: on-disk layout, pure classification, and torn-safe
 *	  read/write of the restart-critical per-node checkpoint state.
 *
 *	  With cluster.controlfile_shared_authority=on the one shared
 *	  pg_control carries the checkpoint fields of whichever node wrote it
 *	  last, so a restarting node must not use them to locate its own
 *	  checkpoint record in its own WAL thread.  Each node instead keeps a
 *	  small per-node sidecar file (the recovery anchor) holding its own
 *	  last checkpoint record LSN, a full CheckPoint copy, and its own
 *	  DBState; StartupXLOG consumes the anchor instead of the shared
 *	  checkpoint fields on a plain (label-less) restart.
 *
 *	  The fail-closed decision -- when an anchor image must be rejected
 *	  rather than trusted -- is a pure function
 *	  (cluster_recovery_anchor_classify) so it can be unit-tested in
 *	  isolation from the durable I/O.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_recovery_anchor.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.6a-per-node-recovery-anchor.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_ANCHOR_H
#define CLUSTER_RECOVERY_ANCHOR_H

#include "catalog/pg_control.h"

/* Identity of the on-disk anchor image. */
#define CLUSTER_RECOVERY_ANCHOR_MAGIC 0x50475241 /* 'PGRA' */
#define CLUSTER_RECOVERY_ANCHOR_VERSION 1

/*
 * On-disk size of the anchor file: one 512-byte sector image so a write is
 * as close to power-fail atomic as the platform allows; torn-safe
 * replacement is still done via temp-write + durable_rename regardless.
 */
#define CLUSTER_RECOVERY_ANCHOR_SIZE 512

/*
 * Relative path (under cluster.shared_data_dir) of one node's anchor.
 * The full family is <name>, <name>.tmp, <name>.bak, <name>.bak.tmp,
 * mirroring the pgrac_oid_authority naming.
 */
#define CLUSTER_RECOVERY_ANCHOR_REL_FMT "global/pgrac_recovery_anchor_n%d"

/*
 * ClusterRecoveryAnchor -- fixed 512-byte per-node restart anchor.
 *
 * Single writer: the owning node (its startup process or checkpointer,
 * serialized by the checkpoint interlock).  CRC32C over [0, crc).
 * Field offsets are locked by StaticAssertDecl below; changing the layout
 * requires a CLUSTER_RECOVERY_ANCHOR_VERSION bump.
 */
typedef struct ClusterRecoveryAnchor {
	uint32 magic;								   /* CLUSTER_RECOVERY_ANCHOR_MAGIC */
	uint16 version;								   /* CLUSTER_RECOVERY_ANCHOR_VERSION */
	uint16 _pad_6;								   /* explicit pad, always 0 */
	int32 node_id;								   /* owner; must equal cluster_node_id at read */
	uint32 state;								   /* DBState of THIS node's WAL thread */
	uint64 system_identifier;					   /* must match the shared authority's sysid */
	XLogRecPtr checkPoint;	   /* THIS node's last checkpoint record LSN */
	pg_time_t write_time;	   /* observational only */
	CheckPoint checkPointCopy; /* THIS node's full CheckPoint record copy */
	XLogRecPtr unloggedLSN;	   /* THIS node's unloggedLSN at last write; a
								* clean restart must restore its own fake-LSN
								* counter, not the last authority writer's
								* (L437 sweep: xlog.c unloggedLSN restore) */
	char _reserved[508 - 40 - 8 - sizeof(CheckPoint)]; /* zero pad to the CRC */
	pg_crc32c crc;			   /* CRC32C over [0, offsetof(crc)) */
} ClusterRecoveryAnchor;

StaticAssertDecl(offsetof(ClusterRecoveryAnchor, node_id) == 8, "recovery anchor node_id offset");
StaticAssertDecl(offsetof(ClusterRecoveryAnchor, state) == 12, "recovery anchor state offset");
StaticAssertDecl(offsetof(ClusterRecoveryAnchor, system_identifier) == 16,
				 "recovery anchor sysid offset");
StaticAssertDecl(offsetof(ClusterRecoveryAnchor, checkPoint) == 24,
				 "recovery anchor checkPoint offset");
StaticAssertDecl(offsetof(ClusterRecoveryAnchor, write_time) == 32,
				 "recovery anchor write_time offset");
StaticAssertDecl(offsetof(ClusterRecoveryAnchor, checkPointCopy) == 40,
				 "recovery anchor CheckPoint copy offset");
StaticAssertDecl(offsetof(ClusterRecoveryAnchor, unloggedLSN) == 128,
				 "recovery anchor unloggedLSN offset (locks sizeof(CheckPoint) too)");
StaticAssertDecl(offsetof(ClusterRecoveryAnchor, crc) == 508, "recovery anchor crc offset");
StaticAssertDecl(sizeof(ClusterRecoveryAnchor) == CLUSTER_RECOVERY_ANCHOR_SIZE,
				 "recovery anchor size");

/*
 * ClusterRecoveryAnchorValidity -- pure classification of one raw image.
 *
 *	VALID			size, CRC, magic/version and identity all check out.
 *	INVALID_SHORT	fewer than CLUSTER_RECOVERY_ANCHOR_SIZE bytes.
 *	INVALID_CRC		stored CRC does not match (torn or corrupt image).
 *	INVALID_MAGIC	CRC fine but magic or version is foreign.
 *	INVALID_IDENTITY	CRC fine but system_identifier or node_id differs
 *					from the expected one (foreign cluster or wrong node's
 *					anchor behind this path).
 *
 * Unlike the shared-authority classifier there is no "skip identity"
 * mode: every anchor consumer knows both the expected sysid (the shared
 * authority is already loaded) and its own node id, so the identity legs
 * are always enforced (fail-closed).
 */
typedef enum ClusterRecoveryAnchorValidity {
	CLUSTER_RA_VALID = 0,
	CLUSTER_RA_INVALID_SHORT,
	CLUSTER_RA_INVALID_CRC,
	CLUSTER_RA_INVALID_MAGIC,
	CLUSTER_RA_INVALID_IDENTITY
} ClusterRecoveryAnchorValidity;

/* Pure classifier (no I/O, no ereport) -- unit-tested in isolation. */
extern ClusterRecoveryAnchorValidity cluster_recovery_anchor_classify(const char *buf, size_t len,
																	  uint64 expected_sysid,
																	  int32 expected_node);

/*
 * Path accessors.  Each returns a pointer to a per-function static buffer
 * rebuilt from cluster_shared_data_dir + cluster_node_id on every call;
 * NULL when cluster_shared_data_dir is unset.
 */
extern const char *cluster_recovery_anchor_path(void);
extern const char *cluster_recovery_anchor_bak_path(void);

/*
 * Read this node's anchor into *out.  Tries the primary first, then the
 * .bak under the same strict classification (an anchor that is merely one
 * checkpoint stale is safe -- the WAL it points into has not been recycled
 * -- so no extra acceptance probe is needed beyond full validity).
 * Returns true and fills *out (setting *used_bak when the .bak was the
 * source) on success; returns false when the read must fail-closed.
 * Never ereports; the caller decides the error face (FATAL 53RB3).
 */
extern bool cluster_recovery_anchor_read(uint64 expected_sysid, ClusterRecoveryAnchor *out,
										 bool *used_bak);

/*
 * Atomically replace this node's anchor with *ra: recompute the CRC, roll
 * the live primary into .bak, then temp-write + durable_rename the new
 * image.  PANICs on any I/O failure -- a WARNING-and-continue anchor would
 * silently lose this node's recoverability once WAL recycling passes the
 * stale redo point.  Not subject to the shared-authority write gate
 * (write-skip / JOIN_READONLY): the anchor is a per-node file and must
 * keep advancing even while this node's shared-authority writes are
 * suppressed.
 */
extern void cluster_recovery_anchor_write(const ClusterRecoveryAnchor *ra);

/*
 * Build an anchor image from a ControlFileData snapshot (seed path: the
 * pre-migration local pg_control still carries this node's own values).
 */
extern void cluster_recovery_anchor_build_from_controlfile(const ControlFileData *cf,
														   ClusterRecoveryAnchor *out);

/*
 * Checkpoint-time publication (write hook #1): build the anchor from this
 * node's just-logged checkpoint and write it durably.  Runs inside the
 * CreateCheckPoint window after UpdateControlFile and before this cycle's
 * WAL recycling, so the anchor never points past recycled WAL.
 */
extern void cluster_recovery_anchor_publish_checkpoint(XLogRecPtr checkpoint_lsn,
													   const CheckPoint *checkpoint_copy,
													   uint64 sysid, uint32 state,
													   XLogRecPtr unlogged_lsn);

/*
 * Production state refresh (write hook #2): flip an existing valid
 * anchor's state (keeping its checkpoint fields) when this node enters
 * DB_IN_PRODUCTION, so a later crash is not misread as a clean shutdown.
 * A missing or invalid anchor is a no-op -- creation happens only at the
 * checkpoint hook and the seed path.  Returns true when the anchor was
 * refreshed.
 */
extern bool cluster_recovery_anchor_refresh_state(uint64 expected_sysid, uint32 state);

/*
 * Boot-time adoption (read side, startup process only): load and validate
 * this node's anchor once per boot into a process-local static.  Returns
 * false when no trustworthy anchor exists (caller raises FATAL 53RB3 on
 * the label-less restart path).  *used_bak as for _read.
 */
extern bool cluster_recovery_anchor_load(uint64 expected_sysid, bool *used_bak);
extern bool cluster_recovery_anchor_active(void);
extern const ClusterRecoveryAnchor *cluster_recovery_anchor_get(void);

#endif /* CLUSTER_RECOVERY_ANCHOR_H */
