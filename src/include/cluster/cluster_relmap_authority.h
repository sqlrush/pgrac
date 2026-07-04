/*-------------------------------------------------------------------------
 *
 * cluster_relmap_authority.h
 *	  Shared relmapper (pg_filenode.map) single authority (spec-6.14 D5).
 *
 *	  Under cluster.shared_catalog=on the mapped-relation file-number map is a
 *	  single shared durable authority (per shared/per-db), replacing the
 *	  node-local pg_filenode.map (AD-001: no local persistent map).  The
 *	  authority is torn-safe (temp + durable_rename + .bak) like
 *	  cluster_cf_authority, and carries a two-phase generation:
 *
 *	    - Writers stage a new map as PENDING (durable) then, once their
 *	      transaction's commit record is durable, PUBLISH it (pending ->
 *	      committed) and only AFTER that broadcast the relmap invalidation
 *	      (INV-14-8).  Readers -- including cold reloads -- only ever adopt the
 *	      COMMITTED image (INV-14-7), so a reload triggered by the
 *	      invalidation necessarily reads the new committed map.
 *	    - The durable owner-identity header {owner_node, owner_xid,
 *	      owner_epoch, relmap_lsn} is the crash-arbitration SSOT (spec §2.1):
 *	      a writer that crashes after staging pending but before publishing is
 *	      arbitrated at the merged-recovery completion point by whether
 *	      owner_xid committed.  The header does NOT rely on xl_relmap_update
 *	      (which carries only {dbid,tsid,nbytes,data}).
 *
 *	  This header exposes the durable substrate (classify / read-committed /
 *	  write-pending / publish / read-header).  The write-path locking (relmap
 *	  global lock, D7), the relmapper.c hook activation, the publish-then-
 *	  invalidate broadcast (spec-2.39 ack) and the crash arbitration wiring are
 *	  the D5-activation half (Wave B) that consume these primitives.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_relmap_authority.h
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-6.14-shared-catalog-single-authority.md (D5, §2.1)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RELMAP_AUTHORITY_H
#define CLUSTER_RELMAP_AUTHORITY_H

#include "c.h"

#include "port/pg_crc32c.h"

/* Authority header magic + version. */
#define CLUSTER_RELMAP_AUTHORITY_MAGIC   0x0140DEA9	/* relmap authority tag */
#define CLUSTER_RELMAP_AUTHORITY_VERSION 1

/*
 * Maximum relmap image the authority stores in each slot.  The stock
 * RelMapFile is ~524 bytes (int32 magic + int32 num + 64 * RelMapping + crc);
 * we cap generously and validate the caller's length against it.  The image
 * bytes are opaque here: they carry RelMapFile's own internal CRC, which the
 * relmapper validates on load, so the authority header CRC only covers the
 * header itself.
 */
#define CLUSTER_RELMAP_IMAGE_MAX 1024

typedef enum ClusterRelmapAuthorityValidity
{
	CLUSTER_RELMAP_AUTHORITY_VALID = 0,
	CLUSTER_RELMAP_AUTHORITY_INVALID_SHORT,
	CLUSTER_RELMAP_AUTHORITY_INVALID_MAGIC,
	CLUSTER_RELMAP_AUTHORITY_INVALID_CRC,
	CLUSTER_RELMAP_AUTHORITY_INVALID_IMAGE_SIZE
} ClusterRelmapAuthorityValidity;

/*
 * ClusterRelmapAuthorityHeader -- durable authority header + crash-arbitration
 * SSOT (spec §2.1).  Laid out on disk as:
 *     [ ClusterRelmapAuthorityHeader ][ committed image ][ pending image ]
 * with each image occupying image_size bytes.
 */
typedef struct ClusterRelmapAuthorityHeader
{
	uint32		magic;
	uint32		version;
	uint64		committed_generation;	/* what every reader adopts   */
	uint64		pending_generation; /* 0 = no pending                 */
	Oid			dbid;			/* InvalidOid for the shared map        */
	uint8		shared_map;		/* 1 = shared (global) map, 0 = per-db  */
	uint8		pad0;
	int16		owner_node;		/* pending owner identity ...           */
	TransactionId owner_xid;
	uint64		owner_epoch;
	uint64		relmap_lsn;		/* XLOG_RELMAP_UPDATE in owner thread   */
	uint32		image_size;		/* bytes per image slot                 */
	uint32		pad1;
	pg_crc32c	crc;			/* CRC of the header (not the images)   */
} ClusterRelmapAuthorityHeader;

/*
 * ClusterRelmapOwner -- the pending owner identity a writer stamps.
 */
typedef struct ClusterRelmapOwner
{
	int16		owner_node;
	TransactionId owner_xid;
	uint64		owner_epoch;
	uint64		relmap_lsn;
} ClusterRelmapOwner;

/* ---- pure classification (standalone-linkable) ------------------------- */

/*
 * cluster_relmap_authority_classify -- validate an authority file buffer
 *	(header magic + CRC + image_size sanity).  len is the whole file length.
 */
extern ClusterRelmapAuthorityValidity
			cluster_relmap_authority_classify(const char *buf, size_t len);

/* ---- authority file I/O (backend) -------------------------------------- */

/*
 * cluster_relmap_authority_read_committed -- fail-closed read of the COMMITTED
 *	image (INV-14-7).  Copies up to image bytes into *map_out (which must be at
 *	least CLUSTER_RELMAP_IMAGE_MAX); sets *out_len to the image size.  Returns
 *	false when neither the primary nor .bak authority is trustworthy.  Never
 *	ereports.
 */
extern bool cluster_relmap_authority_read_committed(bool shared_map, Oid dbid,
													char *map_out, uint32 *out_len);

/*
 * cluster_relmap_authority_read_header -- read just the durable header (for
 *	crash arbitration).  Returns false when unavailable.
 */
extern bool cluster_relmap_authority_read_header(bool shared_map, Oid dbid,
												 ClusterRelmapAuthorityHeader *out);

/*
 * cluster_relmap_authority_write_pending -- stage a new map image as PENDING
 *	under the caller-held relmap X lock (Assert-checked in the activation
 *	layer).  Torn-safe.  The committed image is preserved; only the pending
 *	slot + header {pending_generation, owner} change.  This does NOT make the
 *	new map readable -- readers only ever adopt committed (INV-14-7).  On a
 *	first-ever write (no prior authority) the committed slot is seeded from the
 *	same image so a reader before the first publish still sees a valid map.
 */
extern void cluster_relmap_authority_write_pending(bool shared_map, Oid dbid,
												   const char *map_image, uint32 image_size,
												   uint64 pending_generation,
												   const ClusterRelmapOwner *owner);

/*
 * cluster_relmap_authority_publish -- flip pending -> committed (atomic,
 *	durable).  Runs in the owner backend's post-commit path once the commit
 *	record is durable; the cross-node relmap invalidation is broadcast
 *	strictly AFTER this returns (INV-14-8).  generation must match the staged
 *	pending_generation.  Failure here must never take the ordinary ERROR path
 *	(the owning xact is already committed); the activation layer retries and
 *	PANICs on exhaustion (r4-P1).
 */
extern void cluster_relmap_authority_publish(bool shared_map, Oid dbid,
											 uint64 generation);

#endif							/* CLUSTER_RELMAP_AUTHORITY_H */
