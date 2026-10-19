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

#include "common/cluster_relmap_v2.h"
#include "port/pg_crc32c.h"
#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_relmap_pending.h"
#endif

/* Authority header magic + version. */
#define CLUSTER_RELMAP_AUTHORITY_MAGIC 0x0140DEA9 /* relmap authority tag */
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

typedef enum ClusterRelmapAuthorityValidity {
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
 * Each slot occupies CLUSTER_RELMAP_IMAGE_MAX bytes; image_size is its valid
 * image prefix length.
 */
typedef struct ClusterRelmapAuthorityHeader {
	uint32 magic;
	uint32 version;
	uint64 committed_generation; /* what every reader adopts   */
	uint64 pending_generation;	 /* 0 = no pending                 */
	Oid dbid;					 /* InvalidOid for the shared map        */
	uint8 shared_map;			 /* 1 = shared (global) map, 0 = per-db  */
	uint8 pad0;
	int16 owner_node; /* pending owner identity ...           */
	TransactionId owner_xid;
	uint64 owner_epoch;
	uint64 relmap_lsn; /* XLOG_RELMAP_UPDATE in owner thread   */
	uint32 image_size; /* bytes per image slot                 */
	uint32 pad1;
	pg_crc32c crc; /* CRC of the header (not the images)   */
} ClusterRelmapAuthorityHeader;

/* Fixed slot widths are independent of the inner image_size. */
#define CLUSTER_RELMAP_AUTHORITY_FILE_SIZE                                                         \
	((int)(sizeof(ClusterRelmapAuthorityHeader) + 2 * CLUSTER_RELMAP_IMAGE_MAX))
#define CLUSTER_RELMAP_COMMITTED_OFFSET ((int)sizeof(ClusterRelmapAuthorityHeader))
#define CLUSTER_RELMAP_PENDING_OFFSET (CLUSTER_RELMAP_COMMITTED_OFFSET + CLUSTER_RELMAP_IMAGE_MAX)

typedef enum ClusterRelmapInitResult {
	CLUSTER_RELMAP_INIT_OK = 0,
	CLUSTER_RELMAP_INIT_INVALID_ARGUMENT,
	CLUSTER_RELMAP_INIT_INVALID_LENGTH,
	CLUSTER_RELMAP_INIT_INVALID_MAGIC,
	CLUSTER_RELMAP_INIT_INVALID_CRC,
	CLUSTER_RELMAP_INIT_INVALID_COUNT,
	CLUSTER_RELMAP_INIT_INVALID_ENTRY,
	CLUSTER_RELMAP_INIT_DUPLICATE,
	CLUSTER_RELMAP_INIT_NAMESPACE_MISMATCH,
	CLUSTER_RELMAP_INIT_OUTPUT_NOT_EMPTY
} ClusterRelmapInitResult;

/*
 * Pure frontend/backend fresh-init helper; no I/O or allocation.  Validates
 * exactly one native initdb map and builds the existing authority layout at
 * committed generation 1 with no pending owner.  Accepts only the original
 * bootstrap catalog set and original file numbers, not rewritten maps.
 *
 * shared_map requires dbid == InvalidOid; a local map requires a valid dbid.
 * Native maps do not encode dbid or freshness: the original creating process
 * must bind the input to its fresh directory and create the output file with
 * O_EXCL, fsync/read it back, and publish startup authority last.  This helper
 * cannot authenticate a directory or adopt an existing database.
 *
 * output_len must equal CLUSTER_RELMAP_AUTHORITY_FILE_SIZE and every output
 * byte must initially be zero.  Failure leaves output entirely unchanged.
 * Input and output may be unaligned.  No fallback, repair or partial image.
 */
extern ClusterRelmapInitResult cluster_relmap_authority_init_image(bool shared_map, Oid dbid,
																   const void *native_map,
																   size_t native_len, void *output,
																   size_t output_len);

/*
 * ClusterRelmapOwner -- the pending owner identity a writer stamps.
 */
typedef struct ClusterRelmapOwner {
	int16 owner_node;
	TransactionId owner_xid;
	uint64 owner_epoch;
	uint64 relmap_lsn;
} ClusterRelmapOwner;

/* ---- pure classification (standalone-linkable) ------------------------- */

/*
 * cluster_relmap_authority_classify -- validate an authority file buffer
 *	(header magic + CRC + image_size sanity).  len is the whole file length.
 */
extern ClusterRelmapAuthorityValidity cluster_relmap_authority_classify(const char *buf,
																		size_t len);

/* ---- authority file I/O (backend) -------------------------------------- */

/*
 * cluster_relmap_authority_read_committed -- fail-closed read of the COMMITTED
 *	image (INV-14-7).  Copies up to image bytes into *map_out (which must be at
 *	least CLUSTER_RELMAP_IMAGE_MAX); sets *out_len to the image size.  Returns
 *	false when neither the primary nor .bak authority is trustworthy.  Never
 *	ereports.
 */
extern bool cluster_relmap_authority_read_committed(bool shared_map, Oid dbid, char *map_out,
													uint32 *out_len);

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
extern void cluster_relmap_authority_write_pending(bool shared_map, Oid dbid, const char *map_image,
												   uint32 image_size, uint64 pending_generation,
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
extern void cluster_relmap_authority_publish(bool shared_map, Oid dbid, uint64 generation);

/*
 * cluster_relmap_authority_discard_pending -- drop a staged pending image
 *	(atomic, durable; the committed image is untouched).  Used when the
 *	staging transaction aborts before its commit record (the pending is then
 *	dead) and by crash arbitration when the owner is proven aborted.
 *	Idempotent: a non-matching pending_generation (already discarded or
 *	published) is a no-op.  An unreadable authority LOGs and returns -- the
 *	unresolved pending then keeps relmap writes fail-closed (53RB), which is
 *	the honest posture; this function is called from abort paths and must
 *	not throw.
 */
extern void cluster_relmap_authority_discard_pending(bool shared_map, Oid dbid, uint64 generation);

/*
 * Fresh v2 image only: same native bootstrap inventory and no-partial-output
 * contract as init_image, with a nonzero original system/DB incarnation.
 * Produces generation 1 committed, no pending. Output must be exactly 2176
 * zero bytes. Does not authenticate freshness, identity provenance or startup
 * permission. The original creator owns O_EXCL creation of BOTH synced copies.
 */
extern ClusterRelmapInitResult
cluster_relmap_authority_v2_init_image(const ClusterRelmapV2Namespace *identity,
									 const void *native_map, size_t native_len,
									 void *output, size_t output_len);

/*
 * V2 storage primitives; not wired into the live v1 publisher.  The full-pair
 * read is a raw substrate for exclusively owned, unpublished cold BUILDING
 * restore and exposes committed data and pending obligations separately; it
 * does not establish runtime serialization by itself.  Published runtime
 * readers must use the committed-only facade below, which checks the caller's
 * namespace relmap X lock.  A degraded read never proves the pending set
 * complete.
 */
extern ClusterRelmapV2PairResult
cluster_relmap_authority_v2_read(const ClusterRelmapV2Namespace *expected,
								ClusterRelmapV2View *output);

/*
 * Dormant committed-only v2 reader facade.  The caller must hold the
 * namespace relmap X lock for the entire call; the entry check prevents a
 * runtime reader from racing a publication or terminal cleanup.  A complete,
 * identical pair is required; on success only the committed image and
 * generation are copied to the outputs.  Pending obligations are intentionally
 * not exposed through this API, while cold BUILDING restore callers that need
 * them must use the full v2 read above.  A degraded or incomplete pair clears
 * both outputs and returns false.  There is no live v2 caller or activation
 * through this primitive.
 */
extern bool cluster_relmap_authority_v2_read_committed(const ClusterRelmapV2Namespace *expected,
												RelMapFile *map_out,
												uint64 *generation_out);

#ifdef USE_PGRAC_CLUSTER
/*
 * Dormant cold-reader wiring.  Read one complete, identical v2 pair and
 * rebuild structural DROP/TRUNCATE obligations from the caller's exact cold
 * stream.  Only the committed RelMapFile and generation are returned; the
 * durable v2 pending image is never exposed as a structural directory.  The
 * pending destination must already be initialized and empty.  A degraded,
 * incomplete, or wrong-namespace pair returns a fail-closed result without
 * changing the destination or committed outputs.  This is reader/rebuild
 * substrate only: it does not authorize CLOG/CTRC/SPACE release, issuance,
 * or live v2 activation.
 */
extern ClusterRelmapPendingResult
cluster_relmap_authority_v2_read_rebuild_pending(
	const ClusterRelmapV2Namespace *expected,
	const ClusterRelmapPendingDescriptor *descriptors, size_t descriptor_count,
	ClusterRelmapPendingSet *pending_out, RelMapFile *committed_out,
	uint64 *generation_out);
#endif

/*
 * Stage into an initialized, clean v2 pair, primary before backup, with both
 * copies synced and read back before returning true.  Never initializes or
 * repairs a missing/foreign/degraded authority, nor replaces an old pending.
 * The original owner must sample its real FullXid/admission and register
 * retention_begin BEFORE WAL. Pass that original-origin directory and the
 * SAME lock used for all access to its published state, not held on entry.
 * After read-only preflight this function marks the exact pin may_be_staged
 * under that lock, releases it, then writes. Missing/unknown/mismatching pins
 * cannot stage. The namespace X lock is required and serializes the whole call
 * with terminal cleanup and generation replacement. No disk I/O runs under
 * retention_lock.
 *
 * This does not authenticate a complete census, producer provenance, WAL
 * durability or TT/claim retention. False may leave durable pending in either
 * copy: retain the obligation and propagate the original failure; it is not
 * commit/release permission. No caller may infer cancellation merely from a
 * false return, even when the first write failed. There is no live v2 caller.
 */
struct ClusterRelmapRetentionDirectory;
struct LWLock;

extern bool cluster_relmap_authority_v2_write_pending(const ClusterRelmapV2Namespace *expected,
													uint64 generation, const ClusterRelmapV2Owner *owner,
													const RelMapFile *pending,
													struct ClusterRelmapRetentionDirectory *retention,
													struct LWLock *retention_lock);

typedef enum ClusterRelmapV2ClearResult {
	CLUSTER_RELMAP_V2_CLEARED_POSTREAD,
	CLUSTER_RELMAP_V2_RETAINED_UNKNOWN
} ClusterRelmapV2ClearResult;

/*
 * Finish exactly the original staged image under the same namespace X lock,
 * which is checked at entry.
 * The caller must independently prove its owner's matching commit/abort; this
 * storage primitive supplies no terminal authority and never releases a pin.
 * Both copies must be valid and match the original stage or its legal clean
 * cuts.  CLEARED_POSTREAD requires both final clean copies to be written,
 * synced and read back in this call, including an already-clean retry.
 *
 * Any I/O/sync/close failure returns RETAINED_UNKNOWN with no internal retry,
 * even if both names now read clean.  Retain the original obligation and PG
 * failure polarity (postcommit fail-stop); a later successful call cannot
 * erase that failure.  Missing/degraded/foreign copies are never repaired.
 */
extern ClusterRelmapV2ClearResult
cluster_relmap_authority_v2_publish(const ClusterRelmapV2Image *staged);
extern ClusterRelmapV2ClearResult
cluster_relmap_authority_v2_discard_pending(const ClusterRelmapV2Image *staged);

#endif /* CLUSTER_RELMAP_AUTHORITY_H */
