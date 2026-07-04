/*-------------------------------------------------------------------------
 *
 * cluster_oid_lease.h
 *	  Shared OID authority + per-node OID lease (spec-6.14 D6).
 *
 *	  Under cluster.shared_catalog=on a single shared durable authority file
 *	  holds the cluster-wide OID high-water mark.  Each node leases a block of
 *	  cluster.oid_lease_size OIDs at a time (Oracle sequence CACHE semantics,
 *	  Q4-B), consumes them node-locally, and refills from the authority under
 *	  a cross-node X lock when the block is exhausted.  The authority file is
 *	  torn-safe (temp + durable_rename + .bak), mirroring cluster_cf_authority.
 *
 *	  Layers (mirrors cluster_sequence's pure / backend split):
 *	    - cluster_oid_lease.c: pure resid encoder, authority buffer
 *	      classification, and the lease-consume helper (standalone-linkable so
 *	      cluster_unit exercises them without the full backend), plus the
 *	      torn-safe authority file read/write (fd.c, unit-tested against a
 *	      temp dir like cluster_cf_authority).
 *	    - cluster_oid_lease_shmem.c: the per-node lease shmem region + refill
 *	      coordination (refill_in_progress + ConditionVariable) + the GES
 *	      singleton X lock wrapper + cluster_oid_lease_get_next.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_oid_lease.h
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-6.14-shared-catalog-single-authority.md (D6)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_OID_LEASE_H
#define CLUSTER_OID_LEASE_H

#include "c.h"

#include "cluster/cluster_grd.h"		/* ClusterResId */
#include "port/pg_crc32c.h"				/* pg_crc32c + CRC macros */

/*
 * CLUSTER_OID_RESID_TYPE -- OID-authority resource-id namespace marker for the
 * singleton cross-node X lock.  Above LOCKTAG_LAST_TYPE and distinct from the
 * other synthetic resid types (SQ 0xF0, CF 0xF1, HW 0xF2, DL 0xF3, TT 0xF4,
 * IR 0xF5, KO 0xF6).
 */
#define CLUSTER_OID_RESID_TYPE 0xF7

/* On-disk authority header magic + version. */
#define CLUSTER_OID_AUTHORITY_MAGIC   0x0140D617	/* "OID" authority tag     */
#define CLUSTER_OID_AUTHORITY_VERSION 1

/* Shared authority file paths, relative to cluster.shared_data_dir. */
#define CLUSTER_OID_AUTHORITY_REL_PATH     "global/pgrac_oid_authority"
#define CLUSTER_OID_AUTHORITY_BAK_REL_PATH "global/pgrac_oid_authority.bak"
#define CLUSTER_OID_AUTHORITY_TMP_REL_PATH "global/pgrac_oid_authority.tmp"
#define CLUSTER_OID_AUTHORITY_BAK_TMP_REL_PATH "global/pgrac_oid_authority.bak.tmp"

/*
 * ClusterOidAuthorityHeader -- the on-disk shared OID authority image.  A
 * single monotonically-advancing high-water mark; a node's lease is
 * [old high-water, old high-water + lease_size) and the file is bumped to the
 * new high-water under the cross-node X lock before the lease is handed out.
 */
typedef struct ClusterOidAuthorityHeader
{
	uint32		magic;			/* CLUSTER_OID_AUTHORITY_MAGIC             */
	uint32		version;		/* CLUSTER_OID_AUTHORITY_VERSION           */
	Oid			next_oid;		/* cluster-wide next unallocated OID       */
	uint32		reserved;		/* pad / future use; zero                  */
	pg_crc32c	crc;			/* CRC of all preceding bytes              */
} ClusterOidAuthorityHeader;

/*
 * ClusterOidAuthorityValidity -- classification of an authority image buffer.
 */
typedef enum ClusterOidAuthorityValidity
{
	CLUSTER_OID_AUTHORITY_VALID = 0,
	CLUSTER_OID_AUTHORITY_INVALID_SHORT,
	CLUSTER_OID_AUTHORITY_INVALID_MAGIC,
	CLUSTER_OID_AUTHORITY_INVALID_CRC
} ClusterOidAuthorityValidity;

/*
 * ClusterOidLease -- a per-node contiguous OID lease block.  next is the next
 * OID this node may hand out; end is the exclusive upper bound.  next == end
 * means the block is exhausted and a refill is required.
 */
typedef struct ClusterOidLease
{
	Oid			next;
	Oid			end;
} ClusterOidLease;

/* ---- pure layer (cluster_oid_lease.c) ---------------------------------- */

/*
 * cluster_oid_resid_encode -- build the singleton OID-authority resource id
 *	(all map fields zero; the type byte places it in the OID namespace).
 */
extern void cluster_oid_resid_encode(ClusterResId *dst);

/*
 * cluster_oid_authority_classify -- pure validity check of an authority image
 *	buffer of length len (short / bad magic / bad CRC / valid).
 */
extern ClusterOidAuthorityValidity
			cluster_oid_authority_classify(const char *buf, size_t len);

/*
 * cluster_oid_lease_normalize_start -- force an authority high-water up to
 *	FirstNormalObjectId when it has wrapped below the reserved range (mirrors
 *	the wraparound handling in the stock GetNewObjectId).  Pure.
 */
extern Oid	cluster_oid_lease_normalize_start(Oid start);

/*
 * cluster_oid_lease_consume -- hand out one OID from a lease, advancing it.
 *	Returns InvalidOid and leaves the lease untouched when it is exhausted
 *	(next == end).  refill (cluster_oid_lease_carve) guarantees the block
 *	never contains a reserved (< FirstNormalObjectId) OID, so consume is a
 *	plain next++ with unsigned wrap.  Pure.
 */
extern Oid	cluster_oid_lease_consume(ClusterOidLease *lease);

/*
 * cluster_oid_lease_carve -- pure refill math.  Given the current authority
 *	high-water hw and a lease size, produce the node's new lease block
 *	[*out_start, *out_end) and the value *out_new_authority to durably write
 *	back.  hw is first normalized up past the reserved range.  When the block
 *	would overflow the 32-bit OID space it is capped so it never hands out a
 *	reserved OID, and the authority is reset to FirstNormalObjectId for the
 *	next refill.  Leases carved from monotonically-advancing hw values are
 *	pairwise disjoint (spec-6.14 §3.3).  *out_end == 0 means the block runs to
 *	the top of the OID space (exclusive end wraps to 0).
 */
extern void cluster_oid_lease_carve(Oid hw, uint32 lease_size,
									Oid *out_start, Oid *out_end,
									Oid *out_new_authority);

/* ---- authority file I/O (cluster_oid_lease.c, backend) ----------------- */

/*
 * cluster_oid_authority_read -- read the shared OID high-water.  Returns true
 *	and sets *next_oid on success; returns false (fail-closed) when neither
 *	the primary nor the .bak image is trustworthy.  Never ereports.
 */
extern bool cluster_oid_authority_read(Oid *next_oid);

/*
 * cluster_oid_authority_write -- torn-safe write of a new high-water (temp +
 *	fsync + .bak roll + durable_rename).  Caller must hold the OID X lock.
 *	PANICs on I/O failure (mirrors cluster_cf_authority_write).
 */
extern void cluster_oid_authority_write(Oid next_oid);

/* ---- shmem lease + refill (cluster_oid_lease_shmem.c, backend) ---------- */

extern Size cluster_oid_lease_shmem_size(void);
extern void cluster_oid_lease_shmem_init(void);
extern void cluster_oid_lease_shmem_register(void);

/*
 * cluster_oid_lease_get_next -- allocate one cluster-wide-unique OID from this
 *	node's lease, refilling from the shared authority (cross-node X lock) when
 *	exhausted.  Fail-closed 53RB when the authority is unavailable; never
 *	falls back to the node-local counter.  Called from GetNewObjectId under
 *	shared_catalog=on.
 */
extern Oid	cluster_oid_lease_get_next(void);

/* Observability accessors (D10). */
extern uint64 cluster_oid_lease_acquire_count(void);
extern Oid	cluster_oid_lease_remaining(void);

#endif							/* CLUSTER_OID_LEASE_H */
