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

#include "cluster/cluster_grd.h" /* ClusterResId */
#include "port/pg_crc32c.h"		 /* pg_crc32c + CRC macros */

/*
 * CLUSTER_OID_RESID_TYPE -- OID-authority resource-id namespace marker for the
 * singleton cross-node X lock.  Above LOCKTAG_LAST_TYPE and distinct from the
 * other synthetic resid types (SQ 0xF0, CF 0xF1, HW 0xF2, DL 0xF3, TT 0xF4,
 * IR 0xF5, KO 0xF6).
 */
#define CLUSTER_OID_RESID_TYPE 0xF7

/* On-disk authority header magic + version. */
#define CLUSTER_OID_AUTHORITY_MAGIC 0x0140D617 /* "OID" authority tag     */
#define CLUSTER_OID_AUTHORITY_VERSION 1

/* Shared authority file paths, relative to cluster.shared_data_dir. */
#define CLUSTER_OID_AUTHORITY_REL_PATH "global/pgrac_oid_authority"
#define CLUSTER_OID_AUTHORITY_BAK_REL_PATH "global/pgrac_oid_authority.bak"
#define CLUSTER_OID_AUTHORITY_TMP_REL_PATH "global/pgrac_oid_authority.tmp"
#define CLUSTER_OID_AUTHORITY_BAK_TMP_REL_PATH "global/pgrac_oid_authority.bak.tmp"

/*
 * ClusterOidAuthorityHeader -- the on-disk shared OID authority image.  A
 * single next-candidate pointer; a node's lease is capped at the top of the
 * OID space and the pointer then wraps to FirstNormalObjectId.  The file is
 * advanced under the cross-node X lock before the lease is handed out.
 */
typedef struct ClusterOidAuthorityHeader {
	uint32 magic;	 /* CLUSTER_OID_AUTHORITY_MAGIC             */
	uint32 version;	 /* CLUSTER_OID_AUTHORITY_VERSION           */
	Oid next_oid;	 /* next candidate; zero = legacy cycle end */
	uint32 reserved; /* pad / future use; zero                  */
	pg_crc32c crc;	 /* CRC of all preceding bytes              */
} ClusterOidAuthorityHeader;

/*
 * ClusterOidAuthorityValidity -- classification of an authority image buffer.
 */
typedef enum ClusterOidAuthorityValidity {
	CLUSTER_OID_AUTHORITY_VALID = 0,
	CLUSTER_OID_AUTHORITY_INVALID_SHORT,
	CLUSTER_OID_AUTHORITY_INVALID_MAGIC,
	CLUSTER_OID_AUTHORITY_INVALID_CRC,
	CLUSTER_OID_AUTHORITY_INVALID_VERSION,
	CLUSTER_OID_AUTHORITY_INVALID_STATE
} ClusterOidAuthorityValidity;

/*
 * ClusterOidLease -- a per-node contiguous OID lease block.  next is the next
 * OID this node may hand out; end is the exclusive upper bound.  next == end
 * means the block is exhausted and a refill is required.
 */
typedef struct ClusterOidLease {
	Oid next;
	Oid end;
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
extern ClusterOidAuthorityValidity cluster_oid_authority_classify(const char *buf, size_t len);

/*
 * cluster_oid_lease_normalize_start -- normalize only a new seed's starting
 *	OID above the reserved range.  Never apply to a running authority.  Pure.
 */
extern Oid cluster_oid_lease_normalize_start(Oid start);

/*
 * cluster_oid_lease_consume -- hand out one OID from a lease, advancing it.
 *	Returns InvalidOid and leaves the lease untouched when it is exhausted
 *	(next == end).  refill (cluster_oid_lease_carve) guarantees the block
 *	never contains a reserved (< FirstNormalObjectId) OID, so consume is a
 *	plain next++ with unsigned wrap.  Pure.
 */
extern Oid cluster_oid_lease_consume(ClusterOidLease *lease);

/*
 * cluster_oid_lease_carve -- pure refill math.  Given the current authority
 *	high-water hw and a lease size, produce the node's new lease block
 *	[*out_start, *out_end) and the value *out_new_authority to durably write
 *	back.  The final block is capped at the top of the OID space and writes
 *	FirstNormalObjectId as the next cycle's start.  A validated legacy zero
 *	authority likewise starts a new cycle.  Other reserved hw or a zero size
 *	returns an empty [0,0) lease.  *out_end == 0 on a nonempty lease means the
 *	exclusive end is the top of the OID space.  Candidates can repeat across
 *	cycles, including ones held in another node's old lease; callers must
 *	still check catalog uniqueness and file conflicts.
 */
extern void cluster_oid_lease_carve(Oid hw, uint32 lease_size, Oid *out_start, Oid *out_end,
									Oid *out_new_authority);

/* ---- authority file I/O (cluster_oid_lease.c, backend) ----------------- */

/*
 * cluster_oid_authority_read -- read the shared OID high-water.  Returns true
 *	and sets *next_oid on success; returns false when the primary is not
 *	trustworthy.  The older .bak is never an allocation fallback.  Never ereports.
 */
extern bool cluster_oid_authority_read(Oid *next_oid);

/*
 * cluster_oid_authority_present -- does an authority image (primary or .bak)
 *	exist on disk, trustworthy or not?  Combined with a failed _read this
 *	distinguishes "absent: first seed" from "present but corrupt: fail-closed"
 *	(spec-6.14 §3.6).  Never ereports.
 */
extern bool cluster_oid_authority_present(void);

/*
 * cluster_oid_authority_write -- torn-safe write of a new high-water (temp +
 *	fsync + .bak roll + durable_rename).  Caller must hold the OID X lock.
 *	PANICs on I/O failure (mirrors cluster_cf_authority_write).
 */
extern void cluster_oid_authority_write(Oid next_oid);

/*
 * cluster_oid_authority_seed_if_absent -- create the shared OID authority with
 *	an initial high-water of max(FirstNormalObjectId, initial_next_oid) IF it
 *	does not already exist (D2 seed node).  A join node whose authority already
 *	exists is a no-op (returns false).  Returns true when it seeded.  Idempotent
 *	for the normal seed-then-join bring-up (a designated seed node comes up
 *	first).  Never lowers an existing high-water.  An unreadable existing
 *	authority raises an error even if a caller's earlier read succeeded.
 */
extern bool cluster_oid_authority_seed_if_absent(Oid initial_next_oid);

/* ---- shmem lease + refill (cluster_oid_lease_shmem.c, backend) ---------- */

extern Size cluster_oid_lease_shmem_size(void);
extern void cluster_oid_lease_shmem_init(void);
extern void cluster_oid_lease_shmem_register(void);

/*
 * cluster_oid_lease_get_next -- allocate one OID candidate from this
 *	node's lease, refilling from the shared authority (cross-node X lock) when
 *	exhausted.  Fail-closed 53RB when the authority is unavailable; never
 *	falls back to the node-local counter.  Called from GetNewObjectId under
 *	shared_catalog=on.
 */
extern Oid cluster_oid_lease_get_next(void);

/* Observability accessors (D10). */
extern uint64 cluster_oid_lease_acquire_count(void);
extern Oid cluster_oid_lease_remaining(void);

#endif /* CLUSTER_OID_LEASE_H */
