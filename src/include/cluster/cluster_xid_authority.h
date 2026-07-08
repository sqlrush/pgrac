/*-------------------------------------------------------------------------
 *
 * cluster_xid_authority.h
 *	  Shared XID authority + native-era prehistory (spec-6.15b D1/D2).
 *
 *	  Under cluster.shared_catalog the pre-formation ("native era",
 *	  cluster.enabled=off single-writer) xid consumption of the seed node is
 *	  invisible to every other node: a joiner's per-node recovery anchor was
 *	  seeded from its pre-seed clone-era control file, so its transaction
 *	  horizon (nextXid) stays below the seed's committed xids and MVCC
 *	  silently judges seed rows "in the future" (false-invisible), while its
 *	  local pg_xact carries no commit bits for them (poison-hint hazard on
 *	  the shared catalog pages).
 *
 *	  Two small durable files under cluster.shared_data_dir close this:
 *
 *	   global/pgrac_xid_authority    -- never-lowered native-era xid
 *	       high-water (first xid NOT consumed by the native era) plus two
 *	       one-way lifecycle flags: SEALED (a clean native-era shutdown
 *	       published a complete high-water + prehistory; joiners may adopt)
 *	       and CLUSTER_ERA (a cluster.enabled=on boot happened; the native
 *	       era is closed forever and may not be re-entered).
 *
 *	   global/pgrac_xid_prehistory   -- the raw local pg_xact page run
 *	       covering [0, native_hw), CRC-guarded.  A joiner whose own
 *	       nextXid proves it is a pre-seed lineage adopts these bytes into
 *	       its local pg_xact before StartupCLOG, giving it first-hand
 *	       commit-status truth for every native-era xid (no hint-bit
 *	       trust, no CLOG overlay of cluster-era foreign bits -- see the
 *	       spec's no-clog-overlay boundary note).
 *
 *	  Torn-safe file discipline mirrors cluster_oid_lease.c: temp + fsync +
 *	  .bak roll + durable_rename on write; primary-then-.bak fail-closed
 *	  read; ENOENT-only-absent presence probe.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_xid_authority.h
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-6.15b-xid-authority-native-era.md (D1/D2)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_XID_AUTHORITY_H
#define CLUSTER_XID_AUTHORITY_H

#include "port/pg_crc32c.h"

/* ============================================================
 * On-disk authority image (spec-6.15b §2).
 * ============================================================ */

#define CLUSTER_XID_AUTHORITY_MAGIC 0x0141D617
#define CLUSTER_XID_AUTHORITY_VERSION 1

/* A clean native-era shutdown published a complete hw + prehistory.
 * Cleared again -- only while CLUSTER_ERA is unset -- when a follow-up
 * native-era run boots, so a crash of that run leaves joiners
 * fail-closed instead of adopting the previous pass's stale hw. */
#define CLUSTER_XID_AUTHORITY_FLAG_SEALED 0x0001
/* A cluster.enabled=on boot closed the native era forever (one-way). */
#define CLUSTER_XID_AUTHORITY_FLAG_CLUSTER_ERA 0x0002

typedef struct ClusterXidAuthorityHeader {
	uint32 magic;
	uint32 version;
	uint32 flags;
	uint32 reserved;	   /* zero */
	uint64 native_hw_full; /* FullTransactionId value: first xid NOT
								 * consumed by the native era; never lowered */
	uint64 next_multi;	   /* native-era nextMulti at publish; vet-only
								 * (spec Q5: >FirstMultiXactId refuses seed) */
	pg_crc32c crc;		   /* over all preceding bytes */
} ClusterXidAuthorityHeader;

/* On-disk ABI locked at compile time (review F7; mirrors the recovery
 * anchor's precedent).  The unit truth table re-checks these at runtime. */
StaticAssertDecl(offsetof(ClusterXidAuthorityHeader, flags) == 8,
				 "xid authority header layout is on-disk ABI");
StaticAssertDecl(offsetof(ClusterXidAuthorityHeader, native_hw_full) == 16,
				 "xid authority header layout is on-disk ABI");
StaticAssertDecl(offsetof(ClusterXidAuthorityHeader, next_multi) == 24,
				 "xid authority header layout is on-disk ABI");
StaticAssertDecl(offsetof(ClusterXidAuthorityHeader, crc) == 32,
				 "xid authority header layout is on-disk ABI");
StaticAssertDecl(sizeof(ClusterXidAuthorityHeader) == 40,
				 "xid authority header size is on-disk ABI");

/* Relative paths under cluster.shared_data_dir. */
#define CLUSTER_XID_AUTHORITY_REL_PATH "global/pgrac_xid_authority"
#define CLUSTER_XID_AUTHORITY_BAK_REL_PATH "global/pgrac_xid_authority.bak"
#define CLUSTER_XID_AUTHORITY_TMP_REL_PATH "global/pgrac_xid_authority.tmp"
#define CLUSTER_XID_AUTHORITY_BAK_TMP_REL_PATH "global/pgrac_xid_authority.bak.tmp"

typedef enum ClusterXidAuthorityValidity {
	CLUSTER_XID_AUTHORITY_VALID = 0,
	CLUSTER_XID_AUTHORITY_INVALID_SHORT,
	CLUSTER_XID_AUTHORITY_INVALID_MAGIC,
	CLUSTER_XID_AUTHORITY_INVALID_CRC,
} ClusterXidAuthorityValidity;

/* ============================================================
 * On-disk prehistory image (spec-6.15b D2).
 * ============================================================ */

#define CLUSTER_XID_PREHISTORY_MAGIC 0x0142D617
#define CLUSTER_XID_PREHISTORY_VERSION 1

/*
 * Refusal cap on the native era (spec §3.4): 8M xids = 256 CLOG pages =
 * 2MB of payload.  A seed load that consumes more must be split or moved
 * in-protocol; the cap keeps the blob bounded and the adopt O(small).
 */
#define CLUSTER_XID_PREHISTORY_MAX_XID UINT64CONST(8388608)

typedef struct ClusterXidPrehistoryHeader {
	uint32 magic;
	uint32 version;
	uint64 native_hw_full; /* payload covers xids [0, native_hw_full) */
	uint32 payload_len;	   /* raw pg_xact bytes following this header */
	uint32 reserved;	   /* zero */
	pg_crc32c crc;		   /* over header fields above + payload */
} ClusterXidPrehistoryHeader;

StaticAssertDecl(offsetof(ClusterXidPrehistoryHeader, native_hw_full) == 8,
				 "xid prehistory header layout is on-disk ABI");
StaticAssertDecl(offsetof(ClusterXidPrehistoryHeader, payload_len) == 16,
				 "xid prehistory header layout is on-disk ABI");
StaticAssertDecl(offsetof(ClusterXidPrehistoryHeader, crc) == 24,
				 "xid prehistory header layout is on-disk ABI");
StaticAssertDecl(sizeof(ClusterXidPrehistoryHeader) == 32,
				 "xid prehistory header size is on-disk ABI");

#define CLUSTER_XID_PREHISTORY_REL_PATH "global/pgrac_xid_prehistory"
#define CLUSTER_XID_PREHISTORY_BAK_REL_PATH "global/pgrac_xid_prehistory.bak"
#define CLUSTER_XID_PREHISTORY_TMP_REL_PATH "global/pgrac_xid_prehistory.tmp"
#define CLUSTER_XID_PREHISTORY_BAK_TMP_REL_PATH "global/pgrac_xid_prehistory.bak.tmp"

/* ============================================================
 * Pure layer (standalone-linkable; exercised by cluster_unit).
 * ============================================================ */

extern ClusterXidAuthorityValidity cluster_xid_authority_classify(const char *buf, size_t len);

/*
 * cluster_xid_prehistory_payload_bytes -- whole-CLOG-page payload size
 * covering xids [0, native_hw_full).  0 when native_hw_full is 0 (nothing
 * to carry); the per-page constant mirrors CLOG_XACTS_PER_PAGE without
 * dragging clog.c internals into the unit build.
 */
extern uint32 cluster_xid_prehistory_payload_bytes(uint64 native_hw_full);

extern ClusterXidAuthorityValidity cluster_xid_prehistory_classify(const char *buf, size_t len);

/* ============================================================
 * Torn-safe authority file I/O (mirrors cluster_oid_lease.c).
 * ============================================================ */

/*
 * Fail-closed read: primary then .bak; false when neither validates.
 * Never ereports (safe on the bootstrap early-read path).
 */
extern bool cluster_xid_authority_read(ClusterXidAuthorityHeader *out);

/* As read(), reporting .bak fallback for a consumer-side LOG (review F8). */
extern bool cluster_xid_authority_read_checked(ClusterXidAuthorityHeader *out, bool *used_bak);

/*
 * ENOENT-only-absent presence probe (spec-6.14 §3.6 posture): any stat()
 * failure other than ENOENT reports present, so a transiently unreadable
 * authority is never re-seeded over.
 */
extern bool cluster_xid_authority_present(void);

/*
 * Seed the authority (unsealed) when neither image exists.  Returns true
 * when this call created it.  A trustworthy existing image is a no-op.
 */
extern bool cluster_xid_authority_seed_if_absent(uint64 initial_native_hw);

/*
 * Monotone raise of native_hw_full + set SEALED (never lowers, never
 * clears flags).  seal=false publishes a crash-safe interim high-water
 * during the native era without opening adoption.  PANICs on I/O error.
 */
extern void cluster_xid_authority_publish_native(uint64 native_hw_full, uint64 next_multi,
												 bool seal);

/* One-way: stamp CLUSTER_ERA (first cluster.enabled=on boot). */
extern void cluster_xid_authority_mark_cluster_era(void);

/*
 * A follow-up native-era (cluster.enabled=off) boot on a sealed authority
 * re-opens the era: clears SEALED so a crash of this run never exposes the
 * previous pass's stale high-water to joiners.  Caller vets CLUSTER_ERA
 * first (re-entry is FATAL); no-op when already unsealed.
 */
extern void cluster_xid_authority_begin_native_run(void);

/* ============================================================
 * Prehistory publish / adopt (spec-6.15b D2; P2: adopt is complete and
 * fsynced strictly before StartupCLOG -- both run under the postmaster /
 * shutdown-checkpoint single-writer windows, file-level only).
 * ============================================================ */

/*
 * Seed side: snapshot local pg_xact pages [0, native_hw_full) into the
 * shared prehistory file (torn-safe).  PANICs on I/O error; FATALs when
 * the native era exceeds CLUSTER_XID_PREHISTORY_MAX_XID.
 */
extern void cluster_xid_prehistory_publish(const char *local_pgdata, uint64 native_hw_full);

/*
 * Joiner side: decode the shared prehistory into local pg_xact page files,
 * pg_fsync each touched segment and the pg_xact directory before returning.
 * Idempotent (same-byte overwrite).  FATALs (53RB5) on a missing/corrupt
 * blob or a native_hw mismatch against the sealed authority.
 */
extern void cluster_xid_prehistory_adopt(const char *local_pgdata, uint64 native_hw_full);
extern bool cluster_xid_prehistory_was_adopted(void);

/* ============================================================
 * Divergent-lineage guard (review F2; spec Q6 amendment).
 * ============================================================ */

typedef enum ClusterXidPrefixVerdict {
	CLUSTER_XID_PREFIX_CONSISTENT = 0, /* local prefix matches the blob */
	CLUSTER_XID_PREFIX_DIVERGED,	   /* local pg_xact contradicts the blob */
	CLUSTER_XID_PREFIX_UNAVAILABLE,	   /* no trustworthy blob to compare */
} ClusterXidPrefixVerdict;

/*
 * Compare the local pg_xact bytes covering xids [0, limit_xid_full) with the
 * sealed prehistory blob at 2-bit (per-xact) precision.  A local segment or
 * page that does not exist ends the comparable prefix (a shorter clone has
 * no bits to contradict).  Callers FATAL on DIVERGED/UNAVAILABLE: a joiner
 * whose own native-era history contradicts the seed's is not a pre-seed
 * lineage, and neither skipping (trusting local bits) nor adopting
 * (overwriting the joiner's own outcomes) is sound for it.
 */
extern ClusterXidPrefixVerdict cluster_xid_prehistory_prefix_check(const char *local_pgdata,
																   uint64 native_hw_full,
																   uint64 limit_xid_full);

#endif /* CLUSTER_XID_AUTHORITY_H */
