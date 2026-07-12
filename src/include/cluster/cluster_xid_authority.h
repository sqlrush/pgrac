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
/*
 * Round-3b P0-4: an image carrying NATIVE_RAW_REUSED is written with THIS
 * magic instead.  A pre-barrier binary validates the magic but knows
 * nothing of the flag; on the old magic it would silently ignore the bit
 * and re-latch the native prehistory (wrong LOCAL verdicts while newer
 * nodes allocate epoch>=1 xids).  The stamped magic makes every such
 * binary FAIL the read outright and fail-close (bootstrap 53RB5 refuses
 * the boot; the coverage verify leaves the latch off).  Barrier-capable
 * binaries accept both magics; the one-way settle predicate additionally
 * requires the stamped magic, so a flag written under the old magic (a
 * pre-hardening test tree) is upgraded by the next re-assert.
 */
#define CLUSTER_XID_AUTHORITY_MAGIC_RAW_REUSED 0x0143D617
#define CLUSTER_XID_AUTHORITY_VERSION 1

/* A clean native-era shutdown published a complete hw + prehistory.
 * Cleared again -- only while CLUSTER_ERA is unset -- when a follow-up
 * native-era run boots, so a crash of that run leaves joiners
 * fail-closed instead of adopting the previous pass's stale hw. */
#define CLUSTER_XID_AUTHORITY_FLAG_SEALED 0x0001
/* A cluster.enabled=on boot closed the native era forever (one-way). */
#define CLUSTER_XID_AUTHORITY_FLAG_CLUSTER_ERA 0x0002
/*
 * The cluster is about to (or did) allocate the first epoch>=1 xid, so raw
 * 32-bit values below the native high-water are no longer alias-free
 * native-era identities (GCS-race round-3 P0-1).  One-way; stamped durably
 * BEFORE the first epoch carry by the LMON wrap barrier.  Once set, no boot
 * may latch the native-prehistory coverage again (below-hw recycled refs
 * stay fail-closed 53R97 forever) and every serving reader's latch is
 * cleared through the barrier broadcast before allocation proceeds.
 */
#define CLUSTER_XID_AUTHORITY_FLAG_NATIVE_RAW_REUSED 0x0004

/*
 * GCS-race round-4c P0-1 residual #2 — durable admission proof for the wrap
 * barrier's epoch-allocation gate.  The barrier coordinator stamps this
 * (under the authority flock, after RAW_REUSED settled in both copies)
 * ONLY once the full admission held: every conf-declared member connected,
 * advertising XID_AUTHORITY_FLOCK_V2, and ack'd latch-off.  The boot
 * shortcut (counter already past the native era) may only open the gate on
 * this flag: RAW_REUSED alone proves the stamp landed, not that the
 * admission round ever completed (a coordinator crash between stamp and
 * gate-open would otherwise let a reboot skip the distributed proof).
 * One-way; re-asserted by the LMON tick like the other one-way flags.
 */
#define CLUSTER_XID_AUTHORITY_FLAG_EPOCH_GATE_ADMITTED 0x0008

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
/* Round-3b P0-1: cross-node mutation lock file (flock).  Serializes every
 * authority read-modify-write across nodes; the kernel drops the lock on
 * any process exit, so a crash never leaves a stale lock behind. */
#define CLUSTER_XID_AUTHORITY_LOCK_REL_PATH "global/pgrac_xid_authority.lock"

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

/*
 * Round-3b RISK-1: runtime mapping of an adopted native CLOG status to a
 * prehistory verdict.  COMMITTED and ABORTED are literal; IN_PROGRESS is
 * terminal ONLY under the seal proof (a clean native shutdown sealed with
 * no prepared and no active xacts, so a below-hw in-progress bit is a
 * crash-aborted xact that can never resolve).  SUB_COMMITTED -- and any
 * value outside the 2-bit CLOG alphabet -- is NEVER trusted at runtime,
 * even though the boot verify refuses to latch on such bytes: the consumer
 * falls through fail-closed (53R97) instead of guessing (rule 8.A).
 *
 * The status values mirror the CLOG alphabet without dragging clog.h into
 * the pure layer (same pattern as cluster_xid_prehistory_payload_bytes);
 * cluster_xid_authority.c pins them to TRANSACTION_STATUS_* with
 * StaticAsserts.
 */
#define CLUSTER_NATIVE_CLOG_IN_PROGRESS 0x00
#define CLUSTER_NATIVE_CLOG_COMMITTED 0x01
#define CLUSTER_NATIVE_CLOG_ABORTED 0x02
#define CLUSTER_NATIVE_CLOG_SUB_COMMITTED 0x03

typedef enum ClusterNativePrehistoryVerdict {
	CLUSTER_NATIVE_PREHISTORY_COMMITTED = 0,
	CLUSTER_NATIVE_PREHISTORY_ABORTED,
	CLUSTER_NATIVE_PREHISTORY_UNRESOLVED
} ClusterNativePrehistoryVerdict;

static inline ClusterNativePrehistoryVerdict
cluster_native_prehistory_map_status(int native_status)
{
	switch (native_status) {
	case CLUSTER_NATIVE_CLOG_COMMITTED:
		return CLUSTER_NATIVE_PREHISTORY_COMMITTED;
	case CLUSTER_NATIVE_CLOG_ABORTED:
	case CLUSTER_NATIVE_CLOG_IN_PROGRESS:
		return CLUSTER_NATIVE_PREHISTORY_ABORTED;
	case CLUSTER_NATIVE_CLOG_SUB_COMMITTED:
	default:
		return CLUSTER_NATIVE_PREHISTORY_UNRESOLVED;
	}
}

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

/*
 * One-way: stamp CLUSTER_ERA (first cluster.enabled=on boot).  Idempotent
 * re-assert: rewrites BOTH copies until both carry the flag (repairing a
 * torn previous stamp); no-write no-op once both are stamped.
 */
extern void cluster_xid_authority_mark_cluster_era(void);

/*
 * One-way: stamp NATIVE_RAW_REUSED (LMON wrap barrier, strictly before the
 * cluster's first epoch>=1 xid allocation).  Same idempotent both-copies
 * re-assert discipline as the cluster-era stamp; PANICs on a missing or
 * corrupt authority (the bootstrap seeded it long before any allocator can
 * approach the epoch boundary).
 */
extern void cluster_xid_authority_mark_native_raw_reused(void);

/*
 * Round-3b P0-1: is the NATIVE_RAW_REUSED transition settled in BOTH
 * on-disk copies (flag present AND stamped magic)?  The wrap barrier
 * re-verifies this durable truth right before opening the allocation
 * gate -- shmem marked/done state alone never stands in for the disk.
 */
extern bool cluster_xid_authority_raw_reused_settled(void);

/*
 * GCS-race round-4c P0-1 residual #2 — EPOCH_GATE_ADMITTED one-way stamp +
 * settle probe (see the flag comment above).  mark also re-asserts
 * RAW_REUSED (admission implies the stamp);  settled requires BOTH flags in
 * BOTH copies under the stamped magic.
 */
extern void cluster_xid_authority_mark_epoch_gate_admitted(void);
extern bool cluster_xid_authority_epoch_gate_admitted_settled(void);

/*
 * A follow-up native-era (cluster.enabled=off) boot on a sealed authority
 * re-opens the era: clears SEALED so a crash of this run never exposes the
 * previous pass's stale high-water to joiners.  Caller vets CLUSTER_ERA
 * first (re-entry is FATAL).  Idempotent re-assert: rewrites BOTH copies
 * until neither retains SEALED (repairing a torn previous unseal);
 * no-write no-op once both are open.
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
 * Post-recovery coverage verify + latch (GCS-race round-2 RC-E).
 * ============================================================ */

/*
 * Pure judge: is `xid` provably a native-era transaction whose outcome the
 * local adopted CLOG answers alias-free?  True only when the coverage latch
 * is set (covered_hw_full != 0) and the value's full 64-bit identity --
 * widened in the signed +/- 2^31 window around next_full_xid via
 * cluster_xid_widen -- lies below the native high-water.  Widen failure and
 * every other doubt leg return false (resolver keeps 53R97).
 */
extern bool cluster_xid_native_prehistory_provable_full(uint64 next_full_xid,
														uint64 covered_hw_full, TransactionId xid);

/*
 * StartupXLOG-tail boot latch: prove local CLOG == sealed native prehistory
 * over [oldestXid, native_hw), repair replay-wiped holes through the SLRU,
 * then latch the covered high-water (cluster_cr_native_prehistory_latch).
 * FATALs on a terminal-vs-terminal contradiction (divergent lineage); every
 * skip leg leaves the latch unset (resolver stays fail-closed).
 */
extern void cluster_xid_prehistory_verify_native_coverage(void);

/* ============================================================
 * Pre-migrate xid epoch witness (round-2 review F3).
 * ============================================================
 *
 * Under cluster.controlfile_shared_authority the local global/pg_control
 * becomes a symlink to the SHARED authority, erasing the last local record
 * of this node's OWN nextFullXid.  The backup_label prehistory adopt needs
 * that value: a clone taken after an xid epoch rollover reuses pg_xact
 * positions below the native high-water for cluster-era xids, and adopting
 * native bits over them would corrupt live outcomes.  The witness persists
 * the pre-migration nextFullXid durably in the LOCAL data directory before
 * the symlink flip; magic "PGXW" + CRC32C, torn-safe via tmp+rename.
 */
#define CLUSTER_XID_EPOCH_WITNESS_REL_PATH "global/pgrac_xid_epoch_witness"
#define CLUSTER_XID_EPOCH_WITNESS_TMP_REL_PATH "global/pgrac_xid_epoch_witness.tmp"
#define CLUSTER_PGXW_MAGIC 0x50475857 /* "PGXW" */
#define CLUSTER_PGXW_VERSION 1

typedef struct ClusterXidEpochWitness {
	uint32 magic;		  /* CLUSTER_PGXW_MAGIC */
	uint32 version;		  /* CLUSTER_PGXW_VERSION */
	uint64 next_full_xid; /* this node's own pre-migration nextFullXid */
	uint32 crc;			  /* CRC32C over the fields above */
	uint32 pad;			  /* zero */
} ClusterXidEpochWitness;

/*
 * Durable write of the witness under local_pgdata (tmp + fsync + rename;
 * idempotent -- re-running the migrate arm rewrites the same value).
 * Returns false on any I/O failure (caller fails the migration closed).
 */
extern bool cluster_xid_epoch_witness_write(const char *local_pgdata, uint64 next_full_xid);

/*
 * Read the witness back; false when absent, short, or failing
 * magic/version/CRC validation (callers treat all of those as "no proof"
 * and fail closed).
 */
extern bool cluster_xid_epoch_witness_read(const char *local_pgdata, uint64 *next_full_xid);

/* ============================================================
 * Divergent-lineage guard (review F2; spec Q6 amendment).
 * ============================================================ */

typedef enum ClusterXidPrefixVerdict {
	CLUSTER_XID_PREFIX_CONSISTENT = 0, /* local prefix matches the blob */
	CLUSTER_XID_PREFIX_DIVERGED,	   /* local pg_xact contradicts the blob */
	CLUSTER_XID_PREFIX_UNAVAILABLE,	   /* no trustworthy blob to compare */
} ClusterXidPrefixVerdict;

/*
 * Compare the local pg_xact bytes covering xids [oldest_xid_full,
 * limit_xid_full) with the sealed prehistory blob at 2-bit (per-xact)
 * precision.  Bits below oldest_xid_full are frozen truth that CLOG never
 * consults again and that SimpleLruTruncate may already have removed
 * (whole segments); they are exempt whether the pages survive or not.  A
 * local page missing INSIDE the comparable range is fail-closed
 * UNAVAILABLE, never CONSISTENT: pg_xact covers [oldestXid, nextXid) on a
 * well-formed node, so a hole is an anomaly, and a front-truncated
 * divergent clone must not pass as a "shorter clone" (review r3-X2).
 * Callers FATAL on DIVERGED/UNAVAILABLE: a joiner whose own native-era
 * history contradicts the seed's is not a pre-seed lineage, and neither
 * skipping (trusting local bits) nor adopting (overwriting the joiner's
 * own outcomes) is sound for it.
 */
extern ClusterXidPrefixVerdict cluster_xid_prehistory_prefix_check(const char *local_pgdata,
																   uint64 native_hw_full,
																   uint64 limit_xid_full,
																   uint64 oldest_xid_full);

#endif /* CLUSTER_XID_AUTHORITY_H */
