/*-------------------------------------------------------------------------
 *
 * cluster_wal_thread.h
 *	  pgrac per-thread WAL routing: thread identity, claim file, startup
 *	  validation (spec-4.1).
 *
 *	  Activates the spec-1.19 xlp_thread_id placeholder: this node's WAL
 *	  pages are stamped with thread_id = cluster.node_id + 1 (real range
 *	  [1, CLUSTER_WAL_THREAD_MAX]; 0 stays permanently legacy), and the
 *	  WAL stream may be routed to a per-thread directory on shared
 *	  storage (<cluster.wal_threads_dir>/thread_<id>/) via PG-native
 *	  initdb -X / symlink relocation managed by pgrac-init.  The engine
 *	  contributes startup validation (directory identity + thread claim
 *	  file) and the page-header stamp; the hot write path is otherwise
 *	  untouched (AD-009: no cross-node WAL write coordination).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_wal_thread.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-4.1-per-thread-wal-routing.md FROZEN v1.0
 *	  Design: docs/wal-record-format-design.md §4.2, feature-034, AD-009
 *
 *	  The claim struct + pure helpers are frontend-visible POD (unit
 *	  tests, future frontend tooling); everything that touches GUC
 *	  globals, shmem or ereport is backend-only under #ifndef FRONTEND.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_WAL_THREAD_H
#define CLUSTER_WAL_THREAD_H

#include "cluster/cluster_xlog.h" /* CLUSTER_WAL_THREAD_MAX + XLP_* sentinels */

/*
 * Thread claim file -- "<wal_threads_dir>/thread_<id>/pgrac_thread.claim".
 *
 *	Written ONCE on the first validated startup of the owning node
 *	(O_EXCL create + fsync file + fsync parent directory), then only
 *	ever read back.  Guards a thread directory against a DIFFERENT
 *	node writing into it (spec-4.1 Q5-A).  Never rewritten and never
 *	auto-rebuilt; a corrupt or foreign claim is a 53RA1 FATAL with a
 *	manual-confirmation errhint.
 *
 *	On-disk layout (40 bytes, all fields little-endian native; the file
 *	never crosses platforms by itself -- it lives next to the WAL
 *	segments whose byte order is already platform-native):
 *
 *	  offset  type      field
 *	       0  uint32    magic        0x50475443 ("PGTC")
 *	       4  uint16    version      CLUSTER_WAL_THREAD_CLAIM_VERSION
 *	       6  uint16    thread_id    1..CLUSTER_WAL_THREAD_MAX
 *	       8  int32     node_id      0..127
 *	      12  char[4]   _pad_12      zero
 *	      16  int64     created_at   TimestampTz of first claim
 *	      24  char[8]   _reserved_24 zero (spec-4.2 registry headroom)
 *	      32  uint32    crc          pg_crc32c over bytes [0,32)
 *	      36  char[4]   _pad_36      zero
 */
#define CLUSTER_WAL_THREAD_CLAIM_FILENAME "pgrac_thread.claim"
#define CLUSTER_WAL_THREAD_CLAIM_MAGIC ((uint32)0x50475443) /* "PGTC" */
#define CLUSTER_WAL_THREAD_CLAIM_VERSION ((uint16)1)

typedef struct ClusterWalThreadClaim {
	uint32 magic;
	uint16 version;
	uint16 thread_id;
	int32 node_id;
	char _pad_12[4];
	int64 created_at;
	char _reserved_24[8];
	uint32 crc;
	char _pad_36[4];
} ClusterWalThreadClaim;

StaticAssertDecl(sizeof(ClusterWalThreadClaim) == 40,
				 "spec-4.1 §2.3 claim layout: 40 bytes, crc at offset 32");
StaticAssertDecl(offsetof(ClusterWalThreadClaim, crc) == 32,
				 "spec-4.1 §2.3 claim layout: crc covers bytes [0,32)");

/*
 * Pure helpers (no GUC / shmem / elog dependencies).  Implemented as
 * static inline so cluster_unit covers them by including this header
 * alone (libpgcommon + libpgport for the CRC; no module .o, no stubs --
 * the test_cluster_undo_record linkage pattern) and frontend tooling
 * can reuse them later without a cluster object dependency.
 */
#include <stdio.h>			/* snprintf */
#include "port/pg_crc32c.h" /* COMP_CRC32C; frontend-safe */

/*
 * cluster_wal_thread_id_for -- pure identity mapping.
 *
 *	(enabled, node_id) -> xlp_thread_id stamp value.  Returns the
 *	legacy sentinel for non-cluster configurations (enabled=false or
 *	node_id < 0), else node_id + 1 (spec-1.19 Q2: zero is permanently
 *	legacy; node 0..127 maps to thread 1..128).
 */
static inline uint16
cluster_wal_thread_id_for(bool enabled, int node_id)
{
	if (!enabled || node_id < 0)
		return XLP_THREAD_ID_LEGACY;
	return (uint16)(node_id + 1);
}

/* Compute the claim CRC: pg_crc32c over bytes [0, offsetof(crc)). */
static inline uint32
cluster_wal_thread_claim_crc(const ClusterWalThreadClaim *claim)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, claim, offsetof(ClusterWalThreadClaim, crc));
	FIN_CRC32C(crc);
	return (uint32)crc;
}

/* Fill *claim (including crc) for the given identity. */
static inline void
cluster_wal_thread_claim_fill(ClusterWalThreadClaim *claim, uint16 thread_id, int32 node_id,
							  int64 created_at)
{
	memset(claim, 0, sizeof(*claim));
	claim->magic = CLUSTER_WAL_THREAD_CLAIM_MAGIC;
	claim->version = CLUSTER_WAL_THREAD_CLAIM_VERSION;
	claim->thread_id = thread_id;
	claim->node_id = node_id;
	claim->created_at = created_at;
	claim->crc = cluster_wal_thread_claim_crc(claim);
}

/*
 * Validate *claim against the expected identity.  Returns true when the
 * claim is well-formed (magic / version / crc) AND matches both
 * expect_thread_id and expect_node_id.  On false, *reason_out (when
 * non-NULL) points at a static English fragment naming the first failed
 * check, for use in errdetail.
 */
static inline bool
cluster_wal_thread_claim_validate(const ClusterWalThreadClaim *claim, uint16 expect_thread_id,
								  int32 expect_node_id, const char **reason_out)
{
	const char *reason = NULL;

	if (claim->magic != CLUSTER_WAL_THREAD_CLAIM_MAGIC)
		reason = "bad magic";
	else if (claim->version != CLUSTER_WAL_THREAD_CLAIM_VERSION)
		reason = "bad version";
	else if (claim->crc != cluster_wal_thread_claim_crc(claim))
		reason = "bad crc";
	else if (claim->thread_id != expect_thread_id)
		reason = "thread_id mismatch";
	else if (claim->node_id != expect_node_id)
		reason = "node_id mismatch";

	if (reason_out != NULL)
		*reason_out = reason;
	return reason == NULL;
}

/*
 * cluster_wal_thread_dir_name -- "thread_<id>" directory-name builder.
 *
 *	Raw directory names use the sentinel-aware header value itself
 *	(spec-4.1 §2.1 namespace matrix): valid input is
 *	1..CLUSTER_WAL_THREAD_MAX and "thread_0" can never be produced (the
 *	legacy sentinel is not a directory).  Out-of-range input is a caller
 *	bug: Assert in debug builds, and the function still yields an empty
 *	string so a release-build caller cannot silently address a wrong
 *	directory (L218: the Assert is not the guard).
 */
static inline void
cluster_wal_thread_dir_name(uint16 thread_id, char *buf, size_t buflen)
{
	Assert(thread_id >= XLP_THREAD_ID_FIRST_REAL && thread_id <= CLUSTER_WAL_THREAD_MAX);
	if (thread_id < XLP_THREAD_ID_FIRST_REAL || thread_id > CLUSTER_WAL_THREAD_MAX) {
		if (buflen > 0)
			buf[0] = '\0';
		return;
	}
	snprintf(buf, buflen, "thread_%u", (unsigned)thread_id);
}

#ifndef FRONTEND

/*
 * cluster_wal_thread_id -- this instance's WAL thread identity.
 *
 *	Pure function of two PGC_POSTMASTER globals (cluster_enabled,
 *	cluster_node_id); no caching, no allocation, no error path --
 *	callable from inside AdvanceXLInsertBuffer (WALInsertLock held,
 *	possibly inside a critical section) and trivially EXEC_BACKEND-safe
 *	(no postmaster-inherited static state).
 *
 *	Returns XLP_THREAD_ID_LEGACY (0) when not clustered (cluster.enabled
 *	off OR cluster.node_id = -1), else (uint16) (cluster_node_id + 1).
 */
extern uint16 cluster_wal_thread_id(void);

/*
 * cluster_wal_thread_stamp -- stamp value for a new WAL page header.
 *
 *	Same value as cluster_wal_thread_id(); additionally bumps the
 *	page_stamp_count shmem counter when stamping a real id.  nofail and
 *	critical-section-safe (one atomic add; tolerates unattached shmem
 *	by skipping the counter, never the value -- L19).
 */
extern uint16 cluster_wal_thread_stamp(void);

/*
 * cluster_wal_thread_init -- startup validation (spec-4.1 §2.4).
 *
 *	Called from CreateSharedMemoryAndSemaphores (ipci.c) right after
 *	cluster_shared_fs_init(), i.e. in the postmaster (or a standalone
 *	backend) before StartupXLOG reads any WAL.  EXEC_BACKEND children
 *	re-enter CreateSharedMemoryAndSemaphores; the IsUnderPostmaster
 *	guard makes this a no-op there (postmaster-once, CLAUDE.md rule 16).
 *
 *	No-op when cluster.wal_threads_dir is unset.  Otherwise validates
 *	the routing fail-closed (FATAL 53RA0 / 53RA1; never a silent
 *	fallback to the flat layout):
 *	  1. configuration coherence (cluster.enabled on, node_id >= 0)
 *	  2. $PGDATA/pg_wal resolves to <dir>/thread_<id> (st_dev/st_ino
 *	     identity; covers both symlink and bind-mount relocation)
 *	  3. thread claim file: created once (O_EXCL + fsync file + parent)
 *	     or validated against this node's identity
 */
extern void cluster_wal_thread_init(void);

/* L206 five-step shmem region registration ("pgrac wal thread"). */
extern void cluster_wal_thread_shmem_register(void);

/* Dump accessors (cluster_debug.c category 'wal_thread'). */
extern uint64 cluster_wal_thread_page_stamp_count(void);
extern uint16 cluster_wal_thread_dump_thread_id(void);
extern bool cluster_wal_thread_dir_configured(void);
extern bool cluster_wal_thread_dir_validated(void);
extern bool cluster_wal_thread_claim_created(void);

#endif /* !FRONTEND */

#endif /* CLUSTER_WAL_THREAD_H */
