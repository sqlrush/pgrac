/*-------------------------------------------------------------------------
 *
 * cluster_wal_state.h
 *	  pgrac ClusterWalState registry: per-thread WAL metadata catalog on
 *	  the shared WAL root (spec-4.2).
 *
 *	  One fixed-size file <cluster.wal_threads_dir>/pgrac_wal_state =
 *	  512B header + 128 x 512B slots (66048 bytes total).  Each slot is
 *	  owned by exactly one node (owner-only writes, no locks): startup
 *	  publishes ACTIVE only after recovery succeeded (phase ->
 *	  CLUSTER_PHASE_RUNNING transition), clean shutdown publishes
 *	  STOPPED, cluster_stats refreshes the liveness timestamp and WAL
 *	  watermarks every main-loop interval.  A crashed node naturally
 *	  leaves a stale-ACTIVE slot -- "crashed" is a READER-side inference
 *	  (spec-4.3 Coordinator, staleness x CSSD DEAD), never a state this
 *	  module writes.
 *
 *	  Slots are 512B sector-SHAPED with CRC32C torn-write DETECTION; no
 *	  sector atomicity is claimed (POSIX/NFS/cloud block devices do not
 *	  guarantee it -- the voting-disk precedent's correctness philosophy:
 *	  CRC over atomicity, cluster_voting_disk_io.h).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_wal_state.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-4.2-wal-thread-metadata-catalog.md FROZEN v1.0
 *	  Design: feature-034 (Thread registry), spec-2.6 voting-disk slot
 *	  discipline (cluster_qvotec.h)
 *
 *	  Pure POD structs + inline helpers are frontend-visible (unit tests
 *	  include this header alone); file I/O lives in cluster_wal_state.c
 *	  (backend-only).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_WAL_STATE_H
#define CLUSTER_WAL_STATE_H

#include <stdio.h>

#include "cluster/cluster_xlog.h" /* CLUSTER_WAL_THREAD_MAX */
#include "port/pg_crc32c.h"

#define CLUSTER_WAL_STATE_FILENAME "pgrac_wal_state"
#define CLUSTER_WAL_STATE_SLOT_COUNT 128
#define CLUSTER_WAL_STATE_SLOT_SIZE 512
#define CLUSTER_WAL_STATE_FILE_SIZE                                                                \
	(CLUSTER_WAL_STATE_SLOT_SIZE * (1 + CLUSTER_WAL_STATE_SLOT_COUNT)) /* 66048 */

/*
 * CLUSTER_WAL_STATE_SLOT_OFFSET -- THE single-source slot address.
 *
 *	512 + (thread_id - 1) * 512, identically 512 * thread_id for the
 *	1-based thread id space.  thread 1 -> 512, thread 128 -> 65536; the
 *	slot region is [512, 66048) and a write through this macro can
 *	never extend the file.  NEVER hand-write this formula a second
 *	time (spec-4.2 v0.2 P0: a divergent copy placed thread 128 at
 *	66048, past EOF).
 */
#define CLUSTER_WAL_STATE_SLOT_OFFSET(thread_id)                                                   \
	((off_t)CLUSTER_WAL_STATE_SLOT_SIZE + ((off_t)((thread_id) - 1)) * CLUSTER_WAL_STATE_SLOT_SIZE)

#define CLUSTER_WAL_STATE_HEADER_MAGIC ((uint32)0x50475753) /* "PGWS" */
#define CLUSTER_WAL_STATE_SLOT_MAGIC ((uint32)0x50475754)	/* "PGWT" */
#define CLUSTER_WAL_STATE_VERSION ((uint16)1)

/* On-disk slot states (uint32; 0 and everything else = invalid). */
typedef enum ClusterWalSlotState {
	CLUSTER_WAL_SLOT_STATE_ACTIVE = 1,
	CLUSTER_WAL_SLOT_STATE_STOPPED = 2,
} ClusterWalSlotState;

typedef struct ClusterWalStateHeader {
	uint32 magic;
	uint16 version;
	char _pad_6[2];
	uint32 slot_count;
	char _pad_12[4]; /* explicit (L45); int64 below needs 8-align */
	int64 created_at;
	char _reserved[480]; /* spec-4.3+: coordinator term / merge marks */
	uint32 crc;			 /* crc32c over bytes [0, 504) */
	char _pad_508[4];
} ClusterWalStateHeader;

StaticAssertDecl(sizeof(ClusterWalStateHeader) == CLUSTER_WAL_STATE_SLOT_SIZE,
				 "spec-4.2 header is one 512B sector-shaped block");
StaticAssertDecl(offsetof(ClusterWalStateHeader, _pad_12) == 12,
				 "spec-4.2 v0.2 P2: explicit padding at 12..15");
StaticAssertDecl(offsetof(ClusterWalStateHeader, created_at) == 16, "header layout");
StaticAssertDecl(offsetof(ClusterWalStateHeader, crc) == 504, "header crc covers [0,504)");

typedef struct ClusterWalStateSlot {
	uint32 magic;
	uint16 version;
	uint16 thread_id; /* 1..128; == own slot number (self-describing) */
	int32 node_id;	  /* 0..127 */
	uint32 state;	  /* ClusterWalSlotState; 0 / others invalid */
	uint32 tli;		  /* writer's insertion TimeLineID at write time */
	char _pad_20[4];
	int64 started_at;	/* this incarnation's startup time */
	int64 last_updated; /* liveness stamp: advanced only on real writes */
	uint64 highest_lsn; /* GetXLogWriteRecPtr() at refresh (observational) */
	uint64 highest_scn; /* cluster_scn_current() at refresh */
	/*
	 * spec-4.5 extension region (offset 56..503).  Owner writes are
	 * read-modify-PRESERVE (never memset through fill); §3.3d.4.  Zero
	 * means "unknown" and is fail-closed at the merged-recovery engage
	 * gate.  CRC range [0,504) is unchanged so old slots validate.
	 */
	uint64 checkpoint_redo_lsn; /* 56: this thread's last checkpoint redo
								 * start; merged-recovery start point (Q5).
								 * 0 -> 53RA3. Owner: CreateCheckPoint. */
	uint32 refresh_interval_ms; /* 64: owner's stats refresh cadence
								 * (closes 4.3 R2). 0 -> plan GUC fallback. */
	uint32 fpw_was_off;			/* 68: sticky -- set on the authoritative
								 * full_page_writes on->off path; never auto
								 * cleared.  Any merge-set thread set -> 53RA3
								 * (§3.3d.3). */
	uint64 merge_recovered_lsn; /* 72: recovery authority (§3.3c) -- own-LSN
								 * skip bound for the candidate's later self
								 * recovery.  Writer: coordinator; owner
								 * clears at RUNNING publish. */
	char _reserved[424];		/* 80: remaining headroom */
	uint32 crc;					/* crc32c over bytes [0, 504) */
	char _pad_508[4];
} ClusterWalStateSlot;

StaticAssertDecl(offsetof(ClusterWalStateSlot, checkpoint_redo_lsn) == 56,
				 "spec-4.5 extension region starts at 56");
StaticAssertDecl(offsetof(ClusterWalStateSlot, merge_recovered_lsn) == 72,
				 "spec-4.5 merge_recovered_lsn layout");

StaticAssertDecl(sizeof(ClusterWalStateSlot) == CLUSTER_WAL_STATE_SLOT_SIZE,
				 "spec-4.2 slot is one 512B sector-shaped block");
StaticAssertDecl(offsetof(ClusterWalStateSlot, started_at) == 24, "slot layout");
StaticAssertDecl(offsetof(ClusterWalStateSlot, crc) == 504, "slot crc covers [0,504)");

/* Reader-side classification of a slot read (spec-4.2 §2.1). */
typedef enum ClusterWalSlotVerdict {
	CLUSTER_WAL_SLOT_OK = 0,
	CLUSTER_WAL_SLOT_EMPTY,	  /* all-zero slot: thread never published */
	CLUSTER_WAL_SLOT_CORRUPT, /* bad magic/version/crc/self-description */
	CLUSTER_WAL_SLOT_FOREIGN, /* well-formed but unexpected node identity */
} ClusterWalSlotVerdict;

/* ---- pure helpers (header-only; unit-testable, no backend deps) ---- */

static inline uint32
cluster_wal_state_block_crc(const void *block)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, block, 504);
	FIN_CRC32C(crc);
	return (uint32)crc;
}

static inline void
cluster_wal_state_header_fill(ClusterWalStateHeader *h, int64 created_at)
{
	memset(h, 0, sizeof(*h));
	h->magic = CLUSTER_WAL_STATE_HEADER_MAGIC;
	h->version = CLUSTER_WAL_STATE_VERSION;
	h->slot_count = CLUSTER_WAL_STATE_SLOT_COUNT;
	h->created_at = created_at;
	h->crc = cluster_wal_state_block_crc(h);
}

/*
 * Header validation.  reason_out (when non-NULL) names the first failed
 * check for errdetail.
 */
static inline bool
cluster_wal_state_header_validate(const ClusterWalStateHeader *h, const char **reason_out)
{
	const char *reason = NULL;

	if (h->magic != CLUSTER_WAL_STATE_HEADER_MAGIC)
		reason = "bad magic";
	else if (h->version != CLUSTER_WAL_STATE_VERSION)
		reason = "bad version";
	else if (h->crc != cluster_wal_state_block_crc(h))
		reason = "bad crc";
	else if (h->slot_count != CLUSTER_WAL_STATE_SLOT_COUNT)
		reason = "bad slot_count";

	if (reason_out != NULL)
		*reason_out = reason;
	return reason == NULL;
}

static inline void
cluster_wal_state_slot_fill(ClusterWalStateSlot *s, uint16 thread_id, int32 node_id, uint32 state,
							uint32 tli, int64 started_at, int64 last_updated, uint64 highest_lsn,
							uint64 highest_scn)
{
	memset(s, 0, sizeof(*s));
	s->magic = CLUSTER_WAL_STATE_SLOT_MAGIC;
	s->version = CLUSTER_WAL_STATE_VERSION;
	s->thread_id = thread_id;
	s->node_id = node_id;
	s->state = state;
	s->tli = tli;
	s->started_at = started_at;
	s->last_updated = last_updated;
	s->highest_lsn = highest_lsn;
	s->highest_scn = highest_scn;
	s->crc = cluster_wal_state_block_crc(s);
}

/*
 * EMPTY means the full 512B block is zero.  Checking only a few header
 * fields would classify "zero header + garbage body" as EMPTY -- an
 * absence-as-proof hole (spec-4.2 user codereview round 2, P1).
 */
static inline bool
cluster_wal_state_slot_is_zero(const ClusterWalStateSlot *s)
{
	const unsigned char *p = (const unsigned char *)s;
	size_t i;

	for (i = 0; i < sizeof(ClusterWalStateSlot); i++) {
		if (p[i] != 0)
			return false;
	}
	return true;
}

/*
 * Slot classification.
 *
 *	expect_thread: the slot number being read (self-description check).
 *	expect_node:   -1 = any node (reader mode); >= 0 enforces identity
 *	               and yields FOREIGN on mismatch (owner mode).
 *	Readers MUST surface CORRUPT/FOREIGN as UNKNOWN -- never guess a
 *	state from a slot that fails classification (spec-4.2 §3.3).
 */
static inline ClusterWalSlotVerdict
cluster_wal_state_slot_classify(const ClusterWalStateSlot *s, uint16 expect_thread,
								int32 expect_node, const char **reason_out)
{
	const char *reason = NULL;
	ClusterWalSlotVerdict v = CLUSTER_WAL_SLOT_OK;

	if (cluster_wal_state_slot_is_zero(s)) {
		v = CLUSTER_WAL_SLOT_EMPTY;
		reason = "empty";
	} else if (s->magic != CLUSTER_WAL_STATE_SLOT_MAGIC) {
		v = CLUSTER_WAL_SLOT_CORRUPT;
		reason = "bad magic";
	} else if (s->version != CLUSTER_WAL_STATE_VERSION) {
		v = CLUSTER_WAL_SLOT_CORRUPT;
		reason = "bad version";
	} else if (s->crc != cluster_wal_state_block_crc(s)) {
		v = CLUSTER_WAL_SLOT_CORRUPT;
		reason = "bad crc";
	} else if (s->thread_id != expect_thread) {
		v = CLUSTER_WAL_SLOT_CORRUPT; /* mis-addressed write */
		reason = "slot self-description mismatch";
	} else if (s->state != CLUSTER_WAL_SLOT_STATE_ACTIVE
			   && s->state != CLUSTER_WAL_SLOT_STATE_STOPPED) {
		v = CLUSTER_WAL_SLOT_CORRUPT;
		reason = "invalid state";
	} else if (expect_node >= 0 && s->node_id != expect_node) {
		v = CLUSTER_WAL_SLOT_FOREIGN;
		reason = "node_id mismatch";
	}

	if (reason_out != NULL)
		*reason_out = reason;
	return v;
}

#ifndef FRONTEND

/*
 * Backend API (cluster_wal_state.c).  All paths are no-ops when
 * cluster.wal_threads_dir is unset (the registry co-exists with the
 * per-thread layout, spec-4.2 §3.1).
 */

/* ensure(): create-once (O_EXCL + L47 fsync discipline) or validate
 * the existing header; FATAL 53RA2 on corrupt/IO error.  Returns true
 * when the registry is usable. */
extern bool cluster_wal_state_ensure(void);

/* Owner-only slot publishes (FATAL 53RA2 on failure -- startup-grade). */
extern void cluster_wal_state_publish_active(void);
extern void cluster_wal_state_publish_stopped(void);

/* cluster_stats periodic refresh (best-effort: LOG-once + counter on
 * failure, never FATAL). */
extern void cluster_wal_state_refresh_own_slot(void);

/* Reader: pread + classify slot `thread_id` (1..128).  Returns the
 * verdict; on OK fills *slot_out. */
extern ClusterWalSlotVerdict cluster_wal_state_read_slot(uint16 thread_id,
														 ClusterWalStateSlot *slot_out);

/* Dump accessors (cluster_debug.c category 'wal_thread'). */
extern bool cluster_wal_state_registry_ready(void);
extern uint64 cluster_wal_state_refresh_fail_count(void);

#endif /* !FRONTEND */

#endif /* CLUSTER_WAL_STATE_H */
