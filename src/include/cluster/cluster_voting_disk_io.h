/*-------------------------------------------------------------------------
 *
 * cluster_voting_disk_io.h
 *	  pgrac voting disk slot R/W primitives — spec-2.6 Sprint A Step 2 D3.
 *
 *	  Per-slot 512-byte read/write at offset = node_id × 512 inside a
 *	  voting disk file.  Each operation goes through CRC32C verify (Q2
 *	  v0.2 torn-write detection — POSIX does not guarantee sector-atomic
 *	  writes on NFS / iSCSI / cloud block;defence is generation counter
 *	  + CRC + per-cycle retry + multi-disk redundancy, NOT atomic
 *	  guarantee).
 *
 *	  Implementation strategy (Q2 v0.2):
 *	    - O_DIRECT best-effort (some filesystems reject;fallback to
 *	      O_SYNC + fdatasync after every write)
 *	    - 512-byte aligned buffer (posix_memalign)
 *	    - Read: pread + CRC verify + magic / version / node_id sanity
 *	    - Write: bump generation + recompute CRC + pwrite + fdatasync
 *	    - Failure: any I/O error / CRC mismatch / sanity fail returns
 *	      a non-OK ClusterVotingDiskIoState value;caller (cluster_qvotec
 *	      poll cycle) decrements disks_ok_count and retries next cycle
 *
 *	  Step 2 scope:
 *	    - cluster_voting_disk_open / _close   (best-effort O_DIRECT)
 *	    - cluster_voting_disk_read_slot       (pread + CRC + sanity)
 *	    - cluster_voting_disk_write_slot      (CRC compute + pwrite + fdatasync)
 *	    - cluster_voting_disk_format          (initdb-time slot init for
 *	                                           a fresh voting disk file)
 *	    - cluster_voting_disk_compute_crc32c  (helper used by write +
 *	                                           verified by read)
 *
 *	  Step 2 explicitly DEFERS:
 *	    - read_all_disks_into_view orchestration (Step 2 D4 cluster_
 *	      quorum_decision.c — that module owns the cross-disk loop)
 *	    - cluster_smgr opt-in path (Stage 2.X 共享存储 backend abstraction)
 *	    - retry strategy beyond per-cycle (qvotec main loop owns)
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_voting_disk_io.h
 *
 * NOTES
 *	  pgrac-original file.  Compiled only in --enable-cluster mode.
 *
 *	  Spec authority: pgrac:specs/spec-2.6-voting-disk-quorum-lite.md
 *	  (frozen v0.2 2026-05-09, §2.1 ClusterVotingSlot byte layout +
 *	  §3.1 Q2 v0.2 generation + CRC + torn-write detection protocol).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_VOTING_DISK_IO_H
#define CLUSTER_VOTING_DISK_IO_H

#include "cluster/cluster_conf.h"	/* CLUSTER_MAX_NODES (leave-marker region) */
#include "cluster/cluster_qvotec.h" /* ClusterVotingSlot, ClusterVotingDiskIoState */
#include "port/pg_crc32c.h"


#ifdef USE_PGRAC_CLUSTER

/*
 * Voting disk file slot offset:  each slot is 512 bytes, one slot per
 * node_id.  Offset = node_id × 512.
 */
#define CLUSTER_VOTING_SLOT_BYTES 512
#define CLUSTER_VOTING_SLOT_OFFSET(node_id) ((off_t)(node_id) * CLUSTER_VOTING_SLOT_BYTES)

/*
 * spec-5.13 D2/§2.5 — clean-leave-intent marker region.  A SECOND 512-byte
 * slot per node, laid out immediately after the CLUSTER_MAX_NODES voting slots
 * (region 1 = [0, CLUSTER_MAX_NODES*512), region 2 = leave markers).  Each node
 * writes ONLY its own leave-slot (qvotec sole-writer invariant); a marker
 * reaches a quorum-majority because qvotec replicates that one slot to every
 * disk, exactly like the fence marker.  The voting disk file therefore doubles
 * to 2 × CLUSTER_MAX_NODES × 512 bytes (cluster.voting_disk_size_bytes default
 * bumped to match).  The marker payload (ClusterLeaveIntentMarker) carries its
 * own magic/version/CRC, validated by the clean-leave policy layer — this layer
 * is payload-agnostic and just does aligned 512-byte raw slot I/O at the offset.
 */
#define CLUSTER_VOTING_LEAVE_SLOT_OFFSET(node_id)                                                  \
	((off_t)(CLUSTER_MAX_NODES + (node_id)) * CLUSTER_VOTING_SLOT_BYTES)
#define CLUSTER_VOTING_FILE_BYTES_MIN ((off_t)2 * CLUSTER_MAX_NODES * CLUSTER_VOTING_SLOT_BYTES)


/*
 * cluster_voting_disk_open — open a voting disk file for R/W with
 * best-effort O_DIRECT.  Returns -1 on failure (errno set).  Caller
 * uses cluster_voting_disk_close on the returned fd.
 *
 *	create_if_missing = true:  initdb-time path;O_CREAT | O_EXCL +
 *	  caller follows with cluster_voting_disk_format to seed slots
 *	create_if_missing = false: runtime path;file must exist and be
 *	  at least cluster.voting_disk_size_bytes in size
 */
extern int cluster_voting_disk_open(const char *path, bool create_if_missing);
extern void cluster_voting_disk_close(int fd);


/*
 * cluster_voting_disk_io_install_timeout_handler — install the SIGALRM
 * handler used by cluster_voting_disk_read_slot / write_slot for per-I/O
 * timeout enforcement (Hardening v0.4 P1.4).  Caller is the qvotec aux
 * process at startup (after pqsignal(SIGALRM, SIG_IGN);qvotec ignores
 * SIGALRM in its main loop, so installing this handler is non-conflicting).
 *
 *	The handler is async-signal-safe (only sets a sig_atomic_t flag) and
 *	idempotent (replacing SIG_IGN with the real handler).  It is NOT
 *	intended for any process other than qvotec — backends and other aux
 *	processes use SIGALRM for statement_timeout etc., so installing this
 *	handler in those contexts would clobber PG's machinery.
 *
 *	Pass timeout_ms = 0 to bypass the timer (used by format / fsck tools
 *	that want unbounded I/O).
 */
extern void cluster_voting_disk_io_install_timeout_handler(void);
extern void cluster_voting_disk_io_set_timeout_ms(int timeout_ms);


/*
 * cluster_voting_disk_read_slot — read slot for `node_id` from `fd`,
 * verify CRC + magic + version + node_id round-trip + disk_index
 * round-trip, populate `*out`.
 *
 *	`expected_disk_index` is the caller's record of which voting disk
 *	this fd should be (its 0-based index in cluster.voting_disks CSV).
 *	If slot.disk_index != expected_disk_index → SAN/NFS misroute / wrong
 *	mount / wrong file: refuse to trust the slot and return FAILED.  Per
 *	Q3 v0.2 design this is the only line of defense for misrouted I/O
 *	in shared-storage failure modes.  Pass -1 to opt out of the check
 *	(format / repair tools that read slots without knowing which disk).
 *
 *	Returns:
 *	  CLUSTER_VOTING_DISK_IO_OK         success
 *	  CLUSTER_VOTING_DISK_IO_TORN       CRC mismatch (likely torn write)
 *	  CLUSTER_VOTING_DISK_IO_FAILED     I/O error / EOF / sanity fail /
 *	                                    disk_index mismatch (misroute)
 *	  CLUSTER_VOTING_DISK_IO_NOT_TRIED  caller set fd<0 (programming error)
 */
extern ClusterVotingDiskIoState cluster_voting_disk_read_slot(int fd, int expected_disk_index,
															  uint32 node_id,
															  ClusterVotingSlot *out);


/*
 * cluster_voting_disk_write_slot — write `slot` to disk at the slot's
 * own node_id offset.  Caller has already populated all slot fields
 * EXCEPT crc32c (recomputed here);caller is responsible for bumping
 * `slot->generation` per Q2 v0.2 torn-write protocol.
 *
 *	Returns:
 *	  CLUSTER_VOTING_DISK_IO_OK     success (pwrite + fdatasync done)
 *	  CLUSTER_VOTING_DISK_IO_FAILED I/O error
 */
extern ClusterVotingDiskIoState cluster_voting_disk_write_slot(int fd, ClusterVotingSlot *slot);


/*
 * cluster_voting_disk_format — initdb-time helper.  Writes a freshly
 * zeroed slot for every node_id in [0, max_nodes) to a freshly created
 * voting disk file.  Called from initdb when cluster.voting_disks GUC
 * declares a path that doesn't yet exist (Step 5 wires this).
 *
 *	max_nodes typically = CLUSTER_MAX_NODES (128) so the file size is
 *	~64KB regardless of cluster size.  Caller picks file size via
 *	cluster.voting_disk_size_bytes GUC.
 */
extern ClusterVotingDiskIoState cluster_voting_disk_format(int fd, uint32 max_nodes,
														   uint32 disk_index);


/*
 * cluster_voting_disk_compute_crc32c — helper exposed for testing.
 * CRC covers slot bytes 0..507 (i.e. everything except the trailing
 * 4-byte crc32c field).
 */
extern pg_crc32c cluster_voting_disk_compute_crc32c(const ClusterVotingSlot *slot);


/*
 * spec-5.13 D2 — raw 512-byte leave-slot R/W in the leave-marker region.
 *
 *	These are payload-agnostic: the caller (cluster_clean_leave.c) packs a
 *	ClusterLeaveIntentMarker into the first sizeof(marker) bytes of a 512-byte
 *	buffer (rest zeroed) and owns the marker's magic/version/CRC.  This layer
 *	just does an aligned pread/pwrite at CLUSTER_VOTING_LEAVE_SLOT_OFFSET(node_id)
 *	with the same per-I/O timeout discipline as the voting-slot path.
 *
 *	read: returns OK + 512 bytes into out_slot512 (an unwritten/zeroed slot is
 *	OK but its bytes are zero → the caller's magic check rejects it as "no
 *	marker"); FAILED on short read / EOF (file not yet doubled) / I/O error.
 *	write: pwrite + fdatasync; FAILED on error.  out_slot512 / in_slot512 must
 *	point to at least CLUSTER_VOTING_SLOT_BYTES of storage.
 */
extern ClusterVotingDiskIoState cluster_voting_disk_read_leave_slot(int fd, uint32 node_id,
																	void *out_slot512);
extern ClusterVotingDiskIoState cluster_voting_disk_write_leave_slot(int fd, uint32 node_id,
																	 const void *in_slot512);

#endif /* USE_PGRAC_CLUSTER */


#endif /* CLUSTER_VOTING_DISK_IO_H */
