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
 * cluster_voting_disk_read_slot — read slot for `node_id` from `fd`,
 * verify CRC + magic + version + node_id round-trip, populate `*out`.
 *
 *	Returns:
 *	  CLUSTER_VOTING_DISK_IO_OK         success
 *	  CLUSTER_VOTING_DISK_IO_TORN       CRC mismatch (likely torn write)
 *	  CLUSTER_VOTING_DISK_IO_FAILED     I/O error / EOF / sanity fail
 *	  CLUSTER_VOTING_DISK_IO_NOT_TRIED  caller set fd<0 (programming error)
 */
extern ClusterVotingDiskIoState cluster_voting_disk_read_slot(int fd, uint32 node_id,
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

#endif /* USE_PGRAC_CLUSTER */


#endif /* CLUSTER_VOTING_DISK_IO_H */
