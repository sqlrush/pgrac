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
 *	  Later specs extend the file with raw marker regions after the base
 *	  voting slots: clean leave, join commit, and ADG apply-master lease.
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
 * to 2 × CLUSTER_MAX_NODES × 512 bytes (cluster.voting_disk_size_bytes has
 * grown further as later marker regions were added).  The marker payload
 * (ClusterLeaveIntentMarker) carries its
 * own magic/version/CRC, validated by the clean-leave policy layer — this layer
 * is payload-agnostic and just does aligned 512-byte raw slot I/O at the offset.
 */
#define CLUSTER_VOTING_LEAVE_SLOT_OFFSET(node_id)                                                  \
	((off_t)(CLUSTER_MAX_NODES + (node_id)) * CLUSTER_VOTING_SLOT_BYTES)

/*
 * spec-5.15 D4/§2.6 — join-commit-marker region (region 3).  A THIRD 512-byte
 * slot per node, laid out immediately after the leave-marker region (region 1 =
 * voting slots, region 2 = leave markers, region 3 = join-commit markers).  The
 * coordinator writes the JOINER's join-slot (offset = joiner node_id) — unlike
 * the leave region where each node writes its own slot — so a join marker reaches
 * a quorum-majority because the coordinator's qvotec replicates that one slot to
 * every disk.  The voting disk file therefore grows to at least
 * 3 × CLUSTER_MAX_NODES × 512.  Payload =
 * ClusterJoinCommitMarker (magic 'JCMK'); this layer is payload-agnostic and does
 * aligned 512-byte raw slot I/O at the offset.
 */
#define CLUSTER_VOTING_JOIN_SLOT_OFFSET(node_id)                                                   \
	((off_t)(2 * CLUSTER_MAX_NODES + (node_id)) * CLUSTER_VOTING_SLOT_BYTES)

/*
 * spec-6.4 D7 — ADG Apply Master term/lease region (region 4).  Slot 0 is the
 * global Apply Master authority slot; the remaining slots are reserved for
 * format compatibility.  The region deliberately does NOT reuse
 * ClusterVotingSlot._reserved1, which belongs to the write-fence marker and is
 * rewritten by qvotec heartbeats.  Payload = ClusterAdgApplyMasterLease.
 */
#define CLUSTER_VOTING_APPLY_LEASE_GLOBAL_SLOT 0
#define CLUSTER_VOTING_APPLY_LEASE_SLOT_OFFSET(node_id)                                            \
	((off_t)(3 * CLUSTER_MAX_NODES + (node_id)) * CLUSTER_VOTING_SLOT_BYTES)

/*
 * spec-6.15 D5/appendix B.1 — xid stripe regions.  Region 5 = one 512-byte
 * per-node stripe slot (payload ClusterXidStripeSlotRecord, magic "PGXS"),
 * laid out after the spec-6.4 ADG lease region; sole writer is the owning node
 * (LMON hwm refresh), except the spec-5.18 removal coordinator cross-writes
 * the retired flag (region-3 coordinator-write precedent).  Region 6 = ONE
 * cluster-wide activation record (ClusterXidStripeActivationRecord, magic
 * "PGXA") at a fixed offset right after region 5; written by the activation
 * coordinator inside the spec-5.15 join serialization window.  The regions
 * are materialised lazily exactly like regions 2/3 (an unwritten slot reads
 * back EOF or zeros, which the stripe policy layer rejects as record-absent
 * -- the correct fail-closed empty state).  The voting disk file therefore
 * grows to (5 × CLUSTER_MAX_NODES + 1) × 512 bytes (cluster.voting_disk_
 * size_bytes default bumped to match).  Only stripe slots 0..15 are ever
 * used (CLUSTER_XID_STRIDE); the region is sized by CLUSTER_MAX_NODES to
 * keep the offset arithmetic uniform with regions 1-3.  Payload-agnostic:
 * this layer does aligned 512-byte raw slot I/O, the stripe layer owns the
 * record integrity (cluster_xid_stripe.h).
 */
#define CLUSTER_VOTING_STRIPE_SLOT_OFFSET(node_id)                                                 \
	((off_t)(4 * CLUSTER_MAX_NODES + (node_id)) * CLUSTER_VOTING_SLOT_BYTES)
#define CLUSTER_VOTING_STRIPE_ACTIVATION_OFFSET                                                    \
	((off_t)(5 * CLUSTER_MAX_NODES) * CLUSTER_VOTING_SLOT_BYTES)
#define CLUSTER_VOTING_FILE_BYTES_MIN                                                              \
	((off_t)(5 * CLUSTER_MAX_NODES + 1) * CLUSTER_VOTING_SLOT_BYTES)


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
 * timeout enforcement (Hardening v0.4 P1.4).  Production callers are
 * qvotec and the ADG MRP aux process; both install it at startup after
 * pqsignal(SIGALRM, SIG_IGN), so installing this handler is non-conflicting.
 *
 *	The handler is async-signal-safe (only sets a sig_atomic_t flag) and
 *	idempotent (replacing SIG_IGN with the real handler).  It is NOT
 *	intended for ordinary backends — they use SIGALRM for statement_timeout
 *	etc., so installing this handler there would clobber PG's machinery.
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

/*
 * spec-5.15 D4 — raw 512-byte join-commit-marker slot R/W in region 3, at
 * CLUSTER_VOTING_JOIN_SLOT_OFFSET(node_id).  Payload-agnostic (the caller marshals
 * a ClusterJoinCommitMarker and owns its magic/version/CRC); same per-I/O timeout
 * discipline as the leave-slot path.  read returns OK + 512 bytes (a zeroed slot
 * is OK — the caller's magic check rejects it); FAILED on short read / EOF (file
 * not yet tripled) / I/O error.
 */
extern ClusterVotingDiskIoState cluster_voting_disk_read_join_slot(int fd, uint32 node_id,
																   void *out_slot512);
extern ClusterVotingDiskIoState cluster_voting_disk_write_join_slot(int fd, uint32 node_id,
																	const void *in_slot512);

/*
 * spec-6.4 D7 — raw 512-byte ADG apply-master lease slot R/W in region 4.
 * Payload-agnostic; cluster_adg.c owns the marker CRC and validation.
 */
extern ClusterVotingDiskIoState cluster_voting_disk_read_apply_lease_slot(int fd, uint32 node_id,
																		  void *out_slot512);
extern ClusterVotingDiskIoState cluster_voting_disk_write_apply_lease_slot(int fd, uint32 node_id,
																		   const void *in_slot512);
extern ClusterVotingDiskIoState cluster_voting_disk_read_apply_lease_global_slot(int fd,
																				 void *out_slot512);
extern ClusterVotingDiskIoState
cluster_voting_disk_write_apply_lease_global_slot(int fd, const void *in_slot512);

#endif /* USE_PGRAC_CLUSTER */


#endif /* CLUSTER_VOTING_DISK_IO_H */
