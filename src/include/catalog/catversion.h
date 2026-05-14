/*-------------------------------------------------------------------------
 *
 * catversion.h
 *	  "Catalog version number" for PostgreSQL.
 *
 * The catalog version number is used to flag incompatible changes in
 * the PostgreSQL system catalogs.  Whenever anyone changes the format of
 * a system catalog relation, or adds, deletes, or modifies standard
 * catalog entries in such a way that an updated backend wouldn't work
 * with an old database (or vice versa), the catalog version number
 * should be changed.  The version number stored in pg_control by initdb
 * is checked against the version number compiled into the backend at
 * startup time, so that a backend can refuse to run in an incompatible
 * database.
 *
 * The point of this feature is to provide a finer grain of compatibility
 * checking than is possible from looking at the major version number
 * stored in PG_VERSION.  It shouldn't matter to end users, but during
 * development cycles we usually make quite a few incompatible changes
 * to the contents of the system catalogs, and we don't want to bump the
 * major version number for each one.  What we can do instead is bump
 * this internal version number.  This should save some grief for
 * developers who might otherwise waste time tracking down "bugs" that
 * are really just code-vs-database incompatibilities.
 *
 * The rule for developers is: if you commit a change that requires
 * an initdb, you should update the catalog version number (as well as
 * notifying the pgsql-hackers mailing list, which has been the
 * informal practice for a long time).
 *
 * The catalog version number is placed here since modifying files in
 * include/catalog is the most common kind of initdb-forcing change.
 * But it could be used to protect any kind of incompatible change in
 * database contents or layout, such as altering tuple headers.
 * Another common reason for a catversion update is a change in parsetree
 * external representation, since serialized parsetrees appear in stored
 * rules and new-style SQL functions.  Almost any change in primnodes.h or
 * parsenodes.h will warrant a catversion update.
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/catversion.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CATVERSION_H
#define CATVERSION_H

/*
 * We could use anything we wanted for version numbers, but I recommend
 * following the "YYYYMMDDN" style often used for DNS zone serial numbers.
 * YYYYMMDD are the date of the change, and N is the number of the change
 * on that day.  (Hopefully we'll never commit ten independent sets of
 * catalog changes on the same day...)
 */

/*							yyyymmddN */
/* PGRAC: bumped from PG 202307071. */
/*  - 202604270: cluster_get_wait_events SRF + pg_stat_cluster_wait_events */
/*               view (spec-0.16, stage 0.16). */
/*  - 202604271: cluster_get_gcluster_wait_events SRF + */
/*               pg_stat_gcluster_wait_events view (spec-0.17, stage 0.17). */
/*  - 202604280: cluster_get_nodes SRF + pg_cluster_nodes view */
/*               (spec-0.19, stage 0.19). */
/*  - 202604290: cluster_ic_mock_* test SRFs (spec-0.26, stage 0.26). */
/*  - 202604300: cluster_inject_fault + cluster_get_injection_state SRFs */
/*               + pg_stat_cluster_injections view (spec-0.27, stage 0.27). */
/*  - 202604310: cluster_get_stat_nodes + cluster_get_pgstat_counters SRFs */
/*               + pg_stat_cluster_nodes + pg_stat_cluster_counters views */
/*               (spec-0.28, stage 0.28). */
/*  - 202604320: cluster_dump_state SRF + pg_cluster_state view */
/*               (spec-0.29, stage 0.29). */
/*  - 202605010: cluster.shared_storage_backend GUC + 5 cluster_shared_fs */
/*               wait events (no new SRF; pg_cluster_state extended with */
/*               a shared_fs category) (spec-1.1, stage 1.1). */
/*  - 202605020: cluster_smgr bridge + cluster.smgr_user_relations GUC + */
/*               smgrsw[] extended to 2 entries + 3 cluster-smgr-* */
/*               injection points + pg_cluster_state shared_fs / guc */
/*               extended (spec-1.2, stage 1.2; 方案 C 单文件). */
/*  - 202605030: cluster shmem region registry + cluster_shmem_dump_regions */
/*               SRF (OID 8910) + pg_cluster_shmem view + */
/*               cluster.shmem_max_regions GUC + 4 cluster-shmem-* */
/*               injection points + pg_cluster_state.shmem extended with */
/*               region.<name>.* keys (spec-1.3, stage 1.3; registry */
/*               replaces hard-coded dispatch from spec-0.14). */
/*  - 202605040: cluster block format change: PageHeaderData +8B */
/*               pd_block_scn + PG_PAGE_LAYOUT_VERSION 4 -> 5 + */
/*               cluster_scn.h SCN typedef stub + pg_cluster_state.shmem */
/*               + block_format category 4 keys (spec-1.4, stage 1.4; */
/*               binary not compatible with vanilla PG 16; pgrac 1.3 */
/*               data must be dump+restore migrated to 1.4+). */
/*  - 202605050: cluster ITL slot array (stage 1.5): heap PageHeader */
/*               +384B ITL slot array (INITRANS=8 × 48B/slot) via */
/*               PageInitHeapPage + HeapTupleHeader +1B t_itl_slot_idx */
/*               + cluster_itl_slot.h ClusterItlSlotData typedef stub + */
/*               pd_flags PD_HAS_ITL bit (0x0008) + MaxHeapTupleSize */
/*               reduced 8152 -> 7768 + pg_cluster_state.block_format */
/*               extended 4 -> 9 keys (spec-1.5, stage 1.5; pgrac 1.4 */
/*               data must be dump+restore migrated to 1.5+). */
/* */
/*  Stage 1.6 (BufferDesc cluster fields, BUFFERDESC_PAD_TO_SIZE 64->128, */
/*  cluster_buffer_desc.h, ClusterInitBufferDescFields, pg_cluster_state. */
/*  buffer_format category) does NOT bump catversion: it changes only */
/*  in-memory shared structures (BufferDescriptors array recreated on */
/*  every start) + a SRF return shape (new keys in existing function). */
/*  No on-disk format change (PageHeader / HeapTupleHeader / ITL / */
/*  catalog schema all unchanged), so 1.5 data dirs start cleanly under */
/*  1.6+ binary.  The catversion bump in initial 1.6 commit was reverted */
/*  by spec-stage1-codex-fixes Deliverable 6 (codex review 2026-05-02). */
/* */
/*  Stage 1.6.1 hardening (spec-stage1-codex-fixes, codex review fixes): */
/*  Stage 1.5 t_itl_slot_idx WAL/MinimalTuple/expand_tuple coverage + */
/*  HeapPageUsableBytes capacity macro + Stage 1.6 AssertNotCatalogBuffer */
/*  Lock pcm_lock guard + LWTRANCHE_BUFFER_PCM_LOCK + "cache line" -> */
/*  "64B BufferDesc segment" terminology + pgrac-acceptance numbers */
/*  upgrade.  All hardening fixes are runtime/inline level; no on-disk */
/*  format change.  catversion stays at 202605050. */
/* Stage 1.15: bump for cluster_scn_advance / current / observe SQL */
/*  UDFs (3 new pg_proc entries OID 8911-8913); spec-1.15 Q7+Q10+L1. */
/* Stage 1.15.1 (round 8 P3): cluster_scn_current pg_proc.dat */
/*  provolatile s -> v + proparallel s -> r (it reads dynamically- */
/*  mutating shmem state).  Catalog row attribute change => bump. */
/* Stage 1.18: commit/abort WAL records carry an optional 8-byte */
/*  xl_xact_scn sub-record (spec-1.18, XACT_XINFO_HAS_SCN bit 9). */
/*  WAL record format is on-disk format -> bump.  Old data dirs cannot */
/*  cross-replay 1.17->1.18 because parsers diverge; CATALOG_VERSION_NO */
/*  mismatch is the gate that makes pg_control reject mixed binaries. */
/*  Spec: spec-1.18-wal-record-xl-scn.md Q2 ★. */
/* Stage 1.19: WAL Page Header xlp_thread_id + xlp_cluster_flags */
/*  placeholder fields reuse the existing MAXALIGN tail padding so */
/*  sizeof(XLogPageHeaderData) stays 24 bytes (StaticAssertDecl in */
/*  xlog_internal.h).  vanilla PG MemSet's the entire 8 KB page before */
/*  writing fields (xlog.c:1878 + xlog.c:4685 + pg_resetwal.c:1059) so */
/*  pre-1.19 datadirs land both fields as 0 (= XLP_THREAD_ID_LEGACY + */
/*  XLP_CLUSTER_FLAGS_RESERVED) automatically.  No on-disk format */
/*  change -> NO catversion bump (Q1=A approve).  Spec: */
/*  spec-1.19-wal-page-header-thread-id.md APPROVED v0.2. */
/* Stage 1.20: TTSlot type definition only (cluster_tt_slot.h NEW; 32 */
/*  byte struct + sentinel constants + StaticAssertDecl + inline */
/*  helpers).  No on-disk artefact in Stage 1.20.  Therefore: NO */
/*  catversion bump.  Spec: spec-1.20-tt-slot-data-structure.md v0.2. */
/* Stage 1.21: UndoSegmentHeaderData type definition only */
/*  (cluster_undo_segment.h NEW; 8192 byte struct + segment_state */
/*  enum + sentinels + StaticAssertDecl + inline helpers).  No on- */
/*  disk artefact in Stage 1.21: undo tablespace and segment files */
/*  are created by spec-1.22 (atomic with PageInitUndoSegmentHeader */
/*  caller and PD_UNDO_SEG_HEADER bufpage.h definition).  Therefore: */
/*  NO catversion bump.  Spec: spec-1.21-undo-segment-header.md v0.2. */
/* Stage 1.22: dedicated undo tablespace + atomic batch on-disk */
/*  format change.  Bundles 7 atomic changes in a single commit, */
/*  any one of which would individually require a catversion bump: */
/*    1. UNDOTABLESPACE_OID = 9100 added to pg_tablespace.dat (v0.2 */
/*       Hardening v1.0.2 amend: original 1665 conflicted with */
/*       pg_get_serial_sequence in pg_proc.dat; fallback to 9100 per */
/*       spec §6 R2). */
/*    2. $PGDATA/pg_undo/instance_<N>/seg_<id>.dat layout established */
/*       at initdb time (subdirs[] += "pg_undo", "pg_undo/instance_0"). */
/*    3. PD_UNDO_SEG_HEADER = 0x0010 added to bufpage.h pd_flags + */
/*       PD_VALID_FLAG_BITS bumped 0x000F -> 0x001F (cluster mode). */
/*    4. PageInitUndoSegmentHeader() shipped (bufpage.c) + initdb seed */
/*       segment writer (initdb.c) -- both call shared frontend-safe */
/*       helper cluster_undo_segment_make_header_bytes (D14c). */
/*    5. UndoSegmentHeaderData on-disk format (block 0 of every */
/*       seg_<id>.dat) materialized -- spec-1.21 placeholder type now */
/*       written for real. */
/*    6. RM_CLUSTER_UNDO_ID resource manager registered in rmgrlist.h */
/*       with one subtype XLOG_UNDO_SEGMENT_INIT (D14a B-lite; XLOG_FPI */
/*       was rejected per v0.2 P1-A because RelFileLocator routing */
/*       can't address $PGDATA/pg_undo paths). */
/*    7. user-visible tablespace helpers special-case UNDOTABLESPACE_OID */
/*       (misc.c pg_tablespace_location + pg_tablespace_databases; */
/*       tablespace.c Alter*TableSpace* reject; D14b v0.2 P1-C 联动). */
/*  Stage 1.21 datadir cannot be opened by 1.22 binary (FATAL via */
/*  pg_control catversion check) and vice versa; users must dump+ */
/*  restore (pg_upgrade 1.21->1.22 lands in feature-117). */
/*  Spec: spec-1.22-undo-tablespace-bootstrap.md APPROVED v0.2. */
/*
 * spec-2.2 D10 (2026-05-07) -- bump 202605190 -> 202605200 for the
 * pg_cluster_ic_peers SRF + view (D9) addition to pg_proc.dat +
 * system_views.sql.  Per L46 (atomic batch + catversion bump):
 * downstream TAP / regress hardcoded catversion regex MUST be
 * lower-bound (>= 202605200) not equal-bound; see grep audit in
 * spec-2.2 §6 DoD.
 *
 * spec-2.3 D7 (2026-05-08) -- bump 202605200 -> 202605210 for the
 * pg_cluster_ic_msg_types SRF + view (D8) addition to pg_proc.dat +
 * system_views.sql, plus the wire-protocol pivot from spec-2.2
 * 24-byte ClusterMsgHeader to spec-2.3 36-byte ClusterICEnvelope
 * (incompatible WAL/IC bytes -> datadir from a 202605200 binary
 * cannot be opened by a 202605210 binary and vice versa).  Per
 * L46: downstream TAP / regress hardcoded catversion regex MUST
 * remain lower-bound (>= 202605210) not equal-bound; see grep
 * audit in spec-2.3 §7 DoD.
 */
/*
 * spec-2.4 D7 (2026-05-08) -- bump 202605210 -> 202605220 for
 * pg_cluster_ic_peers SRF/view extension to 23 columns
 * (+ stale_epoch_drop_count + chunk_reassembly_active +
 * chunk_reassembly_timeout_count + lamport_observe_advance_count) +
 * 5 NEW PGC_POSTMASTER GUCs (interconnect_payload_max_bytes +
 * chunk_reassembly_timeout_ms + tcp_keepidle_sec / keepintvl_sec /
 * keepcnt) + 2 NEW SQLSTATE (53R20 + 53R21).  Per L46:downstream
 * TAP / regress hardcoded catversion regex MUST remain lower-bound
 * (>= 202605220) not equal-bound;see grep audit in spec-2.4 §7
 * DoD.
 */
/*
 * spec-2.6 D16 (2026-05-09) -- bump 202605230 -> 202605240 for Step 4
 * D15 NEW 2 SRFs (cluster_get_quorum_state OID 8917 +
 * cluster_get_voting_disks OID 8918) + 2 views (pg_cluster_quorum_state
 * + pg_cluster_voting_disks) + 4 NEW PGC_POSTMASTER GUCs
 * (cluster.voting_disks + quorum_poll_interval_ms +
 * voting_disk_io_timeout_ms + voting_disk_size_bytes) + 4 NEW SQLSTATE
 * (53R40 ERRCODE_CLUSTER_QUORUM_LOST + 53R41 + 53R42 + 53R43) + 3 NEW
 * wait events (BgProcQvotecMainLoop + VotingDiskRead/Write) + 5 NEW
 * inject points + 4 NEW pgstat counters.  Per L46 lower-bound regex
 * convention.
 */
/*
 * 202605250: spec-2.28 Sprint A Step 4 (fence-lite catalog surface) —
 * 1 NEW SQLSTATE 53R50 ERRCODE_CLUSTER_QUORUM_LOST_BACKEND, 1 NEW wait
 * event WAIT_EVENT_CLUSTER_FENCE_BACKEND_INTERRUPT_CHECK (60→61 → 64
 * spec-2.6 → 65 spec-2.28), 1 NEW SRF cluster_get_fence_state (oid
 * 8919) + matching pg_cluster_fence_state view, 4 NEW pgstat counters
 * (cluster.fence.{freeze_broadcast/thaw_broadcast/self_fence_initiated/
 * freeze_signal_received}_count), 3 NEW inject points (cluster-fence-
 * pre-freeze-broadcast / -pre-self-fence-shutdown / -post-thaw-broadcast).
 */
/*
 * spec-2.29 Sprint A Step 1 (2026-05-11): bump for catalog surface
 * delta introduced by reconfig coordinator (internal-only A scope).
 *
 * Surface added across Sprint A:
 *   +1 SQLSTATE 53R60 ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS (Step 3 D8)
 *   +1 wait event BgProcLmonReconfigTick (Step 3 D9)
 *   +1 shmem region "pgrac cluster reconfig" (Step 1 D2)
 *   +1 SRF pg_cluster_reconfig_state (oid 8920, 9 cols, Step 3 D5b/D6)
 *   +5 inject points (cluster-reconfig-*: tick-entry / decide-coord /
 *     epoch-bump-pre / broadcast-procsig-pre + cluster-cssd-mark-peer-
 *     dead for unit/inject tests) (Step 3 D10)
 *   +0 GUC (cssd_heartbeat_interval_ms from spec-2.5 reused)
 *
 * Step 1 bumps catversion as part of shmem region addition so that
 * any catalog-aware tooling using CATALOG_VERSION_NO sees the spec-
 * 2.29 surface from Sprint A inception;Steps 3-4 populate SRF /
 * errcode / wait event / inject point catalog rows behind this
 * already-bumped catversion.
 */
/* spec-2.14 D8 (2026-05-13):  add cluster_get_grd_shards SRF + pg_cluster_grd_shards
 * view + cluster_grd shmem region.  catversion bump to invalidate any catalog-
 * aware tooling caches when spec-2.14 GRD routing substrate lands. */
/* spec-2.15 D9 (2026-05-13):  add cluster_get_grd_entries SRF + pg_cluster_grd_entries
 * view + cluster_grd entry HTAB + cluster.grd_max_entries GUC + named tranche
 * ClusterGrdShard.  catversion bump for catalog-aware tooling caches. */
/* spec-2.17 D30 (2026-05-30):  PGPROC.cluster_grd_generation uint64 +
 * cluster_grd_bast_pending bool fields(P1.7 防 stale BAST/CANCEL);
 * GesRequestOpcode 7 全集(NEW BAST=4/BAST_ACK=5/DEADLOCK_PROBE=6/
 * CANCEL_PENDING=7);PROCSIG_CLUSTER_GES_BAST + GES_CANCEL slot;
 * 53R72/53R73 SQLSTATE;4 NEW wait events;7 NEW GUCs(BAST retry +
 * deadlock budget).  catversion bump for catalog tooling. */
/* spec-2.19 D12 (2026-05-14):  LMD daemon skeleton + deadlock-detection
 * ownership migration from spec-2.17 caller-side placeholder.  Adds
 * pg_cluster_lmd view + cluster_get_lmd_state SRF (D11);3 NEW wait events
 * (LMD_STARTUP/SCAN/IDLE);1 NEW GUC cluster.lmd_enabled PGC_POSTMASTER;
 * 53R81 SQLSTATE cluster_lmd_unavailable;ClusterLmdShmem region +
 * LWTRANCHE_CLUSTER_LMD;LmdProcess AuxProcType + NUM_AUXILIARY_PROCS
 * 13 → 14.  catversion bump for catalog tooling. */
#define CATALOG_VERSION_NO 202605310

/* spec-2.16 D19 (2026-05-29):  GesRequestPayload + GesReplyPayload wire
 * payload structs (48B each + StaticAssertDecl);  ClusterGrdHolderId
 * 4-tuple typedef (24B);  cluster_grd 9 NEW counter (4 cap + 5 nofail);
 * cluster_grd_pending + cluster_grd_outbound + cluster_grd_work_queue
 * shmem regions + LWLock tranches;  cluster.ges_request_timeout_ms GUC;
 * 53R70/53R71 SQLSTATE.  catversion bump for catalog tooling. */
#define CATALOG_VERSION_NO_PRIOR 202605290

#endif
