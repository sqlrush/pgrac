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
/* spec-2.27 D9 (2026-05-17):  GES reliability hardening — master
 * generation + retransmit + perpetual wait gated.  GesRequestPayload wire
 * ABI 48B -> 56B (NEW shard_master_generation field).  NEW reserved
 * opcode GES_REQ_OPCODE_PRIORITY_BOOST = 11 + GesPriorityBoostPayload 32B
 * (RESERVED, NOT SENT — wire-with-stub-receiver反模式 enforcement L107
 * N+5).  NEW shmem region 'pgrac cluster ges dedup' (LMS-owned dedup
 * HTAB, key = 5-tuple {origin_node_id, opcode, request_id, cluster_epoch,
 * shard_master_generation}).  ClusterLmsNativeLockProbeSlot ABI 128B ->
 * 256B (per-slot LWLockPadded spec-2.25 Hardening 候选 1 race fix).
 * NEW 2 GUC cluster.ges_retransmit_max_attempts + cluster.ges_dedup_max_
 * entries + cluster.ges_request_timeout_ms range expand [-1, 600000]
 * (perpetual wait gated, default 60000 不变).  NEW LMS counter
 * priority_starvation_observed_count + lms_restart_generation atomic.
 * catversion bump for catalog tooling. */
/* spec-2.30 D10 (2026-05-17):  PCM 9-state machine activation —
 * spec-1.7 PCM placeholder 4 stub bodies activated.  GrdEntry struct
 * file-private full layout (216B; BufferTag=20B PG 16.13 + atomic fields
 * + LWLockPadded 128B;  spec-2.30 §2.1 nominal 208B mismatch flagged for
 * Hardening v1.0.1 F1 spec amend).  NEW shmem region 'pgrac cluster pcm
 * grd hdr' + HTAB 'pgrac cluster pcm grd htab' (was placeholder array in
 * spec-1.7).  NEW LWTRANCHE_CLUSTER_PCM (per-entry LWLockPadded HC57/HC61).
 * 9 NEW transition counters (8 active + Trans-9 HC60永 0 until Stage 3).
 * GUC cluster.pcm_grd_max_entries default 0 -> -1 sentinel (auto-resolve
 * to NBuffers;  0 = explicit disable preserving spec-1.7 stub behavior;
 * HC62 fail-closed FATAL on shortfall).  2 NEW wait events
 * ClusterPcmGrdInit + ClusterPcmTransitionApply (CLUSTER_WAIT_EVENTS_COUNT
 * 75 -> 77).  cluster_pcm_lock_query 真 lookup (spec-1.7 always-N 行为
 * 撤销).  catversion bump for catalog tooling. */
/* spec-2.33 D11 (2026-05-19):  Cache Fusion block-shipping data plane.
 * NEW msg_type PGRAC_IC_MSG_GCS_BLOCK_REQUEST=14 + GCS_BLOCK_REPLY=15;
 * NEW GcsBlockRequestPayload (64B) + GcsBlockReplyHeader (48B) + 8192B
 * block_data;  NEW GcsBlockReplyStatus 7-value enum (GRANTED +
 * STORAGE_FALLBACK + 4 DENIED + MASTER_NOT_HOLDER);  cluster_gcs_block
 * shmem region + LWTRANCHE_CLUSTER_GCS_BLOCK;  8 NEW data-plane counters
 * exposed via dump_gcs (14→22 rows);  NEW GUC cluster.gcs_reply_timeout_ms
 * (PGC_SUSET 5000ms);  4 NEW wait events
 * (CLUSTER_WAIT_EVENTS_COUNT 79→83):  ClusterGCSBlockShipWait +
 * ClusterGCSBlockRequestDispatch + ClusterGCSBlockReplyDispatch +
 * ClusterGCSBlockChecksumFail.  cluster_pcm_lock_acquire_buffer NEW
 * (BufferDesc-aware variant);  tag-only cluster_pcm_lock_acquire fails
 * closed on remote-master S/X with errhint.  bufmgr cluster_bufmgr_
 * probe/copy_block_for_gcs helpers (HC82 XLogFlush(page_lsn) before ship
 * + HC89 single-retry revalidation).  catversion bump for catalog
 * tooling. */
/* spec-2.34 D11 (2026-05-19):  GCS block reliability hardening.
 * NEW GcsBlockReplyStatus value DENIED_DEDUP_FULL=7 (enum 7→8);
 * NEW cluster_gcs_block_dedup shmem region (HTAB cap × 8312B fixed entry,
 * default cap 1024 → 8.4MB per master node) + LWTRANCHE_CLUSTER_GCS_BLOCK_
 * DEDUP built-in tranche;  3 NEW GUC (cluster.gcs_block_retransmit_max_
 * retries PGC_SUSET 4 + ..._initial_backoff_ms PGC_SUSET 100 + ..._dedup_
 * max_entries PGC_POSTMASTER 1024);  2 NEW wait events
 * (CLUSTER_WAIT_EVENTS_COUNT 83→85):  ClusterGCSBlockRetransmitWait +
 * ClusterGCSBlockEpochStaleRetry;  9 NEW data-plane reliability counters
 * exposed via dump_gcs (22→31 rows):  retransmit_attempt_count /
 * retransmit_send_count / retransmit_exhausted_count / dedup_hit_count /
 * dedup_miss_count / dedup_collision_count / dedup_full_count /
 * epoch_invalidate_wake_count / stale_reply_drop_count;  1 NEW SQLSTATE
 * 53R90 cluster_gcs_block_retransmit_exhausted;  HC90-HC100 11 NEW.
 * cluster_gcs_block_on_epoch_advance hook wired into spec-2.29
 * cluster_reconfig_apply_epoch_bump_as_coordinator after epoch +
 * LSN stamp, before publish_event (HC95 ordering).  2 NEW inject
 * points (cluster-gcs-block-drop-reply-before-send +
 * cluster-gcs-block-force-epoch-stale-reply).  catversion bump for
 * catalog tooling. */
/* spec-2.35 D14 (2026-05-19):  Cache Fusion 2-way protocol S-to-S read sharing.
 * NEW ClusterICMsgType PGRAC_IC_MSG_GCS_BLOCK_FORWARD=16 (master→holder);
 * NEW GcsBlockReplyStatus value GRANTED_FROM_HOLDER=8 (enum 8→9);
 * NEW GcsBlockForwardPayload struct 64B (HC102 wire ABI);
 * GcsBlockReplyHeader.reserved_0[10] reserved 重解读 → forwarding_master_
 *   node_bytes[4] + reserved_0[6] (HC109; sizeof 48B 不变 but semantic 变 →
 *   pg_upgrade catversion 强制 boundary;  uses memcpy helpers to encode
 *   int32 little-endian and avoid struct padding alignment regression);
 * NEW HC108 reply handler authorized chain validation (direct-from-master
 *   OR forwarded-by-expected-master with status in {GRANTED_FROM_HOLDER,
 *   DENIED_MASTER_NOT_HOLDER});
 * HC110 master_holder lifecycle real maintenance across 8 PCM transitions
 *   (was unset since spec-2.30);  cluster_pcm_master_holder_node_by_tag
 *   helper exposed for master-side forward routing decision;
 * HC111 s_holders_bitmap = "cache residency" 语义 (no longer transient
 *   content-lock holding;  bit persists across UnlockBuffer for SCUR);
 * HC112 bufmgr hook bifurcation:  撤 spec-2.31 D3 UnlockBuffer-triggered
 *   release;  NEW cluster_pcm_lock_unlock_content_buffer no-op for SCUR
 *   (bit preserved) + delegate-to-eviction-release for XCUR;  release
 *   hook moved to InvalidateBuffer + InvalidateVictimBuffer eviction
 *   paths (covers DropRelations*Buffers + DropDatabaseBuffers via
 *   InvalidateBuffer chain);  rename cluster_pcm_lock_release_buffer →
 *   cluster_pcm_lock_release_buffer_for_eviction;
 * HC113 dedup state machine extension:  internal FORWARDED_IN_FLIGHT
 *   status via status==GRANTED_FROM_HOLDER on entry + holder_node stored
 *   in reply_header.sender_node;  NEW GCS_BLOCK_DEDUP_FORWARDED_DUPLICATE
 *   return (distinct from CACHED_REPLY and IN_FLIGHT_DUPLICATE) so
 *   duplicate requests re-forward to holder instead of being silent-
 *   dropped;  HC114 entry inspection must check status before treating
 *   as cached 8KB block;
 * 7 NEW counters exposed via dump_gcs (31→38 rows):
 *   block_forward_sent_count / block_forward_received_count /
 *   block_from_holder_ship_count / block_forward_holder_evicted_count /
 *   s_holders_bitmap_redirect_count / master_holder_lifecycle_count /
 *   forward_replay_count;
 * 0 NEW wait events (CLUSTER_WAIT_EVENTS_COUNT 保持 85;  sender does not
 *   know forward path at sleep time so dedicated wait event would not
 *   be observable per Q-D11 rationale);
 * 2 NEW inject points (cluster-gcs-block-forward-master-side +
 *   cluster-gcs-block-evict-holder-before-ship);
 * HC101-HC114 14 NEW.  catversion bump for catalog tooling + wire ABI
 * reserved 重解读 + bufmgr hook 重构. */
/* spec-2.36 D7 (2026-05-20):  Cache Fusion 3-way protocol — X writer
 * transfer + reader starvation guard.
 * NEW ClusterICMsgType:  PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE=17 (master→
 *   S/X holder invalidate request 64B) + PGRAC_IC_MSG_GCS_BLOCK_
 *   INVALIDATE_ACK=18 (holder→master ack 64B);  request+ack MUST be
 *   distinct msg_type because both are 64B fixed payload and IC
 *   dispatcher demuxes by msg_type only (codereview F1 P0).
 * NEW GcsBlockReplyStatus values (8→11):  X_GRANTED_FROM_HOLDER=9
 *   (X-flavored holder direct ship for 3-way writer transfer;  reuses
 *   spec-2.35 HC108 authorized chain) + DENIED_PENDING_X=10 (HC117
 *   reader starvation guard transient deny + backoff retry) +
 *   DENIED_INVALIDATE_TIMEOUT=11 (master invalidate ack budget
 *   exhausted;  sender maps to 53R91).
 * NEW GcsBlockInvalidatePayload 64B + GcsBlockInvalidateAckPayload
 *   64B (HC83 CRC32C @ offset 48;  identical layout offsets for
 *   symmetric header parsing).
 * GrdEntry layout extends 232→248 (+16B):  new pending_x_requester_
 *   node int32 (-1 = none) + pending_x_reserved int32 pad +
 *   pending_x_since_lsn uint64 (HC117 observability;  LSN avoids clock
 *   drift, idle DB acceptable per Q7).
 * NEW HC115 master decision tree X-state path + HC116 broadcast
 *   invalidate sync ack + HC117 S barrier reader starvation guard +
 *   HC118 X transfer holder direct ship + HC120 dedup INVALIDATE_IN_
 *   FLIGHT state + HC121 ClusterTriple 3-node TAP fixture + HC123
 *   XLogFlush-before-X-transfer invariant + HC124 node-dead pending_x
 *   sweep correctness contract.
 * NEW cluster_bufmgr_invalidate_block_for_gcs(tag, expected_mode,
 *   *out_lsn) bufmgr internal helper (InvalidateBuffer is PG static,
 *   must wrap in bufmgr.c;  mirrors spec-2.35 HC112 release_buffer_
 *   for_eviction pattern).
 * 3 NEW GUC:  cluster.gcs_block_invalidate_ack_timeout_ms PGC_SUSET
 *   1500 + cluster.gcs_block_starvation_backoff_ms PGC_SUSET 100 +
 *   cluster.gcs_block_starvation_max_retries PGC_SUSET 8.
 * 2 NEW SQLSTATE:  53R91 cluster_gcs_block_invalidate_timeout +
 *   53R92 cluster_gcs_block_starvation_exhausted.
 * 3 NEW wait events (CLUSTER_WAIT_EVENTS_COUNT 85→88):
 *   ClusterGCSBlockInvalidateBroadcast + ClusterGCSBlockInvalidate
 *   AckWait + ClusterGCSBlockStarvationRetry.
 * 6 NEW counters exposed via dump_gcs (31→37 rows):
 *   block_invalidate_broadcast_count / block_invalidate_ack_received_
 *   count / block_invalidate_timeout_count / block_x_forward_sent_
 *   count / block_x_granted_from_holder_count / starvation_denied_
 *   pending_x_count.
 * 4 NEW inject points (106→110):  cluster-gcs-block-invalidate-drop-
 *   broadcast + cluster-gcs-block-invalidate-stall-ack + cluster-gcs-
 *   block-x-forward-master-side + cluster-gcs-block-starvation-force-
 *   denied.  catversion bump for catalog tooling + wire ABI extension
 *   + GrdEntry sizeof bump. */
/* spec-2.37 D10 (2026-05-20):  PI simplified + lost-write detection
 * (page_lsn watermark MVP).
 * NEW GcsBlockReplyStatus value DENIED_LOST_WRITE=12 (enum 11→12);
 *   master direct ship 自校 失败 OR holder forward validate 失败
 *   都映射到这个 status,sender 走 HC131 terminal 53R93.
 * GrdEntry layout extends 248→256 (+8B): new pi_watermark_lsn uint64
 *   (InvalidXLogRecPtr=0 默认;single max-historical 模型 cover lost-
 *   write detection;Path X MVP 用 page_lsn 而非 pd_block_scn,后者
 *   真写入留独立 spec).  Inserted between pending_x_since_lsn and
 *   wait_cv/entry_lock to keep LWLockPadded must-stay-last invariant.
 * GcsBlockForwardPayload reserved_0[15] 重解读 (sizeof 64B 不变):
 *   first 8B 重解读为 expected_pi_watermark_lsn_bytes[8] at offset 49
 *   (little-endian uint64);剩余 7B 保留 reserved_0[7].  Same HC109
 *   pattern as spec-2.35 forwarding_master_node_bytes[4].
 * NEW HC125 (page_lsn = PI watermark MVP) + HC126 (single field max-
 *   historical) + HC127 (forward path 携 expected) + HC128 (holder/
 *   master validate before ship) + HC129 (DENIED_LOST_WRITE status) +
 *   HC130 (retire only by tag lifecycle + storage durable-confirm,
 *   forbidden by epoch advance) + HC131 (sender 仅映射 SQLSTATE).
 * 0 NEW wait events (CLUSTER_WAIT_EVENTS_COUNT 保持 88;  lost-write
 *   check 是非阻塞 uint64 比较,不是 waiting state).
 * 1 NEW GUC:  cluster.gcs_block_lost_write_action enum {error,warn}
 *   default error;  warn 仅 staging/diagnostic 用,production 必须 error.
 * 1 NEW SQLSTATE:  53R93 cluster_lost_write_detected.
 * 4 NEW counters exposed via dump_gcs (44→48 rows):
 *   pi_watermark_advance_count / pi_watermark_retire_count /
 *   lost_write_detected_count / lost_write_avoid_count.
 * 2 NEW inject points (110→112):  cluster-gcs-block-stale-ship
 *   (master direct ship 强制 page_lsn=0 模拟 stale source) +
 *   cluster-gcs-block-force-pi-watermark (master 端强制 high pi_
 *   watermark 用于 negative TAP).
 * catversion bump for catalog tooling + wire ABI reserved 重解读 +
 * GrdEntry sizeof bump + reply status extension. */
/* spec-2.38 D7 (2026-05-20):  SI Broadcaster skeleton — 真激活
 * PGRAC_IC_MSG_SINVAL=7 wire msg type + B_SINVAL_BCAST aux process +
 * LMON-mediated outbound fanout + 3 wait events (all 占位至
 * spec-2.38 起 wire-up real).
 * NEW SinvalBroadcastHeader 24B fixed prefix + variable-length
 *   N × SharedInvalidationMessage (16B each, HC137 PG ABI 锁) tail;
 *   envelope.payload_length = 24 + 16 * nmsgs.
 * NEW 2 shmem regions:  ClusterSinvalOutbound + ClusterSinvalInbound
 *   (ring buffer + LWLockPadded;  capacity = cluster.sinval_broadcast_
 *   max_queue_size default 1024).
 * NEW AuxProcType SinvalBcastProcess (inbound apply/reset owner;
 *   AuxiliaryProcessMain dispatch;  postmaster Phase 4 spawn).
 * NEW public API cluster_sinval_enqueue_batch() — only outbound entry
 *   point;  returns bool (HC134 fail-closed,禁 silent drop).
 * NEW IC handler cluster_sinval_handle_envelope — checksum L164 + epoch
 *   HC100 + source_node HC135 三层校验 + nonblocking try-enqueue
 *   (LWLockConditionalAcquire only) + SetLatch;  inbound full → set
 *   inbound_overflow_reset_pending flag,SI Broadcaster aux proc 执行
 *   SIResetAll() fail-safe.
 * NEW HC132 outbound queue 独立(防 echo loop;唯一硬防线)+ HC133 IC
 *   handler nonblocking 约束 + HC134 fail-closed/fail-safe + HC135
 *   source_node 辅助 echo defense + HC136 main loop drain pattern +
 *   HC137 SharedInvalidationMessage sizeof 锁 + HC138 wire ABI variable-
 *   length tail + HC139 producer mask/AuxProcType 双注册.
 * NEW 3 GUC:  cluster.sinval_broadcast_batch_size PGC_POSTMASTER 32
 *   (1..CLUSTER_SINVAL_BATCH_MAX check hook) + cluster.sinval_broadcast_
 *   batch_timeout_ms PGC_SIGHUP 10 + cluster.sinval_broadcast_max_queue_
 *   size PGC_POSTMASTER 1024.
 * NEW 1 SQLSTATE:  53R94 cluster_sinval_queue_full (caller of
 *   cluster_sinval_enqueue_batch maps to this on false return).
 * NEW dump_sinval category +9 counter rows.
 * NEW 2 inject points (110→112):  cluster-sinval-broadcast-drop-send +
 *   cluster-sinval-receive-skip-validate.
 * catversion bump for catalog tooling + wire ABI msg_type 7 真激活 +
 * 2 shmem regions + new aux process boundary. */
/* spec-3.2 hardening (2026-05-22): add test-only
 * cluster_test_inject_visibility_tt_ref / cluster_test_clear_visibility_injects
 * pg_proc rows so TAP can drive a real HeapTupleSatisfiesMVCC cluster-path
 * miss and assert 53R97.  Production builds link FEATURE_NOT_SUPPORTED
 * stubs; --enable-injection-points builds wire the shmem implementation. */
/* spec-3.3 D1 (2026-05-23): SnapshotData explicit 24B cluster tail
 * (SCN read_scn + uint64 read_epoch + uint8 cluster_source + uint8 _pad[7]).
 * sizeof(SnapshotData) bump → catalog tooling that reads serialized snapshot
 * payloads (snapbuild.c / pg_export_snapshot / parallel worker carry) must
 * use the new layout.  R4 P1 explicit layout + R9 P2 uint64 epoch (no
 * uint32 wrap alias).  See spec-3.3 §2.1 + D4 6-root ABI ripple audit. */
#define CATALOG_VERSION_NO 202605480

/* spec-2.39 D10 (2026-05-21):  SI Broadcaster production activation —
 * DDL commit hook (AtEOXact_Inval + COMMIT PREPARED via cluster-aware
 * wrapper) + peer_enqueued ack/barrier (3-status enum DONE/DROPPED/
 * RESET_PENDING) + fanout 3-partial-fail counter + RESET-all broadcast
 * fail-safe (v0.3 P1 SINVAL_RESET_ALL_BROADCAST flag走 msg_type 7 复用).
 * NEW IC msg_type 19 SINVAL_ACK (HC140 LMON-only producer mask;
 * SinvalAckHeader 24B fixed).
 * NEW 2 shmem regions:  ClusterSinvalAckWait (HTAB capacity GUC) +
 *   ClusterSinvalAckOutbound (small ring).
 * NEW 3 GUC:  cluster.sinval_ack_mode (enum none/peer_enqueued PGC_
 *   SIGHUP default peer_enqueued) + cluster.sinval_ack_timeout_ms (int
 *   PGC_SIGHUP 5000 [100, 60000]) + cluster.sinval_ack_wait_slots (int
 *   PGC_POSTMASTER 256 [64, 4096]).
 * NEW 1 SQLSTATE:  53R95 cluster_sinval_ack_timeout (WARN-path).
 * NEW 3 wait events:  SinvalAckWait / SinvalAckSend / SinvalAckReceive.
 * NEW 6 counter (3 fanout would_block/hard_error/peer_down + 3 ack
 *   received/timeout/orphan);  dump_sinval category 9 → 15 rows.
 * NEW 2 inject points (112→114):  cluster-sinval-ack-drop-send +
 *   cluster-sinval-ack-skip-validate.
 * v0.3 P1 wire ABI extension:  SINVAL_RESET_ALL_BROADCAST flag in
 *   existing msg_type 7 SinvalBroadcastHeader.flags (nmsgs=0 sentinel
 *   batch);remote handler 见此 flag → SIResetAll + bump existing
 *   inbound_overflow_reset_count (spec-2.38).  SINVAL_KNOWN_FLAGS
 *   扩 2 bits;HC135 validation refit.
 * catversion bump for catalog tooling + dump_sinval row count change. */

/* spec-2.16 D19 (2026-05-29):  GesRequestPayload + GesReplyPayload wire
 * payload structs (48B each + StaticAssertDecl);  ClusterGrdHolderId
 * 4-tuple typedef (24B);  cluster_grd 9 NEW counter (4 cap + 5 nofail);
 * cluster_grd_pending + cluster_grd_outbound + cluster_grd_work_queue
 * shmem regions + LWLock tranches;  cluster.ges_request_timeout_ms GUC;
 * 53R70/53R71 SQLSTATE.  catversion bump for catalog tooling. */
#define CATALOG_VERSION_NO_PRIOR 202605410

#endif
