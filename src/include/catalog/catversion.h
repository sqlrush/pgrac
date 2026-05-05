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
#define CATALOG_VERSION_NO	202605181

#endif
