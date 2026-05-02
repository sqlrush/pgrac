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
#define CATALOG_VERSION_NO	202605050

#endif
