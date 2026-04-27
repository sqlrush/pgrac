/*-------------------------------------------------------------------------
 *
 * cluster_shmem.h
 *	  pgrac cluster shared-memory entry points and control block.
 *
 *	  This header is the single source of truth for cluster shmem C
 *	  declarations.  Stage 0.14 introduces the registration mechanism
 *	  and allocates the first cluster shmem region, ClusterShmemCtl --
 *	  a small immutable control block holding magic, version, and
 *	  startup metadata.  Future cluster shmem regions (GRD, PCM, GES,
 *	  ...) land here at the same time their owning subsystem spec is
 *	  implemented; see docs/cluster-shmem-design.md §3.1 for the
 *	  Single Source of Truth registration roster.
 *
 *	  Responsibilities of this header:
 *
 *	  - Declare the ClusterShmemCtl struct + CLUSTER_SHMEM_MAGIC.
 *	  - Declare the public entry points cluster_request_shmem() and
 *	    cluster_init_shmem(), which PG core (miscinit.c / ipci.c) calls
 *	    under #ifdef USE_PGRAC_CLUSTER during postmaster startup.
 *	  - Declare cluster_shmem_size() for diagnostics.
 *	  - Export the global ClusterShmem pointer so cluster code can read
 *	    metadata fields without going through SQL.
 *
 *	  Stage 0.14 registers ONLY the control block.  ~12 future shmem
 *	  regions (GRD / PCM / GES / Buffer ship / SCN / Sinval / Heartbeat
 *	  / Interconnect / Undo / TT / ADG / Reconfig) are reserved in the
 *	  design doc and registered together with their owning subsystem
 *	  spec (CLAUDE.md rule 8).
 *
 *	  Locking: stage 0.14 control block fields are immutable after
 *	  postmaster init (no concurrent writes), so no LWLock is required.
 *	  The first subsystem that needs concurrent shmem writes registers
 *	  its own LWLock tranche at that time.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_shmem.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Includes datatype/timestamp.h for TimestampTz; otherwise PG-free
 *	  at the declaration level.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SHMEM_H
#define CLUSTER_SHMEM_H

#include "datatype/timestamp.h" /* TimestampTz */


/*
 * CLUSTER_SHMEM_MAGIC -- "PGRC" little-endian.
 *
 *	First 4 bytes of every cluster shmem region.  Readers verify the
 *	magic before trusting the rest of the structure; mismatch usually
 *	means stale shmem segment from a previous binary.
 */
#define CLUSTER_SHMEM_MAGIC 0x50475243UL


/*
 * ClusterShmemCtl -- top-level cluster control block.
 *
 *	One instance per postmaster, allocated by cluster_init_shmem() and
 *	never written again.  Holds version + identity metadata visible to
 *	any cluster code (e.g. for sanity checks during cross-process
 *	communication).
 *
 *	When future subsystems (GRD, PCM, ...) need their own shmem regions,
 *	they allocate independent ShmemInitStruct entries -- they do NOT
 *	embed mutable fields here, because mutable state would require
 *	locking that the control block intentionally avoids.
 */
typedef struct ClusterShmemCtl {
	uint32 magic;			/* CLUSTER_SHMEM_MAGIC */
	uint32 version_packed;	/* (major<<24)|(minor<<16)|(patch<<8)|stage_step */
	int32 node_id_at_init;	/* snapshot of cluster_node_id GUC at init */
	int32 _padding;			/* keep created_at 8-byte aligned */
	TimestampTz created_at; /* GetCurrentTimestamp() at init */
} ClusterShmemCtl;


/*
 * Public entry points called by PG core under #ifdef USE_PGRAC_CLUSTER.
 *
 *	cluster_request_shmem() must run inside process_shmem_requests()
 *	(miscinit.c) so that PG accepts the RequestAddinShmemSpace() call.
 *	cluster_init_shmem() must run inside CreateSharedMemoryAndSemaphores()
 *	(ipci.c) after PG's built-in ShmemInit calls and before the user
 *	shmem_startup_hook fires.
 *
 *	cluster_shmem_size() is a helper that returns the total byte count
 *	the cluster subsystem will request -- useful for diagnostics and
 *	future capacity planning.
 */
extern void cluster_request_shmem(void);
extern void cluster_init_shmem(void);
extern Size cluster_shmem_size(void);


/*
 * Process-local pointer to the cluster control block.
 *
 *	NULL until cluster_init_shmem() runs in this process.  After that,
 *	it points into the shared memory segment and stays valid for the
 *	process lifetime.  Never written after initialisation.
 */
extern ClusterShmemCtl *ClusterShmem;


#endif /* CLUSTER_SHMEM_H */
