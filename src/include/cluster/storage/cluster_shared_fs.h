/*-------------------------------------------------------------------------
 *
 * cluster_shared_fs.h
 *	  pgrac cluster shared-storage abstraction layer (Stage 1.1 skeleton).
 *
 *	  Declares the vtable contract that every pgrac storage backend
 *	  (stub, local, block_device, cluster_fs, rbd, multi_attach, ...)
 *	  implements.  Stage 1.1 ships two built-in backends: stub (always
 *	  ereports FEATURE_NOT_SUPPORTED) and local (single-node passthrough
 *	  to PG's existing fd.c VFD layer).  Stage 1.2 wires this vtable
 *	  into PG's smgrsw[] via cluster-aware smgr; Stage 2+ registers the
 *	  four real cluster backends.  The API surface declared here stays
 *	  unchanged across that evolution.
 *
 *	  Backend selection is start-time only: the cluster.shared_storage_backend
 *	  GUC (PGC_POSTMASTER) picks one of the registered backends; the
 *	  active vtable pointer is then frozen for the postmaster's
 *	  lifetime.  Switching backends requires a restart.
 *
 *	  vtable callbacks intentionally mirror PG's f_smgr signature shape
 *	  (open / close / read / write / extend / nblocks / truncate /
 *	  immedsync / unlink) so that stage-1.2 smgr_cluster.c can forward
 *	  through with no impedance mismatch.
 *
 *	  See docs/cluster-shared-fs-design.md for the full design
 *	  rationale, backend ID assignment table, and Stage evolution path;
 *	  specs/spec-1.1-shared-fs-skeleton.md for the stage-1.1 scope and
 *	  exit criteria.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/storage/cluster_shared_fs.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  The cluster_shared_fs_* symbols are available only when
 *	  configured with --enable-cluster (USE_PGRAC_CLUSTER defined);
 *	  call sites must be guarded.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SHARED_FS_H
#define CLUSTER_SHARED_FS_H

#include "storage/block.h"
#include "storage/relfilelocator.h"
#include "storage/smgr.h" /* for ForkNumber */


/*
 * ClusterSharedFsBackendId -- enum index into the backend registry.
 *
 *	The numeric assignment is a public ABI: the cluster.shared_storage_backend
 *	GUC enum maps positionally onto these values, and the registry
 *	array is indexed by them.  Re-numbering breaks the GUC contract;
 *	new backends must occupy the next free slot (6, 7, ...) only.
 *
 *	16-slot reservation rationale: 6 production backends planned
 *	(stub / local / block_device / cluster_fs / rbd / multi_attach),
 *	plus 10 future-extension slots for test backends, hybrid backends,
 *	and cloud-vendor-specific variants.  See
 *	docs/cluster-shared-fs-design.md §3 for the SSOT table.
 */
typedef enum ClusterSharedFsBackendId {
	CLUSTER_SHARED_FS_BACKEND_STUB = 0,
	CLUSTER_SHARED_FS_BACKEND_LOCAL = 1,
	CLUSTER_SHARED_FS_BACKEND_BLOCK_DEVICE = 2,
	CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS = 3,
	CLUSTER_SHARED_FS_BACKEND_RBD = 4,
	CLUSTER_SHARED_FS_BACKEND_MULTI_ATTACH = 5
	/* IDs 6..15 are reserved; do not reuse without bumping the GUC enum. */
} ClusterSharedFsBackendId;

#define CLUSTER_SHARED_FS_BACKEND_MAX 16


/*
 * ClusterSharedFsHandle -- opaque per-(rlocator, forknum) handle
 *	returned by open().  Each backend defines its own underlying
 *	struct; callers must not deref.  Stage 1.1: stub never returns a
 *	handle (ereports first); local stores a fd.c File and a copy of
 *	the locator/fork.
 */
typedef struct ClusterSharedFsHandle ClusterSharedFsHandle;

/*
 * ClusterSharedFsCaps -- backend capability descriptor.
 *
 * This is process-local metadata, not an on-disk format.  The byte layout is
 * still pinned so production backends added after spec-6.0a can reason about a
 * stable provider contract.
 */
typedef enum ClusterDurabilityClass {
	CLUSTER_DURABILITY_BUFFERED = 0,
	CLUSTER_DURABILITY_ODIRECT_BARRIER = 1,
	CLUSTER_DURABILITY_NONE = 2,
} ClusterDurabilityClass;

typedef enum ClusterFenceCapability {
	CLUSTER_FENCE_CAP_NONE = 0,
	CLUSTER_FENCE_CAP_SCSI3_PR = 1,
} ClusterFenceCapability;

typedef struct ClusterSharedFsCaps {
	bool supports_odirect;		  /* offset 0 */
	uint8 _pad0[3];				  /* offset 1 */
	uint32 required_io_alignment; /* offset 4; 0 = buffered/no special alignment */
	bool supports_scsi3_pr;		  /* offset 8 */
	uint8 durability_class;		  /* offset 9; ClusterDurabilityClass value */
	uint16 max_nodes;			  /* offset 10 */
	uint16 _pad;				  /* offset 12 */
	uint16 _pad1;				  /* offset 14; keep sizeof == 16 */
} ClusterSharedFsCaps;

StaticAssertDecl(sizeof(ClusterSharedFsCaps) == 16, "ClusterSharedFsCaps ABI must stay 16 bytes");
StaticAssertDecl(offsetof(ClusterSharedFsCaps, required_io_alignment) == 4,
				 "ClusterSharedFsCaps.required_io_alignment offset changed");
StaticAssertDecl(offsetof(ClusterSharedFsCaps, durability_class) == 9,
				 "ClusterSharedFsCaps.durability_class offset changed");


/*
 * ClusterSharedFsOps -- vtable.
 *
 *	Eleven storage callbacks plus two lifecycle callbacks, thirteen
 *	function pointers total.  Every member must be non-NULL when
 *	registered; cluster_shared_fs_register_backend rejects partial
 *	implementations to make link-time auditing clean.
 *
 *	Semantic invariants (see docs/cluster-shared-fs-design.md §2.2):
 *	  - read / write operate at BLCKSZ granularity; no partial I/O
 *	  - extend zero-fills the new page (PG semantics)
 *	  - immedsync is synchronous; returns only after persistence
 *	  - unlink takes RelFileLocator (caller has already closed handle)
 *
 *	Spec-1.X-cluster-smgr-hardening Sprint A 2026-05-02: vtable split
 *	`open` (ambiguous create-or-open with O_CREAT side effect) into 3
 *	separate callbacks:
 *	  - exists():       check whether the relation file exists; no I/O
 *	  - open_existing(): open an existing file; ENOENT if missing
 *	  - create(rlocator, forknum, isRedo, &handle):
 *	                    create a new file; matches md.c mdcreate()
 *	                    semantics:
 *	                      !isRedo -> O_CREAT|O_EXCL: error on existing
 *	                      isRedo  -> idempotent: existing file is OK
 *	                    Stage 2 共享存储 spec defines whether shared
 *	                    backends use idempotent CAS-create or O_EXCL
 *	                    semantics.
 *	The legacy single `open` callback is removed since cluster_smgr is
 *	the only caller; cluster_smgr_exists / cluster_smgr_open / cluster_
 *	smgr_create call the appropriate split callback directly.  Stage 2
 *	真后端 (NFS / S3 / Multi-Attach) requires this split because there
 *	is no "local path stat()" fallback for cluster_smgr_exists hack.
 *
 *	Spec-1.7.2-cluster-smgr-warning-create-lifecycle 2026-05-03:
 *	`create` callback signature extended with `bool isRedo` parameter
 *	to match PG md.c mdcreate (see md.c:218).  Internal ABI bugfix-
 *	level amend; total still thirteen function pointers.
 */
typedef struct ClusterSharedFsOps {
	const char *name; /* "stub" / "local" / ... */
	ClusterSharedFsBackendId id;
	const ClusterSharedFsCaps *caps;

	/* Existence + Open + Create (split for vtable契约清晰；Sprint A 2026-05-02;
	 * create(isRedo) signature extended Sprint round 2 2026-05-03 spec-1.7.2). */
	bool (*exists)(RelFileLocator rlocator, ForkNumber forknum);
	void (*open_existing)(RelFileLocator rlocator, ForkNumber forknum,
						  ClusterSharedFsHandle **out_handle);
	void (*create)(RelFileLocator rlocator, ForkNumber forknum, bool isRedo,
				   ClusterSharedFsHandle **out_handle);

	/* Core I/O. */
	void (*close)(ClusterSharedFsHandle *handle);
	int (*read)(ClusterSharedFsHandle *handle, BlockNumber blocknum, char *buf);
	int (*write)(ClusterSharedFsHandle *handle, BlockNumber blocknum, const char *buf);
	void (*extend)(ClusterSharedFsHandle *handle, BlockNumber blocknum);
	BlockNumber (*nblocks)(ClusterSharedFsHandle *handle);
	void (*truncate)(ClusterSharedFsHandle *handle, BlockNumber nblocks);
	void (*immedsync)(ClusterSharedFsHandle *handle);
	void (*unlink)(RelFileLocator rlocator, ForkNumber forknum);

	/* Lifecycle. */
	void (*init)(void);		/* called once after register */
	void (*shutdown)(void); /* called at postmaster exit */

	/* Production-backend extensions (spec-6.0a). */
	int (*barrier_sync)(ClusterSharedFsHandle *handle);
	int (*register_fence_key)(int node_id);
	ClusterFenceCapability (*fence_capability)(void);
} ClusterSharedFsOps;


/* ----------
 * Lifecycle entry points
 *
 *	cluster_shared_fs_init -- called by cluster_init() after
 *	    cluster_init_shmem.  Registers the built-in stub and local
 *	    backends, then resolves the cluster.shared_storage_backend
 *	    GUC value to the active vtable.  FATAL if the GUC names a
 *	    backend that has not been registered.
 *
 *	cluster_shared_fs_shutdown -- before_shmem_exit callback.  Calls
 *	    active_ops->shutdown() if active_ops is set; safe to call when
 *	    no backend was activated.
 * ----------
 */
extern void cluster_shared_fs_init(void);
extern void cluster_shared_fs_shutdown(void);


/* ----------
 * Backend self-registration
 *
 *	Each backend module exposes a static const ClusterSharedFsOps and
 *	calls cluster_shared_fs_register_backend(&that_struct) from inside
 *	cluster_shared_fs_init.  Stage 1.1 has two registrations (stub +
 *	local); Stage 2+ backend modules add theirs the same way.
 *
 *	Errors:
 *	  - duplicate id          -> WARNING; first registration wins
 *	  - id out of range       -> FATAL
 *	  - any callback NULL     -> FATAL
 *	  - called outside init   -> FATAL
 * ----------
 */
extern void cluster_shared_fs_register_backend(const ClusterSharedFsOps *ops);


/* ----------
 * Active backend accessors
 *
 *	cluster_shared_fs_get_active_ops -- returns the vtable that
 *	    cluster.shared_storage_backend selected.  Set during
 *	    cluster_shared_fs_init; read-only and lock-free thereafter.
 *	    Returns NULL only if cluster_shared_fs_init has not yet run
 *	    (e.g. very early postmaster code or disable-cluster builds).
 *
 *	cluster_shared_fs_get_registered_count -- returns how many
 *	    backends have been registered (used by diagnostic dumps and
 *	    cluster_unit assertions).
 *
 *	cluster_shared_fs_get_backend_at -- returns the vtable at a given
 *	    backend id slot, or NULL if unregistered.  Used by the
 *	    cluster_dump_state diagnostic and the unit-test surface.
 * ----------
 */
extern const ClusterSharedFsOps *cluster_shared_fs_get_active_ops(void);
extern const ClusterSharedFsCaps *cluster_shared_fs_get_active_caps(void);
extern int cluster_shared_fs_get_registered_count(void);
extern const ClusterSharedFsOps *cluster_shared_fs_get_backend_at(int id);


/* ----------
 * Caller-facing I/O API
 *
 *	Thin dispatch wrappers around active_ops->*.  Provided so that
 *	callers don't need to fetch the vtable themselves and so that
 *	backend selection logging / wait-event accounting can live in one
 *	place when stage 6+ wires those in.  Stage 1.1: pure dispatch.
 * ----------
 */
/*
 * Existence + open/create wrappers (split per Sprint A 2026-05-02
 * spec-1.X-cluster-smgr-hardening).  Replace the old single
 * cluster_shared_fs_open which had ambiguous create-or-open semantics
 * with O_CREAT side effect.  Callers (cluster_smgr_exists / cluster_
 * smgr_open / cluster_smgr_create) MUST use the appropriate variant.
 */
extern bool cluster_shared_fs_exists(RelFileLocator rlocator, ForkNumber forknum);
extern void cluster_shared_fs_open_existing(RelFileLocator rlocator, ForkNumber forknum,
											ClusterSharedFsHandle **out_handle);
/*
 * Spec-1.7.2 2026-05-03: create() takes isRedo to match md.c mdcreate
 * semantics.  !isRedo -> O_CREAT|O_EXCL (error on existing); isRedo ->
 * idempotent (existing OK).  Internal ABI bugfix-level amend.
 */
extern void cluster_shared_fs_create(RelFileLocator rlocator, ForkNumber forknum, bool isRedo,
									 ClusterSharedFsHandle **out_handle);
extern void cluster_shared_fs_close(ClusterSharedFsHandle *handle);
extern int cluster_shared_fs_read(ClusterSharedFsHandle *handle, BlockNumber blocknum, char *buf);
extern int cluster_shared_fs_write(ClusterSharedFsHandle *handle, BlockNumber blocknum,
								   const char *buf);
extern void cluster_shared_fs_extend(ClusterSharedFsHandle *handle, BlockNumber blocknum);
extern BlockNumber cluster_shared_fs_nblocks(ClusterSharedFsHandle *handle);
extern void cluster_shared_fs_truncate(ClusterSharedFsHandle *handle, BlockNumber nblocks);
extern void cluster_shared_fs_immedsync(ClusterSharedFsHandle *handle);
extern void cluster_shared_fs_unlink(RelFileLocator rlocator, ForkNumber forknum);
extern int cluster_shared_fs_barrier_sync(ClusterSharedFsHandle *handle);
extern int cluster_shared_fs_register_fence_key(int node_id);
extern ClusterFenceCapability cluster_shared_fs_fence_capability(void);


/*
 * Internal: built-in vtable instances exposed for cluster_shared_fs_init's
 * registration path and for cluster_unit linkage assertions.  Backend
 * implementation modules define these in their own .c file.
 */
extern const ClusterSharedFsOps cluster_shared_fs_stub_ops;
extern const ClusterSharedFsOps cluster_shared_fs_local_ops;
/* Stage 6.0a: production raw block-device backend. */
extern const ClusterSharedFsOps cluster_shared_fs_block_device_ops;
/* Stage 4.5a (spec-4.5a D1): first genuinely cross-node-shared backend. */
extern const ClusterSharedFsOps cluster_shared_fs_sharedfs_ops;

/*
 * Length of the shared-storage uuid recorded in the cross-node sentinel:
 * 32 lowercase hex characters plus a NUL.  Public so other cluster modules
 * (e.g. the spec-5.6 CF storage-contract record) can size matching buffers
 * without duplicating the literal.
 */
#define CLUSTER_SHARED_UUID_LEN 33

/*
 * Stage 4.5a D2: cross-node shared-root sentinel (shared_fs backend).
 *
 *	cluster_shared_fs_sentinel_attach records cluster.node_id in the
 *	<shared_data_dir>/pgrac_shared.control participant set (postmaster-
 *	once, from the shared_fs init callback).
 *
 *	cluster_shared_fs_sentinel_has_participant reports whether node_id is
 *	a participant; the merged-recovery capability gate uses it to refuse
 *	to merge a crashed peer that never wrote to this shared root.  A
 *	missing/corrupt sentinel returns false (fail-closed).
 *
 *	cluster_shared_fs_get_storage_uuid copies the recorded storage uuid
 *	into `out` (writes "" when the sentinel is absent/corrupt or
 *	cluster.shared_data_dir is unset).  The spec-5.6 CF storage contract
 *	binds its cross-node verification to this identity so a storage swap
 *	invalidates the recorded verification (fail-closed).
 */
extern void cluster_shared_fs_sentinel_attach(void);
extern bool cluster_shared_fs_sentinel_has_participant(int node_id);
extern void cluster_shared_fs_get_storage_uuid(char *out, size_t outlen);


#endif /* CLUSTER_SHARED_FS_H */
