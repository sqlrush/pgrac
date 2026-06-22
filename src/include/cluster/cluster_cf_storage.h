/*-------------------------------------------------------------------------
 *
 * cluster_cf_storage.h
 *	  Establish and verify the shared pg_control authority on a node and
 *	  on the underlying shared storage (spec-5.6 Da2 / T6).
 *
 *	  Three concerns live here, separate from the authority file I/O in
 *	  cluster_cf_authority.c:
 *	    1. migration + symlink: turn a per-node $PGDATA/global/pg_control
 *	       into the shared authority plus a node-local symlink to it;
 *	    2. symlink status: classify what a node's local control path
 *	       actually is (symlink to the authority, a foreign per-node
 *	       regular file, wrong target, or missing);
 *	    3. the storage rename-contract: a two-phase probe (local, then
 *	       cross-node) that proves POSIX rename gives the cross-node
 *	       visibility the authority depends on, plus the pure write-gate
 *	       that forbids a single-node-authority write until that contract
 *	       is satisfied (spec §3.3 B5 / §3.9 T6).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_cf_storage.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.6-cf-enqueue-shared-controlfile-authority.md (Da2, T6)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CF_STORAGE_H
#define CLUSTER_CF_STORAGE_H

/*
 * Per-node $PGDATA file in which a node records its own storage rename-contract
 * verification, bound to the shared-storage identity it was proven against.
 * It is a REAL local file (only global/pg_control is symlinked to the shared
 * authority): each node trusts its own cross-node verification, which avoids a
 * shared-directory chicken-and-egg during bootstrap (the sole survivor reads
 * its own record).  See ARCH DECISION (Grow-from-single-node) and §3.3 B5.
 */
#define CLUSTER_CF_CONTRACT_REL_PATH "global/pgrac_cf_contract"

/*
 * ClusterCfContractState -- how far the storage rename-contract probe got
 * for the current (storage_uuid, mount).  Persisted so a node that already
 * cross-node-verified the storage need not re-prove it every start.
 *
 *	UNVERIFIED			never probed (or storage changed).
 *	LOCAL_PROBED		Phase-1 local rename probe passed; cross-node
 *						visibility NOT yet proven.
 *	CROSSNODE_VERIFIED	Phase-2 nonce+ack with a peer succeeded.
 */
typedef enum ClusterCfContractState {
	CLUSTER_CF_CONTRACT_UNVERIFIED = 0,
	CLUSTER_CF_CONTRACT_LOCAL_PROBED,
	CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED
} ClusterCfContractState;

/*
 * ClusterCfLiveness -- tri-state assessment of who else is live, used to pick a
 * node's bootstrap authority role.  Distinguishing PEER_ALIVE from UNKNOWN
 * matters (规则 8.A): a count that reads 0 because CSSD shmem is absent must
 * never be taken as "I am the only node".
 *
 *	UNKNOWN		CSSD not READY or not in quorum: liveness cannot be judged.
 *	SOLE		CSSD READY, in quorum, zero alive peers: this is the only node.
 *	PEER_ALIVE	CSSD READY, in quorum, at least one alive peer.
 */
typedef enum ClusterCfLiveness {
	CLUSTER_CF_LIVENESS_UNKNOWN = 0,
	CLUSTER_CF_LIVENESS_SOLE,
	CLUSTER_CF_LIVENESS_PEER_ALIVE
} ClusterCfLiveness;

/*
 * ClusterCfBootstrapRole -- a node's role for control-file authority during the
 * pre-GES bootstrap/recovery window (ARCH DECISION Grow-from-single-node, §3.3).
 *
 *	OWNER			open the bootstrap window; this node may write the shared
 *					authority (single-node cluster, or the verified sole-live
 *					node of a multi-node cluster).
 *	JOIN_READONLY	a live peer owns the authority; this attaching node runs
 *					recovery but MUST NOT write the shared authority -- its
 *					recovery-progress writes are skipped, not clobbering writes.
 *	FAILCLOSED		no safe role can be determined (liveness unknown, or a
 *					multi-node node that has never cross-node-verified the
 *					storage) -> the caller raises FATAL.
 */
typedef enum ClusterCfBootstrapRole {
	CLUSTER_CF_ROLE_OWNER = 0,
	CLUSTER_CF_ROLE_JOIN_READONLY,
	CLUSTER_CF_ROLE_FAILCLOSED
} ClusterCfBootstrapRole;

/*
 * ClusterCfSymlinkStatus -- what a node's $PGDATA/global/pg_control is.
 *
 *	OK				a symlink pointing at the expected shared authority.
 *	MISSING			the path does not exist.
 *	NOT_SYMLINK		a regular file -> a per-node local control file, which
 *					in cluster mode is a split-brain hazard (Da3 fail-closes).
 *	WRONG_TARGET	a symlink, but to some other path (foreign authority).
 */
typedef enum ClusterCfSymlinkStatus {
	CLUSTER_CF_SYMLINK_OK = 0,
	CLUSTER_CF_SYMLINK_MISSING,
	CLUSTER_CF_SYMLINK_NOT_SYMLINK,
	CLUSTER_CF_SYMLINK_WRONG_TARGET
} ClusterCfSymlinkStatus;

/*
 * ClusterCfStartupVerdict -- pure result of the Da3 startup gate: given how a
 * node's local control path classifies and whether the shared authority it
 * points at is readable and identity-matched, decide OK vs the specific
 * fail-closed reason.  Only meaningful when the authority feature is enabled
 * (cluster.controlfile_shared_authority = on); off -> PG-native, no gate.
 */
typedef enum ClusterCfStartupVerdict {
	CLUSTER_CF_STARTUP_OK = 0,
	CLUSTER_CF_STARTUP_FATAL_MISSING,	   /* no control path at all */
	CLUSTER_CF_STARTUP_FATAL_NOT_SYMLINK,  /* per-node local file (DoD-5) */
	CLUSTER_CF_STARTUP_FATAL_WRONG_TARGET, /* symlink to a foreign authority */
	CLUSTER_CF_STARTUP_FATAL_UNREADABLE,   /* authority torn/corrupt/unreadable */
	CLUSTER_CF_STARTUP_FATAL_IDENTITY	   /* authority readable, foreign sysid */
} ClusterCfStartupVerdict;

extern ClusterCfStartupVerdict cluster_cf_startup_verdict(ClusterCfSymlinkStatus link_status,
														  bool authority_readable,
														  bool identity_ok);

/*
 * cluster_cf_storage_write_allowed -- pure §3.3 B5 write-gate.
 *
 *	A single-node-authority write (taken when GES is not yet ready, e.g.
 *	recovery) is allowed only when this is a single-node cluster OR the
 *	storage contract has been cross-node verified.  A multi-node cluster
 *	that has not proven cross-node rename visibility must NOT write the
 *	shared authority even if it believes it is the sole live node, because
 *	the write might not be visible to a peer that later reattaches.
 */
extern bool cluster_cf_storage_write_allowed(ClusterCfContractState state, bool multi_node);

/*
 * cluster_cf_contract_resolve -- pure identity-bound resolution of a persisted
 * contract record (spec ARCH DECISION DoD#3, §3.3 B5).
 *
 *	A persisted CROSSNODE_VERIFIED is trustworthy only if it is still bound to
 *	the current shared-storage identity.  Returns CROSSNODE_VERIFIED iff the
 *	current storage uuid is known (non-empty), the persisted record names the
 *	same uuid, and the persisted state is exactly CROSSNODE_VERIFIED; every
 *	other case (no current identity, no record, a different uuid, or a weaker
 *	persisted state) resolves to UNVERIFIED so the B5 gate fails closed.  This
 *	deliberately collapses the load result to {VERIFIED, UNVERIFIED}: a bare
 *	LOCAL_PROBED never unblocks a multi-node authority write.
 */
extern ClusterCfContractState cluster_cf_contract_resolve(const char *persisted_uuid,
														  const char *current_uuid,
														  ClusterCfContractState persisted_state);

/*
 * cluster_cf_contract_persist -- atomically record `state` and the current
 * shared-storage uuid into $pgdata/global/pgrac_cf_contract (CRC-protected,
 * write-tmp + fsync + durable_rename).  Returns false on any I/O failure
 * without ereport, so a caller (Phase-2) can fail closed -- an unrecorded
 * verification simply re-runs on the next start.
 */
extern bool cluster_cf_contract_persist(const char *pgdata, ClusterCfContractState state);

/*
 * cluster_cf_contract_load -- read $pgdata/global/pgrac_cf_contract and resolve
 * it against the current shared-storage identity.  A missing, short, or
 * CRC-failing record reads as UNVERIFIED (fail-closed; never throws).  This is
 * what the bootstrap window-gate consults instead of a hardcoded contract
 * state.
 */
extern ClusterCfContractState cluster_cf_contract_load(const char *pgdata);

/*
 * cluster_cf_verify_sole_liveness_or_fail -- spec §3.3 B4 positive proof that
 * this is the only live node, safe to take single-node authority before GES
 * is ready.  Requires ALL of: CSSD status == READY, qvotec in quorum, and
 * zero alive peers.  Any not-ready / uncertain condition returns false
 * (fail-closed).  CSSD READY is checked first so a zero alive-peer count is
 * only trusted once CSSD shmem + membership are actually up -- never infer
 * sole-liveness from a count that reads 0 because the shmem is absent (F0-26).
 */
extern bool cluster_cf_verify_sole_liveness_or_fail(void);

/*
 * cluster_cf_assess_liveness -- tri-state liveness from CSSD + qvotec (see
 * ClusterCfLiveness).  cluster_cf_verify_sole_liveness_or_fail() is the
 * boolean "== SOLE" view of this.
 */
extern ClusterCfLiveness cluster_cf_assess_liveness(void);

/*
 * cluster_cf_bootstrap_role -- pure mapping of (config, liveness, contract) to
 * the authority role this node may take during bootstrap/recovery.  A
 * single-node cluster is always OWNER.  A multi-node node is OWNER only if it
 * is the verified sole-live node, JOIN_READONLY if a peer is alive AND the
 * storage was cross-node verified (so it can trust what it reads), and
 * FAILCLOSED otherwise (liveness unknown, or never cross-node verified).
 */
extern ClusterCfBootstrapRole cluster_cf_bootstrap_role(bool multi_node, ClusterCfLiveness liveness,
														ClusterCfContractState contract);

/*
 * cluster_cf_bootstrap_authority_gate -- spec §3.3 B3/B5 write gate for the
 * bootstrap single-node-authority window: returns true only when sole-liveness
 * is proven (B4) AND the storage contract permits a single-node-authority
 * write (single-node cluster, or cross-node rename visibility verified).  A
 * multi-node cluster that has never cross-node-verified the storage returns
 * false even if it believes it is the sole live node.
 */
extern bool cluster_cf_bootstrap_authority_gate(bool multi_node, ClusterCfContractState contract);

/*
 * cluster_cf_enter_bootstrap_window_or_fail -- spec §3.3 B2/B3 bootstrap
 * entry, called once from the startup process before its first authority
 * write (control-file writes during recovery happen before GES is ready).
 *
 *	A no-op when the authority is off.  For a single-node cluster (or with
 *	cluster.enabled off) this node is trivially the sole authority, so the
 *	Phase-1 storage rename probe is run and the bootstrap write window is
 *	opened.  For a multi-node cluster it defers to the B3/B5 gate, which
 *	fails closed until the storage has been cross-node verified (Phase-2);
 *	a failure raises FATAL rather than risk a split-brain control-file
 *	write.  Steady-state writes after startup use CF X instead.
 */
extern void cluster_cf_enter_bootstrap_window_or_fail(void);

/*
 * cluster_cf_symlink_status -- lstat()+readlink() classify local_path
 * against the expected shared-authority target.
 */
extern ClusterCfSymlinkStatus cluster_cf_symlink_status(const char *local_path,
														const char *expected_target);

/*
 * cluster_cf_storage_probe_local -- Phase-1 probe in the shared global/
 * directory: write a nonce to a temp file, fsync, durable_rename it, fsync
 * the directory, then reopen and verify the nonce survived.  Returns true
 * when the storage honours the rename primitive locally.  Does not prove
 * cross-node visibility (that is Phase-2).
 */
extern bool cluster_cf_storage_probe_local(void);

/*
 * cluster_cf_migrate_and_link -- if the shared authority does not yet exist,
 * create it from this node's local pg_control; then replace the local
 * $PGDATA/global/pg_control with a symlink to the shared authority and
 * verify the post-link identity matches.  Idempotent: a node whose local
 * path is already the correct symlink is left untouched.  Returns true on
 * success, false on a mismatch the caller must fail-close on.
 */
extern bool cluster_cf_migrate_and_link(const char *local_pgdata);

/*
 * cluster_cf_startup_prepare -- Da3 startup hook, called once from the
 * postmaster before LocalProcessControlFile.  A no-op when
 * cluster.controlfile_shared_authority is off.  When on: migrate this node's
 * global/pg_control into the shared authority and symlink to it, then verify
 * via cluster_cf_startup_verdict and ereport(FATAL,
 * ERRCODE_CLUSTER_CONTROLFILE_AUTHORITY_UNAVAILABLE) on any mismatch so the
 * node never starts on a per-node, foreign, or torn control file.
 */
extern void cluster_cf_startup_prepare(const char *pgdata);

#endif /* CLUSTER_CF_STORAGE_H */
