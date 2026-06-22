/*-------------------------------------------------------------------------
 *
 * cluster_cf_enqueue.h
 *	  CF (control file) cluster enqueue: a singleton, whole-file S/X lock
 *	  on the shared pg_control authority, routed through the spec-5.3 GES
 *	  substrate (spec-5.6 Db1/Db2).
 *
 *	  Like the SQ resid (spec-5.4), CF uses a dedicated high resid type
 *	  byte so its enqueue lives in its own namespace, distinct from any PG
 *	  LockTagType.  The GES request/grant path only hashes the 16-byte
 *	  resid for shard/master routing and never decodes it back to a real
 *	  LOCKTAG, so a synthetic high type value is safe (spec §0.4 F0-15).
 *	  Unlike SQ (which shipped option B), CF genuinely enqueues through GES
 *	  because pg_control is not buffer-managed and cannot ride a shared
 *	  page (spec §0.4 F0-18) -- this is the faithful Oracle CF enqueue.
 *
 *	  CF is a singleton: one whole-file lock, all resid fields zero.  X
 *	  serializes writers; S gives a strong-consistency reader mutual
 *	  exclusion against a concurrent writer.  There is no convert (X and S
 *	  are taken and released independently, mirroring spec-5.5 advisory).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_cf_enqueue.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.6-cf-enqueue-shared-controlfile-authority.md (Db1/Db2)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CF_ENQUEUE_H
#define CLUSTER_CF_ENQUEUE_H

#include "cluster/cluster_grd.h"	  /* ClusterResId */
#include "cluster/cluster_sequence.h" /* CLUSTER_SQ_RESID_TYPE (collision check) */
#include "storage/lock.h"			  /* LOCKTAG_LAST_TYPE, LOCKMODE */

/*
 * CLUSTER_CF_RESID_TYPE -- CF resource-id namespace marker.  Must be above
 * every PG LockTagType (so it never collides with a real locktag) and
 * distinct from the SQ resid type.
 */
#define CLUSTER_CF_RESID_TYPE 0xF1

StaticAssertDecl(CLUSTER_CF_RESID_TYPE > LOCKTAG_LAST_TYPE,
				 "CF resid namespace must not collide with any PG LockTagType");
StaticAssertDecl(CLUSTER_CF_RESID_TYPE != CLUSTER_SQ_RESID_TYPE,
				 "CF and SQ resid namespaces must be distinct");

/*
 * cluster_cf_resid_encode -- build the singleton CF resource id.  All map
 * fields are zero (one whole-file lock); only the type byte distinguishes it.
 */
extern void cluster_cf_resid_encode(ClusterResId *dst);

/*
 * cluster_cf_lock / cluster_cf_unlock -- acquire/release the singleton CF
 * lock in the given mode (ShareLock for a strong-consistency read, or
 * ExclusiveLock for a writer) over the spec-5.3 GES substrate.
 *
 *	cluster_cf_lock builds a synthetic ClusterLockAcquireRequest for the CF
 *	resid and calls cluster_lock_acquire_seven_step directly -- bypassing
 *	the lock.c should_globalize gate (CF is not a PG LOCKTAG) -- then runs
 *	the S5 promote so the GRD holder is registered for cross-node conflict.
 *	It blocks (no dontwait) until granted or the GES request times out.
 *	Returns true once the lock is held (whether coordinated via GES or,
 *	when the cluster/LMS layer is inactive, locally), false when the lock
 *	could not be proven held so the caller must fail closed.  CF is not
 *	reentrant within a backend: do not call twice for the same mode without
 *	an intervening unlock.
 *
 *	cluster_cf_unlock releases the previously-held lock, draining and waking
 *	any blocked cross-node waiters (via S6 release).  A no-op if not held.
 */
extern bool cluster_cf_lock(LOCKMODE mode);
extern void cluster_cf_unlock(LOCKMODE mode);

/*
 * cluster_cf_held -- true if this backend currently holds the CF lock in the
 * given mode.  Used by the write path to Assert the caller-level CF X is held
 * before an authority write (spec-5.6 Db3 / §3.5 O2).
 */
extern bool cluster_cf_held(LOCKMODE mode);

/*
 * cluster_cf_write_permitted -- true if a shared-authority control-file write
 * is currently allowed: this backend holds CF X (the normal GES-ready path),
 * OR (spec-5.6 Db5) the bootstrap single-node-authority window is active
 * (sole-liveness proven and the storage rename-contract satisfied before GES
 * is ready).  Asserted in UpdateControlFile when the authority is enabled.
 */
extern bool cluster_cf_write_permitted(void);

/*
 * cluster_cf_set_bootstrap_authority -- spec-5.6 Db5.  Mark/clear the
 * bootstrap single-node-authority write window for this process.  When set,
 * cluster_cf_write_permitted() returns true without a held CF X (GES is not
 * yet ready during early recovery); the caller is responsible for having
 * proven sole-liveness and the storage contract first.
 */
extern void cluster_cf_set_bootstrap_authority(bool on);

/*
 * cluster_cf_in_bootstrap_window -- true while this process is the bootstrap
 * single-node authority.  A writer in the bootstrap window already has write
 * permission, so it must NOT also take CF X (GES is not ready during early
 * recovery); steady-state writers (the checkpointer) take CF X instead.
 */
extern bool cluster_cf_in_bootstrap_window(void);

/*
 * cluster_cf_set_join_readonly / cluster_cf_join_readonly -- spec-5.6 increment
 * (ii).  Mark this process as an attaching (join) node whose recovery runs
 * while a live peer owns the shared authority.  A join node must NOT write the
 * shared authority: its recovery-progress control-file writes are deliberately
 * skipped (not clobbering writes, ARCH DECISION #5).  This is a POSITIVE signal,
 * distinct from "write permission unexpectedly absent" (which is a bug and
 * fails closed), so the write path never silently drops a write it should have
 * made (规则 8.A: no silent fallback).
 */
extern void cluster_cf_set_join_readonly(bool on);
extern bool cluster_cf_join_readonly(void);

/*
 * cluster_cf_set_write_skip / cluster_cf_write_skip -- the PROCESS-LOCAL,
 * tightly-scoped flag the authority chokepoint (UpdateControlFile) actually
 * consults to skip a write.  It is set ONLY inside a genuine bring-up window:
 *   - the startup process sets it at the role gate for the duration of its
 *     recovery (it exits afterward, so the flag cannot leak into steady state);
 *   - CreateCheckPoint sets it per-checkpoint, true only for a bring-up
 *     skip-checkpoint (GES not ready, or the end-of-recovery checkpoint), false
 *     for every steady-state checkpoint (which then takes CF X).
 * The chokepoint never consults the node-wide shmem join_readonly flag, so a
 * lingering shmem flag can never silently skip a steady-state authority write
 * from some other path -- such a write hits the chokepoint with neither CF X
 * nor write-skip and fails closed (PANIC).  The shmem join_readonly flag is
 * used only by CreateCheckPoint to DECIDE the per-checkpoint skip cross-process.
 */
extern void cluster_cf_set_write_skip(bool on);
extern bool cluster_cf_write_skip(void);

#endif /* CLUSTER_CF_ENQUEUE_H */
