/*-------------------------------------------------------------------------
 *
 * cluster_lms_shard.h
 *	  pgrac LMS worker-pool shard map — spec-7.3 D1 (pure layer).
 *
 *	  The LMS data plane (spec-7.2) is parallelised across a pool of
 *	  LMS workers (spec-7.3).  Every DATA-plane message that concerns a
 *	  block is routed to a worker chosen by hashing the block's
 *	  BufferTag.  cluster_lms_shard_for_tag() is the ONE shard function:
 *	  the sender selects a stream with it, the receiver reads a stream
 *	  bound to it, the outbound ring is chosen with it, and the per-tag
 *	  private tables are keyed by it.  Because it is the single source of
 *	  the (tag -> worker) mapping, per-tag FIFO ordering survives the
 *	  N-way split (spec-7.3 §3.1, 8.A load-bearing invariant).
 *
 *	  Load-bearing invariant (spec-7.3 D0-①): the shard is a function of
 *	  the BufferTag ALONE — never request_id, backend, or direction.  A
 *	  same-tag REQUEST and its later ACK therefore ride the same
 *	  worker<->worker stream, so the sole wire-FIFO dependency in the
 *	  block family (cluster_gcs_block.c same-tag INVALIDATE-ACK vs
 *	  re-REQUEST) is preserved after the split.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_lms_shard.h
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.  Spec: spec-7.3-lms-worker-pool.md (D1).
 *
 *	  Backend-only: includes storage/buf_internals.h (BufferTag).  Must
 *	  not be pulled into any frontend-reachable header (L8).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_LMS_SHARD_H
#define CLUSTER_LMS_SHARD_H

#include "storage/buf_internals.h" /* BufferTag */

/*
 * Compile-time cap on the LMS worker pool.  The runtime worker count is
 * the GUC cluster.lms_workers in [1, CLUSTER_LMS_MAX_WORKERS] (spec-7.3
 * D2);  shmem arrays (the DATA ring group, per-worker tracks and
 * histograms) are sized to this cap, the live count follows the GUC.
 *
 *	worker 0 = the historic LmsProcess (B_LMS);  workers 1..7 are the
 *	new LmsWorker1..7Process (B_LMS_WORKER).  N=1 is the spec-7.2
 *	topology identity (only worker 0 runs).
 */
#define CLUSTER_LMS_MAX_WORKERS 8

/*
 * cluster_lms_shard_for_tag — map a block's BufferTag to a worker id.
 *
 *	Returns a shard in [0, n_workers).  n_workers must be in
 *	[1, CLUSTER_LMS_MAX_WORKERS] (GUC-bounded and HELLO-negotiated to be
 *	cluster-uniform, spec-7.3 D3);  n_workers == 1 always returns 0.  The
 *	result depends on the BufferTag ALONE and is platform-stable for a
 *	given byte image, so both ends of a connection agree on the stream.
 */
extern int cluster_lms_shard_for_tag(const BufferTag *tag, int n_workers);

#endif /* CLUSTER_LMS_SHARD_H */
