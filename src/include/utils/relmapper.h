/*-------------------------------------------------------------------------
 *
 * relmapper.h
 *	  Catalog-to-filenumber mapping
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/relmapper.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RELMAPPER_H
#define RELMAPPER_H

#include "access/xlogreader.h"
#include "lib/stringinfo.h"

/* ----------------
 *		relmap-related XLOG entries
 * ----------------
 */

#define XLOG_RELMAP_UPDATE		0x00

typedef struct xl_relmap_update
{
	Oid			dbid;			/* database ID, or 0 for shared map */
	Oid			tsid;			/* database's tablespace, or pg_global */
	int32		nbytes;			/* size of relmap data */
	char		data[FLEXIBLE_ARRAY_MEMBER];
} xl_relmap_update;

#define MinSizeOfRelmapUpdate offsetof(xl_relmap_update, data)


extern RelFileNumber RelationMapOidToFilenumber(Oid relationId, bool shared);

extern Oid	RelationMapFilenumberToOid(RelFileNumber filenumber, bool shared);
extern RelFileNumber RelationMapOidToFilenumberForDatabase(char *dbpath,
														   Oid relationId);
extern void RelationMapCopy(Oid dbid, Oid tsid, char *srcdbpath,
							char *dstdbpath);
extern void RelationMapUpdateMap(Oid relationId, RelFileNumber fileNumber,
								 bool shared, bool immediate);

extern void RelationMapRemoveMapping(Oid relationId);

extern void RelationMapInvalidate(bool shared);
extern void RelationMapInvalidateAll(void);

extern void AtCCI_RelationMap(void);
extern void AtEOXact_RelationMap(bool isCommit, bool isParallelWorker);
extern void AtPrepare_RelationMap(void);

#ifdef USE_PGRAC_CLUSTER
struct RelMapFile;
struct ClusterRelmapOwnerSampleV2;
struct ClusterRelmapV2Image;

/* Dormant stage-only producer under the caller's original global relmap X.
 * Captures the original owner before local locking, prepares retention, then
 * merges into the same clean v2 base and inserts/flushes/stages native WAL.
 * Outputs change only on success and retain the original owner for terminal
 * work. False/ERROR may follow preparation, durable WAL or pair writes: keep
 * all original obligations and follow the original failure path; never retry
 * or infer rollback. Caller must hold no mapping, XidGen or retention lock. It keeps
 * global X and all physical-file, commit, cleanup and terminal duties. This
 * does not adopt a local map, arm publication or authorize commit/release. */
extern bool cluster_relmap_producer_stage_v2(bool shared,
	const struct RelMapFile *updates, struct ClusterRelmapOwnerSampleV2 *owner_out,
	struct ClusterRelmapV2Image *staged_out);

/* Dormant original producer preparation. Obtains a successful original stage
 * in this call, rechecks BOTH exact staged copies and the original staged pin,
 * preserves all mapped files across abort, then installs the local map and
 * requests original synchronous commit. Same caller/outputs/failure contract
 * as stage_v2; partial preservation retains files and all pins/debt. No live
 * caller, terminal arm/proof, COMPLETE publication or commit/release authority.
 * Original terminal/physical-lifetime duties remain with the caller. */
extern bool cluster_relmap_producer_prepare_v2(bool shared,
	const struct RelMapFile *updates, struct ClusterRelmapOwnerSampleV2 *owner_out,
	struct ClusterRelmapV2Image *staged_out);

/* Dormant original producer custody. Arm obtains successful original prepare
 * receipts in this invocation and retains them privately per map flavour;
 * no external image can replace them. Same failure duties as prepare_v2.
 * An incomplete arm with possible retention or persistent effects blocks
 * further arm/finish calls, including cleanup of earlier successful receipts.
 * It keeps global X and original pins/debt for the original failure path.
 * Finish is for the original authenticated terminal caller BEFORE top FullXid
 * teardown, with global relmap X retained and no mapping/XidGen/retention lock.
 * Its first attempt latches both slots before fallible checks. False/ERROR
 * keeps custody and pins/debt; never retry or switch terminal polarity.
 * True means only this batch's dual-copy cleanup and original-owner recheck,
 * then local custody retirement. Caller retains original postcommit fail-stop
 * or abort failure handling, local-map/invalidation/lock/physical-file duties.
 * No live hook, terminal classifier/proof, COMPLETE or release authority. */
extern bool cluster_relmap_producer_arm_v2(bool shared, const struct RelMapFile *updates);
extern bool cluster_relmap_producer_finish_v2(bool is_commit);

/* Dormant original precommit batch, before the native commit decision, under
 * the caller's original mapped-rel AEL/global relmap X. Consume only original
 * command-activated shared/per-db update queues through the original arm.
 * Pending CCI work, parallel workers or existing custody refuse with ERROR.
 * Both requested prepares must succeed before either active queue is reset.
 * False from an arm becomes original ERROR; ERROR leaves all queued inputs,
 * saved receipts, unfinished protection and pins/debt for original failure
 * handling. Empty queues are a no-op. No global lock acquisition, terminal
 * authentication, commit permission, retention release or live v2 hook. */
extern void cluster_relmap_producer_precommit_v2(bool is_parallel_worker);

/* Dormant original postcommit caller only, after durable commit and before
 * top FullXid teardown, retaining original mapped-rel AEL and global relmap X.
 * Consume saved prepare receipts once, publish both pairs before invalidation,
 * apply the original local/peer ACK policy and recheck the original owner.
 * Unlock global X last, then retire local custody only. UNKNOWN/ERROR, source
 * loss or exhausted notification retries PANIC; custody/pins/debt remain and
 * uncertain storage is never retried. Incomplete arm preparation also PANICs;
 * only empty custody with no unfinished preparation is a no-op. This
 * authenticates no terminal, releases no retention pin and installs no live
 * v2 caller. */
extern void cluster_relmap_producer_postcommit_v2(void);

/* Dormant original abort caller, before top FullXid teardown, retaining the
 * original mapped-rel AEL/global relmap X and holding no local LWLocks. The
 * abort decision can precede TRANS_ABORT and remains the caller's duty.
 * Discard all saved prepare receipts once, unlock global X last, then retire
 * local custody. UNKNOWN/ERROR/source loss PANIC, keeping custody/pins/debt;
 * never retry or switch polarity. Incomplete arm preparation also PANICs,
 * even without a saved receipt; no earlier receipt is discarded or unlocked.
 * Empty custody without unfinished preparation is a no-op. Standalone
 * stage/prepare callers retain their original failure obligations.
 * No invalidation, terminal proof, retention release or live v2 caller. */
extern void cluster_relmap_producer_abort_v2(void);

/* PGRAC (spec-6.14 D5): post-commit publish / abort discard of a staged
 * shared relmap authority image. */
extern void AtEOXact_ClusterRelmapPublish(void);
extern void AtAbort_ClusterRelmapPending(void);
#endif

extern void CheckPointRelationMap(void);

extern void RelationMapFinishBootstrap(void);

extern void RelationMapInitialize(void);
extern void RelationMapInitializePhase2(void);
extern void RelationMapInitializePhase3(void);

extern Size EstimateRelationMapSpace(void);
extern void SerializeRelationMap(Size maxSize, char *startAddress);
extern void RestoreRelationMap(char *startAddress);

extern void relmap_redo(XLogReaderState *record);
extern void relmap_desc(StringInfo buf, XLogReaderState *record);
extern const char *relmap_identify(uint8 info);

#endif							/* RELMAPPER_H */
