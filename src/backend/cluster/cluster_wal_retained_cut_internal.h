/*-------------------------------------------------------------------------
 *
 * cluster_wal_retained_cut_internal.h
 *	  Private state of the retention lower census (S07), shared by its page
 *	  core (cluster_wal_retained_cut.c), its SIDE and native-record
 *	  classification (cluster_wal_retained_side.c) and its older-generation
 *	  judgement (cluster_wal_retained_generation.c).
 *
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_wal_retained_cut_internal.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-03-shared-wal-and-checkpoint.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_WAL_RETAINED_CUT_INTERNAL_H
#define CLUSTER_WAL_RETAINED_CUT_INTERNAL_H

#include "access/xlogreader.h"
#include "cluster/cluster_page_detached.h"
#include "cluster/cluster_page_wal.h"
#include "cluster/cluster_pi_write.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_side_xact.h"
#include "cluster/cluster_wal_inputs.h"
#include "cluster/cluster_wal_retained_cut.h"
#include "storage/buffile.h"

/* 2^18 buckets of 40 bytes: 10 MiB, independent of the database size. */
#define RETAINED_SKETCH_BITS 18
#define RETAINED_SKETCH_BUCKETS (UINT32_C(1) << RETAINED_SKETCH_BITS)
#define RETAINED_BATCH 256
#define RETAINED_SIDE_CLASSES 9

/*
 * SIDE owners whose history an obligation may need: the undo segment header
 * (TT slots), undo data blocks and prepared transactions.  TERMINAL, CLOG,
 * MULTIXACT and COMMIT_TS effects of a history record are durable at its
 * source's completion and no obligation reads them back (see
 * cluster_wal_retained_side.c); SPACE is keyed like a page and NATIVE_CONTROL
 * never needs an older record.
 */
#define RETAINED_SIDE_ANCESTRY                                                                     \
	(RF_SIDE_CONTRIBUTION_UNDO_HEADER | RF_SIDE_CONTRIBUTION_UNDO_BLOCK                            \
	 | RF_SIDE_CONTRIBUTION_PREPARED)

/* Prepared transactions tracked exactly; beyond this the class is kept. */
#ifndef RETAINED_PREPARED_MAX
#define RETAINED_PREPARED_MAX 1024
#endif

typedef struct RetainedSource {
	ClusterWalSourceRef ref;
	XLogRecPtr completion;
	XLogRecPtr bound;
	/* Earliest history record start per SIDE owner class, and earliest one
	 * whose keys could not be derived. */
	XLogRecPtr side_first[RETAINED_SIDE_CLASSES];
	XLogRecPtr side_unkeyed[RETAINED_SIDE_CLASSES];
	/* Earliest history record that changes a SPACE structure. */
	XLogRecPtr structure_first;
	ClusterWalRetainedPinV1 pin;
} RetainedSource;

/* One history edge: a page, SPACE block or SIDE key a durable record
 * changed.  side is 0 for a page or SPACE block, else the SIDE owner bit. */
typedef struct RetainedEdge {
	uint64 key;
	uint64 result_token;
	XLogRecPtr read_ptr;
	uint32 source;
	uint32 side;
	uint8 incarnation[16];
} RetainedEdge;

/*
 * Obligations of every key hashed here: the earliest before-version and the
 * lowest and highest segment incarnation.  Empty while inc_min > inc_max.
 */
typedef struct RetainedBucket {
	SCN before;
	uint8 inc_min[16];
	uint8 inc_max[16];
} RetainedBucket;

/* A history PREPARE: needed until a history COMMIT/ABORT PREPARED resolves
 * its transaction. */
typedef struct RetainedPrepare {
	TransactionId xid;
	uint32 source;
	XLogRecPtr read_ptr;
} RetainedPrepare;

typedef struct RetainedCutWork {
	uint32 nsources;
	int32 self;
	ClusterPcmLocalPiFloorV1 local_pi;
	ClusterPageWalDirtyFloorV1 dirty;
	RetainedSource sources[CLUSTER_WAL_INPUTS_MAX];
	RetainedBucket *sketch;
	/* SIDE owner bits of all obligations, of obligations whose keys could
	 * not be derived, and of the history the fold actually kept. */
	uint32 obligation_side;
	uint32 obligation_unkeyed;
	uint32 pinned_side;
	RetainedEdge batch[RETAINED_BATCH];
	uint32 batch_count;
	BufFile *spool;
	uint64 spooled;
	uint64 records;
	uint64 history_edges;
	uint64 retained_edges;
	uint32 last_source;
	/* Classification of the record being decoded. */
	uint32 current_source;
	bool current_history;
	XLogRecPtr current_read;
	XLogRecPtr current_end;
	RfPageProofDetailV1 detail;
	/* Prepared transactions of the retained history (exact). */
	uint32 nprepares;
	uint32 nresolved;
	bool prepared_overflow;
	RetainedPrepare prepares[RETAINED_PREPARED_MAX];
	TransactionId resolved[RETAINED_PREPARED_MAX];
	/* Decoder scratch, too large for the stack. */
	RfSideXactOperationV1 xact;
} RetainedCutWork;

/* cluster_wal_retained_cut.c */
extern void retained_key_seen(RetainedCutWork *work, uint64 key, SCN before,
							  const uint8 before_incarnation[16], SCN result,
							  const uint8 result_incarnation[16], uint32 side);
extern void retained_structure(RetainedCutWork *work);
extern void retained_pin(RetainedSource *source, XLogRecPtr at, ClusterWalRetainedPinV1 pin);

/* cluster_wal_retained_side.c */
extern void retained_side_record(RetainedCutWork *work, XLogReaderState *record,
								 const ClusterWalSourceRef *source,
								 const RfContributorStreamCutV1 *cut, uint32 owners);
extern RfPageProofDetailV1 retained_native_record(RetainedCutWork *work, XLogReaderState *record,
												  RfPageProofDetailV1 refused);
extern void retained_side_fold(RetainedCutWork *work);

/* cluster_wal_retained_generation.c */
extern void retained_unneeded(RetainedCutWork *work, ClusterWalInputsV1 *inputs,
							  ClusterWalRetainedCutV1 *out);

#endif /* CLUSTER_WAL_RETAINED_CUT_INTERNAL_H */
