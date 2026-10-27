/*-------------------------------------------------------------------------
 *
 * cluster_savepoint_reader.h
 *   Exact operation decode for a caller-qualified retained undo record.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *   src/include/cluster/cluster_savepoint_reader.h
 *
 * NOTES
 *   Pgrac-original process-local decode; no retained authority or visibility.
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SAVEPOINT_READER_H
#define CLUSTER_SAVEPOINT_READER_H

#include "cluster/cluster_savepoint.h"
#include "cluster/cluster_undo_record.h"

#ifdef USE_PGRAC_CLUSTER
#define CLUSTER_SAVEPOINT_READER_PRESENT 1

typedef struct ClusterSavepointReaderRecord {
	uint16 body_length;
	UndoItlHistoryTrailer history;
	UndoOperationTrailer operation;
} ClusterSavepointReaderRecord;

/* expected is an exact compare input, not a live-backend lookup. The caller
 * owns block/CRC/read qualification, original top outcome, ordered current
 * tail acquisition, ancestry and chain sequence validation. In particular,
 * sample top before acquiring the tail; never pair a cached LIVE tail with
 * a later COMMITTED proof. ACTIVE stays conservative for that construction.
 *
 * This function only checks bytes against that independently qualified
 * identity. It allocates no authority/pin, grants no visibility and does not
 * decide whether to inverse. false clears out; record and body stay intact. */
extern bool cluster_savepoint_reader_decode(const ClusterSavepointOperation *expected,
											const ClusterTxLocator *locator, UBA current_uba,
											const UndoRecordHeader *record, const char *body,
											size_t payload_length,
											ClusterSavepointReaderRecord *out);

#endif
#endif
