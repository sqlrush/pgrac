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
#include "storage/buf_internals.h"

#define CLUSTER_SAVEPOINT_READER_PRESENT 1

typedef struct ClusterSavepointReaderRecord {
	uint16 body_length;
	UndoItlHistoryTrailer history;
	UndoOperationTrailer operation;
} ClusterSavepointReaderRecord;

/* A process-local historical challenge, not a backend publication receipt.
 * Current/PI roots and exact retained records supply these compare inputs;
 * only original current TT/DATA qualification can prove their authority. */
typedef struct ClusterSavepointReaderIdentity {
	ClusterTTSlotPhysicalLocator canonical;
	uint32 canonical_generation;
	FullTransactionId top_fxid;
	FullTransactionId operation_fxid;
	uint64 operation_sequence;
	CommandId command_id;
	SubTransactionId subid;
} ClusterSavepointReaderIdentity;

extern bool
cluster_savepoint_reader_identity_from_operation(const ClusterSavepointOperation *expected,
												 const ClusterTxLocator *locator,
												 ClusterSavepointReaderIdentity *out);
extern bool cluster_savepoint_reader_decode_identity(const ClusterSavepointReaderIdentity *expected,
													 const ClusterTxLocator *locator,
													 UBA current_uba,
													 const UndoRecordHeader *record,
													 const char *body, size_t payload_length,
													 ClusterSavepointReaderRecord *out);
extern bool
cluster_savepoint_reader_extract_identity(const ClusterSavepointReaderIdentity *expected,
										  const ClusterTxLocator *locator, const char page[BLCKSZ],
										  char record_out[BLCKSZ], size_t *record_length_out,
										  ClusterSavepointReaderRecord *out);

/* Discover only this exact immutable page head from already-qualified DATA.
 * head/tag belong to the caller's original current/PI and retention scope.
 * The addressed record supplies a provisional challenge, never PUBLISHED or
 * an invented ACTIVE LSN. No outcome is granted: sample canonical TT, then
 * reacquire current DATA and compare all immutable bytes before consumption.
 * Unknown TT wrap may be filled only from this first addressed head. */
/* Mixed own history additionally requires the caller's published current top.
 * A zero sequence denotes a V1 link, never a child operation or CID receipt. */
extern bool cluster_savepoint_reader_discover_own_head(
	const ClusterSavepointOperation *owner, const ClusterItlSlotData *head, const BufferTag *tag,
	const ClusterTxLocator *locator, const char page[BLCKSZ], ClusterSavepointReaderIdentity *out);
extern bool cluster_savepoint_reader_discover_head(const ClusterItlSlotData *head,
												   const BufferTag *tag,
												   const ClusterTxLocator *locator,
												   const char page[BLCKSZ],
												   ClusterSavepointReaderIdentity *out);

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

/* Extract only the addressed operation from an already-qualified resident
 * DATA copy. Caller owns its block identity/CRC/current guard. Failure clears
 * length/metadata, leaves record_out intact and never grants visibility. */
extern bool cluster_savepoint_reader_extract(const ClusterSavepointOperation *expected,
											 const ClusterTxLocator *locator,
											 const char page[BLCKSZ], char record_out[BLCKSZ],
											 size_t *record_length_out,
											 ClusterSavepointReaderRecord *out);

#endif
#endif
