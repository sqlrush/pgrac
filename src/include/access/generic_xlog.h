/*-------------------------------------------------------------------------
 *
 * generic_xlog.h
 *	  Generic xlog API definition.
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/generic_xlog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GENERIC_XLOG_H
#define GENERIC_XLOG_H

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "storage/bufpage.h"
#include "utils/rel.h"

#define MAX_GENERIC_XLOG_PAGES	XLR_NORMAL_MAX_BLOCK_ID

/* Flag bits for GenericXLogRegisterBuffer */
#define GENERIC_XLOG_FULL_IMAGE 0x0001	/* write full-page image */

/* state of generic xlog record construction */
struct GenericXLogState;
typedef struct GenericXLogState GenericXLogState;

#ifdef USE_PGRAC_CLUSTER
/* PGRAC: only existing internal cleanup owners may emit shared generic WAL. */
#include "cluster/cluster_space_identity.h"
#include "cluster/cluster_page_producer.h"
typedef enum GenericXLogInternalOwner
{
	GENERIC_XLOG_ITL_FINISH = 1,
	GENERIC_XLOG_CTRC_MX,
	GENERIC_XLOG_CTRC_ITL
} GenericXLogInternalOwner;
extern GenericXLogState *GenericXLogStartInternal(bool is_logged,
												GenericXLogInternalOwner owner);
extern Page GenericXLogRegisterBufferVersioned(GenericXLogState *state, Buffer buffer,
												 int flags, const ClusterSpaceIdentity *identity);

/* One caller-owned full-image frame, never recovery/write/release authority.
 * Preparation leaves inputs and output unchanged on refusal. Registration
 * only borrows the frame until native insert/reset; the executing owner must
 * supply page custody, F4 INSERTING, insertion, flush and durable installation. */
typedef struct GenericXLogRecoveryImageV1
{
	GenericXLogInternalOwner owner;
	ClusterSpaceIdentity identity;
	ForkNumber fork_number;
	BlockNumber block_number;
	uint64 result_token;
	RfPageVersionEdgeEntryV1 version;
	PGAlignedBlock image;
} GenericXLogRecoveryImageV1;

extern bool GenericXLogRecoveryImagePrepare(GenericXLogInternalOwner owner,
	const ClusterSpaceIdentity *identity, ForkNumber fork_number, BlockNumber block_number,
	const char *before_image, const char *after_image, GenericXLogRecoveryImageV1 *out);
extern bool GenericXLogRecoveryImageRegister(GenericXLogRecoveryImageV1 *image);
#endif

/* API for construction of generic xlog records */
extern GenericXLogState *GenericXLogStart(Relation relation);
extern GenericXLogState *GenericXLogStartLogged(bool is_logged);
extern Page GenericXLogRegisterBuffer(GenericXLogState *state, Buffer buffer,
									  int flags);
extern XLogRecPtr GenericXLogFinish(GenericXLogState *state);
extern void GenericXLogAbort(GenericXLogState *state);

/* functions defined for rmgr */
extern void generic_redo(XLogReaderState *record);
extern const char *generic_identify(uint8 info);
extern void generic_desc(StringInfo buf, XLogReaderState *record);
extern void generic_mask(char *page, BlockNumber blkno);

#endif							/* GENERIC_XLOG_H */
