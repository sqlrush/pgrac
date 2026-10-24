/*
 * subtrans.h
 *
 * PostgreSQL subtransaction-log manager
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/subtrans.h
 */
#ifndef SUBTRANS_H
#define SUBTRANS_H

/* Number of SLRU buffers to use for subtrans */
#define NUM_SUBTRANS_BUFFERS	32

extern void SubTransSetParent(TransactionId xid, TransactionId parent);
extern TransactionId SubTransGetParent(TransactionId xid);
extern TransactionId SubTransGetTopmostTransaction(TransactionId xid);

extern Size SUBTRANSShmemSize(void);
extern void SUBTRANSShmemInit(void);
extern void BootStrapSUBTRANS(void);
extern void StartupSUBTRANS(TransactionId oldestActiveXID);
extern void CheckPointSUBTRANS(void);
extern void ExtendSUBTRANS(TransactionId newestXact);
extern void TruncateSUBTRANS(TransactionId oldestXact);

#ifdef USE_PGRAC_CLUSTER
/* Exact original-origin ASSIGNMENT storage, under the executable cold owner.
 * Existing base/era qualification remains the caller's responsibility. */
extern bool cluster_subtrans_native_recovery_preflight_v1(int origin, TransactionId top, int count,
														 const TransactionId *children);
extern bool cluster_subtrans_native_recovery_apply_v1(int origin, TransactionId top, int count,
													 const TransactionId *children);
extern bool cluster_subtrans_native_recovery_verify_v1(int origin, TransactionId top, int count,
													  const TransactionId *children);
/* PGRAC: retained shared-origin parentage joins native checkpoint fsync.
 * Author: SqlRush <sqlrush@gmail.com> */
struct FileTag;
extern int subtranssyncfiletag(const struct FileTag *ftag, char *path);
#endif

#endif							/* SUBTRANS_H */
