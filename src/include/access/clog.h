/*
 * clog.h
 *
 * PostgreSQL transaction-commit-log manager
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/clog.h
 */
/*-------------------------------------------------------------------------
 * PGRAC MODIFICATIONS
 *     Modified by: SqlRush <sqlrush@gmail.com>
 *
 *     Declare original-origin recovery I/O, Startup CLOG base observations
 *     and the explicit native FullXid page-birth record.
 *     Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
#ifndef CLOG_H
#define CLOG_H

#include "access/transam.h"
#include "access/xlogreader.h"
#include "storage/sync.h"
#include "lib/stringinfo.h"

/*
 * Possible transaction statuses --- note that all-zeroes is the initial
 * state.
 *
 * A "subcommitted" transaction is a committed subtransaction whose parent
 * hasn't committed or aborted yet.
 */
typedef int XidStatus;

#define TRANSACTION_STATUS_IN_PROGRESS		0x00
#define TRANSACTION_STATUS_COMMITTED		0x01
#define TRANSACTION_STATUS_ABORTED			0x02
#define TRANSACTION_STATUS_SUB_COMMITTED	0x03

typedef struct xl_clog_truncate
{
	int			pageno;
	TransactionId oldestXact;
	Oid			oldestXactDb;
} xl_clog_truncate;

/* Native homogeneous WAL: a physical page-generation birth, not an issued
 * transaction, retained lifetime, terminal outcome or recovery permission. */
typedef struct xl_clog_zeropage_full
{
	int32 pageno;
	uint32 reserved_zero;
	uint64 first_full_xid;
} xl_clog_zeropage_full;
StaticAssertDecl(sizeof(xl_clog_zeropage_full) == 16, "CLOG full birth WAL size");
StaticAssertDecl(offsetof(xl_clog_zeropage_full, first_full_xid) == 8,
				 "CLOG full birth WAL offset");

extern void TransactionIdSetTreeStatus(TransactionId xid, int nsubxids,
									   TransactionId *subxids, XidStatus status, XLogRecPtr lsn);
extern XidStatus TransactionIdGetStatus(TransactionId xid, XLogRecPtr *lsn);
/* PGRAC: startup-only native-prehistory repair write (GCS-race round-2 RC-E) */
extern void ClusterClogAdoptNativeStatus(TransactionId xid, XidStatus status);

#ifdef USE_PGRAC_CLUSTER
/* Original local XactCtl only, never native_side projection. Caller holds
 * XactTruncationLock across qualified FullXid floor check and this I/O; no
 * XidGenLock/SLRU lock may already be held. Native status is derived here.
 * The group LSN is a hint, not an exact terminal WAL record. A complete
 * original page must exist and fsync must be enabled. False may already
 * have written/synced bytes;
 * it does not promise rollback or a safe automatic retry. */
typedef struct ClusterClogNativeTerminalV1 {
	XidStatus status;
	XLogRecPtr status_lsn;
} ClusterClogNativeTerminalV1;
extern bool cluster_clog_native_terminal_sync_v1(TransactionId xid,
												 ClusterClogNativeTerminalV1 *out);

/* Dormant original cold-origin durable base input. This is a complete native
 * page read under that origin's existing truncation lock and original cold
 * authorization, never a recoverer-local CLOG or visibility fallback. Status
 * is derived from the selected on-disk page; nonterminal/missing/short refuse.
 * No native XidGen/truncation/SLRU or selected-origin lock may be held.
 * ERROR releases only this call's
 * FD/lock. False may follow sync and does not imply rollback/safe retry.
 * This raw page does not certify FullXid era, retained floor or a source seal. */
/* Process-local observation only; never serialized or a retained lifetime. */
typedef struct ClusterClogNativeFileV1 {
	dev_t device;
	ino_t inode;
	off_t size;
} ClusterClogNativeFileV1;

typedef struct ClusterClogNativeBaseV1 {
	int origin;
	TransactionId xid;
	uint32 page_number;
	XidStatus status;
	ClusterClogNativeFileV1 file;
	uint8 image[BLCKSZ];
} ClusterClogNativeBaseV1;
extern bool cluster_clog_native_recovery_base_read_v1(int origin, TransactionId xid,
													  ClusterClogNativeBaseV1 *out);

/* Original descriptor custody, not a truncation pin or a certified era.
 * Start with *held NULL; subsequent reads reuse that same tracked descriptor
 * for the exact origin/xid. Refusal/ERROR permanently poisons existing custody
 * and never reopens it. Raw observation above retains its per-call lifetime.
 * Exclusive out/held storage must be separate from the opaque object. The
 * original cold scope and fd.c lifetime must outlive custody; release it on
 * every exit before either ends, including after refusal/ERROR. No native lock
 * is held between calls. Release consumes only local FD/memory, never debt. */
typedef struct ClusterClogNativeBaseHandleV1 ClusterClogNativeBaseHandleV1;
extern bool cluster_clog_native_recovery_base_read_held_v1(int origin, TransactionId xid,
	ClusterClogNativeBaseV1 *out, ClusterClogNativeBaseHandleV1 **held);
extern void cluster_clog_native_recovery_base_release_v1(ClusterClogNativeBaseHandleV1 **held);

/* Original declared Startup participant only. The selected source retains
 * the original simultaneous replay IR set; it is not a C0/cold-write grant.
 * Query FullXid is an identity to inspect, never a certified native era. */
struct ClusterRecoveryRelmapSourceV1;
typedef struct ClusterRelmapClogBaseV1 {
	uint64 queried_full_xid;
	int origin;
	uint32 page_number;
	XidStatus status;
	dev_t device;
	ino_t inode;
	off_t size;
	uint8 image[BLCKSZ];
} ClusterRelmapClogBaseV1;
typedef struct ClusterRelmapClogBaseHandleV1 ClusterRelmapClogBaseHandleV1;
/* Internal SLRU physical-write bookkeeping; -1 means a non-CLOG controller.
 * Every nonnegative begin result must reach end, including ERROR exits. */
struct SlruCtlData;
extern int cluster_clog_relmap_writeback_begin(struct SlruCtlData *ctl);
extern void cluster_clog_relmap_writeback_end(int origin);
/* Start with *held NULL. Subsequent reads require the exact original source,
 * query and full disk image on the same tracked fd. Entered refusal/ERROR
 * poisons custody and the original episode, retaining original protection.
 * Native locks cover only each read; no lock is retained between calls.
 * Native mutation and physical writeback in this process invalidate held
 * observations before the attempt, even if the same bytes are restored.
 * Shared mutation sequences also reject writes by other backends of this
 * postmaster and refuse observations while a native attempt is in progress.
 * Saturation refuses; normal/ERROR completion never restores an old sequence.
 * This does not exclude inter-instance writes or certify physical lifetime.
 * Missing/short/aliased files refuse. IN_PROGRESS stays IN_PROGRESS.
 * No caller native/CF lock or critical section; output/held/source are
 * exclusive and disjoint. Precondition refusal performs no I/O/revocation.
 * Release local custody on every exit before original scope/fd.c teardown.
 * This observes a base; native reuse/truncation/FullXid era/history, complete
 * native/SIDE fold and final joint COMPLETE still need their original proof. */
extern bool cluster_clog_relmap_base_read_v1(
	const struct ClusterRecoveryRelmapSourceV1 *source, uint64 requested_full_xid,
	ClusterRelmapClogBaseV1 *out, ClusterRelmapClogBaseHandleV1 **held);
extern void cluster_clog_relmap_base_release_v1(ClusterRelmapClogBaseHandleV1 **held);
/* Exact fixed native custody allocation, for the original owner's budget. */
extern size_t cluster_clog_relmap_base_custody_bytes_v1(void);
/* Original-origin storage only. The recovery owner must first prove the
 * complete terminal record, original source/base/era and mutation authority.
 * These calls neither resolve an unknown transaction nor publish visibility.
 * Own-origin redo retains the ordinary native CLOG owner; an exact durable
 * C0 top ABORT uses that same cache without discarding unrelated dirty bits. */
extern bool cluster_clog_native_recovery_apply_v1(int origin, TransactionId xid, int nsubxids,
												  const TransactionId *subxids, XidStatus status);
extern bool cluster_clog_native_recovery_verify_v1(int origin, TransactionId xid, int nsubxids,
												   const TransactionId *subxids, XidStatus status);
struct ClusterSideProjectionOperationV1;
/* Exact retained native ZERO/TRUNCATE only, under the same cold scope. */
extern bool cluster_clog_native_recovery_record_apply_v1(
	int origin, const struct ClusterSideProjectionOperationV1 *operation);
extern bool cluster_clog_native_recovery_record_verify_v1(
	int origin, const struct ClusterSideProjectionOperationV1 *operation);
/* FULL requires every owned native16B birth byte, without translating it to
 * a legacy page number. Same original cold authority and native write/sync/
 * readback; these calls do not certify issuance, era or physical lifetime. */
extern bool cluster_clog_native_recovery_full_record_apply_v1(
	int origin, const struct ClusterSideProjectionOperationV1 *operation,
	const uint8 *payload, uint32 length);
extern bool cluster_clog_native_recovery_full_record_verify_v1(
	int origin, const struct ClusterSideProjectionOperationV1 *operation,
	const uint8 *payload, uint32 length);
#endif

extern Size CLOGShmemBuffers(void);
extern Size CLOGShmemSize(void);
extern void CLOGShmemInit(void);
extern void BootStrapCLOG(void);
extern void StartupCLOG(void);
extern void TrimCLOG(void);
extern void CheckPointCLOG(void);
extern void ExtendCLOG(TransactionId newestXact);
/* Dormant original allocator entry. Supply its actual FullXid candidate
 * while holding the original XidGenLock EXCLUSIVE; skipped page boundaries
 * may be initialized without issuing any transaction. No live caller or
 * birth/era/lifetime grant is installed by this entry. Native ERROR can
 * follow a dirty page or inserted WAL; it is not rollback/retry permission. */
extern void ExtendCLOGFull(FullTransactionId newestXact);
extern void TruncateCLOG(TransactionId oldestXact, Oid oldestxid_datoid);

extern int	clogsyncfiletag(const FileTag *ftag, char *path);

/* XLOG stuff */
#define CLOG_ZEROPAGE		0x00
#define CLOG_TRUNCATE		0x10
#define CLOG_ZEROPAGE_FULL	0x20

extern void clog_redo(XLogReaderState *record);
extern void clog_desc(StringInfo buf, XLogReaderState *record);
extern const char *clog_identify(uint8 info);

#endif							/* CLOG_H */
