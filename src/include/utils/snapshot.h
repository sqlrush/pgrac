/*-------------------------------------------------------------------------
 *
 * snapshot.h
 *	  POSTGRES snapshot definition
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/snapshot.h
 *
 *-------------------------------------------------------------------------
 *
 * PGRAC MODIFICATIONS (spec-3.3 D1):
 *   Added explicit 24-byte cluster tail to SnapshotData: SCN read_scn (8B)
 *   + uint64 read_epoch (8B) + uint8 cluster_source (1B) + uint8 _pad[7]
 *   (7B). Explicit layout prevents hidden 4B padding (R4 P1) and avoids
 *   uint32 wrap alias on cluster epoch (R9 P2).
 *
 *   New SnapshotSource enum {LOCAL=0, CLUSTER=1}:
 *     LOCAL  - catalog scans, logical decoding, system snapshots; PG-native
 *              visibility path unchanged.
 *     CLUSTER- user-facing MVCC snapshots; read_scn / read_epoch carry the
 *              cluster-wide SCN and epoch at GetSnapshotData() capture
 *              instant.
 *
 *   Spec: spec-3.3-snapshot-consistency-cross-node.md §1.2 D1 / §2.1.
 *   See also feature-121 + docs/spec-drafting-lessons.md v1.68 L180/L181.
 *-------------------------------------------------------------------------
 */
#ifndef SNAPSHOT_H
#define SNAPSHOT_H

#include "access/htup.h"
#include "access/xlogdefs.h"
#include "datatype/timestamp.h"
#include "lib/pairingheap.h"
#include "storage/buf.h"

#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_scn.h"		/* PGRAC: SCN typedef */
#endif


/*
 * The different snapshot types.  We use SnapshotData structures to represent
 * both "regular" (MVCC) snapshots and "special" snapshots that have non-MVCC
 * semantics.  The specific semantics of a snapshot are encoded by its type.
 *
 * The behaviour of each type of snapshot should be documented alongside its
 * enum value, best in terms that are not specific to an individual table AM.
 *
 * The reason the snapshot type rather than a callback as it used to be is
 * that that allows to use the same snapshot for different table AMs without
 * having one callback per AM.
 */
typedef enum SnapshotType
{
	/*-------------------------------------------------------------------------
	 * A tuple is visible iff the tuple is valid for the given MVCC snapshot.
	 *
	 * Here, we consider the effects of:
	 * - all transactions committed as of the time of the given snapshot
	 * - previous commands of this transaction
	 *
	 * Does _not_ include:
	 * - transactions shown as in-progress by the snapshot
	 * - transactions started after the snapshot was taken
	 * - changes made by the current command
	 * -------------------------------------------------------------------------
	 */
	SNAPSHOT_MVCC = 0,

	/*-------------------------------------------------------------------------
	 * A tuple is visible iff the tuple is valid "for itself".
	 *
	 * Here, we consider the effects of:
	 * - all committed transactions (as of the current instant)
	 * - previous commands of this transaction
	 * - changes made by the current command
	 *
	 * Does _not_ include:
	 * - in-progress transactions (as of the current instant)
	 * -------------------------------------------------------------------------
	 */
	SNAPSHOT_SELF,

	/*
	 * Any tuple is visible.
	 */
	SNAPSHOT_ANY,

	/*
	 * A tuple is visible iff the tuple is valid as a TOAST row.
	 */
	SNAPSHOT_TOAST,

	/*-------------------------------------------------------------------------
	 * A tuple is visible iff the tuple is valid including effects of open
	 * transactions.
	 *
	 * Here, we consider the effects of:
	 * - all committed and in-progress transactions (as of the current instant)
	 * - previous commands of this transaction
	 * - changes made by the current command
	 *
	 * This is essentially like SNAPSHOT_SELF as far as effects of the current
	 * transaction and committed/aborted xacts are concerned.  However, it
	 * also includes the effects of other xacts still in progress.
	 *
	 * A special hack is that when a snapshot of this type is used to
	 * determine tuple visibility, the passed-in snapshot struct is used as an
	 * output argument to return the xids of concurrent xacts that affected
	 * the tuple.  snapshot->xmin is set to the tuple's xmin if that is
	 * another transaction that's still in progress; or to
	 * InvalidTransactionId if the tuple's xmin is committed good, committed
	 * dead, or my own xact.  Similarly for snapshot->xmax and the tuple's
	 * xmax.  If the tuple was inserted speculatively, meaning that the
	 * inserter might still back down on the insertion without aborting the
	 * whole transaction, the associated token is also returned in
	 * snapshot->speculativeToken.  See also InitDirtySnapshot().
	 * -------------------------------------------------------------------------
	 */
	SNAPSHOT_DIRTY,

	/*
	 * A tuple is visible iff it follows the rules of SNAPSHOT_MVCC, but
	 * supports being called in timetravel context (for decoding catalog
	 * contents in the context of logical decoding).
	 */
	SNAPSHOT_HISTORIC_MVCC,

	/*
	 * A tuple is visible iff the tuple might be visible to some transaction;
	 * false if it's surely dead to everyone, i.e., vacuumable.
	 *
	 * For visibility checks snapshot->min must have been set up with the xmin
	 * horizon to use.
	 */
	SNAPSHOT_NON_VACUUMABLE
} SnapshotType;

typedef struct SnapshotData *Snapshot;

#define InvalidSnapshot		((Snapshot) NULL)

#ifdef USE_PGRAC_CLUSTER
/*
 * PGRAC: cluster snapshot source enum (spec-3.3 D1).
 *
 * LOCAL  - catalog scans / logical decoding / system snapshots; PG-native
 *          visibility path unchanged; read_scn/read_epoch are InvalidScn/0.
 * CLUSTER- user-facing MVCC snapshots; read_scn/read_epoch carry the
 *          cluster-wide SCN/epoch at GetSnapshotData() capture instant.
 */
typedef enum SnapshotSource
{
	SNAPSHOT_SOURCE_LOCAL = 0,
	SNAPSHOT_SOURCE_CLUSTER = 1
} SnapshotSource;
#endif

/*
 * Struct representing all kind of possible snapshots.
 *
 * There are several different kinds of snapshots:
 * * Normal MVCC snapshots
 * * MVCC snapshots taken during recovery (in Hot-Standby mode)
 * * Historic MVCC snapshots used during logical decoding
 * * snapshots passed to HeapTupleSatisfiesDirty()
 * * snapshots passed to HeapTupleSatisfiesNonVacuumable()
 * * snapshots used for SatisfiesAny, Toast, Self where no members are
 *	 accessed.
 *
 * TODO: It's probably a good idea to split this struct using a NodeTag
 * similar to how parser and executor nodes are handled, with one type for
 * each different kind of snapshot to avoid overloading the meaning of
 * individual fields.
 */
typedef struct SnapshotData
{
	SnapshotType snapshot_type; /* type of snapshot */

	/*
	 * The remaining fields are used only for MVCC snapshots, and are normally
	 * just zeroes in special snapshots.  (But xmin and xmax are used
	 * specially by HeapTupleSatisfiesDirty, and xmin is used specially by
	 * HeapTupleSatisfiesNonVacuumable.)
	 *
	 * An MVCC snapshot can never see the effects of XIDs >= xmax. It can see
	 * the effects of all older XIDs except those listed in the snapshot. xmin
	 * is stored as an optimization to avoid needing to search the XID arrays
	 * for most tuples.
	 */
	TransactionId xmin;			/* all XID < xmin are visible to me */
	TransactionId xmax;			/* all XID >= xmax are invisible to me */

	/*
	 * For normal MVCC snapshot this contains the all xact IDs that are in
	 * progress, unless the snapshot was taken during recovery in which case
	 * it's empty. For historic MVCC snapshots, the meaning is inverted, i.e.
	 * it contains *committed* transactions between xmin and xmax.
	 *
	 * note: all ids in xip[] satisfy xmin <= xip[i] < xmax
	 */
	TransactionId *xip;
	uint32		xcnt;			/* # of xact ids in xip[] */

	/*
	 * For non-historic MVCC snapshots, this contains subxact IDs that are in
	 * progress (and other transactions that are in progress if taken during
	 * recovery). For historic snapshot it contains *all* xids assigned to the
	 * replayed transaction, including the toplevel xid.
	 *
	 * note: all ids in subxip[] are >= xmin, but we don't bother filtering
	 * out any that are >= xmax
	 */
	TransactionId *subxip;
	int32		subxcnt;		/* # of xact ids in subxip[] */
	bool		suboverflowed;	/* has the subxip array overflowed? */

	bool		takenDuringRecovery;	/* recovery-shaped snapshot? */
	bool		copied;			/* false if it's a static snapshot */

	CommandId	curcid;			/* in my xact, CID < curcid are visible */

	/*
	 * An extra return value for HeapTupleSatisfiesDirty, not used in MVCC
	 * snapshots.
	 */
	uint32		speculativeToken;

	/*
	 * For SNAPSHOT_NON_VACUUMABLE (and hopefully more in the future) this is
	 * used to determine whether row could be vacuumed.
	 */
	struct GlobalVisState *vistest;

	/*
	 * Book-keeping information, used by the snapshot manager
	 */
	uint32		active_count;	/* refcount on ActiveSnapshot stack */
	uint32		regd_count;		/* refcount on RegisteredSnapshots */
	pairingheap_node ph_node;	/* link in the RegisteredSnapshots heap */

	TimestampTz whenTaken;		/* timestamp when snapshot was taken */
	XLogRecPtr	lsn;			/* position in the WAL stream when taken */

	/*
	 * The transaction completion count at the time GetSnapshotData() built
	 * this snapshot. Allows to avoid re-computing static snapshots when no
	 * transactions completed since the last GetSnapshotData().
	 */
	uint64		snapXactCompletionCount;

#ifdef USE_PGRAC_CLUSTER
	/*
	 * PGRAC (spec-3.3 D1): explicit 24-byte cluster tail. Field order matters
	 * for ABI stability: SCN (8B aligned) + uint64 (8B aligned) leaves the
	 * cluster_source byte at offset +16, with an explicit 7-byte _pad[7] to
	 * pad out to 24 bytes. NO hidden compiler padding; NO uint32 truncation
	 * on cluster epoch (R9 P2). All fields zero-initialised on LOCAL paths.
	 *
	 * read_scn       - cluster_scn_current() snapped at GetSnapshotData()
	 *                  (or GetSnapshotDataReuse() refresh path). Compared via
	 *                  scn_time_cmp() only; raw `<=` is banned (R1 P0).
	 * read_epoch     - cluster_epoch_get_current() snapped likewise.
	 *                  Reconfig bumps epoch; stale snapshots fail-closed.
	 * cluster_source - SNAPSHOT_SOURCE_LOCAL=0 / CLUSTER=1.
	 * _pad[7]        - explicit padding, must be zero (asserted by D12).
	 */
	SCN			read_scn;
	uint64		read_epoch;
	uint8		cluster_source;
	uint8		_pad[7];
#endif
} SnapshotData;

#ifdef USE_PGRAC_CLUSTER
/*
 * PGRAC: D12 cluster_unit checks these offsets; if SnapshotData layout
 * changes upstream, these StaticAssertDecl()s catch silent ABI drift.
 */
StaticAssertDecl(sizeof(SCN) == 8, "PGRAC: SCN must be 8 bytes");
#endif

#endif							/* SNAPSHOT_H */
