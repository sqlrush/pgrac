/*-------------------------------------------------------------------------
 *
 * bufpage.h
 *	  Standard POSTGRES buffer page definitions.
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/bufpage.h
 *
 *-------------------------------------------------------------------------
 *
 * PGRAC MODIFICATIONS (5th):
 *	Modified by: SqlRush <sqlrush@gmail.com>
 *
 *	What changed:  When USE_PGRAC_CLUSTER is defined, extend
 *	               PageHeaderData with an 8-byte pd_block_scn field
 *	               (block-level SCN) and bump PG_PAGE_LAYOUT_VERSION
 *	               4 -> 5.
 *	Why:           Stage 1.4 introduces cluster-aware MVCC SCN tracking
 *	               at the page level; the +8B field is the on-disk
 *	               anchor for spec-1.16's local_scn maintenance.  The
 *	               layout version bump uses PG's existing
 *	               pd_pagesize_version sanity check (every page header
 *	               byte 14-15) to make pgrac binary refuse vanilla PG
 *	               16 datafiles (and vice versa) with a clear FATAL,
 *	               instead of silent corruption.  Stage 1.4 only writes
 *	               InvalidScn (0) as a placeholder; spec-1.16 takes
 *	               over real SCN advancement.
 *	               See docs/block-format-design.md v1.1 §4.2 + §15
 *	               stage 1, docs/scn-protocol-design.md v1.1 §3.2,
 *	               specs/spec-1.4-block-format-pageheader-scn.md.
 *
 * PGRAC MODIFICATIONS (7th):
 *	Modified by: SqlRush <sqlrush@gmail.com>
 *
 *	What changed:  When USE_PGRAC_CLUSTER is defined, declare:
 *	               1. PageInitHeapPage extern -- heap-specific PageInit
 *	                  variant that calls PG PageInit with specialSize
 *	                  = CLUSTER_ITL_ARRAY_SIZE (384), placing 8 ITL
 *	                  slots in PG's special area at the page tail.
 *	               2. ClusterPageGetItlSlots inline helper that wraps
 *	                  PageGetSpecialPointer with PD_HAS_ITL +
 *	                  PageGetSpecialSize asserts.
 *	               3. PageHasItl inline helper.
 *	               4. PD_HAS_ITL flag bit (0x0008) -- set by
 *	                  PageInitHeapPage; reader uses it to distinguish
 *	                  heap pages (special = ITL) from index pages
 *	                  (special = btree/hash/gin opaque).
 *	Why:           Stage 1.5 introduces the ITL slot array for Oracle-
 *	               style row-level locking + per-block transaction
 *	               queue.  Heap pages get 384 B ITL in the PG special
 *	               area at the page tail (NOT directly after the
 *	               PageHeader -- doing so would break PG's pd_linp
 *	               array access); index pages keep using their own
 *	               special area for btree/hash/etc. opaque data.
 *	               PageInitHeapPage carries the heap-specific knowledge
 *	               (which PageInit cannot, since it's called from both
 *	               heap am and index am).  Stage 1.5 only writes
 *	               placeholder ITL values (ITL_FLAG_FREE / InvalidScn /
 *	               InvalidUba); Stage 3 (AD-006 第五轮) implements
 *	               actual writes / reuse / cleanout.
 *
 *	               PIVOT A (2026-05-02): user-mandated relocation of
 *	               ITL from "after PageHeader" to "in special area".
 *	               The original layout broke PG's pd_linp[] struct
 *	               access (PageGetMaxOffsetNumber / PageGetItemId etc.
 *	               assume pd_linp starts at offsetof = SizeOfPageHeader-
 *	               Data); special area is the only ABI-safe place to
 *	               put 384 B of per-heap-page metadata.  This is also
 *	               how btree (BTPageOpaque) / hash / gist / brin /
 *	               spgist / gin already use the special area.
 *	               See specs/spec-1.5-itl-slot.md §1.4 例外说明 #6,
 *	               docs/block-format-design.md v1.2 §4.1 layout.
 *
 * PGRAC MODIFICATIONS (Nth, stage 1.22):
 *	Modified by: SqlRush <sqlrush@gmail.com>
 *
 *	What changed:  When USE_PGRAC_CLUSTER is defined, declare:
 *	               1. PD_UNDO_SEG_HEADER flag bit (0x0010) -- set by
 *	                  PageInitUndoSegmentHeader; reader uses it to
 *	                  identify block 0 of an undo segment file
 *	                  (pg_undo/instance_<N>/seg_<id>.dat).  Mutually
 *	                  exclusive with PD_HAS_ITL by relation type.
 *	               2. PD_VALID_FLAG_BITS bumped 0x000F -> 0x001F to
 *	                  account for the new bit (cluster mode only).
 *	               3. PageInitUndoSegmentHeader extern -- writes a
 *	                  freshly-allocated UndoSegmentHeaderData layout
 *	                  to the page (delegates byte generation to the
 *	                  frontend-safe helper in cluster_undo_segment_init.h
 *	                  so backend and initdb produce byte-identical pages).
 *	               4. PageIsUndoSegmentHeader inline helper.
 *	Why:           Stage 1.22 ships the dedicated undo tablespace
 *	               (pg_undo OID 9100; UNDOTABLESPACE_OID per
 *	               Hardening v1.0.2 OID conflict resolution) + atomic
 *	               batch on-disk format change.  block 0 of every
 *	               seg_<id>.dat is laid out
 *	               as UndoSegmentHeaderData (cluster_undo_segment.h);
 *	               PD_UNDO_SEG_HEADER lets tooling and visibility paths
 *	               distinguish three page kinds (vanilla index / ITL
 *	               heap / undo segment header).  Stage 1.22 only writes
 *	               placeholder TT slots (TT_SLOT_UNUSED) + zero
 *	               retention/statistics fields; feature-117 activates
 *	               real TT slot allocation and retention.
 *	               See specs/spec-1.22-undo-tablespace-bootstrap.md §2.1
 *	               + §2.2 + §D1, docs/undo-segment-design.md §3.4-§3.6.
 *
 * PGRAC MODIFICATIONS (stage 4.5a):
 *	Modified by: SqlRush <sqlrush@gmail.com>
 *
 *	What changed:  When USE_PGRAC_CLUSTER is defined (backend only),
 *	               PageSetLSN additionally stamps the merged-recovery
 *	               window SCN into pd_block_scn while the window is
 *	               active, and the two window globals are declared.
 *	Why:           spec-4.5a §3.3b -- inside k-way merged replay the
 *	               page freshness authority is the per-record SCN
 *	               (cross-thread LSNs are incomparable); the central
 *	               PageSetLSN hook makes every redo handler stamp it
 *	               without per-rmgr changes.  Backends never enter the
 *	               window, so the hook is a predictable-false branch.
 */
#ifndef BUFPAGE_H
#define BUFPAGE_H

#include "access/xlogdefs.h"
#include "storage/block.h"
#include "storage/item.h"
#include "storage/off.h"

#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_scn.h"	  /* SCN typedef + InvalidScn (stage 1.4) */
#include "cluster/cluster_itl_slot.h" /* ClusterItlSlotData + CLUSTER_ITL_ARRAY_SIZE (stage 1.5) */
#endif

/*
 * A postgres disk page is an abstraction layered on top of a postgres
 * disk block (which is simply a unit of i/o, see block.h).
 *
 * specifically, while a disk block can be unformatted, a postgres
 * disk page is always a slotted page of the form:
 *
 * +----------------+---------------------------------+
 * | PageHeaderData | linp1 linp2 linp3 ...           |
 * +-----------+----+---------------------------------+
 * | ... linpN |									  |
 * +-----------+--------------------------------------+
 * |		   ^ pd_lower							  |
 * |												  |
 * |			 v pd_upper							  |
 * +-------------+------------------------------------+
 * |			 | tupleN ...                         |
 * +-------------+------------------+-----------------+
 * |	   ... tuple3 tuple2 tuple1 | "special space" |
 * +--------------------------------+-----------------+
 *									^ pd_special
 *
 * a page is full when nothing can be added between pd_lower and
 * pd_upper.
 *
 * all blocks written out by an access method must be disk pages.
 *
 * EXCEPTIONS:
 *
 * obviously, a page is not formatted before it is initialized by
 * a call to PageInit.
 *
 * NOTES:
 *
 * linp1..N form an ItemId (line pointer) array.  ItemPointers point
 * to a physical block number and a logical offset (line pointer
 * number) within that block/page.  Note that OffsetNumbers
 * conventionally start at 1, not 0.
 *
 * tuple1..N are added "backwards" on the page.  Since an ItemPointer
 * offset is used to access an ItemId entry rather than an actual
 * byte-offset position, tuples can be physically shuffled on a page
 * whenever the need arises.  This indirection also keeps crash recovery
 * relatively simple, because the low-level details of page space
 * management can be controlled by standard buffer page code during
 * logging, and during recovery.
 *
 * AM-generic per-page information is kept in PageHeaderData.
 *
 * AM-specific per-page data (if any) is kept in the area marked "special
 * space"; each AM has an "opaque" structure defined somewhere that is
 * stored as the page trailer.  an access method should always
 * initialize its pages with PageInit and then set its own opaque
 * fields.
 */

typedef Pointer Page;


/*
 * location (byte offset) within a page.
 *
 * note that this is actually limited to 2^15 because we have limited
 * ItemIdData.lp_off and ItemIdData.lp_len to 15 bits (see itemid.h).
 */
typedef uint16 LocationIndex;


/*
 * For historical reasons, the 64-bit LSN value is stored as two 32-bit
 * values.
 */
typedef struct {
	uint32 xlogid;	/* high bits */
	uint32 xrecoff; /* low bits */
} PageXLogRecPtr;

static inline XLogRecPtr
PageXLogRecPtrGet(PageXLogRecPtr val)
{
	return (uint64)val.xlogid << 32 | val.xrecoff;
}

#define PageXLogRecPtrSet(ptr, lsn)                                                                \
	((ptr).xlogid = (uint32)((lsn) >> 32), (ptr).xrecoff = (uint32)(lsn))

/*
 * disk page organization
 *
 * space management information generic to any page
 *
 *		pd_lsn		- identifies xlog record for last change to this page.
 *		pd_checksum - page checksum, if set.
 *		pd_flags	- flag bits.
 *		pd_lower	- offset to start of free space.
 *		pd_upper	- offset to end of free space.
 *		pd_special	- offset to start of special space.
 *		pd_pagesize_version - size in bytes and page layout version number.
 *		pd_prune_xid - oldest XID among potentially prunable tuples on page.
 *
 * The LSN is used by the buffer manager to enforce the basic rule of WAL:
 * "thou shalt write xlog before data".  A dirty buffer cannot be dumped
 * to disk until xlog has been flushed at least as far as the page's LSN.
 *
 * pd_checksum stores the page checksum, if it has been set for this page;
 * zero is a valid value for a checksum. If a checksum is not in use then
 * we leave the field unset. This will typically mean the field is zero
 * though non-zero values may also be present if databases have been
 * pg_upgraded from releases prior to 9.3, when the same byte offset was
 * used to store the current timelineid when the page was last updated.
 * Note that there is no indication on a page as to whether the checksum
 * is valid or not, a deliberate design choice which avoids the problem
 * of relying on the page contents to decide whether to verify it. Hence
 * there are no flag bits relating to checksums.
 *
 * pd_prune_xid is a hint field that helps determine whether pruning will be
 * useful.  It is currently unused in index pages.
 *
 * The page version number and page size are packed together into a single
 * uint16 field.  This is for historical reasons: before PostgreSQL 7.3,
 * there was no concept of a page version number, and doing it this way
 * lets us pretend that pre-7.3 databases have page version number zero.
 * We constrain page sizes to be multiples of 256, leaving the low eight
 * bits available for a version number.
 *
 * Minimum possible page size is perhaps 64B to fit page header, opaque space
 * and a minimal tuple; of course, in reality you want it much bigger, so
 * the constraint on pagesize mod 256 is not an important restriction.
 * On the high end, we can only support pages up to 32KB because lp_off/lp_len
 * are 15 bits.
 */

typedef struct PageHeaderData {
	/* XXX LSN is member of *any* block, not only page-organized ones */
	PageXLogRecPtr pd_lsn;	  /* LSN: next byte after last byte of xlog
								 * record for last change to this page */
	uint16 pd_checksum;		  /* checksum */
	uint16 pd_flags;		  /* flag bits, see below */
	LocationIndex pd_lower;	  /* offset to start of free space */
	LocationIndex pd_upper;	  /* offset to end of free space */
	LocationIndex pd_special; /* offset to start of special space */
	uint16 pd_pagesize_version;
	TransactionId pd_prune_xid; /* oldest prunable XID, or zero if none */
#ifdef USE_PGRAC_CLUSTER
	/*
	 * PGRAC (stage 1.4): block-level SCN for cluster-aware MVCC.
	 *
	 *	Placeholder InvalidScn (0) at stage 1.4; spec-1.16 takes over
	 *	real value advancement (commit_scn-style writeback per AD-008).
	 *	8 bytes on 64-bit ABI; offset 24 from start of header (after
	 *	pd_prune_xid) -> SizeOfPageHeaderData becomes 32 (was 24).
	 */
	SCN pd_block_scn;
#endif
	ItemIdData pd_linp[FLEXIBLE_ARRAY_MEMBER]; /* line pointer array */
} PageHeaderData;

typedef PageHeaderData *PageHeader;

/*
 * pd_flags contains the following flag bits.  Undefined bits are initialized
 * to zero and may be used in the future.
 *
 * PD_HAS_FREE_LINES is set if there are any LP_UNUSED line pointers before
 * pd_lower.  This should be considered a hint rather than the truth, since
 * changes to it are not WAL-logged.
 *
 * PD_PAGE_FULL is set if an UPDATE doesn't find enough free space in the
 * page for its new tuple version; this suggests that a prune is needed.
 * Again, this is just a hint.
 */
#define PD_HAS_FREE_LINES 0x0001 /* are there any unused line pointers? */
#define PD_PAGE_FULL 0x0002		 /* not enough free space for new tuple? */
#define PD_ALL_VISIBLE                                                                             \
	0x0004 /* all tuples on page are visible to
									 * everyone */

#ifdef USE_PGRAC_CLUSTER
/*
 * PGRAC (stage 1.5): heap pages contain an ITL slot array immediately
 * after the PageHeader.  Set by PageInitHeapPage (heap am path);
 * cleared / never-set by PageInit (index am path).  Reader uses this
 * bit to know whether (page + SizeOfPageHeaderData) is the start of
 * 384 bytes of ClusterItlSlotData or the start of pd_linp.
 */
#define PD_HAS_ITL 0x0008
/*
 * PGRAC (stage 1.22): block 0 of every undo segment file
 * (pg_undo/instance_<N>/seg_<id>.dat) is laid out as
 * UndoSegmentHeaderData (cluster_undo_segment.h).  Set by
 * PageInitUndoSegmentHeader (cluster_undo_alloc.c path); never set
 * by heap or index paths.  Mutually exclusive with PD_HAS_ITL by
 * relation type — heap pages have ITL slots, undo segment header
 * pages have segment metadata + 48 TT slots.
 *
 * Tooling (pg_filedump etc.) can distinguish 3 page kinds:
 *   vanilla index    : neither bit
 *   ITL heap         : PD_HAS_ITL only
 *   undo seg header  : PD_UNDO_SEG_HEADER only
 *
 * Spec: spec-1.22-undo-tablespace-bootstrap.md §2.1 Q-1 ★ A.
 */
#define PD_UNDO_SEG_HEADER 0x0010
/*
 * PGRAC (spec-4.5 §3.3d): force a full-page image on the next
 * WAL-logged modification of this page, regardless of the (possibly
 * foreign-thread, hence incomparable) page LSN.  Set when a GCS block
 * ship installs a remote image; cleared ONLY after an FPI with
 * BKPIMAGE_APPLY was actually emitted AND the insertion returned a
 * valid EndPos (xloginsert.c).  Persistent: rides the page to shared
 * disk, so eviction/reload/crash keep the guarantee.  Five-rule
 * semantics in the spec (round-7): REGBUF_FORCE_IMAGE/NO_IMAGE/
 * WILL_INIT and !doPageWrites keep their existing priority over this
 * bit; replay restoring a set bit costs one benign extra FPI.
 */
#define PD_CLUSTER_FORCE_FPI 0x0020
#define PD_VALID_FLAG_BITS 0x003F /* OR of all valid pd_flags bits */
#else
#define PD_VALID_FLAG_BITS 0x0007 /* OR of all valid pd_flags bits */
#endif

/*
 * Page layout version number 0 is for pre-7.3 Postgres releases.
 * Releases 7.3 and 7.4 use 1, denoting a new HeapTupleHeader layout.
 * Release 8.0 uses 2; it changed the HeapTupleHeader layout again.
 * Release 8.1 uses 3; it redefined HeapTupleHeader infomask bits.
 * Release 8.3 uses 4; it changed the HeapTupleHeader layout again, and
 *		added the pd_flags field (by stealing some bits from pd_tli),
 *		as well as adding the pd_prune_xid field (which enlarges the header).
 *
 * As of Release 9.3, the checksum version must also be considered when
 * handling pages.
 *
 * pgrac (stage 1.4) uses 5: extends PageHeaderData with pd_block_scn
 *		(8 bytes), enlarging SizeOfPageHeaderData from 24 to 32.  The
 *		layout version bump intentionally makes pgrac binary refuse
 *		vanilla PG 16 datafiles (and vice versa) via PG's existing
 *		pd_pagesize_version sanity check.  See docs/block-format-design.md
 *		v1.1 §15 stage 1 + spec-1.4.
 */
#ifdef USE_PGRAC_CLUSTER
#define PG_PAGE_LAYOUT_VERSION 5
#else
#define PG_PAGE_LAYOUT_VERSION 4
#endif
#define PG_DATA_CHECKSUM_VERSION 1

/* ----------------------------------------------------------------
 *						page support functions
 * ----------------------------------------------------------------
 */

/*
 * line pointer(s) do not count as part of header
 */
#define SizeOfPageHeaderData (offsetof(PageHeaderData, pd_linp))

/*
 * PageIsEmpty
 *		returns true iff no itemid has been allocated on the page
 *
 *	NOTE (PGRAC stage 1.5): this checks pd_lower against the ITL-free
 *	header size.  Heap pages with PD_HAS_ITL still pass this test if
 *	pd_lower has not been advanced past SizeOfPageHeaderWithItl, but
 *	heap am callers should use PageIsEmpty in conjunction with
 *	PageHasItl() when they care whether the ITL array is present.
 *	Most existing callers (vacuum, FSM) treat "empty" as "no rows yet"
 *	and the unchanged check is correct for both heap and index pages.
 */
static inline bool
PageIsEmpty(Page page)
{
	return ((PageHeader)page)->pd_lower <= SizeOfPageHeaderData;
}

/*
 * PageIsNew
 *		returns true iff page has not been initialized (by PageInit)
 */
static inline bool
PageIsNew(Page page)
{
	return ((PageHeader)page)->pd_upper == 0;
}

/*
 * PageGetItemId
 *		Returns an item identifier of a page.
 */
static inline ItemId
PageGetItemId(Page page, OffsetNumber offsetNumber)
{
	return &((PageHeader)page)->pd_linp[offsetNumber - 1];
}

/*
 * PageGetContents
 *		To be used in cases where the page does not contain line pointers.
 *
 * Note: prior to 8.3 this was not guaranteed to yield a MAXALIGN'd result.
 * Now it is.  Beware of old code that might think the offset to the contents
 * is just SizeOfPageHeaderData rather than MAXALIGN(SizeOfPageHeaderData).
 */
static inline char *
PageGetContents(Page page)
{
	return (char *)page + MAXALIGN(SizeOfPageHeaderData);
}

/* ----------------
 *		functions to access page size info
 * ----------------
 */

/*
 * PageGetPageSize
 *		Returns the page size of a page.
 *
 * this can only be called on a formatted page (unlike
 * BufferGetPageSize, which can be called on an unformatted page).
 * however, it can be called on a page that is not stored in a buffer.
 */
static inline Size
PageGetPageSize(Page page)
{
	return (Size)(((PageHeader)page)->pd_pagesize_version & (uint16)0xFF00);
}

/*
 * PageGetPageLayoutVersion
 *		Returns the page layout version of a page.
 */
static inline uint8
PageGetPageLayoutVersion(Page page)
{
	return (((PageHeader)page)->pd_pagesize_version & 0x00FF);
}

/*
 * PageSetPageSizeAndVersion
 *		Sets the page size and page layout version number of a page.
 *
 * We could support setting these two values separately, but there's
 * no real need for it at the moment.
 */
static inline void
PageSetPageSizeAndVersion(Page page, Size size, uint8 version)
{
	Assert((size & 0xFF00) == size);
	Assert((version & 0x00FF) == version);

	((PageHeader)page)->pd_pagesize_version = size | version;
}

/* ----------------
 *		page special data functions
 * ----------------
 */
/*
 * PageGetSpecialSize
 *		Returns size of special space on a page.
 */
static inline uint16
PageGetSpecialSize(Page page)
{
	return (PageGetPageSize(page) - ((PageHeader)page)->pd_special);
}

/*
 * Using assertions, validate that the page special pointer is OK.
 *
 * This is intended to catch use of the pointer before page initialization.
 */
static inline void
PageValidateSpecialPointer(Page page)
{
	Assert(page);
	Assert(((PageHeader)page)->pd_special <= BLCKSZ);
	Assert(((PageHeader)page)->pd_special >= SizeOfPageHeaderData);
}

/*
 * PageGetSpecialPointer
 *		Returns pointer to special space on a page.
 */
static inline char *
PageGetSpecialPointer(Page page)
{
	PageValidateSpecialPointer(page);
	return (char *)page + ((PageHeader)page)->pd_special;
}

/*
 * PageGetItem
 *		Retrieves an item on the given page.
 *
 * Note:
 *		This does not change the status of any of the resources passed.
 *		The semantics may change in the future.
 */
static inline Item
PageGetItem(Page page, ItemId itemId)
{
	Assert(page);
	Assert(ItemIdHasStorage(itemId));

	return (Item)(((char *)page) + ItemIdGetOffset(itemId));
}

/*
 * PageGetMaxOffsetNumber
 *		Returns the maximum offset number used by the given page.
 *		Since offset numbers are 1-based, this is also the number
 *		of items on the page.
 *
 *		NOTE: if the page is not initialized (pd_lower == 0), we must
 *		return zero to ensure sane behavior.
 */
static inline OffsetNumber
PageGetMaxOffsetNumber(Page page)
{
	PageHeader pageheader = (PageHeader)page;

	if (pageheader->pd_lower <= SizeOfPageHeaderData)
		return 0;
	else
		return (pageheader->pd_lower - SizeOfPageHeaderData) / sizeof(ItemIdData);
}

/*
 * Additional functions for access to page headers.
 */
static inline XLogRecPtr
PageGetLSN(Page page)
{
	return PageXLogRecPtrGet(((PageHeader)page)->pd_lsn);
}
#if defined(USE_PGRAC_CLUSTER) && !defined(FRONTEND)
/*
 * PGRAC: spec-4.5a §3.3b -- merged-recovery window state, defined in
 * cluster_recovery_merge.c.  Declared as plain externs so this core
 * header stays free of cluster includes; outside the startup process's
 * merged-replay window every reader sees false/0 (backends never enter
 * the window), so the PageSetLSN hook below is a predictable-false
 * branch on all hot paths.
 */
extern bool cluster_recmerge_window_active;
extern uint64 cluster_recmerge_window_scn;
extern uint64 cluster_recmerge_window_own_lsn;
extern bool cluster_recmerge_apply_foreign;
#endif

static inline void
PageSetLSN(Page page, XLogRecPtr lsn)
{
#if defined(USE_PGRAC_CLUSTER) && !defined(FRONTEND)
	/*
	 * PGRAC: spec-4.5a §3.3b -- a FOREIGN record's lsn is in the peer's
	 * WAL sequence and lies beyond this node's own WAL flush point, so
	 * clamp the materialized page's pd_lsn to the own recovery redo
	 * (durable, comparable).  Otherwise the end-of-recovery checkpoint's
	 * FlushBuffer would demand an unsatisfiable XLogFlush of the peer's
	 * LSN.  pd_block_scn (below) stays the window's freshness authority.
	 */
	if (cluster_recmerge_window_active && cluster_recmerge_apply_foreign)
		lsn = (XLogRecPtr) cluster_recmerge_window_own_lsn;
#endif
	PageXLogRecPtrSet(((PageHeader)page)->pd_lsn, lsn);
#if defined(USE_PGRAC_CLUSTER) && !defined(FRONTEND)

	/*
	 * Inside the merged-replay window every applied record stamps its SCN
	 * as the page's freshness watermark.  Cross-thread pd_lsn values are
	 * incomparable, so pd_block_scn is the window's ordering authority
	 * (XLogReadBufferForRedoExtended judges BLK_DONE/BLK_NEEDS_REDO by it
	 * inside the window); the stamp also survives a window crash-rerun,
	 * making re-applied records skip.
	 */
	if (cluster_recmerge_window_active)
		((PageHeader)page)->pd_block_scn = (SCN)cluster_recmerge_window_scn;
#endif
}

static inline bool
PageHasFreeLinePointers(Page page)
{
	return ((PageHeader)page)->pd_flags & PD_HAS_FREE_LINES;
}
static inline void
PageSetHasFreeLinePointers(Page page)
{
	((PageHeader)page)->pd_flags |= PD_HAS_FREE_LINES;
}
static inline void
PageClearHasFreeLinePointers(Page page)
{
	((PageHeader)page)->pd_flags &= ~PD_HAS_FREE_LINES;
}

static inline bool
PageIsFull(Page page)
{
	return ((PageHeader)page)->pd_flags & PD_PAGE_FULL;
}
static inline void
PageSetFull(Page page)
{
	((PageHeader)page)->pd_flags |= PD_PAGE_FULL;
}
static inline void
PageClearFull(Page page)
{
	((PageHeader)page)->pd_flags &= ~PD_PAGE_FULL;
}

static inline bool
PageIsAllVisible(Page page)
{
	return ((PageHeader)page)->pd_flags & PD_ALL_VISIBLE;
}
static inline void
PageSetAllVisible(Page page)
{
	((PageHeader)page)->pd_flags |= PD_ALL_VISIBLE;
}
static inline void
PageClearAllVisible(Page page)
{
	((PageHeader)page)->pd_flags &= ~PD_ALL_VISIBLE;
}

#ifdef USE_PGRAC_CLUSTER
/* PGRAC spec-4.5 §3.3d: force-FPI bit accessors. */
static inline bool
PageHasForceFpi(Page page)
{
	return (((PageHeader)page)->pd_flags & PD_CLUSTER_FORCE_FPI) != 0;
}
static inline void
PageSetForceFpi(Page page)
{
	((PageHeader)page)->pd_flags |= PD_CLUSTER_FORCE_FPI;
}
static inline void
PageClearForceFpi(Page page)
{
	((PageHeader)page)->pd_flags &= ~PD_CLUSTER_FORCE_FPI;
}
#endif

/*
 * These two require "access/transam.h", so left as macros.
 */
#define PageSetPrunable(page, xid)                                                                 \
	do {                                                                                           \
		Assert(TransactionIdIsNormal(xid));                                                        \
		if (!TransactionIdIsValid(((PageHeader)(page))->pd_prune_xid)                              \
			|| TransactionIdPrecedes(xid, ((PageHeader)(page))->pd_prune_xid))                     \
			((PageHeader)(page))->pd_prune_xid = (xid);                                            \
	} while (0)
#define PageClearPrunable(page) (((PageHeader)(page))->pd_prune_xid = InvalidTransactionId)


/* ----------------------------------------------------------------
 *		extern declarations
 * ----------------------------------------------------------------
 */

/* flags for PageAddItemExtended() */
#define PAI_OVERWRITE (1 << 0)
#define PAI_IS_HEAP (1 << 1)

/* flags for PageIsVerifiedExtended() */
#define PIV_LOG_WARNING (1 << 0)
#define PIV_REPORT_STAT (1 << 1)

#define PageAddItem(page, item, size, offsetNumber, overwrite, is_heap)                            \
	PageAddItemExtended(page, item, size, offsetNumber,                                            \
						((overwrite) ? PAI_OVERWRITE : 0) | ((is_heap) ? PAI_IS_HEAP : 0))

#define PageIsVerified(page, blkno)                                                                \
	PageIsVerifiedExtended(page, blkno, PIV_LOG_WARNING | PIV_REPORT_STAT)

/*
 * Check that BLCKSZ is a multiple of sizeof(size_t).  In
 * PageIsVerifiedExtended(), it is much faster to check if a page is
 * full of zeroes using the native word size.  Note that this assertion
 * is kept within a header to make sure that StaticAssertDecl() works
 * across various combinations of platforms and compilers.
 */
StaticAssertDecl(BLCKSZ == ((BLCKSZ / sizeof(size_t)) * sizeof(size_t)),
				 "BLCKSZ has to be a multiple of sizeof(size_t)");

extern void PageInit(Page page, Size pageSize, Size specialSize);
#ifdef USE_PGRAC_CLUSTER
/*
 * PGRAC (stage 1.5): heap-specific PageInit that allocates the 384B
 * ITL slot array immediately after the PageHeader.  All ITL slot
 * fields are written as placeholders (ITL_FLAG_FREE / InvalidScn /
 * InvalidUba / InvalidTransactionId / InvalidXLogRecPtr) per
 * spec-1.5 §3.2; Stage 3 (AD-006 第五轮) takes over real writes /
 * reuse / cleanout.
 *
 * pd_lower starts at SizeOfPageHeaderWithItl (= 416); PD_HAS_ITL is
 * set in pd_flags so readers can distinguish heap pages (with ITL)
 * from index pages (PageInit, no ITL).
 *
 * Heap am paths (heapam.c, hio.c) MUST call this; index am paths
 * (btree, hash, gin, gist, brin, spgist) MUST keep calling PageInit.
 */
extern void PageInitHeapPage(Page page, Size pageSize, Size specialSize);

/*
 * PageHasItl -- does this heap page carry an ITL slot array?
 *
 *	Read PD_HAS_ITL bit; only heap pages set it (PageInitHeapPage
 *	above).  Use this guard before ClusterPageGetItlSlots.
 *
 *	Index am pages (btree, hash, gin, gist, brin, spgist) have their
 *	own special area data (BTPageOpaque etc.) and MUST NOT have this
 *	bit set.  PG reuses the special area for am-specific metadata; the
 *	bit identifies which interpretation to use.
 */
static inline bool
PageHasItl(Page page)
{
	return (((PageHeader)page)->pd_flags & PD_HAS_ITL) != 0;
}

/*
 * ClusterPageGetItlSlots -- pointer to slot 0 of the per-page ITL array.
 *
 *	Returns PageGetSpecialPointer(page) cast to ClusterItlSlotData *.
 *	Stage 1.5 stores the 8-slot array (384 B) in PG's special area at
 *	the page tail, NOT directly after the PageHeader (PIVOT A
 *	2026-05-02; original layout broke PG's pd_linp[] struct access).
 *
 *	The asserts catch two classes of misuse:
 *	  1. PageHasItl(page) -- caller fed an index page (no ITL)
 *	  2. PageGetSpecialSize(page) >= CLUSTER_ITL_ARRAY_SIZE -- caller
 *	     fed a malformed heap page where the special area is smaller
 *	     than the ITL array (corruption / bug)
 */
static inline ClusterItlSlotData *
ClusterPageGetItlSlots(Page page)
{
	Assert(PageHasItl(page));
	/*
	 * The slot array occupies the FIRST CLUSTER_ITL_ARRAY_SIZE (384) bytes of
	 * the special area, so slot access only requires >= 384 -- NOT the full
	 * 392 special size (spec-3.10 §v0.5: the 8B ITL header trails the slots).
	 * Keeping this at CLUSTER_ITL_ARRAY_SIZE lets pre-v0.5 unit-test fixtures
	 * that reserve a 384-byte special area (and never touch the header) keep
	 * exercising the slot reader.  ClusterPageGetItlHeader below requires the
	 * full CLUSTER_ITL_SPECIAL_SIZE.
	 */
	Assert(PageGetSpecialSize(page) >= CLUSTER_ITL_ARRAY_SIZE);
	return (ClusterItlSlotData *)PageGetSpecialPointer(page);
}

/*
 * ClusterPageGetItlHeader -- pointer to the 8-byte per-page ITL header
 *	(spec-3.10 §v0.5), which lives at the END of the special area, right
 *	after the 384-byte slot array (special offset CLUSTER_ITL_ARRAY_SIZE).
 *	Carries itl_recycle_watermark_scn (slot-reuse fail-closed guard).
 */
static inline ClusterItlPageHeader *
ClusterPageGetItlHeader(Page page)
{
	Assert(PageHasItl(page));
	Assert(PageGetSpecialSize(page) >= CLUSTER_ITL_SPECIAL_SIZE);
	return (ClusterItlPageHeader *)((char *)PageGetSpecialPointer(page) + CLUSTER_ITL_ARRAY_SIZE);
}

/*
 * ClusterPageGetItlSlot -- the slot_idx-th ITL slot (0-based).
 *
 *	Stage 1.5 returns slots in ITL_FLAG_FREE state with all SCN /
 *	UBA / xid fields zero-init'd.  Stage 3 (AD-006 第五轮) populates
 *	real values during heap_insert / heap_update / heap_delete.
 *
 *	slot_idx in 0..CLUSTER_ITL_INITRANS_DEFAULT-1; out-of-range trips
 *	an Assert.  Heap tuple's t_itl_slot_idx == 255 means "no slot
 *	assigned" -- callers must not pass 255 here.
 */
static inline ClusterItlSlotData *
ClusterPageGetItlSlot(Page page, uint8 slot_idx)
{
	Assert(slot_idx < CLUSTER_ITL_INITRANS_DEFAULT);
	return &ClusterPageGetItlSlots(page)[slot_idx];
}

/*
 * PageInitUndoSegmentHeader -- initialize block 0 of an undo segment.
 *
 *	Stage 1.22: writes a freshly-allocated UndoSegmentHeaderData layout
 *	to `page` (8 KB).  Mirrors PageInitHeapPage (spec-1.5) and PageInit
 *	(vanilla) but for undo segment header blocks instead of heap / index
 *	pages.  Sets PD_UNDO_SEG_HEADER bit; specialSize is fixed to 0
 *	(undo segment header uses the entire 8 KB block as a fixed-layout
 *	struct; no special area).
 *
 *	Caller responsibilities:
 *	  - hold an exclusive lock on the buffer (or be running in
 *	    single-process initdb / bootstrap context)
 *	  - pageSize must equal BLCKSZ (asserted)
 *	  - owner_instance must be in [1, UNDO_OWNER_INSTANCE_MAX] (asserted)
 *
 *	Body delegates byte-generation to the frontend-safe helper
 *	cluster_undo_segment_make_header_bytes (cluster_undo_segment_init.h)
 *	so backend (this function) and frontend (initdb) write byte-identical
 *	pages.
 *
 *	Spec: spec-1.22-undo-tablespace-bootstrap.md §2.2 + §D2 (v0.2 Q-6 ★ A).
 */
extern void PageInitUndoSegmentHeader(Page page, Size pageSize, uint32 segment_id,
									  uint8 owner_instance);

/*
 * PageIsUndoSegmentHeader -- is this page block 0 of an undo segment?
 *
 *	Read PD_UNDO_SEG_HEADER bit; only set by PageInitUndoSegmentHeader
 *	above.  Mutually exclusive with PD_HAS_ITL (heap pages do not have
 *	segment headers and undo pages do not have ITL slots).
 *
 *	Use this guard before casting (UndoSegmentHeaderData *) page.
 */
static inline bool
PageIsUndoSegmentHeader(Page page)
{
	return (((PageHeader)page)->pd_flags & PD_UNDO_SEG_HEADER) != 0;
}
#endif
extern bool PageIsVerifiedExtended(Page page, BlockNumber blkno, int flags);
extern OffsetNumber PageAddItemExtended(Page page, Item item, Size size, OffsetNumber offsetNumber,
										int flags);
extern Page PageGetTempPage(Page page);
extern Page PageGetTempPageCopy(Page page);
extern Page PageGetTempPageCopySpecial(Page page);
extern void PageRestoreTempPage(Page tempPage, Page oldPage);
extern void PageRepairFragmentation(Page page);
extern void PageTruncateLinePointerArray(Page page);
extern Size PageGetFreeSpace(Page page);
extern Size PageGetFreeSpaceForMultipleTuples(Page page, int ntups);
extern Size PageGetExactFreeSpace(Page page);
extern Size PageGetHeapFreeSpace(Page page);
extern void PageIndexTupleDelete(Page page, OffsetNumber offnum);
extern void PageIndexMultiDelete(Page page, OffsetNumber *itemnos, int nitems);
extern void PageIndexTupleDeleteNoCompact(Page page, OffsetNumber offnum);
extern bool PageIndexTupleOverwrite(Page page, OffsetNumber offnum, Item newtup, Size newsize);
extern char *PageSetChecksumCopy(Page page, BlockNumber blkno);
extern void PageSetChecksumInplace(Page page, BlockNumber blkno);

#endif /* BUFPAGE_H */
