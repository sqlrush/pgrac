/*-------------------------------------------------------------------------
 *
 * cluster_page_rmgr.h
 *	  RF-PAGE PGDEL-02 — applicable-redo rmgr census + PageVersion hints
 *	  decoder.
 *
 *	  Spec: specs/spec-rf-page-crash-safe-page-replay-journal.md
 *	  §2.1 PGDEL-02 ("applicable redo expected-before/result-version
 *	  producer + decoder；rmgr census 未实现"), §3.1 (SCN/LSN are
 *	  locator/hint only), §3.4 (the redo decoder must declare the
 *	  deterministic mutation under its class), §4.1 (the closed class
 *	  table), §11.1 G1/G3 (nothing here claims the existing replay path
 *	  is PageVersion-compliant).
 *
 *	  DELIVERED HERE:
 *	    - the rmgr census: every rmgr/opcode is either a census row
 *	      (page-affecting class assignment + byte-for-byte differential
 *	      evidence state) or explicitly unknown (fail-closed UNKNOWN);
 *	    - census -> classifier wiring: only rows with verified
 *	      byte-for-byte delta evidence (the existing t/256-differenced
 *	      apply matrix) register into the PGDEL-01 known-set, so the
 *	      closed classifier emits NORMAL exactly for those;
 *	    - cluster_page_redo_decode: extracts the PageIdentity from the
 *	      record's block reference and the §3.1 HINTS (xl_scn, record
 *	      LSN, FPI/WILL_INIT attributes) — explicitly NOT VersionTokens.
 *
 *	  NOT DELIVERED HERE (stays RED, later PGDEL items):
 *	    - the expected-before/result-version VersionToken producer
 *	      contract (needs the §3.1 producer/consumer census proof;
 *	      raw xl_scn/pd_block_scn must not be treated as VersionToken);
 *	    - deterministic-mutation declarations (decoder_registered is
 *	      false for every row — the §3.4 declaration belongs with the
 *	      apply chain, PGDEL-03/06);
 *	    - any mutation, durability, post-read or release.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_page_rmgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PAGE_RMGR_H
#define CLUSTER_PAGE_RMGR_H

#include "access/xlogreader.h"
#include "cluster/cluster_page_version.h"
#include "cluster/cluster_scn.h" /* SCN type */

/*
 * One census row.  `rmid` + `opcode` identify the record (opcode is the
 * info byte after XLR_INFO_MASK); `opcode_mask` allows one row to cover a
 * family (0 = exact opcode match).  A row answers the §4.1 closed-table
 * questions without guessing:
 *
 *   affects_page   the record can carry a data-page block reference
 *                  (SLRU/relmap pages are page-affecting but HEADER-class
 *                  with a typed owner — never generic page replay).
 *   page_class     the §4.1 row the record belongs to when it affects a
 *                  page.
 *   known_delta    the (rmid, opcode) single-block delta passed the
 *                  byte-for-byte differential against PG real redo
 *                  (t/256 evidence; this is the ONLY row state that
 *                  registers into the PGDEL-01 classifier known-set).
 *   will_init      the record carries the full page re-initialization
 *                  semantic (heap XLOG_HEAP_INIT_PAGE).  This is an
 *                  ATTRIBUTE row fact, not a §4.6 full-init rule: the
 *                  exact expected class state + result-version proof
 *                  (the rule) is still RED (PU-17 stays UNKNOWN).
 *   decoder_registered  false for every row until the §3.4 deterministic-
 *                  mutation declaration + VersionToken producer contract
 *                  land with the apply chain (PGDEL-03/06).
 */
typedef struct ClusterPageRmgrCensusEntry {
	uint8 rmid;
	uint16 opcode;
	uint16 opcode_mask;
	ClusterPageClass page_class;
	bool affects_page;
	bool known_delta;
	bool will_init;
	bool decoder_registered;
} ClusterPageRmgrCensusEntry;

/*
 * Census lookup: exactly one row per (rmid, opcode), or false when the
 * record is not in the census (unknown rmgr/opcode -> the classifier and
 * the decoder must fail closed, spec §4.1).
 */
extern bool cluster_page_rmgr_census_lookup(uint8 rmid, uint16 opcode,
											ClusterPageRmgrCensusEntry *out);

/*
 * Census -> PGDEL-01 classifier wiring.  Registers every census row whose
 * single-block delta has byte-for-byte differential evidence into the
 * classifier known-set (idempotent).  Production callers invoke this once
 * at startup; until then the classifier's registry stays empty and every
 * main-fork record classifies UNKNOWN (honest pre-census default).
 */
extern void cluster_page_rmgr_populate_known_set(void);

/*
 * §3.1 hints — locator/hint only, NEVER VersionToken.  `record_scn` is
 * xl_scn (spec-4.5: stamped by XLogInsertRecord, non-decreasing in LSN
 * order within a thread); `page_scn`/`page_lsn` come from the working
 * page (pd_block_scn/pd_lsn) and are filled by the caller who reads the
 * page.  No ordering semantics live in this struct; the exact
 * expected-before -> result-version edge topology is the apply chain's
 * contract (PGDEL-03/06).
 */
typedef struct ClusterPageRedoHints {
	SCN record_scn;		   /* xl_scn from the record header */
	XLogRecPtr record_lsn; /* record EndRecPtr */
	SCN page_scn;		   /* pd_block_scn of the working page (caller) */
	XLogRecPtr page_lsn;   /* pd_lsn of the working page (caller) */
} ClusterPageRedoHints;

typedef struct ClusterPageRedoDecoded {
	ClusterPageClass page_class;  /* census row assignment */
	ClusterPageIdentity identity; /* from the record block reference */
	ClusterPageRedoHints hints;
	bool full_image; /* XLR block image present AND applies */
	bool will_init;	 /* BKPBLOCK_WILL_INIT attribute */
} ClusterPageRedoDecoded;

/*
 * Decode one record's page-affecting facts for ONE block reference.
 * Returns false (fail-closed) when the record is not in the census, does
 * not affect pages, or has no usable block reference for block_id.
 * On true, `out` carries the class row assignment + identity + hints;
 * it performs NO mutation, NO version judgement and NO release.
 */
extern bool cluster_page_redo_decode(XLogReaderState *record, uint8 block_id,
									 ClusterPageRedoDecoded *out);

#endif /* CLUSTER_PAGE_RMGR_H */
