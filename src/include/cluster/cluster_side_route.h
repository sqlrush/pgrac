/*-------------------------------------------------------------------------
 *
 * cluster_side_route.h
 *	  RF-SIDE D-SIDE-01 — the total route registry + the single
 *	  §2.1 verdict surface.
 *
 *	  Spec: specs/spec-rf-side-typed-recovery-and-shared-terminal.md
 *	  §1.2 D-SIDE-01 ("single decoder、total route registry、cold/online
 *	  common primitive graph；unknown default BLOCKED"), §2.1 verdicts,
 *	  §3.2 routing matrix, §5.1 U-SIDE-01/02/03.
 *
 *	  DELIVERED HERE:
 *	    - the route registry: every rmgr is either a route row
 *	      (opcode-granular where the §3.2 matrix names opcodes, rmgr-
 *	      granular otherwise) or explicitly unknown; the unknown default
 *	      is BLOCKED (mutation=0, never released);
 *	    - cluster_side_route_lookup: exactly one row per (rmid, opcode)
 *	      or false;
 *	    - cluster_side_route_verdict: the §2.1 verdict (APPLY /
 *	      PROVED_NOOP / BLOCKED) — a PURE function of the row, so cold
 *	      and online wrappers produce the SAME route and verdict for the
 *	      same record (U-SIDE-02; the cold/online difference is process
 *	      severity only, never the route).
 *
 *	  The opcode known-set is the sole exhaustive STOP-06 generated
 *	  manifest; SIDE adds only its domain disposition.  Payload decoders,
 *	  production cold/online execution and observability stay with their
 *	  owning D-SIDE layers.  No catalog/page/WAL/wire ABI is touched.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_side_route.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SIDE_ROUTE_H
#define CLUSTER_SIDE_ROUTE_H

#include "access/xlogreader.h"

/*
 * §3.2 route kinds.  Every page-family record routes to RF-PAGE (the
 * SIDE only consumes the page verdict); transaction-family records
 * route to the TT/undo truth primitive; CLOG/MULTIXACT/COMMIT_TS route
 * to the derived-projection primitive; smgr lifecycle routes to the
 * canonical storage primitive; PROVED_NOOP carries its proof kind; and
 * everything without an exact route is BLOCKED (the matrix's
 * "unknown/failure direction" is always BLOCKED).
 */
typedef enum ClusterSideRouteKind {
	CLUSTER_SIDE_ROUTE_PAGE = 0,	/* RF-PAGE owns the mutation */
	CLUSTER_SIDE_ROUTE_TT_UNDO,		/* transaction/undo truth primitive */
	CLUSTER_SIDE_ROUTE_PROJECTION,	/* CLOG/MULTIXACT/COMMIT_TS projection */
	CLUSTER_SIDE_ROUTE_STORAGE,		/* canonical storage lifecycle */
	CLUSTER_SIDE_ROUTE_PROVED_NOOP, /* positive no-mutation proof */
	CLUSTER_SIDE_ROUTE_BLOCKED		/* mutation=0, resource never released */
} ClusterSideRouteKind;

typedef struct ClusterSideRouteRow {
	uint8 rmid;
	uint16 opcode;		/* 0 with opcode_mask==0 = whole rmgr */
	uint16 opcode_mask; /* 0 = exact opcode match */
	ClusterSideRouteKind kind;
	const char *noop_reason; /* PROVED_NOOP proof kind (else NULL) */
} ClusterSideRouteRow;

/*
 * §2.1 verdicts.
 */
typedef enum ClusterSideRouteVerdict {
	CLUSTER_SIDE_ROUTE_VERDICT_APPLY = 0,
	CLUSTER_SIDE_ROUTE_VERDICT_PROVED_NOOP,
	CLUSTER_SIDE_ROUTE_VERDICT_BLOCKED
} ClusterSideRouteVerdict;

/*
 * Total-registry lookup: exactly one row per (rmid, opcode), or false
 * for an unknown rmgr/opcode (the caller must treat it as BLOCKED).
 */
extern bool cluster_side_route_lookup(uint8 rmid, uint16 opcode, ClusterSideRouteRow *out);

/*
 * §2.1 verdict — pure function of the row (cold/online identical,
 * U-SIDE-02).  A PROVED_NOOP row carries its proof kind; anything
 * without a positive no-mutation proof is BLOCKED.
 */
extern ClusterSideRouteVerdict cluster_side_route_verdict(const ClusterSideRouteRow *row);

#endif /* CLUSTER_SIDE_ROUTE_H */
