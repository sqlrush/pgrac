/*-------------------------------------------------------------------------
 *
 * cluster_side_recovery.h
 *	  RF-SIDE D-SIDE-06/07/08 — RF-PAGE integration call-site judgements,
 *	  per-resource serve readiness, and the retention proof exporter.
 *
 *	  Spec: specs/spec-rf-side-typed-recovery-and-shared-terminal.md
 *	  §1.2 D-SIDE-06 ("RF-PAGE integration call sites"), D-SIDE-07
 *	  ("resource access/serve call sites"), D-SIDE-08 ("retention proof
 *	  exporter"), §2.2 (consumed cross-spec contracts), §4 (crash matrix
 *	  rows: resource A verified vs B blocked; all-bytes-durable but a
 *	  missing post-read -> deny retire), §5 tests.
 *
 *	  DESIGN CONTRACT (same style as the RF-PAGE PGDEL layers):
 *	    - judgement-only over caller-declared typed facts; the canonical
 *	      TT/undo/pending/space mutation and durability execution stays
 *	      with the production owner (D-SIDE-02..05 wiring, later);
 *	    - D-SIDE-06 consumes the RF-PAGE proof WITHOUT copying the page
 *	      parser: it reads only identity/class/version/coverage/
 *	      durability/post-read/authority facts (spec §1.2 D-SIDE-06);
 *	    - D-SIDE-07 is strictly per-resource/thread (FND-09): a healthy
 *	      unrelated resource is never blocked by another resource's
 *	      failure, and no whole-instance barrier exists here (§4 row);
 *	    - D-SIDE-08 never deletes WAL: it only produces the SIDE side of
 *	      the FND-10 conjunction and the precise retirement denial
 *	      (§2.2/§4 rows).
 *
 *	  NOT DELIVERED HERE (stays RED): route totality census (D-SIDE-01),
 *	  TT/undo/2PC/projection primitives (D-SIDE-02..04), canonical space
 *	  metadata (D-SIDE-05, STOP-RF-SIDE-SPACE-ABI), observability
 *	  (D-SIDE-10).  No catalog/page/WAL/wire ABI is touched.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_side_recovery.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SIDE_RECOVERY_H
#define CLUSTER_SIDE_RECOVERY_H

#include "cluster/cluster_page_apply.h"

/*
 * D-SIDE-06: one canonical TT/undo/space page consuming the RF-PAGE
 * proof.  All facts are the RF-PAGE PGDEL-06/07 proof fields
 * (ClusterPageProof) plus the page class and the exact expected-before
 * version this domain must bind; the consumer never re-parses the page
 * (spec §1.2 D-SIDE-06: 不复制 page parser).
 */
typedef struct ClusterSidePageConsumeInput {
	const ClusterPageIdentity *identity;
	ClusterPageClass page_class;			   /* UNKNOWN/UNCLASSIFIED fails */
	const ClusterPageVersion *expected_before; /* must be valid */
	bool contributor_coverage;				   /* RF-PAGE chain closed */
	bool durability_barrier_ok;				   /* RF-PAGE durability done */
	bool post_read_ok;						   /* RF-PAGE canonical post-read done */
	bool authority_revalidated;				   /* RF-PAGE ROOT revalidation done */
} ClusterSidePageConsumeInput;

/*
 * D-SIDE-06 verdict: true only when the page class is known, the
 * expected-before version is valid, and the complete RF-PAGE proof
 * holds.  Any missing/conflicting fact blocks ONLY this resource (the
 * per-resource scope is D-SIDE-07's).
 */
extern bool cluster_side_page_consumer_ready(const ClusterSidePageConsumeInput *in);

/*
 * D-SIDE-07: per-resource/thread serve readiness.  Strictly resource-
 * scoped (FND-09): the judgement contains no other resource's state, so
 * a healthy unrelated resource is never blocked by a side-wide failure
 * and no whole-instance barrier exists (§4 row "resource A verified、
 * resource B blocked -> A 可 open，B 保持 fenced").
 */
typedef struct ClusterSideReadinessInput {
	uint16 resource_id;	  /* exact resource/thread identity */
	bool page_proof_ok;	  /* D-SIDE-06 verdict for the resource */
	bool side_proof_ok;	  /* this domain's truth proof (TT/undo/pending) */
	bool authority_fresh; /* RF-ROOT authority fresh */
} ClusterSideReadinessInput;

extern bool cluster_side_resource_readiness(const ClusterSideReadinessInput *in);

/*
 * D-SIDE-08: the SIDE side of the FND-10 retirement conjunction.  The
 * affected set is the exact TT/undo/pending/space resource set of the
 * failed-origin duty; every member must be durable AND canonically
 * post-read, and no exact resource/thread consumer may remain.  This
 * exporter NEVER deletes WAL — it only reports ready or the precise
 * denial reason (spec §1.2 D-SIDE-08, §2.2, §4 row "all affected
 * TT/undo/space bytes durable 但 post-read 缺一项 -> deny retire").
 */
typedef enum ClusterSideRetentionVerdict {
	CLUSTER_SIDE_RETENTION_READY = 0,		  /* FND-10 SIDE side holds */
	CLUSTER_SIDE_RETENTION_DENY_NOT_DURABLE,  /* some affected bytes not durable */
	CLUSTER_SIDE_RETENTION_DENY_NO_POST_READ, /* some affected post-read missing */
	CLUSTER_SIDE_RETENTION_DENY_CONSUMER,	  /* an exact consumer remains */
	CLUSTER_SIDE_RETENTION_DENY_INVALID		  /* bad input / empty set */
} ClusterSideRetentionVerdict;

typedef struct ClusterSideRetentionProof {
	uint16 failed_origin_thread;
	uint32 affected_count;	/* affected TT/undo/pending/space resources */
	bool all_bytes_durable; /* every affected byte durable */
	bool all_post_read_ok;	/* every affected canonical post-read done */
	bool consumers_zero;	/* no exact resource/thread consumer remains */
} ClusterSideRetentionProof;

extern ClusterSideRetentionVerdict
cluster_side_retention_proof_ready(const ClusterSideRetentionProof *proof);

#endif /* CLUSTER_SIDE_RECOVERY_H */
