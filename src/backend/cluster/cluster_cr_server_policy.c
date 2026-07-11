/*-------------------------------------------------------------------------
 *
 * cluster_cr_server_policy.c
 *	  pgrac spec-6.12b — pure split policy for the CR-server (no shmem, no
 *	  locks, no elog) so cluster_unit exercises every branch standalone.
 *
 *	  See cluster_cr_server.h for the FULL / PARTIAL / DENY contract and
 *	  why an interleaved home order must refuse (write_scn-DESC peel
 *	  ordering across chains is a correctness invariant, spec-3.10 Q10).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cr_server_policy.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md (wave b)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_cr_server.h"

ClusterCrServerSplit
cluster_cr_server_split_classify(const int32 *chain_origins, int nchains, int32 self_node,
								 int *out_prefix_len)
{
	int prefix = 0;

	if (out_prefix_len != NULL)
		*out_prefix_len = 0;
	if (nchains < 0 || (nchains > 0 && chain_origins == NULL))
		return CLUSTER_CR_SPLIT_DENY; /* malformed input: fail closed */

	/* Leading self-home run = the peelable DESC prefix. */
	while (prefix < nchains && chain_origins[prefix] == self_node)
		prefix++;

	/* Any self-home chain AFTER a foreign one = interleave = refuse. */
	for (int i = prefix; i < nchains; i++) {
		if (chain_origins[i] == self_node)
			return CLUSTER_CR_SPLIT_DENY;
	}

	if (out_prefix_len != NULL)
		*out_prefix_len = prefix;
	return (prefix == nchains) ? CLUSTER_CR_SPLIT_FULL : CLUSTER_CR_SPLIT_PARTIAL;
}

/*
 * cluster_cr_server_invalid_scn_verdict — spec-7.1 D1 serve pure decision.
 *
 *	The origin's durable by-xid scan matched our own xid but the slot carries
 *	no stamped commit_scn (the delayed-cleanout window: XID_MATCH_INVALID_SCN).
 *	Per IN-5 the real population is aborted-unstamped -- an abort writes no
 *	durable commit_scn -- so cross-checking CLOG lets us answer a provably
 *	ABORTED xid positively (invisible at the requester) instead of 53R97.
 *
 *	8.A (positive proof only): ONLY an explicit CLOG abort upgrades.  A
 *	committed-but-unstamped xid (we must never fabricate its commit_scn), an
 *	in-flight / 2PC-prepared / crashed-without-abort-record xid -- for all of
 *	which TransactionIdDidAbort is false -- stays REFUSE (fail-closed, the
 *	refuse direction is unchanged).
 */
ClusterCrInvalidScnVerdict
cluster_cr_server_invalid_scn_verdict(bool clog_did_abort)
{
	return clog_did_abort ? CLUSTER_CR_INVALID_SCN_ABORTED : CLUSTER_CR_INVALID_SCN_REFUSE;
}

#endif /* USE_PGRAC_CLUSTER */
