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

#endif /* USE_PGRAC_CLUSTER */
