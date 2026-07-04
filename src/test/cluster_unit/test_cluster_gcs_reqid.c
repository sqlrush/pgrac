/*-------------------------------------------------------------------------
 *
 * test_cluster_gcs_reqid.c
 *	  Unit tests for the GCS request-id domain tagging (spec-6.14a D1).
 *
 *	  U1  never zero (idle-sentinel safety), including wrapped seq.
 *	  U2  top-bit split: local-upgrade ids always carry it, requester never.
 *	  U3  node 0 coverage: the local domain stays tagged for node 0 (the
 *	      r2-P1 case a plain node<<56 scheme gets wrong), and node0/backend0
 *	      requester ids stay plain counter values (compatible, in-domain
 *	      monotone).
 *	  U4  cross-domain disjointness: sampled (node, backend, seq) grids
 *	      never collide across distinct sources.
 *	  U5  wrap behaviour: masked-to-zero seq maps to 1 in both domains.
 *	  U6  single-node fallback (node_id = -1) masks without corrupting the
 *	      domain flag.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_gcs_reqid.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-6.14a-cf-s-revoke-x-grant.md (D1 / §4 U6)
 *
 *-------------------------------------------------------------------------
 */
#include "cluster/cluster_gcs_reqid.h"

#include <stdlib.h>

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/* Assert stub for libpgport_srv linkage under --enable-cassert (the standard
 * cluster_unit pure-test pattern). */
void ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
	pg_attribute_noreturn();

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("TRAP: %s (%s:%d)\n", conditionName, fileName, lineNumber);
	abort();
}

UT_TEST(nonzero_always)
{
	/* Zero seq (wrapped) maps to 1 -- never the idle sentinel. */
	UT_ASSERT(gcs_reqid_requester(0, 0, 0) != 0);
	UT_ASSERT(gcs_reqid_local_upgrade(0, 0) != 0);
	UT_ASSERT(gcs_reqid_requester(0, 0, UINT64CONST(1) << 40) != 0);
	UT_ASSERT(gcs_reqid_local_upgrade(0, UINT64CONST(1) << 56) != 0);
}

UT_TEST(top_bit_split)
{
	int nodes[] = { 0, 1, 15, 31 };
	uint64 seqs[] = { 1, 7, GCS_REQID_REQUESTER_SEQ_MASK };
	int i, j;

	for (i = 0; i < (int)lengthof(nodes); i++) {
		for (j = 0; j < (int)lengthof(seqs); j++) {
			uint64 req = gcs_reqid_requester(nodes[i], 3, seqs[j]);
			uint64 loc = gcs_reqid_local_upgrade(nodes[i], seqs[j]);

			UT_ASSERT((req & GCS_REQID_LOCAL_DOMAIN_FLAG) == 0);
			UT_ASSERT((loc & GCS_REQID_LOCAL_DOMAIN_FLAG) != 0);
		}
	}
}

UT_TEST(node0_domains)
{
	/* r2-P1: node0's local-upgrade domain must stay tagged (top byte 0x80,
	 * not 0x00) -- a plain node<<56 tag degrades to the raw counter. */
	uint64 loc = gcs_reqid_local_upgrade(0, 5);

	UT_ASSERT((loc >> 56) == UINT64CONST(0x80));
	UT_ASSERT(loc != 5);

	/* node0/backend0 requester ids stay the plain counter value: disjoint
	 * from every OTHER source by their zero node/backend fields, monotone
	 * within their own. */
	UT_ASSERT(gcs_reqid_requester(0, 0, 5) == UINT64CONST(5));
}

UT_TEST(cross_domain_disjoint)
{
	/* Distinct sources must never produce equal ids, whatever their seqs.
	 * Sample a grid and compare every pair from different sources. */
	int nodes[] = { 0, 1, 31 };
	int backends[] = { 0, 1, 65535 };
	uint64 seqs[] = { 1, 42, GCS_REQID_REQUESTER_SEQ_MASK };
	uint64 ids[64];
	int src[64]; /* source key: node*100000 + backend (or -node for local) */
	int n = 0;
	int a, b, s, i, j;

	for (a = 0; a < (int)lengthof(nodes); a++) {
		for (b = 0; b < (int)lengthof(backends); b++)
			for (s = 0; s < (int)lengthof(seqs); s++) {
				ids[n] = gcs_reqid_requester(nodes[a], backends[b], seqs[s]);
				src[n] = nodes[a] * 100000 + backends[b];
				n++;
			}
		for (s = 0; s < (int)lengthof(seqs); s++) {
			ids[n] = gcs_reqid_local_upgrade(nodes[a], seqs[s]);
			src[n] = -(nodes[a] + 1);
			n++;
		}
	}

	for (i = 0; i < n; i++)
		for (j = i + 1; j < n; j++) {
			if (src[i] == src[j])
				continue; /* same source: monotone counter handles it */
			UT_ASSERT(ids[i] != ids[j]);
		}
}

UT_TEST(wrap_maps_to_one)
{
	/* A wrapped (masked-to-zero) seq becomes 1, preserving both the domain
	 * bits and the non-zero invariant. */
	uint64 req = gcs_reqid_requester(1, 2, UINT64CONST(1) << 40);
	uint64 loc = gcs_reqid_local_upgrade(1, UINT64CONST(1) << 56);

	UT_ASSERT((req & GCS_REQID_REQUESTER_SEQ_MASK) == UINT64CONST(1));
	UT_ASSERT((loc & GCS_REQID_LOCAL_SEQ_MASK) == UINT64CONST(1));
	UT_ASSERT((req >> GCS_REQID_NODE_SHIFT) == UINT64CONST(1));
	UT_ASSERT((loc & GCS_REQID_LOCAL_DOMAIN_FLAG) != 0);
}

UT_TEST(single_node_fallback_masks)
{
	/* cluster_node_id = -1 (single-node fallback) masks to 0x7f without
	 * touching the domain flag; no peers exist, so only self-consistency
	 * (non-zero, in-domain) matters. */
	uint64 req = gcs_reqid_requester(-1, 0, 9);
	uint64 loc = gcs_reqid_local_upgrade(-1, 9);

	UT_ASSERT(req != 0);
	UT_ASSERT((req & GCS_REQID_LOCAL_DOMAIN_FLAG) == 0);
	UT_ASSERT((loc & GCS_REQID_LOCAL_DOMAIN_FLAG) != 0);
	UT_ASSERT(((req >> GCS_REQID_NODE_SHIFT) & GCS_REQID_NODE_MASK) == UINT64CONST(0x7f));
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(nonzero_always);
	UT_RUN(top_bit_split);
	UT_RUN(node0_domains);
	UT_RUN(cross_domain_disjoint);
	UT_RUN(wrap_maps_to_one);
	UT_RUN(single_node_fallback_masks);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
