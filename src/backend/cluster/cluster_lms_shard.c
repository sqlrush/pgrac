/*-------------------------------------------------------------------------
 *
 * cluster_lms_shard.c
 *	  pgrac LMS worker-pool shard map — spec-7.3 D1 (pure layer).
 *
 *	  See cluster_lms_shard.h for the contract.  This file has no
 *	  PG-backend dependencies beyond common/hashfn.h so it links into the
 *	  standalone cluster_unit test.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_lms_shard.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.  Spec: spec-7.3-lms-worker-pool.md (D1).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/hashfn.h"
#include "cluster/cluster_lms_shard.h"

#ifdef USE_PGRAC_CLUSTER

/*
 * Packed shard-hash input width: the five BufferTag fields serialized in a
 * fixed order (spcOid|dbOid|relNumber|forkNum|blockNum), 4 bytes each.
 * Packing explicitly (rather than hashing the struct) defeats any struct
 * padding and gives a byte image that is stable for the same field values —
 * every cluster node runs the same build/arch, so both ends of a DATA
 * connection hash to the same worker (spec-7.3 R1).
 */
#define LMS_SHARD_HASH_INPUT_LEN 20

/* Field widths the packing depends on (L3 encoding boundary / L6 math). */
StaticAssertDecl(sizeof(Oid) == 4 && sizeof(RelFileNumber) == 4 && sizeof(BlockNumber) == 4
					 && sizeof(ForkNumber) == 4,
				 "spec-7.3 D1: BufferTag field widths assumed 4B for the shard "
				 "hash packing; a width change requires revisiting the layout");
StaticAssertDecl(LMS_SHARD_HASH_INPUT_LEN == 4 * 5,
				 "spec-7.3 D1: shard hash input is the five 4B BufferTag fields");

int
cluster_lms_shard_for_tag(const BufferTag *tag, int n_workers)
{
	uint8 hash_input[LMS_SHARD_HASH_INPUT_LEN];
	uint64 hash;

	Assert(tag != NULL);
	Assert(n_workers >= 1 && n_workers <= CLUSTER_LMS_MAX_WORKERS);

	/*
	 * N == 1 is the spec-7.2 topology identity (only worker 0 runs) and also
	 * guards the modulo below against a zero/negative divisor: every tag maps
	 * to worker 0, no hash needed.  Worker 0 is a live LMS, not a sentinel
	 * (L449) — the degenerate map is a real single-shard routing.
	 */
	if (n_workers <= 1)
		return 0;

	/* Serialize the five fields in a fixed order (defeats struct padding). */
	memcpy(&hash_input[0], &tag->spcOid, 4);
	memcpy(&hash_input[4], &tag->dbOid, 4);
	memcpy(&hash_input[8], &tag->relNumber, 4);
	memcpy(&hash_input[12], &tag->forkNum, 4);
	memcpy(&hash_input[16], &tag->blockNum, 4);

	hash = hash_bytes_extended(hash_input, LMS_SHARD_HASH_INPUT_LEN, 0);

	return (int)(hash % (uint64)n_workers);
}

#endif /* USE_PGRAC_CLUSTER */
