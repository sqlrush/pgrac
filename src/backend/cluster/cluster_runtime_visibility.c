/*-------------------------------------------------------------------------
 *
 * cluster_runtime_visibility.c
 *	  pgrac spec-6.12 wave 6.12i (缺口 A) — D-i1 undo-block CF fetch
 *	  orchestration + the runtime live-authority gate wrapper.
 *
 *	  Active runtime has no materialized marker to admit the by-xid
 *	  durable-TT resolution of a recycled remote ITL slot, so the origin's
 *	  LMS co-samples a live authority triple {origin_epoch, live_hwm_lsn,
 *	  tt_generation} into the very undo-block reply that carries its TT
 *	  (spec-6.12 §2.11 "live authority source").  This file is the
 *	  requester-side consumer: it rides the spec-6.12b CR-server wire
 *	  (cluster_gcs_block_undo_tt_fetch_and_wait), caches the fetched
 *	  block + authority PAIR (L2 CR pool bytes + a per-backend authority
 *	  memo, Q-i5), and exposes the runtime covers() gate.
 *
 *	  The bytes and the authority are only ever served TOGETHER, exactly
 *	  as co-sampled: a cache hit returns the authority sampled with the
 *	  cached bytes, never a newer one — a scan over the bytes may only
 *	  claim the coverage window of ITS OWN sample, or a 0-match inside a
 *	  later window would be mistaken for proof (D-i2 condition (c)).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_runtime_visibility.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.  Every miss path returns false so the caller keeps the
 *	  pre-existing 53R97 fail-closed boundary (规则 8.A: this wave only
 *	  widens "resolve when provable", never "resolve when unprovable").
 *	  Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md (wave i)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_cr.h"
#include "cluster/cluster_cr_cache.h"
#include "cluster/cluster_cr_pool.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_elog.h" /* cluster_node_id */
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_runtime_visibility.h"
#include "cluster/cluster_uba.h"

/*
 * cluster_vis_live_authority_covers
 *
 * Runtime D-i2 window gate: the pure policy under the CURRENT membership
 * epoch.  An epoch bump (origin fail-stop / any reconfig, D-i3) makes every
 * previously sampled authority fail this gate immediately — the in-flight
 * recycled resolution degrades to the unchanged 53R97 fail-closed.
 */
bool
cluster_vis_live_authority_covers(XLogRecPtr anchor_lsn, ClusterLiveAuthority auth)
{
	return cluster_vis_live_authority_covers_policy(anchor_lsn, auth, cluster_epoch_get_current());
}


/*
 * Per-backend memo of the authority CO-SAMPLED with the block bytes last
 * installed into the L2 CR pool for one (origin, segment) — the pool stores
 * only page bytes, and a hit is unusable without the authority of exactly
 * those bytes (banner).  Direct-mapped, exact-match validated; a collision
 * only costs a re-fetch.  Backend-local by design: no locks, no torn
 * triples, and the fetch path is backend-context anyway.
 */
#define RTVIS_AUTH_MEMO_SLOTS 8

typedef struct RtvisAuthMemo {
	bool valid;
	int origin_node;
	uint32 segment_id;
	ClusterLiveAuthority auth;
} RtvisAuthMemo;

static RtvisAuthMemo rtvis_auth_memo[RTVIS_AUTH_MEMO_SLOTS];

static inline RtvisAuthMemo *
rtvis_auth_memo_slot(int origin_node, uint32 segment_id)
{
	uint32 h = (uint32)origin_node * 31u + segment_id;

	return &rtvis_auth_memo[h % RTVIS_AUTH_MEMO_SLOTS];
}

/*
 * Pool key for the cached undo header block.  Field reuse is deliberate and
 * documented: the CR pool key is an opaque discriminator tuple, so the
 * synthetic locator carries (magic, origin, segment) and the two identity
 * tails carry the epoch and TT generation the bytes were sampled under — a
 * reconfig or a TT rollover changes the key, leaving stale entries
 * unreachable (fail-closed by construction; LRU reclaims them).  The magic
 * spcOid keeps synthetic keys disjoint from every real-relation CR key.
 */
static ClusterCRCacheKey
rtvis_pool_key(int origin_node, uint32 segment_id, uint32 block_no, ClusterLiveAuthority auth)
{
	ClusterCRCacheKey key;

	memset(&key, 0, sizeof(key));
	key.rlocator.spcOid = GCS_BLOCK_UNDO_FETCH_TAG_MAGIC;
	key.rlocator.dbOid = (Oid)origin_node;
	key.rlocator.relNumber = (RelFileNumber)segment_id;
	key.forknum = MAIN_FORKNUM;
	key.blockno = (BlockNumber)block_no;
	key.read_scn = (SCN)auth.origin_epoch;				/* epoch discriminator */
	key.base_page_lsn = (XLogRecPtr)auth.tt_generation; /* generation discriminator */
	return key;
}

/*
 * cluster_undo_block_fetch_for_visibility — D-i1 (spec-6.12i CP2).
 *
 * See cluster_runtime_visibility.h for the contract.  Order of refusal
 * checks mirrors the origin-side serve so both ends agree on the slice
 * boundary (block 0 / TT header only).
 */
bool
cluster_undo_block_fetch_for_visibility(int origin_node, UBA uba, char *out_page,
										ClusterLiveAuthority *auth_out)
{
	uint32 segment_id = 0;
	uint32 block_no = 0;
	uint16 tt_slot_offset = 0;
	uint16 row_offset = 0;
	uint64 local_epoch;
	RtvisAuthMemo *memo;
	ClusterCRPoolHandle handle;

	if (out_page == NULL || auth_out == NULL)
		return false;
	memset(auth_out, 0, sizeof(*auth_out));

	if (!cluster_crossnode_runtime_visibility || origin_node < 0 || origin_node == cluster_node_id
		|| !uba_decode(uba, &segment_id, &block_no, &tt_slot_offset, &row_offset)
		|| block_no != 0) {
		cluster_rtvis_undo_fetch_note_failclosed();
		return false;
	}

	/*
	 * Cache leg (Q-i5): serve the pool bytes ONLY together with the
	 * same-epoch authority they were installed under.  The epoch equality
	 * check here is the D-i3 crash-shrink: after a reconfig the memo (and
	 * with it the pool key) is dead, so every path below re-fetches under
	 * the new epoch.
	 */
	local_epoch = cluster_epoch_get_current();
	memo = rtvis_auth_memo_slot(origin_node, segment_id);
	if (memo->valid && memo->origin_node == origin_node && memo->segment_id == segment_id
		&& memo->auth.origin_epoch == local_epoch) {
		ClusterCRCacheKey key = rtvis_pool_key(origin_node, segment_id, block_no, memo->auth);

		if (cluster_cr_pool_lookup_copy(&key, out_page)) {
			*auth_out = memo->auth;
			cluster_rtvis_undo_fetch_note_cache_hit();
			return true;
		}
	}

	/* Wire leg: fetch block + co-sampled authority from the origin. */
	if (!cluster_gcs_block_undo_tt_fetch_and_wait((int32)origin_node, segment_id, block_no,
												  out_page, auth_out)) {
		cluster_rtvis_undo_fetch_note_failclosed();
		return false;
	}
	cluster_rtvis_undo_fetch_note_wire();

	/*
	 * Install the PAIR: memo takes the reply authority verbatim (never a
	 * max-merge — the authority must stay the one sampled with these
	 * bytes), the pool takes the bytes under that authority's key.  A
	 * reserve refusal (pool off / full / already present) only loses the
	 * cache, never the result.
	 */
	memo->valid = true;
	memo->origin_node = origin_node;
	memo->segment_id = segment_id;
	memo->auth = *auth_out;

	{
		ClusterCRCacheKey key = rtvis_pool_key(origin_node, segment_id, block_no, *auth_out);

		if (cluster_cr_pool_reserve(&key, &handle))
			cluster_cr_pool_publish(&handle, out_page);
	}

	return true;
}

/*
 * rtvis_try_origin_verdict — CP5 (D-i4 / spec-6.15 D4) verdict fallback leg.
 *
 *	Reached when the single-block positive proof came back NONE (0-match /
 *	ambiguity): ask the ORIGIN for a complete own-TT by-xid verdict (the
 *	spec-3.22 retention theorem served cross-instance; see the header and
 *	cluster_cr_server.c lms_undo_verdict_serve for the origin-side legs).
 *
 *	The shipped horizon_scn / commit_scn are Lamport-observed BEFORE any
 *	admissibility decision (AD-008: an SCN that crossed the wire advances
 *	the local clock) — so even a leg-(e) refusal (read_scn behind the
 *	shipped horizon, the t/346 clock-skew case) self-heals: the NEXT
 *	snapshot's read_scn is at/after the observed horizon.
 *
 *	true only on a proven terminal verdict; *out_commit_scn_is_bound marks a
 *	BELOW_HORIZON commit whose scn field is the horizon BOUND (valid against
 *	THIS read_scn only — never stamp/cache it).  false = fail-closed.
 */
static bool
rtvis_try_origin_verdict(int origin_node, uint32 undo_segment_id, TransactionId raw_xid,
						 XLogRecPtr anchor_lsn, SCN read_scn, bool *out_committed,
						 SCN *out_commit_scn, bool *out_commit_scn_is_bound)
{
	ClusterGcsUndoVerdictPage verdict;
	ClusterLiveAuthority auth;

	cluster_rtvis_verdict_note_wire();
	if (!cluster_gcs_block_undo_verdict_fetch_and_wait((int32)origin_node, undo_segment_id, raw_xid,
													   &verdict, &auth)) {
		cluster_rtvis_verdict_note_failclosed();
		return false;
	}

	/* Lamport-observe every SCN that crossed the wire (AD-008), before any
	 * gate can refuse — the observe is what makes a refusal self-heal. */
	cluster_scn_observe((SCN)verdict.horizon_scn);
	cluster_scn_observe((SCN)verdict.commit_scn);

	if (!cluster_vis_live_authority_covers(anchor_lsn, auth)) {
		cluster_rtvis_verdict_note_failclosed();
		return false;
	}

	switch (verdict.verdict) {
	case (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_EXACT:
		*out_committed = true;
		*out_commit_scn = (SCN)verdict.commit_scn;
		cluster_rtvis_verdict_note_exact();
		elog(DEBUG1,
			 "rtvis verdict: xid %u origin %d COMMITTED_EXACT scn " UINT64_FORMAT " wrap %u",
			 raw_xid, origin_node, (uint64)verdict.commit_scn, (unsigned)verdict.wrap);
		return true;

	case (uint8)CLUSTER_GCS_UNDO_VERDICT_ABORTED:
		*out_committed = false;
		cluster_rtvis_verdict_note_exact();
		elog(DEBUG1, "rtvis verdict: xid %u origin %d ABORTED", raw_xid, origin_node);
		return true;

	case (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_BELOW_HORIZON:
		/*
		 * Requester leg (e) of the retention proof: the bound commit_scn <=
		 * horizon decides against read_scn only when the horizon is not
		 * newer than read_scn.  A caller without snapshot semantics
		 * (read_scn invalid) can never consume a bound.
		 */
		if (!SCN_VALID(read_scn) || scn_time_cmp((SCN)verdict.horizon_scn, read_scn) > 0) {
			cluster_rtvis_verdict_note_inadmissible();
			elog(DEBUG1,
				 "rtvis verdict: xid %u origin %d BELOW_HORIZON " UINT64_FORMAT
				 " inadmissible for read_scn " UINT64_FORMAT " (observed; next snapshot heals)",
				 raw_xid, origin_node, (uint64)verdict.horizon_scn, (uint64)read_scn);
			return false;
		}
		*out_committed = true;
		*out_commit_scn = (SCN)verdict.horizon_scn;
		*out_commit_scn_is_bound = true;
		cluster_rtvis_verdict_note_below_horizon();
		elog(DEBUG1,
			 "rtvis verdict: xid %u origin %d COMMITTED_BELOW_HORIZON " UINT64_FORMAT
			 " admissible for read_scn " UINT64_FORMAT,
			 raw_xid, origin_node, (uint64)verdict.horizon_scn, (uint64)read_scn);
		return true;

	default:
		/* page_usable() already fenced the kind range; defense in depth. */
		cluster_rtvis_verdict_note_failclosed();
		return false;
	}
}

/*
 * cluster_runtime_visibility_try_resolve_remote — CP3 + CP5 (D-i2/D-i4
 * wiring body).
 *
 *	Active-runtime terminal resolution of a RECYCLED remote ITL ref:
 *	  fetch (D-i1, block 0 of the ref's segment)  ->  covers gate (D-i2,
 *	  co-sampled authority vs this tuple's page LSN)  ->  positive proof
 *	  (exact xid+wrap slot match on the SHIPPED bytes)  ->  on a proof NONE,
 *	  the CP5 origin-verdict fallback (complete scan at the origin).
 *
 *	The proof scans the very bytes the authority was co-sampled with (also
 *	on a cache hit — the pool/memo only ever serve the pair as installed),
 *	so D-i2 condition (c) generation consistency is structural: there is no
 *	second read whose generation could diverge.  auth.tt_generation is kept
 *	for observability and any future cross-source check.
 *
 *	true only on a proven terminal verdict; every other outcome returns
 *	false so classify_ref keeps the pre-existing STALE_OR_AMBIGUOUS ->
 *	53R97 fail-closed (Rule 8.A: only "resolve when provable" is widened).
 */
bool
cluster_runtime_visibility_try_resolve_remote(int origin_node, uint32 undo_segment_id,
											  TransactionId raw_xid, XLogRecPtr anchor_lsn,
											  SCN read_scn, bool *out_committed,
											  SCN *out_commit_scn, bool *out_commit_scn_is_bound)
{
	PGAlignedBlock page;
	ClusterLiveAuthority auth;
	SCN scn = InvalidScn;
	uint16 wrap = TT_WRAP_INVALID;

	if (out_committed != NULL)
		*out_committed = false;
	if (out_commit_scn != NULL)
		*out_commit_scn = InvalidScn;
	if (out_commit_scn_is_bound != NULL)
		*out_commit_scn_is_bound = false;
	if (out_committed == NULL || out_commit_scn == NULL || out_commit_scn_is_bound == NULL)
		return false;

	if (!cluster_crossnode_runtime_visibility)
		return false;
	if (origin_node < 0 || origin_node == cluster_node_id)
		return false;

	/*
	 * uba_encode contract pre-validation (segment 0 is bootstrap-only and
	 * Assert-fenced there; a ref carrying it is not resolvable evidence).
	 */
	if (undo_segment_id == 0 || undo_segment_id > UINT16_MAX)
		return false;

	/*
	 * Single-block fast leg (CP3), entirely opportunistic since CP5: the
	 * ref's segment id names the CURRENT slot owner's segment, which under
	 * the spec-6.15 D4 xid-derived origin may not even exist on the node we
	 * are asking — a fetch miss there is a routing artifact, not evidence.
	 * A successful exact xid+wrap proof still short-circuits the wire; a
	 * fetch miss or a proof NONE falls to the origin-verdict leg.
	 */
	if (cluster_undo_block_fetch_for_visibility(origin_node, uba_encode(undo_segment_id, 0, 0, 0),
												page.data, &auth)
		&& cluster_vis_live_authority_covers(anchor_lsn, auth)) {
		switch (cluster_vis_tt_block_positive_proof(
			page.data, undo_segment_id, (uint8)(origin_node + 1), raw_xid, &scn, &wrap)) {
		case CLUSTER_VIS_TT_PROOF_COMMITTED:
			*out_committed = true;
			*out_commit_scn = scn;
			cluster_rtvis_resolve_note_committed();
			return true;
		case CLUSTER_VIS_TT_PROOF_ABORTED:
			cluster_rtvis_resolve_note_aborted();
			return true;
		case CLUSTER_VIS_TT_PROOF_NONE:
		default:
			break; /* 0-match / ambiguity: fall to the verdict leg */
		}
	}

	/*
	 * CP5 (D-i4) verdict leg: a single fetched TT block cannot prove
	 * recycled-or-aborted (the slot may live in another segment), and a
	 * fetch/covers miss proves nothing either — ask the origin for the
	 * complete own-TT verdict instead of failing closed outright.
	 */
	if (rtvis_try_origin_verdict(origin_node, undo_segment_id, raw_xid, anchor_lsn, read_scn,
								 out_committed, out_commit_scn, out_commit_scn_is_bound)) {
		if (*out_committed)
			cluster_rtvis_resolve_note_committed();
		else
			cluster_rtvis_resolve_note_aborted();
		return true;
	}
	cluster_rtvis_resolve_note_failclosed();
	return false;
}

#endif /* USE_PGRAC_CLUSTER */
