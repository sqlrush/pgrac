/*-------------------------------------------------------------------------
 *
 * cluster_gcs_block_dedup.h
 *	  pgrac cluster GCS block reliability hardening — master-side dedup HTAB.
 *
 *	  spec-2.34 D2 introduces an LMON-owned dedup HTAB on every node that
 *	  serves as GCS block-shipping master.  When the same logical request
 *	  is retransmitted by the sender (after timeout, eager wake, or epoch
 *	  stale retry), the master-side handler can return the cached reply
 *	  without re-flushing WAL or re-copying the page (HC99).  The HTAB
 *	  uses a 4-tuple key {origin_node, requester_backend_id, request_id,
 *	  cluster_epoch} (HC90), with entry value containing the full reply
 *	  payload plus tag + transition_id for collision validation (HC91).
 *
 *	  Key safety properties:
 *	    HC90  4-tuple key; LMON-owned shmem region; built-in tranche
 *	    HC91  duplicate hit must validate entry.tag == req.tag &&
 *	          entry.transition_id == req.transition_id; mismatch →
 *	          DENIED_VALIDATOR_REJECT + dedup_collision_count++
 *	    HC92  fixed-size sizeof(GcsBlockDedupEntry) == 8448B (PG dynahash
 *	          cap × 8.5KB master memory ceiling; default 1024 → 8.5MB on
 *	          configured cluster nodes; bootstrap/initdb with node_id=-1
 *	          does not allocate the HTAB)
 *	    HC93  TTL sweep (completed_at_ts + registered_at_ts) + local
 *	          before_shmem_exit cleanup + CSSD DEAD cleanup — three-fold
 *	          GC; not solely epoch-based
 *	    HC96  cap-full → DENIED_DEDUP_FULL transient, sender retries
 *	    HC99  entry stores complete reply replay payload
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_gcs_block_dedup.h
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-2.34-gcs-block-reliability-hardening.md (FROZEN v0.3)
 *	  Design: docs/cache-fusion-protocol-design.md
 *	  AD-005 (Cache Fusion full) + AD-002 (PCM lock state machine)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_GCS_BLOCK_DEDUP_H
#define CLUSTER_GCS_BLOCK_DEDUP_H

#include "c.h"
#include "cluster/cluster_gcs_block.h" /* GcsBlockReplyHeader / status */
#include "datatype/timestamp.h"		   /* TimestampTz */
#include "storage/buf_internals.h"	   /* BufferTag */

#ifdef USE_PGRAC_CLUSTER

/* ============================================================
 * GcsBlockDedupKey — 4-tuple key (HC90; 24B).
 *
 *	Layout:
 *	  [ 0,  4) origin_node_id           uint32
 *	  [ 4,  8) requester_backend_id     int32
 *	  [ 8, 16) request_id               uint64
 *	  [16, 24) cluster_epoch            uint64
 *
 *	Routing-wise the 4-tuple is sufficient.  Tag/transition collision
 *	protection lives in the entry value (HC91), not the key — keeps key
 *	small (HTAB partition lock friendly) while still preventing
 *	backend_id reuse + request_id reset from silent replay.
 * ============================================================ */
typedef struct GcsBlockDedupKey {
	uint32 origin_node_id;
	int32 requester_backend_id;
	uint64 request_id;
	uint64 cluster_epoch;
} GcsBlockDedupKey;

StaticAssertDecl(sizeof(GcsBlockDedupKey) == 24, "spec-2.34 D2 GcsBlockDedupKey 24B "
												 "(origin 4 + backend 4 + req 8 + epoch 8)");


/* ============================================================
 * GcsBlockDedupEntry — fixed-size HTAB entry (HC92 + HC99; 8472B).
 *
 *	Layout (offsets explicit so alignment review is mechanical):
 *	  [    0,    24) key                 GcsBlockDedupKey (24B)
 *	  [   24,    44) tag                 BufferTag (20B)         HC91
 *	  [   44,    45) transition_id       uint8                   HC91
 *	  [   45,    46) status              uint8 (GcsBlockReplyStatus)
 *	  [   46,    56) _pad0[10]           explicit pad to 8-align
 *	  [   56,   104) reply_header        GcsBlockReplyHeader (48B)
 *	  [  104,   105) has_sf_dep          bool                    spec-6.2
 *	  [  105,   106) sf_flags            uint8                   spec-6.2
 *	  [  106,   107) sf_dep_count        uint8                   spec-6.2
 *	  [  107,   112) _pad1[5]            explicit pad to 8-align
 *	  [  112,   240) sf_dep_vec          ClusterSfDepVec         spec-6.2
 *	  [  240,  8432) block_data          char[GCS_BLOCK_DATA_SIZE]
 *	  [ 8432,  8440) completed_at_ts     TimestampTz (TTL sweep — replied)
 *	  [ 8440,  8448) registered_at_ts    TimestampTz (TTL sweep — in-flight)
 *	  [ 8448,  8456) done_at_ts          TimestampTz (round-2 DONE proof)
 *	  [ 8456,  8464) pinned_lifetime_us  int64 (round-2 pinned TTL)
 *	  [ 8464,  8472) pinned_done_linger_us int64 (round-2 pinned quarantine)
 *
 *	reply_header lands at offset 56 = 8 × 7, satisfying the 8-byte
 *	alignment required by reply_header.request_id (uint64).  block_data
 *	is BLCKSZ.  All five int64-family tail fields are 8-aligned.
 *
 *	GRANTED replies fill block_data with the page bytes; non-GRANTED
 *	replies leave block_data zeroed but still occupy 8KB (PG dynahash
 *	does not support variable-length entries; accepted trade-off per
 *	spec-2.34 §1.4 example).
 *
 *	completed_at_ts is set when the master finishes producing the reply
 *	(success or rejected).  registered_at_ts is set at the moment the
 *	HTAB slot is first inserted (in-flight); the TTL sweep uses the
 *	earlier of the two thresholds so abandoned in-flight slots (master
 *	crashed mid-reply, network drop before reply install) are also
 *	garbage-collected.
 *
 *	GCS-race round-2 RC-F lifecycle fields:
 *	  done_at_ts            requester completion proof consumed (an
 *	                        identity-verified GCS_BLOCK_DONE arrived for
 *	                        a COMPLETED entry).  A done entry only
 *	                        lingers pinned_done_linger_us for retransmit
 *	                        reorder slop, then ages out -- and it is
 *	                        reclaim-SAFE under cap pressure immediately
 *	                        (the completion proof is exactly what the
 *	                        §3.1 in-window whitelist was waiting for).
 *	  pinned_lifetime_us    the legal-request-lifetime threshold, pinned
 *	                        at REGISTRATION from the requester's wire
 *	                        hint (2x margin applied) or, absent a hint,
 *	                        from the master's GUCs at that moment.  GC
 *	                        never re-reads GUCs: a master-local SUSET
 *	                        change cannot silently shorten the window a
 *	                        live remote request was registered under.
 *	  pinned_done_linger_us the post-DONE quarantine, pinned at
 *	                        registration (2x the reply-timeout then in
 *	                        force).
 * ============================================================ */
typedef struct GcsBlockDedupEntry {
	GcsBlockDedupKey key;				  /* 24B — HTAB key */
	BufferTag tag;						  /* 20B — HC91 collision check */
	uint8 transition_id;				  /*  1B — HC91 collision check */
	uint8 status;						  /*  1B — GcsBlockReplyStatus */
	uint8 _pad0[10];					  /* 10B — explicit pad; header @ 56 */
	GcsBlockReplyHeader reply_header;	  /* 48B — full reply header (HC99) */
	bool has_sf_dep;					  /*  1B — spec-6.2 v2 dep vector present */
	uint8 sf_flags;						  /*  1B — GCS_BLOCK_REPLY_SF_* */
	uint8 sf_dep_count;					  /*  1B — non-empty dep vector entries */
	uint8 _pad1[5];						  /*  5B — dep_vec @ 112 */
	ClusterSfDepVec sf_dep_vec;			  /* 128B — spec-6.2 cached v2 deps */
	char block_data[GCS_BLOCK_DATA_SIZE]; /* 8192B — full page payload */
	TimestampTz completed_at_ts;		  /*  8B — TTL sweep replied */
	TimestampTz registered_at_ts;		  /*  8B — TTL sweep in-flight */
	TimestampTz done_at_ts;				  /*  8B — round-2: DONE proof consumed */
	int64 pinned_lifetime_us;			  /*  8B — round-2: TTL pinned at register */
	int64 pinned_done_linger_us;		  /*  8B — round-2: quarantine pinned */
} GcsBlockDedupEntry;

StaticAssertDecl(offsetof(GcsBlockDedupEntry, sf_dep_vec) == 112,
				 "spec-6.2 GcsBlockDedupEntry dep vector offset must be 112");
StaticAssertDecl(sizeof(GcsBlockDedupEntry) == 8472,
				 "GcsBlockDedupEntry 8472B (8448 spec-6.2 + 24 round-2 DONE lifecycle)");


/* ============================================================
 * GcsBlockDedupResult — outcome of lookup_or_register.
 *
 *	MISS_REGISTERED       new slot installed; caller proceeds with
 *	                      normal master-side flow then calls
 *	                      cluster_gcs_block_dedup_install_reply.
 *	IN_FLIGHT_DUPLICATE   same key seen but no reply installed yet
 *	                      (concurrent retry; first arrival is still
 *	                      processing).  Caller silently drops; the
 *	                      first arrival's reply will broadcast.
 *	CACHED_REPLY          same key + tag + transition_id match;
 *	                      caller may re-send the cached reply payload
 *	                      without re-flushing WAL or re-copying page.
 *	FORWARDED_DUPLICATE   PGRAC spec-2.35 HC113 NEW — same key seen
 *	                      previously forwarded to a holder (entry was
 *	                      installed with status GRANTED_FROM_HOLDER but
 *	                      master holds no 8KB cached block).  Caller
 *	                      must re-forward GCS_BLOCK_FORWARD to the
 *	                      stored holder (holder side is idempotent;
 *	                      counter forward_replay_count++).  Without
 *	                      this distinct return, the generic IN_FLIGHT_
 *	                      DUPLICATE branch would silently drop the
 *	                      retry and the sender's retransmit budget
 *	                      would never reach a holder reply.
 *	VALIDATION_FAIL       HC91 — same key but different tag or
 *	                      transition_id;  caller replies
 *	                      DENIED_VALIDATOR_REJECT + counter++.
 *	FULL                  HC92 — HTAB at cap;  caller replies
 *	                      DENIED_DEDUP_FULL (sender retries via
 *	                      HC96 transient path).
 * ============================================================ */
typedef enum GcsBlockDedupResult {
	GCS_BLOCK_DEDUP_MISS_REGISTERED = 0,
	GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE = 1,
	GCS_BLOCK_DEDUP_CACHED_REPLY = 2,
	GCS_BLOCK_DEDUP_VALIDATION_FAIL = 3,
	GCS_BLOCK_DEDUP_FULL = 4,
	GCS_BLOCK_DEDUP_FORWARDED_DUPLICATE = 5,  /* HC113 spec-2.35 NEW */
	GCS_BLOCK_DEDUP_INVALIDATE_IN_FLIGHT = 6, /* HC120 spec-2.36 NEW —
												* X transfer broadcast invalidate
												* phase in progress;  duplicate
												* requests for the same X tag
												* fall through to retransmit
												* path rather than re-broadcast. */
} GcsBlockDedupResult;


/* ============================================================
 * Eager reclaim safety decision primitives (spec-7.2a; review r1).
 *
 *	These two pure predicates carry the Rule 8.A correctness contract for
 *	the spec-7.2a eager reclaim path: a completed dedup entry may only be
 *	reclaimed under cap pressure when doing so cannot cause a retransmitted
 *	duplicate to be re-served in a way that breaks de-dup correctness.  They
 *	live in the header (matching the GcsBlockReplyStatus* helpers in
 *	cluster_gcs_block.h) so the decision logic is unit-testable without
 *	shmem; the shmem HTAB glue (scan + remove + counters) lives in the .c.
 * ============================================================ */

/*
 * GcsBlockReplyStatusIsReclaimIdempotent -- whether a completed dedup entry
 * carrying this reply status may be reclaimed WHILE the sender's retransmit
 * budget could still be live (in-window), without breaking retransmit de-dup
 * correctness (Rule 8.A).
 *
 * Default = false (UNSAFE / fail-closed).  A status is admitted here ONLY
 * after per-site measure-first proves that every master-side install site
 * which produces it re-serves with NO master state transition and NO
 * side-effect counter, so a duplicate that MISSes after reclaim re-derives a
 * bit-for-bit identical verdict.
 *
 * Statuses proven NOT idempotent (spec-7.2a §3.2, verified against
 * cluster_gcs_block.c on stage7-72-integration):
 *	 GRANTED / READ_IMAGE_FROM_XHOLDER		payload-bearing re-grant
 *	 *_FROM_HOLDER							forward routing marker
 *	 GRANTED_STORAGE_FALLBACK				:3305 N->S / :4089 S->X_UPGRADE
 *	 DENIED_PENDING_X						:4071 broadcast_invalidate
 *	 DENIED_LOST_WRITE						:4393 lost_write_detected_count++
 *
 * The whitelist is currently EMPTY: no status is in-window reclaimable until
 * its sites are individually proven (follow-on increment, spec-7.2a §11).
 * Out-of-window (2x) reclaim, handled by GcsBlockDedupEntryIsReclaimSafe
 * below, is safe for every status regardless.  Kept as an explicit switch so
 * a proven status is added as a `case ...: return true;` here.
 */
static inline bool
GcsBlockReplyStatusIsReclaimIdempotent(GcsBlockReplyStatus status)
{
	switch (status) {
		/* (no in-window-idempotent status proven yet — spec-7.2a §11) */
	default:
		break;
	}
	return false;
}

/*
 * Protocol upper bounds (GCS-race round-2 review F5 / calibration 2).
 * These MIRROR the GUC registration maxima in cluster_guc.c
 * (gcs_block_retransmit_initial_backoff_ms max 5000, _max_retries max 8,
 * gcs_reply_timeout_ms max 60000) — keep both sides in sync.  No legal
 * configuration of ANY peer can produce a request lifetime above
 * GCS_BLOCK_DEDUP_MAX_PROTOCOL_LIFETIME_MS (= 5000×(2^8−1) + 9×60000 =
 * 1,815,000 ms), so:
 *	 - a wire hint above it is a protocol violation (counted, denied), and
 *	 - a legacy peer (no GCS_DONE_V1 capability, hint unknowable) is pinned
 *	   AT it — ~1 h with the 2x margin; an availability cost during rolling
 *	   upgrade, never a correctness one (a master-formula fallback would
 *	   re-open the early-reclaim re-execution P0).
 */
#define GCS_BLOCK_RETRANSMIT_INITIAL_BACKOFF_MS_MAX 5000
#define GCS_BLOCK_RETRANSMIT_MAX_RETRIES_MAX 8
#define GCS_REPLY_TIMEOUT_MS_MAX 60000
#define GCS_BLOCK_DEDUP_MAX_PROTOCOL_LIFETIME_MS                                                   \
	((int64)GCS_BLOCK_RETRANSMIT_INITIAL_BACKOFF_MS_MAX                                            \
		 * ((1 << GCS_BLOCK_RETRANSMIT_MAX_RETRIES_MAX) - 1)                                       \
	 + (int64)(GCS_BLOCK_RETRANSMIT_MAX_RETRIES_MAX + 1) * GCS_REPLY_TIMEOUT_MS_MAX)

/*
 * GcsBlockDedupEntryIsReclaimSafe -- whether a dedup entry may be reclaimed
 * under cap pressure without breaking retransmit de-dup correctness.
 *
 *	fallback_out_of_window_us is consumed ONLY for an entry with no pinned
 *	lifetime (impossible after this build's registration path, cheap to
 *	guard); it is the caller's 2x total-backoff window (review r1: 1x only
 *	bounds the sender's last SEND, not the arrival of an in-flight
 *	retransmit at the master; 2x also covers transit + timing skew — the
 *	same margin the TTL sweep uses, dedup_expiry_threshold_us()).
 *
 *	  in-flight (completed_at_ts == 0)		-> never (still processing)
 *	  DONE-proven (done_at_ts != 0)			-> safe immediately
 *	  completed, age > pinned lifetime		-> safe for EVERY status
 *											   (spec-7.2a §3.1 theorem)
 *	  completed, age <= pinned lifetime		-> safe ONLY if the status is
 *											   site-proven idempotent
 */
static inline bool
GcsBlockDedupEntryIsReclaimSafe(const GcsBlockDedupEntry *entry, TimestampTz now,
								int64 fallback_out_of_window_us)
{
	int64 age_us;
	int64 out_of_window_us;

	if (entry->completed_at_ts == 0)
		return false;

	/*
	 * GCS-race round-2 RC-F: a requester completion proof makes the entry
	 * reclaim-safe immediately.  The identity-verified GCS_BLOCK_DONE
	 * proves the terminal reply was received, CRC-verified, and consumed
	 * on the requester, so no live retransmit of this request can still
	 * demand a re-serve.  This populates the §3.1 in-window whitelist by
	 * PROOF instead of per-status site audits (which stay empty below).
	 */
	if (entry->done_at_ts != 0)
		return true;

	/*
	 * GCS-race round-2 review F5: age against the entry's PINNED
	 * registration-time lifetime, never a caller-recomputed GUC threshold.
	 * The requester's legal window (wire hint, or the legacy protocol
	 * maximum) can be LONGER than this master's current GUC posture -- a
	 * SUSET change, or a legacy peer whose GUCs are longer than this
	 * master's; reclaiming on the shorter recomputation re-opens the
	 * replayed-retransmit re-execution window (the original P0 sequence).
	 */
	out_of_window_us
		= entry->pinned_lifetime_us > 0 ? entry->pinned_lifetime_us : fallback_out_of_window_us;

	age_us = (int64)(now - entry->completed_at_ts);
	if (age_us > out_of_window_us)
		return true;

	return GcsBlockReplyStatusIsReclaimIdempotent((GcsBlockReplyStatus)entry->status);
}


/* ============================================================
 * Public API.
 * ============================================================ */

/*
 * cluster_gcs_block_dedup_lookup_or_register — atomically look up or
 * register a request key.  Returns one of GcsBlockDedupResult.
 *
 *	If MISS_REGISTERED, an in-flight slot has been inserted.
 *	If CACHED_REPLY, cached_reply_out receives a by-value copy of the
 *	cached slot with valid reply_header + block_data ready to re-send.
 *
 *	The API never returns an internal HTAB entry pointer.  TTL sweep,
 *	node-dead cleanup, and backend-exit cleanup can remove entries as soon
 *	as this function releases the dedup lock, so CACHED_REPLY must be
 *	replayed from the copied entry.
 *
 *	PGRAC: spec-7.3 D5 — worker_id selects the per-worker dedup shard.  It
 *	MUST equal cluster_lms_shard_for_tag(tag, cluster_lms_workers): every
 *	message for a given block tag routes to worker[shard(tag)] (D4), so the
 *	dedup entry for that request lives in exactly one shard.  The caller
 *	(master-side handler) asserts shard(tag) == its own DATA channel before
 *	calling here.  worker_id out of [0, live shard count) is a mis-route
 *	(序破坏, 8.A): fail-closed → FULL + misroute_failclosed_count++, never
 *	served from a wrong shard.
 */
/*
 *	GCS-race round-2 RC-F + review F5 (calibration 2): the entry's TTL is
 *	pinned at registration by capability routing, and GC paths never
 *	re-read GUCs for a live entry.
 *
 *	  requester_done_capable (peer HELLO advertised GCS_DONE_V1):
 *	    hint in (0, protocol max]	-> pin 2x hint (the requester's own
 *									   legal-request lifetime carried on
 *									   the request wire, reserved_0[2..5])
 *	    hint == 0 or > protocol max	-> protocol violation: counted
 *									   (hint_violation_count) and DENIED
 *									   (VALIDATION_FAIL), never served
 *	  legacy peer (no capability, hint unknowable even if nonzero):
 *	    pin 2x GCS_BLOCK_DEDUP_MAX_PROTOCOL_LIFETIME_MS (counted,
 *	    legacy_pin_count) -- capacity pressure surfaces as DENIED FULL,
 *	    never as an early reclaim.
 */
extern GcsBlockDedupResult cluster_gcs_block_dedup_lookup_or_register(
	int worker_id, const GcsBlockDedupKey *key, BufferTag tag, uint8 transition_id,
	uint32 requester_lifetime_hint_ms, bool requester_done_capable,
	GcsBlockDedupEntry *cached_reply_out);

/*
 * cluster_gcs_block_dedup_mark_done — consume a requester completion proof
 * (GCS_BLOCK_DONE).  Under the shard's exclusive lock, verifies the FULL
 * identity (key 4-tuple locates the entry; tag + transition_id must match
 * the entry's HC91 fields; the entry must be COMPLETED) and only then
 * stamps done_at_ts.  Returns true when stamped; false on any mismatch or
 * miss (caller counts and drops -- DONE is advisory, TTL remains the
 * backstop).  Never removes the entry outright: the pinned done-linger
 * quarantine absorbs retransmit reorder slop, and eager reclaim may take
 * the entry immediately under cap pressure (IsReclaimSafe).
 */
extern bool cluster_gcs_block_dedup_mark_done(int worker_id, const GcsBlockDedupKey *key,
											  const BufferTag *tag, uint8 transition_id);

/* Count a handler-level DONE drop (transport identity / reserved-pad
 * validation, review F6) on the shard that would have consumed it. */
extern void cluster_gcs_block_dedup_note_done_mismatch(int worker_id);

/*
 * cluster_gcs_block_dedup_lifetime_ms — pure legal-request-lifetime
 * formula shared by both wire sides: backoff total + (retries+1) reply
 * timeouts.  The requester stamps its OWN GUC values into the request
 * hint; the master uses its own values only as the no-hint fallback.
 */
extern uint32 cluster_gcs_block_dedup_lifetime_ms(int initial_backoff_ms, int max_retries,
												  int reply_timeout_ms);

/*
 * Register the local backend cleanup hook.  This must be called from
 * sender/backend context, not from the master-side GCS handler, because
 * the handler may run in an auxiliary process without a backend id.
 */
extern void cluster_gcs_block_dedup_register_backend_exit_hook(void);

/*
 * cluster_gcs_block_dedup_install_reply — populate the in-flight slot
 * with the produced reply payload + set completed_at_ts.  Caller MUST
 * have first received MISS_REGISTERED from lookup_or_register.
 *
 *	block_data may be NULL for non-GRANTED status; the entry's block_data
 *	field is zero-filled in that case.
 */
extern void cluster_gcs_block_dedup_install_reply(int worker_id, const GcsBlockDedupKey *key,
												  GcsBlockReplyStatus status,
												  const GcsBlockReplyHeader *header,
												  const char *block_data);
extern void cluster_gcs_block_dedup_install_reply_ex(int worker_id, const GcsBlockDedupKey *key,
													 GcsBlockReplyStatus status,
													 const GcsBlockReplyHeader *header,
													 const char *block_data,
													 const ClusterSfDepVec *sf_dep_vec,
													 bool has_sf_dep);

/* Remove a specific entry by key (rare path; mostly used by tests).
 * worker_id selects the shard (spec-7.3 D5). */
extern void cluster_gcs_block_dedup_remove(int worker_id, const GcsBlockDedupKey *key);


/* ============================================================
 * GC hooks (HC93 — three-fold; HC95 callsite for epoch sweep separate).
 * ============================================================ */

/*
 * cluster_gcs_block_dedup_sweep_expired — TTL sweep called from LMON
 * tick body.  Removes any entry where:
 *
 *	  status != IN_FLIGHT && now - completed_at_ts > expiry_us
 *	OR
 *	  status == IN_FLIGHT && now - registered_at_ts > expiry_us
 *
 *	The expiry threshold is computed from the retransmit budget:
 *	  expiry_us = 2 × max_total_backoff_ms × 1000
 *	(default 2 × 1500 = 3 000 000 us).
 */
extern void cluster_gcs_block_dedup_sweep_expired(TimestampTz now);

/*
 * cluster_gcs_block_dedup_cleanup_on_backend_exit — local hook invoked
 * via before_shmem_exit.  Removes entries where origin_node_id matches
 * the local cluster_node_id and requester_backend_id matches the
 * exiting backend.  Remote master cleanup (origin = some_remote_node,
 * backend exits on remote) is NOT handled here; those entries are
 * reclaimed by TTL or by cleanup_on_node_dead.
 */
extern void cluster_gcs_block_dedup_cleanup_on_backend_exit(uint32 origin_node_id,
															int32 backend_id);

/*
 * cluster_gcs_block_dedup_cleanup_on_node_dead — called from spec-2.29
 * CSSD DEAD callback.  Removes all entries with the given origin_node_id.
 */
extern void cluster_gcs_block_dedup_cleanup_on_node_dead(uint32 node_id);


/* ============================================================
 * Observability accessors (counter exposure).
 * ============================================================ */
extern uint64 cluster_gcs_block_dedup_get_hit_count(void);
extern uint64 cluster_gcs_block_dedup_get_miss_count(void);
extern uint64 cluster_gcs_block_dedup_get_collision_count(void);
extern uint64 cluster_gcs_block_dedup_get_full_count(void);
extern uint64 cluster_gcs_block_dedup_get_in_flight_count(void);
extern uint64 cluster_gcs_block_dedup_get_evict_count(void);		  /* spec-7.2a D5 */
extern uint64 cluster_gcs_block_dedup_get_max_entries(void);		  /* spec-7.2a D5 */
extern uint64 cluster_gcs_block_dedup_get_done_marked_count(void);	  /* RC-F DONE */
extern uint64 cluster_gcs_block_dedup_get_done_mismatch_count(void);  /* RC-F DONE */
extern uint64 cluster_gcs_block_dedup_get_hint_violation_count(void); /* review F5 */
extern uint64 cluster_gcs_block_dedup_get_legacy_pin_count(void);	  /* review F5 */

/*
 * PGRAC: spec-7.3 D5 — count of dedup accesses rejected because worker_id
 * fell outside [0, live shard count).  A mis-route (a block tag reaching
 * the wrong LMS worker) is a code-path invariant violation (D3 HELLO
 * negotiates a cluster-wide n_workers; D1 shard() is byte-identical on
 * both ends), so this stays 0 in a healthy cluster.  A non-zero value is
 * a fail-closed drop (8.A), never a wrong-shard serve.  Summed across all
 * shards but stored once in the always-present ctl header.
 */
extern uint64 cluster_gcs_block_dedup_get_misroute_failclosed_count(void);

/* Record a mis-routed dedup access (shard(tag) != serving worker); shared
 * by the module bounds guard and the master-side handler (spec-7.3 D5). */
extern void cluster_gcs_block_dedup_note_misroute(void);


/* ============================================================
 * Shmem registry — registered via cluster_shmem_register_region().
 * ============================================================ */
extern Size cluster_gcs_block_dedup_shmem_size(void);
extern void cluster_gcs_block_dedup_shmem_init(void);
extern void cluster_gcs_block_dedup_module_init(void);


#endif /* USE_PGRAC_CLUSTER */

#endif /* CLUSTER_GCS_BLOCK_DEDUP_H */
