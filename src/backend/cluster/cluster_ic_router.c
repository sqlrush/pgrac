/*-------------------------------------------------------------------------
 *
 * cluster_ic_router.c
 *	  pgrac cluster IC message-type registration + send/dispatch
 *	  abstraction (spec-2.3 D4 implementation).
 *
 *	  See cluster_ic_router.h for the API contract + spec-2.3
 *	  references.  This file implements:
 *
 *	    - dispatch_table[256] static process-local array
 *	    - cluster_ic_register_msg_type (postmaster phase 1)
 *	    - cluster_ic_send_envelope (any producer; producer_mask gated)
 *	    - cluster_ic_dispatch_envelope (LMON recv;PG_TRY wrap)
 *	    - cluster_ic_get_msg_type_info (diagnostic accessor)
 *	    - cluster_ic_router_count_registered (test helper)
 *
 *	  spec-2.3 §1.4 hard invariants enforced:
 *	    #4 dispatch_table not in shmem (function pointer is L61 process
 *	       resource); register at postmaster phase 1; duplicate FATAL
 *	    #6 handler 4 hard constraints + PG_TRY catch ERROR only;
 *	       FATAL/PANIC pass through
 *	    #7 outbound payload > 16 MB → ereport ERROR + errhint
 *	    #8 inbound unregistered msg_type → caller (LMON) closes peer
 *	       (this function returns false; never ereport ERROR LMON)
 *
 *	  Spec authority: pgrac:specs/spec-2.3-...md frozen v0.2
 *	  (2026-05-07; user approve Q1-Q14 + 4 hard 修订).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_ic_router.c
 *
 * NOTES
 *	  pgrac-original file.  Built only in --enable-cluster mode
 *	  (USE_PGRAC_CLUSTER); function bodies are gated.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include <string.h>

#include "miscadmin.h" /* MyBackendType */
#include "utils/elog.h"
#include "utils/memutils.h"

#include "cluster/cluster_conf.h"	  /* cluster_conf_lookup_node (spec-2.5 D2.5 fanout) */
#include "cluster/cluster_guc.h"	  /* cluster_node_id */
#include "cluster/cluster_ic.h"		  /* cluster_ic_send_bytes (vtable) */
#include "cluster/cluster_ic_chunk.h" /* PGRAC_IC_CHUNK_MSG_TYPE + chunk_dispatch_frame (v1.0.1 F1) */
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_ic_tier1.h" /* cluster_ic_tier1_get_peer_fd (spec-2.5 D2.5 fanout) */


/* ============================================================
 * dispatch_table -- static process-local array.
 *
 *   spec-2.3 §1.4 invariant 4 + Q11: process-local; populated by
 *   postmaster phase 1; fork-inherited (Linux/macOS) or
 *   re-populated via EXEC_BACKEND child re-init (Windows).
 *   Function pointer is L61 process-resource and CANNOT live in
 *   shmem (cross-process pointer invalid under ASLR).
 *
 *   Storage: ~8 KB process-local (256 slots × ~32 bytes/slot).
 *   Compared with system-wide shmem this is the right tradeoff
 *   per Q4 + Q11 -- mirrors PG RmgrTable[] pattern.
 * ============================================================ */

static ClusterICMsgTypeInfo dispatch_table[CLUSTER_IC_MSG_TYPE_MAX];

/*
 * "registered" predicate: a slot is occupied iff name != NULL.
 * (handler may be NULL for send-only msg_types per the API
 * contract; using `name != NULL` keeps the predicate consistent
 * across send-only and dispatch-having types.)
 */
static inline bool
slot_registered(const ClusterICMsgTypeInfo *slot)
{
	return slot != NULL && slot->name != NULL;
}


/* ============================================================
 * Registration API.
 * ============================================================ */

void
cluster_ic_register_msg_type(const ClusterICMsgTypeInfo *info)
{
	ClusterICMsgTypeInfo *slot;

	if (info == NULL)
		ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster_ic_register_msg_type called with NULL info")));

	if (info->msg_type == 0 || (int)info->msg_type >= CLUSTER_IC_MSG_TYPE_MAX)
		ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster_ic_register_msg_type: msg_type %u out of range "
							   "[1, %d)",
							   info->msg_type, CLUSTER_IC_MSG_TYPE_MAX)));

	if (info->name == NULL || info->name[0] == '\0')
		ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster_ic_register_msg_type: msg_type %u name "
							   "must be non-empty",
							   info->msg_type)));

	slot = &dispatch_table[info->msg_type];

	/*
	 * spec-2.3 §1.4 invariant 4 + Q9: duplicate registration is FATAL.
	 * The init-layer guard in cluster_init_shmem() prevents accidental
	 * re-entry (shmem init runs once per postmaster phase 1); only direct
	 * duplicate cluster_ic_register_msg_type() calls trip this branch.
	 *
	 * NOTE: in cluster_unit standalone tests, ereport(FATAL) is stubbed
	 * to no-op; the explicit `return` below preserves the original
	 * registration when tests deliberately probe the duplicate path
	 * (test_cluster_ic_router U8 verifies the second call did NOT
	 * overwrite the first slot).
	 */
	if (slot_registered(slot)) {
		ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster_ic_register_msg_type: msg_type %u (\"%s\") "
							   "already registered as \"%s\"",
							   info->msg_type, info->name, slot->name),
						errhint("spec-2.3 §1.4 invariant 4: register-once is contract; "
								"init-layer guard should prevent re-entry.")));
		return; /* unreachable in real PG; pertinent for unit-test stubs */
	}

	*slot = *info;
}


/* ============================================================
 * Send path.
 * ============================================================ */

ClusterICSendResult
cluster_ic_send_envelope(uint8 msg_type, int32 dest_node_id, const void *payload,
						 uint32 payload_len)
{
	const ClusterICMsgTypeInfo *info;
	ClusterICEnvelope env;
	ClusterICSendResult rc;
	bool is_chunk_wrap = (msg_type == PGRAC_IC_CHUNK_MSG_TYPE);

	if (msg_type == 0 || (int)msg_type >= CLUSTER_IC_MSG_TYPE_MAX)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cluster_ic_send_envelope: msg_type %u out of range", msg_type)));

	info = &dispatch_table[msg_type];

	/*
	 * spec-2.4 hardening v1.0.1 F1 (L76 register-vs-handler-signature-coupling):
	 * msg_type == PGRAC_IC_CHUNK_MSG_TYPE (255) is a wire-level wrapper, NOT a
	 * dispatch_table entry.  Caller is cluster_ic_send_envelope_chunked which
	 * has already validated the inner_msg_type via its own contract.  Skip
	 * registered + producer_mask + broadcast_ok checks for chunk wrap;
	 * payload size ceiling still applies (16 MB envelope cap).
	 *
	 * Inner_msg_type validation lives in send_envelope_chunked (it rejects
	 * inner=0 and inner=255 at entry); recv-side dispatch_envelope short-
	 * circuits msg_type=255 to chunk_dispatch_frame BEFORE dispatch_table
	 * lookup so the inner handler eventually fires through normal
	 * dispatch_envelope after reassembly.
	 */
	if (!is_chunk_wrap) {
		/* (1) registered? */
		if (!slot_registered(info))
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cluster_ic msg_type %u not registered", msg_type),
							errhint("Each subsystem must call cluster_ic_register_msg_type "
									"in postmaster phase 1 (cluster_init_shmem).")));

		/* (2) producer scope guard (spec-2.3 §3.4 + spec-2.2 §3.9 升级) */
		if ((info->allowed_producer_mask & (1u << MyBackendType)) == 0)
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cluster_ic msg_type %u (\"%s\") not allowed from "
								   "BackendType %d",
								   msg_type, info->name, (int)MyBackendType),
							errhint("msg_type %u allowed_producer_mask = 0x%x; "
									"see pg_cluster_ic_msg_types view (spec-2.3 D8).",
									msg_type, info->allowed_producer_mask)));
	}

	/* (3) dest = self -- short-circuit no-op success.  spec-2.2 stub
	 * tier preserves this; non-LMON callers in spec-2.3 are gated by
	 * (2) above so this branch only matters for HEARTBEAT-from-LMON
	 * targeting self (test fixtures + future loopback diagnostics). */
	if (dest_node_id == cluster_node_id)
		return CLUSTER_IC_SEND_DONE;

	/* (4) broadcast destination check (chunk wrap inherits broadcast_ok=true
	 * implicit -- chunked send is always unicast or broadcast at caller level).
	 */
	if (!is_chunk_wrap && (uint32)dest_node_id == PGRAC_IC_BROADCAST && !info->broadcast_ok)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cluster_ic msg_type %u (\"%s\") does not allow "
							   "BROADCAST destination",
							   msg_type, info->name)));

	/* (5) payload size ceiling -- spec-2.3 §3.5b outbound 16 MB rule */
	if (payload_len > PGRAC_IC_PAYLOAD_MAX)
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						errmsg("cluster_ic envelope payload %u exceeds 16 MB limit", payload_len),
						errhint("Large payload framing/chunking lands in spec-2.4; "
								"reduce msg payload or break into multiple sends.")));

	/* Build envelope + delegate to byte-stream vtable. */
	if (!cluster_ic_envelope_build(&env, msg_type, (uint32)cluster_node_id, (uint32)dest_node_id,
								   payload, payload_len))
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster_ic_envelope_build failed for msg_type %u", msg_type)));

	/*
	 * Two sends per frame: header + (optional) payload.  Per spec-2.0
	 * §4 wire format -- reader recvs envelope first, then payload of
	 * env->payload_length bytes.  Hardening v1.0.1 F1 partial-IO
	 * buffering at tier1_send_bytes preserves frame integrity.
	 *
	 * spec-2.3 hardening v1.0.1 F1 (L68): three-state propagation.
	 * Header-send WOULD_BLOCK / HARD_ERROR returns immediately;
	 * payload (if any) is only attempted on header DONE.
	 */
	rc = cluster_ic_send_bytes(dest_node_id, &env, sizeof(env));
	if (rc != CLUSTER_IC_SEND_DONE)
		return rc;
	if (payload_len > 0)
		return cluster_ic_send_bytes(dest_node_id, payload, payload_len);

	return CLUSTER_IC_SEND_DONE;
}


/* ============================================================
 * Dispatch path (LMON recv).
 * ============================================================ */

bool
cluster_ic_dispatch_envelope(const ClusterICEnvelope *env, const void *payload, int32 peer_id)
{
	const ClusterICMsgTypeInfo *info;
	MemoryContext old_ctx;
	MemoryContext dispatch_ctx;

	if (env == NULL)
		return false;

	if (env->msg_type == 0 || (int)env->msg_type >= CLUSTER_IC_MSG_TYPE_MAX)
		return false; /* peer sent malformed msg_type; caller close peer */

	/*
	 * spec-2.4 hardening v1.0.1 F1 (L76 register-vs-handler-signature-coupling):
	 * msg_type=255 (PGRAC_IC_CHUNK_MSG_TYPE) is a wire-level wrapper, NOT a
	 * dispatch_table entry.  Short-circuit to chunk_dispatch_frame BEFORE
	 * dispatch_table lookup.  chunk_dispatch_frame does its own state-machine
	 * validation + on completion synthesizes inner envelope and re-enters
	 * cluster_ic_dispatch_envelope (so inner_msg_type's handler eventually
	 * fires through the standard dispatch_table + PG_TRY isolation path).
	 */
	if (env->msg_type == PGRAC_IC_CHUNK_MSG_TYPE)
		return cluster_ic_chunk_dispatch_frame(env, payload, peer_id);

	info = &dispatch_table[env->msg_type];

	/* spec-2.3 §1.4 invariant 8 + §3.5b inbound: unregistered msg_type
	 * from peer is a peer-level failure.  Return false; caller (LMON)
	 * is expected to close peer + LOG/WARNING + metric.  NEVER ereport
	 * ERROR LMON (would crash the main loop). */
	if (!slot_registered(info) || info->handler == NULL)
		return false;

	/*
	 * spec-2.3 hardening v1.0.1 F4 (L71 metadata-symmetric-enforce):
	 * inbound broadcast_ok check.  send path (cluster_ic_send_envelope
	 * step 4 above) ereport-rejects BROADCAST when !info->broadcast_ok;
	 * dispatch path mirrors that for peer-originated frames -- a peer
	 * that forges a BROADCAST msg_type whose registered metadata says
	 * point-to-point only is a contract violation.  Return false;
	 * caller (LMON) closes peer (NOT ereport ERROR -- would crash LMON
	 * main loop on hostile remote input).
	 */
	if (env->dest_node_id == PGRAC_IC_BROADCAST && !info->broadcast_ok)
		return false;

	/*
	 * spec-2.3 §3.5 + Q14 + R3 防御层: PG_TRY/PG_CATCH wrap.  Catches
	 * elevel == ERROR (handler violated §3.5 by raising ERROR) -- LOG
	 * + drop frame + LMON continue.  ereport FATAL/PANIC are NOT caught
	 * (PG semantics terminates the process; postmaster crash recovery
	 * restarts).
	 *
	 * spec-2.3 hardening v1.0.1 F5 (L72 dispatch-isolation-context):
	 * handler runs in a fresh per-dispatch short-lived MemoryContext
	 * parented at old_ctx.  On catch we delete the dispatch_ctx -- this
	 * frees any palloc the handler did before raising ERROR but does
	 * NOT touch the caller's (LMON main loop) working memory.  Pre-fix
	 * code did MemoryContextReset(old_ctx) which nuked LMON in-flight
	 * state; spec-2.3 happened to dodge that because HEARTBEAT handler
	 * was a no-op stub, but spec-2.4 chunk reassembly + spec-2.13 GES
	 * handler will allocate + can ereport ERROR for real.
	 */
	old_ctx = CurrentMemoryContext;
	dispatch_ctx = AllocSetContextCreate(old_ctx, "cluster_ic dispatch", ALLOCSET_SMALL_SIZES);
	MemoryContextSwitchTo(dispatch_ctx);
	PG_TRY();
	{
		info->handler(env, payload);
	}
	PG_CATCH();
	{
		ErrorData *err;

		/* Switch BACK to old_ctx before CopyErrorData so the copy lives
		 * in caller (LMON) memory, not in dispatch_ctx (about to be
		 * deleted). */
		MemoryContextSwitchTo(old_ctx);
		err = CopyErrorData();
		ereport(LOG, (errmsg("cluster_ic dispatch handler for msg_type %u "
							 "(\"%s\") violated §3.5 (raised ERROR); frame "
							 "dropped, LMON continues",
							 env->msg_type, info->name),
					  errdetail("%s", err->message)));
		FreeErrorData(err);
		FlushErrorState();
	}
	PG_END_TRY();
	MemoryContextSwitchTo(old_ctx);
	MemoryContextDelete(dispatch_ctx);

	return true;
}


/* ============================================================
 * Diagnostic accessors.
 * ============================================================ */

const ClusterICMsgTypeInfo *
cluster_ic_get_msg_type_info(uint8 msg_type)
{
	if (msg_type == 0 || (int)msg_type >= CLUSTER_IC_MSG_TYPE_MAX)
		return NULL;
	if (!slot_registered(&dispatch_table[msg_type]))
		return NULL;
	return &dispatch_table[msg_type];
}

int
cluster_ic_router_count_registered(void)
{
	int count = 0;
	int i;

	for (i = 1; i < CLUSTER_IC_MSG_TYPE_MAX; i++) /* skip msg_type 0 sentinel */
	{
		if (slot_registered(&dispatch_table[i]))
			count++;
	}
	return count;
}


/* ============================================================
 * Fanout API (spec-2.5 D2.5; v0.2 Q3 修订采纳).
 *
 *   See cluster_ic_router.h for full caller contract + lessons SSOT
 *   inheritance (L61 / L68 / L71 / L74).
 *
 *   Implementation:
 *     1. Pre-loop validation (ereport ERROR on spec violation):
 *        msg_type range / registered / broadcast_ok=true /
 *        MyBackendType ∈ allowed_producer_mask / payload_len ≤
 *        PGRAC_IC_PAYLOAD_MAX
 *
 *     2. Per-peer loop:
 *        - peer == cluster_node_id:                PEER_DOWN (skip self)
 *        - cluster_conf_lookup_node(peer) == NULL: PEER_DOWN (un-declared)
 *        - cluster_ic_tier1_get_peer_fd(peer) < 0: PEER_DOWN (not connected)
 *        - else: build envelope (dest=peer) + cluster_ic_send_bytes;
 *                map ClusterICSendResult → ClusterICFanoutResult
 *
 *   NB: per-peer send path here bypasses cluster_ic_send_envelope's
 *   own dest=BROADCAST + producer_mask + registered checks (already
 *   pre-validated once).  This is intentional: avoid N × constant-cost
 *   re-validation;the per-peer dest is concrete (not BROADCAST sentinel)
 *   so symmetric checks don't apply per peer.
 * ============================================================ */

void
cluster_ic_send_envelope_fanout(uint8 msg_type, const void *payload, uint32 payload_len,
								ClusterICFanoutResult per_peer[])
{
	const ClusterICMsgTypeInfo *info;
	int peer;

	/* (1) msg_type range. */
	if (msg_type == 0 || (int)msg_type >= CLUSTER_IC_MSG_TYPE_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cluster_ic_send_envelope_fanout: msg_type %u out of range", msg_type)));

	info = &dispatch_table[msg_type];

	/* (2) registered. */
	if (!slot_registered(info))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cluster_ic_send_envelope_fanout: msg_type %u not registered", msg_type),
				 errhint("Each subsystem must call cluster_ic_register_msg_type "
						 "in postmaster phase 1 (cluster_init_shmem).")));

	/* (3) broadcast_ok send-side enforce (spec-2.5 R13;L71 metadata-symmetric).
	 *     fanout API is a broadcast-flavored entry point;callers must register
	 *     broadcast_ok=true so inbound dispatch (cluster_ic_dispatch_envelope)
	 *     mirrors the contract for peer-originated frames. */
	if (!info->broadcast_ok)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cluster_ic_send_envelope_fanout: msg_type %u (\"%s\") "
							   "registered with broadcast_ok=false",
							   msg_type, info->name),
						errhint("Fanout is a broadcast-flavored API;register the msg_type "
								"with broadcast_ok=true or use cluster_ic_send_envelope "
								"for unicast.")));

	/* (4) producer_mask. */
	if ((info->allowed_producer_mask & (1u << MyBackendType)) == 0)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cluster_ic_send_envelope_fanout: msg_type %u (\"%s\") not "
							   "allowed from BackendType %d",
							   msg_type, info->name, (int)MyBackendType),
						errhint("msg_type %u allowed_producer_mask = 0x%x.", msg_type,
								info->allowed_producer_mask)));

	/* (5) payload size ceiling. */
	if (payload_len > PGRAC_IC_PAYLOAD_MAX)
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						errmsg("cluster_ic_send_envelope_fanout: payload %u exceeds 16 MB limit",
							   payload_len)));

	/* (6) Per-peer loop.  Initialize all slots PEER_DOWN; overwrite as we go. */
	for (peer = 0; peer < CLUSTER_MAX_NODES; peer++)
		per_peer[peer] = CLUSTER_IC_FANOUT_PEER_DOWN;

	for (peer = 0; peer < CLUSTER_MAX_NODES; peer++) {
		ClusterICEnvelope env;
		ClusterICSendResult rc;

		/* Skip self -- broadcast semantics exclude sender. */
		if (peer == cluster_node_id)
			continue;

		/* Skip un-declared peers (cluster_conf doesn't know about them). */
		if (cluster_conf_lookup_node(peer) == NULL)
			continue;

		/* Skip peers not yet connected (phase 4 first-tick / restart);
		 * spec-2.5 §1.4 例外 #4 PEER_DOWN nonfatal contract. */
		if (cluster_ic_tier1_get_peer_fd(peer) < 0)
			continue;

		/* Build envelope + send.  spec-2.4 v1.0.1 / spec-2.3 ratify ABI. */
		if (!cluster_ic_envelope_build(&env, msg_type, (uint32)cluster_node_id, (uint32)peer,
									   payload, payload_len)) {
			per_peer[peer] = CLUSTER_IC_FANOUT_HARD_ERROR;
			continue;
		}

		rc = cluster_ic_send_bytes(peer, &env, sizeof(env));
		if (rc == CLUSTER_IC_SEND_DONE && payload_len > 0)
			rc = cluster_ic_send_bytes(peer, payload, payload_len);

		switch (rc) {
		case CLUSTER_IC_SEND_DONE:
			per_peer[peer] = CLUSTER_IC_FANOUT_DONE;
			break;
		case CLUSTER_IC_SEND_WOULD_BLOCK:
			per_peer[peer] = CLUSTER_IC_FANOUT_WOULD_BLOCK;
			break;
		case CLUSTER_IC_SEND_HARD_ERROR:
			per_peer[peer] = CLUSTER_IC_FANOUT_HARD_ERROR;
			break;
		}
	}
}


#endif /* USE_PGRAC_CLUSTER */
