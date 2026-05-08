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

#include "cluster/cluster_guc.h" /* cluster_node_id */
#include "cluster/cluster_ic.h"	 /* cluster_ic_send_bytes (vtable) */
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_router.h"


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

bool
cluster_ic_send_envelope(uint8 msg_type, int32 dest_node_id, const void *payload,
						 uint32 payload_len)
{
	const ClusterICMsgTypeInfo *info;
	ClusterICEnvelope env;

	if (msg_type == 0 || (int)msg_type >= CLUSTER_IC_MSG_TYPE_MAX)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cluster_ic_send_envelope: msg_type %u out of range", msg_type)));

	info = &dispatch_table[msg_type];

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

	/* (3) dest = self -- short-circuit no-op success.  spec-2.2 stub
	 * tier preserves this; non-LMON callers in spec-2.3 are gated by
	 * (2) above so this branch only matters for HEARTBEAT-from-LMON
	 * targeting self (test fixtures + future loopback diagnostics). */
	if (dest_node_id == cluster_node_id)
		return true;

	/* (4) broadcast destination check */
	if ((uint32)dest_node_id == PGRAC_IC_BROADCAST && !info->broadcast_ok)
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

	/* Two sends per frame: header + (optional) payload.  Per spec-2.0
	 * §4 wire format -- reader recvs envelope first, then payload of
	 * env->payload_length bytes.  Hardening v1.0.1 F1 partial-IO
	 * buffering at tier1_send_bytes preserves frame integrity. */
	if (!cluster_ic_send_bytes(dest_node_id, &env, sizeof(env)))
		return false;
	if (payload_len > 0)
		return cluster_ic_send_bytes(dest_node_id, payload, payload_len);

	return true;
}


/* ============================================================
 * Dispatch path (LMON recv).
 * ============================================================ */

bool
cluster_ic_dispatch_envelope(const ClusterICEnvelope *env, const void *payload)
{
	const ClusterICMsgTypeInfo *info;
	MemoryContext old_ctx;

	if (env == NULL)
		return false;

	if (env->msg_type == 0 || (int)env->msg_type >= CLUSTER_IC_MSG_TYPE_MAX)
		return false; /* peer sent malformed msg_type; caller close peer */

	info = &dispatch_table[env->msg_type];

	/* spec-2.3 §1.4 invariant 8 + §3.5b inbound: unregistered msg_type
	 * from peer is a peer-level failure.  Return false; caller (LMON)
	 * is expected to close peer + LOG/WARNING + metric.  NEVER ereport
	 * ERROR LMON (would crash the main loop). */
	if (!slot_registered(info) || info->handler == NULL)
		return false;

	/* spec-2.3 §3.5 + Q14 + R3 防御层: PG_TRY/PG_CATCH wrap.  Catches
	 * elevel == ERROR (handler violated §3.5 by raising ERROR) -- LOG
	 * + drop frame + reset MemoryContext + LMON continue.  ereport
	 * FATAL/PANIC are NOT caught (PG semantics terminates the process;
	 * postmaster crash recovery restarts). */
	old_ctx = CurrentMemoryContext;
	PG_TRY();
	{
		info->handler(env, payload);
	}
	PG_CATCH();
	{
		ErrorData *err;

		MemoryContextSwitchTo(old_ctx);
		err = CopyErrorData();
		ereport(LOG, (errmsg("cluster_ic dispatch handler for msg_type %u "
							 "(\"%s\") violated §3.5 (raised ERROR); frame "
							 "dropped, LMON continues",
							 env->msg_type, info->name),
					  errdetail("%s", err->message)));
		FreeErrorData(err);
		FlushErrorState();
		MemoryContextReset(old_ctx);
	}
	PG_END_TRY();

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


#endif /* USE_PGRAC_CLUSTER */
