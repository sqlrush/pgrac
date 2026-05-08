/*-------------------------------------------------------------------------
 *
 * cluster_ic_envelope.c
 *	  pgrac cluster IC envelope ABI implementation (spec-2.3 D2).
 *
 *	  Implements:
 *	    - cluster_ic_envelope_build      (fill fields + compute CRC)
 *	    - cluster_ic_envelope_verify     (6-step inbound path)
 *	    - cluster_ic_envelope_compute_crc (helper; tests + build/verify)
 *
 *	  Plus 4 StaticAssertDecl that lock the wire ABI:
 *	    sizeof(ClusterICEnvelope)             == 36
 *	    offsetof(ClusterICEnvelope, epoch)    == 12
 *	    offsetof(ClusterICEnvelope, scn)      == 20
 *	    offsetof(ClusterICEnvelope, payload_length) == 28
 *
 *	  Spec authority: pgrac:specs/spec-2.3-envelope-abi-ratify-
 *	  transport-agnostic-api.md frozen v0.2 (2026-05-07; user
 *	  approve Q1-Q14 + 4 hard 修订).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_ic_envelope.c
 *
 * NOTES
 *	  pgrac-original file.  Built only in --enable-cluster mode
 *	  (USE_PGRAC_CLUSTER); function bodies are gated.  The
 *	  ClusterICEnvelope struct in cluster_ic_envelope.h is frontend-
 *	  safe so other diagnostic tools can parse wire bytes even on
 *	  --disable-cluster builds.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include <string.h>

#include "port/pg_crc32c.h"

#include "cluster/cluster_conf.h"  /* cluster_conf_lookup_node (F2 L69) */
#include "cluster/cluster_epoch.h" /* cluster_epoch_get_current (spec-2.4 D1) */
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_tier1.h" /* peer_stats counters (spec-2.4 D10) */
#include "cluster/cluster_scn.h"	  /* cluster_scn_observe / current (spec-2.4 D4) */


/* ============================================================
 * Wire ABI lock (spec-2.0 §4 frozen + spec-2.3 §1.4 invariant 1).
 * Compile-time fail if any field reorders.
 * ============================================================ */

StaticAssertDecl(sizeof(ClusterICEnvelope) == PGRAC_IC_ENVELOPE_BYTES,
				 "ClusterICEnvelope must be exactly 36 bytes (spec-2.0 §4 frozen)");
StaticAssertDecl(offsetof(ClusterICEnvelope, epoch) == 12,
				 "ClusterICEnvelope.epoch offset frozen at 12");
StaticAssertDecl(offsetof(ClusterICEnvelope, scn) == 20,
				 "ClusterICEnvelope.scn offset frozen at 20");
StaticAssertDecl(offsetof(ClusterICEnvelope, payload_length) == 28,
				 "ClusterICEnvelope.payload_length offset frozen at 28");


/* ============================================================
 * Public API.
 * ============================================================ */

uint32
cluster_ic_envelope_compute_crc(const ClusterICEnvelope *env, const void *payload)
{
	pg_crc32c crc;
	const uint8 *env_bytes = (const uint8 *)env;
	const size_t crc_offset = offsetof(ClusterICEnvelope, payload_crc32c);

	Assert(env != NULL);

	/*
	 * CRC coverage = envelope[0..32] (the 32 bytes preceding the
	 * payload_crc32c field at offset 32) + payload[0..payload_length].
	 *
	 * Per spec-2.3 §3.3 + Q3 boundary clarification: even with
	 * payload_length == 0, the envelope header has multi-byte content
	 * (magic alone is 2 nonzero bytes), so the resulting CRC is
	 * effectively never zero -- "mandatory non-zero" from spec-2.0 §4
	 * is a wire-correctness invariant, not a value-of-CRC constraint.
	 */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, env_bytes, crc_offset);
	if (env->payload_length > 0 && payload != NULL)
		COMP_CRC32C(crc, payload, env->payload_length);
	FIN_CRC32C(crc);

	return (uint32)crc;
}

bool
cluster_ic_envelope_build(ClusterICEnvelope *out_env, uint8 msg_type, uint32 source_node_id,
						  uint32 dest_node_id, const void *payload, uint32 payload_length)
{
	if (out_env == NULL)
		return false;
	if (payload_length > PGRAC_IC_PAYLOAD_MAX)
		return false; /* outbound caller ereport ERROR per §3.5b */

	/*
	 * spec-2.3 hardening v1.0.1 F3 (L70 contract-API-NULL-payload):
	 * payload_length > 0 with a NULL payload buffer is a contract
	 * violation -- the CRC would silently cover only the envelope
	 * header, which lets a malicious peer forge a frame whose
	 * CRC matches but whose body never existed.  Reject at build.
	 */
	if (payload_length > 0 && payload == NULL)
		return false;

	out_env->magic = PGRAC_IC_ENVELOPE_MAGIC;
	out_env->version = PGRAC_IC_ENVELOPE_VERSION_V1;
	out_env->msg_type = msg_type;
	out_env->source_node_id = source_node_id;
	out_env->dest_node_id = dest_node_id;
	/*
	 * spec-2.4 D4: build now stamps current local epoch + SCN snapshot
	 * (was 0 / 0 in spec-2.3 Q12).  spec-2.4 期 epoch always 0 (spec-
	 * 2.29 reconfig is the first bump);scn is whatever cluster_scn
	 * currently shows -- spec-2.10 walwriter真激活 makes this advance
	 * meaningfully.
	 */
	out_env->epoch = cluster_epoch_get_current();
	out_env->scn = (uint64)cluster_scn_current();
	out_env->payload_length = payload_length;
	out_env->payload_crc32c = 0; /* placeholder; computed below */

	/*
	 * CRC compute MUST come AFTER all other fields are set, and AFTER
	 * payload_crc32c is zeroed -- coverage = env[0..32] (excluding
	 * crc field at offset 32) + payload.  compute_crc only walks the
	 * 32 bytes preceding the crc field, so the placeholder zero in
	 * payload_crc32c doesn't pollute the result.
	 */
	out_env->payload_crc32c = cluster_ic_envelope_compute_crc(out_env, payload);

	return true;
}

bool
cluster_ic_envelope_verify(const ClusterICEnvelope *env, const void *payload, uint32 payload_len,
						   uint32 self_node_id, int32 peer_id)
{
	if (env == NULL)
		return false;

	/* Step 1: magic */
	if (env->magic != PGRAC_IC_ENVELOPE_MAGIC)
		return false;

	/* Step 2: version (anti-mixed-wire defense layer 1, spec-2.3 §3.7) */
	if (env->version != PGRAC_IC_ENVELOPE_VERSION_V1)
		return false;

	/*
	 * Step 3: source_node_id sanity.
	 *
	 * Reject the broadcast sentinel (broadcast can only appear in
	 * dest_node_id).  spec-2.3 hardening v1.0.1 F2 (L69
	 * inbound-identity-binding) adds two further checks: when the
	 * caller knows the peer fd's HELLO-bound identity (peer_id >= 0),
	 * env->source_node_id must == peer_id (otherwise the peer is
	 * faking sender identity);and source_node_id must be a declared
	 * cluster member (cluster_conf_lookup_node).  Pre-handshake
	 * contexts pass peer_id = -1 to skip the identity binding while
	 * still range-scanning ClusterConf.
	 */
	if (env->source_node_id == PGRAC_IC_BROADCAST)
		return false;
	if (peer_id >= 0 && (int32)env->source_node_id != peer_id)
		return false;
	if (cluster_conf_lookup_node((int32)env->source_node_id) == NULL)
		return false;

	/* Step 4: dest = self OR broadcast */
	if (env->dest_node_id != self_node_id && env->dest_node_id != PGRAC_IC_BROADCAST)
		return false;

	/*
	 * Step 5: payload_length sanity (per §3.5b inbound rule) +
	 * spec-2.3 hardening v1.0.1 F3 (L70 contract-API-NULL-payload):
	 * envelope claim must match caller-supplied buffer length, and
	 * non-zero claim with NULL buffer is illegal.
	 */
	if (env->payload_length > PGRAC_IC_PAYLOAD_MAX)
		return false;
	if (env->payload_length != payload_len)
		return false;
	if (env->payload_length > 0 && payload == NULL)
		return false;

	/*
	 * Step 6: CRC.  compute_crc only walks env[0..32] + payload, so
	 * we don't need to zero env->payload_crc32c first.
	 */
	if (env->payload_crc32c != cluster_ic_envelope_compute_crc(env, payload))
		return false;

	/*
	 * Step 7: spec-2.4 D4 -- epoch enforce (Invariant 2 per spec-2.0
	 * §3.2).  Reject envelopes carrying a stale epoch (typically
	 * reconfig in-flight pre-thaw frames in spec-2.29 era).
	 *
	 * spec-2.4 期 current_epoch always = CLUSTER_EPOCH_INITIAL = 0;
	 * sender side also reads the same shmem accessor so production
	 * traffic always passes this check.  fault-inject (cluster_tap
	 * 082) and post-spec-2.29 reconfig flow are the only paths that
	 * exercise the reject branch.
	 *
	 * Counter bump + LOG describe verify's own decision (this is
	 * still validation, not state propagation -- safe inside the
	 * pure verify path per Q2 contract;NO SCN advance here).
	 */
	{
		uint64 my_epoch = cluster_epoch_get_current();
		uint64 env_epoch;

		memcpy(&env_epoch, &env->epoch, sizeof(uint64)); /* L34 unaligned */
		if (env_epoch != my_epoch) {
			cluster_ic_tier1_bump_stale_epoch_drop((int32)env->source_node_id);
			ereport(LOG, (errcode(ERRCODE_CLUSTER_IC_STALE_EPOCH_DROP),
						  errmsg("cluster_ic dropped envelope from node %u: "
								 "stale epoch " UINT64_FORMAT " != current " UINT64_FORMAT,
								 env->source_node_id, env_epoch, my_epoch),
						  errdetail("spec-2.4 Invariant 2 enforce -- pre-reconfig "
									"or replay frame;peer NOT closed.")));
			return false; /* drop -- caller does NOT close peer */
		}
	}

	return true;
}

/*
 * spec-2.4 §2.7 Q2 修订: Lamport SCN piggyback observe.
 *
 * STATEFUL effect.  Caller MUST have called cluster_ic_envelope_verify
 * AND received true return BEFORE calling this function.  Calling
 * observe on a not-yet-verified envelope is a contract violation:
 * forged / spoofed / stale-epoch frames must NEVER reach observe
 * (otherwise hostile peer can spoof local SCN advance via a frame
 * with future env.scn but bad CRC).
 *
 * Effects:
 *   - cluster_scn_observe(env->scn) -- Lamport `>=` advance per L21
 *   - peer_stats[source_node_id].lamport_observe_advance_count++ if
 *     SCN actually advanced
 *
 * Returns true iff local SCN advanced, false on no-op (env_scn == 0
 * or <= current).
 */
bool
cluster_ic_envelope_observe_scn(const ClusterICEnvelope *env, int32 source_node_id)
{
	uint64 env_scn;
	SCN before, after;

	if (env == NULL)
		return false;

	memcpy(&env_scn, &env->scn, sizeof(uint64)); /* L34 */
	if (env_scn == 0)
		return false;

	before = cluster_scn_current();
	cluster_scn_observe((SCN)env_scn);
	after = cluster_scn_current();

	if (after > before) {
		cluster_ic_tier1_bump_lamport_advance(source_node_id);
		return true;
	}
	return false;
}

/*
 * spec-2.4 §2.7 Q2 修订: LMON-facing wrapper.
 *
 * Calls verify() then observe_scn() in the correct order.  Returns
 * true iff verify pass (and observe_scn was invoked).  Verify reject
 * means observe_scn is NOT called -- contract preserved.
 *
 * Tier1 recv heartbeat drain + future spec-2.4 chunked dispatch use
 * this wrapper.  Lower-level callers (mock / dry-run / unit-test)
 * call verify() alone to avoid SCN pollution.
 */
bool
cluster_ic_envelope_accept_and_observe(const ClusterICEnvelope *env, const void *payload,
									   uint32 payload_len, uint32 self_node_id, int32 peer_id)
{
	if (!cluster_ic_envelope_verify(env, payload, payload_len, self_node_id, peer_id))
		return false; /* verify reject -- observe NOT called */
	cluster_ic_envelope_observe_scn(env, (int32)env->source_node_id);
	return true;
}

#endif /* USE_PGRAC_CLUSTER */
