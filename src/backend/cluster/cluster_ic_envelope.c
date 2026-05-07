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

#include "cluster/cluster_ic_envelope.h"


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

	out_env->magic = PGRAC_IC_ENVELOPE_MAGIC;
	out_env->version = PGRAC_IC_ENVELOPE_VERSION_V1;
	out_env->msg_type = msg_type;
	out_env->source_node_id = source_node_id;
	out_env->dest_node_id = dest_node_id;
	out_env->epoch = 0; /* spec-2.3 Q12; spec-2.4 翻 enforce */
	out_env->scn = 0;	/* spec-2.3 Q12; spec-2.10 接 piggyback */
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
cluster_ic_envelope_verify(const ClusterICEnvelope *env, const void *payload, uint32 self_node_id)
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
	 * Step 3: source_node_id sanity.  Caller-side (cluster_conf membership
	 * check) verifies the sender is a declared peer; here we only reject
	 * the broadcast sentinel as source (broadcast can only appear in
	 * dest_node_id).
	 */
	if (env->source_node_id == PGRAC_IC_BROADCAST)
		return false;

	/* Step 4: dest = self OR broadcast */
	if (env->dest_node_id != self_node_id && env->dest_node_id != PGRAC_IC_BROADCAST)
		return false;

	/* Step 5: payload_length sanity (per §3.5b inbound rule) */
	if (env->payload_length > PGRAC_IC_PAYLOAD_MAX)
		return false;

	/*
	 * Step 6: CRC.  compute_crc only walks env[0..32] + payload, so
	 * we don't need to zero env->payload_crc32c first.
	 */
	if (env->payload_crc32c != cluster_ic_envelope_compute_crc(env, payload))
		return false;

	/*
	 * epoch / scn fields read but NOT enforced in spec-2.3 (spec-2.4 /
	 * 2.10 flip enforce flags).  Caller may peek via env->epoch /
	 * env->scn for diagnostic but cannot reject on mismatch yet.
	 */
	return true;
}

#endif /* USE_PGRAC_CLUSTER */
